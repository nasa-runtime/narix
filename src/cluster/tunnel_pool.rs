use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::admin::ProxyMetrics;
use crate::cluster::backend::BackendConnection;
use crate::infrastructure::lifecycle::{TaskScope, TaskSpawner};

/// 单个后端的独占 tunnel 连接池。
///
/// 每个后端在启动时创建独立实例，普通命令不访问本对象；借还路径不查 HashMap、不复制
/// backend 地址，锁内也没有任何异步等待。
#[derive(Debug)]
pub struct TunnelPool {
    /// 当前后端按 RESP 协议隔离的 idle 连接队列；锁只保护内存队列变更，绝不跨越 await。
    state: Mutex<TunnelPoolState>,
    /// 当前后端最多允许保留的 idle 连接数量；达到上限后新归还连接直接关闭。
    max_idle: usize,
    /// 单条 idle 连接的最大空闲时长；None 表示关闭连接复用。
    idle_timeout: Option<Duration>,
    /// 当前代理实例的共享指标句柄，用于累计池命中、淘汰和 idle 总量。
    metrics: ProxyMetrics,
    owned_tasks: Mutex<Option<TaskScope>>,
}

/// 单个后端 tunnel pool 的受锁可变状态。
///
/// 三条队列按后端 RESP 协议隔离，禁止把 RESP2 连接借给 RESP3 会话；`idle_total`
/// 是三条队列长度之和，用于常数时间执行容量判断。
#[derive(Debug, Default)]
struct TunnelPoolState {
    /// 尚未协商 RESP 版本的后端 idle 连接。
    protocol_unspecified: VecDeque<IdleTunnelConnection>,
    /// 已协商 RESP2 的后端 idle 连接。
    resp2: VecDeque<IdleTunnelConnection>,
    /// 已协商 RESP3 的后端 idle 连接。
    resp3: VecDeque<IdleTunnelConnection>,
    /// 三条协议队列当前保存的连接总数。
    idle_total: usize,
}

/// 一条已经证明 clean、当前处于空闲状态的后端连接。
#[derive(Debug)]
struct IdleTunnelConnection {
    /// 已完成 TCP 建连、可选认证和 RESP 协议同步的后端连接。
    conn: BackendConnection,
    /// 最近一次成功归池的单调时钟时间，用于借出和后台回收时判定过期。
    idle_since: Instant,
}

/// 一条独占借出的 tunnel 后端连接。
///
/// Drop 默认丢弃连接；只有调用方完整消费唯一响应、确认无残留字节后显式
/// `release_clean`，连接才允许回池。
#[derive(Debug)]
pub struct TunnelLease {
    /// 创建本 lease 的后端专属池；用于显式 clean 归还或 tainted 丢弃。
    pool: Arc<TunnelPool>,
    /// 本连接已经协商的 RESP 版本；归池时按该值放回对应协议队列。
    protocol_version: Option<u8>,
    /// lease 独占持有的连接；取走后必须为 None，避免 Drop 重复处理。
    conn: Option<BackendConnection>,
}

impl TunnelPool {
    /// 业务作用: 创建一个后端专属 tunnel pool，并在启用复用时启动空闲连接回收任务。
    ///
    /// 参数说明:
    /// - `max_idle`: 当前后端最多保留的 clean idle 连接数；零表示关闭复用。
    /// - `idle_timeout_ms`: idle 连接允许连续空闲的毫秒数；零表示关闭复用。
    /// - `metrics`: 当前代理实例共享的指标对象；多个后端池的 idle 指标在这里聚合。
    ///
    /// 返回:
    /// - 返回可共享的池句柄。池只在特殊 tunnel 路径使用，不进入普通 pipeline 热路径。
    pub fn new(max_idle: usize, idle_timeout_ms: u64, metrics: ProxyMetrics) -> Arc<Self> {
        let scope = TaskScope::default();
        let pool = Self::with_tasks(max_idle, idle_timeout_ms, metrics, scope.spawner());
        *pool
            .owned_tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(scope);
        pool
    }

    /// 业务作用: 把后端隧道池的周期回收绑定到代理运行代，避免池释放后留下独立后台任务。
    /// 参数说明:
    /// - `max_idle`: 当前后端可保留的空闲连接上限。
    /// - `idle_timeout_ms`: 空闲连接存活毫秒数；零表示禁用复用。
    /// - `metrics`: 当前代理共享计数单元。
    /// - `tasks`: 运行代负责关闭和等待的弱任务入口。
    ///
    /// 返回: 按协议隔离的连接池，周期任务由运行代统一回收。
    pub(crate) fn with_tasks(
        max_idle: usize,
        idle_timeout_ms: u64,
        metrics: ProxyMetrics,
        tasks: TaskSpawner,
    ) -> Arc<Self> {
        let max_idle = if idle_timeout_ms == 0 { 0 } else { max_idle };
        let idle_timeout = (max_idle > 0).then(|| Duration::from_millis(idle_timeout_ms));
        let pool = Arc::new(Self {
            state: Mutex::new(TunnelPoolState::default()),
            max_idle,
            idle_timeout,
            metrics,
            owned_tasks: Mutex::new(None),
        });
        pool.metrics.register_tunnel_pool(max_idle);
        spawn_idle_reaper(&pool, &tasks);
        pool
    }

    /// 业务作用: 尝试独占借出一条协议兼容且尚未过期的 idle 连接。
    ///
    /// 参数说明:
    /// - `protocol_version`: 前端会话协商的 RESP 版本；None、RESP2、RESP3 分池存放。
    ///
    /// 返回:
    /// - `Some(TunnelLease)`: 命中一条可独占使用的 clean 连接。
    /// - `None`: 没有可复用连接，调用方必须新建并认证后端连接。
    ///
    /// 关键步骤:
    /// 1. 只在短临界区内弹出队尾连接并剔除过期项。
    /// 2. 释放锁后再销毁过期 socket 和更新指标，避免 close 系统调用扩大锁竞争。
    pub fn acquire_idle(self: &Arc<Self>, protocol_version: Option<u8>) -> Option<TunnelLease> {
        let mut expired = Vec::new();
        let conn = {
            let mut state = self.state.lock().expect("tunnel pool state mutex poisoned");
            let mut conn = None;
            while let Some(entry) = protocol_queue_mut(&mut state, protocol_version).pop_back() {
                if self
                    .idle_timeout
                    .is_some_and(|timeout| entry.idle_since.elapsed() >= timeout)
                {
                    expired.push(entry);
                    continue;
                }
                if !entry.conn.is_idle_connection_usable() {
                    expired.push(entry);
                    continue;
                }
                conn = Some(entry.conn);
                break;
            }
            let removed = expired.len() + usize::from(conn.is_some());
            state.idle_total = state.idle_total.saturating_sub(removed);
            conn
        };

        let removed = expired.len() + usize::from(conn.is_some());
        self.metrics.tunnel_pool_idle_removed(removed);
        if !expired.is_empty() {
            self.metrics
                .record_tunnel_pool_discards(expired.len() as u64);
            drop(expired);
        }

        match conn {
            Some(conn) => {
                self.metrics.record_tunnel_pool_hit();
                Some(TunnelLease {
                    pool: Arc::clone(self),
                    protocol_version,
                    conn: Some(conn),
                })
            }
            None => {
                self.metrics.record_tunnel_pool_miss();
                None
            }
        }
    }

    /// 业务作用: 把新建且已完成认证/协议同步的后端连接包装为独占 lease。
    ///
    /// 参数说明:
    /// - `protocol_version`: 新连接已经同步的 RESP 协议版本。
    /// - `conn`: 已建连、已认证且尚未执行状态型业务命令的后端连接。
    ///
    /// 返回:
    /// - 返回独占 lease；调用方必须显式证明 clean 才能归池。
    pub fn lease_new(
        self: &Arc<Self>,
        protocol_version: Option<u8>,
        conn: BackendConnection,
    ) -> TunnelLease {
        TunnelLease {
            pool: Arc::clone(self),
            protocol_version,
            conn: Some(conn),
        }
    }

    /// 业务作用: 把已经完整消费唯一响应的 clean 连接放回对应协议队列。
    ///
    /// 参数说明:
    /// - `protocol_version`: 连接的 RESP 协议版本，用于选择隔离队列。
    /// - `conn`: 已确认没有残留响应字节和持久连接状态的后端连接。
    ///
    /// 返回:
    /// - 无返回值。池关闭、池满且无过期项时，连接会在锁外自然关闭。
    ///
    /// 关键步骤:
    /// 1. 池满时先在锁内移走过期连接，优先腾出容量。
    /// 2. 只把 clean 连接写入队列；被移走和未归池连接均在锁外析构。
    fn release_clean(&self, protocol_version: Option<u8>, conn: BackendConnection) {
        let Some(idle_timeout) = self.idle_timeout else {
            self.metrics.record_tunnel_pool_discard();
            return;
        };

        let mut expired = Vec::new();
        let mut conn = Some(conn);
        let returned = {
            let mut state = self.state.lock().expect("tunnel pool state mutex poisoned");
            if state.idle_total >= self.max_idle {
                reap_expired_state(&mut state, idle_timeout, &mut expired);
            }
            if state.idle_total < self.max_idle {
                protocol_queue_mut(&mut state, protocol_version).push_back(IdleTunnelConnection {
                    conn: conn
                        .take()
                        .expect("clean tunnel connection must be available"),
                    idle_since: Instant::now(),
                });
                state.idle_total += 1;
                true
            } else {
                false
            }
        };

        if !expired.is_empty() {
            self.metrics.tunnel_pool_idle_removed(expired.len());
            self.metrics
                .record_tunnel_pool_discards(expired.len() as u64);
            drop(expired);
        }
        if returned {
            self.metrics.tunnel_pool_idle_added(1);
            self.metrics.record_tunnel_pool_return();
        } else {
            self.metrics.record_tunnel_pool_discard();
        }
    }

    /// 业务作用: 从当前后端的全部协议队列移走超过 idle 阈值的连接。
    ///
    /// 返回:
    /// - 返回已从池状态移除的连接集合；调用方必须在锁外丢弃该集合以关闭 socket。
    fn reap_expired(&self) -> Vec<IdleTunnelConnection> {
        let Some(idle_timeout) = self.idle_timeout else {
            return Vec::new();
        };
        let mut expired = Vec::new();
        {
            let mut state = self.state.lock().expect("tunnel pool state mutex poisoned");
            reap_expired_state(&mut state, idle_timeout, &mut expired);
        }
        if !expired.is_empty() {
            self.metrics.tunnel_pool_idle_removed(expired.len());
            self.metrics
                .record_tunnel_pool_discards(expired.len() as u64);
        }
        expired
    }
}

impl Drop for TunnelPool {
    /// 业务作用: 销毁不再被任何路由代次引用的后端池，并同步注销容量与残留 idle 指标。
    ///
    /// 返回:
    /// - 无返回值；修正聚合指标后，池内连接随字段析构自动关闭。
    fn drop(&mut self) {
        let state = match self.state.get_mut() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let idle_total = state.idle_total;
        self.metrics.unregister_tunnel_pool(self.max_idle);
        self.metrics.tunnel_pool_idle_removed(idle_total);
        if idle_total > 0 {
            self.metrics.record_tunnel_pool_discards(idle_total as u64);
        }
    }
}

impl TunnelLease {
    /// 业务作用: 获取 lease 独占连接的可变引用，用于写入 tunnel 初始命令或读取唯一响应。
    ///
    /// 返回:
    /// - 返回后端连接可变引用。连接已消费时属于内部编程错误并立即 panic。
    pub fn connection_mut(&mut self) -> &mut BackendConnection {
        self.conn
            .as_mut()
            .expect("tunnel lease connection already consumed")
    }

    /// 业务作用: 显式证明连接 clean 并归还池。
    ///
    /// 返回:
    /// - 无返回值。容量不足或复用关闭时，连接会被安全关闭而不会进入池。
    pub fn release_clean(mut self) {
        let conn = self
            .conn
            .take()
            .expect("tunnel lease connection already consumed");
        self.pool.release_clean(self.protocol_version, conn);
    }

    /// 业务作用: 状态型命令进入透明 relay 后连接不可复用。
    ///
    /// 返回:
    /// - 返回后端连接。该连接结束后只能关闭，禁止重新进入 pool。
    pub fn into_tainted_connection(mut self) -> BackendConnection {
        self.pool.metrics.record_tunnel_pool_discard();
        self.conn
            .take()
            .expect("tunnel lease connection already consumed")
    }
}

impl Drop for TunnelLease {
    /// 业务作用: lease 未显式 clean 归还或移交 relay 时，按 tainted 连接记录并关闭。
    ///
    /// 返回:
    /// - 无返回值；通过连接字段自然析构完成 socket 关闭。
    fn drop(&mut self) {
        if self.conn.is_some() {
            self.pool.metrics.record_tunnel_pool_discard();
        }
    }
}

/// 业务作用: 根据会话 RESP 版本选择严格隔离的 idle 队列。
///
/// 参数说明:
/// - `state`: 当前后端池的受锁状态。
/// - `protocol_version`: 前端/后端已经协商的 RESP 版本。
///
/// 返回:
/// - 返回对应协议队列的可变引用。
fn protocol_queue_mut(
    state: &mut TunnelPoolState,
    protocol_version: Option<u8>,
) -> &mut VecDeque<IdleTunnelConnection> {
    match protocol_version {
        Some(2) => &mut state.resp2,
        Some(3) => &mut state.resp3,
        _ => &mut state.protocol_unspecified,
    }
}

/// 业务作用: 在持锁状态下从全部协议队列移走过期连接并修正 idle 总数。
///
/// 参数说明:
/// - `state`: 当前后端池的受锁状态。
/// - `idle_timeout`: 单条连接允许的最大空闲时长。
/// - `expired`: 接收本轮被移出连接的锁外析构集合；允许调用方预先保存其他待关闭连接。
///
/// 返回:
/// - 无返回值；通过修改 `state` 和 `expired` 完成状态迁移。
fn reap_expired_state(
    state: &mut TunnelPoolState,
    idle_timeout: Duration,
    expired: &mut Vec<IdleTunnelConnection>,
) {
    let previous_expired = expired.len();
    reap_queue(&mut state.protocol_unspecified, idle_timeout, expired);
    reap_queue(&mut state.resp2, idle_timeout, expired);
    reap_queue(&mut state.resp3, idle_timeout, expired);
    let removed = expired.len().saturating_sub(previous_expired);
    state.idle_total = state.idle_total.saturating_sub(removed);
}

/// 业务作用: 从单条协议队列头部连续移走过期连接。
///
/// 参数说明:
/// - `queue`: 按归池时间从旧到新排列的协议队列。
/// - `idle_timeout`: 单条连接允许的最大空闲时长。
/// - `expired`: 接收被移出连接的锁外析构集合。
///
/// 返回:
/// - 无返回值。遇到第一条未过期连接立即停止，因为其后连接只会更新。
fn reap_queue(
    queue: &mut VecDeque<IdleTunnelConnection>,
    idle_timeout: Duration,
    expired: &mut Vec<IdleTunnelConnection>,
) {
    while queue
        .front()
        .is_some_and(|entry| entry.idle_since.elapsed() >= idle_timeout)
    {
        if let Some(entry) = queue.pop_front() {
            expired.push(entry);
        }
    }
}

/// 业务作用: 为启用复用的后端池启动弱引用 idle 回收任务。
///
/// 参数说明:
/// - `pool`: 当前后端专属池；后台任务只持 Weak，不阻止运行时释放池。
/// - `tasks`: 池所属运行代的任务创建入口。
///
/// 返回:
/// - 无返回值。没有 Tokio runtime 或复用关闭时不创建任务。
///
/// 关键步骤:
/// 1. 扫描周期取 idle 阈值一半，并限制在 200ms 到 10s，兼顾回收及时性与空转成本。
/// 2. 每次先在短锁内移出连接，再在锁外关闭，避免 socket 析构阻塞借还路径。
fn spawn_idle_reaper(pool: &Arc<TunnelPool>, tasks: &TaskSpawner) {
    let Some(idle_timeout) = pool.idle_timeout else {
        return;
    };
    let Ok(_) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(pool);
    let interval = (idle_timeout / 2).clamp(Duration::from_millis(200), Duration::from_secs(10));
    let _ = tasks.spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let Some(pool) = weak.upgrade() else {
                return;
            };
            let expired = pool.reap_expired();
            // 连接析构可能触发 socket close，必须在池锁之外执行。
            drop(expired);
        }
    });
}
