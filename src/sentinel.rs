use crate::infrastructure::time::display_millis as now_unix_ms;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use serde::Serialize;
use tokio::sync::{watch, Mutex, Notify, RwLock};
use tracing::{info, warn};

use crate::cluster::backend::{BackendConnectOptions, BackendConnection, BackendTlsConfig};
use crate::config::{normalize_addr, ProxyItem};
use crate::resp::{self, RespValue};

/// Sentinel 已验证的数据路由。只有 `Serving` 中的地址允许承载业务命令。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SentinelRoutingState {
    /// 正在从 Sentinel 发现并验证当前主节点，期间不开放数据路由。
    Resolving {
        /// 尚未产生可服务路由的原因。
        reason: String,
    },
    /// 已发现并验证当前主节点，可以承载业务流量。
    Serving {
        /// 已通过 Sentinel 发现和数据节点角色验证的当前主节点。
        master: String,
        /// 主节点变化或从保护态恢复时递增的路由代次。
        generation: u64,
        /// 最近一次成功确认当前路由的 Sentinel 地址。
        sentinel: String,
    },
    /// 发现或角色验证异常，主动阻止业务流量继续使用旧路由。
    Protected {
        /// 禁止继续把业务流量送往旧路由的保护原因。
        reason: String,
    },
}

impl SentinelRoutingState {
    /// 业务作用: 从当前路由状态读取可服务的主节点和代次。
    ///
    /// 返回：仅 `Serving` 返回主节点与 generation，其他状态返回 `None`。
    pub fn serving_route(&self) -> Option<(&str, u64)> {
        match self {
            Self::Serving {
                master, generation, ..
            } => Some((master, *generation)),
            Self::Resolving { .. } | Self::Protected { .. } => None,
        }
    }
}

/// 管理面使用的 Sentinel 路由快照，不包含任何认证凭据。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelStatus {
    /// Sentinel 监控的主节点逻辑名称。
    pub master_name: String,
    /// 当前路由状态：resolving、serving 或 protected。
    pub routing_state: String,
    /// 已验证且当前可服务的主节点地址。
    pub current_master: Option<String>,
    /// 当前可服务路由代次；非 Serving 状态为零。
    pub generation: u64,
    /// 最近一次成功响应发现请求的 Sentinel 地址。
    pub last_successful_sentinel: Option<String>,
    /// 最近一次发现和角色验证都成功的 Unix 毫秒时间。
    pub last_refresh_unix_ms: u64,
    /// 最近一次刷新失败原因；最近一次成功后清空。
    pub last_error: Option<String>,
    /// 当前代理配置的 Sentinel 控制节点数量。
    pub configured_sentinel_count: usize,
    /// 发现和角色验证成功累计次数。
    pub refresh_success_total: u64,
    /// 所有 Sentinel 或候选角色验证失败累计次数。
    pub refresh_failure_total: u64,
    /// 最近两次已验证主节点地址不同的累计次数，包含保护态期间的切换。
    pub master_change_total: u64,
}

/// Sentinel 客户端控制面。它只发现并验证主节点，不执行任何后端拓扑写操作。
#[derive(Debug)]
pub struct SentinelRuntime {
    /// 当前代理名称，用于日志和错误上下文。
    name: String,
    /// Sentinel 监控的主节点逻辑名称。
    master_name: String,
    /// 已规范化并去重的 Sentinel 控制节点地址。
    sentinel_nodes: Vec<String>,
    /// Sentinel ACL 用户名，与数据节点 ACL 相互独立。
    sentinel_username: Option<String>,
    /// Sentinel 认证密码，与数据节点密码相互独立。
    sentinel_password: Option<crate::infrastructure::credentials::SecretText>,
    /// Sentinel 控制连接专用 RESP TLS 配置。
    sentinel_tls: Option<BackendTlsConfig>,
    /// 业务数据节点 ACL 用户名。
    backend_username: Option<String>,
    /// 业务数据节点认证密码。
    backend_password: Option<crate::infrastructure::credentials::SecretText>,
    /// 业务数据节点专用 RESP TLS 配置。
    backend_tls: Option<BackendTlsConfig>,
    /// TCP 或 TLS 建连允许等待的最长毫秒数。
    connect_timeout_ms: u64,
    /// Sentinel 查询和数据节点角色验证允许等待的最长毫秒数。
    request_timeout_ms: u64,
    /// 正常状态下两轮主动发现之间的间隔毫秒数。
    refresh_interval_ms: u64,
    /// 控制连接和角色验证连接的 TCP keepalive 毫秒数。
    tcp_keepalive_ms: u64,
    /// 最近一次成功 Sentinel 的下标，用于下一轮优先查询。
    preferred_sentinel: AtomicUsize,
    /// 每个 Sentinel 节点独立复用的控制连接；失败后清空对应槽位并在下轮重建。
    sentinel_connections: Vec<Mutex<Option<BackendConnection>>>,
    /// 已验证主节点的角色检查连接；路由切换后只保留新主节点对应连接。
    master_connections: Mutex<HashMap<String, BackendConnection>>,
    /// 向数据面广播 Resolving、Serving 或 Protected 路由状态。
    routing_tx: watch::Sender<SentinelRoutingState>,
    /// 保留保护前的已验证地址与代次，并串行化发现和发布，避免并发刷新覆盖新路由。
    verified_route: Mutex<Option<(String, u64)>>,
    /// 数据面故障触发的即时刷新通知。
    refresh_notify: Notify,
    /// 最近一次完整刷新失败原因。
    last_error: RwLock<Option<String>>,
    /// 最近一次成功返回当前主节点的 Sentinel 地址。
    last_successful_sentinel: RwLock<Option<String>>,
    /// 最近一次完整刷新成功的 Unix 毫秒时间。
    last_refresh_unix_ms: AtomicU64,
    /// 完整刷新成功累计次数。
    refresh_success_total: AtomicU64,
    /// 完整刷新失败累计次数。
    refresh_failure_total: AtomicU64,
    /// 已验证主节点地址变化的累计次数，不因保护态清空历史。
    master_change_total: AtomicU64,
    /// 数据面最近是否报告当前路由连接失败；发现也失败时据此进入保护态。
    backend_failure_reported: AtomicBool,
}

impl SentinelRuntime {
    /// 业务作用: 从已经通过启动校验的代理配置构造 Sentinel 控制面。
    ///
    /// 参数说明：
    /// - `item`：sentinel 模式代理配置，包含控制节点、控制 ACL、数据 ACL、TLS 和超时。
    ///
    /// 返回：返回初始为 `Resolving` 的控制面；字段缺失或节点非法时返回错误。
    pub fn from_proxy_item(item: &ProxyItem) -> anyhow::Result<Self> {
        let master_name = item
            .sentinel_master_name
            .as_deref()
            .context("sentinel_master_name is required")?
            .trim()
            .to_string();
        let sentinel_nodes = item.sentinel_node_addrs()?;
        let sentinel_connections = sentinel_nodes
            .iter()
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>();
        let (routing_tx, _) = watch::channel(SentinelRoutingState::Resolving {
            reason: "sentinel route has not been resolved".to_string(),
        });
        Ok(Self {
            name: item.name.clone(),
            master_name,
            sentinel_nodes,
            sentinel_username: item.sentinel_username.clone(),
            sentinel_password: item.sentinel_password.clone(),
            sentinel_tls: BackendTlsConfig::for_sentinel(item),
            backend_username: item.username.clone(),
            backend_password: item.password.clone(),
            backend_tls: BackendTlsConfig::for_backend(item),
            connect_timeout_ms: item.sentinel_connect_timeout_ms,
            request_timeout_ms: item.sentinel_request_timeout_ms,
            refresh_interval_ms: item.sentinel_refresh_interval_ms,
            tcp_keepalive_ms: item.tcp_keepalive_ms,
            preferred_sentinel: AtomicUsize::new(0),
            sentinel_connections,
            master_connections: Mutex::new(HashMap::new()),
            routing_tx,
            verified_route: Mutex::new(None),
            refresh_notify: Notify::new(),
            last_error: RwLock::new(None),
            last_successful_sentinel: RwLock::new(None),
            last_refresh_unix_ms: AtomicU64::new(0),
            refresh_success_total: AtomicU64::new(0),
            refresh_failure_total: AtomicU64::new(0),
            master_change_total: AtomicU64::new(0),
            backend_failure_reported: AtomicBool::new(false),
        })
    }

    /// 业务作用: 为数据面会话创建当前 Sentinel 路由 watch 接收器。
    ///
    /// 返回：返回可独立消费的路由状态接收器。
    pub fn routing_rx(&self) -> watch::Receiver<SentinelRoutingState> {
        self.routing_tx.subscribe()
    }

    /// 业务作用: 请求后台任务立即执行一次额外发现，不等待固定刷新周期。
    ///
    /// 返回：无显式返回值；通过 Notify 合并并唤醒刷新任务。
    pub fn request_refresh(&self) {
        self.refresh_notify.notify_one();
    }

    /// 业务作用: 报告当前已发布主节点的数据面连接失败，并立即请求重新发现。
    ///
    /// 返回：无显式返回值；设置故障标记并唤醒刷新任务。若发现也失败将进入保护态。
    pub fn report_backend_failure(&self) {
        self.backend_failure_reported.store(true, Ordering::Release);
        self.refresh_notify.notify_one();
    }

    /// 业务作用: 读取就绪检查使用的路由状态和可选保护原因。
    ///
    /// 返回：返回稳定状态标签及 Resolving/Protected 的原因。
    pub fn routing_status(&self) -> (&'static str, Option<String>) {
        match self.routing_tx.borrow().clone() {
            SentinelRoutingState::Resolving { reason } => ("resolving", Some(reason)),
            SentinelRoutingState::Serving { .. } => ("serving", None),
            SentinelRoutingState::Protected { reason } => ("protected", Some(reason)),
        }
    }

    /// 业务作用: 生成不包含任何认证材料的 Sentinel 管理面快照。
    ///
    /// 返回：返回当前主节点、代次、最后成功 Sentinel、错误和累计计数。
    pub async fn status(&self) -> SentinelStatus {
        let route = self.routing_tx.borrow().clone();
        let (routing_state, current_master, generation) = match route {
            SentinelRoutingState::Resolving { .. } => ("resolving", None, 0),
            SentinelRoutingState::Serving {
                master, generation, ..
            } => ("serving", Some(master), generation),
            SentinelRoutingState::Protected { .. } => ("protected", None, 0),
        };
        SentinelStatus {
            master_name: self.master_name.clone(),
            routing_state: routing_state.to_string(),
            current_master,
            generation,
            last_successful_sentinel: self.last_successful_sentinel.read().await.clone(),
            last_refresh_unix_ms: self.last_refresh_unix_ms.load(Ordering::Relaxed),
            last_error: self.last_error.read().await.clone(),
            configured_sentinel_count: self.sentinel_nodes.len(),
            refresh_success_total: self.refresh_success_total.load(Ordering::Relaxed),
            refresh_failure_total: self.refresh_failure_total.load(Ordering::Relaxed),
            master_change_total: self.master_change_total.load(Ordering::Relaxed),
        }
    }

    /// 业务作用: 运行 Sentinel 周期刷新循环，并响应数据面触发的即时刷新。
    ///
    /// 参数说明：
    /// - `shutdown`：进程关闭 watch；变化后停止新的发现请求。
    ///
    /// 返回：正常关闭并等待订阅回收后成功；必需订阅提前结束或 panic 返回错误，由运行代监督关闭进程。
    pub async fn watch(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) -> anyhow::Result<()> {
        if *shutdown.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = self.refresh() => {}
            _ = shutdown.changed() => return Ok(()),
        }
        let mut subscription_tasks = tokio::task::JoinSet::new();
        for sentinel in self.sentinel_nodes.iter().cloned() {
            let runtime = Arc::clone(&self);
            let shutdown = shutdown.clone();
            subscription_tasks.spawn(async move {
                runtime.watch_switch_master_node(sentinel, shutdown).await;
            });
        }
        let mut interval = tokio::time::interval(Duration::from_millis(self.refresh_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // `interval` 的首个 tick 立即完成；首次刷新已经显式执行，先消费该 tick。
        interval.tick().await;
        let mut failed = false;
        loop {
            // 刷新在途时也要响应关闭信号:切代屏障会等待本任务退出,单次刷新的剩余时长
            // 不应叠加进发布窗口;取消等价于该轮刷新未发生,由下一轮或事件触发重试。
            tokio::select! {
                ended = subscription_tasks.join_next(), if !subscription_tasks.is_empty() => {
                    // 每个订阅都应自行重连；提前结束意味着发现监听不再完整，交由运行代监督收口。
                    if ended.is_some() {
                        failed = !*shutdown.borrow() || ended.is_some_and(|result| result.is_err());
                        break;
                    }
                }
                _ = interval.tick() => {
                    tokio::select! {
                        _ = self.refresh() => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
                _ = self.refresh_notify.notified() => {
                    tokio::select! {
                        _ = self.refresh() => {}
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        subscription_tasks.abort_all();
        while let Some(result) = subscription_tasks.join_next().await {
            if let Err(error) = result {
                failed |= !error.is_cancelled();
            }
        }
        anyhow::ensure!(!failed, "Sentinel subscription task exited abnormally");
        Ok(())
    }

    /// 业务作用: 持续订阅一个 Sentinel 的 `+switch-master` 频道，断线后按固定退避重连。
    ///
    /// 参数说明：
    /// - `sentinel`：当前订阅任务固定连接的 Sentinel `host:port` 地址。
    /// - `shutdown`：进程关闭 watch；变化后停止重连并退出订阅任务。
    ///
    /// 返回：无显式返回值；匹配当前 master name 的事件只触发重新查询，不直接提交路由。
    async fn watch_switch_master_node(
        self: Arc<Self>,
        sentinel: String,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let retry_delay = Duration::from_millis(self.refresh_interval_ms.max(500));
        loop {
            if *shutdown.borrow() {
                return;
            }
            if let Err(err) = self
                .consume_switch_master_events(&sentinel, &mut shutdown)
                .await
            {
                warn!(
                    proxy = %self.name,
                    sentinel = %sentinel,
                    error = %format!("{err:#}"),
                    "sentinel switch-master subscription disconnected"
                );
            }
            if *shutdown.borrow() {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(retry_delay) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// 业务作用: 建立一条 Sentinel Pub/Sub 连接，并消费当前服务的切主事件直到断线或关闭。
    ///
    /// 参数说明：
    /// - `sentinel`：本订阅连接使用的 Sentinel `host:port` 地址。
    /// - `shutdown`：进程关闭 watch；触发时立即停止等待下一条 Pub/Sub 消息。
    ///
    /// 返回：关闭信号返回成功；建连、订阅、RESP 解析或连接断开返回错误供外层重连。
    async fn consume_switch_master_events(
        &self,
        sentinel: &str,
        shutdown: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut conn = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
            addr: sentinel,
            username: self.sentinel_username.as_deref(),
            password: self.sentinel_password.as_deref(),
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            tcp_keepalive_ms: self.tcp_keepalive_ms,
            response_bytes_limit: Some(64 * 1024),
            metrics: None,
            tls: self.sentinel_tls.as_ref(),
        })
        .await?;
        let acknowledgement = conn
            .send_pipeline(&[resp::array(&[b"SUBSCRIBE", b"+switch-master"])])
            .await?
            .into_iter()
            .next()
            .context("sentinel SUBSCRIBE returned no acknowledgement")?;
        if acknowledgement.starts_with(b"-") {
            bail!(
                "sentinel SUBSCRIBE failed: {}",
                String::from_utf8_lossy(&acknowledgement).trim()
            );
        }

        loop {
            let response = tokio::select! {
                response = conn.read_one_response_compact() => response?,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    continue;
                }
            };
            if switch_master_event_matches(&response.bytes, &self.master_name)? {
                self.request_refresh();
            }
        }
    }

    /// 业务作用: 执行一轮完整发现、候选角色验证和安全路由提交。
    ///
    /// 参数说明: 无。
    ///
    /// 返回：发现并验证成功时返回成功；全部候选失败时记录错误并返回失败。
    pub async fn refresh(&self) -> anyhow::Result<()> {
        // 发现与发布使用同一串行边界；保护态只撤销可服务性，不能丢弃旧代身份。
        let mut verified_route = self.verified_route.lock().await;
        match self.discover_master().await {
            Ok((master, sentinel_index, sentinel)) => {
                let previous = self.routing_tx.borrow().clone();
                let changed = previous
                    .serving_route()
                    .is_none_or(|(current, _)| current != master);
                let generation = if changed {
                    let previous_generation =
                        verified_route.as_ref().map_or(0, |(_, value)| *value);
                    match previous_generation.checked_add(1) {
                        Some(value) => value,
                        None => {
                            // 代号耗尽时不能复用连接资源身份，保持保护并拒绝发布新路由。
                            let reason = "sentinel route generation exhausted".to_string();
                            self.routing_tx
                                .send_replace(SentinelRoutingState::Protected {
                                    reason: reason.clone(),
                                });
                            *self.last_error.write().await = Some(reason.clone());
                            self.refresh_failure_total.fetch_add(1, Ordering::Relaxed);
                            bail!(reason);
                        }
                    }
                } else {
                    previous
                        .serving_route()
                        .expect("serving route checked above")
                        .1
                };
                self.preferred_sentinel
                    .store(sentinel_index, Ordering::Relaxed);
                self.refresh_success_total.fetch_add(1, Ordering::Relaxed);
                self.backend_failure_reported
                    .store(false, Ordering::Release);
                self.last_refresh_unix_ms
                    .store(now_unix_ms(), Ordering::Relaxed);
                *self.last_successful_sentinel.write().await = Some(sentinel.clone());
                *self.last_error.write().await = None;

                if changed {
                    if verified_route
                        .as_ref()
                        .is_some_and(|(previous, _)| previous != &master)
                    {
                        self.master_change_total.fetch_add(1, Ordering::Relaxed);
                    }
                    // 即使恢复到同一地址，也发布新代；watch 合并中间保护通知时旧资源仍能识别失效。
                    *verified_route = Some((master.clone(), generation));
                    self.routing_tx.send_replace(SentinelRoutingState::Serving {
                        master: master.clone(),
                        generation,
                        sentinel: sentinel.clone(),
                    });
                    info!(
                        proxy = %self.name,
                        master_name = %self.master_name,
                        master = %master,
                        sentinel = %sentinel,
                        generation,
                        "sentinel route committed"
                    );
                }
                Ok(())
            }
            Err(err) => {
                self.refresh_failure_total.fetch_add(1, Ordering::Relaxed);
                let error = err.to_string();
                *self.last_error.write().await = Some(error.clone());
                if self.routing_tx.borrow().serving_route().is_none()
                    || self.backend_failure_reported.load(Ordering::Acquire)
                {
                    // 没有已验证路由，或业务连接与重新发现同时失败时，禁止继续使用旧资源。
                    self.routing_tx
                        .send_replace(SentinelRoutingState::Protected {
                            reason: error.clone(),
                        });
                }
                warn!(
                    proxy = %self.name,
                    master_name = %self.master_name,
                    error = %error,
                    "sentinel route refresh failed"
                );
                Err(err)
            }
        }
    }

    /// 业务作用: 按首选顺序查询所有 Sentinel，并只返回通过数据节点角色校验的候选。
    ///
    /// 返回：返回主节点地址、成功 Sentinel 下标和地址；全部失败时聚合原因。
    async fn discover_master(&self) -> anyhow::Result<(String, usize, String)> {
        let start = self.preferred_sentinel.load(Ordering::Relaxed) % self.sentinel_nodes.len();
        let mut failures = Vec::with_capacity(self.sentinel_nodes.len());
        for offset in 0..self.sentinel_nodes.len() {
            let index = (start + offset) % self.sentinel_nodes.len();
            let sentinel = &self.sentinel_nodes[index];
            match self.query_master(index, sentinel).await {
                Ok(master) => match self.verify_master(&master).await {
                    Ok(()) => return Ok((master, index, sentinel.clone())),
                    Err(err) => failures.push(format!(
                        "sentinel `{sentinel}` candidate role verification failed: {err:#}"
                    )),
                },
                Err(err) => failures.push(format!("sentinel `{sentinel}` query failed: {err:#}")),
            }
        }
        bail!(
            "no sentinel returned a verified master for `{}`: {}",
            self.master_name,
            failures.join("; ")
        )
    }

    /// 业务作用: 从一个 Sentinel 查询指定服务的当前主节点地址。
    ///
    /// 参数说明：
    /// - `sentinel_index`：节点在配置列表中的稳定下标，用于定位该节点私有的复用连接槽。
    /// - `sentinel`：本轮尝试的 Sentinel `host:port` 地址。
    ///
    /// 返回：返回规范化主节点地址；协议空值、错误或非法地址均返回失败。
    async fn query_master(&self, sentinel_index: usize, sentinel: &str) -> anyhow::Result<String> {
        let slot = self
            .sentinel_connections
            .get(sentinel_index)
            .context("sentinel connection slot is missing")?;
        let mut cached = slot.lock().await;
        if cached.is_none() {
            *cached = Some(
                BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
                    addr: sentinel,
                    username: self.sentinel_username.as_deref(),
                    password: self.sentinel_password.as_deref(),
                    connect_timeout_ms: self.connect_timeout_ms,
                    request_timeout_ms: self.request_timeout_ms,
                    tcp_keepalive_ms: self.tcp_keepalive_ms,
                    response_bytes_limit: Some(64 * 1024),
                    metrics: None,
                    tls: self.sentinel_tls.as_ref(),
                })
                .await?,
            );
        }

        let result = async {
            let conn = cached
                .as_mut()
                .context("sentinel connection cache was not initialized")?;
            let frame = conn
                .send_pipeline(&[resp::array(&[
                    b"SENTINEL",
                    b"get-master-addr-by-name",
                    self.master_name.as_bytes(),
                ])])
                .await?
                .into_iter()
                .next()
                .context("sentinel returned no response")?;
            parse_master_address(&frame)
        }
        .await;
        if result.is_err() {
            // 读写或协议失败后连接边界不再可信，禁止把它带入下一轮刷新。
            cached.take();
        }
        result
    }

    /// 业务作用: 直连候选数据节点并验证其当前角色确实为 master。
    ///
    /// 参数说明：
    /// - `master`：Sentinel 返回的候选数据节点地址。
    ///
    /// 返回：`ROLE` 或 `INFO replication` 明确确认主角色时成功，否则拒绝提交。
    async fn verify_master(&self, master: &str) -> anyhow::Result<()> {
        let cached = self.master_connections.lock().await.remove(master);
        let mut conn = match cached {
            Some(conn) => conn,
            None => {
                BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
                    addr: master,
                    username: self.backend_username.as_deref(),
                    password: self.backend_password.as_deref(),
                    connect_timeout_ms: self.connect_timeout_ms,
                    request_timeout_ms: self.request_timeout_ms,
                    tcp_keepalive_ms: self.tcp_keepalive_ms,
                    response_bytes_limit: Some(1024 * 1024),
                    metrics: None,
                    tls: self.backend_tls.as_ref(),
                })
                .await?
            }
        };

        let result = async {
            let role = conn
                .send_pipeline(&[resp::array(&[b"ROLE"])])
                .await?
                .into_iter()
                .next()
                .context("ROLE returned no response")?;
            if response_role_is_master(&role)? {
                return Ok(());
            }

            let info = conn
                .send_pipeline(&[resp::array(&[b"INFO", b"replication"])])
                .await?
                .into_iter()
                .next()
                .context("INFO replication returned no response")?;
            if response_info_role_is_master(&info)? {
                Ok(())
            } else {
                bail!("candidate `{master}` is not master")
            }
        }
        .await;

        if result.is_ok() {
            let mut connections = self.master_connections.lock().await;
            // 路由只允许一个 master；切换后立即淘汰旧主节点的角色检查连接。
            connections.clear();
            connections.insert(master.to_string(), conn);
        }
        result
    }
}

/// 业务作用: 解析 `SENTINEL get-master-addr-by-name` 的二元素地址响应。
///
/// 参数说明：
/// - `frame`：完整 RESP 响应帧。
///
/// 返回：返回规范化 `host:port`；空值、类型、元素数或地址非法时返回错误。
fn parse_master_address(frame: &[u8]) -> anyhow::Result<String> {
    let value = parse_full_value(frame)?;
    let RespValue::Array(Some(items)) = value else {
        bail!("expected sentinel master address array")
    };
    if items.len() != 2 {
        bail!("expected sentinel master address with 2 elements")
    }
    let host = value_text(&items[0]).context("sentinel master host is not a string")?;
    let port = value_text(&items[1]).context("sentinel master port is not a string")?;
    normalize_addr(&format!("{host}:{port}"))
}

/// 业务作用: 判断 `ROLE` 响应是否明确声明当前节点为 master。
///
/// 参数说明：
/// - `frame`：候选数据节点返回的完整 ROLE 响应。
///
/// 返回：主角色返回 true；合法的其他角色返回 false；非法 RESP 返回错误。
fn response_role_is_master(frame: &[u8]) -> anyhow::Result<bool> {
    match parse_full_value(frame)? {
        RespValue::Array(Some(items)) => Ok(items
            .first()
            .and_then(value_text)
            .is_some_and(|role| role.eq_ignore_ascii_case("master"))),
        RespValue::Error(_) => Ok(false),
        _ => Ok(false),
    }
}

/// 业务作用: 判断 `INFO replication` 响应正文是否明确包含 `role:master`。
///
/// 参数说明：
/// - `frame`：候选数据节点返回的完整 INFO 响应。
///
/// 返回：仅独立行 `role:master` 返回 true；其他内容返回 false。
fn response_info_role_is_master(frame: &[u8]) -> anyhow::Result<bool> {
    let value = parse_full_value(frame)?;
    let text = value_text(&value).context("INFO replication response is not a string")?;
    Ok(text
        .lines()
        .map(str::trim)
        .any(|line| line.eq_ignore_ascii_case("role:master")))
}

/// 业务作用: 解析且校验一个输入切片只包含一帧完整 RESP 值。
///
/// 参数说明：
/// - `frame`：待解析的完整响应字节。
///
/// 返回：返回解析后的 RESP 值；半帧或尾随字节均作为协议错误。
fn parse_full_value(frame: &[u8]) -> anyhow::Result<RespValue> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty RESP response")?;
    if consumed != frame.len() {
        bail!("RESP response contains trailing bytes")
    }
    Ok(value)
}

/// 业务作用: 从 Sentinel 响应允许的字符串类型中提取 UTF-8 文本。
///
/// 参数说明：
/// - `value`：待转换的 RESP bulk 或 simple string 值。
///
/// 返回：字符串类型且 UTF-8 合法时返回文本，否则返回 `None`。
fn value_text(value: &RespValue) -> Option<String> {
    match value {
        RespValue::Simple(bytes) | RespValue::Bulk(Some(bytes)) => {
            String::from_utf8(bytes.clone()).ok()
        }
        RespValue::Integer(value) => Some(value.to_string()),
        _ => None,
    }
}

/// 业务作用: 判断一条 Sentinel Pub/Sub 响应是否为当前服务的 `+switch-master` 事件。
///
/// 参数说明：
/// - `frame`：订阅连接读取到的完整 RESP 消息帧。
/// - `master_name`：当前代理配置的 Sentinel 主节点逻辑名称。
///
/// 返回：频道和 payload 服务名都匹配时返回 true；其他合法消息返回 false；非法 RESP 返回错误。
fn switch_master_event_matches(frame: &[u8], master_name: &str) -> anyhow::Result<bool> {
    let value = parse_full_value(frame)?;
    let RespValue::Array(Some(items)) = value else {
        return Ok(false);
    };
    if items.len() < 3 {
        return Ok(false);
    }
    let kind = value_text(&items[0]);
    let channel = value_text(&items[1]);
    let payload = value_text(&items[2]);
    Ok(kind.as_deref() == Some("message")
        && channel.as_deref() == Some("+switch-master")
        && payload
            .as_deref()
            .and_then(|value| value.split_whitespace().next())
            == Some(master_name))
}
