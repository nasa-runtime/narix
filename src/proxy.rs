use std::collections::HashMap;
use std::io::{self, ErrorKind, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bytes::Bytes;
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

use crate::admin::{
    DedicatedTunnelGuard, DedicatedTunnelQueueMetricsGuard, ProxyGenerationActivity, ProxyMetrics,
    ProxyRegistration, ProxyStaticStatus, RuntimeRegistry,
};
use crate::auth::{handle_frontend_auth_with_backend_profile, FrontendAuth};
use crate::backend::BackendProfile;
use crate::cluster::backend::{
    BackendConnectOptions, BackendConnection, BackendRawResponses, BackendSingleResponse,
    BackendStream, BackendTlsConfig,
};
use crate::cluster::pipeline_pool::{NodePipelineHandle, PipelineBackendPool, PipelinePoolProfile};
use crate::cluster::router::{ClientCommand, ClusterPipelineResponse, ClusterRuntime};
use crate::cluster::slot::redis_slot;
use crate::cluster::tunnel_pool::{TunnelLease, TunnelPool};
use crate::config::{normalize_addr, AuthMode, ClusterNodeProxyItem, ProxyItem, ProxyMode};
use crate::coordinator::runtime::CoordinatorHandle;
use crate::ha::backend::ReplicationHaBackend;
use crate::ha::driver::HaBackendDriver;
use crate::ha::model::{parse_nodes as parse_ha_nodes, HaNode, HaRole};
use crate::ha::routing::HaRoutingState;
use crate::ha::runtime::ReplicationHaRuntime;
use crate::resp;
use crate::sentinel::{SentinelRoutingState, SentinelRuntime};

const RELAY_BUFFER_SIZE: usize = 64 * 1024;
// 前端连接可能大量空闲，不能为每条连接预分配 64KB。8KB 读取窗口能覆盖常见命令，
// 大帧仍会在会话缓冲中按实际输入增长，并继续受 pipeline_max_bytes 限制。
const FRONTEND_READ_BUFFER_SIZE: usize = 8 * 1024;
const MAX_RESPONSE_VECTORED_SLICES: usize = 256;
const FRONTEND_ZERO_COPY_BATCH_MIN: usize = 4 * 1024;
// QUIT 的本地应答:proxy 直接回 +OK 并关闭前端,不为断连新建后端连接。
const QUIT_OK_RESPONSE: &[u8] = b"+OK\r\n";

/// 业务作用: 保存前端认证后多读的首包字节和已经协商的 RESP 版本。
#[derive(Debug, Default)]
// 表示前端认证后的连接状态，用于把首包剩余字节和 HELLO 协商协议传给后端转发路径。
struct FrontendAuthResult {
    /// 认证读取阶段已经从 socket 多读、但尚未进入业务命令循环的原始字节。
    leftover: Vec<u8>,
    /// 前端通过 HELLO 协商的 RESP 版本；None 表示尚未显式协商。
    protocol_version: Option<u8>,
}

/// 业务作用: 把一个固定后端地址与其 pipeline 池和专用隧道池绑定为独立数据面代次。
#[derive(Debug)]
// 表示 StandaloneRuntime 的数据结构，用于维护 standalone 普通 mux 和 pipeline pool。
struct StandaloneRuntime {
    /// standalone 代理固定使用的后端地址。
    backend: String,
    /// 普通无状态命令使用的共享 pipeline 后端池。
    pipeline_pool: Arc<PipelineBackendPool>,
    /// 阻塞/状态型命令独占使用的后端专属 tunnel idle 池。
    tunnel_pool: Arc<TunnelPool>,
}

impl StandaloneRuntime {
    /// 业务作用: 为固定 standalone 后端创建 pipeline 与专用隧道资源。
    /// 参数说明:
    /// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    fn from_proxy_item(item: &ProxyItem, metrics: ProxyMetrics) -> anyhow::Result<Self> {
        Self::for_backend(item, item.backend_addr()?, metrics)
    }

    /// 业务作用: 为一个已经通过控制面验证的固定路由代次创建私有连接资源。
    ///
    /// 参数说明：
    /// - `item`：当前代理配置，提供连接池容量、认证、TLS、超时和 tunnel 回收参数。
    /// - `backend`：本资源代次唯一允许连接的已规范化数据节点地址。
    /// - `metrics`：当前代理共享指标句柄，新建的 pipeline/tunnel 池均归属该代理。
    ///
    /// 返回：成功时返回绑定指定后端的完整 standalone 数据面资源。
    fn for_backend(
        item: &ProxyItem,
        backend: String,
        metrics: ProxyMetrics,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            backend,
            pipeline_pool: Arc::new(PipelineBackendPool::new(item, metrics.clone())),
            tunnel_pool: TunnelPool::new(
                item.dedicated_tunnel_pool_max_idle,
                item.dedicated_tunnel_pool_idle_timeout_ms,
                metrics,
            ),
        })
    }
}

/// Sentinel 当前主节点代次的数据面资源。路由切换时原子替换整组 pool，旧代不再接受新批次。
#[derive(Debug)]
struct SentinelDataRuntime {
    /// 当前已发布主节点地址、路由代次及该代次独占的数据面资源。
    current: StdMutex<Option<(String, u64, Arc<StandaloneRuntime>)>>,
    /// 当前代理共享指标句柄，供后续每个动态代次创建连接池。
    metrics: ProxyMetrics,
}

impl SentinelDataRuntime {
    /// 业务作用: 创建尚未绑定任何 Sentinel 路由代次的数据面资源容器。
    ///
    /// 参数说明：
    /// - `metrics`：当前 sentinel 代理的共享指标句柄，后续每代连接池均使用它记账。
    ///
    /// 返回：返回空的代次容器；首次安全路由发布后再按需创建连接池。
    fn new(metrics: ProxyMetrics) -> Self {
        Self {
            current: StdMutex::new(None),
            metrics,
        }
    }

    /// 业务作用: 获取指定 Sentinel 路由代次的数据面资源，相同代次复用，不同代次整体替换。
    ///
    /// 参数说明：
    /// - `item`：当前代理配置，供新代次创建认证、TLS、pipeline 和 tunnel 资源。
    /// - `master`：Sentinel 返回且已经过角色校验的当前主节点地址。
    /// - `generation`：控制面单调递增的路由代次，用于隔离切换前后的连接池。
    ///
    /// 返回：返回只绑定给定主节点和代次的共享数据面资源。
    fn resources(
        &self,
        item: &ProxyItem,
        master: &str,
        generation: u64,
    ) -> anyhow::Result<Arc<StandaloneRuntime>> {
        let mut current = self
            .current
            .lock()
            .expect("sentinel data runtime mutex poisoned");
        if let Some((cached_master, cached_generation, runtime)) = current.as_ref() {
            if cached_master == master && *cached_generation == generation {
                return Ok(runtime.clone());
            }
        }
        let runtime = Arc::new(StandaloneRuntime::for_backend(
            item,
            master.to_string(),
            self.metrics.clone(),
        )?);
        *current = Some((master.to_string(), generation, runtime.clone()));
        Ok(runtime)
    }
}

/// 单条前端会话缓存的 Sentinel 路由代次，用于每个完整批次开始前无锁检查 watch 状态。
struct SentinelSessionContext {
    /// 当前 Sentinel 代理的发现和安全路由控制面。
    control: Arc<SentinelRuntime>,
    /// 当前主节点代次对应的数据面资源容器。
    data: Arc<SentinelDataRuntime>,
    /// 当前前端会话私有的 Sentinel 路由变化接收器。
    routing_rx: watch::Receiver<SentinelRoutingState>,
    /// 当前会话已经同步的数据路由代次。
    generation: u64,
}

/// Standalone 与 Sentinel 共用数据面进入单条前端会话时所需的运行时资源。
struct StandaloneConnectionRuntime {
    /// 当前会话已同步路由代次对应的 standalone 数据面资源。
    runtime: Arc<StandaloneRuntime>,
    /// Sentinel 模式的会话控制上下文；固定 standalone 模式为空。
    sentinel: Option<SentinelSessionContext>,
    /// 当前代理的独占隧道并发和排队限制器。
    tunnel_limiter: Arc<DedicatedTunnelLimiter>,
}

/// 业务作用: 集中保存当前代理各模式私有运行时和共享专用隧道准入器。
#[derive(Clone)]
struct ProxyRuntimes {
    /// Cluster 单入口模式私有运行时；其他模式为空。
    cluster: Option<Arc<ClusterRuntime>>,
    /// Standalone 模式私有运行时；其中包含普通池和当前 proxy 唯一后端的 tunnel 池。
    standalone: Option<Arc<StandaloneRuntime>>,
    /// replication HA 模式私有路由、探测和切换运行时。
    replication_ha: Option<Arc<ReplicationHaRuntime>>,
    /// Sentinel 模式私有发现和动态主节点路由运行时。
    sentinel: Option<Arc<SentinelRuntime>>,
    /// Sentinel 当前路由代次对应的 pipeline 与独占 tunnel 资源。
    sentinel_data: Option<Arc<SentinelDataRuntime>>,
    /// 当前 proxy 所有在用 tunnel lease 共享的并发和排队准入器。
    tunnel_limiter: Arc<DedicatedTunnelLimiter>,
}

/// 透明隧道和阻塞命令统一使用的动态后端路由观察器。
enum DynamicRoutingReceiver {
    /// replication HA 当前主节点路由变化。
    ReplicationHa(watch::Receiver<HaRoutingState>),
    /// Sentinel 当前已验证主节点路由变化。
    Sentinel(watch::Receiver<SentinelRoutingState>),
}

/// 单条可复用阻塞命令等待期间需要观察的后端路由。
struct BlockingRouteContext<'a> {
    /// 当前阻塞命令实际占用的后端地址。
    backend_addr: &'a str,
    /// 可选动态路由观察器；固定路由模式为空。
    backend_change: Option<DynamicRoutingReceiver>,
}

impl DynamicRoutingReceiver {
    /// 业务作用: 判断旧后端是否仍属于当前动态路由的可服务节点。
    ///
    /// 参数说明：
    /// - `old_backend`：现有透明隧道或阻塞命令实际连接的后端地址。
    ///
    /// 返回：旧连接必须关闭时返回原因；路由仍指向原地址时返回 `None`。
    fn close_reason(&self, old_backend: &str) -> Option<String> {
        match self {
            Self::ReplicationHa(receiver) => ha_relay_close_reason(&receiver.borrow(), old_backend),
            Self::Sentinel(receiver) => {
                sentinel_relay_close_reason(&receiver.borrow(), old_backend)
            }
        }
    }

    /// 业务作用: 等待动态路由 watch 发布下一个版本。
    ///
    /// 返回：路由发生变化时成功；发送端销毁时返回 watch 接收错误。
    async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        match self {
            Self::ReplicationHa(receiver) => receiver.changed().await,
            Self::Sentinel(receiver) => receiver.changed().await,
        }
    }
}

/// 业务作用: 以信号量、等待人数和超时限制连接绑定命令占用的独占后端连接。
#[derive(Debug)]
struct DedicatedTunnelLimiter {
    /// 非零 active 上限对应的许可集合；None 表示明确配置为不限并发。
    semaphore: Option<Arc<Semaphore>>,
    /// active 满后最多允许同时等待许可的前端请求数量。
    queue_size: usize,
    /// 进入等待队列后获取 active 许可的最长时间。
    acquire_timeout: Duration,
    /// 当前正在等待 active 许可的请求数，用于无锁执行 queue 上限判断。
    waiters: AtomicUsize,
}

/// 业务作用: 绑定专用隧道并发许可与指标守卫，析构时自动释放两类资源。
#[derive(Debug)]
struct DedicatedTunnelPermit {
    /// 可选 active 信号量许可；permit 析构时自动释放在用名额。
    _semaphore_permit: Option<OwnedSemaphorePermit>,
    /// 专用 tunnel 指标守卫；析构时自动减少 active gauge。
    _metrics_guard: DedicatedTunnelGuard,
}

/// 业务作用: 区分专用隧道满载拒绝、排队超时和信号量关闭，供前端返回稳定错误与指标。
#[derive(Debug)]
enum DedicatedTunnelAcquireError {
    Full,
    Timeout,
    Closed,
}

impl DedicatedTunnelLimiter {
    /// 业务作用: 按专用隧道并发、队列和等待超时配置构造准入限制器。
    /// 参数说明:
    /// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
    ///
    fn from_item(item: &ProxyItem) -> Self {
        Self {
            semaphore: (item.dedicated_tunnel_max_active > 0)
                .then(|| Arc::new(Semaphore::new(item.dedicated_tunnel_max_active))),
            queue_size: item.dedicated_tunnel_queue_size,
            acquire_timeout: Duration::from_millis(item.dedicated_tunnel_acquire_timeout_ms),
            waiters: AtomicUsize::new(0),
        }
    }

    /// 业务作用: 以立即获取、受限排队和超时三段策略申请专用隧道许可，并同步更新指标。
    /// 参数说明:
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 取得许可时返回带指标守卫的专用隧道许可；满载、超时或关闭时返回分类错误。
    async fn acquire(
        &self,
        metrics: &ProxyMetrics,
    ) -> Result<DedicatedTunnelPermit, DedicatedTunnelAcquireError> {
        metrics.record_dedicated_tunnel_acquire_attempt();
        let Some(semaphore) = &self.semaphore else {
            return Ok(DedicatedTunnelPermit {
                _semaphore_permit: None,
                _metrics_guard: metrics.dedicated_tunnel_opened(),
            });
        };
        if let Ok(permit) = semaphore.clone().try_acquire_owned() {
            return Ok(DedicatedTunnelPermit {
                _semaphore_permit: Some(permit),
                _metrics_guard: metrics.dedicated_tunnel_opened(),
            });
        }
        let Some(_queue_guard) = self.try_enter_queue(metrics) else {
            metrics.record_dedicated_tunnel_rejection();
            return Err(DedicatedTunnelAcquireError::Full);
        };
        match tokio::time::timeout(self.acquire_timeout, semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => Ok(DedicatedTunnelPermit {
                _semaphore_permit: Some(permit),
                _metrics_guard: metrics.dedicated_tunnel_opened(),
            }),
            Ok(Err(_)) => {
                metrics.record_dedicated_tunnel_rejection();
                Err(DedicatedTunnelAcquireError::Closed)
            }
            Err(_) => {
                metrics.record_dedicated_tunnel_acquire_timeout();
                Err(DedicatedTunnelAcquireError::Timeout)
            }
        }
    }

    /// 业务作用: 使用原子计数在队列上限内登记等待者，并返回自动离队守卫。
    /// 参数说明:
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    fn try_enter_queue(&self, metrics: &ProxyMetrics) -> Option<DedicatedTunnelQueueGuard<'_>> {
        if self.queue_size == 0 {
            return None;
        }
        let mut current = self.waiters.load(Ordering::Relaxed);
        loop {
            if current >= self.queue_size {
                return None;
            }
            match self.waiters.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let metrics_guard = metrics.dedicated_tunnel_queue_waiter_entered();
                    return Some(DedicatedTunnelQueueGuard {
                        waiters: &self.waiters,
                        _metrics_guard: metrics_guard,
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

/// 业务作用: 持有专用隧道排队名额与指标守卫，离开作用域时原子释放两者。
#[derive(Debug)]
struct DedicatedTunnelQueueGuard<'a> {
    waiters: &'a AtomicUsize,
    _metrics_guard: DedicatedTunnelQueueMetricsGuard,
}

impl Drop for DedicatedTunnelQueueGuard<'_> {
    /// 业务作用: 释放 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// 业务作用: 启动单个代理监听并管理其激活、连接准入和优雅关闭生命周期。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `generation`: 当前代理名称下由 supervisor 分配的配置运行代编号。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 无注册表入口用于嵌入式/测试启动，不产生管理面运行时登记。
pub async fn serve_listener(
    item: ProxyItem,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    serve_listener_with_registry(item, shutdown, None).await
}

/// 业务作用: 在可选管理注册表下启动代理运行代，使监听发布、状态注册和旧代排空遵循同一生命周期。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `registry`: 进程内代理注册表，用于汇总状态并按名称定位运行时。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 单进程入口不绑定集群协调句柄；需要共识权威的调用方走完整 coordinator 入口。
pub async fn serve_listener_with_registry(
    item: ProxyItem,
    shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
) -> anyhow::Result<()> {
    serve_listener_with_registry_and_coordinator(item, shutdown, registry, None, None).await
}

/// 向稳定业务监听器提交新代理运行代的控制句柄。
#[derive(Clone)]
pub struct ProxyGenerationController {
    updates: mpsc::Sender<ProxyGenerationCommand>,
}

impl ProxyGenerationController {
    /// 业务作用: 在稳定监听器内部预构造同一名称、同一监听地址的新运行代，但不发布。
    ///
    /// 参数说明:
    /// - `item`: 已通过全局静态校验和后端预检的候选代理配置。
    /// - `generation`: 运行时 supervisor 分配的单调递增配置代次。
    /// - `cluster_config_digest`: 候选运行代所依据的全局集群配置摘要。
    ///
    /// 返回: 新运行代完整构造并暂存后返回成功，失败时旧运行代保持活动。
    pub async fn prepare(
        &self,
        item: ProxyItem,
        generation: u64,
        cluster_config_digest: Option<String>,
    ) -> Result<(), String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.updates
            .send(ProxyGenerationCommand::Prepare {
                item: Box::new(item),
                generation,
                cluster_config_digest,
                response: response_tx,
            })
            .await
            .map_err(|_| "stable proxy listener is not running".to_string())?;
        response_rx
            .await
            .map_err(|_| "stable proxy listener stopped before preparing generation".to_string())?
    }

    /// 业务作用: 发布已经由同一稳定监听器预构造完成的运行代。
    ///
    /// 参数说明:
    /// - `generation`: 必须与此前成功 prepare 的候选代次完全一致。
    ///
    /// 返回: 候选代原子替换当前代后返回成功；候选不存在或代次不符时保持当前代。
    pub async fn commit(&self, generation: u64) -> Result<(), String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.updates
            .send(ProxyGenerationCommand::Commit {
                generation,
                response: response_tx,
            })
            .await
            .map_err(|_| "stable proxy listener is not running".to_string())?;
        response_rx
            .await
            .map_err(|_| "stable proxy listener stopped before committing generation".to_string())?
    }

    /// 业务作用: 丢弃尚未发布的稳定监听器候选运行代。
    ///
    /// 参数说明:
    /// - `generation`: 必须与需要清理的已 prepare 候选代次完全一致。
    ///
    /// 返回: 对应候选不存在时也按幂等成功处理；存在其他代次候选时返回错误。
    pub async fn abort(&self, generation: u64) -> Result<(), String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.updates
            .send(ProxyGenerationCommand::Abort {
                generation,
                response: response_tx,
            })
            .await
            .map_err(|_| "stable proxy listener is not running".to_string())?;
        response_rx
            .await
            .map_err(|_| "stable proxy listener stopped before aborting generation".to_string())?
    }
}

/// 稳定监听器串行处理的运行代事务命令。
pub enum ProxyGenerationCommand {
    /// 构造候选资源并暂存，不改变当前活动运行代。
    Prepare {
        /// 已完成静态校验和后端预检的候选代理配置。
        item: Box<ProxyItem>,
        /// 本次候选的单调递增运行代次。
        generation: u64,
        /// 候选运行代所依据的全局集群配置摘要。
        cluster_config_digest: Option<String>,
        /// 返回准备是否成功的单次响应通道。
        response: oneshot::Sender<Result<(), String>>,
    },
    /// 原子发布此前准备完成的候选运行代。
    Commit {
        /// 必须与暂存候选一致的运行代次。
        generation: u64,
        /// 返回发布是否成功的单次响应通道。
        response: oneshot::Sender<Result<(), String>>,
    },
    /// 清理此前准备完成但不再提交的候选运行代。
    Abort {
        /// 必须与暂存候选一致的运行代次。
        generation: u64,
        /// 返回清理是否成功的单次响应通道。
        response: oneshot::Sender<Result<(), String>>,
    },
}

/// 业务作用: 创建稳定监听器的运行代控制通道。
///
/// 返回: 运行时 supervisor 使用的控制句柄和监听任务独占的请求接收端。
pub fn proxy_generation_channel() -> (
    ProxyGenerationController,
    mpsc::Receiver<ProxyGenerationCommand>,
) {
    let (updates, receiver) = mpsc::channel(1);
    (ProxyGenerationController { updates }, receiver)
}

/// 表示已经完成模式运行时构造、可以发布给稳定 listener 的代理运行代。
struct PreparedProxyGeneration {
    item: Arc<ProxyItem>,
    runtimes: ProxyRuntimes,
    metrics: ProxyMetrics,
    generation_activity: ProxyGenerationActivity,
    limiter: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
    /// 当前配置代独占的拓扑控制任务；切换代次前必须等待旧任务完全退出，禁止旧配置继续改写后端。
    control_tasks: AsyncMutex<Vec<JoinHandle<()>>>,
    generation: u64,
}

impl PreparedProxyGeneration {
    /// 业务作用: 在不修改当前 listener 和运行代的前提下构造完整候选资源。
    ///
    /// 参数说明:
    /// - `item`: 当前候选代理配置。
    /// - `generation`: 运行时 supervisor 分配的配置代次。
    /// - `metrics`: 同一代理跨代复用的累计指标。
    /// - `coordinator`: 可选的进程级 HA 协调器。
    /// - `cluster_config_digest`: 候选运行代所依据的全局集群配置摘要。
    ///
    /// 返回: Cluster、Sentinel 或 HA 控制面构造失败时返回错误，旧运行代不受影响。
    async fn prepare(
        item: ProxyItem,
        generation: u64,
        metrics: ProxyMetrics,
        coordinator: Option<Arc<CoordinatorHandle>>,
        cluster_config_digest: Option<String>,
    ) -> anyhow::Result<Self> {
        let cluster = match item.mode {
            ProxyMode::Cluster => Some(Arc::new(
                ClusterRuntime::from_proxy_item(&item, metrics.clone()).await?,
            )),
            _ => None,
        };
        let standalone = match item.mode {
            ProxyMode::Standalone => Some(Arc::new(StandaloneRuntime::from_proxy_item(
                &item,
                metrics.clone(),
            )?)),
            _ => None,
        };
        let replication_ha = match item.mode {
            ProxyMode::ReplicationHa => Some(Arc::new(
                ReplicationHaRuntime::from_proxy_item_with_coordinator(
                    &item,
                    coordinator,
                    cluster_config_digest,
                )
                .await?,
            )),
            _ => None,
        };
        let sentinel = match item.mode {
            ProxyMode::Sentinel => Some(Arc::new(SentinelRuntime::from_proxy_item(&item)?)),
            _ => None,
        };
        let sentinel_data = sentinel
            .as_ref()
            .map(|_| Arc::new(SentinelDataRuntime::new(metrics.clone())));
        let limiter = Arc::new(Semaphore::new(item.max_connections));
        let tunnel_limiter = Arc::new(DedicatedTunnelLimiter::from_item(&item));
        let (shutdown, _) = watch::channel(false);
        Ok(Self {
            item: Arc::new(item),
            runtimes: ProxyRuntimes {
                cluster,
                standalone,
                replication_ha,
                sentinel,
                sentinel_data,
                tunnel_limiter,
            },
            metrics,
            generation_activity: ProxyGenerationActivity::default(),
            limiter,
            shutdown,
            control_tasks: AsyncMutex::new(Vec::new()),
            generation,
        })
    }

    /// 业务作用: 发布运行代的管理注册信息并启动该代独占的拓扑后台任务。
    ///
    /// 参数说明:
    /// - `registry`: 可选管理面运行时注册表。
    async fn activate(&self, registry: Option<RuntimeRegistry>) {
        if let Some(registry) = registry {
            registry
                .register_proxy(proxy_registration(
                    &self.item,
                    self.generation,
                    self.generation_activity.clone(),
                    self.runtimes.replication_ha.clone(),
                    self.runtimes.sentinel.clone(),
                    self.metrics.clone(),
                ))
                .await;
        }
        let mut control_tasks = self.control_tasks.lock().await;
        if let Some(runtime) = &self.runtimes.replication_ha {
            control_tasks.push(tokio::spawn(
                runtime.clone().watch(self.shutdown.subscribe()),
            ));
        }
        if let Some(runtime) = &self.runtimes.sentinel {
            control_tasks.push(tokio::spawn(
                runtime.clone().watch(self.shutdown.subscribe()),
            ));
        }
        if let Some(runtime) = &self.runtimes.cluster {
            control_tasks.push(tokio::spawn(watch_cluster_topology(
                self.item.name.clone(),
                runtime.clone(),
                Duration::from_millis(self.item.cluster_topology_refresh_ms),
                self.shutdown.subscribe(),
            )));
        }
        drop(control_tasks);
        log_proxy_generation_started(&self.item, self.generation);
    }

    /// 业务作用: 通知当前运行代的后台任务和全部旧连接退出，并把 HA 运行时标记为已退休。
    ///
    /// 退休标记覆盖管理接口仍持有旧运行时句柄的在途操作：后续任何控制步骤都会被拒绝，
    /// 不能在新代发布后继续按旧配置改写后端。
    fn stop(&self) {
        if let Some(runtime) = &self.runtimes.replication_ha {
            runtime.mark_retired();
        }
        self.shutdown.send_replace(true);
    }

    /// 业务作用: 停止当前配置代并等待全部后端控制任务退出，形成代次之间的后端副作用屏障。
    ///
    /// 返回: HA、Sentinel 和 Cluster 拓扑任务均确认退出后返回。
    async fn stop_and_wait_control_tasks(&self) {
        self.stop();
        let tasks = {
            let mut control_tasks = self.control_tasks.lock().await;
            std::mem::take(&mut *control_tasks)
        };
        for task in tasks {
            if let Err(error) = task.await {
                warn!(
                    proxy = %self.item.name,
                    generation = self.generation,
                    error = %error,
                    "proxy generation control task stopped with join error"
                );
            }
        }
    }
}

/// 业务作用: 记录一个新运行代的认证风险和稳定发布信息。
///
/// 参数说明:
/// - `item`: 已发布的代理配置。
/// - `generation`: 当前代理的配置运行代次。
fn log_proxy_generation_started(item: &ProxyItem, generation: u64) {
    let bind = item.bind_addr();
    let frontend_auth_mode = item.effective_auth_mode();
    if item.allow_unauthenticated {
        warn!(
            proxy = %item.name,
            listen = %bind,
            mode = ?item.mode,
            auth_mode = ?frontend_auth_mode,
            generation,
            "frontend Redis authentication is disabled"
        );
    }
    info!(
        proxy = %item.name,
        listen = %bind,
        backend_profile = item.backend_profile.as_str(),
        mode = ?item.mode,
        auth_mode = ?frontend_auth_mode,
        allow_unauthenticated = item.allow_unauthenticated,
        read_only = item.read_only,
        generation,
        "proxy generation started"
    );
}

/// 业务作用: 启动代理监听器并把进程级协调器注入 replication HA 控制面，其它代理模式保持原数据面构造路径。
/// 参数说明:
/// - `item`: 当前代理配置项。
/// - `shutdown`: 主进程关闭信号。
/// - `registry`: 管理面运行时注册表。
/// - `coordinator`: 可选的进程级协调器共享句柄。
/// - `cluster_config_digest`: 当前运行代所依据的全局集群配置摘要。
///
/// 返回: 返回监听器完整生命周期结果；协调器缺失或运行时构造失败时不会开放业务端口。
pub async fn serve_listener_with_registry_and_coordinator(
    item: ProxyItem,
    shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
    coordinator: Option<Arc<CoordinatorHandle>>,
    cluster_config_digest: Option<String>,
) -> anyhow::Result<()> {
    let (_publication_tx, publication_guard) = watch::channel(true);
    let (_activation_tx, activation) = watch::channel(true);
    let (ready, _ready_rx) = oneshot::channel();
    let (activated, _activated_rx) = oneshot::channel();
    let (listener_closed, _listener_closed_rx) = oneshot::channel();
    serve_listener_with_registry_coordinator_and_publication_guard(
        item,
        1,
        shutdown,
        registry,
        coordinator,
        cluster_config_digest,
        publication_guard,
        activation,
        ready,
        activated,
        listener_closed,
    )
    .await
}

/// 业务作用: 启动带进程级配置发布门禁的代理监听器。
///
/// 参数说明:
/// - `item`: 当前代理配置项。
/// - `generation`: 当前代理名称下由 supervisor 分配的配置运行代编号。
/// - `shutdown`: 当前代理槽位的关闭信号。
/// - `registry`: 可选管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `cluster_config_digest`: 当前运行代所依据的全局集群配置摘要。
/// - `publication_guard`: 多代理配置事务在发布窗口内统一暂停新连接的进程级门禁。
/// - `activation`: 当前 listener 完成绑定后是否允许激活运行时并接受业务连接。
/// - `ready`: 全部 listener 成功绑定后的单次就绪通知。
/// - `activated`: 运行时注册和接收任务均已安装后的单次发布完成通知。
/// - `listener_closed`: 全部 listener 已释放端口、旧连接可继续后台排空的单次通知。
///
/// 返回: 监听器完整生命周期结果；发布门禁关闭时已监听端口只拒绝新连接。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_listener_with_registry_coordinator_and_publication_guard(
    item: ProxyItem,
    generation: u64,
    shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
    coordinator: Option<Arc<CoordinatorHandle>>,
    cluster_config_digest: Option<String>,
    publication_guard: watch::Receiver<bool>,
    activation: watch::Receiver<bool>,
    ready: oneshot::Sender<()>,
    activated: oneshot::Sender<()>,
    listener_closed: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    if item.mode == ProxyMode::ClusterNodeProxy {
        return serve_cluster_node_proxy_listeners(
            item,
            generation,
            shutdown,
            registry,
            coordinator,
            publication_guard,
            activation,
            ready,
            activated,
            listener_closed,
        )
        .await;
    }
    let (_controller, updates) = proxy_generation_channel();
    serve_reloadable_listener_with_registry_coordinator_and_publication_guard(
        item,
        generation,
        shutdown,
        registry,
        coordinator,
        cluster_config_digest,
        updates,
        publication_guard,
        activation,
        ready,
        activated,
        listener_closed,
    )
    .await
}

/// 业务作用: 启动稳定业务监听器，并在不重绑端口的情况下原子切换代理运行代。
///
/// 参数说明:
/// - `item`: 首个活动代理配置。
/// - `generation`: 首个活动配置代次。
/// - `shutdown`: 当前代理槽位的关闭信号。
/// - `registry`: 可选管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `cluster_config_digest`: 首个运行代所依据的全局集群配置摘要。
/// - `updates`: 后续同监听地址运行代替换请求。
///
/// 返回: 监听器完整生命周期结果；候选代构造失败只回复请求，不停止当前代。
#[allow(clippy::too_many_arguments)]
pub async fn serve_reloadable_listener_with_registry_and_coordinator(
    item: ProxyItem,
    generation: u64,
    shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
    coordinator: Option<Arc<CoordinatorHandle>>,
    cluster_config_digest: Option<String>,
    updates: mpsc::Receiver<ProxyGenerationCommand>,
) -> anyhow::Result<()> {
    let (_publication_tx, publication_guard) = watch::channel(true);
    let (_activation_tx, activation) = watch::channel(true);
    let (ready, _ready_rx) = oneshot::channel();
    let (activated, _activated_rx) = oneshot::channel();
    let (listener_closed, _listener_closed_rx) = oneshot::channel();
    serve_reloadable_listener_with_registry_coordinator_and_publication_guard(
        item,
        generation,
        shutdown,
        registry,
        coordinator,
        cluster_config_digest,
        updates,
        publication_guard,
        activation,
        ready,
        activated,
        listener_closed,
    )
    .await
}

/// 业务作用: 启动可热切代且受进程级配置发布门禁保护的稳定业务监听器。
///
/// 参数说明:
/// - `item`: 首个活动代理配置。
/// - `generation`: 首个活动配置代次。
/// - `shutdown`: 当前代理槽位的关闭信号。
/// - `registry`: 可选管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `cluster_config_digest`: 首个运行代所依据的全局集群配置摘要。
/// - `updates`: 后续同监听地址运行代替换请求。
/// - `publication_guard`: 多代理配置事务在发布窗口内统一暂停新连接的进程级门禁。
/// - `activation`: 当前 listener 完成绑定后是否允许激活运行时并接受业务连接。
/// - `ready`: listener 成功绑定后的单次就绪通知。
/// - `activated`: 运行时注册完成并即将进入 accept 循环的单次通知。
/// - `listener_closed`: listener 已释放端口、旧连接可继续后台排空的单次通知。
///
/// 返回: 监听器完整生命周期结果；候选代构造失败只回复请求，不停止当前代。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_reloadable_listener_with_registry_coordinator_and_publication_guard(
    item: ProxyItem,
    generation: u64,
    mut shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
    coordinator: Option<Arc<CoordinatorHandle>>,
    cluster_config_digest: Option<String>,
    mut updates: mpsc::Receiver<ProxyGenerationCommand>,
    publication_guard: watch::Receiver<bool>,
    mut activation: watch::Receiver<bool>,
    ready: oneshot::Sender<()>,
    activated: oneshot::Sender<()>,
    listener_closed: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    if item.mode == ProxyMode::ClusterNodeProxy {
        bail!("cluster-node-proxy requires its dedicated multi-listener runtime");
    }
    let metrics = ProxyMetrics::default();
    let initial = PreparedProxyGeneration::prepare(
        item,
        generation,
        metrics,
        coordinator.clone(),
        cluster_config_digest,
    )
    .await?;
    let bind = initial.item.bind_addr();
    let listener = TcpListener::bind(&bind)
        .await
        .with_context(|| format!("proxy `{}` bind `{bind}`", initial.item.name))?;
    let _ = ready.send(());
    if !wait_for_proxy_activation(&[&listener], &mut activation, &mut shutdown).await {
        initial.stop();
        drop(listener);
        let _ = listener_closed.send(());
        return Ok(());
    }
    initial.activate(registry.clone()).await;
    let _ = activated.send(());
    let mut current = Arc::new(initial);
    let mut prepared: Option<PreparedProxyGeneration> = None;
    let mut updates_open = true;

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!(proxy = %current.item.name, "proxy listener stopping");
                current.stop_and_wait_control_tasks().await;
                break;
            }
            update = updates.recv(), if updates_open => {
                let Some(update) = update else {
                    updates_open = false;
                    continue;
                };
                match update {
                    ProxyGenerationCommand::Prepare {
                        item,
                        generation,
                        cluster_config_digest,
                        response,
                    } => {
                        if prepared.is_some() {
                            let _ = response.send(Err(
                                "stable proxy listener already has a prepared generation"
                                    .to_string(),
                            ));
                            continue;
                        }
                        if item.name != current.item.name
                            || item.bind_addr() != bind
                            || item.mode == ProxyMode::ClusterNodeProxy
                        {
                            let _ = response.send(Err(
                                "stable generation update requires the same proxy name and listener address"
                                    .to_string(),
                            ));
                            continue;
                        }
                        match PreparedProxyGeneration::prepare(
                            *item,
                            generation,
                            current.metrics.clone(),
                            coordinator.clone(),
                            cluster_config_digest,
                        )
                        .await
                        {
                            Ok(next) => {
                                prepared = Some(next);
                                let _ = response.send(Ok(()));
                            }
                            Err(error) => {
                                let _ = response.send(Err(format!("{error:#}")));
                            }
                        }
                    }
                    ProxyGenerationCommand::Commit {
                        generation,
                        response,
                    } => {
                        let Some(next) = prepared.take() else {
                            let _ = response.send(Err(
                                "stable proxy listener has no prepared generation".to_string(),
                            ));
                            continue;
                        };
                        if next.generation != generation {
                            prepared = Some(next);
                            let _ = response.send(Err(
                                "stable proxy listener prepared generation does not match commit"
                                    .to_string(),
                            ));
                            continue;
                        }
                        // 发布门禁已经暂停新连接。先等待旧代控制任务完全退出，再激活候选，
                        // 防止旧 HA 持久化收敛在新代发布后把后端恢复成旧配置。
                        current.stop_and_wait_control_tasks().await;
                        next.activate(registry.clone()).await;
                        current = Arc::new(next);
                        let _ = response.send(Ok(()));
                    }
                    ProxyGenerationCommand::Abort {
                        generation,
                        response,
                    } => {
                        let Some(candidate) = prepared.take() else {
                            let _ = response.send(Ok(()));
                            continue;
                        };
                        if candidate.generation != generation {
                            prepared = Some(candidate);
                            let _ = response.send(Err(
                                "stable proxy listener prepared generation does not match abort"
                                    .to_string(),
                            ));
                            continue;
                        }
                        candidate.stop();
                        let _ = response.send(Ok(()));
                    }
                }
            }
            accepted = listener.accept() => {
                let (client, peer) = accepted.with_context(|| {
                    format!("proxy `{}` accept", current.item.name)
                })?;
                let generation = current.clone();
                let cluster_guard =
                    coordinator.as_ref().map(|coordinator| coordinator.cluster_configuration_guard());
                if !*publication_guard.borrow()
                    || cluster_guard
                    .as_ref()
                    .is_some_and(|guard| !*guard.borrow())
                {
                    generation.metrics.connection_rejected();
                    drop(client);
                    continue;
                }
                let Ok(permit) = generation.limiter.clone().try_acquire_owned() else {
                    generation.metrics.connection_rejected();
                    warn!(proxy = %generation.item.name, client = %peer, "connection rejected: max_connections reached");
                    drop(client);
                    continue;
                };
                connections.spawn(async move {
                    let _permit = permit;
                    generation.metrics.connection_accepted();
                    generation.generation_activity.connection_accepted();
                    let result = handle_generation_connection(
                        generation.item.clone(),
                        generation.runtimes.clone(),
                        client,
                        generation.shutdown.subscribe(),
                        generation.metrics.clone(),
                        cluster_guard,
                    )
                    .await;
                    let has_error = result.is_err();
                    generation.generation_activity.connection_finished();
                    generation.metrics.connection_finished(has_error);
                    if let Err(err) = result {
                        debug!(client = %peer, error = %err, "connection closed with error");
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_join(&current.item.name, joined);
            }
        }
    }

    drop(listener);
    let _ = listener_closed.send(());
    drain_connections(&current.item, connections).await;
    Ok(())
}

/// 业务作用: 等待已绑定但尚未发布的 listener 获得运行代激活许可,并即时拒绝等待期的早连接。
///
/// 预绑定 listener 若在等待期不 accept,抢跑客户端会在内核 backlog 完成握手后无限挂起;
/// 这里把早连接 accept 后立即关闭,让对端得到明确失败而不是悬挂。
///
/// 参数说明:
/// - `listeners`: 已完成绑定、尚未对业务发布的候选 listener 集合。
/// - `activation`: 运行时 supervisor 在原子发布点打开的运行代门禁。
/// - `shutdown`: 候选被回滚或进程关闭时优先终止等待的信号。
///
/// 返回: 获得激活许可时返回真；候选先被关闭或信号发送端释放时返回假。
async fn wait_for_proxy_activation(
    listeners: &[&TcpListener],
    activation: &mut watch::Receiver<bool>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    loop {
        if *shutdown.borrow() {
            return false;
        }
        if *activation.borrow() {
            return true;
        }
        // `select_all` 对空集合会 panic,listener 列表为空时退化为纯信号等待。
        if listeners.is_empty() {
            tokio::select! {
                changed = activation.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return false;
                    }
                }
            }
            continue;
        }
        // accept future 可安全取消,每轮循环重建即可;早连接被 accept 后立刻 drop,
        // 对端收到连接关闭而不是停留在半开状态。
        let accept_any = futures_util::future::select_all(
            listeners.iter().map(|listener| Box::pin(listener.accept())),
        );
        tokio::select! {
            changed = activation.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return false;
                }
            }
            (accepted, _, _) = accept_any => {
                match accepted {
                    Ok((early, _)) => drop(early),
                    // accept 持续报错(如描述符耗尽)时避免热循环;短暂退避不影响
                    // 激活与关闭信号在下一轮被及时处理。
                    Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
                }
            }
        }
    }
}

/// 业务作用: 在代理运行代关闭或集群配置版本落后时终止一个前端连接。
///
/// 参数说明:
/// - `item`: 接受连接时原子取得的代理配置运行代。
/// - `runtimes`: 与该配置代绑定的后端和拓扑运行时。
/// - `client`: 已接受、尚未进入 RESP 会话处理的前端连接。
/// - `shutdown`: 当前代理运行代的关闭信号。
/// - `metrics`: 当前代理跨代复用的指标对象。
/// - `cluster_guard`: 集群部署时用于拒绝陈旧配置数据面的版本门禁。
///
/// 返回: 会话正常结束时成功；运行代、门禁或网络失败时返回对应 I/O 错误。
async fn handle_generation_connection(
    item: Arc<ProxyItem>,
    runtimes: ProxyRuntimes,
    client: TcpStream,
    shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    mut cluster_guard: Option<watch::Receiver<bool>>,
) -> anyhow::Result<()> {
    let connection = handle_connection(item, runtimes, client, shutdown, metrics);
    let Some(guard) = cluster_guard.as_mut() else {
        return connection.await;
    };
    if !*guard.borrow() {
        bail!("cluster configuration is not active on this node");
    }
    tokio::pin!(connection);
    tokio::select! {
        result = &mut connection => result,
        _ = wait_for_cluster_configuration_stale(guard) => {
            Err(anyhow::anyhow!("cluster configuration became stale on this node"))
        },
    }
}

/// 业务作用: 等待集群配置服务门禁从活动状态变成关闭状态。
///
/// 参数说明:
/// - `guard`: 当前连接独占的集群配置门禁订阅。
///
/// 返回: 门禁关闭或协调器发送端释放时结束。
async fn wait_for_cluster_configuration_stale(guard: &mut watch::Receiver<bool>) {
    loop {
        if !*guard.borrow() {
            return;
        }
        if guard.changed().await.is_err() {
            return;
        }
    }
}

/// 业务作用: 在不绑定业务监听器、不修改 HA 拓扑的前提下预检一个代理配置。
///
/// 参数说明:
/// - `item`: 已通过全局静态校验的候选代理配置。
///
/// 返回: 对应模式的 TLS、认证、后端角色或槽位拓扑全部可用时返回成功。
pub async fn preflight_proxy_item(item: &ProxyItem) -> anyhow::Result<()> {
    match item.mode {
        ProxyMode::Standalone => preflight_backend_ping(item, &item.backend_addr()?).await,
        ProxyMode::Sentinel => {
            let runtime = SentinelRuntime::from_proxy_item(item)?;
            runtime
                .refresh()
                .await
                .with_context(|| format!("proxy `{}` sentinel preflight", item.name))
        }
        ProxyMode::Cluster => {
            ClusterRuntime::from_proxy_item(item, ProxyMetrics::default())
                .await
                .with_context(|| {
                    format!("proxy `{}` cluster slot topology preflight", item.name)
                })?;
            Ok(())
        }
        ProxyMode::ClusterNodeProxy => {
            ClusterNodeProxyRuntime::from_proxy_item(item, ProxyMetrics::default())?;
            for node in item.cluster_node_proxies()? {
                preflight_backend_ping(item, &node.backend)
                    .await
                    .with_context(|| {
                        format!(
                            "proxy `{}` cluster node `{}` preflight",
                            item.name, node.backend
                        )
                    })?;
            }
            Ok(())
        }
        ProxyMode::ReplicationHa => preflight_replication_ha(item).await,
    }
}

/// 业务作用: 建立一次带认证和可选 TLS 的后端连接并执行 `PING`。
///
/// 参数说明:
/// - `item`: 提供认证、TLS、超时和 keepalive 的候选代理配置。
/// - `backend`: 本次要验证的真实后端 `host:port` 地址。
///
/// 返回: 建连、认证和 `PING` 都成功时返回成功。
async fn preflight_backend_ping(item: &ProxyItem, backend: &str) -> anyhow::Result<()> {
    let tls = BackendTlsConfig::for_backend(item);
    let mut connection = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: backend,
        username: item.username.as_deref(),
        password: item.password.as_deref(),
        connect_timeout_ms: item.connect_timeout_ms,
        request_timeout_ms: item.backend_request_timeout_ms,
        tcp_keepalive_ms: item.tcp_keepalive_ms,
        response_bytes_limit: Some(item.frame_max_bytes),
        metrics: None,
        tls: tls.as_ref(),
    })
    .await?;
    let response = connection
        .send_pipeline(&[resp::array(&[b"PING".as_slice()])])
        .await?
        .into_iter()
        .next()
        .context("backend PING returned no response")?;
    if response.starts_with(b"-") {
        bail!("proxy `{}` backend `{backend}` rejected PING", item.name);
    }
    Ok(())
}

/// 业务作用: 只读探测托管复制 HA 的全部节点并验证唯一主节点和副本跟随关系。
///
/// 参数说明:
/// - `item`: 提供后端产品、节点、认证、TLS 和复制延迟门禁的候选配置。
///
/// 返回: 全部节点可探测、存在唯一主节点且所有其他节点健康跟随时返回成功。
async fn preflight_replication_ha(item: &ProxyItem) -> anyhow::Result<()> {
    preflight_replication_ha_with_transition(item, None).await
}

/// 业务作用: 预检托管复制 HA 候选，并允许删主事务在计划切换前短暂表达全副本拓扑。
///
/// 参数说明:
/// - `item`: 提供候选节点、产品、认证、TLS 和复制延迟门禁的代理配置。
/// - `transition_master`: 当前活动运行代确认、但可能已从候选节点集合删除的旧主节点。
///
/// 返回: 候选已达到唯一主终态，或全部候选均健康跟随指定旧主时返回成功。
pub(crate) async fn preflight_replication_ha_transition(
    item: &ProxyItem,
    transition_master: &HaNode,
) -> anyhow::Result<()> {
    preflight_replication_ha_with_transition(item, Some(transition_master)).await
}

/// 业务作用: 执行托管复制 HA 的严格终态或删主过渡态探测。
///
/// 参数说明:
/// - `item`: 提供候选节点、产品、认证、TLS 和复制延迟门禁的代理配置。
/// - `transition_master`: 允许全部候选仍跟随的旧主；普通预检为空。
///
/// 返回: 节点角色、上游和复制延迟满足对应状态约束时返回成功。
async fn preflight_replication_ha_with_transition(
    item: &ProxyItem,
    transition_master: Option<&HaNode>,
) -> anyhow::Result<()> {
    let raw_nodes = item
        .nodes
        .as_deref()
        .with_context(|| format!("proxy `{}` replication-ha mode requires nodes", item.name))?;
    let nodes = parse_ha_nodes(raw_nodes)?;
    let driver = HaBackendDriver::from_proxy_item(item, format!("{}-reload-preflight", item.name))?;
    let mut observations = Vec::with_capacity(nodes.len());
    for node in &nodes {
        let observation = driver.inspect_node(node).await.with_context(|| {
            format!(
                "proxy `{}` inspect HA node `{}`",
                item.name, node.client_addr
            )
        })?;
        observations.push((node, observation));
    }
    let masters = observations
        .iter()
        .filter(|(_, observation)| observation.role == HaRole::Master)
        .collect::<Vec<_>>();
    if masters.is_empty() {
        let transition_master = transition_master.with_context(|| {
            format!(
                "proxy `{}` HA preflight requires exactly one master, observed 0",
                item.name
            )
        })?;
        let max_lag = item.replication_ha_config()?.max_replica_lag;
        for (node, observation) in observations {
            if !observation.eligible_replica_of(transition_master, max_lag) {
                bail!(
                    "proxy `{}` HA transition node `{}` is not a healthy replica of current master `{}`",
                    item.name,
                    node.client_addr,
                    transition_master.client_addr
                );
            }
        }
        return Ok(());
    }
    if masters.len() != 1 {
        bail!(
            "proxy `{}` HA preflight requires exactly one master, observed {}",
            item.name,
            masters.len()
        );
    }
    let master = masters[0].0;
    let max_lag = item.replication_ha_config()?.max_replica_lag;
    for (node, observation) in observations {
        if node == master {
            continue;
        }
        if !observation.eligible_replica_of(master, max_lag) {
            bail!(
                "proxy `{}` HA node `{}` is not a healthy replica of master `{}`",
                item.name,
                node.client_addr,
                master.client_addr
            );
        }
    }
    Ok(())
}

/// 业务作用: 保存节点代理地址改写表、共享 pipeline 池和按后端隔离的专用隧道池。
#[derive(Debug)]
// 表示 ClusterNodeProxyRuntime 的数据结构，用于维护 Redis Cluster 节点级代理的拓扑地址改写表。
struct ClusterNodeProxyRuntime {
    /// 源 Redis Cluster 地址到业务可见代理地址的拓扑改写表。
    rewrite_map: HashMap<String, String>,
    /// 普通无状态命令使用的共享 pipeline 后端池。
    pipeline_pool: Arc<PipelineBackendPool>,
    /// 按源节点地址预创建的后端专属 tunnel pool；连接建立时只查找一次。
    tunnel_pools: HashMap<String, Arc<TunnelPool>>,
}

/// 业务作用: 记录 cluster-node-proxy 一批请求从解析到写回各阶段的耗时、数量与错误。
#[derive(Debug, Default)]
struct NodeProxyStageProfile {
    read_parse_micros: u64,
    classify_micros: u64,
    execute_micros: u64,
    pool_acquire_micros: u64,
    backend_write_micros: u64,
    backend_read_micros: u64,
    ordinary_mux_micros: u64,
    rewrite_micros: u64,
    limit_micros: u64,
    writeback_micros: u64,
    command_count: usize,
    response_bytes: usize,
    error_count: usize,
    execution_error: bool,
}

/// 业务作用: 组合普通 mux 与 pipeline pool 的后端阶段耗时，供节点代理 profile 汇总。
#[derive(Debug, Clone, Copy, Default)]
struct NodeProxyBackendProfile {
    ordinary_mux_micros: u64,
    pipeline_pool: PipelinePoolProfile,
}

impl ClusterNodeProxyRuntime {
    /// 业务作用: 为 cluster-node-proxy 构造地址改写表、节点连接池和独立隧道池。
    /// 参数说明:
    /// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    fn from_proxy_item(item: &ProxyItem, metrics: ProxyMetrics) -> anyhow::Result<Self> {
        let mut rewrite_map = HashMap::new();
        let mut tunnel_pools = HashMap::new();
        for node in item.cluster_node_proxies()? {
            let advertised = normalize_addr(&node.advertise_addr(item)?)?;
            rewrite_map.insert(normalize_addr(&node.backend)?, advertised.clone());
            for alias in &node.aliases {
                rewrite_map.insert(normalize_addr(alias)?, advertised.clone());
            }
            tunnel_pools.entry(node.backend.clone()).or_insert_with(|| {
                TunnelPool::new(
                    item.dedicated_tunnel_pool_max_idle,
                    item.dedicated_tunnel_pool_idle_timeout_ms,
                    metrics.clone(),
                )
            });
        }
        Ok(Self {
            rewrite_map,
            pipeline_pool: Arc::new(PipelineBackendPool::new(item, metrics)),
            tunnel_pools,
        })
    }

    /// 业务作用: 获取指定源节点在启动阶段已经创建的 tunnel pool。
    ///
    /// 参数说明:
    /// - `backend`: 当前 listener 固定对应的源 Redis 节点地址。
    ///
    /// 返回:
    /// - 成功时返回后端专属 pool 的共享句柄。
    /// - 地址未在启动拓扑中登记时返回明确错误，禁止运行时临时创建无配置池。
    fn tunnel_pool_for(&self, backend: &str) -> anyhow::Result<Arc<TunnelPool>> {
        self.tunnel_pools
            .get(backend)
            .cloned()
            .with_context(|| format!("missing tunnel pool for cluster backend `{backend}`"))
    }

    /// 业务作用: 改写 pipeline 响应中的 Redis Cluster 拓扑地址和 MOVED/ASK 地址，同时保持响应顺序不变。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    /// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
    ///
    // 改写 pipeline 响应中的 Redis Cluster 拓扑地址和 MOVED/ASK 地址，同时保持响应顺序不变。
    fn rewrite_pipeline_responses(&self, commands: &[ClientCommand], responses: &mut [Vec<u8>]) {
        for (command, response) in commands.iter().zip(responses.iter_mut()) {
            self.rewrite_response(command_rewrite_kind(command), response);
        }
    }

    /// 业务作用: 改写后端响应批次；Joined 批次只有在确实包含拓扑命令或错误响应时才拆帧检查。 has_topology_command 来自 classify_node_proxy_batch 的单遍扫描,这里不再逐命令重扫。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    /// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
    /// - `has_topology_command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    // 改写后端响应批次；Joined 批次只有在确实包含拓扑命令或错误响应时才拆帧检查。
    // has_topology_command 来自 classify_node_proxy_batch 的单遍扫描,这里不再逐命令重扫。
    fn rewrite_response_batch(
        &self,
        commands: &[ClientCommand],
        responses: BackendResponseBatch,
        has_topology_command: bool,
    ) -> BackendResponseBatch {
        match responses {
            BackendResponseBatch::Frames(mut frames) => {
                self.rewrite_pipeline_responses(commands, &mut frames);
                BackendResponseBatch::Frames(frames)
            }
            BackendResponseBatch::Joined(joined)
                if has_topology_command || joined.error_count > 0 =>
            {
                if !has_topology_command && !joined_has_redirect_error(&joined) {
                    return BackendResponseBatch::Joined(joined);
                }
                let mut frames = joined
                    .frame_ranges
                    .iter()
                    .map(|range| joined.bytes[range.clone()].to_vec())
                    .collect::<Vec<_>>();
                self.rewrite_pipeline_responses(commands, &mut frames);
                BackendResponseBatch::Frames(frames)
            }
            BackendResponseBatch::Joined(joined) => BackendResponseBatch::Joined(joined),
        }
    }

    /// 业务作用: 改写单条响应，用于把源 Redis Cluster 节点地址替换成 proxy 对业务暴露的节点地址。
    /// 参数说明:
    /// - `kind`: 当前值、字段或命令的类别，决定解析规则和后续分支。
    /// - `response`: 准备写回客户端或参与聚合的单个 RESP 响应帧。
    ///
    // 改写单条响应，用于把源 Redis Cluster 节点地址替换成 proxy 对业务暴露的节点地址。
    fn rewrite_response(&self, kind: NodeProxyRewriteKind, response: &mut Vec<u8>) {
        if self.rewrite_redirect_error(response) {
            return;
        }
        match kind {
            NodeProxyRewriteKind::None => {}
            NodeProxyRewriteKind::ClusterNodes => {
                self.rewrite_cluster_nodes_response(response);
            }
            NodeProxyRewriteKind::ClusterSlots | NodeProxyRewriteKind::ClusterShards => {
                self.rewrite_topology_value_response(response);
            }
        }
    }

    /// 业务作用: 改写 MOVED/ASK 错误中的目标节点地址，避免业务侧 Redis Cluster client 直连源内网地址。
    /// 参数说明:
    /// - `response`: 准备写回客户端或参与聚合的单个 RESP 响应帧。
    ///
    // 改写 MOVED/ASK 错误中的目标节点地址，避免业务侧 Redis Cluster client 直连源内网地址。
    fn rewrite_redirect_error(&self, response: &mut Vec<u8>) -> bool {
        if !(response.starts_with(b"-MOVED ") || response.starts_with(b"-ASK ")) {
            return false;
        }
        let Some(rewritten) = self.replace_known_addrs(response) else {
            return false;
        };
        *response = rewritten;
        true
    }

    /// 业务作用: 改写 CLUSTER NODES 返回的文本拓扑。
    /// 参数说明:
    /// - `response`: 准备写回客户端或参与聚合的单个 RESP 响应帧。
    ///
    // 改写 CLUSTER NODES 返回的文本拓扑。
    fn rewrite_cluster_nodes_response(&self, response: &mut Vec<u8>) {
        let Ok(Some((mut value, consumed))) = resp::parse_value(response) else {
            return;
        };
        if consumed != response.len() {
            return;
        }
        if self.rewrite_bulk_or_simple_text(&mut value) {
            *response = resp::encode_value(&value);
        }
    }

    /// 业务作用: 改写 CLUSTER SLOTS / CLUSTER SHARDS 返回的结构化拓扑。
    /// 参数说明:
    /// - `response`: 准备写回客户端或参与聚合的单个 RESP 响应帧。
    ///
    // 改写 CLUSTER SLOTS / CLUSTER SHARDS 返回的结构化拓扑。
    fn rewrite_topology_value_response(&self, response: &mut Vec<u8>) {
        let Ok(Some((mut value, consumed))) = resp::parse_value(response) else {
            return;
        };
        if consumed != response.len() {
            return;
        }
        if self.rewrite_topology_value(&mut value) {
            *response = resp::encode_value(&value);
        }
    }

    /// 业务作用: 递归改写 RESP 拓扑值，覆盖 CLUSTER SLOTS 的节点数组和 CLUSTER SHARDS 的 RESP3 map。
    /// 参数说明:
    /// - `value`: 待写入、解析或比较的业务值。
    ///
    // 递归改写 RESP 拓扑值，覆盖 CLUSTER SLOTS 的节点数组和 CLUSTER SHARDS 的 RESP3 map。
    fn rewrite_topology_value(&self, value: &mut resp::RespValue) -> bool {
        match value {
            resp::RespValue::Array(Some(values)) => {
                let mut changed = self.rewrite_node_array(values);
                changed |= rewrite_node_property_array(&self.rewrite_map, values);
                for child in values {
                    changed |= self.rewrite_topology_value(child);
                }
                changed
            }
            resp::RespValue::Map(Some(values)) => {
                let mut changed = self.rewrite_node_map(values);
                for (key, value) in values {
                    changed |= self.rewrite_topology_value(key);
                    changed |= self.rewrite_topology_value(value);
                }
                changed
            }
            resp::RespValue::Set(Some(values)) | resp::RespValue::Push(values) => {
                let mut changed = false;
                for child in values {
                    changed |= self.rewrite_topology_value(child);
                }
                changed
            }
            _ => false,
        }
    }

    /// 业务作用: 改写形如 [host, port, node-id] 的 Redis Cluster 节点数组。
    /// 参数说明:
    /// - `values`: 待编码、比较或聚合的值集合，处理时保持原始顺序。
    ///
    // 改写形如 [host, port, node-id] 的 Redis Cluster 节点数组。
    fn rewrite_node_array(&self, values: &mut [resp::RespValue]) -> bool {
        if values.len() < 2 {
            return false;
        }
        let Some(host) = resp_string(&values[0]) else {
            return false;
        };
        let Some(port) = resp_integer(&values[1]) else {
            return false;
        };
        let addr = format!("{host}:{port}");
        let Some((advertised_host, advertised_port)) = self.rewrite_addr_parts(&addr) else {
            return false;
        };
        set_resp_string(&mut values[0], advertised_host.as_bytes());
        values[1] = resp::RespValue::Integer(i64::from(advertised_port));
        true
    }

    /// 业务作用: 改写形如 {ip|host: ..., port: ...} 的 Redis Cluster shard/node map。
    /// 参数说明:
    /// - `values`: 待编码、比较或聚合的值集合，处理时保持原始顺序。
    ///
    // 改写形如 {ip|host: ..., port: ...} 的 Redis Cluster shard/node map。
    fn rewrite_node_map(&self, values: &mut [(resp::RespValue, resp::RespValue)]) -> bool {
        let mut port_idx = None;
        for (idx, (key, _)) in values.iter().enumerate() {
            let Some(key) = resp_string(key) else {
                continue;
            };
            if key.eq_ignore_ascii_case("port") {
                port_idx = Some(idx);
            }
        }
        let Some(port_idx) = port_idx else {
            return false;
        };
        let Some(port) = resp_integer(&values[port_idx].1) else {
            return false;
        };
        let mut replacements = Vec::new();
        for (idx, (key, value)) in values.iter().enumerate() {
            let Some(key) = resp_string(key) else {
                continue;
            };
            if !matches!(
                key.to_ascii_lowercase().as_str(),
                "ip" | "host" | "endpoint"
            ) {
                continue;
            }
            let Some(host) = resp_string(value) else {
                continue;
            };
            let addr = format!("{host}:{port}");
            if let Some((advertised_host, advertised_port)) = self.rewrite_addr_parts(&addr) {
                replacements.push((idx, advertised_host, advertised_port));
            }
        }
        let Some((_, _, advertised_port)) = replacements.first() else {
            return false;
        };
        let advertised_port = *advertised_port;
        for (idx, advertised_host, _) in replacements {
            set_resp_string(&mut values[idx].1, advertised_host.as_bytes());
        }
        values[port_idx].1 = resp::RespValue::Integer(i64::from(advertised_port));
        true
    }

    /// 业务作用: 改写 bulk/simple 文本中的已知节点地址。
    /// 参数说明:
    /// - `value`: 待写入、解析或比较的业务值。
    ///
    // 改写 bulk/simple 文本中的已知节点地址。
    fn rewrite_bulk_or_simple_text(&self, value: &mut resp::RespValue) -> bool {
        match value {
            resp::RespValue::Bulk(Some(bytes)) | resp::RespValue::Simple(bytes) => {
                let Some(rewritten) = self.replace_known_addrs(bytes) else {
                    return false;
                };
                *bytes = rewritten;
                true
            }
            _ => false,
        }
    }

    /// 业务作用: 查询地址改写后的 host/port，用于结构化 RESP 拓扑值改写。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    // 查询地址改写后的 host/port，用于结构化 RESP 拓扑值改写。
    fn rewrite_addr_parts(&self, addr: &str) -> Option<(String, u16)> {
        let normalized = normalize_addr(addr).ok()?;
        let advertised = self.rewrite_map.get(&normalized)?;
        split_normalized_addr(advertised)
    }

    /// 业务作用: 替换文本响应中的已知节点地址，主要服务 CLUSTER NODES 与 MOVED/ASK。
    /// 参数说明:
    /// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
    ///
    // 替换文本响应中的已知节点地址，主要服务 CLUSTER NODES 与 MOVED/ASK。
    fn replace_known_addrs(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let mut text = String::from_utf8(bytes.to_vec()).ok()?;
        let original = text.clone();
        let mut rewrite_entries = self.rewrite_map.iter().collect::<Vec<_>>();
        rewrite_entries.sort_by_key(|(source, _)| std::cmp::Reverse(source.len()));
        for (source, advertised) in rewrite_entries {
            text = text.replace(source, advertised);
        }
        (text != original).then(|| text.into_bytes())
    }
}

/// 业务作用: 快速扫描 joined 响应索引，判断是否至少包含一个 MOVED 或 ASK 错误。
/// 参数说明:
/// - `joined`: 已等待完成的后台任务数量，用于确认所有监听器都已退出。
///
fn joined_has_redirect_error(joined: &BackendRawResponses) -> bool {
    joined.frame_ranges.iter().any(|range| {
        joined
            .bytes
            .get(range.clone())
            .is_some_and(|frame| frame.starts_with(b"-MOVED ") || frame.starts_with(b"-ASK "))
    })
}

/// 业务作用: 改写 RESP2 形态的 `CLUSTER SHARDS` 节点；其字段使用
/// `[key, value, key, value, ...]` 交替数组编码，而不是 RESP3 map。
/// 参数说明:
/// - `rewrite_map`: 源集群节点地址到代理公布地址的映射。
/// - `values`: 待原位改写的节点属性交替数组。
fn rewrite_node_property_array(
    rewrite_map: &HashMap<String, String>,
    values: &mut [resp::RespValue],
) -> bool {
    if values.len() < 4 || !values.len().is_multiple_of(2) {
        return false;
    }
    let mut port_value_idx = None;
    for key_idx in (0..values.len()).step_by(2) {
        if resp_string(&values[key_idx]).is_some_and(|key| key.eq_ignore_ascii_case("port")) {
            port_value_idx = Some(key_idx + 1);
            break;
        }
    }
    let Some(port_value_idx) = port_value_idx else {
        return false;
    };
    let Some(port) = resp_integer(&values[port_value_idx]) else {
        return false;
    };

    let mut replacements = Vec::new();
    for key_idx in (0..values.len()).step_by(2) {
        let Some(key) = resp_string(&values[key_idx]) else {
            continue;
        };
        if !matches!(
            key.to_ascii_lowercase().as_str(),
            "ip" | "host" | "endpoint"
        ) {
            continue;
        }
        let value_idx = key_idx + 1;
        let Some(host) = resp_string(&values[value_idx]) else {
            continue;
        };
        let addr = format!("{host}:{port}");
        let Some(normalized) = normalize_addr(&addr).ok() else {
            continue;
        };
        let Some(advertised) = rewrite_map.get(&normalized) else {
            continue;
        };
        let Some((advertised_host, advertised_port)) = split_normalized_addr(advertised) else {
            continue;
        };
        replacements.push((value_idx, advertised_host, advertised_port));
    }
    let Some((_, _, advertised_port)) = replacements.first() else {
        return false;
    };
    let advertised_port = *advertised_port;
    for (value_idx, advertised_host, _) in replacements {
        set_resp_string(&mut values[value_idx], advertised_host.as_bytes());
    }
    values[port_value_idx] = resp::RespValue::Integer(i64::from(advertised_port));
    true
}

/// 业务作用: 标记无需改写、重定向文本、CLUSTER NODES 或结构化拓扑四类后端响应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeProxyRewriteKind {
    None,
    ClusterSlots,
    ClusterNodes,
    ClusterShards,
}

/// 业务作用: 判断当前命令对应的响应是否需要 Redis Cluster 拓扑地址改写。
/// 参数说明:
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
// 判断当前命令对应的响应是否需要 Redis Cluster 拓扑地址改写。
fn command_rewrite_kind(command: &ClientCommand) -> NodeProxyRewriteKind {
    let Some(name) = command.arg(0) else {
        return NodeProxyRewriteKind::None;
    };
    if !name.eq_ignore_ascii_case(b"CLUSTER") {
        return NodeProxyRewriteKind::None;
    }
    let Some(subcommand) = command.arg(1) else {
        return NodeProxyRewriteKind::None;
    };
    if subcommand.eq_ignore_ascii_case(b"SLOTS") {
        NodeProxyRewriteKind::ClusterSlots
    } else if subcommand.eq_ignore_ascii_case(b"NODES") {
        NodeProxyRewriteKind::ClusterNodes
    } else if subcommand.eq_ignore_ascii_case(b"SHARDS") {
        NodeProxyRewriteKind::ClusterShards
    } else {
        NodeProxyRewriteKind::None
    }
}

/// 业务作用: 比较命令名，用于避免在高频 pipeline 路径上构造 args Vec 和大写 String。
/// 参数说明:
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
///
// 比较命令名，用于避免在高频 pipeline 路径上构造 args Vec 和大写 String。
fn command_name_eq(command: &ClientCommand, name: &[u8]) -> bool {
    command
        .arg(0)
        .is_some_and(|value| value.eq_ignore_ascii_case(name))
}

/// 业务作用: 返回 pipeline 中首个 QUIT 命令位置，供本地终止连接且忽略其后命令。
/// 参数说明:
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
fn first_quit_idx(commands: &[ClientCommand]) -> Option<usize> {
    commands
        .iter()
        .position(|command| command_name_eq(command, b"QUIT"))
}

/// 业务作用: 在任何后端路由或透明隧道建立前，对整批客户端命令执行统一只读门禁，避免同一 pipeline 出现部分读命令已执行、危险命令才被拒绝的半执行状态。
/// 参数说明:
/// - `item`: 当前代理配置项，函数只在其 `read_only` 开关启用时实施门禁，并用代理名构造可定位的错误。
/// - `commands`: 已完整解析且尚未转发的当前批次命令，必须保留客户端原始顺序。
///
/// 返回: 关闭只读或整批均可证明安全时返回 `None` 让调用方继续；否则按命令数量返回等长 `READONLY` 响应并阻止整批转发。
fn read_only_batch_response(item: &ProxyItem, commands: &[ClientCommand]) -> Option<Vec<Vec<u8>>> {
    if !item.read_only {
        return None;
    }
    // 只有整批命令都能证明为只读时才继续转发；批次中任一命令不安全就整批失败，避免产生部分执行结果。
    if commands.iter().all(command_allowed_in_read_only) {
        return None;
    }
    Some(vec![read_only_error(&item.name); commands.len()])
}

/// 业务作用: 对单条命令执行关闭式只读分类；只有代理能够从命令名和子命令证明不会执行脚本、修改数据或切换到不可继续检查的连接态时才放行。
/// 参数说明:
/// - `command`: 已解析的客户端命令；空命令、未知命令和参数不足的子命令都按不安全处理。
///
/// 返回: 明确安全的普通读命令、只读元数据查询或本地 `QUIT` 返回 `true`，其余命令返回 `false`。
fn command_allowed_in_read_only(command: &ClientCommand) -> bool {
    // QUIT 只关闭前端连接，不触碰后端数据或运行状态，可以在只读会话中安全放行。
    if command_name_eq(command, b"QUIT") {
        return true;
    }
    // 脚本可能在运行时动态选择写命令，无法仅凭命令名或只读后缀证明安全，因此优先于普通只读表拒绝。
    if command_is_lua_execution(command) {
        return false;
    }
    // 隧道建立后代理不再逐帧分类后续请求，所以任何会切换到连接绑定路径的命令都必须失败关闭。
    if node_proxy_command_requires_tunnel(command) {
        return false;
    }
    command_is_read_only_request_response(command)
}

/// 业务作用: 在 read_only 门禁最前面识别所有 Redis Lua 与 Function 执行入口，确保只读变体也不能运行用户代码。
/// 参数说明:
/// - `command`: 已完成 RESP 解码的客户端命令；函数只检查首个参数的命令名，不执行脚本内容解析。
///
/// 返回: 命令属于任一脚本执行入口时返回 `true`，普通脚本缓存查询或其他命令返回 `false`。
///
/// 设计原因: 脚本内部可以动态调用数据命令，代理无法可靠证明脚本没有副作用；同时 `_RO` 只表示后端的执行约束，不满足本代理“完全禁止脚本执行”的安全策略。
fn command_is_lua_execution(command: &ClientCommand) -> bool {
    command.arg(0).is_some_and(|name| {
        matches_ignore_ascii_case(
            name,
            &[
                b"EVAL",
                b"EVALSHA",
                b"EVAL_RO",
                b"EVALSHA_RO",
                b"FCALL",
                b"FCALL_RO",
            ],
        )
    })
}

/// 业务作用: 维护无需连接绑定的一请求一响应只读白名单，并对带子命令、模块前缀或可选写入参数的命令做二次语义检查。
/// 参数说明:
/// - `command`: 已解析的客户端命令；首参数是命令名，其余参数用于判定子命令和 `STORE` 等写入选项。
///
/// 返回: 命令及其参数组合可以证明只读时返回 `true`；未知命令、写入变体或缺少必要子命令时返回 `false`。
fn command_is_read_only_request_response(command: &ClientCommand) -> bool {
    let Some(name) = command.arg(0) else {
        return false;
    };
    if matches_ignore_ascii_case(
        name,
        &[
            b"PING",
            b"ECHO",
            b"COMMAND",
            b"INFO",
            b"TIME",
            b"DBSIZE",
            b"SCAN",
            b"CLUSTERSCAN",
            b"KEYS",
            b"RANDOMKEY",
            b"TYPE",
            b"EXISTS",
            b"TTL",
            b"PTTL",
            b"GET",
            b"MGET",
            b"STRLEN",
            b"GETRANGE",
            b"GETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITFIELD_RO",
            b"DUMP",
            b"HGET",
            b"HGETALL",
            b"HMGET",
            b"HEXISTS",
            b"HLEN",
            b"HKEYS",
            b"HVALS",
            b"HSTRLEN",
            b"HRANDFIELD",
            b"HSCAN",
            b"LLEN",
            b"LRANGE",
            b"LINDEX",
            b"LPOS",
            b"SCARD",
            b"SISMEMBER",
            b"SMEMBERS",
            b"SMISMEMBER",
            b"SRANDMEMBER",
            b"SSCAN",
            b"SDIFF",
            b"SINTER",
            b"SUNION",
            b"ZCARD",
            b"ZCOUNT",
            b"ZLEXCOUNT",
            b"ZRANGE",
            b"ZRANK",
            b"ZREVRANK",
            b"ZMSCORE",
            b"ZSCORE",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"ZDIFF",
            b"ZINTER",
            b"ZUNION",
            b"GEODIST",
            b"GEOHASH",
            b"GEOPOS",
            b"GEOSEARCH",
            b"PFCOUNT",
            b"XRANGE",
            b"XREVRANGE",
            b"XLEN",
            b"XINFO",
            b"XREAD",
            b"PUBSUB",
            b"OBJECT",
        ],
    ) {
        return true;
    }
    if name.eq_ignore_ascii_case(b"MEMORY") {
        return command_subcommand_matches(
            command,
            &[b"USAGE", b"STATS", b"DOCTOR", b"MALLOC-STATS", b"HELP"],
        );
    }
    if name.eq_ignore_ascii_case(b"CONFIG") {
        return command_subcommand_matches(command, &[b"GET", b"HELP"]);
    }
    if name.eq_ignore_ascii_case(b"SCRIPT") {
        return command_subcommand_matches(command, &[b"EXISTS", b"HELP"]);
    }
    if name.eq_ignore_ascii_case(b"FUNCTION") {
        return command_subcommand_matches(command, &[b"LIST", b"DUMP", b"STATS", b"HELP"]);
    }
    if name.eq_ignore_ascii_case(b"CLUSTER") {
        return command_subcommand_matches(
            command,
            &[
                b"SLOTS",
                b"NODES",
                b"SHARDS",
                b"INFO",
                b"KEYSLOT",
                b"COUNTKEYSINSLOT",
                b"GETKEYSINSLOT",
                b"MYID",
                b"MYSHARDID",
                b"REPLICAS",
                b"SLAVES",
                b"HELP",
            ],
        );
    }
    if name.eq_ignore_ascii_case(b"GEORADIUS") || name.eq_ignore_ascii_case(b"GEORADIUSBYMEMBER") {
        return !command_has_arg(command, b"STORE") && !command_has_arg(command, b"STOREDIST");
    }
    if starts_with_ignore_ascii_case(name, b"JSON.") {
        return json_command_is_read_only(name);
    }
    if starts_with_ignore_ascii_case(name, b"FT.") {
        return search_command_is_read_only(name, command);
    }
    false
}

/// 业务作用: 对带二级动作的 Redis 命令执行大小写无关白名单匹配，防止仅凭主命令名错误放行同族写操作。
/// 参数说明:
/// - `command`: 至少应包含命令名和子命令的已解析命令；缺少第二个参数时关闭式返回失败。
/// - `allowed`: 当前主命令允许在只读模式使用的子命令名集合。
///
/// 返回: 第二个参数命中允许集合时返回 `true`，否则返回 `false`。
fn command_subcommand_matches(command: &ClientCommand, allowed: &[&[u8]]) -> bool {
    command
        .arg(1)
        .is_some_and(|subcommand| matches_ignore_ascii_case(subcommand, allowed))
}

/// 业务作用: 对 JSON 模块命令执行只读白名单判断，未登记的新命令默认拒绝，避免模块升级后新增写入口被自动放行。
/// 参数说明:
/// - `name`: 已标准化比较但保留原字节的完整模块命令名。
///
/// 返回: 命中已确认不修改键值的查询命令时返回 `true`。
fn json_command_is_read_only(name: &[u8]) -> bool {
    matches_ignore_ascii_case(
        name,
        &[
            b"JSON.GET",
            b"JSON.MGET",
            b"JSON.TYPE",
            b"JSON.ARRLEN",
            b"JSON.OBJKEYS",
            b"JSON.OBJLEN",
            b"JSON.STRLEN",
            b"JSON.RESP",
            b"JSON.DEBUG",
        ],
    )
}

/// 业务作用: 对搜索模块命令执行关闭式白名单判断，并继续检查 `FT.CONFIG`、`FT.CURSOR`、`FT.PROFILE` 的二级动作，阻止配置修改和游标删除等写操作。
/// 参数说明:
/// - `name`: 当前搜索模块命令名，用于先匹配纯查询命令族。
/// - `command`: 完整已解析命令，用于读取并验证需要二级语义判断的子命令。
///
/// 返回: 命令及子命令组合已经明确登记为只读时返回 `true`，否则返回 `false`。
fn search_command_is_read_only(name: &[u8], command: &ClientCommand) -> bool {
    if matches_ignore_ascii_case(
        name,
        &[
            b"FT.SEARCH",
            b"FT.AGGREGATE",
            b"FT.INFO",
            b"FT.EXPLAIN",
            b"FT.EXPLAINCLI",
            b"FT.SPELLCHECK",
            b"FT.TAGVALS",
            b"FT.SYNDUMP",
            b"FT._LIST",
        ],
    ) {
        return true;
    }
    if name.eq_ignore_ascii_case(b"FT.CONFIG") {
        return command_subcommand_matches(command, &[b"GET", b"HELP"]);
    }
    if name.eq_ignore_ascii_case(b"FT.CURSOR") {
        return command_subcommand_matches(command, &[b"READ"]);
    }
    if name.eq_ignore_ascii_case(b"FT.PROFILE") {
        return command_subcommand_matches(command, &[b"SEARCH", b"AGGREGATE"]);
    }
    false
}

/// 业务作用: 生成统一的 Redis `READONLY` 协议错误，使各代理模式和 pipeline 拒绝路径使用完全相同的安全语义。
/// 参数说明:
/// - `proxy_name`: 拒绝命令的代理项名称，写入错误文本便于客户端日志定位具体监听器。
///
/// 返回: 可直接写回客户端的完整 RESP 错误帧。
fn read_only_error(proxy_name: &str) -> Vec<u8> {
    format!(
        "-READONLY narix read_only proxy `{proxy_name}` rejects write or script execution commands\r\n"
    )
    .into_bytes()
}

/// 业务作用: 汇总响应帧总字节数，并以溢出保护支持 pending response 限制。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
///
fn response_frames_bytes<C: AsRef<[u8]>>(responses: &[C]) -> usize {
    responses
        .iter()
        .map(|response| response.as_ref().len())
        .sum()
}

/// 业务作用: 把专用隧道满载或排队超时转换为稳定 RESP 错误，并记录对应拒绝指标。
/// 参数说明:
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `reason`: 错误或原因文本，用于保护态、日志和失败上下文。
///
/// 返回: 全部字节按原顺序写出时返回成功；客户端断连或底层写入失败时返回 I/O 错误。
async fn write_dedicated_tunnel_limit_error(
    client: &mut TcpStream,
    item: &ProxyItem,
    metrics: &ProxyMetrics,
    command_count: usize,
    reason: DedicatedTunnelAcquireError,
) -> io::Result<()> {
    let reason_text = match reason {
        DedicatedTunnelAcquireError::Full => "queue full",
        DedicatedTunnelAcquireError::Timeout => "acquire timeout",
        DedicatedTunnelAcquireError::Closed => "limiter closed",
    };
    let responses = vec![
        resp::err(&format!(
            "narix dedicated tunnel limit reached for proxy `{}`: {}",
            item.name, reason_text
        ));
        command_count
    ];
    let _pending_pipeline = metrics.start_pending_pipeline(command_count);
    let bytes_to_write = response_frames_bytes(&responses);
    metrics.record_pipeline_result(command_count, bytes_to_write, command_count, 0, true);
    let _pending = metrics.start_pending_response(bytes_to_write);
    write_response_frames(client, &responses).await
}

/// 业务作用: 把隧道后端建连或执行错误脱敏后写回前端，避免内部连接细节泄漏。
/// 参数说明:
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `elapsed_micros`: 本次操作耗时的微秒值，用于延迟累计、最大值和直方指标。
/// - `message`: 返回给调用方或写入状态的业务消息，不应包含认证凭据。
///
/// 返回: 全部字节按原顺序写出时返回成功；客户端断连或底层写入失败时返回 I/O 错误。
async fn write_tunnel_backend_error(
    client: &mut TcpStream,
    metrics: &ProxyMetrics,
    command_count: usize,
    elapsed_micros: u64,
    message: String,
) -> io::Result<()> {
    let responses = vec![resp::err(&message); command_count];
    let bytes_to_write = response_frames_bytes(&responses);
    metrics.record_pipeline_result(
        command_count,
        bytes_to_write,
        command_count,
        elapsed_micros,
        true,
    );
    let _pending = metrics.start_pending_response(bytes_to_write);
    write_response_frames(client, &responses).await
}

/// 业务作用: 为每个源 Cluster 节点绑定一对一监听，在全部 listener 预检成功后统一激活运行代。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `generation`: 当前配置运行代次，用于隔离并回收热更新前的旧数据面。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `registry`: 进程内代理注册表，用于汇总状态并按名称定位运行时。
/// - `coordinator`: 集群部署时用于关闭陈旧配置数据面的进程级协调器。
/// - `publication_guard`: 多代理事务发布期间统一暂停新连接的进程级门禁。
/// - `activation`: 全部节点 listener 完成绑定后是否允许激活运行时。
/// - `ready`: 全部节点 listener 成功绑定后的单次就绪通知。
/// - `activated`: 注册运行时并安装全部节点 accept 任务后的单次通知。
/// - `listener_closed`: 全部节点 listener 已释放端口后的单次通知。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 先绑定全部子 listener、再一次性通知 ready/activation，避免配置只发布了部分节点入口。
#[allow(clippy::too_many_arguments)]
async fn serve_cluster_node_proxy_listeners(
    item: ProxyItem,
    generation: u64,
    mut shutdown: watch::Receiver<bool>,
    registry: Option<RuntimeRegistry>,
    coordinator: Option<Arc<CoordinatorHandle>>,
    publication_guard: watch::Receiver<bool>,
    mut activation: watch::Receiver<bool>,
    ready: oneshot::Sender<()>,
    activated: oneshot::Sender<()>,
    listener_closed: oneshot::Sender<()>,
) -> anyhow::Result<()> {
    let metrics = ProxyMetrics::default();
    let generation_activity = ProxyGenerationActivity::default();
    let runtime = Arc::new(ClusterNodeProxyRuntime::from_proxy_item(
        &item,
        metrics.clone(),
    )?);
    let nodes = item.cluster_node_proxies()?.to_vec();
    let mut bound = Vec::with_capacity(nodes.len());
    for node in nodes {
        let bind = node.bind_addr(&item);
        let listener = TcpListener::bind(&bind)
            .await
            .with_context(|| format!("proxy `{}` bind cluster node `{bind}`", item.name))?;
        bound.push((node, listener));
    }
    let _ = ready.send(());
    let bound_listeners = bound
        .iter()
        .map(|(_, listener)| listener)
        .collect::<Vec<_>>();
    if !wait_for_proxy_activation(&bound_listeners, &mut activation, &mut shutdown).await {
        drop(bound_listeners);
        drop(bound);
        let _ = listener_closed.send(());
        return Ok(());
    }
    drop(bound_listeners);
    if let Some(registry) = registry {
        registry
            .register_proxy(proxy_registration(
                &item,
                generation,
                generation_activity.clone(),
                None,
                None,
                metrics.clone(),
            ))
            .await;
    }

    let limiter = Arc::new(Semaphore::new(item.max_connections));
    let tunnel_limiter = Arc::new(DedicatedTunnelLimiter::from_item(&item));
    let item = Arc::new(item);
    let mut listeners = JoinSet::new();
    let listener_count = bound.len();
    let (listener_close_tx, mut listener_close_rx) = mpsc::unbounded_channel();

    for (node, listener) in bound {
        let item = item.clone();
        let runtime = runtime.clone();
        let shutdown = shutdown.clone();
        let limiter = limiter.clone();
        let tunnel_limiter = tunnel_limiter.clone();
        let metrics = metrics.clone();
        let generation_activity = generation_activity.clone();
        let cluster_guard = coordinator
            .as_ref()
            .map(|coordinator| coordinator.cluster_configuration_guard());
        let publication_guard = publication_guard.clone();
        let listener_close_tx = listener_close_tx.clone();
        listeners.spawn(async move {
            accept_cluster_node_proxy_listener(
                item,
                node,
                listener,
                runtime,
                shutdown,
                limiter,
                tunnel_limiter,
                metrics,
                generation_activity,
                cluster_guard,
                publication_guard,
                listener_close_tx,
            )
            .await
        });
    }
    let _ = activated.send(());

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!(proxy = %item.name, "cluster-node-proxy listeners stopping");
                break;
            }
            joined = listeners.join_next(), if !listeners.is_empty() => {
                match joined {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(err))) => {
                        listeners.abort_all();
                        return Err(err);
                    }
                    Some(Err(err)) => {
                        listeners.abort_all();
                        return Err(anyhow::anyhow!("cluster-node-proxy listener task failed: {err}"));
                    }
                    None => break,
                }
            }
        }
        if listeners.is_empty() {
            break;
        }
    }

    drop(listener_close_tx);
    for _ in 0..listener_count {
        if listener_close_rx.recv().await.is_none() {
            break;
        }
    }
    let _ = listener_closed.send(());
    while let Some(joined) = listeners.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                warn!(proxy = %item.name, error = %err, "cluster-node-proxy listener stopped with error")
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                warn!(proxy = %item.name, error = %err, "cluster-node-proxy listener join error")
            }
        }
    }
    Ok(())
}

/// 业务作用: 接受单个 Redis Cluster 源节点对应的前端连接，并把连接固定转发到该源节点。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `node`: 当前 listener 对应的 cluster-node-proxy 节点映射配置。
/// - `listener`: 已由父任务绑定完成、等待本节点接受连接的 listener。
/// - `runtime`: 当前代理或 HA 运行时句柄，用于读取状态并执行受控操作。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `limiter`: 共享容量限制器，用于原子申请和释放当前操作占用的许可。
/// - `tunnel_limiter`: 专用隧道容量控制器，用于限制连接绑定命令占用的后端连接。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `generation_activity`: 当前配置运行代独立维护的活跃前端连接计数。
/// - `cluster_guard`: 集群部署时用于拒绝陈旧配置连接的版本门禁。
/// - `publication_guard`: 多代理配置事务在发布窗口内统一暂停新连接的进程级门禁。
/// - `listener_close_tx`: listener 释放端口后通知父任务的无界通道。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 接受单个 Redis Cluster 源节点对应的前端连接，并把连接固定转发到该源节点。
#[allow(clippy::too_many_arguments)]
async fn accept_cluster_node_proxy_listener(
    item: Arc<ProxyItem>,
    node: ClusterNodeProxyItem,
    listener: TcpListener,
    runtime: Arc<ClusterNodeProxyRuntime>,
    mut shutdown: watch::Receiver<bool>,
    limiter: Arc<Semaphore>,
    tunnel_limiter: Arc<DedicatedTunnelLimiter>,
    metrics: ProxyMetrics,
    generation_activity: ProxyGenerationActivity,
    cluster_guard: Option<watch::Receiver<bool>>,
    publication_guard: watch::Receiver<bool>,
    listener_close_tx: mpsc::UnboundedSender<()>,
) -> anyhow::Result<()> {
    let bind = node.bind_addr(&item);
    info!(
        proxy = %item.name,
        listen = %bind,
        backend = %node.backend,
        advertised = %node.advertise_addr(&item)?,
        "cluster-node-proxy listener started"
    );

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                info!(proxy = %item.name, listen = %bind, "cluster-node-proxy listener stopping");
                break;
            }
            accepted = listener.accept() => {
                let (client, peer) = accepted.with_context(|| format!("proxy `{}` accept `{bind}`", item.name))?;
                let connection_guard = cluster_guard.clone();
                if !*publication_guard.borrow()
                    || connection_guard
                    .as_ref()
                    .is_some_and(|guard| !*guard.borrow())
                {
                    metrics.connection_rejected();
                    drop(client);
                    continue;
                }
                let Ok(permit) = limiter.clone().try_acquire_owned() else {
                    metrics.connection_rejected();
                    warn!(proxy = %item.name, listen = %bind, client = %peer, "connection rejected: max_connections reached");
                    drop(client);
                    continue;
                };
                let item = item.clone();
                let node = node.clone();
                let runtime = runtime.clone();
                let shutdown = shutdown.clone();
                let metrics = metrics.clone();
                let generation_activity = generation_activity.clone();
                let tunnel_limiter = tunnel_limiter.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    metrics.connection_accepted();
                    generation_activity.connection_accepted();
                    let connection = handle_cluster_node_proxy_connection(
                        item,
                        node,
                        runtime,
                        client,
                        shutdown,
                        metrics.clone(),
                        tunnel_limiter,
                    );
                    let result = match connection_guard {
                        Some(mut guard) => {
                            tokio::pin!(connection);
                            tokio::select! {
                                result = &mut connection => result,
                                _ = wait_for_cluster_configuration_stale(&mut guard) => Err(
                                    anyhow::anyhow!(
                                        "cluster configuration became stale on this node"
                                    )
                                ),
                            }
                        }
                        None => connection.await,
                    };
                    let has_error = result.is_err();
                    generation_activity.connection_finished();
                    metrics.connection_finished(has_error);
                    if let Err(err) = result {
                        debug!(client = %peer, error = %err, "cluster-node-proxy connection closed with error");
                    }
                });
            }
            joined = connections.join_next(), if !connections.is_empty() => {
                log_connection_join(&item.name, joined);
            }
        }
    }

    drop(listener);
    let _ = listener_close_tx.send(());
    drain_connections(&item, connections).await;
    Ok(())
}

/// 业务作用: 关闭旧运行代新连接入口后，在后台等待既有连接完成或到达优雅超时。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `connections`: 连接数量或连接集合，用于容量限制、并发执行和资源回收。
///
async fn drain_connections(item: &ProxyItem, mut connections: JoinSet<()>) {
    if connections.is_empty() {
        return;
    }
    let timeout = Duration::from_millis(item.graceful_shutdown_timeout_ms);
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    while !connections.is_empty() {
        tokio::select! {
            joined = connections.join_next() => {
                log_connection_join(&item.name, joined);
            }
            _ = &mut deadline => {
                let remaining = connections.len();
                warn!(
                    proxy = %item.name,
                    remaining,
                    timeout_ms = item.graceful_shutdown_timeout_ms,
                    "aborting proxy connections after graceful shutdown timeout"
                );
                connections.abort_all();
                while let Some(joined) = connections.join_next().await {
                    log_connection_join(&item.name, Some(joined));
                }
                return;
            }
        }
    }

    info!(
        proxy = %item.name,
        "all proxy connections drained before shutdown"
    );
}

/// 业务作用: 统一记录前端连接任务结束、取消或异常，避免后台错误静默丢失。
/// 参数说明:
/// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
/// - `joined`: 已等待完成的后台任务数量，用于确认所有监听器都已退出。
///
fn log_connection_join(proxy_name: &str, joined: Option<Result<(), tokio::task::JoinError>>) {
    if let Some(Err(err)) = joined {
        debug!(
            proxy = %proxy_name,
            error = %err,
            "connection task join error"
        );
    }
}

/// 业务作用: 从生效配置和运行时句柄生成管理注册信息。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `generation`: 当前代理名称下由 supervisor 分配的配置运行代编号。
/// - `generation_activity`: 当前运行代独立维护的活跃前端连接计数。
/// - `replication_ha`: 目标 HA 运行时句柄，用于查询拓扑或执行受控运维操作。
/// - `sentinel`: Sentinel 主节点发现和动态安全路由运行时；其他模式为空。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
///
fn proxy_registration(
    item: &ProxyItem,
    generation: u64,
    generation_activity: ProxyGenerationActivity,
    replication_ha: Option<Arc<ReplicationHaRuntime>>,
    sentinel: Option<Arc<SentinelRuntime>>,
    metrics: ProxyMetrics,
) -> ProxyRegistration {
    let listen = item
        .listen_addrs()
        .map(|addrs| addrs.join(","))
        .unwrap_or_else(|_| item.bind_addr());
    ProxyRegistration {
        info: ProxyStaticStatus {
            name: item.name.clone(),
            backend_profile: item.backend_profile.as_str().to_string(),
            mode: mode_name(item.mode).to_string(),
            listen,
            backend: match item.mode {
                ProxyMode::Standalone => item.backend_addr().ok(),
                ProxyMode::Sentinel
                | ProxyMode::Cluster
                | ProxyMode::ClusterNodeProxy
                | ProxyMode::ReplicationHa => None,
            },
            nodes: match item.mode {
                ProxyMode::Standalone => Vec::new(),
                ProxyMode::Sentinel => item.sentinel_node_addrs().unwrap_or_default(),
                ProxyMode::Cluster | ProxyMode::ReplicationHa => {
                    item.node_tokens().unwrap_or_default()
                }
                ProxyMode::ClusterNodeProxy => item
                    .cluster_node_proxies()
                    .map(|nodes| nodes.iter().map(|node| node.backend.clone()).collect())
                    .unwrap_or_default(),
            },
            auth_mode: auth_mode_name(item.effective_auth_mode()).to_string(),
            backend_tls_enabled: item.backend_tls_enabled,
            sentinel_tls_enabled: item.sentinel_tls_enabled,
            connect_timeout_ms: item.connect_timeout_ms,
            backend_request_timeout_ms: item.backend_request_timeout_ms,
            frontend_auth_timeout_ms: item.frontend_auth_timeout_ms,
            client_idle_timeout_ms: item.client_idle_timeout_ms,
            tcp_keepalive_ms: item.tcp_keepalive_ms,
            max_connections: item.max_connections,
            dedicated_tunnel_max_active: item.dedicated_tunnel_max_active,
            dedicated_tunnel_queue_size: item.dedicated_tunnel_queue_size,
            dedicated_tunnel_acquire_timeout_ms: item.dedicated_tunnel_acquire_timeout_ms,
            dedicated_tunnel_pool_max_idle: item.dedicated_tunnel_pool_max_idle,
            dedicated_tunnel_pool_idle_timeout_ms: item.dedicated_tunnel_pool_idle_timeout_ms,
            pipeline_max_commands: item.pipeline_max_commands,
            pipeline_max_bytes: item.pipeline_max_bytes,
            pipeline_backend_max_active: item.pipeline_backend_max_active,
            pipeline_backend_queue_size: item.pipeline_backend_queue_size,
            pipeline_backend_submit_timeout_ms: item.pipeline_backend_submit_timeout_ms,
            frame_max_bytes: item.frame_max_bytes,
            bulk_max_bytes: item.bulk_max_bytes,
            aggregate_max_concurrency: item.aggregate_max_concurrency,
            aggregate_max_items: item.aggregate_max_items,
            pending_response_bytes: item.pending_response_bytes,
            graceful_shutdown_timeout_ms: item.graceful_shutdown_timeout_ms,
            cluster_topology_refresh_ms: item.cluster_topology_refresh_ms,
        },
        generation,
        generation_activity,
        replication_ha,
        sentinel,
        metrics,
    }
}

/// 业务作用: 返回代理模式用于状态、日志和指标标签的稳定名称。
/// 参数说明:
/// - `mode`: 当前代理、认证、协调器或持久化模式，决定合法配置和执行分支。
///
fn mode_name(mode: ProxyMode) -> &'static str {
    match mode {
        ProxyMode::Standalone => "standalone",
        ProxyMode::Sentinel => "sentinel",
        ProxyMode::Cluster => "cluster",
        ProxyMode::ClusterNodeProxy => "cluster-node-proxy",
        ProxyMode::ReplicationHa => "replication-ha",
    }
}

/// 业务作用: 返回前端认证模式用于状态展示的稳定名称。
/// 参数说明:
/// - `mode`: 当前代理、认证、协调器或持久化模式，决定合法配置和执行分支。
///
fn auth_mode_name(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::BackendInject => "backend-inject",
        AuthMode::Terminate => "terminate",
        AuthMode::PassThrough => "pass-through",
    }
}

/// 业务作用: 周期刷新 Redis Cluster 槽位拓扑，成功时原子发布新映射，失败时保留最后可用版本并限频告警。
/// 参数说明:
/// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
/// - `runtime`: 当前代理或 HA 运行时句柄，用于读取状态并执行受控操作。
/// - `refresh_interval`: Cluster 拓扑定时刷新间隔。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
///
// 定时刷新失败保留最后一次完整槽位表，不能发布半成品拓扑让请求随机落错节点。
async fn watch_cluster_topology(
    proxy_name: String,
    runtime: Arc<ClusterRuntime>,
    refresh_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(refresh_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                // 刷新在途时同样响应关闭信号,避免把单次拓扑刷新的剩余时长叠加进切代屏障。
                tokio::select! {
                    refreshed = runtime.refresh_topology() => {
                        if let Err(err) = refreshed {
                            warn!(
                                proxy = %proxy_name,
                                error = %err,
                                "periodic cluster topology refresh failed"
                            );
                        } else {
                            debug!(proxy = %proxy_name, "periodic cluster topology refresh completed");
                        }
                    }
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            debug!(proxy = %proxy_name, "cluster topology watcher stopped");
                            return;
                        }
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    debug!(proxy = %proxy_name, "cluster topology watcher stopped");
                    return;
                }
            }
        }
    }
}

/// 业务作用: 完成前端 socket 配置和认证后，按代理模式把连接分派到固定后端、Sentinel、Cluster 或 replication-ha 数据面。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `runtimes`: 同进程内注册的运行时集合，用于批量启动、关闭或聚合状态。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// terminate 鉴权必须在任何模式分派之前完成；pass-through 则保留原始连接级认证帧。
async fn handle_connection(
    item: Arc<ProxyItem>,
    runtimes: ProxyRuntimes,
    mut client: TcpStream,
    shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    configure_tcp_stream(&client, item.tcp_keepalive_ms, &item.name, "client");

    let mut auth_result = FrontendAuthResult::default();
    if item.effective_auth_mode() == AuthMode::Terminate {
        // 终止鉴权模式必须先消费前端 AUTH，这样后续各模式只处理已认证会话；
        // 如果客户端把 AUTH 和业务命令放在同一个 TCP 包里，leftover 会把未消费字节继续交给数据面。
        auth_result = frontend_auth(&item, &mut client).await?;
    }

    if item.mode == ProxyMode::Cluster {
        // Cluster 模式需要解析 RESP 并按 slot 路由，不能走透明转发；
        // 这里进入专用路径后才能处理 pipeline、MOVED/ASK 改写以及跨节点连接复用。
        let cluster = runtimes.cluster.context("cluster runtime is missing")?;
        return handle_cluster_connection(
            item,
            cluster,
            client,
            auth_result,
            shutdown,
            metrics,
            runtimes.tunnel_limiter,
        )
        .await;
    }

    if item.mode == ProxyMode::Standalone {
        // Standalone 混合模式优先用协议级代理承接普通命令，以获得连接复用和 pipeline 能力；
        // 遇到 Pub/Sub、事务、阻塞读等绑定连接语义时，再由内部逻辑切到隧道路径。
        let standalone = runtimes
            .standalone
            .context("standalone runtime is missing")?;
        return handle_standalone_connection(
            item,
            StandaloneConnectionRuntime {
                runtime: standalone,
                sentinel: None,
                tunnel_limiter: runtimes.tunnel_limiter,
            },
            client,
            auth_result,
            shutdown,
            metrics,
        )
        .await;
    }

    if item.mode == ProxyMode::Sentinel {
        let control = runtimes.sentinel.context("sentinel runtime is missing")?;
        let data = runtimes
            .sentinel_data
            .context("sentinel data runtime is missing")?;
        let routing_rx = control.routing_rx();
        let route = routing_rx.borrow().clone();
        let (master, generation) = match route {
            SentinelRoutingState::Serving {
                master, generation, ..
            } => (master, generation),
            SentinelRoutingState::Resolving { reason }
            | SentinelRoutingState::Protected { reason } => {
                let _ = client
                    .write_all(&resp::err(&format!(
                        "Sentinel proxy `{}` is not ready: {reason}",
                        item.name
                    )))
                    .await;
                control.request_refresh();
                return Ok(());
            }
        };
        let resources = data.resources(&item, &master, generation)?;
        let result = handle_standalone_connection(
            item,
            StandaloneConnectionRuntime {
                runtime: resources,
                sentinel: Some(SentinelSessionContext {
                    control: control.clone(),
                    data,
                    routing_rx,
                    generation,
                }),
                tunnel_limiter: runtimes.tunnel_limiter,
            },
            client,
            auth_result,
            shutdown,
            metrics,
        )
        .await;
        if result.is_err() {
            control.request_refresh();
        }
        return result;
    }

    if item.mode == ProxyMode::ReplicationHa {
        let runtime = runtimes
            .replication_ha
            .context("replication_ha runtime is missing")?;
        let mut routing_rx = runtime.routing_rx();
        // HA 数据面必须先读取控制面的路由状态；Protected 表示当前拓扑不满足安全写入条件，
        // 直接 fail-closed 可以避免把流量打到旧主、未同步节点或正在恢复的节点上。
        let routing_state = routing_rx.borrow_and_update().clone();
        let backend_addr = match routing_state {
            HaRoutingState::Serving { master } => master,
            HaRoutingState::Protected { reason } => {
                let _ = client
                    .write_all(&resp::err(&format!(
                        "replication HA proxy `{}` is protected: {reason}",
                        item.name
                    )))
                    .await;
                return Ok(());
            }
        };
        if item.read_only {
            return handle_replication_ha_readonly_connection(
                item,
                client,
                auth_result,
                shutdown,
                backend_addr,
                routing_rx,
                metrics,
            )
            .await;
        }
        // 写连接携带 routing_rx 进入透明隧道，控制面切主时隧道会主动断开；
        // 这样长连接不会继续向旧 master 写入，客户端重连后会拿到新的安全路由。
        return handle_tcp_relay(
            item,
            client,
            auth_result,
            shutdown,
            backend_addr,
            Some(DynamicRoutingReceiver::ReplicationHa(routing_rx)),
            metrics,
        )
        .await;
    }

    // 其余固定后端模式没有动态拓扑，保持最薄的 TCP relay，降低代理层额外开销。
    let backend_addr = item.backend_addr()?;
    handle_tcp_relay(
        item,
        client,
        auth_result,
        shutdown,
        backend_addr,
        None,
        metrics,
    )
    .await
}

/// 业务作用: 驱动 standalone 与 Sentinel 会话的本地握手、普通 pipeline 和连接绑定命令，并在动态路由换代时切换资源。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `connection_runtime`: 当前路由代次的数据面、可选 Sentinel 会话和独占隧道限制器。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `auth_result`: 前端认证解析结果，决定写回握手响应、拒绝连接或继续读取命令。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 执行 standalone 混合数据面，用于普通命令 mux、pipeline pool 和特殊命令 dedicated tunnel。
async fn handle_standalone_connection(
    item: Arc<ProxyItem>,
    connection_runtime: StandaloneConnectionRuntime,
    mut client: TcpStream,
    auth_result: FrontendAuthResult,
    mut shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let StandaloneConnectionRuntime {
        mut runtime,
        mut sentinel,
        tunnel_limiter,
    } = connection_runtime;
    let mut protocol_version = auth_result.protocol_version;
    let mut buf = auth_result.leftover;
    let mut tmp = vec![0u8; FRONTEND_READ_BUFFER_SIZE];
    let client_idle_timeout = optional_duration_ms(item.client_idle_timeout_ms);
    // backend/protocol 在 session 内不变:握手后解析一次 pool 句柄,热循环不再查表拼 key。
    let mut pool_handle = runtime.pipeline_pool.node_handle(
        &runtime.backend,
        item.backend_profile
            .backend_protocol_version(protocol_version),
    );

    loop {
        let batch = loop {
            let batch = match parse_node_proxy_batch(&mut buf, &item) {
                Ok(batch) => batch,
                Err(err)
                    if should_defer_valkey_protocol_error(
                        item.backend_profile,
                        buf.len(),
                        &err,
                    ) =>
                {
                    NodeProxyBatch {
                        raw: Bytes::new(),
                        commands: Vec::new(),
                        command_spans: Vec::new(),
                    }
                }
                Err(err) => {
                    write_frontend_protocol_error(&mut client, &err).await?;
                    return Ok(());
                }
            };
            if !batch.commands.is_empty() {
                break batch;
            }
            if buf.len() > item.pipeline_max_bytes {
                bail!(
                    "client pipeline buffer exceeds limit: {} > {}",
                    buf.len(),
                    item.pipeline_max_bytes
                );
            }
            if let Some(timeout) = client_idle_timeout {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = tokio::time::sleep(timeout) => {
                        bail!("client idle timeout");
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing standalone connection on shutdown");
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing standalone connection on shutdown");
                        return Ok(());
                    }
                }
            }
        };

        // Sentinel 前端会话可以跨越多次主节点切换存活；每个完整批次只读取一次
        // watch 快照，无变化时不加锁。发现新代次后替换整组 pool handle，已经开始
        // 执行的上一批不重放，下一批只允许进入新主节点资源。
        if let Some(context) = sentinel.as_mut() {
            if let Some(reason) = synchronize_sentinel_session_route(
                &item,
                context,
                &mut runtime,
                &mut pool_handle,
                protocol_version,
            )? {
                client
                    .write_all(&resp::err(&format!(
                        "Sentinel proxy `{}` is not ready: {reason}",
                        item.name
                    )))
                    .await?;
                context.control.request_refresh();
                return Ok(());
            }
        }

        let previous_protocol_version = protocol_version;
        let local_prefix = consume_local_handshake_prefix(
            &item,
            &mut client,
            &batch.commands,
            "standalone",
            &mut protocol_version,
            &metrics,
        )
        .await?;
        // AUTH/SETINFO 不改变 RESP 版本；只有 HELLO 真正切换协议队列时才更新 pool handle。
        if protocol_version != previous_protocol_version {
            pool_handle = runtime.pipeline_pool.node_handle(
                &runtime.backend,
                item.backend_profile
                    .backend_protocol_version(protocol_version),
            );
        }
        if local_prefix == batch.commands.len() {
            continue;
        }
        let commands = &batch.commands[local_prefix..];
        let raw_start = batch.command_spans[local_prefix].start;
        let raw = batch.raw.slice(raw_start..);
        if let Some(responses) = read_only_batch_response(&item, commands) {
            let _pending_pipeline = metrics.start_pending_pipeline(commands.len());
            let bytes_to_write = response_frames_bytes(&responses);
            metrics.record_pipeline_result(
                commands.len(),
                bytes_to_write,
                responses.len(),
                0,
                false,
            );
            let _pending = metrics.start_pending_response(bytes_to_write);
            write_response_frames(&mut client, &responses).await?;
            continue;
        }
        let plan = classify_node_proxy_batch(commands);

        if let Some(tunnel_idx) = plan.tunnel_idx {
            if tunnel_idx > 0 {
                let absolute_tunnel_idx = local_prefix + tunnel_idx;
                let prefix_raw = batch
                    .raw
                    .slice(raw_start..batch.command_spans[absolute_tunnel_idx].start);
                let _pending_pipeline = metrics.start_pending_pipeline(tunnel_idx);
                let started = Instant::now();
                let execution = execute_session_request_response(
                    &pool_handle,
                    &commands[..tunnel_idx],
                    prefix_raw,
                )
                .await;
                let elapsed_micros = duration_micros_u64(started.elapsed());
                let execution_error = execution.is_err();
                metrics.record_backend_forward_result(tunnel_idx, elapsed_micros, execution_error);
                let mut responses = match execution {
                    Ok(responses) => responses,
                    Err(err) => {
                        if let Some(context) = sentinel.as_ref() {
                            context.control.report_backend_failure();
                        }
                        warn!(
                            proxy = %item.name,
                            backend = %runtime.backend,
                            error = %err,
                            "standalone pre-tunnel pipeline failed"
                        );
                        BackendResponseBatch::Frames(vec![
                            resp::err(&format!(
                                "standalone backend error: {err}"
                            ));
                            tunnel_idx
                        ])
                    }
                };
                let readonly_route_error =
                    sentinel.is_some() && backend_batch_has_readonly_error(&responses);
                if readonly_route_error {
                    sentinel
                        .as_ref()
                        .expect("sentinel context checked above")
                        .control
                        .report_backend_failure();
                }
                let response_limit = enforce_backend_response_batch_limit(
                    &mut responses,
                    item.pending_response_bytes,
                );
                if response_limit.limit_hit {
                    metrics.record_pending_response_limit_hit();
                }
                metrics.record_pipeline_result(
                    tunnel_idx,
                    response_limit.bytes_to_write,
                    response_limit.error_count,
                    elapsed_micros,
                    execution_error,
                );
                let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
                write_backend_response_batch(&mut client, &responses).await?;
                if execution_error || readonly_route_error {
                    return Ok(());
                }
            }

            let absolute_tunnel_idx = local_prefix + tunnel_idx;
            let tunnel_start = batch.command_spans[absolute_tunnel_idx].start;
            let tunnel_raw = batch.raw.slice(tunnel_start..);
            let tunnel_command_count = commands.len() - tunnel_idx;
            let _tunnel_permit = match tunnel_limiter.acquire(&metrics).await {
                Ok(permit) => permit,
                Err(err) => {
                    write_dedicated_tunnel_limit_error(
                        &mut client,
                        &item,
                        &metrics,
                        tunnel_command_count,
                        err,
                    )
                    .await?;
                    return Ok(());
                }
            };
            let pending_pipeline = metrics.start_pending_pipeline(tunnel_command_count);
            let started = Instant::now();
            let mut tunnel_lease = match acquire_standalone_tunnel_lease(
                &item,
                &runtime,
                &metrics,
                item.backend_profile
                    .backend_protocol_version(protocol_version),
                &tunnel_raw,
            )
            .await
            {
                Ok(lease) => lease,
                Err(err) => {
                    if let Some(context) = sentinel.as_ref() {
                        context.control.report_backend_failure();
                    }
                    let elapsed_micros = duration_micros_u64(started.elapsed());
                    warn!(
                        proxy = %item.name,
                        backend = %runtime.backend,
                        error = %err,
                        "standalone tunnel backend acquire/write failed"
                    );
                    write_tunnel_backend_error(
                        &mut client,
                        &metrics,
                        tunnel_command_count,
                        elapsed_micros,
                        format!("standalone backend error: {err}"),
                    )
                    .await?;
                    return Ok(());
                }
            };

            // 只有单条、响应边界明确且执行结束后不会保留连接状态的阻塞命令才能归池。
            // 事务、订阅、SELECT 等状态型命令仍进入官方透明 relay，并在结束后直接关闭。
            let reusable_blocking = tunnel_idx == 0
                && commands.len() == 1
                && command_is_reusable_blocking(&commands[0]);
            if reusable_blocking {
                let response = match wait_reusable_blocking_response(
                    &item,
                    &mut client,
                    &mut buf,
                    &mut tmp,
                    &mut shutdown,
                    &mut tunnel_lease,
                    BlockingRouteContext {
                        backend_addr: &runtime.backend,
                        backend_change: sentinel.as_ref().map(|context| {
                            DynamicRoutingReceiver::Sentinel(context.control.routing_rx())
                        }),
                    },
                )
                .await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => return Ok(()),
                    Err(err) => {
                        if let Some(context) = sentinel.as_ref() {
                            context.control.report_backend_failure();
                        }
                        let elapsed_micros = duration_micros_u64(started.elapsed());
                        warn!(
                            proxy = %item.name,
                            backend = %runtime.backend,
                            error = %err,
                            "standalone reusable blocking tunnel read failed"
                        );
                        write_tunnel_backend_error(
                            &mut client,
                            &metrics,
                            tunnel_command_count,
                            elapsed_micros,
                            format!("standalone backend error: {err}"),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                let elapsed_micros = duration_micros_u64(started.elapsed());
                let response_bytes = response.bytes.len();
                metrics.record_pipeline_result(
                    tunnel_command_count,
                    response_bytes,
                    usize::from(response.is_error),
                    elapsed_micros,
                    false,
                );
                finish_single_response_tunnel(
                    tunnel_lease,
                    &item.name,
                    &runtime.backend,
                    "standalone",
                );
                let _pending = metrics.start_pending_response(response_bytes);
                client.write_all(&response.bytes).await?;
                continue;
            }

            let elapsed_micros = duration_micros_u64(started.elapsed());
            metrics.record_pipeline_result(tunnel_command_count, 0, 0, elapsed_micros, false);

            // 长生命周期透明 relay 不属于“待执行 pipeline”；专用隧道活跃数由 permit
            // 内部 guard 独立计量，避免 SUBSCRIBE/XREAD BLOCK 0 永久抬高 pending 指标。
            drop(pending_pipeline);
            let backend = tunnel_lease.into_tainted_connection();
            let relay_parts = backend.into_relay_parts();
            let _backend_connection = relay_parts.backend_connection_guard;
            let pending_client_to_backend = std::mem::take(&mut buf);
            // Sentinel 的事务、订阅和无限阻塞命令不能迁移。为透明隧道额外订阅
            // 路由 watch；新主发布或进入保护态时同时关闭前后端，由 SDK 重建状态。
            let (relay_shutdown, route_stop_task) = if let Some(context) = sentinel.as_ref() {
                let (receiver, task) = spawn_dynamic_relay_stop(
                    item.name.clone(),
                    runtime.backend.clone(),
                    DynamicRoutingReceiver::Sentinel(context.routing_rx.clone()),
                    shutdown.clone(),
                );
                (receiver, Some(task))
            } else {
                (shutdown, None)
            };
            let (client_to_backend, backend_to_client) = copy_bidirectional_bounded_with_preface(
                client,
                relay_parts.stream,
                pending_client_to_backend,
                relay_parts.pending_response,
                relay_shutdown,
                metrics.clone(),
                client_idle_timeout,
            )
            .await?;
            if let Some(task) = route_stop_task {
                task.abort();
            }
            metrics.record_relay_bytes(client_to_backend, backend_to_client);
            debug!(
                proxy = %item.name,
                backend = %runtime.backend,
                client_to_backend,
                backend_to_client,
                "standalone switched to dedicated tunnel"
            );
            return Ok(());
        }

        // QUIT 由 proxy 本地应答:先执行 QUIT 之前的前缀,回 +OK 后关闭;其后命令按 Redis 语义丢弃。
        let cut = plan.quit_idx.unwrap_or(commands.len());
        if cut == 0 {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        let exec_commands = &commands[..cut];
        let exec_raw = if cut == commands.len() {
            raw.clone()
        } else {
            batch
                .raw
                .slice(raw_start..batch.command_spans[local_prefix + cut].start)
        };

        let _pending_pipeline = metrics.start_pending_pipeline(exec_commands.len());
        let started = Instant::now();
        let execution =
            execute_session_request_response(&pool_handle, exec_commands, exec_raw).await;
        let elapsed_micros = duration_micros_u64(started.elapsed());
        let execution_error = execution.is_err();
        metrics.record_backend_forward_result(exec_commands.len(), elapsed_micros, execution_error);
        let mut responses = match execution {
            Ok(responses) => responses,
            Err(err) => {
                if let Some(context) = sentinel.as_ref() {
                    context.control.report_backend_failure();
                }
                warn!(
                    proxy = %item.name,
                    backend = %runtime.backend,
                    error = %err,
                    "standalone backend pipeline failed"
                );
                BackendResponseBatch::Frames(vec![
                    resp::err(&format!(
                        "standalone backend error: {err}"
                    ));
                    exec_commands.len()
                ])
            }
        };
        let readonly_route_error =
            sentinel.is_some() && backend_batch_has_readonly_error(&responses);
        if readonly_route_error {
            sentinel
                .as_ref()
                .expect("sentinel context checked above")
                .control
                .report_backend_failure();
        }
        let response_limit =
            enforce_backend_response_batch_limit(&mut responses, item.pending_response_bytes);
        if response_limit.limit_hit {
            metrics.record_pending_response_limit_hit();
        }
        metrics.record_pipeline_result(
            exec_commands.len(),
            response_limit.bytes_to_write,
            response_limit.error_count,
            elapsed_micros,
            execution_error,
        );
        let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
        write_backend_response_batch(&mut client, &responses).await?;
        if plan.quit_idx.is_some() {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        if execution_error || readonly_route_error {
            return Ok(());
        }
    }
}

/// 业务作用: 把 Sentinel 会话缓存的路由代次与控制面 watch 快照同步。
///
/// 参数说明：
/// - `item`：当前代理配置，决定后端协议版本和新代次连接池参数。
/// - `context`：当前前端会话的 Sentinel 控制面、数据面和已见代次缓存。
/// - `runtime`：当前会话正在使用的数据面资源；切主时原子替换其共享句柄。
/// - `pool_handle`：当前 RESP 协议对应的 pipeline 提交句柄；切主时同步替换。
/// - `protocol_version`：前端已协商 RESP 版本；用于选择隔离的后端连接队列。
///
/// 返回：`None` 表示存在可服务路由；`Some(reason)` 表示必须保护性关闭会话。
///
/// 关键约束：
/// 1. 相同代次直接返回，普通热路径只付出一次 watch 借用和整数比较。
/// 2. 新代次先取得对应的整组 pipeline/tunnel 资源，再替换会话句柄。
/// 3. 非 Serving 状态只返回保护原因，不回退到旧主或任意副本。
fn synchronize_sentinel_session_route(
    item: &ProxyItem,
    context: &mut SentinelSessionContext,
    runtime: &mut Arc<StandaloneRuntime>,
    pool_handle: &mut NodePipelineHandle,
    protocol_version: Option<u8>,
) -> anyhow::Result<Option<String>> {
    let state = context.routing_rx.borrow().clone();
    match state {
        SentinelRoutingState::Serving {
            master, generation, ..
        } => {
            if generation != context.generation || master != runtime.backend {
                let resources = context.data.resources(item, &master, generation)?;
                *pool_handle = resources.pipeline_pool.node_handle(
                    &resources.backend,
                    item.backend_profile
                        .backend_protocol_version(protocol_version),
                );
                *runtime = resources;
                context.generation = generation;
            }
            Ok(None)
        }
        SentinelRoutingState::Resolving { reason } | SentinelRoutingState::Protected { reason } => {
            Ok(Some(reason))
        }
    }
}

/// 业务作用: 在 replication-ha 只读模式逐批校验命令，并在路由保护或切主时丢弃旧主在途响应并关闭会话。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `auth_result`: 前端认证解析结果，决定写回握手响应、拒绝连接或继续读取命令。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `backend_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `backend_change`: 后端路由是否已经变化的标记，用于决定旧连接是否必须关闭。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// replication-ha 在 read_only=false 时走透明 TCP relay；read_only=true 时解析每批命令并执行只读门禁。
async fn handle_replication_ha_readonly_connection(
    item: Arc<ProxyItem>,
    mut client: TcpStream,
    auth_result: FrontendAuthResult,
    mut shutdown: watch::Receiver<bool>,
    backend_addr: String,
    mut backend_change: watch::Receiver<HaRoutingState>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let mut protocol_version = auth_result.protocol_version;
    let mut buf = auth_result.leftover;
    let mut tmp = vec![0u8; FRONTEND_READ_BUFFER_SIZE];
    let client_idle_timeout = optional_duration_ms(item.client_idle_timeout_ms);
    let mut backend = connect_replication_ha_request_response_backend(
        &item,
        &backend_addr,
        &metrics,
        protocol_version,
    )
    .await?;

    loop {
        if let Some(reason) = ha_relay_close_reason(&backend_change.borrow(), &backend_addr) {
            info!(
                proxy = %item.name,
                old_backend = %backend_addr,
                reason = %reason,
                "closing replication-ha readonly session because route changed"
            );
            return Ok(());
        }

        let batch = loop {
            let batch = match parse_node_proxy_batch(&mut buf, &item) {
                Ok(batch) => batch,
                Err(err)
                    if should_defer_valkey_protocol_error(
                        item.backend_profile,
                        buf.len(),
                        &err,
                    ) =>
                {
                    NodeProxyBatch {
                        raw: Bytes::new(),
                        commands: Vec::new(),
                        command_spans: Vec::new(),
                    }
                }
                Err(err) => {
                    write_frontend_protocol_error(&mut client, &err).await?;
                    return Ok(());
                }
            };
            if !batch.commands.is_empty() {
                break batch;
            }
            if buf.len() > item.pipeline_max_bytes {
                bail!(
                    "client pipeline buffer exceeds limit: {} > {}",
                    buf.len(),
                    item.pipeline_max_bytes
                );
            }
            if let Some(timeout) = client_idle_timeout {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = tokio::time::sleep(timeout) => {
                        bail!("client idle timeout");
                    }
                    changed = backend_change.changed() => {
                        if changed.is_err() || ha_relay_close_reason(&backend_change.borrow(), &backend_addr).is_some() {
                            return Ok(());
                        }
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing replication-ha readonly connection on shutdown");
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    changed = backend_change.changed() => {
                        if changed.is_err() || ha_relay_close_reason(&backend_change.borrow(), &backend_addr).is_some() {
                            return Ok(());
                        }
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing replication-ha readonly connection on shutdown");
                        return Ok(());
                    }
                }
            }
        };

        let previous_protocol_version = protocol_version;
        let local_prefix = consume_local_handshake_prefix(
            &item,
            &mut client,
            &batch.commands,
            "standalone",
            &mut protocol_version,
            &metrics,
        )
        .await?;
        // CLIENT SETINFO 只产生本地响应，不改变后端协议；只有 HELLO 实际切换 RESP
        // 版本时才重建本会话专属后端，避免每个 Lettuce 连接多做一次无意义 TCP 建连。
        if protocol_version != previous_protocol_version {
            backend = connect_replication_ha_request_response_backend(
                &item,
                &backend_addr,
                &metrics,
                protocol_version,
            )
            .await?;
        }
        if local_prefix == batch.commands.len() {
            continue;
        }
        let commands = &batch.commands[local_prefix..];
        let raw_start = batch.command_spans[local_prefix].start;
        let raw = batch.raw.slice(raw_start..);

        if let Some(responses) = read_only_batch_response(&item, commands) {
            let _pending_pipeline = metrics.start_pending_pipeline(commands.len());
            let bytes_to_write = response_frames_bytes(&responses);
            metrics.record_pipeline_result(
                commands.len(),
                bytes_to_write,
                responses.len(),
                0,
                false,
            );
            let _pending = metrics.start_pending_response(bytes_to_write);
            write_response_frames(&mut client, &responses).await?;
            continue;
        }

        let plan = classify_node_proxy_batch(commands);
        if let Some(tunnel_idx) = plan.tunnel_idx {
            let tunnel_command_count = commands.len() - tunnel_idx;
            let responses = vec![read_only_error(&item.name); tunnel_command_count];
            let _pending_pipeline = metrics.start_pending_pipeline(tunnel_command_count);
            let bytes_to_write = response_frames_bytes(&responses);
            metrics.record_pipeline_result(
                tunnel_command_count,
                bytes_to_write,
                responses.len(),
                0,
                false,
            );
            let _pending = metrics.start_pending_response(bytes_to_write);
            write_response_frames(&mut client, &responses).await?;
            continue;
        }

        let cut = plan.quit_idx.unwrap_or(commands.len());
        if cut == 0 {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        let exec_commands = &commands[..cut];
        let exec_raw = if cut == commands.len() {
            raw.clone()
        } else {
            batch
                .raw
                .slice(raw_start..batch.command_spans[local_prefix + cut].start)
        };

        let _pending_pipeline = metrics.start_pending_pipeline(exec_commands.len());
        let started = Instant::now();
        // 只读连接会复用后端连接，因此切主可能恰好发生在命令已经解析、
        // 后端响应尚未返回的窗口。若只在循环开头检查路由，旧后端响应仍可能
        // 被写回客户端。这里把请求执行和路由通知放进同一个竞争等待：一旦
        // master 变化或进入保护态，立即丢弃旧连接及其未完成响应，让客户端
        // 重连到新 master。数据正确性优先于切换窗口内的一次请求可用性。
        let execution = tokio::select! {
            biased;
            changed = backend_change.changed() => {
                let reason = if changed.is_err() {
                    "routing state channel closed".to_string()
                } else {
                    ha_relay_close_reason(&backend_change.borrow(), &backend_addr)
                        .unwrap_or_else(|| "routing state changed".to_string())
                };
                info!(
                    proxy = %item.name,
                    old_backend = %backend_addr,
                    reason = %reason,
                    "closing replication-ha readonly session during backend request"
                );
                return Ok(());
            }
            execution = backend.send_raw_pipeline_joined(exec_raw, exec_commands.len()) => {
                execution.map(BackendResponseBatch::Joined)
            }
        };
        // 后端响应和路由通知可能在相邻的调度周期完成。即使上面的请求分支
        // 先返回，也必须在写客户端前再次读取最新状态，封住最后一个旧响应
        // 外泄窗口。此处只丢弃响应，不重放命令，避免未来新增带副作用命令时
        // 因自动重试产生重复执行。
        if let Some(reason) = ha_relay_close_reason(&backend_change.borrow(), &backend_addr) {
            info!(
                proxy = %item.name,
                old_backend = %backend_addr,
                reason = %reason,
                "discarding replication-ha readonly response because route changed"
            );
            return Ok(());
        }
        let elapsed_micros = duration_micros_u64(started.elapsed());
        let execution_error = execution.is_err();
        metrics.record_backend_forward_result(exec_commands.len(), elapsed_micros, execution_error);
        let mut responses = match execution {
            Ok(responses) => responses,
            Err(err) => {
                warn!(
                    proxy = %item.name,
                    backend = %backend_addr,
                    error = %err,
                    "replication-ha readonly backend pipeline failed"
                );
                BackendResponseBatch::Frames(vec![
                    resp::err(&format!(
                        "replication-ha backend error: {err}"
                    ));
                    exec_commands.len()
                ])
            }
        };
        let response_limit =
            enforce_backend_response_batch_limit(&mut responses, item.pending_response_bytes);
        if response_limit.limit_hit {
            metrics.record_pending_response_limit_hit();
        }
        metrics.record_pipeline_result(
            exec_commands.len(),
            response_limit.bytes_to_write,
            response_limit.error_count,
            elapsed_micros,
            execution_error,
        );
        let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
        write_backend_response_batch(&mut client, &responses).await?;
        if plan.quit_idx.is_some() {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        if execution_error {
            return Ok(());
        }
    }
}

/// 业务作用: 建立指定后端的认证连接并执行有界双向透明转发，动态路由变化时主动终止旧后端隧道。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `auth_result`: 前端认证解析结果，决定写回握手响应、拒绝连接或继续读取命令。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `backend_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `backend_change`: 后端路由是否已经变化的标记，用于决定旧连接是否必须关闭。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 后端地址变化与 shutdown 同时参与 relay 取消，旧主连接不能在新路由发布后继续传递响应。
async fn handle_tcp_relay(
    item: Arc<ProxyItem>,
    client: TcpStream,
    auth_result: FrontendAuthResult,
    shutdown: watch::Receiver<bool>,
    backend_addr: String,
    backend_change: Option<DynamicRoutingReceiver>,
    metrics: ProxyMetrics,
) -> anyhow::Result<()> {
    let FrontendAuthResult {
        leftover,
        protocol_version: frontend_protocol_version,
    } = auth_result;
    let (username, password) = backend_auth_parts(&item);
    let tls = BackendTlsConfig::for_backend(&item);
    let mut backend = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: &backend_addr,
        username,
        password,
        connect_timeout_ms: item.connect_timeout_ms,
        request_timeout_ms: item.backend_request_timeout_ms,
        tcp_keepalive_ms: item.tcp_keepalive_ms,
        response_bytes_limit: Some(item.pending_response_bytes),
        metrics: Some(metrics.clone()),
        tls: tls.as_ref(),
    })
    .await
    .with_context(|| format!("proxy `{}` connect `{backend_addr}`", item.name))?;
    if let Some(protocol_version) = item
        .backend_profile
        .backend_protocol_version(frontend_protocol_version)
    {
        backend.set_protocol_version(protocol_version).await?;
    }
    let relay_parts = backend.into_relay_parts();
    let _backend_connection = relay_parts.backend_connection_guard;
    let mut backend = relay_parts.stream;

    if !leftover.is_empty() {
        backend.write_all(&leftover).await?;
    }

    let (client_to_backend, backend_to_client) = if let Some(backend_change) = backend_change {
        let (relay_stop_rx, stop_task) = spawn_dynamic_relay_stop(
            item.name.clone(),
            backend_addr.clone(),
            backend_change,
            shutdown.clone(),
        );
        let result = copy_bidirectional_bounded(
            client,
            backend,
            relay_stop_rx,
            metrics.clone(),
            optional_duration_ms(item.client_idle_timeout_ms),
        )
        .await;
        stop_task.abort();
        result?
    } else {
        copy_bidirectional_bounded(
            client,
            backend,
            shutdown,
            metrics.clone(),
            optional_duration_ms(item.client_idle_timeout_ms),
        )
        .await?
    };
    metrics.record_relay_bytes(client_to_backend, backend_to_client);
    debug!(
        proxy = %item.name,
        client_to_backend,
        backend_to_client,
        "relay finished"
    );

    Ok(())
}

/// 业务作用: 启动动态路由隧道的停止观察器。
///
/// 参数说明：
/// - `proxy_name`：当前代理名称，用于切路由关闭日志。
/// - `old_backend`：当前透明隧道实际连接的旧后端地址。
/// - `backend_change`：replication HA 或 Sentinel 路由 watch 的统一接收器。
/// - `shutdown`：进程级关闭信号，保证退出时观察器和 relay 同步收敛。
///
/// 返回：返回 relay 停止接收器和观察任务句柄；调用方在 relay 结束后取消任务。
///
/// 观察器同时接收进程关闭与后端路由 watch；一旦旧地址不再可服务，就发布本地
/// stop 信号。调用方持有任务句柄并在 relay 正常结束后主动取消，避免后台任务泄漏。
fn spawn_dynamic_relay_stop(
    proxy_name: String,
    old_backend: String,
    mut backend_change: DynamicRoutingReceiver,
    mut shutdown: watch::Receiver<bool>,
) -> (watch::Receiver<bool>, tokio::task::JoinHandle<()>) {
    let (relay_stop_tx, relay_stop_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut poll = tokio::time::interval(Duration::from_millis(50));
        poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if let Some(reason) = backend_change.close_reason(&old_backend) {
                info!(
                    proxy = %proxy_name,
                    old_backend = %old_backend,
                    reason = %reason,
                    "closing relay because dynamic backend route changed"
                );
                break;
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                changed = backend_change.changed() => {
                    if changed.is_err() {
                        break;
                    }
                },
                _ = poll.tick() => {}
            }
        }
        let _ = relay_stop_tx.send(true);
    });
    (relay_stop_rx, task)
}

/// 业务作用: 判断 replication-ha 路由变化后指向旧主的透明连接是否必须关闭。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `old_backend`: 切换前的后端地址，用于识别并关闭仍指向旧 master 的连接。
///
fn ha_relay_close_reason(state: &HaRoutingState, old_backend: &str) -> Option<String> {
    match state {
        HaRoutingState::Serving { master } if master != old_backend => {
            Some(format!("backend master changed to {master}"))
        }
        HaRoutingState::Serving { .. } => None,
        HaRoutingState::Protected { reason } => Some(format!("protected: {reason}")),
    }
}

/// 业务作用: 判断 Sentinel 路由变化后，连接旧主节点的 relay 是否必须关闭。
///
/// 参数说明：
/// - `state`：最新 Sentinel 路由状态快照。
/// - `old_backend`：现有 relay 实际连接的旧数据节点地址。
///
/// 返回：旧地址已不可服务时返回关闭原因；仍是当前已验证主节点时返回 `None`。
fn sentinel_relay_close_reason(state: &SentinelRoutingState, old_backend: &str) -> Option<String> {
    match state {
        SentinelRoutingState::Serving { master, .. } if master != old_backend => {
            Some(format!("backend master changed to {master}"))
        }
        SentinelRoutingState::Serving { .. } => None,
        SentinelRoutingState::Resolving { reason } => Some(format!("resolving: {reason}")),
        SentinelRoutingState::Protected { reason } => Some(format!("protected: {reason}")),
    }
}

/// 业务作用: 原子累计透明隧道两个方向已经成功转发的字节数。
#[derive(Default)]
// 表示 RelayCounters 的数据结构，用于处理前端连接、后端转发、限流和优雅关闭。
struct RelayCounters {
    client_to_backend: AtomicU64,
    backend_to_client: AtomicU64,
}

/// 业务作用: 同时保存节点代理一批原始 pipeline、解析命令及各命令字节区间。
#[derive(Debug)]
// 表示 NodeProxyBatch 的数据结构，用于 cluster-node-proxy 同时持有连续原始 pipeline 和命令元数据。
struct NodeProxyBatch {
    raw: Bytes,
    commands: Vec<ClientCommand>,
    command_spans: Vec<std::ops::Range<usize>>,
}

/// 业务作用: 在关闭、路由切换和双向 idle 门禁下复制前后端字节流。
/// 参数说明:
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `client_idle_timeout`: 前端连接在 relay 阶段允许保持空闲的最长时间。
///
/// 返回: 双向转发结束时返回两个方向的字节数；关闭、idle 或 I/O 异常按对应结果返回。
async fn copy_bidirectional_bounded(
    client: TcpStream,
    backend: BackendStream,
    shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    client_idle_timeout: Option<Duration>,
) -> io::Result<(u64, u64)> {
    copy_bidirectional_bounded_with_preface(
        client,
        backend,
        Vec::new(),
        Vec::new(),
        shutdown,
        metrics,
        client_idle_timeout,
    )
    .await
}

/// 业务作用: 执行带预置字节的双向转发，用于把已解析连接切换到透明隧道且不丢失缓冲区数据。
/// 参数说明:
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
/// - `client_to_backend_preface`: 进入透明转发前已从客户端读取的前置字节，必须先转发到后端。
/// - `backend_to_client_preface`: 建立透明转发前已从后端读取的前置字节，必须先按原顺序写回客户端。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `client_idle_timeout`: 前端连接在 relay 阶段允许保持空闲的最长时间。
///
/// 返回: 双向转发结束时返回两个方向的字节数；关闭、idle 或 I/O 异常按对应结果返回。
// 执行带预置字节的双向转发，用于把已解析连接切换到透明隧道且不丢失缓冲区数据。
async fn copy_bidirectional_bounded_with_preface(
    mut client: TcpStream,
    mut backend: BackendStream,
    client_to_backend_preface: Vec<u8>,
    backend_to_client_preface: Vec<u8>,
    shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    client_idle_timeout: Option<Duration>,
) -> io::Result<(u64, u64)> {
    let counters = Arc::new(RelayCounters::default());
    if !backend_to_client_preface.is_empty() {
        client.write_all(&backend_to_client_preface).await?;
        counters
            .backend_to_client
            .fetch_add(backend_to_client_preface.len() as u64, Ordering::Relaxed);
    }
    if !client_to_backend_preface.is_empty() {
        backend.write_all(&client_to_backend_preface).await?;
        counters
            .client_to_backend
            .fetch_add(client_to_backend_preface.len() as u64, Ordering::Relaxed);
    }
    let (client_read, client_write) = client.into_split();
    let (backend_read, backend_write) = tokio::io::split(backend);

    // idle 判定基于「双向」活动共享的最近活动时间戳：任一方向有字节流动即刷新，
    // 使持续收 pub/sub 推送或阻塞等待结果的连接(SUBSCRIBE/BLPOP/XREAD BLOCK/WAIT)不被误杀，
    // 同时真正双向静默的连接仍在 idle_timeout 后被精确回收。tunnel 之外的普通 pipeline 不走此路径。
    let last_activity = Arc::new(AtomicU64::new(0));
    let idle_base = Instant::now();

    let mut client_to_backend_task = tokio::spawn(copy_relay_direction(
        client_read,
        backend_write,
        shutdown.clone(),
        metrics.clone(),
        counters.clone(),
        true,
        RelayIdle {
            timeout: client_idle_timeout,
            last_activity: last_activity.clone(),
            base: idle_base,
        },
    ));
    let mut backend_to_client_task = tokio::spawn(copy_relay_direction(
        backend_read,
        client_write,
        shutdown,
        metrics,
        counters.clone(),
        false,
        RelayIdle {
            timeout: None,
            last_activity,
            base: idle_base,
        },
    ));

    let result = tokio::select! {
        result = &mut client_to_backend_task => join_relay_result(result),
        result = &mut backend_to_client_task => join_relay_result(result),
    };

    client_to_backend_task.abort();
    backend_to_client_task.abort();
    result?;

    Ok((
        counters.client_to_backend.load(Ordering::Relaxed),
        counters.backend_to_client.load(Ordering::Relaxed),
    ))
}

/// 业务作用: 保存双向 relay 共享活动时钟和 idle 超时，使任一方向有流量即可续活。
// tunnel/relay 的 idle 计时上下文：timeout 只在 client->backend 方向生效；
// last_activity/base 由两个方向共享，任一方向有字节流动即刷新，实现「双向活动」的精确 idle 判定。
struct RelayIdle {
    timeout: Option<Duration>,
    last_activity: Arc<AtomicU64>,
    base: Instant,
}

/// 业务作用: 复制透明隧道的单向字节流，并更新活动时间与方向流量计数。
/// 参数说明:
/// - `reader`: 协议读取端，函数从中增量读取并处理半帧、断连和大小限制。
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `counters`: 运行时原子计数器集合，用于记录连接、请求、错误和资源占用。
/// - `client_to_backend`: 客户端到后端方向累计传输的字节数，用于流量指标统计。
/// - `idle`: 连接进入空闲状态的时间点或时长，用于判断是否应回收。
///
/// 返回: 任务正常完成或被有序取消时返回成功；panic 或底层 I/O 失败时返回错误。
async fn copy_relay_direction<R, W>(
    mut reader: R,
    mut writer: W,
    mut shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    counters: Arc<RelayCounters>,
    client_to_backend: bool,
    idle: RelayIdle,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; RELAY_BUFFER_SIZE];
    loop {
        let read = read_relay_chunk(&mut reader, &mut buf, &mut shutdown, &idle).await?;
        if read == 0 {
            return Ok(());
        }
        // 本方向有字节到达即刷新共享活动时间戳，用于重置对向(client->backend)的 idle 计时。
        idle.last_activity.store(
            idle.base.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
            Ordering::Relaxed,
        );
        let written = write_all_with_backpressure(
            &mut writer,
            &buf[..read],
            &mut shutdown,
            &metrics,
            client_to_backend,
        )
        .await?;
        let counter = if client_to_backend {
            &counters.client_to_backend
        } else {
            &counters.backend_to_client
        };
        counter.fetch_add(written as u64, Ordering::Relaxed);
        if written < read {
            return Ok(());
        }
    }
}

/// 业务作用: 在关闭与双向 idle 门禁下读取一段 relay 字节，并刷新共享活动时间。
/// 参数说明:
/// - `reader`: 协议读取端，函数从中增量读取并处理半帧、断连和大小限制。
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `idle`: 连接进入空闲状态的时间点或时长，用于判断是否应回收。
///
/// 返回: 输入可安全转换且未越界时返回数量；缺失、格式非法或溢出时返回错误。
async fn read_relay_chunk<R>(
    reader: &mut R,
    buf: &mut [u8],
    shutdown: &mut watch::Receiver<bool>,
    idle: &RelayIdle,
) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    if *shutdown.borrow() {
        return Ok(0);
    }
    let Some(timeout) = idle.timeout else {
        return tokio::select! {
            read = reader.read(buf) => read,
            _ = shutdown.changed() => Ok(0),
        };
    };
    // 精确 idle：以共享 last_activity(任一方向最近一次字节流动) 为基准，仅当距最近活动已满 timeout 才回收。
    // 这样持续收 pub/sub 推送或阻塞等待结果的连接(SUBSCRIBE/BLPOP/XREAD BLOCK/WAIT)不会被误杀，
    // 与 Redis 对阻塞/订阅连接豁免 idle timeout 的语义一致；真正双向静默的连接仍在 timeout 后被精确回收。
    // tunnel 之外的普通 pipeline 不走此路径，热路径零影响。
    loop {
        if *shutdown.borrow() {
            return Ok(0);
        }
        let idle_for = idle.base.elapsed().saturating_sub(Duration::from_millis(
            idle.last_activity.load(Ordering::Relaxed),
        ));
        if idle_for >= timeout {
            return Err(io::Error::new(ErrorKind::TimedOut, "tunnel idle timeout"));
        }
        let remaining = timeout - idle_for; // > 0
        tokio::select! {
            read = reader.read(buf) => return read,
            _ = shutdown.changed() => return Ok(0),
            // 醒来后回到循环顶部重算 idle_for：对向在此期间有活动(last_activity 前移)则继续等待，否则精确回收。
            _ = tokio::time::sleep(remaining) => {}
        }
    }
}

/// 业务作用: 完整写出 relay 缓冲并累计实际等待可写的背压时间。
/// 参数说明:
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `client_to_backend`: 客户端到后端方向累计传输的字节数，用于流量指标统计。
///
/// 返回: 输入可安全转换且未越界时返回数量；缺失、格式非法或溢出时返回错误。
async fn write_all_with_backpressure<W>(
    writer: &mut W,
    mut bytes: &[u8],
    shutdown: &mut watch::Receiver<bool>,
    metrics: &ProxyMetrics,
    client_to_backend: bool,
) -> io::Result<usize>
where
    W: AsyncWrite + Unpin,
{
    let mut total_written = 0usize;
    while !bytes.is_empty() {
        if *shutdown.borrow() {
            return Ok(total_written);
        }
        let mut backpressure_recorded = false;
        let write = std::future::poll_fn(|cx| {
            let result = Pin::new(&mut *writer).poll_write(cx, bytes);
            if result.is_pending() && !backpressure_recorded {
                metrics.record_relay_backpressure(client_to_backend);
                backpressure_recorded = true;
            }
            result
        });
        match tokio::select! {
            written = write => written,
            _ = shutdown.changed() => return Ok(total_written),
        } {
            Ok(0) => {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "relay write returned zero bytes",
                ));
            }
            Ok(written) => {
                total_written = total_written.saturating_add(written);
                bytes = &bytes[written..];
            }
            Err(err) => return Err(err),
        }
    }
    Ok(total_written)
}

/// 业务作用: 归一化 relay 任务完成、取消和 I/O 错误，确保另一方向及时停止。
/// 参数说明:
/// - `result`: 异步任务或后端操作结果，用于传播错误并更新成功失败指标。
///
/// 返回: 任务正常完成或被有序取消时返回成功；panic 或底层 I/O 失败时返回错误。
fn join_relay_result(result: Result<io::Result<()>, tokio::task::JoinError>) -> io::Result<()> {
    match result {
        Ok(result) => result,
        Err(err) => Err(io::Error::other(format!("relay task failed: {err}"))),
    }
}

/// 业务作用: 驱动 Cluster 单入口会话的命令规划、跨节点执行、响应聚合和连接绑定隧道，同时保持 pipeline 原始顺序。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `cluster`: 集群运行时或拓扑句柄，用于槽位路由、刷新和节点选择。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `auth_result`: 前端认证阶段留下的未消费字节和已协商 RESP 版本；
///   前者必须继续参与本会话解析，后者用于 Cluster 节点 tunnel pool 的协议隔离。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `tunnel_limiter`: 当前 proxy 私有的专用隧道并发与等待队列限制器。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// Cluster 会话必须保留认证阶段多读出的字节和协议版本，否则首批 pipeline 会丢命令或串用连接池语义。
async fn handle_cluster_connection(
    item: Arc<ProxyItem>,
    cluster: Arc<ClusterRuntime>,
    mut client: TcpStream,
    auth_result: FrontendAuthResult,
    mut shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    tunnel_limiter: Arc<DedicatedTunnelLimiter>,
) -> anyhow::Result<()> {
    // 认证阶段可能已经从 socket 一次读入 AUTH 后面的业务命令；必须原样移交，
    // 否则会丢失同一个 TCP 包里的初始命令。协议版本则贯穿整个会话，用于防止
    // RESP2 与 RESP3 连接进入同一个节点池队列后发生响应语义串用。
    let FrontendAuthResult {
        leftover: mut buf,
        protocol_version,
    } = auth_result;
    let backend_protocol_version = item
        .backend_profile
        .backend_protocol_version(protocol_version);
    let mut tmp = vec![0u8; FRONTEND_READ_BUFFER_SIZE];
    let client_idle_timeout = optional_duration_ms(item.client_idle_timeout_ms);

    loop {
        let commands = loop {
            let commands = match parse_client_batch(&mut buf, &item) {
                Ok(commands) => commands,
                Err(err)
                    if should_defer_valkey_protocol_error(
                        item.backend_profile,
                        buf.len(),
                        &err,
                    ) =>
                {
                    Vec::new()
                }
                Err(err) => {
                    write_frontend_protocol_error(&mut client, &err).await?;
                    return Ok(());
                }
            };
            if !commands.is_empty() {
                break commands;
            }
            if buf.len() > item.pipeline_max_bytes {
                bail!(
                    "client pipeline buffer exceeds limit: {} > {}",
                    buf.len(),
                    item.pipeline_max_bytes
                );
            }
            if let Some(timeout) = client_idle_timeout {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = tokio::time::sleep(timeout) => {
                        bail!("client idle timeout");
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing cluster connection on shutdown");
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, "closing cluster connection on shutdown");
                        return Ok(());
                    }
                }
            }
        };

        if let Some(responses) = read_only_batch_response(&item, &commands) {
            let _pending_pipeline = metrics.start_pending_pipeline(commands.len());
            let bytes_to_write = response_frames_bytes(&responses);
            metrics.record_pipeline_result(
                commands.len(),
                bytes_to_write,
                responses.len(),
                0,
                false,
            );
            let _pending = metrics.start_pending_response(bytes_to_write);
            write_response_frames(&mut client, &responses).await?;
            continue;
        }

        // Cluster 单入口只有在本批恰好一条、且全部 key 已证明属于同一 slot 时才进入
        // 可复用阻塞隧道。含其他命令的 pipeline 继续沿用官方 fail-closed 规划结果，
        // 避免把后续可能属于其他节点的字节错误地绑定到当前 node。
        if commands.len() == 1 && command_is_reusable_blocking(&commands[0]) {
            let command = &commands[0];
            let slot = match cluster_reusable_blocking_slot(command) {
                Ok(slot) => slot,
                Err(err) => {
                    let response = if err.to_string().contains("CROSSSLOT") {
                        resp::crossslot(&err.to_string())
                    } else {
                        resp::err(&err.to_string())
                    };
                    let response_bytes = response.len();
                    let _pending_pipeline = metrics.start_pending_pipeline(1);
                    metrics.record_pipeline_result(1, response_bytes, 1, 0, false);
                    let _pending = metrics.start_pending_response(response_bytes);
                    client.write_all(&response).await?;
                    continue;
                }
            };
            let _tunnel_permit = match tunnel_limiter.acquire(&metrics).await {
                Ok(permit) => permit,
                Err(err) => {
                    write_dedicated_tunnel_limit_error(&mut client, &item, &metrics, 1, err)
                        .await?;
                    return Ok(());
                }
            };
            let _pending_pipeline = metrics.start_pending_pipeline(1);
            let started = Instant::now();
            let (backend, mut tunnel_lease) = match cluster
                .acquire_tunnel_for_slot(slot, backend_protocol_version, command.raw.as_ref())
                .await
            {
                Ok(target) => target,
                Err(err) => {
                    let elapsed_micros = duration_micros_u64(started.elapsed());
                    warn!(
                        proxy = %item.name,
                        slot,
                        error = %err,
                        "cluster reusable blocking tunnel acquire/write failed"
                    );
                    write_tunnel_backend_error(
                        &mut client,
                        &metrics,
                        1,
                        elapsed_micros,
                        format!("cluster backend error: {err:#}"),
                    )
                    .await?;
                    return Ok(());
                }
            };
            let mut response = match wait_reusable_blocking_response(
                &item,
                &mut client,
                &mut buf,
                &mut tmp,
                &mut shutdown,
                &mut tunnel_lease,
                BlockingRouteContext {
                    backend_addr: &backend,
                    backend_change: None,
                },
            )
            .await
            {
                Ok(Some(response)) => response,
                Ok(None) => return Ok(()),
                Err(err) => {
                    let elapsed_micros = duration_micros_u64(started.elapsed());
                    warn!(
                        proxy = %item.name,
                        backend = %backend,
                        slot,
                        error = %err,
                        "cluster reusable blocking tunnel read failed"
                    );
                    write_tunnel_backend_error(
                        &mut client,
                        &metrics,
                        1,
                        elapsed_micros,
                        format!("cluster backend error: {err:#}"),
                    )
                    .await?;
                    return Ok(());
                }
            };
            finish_single_response_tunnel(tunnel_lease, &item.name, &backend, "cluster-initial");

            // 普通 Cluster pipeline 会受控处理一次 MOVED/ASK；池化阻塞路径必须保持相同
            // 路由语义，同时继续使用可观察前端断连的阻塞读取，不能退回带请求超时的普通 I/O。
            if response.bytes.starts_with(b"-MOVED ") || response.bytes.starts_with(b"-ASK ") {
                let redirect_frame = std::mem::take(&mut response.bytes);
                let (redirect_backend, mut redirect_lease) = match cluster
                    .acquire_tunnel_for_redirect(
                        backend_protocol_version,
                        command.raw.as_ref(),
                        &redirect_frame,
                    )
                    .await
                {
                    Ok(target) => target,
                    Err(err) => {
                        let elapsed_micros = duration_micros_u64(started.elapsed());
                        warn!(
                            proxy = %item.name,
                            backend = %backend,
                            slot,
                            error = %err,
                            "cluster blocking redirect retry acquire/write failed"
                        );
                        write_tunnel_backend_error(
                            &mut client,
                            &metrics,
                            1,
                            elapsed_micros,
                            format!("cluster redirect retry failed: {err:#}"),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                response = match wait_reusable_blocking_response(
                    &item,
                    &mut client,
                    &mut buf,
                    &mut tmp,
                    &mut shutdown,
                    &mut redirect_lease,
                    BlockingRouteContext {
                        backend_addr: &redirect_backend,
                        backend_change: None,
                    },
                )
                .await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => return Ok(()),
                    Err(err) => {
                        let elapsed_micros = duration_micros_u64(started.elapsed());
                        warn!(
                            proxy = %item.name,
                            backend = %redirect_backend,
                            slot,
                            error = %err,
                            "cluster blocking redirect retry read failed"
                        );
                        write_tunnel_backend_error(
                            &mut client,
                            &metrics,
                            1,
                            elapsed_micros,
                            format!("cluster redirect retry failed: {err:#}"),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                finish_single_response_tunnel(
                    redirect_lease,
                    &item.name,
                    &redirect_backend,
                    "cluster-redirect",
                );
                // 与普通 Cluster 路径一致：最多重试一次，防止故障拓扑形成无界重定向环。
                if response.bytes.starts_with(b"-MOVED ") || response.bytes.starts_with(b"-ASK ") {
                    response = BackendSingleResponse {
                        bytes: resp::err("cluster redirect retry returned another redirect"),
                        is_error: true,
                    };
                }
            }

            let elapsed_micros = duration_micros_u64(started.elapsed());
            let response_bytes = response.bytes.len();
            metrics.record_pipeline_result(
                1,
                response_bytes,
                usize::from(response.is_error),
                elapsed_micros,
                false,
            );
            let _pending = metrics.start_pending_response(response_bytes);
            client.write_all(&response.bytes).await?;
            continue;
        }

        let quit_idx = first_quit_idx(&commands);
        let exec_count = quit_idx.unwrap_or(commands.len());
        if exec_count == 0 {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        let exec_commands = &commands[..exec_count];
        let _pending_pipeline = metrics.start_pending_pipeline(exec_commands.len());
        let started = Instant::now();
        let execution = cluster.execute_pipeline(exec_commands).await;
        let elapsed_micros = duration_micros_u64(started.elapsed());
        let execution_error = execution.is_err();
        let mut responses = match execution {
            Ok(responses) => responses,
            Err(err) => {
                warn!(proxy = %item.name, error = %err, "cluster pipeline execution failed");
                ClusterPipelineResponse::Frames(vec![
                    resp::err(&format!(
                        "cluster proxy error: {err}"
                    ));
                    exec_commands.len()
                ])
            }
        };
        let response_limit =
            enforce_cluster_pipeline_response_limit(&mut responses, item.pending_response_bytes);
        if response_limit.limit_hit {
            metrics.record_pending_response_limit_hit();
            warn!(
                proxy = %item.name,
                response_bytes = response_limit.original_bytes,
                limit = item.pending_response_bytes,
                "cluster pending response bytes limit hit"
            );
        }
        metrics.record_pipeline_result(
            exec_commands.len(),
            response_limit.bytes_to_write,
            response_limit.error_count,
            elapsed_micros,
            execution_error,
        );
        let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
        write_cluster_pipeline_response(&mut client, &responses).await?;
        if quit_idx.is_some() {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
    }
}

/// 业务作用: 驱动单节点 Cluster 代理会话，在固定源节点上执行普通批次或独占隧道，并改写所有对外拓扑地址。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `node`: 当前连接固定使用的 cluster-node-proxy 节点映射配置。
/// - `runtime`: 当前代理或 HA 运行时句柄，用于读取状态并执行受控操作。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `tunnel_limiter`: 专用隧道容量控制器，用于限制连接绑定命令占用的后端连接。
///
/// 返回: 连接或批次完整结束并完成资源回收时返回成功；认证、路由、后端或 I/O 失败时返回错误。
// 会话固定绑定一个真实节点；所有拓扑帧仍需改写为代理公布地址，不能泄漏后端网络地址。
async fn handle_cluster_node_proxy_connection(
    item: Arc<ProxyItem>,
    node: ClusterNodeProxyItem,
    runtime: Arc<ClusterNodeProxyRuntime>,
    mut client: TcpStream,
    mut shutdown: watch::Receiver<bool>,
    metrics: ProxyMetrics,
    tunnel_limiter: Arc<DedicatedTunnelLimiter>,
) -> anyhow::Result<()> {
    configure_tcp_stream(&client, item.tcp_keepalive_ms, &item.name, "client");

    let mut auth_result = FrontendAuthResult::default();
    if item.effective_auth_mode() == AuthMode::Terminate {
        auth_result = frontend_auth(&item, &mut client).await?;
    }
    let mut protocol_version = auth_result.protocol_version;
    let mut buf = auth_result.leftover;
    // backend/protocol 在 session 内不变:握手后解析一次 pool 句柄,热循环不再查表拼 key。
    let mut pool_handle = runtime.pipeline_pool.node_handle(
        &node.backend,
        item.backend_profile
            .backend_protocol_version(protocol_version),
    );
    // node 与 backend 在单条前端连接生命周期内固定，因此只在进入会话时解析一次
    // 后端专属 tunnel pool。普通 SET/GET 热循环不会查 HashMap，也不会复制 backend 字符串。
    let tunnel_pool = runtime.tunnel_pool_for(&node.backend)?;

    let mut tmp = vec![0u8; FRONTEND_READ_BUFFER_SIZE];
    let client_idle_timeout = optional_duration_ms(item.client_idle_timeout_ms);
    let stage_profile_enabled = node_proxy_stage_profile_enabled();
    // 可选诊断模式让普通批次走 session 专属连接，用于比较连接池借还与连接亲和模型的成本。
    let dedicated_session_mode = node_proxy_dedicated_session_enabled();
    let mut dedicated_backend: Option<BackendConnection> = None;
    loop {
        let read_parse_started = stage_profile_enabled.then(Instant::now);
        let batch = loop {
            let batch = match parse_node_proxy_batch(&mut buf, &item) {
                Ok(batch) => batch,
                Err(err)
                    if should_defer_valkey_protocol_error(
                        item.backend_profile,
                        buf.len(),
                        &err,
                    ) =>
                {
                    NodeProxyBatch {
                        raw: Bytes::new(),
                        commands: Vec::new(),
                        command_spans: Vec::new(),
                    }
                }
                Err(err) => {
                    write_frontend_protocol_error(&mut client, &err).await?;
                    return Ok(());
                }
            };
            if !batch.commands.is_empty() {
                break batch;
            }
            if buf.len() > item.pipeline_max_bytes {
                bail!(
                    "client pipeline buffer exceeds limit: {} > {}",
                    buf.len(),
                    item.pipeline_max_bytes
                );
            }
            if let Some(timeout) = client_idle_timeout {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = tokio::time::sleep(timeout) => {
                        bail!("client idle timeout");
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, backend = %node.backend, "closing cluster-node-proxy connection on shutdown");
                        return Ok(());
                    }
                }
            } else {
                tokio::select! {
                    read = client.read(&mut tmp) => {
                        let n = read?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    _ = shutdown.changed() => {
                        debug!(proxy = %item.name, backend = %node.backend, "closing cluster-node-proxy connection on shutdown");
                        return Ok(());
                    }
                }
            }
        };

        let mut stage_profile = NodeProxyStageProfile {
            command_count: batch.commands.len(),
            ..Default::default()
        };
        if let Some(started) = read_parse_started {
            stage_profile.read_parse_micros = duration_micros_u64(started.elapsed());
        }

        let previous_protocol_version = protocol_version;
        let local_prefix = consume_local_handshake_prefix(
            &item,
            &mut client,
            &batch.commands,
            "cluster",
            &mut protocol_version,
            &metrics,
        )
        .await?;
        // 当前 listener 的 backend 固定，只有 HELLO 改变 RESP 版本时才需要切换节点池队列。
        if protocol_version != previous_protocol_version {
            pool_handle = runtime.pipeline_pool.node_handle(
                &node.backend,
                item.backend_profile
                    .backend_protocol_version(protocol_version),
            );
        }
        if local_prefix == batch.commands.len() {
            if stage_profile_enabled {
                log_node_proxy_stage_profile(&item.name, &node.backend, &stage_profile);
            }
            continue;
        }
        let commands = &batch.commands[local_prefix..];
        let raw_start = batch.command_spans[local_prefix].start;
        let raw = batch.raw.slice(raw_start..);

        if let Some(responses) = read_only_batch_response(&item, commands) {
            let _pending_pipeline = metrics.start_pending_pipeline(commands.len());
            let bytes_to_write = response_frames_bytes(&responses);
            metrics.record_pipeline_result(
                commands.len(),
                bytes_to_write,
                responses.len(),
                0,
                false,
            );
            let _pending = metrics.start_pending_response(bytes_to_write);
            write_response_frames(&mut client, &responses).await?;
            continue;
        }

        let classify_started = stage_profile_enabled.then(Instant::now);
        let plan = classify_node_proxy_batch(commands);
        if let Some(started) = classify_started {
            stage_profile.classify_micros = duration_micros_u64(started.elapsed());
        }

        if let Some(tunnel_idx) = plan.tunnel_idx {
            if tunnel_idx > 0 {
                let absolute_tunnel_idx = local_prefix + tunnel_idx;
                let prefix_raw = batch
                    .raw
                    .slice(raw_start..batch.command_spans[absolute_tunnel_idx].start);
                let _pending_pipeline = metrics.start_pending_pipeline(tunnel_idx);
                let started = Instant::now();
                let execution = execute_session_request_response(
                    &pool_handle,
                    &commands[..tunnel_idx],
                    prefix_raw,
                )
                .await;
                let elapsed_micros = duration_micros_u64(started.elapsed());
                let execution_error = execution.is_err();
                metrics.record_backend_forward_result(tunnel_idx, elapsed_micros, execution_error);
                let mut responses = match execution {
                    Ok(responses) => runtime.rewrite_response_batch(
                        &commands[..tunnel_idx],
                        responses,
                        plan.needs_topology_rewrite,
                    ),
                    Err(err) => {
                        warn!(
                            proxy = %item.name,
                            backend = %node.backend,
                            error = %err,
                            "cluster-node-proxy pre-tunnel pipeline failed"
                        );
                        BackendResponseBatch::Frames(vec![
                            resp::err(&format!(
                                "cluster-node-proxy backend error: {err:#}"
                            ));
                            tunnel_idx
                        ])
                    }
                };
                let response_limit = enforce_backend_response_batch_limit(
                    &mut responses,
                    item.pending_response_bytes,
                );
                if response_limit.limit_hit {
                    metrics.record_pending_response_limit_hit();
                    warn!(
                        proxy = %item.name,
                        backend = %node.backend,
                        response_bytes = response_limit.original_bytes,
                        limit = item.pending_response_bytes,
                        "cluster-node-proxy pending response bytes limit hit"
                    );
                }
                metrics.record_pipeline_result(
                    tunnel_idx,
                    response_limit.bytes_to_write,
                    response_limit.error_count,
                    elapsed_micros,
                    execution_error,
                );
                let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
                write_backend_response_batch(&mut client, &responses).await?;
                if execution_error {
                    return Ok(());
                }
            }

            let absolute_tunnel_idx = local_prefix + tunnel_idx;
            let tunnel_start = batch.command_spans[absolute_tunnel_idx].start;
            let tunnel_raw = batch.raw.slice(tunnel_start..);
            let tunnel_command_count = commands.len() - tunnel_idx;
            let _tunnel_permit = match tunnel_limiter.acquire(&metrics).await {
                Ok(permit) => permit,
                Err(err) => {
                    write_dedicated_tunnel_limit_error(
                        &mut client,
                        &item,
                        &metrics,
                        tunnel_command_count,
                        err,
                    )
                    .await?;
                    return Ok(());
                }
            };
            let pending_pipeline = metrics.start_pending_pipeline(tunnel_command_count);
            let started = Instant::now();
            let mut tunnel_lease = match acquire_cluster_node_tunnel_lease(
                &item,
                &node,
                &tunnel_pool,
                &metrics,
                item.backend_profile
                    .backend_protocol_version(protocol_version),
                &tunnel_raw,
            )
            .await
            {
                Ok(lease) => lease,
                Err(err) => {
                    let elapsed_micros = duration_micros_u64(started.elapsed());
                    warn!(
                        proxy = %item.name,
                        backend = %node.backend,
                        error = %err,
                        "cluster-node-proxy tunnel backend acquire/write failed"
                    );
                    write_tunnel_backend_error(
                        &mut client,
                        &metrics,
                        tunnel_command_count,
                        elapsed_micros,
                        format!("cluster-node-proxy backend error: {err:#}"),
                    )
                    .await?;
                    return Ok(());
                }
            };

            // 只有单条且结束后能证明连接无状态的阻塞命令可以复用；批量尾部可能改变
            // 后端会话状态，因此即便首条是 XREAD/BLPOP 也必须走 tainted relay。
            let reusable_blocking = tunnel_idx == 0
                && commands.len() == 1
                && command_is_reusable_blocking(&commands[0]);
            if reusable_blocking {
                let response = match wait_reusable_blocking_response(
                    &item,
                    &mut client,
                    &mut buf,
                    &mut tmp,
                    &mut shutdown,
                    &mut tunnel_lease,
                    BlockingRouteContext {
                        backend_addr: &node.backend,
                        backend_change: None,
                    },
                )
                .await
                {
                    Ok(Some(response)) => response,
                    Ok(None) => return Ok(()),
                    Err(err) => {
                        let elapsed_micros = duration_micros_u64(started.elapsed());
                        warn!(
                            proxy = %item.name,
                            backend = %node.backend,
                            error = %err,
                            "cluster-node-proxy reusable blocking tunnel read failed"
                        );
                        write_tunnel_backend_error(
                            &mut client,
                            &metrics,
                            tunnel_command_count,
                            elapsed_micros,
                            format!("cluster-node-proxy backend error: {err:#}"),
                        )
                        .await?;
                        return Ok(());
                    }
                };
                let elapsed_micros = duration_micros_u64(started.elapsed());
                let response_bytes = response.bytes.len();
                metrics.record_pipeline_result(
                    tunnel_command_count,
                    response_bytes,
                    usize::from(response.is_error),
                    elapsed_micros,
                    false,
                );
                finish_single_response_tunnel(
                    tunnel_lease,
                    &item.name,
                    &node.backend,
                    "cluster-node-proxy",
                );
                let _pending = metrics.start_pending_response(response_bytes);
                client.write_all(&response.bytes).await?;
                continue;
            }

            let elapsed_micros = duration_micros_u64(started.elapsed());
            metrics.record_pipeline_result(tunnel_command_count, 0, 0, elapsed_micros, false);

            // 状态型透明 relay 一旦开始就不再属于待执行 pipeline；其 lease 标记为 tainted，
            // relay 结束时直接关闭，禁止把 SELECT/SUBSCRIBE/MULTI 等会话状态带回池。
            drop(pending_pipeline);
            let backend = tunnel_lease.into_tainted_connection();
            let relay_parts = backend.into_relay_parts();
            let _backend_connection = relay_parts.backend_connection_guard;
            let pending_client_to_backend = std::mem::take(&mut buf);
            let (client_to_backend, backend_to_client) = copy_bidirectional_bounded_with_preface(
                client,
                relay_parts.stream,
                pending_client_to_backend,
                relay_parts.pending_response,
                shutdown,
                metrics.clone(),
                client_idle_timeout,
            )
            .await?;
            metrics.record_relay_bytes(client_to_backend, backend_to_client);
            debug!(
                proxy = %item.name,
                backend = %node.backend,
                client_to_backend,
                backend_to_client,
                "cluster-node-proxy switched to transparent tunnel"
            );
            return Ok(());
        }

        // QUIT 由 proxy 本地应答:先执行 QUIT 之前的前缀,回 +OK 后关闭;其后命令按 Redis 语义丢弃。
        let cut = plan.quit_idx.unwrap_or(commands.len());
        if cut == 0 {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        let exec_commands = &commands[..cut];
        let exec_raw = if cut == commands.len() {
            raw.clone()
        } else {
            batch
                .raw
                .slice(raw_start..batch.command_spans[local_prefix + cut].start)
        };

        let _pending_pipeline = metrics.start_pending_pipeline(exec_commands.len());
        let started = Instant::now();
        let execution = if dedicated_session_mode {
            execute_dedicated_session_request_response(
                &item,
                &node,
                &metrics,
                item.backend_profile
                    .backend_protocol_version(protocol_version),
                &mut dedicated_backend,
                exec_commands,
                exec_raw,
            )
            .await
        } else if stage_profile_enabled {
            match execute_session_request_response_profiled(&pool_handle, exec_commands, exec_raw)
                .await
            {
                Ok((responses, backend_profile)) => {
                    stage_profile.ordinary_mux_micros = backend_profile.ordinary_mux_micros;
                    stage_profile.pool_acquire_micros =
                        backend_profile.pipeline_pool.acquire_micros;
                    stage_profile.backend_write_micros =
                        backend_profile.pipeline_pool.backend_write_micros;
                    stage_profile.backend_read_micros =
                        backend_profile.pipeline_pool.backend_read_micros;
                    Ok(responses)
                }
                Err(err) => Err(err),
            }
        } else {
            execute_session_request_response(&pool_handle, exec_commands, exec_raw).await
        };
        let elapsed_micros = duration_micros_u64(started.elapsed());
        let execution_error = execution.is_err();
        if stage_profile_enabled {
            stage_profile.execute_micros = elapsed_micros;
            stage_profile.execution_error = execution_error;
        }
        metrics.record_backend_forward_result(exec_commands.len(), elapsed_micros, execution_error);
        let mut responses = match execution {
            Ok(responses) => {
                let rewrite_started = stage_profile_enabled.then(Instant::now);
                let responses = runtime.rewrite_response_batch(
                    exec_commands,
                    responses,
                    plan.needs_topology_rewrite,
                );
                if let Some(started) = rewrite_started {
                    stage_profile.rewrite_micros = duration_micros_u64(started.elapsed());
                }
                responses
            }
            Err(err) => {
                warn!(
                    proxy = %item.name,
                    backend = %node.backend,
                    error = %err,
                    "cluster-node-proxy backend pipeline failed"
                );
                BackendResponseBatch::Frames(vec![
                    resp::err(&format!(
                        "cluster-node-proxy backend error: {err:#}"
                    ));
                    exec_commands.len()
                ])
            }
        };
        let limit_started = stage_profile_enabled.then(Instant::now);
        let response_limit =
            enforce_backend_response_batch_limit(&mut responses, item.pending_response_bytes);
        if let Some(started) = limit_started {
            stage_profile.limit_micros = duration_micros_u64(started.elapsed());
        }
        if response_limit.limit_hit {
            metrics.record_pending_response_limit_hit();
            warn!(
                proxy = %item.name,
                backend = %node.backend,
                response_bytes = response_limit.original_bytes,
                limit = item.pending_response_bytes,
                "cluster-node-proxy pending response bytes limit hit"
            );
        }
        metrics.record_pipeline_result(
            exec_commands.len(),
            response_limit.bytes_to_write,
            response_limit.error_count,
            elapsed_micros,
            execution_error,
        );
        let _pending = metrics.start_pending_response(response_limit.bytes_to_write);
        if stage_profile_enabled {
            stage_profile.response_bytes = response_limit.bytes_to_write;
            stage_profile.error_count = response_limit.error_count;
        }
        let writeback_started = stage_profile_enabled.then(Instant::now);
        write_backend_response_batch(&mut client, &responses).await?;
        if let Some(started) = writeback_started {
            stage_profile.writeback_micros = duration_micros_u64(started.elapsed());
        }
        if stage_profile_enabled {
            log_node_proxy_stage_profile(&item.name, &node.backend, &stage_profile);
        }
        if plan.quit_idx.is_some() {
            write_response_frames(&mut client, &[QUIT_OK_RESPONSE]).await?;
            return Ok(());
        }
        if execution_error {
            return Ok(());
        }
    }
}

/// 业务作用: NARIX_V1_NODE_PROXY_DEDICATED=1 时，普通批次改用 session 专属后端连接， 用于受控比较「连接池借还」与「连接亲和」的吞吐差异；默认生产路径仍使用有界连接池。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `node`: 当前专用连接固定使用的 cluster-node-proxy 节点映射配置。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `dedicated`: 专用隧道资源控制器，用于限制连接绑定命令占用的后端连接。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
///
/// 返回: 返回与请求顺序一致的后端响应批次；连接池、I/O 或协议失败时返回错误。
// NARIX_V1_NODE_PROXY_DEDICATED=1 时，普通批次改用 session 专属后端连接，
// 用于受控比较「连接池借还」与「连接亲和」的吞吐差异；默认生产路径仍使用有界连接池。
async fn execute_dedicated_session_request_response(
    item: &ProxyItem,
    node: &ClusterNodeProxyItem,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
    dedicated: &mut Option<BackendConnection>,
    commands: &[ClientCommand],
    raw_pipeline: Bytes,
) -> anyhow::Result<BackendResponseBatch> {
    if dedicated.is_none() {
        *dedicated =
            Some(connect_cluster_node_proxy_backend(item, node, metrics, protocol_version).await?);
    }
    let Some(conn) = dedicated.as_mut() else {
        unreachable!("dedicated backend connection was just initialized");
    };
    match conn
        .send_raw_pipeline_joined(raw_pipeline, commands.len())
        .await
    {
        Ok(responses) => Ok(BackendResponseBatch::Joined(responses)),
        Err(err) => {
            let _ = dedicated.take();
            Err(err)
        }
    }
}

/// 业务作用: 单命令与多命令统一走 session 缓存的 pool 句柄。session 任务直接完成后端 I/O， 避免额外的 channel/worker/oneshot 调度跳转；连接总数仍由 pipeline_backend_max_active 限制。
/// 参数说明:
/// - `pool_handle`: 连接池任务控制句柄，用于关闭后台维护任务并等待退出。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
///
/// 返回: 返回与请求顺序一致的后端响应批次；连接池、I/O 或协议失败时返回错误。
// 单命令与多命令统一走 session 缓存的 pool 句柄。session 任务直接完成后端 I/O，
// 避免额外的 channel/worker/oneshot 调度跳转；连接总数仍由 pipeline_backend_max_active 限制。
async fn execute_session_request_response(
    pool_handle: &NodePipelineHandle,
    commands: &[ClientCommand],
    raw_pipeline: Bytes,
) -> anyhow::Result<BackendResponseBatch> {
    let responses = pool_handle
        .send_raw_pipeline_joined(raw_pipeline, commands.len())
        .await?;
    Ok(BackendResponseBatch::Joined(responses))
}

/// 业务作用: 执行普通 request/response 批次并返回分段耗时，用于 stage profile 下的 session 热路径。
/// 参数说明:
/// - `pool_handle`: 连接池任务控制句柄，用于关闭后台维护任务并等待退出。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
///
/// 返回: 返回响应批次和执行分段耗时；连接池、I/O 或协议失败时返回错误。
// 执行普通 request/response 批次并返回分段耗时，用于 stage profile 下的 session 热路径。
async fn execute_session_request_response_profiled(
    pool_handle: &NodePipelineHandle,
    commands: &[ClientCommand],
    raw_pipeline: Bytes,
) -> anyhow::Result<(BackendResponseBatch, NodeProxyBackendProfile)> {
    let (responses, pipeline_pool) = pool_handle
        .send_raw_pipeline_joined_profiled(raw_pipeline, commands.len())
        .await?;
    Ok((
        BackendResponseBatch::Joined(responses),
        NodeProxyBackendProfile {
            pipeline_pool,
            ..Default::default()
        },
    ))
}

/// 业务作用: 从 standalone proxy 自己的后端专属池借出或新建一条 tunnel 连接，并发送初始命令。
///
/// 参数说明:
/// - `item`: 当前 standalone proxy 的配置；认证、超时和 keepalive 只取自本 proxy。
/// - `runtime`: 当前 standalone proxy 私有运行时；其中的池绝不与其他 proxy 共享。
/// - `metrics`: 当前 proxy 的指标句柄，用于记录新建连接和池命中情况。
/// - `protocol_version`: 前端已协商的 RESP 版本；不同版本的连接严格分队列复用。
/// - `raw_pipeline`: 从前端批次切出的 tunnel 原始字节；直接写出，不重新编码或复制。
///
/// 返回:
/// - 成功时返回已经写入初始命令的独占 lease。
/// - 建连、认证、协议同步或写入失败时返回带 standalone 后端上下文的错误。
///
/// 关键步骤:
/// 1. 只访问 `runtime.tunnel_pool`，从结构上保证不同 proxy 不会串池。
/// 2. 池未命中时才创建连接；普通 SET/GET 路径不会调用本函数。
/// 3. 命令写入后不自动归池，必须由上层消费响应并证明 clean。
async fn acquire_standalone_tunnel_lease(
    item: &ProxyItem,
    runtime: &StandaloneRuntime,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
    raw_pipeline: &[u8],
) -> anyhow::Result<TunnelLease> {
    let mut lease = match runtime.tunnel_pool.acquire_idle(protocol_version) {
        Some(lease) => lease,
        None => {
            let backend =
                connect_standalone_backend(item, &runtime.backend, metrics, protocol_version)
                    .await?;
            runtime.tunnel_pool.lease_new(protocol_version, backend)
        }
    };
    lease
        .connection_mut()
        .write_raw_pipeline(raw_pipeline)
        .await
        .with_context(|| {
            format!(
                "proxy `{}` write standalone dedicated tunnel `{}`",
                item.name, runtime.backend
            )
        })?;
    Ok(lease)
}

/// 业务作用: 从 cluster-node proxy 当前 1:1 节点自己的池借出或新建 tunnel，并发送初始命令。
///
/// 参数说明:
/// - `item`: 当前 cluster-node proxy 的配置；不同 proxy 之间不会共享连接。
/// - `node`: 当前监听器固定映射的 1:1 源节点，决定新连接的唯一后端地址。
/// - `tunnel_pool`: 会话启动时按 `node.backend` 解析出的节点私有池。
/// - `metrics`: 当前 proxy 的指标句柄，用于记录新建连接和池命中情况。
/// - `protocol_version`: 前端已协商的 RESP 版本；不同版本的连接严格分队列复用。
/// - `raw_pipeline`: 从前端批次切出的 tunnel 原始字节；直接写出，不重新编码或复制。
///
/// 返回:
/// - 成功时返回已经写入初始命令的独占 lease。
/// - 建连、认证、协议同步或写入失败时返回包含 proxy 与节点地址的错误。
///
/// 关键步骤:
/// 1. 调用方传入的 pool 已绑定当前 `node.backend`，运行时不再执行全局池查找。
/// 2. 该节点池耗尽时只为同一节点新建连接，绝不借用其他 1:1 节点的 idle socket。
/// 3. 命令写入后 lease 默认 tainted，只有上层明确证明 clean 才可归还本节点池。
async fn acquire_cluster_node_tunnel_lease(
    item: &ProxyItem,
    node: &ClusterNodeProxyItem,
    tunnel_pool: &Arc<TunnelPool>,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
    raw_pipeline: &[u8],
) -> anyhow::Result<TunnelLease> {
    let mut lease = match tunnel_pool.acquire_idle(protocol_version) {
        Some(lease) => lease,
        None => {
            let backend =
                connect_cluster_node_proxy_backend(item, node, metrics, protocol_version).await?;
            tunnel_pool.lease_new(protocol_version, backend)
        }
    };
    lease
        .connection_mut()
        .write_raw_pipeline(raw_pipeline)
        .await
        .with_context(|| {
            format!(
                "proxy `{}` write cluster-node dedicated tunnel `{}`",
                item.name, node.backend
            )
        })?;
    Ok(lease)
}

/// 业务作用: 在读取唯一响应后完成 tunnel lease 的最终 clean/tainted 判定。
///
/// 参数说明:
/// - `lease`: 已完整读取一条 RESP 响应的独占后端连接。
/// - `proxy_name`: 当前 proxy 配置名，用于异常日志精确定位池所有者。
/// - `backend`: lease 绑定的唯一真实后端地址。
/// - `tunnel_kind`: standalone、cluster 初始路由、redirect 或 node-proxy 等路径标签。
///
/// 返回:
/// - 无返回值；无缓冲尾部时显式 clean 归池，否则保持默认 tainted 并关闭。
///
/// 关键步骤:
/// 1. 只检查连接内部已经多读到的响应尾部，不额外执行 socket read 或协议探测。
/// 2. 任意尾部字节都表示“唯一响应”假设无法证明，必须 fail-closed 丢弃连接。
/// 3. clean 归还仍回到 lease 原始 pool，因此不会因日志参数或调用路径跨 proxy/node 串池。
fn finish_single_response_tunnel(
    mut lease: TunnelLease,
    proxy_name: &str,
    backend: &str,
    tunnel_kind: &str,
) {
    if lease.connection_mut().has_buffered_response_bytes() {
        warn!(
            proxy = %proxy_name,
            backend,
            tunnel_kind,
            "blocking response left buffered backend bytes; discarding tunnel"
        );
        drop(lease);
    } else {
        lease.release_clean();
    }
}

/// 业务作用: 等待一条可复用阻塞命令的唯一后端响应，同时监视前端断连、积压和进程关闭。
///
/// 参数说明:
/// - `item`: 当前 proxy 配置，用于前端空闲时限和积压字节上限校验。
/// - `client`: 当前前端 TCP 连接；等待后端期间仍需检测断连和后续 pipeline 字节。
/// - `client_buffer`: 当前会话尚未解析的前端尾部；新到字节按原顺序追加。
/// - `client_read_buffer`: 会话级复用的前端读取工作区，避免阻塞轮次重复分配。
/// - `shutdown`: 当前 proxy 的关闭信号；触发后立即放弃 lease 并结束会话。
/// - `lease`: 已写入单条阻塞命令的独占后端连接。
/// - `route`: 当前 lease 的后端地址和可选动态路由观察器。
///
/// 返回:
/// - `Ok(Some(response))`: 已完整读取唯一 RESP 帧，可继续执行 clean 判定。
/// - `Ok(None)`: 前端关闭、空闲超时或进程关闭；调用方直接结束会话并丢弃连接。
/// - `Err`: 前端读、后端读、RESP 解析或积压上限校验失败。
///
/// 关键步骤:
/// 1. 后端读取与前端存活检测并行等待，避免 `XREAD BLOCK 0` 在客户端消失后长期占连接。
/// 2. 前端追加命令只进入会话缓冲，不会越过当前阻塞命令提前发往后端。
/// 3. `BackendConnection` 自己保留半帧字节；select 取消后下一轮继续解析，不丢数据。
async fn wait_reusable_blocking_response(
    item: &ProxyItem,
    client: &mut TcpStream,
    client_buffer: &mut Vec<u8>,
    client_read_buffer: &mut [u8],
    shutdown: &mut watch::Receiver<bool>,
    lease: &mut TunnelLease,
    mut route: BlockingRouteContext<'_>,
) -> anyhow::Result<Option<BackendSingleResponse>> {
    let idle_timeout = optional_duration_ms(item.client_idle_timeout_ms);
    loop {
        if let Some(timeout) = idle_timeout {
            tokio::select! {
                response = lease.connection_mut().read_one_response_compact() => {
                    return response.map(Some);
                }
                read = client.read(client_read_buffer) => {
                    let read = read?;
                    if read == 0 {
                        return Ok(None);
                    }
                    client_buffer.extend_from_slice(&client_read_buffer[..read]);
                    if client_buffer.len() > item.pipeline_max_bytes {
                        bail!(
                            "client pipeline buffer exceeds limit while blocking: {} > {}",
                            client_buffer.len(),
                            item.pipeline_max_bytes
                        );
                    }
                }
                _ = tokio::time::sleep(timeout) => {
                    return Ok(None);
                }
                _ = shutdown.changed() => {
                    return Ok(None);
                }
                reason = wait_dynamic_backend_change(&mut route.backend_change, route.backend_addr) => {
                    info!(
                        proxy = %item.name,
                        backend = %route.backend_addr,
                        reason = %reason,
                        "closing reusable blocking command because dynamic backend route changed"
                    );
                    return Ok(None);
                }
            }
        } else {
            tokio::select! {
                response = lease.connection_mut().read_one_response_compact() => {
                    return response.map(Some);
                }
                read = client.read(client_read_buffer) => {
                    let read = read?;
                    if read == 0 {
                        return Ok(None);
                    }
                    client_buffer.extend_from_slice(&client_read_buffer[..read]);
                    if client_buffer.len() > item.pipeline_max_bytes {
                        bail!(
                            "client pipeline buffer exceeds limit while blocking: {} > {}",
                            client_buffer.len(),
                            item.pipeline_max_bytes
                        );
                    }
                }
                _ = shutdown.changed() => {
                    return Ok(None);
                }
                reason = wait_dynamic_backend_change(&mut route.backend_change, route.backend_addr) => {
                    info!(
                        proxy = %item.name,
                        backend = %route.backend_addr,
                        reason = %reason,
                        "closing reusable blocking command because dynamic backend route changed"
                    );
                    return Ok(None);
                }
            }
        }
    }
}

/// 业务作用: 等待动态路由明确要求关闭当前旧后端连接。
///
/// 参数说明：
/// - `backend_change`：可选 replication HA 或 Sentinel 路由接收器；固定路由模式为空。
/// - `old_backend`：当前阻塞命令实际占用的后端地址。
///
/// 返回：返回必须关闭旧连接的原因；没有动态路由时永久等待，由外层 select 取消。
async fn wait_dynamic_backend_change(
    backend_change: &mut Option<DynamicRoutingReceiver>,
    old_backend: &str,
) -> String {
    let Some(receiver) = backend_change.as_mut() else {
        return std::future::pending::<String>().await;
    };
    loop {
        if let Some(reason) = receiver.close_reason(old_backend) {
            return reason;
        }
        if receiver.changed().await.is_err() {
            return "dynamic backend route watcher closed".to_string();
        }
    }
}

/// 业务作用: 建立 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `backend_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
///
/// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
// 创建 standalone dedicated backend，用于 Pub/Sub、blocking、事务和连接状态命令切换透明隧道。
async fn connect_standalone_backend(
    item: &ProxyItem,
    backend_addr: &str,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
) -> anyhow::Result<BackendConnection> {
    let (username, password) = backend_auth_parts(item);
    let tls = BackendTlsConfig::for_backend(item);
    let mut backend = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: backend_addr,
        username,
        password,
        connect_timeout_ms: item.connect_timeout_ms,
        request_timeout_ms: item.backend_request_timeout_ms,
        tcp_keepalive_ms: item.tcp_keepalive_ms,
        response_bytes_limit: Some(item.pending_response_bytes),
        metrics: Some(metrics.clone()),
        tls: tls.as_ref(),
    })
    .await
    .with_context(|| {
        format!(
            "proxy `{}` connect standalone dedicated backend `{backend_addr}`",
            item.name
        )
    })?;
    if let Some(protocol_version) = item
        .backend_profile
        .backend_protocol_version(protocol_version)
    {
        backend
            .set_protocol_version(protocol_version)
            .await
            .with_context(|| {
                format!(
                    "proxy `{}` sync RESP protocol with standalone dedicated backend `{backend_addr}`",
                    item.name
                )
            })?;
    }
    Ok(backend)
}

/// 业务作用: 建立 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `node`: 提供真实后端地址的 cluster-node-proxy 节点映射配置。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
///
/// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
// 创建 cluster-node-proxy dedicated backend，用于 Pub/Sub、blocking、事务和连接状态命令切换透明隧道。
async fn connect_cluster_node_proxy_backend(
    item: &ProxyItem,
    node: &ClusterNodeProxyItem,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
) -> anyhow::Result<BackendConnection> {
    let (username, password) = backend_auth_parts(item);
    let tls = BackendTlsConfig::for_backend(item);
    let mut backend = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: &node.backend,
        username,
        password,
        connect_timeout_ms: item.connect_timeout_ms,
        request_timeout_ms: item.backend_request_timeout_ms,
        tcp_keepalive_ms: item.tcp_keepalive_ms,
        response_bytes_limit: Some(item.pending_response_bytes),
        metrics: Some(metrics.clone()),
        tls: tls.as_ref(),
    })
    .await
    .with_context(|| {
        format!(
            "proxy `{}` connect cluster-node-proxy dedicated backend `{}`",
            item.name, node.backend
        )
    })?;
    if let Some(protocol_version) = item
        .backend_profile
        .backend_protocol_version(protocol_version)
    {
        backend
            .set_protocol_version(protocol_version)
            .await
            .with_context(|| {
                format!(
                    "proxy `{}` sync RESP protocol with dedicated backend `{}`",
                    item.name, node.backend
                )
            })?;
    }
    Ok(backend)
}

/// 业务作用: 建立 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `backend_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
///
/// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
// 创建 replication-ha 只读解析路径的后端 request/response 连接。
async fn connect_replication_ha_request_response_backend(
    item: &ProxyItem,
    backend_addr: &str,
    metrics: &ProxyMetrics,
    protocol_version: Option<u8>,
) -> anyhow::Result<BackendConnection> {
    let (username, password) = backend_auth_parts(item);
    let tls = BackendTlsConfig::for_backend(item);
    let mut backend = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: backend_addr,
        username,
        password,
        connect_timeout_ms: item.connect_timeout_ms,
        request_timeout_ms: item.backend_request_timeout_ms,
        tcp_keepalive_ms: item.tcp_keepalive_ms,
        response_bytes_limit: Some(item.pending_response_bytes),
        metrics: Some(metrics.clone()),
        tls: tls.as_ref(),
    })
    .await
    .with_context(|| {
        format!(
            "proxy `{}` connect replication-ha readonly backend `{backend_addr}`",
            item.name
        )
    })?;
    if let Some(protocol_version) = protocol_version {
        backend
            .set_protocol_version(protocol_version)
            .await
            .with_context(|| {
                format!(
                    "proxy `{}` sync RESP protocol with replication-ha readonly backend `{backend_addr}`",
                    item.name
                )
            })?;
    }
    Ok(backend)
}

/// 业务作用: 生成 dedicated backend 的认证参数，用于让 pass-through 模式保持客户端认证原样透传。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
// 生成 dedicated backend 的认证参数，用于让 pass-through 模式保持客户端认证原样透传。
fn backend_auth_parts(item: &ProxyItem) -> (Option<&str>, Option<&str>) {
    if item.effective_auth_mode() == AuthMode::PassThrough {
        (None, None)
    } else {
        (item.username.as_deref(), item.password.as_deref())
    }
}

/// 业务作用: 按连续或分帧形态写回普通后端响应批次，保持原始 RESP 顺序。
/// 参数说明:
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `batch`: 当前待执行的命令批次，必须保持批内顺序和统一错误边界。
///
/// 返回: 返回与请求顺序一致的后端响应批次；连接池、I/O 或协议失败时返回错误。
// 批量写出 cluster pipeline 响应，用于减少大量小响应逐帧 write_all 带来的系统调用开销。
async fn write_backend_response_batch<W>(
    writer: &mut W,
    batch: &BackendResponseBatch,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match batch {
        BackendResponseBatch::Frames(frames) => write_response_frames(writer, frames).await,
        BackendResponseBatch::Joined(joined) => writer.write_all(&joined.bytes).await,
    }
}

/// 业务作用: 将 Cluster pipeline 聚合结果按原前端命令顺序完整写回客户端。
/// 参数说明:
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `batch`: 当前待执行的命令批次，必须保持批内顺序和统一错误边界。
///
/// 返回: 全部字节按原顺序写出时返回成功；客户端断连或底层写入失败时返回 I/O 错误。
// 批量写出 cluster-single-entry 响应；Raw 热路径一次写出连续 response buffer。
async fn write_cluster_pipeline_response<W>(
    writer: &mut W,
    batch: &ClusterPipelineResponse,
) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    match batch {
        ClusterPipelineResponse::Frames(frames) => write_response_frames(writer, frames).await,
        ClusterPipelineResponse::Raw(raw) => writer.write_all(&raw.bytes).await,
    }
}

/// 业务作用: 顺序写回一组独立 RESP 帧，并在任一 I/O 失败时停止当前批次。
/// 参数说明:
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `chunks`: 拆分后的协议数据块集合，按原始顺序写入以保持帧边界。
///
/// 返回: 全部字节按原顺序写出时返回成功；客户端断连或底层写入失败时返回 I/O 错误。
// 批量写出 cluster pipeline 响应，用于减少大量小响应逐帧 write_all 带来的系统调用开销。
async fn write_response_frames<W, C>(writer: &mut W, chunks: &[C]) -> io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
    C: AsRef<[u8]>,
{
    let mut chunk_idx = 0;
    let mut offset = 0;

    loop {
        while chunk_idx < chunks.len() && offset >= chunks[chunk_idx].as_ref().len() {
            chunk_idx += 1;
            offset = 0;
        }
        if chunk_idx >= chunks.len() {
            return Ok(());
        }

        let mut slices = Vec::with_capacity(MAX_RESPONSE_VECTORED_SLICES);
        let first = chunks[chunk_idx].as_ref();
        slices.push(IoSlice::new(&first[offset..]));
        for chunk in chunks.iter().skip(chunk_idx + 1) {
            if slices.len() == MAX_RESPONSE_VECTORED_SLICES {
                break;
            }
            let bytes = chunk.as_ref();
            if !bytes.is_empty() {
                slices.push(IoSlice::new(bytes));
            }
        }

        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "failed to write vectored Redis responses",
            ));
        }

        let mut remaining = written;
        while chunk_idx < chunks.len() {
            let available = chunks[chunk_idx].as_ref().len().saturating_sub(offset);
            if remaining < available {
                offset += remaining;
                break;
            }
            remaining -= available;
            chunk_idx += 1;
            offset = 0;
            if remaining == 0 {
                break;
            }
        }
    }
}

/// 业务作用: 把数据面阶段耗时饱和转换为 u64 微秒指标。
/// 参数说明:
/// - `duration`: 时间跨度，用于统计窗口、等待上限或吞吐计算。
///
fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

/// 业务作用: 判断是否启用节点代理热路径的分阶段耗时采样；该诊断默认关闭，避免生产请求承担计时和输出开销。
/// 参数说明: 无。
/// 返回: 专用环境变量取明确真值时返回 `true`；结果在进程内只读取一次并缓存，防止每批命令重复访问环境变量。
fn node_proxy_stage_profile_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("NARIX_V1_STAGE_PROFILE").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        )
    })
}

/// 业务作用: 诊断开关：让 node-proxy 普通批次改用 session 专属连接，以受控比较连接池借还成本。
/// 参数说明: 无。
// 诊断开关：让 node-proxy 普通批次改用 session 专属连接，以受控比较连接池借还成本。
fn node_proxy_dedicated_session_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        matches!(
            std::env::var("NARIX_V1_NODE_PROXY_DEDICATED").as_deref(),
            Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
        )
    })
}

/// 业务作用: 聚合节点代理多个批次在解析、分类、后端 I/O、改写和写回阶段的累计耗时，供受控性能诊断定位瓶颈。
/// 字段说明: 所有计数只做诊断性宽松原子累加，不参与路由或正确性判断；每 256 批才输出一次以降低测量扰动。
#[derive(Debug, Default)]
// 表示 stage profile 的进程级累计聚合，用于避免热路径逐批 eprintln 干扰测量。
struct NodeProxyStageAggregate {
    batches: AtomicU64,
    commands: AtomicU64,
    read_parse: AtomicU64,
    classify: AtomicU64,
    execute: AtomicU64,
    pool_acquire: AtomicU64,
    backend_write: AtomicU64,
    backend_read: AtomicU64,
    ordinary_mux: AtomicU64,
    rewrite: AtomicU64,
    limit: AtomicU64,
    writeback: AtomicU64,
}

/// 业务作用: 把单批节点代理分阶段耗时累加到进程级诊断快照，并按固定批次间隔输出一行汇总，避免逐请求输出扭曲性能数据。
/// 参数说明:
/// - `proxy`: 产生本批数据的代理项名称，用于区分同一进程中的多个业务监听器。
/// - `backend`: 当前节点代理绑定的真实后端地址，用于把耗时归属到具体 Redis Cluster 节点。
/// - `stage`: 当前批次已经采集的解析、分类、执行、I/O、改写、限额和写回耗时。
///
/// 返回: 无显式返回值；更新诊断计数，并在累计批次数为 256 的整数倍时输出汇总。
fn log_node_proxy_stage_profile(proxy: &str, backend: &str, stage: &NodeProxyStageProfile) {
    static AGG: OnceLock<NodeProxyStageAggregate> = OnceLock::new();
    let agg = AGG.get_or_init(NodeProxyStageAggregate::default);
    agg.commands
        .fetch_add(stage.command_count as u64, Ordering::Relaxed);
    agg.read_parse
        .fetch_add(stage.read_parse_micros, Ordering::Relaxed);
    agg.classify
        .fetch_add(stage.classify_micros, Ordering::Relaxed);
    agg.execute
        .fetch_add(stage.execute_micros, Ordering::Relaxed);
    agg.pool_acquire
        .fetch_add(stage.pool_acquire_micros, Ordering::Relaxed);
    agg.backend_write
        .fetch_add(stage.backend_write_micros, Ordering::Relaxed);
    agg.backend_read
        .fetch_add(stage.backend_read_micros, Ordering::Relaxed);
    agg.ordinary_mux
        .fetch_add(stage.ordinary_mux_micros, Ordering::Relaxed);
    agg.rewrite
        .fetch_add(stage.rewrite_micros, Ordering::Relaxed);
    agg.limit.fetch_add(stage.limit_micros, Ordering::Relaxed);
    agg.writeback
        .fetch_add(stage.writeback_micros, Ordering::Relaxed);
    let batches = agg.batches.fetch_add(1, Ordering::Relaxed) + 1;
    if !batches.is_multiple_of(256) {
        return;
    }
    eprintln!(
        "[stage-agg] proxy={} backend={} batches={} commands={} read_parse_us={} classify_us={} execute_us={} pool_acquire_us={} backend_write_us={} backend_read_us={} ordinary_mux_us={} rewrite_us={} limit_us={} writeback_us={}",
        proxy,
        backend,
        batches,
        agg.commands.load(Ordering::Relaxed),
        agg.read_parse.load(Ordering::Relaxed),
        agg.classify.load(Ordering::Relaxed),
        agg.execute.load(Ordering::Relaxed),
        agg.pool_acquire.load(Ordering::Relaxed),
        agg.backend_write.load(Ordering::Relaxed),
        agg.backend_read.load(Ordering::Relaxed),
        agg.ordinary_mux.load(Ordering::Relaxed),
        agg.rewrite.load(Ordering::Relaxed),
        agg.limit.load(Ordering::Relaxed),
        agg.writeback.load(Ordering::Relaxed)
    );
}

/// 业务作用: 把零值禁用语义的毫秒配置转换为可选 Duration。
/// 参数说明:
/// - `ms`: 毫秒值，`0` 表示禁用对应超时或空闲回收配置。
///
fn optional_duration_ms(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

/// 业务作用: 对已连接 socket 应用 TCP_NODELAY 与可选 keepalive。
/// 参数说明:
/// - `stream`: 已建立的 TCP 字节流，函数在其上执行协议收发或透明转发。
/// - `keepalive_ms`: TCP keepalive 配置毫秒数，用于尽早发现半开连接。
/// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
/// - `side`: 当前处理的数据方向或拓扑侧，用于选择指标和错误标签。
///
fn configure_tcp_stream(stream: &TcpStream, keepalive_ms: u64, proxy_name: &str, side: &str) {
    if let Err(err) = stream.set_nodelay(true) {
        warn!(proxy = %proxy_name, side, error = %err, "failed to set TCP_NODELAY");
    }
    if let Some(duration) = optional_duration_ms(keepalive_ms) {
        let keepalive = TcpKeepalive::new().with_time(duration);
        if let Err(err) = SockRef::from(stream).set_tcp_keepalive(&keepalive) {
            warn!(proxy = %proxy_name, side, error = %err, "failed to set TCP keepalive");
        }
    }
}

/// 业务作用: 保存响应大小门禁结果及写回统计所需的字节数和错误数。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// 表示 PendingResponseCheck 的数据结构，用于处理前端连接、后端转发、限流和优雅关闭。
struct PendingResponseCheck {
    bytes_to_write: usize,
    original_bytes: usize,
    error_count: usize,
    limit_hit: bool,
}

/// 业务作用: 区分逐帧与连续 joined 后端响应，使限制校验和写回无需强制转换。
// 表示后端响应批次，Joined 用于高吞吐 pipeline 快路径避免 per-frame Vec 分配。
enum BackendResponseBatch {
    Frames(Vec<Vec<u8>>),
    Joined(BackendRawResponses),
}

/// 业务作用: 判断后端响应批次是否包含数据节点已经降级为只读副本的错误。
///
/// 参数说明:
/// - `responses`: 保持后端原始帧边界的普通或连续响应批次。
///
/// 返回:
/// - 任一帧以 Redis `-READONLY` 错误开头时返回 true，否则返回 false。
fn backend_batch_has_readonly_error(responses: &BackendResponseBatch) -> bool {
    match responses {
        BackendResponseBatch::Frames(frames) => {
            frames.iter().any(|frame| frame.starts_with(b"-READONLY"))
        }
        BackendResponseBatch::Joined(responses) => responses
            .frame_ranges
            .iter()
            .any(|range| responses.bytes[range.clone()].starts_with(b"-READONLY")),
    }
}

/// 业务作用: 在写回前校验响应总字节上限，超限时记录拒绝并关闭当前批次。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `limit`: 对应资源或数据规模的硬上限，超过时立即拒绝以保护内存和连接容量。
///
fn enforce_pending_response_limit(
    responses: &mut Vec<Vec<u8>>,
    limit: usize,
) -> PendingResponseCheck {
    let original_bytes = responses.iter().map(Vec::len).sum();
    if original_bytes <= limit {
        return PendingResponseCheck {
            bytes_to_write: original_bytes,
            original_bytes,
            error_count: responses
                .iter()
                .filter(|response| response.starts_with(b"-"))
                .count(),
            limit_hit: false,
        };
    }

    let count = responses.len();
    let err = resp::err(&format!(
        "proxy pending response bytes exceeds limit: {original_bytes} > {limit}"
    ));
    responses.clear();
    responses.resize(count, err);
    PendingResponseCheck {
        bytes_to_write: responses.iter().map(Vec::len).sum(),
        original_bytes,
        error_count: count,
        limit_hit: true,
    }
}

/// 业务作用: 对 cluster-single-entry 响应批次执行 pending response 限制，未超限时保持 joined 热路径形态。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `limit`: 对应资源或数据规模的硬上限，超过时立即拒绝以保护内存和连接容量。
///
// 对 cluster-single-entry 响应批次执行 pending response 限制，未超限时保持 joined 热路径形态。
fn enforce_cluster_pipeline_response_limit(
    responses: &mut ClusterPipelineResponse,
    limit: usize,
) -> PendingResponseCheck {
    let original_bytes = responses.bytes_to_write();
    if original_bytes <= limit {
        return PendingResponseCheck {
            bytes_to_write: original_bytes,
            original_bytes,
            error_count: responses.error_count(),
            limit_hit: false,
        };
    }

    let count = responses.len();
    let err = resp::err(&format!(
        "proxy pending response bytes exceeds limit: {original_bytes} > {limit}"
    ));
    let frames = vec![err; count];
    let bytes_to_write = frames.iter().map(Vec::len).sum();
    *responses = ClusterPipelineResponse::Frames(frames);
    PendingResponseCheck {
        bytes_to_write,
        original_bytes,
        error_count: count,
        limit_hit: true,
    }
}

/// 业务作用: 对后端响应批次执行 pending response 限制，Joined 未超限时保持连续 buffer 形态。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `limit`: 对应资源或数据规模的硬上限，超过时立即拒绝以保护内存和连接容量。
///
// 对后端响应批次执行 pending response 限制，Joined 未超限时保持连续 buffer 形态。
fn enforce_backend_response_batch_limit(
    responses: &mut BackendResponseBatch,
    limit: usize,
) -> PendingResponseCheck {
    match responses {
        BackendResponseBatch::Frames(frames) => enforce_pending_response_limit(frames, limit),
        BackendResponseBatch::Joined(joined) => {
            let original_bytes = joined.bytes.len();
            if original_bytes <= limit {
                return PendingResponseCheck {
                    bytes_to_write: original_bytes,
                    original_bytes,
                    error_count: joined.error_count,
                    limit_hit: false,
                };
            }

            let count = joined.len();
            let err = resp::err(&format!(
                "proxy pending response bytes exceeds limit: {original_bytes} > {limit}"
            ));
            let frames = vec![err; count];
            let bytes_to_write = frames.iter().map(Vec::len).sum();
            *responses = BackendResponseBatch::Frames(frames);
            PendingResponseCheck {
                bytes_to_write,
                original_bytes,
                error_count: count,
                limit_hit: true,
            }
        }
    }
}

/// 业务作用: 解析 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
/// 返回: 完整批次返回保序命令集合，半包返回空集合继续等待；协议或资源限制违规时返回错误。
// 解析 parse_client_batch 输入，用于支撑处理前端连接、后端转发、限流和优雅关闭。
fn parse_client_batch(buf: &mut Vec<u8>, item: &ProxyItem) -> anyhow::Result<Vec<ClientCommand>> {
    let mut parsed_commands = Vec::new();
    let mut pos = 0;

    while parsed_commands.len() < item.pipeline_max_commands {
        let Some(parsed) = resp::parse_command_ranges(&buf[pos..])? else {
            break;
        };
        let start = pos;
        let end = pos + parsed.consumed;
        validate_command_limits(&parsed.arg_ranges, parsed.consumed, item)?;
        if end > item.pipeline_max_bytes {
            bail!(
                "client pipeline batch exceeds limit: {end} > {}",
                item.pipeline_max_bytes
            );
        }
        if !parsed.arg_ranges.is_empty() {
            parsed_commands.push((start, end, parsed.arg_ranges));
        }
        pos = end;
        if pos == buf.len() {
            break;
        }
    }

    let mut commands = Vec::with_capacity(parsed_commands.len());
    if pos > 0 {
        let batch = parsed_batch_bytes(buf, pos);
        for (start, end, arg_ranges) in parsed_commands {
            commands.push(ClientCommand::new(batch.slice(start..end), arg_ranges));
        }
    }
    Ok(commands)
}

/// 业务作用: 解析 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
/// 返回: 返回连续原始 pipeline、命令元数据和字节区间；半包继续等待，协议或限制违规时返回错误。
// 解析 cluster-node-proxy 前端批次，用于保留连续原始 pipeline 并只记录命令元数据。
fn parse_node_proxy_batch(buf: &mut Vec<u8>, item: &ProxyItem) -> anyhow::Result<NodeProxyBatch> {
    let mut parsed_commands = Vec::new();
    let mut pos = 0;

    while parsed_commands.len() < item.pipeline_max_commands {
        let Some(parsed) = resp::parse_command_ranges(&buf[pos..])? else {
            break;
        };
        let start = pos;
        let end = pos + parsed.consumed;
        validate_command_limits(&parsed.arg_ranges, parsed.consumed, item)?;
        if end > item.pipeline_max_bytes {
            bail!(
                "client pipeline batch exceeds limit: {end} > {}",
                item.pipeline_max_bytes
            );
        }
        if !parsed.arg_ranges.is_empty() {
            parsed_commands.push((start, end, parsed.arg_ranges));
        }
        pos = end;
        if pos == buf.len() {
            break;
        }
    }

    let mut commands = Vec::with_capacity(parsed_commands.len());
    let mut command_spans = Vec::with_capacity(parsed_commands.len());
    let raw = if pos > 0 {
        let batch = parsed_batch_bytes(buf, pos);
        for (start, end, arg_ranges) in parsed_commands {
            command_spans.push(start..end);
            commands.push(ClientCommand::new(batch.slice(start..end), arg_ranges));
        }
        batch
    } else {
        Bytes::new()
    };

    Ok(NodeProxyBatch {
        raw,
        commands,
        command_spans,
    })
}

/// 业务作用: 解析 Redis 协议前端会话、透明隧道、pipeline 和代理数据面 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `len`: 当前数据、帧或参数的长度，用于容量检查和边界计算。
///
// 接管或复制已解析的前端批次，用于大 pipeline 避免把整段请求从 Vec 再复制到 Bytes。
fn parsed_batch_bytes(buf: &mut Vec<u8>, len: usize) -> Bytes {
    if len == buf.len() && len >= FRONTEND_ZERO_COPY_BATCH_MIN {
        Bytes::from(std::mem::take(buf))
    } else {
        let batch = Bytes::copy_from_slice(&buf[..len]);
        buf.drain(..len);
        batch
    }
}

/// 业务作用: 保存节点代理单批命令的本地握手前缀、QUIT 位置、隧道需求和响应改写门禁。
#[derive(Debug, Clone, Copy, Default)]
// 表示一批前端命令的执行计划，用于单遍扫描同时得出隧道切换点、QUIT 截断点和拓扑改写需求。
struct NodeProxyBatchPlan {
    // 第一个必须切透明隧道的命令下标;其后的原始字节随隧道转发。
    tunnel_idx: Option<usize>,
    // 第一个 QUIT 的下标;QUIT 由 proxy 本地回 +OK 并关闭连接,其后的命令按 Redis 语义丢弃。
    quit_idx: Option<usize>,
    // 被执行前缀里是否含拓扑命令(CLUSTER NODES/SLOTS/SHARDS),用于 joined response 改写门控。
    needs_topology_rewrite: bool,
}

/// 业务作用: 单遍扫描一批命令，用于把原先的 QUIT 扫描、隧道扫描和改写门控扫描合并成一次命令名匹配。
/// 参数说明:
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
// 单遍扫描一批命令，用于把原先的 QUIT 扫描、隧道扫描和改写门控扫描合并成一次命令名匹配。
fn classify_node_proxy_batch(commands: &[ClientCommand]) -> NodeProxyBatchPlan {
    let mut plan = NodeProxyBatchPlan::default();
    for (idx, command) in commands.iter().enumerate() {
        if command_name_eq(command, b"QUIT") {
            plan.quit_idx = Some(idx);
            return plan;
        }
        if node_proxy_command_requires_tunnel(command) {
            plan.tunnel_idx = Some(idx);
            return plan;
        }
        if command_rewrite_kind(command) != NodeProxyRewriteKind::None {
            plan.needs_topology_rewrite = true;
        }
    }
    plan
}

/// 业务作用: 表示代理本地完成的客户端兼容握手响应，避免纯元数据命令误切独占隧道。
#[derive(Debug)]
// 表示可由 proxy 本地消费的兼容握手响应。
struct LocalHandshakeResponse {
    /// 准备直接写回前端客户端的完整 RESP 响应帧。
    response: Vec<u8>,
    /// HELLO 本地响应协商出的 RESP 版本；其他兼容握手保持 None。
    protocol_version: Option<u8>,
}

/// 业务作用: 消费代理能够本地完成的连续握手前缀，并把这些命令计入总 OPS。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `hello_mode`: HELLO 响应中的服务模式，决定客户端采用单节点还是集群语义。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `metrics`: 指标采集对象；本地完成的握手不会经过普通 pipeline，必须在这里计数。
///
/// 返回: 输入可安全转换且未越界时返回数量；缺失、格式非法或溢出时返回错误。
// backend-inject 下本地完成 AUTH/HELLO；所有认证模式下本地完成 CLIENT SETINFO。
// 普通 SET/GET 批次不创建临时集合，只做与旧代码同量级的命令名判断。
async fn consume_local_handshake_prefix<W>(
    item: &ProxyItem,
    client: &mut W,
    commands: &[ClientCommand],
    hello_mode: &str,
    protocol_version: &mut Option<u8>,
    metrics: &ProxyMetrics,
) -> anyhow::Result<usize>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut consumed = 0usize;
    let mut response_bytes = 0usize;
    let mut error_count = 0usize;
    for command in commands {
        let Some(response) = local_handshake_response(item, command, hello_mode)? else {
            break;
        };
        client.write_all(&response.response).await?;
        response_bytes = response_bytes.saturating_add(response.response.len());
        error_count += usize::from(response.response.first() == Some(&b'-'));
        if let Some(version) = response.protocol_version {
            *protocol_version = Some(version);
        }
        consumed += 1;
    }
    if consumed > 0 {
        metrics.record_local_handshake_commands(consumed);
        metrics.record_pipeline_result(consumed, response_bytes, error_count, 0, false);
    }
    Ok(consumed)
}

/// 业务作用: 识别当前认证模式下可以在代理本地完成的客户端握手命令。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `hello_mode`: HELLO 响应中的服务模式，决定客户端采用单节点还是集群语义。
///
/// 返回: 需要代理本地终止时返回响应和协议变化，不适用时返回空；握手参数非法时返回错误。
// CLIENT SETINFO 只登记客户端库元数据，共享后端连接无法保留该属性；本地确认可以防止
// Lettuce 初始化连接误入 opaque tunnel。backend-inject 的 AUTH/HELLO 保持官方原有语义。
fn local_handshake_response(
    item: &ProxyItem,
    command: &ClientCommand,
    hello_mode: &str,
) -> anyhow::Result<Option<LocalHandshakeResponse>> {
    if command_name_eq(command, b"CLIENT")
        && command
            .arg(1)
            .is_some_and(|arg| arg.eq_ignore_ascii_case(b"SETINFO"))
    {
        return Ok(Some(client_setinfo_local_response(command)));
    }
    if item.effective_auth_mode() == AuthMode::BackendInject {
        if command_name_eq(command, b"AUTH") {
            return Ok(Some(backend_inject_auth_response(item, command)));
        }
        if command_name_eq(command, b"HELLO") {
            return Ok(Some(backend_inject_hello_response(
                item, command, hello_mode,
            )));
        }
    }
    Ok(None)
}

/// 业务作用: 本地完成 Redis 7.2 客户端库元数据握手；只接受 Redis 定义的两个字段。
///
/// 参数说明:
/// - `command`: 已解析但仍借用原始前端批次字节的 CLIENT SETINFO 命令视图。
///
/// 返回:
/// - 返回完整 RESP 成功或错误帧；不会复制或保存客户端提供的元数据内容。
fn client_setinfo_local_response(command: &ClientCommand) -> LocalHandshakeResponse {
    let response = if command.arg_count() != 4 {
        resp::err("wrong number of arguments for 'client setinfo' command")
    } else if command.arg(2).is_some_and(|option| {
        option.eq_ignore_ascii_case(b"LIB-NAME") || option.eq_ignore_ascii_case(b"LIB-VER")
    }) {
        resp::simple_ok().to_vec()
    } else {
        resp::err("unsupported CLIENT SETINFO option; expected LIB-NAME or LIB-VER")
    };
    LocalHandshakeResponse {
        response,
        protocol_version: None,
    }
}

/// 业务作用: 在 backend-inject 模式本地校验 AUTH 形态，并生成不转发凭据的兼容响应。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
fn backend_inject_auth_response(
    item: &ProxyItem,
    command: &ClientCommand,
) -> LocalHandshakeResponse {
    let arg_count = command.arg_count();
    let response = if arg_count != 2 && arg_count != 3 {
        resp::err("wrong number of arguments for 'auth' command")
    } else if let Some(expected) = item.password.as_deref() {
        let password = command
            .arg(arg_count - 1)
            .expect("AUTH arg count checked before password lookup");
        if password == expected.as_bytes() {
            resp::simple_ok().to_vec()
        } else {
            resp::wrongpass().to_vec()
        }
    } else if backend_accepts_unconfigured_acl_auth(item.backend_profile, arg_count) {
        resp::simple_ok().to_vec()
    } else if item.backend_profile == BackendProfile::Pika {
        resp::err("Client sent AUTH, but no password is set")
    } else {
        resp::err("AUTH called without any password configured")
    };
    LocalHandshakeResponse {
        response,
        protocol_version: None,
    }
}

/// 业务作用: KeyDB 6.3.4 在默认用户未配置密码时仍接受双参数 ACL 形式的 AUTH。
///
/// 单参数 AUTH 继续返回未配置密码错误；其他后端档案保持各自现有语义，不能把
/// KeyDB 的兼容分支扩散到 Redis、Valkey 等产品。
///
/// 参数说明:
/// - `backend_profile`: 当前 listener 使用的后端产品兼容档案。
/// - `arg_count`: AUTH 命令包含命令名在内的参数数量。
fn backend_accepts_unconfigured_acl_auth(
    backend_profile: BackendProfile,
    arg_count: usize,
) -> bool {
    backend_profile == BackendProfile::KeyDb && arg_count == 3
}

/// 业务作用: 在 backend-inject 模式解析 HELLO 认证与协议协商并生成本地产品响应。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `hello_mode`: HELLO 响应中的服务模式，决定客户端采用单节点还是集群语义。
///
fn backend_inject_hello_response(
    item: &ProxyItem,
    command: &ClientCommand,
    hello_mode: &str,
) -> LocalHandshakeResponse {
    let Some(version_arg) = command.arg(1) else {
        return LocalHandshakeResponse {
            response: resp::err("wrong number of arguments for 'hello' command"),
            protocol_version: None,
        };
    };
    let (protocol_version, response) = match version_arg {
        b"2" => (
            2,
            resp::hello2_response_for_server_mode(
                item.backend_profile.hello_server_name(),
                hello_mode,
            ),
        ),
        b"3" if item.backend_profile.capabilities().resp3 => (
            3,
            resp::hello3_response_for_server_mode(
                item.backend_profile.hello_server_name(),
                hello_mode,
            ),
        ),
        b"3" if item
            .backend_profile
            .capabilities()
            .hello3_downgrade_to_resp2 =>
        {
            (
                2,
                resp::hello2_response_for_server_mode(
                    item.backend_profile.hello_server_name(),
                    hello_mode,
                ),
            )
        }
        b"3" => {
            return LocalHandshakeResponse {
                response: resp::noproto().to_vec(),
                protocol_version: None,
            };
        }
        _ => {
            return LocalHandshakeResponse {
                response: resp::noproto().to_vec(),
                protocol_version: None,
            };
        }
    };

    let mut idx = 2usize;
    while idx < command.arg_count() {
        let Some(token) = command.arg(idx) else {
            break;
        };
        if token.eq_ignore_ascii_case(b"AUTH") {
            if idx + 2 >= command.arg_count() {
                return LocalHandshakeResponse {
                    response: resp::err("wrong number of arguments for 'hello auth'"),
                    protocol_version: None,
                };
            }
            if let Some(expected) = item.password.as_deref() {
                let password = command
                    .arg(idx + 2)
                    .expect("HELLO AUTH password arg checked");
                if password != expected.as_bytes() {
                    return LocalHandshakeResponse {
                        response: resp::wrongpass().to_vec(),
                        protocol_version: None,
                    };
                }
            } else if item.backend_profile != BackendProfile::KeyDb {
                return LocalHandshakeResponse {
                    response: resp::err("AUTH called without any password configured"),
                    protocol_version: None,
                };
            }
            idx += 3;
        } else if token.eq_ignore_ascii_case(b"SETNAME") {
            if idx + 1 >= command.arg_count() {
                return LocalHandshakeResponse {
                    response: resp::err("wrong number of arguments for 'hello setname'"),
                    protocol_version: None,
                };
            }
            idx += 2;
        } else {
            return LocalHandshakeResponse {
                response: resp::err(&format!(
                    "unsupported HELLO option `{}`",
                    String::from_utf8_lossy(token).to_ascii_uppercase()
                )),
                protocol_version: None,
            };
        }
    }

    LocalHandshakeResponse {
        response,
        protocol_version: Some(protocol_version),
    }
}

/// 业务作用: 判断命令是否需要透明隧道，用于避免 request/response 计数模型卡住后端主动推送或长阻塞响应。 QUIT 不在名单里:它由 classify_node_proxy_batch 识别并本地应答,不为断连新建后端连接。
/// 参数说明:
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
// 判断命令是否需要透明隧道，用于避免 request/response 计数模型卡住后端主动推送或长阻塞响应。
// QUIT 不在名单里:它由 classify_node_proxy_batch 识别并本地应答,不为断连新建后端连接。
fn node_proxy_command_requires_tunnel(command: &ClientCommand) -> bool {
    let Some(name) = command.arg(0) else {
        return false;
    };
    if matches_ignore_ascii_case(
        name,
        &[
            b"AUTH",
            b"HELLO",
            b"SELECT",
            b"CLIENT",
            b"READONLY",
            b"READWRITE",
            b"ASKING",
            b"MULTI",
            b"EXEC",
            b"DISCARD",
            b"WATCH",
            b"UNWATCH",
            b"SUBSCRIBE",
            b"PSUBSCRIBE",
            b"SSUBSCRIBE",
            b"UNSUBSCRIBE",
            b"PUNSUBSCRIBE",
            b"SUNSUBSCRIBE",
            b"MONITOR",
            b"SYNC",
            b"PSYNC",
            b"DEBUG",
            b"BLPOP",
            b"BRPOP",
            b"BRPOPLPUSH",
            b"BZPOPMIN",
            b"BZPOPMAX",
            b"BZMPOP",
            b"BLMOVE",
            b"BLMPOP",
        ],
    ) {
        return true;
    }
    if xread_has_block_option(command) {
        return true;
    }
    !is_known_stateless_request_response(name)
}

/// 业务作用: 判断一条 tunnel 命令在读取唯一响应后是否一定恢复为无状态连接。
///
/// 参数说明:
/// - `command`: 已解析但仍借用原始前端字节的单条 Redis 命令。
///
/// 返回:
/// - `true`: 阻塞只影响本次请求等待，响应结束后不保留订阅、事务、DB 等连接状态。
/// - `false`: 命令可能保留连接状态或不是已审核的阻塞命令，连接必须按 tainted 关闭。
///
/// 关键约束:
/// - 调用方还必须保证当前 tunnel 批次只有这一条命令，并确认后端缓冲没有尾部字节。
/// - `XREADGROUP` 会修改消费组数据，但不会修改连接会话状态，因此响应完成后可以复用。
fn command_is_reusable_blocking(command: &ClientCommand) -> bool {
    let Some(name) = command.arg(0) else {
        return false;
    };
    if matches_ignore_ascii_case(
        name,
        &[
            b"BLPOP",
            b"BRPOP",
            b"BRPOPLPUSH",
            b"BZPOPMIN",
            b"BZPOPMAX",
            b"BZMPOP",
            b"BLMOVE",
            b"BLMPOP",
        ],
    ) {
        return true;
    }
    xread_has_block_option(command)
}

/// 业务作用: 判断 `XREAD`/`XREADGROUP` 的选项区是否包含真正的 `BLOCK <milliseconds>`。
///
/// 参数说明:
/// - `command`: 当前已解析命令；参数切片仍借用原始 RESP 帧。
///
/// 返回:
/// - `true`: 命令名正确，且在 `STREAMS` 之前存在带数值位置的 `BLOCK` 选项。
/// - `false`: 非 XREAD 命令、参数不足，或 `BLOCK` 只作为 stream key/ID 出现。
///
/// 关键步骤:
/// 1. `XREADGROUP` 跳过固定的 `GROUP <group> <consumer>` 前缀。
/// 2. 扫描在首个 `STREAMS` 处停止，避免 key 恰好名为 `BLOCK` 时误建长连接 tunnel。
/// 3. 这里只识别阻塞属性；完整语法和 timeout 数值合法性仍由 Redis 后端校验。
fn xread_has_block_option(command: &ClientCommand) -> bool {
    let Some(name) = command.arg(0) else {
        return false;
    };
    let option_start = if name.eq_ignore_ascii_case(b"XREAD") {
        1
    } else if name.eq_ignore_ascii_case(b"XREADGROUP") {
        4
    } else {
        return false;
    };
    for index in option_start..command.arg_count() {
        let Some(argument) = command.arg(index) else {
            return false;
        };
        if argument.eq_ignore_ascii_case(b"STREAMS") {
            return false;
        }
        if argument.eq_ignore_ascii_case(b"BLOCK") {
            return index + 1 < command.arg_count();
        }
    }
    false
}

/// 业务作用: 解析 Cluster 单入口可复用阻塞命令访问的唯一 hash slot。
///
/// 参数说明:
/// - `command`: 已通过阻塞命令白名单的单条 Redis 命令，参数仍零复制借用原始帧。
///
/// 返回:
/// - 全部 key 属于同一 slot 时返回该 slot。
/// - 参数数量、numkeys、STREAMS 结构错误或跨 slot 时返回可直接转成 Redis 错误的原因。
///
/// 关键步骤:
/// - 按各命令真实语法确定 key 区间，不把 timeout、方向、group、consumer 或 stream ID 当 key。
/// - 只遍历参数切片并计算 CRC16，不构造参数 `Vec` 或大写命令 `String`。
fn cluster_reusable_blocking_slot(command: &ClientCommand) -> anyhow::Result<u16> {
    let name = command
        .arg(0)
        .context("blocking command is missing command name")?;
    let arg_count = command.arg_count();
    let (key_start, key_end) =
        if matches_ignore_ascii_case(name, &[b"BLPOP", b"BRPOP", b"BZPOPMIN", b"BZPOPMAX"]) {
            if arg_count < 3 {
                bail!("wrong number of arguments for blocking pop command");
            }
            (1, arg_count - 1)
        } else if name.eq_ignore_ascii_case(b"BRPOPLPUSH") {
            if arg_count != 4 {
                bail!("wrong number of arguments for BRPOPLPUSH");
            }
            (1, 3)
        } else if name.eq_ignore_ascii_case(b"BLMOVE") {
            if arg_count != 6 {
                bail!("wrong number of arguments for BLMOVE");
            }
            (1, 3)
        } else if name.eq_ignore_ascii_case(b"BLMPOP") || name.eq_ignore_ascii_case(b"BZMPOP") {
            if arg_count < 5 {
                bail!("wrong number of arguments for blocking multi-pop command");
            }
            let numkeys = parse_command_usize(command, 2, "blocking multi-pop numkeys")?;
            if numkeys == 0 {
                bail!("blocking multi-pop numkeys must be greater than zero");
            }
            let key_end = 3usize
                .checked_add(numkeys)
                .context("blocking multi-pop numkeys overflow")?;
            if key_end >= arg_count {
                bail!("blocking multi-pop numkeys exceeds supplied key count");
            }
            (3, key_end)
        } else if name.eq_ignore_ascii_case(b"XREAD") || name.eq_ignore_ascii_case(b"XREADGROUP") {
            let option_start = if name.eq_ignore_ascii_case(b"XREADGROUP") {
                if arg_count < 5
                    || !command
                        .arg(1)
                        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"GROUP"))
                {
                    bail!("XREADGROUP requires GROUP <group> <consumer>");
                }
                4
            } else {
                1
            };
            let streams_idx = (option_start..arg_count)
                .find(|idx| {
                    command
                        .arg(*idx)
                        .is_some_and(|arg| arg.eq_ignore_ascii_case(b"STREAMS"))
                })
                .context("XREAD blocking command requires STREAMS section")?;
            let remaining = arg_count.saturating_sub(streams_idx + 1);
            if remaining < 2 || !remaining.is_multiple_of(2) {
                bail!("wrong number of arguments for XREAD STREAMS");
            }
            let key_start = streams_idx + 1;
            (key_start, key_start + remaining / 2)
        } else {
            bail!("command is not an approved reusable blocking command");
        };

    command_key_range_slot(command, key_start, key_end)
}

/// 业务作用: 解析命令指定位置的非负整数参数，避免为了 `parse` 复制参数字节。
///
/// 参数说明:
/// - `command`: 当前已解析 Redis 命令。
/// - `index`: 要读取的参数下标。
/// - `label`: 解析失败时用于定位具体语法字段的名称。
///
/// 返回:
/// - 成功时返回 `usize`。
/// - 参数缺失、不是 UTF-8 十进制整数或超出平台范围时返回错误。
fn parse_command_usize(
    command: &ClientCommand,
    index: usize,
    label: &str,
) -> anyhow::Result<usize> {
    let raw = command
        .arg(index)
        .with_context(|| format!("{label} is missing"))?;
    std::str::from_utf8(raw)
        .with_context(|| format!("{label} is not UTF-8"))?
        .parse::<usize>()
        .with_context(|| format!("{label} is not a valid non-negative integer"))
}

/// 业务作用: 校验一段命令参数中的全部 key 属于同一 Redis Cluster slot。
///
/// 参数说明:
/// - `command`: 当前已解析 Redis 命令，key 直接借用其原始 RESP 帧。
/// - `start`: 第一个 key 的参数下标，包含该位置。
/// - `end`: 最后一个 key 之后的参数下标，不包含该位置。
///
/// 返回:
/// - 至少一个 key 且全部同 slot 时返回 slot。
/// - 空区间、参数缺失或任意 key 跨 slot 时返回错误。
fn command_key_range_slot(
    command: &ClientCommand,
    start: usize,
    end: usize,
) -> anyhow::Result<u16> {
    if start >= end {
        bail!("blocking command requires at least one key");
    }
    let first = command
        .arg(start)
        .context("blocking command key is missing")?;
    let slot = redis_slot(first);
    for index in start + 1..end {
        let key = command
            .arg(index)
            .context("blocking command key is missing")?;
        if redis_slot(key) != slot {
            bail!("CROSSSLOT Keys in request don't hash to the same slot");
        }
    }
    Ok(slot)
}

/// 业务作用: 判断命令是否包含指定参数，用于识别 XREAD/XREADGROUP BLOCK。
/// 参数说明:
/// - `command`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `needle`: 需要在目标字节序列中查找的模式，不进行字符编码转换。
///
// 判断命令是否包含指定参数，用于识别 XREAD/XREADGROUP BLOCK。
fn command_has_arg(command: &ClientCommand, needle: &[u8]) -> bool {
    (1..)
        .map_while(|idx| command.arg(idx))
        .any(|arg| arg.eq_ignore_ascii_case(needle))
}

/// 业务作用: 对一组命令名做大小写无关匹配，用于保持热路径判断逻辑集中。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
/// - `names`: 按顺序查找的名称列表，用于兼容多个配置入口或候选标识。
///
// 对一组命令名做大小写无关匹配，用于保持热路径判断逻辑集中。
fn matches_ignore_ascii_case(value: &[u8], names: &[&[u8]]) -> bool {
    names.iter().any(|name| value.eq_ignore_ascii_case(name))
}

/// 业务作用: 判断命令是否是明确的一请求一响应且不修改连接状态的普通命令。 名单按生产命令频率排序(热命令前置),降低每批 1000 条命令的线性匹配成本; FT./JSON. 前缀族放在精确名单之后,让 GET/SET 这类高频命令少走两次前缀比较。
/// 参数说明:
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
///
// 判断命令是否是明确的一请求一响应且不修改连接状态的普通命令。
// 名单按生产命令频率排序(热命令前置),降低每批 1000 条命令的线性匹配成本;
// FT./JSON. 前缀族放在精确名单之后,让 GET/SET 这类高频命令少走两次前缀比较。
fn is_known_stateless_request_response(name: &[u8]) -> bool {
    if matches_ignore_ascii_case(
        name,
        &[
            b"GET",
            b"SET",
            b"DEL",
            b"HSET",
            b"HGET",
            b"HGETALL",
            b"HMGET",
            b"HDEL",
            b"EXPIRE",
            b"TTL",
            b"EXISTS",
            b"INCR",
            b"INCRBY",
            b"DECR",
            b"SETEX",
            b"SETNX",
            b"MGET",
            b"MSET",
            b"ZADD",
            b"ZRANGE",
            b"ZREM",
            b"ZSCORE",
            b"ZINCRBY",
            b"ZCARD",
            b"SADD",
            b"SREM",
            b"SISMEMBER",
            b"SMEMBERS",
            b"LPUSH",
            b"RPUSH",
            b"LPOP",
            b"RPOP",
            b"LRANGE",
            b"LLEN",
            b"XADD",
            b"XRANGE",
            b"XLEN",
            b"XREAD",
            b"XREADGROUP",
            b"EVAL",
            b"EVALSHA",
            b"EVAL_RO",
            b"EVALSHA_RO",
            b"SCRIPT",
            b"PUBLISH",
            b"PING",
            b"TYPE",
            b"UNLINK",
            b"FT.",
            b"JSON.",
            b"ECHO",
            b"COMMAND",
            b"INFO",
            b"TIME",
            b"CONFIG",
            b"DBSIZE",
            b"SCAN",
            b"CLUSTERSCAN",
            b"KEYS",
            b"RANDOMKEY",
            b"TOUCH",
            b"EXPIREAT",
            b"PEXPIRE",
            b"PEXPIREAT",
            b"PTTL",
            b"PERSIST",
            b"RENAME",
            b"RENAMENX",
            b"COPY",
            b"DUMP",
            b"GETBIT",
            b"SETBIT",
            b"BITCOUNT",
            b"BITPOS",
            b"BITOP",
            b"BITFIELD",
            b"BITFIELD_RO",
            b"RESTORE",
            b"GETDEL",
            b"GETEX",
            b"GETRANGE",
            b"GETSET",
            b"PSETEX",
            b"SETRANGE",
            b"STRLEN",
            b"APPEND",
            b"INCRBYFLOAT",
            b"DECRBY",
            b"MSETNX",
            b"HSETNX",
            b"HMSET",
            b"HEXISTS",
            b"HLEN",
            b"HKEYS",
            b"HVALS",
            b"HSTRLEN",
            b"HRANDFIELD",
            b"HSCAN",
            b"HINCRBY",
            b"HINCRBYFLOAT",
            b"LPUSHX",
            b"RPUSHX",
            b"LINDEX",
            b"LSET",
            b"LTRIM",
            b"LREM",
            b"LMOVE",
            b"RPOPLPUSH",
            b"LINSERT",
            b"LPOS",
            b"SPOP",
            b"SCARD",
            b"SMISMEMBER",
            b"SRANDMEMBER",
            b"SSCAN",
            b"SMOVE",
            b"SDIFF",
            b"SDIFFSTORE",
            b"SINTER",
            b"SINTERSTORE",
            b"SUNION",
            b"SUNIONSTORE",
            b"ZCOUNT",
            b"ZLEXCOUNT",
            b"ZRANGESTORE",
            b"ZRANK",
            b"ZREVRANK",
            b"ZMSCORE",
            b"ZRANDMEMBER",
            b"ZSCAN",
            b"ZPOPMIN",
            b"ZPOPMAX",
            b"ZDIFF",
            b"ZDIFFSTORE",
            b"ZINTER",
            b"ZINTERSTORE",
            b"ZUNION",
            b"ZUNIONSTORE",
            b"GEOADD",
            b"GEODIST",
            b"GEOHASH",
            b"GEOPOS",
            b"GEORADIUS",
            b"GEORADIUSBYMEMBER",
            b"GEOSEARCH",
            b"GEOSEARCHSTORE",
            b"PFADD",
            b"PFCOUNT",
            b"PFMERGE",
            b"XREVRANGE",
            b"XDEL",
            b"XTRIM",
            b"XINFO",
            b"XGROUP",
            b"FUNCTION",
            b"FCALL",
            b"FCALL_RO",
            b"SPUBLISH",
            b"PUBSUB",
            b"CLUSTER",
            b"OBJECT",
            b"MEMORY",
        ],
    ) {
        return true;
    }
    starts_with_ignore_ascii_case(name, b"FT.") || starts_with_ignore_ascii_case(name, b"JSON.")
}

/// 业务作用: 以 ASCII 大小写无关方式判断命令名是否属于指定模块前缀。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
/// - `prefix`: 测试 key、协议字段或配置路径前缀，用于限定匹配和资源命名空间。
///
// 先检查长度再切片，避免短命令在前缀比较时越界。
fn starts_with_ignore_ascii_case(value: &[u8], prefix: &[u8]) -> bool {
    value.len() >= prefix.len() && value[..prefix.len()].eq_ignore_ascii_case(prefix)
}

/// 业务作用: 在命令进入路由前校验参数数量、frame 和 bulk 大小，超限即拒绝当前连接输入。
/// 参数说明:
/// - `arg_ranges`: 命令参数在原始帧中的边界集合，用于无复制地定位和改写指定参数。
/// - `frame_len`: 已经确认的完整 RESP 帧长度，用于切分缓冲区且不吞掉后续帧。
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
/// 返回: 命令参数数、frame 大小和每个 bulk 长度均未超限时返回成功；任一限制违反时返回错误。
fn validate_command_limits(
    arg_ranges: &[std::ops::Range<usize>],
    frame_len: usize,
    item: &ProxyItem,
) -> anyhow::Result<()> {
    if frame_len > item.frame_max_bytes {
        bail!(
            "client command frame exceeds limit: {} > {}",
            frame_len,
            item.frame_max_bytes
        );
    }
    if let Some(max_bulk_len) = arg_ranges.iter().map(|range| range.len()).max() {
        if max_bulk_len > item.bulk_max_bytes {
            bail!(
                "client command bulk arg exceeds limit: {} > {}",
                max_bulk_len,
                item.bulk_max_bytes
            );
        }
    }
    Ok(())
}

/// 业务作用: 在关闭非法前端连接前返回 Redis 兼容的协议错误。
/// 解析器错误详情会先清洗，确保错误内容不能注入额外 RESP 行。
/// 参数说明:
/// - `client`: 接收协议错误响应的前端连接。
/// - `error`: 已由解析器生成且需要清洗换行和 NUL 的错误。
async fn write_frontend_protocol_error(
    client: &mut TcpStream,
    error: &anyhow::Error,
) -> anyhow::Result<()> {
    let detail = error
        .to_string()
        .replace(['\r', '\n'], " ")
        .replace('\0', "\\0");
    client
        .write_all(&resp::err(&format!("Protocol error: {detail}")))
        .await?;
    Ok(())
}

/// 业务作用: Valkey 9.1.1 对含 NUL 的失步请求会继续累计到 64 KiB inline 上限后再报错；
/// Redis 则可以立即拒绝。该差异由后端兼容档案控制，避免跨产品混用协议时序。
/// 参数说明:
/// - `backend_profile`: 当前 listener 使用的后端产品兼容档案。
/// - `buffered_bytes`: 当前尚未完成解析的前端缓冲字节数。
/// - `error`: 本轮解析得到的协议错误。
fn should_defer_valkey_protocol_error(
    backend_profile: BackendProfile,
    buffered_bytes: usize,
    error: &anyhow::Error,
) -> bool {
    const VALKEY_INLINE_MAX_BYTES: usize = 64 * 1024;
    backend_profile == BackendProfile::Valkey
        && buffered_bytes <= VALKEY_INLINE_MAX_BYTES
        && error.to_string().contains("invalid NUL byte in request")
}

/// 业务作用: 按 terminate、backend-inject 或 pass-through 策略处理前端认证前缀并保留首包剩余字节。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
/// - `client`: 当前前端客户端连接或测试客户端，用于发送命令并读取对应响应。
///
/// 返回: 认证成功或允许匿名时返回协议版本和首包剩余字节；超时、拒绝或协议非法时返回错误。
async fn frontend_auth(
    item: &ProxyItem,
    client: &mut TcpStream,
) -> anyhow::Result<FrontendAuthResult> {
    let expected = item
        .frontend_password
        .as_deref()
        .context("frontend_password is required")?;
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];

    let hello_mode = if item.mode == ProxyMode::ClusterNodeProxy {
        "cluster"
    } else {
        "standalone"
    };
    // Cluster 单入口内部共享 RESP2 lane，无法保证 HGETALL 等命令返回 RESP3 类型。
    // 必须在认证握手阶段明确拒绝 HELLO 3，不能先向客户端宣称 proto=3 再返回 RESP2 数据。
    let allow_resp3 = item.mode != ProxyMode::Cluster && item.backend_profile.capabilities().resp3;
    let hello3_downgrade_to_resp2 = item.mode != ProxyMode::Cluster
        && item
            .backend_profile
            .capabilities()
            .hello3_downgrade_to_resp2;

    loop {
        let parsed_result = tokio::time::timeout(
            Duration::from_millis(item.frontend_auth_timeout_ms),
            async {
                loop {
                    match resp::parse_command(&buf) {
                        Ok(Some(parsed)) => break Ok::<_, anyhow::Error>(parsed),
                        Ok(None) => {}
                        Err(err)
                            if should_defer_valkey_protocol_error(
                                item.backend_profile,
                                buf.len(),
                                &err,
                            ) => {}
                        Err(err) => return Err(err),
                    }
                    if buf.len() >= item.max_auth_bytes {
                        bail!("frontend auth frame too large");
                    }
                    // 单次 read 也受剩余额度约束，避免先检查后读取导致实际上限比配置多出
                    // 一个临时缓冲区。完整帧恰好等于上限时会在上面的解析分支正常通过。
                    let remaining = item.max_auth_bytes - buf.len();
                    let read_len = remaining.min(tmp.len());
                    let n = client.read(&mut tmp[..read_len]).await?;
                    if n == 0 {
                        bail!("client closed before auth");
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
            },
        )
        .await
        .context("frontend auth timeout")?;
        let parsed = match parsed_result {
            Ok(parsed) => parsed,
            Err(err) => {
                write_frontend_protocol_error(client, &err).await?;
                return Err(err).context("invalid frontend auth protocol");
            }
        };
        let consumed = parsed.consumed;

        match handle_frontend_auth_with_backend_profile(
            &parsed.args,
            expected,
            item.backend_profile.hello_server_name(),
            hello_mode,
            allow_resp3,
            hello3_downgrade_to_resp2,
        )? {
            FrontendAuth::Authenticated {
                response,
                protocol_version,
            } => {
                client.write_all(&response).await?;
                return Ok(FrontendAuthResult {
                    leftover: buf[consumed..].to_vec(),
                    protocol_version,
                });
            }
            FrontendAuth::Rejected { response } => {
                // 官方服务会保留认证失败的连接，允许客户端在同一会话重新提交凭据。
                client.write_all(&response).await?;
                client.flush().await?;
                buf.drain(..consumed);
            }
            FrontendAuth::NotAuthCommand => {
                // 未认证业务命令只返回 NOAUTH，不应强制 SDK 重新建立 TCP 连接。
                client.write_all(resp::noauth()).await?;
                client.flush().await?;
                buf.drain(..consumed);
            }
        }
    }
}

/// 业务作用: 拆分已经校验过的 host:port 地址，用于把拓扑响应中的地址写回 RESP 字段。
/// 参数说明:
/// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
///
// 拆分已经校验过的 host:port 地址，用于把拓扑响应中的地址写回 RESP 字段。
fn split_normalized_addr(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse().ok()?;
    Some((host.to_string(), port))
}

/// 业务作用: 从 RESP 字符串类型中提取 UTF-8 文本，用于识别拓扑字段名和节点 host。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
// 从 RESP 字符串类型中提取 UTF-8 文本，用于识别拓扑字段名和节点 host。
fn resp_string(value: &resp::RespValue) -> Option<String> {
    match value {
        resp::RespValue::Simple(bytes) | resp::RespValue::Bulk(Some(bytes)) => {
            String::from_utf8(bytes.clone()).ok()
        }
        _ => None,
    }
}

/// 业务作用: 从 RESP 整数或数字字符串中提取端口值，用于识别 Redis Cluster 节点地址。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
// 从 RESP 整数或数字字符串中提取端口值，用于识别 Redis Cluster 节点地址。
fn resp_integer(value: &resp::RespValue) -> Option<i64> {
    match value {
        resp::RespValue::Integer(value) => Some(*value),
        resp::RespValue::Simple(bytes) | resp::RespValue::Bulk(Some(bytes)) => {
            std::str::from_utf8(bytes).ok()?.parse().ok()
        }
        _ => None,
    }
}

/// 业务作用: 按原 RESP 字符串形态写回新内容，用于减少拓扑响应改写后的协议形态变化。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
// 按原 RESP 字符串形态写回新内容，用于减少拓扑响应改写后的协议形态变化。
fn set_resp_string(value: &mut resp::RespValue, bytes: &[u8]) {
    match value {
        resp::RespValue::Simple(current) => *current = bytes.to_vec(),
        resp::RespValue::Bulk(Some(current)) => *current = bytes.to_vec(),
        _ => *value = resp::RespValue::Bulk(Some(bytes.to_vec())),
    }
}
