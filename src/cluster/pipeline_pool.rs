use crate::infrastructure::lifecycle::{TaskScope, TaskSpawner};
use crate::infrastructure::time::duration_micros as duration_micros_u64;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bytes::Bytes;
use tokio::sync::Notify;
use tracing::warn;

use crate::admin::ProxyMetrics;
use crate::backend::BackendProfile;
use crate::cluster::backend::{
    BackendConnectOptions, BackendConnection, BackendRawResponses, BackendTlsConfig,
};
use crate::config::{AuthMode, ProxyItem};

/// 业务作用: 管理所有后端节点按 RESP 版本隔离的有界 pipeline 连接池。
#[derive(Debug)]
// 表示 PipelineBackendPool 的数据结构，用于管理 backend pipeline 专用 idle-first 连接池。
pub struct PipelineBackendPool {
    factory: Arc<BackendConnectionFactory>,
    nodes: StdMutex<HashMap<String, Arc<NodePipelinePool>>>,
    max_active: usize,
    queue_size: usize,
    submit_timeout_ms: u64,
    idle_timeout: Option<Duration>,
    tasks: TaskSpawner,
    owned_tasks: Option<TaskScope>,
}

/// 业务作用: 记录一次连接池请求的排队、建连、写入和读取耗时。
#[derive(Debug, Clone, Copy, Default)]
// 表示 pipeline pool 分段耗时，用于诊断 acquire、backend write 和 backend read 成本。
pub struct PipelinePoolProfile {
    /// 从连接池取得可用后端连接所用的时间，单位为微秒。
    pub acquire_micros: u64,
    /// 把 pipeline 写入后端连接所用的时间，单位为微秒。
    pub backend_write_micros: u64,
    /// 从后端读取完整 pipeline 响应所用的时间，单位为微秒。
    pub backend_read_micros: u64,
}

/// 业务作用: 保存 ordinary mux 新建后端连接所需的认证、TLS、超时和指标配置。
#[derive(Debug)]
// 表示 BackendConnectionFactory 的数据结构，用于创建 pipeline pool 的后端连接。
struct BackendConnectionFactory {
    profile: BackendProfile,
    username: Option<String>,
    password: Option<crate::infrastructure::credentials::SecretText>,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    response_bytes_limit: usize,
    metrics: ProxyMetrics,
    tls: Option<BackendTlsConfig>,
}

/// 业务作用: 维护单节点 pipeline 连接的 active 上限、等待队列、idle 复用和回收。
#[derive(Debug)]
// 表示 NodePipelinePool 的数据结构，用于单个 Redis 后端节点的 pipeline 连接池。
struct NodePipelinePool {
    node: String,
    protocol_version: Option<u8>,
    factory: Arc<BackendConnectionFactory>,
    state: StdMutex<NodePipelineState>,
    notify: Notify,
    max_active: usize,
    queue_size: usize,
    submit_timeout_ms: u64,
    idle_timeout: Option<Duration>,
    reuse_lifo: bool,
}

/// 业务作用: 给空闲后端连接附加归还时间，用于借出与后台回收共用过期判断。
#[derive(Debug)]
// 表示带归还时间的 idle 连接，用于 TTL 回收与 acquire 验龄。
struct IdleConnection {
    conn: BackendConnection,
    idle_since: Instant,
}

/// 业务作用: 区分 pipeline 池本轮应复用 idle 连接、创建新连接还是继续等待容量。
#[derive(Debug)]
// 表示一次 acquire 在锁内得出的行动决策,用于把 await 全部留在锁外。
enum AcquireStep {
    Reuse(BackendConnection),
    Create,
    Wait,
}

/// 业务作用: 保存单节点池的活动连接数和按最近归还顺序排列的 idle 连接。
#[derive(Debug, Default)]
// 表示 NodePipelineState 的数据结构，用于维护 idle connection、active 数和等待者数量。
struct NodePipelineState {
    idle: VecDeque<IdleConnection>,
    active: usize,
    waiters: usize,
}

impl PipelineBackendPool {
    /// 业务作用: 按代理的容量、超时、TLS 与认证配置创建节点隔离的 pipeline 连接池集合。
    /// 参数说明:
    /// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 由池自身持有后台任务作用域的节点池集合，释放池时同步撤销周期任务。
    pub fn new(item: &ProxyItem, metrics: ProxyMetrics) -> Self {
        let scope = TaskScope::default();
        let mut pool = Self::with_tasks(item, metrics, scope.spawner());
        pool.owned_tasks = Some(scope);
        pool
    }

    /// 业务作用: 将节点池的周期回收任务绑定到代理运行代，由该代在连接排空后统一等待。
    /// 参数说明:
    /// - `item`: 后端连接、认证和池容量配置。
    /// - `metrics`: 当前代理共享计数单元。
    /// - `tasks`: 运行代持有的弱任务入口，不参与命令转发。
    ///
    /// 返回: 尚未建连的池集合；后台任务只在节点池首次建立时登记。
    pub(crate) fn with_tasks(item: &ProxyItem, metrics: ProxyMetrics, tasks: TaskSpawner) -> Self {
        let (username, password) = backend_credentials(item);
        Self {
            factory: Arc::new(BackendConnectionFactory {
                profile: item.backend_profile,
                username,
                password,
                connect_timeout_ms: item.connect_timeout_ms,
                request_timeout_ms: item.backend_request_timeout_ms,
                tcp_keepalive_ms: item.tcp_keepalive_ms,
                response_bytes_limit: item.pending_response_bytes,
                metrics,
                tls: BackendTlsConfig::for_backend(item),
            }),
            nodes: StdMutex::new(HashMap::new()),
            max_active: item.pipeline_backend_max_active,
            queue_size: item.pipeline_backend_queue_size,
            submit_timeout_ms: item.pipeline_backend_submit_timeout_ms,
            idle_timeout: optional_idle_timeout(item.pipeline_backend_idle_timeout_ms),
            tasks,
            owned_tasks: None,
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 提交一次后端 pipeline，用于从 idle-first 连接池借出连接并在整批执行期间独占。
    pub async fn send_pipeline(
        &self,
        node: &str,
        commands: Vec<Bytes>,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        self.send_pipeline_with_protocol(node, None, commands).await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 提交一次带 RESP 协议 profile 的后端 pipeline，用于隔离 RESP2/RESP3 后端连接。
    pub async fn send_pipeline_with_protocol(
        &self,
        node: &str,
        protocol_version: Option<u8>,
        commands: Vec<Bytes>,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let pool = self.node_pool(node, protocol_version);
        pool.send_pipeline(&commands).await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 提交 vectored pipeline 并返回 joined responses，用于 cluster-single-entry 热路径减少响应分配。
    pub async fn send_pipeline_joined(
        &self,
        node: &str,
        commands: Vec<Bytes>,
    ) -> anyhow::Result<BackendRawResponses> {
        let pool = self.node_pool(node, None);
        pool.send_pipeline_joined(&commands).await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 提交一段连续原始后端 pipeline，用于 standalone / cluster-node-proxy 避免把已连续的前端 pipeline 再拆成逐命令 slices。
    pub async fn send_raw_pipeline_with_protocol(
        &self,
        node: &str,
        protocol_version: Option<u8>,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let pool = self.node_pool(node, protocol_version);
        pool.send_raw_pipeline(raw_pipeline, response_count).await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 提交连续 raw pipeline 并把响应保留为连续 buffer，用于减少大量小响应分配。
    pub async fn send_raw_pipeline_joined_with_protocol(
        &self,
        node: &str,
        protocol_version: Option<u8>,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<BackendRawResponses> {
        let pool = self.node_pool(node, protocol_version);
        pool.send_raw_pipeline_joined(raw_pipeline, response_count)
            .await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应及分段耗时；容量、连接、协议或响应大小异常时返回错误。
    // 提交连续 raw pipeline 并返回 pipeline pool 分段耗时，用于 cluster-node-proxy profiling。
    pub async fn send_raw_pipeline_joined_profiled_with_protocol(
        &self,
        node: &str,
        protocol_version: Option<u8>,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<(BackendRawResponses, PipelinePoolProfile)> {
        let pool = self.node_pool(node, protocol_version);
        pool.send_raw_pipeline_joined_profiled(raw_pipeline, response_count)
            .await
    }

    /// 业务作用: 预解析节点级 pool 句柄，用于 backend/protocol 在 session 内不变的 standalone / cluster-node-proxy 把「pool key 拼接 + 外层 map 加锁」从每批热路径挪到握手后只做一次。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    // 预解析节点级 pool 句柄，用于 backend/protocol 在 session 内不变的 standalone / cluster-node-proxy
    // 把「pool key 拼接 + 外层 map 加锁」从每批热路径挪到握手后只做一次。
    pub fn node_handle(&self, node: &str, protocol_version: Option<u8>) -> NodePipelineHandle {
        NodePipelineHandle {
            pool: self.node_pool(node, protocol_version),
        }
    }

    /// 业务作用: 初始化指定节点的 pool 状态，用于路由拓扑预热但不预创建后端连接。
    /// 参数说明:
    /// - `nodes`: 需要预热 pool 状态的后端节点地址集合。
    ///
    // 初始化指定节点的 pool 状态，用于路由拓扑预热但不预创建后端连接。
    pub fn warmup_nodes(&self, nodes: &[String]) {
        for node in nodes {
            let _ = self.node_pool(node, None);
        }
    }

    /// 业务作用: 查询或创建节点级 pool，用于每个 Redis 后端节点独立做 pipeline 连接复用。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级连接池的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    // 查询或创建节点级 pool，用于每个 Redis 后端节点独立做 pipeline 连接复用。
    fn node_pool(&self, node: &str, protocol_version: Option<u8>) -> Arc<NodePipelinePool> {
        let mut nodes = self
            .nodes
            .lock()
            .expect("cluster pipeline backend pool map mutex poisoned");
        nodes
            .entry(pool_key(node, protocol_version))
            .or_insert_with(|| {
                let pool = Arc::new(NodePipelinePool::new(
                    node,
                    protocol_version,
                    self.max_active,
                    self.queue_size,
                    self.submit_timeout_ms,
                    self.idle_timeout,
                    self.factory.clone(),
                ));
                spawn_idle_reaper(&pool, &self.tasks);
                pool
            })
            .clone()
    }
}

/// 业务作用: 在运行代的任务集合内启动节点级 idle 回收，池释放后自动结束，不反向保活池。
/// 参数说明:
/// - `pool`: 后端连接池句柄，用于受限获取、复用和回收连接。
/// - `tasks`: 池所属运行代的任务创建入口。
///
/// 返回: 启用 TTL 且运行时代仍存活时登记周期任务，否则不创建后台工作。
fn spawn_idle_reaper(pool: &Arc<NodePipelinePool>, tasks: &TaskSpawner) {
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
                break;
            };
            pool.reap_expired_idle();
        }
    });
}

/// 业务作用: 提供会话缓存的节点级 pipeline 池入口，避免热路径重复计算 key 和锁外层映射。
#[derive(Debug, Clone)]
// 表示 NodePipelineHandle 的数据结构，用于 session 级缓存单节点 pipeline pool 的直达入口。
pub struct NodePipelineHandle {
    pool: Arc<NodePipelinePool>,
}

impl NodePipelineHandle {
    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 提交连续 raw pipeline 并返回 joined response，用于 session 热路径跳过 pool 查表。
    pub async fn send_raw_pipeline_joined(
        &self,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<BackendRawResponses> {
        self.pool
            .send_raw_pipeline_joined(raw_pipeline, response_count)
            .await
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应及分段耗时；容量、连接、协议或响应大小异常时返回错误。
    // 提交连续 raw pipeline 并返回分段耗时，用于 stage profile 下的 session 热路径。
    pub async fn send_raw_pipeline_joined_profiled(
        &self,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<(BackendRawResponses, PipelinePoolProfile)> {
        self.pool
            .send_raw_pipeline_joined_profiled(raw_pipeline, response_count)
            .await
    }
}

impl NodePipelinePool {
    /// 业务作用: 构造单个后端节点的 pipeline 连接池，控制最大活跃连接、排队容量和 idle 回收。
    /// 参数说明:
    /// - `node`: 后端 Redis 节点地址。
    /// - `protocol_version`: 该池中后端连接需要维持的 RESP 协议版本。
    /// - `max_active`: 允许同时打开的最大后端连接数；配置校验要求大于 0。
    /// - `queue_size`: 连接池满时允许等待的任务数量上限，超过后立即返回背压错误。
    /// - `submit_timeout_ms`: 等待连接池容量的最长时间，超时后前端会收到错误而不是无限堆积。
    /// - `idle_timeout`: idle 后端连接的存活时间；为空时不按空闲时间回收。
    /// - `factory`: 创建新后端连接的工厂函数。
    ///
    /// 返回: 初始化后的节点级 pipeline pool。
    fn new(
        node: &str,
        protocol_version: Option<u8>,
        max_active: usize,
        queue_size: usize,
        submit_timeout_ms: u64,
        idle_timeout: Option<Duration>,
        factory: Arc<BackendConnectionFactory>,
    ) -> Self {
        Self {
            node: node.to_string(),
            protocol_version,
            factory,
            state: StdMutex::new(NodePipelineState::default()),
            notify: Notify::new(),
            max_active,
            queue_size,
            submit_timeout_ms,
            idle_timeout,
            reuse_lifo: env_bool("NARIX_V1_PIPELINE_POOL_LIFO").unwrap_or(true),
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 执行 pipeline，用于借出一条后端连接、独占执行整批命令并按健康状态归还或丢弃。
    async fn send_pipeline(&self, commands: &[Bytes]) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut conn = self.acquire().await?;
        match conn.send_pipeline(commands).await {
            Ok(frames) => {
                self.release(conn);
                Ok(frames)
            }
            Err(err) => {
                warn!(
                    backend = %self.node,
                    error = %err,
                    "cluster pipeline backend pool invalidated connection"
                );
                self.discard(conn);
                Err(err)
            }
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 执行 vectored pipeline 并返回 joined responses，用于 cluster sub-pipeline 直接保留后端连续回包。
    async fn send_pipeline_joined(
        &self,
        commands: &[Bytes],
    ) -> anyhow::Result<BackendRawResponses> {
        let mut conn = self.acquire().await?;
        match conn.send_pipeline_joined(commands).await {
            Ok(responses) => {
                self.release(conn);
                Ok(responses)
            }
            Err(err) => {
                warn!(
                    backend = %self.node,
                    error = %err,
                    "cluster joined pipeline backend pool invalidated connection"
                );
                self.discard(conn);
                Err(err)
            }
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 执行连续 raw pipeline，用于让节点级代理和 standalone pipeline 直接复用前端已解析出的原始批次。
    async fn send_raw_pipeline(
        &self,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut conn = self.acquire().await?;
        match conn.send_raw_pipeline(raw_pipeline, response_count).await {
            Ok(frames) => {
                self.release(conn);
                Ok(frames)
            }
            Err(err) => {
                warn!(
                    backend = %self.node,
                    error = %err,
                    "cluster raw pipeline backend pool invalidated connection"
                );
                self.discard(conn);
                Err(err)
            }
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 执行连续 raw pipeline 并返回 joined response，用于 standalone / cluster-node-proxy 高吞吐 pipeline 回包。
    async fn send_raw_pipeline_joined(
        &self,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<BackendRawResponses> {
        let mut conn = self.acquire().await?;
        match conn
            .send_raw_pipeline_joined(raw_pipeline, response_count)
            .await
        {
            Ok(responses) => {
                self.release(conn);
                Ok(responses)
            }
            Err(err) => {
                warn!(
                    backend = %self.node,
                    error = %err,
                    "cluster joined raw pipeline backend pool invalidated connection"
                );
                self.discard(conn);
                Err(err)
            }
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster pipeline 后端连接池和背压控制 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应及分段耗时；容量、连接、协议或响应大小异常时返回错误。
    // 执行连续 raw pipeline 并返回分段耗时，用于定位 pool acquire 与 Redis I/O 成本。
    async fn send_raw_pipeline_joined_profiled(
        &self,
        raw_pipeline: Bytes,
        response_count: usize,
    ) -> anyhow::Result<(BackendRawResponses, PipelinePoolProfile)> {
        let acquire_started = Instant::now();
        let mut conn = self.acquire().await?;
        let acquire_micros = duration_micros_u64(acquire_started.elapsed());
        match conn
            .send_raw_pipeline_joined_profiled(raw_pipeline, response_count)
            .await
        {
            Ok((responses, io_profile)) => {
                self.release(conn);
                Ok((
                    responses,
                    PipelinePoolProfile {
                        acquire_micros,
                        backend_write_micros: io_profile.write_micros,
                        backend_read_micros: io_profile.read_micros,
                    },
                ))
            }
            Err(err) => {
                warn!(
                    backend = %self.node,
                    error = %err,
                    "cluster profiled joined raw pipeline backend pool invalidated connection"
                );
                self.discard(conn);
                Err(err)
            }
        }
    }

    /// 业务作用: 借出一条连接，用于实现 idle-first、没有 idle 才按 max_active 新建、满池时有界等待。 等待使用进入本函数时起算的总预算(deadline),被唤醒后再度落空不会重置超时。
    ///
    /// 返回: 优先返回健康 idle 连接，必要时新建；池满等待超时或建连失败时返回错误。
    // 借出一条连接，用于实现 idle-first、没有 idle 才按 max_active 新建、满池时有界等待。
    // 等待使用进入本函数时起算的总预算(deadline),被唤醒后再度落空不会重置超时。
    async fn acquire(&self) -> anyhow::Result<BackendConnection> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(self.submit_timeout_ms);
        loop {
            // 锁内只做决策,await 全部在锁外。
            let (step, purged) = {
                let mut state = self
                    .state
                    .lock()
                    .expect("cluster pipeline backend pool state mutex poisoned");
                // 默认 LIFO:优先复用最近归还的热连接,减少活跃连接扩散和冷连接命中。
                // 可用 NARIX_V1_PIPELINE_POOL_LIFO=0 回退 FIFO 做公平性诊断。
                let mut purged: Vec<IdleConnection> = Vec::new();
                let reusable = loop {
                    let next_idle = if self.reuse_lifo {
                        state.idle.pop_back()
                    } else {
                        state.idle.pop_front()
                    };
                    match next_idle {
                        Some(entry)
                            if !self.idle_entry_expired(&entry)
                                && entry.conn.is_idle_connection_usable() =>
                        {
                            break Some(entry.conn);
                        }
                        Some(expired) => purged.push(expired),
                        None => break None,
                    }
                };
                state.active = state.active.saturating_sub(purged.len());
                let step = if let Some(conn) = reusable {
                    AcquireStep::Reuse(conn)
                } else if state.active < self.max_active {
                    state.active += 1;
                    AcquireStep::Create
                } else {
                    if state.waiters >= self.queue_size {
                        bail!(
                            "cluster pipeline backend `{}` wait queue full: {}",
                            self.node,
                            self.queue_size
                        );
                    }
                    state.waiters += 1;
                    AcquireStep::Wait
                };
                (step, purged)
            };
            // 过龄连接在锁外关闭;释放的 active 名额唤醒等待者。
            let purged_count = purged.len();
            drop(purged);
            for _ in 0..purged_count {
                self.notify.notify_one();
            }
            match step {
                AcquireStep::Reuse(conn) => return Ok(conn),
                AcquireStep::Create => {
                    match connect_with_retry(&self.node, self.protocol_version, &self.factory).await
                    {
                        Ok(conn) => return Ok(conn),
                        Err(err) => {
                            self.release_active_slot();
                            return Err(err);
                        }
                    }
                }
                AcquireStep::Wait => {
                    let wait_result =
                        tokio::time::timeout_at(deadline, self.notify.notified()).await;
                    let mut state = self
                        .state
                        .lock()
                        .expect("cluster pipeline backend pool state mutex poisoned");
                    state.waiters = state.waiters.saturating_sub(1);
                    drop(state);
                    wait_result.with_context(|| {
                        format!(
                            "cluster pipeline backend `{}` acquire timeout after {}ms",
                            self.node, self.submit_timeout_ms
                        )
                    })?;
                }
            }
        }
    }

    /// 业务作用: 判断 idle 连接是否超过回收阈值,用于 acquire 拒用与后台回收共用同一口径。
    /// 参数说明:
    /// - `entry`: 当前正在合并或校验的配置条目，按其业务标识定位目标位置。
    ///
    // 判断 idle 连接是否超过回收阈值,用于 acquire 拒用与后台回收共用同一口径。
    fn idle_entry_expired(&self, entry: &IdleConnection) -> bool {
        match self.idle_timeout {
            Some(ttl) => entry.idle_since.elapsed() > ttl,
            None => false,
        }
    }

    // 回收过龄 idle 连接,用于后台 reaper 周期调用;返回本次回收数量。
    /// 业务作用: 回收过龄 idle 连接,用于后台 reaper 周期调用;返回本次回收数量。
    fn reap_expired_idle(&self) -> usize {
        if self.idle_timeout.is_none() {
            return 0;
        }
        let purged = {
            let mut state = self
                .state
                .lock()
                .expect("cluster pipeline backend pool state mutex poisoned");
            // LIFO/FIFO 都从队首回收冷连接:release 始终 push_back,最近归还的热连接在队尾。
            let mut purged: Vec<IdleConnection> = Vec::new();
            while let Some(front) = state.idle.front() {
                if !self.idle_entry_expired(front) {
                    break;
                }
                if let Some(entry) = state.idle.pop_front() {
                    purged.push(entry);
                }
            }
            state.active = state.active.saturating_sub(purged.len());
            purged
        };
        let count = purged.len();
        drop(purged);
        if count > 0 {
            for _ in 0..count {
                self.notify.notify_one();
            }
        }
        count
    }

    /// 业务作用: 归还健康连接，用于让后续 pipeline 优先复用 idle connection。
    /// 参数说明:
    /// - `conn`: 当前已建立的 Redis 协议连接，函数在其上完成认证、收发或状态校验。
    ///
    // 归还健康连接，用于让后续 pipeline 优先复用 idle connection。
    fn release(&self, conn: BackendConnection) {
        let mut state = self
            .state
            .lock()
            .expect("cluster pipeline backend pool state mutex poisoned");
        state.idle.push_back(IdleConnection {
            conn,
            idle_since: Instant::now(),
        });
        drop(state);
        self.notify.notify_one();
    }

    /// 业务作用: 丢弃坏连接，用于响应错误或连接断开后释放 active 名额。
    /// 参数说明:
    /// - `_conn`: 保留的连接占位参数；当前实现不读取它，但签名需与统一回调契约一致。
    ///
    // 丢弃坏连接，用于响应错误或连接断开后释放 active 名额。
    fn discard(&self, _conn: BackendConnection) {
        self.release_active_slot();
    }

    // 释放一个 active 名额，用于连接创建失败或坏连接丢弃后唤醒等待者。
    /// 业务作用: 释放一个 active 名额，用于连接创建失败或坏连接丢弃后唤醒等待者。
    fn release_active_slot(&self) {
        let mut state = self
            .state
            .lock()
            .expect("cluster pipeline backend pool state mutex poisoned");
        state.active = state.active.saturating_sub(1);
        drop(state);
        self.notify.notify_one();
    }
}

/// 业务作用: 建立 Redis Cluster pipeline 后端连接池和背压控制 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
/// 参数说明:
/// - `node`: `host:port` 后端地址，重试期间始终连接同一节点。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `factory`: 后端连接工厂，用于按目标地址和认证配置创建受控连接。
///
/// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
// 创建后端连接并短重试，用于 pool 首次扩容或坏连接丢弃后的补充。
async fn connect_with_retry(
    node: &str,
    protocol_version: Option<u8>,
    factory: &BackendConnectionFactory,
) -> anyhow::Result<BackendConnection> {
    const CONNECT_ATTEMPTS: usize = 3;
    for attempt in 1..=CONNECT_ATTEMPTS {
        match factory.connect(node, protocol_version).await {
            Ok(conn) => return Ok(conn),
            Err(_) if attempt < CONNECT_ATTEMPTS => {
                tokio::time::sleep(Duration::from_millis((attempt * 20) as u64)).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("CONNECT_ATTEMPTS loop always returns");
}

impl BackendConnectionFactory {
    /// 业务作用: 建立 Redis Cluster pipeline 后端连接池和背压控制 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    // 创建后端连接，用于 pool 扩容或连接失效后的重连。
    async fn connect(
        &self,
        addr: &str,
        protocol_version: Option<u8>,
    ) -> anyhow::Result<BackendConnection> {
        let mut conn = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
            addr,
            username: self.username.as_deref(),
            password: self.password.as_deref(),
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            tcp_keepalive_ms: self.tcp_keepalive_ms,
            response_bytes_limit: Some(self.response_bytes_limit),
            metrics: Some(self.metrics.clone()),
            tls: self.tls.as_ref(),
        })
        .await?;
        conn.prepare_command_compatibility(self.profile).await?;
        if let Some(protocol_version) = protocol_version {
            conn.set_protocol_version(protocol_version).await?;
        }
        Ok(conn)
    }
}

/// 业务作用: 把毫秒配置转换为可选回收阈值,0 表示不回收(连接常驻)。
/// 参数说明:
/// - `ms`: 毫秒值，`0` 表示禁用对应超时或空闲回收配置。
///
// 把毫秒配置转换为可选回收阈值,0 表示不回收(连接常驻)。
fn optional_idle_timeout(ms: u64) -> Option<Duration> {
    if ms == 0 {
        None
    } else {
        Some(Duration::from_millis(ms))
    }
}

/// 业务作用: 读取可选布尔开关,用于只在构建 pool 时决定一次行为,避免热路径每批查询环境变量。
/// 参数说明:
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
///
// 读取可选布尔开关,用于只在构建 pool 时决定一次行为,避免热路径每批查询环境变量。
fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).as_deref() {
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES") => Some(true),
        Ok("0") | Ok("false") | Ok("FALSE") | Ok("no") | Ok("NO") => Some(false),
        _ => None,
    }
}

/// 业务作用: 构造 pool key，用于把 RESP2/RESP3 后端连接池隔离开。
/// 参数说明:
/// - `node`: `host:port` 后端地址，与协议版本共同组成连接池键。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
///
// 构造 pool key，用于把 RESP2/RESP3 后端连接池隔离开。
fn pool_key(node: &str, protocol_version: Option<u8>) -> String {
    match protocol_version {
        Some(version) => format!("{node}|resp{version}"),
        None => node.to_string(),
    }
}

/// 业务作用: 生成后端认证配置，用于确保 pass-through 模式不会被 mux/pool 连接提前认证。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
// 生成后端认证配置，用于确保 pass-through 模式不会被 mux/pool 连接提前认证。
fn backend_credentials(
    item: &ProxyItem,
) -> (
    Option<String>,
    Option<crate::infrastructure::credentials::SecretText>,
) {
    if item.effective_auth_mode() == AuthMode::PassThrough {
        (None, None)
    } else {
        (item.username.clone(), item.password.clone())
    }
}
