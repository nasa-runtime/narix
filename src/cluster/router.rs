use crate::infrastructure::lifecycle::{TaskScope, TaskSpawner};
use crate::infrastructure::time::duration_micros as duration_micros_u64;
use std::collections::{hash_map::RandomState, HashMap};
use std::hash::{BuildHasher, Hasher};
use std::ops::Range;
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bytes::Bytes;
use futures_util::future::join_all;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tracing::warn;

use crate::admin::{ClusterAggregateGuard, ProxyMetrics};
use crate::backend::BackendProfile;
use crate::cluster::backend::{
    BackendConnectOptions, BackendConnection, BackendRawResponses, BackendTlsConfig,
};
use crate::cluster::pipeline_pool::PipelineBackendPool;
use crate::cluster::plan::{plan_pipeline_for_execution, PlannedRoute};
use crate::cluster::response::local_response_slots;
use crate::cluster::slot::redis_slot;
use crate::cluster::spec::{is_hot_single_key_command, AggregatePolicy, KeylessPolicy};
use crate::cluster::topology::{load_slot_map_from_seed, SlotMap};
use crate::cluster::tunnel_pool::{TunnelLease, TunnelPool};
use crate::config::ProxyItem;
use crate::resp::{self, RespValue};

/// 业务作用: 以共享原始缓冲和参数区间表示前端命令，避免 pipeline 解析复制参数。
#[derive(Debug, Clone)]
// 表示 ClientCommand 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
pub struct ClientCommand {
    /// 客户端发送的完整原始 RESP 帧，转发时无需重新编码。
    pub raw: Bytes,
    arg_ranges: resp::CommandArgRanges,
}

impl ClientCommand {
    /// 业务作用: 从原始命令和参数区间构造零复制的借用命令视图。
    /// 参数说明:
    /// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
    /// - `arg_ranges`: 命令参数在原始帧中的边界集合，用于无复制地定位和改写指定参数。
    ///
    /// 返回: 命令接管原始字节与参数区间；区间仍须由调用方保证位于原始帧内。
    pub fn new(raw: Bytes, arg_ranges: Vec<Range<usize>>) -> Self {
        Self::from_compact_ranges(raw, resp::CommandArgRanges::Heap(arg_ranges))
    }

    /// 业务作用: 接管已解析的紧凑参数区间，使转发路径无需再次构造参数容器。
    /// 参数说明:
    /// - `raw`: 与参数区间对应的完整原始命令帧。
    /// - `arg_ranges`: 已通过协议校验、相对原始命令起点的参数区间。
    ///
    /// 返回: 命令共享原始帧并接管参数元数据，不复制参数内容。
    #[rustfmt::skip]
    pub(crate) fn from_compact_ranges(raw: Bytes, arg_ranges: resp::CommandArgRanges) -> Self {
        Self {
            raw,
            arg_ranges,
        }
    }

    /// 业务作用: 查询指定参数切片，用于在 cluster 热路径上避免重新构造整条命令的参数 Vec。
    /// 参数说明:
    /// - `idx`: 当前元素或参数下标，用于在保持原始顺序的同时定位处理位置。
    ///
    // 查询指定参数切片，用于在 cluster 热路径上避免重新构造整条命令的参数 Vec。
    pub fn arg(&self, idx: usize) -> Option<&[u8]> {
        self.arg_ranges.get(idx).map(|range| &self.raw[range])
    }

    /// 业务作用: 返回借用命令包含的参数数量。
    /// 返回已解析参数数量，不重新分配参数切片集合。
    pub fn arg_count(&self) -> usize {
        self.arg_ranges.len()
    }

    /// 业务作用: 按需生成参数切片迭代结果，避免解析热路径复制参数。
    /// 按命令原始顺序返回全部参数切片；切片继续借用原始 RESP 帧。
    pub fn arg_slices(&self) -> Vec<&[u8]> {
        self.arg_ranges
            .iter()
            .map(|range| &self.raw[range])
            .collect()
    }
}

/// 业务作用: 区分逐帧响应与连续 joined 响应，写回时保持相同顺序和统计口径。
#[derive(Debug)]
// 表示 cluster pipeline 的前端回包形态，Raw 用于热路径避免后端响应逐帧分配。
pub enum ClusterPipelineResponse {
    /// 已分别编码的响应帧，适用于需要代理本地构造或聚合的路径。
    Frames(Vec<Vec<u8>>),
    /// 后端原始连续响应及帧边界，适用于无复制快速转发路径。
    Raw(BackendRawResponses),
}

impl ClusterPipelineResponse {
    /// 业务作用: 返回聚合 Cluster 响应应写回的前端帧数。
    /// 返回前端应收到的响应帧数量。
    pub fn len(&self) -> usize {
        match self {
            Self::Frames(frames) => frames.len(),
            Self::Raw(raw) => raw.len(),
        }
    }

    /// 业务作用: 判断聚合 Cluster 响应是否没有可写回帧。
    /// 判断批次是否不包含任何前端响应帧。
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Frames(frames) => frames.is_empty(),
            Self::Raw(raw) => raw.is_empty(),
        }
    }

    /// 业务作用: 返回聚合响应写回前端的总字节数，供 pending 限制和指标使用。
    /// 返回完整批次需要写回前端的总字节数。
    pub fn bytes_to_write(&self) -> usize {
        match self {
            Self::Frames(frames) => frames.iter().map(Vec::len).sum(),
            Self::Raw(raw) => raw.bytes.len(),
        }
    }

    /// 业务作用: 返回聚合响应中的错误帧数量。
    /// 返回批次中的 Redis 错误帧数量，供命令错误指标累计。
    pub fn error_count(&self) -> usize {
        match self {
            Self::Frames(frames) => frames
                .iter()
                .filter(|frame| frame.starts_with(b"-"))
                .count(),
            Self::Raw(raw) => raw.error_count,
        }
    }

    /// 业务作用: 把聚合响应按前端顺序转换为独立帧集合。
    /// 转换成逐帧拥有型响应；原始连续缓冲区会按既有边界切分。
    pub fn into_frames(self) -> Vec<Vec<u8>> {
        match self {
            Self::Frames(frames) => frames,
            Self::Raw(raw) => backend_raw_into_frames(raw),
        }
    }
}

/// 业务作用: 用共享连续缓冲与帧索引表示保序 Cluster 响应，并预计算字节数和错误数。
#[derive(Debug)]
// 表示按 frontend sequence 排好序的 joined cluster 响应。
pub struct ClusterJoinedResponses {
    buffers: Vec<BackendRawResponses>,
    slots: Vec<ClusterResponseSlot>,
    bytes_to_write: usize,
    error_count: usize,
}

impl ClusterJoinedResponses {
    /// 业务作用: 构造 joined response 索引表，并预计算 metrics 所需的字节数与错误数。
    /// 参数说明:
    /// - `buffers`: 按节点暂存的请求或响应缓冲区，用于分桶发送和最终保序合并。
    /// - `slots`: 命令涉及的槽位集合，用于判断单槽位、跨槽位或聚合策略。
    ///
    // 构造 joined response 索引表，并预计算 metrics 所需的字节数与错误数。
    fn new(buffers: Vec<BackendRawResponses>, slots: Vec<ClusterResponseSlot>) -> Self {
        let mut bytes_to_write = 0usize;
        let mut error_count = 0usize;
        for slot in &slots {
            let frame = cluster_response_slot_frame(&buffers, slot);
            bytes_to_write = bytes_to_write.saturating_add(frame.len());
            if frame.starts_with(b"-") {
                error_count += 1;
            }
        }
        Self {
            buffers,
            slots,
            bytes_to_write,
            error_count,
        }
    }

    /// 业务作用: 返回 joined 响应索引表中的帧数量。
    /// 返回按前端原始命令顺序索引的响应帧数量。
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// 业务作用: 判断 joined 响应索引表是否为空。
    /// 判断聚合结果是否不包含任何前端响应帧。
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// 业务作用: 返回第 index 条 frontend 响应帧的字节切片。
    /// 参数说明:
    /// - `index`: 当前数组、命令或节点位置，用于保持输入与输出的顺序关系。
    ///
    // 返回第 index 条 frontend 响应帧的字节切片。
    pub fn frame(&self, index: usize) -> &[u8] {
        cluster_response_slot_frame(&self.buffers, &self.slots[index])
    }

    /// 业务作用: 返回 joined 响应实际需要写回的总字节数。
    /// 返回各节点响应按前端顺序聚合后的总字节数。
    pub fn bytes_to_write(&self) -> usize {
        self.bytes_to_write
    }

    /// 业务作用: 返回 joined 响应中预计算的错误帧数量。
    /// 返回聚合结果中的 Redis 错误帧数量。
    pub fn error_count(&self) -> usize {
        self.error_count
    }

    // 按 frontend sequence 组装连续 raw response；自然单 buffer 顺序时直接复用后端原始 buffer。
    /// 业务作用: 按 frontend sequence 组装连续 raw response；自然单 buffer 顺序时直接复用后端原始 buffer。
    fn into_ordered_raw(mut self) -> BackendRawResponses {
        if self.buffers.len() == 1
            && self.slots.len() == self.buffers[0].len()
            && self.slots.iter().enumerate().all(|(idx, slot)| {
                matches!(slot, ClusterResponseSlot::Joined { buffer: 0, frame } if *frame == idx)
            })
        {
            return self.buffers.remove(0);
        }

        let mut bytes = Vec::with_capacity(self.bytes_to_write);
        let mut frame_ranges = Vec::with_capacity(self.slots.len());
        let mut error_count = 0usize;
        let Self { buffers, slots, .. } = self;
        for slot in slots {
            let start = bytes.len();
            match slot {
                ClusterResponseSlot::Joined { buffer, frame } => {
                    let range = buffers[buffer].frame_ranges[frame].clone();
                    bytes.extend_from_slice(&buffers[buffer].bytes[range]);
                }
                ClusterResponseSlot::Owned(frame) => bytes.extend_from_slice(&frame),
            }
            let end = bytes.len();
            if bytes[start] == b'-' {
                error_count += 1;
            }
            frame_ranges.push(start..end);
        }

        BackendRawResponses {
            bytes,
            frame_ranges,
            error_count,
        }
    }
}

/// 业务作用: 按 frontend sequence 组装连续 raw response，用于 hot path 避免先统计再复制的双遍历。
/// 参数说明:
/// - `buffers`: 按节点暂存的请求或响应缓冲区，用于分桶发送和最终保序合并。
/// - `slots`: 命令涉及的槽位集合，用于判断单槽位、跨槽位或聚合策略。
///
// 按 frontend sequence 组装连续 raw response，用于 hot path 避免先统计再复制的双遍历。
fn build_ordered_raw_response(
    mut buffers: Vec<BackendRawResponses>,
    slots: Vec<ClusterResponseSlot>,
) -> BackendRawResponses {
    if buffers.len() == 1
        && slots.len() == buffers[0].len()
        && slots.iter().enumerate().all(|(idx, slot)| {
            matches!(slot, ClusterResponseSlot::Joined { buffer: 0, frame } if *frame == idx)
        })
    {
        return buffers.remove(0);
    }

    let mut bytes = Vec::with_capacity(
        buffers
            .iter()
            .map(|buffer| buffer.bytes.len())
            .sum::<usize>(),
    );
    let mut frame_ranges = Vec::with_capacity(slots.len());
    let mut error_count = 0usize;

    for slot in slots {
        let start = bytes.len();
        match slot {
            ClusterResponseSlot::Joined { buffer, frame } => {
                let range = buffers[buffer].frame_ranges[frame].clone();
                bytes.extend_from_slice(&buffers[buffer].bytes[range]);
            }
            ClusterResponseSlot::Owned(frame) => bytes.extend_from_slice(&frame),
        }
        let end = bytes.len();
        if bytes[start] == b'-' {
            error_count += 1;
        }
        frame_ranges.push(start..end);
    }

    BackendRawResponses {
        bytes,
        frame_ranges,
        error_count,
    }
}

/// 业务作用: 把连续 raw response 拆成逐帧 Vec，用于少数兼容路径。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
// 把连续 raw response 拆成逐帧 Vec，用于少数兼容路径。
fn backend_raw_into_frames(raw: BackendRawResponses) -> Vec<Vec<u8>> {
    let BackendRawResponses {
        bytes,
        frame_ranges,
        ..
    } = raw;
    frame_ranges
        .into_iter()
        .map(|range| bytes[range].to_vec())
        .collect()
}

/// 业务作用: 表示聚合 pipeline 中尚待填充、独立帧或 joined 缓冲区间三种响应槽状态。
#[derive(Debug)]
// 表示单个 frontend response slot 的数据来源。
enum ClusterResponseSlot {
    Joined { buffer: usize, frame: usize },
    Owned(Vec<u8>),
}

/// 业务作用: 解析 response slot 对应的字节切片，用于 joined cluster response 写回。
/// 参数说明:
/// - `buffers`: 按节点暂存的请求或响应缓冲区，用于分桶发送和最终保序合并。
/// - `slot`: 已计算的 Redis Cluster 槽位，用于查找负责该范围的 master。
///
// 解析 response slot 对应的字节切片，用于 joined cluster response 写回。
fn cluster_response_slot_frame<'a>(
    buffers: &'a [BackendRawResponses],
    slot: &'a ClusterResponseSlot,
) -> &'a [u8] {
    match slot {
        ClusterResponseSlot::Joined { buffer, frame } => {
            let response = &buffers[*buffer];
            &response.bytes[response.frame_ranges[*frame].clone()]
        }
        ClusterResponseSlot::Owned(frame) => frame,
    }
}

/// 业务作用: 聚合槽位拓扑、后端池、聚合并发门禁和 SCAN 状态，驱动 Cluster 单入口数据面。
#[derive(Debug)]
// 表示 ClusterRuntime 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
pub struct ClusterRuntime {
    tasks: TaskSpawner,
    owned_tasks: Option<TaskScope>,
    /// 当前 `proxies` 配置项的唯一名称，用于错误上下文；不同名称对应不同运行时和池集合。
    name: String,
    /// 驱动本地握手身份和 Cluster 能力边界的后端兼容档案。
    backend_profile: BackendProfile,
    /// 用于初次加载及后续刷新 slot map 的 Redis Cluster seed 地址。
    seeds: Vec<String>,
    /// 当前 proxy 访问全部 Redis Cluster 节点时使用的可选 ACL 用户名。
    username: Option<String>,
    /// 当前 proxy 访问全部 Redis Cluster 节点时使用的可选认证密码。
    password: Option<crate::infrastructure::credentials::SecretText>,
    /// 业务数据节点 RESP TLS 配置；seed、拓扑节点与重定向目标统一使用。
    backend_tls: Option<BackendTlsConfig>,
    /// 建立后端 TCP 连接允许等待的最长毫秒数。
    connect_timeout_ms: u64,
    /// 非阻塞后端请求及认证允许等待的最长毫秒数。
    backend_request_timeout_ms: u64,
    /// 后端 TCP keepalive 周期，单位为毫秒；零沿用系统默认。
    tcp_keepalive_ms: u64,
    /// 当前 proxy 最新的 slot 到真实 Redis Cluster node 路由快照。
    slot_map: Arc<RwLock<Arc<SlotMap>>>,
    /// 当前 proxy 普通无状态命令使用的 pipeline 连接池。
    pipeline_pool: Arc<PipelineBackendPool>,
    /// 当前 proxy 内按真实 Redis Cluster node 地址严格隔离的 tunnel pool。
    ///
    /// 该映射属于本 `ClusterRuntime`，不会被其他 proxy 或 Narix 实例共享；
    /// 映射锁只在特殊阻塞命令借池或拓扑新增节点时使用，SET/GET 热路径不访问。
    tunnel_pools: StdRwLock<HashMap<String, Arc<TunnelPool>>>,
    /// 每个真实 Redis Cluster node 最多保留的 clean idle tunnel 数量。
    tunnel_pool_max_idle: usize,
    /// 每个节点池的 idle 连接回收阈值，单位为毫秒。
    tunnel_pool_idle_timeout_ms: u64,
    /// 跨节点 SCAN 的代理游标状态；只服务 SCAN 聚合路径。
    scan_states: Arc<AsyncMutex<ScanStateStore>>,
    /// 限制同一 proxy 同时执行的跨节点聚合任务数量。
    aggregate_limiter: Arc<Semaphore>,
    /// 单次聚合最多允许产生的元素数量。
    aggregate_max_items: usize,
    /// 当前前端会话允许积压的最大响应字节数。
    pending_response_bytes: usize,
    /// 当前 proxy 的指标句柄；连接池仅共享计数器，不共享连接所有权。
    metrics: ProxyMetrics,
}

/// 业务作用: 保存发送到单个后端的原始命令以及对应前端序号。
#[derive(Debug, Clone)]
// 表示 BackendCommand 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
struct BackendCommand {
    sequence: usize,
    request: BackendRequest,
    target: ResponseTarget,
}

/// 业务作用: 把同一后端的热路径命令合并为一段连续 pipeline，并保留响应回填位置。
#[derive(Debug, Default)]
// 表示 HotBackendBatch 的数据结构，用于高频单 key pipeline 快路径按后端节点聚合请求。
struct HotBackendBatch {
    sequences: Vec<usize>,
    commands: Vec<Bytes>,
}

/// 业务作用: 区分需要重新编码的命令参数与可直接转发的连续原始命令。
#[derive(Debug, Clone)]
enum BackendRequest {
    Original(usize),
    Owned(Vec<u8>),
}

impl BackendRequest {
    /// 业务作用: 返回命令存储的原始连续字节视图，供批量发送复用。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    fn as_slice<'a>(&'a self, commands: &'a [ClientCommand]) -> &'a [u8] {
        match self {
            Self::Original(idx) => commands[*idx].raw.as_ref(),
            Self::Owned(raw) => raw,
        }
    }

    /// 业务作用: 转成共享请求数据，用于把后端 pipeline job 交给连接池且尽量减少复制。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    // 转成共享请求数据，用于把后端 pipeline job 交给连接池且尽量减少复制。
    fn to_shared(&self, commands: &[ClientCommand]) -> Bytes {
        match self {
            Self::Original(idx) => commands[*idx].raw.clone(),
            Self::Owned(raw) => Bytes::from(raw.clone()),
        }
    }
}

/// 业务作用: 持有跨节点聚合并发许可并在析构时同步减少活动指标。
// 表示 AggregatePermit 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
struct AggregatePermit {
    _permit: OwnedSemaphorePermit,
    _guard: ClusterAggregateGuard,
}

/// 业务作用: 描述一个后端响应应回填到单命令、求和、数组或 SCAN 聚合器的目标位置。
#[derive(Debug, Clone, Copy)]
enum ResponseTarget {
    Direct,
    Aggregate,
    AggregatePart(usize),
}

/// 业务作用: 保存跨节点命令当前的求和、数组、SCAN 或错误聚合状态。
#[derive(Debug)]
enum AggregateState {
    SumIntegers {
        remaining: usize,
        total: i64,
    },
    BulkArray {
        remaining: usize,
        items: Vec<Vec<u8>>,
        max_items: usize,
    },
    BulkConsensus {
        remaining: usize,
        value: Option<Vec<u8>>,
    },
    BulkArrayInOrder {
        remaining: usize,
        items: Vec<Option<Vec<u8>>>,
    },
}

/// 业务作用: 保存代理 SCAN 游标到后端进度的映射及下一游标编号。
#[derive(Debug)]
// 表示 ScanStateStore 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
struct ScanStateStore {
    next_id: u64,
    random_state: RandomState,
    states: HashMap<u64, ScanState>,
}

impl Default for ScanStateStore {
    /// 业务作用: 创建空的 SCAN 游标状态表和单调分配器。
    /// 参数说明: 无。
    fn default() -> Self {
        Self {
            next_id: 0,
            random_state: RandomState::new(),
            states: HashMap::new(),
        }
    }
}

/// 业务作用: 保存跨多 master 执行 `SCAN` 的代理游标状态，让客户端仍按单入口 Redis 游标模型调用。
/// 字段说明: `nodes` 和 `node_index` 标记当前扫描到的 master；`backend_cursor` 保存该节点原始游标；`tail_args` 防止客户端换参数复用游标。
#[derive(Debug)]
// 表示 ScanState 的数据结构，用于执行 Redis Cluster slot 路由、聚合和重定向处理。
struct ScanState {
    nodes: Vec<String>,
    node_index: usize,
    backend_cursor: Vec<u8>,
    tail_args: Vec<Vec<u8>>,
    updated_at: Instant,
}

/// 业务作用: 表示后端返回的 Cluster 重定向指令，供代理选择刷新拓扑或执行一次 ASKING 重试。
/// 分支说明: `Moved` 携带 slot 和新地址，表示拓扑变化；`Ask` 携带临时迁移地址，只对当前命令生效。
#[derive(Debug, Clone, PartialEq, Eq)]
enum Redirect {
    Moved { slot: u16, addr: String },
    Ask { addr: String },
}

impl ClusterRuntime {
    /// 业务作用: 从 listener 配置构造 Cluster 运行时，完成初始 slot map 加载、后端连接池和聚合限流器初始化。
    /// 参数说明:
    /// - `item`: 当前 Cluster listener 的强类型配置，提供 seeds、认证、超时、限流和响应大小上限。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 初始化完成的 Cluster runtime；任何 seed 无法加载完整 slot map 时返回错误并阻止 listener 半启动。
    pub async fn from_proxy_item(item: &ProxyItem, metrics: ProxyMetrics) -> anyhow::Result<Self> {
        let scope = TaskScope::default();
        let mut runtime = Self::with_tasks(item, metrics, scope.spawner()).await?;
        runtime.owned_tasks = Some(scope);
        Ok(runtime)
    }

    /// 业务作用: 在代理运行代的任务责任范围内构造 Cluster 路由与各节点的连接资源。
    /// 参数说明:
    /// - `item`: Cluster 路由、认证、传输和容量配置。
    /// - `metrics`: 当前代理共享计数单元。
    /// - `tasks`: 运行代负责关闭和等待的弱任务入口。
    ///
    /// 返回: 初始拓扑加载完整时返回运行时；失败不开放监听或发布部分拓扑。
    pub(crate) async fn with_tasks(
        item: &ProxyItem,
        metrics: ProxyMetrics,
        tasks: TaskSpawner,
    ) -> anyhow::Result<Self> {
        let seeds = item.cluster_nodes()?;
        let backend_tls = BackendTlsConfig::for_backend(item);
        let slot_map = load_first_slot_map(
            &seeds,
            item.username.as_deref(),
            item.password.as_deref(),
            item.connect_timeout_ms,
            item.backend_request_timeout_ms,
            item.tcp_keepalive_ms,
            backend_tls.as_ref(),
        )
        .await?;

        let pipeline_pool = Arc::new(PipelineBackendPool::with_tasks(
            item,
            metrics.clone(),
            tasks.clone(),
        ));
        let cluster_nodes = slot_map.all_nodes();
        pipeline_pool.warmup_nodes(&cluster_nodes);
        let tunnel_pools = cluster_nodes
            .iter()
            .map(|backend| {
                (
                    backend.clone(),
                    TunnelPool::with_tasks(
                        item.dedicated_tunnel_pool_max_idle,
                        item.dedicated_tunnel_pool_idle_timeout_ms,
                        metrics.clone(),
                        tasks.clone(),
                    ),
                )
            })
            .collect();

        Ok(Self {
            tasks,
            owned_tasks: None,
            name: item.name.clone(),
            backend_profile: item.backend_profile,
            seeds,
            username: item.username.clone(),
            password: item.password.clone(),
            backend_tls,
            connect_timeout_ms: item.connect_timeout_ms,
            backend_request_timeout_ms: item.backend_request_timeout_ms,
            tcp_keepalive_ms: item.tcp_keepalive_ms,
            slot_map: Arc::new(RwLock::new(Arc::new(slot_map))),
            pipeline_pool,
            tunnel_pools: StdRwLock::new(tunnel_pools),
            tunnel_pool_max_idle: item.dedicated_tunnel_pool_max_idle,
            tunnel_pool_idle_timeout_ms: item.dedicated_tunnel_pool_idle_timeout_ms,
            scan_states: Arc::new(AsyncMutex::new(ScanStateStore::default())),
            aggregate_limiter: Arc::new(Semaphore::new(item.aggregate_max_concurrency)),
            aggregate_max_items: item.aggregate_max_items,
            pending_response_bytes: item.pending_response_bytes,
            metrics,
        })
    }

    /// 业务作用: 从配置的 seed 重新加载 `CLUSTER SLOTS` 并原子替换 slot map，用于周期刷新和 MOVED 后修正路由。
    ///
    /// 返回: 刷新成功返回 `Ok`；所有 seed 都失败时保留旧拓扑并返回错误。
    pub async fn refresh_topology(&self) -> anyhow::Result<()> {
        let slot_map = load_first_slot_map(
            &self.seeds,
            self.username.as_deref(),
            self.password.as_deref(),
            self.connect_timeout_ms,
            self.backend_request_timeout_ms,
            self.tcp_keepalive_ms,
            self.backend_tls.as_ref(),
        )
        .await?;
        // 先为拓扑中新出现的真实 node 建立该 proxy 私有的池对象，再发布 slot map。
        // 旧 node 的池暂时保留，让已借出的 lease 能安全归还或自然过期，不会串到新地址。
        self.ensure_tunnel_pools(&slot_map.all_nodes())?;
        *self.slot_map.write().await = Arc::new(slot_map);
        Ok(())
    }

    /// 业务作用: 为一条单 slot 阻塞命令选择真实 Redis Cluster node，借出其独立池连接并写入命令。
    ///
    /// 参数说明:
    /// - `slot`: 已校验全部 key 一致后得到的 Redis Cluster hash slot。
    /// - `protocol_version`: 前端协商的 RESP 版本；不同版本在节点池内继续隔离。
    /// - `raw_pipeline`: 单条阻塞命令的原始 RESP 字节，发送时不重新编码或复制。
    ///
    /// 返回:
    /// - 成功时返回真实 node 地址和已写入命令的独占 lease。
    /// - slot 无主节点、池锁异常、建连、认证、协议同步或写入失败时返回明确错误。
    ///
    /// 关键步骤:
    /// 1. 先从本 runtime 的 slot map 得到唯一真实 node。
    /// 2. 只从该 node 在本 runtime 中的独立池借连接，绝不访问全局池。
    /// 3. 未命中时只新建到同一 node 的连接；写入失败不重试，避免重复执行命令。
    pub async fn acquire_tunnel_for_slot(
        &self,
        slot: u16,
        protocol_version: Option<u8>,
        raw_pipeline: &[u8],
    ) -> anyhow::Result<(String, TunnelLease)> {
        let backend = {
            let slot_map = self.slot_map.read().await;
            slot_map
                .node_for_slot(slot)
                .map(str::to_owned)
                .with_context(|| format!("cluster slot {slot} has no master"))?
        };
        let lease = self
            .acquire_tunnel_for_backend(&backend, protocol_version, raw_pipeline, false)
            .await?;
        Ok((backend, lease))
    }

    /// 业务作用: 按 Redis Cluster `MOVED`/`ASK` 错误指定的真实 node 重试一条阻塞命令。
    ///
    /// 参数说明:
    /// - `protocol_version`: 前端协商的 RESP 版本；重试连接仍按版本隔离。
    /// - `raw_pipeline`: 原始单条阻塞命令字节；直接借用前端批次，不重新编码或复制。
    /// - `redirect_frame`: 首次目标返回的完整 `MOVED`/`ASK` RESP 错误帧。
    ///
    /// 返回:
    /// - 成功时返回重定向 node 地址和已经写入原命令的独占 lease。
    /// - 响应不是合法重定向、拓扑目标无法连接、`ASKING` 失败或命令写入失败时返回错误。
    ///
    /// 关键步骤:
    /// 1. `MOVED` 先尝试刷新 slot map，但刷新失败不阻止按错误帧中的权威地址重试。
    /// 2. `ASK` 在同一独占连接上先完成 `ASKING`，再写入原阻塞命令。
    /// 3. 重试仍从 `(当前 proxy, 重定向 node)` 私有池获取，不会跨 proxy 或跨 node 借连接。
    pub async fn acquire_tunnel_for_redirect(
        &self,
        protocol_version: Option<u8>,
        raw_pipeline: &[u8],
        redirect_frame: &[u8],
    ) -> anyhow::Result<(String, TunnelLease)> {
        let redirect = parse_redirect(redirect_frame)
            .context("cluster tunnel retry requires a valid MOVED or ASK response")?;
        let (backend, send_asking) = match redirect {
            Redirect::Moved { slot, addr } => {
                if let Err(err) = self.refresh_topology().await {
                    warn!(
                        proxy = %self.name,
                        slot,
                        error = %err,
                        "cluster topology refresh before blocking MOVED retry failed"
                    );
                }
                (addr, false)
            }
            Redirect::Ask { addr } => (addr, true),
        };
        let lease = self
            .acquire_tunnel_for_backend(&backend, protocol_version, raw_pipeline, send_asking)
            .await?;
        Ok((backend, lease))
    }

    /// 业务作用: 从当前 proxy 指定真实后端的独立池借连接，可选执行 `ASKING`，再写入阻塞命令。
    ///
    /// 参数说明:
    /// - `backend`: slot map 或重定向错误给出的真实 Redis Cluster node 地址。
    /// - `protocol_version`: 前端协商的 RESP 版本，用于池队列隔离和新连接协议同步。
    /// - `raw_pipeline`: 一条已经完整解析的阻塞命令原始字节。
    /// - `send_asking`: 是否必须先在同一连接执行并确认 `ASKING` 成功。
    ///
    /// 返回:
    /// - 返回已写入阻塞命令的独占 lease；调用方必须完整读取唯一响应后再判定 clean。
    /// - 池锁、建连、认证、协议同步、`ASKING` 或命令写入失败时返回带 node 上下文的错误。
    ///
    /// 关键步骤:
    /// 1. 池命中只移动连接所有权；未命中才建立到该 node 的新连接。
    /// 2. `ASKING` 的响应在写业务命令前完整消费，防止把内部响应误交给业务客户端。
    /// 3. 任一步失败时 lease 默认 tainted 并关闭，绝不把不确定协议状态归池。
    async fn acquire_tunnel_for_backend(
        &self,
        backend: &str,
        protocol_version: Option<u8>,
        raw_pipeline: &[u8],
        send_asking: bool,
    ) -> anyhow::Result<TunnelLease> {
        let tunnel_pool = self.tunnel_pool_for_backend(backend)?;
        let mut lease = match tunnel_pool.acquire_idle(protocol_version) {
            Some(lease) => lease,
            None => {
                let mut backend_connection =
                    BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
                        addr: backend,
                        username: self.username.as_deref(),
                        password: self.password.as_deref(),
                        connect_timeout_ms: self.connect_timeout_ms,
                        request_timeout_ms: self.backend_request_timeout_ms,
                        tcp_keepalive_ms: self.tcp_keepalive_ms,
                        response_bytes_limit: Some(self.pending_response_bytes),
                        metrics: Some(self.metrics.clone()),
                        tls: self.backend_tls.as_ref(),
                    })
                    .await
                    .with_context(|| {
                        format!(
                            "cluster proxy `{}` connect dedicated tunnel backend `{backend}`",
                            self.name
                        )
                    })?;
                backend_connection
                    .prepare_command_compatibility(self.backend_profile)
                    .await?;
                if let Some(version) = protocol_version {
                    backend_connection
                        .set_protocol_version(version)
                        .await
                        .with_context(|| {
                            format!(
                                "cluster proxy `{}` sync RESP protocol with dedicated backend `{backend}`",
                                self.name
                            )
                        })?;
                }
                tunnel_pool.lease_new(protocol_version, backend_connection)
            }
        };
        if send_asking {
            let started = Instant::now();
            let asking_result = async {
                lease
                    .connection_mut()
                    .write_raw_pipeline(resp::array(&[b"ASKING"]))
                    .await?;
                let response = lease.connection_mut().read_one_response_compact().await?;
                if response.bytes != b"+OK\r\n" {
                    bail!(
                        "cluster ASKING returned unexpected response: {}",
                        String::from_utf8_lossy(&response.bytes).trim_end()
                    );
                }
                if lease.connection_mut().has_buffered_response_bytes() {
                    bail!("cluster ASKING left unexpected buffered response bytes");
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            self.metrics.record_backend_forward_result(
                1,
                duration_micros_u64(started.elapsed()),
                asking_result.is_err(),
            );
            asking_result.with_context(|| {
                format!(
                    "cluster proxy `{}` prepare ASK tunnel backend `{backend}`",
                    self.name
                )
            })?;
        }
        lease
            .connection_mut()
            .write_raw_pipeline(raw_pipeline)
            .await
            .with_context(|| {
                format!(
                    "cluster proxy `{}` write dedicated tunnel backend `{backend}`",
                    self.name
                )
            })?;
        Ok(lease)
    }

    /// 业务作用: 获取或创建当前 proxy 内、指定真实 Redis Cluster node 独占的 tunnel pool。
    ///
    /// 参数说明:
    /// - `backend`: 已由当前 slot map 解析出的真实 Redis Cluster node 地址。
    ///
    /// 返回:
    /// - 返回只属于 `(当前 proxy, backend)` 的共享池句柄。
    /// - 池映射锁损坏时返回错误，不回退到任何其他节点或全局池。
    ///
    /// 关键步骤:
    /// - 读命中只克隆 `Arc`；缺失时在写锁内执行一次 entry 初始化，避免并发创建重复池。
    fn tunnel_pool_for_backend(&self, backend: &str) -> anyhow::Result<Arc<TunnelPool>> {
        if let Some(pool) = self
            .tunnel_pools
            .read()
            .map_err(|_| anyhow::anyhow!("cluster tunnel pool read lock poisoned"))?
            .get(backend)
            .cloned()
        {
            return Ok(pool);
        }
        let mut pools = self
            .tunnel_pools
            .write()
            .map_err(|_| anyhow::anyhow!("cluster tunnel pool write lock poisoned"))?;
        Ok(pools
            .entry(backend.to_owned())
            .or_insert_with(|| {
                TunnelPool::with_tasks(
                    self.tunnel_pool_max_idle,
                    self.tunnel_pool_idle_timeout_ms,
                    self.metrics.clone(),
                    self.tasks.clone(),
                )
            })
            .clone())
    }

    /// 业务作用: 确保当前拓扑的每个真实 node 都有当前 proxy 私有的独立 tunnel pool。
    ///
    /// 参数说明:
    /// - `backends`: 新 slot map 中去重后的真实 Redis Cluster node 地址集合。
    ///
    /// 返回:
    /// - 全部节点池存在时返回成功；池映射锁异常时停止发布新拓扑并返回错误。
    fn ensure_tunnel_pools(&self, backends: &[String]) -> anyhow::Result<()> {
        for backend in backends {
            self.tunnel_pool_for_backend(backend)?;
        }
        Ok(())
    }

    /// 业务作用: 执行前端一批 pipeline 命令，优先尝试高频单 key 快路径，失败再进入完整 Cluster 语义规划。
    /// 参数说明:
    /// - `commands`: 已解析出的前端命令列表，必须保持原始顺序以便最终按序回包。
    ///
    /// 返回: 返回可写回客户端的响应集合；单后端快路径可能保留 joined raw 以减少复制。
    // 执行一批来自前端连接的 pipeline 命令，热路径尽量保留后端 joined response 形态。
    pub async fn execute_pipeline(
        &self,
        commands: &[ClientCommand],
    ) -> anyhow::Result<ClusterPipelineResponse> {
        if commands.is_empty() {
            return Ok(ClusterPipelineResponse::Frames(Vec::new()));
        }
        if let Some(responses) = self.try_execute_hot_single_key_pipeline(commands).await? {
            return Ok(responses);
        }

        self.execute_pipeline_frames(commands)
            .await
            .map(ClusterPipelineResponse::Frames)
    }

    /// 业务作用: 执行完整 Cluster 语义 pipeline，负责本地响应、单 slot 路由、全节点聚合和跨 slot 拆分聚合。
    /// 参数说明:
    /// - `commands`: 已解析出的前端命令列表，`sequence` 必须和客户端 pipeline 顺序一致。
    ///
    /// 返回: 每条前端命令一帧响应；后端失败会转成对应 sequence 的 Redis 错误帧。
    // 执行需要完整 cluster 语义的 pipeline：规划路由、处理聚合并按原始 sequence 回填 owned frame。
    async fn execute_pipeline_frames(
        &self,
        commands: &[ClientCommand],
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        if commands.is_empty() {
            return Ok(Vec::new());
        }

        let command_args: Vec<Vec<&[u8]>> =
            commands.iter().map(ClientCommand::arg_slices).collect();
        let plan = plan_pipeline_for_execution(&command_args)?;
        let mut responses = local_response_slots(self.backend_profile, &command_args, &plan);
        let slot_map = self.slot_map.read().await.clone();
        let default_node = slot_map
            .default_node()
            .context("cluster slot map has no default node")?;

        let mut batches: HashMap<&str, Vec<BackendCommand>> = HashMap::new();
        let mut aggregates: HashMap<usize, AggregateState> = HashMap::new();
        let mut aggregate_permit = None;
        let all_nodes = plan
            .entries
            .iter()
            .any(|entry| matches!(entry.route, PlannedRoute::Keyless(KeylessPolicy::AllNodes)))
            .then(|| slot_map.all_nodes());

        for entry in &plan.entries {
            let sequence = entry.sequence;
            if responses[sequence].is_some() {
                continue;
            }

            match &entry.route {
                PlannedRoute::Keyless(KeylessPolicy::DefaultNode) => {
                    push_backend(
                        &mut batches,
                        default_node,
                        sequence,
                        BackendRequest::Original(sequence),
                        ResponseTarget::Direct,
                    );
                }
                PlannedRoute::Keyless(KeylessPolicy::AllNodes) => {
                    let nodes = all_nodes.as_deref().unwrap_or(&[]);
                    if resp::command_name(&command_args[sequence]) == "SCAN" {
                        responses[sequence] =
                            Some(self.execute_scan(&commands[sequence], nodes).await);
                        continue;
                    }
                    if let Err(err) = self.ensure_pipeline_aggregate_permit(&mut aggregate_permit) {
                        responses[sequence] = Some(resp::err(&err.to_string()));
                        continue;
                    }
                    plan_all_nodes(
                        &mut batches,
                        &mut aggregates,
                        &mut responses,
                        nodes,
                        sequence,
                        &command_args[sequence],
                        self.aggregate_max_items,
                    );
                }
                PlannedRoute::Keyless(KeylessPolicy::Local) => {}
                PlannedRoute::SingleSlot(slot) => {
                    let Some(node) = slot_map.node_for_slot(*slot) else {
                        responses[sequence] =
                            Some(resp::err(&format!("cluster slot {slot} has no master")));
                        continue;
                    };
                    push_backend(
                        &mut batches,
                        node,
                        sequence,
                        BackendRequest::Original(sequence),
                        ResponseTarget::Direct,
                    );
                }
                PlannedRoute::SplitKeys { aggregate, .. } => {
                    if let Err(err) = self.ensure_pipeline_aggregate_permit(&mut aggregate_permit) {
                        responses[sequence] = Some(resp::err(&err.to_string()));
                        continue;
                    }
                    plan_split_keys(
                        &mut batches,
                        &mut aggregates,
                        &mut responses,
                        &slot_map,
                        sequence,
                        &command_args[sequence],
                        *aggregate,
                    );
                }
                PlannedRoute::LocalError(_) => {}
            }
        }

        self.execute_backend_batches(commands, &mut responses, &mut aggregates, batches)
            .await;

        for (sequence, state) in aggregates {
            if responses[sequence].is_some() {
                continue;
            }
            responses[sequence] = Some(match state {
                AggregateState::SumIntegers {
                    remaining: 0,
                    total,
                } => resp::integer(total),
                AggregateState::SumIntegers { remaining, .. } => resp::err(&format!(
                    "missing {remaining} aggregate integer response(s)"
                )),
                AggregateState::BulkArray {
                    remaining: 0,
                    items,
                    ..
                } => resp::bulk_array(&items),
                AggregateState::BulkArray { remaining, .. } => {
                    resp::err(&format!("missing {remaining} aggregate array response(s)"))
                }
                AggregateState::BulkConsensus {
                    remaining,
                    value: Some(value),
                } => {
                    if remaining == 0 {
                        resp::bulk(&value)
                    } else {
                        resp::err(&format!("missing {remaining} aggregate bulk response(s)"))
                    }
                }
                AggregateState::BulkConsensus { value: None, .. } => {
                    resp::err("missing aggregate bulk value")
                }
                AggregateState::BulkArrayInOrder {
                    remaining: 0,
                    items,
                } => resp::bulk_array_nullable(&items),
                AggregateState::BulkArrayInOrder { remaining, .. } => {
                    resp::err(&format!("missing {remaining} aggregate bulk response(s)"))
                }
            });
        }

        Ok(responses
            .into_iter()
            .enumerate()
            .map(|(idx, response)| {
                response
                    .unwrap_or_else(|| resp::err(&format!("missing response for command {idx}")))
            })
            .collect())
    }

    /// 业务作用: 尝试执行全由高频单 key 命令组成的 pipeline，用最少解析和分配换取 SET/GET 热路径吞吐。
    /// 参数说明:
    /// - `commands`: 前端 pipeline 命令，函数要求每条命令都有命令名和 key 参数。
    ///
    /// 返回: 满足快路径条件时返回响应；遇到非热命令、缺 key 或需要复杂语义时返回 `None`。
    // 尝试执行全高频单 key pipeline，用于跳过完整语义规划和参数 Vec 重建以提升 SET/GET 吞吐。
    async fn try_execute_hot_single_key_pipeline(
        &self,
        commands: &[ClientCommand],
    ) -> anyhow::Result<Option<ClusterPipelineResponse>> {
        let slot_map = self.slot_map.read().await.clone();
        let mut batches: HashMap<&str, HotBackendBatch> = HashMap::new();
        let mut slots: Vec<Option<ClusterResponseSlot>> =
            (0..commands.len()).map(|_| None).collect();

        for (sequence, command) in commands.iter().enumerate() {
            let Some(name) = command.arg(0) else {
                return Ok(None);
            };
            if !is_hot_single_key_command(name) {
                return Ok(None);
            }
            let Some(key) = command.arg(1) else {
                return Ok(None);
            };
            let slot = redis_slot(key);
            let Some(node) = slot_map.node_for_slot(slot) else {
                slots[sequence] = Some(ClusterResponseSlot::Owned(resp::err(&format!(
                    "cluster slot {slot} has no master"
                ))));
                continue;
            };
            let batch = batches.entry(node).or_default();
            batch.sequences.push(sequence);
            batch.commands.push(command.raw.clone());
        }

        Ok(Some(
            self.execute_hot_backend_batches(commands, slots, batches)
                .await,
        ))
    }

    /// 业务作用: 执行已经按后端节点分组的热路径批次，单节点时保留连续回包，多节点时按 sequence 回填。
    /// 参数说明:
    /// - `commands`: 原始前端命令，用于遇到 MOVED/ASK 时重试当前命令。
    /// - `slots`: 按前端 sequence 预留的响应位置，可能已有本地错误帧。
    /// - `batches`: 按后端节点聚合的热路径请求批次。
    ///
    /// 返回: 单节点返回 joined raw 响应，多节点返回按前端顺序排列的普通 frame 列表。
    // 执行高频单 key pipeline 快路径；单 backend 保留 joined raw，多 backend 用逐帧回填避免重排二次复制。
    async fn execute_hot_backend_batches(
        &self,
        commands: &[ClientCommand],
        mut slots: Vec<Option<ClusterResponseSlot>>,
        batches: HashMap<&str, HotBackendBatch>,
    ) -> ClusterPipelineResponse {
        if batches.len() != 1 {
            let mut responses: Vec<Option<Vec<u8>>> = slots
                .into_iter()
                .map(|slot| match slot {
                    Some(ClusterResponseSlot::Owned(frame)) => Some(frame),
                    Some(ClusterResponseSlot::Joined { .. }) | None => None,
                })
                .collect();
            self.execute_hot_backend_batches_frames(commands, &mut responses, batches)
                .await;
            return ClusterPipelineResponse::Frames(collect_responses(responses));
        }

        let mut jobs = Vec::with_capacity(batches.len());
        for (node, batch) in batches {
            let node = node.to_string();
            let checkout_started = Instant::now();
            let metrics = self.metrics.clone();
            let pool = self.pipeline_pool.clone();
            jobs.push(async move {
                let command_count = batch.commands.len();
                let result = pool.send_pipeline_joined(&node, batch.commands).await;
                metrics.record_backend_forward_result(
                    command_count,
                    duration_micros_u64(checkout_started.elapsed()),
                    result.is_err(),
                );
                (node, batch.sequences, result)
            });
        }

        let mut buffers = Vec::new();
        for (node, sequences, result) in join_all(jobs).await {
            match result {
                Ok(joined) => {
                    let buffer = buffers.len();
                    for (frame, sequence) in sequences.into_iter().enumerate() {
                        let redirect_frame = joined.frame_ranges.get(frame).and_then(|range| {
                            let response = &joined.bytes[range.clone()];
                            is_redirect_frame(response).then(|| response.to_vec())
                        });
                        slots[sequence] = Some(if let Some(response) = redirect_frame {
                            let frame = self
                                .resolve_redirect(
                                    &BackendCommand {
                                        sequence,
                                        request: BackendRequest::Original(sequence),
                                        target: ResponseTarget::Direct,
                                    },
                                    commands,
                                    response,
                                )
                                .await
                                .unwrap_or_else(|err| {
                                    resp::err(&format!("cluster redirect retry failed: {err}"))
                                });
                            ClusterResponseSlot::Owned(frame)
                        } else {
                            ClusterResponseSlot::Joined { buffer, frame }
                        });
                    }
                    buffers.push(joined);
                }
                Err(err) => {
                    warn!(
                        proxy = %self.name,
                        backend = %node,
                        error = %err,
                        "cluster hot backend pipeline failed"
                    );
                    let err = resp::err(&format!("cluster backend `{node}` error: {err:#}"));
                    for sequence in sequences {
                        slots[sequence] = Some(ClusterResponseSlot::Owned(err.clone()));
                    }
                }
            }
        }

        ClusterPipelineResponse::Raw(build_ordered_raw_response(
            buffers,
            collect_cluster_response_slots(slots),
        ))
    }

    /// 业务作用: 执行多后端热路径批次，并把每个后端响应按前端 sequence 填回响应数组。
    /// 参数说明:
    /// - `commands`: 原始前端命令，用于重定向重试。
    /// - `responses`: 按前端 sequence 排列的可变响应槽位。
    /// - `batches`: 按后端节点分组的请求批次。
    ///
    // 执行多 backend 高频 pipeline，用旧的逐帧响应数组回填，减少 joined raw 多节点重排成本。
    async fn execute_hot_backend_batches_frames(
        &self,
        commands: &[ClientCommand],
        responses: &mut [Option<Vec<u8>>],
        batches: HashMap<&str, HotBackendBatch>,
    ) {
        let mut jobs = Vec::with_capacity(batches.len());
        for (node, batch) in batches {
            let node = node.to_string();
            let checkout_started = Instant::now();
            let metrics = self.metrics.clone();
            let pool = self.pipeline_pool.clone();
            jobs.push(async move {
                let command_count = batch.commands.len();
                let result = pool.send_pipeline(&node, batch.commands).await;
                metrics.record_backend_forward_result(
                    command_count,
                    duration_micros_u64(checkout_started.elapsed()),
                    result.is_err(),
                );
                (node, batch.sequences, result)
            });
        }

        for (node, sequences, result) in join_all(jobs).await {
            match result {
                Ok(frames) => {
                    for (sequence, frame) in sequences.into_iter().zip(frames) {
                        let frame = if is_redirect_frame(&frame) {
                            self.resolve_redirect(
                                &BackendCommand {
                                    sequence,
                                    request: BackendRequest::Original(sequence),
                                    target: ResponseTarget::Direct,
                                },
                                commands,
                                frame,
                            )
                            .await
                            .unwrap_or_else(|err| {
                                resp::err(&format!("cluster redirect retry failed: {err}"))
                            })
                        } else {
                            frame
                        };
                        responses[sequence] = Some(frame);
                    }
                }
                Err(err) => {
                    warn!(
                        proxy = %self.name,
                        backend = %node,
                        error = %err,
                        "cluster hot backend pipeline failed"
                    );
                    let err = resp::err(&format!("cluster backend `{node}` error: {err:#}"));
                    for sequence in sequences {
                        responses[sequence] = Some(err.clone());
                    }
                }
            }
        }
    }

    /// 业务作用: 执行完整规划后的后端批次，把直接响应和聚合响应统一回填到前端 sequence。
    /// 参数说明:
    /// - `commands`: 原始前端命令，用于构造后端请求和处理重定向。
    /// - `responses`: 直接响应槽位，函数会按 sequence 写入成功帧或错误帧。
    /// - `aggregates`: 聚合命令状态表，按 sequence 收集多后端分片结果。
    /// - `batches`: 按后端节点分组的请求集合。
    ///
    // 执行已经分组好的后端批次，并按原始 pipeline sequence 回填普通响应或聚合响应。
    async fn execute_backend_batches(
        &self,
        commands: &[ClientCommand],
        responses: &mut [Option<Vec<u8>>],
        aggregates: &mut HashMap<usize, AggregateState>,
        batches: HashMap<&str, Vec<BackendCommand>>,
    ) {
        let mut jobs = Vec::new();
        for (node, backend_commands) in batches {
            let node = node.to_string();
            let checkout_started = Instant::now();
            let metrics = self.metrics.clone();
            let pool = self.pipeline_pool.clone();
            let raw: Vec<Bytes> = backend_commands
                .iter()
                .map(|cmd| cmd.request.to_shared(commands))
                .collect();
            jobs.push(async move {
                let result = pool.send_pipeline(&node, raw).await;
                metrics.record_backend_forward_result(
                    backend_commands.len(),
                    duration_micros_u64(checkout_started.elapsed()),
                    result.is_err(),
                );
                (node, backend_commands, result)
            });
        }

        for (node, backend_commands, result) in join_all(jobs).await {
            match result {
                Ok(frames) => {
                    for (backend_command, frame) in backend_commands.into_iter().zip(frames) {
                        let frame = if is_redirect_frame(&frame) {
                            self.resolve_redirect(&backend_command, commands, frame)
                                .await
                                .unwrap_or_else(|err| {
                                    resp::err(&format!("cluster redirect retry failed: {err}"))
                                })
                        } else {
                            frame
                        };
                        apply_backend_frame(
                            responses,
                            aggregates,
                            backend_command.sequence,
                            backend_command.target,
                            frame,
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        proxy = %self.name,
                        backend = %node,
                        error = %err,
                        "cluster backend pipeline failed"
                    );
                    for backend_command in backend_commands {
                        apply_backend_error(
                            responses,
                            aggregates,
                            backend_command.sequence,
                            &format!("cluster backend `{node}` error: {err}"),
                        );
                    }
                }
            }
        }
    }

    /// 业务作用: 确保同一前端 pipeline 只占用一个聚合并发许可，避免一批大查询成倍放大后端压力。
    /// 参数说明:
    /// - `holder`: 当前 pipeline 持有的聚合许可槽位，已有许可时直接复用。
    ///
    /// 返回: 已有许可或成功取得聚合许可时返回成功；并发上限耗尽时返回背压错误。
    // 确保同一个前端 pipeline 内的聚合命令共享一个并发许可，避免大 pipeline 被自身的多条聚合命令打爆限流。
    fn ensure_pipeline_aggregate_permit(
        &self,
        holder: &mut Option<AggregatePermit>,
    ) -> anyhow::Result<()> {
        if holder.is_some() {
            return Ok(());
        }
        *holder = Some(self.try_acquire_aggregate_permit()?);
        Ok(())
    }

    /// 业务作用: 申请 Cluster 聚合并发许可，限制 all-node、跨 slot 拆分和 SCAN 对后端请求量的放大。
    ///
    /// 返回: 聚合容量可用时返回自动释放的许可；达到并发上限时返回背压错误。
    // 申请一个集群聚合许可，用于限制 all-node、跨 slot 拆分、SCAN 这类会放大后端请求量的命令。
    fn try_acquire_aggregate_permit(&self) -> anyhow::Result<AggregatePermit> {
        let permit = match self.aggregate_limiter.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(err) => {
                self.metrics.record_cluster_aggregate_rejection();
                return Err(err).context("cluster aggregate concurrency limit reached");
            }
        };
        Ok(AggregatePermit {
            _permit: permit,
            _guard: self.metrics.start_cluster_aggregate(),
        })
    }

    /// 业务作用: 执行 Cluster 单入口 `SCAN`，把内部错误转成 Redis 错误帧，保证前端连接不中断。
    /// 参数说明:
    /// - `command`: 客户端发来的 SCAN 命令帧。
    /// - `all_nodes`: 当前 slot map 中所有 master 节点地址。
    ///
    /// 返回: 可直接写回客户端的 SCAN 响应或错误帧。
    async fn execute_scan(&self, command: &ClientCommand, all_nodes: &[String]) -> Vec<u8> {
        match self.try_execute_scan(command, all_nodes).await {
            Ok(frame) => frame,
            Err(err) => resp::err(&format!("SCAN cluster proxy error: {err}")),
        }
    }

    /// 业务作用: 在多个 master 之间推进代理 SCAN 游标，每次只扫描一个后端节点，避免一次性全量聚合撑爆内存。
    /// 参数说明:
    /// - `command`: 客户端发来的 SCAN 命令帧，包含游标和 MATCH/COUNT 等尾部参数。
    /// - `all_nodes`: 当前需要遍历的 master 节点列表。
    ///
    /// 返回: 成功返回 SCAN 响应；后端错误会恢复原游标状态，便于客户端重试。
    async fn try_execute_scan(
        &self,
        command: &ClientCommand,
        all_nodes: &[String],
    ) -> anyhow::Result<Vec<u8>> {
        let _permit = self.try_acquire_aggregate_permit()?;
        let args = command.arg_slices();
        if args.len() < 2 {
            bail!("wrong number of arguments for SCAN");
        }
        if all_nodes.is_empty() {
            return Ok(resp::scan_response(b"0", &[]));
        }

        let input_cursor = args[1];
        let tail_args = args[2..].iter().map(|arg| (*arg).to_vec()).collect();
        let (scan_id, mut state) = self
            .take_scan_state(input_cursor, tail_args, all_nodes.to_vec())
            .await?;

        if state.node_index >= state.nodes.len() {
            return Ok(resp::scan_response(b"0", &[]));
        }

        let node = state.nodes[state.node_index].clone();
        let mut args: Vec<&[u8]> = Vec::with_capacity(2 + state.tail_args.len());
        args.push(b"SCAN");
        args.push(&state.backend_cursor);
        for arg in &state.tail_args {
            args.push(arg);
        }
        let raw = resp::array(&args);

        let checkout_started = Instant::now();
        let result = self
            .pipeline_pool
            .send_pipeline(&node, vec![Bytes::from(raw)])
            .await;
        self.metrics.record_backend_forward_result(
            1,
            duration_micros_u64(checkout_started.elapsed()),
            result.is_err(),
        );
        match result {
            Ok(mut frames) => {
                let frame = frames
                    .pop()
                    .context("SCAN backend returned no response frame")?;
                if frame.starts_with(b"-") {
                    self.restore_scan_state(scan_id, state).await;
                    return Ok(frame);
                }
                let (backend_cursor, keys) = parse_scan_backend_response(&frame)?;
                if keys.len() > self.aggregate_max_items {
                    bail!(
                        "SCAN response item count exceeds limit: {} > {}",
                        keys.len(),
                        self.aggregate_max_items
                    );
                }
                if backend_cursor.as_slice() == b"0" {
                    state.node_index += 1;
                    state.backend_cursor = b"0".to_vec();
                } else {
                    state.backend_cursor = backend_cursor;
                }

                let next_cursor = self.store_scan_state(scan_id, state).await;
                Ok(resp::scan_response(next_cursor.as_bytes(), &keys))
            }
            Err(err) => {
                self.restore_scan_state(scan_id, state).await;
                Err(err)
            }
        }
    }

    /// 业务作用: 根据客户端游标取出代理保存的 SCAN 状态，游标为 `0` 时创建新的跨节点扫描状态。
    /// 参数说明:
    /// - `input_cursor`: 客户端传入的游标，可能是 `0` 或代理生成的游标。
    /// - `tail_args`: SCAN 的 MATCH/COUNT 等尾部参数，用于防止同一游标被换参数复用。
    /// - `nodes`: 本轮扫描需要遍历的 master 节点列表。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    async fn take_scan_state(
        &self,
        input_cursor: &[u8],
        tail_args: Vec<Vec<u8>>,
        nodes: Vec<String>,
    ) -> anyhow::Result<(Option<u64>, ScanState)> {
        if input_cursor == b"0" {
            return Ok((
                None,
                ScanState {
                    nodes,
                    node_index: 0,
                    backend_cursor: b"0".to_vec(),
                    tail_args,
                    updated_at: Instant::now(),
                },
            ));
        }

        let cursor = parse_proxy_scan_cursor(input_cursor)?;
        let mut store = self.scan_states.lock().await;
        cleanup_scan_states(&mut store);
        let state = remove_scan_state_if_tail_matches(&mut store, cursor, &tail_args)?;
        Ok((Some(cursor), state))
    }

    /// 业务作用: 保存尚未完成的跨节点 SCAN 状态，并返回给客户端下一轮要带回来的代理游标。
    /// 参数说明:
    /// - `scan_id`: 已存在的代理游标；首次扫描时为空，需要分配新游标。
    /// - `state`: 更新后的跨节点 SCAN 状态。
    ///
    /// 返回: 客户端下一次 SCAN 要携带的游标；全部节点结束时返回 `0`。
    async fn store_scan_state(&self, scan_id: Option<u64>, mut state: ScanState) -> String {
        if state.node_index >= state.nodes.len() {
            return "0".to_string();
        }

        state.updated_at = Instant::now();
        let mut store = self.scan_states.lock().await;
        cleanup_scan_states(&mut store);
        let id = scan_id.unwrap_or_else(|| allocate_scan_state_id(&mut store));
        store.states.insert(id, state);
        id.to_string()
    }

    /// 业务作用: 在后端请求失败时恢复原 SCAN 状态，避免客户端重试时丢失扫描进度。
    /// 参数说明:
    /// - `scan_id`: 需要恢复的代理游标；首次扫描失败时为空，无需保存。
    /// - `state`: 失败前取出的 SCAN 状态。
    ///
    async fn restore_scan_state(&self, scan_id: Option<u64>, mut state: ScanState) {
        let Some(scan_id) = scan_id else {
            return;
        };
        state.updated_at = Instant::now();
        let mut store = self.scan_states.lock().await;
        cleanup_scan_states(&mut store);
        store.states.insert(scan_id, state);
    }

    /// 业务作用: 处理后端 `MOVED`/`ASK` 响应，对当前命令做一次受控重试并在 `MOVED` 后刷新拓扑。
    /// 参数说明:
    /// - `backend_command`: 触发重定向的后端命令描述，包含原始 sequence 和响应落点。
    /// - `commands`: 前端原始命令列表，用于取回需要重试的请求字节。
    /// - `frame`: 后端返回的原始响应帧，可能是普通响应、MOVED 或 ASK。
    ///
    /// 返回: 返回一条完整且已按代理语义处理的 RESP 帧；路由、I/O 或协议失败时返回错误。
    async fn resolve_redirect(
        &self,
        backend_command: &BackendCommand,
        commands: &[ClientCommand],
        frame: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        self.resolve_raw_redirect(backend_command.request.as_slice(commands), frame)
            .await
    }

    /// 业务作用: 对普通非阻塞命令执行一次 `MOVED`/`ASK` 重试，并拒绝第二次重定向。
    ///
    /// 参数说明:
    /// - `raw_command`: 需要重试的单条原始 RESP 命令；函数只借用，不复制请求。
    /// - `frame`: 首次后端返回的完整响应，非重定向时原样返回。
    ///
    /// 返回:
    /// - 非重定向响应原样返回。
    /// - `MOVED`/`ASK` 成功时返回重试响应；第二次重定向转换为稳定代理错误。
    ///
    /// 关键步骤:
    /// 1. 直接连接错误帧指定的 node，避免继续命中旧 slot map。
    /// 2. `ASK` 使用一次 vectored pipeline 依次发送 `ASKING` 和原命令，不复制原命令。
    /// 3. `MOVED` 完成重试后刷新拓扑，刷新失败只记录日志，不覆盖本次成功响应。
    async fn resolve_raw_redirect(
        &self,
        raw_command: &[u8],
        frame: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        let Some(redirect) = parse_redirect(&frame) else {
            return Ok(frame);
        };

        let addr = match &redirect {
            Redirect::Moved { addr, .. } | Redirect::Ask { addr } => addr,
        };
        let mut conn = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
            addr,
            username: self.username.as_deref(),
            password: self.password.as_deref(),
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.backend_request_timeout_ms,
            tcp_keepalive_ms: self.tcp_keepalive_ms,
            response_bytes_limit: Some(self.pending_response_bytes),
            metrics: Some(self.metrics.clone()),
            tls: self.backend_tls.as_ref(),
        })
        .await?;

        conn.prepare_command_compatibility(self.backend_profile)
            .await?;
        let frames = match &redirect {
            Redirect::Moved { slot, .. } => {
                let retry =
                    send_tracked_backend_pipeline(&self.metrics, &mut conn, &[raw_command]).await?;
                if let Err(err) = self.refresh_topology().await {
                    warn!(
                        proxy = %self.name,
                        slot,
                        error = %err,
                        "cluster topology refresh after MOVED failed"
                    );
                }
                retry
            }
            Redirect::Ask { .. } => {
                let asking = resp::array(&[b"ASKING"]);
                let retry = send_tracked_backend_pipeline(
                    &self.metrics,
                    &mut conn,
                    &[asking.as_slice(), raw_command],
                )
                .await?;
                retry.into_iter().skip(1).collect()
            }
        };

        let frame = frames
            .into_iter()
            .next()
            .context("redirect retry returned no response")?;
        if is_redirect_frame(&frame) {
            return Ok(resp::err(
                "cluster redirect retry returned another redirect",
            ));
        }
        Ok(frame)
    }
}

/// 业务作用: 向后端发送 Redis Cluster slot 路由、聚合命令和拓扑改写 相关命令，并按 RESP 协议读取可用于继续决策的响应。
/// 参数说明:
/// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
/// - `conn`: 当前已建立的 Redis 协议连接，函数在其上完成认证、收发或状态校验。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 返回与命令顺序一致的后端响应帧并记录结果；I/O 或协议失败时返回错误。
async fn send_tracked_backend_pipeline<C>(
    metrics: &ProxyMetrics,
    conn: &mut BackendConnection,
    commands: &[C],
) -> anyhow::Result<Vec<Vec<u8>>>
where
    C: AsRef<[u8]>,
{
    let started = Instant::now();
    let result = conn.send_pipeline(commands).await;
    metrics.record_backend_forward_result(
        commands.len(),
        duration_micros_u64(started.elapsed()),
        result.is_err(),
    );
    result
}

/// 业务作用: 收拢普通响应槽位，用于按原始 pipeline 顺序返回完整响应。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
///
// 收拢普通响应槽位，用于按原始 pipeline 顺序返回完整响应。
fn collect_responses(responses: Vec<Option<Vec<u8>>>) -> Vec<Vec<u8>> {
    responses
        .into_iter()
        .enumerate()
        .map(|(idx, response)| {
            response.unwrap_or_else(|| resp::err(&format!("missing response for command {idx}")))
        })
        .collect()
}

/// 业务作用: 收拢 joined response slot，用于确保热路径异常情况下仍返回等量响应。
/// 参数说明:
/// - `slots`: 命令涉及的槽位集合，用于判断单槽位、跨槽位或聚合策略。
///
// 收拢 joined response slot，用于确保热路径异常情况下仍返回等量响应。
fn collect_cluster_response_slots(
    slots: Vec<Option<ClusterResponseSlot>>,
) -> Vec<ClusterResponseSlot> {
    slots
        .into_iter()
        .enumerate()
        .map(|(idx, slot)| {
            slot.unwrap_or_else(|| {
                ClusterResponseSlot::Owned(resp::err(&format!(
                    "missing response for command {idx}"
                )))
            })
        })
        .collect()
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
// 解析 parse_redirect 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_redirect(frame: &[u8]) -> Option<Redirect> {
    if !is_redirect_frame(frame) {
        return None;
    }
    let line = std::str::from_utf8(frame).ok()?.trim_end_matches("\r\n");
    let mut parts = line.trim_start_matches('-').split_whitespace();
    match parts.next()? {
        "MOVED" => {
            let slot: u16 = parts.next()?.parse().ok()?;
            let addr = parts.next()?.to_string();
            Some(Redirect::Moved { slot, addr })
        }
        "ASK" => {
            let _slot: u16 = parts.next()?.parse().ok()?;
            let addr = parts.next()?.to_string();
            Some(Redirect::Ask { addr })
        }
        _ => None,
    }
}

/// 业务作用: 判断后端响应是否是 Redis Cluster 重定向错误，用于让普通成功响应跳过 redirect 解析和 async retry 分支。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
// 判断后端响应是否是 Redis Cluster 重定向错误，用于让普通成功响应跳过 redirect 解析和 async retry 分支。
fn is_redirect_frame(frame: &[u8]) -> bool {
    frame.starts_with(b"-MOVED ") || frame.starts_with(b"-ASK ")
}

const SCAN_CURSOR_TTL: Duration = Duration::from_secs(300);

/// 业务作用: 删除超过 TTL 的代理 SCAN 游标，避免客户端遗弃游标导致状态泄漏。
/// 参数说明:
/// - `store`: 共享后端状态存储，用于读取或原子更新连接、租约或路由信息。
///
fn cleanup_scan_states(store: &mut ScanStateStore) {
    store
        .states
        .retain(|_, state| state.updated_at.elapsed() <= SCAN_CURSOR_TTL);
}

/// 业务作用: 分配非零且不与现存状态冲突的代理 SCAN 游标编号。
/// 参数说明:
/// - `store`: 共享后端状态存储，用于读取或原子更新连接、租约或路由信息。
///
fn allocate_scan_state_id(store: &mut ScanStateStore) -> u64 {
    loop {
        store.next_id = store.next_id.wrapping_add(1).max(1);
        let mut hasher = store.random_state.build_hasher();
        hasher.write_u64(store.next_id);
        let id = hasher.finish();
        if id != 0 && !store.states.contains_key(&id) {
            return id;
        }
    }
}

/// 业务作用: 仅在后端尾游标仍匹配时删除状态，避免并发旧请求误删新进度。
/// 参数说明:
/// - `store`: 共享后端状态存储，用于读取或原子更新连接、租约或路由信息。
/// - `cursor`: SCAN 类命令的游标位置，用于继续增量遍历并判断是否完成。
/// - `tail_args`: 命令中主参数之后的尾部选项，解析时必须保留原始顺序。
///
/// 返回: 游标存在且尾参数匹配时返回并移除状态；状态缺失或已变化时返回错误。
fn remove_scan_state_if_tail_matches(
    store: &mut ScanStateStore,
    cursor: u64,
    tail_args: &[Vec<u8>],
) -> anyhow::Result<ScanState> {
    let Some(state) = store.states.get(&cursor) else {
        bail!("invalid or expired cluster SCAN cursor");
    };
    if state.tail_args != tail_args {
        bail!("SCAN cursor arguments changed");
    }
    Ok(store
        .states
        .remove(&cursor)
        .expect("SCAN cursor exists after tail args validation"))
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
/// 返回: 输入是合法非零十进制值时返回数值；格式或范围非法时返回错误。
// 解析 parse_proxy_scan_cursor 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_proxy_scan_cursor(raw: &[u8]) -> anyhow::Result<u64> {
    let cursor = std::str::from_utf8(raw).context("SCAN cursor is not utf8")?;
    let cursor: u64 = cursor.parse().context("SCAN cursor is not an integer")?;
    if cursor == 0 {
        bail!("zero cursor should have been handled before proxy cursor parsing");
    }
    Ok(cursor)
}

/// 业务作用: 加载 Redis Cluster slot 路由、聚合命令和拓扑改写 所需数据，并按覆盖优先级或来源差异合并成统一运行视图。
/// 参数说明:
/// - `seeds`: 可尝试的集群种子节点集合，用于在单节点不可用时继续发现拓扑。
/// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
/// - `password`: 后端认证密码，调用方负责从安全配置提供。
/// - `connect_timeout_ms`: 建立后端或 coordinator TCP 连接的超时毫秒数。
/// - `request_timeout_ms`: 发送命令后等待后端或 coordinator 响应的超时毫秒数。
/// - `tcp_keepalive_ms`: 后端 TCP keepalive 配置毫秒数，用于清理异常断开的空闲连接。
/// - `tls`: 可选 RESP TLS 配置，供所有 Cluster seed 使用相同证书校验策略。
///
/// 返回: 成功发现并校验全部槽位时返回拓扑映射；连接、认证或响应解析失败时返回错误。
async fn load_first_slot_map(
    seeds: &[String],
    username: Option<&str>,
    password: Option<&str>,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    tls: Option<&BackendTlsConfig>,
) -> anyhow::Result<SlotMap> {
    let mut last_error = None;
    for seed in seeds {
        match load_slot_map_from_seed(
            seed,
            username,
            password,
            connect_timeout_ms,
            request_timeout_ms,
            tcp_keepalive_ms,
            tls,
        )
        .await
        {
            Ok(slot_map) => {
                return Ok(slot_map);
            }
            Err(err) => {
                last_error = Some(format!("{seed}: {err}"));
            }
        }
    }
    bail!(
        "failed to load CLUSTER SLOTS from seeds: {}",
        last_error.unwrap_or_else(|| "no seeds".to_string())
    )
}

/// 业务作用: 把一个后端节点加入全节点命令计划，并记录对应响应聚合位置。
/// 参数说明:
/// - `batches`: 按后端节点累计的命令批次，用于组织并发执行并保持响应顺序。
/// - `node`: 当前命令根据槽位规划得到的 `host:port` 后端地址。
/// - `sequence`: 命令在前端 pipeline 中的原始序号，用于并发执行后恢复响应顺序。
/// - `request`: 已解析的管理或代理请求，包含后续授权和执行所需信息。
/// - `target`: 当前操作的目标节点、配置项或输出位置，函数只修改该明确范围。
///
fn push_backend<'a>(
    batches: &mut HashMap<&'a str, Vec<BackendCommand>>,
    node: &'a str,
    sequence: usize,
    request: BackendRequest,
    target: ResponseTarget,
) {
    batches.entry(node).or_default().push(BackendCommand {
        sequence,
        request,
        target,
    });
}

/// 业务作用: 为需要发往所有 master 的命令生成后端批次和聚合状态，例如 DBSIZE、KEYS 和全局 SCAN 之外的聚合命令。
/// 参数说明:
/// - `batches`: 按后端节点累计的待发送命令批次。
/// - `aggregates`: 按前端 sequence 保存的聚合状态。
/// - `responses`: 直接响应槽位，参数错误或限流时会立即填入错误帧。
/// - `all_nodes`: 当前 slot map 中所有 master 节点。
/// - `sequence`: 当前前端命令在 pipeline 中的位置。
/// - `args`: 当前命令参数。
/// - `aggregate_max_items`: 聚合命令允许累计返回的最大元素数，防止 KEYS 这类命令撑爆内存。
///
// 计算 plan_all_nodes 路由规则，用于决定执行 Redis Cluster slot 路由、聚合和重定向处理的执行路径。
fn plan_all_nodes<'a>(
    batches: &mut HashMap<&'a str, Vec<BackendCommand>>,
    aggregates: &mut HashMap<usize, AggregateState>,
    responses: &mut [Option<Vec<u8>>],
    all_nodes: &'a [String],
    sequence: usize,
    args: &[&[u8]],
    aggregate_max_items: usize,
) {
    if all_nodes.is_empty() {
        responses[sequence] = Some(resp::err("cluster has no nodes"));
        return;
    }

    let name = resp::command_name(args);
    match name.as_str() {
        "DBSIZE" => {
            aggregates.insert(
                sequence,
                AggregateState::SumIntegers {
                    remaining: all_nodes.len(),
                    total: 0,
                },
            );
        }
        "KEYS" => {
            aggregates.insert(
                sequence,
                AggregateState::BulkArray {
                    remaining: all_nodes.len(),
                    items: Vec::new(),
                    max_items: aggregate_max_items,
                },
            );
        }
        "SCRIPT" if args.len() >= 2 && args[1].eq_ignore_ascii_case(b"LOAD") => {
            aggregates.insert(
                sequence,
                AggregateState::BulkConsensus {
                    remaining: all_nodes.len(),
                    value: None,
                },
            );
        }
        _ => {
            responses[sequence] = Some(resp::err(&format!(
                "{name} all-node cluster aggregation is not supported"
            )));
            return;
        }
    }

    for node in all_nodes {
        push_backend(
            batches,
            node,
            sequence,
            BackendRequest::Original(sequence),
            ResponseTarget::Aggregate,
        );
    }
}

/// 业务作用: 按 key 的 slot 把可拆分命令分组到后端，并保存原索引供响应保序聚合。
/// 参数说明:
/// - `batches`: 按后端节点累计的命令批次，用于组织并发执行并保持响应顺序。
/// - `aggregates`: 待汇总的跨节点执行结果集合，按原命令语义合并为单个响应。
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `slot_map`: 槽位到节点的映射表，用于快速路由并校验覆盖完整性。
/// - `sequence`: 命令在前端 pipeline 中的原始序号，用于并发执行后恢复响应顺序。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `aggregate`: 聚合执行上下文，保存跨节点命令的并发许可和结果合并状态。
///
// 计算 plan_split_keys 路由规则，用于决定执行 Redis Cluster slot 路由、聚合和重定向处理的执行路径。
fn plan_split_keys<'a>(
    batches: &mut HashMap<&'a str, Vec<BackendCommand>>,
    aggregates: &mut HashMap<usize, AggregateState>,
    responses: &mut [Option<Vec<u8>>],
    slot_map: &'a SlotMap,
    sequence: usize,
    args: &[&[u8]],
    aggregate: AggregatePolicy,
) {
    let name = resp::command_name(args);
    if aggregate != AggregatePolicy::SumIntegers && aggregate != AggregatePolicy::BulkArrayInOrder {
        responses[sequence] = Some(resp::err("unsupported split-key aggregate policy"));
        return;
    }
    if aggregate == AggregatePolicy::BulkArrayInOrder && name != "MGET" && name != "JSON.MGET" {
        responses[sequence] = Some(resp::err(&format!(
            "{name} split-key bulk aggregation is not supported"
        )));
        return;
    }
    if args.len() < 2 {
        responses[sequence] = Some(resp::err("split-key command requires keys"));
        return;
    }

    let (keys, json_mget_path) = if name == "JSON.MGET" {
        if args.len() < 3 {
            responses[sequence] = Some(resp::err("wrong number of arguments for JSON.MGET"));
            return;
        }
        (&args[1..args.len() - 1], Some(args[args.len() - 1]))
    } else {
        (&args[1..], None)
    };
    match aggregate {
        AggregatePolicy::SumIntegers => {
            aggregates.insert(
                sequence,
                AggregateState::SumIntegers {
                    remaining: keys.len(),
                    total: 0,
                },
            );
        }
        AggregatePolicy::BulkArrayInOrder => {
            aggregates.insert(
                sequence,
                AggregateState::BulkArrayInOrder {
                    remaining: keys.len(),
                    items: vec![None; keys.len()],
                },
            );
        }
    }

    for (idx, key) in keys.iter().enumerate() {
        let slot = redis_slot(key);
        let Some(node) = slot_map.node_for_slot(slot) else {
            responses[sequence] = Some(resp::err(&format!("cluster slot {slot} has no master")));
            return;
        };
        let raw = match aggregate {
            AggregatePolicy::SumIntegers => resp::array(&[args[0], key]),
            AggregatePolicy::BulkArrayInOrder => match json_mget_path {
                Some(path) => resp::array(&[b"JSON.GET".as_slice(), key, path]),
                None => resp::array(&[b"GET".as_slice(), key]),
            },
        };
        let target = match aggregate {
            AggregatePolicy::SumIntegers => ResponseTarget::Aggregate,
            AggregatePolicy::BulkArrayInOrder => ResponseTarget::AggregatePart(idx),
        };
        push_backend(batches, node, sequence, BackendRequest::Owned(raw), target);
    }

    if aggregate == AggregatePolicy::SumIntegers
        && name != "DEL"
        && name != "EXPDEL"
        && name != "UNLINK"
        && name != "EXISTS"
        && name != "TOUCH"
    {
        responses[sequence] = Some(resp::err(&format!(
            "{name} split-key aggregation is not supported"
        )));
    }
}

/// 业务作用: 把单个后端响应合并进命令聚合器，同时保留错误与空值语义。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `aggregates`: 待汇总的跨节点执行结果集合，按原命令语义合并为单个响应。
/// - `sequence`: 命令在前端 pipeline 中的原始序号，用于并发执行后恢复响应顺序。
/// - `target`: 当前操作的目标节点、配置项或输出位置，函数只修改该明确范围。
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
fn apply_backend_frame(
    responses: &mut [Option<Vec<u8>>],
    aggregates: &mut HashMap<usize, AggregateState>,
    sequence: usize,
    target: ResponseTarget,
    frame: Vec<u8>,
) {
    if responses[sequence].is_some() {
        return;
    }
    if frame.starts_with(b"-") {
        if matches!(target, ResponseTarget::AggregatePart(_)) && is_wrongtype_error(&frame) {
            aggregate_frame(
                responses,
                aggregates,
                sequence,
                match target {
                    ResponseTarget::AggregatePart(part_index) => Some(part_index),
                    _ => None,
                },
                resp::null_bulk().to_vec(),
            );
            return;
        }
        responses[sequence] = Some(frame);
        return;
    }
    match target {
        ResponseTarget::Direct => responses[sequence] = Some(frame),
        ResponseTarget::Aggregate => aggregate_frame(responses, aggregates, sequence, None, frame),
        ResponseTarget::AggregatePart(part_index) => {
            aggregate_frame(responses, aggregates, sequence, Some(part_index), frame)
        }
    }
}

/// 业务作用: 识别需要按命令聚合规则特殊处理的 WRONGTYPE 后端错误。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
fn is_wrongtype_error(frame: &[u8]) -> bool {
    frame
        .get(..10)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"-WRONGTYPE"))
}

/// 业务作用: 按聚合策略记录后端错误，决定立即失败还是保留部分结果。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `aggregates`: 待汇总的跨节点执行结果集合，按原命令语义合并为单个响应。
/// - `sequence`: 命令在前端 pipeline 中的原始序号，用于并发执行后恢复响应顺序。
/// - `message`: 返回给调用方或写入状态的业务消息，不应包含认证凭据。
///
fn apply_backend_error(
    responses: &mut [Option<Vec<u8>>],
    aggregates: &mut HashMap<usize, AggregateState>,
    sequence: usize,
    message: &str,
) {
    if responses[sequence].is_none() {
        responses[sequence] = Some(resp::err(message));
    }
    aggregates.remove(&sequence);
}

/// 业务作用: 完成跨节点结果聚合并编码为一条前端 RESP 响应。
/// 参数说明:
/// - `responses`: 按原始命令位置保存的响应集合，用于最终保序写回。
/// - `aggregates`: 待汇总的跨节点执行结果集合，按原命令语义合并为单个响应。
/// - `sequence`: 命令在前端 pipeline 中的原始序号，用于并发执行后恢复响应顺序。
/// - `part_index`: 聚合响应分片下标，用于把并发返回值放回原始位置。
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
fn aggregate_frame(
    responses: &mut [Option<Vec<u8>>],
    aggregates: &mut HashMap<usize, AggregateState>,
    sequence: usize,
    part_index: Option<usize>,
    frame: Vec<u8>,
) {
    let Some(state) = aggregates.get_mut(&sequence) else {
        responses[sequence] = Some(resp::err("missing aggregate state"));
        return;
    };

    match state {
        AggregateState::SumIntegers { remaining, total } => {
            match parse_integer_response(&frame) {
                Ok(value) => *total += value,
                Err(err) => {
                    responses[sequence] = Some(resp::err(&format!("aggregate integer: {err}")));
                    return;
                }
            }
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                responses[sequence] = Some(resp::integer(*total));
            }
        }
        AggregateState::BulkArray {
            remaining,
            items,
            max_items,
        } => {
            match parse_bulk_array_response(&frame) {
                Ok(mut values) => {
                    if items.len() + values.len() > *max_items {
                        responses[sequence] = Some(resp::err(&format!(
                            "aggregate array item count exceeds limit: {} > {}",
                            items.len() + values.len(),
                            *max_items
                        )));
                        return;
                    }
                    items.append(&mut values);
                }
                Err(err) => {
                    responses[sequence] = Some(resp::err(&format!("aggregate array: {err}")));
                    return;
                }
            }
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                responses[sequence] = Some(resp::bulk_array(items));
            }
        }
        AggregateState::BulkConsensus { remaining, value } => {
            match parse_bulk_or_simple_response(&frame) {
                Ok(next) => match value {
                    Some(existing) if existing != &next => {
                        responses[sequence] =
                            Some(resp::err("aggregate bulk values are inconsistent"));
                        return;
                    }
                    Some(_) => {}
                    None => *value = Some(next),
                },
                Err(err) => {
                    responses[sequence] = Some(resp::err(&format!("aggregate bulk: {err}")));
                    return;
                }
            }
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                responses[sequence] =
                    Some(resp::bulk(value.as_deref().expect(
                        "bulk consensus has value after at least one response",
                    )));
            }
        }
        AggregateState::BulkArrayInOrder { remaining, items } => {
            let Some(part_index) = part_index else {
                responses[sequence] = Some(resp::err("missing aggregate part index"));
                return;
            };
            if part_index >= items.len() {
                responses[sequence] = Some(resp::err("aggregate part index out of bounds"));
                return;
            }
            match parse_bulk_nullable_response(&frame) {
                Ok(value) => items[part_index] = value,
                Err(err) => {
                    responses[sequence] = Some(resp::err(&format!("aggregate bulk: {err}")));
                    return;
                }
            }
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                responses[sequence] = Some(resp::bulk_array_nullable(items));
            }
        }
    }
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 值符合 RESP integer 语义时返回数值；类型或编码非法时返回错误。
// 解析 parse_integer_response 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_integer_response(frame: &[u8]) -> anyhow::Result<i64> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty response")?;
    if consumed != frame.len() {
        bail!("response has trailing bytes");
    }
    match value {
        RespValue::Integer(value) => Ok(value),
        _ => bail!("expected integer response"),
    }
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
// 解析 parse_bulk_array_response 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_bulk_array_response(frame: &[u8]) -> anyhow::Result<Vec<Vec<u8>>> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty response")?;
    if consumed != frame.len() {
        bail!("response has trailing bytes");
    }
    let RespValue::Array(Some(items)) = value else {
        bail!("expected array response");
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        match item {
            RespValue::Bulk(Some(bytes)) | RespValue::Simple(bytes) => out.push(bytes),
            RespValue::Bulk(None) => {}
            _ => bail!("expected bulk array item"),
        }
    }
    Ok(out)
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 返回一条完整且已按代理语义处理的 RESP 帧；路由、I/O 或协议失败时返回错误。
// 解析 parse_bulk_or_simple_response 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_bulk_or_simple_response(frame: &[u8]) -> anyhow::Result<Vec<u8>> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty response")?;
    if consumed != frame.len() {
        bail!("response has trailing bytes");
    }
    match value {
        RespValue::Bulk(Some(bytes)) | RespValue::Simple(bytes) => Ok(bytes),
        _ => bail!("expected bulk string response"),
    }
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 返回 bulk 内容或 Redis null；其它响应类型返回协议错误。
// 解析 parse_bulk_nullable_response 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_bulk_nullable_response(frame: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty response")?;
    if consumed != frame.len() {
        bail!("response has trailing bytes");
    }
    match value {
        RespValue::Bulk(value) => Ok(value),
        RespValue::Simple(bytes) => Ok(Some(bytes)),
        _ => bail!("expected bulk string response"),
    }
}

/// 业务作用: 解析 Redis Cluster slot 路由、聚合命令和拓扑改写 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `frame`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
// 解析 parse_scan_backend_response 输入，用于支撑执行 Redis Cluster slot 路由、聚合和重定向处理。
fn parse_scan_backend_response(frame: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty SCAN response")?;
    if consumed != frame.len() {
        bail!("SCAN response has trailing bytes");
    }
    let RespValue::Array(Some(items)) = value else {
        bail!("SCAN response must be an array");
    };
    if items.len() != 2 {
        bail!("SCAN response must contain cursor and keys");
    }

    let cursor = match &items[0] {
        RespValue::Bulk(Some(value)) | RespValue::Simple(value) => value.clone(),
        _ => bail!("SCAN cursor must be bulk string"),
    };
    let RespValue::Array(Some(values)) = &items[1] else {
        bail!("SCAN keys must be array");
    };
    let mut keys = Vec::with_capacity(values.len());
    for value in values {
        match value {
            RespValue::Bulk(Some(key)) | RespValue::Simple(key) => keys.push(key.clone()),
            RespValue::Bulk(None) => {}
            _ => bail!("SCAN key item must be bulk string"),
        }
    }

    Ok((cursor, keys))
}
