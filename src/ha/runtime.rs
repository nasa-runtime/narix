use crate::infrastructure::time::{
    display_millis as now_ms, elapsed_micros, now_millis as now_unix_millis, unix_millis,
};
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::infrastructure::identity::instance_nonce;
use crate::infrastructure::lifecycle::{DomainTask, TaskScope, TaskSpawner};
use anyhow::{bail, Context};
use futures_util::future::join_all;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::process::Command;
use tokio::sync::{watch, Mutex as AsyncMutex, RwLock};
use tracing::{info, warn};

use crate::cluster::backend::{BackendConnectOptions, BackendConnection};
use crate::config::{
    HaCoordinatorMode, HaFencingMode, HaPersistencePolicy, ProxyItem, ReplicationHaConfig,
};
use crate::coordinator::runtime::CoordinatorHandle;
use crate::coordinator::state::CoordinationActionKind;
use crate::ha::backend::{HaActionAuthority, ReplicationHaBackend};
use crate::ha::container::HaRuntime;
use crate::ha::control_client::HaGenerationGuard;
use crate::ha::driver::HaBackendDriver;
use crate::ha::model::{
    parse_nodes, replica_failover_lag_seconds, replica_lag_is_eligible, HaNode, HaRole,
};
use crate::ha::persistence::{CheckpointReason, DynamicPersistenceBackend, PersistenceTarget};
use crate::ha::topology::{select_persistence_layout, PersistenceLayout};
use crate::resp::{self, RespValue};

pub use crate::ha::routing::HaRoutingState;

const REDIS_RENEW_LEASE_SCRIPT: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end";
const REDIS_RELEASE_LEASE_SCRIPT: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) else return 0 end";
const REDIS_CHECK_LEASE_SCRIPT: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('PTTL', KEYS[1]) else return -3 end";
const REDIS_WRITE_OUTCOME_SCRIPT: &str =
    "if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3]); return 1 else return 0 end";
const RECONCILE_WARNING_INTERVAL_MS: u64 = 5_000;

/// 业务作用: 维护单个 replication-ha listener 的控制面状态，负责发现 master、发布安全路由、执行切主和编排持久化 owner。
/// 字段说明: 节点、认证和超时字段来自配置；`current_master` 与 `routing_tx` 是数据面路由来源；租约、fencing 和 persistence 字段共同保证多 proxy 场景下只发生一次安全切换。
#[derive(Debug)]
// 表示 ReplicationHaRuntime 的数据结构，用于维护 replication HA 主从拓扑、探测状态和切主控制。
pub struct ReplicationHaRuntime {
    name: String,
    ha: HaRuntime<HaBackendDriver>,
    probe_interval_ms: u64,
    master_down_after_failures: u64,
    max_replica_lag_seconds: u64,
    auto_failover: bool,
    /// 观察者模式：只探测与跟随，任何后端控制命令和手动拓扑接口都被拒绝。
    observer: bool,
    /// 证据不足时连续观测到的唯一接管主节点，支撑旧主确认宕机后的受限跟随通道。
    observed_takeover: RwLock<ObservedTakeover>,
    /// 已明确观测为额外可写主、但尚未重新确认成副本的节点；跨探测轮保留，防止不对称分区把双主事实隐藏后提前开放路由。
    unconfirmed_extra_masters: RwLock<HashSet<String>>,
    fencing: RuntimeFencingConfig,
    coordinator: RuntimeCoordinatorConfig,
    cluster_coordinator: Option<RuntimeClusterCoordinatorConfig>,
    /// 当前 HA 关键配置的稳定摘要，仅 `proxy-cluster` 协调模式需要。
    group_config_digest: Option<String>,
    /// 构造本运行时所依据的全局集群配置摘要，仅 `proxy-cluster` 协调模式需要。
    cluster_config_digest: Option<String>,
    /// 本运行时已经通过组注册绑定的全局配置版本，仅 `proxy-cluster` 协调模式需要。
    cluster_config_revision: AtomicU64,
    persistence: RuntimePersistenceConfig,
    failover_lock: AsyncMutex<()>,
    cleanup_tasks: TaskScope,
    current_master: RwLock<HaNode>,
    routing_tx: watch::Sender<HaRoutingState>,
    last_error: RwLock<Option<String>>,
    replica_seen_since_ms: RwLock<HashMap<String, u128>>,
    /// 各副本最近一次成功探测的 Unix 毫秒时间，用于过滤快照阻塞造成的瞬时不可见。
    replica_last_seen_ms: RwLock<HashMap<String, u128>>,
    persistence_topology: RwLock<PersistenceTopologyState>,
    persistence_last_error: RwLock<Option<String>>,
    persistence_protected_reason: RwLock<Option<String>>,
    /// 持久化收敛失败后下一次允许执行后端控制命令的 Unix 毫秒时间。
    persistence_retry_not_before_ms: AtomicU64,
    master_failure_streak: AtomicU64,
    /// 下一次允许输出收敛失败 WARN 的 Unix 毫秒时间，避免失败计数变化绕过去重。
    reconcile_warning_not_before_ms: AtomicU64,
    /// fencing 失败后下一次允许执行自动隔离命令的 Unix 毫秒时间，防止每轮探测重复调用基础设施。
    automatic_retry_not_before_ms: AtomicU64,
    /// 副本上游收敛失败后下一次允许执行拓扑收敛命令的 Unix 毫秒时间，防止坏副本按探测周期反复触发控制命令。
    repair_retry_not_before_ms: AtomicU64,
    /// 运行代有效性令牌；退休后运行时步骤和适配器内部的每条控制命令都会拒绝继续执行。
    generation_guard: HaGenerationGuard,
    /// 成功完成副本上游收敛的累计次数。
    repair_count: AtomicU64,
    /// 副本上游收敛失败（含权威获取失败）的累计次数。
    repair_failure_count: AtomicU64,
    /// 最近一次成功副本上游收敛的耗时，单位为微秒。
    last_repair_elapsed_micros: AtomicU64,
    /// 各节点下一次允许输出探测失败日志的 Unix 毫秒时间，避免故障期日志随探测频率放大。
    probe_failure_log_not_before_ms: RwLock<HashMap<String, u64>>,
    failover_count: AtomicU64,
    last_failover_elapsed_micros: AtomicU64,
    fence_count: AtomicU64,
    protection_count: AtomicU64,
    fence_failure_count: AtomicU64,
    last_fence_elapsed_micros: AtomicU64,
    lease_acquire_count: AtomicU64,
    lease_acquire_failure_count: AtomicU64,
    last_lease_acquire_elapsed_micros: AtomicU64,
    persistence_handoff_count: AtomicU64,
    persistence_adopt_count: AtomicU64,
    persistence_enable_count: AtomicU64,
    persistence_enable_failure_count: AtomicU64,
    persistence_disable_count: AtomicU64,
    persistence_disable_failure_count: AtomicU64,
    persistence_snapshot_count: AtomicU64,
    persistence_snapshot_failure_count: AtomicU64,
    persistence_verify_count: AtomicU64,
    persistence_verify_failure_count: AtomicU64,
    persistence_protection_count: AtomicU64,
    persistence_last_handoff_ms: AtomicU64,
    persistence_last_snapshot_ms: AtomicU64,
    persistence_last_verify_duration_micros: AtomicU64,
}

/// 业务作用: 记录证据不足时连续观测到的唯一接管主节点，为"旧主确认宕机 + 接管者
/// 连续稳定"的受限跟随通道积累轮数；换人、拓扑变化或完成切换即归零。
/// 字段说明: `client_addr` 为候选接管者业务地址；`stable_rounds` 为连续稳定观测轮数。
#[derive(Debug, Default)]
struct ObservedTakeover {
    client_addr: String,
    stable_rounds: u64,
}

/// 业务作用: 表示可参与 failover 或 persistence 拓扑选择的副本候选，附带复制延迟和跟随来源。
/// 字段说明: `lag_seconds` 决定是否允许承接持久化；切主候选门禁额外结合 `link_down_seconds`，
/// 主节点宕机导致的断链副本按断链时长参选；`master_addr` 用于确认副本确实跟随当前 master。
#[derive(Debug, Clone)]
// 表示 ReplicaCandidate 的数据结构，用于维护 replication HA 主从拓扑、探测状态和切主控制。
struct ReplicaCandidate {
    node: HaNode,
    lag_seconds: Option<u64>,
    link_down_seconds: Option<u64>,
    master_addr: Option<String>,
}

/// 业务作用: 保存切主前的隔离策略，自动 failover 只有在 fencing 成功后才允许提主。
/// 字段说明: `mode` 决定手动、外部命令或禁用策略；`command` 是外部隔离脚本；`timeout_ms` 防止隔离命令卡住控制面。
#[derive(Debug, Clone)]
// 表示 RuntimeFencingConfig 的数据结构，用于执行基础设施 fencing 门禁。
struct RuntimeFencingConfig {
    mode: HaFencingMode,
    command: Vec<String>,
    timeout_ms: u64,
}

/// 业务作用: 保存多 proxy failover 的互斥租约配置，避免多个代理同时 fencing 或同时提主。
/// 字段说明: `owner_id` 标识当前进程；`lease_path` 和 `redis` 分别对应本地文件和跨机器租约；TTL、renew 和 acquire 超时共同限制租约窗口。
#[derive(Debug, Clone)]
// 表示 RuntimeCoordinatorConfig 的数据结构，用于在多 proxy 部署下串行化自动 failover。
struct RuntimeCoordinatorConfig {
    mode: HaCoordinatorMode,
    owner_id: String,
    lease_path: Option<PathBuf>,
    redis: Option<RedisCoordinatorConfig>,
    lease_ttl_ms: u64,
    lease_renew_interval_ms: u64,
    lease_acquire_timeout_ms: u64,
}

/// 业务作用: 保存当前 replication HA 代理项接入进程级协调器所需的组标识、观察门槛和共享句柄。
/// 字段说明: 进程中的所有代理项共享同一 `handle`，但通过 `group_id` 隔离状态机；观察窗口和门槛控制故障事实批准条件。
#[derive(Debug, Clone)]
struct RuntimeClusterCoordinatorConfig {
    handle: Arc<CoordinatorHandle>,
    group_id: String,
    observation_quorum: usize,
    observation_window: Duration,
}

impl RuntimeCoordinatorConfig {
    /// 业务作用: 根据本地租约文件派生 failover outcome 文件路径，让等待租约的 proxy 能观察到已完成的新 master。
    ///
    /// 返回: local-file coordinator 返回 outcome 文件路径；其它 coordinator 返回 `None`。
    fn local_outcome_path(&self) -> Option<PathBuf> {
        self.lease_path
            .as_ref()
            .map(|path| PathBuf::from(format!("{}.outcome", path.to_string_lossy())))
    }
}

/// 业务作用: 保存 Redis coordinator 的连接和 key 配置，用 Redis 原子脚本实现跨机器租约。
/// 字段说明: `key` 是租约 key，`outcome_key` 记录最近切主结果；连接超时独立配置，避免 coordinator 慢查询拖垮主控制面。
#[derive(Debug, Clone)]
// 表示 Redis 外部 coordinator 配置，用于跨机器多 proxy failover 租约互斥。
struct RedisCoordinatorConfig {
    addr: String,
    username: Option<String>,
    password: Option<crate::infrastructure::credentials::SecretText>,
    key: String,
    outcome_key: String,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
}

/// 业务作用: 保存动态快照责任策略，控制单节点 master 或多节点 replica 的快照归属，以及失败时是否进入保护态。
/// 字段说明: `policy` 决定是否启用自适应拓扑；强制持久化点、稳定时间、零延迟和 verify 控制 handoff 安全性。
#[derive(Debug, Clone)]
// 表示 replication HA 动态快照责任编排配置。
struct RuntimePersistenceConfig {
    policy: HaPersistencePolicy,
    force_snapshot_on_handoff: bool,
    min_replica_stable_ms: u64,
    require_zero_lag: bool,
    verify_timeout_ms: u64,
    fail_closed: bool,
}

/// 业务作用: 记录当前已启用 snapshot 的节点及其验证状态，避免未确认 owner 被当成可靠备份来源。
/// 字段说明: `verified` 表示该 owner 已通过配置和探活验证；`role` 用于管理面展示和异常诊断。
#[derive(Debug, Clone)]
// 记录本 proxy 已验证的持久化 owner，用于安全执行 owner handoff。
struct PersistenceOwnerState {
    node: HaNode,
    verified: bool,
    role: HaRole,
}

/// 业务作用: 表示持久化自适应拓扑形态，根据配置规模和可用副本数量决定主节点、热备与快照责任。
/// 分支说明: 真实单节点部署由主节点快照；1 个合格副本只做快照 owner；2 个及以上时第一个稳定低延迟副本做热备，其余副本做快照。
#[derive(Debug, Clone, PartialEq, Eq)]
// 描述 persistence 自适应拓扑形态：单节点主快照、1 replica 快照、2+ replica 热备 + 多快照。
enum PersistenceTopologyKind {
    None,
    SingleMasterSnapshot,
    SingleReplicaSnapshot,
    HotStandbyReplicaSnapshots,
}

impl PersistenceTopologyKind {
    /// 业务作用: 将拓扑枚举转换成管理面稳定字符串，避免状态接口暴露 Rust 枚举格式。
    ///
    /// 返回: 可用于 JSON 状态和指标标签的拓扑名称。
    fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::SingleMasterSnapshot => "single-master-snapshot",
            Self::SingleReplicaSnapshot => "single-replica-snapshot",
            Self::HotStandbyReplicaSnapshots => "hot-standby-replica-snapshots",
        }
    }
}

/// 业务作用: 保存最近一次已验证的自适应持久化拓扑，供状态接口和后续 handoff 判断当前 owner。
/// 字段说明: `hot_standby` 不启用 snapshot，专注接管 master；`snapshot_owners` 从热备之外的副本中选择，降低 master 负担。
#[derive(Debug, Clone)]
// 该缓存只记录已经完成后端复验的拓扑，未完成的 handoff 不能提前覆盖它。
struct PersistenceTopologyState {
    kind: PersistenceTopologyKind,
    hot_standby: Option<HaNode>,
    snapshot_owners: Vec<PersistenceOwnerState>,
}

impl Default for PersistenceTopologyState {
    /// 业务作用: 创建初始为空的 HA 运行时可变状态和累计指标。
    /// 参数说明: 无。
    fn default() -> Self {
        Self {
            kind: PersistenceTopologyKind::None,
            hot_standby: None,
            snapshot_owners: Vec::new(),
        }
    }
}

/// 业务作用: 绑定期望快照 owner 节点与其在该拓扑中的角色。
#[derive(Debug, Clone)]
struct DesiredPersistenceOwner {
    node: HaNode,
    role: HaRole,
}

/// 业务作用: 描述期望副本节点必须直接跟随的上游节点。
#[derive(Debug, Clone)]
struct DesiredReplicaLink {
    node: HaNode,
    upstream: HaNode,
}

/// 业务作用: 保存期望快照 owner、热备和复制链路的完整动态持久化布局。
#[derive(Debug, Clone)]
struct DesiredPersistenceTopology {
    kind: PersistenceTopologyKind,
    hot_standby: Option<HaNode>,
    snapshot_owners: Vec<DesiredPersistenceOwner>,
    replica_links: Vec<DesiredReplicaLink>,
}

impl DesiredPersistenceTopology {
    /// 业务作用: 判断指定客户端地址是否属于期望快照 owner 集合。
    /// 参数说明:
    /// - `client_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    fn is_snapshot_owner(&self, client_addr: &str) -> bool {
        self.snapshot_owners
            .iter()
            .any(|owner| owner.node.client_addr == client_addr)
    }

    /// 业务作用: 把已经远端验证的期望持久化拓扑转换为可缓存状态。
    fn to_verified_state(&self) -> PersistenceTopologyState {
        PersistenceTopologyState {
            kind: self.kind.clone(),
            hot_standby: self.hot_standby.clone(),
            snapshot_owners: self
                .snapshot_owners
                .iter()
                .map(|owner| PersistenceOwnerState {
                    node: owner.node.clone(),
                    verified: true,
                    role: owner.role,
                })
                .collect(),
        }
    }
}

/// 业务作用: 区分动态持久化启用与停用动作，驱动产品适配器写入目标配置。
#[derive(Debug, Clone, Copy)]
enum PersistenceAction {
    Enable,
    Disable,
}

/// 业务作用: 保存本地文件或外部 Redis 租约实现及其执行配置。
#[derive(Debug, Clone)]
enum LeaseBackend {
    LocalFile { path: PathBuf },
    Redis { config: RedisCoordinatorConfig },
}

impl LeaseBackend {
    /// 业务作用: 返回租约后端用于状态和错误定位的非敏感标识。
    fn label(&self) -> String {
        match self {
            Self::LocalFile { path } => path.display().to_string(),
            Self::Redis { config } => format!("redis://{}/{}", config.addr, config.key),
        }
    }
}

/// 业务作用: 区分自动切主已取得租约、已观察到其它实例收敛和租约仍忙碌三种结果。
#[derive(Debug)]
// 描述自动 failover 抢租约结果；等待期间若已看到新 master，就直接收敛路由。
enum AutomaticLeaseOutcome {
    Acquired(Option<LeaseGuard>),
    AlreadyConverged,
    Busy { reason: String },
}

/// 业务作用: 持有进程级协调器批准的 replication HA 动作令牌，并在异常返回时尽力提交失败结果释放组内互斥。
/// 字段说明: `target_master` 绑定批准目标；`completed` 防止成功完成后 Drop 再提交取消命令。
#[derive(Debug)]
struct ClusterActionGuard {
    coordinator: RuntimeClusterCoordinatorConfig,
    token: String,
    target_master: String,
    completed: bool,
}

/// 把当前运行时持有的旧租约或集群动作令牌暴露给产品适配器，
/// 使多命令切换能在每条后端写入前重新确认权威。
struct RuntimeActionAuthority<'a> {
    runtime: &'a ReplicationHaRuntime,
    lease: Option<&'a LeaseGuard>,
    cluster_action: Option<&'a ClusterActionGuard>,
}

impl HaActionAuthority for RuntimeActionAuthority<'_> {
    /// 业务作用: 在 fencing、提主等每个外部副作用前复验本节点仍是领导者且令牌仍处于活动状态。
    fn ensure_active<'a>(
        &'a self,
        step: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.runtime
                .ensure_automatic_authority_or_protect(self.lease, self.cluster_action, step)
                .await
        })
    }
}

/// 动态持久化动作使用的权威门禁。
///
/// 与拓扑动作不同，持久化失权是否关闭业务路由由 `fail_closed` 决定；但无论该
/// 开关取值如何，失权检查都必须返回错误并停止后续后端写。
struct RuntimePersistenceAuthority<'a> {
    runtime: &'a ReplicationHaRuntime,
    lease: Option<&'a LeaseGuard>,
    cluster_action: Option<&'a ClusterActionGuard>,
}

impl HaActionAuthority for RuntimePersistenceAuthority<'_> {
    /// 业务作用: 在每个动态持久化副作用前复验租约或集群动作令牌，失权后立即阻止后续配置写入。
    fn ensure_active<'a>(
        &'a self,
        step: &'a str,
    ) -> futures_util::future::BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async move {
            self.runtime
                .ensure_persistence_authority(self.lease, self.cluster_action, step)
                .await
        })
    }
}

impl ClusterActionGuard {
    /// 业务作用: 在 fencing、提主等每个外部副作用前复验本节点仍是领导者且令牌仍处于活动状态。
    ///
    /// 返回: 协调权威和活动令牌均有效时返回成功。
    async fn ensure_active(&self) -> anyhow::Result<()> {
        self.coordinator
            .handle
            .ensure_action_active(&self.coordinator.group_id, &self.token)
            .await
    }

    /// 业务作用: 在新主节点复验完成后先提交动作结果和路由代际，再允许本地数据面发布新路由。
    /// 参数说明:
    /// - `resulting_master`: 动作完成后仍应生效的主节点地址；故障切换时也是批准目标，持久化交接时保持原主节点。
    ///
    /// 返回: 状态机接受相同目标和令牌时返回成功。
    async fn complete_success(&mut self, resulting_master: &str) -> anyhow::Result<()> {
        self.coordinator
            .handle
            .complete_action(
                &self.coordinator.group_id,
                self.token.clone(),
                true,
                resulting_master.to_string(),
            )
            .await?;
        self.completed = true;
        Ok(())
    }

    /// 业务作用: 外部操作已部分执行但不能安全发布新路由时显式取消活动令牌，避免仅依赖析构时的尽力清理延长组内阻塞。
    /// 参数说明:
    /// - `observed_master`: 清理时本代理仍认定的主节点，仅用于形成完整完成命令，失败结果不会改写路由。
    ///
    /// 返回: 状态机接受失败完成命令时返回成功。
    async fn complete_failure(&mut self, observed_master: &str) -> anyhow::Result<()> {
        self.coordinator
            .handle
            .complete_action(
                &self.coordinator.group_id,
                self.token.clone(),
                false,
                observed_master.to_string(),
            )
            .await?;
        self.completed = true;
        Ok(())
    }
}

impl Drop for ClusterActionGuard {
    /// 业务作用: 对尚未完成的动作异步提交失败结果，正常错误路径不应把 HA 组永久留在活动动作状态。
    /// 参数说明: 无。
    ///
    /// 返回: 补偿由协调器持有并等待；协调器已关闭时仅记录拒绝，不启动脱管任务。
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Err(error) = self.coordinator.handle.schedule_action_cleanup(
            self.coordinator.group_id.clone(),
            self.token.clone(),
            self.target_master.clone(),
        ) {
            warn!(
                group_id = %self.coordinator.group_id,
                error = %error,
                "协调器已关闭，未接纳 HA 动作失败报告"
            );
        }
    }
}

/// 业务作用: 汇总托管复制 HA 当前主节点、路由保护、协调权威、节点观测和持久化状态。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 ReplicationHaStatus 的数据结构，用于维护 replication HA 主从拓扑、探测状态和切主控制。
pub struct ReplicationHaStatus {
    /// 当前已确认并发布给数据面的主节点地址。
    pub current_master: String,
    /// 节点角色和复制状态探测周期，单位为毫秒。
    pub probe_interval_ms: u64,
    /// 判定主节点不可用所需的连续探测失败次数。
    pub master_down_after_failures: u64,
    /// 允许参与晋升的从节点最大复制延迟，单位为秒。
    pub max_replica_lag_seconds: u64,
    /// 是否允许满足隔离与协调门禁后自动切换主节点。
    pub auto_failover: bool,
    /// 是否处于观察者模式；为真时本代理绝不发送后端控制命令。
    pub observer: bool,
    /// 当前主节点已经连续探测失败的次数。
    pub master_failure_streak: u64,
    /// 运行期成功完成的主节点切换次数。
    pub failover_count: u64,
    /// 最近一次成功切换主节点的总耗时，单位为微秒。
    pub last_failover_elapsed_micros: u64,
    /// 当前配置的旧主节点隔离模式。
    pub fencing_mode: String,
    /// 成功完成的旧主节点隔离次数。
    pub fence_count: u64,
    /// 因拓扑、隔离、协调或持久化不安全进入保护态的累计次数。
    pub protection_count: u64,
    /// 旧主节点隔离失败或超时的累计次数。
    pub fence_failure_count: u64,
    /// 最近一次隔离动作的耗时，单位为微秒。
    pub last_fence_elapsed_micros: u64,
    /// 成功完成副本上游收敛的累计次数。
    pub replica_repair_count: u64,
    /// 副本上游收敛失败（含权威获取失败）的累计次数。
    pub replica_repair_failure_count: u64,
    /// 最近一次成功副本上游收敛的耗时，单位为微秒。
    pub last_replica_repair_elapsed_micros: u64,
    /// 当前使用的多代理协调模式。
    pub coordinator_mode: String,
    /// 文件或独立 Redis 租约模式下的当前实例身份。
    pub coordinator_owner_id: String,
    /// 成功取得协调租约的累计次数。
    pub lease_acquire_count: u64,
    /// 获取协调租约失败或超时的累计次数。
    pub lease_acquire_failure_count: u64,
    /// 最近一次获取协调租约的耗时，单位为微秒。
    pub last_lease_acquire_elapsed_micros: u64,
    /// 当前数据面路由状态的稳定文本名称。
    pub routing_state: String,
    /// 当前处于保护态时的原因；服务态为空。
    pub protection_reason: Option<String>,
    /// 最近一轮探测得到的全部节点角色与复制状态。
    pub nodes: Vec<ReplicationHaNodeStatus>,
    /// 动态快照责任编排的当前状态与累计指标；单节点拓扑允许 master 承担快照。
    pub persistence: ReplicationHaPersistenceStatus,
    /// 最近一次探测、切换或协调失败的错误摘要。
    pub last_error: Option<String>,
}

/// 业务作用: 暴露动态持久化策略、拓扑、owner、热备、错误和累计动作指标。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 replication HA 动态快照责任编排状态。
pub struct ReplicationHaPersistenceStatus {
    /// 当前启用的快照责任策略。
    pub policy: String,
    /// 快照启停、执行和验证动作所使用的执行模式。
    pub switch_mode: String,
    /// 按健康从节点数量计算出的当前持久化拓扑名称。
    pub topology: String,
    /// 当前关闭周期快照、优先用于接管的热备节点；降级拓扑下为空。
    pub hot_standby: Option<String>,
    /// 兼容单责任节点接口的首个快照责任节点地址。
    pub owner: Option<String>,
    /// 首个快照责任节点是否已经通过实际快照验证。
    pub owner_verified: bool,
    /// 首个快照责任节点在当前拓扑中的业务角色。
    pub owner_role: Option<String>,
    /// 当前全部快照责任节点及其验证状态。
    pub snapshot_owners: Vec<ReplicationHaPersistenceOwnerStatus>,
    /// 最近一次完成快照责任交接的 Unix 毫秒时间。
    pub last_handoff_ms: u64,
    /// 最近一次成功触发快照的 Unix 毫秒时间。
    pub last_snapshot_ms: u64,
    /// 最近一次持久化动作失败的错误摘要。
    pub last_error: Option<String>,
    /// 持久化无法安全收敛而触发保护时的原因。
    pub protected_reason: Option<String>,
    /// 成功完成快照责任交接的累计次数。
    pub handoff_count: u64,
    /// 启动或恢复时采纳已有合格快照责任的累计次数。
    pub adopt_count: u64,
    /// 成功启用节点快照责任的累计次数。
    pub enable_count: u64,
    /// 启用节点快照责任失败的累计次数。
    pub enable_failure_count: u64,
    /// 成功停用旧节点快照责任的累计次数。
    pub disable_count: u64,
    /// 停用旧节点快照责任失败的累计次数。
    pub disable_failure_count: u64,
    /// 成功触发即时快照的累计次数。
    pub snapshot_count: u64,
    /// 触发即时快照失败的累计次数。
    pub snapshot_failure_count: u64,
    /// 成功验证快照责任和快照产物的累计次数。
    pub verify_count: u64,
    /// 验证快照责任或产物失败的累计次数。
    pub verify_failure_count: u64,
    /// 持久化安全门禁触发保护态的累计次数。
    pub protection_count: u64,
    /// 最近一次快照验证动作的耗时，单位为微秒。
    pub last_verify_duration_micros: u64,
}

/// 业务作用: 描述一个快照 owner 的地址、角色和是否已经远端验证。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicationHaPersistenceOwnerStatus {
    /// 承担快照责任的 replication HA 节点客户端地址。
    pub node: String,
    /// 当前节点在持久化拓扑中的角色名称。
    pub role: String,
    /// 当前节点是否已经通过快照启用和产物验证。
    pub verified: bool,
}

/// 业务作用: 描述单个 HA 节点最近观测的角色、上游、延迟、链路状态和可提升资格。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 ReplicationHaNodeStatus 的数据结构，用于维护 replication HA 主从拓扑、探测状态和切主控制。
pub struct ReplicationHaNodeStatus {
    /// narix 连接该节点处理业务和管理命令的地址。
    pub client_addr: String,
    /// 其他 replication HA 节点执行复制时使用的地址。
    pub replication_addr: String,
    /// 最近探测确认的主、从或未知角色。
    pub role: String,
    /// 最近一次节点探测往返耗时，单位为微秒。
    pub probe_latency_micros: u64,
    /// 从节点报告的复制延迟，无法取得或非从节点时为空。
    pub replica_lag_seconds: Option<u64>,
}

include!("runtime/construction.rs");
include!("runtime/operations.rs");
include!("runtime/reconcile.rs");
include!("runtime/persistence.rs");
include!("runtime/routing.rs");
include!("runtime/coordination.rs");
include!("runtime/actions.rs");
include!("runtime/lease.rs");
include!("runtime/helpers.rs");
