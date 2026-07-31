use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use futures_util::future::join_all;
use openraft::{BasicNode, Config, SnapshotPolicy};
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{info, warn};

use super::network::CoordinatorNetworkFactory;
use super::pair::PairCoordinator;
use super::rpc::{self, CoordinatorRpcService};
use super::state::{
    ClusterConfigPayload, CoordinationActionKind, CoordinationCommand, CoordinationObservation,
    CoordinationResponse, CoordinationState,
};
use super::store::{CoordinatorGenesis, CoordinatorLogStore, CoordinatorStateMachineStore};
use super::wire::{ClusterConfigRequest, ClusterConfigResponse, RpcEnvelope};
use super::{cluster_configuration_digest, wire_cluster_id, CoordinatorRaft};
use crate::config::{ProxyClusterCoordinationConfig, ProxyClusterProtocol};
use crate::reload::{
    ApplyMode, ClusterConfigStore, LoadedConfig, PersistedClusterConfig, ReloadHandle,
};

/// 当前节点支持的集群热更新规范化载荷契约版本。
pub(crate) const CLUSTER_CONFIG_SCHEMA_VERSION: u32 = 2;

/// 保存本节点已经完成运行代发布和活动指针同步的配置版本。
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveClusterConfig {
    /// 与协调状态机严格相等的单调递增版本。
    revision: u64,
    /// 与协调状态机严格相等的规范化配置摘要。
    digest: String,
}

/// 绑定候选代际的数据面冻结租约，阻止延迟的旧事务消息影响新候选。
struct ClusterConfigServingFreeze {
    /// 当前冻结所属的候选配置版本。
    revision: u64,
    /// 当前冻结所属的候选配置摘要。
    digest: String,
    /// 领导节点不再回调时自动解除孤儿冻结的单调时钟期限。
    deadline: Instant,
}

/// 连接协调 gRPC 阶段与本节点配置 supervisor、受保护磁盘存储。
struct ClusterConfigRuntime {
    reload: RwLock<Option<ReloadHandle>>,
    store: ClusterConfigStore,
    active: RwLock<Option<ActiveClusterConfig>>,
    serving: watch::Sender<bool>,
    runtime_synchronized: AtomicBool,
    startup_synchronized: AtomicBool,
    serving_freeze: RwLock<Option<ClusterConfigServingFreeze>>,
    transition: Mutex<()>,
}

impl ClusterConfigRuntime {
    /// 业务作用: 使用本节点协调数据目录创建尚未安装 reload handle 的集群配置运行时。
    ///
    /// 参数说明:
    /// - `data_dir`: 当前节点独占的协调持久化目录。
    ///
    /// 返回: 配置子目录权限和同步成功后返回共享运行时。
    fn open(data_dir: &Path) -> anyhow::Result<Self> {
        let store = ClusterConfigStore::open(data_dir)?;
        let active = store.load_active()?.map(|persisted| ActiveClusterConfig {
            revision: persisted.revision,
            digest: persisted.digest,
        });
        let (serving, _) = watch::channel(false);
        Ok(Self {
            reload: RwLock::new(None),
            store,
            active: RwLock::new(active),
            serving,
            runtime_synchronized: AtomicBool::new(false),
            startup_synchronized: AtomicBool::new(false),
            serving_freeze: RwLock::new(None),
            transition: Mutex::new(()),
        })
    }

    /// 业务作用: 安装进程启动后创建的配置 supervisor 请求句柄。
    ///
    /// 参数说明:
    /// - `reload`: 本节点串行配置加载、预检和发布句柄。
    async fn install(&self, reload: ReloadHandle) {
        *self.reload.write().await = Some(reload);
    }

    /// 业务作用: 将候选配置安全落盘并执行不发布的本地预检。
    ///
    /// 参数说明:
    /// - `revision`: 领导节点分配的候选版本。
    /// - `digest`: 规范化配置载荷摘要。
    /// - `payload`: 通过协调 gRPC 收到的规范化配置正文。
    /// - `transaction_timeout`: 候选事务失去领导节点回调后自动解除数据面冻结的最长时间。
    ///
    /// 返回: 文件完成同步且运行时预检成功时返回成功。
    async fn prepare(
        &self,
        revision: u64,
        digest: &str,
        payload: &[u8],
        transaction_timeout: Duration,
    ) -> anyhow::Result<()> {
        let _transition = self.transition.lock().await;
        self.store.prepare(revision, digest, payload)?;
        let reload = self
            .reload
            .read()
            .await
            .clone()
            .context("configuration supervisor is not installed")?;
        if let Err(error) = reload
            .apply_cluster_payload(
                payload.to_vec(),
                ApplyMode::ValidateOnly,
                revision,
                digest.to_string(),
            )
            .await
        {
            let _ = self.store.abort(revision, digest);
            bail!("cluster configuration preflight failed: {error}");
        }
        self.freeze_serving(revision, digest, transaction_timeout)
            .await;
        Ok(())
    }

    /// 业务作用: 让当前领导节点在广播只读预检前完成唯一一次 HA 拓扑迁移。
    ///
    /// 参数说明:
    /// - `revision`: 领导节点为候选分配的配置版本。
    /// - `digest`: 规范化配置载荷摘要。
    /// - `payload`: 已验证摘要、将由全部成员预检的规范化候选配置。
    ///
    /// 返回: 不需要外部动作或安全拓扑迁移完成时返回成功。
    async fn precommit(&self, revision: u64, digest: &str, payload: &[u8]) -> anyhow::Result<()> {
        let reload = self
            .reload
            .read()
            .await
            .clone()
            .context("configuration supervisor is not installed")?;
        reload
            .apply_cluster_payload(
                payload.to_vec(),
                ApplyMode::HaPrecommit,
                revision,
                digest.to_string(),
            )
            .await
            .map(|_| ())
            .map_err(anyhow::Error::msg)
    }

    /// 业务作用: 提交磁盘候选、发布运行代并原子更新活动版本指针。
    ///
    /// 参数说明:
    /// - `revision`: 协调日志已经提交的配置版本。
    /// - `digest`: 协调日志已经提交的配置摘要。
    /// - `payload`: 与候选文件逐字一致的规范化配置正文。
    ///
    /// 返回: 本地运行代和活动指针都完成发布时返回成功。
    async fn activate(&self, revision: u64, digest: &str, payload: &[u8]) -> anyhow::Result<()> {
        let _transition = self.transition.lock().await;
        if self.is_active(revision, digest).await {
            self.clear_serving_freeze_through(revision).await;
            return Ok(());
        }
        self.activate_locked(revision, digest, payload).await
    }

    /// 业务作用: 在已经持有配置转换互斥锁时提交磁盘候选、发布运行代并更新活动指针。
    ///
    /// 参数说明:
    /// - `revision`: 协调日志已经提交的配置版本。
    /// - `digest`: 协调日志已经提交的配置摘要。
    /// - `payload`: 与协调状态机逐字一致的规范化配置正文。
    ///
    /// 返回: 本地运行代和活动指针都完成发布时返回成功。
    async fn activate_locked(
        &self,
        revision: u64,
        digest: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.runtime_synchronized.store(false, Ordering::Release);
        self.serving.send_replace(false);
        self.store.commit(revision, digest)?;
        let reload = self
            .reload
            .read()
            .await
            .clone()
            .context("configuration supervisor is not installed")?;
        reload
            .apply_cluster_payload(
                payload.to_vec(),
                ApplyMode::Publish,
                revision,
                digest.to_string(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
        self.store.activate(revision, digest)?;
        *self.active.write().await = Some(ActiveClusterConfig {
            revision,
            digest: digest.to_string(),
        });
        self.runtime_synchronized.store(true, Ordering::Release);
        self.clear_serving_freeze_through(revision).await;
        Ok(())
    }

    /// 业务作用: 从已经提交的磁盘版本恢复本节点运行代和活动指针。
    ///
    /// 参数说明:
    /// - `revision`: 当前协调状态机确认的版本。
    /// - `digest`: 当前协调状态机确认的配置摘要。
    /// - `payload`: 与协调状态机摘要逐字匹配的规范化配置正文。
    ///
    /// 返回: 指定提交文件存在、运行时代发布成功并更新活动指针时返回成功。
    async fn recover(&self, revision: u64, digest: &str, payload: &[u8]) -> anyhow::Result<()> {
        let _transition = self.transition.lock().await;
        if self.is_active(revision, digest).await {
            self.clear_serving_freeze_through(revision).await;
            return Ok(());
        }
        self.runtime_synchronized.store(false, Ordering::Release);
        self.serving.send_replace(false);
        self.store.prepare(revision, digest, payload)?;
        self.activate_locked(revision, digest, payload).await
    }

    /// 业务作用: 判断本节点运行代和活动指针是否已经对应指定集群配置。
    ///
    /// 参数说明:
    /// - `revision`: 要核对的协调提交版本。
    /// - `digest`: 要核对的规范化配置摘要。
    ///
    /// 返回: 运行代已完成同步且内存活动版本精确匹配时返回真。
    async fn is_active(&self, revision: u64, digest: &str) -> bool {
        self.runtime_synchronized.load(Ordering::Acquire)
            && self
                .active
                .read()
                .await
                .as_ref()
                .is_some_and(|active| active.revision == revision && active.digest == digest)
    }

    /// 业务作用: 返回用于关闭陈旧数据连接的集群配置服务门禁订阅。
    ///
    /// 返回: `true` 仅表示本地活动版本已经与协调状态机一致。
    fn serving_guard(&self) -> watch::Receiver<bool> {
        self.serving.subscribe()
    }

    /// 业务作用: 根据协调状态机版本刷新本地数据面服务门禁。
    ///
    /// 参数说明:
    /// - `committed_revision`: 协调状态机最近提交的全局配置版本。
    /// - `committed_digest`: 与提交版本绑定的规范化配置摘要。
    ///
    /// 返回: 本地活动版本与集群提交版本严格相等时返回真。
    async fn refresh_serving_guard(
        &self,
        committed_revision: u64,
        committed_digest: Option<&str>,
    ) -> bool {
        if self.serving_frozen().await {
            self.serving.send_replace(false);
            return false;
        }
        let active = self.active.read().await;
        let consistent = self.startup_synchronized.load(Ordering::Acquire)
            && self.runtime_synchronized.load(Ordering::Acquire)
            && committed_revision > 0
            && committed_digest.is_some_and(|digest| {
                active.as_ref().is_some_and(|active| {
                    active.revision == committed_revision && active.digest == digest
                })
            });
        self.serving.send_replace(consistent);
        consistent
    }

    /// 业务作用: 冻结数据面门禁，防止周期守护在配置事务提交前重新开放旧运行代。
    ///
    /// 参数说明:
    /// - `revision`: 本次冻结所属的候选配置版本。
    /// - `digest`: 本次冻结所属的候选配置摘要。
    /// - `transaction_timeout`: 领导节点失联且没有提交或中止回调时，冻结自动失效的最长时间。
    async fn freeze_serving(&self, revision: u64, digest: &str, transaction_timeout: Duration) {
        let deadline = Instant::now() + transaction_timeout;
        *self.serving_freeze.write().await = Some(ClusterConfigServingFreeze {
            revision,
            digest: digest.to_string(),
            deadline,
        });
        self.serving.send_replace(false);
    }

    /// 业务作用: 清除版本不晚于已经成功发布版本的配置事务冻结。
    ///
    /// 参数说明:
    /// - `revision`: 已经成功发布到本节点的配置版本。
    async fn clear_serving_freeze_through(&self, revision: u64) {
        let mut freeze = self.serving_freeze.write().await;
        if freeze
            .as_ref()
            .is_some_and(|candidate| candidate.revision <= revision)
        {
            *freeze = None;
        }
    }

    /// 业务作用: 只清除与明确中止命令的版本和摘要完全一致的配置事务冻结。
    ///
    /// 参数说明:
    /// - `revision`: 已明确中止的候选配置版本。
    /// - `digest`: 已明确中止的候选配置摘要。
    async fn clear_serving_freeze_for(&self, revision: u64, digest: &str) {
        let mut freeze = self.serving_freeze.write().await;
        if freeze
            .as_ref()
            .is_some_and(|candidate| candidate.revision == revision && candidate.digest == digest)
        {
            *freeze = None;
        }
    }

    /// 业务作用: 判断配置事务数据面冻结是否仍在有效期限内，并清理已经过期的孤儿冻结。
    ///
    /// 返回: 当前事务仍禁止周期守护重新开放数据面时返回真。
    async fn serving_frozen(&self) -> bool {
        // 必须先取得与 prepare/activate/abort 相同的转换锁，再读取冻结代际。若先取出
        // 过期冻结、等待转换锁期间同 revision 被领导节点重新 prepare，延迟清理会误删
        // 刚重新确认的候选文件，并留下没有磁盘候选的新冻结。
        let _transition = self.transition.lock().await;
        let expired = {
            let mut freeze = self.serving_freeze.write().await;
            match freeze.as_ref() {
                Some(candidate) if Instant::now() < candidate.deadline => return true,
                Some(_) => freeze.take(),
                None => None,
            }
        };
        if let Some(candidate) = expired {
            if let Err(error) = self.store.abort(candidate.revision, &candidate.digest) {
                warn!(
                    revision = candidate.revision,
                    error = %error,
                    "清理已过期的集群配置候选失败"
                );
            }
        }
        false
    }

    /// 业务作用: 标记本节点已经从当前领导者确认启动时的最新配置头。
    ///
    /// 只有启动同步流程可以调用该方法；设置后周期收敛任务才允许重新开放数据面。
    fn mark_startup_synchronized(&self) {
        self.startup_synchronized.store(true, Ordering::Release);
    }

    /// 业务作用: 删除尚未进入协调日志提交点的候选配置。
    ///
    /// 参数说明:
    /// - `revision`: 要放弃的候选配置版本。
    /// - `digest`: 要放弃的候选配置摘要。
    ///
    /// 返回: 候选不存在或已安全删除时返回成功。
    async fn abort(&self, revision: u64, digest: &str) -> anyhow::Result<()> {
        // 与 prepare/activate/recover 共用转换锁，避免延迟 Abort 在预检尚未返回时
        // 先删候选、随后又被该预检重新冻结成没有磁盘候选的孤儿事务。
        let _transition = self.transition.lock().await;
        let result = self.store.abort(revision, digest);
        if result.is_ok() {
            self.clear_serving_freeze_for(revision, digest).await;
        }
        result
    }
}

/// 业务作用: 保存进程级协调器已经启动的业务句柄和后台任务，主进程把任务纳入统一故障与关闭管理。
/// 字段说明: `handle` 供所有代理监听器共享；任一后台任务异常退出都应触发整个进程进入非就绪状态。
pub struct StartedCoordinator {
    /// 供同一进程所有托管复制 HA 代理项共享的协议无关句柄。
    pub handle: Arc<CoordinatorHandle>,
    /// 必须由主进程监管并在异常退出时触发整体关闭的后台任务。
    pub tasks: Vec<JoinHandle<anyhow::Result<()>>>,
}

/// 业务作用: 区分静态创世信息选择的双节点与多数派运行时，分支在进程生命周期内不可改变。
/// 分支说明: 两个分支共享同一业务状态机，但领导权和复制门槛分别由各自协议维护。
enum CoordinatorProtocolRuntime {
    Pair(Arc<PairCoordinator>),
    Quorum(CoordinatorRaft),
}

/// 业务作用: 向托管复制 HA 控制面提供协议无关的观察上报、动作批准、动作完成和路由权威检查接口。
/// 字段说明: 所有克隆句柄都指向同一协议运行时和持久化状态，不允许每个代理项自行选举。
pub struct CoordinatorHandle {
    cluster_id: Arc<str>,
    local_node_id: u64,
    bootstrap_node_id: u64,
    member_count: usize,
    member_ids: Arc<Vec<u64>>,
    protocol: CoordinatorProtocolRuntime,
    network: CoordinatorNetworkFactory,
    state_store: CoordinatorStateMachineStore,
    route_guard_timeout: Duration,
    action_timeout: Duration,
    last_raft_authority_contact: Arc<Mutex<Instant>>,
    cluster_config: Arc<ClusterConfigRuntime>,
    configuration_transaction: Mutex<()>,
}

impl std::fmt::Debug for CoordinatorHandle {
    /// 业务作用: 输出协调器句柄的非敏感摘要，不展开共识日志、证书材料或内部通道。
    /// 参数说明:
    /// - `formatter`: 标准调试格式化器。
    ///
    /// 返回: 返回格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinatorHandle")
            .field("cluster_id", &self.cluster_id)
            .field("local_node_id", &self.local_node_id)
            .field("bootstrap_node_id", &self.bootstrap_node_id)
            .field("member_count", &self.member_count)
            .field(
                "protocol",
                &match &self.protocol {
                    CoordinatorProtocolRuntime::Pair(_) => "pair",
                    CoordinatorProtocolRuntime::Quorum(_) => "quorum",
                },
            )
            .finish()
    }
}

impl StartedCoordinator {
    /// 业务作用: 构造、持久化并启动进程级协调器，包括 gRPC 服务、协议循环和多数派权威探测。
    /// 参数说明:
    /// - `config`: 已通过全局启动校验的协调配置。
    /// - `shutdown`: 主进程关闭信号。
    ///
    /// 返回: 返回共享句柄和必须由主进程监管的后台任务。
    pub async fn start(
        config: ProxyClusterCoordinationConfig,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<Self> {
        // 主进程通常已经校验完整 ProxyConfig；这里仍重复校验协调子配置，使库调用方和未来
        // 其它启动入口无法绕过静态成员、计时互斥或 TLS 字段约束。
        config.validate()?;
        CoordinatorNetworkFactory::validate_local_tls_identity(&config)?;
        let protocol = config.protocol()?;
        let members: BTreeMap<u64, String> = config
            .peers
            .iter()
            .map(|peer| (peer.node_id, peer.address.clone()))
            .collect();
        let genesis = CoordinatorGenesis {
            cluster_id: config.cluster_id.clone(),
            protocol: match protocol {
                ProxyClusterProtocol::Pair => "pair",
                ProxyClusterProtocol::Quorum => "quorum",
            }
            .to_string(),
            members: members.clone(),
            configuration_digest: cluster_configuration_digest(&config),
        };
        let (log_store, state_store) =
            CoordinatorLogStore::open(Path::new(&config.data_dir), genesis)?;
        let network = CoordinatorNetworkFactory::from_config(&config)?;
        let last_raft_authority_contact = Arc::new(Mutex::new(Instant::now()));

        let (protocol_runtime, raft, pair) = match protocol {
            ProxyClusterProtocol::Pair => {
                let pair = PairCoordinator::new(&config, state_store.clone(), network.clone())?;
                (
                    CoordinatorProtocolRuntime::Pair(pair.clone()),
                    None,
                    Some(pair),
                )
            }
            ProxyClusterProtocol::Quorum => {
                let raft_config = Config {
                    cluster_name: config.cluster_id.clone(),
                    heartbeat_interval: config.heartbeat_interval_ms,
                    election_timeout_min: config.election_timeout_min_ms,
                    election_timeout_max: config.election_timeout_max_ms,
                    install_snapshot_timeout: config.rpc_timeout_ms,
                    snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_log_entries),
                    replication_lag_threshold: config.snapshot_log_entries.saturating_mul(2),
                    max_in_snapshot_log_to_keep: config.snapshot_log_entries.min(1_000),
                    ..Config::default()
                };
                let raft_config = Arc::new(
                    raft_config
                        .validate()
                        .context("validate quorum consensus configuration")?,
                );
                let raft = CoordinatorRaft::new(
                    config.node_id,
                    raft_config,
                    network.clone(),
                    log_store,
                    state_store.clone(),
                )
                .await
                .context("create quorum coordinator runtime")?;
                (
                    CoordinatorProtocolRuntime::Quorum(raft.clone()),
                    Some(raft),
                    None,
                )
            }
        };

        let handle = Arc::new(CoordinatorHandle {
            cluster_id: Arc::from(wire_cluster_id(&config)),
            local_node_id: config.node_id,
            bootstrap_node_id: config.bootstrap_node_id,
            member_count: config.peers.len(),
            member_ids: Arc::new(config.peers.iter().map(|peer| peer.node_id).collect()),
            protocol: protocol_runtime,
            network,
            state_store,
            route_guard_timeout: Duration::from_millis(config.route_guard_timeout_ms),
            action_timeout: Duration::from_millis(config.action_timeout_ms),
            last_raft_authority_contact: last_raft_authority_contact.clone(),
            cluster_config: Arc::new(ClusterConfigRuntime::open(Path::new(&config.data_dir))?),
            configuration_transaction: Mutex::new(()),
        });
        let service = CoordinatorRpcService::new(
            &config,
            raft.clone(),
            pair.clone(),
            last_raft_authority_contact,
            handle.clone(),
        );
        let mut tasks = Vec::new();
        let server_config = config.clone();
        let server_shutdown = shutdown.clone();
        tasks.push(tokio::spawn(async move {
            rpc::serve(server_config, service, server_shutdown).await
        }));
        let guard_handle = handle.clone();
        let guard_shutdown = shutdown.clone();
        tasks.push(tokio::spawn(async move {
            guard_handle
                .run_cluster_configuration_guard(guard_shutdown)
                .await;
            Ok(())
        }));
        if let Some(pair) = pair {
            let pair_shutdown = shutdown.clone();
            tasks.push(tokio::spawn(async move {
                pair.run(pair_shutdown).await;
                Ok(())
            }));
        }
        if let Some(raft) = raft {
            if config.node_id == config.bootstrap_node_id {
                let bootstrap_members = members
                    .iter()
                    .map(|(node_id, address)| {
                        (
                            *node_id,
                            BasicNode {
                                addr: address.clone(),
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                // 初始化是启动门禁而不是长期服务。若把它作为普通后台任务加入主进程任务列表，
                // 正常完成会被误判为监听器退出并触发整进程关闭，因此必须在返回前明确完成。
                if !raft
                    .is_initialized()
                    .await
                    .context("inspect quorum initialization state")?
                {
                    raft.initialize(bootstrap_members)
                        .await
                        .context("initialize static quorum membership")?;
                }
            }
            let authority_handle = handle.clone();
            let authority_shutdown = shutdown;
            tasks.push(tokio::spawn(async move {
                authority_handle
                    .run_quorum_authority_probe(authority_shutdown)
                    .await;
                Ok(())
            }));
        }
        info!(
            cluster_id = %config.cluster_id,
            node_id = config.node_id,
            protocol = ?protocol,
            members = config.peers.len(),
            "进程级协调器已启动"
        );
        Ok(Self { handle, tasks })
    }
}

impl CoordinatorHandle {
    /// 业务作用: 安装本节点配置 supervisor，使协调 gRPC 可以执行 prepare 和 activate 阶段。
    ///
    /// 参数说明:
    /// - `reload`: 本进程唯一的串行配置热更新句柄。
    pub async fn install_reload_handle(&self, reload: ReloadHandle) {
        self.cluster_config.install(reload).await;
    }

    /// 业务作用: 在开放数据面前建立或恢复当前领导者线性确认的集群配置版本。
    ///
    /// 参数说明:
    /// - `initial`: 本节点已经静态校验并用于首代运行时的启动配置。
    /// - `timeout`: 等待领导形成、多数派提交、本地状态追平和运行代恢复的最长时间。
    ///
    /// 返回: 本节点活动磁盘版本和运行代与协调状态机一致时返回成功。
    pub async fn synchronize_startup_cluster_config(
        self: &Arc<Self>,
        initial: &LoadedConfig,
        timeout: Duration,
    ) -> anyhow::Result<()> {
        let digest = initial.cluster_digest().to_string();
        let payload = initial.cluster_payload().to_vec();
        let deadline = Instant::now() + timeout;
        let mut last_error = None;
        loop {
            let (committed_revision, committed_digest) =
                self.state_store.read_cluster_config_head()?;
            if committed_revision == 0 && self.local_node_id == self.bootstrap_node_id {
                match self
                    .propose_cluster_config(0, timeout, false, digest.clone(), payload.clone())
                    .await
                {
                    Ok(_) => continue,
                    Err(error) => last_error = Some(error),
                }
            }
            if committed_revision > 0 {
                match self.authoritative_cluster_config().await {
                    Ok(committed)
                        if committed_revision == committed.revision
                            && committed_digest.as_deref() == Some(committed.digest.as_str()) =>
                    {
                        self.cluster_config
                            .recover(committed.revision, &committed.digest, &committed.payload)
                            .await
                            .context("recover committed cluster configuration")?;
                        self.cluster_config.mark_startup_synchronized();
                        if self.refresh_cluster_serving_guard_from_store().await {
                            return Ok(());
                        }
                        last_error = Some(anyhow::anyhow!(
                            "local cluster configuration changed while startup recovery was completing"
                        ));
                    }
                    Ok(_) => {
                        last_error = Some(anyhow::anyhow!(
                            "local coordination state is still behind the current leader"
                        ));
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            if Instant::now() >= deadline {
                return Err(last_error.unwrap_or_else(|| {
                    anyhow::anyhow!(
                        "timed out waiting for bootstrap member to commit initial cluster configuration"
                    )
                }));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 业务作用: 返回数据连接使用的集群配置一致性门禁。
    ///
    /// 返回: 当前值为假时新连接必须拒绝，已有连接必须立即关闭。
    pub fn cluster_configuration_guard(&self) -> watch::Receiver<bool> {
        self.cluster_config.serving_guard()
    }

    /// 业务作用: 返回本节点是否已经完成启动时的权威配置恢复，可以接受新的配置提案。
    ///
    /// 返回: 已从当前领导者确认提交头并恢复运行代时返回真。
    pub fn cluster_configuration_proposals_ready(&self) -> bool {
        self.cluster_config
            .startup_synchronized
            .load(Ordering::Acquire)
    }

    /// 业务作用: 判断一个运行时是否由当前协调状态机提交的全局配置构造。
    ///
    /// 参数说明:
    /// - `cluster_config_digest`: 运行时构造时使用的规范化全局配置摘要。
    ///
    /// 返回: 首次引导尚无提交版本，或摘要与当前提交版本完全一致时返回真。
    pub fn cluster_config_accepts_digest(
        &self,
        cluster_config_digest: &str,
    ) -> anyhow::Result<bool> {
        let (committed_revision, committed_digest) = self.state_store.read_cluster_config_head()?;
        Ok(committed_revision == 0 || committed_digest.as_deref() == Some(cluster_config_digest))
    }

    /// 业务作用: 返回本节点是否正在运行协调状态机要求的精确配置版本。
    ///
    /// 返回: 本地活动磁盘指针、运行代和协调提交版本相等时返回真。
    pub async fn cluster_configuration_ready(&self) -> bool {
        if !self
            .cluster_config
            .startup_synchronized
            .load(Ordering::Acquire)
        {
            self.cluster_config.serving.send_replace(false);
            return false;
        }
        match self.state_store.read_cluster_config_head() {
            Ok((committed_revision, committed_digest)) => {
                if self.cluster_config.serving_frozen().await {
                    let still_running_precommit_revision =
                        if let Some(digest) = committed_digest.as_deref() {
                            self.cluster_config
                                .is_active(committed_revision, digest)
                                .await
                        } else {
                            false
                        };
                    if still_running_precommit_revision {
                        self.cluster_config.serving.send_replace(false);
                        return false;
                    }
                }
                if self
                    .refresh_cluster_serving_guard_head(
                        committed_revision,
                        committed_digest.as_deref(),
                    )
                    .await
                {
                    return true;
                }
                let state = match self.state() {
                    Ok(state) => state,
                    Err(_) => {
                        self.cluster_config.serving.send_replace(false);
                        return false;
                    }
                };
                let Some(digest) = state.config_digest.as_deref() else {
                    return false;
                };
                if state.config_revision == 0 || state.config_payload.as_slice().is_empty() {
                    return false;
                }
                if let Err(error) = self
                    .cluster_config
                    .recover(
                        state.config_revision,
                        digest,
                        state.config_payload.as_slice(),
                    )
                    .await
                {
                    warn!(
                        revision = state.config_revision,
                        error = %error,
                        "落后成员自动恢复已提交集群配置失败"
                    );
                    return false;
                }
                self.refresh_cluster_serving_guard_from_store().await
            }
            Err(_) => {
                self.cluster_config.serving.send_replace(false);
                false
            }
        }
    }

    /// 业务作用: 返回静态创世成员表的多数派门槛，存活节点数变化不会降低该值。
    /// 返回: 返回 `成员总数 / 2 + 1`。
    pub fn majority(&self) -> usize {
        self.member_count / 2 + 1
    }

    /// 业务作用: 从状态存储轻量读取已提交版本头，同时依据当前共识权威刷新全局数据面门禁。
    ///
    /// 返回: 版本头可读、配置版本一致且领导联系仍在保护窗口内时返回真。
    async fn refresh_cluster_serving_guard_from_store(&self) -> bool {
        match self.state_store.read_cluster_config_head() {
            Ok((committed_revision, committed_digest)) => {
                self.refresh_cluster_serving_guard_head(
                    committed_revision,
                    committed_digest.as_deref(),
                )
                .await
            }
            Err(_) => {
                self.cluster_config.serving.send_replace(false);
                false
            }
        }
    }

    /// 业务作用: 使用轻量配置版本头刷新数据面门禁，高频路径不得克隆完整配置正文。
    ///
    /// 参数说明:
    /// - `committed_revision`: 协调状态机最近提交的全局配置版本。
    /// - `committed_digest`: 与提交版本绑定的规范化配置摘要。
    ///
    /// 返回: 协调权威有效且本地运行代与提交头一致时返回真。
    async fn refresh_cluster_serving_guard_head(
        &self,
        committed_revision: u64,
        committed_digest: Option<&str>,
    ) -> bool {
        if !self.route_authoritative().await {
            self.cluster_config.serving.send_replace(false);
            return false;
        }
        self.cluster_config
            .refresh_serving_guard(committed_revision, committed_digest)
            .await
    }

    /// 业务作用: 返回单次外部协调动作的最长批准窗口，供代理启动阶段等待领导者完成初始拓扑修复。
    /// 返回: 返回由 `coordination.action_timeout_ms` 构造的持续时间。
    pub fn action_timeout(&self) -> Duration {
        self.action_timeout
    }

    /// 业务作用: 返回数据路由允许沿用最近一次协调权威的保护窗口，供托管复制 HA 数据面建立独立失权监视器。
    /// 返回: 返回由 `coordination.route_guard_timeout_ms` 构造的持续时间。
    pub fn route_guard_timeout(&self) -> Duration {
        self.route_guard_timeout
    }

    /// 业务作用: 判断本节点是否是当前协议确认的领导者，只有领导节点上的 HA watcher 才能执行外部控制动作。
    /// 返回: 双节点角色或多数派当前领导者指向本节点时返回真。
    pub async fn is_local_leader(&self) -> bool {
        match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => pair.is_local_leader().await,
            CoordinatorProtocolRuntime::Quorum(raft) => {
                raft.current_leader().await == Some(self.local_node_id)
            }
        }
    }

    /// 业务作用: 返回当前协调协议确认的领导节点编号。
    ///
    /// 返回: 领导者尚未形成时返回 `None`。
    pub(crate) async fn leader_node_id(&self) -> anyhow::Result<Option<u64>> {
        match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => pair.leader_node_id(),
            CoordinatorProtocolRuntime::Quorum(raft) => Ok(raft.current_leader().await),
        }
    }

    /// 业务作用: 从当前协调领导者读取已经完成线性一致确认的最新配置正文。
    ///
    /// 返回: 当前提交 revision、摘要和规范化正文；领导尚未形成或响应身份不匹配时返回错误。
    async fn authoritative_cluster_config(&self) -> anyhow::Result<PersistedClusterConfig> {
        let leader = self
            .leader_node_id()
            .await?
            .context("coordination cluster has no current leader")?;
        if leader == self.local_node_id {
            return self.linearizable_local_cluster_config().await;
        }
        let mut client = self.network.configuration_client(leader).await?;
        let response = client
            .get_cluster_config(ClusterConfigRequest {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                expected_revision: 0,
                revision: 0,
                digest: String::new(),
                payload: Vec::new(),
                schema_version: CLUSTER_CONFIG_SCHEMA_VERSION,
                transaction_timeout_ms: duration_millis_u64(self.action_timeout),
                force_revision: false,
            })
            .await
            .context("read authoritative cluster configuration from leader")?
            .into_inner();
        validate_cluster_config_response(&response, leader, None, None)?;
        let persisted = PersistedClusterConfig {
            revision: response.revision,
            digest: response.digest,
            payload: response.payload,
        };
        validate_persisted_cluster_config(&persisted)?;
        Ok(persisted)
    }

    /// 业务作用: 在本节点仍为当前领导者的前提下取得线性一致的协调状态。
    ///
    /// 返回: 已确认不会被旧领导者读取覆盖的最新状态；领导权变化或多数派不可用时返回错误。
    pub(crate) async fn linearizable_local_cluster_config(
        &self,
    ) -> anyhow::Result<PersistedClusterConfig> {
        if !self.is_local_leader().await {
            bail!("local coordinator is not the current leader");
        }
        match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => {
                if !pair.route_authoritative().await {
                    bail!("pair coordinator cannot confirm current leader authority");
                }
            }
            CoordinatorProtocolRuntime::Quorum(raft) => {
                raft.ensure_linearizable()
                    .await
                    .context("confirm linearizable quorum configuration read")?;
            }
        }
        let state = self.state()?;
        if state.config_revision == 0 {
            bail!("cluster configuration has not been committed");
        }
        if state.config_payload.as_slice().is_empty() {
            let digest = state
                .config_digest
                .as_deref()
                .context("committed cluster configuration has no digest")?;
            return self
                .cluster_config
                .store
                .load_committed(state.config_revision, digest)
                .context("load pre-payload-schema committed cluster configuration");
        }
        persisted_cluster_config_from_state(&state)
    }

    /// 业务作用: 处理领导节点广播到本成员的配置 prepare 阶段。
    ///
    /// 参数说明:
    /// - `request`: 已通过 gRPC 集群和成员身份校验的候选配置。
    ///
    /// 返回: 本地协调版本匹配、落盘和预检全部完成时返回成功。
    pub(crate) async fn prepare_cluster_config_from_leader(
        &self,
        request: &ClusterConfigRequest,
    ) -> anyhow::Result<()> {
        let (committed_revision, _) = self.state_store.read_cluster_config_head()?;
        if committed_revision != request.expected_revision {
            bail!("local cluster configuration revision does not match proposal");
        }
        let next_revision = request
            .expected_revision
            .checked_add(1)
            .context("cluster configuration revision space is exhausted")?;
        if request.revision != next_revision {
            bail!("candidate cluster configuration revision is not consecutive");
        }
        self.cluster_config
            .prepare(
                request.revision,
                &request.digest,
                &request.payload,
                Duration::from_millis(request.transaction_timeout_ms),
            )
            .await
    }

    /// 业务作用: 处理协调日志已经提交后的配置 activate 阶段。
    ///
    /// 参数说明:
    /// - `request`: 已通过领导身份校验的已提交配置版本。
    ///
    /// 返回: 本地协调状态、磁盘候选和运行代全部收敛时返回成功。
    pub(crate) async fn activate_cluster_config_from_leader(
        &self,
        request: &ClusterConfigRequest,
    ) -> anyhow::Result<()> {
        self.wait_for_cluster_config_commit(request.revision, &request.digest)
            .await?;
        self.cluster_config
            .recover(request.revision, &request.digest, &request.payload)
            .await
    }

    /// 业务作用: 删除领导节点在共识提交前明确中止的本地候选配置。
    ///
    /// 参数说明:
    /// - `revision`: 要中止的候选配置版本。
    /// - `digest`: 要中止的候选配置摘要。
    ///
    /// 返回: 候选不存在或删除完成时返回成功。
    pub(crate) async fn abort_cluster_config_from_leader(
        &self,
        revision: u64,
        digest: &str,
    ) -> anyhow::Result<()> {
        self.cluster_config.abort(revision, digest).await
    }

    /// 业务作用: 从任意成员接收一份配置并由当前领导者执行多数派落盘、提交和在线成员发布。
    ///
    /// 参数说明:
    /// - `expected_revision`: 生成候选配置时本地活动运行代对应的协调版本。
    /// - `transaction_timeout`: 完整 prepare、提交和发布事务的最长持续时间。
    /// - `force_revision`: 正文相同时是否仍需让全部成员重读本地代理 TLS 依赖。
    /// - `digest`: 规范化配置载荷的 SHA-256 摘要。
    /// - `payload`: 移除节点本地身份字段后的规范化 JSON 配置正文。
    ///
    /// 返回: 多数派确认活动 revision 和摘要后返回响应，并携带尚未收敛成员数。
    pub async fn propose_cluster_config(
        self: &Arc<Self>,
        expected_revision: u64,
        transaction_timeout: Duration,
        force_revision: bool,
        digest: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<ClusterConfigResponse> {
        let leader = self
            .leader_node_id()
            .await?
            .context("coordination cluster has no current leader")?;
        if leader != self.local_node_id {
            let mut client = self.network.configuration_client(leader).await?;
            let mut request = tonic::Request::new(ClusterConfigRequest {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                expected_revision,
                revision: 0,
                digest,
                payload,
                schema_version: CLUSTER_CONFIG_SCHEMA_VERSION,
                transaction_timeout_ms: duration_millis_u64(transaction_timeout),
                force_revision,
            });
            request.set_timeout(transaction_timeout);
            let response = client
                .propose_cluster_config(request)
                .await
                .context("forward cluster configuration proposal")?
                .into_inner();
            validate_cluster_config_response(&response, leader, None, None)?;
            return Ok(response);
        }
        // HTTP/gRPC 调用方可能在客户端超时或断开后丢弃等待 future。领导节点事务一旦
        // 进入 prepare/HA precommit 就必须自行收口，不能让请求生命周期取消外部副作用。
        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .commit_cluster_config_as_leader(
                    expected_revision,
                    transaction_timeout,
                    force_revision,
                    digest,
                    payload,
                )
                .await
        })
        .await
        .context("join cluster configuration transaction task")?
    }

    /// 业务作用: 由当前领导者执行多数派配置事务并通知全部可达成员并发发布。
    ///
    /// 参数说明:
    /// - `expected_revision`: 提案创建时活动配置的版本，用于拒绝丢失更新。
    /// - `transaction_timeout`: 完整事务允许持续的最长时间。
    /// - `force_revision`: 正文相同时是否仍需推进版本并重读本地代理 TLS 依赖。
    /// - `digest`: 规范化配置载荷摘要。
    /// - `payload`: 规范化配置正文。
    ///
    /// 返回: 多数派 prepare、共识提交和已 prepare 成员 activate 成功时返回确认。
    async fn commit_cluster_config_as_leader(
        &self,
        expected_revision: u64,
        transaction_timeout: Duration,
        force_revision: bool,
        digest: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<ClusterConfigResponse> {
        let _transaction = self.configuration_transaction.lock().await;
        if transaction_timeout.is_zero() {
            bail!("cluster configuration transaction timeout must be > 0");
        }
        let started = Instant::now();
        // 不能用 tokio::time::timeout 直接丢弃整个事务 future：HA precommit 已经进入
        // supervisor 后会独立执行 fencing、切主或持久化交接，取消等待者并不会取消这些
        // 外部副作用。这里让各网络和后端步骤继续使用自身有界超时，并把已经开始的事务
        // 明确收口，避免先 Abort、后异步完成切主的失序结果。
        let result = self
            .commit_cluster_config_as_leader_locked(
                expected_revision,
                transaction_timeout,
                force_revision,
                digest,
                payload,
            )
            .await;
        if started.elapsed() > transaction_timeout {
            warn!(
                expected_revision,
                configured_timeout_ms = duration_millis_u64(transaction_timeout),
                elapsed_ms = duration_millis_u64(started.elapsed()),
                committed = result.is_ok(),
                "集群配置事务超过总预算；已等待不可取消的 HA 外部动作安全收口"
            );
        }
        result
    }

    /// 业务作用: 在已经持有配置事务锁并建立超时边界后执行领导节点事务主体。
    ///
    /// 参数说明:
    /// - `expected_revision`: 提案创建时活动配置的版本。
    /// - `transaction_timeout`: 远端配置阶段沿用的请求时间预算。
    /// - `force_revision`: 正文相同时是否仍需推进版本。
    /// - `digest`: 规范化配置载荷摘要。
    /// - `payload`: 规范化配置正文。
    ///
    /// 返回: 多数派提交并发布完成时返回确认。
    async fn commit_cluster_config_as_leader_locked(
        &self,
        expected_revision: u64,
        transaction_timeout: Duration,
        force_revision: bool,
        digest: String,
        payload: Vec<u8>,
    ) -> anyhow::Result<ClusterConfigResponse> {
        let precommit_deadline = Instant::now() + transaction_timeout;
        if !self.is_local_leader().await {
            bail!("local coordinator is not the current leader");
        }
        let state = self.state()?;
        if state.config_digest.as_deref() == Some(digest.as_str())
            && state.config_payload.as_slice() == payload.as_slice()
            && (!force_revision || expected_revision < state.config_revision)
        {
            self.cluster_config
                .recover(state.config_revision, &digest, &payload)
                .await
                .context("reconcile local member for an idempotent configuration proposal")?;
            let request = ClusterConfigRequest {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                expected_revision: state.config_revision.saturating_sub(1),
                revision: state.config_revision,
                digest: digest.clone(),
                payload: payload.clone(),
                schema_version: CLUSTER_CONFIG_SCHEMA_VERSION,
                transaction_timeout_ms: duration_millis_u64(transaction_timeout),
                force_revision: false,
            };
            let remote_members = self
                .member_ids
                .iter()
                .copied()
                .filter(|member| *member != self.local_node_id)
                .collect::<Vec<_>>();
            let (remote_activated, activation_errors) = self
                .activate_cluster_config_members(&request, &remote_members, transaction_timeout)
                .await;
            for error in activation_errors {
                warn!(
                    revision = state.config_revision,
                    error = %error,
                    "幂等配置提案仍有成员尚未完成活动版本收敛"
                );
            }
            let activated_count = remote_activated.saturating_add(1);
            let pending_members =
                activation_pending_members(self.member_count, self.majority(), activated_count)?;
            return Ok(ClusterConfigResponse {
                accepted: true,
                responder_node_id: self.local_node_id,
                revision: state.config_revision,
                digest,
                error: String::new(),
                payload: Vec::new(),
                pending_members,
            });
        }
        if state.config_revision != expected_revision {
            bail!(
                "cluster configuration proposal is based on revision {expected_revision}, but current revision is {}",
                state.config_revision
            );
        }
        let revision = expected_revision
            .checked_add(1)
            .context("cluster configuration revision space is exhausted")?;
        let request = ClusterConfigRequest {
            cluster_id: self.cluster_id.to_string(),
            sender_node_id: self.local_node_id,
            expected_revision,
            revision,
            digest: digest.clone(),
            payload: payload.clone(),
            schema_version: CLUSTER_CONFIG_SCHEMA_VERSION,
            transaction_timeout_ms: duration_millis_u64(transaction_timeout),
            force_revision,
        };

        self.cluster_config
            .prepare(revision, &digest, &payload, transaction_timeout)
            .await?;
        let remote_members = self
            .member_ids
            .iter()
            .copied()
            .filter(|member| *member != self.local_node_id)
            .collect::<Vec<_>>();
        let prepare_results = join_all(remote_members.iter().copied().map(|member| {
            let network = self.network.clone();
            let request = request.clone();
            let digest = digest.clone();
            async move {
                let mut client = network.configuration_client(member).await?;
                let mut request = tonic::Request::new(request);
                request.set_timeout(transaction_timeout);
                let response = client
                    .prepare_cluster_config(request)
                    .await
                    .with_context(|| format!("prepare cluster configuration on member `{member}`"))?
                    .into_inner();
                validate_cluster_config_response(&response, member, Some(revision), Some(&digest))?;
                Ok::<u64, anyhow::Error>(member)
            }
        }))
        .await;
        let mut prepared_members = Vec::with_capacity(remote_members.len());
        let mut prepare_errors = Vec::new();
        for result in prepare_results {
            match result {
                Ok(member) => prepared_members.push(member),
                Err(error) => prepare_errors.push(error),
            }
        }
        let prepared_count = prepared_members.len().saturating_add(1);
        if prepared_count < self.majority() {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let reasons = prepare_errors
                .iter()
                .map(|error| format!("{error:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!(
                "cluster configuration prepared by {prepared_count} members but requires majority {}: {reasons}",
                self.majority()
            );
        }
        for error in &prepare_errors {
            warn!(
                revision,
                error = %format!("{error:#}"),
                "少数派成员未参与配置预检，将在恢复后自动追平"
            );
        }

        if Instant::now() >= precommit_deadline {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let _ = self.refresh_cluster_serving_guard_from_store().await;
            bail!("cluster configuration transaction budget expired before HA topology precommit");
        }

        if let Err(error) = self
            .cluster_config
            .precommit(revision, &digest, &payload)
            .await
        {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let _ = self.refresh_cluster_serving_guard_from_store().await;
            return Err(error).context(
                "apply HA topology side effects after every prepared member completed preflight",
            );
        }

        if let Err(error) = self
            .cluster_config
            .prepare(revision, &digest, &payload, transaction_timeout)
            .await
        {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let _ = self.refresh_cluster_serving_guard_from_store().await;
            return Err(error).context("revalidate local configuration after HA topology changes");
        }
        let revalidate_results = join_all(remote_members.iter().copied().map(|member| {
            let network = self.network.clone();
            let request = request.clone();
            let digest = digest.clone();
            async move {
                let mut client = network.configuration_client(member).await?;
                let mut request = tonic::Request::new(request);
                request.set_timeout(transaction_timeout);
                let response = client
                    .prepare_cluster_config(request)
                    .await
                    .with_context(|| {
                        format!(
                            "revalidate cluster configuration on member `{member}` after HA topology changes"
                        )
                    })?
                    .into_inner();
                validate_cluster_config_response(
                    &response,
                    member,
                    Some(revision),
                    Some(&digest),
                )?;
                Ok::<u64, anyhow::Error>(member)
            }
        }))
        .await;
        let mut revalidated_members = Vec::with_capacity(remote_members.len());
        let mut revalidate_errors = Vec::new();
        for result in revalidate_results {
            match result {
                Ok(member) => revalidated_members.push(member),
                Err(error) => revalidate_errors.push(error),
            }
        }
        let revalidated_count = revalidated_members.len().saturating_add(1);
        if revalidated_count < self.majority() {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let _ = self.refresh_cluster_serving_guard_from_store().await;
            let reasons = revalidate_errors
                .iter()
                .map(|error| format!("{error:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!(
                "cluster configuration revalidated by {revalidated_count} members after HA topology changes but requires majority {}: {reasons}",
                self.majority()
            );
        }
        let failed_revalidation_members = prepared_members
            .iter()
            .copied()
            .filter(|member| !revalidated_members.contains(member))
            .collect::<Vec<_>>();
        if !failed_revalidation_members.is_empty() {
            self.abort_cluster_config_members(revision, &digest, &failed_revalidation_members)
                .await;
        }
        prepared_members = revalidated_members;

        let committed = match self
            .submit(CoordinationCommand::CommitClusterConfig {
                expected_revision,
                revision,
                config_digest: digest.clone(),
                config_payload: ClusterConfigPayload::new(payload.clone()),
            })
            .await
        {
            Ok(committed) => committed,
            Err(error) => {
                self.abort_cluster_config_members(revision, &digest, &prepared_members)
                    .await;
                let _ = self.cluster_config.abort(revision, &digest).await;
                let _ = self.refresh_cluster_serving_guard_from_store().await;
                return Err(error);
            }
        };
        if !committed.accepted {
            self.abort_cluster_config_members(revision, &digest, &prepared_members)
                .await;
            let _ = self.cluster_config.abort(revision, &digest).await;
            let _ = self.refresh_cluster_serving_guard_from_store().await;
            bail!(
                "coordinator rejected cluster configuration commit: {}",
                committed
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }

        let local_activation = self.cluster_config.activate(revision, &digest, &payload);
        let remote_activations =
            self.activate_cluster_config_members(&request, &prepared_members, transaction_timeout);
        let (local_result, activate_results) = tokio::join!(local_activation, remote_activations);
        let mut activation_errors = Vec::new();
        let mut activated_count = 0_usize;
        match local_result {
            Ok(()) => activated_count = activated_count.saturating_add(1),
            Err(error) => {
                activation_errors.push(format!("local member activation failed: {error:#}"))
            }
        }
        activated_count = activated_count.saturating_add(activate_results.0);
        activation_errors.extend(activate_results.1);
        for error in &activation_errors {
            warn!(
                revision,
                error = %error,
                "已提交配置仍有成员未完成活动版本发布，将保持关闭数据面并自动收敛"
            );
        }
        let pending_members =
            activation_pending_members(self.member_count, self.majority(), activated_count)?;
        Ok(ClusterConfigResponse {
            accepted: true,
            responder_node_id: self.local_node_id,
            revision,
            digest,
            error: String::new(),
            payload: Vec::new(),
            pending_members,
        })
    }

    /// 业务作用: 并发要求指定远端成员发布同一份已经提交的集群配置。
    ///
    /// 参数说明:
    /// - `request`: 包含精确 revision、摘要和正文的已提交配置请求。
    /// - `members`: 本轮需要尝试收敛的远端静态成员编号。
    /// - `transaction_timeout`: 每个远端发布请求允许使用的时间预算。
    ///
    /// 返回: 成功发布的成员数量和不包含配置正文的失败原因。
    async fn activate_cluster_config_members(
        &self,
        request: &ClusterConfigRequest,
        members: &[u64],
        transaction_timeout: Duration,
    ) -> (usize, Vec<String>) {
        let revision = request.revision;
        let digest = request.digest.clone();
        let results = join_all(members.iter().copied().map(|member| {
            let network = self.network.clone();
            let request = request.clone();
            let digest = digest.clone();
            async move {
                let mut client = network.configuration_client(member).await?;
                let mut request = tonic::Request::new(request);
                request.set_timeout(transaction_timeout);
                let response = client
                    .activate_cluster_config(request)
                    .await
                    .with_context(|| {
                        format!("activate cluster configuration on member `{member}`")
                    })?
                    .into_inner();
                validate_cluster_config_response(&response, member, Some(revision), Some(&digest))
            }
        }))
        .await;
        let mut activated = 0_usize;
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(()) => activated = activated.saturating_add(1),
                Err(error) => errors.push(format!("{error:#}")),
            }
        }
        (activated, errors)
    }

    /// 业务作用: 向已经完成 prepare 的远端成员发送候选中止请求。
    ///
    /// 参数说明:
    /// - `revision`: 要放弃的候选配置版本。
    /// - `digest`: 要放弃的候选摘要。
    /// - `members`: 已确认 prepare 成功的远端成员编号。
    async fn abort_cluster_config_members(&self, revision: u64, digest: &str, members: &[u64]) {
        join_all(members.iter().copied().map(|member| {
            let network = self.network.clone();
            let request = ClusterConfigRequest {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                expected_revision: revision.saturating_sub(1),
                revision,
                digest: digest.to_string(),
                payload: Vec::new(),
                schema_version: CLUSTER_CONFIG_SCHEMA_VERSION,
                transaction_timeout_ms: duration_millis_u64(self.action_timeout),
                force_revision: false,
            };
            async move {
                let Ok(mut client) = network.configuration_client(member).await else {
                    return;
                };
                let mut request = tonic::Request::new(request);
                request.set_timeout(network.rpc_timeout());
                let _ = client.abort_cluster_config(request).await;
            }
        }))
        .await;
    }

    /// 业务作用: 等待本成员的复制状态机应用领导节点已经提交的配置日志。
    ///
    /// 参数说明:
    /// - `revision`: activate 请求要求的精确配置版本。
    /// - `digest`: activate 请求要求的精确规范化摘要。
    ///
    /// 返回: 状态机在协调动作超时前收敛到目标版本时返回成功。
    async fn wait_for_cluster_config_commit(
        &self,
        revision: u64,
        digest: &str,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.action_timeout;
        loop {
            let (committed_revision, committed_digest) =
                self.state_store.read_cluster_config_head()?;
            if committed_revision == revision && committed_digest.as_deref() == Some(digest) {
                return Ok(());
            }
            if committed_revision > revision {
                bail!("local coordinator has already committed a newer cluster configuration");
            }
            if Instant::now() >= deadline {
                bail!("local coordinator did not commit the requested cluster configuration");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// 业务作用: 提交一条协议无关业务命令；跟随者会转发到已知领导者，本地不会伪造提交结果。
    /// 参数说明:
    /// - `command`: 需要获得集群全序的业务命令。
    ///
    /// 返回: 返回已提交状态机响应；领导未知、转发失败或多数派不足时返回错误。
    pub async fn submit(
        &self,
        command: CoordinationCommand,
    ) -> anyhow::Result<CoordinationResponse> {
        match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => pair.submit(command).await,
            CoordinatorProtocolRuntime::Quorum(raft) => {
                let leader = raft
                    .current_leader()
                    .await
                    .context("quorum coordinator has no current leader")?;
                if leader == self.local_node_id {
                    return raft
                        .client_write(command)
                        .await
                        .map(|response| response.data)
                        .context("commit quorum coordination command");
                }
                let mut client = self.network.pair_client(leader).await?;
                let response = client
                    .submit_command(RpcEnvelope {
                        cluster_id: self.cluster_id.to_string(),
                        sender_node_id: self.local_node_id,
                        payload: serde_json::to_vec(&command)
                            .context("encode forwarded quorum coordination command")?,
                        error: String::new(),
                    })
                    .await
                    .context("forward quorum coordination command")?
                    .into_inner();
                if response.cluster_id != self.cluster_id.as_ref()
                    || response.sender_node_id != leader
                {
                    bail!("quorum leader response identity does not match the requested member");
                }
                if !response.error.is_empty() {
                    bail!(
                        "quorum leader rejected forwarded command: {}",
                        response.error
                    );
                }
                serde_json::from_slice(&response.payload)
                    .context("decode forwarded quorum coordination response")
            }
        }
    }

    /// 业务作用: 上报本代理节点对一个 HA 组的最新观察，同一节点的新观察会覆盖旧观察而不会重复计票。
    /// 参数说明:
    /// - `group_id`: 托管复制 HA 组标识。
    /// - `config_revision`: 生成观察的 HA 运行时所绑定的全局配置版本。
    /// - `group_config_digest`: 生成观察的 HA 运行时所绑定的组配置摘要。
    /// - `fingerprint`: 主节点、角色和故障类型构成的稳定事实指纹。
    /// - `current_master`: 本代理当前认为的主节点。
    /// - `master_reachable`: 当前主节点是否可达。
    ///
    /// 返回: 观察经复制提交后返回成功，否则返回错误。
    pub async fn report_observation(
        &self,
        group_id: &str,
        config_revision: u64,
        group_config_digest: String,
        fingerprint: String,
        current_master: String,
        master_reachable: bool,
    ) -> anyhow::Result<()> {
        let observed_at_unix_ms = now_unix_millis()?;
        let response = self
            .submit(CoordinationCommand::RecordObservation {
                group_id: group_id.to_string(),
                config_revision,
                group_config_digest,
                observation: CoordinationObservation {
                    reporter_node_id: self.local_node_id,
                    observed_at_unix_ms,
                    fingerprint,
                    current_master,
                    master_reachable,
                },
            })
            .await?;
        if !response.accepted {
            bail!(
                "coordinator rejected observation: {}",
                response
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }
        Ok(())
    }

    /// 业务作用: 通过复制日志注册或核对托管复制 HA 组关键配置摘要，阻止配置不同的代理成员共同控制同一数据拓扑。
    /// 参数说明:
    /// - `group_id`: 需要注册的 HA 协调组标识。
    /// - `config_digest`: 调用方根据规范字段顺序计算的 SHA-256 摘要。
    /// - `cluster_config_digest`: 构造该 HA 运行时所依据的全局集群配置摘要。
    ///
    /// 返回: 摘要首次提交或与已提交值一致时返回绑定的全局配置版本；冲突时返回拒绝原因。
    pub async fn register_group_config(
        &self,
        group_id: &str,
        config_digest: String,
        cluster_config_digest: String,
    ) -> anyhow::Result<u64> {
        let response = self
            .submit(CoordinationCommand::RegisterGroupConfig {
                group_id: group_id.to_string(),
                config_digest,
                cluster_config_digest,
            })
            .await?;
        if !response.accepted {
            bail!(
                "coordinator rejected group configuration: {}",
                response
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }
        Ok(response.config_revision)
    }

    /// 业务作用: 请求批准一个托管复制 HA 外部动作，状态机同时校验观察票数、预期主节点和组内互斥。
    /// 参数说明:
    /// - `group_id`: 动作所属 HA 组。
    /// - `config_revision`: 申请动作的 HA 运行时所绑定的全局配置版本。
    /// - `group_config_digest`: 申请动作的 HA 运行时所绑定的组配置摘要。
    /// - `action_id`: 本次尝试唯一标识。
    /// - `kind`: 动作类型。
    /// - `fingerprint`: 需要达到票数门槛的故障事实指纹。
    /// - `expected_master`: 动作前预期主节点。
    /// - `target_master`: 计划操作的目标节点。
    /// - `observation_window`: 允许计票的观察时间窗口。
    /// - `required_observations`: 至少需要的不同代理报告数。
    ///
    /// 返回: 批准时返回绑定任期和日志位置的动作令牌，拒绝时返回具体原因。
    #[allow(clippy::too_many_arguments)]
    pub async fn begin_action(
        &self,
        group_id: &str,
        config_revision: u64,
        group_config_digest: String,
        action_id: String,
        kind: CoordinationActionKind,
        fingerprint: String,
        expected_master: String,
        target_master: String,
        observation_window: Duration,
        required_observations: usize,
    ) -> anyhow::Result<String> {
        self.wait_for_active_action_quiescence(group_id).await?;
        let cutoff = now_unix_millis()?
            .saturating_sub(u64::try_from(observation_window.as_millis()).unwrap_or(u64::MAX));
        let requested_at_unix_ms = now_unix_millis()?;
        let expires_at_unix_ms = requested_at_unix_ms
            .saturating_add(u64::try_from(self.action_timeout.as_millis()).unwrap_or(u64::MAX));
        let response = self
            .submit(CoordinationCommand::BeginAction {
                group_id: group_id.to_string(),
                config_revision,
                group_config_digest,
                action_id,
                kind,
                requester_node_id: self.local_node_id,
                fingerprint,
                expected_master,
                target_master,
                observation_cutoff_unix_ms: cutoff,
                required_observations,
                requested_at_unix_ms,
                expires_at_unix_ms,
            })
            .await?;
        if !response.accepted {
            bail!(
                "coordinator rejected action: {}",
                response
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }
        response
            .action_token
            .context("coordinator accepted action without returning a token")
    }

    /// 业务作用: 新领导者替换遗留动作前等待完整动作窗口和路由保护窗口，隔离不同领导任期中的外部副作用。
    /// 参数说明:
    /// - `group_id`: 即将申请新动作的托管复制 HA 组。
    ///
    /// 返回: 活动动作已由原领导者提交结果时立即成功；否则只有本节点持续保持领导权和路由权威并等待安全窗口后才允许尝试替换。
    async fn wait_for_active_action_quiescence(&self, group_id: &str) -> anyhow::Result<()> {
        loop {
            let Some(active_token) = self
                .group_state(group_id)?
                .and_then(|group| group.active_action)
                .map(|action| action.token)
            else {
                return Ok(());
            };
            let wait_window = self.action_timeout.saturating_add(self.route_guard_timeout);
            let deadline = Instant::now() + wait_window;
            loop {
                if !self.is_local_leader().await || !self.route_authoritative().await {
                    bail!(
                        "coordinator lost authority while waiting for an earlier action to quiesce"
                    );
                }
                let current_token = self
                    .group_state(group_id)?
                    .and_then(|group| group.active_action)
                    .map(|action| action.token);
                match current_token {
                    None => return Ok(()),
                    Some(token) if token != active_token => break,
                    Some(_) if Instant::now() >= deadline => return Ok(()),
                    Some(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        }
    }

    /// 业务作用: 使用原动作令牌报告外部动作结果，成功时发布新主节点并推进路由代际。
    /// 参数说明:
    /// - `group_id`: 动作所属 HA 组。
    /// - `token`: 开始动作时返回的完整令牌。
    /// - `success`: fencing、提主和复验是否全部成功。
    /// - `resulting_master`: 成功复验后的主节点地址。
    ///
    /// 返回: 状态机接受完成命令时返回响应，旧令牌或错目标会返回错误。
    pub async fn complete_action(
        &self,
        group_id: &str,
        token: String,
        success: bool,
        resulting_master: String,
    ) -> anyhow::Result<CoordinationResponse> {
        let response = self
            .submit(CoordinationCommand::CompleteAction {
                group_id: group_id.to_string(),
                token,
                success,
                resulting_master,
                completed_at_unix_ms: now_unix_millis()?,
            })
            .await?;
        if !response.accepted {
            bail!(
                "coordinator rejected action completion: {}",
                response
                    .reason
                    .unwrap_or_else(|| "unknown reason".to_string())
            );
        }
        Ok(response)
    }

    /// 业务作用: 判断当前节点最近是否拥有可验证的集群权威，数据面据此在失去领导通信后进入保护态。
    /// 返回: 双节点依据对端心跳或 fencing，多数派依据当前领导和最近共识联系判断。
    pub async fn route_authoritative(&self) -> bool {
        // 持久化层一旦出现无法确认最终落盘状态的错误会永久失败关闭。即使共识运行时仍暂存
        // 旧领导信息，也不能仅凭计时窗口继续发布路由。
        if self.state_store.check_healthy().is_err() {
            return false;
        }
        match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => pair.route_authoritative().await,
            CoordinatorProtocolRuntime::Quorum(raft) => {
                raft.current_leader().await.is_some()
                    && self.last_raft_authority_contact.lock().await.elapsed()
                        <= self.route_guard_timeout
            }
        }
    }

    /// 业务作用: 返回最近已提交的完整业务状态副本，读取不会绕过路由权威检查自动开放服务。
    /// 返回: 返回持久化状态副本。
    pub fn state(&self) -> anyhow::Result<CoordinationState> {
        self.state_store.read_state()
    }

    /// 业务作用: 读取单个 HA 组的已提交协调状态，供周期拓扑探测和动作复验使用。
    ///
    /// 参数说明:
    /// - `group_id`: 需要读取的稳定 HA 组标识。
    ///
    /// 返回: 组存在时返回其轻量副本；协调存储失败时返回错误。
    pub fn group_state(
        &self,
        group_id: &str,
    ) -> anyhow::Result<Option<super::state::CoordinationGroupState>> {
        self.state_store.read_group(group_id)
    }

    /// 业务作用: 在每个外部副作用前复验领导权和活动令牌，旧领导者或已经完成的动作会立即失去执行资格。
    /// 参数说明:
    /// - `group_id`: 动作所属 HA 组。
    /// - `token`: 开始动作时获得的完整令牌。
    ///
    /// 返回: 本节点仍是领导者、路由权威新鲜且状态机活动令牌匹配时返回成功。
    pub async fn ensure_action_active(&self, group_id: &str, token: &str) -> anyhow::Result<()> {
        if !self.is_local_leader().await {
            bail!("local coordinator is no longer leader");
        }
        if !self.route_authoritative().await {
            bail!("coordinator authority is outside route guard window");
        }
        let group = self
            .group_state(group_id)?
            .with_context(|| format!("coordinator group `{group_id}` does not exist"))?;
        let active = group
            .active_action
            .as_ref()
            .context("coordinator group has no active action")?;
        if active.token != token {
            bail!("coordinator action token is no longer active");
        }
        let current_term = match &self.protocol {
            CoordinatorProtocolRuntime::Pair(pair) => pair.current_term()?,
            CoordinatorProtocolRuntime::Quorum(raft) => raft.metrics().borrow().current_term,
        };
        if current_term != active.approved_log_id.leader_id.term {
            bail!("coordinator action token belongs to an earlier leader term");
        }
        if now_unix_millis()? >= active.expires_at_unix_ms {
            bail!("coordinator action token has expired");
        }
        Ok(())
    }

    /// 业务作用: 周期比较本地活动配置和协调状态机版本，版本落后时立即关闭数据面门禁。
    ///
    /// 参数说明:
    /// - `shutdown`: 主进程统一关闭信号。
    ///
    /// 返回: 收到关闭信号后结束；读取错误按不一致处理并保持关闭式保护。
    async fn run_cluster_configuration_guard(&self, mut shutdown: watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = self.cluster_configuration_ready().await;
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.cluster_config.serving.send_replace(false);
                        return;
                    }
                }
            }
        }
    }

    /// 业务作用: 周期性要求多数派确认本节点领导权；成功后刷新本地领导者的路由权威时间。
    /// 参数说明:
    /// - `shutdown`: 主进程关闭信号。
    ///
    /// 返回: 无显式返回值；非领导者和少数派错误只让权威时间自然过期，不伪造健康状态。
    async fn run_quorum_authority_probe(&self, mut shutdown: watch::Receiver<bool>) {
        let CoordinatorProtocolRuntime::Quorum(raft) = &self.protocol else {
            return;
        };
        let interval_duration = (self.route_guard_timeout / 3).max(Duration::from_millis(100));
        let mut interval = tokio::time::interval(interval_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if raft.current_leader().await == Some(self.local_node_id) {
                        match tokio::time::timeout(
                            self.route_guard_timeout,
                            raft.ensure_linearizable(),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                *self.last_raft_authority_contact.lock().await = Instant::now();
                            }
                            Ok(Err(error)) => warn!(
                                node_id = self.local_node_id,
                                error = %error,
                                "多数派协调器无法确认线性一致领导权"
                            ),
                            Err(_) => warn!(
                                node_id = self.local_node_id,
                                "多数派协调器确认领导权超时"
                            ),
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }
}

/// 业务作用: 从协调状态机提取已经提交且可供成员恢复的配置版本。
///
/// 参数说明:
/// - `state`: 经过线性一致读取或本地复制应用取得的协调状态。
///
/// 返回: revision、摘要和正文全部自洽时返回持久化配置对象。
fn persisted_cluster_config_from_state(
    state: &CoordinationState,
) -> anyhow::Result<PersistedClusterConfig> {
    let persisted = PersistedClusterConfig {
        revision: state.config_revision,
        digest: state
            .config_digest
            .clone()
            .context("committed cluster configuration has no digest")?,
        payload: state.config_payload.as_slice().to_vec(),
    };
    validate_persisted_cluster_config(&persisted)?;
    Ok(persisted)
}

/// 业务作用: 校验从协调状态或 gRPC 取得的配置版本没有正文缺失或摘要错配。
///
/// 参数说明:
/// - `persisted`: 要用于本地原子落盘和运行代恢复的集群配置。
///
/// 返回: 版本、摘要格式、载荷大小和 SHA-256 全部有效时返回成功。
fn validate_persisted_cluster_config(persisted: &PersistedClusterConfig) -> anyhow::Result<()> {
    if persisted.revision == 0 {
        bail!("cluster configuration revision must be > 0");
    }
    if persisted.digest.len() != 64
        || !persisted
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("cluster configuration digest must be lowercase SHA-256");
    }
    if persisted.payload.is_empty()
        || persisted.payload.len() > crate::reload::MAX_CLUSTER_CONFIG_BYTES
    {
        bail!("cluster configuration payload must be between 1 byte and 16 MiB");
    }
    let actual = format!("{:x}", Sha256::digest(&persisted.payload));
    if actual != persisted.digest {
        bail!("cluster configuration payload digest does not match committed digest");
    }
    Ok(())
}

/// 业务作用: 读取当前纪元毫秒作为观察日志中的显式数据，状态机自身不访问各节点本地时钟。
/// 参数说明: 无。
/// 返回: 返回饱和到 u64 的纪元毫秒；系统时间非法时返回错误。
fn now_unix_millis() -> anyhow::Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis();
    Ok(u64::try_from(millis).unwrap_or(u64::MAX))
}

/// 业务作用: 将内部持续时间安全转换为协议使用的毫秒整数。
///
/// 参数说明:
/// - `duration`: 需要写入 gRPC 请求或诊断文本的持续时间。
///
/// 返回: 至少为一毫秒并在溢出时饱和到 `u64::MAX` 的数值。
fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

/// 业务作用: 根据已完成发布的静态成员数判断集群配置事务能否向运维端返回成功。
///
/// 参数说明:
/// - `member_count`: 静态创世成员总数。
/// - `majority`: 当前静态成员表要求的固定多数派数量。
/// - `activated_count`: 已确认活动 revision 和摘要匹配的成员数量。
///
/// 返回: 达到多数派时返回仍待收敛成员数；提交后尚未达到多数派时返回明确错误。
fn activation_pending_members(
    member_count: usize,
    majority: usize,
    activated_count: usize,
) -> anyhow::Result<u32> {
    if activated_count < majority {
        bail!(
            "cluster configuration committed but activated by only {activated_count} members; majority {majority} is required"
        );
    }
    Ok(u32::try_from(member_count.saturating_sub(activated_count)).unwrap_or(u32::MAX))
}

/// 业务作用: 校验集群配置 RPC 响应来自预期成员并确认预期版本和摘要。
///
/// 参数说明:
/// - `response`: 远端成员返回的配置阶段响应。
/// - `expected_member`: 本次 gRPC 实际连接的静态成员编号。
/// - `expected_revision`: prepare 或 activate 阶段要求确认的版本。
/// - `expected_digest`: prepare 或 activate 阶段要求确认的摘要。
///
/// 返回: 身份、结果、版本和摘要全部一致时返回成功。
fn validate_cluster_config_response(
    response: &ClusterConfigResponse,
    expected_member: u64,
    expected_revision: Option<u64>,
    expected_digest: Option<&str>,
) -> anyhow::Result<()> {
    if response.responder_node_id != expected_member {
        bail!("cluster configuration response member does not match requested member");
    }
    if !response.accepted {
        bail!(
            "cluster configuration member `{expected_member}` rejected request: {}",
            response.error
        );
    }
    if expected_revision.is_some_and(|revision| response.revision != revision) {
        bail!("cluster configuration member `{expected_member}` confirmed an unexpected revision");
    }
    if expected_digest.is_some_and(|digest| response.digest != digest) {
        bail!("cluster configuration member `{expected_member}` confirmed an unexpected digest");
    }
    Ok(())
}
