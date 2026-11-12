use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use crate::infrastructure::lifecycle::{TaskScope, TaskSpawner};
use anyhow::Context;
use futures_util::FutureExt;
use nasa::application::ConfigPoller;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::coordinator::runtime::CoordinatorHandle;

use super::{
    ConfigDiff, ConfigSource, LoadedConfig, ReloadPhase, ReloadSnapshot, ReloadState,
    ReloadTransactionId, ReloadTrigger,
};

const FAILED_CANDIDATE_RETRY_BASE: Duration = Duration::from_secs(1);
const FAILED_CANDIDATE_RETRY_MAX: Duration = Duration::from_secs(30);

/// 自动热更新失败后针对同一来源摘要执行的有界退避状态。
struct FailedCandidateRetry {
    digest: String,
    failures: u32,
    retry_not_before: Instant,
}

impl FailedCandidateRetry {
    /// 业务作用: 记录一次候选失败，并计算同摘要下一次自动尝试时间。
    ///
    /// 参数说明:
    /// - `previous`: 上一次失败状态；摘要变化时不会继承旧失败次数。
    /// - `digest`: 本次失败的本地配置来源摘要。
    ///
    /// 返回: 退避不超过固定上限的新失败状态。
    fn after_failure(previous: Option<&Self>, digest: String) -> Self {
        let failures = previous
            .filter(|failed| failed.digest == digest)
            .map_or(1, |failed| failed.failures.saturating_add(1));
        let exponent = failures.saturating_sub(1).min(5);
        let delay = FAILED_CANDIDATE_RETRY_BASE
            .saturating_mul(1_u32 << exponent)
            .min(FAILED_CANDIDATE_RETRY_MAX);
        Self {
            digest,
            failures,
            retry_not_before: Instant::now() + delay,
        }
    }

    /// 业务作用: 判断当前摘要是否仍处于失败退避窗口。
    ///
    /// 参数说明:
    /// - `digest`: 本次文件轮询读到的配置来源摘要。
    ///
    /// 返回: 同一摘要尚未到达重试时间时返回真。
    fn defers(&self, digest: &str) -> bool {
        self.digest == digest && Instant::now() < self.retry_not_before
    }
}

/// 表示运行时对候选配置执行只预检还是完整发布。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyMode {
    /// 仅由集群领导节点执行可能改变 HA 后端拓扑的提交前动作。
    HaPrecommit,
    /// 构造并预检临时资源，完成后全部释放，不修改活动运行代。
    ValidateOnly,
    /// 预检成功后原子发布新运行代并排空旧运行代。
    Publish,
}

/// 表示候选配置来自本地来源还是已经进入集群提交协议。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyOrigin {
    /// 单节点文件、SIGHUP 或管理接口请求。
    Local,
    /// 经过协调 gRPC prepare 或 activate 阶段的集群配置。
    Cluster,
}

/// 配置 supervisor 发给运行时管理器的单次串行应用请求。
pub struct ApplyConfigRequest {
    candidate: LoadedConfig,
    diff: ConfigDiff,
    mode: ApplyMode,
    origin: ApplyOrigin,
    response: oneshot::Sender<Result<(), String>>,
}

impl ApplyConfigRequest {
    /// 业务作用: 返回已经通过加载和静态校验的候选配置。
    ///
    /// 返回: 候选配置只读引用，运行时不得修改其中字段。
    pub fn candidate(&self) -> &LoadedConfig {
        &self.candidate
    }

    /// 业务作用: 返回候选版本相对当前活动版本的确定性差异。
    ///
    /// 返回: 代理项和进程级配置差异只读引用。
    pub fn diff(&self) -> &ConfigDiff {
        &self.diff
    }

    /// 业务作用: 返回本次请求只预检还是需要发布。
    ///
    /// 返回: 运行时事务执行模式。
    pub fn mode(&self) -> ApplyMode {
        self.mode
    }

    /// 业务作用: 返回候选配置来自本地来源还是集群提交协议。
    ///
    /// 返回: 运行时用来禁止集群节点绕过 gRPC 一致性事务的来源标识。
    pub fn origin(&self) -> ApplyOrigin {
        self.origin
    }

    /// 业务作用: 完成运行时应用请求并把结果返回配置 supervisor。
    ///
    /// 参数说明:
    /// - `result`: 预检或发布结果；错误文本必须已经完成凭据脱敏。
    pub fn complete(self, result: Result<(), String>) {
        let _ = self.response.send(result);
    }
}

/// 管理接口返回的一次配置校验或发布结果。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReloadOperationResponse {
    /// 请求完成后的配置热更新状态。
    pub status: ReloadSnapshot,
}

/// 管理节点准备交给协调领导者的规范化集群配置提案。
pub(crate) enum ClusterProposalCandidate {
    /// 有效配置与当前活动版本一致，不需要进入协调日志。
    NoOp(ReloadOperationResponse),
    /// 需要由领导节点广播、落盘、提交和发布的新配置。
    Candidate {
        /// 候选旧版本和摘要组成的精确事务身份，用于隔离并发提案的失败归因。
        transaction: ReloadTransactionId,
        /// 本地活动运行代所对应的协调配置版本，用作领导节点提交前的 CAS 基准。
        expected_revision: u64,
        /// 完整集群配置事务允许持续的最长时间，单位毫秒。
        transaction_timeout_ms: u64,
        /// 正文不变但代理 TLS 依赖轮换时要求全部成员重读本地文件并推进版本。
        force_revision: bool,
        /// 全部成员必须确认的规范化载荷摘要。
        digest: String,
        /// 仍可能包含业务凭据、不得记录正文的规范化 JSON 字节。
        payload: Vec<u8>,
        /// 持有本次外部热更新的进程级串行租约，直到协调事务返回或调用方放弃候选。
        transaction_lease: ReloadTransactionLease,
    },
}

/// 外部配置事务的进程级非阻塞串行租约。
pub(crate) struct ReloadTransactionLease {
    busy: Arc<AtomicBool>,
}

impl Drop for ReloadTransactionLease {
    /// 业务作用: 无论请求成功、失败还是调用方中途退出，都释放下一次外部配置事务入口。
    fn drop(&mut self) {
        self.busy.store(false, Ordering::Release);
    }
}

/// 管理端和信号任务共享的配置热更新请求句柄。
#[derive(Clone)]
pub struct ReloadHandle {
    requests: mpsc::Sender<SupervisorRequest>,
    cluster_candidates: mpsc::Sender<ClusterCandidateRequest>,
    state: ReloadState,
    coordinator: Arc<StdRwLock<Option<Arc<CoordinatorHandle>>>>,
    transaction_busy: Arc<AtomicBool>,
}

impl ReloadHandle {
    /// 业务作用: 只加载、校验和预检当前配置来源，不发布新运行代。
    ///
    /// 返回: 请求完成后的状态；supervisor 已停止时返回错误。
    pub async fn validate(&self) -> Result<ReloadOperationResponse, String> {
        let _transaction = try_acquire_reload_transaction(&self.transaction_busy)?;
        self.request(SupervisorOperation::Validate).await
    }

    /// 业务作用: 立即加载当前配置来源并执行完整热更新事务。
    ///
    /// 返回: 请求完成后的状态；加载、预检或发布失败时返回错误。
    pub async fn reload(&self) -> Result<ReloadOperationResponse, String> {
        let coordinator = self
            .coordinator
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(coordinator) = coordinator {
            if !coordinator.cluster_configuration_proposals_ready() {
                return Err(
                    "cluster configuration startup synchronization is not complete".to_string(),
                );
            }
            return match self.cluster_candidate().await? {
                ClusterProposalCandidate::NoOp(response) => Ok(response),
                ClusterProposalCandidate::Candidate {
                    transaction,
                    expected_revision,
                    transaction_timeout_ms,
                    force_revision,
                    digest,
                    payload,
                    transaction_lease: _transaction_lease,
                } => {
                    self.state
                        .register_cluster_transaction(
                            transaction.clone(),
                            ReloadTrigger::AdminReload,
                        )
                        .await;
                    let response = match coordinator
                        .propose_cluster_config(
                            expected_revision,
                            Duration::from_millis(transaction_timeout_ms),
                            force_revision,
                            digest,
                            payload,
                        )
                        .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            let error = format!("{error:#}");
                            self.state
                                .finish_cluster_transaction_failure(
                                    ReloadTrigger::AdminReload,
                                    &transaction,
                                    error.clone(),
                                )
                                .await;
                            return Err(error);
                        }
                    };
                    self.state
                        .set_pending_members(response.pending_members)
                        .await;
                    self.state
                        .finish_cluster_transaction_success(&transaction)
                        .await;
                    Ok(ReloadOperationResponse {
                        status: self.snapshot().await,
                    })
                }
            };
        }
        let _transaction = try_acquire_reload_transaction(&self.transaction_busy)?;
        self.request(SupervisorOperation::Reload).await
    }

    /// 业务作用: 记录最近一次集群提交响应中尚未完成发布的静态成员数量。
    ///
    /// 参数说明:
    /// - `pending_members`: 已提交目标 revision、但响应时尚未确认活动版本发布的成员数量。
    pub async fn set_pending_members(&self, pending_members: u32) {
        self.state.set_pending_members(pending_members).await;
    }

    /// 业务作用: 安装进程级协调器，使文件、信号和显式请求统一进入多数派配置事务。
    ///
    /// 参数说明:
    /// - `coordinator`: 当前节点共享的 gRPC 和复制协议协调句柄。
    pub fn install_coordinator(&self, coordinator: Arc<CoordinatorHandle>) {
        let mut current = self
            .coordinator
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = Some(coordinator);
    }

    /// 业务作用: 使用集群领导节点广播的规范化载荷执行本地预检或发布。
    ///
    /// 参数说明:
    /// - `payload`: 已通过协调 gRPC 身份和摘要校验的规范化业务配置。
    /// - `mode`: prepare 阶段只预检，activate 阶段完整发布。
    /// - `revision`: 领导节点为候选分配、提交后保持不变的配置版本。
    /// - `digest`: 全部阶段共同使用的规范化配置摘要。
    ///
    /// 返回: 本节点完成对应阶段后的状态；载荷或运行时事务失败时返回错误。
    pub(crate) async fn apply_cluster_payload(
        &self,
        payload: Vec<u8>,
        mode: ApplyMode,
        revision: u64,
        digest: String,
    ) -> Result<ReloadOperationResponse, String> {
        let expected_revision = revision
            .checked_sub(1)
            .ok_or_else(|| "cluster candidate revision must be greater than zero".to_string())?;
        let transaction = ReloadTransactionId::new(expected_revision, digest.clone());
        let operation = match mode {
            ApplyMode::HaPrecommit => SupervisorOperation::ClusterPrecommit {
                payload,
                transaction,
            },
            ApplyMode::ValidateOnly => SupervisorOperation::ClusterValidate {
                payload,
                transaction,
            },
            ApplyMode::Publish => SupervisorOperation::ClusterReload {
                payload,
                transaction,
                revision,
                digest,
            },
        };
        self.request(operation).await
    }

    /// 业务作用: 加载本地候选配置并生成交给协调领导者的规范化载荷。
    ///
    /// 返回: 无变化时返回 `NoOp`；有变化时返回摘要和不含节点本地身份字段的正文。
    pub(crate) async fn cluster_candidate(&self) -> Result<ClusterProposalCandidate, String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.cluster_candidates
            .send(ClusterCandidateRequest {
                response: response_tx,
            })
            .await
            .map_err(|_| "configuration supervisor is not running".to_string())?;
        response_rx.await.map_err(|_| {
            "configuration supervisor stopped before preparing cluster proposal".to_string()
        })?
    }

    /// 业务作用: 记录集群领导者在 prepare、提交或 activate 阶段返回的失败。
    ///
    /// 参数说明:
    /// - `transaction`: 失败提案的精确旧版本和摘要身份。
    /// - `error`: 不包含配置正文和凭据的集群事务错误。
    pub(crate) async fn finish_cluster_failure(
        &self,
        transaction: &ReloadTransactionId,
        error: String,
    ) {
        self.state
            .finish_cluster_transaction_failure(ReloadTrigger::AdminReload, transaction, error)
            .await;
    }

    /// 业务作用: 登记管理端即将交给协调器的外层集群提案。
    ///
    /// 参数说明:
    /// - `transaction`: 候选旧版本和摘要组成的精确事务身份。
    pub(crate) async fn register_cluster_proposal(&self, transaction: ReloadTransactionId) {
        self.state
            .register_cluster_transaction(transaction, ReloadTrigger::AdminReload)
            .await;
    }

    /// 业务作用: 清理已经成功收口的管理端集群提案归因状态。
    ///
    /// 参数说明:
    /// - `transaction`: 成功提案的精确旧版本和摘要身份。
    pub(crate) async fn finish_cluster_proposal_success(&self, transaction: &ReloadTransactionId) {
        self.state
            .finish_cluster_transaction_success(transaction)
            .await;
    }

    /// 业务作用: 返回当前配置版本和最近一次事务状态。
    ///
    /// 返回: 不持有 supervisor 内部锁的独立状态快照。
    pub async fn snapshot(&self) -> ReloadSnapshot {
        self.state.snapshot().await
    }

    /// 业务作用: 将管理请求送入串行 supervisor 并等待对应结果。
    ///
    /// 参数说明:
    /// - `operation`: 本次请求只校验还是完整发布。
    ///
    /// 返回: 请求完成后的状态或 supervisor 通道错误。
    async fn request(
        &self,
        operation: SupervisorOperation,
    ) -> Result<ReloadOperationResponse, String> {
        let (response_tx, response_rx) = oneshot::channel();
        self.requests
            .send(SupervisorRequest {
                operation,
                response: response_tx,
            })
            .await
            .map_err(|_| "configuration supervisor is not running".to_string())?;
        response_rx
            .await
            .map_err(|_| "configuration supervisor stopped before responding".to_string())?
    }
}

/// 配置 supervisor 支持的显式请求类型。
#[derive(Debug)]
enum SupervisorOperation {
    /// 只执行加载、静态校验和运行时预检。
    Validate,
    /// 执行加载、预检、发布和排空事务。
    Reload,
    /// 仅在领导节点执行 HA 节点集合变化所需的安全拓扑动作。
    ClusterPrecommit {
        /// 已验证摘要、即将执行 HA 提交前动作的规范化载荷。
        payload: Vec<u8>,
        /// 候选旧版本和摘要组成的精确事务身份。
        transaction: ReloadTransactionId,
    },
    /// 校验领导节点广播的候选配置，不读取本地业务配置覆盖。
    ClusterValidate {
        /// 已验证摘要、只执行本地预检的规范化载荷。
        payload: Vec<u8>,
        /// 候选旧版本和摘要组成的精确事务身份。
        transaction: ReloadTransactionId,
    },
    /// 发布已经由集群协调日志提交的配置载荷。
    ClusterReload {
        /// 已完成摘要校验的规范化集群业务配置。
        payload: Vec<u8>,
        /// 候选旧版本和摘要组成的精确事务身份。
        transaction: ReloadTransactionId,
        /// 协调日志已经提交的精确配置版本。
        revision: u64,
        /// 协调日志已经提交的规范化配置摘要。
        digest: String,
    },
}

/// 一个等待串行处理的显式配置请求。
struct SupervisorRequest {
    operation: SupervisorOperation,
    response: oneshot::Sender<Result<ReloadOperationResponse, String>>,
}

/// 等待配置 supervisor 生成集群规范化候选载荷的请求。
struct ClusterCandidateRequest {
    response: oneshot::Sender<Result<ClusterProposalCandidate, String>>,
}

/// 文件或信号异步发起的集群配置提案完成事件。
struct ClusterProposalFinished {
    /// 本地配置来源的完整摘要，用于失败后抑制重复自动尝试。
    source_digest: String,
    /// 发起本次外层提案的文件或信号来源，用于失败指标归因。
    trigger: ReloadTrigger,
    /// 候选旧版本和摘要组成的精确事务身份。
    transaction: ReloadTransactionId,
    /// 多数派 prepare、提交和在线成员 activate 的最终结果及待收敛成员数。
    result: Result<u32, String>,
}

/// 启动并持有配置来源检查、SIGHUP 和管理请求串行处理循环。
pub struct ConfigSupervisor;

impl ConfigSupervisor {
    /// 业务作用: 启动配置热更新 supervisor。
    ///
    /// 参数说明:
    /// - `source`: 可重复加载的本地配置来源。
    /// - `initial`: 启动时已经验证并用于创建首代运行时的配置。
    /// - `apply_tx`: 向运行时管理器发送预检或发布事务的通道。
    /// - `shutdown`: 进程级关闭信号。
    ///
    /// 返回: 管理请求句柄和 supervisor 后台任务；SIGHUP handler 创建失败时返回错误。
    pub fn spawn(
        source: ConfigSource,
        initial: LoadedConfig,
        apply_tx: mpsc::Sender<ApplyConfigRequest>,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<(ReloadHandle, JoinHandle<anyhow::Result<()>>)> {
        let initial_revision = if initial.config().coordination.enabled {
            0
        } else {
            1
        };
        let state = ReloadState::new(initial.digest(), initial_revision);
        let (request_tx, request_rx) = mpsc::channel(16);
        let (cluster_candidate_tx, cluster_candidate_rx) = mpsc::channel(2);
        let coordinator = Arc::new(StdRwLock::new(None));
        let transaction_busy = Arc::new(AtomicBool::new(false));
        let handle = ReloadHandle {
            requests: request_tx,
            cluster_candidates: cluster_candidate_tx,
            state: state.clone(),
            coordinator: coordinator.clone(),
            transaction_busy: transaction_busy.clone(),
        };
        let signal = hangup_signal()?;
        let task = tokio::spawn(run_supervisor(
            source,
            initial,
            apply_tx,
            request_rx,
            cluster_candidate_rx,
            coordinator,
            transaction_busy,
            state,
            shutdown,
            signal,
        ));
        Ok((handle, task))
    }
}

/// 业务作用: 运行文件检查、信号和管理请求的单线程事务循环。
///
/// 参数说明:
/// - `source`: 可重复加载的本地配置来源。
/// - `current`: 当前活动配置版本。
/// - `apply_tx`: 运行时预检和发布请求发送通道。
/// - `request_rx`: 管理端显式请求接收通道。
/// - `cluster_candidate_rx`: 集群提案载荷生成请求接收通道。
/// - `coordinator`: 协调器安装后，文件和信号变化由其执行多数派提交和成员收敛事务。
/// - `transaction_busy`: 外部管理、文件和信号触发共用的非阻塞串行门禁。
/// - `state`: 管理状态和累计指标。
/// - `shutdown`: 进程级关闭信号。
/// - `signal`: Unix SIGHUP 接收器。
///
/// 返回: 收到关闭信号或全部请求发送端释放时成功退出；周期加载任务异常退出时返回失败。
#[allow(clippy::too_many_arguments)]
async fn run_supervisor(
    source: ConfigSource,
    mut current: LoadedConfig,
    apply_tx: mpsc::Sender<ApplyConfigRequest>,
    mut request_rx: mpsc::Receiver<SupervisorRequest>,
    mut cluster_candidate_rx: mpsc::Receiver<ClusterCandidateRequest>,
    coordinator: Arc<StdRwLock<Option<Arc<CoordinatorHandle>>>>,
    transaction_busy: Arc<AtomicBool>,
    state: ReloadState,
    mut shutdown: watch::Receiver<bool>,
    mut signal: tokio::signal::unix::Signal,
) -> anyhow::Result<()> {
    let mut poll = reload_interval(&current)?;
    let mut pending_digest: Option<String> = None;
    let mut stable_observations = 0_u32;
    let mut last_load_error: Option<String> = None;
    let mut failed_candidate: Option<FailedCandidateRetry> = None;
    // 节点从集群权威版本恢复后，本地磁盘可能仍保留旧启动文件。该摘要必须与普通失败
    // 重试分开抑制，防止恢复节点把旧文件重新提案并污染新集群配置。
    let mut suppressed_local_digest: Option<String> = None;
    let (proposal_result_tx, mut proposal_result_rx) = mpsc::channel(2);
    let mut cluster_proposal_running = false;
    let proposal_tasks = TaskScope::default();

    loop {
        tokio::select! {
            _ = poll.tick(), if current.config().reload.enabled && !cluster_proposal_running => {
                // 周期加载只构造候选，不持有发布权威；使用阻塞池避免移交正在处理 RESP 的工作线程。
                // 逐次等待本轮结果，保持来源观察和候选发布的串行顺序。
                let poll_source = source.clone();
                let loaded = poll.read(move || poll_source.load())
                    .await
                    .context("configuration polling task failed")?;
                // 文件读取期间可能收到停机；尚未发布的候选必须丢弃，不能在停机后开始新配置事务。
                if shutdown.has_changed().is_err() || *shutdown.borrow() {
                    break;
                }
                match loaded {
                    Ok(candidate) if candidate.digest() == current.digest() => {
                        pending_digest = None;
                        stable_observations = 0;
                        last_load_error = None;
                        failed_candidate = None;
                        suppressed_local_digest = None;
                    }
                    Ok(candidate)
                        if suppressed_local_digest.as_deref() == Some(candidate.digest()) =>
                    {
                        pending_digest = None;
                        stable_observations = 0;
                        last_load_error = None;
                    }
                    Ok(candidate)
                        if failed_candidate
                            .as_ref()
                            .is_some_and(|failed| failed.defers(candidate.digest())) =>
                    {
                        pending_digest = None;
                        stable_observations = 0;
                        last_load_error = None;
                    }
                    Ok(candidate) => {
                        last_load_error = None;
                        if suppressed_local_digest.as_deref() != Some(candidate.digest()) {
                            suppressed_local_digest = None;
                        }
                        if failed_candidate
                            .as_ref()
                            .is_some_and(|failed| failed.digest != candidate.digest())
                        {
                            failed_candidate = None;
                        }
                        if pending_digest.as_deref() == Some(candidate.digest()) {
                            stable_observations = stable_observations.saturating_add(1);
                        } else {
                            pending_digest = Some(candidate.digest().to_string());
                            stable_observations = 1;
                        }
                        if stable_observations >= current.config().reload.stable_observations {
                            let candidate_digest = candidate.digest().to_string();
                            let result = match installed_coordinator(&coordinator) {
                                Some(coordinator) => {
                                    schedule_cluster_proposal(
                                        &current,
                                        candidate,
                                        ReloadTrigger::File,
                                        coordinator,
                                        proposal_result_tx.clone(),
                                        &transaction_busy,
                                        &state,
                                        proposal_tasks.spawner(),
                                    )
                                    .await
                                    .map(|scheduled| {
                                        cluster_proposal_running = scheduled;
                                    })
                                }
                                None => execute_candidate(
                                    &mut current,
                                    candidate,
                                    ApplyMode::Publish,
                                    ReloadTrigger::File,
                                    &apply_tx,
                                    &state,
                                ).await.map(|_| ()),
                            };
                            if let Err(error) = result {
                                warn!(error = %error, "automatic configuration reload failed");
                                failed_candidate = Some(FailedCandidateRetry::after_failure(
                                    failed_candidate.as_ref(),
                                    candidate_digest,
                                ));
                            } else {
                                failed_candidate = None;
                            }
                            pending_digest = None;
                            stable_observations = 0;
                            poll = reload_interval(&current)?;
                        }
                    }
                    Err(error) => {
                        let message = format!("{error:#}");
                        if last_load_error.as_deref() != Some(message.as_str()) {
                            state.begin(ReloadTrigger::File).await;
                            state
                                .finish_failure(ReloadTrigger::File, None, message.clone())
                                .await;
                            warn!(error = %message, "configuration source check failed");
                            last_load_error = Some(message);
                        }
                        pending_digest = None;
                        stable_observations = 0;
                    }
                }
            }
            Some(request) = request_rx.recv() => {
                let result = match request.operation {
                    SupervisorOperation::Validate => execute_from_source(
                        &source,
                        &mut current,
                        ApplyMode::ValidateOnly,
                        ReloadTrigger::AdminValidate,
                        &apply_tx,
                        &state,
                        &shutdown,
                    ).await,
                    SupervisorOperation::Reload => execute_from_source(
                        &source,
                        &mut current,
                        ApplyMode::Publish,
                        ReloadTrigger::AdminReload,
                        &apply_tx,
                        &state,
                        &shutdown,
                    ).await,
                    SupervisorOperation::ClusterValidate {
                        payload,
                        transaction,
                    } => {
                        execute_cluster_payload(
                            &source,
                            &mut current,
                            payload,
                            ApplyMode::ValidateOnly,
                            ReloadTrigger::ClusterPrepare,
                            transaction,
                            None,
                            &apply_tx,
                            &state,
                            &shutdown,
                        ).await
                    }
                    SupervisorOperation::ClusterPrecommit {
                        payload,
                        transaction,
                    } => {
                        execute_cluster_payload(
                            &source,
                            &mut current,
                            payload,
                            ApplyMode::HaPrecommit,
                            ReloadTrigger::ClusterPrecommit,
                            transaction,
                            None,
                            &apply_tx,
                            &state,
                            &shutdown,
                        ).await
                    }
                    SupervisorOperation::ClusterReload {
                        payload,
                        transaction,
                        revision,
                        digest,
                    } => {
                        let result = execute_cluster_payload(
                            &source,
                            &mut current,
                            payload,
                            ApplyMode::Publish,
                            ReloadTrigger::ClusterActivate,
                            transaction,
                            Some(ClusterCommit { revision, digest }),
                            &apply_tx,
                            &state,
                            &shutdown,
                        ).await;
                        if result.is_ok() {
                            // 已提交版本先保持有效；磁盘补读只决定旧来源抑制，不再占用业务工作线程。
                            suppressed_local_digest = read_reload_candidate(&source, None, &shutdown)
                                .await
                                .ok()
                                .filter(|local| local.digest() != current.digest())
                                .map(|local| local.digest().to_string());
                            failed_candidate = None;
                        }
                        result
                    }
                };
                let _ = request.response.send(result);
                poll = reload_interval(&current)?;
            }
            Some(finished) = proposal_result_rx.recv(), if cluster_proposal_running => {
                cluster_proposal_running = false;
                match finished.result {
                    Ok(pending_members) => {
                        failed_candidate = None;
                        state.set_pending_members(pending_members).await;
                        state
                            .finish_cluster_transaction_success(&finished.transaction)
                            .await;
                        info!(
                            digest = %digest_prefix(&finished.source_digest),
                            pending_members,
                            "cluster configuration proposal completed"
                        );
                    }
                    Err(error) => {
                        failed_candidate = Some(FailedCandidateRetry::after_failure(
                            failed_candidate.as_ref(),
                            finished.source_digest,
                        ));
                        state
                            .finish_cluster_transaction_failure(
                                finished.trigger,
                                &finished.transaction,
                                error.clone(),
                            )
                            .await;
                        warn!(error = %error, "cluster configuration proposal failed");
                    }
                }
                poll = reload_interval(&current)?;
            }
            Some(request) = cluster_candidate_rx.recv() => {
                let result =
                    prepare_cluster_candidate(
                        &source,
                        &current,
                        &transaction_busy,
                        &state,
                        &shutdown,
                    ).await;
                let _ = request.response.send(result);
            }
            _ = signal.recv(), if current.config().reload.enabled && !cluster_proposal_running => {
                let result = match installed_coordinator(&coordinator) {
                    Some(coordinator) => match read_reload_candidate(&source, None, &shutdown).await {
                        Ok(candidate) => schedule_cluster_proposal(
                            &current,
                            candidate,
                            ReloadTrigger::Signal,
                            coordinator,
                            proposal_result_tx.clone(),
                            &transaction_busy,
                            &state,
                            proposal_tasks.spawner(),
                        )
                        .await
                        .map(|scheduled| {
                            cluster_proposal_running = scheduled;
                        }),
                        Err(error) => {
                            let error = format!("{error:#}");
                            state.begin(ReloadTrigger::Signal).await;
                            state
                                .finish_failure(ReloadTrigger::Signal, None, error.clone())
                                .await;
                            Err(error)
                        }
                    },
                    None => execute_from_source(
                        &source,
                        &mut current,
                        ApplyMode::Publish,
                        ReloadTrigger::Signal,
                        &apply_tx,
                        &state,
                        &shutdown,
                    )
                    .await
                    .map(|_| ()),
                };
                if let Err(error) = result {
                    warn!(error = %error, "SIGHUP configuration reload failed");
                }
                poll = reload_interval(&current)?;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            else => break,
        }
    }
    // 停止后拒绝尚未消费及后来到达的配置阶段，避免提案收尾等待本节点已经不再消费的队列。
    drop(request_rx);
    drop(cluster_candidate_rx);
    drop(proposal_result_rx);
    // 提案已进入跨成员事务后不能由文件监听任务的结束直接撤销，收尾继续消费进程统一预算。
    proposal_tasks.finish().await
}

/// 业务作用: 从共享槽位读取当前已经安装的进程级协调器。
///
/// 参数说明:
/// - `coordinator`: 主进程在协调器启动后写入的共享槽位。
///
/// 返回: 集群部署时返回共享协调器，单节点模式或尚未安装时返回空。
fn installed_coordinator(
    coordinator: &Arc<StdRwLock<Option<Arc<CoordinatorHandle>>>>,
) -> Option<Arc<CoordinatorHandle>> {
    coordinator
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// 业务作用: 将文件或信号候选交给独立任务发起集群配置事务，保持 supervisor 可受理回调阶段。
///
/// 参数说明:
/// - `current`: 当前活动配置，用于计算差异和识别无操作。
/// - `candidate`: 从本节点配置来源加载并完成静态校验的候选。
/// - `trigger`: 文件稳定变化或 SIGHUP 触发入口。
/// - `coordinator`: 向当前领导者转发提案的进程级协调器。
/// - `result_tx`: 把事务终态送回串行 supervisor 的通道。
/// - `transaction_busy`: 与管理端请求共享的进程级外部事务门禁。
/// - `state`: 管理状态和累计指标。
/// - `tasks`: 配置 supervisor 持有的事务任务入口，跨请求收尾仍由原父任务等待。
///
/// 返回: 已启动异步提案时返回真；无操作返回假；本地身份字段变化时返回错误。
#[expect(
    clippy::too_many_arguments,
    reason = "候选、协调器、事务门禁和任务入口分别保留所有权，提案脱离请求后仍由 supervisor 收尾"
)]
async fn schedule_cluster_proposal(
    current: &LoadedConfig,
    candidate: LoadedConfig,
    trigger: ReloadTrigger,
    coordinator: Arc<CoordinatorHandle>,
    result_tx: mpsc::Sender<ClusterProposalFinished>,
    transaction_busy: &Arc<AtomicBool>,
    state: &ReloadState,
    tasks: TaskSpawner,
) -> Result<bool, String> {
    if !coordinator.cluster_configuration_proposals_ready() {
        return Ok(false);
    }
    let transaction_lease = match try_acquire_reload_transaction(transaction_busy) {
        Ok(lease) => lease,
        Err(_) => return Ok(false),
    };
    state.begin(trigger).await;
    let diff = ConfigDiff::between(current, &candidate);
    state.set_diff(diff.clone()).await;
    if candidate.digest() == current.digest() || diff.is_empty() {
        state.finish_noop().await;
        return Ok(false);
    }
    let force_revision = candidate.cluster_digest() == current.cluster_digest();
    if !current.local_bootstrap_compatible_with(&candidate) {
        let error =
            "node-local coordination or admin identity cannot be changed by cluster hot reload"
                .to_string();
        state.finish_failure(trigger, None, error.clone()).await;
        return Err(error);
    }
    state.set_phase(ReloadPhase::HaPrecommit).await;
    let source_digest = candidate.digest().to_string();
    let cluster_digest = candidate.cluster_digest().to_string();
    let payload = candidate.cluster_payload().to_vec();
    let expected_revision = state.snapshot().await.config_revision;
    let transaction = ReloadTransactionId::new(expected_revision, cluster_digest.clone());
    state
        .register_cluster_transaction(transaction.clone(), trigger)
        .await;
    let transaction_timeout =
        Duration::from_millis(candidate.config().reload.transaction_timeout_ms);
    tasks
        .spawn(async move {
            let _transaction_lease = transaction_lease;
            let result = AssertUnwindSafe(coordinator.propose_cluster_config(
                expected_revision,
                transaction_timeout,
                force_revision,
                cluster_digest,
                payload,
            ))
            .catch_unwind()
            .await;
            let result = match result {
                Ok(Ok(response)) => Ok(response.pending_members),
                Ok(Err(error)) => Err(format!("{error:#}")),
                Err(_) => Err("cluster configuration proposal task panicked".to_string()),
            };
            let _ = result_tx
                .send(ClusterProposalFinished {
                    source_digest,
                    trigger,
                    transaction,
                    result,
                })
                .await;
        })
        .map_err(|_| "configuration proposal task scope is closed".to_string())?;
    Ok(true)
}

/// 业务作用: 通过共享阻塞池读取显式热更候选，读取期间不占用业务调度线程且不持有发布权限。
/// 参数说明:
/// - `source`: 当前节点的本地来源及引导身份。
/// - `payload`: 集群阶段已收到的完整载荷；为空时加载本地完整配置。
/// - `shutdown`: 当前 supervisor 的关闭权威，读取前后都必须保持有效。
///
/// 返回: 加载与静态校验成功且尚未停机时交还候选；读取、任务或关闭错误均不允许开始发布。
async fn read_reload_candidate(
    source: &ConfigSource,
    payload: Option<Vec<u8>>,
    shutdown: &watch::Receiver<bool>,
) -> anyhow::Result<LoadedConfig> {
    // 已经进入关闭流程时不再开始磁盘读取，避免关闭期间新增候选及无用的阻塞任务。
    if shutdown.has_changed().is_err() || *shutdown.borrow() {
        anyhow::bail!("configuration supervisor is stopping");
    }
    let source = source.clone();
    let candidate = nasa::application::read_config_candidate(move || match payload {
        Some(payload) => source.load_cluster_payload(&payload),
        None => source.load(),
    })
    .await
    .context("configuration candidate reader failed")??;
    // 同步读取一旦开始不能强制取消；闭包只构造候选，必须回到持有权威的调用方复验后才能发布。
    if shutdown.has_changed().is_err() || *shutdown.borrow() {
        anyhow::bail!("configuration supervisor stopped while reading candidate");
    }
    Ok(candidate)
}

/// 业务作用: 异步加载本地配置来源并生成一次规范化集群提案，停机后的候选不得发起协调事务。
///
/// 参数说明:
/// - `source`: 当前节点可重复读取的配置来源。
/// - `current`: 当前活动配置，用于计算无操作和差异。
/// - `transaction_busy`: 与文件、信号和其他管理请求共享的外部事务门禁。
/// - `state`: 管理状态和累计指标。
/// - `shutdown`: 配置来源读取前后需要复验的进程关闭权威。
///
/// 返回: 无变化状态或需要交给协调领导者的摘要及载荷。
async fn prepare_cluster_candidate(
    source: &ConfigSource,
    current: &LoadedConfig,
    transaction_busy: &Arc<AtomicBool>,
    state: &ReloadState,
    shutdown: &watch::Receiver<bool>,
) -> Result<ClusterProposalCandidate, String> {
    let transaction_lease = try_acquire_reload_transaction(transaction_busy)?;
    state.begin(ReloadTrigger::AdminReload).await;
    if !current.config().reload.enabled {
        let error = "configuration reload is disabled".to_string();
        state
            .finish_failure(ReloadTrigger::AdminReload, None, error.clone())
            .await;
        return Err(error);
    }
    let candidate = match read_reload_candidate(source, None, shutdown).await {
        Ok(candidate) => candidate,
        Err(error) => {
            let error = format!("{error:#}");
            state
                .finish_failure(ReloadTrigger::AdminReload, None, error.clone())
                .await;
            return Err(error);
        }
    };
    let diff = ConfigDiff::between(current, &candidate);
    state.set_diff(diff.clone()).await;
    if candidate.digest() == current.digest() || diff.is_empty() {
        state.finish_noop().await;
        return Ok(ClusterProposalCandidate::NoOp(ReloadOperationResponse {
            status: state.snapshot().await,
        }));
    }
    let force_revision = candidate.cluster_digest() == current.cluster_digest();
    if !current.local_bootstrap_compatible_with(&candidate) {
        let error =
            "node-local coordination or admin identity cannot be changed by cluster hot reload"
                .to_string();
        state
            .finish_failure(ReloadTrigger::AdminReload, None, error.clone())
            .await;
        return Err(error);
    }
    state.set_phase(ReloadPhase::HaPrecommit).await;
    let expected_revision = state.snapshot().await.config_revision;
    let digest = candidate.cluster_digest().to_string();
    let transaction = ReloadTransactionId::new(expected_revision, digest.clone());
    Ok(ClusterProposalCandidate::Candidate {
        transaction,
        expected_revision,
        transaction_timeout_ms: candidate.config().reload.transaction_timeout_ms,
        force_revision,
        digest,
        payload: candidate.cluster_payload().to_vec(),
        transaction_lease,
    })
}

/// 业务作用: 非阻塞地取得一次外部配置事务的唯一进程级租约。
///
/// 参数说明:
/// - `busy`: 全部管理、文件和信号入口共享的原子占用标记。
///
/// 返回: 空闲时返回自动释放的租约；已有事务时立即返回忙错误，不覆盖活动事务状态。
fn try_acquire_reload_transaction(
    busy: &Arc<AtomicBool>,
) -> Result<ReloadTransactionLease, String> {
    busy.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| "another configuration transaction is already in progress".to_string())?;
    Ok(ReloadTransactionLease { busy: busy.clone() })
}

/// 业务作用: 异步加载领导节点载荷及本地引导来源，在关闭权威有效时执行本地集群配置阶段。
///
/// 参数说明:
/// - `source`: 提供本节点协调身份字段的本地引导配置来源。
/// - `current`: 当前活动配置，发布成功后原地替换。
/// - `payload`: 不包含节点本地身份字段的规范化集群业务配置。
/// - `mode`: 只预检还是发布已提交版本。
/// - `trigger`: prepare 或 activate 集群触发阶段。
/// - `transaction`: 候选旧版本和摘要组成的精确事务身份。
/// - `cluster_commit`: activate 阶段由协调日志确认的精确版本和摘要。
/// - `apply_tx`: 运行时事务发送通道。
/// - `state`: 管理状态和累计指标。
/// - `shutdown`: 配置来源读取前后需要复验的进程关闭权威。
///
/// 返回: 本节点完成预检或发布后的状态；载荷解析、校验或运行时事务失败时返回错误。
#[allow(clippy::too_many_arguments)]
async fn execute_cluster_payload(
    source: &ConfigSource,
    current: &mut LoadedConfig,
    payload: Vec<u8>,
    mode: ApplyMode,
    trigger: ReloadTrigger,
    transaction: ReloadTransactionId,
    cluster_commit: Option<ClusterCommit>,
    apply_tx: &mpsc::Sender<ApplyConfigRequest>,
    state: &ReloadState,
    shutdown: &watch::Receiver<bool>,
) -> Result<ReloadOperationResponse, String> {
    state.begin(trigger).await;
    let candidate = match read_reload_candidate(source, Some(payload), shutdown).await {
        Ok(candidate) => candidate,
        Err(error) => {
            let error = format!("{error:#}");
            state
                .finish_failure(trigger, Some(&transaction), error.clone())
                .await;
            return Err(error);
        }
    };
    execute_loaded_candidate(
        current,
        candidate,
        mode,
        CandidateExecutionContext {
            origin: ApplyOrigin::Cluster,
            cluster_commit,
            failure_trigger: trigger,
            failure_transaction: Some(&transaction),
        },
        apply_tx,
        state,
    )
    .await
}

/// 业务作用: 异步读取本地候选并执行校验或发布，停机后不再把读取结果交给运行时。
///
/// 参数说明:
/// - `source`: 本次请求要读取的配置来源。
/// - `current`: 当前活动配置，发布成功后原地替换。
/// - `mode`: 只预检还是完整发布。
/// - `trigger`: 本次请求的触发入口。
/// - `apply_tx`: 运行时事务发送通道。
/// - `state`: 管理状态和累计指标。
/// - `shutdown`: 配置来源读取前后需要复验的进程关闭权威。
///
/// 返回: 请求完成后的状态；任一阶段失败时返回脱敏错误。
async fn execute_from_source(
    source: &ConfigSource,
    current: &mut LoadedConfig,
    mode: ApplyMode,
    trigger: ReloadTrigger,
    apply_tx: &mpsc::Sender<ApplyConfigRequest>,
    state: &ReloadState,
    shutdown: &watch::Receiver<bool>,
) -> Result<ReloadOperationResponse, String> {
    state.begin(trigger).await;
    if !current.config().reload.enabled {
        let error = "configuration reload is disabled".to_string();
        state.finish_failure(trigger, None, error.clone()).await;
        return Err(error);
    }
    let candidate = match read_reload_candidate(source, None, shutdown).await {
        Ok(candidate) => candidate,
        Err(error) => {
            let error = format!("{error:#}");
            state.finish_failure(trigger, None, error.clone()).await;
            return Err(error);
        }
    };
    execute_loaded_candidate(
        current,
        candidate,
        mode,
        CandidateExecutionContext {
            origin: ApplyOrigin::Local,
            cluster_commit: None,
            failure_trigger: trigger,
            failure_transaction: None,
        },
        apply_tx,
        state,
    )
    .await
}

/// 业务作用: 执行已经稳定观察到的自动文件候选配置。
///
/// 参数说明:
/// - `current`: 当前活动配置，发布成功后原地替换。
/// - `candidate`: 已加载并静态校验的候选配置。
/// - `mode`: 只预检还是完整发布。
/// - `trigger`: 本次候选的触发入口。
/// - `apply_tx`: 运行时事务发送通道。
/// - `state`: 管理状态和累计指标。
///
/// 返回: 请求完成后的状态；任一阶段失败时返回脱敏错误。
async fn execute_candidate(
    current: &mut LoadedConfig,
    candidate: LoadedConfig,
    mode: ApplyMode,
    trigger: ReloadTrigger,
    apply_tx: &mpsc::Sender<ApplyConfigRequest>,
    state: &ReloadState,
) -> Result<ReloadOperationResponse, String> {
    state.begin(trigger).await;
    execute_loaded_candidate(
        current,
        candidate,
        mode,
        CandidateExecutionContext {
            origin: ApplyOrigin::Local,
            cluster_commit: None,
            failure_trigger: trigger,
            failure_transaction: None,
        },
        apply_tx,
        state,
    )
    .await
}

/// 业务作用: 比较候选版本、调用运行时事务并在发布成功后更新活动版本。
///
/// 参数说明:
/// - `current`: 当前活动配置，发布成功后原地替换。
/// - `candidate`: 已加载并静态校验的候选配置。
/// - `mode`: 只预检还是完整发布。
/// - `execution`: 本地或集群来源、提交版本和失败归因上下文。
/// - `apply_tx`: 运行时事务发送通道。
/// - `state`: 管理状态和累计指标。
///
/// 返回: 请求完成后的状态；运行时通道或应用失败时返回错误。
async fn execute_loaded_candidate(
    current: &mut LoadedConfig,
    candidate: LoadedConfig,
    mode: ApplyMode,
    execution: CandidateExecutionContext<'_>,
    apply_tx: &mpsc::Sender<ApplyConfigRequest>,
    state: &ReloadState,
) -> Result<ReloadOperationResponse, String> {
    let CandidateExecutionContext {
        origin,
        cluster_commit,
        failure_trigger,
        failure_transaction,
    } = execution;
    state.set_phase(ReloadPhase::Validating).await;
    let diff = ConfigDiff::between(current, &candidate);
    state.set_diff(diff.clone()).await;
    let committed_publish = cluster_commit.is_some() && mode == ApplyMode::Publish;
    if (candidate.digest() == current.digest() || diff.is_empty()) && !committed_publish {
        if let Some(commit) = cluster_commit {
            state
                .finish_cluster_success(commit.revision, commit.digest)
                .await;
        } else {
            state.finish_noop().await;
        }
        return Ok(ReloadOperationResponse {
            status: state.snapshot().await,
        });
    }
    if mode == ApplyMode::Publish
        && cluster_commit.is_none()
        && state.snapshot().await.config_revision == u64::MAX
    {
        let error = "configuration revision space is exhausted".to_string();
        state
            .finish_failure(failure_trigger, failure_transaction, error.clone())
            .await;
        return Err(error);
    }

    state.set_phase(ReloadPhase::Preparing).await;
    let (response_tx, response_rx) = oneshot::channel();
    if apply_tx
        .send(ApplyConfigRequest {
            candidate: candidate.clone(),
            diff,
            mode,
            origin,
            response: response_tx,
        })
        .await
        .is_err()
    {
        let error = "runtime configuration manager is not running".to_string();
        state
            .finish_failure(failure_trigger, failure_transaction, error.clone())
            .await;
        return Err(error);
    }
    let runtime_result = match response_rx.await {
        Ok(result) => result,
        Err(_) => {
            let error = "runtime configuration manager stopped before responding".to_string();
            state
                .finish_failure(failure_trigger, failure_transaction, error.clone())
                .await;
            return Err(error);
        }
    };
    if let Err(error) = runtime_result {
        state
            .finish_failure(failure_trigger, failure_transaction, error.clone())
            .await;
        return Err(error);
    }

    match mode {
        ApplyMode::HaPrecommit | ApplyMode::ValidateOnly => {
            state.finish_validated().await;
        }
        ApplyMode::Publish => {
            state.set_phase(ReloadPhase::Publishing).await;
            *current = candidate;
            let committed_digest = if let Some(commit) = cluster_commit {
                let digest = commit.digest.clone();
                state
                    .finish_cluster_success(commit.revision, commit.digest)
                    .await;
                digest
            } else {
                state.finish_success(current.digest()).await?;
                current.digest().to_string()
            };
            let revision = state.snapshot().await.config_revision;
            info!(
                revision,
                digest = %digest_prefix(&committed_digest),
                "configuration reload committed"
            );
        }
    }
    Ok(ReloadOperationResponse {
        status: state.snapshot().await,
    })
}

/// 绑定候选运行时执行来源、集群提交信息和失败归因身份。
struct CandidateExecutionContext<'a> {
    /// 本地配置源或集群协调协议来源。
    origin: ApplyOrigin,
    /// 集群 activate 阶段已经提交的版本；其他阶段为空。
    cluster_commit: Option<ClusterCommit>,
    /// 当前运行阶段失败时应记录的明确来源。
    failure_trigger: ReloadTrigger,
    /// 当前集群阶段所属的精确事务身份；本地事务为空。
    failure_transaction: Option<&'a ReloadTransactionId>,
}

/// 绑定一次集群 activate 请求已经由协调日志确认的版本信息。
struct ClusterCommit {
    /// 协调状态机提交的精确配置版本。
    revision: u64,
    /// 全部成员共享的规范化配置摘要。
    digest: String,
}

/// 业务作用: 将当前领域配置交给共享补读计时器，保持候选发布由本 supervisor 串行裁决。
///
/// 参数说明:
/// - `current`: 当前活动配置，提供轮询周期。
///
/// 返回: 首次检查立即可用的框架计时器；非法周期拒绝创建，禁用状态不消费配置中的周期。
fn reload_interval(current: &LoadedConfig) -> anyhow::Result<ConfigPoller> {
    let configuration = current.config();
    let interval_ms = if configuration.reload.enabled {
        configuration.reload.poll_interval_ms
    } else {
        nasa::application::DEFAULT_CONFIG_POLL_INTERVAL_MS
    };
    ConfigPoller::new(interval_ms).context("configure configuration polling")
}

/// 业务作用: 创建 Unix SIGHUP 接收器，供进程不退出地触发配置事务。
///
/// 返回: 已向 Tokio 注册的 SIGHUP 信号流。
fn hangup_signal() -> anyhow::Result<tokio::signal::unix::Signal> {
    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        .context("register SIGHUP handler")
}

/// 业务作用: 截取配置摘要前十二位用于日志关联，避免重复输出完整摘要。
///
/// 参数说明:
/// - `digest`: 完整六十四位 SHA-256 摘要。
///
/// 返回: 最多十二个 ASCII 字符的摘要前缀。
fn digest_prefix(digest: &str) -> &str {
    digest.get(..12).unwrap_or(digest)
}
