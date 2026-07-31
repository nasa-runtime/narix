use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::RwLock;

use super::ConfigDiff;

/// 表示一次配置检查或事务由哪个入口触发。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReloadTrigger {
    /// 配置文件或证书依赖内容发生稳定变化。
    File,
    /// Unix `SIGHUP` 信号触发。
    Signal,
    /// 管理接口只执行解析和预检。
    AdminValidate,
    /// 管理接口要求执行完整发布事务。
    AdminReload,
    /// 集群领导节点广播候选配置并要求本节点只预检。
    ClusterPrepare,
    /// 集群领导节点执行 HA 节点集合变化所需的唯一拓扑动作。
    ClusterPrecommit,
    /// 集群协调日志提交后要求本节点发布已持久化版本。
    ClusterActivate,
}

impl ReloadTrigger {
    /// 业务作用: 返回与序列化一致的稳定触发来源名称,供指标标签和按来源计数使用。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Signal => "signal",
            Self::AdminValidate => "admin-validate",
            Self::AdminReload => "admin-reload",
            Self::ClusterPrepare => "cluster-prepare",
            Self::ClusterPrecommit => "cluster-precommit",
            Self::ClusterActivate => "cluster-activate",
        }
    }
}

/// 使用候选所基于的旧版本和规范化摘要唯一标识一次集群配置提案。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ReloadTransactionId {
    expected_revision: u64,
    digest: String,
}

impl ReloadTransactionId {
    /// 业务作用: 构造不包含配置正文或凭据的集群配置事务身份。
    ///
    /// 参数说明:
    /// - `expected_revision`: 候选配置生成时所基于的已提交版本。
    /// - `digest`: 候选规范化集群配置正文的 SHA-256 摘要。
    ///
    /// 返回: 可用于关联外层提案与本地集群阶段失败的稳定事务身份。
    pub(crate) fn new(expected_revision: u64, digest: impl Into<String>) -> Self {
        Self {
            expected_revision,
            digest: digest.into(),
        }
    }
}

/// 表示配置事务当前所在的阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReloadPhase {
    /// 当前没有配置事务执行。
    Idle,
    /// 正在读取和合并配置来源。
    Loading,
    /// 正在执行静态校验和运行时预检。
    Validating,
    /// 正在准备新代理运行代和临时监听器。
    Preparing,
    /// 正在执行 HA 拓扑或协调提交前置动作。
    HaPrecommit,
    /// 正在原子发布新配置和代理运行代。
    Publishing,
    /// 正在排空和清理旧运行代。
    Draining,
}

/// 表示最近一次配置请求的终态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReloadResultKind {
    /// 配置已完成预检或发布。
    Success,
    /// 候选内容与当前版本一致，没有重建运行时。
    NoOp,
    /// 配置加载、预检、提交或清理失败。
    Failure,
}

/// 管理接口可读取的配置热更新状态快照。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReloadSnapshot {
    /// 当前已提交的单调递增配置版本。
    pub config_revision: u64,
    /// 当前有效配置的脱敏 SHA-256 摘要。
    pub digest: String,
    /// 最近一次请求的触发入口。
    pub last_trigger: Option<ReloadTrigger>,
    /// 当前事务阶段。
    pub phase: ReloadPhase,
    /// 最近一次请求开始时间，Unix 毫秒。
    pub started_at_ms: Option<u64>,
    /// 最近一次请求结束时间，Unix 毫秒。
    pub finished_at_ms: Option<u64>,
    /// 最近一次请求结果。
    pub last_result: Option<ReloadResultKind>,
    /// 最近一次失败的脱敏原因。
    pub last_error: Option<String>,
    /// 最近一次候选版本与活动版本的代理差异。
    pub diff: ConfigDiff,
    /// 启动以来收到的配置请求总数。
    pub reload_total: u64,
    /// 启动以来失败的配置请求总数。
    pub reload_failures_total: u64,
    /// 启动以来按触发来源拆分的失败次数,用于区分文件轮询、信号、管理和集群阶段失败。
    pub reload_failures_by_trigger: BTreeMap<&'static str, u64>,
    /// 本节点仍在等待外层结果的集群提案及其发起来源。
    #[serde(skip)]
    outer_transactions: BTreeMap<ReloadTransactionId, ReloadTrigger>,
    /// 已经由本地集群阶段计数、等待同事务外层失败收口的来源。
    #[serde(skip)]
    pending_transaction_failures: BTreeMap<ReloadTransactionId, ReloadTrigger>,
    /// 最近一次请求持续时间，单位秒。
    pub last_duration_seconds: f64,
    /// 最近一次集群提交响应时尚未完成运行代发布的静态成员数量。
    pub pending_members: u32,
}

/// 保存热更新状态和累计指标，所有管理查询都从这里生成短生命周期快照。
#[derive(Clone)]
pub struct ReloadState {
    inner: Arc<RwLock<ReloadSnapshot>>,
    reload_total: Arc<AtomicU64>,
    reload_failures_total: Arc<AtomicU64>,
}

impl ReloadState {
    /// 业务作用: 使用启动时已验证配置初始化热更新状态。
    ///
    /// 参数说明:
    /// - `digest`: 启动配置的脱敏确定性摘要。
    /// - `initial_revision`: 单节点从一开始，集群节点在首个多数派提交前从零开始。
    ///
    /// 返回: 使用指定初始版本、当前阶段为空闲的共享状态。
    pub fn new(digest: impl Into<String>, initial_revision: u64) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ReloadSnapshot {
                config_revision: initial_revision,
                digest: digest.into(),
                last_trigger: None,
                phase: ReloadPhase::Idle,
                started_at_ms: None,
                finished_at_ms: None,
                last_result: None,
                last_error: None,
                diff: ConfigDiff::default(),
                reload_total: 0,
                reload_failures_total: 0,
                reload_failures_by_trigger: BTreeMap::new(),
                outer_transactions: BTreeMap::new(),
                pending_transaction_failures: BTreeMap::new(),
                last_duration_seconds: 0.0,
                pending_members: 0,
            })),
            reload_total: Arc::new(AtomicU64::new(0)),
            reload_failures_total: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 业务作用: 记录一个配置请求开始并切换事务阶段。
    ///
    /// 参数说明:
    /// - `trigger`: 本次请求的触发入口。
    pub async fn begin(&self, trigger: ReloadTrigger) {
        self.reload_total.fetch_add(1, Ordering::Relaxed);
        let mut snapshot = self.inner.write().await;
        snapshot.last_trigger = Some(trigger);
        snapshot.phase = ReloadPhase::Loading;
        snapshot.started_at_ms = Some(now_epoch_ms());
        snapshot.finished_at_ms = None;
        snapshot.last_error = None;
        snapshot.diff = ConfigDiff::default();
    }

    /// 业务作用: 登记一个已经完成本地候选构造、即将提交协调器的外层集群事务。
    ///
    /// 参数说明:
    /// - `transaction`: 候选旧版本和规范化摘要组成的事务身份。
    /// - `trigger`: 发起提案的文件、信号或管理端入口。
    pub(crate) async fn register_cluster_transaction(
        &self,
        transaction: ReloadTransactionId,
        trigger: ReloadTrigger,
    ) {
        self.inner
            .write()
            .await
            .outer_transactions
            .insert(transaction, trigger);
    }

    /// 业务作用: 更新正在执行的配置事务阶段。
    ///
    /// 参数说明:
    /// - `phase`: 已进入的新事务阶段。
    pub async fn set_phase(&self, phase: ReloadPhase) {
        self.inner.write().await.phase = phase;
    }

    /// 业务作用: 保存已经通过加载和静态校验的候选差异。
    ///
    /// 参数说明:
    /// - `diff`: 当前活动版本与候选版本的确定性差异。
    pub async fn set_diff(&self, diff: ConfigDiff) {
        self.inner.write().await.diff = diff;
    }

    /// 业务作用: 将一次成功发布记录为新的活动配置版本。
    ///
    /// 参数说明:
    /// - `digest`: 新活动配置的脱敏摘要。
    ///
    /// 返回: 版本成功严格递增时返回成功；版本空间耗尽时保持原状态并返回错误。
    pub async fn finish_success(&self, digest: impl Into<String>) -> Result<(), String> {
        let mut snapshot = self.inner.write().await;
        snapshot.config_revision = snapshot
            .config_revision
            .checked_add(1)
            .ok_or_else(|| "configuration revision space is exhausted".to_string())?;
        snapshot.digest = digest.into();
        finish_snapshot(&mut snapshot, ReloadResultKind::Success, None);
        Ok(())
    }

    /// 业务作用: 将协调日志已经提交的精确版本和摘要记录为本节点活动配置。
    ///
    /// 参数说明:
    /// - `revision`: 协调状态机确认的集群配置版本。
    /// - `digest`: 全成员一致的规范化集群配置摘要。
    pub async fn finish_cluster_success(&self, revision: u64, digest: impl Into<String>) {
        let mut snapshot = self.inner.write().await;
        snapshot.config_revision = revision;
        snapshot.digest = digest.into();
        finish_snapshot(&mut snapshot, ReloadResultKind::Success, None);
    }

    /// 业务作用: 更新最近一次集群配置提交响应时仍待收敛的成员数量。
    ///
    /// 参数说明:
    /// - `pending_members`: 已提交但尚未确认完成运行代发布的静态成员数量。
    pub async fn set_pending_members(&self, pending_members: u32) {
        self.inner.write().await.pending_members = pending_members;
    }

    /// 业务作用: 记录只预检成功，不改变活动 revision 和摘要。
    pub async fn finish_validated(&self) {
        let mut snapshot = self.inner.write().await;
        finish_snapshot(&mut snapshot, ReloadResultKind::Success, None);
    }

    /// 业务作用: 将候选内容与当前版本相同的请求记录为无操作。
    pub async fn finish_noop(&self) {
        let mut snapshot = self.inner.write().await;
        finish_snapshot(&mut snapshot, ReloadResultKind::NoOp, None);
    }

    /// 业务作用: 记录一次失败事务并保留当前活动版本和摘要。
    ///
    /// 参数说明:
    /// - `trigger`: 本次失败实际发生的文件、信号、管理或集群阶段；已登记外层事务时会立即改归因到发起入口。
    /// - `transaction`: 集群阶段所属的事务身份；非集群阶段为空。
    /// - `error`: 已完成凭据脱敏、可供运维定位阶段的错误文本。
    pub(crate) async fn finish_failure(
        &self,
        trigger: ReloadTrigger,
        transaction: Option<&ReloadTransactionId>,
        error: impl Into<String>,
    ) {
        self.reload_failures_total.fetch_add(1, Ordering::Relaxed);
        let mut snapshot = self.inner.write().await;
        // 领导节点同时执行自身 prepare/activate 时，内部阶段可能先于外层提案任务
        // 返回错误。以精确事务身份立即查回发起来源，避免管理状态短暂暴露
        // cluster-prepare 等中间归因，也避免并发事务之间互相搬移计数。
        let attributed_trigger = transaction
            .and_then(|transaction| snapshot.outer_transactions.get(transaction).copied())
            .unwrap_or(trigger);
        record_trigger_failure(&mut snapshot, attributed_trigger);
        if let Some(transaction) = transaction {
            if snapshot.outer_transactions.contains_key(transaction) {
                snapshot
                    .pending_transaction_failures
                    .insert(transaction.clone(), attributed_trigger);
            }
        }
        snapshot.last_trigger = Some(attributed_trigger);
        finish_snapshot(&mut snapshot, ReloadResultKind::Failure, Some(error.into()));
    }

    /// 业务作用: 收口外层集群提案错误，并避免本地 prepare 或 activate 已记录失败时重复计数。
    ///
    /// 参数说明:
    /// - `trigger`: 发起本次外层集群提案的文件、信号或管理入口。
    /// - `transaction`: 候选旧版本和摘要组成的精确事务身份。
    /// - `error`: 外层协调事务补充的脱敏错误上下文。
    pub(crate) async fn finish_cluster_transaction_failure(
        &self,
        trigger: ReloadTrigger,
        transaction: &ReloadTransactionId,
        error: impl Into<String>,
    ) {
        let error = error.into();
        let mut snapshot = self.inner.write().await;
        // 已登记的发起来源与事务身份一起保存，是失败归因的权威值；显式参数只为
        // 没有本地外层登记的收口路径提供后备来源。
        let trigger = snapshot
            .outer_transactions
            .remove(transaction)
            .unwrap_or(trigger);
        if let Some(previous) = snapshot.pending_transaction_failures.remove(transaction) {
            reattribute_trigger_failure(&mut snapshot, previous, trigger);
        } else {
            self.reload_failures_total.fetch_add(1, Ordering::Relaxed);
            record_trigger_failure(&mut snapshot, trigger);
        }
        snapshot.last_trigger = Some(trigger);
        finish_snapshot(&mut snapshot, ReloadResultKind::Failure, Some(error));
    }

    /// 业务作用: 清理一个已经成功提交并返回外层调用方的集群事务归因状态。
    ///
    /// 参数说明:
    /// - `transaction`: 已经成功收口的候选事务身份。
    pub(crate) async fn finish_cluster_transaction_success(
        &self,
        transaction: &ReloadTransactionId,
    ) {
        let mut snapshot = self.inner.write().await;
        snapshot.outer_transactions.remove(transaction);
        snapshot.pending_transaction_failures.remove(transaction);
    }

    /// 业务作用: 返回当前热更新状态及最新累计计数。
    ///
    /// 返回: 不持有内部锁的独立管理状态快照。
    pub async fn snapshot(&self) -> ReloadSnapshot {
        let mut snapshot = self.inner.read().await.clone();
        snapshot.reload_total = self.reload_total.load(Ordering::Relaxed);
        snapshot.reload_failures_total = self.reload_failures_total.load(Ordering::Relaxed);
        snapshot
    }
}

/// 业务作用: 按明确触发来源累计一次失败。
///
/// 参数说明:
/// - `snapshot`: 持有 `last_trigger` 和按来源失败计数的共享状态快照。
/// - `trigger`: 本次失败应归属的外部请求或内部集群阶段。
fn record_trigger_failure(snapshot: &mut ReloadSnapshot, trigger: ReloadTrigger) {
    *snapshot
        .reload_failures_by_trigger
        .entry(trigger.as_str())
        .or_insert(0) += 1;
}

/// 业务作用: 把同一集群事务的本地阶段失败改归因到真正发起提案的外部入口。
///
/// 参数说明:
/// - `snapshot`: 保存累计失败指标的共享状态快照。
/// - `previous`: 此事务先前计数的内部集群阶段来源。
/// - `trigger`: 文件、信号或管理端发起的外层提案来源。
fn reattribute_trigger_failure(
    snapshot: &mut ReloadSnapshot,
    previous: ReloadTrigger,
    trigger: ReloadTrigger,
) {
    if previous == trigger {
        return;
    }
    let previous = previous.as_str();
    if let Some(count) = snapshot.reload_failures_by_trigger.get_mut(previous) {
        *count = count.saturating_sub(1);
        if *count == 0 {
            snapshot.reload_failures_by_trigger.remove(previous);
        }
    }
    record_trigger_failure(snapshot, trigger);
}

/// 业务作用: 完成一次配置请求并计算持续时间。
///
/// 参数说明:
/// - `snapshot`: 正在原地更新的共享状态快照。
/// - `result`: 本次请求的成功、无操作或失败结果。
/// - `error`: 失败时保存的脱敏错误，成功和无操作时为空。
fn finish_snapshot(snapshot: &mut ReloadSnapshot, result: ReloadResultKind, error: Option<String>) {
    let finished_at_ms = now_epoch_ms();
    snapshot.finished_at_ms = Some(finished_at_ms);
    snapshot.last_duration_seconds = snapshot
        .started_at_ms
        .map(|started| finished_at_ms.saturating_sub(started) as f64 / 1_000.0)
        .unwrap_or_default();
    snapshot.phase = ReloadPhase::Idle;
    snapshot.last_result = Some(result);
    snapshot.last_error = error;
}

/// 业务作用: 返回当前 Unix 时间的毫秒值，系统时钟早于纪元时安全返回零。
///
/// 返回: 可供状态排序和持续时间计算的 Unix 毫秒时间。
fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}
