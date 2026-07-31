use crate::ha::backend::HaActionAuthority;
use crate::ha::model::{HaNode, HaRole};

/// HA 状态机要求节点达到的持久化职责。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistenceTarget {
    /// 节点承担当前持久化 owner。
    Owner,
    /// 节点作为热备保留，但不承担周期持久化任务。
    Standby,
    /// 节点持久化任务被显式暂停。
    Suspended,
}

/// 后端适配器声明的动态持久化能力。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistenceCapabilities {
    /// 是否支持在运行期改变持久化配置。
    pub(crate) runtime_reconfigure: bool,
    /// 是否支持由 NARIX 强制创建持久化点。
    pub(crate) force_checkpoint: bool,
    /// 是否支持读取后端状态验证持久化点。
    pub(crate) verify_checkpoint: bool,
    /// 动态配置是否由后端自身保证重启后仍然保留。
    pub(crate) restart_durable: bool,
}

/// 节点当前实际生效的持久化状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PersistenceState {
    /// 已识别的持久化职责；产品配置异常或无法映射时为空。
    pub(crate) target: Option<PersistenceTarget>,
    /// 运行时重新读取的配置是否与目标职责一致。
    pub(crate) effective: bool,
    /// 后端是否自行保证当前动态配置在重启后保留。
    pub(crate) restart_durable: bool,
}

/// 动态持久化配置动作的确认结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PersistenceApplyResult {
    /// 重新读取是否确认运行时配置已经生效。
    pub(crate) effective: bool,
    /// 后端是否确认配置能在重启后保留。
    pub(crate) restart_durable: bool,
}

/// 强制持久化点的触发原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointReason {
    /// HA 持久化 owner 迁移。
    Handoff,
}

/// 后端适配器返回的持久化点验证令牌。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointToken {
    /// 产品适配器可选的持久化点标识；没有稳定标识时为空。
    pub(crate) backend_marker: Option<String>,
}

/// 持久化点的验证状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckpointStatus {
    /// 后端已经确认持久化点完成。
    Complete,
}

/// 产品动态持久化契约。
///
/// 通用 HA 状态机只表达 owner、standby、suspended 和持久化点，不接触产品配置名。
pub(crate) trait DynamicPersistenceBackend: Send + Sync {
    /// 业务作用: 返回适配器支持的动态持久化能力。
    fn persistence_capabilities(&self) -> PersistenceCapabilities;

    /// 业务作用: 查询节点当前实际生效的持久化状态。
    ///
    /// 参数:
    /// - `node`: 要查询的后端节点。
    /// - `role`: 节点当前角色，用于展开按角色区分的产品配置。
    async fn inspect_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
    ) -> anyhow::Result<PersistenceState>;

    /// 业务作用: 幂等地把节点收敛到目标持久化职责。
    ///
    /// 参数:
    /// - `node`: 要动态配置的后端节点。
    /// - `role`: 节点当前角色，用于选择产品持久化策略。
    /// - `target`: HA 状态机要求的目标持久化职责。
    /// - `authority`: 每条产品配置写之前必须重新检查的动作权威。
    async fn reconcile_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
        target: PersistenceTarget,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<PersistenceApplyResult>;

    /// 业务作用: 强制创建一次可验证的持久化点。
    ///
    /// 参数:
    /// - `node`: 执行持久化动作的后端节点。
    /// - `reason`: 本次持久化动作的业务触发原因。
    /// - `authority`: 触发命令及前置等待期间持续检查的动作权威。
    async fn force_checkpoint(
        &self,
        node: &HaNode,
        reason: CheckpointReason,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointToken>;

    /// 业务作用: 验证指定持久化点已经完成。
    ///
    /// 参数:
    /// - `node`: 生成持久化点的后端节点。
    /// - `token`: `force_checkpoint` 返回的后端验证令牌。
    /// - `authority`: 验证轮询期间持续检查的动作权威。
    async fn verify_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus>;
}
