use futures_util::future::BoxFuture;

use crate::backend::BackendProfile;
use crate::ha::model::{HaNode, HaNodeObservation};
use crate::ha::persistence::DynamicPersistenceBackend;

/// HA 后端多命令动作的动态权威门禁。
///
/// 产品适配器在每条会改变角色、复制上游、写暂停或持久化配置的命令前调用该门禁，
/// 防止一个上层动作在 local-file/Redis 租约或 `proxy-cluster` 令牌失效后
/// 继续执行后续写步骤；持久化点等待循环也使用它及时停止过期动作。
pub(crate) trait HaActionAuthority: Send + Sync {
    /// 业务作用: 确认当前动作仍然拥有执行下一条后端写命令的权威。
    fn ensure_active<'a>(&'a self, step: &'a str) -> BoxFuture<'a, anyhow::Result<()>>;
}

/// 测试直接认证产品适配器时使用的恒有效动作门禁。
#[cfg(test)]
#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct AlwaysActiveHaAuthority;

#[cfg(test)]
impl HaActionAuthority for AlwaysActiveHaAuthority {
    /// 业务作用: 为产品适配器单元测试提供恒有效权威，使测试只验证后端协议而不引入租约依赖。
    fn ensure_active<'a>(&'a self, _step: &'a str) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }
}

/// 托管复制 HA 的产品适配契约。
///
/// trait 只运行在探测、切换和拓扑收敛控制面，不参与每条业务命令的数据面转发。
pub(crate) trait ReplicationHaBackend: DynamicPersistenceBackend + Send + Sync {
    /// 业务作用: 返回当前适配器对应的后端档案。
    fn profile(&self) -> BackendProfile;

    /// 业务作用: 查询一个节点的角色和复制状态。
    ///
    /// 参数:
    /// - `node`: 要探测的后端节点。
    async fn inspect_node(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation>;

    /// 业务作用: 执行有健康旧主参与的计划切换。
    ///
    /// 参数:
    /// - `current_primary`: 当前已确认的健康主节点。
    /// - `candidate`: 要接管写流量的副本节点。
    async fn planned_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()>;

    /// 业务作用: 在旧主完成 fencing 后紧急提升候选副本。
    ///
    /// 参数:
    /// - `candidate`: 已通过候选门禁的副本节点。
    async fn emergency_promote(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()>;

    /// 业务作用: 将节点收敛为指定主节点的副本。
    ///
    /// 参数:
    /// - `node`: 要改为副本的后端节点。
    /// - `primary`: 该节点必须跟随的主节点。
    async fn configure_replica(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()>;

    /// 业务作用: 确认节点已经稳定跟随指定主节点。
    ///
    /// 参数:
    /// - `node`: 要校验的副本节点。
    /// - `primary`: 期望的上游主节点。
    async fn verify_replication(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()>;
}
