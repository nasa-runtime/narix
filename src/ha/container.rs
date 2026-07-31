use crate::backend::BackendProfile;
use crate::ha::backend::ReplicationHaBackend;
use crate::ha::model::HaNode;

/// 通用 HA 运行时持有的静态拓扑和产品适配器。
///
/// 通用状态机只依赖产品适配器契约，避免把任一后端的复制命令泄漏到运行时编排层。
#[derive(Debug)]
pub(crate) struct HaRuntime<B> {
    nodes: Vec<HaNode>,
    backend: B,
}

impl<B> HaRuntime<B>
where
    B: ReplicationHaBackend,
{
    /// 业务作用: 构造一个后端类型确定的通用 HA 运行时。
    ///
    /// 参数:
    /// - `nodes`: 当前 HA 组允许参与拓扑探测和切换的节点。
    /// - `backend`: 负责产品命令、响应解析和动态持久化的适配器。
    /// - `expected_profile`: 配置层已经校验并传入的后端档案。
    pub(crate) fn new(nodes: Vec<HaNode>, backend: B, expected_profile: BackendProfile) -> Self {
        debug_assert!(!nodes.is_empty());
        debug_assert_eq!(backend.profile(), expected_profile);
        Self { nodes, backend }
    }

    /// 业务作用: 返回当前 HA 组的静态节点清单。
    pub(crate) fn nodes(&self) -> &[HaNode] {
        &self.nodes
    }

    /// 业务作用: 返回产品 HA 适配器。
    pub(crate) fn backend(&self) -> &B {
        &self.backend
    }
}
