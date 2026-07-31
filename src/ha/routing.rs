/// 数据面使用的 HA 安全路由状态。
///
/// 所有 replication-ha 产品共用该状态，数据面只能在 `Serving` 时建立后端路由。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaRoutingState {
    /// 已确认唯一安全主节点，数据面可以建立和维持业务连接。
    Serving {
        /// 当前所有业务请求必须路由到的主节点地址。
        master: String,
    },
    /// 无法证明拓扑或控制动作安全，数据面必须关闭。
    Protected {
        /// 进入保护态的可观测原因。
        reason: String,
    },
}
