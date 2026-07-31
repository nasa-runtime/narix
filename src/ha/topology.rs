// 托管复制 HA 的后端无关持久化拓扑规则。

/// HA 组在当前健康节点规模下应采用的持久化布局。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PersistenceLayout {
    /// 只有一个配置节点时，由主节点承担快照持久化。
    SingleMasterSnapshot,
    /// 当前只有一个合格副本时，由该副本承担快照，不保留独立热备。
    SingleReplicaSnapshot,
    /// 当前至少有两个合格副本时，保留一个无持久化热备，其余副本全部承担快照。
    HotStandbyReplicaSnapshots,
}

/// 业务作用: 根据配置规模和当前合格副本数量选择持久化布局。
///
/// 参数:
/// - `configured_node_count`: HA 配置中的静态节点总数，用于限制只有真实单节点部署才能让主节点持久化。
/// - `eligible_replica_count`: 当前跟随正确主节点、复制延迟合格且通过稳定窗口的副本数量。
///
/// 返回: 能形成满足持久化隔离约束的布局；多节点部署暂时没有合格副本时返回空并由运行时进入保护。
pub(crate) fn select_persistence_layout(
    configured_node_count: usize,
    eligible_replica_count: usize,
) -> Option<PersistenceLayout> {
    match (configured_node_count, eligible_replica_count) {
        (0, _) => None,
        (1, _) => Some(PersistenceLayout::SingleMasterSnapshot),
        (_, 0) => None,
        (_, 1) => Some(PersistenceLayout::SingleReplicaSnapshot),
        (_, _) => Some(PersistenceLayout::HotStandbyReplicaSnapshots),
    }
}
