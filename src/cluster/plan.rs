use std::collections::BTreeMap;

use crate::cluster::spec::{route_plan, AggregatePolicy, KeylessPolicy, RoutePlan};

/// 业务作用: 保存整批命令的路由计划、按节点分组和是否需要跨节点聚合的执行摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
// 表示 PipelinePlan 的数据结构，用于规划 pipeline 命令的路由和响应顺序。
pub struct PipelinePlan {
    /// 按客户端原始顺序保存的逐命令路由计划。
    pub entries: Vec<PlannedCommand>,
    /// 诊断模式下按槽位归组的命令下标；执行模式可保持为空以减少分配。
    pub slot_batches: BTreeMap<u16, Vec<usize>>,
}

/// 业务作用: 把单条前端命令与其路由策略绑定，供执行阶段保持原始顺序。
#[derive(Debug, Clone, PartialEq, Eq)]
// 表示 PlannedCommand 的数据结构，用于规划 pipeline 命令的路由和响应顺序。
pub struct PlannedCommand {
    /// 命令在原始 pipeline 中的位置，用于聚合后恢复响应顺序。
    pub sequence: usize,
    /// 当前命令经语义分析得到的明确路由方式。
    pub route: PlannedRoute,
}

/// 业务作用: 区分本地响应、单后端、按节点拆分和拒绝等已解析路由形态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlannedRoute {
    /// 不依赖具体 key 槽位，按本地、默认节点或全节点策略执行。
    Keyless(KeylessPolicy),
    /// 整条命令只访问一个 Redis Cluster 槽位。
    SingleSlot(u16),
    /// 命令可以拆到多个槽位执行，再按指定策略聚合响应。
    SplitKeys {
        /// 与命令中各 key 顺序对应的槽位列表。
        slots: Vec<u16>,
        /// 所有子请求完成后使用的响应聚合方式。
        aggregate: AggregatePolicy,
    },
    /// 命令语义或参数不受支持，直接向该序号返回本地错误。
    LocalError(String),
}

/// 业务作用: 为前端 pipeline 生成可观测的 Cluster 路由计划。
/// 参数说明:
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
/// 构建完整的诊断计划，同时保留排查路由所需的 slot 到命令索引。
pub fn plan_pipeline<S: AsRef<[u8]>>(commands: &[Vec<S>]) -> anyhow::Result<PipelinePlan> {
    plan_pipeline_inner(commands, true)
}

/// 业务作用: 为实际执行生成路由计划，并允许受控放宽未注册命令策略。
/// 参数说明:
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
/// 构建运行时计划，不收集额外的 slot 批次索引，减少高并发热路径开销。
pub fn plan_pipeline_for_execution<S: AsRef<[u8]>>(
    commands: &[Vec<S>],
) -> anyhow::Result<PipelinePlan> {
    plan_pipeline_inner(commands, false)
}

/// 业务作用: 逐条解析 pipeline 命令规格并构造本地、单槽、拆分或拒绝计划。
/// 参数说明:
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `collect_slot_batches`: 是否收集按槽位拆分的子批次；关闭时只做路由校验而不保留批次。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
/// 按命令语义分类每条 pipeline 命令，并按需要收集辅助索引。
fn plan_pipeline_inner<S: AsRef<[u8]>>(
    commands: &[Vec<S>],
    collect_slot_batches: bool,
) -> anyhow::Result<PipelinePlan> {
    let mut entries = Vec::with_capacity(commands.len());
    let mut slot_batches: BTreeMap<u16, Vec<usize>> = BTreeMap::new();

    for (sequence, args) in commands.iter().enumerate() {
        let route = match route_plan(args)? {
            RoutePlan::Keyless(policy) => PlannedRoute::Keyless(policy),
            RoutePlan::SingleSlot { slot } => {
                if collect_slot_batches {
                    slot_batches.entry(slot).or_default().push(sequence);
                }
                PlannedRoute::SingleSlot(slot)
            }
            RoutePlan::SplitKeys { slots, aggregate } => {
                if collect_slot_batches {
                    for slot in &slots {
                        slot_batches.entry(*slot).or_default().push(sequence);
                    }
                }
                PlannedRoute::SplitKeys { slots, aggregate }
            }
            RoutePlan::Unsupported { reason } => PlannedRoute::LocalError(reason),
        };
        entries.push(PlannedCommand { sequence, route });
    }

    Ok(PipelinePlan {
        entries,
        slot_batches,
    })
}
