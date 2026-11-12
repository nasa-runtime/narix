//! NARIX 提供高性能 RESP2/RESP3 前置代理、托管复制 HA 控制面与开发运维只读查询入口。
//!
//! # Dragonfly HA 特色模式与数据转发
//!
//! `replication-ha` 持续探测主从角色和复制状态，在故障确认后先保护路由，再经过候选门禁、
//! 动作权威和 fencing 执行切主。普通读写使用一对一透明连接，切主或保护时关闭旧连接，
//! 客户端重连后进入已确认主节点；同一控制模型也适配 Redis、Valkey、Redict、KeyDB 和 Kvrocks。
//!
//! 动态持久化把多节点稳态的快照责任交给副本，副本充足时分离无快照热备和快照节点。
//! 交接先启用新 owner 并验证 checkpoint，再停用旧 owner，控制结果提交后才发布安全拓扑。
//! 内置协调集群或与部署范围匹配的租约串行化控制动作，每个外部步骤复验当前权威。
//!
//! # 配置与运行边界
//!
//! 配置事务统一校验、准备并发布日志、凭据与运行代；NASA Application 管理周期补读、
//! 阻塞读取隔离、任务监督与资源关闭。失败候选保留当前配置，回滚不完整时保持业务门禁关闭。
//! readonly 查询入口拒绝写入、脚本执行和未知命令形态；其他代理模式按后端原生能力准入。
//!
//! 异步复制与持久化编排不提供零数据丢失保证，默认软隔离不证明旧主失去写能力。
//! 强隔离依赖外部基础设施；断连或取消也不能证明已经发送的命令未在后端执行。

/// 管理接口、状态快照和运行指标。
pub mod admin;
/// 前端与后端 Redis 认证状态转换。
pub mod auth;
/// RESP KV 后端兼容档案及会改变运行路径的产品级能力。
pub mod backend;
#[allow(dead_code)]
/// Redis Cluster 拓扑、路由计划、连接池与响应合并。
pub mod cluster;
/// YAML、JSON、TOML 加载、环境覆盖、默认值和启动前校验。
pub mod config;
/// narix 内置的双节点或多数派协调集群。
pub mod coordinator;
/// 托管复制 HA 的通用模型、控制面契约和产品适配器。
pub(crate) mod ha;
/// 业务监听、会话状态机和各代理模式的数据转发。
pub mod proxy;
/// 全模式配置热更新的来源监控、差异、事务和运行时发布。
pub mod reload;
/// RESP 帧读取、编码和命令参数访问。
pub mod resp;
/// Redis Sentinel 主节点发现、角色校验和动态路由状态。
pub mod sentinel;

/// 日志、配置解析、指标、凭据、生命周期和时间能力的领域适配。
pub mod infrastructure;
