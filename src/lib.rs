//! NARIX 的配置、协议解析、路由、鉴权、管理面、托管复制 HA 与集群协调实现。

/// 管理接口、状态快照和运行指标。
pub mod admin;
/// 前端与后端 Redis 认证状态转换。
pub mod auth;
/// RESP KV 后端兼容档案及会改变运行路径的产品级能力。
pub mod backend;
#[allow(dead_code)]
/// Redis Cluster 拓扑、路由计划、连接池与响应合并。
pub mod cluster;
/// YAML 加载、环境覆盖、默认值和启动前校验。
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
