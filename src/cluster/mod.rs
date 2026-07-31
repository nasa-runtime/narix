/// Redis Cluster 后端连接建立与认证。
pub mod backend;
/// 普通多路复用连接的请求响应匹配。
pub mod ordinary_mux;
/// Pipeline 专用后端连接池与借还策略。
pub mod pipeline_pool;
/// 跨槽命令的子请求规划与结果形状。
pub mod plan;
/// 多节点响应校验、排序和聚合。
pub mod response;
/// 命令键提取、槽位路由和重定向处理。
pub mod router;
/// Redis Cluster 槽位哈希算法。
pub mod slot;
/// Redis 命令路由规格和参数规则。
pub mod spec;
/// Cluster Slots 拓扑发现、刷新与地址映射。
pub mod topology;
/// Tunnel 独占后端连接的有界 idle 复用池。
pub mod tunnel_pool;
