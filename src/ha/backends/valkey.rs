// Valkey 托管复制 HA 适配器。
//
// 复制控制依赖 `INFO replication`、`FAILOVER TO` 和 `REPLICAOF`，持久化要求
// 动态 RDB/AOF 配置及 checkpoint 证据。独立类型别名保留封闭驱动中的产品身份。

/// Valkey 复制拓扑控制适配器。
pub(crate) type ValkeyHaBackend = super::redis::RedisHaBackend;
/// Valkey 动态 RDB/AOF 目标配置。
pub(crate) type ValkeyPersistenceSettings = super::redis::RedisPersistenceSettings;
