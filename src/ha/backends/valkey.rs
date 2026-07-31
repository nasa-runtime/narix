// Valkey 9.1.1 托管复制 HA 适配器。
//
// Valkey 9.1.1 已通过官方镜像确认与 Redis-family 内核共享 `INFO replication`、
// `FAILOVER TO`、`REPLICAOF`、RDB/AOF 动态配置和持久化点语义。独立类型别名保留
// 封闭驱动中的产品身份，后续版本必须重新认证后才能继续复用。

/// Valkey 9.1.1 复制拓扑控制适配器。
pub(crate) type ValkeyHaBackend = super::redis::RedisHaBackend;
/// Valkey 9.1.1 动态 RDB/AOF 目标配置。
pub(crate) type ValkeyPersistenceSettings = super::redis::RedisPersistenceSettings;
