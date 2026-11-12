// KeyDB 单主托管复制 HA 适配器。
//
// 复制控制依赖 `INFO replication`、`FAILOVER TO`、`REPLICAOF` 和动态复制凭据。
// 持久化点按产品身份使用严格推进的 `LASTSAVE`，不依赖 `rdb_saves` 字段。

/// KeyDB 单主复制拓扑控制适配器。
pub(crate) type KeyDbHaBackend = super::redis::RedisHaBackend;
/// KeyDB 动态 RDB/AOF 目标配置。
pub(crate) type KeyDbPersistenceSettings = super::redis::RedisPersistenceSettings;
