// KeyDB 6.3.4 托管复制 HA 适配器。
//
// 官方镜像确认该版本与 Redis-family 内核共享 `INFO replication`、`FAILOVER TO`、
// `REPLICAOF` 和动态 RDB/AOF 配置。KeyDB 的 `INFO persistence` 没有 `rdb_saves`
// 计数，因此共享内核按产品身份改用 `LASTSAVE` 完成持久化点验证。

/// KeyDB 6.3.4 复制拓扑控制适配器。
pub(crate) type KeyDbHaBackend = super::redis::RedisHaBackend;
/// KeyDB 6.3.4 动态 RDB/AOF 目标配置。
pub(crate) type KeyDbPersistenceSettings = super::redis::RedisPersistenceSettings;
