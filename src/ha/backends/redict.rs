// Redict 7.3.6 托管复制 HA 适配器。
//
// 该版本通过官方镜像确认后才允许复用 Redis-family 的复制控制、RDB/AOF 动态配置
// 和持久化点内核。独立类型别名保留封闭驱动中的产品身份，后续版本必须重新认证。

/// Redict 7.3.6 复制拓扑控制适配器。
pub(crate) type RedictHaBackend = super::redis::RedisHaBackend;
/// Redict 7.3.6 动态 RDB/AOF 目标配置。
pub(crate) type RedictPersistenceSettings = super::redis::RedisPersistenceSettings;
