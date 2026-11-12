// Redict 托管复制 HA 适配器。
//
// 依赖 Redis-family 复制控制、动态 RDB/AOF 配置和可验证的持久化点语义。
// 独立类型别名保留封闭驱动中的产品身份，原生能力缺失时不得跳过验证。

/// Redict 复制拓扑控制适配器。
pub(crate) type RedictHaBackend = super::redis::RedisHaBackend;
/// Redict 动态 RDB/AOF 目标配置。
pub(crate) type RedictPersistenceSettings = super::redis::RedisPersistenceSettings;
