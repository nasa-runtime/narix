use anyhow::Context;

use crate::backend::BackendProfile;
use crate::config::{ProxyItem, RedisHaPersistenceConfig};
use crate::ha::backend::{HaActionAuthority, ReplicationHaBackend};
use crate::ha::backends::dragonfly::DragonflyHaBackend;
use crate::ha::backends::keydb::{KeyDbHaBackend, KeyDbPersistenceSettings};
use crate::ha::backends::kvrocks::KvrocksHaBackend;
use crate::ha::backends::redict::{RedictHaBackend, RedictPersistenceSettings};
use crate::ha::backends::redis::{RedisHaBackend, RedisPersistenceSettings};
use crate::ha::backends::valkey::{ValkeyHaBackend, ValkeyPersistenceSettings};
use crate::ha::control_client::HaGenerationGuard;
use crate::ha::model::{HaNode, HaNodeObservation, HaRole};
use crate::ha::persistence::{
    CheckpointReason, CheckpointStatus, CheckpointToken, DynamicPersistenceBackend,
    PersistenceApplyResult, PersistenceCapabilities, PersistenceState, PersistenceTarget,
};

/// 编译期封闭的托管复制 HA 后端驱动。
///
/// 枚举分发只发生在低频控制面，业务命令数据面仍直接使用已经发布的主节点地址。
#[derive(Debug, Clone)]
pub(crate) enum HaBackendDriver {
    /// Dragonfly 原生命令和动态快照实现。
    Dragonfly(DragonflyHaBackend),
    /// Redis 8.2.2 复制及 RDB/AOF 实现。
    Redis(RedisHaBackend),
    /// Valkey 9.1.1 复制及 RDB/AOF 实现。
    Valkey(ValkeyHaBackend),
    /// Redict 7.3.6 复制及 RDB/AOF 实现。
    Redict(RedictHaBackend),
    /// Kvrocks 2.16.0 原生 `SLAVEOF` 主从切换实现。
    Kvrocks(KvrocksHaBackend),
    /// KeyDB 6.3.4 复制及 RDB/AOF 实现。
    KeyDb(KeyDbHaBackend),
}

impl HaBackendDriver {
    /// 业务作用: 从已校验的代理配置构造封闭产品驱动。
    ///
    /// 参数:
    /// - `item`: 提供后端档案、认证、TLS、超时和产品持久化子配置的代理项。
    /// - `owner_id`: Dragonfly 持久化模板兼容使用的 coordinator owner 标识。
    pub(crate) fn from_proxy_item(item: &ProxyItem, owner_id: String) -> anyhow::Result<Self> {
        Self::from_proxy_item_with_guard(item, owner_id, HaGenerationGuard::default())
    }

    /// 业务作用: 从代理配置和共享运行代令牌构造封闭产品驱动。
    ///
    /// 参数:
    /// - `item`: 提供后端档案、认证、TLS、超时和产品持久化子配置的代理项。
    /// - `owner_id`: Dragonfly 持久化模板兼容使用的 coordinator owner 标识。
    /// - `generation_guard`: 与所属 replication HA 运行代共享的退休状态。
    pub(crate) fn from_proxy_item_with_guard(
        item: &ProxyItem,
        owner_id: String,
        generation_guard: HaGenerationGuard,
    ) -> anyhow::Result<Self> {
        match item.backend_profile {
            BackendProfile::Dragonfly => Ok(Self::Dragonfly(DragonflyHaBackend::from_proxy_item(
                item,
                owner_id,
                generation_guard,
            )?)),
            BackendProfile::Redis => {
                let ha = item.replication_ha_config()?;
                let persistence = redis_family_persistence_settings(
                    ha.persistence.enabled,
                    ha.persistence.redis.as_ref(),
                    item.backend_profile,
                )?;
                Ok(Self::Redis(RedisHaBackend::from_proxy_item(
                    item,
                    persistence,
                    generation_guard,
                )?))
            }
            BackendProfile::Valkey => {
                let ha = item.replication_ha_config()?;
                let persistence: ValkeyPersistenceSettings = redis_family_persistence_settings(
                    ha.persistence.enabled,
                    ha.persistence.valkey.as_ref(),
                    item.backend_profile,
                )?;
                Ok(Self::Valkey(ValkeyHaBackend::from_proxy_item(
                    item,
                    persistence,
                    generation_guard,
                )?))
            }
            BackendProfile::Redict => {
                let ha = item.replication_ha_config()?;
                let persistence: RedictPersistenceSettings = redis_family_persistence_settings(
                    ha.persistence.enabled,
                    ha.persistence.redict.as_ref(),
                    item.backend_profile,
                )?;
                Ok(Self::Redict(RedictHaBackend::from_proxy_item(
                    item,
                    persistence,
                    generation_guard,
                )?))
            }
            BackendProfile::Kvrocks => Ok(Self::Kvrocks(KvrocksHaBackend::from_proxy_item(
                item,
                generation_guard,
            )?)),
            BackendProfile::KeyDb => {
                let ha = item.replication_ha_config()?;
                let persistence: KeyDbPersistenceSettings = redis_family_persistence_settings(
                    ha.persistence.enabled,
                    ha.persistence.keydb.as_ref(),
                    item.backend_profile,
                )?;
                Ok(Self::KeyDb(KeyDbHaBackend::from_proxy_item(
                    item,
                    persistence,
                    generation_guard,
                )?))
            }
            profile => anyhow::bail!(
                "backend_profile={} is not implemented for replication-ha",
                profile.as_str()
            ),
        }
    }
}

impl ReplicationHaBackend for HaBackendDriver {
    /// 业务作用: 返回当前驱动对应的后端产品档案。
    fn profile(&self) -> BackendProfile {
        match self {
            Self::Dragonfly(backend) => backend.profile(),
            Self::Redis(backend) => backend.profile(),
            Self::Valkey(backend) => backend.profile(),
            Self::Redict(backend) => backend.profile(),
            Self::Kvrocks(backend) => backend.profile(),
            Self::KeyDb(backend) => backend.profile(),
        }
    }

    /// 业务作用: 查询节点角色和复制状态。
    ///
    /// 参数:
    /// - `node`: 要交给当前产品适配器探测的后端节点。
    async fn inspect_node(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        match self {
            Self::Dragonfly(backend) => backend.inspect_node(node).await,
            Self::Redis(backend) => backend.inspect_node(node).await,
            Self::Valkey(backend) => backend.inspect_node(node).await,
            Self::Redict(backend) => backend.inspect_node(node).await,
            Self::Kvrocks(backend) => backend.inspect_node(node).await,
            Self::KeyDb(backend) => backend.inspect_node(node).await,
        }
    }

    /// 业务作用: 执行有健康旧主参与的产品原生计划切换。
    ///
    /// 参数:
    /// - `current_primary`: 当前已确认的健康主节点。
    /// - `candidate`: 要接管写流量的副本节点。
    async fn planned_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        match self {
            Self::Dragonfly(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
            Self::Redis(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
            Self::Valkey(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
            Self::Redict(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
            Self::Kvrocks(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
            Self::KeyDb(backend) => {
                backend
                    .planned_switchover(current_primary, candidate, authority)
                    .await
            }
        }
    }

    /// 业务作用: 在通用状态机完成旧主隔离后紧急提升候选。
    ///
    /// 参数:
    /// - `candidate`: 已通过候选门禁的副本节点。
    async fn emergency_promote(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        match self {
            Self::Dragonfly(backend) => backend.emergency_promote(candidate, authority).await,
            Self::Redis(backend) => backend.emergency_promote(candidate, authority).await,
            Self::Valkey(backend) => backend.emergency_promote(candidate, authority).await,
            Self::Redict(backend) => backend.emergency_promote(candidate, authority).await,
            Self::Kvrocks(backend) => backend.emergency_promote(candidate, authority).await,
            Self::KeyDb(backend) => backend.emergency_promote(candidate, authority).await,
        }
    }

    /// 业务作用: 将节点收敛为指定主节点的副本。
    ///
    /// 参数:
    /// - `node`: 要改为副本的后端节点。
    /// - `primary`: 该节点必须跟随的主节点。
    async fn configure_replica(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        match self {
            Self::Dragonfly(backend) => backend.configure_replica(node, primary, authority).await,
            Self::Redis(backend) => backend.configure_replica(node, primary, authority).await,
            Self::Valkey(backend) => backend.configure_replica(node, primary, authority).await,
            Self::Redict(backend) => backend.configure_replica(node, primary, authority).await,
            Self::Kvrocks(backend) => backend.configure_replica(node, primary, authority).await,
            Self::KeyDb(backend) => backend.configure_replica(node, primary, authority).await,
        }
    }

    /// 业务作用: 确认节点已经稳定跟随指定主节点。
    ///
    /// 参数:
    /// - `node`: 要校验的副本节点。
    /// - `primary`: 期望的上游主节点。
    async fn verify_replication(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        match self {
            Self::Dragonfly(backend) => backend.verify_replication(node, primary, authority).await,
            Self::Redis(backend) => backend.verify_replication(node, primary, authority).await,
            Self::Valkey(backend) => backend.verify_replication(node, primary, authority).await,
            Self::Redict(backend) => backend.verify_replication(node, primary, authority).await,
            Self::Kvrocks(backend) => backend.verify_replication(node, primary, authority).await,
            Self::KeyDb(backend) => backend.verify_replication(node, primary, authority).await,
        }
    }
}

impl DynamicPersistenceBackend for HaBackendDriver {
    /// 业务作用: 返回当前产品适配器支持的动态持久化能力。
    fn persistence_capabilities(&self) -> PersistenceCapabilities {
        match self {
            Self::Dragonfly(backend) => backend.persistence_capabilities(),
            Self::Redis(backend) => backend.persistence_capabilities(),
            Self::Valkey(backend) => backend.persistence_capabilities(),
            Self::Redict(backend) => backend.persistence_capabilities(),
            Self::Kvrocks(backend) => backend.persistence_capabilities(),
            Self::KeyDb(backend) => backend.persistence_capabilities(),
        }
    }

    /// 业务作用: 查询节点当前实际生效的产品持久化状态。
    ///
    /// 参数:
    /// - `node`: 要查询的后端节点。
    /// - `role`: 当前节点角色。
    async fn inspect_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
    ) -> anyhow::Result<PersistenceState> {
        match self {
            Self::Dragonfly(backend) => backend.inspect_persistence(node, role).await,
            Self::Redis(backend) => backend.inspect_persistence(node, role).await,
            Self::Valkey(backend) => backend.inspect_persistence(node, role).await,
            Self::Redict(backend) => backend.inspect_persistence(node, role).await,
            Self::Kvrocks(backend) => backend.inspect_persistence(node, role).await,
            Self::KeyDb(backend) => backend.inspect_persistence(node, role).await,
        }
    }

    /// 业务作用: 把节点幂等收敛到通用状态机要求的持久化职责。
    ///
    /// 参数:
    /// - `node`: 要动态配置的后端节点。
    /// - `role`: 当前节点角色。
    /// - `target`: owner、standby 或 suspended 目标。
    async fn reconcile_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
        target: PersistenceTarget,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<PersistenceApplyResult> {
        match self {
            Self::Dragonfly(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
            Self::Redis(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
            Self::Valkey(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
            Self::Redict(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
            Self::Kvrocks(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
            Self::KeyDb(backend) => {
                backend
                    .reconcile_persistence(node, role, target, authority)
                    .await
            }
        }
    }

    /// 业务作用: 强制创建一个产品可验证的持久化点。
    ///
    /// 参数:
    /// - `node`: 执行持久化动作的后端节点。
    /// - `reason`: 本次动作的 HA 业务原因。
    async fn force_checkpoint(
        &self,
        node: &HaNode,
        reason: CheckpointReason,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointToken> {
        match self {
            Self::Dragonfly(backend) => backend.force_checkpoint(node, reason, authority).await,
            Self::Redis(backend) => backend.force_checkpoint(node, reason, authority).await,
            Self::Valkey(backend) => backend.force_checkpoint(node, reason, authority).await,
            Self::Redict(backend) => backend.force_checkpoint(node, reason, authority).await,
            Self::Kvrocks(backend) => backend.force_checkpoint(node, reason, authority).await,
            Self::KeyDb(backend) => backend.force_checkpoint(node, reason, authority).await,
        }
    }

    /// 业务作用: 验证指定产品持久化点已经完成。
    ///
    /// 参数:
    /// - `node`: 生成持久化点的后端节点。
    /// - `token`: 适配器返回的产品验证令牌。
    async fn verify_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus> {
        match self {
            Self::Dragonfly(backend) => backend.verify_checkpoint(node, token, authority).await,
            Self::Redis(backend) => backend.verify_checkpoint(node, token, authority).await,
            Self::Valkey(backend) => backend.verify_checkpoint(node, token, authority).await,
            Self::Redict(backend) => backend.verify_checkpoint(node, token, authority).await,
            Self::Kvrocks(backend) => backend.verify_checkpoint(node, token, authority).await,
            Self::KeyDb(backend) => backend.verify_checkpoint(node, token, authority).await,
        }
    }
}

/// 业务作用: 把 Redis-family 产品配置转换为适配器内部 RDB/AOF 目标。
///
/// 参数:
/// - `enabled`: 是否由 NARIX 动态编排持久化 owner。
/// - `config`: 当前产品专属的 RDB/AOF 配置。
/// - `profile`: 用于错误上下文的已认证后端档案。
///
/// 返回: 持久化关闭时返回保守默认值；开启时返回完整产品目标。
fn redis_family_persistence_settings(
    enabled: bool,
    config: Option<&RedisHaPersistenceConfig>,
    profile: BackendProfile,
) -> anyhow::Result<RedisPersistenceSettings> {
    if !enabled {
        return Ok(RedisPersistenceSettings::default());
    }
    let config = config.with_context(|| {
        format!(
            "enabled {} HA persistence configuration is missing",
            profile.as_str()
        )
    })?;
    let save_rules = if config.rdb.enabled {
        config
            .rdb
            .save_rules
            .iter()
            .map(|rule| format!("{} {}", rule.seconds, rule.changes))
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        String::new()
    };
    RedisPersistenceSettings::new(
        save_rules,
        config.aof.enabled,
        config.aof.fsync.clone(),
        config.verify_timeout_ms,
    )
}
