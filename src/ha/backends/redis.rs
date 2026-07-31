use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use tracing::info;

use crate::backend::BackendProfile;
use crate::config::ProxyItem;
use crate::ha::backend::{HaActionAuthority, ReplicationHaBackend};
use crate::ha::control_client::{HaControlClient, HaGenerationGuard};
use crate::ha::model::{format_host_port, split_host_port, HaNode, HaNodeObservation, HaRole};
use crate::ha::persistence::{
    CheckpointReason, CheckpointStatus, CheckpointToken, DynamicPersistenceBackend,
    PersistenceApplyResult, PersistenceCapabilities, PersistenceState, PersistenceTarget,
};
use crate::ha::protocol::{
    ensure_accepted_response, ensure_ok, parse_bulk_string, parse_config_get_value,
    parse_info_string, parse_info_u64, parse_integer_response, parse_replica_master_addr,
    parse_time_seconds,
};

/// Redis 协议家族的 RDB/AOF 动态持久化目标配置。
///
/// 配置只属于经过逐版本认证的产品适配器，通用 HA 状态机不识别 `save`、`appendonly` 或
/// `appendfsync` 等产品字段。
#[derive(Debug, Clone)]
pub(crate) struct RedisPersistenceSettings {
    rdb_save_rules: String,
    aof_enabled: bool,
    aof_fsync: String,
    verify_timeout_ms: u64,
}

impl RedisPersistenceSettings {
    /// 业务作用: 构造 Redis owner 节点必须生效的 RDB/AOF 配置。
    ///
    /// 参数:
    /// - `rdb_save_rules`: Redis `save` 配置值；空字符串表示关闭周期 RDB。
    /// - `aof_enabled`: owner 节点是否启用 AOF。
    /// - `aof_fsync`: AOF 启用时使用的 `always`、`everysec` 或 `no`。
    /// - `verify_timeout_ms`: 强制持久化点完成校验的最长等待毫秒数。
    pub(crate) fn new(
        rdb_save_rules: String,
        aof_enabled: bool,
        aof_fsync: String,
        verify_timeout_ms: u64,
    ) -> anyhow::Result<Self> {
        if rdb_save_rules.trim().is_empty() && !aof_enabled {
            bail!("RDB/AOF HA persistence must enable RDB or AOF");
        }
        if !matches!(aof_fsync.as_str(), "always" | "everysec" | "no") {
            bail!("RDB/AOF appendfsync must be always, everysec, or no");
        }
        if verify_timeout_ms == 0 {
            bail!("RDB/AOF persistence verify timeout must be > 0");
        }
        Ok(Self {
            rdb_save_rules,
            aof_enabled,
            aof_fsync,
            verify_timeout_ms,
        })
    }
}

impl Default for RedisPersistenceSettings {
    /// 业务作用: 返回 Redis HA 的保守持久化默认值。
    fn default() -> Self {
        Self {
            rdb_save_rules: "300 10".to_string(),
            aof_enabled: true,
            aof_fsync: "everysec".to_string(),
            verify_timeout_ms: 10_000,
        }
    }
}

/// 经过逐版本认证的 Redis 协议家族托管复制 HA 适配器内核。
///
/// 当前仅允许 Redis 8.2.2、Valkey 9.1.1、Redict 7.3.6 与 KeyDB 6.3.4 构造该类型。
/// 它们共享已经实测一致的角色探测、`FAILOVER TO`、`REPLICAOF` 和 RDB/AOF 命令，
/// 但仍由封闭驱动保留独立产品身份。
#[derive(Debug, Clone)]
pub(crate) struct RedisHaBackend {
    proxy_name: String,
    profile: BackendProfile,
    control: HaControlClient,
    persistence: RedisPersistenceSettings,
}

impl RedisHaBackend {
    /// 业务作用: 从配置、产品持久化参数和共享运行代令牌构造 Redis-family 适配器。
    ///
    /// 参数:
    /// - `item`: 提供后端认证、RESP TLS、连接超时和探测超时的代理配置。
    /// - `persistence`: owner 和 standby 节点的 RDB/AOF 目标配置。
    /// - `generation_guard`: 与所属 replication HA 运行代共享的退休状态。
    pub(crate) fn from_proxy_item(
        item: &ProxyItem,
        persistence: RedisPersistenceSettings,
        generation_guard: HaGenerationGuard,
    ) -> anyhow::Result<Self> {
        if !matches!(
            item.backend_profile,
            BackendProfile::Redis
                | BackendProfile::Valkey
                | BackendProfile::Redict
                | BackendProfile::KeyDb
        ) {
            bail!(
                "backend_profile={} is not certified by the Redis-family HA adapter",
                item.backend_profile.as_str()
            );
        }
        Ok(Self {
            proxy_name: item.name.clone(),
            profile: item.backend_profile,
            control: HaControlClient::from_proxy_item(item, generation_guard),
            persistence,
        })
    }

    /// 业务作用: 返回错误、日志和运维状态使用的稳定产品名称。
    fn product_name(&self) -> &'static str {
        match self.profile {
            BackendProfile::Redis => "Redis",
            BackendProfile::Valkey => "Valkey",
            BackendProfile::Redict => "Redict",
            BackendProfile::KeyDb => "KeyDB",
            _ => unreachable!("constructor restricts Redis-family HA profiles"),
        }
    }

    /// 业务作用: 读取 Redis `INFO replication` 并转换成通用 HA 观测。
    ///
    /// 参数:
    /// - `node`: 要探测角色、上游和同步状态的 Redis 节点。
    async fn inspect_replication(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        let frame = self
            .control
            .send_probe_command(node, &[b"INFO".as_slice(), b"replication".as_slice()])
            .await?;
        let info = parse_bulk_string(&frame)?;
        let role = parse_role(&info);
        Ok(HaNodeObservation {
            role,
            upstream_addr: parse_replica_master_addr(&info),
            replica_lag_seconds: parse_replica_lag_seconds(&info, role),
            link_down_since_seconds: parse_replica_link_down_seconds(&info, role),
        })
    }

    /// 业务作用: 确认 Redis 主节点使用与 NARIX 相同的复制地址登记计划切换目标。
    ///
    /// 参数:
    /// - `current_primary`: 执行 `FAILOVER TO` 的当前 Redis 主节点。
    /// - `candidate`: 必须出现在主节点在线副本列表中的切换目标。
    async fn verify_failover_target_announced(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
    ) -> anyhow::Result<()> {
        let frame = self
            .control
            .send_probe_command(
                current_primary,
                &[b"INFO".as_slice(), b"replication".as_slice()],
            )
            .await?;
        let info = parse_bulk_string(&frame)?;
        let announced = parse_online_replica_addrs(&info);
        if announced
            .iter()
            .any(|addr| host_port_eq(addr, &candidate.replication_addr))
        {
            return Ok(());
        }
        let observed = if announced.is_empty() {
            "none".to_string()
        } else {
            announced.join(",")
        };
        bail!(
            "{} primary `{}` does not advertise candidate replication address `{}` as an online replica; observed `{observed}`; configure replica-announce-ip and replica-announce-port to match nodes replication_addr",
            self.product_name(),
            current_primary.client_addr,
            candidate.replication_addr
        )
    }

    /// 业务作用: 在探测超时窗口内等待 Redis 节点达到指定角色。
    ///
    /// 参数:
    /// - `node`: 要持续探测的 Redis 节点。
    /// - `expected`: 控制动作完成后必须达到的角色。
    /// - `purpose`: 写入错误上下文的控制动作名称。
    async fn wait_for_role(
        &self,
        node: &HaNode,
        expected: HaRole,
        purpose: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let timeout = self.role_transition_timeout();
        let deadline = Instant::now() + timeout;
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while waiting for a Redis-family role transition")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.role == expected => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} node `{}` did not become {} after {purpose}; last probe error: {}",
                    self.product_name(),
                    node.client_addr,
                    expected.as_str(),
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 业务作用: 返回角色变化和复制确认使用的有界等待时间。
    fn role_transition_timeout(&self) -> Duration {
        Duration::from_millis(
            self.control
                .probe_request_timeout_ms()
                .saturating_mul(5)
                .clamp(1_000, 30_000),
        )
    }

    /// 业务作用: 执行 `REPLICAOF NO ONE` 并等待候选节点成为主节点。
    ///
    /// 参数:
    /// - `candidate`: 已通过通用状态机安全门禁的 Redis 副本。
    async fn promote_candidate(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        authority
            .ensure_active("before Redis-family REPLICAOF NO ONE")
            .await?;
        let frame = self
            .control
            .send_command(
                candidate,
                &[b"REPLICAOF".as_slice(), b"NO".as_slice(), b"ONE".as_slice()],
            )
            .await?;
        ensure_ok(&frame).with_context(|| format!("promote `{}`", candidate.client_addr))?;
        self.wait_for_role(candidate, HaRole::Master, "emergency promotion", authority)
            .await?;
        info!(
            proxy = %self.proxy_name,
            backend_profile = self.profile.as_str(),
            node = %candidate.client_addr,
            "Redis-family replica promoted to master"
        );
        Ok(())
    }

    /// 业务作用: 读取一个 Redis 运行时配置值。
    ///
    /// 参数:
    /// - `node`: 要查询配置的 Redis 节点。
    /// - `name`: Redis 原生配置项名称。
    async fn config_get(&self, node: &HaNode, name: &str) -> anyhow::Result<String> {
        let frame = self
            .control
            .send_command(
                node,
                &[b"CONFIG".as_slice(), b"GET".as_slice(), name.as_bytes()],
            )
            .await?;
        parse_config_get_value(&frame)
    }

    /// 业务作用: 写入一个 Redis 运行时配置值。
    ///
    /// 参数:
    /// - `node`: 接收动态配置的 Redis 节点。
    /// - `name`: Redis 原生配置项名称。
    /// - `value`: 要立即生效的配置值。
    async fn config_set(&self, node: &HaNode, name: &str, value: &str) -> anyhow::Result<()> {
        let frame = self
            .control
            .send_command(
                node,
                &[
                    b"CONFIG".as_slice(),
                    b"SET".as_slice(),
                    name.as_bytes(),
                    value.as_bytes(),
                ],
            )
            .await?;
        ensure_ok(&frame)
    }

    /// 业务作用: 幂等收敛一个 Redis 运行时配置并重新读取确认。
    ///
    /// 参数:
    /// - `node`: 要动态配置的 Redis 节点。
    /// - `name`: Redis 原生配置项名称。
    /// - `expected`: HA 持久化策略要求的最终值。
    async fn reconcile_config_value(
        &self,
        node: &HaNode,
        name: &str,
        expected: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        if self.config_get(node, name).await? == expected {
            return Ok(());
        }
        authority
            .ensure_active("before changing Redis-family runtime configuration")
            .await?;
        self.config_set(node, name, expected).await?;
        let actual = self.config_get(node, name).await?;
        if actual != expected {
            bail!(redis_config_mismatch_message(
                self.product_name(),
                name,
                &node.client_addr,
                expected,
                &actual,
            ));
        }
        Ok(())
    }

    /// 业务作用: 在多配置持久化动作失败后恢复调用前的原始 Redis-family 配置。
    /// 每条补偿写仍受同一动作权威约束；失权时宁可保留中间态交给新 owner 收敛，
    /// 也不允许旧 owner 继续写入。
    async fn restore_persistence_config(
        &self,
        node: &HaNode,
        save: &str,
        appendonly: &str,
        appendfsync: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.reconcile_config_value(node, "appendfsync", appendfsync, authority)
            .await?;
        self.reconcile_config_value(node, "save", save, authority)
            .await?;
        self.reconcile_config_value(node, "appendonly", appendonly, authority)
            .await
    }

    /// 业务作用: 读取 Redis 持久化运行状态。
    ///
    /// 参数:
    /// - `node`: 要读取 `INFO persistence` 的 Redis 节点。
    async fn persistence_info(&self, node: &HaNode) -> anyhow::Result<String> {
        let frame = self
            .control
            .send_probe_command(node, &[b"INFO".as_slice(), b"persistence".as_slice()])
            .await?;
        parse_bulk_string(&frame)
    }

    /// 业务作用: 发送 Redis 持久化命令并拒绝错误或空响应。
    ///
    /// 参数:
    /// - `node`: 执行持久化命令的 Redis 节点。
    /// - `args`: 已按 RESP 参数边界拆分的命令。
    async fn persistence_command(&self, node: &HaNode, args: &[&[u8]]) -> anyhow::Result<()> {
        let frame = self.control.send_command(node, args).await?;
        ensure_accepted_response(&frame)
    }

    /// 业务作用: 读取后端最近一次成功 RDB 持久化的服务端纪元秒。
    ///
    /// 参数:
    /// - `node`: 要查询 `LASTSAVE` 的 KeyDB 节点。
    async fn lastsave(&self, node: &HaNode) -> anyhow::Result<u64> {
        let frame = self
            .control
            .send_command(node, &[b"LASTSAVE".as_slice()])
            .await?;
        parse_integer_response(&frame, "LASTSAVE")
    }

    /// 业务作用: 等待 KeyDB 服务端时间越过旧 `LASTSAVE` 秒，保证新持久化点拥有可区分标记。
    ///
    /// 参数:
    /// - `node`: 要查询 `TIME` 的 KeyDB 节点。
    /// - `previous_lastsave`: 触发新持久化点前读取的 `LASTSAVE` 秒。
    async fn wait_for_checkpoint_epoch(
        &self,
        node: &HaNode,
        previous_lastsave: u64,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_millis(self.persistence.verify_timeout_ms);
        loop {
            authority
                .ensure_active("while waiting for the KeyDB checkpoint epoch")
                .await?;
            let frame = self
                .control
                .send_command(node, &[b"TIME".as_slice()])
                .await?;
            if parse_time_seconds(&frame)? > previous_lastsave {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "KeyDB server time on `{}` did not advance beyond LASTSAVE before timeout",
                    node.client_addr
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// 业务作用: 使用 KeyDB `LASTSAVE` 标记触发一个可验证的 RDB 持久化点。
    ///
    /// 参数:
    /// - `node`: 执行强制持久化的 KeyDB 节点。
    /// - `reason`: 本次持久化动作的 HA 业务原因。
    async fn force_keydb_checkpoint(
        &self,
        node: &HaNode,
        reason: CheckpointReason,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointToken> {
        let previous_lastsave = self.lastsave(node).await?;
        self.wait_for_checkpoint_epoch(node, previous_lastsave, authority)
            .await?;
        info!(
            proxy = %self.proxy_name,
            backend_profile = self.profile.as_str(),
            node = %node.client_addr,
            reason = ?reason,
            previous_lastsave,
            "KeyDB persistence checkpoint starts"
        );
        authority
            .ensure_active("before starting the KeyDB persistence checkpoint")
            .await?;
        self.persistence_command(node, &[b"BGSAVE".as_slice(), b"SCHEDULE".as_slice()])
            .await?;
        Ok(CheckpointToken {
            backend_marker: Some(previous_lastsave.to_string()),
        })
    }

    /// 业务作用: 等待 KeyDB `LASTSAVE` 严格增长，确认本次 RDB 持久化点已经完成。
    ///
    /// 参数:
    /// - `node`: 生成持久化点的 KeyDB 节点。
    /// - `token`: 强制持久化前读取的 `LASTSAVE` 服务端纪元秒。
    async fn verify_keydb_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus> {
        let previous_lastsave = token
            .backend_marker
            .as_deref()
            .context("KeyDB checkpoint token is missing LASTSAVE marker")?
            .parse::<u64>()
            .context("KeyDB checkpoint token contains invalid LASTSAVE marker")?;
        let deadline = Instant::now() + Duration::from_millis(self.persistence.verify_timeout_ms);
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while verifying the KeyDB persistence checkpoint")
                .await?;
            match self.lastsave(node).await {
                Ok(lastsave) if lastsave > previous_lastsave => {
                    return Ok(CheckpointStatus::Complete);
                }
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "KeyDB checkpoint on `{}` did not advance LASTSAVE within {}ms; last probe error: {}",
                    node.client_addr,
                    self.persistence.verify_timeout_ms,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 业务作用: 判断运行时配置是否等于 Redis owner 的 RDB/AOF 目标。
    ///
    /// 参数:
    /// - `save`: 当前 Redis `save` 配置值。
    /// - `appendonly`: 当前 Redis `appendonly` 配置值。
    /// - `appendfsync`: 当前 Redis `appendfsync` 配置值。
    fn is_owner_persistence(&self, save: &str, appendonly: &str, appendfsync: &str) -> bool {
        save == self.persistence.rdb_save_rules
            && appendonly
                == if self.persistence.aof_enabled {
                    "yes"
                } else {
                    "no"
                }
            && (!self.persistence.aof_enabled || appendfsync == self.persistence.aof_fsync)
    }
}

impl ReplicationHaBackend for RedisHaBackend {
    /// 业务作用: 返回构造时已经完成版本认证的产品档案。
    fn profile(&self) -> BackendProfile {
        self.profile
    }

    /// 业务作用: 查询 Redis 节点角色、上游和同步门禁状态。
    ///
    /// 参数:
    /// - `node`: 要探测的 Redis 节点。
    async fn inspect_node(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        self.inspect_replication(node).await
    }

    /// 业务作用: 使用 Redis 原生 `FAILOVER TO` 执行有旧主参与的计划切换。
    ///
    /// 参数:
    /// - `current_primary`: 当前健康且可协调切换的 Redis 主节点。
    /// - `candidate`: 要接管写流量的 Redis 副本。
    async fn planned_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.verify_failover_target_announced(current_primary, candidate)
            .await?;
        let (host, port) = split_host_port(&candidate.replication_addr)?;
        let timeout_ms = self.role_transition_timeout().as_millis().to_string();
        authority
            .ensure_active("before Redis-family FAILOVER TO")
            .await?;
        let frame = self
            .control
            .send_command(
                current_primary,
                &[
                    b"FAILOVER".as_slice(),
                    b"TO".as_slice(),
                    host.as_bytes(),
                    port.as_bytes(),
                    b"TIMEOUT".as_slice(),
                    timeout_ms.as_bytes(),
                ],
            )
            .await?;
        ensure_ok(&frame).with_context(|| {
            format!(
                "start {} failover from `{}` to `{}`",
                self.product_name(),
                current_primary.client_addr,
                candidate.replication_addr
            )
        })?;
        self.wait_for_role(candidate, HaRole::Master, "planned failover", authority)
            .await?;
        self.verify_replication(current_primary, candidate, authority)
            .await
            .with_context(|| {
                format!(
                    "verify old {} primary `{}` follows new primary `{}`",
                    self.product_name(),
                    current_primary.client_addr,
                    candidate.client_addr
                )
            })?;
        info!(
            proxy = %self.proxy_name,
            backend_profile = self.profile.as_str(),
            current_primary = %current_primary.client_addr,
            candidate = %candidate.client_addr,
            "Redis-family planned switchover completed"
        );
        Ok(())
    }

    /// 业务作用: 在旧主完成 fencing 后紧急提升 Redis 副本。
    ///
    /// 参数:
    /// - `candidate`: 已通过候选门禁的 Redis 副本。
    async fn emergency_promote(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.promote_candidate(candidate, authority).await
    }

    /// 业务作用: 写入复制凭据并使用 `REPLICAOF` 把节点收敛为副本。
    ///
    /// 参数:
    /// - `node`: 要改为副本的 Redis 节点。
    /// - `primary`: 该节点必须跟随的 Redis 主节点。
    async fn configure_replica(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.reconcile_config_value(
            node,
            "masteruser",
            self.control.username().unwrap_or_default(),
            authority,
        )
        .await
        .with_context(|| format!("set `{}` masteruser", node.client_addr))?;
        self.reconcile_config_value(
            node,
            "masterauth",
            self.control.password().unwrap_or_default(),
            authority,
        )
        .await
        .with_context(|| format!("set `{}` masterauth", node.client_addr))?;
        let (host, port) = split_host_port(&primary.replication_addr)?;
        authority
            .ensure_active("before Redis-family REPLICAOF")
            .await?;
        let frame = self
            .control
            .send_command(
                node,
                &[b"REPLICAOF".as_slice(), host.as_bytes(), port.as_bytes()],
            )
            .await?;
        ensure_ok(&frame).with_context(|| {
            format!(
                "set {} `{}` as replica of `{}`",
                self.product_name(),
                node.client_addr,
                primary.replication_addr
            )
        })?;
        info!(
            proxy = %self.proxy_name,
            backend_profile = self.profile.as_str(),
            node = %node.client_addr,
            primary = %primary.replication_addr,
            "Redis-family node configured as replica"
        );
        Ok(())
    }

    /// 业务作用: 等待 Redis 节点稳定跟随指定主节点并完成全量同步。
    ///
    /// 参数:
    /// - `node`: 要校验的 Redis 副本。
    /// - `primary`: 期望的上游 Redis 主节点。
    async fn verify_replication(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.role_transition_timeout();
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while verifying Redis-family replication")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.eligible_replica_of(primary, 0) => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} node `{}` did not follow `{}` before timeout; last probe error: {}",
                    self.product_name(),
                    node.client_addr,
                    primary.replication_addr,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl DynamicPersistenceBackend for RedisHaBackend {
    /// 业务作用: 声明 Redis 运行时 RDB/AOF 和强制持久化点能力。
    fn persistence_capabilities(&self) -> PersistenceCapabilities {
        PersistenceCapabilities {
            runtime_reconfigure: true,
            force_checkpoint: true,
            verify_checkpoint: true,
            restart_durable: false,
        }
    }

    /// 业务作用: 读取 Redis `save`、`appendonly` 和 `appendfsync` 的实际状态。
    ///
    /// 参数:
    /// - `node`: 要查询动态持久化状态的 Redis 节点。
    /// - `role`: 当前节点角色；Redis 配置不按角色展开，但保留通用接口参数。
    async fn inspect_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
    ) -> anyhow::Result<PersistenceState> {
        let _role = role;
        let save = self.config_get(node, "save").await?;
        let appendonly = self.config_get(node, "appendonly").await?;
        let appendfsync = self.config_get(node, "appendfsync").await?;
        let target = if self.is_owner_persistence(&save, &appendonly, &appendfsync) {
            Some(PersistenceTarget::Owner)
        } else if save.is_empty() && appendonly == "no" {
            Some(PersistenceTarget::Suspended)
        } else {
            None
        };
        Ok(PersistenceState {
            target,
            effective: target.is_some(),
            restart_durable: false,
        })
    }

    /// 业务作用: 把 Redis 节点幂等收敛到 owner 或暂停持久化状态。
    ///
    /// 参数:
    /// - `node`: 要动态配置 RDB/AOF 的 Redis 节点。
    /// - `role`: 当前节点角色；Redis 配置不按角色展开，但保留通用接口参数。
    /// - `target`: 通用状态机要求的持久化职责。
    async fn reconcile_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
        target: PersistenceTarget,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<PersistenceApplyResult> {
        let original_save = self.config_get(node, "save").await?;
        let original_appendonly = self.config_get(node, "appendonly").await?;
        let original_appendfsync = self.config_get(node, "appendfsync").await?;
        let apply_result = async {
            match target {
                PersistenceTarget::Owner => {
                    if self.persistence.aof_enabled {
                        self.reconcile_config_value(
                            node,
                            "appendfsync",
                            &self.persistence.aof_fsync,
                            authority,
                        )
                        .await?;
                    }
                    self.reconcile_config_value(
                        node,
                        "save",
                        &self.persistence.rdb_save_rules,
                        authority,
                    )
                    .await?;
                    self.reconcile_config_value(
                        node,
                        "appendonly",
                        if self.persistence.aof_enabled {
                            "yes"
                        } else {
                            "no"
                        },
                        authority,
                    )
                    .await?;
                }
                PersistenceTarget::Standby | PersistenceTarget::Suspended => {
                    self.reconcile_config_value(node, "appendonly", "no", authority)
                        .await?;
                    self.reconcile_config_value(node, "save", "", authority)
                        .await?;
                }
            }
            let state = self.inspect_persistence(node, role).await?;
            let matches_target = match target {
                PersistenceTarget::Owner => state.target == Some(PersistenceTarget::Owner),
                PersistenceTarget::Standby | PersistenceTarget::Suspended => {
                    state.target == Some(PersistenceTarget::Suspended)
                }
            };
            if !state.effective || !matches_target {
                bail!(
                    "{} persistence on `{}` did not converge to {:?}",
                    self.product_name(),
                    node.client_addr,
                    target
                );
            }
            Ok(PersistenceApplyResult {
                effective: true,
                restart_durable: false,
            })
        }
        .await;
        match apply_result {
            Ok(result) => Ok(result),
            Err(apply_error) => match self
                .restore_persistence_config(
                    node,
                    &original_save,
                    &original_appendonly,
                    &original_appendfsync,
                    authority,
                )
                .await
            {
                Ok(()) => Err(apply_error),
                Err(rollback_error) => Err(anyhow::anyhow!(
                    "{apply_error:#}; failed to restore the previous {} persistence configuration on `{}`: {rollback_error:#}",
                    self.product_name(),
                    node.client_addr
                )),
            },
        }
    }

    /// 业务作用: 使用 `BGSAVE SCHEDULE` 触发可验证的 Redis RDB 持久化点。
    ///
    /// 参数:
    /// - `node`: 执行强制持久化的 Redis 节点。
    /// - `reason`: 本次持久化动作的 HA 业务原因。
    async fn force_checkpoint(
        &self,
        node: &HaNode,
        reason: CheckpointReason,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointToken> {
        if self.profile == BackendProfile::KeyDb {
            return self.force_keydb_checkpoint(node, reason, authority).await;
        }
        let before = self.persistence_info(node).await?;
        let saves = parse_info_u64(&before, "rdb_saves").with_context(|| {
            format!(
                "{} INFO persistence did not report rdb_saves",
                self.product_name()
            )
        })?;
        info!(
            proxy = %self.proxy_name,
            backend_profile = self.profile.as_str(),
            node = %node.client_addr,
            reason = ?reason,
            rdb_saves = saves,
            "Redis-family persistence checkpoint starts"
        );
        authority
            .ensure_active("before starting the Redis-family persistence checkpoint")
            .await?;
        self.persistence_command(node, &[b"BGSAVE".as_slice(), b"SCHEDULE".as_slice()])
            .await?;
        Ok(CheckpointToken {
            backend_marker: Some(saves.to_string()),
        })
    }

    /// 业务作用: 等待 Redis `rdb_saves` 增长且最近一次后台保存成功。
    ///
    /// 参数:
    /// - `node`: 生成持久化点的 Redis 节点。
    /// - `token`: `force_checkpoint` 返回的原始 `rdb_saves` 计数。
    async fn verify_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus> {
        if self.profile == BackendProfile::KeyDb {
            return self.verify_keydb_checkpoint(node, token, authority).await;
        }
        let previous_saves = token
            .backend_marker
            .as_deref()
            .with_context(|| {
                format!(
                    "{} checkpoint token is missing rdb_saves",
                    self.product_name()
                )
            })?
            .parse::<u64>()
            .with_context(|| {
                format!(
                    "{} checkpoint token contains invalid rdb_saves",
                    self.product_name()
                )
            })?;
        let deadline = Instant::now() + Duration::from_millis(self.persistence.verify_timeout_ms);
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while verifying the Redis-family persistence checkpoint")
                .await?;
            match self.persistence_info(node).await {
                Ok(info) => {
                    last_error = None;
                    let saves = parse_info_u64(&info, "rdb_saves").unwrap_or(previous_saves);
                    let in_progress = parse_info_u64(&info, "rdb_bgsave_in_progress").unwrap_or(1);
                    let succeeded =
                        parse_info_string(&info, "rdb_last_bgsave_status").as_deref() == Some("ok");
                    if saves > previous_saves && in_progress == 0 && succeeded {
                        return Ok(CheckpointStatus::Complete);
                    }
                }
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "{} checkpoint on `{}` did not complete within {}ms; last probe error: {}",
                    self.product_name(),
                    node.client_addr,
                    self.persistence.verify_timeout_ms,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// 业务作用: 生成 Redis-family 配置写后复验失败信息，并阻止复制凭据进入日志、状态接口或错误链。
/// 参数说明:
/// - `product`: 当前后端产品名称。
/// - `name`: Redis 原生配置键。
/// - `node_addr`: 配置目标节点地址。
/// - `expected`: 期望写入值。
/// - `actual`: 写后读回值。
///
/// 返回: 敏感键只描述未收敛事实；非敏感键保留期望值和实际值用于诊断。
fn redis_config_mismatch_message(
    product: &str,
    name: &str,
    node_addr: &str,
    expected: &str,
    actual: &str,
) -> String {
    if matches!(
        name.to_ascii_lowercase().as_str(),
        "masterauth" | "masteruser" | "requirepass"
    ) {
        return format!(
            "{product} sensitive config `{name}` on `{node_addr}` did not converge after update"
        );
    }
    format!(
        "{product} config `{name}` on `{node_addr}` expected `{expected}` but observed `{actual}`"
    )
}

/// 业务作用: 从 Redis `INFO replication` 文本识别节点角色。
///
/// 参数:
/// - `info`: Redis 返回的 replication 信息文本。
fn parse_role(info: &str) -> HaRole {
    match parse_info_string(info, "role").as_deref() {
        Some("master") => HaRole::Master,
        Some("slave" | "replica") => HaRole::Replica,
        _ => HaRole::Unknown,
    }
}

/// 业务作用: 解析 Redis 主节点登记的全部在线副本复制地址。
///
/// 参数:
/// - `info`: Redis `INFO replication` 文本。
///
/// 返回: 已按 `host:port` 或 `[IPv6]:port` 规范化的在线副本地址。
fn parse_online_replica_addrs(info: &str) -> Vec<String> {
    info.lines()
        .filter_map(|line| {
            let (name, fields) = line.trim_end_matches('\r').split_once(':')?;
            if !name.starts_with("slave") && !name.starts_with("replica") {
                return None;
            }
            let fields = fields
                .split(',')
                .filter_map(|field| field.split_once('='))
                .collect::<HashMap<_, _>>();
            if fields.get("state").copied() != Some("online") {
                return None;
            }
            let host = fields.get("ip")?;
            let port = fields.get("port")?;
            Some(format_host_port(host, port))
        })
        .collect()
}

/// 业务作用: 比较两个已经包含端口的后端地址，兼容 IPv6 方括号和主机名大小写。
///
/// 参数:
/// - `left`: 第一个 `host:port` 或 `[IPv6]:port` 地址。
/// - `right`: 第二个 `host:port` 或 `[IPv6]:port` 地址。
///
/// 返回: 主机名不区分 ASCII 大小写且端口完全一致时返回真。
fn host_port_eq(left: &str, right: &str) -> bool {
    let Ok((left_host, left_port)) = split_host_port(left) else {
        return false;
    };
    let Ok((right_host, right_port)) = split_host_port(right) else {
        return false;
    };
    left_port == right_port && left_host.eq_ignore_ascii_case(right_host)
}

/// 业务作用: 把 Redis 协议家族副本同步状态转换成候选门禁值。
///
/// Redis 与 Valkey 都提供副本和主节点复制 offset。两者相等是零积压的直接证据，
/// 不受空闲链路心跳周期影响；offset 不等时至少返回一秒，禁止 `require_zero_lag`
/// 把尚未追平的副本选为持久化 owner。旧版本没有 offset 时才回退到心跳年龄。
///
/// 参数:
/// - `info`: Redis 返回的 replication 信息文本。
/// - `role`: 已解析的 Redis 节点角色。
fn parse_replica_lag_seconds(info: &str, role: HaRole) -> Option<u64> {
    if role != HaRole::Replica {
        return None;
    }
    if parse_info_u64(info, "master_sync_in_progress") == Some(1) {
        return Some(u64::MAX);
    }
    match parse_info_string(info, "master_link_status").as_deref() {
        Some("up") => match (
            parse_info_u64(info, "slave_repl_offset")
                .or_else(|| parse_info_u64(info, "replica_repl_offset"))
                .or_else(|| parse_info_u64(info, "slave_read_repl_offset")),
            parse_info_u64(info, "master_repl_offset"),
        ) {
            (Some(replica), Some(master)) if replica == master => Some(0),
            (Some(_), Some(_)) => parse_info_u64(info, "master_last_io_seconds_ago")
                .map_or(Some(1), |lag| Some(lag.saturating_sub(1).max(1))),
            _ => {
                parse_info_u64(info, "master_last_io_seconds_ago").map(|lag| lag.saturating_sub(1))
            }
        },
        Some("down") => Some(u64::MAX),
        _ => None,
    }
}

/// 业务作用: 解析副本与主节点断链的已持续秒数，供切主候选门禁使用。
///
/// Redis 家族在 `master_link_status:down` 时提供 `master_link_down_since_seconds`；
/// 个别版本缺失该字段时回退到非负的 `master_last_io_seconds_ago`。全量同步中的
/// 副本数据集不完整，即便断链也不提供断链证据，保持不可提升。
///
/// 参数:
/// - `info`: Redis 返回的 replication 信息文本。
/// - `role`: 已解析的 Redis 节点角色。
fn parse_replica_link_down_seconds(info: &str, role: HaRole) -> Option<u64> {
    if role != HaRole::Replica {
        return None;
    }
    if parse_info_u64(info, "master_sync_in_progress") == Some(1) {
        return None;
    }
    if parse_info_string(info, "master_link_status").as_deref() != Some("down") {
        return None;
    }
    parse_info_u64(info, "master_link_down_since_seconds")
        .or_else(|| parse_info_u64(info, "master_last_io_seconds_ago"))
}
