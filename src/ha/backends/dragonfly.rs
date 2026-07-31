use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context};
use tracing::info;

use crate::backend::BackendProfile;
use crate::config::ProxyItem;
use crate::ha::backend::{HaActionAuthority, ReplicationHaBackend};
use crate::ha::control_client::{HaControlClient, HaGenerationGuard};
use crate::ha::model::{split_host_port, HaNode, HaNodeObservation, HaRole};
use crate::ha::persistence::{
    CheckpointReason, CheckpointStatus, CheckpointToken, DynamicPersistenceBackend,
    PersistenceApplyResult, PersistenceCapabilities, PersistenceState, PersistenceTarget,
};
use crate::ha::protocol::{
    ensure_accepted_response, ensure_ok, parse_bulk_string, parse_config_get_value,
    parse_info_string, parse_info_u64, parse_integer_response, parse_replica_master_addr,
    parse_time_seconds,
};

/// Dragonfly 原生动态持久化参数。
///
/// 这些字段只存在于 Dragonfly 适配器，通用 HA 状态机不识别 `snapshot_cron`。
#[derive(Debug, Clone)]
struct DragonflyPersistenceSettings {
    snapshot_cron: String,
    dbfilename: String,
    dir: Option<String>,
    verify_timeout_ms: u64,
    owner_id: String,
}

/// Dragonfly 托管复制 HA 适配器。
///
/// 该类型负责 Dragonfly 命令、复制响应解析和原生动态持久化；通用运行时只调用 trait。
#[derive(Debug, Clone)]
pub(crate) struct DragonflyHaBackend {
    proxy_name: String,
    control: HaControlClient,
    persistence: DragonflyPersistenceSettings,
}

impl DragonflyHaBackend {
    /// 业务作用: 从配置和共享运行代令牌构造 Dragonfly 产品适配器。
    ///
    /// 参数:
    /// - `item`: 提供节点认证、TLS、探测超时和 Dragonfly 持久化配置的代理项。
    /// - `owner_id`: coordinator 为当前 NARIX 实例生成的持久化 owner 标识。
    /// - `generation_guard`: 与所属 replication HA 运行代共享的退休状态。
    pub(crate) fn from_proxy_item(
        item: &ProxyItem,
        owner_id: String,
        generation_guard: HaGenerationGuard,
    ) -> anyhow::Result<Self> {
        let product = item
            .replication_ha_config()?
            .persistence
            .dragonfly
            .clone()
            .unwrap_or_default();
        Ok(Self {
            proxy_name: item.name.clone(),
            control: HaControlClient::from_proxy_item(item, generation_guard),
            persistence: DragonflyPersistenceSettings {
                snapshot_cron: product.snapshot_cron,
                dbfilename: product.dbfilename,
                dir: product.dir,
                verify_timeout_ms: product.verify_timeout_ms,
                owner_id,
            },
        })
    }

    /// 业务作用: 读取 Dragonfly `INFO replication` 并转换成通用观测。
    ///
    /// 参数:
    /// - `node`: 要探测角色和复制进度的 Dragonfly 节点。
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

    /// 业务作用: 执行 Dragonfly `REPLICAOF NO ONE` 并等待角色确认。
    ///
    /// 参数:
    /// - `candidate`: 要提升为主节点的 Dragonfly 副本。
    async fn promote_candidate(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        authority
            .ensure_active("before Dragonfly REPLICAOF NO ONE")
            .await?;
        let frame = self
            .control
            .send_command(
                candidate,
                &[b"REPLICAOF".as_slice(), b"NO".as_slice(), b"ONE".as_slice()],
            )
            .await?;
        ensure_ok(&frame).with_context(|| format!("promote `{}`", candidate.client_addr))?;
        self.wait_for_role(candidate, HaRole::Master, "promote", authority)
            .await?;
        info!(
            proxy = %self.proxy_name,
            node = %candidate.client_addr,
            "Dragonfly replica promoted to master"
        );
        Ok(())
    }

    /// 业务作用: 在超时窗口内等待节点达到期望角色。
    ///
    /// 参数:
    /// - `node`: 要持续探测的 Dragonfly 节点。
    /// - `expected`: 控制动作完成后必须达到的角色。
    /// - `purpose`: 写入错误信息的控制动作名称。
    async fn wait_for_role(
        &self,
        node: &HaNode,
        expected: HaRole,
        purpose: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let timeout = Duration::from_millis(
            self.control
                .probe_request_timeout_ms()
                .saturating_mul(2)
                .clamp(500, 10_000),
        );
        let deadline = Instant::now() + timeout;
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while waiting for a Dragonfly role transition")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.role == expected => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "Dragonfly node `{}` did not become {} after {purpose}; last probe error: {}",
                    node.client_addr,
                    expected.as_str(),
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 业务作用: 向 Dragonfly 写入一个原生持久化配置项。
    ///
    /// 参数:
    /// - `node`: 接收动态配置的 Dragonfly 节点。
    /// - `name`: Dragonfly 原生配置项名称。
    /// - `value`: 要写入的配置值。
    async fn persistence_config_set(
        &self,
        node: &HaNode,
        name: &str,
        value: &str,
    ) -> anyhow::Result<()> {
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
        ensure_accepted_response(&frame)
    }

    /// 业务作用: 从 Dragonfly 读取一个原生持久化配置项。
    ///
    /// 参数:
    /// - `node`: 要查询动态配置的 Dragonfly 节点。
    /// - `name`: Dragonfly 原生配置项名称。
    async fn persistence_config_get(&self, node: &HaNode, name: &str) -> anyhow::Result<String> {
        let frame = self
            .control
            .send_command(
                node,
                &[b"CONFIG".as_slice(), b"GET".as_slice(), name.as_bytes()],
            )
            .await?;
        parse_config_get_value(&frame)
    }

    /// 业务作用: 幂等地收敛一个 Dragonfly 原生配置项并重新读取确认。
    ///
    /// 参数:
    /// - `node`: 要动态配置的 Dragonfly 节点。
    /// - `name`: Dragonfly 原生配置项名称。
    /// - `expected`: HA 策略要求的最终配置值。
    async fn reconcile_config_value(
        &self,
        node: &HaNode,
        name: &str,
        expected: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        if self.persistence_config_get(node, name).await? == expected {
            return Ok(());
        }
        authority
            .ensure_active("before changing Dragonfly persistence configuration")
            .await?;
        self.persistence_config_set(node, name, expected).await?;
        let effective = self.persistence_config_get(node, name).await?;
        if effective != expected {
            bail!(
                "Dragonfly config `{name}` on `{}` expected `{expected}` but observed `{effective}`",
                node.client_addr
            );
        }
        Ok(())
    }

    /// 业务作用: 在 Dragonfly 多配置持久化动作失败后恢复调用前的文件名与快照计划。
    /// 补偿写继续复用原动作权威，失权后不会由旧 owner 执行回滚。
    async fn restore_persistence_config(
        &self,
        node: &HaNode,
        dbfilename: &str,
        snapshot_cron: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.reconcile_config_value(node, "dbfilename", dbfilename, authority)
            .await?;
        self.reconcile_config_value(node, "snapshot_cron", snapshot_cron, authority)
            .await
    }

    /// 业务作用: 校验 Dragonfly 启动时固定的配置值，不尝试发送必然失败的动态修改命令。
    ///
    /// 参数:
    /// - `node`: 要检查启动配置的 Dragonfly 节点。
    /// - `name`: Dragonfly 不支持运行时修改的配置项名称。
    /// - `expected`: NARIX 持久化策略要求节点启动时已经具备的值。
    async fn verify_immutable_config_value(
        &self,
        node: &HaNode,
        name: &str,
        expected: &str,
    ) -> anyhow::Result<()> {
        let actual = self.persistence_config_get(node, name).await?;
        if actual != expected {
            bail!(
                "Dragonfly immutable config `{name}` on `{}` must be `{expected}` at process startup, observed `{actual}`",
                node.client_addr
            );
        }
        Ok(())
    }

    /// 业务作用: 发送一条持久化命令并拒绝错误或空响应。
    ///
    /// 参数:
    /// - `node`: 执行持久化命令的 Dragonfly 节点。
    /// - `args`: 已按 RESP 参数边界拆分的命令。
    async fn persistence_command(&self, node: &HaNode, args: &[&[u8]]) -> anyhow::Result<()> {
        let frame = self.control.send_command(node, args).await?;
        ensure_accepted_response(&frame)
    }

    /// 业务作用: 读取 Dragonfly 最近一次成功持久化的服务端纪元秒。
    ///
    /// 参数:
    /// - `node`: 要查询 `LASTSAVE` 的 Dragonfly 节点。
    async fn lastsave(&self, node: &HaNode) -> anyhow::Result<u64> {
        let frame = self
            .control
            .send_command(node, &[b"LASTSAVE".as_slice()])
            .await?;
        parse_integer_response(&frame, "LASTSAVE")
    }

    /// 业务作用: 等待服务端时钟越过旧 `LASTSAVE` 秒，避免同一秒内两次快照得到相同验证标记。
    ///
    /// 参数:
    /// - `node`: 要查询 `TIME` 的 Dragonfly 节点。
    /// - `previous_lastsave`: 触发新快照前读取的服务端 `LASTSAVE` 秒。
    async fn wait_for_checkpoint_epoch(
        &self,
        node: &HaNode,
        previous_lastsave: u64,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + Duration::from_millis(self.persistence.verify_timeout_ms);
        loop {
            authority
                .ensure_active("while waiting for the Dragonfly checkpoint epoch")
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
                    "Dragonfly server time on `{}` did not advance beyond LASTSAVE before timeout",
                    node.client_addr
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// 业务作用: 展开 Dragonfly 持久化配置模板。
    ///
    /// 参数:
    /// - `template`: 包含 Dragonfly 动态持久化受支持占位符的原始配置值。
    /// - `action`: 当前动态持久化动作名称。
    /// - `node`: 当前动作作用的 Dragonfly 节点。
    /// - `role`: 当前节点角色。
    fn render_persistence_value(
        &self,
        template: &str,
        action: &str,
        node: &HaNode,
        role: HaRole,
    ) -> String {
        template
            .replace("{proxy}", &self.proxy_name)
            .replace("{proxy_safe}", &template_safe(&self.proxy_name))
            .replace("{action}", action)
            .replace("{client_addr}", &node.client_addr)
            .replace("{client_addr_safe}", &template_safe(&node.client_addr))
            .replace("{replication_addr}", &node.replication_addr)
            .replace(
                "{replication_addr_safe}",
                &template_safe(&node.replication_addr),
            )
            .replace("{role}", role.as_str())
            .replace("{owner_id}", &self.persistence.owner_id)
            .replace(
                "{owner_id_safe}",
                &template_safe(&self.persistence.owner_id),
            )
            .replace("{snapshot_cron}", &self.persistence.snapshot_cron)
            .replace("{dbfilename}", &self.persistence.dbfilename)
            .replace("{dir}", self.persistence.dir.as_deref().unwrap_or_default())
            .replace("{timestamp}", &now_ms().to_string())
    }

    /// 业务作用: 计算指定节点承担 owner 时必须生效的 Dragonfly cron。
    ///
    /// 参数:
    /// - `node`: 将承担持久化 owner 的 Dragonfly 节点。
    /// - `role`: owner 当前节点角色。
    fn expected_snapshot_cron(&self, node: &HaNode, role: HaRole) -> String {
        self.render_persistence_value(&self.persistence.snapshot_cron, "enable", node, role)
    }
}

impl ReplicationHaBackend for DragonflyHaBackend {
    /// 业务作用: 返回 Dragonfly 产品档案。
    fn profile(&self) -> BackendProfile {
        BackendProfile::Dragonfly
    }

    /// 业务作用: 查询 Dragonfly 节点角色、上游和复制延迟。
    ///
    /// 参数:
    /// - `node`: 要探测的 Dragonfly 节点。
    async fn inspect_node(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        self.inspect_replication(node).await
    }

    /// 业务作用: 保持现有 Dragonfly 计划切换语义：先提升候选，再由通用状态机降级旧主。
    ///
    /// 参数:
    /// - `current_primary`: 当前健康主节点，用于记录计划切换上下文。
    /// - `candidate`: 要提升的 Dragonfly 副本。
    async fn planned_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        info!(
            proxy = %self.proxy_name,
            current_primary = %current_primary.client_addr,
            candidate = %candidate.client_addr,
            "Dragonfly planned switchover starts"
        );
        self.promote_candidate(candidate, authority).await?;
        self.configure_replica(current_primary, candidate, authority)
            .await
            .with_context(|| {
                format!(
                    "demote old Dragonfly primary `{}` after promoting `{}`",
                    current_primary.client_addr, candidate.client_addr
                )
            })?;
        self.verify_replication(current_primary, candidate, authority)
            .await
            .with_context(|| {
                format!(
                    "verify old Dragonfly primary `{}` follows new primary `{}`",
                    current_primary.client_addr, candidate.client_addr
                )
            })
    }

    /// 业务作用: 在通用状态机完成旧主隔离后提升 Dragonfly 候选。
    ///
    /// 参数:
    /// - `candidate`: 已通过候选门禁的 Dragonfly 副本。
    async fn emergency_promote(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.promote_candidate(candidate, authority).await
    }

    /// 业务作用: 使用 `masterauth` 和 `REPLICAOF` 把 Dragonfly 节点收敛为副本。
    ///
    /// 参数:
    /// - `node`: 要改为副本的 Dragonfly 节点。
    /// - `primary`: 该节点必须跟随的 Dragonfly 主节点。
    async fn configure_replica(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let password = self.control.password().unwrap_or_default();
        authority
            .ensure_active("before changing Dragonfly replication credentials")
            .await?;
        let frame = self
            .control
            .send_command(
                node,
                &[
                    b"CONFIG".as_slice(),
                    b"SET".as_slice(),
                    b"masterauth".as_slice(),
                    password.as_bytes(),
                ],
            )
            .await?;
        ensure_ok(&frame)
            .with_context(|| format!("set `{}` masterauth before REPLICAOF", node.client_addr))?;
        let (host, port) = split_host_port(&primary.replication_addr)?;
        authority
            .ensure_active("before Dragonfly REPLICAOF")
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
                "set `{}` as replica of `{}`",
                node.client_addr, primary.replication_addr
            )
        })?;
        info!(
            proxy = %self.proxy_name,
            node = %node.client_addr,
            master = %primary.replication_addr,
            "Dragonfly node configured as replica"
        );
        Ok(())
    }

    /// 业务作用: 等待 Dragonfly 节点确认成为指定主节点的副本。
    ///
    /// 参数:
    /// - `node`: 要校验的 Dragonfly 副本。
    /// - `primary`: 期望的上游 Dragonfly 主节点。
    async fn verify_replication(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let timeout = Duration::from_millis(
            self.control
                .probe_request_timeout_ms()
                .saturating_mul(2)
                .clamp(500, 10_000),
        );
        let deadline = Instant::now() + timeout;
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while verifying Dragonfly replication")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.eligible_replica_of(primary, 0) => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "Dragonfly node `{}` did not follow `{}` before timeout; last probe error: {}",
                    node.client_addr,
                    primary.replication_addr,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

impl DynamicPersistenceBackend for DragonflyHaBackend {
    /// 业务作用: 声明 Dragonfly 原生动态快照能力。
    fn persistence_capabilities(&self) -> PersistenceCapabilities {
        PersistenceCapabilities {
            runtime_reconfigure: true,
            force_checkpoint: true,
            verify_checkpoint: true,
            restart_durable: false,
        }
    }

    /// 业务作用: 读取 `snapshot_cron` 并映射成通用持久化职责。
    ///
    /// 参数:
    /// - `node`: 要查询的 Dragonfly 节点。
    /// - `role`: 用于展开目标 cron 模板的节点角色。
    async fn inspect_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
    ) -> anyhow::Result<PersistenceState> {
        let actual = self.persistence_config_get(node, "snapshot_cron").await?;
        let expected = self.expected_snapshot_cron(node, role);
        let target = if actual == expected {
            Some(PersistenceTarget::Owner)
        } else if actual.is_empty() {
            Some(PersistenceTarget::Suspended)
        } else {
            None
        };
        if target == Some(PersistenceTarget::Owner) {
            if let Some(dir_template) = self.persistence.dir.as_deref() {
                let expected_dir =
                    self.render_persistence_value(dir_template, "enable", node, role);
                self.verify_immutable_config_value(node, "dir", &expected_dir)
                    .await
                    .with_context(|| format!("verify `{}` persistence dir", node.client_addr))?;
            }
        }
        Ok(PersistenceState {
            target,
            effective: target.is_some(),
            restart_durable: false,
        })
    }

    /// 业务作用: 动态启用或关闭 Dragonfly `snapshot_cron` 并重新读取确认。
    ///
    /// 参数:
    /// - `node`: 要动态配置的 Dragonfly 节点。
    /// - `role`: 用于展开文件名、目录和 cron 模板的节点角色。
    /// - `target`: 通用 HA 状态机要求的持久化职责。
    async fn reconcile_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
        target: PersistenceTarget,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<PersistenceApplyResult> {
        let original_dbfilename = self.persistence_config_get(node, "dbfilename").await?;
        let original_snapshot_cron = self.persistence_config_get(node, "snapshot_cron").await?;
        let apply_result = async {
            match target {
                PersistenceTarget::Owner => {
                    let dbfilename = native_dbfilename(&self.render_persistence_value(
                        &self.persistence.dbfilename,
                        "enable",
                        node,
                        role,
                    ));
                    self.reconcile_config_value(node, "dbfilename", &dbfilename, authority)
                        .await
                        .with_context(|| {
                            format!("set `{}` persistence dbfilename", node.client_addr)
                        })?;
                    if let Some(dir_template) = self.persistence.dir.as_deref() {
                        let dir = self.render_persistence_value(dir_template, "enable", node, role);
                        self.verify_immutable_config_value(node, "dir", &dir)
                            .await
                            .with_context(|| {
                                format!("verify `{}` persistence dir", node.client_addr)
                            })?;
                    }
                    let cron = self.expected_snapshot_cron(node, role);
                    self.reconcile_config_value(node, "snapshot_cron", &cron, authority)
                        .await
                        .with_context(|| {
                            format!("enable `{}` persistence snapshot_cron", node.client_addr)
                        })?;
                }
                PersistenceTarget::Standby | PersistenceTarget::Suspended => {
                    self.reconcile_config_value(node, "snapshot_cron", "", authority)
                        .await
                        .with_context(|| {
                            format!("disable `{}` persistence snapshot_cron", node.client_addr)
                        })?;
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
                    "Dragonfly persistence on `{}` did not converge to {:?}",
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
                    &original_dbfilename,
                    &original_snapshot_cron,
                    authority,
                )
                .await
            {
                Ok(()) => Err(apply_error),
                Err(rollback_error) => Err(anyhow::anyhow!(
                    "{apply_error:#}; failed to restore the previous Dragonfly persistence configuration on `{}`: {rollback_error:#}",
                    node.client_addr
                )),
            },
        }
    }

    /// 业务作用: 通过 `BGSAVE` 创建持久化点，失败时保持现有 `SAVE` 回退。
    ///
    /// 参数:
    /// - `node`: 执行强制快照的 Dragonfly 节点。
    /// - `reason`: 触发快照的 HA 业务原因。
    async fn force_checkpoint(
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
            node = %node.client_addr,
            reason = ?reason,
            "Dragonfly persistence checkpoint starts"
        );
        authority
            .ensure_active("before starting the Dragonfly persistence checkpoint")
            .await?;
        match self
            .persistence_command(node, &[b"BGSAVE".as_slice()])
            .await
        {
            Ok(()) => {}
            Err(background_error) => {
                authority
                    .ensure_active("before falling back to a synchronous Dragonfly checkpoint")
                    .await?;
                self.persistence_command(node, &[b"SAVE".as_slice()])
                    .await
                    .with_context(|| {
                        format!("BGSAVE failed first: {background_error}; SAVE failed")
                    })?;
            }
        }
        Ok(CheckpointToken {
            backend_marker: Some(previous_lastsave.to_string()),
        })
    }

    /// 业务作用: 等待 `LASTSAVE` 严格晚于触发前标记，确认本次 Dragonfly 持久化已经完成。
    ///
    /// 参数:
    /// - `node`: 要验证持久化点的 Dragonfly 节点。
    /// - `token`: 强制快照前读取的 `LASTSAVE` 服务端纪元秒。
    async fn verify_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus> {
        let previous_lastsave = token
            .backend_marker
            .as_deref()
            .context("Dragonfly checkpoint token is missing LASTSAVE marker")?
            .parse::<u64>()
            .context("Dragonfly checkpoint token contains invalid LASTSAVE marker")?;
        let deadline = Instant::now() + Duration::from_millis(self.persistence.verify_timeout_ms);
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while verifying the Dragonfly persistence checkpoint")
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
                    "Dragonfly checkpoint on `{}` did not advance LASTSAVE within {}ms; last probe error: {}",
                    node.client_addr,
                    self.persistence.verify_timeout_ms,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// 业务作用: 从 `INFO replication` 文本识别 Dragonfly 节点角色。
///
/// 参数:
/// - `info`: Dragonfly 返回的 replication 信息文本。
fn parse_role(info: &str) -> HaRole {
    for line in info.lines() {
        match line.trim() {
            "role:master" => return HaRole::Master,
            "role:slave" | "role:replica" => {
                return HaRole::Replica;
            }
            _ => {}
        }
    }
    HaRole::Unknown
}

/// 业务作用: 从 Dragonfly 复制信息计算秒级候选门禁值。
///
/// Dragonfly 的 `master_last_io_seconds_ago` 是秒级链路心跳年龄，不是字节偏移差；
/// 正常链路会在 0 和 1 之间跳动。因此把一个秒级采样误差归零，超过一个心跳周期
/// 的部分才作为候选门禁延迟；断链或全量同步仍使用不可选哨兵值。
///
/// 参数:
/// - `info`: Dragonfly 返回的 replication 信息文本。
/// - `role`: 已解析的节点角色。
fn parse_replica_lag_seconds(info: &str, role: HaRole) -> Option<u64> {
    if role != HaRole::Replica {
        return None;
    }
    if parse_info_u64(info, "master_sync_in_progress") == Some(1) {
        return Some(u64::MAX);
    }
    if parse_info_string(info, "master_link_status").as_deref() == Some("down") {
        return Some(u64::MAX);
    }
    if let Some(lag) = parse_info_u64(info, "master_last_io_seconds_ago") {
        return Some(lag.saturating_sub(1));
    }
    if let Some(lag) = parse_field_u64(info, "lag") {
        return Some(lag);
    }
    None
}

/// 业务作用: 解析 Dragonfly 副本断链已持续秒数，供切主候选门禁使用。
///
/// Dragonfly 兼容 Redis 的 `master_link_down_since_seconds` 时优先使用；缺失时回退
/// 到断链后仍持续增长的 `master_last_io_seconds_ago` 心跳年龄。全量同步中的副本
/// 数据集不完整，不提供断链证据。
///
/// 参数:
/// - `info`: Dragonfly 返回的 replication 信息文本。
/// - `role`: 已解析的节点角色。
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

/// 业务作用: 从逗号分隔的 `name=value` 内容读取无符号整数字段。
///
/// 参数:
/// - `info`: Dragonfly 返回的 INFO 文本。
/// - `field`: 要读取的字段名称。
fn parse_field_u64(info: &str, field: &str) -> Option<u64> {
    info.lines()
        .map(str::trim)
        .flat_map(|line| line.split(','))
        .filter_map(|part| part.split_once('='))
        .find_map(|(name, value)| (name == field).then(|| value.trim().parse().ok()).flatten())
}

/// 业务作用: 把任意模板值转换成 Dragonfly 文件名安全片段。
///
/// 参数:
/// - `value`: 要转换的原始模板值。
fn template_safe(value: &str) -> String {
    let mut safe = String::with_capacity(value.len());
    let mut last_separator = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() {
            safe.push(character);
            last_separator = false;
        } else if !last_separator {
            safe.push('-');
            last_separator = true;
        }
    }
    safe.trim_matches('-').to_string()
}

/// 业务作用: 生成 Dragonfly 可接受的非空数据库文件名。
///
/// 参数:
/// - `value`: 展开模板后的数据库文件名。
fn native_dbfilename(value: &str) -> String {
    let safe = template_safe(value);
    if safe.is_empty() {
        "dump".to_string()
    } else {
        safe
    }
}

/// 业务作用: 返回当前 Unix 毫秒时间，系统时间异常时返回零。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| duration.as_millis().try_into().ok())
        .unwrap_or(0)
}
