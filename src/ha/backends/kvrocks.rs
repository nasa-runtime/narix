use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use tracing::{info, warn};

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
    ensure_ok, parse_bulk_string, parse_config_get_value, parse_info_string, parse_info_u64,
    parse_integer_response, parse_replica_master_addr,
};

/// Kvrocks 计划切换期间角色与复制状态的轮询间隔。
///
/// 该间隔只在低频控制动作中使用，不改变常态 HA 探测周期；10 毫秒可以避免把一次已经完成的
/// 本地角色变化额外量化到 50 毫秒，同时不会在业务数据面引入轮询。
const TRANSITION_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Kvrocks 的主从切换适配器，要求原生支持 `CLIENT PAUSE WRITE`。
///
/// Kvrocks 使用 RocksDB 作为每个节点的持久化存储。NARIX 只编排 `SLAVEOF` 主从关系，
/// 不关闭任何节点的持久化，也不把节点划分成热备和快照 owner。
#[derive(Debug, Clone)]
pub(crate) struct KvrocksHaBackend {
    proxy_name: String,
    control: HaControlClient,
}

/// Kvrocks 计划切换失败后的拓扑恢复结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SwitchoverRecovery {
    /// 新主和旧主的复制关系已经完整提交。
    Committed,
    /// 候选已重新跟随旧主，原拓扑已经恢复。
    RolledBack,
}

impl KvrocksHaBackend {
    /// 业务作用: 从配置和共享运行代令牌构造 Kvrocks 产品适配器。
    ///
    /// 参数:
    /// - `item`: 提供 Kvrocks 节点认证、RESP TLS、连接超时和探测超时的代理配置。
    /// - `generation_guard`: 与所属 replication HA 运行代共享的退休状态。
    pub(crate) fn from_proxy_item(
        item: &ProxyItem,
        generation_guard: HaGenerationGuard,
    ) -> anyhow::Result<Self> {
        if item.backend_profile != BackendProfile::Kvrocks {
            bail!(
                "backend_profile={} is not certified by the Kvrocks HA adapter",
                item.backend_profile.as_str()
            );
        }
        if item.username.is_some() {
            bail!(
                "Kvrocks replication HA requires global password authentication without username"
            );
        }
        Ok(Self {
            proxy_name: item.name.clone(),
            control: HaControlClient::from_proxy_item(item, generation_guard),
        })
    }

    /// 业务作用: 返回角色变化、追平和复制确认使用的有界等待时间。
    fn role_transition_timeout(&self) -> Duration {
        Duration::from_millis(
            self.control
                .probe_request_timeout_ms()
                .saturating_mul(5)
                .clamp(1_000, 30_000),
        )
    }

    /// 业务作用: 读取 Kvrocks `INFO replication` 原始文本。
    ///
    /// 参数:
    /// - `node`: 要探测复制角色、上游和 offset 的 Kvrocks 节点。
    async fn replication_info(&self, node: &HaNode) -> anyhow::Result<String> {
        let frame = self
            .control
            .send_probe_command(node, &[b"INFO".as_slice(), b"replication".as_slice()])
            .await?;
        parse_bulk_string(&frame)
    }

    /// 业务作用: 读取 Kvrocks 复制状态并转换成通用 HA 观测。
    ///
    /// 参数:
    /// - `node`: 要探测角色、上游和同步状态的 Kvrocks 节点。
    async fn inspect_replication(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        let info = self.replication_info(node).await?;
        let role = parse_kvrocks_role(&info);
        Ok(HaNodeObservation {
            role,
            upstream_addr: parse_replica_master_addr(&info),
            replica_lag_seconds: parse_kvrocks_replica_lag_seconds(&info, role),
            link_down_since_seconds: parse_kvrocks_link_down_seconds(&info, role),
        })
    }

    /// 业务作用: 在探测超时窗口内等待 Kvrocks 节点达到指定角色。
    ///
    /// 参数:
    /// - `node`: 要持续探测的 Kvrocks 节点。
    /// - `expected`: 控制动作完成后必须达到的角色。
    /// - `purpose`: 写入错误上下文的控制动作名称。
    async fn wait_for_role(
        &self,
        node: &HaNode,
        expected: HaRole,
        purpose: &str,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.role_transition_timeout();
        let mut last_error: Option<String>;
        loop {
            authority
                .ensure_active("while waiting for a Kvrocks role transition")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.role == expected => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "Kvrocks node `{}` did not become {} after {purpose}; last probe error: {}",
                    node.client_addr,
                    expected.as_str(),
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(TRANSITION_POLL_INTERVAL).await;
        }
    }

    /// 业务作用: 读取一个 Kvrocks 运行时配置值。
    ///
    /// 参数:
    /// - `node`: 要查询配置的 Kvrocks 节点。
    /// - `name`: Kvrocks 原生配置项名称。
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

    /// 业务作用: 写入一个 Kvrocks 运行时配置值。
    ///
    /// 参数:
    /// - `node`: 接收动态配置的 Kvrocks 节点。
    /// - `name`: Kvrocks 原生配置项名称。
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

    /// 业务作用: 把 Kvrocks 复制认证密码收敛到 NARIX 使用的全局后端密码。
    ///
    /// 参数:
    /// - `node`: 后续要建立上游复制连接的 Kvrocks 节点。
    async fn reconcile_masterauth(
        &self,
        node: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let expected = self.control.password().unwrap_or_default();
        if self.config_get(node, "masterauth").await? == expected {
            return Ok(());
        }
        authority
            .ensure_active("before changing Kvrocks replication credentials")
            .await?;
        self.config_set(node, "masterauth", expected).await?;
        let actual = self.config_get(node, "masterauth").await?;
        if actual != expected {
            bail!(
                "Kvrocks masterauth on `{}` did not match the configured backend password",
                node.client_addr
            );
        }
        Ok(())
    }

    /// 业务作用: 暂停旧主业务写命令，为多步 `SLAVEOF` 切换建立有界静默窗口。
    ///
    /// 参数:
    /// - `current_primary`: 当前健康且仍承担业务写入的 Kvrocks 主节点。
    async fn pause_primary_writes(&self, current_primary: &HaNode) -> anyhow::Result<()> {
        let pause_ms = self
            .role_transition_timeout()
            .as_millis()
            .saturating_mul(4)
            .min(120_000)
            .to_string();
        let frame = self
            .control
            .send_command(
                current_primary,
                &[
                    b"CLIENT".as_slice(),
                    b"PAUSE".as_slice(),
                    pause_ms.as_bytes(),
                    b"WRITE".as_slice(),
                ],
            )
            .await?;
        ensure_ok(&frame).with_context(|| {
            format!(
                "pause Kvrocks writes on `{}` before planned switchover",
                current_primary.client_addr
            )
        })
    }

    /// 业务作用: 解除节点的客户端暂停状态。
    ///
    /// 参数:
    /// - `node`: 要恢复客户端命令处理的 Kvrocks 节点。
    async fn unpause_clients(&self, node: &HaNode) -> anyhow::Result<()> {
        let frame = self
            .control
            .send_command(node, &[b"CLIENT".as_slice(), b"UNPAUSE".as_slice()])
            .await?;
        ensure_ok(&frame)
    }

    /// 业务作用: 等待指定副本在旧主写入暂停后追平同一个 RocksDB 序列号。
    ///
    /// 参数:
    /// - `current_primary`: 已暂停业务写入的当前 Kvrocks 主节点。
    /// - `candidate`: 必须完整追平后才能提升的 Kvrocks 副本。
    async fn wait_candidate_caught_up(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        let deadline = Instant::now() + self.role_transition_timeout();
        loop {
            authority
                .ensure_active("while waiting for a Kvrocks candidate to catch up")
                .await?;
            let primary_info = self.replication_info(current_primary).await;
            let candidate_info = self.replication_info(candidate).await;
            let last_observation = match (primary_info, candidate_info) {
                (Ok(primary_info), Ok(candidate_info)) => {
                    let observation = HaNodeObservation {
                        role: parse_kvrocks_role(&candidate_info),
                        upstream_addr: parse_replica_master_addr(&candidate_info),
                        replica_lag_seconds: parse_kvrocks_replica_lag_seconds(
                            &candidate_info,
                            HaRole::Replica,
                        ),
                        link_down_since_seconds: parse_kvrocks_link_down_seconds(
                            &candidate_info,
                            HaRole::Replica,
                        ),
                    };
                    let primary_offset = parse_info_u64(&primary_info, "master_repl_offset");
                    let candidate_offset = parse_info_u64(&candidate_info, "slave_repl_offset");
                    let details = format!(
                        "primary_offset={primary_offset:?}, candidate_offset={candidate_offset:?}, candidate_role={}",
                        observation.role.as_str()
                    );
                    if parse_kvrocks_role(&primary_info) == HaRole::Master
                        && observation.follows(current_primary)
                        && observation.replica_lag_seconds == Some(0)
                        && primary_offset.is_some()
                        && primary_offset == candidate_offset
                    {
                        return Ok(());
                    }
                    details
                }
                (Err(primary_error), Err(candidate_error)) => format!(
                    "primary probe failed: {primary_error:#}; candidate probe failed: {candidate_error:#}"
                ),
                (Err(error), Ok(_)) => format!("primary probe failed: {error:#}"),
                (Ok(_), Err(error)) => format!("candidate probe failed: {error:#}"),
            };
            if Instant::now() >= deadline {
                bail!(
                    "Kvrocks candidate `{}` did not catch up with `{}` before timeout; {last_observation}",
                    candidate.client_addr,
                    current_primary.client_addr
                );
            }
            tokio::time::sleep(TRANSITION_POLL_INTERVAL).await;
        }
    }

    /// 业务作用: 使用 Kvrocks 原生 `SLAVEOF NO ONE` 提升候选节点。
    ///
    /// 参数:
    /// - `candidate`: 已通过通用候选门禁的 Kvrocks 副本。
    async fn promote_candidate(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        authority
            .ensure_active("before Kvrocks SLAVEOF NO ONE")
            .await?;
        let frame = self
            .control
            .send_command(
                candidate,
                &[b"SLAVEOF".as_slice(), b"NO".as_slice(), b"ONE".as_slice()],
            )
            .await?;
        ensure_ok(&frame)
            .with_context(|| format!("promote Kvrocks `{}`", candidate.client_addr))?;
        self.wait_for_role(candidate, HaRole::Master, "SLAVEOF NO ONE", authority)
            .await
    }

    /// 业务作用: 在候选已成为主节点后，结束旧主的出站复制关系，使旧主可以反向跟随新主。
    ///
    /// 参数说明:
    /// - `current_primary`: 业务写入仍被暂停、即将降级的 Kvrocks 旧主。
    /// - `authority`: 每次外部动作前必须复验的切换控制权威。
    ///
    /// 返回: 原生确认关闭复制连接后成功；失权、通信失败或响应不合法时停止后续降级。
    async fn detach_primary_replicas(
        &self,
        current_primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        // 候选提升不会同步移除旧主中的复制线程；保留该记录会触发原生防复制环拒绝。
        // 此时数据已经追平且旧主仍禁止写入，关闭旧复制链后由新拓扑重新分配其余副本。
        authority
            .ensure_active("before closing Kvrocks old-primary replication links")
            .await?;
        let frame = self
            .control
            .send_command(
                current_primary,
                &[
                    b"CLIENT".as_slice(),
                    b"KILL".as_slice(),
                    b"TYPE".as_slice(),
                    b"replica".as_slice(),
                ],
            )
            .await?;
        parse_integer_response(&frame, "Kvrocks CLIENT KILL TYPE replica")?;
        Ok(())
    }

    /// 业务作用: 执行已经进入写暂停窗口的 Kvrocks 主从切换。
    ///
    /// 参数说明:
    /// - `current_primary`: 已暂停业务写入的旧主节点。
    /// - `candidate`: 已选定的新主候选节点。
    /// - `authority`: 覆盖追平、提升、断开旧复制链及角色校验的切换控制权威。
    ///
    /// 返回: 新主和旧主的反向复制关系完成验证后成功；任一步失败交由调用方恢复拓扑。
    async fn execute_paused_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.wait_candidate_caught_up(current_primary, candidate, authority)
            .await?;
        // 必须先确认候选完整追平并已提升，才可断开旧复制链；旧主降级完成前保持写暂停。
        self.promote_candidate(candidate, authority).await?;
        self.detach_primary_replicas(current_primary, authority)
            .await?;
        self.configure_replica(current_primary, candidate, authority)
            .await?;
        self.verify_replication(current_primary, candidate, authority)
            .await
    }

    /// 业务作用: 在计划切换步骤报错后确认已提交的新拓扑，或恢复原来的单主拓扑。
    ///
    /// 参数:
    /// - `current_primary`: 切换前的 Kvrocks 主节点。
    /// - `candidate`: 切换目标 Kvrocks 副本。
    async fn recover_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<SwitchoverRecovery> {
        authority
            .ensure_active("before recovering a Kvrocks planned switchover")
            .await?;
        let current_observation = self.inspect_replication(current_primary).await?;
        let candidate_observation = self.inspect_replication(candidate).await?;

        if candidate_observation.role == HaRole::Master {
            if current_observation.follows(candidate) {
                self.verify_replication(current_primary, candidate, authority)
                    .await?;
                return Ok(SwitchoverRecovery::Committed);
            }
            if current_observation.role == HaRole::Replica {
                self.configure_replica(current_primary, candidate, authority)
                    .await?;
                self.verify_replication(current_primary, candidate, authority)
                    .await?;
                return Ok(SwitchoverRecovery::Committed);
            }
        }

        if current_observation.role == HaRole::Master {
            self.configure_replica(candidate, current_primary, authority)
                .await?;
            self.verify_replication(candidate, current_primary, authority)
                .await?;
            return Ok(SwitchoverRecovery::RolledBack);
        }

        bail!(
            "Kvrocks switchover recovery observed ambiguous roles: old_primary={}, candidate={}",
            current_observation.role.as_str(),
            candidate_observation.role.as_str()
        )
    }
}

impl ReplicationHaBackend for KvrocksHaBackend {
    /// 业务作用: 返回 Kvrocks 产品档案。
    fn profile(&self) -> BackendProfile {
        BackendProfile::Kvrocks
    }

    /// 业务作用: 查询 Kvrocks 节点角色、上游和同步门禁状态。
    ///
    /// 参数:
    /// - `node`: 要探测的 Kvrocks 节点。
    async fn inspect_node(&self, node: &HaNode) -> anyhow::Result<HaNodeObservation> {
        self.inspect_replication(node).await
    }

    /// 业务作用: 使用 `CLIENT PAUSE WRITE` 和 Kvrocks 原生 `SLAVEOF` 执行计划切换。
    ///
    /// 参数:
    /// - `current_primary`: 当前健康且可协调切换的 Kvrocks 主节点。
    /// - `candidate`: 要接管写流量的 Kvrocks 副本。
    async fn planned_switchover(
        &self,
        current_primary: &HaNode,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        authority
            .ensure_active("before pausing Kvrocks primary writes")
            .await?;
        self.pause_primary_writes(current_primary).await?;
        let switch_result = self
            .execute_paused_switchover(current_primary, candidate, authority)
            .await;

        let final_result = match switch_result {
            Ok(()) => Ok(()),
            Err(switch_error) => match self
                .recover_switchover(current_primary, candidate, authority)
                .await
            {
                Ok(SwitchoverRecovery::Committed) => {
                    warn!(
                        proxy = %self.proxy_name,
                        old_primary = %current_primary.client_addr,
                        candidate = %candidate.client_addr,
                        error = %switch_error,
                        "Kvrocks planned switchover reported an intermediate error but the new topology is committed"
                    );
                    Ok(())
                }
                Ok(SwitchoverRecovery::RolledBack) => Err(switch_error.context(
                    "Kvrocks planned switchover failed and the original topology was restored",
                )),
                Err(recovery_error) => Err(anyhow::anyhow!(
                    "Kvrocks planned switchover failed: {switch_error:#}; topology recovery failed: {recovery_error:#}"
                )),
            },
        };

        let unpause_result = match authority
            .ensure_active("before unpausing Kvrocks primary writes")
            .await
        {
            Ok(()) => self.unpause_clients(current_primary).await,
            Err(error) => Err(error.context(
                "leave the bounded Kvrocks write pause in place after action authority loss",
            )),
        };
        if let Err(unpause_error) = unpause_result {
            if final_result.is_ok() {
                warn!(
                    proxy = %self.proxy_name,
                    node = %current_primary.client_addr,
                    error = %unpause_error,
                    "Kvrocks switchover completed but CLIENT UNPAUSE failed; the bounded pause will expire automatically"
                );
            } else {
                return Err(final_result
                    .expect_err("checked error result")
                    .context(format!("CLIENT UNPAUSE also failed: {unpause_error:#}")));
            }
        }
        final_result?;

        info!(
            proxy = %self.proxy_name,
            old_primary = %current_primary.client_addr,
            candidate = %candidate.client_addr,
            "Kvrocks planned switchover completed"
        );
        Ok(())
    }

    /// 业务作用: 在旧主完成 fencing 后使用 `SLAVEOF NO ONE` 紧急提升 Kvrocks 副本。
    ///
    /// 参数:
    /// - `candidate`: 已通过候选门禁的 Kvrocks 副本。
    async fn emergency_promote(
        &self,
        candidate: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.promote_candidate(candidate, authority).await
    }

    /// 业务作用: 写入复制密码并使用 `SLAVEOF host port` 把节点收敛为副本。
    ///
    /// 参数:
    /// - `node`: 要改为副本的 Kvrocks 节点。
    /// - `primary`: 该节点必须跟随的 Kvrocks 主节点。
    async fn configure_replica(
        &self,
        node: &HaNode,
        primary: &HaNode,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<()> {
        self.reconcile_masterauth(node, authority).await?;
        let (host, port) = split_host_port(&primary.replication_addr)?;
        authority.ensure_active("before Kvrocks SLAVEOF").await?;
        let frame = self
            .control
            .send_command(
                node,
                &[b"SLAVEOF".as_slice(), host.as_bytes(), port.as_bytes()],
            )
            .await?;
        ensure_ok(&frame).with_context(|| {
            format!(
                "set Kvrocks `{}` as replica of `{}`",
                node.client_addr, primary.replication_addr
            )
        })?;
        info!(
            proxy = %self.proxy_name,
            node = %node.client_addr,
            primary = %primary.replication_addr,
            "Kvrocks node configured as replica"
        );
        Ok(())
    }

    /// 业务作用: 等待 Kvrocks 节点稳定跟随指定主节点并完成全量同步。
    ///
    /// 参数:
    /// - `node`: 要校验的 Kvrocks 副本。
    /// - `primary`: 期望的上游 Kvrocks 主节点。
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
                .ensure_active("while verifying Kvrocks replication")
                .await?;
            match self.inspect_replication(node).await {
                Ok(observation) if observation.eligible_replica_of(primary, 0) => return Ok(()),
                Ok(_) => last_error = None,
                Err(error) => last_error = Some(format!("{error:#}")),
            }
            if Instant::now() >= deadline {
                bail!(
                    "Kvrocks node `{}` did not follow `{}` before timeout; last probe error: {}",
                    node.client_addr,
                    primary.replication_addr,
                    last_error.as_deref().unwrap_or("none")
                );
            }
            tokio::time::sleep(TRANSITION_POLL_INTERVAL).await;
        }
    }
}

impl DynamicPersistenceBackend for KvrocksHaBackend {
    /// 业务作用: 声明 Kvrocks 持久化由每个节点自身管理，NARIX 不执行动态持久化动作。
    fn persistence_capabilities(&self) -> PersistenceCapabilities {
        PersistenceCapabilities {
            runtime_reconfigure: false,
            force_checkpoint: false,
            verify_checkpoint: false,
            restart_durable: true,
        }
    }

    /// 业务作用: 拒绝把 Kvrocks 节点映射成 NARIX 动态持久化 owner。
    ///
    /// 参数:
    /// - `node`: 被查询的 Kvrocks 节点。
    /// - `role`: 节点当前复制角色。
    async fn inspect_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
    ) -> anyhow::Result<PersistenceState> {
        let _ = (node, role);
        Ok(PersistenceState {
            target: None,
            effective: false,
            restart_durable: true,
        })
    }

    /// 业务作用: 拒绝修改 Kvrocks 节点自身的 RocksDB 持久化配置。
    ///
    /// 参数:
    /// - `node`: 请求修改持久化职责的 Kvrocks 节点。
    /// - `role`: 节点当前复制角色。
    /// - `target`: 通用状态机请求的动态持久化职责。
    async fn reconcile_persistence(
        &self,
        node: &HaNode,
        role: HaRole,
        target: PersistenceTarget,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<PersistenceApplyResult> {
        let _ = (node, role, target, authority);
        bail!("Kvrocks persistence is node-local and is not managed by NARIX")
    }

    /// 业务作用: 拒绝由 NARIX 强制创建 Kvrocks 持久化点。
    ///
    /// 参数:
    /// - `node`: 请求执行持久化点的 Kvrocks 节点。
    /// - `reason`: 本次持久化动作的业务原因。
    async fn force_checkpoint(
        &self,
        node: &HaNode,
        reason: CheckpointReason,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointToken> {
        let _ = (node, reason, authority);
        bail!("Kvrocks checkpoints are not managed by NARIX")
    }

    /// 业务作用: 拒绝验证不存在的 NARIX 托管 Kvrocks 持久化点。
    ///
    /// 参数:
    /// - `node`: 请求验证持久化点的 Kvrocks 节点。
    /// - `token`: 调用方提供的持久化点令牌。
    async fn verify_checkpoint(
        &self,
        node: &HaNode,
        token: &CheckpointToken,
        authority: &dyn HaActionAuthority,
    ) -> anyhow::Result<CheckpointStatus> {
        let _ = (node, token, authority);
        bail!("Kvrocks checkpoints are not managed by NARIX")
    }
}

/// 业务作用: 从 Kvrocks `INFO replication` 文本识别节点角色。
///
/// 参数:
/// - `info`: Kvrocks 返回的 replication 信息文本。
fn parse_kvrocks_role(info: &str) -> HaRole {
    match parse_info_string(info, "role").as_deref() {
        Some("master") => HaRole::Master,
        Some("slave" | "replica") => HaRole::Replica,
        _ => HaRole::Unknown,
    }
}

/// 业务作用: 把 Kvrocks RocksDB 序列号和复制链路状态转换成候选门禁延迟。
///
/// 参数:
/// - `info`: Kvrocks 返回的 replication 信息文本。
/// - `role`: 已解析的 Kvrocks 节点角色。
fn parse_kvrocks_replica_lag_seconds(info: &str, role: HaRole) -> Option<u64> {
    if role != HaRole::Replica {
        return None;
    }
    if parse_info_u64(info, "master_sync_in_progress") == Some(1)
        || parse_info_string(info, "master_sync_unrecoverable_error").as_deref() == Some("yes")
    {
        return Some(u64::MAX);
    }
    match parse_info_string(info, "master_link_status").as_deref() {
        Some("up") => match (
            parse_info_u64(info, "slave_repl_offset"),
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

/// 业务作用: 解析 Kvrocks 副本断链已持续秒数，供切主候选门禁使用。
///
/// 全量同步中或出现不可恢复同步错误的副本数据集不完整，不提供断链证据；
/// 仅纯断链副本按 `master_link_down_since_seconds`（缺失时回退心跳年龄）参选。
///
/// 参数:
/// - `info`: Kvrocks 返回的 replication 信息文本。
/// - `role`: 已解析的 Kvrocks 节点角色。
fn parse_kvrocks_link_down_seconds(info: &str, role: HaRole) -> Option<u64> {
    if role != HaRole::Replica {
        return None;
    }
    if parse_info_u64(info, "master_sync_in_progress") == Some(1)
        || parse_info_string(info, "master_sync_unrecoverable_error").as_deref() == Some("yes")
    {
        return None;
    }
    if parse_info_string(info, "master_link_status").as_deref() != Some("down") {
        return None;
    }
    parse_info_u64(info, "master_link_down_since_seconds")
        .or_else(|| parse_info_u64(info, "master_last_io_seconds_ago"))
}
