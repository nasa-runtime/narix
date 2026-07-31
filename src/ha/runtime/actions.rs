impl ReplicationHaRuntime {
/// 业务作用: 在观察者和动作权威门禁下执行动态持久化启停或强制快照，并在每个外部步骤前复验控制权。
    /// 参数说明:
    /// - `action`: 当前要执行的拓扑或持久化动作，决定命令模板、指标标签和错误语义。
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    /// - `role`: 节点在当前拓扑中的角色，用于选择路由、晋升、降级或快照动作。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn run_persistence_action(
        &self,
        action: PersistenceAction,
        node: &HaNode,
        role: HaRole,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("before a persistence action")?;
        let authority = RuntimePersistenceAuthority {
            runtime: self,
            lease: lease_guard,
            cluster_action,
        };
        match action {
            PersistenceAction::Enable => {
                let result = self
                    .ha
                    .backend()
                    .reconcile_persistence(
                        node,
                        role,
                        PersistenceTarget::Owner,
                        &authority,
                    )
                    .await?;
                if !result.effective {
                    bail!(
                        "replication HA persistence enable on `{}` was applied but not effective",
                        node.client_addr
                    );
                }
                let _restart_durable = result.restart_durable;
                Ok(())
            }
            PersistenceAction::Disable => {
                let result = self
                    .ha
                    .backend()
                    .reconcile_persistence(
                        node,
                        role,
                        if role == HaRole::Replica {
                            PersistenceTarget::Standby
                        } else {
                            PersistenceTarget::Suspended
                        },
                        &authority,
                    )
                    .await?;
                if !result.effective {
                    bail!(
                        "replication HA persistence disable on `{}` was applied but not effective",
                        node.client_addr
                    );
                }
                let _restart_durable = result.restart_durable;
                Ok(())
            }
        }
    }

    /// 业务作用: 校验节点已经以 owner 身份应用原生持久化配置。
    ///
    /// 参数说明:
    /// - `node`: 要从后端重新读取持久化配置的节点。
    /// - `role`: 节点在当前拓扑中的角色，用于展开对应的原生持久化策略。
    ///
    /// 返回: 后端重新读取的配置与 owner 目标一致时返回成功。
    async fn verify_persistence_owner(
        &self,
        node: &HaNode,
        role: HaRole,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        let authority = RuntimePersistenceAuthority {
            runtime: self,
            lease: lease_guard,
            cluster_action,
        };
        authority
            .ensure_active("before verifying the persistence owner")
            .await?;
        let state = self
            .ha
            .backend()
            .inspect_persistence(node, role)
            .await
            .with_context(|| {
                format!(
                    "inspect persistence owner configuration on `{}`",
                    node.client_addr
                )
            })?;
        authority
            .ensure_active("after verifying the persistence owner")
            .await?;
        if !state.effective || state.target != Some(PersistenceTarget::Owner) {
            bail!(
                "persistence owner configuration on `{}` is not effective",
                node.client_addr
            );
        }
        Ok(())
    }

    /// 业务作用: 在当前动作权威有效时调用产品适配器提升目标节点，并确认角色收敛。
    /// 参数说明:
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn promote(
        &self,
        node: &HaNode,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("before promotion")?;
        self.ensure_not_observer("backend promotion")?;
        let authority = RuntimeActionAuthority {
            runtime: self,
            lease: lease_guard,
            cluster_action,
        };
        self.ha
            .backend()
            .emergency_promote(node, &authority)
            .await
    }

    /// 业务作用: 执行基础设施隔离命令，并在集群协调模式下把不可复用动作令牌作为独立参数模板传给执行器。
    /// 参数说明:
    /// - `node`: 需要先切断写入能力的旧主节点。
    /// - `reason`: 本次隔离动作的业务原因。
    /// - `lease_guard`: local-file/Redis 协调模式的当前租约。
    /// - `cluster_action`: 进程级协调器批准的动作守卫；其令牌同时传给隔离执行器。
    ///
    /// 返回: 隔离命令在时限内成功退出时返回成功，否则增加失败指标并返回错误。
    async fn fence_node_inner_with_token(
        &self,
        node: &HaNode,
        reason: &str,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("before fencing")?;
        self.ensure_not_observer("backend fencing")?;
        let authority = RuntimeActionAuthority {
            runtime: self,
            lease: lease_guard,
            cluster_action,
        };
        authority.ensure_active("before fencing").await?;
        match self.fencing.mode {
            HaFencingMode::Manual => {
                // 软隔离档位：不执行外部隔离命令。旧主的隔离交给切主后的 demote_extra_masters——
                // 一旦旧主可达即被 REPLICAOF 降级为新主副本；旧主暂不可达时下一轮 reconcile 再降级。
                // 存在旧主复活到被降级之间的短暂双写窗口；集群协调部署额外在提主前等满一个
                // route_guard_timeout（见 wait_soft_fencing_route_barrier），让分区成员的旧路由
                // 先行断路。需零脑裂窗口时改用 external-command。
                info!(
                    proxy = %self.name,
                    node = %node.client_addr,
                    reason = %reason,
                    "replication HA soft fencing: skipping external isolation; old master will be demoted after switchover"
                );
                Ok(())
            }
            HaFencingMode::ExternalCommand => {
                if self.fencing.command.is_empty() {
                    bail!("replication HA external fencing command is empty");
                }
                let action_token = cluster_action.map(|action| action.token.as_str());
                let started = Instant::now();
                let program =
                    render_fence_template(&self.fencing.command[0], &self.name, node, action_token);
                let args = self
                    .fencing
                    .command
                    .iter()
                    .skip(1)
                    .map(|arg| render_fence_template(arg, &self.name, node, action_token))
                    .collect::<Vec<_>>();
                let mut command = Command::new(&program);
                command.args(&args).kill_on_drop(true);
                let output_future = command.output();
                tokio::pin!(output_future);
                let timeout = tokio::time::sleep(Duration::from_millis(self.fencing.timeout_ms));
                tokio::pin!(timeout);
                let mut authority_ticker =
                    tokio::time::interval(Duration::from_millis(50));
                authority_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // 外部脚本是有界但不是原子动作。执行期间并发复验权威，
                // 失权时 drop output future；`kill_on_drop` 终止直接子进程，
                // 避免旧 owner 在新 owner 已接管后仍完成过期隔离。
                let output = loop {
                    tokio::select! {
                        biased;
                        _ = authority_ticker.tick() => {
                            authority
                                .ensure_active("while running the external fencing command")
                                .await?;
                        }
                        result = &mut output_future => {
                            break result.map_err(|error| {
                                anyhow::anyhow!(
                                    "run replication HA fencing command `{program}`: {error}"
                                )
                            })?;
                        }
                        _ = &mut timeout => {
                            bail!(
                                "fence `{}` timed out after {}ms",
                                node.client_addr,
                                self.fencing.timeout_ms
                            );
                        }
                    }
                };
                let elapsed = elapsed_micros(started);
                self.last_fence_elapsed_micros
                    .store(elapsed, Ordering::Relaxed);
                if !output.status.success() {
                    self.fence_failure_count.fetch_add(1, Ordering::Relaxed);
                    bail!(
                        "fence `{}` command exited with {}; stdout={}; stderr={}",
                        node.client_addr,
                        output.status,
                        short_output(&output.stdout),
                        short_output(&output.stderr)
                    );
                }
                authority
                    .ensure_active("after the external fencing command succeeded")
                    .await?;
                let cleared_risk = self
                    .unconfirmed_extra_masters
                    .write()
                    .await
                    .remove(&node.client_addr);
                if cleared_risk {
                    // 强 fencing 成功表示该节点已经在基础设施层失去写能力，即使它
                    // 永久不可达也不必再等待 REPLICAOF 证词。这里只消费
                    // external-command 的成功结果；manual 分支是 no-op，绝不能出清风险。
                    info!(
                        proxy = %self.name,
                        node = %node.client_addr,
                        "external fencing cleared a previously observed extra-master risk"
                    );
                }
                self.fence_count.fetch_add(1, Ordering::Relaxed);
                info!(
                    proxy = %self.name,
                    node = %node.client_addr,
                    reason = %reason,
                    elapsed_micros = elapsed,
                    "replication HA node fenced"
                );
                Ok(())
            }
        }
    }

    /// 业务作用: 在权威门禁下把节点改接到指定主节点，并验证复制关系收敛。
    /// 参数说明:
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    /// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn replicate_from(
        &self,
        node: &HaNode,
        master: &HaNode,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("before replica configuration")?;
        self.ensure_not_observer("backend replica configuration")?;
        let authority = RuntimeActionAuthority {
            runtime: self,
            lease: lease_guard,
            cluster_action,
        };
        self.ha
            .backend()
            .configure_replica(node, master, &authority)
            .await
    }
}
