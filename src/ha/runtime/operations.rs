impl ReplicationHaRuntime {
    /// 业务作用: 在租约、fencing 和多主保护门禁下执行人工故障切换并发布已验证路由。
    /// 参数说明: 无。
    ///
    /// 返回: 动作完成、拓扑复验并发布后返回最新 HA 状态；失权、隔离或收敛失败时返回错误。
    pub async fn manual_failover(&self) -> anyhow::Result<ReplicationHaStatus> {
        // 人工动作与后台收敛遵循同一配置事务屏障，不能改写正在交接的旧拓扑。
        let _control = self.acquire_regular_control().await?;
        self.ensure_not_retired("before manual failover")?;
        self.ensure_not_observer("manual failover")?;
        let _operation = self.acquire_operation().await?;
        let started = Instant::now();
        let current = self.current_master.read().await.clone();
        if self.node_role(&current).await? == HaRole::Master {
            bail!(
                "replication HA current master `{}` is healthy; use switchover for planned master changes",
                current.client_addr
            );
        }
        let lease = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            None
        } else {
            self.acquire_failover_lease("manual failover").await?
        };

        let (mut masters, replicas) = self.discover_roles_with_replica_lag().await;
        if masters.len() > 1 {
            let reason = format!(
                "multiple replication HA masters detected: {}",
                format_node_addrs(&masters)
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }

        let (new_master, needs_promotion, old_master_already_follows) =
            if let Some(master) = masters.pop() {
                let follows = old_master_follows_new(&replicas, &current, &master);
                (master, false, follows)
            } else {
                let candidate = self
                    .select_failover_candidate(replicas, Some(&current), "manual failover")
                    .await?;
                (candidate, true, false)
            };
        let mut cluster_action = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            Some(
                self.acquire_cluster_manual_action(
                    CoordinationActionKind::Failover,
                    "failover",
                    current.client_addr.clone(),
                    new_master.client_addr.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual failover fencing",
        )
        .await?;
        // 已有唯一新主且旧主明确直连跟随它时，角色收敛本身就是隔离证据；再次
        // 执行 external-command 只会误关健康副本。跳过副作用不等于跳过权威，
        // 后续拓扑收敛、协调提交和路由发布仍使用同一租约/动作令牌。
        if (needs_promotion || new_master.client_addr != current.client_addr)
            && !old_master_already_follows
        {
            let result = self
                .fence_node_inner_with_token(
                    &current,
                    "manual failover old master fencing",
                    lease.as_ref(),
                    cluster_action.as_ref(),
                )
                .await;
            self.protect_ha_control_step(result, "during manual failover fencing")
                .await?;
            self.ensure_automatic_authority_or_protect(
                lease.as_ref(),
                cluster_action.as_ref(),
                "after manual failover fencing",
            )
            .await?;
        } else if old_master_already_follows {
            info!(
                proxy = %self.name,
                old_master = %current.client_addr,
                new_master = %new_master.client_addr,
                "manual failover skips fencing because the old master already follows the observed new master"
            );
        }
        if needs_promotion {
            // 软隔离下与自动切主同一时间围栏：等分区成员的旧路由先断路再提主。
            self.wait_soft_fencing_route_barrier(
                None,
                cluster_action.as_ref(),
                "after the manual failover route barrier",
            )
            .await?;
            self.ensure_automatic_authority_or_protect(
                lease.as_ref(),
                cluster_action.as_ref(),
                "before manual failover promotion",
            )
            .await?;
            let result = self
                .promote(&new_master, lease.as_ref(), cluster_action.as_ref())
                .await;
            self.protect_ha_control_step(result, "during manual failover promotion")
                .await?;
            self.ensure_automatic_authority_or_protect(
                lease.as_ref(),
                cluster_action.as_ref(),
                "after manual failover promotion",
            )
            .await?;
        }
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual failover convergence",
        )
        .await?;
        let result = self
            .demote_extra_masters_checked(
                &new_master,
                lease.as_ref(),
                cluster_action.as_ref(),
                None,
            )
            .await;
        self.protect_ha_control_step(result, "during manual failover convergence")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after manual failover convergence",
        )
        .await?;
        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing manual failover")
                .await?;
            self.complete_cluster_action_or_protect(
                action,
                &new_master.client_addr,
                "after manual failover",
            )
            .await?;
        }
        if let Some(lease) = lease.as_ref() {
            let result = self.record_failover_outcome(lease, &new_master).await;
            self.protect_ha_control_step(result, "while publishing manual failover outcome")
                .await?;
        }
        self.finish_master_switch(current.client_addr, new_master, started, "manual failover")
            .await?;
        Ok(self.status().await)
    }

    /// 业务作用: 在健康旧主参与下执行有序计划切换，完成降级与复制收敛后才开放路由。
    ///
    /// 返回: 动作完成、拓扑复验并发布后返回最新 HA 状态；失权、隔离或收敛失败时返回错误。
    pub async fn manual_switchover(&self) -> anyhow::Result<ReplicationHaStatus> {
        // 管理入口必须受事务冻结约束；配置预提交使用内部计划切换路径。
        let _control = self.acquire_regular_control().await?;
        self.planned_switchover_target(None, "manual switchover")
            .await
    }

    /// 业务作用: 在配置事务提交前把主节点计划切换到候选配置保留的健康副本。
    ///
    /// 参数说明:
    /// - `retained_client_addrs`: 新配置保留的节点地址；候选必须同时在该集合中并健康跟随当前主节点。
    ///
    /// 返回: 一个保留候选成为唯一主节点、复制与持久化拓扑收敛后的状态。
    pub async fn planned_switchover_to_retained(
        &self,
        retained_client_addrs: &[String],
    ) -> anyhow::Result<ReplicationHaStatus> {
        if retained_client_addrs.is_empty() {
            bail!("configuration topology switchover requires at least one retained HA node");
        }
        self.planned_switchover_target(
            Some(retained_client_addrs),
            "configuration topology switchover",
        )
        .await
    }

    /// 业务作用: 在配置删除节点前把动态持久化责任迁移到候选配置仍保留的节点。
    ///
    /// 参数说明:
    /// - `retained_client_addrs`: 候选 HA 节点集合中的业务地址；不在集合中的节点不得成为新 owner。
    ///
    /// 返回: 新持久化 owner 已生成并验证持久化点、旧 owner 已停用时返回成功。
    pub async fn planned_persistence_handoff_to_retained(
        &self,
        retained_client_addrs: &[String],
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("before configuration persistence handoff")?;
        if self.persistence.policy == HaPersistencePolicy::None {
            return Ok(());
        }
        if retained_client_addrs.is_empty() {
            bail!("configuration persistence handoff requires at least one retained HA node");
        }
        let _operation = self.acquire_operation().await?;
        let retained = retained_client_addrs
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let (masters, replicas) = self.discover_roles_with_replica_lag().await;
        // 被候选配置删除的节点提交后不再受任何运行时管理；必须在旧配置仍有效
        // 时确认它可达，后面的停用事务才能证明不会遗留逃逸 owner。
        let discovered = masters
            .iter()
            .map(|node| node.client_addr.as_str())
            .chain(
                replicas
                    .iter()
                    .map(|candidate| candidate.node.client_addr.as_str()),
            )
            .collect::<HashSet<_>>();
        let unavailable_removed = self
            .ha
            .nodes()
            .iter()
            .filter(|node| {
                !retained.contains(node.client_addr.as_str())
                    && !discovered.contains(node.client_addr.as_str())
            })
            .map(|node| node.client_addr.clone())
            .collect::<Vec<_>>();
        if !unavailable_removed.is_empty() {
            bail!(
                "configuration persistence handoff cannot remove unreachable nodes before verifying their persistence is disabled: {}",
                unavailable_removed.join(",")
            );
        }
        self.update_replica_seen_since(&replicas).await?;
        let retained_masters = masters
            .iter()
            .filter(|node| retained.contains(node.client_addr.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let retained_replicas = replicas
            .iter()
            .filter(|candidate| retained.contains(candidate.node.client_addr.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        let desired = self
            .select_persistence_topology(
                &retained_masters,
                &retained_replicas,
                retained.len(),
                false,
            )
            .await?
            .context(
                "configuration persistence handoff cannot find a healthy retained master or replica",
            )?;
        if desired
            .snapshot_owners
            .iter()
            .any(|owner| !retained.contains(owner.node.client_addr.as_str()))
        {
            bail!("configuration persistence handoff selected a removed HA node");
        }
        self.apply_persistence_topology(desired, &masters, &replicas)
            .await?;

        let converged = self.persistence_topology.read().await;
        if converged.snapshot_owners.is_empty()
            || converged
                .snapshot_owners
                .iter()
                .any(|owner| !owner.verified || !retained.contains(owner.node.client_addr.as_str()))
        {
            bail!("configuration persistence handoff did not verify a retained owner");
        }
        Ok(())
    }

    /// 业务作用: 在配置关闭动态持久化前停用全部后端节点的持久化责任。
    ///
    /// 返回: 全部节点均确认进入 standby 或 suspended 状态、协调动作完成后返回成功。
    pub async fn planned_persistence_disable(&self) -> anyhow::Result<()> {
        self.ensure_not_retired("before configuration persistence disable")?;
        if self.persistence.policy == HaPersistencePolicy::None {
            return Ok(());
        }
        let _operation = self.acquire_operation().await?;
        let (masters, replicas) = self.discover_roles_with_replica_lag().await;
        if masters.len() != 1 {
            bail!(
                "configuration persistence disable requires exactly one healthy master, observed {}",
                masters.len()
            );
        }
        // 关闭整个持久化编排是不可延后清理的生命周期边界：任一配置节点缺席
        // 都无法证明其 owner 已停用，因此在取得动作权威前直接拒绝事务。
        let discovered = masters
            .iter()
            .map(|node| node.client_addr.as_str())
            .chain(
                replicas
                    .iter()
                    .map(|candidate| candidate.node.client_addr.as_str()),
            )
            .collect::<HashSet<_>>();
        let unavailable = self
            .ha
            .nodes()
            .iter()
            .filter(|node| !discovered.contains(node.client_addr.as_str()))
            .map(|node| node.client_addr.clone())
            .collect::<Vec<_>>();
        if !unavailable.is_empty() {
            bail!(
                "configuration persistence disable requires every HA node to be reachable so no owner escapes management; unavailable: {}",
                unavailable.join(",")
            );
        }
        let current_master = masters[0].client_addr.clone();
        let (lease, mut cluster_action) =
            if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
                let action = self
                    .acquire_cluster_manual_action(
                        CoordinationActionKind::PersistenceDisable,
                        "persistence-disable",
                        current_master.clone(),
                        current_master.clone(),
                    )
                    .await?;
                (None, Some(action))
            } else {
                (
                    self.acquire_failover_lease("configuration persistence disable")
                        .await?,
                    None,
                )
            };
        self.ensure_persistence_authority(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before disabling all persistence owners",
        )
        .await?;
        let desired = DesiredPersistenceTopology {
            kind: PersistenceTopologyKind::None,
            hot_standby: None,
            snapshot_owners: Vec::new(),
            replica_links: Vec::new(),
        };
        self.disable_non_owner_persistence(
            &desired,
            &masters,
            &replicas,
            Vec::new(),
            lease.as_ref(),
            cluster_action.as_ref(),
        )
        .await
        .context("configuration persistence disable failed")?;
        if !self
            .persistence_remote_state_converged(&desired, &masters, &replicas)
            .await
        {
            bail!("configuration persistence disable did not converge on every HA node");
        }
        if let Some(action) = cluster_action.as_mut() {
            action.complete_success(&current_master).await?;
        } else {
            self.ensure_persistence_authority(
                lease.as_ref(),
                None,
                "before publishing disabled persistence topology",
            )
            .await?;
        }
        *self.persistence_topology.write().await = PersistenceTopologyState::default();
        self.clear_persistence_issue().await;
        Ok(())
    }

    /// 业务作用: 执行自动选择或明确目标的健康主节点计划切换。
    ///
    /// 参数说明:
    /// - `retained_targets`: 指定保留集合时只允许集合内的健康副本接管；空值使用常规候选选择策略。
    /// - `purpose`: 日志、协调动作和错误文本使用的非敏感操作名称。
    ///
    /// 返回: 新主路由、旧主降级和持久化责任全部收敛后的状态。
    async fn planned_switchover_target(
        &self,
        retained_targets: Option<&[String]>,
        purpose: &str,
    ) -> anyhow::Result<ReplicationHaStatus> {
        self.ensure_not_retired("before planned switchover")?;
        self.ensure_not_observer("planned switchover")?;
        let _operation = self.acquire_operation().await?;
        let started = Instant::now();
        let current = self.current_master.read().await.clone();
        if self.node_role(&current).await? != HaRole::Master {
            bail!(
                "replication HA current master `{}` is not healthy; use failover for unplanned master changes",
                current.client_addr
            );
        }
        let lease = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            None
        } else {
            self.acquire_failover_lease(purpose).await?
        };

        let (masters, replicas) = self.discover_roles_with_replica_lag().await;
        if masters.len() > 1 {
            let reason = format!(
                "multiple replication HA masters detected: {}",
                format_node_addrs(&masters)
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        let candidate = match retained_targets {
            Some(retained_targets) => {
                let retained = retained_targets
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                let retained_replicas = replicas
                    .into_iter()
                    .filter(|candidate| retained.contains(candidate.node.client_addr.as_str()))
                    .collect::<Vec<_>>();
                self.select_failover_candidate(retained_replicas, Some(&current), purpose)
                    .await
                    .context(
                        "no retained HA node is an eligible replica for configuration switchover",
                    )?
            }
            None => {
                self.select_failover_candidate(replicas, Some(&current), purpose)
                    .await?
            }
        };

        let mut cluster_action = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            Some(
                self.acquire_cluster_manual_action(
                    CoordinationActionKind::Failover,
                    purpose,
                    current.client_addr.clone(),
                    candidate.client_addr.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before planned switchover promotion",
        )
        .await?;
        self.ensure_not_retired("before the product planned switchover")?;
        let authority = RuntimeActionAuthority {
            runtime: self,
            lease: lease.as_ref(),
            cluster_action: cluster_action.as_ref(),
        };
        let result = self
            .ha
            .backend()
            .planned_switchover(&current, &candidate, &authority)
            .await;
        self.protect_ha_control_step(result, "during planned switchover promotion")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after planned switchover promotion",
        )
        .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before planned switchover convergence",
        )
        .await?;
        let result = self
            .demote_extra_masters_checked(&candidate, lease.as_ref(), cluster_action.as_ref(), None)
            .await;
        self.protect_ha_control_step(result, "during planned switchover convergence")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after planned switchover convergence",
        )
        .await?;
        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing planned switchover")
                .await?;
            self.complete_cluster_action_or_protect(
                action,
                &candidate.client_addr,
                "after planned switchover",
            )
            .await?;
        }
        if let Some(lease) = lease.as_ref() {
            let result = self.record_failover_outcome(lease, &candidate).await;
            self.protect_ha_control_step(result, "while publishing planned switchover outcome")
                .await?;
        }
        self.finish_master_switch(current.client_addr, candidate, started, purpose)
            .await?;
        Ok(self.status().await)
    }

    /// 业务作用: 通过管理接口提升指定节点，同时复用完整权威、隔离和拓扑复验链。
    /// 参数说明:
    /// - `client_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    /// 返回: 动作完成、拓扑复验并发布后返回最新 HA 状态；失权、隔离或收敛失败时返回错误。
    pub async fn promote_node(&self, client_addr: &str) -> anyhow::Result<ReplicationHaStatus> {
        // 晋升前先排除配置事务，避免旧代把已移出候选的节点重新变成主节点。
        let _control = self.acquire_regular_control().await?;
        self.ensure_not_retired("before manual promotion")?;
        self.ensure_not_observer("manual promotion")?;
        let _operation = self.acquire_operation().await?;
        let started = Instant::now();
        let current = self.current_master_addr().await;
        let current_node = self.find_node(&current)?;
        let node = self.find_node(client_addr)?;
        let lease = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            None
        } else {
            self.acquire_failover_lease("manual promote").await?
        };
        let mut cluster_action = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            Some(
                self.acquire_cluster_manual_action(
                    CoordinationActionKind::Failover,
                    "promote",
                    current.clone(),
                    node.client_addr.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual promotion fencing",
        )
        .await?;
        if node.client_addr != current {
            let result = self
                .fence_node_inner_with_token(
                    &current_node,
                    "manual promotion old master fencing",
                    lease.as_ref(),
                    cluster_action.as_ref(),
                )
                .await;
            self.protect_ha_control_step(result, "during manual promotion fencing")
                .await?;
            self.ensure_automatic_authority_or_protect(
                lease.as_ref(),
                cluster_action.as_ref(),
                "after manual promotion fencing",
            )
            .await?;
        }
        // 软隔离下与自动切主同一时间围栏：等分区成员的旧路由先断路再提主。
        self.wait_soft_fencing_route_barrier(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after the manual promotion route barrier",
        )
        .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual promotion",
        )
        .await?;
        let result = self
            .promote(&node, lease.as_ref(), cluster_action.as_ref())
            .await;
        self.protect_ha_control_step(result, "during manual promotion")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after manual promotion",
        )
        .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual promotion convergence",
        )
        .await?;
        let result = self
            .demote_extra_masters_checked(&node, lease.as_ref(), cluster_action.as_ref(), None)
            .await;
        self.protect_ha_control_step(result, "during manual promotion convergence")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after manual promotion convergence",
        )
        .await?;
        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing manual promotion")
                .await?;
            self.complete_cluster_action_or_protect(
                action,
                &node.client_addr,
                "after manual promotion",
            )
            .await?;
        }
        if let Some(lease) = lease.as_ref() {
            let result = self.record_failover_outcome(lease, &node).await;
            self.protect_ha_control_step(result, "while publishing manual promotion outcome")
                .await?;
        }
        self.finish_master_switch(current, node, started, "manual promote")
            .await?;
        Ok(self.status().await)
    }

    /// 业务作用: 通过管理接口安全改写指定节点的复制上游并等待观测收敛。
    /// 参数说明:
    /// - `client_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `master_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    /// 返回: 动作完成、拓扑复验并发布后返回最新 HA 状态；失权、隔离或收敛失败时返回错误。
    pub async fn replicaof_node(
        &self,
        client_addr: &str,
        master_addr: &str,
    ) -> anyhow::Result<ReplicationHaStatus> {
        // 复制指派不能跨越配置预提交到激活的窗口，否则会覆盖候选已验证的上游。
        let _control = self.acquire_regular_control().await?;
        self.ensure_not_retired("before manual replica assignment")?;
        self.ensure_not_observer("manual replica assignment")?;
        let _operation = self.acquire_operation().await?;
        let started = Instant::now();
        let node = self.find_node(client_addr)?;
        let master = self.find_node(master_addr)?;
        if node.client_addr == master.client_addr {
            bail!("HA node `{client_addr}` cannot replicate from itself");
        }
        if self.node_role(&master).await? != HaRole::Master {
            bail!("HA target `{master_addr}` is not a healthy master");
        }
        let lease = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            None
        } else {
            self.acquire_failover_lease("manual replicaof").await?
        };

        let current = self.current_master_addr().await;
        let resulting_master = if current == node.client_addr {
            master.client_addr.clone()
        } else {
            current.clone()
        };
        let mut cluster_action = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            Some(
                self.acquire_cluster_manual_action(
                    CoordinationActionKind::TopologyRepair,
                    "replicaof",
                    current.clone(),
                    resulting_master.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual replica assignment",
        )
        .await?;
        let result = self
            .replicate_from(&node, &master, lease.as_ref(), cluster_action.as_ref())
            .await;
        self.protect_ha_control_step(result, "during manual replica assignment")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after manual replica assignment",
        )
        .await?;
        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing replica assignment")
                .await?;
            self.complete_cluster_action_or_protect(
                action,
                &resulting_master,
                "after manual replica assignment",
            )
            .await?;
        }
        if current == node.client_addr {
            if let Some(lease) = lease.as_ref() {
                let result = self.record_failover_outcome(lease, &master).await;
                self.protect_ha_control_step(
                    result,
                    "while publishing manual replica assignment outcome",
                )
                .await?;
            }
            self.finish_master_switch(current, master, started, "manual replicaof")
                .await?;
        } else {
            self.master_failure_streak.store(0, Ordering::Relaxed);
            let serving = matches!(&*self.routing_tx.borrow(), HaRoutingState::Serving { .. });
            if serving {
                self.set_last_error(None).await;
            }
        }
        Ok(self.status().await)
    }

    /// 业务作用: 通过管理接口对指定节点执行外部隔离，并在副作用前后复验动作权威。
    /// 参数说明:
    /// - `client_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    /// 返回: 动作完成、拓扑复验并发布后返回最新 HA 状态；失权、隔离或收敛失败时返回错误。
    pub async fn fence_node(&self, client_addr: &str) -> anyhow::Result<ReplicationHaStatus> {
        // 配置交接中的角色判断由事务负责，管理入口不得并发隔离其候选节点。
        let _control = self.acquire_regular_control().await?;
        self.ensure_not_retired("before manual fencing")?;
        self.ensure_not_observer("manual fencing")?;
        let _operation = self.acquire_operation().await?;
        let node = self.find_node(client_addr)?;
        let current = self.current_master_addr().await;
        let lease = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            None
        } else {
            self.acquire_failover_lease("manual fence").await?
        };
        let mut cluster_action = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            Some(
                self.acquire_cluster_manual_action(
                    CoordinationActionKind::TopologyRepair,
                    "fence",
                    current.clone(),
                    current.clone(),
                )
                .await?,
            )
        } else {
            None
        };
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before manual fencing",
        )
        .await?;
        let result = self
            .fence_node_inner_with_token(
                &node,
                "manual admin fence",
                lease.as_ref(),
                cluster_action.as_ref(),
            )
            .await;
        self.protect_ha_control_step(result, "during manual fencing")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease.as_ref(),
            cluster_action.as_ref(),
            "after manual fencing",
        )
        .await?;
        if node.client_addr == current {
            self.enter_protection(format!(
                "replication HA current master `{current}` was fenced by an administrator"
            ))
            .await;
            if let Some(action) = cluster_action.as_mut() {
                action.complete_failure(&current).await?;
            }
        } else if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing manual fencing")
                .await?;
            self.complete_cluster_action_or_protect(action, &current, "after manual fencing")
                .await?;
        }
        Ok(self.status().await)
    }
}
