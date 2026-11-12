impl ReplicationHaRuntime {
    /// 业务作用: 收敛 replication-ha 动态持久化拓扑，确保单节点 master 或多节点 replica 的 snapshot owner、热备和非 owner 配置符合当前健康节点数量。
    /// 参数说明:
    /// - `masters`: 本轮探测到的 master 节点列表，用于确认当前可服务 master 和 master fallback。
    /// - `replicas`: 本轮探测到的副本候选，函数会按 lag、稳定时间和跟随关系筛选。
    ///
    /// 返回: 收敛成功返回 `Ok`；无法找到安全拓扑时按 `fail_closed` 策略告警或进入保护态。
    async fn reconcile_persistence(
        &self,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
    ) -> anyhow::Result<()> {
        if self.persistence.policy == HaPersistencePolicy::None {
            *self.persistence_topology.write().await = PersistenceTopologyState::default();
            self.clear_persistence_issue().await;
            return Ok(());
        }
        if now_ms() < self.persistence_retry_not_before_ms.load(Ordering::Relaxed) {
            if let Some(reason) = self.persistence_protected_reason.read().await.clone() {
                // 已经因硬拓扑约束或 fail-closed 进入保护后，退避窗口只能减少
                // 控制命令重试，不能让后续 ensure_serving 提前重新开放数据面。
                bail!(reason);
            }
            if self.persistence.fail_closed {
                let reason = self
                    .persistence_last_error
                    .read()
                    .await
                    .clone()
                    .unwrap_or_else(|| "HA persistence retry is deferred".to_string());
                bail!(reason);
            }
            return Ok(());
        }

        self.update_replica_seen_since(replicas).await?;
        let Some(desired) = self
            .select_persistence_topology(masters, replicas, self.ha.nodes().len(), true)
            .await?
        else {
            return self
                .handle_persistence_topology_unavailable(
                    "HA persistence cannot satisfy the configured node topology with a healthy master and stable replica".to_string(),
                )
                .await;
        };
        self.apply_persistence_topology(desired, masters, replicas)
            .await
    }

    /// 业务作用: 将已经完成候选筛选的持久化拓扑按启用、快照、验证、停用旧 owner 的顺序发布。
    ///
    /// 参数说明:
    /// - `desired`: 已通过复制延迟、稳定窗口和保留节点约束的目标持久化拓扑。
    /// - `masters`: 本轮探测到的完整主节点列表，用于停用不再承担责任的旧节点。
    /// - `replicas`: 本轮探测到的完整副本列表，用于停用旧 owner 并复验复制链路。
    ///
    /// 返回: 新 owner 验证成功且旧 owner 已安全停用时返回成功。
    async fn apply_persistence_topology(
        &self,
        desired: DesiredPersistenceTopology,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
    ) -> anyhow::Result<()> {
        if self.persistence_topology_matches(&desired).await
            && self
                .persistence_remote_state_converged(&desired, masters, replicas)
                .await
            && self.persistence_cluster_commit_matches(&desired)
        {
            self.clear_persistence_issue().await;
            return Ok(());
        }

        if self
            .adopt_observed_persistence_topology(&desired, masters, replicas)
            .await
        {
            return Ok(());
        }

        if let Some(cluster) = &self.cluster_coordinator {
            if !cluster.handle.is_local_leader().await {
                // 跟随者只观察并采用领导者已经完成的远端拓扑，不与领导者并发修改快照责任节点。
                // 下个探测周期会再次尝试 adopt。已有已验证 owner 时可继续沿用；
                // 冷启动尚无任何已验证 owner 时，fail-closed 必须保持保护，不能把
                // “领导者可能正在处理”误当成持久化已经成立。
                if !previous_topology_has_verified_owner(&*self.persistence_topology.read().await) {
                    return self
                        .handle_persistence_failure(
                            "HA persistence waits for the proxy-cluster leader to commit and verify an initial owner"
                                .to_string(),
                        )
                        .await;
                }
                return Ok(());
            }
        }

        let (lease, mut cluster_action) = if self.coordinator.mode
            == HaCoordinatorMode::ProxyCluster
        {
            match self.acquire_cluster_persistence_action(&desired).await {
                Ok(action) => (None, Some(action)),
                Err(error) => {
                    return self
                        .handle_persistence_failure(format!(
                            "HA persistence handoff cannot acquire proxy-cluster action: {error}"
                        ))
                        .await;
                }
            }
        } else {
            let lease = match self.acquire_failover_lease("persistence handoff").await {
                Ok(lease) => lease,
                Err(err) => {
                    if self
                        .adopt_observed_persistence_topology(&desired, masters, replicas)
                        .await
                    {
                        return Ok(());
                    }
                    if matches!(self.coordinator_lease_is_occupied_result().await, Ok(true)) {
                        // 其它 proxy 持锁只证明“有人正在处理”，不证明持久化已经
                        // 收敛。已有已验证 owner 时可等待远端 handoff；冷启动尚无
                        // 已验证 owner 时必须建立 fail-closed，不能先开放业务路由。
                        if !previous_topology_has_verified_owner(
                            &*self.persistence_topology.read().await,
                        ) {
                            return self
                                    .handle_persistence_failure(format!(
                                        "HA persistence handoff is owned by another proxy and no verified owner is available locally: {err}"
                                    ))
                                    .await;
                        }
                        // 保留既有错误/保护原因，直到远端状态可被 adopt；否则
                        // 本轮返回后 ensure_serving 会提前解除 fail-closed。
                        return Ok(());
                    }
                    return self
                        .handle_persistence_failure(format!(
                            "HA persistence handoff cannot acquire coordinator lease: {err}"
                        ))
                        .await;
                }
            };
            (lease, None)
        };
        self.ensure_persistence_authority(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before enabling new owner",
        )
        .await?;

        let previous_topology = self.persistence_topology.read().await.clone();
        if self.persistence_topology_matches(&desired).await
            && self
                .persistence_remote_state_converged(&desired, masters, replicas)
                .await
            && self.persistence_cluster_commit_matches(&desired)
        {
            self.clear_persistence_issue().await;
            return Ok(());
        }
        if self
            .adopt_observed_persistence_topology(&desired, masters, replicas)
            .await
        {
            return Ok(());
        }

        let mut enabled_nodes = Vec::new();
        let mut handoff_error = None;
        if let Err(err) = self
            .ensure_desired_replica_links(
                &desired,
                replicas,
                lease.as_ref(),
                cluster_action.as_ref(),
            )
            .await
        {
            handoff_error = Some(format!(
                "HA persistence failed to converge replica links: {err}"
            ));
        }

        if handoff_error.is_none() {
            for owner in &desired.snapshot_owners {
                self.ensure_persistence_authority(
                    lease.as_ref(),
                    cluster_action.as_ref(),
                    "before enabling new owner",
                )
                .await?;
                if let Err(err) = self
                    .run_persistence_action(
                        PersistenceAction::Enable,
                        &owner.node,
                        owner.role,
                        lease.as_ref(),
                        cluster_action.as_ref(),
                    )
                    .await
                {
                    self.persistence_enable_failure_count
                        .fetch_add(1, Ordering::Relaxed);
                    handoff_error = Some(format!(
                        "HA persistence failed to enable owner on `{}`: {err:#}",
                        owner.node.client_addr
                    ));
                    break;
                }
                self.persistence_enable_count
                    .fetch_add(1, Ordering::Relaxed);
                enabled_nodes.push((owner.node.clone(), owner.role));

                self.ensure_persistence_authority(
                    lease.as_ref(),
                    cluster_action.as_ref(),
                    "before snapshot/verify",
                )
                .await?;
                let checkpoint_token = if self.persistence.force_snapshot_on_handoff {
                    let authority = RuntimePersistenceAuthority {
                        runtime: self,
                        lease: lease.as_ref(),
                        cluster_action: cluster_action.as_ref(),
                    };
                    match self
                        .ha
                        .backend()
                        .force_checkpoint(&owner.node, CheckpointReason::Handoff, &authority)
                        .await
                    {
                        Ok(token) => {
                            self.persistence_snapshot_count
                                .fetch_add(1, Ordering::Relaxed);
                            self.persistence_last_snapshot_ms
                                .store(now_ms(), Ordering::Relaxed);
                            Some(token)
                        }
                        Err(err) => {
                            self.persistence_snapshot_failure_count
                                .fetch_add(1, Ordering::Relaxed);
                            handoff_error = Some(format!(
                                "HA persistence checkpoint failed on `{}`: {err:#}",
                                owner.node.client_addr
                            ));
                            break;
                        }
                    }
                } else {
                    None
                };

                let started = Instant::now();
                let authority = RuntimePersistenceAuthority {
                    runtime: self,
                    lease: lease.as_ref(),
                    cluster_action: cluster_action.as_ref(),
                };
                let verification = match checkpoint_token.as_ref() {
                    Some(token) => self
                        .ha
                        .backend()
                        .verify_checkpoint(&owner.node, token, &authority)
                        .await
                        .map(|_| ()),
                    None => {
                        self.verify_persistence_owner(
                            &owner.node,
                            owner.role,
                            lease.as_ref(),
                            cluster_action.as_ref(),
                        )
                        .await
                    }
                };
                match verification {
                    Ok(()) => {
                        self.persistence_verify_count
                            .fetch_add(1, Ordering::Relaxed);
                        self.persistence_last_verify_duration_micros
                            .store(elapsed_micros(started), Ordering::Relaxed);
                    }
                    Err(err) => {
                        self.persistence_verify_failure_count
                            .fetch_add(1, Ordering::Relaxed);
                        self.persistence_last_verify_duration_micros
                            .store(elapsed_micros(started), Ordering::Relaxed);
                        handoff_error = Some(format!(
                            "HA persistence checkpoint verification failed on `{}`: {err:#}",
                            owner.node.client_addr
                        ));
                        break;
                    }
                }
            }
        }

        if let Some(error) = handoff_error {
            self.set_persistence_error(Some(error.clone())).await;
            let has_previous_owner = self
                .previous_persistence_topology_has_usable_owner(
                    &previous_topology,
                    masters,
                    replicas,
                )
                .await;
            let previous_owner_addrs = previous_topology
                .snapshot_owners
                .iter()
                .filter(|owner| owner.verified)
                .map(|owner| owner.node.client_addr.as_str())
                .collect::<HashSet<_>>();
            let mut rollback_failure = None;
            // 只回滚本轮新引入的 owner；与上一版已验证拓扑重叠的 owner 必须保留，
            // 否则一次新增 owner 失败会反过来关闭仍承担持久化责任的旧 owner。
            for (node, role) in enabled_nodes {
                if previous_owner_addrs.contains(node.client_addr.as_str()) {
                    continue;
                }
                if let Err(rollback_err) = self
                    .run_persistence_action(
                        PersistenceAction::Disable,
                        &node,
                        role,
                        lease.as_ref(),
                        cluster_action.as_ref(),
                    )
                    .await
                {
                    rollback_failure = Some(format!(
                        "failed to roll back new owner `{}`: {rollback_err:#}",
                        node.client_addr
                    ));
                    break;
                }
            }
            if let Some(rollback_error) = rollback_failure {
                return self
                    .handle_persistence_failure(format!("{error}; HA persistence {rollback_error}"))
                    .await;
            }
            if has_previous_owner {
                warn!(
                    proxy = %self.name,
                    desired_topology = %desired.kind.as_str(),
                    error = %error,
                    "HA persistence keeps previous verified owner after failed handoff"
                );
                return Ok(());
            }
            return self.handle_persistence_failure(error).await;
        }

        self.ensure_persistence_authority(
            lease.as_ref(),
            cluster_action.as_ref(),
            "before disabling old owner",
        )
        .await?;
        if let Err(error) = self
            .disable_non_owner_persistence(
                &desired,
                masters,
                replicas,
                Vec::new(),
                lease.as_ref(),
                cluster_action.as_ref(),
            )
            .await
        {
            return self
                .handle_persistence_failure(format!(
                    "replication HA persistence failed to disable a non-owner: {error:#}"
                ))
                .await;
        }

        if let Some(action) = cluster_action.as_mut() {
            let current_master = self.current_master_addr().await;
            if let Err(error) = action.complete_success(&current_master).await {
                return self
                    .handle_persistence_failure(format!(
                        "replication HA persistence result was not committed through proxy-cluster: {error}"
                    ))
                    .await;
            }
        } else {
            self.ensure_persistence_authority(
                lease.as_ref(),
                None,
                "before publishing the verified persistence topology",
            )
            .await?;
        }

        let topology_changed = !persistence_topology_state_matches(&previous_topology, &desired);
        *self.persistence_topology.write().await = desired.to_verified_state();
        if topology_changed {
            self.persistence_handoff_count
                .fetch_add(1, Ordering::Relaxed);
            self.persistence_last_handoff_ms
                .store(now_ms(), Ordering::Relaxed);
        }
        self.clear_persistence_issue().await;
        Ok(())
    }

    /// 业务作用: 把远端已收敛且符合策略的持久化布局采纳为本地已验证状态，避免无谓改写。
    /// 参数说明:
    /// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
    /// - `masters`: 当前探测到的 master 节点集合，用于单主校验、降级和保护模式判断。
    /// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
    ///
    async fn adopt_observed_persistence_topology(
        &self,
        desired: &DesiredPersistenceTopology,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
    ) -> bool {
        if self.persistence.force_snapshot_on_handoff && self.cluster_coordinator.is_none() {
            // local-file/Redis 协调器不保存 checkpoint 验证结果。进程重启或旧 owner
            // 失权后，仅看到 CONFIG/cron 已启用不能证明强制持久化点完成；必须重新
            // 取得租约并执行 checkpoint。proxy-cluster 的 CompleteAction 则提供了
            // 可跨进程恢复的验证提交证据。
            return false;
        }
        if !self.persistence_cluster_commit_matches(desired) {
            // proxy-cluster 后端改写先于 CompleteAction。跟随者只能采纳已经由
            // 共识记录 owner 且不存在在途动作的状态，不能把领导者的中间态
            // 洗成已验证拓扑；领导者会继续申请/恢复动作并完成提交。
            return false;
        }
        if !self
            .persistence_remote_state_converged(desired, masters, replicas)
            .await
        {
            return false;
        }
        *self.persistence_topology.write().await = desired.to_verified_state();
        self.persistence_adopt_count.fetch_add(1, Ordering::Relaxed);
        self.clear_persistence_issue().await;
        true
    }

    /// 业务作用: 只在 owner、热备、复制链路和产品持久化配置均匹配时认定远端拓扑收敛。
    /// 参数说明:
    /// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
    /// - `masters`: 当前探测到的 master 节点集合，用于单主校验、降级和保护模式判断。
    /// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
    ///
    async fn persistence_remote_state_converged(
        &self,
        desired: &DesiredPersistenceTopology,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
    ) -> bool {
        let mut recently_missing_replicas = HashSet::new();
        for link in &desired.replica_links {
            if self
                .recently_missing_verified_persistence_replica(&link.node, replicas)
                .await
            {
                recently_missing_replicas.insert(link.node.client_addr.clone());
            }
        }
        for owner in &desired.snapshot_owners {
            // 快照或 checkpoint 可能让一个已验证副本短暂错过角色探测；宽限内
            // 继续使用上一版 owner 事实，不能为了“验证”再向疑似阻塞节点写控制命令。
            if recently_missing_replicas.contains(&owner.node.client_addr) {
                continue;
            }
            match self
                .ha
                .backend()
                .inspect_persistence(&owner.node, owner.role)
                .await
            {
                Ok(state) if state.effective && state.target == Some(PersistenceTarget::Owner) => {}
                Ok(_) | Err(_) => return false,
            }
        }

        for link in &desired.replica_links {
            let Some(replica) = replicas
                .iter()
                .find(|replica| replica.node.client_addr == link.node.client_addr)
            else {
                if recently_missing_replicas.contains(&link.node.client_addr) {
                    continue;
                }
                return false;
            };
            if !replica_candidate_follows_node(replica, &link.upstream) {
                return false;
            }
        }

        let mut checked = Vec::new();
        for master in masters {
            if !self
                .persistence_non_owner_disabled(desired, master, HaRole::Master, &mut checked)
                .await
            {
                return false;
            }
        }
        for replica in replicas {
            if !self
                .persistence_non_owner_disabled(
                    desired,
                    &replica.node,
                    HaRole::Replica,
                    &mut checked,
                )
                .await
            {
                return false;
            }
        }
        // `masters`/`replicas` 只包含本轮可探测节点。上一版已发布 owner 若在
        // handoff 期间失联，不能因为它缺席于观测集合就被当作已停用；否则新
        // owner 会先发布，旧 owner 恢复后仍继续执行快照。缓存中的 owner 是
        // 我们确知可能带有持久化职责的最小集合，必须逐个确认已停用。
        let previous_owners = self
            .persistence_topology
            .read()
            .await
            .snapshot_owners
            .clone();
        for owner in previous_owners {
            if !self
                .persistence_non_owner_disabled(desired, &owner.node, owner.role, &mut checked)
                .await
            {
                return false;
            }
        }
        true
    }

    /// 业务作用: 判断失败 handoff 后是否仍有一个真实可用的上一版持久化 owner，避免历史缓存绕过 fail-closed。
    ///
    /// 本地 `verified=true` 只证明历史上验证成功，不能证明节点当前在线、角色未
    /// 改变且仍在跟随唯一主节点。只有重新满足这些条件并读回 owner 配置，才能
    /// 在新 owner 启用失败时继续沿用旧拓扑；否则 `fail_closed=true` 必须保护。
    /// 参数说明:
    /// - `previous`: 本轮 handoff 开始前已经发布的本地持久化拓扑。
    /// - `masters`: 本轮真实探测到的主节点集合。
    /// - `replicas`: 本轮真实探测到的副本角色、上游和延迟证据。
    ///
    /// 返回: 至少一个历史 owner 当前仍在线、角色一致、复制合格且后端配置有效时返回 `true`。
    async fn previous_persistence_topology_has_usable_owner(
        &self,
        previous: &PersistenceTopologyState,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
    ) -> bool {
        for owner in previous
            .snapshot_owners
            .iter()
            .filter(|owner| owner.verified)
        {
            // 先用本轮角色视图淘汰失联或已换角色的历史 owner；副本还必须继续
            // 直连唯一主并满足延迟门禁，历史 verified 标志不参与这一事实判断。
            let current_role = if masters
                .iter()
                .any(|master| master.client_addr == owner.node.client_addr)
            {
                HaRole::Master
            } else if let Some(replica) = replicas
                .iter()
                .find(|replica| replica.node.client_addr == owner.node.client_addr)
            {
                let Some(master) = masters.first().filter(|_| masters.len() == 1) else {
                    continue;
                };
                if !persistence_replica_eligible_for_upstream(
                    replica,
                    master,
                    self.persistence.require_zero_lag,
                    self.max_replica_lag_seconds,
                ) {
                    continue;
                }
                HaRole::Replica
            } else if owner.role == HaRole::Replica
                && masters.len() == 1
                && masters[0].client_addr != owner.node.client_addr
                && self
                    .recently_missing_verified_persistence_replica(&owner.node, replicas)
                    .await
            {
                // 仅在当前本地拓扑已经验证过该 owner、且最近成功副本探测仍在
                // 连败宽限内时沿用历史事实。宽限耗尽后仍必须重新读取角色和配置。
                return true;
            } else {
                continue;
            };
            if current_role != owner.role {
                continue;
            }
            // 最后从产品后端重新读取原生配置，只有当前事实仍是 Owner 才允许
            // 失败 handoff 沿用旧拓扑；读取失败一律视为不可用。
            if matches!(
                self.ha
                    .backend()
                    .inspect_persistence(&owner.node, current_role)
                    .await,
                Ok(state)
                    if state.effective
                        && state.target == Some(PersistenceTarget::Owner)
            ) {
                return true;
            }
        }
        false
    }

    /// 业务作用: 判断已验证持久化副本是否只是在本轮探测中短暂缺席，为 checkpoint 自身抖动提供有界宽限。
    /// 参数说明:
    /// - `node`: 本轮未得到副本观测的持久化节点。
    /// - `replicas`: 本轮成功探测到的副本集合。
    ///
    /// 返回: 节点属于当前已验证拓扑、最近成功探测未超过故障阈值窗口且本轮确实缺席时返回 `true`。
    async fn recently_missing_verified_persistence_replica(
        &self,
        node: &HaNode,
        replicas: &[ReplicaCandidate],
    ) -> bool {
        if replicas
            .iter()
            .any(|replica| replica.node.client_addr == node.client_addr)
        {
            return false;
        }
        let current = self.persistence_topology.read().await;
        let tracked_verified_replica = current.snapshot_owners.iter().any(|owner| {
            owner.verified
                && owner.role == HaRole::Replica
                && owner.node.client_addr == node.client_addr
        }) || current
            .hot_standby
            .as_ref()
            .is_some_and(|standby| standby.client_addr == node.client_addr);
        drop(current);
        if !tracked_verified_replica {
            return false;
        }
        let Ok(now) = now_unix_millis() else {
            return false;
        };
        let grace_ms = self
            .probe_interval_ms
            .saturating_mul(self.master_down_after_failures.max(1));
        self.replica_last_seen_ms
            .read()
            .await
            .get(&node.client_addr)
            .is_some_and(|seen| now.saturating_sub(*seen) < u128::from(grace_ms))
    }

    /// 业务作用: 确认全部非 owner 节点的动态持久化已经关闭。
    /// 参数说明:
    /// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    /// - `role`: 节点当前角色，用于让产品适配器解释目标持久化配置。
    /// - `checked`: 已完成校验的节点集合，避免拓扑遍历中重复探测同一节点。
    ///
    async fn persistence_non_owner_disabled(
        &self,
        desired: &DesiredPersistenceTopology,
        node: &HaNode,
        role: HaRole,
        checked: &mut Vec<String>,
    ) -> bool {
        if desired.is_snapshot_owner(&node.client_addr)
            || checked.iter().any(|addr| addr == &node.client_addr)
        {
            return true;
        }
        checked.push(node.client_addr.clone());
        matches!(
            self.ha.backend().inspect_persistence(node, role).await,
            Ok(state)
                if state.effective
                    && matches!(
                        state.target,
                        Some(PersistenceTarget::Standby)
                            | Some(PersistenceTarget::Suspended)
                    )
        )
    }

    /// 业务作用: 在发布新 owner 前逐节点停用非 owner 持久化，防止同一拓扑出现重复快照责任。
    /// 参数说明:
    /// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
    /// - `masters`: 当前探测到的 master 节点集合，用于单主校验、降级和保护模式判断。
    /// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
    /// - `visited`: 拓扑遍历中已经访问的节点集合，用于阻止环路和重复动作。
    ///
    async fn disable_non_owner_persistence(
        &self,
        desired: &DesiredPersistenceTopology,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
        mut visited: Vec<String>,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        for master in masters {
            self.disable_non_owner_node(
                desired,
                master,
                HaRole::Master,
                &mut visited,
                lease_guard,
                cluster_action,
            )
            .await?;
        }
        for replica in replicas {
            self.disable_non_owner_node(
                desired,
                &replica.node,
                HaRole::Replica,
                &mut visited,
                lease_guard,
                cluster_action,
            )
            .await?;
        }
        // 角色发现会省略不可达节点，但上一版 owner 不能因此从停用事务中消失。
        // 对它的写若因失联失败，调用方会保持旧本地拓扑并按 fail-closed 策略
        // 保护；绝不能先发布新 owner、等旧节点恢复后再碰运气收敛。
        let previous_owners = self
            .persistence_topology
            .read()
            .await
            .snapshot_owners
            .clone();
        for owner in previous_owners {
            self.disable_non_owner_node(
                desired,
                &owner.node,
                owner.role,
                &mut visited,
                lease_guard,
                cluster_action,
            )
            .await?;
        }
        Ok(())
    }

    /// 业务作用: 停用并复验单个非 owner 节点的产品持久化配置。
    /// 参数说明:
    /// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    /// - `role`: 节点在当前拓扑中的角色，用于选择路由、晋升、降级或快照动作。
    /// - `visited`: 拓扑遍历中已经访问的节点集合，用于阻止环路和重复动作。
    ///
    async fn disable_non_owner_node(
        &self,
        desired: &DesiredPersistenceTopology,
        node: &HaNode,
        role: HaRole,
        visited: &mut Vec<String>,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        if desired.is_snapshot_owner(&node.client_addr)
            || visited.iter().any(|addr| addr == &node.client_addr)
        {
            return Ok(());
        }
        visited.push(node.client_addr.clone());
        if let Err(err) = self
            .run_persistence_action(
                PersistenceAction::Disable,
                node,
                role,
                lease_guard,
                cluster_action,
            )
            .await
        {
            self.persistence_disable_failure_count
                .fetch_add(1, Ordering::Relaxed);
            let error = format!(
                "replication HA persistence topology `{}` is verified, but disabling non-owner `{}` failed: {err}",
                desired.kind.as_str(), node.client_addr
            );
            self.set_persistence_error(Some(error.clone())).await;
            warn!(proxy = %self.name, error = %error);
            bail!(error);
        }
        self.persistence_disable_count
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// 业务作用: 按目标配置节点数和健康副本数量选择持久化拓扑，保证多节点部署永不把持久化回退到主节点。
    /// 参数说明:
    /// - `masters`: 本轮健康 master 视图，优先使用当前已发布 master。
    /// - `replicas`: 本轮副本候选，必须满足复制延迟和稳定窗口后才参与拓扑选择。
    /// - `configured_node_count`: 本次选择对应的目标配置节点数量；热更新预提交必须使用候选配置数量。
    /// - `retain_recent`: 是否允许在短暂探测抖动时保留当前拓扑；删除节点的预提交必须关闭。
    ///
    /// 返回: 有安全 master 和满足布局约束的副本时返回目标拓扑；多节点暂时没有合格副本时返回 `None` 并保护数据面。
    async fn select_persistence_topology(
        &self,
        masters: &[HaNode],
        replicas: &[ReplicaCandidate],
        configured_node_count: usize,
        retain_recent: bool,
    ) -> anyhow::Result<Option<DesiredPersistenceTopology>> {
        let current = self.current_master.read().await.clone();
        let seen_since = self.replica_seen_since_ms.read().await;
        let now = now_unix_millis()?;
        let master = masters
            .iter()
            .find(|master| master.client_addr == current.client_addr)
            .cloned()
            .or_else(|| masters.first().cloned());
        let Some(master) = master else {
            return Ok(None);
        };
        let mut eligible = Vec::new();
        for replica in replicas {
            // 自适应持久化拓扑只使用已经追平并稳定了一段时间的 replica。刚上线或刚
            // 换角色的节点可能还在全量同步，立即让它承担热备或快照会造成
            // 后续 failover 读不到最新数据，或者把一次恢复抖动扩散到业务面。
            if !persistence_replica_eligible_for_upstream(
                replica,
                &master,
                self.persistence.require_zero_lag,
                self.max_replica_lag_seconds,
            ) {
                continue;
            }
            let seen = seen_since
                .get(&replica.node.client_addr)
                .copied()
                .unwrap_or(now);
            if now.saturating_sub(seen) < u128::from(self.persistence.min_replica_stable_ms) {
                continue;
            }
            let lag = replica.lag_seconds.unwrap_or(u64::MAX);
            eligible.push((replica.clone(), lag));
        }
        // 稳定时间只作为准入门禁，不能参与跨 proxy 排序：各进程首次看到同一
        // 副本的本地毫秒值不同，会让它们选出相反的 hot-standby/owner。
        eligible.sort_by(compare_persistence_candidate_rank);

        let eligible_client_addrs = eligible
            .iter()
            .map(|candidate| candidate.0.node.client_addr.clone())
            .collect::<HashSet<_>>();
        if retain_recent {
            if let Some(retained) = self
                .retain_recent_persistence_topology(&master, &eligible_client_addrs, now)
                .await
            {
                return Ok(Some(retained));
            }
        }
        let Some(layout) = select_persistence_layout(configured_node_count, eligible.len()) else {
            // 多节点部署没有合格副本时不能把持久化临时打开在 master 上。
            // fail-closed 会保持数据面保护，直到至少一个副本完成同步和稳定窗口。
            return Ok(None);
        };
        if layout == PersistenceLayout::SingleMasterSnapshot {
            return Ok(Some(DesiredPersistenceTopology {
                kind: PersistenceTopologyKind::SingleMasterSnapshot,
                hot_standby: None,
                snapshot_owners: vec![DesiredPersistenceOwner {
                    node: master,
                    role: HaRole::Master,
                }],
                replica_links: Vec::new(),
            }));
        }
        if layout == PersistenceLayout::SingleReplicaSnapshot {
            // 只有一个健康 replica 时不再额外划分热备，否则没有节点承担快照。
            // 此时该 replica 负责快照，failover 仍可在 master 故障时按 lag 规则选择它。
            let owner = eligible.remove(0).0.node;
            return Ok(Some(DesiredPersistenceTopology {
                kind: PersistenceTopologyKind::SingleReplicaSnapshot,
                hot_standby: None,
                snapshot_owners: vec![DesiredPersistenceOwner {
                    node: owner.clone(),
                    role: HaRole::Replica,
                }],
                replica_links: vec![DesiredReplicaLink {
                    node: owner,
                    upstream: master,
                }],
            }));
        }

        // 两个及以上健康 replica 时，第一个稳定低延迟 replica 留作无持久化热备，
        // 其余 replica 承担 snapshot。这样 master 不做快照，且切主候选不会被
        // snapshot I/O 干扰。
        let hot_standby = eligible.remove(0).0.node;
        let mut snapshot_owners = Vec::with_capacity(eligible.len());
        let mut replica_links = Vec::with_capacity(eligible.len() + 1);
        replica_links.push(DesiredReplicaLink {
            node: hot_standby.clone(),
            upstream: master.clone(),
        });
        for (replica, _) in eligible {
            snapshot_owners.push(DesiredPersistenceOwner {
                node: replica.node.clone(),
                role: HaRole::Replica,
            });
            replica_links.push(DesiredReplicaLink {
                node: replica.node,
                upstream: master.clone(),
            });
        }
        Ok(Some(DesiredPersistenceTopology {
            kind: PersistenceTopologyKind::HotStandbyReplicaSnapshots,
            hot_standby: Some(hot_standby),
            snapshot_owners,
            replica_links,
        }))
    }

    /// 业务作用: 在已验证副本仅短暂探测失败时保留当前持久化拓扑，避免 checkpoint 自身触发 owner 振荡。
    ///
    /// 参数说明:
    /// - `master`: 当前唯一健康主节点，保留拓扑中的副本仍应跟随该节点。
    /// - `eligible_client_addrs`: 本轮探测仍满足稳定性和复制延迟要求的副本业务地址。
    /// - `now`: 本轮拓扑选择使用的 Unix 毫秒时间，保证全部副本采用同一时间基准。
    ///
    /// 返回: 当前副本只在故障阈值宽限内短暂不可见时返回保留拓扑，否则返回空。
    async fn retain_recent_persistence_topology(
        &self,
        master: &HaNode,
        eligible_client_addrs: &HashSet<String>,
        now: u128,
    ) -> Option<DesiredPersistenceTopology> {
        let current = self.persistence_topology.read().await.clone();
        if matches!(
            current.kind,
            PersistenceTopologyKind::None | PersistenceTopologyKind::SingleMasterSnapshot
        ) {
            return None;
        }

        let mut replica_nodes = current
            .snapshot_owners
            .iter()
            .filter(|owner| owner.role == HaRole::Replica)
            .map(|owner| owner.node.clone())
            .collect::<Vec<_>>();
        if let Some(hot_standby) = current.hot_standby.as_ref() {
            if !replica_nodes
                .iter()
                .any(|node| node.client_addr == hot_standby.client_addr)
            {
                replica_nodes.push(hot_standby.clone());
            }
        }
        if replica_nodes.is_empty()
            || replica_nodes
                .iter()
                .any(|node| node.client_addr == master.client_addr)
            || replica_nodes
                .iter()
                .all(|node| eligible_client_addrs.contains(&node.client_addr))
        {
            return None;
        }

        let grace_ms = self
            .probe_interval_ms
            .saturating_mul(self.master_down_after_failures.max(1));
        let last_seen = self.replica_last_seen_ms.read().await;
        if replica_nodes.iter().any(|node| {
            last_seen
                .get(&node.client_addr)
                .is_none_or(|seen| now.saturating_sub(*seen) >= u128::from(grace_ms))
        }) {
            return None;
        }

        let replica_links = replica_nodes
            .into_iter()
            .map(|node| DesiredReplicaLink {
                node,
                upstream: master.clone(),
            })
            .collect();
        Some(DesiredPersistenceTopology {
            kind: current.kind,
            hot_standby: current.hot_standby,
            snapshot_owners: current
                .snapshot_owners
                .into_iter()
                .map(|owner| DesiredPersistenceOwner {
                    node: owner.node,
                    role: owner.role,
                })
                .collect(),
            replica_links,
        })
    }

    /// 业务作用: 判断当前已验证持久化拓扑是否等于目标拓扑，避免每个 tick 重复执行快照开关和 owner handoff。
    /// 参数说明:
    /// - `desired`: 本轮根据健康副本数量计算出的目标拓扑。
    ///
    async fn persistence_topology_matches(&self, desired: &DesiredPersistenceTopology) -> bool {
        let current = self.persistence_topology.read().await;
        persistence_topology_state_matches(&current, desired)
    }

    /// 业务作用: 确认进程级协调状态已经提交目标持久化 owner，且不存在尚未收口的并发动作。
    /// 旧协调模式没有共享持久化结果，实际后端状态与本地租约共同构成提交证据。
    fn persistence_cluster_commit_matches(&self, desired: &DesiredPersistenceTopology) -> bool {
        let Some(cluster) = &self.cluster_coordinator else {
            return true;
        };
        let desired_owner = desired
            .snapshot_owners
            .first()
            .map(|owner| owner.node.client_addr.as_str());
        let desired_fingerprint = persistence_topology_fingerprint(desired);
        cluster
            .handle
            .group_state(&cluster.group_id)
            .ok()
            .flatten()
            .is_some_and(|group| {
                group.active_action.is_none()
                    && group.persistence_owner.as_deref() == desired_owner
                    && group.persistence_topology_fingerprint.as_deref()
                        == Some(desired_fingerprint.as_str())
            })
    }

    /// 业务作用: 确认目标拓扑中的副本链路全部跟随期望 master，必要时先下发 `REPLICAOF` 再等待同步稳定。
    /// 参数说明:
    /// - `desired`: 目标持久化拓扑，包含热备和快照副本应跟随的上游关系。
    ///
    /// 返回: 所有副本链路稳定后返回成功；任一副本无法跟随或延迟不合格时返回错误。
    async fn ensure_desired_replica_links(
        &self,
        desired: &DesiredPersistenceTopology,
        replicas: &[ReplicaCandidate],
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        for link in &desired.replica_links {
            let observation = self.ha.backend().inspect_node(&link.node).await;
            if observation.is_err()
                && self
                    .recently_missing_verified_persistence_replica(&link.node, replicas)
                    .await
            {
                // 这里处于已经取得动作权威后的二次复验。最近已验证副本短暂
                // 不可探测时既不能发送 REPLICAOF，也不能假装完成新 handoff；
                // 返回错误后调用方保留上一版 owner，等待下轮新观测。
                bail!(
                    "persistence replica `{}` is temporarily unavailable within its verified probe grace; keep the previous topology without issuing REPLICAOF",
                    link.node.client_addr
                );
            }
            let already_following = observation
                .as_ref()
                .is_ok_and(|observation| observation.follows(&link.upstream));
            if !already_following {
                self.replicate_from(&link.node, &link.upstream, lease_guard, cluster_action)
                    .await
                    .with_context(|| {
                        format!(
                            "set persistence replica `{}` to follow `{}`",
                            link.node.client_addr, link.upstream.replication_addr
                        )
                    })?;
            }
            self.wait_for_replica_following(
                &link.node,
                &link.upstream,
                lease_guard,
                cluster_action,
            )
            .await?;
            let authority = RuntimeActionAuthority {
                runtime: self,
                lease: lease_guard,
                cluster_action,
            };
            self.ha
                .backend()
                .verify_replication(&link.node, &link.upstream, &authority)
                .await?;
        }
        Ok(())
    }

    /// 业务作用: 等待副本跟随指定上游并达到可接受复制延迟，避免刚改 `REPLICAOF` 就立即启用快照或热备。
    /// 参数说明:
    /// - `node`: 需要验证的副本节点。
    /// - `upstream`: 期望该副本正在跟随的上游节点。
    ///
    /// 返回: 副本在超时内进入稳定跟随状态则成功，否则返回错误阻止 handoff。
    async fn wait_for_replica_following(
        &self,
        node: &HaNode,
        upstream: &HaNode,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
    ) -> anyhow::Result<()> {
        let timeout = Duration::from_millis(self.persistence.verify_timeout_ms.max(500));
        let deadline = Instant::now() + timeout;
        loop {
            self.ensure_persistence_authority(
                lease_guard,
                cluster_action,
                "while waiting for persistence replica convergence",
            )
            .await?;
            if let Ok(observation) = self.ha.backend().inspect_node(node).await {
                let candidate = ReplicaCandidate {
                    node: node.clone(),
                    lag_seconds: observation.replica_lag_seconds,
                    link_down_seconds: observation.link_down_since_seconds,
                    master_addr: observation.upstream_addr,
                };
                if observation.role == HaRole::Replica
                    && replica_candidate_follows_node(&candidate, upstream)
                    && persistence_replica_lag_allowed(
                        &candidate,
                        self.persistence.require_zero_lag,
                        self.max_replica_lag_seconds,
                    )
                {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "replication HA replica `{}` did not follow `{}` within {}ms",
                    node.client_addr,
                    upstream.replication_addr,
                    self.persistence.verify_timeout_ms
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 业务作用: 记录每个可见副本首次连续出现时间，用最小稳定窗口过滤刚重启或刚换上游的节点。
    /// 参数说明:
    /// - `replicas`: 本轮探测到的副本候选列表。
    ///
    /// 返回: 时间戳读取或状态更新失败时返回错误。
    async fn update_replica_seen_since(&self, replicas: &[ReplicaCandidate]) -> anyhow::Result<()> {
        let now = now_unix_millis()?;
        let current_master = self.current_master.read().await.clone();
        let eligible = replicas
            .iter()
            .filter(|replica| {
                persistence_replica_eligible_for_upstream(
                    replica,
                    &current_master,
                    self.persistence.require_zero_lag,
                    self.max_replica_lag_seconds,
                )
            })
            .collect::<Vec<_>>();
        let live = eligible
            .iter()
            .map(|replica| replica.node.client_addr.as_str())
            .collect::<Vec<_>>();
        let mut seen_since = self.replica_seen_since_ms.write().await;
        seen_since.retain(|addr, _| live.iter().any(|live_addr| live_addr == addr));
        for replica in &eligible {
            seen_since
                .entry(replica.node.client_addr.clone())
                .or_insert(now);
        }
        drop(seen_since);

        let configured = self
            .ha
            .nodes()
            .iter()
            .map(|node| node.client_addr.as_str())
            .collect::<HashSet<_>>();
        let mut last_seen = self.replica_last_seen_ms.write().await;
        // 已经成为当前 master 的节点不再具备 replica 身份，不能继续携带上一轮
        // `last_seen` 宽限。否则外部切主后，旧 snapshot owner 可能被短暂保留成
        // 指向自己的 replica link，随后 `REPLICAOF self` 会把唯一新主降级。
        last_seen.retain(|addr, _| {
            configured.contains(addr.as_str()) && addr != &current_master.client_addr
        });
        for replica in eligible {
            last_seen.insert(replica.node.client_addr.clone(), now);
        }
        Ok(())
    }

    /// 业务作用: 处理无法形成合法持久化拓扑的硬失败，不受可选告警策略影响并始终关闭数据面。
    /// 参数说明:
    /// - `reason`: 触发拓扑保护的具体原因，会写入状态接口和保护态说明。
    ///
    /// 返回: 始终进入保护态并返回错误，阻止后续逻辑重新发布主节点路由。
    async fn handle_persistence_topology_unavailable(&self, reason: String) -> anyhow::Result<()> {
        self.set_persistence_error(Some(reason.clone())).await;
        let same_reason =
            self.persistence_protected_reason.read().await.as_deref() == Some(reason.as_str());
        if !same_reason {
            self.persistence_protection_count
                .fetch_add(1, Ordering::Relaxed);
        }
        *self.persistence_protected_reason.write().await = Some(reason.clone());
        self.enter_protection(format!("replication HA persistence protected: {reason}"))
            .await;
        bail!(reason);
    }

    /// 业务作用: 统一处理持久化动作失败，根据 `fail_closed` 决定只记录告警还是进入全局保护态。
    /// 参数说明:
    /// - `reason`: 触发失败的具体原因，会写入状态接口和保护态说明。
    ///
    /// 返回: `fail_closed=false` 时返回成功并保持业务路由；`fail_closed=true` 时进入保护态并返回错误。
    async fn handle_persistence_failure(&self, reason: String) -> anyhow::Result<()> {
        self.set_persistence_error(Some(reason.clone())).await;
        if self.persistence.fail_closed {
            let same_reason =
                self.persistence_protected_reason.read().await.as_deref() == Some(reason.as_str());
            if !same_reason {
                self.persistence_protection_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            *self.persistence_protected_reason.write().await = Some(reason.clone());
            self.enter_protection(format!("replication HA persistence protected: {reason}"))
                .await;
            bail!(reason);
        }
        warn!(
            proxy = %self.name,
            reason = %reason,
            "replication HA persistence warning; business routing remains serving because fail_closed=false"
        );
        Ok(())
    }

    /// 业务作用: 清除 persistence 错误和保护原因，表示本轮拓扑编排已经重新验证成功。
    async fn clear_persistence_issue(&self) {
        self.set_persistence_error(None).await;
        *self.persistence_protected_reason.write().await = None;
    }

    /// 业务作用: 更新 persistence 最近错误文本，供管理面展示最后一次 handoff 或快照验证失败原因。
    /// 参数说明:
    /// - `error`: 新的错误文本；传 `None` 表示清除错误。
    ///
    async fn set_persistence_error(&self, error: Option<String>) {
        if error.is_some() {
            let retry_delay_ms = self.probe_interval_ms.saturating_mul(10).max(5_000);
            self.persistence_retry_not_before_ms
                .store(now_ms().saturating_add(retry_delay_ms), Ordering::Relaxed);
        } else {
            self.persistence_retry_not_before_ms
                .store(0, Ordering::Relaxed);
        }
        *self.persistence_last_error.write().await = error;
    }

    /// 业务作用: 在 persistence handoff 的关键步骤前确认 coordinator 租约仍有效，避免多个 proxy 同时改快照 owner。
    /// 参数说明:
    /// - `lease_guard`: 当前持有的 coordinator 租约；单 proxy 或无需租约时为空。
    /// - `step`: 当前 handoff 阶段名称，用于错误上下文。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn ensure_persistence_lease_active(
        &self,
        lease_guard: Option<&LeaseGuard>,
        step: &str,
    ) -> anyhow::Result<()> {
        if let Some(lease_guard) = lease_guard {
            if let Err(err) = lease_guard.ensure_active().await {
                let reason =
                    format!("replication HA persistence lost coordinator lease {step}: {err}");
                self.handle_persistence_failure(reason.clone()).await?;
                bail!(reason);
            }
        }
        Ok(())
    }
}
