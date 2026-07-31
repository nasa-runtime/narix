impl ReplicationHaRuntime {
/// 业务作用: 在每轮拓扑收敛开始前检查进程级协调权威，超过保护窗口立即关闭新旧业务路由。
    /// 返回: 非集群协调模式或权威新鲜时返回成功；否则进入保护态并返回错误。
    async fn ensure_cluster_route_authority(&self) -> anyhow::Result<()> {
        let Some(cluster) = &self.cluster_coordinator else {
            return Ok(());
        };
        if cluster.handle.route_authoritative().await {
            return Ok(());
        }
        let reason =
            "replication HA routing protected because proxy-cluster coordinator authority expired"
                .to_string();
        self.enter_protection(reason.clone()).await;
        bail!(reason)
    }

    /// 业务作用: 首次启动且协调状态尚无主节点时，由唯一领导者把已探测到的单主拓扑写入复制状态机，跟随者保持协调服务在线并等待结果。
    /// 参数说明:
    /// - `master`: 已通过真实角色探测确认的唯一主节点。
    ///
    /// 返回: 已提交主节点与探测结果一致时返回成功；出现冲突或在动作窗口内无法形成提交时拒绝开放数据路由。
    async fn ensure_cluster_initial_master_committed(
        &self,
        master: &HaNode,
    ) -> anyhow::Result<()> {
        let Some(cluster) = &self.cluster_coordinator else {
            return Ok(());
        };
        let deadline = Instant::now() + cluster.handle.action_timeout();
        loop {
            let active_revision = cluster.handle.state()?.config_revision;
            if active_revision == 0 {
                if Instant::now() >= deadline {
                    bail!(
                        "timed out waiting for the initial cluster configuration before committing the HA master"
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            if self.cluster_config_revision.load(Ordering::Acquire) != active_revision {
                let cluster_config_digest = self
                    .cluster_config_digest
                    .as_deref()
                    .context("proxy-cluster HA runtime has no global configuration digest")?;
                self.refresh_cluster_group_registration(cluster_config_digest)
                    .await
                    .context("bind HA group before committing the initial master")?;
            }
            let committed_master = cluster
                .handle
                .group_state(&cluster.group_id)?
                .and_then(|group| group.current_master);
            match committed_master {
                Some(committed) if committed == master.client_addr => return Ok(()),
                Some(committed) => {
                    bail!(
                        "replication HA discovered master `{}` differs from coordinator-committed master `{committed}`",
                        master.client_addr
                    );
                }
                None if cluster.handle.is_local_leader().await => {
                    match self
                        .acquire_cluster_manual_action(
                            CoordinationActionKind::TopologyRepair,
                            "initial-master",
                            master.client_addr.clone(),
                            master.client_addr.clone(),
                        )
                        .await
                    {
                        Ok(mut action) => {
                            self.ensure_cluster_action_or_protect(
                                &action,
                                "before committing the initial master",
                            )
                            .await?;
                            self.complete_cluster_action_or_protect(
                                &mut action,
                                &master.client_addr,
                                "while committing the initial master",
                            )
                            .await?;
                            return Ok(());
                        }
                        Err(error) if Instant::now() < deadline => {
                            warn!(
                                proxy = %self.name,
                                error = %error,
                                "初始化 replication HA 协调主节点暂未完成，将继续等待"
                            );
                        }
                        Err(error) => return Err(error),
                    }
                }
                None => {}
            }
            if Instant::now() >= deadline {
                bail!("timed out committing the initial replication HA master through coordinator");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 业务作用: 冷启动待恢复收尾——状态机从未提交过主节点而恢复目标已自行回归时，
    /// 由协调领导者以拓扑修复动作提交该节点为初始主，消除"未提交即保护"的死锁。
    /// 参数说明:
    /// - `master`: 已探测为唯一主且与本代恢复目标一致的节点。
    ///
    /// 返回: 领导者提交成功后返回成功并由调用方按一致状态放行；跟随者进入保护并
    /// 返回错误，等待领导者的提交结果在后续探测轮次生效。
    async fn commit_recovered_initial_master(&self, master: &HaNode) -> anyhow::Result<()> {
        let Some(cluster) = &self.cluster_coordinator else {
            return Ok(());
        };
        if !cluster.handle.is_local_leader().await {
            let reason = format!(
                "replication HA waits for the coordinator leader to commit recovered master `{}`",
                master.client_addr
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        let mut action = match self
            .acquire_cluster_manual_action(
                CoordinationActionKind::TopologyRepair,
                "recovered-initial-master",
                master.client_addr.clone(),
                master.client_addr.clone(),
            )
            .await
        {
            Ok(action) => action,
            Err(error) => {
                let reason = format!(
                    "replication HA leader could not begin committing recovered master `{}`: {error}",
                    master.client_addr
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
        };
        self.ensure_cluster_action_or_protect(&action, "before committing the recovered master")
            .await?;
        self.complete_cluster_action_or_protect(
            &mut action,
            &master.client_addr,
            "while committing the recovered master",
        )
        .await?;
        Ok(())
    }

    /// 业务作用: 上报当前主节点故障观察，并仅在本代理节点是协调领导者且观察达到门槛时申请故障转移动作令牌。
    /// 参数说明:
    /// - `current`: 本轮确认不可达的当前主节点。
    /// - `replicas`: 本轮健康副本候选，用于在批准前确定唯一目标主节点。
    /// - `observed_master`: 崩溃恢复窗口中已经提升但尚未提交的唯一主节点。
    ///
    /// 返回: 返回绑定批准目标的动作守卫；跟随者、票数不足或无安全候选时返回错误且不执行外部动作。
    async fn acquire_cluster_failover_action(
        &self,
        current: &HaNode,
        replicas: Vec<ReplicaCandidate>,
        observed_master: Option<&HaNode>,
    ) -> anyhow::Result<ClusterActionGuard> {
        let cluster = self
            .cluster_coordinator
            .as_ref()
            .context("proxy-cluster coordinator configuration is absent")?;
        let (config_revision, group_config_digest) = self.cluster_runtime_binding()?;
        let fingerprint = format!("master-down:{}", current.client_addr);
        cluster
            .handle
            .report_observation(
                &cluster.group_id,
                config_revision,
                group_config_digest.clone(),
                fingerprint.clone(),
                current.client_addr.clone(),
                false,
            )
            .await
            .context("commit replication HA master-down observation")?;
        if !cluster.handle.is_local_leader().await {
            bail!("replication HA failover waits for the proxy-cluster coordinator leader");
        }
        let candidate = match observed_master {
            Some(master) => master.clone(),
            None => {
                self.select_failover_candidate(
                    replicas,
                    Some(current),
                    "proxy-cluster automatic failover approval",
                )
                .await?
            }
        };
        let action_id = format!(
            "{}:{}:{}",
            self.name,
            current.client_addr,
            now_unix_millis()?
        );
        let token = cluster
            .handle
            .begin_action(
                &cluster.group_id,
                config_revision,
                group_config_digest,
                action_id,
                CoordinationActionKind::Failover,
                fingerprint,
                current.client_addr.clone(),
                candidate.client_addr.clone(),
                cluster.observation_window,
                cluster.observation_quorum,
            )
            .await?;
        Ok(ClusterActionGuard {
            coordinator: cluster.clone(),
            token,
            target_master: candidate.client_addr,
            completed: false,
        })
    }

    /// 业务作用: 仅由协调领导者为持久化责任节点交接申请动作令牌，目标绑定为本轮计算出的快照 owner。
    /// 参数说明:
    /// - `desired`: 已通过副本延迟与稳定窗口筛选的目标持久化拓扑。
    ///
    /// 返回: 返回持久化交接动作守卫；没有快照 owner 或本节点不是领导者时返回错误。
    async fn acquire_cluster_persistence_action(
        &self,
        desired: &DesiredPersistenceTopology,
    ) -> anyhow::Result<ClusterActionGuard> {
        let cluster = self
            .cluster_coordinator
            .as_ref()
            .context("proxy-cluster coordinator configuration is absent")?;
        if !cluster.handle.is_local_leader().await {
            bail!("replication HA persistence handoff waits for coordinator leader");
        }
        let (config_revision, group_config_digest) = self.cluster_runtime_binding()?;
        let target_owner = desired
            .snapshot_owners
            .first()
            .context("replication HA persistence topology has no snapshot owner")?
            .node
            .client_addr
            .clone();
        let topology_fingerprint = persistence_topology_fingerprint(desired);
        let current_master = self.current_master_addr().await;
        let action_id = format!(
            "{}:persistence:{}:{}",
            self.name,
            target_owner,
            now_unix_millis()?
        );
        let token = cluster
            .handle
            .begin_action(
                &cluster.group_id,
                config_revision,
                group_config_digest,
                action_id,
                CoordinationActionKind::PersistenceHandoff,
                topology_fingerprint,
                current_master,
                target_owner.clone(),
                cluster.observation_window,
                0,
            )
            .await?;
        Ok(ClusterActionGuard {
            coordinator: cluster.clone(),
            token,
            target_master: target_owner,
            completed: false,
        })
    }

    /// 业务作用: 为管理接口发起的切主或拓扑修复申请进程级协调令牌，确保人工操作与自动故障转移共享同一组内互斥和路由代际。
    /// 参数说明:
    /// - `kind`: 本次操作是主节点变更还是不改变服务主节点的拓扑修复。
    /// - `purpose`: 可观察的人工操作名称，用于构造不可重复动作编号。
    /// - `expected_master`: 操作前本代理认定的主节点，必须与已提交协调状态一致。
    /// - `target_master`: 操作成功后应由全体代理采用的主节点。
    ///
    /// 返回: 仅协调领导者能取得绑定目标的动作守卫；人工明确发起的操作不累计自动故障观察票。
    async fn acquire_cluster_manual_action(
        &self,
        kind: CoordinationActionKind,
        purpose: &str,
        expected_master: String,
        target_master: String,
    ) -> anyhow::Result<ClusterActionGuard> {
        let cluster = self
            .cluster_coordinator
            .as_ref()
            .context("proxy-cluster coordinator configuration is absent")?;
        if !cluster.handle.is_local_leader().await {
            bail!("replication HA manual operation must be sent to the coordinator leader");
        }
        let (config_revision, group_config_digest) = self.cluster_runtime_binding()?;
        let action_id = format!("{}:manual:{}:{}", self.name, purpose, now_unix_millis()?);
        let token = cluster
            .handle
            .begin_action(
                &cluster.group_id,
                config_revision,
                group_config_digest,
                action_id,
                kind,
                String::new(),
                expected_master,
                target_master.clone(),
                cluster.observation_window,
                0,
            )
            .await?;
        Ok(ClusterActionGuard {
            coordinator: cluster.clone(),
            token,
            target_master,
            completed: false,
        })
    }

    /// 业务作用: 在自动故障转移或人工拓扑动作的关键步骤前同时复验旧租约和进程级动作令牌，任一权威失效都进入保护态。
    /// 参数说明:
    /// - `lease_guard`: 旧协调模式持有的租约守卫。
    /// - `cluster_action`: 进程级协调模式持有的动作令牌守卫。
    /// - `step`: 当前外部动作阶段，用于错误诊断。
    ///
    /// 返回: 所有适用权威仍有效时返回成功，否则关闭路由并返回错误。
    async fn ensure_automatic_authority_or_protect(
        &self,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
        step: &str,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("while verifying replication HA action authority")?;
        self.ensure_automatic_lease_active_or_protect(lease_guard, step)
            .await?;
        if let Some(action) = cluster_action {
            if let Err(error) = action.ensure_active().await {
                let reason = format!(
                    "replication HA action lost proxy-cluster authority {step}: {error}"
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
        }
        Ok(())
    }

    /// 业务作用: 软隔离下在提主前等待一个完整路由保护窗口，让可能分区的集群成员先行断路。
    ///
    /// proxy-cluster 的跟随者依靠本地单调租约维持路由权威，与领导者分区后最长仍会在
    /// `route_guard_timeout` 内把业务写请求继续路由到旧主节点。软隔离不切断旧主写入，
    /// 若领导者立即提主，会出现"分区成员写旧主、新连接写新主"的双写窗口。因此提主
    /// 前先等满一个保护窗口，确保动作批准前已分区的成员全部断路。外部隔离命令已经
    /// 物理切断旧主写入，无需等待。
    ///
    /// 参数说明:
    /// - `lease_guard`: 旧协调模式租约；proxy-cluster 模式为空。
    /// - `cluster_action`: 进程级协调动作令牌。
    /// - `step`: 围栏结束后复验权威使用的阶段名称。
    ///
    /// 返回: 实际等待了保护窗口且权威仍有效时返回 `true`；非集群协调或已配置外部
    /// 隔离时不等待并返回 `false`，调用方无需为未等待的路径重新探测拓扑。
    async fn wait_soft_fencing_route_barrier(
        &self,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
        step: &str,
    ) -> anyhow::Result<bool> {
        let Some(cluster) = &self.cluster_coordinator else {
            return Ok(false);
        };
        if self.fencing.mode == HaFencingMode::ExternalCommand {
            return Ok(false);
        }
        tokio::time::sleep(cluster.handle.route_guard_timeout()).await;
        self.ensure_automatic_authority_or_protect(lease_guard, cluster_action, step)
            .await?;
        Ok(true)
    }

    /// 业务作用: 在任意进程级协调动作的外部副作用前复验动作权威，失败时立即关闭本地路由。
    /// 参数说明:
    /// - `action`: 已经由协调状态机提交并绑定目标节点的动作守卫。
    /// - `step`: 当前外部动作阶段，用于记录可定位的保护原因。
    ///
    /// 返回: 领导权、路由保护窗口和动作令牌均有效时返回成功；否则进入保护态并返回错误。
    async fn ensure_cluster_action_or_protect(
        &self,
        action: &ClusterActionGuard,
        step: &str,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("while verifying cluster action authority")?;
        if let Err(error) = action.ensure_active().await {
            let reason = format!("replication HA proxy-cluster action lost authority {step}: {error}");
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        Ok(())
    }

    /// 业务作用: 把外部 HA 控制错误统一转换为保护态，避免旧路由继续指向已经被隔离、提主或降级的节点。
    /// 参数说明:
    /// - `result`: fencing、提主、降级或复制命令的执行结果。
    /// - `step`: 失败动作的业务阶段，用于保护原因和运维诊断。
    ///
    /// 返回: 外部步骤成功时透传结果；失败时先关闭路由再返回原始错误上下文。
    async fn protect_ha_control_step<T>(
        &self,
        result: anyhow::Result<T>,
        step: &str,
    ) -> anyhow::Result<T> {
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                let reason = format!("replication HA control failed {step}: {error}");
                // 退休代已经从活动注册表和业务路由中摘除，继续向它的 watch 通道写
                // Protected 只会制造无消费方的死状态。活动代仍保持 fail-closed。
                if !self.is_retired() {
                    self.enter_protection(reason.clone()).await;
                }
                bail!(reason);
            }
        }
    }

    /// 业务作用: 安全提交进程级动作结果；提交失败意味着新路由未获得多数派确认，因此必须保持保护态。
    /// 参数说明:
    /// - `action`: 待完成并标记为不可再次清理的动作守卫。
    /// - `resulting_master`: 外部操作后复验得到的唯一主节点。
    /// - `step`: 本次完成提交所在的业务阶段。
    ///
    /// 返回: 多数派提交动作结果成功时返回成功；失败时关闭路由并返回错误。
    async fn complete_cluster_action_or_protect(
        &self,
        action: &mut ClusterActionGuard,
        resulting_master: &str,
        step: &str,
    ) -> anyhow::Result<()> {
        if let Err(error) = action.complete_success(resulting_master).await {
            let reason =
                format!("replication HA proxy-cluster action result was not committed {step}: {error}");
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        Ok(())
    }

    /// 业务作用: 在持久化交接每个外部动作前复验旧租约或集群动作令牌，失效时按持久化 fail-closed 策略处理。
    /// 参数说明:
    /// - `lease_guard`: 旧协调模式租约守卫。
    /// - `cluster_action`: 进程级协调模式动作守卫。
    /// - `step`: 当前持久化交接阶段。
    ///
    /// 返回: 适用权威有效时返回成功，否则记录持久化错误并按配置决定是否关闭路由。
    async fn ensure_persistence_authority(
        &self,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
        step: &str,
    ) -> anyhow::Result<()> {
        self.ensure_not_retired("while verifying persistence authority")?;
        if let Err(error) = self.ensure_not_observer("dynamic persistence action") {
            // 配置校验是第一道门禁；这里是所有 CONFIG/checkpoint 权威复验共用的
            // 运行时保险，防止未来新增调用方或热更新缺陷让 observer 写入后端。
            let reason = format!(
                "replication HA observer runtime blocked a persistence write path: {error}"
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        self.ensure_persistence_lease_active(lease_guard, step)
            .await?;
        if let Some(action) = cluster_action {
            if let Err(error) = action.ensure_active().await {
                let reason = format!(
                    "replication HA persistence lost proxy-cluster action authority {step}: {error}"
                );
                // fail_closed 只决定数据面是否保护，绝不能把“失权”转换成成功；
                // 即使 fail-open，也必须向适配器返回错误以停止旧 owner 后续写入。
                self.handle_persistence_failure(reason.clone()).await?;
                bail!(reason);
            }
        }
        Ok(())
    }

    /// 业务作用: 按配置选择本地文件或外部 Redis 租约后端；proxy-cluster 使用独立动作令牌。
    /// 返回: 返回当前协调模式对应的租约后端；配置缺失或模式不适用时返回错误。
    fn coordinator_lease_backend(&self) -> anyhow::Result<LeaseBackend> {
        match self.coordinator.mode {
            HaCoordinatorMode::None => {
                bail!("replication HA coordinator mode=none does not provide a lease backend")
            }
            HaCoordinatorMode::LocalFile => {
                let Some(path) = self.coordinator.lease_path.clone() else {
                    bail!("replication HA coordinator local-file requires lease path");
                };
                Ok(LeaseBackend::LocalFile { path })
            }
            HaCoordinatorMode::Redis => {
                let Some(config) = self.coordinator.redis.clone() else {
                    bail!("replication HA coordinator redis requires redis config");
                };
                Ok(LeaseBackend::Redis { config })
            }
            HaCoordinatorMode::ProxyCluster => {
                bail!("replication HA proxy-cluster coordinator requires the process-level coordinator handle")
            }
        }
    }

    /// 业务作用: 获取 failover 租约，用于在多 proxy 同时探测到故障时保证只有一个执行拓扑改写。
    /// 参数说明:
    /// - `purpose`: 本次租约、连接或动作的业务目的，用于构造唯一键和可观测标签。
    ///
    /// 返回: 无需外部租约时返回空，成功竞争时返回守卫；超时、忙碌或后端失败时返回错误。
    // 获取 failover 租约，用于在多 proxy 同时探测到故障时保证只有一个执行拓扑改写。
    async fn acquire_failover_lease(
        &self,
        purpose: &str,
    ) -> anyhow::Result<Option<LeaseGuard>> {
        if self.coordinator.mode == HaCoordinatorMode::None {
            return Ok(None);
        }
        let backend = self.coordinator_lease_backend()?;
        let label = backend.label();
        let started = Instant::now();
        match LeaseGuard::acquire(
            backend,
            self.coordinator.owner_id.clone(),
            self.coordinator.lease_ttl_ms,
            self.coordinator.lease_renew_interval_ms,
            self.coordinator.lease_acquire_timeout_ms,
        )
        .await
        {
            Ok(lease) => {
                self.lease_acquire_count.fetch_add(1, Ordering::Relaxed);
                self.last_lease_acquire_elapsed_micros
                    .store(elapsed_micros(started), Ordering::Relaxed);
                info!(
                    proxy = %self.name,
                    owner = %self.coordinator.owner_id,
                    coordinator = %label,
                    purpose = %purpose,
                    "HA coordinator lease acquired"
                );
                Ok(Some(lease))
            }
            Err(err) => {
                self.lease_acquire_failure_count
                    .fetch_add(1, Ordering::Relaxed);
                self.last_lease_acquire_elapsed_micros
                    .store(elapsed_micros(started), Ordering::Relaxed);
                Err(err).with_context(|| format!("acquire HA coordinator lease for {purpose}"))
            }
        }
    }

    /// 业务作用: 自动 failover 专用租约获取：等待锁期间持续观察拓扑，避免 leader 完成后 follower 再抢锁。
    /// 参数说明:
    /// - `current`: 当前已观测或已生效的状态，用于计算下一步变更而非盲目覆盖。
    ///
    /// 返回: 返回已取得租约、已由其它实例收敛或仍忙碌三种结果；协调失败时返回错误。
    // 自动 failover 专用租约获取：等待锁期间持续观察拓扑，避免 leader 完成后 follower 再抢锁。
    async fn acquire_automatic_failover_lease(
        &self,
        current: &HaNode,
    ) -> anyhow::Result<AutomaticLeaseOutcome> {
        if self.coordinator.mode == HaCoordinatorMode::None {
            return Ok(AutomaticLeaseOutcome::Acquired(None));
        }
        let backend = self.coordinator_lease_backend()?;
        let label = backend.label();
        let started = Instant::now();
        let timeout = Duration::from_millis(self.coordinator.lease_acquire_timeout_ms);
        let mut last_error = None;
        let mut observed_owned_lease = false;
        loop {
            match try_acquire_coordinator_lease(
                &backend,
                &self.coordinator.owner_id,
                self.coordinator.lease_ttl_ms,
            )
            .await
            {
                Ok(true) => {
                    let lease = LeaseGuard::from_acquired(
                        backend,
                        self.coordinator.owner_id.clone(),
                        self.coordinator.lease_ttl_ms,
                        self.coordinator.lease_renew_interval_ms,
                    );
                    let already_converged = if observed_owned_lease {
                        self.wait_for_observed_failover(current, &lease).await?
                    } else {
                        self.converge_if_failover_observed(current, Some(&lease))
                            .await?
                    };
                    if already_converged {
                        return Ok(AutomaticLeaseOutcome::AlreadyConverged);
                    }
                    self.lease_acquire_count.fetch_add(1, Ordering::Relaxed);
                    self.last_lease_acquire_elapsed_micros
                        .store(elapsed_micros(started), Ordering::Relaxed);
                    info!(
                        proxy = %self.name,
                        owner = %self.coordinator.owner_id,
                        coordinator = %label,
                        purpose = "automatic failover",
                        "HA coordinator lease acquired"
                    );
                    return Ok(AutomaticLeaseOutcome::Acquired(Some(lease)));
                }
                Ok(false) => {
                    observed_owned_lease = true;
                    if self.converge_if_failover_observed(current, None).await? {
                        return Ok(AutomaticLeaseOutcome::AlreadyConverged);
                    }
                }
                Err(err) => last_error = Some(err),
            }

            if started.elapsed() >= timeout {
                self.last_lease_acquire_elapsed_micros
                    .store(elapsed_micros(started), Ordering::Relaxed);
                if observed_owned_lease {
                    let reason = last_error
                        .map(|err| format!("lease is owned by another proxy; last error: {err}"))
                        .unwrap_or_else(|| "lease is owned by another proxy".to_string());
                    return Ok(AutomaticLeaseOutcome::Busy { reason });
                }
                self.lease_acquire_failure_count
                    .fetch_add(1, Ordering::Relaxed);
                let reason = last_error
                    .map(|err| format!("last error: {err}"))
                    .unwrap_or_else(|| "lease is owned by another proxy".to_string());
                bail!(
                    "timed out after {}ms acquiring replication HA failover lease `{}`: {reason}",
                    self.coordinator.lease_acquire_timeout_ms,
                    label
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 业务作用: 在曾观察到其它 proxy 持锁后，短暂等待已完成的 failover outcome 或拓扑变化，避免重复切主。
    /// 参数说明:
    /// - `current`: 本 proxy 等待前认为仍在服务的 master 节点。
    ///
    /// 返回: 观察到其它 proxy 已完成收敛返回 `true`；超时未观察到返回 `false`。
    // proxy 曾经看到 lease 被占用时，抢到锁后给已完成的 leader 结果一个短收敛窗口。
    async fn wait_for_observed_failover(
        &self,
        current: &HaNode,
        lease: &LeaseGuard,
    ) -> anyhow::Result<bool> {
        let deadline = Instant::now() + Duration::from_millis(self.failover_lease_settle_ms());
        loop {
            if self
                .converge_if_failover_observed(current, Some(lease))
                .await?
            {
                return Ok(true);
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 业务作用: 计算等待其它 proxy failover 结果的短收敛窗口，限制在探活间隔的合理范围内。
    /// 返回: 等待窗口毫秒数，最小 500ms、最大 2000ms。
    // 曾观察到其它 owner 持锁时，抢到锁后只给 outcome/拓扑一个短收敛窗口。
    fn failover_lease_settle_ms(&self) -> u64 {
        self.probe_interval_ms.saturating_mul(2).clamp(500, 2_000)
    }

    /// 业务作用: 在 HA 拓扑动作的关键步骤前确认 coordinator 租约仍有效，避免租约过期后继续 fencing、promote 或复制改写。
    /// 参数说明:
    /// - `lease_guard`: 当前 HA 动作持有的租约；不需要租约的路径为空。
    /// - `step`: 当前切主阶段名称，用于错误上下文。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn ensure_automatic_lease_active(
        &self,
        lease_guard: Option<&LeaseGuard>,
        step: &str,
    ) -> anyhow::Result<()> {
        if let Some(lease_guard) = lease_guard {
            lease_guard.ensure_active().await.with_context(|| {
                format!("replication HA action lost coordinator lease {step}")
            })?;
        }
        Ok(())
    }

    /// 业务作用: 租约失效时立即进入保护态，防止当前 proxy 在失去互斥权后继续对外提供可能过期的路由。
    /// 参数说明:
    /// - `lease_guard`: 当前 HA 动作持有的租约；为空时表示当前步骤不要求租约。
    /// - `step`: 当前切主阶段名称，用于保护态原因。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn ensure_automatic_lease_active_or_protect(
        &self,
        lease_guard: Option<&LeaseGuard>,
        step: &str,
    ) -> anyhow::Result<()> {
        if let Err(err) = self.ensure_automatic_lease_active(lease_guard, step).await {
            let reason = format!(
                "replication HA action blocked because coordinator lease was lost {step}: {err}"
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        Ok(())
    }

    /// 业务作用: 如果观察到其它 proxy 已经完成切主，则本 proxy 跟随新拓扑收敛，避免重复 promote。
    /// 参数说明:
    /// - `current`: 当前 proxy 等待前认为正在服务的 master。
    ///
    /// 返回: 已成功跟随其它 proxy 的切主结果时返回 `true`；仍需本 proxy 继续处理时返回 `false`。
    // 等待租约期间如果观察到其它 proxy 已经完成 failover，则直接收敛本 proxy 路由。
    async fn converge_if_failover_observed(
        &self,
        current: &HaNode,
        lease_guard: Option<&LeaseGuard>,
    ) -> anyhow::Result<bool> {
        if self
            .converge_to_recent_failover_outcome(current, "observed failover outcome")
            .await?
        {
            return Ok(true);
        }
        let (masters, replicas) = self.discover_roles_with_replica_lag().await;
        if masters.len() > 1 {
            let observed_extra_masters = self
                .remember_unconfirmed_extra_masters(current, &masters)
                .await;
            if masters
                .iter()
                .any(|master| master.client_addr == current.client_addr)
            {
                self.enter_protection(format!(
                    "replication HA detected multiple writable masters while waiting for the failover lease: {}",
                    format_node_addrs(&masters)
                ))
                .await;
                let Some(lease) = lease_guard else {
                    return Ok(false);
                };
                self.ensure_automatic_lease_active_or_protect(
                    Some(lease),
                    "before multi-master convergence while waiting for failover",
                )
                .await?;
                let repair_started = Instant::now();
                let demote_result = self
                    .demote_extra_masters_checked(
                        current,
                        Some(lease),
                        None,
                        Some(&observed_extra_masters),
                    )
                    .await;
                self.master_failure_streak.store(0, Ordering::Relaxed);
                if demote_result.is_ok() {
                    self.repair_retry_not_before_ms.store(0, Ordering::Relaxed);
                    self.repair_count.fetch_add(1, Ordering::Relaxed);
                    self.last_repair_elapsed_micros
                        .store(elapsed_micros(repair_started), Ordering::Relaxed);
                } else {
                    let retry_delay_ms = self
                        .coordinator
                        .lease_ttl_ms
                        .max(self.probe_interval_ms.saturating_mul(10));
                    self.repair_retry_not_before_ms.store(
                        now_ms().saturating_add(retry_delay_ms),
                        Ordering::Relaxed,
                    );
                    self.repair_failure_count.fetch_add(1, Ordering::Relaxed);
                }
                demote_result.with_context(|| {
                    format!(
                        "kept routing protected while demoting extra masters around `{}` during failover lease wait",
                        current.client_addr
                    )
                })?;
                self.ensure_automatic_lease_active_or_protect(
                    Some(lease),
                    "after multi-master convergence while waiting for failover",
                )
                .await?;
                self.ensure_serving(current.clone()).await;
                return Ok(true);
            }
            let reason = format!(
                "multiple replication HA masters detected while waiting failover lease: {}",
                format_node_addrs(&masters)
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        if masters.len() == 1 && masters[0].client_addr != current.client_addr {
            let observed_master = masters[0].clone();
            if old_master_follows_new(&replicas, current, &observed_master) {
                self.finish_master_switch(
                    current.client_addr.clone(),
                    observed_master,
                    Instant::now(),
                    "observed converged failover while waiting lease",
                )
                .await?;
                return Ok(true);
            }
            return Ok(false);
        }
        if masters.len() == 1 && masters[0].client_addr == current.client_addr {
            self.master_failure_streak.store(0, Ordering::Relaxed);
            self.ensure_serving(current.clone()).await;
            return Ok(true);
        }
        Ok(false)
    }

    /// 业务作用: 复核近期协调切主结果与真实拓扑一致后收敛本地路由，不能仅信任历史记录。
    /// 参数说明:
    /// - `current`: 当前已观测或已生效的状态，用于计算下一步变更而非盲目覆盖。
    /// - `action`: 当前要执行的拓扑或持久化动作，决定命令模板、指标标签和错误语义。
    ///
    /// 返回: 真实拓扑证明近期结果成立时返回真并收敛路由；无可用结果返回假，证据冲突返回错误。
    async fn converge_to_recent_failover_outcome(
        &self,
        current: &HaNode,
        action: &str,
    ) -> anyhow::Result<bool> {
        let Some(master_addr) = self.recent_failover_outcome().await else {
            return Ok(false);
        };
        if master_addr == current.client_addr {
            return Ok(false);
        }
        let (masters, _) = self.discover_roles_with_replica_lag().await;
        if failover_outcome_matches_unique_master(&masters, &master_addr) {
            let master = self.find_node(&master_addr)?;
            self.finish_master_switch(current.client_addr.clone(), master, Instant::now(), action)
                .await?;
            return Ok(true);
        }
        warn!(
            proxy = %self.name,
            outcome_master = %master_addr,
            observed_masters = %format_node_addrs(&masters),
            "ignored replication HA failover outcome because target is not the unique healthy master"
        );
        Ok(false)
    }

    /// 业务作用: 把协调租约被其它实例持有转换为明确的忙碌结果和重试原因。
    /// 返回: 租约由其它 owner 持有时返回真，空闲或自身持有时返回假；读取失败返回错误。
    async fn coordinator_lease_held_by_other_result(&self) -> anyhow::Result<bool> {
        if self.coordinator.mode == HaCoordinatorMode::None {
            return Ok(false);
        }
        if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            let cluster = self
                .cluster_coordinator
                .as_ref()
                .context("proxy-cluster coordinator configuration is absent")?;
            return Ok(!cluster.handle.is_local_leader().await
                || !cluster.handle.route_authoritative().await);
        }
        let backend = self.coordinator_lease_backend()?;
        match backend {
            LeaseBackend::LocalFile { path } => Ok(read_active_local_lease_owner(&path)?
                .is_some_and(|owner| owner != self.coordinator.owner_id)),
            LeaseBackend::Redis { config } => Ok(read_redis_lease_owner(&config)
                .await?
                .is_some_and(|owner| owner != self.coordinator.owner_id)),
        }
    }

    /// 业务作用: 在当前租约仍归本 proxy 所有时发布切主结果，供其它 proxy 避免重复 fencing 和 promote。
    /// 参数说明:
    /// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
    ///
    /// 返回: 租约仍属于当前 owner 且结果已发布时成功；租约转移、过期或存储失败时返回错误。
    // 写 outcome 前后都校验租约 owner，不能让过期持有者覆盖新 owner 已发布的切主结果。
    async fn record_failover_outcome(
        &self,
        lease: &LeaseGuard,
        master: &HaNode,
    ) -> anyhow::Result<()> {
        lease.ensure_active().await?;
        let written = match &lease.backend {
            LeaseBackend::LocalFile { path: lease_path } => {
                let outcome_path = self
                    .coordinator
                    .local_outcome_path()
                    .context("local-file coordinator has no failover outcome path")?;
                write_local_failover_outcome_if_owner(
                    lease_path,
                    &outcome_path,
                    &lease.owner_id,
                    &master.client_addr,
                )?
            }
            LeaseBackend::Redis { config } => {
                write_redis_failover_outcome_if_owner(
                    config,
                    &lease.owner_id,
                    &master.client_addr,
                    self.coordinator.lease_ttl_ms,
                )
                .await?
            }
        };
        if !written {
            bail!(
                "replication HA failover outcome for `{}` was not published because the coordinator lease is no longer owned by `{}`",
                master.client_addr,
                lease.owner_id
            );
        }
        Ok(())
    }

    // 读取仍在 TTL 内的 failover outcome；过期 outcome 不参与收敛判断。
    /// 业务作用: 读取仍在 TTL 内的 failover outcome；过期 outcome 不参与收敛判断。
    async fn recent_failover_outcome(&self) -> Option<String> {
        match self.coordinator.mode {
            HaCoordinatorMode::None => None,
            HaCoordinatorMode::LocalFile => {
                let path = self.coordinator.local_outcome_path()?;
                let outcome = match read_local_failover_outcome(&path) {
                    Ok(Some(outcome)) => outcome,
                    Ok(None) => return None,
                    Err(err) => {
                        warn!(
                            proxy = %self.name,
                            path = %path.display(),
                            error = %err,
                            "failed to read replication HA failover outcome"
                        );
                        return None;
                    }
                };
                let now = match now_unix_millis() {
                    Ok(now) => now,
                    Err(err) => {
                        warn!(proxy = %self.name, error = %err, "failed to read system time");
                        return None;
                    }
                };
                if now.saturating_sub(outcome.updated_at_ms)
                    <= u128::from(self.coordinator.lease_ttl_ms)
                {
                    Some(outcome.master)
                } else {
                    None
                }
            }
            HaCoordinatorMode::Redis => {
                let config = self.coordinator.redis.as_ref()?;
                match read_redis_failover_outcome(config).await {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        warn!(
                            proxy = %self.name,
                            key = %config.outcome_key,
                            error = %err,
                            "failed to read replication HA failover outcome"
                        );
                        None
                    }
                }
            }
            HaCoordinatorMode::ProxyCluster => {
                let cluster = self.cluster_coordinator.as_ref()?;
                match cluster.handle.group_state(&cluster.group_id) {
                    Ok(state) => state.and_then(|group| group.current_master),
                    Err(error) => {
                        warn!(
                            proxy = %self.name,
                            error = %error,
                            "读取 replication HA 集群协调切换结果失败"
                        );
                        None
                    }
                }
            }
        }
    }
}
