impl ReplicationHaRuntime {
    /// 业务作用: 完成 replication HA master 切换的本地路由收尾；拓扑写入必须由调用方在协调权威内先行完成，或交给下一轮受权收敛。
    /// 参数说明:
    /// - `old_master`: 切换前本 proxy 认为正在服务的 master 地址。
    /// - `master`: 新确认可服务的 master 节点。
    /// - `started`: 本次切换开始时间，用于记录切换耗时指标。
    /// - `action`: 触发切换的动作名称，用于日志区分自动、手动和收敛路径。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    async fn finish_master_switch(
        &self,
        old_master: String,
        master: HaNode,
        started: Instant,
        action: &str,
    ) -> anyhow::Result<()> {
        self.master_failure_streak.store(0, Ordering::Relaxed);
        self.automatic_retry_not_before_ms
            .store(0, Ordering::Relaxed);
        self.reset_observed_takeover().await;
        if old_master != master.client_addr {
            self.failover_count.fetch_add(1, Ordering::Relaxed);
            self.last_failover_elapsed_micros
                .store(elapsed_micros(started), Ordering::Relaxed);
            info!(
                proxy = %self.name,
                action = %action,
                old_master = %old_master,
                new_master = %master.client_addr,
                "replication-ha master switched"
            );
            // 这里只发布 current_master 和 Serving 路由。该通用收尾函数也用于
            // observer 与无租约的被动跟随，因此不得在内部隐式发送拓扑控制命令
            // 或写共享 outcome；需要这些副作用的调用方必须持权威显式执行。
            self.set_current_master(master.clone()).await;
        } else {
            self.ensure_serving(master.clone()).await;
        }
        // 独立权威监视器可能在本次慢控制动作期间先进入保护态。只有路由最终确实
        // 发布成功时才清除错误，避免管理状态把“仍受保护”误报成无异常。
        let serving = matches!(&*self.routing_tx.borrow(), HaRoutingState::Serving { .. });
        if serving {
            self.set_last_error(None).await;
        }
        Ok(())
    }

    /// 业务作用: 并发探测全部 HA 节点，归一化角色、上游、延迟和探测失败证据。
    /// 参数说明: 无。
    ///
    /// 返回: 已确认的主节点和副本候选；失败与未知角色不进入候选集合。
    async fn discover_roles_with_replica_lag(&self) -> (Vec<HaNode>, Vec<ReplicaCandidate>) {
        let mut masters = Vec::new();
        let mut replicas = Vec::new();
        let probes = self.ha.nodes().iter().map(|node| async move {
            let result = self.ha.backend().inspect_node(node).await;
            (node, result)
        });
        for (node, result) in join_all(probes).await {
            match result {
                Ok(observation) => match observation.role {
                    HaRole::Master => masters.push(node.clone()),
                    HaRole::Replica => replicas.push(ReplicaCandidate {
                        node: node.clone(),
                        lag_seconds: observation.replica_lag_seconds,
                        link_down_seconds: observation.link_down_since_seconds,
                        master_addr: observation.upstream_addr,
                    }),
                    HaRole::Unknown => {}
                },
                Err(err) => {
                    self.warn_probe_failure(node, &err).await;
                }
            }
        }
        (masters, replicas)
    }

    /// 业务作用: 按节点限制 HA 探测失败日志频率，保留故障证据同时避免短探测周期放大日志和 CPU 消耗。
    /// 参数说明:
    /// - `node`: 本次探测失败的后端节点。
    /// - `error`: 本次探测返回的具体错误。
    ///
    /// 返回: 无返回值；达到输出时间时记录一次告警，否则静默保留探测结果。
    async fn warn_probe_failure(&self, node: &HaNode, error: &anyhow::Error) {
        let current_ms = now_ms();
        let mut log_deadlines = self.probe_failure_log_not_before_ms.write().await;
        let next_log_ms = log_deadlines.entry(node.client_addr.clone()).or_default();
        if current_ms < *next_log_ms {
            return;
        }
        *next_log_ms = current_ms.saturating_add(self.probe_interval_ms.max(5_000));
        drop(log_deadlines);
        warn!(
            proxy = %self.name,
            node = %node.client_addr,
            error = %error,
            "HA node probe failed"
        );
    }

    /// 业务作用: 按规范化客户端地址查找 HA 配置节点。
    /// 参数说明:
    /// - `client_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    /// 返回: 地址或候选门禁唯一命中时返回 HA 节点；缺失、歧义或无合格候选时返回错误。
    fn find_node(&self, client_addr: &str) -> anyhow::Result<HaNode> {
        self.node_by_any_addr(client_addr)
            .cloned()
            .with_context(|| format!("HA node `{client_addr}` is not configured"))
    }

    /// 业务作用: 同时按客户端地址和复制登记地址解析 HA 节点。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    ///
    fn node_by_any_addr(&self, addr: &str) -> Option<&HaNode> {
        self.ha
            .nodes()
            .iter()
            .find(|node| node.client_addr == addr || node.replication_addr == addr)
    }

    /// 业务作用: 更新已验证当前主节点，并清除与新事实冲突的短期故障计数。
    /// 参数说明:
    /// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
    ///
    async fn set_current_master(&self, master: HaNode) {
        let addr = master.client_addr.clone();
        *self.current_master.write().await = master;
        self.publish_serving_if_authoritative(addr).await;
    }

    /// 业务作用: 关闭不再安全的业务路由，保留保护原因并记录状态变化，等待控制面确认恢复条件。
    /// 参数说明:
    /// - `reason`: 错误或原因文本，用于保护态、日志和失败上下文。
    ///
    /// 返回: 旧连接被通知停止，新连接被拒绝；相同保护原因不重复输出日志。
    async fn enter_protection(&self, reason: String) {
        if matches!(
            &*self.routing_tx.borrow(),
            HaRoutingState::Protected { reason: current } if current == &reason
        ) {
            self.set_last_error(Some(reason)).await;
            return;
        }
        self.protection_count.fetch_add(1, Ordering::Relaxed);
        self.set_last_error(Some(reason.clone())).await;
        // 先撤销数据面路由，再记录已发布的保护原因，日志不能延后关闭旧主连接。
        let previous = self.routing_tx.send_replace(HaRoutingState::Protected {
            reason: reason.clone(),
        });
        if !matches!(previous, HaRoutingState::Protected { reason: current } if current == reason) {
            info!(
                proxy = %self.name,
                reason = %reason,
                "replication-ha routing protected"
            );
        }
    }

    /// 业务作用: 只有当前动作权威和持久化发布门禁同时满足时才开放主节点业务路由。
    /// 参数说明:
    /// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
    ///
    // 确认单 master 拓扑恢复后重新开放业务路由。
    async fn ensure_serving(&self, master: HaNode) {
        let addr = master.client_addr.clone();
        *self.current_master.write().await = master;
        self.publish_serving_if_authoritative(addr).await;
    }

    /// 业务作用: 在所有发布 `Serving` 的入口执行最后一次协调权威检查，防止并发的慢控制动作把独立监视器已经关闭的路由重新打开。
    /// 参数说明:
    /// - `master_addr`: 已经通过拓扑流程确认的主节点客户端地址。
    ///
    /// 返回: 无显式返回值；权威失效时保持或进入保护态，权威有效时才发布主节点路由。
    async fn publish_serving_if_authoritative(&self, master_addr: String) {
        let unresolved = self
            .resolve_unconfirmed_extra_masters(&master_addr, &[])
            .await;
        if !unresolved.is_empty() {
            // Serving 的全部入口都必须经过同一风险门禁。一次有证据的主切换可以
            // 把风险集合成员立为新权威主，但只能清除目标自身；其它曾确认可写且
            // 仍然缺席的节点继续阻止开放，避免切换收尾制造一个探测周期的双写窗口。
            self.enter_protection(format!(
                "replication HA keeps routing protected because previously observed extra masters are not yet confirmed as replicas of `{master_addr}`: {}",
                unresolved.join(",")
            ))
            .await;
            return;
        }
        if let Some(reason) = self.persistence_protected_reason.read().await.clone() {
            // 所有 Serving 发布入口共享同一持久化 fail-closed 门禁。多主收敛、主节点
            // 恢复或切换收尾不能绕过上一轮已经建立的严格持久化保护；只有一次成功的
            // reconcile_persistence 清除该原因后，后续发布才可重新开放数据面。
            if matches!(&*self.routing_tx.borrow(), HaRoutingState::Serving { .. }) {
                self.enter_protection(format!("replication HA persistence protected: {reason}"))
                    .await;
            }
            return;
        }
        if let Some(cluster) = &self.cluster_coordinator {
            if !cluster.handle.route_authoritative().await {
                let serving = matches!(&*self.routing_tx.borrow(), HaRoutingState::Serving { .. });
                if serving {
                    self.enter_protection(
                        "replication HA routing protected because proxy-cluster coordinator authority expired"
                            .to_string(),
                    )
                    .await;
                }
                return;
            }
        }
        if !matches!(
            &*self.routing_tx.borrow(),
            HaRoutingState::Serving { master } if master == &master_addr
        ) {
            // 拓扑、持久化和协调权威全部通过后才发布路由，诊断只记录实际发生的切换。
            let previous = self.routing_tx.send_replace(HaRoutingState::Serving {
                master: master_addr.clone(),
            });
            if !matches!(previous, HaRoutingState::Serving { master } if master == master_addr) {
                info!(
                    proxy = %self.name,
                    master = %master_addr,
                    "replication-ha routing serving"
                );
            }
        }
    }

    /// 业务作用: 记录脱敏控制面错误摘要，供状态接口和限频日志观测。
    /// 参数说明:
    /// - `error`: 错误或原因文本，用于保护态、日志和失败上下文。
    ///
    async fn set_last_error(&self, error: Option<String>) {
        *self.last_error.write().await = error;
    }

    /// 业务作用: 启动或热更新构造期发现当前主节点；构造期绝不执行 fence/promote 等拓扑改写。
    ///
    /// 构造发生在上一运行代仍然活跃的窗口内（热更新先构造候选代、提交时才停旧代），
    /// `coordinator.mode=none` 下两代各持独立 `failover_lock`，构造期改写拓扑会与旧代
    /// 自动切主并发形成双主。因此无主时只推导恢复目标并交给激活后的 reconcile 循环，
    /// 由标准机制（连败宽限、断链候选门禁、租约或集群动作令牌）串行完成恢复。
    ///
    /// 返回: 观测到唯一主节点时返回确认结果；无主但能定位恢复目标时返回待恢复结果；
    /// 多主或无法定位旧主时返回错误并阻止构造。
    async fn discover_startup_master(&self) -> anyhow::Result<StartupMasterDiscovery> {
        let (mut masters, replicas) = self.discover_roles_with_replica_lag().await;

        if masters.len() == 1 {
            return Ok(StartupMasterDiscovery::Confirmed(masters.remove(0)));
        }
        if masters.len() > 1 {
            bail!(
                "multiple replication HA masters detected: {}",
                format_node_addrs(&masters)
            );
        }

        // 集群协调模式优先采用状态机已提交的主节点：本实例可能只是与主节点分区，
        // 其余成员仍在正常服务，此时恢复目标必须与集群共识一致而不是本地推导。
        if let Some(cluster) = &self.cluster_coordinator {
            if let Some(committed) = cluster
                .handle
                .group_state(&cluster.group_id)?
                .and_then(|group| group.current_master)
            {
                if let Some(node) = self.node_by_any_addr(&committed) {
                    return Ok(StartupMasterDiscovery::PendingRecovery(node.clone()));
                }
            }
        }

        // 从副本登记的上游推导旧主。全部在线副本必须指向同一个配置内节点，
        // 否则拓扑本身已经分裂，构造期不猜测恢复目标。
        let mut derived: Option<HaNode> = None;
        for replica in &replicas {
            let Some(upstream) = replica
                .master_addr
                .as_deref()
                .and_then(|address| self.node_by_any_addr(address))
            else {
                continue;
            };
            match &derived {
                Some(existing) if existing.client_addr != upstream.client_addr => {
                    bail!(
                        "no replication HA master observed and replicas follow different upstreams (`{}` vs `{}`)",
                        existing.client_addr,
                        upstream.client_addr
                    );
                }
                Some(_) => {}
                None => derived = Some(upstream.clone()),
            }
        }
        let Some(old_master) = derived else {
            bail!(
                "no replication HA master observed and the previous master could not be derived from replica upstreams; candidates: {}",
                format_replica_candidates(&replicas)
            );
        };
        Ok(StartupMasterDiscovery::PendingRecovery(old_master))
    }

    /// 业务作用: 选择 replication HA 自动切换候选节点，优先使用已验证热备，避免把 snapshot owner 误提为 master。
    /// 参数说明:
    /// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
    /// - `expected_upstream`: 已知旧主节点；启动时无法确定旧主时为空。
    /// - `purpose`: 本次租约、连接或动作的业务目的，用于构造唯一键和可观测标签。
    ///
    /// 返回: 地址或候选门禁唯一命中时返回 HA 节点；缺失、歧义或无合格候选时返回错误。
    async fn select_failover_candidate(
        &self,
        mut replicas: Vec<ReplicaCandidate>,
        expected_upstream: Option<&HaNode>,
        purpose: &str,
    ) -> anyhow::Result<HaNode> {
        if let Some(expected_upstream) = expected_upstream {
            replicas
                .retain(|candidate| replica_candidate_follows_node(candidate, expected_upstream));
        }
        let hot_standby = self.persistence_topology.read().await.hot_standby.clone();
        if let Some(hot_standby) = hot_standby {
            // 热备优先同样使用切主资格语义：主节点宕机后热备也会断链，
            // 按断链时长过门禁，不能因此把最优先候选一票否决。
            if let Some(candidate) = replicas.iter().find(|candidate| {
                candidate.node.client_addr == hot_standby.client_addr
                    && replica_failover_allowed(candidate, self.max_replica_lag_seconds)
            }) {
                return Ok(candidate.node.clone());
            }
            warn!(
                proxy = %self.name,
                hot_standby = %hot_standby.client_addr,
                purpose = %purpose,
                "replication HA hot standby is not eligible for failover; falling back to lag-based replica selection"
            );
        }
        select_replica_candidate(replicas, purpose, self.max_replica_lag_seconds)
    }

    /// 业务作用: 发现在线副本仍跟随旧主或其它上游时，在协调权威保护下把它们全部改为直连当前主节点。
    ///
    /// 参数说明:
    /// - `master`: 当前已经确认且与数据面路由一致的唯一主节点。
    /// - `replicas`: 本轮真实探测到的副本观测，用于避免无拓扑漂移时申请控制权。
    ///
    /// 返回: 执行过拓扑收敛时返回 `true`，调用方必须等待下一轮新观测后再开放路由或分配持久化角色。
    async fn repair_replica_upstream_drift(
        &self,
        master: &HaNode,
        replicas: &[ReplicaCandidate],
    ) -> anyhow::Result<bool> {
        // 观察者不收敛拓扑:副本漂移由管理这组后端的控制面收敛,这里照常开放路由。
        if self.observer {
            return Ok(false);
        }
        let drifted = replicas
            .iter()
            .any(|replica| !replica_candidate_follows_node(replica, master));
        if !drifted {
            self.repair_retry_not_before_ms.store(0, Ordering::Relaxed);
            return Ok(false);
        }
        // 收敛失败后按退避窗口保持保护，不重复对坏副本执行复制控制命令；
        // 与持久化和 fencing 的失败退避同一口径。
        if now_ms() < self.repair_retry_not_before_ms.load(Ordering::Relaxed) {
            let reason = format!(
                "replication HA replica upstream repair is backing off after a failure; replicas still do not directly follow `{}`",
                master.client_addr
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        let started = Instant::now();
        // 权威获取失败(非领导者、租约争用等瞬断)只做一个探测周期的短退避,
        // 不惩罚随后的合法收敛;控制命令真实失败才用全额退避。
        let authority = match self.acquire_repair_authority(master).await {
            Ok(authority) => authority,
            Err(error) => {
                self.repair_failure_count.fetch_add(1, Ordering::Relaxed);
                self.repair_retry_not_before_ms.store(
                    now_ms().saturating_add(self.probe_interval_ms),
                    Ordering::Relaxed,
                );
                return Err(error);
            }
        };
        match self
            .execute_replica_upstream_repair(master, authority)
            .await
        {
            Ok(()) => {
                self.repair_retry_not_before_ms.store(0, Ordering::Relaxed);
                self.repair_count.fetch_add(1, Ordering::Relaxed);
                self.last_repair_elapsed_micros
                    .store(elapsed_micros(started), Ordering::Relaxed);
                Ok(true)
            }
            Err(error) => {
                self.repair_failure_count.fetch_add(1, Ordering::Relaxed);
                let retry_delay_ms = self
                    .coordinator
                    .lease_ttl_ms
                    .max(self.probe_interval_ms.saturating_mul(10));
                self.repair_retry_not_before_ms
                    .store(now_ms().saturating_add(retry_delay_ms), Ordering::Relaxed);
                Err(error)
            }
        }
    }

    /// 业务作用: 为一次副本上游收敛取得协调权威，先进保护再申请，失败视为权威瞬断。
    ///
    /// 参数说明:
    /// - `master`: 收敛目标主节点，用于保护原因与协调动作绑定。
    ///
    /// 返回: 持有旧租约或集群动作令牌的收敛权威。
    async fn acquire_repair_authority(&self, master: &HaNode) -> anyhow::Result<RepairAuthority> {
        self.enter_protection(format!(
            "replication HA is repairing replicas that do not directly follow `{}`",
            master.client_addr
        ))
        .await;
        let (lease, cluster_action) = if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
            let action = self
                .acquire_cluster_manual_action(
                    CoordinationActionKind::TopologyRepair,
                    "replica-upstream-repair",
                    master.client_addr.clone(),
                    master.client_addr.clone(),
                )
                .await?;
            (None, Some(action))
        } else {
            (
                self.acquire_failover_lease("replica upstream repair")
                    .await?,
                None,
            )
        };
        if let Some(action) = cluster_action.as_ref() {
            self.ensure_cluster_action_or_protect(action, "before replica upstream repair")
                .await?;
        }
        if let Some(lease) = lease.as_ref() {
            if let Err(error) = lease.ensure_active().await {
                let reason = format!(
                    "replication HA replica upstream repair lost coordinator lease before changing topology: {error}"
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
        }
        Ok(RepairAuthority {
            lease,
            cluster_action,
        })
    }

    /// 业务作用: 在已取得的协调权威下执行一次副本上游收敛动作。
    ///
    /// 参数说明:
    /// - `master`: 全部副本必须直接跟随的当前唯一主节点。
    /// - `authority`: 已通过活跃性校验的旧租约或集群动作令牌。
    ///
    /// 返回: 全部漂移副本完成收敛并确认复制后返回成功。
    async fn execute_replica_upstream_repair(
        &self,
        master: &HaNode,
        authority: RepairAuthority,
    ) -> anyhow::Result<()> {
        let RepairAuthority {
            lease,
            mut cluster_action,
        } = authority;
        let result = self
            .demote_extra_masters_checked(master, lease.as_ref(), cluster_action.as_ref(), None)
            .await;
        self.protect_ha_control_step(result, "during replica upstream repair")
            .await?;

        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(
                action,
                "before committing replica upstream repair",
            )
            .await?;
            self.complete_cluster_action_or_protect(
                action,
                &master.client_addr,
                "after replica upstream repair",
            )
            .await?;
        }
        Ok(())
    }

    /// 业务作用: 把额外 master 或漂移副本收敛到目标主节点，并对本轮已确认的额外 master 执行 fail-closed 复验。
    /// 参数说明:
    /// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
    /// - `lease_guard`: local-file/Redis 协调模式的活动租约；存在时在每个后端步骤前复验。
    /// - `cluster_action`: 进程级协调动作守卫；存在时在每个探测和复制步骤前复验，旧协调模式为空。
    /// - `newly_required_confirmations`: 本轮新观测为额外 master 的地址；函数会与历史未确认集合合并，探测失败或角色未知时必须视为收敛失败。
    ///
    /// 返回: 所有需要改写的节点完成复制验证，且必须确认的额外 master 均已证明为 replica 时返回成功。
    async fn demote_extra_masters_checked(
        &self,
        master: &HaNode,
        lease_guard: Option<&LeaseGuard>,
        cluster_action: Option<&ClusterActionGuard>,
        newly_required_confirmations: Option<&HashSet<String>>,
    ) -> anyhow::Result<()> {
        let mut required_confirmations = self.unconfirmed_extra_masters.read().await.clone();
        if let Some(newly_required) = newly_required_confirmations {
            required_confirmations.extend(newly_required.iter().cloned());
        }
        let mut failures = Vec::new();
        let mut confirmed_required = HashSet::new();
        let mut failed_required = HashSet::new();
        for node in self.ha.nodes() {
            if node.client_addr == master.client_addr {
                continue;
            }
            // 单个组可能包含多个待收敛节点，不能把循环视为一个原子动作。每次访问后端前都复验
            // 协调令牌，使领导权变化、路由权威过期或动作被替换后最多停在当前有界网络请求，
            // 不会继续对下一个节点发出复制命令。
            if let Some(lease) = lease_guard {
                lease.ensure_active().await.with_context(|| {
                    "replication HA topology convergence lost coordinator lease before probing a node"
                })?;
            }
            if let Some(action) = cluster_action {
                self.ensure_cluster_action_or_protect(action, "between extra-master probe steps")
                    .await?;
            }
            match self.ha.backend().inspect_node(node).await {
                Ok(observation) => {
                    let should_replicate = match observation.role {
                        HaRole::Master => true,
                        HaRole::Replica => !observation.follows(master),
                        HaRole::Unknown => {
                            if required_confirmations.contains(&node.client_addr) {
                                failed_required.insert(node.client_addr.clone());
                                failures.push(format!(
                                    "{}: role is unknown after this node was observed as an extra writable master",
                                    node.client_addr
                                ));
                            }
                            false
                        }
                    };
                    if should_replicate {
                        if let Some(lease) = lease_guard {
                            lease.ensure_active().await.with_context(|| {
                                "replication HA topology convergence lost coordinator lease before changing a node"
                            })?;
                        }
                        if let Some(action) = cluster_action {
                            self.ensure_cluster_action_or_protect(
                                action,
                                "between extra-master replication steps",
                            )
                            .await?;
                        }
                        let convergence = async {
                            self.replicate_from(node, master, lease_guard, cluster_action)
                                .await?;
                            let authority = RuntimeActionAuthority {
                                runtime: self,
                                lease: lease_guard,
                                cluster_action,
                            };
                            self.ha
                                .backend()
                                .verify_replication(node, master, &authority)
                                .await
                        }
                        .await;
                        if let Err(err) = convergence {
                            failed_required.insert(node.client_addr.clone());
                            failures.push(format!("{}: {err}", node.client_addr));
                            warn!(
                                proxy = %self.name,
                                node = %node.client_addr,
                                master = %master.replication_addr,
                                error = %err,
                                "failed to converge replication HA node to current master"
                            );
                        } else if required_confirmations.contains(&node.client_addr) {
                            confirmed_required.insert(node.client_addr.clone());
                        }
                    } else if observation.role == HaRole::Replica
                        && required_confirmations.contains(&node.client_addr)
                    {
                        confirmed_required.insert(node.client_addr.clone());
                    }
                }
                Err(err) => {
                    self.warn_probe_failure(node, &err).await;
                    // 本轮刚确认过的可写节点不能因第二次探测失败就从安全证明中
                    // 消失；不对称分区下它可能仍接受旁路写入，必须跨 tick 保持保护。
                    if required_confirmations.contains(&node.client_addr) {
                        failed_required.insert(node.client_addr.clone());
                        failures.push(format!(
                            "{}: probe failed after this node was observed as an extra writable master: {err}",
                            node.client_addr
                        ));
                    }
                }
            }
        }
        for addr in &required_confirmations {
            if addr == &master.client_addr {
                // 人工定向 promote 可以把此前的额外 master 选成新的权威主节点；
                // 它已由 fencing、动作提交和本次目标绑定证明，不应再要求降为副本。
                confirmed_required.insert(addr.clone());
            }
            if !confirmed_required.contains(addr) && !failed_required.contains(addr) {
                failures.push(format!(
                    "{addr}: configured extra master was not visited during convergence"
                ));
            }
        }
        if failures.is_empty() {
            if !required_confirmations.is_empty() {
                // 只有本轮后端事实已经证明节点不再可写，才清除跨 tick 风险记录；
                // 单纯的探测缺席绝不能让下一轮唯一主快速路径重新开放路由。
                self.unconfirmed_extra_masters
                    .write()
                    .await
                    .retain(|addr| !required_confirmations.contains(addr));
            }
            Ok(())
        } else {
            let recovery = if self.fencing.mode == HaFencingMode::ExternalCommand {
                "restore every unresolved node as a replica, or fence each permanently unavailable risk with POST /ha/node/{node}/fence before retrying the target operation"
            } else {
                "restore every unresolved node as a replica; fencing.mode=manual is not physical isolation proof and cannot clear an unavailable master risk"
            };
            bail!(
                "failed to demote replication HA extra masters to `{}`: {}; recovery: {recovery}",
                master.client_addr,
                failures.join("; ")
            )
        }
    }

    /// 业务作用: 记录一次多主观测中除当前路由主节点外的全部可写节点，供后续探测轮和统一 Serving 门禁复核。
    /// 参数说明:
    /// - `current`: 本轮开始时的数据面权威主节点。
    /// - `masters`: 本轮真实探测到的全部 master。
    ///
    /// 返回: 本轮新增的额外 master 地址集合，调用方可直接作为降级动作的必须确认集合。
    async fn remember_unconfirmed_extra_masters(
        &self,
        current: &HaNode,
        masters: &[HaNode],
    ) -> HashSet<String> {
        let observed = masters
            .iter()
            .filter(|master| master.client_addr != current.client_addr)
            .map(|master| master.client_addr.clone())
            .collect::<HashSet<_>>();
        if !observed.is_empty() {
            // 记账只保存观测事实，不发送任何后端命令，因此 observer 也必须执行；
            // 否则额外 master 在不对称分区中消失后，旁路会误把旧 current 当成唯一主。
            self.unconfirmed_extra_masters
                .write()
                .await
                .extend(observed.iter().cloned());
        }
        observed
    }

    /// 业务作用: 清除已经具备“不再是额外可写节点”证据的风险项，并返回仍会阻止 Serving 的地址。
    /// 参数说明:
    /// - `authoritative_master`: 已由调用路径完成证据门禁、即将或已经成为 current 的主节点。
    /// - `replicas`: 本轮明确探测为 replica 的节点；发布总门禁没有角色快照时传空集合。
    ///
    /// 返回: 排序后的未确认风险地址，便于保护原因稳定展示。
    async fn resolve_unconfirmed_extra_masters(
        &self,
        authoritative_master: &str,
        replicas: &[ReplicaCandidate],
    ) -> Vec<String> {
        let mut pending = self.unconfirmed_extra_masters.write().await;
        // 风险项只可能在它还是“非 current 的额外 master”时写入。若后续证词、
        // 协调 outcome、受限跟随或受权运维动作把它选成 current，发布调用本身
        // 已经过对应证据门禁，因此目标不再属于“额外”可写节点。
        pending.remove(authoritative_master);
        pending.retain(|addr| {
            !replicas
                .iter()
                .any(|replica| replica.node.client_addr == *addr)
        });
        let mut unresolved = pending.iter().cloned().collect::<Vec<_>>();
        unresolved.sort_unstable();
        unresolved
    }

    /// 业务作用: 在配置事务替换或删除当前 HA 运行代前，阻止未确认多主证据被热更新静默清空。
    ///
    /// 返回: 风险集合为空时允许事务继续；否则返回包含稳定地址列表和安全解锁流程的错误。
    pub(crate) async fn ensure_configuration_change_has_no_unconfirmed_extra_masters(
        &self,
    ) -> anyhow::Result<()> {
        let mut pending = self
            .unconfirmed_extra_masters
            .read()
            .await
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        pending.sort_unstable();
        bail!(
            "replication HA configuration change is blocked because unresolved extra-master evidence would be lost: {}; first restore each node as a replica or, with fencing.mode=external-command, fence each permanently unavailable risk via POST /ha/node/{{node}}/fence; see docs/operations.md",
            pending.join(",")
        )
    }

    /// 业务作用: 在唯一主快速路径开放路由前，复核此前观测到的额外 master 已重新出现为副本。
    /// 参数说明:
    /// - `current`: 当前唯一主节点。
    /// - `replicas`: 本轮成功探测到的副本集合。
    ///
    /// 返回: 历史额外 master 均已确认不再可写时返回成功；节点缺席或角色未知时保持保护并返回错误。
    async fn ensure_no_unconfirmed_extra_masters(
        &self,
        current: &HaNode,
        replicas: &[ReplicaCandidate],
    ) -> anyhow::Result<()> {
        let unresolved = self
            .resolve_unconfirmed_extra_masters(&current.client_addr, replicas)
            .await;
        if unresolved.is_empty() {
            return Ok(());
        }
        let reason = format!(
            "replication HA keeps routing protected because previously observed extra masters are not yet confirmed as replicas of `{}`: {}",
            current.client_addr,
            unresolved.join(",")
        );
        self.enter_protection(reason.clone()).await;
        bail!(reason)
    }

    /// 业务作用: 从最近观测中返回指定节点角色，缺席时保持 Unknown 而不猜测拓扑。
    /// 参数说明:
    /// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
    ///
    /// 返回: 节点角色探测成功时返回归一化角色；运行代在探测期间退休时保留退休错误。
    // 普通探测故障降级为 Unknown 供上层按证据不足处理；运行代退休则继续返回错误，
    // 防止旧代在探测结束后参与新的控制动作。
    async fn node_role(&self, node: &HaNode) -> anyhow::Result<HaRole> {
        match self.ha.backend().inspect_node(node).await {
            Ok(observation) => {
                self.ensure_not_retired("after probing a node role")?;
                Ok(observation.role)
            }
            Err(err) => {
                self.ensure_not_retired("after a node role probe failed")?;
                warn!(
                    proxy = %self.name,
                    node = %node.client_addr,
                    error = %err,
                    "replication HA node probe failed"
                );
                Ok(HaRole::Unknown)
            }
        }
    }
}

/// 一次副本上游收敛动作持有的协调权威。
///
/// 字段说明: `lease` 为旧协调模式租约,`cluster_action` 为进程级动作令牌;二者按协调模式二选一。
struct RepairAuthority {
    lease: Option<LeaseGuard>,
    cluster_action: Option<ClusterActionGuard>,
}

/// 业务作用: 表示构造期主节点发现结果，把"能直接服务"与"需等待恢复"两种启动形态显式分开。
/// 分支说明: `Confirmed` 表示观测到唯一在线主节点，可直接发布路由；`PendingRecovery`
/// 携带集群已提交或由副本上游推导的旧主节点，构造期以保护态启动并把恢复交给
/// reconcile 循环的标准切主机制。
enum StartupMasterDiscovery {
    Confirmed(HaNode),
    PendingRecovery(HaNode),
}
