impl ReplicationHaRuntime {
/// 业务作用: 从 proxy 配置构造 replication HA runtime，先以保护态启动，发现唯一在线 master 后发布路由。
    /// 构造期只做拓扑发现，绝不执行 fence/promote；无主时以保护态返回并把恢复交给激活后的 reconcile 循环。
    /// 参数说明:
    /// - `item`: replication-ha listener 配置，包含节点列表、认证、探活、fencing、coordinator 和产品持久化参数。
    ///
    /// 返回: 初始化完成的 runtime；节点配置缺失、租约配置不完整、观测到多主或无法定位恢复目标时返回错误并阻止 listener 启动。
    pub async fn from_proxy_item(item: &ProxyItem) -> anyhow::Result<Self> {
        Self::from_proxy_item_with_coordinator(item, None, None).await
    }

    /// 业务作用: 从代理配置和可选进程级协调器构造 replication HA runtime，`proxy-cluster` 模式缺少共享句柄时拒绝启动。
    /// 参数说明:
    /// - `item`: replication HA 代理项配置。
    /// - `cluster_coordinator`: 主进程启动的共享协调器；旧协调模式为空。
    /// - `cluster_config_digest`: 构造该运行时所依据的全局集群配置摘要。
    ///
    /// 返回: 返回先发现后端拓扑、再依据协调权威决定是否开放路由的运行时。
    pub async fn from_proxy_item_with_coordinator(
        item: &ProxyItem,
        cluster_coordinator: Option<Arc<CoordinatorHandle>>,
        cluster_config_digest: Option<String>,
    ) -> anyhow::Result<Self> {
        let replication_ha = item.replication_ha_config()?;
        // coordinator.mode 省略时按是否组集群自动解析:有内置协调集群 handle → proxy-cluster（leader 切主）,
        // 否则 → none（单实例进程内串行）。多实例必然组集群,不存在"多实例 + none"的双主中间态。
        let coordinator_mode = replication_ha
            .coordinator
            .effective_mode(cluster_coordinator.is_some());
        let group_config_digest = (coordinator_mode == HaCoordinatorMode::ProxyCluster)
            .then(|| ha_group_config_digest(item, replication_ha));
        let persistence_verify_timeout_ms = replication_ha
            .persistence
            .dragonfly
            .as_ref()
            .map(|dragonfly| dragonfly.verify_timeout_ms)
            .or_else(|| {
                replication_ha
                    .persistence
                    .redis
                    .as_ref()
                    .map(|redis| redis.verify_timeout_ms)
            })
            .or_else(|| {
                replication_ha
                    .persistence
                    .valkey
                    .as_ref()
                    .map(|valkey| valkey.verify_timeout_ms)
            })
            .or_else(|| {
                replication_ha
                    .persistence
                    .redict
                    .as_ref()
                    .map(|redict| redict.verify_timeout_ms)
            })
            .or_else(|| {
                replication_ha
                    .persistence
                    .keydb
                    .as_ref()
                    .map(|keydb| keydb.verify_timeout_ms)
            })
            .unwrap_or(item.backend_request_timeout_ms);
        let nodes = parse_nodes(item.nodes.as_deref().with_context(|| {
            format!("proxy `{}` replication-ha mode requires nodes", item.name)
        })?)?;
        let (routing_tx, _) = watch::channel(HaRoutingState::Protected {
            reason: "replication-ha runtime is initializing".to_string(),
        });
        let coordinator_owner_id = item
            .replication_ha_config()?
            .coordinator
            .owner_id
            .clone()
            .unwrap_or_else(|| default_coordinator_owner_id(&item.name));
        let generation_guard = HaGenerationGuard::default();
        let backend = HaBackendDriver::from_proxy_item_with_guard(
            item,
            coordinator_owner_id.clone(),
            generation_guard.clone(),
        )?;
        let persistence_capabilities = backend.persistence_capabilities();
        if replication_ha.persistence.enabled
            && (!persistence_capabilities.runtime_reconfigure
                || !persistence_capabilities.force_checkpoint
                || !persistence_capabilities.verify_checkpoint
                || persistence_capabilities.restart_durable)
        {
            anyhow::bail!(
                "backend_profile={} does not satisfy NARIX-managed dynamic persistence capabilities",
                item.backend_profile.as_str()
            );
        }
        let runtime = Self {
            name: item.name.clone(),
            ha: HaRuntime::new(nodes, backend, item.backend_profile),
            probe_interval_ms: replication_ha.probe_interval_ms,
            master_down_after_failures: replication_ha.master_down_after_failures,
            max_replica_lag_seconds: replication_ha.max_replica_lag,
            // 观察者绝不发起切主；生效值在此收敛为 false,状态接口据此如实呈现。
            auto_failover: replication_ha.auto_failover && !replication_ha.observer,
            observer: replication_ha.observer,
            fencing: RuntimeFencingConfig {
                mode: replication_ha.fencing.mode,
                command: replication_ha.fencing.command.clone(),
                timeout_ms: replication_ha.fencing.timeout_ms,
            },
            coordinator: RuntimeCoordinatorConfig {
                mode: coordinator_mode,
                owner_id: coordinator_owner_id,
                lease_path: replication_ha
                    .coordinator
                    .lease_path
                    .as_ref()
                    .map(PathBuf::from),
                redis: if coordinator_mode == HaCoordinatorMode::Redis {
                    let key = replication_ha
                        .coordinator
                        .redis_key
                        .clone()
                        .with_context(|| {
                            format!(
                                "proxy `{}` ha.coordinator.mode=redis requires redis_key",
                                item.name
                            )
                        })?;
                    Some(RedisCoordinatorConfig {
                        addr: replication_ha
                            .coordinator
                            .redis_addr
                            .clone()
                            .with_context(|| {
                                format!(
                                    "proxy `{}` ha.coordinator.mode=redis requires redis_addr",
                                    item.name
                                )
                            })?,
                        username: replication_ha.coordinator.redis_username.clone(),
                        password: replication_ha.coordinator.redis_password.clone(),
                        outcome_key: format!("{key}:outcome"),
                        key,
                        connect_timeout_ms: replication_ha.coordinator.redis_connect_timeout_ms,
                        request_timeout_ms: replication_ha.coordinator.redis_request_timeout_ms,
                        tcp_keepalive_ms: item.tcp_keepalive_ms,
                    })
                } else {
                    None
                },
                lease_ttl_ms: replication_ha.coordinator.lease_ttl_ms,
                lease_renew_interval_ms: replication_ha.coordinator.lease_renew_interval_ms,
                lease_acquire_timeout_ms: replication_ha.coordinator.lease_acquire_timeout_ms,
            },
            cluster_coordinator: if coordinator_mode == HaCoordinatorMode::ProxyCluster {
                let handle = cluster_coordinator
                    .context("ha.coordinator.mode=proxy-cluster requires process coordinator")?;
                Some(RuntimeClusterCoordinatorConfig {
                    observation_quorum: if replication_ha.coordinator.observation_quorum == 0 {
                        handle.majority()
                    } else {
                        replication_ha.coordinator.observation_quorum
                    },
                    handle,
                    group_id: replication_ha.coordinator.effective_group_id(&item.name),
                    observation_window: Duration::from_millis(
                        replication_ha.coordinator.observation_window_ms,
                    ),
                })
            } else {
                None
            },
            group_config_digest: group_config_digest.clone(),
            cluster_config_digest: if coordinator_mode == HaCoordinatorMode::ProxyCluster {
                Some(cluster_config_digest.context(
                    "ha.coordinator.mode=proxy-cluster requires cluster configuration digest",
                )?)
            } else {
                None
            },
            cluster_config_revision: AtomicU64::new(0),
            persistence: RuntimePersistenceConfig {
                policy: if replication_ha.persistence.enabled {
                    HaPersistencePolicy::ReplicaOnly
                } else {
                    HaPersistencePolicy::None
                },
                force_snapshot_on_handoff: replication_ha.persistence.force_checkpoint_on_handoff,
                min_replica_stable_ms: replication_ha.persistence.min_replica_stable_ms,
                require_zero_lag: replication_ha.persistence.require_zero_lag,
                verify_timeout_ms: persistence_verify_timeout_ms,
                fail_closed: replication_ha.persistence.fail_closed,
            },
            failover_lock: AsyncMutex::new(()),
            observed_takeover: RwLock::new(ObservedTakeover::default()),
            unconfirmed_extra_masters: RwLock::new(HashSet::new()),
            current_master: RwLock::new(HaNode {
                client_addr: String::new(),
                replication_addr: String::new(),
            }),
            routing_tx,
            last_error: RwLock::new(None),
            replica_seen_since_ms: RwLock::new(HashMap::new()),
            replica_last_seen_ms: RwLock::new(HashMap::new()),
            persistence_topology: RwLock::new(PersistenceTopologyState::default()),
            persistence_last_error: RwLock::new(None),
            persistence_protected_reason: RwLock::new(None),
            persistence_retry_not_before_ms: AtomicU64::new(0),
            master_failure_streak: AtomicU64::new(0),
            reconcile_warning_not_before_ms: AtomicU64::new(0),
            automatic_retry_not_before_ms: AtomicU64::new(0),
            repair_retry_not_before_ms: AtomicU64::new(0),
            generation_guard,
            repair_count: AtomicU64::new(0),
            repair_failure_count: AtomicU64::new(0),
            last_repair_elapsed_micros: AtomicU64::new(0),
            probe_failure_log_not_before_ms: RwLock::new(HashMap::new()),
            failover_count: AtomicU64::new(0),
            last_failover_elapsed_micros: AtomicU64::new(0),
            fence_count: AtomicU64::new(0),
            protection_count: AtomicU64::new(0),
            fence_failure_count: AtomicU64::new(0),
            last_fence_elapsed_micros: AtomicU64::new(0),
            lease_acquire_count: AtomicU64::new(0),
            lease_acquire_failure_count: AtomicU64::new(0),
            last_lease_acquire_elapsed_micros: AtomicU64::new(0),
            persistence_handoff_count: AtomicU64::new(0),
            persistence_adopt_count: AtomicU64::new(0),
            persistence_enable_count: AtomicU64::new(0),
            persistence_enable_failure_count: AtomicU64::new(0),
            persistence_disable_count: AtomicU64::new(0),
            persistence_disable_failure_count: AtomicU64::new(0),
            persistence_snapshot_count: AtomicU64::new(0),
            persistence_snapshot_failure_count: AtomicU64::new(0),
            persistence_verify_count: AtomicU64::new(0),
            persistence_verify_failure_count: AtomicU64::new(0),
            persistence_protection_count: AtomicU64::new(0),
            persistence_last_handoff_ms: AtomicU64::new(0),
            persistence_last_snapshot_ms: AtomicU64::new(0),
            persistence_last_verify_duration_micros: AtomicU64::new(0),
        };
        if let (Some(cluster), Some(config_digest), Some(cluster_config_digest)) = (
            &runtime.cluster_coordinator,
            group_config_digest,
            runtime.cluster_config_digest.as_ref(),
        ) {
            if !cluster
                .handle
                .cluster_config_accepts_digest(cluster_config_digest)?
            {
                runtime
                    .enter_protection(
                        "HA runtime was built from a stale cluster configuration".to_string(),
                    )
                    .await;
                return Ok(runtime);
            }
            let deadline = Instant::now() + cluster.handle.action_timeout();
            loop {
                match cluster
                    .handle
                    .register_group_config(
                        &cluster.group_id,
                        config_digest.clone(),
                        cluster_config_digest.clone(),
                    )
                    .await
                {
                    Ok(config_revision) => {
                        runtime
                            .cluster_config_revision
                            .store(config_revision, Ordering::Release);
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        warn!(
                            proxy = %runtime.name,
                            error = %error,
                            "等待协调领导者提交 replication HA 组配置摘要"
                        );
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(error) => {
                        return Err(error)
                            .context("register replication HA proxy-cluster group configuration");
                    }
                }
            }
        }
        let master = match runtime.discover_startup_master().await? {
            StartupMasterDiscovery::Confirmed(master) => master,
            StartupMasterDiscovery::PendingRecovery(old_master) => {
                // 构造期不改写拓扑：上一运行代可能仍在活跃执行自动切主，构造期 promote
                // 会绕过跨代互斥形成双主。以保护态启动并把旧主设为当前目标,激活后的
                // reconcile 循环按连败宽限、候选门禁与协调令牌完成恢复或等旧主归队。
                *runtime.current_master.write().await = old_master.clone();
                runtime.protect_until_initial_persistence_is_verified().await;
                runtime
                    .enter_protection(format!(
                        "no replication HA master observed at startup; routing stays closed while automatic recovery confirms `{}` is down",
                        old_master.client_addr
                    ))
                    .await;
                return Ok(runtime);
            }
        };
        if let Some(cluster) = &runtime.cluster_coordinator {
            let committed_master = cluster
                .handle
                .group_state(&cluster.group_id)?
                .and_then(|group| group.current_master);
            if let Some(committed) = committed_master.as_deref() {
                if committed != master.client_addr {
                    let committed_node = runtime.find_node(committed).with_context(|| {
                        format!(
                            "coordinator-committed replication HA master `{committed}` is not configured"
                        )
                    })?;
                    *runtime.current_master.write().await = committed_node;
                    runtime
                        .enter_protection(format!(
                            "replication HA discovered master `{}` differs from coordinator-committed master `{committed}`; automatic recovery must fence and commit the observed topology",
                            master.client_addr
                        ))
                        .await;
                    return Ok(runtime);
                }
            }
            runtime
                .ensure_cluster_initial_master_committed(&master)
                .await?;
            *runtime.current_master.write().await = master.clone();
            runtime.protect_until_initial_persistence_is_verified().await;
            if !cluster.handle.route_authoritative().await {
                runtime
                    .enter_protection(
                        "replication HA routing waits for proxy-cluster coordinator authority"
                            .to_string(),
                    )
                    .await;
            } else {
                let master = runtime.current_master.read().await.clone();
                runtime.set_current_master(master).await;
            }
        } else {
            runtime
                .ensure_cluster_initial_master_committed(&master)
                .await?;
            runtime.protect_until_initial_persistence_is_verified().await;
            runtime.set_current_master(master).await;
        }
        Ok(runtime)
    }

    /// 业务作用: 在严格持久化模式首次收敛完成前建立粘性数据面保护，阻止构造期发现唯一主节点后提前开放路由。
    ///
    /// 构造期只确认唯一主节点，不会执行动态配置或 checkpoint；因此看到主节点
    /// 不能等价为持久化 owner 已验证。该原因由首次成功的
    /// `reconcile_persistence` 清除，并同时阻止其它拓扑路径提前发布 Serving。
    /// 参数说明: 无。
    ///
    /// 返回: 无显式返回值；严格模式写入持久化保护原因并保持 Protected，非严格模式不改变路由。
    async fn protect_until_initial_persistence_is_verified(&self) {
        if self.persistence.policy == HaPersistencePolicy::None || !self.persistence.fail_closed {
            return;
        }
        let reason = "initial HA persistence owner has not been verified".to_string();
        *self.persistence_protected_reason.write().await = Some(reason.clone());
        self.enter_protection(format!("replication HA persistence protected: {reason}"))
            .await;
    }

    /// 业务作用: 将当前 HA 摘要重新绑定到已经提交的全局配置代际。
    ///
    /// 参数说明:
    /// - `cluster_config_digest`: 当前配置事务已经提交的全局规范化摘要。
    ///
    /// 返回: 非 `proxy-cluster` 模式直接成功；组摘要经共识提交后返回成功。
    pub(crate) async fn refresh_cluster_group_registration(
        &self,
        cluster_config_digest: &str,
    ) -> anyhow::Result<()> {
        let Some(cluster) = &self.cluster_coordinator else {
            return Ok(());
        };
        let config_digest = self
            .group_config_digest
            .clone()
            .context("proxy-cluster HA runtime has no group configuration digest")?;
        let config_revision = cluster
            .handle
            .register_group_config(
                &cluster.group_id,
                config_digest,
                cluster_config_digest.to_string(),
            )
            .await?;
        self.cluster_config_revision
            .store(config_revision, Ordering::Release);
        Ok(())
    }

    /// 业务作用: 返回本 HA 运行时已经由协调状态机确认的全局版本和组摘要。
    ///
    /// 返回: `proxy-cluster` 运行时的配置绑定；其他模式或摘要缺失时返回错误。
    fn cluster_runtime_binding(&self) -> anyhow::Result<(u64, String)> {
        let group_config_digest = self
            .group_config_digest
            .clone()
            .context("proxy-cluster HA runtime has no group configuration digest")?;
        Ok((
            self.cluster_config_revision.load(Ordering::Acquire),
            group_config_digest,
        ))
    }

    /// 业务作用: 读取当前已发布的 master 地址，供管理接口、测试流程和切主前后对比使用。
    /// 返回: 当前 master 的客户端连接地址。
    // 返回克隆值而不是暴露锁守卫，调用方不会在后续网络 await 期间占用主节点读锁。
    pub async fn current_master_addr(&self) -> String {
        self.current_master.read().await.client_addr.clone()
    }

    /// 业务作用: 返回当前活动运行代确认的完整主节点地址，供配置事务验证删主前的副本过渡态。
    ///
    /// 返回: 同时包含业务连接地址和复制登记地址的主节点副本。
    pub(crate) async fn current_master_node(&self) -> HaNode {
        self.current_master.read().await.clone()
    }

    /// 业务作用: 订阅 HA 路由状态变化，让数据面 relay 能在切主或保护态时主动断开旧连接。
    /// 返回: watch receiver，初始值为当前 Serving 或 Protected 状态。
    pub fn routing_rx(&self) -> watch::Receiver<HaRoutingState> {
        self.routing_tx.subscribe()
    }

    /// 业务作用: 读取轻量路由状态摘要，管理面的 ready/status 不需要重新探活即可判断是否可服务。
    /// 返回: 状态名称和保护原因；Serving 时原因为空。
    // 管理面只读 watch 快照，不在健康请求路径触发新的后端探测或控制动作。
    pub fn routing_status(&self) -> (&'static str, Option<String>) {
        match self.routing_tx.borrow().clone() {
            HaRoutingState::Serving { .. } => ("serving", None),
            HaRoutingState::Protected { reason } => ("protected", Some(reason)),
        }
    }

    /// 业务作用: 构造 replication HA 管理面完整状态快照，包含当前路由、探活结果、切主计数、租约和自适应持久化状态。
    /// 返回: 可序列化的状态对象；节点探活失败会标记为 unknown，不会让状态接口整体失败。
    pub async fn status(&self) -> ReplicationHaStatus {
        let current_master = self.current_master_addr().await;
        let last_error = self.last_error.read().await.clone();
        let (routing_state, protection_reason) = self.routing_status();
        let probes = self.ha.nodes().iter().cloned().map(|node| async move {
            let started = Instant::now();
            let (role, replica_lag_seconds) = match self.ha.backend().inspect_node(&node).await {
                Ok(observation) => (observation.role, observation.replica_lag_seconds),
                Err(_) => (HaRole::Unknown, None),
            };
            let probe_latency_micros = elapsed_micros(started);
            ReplicationHaNodeStatus {
                client_addr: node.client_addr.clone(),
                replication_addr: node.replication_addr.clone(),
                role: role.as_str().to_string(),
                probe_latency_micros,
                replica_lag_seconds,
            }
        });
        let nodes = join_all(probes).await;
        ReplicationHaStatus {
            current_master,
            probe_interval_ms: self.probe_interval_ms,
            master_down_after_failures: self.master_down_after_failures,
            max_replica_lag_seconds: self.max_replica_lag_seconds,
            auto_failover: self.auto_failover,
            observer: self.observer,
            master_failure_streak: self.master_failure_streak.load(Ordering::Relaxed),
            failover_count: self.failover_count.load(Ordering::Relaxed),
            last_failover_elapsed_micros: self.last_failover_elapsed_micros.load(Ordering::Relaxed),
            fencing_mode: self.fencing.mode.as_str().to_string(),
            fence_count: self.fence_count.load(Ordering::Relaxed),
            protection_count: self.protection_count.load(Ordering::Relaxed),
            fence_failure_count: self.fence_failure_count.load(Ordering::Relaxed),
            last_fence_elapsed_micros: self.last_fence_elapsed_micros.load(Ordering::Relaxed),
            replica_repair_count: self.repair_count.load(Ordering::Relaxed),
            replica_repair_failure_count: self.repair_failure_count.load(Ordering::Relaxed),
            last_replica_repair_elapsed_micros: self
                .last_repair_elapsed_micros
                .load(Ordering::Relaxed),
            coordinator_mode: self.coordinator.mode.as_str().to_string(),
            coordinator_owner_id: self.coordinator.owner_id.clone(),
            lease_acquire_count: self.lease_acquire_count.load(Ordering::Relaxed),
            lease_acquire_failure_count: self.lease_acquire_failure_count.load(Ordering::Relaxed),
            last_lease_acquire_elapsed_micros: self
                .last_lease_acquire_elapsed_micros
                .load(Ordering::Relaxed),
            routing_state: routing_state.to_string(),
            protection_reason,
            nodes,
            persistence: self.persistence_status().await,
            last_error,
        }
    }

    /// 业务作用: 生成自适应 persistence 状态快照，展示热备、副本快照 owner、最近 handoff 和失败保护原因。
    /// 返回: 可序列化的持久化状态对象，供管理接口和稳定性测试读取。
    async fn persistence_status(&self) -> ReplicationHaPersistenceStatus {
        let topology = self.persistence_topology.read().await.clone();
        let primary_owner = topology.snapshot_owners.first().cloned();
        let persistence_capabilities = self.ha.backend().persistence_capabilities();
        ReplicationHaPersistenceStatus {
            policy: self.persistence.policy.as_str().to_string(),
            switch_mode: if persistence_capabilities.runtime_reconfigure {
                "native-config"
            } else {
                "backend-managed"
            }
            .to_string(),
            topology: topology.kind.as_str().to_string(),
            hot_standby: topology
                .hot_standby
                .as_ref()
                .map(|node| node.client_addr.clone()),
            owner: primary_owner
                .as_ref()
                .map(|owner| owner.node.client_addr.clone()),
            owner_verified: primary_owner.as_ref().is_some_and(|owner| owner.verified),
            owner_role: primary_owner.map(|owner| owner.role.as_str().to_string()),
            snapshot_owners: topology
                .snapshot_owners
                .iter()
                .map(|owner| ReplicationHaPersistenceOwnerStatus {
                    node: owner.node.client_addr.clone(),
                    role: owner.role.as_str().to_string(),
                    verified: owner.verified,
                })
                .collect(),
            last_handoff_ms: self.persistence_last_handoff_ms.load(Ordering::Relaxed),
            last_snapshot_ms: self.persistence_last_snapshot_ms.load(Ordering::Relaxed),
            last_error: self.persistence_last_error.read().await.clone(),
            protected_reason: self.persistence_protected_reason.read().await.clone(),
            handoff_count: self.persistence_handoff_count.load(Ordering::Relaxed),
            adopt_count: self.persistence_adopt_count.load(Ordering::Relaxed),
            enable_count: self.persistence_enable_count.load(Ordering::Relaxed),
            enable_failure_count: self
                .persistence_enable_failure_count
                .load(Ordering::Relaxed),
            disable_count: self.persistence_disable_count.load(Ordering::Relaxed),
            disable_failure_count: self
                .persistence_disable_failure_count
                .load(Ordering::Relaxed),
            snapshot_count: self.persistence_snapshot_count.load(Ordering::Relaxed),
            snapshot_failure_count: self
                .persistence_snapshot_failure_count
                .load(Ordering::Relaxed),
            verify_count: self.persistence_verify_count.load(Ordering::Relaxed),
            verify_failure_count: self
                .persistence_verify_failure_count
                .load(Ordering::Relaxed),
            protection_count: self.persistence_protection_count.load(Ordering::Relaxed),
            last_verify_duration_micros: self
                .persistence_last_verify_duration_micros
                .load(Ordering::Relaxed),
        }
    }
}
