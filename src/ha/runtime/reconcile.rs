impl ReplicationHaRuntime {
    /// 业务作用: 周期驱动 replication-ha 探测与收敛，并在关闭或运行代退休时立即停止控制任务。
    /// 参数说明:
    /// - `shutdown`: 关闭信号通道，用于停止监听并通知所属任务收尾。
    ///
    // 路由权威监视必须与可能阻塞的后端收敛并行，避免慢探测延后失权断路。
    pub async fn watch(self: Arc<Self>, shutdown: watch::Receiver<bool>) {
        // 拓扑收敛可能等待探测、隔离或持久化命令，不能让数据路由权威检查排在同一个
        // 串行循环后面。两个循环共享 routing_tx：独立监视器负责按时关路由，收敛循环
        // 负责后端控制；任一慢操作都不会阻塞另一个循环。
        let authority_runtime = self.clone();
        tokio::join!(
            self.watch_reconcile_loop(shutdown.clone()),
            authority_runtime.watch_cluster_route_authority(shutdown),
        );
    }

    /// 业务作用: 周期执行 replication HA 拓扑和持久化收敛；外部动作通过 `failover_lock` 保持进程内串行。
    /// 参数说明:
    /// - `shutdown`: 进程关闭信号。
    ///
    /// 返回: 收到关闭信号后结束；单轮错误写入状态并在下一周期重试。
    async fn watch_reconcile_loop(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(self.probe_interval_ms));
        // HA 连败和受限接管都把“连续轮次”当作稳定性证据。默认 Burst 会在任务
        // 曾被慢探测或控制动作阻塞后瞬间补跑积压 tick，把多轮宽限压缩成几次
        // 紧邻探测；Delay 保证错过的周期不会被追赶执行。
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    // 关闭信号必须能取消在途收敛:切代屏障会等待本任务退出,不能让一次慢控制
                    // 动作(长探测、复制验证、持久化交接)把发布窗口拉长到动作完成。取消点落在
                    // 任意 await 边界,半程外部动作等价于进程崩溃,由租约/动作守卫的 Drop 清理
                    // 并交给下一代按幂等收敛恢复;这也阻止旧代在退出后继续改写后端。
                    let cancelled = tokio::select! {
                        result = self.reconcile() => {
                            match result {
                                Ok(()) => {
                                    let serving = matches!(
                                        &*self.routing_tx.borrow(),
                                        HaRoutingState::Serving { .. }
                                    );
                                    if serving {
                                        self.set_last_error(None).await;
                                        self.reconcile_warning_not_before_ms
                                            .store(0, Ordering::Relaxed);
                                    }
                                }
                                Err(err) => {
                                    let error = err.to_string();
                                    self.set_last_error(Some(error.clone())).await;
                                    let now = now_ms();
                                    let not_before = self
                                        .reconcile_warning_not_before_ms
                                        .load(Ordering::Relaxed);
                                    if now >= not_before
                                        && self
                                            .reconcile_warning_not_before_ms
                                            .compare_exchange(
                                                not_before,
                                                now.saturating_add(RECONCILE_WARNING_INTERVAL_MS),
                                                Ordering::Relaxed,
                                                Ordering::Relaxed,
                                            )
                                            .is_ok()
                                    {
                                        warn!(proxy = %self.name, error = %error, "replication-ha reconcile failed");
                                    }
                                }
                            }
                            false
                        }
                        changed = shutdown.changed() => {
                            changed.is_err() || *shutdown.borrow()
                        }
                    };
                    if cancelled {
                        self.enter_protection(
                            "replication HA runtime generation is retiring".to_string(),
                        )
                        .await;
                        return;
                    }
                }
                _ = shutdown.changed() => {
                    // 与取消路径保持一致:退出前统一进入保护,状态接口不再依赖退出时机呈现不同路由态。
                    self.enter_protection(
                        "replication HA runtime generation is retiring".to_string(),
                    )
                    .await;
                    return;
                }
            }
        }
    }

    /// 业务作用: 独立监视进程级协调权威，避免慢探测或外部命令阻塞保护窗口到期后的数据面断路。
    /// 参数说明:
    /// - `shutdown`: 进程关闭信号。
    ///
    /// 返回: 非进程级协调模式立即结束；否则持续到收到关闭信号。
    async fn watch_cluster_route_authority(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let Some(cluster) = &self.cluster_coordinator else {
            return;
        };
        // 检查间隔最多 250 毫秒，同时至少把一个保护窗口切成十段。较短间隔降低
        // 到期到实际断路之间的调度误差，最小 25 毫秒避免异常小配置形成忙循环。
        let check_interval = (cluster.handle.route_guard_timeout() / 10)
            .max(Duration::from_millis(25))
            .min(Duration::from_millis(250));
        let mut ticker = tokio::time::interval(check_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !cluster.handle.route_authoritative().await {
                        let serving = matches!(
                            &*self.routing_tx.borrow(),
                            HaRoutingState::Serving { .. }
                        );
                        if serving {
                            self.enter_protection(
                                "replication HA routing protected because proxy-cluster coordinator authority expired"
                                    .to_string(),
                            )
                            .await;
                        }
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return;
                    }
                }
            }
        }
    }

    /// 业务作用: 执行 replication HA 控制面的单轮收敛，先发现当前拓扑，再按安全约束决定服务、保护或切换。
    /// 参数说明: 无。
    ///
    /// 返回: 权威全程有效且后端状态完成复验时返回成功；失权、证据不足或外部动作失败时返回错误。
    // failover_lock 覆盖整轮外部副作用，防止管理操作与后台收敛在同一运行代内并发改写拓扑。
    async fn reconcile(&self) -> anyhow::Result<()> {
        // 先取得配置事务之外的控制许可，再取本代理动作锁，保证事务排空与预提交不发生反向锁等待。
        let _control = self.acquire_regular_control().await?;
        let _operation = self.acquire_operation().await?;
        // 数据面是否继续开放先取决于协调权威，而不是后端恰好仍可连接。
        // 这样网络分区中的旧代理会在保护窗口到期后主动断路，不能长期发布旧主节点。
        self.ensure_cluster_route_authority().await?;
        let mut current = self.current_master.read().await.clone();
        let (masters, replicas) = self.discover_roles_with_replica_lag().await;
        if masters.is_empty() {
            // 已提交的新主可能在本节点完成路由跟随前立即失联。没有可探测主节点时，
            // 故障票仍须指向共识确认的主节点，否则不同成员会永久报告不同故障对象。
            // 同步只改变受保护的控制目标，不以共识记录代替后端健康和持久化验证。
            current = self.sync_unavailable_committed_master(current).await?;
        }
        if masters.len() == 1 {
            if let Some(cluster) = &self.cluster_coordinator {
                let committed_master = cluster
                    .handle
                    .group_state(&cluster.group_id)?
                    .and_then(|group| group.current_master);
                if committed_master.as_deref() != Some(masters[0].client_addr.as_str()) {
                    if committed_master.is_none() && masters[0].client_addr == current.client_addr {
                        // 冷启动待恢复收尾:状态机从未提交过主节点,而本代恢复目标已经
                        // 自行回归为唯一主。没有提交通道时该检查会每轮保护并提前返回,
                        // 失败计数与观察票都无法推进,形成永久保护死锁。由协调领导者当
                        // 场提交初始主,跟随者保持保护等待提交结果后按一致状态放行。
                        self.commit_recovered_initial_master(&masters[0]).await?;
                    } else {
                        let reason = format!(
                            "replication HA observed master `{}` differs from coordinator-committed master `{}`",
                            masters[0].client_addr,
                            committed_master.as_deref().unwrap_or("<unset>")
                        );
                        self.enter_protection(reason.clone()).await;
                        // 崩溃可能发生在“候选已提升、CompleteAction 尚未提交”之间。
                        // 只有本地 current 仍等于已提交旧主（或状态机尚无初始主）且
                        // 自动切主开启时，才允许继续累计故障票并通过标准动作令牌、
                        // fencing 和 CompleteAction 恢复；其它分叉保持保护，绝不直跟。
                        let committed_still_current = committed_master
                            .as_deref()
                            .is_none_or(|committed| committed == current.client_addr);
                        if !self.auto_failover || !committed_still_current {
                            bail!(reason);
                        }
                    }
                }
            }
        }
        // 每个 tick 都以真实节点角色为准，而不是只相信本地 current_master。
        // 这样能覆盖两类生产场景：外部控制面已经完成切主，或者旧 master
        // 重新上线后短暂呈现多 master。控制面先观测再动作，避免把尚未确认的
        // 本地缓存直接发布给数据面。
        if masters.len() > 1 {
            // 多主意味着不存在"唯一稳定接管者",受限跟随的连续观测被打断。
            self.reset_observed_takeover().await;
            // 所有运行档先记录多主事实再决定是否有权收敛。observer 虽然绝不
            // 写后端，也必须保留这份纯本地证据，防止额外 master 随后只对本代理失联。
            let observed_extra_masters = self
                .remember_unconfirmed_extra_masters(&current, &masters)
                .await;
            // current 仍是本代理路由目标时，多余 master 通常是回归中的旧节点。旧协调模式由租约
            // 持有者负责降级；进程级协调模式只有共识领导者能创建拓扑动作，其余成员先进入保护态，
            // 等待已提交结果后再恢复，不能在明确观察到双主时仅凭本地 current 继续开放入口。
            if masters
                .iter()
                .any(|master| master.client_addr == current.client_addr)
            {
                // 观察者对双主没有裁决权，也不能把“保持旧路由”误当成安全：
                // 两个节点都可写时继续 Serving 会把旁路流量留在已失去唯一性的主节点。
                // 因此只关闭本地路由并等待外部控制面收敛，绝不发出降级命令。
                if self.observer {
                    self.master_failure_streak.store(0, Ordering::Relaxed);
                    let reason = format!(
                        "replication HA observer detected multiple masters and waits for the managing control plane to converge them: {}",
                        format_node_addrs(&masters)
                    );
                    self.enter_protection(reason.clone()).await;
                    bail!(reason);
                }
                let multi_master_reason = format!(
                    "replication HA detected multiple writable masters and closed routing while converging them: {}",
                    format_node_addrs(&masters)
                );
                self.enter_protection(multi_master_reason).await;
                if self
                    .converge_to_recent_failover_outcome(
                        &current,
                        "observed failover outcome during multi-master reconcile",
                    )
                    .await?
                {
                    return Ok(());
                }
                if now_ms() < self.repair_retry_not_before_ms.load(Ordering::Relaxed) {
                    bail!(
                        "replication HA multi-master convergence remains protected during topology repair backoff"
                    );
                }
                let repair_started = Instant::now();
                let (repair_lease, mut cluster_action) =
                    if self.coordinator.mode == HaCoordinatorMode::ProxyCluster {
                        (
                            None,
                            Some(
                                self.acquire_cluster_manual_action(
                                    CoordinationActionKind::TopologyRepair,
                                    "multi-master-repair",
                                    current.client_addr.clone(),
                                    current.client_addr.clone(),
                                )
                                .await?,
                            ),
                        )
                    } else {
                        (
                            self.acquire_failover_lease("multi-master repair").await?,
                            None,
                        )
                    };
                if let Some(lease) = repair_lease.as_ref() {
                    self.ensure_automatic_lease_active_or_protect(
                        Some(lease),
                        "before multi-master convergence",
                    )
                    .await?;
                }
                if let Some(action) = cluster_action.as_ref() {
                    self.ensure_cluster_action_or_protect(
                        action,
                        "before multi-master convergence",
                    )
                    .await?;
                }
                let demote_result = self
                    .demote_extra_masters_checked(
                        &current,
                        repair_lease.as_ref(),
                        cluster_action.as_ref(),
                        Some(&observed_extra_masters),
                    )
                    .await;
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
                    self.repair_retry_not_before_ms
                        .store(now_ms().saturating_add(retry_delay_ms), Ordering::Relaxed);
                    self.repair_failure_count.fetch_add(1, Ordering::Relaxed);
                }
                if demote_result.is_ok() {
                    if let Some(action) = cluster_action.as_mut() {
                        self.ensure_cluster_action_or_protect(
                            action,
                            "before committing multi-master convergence",
                        )
                        .await?;
                        self.complete_cluster_action_or_protect(
                            action,
                            &current.client_addr,
                            "after multi-master convergence",
                        )
                        .await?;
                    }
                } else if let Err(error) = &demote_result {
                    self.enter_protection(format!(
                        "replication HA multi-master convergence failed and routing remains closed: {error}"
                    ))
                    .await;
                }
                self.master_failure_streak.store(0, Ordering::Relaxed);
                if demote_result.is_ok() {
                    self.ensure_serving(current.clone()).await;
                }
                demote_result.with_context(|| {
                    format!(
                        "entered protection while demoting extra masters around `{}`",
                        current.client_addr
                    )
                })?;
                return Ok(());
            }
            let reason = format!(
                "multiple replication HA masters detected while the routed current master `{}` is absent: {}; routing stays protected; with fencing.mode=external-command, fence every permanently unavailable non-target through POST /ha/node/{{node}}/fence, then select one target with POST /ha/node/{{target}}/promote; manual fencing requires replica proof",
                current.client_addr,
                format_node_addrs(&masters),
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }

        if masters.len() == 1 && masters[0].client_addr != current.client_addr {
            let observed_master = masters[0].clone();
            // 未加入内置协调集群时，只有两类证据足以绕过主动 failover：旧主已经
            // 明确成为新主的副本，或外部租约 owner 已发布仍有效的切主结果。仅仅
            // “旧主本轮不可达、另一个节点自称 master”不能证明旧主失去写能力，
            // 否则一次探测抖动或未协调的外部提升就能绕过连败、fencing 和租约门禁。
            if self.cluster_coordinator.is_none()
                && old_master_follows_new(&replicas, &current, &observed_master)
            {
                self.finish_master_switch(
                    current.client_addr,
                    observed_master,
                    Instant::now(),
                    "observed converged failover",
                )
                .await?;
                return Ok(());
            }
            if self
                .converge_to_recent_failover_outcome(
                    &current,
                    "coordinator-confirmed observed failover",
                )
                .await?
            {
                return Ok(());
            }
            // 证据不足:记录这一轮"唯一接管者"观测。旧主永久死亡时它永远无法作证,
            // 受限跟随通道以"旧主连败确认宕机 + 同一接管者连续稳定同样轮数"作为
            // 等价证据,在下方故障判定达标后跟随,避免 observer/关闭自动切主的节点
            // 陷入只能重启才能恢复的永久保护。
            self.note_observed_takeover(&observed_master).await;
            // 继续进入下面的 current-master 故障判定；达到阈值后由受限跟随、
            // observer/禁用自动切主保护分支或标准自动 failover 流程处理。
        }

        if masters.len() == 1 && masters[0].client_addr == current.client_addr {
            self.master_failure_streak.store(0, Ordering::Relaxed);
            self.automatic_retry_not_before_ms
                .store(0, Ordering::Relaxed);
            self.reset_observed_takeover().await;
            // 之前明确看到过的额外 master 必须重新证明为 replica；节点本轮缺席
            // 不能作为降级证据，否则不对称分区会把真实双主隐藏成“唯一主”。
            self.ensure_no_unconfirmed_extra_masters(&current, &replicas)
                .await?;
            if self
                .repair_replica_upstream_drift(&current, &replicas)
                .await?
            {
                bail!(
                    "replication HA replica upstream topology was repaired and awaits a fresh probe"
                );
            }
            // 先 reconcile 持久化：若 fail_closed 下持久化异常，reconcile_persistence 会 enter_protection 并 bail，
            // 从而跳过下面的 ensure_serving，让保护状态「粘住」；否则(持久化正常或 policy=none)再开放业务路由。
            // 反过来先 ensure_serving 会在每个 tick 把 Protected 翻回 Serving，使 fail_closed 形同虚设。
            self.reconcile_persistence(&masters, &replicas).await?;
            self.ensure_serving(current).await;
            return Ok(());
        }

        if masters.is_empty() {
            // 本轮连唯一接管者都不可见,受限跟随的"连续稳定"观测中断。
            self.reset_observed_takeover().await;
        }
        let failures = self.master_failure_streak.fetch_add(1, Ordering::Relaxed) + 1;
        if failures < self.master_down_after_failures {
            bail!(
                "replication HA current master `{}` is not healthy, failure {}/{}",
                current.client_addr,
                failures,
                self.master_down_after_failures
            );
        }
        // 受限跟随通道:旧主已按连败阈值确认宕机,且同一接管者连续稳定了同样轮数。
        // 相比构造期发现的"单轮采信唯一主",这里要求两个独立的连续确认,语义更严格;
        // 仅用于不主动改写拓扑的运行档(observer 或 auto_failover=false)——自动切主档
        // 拥有 fencing+promote 全流程,不需要也不应走无隔离的跟随捷径。
        if self.cluster_coordinator.is_none() && (self.observer || !self.auto_failover) {
            if let Some(takeover_addr) = self.confirmed_takeover().await {
                let takeover = self.find_node(&takeover_addr)?;
                self.finish_master_switch(
                    current.client_addr.clone(),
                    takeover,
                    Instant::now(),
                    "confirmed takeover after master loss",
                )
                .await?;
                return Ok(());
            }
        }
        if self.observer {
            let reason = format!(
                "replication HA observer confirmed master `{}` down after {} consecutive failures and waits for the managing control plane to switch",
                current.client_addr, self.master_down_after_failures
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        if !self.auto_failover {
            let reason = format!(
                "replication HA master `{}` is confirmed down after {} consecutive failures, but ha.auto_failover=false",
                current.client_addr, self.master_down_after_failures
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        // 达到故障阈值后先关闭数据面，再等待协调租约、fencing 和 promotion。
        // 租约争用、候选不足或退避都不能让旧 Serving 状态在控制面等待期间残留。
        if matches!(&*self.routing_tx.borrow(), HaRoutingState::Serving { .. }) {
            self.enter_protection(format!(
                "replication HA master `{}` is confirmed down; automatic failover is pending",
                current.client_addr
            ))
            .await;
        }
        let retry_not_before_ms = self.automatic_retry_not_before_ms.load(Ordering::Relaxed);
        if now_ms() < retry_not_before_ms {
            // 保护态已经对业务关闭路由；退避窗口内保持现状即可。这里返回成功，
            // 避免每轮探测重复打印同一条 reconcile 失败日志或触发外部隔离命令。
            return Ok(());
        }
        // 自动 failover 必须先拿协调租约。没有租约时不做 fencing 或 promote，
        // 因为多 proxy 同时操作同一组源会把一次故障放大成双主或反复切换。
        let (lease_guard, mut cluster_action) = if self.coordinator.mode
            == HaCoordinatorMode::ProxyCluster
        {
            let observed_master = masters
                .iter()
                .find(|master| master.client_addr != current.client_addr);
            match self
                .acquire_cluster_failover_action(&current, replicas.clone(), observed_master)
                .await
            {
                Ok(action) => (None, Some(action)),
                Err(error) => {
                    let reason = format!(
                        "replication HA automatic failover waits for proxy-cluster approval: {error}"
                    );
                    self.enter_protection(reason.clone()).await;
                    bail!(reason);
                }
            }
        } else {
            let lease = match self.acquire_automatic_failover_lease(&current).await {
                Ok(AutomaticLeaseOutcome::Acquired(lease)) => lease,
                Ok(AutomaticLeaseOutcome::AlreadyConverged) => return Ok(()),
                Ok(AutomaticLeaseOutcome::Busy { reason }) => {
                    bail!("replication HA automatic failover deferred because coordinator lease is busy: {reason}");
                }
                Err(err) => {
                    let reason = format!(
                            "replication HA automatic failover blocked because coordinator lease failed: {err}"
                        );
                    self.enter_protection(reason.clone()).await;
                    bail!(reason);
                }
            };
            (lease, None)
        };
        self.ensure_automatic_authority_or_protect(
            lease_guard.as_ref(),
            cluster_action.as_ref(),
            "before fencing",
        )
        .await?;
        let replicas = {
            // 拿到租约后再次探测。这个复验窗口用于处理竞态：其它控制面可能
            // 已经完成切主，或者原 master 在租约等待期间恢复。若新主已经出现，
            // 旧主状态未知时仍须隔离；旧主已明确降级时可跳过隔离，但仍须复验权威。
            let (masters, replicas) = self.discover_roles_with_replica_lag().await;
            if masters.len() > 1 {
                let reason = format!(
                    "multiple replication HA masters detected after acquiring failover lease: {}",
                    format_node_addrs(&masters)
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
            if masters.len() == 1 && masters[0].client_addr != current.client_addr {
                if let Some(action) = cluster_action.as_mut() {
                    if masters[0].client_addr != action.target_master {
                        let reason = format!(
                            "replication HA observed master `{}` differs from coordinator-approved target `{}`",
                            masters[0].client_addr, action.target_master
                        );
                        self.enter_protection(reason.clone()).await;
                        bail!(reason);
                    }
                    self.ensure_cluster_action_or_protect(
                        action,
                        "before fencing an already changed master",
                    )
                    .await?;
                }
                // 取得动作权威后的复验即使已经看到新主，也必须先隔离旧主——但仅当
                // 旧主状态未知或仍可能持有写身份。副本证词表明旧主已在租约等待期间
                // 恢复并直接跟随新主时，它已完成降级；此时再执行 external-command
                // 只会无谓关停一台健康副本，因此跳过隔离直接收尾。
                if old_master_follows_new(&replicas, &current, &masters[0]) {
                    info!(
                        proxy = %self.name,
                        old_master = %current.client_addr,
                        new_master = %masters[0].client_addr,
                        "skipping old master fencing because it already follows the new master"
                    );
                } else {
                    let result = self
                        .fence_node_inner_with_token(
                            &current,
                            "observed failover old master fencing",
                            lease_guard.as_ref(),
                            cluster_action.as_ref(),
                        )
                        .await;
                    self.protect_ha_control_step(result, "while fencing an already changed master")
                        .await?;
                }
                // 复验发生在可能较慢的一整轮后端探测之后；无论是否因降级证词
                // 跳过 fencing，都必须确认 local-file/Redis 租约或集群动作仍有效，
                // 才能发布新路由和提交协调结果。
                self.ensure_automatic_authority_or_protect(
                    lease_guard.as_ref(),
                    cluster_action.as_ref(),
                    "after revalidating an already changed master",
                )
                .await?;
                let result = self
                    .demote_extra_masters_checked(
                        &masters[0],
                        lease_guard.as_ref(),
                        cluster_action.as_ref(),
                        None,
                    )
                    .await;
                if result.is_err() {
                    // 旧主可能已经被成功隔离；若其它历史风险节点仍不可达，demote
                    // 会按设计失败。此时必须退避整个自动动作，不能每个 tick 再次
                    // 调用同一 external fencing 云 API。
                    self.defer_automatic_retry();
                }
                self.protect_ha_control_step(result, "while converging an already changed master")
                    .await?;
                self.ensure_automatic_authority_or_protect(
                    lease_guard.as_ref(),
                    cluster_action.as_ref(),
                    "after converging an already changed master",
                )
                .await?;
                if let Some(action) = cluster_action.as_mut() {
                    self.ensure_cluster_action_or_protect(
                        action,
                        "before committing an already changed master",
                    )
                    .await?;
                    self.complete_cluster_action_or_protect(
                        action,
                        &masters[0].client_addr,
                        "after observing an already changed master",
                    )
                    .await?;
                }
                if let Some(lease) = lease_guard.as_ref() {
                    let result = self.record_failover_outcome(lease, &masters[0]).await;
                    self.protect_ha_control_step(
                        result,
                        "while publishing an already changed master outcome",
                    )
                    .await?;
                }
                self.finish_master_switch(
                    current.client_addr.clone(),
                    masters[0].clone(),
                    Instant::now(),
                    "observed failover after lease",
                )
                .await?;
                return Ok(());
            }
            if masters.len() == 1 && masters[0].client_addr == current.client_addr {
                self.master_failure_streak.store(0, Ordering::Relaxed);
                self.ensure_serving(current).await;
                return Ok(());
            }
            // 上面的分支已经覆盖多主、唯一旧主和唯一新主；只有零主观测能够
            // 到达候选提升阶段，避免死分支掩盖真正的 failover 前置条件。
            replicas
        };
        self.ensure_automatic_authority_or_protect(
            lease_guard.as_ref(),
            cluster_action.as_ref(),
            "before fencing command",
        )
        .await?;
        // fencing 放在 promote 前，目的是先切断旧 master 的写入能力，再让新
        // master 对外服务。若 fencing 失败，宁可进入保护态，也不允许业务同时
        // 写入两个 master。
        if let Err(err) = self
            .fence_node_inner_with_token(
                &current,
                "automatic failover old master fencing",
                lease_guard.as_ref(),
                cluster_action.as_ref(),
            )
            .await
        {
            self.defer_automatic_retry();
            let reason = format!(
                "replication HA automatic failover blocked because fencing `{}` failed: {err}",
                current.client_addr
            );
            self.enter_protection(reason.clone()).await;
            bail!(reason);
        }
        self.ensure_automatic_authority_or_protect(
            lease_guard.as_ref(),
            cluster_action.as_ref(),
            "after fencing command",
        )
        .await?;
        let failover_started = Instant::now();
        let master = {
            // 主节点已确认宕机却选不出候选时必须关闭数据面：继续 Serving 会把业务
            // 流量黑洞在死主上,状态接口还呈现为正常服务。保护态让运维立即看到
            // 自动切主关闭时只做保护；运维可收敛副本，或通过节点级 promote 接口显式选择目标。
            // proxy-cluster 动作已经把目标写入共识令牌；复验后目标不再是 master
            // 时只能继续评估同一节点，不能先提升另一个本地最优副本、再在事后
            // 发现与令牌不一致。目标缺席或不合格直接保护。
            let eligible_replicas = if let Some(action) = cluster_action.as_ref() {
                replicas
                    .into_iter()
                    .filter(|replica| replica.node.client_addr == action.target_master)
                    .collect()
            } else {
                replicas
            };
            let candidate = match self
                .select_failover_candidate(eligible_replicas, Some(&current), "automatic failover")
                .await
            {
                Ok(candidate) => candidate,
                Err(error) => {
                    let reason = format!(
                        "replication HA master `{}` is down but no replica is eligible for automatic failover: {error}",
                        current.client_addr
                    );
                    self.enter_protection(reason.clone()).await;
                    bail!(reason);
                }
            };
            let barrier_waited = self
                .wait_soft_fencing_route_barrier(
                    lease_guard.as_ref(),
                    cluster_action.as_ref(),
                    "after the soft fencing route barrier",
                )
                .await?;
            // 围栏真实等待过时旧主可能已在窗口内恢复；此时放弃提主，按原主继续服务,
            // 未完成的协调动作由守卫释放时提交失败结果。未等待的路径(单实例或外部
            // 隔离)紧接上一轮复验探测,不重复对死主发起探测拖慢切主。
            if barrier_waited && self.node_role(&current).await? == HaRole::Master {
                self.master_failure_streak.store(0, Ordering::Relaxed);
                self.ensure_serving(current).await;
                return Ok(());
            }
            if let Err(error) = self
                .promote(&candidate, lease_guard.as_ref(), cluster_action.as_ref())
                .await
            {
                let reason = format!(
                    "replication HA automatic failover promotion of `{}` failed after fencing: {error}",
                    candidate.client_addr
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
            self.ensure_automatic_authority_or_protect(
                lease_guard.as_ref(),
                cluster_action.as_ref(),
                "after promotion",
            )
            .await?;
            candidate
        };
        if let Some(action) = cluster_action.as_ref() {
            if master.client_addr != action.target_master {
                let reason = format!(
                    "replication HA promoted master `{}` differs from coordinator-approved target `{}`",
                    master.client_addr, action.target_master
                );
                self.enter_protection(reason.clone()).await;
                bail!(reason);
            }
        }
        self.ensure_automatic_authority_or_protect(
            lease_guard.as_ref(),
            cluster_action.as_ref(),
            "before automatic convergence",
        )
        .await?;
        let result = self
            .demote_extra_masters_checked(
                &master,
                lease_guard.as_ref(),
                cluster_action.as_ref(),
                None,
            )
            .await;
        if result.is_err() {
            // promotion 已完成但风险节点未全部收口时，下一轮会进入“已观测新主”
            // 恢复路径；提前设置同一退避，避免重复隔离旧主和重复提交控制动作。
            self.defer_automatic_retry();
        }
        self.protect_ha_control_step(result, "during automatic convergence")
            .await?;
        self.ensure_automatic_authority_or_protect(
            lease_guard.as_ref(),
            cluster_action.as_ref(),
            "after automatic convergence",
        )
        .await?;
        if let Some(action) = cluster_action.as_mut() {
            self.ensure_cluster_action_or_protect(action, "before committing automatic failover")
                .await?;
            self.complete_cluster_action_or_protect(
                action,
                &master.client_addr,
                "after automatic failover",
            )
            .await?;
        }
        if let Some(lease) = lease_guard.as_ref() {
            let result = self.record_failover_outcome(lease, &master).await;
            self.protect_ha_control_step(result, "while publishing automatic failover outcome")
                .await?;
        }
        let old = self.current_master_addr().await;
        self.finish_master_switch(old, master, failover_started, "automatic failover")
            .await?;
        Ok(())
    }
}
