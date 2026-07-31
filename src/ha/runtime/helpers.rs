impl ReplicationHaRuntime {
    /// 业务作用: 将当前运行代标记为已退休；置位后所有新的后端控制副作用都会被拒绝。
    pub(crate) fn mark_retired(&self) {
        self.generation_guard.mark_retired();
    }

    /// 业务作用: 返回当前运行代是否已经退出活动槽位。
    fn is_retired(&self) -> bool {
        self.generation_guard.is_retired()
    }

    /// 业务作用: 校验当前运行代尚未退休，退休后拒绝继续执行外部控制步骤。
    ///
    /// 参数说明:
    /// - `step`: 即将执行的控制步骤名称，用于错误定位。
    fn ensure_not_retired(&self, step: &str) -> anyhow::Result<()> {
        self.generation_guard.ensure_active(step)
    }

    /// 业务作用: 记录一次"唯一新主可见但收敛证据不足"的观测，为受限跟随通道积累轮数。
    /// 参数说明:
    /// - `observed`: 本轮观测到的唯一非当前主节点。
    ///
    /// 返回: 该接管者连续稳定的观测轮数；换人时从 1 重新计数。
    async fn note_observed_takeover(&self, observed: &HaNode) -> u64 {
        let mut state = self.observed_takeover.write().await;
        if state.client_addr == observed.client_addr {
            state.stable_rounds = state.stable_rounds.saturating_add(1);
        } else {
            state.client_addr = observed.client_addr.clone();
            state.stable_rounds = 1;
        }
        state.stable_rounds
    }

    /// 业务作用: 清空接管观测轮数；拓扑回到当前主、无主、多主或完成任何切换后调用，
    /// 保证受限跟随只统计"连续"稳定观测。
    async fn reset_observed_takeover(&self) {
        let mut state = self.observed_takeover.write().await;
        state.client_addr.clear();
        state.stable_rounds = 0;
    }

    /// 业务作用: 读取已达到稳定门槛（`master_down_after_failures` 轮）的接管者地址。
    ///
    /// 返回: 达标时返回接管者业务地址；未达标返回空。
    async fn confirmed_takeover(&self) -> Option<String> {
        let state = self.observed_takeover.read().await;
        if !state.client_addr.is_empty() && state.stable_rounds >= self.master_down_after_failures {
            Some(state.client_addr.clone())
        } else {
            None
        }
    }

    /// 业务作用: 为自动切主的外部控制失败设置统一退避，避免每个探测 tick 重复 fencing 或拓扑写入。
    fn defer_automatic_retry(&self) {
        let retry_delay_ms = self
            .coordinator
            .lease_ttl_ms
            .max(self.probe_interval_ms.saturating_mul(10));
        self.automatic_retry_not_before_ms.store(
            now_ms().saturating_add(retry_delay_ms),
            Ordering::Relaxed,
        );
    }

    /// 业务作用: 校验当前运行时不是观察者，观察者统一拒绝一切拓扑改写入口。
    ///
    /// 这组后端由另一套 NARIX 控制面管理；从观察节点发出的任何切主、降级或
    /// 隔离都会绕过那套控制面的协调与互斥，因此在入口处 fail-closed。
    ///
    /// 参数说明:
    /// - `operation`: 被拒绝的操作名称，用于错误定位。
    fn ensure_not_observer(&self, operation: &str) -> anyhow::Result<()> {
        if self.observer {
            bail!(
                "replication HA observer rejects {operation}; this proxy only watches and follows the topology managed by another control plane"
            );
        }
        Ok(())
    }
}

/// 业务作用: 生成包含主机、进程与随机信息的默认租约持有者标识，降低跨实例碰撞。
/// 参数说明:
/// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
///
fn default_coordinator_owner_id(proxy_name: &str) -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .filter(|host| !host.trim().is_empty())
        .unwrap_or_else(|| "unknown-host".to_string());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    format!("{proxy_name}:{host}:{}:{nonce}", std::process::id())
}

/// 业务作用: 把 replication HA 主从探活、受控切换、租约互斥和持久化编排 的内部状态渲染成外部命令、指标、文件内容或日志可读文本。
/// 参数说明:
/// - `nodes`: 节点集合，用于拓扑发现、候选选择、批量验证或测试环境清理。
///
// 格式化 format_node_addrs 输出，用于对外呈现维护 replication HA 主从拓扑、探测状态和切主控制结果。
fn format_node_addrs(nodes: &[HaNode]) -> String {
    nodes
        .iter()
        .map(|node| node.client_addr.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

/// 业务作用: 把 replication HA 主从探活、受控切换、租约互斥和持久化编排 的内部状态渲染成外部命令、指标、文件内容或日志可读文本。
/// 参数说明:
/// - `template`: 包含受支持占位符的命令或文件名模板，展开前必须校验未知占位符。
/// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
/// - `node`: Redis 或 replication HA 节点描述，提供客户端地址、复制地址和角色判断依据。
/// - `action_token`: 进程级协调器批准的动作令牌；未使用集群协调时为空字符串。
///
// 渲染 fencing command 参数模板，避免通过 shell 拼接命令。
fn render_fence_template(
    template: &str,
    proxy_name: &str,
    node: &HaNode,
    action_token: Option<&str>,
) -> String {
    template
        .replace("{proxy}", proxy_name)
        .replace("{client_addr}", &node.client_addr)
        .replace("{replication_addr}", &node.replication_addr)
        .replace("{action_token}", action_token.unwrap_or_default())
}

/// 业务作用: 截断外部命令输出，避免错误响应/日志被大输出撑爆。
/// 参数说明:
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
// 截断外部命令输出，避免错误响应/日志被大输出撑爆。
fn short_output(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    text.chars().take(512).collect::<String>()
}

/// 业务作用: 从 replication HA 主从探活、受控切换、租约互斥和持久化编排 的候选集合中选择最安全的目标，避免在关键路径中依赖调用方猜测。
/// 参数说明:
/// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
/// - `purpose`: 本次租约、连接或动作的业务目的，用于构造唯一键和可观测标签。
/// - `max_replica_lag_seconds`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
///
/// 返回: 地址或候选门禁唯一命中时返回 HA 节点；缺失、歧义或无合格候选时返回错误。
// 计算 select_replica_candidate 路由规则，用于决定维护 replication HA 主从拓扑、探测状态和切主控制的执行路径。
fn select_replica_candidate(
    replicas: Vec<ReplicaCandidate>,
    purpose: &str,
    max_replica_lag_seconds: u64,
) -> anyhow::Result<HaNode> {
    if replicas.is_empty() {
        bail!("no healthy HA replica found for {purpose}");
    }
    if let Some(candidate) = replicas
        .iter()
        .filter(|candidate| replica_failover_allowed(candidate, max_replica_lag_seconds))
        .min_by_key(|candidate| candidate_failover_lag(candidate).unwrap_or(u64::MAX))
    {
        return Ok(candidate.node.clone());
    }

    bail!(
        "no HA replica found for {purpose} within max_replica_lag_seconds={max_replica_lag_seconds}; candidates: {}",
        format_replica_candidates(&replicas)
    )
}

/// 业务作用: 计算副本参与切主候选时的有效延迟；断链副本按断链时长参选，在线副本按真实延迟参选。
/// 参数说明:
/// - `candidate`: 本轮探测到的副本候选。
///
/// 返回: 可用于门禁和最小延迟排序的有效延迟；证据不足时为空。
fn candidate_failover_lag(candidate: &ReplicaCandidate) -> Option<u64> {
    replica_failover_lag_seconds(candidate.lag_seconds, candidate.link_down_seconds)
}

/// 业务作用: 判断副本是否可参与切主候选。主节点宕机后副本必然断链，断链时长是复制延迟的
/// 可证明下界，因此断链副本按断链时长通过同一套 `max_replica_lag` 门禁；全量同步中或
/// 断链时长不可知的副本保持不可选。持久化 owner 门禁不使用本函数。
/// 参数说明:
/// - `candidate`: 本轮探测到的副本候选。
/// - `max_replica_lag_seconds`: 允许的最大已知有效延迟；零表示不设数值上限。
///
fn replica_failover_allowed(candidate: &ReplicaCandidate, max_replica_lag_seconds: u64) -> bool {
    replica_lag_is_eligible(candidate_failover_lag(candidate), max_replica_lag_seconds)
}

/// 业务作用: 按在线复制语义判断副本延迟是否合格；断链副本一律不合格。该门禁仅用于
/// 持久化 owner 与热备等要求链路在线的角色，切主候选应使用 `replica_failover_allowed`。
/// 参数说明:
/// - `candidate`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
/// - `max_replica_lag_seconds`: 允许的最大已知在线延迟；零表示不设数值上限。
///
fn replica_lag_allowed(candidate: &ReplicaCandidate, max_replica_lag_seconds: u64) -> bool {
    replica_lag_is_eligible(candidate.lag_seconds, max_replica_lag_seconds)
}

/// 业务作用: 按未知、全量同步、在线状态和零延迟策略判断副本能否承担持久化责任。
/// 参数说明:
/// - `candidate`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
/// - `require_zero_lag`: 是否要求复制延迟严格为零，决定候选节点能否承担持久化角色。
/// - `max_replica_lag_seconds`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
///
fn persistence_replica_lag_allowed(
    candidate: &ReplicaCandidate,
    require_zero_lag: bool,
    max_replica_lag_seconds: u64,
) -> bool {
    if require_zero_lag {
        return candidate.lag_seconds == Some(0);
    }
    replica_lag_allowed(candidate, max_replica_lag_seconds)
}

/// 业务作用: 判断副本是否同时满足目标上游和持久化延迟门禁，稳定计时不能跨错误上游沿用。
///
/// 参数说明:
/// - `candidate`: 本轮探测到的副本角色、上游和延迟。
/// - `upstream`: 当前唯一主节点，候选必须明确跟随该节点。
/// - `require_zero_lag`: 是否要求归一化复制延迟严格为零。
/// - `max_replica_lag_seconds`: 未要求零延迟时允许的最大秒数；零表示不设数值上限。
///
/// 返回: 上游匹配且延迟证据满足门禁时返回真。
fn persistence_replica_eligible_for_upstream(
    candidate: &ReplicaCandidate,
    upstream: &HaNode,
    require_zero_lag: bool,
    max_replica_lag_seconds: u64,
) -> bool {
    replica_candidate_follows_node(candidate, upstream)
        && persistence_replica_lag_allowed(candidate, require_zero_lag, max_replica_lag_seconds)
}

/// 业务作用: 判断副本证词是否证明指定旧主已经直接跟随新主，即旧主已完成降级、失去独立写入身份。
/// 参数说明:
/// - `replicas`: 本轮真实探测到的副本观测。
/// - `old_master`: 需要证明已降级的旧主节点。
/// - `new_master`: 期望旧主直接跟随的新主节点。
///
/// 返回: 证词成立时返回真；旧主不可达、角色未知或跟随其它上游时返回假。
fn old_master_follows_new(
    replicas: &[ReplicaCandidate],
    old_master: &HaNode,
    new_master: &HaNode,
) -> bool {
    replicas.iter().any(|replica| {
        replica.node.client_addr == old_master.client_addr
            && replica_candidate_follows_node(replica, new_master)
    })
}

/// 业务作用: 确认候选副本当前直接跟随指定节点的复制地址。
/// 参数说明:
/// - `candidate`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
/// - `upstream`: 复制链路的上游节点地址，用于验证 replica 跟随关系。
///
fn replica_candidate_follows_node(candidate: &ReplicaCandidate, upstream: &HaNode) -> bool {
    candidate.master_addr.as_ref().is_some_and(|master_addr| {
        master_addr == &upstream.replication_addr || master_addr == &upstream.client_addr
    })
}

/// 业务作用: 为已经通过稳定窗口的持久化候选提供跨进程确定性排序。
/// 本地首次观测时间不得进入排序键；相同复制延迟时使用稳定客户端地址裁决。
fn compare_persistence_candidate_rank(
    left: &(ReplicaCandidate, u64),
    right: &(ReplicaCandidate, u64),
) -> std::cmp::Ordering {
    (left.1, left.0.node.client_addr.as_str())
        .cmp(&(right.1, right.0.node.client_addr.as_str()))
}

/// 业务作用: 共享 failover outcome 只能在目标是本轮唯一健康主节点时作为路由收敛证据。
/// 仍存在旧主或第三个主节点时必须先保持保护并完成双主修复，不能仅凭一个仍在
/// TTL 内的历史结果重新开放数据面。
fn failover_outcome_matches_unique_master(masters: &[HaNode], outcome_master: &str) -> bool {
    matches!(masters, [master] if master.client_addr == outcome_master)
}

/// 业务作用: 判断上一版持久化拓扑是否至少保留一个已验证快照 owner。
/// 参数说明:
/// - `topology`: 当前观测到的节点拓扑，用于计算 master、热备和快照 owner。
///
fn previous_topology_has_verified_owner(topology: &PersistenceTopologyState) -> bool {
    topology.snapshot_owners.iter().any(|owner| owner.verified)
}

/// 业务作用: 比较期望拓扑与已验证状态的 owner、热备和角色是否完全一致。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `desired`: 根据健康状态计算出的目标拓扑或持久化角色集合，用于与现状做差异协调。
///
fn persistence_topology_state_matches(
    state: &PersistenceTopologyState,
    desired: &DesiredPersistenceTopology,
) -> bool {
    if state.kind != desired.kind {
        return false;
    }
    if state
        .hot_standby
        .as_ref()
        .map(|node| node.client_addr.as_str())
        != desired
            .hot_standby
            .as_ref()
            .map(|node| node.client_addr.as_str())
    {
        return false;
    }
    if state.snapshot_owners.len() != desired.snapshot_owners.len() {
        return false;
    }
    desired.snapshot_owners.iter().all(|desired_owner| {
        state.snapshot_owners.iter().any(|owner| {
            owner.verified
                && owner.node.client_addr == desired_owner.node.client_addr
                && owner.role == desired_owner.role
        })
    })
}

/// 业务作用: 为完整持久化目标拓扑生成跨 proxy 稳定摘要，作为共识提交与远端采用的完整证据。
///
/// snapshot owner 是集合语义，副本链路也按节点地址排序后编码；因此不同进程
/// 即使本地向量构造顺序不同，只要热备、owner、角色和上游完全相同就会得到
/// 同一摘要。摘要进入 proxy-cluster 共识，防止只比较首 owner 时把旧提交误用
/// 到新增 owner、不同热备或不同复制链路。
/// 参数说明:
/// - `desired`: 已完成健康门禁和确定性候选选择的目标持久化拓扑。
///
/// 返回: 覆盖拓扑类型、热备、全部 owner 及复制链路的六十四位小写 SHA-256 摘要。
fn persistence_topology_fingerprint(desired: &DesiredPersistenceTopology) -> String {
    let mut hasher = Sha256::new();
    update_group_config_digest(
        &mut hasher,
        "topology_kind",
        desired.kind.as_str().as_bytes(),
    );
    let hot_standby = desired
        .hot_standby
        .as_ref()
        .map(|node| format!("{}|{}", node.client_addr, node.replication_addr))
        .unwrap_or_default();
    update_group_config_digest(&mut hasher, "hot_standby", hot_standby.as_bytes());

    // owner 与链路是集合语义；提交前排序，避免不同进程的向量构造顺序制造
    // 不同共识摘要，同时保留计数和长度边界防止字段拼接歧义。
    let mut owners = desired
        .snapshot_owners
        .iter()
        .map(|owner| {
            format!(
                "{}|{}|{}",
                owner.node.client_addr,
                owner.node.replication_addr,
                owner.role.as_str()
            )
        })
        .collect::<Vec<_>>();
    owners.sort();
    update_group_config_digest(
        &mut hasher,
        "snapshot_owner_count",
        owners.len().to_string().as_bytes(),
    );
    for owner in owners {
        update_group_config_digest(&mut hasher, "snapshot_owner", owner.as_bytes());
    }

    let mut links = desired
        .replica_links
        .iter()
        .map(|link| {
            format!(
                "{}|{}|{}|{}",
                link.node.client_addr,
                link.node.replication_addr,
                link.upstream.client_addr,
                link.upstream.replication_addr
            )
        })
        .collect::<Vec<_>>();
    links.sort();
    update_group_config_digest(
        &mut hasher,
        "replica_link_count",
        links.len().to_string().as_bytes(),
    );
    for link in links {
        update_group_config_digest(&mut hasher, "replica_link", link.as_bytes());
    }

    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用: 把 replication HA 主从探活、受控切换、租约互斥和持久化编排 的内部状态渲染成外部命令、指标、文件内容或日志可读文本。
/// 参数说明:
/// - `replicas`: 副本候选或副本状态，用于选择热备、快照 owner 或 failover 目标。
///
// 格式化 format_replica_candidates 输出，用于对外呈现维护 replication HA 主从拓扑、探测状态和切主控制结果。
fn format_replica_candidates(replicas: &[ReplicaCandidate]) -> String {
    replicas
        .iter()
        .map(|candidate| {
            let lag = candidate
                .lag_seconds
                .map(|lag| lag.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            match candidate.link_down_seconds {
                Some(down) => format!(
                    "{} lag={lag} link_down_since={down}s",
                    candidate.node.client_addr
                ),
                None => format!("{} lag={lag}", candidate.node.client_addr),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// 业务作用: 以固定字段顺序计算托管复制 HA 组关键配置摘要，确保不同代理成员不会用不同节点清单或控制策略操作同一协调组。
/// 参数说明:
/// - `item`: 已完成本地校验的托管复制 HA 代理项。
/// - `ha`: 当前代理项使用的中立 HA 配置。
///
/// 返回: 返回不泄露密码或命令参数原文的六十四位小写 SHA-256 摘要。
fn ha_group_config_digest(item: &ProxyItem, ha: &ReplicationHaConfig) -> String {
    let mut hasher = Sha256::new();
    let fields = [
        ("name", item.name.clone()),
        ("backend_profile", item.backend_profile.as_str().to_string()),
        ("nodes", item.nodes.clone().unwrap_or_default()),
        ("username", format!("{:?}", item.username)),
        ("password", format!("{:?}", item.password)),
        ("backend_tls_enabled", item.backend_tls_enabled.to_string()),
        (
            "backend_tls_ca_path",
            format!("{:?}", item.backend_tls_ca_path),
        ),
        (
            "backend_tls_server_name",
            format!("{:?}", item.backend_tls_server_name),
        ),
        (
            "backend_tls_certificate_path",
            format!("{:?}", item.backend_tls_certificate_path),
        ),
        (
            "backend_tls_private_key_path",
            format!("{:?}", item.backend_tls_private_key_path),
        ),
        ("connect_timeout_ms", item.connect_timeout_ms.to_string()),
        (
            "backend_request_timeout_ms",
            item.backend_request_timeout_ms.to_string(),
        ),
        ("ha_probe_interval_ms", ha.probe_interval_ms.to_string()),
        (
            "ha_master_down_after_failures",
            ha.master_down_after_failures.to_string(),
        ),
        ("ha_max_replica_lag", ha.max_replica_lag.to_string()),
        ("ha_auto_failover", ha.auto_failover.to_string()),
        ("ha_fencing_mode", ha.fencing.mode.as_str().to_string()),
        ("ha_fencing_command", format!("{:?}", ha.fencing.command)),
        ("ha_fencing_timeout_ms", ha.fencing.timeout_ms.to_string()),
        (
            "ha_coordinator_group_id",
            format!("{:?}", ha.coordinator.group_id),
        ),
        (
            "ha_coordinator_observation_quorum",
            ha.coordinator.observation_quorum.to_string(),
        ),
        (
            "ha_coordinator_observation_window_ms",
            ha.coordinator.observation_window_ms.to_string(),
        ),
        ("ha_persistence_enabled", ha.persistence.enabled.to_string()),
        (
            "ha_persistence_fail_closed",
            ha.persistence.fail_closed.to_string(),
        ),
        (
            "ha_persistence_force_checkpoint_on_handoff",
            ha.persistence.force_checkpoint_on_handoff.to_string(),
        ),
        (
            "ha_persistence_min_replica_stable_ms",
            ha.persistence.min_replica_stable_ms.to_string(),
        ),
        (
            "ha_persistence_require_zero_lag",
            ha.persistence.require_zero_lag.to_string(),
        ),
        (
            "ha_persistence_dragonfly",
            format!("{:?}", ha.persistence.dragonfly),
        ),
        (
            "ha_persistence_redis",
            format!("{:?}", ha.persistence.redis),
        ),
        (
            "ha_persistence_valkey",
            format!("{:?}", ha.persistence.valkey),
        ),
        (
            "ha_persistence_redict",
            format!("{:?}", ha.persistence.redict),
        ),
        (
            "ha_persistence_keydb",
            format!("{:?}", ha.persistence.keydb),
        ),
    ];
    for (name, value) in fields {
        update_group_config_digest(&mut hasher, name, value.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用: 以字段名和显式长度边界写入配置摘要，避免不同字段拼接后形成相同字节序列。
/// 参数说明:
/// - `hasher`: 正在累计 HA 组配置的 SHA-256 计算器。
/// - `name`: 当前字段的稳定名称。
/// - `value`: 当前字段的规范字节表示，可能包含敏感值但只进入摘要而不落盘或记录日志。
///
/// 返回: 无显式返回值；调用后摘要状态包含当前字段。
fn update_group_config_digest(hasher: &mut Sha256, name: &str, value: &[u8]) {
    hasher.update((name.len() as u64).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

/// 业务作用: 把操作开始时刻转换为饱和 u64 微秒耗时指标。
/// 参数说明:
/// - `started`: 操作开始的单调时钟时间点，用于计算耗时且不受系统时钟回拨影响。
///
fn elapsed_micros(started: Instant) -> u64 {
    started.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}
