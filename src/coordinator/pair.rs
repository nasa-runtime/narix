use crate::infrastructure::time::unix_nanos_u64 as now_unix_nanos;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Mutex, RwLock};
use tokio::time::Instant;
use tracing::{info, warn};

use super::network::CoordinatorNetworkFactory;
use super::state::{CoordinationCommand, CoordinationResponse};
use super::store::CoordinatorStateMachineStore;
use super::wire::{
    PairCampaignRequest, PairCampaignResponse, PairCommitRequest, PairCommitResponse,
    PairHeartbeatRequest, PairHeartbeatResponse, PairReplicateRequest, PairReplicateResponse,
    RpcEnvelope,
};
use super::wire_cluster_id;
use crate::config::ProxyClusterCoordinationConfig;

/// 业务作用: 表示双节点运行时当前可执行的角色，候选过程不会作为可写角色暴露给业务层。
/// 分支说明: 普通领导者依赖持续 2/2 通信；隔离领导者仅在外部确认对端已被 fencing 后存在。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairRole {
    Follower,
    Leader,
    FencedLeader,
}

/// 业务作用: 实现双节点选举、心跳、2/2 哈希链复制以及可选的对端隔离接管。
/// 字段说明: 所有长期安全状态由 `store` 持久化；角色和最后联系时间只是可丢失的运行时缓存。
#[derive(Debug)]
pub struct PairCoordinator {
    wire_cluster_id: Arc<str>,
    fencing_cluster_id: Arc<str>,
    local_node_id: u64,
    peer_node_id: u64,
    peer_address: String,
    startup_nonce: Arc<str>,
    heartbeat_interval: Duration,
    election_timeout: Duration,
    election_retry: Duration,
    route_guard_timeout: Duration,
    allow_single_survivor: bool,
    peer_fencing_command: Vec<String>,
    peer_fencing_timeout: Duration,
    store: CoordinatorStateMachineStore,
    network: CoordinatorNetworkFactory,
    role: RwLock<PairRole>,
    last_peer_contact: Mutex<Instant>,
}

impl PairCoordinator {
    /// 业务作用: 从静态双成员配置构造运行时，明确找出唯一对端并为本次进程启动生成不可复用实例标识。
    /// 参数说明:
    /// - `config`: 已通过双节点协议校验的进程级协调配置。
    /// - `store`: 保存任期、日志和业务状态的持久化存储。
    /// - `network`: 与共识协议共享成员身份和 TLS 规则的 gRPC 网络工厂。
    ///
    /// 返回: 返回尚未竞选的跟随者运行时；静态成员不包含唯一对端时返回错误。
    pub fn new(
        config: &ProxyClusterCoordinationConfig,
        store: CoordinatorStateMachineStore,
        network: CoordinatorNetworkFactory,
    ) -> anyhow::Result<Arc<Self>> {
        let peer = config
            .peers
            .iter()
            .find(|peer| peer.node_id != config.node_id)
            .context("pair coordination requires exactly one peer")?;
        Ok(Arc::new(Self {
            wire_cluster_id: Arc::from(wire_cluster_id(config)),
            fencing_cluster_id: Arc::from(config.cluster_id.as_str()),
            local_node_id: config.node_id,
            peer_node_id: peer.node_id,
            peer_address: peer.address.clone(),
            startup_nonce: Arc::from(startup_nonce(config.node_id)?.as_str()),
            heartbeat_interval: Duration::from_millis(config.heartbeat_interval_ms),
            election_timeout: Duration::from_millis(config.election_timeout_max_ms),
            election_retry: Duration::from_millis(config.pair_election_retry_ms),
            route_guard_timeout: Duration::from_millis(config.route_guard_timeout_ms),
            allow_single_survivor: config.pair_allow_single_survivor,
            peer_fencing_command: config.pair_peer_fencing_command.clone(),
            peer_fencing_timeout: Duration::from_millis(config.pair_peer_fencing_timeout_ms),
            store,
            network,
            role: RwLock::new(PairRole::Follower),
            last_peer_contact: Mutex::new(Instant::now()),
        }))
    }

    /// 业务作用: 运行双节点心跳与超时竞选循环；跟随者只在领导心跳过期后发起新任期。
    /// 参数说明:
    /// - `shutdown`: 进程关闭信号，收到后停止心跳和竞选。
    ///
    /// 返回: 无显式返回值；瞬时网络错误记录后继续下一轮，持久化错误不会授予领导权。
    pub async fn run(self: Arc<Self>, mut shutdown: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(self.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let role = *self.role.read().await;
                    match role {
                        PairRole::Leader => {
                            if let Err(error) = self.send_heartbeat().await {
                                warn!(
                                    node_id = self.local_node_id,
                                    peer_node_id = self.peer_node_id,
                                    error = %error,
                                    "双节点协调器心跳失败，普通领导者立即停止写入"
                                );
                                *self.role.write().await = PairRole::Follower;
                            }
                        }
                        PairRole::FencedLeader => {
                            match self.send_heartbeat().await {
                                Ok(true) => {
                                    // 对端已恢复并接受相同任期、实例和提交水位，单存活领导权才可
                                    // 安全升级回需要 2/2 持续确认的普通领导权。
                                    *self.role.write().await = PairRole::Leader;
                                }
                                Ok(false) => {
                                    // 外部隔离只在对端不可达期间提供单存活保证。对端已经能够返回
                                    // 明确拒绝时，继续依赖旧隔离结果会允许两个控制面同时存活，必须退位。
                                    *self.role.write().await = PairRole::Follower;
                                    warn!(
                                        node_id = self.local_node_id,
                                        peer_node_id = self.peer_node_id,
                                        "双节点隔离领导者收到对端拒绝，立即退位并关闭单存活权威"
                                    );
                                }
                                Err(_) => {
                                    // 对端仍不可达时保持外部隔离授予的角色；持久领导身份和存储健康
                                    // 还会在 route_authoritative 中独立复验，任一失效都会关闭路由。
                                }
                            }
                        }
                        PairRole::Follower => {
                            if self.last_peer_contact.lock().await.elapsed() >= self.election_timeout {
                                if let Err(error) = self.campaign().await {
                                    warn!(
                                        node_id = self.local_node_id,
                                        peer_node_id = self.peer_node_id,
                                        error = %error,
                                        "双节点协调器竞选未完成"
                                    );
                                    tokio::time::sleep(self.election_retry).await;
                                }
                            }
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

    /// 业务作用: 返回当前持久化领导节点编号，供跟随者把观察和动作请求转发到唯一写入者。
    ///
    /// 返回: 已确认领导者存在时返回节点编号，否则返回空值。
    pub fn leader_node_id(&self) -> anyhow::Result<Option<u64>> {
        Ok(self.store.pair_status()?.leader_node_id)
    }

    /// 业务作用: 返回双节点存储已经稳定落盘的当前任期，供动作令牌阻止同一节点跨任期复用旧批准。
    ///
    /// 返回: 返回持久化任期；存储失败关闭时返回错误。
    pub fn current_term(&self) -> anyhow::Result<u64> {
        Ok(self.store.pair_status()?.current_term)
    }

    /// 业务作用: 判断本节点是否具有可提交业务命令的领导角色。
    ///
    /// 返回: 普通领导者或已完成外部隔离的领导者返回真。
    pub async fn is_local_leader(&self) -> bool {
        matches!(
            *self.role.read().await,
            PairRole::Leader | PairRole::FencedLeader
        )
    }

    /// 业务作用: 检查本代理的数据路由是否仍有新鲜协调权威；失去领导心跳后跟随者会在保护窗口到期时关闭路由。
    ///
    /// 返回: 最近与对端确认仍在保护窗口内，或本节点持有外部隔离领导权时返回真。
    pub async fn route_authoritative(&self) -> bool {
        // 即使外部隔离仍有效，任期和状态机也必须来自可读的持久化镜像。一次落盘失败会把
        // 存储永久标记为失败关闭，不能让 FencedLeader 绕过该标记无限延长业务路由。
        let Ok(status) = self.store.pair_status() else {
            return false;
        };
        if *self.role.read().await == PairRole::FencedLeader {
            // 可达对端发起的更高任期竞选会先持久化清空 leader。即使运行时循环尚未
            // 执行下一次心跳，也不能让旧 FencedLeader 仅凭内存角色继续开放业务路由。
            return status.leader_node_id == Some(self.local_node_id);
        }
        self.last_peer_contact.lock().await.elapsed() <= self.route_guard_timeout
    }

    /// 业务作用: 由本地领导者把业务命令写入本地哈希链、复制到对端、提交状态机并通知对端提交。
    /// 参数说明:
    /// - `command`: 需要获得全局顺序的协调业务命令。
    ///
    /// 返回: 返回已提交状态机响应；普通模式下对端未确认落盘时绝不返回动作令牌。
    pub async fn write(
        &self,
        command: CoordinationCommand,
    ) -> anyhow::Result<CoordinationResponse> {
        let role = *self.role.read().await;
        if !matches!(role, PairRole::Leader | PairRole::FencedLeader) {
            bail!("local pair coordinator is not leader");
        }
        if role == PairRole::Leader
            && self.last_peer_contact.lock().await.elapsed() > self.route_guard_timeout
        {
            *self.role.write().await = PairRole::Follower;
            bail!("pair leader lost peer authority before command replication");
        }
        let status = self.store.pair_status()?;
        let term = status.current_term;
        let sequence = status
            .last_sequence
            .checked_add(1)
            .context("pair record sequence is exhausted")?;
        let payload = serde_json::to_vec(&command).context("encode pair coordination command")?;
        let record_hash = pair_record_hash(
            &status.last_hash,
            term,
            sequence,
            self.local_node_id,
            &self.startup_nonce,
            &payload,
        );
        self.store.pair_append_record(
            self.local_node_id,
            term,
            &self.startup_nonce,
            sequence,
            &status.last_hash,
            &record_hash,
            payload.clone(),
        )?;
        if role == PairRole::Leader {
            let response = async {
                let mut client = self.network.pair_client(self.peer_node_id).await?;
                client
                    .pair_replicate(PairReplicateRequest {
                        cluster_id: self.wire_cluster_id.to_string(),
                        leader_node_id: self.local_node_id,
                        term,
                        leader_startup_nonce: self.startup_nonce.to_string(),
                        sequence,
                        previous_hash: status.last_hash.clone(),
                        record_hash: record_hash.clone(),
                        payload,
                    })
                    .await
                    .context("replicate pair coordination record")
                    .map(|reply| reply.into_inner())
            }
            .await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    // 本地记录尚未得到 2/2 确认，因此不能继续追加下一条记录；先退位，恢复通信后的竞选会对比双方尾部并安全收敛。
                    *self.role.write().await = PairRole::Follower;
                    return Err(error);
                }
            };
            if response.term > term {
                self.store.pair_observe_term(response.term)?;
                *self.role.write().await = PairRole::Follower;
                bail!(
                    "peer rejected pair record with higher term `{}`",
                    response.term
                );
            }
            if !response.persisted {
                *self.role.write().await = PairRole::Follower;
                bail!("peer rejected pair record: {}", response.reason);
            }
            if response.term != term
                || response.last_sequence != sequence
                || response.last_hash != record_hash
            {
                *self.role.write().await = PairRole::Follower;
                bail!("peer pair replication acknowledgement does not match record");
            }
            *self.last_peer_contact.lock().await = Instant::now();
        }
        let local_response = self.store.pair_commit_record(
            self.local_node_id,
            term,
            &self.startup_nonce,
            sequence,
            &record_hash,
        )?;
        if role == PairRole::Leader {
            match self.network.pair_client(self.peer_node_id).await {
                Ok(mut client) => match client
                    .pair_commit(PairCommitRequest {
                        cluster_id: self.wire_cluster_id.to_string(),
                        leader_node_id: self.local_node_id,
                        term,
                        leader_startup_nonce: self.startup_nonce.to_string(),
                        sequence,
                        record_hash: record_hash.clone(),
                    })
                    .await
                {
                    Ok(response)
                        if response.get_ref().committed && response.get_ref().term == term =>
                    {
                        *self.last_peer_contact.lock().await = Instant::now();
                    }
                    Ok(response) => {
                        if response.get_ref().term > term {
                            if let Err(error) =
                                self.store.pair_observe_term(response.get_ref().term)
                            {
                                warn!(
                                    node_id = self.local_node_id,
                                    error = %error,
                                    "双节点协调器记录更高任期失败，将保持退位并等待人工处理"
                                );
                            }
                        }
                        *self.role.write().await = PairRole::Follower;
                        warn!(
                            node_id = self.local_node_id,
                            sequence,
                            reason = %response.get_ref().reason,
                            "双节点记录已完成 2/2 落盘并在本地提交，但对端提交通知被拒绝；本节点退位，后续竞选将恢复提交水位"
                        );
                    }
                    Err(error) => warn!(
                        node_id = self.local_node_id,
                        sequence,
                        error = %error,
                        "双节点记录已完成 2/2 落盘，但对端提交通知暂时失败"
                    ),
                },
                Err(error) => warn!(
                    node_id = self.local_node_id,
                    sequence,
                    error = %error,
                    "双节点记录已完成 2/2 落盘，但无法建立对端提交通道"
                ),
            }
        }
        Ok(local_response)
    }

    /// 业务作用: 在跟随者上把业务命令转发给当前持久化领导者，本地不生成日志或动作令牌。
    /// 参数说明:
    /// - `command`: 需要提交的协调业务命令。
    ///
    /// 返回: 返回领导者提交后的响应；没有已确认领导者或转发失败时返回错误。
    pub async fn submit(
        &self,
        command: CoordinationCommand,
    ) -> anyhow::Result<CoordinationResponse> {
        if self.is_local_leader().await {
            return self.write(command).await;
        }
        let leader = self
            .leader_node_id()?
            .context("pair coordinator has no confirmed leader")?;
        if leader == self.local_node_id {
            bail!("persisted pair leader is local but runtime authority is stale");
        }
        let mut client = self.network.pair_client(leader).await?;
        let response = client
            .submit_command(RpcEnvelope {
                cluster_id: self.wire_cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                payload: serde_json::to_vec(&command)
                    .context("encode forwarded pair coordination command")?,
                error: String::new(),
            })
            .await
            .context("forward pair coordination command")?
            .into_inner();
        if response.cluster_id != self.wire_cluster_id.as_ref() || response.sender_node_id != leader
        {
            bail!("pair leader response identity does not match the requested member");
        }
        if !response.error.is_empty() {
            bail!("pair leader rejected forwarded command: {}", response.error);
        }
        serde_json::from_slice(&response.payload)
            .context("decode forwarded pair coordination response")
    }

    /// 业务作用: 处理对端竞选请求并返回本地持久化后的任期、日志摘要和优胜者。
    /// 参数说明:
    /// - `request`: 对端候选状态。
    ///
    /// 返回: 返回竞选决定；集群或成员身份不匹配时返回错误。
    pub fn handle_campaign(
        &self,
        request: PairCampaignRequest,
    ) -> anyhow::Result<PairCampaignResponse> {
        self.validate_peer(&request.cluster_id, request.sender_node_id)?;
        let decision = self.store.pair_accept_campaign(
            request.sender_node_id,
            request.term,
            request.election_time_ns,
            request.startup_nonce,
            request.last_sequence,
            &request.last_hash,
            request.committed_sequence,
            &request.committed_hash,
        )?;
        Ok(PairCampaignResponse {
            accepted: decision.accepted,
            term: decision.status.current_term,
            responder_node_id: self.local_node_id,
            election_time_ns: decision.status.winner_election_time_ns.unwrap_or(0),
            startup_nonce: self.startup_nonce.to_string(),
            last_sequence: decision.status.last_sequence,
            last_hash: decision.status.last_hash,
            committed_sequence: decision.status.committed_sequence,
            reason: decision.reason,
            winner_node_id: decision.status.winner_node_id.unwrap_or(0),
            winner_startup_nonce: decision.status.winner_startup_nonce.unwrap_or_default(),
            committed_hash: decision.status.committed_hash,
        })
    }

    /// 业务作用: 处理领导者复制请求，仅在身份、任期、连续位置和哈希链全部匹配后确认稳定落盘。
    /// 参数说明:
    /// - `request`: 需要持久化的双节点记录。
    ///
    /// 返回: 返回落盘结果和本地日志摘要，业务拒绝使用响应字段表达。
    pub fn handle_replicate(&self, request: PairReplicateRequest) -> PairReplicateResponse {
        if let Err(error) = self.validate_peer(&request.cluster_id, request.leader_node_id) {
            return pair_replicate_error(self.store.pair_status().ok(), error.to_string());
        }
        match self.store.pair_append_record(
            request.leader_node_id,
            request.term,
            &request.leader_startup_nonce,
            request.sequence,
            &request.previous_hash,
            &request.record_hash,
            request.payload,
        ) {
            Ok(status) => PairReplicateResponse {
                persisted: true,
                term: status.current_term,
                last_sequence: status.last_sequence,
                last_hash: status.last_hash,
                reason: String::new(),
            },
            Err(error) => pair_replicate_error(self.store.pair_status().ok(), error.to_string()),
        }
    }

    /// 业务作用: 处理领导者提交通知，校验目标记录后连续应用状态机并返回提交水位。
    /// 参数说明:
    /// - `request`: 领导者声明的提交位置和哈希。
    ///
    /// 返回: 返回是否提交成功；错误不会被伪装成成功水位。
    pub fn handle_commit(&self, request: PairCommitRequest) -> PairCommitResponse {
        if let Err(error) = self.validate_peer(&request.cluster_id, request.leader_node_id) {
            return pair_commit_error(self.store.pair_status().ok(), error.to_string());
        }
        match self.store.pair_commit_record(
            request.leader_node_id,
            request.term,
            &request.leader_startup_nonce,
            request.sequence,
            &request.record_hash,
        ) {
            Ok(_) => match self.store.pair_status() {
                Ok(status) => PairCommitResponse {
                    committed: true,
                    term: status.current_term,
                    committed_sequence: status.committed_sequence,
                    reason: String::new(),
                },
                Err(error) => PairCommitResponse {
                    committed: false,
                    term: request.term,
                    committed_sequence: 0,
                    reason: error.to_string(),
                },
            },
            Err(error) => pair_commit_error(self.store.pair_status().ok(), error.to_string()),
        }
    }

    /// 业务作用: 处理领导者心跳，必要时用本地已复制日志追上提交水位并刷新路由权威时间。
    /// 参数说明:
    /// - `request`: 领导者身份、任期和提交摘要。
    ///
    /// 返回: 返回心跳接受结果；接受时异步调用方还会刷新运行时联系时间。
    pub async fn handle_heartbeat(&self, request: PairHeartbeatRequest) -> PairHeartbeatResponse {
        if let Err(error) = self.validate_peer(&request.cluster_id, request.leader_node_id) {
            return pair_heartbeat_error(self.store.pair_status().ok(), error.to_string());
        }
        match self.store.pair_accept_heartbeat(
            request.leader_node_id,
            request.term,
            &request.leader_startup_nonce,
            request.committed_sequence,
            &request.committed_hash,
        ) {
            Ok(decision) if decision.accepted => {
                *self.role.write().await = PairRole::Follower;
                *self.last_peer_contact.lock().await = Instant::now();
                PairHeartbeatResponse {
                    accepted: true,
                    term: decision.status.current_term,
                    committed_sequence: decision.status.committed_sequence,
                    reason: String::new(),
                }
            }
            Ok(decision) => pair_heartbeat_error(Some(decision.status), decision.reason),
            Err(error) => pair_heartbeat_error(self.store.pair_status().ok(), error.to_string()),
        }
    }

    /// 业务作用: 发起一个已经先行持久化的新任期，与对端交换日志摘要并只确认双方相同的优胜候选。
    ///
    /// 返回: 双方确认本节点后建立普通领导权；对端不可达且外部 fencing 成功时建立隔离领导权。
    async fn campaign(&self) -> anyhow::Result<()> {
        let election_time_ns = now_unix_nanos()?;
        let status = self.store.pair_start_campaign(
            self.local_node_id,
            election_time_ns,
            self.startup_nonce.to_string(),
        )?;
        let request = PairCampaignRequest {
            cluster_id: self.wire_cluster_id.to_string(),
            sender_node_id: self.local_node_id,
            term: status.current_term,
            election_time_ns,
            startup_nonce: self.startup_nonce.to_string(),
            last_sequence: status.last_sequence,
            last_hash: status.last_hash.clone(),
            committed_sequence: status.committed_sequence,
            committed_hash: status.committed_hash.clone(),
        };
        let response = match self.network.pair_client(self.peer_node_id).await {
            Ok(mut client) => client
                .pair_campaign(request)
                .await
                .map(|reply| reply.into_inner()),
            Err(error) => Err(nasa::grpc::Status::unavailable(error.to_string())),
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                if !self.allow_single_survivor {
                    return Err(error).context("pair campaign requires peer confirmation");
                }
                self.fence_peer().await?;
                self.store.pair_confirm_leader(
                    status.current_term,
                    self.local_node_id,
                    election_time_ns,
                    &self.startup_nonce,
                )?;
                *self.role.write().await = PairRole::FencedLeader;
                info!(
                    node_id = self.local_node_id,
                    term = status.current_term,
                    "双节点协调器在确认对端已隔离后进入单存活领导状态"
                );
                return Ok(());
            }
        };
        *self.last_peer_contact.lock().await = Instant::now();
        if response.term > status.current_term {
            self.store.pair_observe_term(response.term)?;
            *self.role.write().await = PairRole::Follower;
            bail!("peer already persisted higher term `{}`", response.term);
        }
        if response.responder_node_id != self.peer_node_id {
            bail!("pair campaign response declares an unexpected responder node");
        }
        if response.term != status.current_term {
            bail!("pair campaign response term does not match local campaign");
        }
        if response.last_sequence != status.last_sequence || response.last_hash != status.last_hash
        {
            // 本地可能在前一次写入时先落盘、但从未得到对端确认。只有对端明确停在相同提交点时才能删掉该尾部；
            // 若双方各有不同尾部则保持失败关闭，避免凭时间或节点编号覆盖真实分叉。
            if status.last_sequence > status.committed_sequence
                && response.last_sequence == status.committed_sequence
                && response.committed_sequence == status.committed_sequence
                && response.last_hash == status.committed_hash
                && response.committed_hash == status.committed_hash
            {
                self.store.pair_truncate_uncommitted_tail(
                    response.last_sequence,
                    &response.last_hash,
                    response.committed_sequence,
                    &response.committed_hash,
                )?;
                bail!("local uncommitted pair tail was safely truncated; campaign will retry");
            }
            bail!("pair campaign response does not match local log summary");
        }
        if response.committed_sequence > status.committed_sequence {
            self.store
                .pair_recover_commit(response.committed_sequence, &response.committed_hash)?;
            bail!("local pair commit watermark recovered; campaign will retry");
        }
        if response.committed_sequence != status.committed_sequence
            || response.committed_hash != status.committed_hash
        {
            bail!("pair campaign response does not match local committed watermark");
        }
        if response.accepted && response.winner_node_id == self.local_node_id {
            self.store.pair_confirm_leader(
                status.current_term,
                self.local_node_id,
                election_time_ns,
                &self.startup_nonce,
            )?;
            *self.role.write().await = PairRole::Leader;
            if !self.send_heartbeat().await? {
                *self.role.write().await = PairRole::Follower;
                bail!("peer did not confirm first leader heartbeat");
            }
            info!(
                node_id = self.local_node_id,
                term = status.current_term,
                election_time_ns,
                "双节点协调器完成 2/2 领导选举"
            );
            return Ok(());
        }
        if response.winner_node_id == self.peer_node_id {
            self.store.pair_confirm_leader(
                status.current_term,
                response.winner_node_id,
                response.election_time_ns,
                &response.winner_startup_nonce,
            )?;
            *self.role.write().await = PairRole::Follower;
            return Ok(());
        }
        bail!("pair campaign did not converge to a configured winner")
    }

    /// 业务作用: 向对端发送当前任期和提交摘要，接受响应后刷新普通领导权的联系时间。
    ///
    /// 返回: 对端接受心跳返回真，业务拒绝返回假，网络错误返回错误。
    async fn send_heartbeat(&self) -> anyhow::Result<bool> {
        let status = self.store.pair_status()?;
        let mut client = self.network.pair_client(self.peer_node_id).await?;
        let response = client
            .pair_heartbeat(PairHeartbeatRequest {
                cluster_id: self.wire_cluster_id.to_string(),
                leader_node_id: self.local_node_id,
                term: status.current_term,
                leader_startup_nonce: self.startup_nonce.to_string(),
                committed_sequence: status.committed_sequence,
                committed_hash: status.committed_hash,
            })
            .await
            .context("send pair leader heartbeat")?
            .into_inner();
        if response.term > status.current_term {
            self.store.pair_observe_term(response.term)?;
            *self.role.write().await = PairRole::Follower;
            return Ok(false);
        }
        if response.accepted {
            *self.last_peer_contact.lock().await = Instant::now();
        }
        Ok(response.accepted)
    }

    /// 业务作用: 在允许单存活接管时执行外部对端隔离命令，只有成功退出才授予隔离领导权。
    ///
    /// 返回: 命令在时限内成功退出时返回成功，超时、启动失败或非零退出均拒绝接管。
    async fn fence_peer(&self) -> anyhow::Result<()> {
        let (program, arguments) = self
            .peer_fencing_command
            .split_first()
            .context("pair peer fencing command is empty")?;
        let replace = |value: &str| {
            value
                .replace("{cluster_id}", &self.fencing_cluster_id)
                .replace("{peer_node_id}", &self.peer_node_id.to_string())
                .replace("{peer_address}", &self.peer_address)
        };
        let mut command = tokio::process::Command::new(replace(program));
        command.args(arguments.iter().map(|argument| replace(argument)));
        let status = tokio::time::timeout(self.peer_fencing_timeout, command.status())
            .await
            .context("pair peer fencing command timed out")?
            .context("start pair peer fencing command")?;
        if !status.success() {
            bail!("pair peer fencing command exited with `{status}`");
        }
        Ok(())
    }

    /// 业务作用: 校验双节点请求的集群标识和对端节点编号，防止其它集群或本节点回环请求修改状态。
    /// 参数说明:
    /// - `cluster_id`: 请求携带的集群标识。
    /// - `sender_node_id`: 请求发送者节点编号。
    ///
    /// 返回: 两项均匹配静态配置时返回成功，否则拒绝请求。
    fn validate_peer(&self, cluster_id: &str, sender_node_id: u64) -> anyhow::Result<()> {
        if cluster_id != self.wire_cluster_id.as_ref() {
            bail!("pair request cluster id does not match local genesis");
        }
        if sender_node_id != self.peer_node_id {
            bail!("pair request sender is not the configured peer");
        }
        Ok(())
    }
}

/// 业务作用: 生成绑定节点、进程和独立随机材料的启动实例标识，隔离同编号旧进程的延迟请求。
/// 参数说明:
/// - `node_id`: 当前静态节点编号。
///
/// 返回: 返回经过哈希压缩的实例标识；系统时间早于纪元或随机源不可用时返回错误。
fn startup_nonce(node_id: u64) -> anyhow::Result<String> {
    let nanos = crate::infrastructure::time::unix_nanos()?;
    // 墙钟只保留原有时间有效性边界；随机材料使同一时刻和 PID 复用不再重复启动身份。
    let nonce = crate::infrastructure::identity::instance_nonce()?;
    let source = format!("{node_id}:{}:{nanos}:{nonce}", std::process::id());
    Ok(hex_digest(&Sha256::digest(source.as_bytes())))
}

/// 业务作用: 计算双节点日志记录哈希，把前序哈希、任期、位置、领导身份和业务内容绑定为一条不可换序链。
/// 参数说明:
/// - `previous_hash`: 上一条记录哈希。
/// - `term`: 当前领导任期。
/// - `sequence`: 本条连续位置。
/// - `leader_node_id`: 领导节点编号。
/// - `startup_nonce`: 领导启动实例标识。
/// - `payload`: 业务命令字节。
///
/// 返回: 返回小写十六进制 SHA-256 摘要。
fn pair_record_hash(
    previous_hash: &str,
    term: u64,
    sequence: u64,
    leader_node_id: u64,
    startup_nonce: &str,
    payload: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous_hash.as_bytes());
    hasher.update(term.to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    hasher.update(leader_node_id.to_be_bytes());
    hasher.update(startup_nonce.as_bytes());
    hasher.update(payload);
    hex_digest(&hasher.finalize())
}

/// 业务作用: 将字节序列编码为固定小写十六进制文本，用于启动实例和日志哈希的可观察表示。
/// 参数说明:
/// - `bytes`: 需要编码的原始字节。
///
/// 返回: 返回长度为输入两倍的十六进制字符串。
fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

/// 业务作用: 构造双节点复制拒绝响应，并尽可能附带当前持久化摘要帮助领导者退位或诊断。
/// 参数说明:
/// - `status`: 可选的本地状态摘要。
/// - `reason`: 拒绝原因。
///
/// 返回: 返回 `persisted=false` 的复制响应。
fn pair_replicate_error(
    status: Option<super::store::PairStorageStatus>,
    reason: String,
) -> PairReplicateResponse {
    PairReplicateResponse {
        persisted: false,
        term: status.as_ref().map(|value| value.current_term).unwrap_or(0),
        last_sequence: status
            .as_ref()
            .map(|value| value.last_sequence)
            .unwrap_or(0),
        last_hash: status.map(|value| value.last_hash).unwrap_or_default(),
        reason,
    }
}

/// 业务作用: 构造双节点提交拒绝响应，并携带本地提交水位。
/// 参数说明:
/// - `status`: 可选的本地状态摘要。
/// - `reason`: 拒绝原因。
///
/// 返回: 返回 `committed=false` 的提交响应。
fn pair_commit_error(
    status: Option<super::store::PairStorageStatus>,
    reason: String,
) -> PairCommitResponse {
    PairCommitResponse {
        committed: false,
        term: status.as_ref().map(|value| value.current_term).unwrap_or(0),
        committed_sequence: status.map(|value| value.committed_sequence).unwrap_or(0),
        reason,
    }
}

/// 业务作用: 构造双节点心跳拒绝响应，并携带本地任期与提交位置促使旧领导者停止写入。
/// 参数说明:
/// - `status`: 可选的本地状态摘要。
/// - `reason`: 拒绝原因。
///
/// 返回: 返回 `accepted=false` 的心跳响应。
fn pair_heartbeat_error(
    status: Option<super::store::PairStorageStatus>,
    reason: String,
) -> PairHeartbeatResponse {
    PairHeartbeatResponse {
        accepted: false,
        term: status.as_ref().map(|value| value.current_term).unwrap_or(0),
        committed_sequence: status.map(|value| value.committed_sequence).unwrap_or(0),
        reason,
    }
}
