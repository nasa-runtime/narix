use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::Vote;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Mutex};
use tokio::time::Instant;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

use super::pair::PairCoordinator;
use super::runtime::{CoordinatorHandle, CLUSTER_CONFIG_SCHEMA_VERSION};
use super::state::CoordinationCommand;
use super::wire::coordinator_rpc_server::{CoordinatorRpc, CoordinatorRpcServer};
use super::wire::{
    ClusterConfigRequest, ClusterConfigResponse, PairCampaignRequest, PairCampaignResponse,
    PairCommitRequest, PairCommitResponse, PairHeartbeatRequest, PairHeartbeatResponse,
    PairReplicateRequest, PairReplicateResponse, RpcEnvelope,
};
use super::{
    wire_cluster_id, CoordinatorRaft, CoordinatorTypeConfig, MAX_COORDINATION_GRPC_MESSAGE_BYTES,
};
use crate::config::ProxyClusterCoordinationConfig;
use crate::reload::MAX_CLUSTER_CONFIG_BYTES;

/// 业务作用: 提供节点间协调 gRPC 服务，把线上信封严格分派给三节点共识或双节点协议运行时。
/// 字段说明: `members` 在启动时固定；启用 TLS 后，`member_certificate_sha256` 还会把证书指纹固定到节点编号，避免持有合法证书的成员冒用其他编号。
#[derive(Clone)]
pub struct CoordinatorRpcService {
    cluster_id: Arc<str>,
    local_node_id: u64,
    members: Arc<HashSet<u64>>,
    member_certificate_sha256: Arc<BTreeMap<u64, String>>,
    raft: Option<CoordinatorRaft>,
    pair: Option<Arc<PairCoordinator>>,
    last_raft_authority_contact: Arc<Mutex<Instant>>,
    handle: Arc<CoordinatorHandle>,
}

impl std::fmt::Debug for CoordinatorRpcService {
    /// 业务作用: 输出不含证书和内部共识状态的服务摘要，避免调试日志泄露敏感材料或打印巨量日志。
    /// 参数说明:
    /// - `formatter`: 标准调试格式化器。
    ///
    /// 返回: 返回格式化结果。
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CoordinatorRpcService")
            .field("cluster_id", &self.cluster_id)
            .field("local_node_id", &self.local_node_id)
            .field("member_count", &self.members.len())
            .field(
                "certificate_identity_binding_count",
                &self.member_certificate_sha256.len(),
            )
            .field("raft_enabled", &self.raft.is_some())
            .field("pair_enabled", &self.pair.is_some())
            .finish()
    }
}

impl CoordinatorRpcService {
    /// 业务作用: 构造协调 gRPC 服务，并保证双节点与多数派协议在同一进程中只能启用一个。
    /// 参数说明:
    /// - `config`: 已校验的静态协调配置。
    /// - `raft`: 多数派协议运行时；双节点模式为空。
    /// - `pair`: 双节点协议运行时；多数派模式为空。
    /// - `last_raft_authority_contact`: 多数派跟随者最近收到共识消息的单调时间。
    /// - `handle`: 配置事务和协议无关业务命令共用的协调器句柄。
    ///
    /// 返回: 返回完成成员白名单固化的服务实例。
    pub fn new(
        config: &ProxyClusterCoordinationConfig,
        raft: Option<CoordinatorRaft>,
        pair: Option<Arc<PairCoordinator>>,
        last_raft_authority_contact: Arc<Mutex<Instant>>,
        handle: Arc<CoordinatorHandle>,
    ) -> Self {
        Self {
            cluster_id: Arc::from(wire_cluster_id(config)),
            local_node_id: config.node_id,
            members: Arc::new(config.peers.iter().map(|peer| peer.node_id).collect()),
            member_certificate_sha256: Arc::new(
                config
                    .peers
                    .iter()
                    .filter_map(|peer| {
                        peer.certificate_sha256
                            .as_ref()
                            .map(|fingerprint| (peer.node_id, fingerprint.to_ascii_lowercase()))
                    })
                    .collect(),
            ),
            raft,
            pair,
            last_raft_authority_contact,
            handle,
        }
    }

    /// 业务作用: 校验共识信封的集群隔离标识和静态发送成员，防止误连或未授权节点污染任期和日志。
    /// 参数说明:
    /// - `envelope`: 收到的共识信封。
    ///
    /// 返回: 身份与创世信息一致时返回成功，否则返回 gRPC 前置条件错误。
    fn validate_envelope(&self, envelope: &RpcEnvelope) -> Result<(), Status> {
        if envelope.cluster_id != self.cluster_id.as_ref() {
            return Err(Status::failed_precondition(
                "coordination cluster id does not match local genesis",
            ));
        }
        if envelope.sender_node_id == self.local_node_id
            || !self.members.contains(&envelope.sender_node_id)
        {
            return Err(Status::permission_denied(
                "coordination sender is not a configured remote member",
            ));
        }
        Ok(())
    }

    /// 业务作用: 校验集群配置请求的集群隔离标识和静态发送成员。
    ///
    /// 参数说明:
    /// - `request`: 尚未执行配置阶段的 gRPC 请求正文。
    ///
    /// 返回: 集群和发送者均属于本节点创世成员表时返回成功。
    fn validate_cluster_config_request(
        &self,
        request: &ClusterConfigRequest,
    ) -> Result<(), Status> {
        if request.cluster_id != self.cluster_id.as_ref() {
            return Err(Status::failed_precondition(
                "configuration cluster id does not match local genesis",
            ));
        }
        if request.schema_version != CLUSTER_CONFIG_SCHEMA_VERSION {
            return Err(Status::failed_precondition(
                "configuration schema version is not supported",
            ));
        }
        if request.transaction_timeout_ms == 0 || request.transaction_timeout_ms > 86_400_000 {
            return Err(Status::invalid_argument(
                "configuration transaction timeout must be between 1 and 86400000 ms",
            ));
        }
        if request.payload.len() > MAX_CLUSTER_CONFIG_BYTES {
            return Err(Status::resource_exhausted(
                "cluster configuration payload exceeds 16 MiB",
            ));
        }
        if request.sender_node_id == self.local_node_id
            || !self.members.contains(&request.sender_node_id)
        {
            return Err(Status::permission_denied(
                "configuration sender is not a configured remote member",
            ));
        }
        Ok(())
    }

    /// 业务作用: 确认 prepare、activate 和 abort 请求由当前协调领导者发送。
    ///
    /// 参数说明:
    /// - `sender_node_id`: 已通过传输身份绑定的请求发送成员。
    ///
    /// 返回: 发送者等于当前协议领导者时返回成功。
    async fn validate_cluster_config_leader(&self, sender_node_id: u64) -> Result<(), Status> {
        let leader = self
            .handle
            .leader_node_id()
            .await
            .map_err(internal_status)?
            .ok_or_else(|| {
                Status::failed_precondition("coordination cluster has no current leader")
            })?;
        if leader != sender_node_id {
            return Err(Status::permission_denied(
                "configuration phase sender is not current leader",
            ));
        }
        Ok(())
    }

    /// 业务作用: 把共识载荷中的候选或领导节点编号绑定到已经通过传输层认证的信封发送者。
    /// 参数说明:
    /// - `sender_node_id`: 由静态成员表和客户端证书共同确认的实际发送成员。
    /// - `vote`: 日志复制、快照或投票载荷声明的候选身份。
    ///
    /// 返回: 载荷身份与实际发送成员一致时成功，否则拒绝把任期信息交给共识核心。
    fn validate_vote_sender(&self, sender_node_id: u64, vote: &Vote<u64>) -> Result<(), Status> {
        if vote.leader_id().voted_for() != Some(sender_node_id) {
            return Err(Status::permission_denied(
                "coordination payload vote does not match authenticated sender",
            ));
        }
        Ok(())
    }

    /// 业务作用: 校验跟随者转发的业务命令没有伪造另一个静态成员的观察票或动作申请身份。
    /// 参数说明:
    /// - `command`: 已完成严格反序列化的协调业务命令。
    /// - `sender_node_id`: 由信封和传输证书确认的实际发送成员。
    ///
    /// 返回: 命令不携带报告身份或携带身份与实际发送者一致时成功，否则拒绝转发。
    fn validate_command_sender(
        &self,
        command: &CoordinationCommand,
        sender_node_id: u64,
    ) -> Result<(), Status> {
        let declared_node_id = match command {
            CoordinationCommand::RecordObservation { observation, .. } => {
                Some(observation.reporter_node_id)
            }
            CoordinationCommand::BeginAction {
                requester_node_id, ..
            } => Some(*requester_node_id),
            CoordinationCommand::RegisterGroupConfig { .. }
            | CoordinationCommand::CommitClusterConfig { .. }
            | CoordinationCommand::CompleteAction { .. } => None,
        };
        if declared_node_id.is_some_and(|node_id| node_id != sender_node_id) {
            return Err(Status::permission_denied(
                "coordination command actor does not match authenticated sender",
            ));
        }
        Ok(())
    }

    /// 业务作用: 把传输层实际客户端证书绑定到请求声明的节点编号，补足“证书由可信根签发”无法阻止成员间身份冒用的缺口。
    /// 参数说明:
    /// - `request`: 尚未取出消息体的 gRPC 请求，用于读取 TLS 会话中的客户端证书。
    /// - `sender_node_id`: 消息体声明的静态发送节点编号。
    ///
    /// 返回: 未启用 TLS 指纹绑定时直接成功；启用后只有首张客户端证书的 DER 摘要与静态配置完全一致才成功。
    fn validate_transport_identity<T>(
        &self,
        request: &Request<T>,
        sender_node_id: u64,
    ) -> Result<(), Status> {
        if self.member_certificate_sha256.is_empty() {
            return Ok(());
        }
        let expected = self
            .member_certificate_sha256
            .get(&sender_node_id)
            .ok_or_else(|| {
                Status::permission_denied("coordination sender has no certificate binding")
            })?;
        let certificates = request.peer_certs().ok_or_else(|| {
            Status::unauthenticated("coordination request has no client certificate")
        })?;
        let certificate = certificates.first().ok_or_else(|| {
            Status::unauthenticated("coordination request has an empty client certificate chain")
        })?;
        let actual = sha256_hex(certificate.as_ref());
        if actual != *expected {
            return Err(Status::permission_denied(
                "coordination client certificate does not match sender node id",
            ));
        }
        Ok(())
    }

    /// 业务作用: 取得多数派共识运行时；双节点模式收到共识 RPC 时明确拒绝而不是尝试协议降级。
    /// 返回: 多数派运行时引用，或协议类型不匹配错误。
    fn raft(&self) -> Result<&CoordinatorRaft, Status> {
        self.raft.as_ref().ok_or_else(|| {
            Status::failed_precondition("local genesis uses pair protocol, not quorum protocol")
        })
    }

    /// 业务作用: 取得双节点运行时；多数派模式收到双节点 RPC 时明确拒绝，防止存活节点数变化触发退化。
    /// 返回: 双节点运行时引用，或协议类型不匹配错误。
    fn pair(&self) -> Result<&Arc<PairCoordinator>, Status> {
        self.pair.as_ref().ok_or_else(|| {
            Status::failed_precondition("local genesis uses quorum protocol, not pair protocol")
        })
    }
}

#[tonic::async_trait]
impl CoordinatorRpc for CoordinatorRpcService {
    /// 业务作用: 接收多数派日志复制与心跳请求，并在共识核心处理完成后刷新跟随者路由权威时间。
    /// 参数说明:
    /// - `request`: 包含日志复制请求 JSON 的集群信封。
    ///
    /// 返回: 返回序列化后的共识处理结果。
    async fn raft_append(
        &self,
        request: Request<RpcEnvelope>,
    ) -> Result<Response<RpcEnvelope>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        let envelope = request.into_inner();
        self.validate_envelope(&envelope)?;
        let sender_node_id = envelope.sender_node_id;
        let request: AppendEntriesRequest<CoordinatorTypeConfig> = decode_payload(&envelope)?;
        self.validate_vote_sender(sender_node_id, &request.vote)?;
        let result = self.raft()?.append_entries(request).await;
        // 只有处理请求后仍被共识核心认定为当前领导者的发送者，才能刷新数据面保护窗口。
        // 旧领导者或较低任期成员即使得到结构化拒绝响应，也不能延长过期路由的寿命。
        if result.is_ok() && self.raft()?.current_leader().await == Some(sender_node_id) {
            *self.last_raft_authority_contact.lock().await = Instant::now();
        }
        Ok(Response::new(encode_result(
            &self.cluster_id,
            self.local_node_id,
            &result,
        )?))
    }

    /// 业务作用: 接收多数派预投票或正式投票请求，投票结果只有在持久化存储返回后才会发送。
    /// 参数说明:
    /// - `request`: 包含投票请求 JSON 的集群信封。
    ///
    /// 返回: 返回序列化后的投票结果。
    async fn raft_vote(
        &self,
        request: Request<RpcEnvelope>,
    ) -> Result<Response<RpcEnvelope>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        let envelope = request.into_inner();
        self.validate_envelope(&envelope)?;
        let request: VoteRequest<u64> = decode_payload(&envelope)?;
        self.validate_vote_sender(envelope.sender_node_id, &request.vote)?;
        let result = self.raft()?.vote(request).await;
        Ok(Response::new(encode_result(
            &self.cluster_id,
            self.local_node_id,
            &result,
        )?))
    }

    /// 业务作用: 接收多数派状态机快照分块并交给共识核心验证安装。
    /// 参数说明:
    /// - `request`: 包含快照安装请求 JSON 的集群信封。
    ///
    /// 返回: 返回序列化后的快照安装结果。
    async fn raft_install_snapshot(
        &self,
        request: Request<RpcEnvelope>,
    ) -> Result<Response<RpcEnvelope>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        let envelope = request.into_inner();
        self.validate_envelope(&envelope)?;
        let sender_node_id = envelope.sender_node_id;
        let request: InstallSnapshotRequest<CoordinatorTypeConfig> = decode_payload(&envelope)?;
        self.validate_vote_sender(sender_node_id, &request.vote)?;
        let result = self.raft()?.install_snapshot(request).await;
        if result.is_ok() && self.raft()?.current_leader().await == Some(sender_node_id) {
            *self.last_raft_authority_contact.lock().await = Instant::now();
        }
        Ok(Response::new(encode_result(
            &self.cluster_id,
            self.local_node_id,
            &result,
        )?))
    }

    /// 业务作用: 接收双节点候选状态并返回经过持久化的确定性竞选结果。
    /// 参数说明:
    /// - `request`: 双节点竞选请求。
    ///
    /// 返回: 返回任期、日志摘要和本任期优胜候选。
    async fn pair_campaign(
        &self,
        request: Request<PairCampaignRequest>,
    ) -> Result<Response<PairCampaignResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        let response = self
            .pair()?
            .handle_campaign(request.into_inner())
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
        Ok(Response::new(response))
    }

    /// 业务作用: 接收并稳定落盘一条双节点哈希链记录。
    /// 参数说明:
    /// - `request`: 双节点复制记录。
    ///
    /// 返回: 返回落盘确认或业务拒绝原因。
    async fn pair_replicate(
        &self,
        request: Request<PairReplicateRequest>,
    ) -> Result<Response<PairReplicateResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().leader_node_id)?;
        Ok(Response::new(
            self.pair()?.handle_replicate(request.into_inner()),
        ))
    }

    /// 业务作用: 接收双节点提交通知并应用已经落盘的连续记录。
    /// 参数说明:
    /// - `request`: 双节点提交位置和哈希。
    ///
    /// 返回: 返回本地提交水位或拒绝原因。
    async fn pair_commit(
        &self,
        request: Request<PairCommitRequest>,
    ) -> Result<Response<PairCommitResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().leader_node_id)?;
        Ok(Response::new(
            self.pair()?.handle_commit(request.into_inner()),
        ))
    }

    /// 业务作用: 接收双节点领导者心跳，刷新路由权威并追赶已复制记录的提交水位。
    /// 参数说明:
    /// - `request`: 双节点领导身份和提交摘要。
    ///
    /// 返回: 返回是否继续承认该领导者。
    async fn pair_heartbeat(
        &self,
        request: Request<PairHeartbeatRequest>,
    ) -> Result<Response<PairHeartbeatResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().leader_node_id)?;
        Ok(Response::new(
            self.pair()?.handle_heartbeat(request.into_inner()).await,
        ))
    }

    /// 业务作用: 接收跟随者转发的业务协调命令，只有本节点当前是领导者时才写入复制日志。
    /// 参数说明:
    /// - `request`: 包含业务命令 JSON 的集群信封。
    ///
    /// 返回: 返回业务状态机响应；非领导者、身份错误或提交失败通过信封错误字段返回。
    async fn submit_command(
        &self,
        request: Request<RpcEnvelope>,
    ) -> Result<Response<RpcEnvelope>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        let envelope = request.into_inner();
        self.validate_envelope(&envelope)?;
        let command: CoordinationCommand = decode_payload(&envelope)?;
        self.validate_command_sender(&command, envelope.sender_node_id)?;
        let result = if let Some(pair) = &self.pair {
            pair.write(command).await
        } else if let Some(raft) = &self.raft {
            raft.client_write(command)
                .await
                .map(|response| response.data)
                .map_err(anyhow::Error::from)
        } else {
            Err(anyhow::anyhow!("coordinator protocol runtime is absent"))
        };
        let response = match result {
            Ok(data) => RpcEnvelope {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                payload: serde_json::to_vec(&data)
                    .map_err(|error| Status::internal(error.to_string()))?,
                error: String::new(),
            },
            Err(error) => RpcEnvelope {
                cluster_id: self.cluster_id.to_string(),
                sender_node_id: self.local_node_id,
                payload: Vec::new(),
                error: error.to_string(),
            },
        };
        Ok(Response::new(response))
    }

    /// 业务作用: 接收任意静态成员转发的配置提案，并由当前领导节点执行多数派事务。
    ///
    /// 参数说明:
    /// - `request`: 包含规范化配置正文和摘要的提案请求。
    ///
    /// 返回: 多数派活动版本一致时返回成功；本节点不是领导者或事务失败时返回错误状态。
    async fn propose_cluster_config(
        &self,
        request: Request<ClusterConfigRequest>,
    ) -> Result<Response<ClusterConfigResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        self.validate_cluster_config_request(request.get_ref())?;
        if !self.handle.is_local_leader().await {
            return Ok(Response::new(cluster_config_error(
                self.local_node_id,
                request.get_ref().revision,
                &request.get_ref().digest,
                "configuration proposal reached a non-leader member",
            )));
        }
        let request = request.into_inner();
        let response = self
            .handle
            .propose_cluster_config(
                request.expected_revision,
                Duration::from_millis(request.transaction_timeout_ms),
                request.force_revision,
                request.digest.clone(),
                request.payload,
            )
            .await
            .unwrap_or_else(|error| {
                cluster_config_error(
                    self.local_node_id,
                    request.revision,
                    &request.digest,
                    &error.to_string(),
                )
            });
        Ok(Response::new(response))
    }

    /// 业务作用: 向静态成员返回当前领导者线性一致确认的已提交配置。
    ///
    /// 参数说明:
    /// - `request`: 请求成员身份和配置载荷 schema 版本；请求正文不携带候选配置。
    ///
    /// 返回: 最新 revision、摘要和规范化配置正文；本节点不是领导者时返回拒绝结果。
    async fn get_cluster_config(
        &self,
        request: Request<ClusterConfigRequest>,
    ) -> Result<Response<ClusterConfigResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        self.validate_cluster_config_request(request.get_ref())?;
        let request = request.into_inner();
        let response = match self.handle.linearizable_local_cluster_config().await {
            Ok(committed) => ClusterConfigResponse {
                accepted: true,
                responder_node_id: self.local_node_id,
                revision: committed.revision,
                digest: committed.digest,
                error: String::new(),
                payload: committed.payload,
                pending_members: 0,
            },
            Err(error) => cluster_config_error(
                self.local_node_id,
                request.revision,
                &request.digest,
                &error.to_string(),
            ),
        };
        Ok(Response::new(response))
    }

    /// 业务作用: 接收领导节点广播的候选配置，安全落盘并执行本地预检。
    ///
    /// 参数说明:
    /// - `request`: 领导节点分配的候选 revision、摘要和规范化正文。
    ///
    /// 返回: 本地候选落盘和运行时预检全部成功时确认相同版本。
    async fn prepare_cluster_config(
        &self,
        request: Request<ClusterConfigRequest>,
    ) -> Result<Response<ClusterConfigResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        self.validate_cluster_config_request(request.get_ref())?;
        self.validate_cluster_config_leader(request.get_ref().sender_node_id)
            .await?;
        let request = request.into_inner();
        let result = self
            .handle
            .prepare_cluster_config_from_leader(&request)
            .await;
        Ok(Response::new(cluster_config_result(
            self.local_node_id,
            &request,
            result,
        )))
    }

    /// 业务作用: 接收领导节点广播的已提交配置，发布运行代并更新活动磁盘指针。
    ///
    /// 参数说明:
    /// - `request`: 已经进入协调日志提交点的配置版本和正文。
    ///
    /// 返回: 本地运行代和活动指针都完成发布时确认相同版本。
    async fn activate_cluster_config(
        &self,
        request: Request<ClusterConfigRequest>,
    ) -> Result<Response<ClusterConfigResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        self.validate_cluster_config_request(request.get_ref())?;
        self.validate_cluster_config_leader(request.get_ref().sender_node_id)
            .await?;
        let request = request.into_inner();
        let result = self
            .handle
            .activate_cluster_config_from_leader(&request)
            .await;
        Ok(Response::new(cluster_config_result(
            self.local_node_id,
            &request,
            result,
        )))
    }

    /// 业务作用: 删除领导节点在共识提交前中止的候选配置。
    ///
    /// 参数说明:
    /// - `request`: 要中止的候选 revision 和摘要。
    ///
    /// 返回: 候选不存在或删除完成时确认对应版本。
    async fn abort_cluster_config(
        &self,
        request: Request<ClusterConfigRequest>,
    ) -> Result<Response<ClusterConfigResponse>, Status> {
        self.validate_transport_identity(&request, request.get_ref().sender_node_id)?;
        self.validate_cluster_config_request(request.get_ref())?;
        self.validate_cluster_config_leader(request.get_ref().sender_node_id)
            .await?;
        let request = request.into_inner();
        let result = self
            .handle
            .abort_cluster_config_from_leader(request.revision, &request.digest)
            .await;
        Ok(Response::new(cluster_config_result(
            self.local_node_id,
            &request,
            result,
        )))
    }
}

/// 业务作用: 启动协调器 gRPC 监听，并根据配置选择双向 TLS 或受控明文传输。
/// 参数说明:
/// - `config`: 提供监听地址和服务端证书路径的协调配置。
/// - `service`: 已绑定具体协议运行时的 gRPC 服务。
/// - `shutdown`: 进程关闭信号，用于停止接受新连接并优雅退出。
///
/// 返回: 监听、证书加载或服务运行失败时返回错误。
pub async fn serve(
    config: ProxyClusterCoordinationConfig,
    service: CoordinatorRpcService,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let address: SocketAddr = config
        .listen
        .parse()
        .with_context(|| format!("parse coordination listen `{}`", config.listen))?;
    let mut builder = Server::builder();
    if config.tls.enabled {
        let certificate_path = config
            .tls
            .certificate_path
            .as_deref()
            .context("coordination TLS certificate path is missing")?;
        let private_key_path = config
            .tls
            .private_key_path
            .as_deref()
            .context("coordination TLS private key path is missing")?;
        let client_ca_path = config
            .tls
            .client_ca_path
            .as_deref()
            .context("coordination TLS client CA path is missing")?;
        let identity = Identity::from_pem(
            std::fs::read(certificate_path).with_context(|| {
                format!("read coordination TLS certificate `{certificate_path}`")
            })?,
            std::fs::read(private_key_path).with_context(|| {
                format!("read coordination TLS private key `{private_key_path}`")
            })?,
        );
        let client_ca = Certificate::from_pem(
            std::fs::read(client_ca_path)
                .with_context(|| format!("read coordination TLS client CA `{client_ca_path}`"))?,
        );
        builder = builder
            .tls_config(
                ServerTlsConfig::new()
                    .identity(identity)
                    .client_ca_root(client_ca),
            )
            .context("configure coordination server TLS")?;
    }
    builder
        .add_service(
            CoordinatorRpcServer::new(service)
                .max_decoding_message_size(MAX_COORDINATION_GRPC_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_COORDINATION_GRPC_MESSAGE_BYTES),
        )
        .serve_with_shutdown(address, async move {
            loop {
                if *shutdown.borrow() || shutdown.changed().await.is_err() {
                    return;
                }
            }
        })
        .await
        .context("serve coordination gRPC endpoint")
}

/// 业务作用: 从 gRPC 信封严格反序列化具体请求，空载荷或字段不兼容会作为无效参数拒绝。
/// 参数说明:
/// - `envelope`: 已通过集群和成员身份校验的信封。
///
/// 返回: 返回反序列化后的请求对象。
fn decode_payload<T: DeserializeOwned>(envelope: &RpcEnvelope) -> Result<T, Status> {
    serde_json::from_slice(&envelope.payload)
        .map_err(|error| Status::invalid_argument(format!("decode coordination payload: {error}")))
}

/// 业务作用: 把共识运行结果序列化为响应信封，保留远端共识错误的类型结构供客户端正确退避或转发。
/// 参数说明:
/// - `cluster_id`: 本地不可变集群标识。
/// - `local_node_id`: 本地静态节点编号。
/// - `result`: 共识核心返回的成功或协议错误。
///
/// 返回: 返回带 JSON 结果的信封；序列化失败时返回服务内部错误。
fn encode_result<T: Serialize>(
    cluster_id: &str,
    local_node_id: u64,
    result: &T,
) -> Result<RpcEnvelope, Status> {
    Ok(RpcEnvelope {
        cluster_id: cluster_id.to_string(),
        sender_node_id: local_node_id,
        payload: serde_json::to_vec(result)
            .map_err(|error| Status::internal(format!("encode coordination result: {error}")))?,
        error: String::new(),
    })
}

/// 业务作用: 将本地集群配置阶段结果编码成不包含配置正文的响应。
///
/// 参数说明:
/// - `local_node_id`: 实际处理请求的本地静态成员编号。
/// - `request`: 本次处理的配置版本和摘要。
/// - `result`: 本地落盘、预检、发布或删除结果。
///
/// 返回: 成功时确认相同 revision 和摘要，失败时只携带错误文本。
fn cluster_config_result(
    local_node_id: u64,
    request: &ClusterConfigRequest,
    result: anyhow::Result<()>,
) -> ClusterConfigResponse {
    match result {
        Ok(()) => ClusterConfigResponse {
            accepted: true,
            responder_node_id: local_node_id,
            revision: request.revision,
            digest: request.digest.clone(),
            error: String::new(),
            payload: Vec::new(),
            pending_members: 0,
        },
        Err(error) => cluster_config_error(
            local_node_id,
            request.revision,
            &request.digest,
            &error.to_string(),
        ),
    }
}

/// 业务作用: 构造一个不包含配置正文和凭据的配置阶段拒绝响应。
///
/// 参数说明:
/// - `local_node_id`: 实际拒绝请求的成员编号。
/// - `revision`: 被拒绝的候选配置版本。
/// - `digest`: 被拒绝的候选摘要。
/// - `error`: 已由底层限制为不输出配置正文的错误原因。
///
/// 返回: `accepted=false` 的稳定 gRPC 业务响应。
fn cluster_config_error(
    local_node_id: u64,
    revision: u64,
    digest: &str,
    error: &str,
) -> ClusterConfigResponse {
    ClusterConfigResponse {
        accepted: false,
        responder_node_id: local_node_id,
        revision,
        digest: digest.to_string(),
        error: error.to_string(),
        payload: Vec::new(),
        pending_members: 0,
    }
}

/// 业务作用: 将内部协调错误转换为不泄露配置正文的 gRPC 内部状态。
///
/// 参数说明:
/// - `error`: 协调状态读取或协议查询错误。
///
/// 返回: gRPC internal 状态。
fn internal_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

/// 业务作用: 计算客户端证书 DER 内容的规范 SHA-256 指纹，供传输层身份与静态节点编号做常量格式比较。
/// 参数说明:
/// - `certificate_der`: TLS 会话提供的叶子证书 DER 字节。
///
/// 返回: 返回六十四位小写十六进制摘要，不包含分隔符。
fn sha256_hex(certificate_der: &[u8]) -> String {
    Sha256::digest(certificate_der)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
