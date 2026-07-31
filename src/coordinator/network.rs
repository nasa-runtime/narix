use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use super::wire::coordinator_rpc_client::CoordinatorRpcClient;
use super::wire::RpcEnvelope;
use super::{wire_cluster_id, CoordinatorTypeConfig, MAX_COORDINATION_GRPC_MESSAGE_BYTES};
use crate::config::ProxyClusterCoordinationConfig;

/// 业务作用: 为共识网络错误固定节点和远端错误类型，确保不可达、传输失败和远端协议拒绝得到不同处理。
pub type CoordinatorRpcError<E = openraft::error::Infallible> =
    RPCError<u64, BasicNode, openraft::error::RaftError<u64, E>>;

/// 业务作用: 描述一个静态成员的 gRPC 连接目标和证书域名。
/// 字段说明: 地址来自不可变成员表，证书域名仅在启用 TLS 时使用。
#[derive(Debug, Clone)]
struct PeerTransport {
    address: String,
    server_name: Option<String>,
}

/// 业务作用: 保存客户端双向 TLS 所需的原始证书材料，每次创建端点时据此构造独立连接配置。
/// 字段说明: 只在进程启动时读取文件，运行期间证书内容不会因路径文件被替换而半更新。
#[derive(Debug, Clone)]
struct ClientTlsMaterials {
    certificate: Vec<u8>,
    private_key: Vec<u8>,
    ca_certificate: Vec<u8>,
}

/// 业务作用: 创建并缓存节点间 gRPC 通道，为共识核心提供按静态节点编号寻址的网络工厂。
/// 字段说明: 通道可以安全克隆并由传输层自动重连，缓存避免每次心跳重复进行握手。
#[derive(Debug, Clone)]
pub struct CoordinatorNetworkFactory {
    cluster_id: Arc<str>,
    local_node_id: u64,
    rpc_timeout: Duration,
    peers: Arc<BTreeMap<u64, PeerTransport>>,
    tls: Option<Arc<ClientTlsMaterials>>,
    channels: Arc<RwLock<BTreeMap<u64, Channel>>>,
}

/// 业务作用: 表示到一个指定共识成员的逻辑连接，实现日志复制、投票和快照三个 RPC。
/// 字段说明: 实际通道由共享工厂按需建立和复用。
#[derive(Debug, Clone)]
pub struct CoordinatorNetworkConnection {
    factory: CoordinatorNetworkFactory,
    target_node_id: u64,
}

/// 业务作用: 区分共识信封要调用的远端方法，使序列化、连接和错误转换逻辑只实现一次。
/// 分支说明: 三个分支分别对应日志与心跳、投票、快照安装。
#[derive(Debug, Clone, Copy)]
enum RaftRpcKind {
    Append,
    Vote,
    InstallSnapshot,
}

impl CoordinatorNetworkFactory {
    /// 业务作用: 返回协调 gRPC 的连接和普通请求超时。
    ///
    /// 返回: 从静态协调配置构造的持续时间。
    pub(crate) fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    /// 业务作用: 启动前核对本进程实际发送的叶子证书与静态节点指纹，避免配置复制错误直到远端拒绝心跳后才暴露。
    /// 参数说明:
    /// - `config`: 已完成字段校验的进程级协调配置。
    ///
    /// 返回: 未启用 TLS 时直接成功；启用后只有证书文件首张 DER 证书的 SHA-256 指纹与本节点配置一致才成功。
    pub fn validate_local_tls_identity(
        config: &ProxyClusterCoordinationConfig,
    ) -> anyhow::Result<()> {
        if !config.tls.enabled {
            return Ok(());
        }
        let certificate_path = config
            .tls
            .certificate_path
            .as_deref()
            .context("coordination TLS certificate path is missing")?;
        let certificate_pem = std::fs::read(certificate_path)
            .with_context(|| format!("read coordination TLS certificate `{certificate_path}`"))?;
        let mut reader = certificate_pem.as_slice();
        let certificate = rustls_pemfile::certs(&mut reader)
            .next()
            .transpose()
            .context("parse coordination TLS leaf certificate")?
            .context("coordination TLS certificate file contains no certificate")?;
        let expected = config
            .peers
            .iter()
            .find(|peer| peer.node_id == config.node_id)
            .and_then(|peer| peer.certificate_sha256.as_deref())
            .context("local coordination member has no certificate fingerprint")?;
        let actual: String = Sha256::digest(certificate.as_ref())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if actual != expected.to_ascii_lowercase() {
            anyhow::bail!(
                "local coordination TLS certificate does not match peers[].certificate_sha256"
            );
        }
        Ok(())
    }

    /// 业务作用: 从已经通过启动校验的静态协调配置构造网络工厂，并一次性读取客户端证书材料。
    /// 参数说明:
    /// - `config`: 进程级协调配置，提供集群标识、本地节点、成员地址、RPC 超时和 TLS 文件。
    ///
    /// 返回: 返回可供共识核心克隆使用的网络工厂；证书读取失败时阻止协调器启动。
    pub fn from_config(config: &ProxyClusterCoordinationConfig) -> anyhow::Result<Self> {
        let peers = config
            .peers
            .iter()
            .map(|peer| {
                (
                    peer.node_id,
                    PeerTransport {
                        address: peer.address.clone(),
                        server_name: peer.server_name.clone(),
                    },
                )
            })
            .collect();
        let tls = if config.tls.enabled {
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
            Some(Arc::new(ClientTlsMaterials {
                certificate: std::fs::read(certificate_path).with_context(|| {
                    format!("read coordination TLS certificate `{certificate_path}`")
                })?,
                private_key: std::fs::read(private_key_path).with_context(|| {
                    format!("read coordination TLS private key `{private_key_path}`")
                })?,
                ca_certificate: std::fs::read(client_ca_path).with_context(|| {
                    format!("read coordination TLS client CA `{client_ca_path}`")
                })?,
            }))
        } else {
            None
        };
        Ok(Self {
            cluster_id: Arc::from(wire_cluster_id(config)),
            local_node_id: config.node_id,
            rpc_timeout: Duration::from_millis(config.rpc_timeout_ms),
            peers: Arc::new(peers),
            tls,
            channels: Arc::new(RwLock::new(BTreeMap::new())),
        })
    }

    /// 业务作用: 获取或建立到静态成员的 HTTP/2 通道，并应用连接、请求超时和双向 TLS 身份校验。
    /// 参数说明:
    /// - `target_node_id`: 目标静态成员编号。
    ///
    /// 返回: 返回可复用通道；成员不存在、地址非法或握手失败时返回错误。
    async fn channel(&self, target_node_id: u64) -> anyhow::Result<Channel> {
        if let Some(channel) = self.channels.read().await.get(&target_node_id).cloned() {
            return Ok(channel);
        }
        let peer = self
            .peers
            .get(&target_node_id)
            .with_context(|| format!("coordination peer `{target_node_id}` is not configured"))?;
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}", peer.address);
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .with_context(|| format!("build coordination endpoint `{uri}`"))?
            .connect_timeout(self.rpc_timeout)
            .timeout(self.rpc_timeout);
        if let Some(materials) = &self.tls {
            let server_name = peer.server_name.as_deref().with_context(|| {
                format!("coordination peer `{target_node_id}` has no TLS server name")
            })?;
            let tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(materials.ca_certificate.clone()))
                .identity(Identity::from_pem(
                    materials.certificate.clone(),
                    materials.private_key.clone(),
                ))
                .domain_name(server_name.to_string());
            endpoint = endpoint.tls_config(tls).with_context(|| {
                format!("configure TLS for coordination peer `{target_node_id}`")
            })?;
        }
        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connect coordination peer `{target_node_id}`"))?;
        self.channels
            .write()
            .await
            .insert(target_node_id, channel.clone());
        Ok(channel)
    }

    /// 业务作用: 为双节点协议创建使用同一成员表、超时和 TLS 规则的 gRPC 客户端，避免两套协议出现传输身份差异。
    /// 参数说明:
    /// - `target_node_id`: 双节点集群中的另一成员编号。
    ///
    /// 返回: 返回已连接的协调器客户端；连接或证书校验失败时返回错误。
    pub(crate) async fn pair_client(
        &self,
        target_node_id: u64,
    ) -> anyhow::Result<CoordinatorRpcClient<Channel>> {
        Ok(configure_client(CoordinatorRpcClient::new(
            self.channel(target_node_id).await?,
        )))
    }

    /// 业务作用: 为可能包含磁盘同步和后端预检的配置事务创建不使用普通心跳超时的独立客户端。
    ///
    /// 参数说明:
    /// - `target_node_id`: 要执行 prepare、activate 或提案转发的静态成员编号。
    ///
    /// 返回: 连接阶段仍受 `rpc_timeout_ms` 限制、请求阶段由配置事务自身控制的客户端。
    pub(crate) async fn configuration_client(
        &self,
        target_node_id: u64,
    ) -> anyhow::Result<CoordinatorRpcClient<Channel>> {
        let peer = self
            .peers
            .get(&target_node_id)
            .with_context(|| format!("coordination peer `{target_node_id}` is not configured"))?;
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        let uri = format!("{scheme}://{}", peer.address);
        let mut endpoint = Endpoint::from_shared(uri.clone())
            .with_context(|| format!("build coordination endpoint `{uri}`"))?
            .connect_timeout(self.rpc_timeout);
        if let Some(materials) = &self.tls {
            let server_name = peer.server_name.as_deref().with_context(|| {
                format!("coordination peer `{target_node_id}` has no TLS server name")
            })?;
            endpoint = endpoint
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(Certificate::from_pem(materials.ca_certificate.clone()))
                        .identity(Identity::from_pem(
                            materials.certificate.clone(),
                            materials.private_key.clone(),
                        ))
                        .domain_name(server_name.to_string()),
                )
                .with_context(|| {
                    format!("configure TLS for coordination peer `{target_node_id}`")
                })?;
        }
        let channel = endpoint
            .connect()
            .await
            .with_context(|| format!("connect coordination peer `{target_node_id}`"))?;
        Ok(configure_client(CoordinatorRpcClient::new(channel)))
    }

    /// 业务作用: 将一个共识请求编码进集群隔离信封，调用对应 gRPC 方法，再还原远端业务结果。
    /// 参数说明:
    /// - `target_node_id`: 目标静态成员编号。
    /// - `kind`: 需要调用的共识 RPC 种类。
    /// - `request`: 具体共识请求。
    ///
    /// 返回: 返回远端共识响应；网络不可达、信封错误和远端共识错误分别映射为对应错误类型。
    async fn send<Req, Resp, RemoteErr>(
        &self,
        target_node_id: u64,
        kind: RaftRpcKind,
        request: Req,
    ) -> Result<Resp, CoordinatorRpcError<RemoteErr>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        RemoteErr: Error + DeserializeOwned,
    {
        let payload = serde_json::to_vec(&request)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let channel = self.channel(target_node_id).await.map_err(|error| {
            let source = std::io::Error::other(error.to_string());
            RPCError::Unreachable(Unreachable::new(&source))
        })?;
        let mut client = configure_client(CoordinatorRpcClient::new(channel));
        let envelope = RpcEnvelope {
            cluster_id: self.cluster_id.to_string(),
            sender_node_id: self.local_node_id,
            payload,
            error: String::new(),
        };
        let response = match kind {
            RaftRpcKind::Append => client.raft_append(envelope).await,
            RaftRpcKind::Vote => client.raft_vote(envelope).await,
            RaftRpcKind::InstallSnapshot => client.raft_install_snapshot(envelope).await,
        }
        .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?
        .into_inner();
        // 服务端证书验证的是连接目标，响应信封还必须声明相同静态节点和配置摘要。
        // 该复验能在端口误配、反向代理串线或错误服务复用证书时阻止跨成员响应进入共识核心。
        if response.cluster_id != self.cluster_id.as_ref()
            || response.sender_node_id != target_node_id
        {
            let error = std::io::Error::other(
                "coordination response identity does not match the requested cluster member",
            );
            return Err(RPCError::Network(NetworkError::new(&error)));
        }
        if !response.error.is_empty() {
            let error = std::io::Error::other(response.error);
            return Err(RPCError::Network(NetworkError::new(&error)));
        }
        let result: Result<Resp, openraft::error::RaftError<u64, RemoteErr>> =
            serde_json::from_slice(&response.payload)
                .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        result.map_err(|error| RPCError::RemoteError(RemoteError::new(target_node_id, error)))
    }
}

/// 业务作用: 为所有协调 RPC 客户端统一配置可恢复集群配置和共识快照所需的消息上限。
///
/// 参数说明:
/// - `client`: 已绑定目标成员 HTTP/2 通道的生成客户端。
///
/// 返回: 编码与解码上限一致的协调客户端。
fn configure_client(client: CoordinatorRpcClient<Channel>) -> CoordinatorRpcClient<Channel> {
    client
        .max_decoding_message_size(MAX_COORDINATION_GRPC_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_COORDINATION_GRPC_MESSAGE_BYTES)
}

impl RaftNetworkFactory<CoordinatorTypeConfig> for CoordinatorNetworkFactory {
    type Network = CoordinatorNetworkConnection;

    /// 业务作用: 为共识核心创建指向指定成员的逻辑连接；实际网络通道延迟到首次 RPC 时建立。
    /// 参数说明:
    /// - `target`: 目标静态成员编号。
    /// - `_node`: 共识成员记录；地址以已经通过创世校验的静态配置为准。
    ///
    /// 返回: 返回目标逻辑连接。
    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> CoordinatorNetworkConnection {
        CoordinatorNetworkConnection {
            factory: self.clone(),
            target_node_id: target,
        }
    }
}

impl RaftNetwork<CoordinatorTypeConfig> for CoordinatorNetworkConnection {
    /// 业务作用: 发送日志复制或心跳请求。
    /// 参数说明:
    /// - `request`: 共识日志复制请求。
    /// - `_option`: 共识核心提供的 RPC 选项，传输超时由统一配置控制。
    ///
    /// 返回: 返回远端日志匹配与提交结果。
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<CoordinatorTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, CoordinatorRpcError> {
        self.factory
            .send(self.target_node_id, RaftRpcKind::Append, request)
            .await
    }

    /// 业务作用: 发送完整状态机快照给日志落后的成员。
    /// 参数说明:
    /// - `request`: 分块快照安装请求。
    /// - `_option`: 共识核心提供的 RPC 选项。
    ///
    /// 返回: 返回远端快照安装进度或快照协议错误。
    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<CoordinatorTypeConfig>,
        _option: RPCOption,
    ) -> Result<InstallSnapshotResponse<u64>, CoordinatorRpcError<InstallSnapshotError>> {
        self.factory
            .send(self.target_node_id, RaftRpcKind::InstallSnapshot, request)
            .await
    }

    /// 业务作用: 发送预投票或正式投票请求，远端只有在持久化投票后才会响应成功。
    /// 参数说明:
    /// - `request`: 共识投票请求。
    /// - `_option`: 共识核心提供的 RPC 选项。
    ///
    /// 返回: 返回远端投票决定和日志新旧判断。
    async fn vote(
        &mut self,
        request: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, CoordinatorRpcError> {
        self.factory
            .send(self.target_node_id, RaftRpcKind::Vote, request)
            .await
    }
}
