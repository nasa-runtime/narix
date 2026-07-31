/// 业务作用: 收纳协议文件生成的 gRPC 消息与服务定义；业务模块只通过该命名空间访问线上的稳定字段编号。
pub mod wire {
    tonic::include_proto!("narix.coordinator.v1");
}

/// 协调成员 RPC 客户端、静态身份和超时边界。
pub mod network;
/// 恰好两个静态成员时使用的严格 2/2 复制与竞选协议。
pub mod pair;
/// 协调 gRPC 服务端和入站证书身份绑定。
pub mod rpc;
/// 协调器启动、统一业务句柄和后台权威监控。
pub mod runtime;
/// 托管复制 HA 观察、动作和路由结果的确定性状态机。
pub mod state;
/// 任期、日志、状态机、快照和双节点哈希链的原子持久化。
pub mod store;

use std::io::Cursor;

use crate::config::ProxyClusterCoordinationConfig;
use sha2::{Digest, Sha256};
use state::{CoordinationCommand, CoordinationResponse};

openraft::declare_raft_types!(
    /// 业务作用: 固定协调共识实例使用的命令、响应、节点编号、日志、快照和异步运行时类型。
    /// 字段说明: 显式类型配置让网络层、持久化层与状态机在编译期共享同一协议边界。
    pub CoordinatorTypeConfig:
        D = CoordinationCommand,
        R = CoordinationResponse,
);

/// 业务作用: 固定多数派共识运行时类型，供网络服务、业务句柄和进程生命周期共享同一实例。
pub type CoordinatorRaft = openraft::Raft<CoordinatorTypeConfig>;

/// 协调 gRPC 最大消息尺寸。
///
/// 集群配置正文上限为 16 MiB；共识快照还会把该正文包进 JSON 信封。该上限只控制
/// 实际收到的单条消息，不会在建连时预分配同等内存。
pub(crate) const MAX_COORDINATION_GRPC_MESSAGE_BYTES: usize = 192 * 1024 * 1024;

/// 业务作用: 对所有成员必须一致的协调参数生成稳定摘要，防止超时、证书身份表或隔离策略不同的节点互相交换协议状态。
/// 参数说明:
/// - `config`: 已通过启动校验的当前节点协调配置；本机路径和本机编号不会进入集群级摘要。
///
/// 返回: 返回六十四位小写十六进制 SHA-256 摘要。
pub(crate) fn cluster_configuration_digest(config: &ProxyClusterCoordinationConfig) -> String {
    let mut hasher = Sha256::new();
    let fields = [
        ("bootstrap_node_id", config.bootstrap_node_id.to_string()),
        (
            "heartbeat_interval_ms",
            config.heartbeat_interval_ms.to_string(),
        ),
        (
            "election_timeout_min_ms",
            config.election_timeout_min_ms.to_string(),
        ),
        (
            "election_timeout_max_ms",
            config.election_timeout_max_ms.to_string(),
        ),
        ("rpc_timeout_ms", config.rpc_timeout_ms.to_string()),
        (
            "route_guard_timeout_ms",
            config.route_guard_timeout_ms.to_string(),
        ),
        ("action_timeout_ms", config.action_timeout_ms.to_string()),
        (
            "snapshot_log_entries",
            config.snapshot_log_entries.to_string(),
        ),
        (
            "pair_election_retry_ms",
            config.pair_election_retry_ms.to_string(),
        ),
        (
            "pair_allow_single_survivor",
            config.pair_allow_single_survivor.to_string(),
        ),
        (
            "pair_peer_fencing_timeout_ms",
            config.pair_peer_fencing_timeout_ms.to_string(),
        ),
        ("tls_enabled", config.tls.enabled.to_string()),
    ];
    for (name, value) in fields {
        update_configuration_digest(&mut hasher, name, value.as_bytes());
    }

    let mut peers = config.peers.iter().collect::<Vec<_>>();
    peers.sort_by_key(|peer| peer.node_id);
    for peer in peers {
        let prefix = format!("peer.{}", peer.node_id);
        update_configuration_digest(
            &mut hasher,
            &format!("{prefix}.address"),
            peer.address.as_bytes(),
        );
        update_configuration_digest(
            &mut hasher,
            &format!("{prefix}.server_name"),
            peer.server_name.as_deref().unwrap_or_default().as_bytes(),
        );
        let fingerprint = peer
            .certificate_sha256
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase();
        update_configuration_digest(
            &mut hasher,
            &format!("{prefix}.certificate_sha256"),
            fingerprint.as_bytes(),
        );
    }
    for (index, part) in config.pair_peer_fencing_command.iter().enumerate() {
        update_configuration_digest(
            &mut hasher,
            &format!("pair_peer_fencing_command.{index}"),
            part.as_bytes(),
        );
    }
    Sha256Hex::from_digest(hasher.finalize()).0
}

/// 业务作用: 构造内部协议使用的完整集群身份，把用户标识与严格配置摘要同时绑定到每个请求。
/// 参数说明:
/// - `config`: 当前节点协调配置。
///
/// 返回: 返回只在线上协议中使用的集群身份；运维配置和隔离命令仍使用原始 `cluster_id`。
pub(crate) fn wire_cluster_id(config: &ProxyClusterCoordinationConfig) -> String {
    format!(
        "{}:{}",
        config.cluster_id,
        cluster_configuration_digest(config)
    )
}

/// 业务作用: 用字段名和长度前缀更新配置摘要，避免不同字段拼接后产生边界歧义。
/// 参数说明:
/// - `hasher`: 当前增量 SHA-256 状态。
/// - `name`: 不随配置顺序变化的规范字段名。
/// - `value`: 字段的规范字节值。
///
/// 返回: 无显式返回值；摘要状态被原地更新。
fn update_configuration_digest(hasher: &mut Sha256, name: &str, value: &[u8]) {
    let name_length = u64::try_from(name.len()).unwrap_or(u64::MAX);
    let value_length = u64::try_from(value.len()).unwrap_or(u64::MAX);
    hasher.update(name_length.to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update(value_length.to_be_bytes());
    hasher.update(value);
}

/// 业务作用: 把固定长度摘要编码成无分隔符的小写十六进制文本。
/// 字段说明: 新类型限制转换入口，避免多处手写格式化出现大小写差异。
struct Sha256Hex(String);

impl Sha256Hex {
    /// 业务作用: 编码 SHA-256 输出字节。
    /// 参数说明:
    /// - `digest`: SHA-256 完成后得到的固定长度字节序列。
    ///
    /// 返回: 返回小写十六进制包装值。
    fn from_digest(digest: impl AsRef<[u8]>) -> Self {
        Self(
            digest
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
    }
}
