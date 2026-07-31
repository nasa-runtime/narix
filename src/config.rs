use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use config::{Config as ConfigSource, File, FileFormat};
use serde::Deserialize;
use serde_json::{Map, Number, Value};

use crate::backend::BackendProfile;

/// 未显式指定外部配置时加载的仓库内基础配置路径。
pub const DEFAULT_APPLICATION_CONFIG: &str = "zcf/application.yml";
const NARIX_PROFILE_ENV: &str = "NARIX_PROFILE";
const NARIX_ENV_PREFIX: &str = "NARIX_";
const PLACEHOLDER_MAX_PASSES: usize = 16;

/// 业务作用: 保存管理端、协调集群和全部代理项，是配置合并、校验与运行时组装的根对象。
#[derive(Debug, Clone, Deserialize)]
// 表示 ProxyConfig 的数据结构，用于加载、解析和校验 narix 配置。
pub struct ProxyConfig {
    /// 管理端监听、认证和健康检查配置。
    #[serde(default)]
    pub admin: AdminConfig,
    /// 配置来源轮询、防抖和单次事务超时配置。
    #[serde(default)]
    pub reload: ReloadConfig,
    /// 当前进程加入 narix 协调集群所需的成员与传输配置。
    #[serde(default)]
    pub coordination: ProxyClusterCoordinationConfig,
    /// 当前进程要启动的全部业务代理项；最终为空时启动校验会拒绝运行。
    #[serde(default)]
    pub proxies: Vec<ProxyItem>,
}

/// 业务作用: 保存 narix 进程级协调集群配置；同一进程内的多个 replication HA 代理项共享一套成员关系、任期和复制日志。
/// 字段说明: `peers` 是不可由存活节点数改写的静态成员表；成员总数为二时选择双节点协议，至少为三时选择多数派共识协议。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyClusterCoordinationConfig {
    /// 是否启动当前进程的协调服务和持久化协议。
    pub enabled: bool,
    /// 同一协调集群所有成员共享且永久不变的业务标识。
    pub cluster_id: String,
    /// 当前成员在静态成员表中的稳定正整数编号。
    pub node_id: u64,
    /// 当前进程接受协调 RPC 的本地 IP 与端口。
    pub listen: String,
    /// 其他静态成员访问当前节点时使用的 IP 与端口。
    pub advertise: String,
    /// 当前节点独占的任期、日志、状态机和快照目录。
    pub data_dir: String,
    /// 首次初始化多数派成员关系的固定成员编号。
    pub bootstrap_node_id: u64,
    /// 决定协议类型且写入创世状态的完整静态成员表。
    pub peers: Vec<ProxyClusterPeerConfig>,
    /// 领导者心跳发送周期，单位为毫秒。
    pub heartbeat_interval_ms: u64,
    /// 随机选举超时下界，单位为毫秒。
    pub election_timeout_min_ms: u64,
    /// 随机选举超时上界，单位为毫秒。
    pub election_timeout_max_ms: u64,
    /// 单次成员间 RPC 的最长等待时间，单位为毫秒。
    pub rpc_timeout_ms: u64,
    /// 未确认领导权或对端联系后仍可沿用路由的最长保护窗口。
    pub route_guard_timeout_ms: u64,
    /// 一次外部拓扑或持久化动作令牌的有效期。
    pub action_timeout_ms: u64,
    /// 多数派模式触发日志快照的已提交条目数量。
    pub snapshot_log_entries: u64,
    /// 双节点竞选失败后的重试间隔，单位为毫秒。
    pub pair_election_retry_ms: u64,
    /// 双节点失联后是否允许先隔离对端代理再单存活接管。
    pub pair_allow_single_survivor: bool,
    /// 双节点单存活接管前执行的对端代理隔离程序及独立参数。
    pub pair_peer_fencing_command: Vec<String>,
    /// 对端代理隔离程序的最长执行时间，单位为毫秒。
    pub pair_peer_fencing_timeout_ms: u64,
    /// 协调端口的双向 TLS 与证书文件配置。
    pub tls: ProxyClusterTlsConfig,
}

impl Default for ProxyClusterCoordinationConfig {
    /// 业务作用: 提供不启用进程级协调器时的安全默认值，使旧配置保持兼容且不会额外开放网络监听。
    /// 参数说明: 无。
    /// 返回: 返回禁用状态的协调配置；启用时调用方必须显式填写集群标识、成员和存储目录。
    fn default() -> Self {
        Self {
            enabled: false,
            cluster_id: String::new(),
            node_id: 0,
            listen: "127.0.0.1:19090".to_string(),
            advertise: "127.0.0.1:19090".to_string(),
            data_dir: String::new(),
            bootstrap_node_id: 0,
            peers: Vec::new(),
            heartbeat_interval_ms: 500,
            election_timeout_min_ms: 1_500,
            election_timeout_max_ms: 3_000,
            rpc_timeout_ms: 1_000,
            route_guard_timeout_ms: 5_000,
            action_timeout_ms: 30_000,
            snapshot_log_entries: 10_000,
            pair_election_retry_ms: 500,
            pair_allow_single_survivor: false,
            pair_peer_fencing_command: Vec::new(),
            pair_peer_fencing_timeout_ms: 5_000,
            tls: ProxyClusterTlsConfig::default(),
        }
    }
}

impl ProxyClusterCoordinationConfig {
    /// 业务作用: 根据配置文件中的静态成员总数选择双节点或多数派协议；该结果随后写入本地创世信息，不能随在线节点数变化。
    /// 返回: 两个成员返回双节点协议，至少三个成员返回多数派协议；成员数不足时返回配置错误。
    pub fn protocol(&self) -> anyhow::Result<ProxyClusterProtocol> {
        match self.peers.len() {
            2 => Ok(ProxyClusterProtocol::Pair),
            count if count >= 3 => Ok(ProxyClusterProtocol::Quorum),
            count => bail!("coordination.peers requires at least 2 static members, got {count}"),
        }
    }

    /// 业务作用: 校验进程级协调器的成员、计时、持久化和传输安全约束，避免节点以不一致参数加入同一集群。
    /// 返回: 配置满足安全约束时返回成功，否则返回指向具体字段的启动错误。
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            if !self.peers.is_empty() {
                bail!("coordination.peers requires coordination.enabled=true");
            }
            return Ok(());
        }
        if self.cluster_id.trim().is_empty() {
            bail!("coordination.cluster_id must not be empty");
        }
        if self.node_id == 0 {
            bail!("coordination.node_id must be > 0");
        }
        if self.bootstrap_node_id == 0 {
            bail!("coordination.bootstrap_node_id must be > 0");
        }
        if self.data_dir.trim().is_empty() {
            bail!("coordination.data_dir must not be empty");
        }
        let listen: SocketAddr = self
            .listen
            .parse()
            .with_context(|| format!("coordination.listen `{}`", self.listen))?;
        let advertise: SocketAddr = self
            .advertise
            .parse()
            .with_context(|| format!("coordination.advertise `{}`", self.advertise))?;
        if advertise.ip().is_unspecified() {
            bail!("coordination.advertise must not use an unspecified address");
        }
        if listen.port() != advertise.port() {
            bail!("coordination.listen and coordination.advertise must use the same port");
        }
        if !self.tls.enabled && (!listen.ip().is_loopback() || !advertise.ip().is_loopback()) {
            bail!("coordination without TLS may only use loopback listen and advertise addresses");
        }

        let protocol = self.protocol()?;
        let mut node_ids = HashSet::new();
        let mut addresses = HashSet::new();
        let mut local_member = None;
        let mut bootstrap_found = false;
        for peer in &self.peers {
            if peer.node_id == 0 {
                bail!("coordination.peers[].node_id must be > 0");
            }
            if !node_ids.insert(peer.node_id) {
                bail!(
                    "coordination.peers contains duplicate node_id `{}`",
                    peer.node_id
                );
            }
            let peer_addr: SocketAddr = peer.address.parse().with_context(|| {
                format!(
                    "coordination peer `{}` address `{}`",
                    peer.node_id, peer.address
                )
            })?;
            if peer_addr.ip().is_unspecified() {
                bail!(
                    "coordination peer `{}` address must not be unspecified",
                    peer.node_id
                );
            }
            if !self.tls.enabled && !peer_addr.ip().is_loopback() {
                bail!(
                    "coordination peer `{}` requires TLS because its address is not loopback",
                    peer.node_id
                );
            }
            if !addresses.insert(peer_addr) {
                bail!(
                    "coordination.peers contains duplicate address `{}`",
                    peer.address
                );
            }
            if peer.node_id == self.node_id {
                local_member = Some(peer_addr);
            }
            if peer.node_id == self.bootstrap_node_id {
                bootstrap_found = true;
            }
            if self.tls.enabled
                && peer
                    .server_name
                    .as_deref()
                    .is_none_or(|name| name.trim().is_empty())
            {
                bail!(
                    "coordination peer `{}` requires server_name when TLS is enabled",
                    peer.node_id
                );
            }
            if self.tls.enabled {
                let fingerprint = peer.certificate_sha256.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "coordination peer `{}` requires certificate_sha256 when TLS is enabled",
                        peer.node_id
                    )
                })?;
                if !valid_sha256_fingerprint(fingerprint) {
                    bail!(
                        "coordination peer `{}` certificate_sha256 must contain exactly 64 hexadecimal characters",
                        peer.node_id
                    );
                }
            } else {
                if peer.server_name.is_some() {
                    bail!(
                        "coordination peer `{}` server_name requires TLS to be enabled",
                        peer.node_id
                    );
                }
                if peer.certificate_sha256.is_some() {
                    bail!(
                        "coordination peer `{}` certificate_sha256 requires TLS to be enabled",
                        peer.node_id
                    );
                }
            }
        }
        if local_member != Some(advertise) {
            bail!(
                "coordination.node_id `{}` must map to advertise address `{}` in peers",
                self.node_id,
                self.advertise
            );
        }
        if !bootstrap_found {
            bail!(
                "coordination.bootstrap_node_id `{}` is not present in peers",
                self.bootstrap_node_id
            );
        }
        if self.heartbeat_interval_ms == 0 {
            bail!("coordination.heartbeat_interval_ms must be > 0");
        }
        if self.election_timeout_min_ms <= self.heartbeat_interval_ms.saturating_mul(2) {
            bail!(
                "coordination.election_timeout_min_ms must be greater than twice heartbeat_interval_ms"
            );
        }
        if self.election_timeout_max_ms <= self.election_timeout_min_ms {
            bail!(
                "coordination.election_timeout_max_ms must be greater than election_timeout_min_ms"
            );
        }
        if self.rpc_timeout_ms == 0 || self.rpc_timeout_ms >= self.election_timeout_min_ms {
            bail!("coordination.rpc_timeout_ms must be > 0 and less than election_timeout_min_ms");
        }
        if self.route_guard_timeout_ms <= self.election_timeout_max_ms {
            bail!(
                "coordination.route_guard_timeout_ms must be greater than election_timeout_max_ms"
            );
        }
        if self.action_timeout_ms <= self.route_guard_timeout_ms {
            bail!("coordination.action_timeout_ms must be greater than route_guard_timeout_ms");
        }
        if self.snapshot_log_entries == 0 {
            bail!("coordination.snapshot_log_entries must be > 0");
        }
        if self.pair_election_retry_ms == 0 {
            bail!("coordination.pair_election_retry_ms must be > 0");
        }
        if self.pair_peer_fencing_timeout_ms == 0 {
            bail!("coordination.pair_peer_fencing_timeout_ms must be > 0");
        }
        if protocol == ProxyClusterProtocol::Pair
            && self.pair_allow_single_survivor
            && self.pair_peer_fencing_command.is_empty()
        {
            bail!(
                "coordination.pair_allow_single_survivor=true requires pair_peer_fencing_command"
            );
        }
        if !self.pair_allow_single_survivor && !self.pair_peer_fencing_command.is_empty() {
            bail!(
                "coordination.pair_peer_fencing_command requires pair_allow_single_survivor=true"
            );
        }
        if protocol != ProxyClusterProtocol::Pair
            && (self.pair_allow_single_survivor || !self.pair_peer_fencing_command.is_empty())
        {
            bail!(
                "coordination pair single-survivor fields are only valid with exactly 2 static members"
            );
        }
        if self
            .pair_peer_fencing_command
            .iter()
            .any(|part| part.trim().is_empty())
        {
            bail!("coordination.pair_peer_fencing_command must not contain empty parts");
        }
        self.tls.validate()?;
        Ok(())
    }
}

/// 业务作用: 固化协调集群使用的安全协议，防止三成员集群只剩两个存活节点时错误退化为双节点协议。
/// 分支说明: `Pair` 需要 2/2 确认；`Quorum` 始终依据创世成员表计算多数派。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyClusterProtocol {
    /// 恰好两个静态成员，所有普通控制写必须双方落盘确认。
    Pair,
    /// 至少三个静态成员，所有控制写必须由创世成员多数派提交。
    Quorum,
}

/// 业务作用: 描述协调集群中的一个静态成员，节点编号和地址会写入创世信息，运行期间不得因节点暂时离线而删除。
/// 字段说明: `server_name` 校验服务端名称；`certificate_sha256` 把客户端证书的 DER 指纹固定到节点编号，二者共同阻止合法成员冒用其他节点身份。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyClusterPeerConfig {
    /// 成员稳定编号，也是证书指纹绑定和协议消息身份。
    pub node_id: u64,
    /// 其他成员访问该节点时使用的协调 IP 与端口。
    pub address: String,
    #[serde(default)]
    /// TLS 开启时用于校验该成员服务端证书的唯一名称。
    pub server_name: Option<String>,
    #[serde(default)]
    /// TLS 开启时绑定该成员身份的叶子证书 DER SHA-256 指纹。
    pub certificate_sha256: Option<String>,
}

/// 业务作用: 保存协调端口的双向 TLS 配置，使节点身份同时受到静态成员表和证书链约束。
/// 字段说明: 启用后证书、私钥和信任根必须同时提供；禁用时协调监听、通告和全部成员地址都被限制为本机回环地址。
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ProxyClusterTlsConfig {
    /// 是否同时启用服务端与客户端证书校验。
    pub enabled: bool,
    /// 当前节点证书链的 PEM 文件路径。
    pub certificate_path: Option<String>,
    /// 当前节点私钥的 PEM 文件路径。
    pub private_key_path: Option<String>,
    /// 用于校验成员客户端证书的信任根 PEM 文件路径。
    pub client_ca_path: Option<String>,
}

impl ProxyClusterTlsConfig {
    /// 业务作用: 校验协调端口的证书配置是否成套，防止只启用单向认证或漏配私钥后以明文方式意外启动。
    /// 返回: 禁用或三个证书路径均为非空值时返回成功，否则返回启动错误。
    fn validate(&self) -> anyhow::Result<()> {
        let fields = [
            ("certificate_path", self.certificate_path.as_deref()),
            ("private_key_path", self.private_key_path.as_deref()),
            ("client_ca_path", self.client_ca_path.as_deref()),
        ];
        if !self.enabled {
            if fields.iter().any(|(_, value)| value.is_some()) {
                bail!("coordination.tls certificate fields require tls.enabled=true");
            }
            return Ok(());
        }
        for (name, value) in fields {
            if value.is_none_or(|path| path.trim().is_empty()) {
                bail!("coordination.tls.{name} is required when TLS is enabled");
            }
        }
        Ok(())
    }
}

/// 业务作用: 校验配置中的证书指纹是否为规范的 SHA-256 十六进制文本，避免运行期因分隔符或截断产生身份歧义。
/// 参数说明:
/// - `fingerprint`: 待校验的证书 DER 摘要，只接受不带冒号的六十四位十六进制文本。
///
/// 返回: 长度和字符集合都符合要求时返回 `true`。
fn valid_sha256_fingerprint(fingerprint: &str) -> bool {
    fingerprint.len() == 64 && fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// 业务作用: 保存管理监听、令牌和匿名访问策略，决定管理服务是否启动及受保护端点认证边界。
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
// 表示 AdminConfig 的数据结构，用于加载、解析和校验 narix 配置。
pub struct AdminConfig {
    /// 是否启动管理端 HTTP 服务。
    pub enabled: bool,
    /// 管理端绑定的 IP 与端口。
    pub listen: String,
    /// 受保护管理接口使用的 Bearer 令牌；空值表示没有可用令牌。
    pub token: Option<String>,
    /// 是否显式允许不携带令牌访问受保护管理接口。
    #[serde(default)]
    pub allow_unauthenticated: bool,
}

impl Default for AdminConfig {
    /// 业务作用: 返回默认开启回环管理监听但不允许匿名访问的安全管理配置。
    /// 参数说明: 无。
    fn default() -> Self {
        Self {
            enabled: true,
            listen: "127.0.0.1:18080".to_string(),
            token: None,
            allow_unauthenticated: false,
        }
    }
}

/// 配置热更新的来源轮询、防抖和事务超时配置。
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ReloadConfig {
    /// 是否监控配置来源和证书依赖内容；管理接口和 SIGHUP 仍受该总开关约束。
    pub enabled: bool,
    /// 两次自动内容检查之间的等待时间，单位为毫秒。
    pub poll_interval_ms: u64,
    /// 同一候选摘要连续出现多少次后才允许自动提交。
    pub stable_observations: u32,
    /// 一次完整加载、预检、集群提交和本地发布允许持续的最长时间，单位为毫秒。
    pub transaction_timeout_ms: u64,
}

impl Default for ReloadConfig {
    /// 业务作用: 返回适合本地文件和 ConfigMap 符号链接轮换的安全热更新默认值。
    fn default() -> Self {
        Self {
            enabled: true,
            poll_interval_ms: 1_000,
            stable_observations: 2,
            transaction_timeout_ms: 60_000,
        }
    }
}

impl ReloadConfig {
    /// 业务作用: 校验热更新轮询、防抖和事务超时，避免零间隔忙轮询或未防抖发布半写文件。
    ///
    /// 返回: 配置满足运行约束时返回成功，否则返回具体字段错误。
    fn validate(&self) -> anyhow::Result<()> {
        if self.transaction_timeout_ms == 0 {
            bail!("reload.transaction_timeout_ms must be > 0");
        }
        if self.transaction_timeout_ms > 86_400_000 {
            bail!("reload.transaction_timeout_ms must be <= 86400000");
        }
        if !self.enabled {
            return Ok(());
        }
        if self.poll_interval_ms == 0 {
            bail!("reload.poll_interval_ms must be > 0");
        }
        if self.stable_observations < 2 {
            bail!("reload.stable_observations must be >= 2");
        }
        Ok(())
    }
}

/// 中立托管复制 HA 的探测、切换和产品持久化配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationHaConfig {
    /// 后台探测全部数据节点的周期，单位为毫秒。
    pub probe_interval_ms: u64,
    /// HA 探测建连的独立超时；省略时继承代理普通后端建连超时。
    pub probe_connect_timeout_ms: Option<u64>,
    /// HA 探测响应的独立超时；省略时继承代理普通后端请求超时。
    pub probe_request_timeout_ms: Option<u64>,
    /// 当前主节点连续失败达到多少轮后形成故障判定。
    pub master_down_after_failures: u64,
    /// 是否允许在 fencing 和 coordinator 门禁满足后自动提升副本。
    pub auto_failover: bool,
    /// 观察者模式：只探测并跟随已经证明收敛的主节点，绝不发送任何后端控制命令
    /// （不切主、不降级归队旧主、不修复副本漂移、不做持久化编排）；主故障、
    /// 未确认新主或双主时关闭路由。手动拓扑接口一并拒绝，隐含
    /// `auto_failover=false`。该字段只限制 HA 控制面；旁路业务只读还需 `read_only=true`。
    pub observer: bool,
    /// 允许参与提升的副本最大秒级延迟；零表示不设数值上限。
    pub max_replica_lag: u64,
    /// 旧主基础设施隔离配置。
    pub fencing: ReplicationHaFencingConfig,
    /// HA 动作租约配置。
    pub coordinator: ReplicationHaCoordinatorConfig,
    /// 产品动态持久化配置。
    pub persistence: ReplicationHaPersistenceConfig,
}

impl Default for ReplicationHaConfig {
    /// 业务作用: 返回中立托管复制 HA 的安全默认值。
    fn default() -> Self {
        Self {
            probe_interval_ms: 1_000,
            probe_connect_timeout_ms: None,
            probe_request_timeout_ms: None,
            master_down_after_failures: 3,
            auto_failover: true,
            observer: false,
            max_replica_lag: 0,
            fencing: ReplicationHaFencingConfig::default(),
            coordinator: ReplicationHaCoordinatorConfig::default(),
            persistence: ReplicationHaPersistenceConfig::default(),
        }
    }
}

/// 中立托管复制 HA 的旧主隔离配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationHaFencingConfig {
    /// 不执行外部命令的软隔离，或由外部基础设施完成的强隔离模式。
    pub mode: HaFencingMode,
    /// 外部隔离程序及其独立参数。
    pub command: Vec<String>,
    /// 外部隔离程序的最长执行时间，单位为毫秒。
    pub timeout_ms: u64,
}

impl Default for ReplicationHaFencingConfig {
    /// 业务作用: 返回不执行外部命令、依赖切主后降级旧主的软隔离默认值。
    fn default() -> Self {
        Self {
            mode: HaFencingMode::Manual,
            command: Vec::new(),
            timeout_ms: 10_000,
        }
    }
}

/// 中立托管复制 HA 的动作租约和多 NARIX 协调配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationHaCoordinatorConfig {
    /// 动作协调模式；省略时按 `coordination.enabled` 自动选择：组集群时为 `proxy-cluster`（leader 切主），
    /// 单实例时为 `none`（进程内串行）。显式写 `none`/`local-file`/`redis` 且已组集群会被拒绝。
    pub mode: Option<HaCoordinatorMode>,
    /// 当前 NARIX 实例参与 HA 动作时使用的稳定身份。
    pub owner_id: Option<String>,
    /// 本地文件租约模式使用的租约路径。
    pub lease_path: Option<String>,
    /// 内置协调集群中标识同一后端拓扑的稳定组名。
    pub group_id: Option<String>,
    /// 内置协调集群批准故障转移所需的观察票数；零表示静态成员多数派。
    pub observation_quorum: usize,
    /// 相同故障指纹参与计票的时间窗口，单位为毫秒。
    pub observation_window_ms: u64,
    /// 独立 Redis 租约服务地址，不能指向被当前 HA 拓扑管理的数据节点。
    pub redis_addr: Option<String>,
    /// 独立 Redis 租约服务的可选 ACL 用户名。
    pub redis_username: Option<String>,
    /// 独立 Redis 租约服务的可选认证密码。
    pub redis_password: Option<String>,
    /// 同一 HA 拓扑共享的独立 Redis 租约键。
    pub redis_key: Option<String>,
    /// 独立 Redis 租约服务建连超时，单位为毫秒。
    pub redis_connect_timeout_ms: u64,
    /// 独立 Redis 租约命令响应超时，单位为毫秒。
    pub redis_request_timeout_ms: u64,
    /// 租约有效期，单位为毫秒。
    pub lease_ttl_ms: u64,
    /// 已持有租约的续约周期，单位为毫秒。
    pub lease_renew_interval_ms: u64,
    /// 单轮竞争租约的最长等待时间，单位为毫秒。
    pub lease_acquire_timeout_ms: u64,
}

impl ReplicationHaCoordinatorConfig {
    /// 业务作用: 解析实际协调模式：省略时组集群自动 `proxy-cluster`、单实例自动 `none`；显式值原样返回。
    ///
    /// 参数说明:
    /// - `coordination_enabled`: 当前进程是否启用了内置协调集群。
    pub fn effective_mode(&self, coordination_enabled: bool) -> HaCoordinatorMode {
        self.mode.unwrap_or(if coordination_enabled {
            HaCoordinatorMode::ProxyCluster
        } else {
            HaCoordinatorMode::None
        })
    }

    /// 业务作用: 返回内置协调集群使用的组标识；省略时默认使用代理项名称（集群内各成员同名代理天然一致且唯一）。
    ///
    /// 参数说明:
    /// - `proxy_name`: 当前 replication-ha 代理项名称。
    pub fn effective_group_id(&self, proxy_name: &str) -> String {
        self.group_id
            .clone()
            .unwrap_or_else(|| proxy_name.to_string())
    }
}

impl Default for ReplicationHaCoordinatorConfig {
    /// 业务作用: 返回不启用动作租约的安全默认值。
    fn default() -> Self {
        Self {
            mode: None,
            owner_id: None,
            lease_path: None,
            group_id: None,
            observation_quorum: 0,
            observation_window_ms: 3_000,
            redis_addr: None,
            redis_username: None,
            redis_password: None,
            redis_key: None,
            redis_connect_timeout_ms: 3_000,
            redis_request_timeout_ms: 3_000,
            lease_ttl_ms: 15_000,
            lease_renew_interval_ms: 5_000,
            lease_acquire_timeout_ms: 3_000,
        }
    }
}

/// 中立 HA 状态机使用的产品持久化入口。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReplicationHaPersistenceConfig {
    /// 是否由 NARIX 动态分配和迁移持久化 owner。
    pub enabled: bool,
    /// 持久化目标无法安全收敛时是否关闭业务路由。
    pub fail_closed: bool,
    /// owner 迁移时是否立即生成后端原生持久化点。
    pub force_checkpoint_on_handoff: bool,
    /// 副本稳定达到多少毫秒后才允许承担持久化 owner。
    pub min_replica_stable_ms: u64,
    /// 分配持久化 owner 前是否要求副本复制延迟为零。
    pub require_zero_lag: bool,
    /// Dragonfly 的动态快照配置；其他产品不得读取该字段。
    pub dragonfly: Option<DragonflyHaPersistenceConfig>,
    /// Redis 8.2.2 的 RDB/AOF 配置；其他产品不得读取该字段。
    pub redis: Option<RedisHaPersistenceConfig>,
    /// Valkey 9.1.1 的 RDB/AOF 配置；其他产品不得读取该字段。
    pub valkey: Option<RedisHaPersistenceConfig>,
    /// Redict 7.3.6 的 RDB/AOF 配置；其他产品不得读取该字段。
    pub redict: Option<RedisHaPersistenceConfig>,
    /// KeyDB 6.3.4 的 RDB/AOF 配置；其他产品不得读取该字段。
    pub keydb: Option<RedisHaPersistenceConfig>,
}

impl Default for ReplicationHaPersistenceConfig {
    /// 业务作用: 返回默认关闭动态持久化编排的配置。
    fn default() -> Self {
        Self {
            enabled: false,
            fail_closed: true,
            force_checkpoint_on_handoff: true,
            min_replica_stable_ms: 5_000,
            require_zero_lag: true,
            dragonfly: None,
            redis: None,
            valkey: None,
            redict: None,
            keydb: None,
        }
    }
}

/// Dragonfly 的原生动态快照配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DragonflyHaPersistenceConfig {
    /// 持久化 owner 节点使用的 `snapshot_cron`。
    pub snapshot_cron: String,
    /// 快照文件名模板。
    pub dbfilename: String,
    /// 可选的快照目录模板；Dragonfly 1.39.0 要求节点启动时已配置为该值。
    pub dir: Option<String>,
    /// 强制快照完成校验的最长等待时间，单位为毫秒。
    pub verify_timeout_ms: u64,
}

impl Default for DragonflyHaPersistenceConfig {
    /// 业务作用: 返回 Dragonfly 原生动态快照的默认配置。
    fn default() -> Self {
        Self {
            snapshot_cron: "*/5 * * * *".to_string(),
            dbfilename: "dump-{role}-{client_addr_safe}-{timestamp}".to_string(),
            dir: None,
            verify_timeout_ms: 30_000,
        }
    }
}

/// Redis 8.2.2 的动态 RDB/AOF 配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisHaPersistenceConfig {
    /// Redis 周期 RDB 配置。
    pub rdb: RedisHaRdbConfig,
    /// Redis AOF 配置。
    pub aof: RedisHaAofConfig,
    /// 强制持久化点完成校验的最长等待时间，单位为毫秒。
    pub verify_timeout_ms: u64,
}

impl Default for RedisHaPersistenceConfig {
    /// 业务作用: 返回同时启用 RDB 和 everysec AOF 的 Redis 默认值。
    fn default() -> Self {
        Self {
            rdb: RedisHaRdbConfig::default(),
            aof: RedisHaAofConfig::default(),
            verify_timeout_ms: 10_000,
        }
    }
}

/// Redis 周期 RDB 规则集合。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisHaRdbConfig {
    /// 是否在持久化 owner 节点启用周期 RDB。
    pub enabled: bool,
    /// 交给 Redis `save` 配置的触发规则。
    pub save_rules: Vec<RedisHaRdbSaveRule>,
}

impl Default for RedisHaRdbConfig {
    /// 业务作用: 返回五分钟内发生十次变更时生成 RDB 的默认规则。
    fn default() -> Self {
        Self {
            enabled: true,
            save_rules: vec![RedisHaRdbSaveRule {
                seconds: 300,
                changes: 10,
            }],
        }
    }
}

/// 一条 Redis RDB 周期保存规则。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisHaRdbSaveRule {
    /// 统计数据变更的时间窗口，单位为秒。
    pub seconds: u64,
    /// 时间窗口内触发 RDB 所需的最少变更次数。
    pub changes: u64,
}

/// Redis AOF 动态配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RedisHaAofConfig {
    /// 是否在持久化 owner 节点启用 AOF。
    pub enabled: bool,
    /// Redis `appendfsync` 使用的 `always`、`everysec` 或 `no`。
    pub fsync: String,
}

impl Default for RedisHaAofConfig {
    /// 业务作用: 返回启用 everysec AOF 的默认配置。
    fn default() -> Self {
        Self {
            enabled: true,
            fsync: "everysec".to_string(),
        }
    }
}

/// 业务作用: 保存单个代理的模式、监听、后端、认证、容量、协议和可选 HA 配置。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
// 表示 ProxyItem 的数据结构，用于加载、解析和校验 narix 配置。
pub struct ProxyItem {
    /// 代理项唯一名称，用于日志、指标、状态和协调分组。
    pub name: String,
    /// 后端产品兼容档案；省略时为兼容旧配置默认使用 Redis。
    #[serde(default)]
    pub backend_profile: BackendProfile,
    /// 当前代理项的数据面工作模式。
    pub mode: ProxyMode,
    /// 中立 `replication-ha` 模式的探测、切换和产品持久化配置。
    pub ha: Option<ReplicationHaConfig>,
    /// 业务监听端口；节点代理模式改由各 `node_proxies` 项指定端口。
    #[serde(default)]
    pub listen: u16,
    /// 单节点模式使用的后端主机名或 IP。
    pub host: Option<String>,
    /// 单节点模式使用的后端端口。
    pub port: Option<u16>,
    /// 是否使用 RESP over TLS 连接业务数据节点。
    #[serde(default)]
    pub backend_tls_enabled: bool,
    /// 校验业务数据节点证书的 PEM CA 文件。
    pub backend_tls_ca_path: Option<String>,
    /// 业务数据节点证书的 SNI 名称；空值时从目标地址提取。
    pub backend_tls_server_name: Option<String>,
    /// 可选的业务数据节点 TLS 客户端证书。
    pub backend_tls_certificate_path: Option<String>,
    /// 可选的业务数据节点 TLS 客户端私钥。
    pub backend_tls_private_key_path: Option<String>,
    /// Sentinel 监控的主从服务名称，用于解析当前可写主节点。
    pub sentinel_master_name: Option<String>,
    /// Sentinel 控制节点地址，使用逗号分隔的 `host:port` 列表。
    pub sentinel_nodes: Option<String>,
    /// 访问 Sentinel 控制节点时使用的 ACL 用户名。
    pub sentinel_username: Option<String>,
    /// 访问 Sentinel 控制节点时使用的认证密码。
    pub sentinel_password: Option<String>,
    /// 是否使用 RESP over TLS 连接 Sentinel 控制节点。
    #[serde(default)]
    pub sentinel_tls_enabled: bool,
    /// 校验 Sentinel 控制节点证书的 PEM CA 文件。
    pub sentinel_tls_ca_path: Option<String>,
    /// Sentinel 证书的 SNI 名称；空值时从当前 Sentinel 地址提取。
    pub sentinel_tls_server_name: Option<String>,
    /// 可选的 Sentinel TLS 客户端证书。
    pub sentinel_tls_certificate_path: Option<String>,
    /// 可选的 Sentinel TLS 客户端私钥。
    pub sentinel_tls_private_key_path: Option<String>,
    /// Sentinel 控制连接的建连超时，单位为毫秒。
    #[serde(default = "default_sentinel_connect_timeout_ms")]
    pub sentinel_connect_timeout_ms: u64,
    /// Sentinel 命令及候选主节点角色校验的响应超时，单位为毫秒。
    #[serde(default = "default_sentinel_request_timeout_ms")]
    pub sentinel_request_timeout_ms: u64,
    /// 后台重新解析当前主节点的周期，单位为毫秒。
    #[serde(default = "default_sentinel_refresh_interval_ms")]
    pub sentinel_refresh_interval_ms: u64,
    /// 集群或 replication HA 模式使用的逗号分隔节点清单。
    pub nodes: Option<String>,
    /// 后端 ACL 用户名；空值表示只使用密码或后端无需认证。
    pub username: Option<String>,
    /// 代理访问后端节点时使用的密码。
    pub password: Option<String>,
    /// 代理验证业务客户端时使用的前端密码。
    pub frontend_password: Option<String>,
    /// 节点代理模式向 Redis Cluster 客户端统一公布的主机地址。
    pub cluster_node_advertise_host: Option<String>,
    /// 节点代理模式的一对一后端、监听和公布地址配置。
    pub node_proxies: Option<Vec<ClusterNodeProxyItem>>,
    /// 前后端认证的终止、注入或透传策略。
    #[serde(default)]
    pub auth_mode: Option<AuthMode>,
    /// 是否允许业务客户端不认证；生产环境通常应为 `false`。
    #[serde(default)]
    pub allow_unauthenticated: bool,
    /// 是否启用统一只读门禁；启用后写命令及全部脚本、函数执行入口均被拒绝。
    #[serde(default = "default_read_only")]
    pub read_only: bool,
    /// 连接绑定型命令可同时占用的专用后端连接上限。
    #[serde(default = "default_dedicated_tunnel_max_active")]
    pub dedicated_tunnel_max_active: usize,
    /// 专用后端连接达到上限后允许排队的请求数量。
    #[serde(default = "default_dedicated_tunnel_queue_size")]
    pub dedicated_tunnel_queue_size: usize,
    /// 等待专用后端连接许可的最长时间，单位为毫秒。
    #[serde(default = "default_dedicated_tunnel_acquire_timeout_ms")]
    pub dedicated_tunnel_acquire_timeout_ms: u64,
    /// 每个真实后端的 clean tunnel pool 允许保留的最大空闲连接数；零表示关闭复用。
    #[serde(default = "default_dedicated_tunnel_pool_max_idle")]
    pub dedicated_tunnel_pool_max_idle: usize,
    /// 每个后端 tunnel pool 的空闲连接回收阈值，单位为毫秒；零表示关闭复用。
    #[serde(default = "default_dedicated_tunnel_pool_idle_timeout_ms")]
    pub dedicated_tunnel_pool_idle_timeout_ms: u64,
    /// 业务监听器绑定的主机地址。
    #[serde(default = "default_listen_host")]
    pub listen_host: String,
    /// 普通后端 TCP 建连超时，单位为毫秒。
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// 普通后端命令的响应超时，单位为毫秒。
    #[serde(default = "default_backend_request_timeout_ms")]
    pub backend_request_timeout_ms: u64,
    /// 前端连接完成认证的最长等待时间，单位为毫秒。
    #[serde(default = "default_frontend_auth_timeout_ms")]
    pub frontend_auth_timeout_ms: u64,
    /// 已认证客户端允许的空闲时间，单位为毫秒；零表示不主动关闭。
    #[serde(default)]
    pub client_idle_timeout_ms: u64,
    /// TCP keepalive 周期，单位为毫秒；零表示使用系统默认行为。
    #[serde(default)]
    pub tcp_keepalive_ms: u64,
    /// 当前代理项允许同时存在的前端连接上限。
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// 完成认证前允许累计读取的最大字节数。
    #[serde(default = "default_max_auth_bytes")]
    pub max_auth_bytes: usize,
    /// 单批 pipeline 允许包含的最大命令数量。
    #[serde(default = "default_pipeline_max_commands")]
    pub pipeline_max_commands: usize,
    /// 单批 pipeline 允许占用的最大输入字节数。
    #[serde(default = "default_pipeline_max_bytes")]
    pub pipeline_max_bytes: usize,
    /// pipeline 后端连接池允许同时借出的连接数量。
    #[serde(default = "default_pipeline_backend_max_active")]
    pub pipeline_backend_max_active: usize,
    /// pipeline 后端连接池耗尽后允许等待的请求数量。
    #[serde(default = "default_pipeline_backend_queue_size")]
    pub pipeline_backend_queue_size: usize,
    /// 等待向 pipeline 后端池提交任务的最长时间，单位为毫秒。
    #[serde(default = "default_pipeline_backend_submit_timeout_ms")]
    pub pipeline_backend_submit_timeout_ms: u64,
    /// pipeline 后端连接在池中的最大空闲时间，单位为毫秒。
    #[serde(default = "default_pipeline_backend_idle_timeout_ms")]
    pub pipeline_backend_idle_timeout_ms: u64,
    /// 单个 RESP 帧允许占用的最大字节数。
    #[serde(default = "default_frame_max_bytes")]
    pub frame_max_bytes: usize,
    /// 单个 RESP bulk string 声明和接收的最大字节数。
    #[serde(default = "default_bulk_max_bytes")]
    pub bulk_max_bytes: usize,
    /// 跨 Redis Cluster 节点聚合请求允许的并发子任务数量。
    #[serde(default = "default_aggregate_max_concurrency")]
    pub aggregate_max_concurrency: usize,
    /// 聚合类命令允许返回或处理的最大元素数量。
    #[serde(default = "default_aggregate_max_items")]
    pub aggregate_max_items: usize,
    /// 单连接尚未写回客户端的响应字节上限。
    #[serde(default = "default_pending_response_bytes")]
    pub pending_response_bytes: usize,
    /// 进程退出时等待连接和后台任务收敛的时间，单位为毫秒。
    #[serde(default = "default_graceful_shutdown_timeout_ms")]
    pub graceful_shutdown_timeout_ms: u64,
    /// Redis Cluster 槽位拓扑的后台刷新周期，单位为毫秒。
    #[serde(default = "default_cluster_topology_refresh_ms")]
    pub cluster_topology_refresh_ms: u64,
}

/// 业务作用: 定义 Narix 五种互斥代理拓扑模式，并作为配置校验和数据面分派的唯一模式来源。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProxyMode {
    /// 单个固定 Redis 后端的透明代理。
    Standalone,
    /// 通过 Redis Sentinel 发现当前主节点的动态单主代理。
    Sentinel,
    /// 单一入口访问完整 Redis Cluster 的槽位路由代理。
    Cluster,
    /// 为每个 Redis Cluster 节点提供独立公布地址的节点代理。
    ClusterNodeProxy,
    /// 由产品适配器管理主从拓扑和动态持久化的中立 HA 代理。
    ReplicationHa,
}

/// 业务作用: 保存一个源 Cluster 节点对应的监听、发布地址、别名和固定后端映射。
#[derive(Debug, Clone, Deserialize)]
// 表示 ClusterNodeProxyItem 的数据结构，用于配置 Redis Cluster 每个源节点的一对一代理。
pub struct ClusterNodeProxyItem {
    /// 当前监听器一对一转发的真实 Redis Cluster 节点地址。
    pub backend: String,
    /// 当前节点代理监听的业务端口。
    pub listen: u16,
    /// 返回给集群客户端的端口；空值时沿用业务监听端口。
    pub advertise_port: Option<u16>,
    /// 返回给集群客户端的主机；空值时沿用父级统一公布地址。
    pub advertise_host: Option<String>,
    /// 当前节点监听器的绑定主机；空值时沿用父级监听主机。
    pub listen_host: Option<String>,
    /// 需要归一化到当前节点代理地址的其他后端地址别名。
    #[serde(default)]
    pub aliases: Vec<String>,
}

/// 业务作用: 定义前端认证由代理终止、后端注入或原样透传三种责任边界。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum AuthMode {
    /// 允许前端认证帧继续流向后端，并可在缺失时注入后端认证。
    BackendInject,
    /// 由代理验证前端凭据，再使用独立凭据连接后端。
    Terminate,
    /// 不终止认证，保持客户端与后端的连接级认证语义。
    PassThrough,
}

/// 业务作用: 定义切主前仅依赖软隔离窗口或执行外部强隔离命令两种策略。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
// 描述 replication-ha fencing 策略，用于区分软隔离与基础设施强隔离。
pub enum HaFencingMode {
    /// 不运行外部命令，自动或人工切主后再把旧主降级；允许短暂双写窗口。
    Manual,
    /// 调用外部基础设施程序可靠隔离旧主节点。
    ExternalCommand,
}

impl HaFencingMode {
    /// 业务作用: 将 fencing 模式输出为配置和状态接口使用的稳定字符串。
    /// 返回: 与 yml 配置值一致的模式名称。
    pub fn as_str(self) -> &'static str {
        match self {
            HaFencingMode::Manual => "manual",
            HaFencingMode::ExternalCommand => "external-command",
        }
    }
}

/// 业务作用: 定义单控制面、本地租约、外部 Redis 租约和内置协调集群四种动作互斥方式。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
// 描述 replication HA 多 proxy 自动 failover 互斥协调策略。
pub enum HaCoordinatorMode {
    /// 不启用跨进程互斥，仅适用于同一后端组只有一个控制面进程的部署。
    None,
    /// 使用同机文件锁和租约，仅适用于本机验证或单机部署。
    LocalFile,
    /// 使用独立 Redis 兼容存储提供跨机器租约。
    Redis,
    /// 使用 narix 自身的双节点或多数派协调集群。
    ProxyCluster,
}

impl HaCoordinatorMode {
    /// 业务作用: 返回 fencing 模式用于配置、日志和状态接口的稳定名称。
    /// 返回与 YAML 配置及状态接口一致的稳定协调器模式名。
    pub fn as_str(self) -> &'static str {
        match self {
            HaCoordinatorMode::None => "none",
            HaCoordinatorMode::LocalFile => "local-file",
            HaCoordinatorMode::Redis => "redis",
            HaCoordinatorMode::ProxyCluster => "proxy-cluster",
        }
    }
}

/// 业务作用: 定义关闭动态持久化或按拓扑分配快照责任的策略。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
// 描述 replication-ha 动态持久化责任是否按拓扑编排；单节点由主节点兜底，多节点只使用合格副本。
pub enum HaPersistencePolicy {
    /// 不编排后端快照责任。
    None,
    /// 单节点由主节点承担快照，多节点只让合格从节点承担快照责任。
    ReplicaOnly,
}

impl HaPersistencePolicy {
    /// 业务作用: 返回持久化责任策略用于配置与状态接口的稳定名称。
    /// 返回与 YAML 配置及状态接口一致的稳定持久化策略名。
    pub fn as_str(self) -> &'static str {
        match self {
            HaPersistencePolicy::None => "none",
            HaPersistencePolicy::ReplicaOnly => "replica-only",
        }
    }
}

impl ProxyConfig {
    /// 业务作用: 从默认内置配置、可选外部配置和环境变量加载最终 proxy 配置。
    /// 参数说明:
    /// - `external_config`: 外部 yml/json/toml 配置路径；为空时只使用内置配置和环境变量覆盖。
    ///
    /// 返回: 反序列化后的强类型配置；读取、合并、占位符解析或 schema 失败时返回错误。
    pub fn load(external_config: Option<&str>) -> anyhow::Result<Self> {
        let base_file = default_application_path();
        Self::load_from_path(&base_file, external_config)
    }

    /// 业务作用: 从指定 base 配置加载最终配置，主要供测试和自定义部署入口复用。
    /// 参数说明:
    /// - `base_file`: base 配置文件路径，profile 文件会以该路径规则派生。
    /// - `external_config`: 外部覆盖配置路径；同名 proxy 按业务 identity 合并。
    ///
    /// 返回: 反序列化后的强类型配置。
    pub fn load_from(base_file: &str, external_config: Option<&str>) -> anyhow::Result<Self> {
        Self::load_from_path(Path::new(base_file), external_config)
    }

    /// 业务作用: 加载配置并保留完成合并和占位符解析后的有效配置树，供热更新生成确定性摘要。
    ///
    /// 参数说明:
    /// - `base_file`: 基础配置文件路径，profile 文件按相同目录和文件名规则派生。
    /// - `external_config`: 可选的外部配置路径，存在时覆盖基础配置。
    ///
    /// 返回: 强类型配置及其有效配置树；任一来源读取、解析或反序列化失败时返回错误。
    pub(crate) fn load_effective_from(
        base_file: &Path,
        external_config: Option<&str>,
    ) -> anyhow::Result<(Self, Value)> {
        let tree = Self::load_effective_tree(base_file, external_config)?;
        let config = serde_json::from_value(tree.clone()).with_context(|| {
            if let Some(path) = external_config {
                format!(
                    "deserialize config from base `{}` with external overlay `{path}`",
                    base_file.display()
                )
            } else {
                format!("deserialize config from base `{}`", base_file.display())
            }
        })?;
        Ok((config, tree))
    }

    /// 业务作用: 实现配置加载主流程，合并顺序固定为 base、profile、外部 overlay、环境变量、占位符解析。
    /// 参数说明:
    /// - `base_file`: base 配置文件路径。
    /// - `external_config`: 外部覆盖配置路径；空字符串会被忽略。
    ///
    /// 返回: 最终强类型配置；错误上下文会带上 base 和 external 路径，便于定位配置来源。
    fn load_from_path(base_file: &Path, external_config: Option<&str>) -> anyhow::Result<Self> {
        Self::load_effective_from(base_file, external_config).map(|(config, _)| config)
    }

    /// 业务作用: 按固定优先级合并全部配置来源并解析占位符，生成热更新比较使用的有效配置树。
    ///
    /// 参数说明:
    /// - `base_file`: 基础配置文件路径。
    /// - `external_config`: 可选外部覆盖文件路径；空字符串按未配置处理。
    ///
    /// 返回: 尚未反序列化的有效配置树，保留所有最终生效字段。
    fn load_effective_tree(
        base_file: &Path,
        external_config: Option<&str>,
    ) -> anyhow::Result<Value> {
        let external_config = external_config
            .map(str::trim)
            .filter(|path| !path.is_empty());
        let mut tree = load_config_tree(base_file, false)
            .with_context(|| format!("read base config from `{}`", base_file.display()))?;
        let profiled_tree = load_config_tree(base_file, true)
            .with_context(|| format!("read base/profile config from `{}`", base_file.display()))?;
        merge_config_value(&mut tree, profiled_tree, MergePolicy::ExpectedState);

        if let Some(path) = external_config {
            let external = load_config_tree(Path::new(path), true)
                .with_context(|| format!("read external config overlay from `{path}`"))?;
            merge_config_value(&mut tree, external, MergePolicy::ExpectedState);
        }

        if let Some(env_tree) = narix_env_tree()? {
            merge_config_value(&mut tree, env_tree, MergePolicy::AllowIndexFallback);
        }
        resolve_placeholders(&mut tree)?;
        Ok(tree)
    }

    /// 业务作用: 校验最终配置能安全启动，避免空代理、重复名称、端口冲突或不安全认证策略进入运行时。
    /// 返回: 全部配置满足约束时返回成功；任一 listener 或管理端配置非法时 fail-fast。
    pub fn validate(&self) -> anyhow::Result<()> {
        self.admin.validate()?;
        self.reload.validate()?;
        if self.proxies.is_empty() && !(self.admin.enabled && self.reload.enabled) {
            bail!("empty proxies requires admin.enabled=true and reload.enabled=true");
        }
        self.coordination.validate()?;
        let mut names = HashSet::new();
        let mut coordination_groups = HashSet::new();
        let mut listens = Vec::new();
        for item in &self.proxies {
            item.validate(self.coordination.enabled)?;
            if !names.insert(item.name.clone()) {
                bail!("proxy `{}` duplicate name", item.name);
            }
            for bind in item.listen_addrs()? {
                let parsed = parse_listen_bind(&bind)?;
                if let Some((existing_proxy, existing_bind)) = listens
                    .iter()
                    .find(|(_, existing)| listen_bind_conflicts(existing, &parsed))
                {
                    bail!(
                        "proxy `{}` listen address `{}` conflicts with proxy `{}` listen address `{}`",
                        item.name,
                        bind,
                        existing_proxy,
                        existing_bind.addr
                    );
                }
                listens.push((item.name.clone(), parsed));
            }
            let replication_ha = (item.mode == ProxyMode::ReplicationHa)
                .then(|| item.replication_ha_config())
                .transpose()?;
            if let Some(ha) = replication_ha.filter(|ha| {
                ha.coordinator.effective_mode(self.coordination.enabled)
                    == HaCoordinatorMode::ProxyCluster
            }) {
                if !self.coordination.enabled {
                    bail!(
                        "proxy `{}` uses ha.coordinator.mode=proxy-cluster but coordination.enabled=false",
                        item.name
                    );
                }
                let group_id = ha.coordinator.effective_group_id(&item.name);
                if !coordination_groups.insert(group_id.clone()) {
                    bail!(
                        "HA coordinator group_id `{group_id}` is used by more than one proxy item in this process"
                    );
                }
                let configured = ha.coordinator.observation_quorum;
                let majority = self.coordination.peers.len() / 2 + 1;
                if configured != 0 && configured < majority {
                    bail!(
                        "proxy `{}` ha.coordinator.observation_quorum must be 0 or at least static-member majority `{majority}`",
                        item.name
                    );
                }
                if configured > self.coordination.peers.len() {
                    bail!(
                        "proxy `{}` ha.coordinator.observation_quorum exceeds coordination peer count",
                        item.name
                    );
                }
                // 协调令牌失效只能阻止下一步，无法撤销已经发给后端的网络请求或外部命令。
                // 因此每个可能产生外部副作用的单步超时都不能长于动作令牌有效期：这样领导权
                // 变化后，旧领导者至多等待当前有界步骤返回，不会无限占用后端控制面。
                let action_timeout_ms = self.coordination.action_timeout_ms;
                let persistence_verify_timeout_ms = ha
                    .persistence
                    .dragonfly
                    .as_ref()
                    .map(|config| config.verify_timeout_ms)
                    .or_else(|| {
                        ha.persistence
                            .redis
                            .as_ref()
                            .map(|config| config.verify_timeout_ms)
                    })
                    .or_else(|| {
                        ha.persistence
                            .valkey
                            .as_ref()
                            .map(|config| config.verify_timeout_ms)
                    })
                    .or_else(|| {
                        ha.persistence
                            .redict
                            .as_ref()
                            .map(|config| config.verify_timeout_ms)
                    })
                    .or_else(|| {
                        ha.persistence
                            .keydb
                            .as_ref()
                            .map(|config| config.verify_timeout_ms)
                    })
                    .unwrap_or(item.backend_request_timeout_ms);
                let phase_timeouts = [
                    ("connect_timeout_ms", item.connect_timeout_ms),
                    (
                        "backend_request_timeout_ms",
                        item.backend_request_timeout_ms,
                    ),
                    ("ha.fencing.timeout_ms", ha.fencing.timeout_ms),
                    (
                        "ha.persistence product verify_timeout_ms",
                        persistence_verify_timeout_ms,
                    ),
                ];
                for (field, timeout_ms) in phase_timeouts {
                    if timeout_ms > action_timeout_ms {
                        bail!(
                            "proxy `{}` {field} must not exceed coordination.action_timeout_ms `{action_timeout_ms}` in proxy-cluster mode",
                            item.name
                        );
                    }
                }
            }
        }
        if self.coordination.enabled {
            let coordination_bind = parse_listen_bind(&self.coordination.listen)?;
            if let Some((existing_proxy, existing_bind)) = listens
                .iter()
                .find(|(_, existing)| listen_bind_conflicts(existing, &coordination_bind))
            {
                bail!(
                    "coordination.listen `{}` conflicts with proxy `{}` listen address `{}`",
                    self.coordination.listen,
                    existing_proxy,
                    existing_bind.addr
                );
            }
            if self.admin.enabled {
                let admin_bind = parse_listen_bind(&self.admin.listen)?;
                if listen_bind_conflicts(&admin_bind, &coordination_bind) {
                    bail!(
                        "coordination.listen `{}` conflicts with admin.listen `{}`",
                        self.coordination.listen,
                        self.admin.listen
                    );
                }
            }
        }
        if self.admin.enabled {
            let admin_bind = parse_listen_bind(&self.admin.listen)?;
            if let Some((existing_proxy, existing_bind)) = listens
                .iter()
                .find(|(_, existing)| listen_bind_conflicts(existing, &admin_bind))
            {
                bail!(
                    "admin.listen `{}` conflicts with proxy `{}` listen address `{}`",
                    self.admin.listen,
                    existing_proxy,
                    existing_bind.addr
                );
            }
        }
        Ok(())
    }
}

/// 业务作用: 按当前目录、系统目录和清单目录顺序选择默认 application.yml 路径。
/// 参数说明: 无。
pub(crate) fn default_application_path() -> PathBuf {
    let cwd = PathBuf::from(DEFAULT_APPLICATION_CONFIG);
    if cwd.exists() {
        return cwd;
    }

    let etc = PathBuf::from("/etc/narix").join(DEFAULT_APPLICATION_CONFIG);
    if etc.exists() {
        return etc;
    }

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_APPLICATION_CONFIG);
    if manifest.exists() {
        return manifest;
    }

    cwd
}

/// 业务作用: 加载 配置加载、覆盖合并、环境变量映射和启动前校验 所需数据，并按覆盖优先级或来源差异合并成统一运行视图。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `include_profile`: 是否加载配置档覆盖层，用于区分完整启动加载与单文件解析。
///
/// 返回: 配置读取和合并成功时返回 YAML 树；文件、语法或结构非法时返回错误。
fn load_config_tree(path: &Path, include_profile: bool) -> anyhow::Result<Value> {
    let mut tree = load_required_config_file(path)
        .with_context(|| format!("read config file `{}`", path.display()))?;
    if include_profile {
        if let Some(profile_file) = profile_file_for_base(path)? {
            let profile_tree = load_required_config_file(&profile_file)
                .with_context(|| format!("read profile config `{}`", profile_file.display()))?;
            merge_config_value(&mut tree, profile_tree, MergePolicy::ExpectedState);
        }
    }
    Ok(tree)
}

/// 业务作用: 读取一个配置文件并转换为 JSON tree，供后续业务化数组合并和强类型反序列化使用。
/// 参数说明:
/// - `path`: 配置文件路径；没有扩展名时按 YAML 解析。
///
/// 返回: 文件解析后的通用配置树；格式未知、文件缺失或语法错误时返回明确上下文。
fn load_required_config_file(path: &Path) -> anyhow::Result<Value> {
    let format = config_file_format(path)?;
    let source = File::from(path).format(format);
    let tree = ConfigSource::builder()
        .add_source(source)
        .build()
        .with_context(|| format!("parse config file `{}`", path.display()))?
        .try_deserialize::<Value>()
        .with_context(|| format!("convert config file `{}` into JSON tree", path.display()))?;
    // 唯一性必须在单个来源进入跨层合并前检查。否则同一文件中两个同名代理会被误判成
    // “高优先级覆盖低优先级”，后一个元素递归覆盖前一个元素，最终强类型校验已经看不到重复项。
    validate_source_array_identities(&tree)
        .with_context(|| format!("validate identities in config file `{}`", path.display()))?;
    Ok(tree)
}

/// 业务作用: 校验单个配置来源内依赖业务身份合并的数组，防止同层重复项被跨层覆盖算法静默吞并。
///
/// 该检查只观察一个刚解析完成的文件或一棵环境变量配置树，不比较不同优先级来源。因此
/// base、profile 和外部文件仍可用相同 `name` 或 `backend` 定向覆盖已有项，而同一来源内的
/// 重复身份会在任何合并发生前失败。
///
/// # 参数
///
/// - `tree`：尚未与其他优先级配置合并的通用配置树。
///
/// # 返回
///
/// 所有可见身份在各自数组内唯一时返回成功；发现重复代理名称或重复节点后端时返回错误。
fn validate_source_array_identities(tree: &Value) -> anyhow::Result<()> {
    let Some(proxies) = tree.get("proxies").and_then(Value::as_array) else {
        // 缺少 proxies 或字段类型错误交给后续反序列化产生完整的 schema 错误；这里仅负责
        // 数组身份唯一性，避免重复实现类型校验并给出互相矛盾的诊断。
        return Ok(());
    };
    validate_array_identity(proxies, "name", "proxies")?;

    for (proxy_index, proxy) in proxies.iter().enumerate() {
        let Some(node_proxies) = proxy.get("node_proxies").and_then(Value::as_array) else {
            continue;
        };
        validate_array_identity(
            node_proxies,
            "backend",
            &format!("proxies[{proxy_index}].node_proxies"),
        )?;
    }
    Ok(())
}

/// 业务作用: 校验一个配置数组中的字符串身份字段是否唯一。
///
/// 缺少身份字段或身份类型错误由最终 schema 与模式校验负责；这里跳过这些元素，只拒绝
/// 能够明确证明属于同一来源的重复字符串，避免把不完整覆盖项误报成重复项。
///
/// # 参数
///
/// - `items`：当前单一配置来源中的数组元素。
/// - `identity_key`：跨层合并使用的稳定身份字段名。
/// - `array_path`：错误信息使用的 YAML 路径，帮助定位重复项所属数组。
///
/// # 返回
///
/// 字符串身份均唯一时返回成功；第二次遇到相同身份时立即返回包含数组下标的错误。
fn validate_array_identity(
    items: &[Value],
    identity_key: &str,
    array_path: &str,
) -> anyhow::Result<()> {
    let mut identities = HashSet::new();
    for (index, item) in items.iter().enumerate() {
        let Some(identity) = item.get(identity_key).and_then(Value::as_str) else {
            continue;
        };
        if !identities.insert(identity) {
            bail!(
                "{array_path}[{index}].{identity_key} duplicates `{identity}` in the same config source"
            );
        }
    }
    Ok(())
}

/// 业务作用: 根据配置文件扩展名选择解析格式，避免不同入口对 YAML/JSON/TOML 的支持范围不一致。
/// 参数说明:
/// - `path`: 配置文件路径；扩展名为空时兼容历史行为并默认 YAML。
///
/// 返回: `config` crate 使用的文件格式枚举。
fn config_file_format(path: &Path) -> anyhow::Result<FileFormat> {
    match path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("yml" | "yaml") | None => Ok(FileFormat::Yaml),
        Some("json") => Ok(FileFormat::Json),
        Some("toml") => Ok(FileFormat::Toml),
        Some(ext) => bail!(
            "config file `{}` uses unsupported extension `{ext}`",
            path.display()
        ),
    }
}

/// 业务作用: 根据基础配置路径和 profile 名推导同目录的覆盖文件路径。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
///
/// 返回: `NARIX_PROFILE` 为空或派生文件不存在时返回 `None`，避免未配置 profile 阻塞启动。
fn profile_file_for_base(path: &Path) -> anyhow::Result<Option<PathBuf>> {
    let profile = match std::env::var(NARIX_PROFILE_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => return Ok(None),
    };
    let stem = path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("application");
    let file_name = match path.extension().and_then(|value| value.to_str()) {
        Some(ext) if !ext.is_empty() => format!("{stem}-{profile}.{ext}"),
        _ => format!("{stem}-{profile}"),
    };
    let candidate = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(file_name),
        _ => PathBuf::from(file_name),
    };
    Ok(candidate.exists().then_some(candidate))
}

/// 业务作用: 解析并替换 `${path.to.field}` 占位符，使外部覆盖配置可以复用已经合并出的基础配置值。
/// 参数说明:
/// - `tree`: 已完成 base/profile/external/env 合并的配置树，会被原地更新。
///
/// 返回: 所有占位符解析成功时返回成功；引用不存在、引用非标量或递归过深时 fail-fast。
fn resolve_placeholders(tree: &mut Value) -> anyhow::Result<()> {
    for _ in 0..PLACEHOLDER_MAX_PASSES {
        let before = tree.clone();
        let root = before.clone();
        resolve_placeholders_once(tree, &root)?;
        if *tree == before {
            return Ok(());
        }
    }
    bail!(
        "config placeholder resolution exceeded {PLACEHOLDER_MAX_PASSES} passes; check for cycles"
    )
}

/// 业务作用: 单轮扫描配置树并替换字符串中的占位符，供外层多轮解析处理嵌套引用。
/// 参数说明:
/// - `value`: 当前正在处理的配置子树。
/// - `root`: 本轮解析使用的完整配置快照。
///
/// 返回: 本轮处理成功时返回成功。
fn resolve_placeholders_once(value: &mut Value, root: &Value) -> anyhow::Result<()> {
    match value {
        Value::Object(map) => {
            for child in map.values_mut() {
                resolve_placeholders_once(child, root)?;
            }
        }
        Value::Array(items) => {
            for child in items {
                resolve_placeholders_once(child, root)?;
            }
        }
        Value::String(text) => {
            *text = resolve_placeholders_in_string(text, root)?;
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

/// 业务作用: 替换单个字符串内的一个或多个 `${...}` 引用，保持普通文本片段原样。
/// 参数说明:
/// - `text`: 待解析的配置字符串。
/// - `root`: 完整配置树，用于按点分路径查找引用值。
///
/// 返回: 替换后的字符串。
fn resolve_placeholders_in_string(text: &str, root: &Value) -> anyhow::Result<String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        let Some(end) = after_start.find('}') else {
            bail!("config placeholder `{text}` is missing closing `}}`");
        };
        let path = after_start[..end].trim();
        if path.is_empty() {
            bail!("config placeholder `{text}` contains an empty path");
        }
        out.push_str(&placeholder_scalar(root, path)?);
        rest = &after_start[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// 业务作用: 从配置树读取占位符引用的标量值，并转换为可嵌入配置字符串的文本。
/// 参数说明:
/// - `root`: 完整配置树。
/// - `path`: 以点号分隔的引用路径，数组下标用十进制数字表示。
///
/// 返回: 字符串、数字或布尔值的文本表示；对象、数组和空值不允许作为字符串占位符。
fn placeholder_scalar(root: &Value, path: &str) -> anyhow::Result<String> {
    let mut current = root;
    for segment in path.split('.') {
        current = match current {
            Value::Object(map) => map
                .get(segment)
                .with_context(|| format!("config placeholder `${{{path}}}` not found"))?,
            Value::Array(items) => {
                let index = segment.parse::<usize>().with_context(|| {
                    format!("config placeholder `${{{path}}}` array index `{segment}`")
                })?;
                items
                    .get(index)
                    .with_context(|| format!("config placeholder `${{{path}}}` not found"))?
            }
            _ => bail!("config placeholder `${{{path}}}` crosses a non-container value"),
        };
    }
    match current {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => bail!("config placeholder `${{{path}}}` references null"),
        Value::Array(_) | Value::Object(_) => {
            bail!("config placeholder `${{{path}}}` references a non-scalar value")
        }
    }
}

/// 业务作用: 区分配置数组整体替换与按稳定标识合并两种覆盖语义。
#[derive(Debug, Clone, Copy)]
enum MergePolicy {
    /// 文件显式给出的业务数组是完整期望集合，缺失身份表示删除；省略整个数组才继承。
    ExpectedState,
    /// 环境变量按身份优先、数组下标兜底覆盖，不删除文件层已经声明的其他元素。
    AllowIndexFallback,
}

/// 业务作用: 递归合并配置树，并让高优先级标量或数组覆盖低优先级值。
/// 参数说明:
/// - `target`: 当前操作的目标节点、配置项或输出位置，函数只修改该明确范围。
/// - `overlay`: 较高优先级配置覆盖层，合并时只替换显式提供的字段。
/// - `policy`: 当前合并、路由或持久化策略，限定允许的状态转换。
///
fn merge_config_value(target: &mut Value, overlay: Value, policy: MergePolicy) {
    match overlay {
        Value::Object(overlay_map) => {
            // 对象必须递归按字段合并，不能整块替换；否则外部配置只覆盖一个字段时会意外丢失内置安全默认值。
            if !target.is_object() {
                *target = Value::Object(Map::new());
            }
            let target_map = target.as_object_mut().expect("target object initialized");
            for (key, value) in overlay_map {
                if key == "proxies" {
                    // proxies 使用 name 作为稳定业务标识，避免环境变量或配置档因数组位置变化覆盖到错误代理。
                    let entry = target_map.entry(key).or_insert(Value::Array(Vec::new()));
                    merge_array_by_identity(entry, value, "name", policy);
                } else if key == "node_proxies" {
                    // node_proxies 使用真实 backend 标识节点；监听端口只是部署属性，不能用于判断是否为同一节点。
                    let entry = target_map.entry(key).or_insert(Value::Array(Vec::new()));
                    merge_array_by_identity(entry, value, "backend", policy);
                } else {
                    let entry = target_map.entry(key).or_insert(Value::Null);
                    merge_config_value(entry, value, policy);
                }
            }
        }
        // 标量和显式 null 采用高优先级值直接覆盖，这是命令行/环境变量能够关闭默认配置的必要条件。
        value => *target = value,
    }
}

/// 业务作用: 按对象稳定标识合并配置数组，保留未覆盖项并支持局部字段覆盖。
/// 参数说明:
/// - `target`: 当前操作的目标节点、配置项或输出位置，函数只修改该明确范围。
/// - `overlay`: 较高优先级配置覆盖层，合并时只替换显式提供的字段。
/// - `identity_key`: 数组配置中标识同一业务对象的字段名，例如代理名称或后端地址。
/// - `policy`: 当前合并、路由或持久化策略，限定允许的状态转换。
///
fn merge_array_by_identity(
    target: &mut Value,
    overlay: Value,
    identity_key: &str,
    policy: MergePolicy,
) {
    let Value::Array(overlay_items) = overlay else {
        *target = overlay;
        return;
    };
    if !target.is_array() {
        *target = Value::Array(Vec::new());
    }
    let target_items = target.as_array_mut().expect("target array initialized");
    let expected_identities = matches!(policy, MergePolicy::ExpectedState).then(|| {
        overlay_items
            .iter()
            .filter_map(|item| item.get(identity_key).and_then(Value::as_str))
            .map(str::to_string)
            .collect::<HashSet<_>>()
    });

    for (index, overlay_item) in overlay_items.into_iter().enumerate() {
        // null 表示该索引没有提供覆盖内容，不应把已有完整条目清空。
        if overlay_item.is_null() {
            continue;
        }
        if let Some(position) = find_identity_position(target_items, &overlay_item, identity_key) {
            // 找到业务 identity 后递归合并，保留目标条目中未被覆盖的认证、超时和容量字段。
            merge_config_value(&mut target_items[position], overlay_item, policy);
        } else if matches!(policy, MergePolicy::AllowIndexFallback)
            && overlay_item.is_object()
            && index < target_items.len()
        {
            // 环境变量天然通过索引表达嵌套数组，因此仅在明确允许时回退到索引；普通文件合并不采用该规则。
            merge_config_value(&mut target_items[index], overlay_item, policy);
        } else {
            // identity 和索引都无法匹配时视为新增业务项，不能猜测目标并覆盖现有代理。
            target_items.push(overlay_item);
        }
    }
    if let Some(expected_identities) = expected_identities {
        // 文件层只要显式写出数组，它就表达该层的完整期望集合。先完成同身份字段继承，
        // 再删除高优先级数组没有保留的低优先级项，既支持局部字段覆盖，也支持热更删除。
        target_items.retain(|item| {
            item.get(identity_key)
                .and_then(Value::as_str)
                .is_none_or(|identity| expected_identities.contains(identity))
        });
    }
}

/// 业务作用: 在数组配置中按 identity 字段查找已有条目，用于同名 proxy 或同 backend 子代理的深合并。
/// 参数说明:
/// - `items`: 目标数组中已经存在的配置条目。
/// - `needle`: overlay 中准备合并的新配置条目。
/// - `identity_key`: 用于判定同一业务对象的字段名，例如 proxy `name` 或子代理 `backend`。
///
/// 返回: 找到同 identity 条目时返回数组下标，否则返回 `None`。
fn find_identity_position(items: &[Value], needle: &Value, identity_key: &str) -> Option<usize> {
    let needle_value = needle.get(identity_key)?.as_str()?;
    items.iter().position(|item| {
        item.get(identity_key)
            .and_then(Value::as_str)
            .is_some_and(|value| value == needle_value)
    })
}

/// 业务作用: 声明环境变量值应按字符串、布尔、整数或列表中的哪种类型解析。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnvValueKind {
    String,
    U64,
    Bool,
    StringList,
}

/// 业务作用: 声明一个环境变量后缀对应的配置字段名和值解析类型。
#[derive(Debug, Clone, Copy)]
struct FieldSpec {
    env: &'static str,
    key: &'static str,
    kind: EnvValueKind,
}

/// 业务作用: 表示环境变量覆盖路径中的对象字段或数组下标。
#[derive(Debug, Clone)]
enum PathSegment {
    Key(&'static str),
    Index(usize),
}

/// 业务作用: 保存环境字段匹配结果和后续尚未消费的路径后缀。
#[derive(Debug, Clone, Copy)]
struct FieldMatch {
    field: FieldSpec,
    kind: EnvValueKind,
    list_index: Option<usize>,
}

const ADMIN_FIELDS: &[FieldSpec] = &[
    field(
        "ALLOW_UNAUTHENTICATED",
        "allow_unauthenticated",
        EnvValueKind::Bool,
    ),
    field("ENABLED", "enabled", EnvValueKind::Bool),
    field("LISTEN", "listen", EnvValueKind::String),
    field("TOKEN", "token", EnvValueKind::String),
];

const PROXY_FIELDS: &[FieldSpec] = &[
    field("BACKEND_PROFILE", "backend_profile", EnvValueKind::String),
    field(
        "AGGREGATE_MAX_CONCURRENCY",
        "aggregate_max_concurrency",
        EnvValueKind::U64,
    ),
    field(
        "AGGREGATE_MAX_ITEMS",
        "aggregate_max_items",
        EnvValueKind::U64,
    ),
    field(
        "ALLOW_UNAUTHENTICATED",
        "allow_unauthenticated",
        EnvValueKind::Bool,
    ),
    field("AUTH_MODE", "auth_mode", EnvValueKind::String),
    field(
        "BACKEND_TLS_CA_PATH",
        "backend_tls_ca_path",
        EnvValueKind::String,
    ),
    field(
        "BACKEND_TLS_CERTIFICATE_PATH",
        "backend_tls_certificate_path",
        EnvValueKind::String,
    ),
    field(
        "BACKEND_TLS_ENABLED",
        "backend_tls_enabled",
        EnvValueKind::Bool,
    ),
    field(
        "BACKEND_TLS_PRIVATE_KEY_PATH",
        "backend_tls_private_key_path",
        EnvValueKind::String,
    ),
    field(
        "BACKEND_TLS_SERVER_NAME",
        "backend_tls_server_name",
        EnvValueKind::String,
    ),
    field(
        "BACKEND_REQUEST_TIMEOUT_MS",
        "backend_request_timeout_ms",
        EnvValueKind::U64,
    ),
    field("BULK_MAX_BYTES", "bulk_max_bytes", EnvValueKind::U64),
    field(
        "CLIENT_IDLE_TIMEOUT_MS",
        "client_idle_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "CLUSTER_NODE_ADVERTISE_HOST",
        "cluster_node_advertise_host",
        EnvValueKind::String,
    ),
    field(
        "CLUSTER_TOPOLOGY_REFRESH_MS",
        "cluster_topology_refresh_ms",
        EnvValueKind::U64,
    ),
    field(
        "CONNECT_TIMEOUT_MS",
        "connect_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "SENTINEL_CONNECT_TIMEOUT_MS",
        "sentinel_connect_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "SENTINEL_MASTER_NAME",
        "sentinel_master_name",
        EnvValueKind::String,
    ),
    field("SENTINEL_NODES", "sentinel_nodes", EnvValueKind::String),
    field(
        "SENTINEL_PASSWORD",
        "sentinel_password",
        EnvValueKind::String,
    ),
    field(
        "SENTINEL_REFRESH_INTERVAL_MS",
        "sentinel_refresh_interval_ms",
        EnvValueKind::U64,
    ),
    field(
        "SENTINEL_REQUEST_TIMEOUT_MS",
        "sentinel_request_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "SENTINEL_TLS_CA_PATH",
        "sentinel_tls_ca_path",
        EnvValueKind::String,
    ),
    field(
        "SENTINEL_TLS_CERTIFICATE_PATH",
        "sentinel_tls_certificate_path",
        EnvValueKind::String,
    ),
    field(
        "SENTINEL_TLS_ENABLED",
        "sentinel_tls_enabled",
        EnvValueKind::Bool,
    ),
    field(
        "SENTINEL_TLS_PRIVATE_KEY_PATH",
        "sentinel_tls_private_key_path",
        EnvValueKind::String,
    ),
    field(
        "SENTINEL_TLS_SERVER_NAME",
        "sentinel_tls_server_name",
        EnvValueKind::String,
    ),
    field(
        "SENTINEL_USERNAME",
        "sentinel_username",
        EnvValueKind::String,
    ),
    field(
        "DEDICATED_TUNNEL_ACQUIRE_TIMEOUT_MS",
        "dedicated_tunnel_acquire_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "DEDICATED_TUNNEL_MAX_ACTIVE",
        "dedicated_tunnel_max_active",
        EnvValueKind::U64,
    ),
    field(
        "DEDICATED_TUNNEL_POOL_IDLE_TIMEOUT_MS",
        "dedicated_tunnel_pool_idle_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "DEDICATED_TUNNEL_POOL_MAX_IDLE",
        "dedicated_tunnel_pool_max_idle",
        EnvValueKind::U64,
    ),
    field(
        "DEDICATED_TUNNEL_QUEUE_SIZE",
        "dedicated_tunnel_queue_size",
        EnvValueKind::U64,
    ),
    field("FRAME_MAX_BYTES", "frame_max_bytes", EnvValueKind::U64),
    field(
        "FRONTEND_AUTH_TIMEOUT_MS",
        "frontend_auth_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "FRONTEND_PASSWORD",
        "frontend_password",
        EnvValueKind::String,
    ),
    field(
        "GRACEFUL_SHUTDOWN_TIMEOUT_MS",
        "graceful_shutdown_timeout_ms",
        EnvValueKind::U64,
    ),
    field("HOST", "host", EnvValueKind::String),
    field("LISTEN", "listen", EnvValueKind::U64),
    field("LISTEN_HOST", "listen_host", EnvValueKind::String),
    field("MAX_AUTH_BYTES", "max_auth_bytes", EnvValueKind::U64),
    field("MAX_CONNECTIONS", "max_connections", EnvValueKind::U64),
    field("MODE", "mode", EnvValueKind::String),
    field("NAME", "name", EnvValueKind::String),
    field("NODES", "nodes", EnvValueKind::String),
    field("PASSWORD", "password", EnvValueKind::String),
    field(
        "PENDING_RESPONSE_BYTES",
        "pending_response_bytes",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_BACKEND_IDLE_TIMEOUT_MS",
        "pipeline_backend_idle_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_BACKEND_MAX_ACTIVE",
        "pipeline_backend_max_active",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_BACKEND_QUEUE_SIZE",
        "pipeline_backend_queue_size",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_BACKEND_SUBMIT_TIMEOUT_MS",
        "pipeline_backend_submit_timeout_ms",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_MAX_BYTES",
        "pipeline_max_bytes",
        EnvValueKind::U64,
    ),
    field(
        "PIPELINE_MAX_COMMANDS",
        "pipeline_max_commands",
        EnvValueKind::U64,
    ),
    field("PORT", "port", EnvValueKind::U64),
    field("TCP_KEEPALIVE_MS", "tcp_keepalive_ms", EnvValueKind::U64),
    field("USERNAME", "username", EnvValueKind::String),
    field("READ_ONLY", "read_only", EnvValueKind::Bool),
];

const NODE_PROXY_FIELDS: &[FieldSpec] = &[
    field("ADVERTISE_HOST", "advertise_host", EnvValueKind::String),
    field("ADVERTISE_PORT", "advertise_port", EnvValueKind::U64),
    field("ALIASES", "aliases", EnvValueKind::StringList),
    field("BACKEND", "backend", EnvValueKind::String),
    field("LISTEN", "listen", EnvValueKind::U64),
    field("LISTEN_HOST", "listen_host", EnvValueKind::String),
];

/// 业务作用: 声明环境变量到强类型配置字段的稳定映射，供覆盖树构造流程统一解析键名和值类型。
/// 参数说明:
/// - `env`: 环境变量键值集合，作为最高优先级配置覆盖来源。
/// - `key`: 当前命令、租约或配置项使用的唯一键，用于定位明确的数据或状态。
/// - `kind`: 当前值、字段或命令的类别，决定解析规则和后续分支。
///
const fn field(env: &'static str, key: &'static str, kind: EnvValueKind) -> FieldSpec {
    FieldSpec { env, key, kind }
}

/// 业务作用: 把 NARIX 环境变量构造成配置覆盖树，供统一合并流程复用。
/// 参数说明: 无。
/// 返回: 存在有效环境覆盖时返回配置树，没有匹配变量时返回空；键或值非法时返回错误。
fn narix_env_tree() -> anyhow::Result<Option<Value>> {
    narix_env_tree_from_iter(std::env::vars())
}

/// 业务作用: 从任意环境变量迭代器构造 NARIX 配置覆盖树，保证真实进程环境与受控输入遵循同一规则。
/// 参数说明:
/// - `vars`: 已收集的环境变量列表，应用前按配置路径进行排序和类型解析。
///
/// 返回: 存在有效环境覆盖时返回配置树，没有匹配变量时返回空；键或值非法时返回错误。
fn narix_env_tree_from_iter<I, K, V>(vars: I) -> anyhow::Result<Option<Value>>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut root = Value::Object(Map::new());
    let mut found = false;
    for (name, value) in vars {
        let name = name.as_ref();
        let Some(rest) = name.strip_prefix(NARIX_ENV_PREFIX) else {
            continue;
        };
        if is_narix_control_env(rest) {
            continue;
        }
        let (path, kind) = parse_narix_env_path(rest)
            .with_context(|| format!("parse environment variable `{name}`"))?;
        let value = parse_env_value(name, value.as_ref(), kind)?;
        insert_path_value(&mut root, &path, value)
            .with_context(|| format!("apply environment variable `{name}`"))?;
        found = true;
    }
    if !found {
        return Ok(None);
    }
    // 不同数组下标若声明同一个业务身份，也属于同一环境变量覆盖来源；在与文件配置合并前
    // 拒绝它，避免遍历顺序决定最终保留哪组字段。
    validate_source_array_identities(&root)
        .context("validate identities in RP environment variables")?;
    Ok(Some(root))
}

/// 业务作用: 识别只控制进程启动而不应进入配置覆盖树的 NARIX 环境变量。
/// 参数说明:
/// - `rest`: 去掉当前已识别段后的剩余配置路径，供下一层继续解析。
///
fn is_narix_control_env(rest: &str) -> bool {
    matches!(rest, "CONFIG" | "PROFILE")
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `rest`: 去掉当前已识别段后的剩余配置路径，供下一层继续解析。
///
/// 返回: 环境变量后缀唯一匹配时返回配置路径和值类型；未知、歧义或截断时返回错误。
fn parse_narix_env_path(rest: &str) -> anyhow::Result<(Vec<PathSegment>, EnvValueKind)> {
    if let Some(admin_rest) = rest.strip_prefix("ADMIN_") {
        let Some(field) = match_field(admin_rest, ADMIN_FIELDS)? else {
            bail!("unsupported admin field `{admin_rest}`");
        };
        let mut path = vec![PathSegment::Key("admin")];
        append_field_path(&mut path, field);
        return Ok((path, field.kind));
    }

    let Some(proxy_rest) = rest.strip_prefix("PROXIES_") else {
        bail!("unsupported RP environment variable suffix `{rest}`");
    };
    let (proxy_index, proxy_field_rest) = take_index(proxy_rest, "PROXIES")?;
    let mut path = vec![PathSegment::Key("proxies"), PathSegment::Index(proxy_index)];

    if let Some(node_rest) = proxy_field_rest.strip_prefix("NODE_PROXIES_") {
        let (node_index, node_field_rest) = take_index(node_rest, "NODE_PROXIES")?;
        path.push(PathSegment::Key("node_proxies"));
        path.push(PathSegment::Index(node_index));
        let Some(field) = match_field(node_field_rest, NODE_PROXY_FIELDS)? else {
            bail!("unsupported node_proxies field `{node_field_rest}`");
        };
        append_field_path(&mut path, field);
        return Ok((path, field.kind));
    }

    let Some(field) = match_field(proxy_field_rest, PROXY_FIELDS)? else {
        bail!("unsupported proxy field `{proxy_field_rest}`");
    };
    append_field_path(&mut path, field);
    Ok((path, field.kind))
}

/// 业务作用: 在允许字段表中解析环境变量后缀并拒绝未知或歧义映射。
/// 参数说明:
/// - `rest`: 去掉当前已识别段后的剩余配置路径，供下一层继续解析。
/// - `fields`: 允许识别的字段定义集合，用于环境变量路径匹配和类型转换。
///
/// 返回: 唯一匹配字段时返回映射，完全不匹配时返回空；存在歧义时返回错误。
fn match_field(rest: &str, fields: &[FieldSpec]) -> anyhow::Result<Option<FieldMatch>> {
    // 先做精确匹配，确保同名前缀的标量字段不会被 StringList 字段抢先匹配。
    for field in fields {
        if rest == field.env {
            return Ok(Some(FieldMatch {
                field: *field,
                kind: field.kind,
                list_index: None,
            }));
        }
    }
    // 再做 StringList 的 `<字段名>_<下标>` 前缀匹配。
    for field in fields {
        if field.kind == EnvValueKind::StringList {
            if let Some(index_rest) = rest
                .strip_prefix(field.env)
                .and_then(|tail| tail.strip_prefix('_'))
            {
                let index = parse_index(index_rest, field.env)?;
                return Ok(Some(FieldMatch {
                    field: *field,
                    kind: EnvValueKind::String,
                    list_index: Some(index),
                }));
            }
        }
    }
    Ok(None)
}

/// 业务作用: 把一个配置字段安全追加到环境变量覆盖路径。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `field_match`: 字段匹配结果，包含规范字段名及后续解析所需的剩余路径。
///
fn append_field_path(path: &mut Vec<PathSegment>, field_match: FieldMatch) {
    path.push(PathSegment::Key(field_match.field.key));
    if let Some(index) = field_match.list_index {
        path.push(PathSegment::Index(index));
    }
}

/// 业务作用: 从环境变量路径中解析数组下标并推进剩余后缀。
/// 参数说明:
/// - `rest`: 去掉当前已识别段后的剩余配置路径，供下一层继续解析。
/// - `context`: 当前解析、模板展开或错误处理上下文，为字段解释和错误信息提供业务边界。
///
/// 返回: 下标合法时返回数值与剩余路径；缺失、格式非法或溢出时返回错误。
fn take_index<'a>(rest: &'a str, context: &str) -> anyhow::Result<(usize, &'a str)> {
    let (index, tail) = rest
        .split_once('_')
        .with_context(|| format!("{context} requires an index and field name"))?;
    Ok((parse_index(index, context)?, tail))
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
/// - `context`: 当前解析、模板展开或错误处理上下文，为字段解释和错误信息提供业务边界。
///
/// 返回: 输入可安全转换且未越界时返回数量；缺失、格式非法或溢出时返回错误。
fn parse_index(raw: &str, context: &str) -> anyhow::Result<usize> {
    raw.parse::<usize>()
        .with_context(|| format!("{context} index `{raw}` is not a non-negative integer"))
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
/// - `kind`: 当前值、字段或命令的类别，决定解析规则和后续分支。
///
/// 返回: 返回按声明类型解析的 YAML 标量或列表；布尔、整数或列表格式非法时返回带变量名错误。
fn parse_env_value(name: &str, raw: &str, kind: EnvValueKind) -> anyhow::Result<Value> {
    match kind {
        EnvValueKind::String => Ok(Value::String(raw.to_string())),
        EnvValueKind::U64 => Ok(Value::Number(Number::from(
            raw.parse::<u64>()
                .with_context(|| format!("{name} value `{raw}` is not an unsigned integer"))?,
        ))),
        EnvValueKind::Bool => {
            Ok(Value::Bool(parse_bool(raw).with_context(|| {
                format!("{name} value `{raw}` is not a boolean")
            })?))
        }
        EnvValueKind::StringList => parse_string_list(raw)
            .map(|items| Value::Array(items.into_iter().map(Value::String).collect()))
            .with_context(|| format!("{name} value is not a string list")),
    }
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
/// 返回: 接受约定的真假字面量并返回布尔值；其它输入返回错误。
fn parse_bool(raw: &str) -> anyhow::Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => bail!("invalid boolean"),
    }
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
/// 返回: 返回去除空白后的非空字符串列表；包含空元素时返回错误。
fn parse_string_list(raw: &str) -> anyhow::Result<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).context("parse JSON string array");
    }
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    Ok(raw.split(',').map(|item| item.trim().to_string()).collect())
}

/// 业务作用: 沿字段和数组下标路径创建容器并写入已解析的环境变量值。
/// 参数说明:
/// - `root`: 配置树根节点，环境变量覆盖和占位符解析都从该节点开始。
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 值成功写入唯一字段或数组位置时返回成功；路径类型冲突或重复赋值时返回错误。
fn insert_path_value(root: &mut Value, path: &[PathSegment], value: Value) -> anyhow::Result<()> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    let mut current = root;
    for (index, segment) in path.iter().enumerate() {
        let last = index + 1 == path.len();
        match segment {
            PathSegment::Key(key) => {
                if !current.is_object() {
                    *current = Value::Object(Map::new());
                }
                let map = current.as_object_mut().expect("object initialized");
                if last {
                    map.insert((*key).to_string(), value);
                    return Ok(());
                }
                current = map
                    .entry((*key).to_string())
                    .or_insert_with(|| empty_value_for_next(&path[index + 1]));
            }
            PathSegment::Index(item_index) => {
                if !current.is_array() {
                    *current = Value::Array(Vec::new());
                }
                let array = current.as_array_mut().expect("array initialized");
                if array.len() <= *item_index {
                    array.resize_with(item_index + 1, || Value::Null);
                }
                if last {
                    array[*item_index] = value;
                    return Ok(());
                }
                if array[*item_index].is_null() {
                    array[*item_index] = empty_value_for_next(&path[index + 1]);
                }
                current = &mut array[*item_index];
            }
        }
    }
    Ok(())
}

/// 业务作用: 根据下一段路径是字段还是下标创建空对象或空数组。
/// 参数说明:
/// - `next`: 准备写入或切换到的下一状态，用于与当前状态比较并避免重复更新。
///
fn empty_value_for_next(next: &PathSegment) -> Value {
    match next {
        PathSegment::Key(_) => Value::Object(Map::new()),
        PathSegment::Index(_) => Value::Array(Vec::new()),
    }
}

impl AdminConfig {
    /// 业务作用: 校验管理监听和认证组合，禁止启用管理端却既无令牌也未显式允许匿名访问。
    /// 返回: 全部配置结构和安全约束满足时返回成功；首个非法字段或冲突返回带路径错误。
    fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        self.listen.parse::<SocketAddr>().with_context(|| {
            format!(
                "admin.listen `{}` is not a valid socket address",
                self.listen
            )
        })?;
        if self.allow_unauthenticated {
            return Ok(());
        }
        if self.token.as_deref().unwrap_or_default().trim().is_empty() {
            bail!("admin.token is required unless admin.allow_unauthenticated=true");
        }
        if self
            .token
            .as_deref()
            .is_some_and(|token| token == "change-me-admin-token")
        {
            bail!("admin.token must be changed from the built-in placeholder");
        }
        Ok(())
    }
}

impl ProxyItem {
    /// 业务作用: 返回中立托管复制 HA 配置。
    ///
    /// 返回: `replication-ha` 缺少 `ha` 子配置时返回带代理名称的启动错误。
    pub fn replication_ha_config(&self) -> anyhow::Result<&ReplicationHaConfig> {
        self.ha.as_ref().with_context(|| {
            format!(
                "proxy `{}` mode=replication-ha requires ha configuration",
                self.name
            )
        })
    }

    /// 业务作用: 校验中立 HA、产品准入和各后端专属持久化子配置。
    ///
    /// 参数:
    /// - `ha`: 当前 `replication-ha` 代理项的中立 HA 配置。
    fn validate_replication_ha_config(
        &self,
        ha: &ReplicationHaConfig,
        coordination_enabled: bool,
    ) -> anyhow::Result<()> {
        // 组了内置协调集群即多实例部署：HA 动作必须由 leader 串行执行，
        // 只允许 proxy-cluster（或省略后自动解析为 proxy-cluster）。显式配 none/local-file/redis
        // 会造成多实例各自切主的双主风险，直接拒绝。
        if coordination_enabled {
            if let Some(explicit) = ha.coordinator.mode {
                if explicit != HaCoordinatorMode::ProxyCluster {
                    bail!(
                        "proxy `{}` runs in a coordination cluster (coordination.enabled=true);                          ha.coordinator.mode must be proxy-cluster or omitted, not {}",
                        self.name,
                        explicit.as_str()
                    );
                }
            }
        }
        // 观察者是"彻底不是控制面"的档位：任何会改写后端的能力都必须在启动期拒绝，
        // 而不是运行期静默忽略，否则运维会误以为配置生效。
        if ha.observer {
            if coordination_enabled {
                bail!(
                    "proxy `{}` ha.observer=true must not join a coordination cluster; observers never vote, lead, or execute coordinated actions",
                    self.name
                );
            }
            if ha
                .coordinator
                .mode
                .is_some_and(|mode| mode != HaCoordinatorMode::None)
            {
                bail!(
                    "proxy `{}` ha.observer=true requires ha.coordinator.mode=none (or omitted); observers take no leases and run no coordinated actions",
                    self.name
                );
            }
            if ha.persistence.enabled {
                bail!(
                    "proxy `{}` ha.observer=true cannot orchestrate dynamic persistence; set ha.persistence.enabled=false",
                    self.name
                );
            }
            if ha.fencing.mode == HaFencingMode::ExternalCommand {
                bail!(
                    "proxy `{}` ha.observer=true never executes fencing; keep ha.fencing.mode=manual",
                    self.name
                );
            }
        }
        if ha.probe_interval_ms == 0 {
            bail!("proxy `{}` ha.probe_interval_ms must be > 0", self.name);
        }
        if ha.probe_connect_timeout_ms == Some(0) {
            bail!(
                "proxy `{}` ha.probe_connect_timeout_ms must be > 0 when configured",
                self.name
            );
        }
        if ha.probe_request_timeout_ms == Some(0) {
            bail!(
                "proxy `{}` ha.probe_request_timeout_ms must be > 0 when configured",
                self.name
            );
        }
        if ha.master_down_after_failures == 0 {
            bail!(
                "proxy `{}` ha.master_down_after_failures must be > 0",
                self.name
            );
        }
        if ha.fencing.timeout_ms == 0 {
            bail!("proxy `{}` ha.fencing.timeout_ms must be > 0", self.name);
        }
        match ha.fencing.mode {
            HaFencingMode::Manual => {
                // manual 是软隔离档位：自动切主跳过外部隔离命令，靠切主后把旧主 REPLICAOF
                // 降级为副本收敛（Redis Sentinel 级默认），因此不再强制 external-command。
                // 需要零脑裂窗口的强隔离时改用 external-command。
                if !ha.fencing.command.is_empty() {
                    bail!(
                        "proxy `{}` ha.fencing.command requires mode=external-command",
                        self.name
                    );
                }
            }
            HaFencingMode::ExternalCommand => {
                if ha.fencing.command.is_empty()
                    || ha.fencing.command.iter().any(|part| part.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` external HA fencing requires non-empty command parts",
                        self.name
                    );
                }
            }
        }
        if ha.coordinator.lease_ttl_ms == 0
            || ha.coordinator.lease_renew_interval_ms == 0
            || ha.coordinator.lease_acquire_timeout_ms == 0
            || ha.coordinator.observation_window_ms == 0
            || ha.coordinator.redis_connect_timeout_ms == 0
            || ha.coordinator.redis_request_timeout_ms == 0
        {
            bail!("proxy `{}` HA coordinator timeouts must be > 0", self.name);
        }
        if ha.coordinator.lease_renew_interval_ms >= ha.coordinator.lease_ttl_ms {
            bail!(
                "proxy `{}` HA coordinator renew interval must be less than lease TTL",
                self.name
            );
        }
        if ha
            .coordinator
            .owner_id
            .as_deref()
            .is_some_and(|owner| owner.trim().is_empty())
        {
            bail!("proxy `{}` HA coordinator owner_id is empty", self.name);
        }
        if ha
            .coordinator
            .group_id
            .as_deref()
            .is_some_and(|group| group.trim().is_empty())
        {
            bail!("proxy `{}` HA coordinator group_id is empty", self.name);
        }
        // Kvrocks 的产品硬约束必须先于通用 coordinator 要求报错:否则运维会先被引导
        // 去补协调配置,配置完成后才发现该产品根本不允许开启 NARIX 托管持久化。
        if self.backend_profile == BackendProfile::Kvrocks && ha.persistence.enabled {
            bail!(
                "proxy `{}` Kvrocks keeps every node persistent and requires ha.persistence.enabled=false",
                self.name
            );
        }
        match ha.coordinator.effective_mode(coordination_enabled) {
            HaCoordinatorMode::None => {
                if ha.coordinator.owner_id.is_some()
                    || ha.coordinator.lease_path.is_some()
                    || ha.coordinator.group_id.is_some()
                    || ha.coordinator.redis_addr.is_some()
                    || ha.coordinator.redis_username.is_some()
                    || ha.coordinator.redis_password.is_some()
                    || ha.coordinator.redis_key.is_some()
                    || ha.coordinator.observation_quorum != 0
                {
                    bail!(
                        "proxy `{}` HA coordinator backend fields require a matching mode",
                        self.name
                    );
                }
                // 单实例自动切主由进程内 failover_lock 串行化，coordinator=none 安全。
                // 多实例部署必须显式配置协调器（proxy-cluster/redis），否则会双主——见文档强警告。
                // 动态持久化交接涉及数据安全，保持强制协调器。
                if ha.persistence.enabled {
                    bail!(
                        "proxy `{}` dynamic persistence requires an HA coordinator",
                        self.name
                    );
                }
            }
            HaCoordinatorMode::LocalFile => {
                if ha.coordinator.group_id.is_some()
                    || ha.coordinator.redis_addr.is_some()
                    || ha.coordinator.redis_username.is_some()
                    || ha.coordinator.redis_password.is_some()
                    || ha.coordinator.redis_key.is_some()
                    || ha.coordinator.observation_quorum != 0
                {
                    bail!(
                        "proxy `{}` HA local-file coordinator must not set Redis or proxy-cluster fields",
                        self.name
                    );
                }
                if ha
                    .coordinator
                    .lease_path
                    .as_deref()
                    .is_none_or(|path| path.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` HA local-file coordinator requires lease_path",
                        self.name
                    );
                }
            }
            HaCoordinatorMode::Redis => {
                if ha.coordinator.lease_path.is_some()
                    || ha.coordinator.group_id.is_some()
                    || ha.coordinator.observation_quorum != 0
                {
                    bail!(
                        "proxy `{}` HA Redis coordinator must not set local-file or proxy-cluster fields",
                        self.name
                    );
                }
                let redis_addr = ha
                    .coordinator
                    .redis_addr
                    .as_deref()
                    .filter(|addr| !addr.trim().is_empty())
                    .with_context(|| {
                        format!(
                            "proxy `{}` HA Redis coordinator requires redis_addr",
                            self.name
                        )
                    })?;
                normalize_addr(redis_addr).with_context(|| {
                    format!(
                        "proxy `{}` HA Redis coordinator redis_addr `{redis_addr}`",
                        self.name
                    )
                })?;
                if ha
                    .coordinator
                    .redis_key
                    .as_deref()
                    .is_none_or(|key| key.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` HA Redis coordinator requires redis_key",
                        self.name
                    );
                }
                if ha
                    .coordinator
                    .redis_username
                    .as_deref()
                    .is_some_and(|username| username.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` HA Redis coordinator redis_username is empty",
                        self.name
                    );
                }
                if ha
                    .coordinator
                    .redis_password
                    .as_deref()
                    .is_some_and(str::is_empty)
                {
                    bail!(
                        "proxy `{}` HA Redis coordinator redis_password is empty",
                        self.name
                    );
                }
                if ha.coordinator.redis_username.is_some()
                    && ha.coordinator.redis_password.is_none()
                {
                    bail!(
                        "proxy `{}` HA Redis coordinator redis_username requires redis_password",
                        self.name
                    );
                }
            }
            HaCoordinatorMode::ProxyCluster => {
                if ha.coordinator.lease_path.is_some()
                    || ha.coordinator.redis_addr.is_some()
                    || ha.coordinator.redis_username.is_some()
                    || ha.coordinator.redis_password.is_some()
                    || ha.coordinator.redis_key.is_some()
                {
                    bail!(
                        "proxy `{}` HA proxy-cluster coordinator must not set local-file or Redis fields",
                        self.name
                    );
                }
                // group_id 省略时默认取代理项名称（effective_group_id），因此不再强制显式配置；
                // 但显式写空白值仍属误配。
                if ha
                    .coordinator
                    .group_id
                    .as_deref()
                    .is_some_and(|group| group.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` HA proxy-cluster coordinator group_id must not be blank",
                        self.name
                    );
                }
            }
        }
        match self.backend_profile {
            BackendProfile::Dragonfly => {
                if ha.persistence.redis.is_some()
                    || ha.persistence.valkey.is_some()
                    || ha.persistence.redict.is_some()
                    || ha.persistence.keydb.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=dragonfly must not set Redis-family persistence fields",
                        self.name
                    );
                }
                let Some(dragonfly) = ha.persistence.dragonfly.as_ref() else {
                    if ha.persistence.enabled {
                        bail!(
                            "proxy `{}` enabled Dragonfly persistence requires ha.persistence.dragonfly",
                            self.name
                        );
                    }
                    return Ok(());
                };
                if dragonfly.verify_timeout_ms == 0
                    || dragonfly.snapshot_cron.trim().is_empty()
                    || dragonfly.dbfilename.trim().is_empty()
                {
                    bail!(
                        "proxy `{}` Dragonfly persistence requires snapshot_cron, dbfilename and verify_timeout_ms > 0",
                        self.name
                    );
                }
            }
            BackendProfile::Redis => {
                if ha.persistence.dragonfly.is_some()
                    || ha.persistence.valkey.is_some()
                    || ha.persistence.redict.is_some()
                    || ha.persistence.keydb.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=redis must only set ha.persistence.redis",
                        self.name
                    );
                }
                let Some(redis) = ha.persistence.redis.as_ref() else {
                    if ha.persistence.enabled {
                        bail!(
                            "proxy `{}` ha.persistence.enabled=true requires ha.persistence.redis",
                            self.name
                        );
                    }
                    return Ok(());
                };
                if redis.verify_timeout_ms == 0 {
                    bail!(
                        "proxy `{}` ha.persistence.redis.verify_timeout_ms must be > 0",
                        self.name
                    );
                }
                if !matches!(redis.aof.fsync.as_str(), "always" | "everysec" | "no") {
                    bail!(
                        "proxy `{}` ha.persistence.redis.aof.fsync must be always, everysec, or no",
                        self.name
                    );
                }
                if redis
                    .rdb
                    .save_rules
                    .iter()
                    .any(|rule| rule.seconds == 0 || rule.changes == 0)
                {
                    bail!(
                        "proxy `{}` Redis RDB save rules require seconds > 0 and changes > 0",
                        self.name
                    );
                }
                if redis.rdb.enabled && redis.rdb.save_rules.is_empty() {
                    bail!(
                        "proxy `{}` enabled Redis RDB requires at least one save rule",
                        self.name
                    );
                }
                if ha.persistence.enabled && !redis.rdb.enabled && !redis.aof.enabled {
                    bail!(
                        "proxy `{}` Redis HA persistence must enable RDB or AOF",
                        self.name
                    );
                }
            }
            BackendProfile::Valkey => {
                if ha.persistence.dragonfly.is_some()
                    || ha.persistence.redis.is_some()
                    || ha.persistence.redict.is_some()
                    || ha.persistence.keydb.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=valkey must only set ha.persistence.valkey",
                        self.name
                    );
                }
                let Some(valkey) = ha.persistence.valkey.as_ref() else {
                    if ha.persistence.enabled {
                        bail!(
                            "proxy `{}` ha.persistence.enabled=true requires ha.persistence.valkey",
                            self.name
                        );
                    }
                    return Ok(());
                };
                if valkey.verify_timeout_ms == 0 {
                    bail!(
                        "proxy `{}` ha.persistence.valkey.verify_timeout_ms must be > 0",
                        self.name
                    );
                }
                if !matches!(valkey.aof.fsync.as_str(), "always" | "everysec" | "no") {
                    bail!(
                        "proxy `{}` ha.persistence.valkey.aof.fsync must be always, everysec, or no",
                        self.name
                    );
                }
                if valkey
                    .rdb
                    .save_rules
                    .iter()
                    .any(|rule| rule.seconds == 0 || rule.changes == 0)
                {
                    bail!(
                        "proxy `{}` Valkey RDB save rules require seconds > 0 and changes > 0",
                        self.name
                    );
                }
                if valkey.rdb.enabled && valkey.rdb.save_rules.is_empty() {
                    bail!(
                        "proxy `{}` enabled Valkey RDB requires at least one save rule",
                        self.name
                    );
                }
                if ha.persistence.enabled && !valkey.rdb.enabled && !valkey.aof.enabled {
                    bail!(
                        "proxy `{}` Valkey HA persistence must enable RDB or AOF",
                        self.name
                    );
                }
            }
            BackendProfile::Redict => {
                if ha.persistence.dragonfly.is_some()
                    || ha.persistence.redis.is_some()
                    || ha.persistence.valkey.is_some()
                    || ha.persistence.keydb.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=redict must only set ha.persistence.redict",
                        self.name
                    );
                }
                let Some(redict) = ha.persistence.redict.as_ref() else {
                    if ha.persistence.enabled {
                        bail!(
                            "proxy `{}` ha.persistence.enabled=true requires ha.persistence.redict",
                            self.name
                        );
                    }
                    return Ok(());
                };
                if redict.verify_timeout_ms == 0 {
                    bail!(
                        "proxy `{}` ha.persistence.redict.verify_timeout_ms must be > 0",
                        self.name
                    );
                }
                if !matches!(redict.aof.fsync.as_str(), "always" | "everysec" | "no") {
                    bail!(
                        "proxy `{}` ha.persistence.redict.aof.fsync must be always, everysec, or no",
                        self.name
                    );
                }
                if redict
                    .rdb
                    .save_rules
                    .iter()
                    .any(|rule| rule.seconds == 0 || rule.changes == 0)
                {
                    bail!(
                        "proxy `{}` Redict RDB save rules require seconds > 0 and changes > 0",
                        self.name
                    );
                }
                if redict.rdb.enabled && redict.rdb.save_rules.is_empty() {
                    bail!(
                        "proxy `{}` enabled Redict RDB requires at least one save rule",
                        self.name
                    );
                }
                if ha.persistence.enabled && !redict.rdb.enabled && !redict.aof.enabled {
                    bail!(
                        "proxy `{}` Redict HA persistence must enable RDB or AOF",
                        self.name
                    );
                }
            }
            BackendProfile::Kvrocks => {
                if ha.persistence.dragonfly.is_some()
                    || ha.persistence.redis.is_some()
                    || ha.persistence.valkey.is_some()
                    || ha.persistence.redict.is_some()
                    || ha.persistence.keydb.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=kvrocks must not set NARIX-managed persistence fields",
                        self.name
                    );
                }
                if ha.persistence.enabled {
                    bail!(
                        "proxy `{}` Kvrocks keeps every node persistent and requires ha.persistence.enabled=false",
                        self.name
                    );
                }
                if self.username.is_some() {
                    bail!(
                        "proxy `{}` Kvrocks replication HA requires global password authentication without username",
                        self.name
                    );
                }
            }
            BackendProfile::KeyDb => {
                if ha.persistence.dragonfly.is_some()
                    || ha.persistence.redis.is_some()
                    || ha.persistence.valkey.is_some()
                    || ha.persistence.redict.is_some()
                {
                    bail!(
                        "proxy `{}` backend_profile=keydb must only set ha.persistence.keydb",
                        self.name
                    );
                }
                let Some(keydb) = ha.persistence.keydb.as_ref() else {
                    if ha.persistence.enabled {
                        bail!(
                            "proxy `{}` ha.persistence.enabled=true requires ha.persistence.keydb",
                            self.name
                        );
                    }
                    return Ok(());
                };
                if keydb.verify_timeout_ms == 0 {
                    bail!(
                        "proxy `{}` ha.persistence.keydb.verify_timeout_ms must be > 0",
                        self.name
                    );
                }
                if !matches!(keydb.aof.fsync.as_str(), "always" | "everysec" | "no") {
                    bail!(
                        "proxy `{}` ha.persistence.keydb.aof.fsync must be always, everysec, or no",
                        self.name
                    );
                }
                if keydb
                    .rdb
                    .save_rules
                    .iter()
                    .any(|rule| rule.seconds == 0 || rule.changes == 0)
                {
                    bail!(
                        "proxy `{}` KeyDB RDB save rules require seconds > 0 and changes > 0",
                        self.name
                    );
                }
                if keydb.rdb.enabled && keydb.rdb.save_rules.is_empty() {
                    bail!(
                        "proxy `{}` enabled KeyDB RDB requires at least one save rule",
                        self.name
                    );
                }
                if ha.persistence.enabled && !keydb.rdb.enabled && !keydb.aof.enabled {
                    bail!(
                        "proxy `{}` KeyDB HA persistence must enable RDB or AOF",
                        self.name
                    );
                }
            }
            profile => bail!(
                "proxy `{}` backend_profile={} is not implemented for replication-ha",
                self.name,
                profile.as_str()
            ),
        }
        Ok(())
    }

    /// 业务作用: 计算 listener 的实际前端认证模式，兼容旧配置同时让显式 `auth_mode` 优先。
    /// 返回: 显式配置优先；存在 `frontend_password` 时默认 terminate；否则默认 backend-inject。
    // 保留未显式配置 auth_mode 的兼容推导；一旦显式配置就不得再由密码字段改写语义。
    pub fn effective_auth_mode(&self) -> AuthMode {
        match self.auth_mode {
            Some(mode) => mode,
            None if self.frontend_password.is_some() => AuthMode::Terminate,
            None => AuthMode::BackendInject,
        }
    }

    /// 业务作用: 生成当前 proxy listener 的绑定地址。
    /// 返回: `listen_host:listen` 格式的地址字符串。
    // 地址合法性在统一 validate 阶段处理，此处只生成稳定的冲突检测键。
    pub fn bind_addr(&self) -> String {
        format!("{}:{}", self.listen_host, self.listen)
    }

    /// 业务作用: 展开一个 proxy item 实际会绑定的所有监听地址，用于启动前端口冲突检测。
    /// 返回: 普通模式返回单个 listener；cluster-node-proxy 返回所有子节点 listener。
    // cluster-node-proxy 没有父级业务 listener，必须展开全部子 listener，避免漏报端口冲突。
    pub fn listen_addrs(&self) -> anyhow::Result<Vec<String>> {
        if self.mode == ProxyMode::ClusterNodeProxy {
            return Ok(self
                .cluster_node_proxies()?
                .iter()
                .map(|node| node.bind_addr(self))
                .collect());
        }
        Ok(vec![self.bind_addr()])
    }

    /// 业务作用: 生成固定后端地址，用于 standalone 和固定上游代理模式。
    /// 返回: `host:port` 字符串；缺少 host 或 port 时带 proxy name 返回错误。
    // 在拼接前分别拒绝缺失字段，使错误能精确指出当前 proxy 的 host 或 port。
    pub fn backend_addr(&self) -> anyhow::Result<String> {
        let host = self
            .host
            .as_ref()
            .with_context(|| format!("proxy `{}` requires host", self.name))?;
        let port = self
            .port
            .with_context(|| format!("proxy `{}` requires port", self.name))?;
        Ok(format!("{host}:{port}"))
    }

    /// 业务作用: 解析 Sentinel 控制节点列表，保持配置顺序并在启动前拒绝空地址和非法端口。
    ///
    /// 返回：返回规范化并去重的 `host:port` 地址列表；未配置或全部为空时返回错误。
    pub fn sentinel_node_addrs(&self) -> anyhow::Result<Vec<String>> {
        let raw = self.sentinel_nodes.as_deref().with_context(|| {
            format!(
                "proxy `{}` sentinel mode requires sentinel_nodes",
                self.name
            )
        })?;
        let mut seen = HashSet::new();
        let mut nodes = Vec::new();
        for token in raw
            .split(',')
            .map(str::trim)
            .filter(|token| !token.is_empty())
        {
            let normalized = normalize_addr(token)
                .with_context(|| format!("proxy `{}` sentinel node `{token}`", self.name))?;
            if seen.insert(normalized.clone()) {
                nodes.push(normalized);
            }
        }
        if nodes.is_empty() {
            bail!(
                "proxy `{}` sentinel mode requires at least one sentinel node",
                self.name
            );
        }
        Ok(nodes)
    }

    /// 业务作用: 解析 Redis Cluster seed 列表，并在启动前校验每个 seed 都是合法 `host:port`。
    /// 返回: 清理空白后的 seed 地址列表；空列表、非法端口或空 host 返回错误。
    // seed 在启动前逐项校验，避免把格式错误延迟到后台拓扑刷新后才暴露。
    pub fn cluster_nodes(&self) -> anyhow::Result<Vec<String>> {
        let nodes = self
            .nodes
            .as_deref()
            .with_context(|| format!("proxy `{}` cluster mode requires nodes", self.name))?;
        let parsed: Vec<String> = nodes
            .split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        if parsed.is_empty() {
            bail!(
                "proxy `{}` cluster mode requires at least one node",
                self.name
            );
        }
        for node in &parsed {
            let Some((host, port)) = node.rsplit_once(':') else {
                bail!(
                    "proxy `{}` cluster node `{node}` must be host:port",
                    self.name
                );
            };
            if host.trim().is_empty() {
                bail!("proxy `{}` cluster node `{node}` host is empty", self.name);
            }
            let port: u16 = port
                .parse()
                .with_context(|| format!("proxy `{}` cluster node `{node}` port", self.name))?;
            if port == 0 {
                bail!(
                    "proxy `{}` cluster node `{node}` port must be > 0",
                    self.name
                );
            }
        }
        Ok(parsed)
    }

    /// 业务作用: 解析通用节点列表，供 cluster 和 replication-ha 等多节点模式复用。
    /// 返回: 非空节点 token 列表；缺少配置或解析后为空时返回错误。
    // 这里只保留配置顺序并过滤空项；产品特定地址语义由后续模式校验负责。
    pub fn node_tokens(&self) -> anyhow::Result<Vec<String>> {
        let nodes = self
            .nodes
            .as_deref()
            .with_context(|| format!("proxy `{}` mode requires nodes", self.name))?;
        let parsed: Vec<String> = nodes
            .split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        if parsed.is_empty() {
            bail!("proxy `{}` mode requires at least one node", self.name);
        }
        Ok(parsed)
    }

    /// 业务作用: 读取 cluster-node-proxy 子 listener 配置，确保节点级一对一代理不会以空列表启动。
    /// 返回: 子节点代理配置切片；缺少或为空时返回带 proxy name 的错误。
    // 空子列表不能退化成一个没有 listener 的“成功启动”代理项。
    pub fn cluster_node_proxies(&self) -> anyhow::Result<&[ClusterNodeProxyItem]> {
        let nodes = self.node_proxies.as_deref().with_context(|| {
            format!(
                "proxy `{}` cluster-node-proxy mode requires node_proxies",
                self.name
            )
        })?;
        if nodes.is_empty() {
            bail!(
                "proxy `{}` cluster-node-proxy mode requires at least one node_proxies item",
                self.name
            );
        }
        Ok(nodes)
    }

    /// 业务作用: 校验单代理的模式、产品能力、认证、监听、资源限制以及 HA 安全约束。
    /// 返回: 全部配置结构和安全约束满足时返回成功；首个非法字段或冲突返回带路径错误。
    fn validate(&self, coordination_enabled: bool) -> anyhow::Result<()> {
        if self.name.trim().is_empty() {
            bail!("proxy name must not be empty");
        }
        validate_resp_tls_client(
            &self.name,
            "backend_tls",
            self.backend_tls_enabled,
            &self.backend_tls_ca_path,
            &self.backend_tls_server_name,
            &self.backend_tls_certificate_path,
            &self.backend_tls_private_key_path,
        )?;
        validate_resp_tls_client(
            &self.name,
            "sentinel_tls",
            self.sentinel_tls_enabled,
            &self.sentinel_tls_ca_path,
            &self.sentinel_tls_server_name,
            &self.sentinel_tls_certificate_path,
            &self.sentinel_tls_private_key_path,
        )?;
        let backend_capabilities = self.backend_profile.capabilities();
        if matches!(self.mode, ProxyMode::Cluster | ProxyMode::ClusterNodeProxy)
            && !backend_capabilities.standard_cluster
        {
            bail!(
                "proxy `{}` backend_profile={} does not support standard cluster mode `{}`",
                self.name,
                self.backend_profile.as_str(),
                match self.mode {
                    ProxyMode::Cluster => "cluster",
                    ProxyMode::ClusterNodeProxy => "cluster-node-proxy",
                    ProxyMode::Standalone | ProxyMode::Sentinel | ProxyMode::ReplicationHa => {
                        unreachable!()
                    }
                }
            );
        }
        if self.mode == ProxyMode::Sentinel && !backend_capabilities.sentinel {
            bail!(
                "proxy `{}` mode=sentinel is not certified for backend_profile={}",
                self.name,
                self.backend_profile.as_str()
            );
        }
        if self.mode == ProxyMode::ReplicationHa && !backend_capabilities.replication_ha {
            bail!(
                "proxy `{}` mode=replication-ha is not implemented for backend_profile={}",
                self.name,
                self.backend_profile.as_str()
            );
        }
        if self.mode == ProxyMode::ReplicationHa {
            self.validate_replication_ha_config(
                self.replication_ha_config()?,
                coordination_enabled,
            )?;
        } else if self.ha.is_some() {
            bail!(
                "proxy `{}` ha configuration requires mode=replication-ha",
                self.name
            );
        }
        if self.mode != ProxyMode::ClusterNodeProxy && self.listen == 0 {
            bail!("proxy `{}` listen must be > 0", self.name);
        }
        if self.max_connections == 0 {
            bail!("proxy `{}` max_connections must be > 0", self.name);
        }
        if self.connect_timeout_ms == 0 {
            bail!("proxy `{}` connect_timeout_ms must be > 0", self.name);
        }
        if self.frontend_auth_timeout_ms == 0 {
            bail!("proxy `{}` frontend_auth_timeout_ms must be > 0", self.name);
        }
        if self.max_auth_bytes == 0 {
            bail!("proxy `{}` max_auth_bytes must be > 0", self.name);
        }
        if self.pipeline_max_commands == 0 {
            bail!("proxy `{}` pipeline_max_commands must be > 0", self.name);
        }
        if self.pipeline_max_bytes == 0 {
            bail!("proxy `{}` pipeline_max_bytes must be > 0", self.name);
        }
        if self.pipeline_backend_max_active == 0 {
            bail!(
                "proxy `{}` pipeline_backend_max_active must be > 0",
                self.name
            );
        }
        if self.pipeline_backend_queue_size == 0 {
            bail!(
                "proxy `{}` pipeline_backend_queue_size must be > 0",
                self.name
            );
        }
        if self.pipeline_backend_submit_timeout_ms == 0 {
            bail!(
                "proxy `{}` pipeline_backend_submit_timeout_ms must be > 0",
                self.name
            );
        }
        if self.dedicated_tunnel_max_active > 0
            && self.dedicated_tunnel_queue_size > 0
            && self.dedicated_tunnel_acquire_timeout_ms == 0
        {
            bail!(
                "proxy `{}` dedicated_tunnel_acquire_timeout_ms must be > 0 when dedicated_tunnel_queue_size > 0",
                self.name
            );
        }
        if self.dedicated_tunnel_max_active > 0
            && self.dedicated_tunnel_pool_max_idle > self.dedicated_tunnel_max_active
        {
            bail!(
                "proxy `{}` dedicated_tunnel_pool_max_idle must be <= dedicated_tunnel_max_active",
                self.name
            );
        }
        if self.frame_max_bytes == 0 {
            bail!("proxy `{}` frame_max_bytes must be > 0", self.name);
        }
        if self.bulk_max_bytes == 0 {
            bail!("proxy `{}` bulk_max_bytes must be > 0", self.name);
        }
        if self.aggregate_max_concurrency == 0 {
            bail!(
                "proxy `{}` aggregate_max_concurrency must be > 0",
                self.name
            );
        }
        if self.aggregate_max_items == 0 {
            bail!("proxy `{}` aggregate_max_items must be > 0", self.name);
        }
        if self.pending_response_bytes == 0 {
            bail!("proxy `{}` pending_response_bytes must be > 0", self.name);
        }
        if self.graceful_shutdown_timeout_ms == 0 {
            bail!(
                "proxy `{}` graceful_shutdown_timeout_ms must be > 0",
                self.name
            );
        }
        if self.cluster_topology_refresh_ms == 0 {
            bail!(
                "proxy `{}` cluster_topology_refresh_ms must be > 0",
                self.name
            );
        }
        if self.backend_request_timeout_ms == 0 {
            bail!(
                "proxy `{}` backend_request_timeout_ms must be > 0",
                self.name
            );
        }
        if self.sentinel_connect_timeout_ms == 0 {
            bail!(
                "proxy `{}` sentinel_connect_timeout_ms must be > 0",
                self.name
            );
        }
        if self.sentinel_request_timeout_ms == 0 {
            bail!(
                "proxy `{}` sentinel_request_timeout_ms must be > 0",
                self.name
            );
        }
        if self.sentinel_refresh_interval_ms == 0 {
            bail!(
                "proxy `{}` sentinel_refresh_interval_ms must be > 0",
                self.name
            );
        }
        if self.frame_max_bytes > self.pipeline_max_bytes {
            bail!(
                "proxy `{}` frame_max_bytes must be <= pipeline_max_bytes",
                self.name
            );
        }

        match self.mode {
            ProxyMode::Standalone => {
                self.backend_addr()?;
            }
            ProxyMode::Sentinel => {
                if option_has_text(&self.host)
                    || self.port.is_some()
                    || option_has_text(&self.nodes)
                    || self.node_proxies.is_some()
                {
                    bail!(
                        "proxy `{}` sentinel mode must not set host, port, nodes or node_proxies",
                        self.name
                    );
                }
                if self
                    .sentinel_master_name
                    .as_deref()
                    .is_none_or(|name| name.trim().is_empty())
                {
                    bail!(
                        "proxy `{}` sentinel mode requires sentinel_master_name",
                        self.name
                    );
                }
                self.sentinel_node_addrs()?;
                if self
                    .sentinel_username
                    .as_deref()
                    .is_some_and(|username| username.trim().is_empty())
                {
                    bail!("proxy `{}` sentinel_username must not be empty", self.name);
                }
                if self
                    .sentinel_password
                    .as_deref()
                    .is_some_and(|password| password.is_empty())
                {
                    bail!("proxy `{}` sentinel_password must not be empty", self.name);
                }
                if option_has_text(&self.sentinel_username) && self.sentinel_password.is_none() {
                    bail!(
                        "proxy `{}` sentinel_username requires sentinel_password",
                        self.name
                    );
                }
            }
            ProxyMode::Cluster => {
                self.cluster_nodes()?;
            }
            ProxyMode::ClusterNodeProxy => {
                let nodes = self.cluster_node_proxies()?;
                let mut listens = HashSet::new();
                let mut backends = HashSet::new();
                for node in nodes {
                    node.validate(self)?;
                    let listen = node.bind_addr(self);
                    if !listens.insert(listen.clone()) {
                        bail!(
                            "proxy `{}` cluster-node-proxy duplicate node listen `{listen}`",
                            self.name
                        );
                    }
                    let normalized = normalize_addr(&node.backend).with_context(|| {
                        format!(
                            "proxy `{}` cluster-node-proxy backend `{}`",
                            self.name, node.backend
                        )
                    })?;
                    if !backends.insert(normalized.clone()) {
                        bail!(
                            "proxy `{}` cluster-node-proxy duplicate backend `{normalized}`",
                            self.name
                        );
                    }
                }
            }
            ProxyMode::ReplicationHa => {
                self.node_tokens()?;
            }
        }

        if self.mode != ProxyMode::Sentinel
            && (option_has_text(&self.sentinel_master_name)
                || option_has_text(&self.sentinel_nodes)
                || option_has_text(&self.sentinel_username)
                || option_has_text(&self.sentinel_password)
                || self.sentinel_tls_enabled
                || option_has_text(&self.sentinel_tls_ca_path)
                || option_has_text(&self.sentinel_tls_server_name)
                || option_has_text(&self.sentinel_tls_certificate_path)
                || option_has_text(&self.sentinel_tls_private_key_path))
        {
            bail!(
                "proxy `{}` sentinel fields require mode=sentinel",
                self.name
            );
        }

        match self.effective_auth_mode() {
            AuthMode::Terminate => {
                if self
                    .frontend_password
                    .as_deref()
                    .unwrap_or_default()
                    .is_empty()
                {
                    bail!(
                        "proxy `{}` auth_mode=terminate requires frontend_password",
                        self.name
                    );
                }
            }
            AuthMode::BackendInject => {
                if !self.allow_unauthenticated {
                    bail!(
                        "proxy `{}` disables frontend auth; set allow_unauthenticated=true explicitly for trusted/test networks",
                        self.name
                    );
                }
            }
            AuthMode::PassThrough => {
                if matches!(
                    self.mode,
                    ProxyMode::Sentinel | ProxyMode::Cluster | ProxyMode::ClusterNodeProxy
                ) {
                    bail!(
                        "proxy `{}` mode={} does not support auth_mode=pass-through; use terminate or backend-inject",
                        self.name,
                        match self.mode {
                            ProxyMode::Standalone => "standalone",
                            ProxyMode::Sentinel => "sentinel",
                            ProxyMode::Cluster => "cluster",
                            ProxyMode::ClusterNodeProxy => "cluster-node-proxy",
                            ProxyMode::ReplicationHa => "replication-ha",
                        }
                    );
                }
                if !self.allow_unauthenticated {
                    bail!(
                        "proxy `{}` auth_mode=pass-through requires allow_unauthenticated=true",
                        self.name
                    );
                }
            }
        }
        Ok(())
    }
}

impl ClusterNodeProxyItem {
    /// 业务作用: 组合节点代理继承后的监听主机和端口，生成实际 socket 绑定地址。
    /// 参数说明:
    /// - `parent`: 父级配置或状态节点，为嵌套项提供继承值和归属关系。
    ///
    // 子项未覆盖监听主机时继承父项，但端口始终使用子项的一对一映射值。
    pub fn bind_addr(&self, parent: &ProxyItem) -> String {
        format!(
            "{}:{}",
            self.listen_host
                .as_deref()
                .unwrap_or(parent.listen_host.as_str()),
            self.listen
        )
    }

    /// 业务作用: 查询 advertise_addr 状态，用于改写 CLUSTER 拓扑和 MOVED/ASK 中的节点地址。
    /// 参数说明:
    /// - `parent`: 父级配置或状态节点，为嵌套项提供继承值和归属关系。
    ///
    /// 返回: 返回节点代理对客户端发布的规范化 host:port；主机、端口或地址非法时返回错误。
    // 公布端口默认等于该子 listener，公布主机则允许子项覆盖父级统一出口地址。
    pub fn advertise_addr(&self, parent: &ProxyItem) -> anyhow::Result<String> {
        let host = self
            .advertise_host
            .as_deref()
            .or(parent.cluster_node_advertise_host.as_deref())
            .with_context(|| {
                format!(
                    "proxy `{}` cluster-node-proxy node `{}` requires advertise_host or cluster_node_advertise_host",
                    parent.name, self.backend
                )
            })?;
        let port = self.advertise_port.unwrap_or(self.listen);
        Ok(format!("{host}:{port}"))
    }

    /// 业务作用: 校验节点代理的固定后端、监听、发布地址和别名均合法且不会形成歧义映射。
    /// 参数说明:
    /// - `parent`: 父级配置或状态节点，为嵌套项提供继承值和归属关系。
    ///
    /// 返回: 全部配置结构和安全约束满足时返回成功；首个非法字段或冲突返回带路径错误。
    fn validate(&self, parent: &ProxyItem) -> anyhow::Result<()> {
        normalize_addr(&self.backend).with_context(|| {
            format!(
                "proxy `{}` cluster-node-proxy node backend `{}` must be host:port",
                parent.name, self.backend
            )
        })?;
        if self.listen == 0 {
            bail!(
                "proxy `{}` cluster-node-proxy node `{}` listen must be > 0",
                parent.name,
                self.backend
            );
        }
        if self.advertise_port == Some(0) {
            bail!(
                "proxy `{}` cluster-node-proxy node `{}` advertise_port must be > 0",
                parent.name,
                self.backend
            );
        }
        let _ = self.advertise_addr(parent)?;
        for alias in &self.aliases {
            normalize_addr(alias).with_context(|| {
                format!(
                    "proxy `{}` cluster-node-proxy node `{}` alias `{}` must be host:port",
                    parent.name, self.backend, alias
                )
            })?;
        }
        Ok(())
    }
}

/// 业务作用: 归一化 配置加载、覆盖合并、环境变量映射和启动前校验 的地址或字段值，使后续比较、映射和校验使用同一口径。
/// 参数说明:
/// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
///
/// 返回: 输入是合法 host:port 时返回统一格式；主机为空、端口缺失或越界时返回错误。
// 规范化 host:port 地址，用于校验 Redis 节点、别名和拓扑改写映射。
pub fn normalize_addr(addr: &str) -> anyhow::Result<String> {
    let Some((host, port)) = addr.rsplit_once(':') else {
        bail!("address `{addr}` must be host:port");
    };
    let host = host.trim();
    if host.is_empty() {
        bail!("address `{addr}` host is empty");
    }
    let port: u16 = port
        .trim()
        .parse()
        .with_context(|| format!("address `{addr}` port"))?;
    if port == 0 {
        bail!("address `{addr}` port must be > 0");
    }
    Ok(format!("{host}:{port}"))
}

/// 业务作用: 把监听地址规范化为主机和端口，供启动前冲突检测统一比较。
#[derive(Debug, Clone)]
struct ListenBind {
    addr: String,
    host: String,
    port: u16,
}

/// 业务作用: 解析 配置加载、覆盖合并、环境变量映射和启动前校验 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
///
/// 返回: 返回用于监听冲突比较的规范化主机和端口；地址或端口非法时返回错误。
fn parse_listen_bind(addr: &str) -> anyhow::Result<ListenBind> {
    let normalized = normalize_addr(addr)?;
    let (host, port) = normalized
        .rsplit_once(':')
        .expect("normalize_addr returned host:port");
    let host = host.to_ascii_lowercase();
    let port = port
        .parse()
        .expect("normalize_addr returned a valid u16 port");
    Ok(ListenBind {
        addr: normalized,
        host,
        port,
    })
}

/// 业务作用: 按通配、回环和具体地址语义判断两个监听是否会占用同一 socket。
/// 参数说明:
/// - `left`: 比较操作左侧的实际或候选值，与右侧共同完成一致性判断。
/// - `right`: 比较操作右侧的期望或参考值，与左侧共同完成一致性判断。
///
fn listen_bind_conflicts(left: &ListenBind, right: &ListenBind) -> bool {
    left.port == right.port
        && (left.host == right.host
            || is_wildcard_listen_host(&left.host)
            || is_wildcard_listen_host(&right.host)
            || (is_loopback_listen_host(&left.host) && is_loopback_listen_host(&right.host)))
}

/// 业务作用: 判断两个监听地址是否会争用同一业务端口，供运行代迁移识别即将释放的旧 listener。
///
/// 参数说明:
/// - `left`: 当前活动运行代的监听地址。
/// - `right`: 候选运行代准备绑定的监听地址。
///
/// 返回: 地址均可解析时返回是否冲突；任一地址非法时返回配置错误。
pub(crate) fn listen_addresses_conflict(left: &str, right: &str) -> anyhow::Result<bool> {
    Ok(listen_bind_conflicts(
        &parse_listen_bind(left)?,
        &parse_listen_bind(right)?,
    ))
}

/// 业务作用: 识别 IPv4 或 IPv6 通配监听地址。
/// 参数说明:
/// - `host`: 主机名或 IP，用于构造网络地址和拓扑改写。
///
fn is_wildcard_listen_host(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::" | "[::]")
}

/// 业务作用: 识别主机名或 IP 形式的本机回环地址。
/// 参数说明:
/// - `host`: 主机名或 IP，用于构造网络地址和拓扑改写。
///
fn is_loopback_listen_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

/// 业务作用: 判断可选字符串存在且去除空白后非空。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
fn option_has_text(value: &Option<String>) -> bool {
    value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

/// 业务作用: 校验 RESP TLS 客户端配置，保证加密启用时始终执行证书身份校验，并约束 mTLS 材料成对出现。
///
/// 参数说明：
/// - `proxy_name`：当前代理名称，用于生成可定位到配置项的启动错误。
/// - `prefix`：字段组前缀，取 `backend_tls` 或 `sentinel_tls`。
/// - `enabled`：是否启用本组 RESP TLS 传输。
/// - `ca_path`：验证服务端证书链的 CA 文件路径。
/// - `server_name`：可选 SNI 和证书身份覆盖值。
/// - `certificate_path`：可选 mTLS 客户端证书路径。
/// - `private_key_path`：可选 mTLS 客户端私钥路径。
///
/// 返回：字段组合安全且完整时成功，否则在监听器启动前返回错误。
fn validate_resp_tls_client(
    proxy_name: &str,
    prefix: &str,
    enabled: bool,
    ca_path: &Option<String>,
    server_name: &Option<String>,
    certificate_path: &Option<String>,
    private_key_path: &Option<String>,
) -> anyhow::Result<()> {
    let fields_present = option_has_text(ca_path)
        || option_has_text(server_name)
        || option_has_text(certificate_path)
        || option_has_text(private_key_path);
    if !enabled && fields_present {
        bail!("proxy `{proxy_name}` {prefix}_* fields require {prefix}_enabled=true");
    }
    if !enabled {
        return Ok(());
    }
    if ca_path.as_deref().is_none_or(|path| path.trim().is_empty()) {
        bail!("proxy `{proxy_name}` {prefix}_enabled=true requires {prefix}_ca_path");
    }
    if server_name
        .as_deref()
        .is_some_and(|name| name.trim().is_empty())
    {
        bail!("proxy `{proxy_name}` {prefix}_server_name must not be empty");
    }
    let has_certificate = option_has_text(certificate_path);
    let has_private_key = option_has_text(private_key_path);
    if has_certificate != has_private_key {
        bail!(
            "proxy `{proxy_name}` {prefix}_certificate_path and {prefix}_private_key_path must be configured together"
        );
    }
    Ok(())
}

/// 业务作用: 返回业务 listener 默认绑定全部 IPv4 网卡的地址；是否允许外部访问由前端认证和部署网络共同约束。
/// 参数说明: 无。
fn default_listen_host() -> String {
    "0.0.0.0".to_string()
}

/// 业务作用: 返回默认允许写命令的数据面只读开关值。
/// 参数说明: 无。
fn default_read_only() -> bool {
    false
}

/// 业务作用: DedicatedTunnel 是特殊命令 1:1 后端连接池外占用；默认给保护上限，避免 Pub/Sub/blocking 连接打满源 maxclients。
/// 参数说明: 无。
// DedicatedTunnel 是特殊命令 1:1 后端连接池外占用；默认给保护上限，避免 Pub/Sub/blocking 连接打满源 maxclients。
fn default_dedicated_tunnel_max_active() -> usize {
    1_024
}

/// 业务作用: 0 表示 active 满后直接 fail-fast；默认允许少量等待，避免瞬时突刺误伤。
/// 参数说明: 无。
// 0 表示 active 满后直接 fail-fast；默认允许少量等待，避免瞬时突刺误伤。
fn default_dedicated_tunnel_queue_size() -> usize {
    1_024
}

/// 业务作用: 返回专用隧道排队获取许可的默认超时。
/// 参数说明: 无。
fn default_dedicated_tunnel_acquire_timeout_ms() -> u64 {
    1_000
}

/// 业务作用: 返回单个后端 tunnel pool 默认允许保留的 clean idle 连接数量。
/// 参数说明: 无。
///
/// 返回:
/// - 默认容量 1000；实际连接仍受 idle timeout 和专用隧道 active 上限约束。
fn default_dedicated_tunnel_pool_max_idle() -> usize {
    1_000
}

/// 业务作用: 返回 tunnel pool 默认空闲连接回收阈值。
/// 参数说明: 无。
///
/// 返回:
/// - 默认 30000 毫秒；连接超过阈值后不再借出，并由后台任务在锁外关闭。
fn default_dedicated_tunnel_pool_idle_timeout_ms() -> u64 {
    30_000
}

/// 业务作用: 返回代理连接后端的默认建连超时。
/// 参数说明: 无。
fn default_connect_timeout_ms() -> u64 {
    3_000
}

/// 业务作用: 返回 Sentinel TCP/TLS 建连的默认超时。
///
/// 参数说明：无。
///
/// 返回：默认 300 毫秒。
fn default_sentinel_connect_timeout_ms() -> u64 {
    300
}

/// 业务作用: 返回单次 Sentinel 查询和候选角色验证的默认请求超时。
///
/// 参数说明：无。
///
/// 返回：默认 1000 毫秒。
fn default_sentinel_request_timeout_ms() -> u64 {
    1_000
}

/// 业务作用: 返回 Sentinel 正常状态下的默认主动刷新周期。
///
/// 参数说明：无。
///
/// 返回：默认 1000 毫秒。
fn default_sentinel_refresh_interval_ms() -> u64 {
    1_000
}

/// 业务作用: 返回普通后端请求的默认响应超时。
/// 参数说明: 无。
fn default_backend_request_timeout_ms() -> u64 {
    10_000
}

/// 业务作用: 返回前端完成认证握手的默认超时。
/// 参数说明: 无。
fn default_frontend_auth_timeout_ms() -> u64 {
    10_000
}

/// 业务作用: 返回单代理允许的默认前端连接上限。
/// 参数说明: 无。
fn default_max_connections() -> usize {
    65_535
}

/// 业务作用: 返回认证阶段允许缓存的默认最大字节数。
/// 参数说明: 无。
fn default_max_auth_bytes() -> usize {
    16 * 1024
}

/// 业务作用: 返回单批 pipeline 允许的默认命令数量上限。
/// 参数说明: 无。
fn default_pipeline_max_commands() -> usize {
    1_000
}

/// 业务作用: 返回单批 pipeline 允许的默认请求字节上限。
/// 参数说明: 无。
fn default_pipeline_max_bytes() -> usize {
    4 * 1024 * 1024
}

/// 业务作用: 返回单节点 pipeline 池允许的默认活动后端连接数。
/// 参数说明: 无。
fn default_pipeline_backend_max_active() -> usize {
    5_000
}

/// 业务作用: 返回单节点 pipeline 池容量耗尽时的默认等待队列大小。
/// 参数说明: 无。
fn default_pipeline_backend_queue_size() -> usize {
    4_096
}

/// 业务作用: 返回 pipeline 等待后端池容量的默认总预算。
/// 参数说明: 无。
fn default_pipeline_backend_submit_timeout_ms() -> u64 {
    1_000
}

/// 业务作用: pipeline pool idle 连接回收阈值:空闲超过该时长的后端连接被后台回收,acquire 时也拒用过龄连接, 避免后端重启后的死 idle 连接逐个毒死前端批次;0 表示不回收(连接常驻)。
/// 参数说明: 无。
// pipeline pool idle 连接回收阈值:空闲超过该时长的后端连接被后台回收,acquire 时也拒用过龄连接,
// 避免后端重启后的死 idle 连接逐个毒死前端批次;0 表示不回收(连接常驻)。
fn default_pipeline_backend_idle_timeout_ms() -> u64 {
    60_000
}

/// 业务作用: 返回单个 RESP frame 的默认最大字节数。
/// 参数说明: 无。
fn default_frame_max_bytes() -> usize {
    4 * 1024 * 1024
}

/// 业务作用: 返回单个 RESP bulk payload 的默认最大字节数。
/// 参数说明: 无。
fn default_bulk_max_bytes() -> usize {
    4 * 1024 * 1024
}

/// 业务作用: 返回跨节点聚合命令允许的默认并发后端数。
/// 参数说明: 无。
fn default_aggregate_max_concurrency() -> usize {
    32
}

/// 业务作用: 返回聚合命令允许处理的默认元素数量上限。
/// 参数说明: 无。
fn default_aggregate_max_items() -> usize {
    10_000
}

/// 业务作用: 返回单连接尚未写回响应允许占用的默认字节上限。
/// 参数说明: 无。
fn default_pending_response_bytes() -> usize {
    16 * 1024 * 1024
}

/// 业务作用: 返回代理运行代优雅关闭的默认等待时长。
/// 参数说明: 无。
fn default_graceful_shutdown_timeout_ms() -> u64 {
    10_000
}

/// 业务作用: 返回 Redis Cluster 拓扑主动刷新的默认周期。
/// 参数说明: 无。
fn default_cluster_topology_refresh_ms() -> u64 {
    60_000
}
