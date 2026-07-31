use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use openraft::LogId;
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::reload::MAX_CLUSTER_CONFIG_BYTES;

/// 保存需要进入协调复制日志和状态机快照的规范化配置正文。
///
/// 该类型的调试输出只展示字节长度，避免配置中的认证信息被日志意外打印；
/// 序列化仍保留完整字节，使未参与热更新的成员可以通过日志追赶恢复精确版本。
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ClusterConfigPayload(Arc<[u8]>);

impl ClusterConfigPayload {
    /// 业务作用: 使用已经完成大小和摘要校验的规范化配置正文创建载荷。
    ///
    /// 参数说明:
    /// - `payload`: 要随协调提交复制并进入状态机快照的规范化 JSON 字节。
    ///
    /// 返回: 调试输出经过脱敏的配置载荷。
    pub(crate) fn new(payload: Vec<u8>) -> Self {
        Self(Arc::from(payload))
    }

    /// 业务作用: 返回配置载荷的只读字节视图。
    ///
    /// 返回: 用于摘要校验、本地落盘和运行代恢复的规范化 JSON 字节。
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for ClusterConfigPayload {
    /// 业务作用: 以紧凑十六进制字符串编码配置正文，避免 JSON 默认把每个字节展开成十进制数组。
    ///
    /// 参数说明:
    /// - `serializer`: 当前协调日志、快照或 RPC 信封使用的 serde 编码器。
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(self.0.len().saturating_mul(2));
        for byte in self.0.iter().copied() {
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0f) as usize] as char);
        }
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for ClusterConfigPayload {
    /// 业务作用: 读取新十六进制正文，同时兼容已有磁盘状态中的旧字节数组表示。
    ///
    /// 参数说明:
    /// - `deserializer`: 当前协调日志、快照或持久化状态使用的 serde 解码器。
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum PayloadEncoding {
            Hex(String),
            Legacy(Vec<u8>),
        }

        match PayloadEncoding::deserialize(deserializer)? {
            PayloadEncoding::Legacy(bytes) => Ok(Self::new(bytes)),
            PayloadEncoding::Hex(encoded) => {
                if encoded.len() % 2 != 0 {
                    return Err(D::Error::custom(
                        "cluster configuration payload hex length must be even",
                    ));
                }
                let mut bytes = Vec::with_capacity(encoded.len() / 2);
                for pair in encoded.as_bytes().chunks_exact(2) {
                    let high = decode_hex_nibble(pair[0]).ok_or_else(|| {
                        D::Error::custom("cluster configuration payload contains invalid hex")
                    })?;
                    let low = decode_hex_nibble(pair[1]).ok_or_else(|| {
                        D::Error::custom("cluster configuration payload contains invalid hex")
                    })?;
                    bytes.push((high << 4) | low);
                }
                Ok(Self::new(bytes))
            }
        }
    }
}

impl fmt::Debug for ClusterConfigPayload {
    /// 业务作用: 输出不含配置正文和凭据的调试摘要。
    ///
    /// 参数说明:
    /// - `formatter`: 标准调试格式化器。
    ///
    /// 返回: 只包含载荷字节长度的格式化结果。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClusterConfigPayload")
            .field("len", &self.0.len())
            .finish()
    }
}

/// 业务作用: 把一个 ASCII 十六进制字符转换成四位数值。
///
/// 参数说明:
/// - `byte`: JSON 字符串中的单个 ASCII 字节。
///
/// 返回: 合法十六进制字符的数值；其他字符返回空。
fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// 业务作用: 表示需要协调集群串行化的托管复制 HA 控制动作，枚举值会进入复制日志并参与动作令牌校验。
/// 分支说明: 故障切换改变主节点；持久化交接改变快照责任节点；拓扑修复处理异常双主或复制关系。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CoordinationActionKind {
    /// 主节点不可达或计划切换时改变唯一可写主节点。
    Failover,
    /// 在副本之间转移快照和持久化所有权。
    PersistenceHandoff,
    /// 在配置关闭动态持久化时清除全部节点的持久化责任。
    PersistenceDisable,
    /// 修复双主、复制方向或启动时缺失主节点等拓扑异常。
    TopologyRepair,
}

/// 业务作用: 描述一个代理节点对托管复制 HA 组的带时间观察，领导者只能基于观察窗口内达到门槛的相同故障事实批准动作。
/// 字段说明: `fingerprint` 必须由主节点、角色集合和故障类型稳定生成，防止把互相矛盾的观察票数相加。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoordinationObservation {
    /// 提交本次观察的静态协调成员编号，同一成员的新观察会覆盖旧值。
    pub reporter_node_id: u64,
    /// 观察产生时的 Unix 毫秒时间，用于限定有效观察窗口。
    pub observed_at_unix_ms: u64,
    /// 对当前主节点和故障事实生成的稳定摘要，相同摘要才能合并计票。
    pub fingerprint: String,
    /// 报告成员观察到的当前主节点地址。
    pub current_master: String,
    /// 报告成员能否在探测门槛内确认当前主节点可达。
    pub master_reachable: bool,
}

/// 业务作用: 表示写入协调复制日志的业务命令，所有会影响外部拓扑的决定必须先经过该命令序列化。
/// 字段说明: 命令只携带确定性数据；状态机不得读取本地时钟或网络，确保每个成员应用后得到相同结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum CoordinationCommand {
    /// 首次登记或核对某个 HA 组的关键配置摘要。
    RegisterGroupConfig {
        /// 进程内唯一且跨成员一致的托管复制 HA 组标识。
        group_id: String,
        /// 对节点、认证、门禁、隔离和持久化配置生成的稳定摘要。
        config_digest: String,
        /// 构造该 HA 运行时所依据的全局集群配置摘要。
        #[serde(default)]
        cluster_config_digest: String,
    },
    /// 提交已经由静态成员多数派落盘并预检通过的集群业务配置版本。
    CommitClusterConfig {
        /// 提案开始时协调状态机已经提交的旧配置版本。
        expected_revision: u64,
        /// 必须严格等于旧版本加一的新配置版本。
        revision: u64,
        /// 全部成员逐字持久化的规范化配置载荷摘要。
        config_digest: String,
        /// 进入复制日志和状态机快照、供掉线成员恢复的规范化配置正文。
        config_payload: ClusterConfigPayload,
    },
    /// 记录一个静态成员对 HA 故障事实的最新观察。
    RecordObservation {
        /// 观察所属的托管复制 HA 组标识。
        group_id: String,
        /// 生成观察的运行时所绑定的全局配置版本。
        #[serde(default)]
        config_revision: u64,
        /// 生成观察的运行时所绑定的 HA 组配置摘要。
        #[serde(default)]
        group_config_digest: String,
        /// 包含报告者、时间和故障摘要的观察内容。
        observation: CoordinationObservation,
    },
    /// 在门槛满足后创建一个绑定目标且不可复用的活动动作。
    BeginAction {
        /// 动作所属的托管复制 HA 组标识。
        group_id: String,
        /// 申请动作的运行时所绑定的全局配置版本。
        #[serde(default)]
        config_revision: u64,
        /// 申请动作的运行时所绑定的 HA 组配置摘要。
        #[serde(default)]
        group_config_digest: String,
        /// 调用方生成的幂等动作标识。
        action_id: String,
        /// 外部动作的受限业务类型。
        kind: CoordinationActionKind,
        /// 请求动作的静态协调成员编号。
        requester_node_id: u64,
        /// 自动动作要求达到观察门槛的故障摘要；人工动作可为空。
        fingerprint: String,
        /// 动作开始前必须匹配已提交状态的主节点。
        expected_master: String,
        /// 动作成功后唯一允许发布的目标主节点。
        target_master: String,
        /// 观察票允许的最早 Unix 毫秒时间。
        observation_cutoff_unix_ms: u64,
        /// 自动动作要求的不同报告成员数量；人工动作使用零。
        required_observations: usize,
        /// 请求动作时的 Unix 毫秒时间。
        requested_at_unix_ms: u64,
        /// 动作令牌不再允许执行或提交的 Unix 毫秒时间。
        expires_at_unix_ms: u64,
    },
    /// 以原动作令牌提交成功结果或显式清理失败动作。
    CompleteAction {
        /// 动作所属的托管复制 HA 组标识。
        group_id: String,
        /// 由组、日志位置、请求者和动作内容共同生成的不可复用令牌。
        token: String,
        /// 外部动作是否已安全完成；失败结果不会改写路由。
        success: bool,
        /// 成功动作完成后复验得到的唯一主节点。
        resulting_master: String,
        /// 动作完成时的 Unix 毫秒时间，用于拒绝过期令牌。
        completed_at_unix_ms: u64,
    },
}

/// 业务作用: 返回复制命令的确定性处理结果，调用方只有在 `accepted=true` 且收到动作令牌后才能执行外部副作用。
/// 字段说明: `route_epoch` 单调递增，用于拒绝把旧主节点重新发布为当前路由。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinationResponse {
    /// 状态机是否接受了业务前置条件并应用预期变更。
    pub accepted: bool,
    /// 开始动作成功时返回的不可复用权威令牌。
    pub action_token: Option<String>,
    /// 每次成功发布新主节点后单调递增的路由代际。
    pub route_epoch: u64,
    /// 命令处理后该组已提交的唯一主节点。
    pub current_master: Option<String>,
    /// 当前协调日志已经提交的集群配置版本。
    pub config_revision: u64,
    /// 当前协调日志已经提交的集群规范化配置摘要。
    pub config_digest: Option<String>,
    /// 命令被拒绝时供调用方诊断的确定性原因。
    pub reason: Option<String>,
}

/// 业务作用: 保存一个已经由复制协议批准、但尚未完成的外部动作；同一 HA 组任意时刻最多存在一个活动动作。
/// 字段说明: 令牌绑定动作内容和提交日志位置，完成命令必须逐字匹配令牌才能清除该状态。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveCoordinationAction {
    /// 调用方生成并用于幂等识别的动作标识。
    pub action_id: String,
    /// 当前正在执行的受限动作类型。
    pub kind: CoordinationActionKind,
    #[serde(default)]
    /// 动作事实或目标拓扑的稳定摘要；旧日志缺失时为空并不得作为新拓扑提交证据。
    pub fingerprint: String,
    /// 获准执行外部副作用的协调成员编号。
    pub requester_node_id: u64,
    /// 动作批准前已提交的主节点。
    pub expected_master: String,
    /// 动作成功后唯一允许发布的主节点。
    pub target_master: String,
    /// 绑定动作内容和批准日志位置的不可复用令牌。
    pub token: String,
    /// 批准该动作的共识日志编号。
    pub approved_log_id: LogId<u64>,
    /// 动作权威失效的 Unix 毫秒时间。
    pub expires_at_unix_ms: u64,
}

/// 业务作用: 保存单个托管复制 HA 组经复制确认的路由、观察和活动动作状态。
/// 字段说明: 观察按报告节点覆盖更新，避免同一节点重复上报被错误计为多票。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinationGroupState {
    #[serde(default)]
    /// 当前全局配置代际对应的 HA 业务配置摘要。
    pub config_digest: Option<String>,
    #[serde(default)]
    /// 当前 HA 摘要所依据的全局集群配置摘要。
    pub cluster_config_digest: Option<String>,
    #[serde(default)]
    /// 当前 HA 摘要已经确认对应的全局配置版本。
    pub config_revision: u64,
    /// 成功发布新主节点时单调递增的路由代际。
    pub route_epoch: u64,
    /// 当前由复制状态机确认的唯一业务主节点。
    pub current_master: Option<String>,
    #[serde(default)]
    /// 当前经协调确认的快照持久化责任节点。
    pub persistence_owner: Option<String>,
    #[serde(default)]
    /// 最近一次成功持久化交接提交的完整拓扑摘要，不只覆盖首个 owner。
    pub persistence_topology_fingerprint: Option<String>,
    /// 按报告成员编号保存的最新故障观察。
    pub observations: BTreeMap<u64, CoordinationObservation>,
    /// 当前尚未成功完成或显式清理的唯一外部动作。
    pub active_action: Option<ActiveCoordinationAction>,
}

/// 业务作用: 保存整个 narix 协调集群的业务状态机，一个进程中的多个 HA 组通过 `group_id` 隔离。
/// 字段说明: BTreeMap 提供稳定序列化顺序，使快照内容和哈希校验不受映射随机顺序影响。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CoordinationState {
    #[serde(default)]
    /// 全部静态成员应当运行的已提交集群配置版本。
    pub config_revision: u64,
    #[serde(default)]
    /// 已提交集群规范化配置载荷摘要。
    pub config_digest: Option<String>,
    #[serde(default)]
    /// 已提交集群规范化配置正文；调试输出不展示其内容。
    pub config_payload: ClusterConfigPayload,
    /// 按稳定组标识隔离保存的所有托管复制 HA 协调状态。
    pub groups: BTreeMap<String, CoordinationGroupState>,
}

impl CoordinationState {
    /// 业务作用: 以复制日志编号为权威顺序应用一条业务命令，生成所有成员完全一致的响应和状态。
    /// 参数说明:
    /// - `command`: 本次由复制协议提交的业务命令。
    /// - `log_id`: 命令的任期与日志位置，用于生成不可复用的动作令牌。
    ///
    /// 返回: 返回确定性的接受结果；业务前置条件不满足时拒绝命令但仍保持日志已应用语义。
    pub fn apply(
        &mut self,
        command: CoordinationCommand,
        log_id: LogId<u64>,
    ) -> CoordinationResponse {
        match command {
            CoordinationCommand::RegisterGroupConfig {
                group_id,
                config_digest,
                cluster_config_digest,
            } => self.register_group_config(group_id, config_digest, cluster_config_digest),
            CoordinationCommand::CommitClusterConfig {
                expected_revision,
                revision,
                config_digest,
                config_payload,
            } => self.commit_cluster_config(
                expected_revision,
                revision,
                config_digest,
                config_payload,
            ),
            CoordinationCommand::RecordObservation {
                group_id,
                config_revision,
                group_config_digest,
                observation,
            } => {
                self.record_observation(group_id, config_revision, group_config_digest, observation)
            }
            CoordinationCommand::BeginAction {
                group_id,
                config_revision,
                group_config_digest,
                action_id,
                kind,
                requester_node_id,
                fingerprint,
                expected_master,
                target_master,
                observation_cutoff_unix_ms,
                required_observations,
                requested_at_unix_ms,
                expires_at_unix_ms,
            } => self.begin_action(
                group_id,
                config_revision,
                group_config_digest,
                action_id,
                kind,
                requester_node_id,
                fingerprint,
                expected_master,
                target_master,
                observation_cutoff_unix_ms,
                required_observations,
                requested_at_unix_ms,
                expires_at_unix_ms,
                log_id,
            ),
            CoordinationCommand::CompleteAction {
                group_id,
                token,
                success,
                resulting_master,
                completed_at_unix_ms,
            } => self.complete_action(
                group_id,
                token,
                success,
                resulting_master,
                completed_at_unix_ms,
                log_id,
            ),
        }
    }

    /// 业务作用: 提交一个已经在静态成员多数派完成安全落盘和预检的集群配置版本。
    ///
    /// 参数说明:
    /// - `expected_revision`: 提案声明的旧活动版本。
    /// - `revision`: 必须严格递增一的新版本。
    /// - `config_digest`: 规范化集群配置载荷摘要。
    /// - `config_payload`: 经过脱敏调试包装、需要随状态机复制的规范化配置正文。
    ///
    /// 返回: 不存在活动 HA 动作且版本前置条件一致时提交新版本，否则保持旧状态。
    fn commit_cluster_config(
        &mut self,
        expected_revision: u64,
        revision: u64,
        config_digest: String,
        config_payload: ClusterConfigPayload,
    ) -> CoordinationResponse {
        if expected_revision < self.config_revision
            && revision <= self.config_revision
            && self.config_digest.as_deref() == Some(config_digest.as_str())
            && self.config_payload.as_slice() == config_payload.as_slice()
        {
            return CoordinationResponse {
                accepted: true,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                ..CoordinationResponse::default()
            };
        }
        if self
            .groups
            .values()
            .any(|group| group.active_action.is_some())
        {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some(
                    "cluster configuration cannot commit while an HA action is active".to_string(),
                ),
                ..CoordinationResponse::default()
            };
        }
        if expected_revision != self.config_revision {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some(format!(
                    "cluster configuration expected revision {expected_revision} but current revision is {}",
                    self.config_revision
                )),
                ..CoordinationResponse::default()
            };
        }
        let Some(next_revision) = expected_revision.checked_add(1) else {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some("cluster configuration revision space is exhausted".to_string()),
                ..CoordinationResponse::default()
            };
        };
        if revision != next_revision {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some(
                    "cluster configuration revision must increase by exactly one".to_string(),
                ),
                ..CoordinationResponse::default()
            };
        }
        if !is_sha256_digest(&config_digest) {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some("cluster configuration digest must be lowercase SHA-256".to_string()),
                ..CoordinationResponse::default()
            };
        }
        if config_payload.as_slice().is_empty()
            || config_payload.as_slice().len() > MAX_CLUSTER_CONFIG_BYTES
        {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some(
                    "cluster configuration payload must be between 1 byte and 16 MiB".to_string(),
                ),
                ..CoordinationResponse::default()
            };
        }
        let payload_digest = format!("{:x}", Sha256::digest(config_payload.as_slice()));
        if payload_digest != config_digest {
            return CoordinationResponse {
                accepted: false,
                config_revision: self.config_revision,
                config_digest: self.config_digest.clone(),
                reason: Some(
                    "cluster configuration payload digest does not match committed digest"
                        .to_string(),
                ),
                ..CoordinationResponse::default()
            };
        }
        self.config_revision = revision;
        self.config_digest = Some(config_digest);
        self.config_payload = config_payload;
        CoordinationResponse {
            accepted: true,
            config_revision: self.config_revision,
            config_digest: self.config_digest.clone(),
            ..CoordinationResponse::default()
        }
    }

    /// 业务作用: 把 HA 组摘要绑定到当前全局配置代际，只有运行目标版本的成员才能参与该组控制。
    /// 参数说明:
    /// - `group_id`: 需要注册或核对的托管复制 HA 组。
    /// - `config_digest`: 节点清单、故障门禁、隔离和持久化策略的规范摘要。
    /// - `cluster_config_digest`: 构造该运行时所依据的全局集群配置摘要。
    ///
    /// 返回: 全局代际匹配且组内没有活动动作时更新摘要；旧配置运行时代注册会被拒绝。
    fn register_group_config(
        &mut self,
        group_id: String,
        config_digest: String,
        cluster_config_digest: String,
    ) -> CoordinationResponse {
        let global_revision = self.config_revision;
        let global_digest = self.config_digest.clone();
        let group = self.groups.entry(group_id).or_default();
        if !is_sha256_digest(&config_digest) {
            return rejected(
                group,
                "group config digest must be lowercase SHA-256".to_string(),
            );
        }
        let legacy_unbound_registration = global_revision == 0 && cluster_config_digest.is_empty();
        if !legacy_unbound_registration && !is_sha256_digest(&cluster_config_digest) {
            return rejected(
                group,
                "group global config digest must be lowercase SHA-256".to_string(),
            );
        }
        if global_revision > 0 && global_digest.as_deref() != Some(cluster_config_digest.as_str()) {
            return rejected(
                group,
                "group runtime was built from a stale cluster configuration".to_string(),
            );
        }
        if group.active_action.is_some()
            && group.config_digest.as_deref() != Some(config_digest.as_str())
        {
            return rejected(
                group,
                "group configuration cannot change while an HA action is active".to_string(),
            );
        }
        let registration_changed = group.config_revision != global_revision
            || group.config_digest.as_deref() != Some(config_digest.as_str())
            || (!legacy_unbound_registration
                && group.cluster_config_digest.as_deref() != Some(cluster_config_digest.as_str()));
        if registration_changed {
            // 观察票只对生成它们时的节点清单、门禁和全局配置代际有效。
            // 新运行时完成注册时必须丢弃旧票，防止旧故障事实直接授权新拓扑动作。
            group.observations.clear();
        }
        let group_was_absent_for_a_committed_revision =
            group.config_revision > 0 && global_revision > group.config_revision.saturating_add(1);
        if group_was_absent_for_a_committed_revision {
            // 同名 HA 代理被删除至少一个配置版本后再加入时属于新生命周期。旧生命周期的
            // 主节点和持久化 owner 不能污染新拓扑；连续版本更新则保留 precommit 已确认
            // 的计划切主结果。
            group.current_master = None;
            group.persistence_owner = None;
            group.persistence_topology_fingerprint = None;
        }
        group.config_digest = Some(config_digest);
        // 旧日志中的注册命令没有全局摘要；只允许它在首个全局版本提交前重放。
        group.cluster_config_digest =
            (!legacy_unbound_registration).then_some(cluster_config_digest);
        group.config_revision = global_revision;
        CoordinationResponse {
            accepted: true,
            route_epoch: group.route_epoch,
            current_master: group.current_master.clone(),
            config_revision: global_revision,
            config_digest: global_digest,
            ..CoordinationResponse::default()
        }
    }

    /// 业务作用: 按报告节点覆盖保存最新观察，确保一个代理节点在同一决策窗口内最多贡献一票。
    /// 参数说明:
    /// - `group_id`: 观察所属托管复制 HA 组。
    /// - `config_revision`: 生成观察的运行时所绑定的全局配置版本。
    /// - `group_config_digest`: 生成观察的运行时所绑定的 HA 组摘要。
    /// - `observation`: 带报告者和时间的观察内容。
    ///
    /// 返回: 返回观察已记录后的当前路由信息。
    fn record_observation(
        &mut self,
        group_id: String,
        config_revision: u64,
        group_config_digest: String,
        observation: CoordinationObservation,
    ) -> CoordinationResponse {
        let global_revision = self.config_revision;
        let group = self.groups.entry(group_id).or_default();
        if group.config_digest.is_none() {
            return rejected(
                group,
                "group configuration must be registered before observations".to_string(),
            );
        }
        if group.config_revision != global_revision {
            return rejected(
                group,
                "group configuration is not registered for the active cluster revision".to_string(),
            );
        }
        if !runtime_binding_matches(group, config_revision, &group_config_digest) {
            return rejected(
                group,
                "observation runtime configuration is stale".to_string(),
            );
        }
        if observation.reporter_node_id == 0
            || observation.fingerprint.is_empty()
            || observation.current_master.is_empty()
        {
            return rejected(
                group,
                "observation requires reporter, fingerprint and current master".to_string(),
            );
        }
        group
            .observations
            .insert(observation.reporter_node_id, observation);
        CoordinationResponse {
            accepted: true,
            route_epoch: group.route_epoch,
            current_master: group.current_master.clone(),
            ..CoordinationResponse::default()
        }
    }

    /// 业务作用: 在观察票数、预期主节点和活动动作均满足约束时批准新动作，并生成绑定日志位置的 fencing 令牌。
    /// 参数说明:
    /// - `group_id`: 动作所属 HA 组。
    /// - `config_revision`: 申请动作的运行时所绑定的全局配置版本。
    /// - `group_config_digest`: 申请动作的运行时所绑定的 HA 组摘要。
    /// - `action_id`: 调用方为本次尝试生成的唯一标识。
    /// - `kind`: 动作类型。
    /// - `requester_node_id`: 请求动作的代理节点编号。
    /// - `fingerprint`: 需要达到观察门槛的故障事实指纹。
    /// - `expected_master`: 动作开始前预期的主节点。
    /// - `target_master`: 动作计划切换到的目标节点。
    /// - `observation_cutoff_unix_ms`: 观察允许参与决策的最早时间。
    /// - `required_observations`: 至少需要多少个不同代理节点确认同一故障事实。
    /// - `requested_at_unix_ms`: 领导者写入本次申请时的显式纪元毫秒，用于确定性判断旧动作是否已过期。
    /// - `expires_at_unix_ms`: 本动作的明确过期时间，必须晚于申请时间。
    /// - `log_id`: 批准命令的任期与日志位置。
    ///
    /// 返回: 前置条件满足时返回动作令牌，否则返回可诊断的拒绝原因。
    #[allow(clippy::too_many_arguments)]
    fn begin_action(
        &mut self,
        group_id: String,
        config_revision: u64,
        group_config_digest: String,
        action_id: String,
        kind: CoordinationActionKind,
        requester_node_id: u64,
        fingerprint: String,
        expected_master: String,
        target_master: String,
        observation_cutoff_unix_ms: u64,
        required_observations: usize,
        requested_at_unix_ms: u64,
        expires_at_unix_ms: u64,
        log_id: LogId<u64>,
    ) -> CoordinationResponse {
        let global_revision = self.config_revision;
        let group = self.groups.entry(group_id.clone()).or_default();
        if group.config_digest.is_none() {
            return rejected(
                group,
                "group configuration must be registered before actions".to_string(),
            );
        }
        if group.config_revision != global_revision {
            return rejected(
                group,
                "group configuration is not registered for the active cluster revision".to_string(),
            );
        }
        if !runtime_binding_matches(group, config_revision, &group_config_digest) {
            return rejected(group, "action runtime configuration is stale".to_string());
        }
        if action_id.is_empty() || requester_node_id == 0 || target_master.is_empty() {
            return rejected(
                group,
                "action requires action id, requester and target master".to_string(),
            );
        }
        // 动作只能由把本命令写入共识日志的当前领导节点申请。跟随者即使知道业务字段，
        // 也不能把 BeginAction 转发给领导者后借用领导者的日志位置生成自己的动作令牌。
        if log_id.leader_id.node_id != requester_node_id {
            return rejected(
                group,
                "action requester is not the leader that appended this command".to_string(),
            );
        }
        if expires_at_unix_ms <= requested_at_unix_ms {
            return rejected(
                group,
                "action expiry must be later than request time".to_string(),
            );
        }
        if observation_cutoff_unix_ms > requested_at_unix_ms {
            return rejected(
                group,
                "observation cutoff must not be later than request time".to_string(),
            );
        }
        if required_observations > 0 && fingerprint.is_empty() {
            return rejected(
                group,
                "automatic action requires a non-empty observation fingerprint".to_string(),
            );
        }
        if let Some(active) = &group.active_action {
            if active.expires_at_unix_ms > requested_at_unix_ms {
                return rejected(
                    group,
                    format!("group already has active action `{}`", active.action_id),
                );
            }
            // 过期替换由新一条已提交日志显式驱动，所有成员使用命令内同一时间值，
            // 不读取各自本地时钟，因此不会因时钟偏差得到不同状态机结果。
            group.active_action = None;
        }
        if let Some(master) = &group.current_master {
            if master != &expected_master {
                return rejected(
                    group,
                    format!(
                        "expected master `{expected_master}` does not match committed master `{master}`"
                    ),
                );
            }
        }
        let votes = group
            .observations
            .values()
            .filter(|observation| {
                observation.observed_at_unix_ms >= observation_cutoff_unix_ms
                    && observation.observed_at_unix_ms <= requested_at_unix_ms
                    && observation.fingerprint == fingerprint
                    && observation.current_master == expected_master
                    && !observation.master_reachable
            })
            .count();
        if votes < required_observations {
            return rejected(
                group,
                format!(
                    "matching observations `{votes}` are below required `{required_observations}`"
                ),
            );
        }
        let token = format!(
            "{}:{}:{}:{}:{}",
            group_id, log_id.leader_id.term, log_id.index, requester_node_id, action_id
        );
        group.active_action = Some(ActiveCoordinationAction {
            action_id,
            kind,
            fingerprint,
            requester_node_id,
            expected_master,
            target_master,
            token: token.clone(),
            approved_log_id: log_id,
            expires_at_unix_ms,
        });
        CoordinationResponse {
            accepted: true,
            action_token: Some(token),
            route_epoch: group.route_epoch,
            current_master: group.current_master.clone(),
            reason: None,
            ..CoordinationResponse::default()
        }
    }

    /// 业务作用: 以动作令牌完成或取消活动动作，成功时单调推进路由代际并发布新主节点。
    /// 参数说明:
    /// - `group_id`: 动作所属 HA 组。
    /// - `token`: `BeginAction` 返回的完整动作令牌。
    /// - `success`: 外部副作用和复验是否全部成功。
    /// - `resulting_master`: 成功后经过复验的主节点地址。
    /// - `completed_at_unix_ms`: 领导者写入完成命令时的显式纪元毫秒，用于拒绝过期动作。
    /// - `log_id`: 完成命令的共识日志编号，用于确认动作仍由原批准领导节点收口。
    ///
    /// 返回: 令牌匹配时返回完成结果；旧令牌、错组令牌或空动作均被拒绝。
    fn complete_action(
        &mut self,
        group_id: String,
        token: String,
        success: bool,
        resulting_master: String,
        completed_at_unix_ms: u64,
        log_id: LogId<u64>,
    ) -> CoordinationResponse {
        let group = self.groups.entry(group_id).or_default();
        let Some(active) = group.active_action.as_ref() else {
            return rejected(group, "group has no active action".to_string());
        };
        if active.token != token {
            return rejected(
                group,
                "action token does not match active action".to_string(),
            );
        }
        // 运行时在每个外部副作用前先复验领导权；这里再把完成日志绑定到批准动作的
        // 完整领导任期。同一节点失去领导权后即使再次当选，也不能在新任期恢复旧动作。
        if log_id.leader_id != active.approved_log_id.leader_id {
            return rejected(
                group,
                "action completion was appended by a different leader term".to_string(),
            );
        }
        if completed_at_unix_ms >= active.expires_at_unix_ms {
            return rejected(group, "action token expired before completion".to_string());
        }
        if success {
            match active.kind {
                CoordinationActionKind::Failover | CoordinationActionKind::TopologyRepair => {
                    if resulting_master != active.target_master {
                        return rejected(
                            group,
                            format!(
                                "resulting master `{resulting_master}` does not match approved target `{}`",
                                active.target_master
                            ),
                        );
                    }
                    let Some(next_route_epoch) = group.route_epoch.checked_add(1) else {
                        return rejected(group, "route epoch is exhausted".to_string());
                    };
                    group.route_epoch = next_route_epoch;
                    group.current_master = Some(resulting_master);
                }
                CoordinationActionKind::PersistenceHandoff => {
                    if resulting_master != active.expected_master {
                        return rejected(
                            group,
                            format!(
                                "persistence handoff master `{resulting_master}` does not match expected master `{}`",
                                active.expected_master
                            ),
                        );
                    }
                    // 后端 owner 配置与 checkpoint 已在动作权威内完成；同一条
                    // CompleteAction 原子提交首 owner 和完整拓扑摘要，跟随者只有
                    // 两者都匹配时才能把远端配置采用为已验证状态。
                    group.persistence_owner = Some(active.target_master.clone());
                    group.persistence_topology_fingerprint =
                        (!active.fingerprint.is_empty()).then(|| active.fingerprint.clone());
                }
                CoordinationActionKind::PersistenceDisable => {
                    if resulting_master != active.expected_master {
                        return rejected(
                            group,
                            format!(
                                "persistence disable master `{resulting_master}` does not match expected master `{}`",
                                active.expected_master
                            ),
                        );
                    }
                    group.persistence_owner = None;
                    group.persistence_topology_fingerprint = None;
                }
            }
        }
        group.active_action = None;
        CoordinationResponse {
            accepted: true,
            route_epoch: group.route_epoch,
            current_master: group.current_master.clone(),
            ..CoordinationResponse::default()
        }
    }
}

/// 业务作用: 构造不改变状态的确定性拒绝响应，统一携带当前路由代际和主节点，便于调用方立即收敛。
/// 参数说明:
/// - `group`: 当前 HA 组状态。
/// - `reason`: 具体业务前置条件失败原因。
///
/// 返回: 返回 `accepted=false` 的协调响应。
fn rejected(group: &CoordinationGroupState, reason: String) -> CoordinationResponse {
    CoordinationResponse {
        accepted: false,
        action_token: None,
        route_epoch: group.route_epoch,
        current_master: group.current_master.clone(),
        reason: Some(reason),
        ..CoordinationResponse::default()
    }
}

/// 业务作用: 判断观察或动作申请是否由当前 HA 组配置运行时生成。
///
/// 参数说明:
/// - `group`: 已绑定全局版本和组摘要的协调状态。
/// - `config_revision`: 调用运行时创建时绑定的全局配置版本。
/// - `group_config_digest`: 调用运行时创建时绑定的 HA 组摘要。
///
/// 返回: 当前格式逐项匹配时返回真；升级前缺少绑定字段的日志只允许在 revision 0 重放。
fn runtime_binding_matches(
    group: &CoordinationGroupState,
    config_revision: u64,
    group_config_digest: &str,
) -> bool {
    if group_config_digest.is_empty() {
        return config_revision == 0 && group.config_revision == 0;
    }
    group.config_revision == config_revision
        && group.config_digest.as_deref() == Some(group_config_digest)
}

/// 业务作用: 判断字符串是否是规范的六十四位小写 SHA-256 十六进制摘要。
///
/// 参数说明:
/// - `value`: 需要校验的摘要文本。
///
/// 返回: 长度和字符范围都满足规范时返回真。
fn is_sha256_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
