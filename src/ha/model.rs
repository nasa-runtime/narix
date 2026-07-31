use anyhow::{bail, Context};

/// HA 控制面使用的节点地址。
///
/// 客户端地址用于 NARIX 控制连接和数据面路由，复制地址用于后端节点之间建立复制关系。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HaNode {
    /// NARIX 连接该节点时使用的地址。
    pub(crate) client_addr: String,
    /// 其他后端节点连接该节点进行复制时使用的地址。
    pub(crate) replication_addr: String,
}

/// 业务作用: 解析托管复制 HA 的客户端地址和可选复制地址列表。
///
/// 参数:
/// - `raw`: 逗号分隔的 `client_addr@replication_addr` 节点列表。
///
/// 返回: 至少一个地址完整、端口非零的通用 HA 节点。
pub(crate) fn parse_nodes(raw: &str) -> anyhow::Result<Vec<HaNode>> {
    let nodes: Vec<HaNode> = raw
        .split(',')
        .map(str::trim)
        .filter(|node| !node.is_empty())
        .map(|node| {
            let (client_addr, replication_addr) = node
                .split_once('@')
                .map(|(client, replication)| (client.trim(), replication.trim()))
                .unwrap_or((node, node));
            validate_host_port(client_addr)?;
            validate_host_port(replication_addr)?;
            Ok(HaNode {
                client_addr: client_addr.to_string(),
                replication_addr: replication_addr.to_string(),
            })
        })
        .collect::<anyhow::Result<_>>()?;
    if nodes.is_empty() {
        bail!("replication-ha nodes must not be empty");
    }
    Ok(nodes)
}

/// 业务作用: 校验通用 HA 节点地址包含非空主机和非零端口。
///
/// 参数:
/// - `addr`: 要校验的 `host:port` 或 `[IPv6]:port` 地址。
///
/// 返回: 地址可安全用于客户端连接或复制关系时返回成功。
fn validate_host_port(addr: &str) -> anyhow::Result<()> {
    let (host, port) = split_host_port(addr)?;
    if host.trim().is_empty() {
        bail!("node host is empty");
    }
    let port: u16 = port.parse().context("node port is invalid")?;
    if port == 0 {
        bail!("node port must be > 0");
    }
    Ok(())
}

/// HA 控制面归一化后的节点角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HaRole {
    /// 节点当前承担可写主节点角色。
    Master,
    /// 节点当前跟随某个主节点。
    Replica,
    /// 探测失败或后端响应无法确定角色。
    Unknown,
}

impl HaRole {
    /// 业务作用: 返回管理状态、日志和模板使用的稳定角色名称。
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Replica => "replica",
            Self::Unknown => "unknown",
        }
    }
}

/// 产品适配器对一个节点复制状态的归一化观测。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HaNodeObservation {
    /// 当前节点角色。
    pub(crate) role: HaRole,
    /// 副本当前跟随的上游地址；主节点或无法识别时为空。
    pub(crate) upstream_addr: Option<String>,
    /// 后端适配器换算出的秒级延迟；无法安全换算时为空。
    pub(crate) replica_lag_seconds: Option<u64>,
    /// 副本与上游断链的已持续秒数；链路在线、全量同步中或产品无法提供证据时为空。
    ///
    /// 主节点真实宕机后所有副本都会断链，此时该值是复制延迟的可证明下界，
    /// 供切主候选门禁使用；持久化 owner 门禁不使用该值，断链副本仍不能承担持久化。
    pub(crate) link_down_since_seconds: Option<u64>,
}

impl HaNodeObservation {
    /// 业务作用: 判断当前副本观测是否指向指定主节点。
    ///
    /// 参数:
    /// - `master`: 期望副本跟随的主节点。
    pub(crate) fn follows(&self, master: &HaNode) -> bool {
        self.role == HaRole::Replica
            && self.upstream_addr.as_ref().is_some_and(|upstream| {
                upstream == &master.replication_addr || upstream == &master.client_addr
            })
    }

    /// 业务作用: 判断当前观测是否是指定主节点的健康可提升副本。
    ///
    /// 参数:
    /// - `master`: 期望副本跟随的主节点。
    /// - `max_lag_seconds`: 允许的最大已知复制延迟；零只关闭数值上限，不接受未知延迟。
    pub(crate) fn eligible_replica_of(&self, master: &HaNode, max_lag_seconds: u64) -> bool {
        self.follows(master) && replica_lag_is_eligible(self.replica_lag_seconds, max_lag_seconds)
    }
}

/// 业务作用: 判断复制延迟是否足以证明副本可参与拓扑动作。
///
/// 参数:
/// - `lag_seconds`: 产品适配器解析出的复制延迟；空值表示后端没有提供充分证据。
/// - `max_lag_seconds`: 允许的最大已知复制延迟；零表示不设数值上限。
pub(crate) fn replica_lag_is_eligible(lag_seconds: Option<u64>, max_lag_seconds: u64) -> bool {
    let Some(lag_seconds) = lag_seconds else {
        return false;
    };
    lag_seconds != u64::MAX && (max_lag_seconds == 0 || lag_seconds <= max_lag_seconds)
}

/// 业务作用: 计算副本参与切主候选时的有效延迟。
///
/// 主节点宕机后副本必然断链，链路在线语义的延迟哨兵值会把全部候选一票否决，
/// 使自动切主在最核心的主库崩溃场景永远选不出目标。断链时长本身是复制延迟的
/// 可证明下界（断开 N 秒至少落后 N 秒），因此断链副本按断链时长参与同一套
/// `max_replica_lag` 门禁与最小延迟排序；在线副本仍按真实延迟参与。全量同步中
/// 或断链时长不可知的副本保持不可选。
///
/// 参数:
/// - `lag_seconds`: 链路在线语义的复制延迟；断链或全量同步时为哨兵值。
/// - `link_down_since_seconds`: 断链已持续秒数；链路在线或证据缺失时为空。
///
/// 返回: 参与切主门禁与最小延迟排序的有效延迟；断链副本为断链时长（至少一秒），
/// 在线副本为原始延迟，证据不足时保持原哨兵值或空值使候选不可选。
pub(crate) fn replica_failover_lag_seconds(
    lag_seconds: Option<u64>,
    link_down_since_seconds: Option<u64>,
) -> Option<u64> {
    match link_down_since_seconds {
        // 断链下界至少一秒，避免把断链副本洗成零延迟排到在线副本之前。
        Some(down_seconds) if down_seconds != u64::MAX => Some(down_seconds.max(1)),
        _ => lag_seconds,
    }
}

/// 业务作用: 从后端地址拆分主机和端口，并移除 IPv6 方括号。
///
/// 参数:
/// - `addr`: 要拆分的 `host:port` 或 `[IPv6]:port` 地址。
pub(crate) fn split_host_port(addr: &str) -> anyhow::Result<(&str, &str)> {
    if let Some(rest) = addr.strip_prefix('[') {
        let closing = rest
            .find(']')
            .with_context(|| format!("`{addr}` has an unterminated IPv6 host"))?;
        let host = &rest[..closing];
        let port = rest[closing + 1..]
            .strip_prefix(':')
            .with_context(|| format!("`{addr}` must be [IPv6]:port"))?;
        if host.is_empty() || port.is_empty() {
            bail!("`{addr}` must contain a non-empty host and port");
        }
        return Ok((host, port));
    }

    let (host, port) = addr
        .rsplit_once(':')
        .with_context(|| format!("`{addr}` must be host:port"))?;
    if host.contains(':') {
        bail!("IPv6 address `{addr}` must use [IPv6]:port notation");
    }
    if host.is_empty() || port.is_empty() {
        bail!("`{addr}` must contain a non-empty host and port");
    }
    Ok((host, port))
}

/// 业务作用: 把 INFO 返回的主机和端口规范化为可比较的后端地址。
///
/// 参数:
/// - `host`: 后端返回的不带方括号主机名或 IP 地址。
/// - `port`: 后端返回的十进制端口文本。
pub(crate) fn format_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}
