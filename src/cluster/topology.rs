use anyhow::{bail, Context};

use crate::cluster::backend::{send_pipeline, BackendConnectOptions, BackendTlsConfig};
use crate::resp::RespValue;
use crate::{resp, resp::parse_value};

/// 业务作用: 表示闭区间槽位范围及其当前主节点地址。
#[derive(Debug, Clone, PartialEq, Eq)]
// 表示 SlotRange 的数据结构，用于解析和维护 Redis Cluster 槽位拓扑。
pub struct SlotRange {
    /// 当前连续槽位范围的起始槽位，包含该值。
    pub start: u16,
    /// 当前连续槽位范围的结束槽位，包含该值。
    pub end: u16,
    /// 负责该槽位范围的主节点规范地址。
    pub master: String,
}

/// 业务作用: 保存经校验且有序的完整槽位范围，支持按 slot 查询后端节点。
#[derive(Debug, Clone, PartialEq, Eq)]
// 表示 SlotMap 的数据结构，用于解析和维护 Redis Cluster 槽位拓扑。
pub struct SlotMap {
    slots: Vec<String>,
}

impl SlotMap {
    /// 业务作用: 校验并构造覆盖全部 Redis Cluster slots 的有序节点映射。
    /// 参数说明:
    /// - `ranges`: Redis Cluster 槽位范围表，用于把 key 槽位映射到实际 master。
    ///
    /// 返回: slot 范围合法且完整时返回可查询拓扑；重叠、缺口或越界时返回错误。
    pub fn from_ranges(ranges: &[SlotRange]) -> anyhow::Result<Self> {
        let mut slots = vec![String::new(); 16_384];
        for range in ranges {
            for slot in range.start..=range.end {
                let target = &mut slots[slot as usize];
                if !target.is_empty() {
                    bail!("slot {slot} is assigned more than once");
                }
                *target = range.master.clone();
            }
        }
        if let Some((slot, _)) = slots.iter().enumerate().find(|(_, addr)| addr.is_empty()) {
            bail!("slot {slot} is not covered by CLUSTER SLOTS");
        }
        Ok(Self { slots })
    }

    /// 业务作用: 通过有序槽位区间查找目标 slot 的当前后端节点。
    /// 参数说明:
    /// - `slot`: 已计算的 Redis Cluster 槽位，用于查找负责该范围的 master。
    ///
    // 构造阶段已验证 16384 个槽全覆盖，因此合法 slot 应始终命中；Option 仍保护越界调用。
    pub fn node_for_slot(&self, slot: u16) -> Option<&str> {
        self.slots.get(slot as usize).map(String::as_str)
    }

    /// 业务作用: 返回拓扑中去重后的全部后端节点地址。
    /// 按首次槽位出现顺序返回拓扑中的去重后端节点地址。
    pub fn all_nodes(&self) -> Vec<String> {
        let mut nodes = Vec::new();
        for addr in &self.slots {
            if !nodes.iter().any(|existing| existing == addr) {
                nodes.push(addr.clone());
            }
        }
        nodes
    }

    /// 业务作用: 返回拓扑内可用于无键命令的稳定默认节点。
    /// 返回槽位 0 的后端节点，供没有 key 的 Cluster 命令选择稳定默认目标。
    pub fn default_node(&self) -> Option<&str> {
        self.slots.first().map(String::as_str)
    }
}

/// 业务作用: 加载 Redis Cluster 拓扑加载、slot 映射和节点地址归一化 所需数据，并按覆盖优先级或来源差异合并成统一运行视图。
/// 参数说明:
/// - `seed_addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
/// - `password`: 后端认证密码，调用方负责从安全配置提供。
/// - `connect_timeout_ms`: 建立后端或 coordinator TCP 连接的超时毫秒数。
/// - `request_timeout_ms`: 发送命令后等待后端或 coordinator 响应的超时毫秒数。
/// - `tcp_keepalive_ms`: 后端 TCP keepalive 配置毫秒数，用于清理异常断开的空闲连接。
/// - `tls`: 可选 RESP TLS 配置；启用时 seed 连接必须通过 CA 和 SNI 校验。
///
/// 返回: 成功发现并校验全部槽位时返回拓扑映射；连接、认证或响应解析失败时返回错误。
#[allow(dead_code)]
pub async fn load_slot_map_from_seed(
    seed_addr: &str,
    username: Option<&str>,
    password: Option<&str>,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    tls: Option<&BackendTlsConfig>,
) -> anyhow::Result<SlotMap> {
    let command = resp::array(&[b"CLUSTER", b"SLOTS"]);
    let responses = send_pipeline(
        BackendConnectOptions {
            addr: seed_addr,
            username,
            password,
            connect_timeout_ms,
            request_timeout_ms,
            tcp_keepalive_ms,
            response_bytes_limit: None,
            metrics: None,
            tls,
        },
        &[command],
    )
    .await?;
    let response = responses
        .first()
        .context("missing CLUSTER SLOTS response")?;
    let (value, consumed) = parse_value(response)?.context("incomplete CLUSTER SLOTS response")?;
    if consumed != response.len() {
        bail!("CLUSTER SLOTS response has trailing bytes");
    }
    let ranges = parse_cluster_slots(&value)?;
    SlotMap::from_ranges(&ranges)
}

/// 业务作用: 解析 Redis Cluster 拓扑加载、slot 映射和节点地址归一化 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 响应覆盖合法槽位范围时返回有序区间集合；结构、边界或节点地址非法时返回错误。
// 解析 parse_cluster_slots 输入，用于支撑解析和维护 Redis Cluster 槽位拓扑。
pub fn parse_cluster_slots(value: &RespValue) -> anyhow::Result<Vec<SlotRange>> {
    let entries = array(value).context("CLUSTER SLOTS response must be array")?;
    let mut ranges = Vec::with_capacity(entries.len());
    for entry in entries {
        let fields = array(entry).context("slot entry must be array")?;
        if fields.len() < 3 {
            bail!("slot entry must contain start, end and master node");
        }
        let start = integer(&fields[0]).context("slot start must be integer")?;
        let end = integer(&fields[1]).context("slot end must be integer")?;
        if !(0..=16_383).contains(&start) || !(0..=16_383).contains(&end) || start > end {
            bail!("invalid slot range {start}-{end}");
        }
        let master = parse_node_addr(&fields[2]).context("master node address")?;
        ranges.push(SlotRange {
            start: start as u16,
            end: end as u16,
            master,
        });
    }
    ranges.sort_by_key(|r| r.start);
    Ok(ranges)
}

/// 业务作用: 解析 Redis Cluster 拓扑加载、slot 映射和节点地址归一化 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 输入有效时返回规范化文本；格式、编码或边界非法时返回错误。
// 解析 parse_node_addr 输入，用于支撑解析和维护 Redis Cluster 槽位拓扑。
fn parse_node_addr(value: &RespValue) -> anyhow::Result<String> {
    let fields = array(value).context("node must be array")?;
    if fields.len() < 2 {
        bail!("node must contain host and port");
    }
    let host = string(&fields[0]).context("node host")?;
    let port = integer(&fields[1]).context("node port")?;
    if host.trim().is_empty() {
        bail!("node host is empty");
    }
    if !(1..=65_535).contains(&port) {
        bail!("node port out of range: {port}");
    }
    Ok(format!("{host}:{port}"))
}

/// 业务作用: 把 RESP 值严格解析为非空或可用数组结构。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 值是可用 RESP array 时返回元素切片；类型不符时返回错误。
fn array(value: &RespValue) -> anyhow::Result<&[RespValue]> {
    match value {
        RespValue::Array(Some(values)) => Ok(values),
        _ => bail!("expected array"),
    }
}

/// 业务作用: 从 RESP 整数值提取槽位或端口数字。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 值符合 RESP integer 语义时返回数值；类型或编码非法时返回错误。
fn integer(value: &RespValue) -> anyhow::Result<i64> {
    match value {
        RespValue::Integer(v) => Ok(*v),
        _ => bail!("expected integer"),
    }
}

/// 业务作用: 从 RESP simple 或 bulk string 提取 UTF-8 节点地址文本。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 输入有效时返回规范化文本；格式、编码或边界非法时返回错误。
fn string(value: &RespValue) -> anyhow::Result<String> {
    match value {
        RespValue::Bulk(Some(v)) | RespValue::Simple(v) => Ok(String::from_utf8(v.clone())?),
        _ => bail!("expected string"),
    }
}
