use serde::Deserialize;

/// 一个代理项选择的后端兼容档案。
///
/// 档案不是展示标签：配置校验、前端 HELLO 身份、RESP3 准入和拓扑模式都会读取它。
/// 命令、参数和模块能力仍受后端实际版本约束，不能仅凭产品名推断完整兼容。
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum BackendProfile {
    /// Redis 是未显式配置 `backend_profile` 时的迁移兼容默认值。
    #[default]
    Redis,
    /// Valkey 后端兼容档案。
    Valkey,
    /// Dragonfly 后端兼容档案。
    Dragonfly,
    /// Apache Kvrocks 后端兼容档案。
    Kvrocks,
    /// Garnet 后端兼容档案。
    Garnet,
    /// KeyDB 后端兼容档案。
    #[serde(rename = "keydb", alias = "key-db")]
    KeyDb,
    /// Redict 后端兼容档案。
    Redict,
    /// Pika/PikiwiDB 后端兼容档案。
    #[serde(alias = "pikiwidb", alias = "pikiwi-db")]
    Pika,
    /// Tendis 后端兼容档案。
    Tendis,
}

/// 产品级保守能力上限。
///
/// 这里仅保存会改变代理状态机或启动安全性的能力。命令级能力不能放进一个静态布尔表，
/// 必须继续按后端精确版本通过原生账本认证。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendCapabilities {
    /// 是否允许 NARIX 在终止 HELLO 后向业务侧承诺 RESP3 会话。
    pub resp3: bool,
    /// 是否接受 `HELLO 3`，但按后端原生语义把实际会话保持在 RESP2。
    pub hello3_downgrade_to_resp2: bool,
    /// 后端是否实现 `HELLO`，允许 NARIX 在新建后端连接时同步 RESP 版本。
    pub backend_hello: bool,
    /// 是否能够使用标准槽位、MOVED/ASK 和 Cluster Client 语义。
    pub standard_cluster: bool,
    /// 是否允许通过 Redis Sentinel 客户端协议发现非 Cluster 主从部署的当前主节点。
    pub sentinel: bool,
    /// 是否已经接入 NARIX 中立托管复制 HA 控制面。
    pub replication_ha: bool,
}

impl BackendProfile {
    /// 业务作用: YAML、状态接口和日志使用的稳定档案名。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Redis => "redis",
            Self::Valkey => "valkey",
            Self::Dragonfly => "dragonfly",
            Self::Kvrocks => "kvrocks",
            Self::Garnet => "garnet",
            Self::KeyDb => "keydb",
            Self::Redict => "redict",
            Self::Pika => "pika",
            Self::Tendis => "tendis",
        }
    }

    /// 业务作用: NARIX 本地终止 HELLO 时写入 `server` 字段的产品身份。
    ///
    /// 这使 Valkey、Kvrocks、Garnet 等 SDK 不会被硬编码的 `server=narix` 误导；版本字段
    /// 仍表示代理版本，不能冒充未探测的后端精确版本。
    pub const fn hello_server_name(self) -> &'static str {
        self.as_str()
    }

    /// 业务作用: 返回会实际约束代理运行路径的产品级能力。
    pub const fn capabilities(self) -> BackendCapabilities {
        match self {
            Self::Redis => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: true,
                replication_ha: true,
            },
            Self::Valkey => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: true,
                replication_ha: true,
            },
            Self::Redict => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: true,
                replication_ha: true,
            },
            Self::Kvrocks => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: false,
                replication_ha: true,
            },
            Self::KeyDb => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: false,
                replication_ha: true,
            },
            Self::Garnet => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: true,
                sentinel: false,
                replication_ha: false,
            },
            Self::Dragonfly => BackendCapabilities {
                resp3: true,
                hello3_downgrade_to_resp2: false,
                backend_hello: true,
                standard_cluster: false,
                sentinel: false,
                replication_ha: true,
            },
            Self::Pika => BackendCapabilities {
                resp3: false,
                hello3_downgrade_to_resp2: true,
                backend_hello: true,
                standard_cluster: false,
                sentinel: false,
                replication_ha: false,
            },
            Self::Tendis => BackendCapabilities {
                resp3: false,
                hello3_downgrade_to_resp2: false,
                backend_hello: false,
                standard_cluster: true,
                sentinel: false,
                replication_ha: false,
            },
        }
    }

    /// 业务作用: 把前端会话协议映射为后端连接实际需要协商的协议。
    ///
    /// Tendis 等固定 RESP2 且没有 `HELLO` 的后端返回 `None`，避免代理在接受
    /// 前端 `HELLO 2` 后向后端发送其不认识的内部握手命令。
    ///
    /// 参数说明:
    /// - `frontend_protocol_version`: 前端会话已经协商的 RESP 版本；尚未协商时为空。
    pub const fn backend_protocol_version(
        self,
        frontend_protocol_version: Option<u8>,
    ) -> Option<u8> {
        if self.capabilities().backend_hello {
            frontend_protocol_version
        } else {
            None
        }
    }
}
