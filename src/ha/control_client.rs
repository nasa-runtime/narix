use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context};

use crate::cluster::backend::{BackendConnectOptions, BackendConnection, BackendTlsConfig};
use crate::config::ProxyItem;
use crate::ha::model::HaNode;
use crate::resp;

/// HA 控制命令只读取 INFO、CONFIG 和单值确认，限制异常后端响应避免控制面无界扩容。
const HA_CONTROL_RESPONSE_BYTES_LIMIT: usize = 4 * 1024 * 1024;

/// 共享一个 replication HA 运行代是否仍允许执行后端控制命令的状态。
///
/// 运行代退休后，适配器内部的多命令序列会在每条 RESP 命令前后复验该令牌，
/// 防止已经退出活动槽位的旧配置继续推进后续拓扑或持久化副作用。
#[derive(Debug, Clone, Default)]
pub(crate) struct HaGenerationGuard {
    retired: Arc<AtomicBool>,
}

impl HaGenerationGuard {
    /// 业务作用: 将当前运行代永久标记为已退休。
    pub(crate) fn mark_retired(&self) {
        self.retired.store(true, Ordering::Release);
    }

    /// 业务作用: 返回当前运行代是否已经退休。
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }

    /// 业务作用: 在即将执行或刚完成后端控制步骤时复验运行代有效性。
    ///
    /// 参数:
    /// - `step`: 当前控制步骤的稳定名称，用于返回可定位的退休错误。
    pub(crate) fn ensure_active(&self, step: &str) -> anyhow::Result<()> {
        if self.is_retired() {
            bail!(
                "replication HA runtime generation has been retired {step}; retry against the active generation"
            );
        }
        Ok(())
    }
}

/// HA 控制连接的认证、TLS 和超时配置。
///
/// 产品适配器只负责选择命令和解析响应，连接创建、认证和 RESP 收发统一由该类型处理。
#[derive(Debug, Clone)]
pub(crate) struct HaControlClient {
    username: Option<String>,
    password: Option<String>,
    tls: Option<BackendTlsConfig>,
    connect_timeout_ms: u64,
    probe_connect_timeout_ms: u64,
    request_timeout_ms: u64,
    probe_request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    generation_guard: HaGenerationGuard,
}

impl HaControlClient {
    /// 业务作用: 从代理配置和共享运行代令牌构造 HA 控制客户端。
    ///
    /// 参数:
    /// - `item`: 提供后端认证、RESP TLS、普通命令超时和探测超时的代理配置。
    /// - `generation_guard`: 与所属 replication HA 运行代共享的退休状态。
    pub(crate) fn from_proxy_item(item: &ProxyItem, generation_guard: HaGenerationGuard) -> Self {
        let ha = item.ha.as_ref();
        Self {
            username: item.username.clone(),
            password: item.password.clone(),
            tls: BackendTlsConfig::for_backend(item),
            connect_timeout_ms: item.connect_timeout_ms,
            probe_connect_timeout_ms: ha
                .and_then(|config| config.probe_connect_timeout_ms)
                .unwrap_or(item.connect_timeout_ms),
            request_timeout_ms: item.backend_request_timeout_ms,
            probe_request_timeout_ms: ha
                .and_then(|config| config.probe_request_timeout_ms)
                .unwrap_or(item.backend_request_timeout_ms),
            tcp_keepalive_ms: item.tcp_keepalive_ms,
            generation_guard,
        }
    }

    /// 业务作用: 返回配置给后端节点的复制认证密码。
    pub(crate) fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    /// 业务作用: 返回配置给后端节点的复制认证用户名。
    pub(crate) fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    /// 业务作用: 返回探测后端角色收敛时使用的请求超时。
    pub(crate) const fn probe_request_timeout_ms(&self) -> u64 {
        self.probe_request_timeout_ms
    }

    /// 业务作用: 使用普通控制超时向指定节点发送一条 RESP 命令。
    ///
    /// 参数:
    /// - `node`: 接收命令的后端节点。
    /// - `args`: 已按命令参数边界拆分的 RESP 参数。
    pub(crate) async fn send_command(
        &self,
        node: &HaNode,
        args: &[&[u8]],
    ) -> anyhow::Result<Vec<u8>> {
        self.send_command_with_timeouts(
            node,
            args,
            self.connect_timeout_ms,
            self.request_timeout_ms,
        )
        .await
    }

    /// 业务作用: 使用短探测超时向指定节点发送一条 RESP 命令。
    ///
    /// 参数:
    /// - `node`: 接收探测命令的后端节点。
    /// - `args`: 已按命令参数边界拆分的 RESP 参数。
    pub(crate) async fn send_probe_command(
        &self,
        node: &HaNode,
        args: &[&[u8]],
    ) -> anyhow::Result<Vec<u8>> {
        self.send_command_with_timeouts(
            node,
            args,
            self.probe_connect_timeout_ms,
            self.probe_request_timeout_ms,
        )
        .await
    }

    /// 业务作用: 使用调用方指定的超时建立控制连接并发送一条 RESP 命令。
    ///
    /// 参数:
    /// - `node`: 接收命令的后端节点。
    /// - `args`: 已按命令参数边界拆分的 RESP 参数。
    /// - `connect_timeout_ms`: TCP 和可选 TLS 建连的最长等待毫秒数。
    /// - `request_timeout_ms`: 认证和命令响应的最长等待毫秒数。
    async fn send_command_with_timeouts(
        &self,
        node: &HaNode,
        args: &[&[u8]],
        connect_timeout_ms: u64,
        request_timeout_ms: u64,
    ) -> anyhow::Result<Vec<u8>> {
        self.generation_guard
            .ensure_active("before opening a backend control connection")?;
        let mut connection =
            BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
                addr: &node.client_addr,
                username: self.username.as_deref(),
                password: self.password.as_deref(),
                connect_timeout_ms,
                request_timeout_ms,
                tcp_keepalive_ms: self.tcp_keepalive_ms,
                response_bytes_limit: Some(HA_CONTROL_RESPONSE_BYTES_LIMIT),
                metrics: None,
                tls: self.tls.as_ref(),
            })
            .await?;
        self.generation_guard
            .ensure_active("before sending a backend control command")?;
        let frames = connection.send_pipeline(&[resp::array(args)]).await?;
        self.generation_guard
            .ensure_active("after receiving a backend control response")?;
        frames
            .into_iter()
            .next()
            .context("missing HA backend response")
    }
}
