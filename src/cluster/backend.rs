use std::collections::HashMap;
use std::io::{self, ErrorKind, IoSlice};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context as TaskContext, Poll};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use socket2::{SockRef, TcpKeepalive};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tracing::warn;

use crate::admin::{BackendConnectionGuard, ProxyMetrics};
use crate::config::ProxyItem;
use crate::resp;

const BACKEND_READ_BUF_SIZE: usize = 64 * 1024;
const MAX_VECTORED_SLICES: usize = 256;

/// 业务作用: 封装已认证后端流、响应缓冲、超时、大小门禁和连接生命周期指标。
#[derive(Debug)]
// 表示 BackendConnection 的数据结构，用于管理 Redis Cluster 后端连接和 pipeline 转发。
pub struct BackendConnection {
    /// 已建立并完成可选 TLS、认证的后端字节流。
    stream: BackendStream,
    /// 跨读取保留的 RESP 响应缓冲；可能包含半帧或一次系统调用多读出的尾部。
    buf: Vec<u8>,
    /// 固定复用的 64 KiB socket 读取工作区，避免每个响应重复分配。
    read_tmp: Vec<u8>,
    /// 普通有限请求等待响应的最长时间，单位为毫秒。
    request_timeout_ms: u64,
    /// 当前连接单批允许累计的响应字节上限；None 表示由外层控制。
    response_bytes_limit: Option<usize>,
    /// 后端活动连接指标守卫；连接析构时自动减少 gauge。
    _backend_connection: Option<BackendConnectionGuard>,
}

/// 业务作用: 用一段连续字节和帧边界索引表示整批后端响应，减少逐帧分配与复制。
#[derive(Debug)]
// 表示一批已连续读出的后端响应，用于减少 pipeline 中每个小响应单独分配 Vec 的开销。
pub struct BackendRawResponses {
    /// 按后端读取顺序连续保存的原始 RESP 响应字节。
    pub bytes: Vec<u8>,
    /// 每条完整响应帧在 `bytes` 中的范围，用于无复制写回客户端。
    pub frame_ranges: Vec<Range<usize>>,
    /// 后端返回错误帧的数量，用于批次指标和失败判断。
    pub error_count: usize,
}

/// 一条已经完整读取、可在后端连接归池后独立写回前端的 RESP 响应。
#[derive(Debug)]
pub struct BackendSingleResponse {
    /// 完整 RESP 帧字节；只复制实际响应长度，不复制后端连接的 64 KiB 读取容量。
    pub bytes: Vec<u8>,
    /// 当前响应是否为 Redis 错误帧，用于命令错误指标累计。
    pub is_error: bool,
}

/// 业务作用: 记录一批后端 pipeline 的发送、写入和读取阶段耗时，供热路径性能诊断。
#[derive(Debug, Clone, Copy, Default)]
// 表示后端 pipeline I/O 阶段耗时，用于诊断 proxy 到 Redis 的写入和读取成本。
pub struct BackendPipelineIoProfile {
    /// 把完整 pipeline 写入后端连接所用的时间，单位为微秒。
    pub write_micros: u64,
    /// 从后端读取全部对应响应所用的时间，单位为微秒。
    pub read_micros: u64,
}

impl BackendRawResponses {
    /// 业务作用: 返回连续后端响应中已切分的 RESP 帧数量。
    /// 返回连续缓冲区中已索引的完整 RESP 帧数量。
    pub fn len(&self) -> usize {
        self.frame_ranges.len()
    }

    /// 业务作用: 判断连续后端响应是否不包含任何完整 RESP 帧。
    /// 判断连续响应缓冲区是否不包含任何完整 RESP 帧。
    pub fn is_empty(&self) -> bool {
        self.frame_ranges.is_empty()
    }
}

/// 业务作用: 把后端连接拆成透明转发流和仍需保持的连接计数守卫。
#[derive(Debug)]
// 表示 BackendRelayParts 的数据结构，用于把已认证的后端连接切换到透明双向转发。
pub struct BackendRelayParts {
    /// 已完成可选 TLS 和认证、可直接进入透明转发的后端字节流。
    pub stream: BackendStream,
    /// 建连或认证阶段额外读到的后续响应，开始转发前必须优先写回。
    pub pending_response: Vec<u8>,
    /// 连接存活期间持有的指标守卫，释放时自动减少活跃后端连接数。
    pub backend_connection_guard: Option<BackendConnectionGuard>,
}

/// 业务作用: 集中传递后端地址、认证、TLS、超时、keepalive、响应限制和指标配置。
#[derive(Debug)]
// 聚合一次后端连接创建所需的全部参数，避免调用点用过长参数列表传递超时、认证和指标配置。
pub struct BackendConnectOptions<'a> {
    /// 要连接的后端 `host:port` 地址。
    pub addr: &'a str,
    /// 可选后端 ACL 用户名。
    pub username: Option<&'a str>,
    /// 可选后端认证密码。
    pub password: Option<&'a str>,
    /// TCP 建连最长等待时间，单位为毫秒。
    pub connect_timeout_ms: u64,
    /// 后端认证和普通命令响应的最长等待时间，单位为毫秒。
    pub request_timeout_ms: u64,
    /// 后端套接字 keepalive 周期，单位为毫秒；零表示不主动设置。
    pub tcp_keepalive_ms: u64,
    /// 单批响应允许累计的字节上限；空值表示由上层自行限制。
    pub response_bytes_limit: Option<usize>,
    /// 可选代理指标句柄，用于记录建连、认证和活跃连接。
    pub metrics: Option<ProxyMetrics>,
    /// 可选 RESP TLS 客户端配置；为空时使用明文 TCP。
    pub tls: Option<&'a BackendTlsConfig>,
}

/// 一组可复用的 RESP TLS 客户端参数。CA 和客户端身份只在首次使用该组合时解析。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BackendTlsConfig {
    /// 用于验证服务端证书链的 PEM CA 文件路径。
    pub ca_path: String,
    /// 显式 SNI/证书身份；空值时从当前连接地址提取主机名或 IP。
    pub server_name: Option<String>,
    /// mTLS 客户端证书 PEM 文件；必须与私钥同时配置。
    pub certificate_path: Option<String>,
    /// mTLS 客户端私钥 PEM 文件；必须与客户端证书同时配置。
    pub private_key_path: Option<String>,
}

impl BackendTlsConfig {
    /// 业务作用: 从代理项提取业务数据节点 TLS 配置；未启用时返回空值。
    ///
    /// 参数说明：
    /// - `item`：当前代理配置，提供业务后端 CA、SNI 和可选 mTLS 身份路径。
    ///
    /// 返回：启用 `backend_tls_enabled` 时返回独立配置，否则返回 `None`。
    pub fn for_backend(item: &ProxyItem) -> Option<Self> {
        item.backend_tls_enabled.then(|| Self {
            ca_path: item.backend_tls_ca_path.clone().unwrap_or_default(),
            server_name: item.backend_tls_server_name.clone(),
            certificate_path: item.backend_tls_certificate_path.clone(),
            private_key_path: item.backend_tls_private_key_path.clone(),
        })
    }

    /// 业务作用: 从代理项提取 Sentinel 控制连接 TLS 配置；未启用时返回空值。
    ///
    /// 参数说明：
    /// - `item`：当前 sentinel 代理配置，提供控制节点 CA、SNI 和可选 mTLS 身份路径。
    ///
    /// 返回：启用 `sentinel_tls_enabled` 时返回独立配置，否则返回 `None`。
    pub fn for_sentinel(item: &ProxyItem) -> Option<Self> {
        item.sentinel_tls_enabled.then(|| Self {
            ca_path: item.sentinel_tls_ca_path.clone().unwrap_or_default(),
            server_name: item.sentinel_tls_server_name.clone(),
            certificate_path: item.sentinel_tls_certificate_path.clone(),
            private_key_path: item.sentinel_tls_private_key_path.clone(),
        })
    }
}

/// 后端传输流；上层 RESP 解析、连接池和透明隧道不需要区分明文与 TLS。
#[derive(Debug)]
pub enum BackendStream {
    /// 未启用 RESP TLS 的普通 TCP 后端流。
    Plain(TcpStream),
    /// 已完成 CA/SNI 校验和可选 mTLS 的 rustls 后端流。
    Tls(Box<TlsStream<TcpStream>>),
}

impl BackendStream {
    /// 业务作用: 非阻塞探测底层 socket 是否有未消费字节，用于连接归池前的 clean 校验。
    ///
    /// 参数说明：
    /// - `buf`：调用方提供的单次探测缓冲区。
    ///
    /// 返回：沿用 `TcpStream::try_read` 语义；`WouldBlock` 表示当前无待读数据。
    fn try_read(&self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.try_read(buf),
            Self::Tls(stream) => stream.get_ref().0.try_read(buf),
        }
    }
}

impl AsyncRead for BackendStream {
    /// 业务作用: 把统一后端流的异步读取转发给明文 TCP 或 rustls 流。
    ///
    /// 参数说明：
    /// - `cx`：异步任务唤醒上下文。
    /// - `buf`：Tokio 读取目标缓冲区。
    ///
    /// 返回：返回底层传输的 poll 结果。
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BackendStream {
    /// 业务作用: 把普通异步写入转发给明文 TCP 或 rustls 流。
    ///
    /// 参数说明：
    /// - `cx`：异步任务唤醒上下文。
    /// - `buf`：本轮准备写入的 RESP 或 TLS 明文字节。
    ///
    /// 返回：返回底层传输本轮写入字节数或错误。
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    /// 业务作用: 刷新明文或 TLS 后端流中尚未提交的输出。
    ///
    /// 参数说明：
    /// - `cx`：异步任务唤醒上下文。
    ///
    /// 返回：返回底层 flush 的 poll 结果。
    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    /// 业务作用: 有序关闭明文或 TLS 后端写方向；TLS 分支由 rustls 生成关闭通知。
    ///
    /// 参数说明：
    /// - `cx`：异步任务唤醒上下文。
    ///
    /// 返回：返回底层 shutdown 的 poll 结果。
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }

    /// 业务作用: 查询当前底层传输是否支持 vectored write。
    ///
    /// 返回：返回底层流公布的 vectored write 能力。
    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Plain(stream) => stream.is_write_vectored(),
            Self::Tls(stream) => stream.is_write_vectored(),
        }
    }

    /// 业务作用: 把 pipeline 的分段 vectored write 转发给明文 TCP 或 rustls 流。
    ///
    /// 参数说明：
    /// - `cx`：异步任务唤醒上下文。
    /// - `bufs`：保持命令顺序的 I/O slice 集合。
    ///
    /// 返回：返回底层传输本轮写入字节数或错误。
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Tls(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }
}

impl BackendConnection {
    /// 业务作用: 在连接池复用前检查空闲连接是否仍然可用。
    ///
    /// 空闲连接不应携带任何未消费响应；读取到字节、EOF 或套接字错误都说明连接
    /// 已失去可复用条件。`WouldBlock` 表示当前没有可读事件，连接仍可进入下一次请求。
    ///
    /// 返回：连接没有缓冲响应且底层 socket 当前仍可复用时返回 true。
    pub fn is_idle_connection_usable(&self) -> bool {
        if !self.buf.is_empty() {
            return false;
        }
        let mut byte = [0u8; 1];
        match self.stream.try_read(&mut byte) {
            Err(err) if err.kind() == ErrorKind::WouldBlock => true,
            Ok(0) | Ok(_) | Err(_) => false,
        }
    }

    /// 业务作用: 建立 Redis Cluster 后端连接、认证、批量写入和响应读取 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
    /// - `password`: 认证密码，用于连接后端、协调器或测试代理，调用方负责从安全配置注入。
    /// - `timeout_ms`: 本次连接、认证、请求或外部命令等待的超时毫秒数。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    pub async fn connect(
        addr: &str,
        username: Option<&str>,
        password: Option<&str>,
        timeout_ms: u64,
    ) -> anyhow::Result<Self> {
        Self::connect_with_response_limit(addr, username, password, timeout_ms, None).await
    }

    /// 业务作用: 建立 Redis Cluster 后端连接、认证、批量写入和响应读取 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
    /// - `password`: 认证密码，用于连接后端、协调器或测试代理，调用方负责从安全配置注入。
    /// - `timeout_ms`: 本次连接、认证、请求或外部命令等待的超时毫秒数。
    /// - `response_bytes_limit`: 单会话允许排队的响应字节上限，用于阻止慢读客户端耗尽内存。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    pub async fn connect_with_response_limit(
        addr: &str,
        username: Option<&str>,
        password: Option<&str>,
        timeout_ms: u64,
        response_bytes_limit: Option<usize>,
    ) -> anyhow::Result<Self> {
        Self::connect_tracked(
            addr,
            username,
            password,
            timeout_ms,
            response_bytes_limit,
            None,
        )
        .await
    }

    /// 业务作用: 建立 Redis Cluster 后端连接、认证、批量写入和响应读取 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
    /// - `password`: 认证密码，用于连接后端、协调器或测试代理，调用方负责从安全配置注入。
    /// - `timeout_ms`: 本次连接、认证、请求或外部命令等待的超时毫秒数。
    /// - `response_bytes_limit`: 单会话允许排队的响应字节上限，用于阻止慢读客户端耗尽内存。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    pub async fn connect_tracked(
        addr: &str,
        username: Option<&str>,
        password: Option<&str>,
        timeout_ms: u64,
        response_bytes_limit: Option<usize>,
        metrics: Option<ProxyMetrics>,
    ) -> anyhow::Result<Self> {
        Self::connect_tracked_with_timeouts(BackendConnectOptions {
            addr,
            username,
            password,
            connect_timeout_ms: timeout_ms,
            request_timeout_ms: timeout_ms,
            tcp_keepalive_ms: 0,
            response_bytes_limit,
            metrics,
            tls: None,
        })
        .await
    }

    /// 业务作用: 建立 Redis Cluster 后端连接、认证、批量写入和响应读取 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `options`: 命令或运行步骤的可选参数集合，决定附加行为但不改变基础契约。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    // 使用显式连接参数创建后端 TCP 连接，并完成认证、超时、keepalive 和指标挂钩初始化。
    pub async fn connect_tracked_with_timeouts(
        options: BackendConnectOptions<'_>,
    ) -> anyhow::Result<Self> {
        let BackendConnectOptions {
            addr,
            username,
            password,
            connect_timeout_ms,
            request_timeout_ms,
            tcp_keepalive_ms,
            response_bytes_limit,
            metrics,
            tls,
        } = options;
        if let Some(metrics) = &metrics {
            metrics.record_backend_connection_attempt();
        }

        let connect_result =
            tokio::time::timeout(Duration::from_millis(connect_timeout_ms), async {
                let tcp = TcpStream::connect(addr).await?;
                configure_backend_tcp(&tcp, tcp_keepalive_ms, addr);
                if let Some(tls) = tls {
                    connect_tls(tcp, addr, tls).await
                } else {
                    Ok(BackendStream::Plain(tcp))
                }
            })
            .await;
        let mut conn = match connect_result {
            Ok(Ok(conn)) => conn,
            Ok(Err(err)) => {
                if let Some(metrics) = &metrics {
                    metrics.record_backend_connection_failure();
                }
                return Err(err).with_context(|| format!("cluster backend connect `{addr}`"));
            }
            Err(err) => {
                if let Some(metrics) = &metrics {
                    metrics.record_backend_connection_failure();
                }
                return Err(err)
                    .with_context(|| format!("cluster backend connect timeout `{addr}`"));
            }
        };
        if let Some(password) = password {
            if let Err(err) = auth_backend(&mut conn, username, password, request_timeout_ms).await
            {
                if let Some(metrics) = &metrics {
                    metrics.record_backend_auth_failure();
                }
                return Err(err);
            }
        }
        let backend_connection = metrics.map(|metrics| metrics.backend_connection_opened());

        Ok(Self {
            stream: conn,
            buf: Vec::with_capacity(BACKEND_READ_BUF_SIZE),
            read_tmp: vec![0; BACKEND_READ_BUF_SIZE],
            request_timeout_ms,
            response_bytes_limit,
            _backend_connection: backend_connection,
        })
    }

    /// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    pub async fn send_pipeline<C>(&mut self, commands: &[C]) -> anyhow::Result<Vec<Vec<u8>>>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, async {
            write_all_vectored_chunks(&mut self.stream, commands).await?;
            self.read_n_frames(commands.len()).await
        })
        .await
        .context("cluster backend pipeline timeout")?
    }

    /// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 写入 vectored pipeline 并把后端响应读取为连续 buffer，用于 cluster sub-pipeline 少拷贝回包。
    pub async fn send_pipeline_joined<C>(
        &mut self,
        commands: &[C],
    ) -> anyhow::Result<BackendRawResponses>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, async {
            write_all_vectored_chunks(&mut self.stream, commands).await?;
            self.read_n_frames_joined(commands.len()).await
        })
        .await
        .context("cluster backend joined pipeline timeout")?
    }

    /// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    // 写入一段连续 pipeline 并读取指定数量响应，用于 cluster-node-proxy 保持 1v1 节点代理的低开销路径。
    pub async fn send_raw_pipeline<C>(
        &mut self,
        raw_pipeline: C,
        response_count: usize,
    ) -> anyhow::Result<Vec<Vec<u8>>>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, async {
            self.stream.write_all(raw_pipeline.as_ref()).await?;
            self.read_n_frames(response_count).await
        })
        .await
        .context("cluster backend raw pipeline timeout")?
    }

    /// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 写入连续 pipeline 并把后端响应读取为一个连续 buffer，用于减少大量小响应的 per-frame Vec 分配。
    pub async fn send_raw_pipeline_joined<C>(
        &mut self,
        raw_pipeline: C,
        response_count: usize,
    ) -> anyhow::Result<BackendRawResponses>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, async {
            self.stream.write_all(raw_pipeline.as_ref()).await?;
            self.read_n_frames_joined(response_count).await
        })
        .await
        .context("cluster backend joined raw pipeline timeout")?
    }

    /// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    /// - `response_count`: 原始 pipeline 预期返回的 RESP 帧数，用于完整读取且避免串入下一批响应。
    ///
    /// 返回: 返回连续 RESP 响应及分段耗时；容量、连接、协议或响应大小异常时返回错误。
    // 写入连续 pipeline 并返回后端 I/O 分段耗时，用于 node-proxy 热路径 profiling。
    pub async fn send_raw_pipeline_joined_profiled<C>(
        &mut self,
        raw_pipeline: C,
        response_count: usize,
    ) -> anyhow::Result<(BackendRawResponses, BackendPipelineIoProfile)>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, async {
            let write_started = Instant::now();
            self.stream.write_all(raw_pipeline.as_ref()).await?;
            let write_micros = duration_micros_u64(write_started.elapsed());
            let read_started = Instant::now();
            let responses = self.read_n_frames_joined(response_count).await?;
            let read_micros = duration_micros_u64(read_started.elapsed());
            Ok((
                responses,
                BackendPipelineIoProfile {
                    write_micros,
                    read_micros,
                },
            ))
        })
        .await
        .context("cluster backend profiled joined raw pipeline timeout")?
    }

    /// 业务作用: 以单次连续写把原始 pipeline 发往后端，并记录发送阶段耗时。
    /// 参数说明:
    /// - `raw_pipeline`: 包含多个连续 RESP 命令的原始 pipeline 字节，发送时不得逐命令重排。
    ///
    /// 返回: 全部 pipeline 字节写入后端时返回成功；超时或 I/O 失败时返回错误。
    // 只写入连续 pipeline 不读取响应，用于订阅、监控、阻塞命令切换到透明隧道前的最后一次写入。
    pub async fn write_raw_pipeline<C>(&mut self, raw_pipeline: C) -> anyhow::Result<()>
    where
        C: AsRef<[u8]>,
    {
        let timeout = Duration::from_millis(self.request_timeout_ms);
        tokio::time::timeout(timeout, self.stream.write_all(raw_pipeline.as_ref()))
            .await
            .context("cluster backend raw pipeline write timeout")??;
        Ok(())
    }

    /// 业务作用: 读取一条完整 RESP 响应，并把实际帧字节压缩复制为独立所有权。
    ///
    /// 返回:
    /// - 成功时返回一条完整响应；该值可在连接归池后继续写回前端。
    /// - 后端提前关闭、RESP 非法或响应超过配置上限时返回带阶段信息的错误。
    ///
    /// 关键步骤:
    /// 1. 优先解析连接缓冲中已有字节，避免额外系统调用。
    /// 2. 只复制完整响应的实际长度，保留连接自身读取缓冲供下一次 pool 复用。
    /// 3. 消费首帧后保留尾部字节；上层发现尾部非空时必须把连接视为 tainted。
    pub async fn read_one_response_compact(&mut self) -> anyhow::Result<BackendSingleResponse> {
        loop {
            if let Some(len) = resp::frame_len(&self.buf)? {
                ensure_backend_response_limit(len, self.response_bytes_limit)?;
                let response = BackendSingleResponse {
                    bytes: self.buf[..len].to_vec(),
                    is_error: self.buf.first() == Some(&b'-'),
                };
                discard_consumed(&mut self.buf, len);
                return Ok(response);
            }
            let read = self.stream.read(&mut self.read_tmp).await?;
            if read == 0 {
                bail!("cluster backend closed before tunnel response was complete");
            }
            self.buf.extend_from_slice(&self.read_tmp[..read]);
            ensure_backend_response_limit(self.buf.len(), self.response_bytes_limit)?;
        }
    }

    /// 业务作用: 判断读取一条响应后，连接缓冲是否仍有无法归属的尾部字节。
    ///
    /// 返回:
    /// - `true`: 存在尾部字节，连接状态不能证明 clean，禁止归池。
    /// - `false`: 没有缓冲尾部；结合命令语义检查后可归池。
    pub fn has_buffered_response_bytes(&self) -> bool {
        !self.buf.is_empty()
    }

    /// 业务作用: 同步后端连接 RESP 协议版本，用于 front HELLO 认证由 proxy 终止时保持后端语义一致。
    /// 参数说明:
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    /// 返回: 后端接受目标 RESP 版本时返回成功；HELLO 失败或响应异常时返回错误。
    // 同步后端连接 RESP 协议版本，用于 front HELLO 认证由 proxy 终止时保持后端语义一致。
    pub async fn set_protocol_version(&mut self, protocol_version: u8) -> anyhow::Result<()> {
        if protocol_version != 2 && protocol_version != 3 {
            bail!("unsupported RESP protocol version `{protocol_version}`");
        }
        let version = protocol_version.to_string();
        let command = resp::array(&[b"HELLO", version.as_bytes()]);
        let timeout = Duration::from_millis(self.request_timeout_ms);
        let frames = tokio::time::timeout(timeout, async {
            self.stream.write_all(&command).await?;
            self.read_n_frames(1).await
        })
        .await
        .context("cluster backend HELLO timeout")??;
        let response = frames.first().context("missing HELLO response")?;
        if response.starts_with(b"-") {
            bail!(
                "cluster backend HELLO failed: {}",
                String::from_utf8_lossy(response).trim()
            );
        }
        Ok(())
    }

    /// 业务作用: 拆出可用于透明双向转发的底层流与连接指标守卫，保持生命周期计数有效。
    /// 把请求/响应连接拆成透明 relay 所需的流、预读响应和连接指标守卫。
    ///
    /// `pending_response` 必须先于后端后续字节写回客户端，否则切换隧道时会打乱响应顺序。
    pub fn into_relay_parts(self) -> BackendRelayParts {
        let Self {
            stream,
            buf,
            _backend_connection,
            ..
        } = self;
        BackendRelayParts {
            stream,
            pending_response: buf,
            backend_connection_guard: _backend_connection,
        }
    }

    /// 业务作用: 从后端缓冲中读取并切分指定数量的完整 RESP 帧，保留多读字节供下一批复用。
    /// 参数说明:
    /// - `n`: 本次底层读取实际得到的字节数；为零表示对端已经关闭连接。
    ///
    /// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
    async fn read_n_frames(&mut self, n: usize) -> anyhow::Result<Vec<Vec<u8>>> {
        let mut frames = Vec::with_capacity(n);
        let mut response_bytes = 0usize;
        let mut consumed = 0usize;

        while frames.len() < n {
            while let Some(len) = resp::frame_len(&self.buf[consumed..])? {
                ensure_backend_response_limit(
                    response_bytes.saturating_add(len),
                    self.response_bytes_limit,
                )?;
                let start = consumed;
                let end = consumed + len;
                let frame = self.buf[start..end].to_vec();
                consumed = end;
                response_bytes = response_bytes.saturating_add(len);
                frames.push(frame);
                if frames.len() == n {
                    discard_consumed(&mut self.buf, consumed);
                    return Ok(frames);
                }
            }
            discard_consumed(&mut self.buf, consumed);
            consumed = 0;
            let read = self.stream.read(&mut self.read_tmp).await?;
            if read == 0 {
                bail!("cluster backend closed before all pipeline responses were read");
            }
            self.buf.extend_from_slice(&self.read_tmp[..read]);
            ensure_backend_response_limit(
                response_bytes.saturating_add(self.buf.len()),
                self.response_bytes_limit,
            )?;
        }

        Ok(frames)
    }

    /// 业务作用: 读取指定数量响应并保留为单个连续 buffer 与帧索引，减少 pipeline 热路径分配。
    /// 参数说明:
    /// - `n`: 本次底层读取实际得到的字节数；为零表示对端已经关闭连接。
    ///
    /// 返回: 返回连续 RESP 响应缓冲与帧索引；容量、连接、协议或响应大小异常时返回错误。
    // 读取指定数量响应并追加到单个连续 buffer，用于 standalone/node-proxy pipeline 快路径直接写回前端。
    async fn read_n_frames_joined(&mut self, n: usize) -> anyhow::Result<BackendRawResponses> {
        if n == 0 {
            return Ok(BackendRawResponses {
                bytes: Vec::new(),
                frame_ranges: Vec::new(),
                error_count: 0,
            });
        }

        let mut frame_ranges = Vec::with_capacity(n);
        let mut error_count = 0usize;
        let mut response_bytes = 0usize;
        let mut consumed = 0usize;

        while frame_ranges.len() < n {
            while let Some(len) = resp::frame_len(&self.buf[consumed..])? {
                ensure_backend_response_limit(
                    response_bytes.saturating_add(len),
                    self.response_bytes_limit,
                )?;
                let start = consumed;
                let end = consumed + len;
                if self.buf[start] == b'-' {
                    error_count += 1;
                }
                frame_ranges.push(start..end);
                consumed = end;
                response_bytes = response_bytes.saturating_add(len);
                if frame_ranges.len() == n {
                    let bytes = take_consumed_response_bytes(&mut self.buf, consumed);
                    return Ok(BackendRawResponses {
                        bytes,
                        frame_ranges,
                        error_count,
                    });
                }
            }
            let read = self.stream.read(&mut self.read_tmp).await?;
            if read == 0 {
                bail!("cluster backend closed before all pipeline responses were read");
            }
            self.buf.extend_from_slice(&self.read_tmp[..read]);
            ensure_backend_response_limit(
                response_bytes.saturating_add(self.buf.len().saturating_sub(consumed)),
                self.response_bytes_limit,
            )?;
        }

        unreachable!("read_n_frames_joined loop returns when enough frames are read")
    }
}

/// 业务作用: 在每次扩充响应缓冲前校验累计字节上限，防止不可信后端耗尽代理内存。
/// 参数说明:
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `limit`: 对应资源或数据规模的硬上限，超过时立即拒绝以保护内存和连接容量。
///
/// 返回: 累计响应未超过配置上限时返回成功；字节数溢出或超限时返回错误。
fn ensure_backend_response_limit(bytes: usize, limit: Option<usize>) -> anyhow::Result<()> {
    let Some(limit) = limit else {
        return Ok(());
    };
    if bytes > limit {
        bail!("cluster backend response bytes exceeds limit: {bytes} > {limit}");
    }
    Ok(())
}

/// 业务作用: 循环执行 vectored write 直到全部命令切片写完，正确推进部分写入后的 IoSlice 游标。
/// 参数说明:
/// - `writer`: 协议写入端，负责按顺序完整写出响应并传播背压。
/// - `chunks`: 拆分后的协议数据块集合，按原始顺序写入以保持帧边界。
///
/// 返回: 全部字节按原顺序写出时返回成功；客户端断连或底层写入失败时返回 I/O 错误。
async fn write_all_vectored_chunks<W, C>(writer: &mut W, chunks: &[C]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    C: AsRef<[u8]>,
{
    let mut chunk_idx = 0;
    let mut offset = 0;

    loop {
        while chunk_idx < chunks.len() && offset >= chunks[chunk_idx].as_ref().len() {
            chunk_idx += 1;
            offset = 0;
        }
        if chunk_idx >= chunks.len() {
            return Ok(());
        }

        let mut slices = Vec::with_capacity(MAX_VECTORED_SLICES);
        let first = chunks[chunk_idx].as_ref();
        slices.push(IoSlice::new(&first[offset..]));
        for chunk in chunks.iter().skip(chunk_idx + 1) {
            if slices.len() == MAX_VECTORED_SLICES {
                break;
            }
            let bytes = chunk.as_ref();
            if !bytes.is_empty() {
                slices.push(IoSlice::new(bytes));
            }
        }

        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(io::Error::new(
                ErrorKind::WriteZero,
                "failed to write vectored Redis pipeline",
            ));
        }

        let mut remaining = written;
        while chunk_idx < chunks.len() {
            let available = chunks[chunk_idx].as_ref().len().saturating_sub(offset);
            if remaining < available {
                offset += remaining;
                break;
            }
            remaining -= available;
            chunk_idx += 1;
            offset = 0;
            if remaining == 0 {
                break;
            }
        }
    }
}

/// 业务作用: 向后端发送 Redis Cluster 后端连接、认证、批量写入和响应读取 相关命令，并按 RESP 协议读取可用于继续决策的响应。
/// 参数说明:
/// - `options`: 后端地址、认证、超时、keepalive、指标和可选 RESP TLS 参数。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
pub async fn send_pipeline(
    options: BackendConnectOptions<'_>,
    commands: &[Vec<u8>],
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut conn = BackendConnection::connect_tracked_with_timeouts(options).await?;
    conn.send_pipeline(commands).await
}

/// 业务作用: 对后端 TCP socket 应用 keepalive 等连接级参数。
/// 参数说明:
/// - `stream`: 已建立的 TCP 字节流，函数在其上执行协议收发或透明转发。
/// - `keepalive_ms`: TCP keepalive 配置毫秒数，用于尽早发现半开连接。
/// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
///
fn configure_backend_tcp(stream: &TcpStream, keepalive_ms: u64, addr: &str) {
    if let Err(err) = stream.set_nodelay(true) {
        warn!(backend = %addr, error = %err, "failed to set backend TCP_NODELAY");
    }
    if keepalive_ms == 0 {
        return;
    }
    let keepalive = TcpKeepalive::new().with_time(Duration::from_millis(keepalive_ms));
    if let Err(err) = SockRef::from(stream).set_tcp_keepalive(&keepalive) {
        warn!(backend = %addr, error = %err, "failed to set backend TCP keepalive");
    }
}

/// 业务作用: 在已配置 TCP socket 上完成受 CA 和 SNI 校验的 TLS 握手。
///
/// 参数说明：
/// - `tcp`：已经完成连接、TCP_NODELAY 和 keepalive 配置的底层 socket。
/// - `addr`：真实连接目标 `host:port`，未显式配置 SNI 时从此提取身份。
/// - `tls`：当前控制面或数据面的 CA、SNI 和可选 mTLS 参数。
///
/// 返回：握手成功时返回统一 TLS 后端流；证书、身份或握手失败时返回上下文错误。
async fn connect_tls(
    tcp: TcpStream,
    addr: &str,
    tls: &BackendTlsConfig,
) -> anyhow::Result<BackendStream> {
    let client_config = cached_tls_client_config(tls)?;
    let server_name = tls
        .server_name
        .clone()
        .unwrap_or_else(|| tls_server_name_from_addr(addr));
    let server_name = ServerName::try_from(server_name.clone())
        .with_context(|| format!("invalid RESP TLS server name `{server_name}`"))?;
    let stream = TlsConnector::from(client_config)
        .connect(server_name, tcp)
        .await
        .with_context(|| format!("RESP TLS handshake with `{addr}`"))?;
    Ok(BackendStream::Tls(Box::new(stream)))
}

/// 业务作用: 对相同证书材料复用 rustls ClientConfig，避免连接池扩容时重复读取和解析 PEM 文件。
///
/// 参数说明：
/// - `tls`：作为缓存键和证书加载来源的完整 TLS 参数。
///
/// 返回：返回可复用 rustls 客户端配置；文件、PEM 或密钥不合法时拒绝连接。
fn cached_tls_client_config(tls: &BackendTlsConfig) -> anyhow::Result<Arc<ClientConfig>> {
    static CACHE: OnceLock<Mutex<HashMap<BackendTlsConfig, Arc<ClientConfig>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(config) = cache
        .lock()
        .expect("RESP TLS client config cache mutex poisoned")
        .get(tls)
        .cloned()
    {
        return Ok(config);
    }

    let ca_bytes = std::fs::read(&tls.ca_path)
        .with_context(|| format!("read RESP TLS CA `{}`", tls.ca_path))?;
    let mut ca_reader = ca_bytes.as_slice();
    let certificates = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .context("parse RESP TLS CA certificates")?;
    if certificates.is_empty() {
        bail!("RESP TLS CA file contains no certificate");
    }
    let mut roots = RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .context("add RESP TLS CA certificate")?;
    }

    let builder =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .context("select RESP TLS protocol versions")?
            .with_root_certificates(roots);
    let config = match (&tls.certificate_path, &tls.private_key_path) {
        (Some(certificate_path), Some(private_key_path)) => {
            let certificate_bytes = std::fs::read(certificate_path).with_context(|| {
                format!("read RESP TLS client certificate `{certificate_path}`")
            })?;
            let mut certificate_reader = certificate_bytes.as_slice();
            let certificates = rustls_pemfile::certs(&mut certificate_reader)
                .collect::<Result<Vec<_>, _>>()
                .context("parse RESP TLS client certificate")?;
            if certificates.is_empty() {
                bail!("RESP TLS client certificate file contains no certificate");
            }
            let private_key_bytes = std::fs::read(private_key_path)
                .with_context(|| format!("read RESP TLS private key `{private_key_path}`"))?;
            let mut private_key_reader = private_key_bytes.as_slice();
            let private_key = rustls_pemfile::private_key(&mut private_key_reader)
                .context("parse RESP TLS private key")?
                .context("RESP TLS private key file contains no supported key")?;
            builder
                .with_client_auth_cert(certificates, private_key)
                .context("configure RESP mutual TLS identity")?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => bail!("RESP TLS client certificate and private key must be configured together"),
    };
    let config = Arc::new(config);
    cache
        .lock()
        .expect("RESP TLS client config cache mutex poisoned")
        .insert(tls.clone(), config.clone());
    Ok(config)
}

/// 业务作用: 从 `host:port` 或 `[ipv6]:port` 地址中提取默认 SNI/IP 身份。
///
/// 参数说明：
/// - `addr`：当前实际连接的 IPv4、IPv6 或域名地址。
///
/// 返回：返回不含端口和 IPv6 方括号的证书校验身份。
fn tls_server_name_from_addr(addr: &str) -> String {
    if let Some(rest) = addr.strip_prefix('[') {
        if let Some((host, _)) = rest.split_once(']') {
            return host.to_string();
        }
    }
    addr.rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(addr)
        .to_string()
}

/// 业务作用: 按用户名和密码组合向后端完成 ACL 或密码认证。
/// 参数说明:
/// - `conn`: 当前已建立的 Redis 协议连接，函数在其上完成认证、收发或状态校验。
/// - `username`: 认证用户名，用于 Redis ACL 场景；未启用 ACL 时通常为空。
/// - `password`: 认证密码，用于连接后端、协调器或测试代理，调用方负责从安全配置注入。
/// - `timeout_ms`: 本次连接、认证、请求或外部命令等待的超时毫秒数。
///
/// 返回: 后端确认 AUTH 时返回成功；拒绝、超时或协议异常时返回错误。
async fn auth_backend(
    conn: &mut BackendStream,
    username: Option<&str>,
    password: &str,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    let command = match username {
        Some(username) if !username.is_empty() => {
            resp::array(&[b"AUTH", username.as_bytes(), password.as_bytes()])
        }
        _ => resp::array(&[b"AUTH", password.as_bytes()]),
    };
    conn.write_all(&command).await?;
    let frames = tokio::time::timeout(Duration::from_millis(timeout_ms), read_n_frames(conn, 1))
        .await
        .context("cluster backend auth timeout")??;
    let response = frames.first().context("missing auth response")?;
    if response.starts_with(b"+OK") {
        Ok(())
    } else {
        bail!(
            "cluster backend auth failed: {}",
            String::from_utf8_lossy(response).trim()
        )
    }
}

/// 业务作用: 从通用异步流读取指定数量 RESP 帧，供认证和协议协商等建连阶段复用。
/// 参数说明:
/// - `conn`: 当前已建立的 Redis 协议连接，函数在其上完成认证、收发或状态校验。
/// - `n`: 本次底层读取实际得到的字节数；为零表示对端已经关闭连接。
///
/// 返回: 返回与输入顺序一致的完整 RESP 帧集合；I/O、协议、容量或大小限制失败时返回错误。
async fn read_n_frames<R>(conn: &mut R, n: usize) -> anyhow::Result<Vec<Vec<u8>>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(BACKEND_READ_BUF_SIZE);
    let mut tmp = vec![0; BACKEND_READ_BUF_SIZE];
    let mut frames = Vec::with_capacity(n);
    let mut consumed = 0usize;

    while frames.len() < n {
        while let Some(len) = resp::frame_len(&buf[consumed..])? {
            let start = consumed;
            let end = consumed + len;
            frames.push(buf[start..end].to_vec());
            consumed = end;
            if frames.len() == n {
                discard_consumed(&mut buf, consumed);
                return Ok(frames);
            }
        }
        discard_consumed(&mut buf, consumed);
        consumed = 0;
        let read = conn.read(&mut tmp).await?;
        if read == 0 {
            bail!("cluster backend closed before all pipeline responses were read");
        }
        buf.extend_from_slice(&tmp[..read]);
    }

    Ok(frames)
}

/// 业务作用: 丢弃已经解析完成的响应字节，用于避免每个 RESP frame 都 drain 导致反复搬移缓冲区。
/// 参数说明:
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `consumed`: 当前已消费的输入字节数，用于推进解析游标并保留未完成帧。
///
// 丢弃已经解析完成的响应字节，用于避免每个 RESP frame 都 drain 导致反复搬移缓冲区。
fn discard_consumed(buf: &mut Vec<u8>, consumed: usize) {
    if consumed == 0 {
        return;
    }
    if consumed >= buf.len() {
        buf.clear();
    } else {
        buf.drain(..consumed);
    }
}

/// 业务作用: 移出已经解析完成的响应字节，同时保留多读到的后续字节供下一次请求使用。
/// 参数说明:
/// - `buf`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
/// - `consumed`: 当前已消费的输入字节数，用于推进解析游标并保留未完成帧。
///
// 移出已经解析完成的响应字节，同时保留多读到的后续字节供下一次请求使用。
fn take_consumed_response_bytes(buf: &mut Vec<u8>, consumed: usize) -> Vec<u8> {
    if consumed >= buf.len() {
        return std::mem::take(buf);
    }
    let remainder = buf.split_off(consumed);
    std::mem::replace(buf, remainder)
}

/// 业务作用: 把后端 I/O profiling 阶段耗时饱和转换为 u64 微秒指标，避免极端时长截断。
/// 参数说明:
/// - `duration`: 时间跨度，用于统计窗口、等待上限或吞吐计算。
///
// Duration 使用 u128 微秒，写入公开 u64 指标前饱和截断，不能在极端时长下回绕。
fn duration_micros_u64(duration: Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}
