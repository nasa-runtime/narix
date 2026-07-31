use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use crate::admin::ProxyMetrics;
use crate::cluster::backend::{BackendConnectOptions, BackendConnection, BackendTlsConfig};
use crate::config::{AuthMode, ProxyItem};

const ORDINARY_MUX_MAX_BATCH: usize = 256;

/// 业务作用: 按后端节点与 RESP 版本隔离普通命令 worker，使多前端共享少量 FIFO 后端连接。
#[derive(Debug)]
// 表示 OrdinaryBackendMux 的数据结构，用于让普通单命令共享同一条后端连接。
pub struct OrdinaryBackendMux {
    factory: Arc<BackendConnectionFactory>,
    nodes: StdMutex<HashMap<String, mpsc::Sender<OrdinaryJob>>>,
    queue_size: usize,
    submit_timeout_ms: u64,
}

/// 业务作用: 保存 ordinary mux 新建后端连接所需的认证、TLS、超时和指标配置。
#[derive(Debug)]
// 表示 BackendConnectionFactory 的数据结构，用于创建 ordinary mux 持有的后端连接。
struct BackendConnectionFactory {
    username: Option<String>,
    password: Option<String>,
    connect_timeout_ms: u64,
    request_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    response_bytes_limit: usize,
    metrics: ProxyMetrics,
    tls: Option<BackendTlsConfig>,
}

/// 业务作用: 表示一条排入 ordinary mux 的原始命令及其一次性响应通道。
#[derive(Debug)]
// 表示 OrdinaryJob 的数据结构，用于把单条普通命令提交到共享后端连接。
struct OrdinaryJob {
    raw: Bytes,
    reply: oneshot::Sender<anyhow::Result<Vec<u8>>>,
}

impl OrdinaryBackendMux {
    /// 业务作用: 为单个后端节点创建有界普通命令 FIFO worker 和提交通道。
    /// 参数说明:
    /// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
    /// - `metrics`: 指标采集对象，用于记录连接、请求、错误和资源占用。
    ///
    pub fn new(item: &ProxyItem, metrics: ProxyMetrics) -> Self {
        let (username, password) = backend_credentials(item);
        Self {
            factory: Arc::new(BackendConnectionFactory {
                username,
                password,
                connect_timeout_ms: item.connect_timeout_ms,
                request_timeout_ms: item.backend_request_timeout_ms,
                tcp_keepalive_ms: item.tcp_keepalive_ms,
                response_bytes_limit: item.pending_response_bytes,
                metrics,
                tls: BackendTlsConfig::for_backend(item),
            }),
            nodes: StdMutex::new(HashMap::new()),
            queue_size: item.pipeline_backend_queue_size,
            submit_timeout_ms: item.pipeline_backend_submit_timeout_ms,
        }
    }

    /// 业务作用: 向后端发送 Redis Cluster 普通命令到后端连接池的复用通道 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级 mux 的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    /// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
    ///
    /// 返回: 返回一条完整且已按代理语义处理的 RESP 帧；路由、I/O 或协议失败时返回错误。
    // 发送一条普通命令，用于共享同一 backend/protocol 的 ordinary mux connection。
    pub async fn send_command(
        &self,
        node: &str,
        protocol_version: Option<u8>,
        raw: Bytes,
    ) -> anyhow::Result<Vec<u8>> {
        let sender = self.node_sender(node, protocol_version);
        submit_ordinary_job(&sender, node, self.submit_timeout_ms, raw).await
    }

    /// 业务作用: 预解析节点级 mux 句柄，用于 backend/protocol 在 session 内不变的 standalone / cluster-node-proxy 把「mux key 拼接 + 外层 map 加锁」从每批热路径挪到握手后只做一次。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级 mux 的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    // 预解析节点级 mux 句柄，用于 backend/protocol 在 session 内不变的 standalone / cluster-node-proxy
    // 把「mux key 拼接 + 外层 map 加锁」从每批热路径挪到握手后只做一次。
    pub fn node_handle(&self, node: &str, protocol_version: Option<u8>) -> OrdinaryMuxHandle {
        OrdinaryMuxHandle {
            sender: self.node_sender(node, protocol_version),
            node: node.to_string(),
            submit_timeout_ms: self.submit_timeout_ms,
        }
    }

    /// 业务作用: 查询或创建节点级 ordinary mux worker，用于隔离不同 backend/protocol 的连接状态。
    /// 参数说明:
    /// - `node`: `host:port` 后端地址，也是节点级 mux 的隔离键。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    // 查询或创建节点级 ordinary mux worker，用于隔离不同 backend/protocol 的连接状态。
    fn node_sender(&self, node: &str, protocol_version: Option<u8>) -> mpsc::Sender<OrdinaryJob> {
        let mut nodes = self
            .nodes
            .lock()
            .expect("ordinary backend mux map mutex poisoned");
        nodes
            .entry(mux_key(node, protocol_version))
            .or_insert_with(|| {
                let (tx, rx) = mpsc::channel(self.queue_size);
                tokio::spawn(run_ordinary_mux_worker(
                    node.to_string(),
                    protocol_version,
                    self.factory.clone(),
                    rx,
                ));
                tx
            })
            .clone()
    }
}

/// 业务作用: 提供会话缓存的节点级 ordinary mux 直达入口，避免每批重复查表。
#[derive(Debug, Clone)]
// 表示 OrdinaryMuxHandle 的数据结构，用于 session 级缓存单节点 ordinary mux 的提交入口。
pub struct OrdinaryMuxHandle {
    sender: mpsc::Sender<OrdinaryJob>,
    node: String,
    submit_timeout_ms: u64,
}

impl OrdinaryMuxHandle {
    /// 业务作用: 向后端发送 Redis Cluster 普通命令到后端连接池的复用通道 相关命令，并按 RESP 协议读取可用于继续决策的响应。
    /// 参数说明:
    /// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
    ///
    /// 返回: 返回一条完整且已按代理语义处理的 RESP 帧；路由、I/O 或协议失败时返回错误。
    // 发送一条普通命令，用于 session 热路径跳过 mux 查表。
    pub async fn send_command(&self, raw: Bytes) -> anyhow::Result<Vec<u8>> {
        submit_ordinary_job(&self.sender, &self.node, self.submit_timeout_ms, raw).await
    }
}

/// 业务作用: 提交单条普通命令到 mux worker 并等待响应，用于 send_command 与 session 句柄共用提交语义。
/// 参数说明:
/// - `sender`: 异步消息发送端，用于把响应、状态变化或关闭信号交给对应任务。
/// - `node`: `host:port` 后端地址，用于错误上下文和目标隔离。
/// - `submit_timeout_ms`: 等待队列或连接池容量的提交超时毫秒数。
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
/// 返回: 返回一条完整且已按代理语义处理的 RESP 帧；路由、I/O 或协议失败时返回错误。
// 提交单条普通命令到 mux worker 并等待响应，用于 send_command 与 session 句柄共用提交语义。
async fn submit_ordinary_job(
    sender: &mpsc::Sender<OrdinaryJob>,
    node: &str,
    submit_timeout_ms: u64,
    raw: Bytes,
) -> anyhow::Result<Vec<u8>> {
    let (reply, result) = oneshot::channel();
    let job = OrdinaryJob { raw, reply };
    tokio::time::timeout(Duration::from_millis(submit_timeout_ms), sender.send(job))
        .await
        .with_context(|| {
            format!("ordinary backend `{node}` submit timeout after {submit_timeout_ms}ms")
        })?
        .context("ordinary backend mux stopped")?;
    result.await.context("ordinary backend mux dropped reply")?
}

/// 业务作用: 运行 ordinary mux worker，用于在一条后端连接上 FIFO 执行普通单命令并做小批量合并。
/// 参数说明:
/// - `node`: worker 固定服务的 `host:port` 后端地址。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `factory`: 后端连接工厂，用于按目标地址和认证配置创建受控连接。
/// - `rx`: 异步通道接收端，用于等待状态变化、关闭信号或后台结果。
///
// 运行 ordinary mux worker，用于在一条后端连接上 FIFO 执行普通单命令并做小批量合并。
async fn run_ordinary_mux_worker(
    node: String,
    protocol_version: Option<u8>,
    factory: Arc<BackendConnectionFactory>,
    mut rx: mpsc::Receiver<OrdinaryJob>,
) {
    let mut conn = None;
    while let Some(first) = rx.recv().await {
        let mut jobs = vec![first];
        while jobs.len() < ORDINARY_MUX_MAX_BATCH {
            match rx.try_recv() {
                Ok(job) => jobs.push(job),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        let result =
            send_with_mux_connection(&node, protocol_version, &factory, &mut conn, &jobs).await;
        match result {
            Ok(frames) => {
                for (job, frame) in jobs.into_iter().zip(frames) {
                    let _ = job.reply.send(Ok(frame));
                }
            }
            Err(err) => {
                warn!(
                    backend = %node,
                    error = %err,
                    "ordinary backend mux invalidated connection"
                );
                let message = err.to_string();
                for job in jobs {
                    let _ = job.reply.send(Err(anyhow!(message.clone())));
                }
            }
        }
    }
}

/// 业务作用: 向后端发送 Redis Cluster 普通命令到后端连接池的复用通道 相关命令，并按 RESP 协议读取可用于继续决策的响应。
/// 参数说明:
/// - `node`: 当前 mux 连接绑定的 `host:port` 后端地址。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `factory`: 后端连接工厂，用于按目标地址和认证配置创建受控连接。
/// - `conn`: 当前已建立的 Redis 协议连接，函数在其上完成认证、收发或状态校验。
/// - `jobs`: 待并发执行的后端任务集合，完成后按原始命令位置归并。
///
/// 返回: 返回与作业顺序一致的响应集合；连接、写入或读取失败时丢弃连接并返回错误。
// 使用 mux 持有的连接执行一批普通单命令，用于保持单连接 FIFO 和失败后重连。
async fn send_with_mux_connection(
    node: &str,
    protocol_version: Option<u8>,
    factory: &BackendConnectionFactory,
    conn: &mut Option<BackendConnection>,
    jobs: &[OrdinaryJob],
) -> anyhow::Result<Vec<Vec<u8>>> {
    if conn.is_none() {
        *conn = Some(connect_with_retry(node, protocol_version, factory).await?);
    }
    let Some(active) = conn.as_mut() else {
        unreachable!("ordinary mux connection was just initialized");
    };
    let commands: Vec<Bytes> = jobs.iter().map(|job| job.raw.clone()).collect();
    match active.send_pipeline(&commands).await {
        Ok(frames) => Ok(frames),
        Err(err) => {
            let _ = conn.take();
            Err(err)
        }
    }
}

/// 业务作用: 建立 Redis Cluster 普通命令到后端连接池的复用通道 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
/// 参数说明:
/// - `node`: `host:port` 后端地址，重试期间始终连接同一节点。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
/// - `factory`: 后端连接工厂，用于按目标地址和认证配置创建受控连接。
///
/// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
// 创建后端连接并短重试，用于 ordinary mux 首次连接或失败后重连。
async fn connect_with_retry(
    node: &str,
    protocol_version: Option<u8>,
    factory: &BackendConnectionFactory,
) -> anyhow::Result<BackendConnection> {
    const CONNECT_ATTEMPTS: usize = 3;
    for attempt in 1..=CONNECT_ATTEMPTS {
        match factory.connect(node, protocol_version).await {
            Ok(conn) => return Ok(conn),
            Err(err) if attempt < CONNECT_ATTEMPTS => {
                debug!(
                    backend = %node,
                    attempt,
                    error = %err,
                    "ordinary backend mux connect failed, retrying"
                );
                tokio::time::sleep(Duration::from_millis((attempt * 20) as u64)).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("CONNECT_ATTEMPTS loop always returns");
}

impl BackendConnectionFactory {
    /// 业务作用: 建立 Redis Cluster 普通命令到后端连接池的复用通道 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
    /// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
    ///
    /// 返回: 建连、认证、协议协商和资源初始化全部成功时返回可用实例；任一步失败时返回错误。
    // 创建后端连接，用于 ordinary mux 初始化或连接失效后的重连。
    async fn connect(
        &self,
        addr: &str,
        protocol_version: Option<u8>,
    ) -> anyhow::Result<BackendConnection> {
        let mut conn = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
            addr,
            username: self.username.as_deref(),
            password: self.password.as_deref(),
            connect_timeout_ms: self.connect_timeout_ms,
            request_timeout_ms: self.request_timeout_ms,
            tcp_keepalive_ms: self.tcp_keepalive_ms,
            response_bytes_limit: Some(self.response_bytes_limit),
            metrics: Some(self.metrics.clone()),
            tls: self.tls.as_ref(),
        })
        .await?;
        if let Some(protocol_version) = protocol_version {
            conn.set_protocol_version(protocol_version).await?;
        }
        Ok(conn)
    }
}

/// 业务作用: 构造 mux key，用于把 RESP2/RESP3 ordinary 后端连接隔离开。
/// 参数说明:
/// - `node`: `host:port` 后端地址，与协议版本共同组成 mux 键。
/// - `protocol_version`: 当前前端会话使用的 RESP 版本，决定后端连接隔离和响应编码形态。
///
// 构造 mux key，用于把 RESP2/RESP3 ordinary 后端连接隔离开。
fn mux_key(node: &str, protocol_version: Option<u8>) -> String {
    match protocol_version {
        Some(version) => format!("{node}|resp{version}"),
        None => node.to_string(),
    }
}

/// 业务作用: 生成后端认证配置，用于确保 pass-through 模式不会被 ordinary mux 连接提前认证。
/// 参数说明:
/// - `item`: 当前代理配置项，包含模式、监听地址、后端、认证和资源限制。
///
// 生成后端认证配置，用于确保 pass-through 模式不会被 ordinary mux 连接提前认证。
fn backend_credentials(item: &ProxyItem) -> (Option<String>, Option<String>) {
    if item.effective_auth_mode() == AuthMode::PassThrough {
        (None, None)
    } else {
        (item.username.clone(), item.password.clone())
    }
}
