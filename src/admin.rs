use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::header;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};
use tracing::{info, warn};

use crate::coordinator::runtime::CoordinatorHandle;
use crate::ha::runtime::{ReplicationHaRuntime, ReplicationHaStatus};
use crate::reload::{ClusterProposalCandidate, ReloadHandle, ReloadSnapshot};
use crate::sentinel::{SentinelRuntime, SentinelStatus};

/// 业务作用: 汇总管理端服务所需的全局就绪标记、代理运行时注册表和认证策略，作为所有 HTTP handler 的只读共享入口。
/// 字段说明: `ready` 表示进程级开关，`registry` 提供各代理实时状态，`auth` 统一执行管理请求认证；三者通过克隆共享句柄而不是复制状态。
///
/// 设计原因: handler 不直接持有代理内部可变对象，而是按请求生成短生命周期快照，可避免管理查询长时间占用数据面锁。
#[derive(Clone)]
// 表示 AdminState 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct AdminState {
    ready: Arc<AtomicBool>,
    registry: RuntimeRegistry,
    auth: Arc<std::sync::RwLock<AdminAuth>>,
    reload: Option<ReloadHandle>,
    coordinator: Arc<std::sync::RwLock<Option<Arc<CoordinatorHandle>>>>,
}

impl AdminState {
    /// 业务作用: 构造默认已就绪且显式免认证的管理状态，供测试或已确认可信边界的调用方使用。
    /// 参数说明: 无。
    pub fn ready() -> Self {
        Self::ready_with_auth(AdminAuth::disabled())
    }

    /// 业务作用: 使用指定认证策略构造初始已就绪的空管理状态。
    /// 参数说明:
    /// - `auth`: 管理接口认证策略，决定请求是否必须携带有效令牌。
    ///
    pub fn ready_with_auth(auth: AdminAuth) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(true)),
            registry: RuntimeRegistry::default(),
            auth: Arc::new(std::sync::RwLock::new(auth)),
            reload: None,
            coordinator: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// 业务作用: 使用明确的认证策略和配置热更新句柄创建就绪管理状态。
    ///
    /// 参数说明:
    /// - `auth`: 管理接口认证策略。
    /// - `reload`: 串行配置校验、发布和状态查询句柄。
    ///
    /// 返回: 已启用配置管理端点的共享管理状态。
    pub fn ready_with_auth_and_reload(auth: AdminAuth, reload: ReloadHandle) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(true)),
            registry: RuntimeRegistry::default(),
            auth: Arc::new(std::sync::RwLock::new(auth)),
            reload: Some(reload),
            coordinator: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    /// 业务作用: 安装进程级协调器，使管理端热更新自动进入 gRPC 多数派提交和成员收敛事务。
    ///
    /// 参数说明:
    /// - `coordinator`: 当前进程共享的协调器句柄。
    pub fn set_coordinator(&self, coordinator: Arc<CoordinatorHandle>) {
        let mut current = self
            .coordinator
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = Some(coordinator);
    }

    /// 业务作用: 原子替换受保护管理接口使用的认证策略。
    ///
    /// 参数说明:
    /// - `auth`: 已通过配置校验的新令牌或显式免认证策略。
    pub fn set_auth(&self, auth: AdminAuth) {
        let mut current = self
            .auth
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = auth;
    }

    /// 业务作用: 更新进程基础就绪标记，供健康门禁与动态路由状态共同计算最终就绪结果。
    /// 参数说明:
    /// - `ready`: 进程或代理的就绪状态，决定管理接口是否允许流量进入。
    ///
    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// 业务作用: 返回共享运行时注册表，供配置发布与管理状态读取同一组代理运行代。
    /// 返回共享运行时注册表句柄，供管理面和配置发布流程查询或替换代理运行代。
    pub fn registry(&self) -> RuntimeRegistry {
        self.registry.clone()
    }

    // 这里只读取进程启动门禁；协调 revision 和 HA 路由状态由 effective_ready 统一叠加，
    // 避免基础标记覆盖动态保护原因。
    /// 业务作用: 读取进程基础就绪标记；最终 /ready 还会叠加协调版本与动态路由门禁。
    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// 业务作用: 使用当前热更新后的认证策略校验管理请求头。
    /// 参数说明:
    /// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
    ///
    fn authorize(&self, headers: &HeaderMap) -> bool {
        self.auth
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .authorize(headers)
    }

    /// 业务作用: 汇总代理运行代、配置事务与协调状态，生成管理端一致性快照。
    async fn snapshot(&self) -> AdminStatus {
        let registry = self.registry.snapshot().await;
        let config_reload = match &self.reload {
            Some(reload) => Some(reload.snapshot().await),
            None => None,
        };
        let coordinator = self
            .coordinator
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let cluster_configuration_ready = match coordinator {
            Some(coordinator) => coordinator.cluster_configuration_ready().await,
            None => true,
        };
        let ready = self.is_ready()
            && cluster_configuration_ready
            && registry.proxies.iter().all(|proxy| {
                proxy
                    .ha
                    .as_ref()
                    .is_none_or(|ha| ha.routing_state == "serving")
                    && proxy
                        .sentinel
                        .as_ref()
                        .is_none_or(|sentinel| sentinel.routing_state == "serving")
            });
        AdminStatus {
            ready,
            proxy_count: registry.proxies.len(),
            proxies: registry.proxies,
            draining_generations: registry.draining_generations,
            config_reload,
        }
    }

    /// 业务作用: 查询 effective_ready 状态，让 /ready 同时反映托管复制 HA 的保护状态。
    // 基础 ready 只是必要条件；任一协调 revision 落后、Sentinel 未确认主节点或
    // replication-ha 处于保护态，都必须让负载均衡摘除当前进程。
    async fn effective_ready(&self) -> (bool, Vec<String>) {
        let mut reasons = Vec::new();
        if !self.is_ready() {
            reasons.push("admin state is not ready".to_string());
        }
        reasons.extend(self.registry.replication_ha_not_ready_reasons().await);
        reasons.extend(self.registry.sentinel_not_ready_reasons().await);
        let coordinator = self
            .coordinator
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(coordinator) = coordinator {
            if !coordinator.cluster_configuration_ready().await {
                reasons.push(
                    "local active configuration does not match cluster committed revision"
                        .to_string(),
                );
            }
        }
        (reasons.is_empty(), reasons)
    }
}

/// 业务作用: 保存管理接口的可选认证令牌，并统一校验自定义请求头与 Bearer 认证头。
/// 字段说明: `token=None` 仅表示配置已经明确允许免认证；存在令牌时所有受保护端点必须通过固定轮数比较。
///
/// 设计原因: 把认证策略集中在单一对象中，可防止新增管理端点遗漏认证判断或采用不一致的比较方式。
#[derive(Clone)]
// 表示 AdminAuth 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct AdminAuth {
    token: Option<Arc<str>>,
}

impl AdminAuth {
    /// 业务作用: 构造由配置显式授权的免认证管理策略。
    /// 参数说明: 无。
    pub fn disabled() -> Self {
        Self { token: None }
    }

    /// 业务作用: 构造持有指定令牌的管理认证策略。
    /// 参数说明:
    /// - `token`: 鉴权令牌或配置片段，用于识别管理端请求或解析节点输入。
    ///
    pub fn token(token: impl Into<String>) -> Self {
        Self {
            token: Some(Arc::from(token.into())),
        }
    }

    /// 业务作用: 同时支持专用请求头和 Bearer 令牌，并以固定轮数比较避免时序泄漏。
    /// 参数说明:
    /// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
    ///
    fn authorize(&self, headers: &HeaderMap) -> bool {
        let Some(expected) = self.token.as_deref() else {
            // 免认证只能由配置校验显式构造，运行时在这里不再猜测网络是否可信。
            return true;
        };

        // 优先支持简洁的专用请求头，比较过程不因首个不同字节提前结束。
        if let Some(value) = headers
            .get("x-admin-token")
            .and_then(|value| value.to_str().ok())
        {
            if constant_time_eq(value.as_bytes(), expected.as_bytes()) {
                return true;
            }
        }

        // Bearer 方案用于标准 HTTP 客户端；前缀必须完全匹配，避免接受语义模糊的认证格式。
        let Some(value) = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
        else {
            return false;
        };
        let Some(token) = value.strip_prefix("Bearer ") else {
            return false;
        };
        constant_time_eq(token.as_bytes(), expected.as_bytes())
    }
}

/// 业务作用: 保存当前活动代理运行代和仍有连接待排空的旧代，是管理查询与配置发布共享的权威注册表。
#[derive(Clone, Default)]
// 表示 RuntimeRegistry 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct RuntimeRegistry {
    inner: Arc<RwLock<RuntimeRegistryInner>>,
}

/// 保存活动代理和仍有连接待排空的旧运行代。
#[derive(Default)]
struct RuntimeRegistryInner {
    proxies: Vec<ProxyRuntime>,
    draining_generations: Vec<DrainingProxyGeneration>,
}

impl RuntimeRegistry {
    /// 业务作用: 按稳定名称注册或替换活动代理运行代，并保留仍有连接的旧代排空观测。
    /// 参数说明:
    /// - `registration`: 单个代理运行时注册信息，包含静态配置、指标和可选 HA 句柄。
    ///
    pub async fn register_proxy(&self, registration: ProxyRegistration) {
        let mut inner = self.inner.write().await;
        prune_drained_generations(&mut inner.draining_generations);
        let replacement = ProxyRuntime::from(registration);
        if let Some(index) = inner
            .proxies
            .iter_mut()
            .position(|proxy| proxy.info.name == replacement.info.name)
        {
            let existing = std::mem::replace(&mut inner.proxies[index], replacement);
            if existing.generation != inner.proxies[index].generation {
                retain_draining_generation(
                    &mut inner.draining_generations,
                    existing.info.name,
                    existing.generation,
                    existing.generation_activity,
                );
            }
            return;
        }
        inner.proxies.push(replacement);
    }

    /// 业务作用: 从活动管理注册表移除代理项，并保留仍有连接待排空的旧运行代观测。
    ///
    /// 参数说明:
    /// - `proxy_name`: 要注销的代理稳定名称。
    ///
    /// 返回: 找到并移除代理时返回 `true`，代理已经不存在时返回 `false`。
    pub async fn unregister_proxy(&self, proxy_name: &str) -> bool {
        let mut inner = self.inner.write().await;
        prune_drained_generations(&mut inner.draining_generations);
        let Some(index) = inner
            .proxies
            .iter()
            .position(|proxy| proxy.info.name == proxy_name)
        else {
            return false;
        };
        let removed = inner.proxies.remove(index);
        retain_draining_generation(
            &mut inner.draining_generations,
            removed.info.name,
            removed.generation,
            removed.generation_activity,
        );
        true
    }

    /// 业务作用: 返回指定代理当前活动的托管复制 HA 运行时。
    ///
    /// 参数说明:
    /// - `proxy_name`: 配置事务要执行拓扑提交前动作的稳定代理名称。
    ///
    /// 返回: 同名代理属于 `replication-ha` 时返回共享运行时，否则返回空。
    pub(crate) async fn replication_ha_runtime(
        &self,
        proxy_name: &str,
    ) -> Option<Arc<ReplicationHaRuntime>> {
        self.inner
            .read()
            .await
            .proxies
            .iter()
            .find(|proxy| proxy.info.name == proxy_name)
            .and_then(|proxy| proxy.replication_ha.clone())
    }

    /// 业务作用: 读取全部活动代理与未排空旧代，生成包含动态 HA 状态和指标的注册表快照。
    async fn snapshot(&self) -> RuntimeRegistrySnapshot {
        let (proxies, draining_generations) = {
            let mut inner = self.inner.write().await;
            prune_drained_generations(&mut inner.draining_generations);
            (inner.proxies.clone(), inner.draining_generations.clone())
        };
        let mut out = Vec::with_capacity(proxies.len());
        for proxy in proxies {
            let proxy_name = proxy.info.name.clone();
            let ha = match &proxy.replication_ha {
                Some(runtime) => Some(runtime.status().await),
                None => None,
            };
            let sentinel = match &proxy.sentinel {
                Some(runtime) => Some(runtime.status().await),
                None => None,
            };
            out.push(ProxyStatus {
                name: proxy.info.name,
                backend_profile: proxy.info.backend_profile,
                mode: proxy.info.mode,
                listen: proxy.info.listen,
                backend: proxy.info.backend,
                nodes: proxy.info.nodes,
                auth_mode: proxy.info.auth_mode,
                backend_tls_enabled: proxy.info.backend_tls_enabled,
                sentinel_tls_enabled: proxy.info.sentinel_tls_enabled,
                connect_timeout_ms: proxy.info.connect_timeout_ms,
                backend_request_timeout_ms: proxy.info.backend_request_timeout_ms,
                frontend_auth_timeout_ms: proxy.info.frontend_auth_timeout_ms,
                client_idle_timeout_ms: proxy.info.client_idle_timeout_ms,
                tcp_keepalive_ms: proxy.info.tcp_keepalive_ms,
                max_connections: proxy.info.max_connections,
                dedicated_tunnel_max_active: proxy.info.dedicated_tunnel_max_active,
                dedicated_tunnel_queue_size: proxy.info.dedicated_tunnel_queue_size,
                dedicated_tunnel_acquire_timeout_ms: proxy.info.dedicated_tunnel_acquire_timeout_ms,
                dedicated_tunnel_pool_max_idle: proxy.info.dedicated_tunnel_pool_max_idle,
                dedicated_tunnel_pool_idle_timeout_ms: proxy
                    .info
                    .dedicated_tunnel_pool_idle_timeout_ms,
                pipeline_max_commands: proxy.info.pipeline_max_commands,
                pipeline_max_bytes: proxy.info.pipeline_max_bytes,
                pipeline_backend_max_active: proxy.info.pipeline_backend_max_active,
                pipeline_backend_queue_size: proxy.info.pipeline_backend_queue_size,
                pipeline_backend_submit_timeout_ms: proxy.info.pipeline_backend_submit_timeout_ms,
                frame_max_bytes: proxy.info.frame_max_bytes,
                bulk_max_bytes: proxy.info.bulk_max_bytes,
                aggregate_max_concurrency: proxy.info.aggregate_max_concurrency,
                aggregate_max_items: proxy.info.aggregate_max_items,
                pending_response_bytes: proxy.info.pending_response_bytes,
                graceful_shutdown_timeout_ms: proxy.info.graceful_shutdown_timeout_ms,
                cluster_topology_refresh_ms: proxy.info.cluster_topology_refresh_ms,
                generation: proxy.generation,
                draining_generations: draining_generations
                    .iter()
                    .filter(|draining| draining.proxy == proxy_name)
                    .map(DrainingProxyGeneration::snapshot)
                    .collect(),
                metrics: proxy.metrics.snapshot(),
                ha,
                sentinel,
            });
        }
        RuntimeRegistrySnapshot {
            proxies: out,
            draining_generations: draining_generations
                .iter()
                .map(DrainingProxyGeneration::snapshot)
                .collect(),
        }
    }

    // readiness 必须汇总所有 replication-ha 运行代的保护原因，不能只反映进程存活。
    /// 业务作用: readiness 必须汇总所有 replication-ha 运行代的保护原因，不能只反映进程存活。
    async fn replication_ha_not_ready_reasons(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        let mut reasons = Vec::new();
        for proxy in inner.proxies.iter() {
            let Some(runtime) = &proxy.replication_ha else {
                continue;
            };
            let (state, reason) = runtime.routing_status();
            if state != "serving" {
                reasons.push(format!(
                    "proxy `{}` replication_ha routing_state={state}: {}",
                    proxy.info.name,
                    reason.unwrap_or_else(|| "protected".to_string())
                ));
            }
        }
        reasons
    }

    /// 业务作用: 汇总所有 Sentinel 代理当前不可就绪的安全路由原因。
    ///
    /// 返回：只包含非 Serving Sentinel 代理的可读原因列表。
    async fn sentinel_not_ready_reasons(&self) -> Vec<String> {
        let inner = self.inner.read().await;
        let mut reasons = Vec::new();
        for proxy in inner.proxies.iter() {
            let Some(runtime) = &proxy.sentinel else {
                continue;
            };
            let (state, reason) = runtime.routing_status();
            if state != "serving" {
                reasons.push(format!(
                    "proxy `{}` sentinel routing_state={state}: {}",
                    proxy.info.name,
                    reason.unwrap_or_else(|| "route unavailable".to_string())
                ));
            }
        }
        reasons
    }

    /// 业务作用: 按可选代理名解析唯一 replication-ha 运行时，歧义、缺失或模式不符时拒绝管理动作。
    /// 参数说明:
    /// - `proxy_name`: 目标代理项名称，用于在同进程多代理中选择唯一运行时。
    ///
    /// 返回: 命中唯一 HA 代理时返回代理名和运行时；缺失、歧义或模式不符时返回错误。
    async fn replication_ha_runtime_handle(
        &self,
        proxy_name: Option<&str>,
    ) -> Result<(String, Arc<ReplicationHaRuntime>), String> {
        let inner = self.inner.read().await;
        let mut matches = Vec::new();
        let mut named_proxy_exists = false;
        for proxy in inner.proxies.iter() {
            if let Some(name) = proxy_name {
                if proxy.info.name != name {
                    continue;
                }
                named_proxy_exists = true;
            }
            if let Some(runtime) = &proxy.replication_ha {
                matches.push((proxy.info.name.clone(), runtime.clone()));
            }
        }

        match (proxy_name, matches.len()) {
            (_, 1) => Ok(matches.remove(0)),
            (Some(name), 0) if named_proxy_exists => {
                Err(format!("proxy `{name}` is not a replication HA proxy"))
            }
            (Some(name), 0) => Err(format!("replication HA proxy `{name}` not found")),
            (None, 0) => Err("no replication HA proxy is registered".to_string()),
            (None, _) => {
                Err("multiple replication HA proxies are registered; add ?proxy=<name>".to_string())
            }
            (Some(_), _) => {
                Err("unexpected duplicate replication HA proxy registration".to_string())
            }
        }
    }
}

/// 业务作用: 承载代理向管理面注册所需的静态配置、运行代、指标和可选 HA 控制句柄。
#[derive(Clone)]
// 表示 ProxyRegistration 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct ProxyRegistration {
    /// 启动时确定且不会随运行状态变化的代理配置摘要。
    pub info: ProxyStaticStatus,
    /// 当前配置运行代的单调递增编号。
    pub generation: u64,
    /// 当前运行代独立维护的活跃前端连接计数。
    pub generation_activity: ProxyGenerationActivity,
    /// replication HA 代理的运行时句柄；其他模式不提供该句柄。
    pub replication_ha: Option<Arc<ReplicationHaRuntime>>,
    /// Sentinel 代理的发现与安全路由运行时；其他模式不提供该句柄。
    pub sentinel: Option<Arc<SentinelRuntime>>,
    /// 当前代理项独立维护的连接、流量、后端和保护指标。
    pub metrics: ProxyMetrics,
}

/// 业务作用: 保存注册表内部可克隆的活动代理运行时，用于异步生成状态快照而不长期持锁。
#[derive(Clone)]
// 表示 ProxyRuntime 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
struct ProxyRuntime {
    info: ProxyStaticStatus,
    generation: u64,
    generation_activity: ProxyGenerationActivity,
    replication_ha: Option<Arc<ReplicationHaRuntime>>,
    sentinel: Option<Arc<SentinelRuntime>>,
    metrics: ProxyMetrics,
}

impl From<ProxyRegistration> for ProxyRuntime {
    /// 业务作用: 把代理注册信息转换为管理注册表内部运行时代码，并保留共享指标与控制句柄。
    /// 参数说明:
    /// - `registration`: 单个代理运行时注册信息，包含静态配置、指标和可选 HA 句柄。
    ///
    fn from(registration: ProxyRegistration) -> Self {
        Self {
            info: registration.info,
            generation: registration.generation,
            generation_activity: registration.generation_activity,
            replication_ha: registration.replication_ha,
            sentinel: registration.sentinel,
            metrics: registration.metrics,
        }
    }
}

/// 当前代理运行代独立维护的活跃连接计数，不与跨代累计业务指标混用。
#[derive(Clone, Default)]
pub struct ProxyGenerationActivity {
    active_connections: Arc<AtomicUsize>,
}

impl ProxyGenerationActivity {
    /// 业务作用: 记录一个前端连接进入当前配置运行代。
    pub fn connection_accepted(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一个前端连接离开当前配置运行代。
    pub fn connection_finished(&self) {
        let _ =
            self.active_connections
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                    Some(active.saturating_sub(1))
                });
    }

    /// 业务作用: 返回当前运行代仍未完成排空的前端连接数。
    ///
    /// 返回: 当前活跃连接的瞬时 gauge 值。
    fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
}

/// 保存一个已经退出活动槽位、但仍有前端连接待关闭的运行代。
#[derive(Clone)]
struct DrainingProxyGeneration {
    proxy: String,
    generation: u64,
    activity: ProxyGenerationActivity,
}

impl DrainingProxyGeneration {
    /// 业务作用: 生成不持有注册表锁的管理状态快照。
    ///
    /// 返回: 代理名、运行代编号和当前排空连接数。
    fn snapshot(&self) -> ProxyDrainingGenerationStatus {
        ProxyDrainingGenerationStatus {
            proxy: self.proxy.clone(),
            generation: self.generation,
            active_connections: self.activity.active_connections(),
        }
    }
}

/// 业务作用: 仅在旧运行代仍有连接时保留其排空观测，并避免重复登记同一代。
///
/// 参数说明:
/// - `draining`: 注册表中全部正在排空的旧运行代。
/// - `proxy`: 旧运行代所属的稳定代理名称。
/// - `generation`: 旧运行代的单调递增编号。
/// - `activity`: 旧运行代独立维护的活跃连接计数。
fn retain_draining_generation(
    draining: &mut Vec<DrainingProxyGeneration>,
    proxy: String,
    generation: u64,
    activity: ProxyGenerationActivity,
) {
    if activity.active_connections() == 0
        || draining
            .iter()
            .any(|current| current.proxy == proxy && current.generation == generation)
    {
        return;
    }
    draining.push(DrainingProxyGeneration {
        proxy,
        generation,
        activity,
    });
}

/// 业务作用: 删除已经没有活跃连接的旧运行代，避免状态和指标长期残留零值序列。
///
/// 参数说明:
/// - `draining`: 要原地清理的旧运行代注册集合。
fn prune_drained_generations(draining: &mut Vec<DrainingProxyGeneration>) {
    draining.retain(|generation| generation.activity.active_connections() > 0);
}

/// 业务作用: 提供单代理共享指标入口，所有数据面任务通过无锁计数和短临界区累计连接、命令、延迟与背压。
#[derive(Clone, Default)]
// 表示 ProxyMetrics 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct ProxyMetrics {
    inner: Arc<ProxyMetricsInner>,
}

impl std::fmt::Debug for ProxyMetrics {
    /// 业务作用: 格式化 管理端 HTTP API、运行时状态快照和指标采集 的调试输出，隐藏不适合直接打印的内部细节。
    /// 参数说明:
    /// - `f`: 对受保护内部状态执行的闭包，锁只在闭包运行期间持有。
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyMetrics").finish_non_exhaustive()
    }
}

/// 业务作用: 保存代理指标的原子计数器、峰值、近期速率窗口和延迟累计值。
#[derive(Default)]
// 表示 ProxyMetricsInner 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
struct ProxyMetricsInner {
    active_connections: AtomicUsize,
    accepted_connections: AtomicU64,
    rejected_connections: AtomicU64,
    closed_connections: AtomicU64,
    connection_errors: AtomicU64,
    pipeline_batches: AtomicU64,
    commands: AtomicU64,
    /// 由代理本地完成、未进入后端 pipeline 的兼容握手命令累计数。
    local_handshake_commands: AtomicU64,
    command_errors: AtomicU64,
    response_bytes: AtomicU64,
    max_pipeline_depth: AtomicUsize,
    pipeline_execution_errors: AtomicU64,
    pipeline_elapsed_micros: AtomicU64,
    max_pipeline_elapsed_micros: AtomicU64,
    rate_epoch_second: AtomicU64,
    rate_pipeline_batches_current: AtomicU64,
    rate_commands_current: AtomicU64,
    rate_pipeline_batches_last: AtomicU64,
    rate_commands_last: AtomicU64,
    pending_pipeline_batches: AtomicUsize,
    max_pending_pipeline_batches: AtomicUsize,
    pending_pipeline_commands: AtomicUsize,
    max_pending_pipeline_commands: AtomicUsize,
    pending_response_bytes: AtomicUsize,
    max_pending_response_bytes: AtomicUsize,
    pending_response_limit_hits: AtomicU64,
    relay_client_to_backend_bytes: AtomicU64,
    relay_backend_to_client_bytes: AtomicU64,
    max_relay_client_to_backend_bytes: AtomicU64,
    max_relay_backend_to_client_bytes: AtomicU64,
    relay_client_to_backend_backpressure_events: AtomicU64,
    relay_backend_to_client_backpressure_events: AtomicU64,
    backend_active_connections: AtomicUsize,
    max_backend_active_connections: AtomicUsize,
    backend_connection_attempts: AtomicU64,
    backend_connection_successes: AtomicU64,
    backend_connection_failures: AtomicU64,
    backend_auth_failures: AtomicU64,
    backend_forward_batches: AtomicU64,
    backend_forward_commands: AtomicU64,
    backend_forward_errors: AtomicU64,
    backend_forward_elapsed_micros: AtomicU64,
    max_backend_forward_elapsed_micros: AtomicU64,
    dedicated_tunnel_active: AtomicUsize,
    max_dedicated_tunnel_active: AtomicUsize,
    dedicated_tunnel_queue_waiters: AtomicUsize,
    max_dedicated_tunnel_queue_waiters: AtomicUsize,
    dedicated_tunnel_acquire_attempts: AtomicU64,
    dedicated_tunnel_acquire_successes: AtomicU64,
    dedicated_tunnel_rejections: AtomicU64,
    dedicated_tunnel_acquire_timeouts: AtomicU64,
    /// 从后端专属 tunnel pool 成功借出 clean idle 连接的累计次数。
    tunnel_pool_hits: AtomicU64,
    /// 后端专属 tunnel pool 无可用连接、需要新建连接的累计次数。
    tunnel_pool_misses: AtomicU64,
    /// 完整消费唯一响应后，clean 连接成功归池的累计次数。
    tunnel_pool_returns: AtomicU64,
    /// tainted、过期、池满或复用关闭导致连接未归池的累计次数。
    tunnel_pool_discards: AtomicU64,
    /// 当前代理全部后端专属 tunnel pool 保存的 idle 连接总数。
    tunnel_pool_idle: AtomicUsize,
    /// 当前代理全部后端专属 tunnel pool 的配置容量总和。
    tunnel_pool_max_idle: AtomicUsize,
    cluster_aggregate_active: AtomicUsize,
    max_cluster_aggregate_active: AtomicUsize,
    cluster_aggregate_operations: AtomicU64,
    cluster_aggregate_rejections: AtomicU64,
    cluster_aggregate_elapsed_micros: AtomicU64,
    max_cluster_aggregate_elapsed_micros: AtomicU64,
}

impl ProxyMetrics {
    /// 业务作用: 记录由代理本地完成的客户端兼容握手，只补充分类指标；总命令数仍由
    /// `record_pipeline_result` 统一累计。
    ///
    /// 参数说明:
    /// - `command_count`: 本批已经成功生成本地响应的握手命令数量。
    ///
    /// 返回:
    /// - 无返回值；单调增加本地握手分类 counter。
    pub fn record_local_handshake_commands(&self, command_count: usize) {
        self.inner
            .local_handshake_commands
            .fetch_add(command_count as u64, Ordering::Relaxed);
    }

    /// 业务作用: 结束一个前端连接的生命周期计数，并按结果累计连接错误。
    pub fn connection_accepted(&self) {
        self.inner
            .accepted_connections
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 建立 管理端 HTTP API、运行时状态快照和指标采集 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    pub fn connection_rejected(&self) {
        self.inner
            .rejected_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 建立 管理端 HTTP API、运行时状态快照和指标采集 使用的网络连接，并在连接阶段完成超时、认证或资源统计。
    /// 参数说明:
    /// - `error`: 错误或原因文本，用于保护态、日志和失败上下文。
    ///
    pub fn connection_finished(&self, error: bool) {
        self.inner
            .closed_connections
            .fetch_add(1, Ordering::Relaxed);
        let _ = self.inner.active_connections.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |active| Some(active.saturating_sub(1)),
        );
        if error {
            self.inner.connection_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 业务作用: 一次性累计 pipeline 的批次、命令、响应、错误、耗时和近期速率指标。
    /// 参数说明:
    /// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    /// - `response_bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
    /// - `error_count`: 错误或原因文本，用于保护态、日志和失败上下文。
    /// - `elapsed_micros`: 本次操作耗时的微秒值，用于延迟累计、最大值和直方指标。
    /// - `execution_error`: 错误或原因文本，用于保护态、日志和失败上下文。
    ///
    // 同一次 pipeline 的关联指标在一个入口更新，避免调用方错误遗漏部分计数。
    pub fn record_pipeline_result(
        &self,
        command_count: usize,
        response_bytes: usize,
        error_count: usize,
        elapsed_micros: u64,
        execution_error: bool,
    ) {
        self.inner.pipeline_batches.fetch_add(1, Ordering::Relaxed);
        self.inner
            .commands
            .fetch_add(command_count as u64, Ordering::Relaxed);
        self.inner
            .response_bytes
            .fetch_add(response_bytes as u64, Ordering::Relaxed);
        self.inner
            .command_errors
            .fetch_add(error_count as u64, Ordering::Relaxed);
        let _ = self.inner.max_pipeline_depth.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(command_count)),
        );
        if execution_error {
            self.inner
                .pipeline_execution_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .pipeline_elapsed_micros
            .fetch_add(elapsed_micros, Ordering::Relaxed);
        let _ = self.inner.max_pipeline_elapsed_micros.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(elapsed_micros)),
        );
        self.record_recent_rate(command_count);
    }

    /// 业务作用: 把本批 pipeline 和命令数累计到当前秒的近期速率窗口。
    /// 参数说明:
    /// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    // 先旋转时间窗口再加当前批次，防止跨秒请求被计入已经结束的桶。
    fn record_recent_rate(&self, command_count: usize) {
        self.rotate_rate_window(current_unix_second());
        self.inner
            .rate_pipeline_batches_current
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .rate_commands_current
            .fetch_add(command_count as u64, Ordering::Relaxed);
    }

    /// 业务作用: 按 Unix 秒推进近期速率环形窗口，清空跨过的桶并保留当前秒增量。
    /// 参数说明:
    /// - `now_second`: 当前秒级时间桶，用于旋转速率窗口并避免跨秒混算。
    ///
    fn rotate_rate_window(&self, now_second: u64) {
        loop {
            let current_second = self.inner.rate_epoch_second.load(Ordering::Relaxed);
            if current_second == 0 {
                if self
                    .inner
                    .rate_epoch_second
                    .compare_exchange(0, now_second, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    return;
                }
                continue;
            }
            if now_second <= current_second {
                return;
            }
            if self
                .inner
                .rate_epoch_second
                .compare_exchange(
                    current_second,
                    now_second,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                let batches = self
                    .inner
                    .rate_pipeline_batches_current
                    .swap(0, Ordering::Relaxed);
                let commands = self.inner.rate_commands_current.swap(0, Ordering::Relaxed);
                if now_second == current_second.saturating_add(1) {
                    self.inner
                        .rate_pipeline_batches_last
                        .store(batches, Ordering::Relaxed);
                    self.inner
                        .rate_commands_last
                        .store(commands, Ordering::Relaxed);
                } else {
                    self.inner
                        .rate_pipeline_batches_last
                        .store(0, Ordering::Relaxed);
                    self.inner.rate_commands_last.store(0, Ordering::Relaxed);
                }
                return;
            }
        }
    }

    /// 业务作用: 登记一个待完成 pipeline 的批次和命令数，并返回离开作用域时自动减计数的守卫。
    /// 参数说明:
    /// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    ///
    // 批次与命令 gauge 必须由同一守卫成对回收，错误返回也不会残留虚高值。
    pub fn start_pending_pipeline(&self, command_count: usize) -> PendingPipelineGuard {
        let batches = self
            .inner
            .pending_pipeline_batches
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _ = self.inner.max_pending_pipeline_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(batches)),
        );

        let commands = self
            .inner
            .pending_pipeline_commands
            .fetch_add(command_count, Ordering::Relaxed)
            .saturating_add(command_count);
        let _ = self.inner.max_pending_pipeline_commands.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(commands)),
        );

        PendingPipelineGuard {
            metrics: self.clone(),
            command_count,
        }
    }

    /// 业务作用: 登记当前待写响应字节，并返回离开作用域时自动减计数的守卫。
    /// 参数说明:
    /// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
    ///
    // 使用生命周期守卫覆盖正常写回和错误退出，避免 pending 字节 gauge 泄漏。
    pub fn start_pending_response(&self, bytes: usize) -> PendingResponseGuard {
        let current = self
            .inner
            .pending_response_bytes
            .fetch_add(bytes, Ordering::Relaxed)
            .saturating_add(bytes);
        let _ = self.inner.max_pending_response_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(current)),
        );
        PendingResponseGuard {
            metrics: self.clone(),
            bytes,
        }
    }

    /// 业务作用: 记录一次待写响应超过单连接字节上限的拒绝事件。
    pub fn record_pending_response_limit_hit(&self) {
        self.inner
            .pending_response_limit_hits
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 累计本次透明 relay 双向传输字节，并更新两个方向的单连接峰值。
    /// 参数说明:
    /// - `client_to_backend`: 客户端到后端方向累计传输的字节数，用于流量指标统计。
    /// - `backend_to_client`: 后端到客户端方向累计传输的字节数，用于流量指标统计。
    ///
    // 总量和峰值必须使用同一批采样，确保管理快照可解释同一次 relay 结果。
    pub fn record_relay_bytes(&self, client_to_backend: u64, backend_to_client: u64) {
        self.inner
            .relay_client_to_backend_bytes
            .fetch_add(client_to_backend, Ordering::Relaxed);
        self.inner
            .relay_backend_to_client_bytes
            .fetch_add(backend_to_client, Ordering::Relaxed);
        let _ = self.inner.max_relay_client_to_backend_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(client_to_backend)),
        );
        let _ = self.inner.max_relay_backend_to_client_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(backend_to_client)),
        );
    }

    /// 业务作用: 按方向记录透明 relay 因写端跟不上而发生的背压事件。
    /// 参数说明:
    /// - `client_to_backend`: 客户端到后端方向累计传输的字节数，用于流量指标统计。
    ///
    // 布尔方向在这里映射为固定 counter，调用方无需持有内部指标结构。
    pub fn record_relay_backpressure(&self, client_to_backend: bool) {
        let counter = if client_to_backend {
            &self.inner.relay_client_to_backend_backpressure_events
        } else {
            &self.inner.relay_backend_to_client_backpressure_events
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一次后端连接尝试；成功或失败计数由后续结果路径分别累计。
    pub fn record_backend_connection_attempt(&self) {
        self.inner
            .backend_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一次后端连接建立失败。
    pub fn record_backend_connection_failure(&self) {
        self.inner
            .backend_connection_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一次后端认证失败。
    pub fn record_backend_auth_failure(&self) {
        self.inner
            .backend_auth_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 增加活动后端连接 gauge 并返回析构时自动减计数的守卫。
    /// 记录一个后端连接成功建立，并返回在析构时自动递减活跃连接数的守卫。
    ///
    /// 调用方必须让守卫与底层连接保持相同生命周期，避免连接指标只增不减。
    pub fn backend_connection_opened(&self) -> BackendConnectionGuard {
        self.inner
            .backend_connection_successes
            .fetch_add(1, Ordering::Relaxed);
        let active = self
            .inner
            .backend_active_connections
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _ = self.inner.max_backend_active_connections.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(active)),
        );
        BackendConnectionGuard {
            metrics: self.clone(),
        }
    }

    /// 业务作用: 累计一次后端转发批次的命令数、耗时和执行结果，并更新峰值延迟。
    /// 参数说明:
    /// - `command_count`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
    /// - `elapsed_micros`: 本次操作耗时的微秒值，用于延迟累计、最大值和直方指标。
    /// - `error`: 错误或原因文本，用于保护态、日志和失败上下文。
    ///
    // 批次计数、命令计数与失败计数从同一结果更新，避免部分指标因错误分支漏记。
    pub fn record_backend_forward_result(
        &self,
        command_count: usize,
        elapsed_micros: u64,
        error: bool,
    ) {
        self.inner
            .backend_forward_batches
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .backend_forward_commands
            .fetch_add(command_count as u64, Ordering::Relaxed);
        if error {
            self.inner
                .backend_forward_errors
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner
            .backend_forward_elapsed_micros
            .fetch_add(elapsed_micros, Ordering::Relaxed);
        let _ = self.inner.max_backend_forward_elapsed_micros.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(elapsed_micros)),
        );
    }

    /// 业务作用: 记录一次专用隧道许可申请。
    pub fn record_dedicated_tunnel_acquire_attempt(&self) {
        self.inner
            .dedicated_tunnel_acquire_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 登记一条活动专用隧道并返回自动回收 gauge 的生命周期守卫。
    /// 记录一次专用隧道成功取得，并返回在隧道关闭时自动递减活跃数的守卫。
    pub fn dedicated_tunnel_opened(&self) -> DedicatedTunnelGuard {
        self.inner
            .dedicated_tunnel_acquire_successes
            .fetch_add(1, Ordering::Relaxed);
        let active = self
            .inner
            .dedicated_tunnel_active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _ = self.inner.max_dedicated_tunnel_active.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(active)),
        );
        DedicatedTunnelGuard {
            metrics: self.clone(),
        }
    }

    /// 业务作用: 登记一名专用隧道排队者并返回离队时自动减计数的守卫。
    /// 记录一个等待专用隧道许可的客户端，并用返回守卫保证退出等待时递减计数。
    pub fn dedicated_tunnel_queue_waiter_entered(&self) -> DedicatedTunnelQueueMetricsGuard {
        let waiters = self
            .inner
            .dedicated_tunnel_queue_waiters
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _ = self.inner.max_dedicated_tunnel_queue_waiters.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(waiters)),
        );
        DedicatedTunnelQueueMetricsGuard {
            metrics: self.clone(),
        }
    }

    /// 业务作用: 记录一次专用隧道因容量或队列边界被拒绝。
    pub fn record_dedicated_tunnel_rejection(&self) {
        self.inner
            .dedicated_tunnel_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一次专用隧道获取超时，并同时计入拒绝总数。
    pub fn record_dedicated_tunnel_acquire_timeout(&self) {
        self.inner
            .dedicated_tunnel_acquire_timeouts
            .fetch_add(1, Ordering::Relaxed);
        self.record_dedicated_tunnel_rejection();
    }

    /// 业务作用: 登记一个后端专属 tunnel pool 的静态容量。
    ///
    /// 参数说明:
    /// - `max_idle`: 新建后端池允许保留的最大 idle 连接数。
    ///
    /// 返回:
    /// - 无返回值；容量累加到代理级静态指标。
    pub fn register_tunnel_pool(&self, max_idle: usize) {
        self.inner
            .tunnel_pool_max_idle
            .fetch_add(max_idle, Ordering::Relaxed);
    }

    /// 业务作用: 注销一个已经退出路由代次的后端专属 tunnel pool 静态容量。
    ///
    /// 参数说明:
    /// - `max_idle`: 被销毁后端池原先登记的最大 idle 连接数。
    ///
    /// 返回:
    /// - 无返回值；使用饱和减法修正代理级容量 gauge，避免动态路由切换后容量累加。
    pub fn unregister_tunnel_pool(&self, max_idle: usize) {
        let _ = self.inner.tunnel_pool_max_idle.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |capacity| Some(capacity.saturating_sub(max_idle)),
        );
    }

    /// 业务作用: 记录一次 tunnel pool clean idle 连接命中。
    ///
    /// 返回:
    /// - 无返回值；单调增加命中 counter。
    pub fn record_tunnel_pool_hit(&self) {
        self.inner.tunnel_pool_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一次 tunnel pool 未命中，调用方随后需要新建后端连接。
    ///
    /// 返回:
    /// - 无返回值；单调增加未命中 counter。
    pub fn record_tunnel_pool_miss(&self) {
        self.inner
            .tunnel_pool_misses
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一条 clean 连接成功归池。
    ///
    /// 返回:
    /// - 无返回值；单调增加归池 counter。
    pub fn record_tunnel_pool_return(&self) {
        self.inner
            .tunnel_pool_returns
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 记录一条连接因 tainted、过期、池满或复用关闭而被丢弃。
    ///
    /// 返回:
    /// - 无返回值；等价于批量丢弃一条连接。
    pub fn record_tunnel_pool_discard(&self) {
        self.record_tunnel_pool_discards(1);
    }

    /// 业务作用: 批量记录被回收或丢弃的 tunnel 连接，避免逐条执行原子操作。
    ///
    /// 参数说明:
    /// - `count`: 本次从池中移除且不会复用的连接数量。
    ///
    /// 返回:
    /// - 无返回值；把数量累加到丢弃 counter。
    pub fn record_tunnel_pool_discards(&self, count: u64) {
        self.inner
            .tunnel_pool_discards
            .fetch_add(count, Ordering::Relaxed);
    }

    /// 业务作用: 增加当前代理全部后端池的 idle 连接总数。
    ///
    /// 参数说明:
    /// - `count`: 本次成功写入 idle 队列的连接数量。
    ///
    /// 返回:
    /// - 无返回值；原子增加当前 idle gauge。
    pub fn tunnel_pool_idle_added(&self, count: usize) {
        self.inner
            .tunnel_pool_idle
            .fetch_add(count, Ordering::Relaxed);
    }

    /// 业务作用: 减少当前代理全部后端池的 idle 连接总数。
    ///
    /// 参数说明:
    /// - `count`: 本次借出或过期移除的 idle 连接数量。
    ///
    /// 返回:
    /// - 无返回值；使用饱和减法防止异常路径导致 gauge 下溢。
    pub fn tunnel_pool_idle_removed(&self, count: usize) {
        let _ = self.inner.tunnel_pool_idle.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |idle| Some(idle.saturating_sub(count)),
        );
    }

    /// 业务作用: 登记一个活动 Cluster 聚合操作，并返回结束时记录耗时和回收 gauge 的守卫。
    // 聚合操作可能从多个错误分支返回，生命周期守卫保证活跃数与完成计数始终成对更新。
    pub fn start_cluster_aggregate(&self) -> ClusterAggregateGuard {
        let active = self
            .inner
            .cluster_aggregate_active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let _ = self.inner.max_cluster_aggregate_active.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |max| Some(max.max(active)),
        );
        ClusterAggregateGuard {
            metrics: self.clone(),
            started: Instant::now(),
        }
    }

    /// 业务作用: 记录一次 Cluster 聚合请求因并发或容量限制被拒绝。
    pub fn record_cluster_aggregate_rejection(&self) {
        self.inner
            .cluster_aggregate_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 业务作用: 原子读取连接、命令、延迟、背压和资源计数，生成同一时点的代理指标快照。
    fn snapshot(&self) -> ProxyMetricsSnapshot {
        self.rotate_rate_window(current_unix_second());
        let recent_pipeline_batches_per_second = self
            .inner
            .rate_pipeline_batches_current
            .load(Ordering::Relaxed)
            .max(
                self.inner
                    .rate_pipeline_batches_last
                    .load(Ordering::Relaxed),
            );
        let recent_commands_per_second = self
            .inner
            .rate_commands_current
            .load(Ordering::Relaxed)
            .max(self.inner.rate_commands_last.load(Ordering::Relaxed));
        ProxyMetricsSnapshot {
            active_connections: self.inner.active_connections.load(Ordering::Relaxed),
            accepted_connections: self.inner.accepted_connections.load(Ordering::Relaxed),
            rejected_connections: self.inner.rejected_connections.load(Ordering::Relaxed),
            closed_connections: self.inner.closed_connections.load(Ordering::Relaxed),
            connection_errors: self.inner.connection_errors.load(Ordering::Relaxed),
            pipeline_batches: self.inner.pipeline_batches.load(Ordering::Relaxed),
            commands: self.inner.commands.load(Ordering::Relaxed),
            local_handshake_commands: self.inner.local_handshake_commands.load(Ordering::Relaxed),
            command_errors: self.inner.command_errors.load(Ordering::Relaxed),
            response_bytes: self.inner.response_bytes.load(Ordering::Relaxed),
            max_pipeline_depth: self.inner.max_pipeline_depth.load(Ordering::Relaxed),
            pipeline_execution_errors: self.inner.pipeline_execution_errors.load(Ordering::Relaxed),
            pipeline_elapsed_micros: self.inner.pipeline_elapsed_micros.load(Ordering::Relaxed),
            max_pipeline_elapsed_micros: self
                .inner
                .max_pipeline_elapsed_micros
                .load(Ordering::Relaxed),
            recent_pipeline_batches_per_second,
            recent_commands_per_second,
            pending_pipeline_batches: self.inner.pending_pipeline_batches.load(Ordering::Relaxed),
            max_pending_pipeline_batches: self
                .inner
                .max_pending_pipeline_batches
                .load(Ordering::Relaxed),
            pending_pipeline_commands: self.inner.pending_pipeline_commands.load(Ordering::Relaxed),
            max_pending_pipeline_commands: self
                .inner
                .max_pending_pipeline_commands
                .load(Ordering::Relaxed),
            pending_response_bytes: self.inner.pending_response_bytes.load(Ordering::Relaxed),
            max_pending_response_bytes: self
                .inner
                .max_pending_response_bytes
                .load(Ordering::Relaxed),
            pending_response_limit_hits: self
                .inner
                .pending_response_limit_hits
                .load(Ordering::Relaxed),
            relay_client_to_backend_bytes: self
                .inner
                .relay_client_to_backend_bytes
                .load(Ordering::Relaxed),
            relay_backend_to_client_bytes: self
                .inner
                .relay_backend_to_client_bytes
                .load(Ordering::Relaxed),
            max_relay_client_to_backend_bytes: self
                .inner
                .max_relay_client_to_backend_bytes
                .load(Ordering::Relaxed),
            max_relay_backend_to_client_bytes: self
                .inner
                .max_relay_backend_to_client_bytes
                .load(Ordering::Relaxed),
            relay_client_to_backend_backpressure_events: self
                .inner
                .relay_client_to_backend_backpressure_events
                .load(Ordering::Relaxed),
            relay_backend_to_client_backpressure_events: self
                .inner
                .relay_backend_to_client_backpressure_events
                .load(Ordering::Relaxed),
            backend_active_connections: self
                .inner
                .backend_active_connections
                .load(Ordering::Relaxed),
            max_backend_active_connections: self
                .inner
                .max_backend_active_connections
                .load(Ordering::Relaxed),
            backend_connection_attempts: self
                .inner
                .backend_connection_attempts
                .load(Ordering::Relaxed),
            backend_connection_successes: self
                .inner
                .backend_connection_successes
                .load(Ordering::Relaxed),
            backend_connection_failures: self
                .inner
                .backend_connection_failures
                .load(Ordering::Relaxed),
            backend_auth_failures: self.inner.backend_auth_failures.load(Ordering::Relaxed),
            backend_forward_batches: self.inner.backend_forward_batches.load(Ordering::Relaxed),
            backend_forward_commands: self.inner.backend_forward_commands.load(Ordering::Relaxed),
            backend_forward_errors: self.inner.backend_forward_errors.load(Ordering::Relaxed),
            backend_forward_elapsed_micros: self
                .inner
                .backend_forward_elapsed_micros
                .load(Ordering::Relaxed),
            max_backend_forward_elapsed_micros: self
                .inner
                .max_backend_forward_elapsed_micros
                .load(Ordering::Relaxed),
            dedicated_tunnel_active: self.inner.dedicated_tunnel_active.load(Ordering::Relaxed),
            max_dedicated_tunnel_active: self
                .inner
                .max_dedicated_tunnel_active
                .load(Ordering::Relaxed),
            dedicated_tunnel_queue_waiters: self
                .inner
                .dedicated_tunnel_queue_waiters
                .load(Ordering::Relaxed),
            max_dedicated_tunnel_queue_waiters: self
                .inner
                .max_dedicated_tunnel_queue_waiters
                .load(Ordering::Relaxed),
            dedicated_tunnel_acquire_attempts: self
                .inner
                .dedicated_tunnel_acquire_attempts
                .load(Ordering::Relaxed),
            dedicated_tunnel_acquire_successes: self
                .inner
                .dedicated_tunnel_acquire_successes
                .load(Ordering::Relaxed),
            dedicated_tunnel_rejections: self
                .inner
                .dedicated_tunnel_rejections
                .load(Ordering::Relaxed),
            dedicated_tunnel_acquire_timeouts: self
                .inner
                .dedicated_tunnel_acquire_timeouts
                .load(Ordering::Relaxed),
            tunnel_pool_hits: self.inner.tunnel_pool_hits.load(Ordering::Relaxed),
            tunnel_pool_misses: self.inner.tunnel_pool_misses.load(Ordering::Relaxed),
            tunnel_pool_returns: self.inner.tunnel_pool_returns.load(Ordering::Relaxed),
            tunnel_pool_discards: self.inner.tunnel_pool_discards.load(Ordering::Relaxed),
            tunnel_pool_idle: self.inner.tunnel_pool_idle.load(Ordering::Relaxed),
            tunnel_pool_max_idle: self.inner.tunnel_pool_max_idle.load(Ordering::Relaxed),
            cluster_aggregate_active: self.inner.cluster_aggregate_active.load(Ordering::Relaxed),
            max_cluster_aggregate_active: self
                .inner
                .max_cluster_aggregate_active
                .load(Ordering::Relaxed),
            cluster_aggregate_operations: self
                .inner
                .cluster_aggregate_operations
                .load(Ordering::Relaxed),
            cluster_aggregate_rejections: self
                .inner
                .cluster_aggregate_rejections
                .load(Ordering::Relaxed),
            cluster_aggregate_elapsed_micros: self
                .inner
                .cluster_aggregate_elapsed_micros
                .load(Ordering::Relaxed),
            max_cluster_aggregate_elapsed_micros: self
                .inner
                .max_cluster_aggregate_elapsed_micros
                .load(Ordering::Relaxed),
        }
    }
}

/// 业务作用: 登记未完成 pipeline 的命令和字节占用，析构时自动回收 pending gauge。
// 表示 PendingPipelineGuard 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct PendingPipelineGuard {
    metrics: ProxyMetrics,
    command_count: usize,
}

impl Drop for PendingPipelineGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self.metrics.inner.pending_pipeline_batches.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
        let _ = self.metrics.inner.pending_pipeline_commands.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(self.command_count)),
        );
    }
}

/// 业务作用: 登记尚未写回前端的响应字节，析构时自动回收 pending response gauge。
// 表示 PendingResponseGuard 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct PendingResponseGuard {
    metrics: ProxyMetrics,
    bytes: usize,
}

impl Drop for PendingResponseGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self.metrics.inner.pending_response_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(self.bytes)),
        );
    }
}

/// 业务作用: 登记活动后端连接，连接生命周期结束时自动减少 gauge。
// 表示 BackendConnectionGuard 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct BackendConnectionGuard {
    metrics: ProxyMetrics,
}

impl std::fmt::Debug for BackendConnectionGuard {
    /// 业务作用: 格式化 管理端 HTTP API、运行时状态快照和指标采集 的调试输出，隐藏不适合直接打印的内部细节。
    /// 参数说明:
    /// - `f`: 对受保护内部状态执行的闭包，锁只在闭包运行期间持有。
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConnectionGuard")
            .finish_non_exhaustive()
    }
}

impl Drop for BackendConnectionGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self.metrics.inner.backend_active_connections.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }
}

/// 业务作用: 登记活动专用隧道并维护峰值，隧道释放时自动减少 gauge。
pub struct DedicatedTunnelGuard {
    metrics: ProxyMetrics,
}

impl std::fmt::Debug for DedicatedTunnelGuard {
    /// 业务作用: 格式化 管理端 HTTP API、运行时状态快照和指标采集 的调试输出，隐藏不适合直接打印的内部细节。
    /// 参数说明:
    /// - `f`: 对受保护内部状态执行的闭包，锁只在闭包运行期间持有。
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DedicatedTunnelGuard")
            .finish_non_exhaustive()
    }
}

impl Drop for DedicatedTunnelGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self.metrics.inner.dedicated_tunnel_active.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
    }
}

/// 业务作用: 登记专用隧道等待者，离开队列时自动减少等待 gauge。
pub struct DedicatedTunnelQueueMetricsGuard {
    metrics: ProxyMetrics,
}

impl std::fmt::Debug for DedicatedTunnelQueueMetricsGuard {
    /// 业务作用: 格式化 管理端 HTTP API、运行时状态快照和指标采集 的调试输出，隐藏不适合直接打印的内部细节。
    /// 参数说明:
    /// - `f`: 对受保护内部状态执行的闭包，锁只在闭包运行期间持有。
    ///
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DedicatedTunnelQueueMetricsGuard")
            .finish_non_exhaustive()
    }
}

impl Drop for DedicatedTunnelQueueMetricsGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self
            .metrics
            .inner
            .dedicated_tunnel_queue_waiters
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
    }
}

/// 业务作用: 登记活动跨节点聚合任务，完成或失败时自动减少并发 gauge。
// 表示 ClusterAggregateGuard 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct ClusterAggregateGuard {
    metrics: ProxyMetrics,
    started: Instant,
}

impl Drop for ClusterAggregateGuard {
    /// 业务作用: 释放 管理端 HTTP API、运行时状态快照和指标采集 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        let _ = self.metrics.inner.cluster_aggregate_active.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.saturating_sub(1)),
        );
        self.metrics
            .inner
            .cluster_aggregate_operations
            .fetch_add(1, Ordering::Relaxed);
        let elapsed = self.started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
        self.metrics
            .inner
            .cluster_aggregate_elapsed_micros
            .fetch_add(elapsed, Ordering::Relaxed);
        let _ = self
            .metrics
            .inner
            .max_cluster_aggregate_elapsed_micros
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.max(elapsed))
            });
    }
}

/// 业务作用: 冻结单个代理当前连接、吞吐、延迟、错误、背压和资源占用指标，供管理输出一致读取。
#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 ProxyMetricsSnapshot 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
struct ProxyMetricsSnapshot {
    active_connections: usize,
    accepted_connections: u64,
    rejected_connections: u64,
    closed_connections: u64,
    connection_errors: u64,
    pipeline_batches: u64,
    commands: u64,
    /// 代理本地完成的客户端兼容握手命令累计值。
    local_handshake_commands: u64,
    command_errors: u64,
    response_bytes: u64,
    max_pipeline_depth: usize,
    pipeline_execution_errors: u64,
    pipeline_elapsed_micros: u64,
    max_pipeline_elapsed_micros: u64,
    recent_pipeline_batches_per_second: u64,
    recent_commands_per_second: u64,
    pending_pipeline_batches: usize,
    max_pending_pipeline_batches: usize,
    pending_pipeline_commands: usize,
    max_pending_pipeline_commands: usize,
    pending_response_bytes: usize,
    max_pending_response_bytes: usize,
    pending_response_limit_hits: u64,
    relay_client_to_backend_bytes: u64,
    relay_backend_to_client_bytes: u64,
    max_relay_client_to_backend_bytes: u64,
    max_relay_backend_to_client_bytes: u64,
    relay_client_to_backend_backpressure_events: u64,
    relay_backend_to_client_backpressure_events: u64,
    backend_active_connections: usize,
    max_backend_active_connections: usize,
    backend_connection_attempts: u64,
    backend_connection_successes: u64,
    backend_connection_failures: u64,
    backend_auth_failures: u64,
    backend_forward_batches: u64,
    backend_forward_commands: u64,
    backend_forward_errors: u64,
    backend_forward_elapsed_micros: u64,
    max_backend_forward_elapsed_micros: u64,
    dedicated_tunnel_active: usize,
    max_dedicated_tunnel_active: usize,
    dedicated_tunnel_queue_waiters: usize,
    max_dedicated_tunnel_queue_waiters: usize,
    dedicated_tunnel_acquire_attempts: u64,
    dedicated_tunnel_acquire_successes: u64,
    dedicated_tunnel_rejections: u64,
    dedicated_tunnel_acquire_timeouts: u64,
    /// tunnel pool 成功命中累计值。
    tunnel_pool_hits: u64,
    /// tunnel pool 未命中累计值。
    tunnel_pool_misses: u64,
    /// clean 连接成功归池累计值。
    tunnel_pool_returns: u64,
    /// 不可复用连接丢弃累计值。
    tunnel_pool_discards: u64,
    /// 当前全部后端池 idle 连接数。
    tunnel_pool_idle: usize,
    /// 全部后端池最大 idle 容量总和。
    tunnel_pool_max_idle: usize,
    cluster_aggregate_active: usize,
    max_cluster_aggregate_active: usize,
    cluster_aggregate_operations: u64,
    cluster_aggregate_rejections: u64,
    cluster_aggregate_elapsed_micros: u64,
    max_cluster_aggregate_elapsed_micros: u64,
}

/// 业务作用: 保存单代理已生效且在运行代内不变的模式、地址、认证、TLS 和资源限制摘要。
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 ProxyStaticStatus 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
pub struct ProxyStaticStatus {
    /// 代理项唯一名称。
    pub name: String,
    /// 驱动协议、命令和拓扑策略的后端兼容档案。
    pub backend_profile: String,
    /// 对外展示的代理模式名称。
    pub mode: String,
    /// 业务监听器的实际绑定地址。
    pub listen: String,
    /// 单节点模式的后端地址；其他模式为空。
    pub backend: Option<String>,
    /// 集群、节点代理或 replication HA 模式涉及的后端节点地址。
    pub nodes: Vec<String>,
    /// 当前代理项实际采用的认证模式。
    pub auth_mode: String,
    /// 业务数据节点是否启用受证书校验的 RESP TLS。
    pub backend_tls_enabled: bool,
    /// Sentinel 控制连接是否启用受证书校验的 RESP TLS。
    pub sentinel_tls_enabled: bool,
    /// 普通后端 TCP 建连超时，单位为毫秒。
    pub connect_timeout_ms: u64,
    /// 普通后端命令响应超时，单位为毫秒。
    pub backend_request_timeout_ms: u64,
    /// 前端连接完成认证的超时，单位为毫秒。
    pub frontend_auth_timeout_ms: u64,
    /// 客户端连接空闲超时，单位为毫秒；零表示不主动关闭。
    pub client_idle_timeout_ms: u64,
    /// TCP keepalive 周期，单位为毫秒；零表示使用系统默认行为。
    pub tcp_keepalive_ms: u64,
    /// 当前代理项允许的前端并发连接上限。
    pub max_connections: usize,
    /// 连接绑定型命令可占用的专用后端连接上限。
    pub dedicated_tunnel_max_active: usize,
    /// 专用后端连接耗尽后允许等待的请求数量。
    pub dedicated_tunnel_queue_size: usize,
    /// 等待专用后端连接的最长时间，单位为毫秒。
    pub dedicated_tunnel_acquire_timeout_ms: u64,
    /// 每个后端 tunnel pool 允许保留的最大空闲连接数。
    pub dedicated_tunnel_pool_max_idle: usize,
    /// tunnel pool 空闲连接回收阈值，单位为毫秒。
    pub dedicated_tunnel_pool_idle_timeout_ms: u64,
    /// 单批 pipeline 允许包含的命令数量上限。
    pub pipeline_max_commands: usize,
    /// 单批 pipeline 允许占用的输入字节上限。
    pub pipeline_max_bytes: usize,
    /// pipeline 后端池允许同时借出的连接数量。
    pub pipeline_backend_max_active: usize,
    /// pipeline 后端池耗尽后允许排队的任务数量。
    pub pipeline_backend_queue_size: usize,
    /// 向 pipeline 后端池提交任务的超时，单位为毫秒。
    pub pipeline_backend_submit_timeout_ms: u64,
    /// 单个 RESP 帧允许占用的最大字节数。
    pub frame_max_bytes: usize,
    /// 单个 RESP bulk string 允许占用的最大字节数。
    pub bulk_max_bytes: usize,
    /// 跨节点聚合请求允许的并发子任务数量。
    pub aggregate_max_concurrency: usize,
    /// 聚合类命令允许处理的最大元素数量。
    pub aggregate_max_items: usize,
    /// 单连接尚未写回客户端的响应字节上限。
    pub pending_response_bytes: usize,
    /// 进程退出时等待连接和后台任务收敛的时间，单位为毫秒。
    pub graceful_shutdown_timeout_ms: u64,
    /// Redis Cluster 槽位拓扑刷新周期，单位为毫秒。
    pub cluster_topology_refresh_ms: u64,
}

/// 业务作用: 汇总进程最终就绪结果、全部代理状态、排空运行代和最近配置事务。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 AdminStatus 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
struct AdminStatus {
    ready: bool,
    proxy_count: usize,
    proxies: Vec<ProxyStatus>,
    /// 全部仍有连接待排空的旧运行代；仅供指标渲染，状态接口已按代理嵌入。
    #[serde(skip)]
    draining_generations: Vec<ProxyDrainingGenerationStatus>,
    /// 当前配置版本、最近一次热更新结果和累计计数。
    config_reload: Option<ReloadSnapshot>,
}

/// 注册表一次性生成的活动代理和旧运行代排空快照。
struct RuntimeRegistrySnapshot {
    proxies: Vec<ProxyStatus>,
    draining_generations: Vec<ProxyDrainingGenerationStatus>,
}

/// 对外展示的旧运行代排空状态。
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProxyDrainingGenerationStatus {
    proxy: String,
    generation: u64,
    active_connections: usize,
}

/// 业务作用: 组合单代理静态配置、运行代、指标及可选 HA/Sentinel 动态状态。
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
// 表示 ProxyStatus 的数据结构，用于管理健康检查、状态查询、指标和 replication HA 运维接口。
struct ProxyStatus {
    name: String,
    backend_profile: String,
    mode: String,
    listen: String,
    backend: Option<String>,
    nodes: Vec<String>,
    auth_mode: String,
    /// 业务数据节点是否使用 RESP TLS；证书路径和身份材料不进入状态接口。
    backend_tls_enabled: bool,
    /// Sentinel 控制连接是否使用 RESP TLS；证书路径和身份材料不进入状态接口。
    sentinel_tls_enabled: bool,
    connect_timeout_ms: u64,
    backend_request_timeout_ms: u64,
    frontend_auth_timeout_ms: u64,
    client_idle_timeout_ms: u64,
    tcp_keepalive_ms: u64,
    max_connections: usize,
    dedicated_tunnel_max_active: usize,
    dedicated_tunnel_queue_size: usize,
    dedicated_tunnel_acquire_timeout_ms: u64,
    /// 每个真实后端节点 tunnel pool 允许保留的最大 clean idle 连接数。
    dedicated_tunnel_pool_max_idle: usize,
    /// tunnel pool 连接连续空闲多久后关闭，单位为毫秒。
    dedicated_tunnel_pool_idle_timeout_ms: u64,
    pipeline_max_commands: usize,
    pipeline_max_bytes: usize,
    pipeline_backend_max_active: usize,
    pipeline_backend_queue_size: usize,
    pipeline_backend_submit_timeout_ms: u64,
    frame_max_bytes: usize,
    bulk_max_bytes: usize,
    aggregate_max_concurrency: usize,
    aggregate_max_items: usize,
    pending_response_bytes: usize,
    graceful_shutdown_timeout_ms: u64,
    cluster_topology_refresh_ms: u64,
    /// 当前对新连接生效的配置运行代。
    generation: u64,
    /// 同名代理仍有连接待排空的旧运行代。
    draining_generations: Vec<ProxyDrainingGenerationStatus>,
    metrics: ProxyMetricsSnapshot,
    /// 托管复制 HA 动态拓扑和持久化状态；具体产品由 backendProfile 标识。
    ha: Option<ReplicationHaStatus>,
    /// Sentinel 动态路由快照；不包含控制面或数据面认证凭据。
    sentinel: Option<SentinelStatus>,
}

/// replication-ha 运维接口用于选择目标代理项的查询参数。
#[derive(Debug, Deserialize)]
struct ReplicationHaProxyQuery {
    proxy: Option<String>,
}

/// 业务作用: 绑定管理端监听并统一挂载健康、状态、指标、配置事务和 replication-ha 运维接口。
/// 参数说明:
/// - `addr`: 目标网络地址，通常为 `host:port`，用于连接、监听或状态查询。
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `shutdown`: 关闭信号通道，用于让后台任务在进程退出或测试结束时有序停止。
///
/// 返回: 管理监听完成绑定并在关闭信号后正常退出时返回成功；绑定或 HTTP 服务失败时返回错误。
// `/health` 与 `/ready` 必须供探针匿名访问；其余处理器在读取或修改敏感状态前
// 各自调用统一鉴权，新增管理路由时不能把鉴权责任遗漏在挂载层之外。
pub async fn serve(
    addr: SocketAddr,
    state: AdminState,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/status", get(status))
        .route("/metrics", get(metrics))
        .route(
            "/config/reload",
            get(config_reload_status).post(config_reload),
        )
        .route("/config/validate", post(config_validate))
        .route("/ha/topology", get(replication_ha_topology))
        .route("/ha/master", get(replication_ha_master))
        .route("/ha/replicas", get(replication_ha_replicas))
        .route("/ha/failover", post(replication_ha_failover))
        .route("/ha/switchover", post(replication_ha_switchover))
        .route("/ha/node/{node}/promote", post(replication_ha_node_promote))
        .route(
            "/ha/node/{node}/replicaof/{master}",
            post(replication_ha_node_replicaof),
        )
        .route("/ha/node/{node}/fence", post(replication_ha_node_fence))
        .with_state(state);

    let listener = TcpListener::bind(addr).await?;
    info!(listen = %addr, "admin server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            while !*shutdown.borrow() {
                if shutdown.changed().await.is_err() {
                    break;
                }
            }
        })
        .await?;

    Ok(())
}

/// 业务作用: 提供不依赖后端状态的进程存活探针。
/// 参数说明: 无。
async fn health() -> Response {
    Json(json!({"status": "UP"})).into_response()
}

/// 业务作用: 汇总进程、协调配置和动态路由门禁，为 Kubernetes 等探针返回真实业务就绪状态。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
///
async fn ready(State(state): State<AdminState>) -> Response {
    let (ready, reasons) = state.effective_ready().await;
    if ready {
        (StatusCode::OK, Json(json!({"status": "READY"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "NOT_READY", "reasons": reasons})),
        )
            .into_response()
    }
}

/// 业务作用: 鉴权后返回包含配置运行代、数据面与 HA 状态的管理快照。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn status(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    Json(state.snapshot().await).into_response()
}

/// 业务作用: 鉴权后导出进程、代理、Sentinel、replication HA 与配置事务的 Prometheus 指标。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
// 先鉴权再构造完整快照，避免未授权请求利用指标标签枚举后端和代理拓扑。
async fn metrics(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let snapshot = state.snapshot().await;
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        render_metrics(&snapshot),
    )
        .into_response()
}

/// 业务作用: 返回当前配置 revision、摘要和最近一次热更新事务状态。
///
/// 参数说明:
/// - `state`: 共享管理状态，提供热更新请求句柄。
/// - `headers`: HTTP 请求头，用于统一管理端鉴权。
///
/// 返回: 热更新已启用时返回状态快照，否则返回服务不可用。
async fn config_reload_status(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let Some(reload) = &state.reload else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "UNAVAILABLE", "error": "configuration reload is not running"})),
        )
            .into_response();
    };
    Json(reload.snapshot().await).into_response()
}

/// 业务作用: 立即从配置来源加载候选版本并执行完整热更新事务。
///
/// 参数说明:
/// - `state`: 共享管理状态，提供串行热更新请求句柄。
/// - `headers`: HTTP 请求头，用于统一管理端鉴权。
///
/// 返回: 发布成功或无操作时返回状态快照，失败时返回脱敏错误。
async fn config_reload(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let Some(reload) = &state.reload else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "UNAVAILABLE", "error": "configuration reload is not running"})),
        )
            .into_response();
    };
    let coordinator = state
        .coordinator
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(coordinator) = coordinator {
        if !coordinator.cluster_configuration_proposals_ready() {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "UNAVAILABLE",
                    "error": "cluster configuration startup synchronization is not complete",
                    "reload": reload.snapshot().await,
                })),
            )
                .into_response();
        }
        let candidate = match reload.cluster_candidate().await {
            Ok(candidate) => candidate,
            Err(error) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "status": "FAILURE",
                        "error": error,
                        "reload": reload.snapshot().await,
                    })),
                )
                    .into_response();
            }
        };
        match candidate {
            ClusterProposalCandidate::NoOp(response) => {
                return Json(response).into_response();
            }
            ClusterProposalCandidate::Candidate {
                transaction,
                expected_revision,
                transaction_timeout_ms,
                force_revision,
                digest,
                payload,
                transaction_lease: _transaction_lease,
            } => {
                reload.register_cluster_proposal(transaction.clone()).await;
                return match coordinator
                    .propose_cluster_config(
                        expected_revision,
                        Duration::from_millis(transaction_timeout_ms),
                        force_revision,
                        digest,
                        payload,
                    )
                    .await
                {
                    Ok(response) => {
                        reload.set_pending_members(response.pending_members).await;
                        reload.finish_cluster_proposal_success(&transaction).await;
                        Json(json!({
                            "status": "SUCCESS",
                            "clusterRevision": response.revision,
                            "digest": response.digest,
                            "pendingMembers": response.pending_members,
                            "reload": reload.snapshot().await,
                        }))
                        .into_response()
                    }
                    Err(error) => {
                        let error = error.to_string();
                        reload
                            .finish_cluster_failure(&transaction, error.clone())
                            .await;
                        (
                            StatusCode::BAD_REQUEST,
                            Json(json!({
                                "status": "FAILURE",
                                "error": error,
                                "reload": reload.snapshot().await,
                            })),
                        )
                            .into_response()
                    }
                };
            }
        }
    }
    match reload.reload().await {
        Ok(response) => Json(response).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "FAILURE",
                "error": error,
                "reload": reload.snapshot().await,
            })),
        )
            .into_response(),
    }
}

/// 业务作用: 从配置来源加载候选版本并完成运行时预检，但不发布新运行代。
///
/// 参数说明:
/// - `state`: 共享管理状态，提供串行配置校验句柄。
/// - `headers`: HTTP 请求头，用于统一管理端鉴权。
///
/// 返回: 预检成功或无操作时返回状态快照，失败时返回脱敏错误。
async fn config_validate(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let Some(reload) = &state.reload else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "UNAVAILABLE", "error": "configuration reload is not running"})),
        )
            .into_response();
    };
    match reload.validate().await {
        Ok(response) => Json(response).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "status": "FAILURE",
                "error": error,
                "reload": reload.snapshot().await,
            })),
        )
            .into_response(),
    }
}

/// 业务作用: 鉴权并解析目标代理后返回托管复制 HA 的完整拓扑状态。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_topology(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let snapshot = state.snapshot().await;
    let proxies: Vec<_> = snapshot
        .proxies
        .into_iter()
        .filter_map(|proxy| proxy.ha.map(|ha| json!({"name": proxy.name, "ha": ha})))
        .collect();
    Json(json!({"proxies": proxies})).into_response()
}

/// 业务作用: 返回目标 HA 组当前经过安全路由确认的主节点。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_master(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let snapshot = state.snapshot().await;
    let masters: Vec<_> = snapshot
        .proxies
        .into_iter()
        .filter_map(|proxy| {
            proxy.ha.map(|replication_ha| {
                json!({
                    "name": proxy.name,
                    "currentMaster": replication_ha.current_master,
                    "autoFailover": replication_ha.auto_failover,
                    "observer": replication_ha.observer,
                    "masterFailureStreak": replication_ha.master_failure_streak,
                    "failoverCount": replication_ha.failover_count,
                    "lastError": replication_ha.last_error,
                    "routingState": replication_ha.routing_state,
                    "protectionReason": replication_ha.protection_reason,
                    "fencingMode": replication_ha.fencing_mode,
                    "fenceCount": replication_ha.fence_count,
                    "fenceFailureCount": replication_ha.fence_failure_count,
                    "lastFenceElapsedMicros": replication_ha.last_fence_elapsed_micros,
                })
            })
        })
        .collect();
    Json(json!({"masters": masters})).into_response()
}

/// 业务作用: 返回目标 HA 组最近观测到的副本集合及复制健康状态。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_replicas(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let snapshot = state.snapshot().await;
    let replicas: Vec<_> = snapshot
        .proxies
        .into_iter()
        .filter_map(|proxy| proxy.ha.map(|replication_ha| (proxy.name, replication_ha)))
        .flat_map(|(name, replication_ha)| {
            replication_ha
                .nodes
                .into_iter()
                .filter(|node| node.role == "replica")
                .map(move |node| {
                    json!({
                        "name": name,
                        "clientAddr": node.client_addr,
                        "replicationAddr": node.replication_addr,
                        "probeLatencyMicros": node.probe_latency_micros,
                        "replicaLagSeconds": node.replica_lag_seconds,
                    })
                })
        })
        .collect();
    Json(json!({"replicas": replicas})).into_response()
}

/// 业务作用: 鉴权后触发指定候选或自动候选的人工故障切换。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `query`: 管理接口解析后的查询参数，用于选择目标代理和运维对象。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_failover(
    State(state): State<AdminState>,
    Query(query): Query<ReplicationHaProxyQuery>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let (name, runtime) = match state
        .registry
        .replication_ha_runtime_handle(query.proxy.as_deref())
        .await
    {
        Ok(runtime) => runtime,
        Err(err) => return bad_request(err),
    };
    info!(proxy = %name, operation = "failover", "HA admin operation requested");
    match runtime.manual_failover().await {
        Ok(status) => replication_ha_operation_ok(name, "failover", status),
        Err(err) => replication_ha_operation_failed(name, "failover", err.to_string()),
    }
}

/// 业务作用: 鉴权后执行有健康旧主参与的计划切换。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `query`: 管理接口解析后的查询参数，用于选择目标代理和运维对象。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_switchover(
    State(state): State<AdminState>,
    Query(query): Query<ReplicationHaProxyQuery>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let (name, runtime) = match state
        .registry
        .replication_ha_runtime_handle(query.proxy.as_deref())
        .await
    {
        Ok(runtime) => runtime,
        Err(err) => return bad_request(err),
    };
    info!(proxy = %name, operation = "switchover", "HA admin operation requested");
    match runtime.manual_switchover().await {
        Ok(status) => replication_ha_operation_ok(name, "switchover", status),
        Err(err) => replication_ha_operation_failed(name, "switchover", err.to_string()),
    }
}

/// 业务作用: 鉴权后在 HA 权威门禁下提升指定节点。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `query`: 管理接口解析后的查询参数，用于选择目标代理和运维对象。
/// - `node`: URL 路径中的托管节点地址，用于定位要提升的 HA 节点。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_node_promote(
    State(state): State<AdminState>,
    Query(query): Query<ReplicationHaProxyQuery>,
    Path(node): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let (name, runtime) = match state
        .registry
        .replication_ha_runtime_handle(query.proxy.as_deref())
        .await
    {
        Ok(runtime) => runtime,
        Err(err) => return bad_request(err),
    };
    info!(proxy = %name, operation = "promote", node = %node, "HA admin operation requested");
    match runtime.promote_node(&node).await {
        Ok(status) => replication_ha_operation_ok(name, "promote", status),
        Err(err) => replication_ha_operation_failed(name, "promote", err.to_string()),
    }
}

/// 业务作用: 鉴权后把指定节点安全改接到目标主节点。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `query`: 管理接口解析后的查询参数，用于选择目标代理和运维对象。
/// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_node_replicaof(
    State(state): State<AdminState>,
    Query(query): Query<ReplicationHaProxyQuery>,
    Path((node, master)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let (name, runtime) = match state
        .registry
        .replication_ha_runtime_handle(query.proxy.as_deref())
        .await
    {
        Ok(runtime) => runtime,
        Err(err) => return bad_request(err),
    };
    info!(
        proxy = %name,
        operation = "replicaof",
        node = %node,
        master = %master,
        "HA admin operation requested"
    );
    match runtime.replicaof_node(&node, &master).await {
        Ok(status) => replication_ha_operation_ok(name, "replicaof", status),
        Err(err) => replication_ha_operation_failed(name, "replicaof", err.to_string()),
    }
}

/// 业务作用: 鉴权后对指定节点执行配置声明的隔离动作。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `query`: 管理接口解析后的查询参数，用于选择目标代理和运维对象。
/// - `node`: URL 路径中的托管节点地址，用于定位要执行物理或软隔离的 HA 节点。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
async fn replication_ha_node_fence(
    State(state): State<AdminState>,
    Query(query): Query<ReplicationHaProxyQuery>,
    Path(node): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = unauthorized_if_needed(&state, &headers) {
        return response;
    }
    let (name, runtime) = match state
        .registry
        .replication_ha_runtime_handle(query.proxy.as_deref())
        .await
    {
        Ok(runtime) => runtime,
        Err(err) => return bad_request(err),
    };
    info!(proxy = %name, operation = "fence", node = %node, "HA admin operation requested");
    match runtime.fence_node(&node).await {
        Ok(status) => replication_ha_operation_ok(name, "fence", status),
        Err(err) => replication_ha_operation_failed(name, "fence", err.to_string()),
    }
}

/// 业务作用: 把成功的 HA 管理动作统一编码为包含代理名和最新状态的 HTTP 响应。
/// 参数说明:
/// - `proxy`: 代理实例、代理地址或代理配置，用于定位当前业务 listener。
/// - `operation`: 管理接口请求的运维操作名称，用于权限、日志和响应分类。
/// - `replication_ha`: 目标 HA 运行时句柄，用于查询拓扑或执行受控运维操作。
///
fn replication_ha_operation_ok(
    proxy: String,
    operation: &'static str,
    replication_ha: ReplicationHaStatus,
) -> Response {
    Json(json!({
        "status": "OK",
        "operation": operation,
        "name": proxy,
        "ha": replication_ha,
    }))
    .into_response()
}

/// 业务作用: 把请求参数或运行时选择错误统一转换为 HTTP 400 JSON 响应。
/// 参数说明:
/// - `message`: 返回给调用方或写入状态的业务消息，不应包含认证凭据。
///
fn bad_request(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"status": "ERROR", "msg": message.into()})),
    )
        .into_response()
}

/// 业务作用: 把 HA 动作失败映射为安全 HTTP 状态与脱敏错误正文。
/// 参数说明:
/// - `proxy`: 代理实例、代理地址或代理配置，用于定位当前业务 listener。
/// - `operation`: 管理接口请求的运维操作名称，用于权限、日志和响应分类。
/// - `message`: 返回给调用方或写入状态的业务消息，不应包含认证凭据。
///
fn replication_ha_operation_failed(
    proxy: String,
    operation: &'static str,
    message: String,
) -> Response {
    warn!(
        proxy = %proxy,
        operation = %operation,
        error = %message,
        "HA admin operation failed"
    );
    bad_request(message)
}

/// 业务作用: 把管理端状态快照渲染成指标文本，供监控系统按标签采集代理和 HA 状态。
/// 参数说明:
/// - `snapshot`: 当前运行时状态快照，用于生成管理响应和指标而不长期持锁。
///
// 格式化 render_metrics 输出，用于对外呈现管理健康检查、状态查询、指标和 replication HA 运维接口结果。
fn render_metrics(snapshot: &AdminStatus) -> String {
    let mut out = String::new();
    append_process_metrics(&mut out, &process_metrics_snapshot());
    out.push_str("# HELP narix_ready Whether the narix process is ready.\n");
    out.push_str("# TYPE narix_ready gauge\n");
    out.push_str(&format!(
        "narix_ready {}\n",
        if snapshot.ready { 1 } else { 0 }
    ));
    out.push_str("# HELP narix_proxies Number of configured proxy listeners.\n");
    out.push_str("# TYPE narix_proxies gauge\n");
    out.push_str(&format!("narix_proxies {}\n", snapshot.proxy_count));
    if let Some(reload) = &snapshot.config_reload {
        out.push_str(
            "# HELP narix_config_reload_total Configuration validation and reload requests.\n",
        );
        out.push_str("# TYPE narix_config_reload_total counter\n");
        let _ = writeln!(out, "narix_config_reload_total {}", reload.reload_total);
        out.push_str(
            "# HELP narix_config_reload_failures_total Failed configuration validation and reload requests.\n",
        );
        out.push_str("# TYPE narix_config_reload_failures_total counter\n");
        let _ = writeln!(
            out,
            "narix_config_reload_failures_total {}",
            reload.reload_failures_total
        );
        if !reload.reload_failures_by_trigger.is_empty() {
            out.push_str(
                "# HELP narix_config_reload_failures_by_trigger_total Failed configuration requests split by trigger source.\n",
            );
            out.push_str("# TYPE narix_config_reload_failures_by_trigger_total counter\n");
            for (trigger, count) in &reload.reload_failures_by_trigger {
                let _ = writeln!(
                    out,
                    "narix_config_reload_failures_by_trigger_total{{trigger=\"{trigger}\"}} {count}"
                );
            }
        }
        out.push_str(
            "# HELP narix_config_reload_duration_seconds Duration of the most recent configuration request.\n",
        );
        out.push_str("# TYPE narix_config_reload_duration_seconds gauge\n");
        let _ = writeln!(
            out,
            "narix_config_reload_duration_seconds {}",
            reload.last_duration_seconds
        );
        out.push_str("# HELP narix_config_revision Current committed configuration revision.\n");
        out.push_str("# TYPE narix_config_revision gauge\n");
        let _ = writeln!(out, "narix_config_revision {}", reload.config_revision);
        out.push_str(
            "# HELP narix_config_pending_members Static members not yet running the committed revision when the latest configuration transaction responded.\n",
        );
        out.push_str("# TYPE narix_config_pending_members gauge\n");
        let _ = writeln!(
            out,
            "narix_config_pending_members {}",
            reload.pending_members
        );
    }
    out.push_str(
        "# HELP narix_parsed_commands_total Parsed frontend Redis commands processed by Narix; later opaque tunnel bytes are excluded.\n",
    );
    out.push_str("# TYPE narix_parsed_commands_total counter\n");
    out.push_str(
        "# HELP narix_local_handshake_commands_total Compatibility handshake commands answered locally by Narix.\n",
    );
    out.push_str("# TYPE narix_local_handshake_commands_total counter\n");
    out.push_str(
        "# HELP narix_tunnel_pool_hits_total Clean idle backend tunnel leases acquired from a backend-specific pool.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_hits_total counter\n");
    out.push_str(
        "# HELP narix_tunnel_pool_misses_total Backend-specific tunnel pool misses that require a new backend connection.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_misses_total counter\n");
    out.push_str(
        "# HELP narix_tunnel_pool_returns_total Clean exclusive tunnel connections returned to their original backend pool.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_returns_total counter\n");
    out.push_str(
        "# HELP narix_tunnel_pool_discards_total Tainted, expired, disabled or over-capacity tunnel connections not reused.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_discards_total counter\n");
    out.push_str(
        "# HELP narix_tunnel_pool_idle Current clean idle tunnel connections aggregated across this proxy's backend-specific pools.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_idle gauge\n");
    out.push_str(
        "# HELP narix_tunnel_pool_max_idle Configured idle capacity summed across this proxy's backend-specific pools.\n",
    );
    out.push_str("# TYPE narix_tunnel_pool_max_idle gauge\n");
    out.push_str(
        "# HELP narix_sentinel_refresh_total Sentinel discovery and candidate-role verification attempts.\n",
    );
    out.push_str("# TYPE narix_sentinel_refresh_total counter\n");
    out.push_str(
        "# HELP narix_sentinel_master_change_total Committed Sentinel master address changes.\n",
    );
    out.push_str("# TYPE narix_sentinel_master_change_total counter\n");
    out.push_str(
        "# HELP narix_sentinel_route_generation Current committed Sentinel route generation.\n",
    );
    out.push_str("# TYPE narix_sentinel_route_generation gauge\n");
    out.push_str(
        "# HELP narix_sentinel_last_success_unixtime Unix time of the last successful Sentinel refresh.\n",
    );
    out.push_str("# TYPE narix_sentinel_last_success_unixtime gauge\n");
    out.push_str(
        "# HELP narix_sentinel_route_ready Whether a verified Sentinel master route is serving.\n",
    );
    out.push_str("# TYPE narix_sentinel_route_ready gauge\n");
    out.push_str(
        "# HELP narix_sentinel_current_master Current verified Sentinel master route identity.\n",
    );
    out.push_str("# TYPE narix_sentinel_current_master gauge\n");
    out.push_str(
        "# HELP narix_proxy_generation Current configuration generation serving new connections.\n",
    );
    out.push_str("# TYPE narix_proxy_generation gauge\n");
    out.push_str(
        "# HELP narix_proxy_draining_connections Frontend connections still attached to a retired configuration generation.\n",
    );
    out.push_str("# TYPE narix_proxy_draining_connections gauge\n");

    for proxy in &snapshot.proxies {
        let _ = writeln!(
            out,
            "narix_proxy_generation{{proxy=\"{}\"}} {}",
            escape_label(&proxy.name),
            proxy.generation
        );
        out.push_str(&format!(
            "narix_listener_info{{proxy=\"{}\",mode=\"{}\",listen=\"{}\"}} 1\n",
            escape_label(&proxy.name),
            escape_label(&proxy.mode),
            escape_label(&proxy.listen)
        ));
        out.push_str(&format!(
            "narix_active_connections{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.active_connections
        ));
        out.push_str(&format!(
            "narix_accepted_connections_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.accepted_connections
        ));
        out.push_str(&format!(
            "narix_rejected_connections_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.rejected_connections
        ));
        out.push_str(&format!(
            "narix_closed_connections_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.closed_connections
        ));
        out.push_str(&format!(
            "narix_connection_errors_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.connection_errors
        ));
        out.push_str(&format!(
            "narix_pipeline_batches_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pipeline_batches
        ));
        out.push_str(&format!(
            "narix_commands_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.commands
        ));
        out.push_str(&format!(
            "narix_parsed_commands_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.commands
        ));
        out.push_str(&format!(
            "narix_local_handshake_commands_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.local_handshake_commands
        ));
        out.push_str(&format!(
            "narix_command_errors_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.command_errors
        ));
        out.push_str(&format!(
            "narix_response_bytes_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.response_bytes
        ));
        out.push_str(&format!(
            "narix_max_pipeline_depth{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_pipeline_depth
        ));
        out.push_str(&format!(
            "narix_pipeline_execution_errors_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pipeline_execution_errors
        ));
        out.push_str(&format!(
            "narix_pipeline_elapsed_micros_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pipeline_elapsed_micros
        ));
        out.push_str(&format!(
            "narix_max_pipeline_elapsed_micros{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_pipeline_elapsed_micros
        ));
        out.push_str(&format!(
            "narix_recent_pipeline_batches_per_second{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.recent_pipeline_batches_per_second
        ));
        out.push_str(&format!(
            "narix_recent_commands_per_second{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.recent_commands_per_second
        ));
        out.push_str(&format!(
            "narix_pending_pipeline_batches{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pending_pipeline_batches
        ));
        out.push_str(&format!(
            "narix_max_pending_pipeline_batches{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_pending_pipeline_batches
        ));
        out.push_str(&format!(
            "narix_pending_pipeline_commands{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pending_pipeline_commands
        ));
        out.push_str(&format!(
            "narix_max_pending_pipeline_commands{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_pending_pipeline_commands
        ));
        out.push_str(&format!(
            "narix_pending_response_bytes{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pending_response_bytes
        ));
        out.push_str(&format!(
            "narix_max_pending_response_bytes{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_pending_response_bytes
        ));
        out.push_str(&format!(
            "narix_pending_response_limit_hits_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.pending_response_limit_hits
        ));
        out.push_str(&format!(
            "narix_relay_client_to_backend_bytes_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.relay_client_to_backend_bytes
        ));
        out.push_str(&format!(
            "narix_relay_backend_to_client_bytes_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.relay_backend_to_client_bytes
        ));
        out.push_str(&format!(
            "narix_max_relay_client_to_backend_bytes{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_relay_client_to_backend_bytes
        ));
        out.push_str(&format!(
            "narix_max_relay_backend_to_client_bytes{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_relay_backend_to_client_bytes
        ));
        out.push_str(&format!(
            "narix_relay_client_to_backend_backpressure_events_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.relay_client_to_backend_backpressure_events
        ));
        out.push_str(&format!(
            "narix_relay_backend_to_client_backpressure_events_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.relay_backend_to_client_backpressure_events
        ));
        out.push_str(&format!(
            "narix_backend_active_connections{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_active_connections
        ));
        out.push_str(&format!(
            "narix_max_backend_active_connections{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_backend_active_connections
        ));
        out.push_str(&format!(
            "narix_backend_connection_attempts_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_connection_attempts
        ));
        out.push_str(&format!(
            "narix_backend_connection_successes_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_connection_successes
        ));
        out.push_str(&format!(
            "narix_backend_connection_failures_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_connection_failures
        ));
        out.push_str(&format!(
            "narix_backend_auth_failures_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_auth_failures
        ));
        out.push_str(&format!(
            "narix_backend_forward_batches_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_forward_batches
        ));
        out.push_str(&format!(
            "narix_backend_forward_commands_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_forward_commands
        ));
        out.push_str(&format!(
            "narix_backend_forward_errors_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_forward_errors
        ));
        out.push_str(&format!(
            "narix_backend_forward_elapsed_micros_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.backend_forward_elapsed_micros
        ));
        out.push_str(&format!(
            "narix_max_backend_forward_elapsed_micros{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_backend_forward_elapsed_micros
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_active{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_active
        ));
        out.push_str(&format!(
            "narix_max_dedicated_tunnel_active{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_dedicated_tunnel_active
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_queue_waiters{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_queue_waiters
        ));
        out.push_str(&format!(
            "narix_max_dedicated_tunnel_queue_waiters{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_dedicated_tunnel_queue_waiters
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_acquire_attempts_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_acquire_attempts
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_acquire_successes_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_acquire_successes
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_rejections_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_rejections
        ));
        out.push_str(&format!(
            "narix_dedicated_tunnel_acquire_timeouts_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.dedicated_tunnel_acquire_timeouts
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_hits_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_hits
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_misses_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_misses
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_returns_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_returns
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_discards_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_discards
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_idle{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_idle
        ));
        out.push_str(&format!(
            "narix_tunnel_pool_max_idle{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.tunnel_pool_max_idle
        ));
        out.push_str(&format!(
            "narix_cluster_aggregate_active{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.cluster_aggregate_active
        ));
        out.push_str(&format!(
            "narix_max_cluster_aggregate_active{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_cluster_aggregate_active
        ));
        out.push_str(&format!(
            "narix_cluster_aggregate_operations_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.cluster_aggregate_operations
        ));
        out.push_str(&format!(
            "narix_cluster_aggregate_rejections_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.cluster_aggregate_rejections
        ));
        out.push_str(&format!(
            "narix_cluster_aggregate_elapsed_micros_total{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.cluster_aggregate_elapsed_micros
        ));
        out.push_str(&format!(
            "narix_max_cluster_aggregate_elapsed_micros{{proxy=\"{}\"}} {}\n",
            escape_label(&proxy.name),
            proxy.metrics.max_cluster_aggregate_elapsed_micros
        ));
        if let Some(sentinel) = &proxy.sentinel {
            out.push_str(&format!(
                "narix_sentinel_refresh_total{{proxy=\"{}\",result=\"success\"}} {}\n",
                escape_label(&proxy.name),
                sentinel.refresh_success_total
            ));
            out.push_str(&format!(
                "narix_sentinel_refresh_total{{proxy=\"{}\",result=\"failure\"}} {}\n",
                escape_label(&proxy.name),
                sentinel.refresh_failure_total
            ));
            out.push_str(&format!(
                "narix_sentinel_master_change_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                sentinel.master_change_total
            ));
            out.push_str(&format!(
                "narix_sentinel_route_generation{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                sentinel.generation
            ));
            out.push_str(&format!(
                "narix_sentinel_last_success_unixtime{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                sentinel.last_refresh_unix_ms / 1_000
            ));
            out.push_str(&format!(
                "narix_sentinel_route_ready{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                if sentinel.routing_state == "serving" {
                    1
                } else {
                    0
                }
            ));
            if let Some(master) = &sentinel.current_master {
                out.push_str(&format!(
                    "narix_sentinel_current_master{{proxy=\"{}\",master=\"{}\"}} 1\n",
                    escape_label(&proxy.name),
                    escape_label(master)
                ));
            }
        }
        if let Some(replication_ha) = &proxy.ha {
            out.push_str(&format!(
                "narix_replication_ha_current_master{{proxy=\"{}\",master=\"{}\"}} 1\n",
                escape_label(&proxy.name),
                escape_label(&replication_ha.current_master)
            ));
            out.push_str(&format!(
                "narix_replication_ha_master_failure_streak{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.master_failure_streak
            ));
            out.push_str(&format!(
                "narix_replication_ha_failovers_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.failover_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_last_failover_elapsed_micros{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.last_failover_elapsed_micros
            ));
            out.push_str(&format!(
                "narix_replication_ha_fences_total{{proxy=\"{}\",mode=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                escape_label(&replication_ha.fencing_mode),
                replication_ha.fence_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_fence_failures_total{{proxy=\"{}\",mode=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                escape_label(&replication_ha.fencing_mode),
                replication_ha.fence_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_last_fence_elapsed_micros{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.last_fence_elapsed_micros
            ));
            out.push_str(&format!(
                "narix_replication_ha_replica_repairs_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.replica_repair_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_replica_repair_failures_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.replica_repair_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_last_replica_repair_elapsed_micros{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.last_replica_repair_elapsed_micros
            ));
            out.push_str(&format!(
                "narix_replication_ha_lease_acquires_total{{proxy=\"{}\",mode=\"{}\",owner=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                escape_label(&replication_ha.coordinator_mode),
                escape_label(&replication_ha.coordinator_owner_id),
                replication_ha.lease_acquire_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_lease_acquire_failures_total{{proxy=\"{}\",mode=\"{}\",owner=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                escape_label(&replication_ha.coordinator_mode),
                escape_label(&replication_ha.coordinator_owner_id),
                replication_ha.lease_acquire_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_last_lease_acquire_elapsed_micros{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                replication_ha.last_lease_acquire_elapsed_micros
            ));
            let persistence = &replication_ha.persistence;
            for owner in &persistence.snapshot_owners {
                out.push_str(&format!(
                    "narix_replication_ha_persistence_owner{{proxy=\"{}\",node=\"{}\",role=\"{}\"}} 1\n",
                    escape_label(&proxy.name),
                    escape_label(&owner.node),
                    escape_label(&owner.role)
                ));
                out.push_str(&format!(
                    "narix_replication_ha_persistence_owner_verified{{proxy=\"{}\",node=\"{}\"}} {}\n",
                    escape_label(&proxy.name),
                    escape_label(&owner.node),
                    if owner.verified { 1 } else { 0 }
                ));
            }
            out.push_str(&format!(
                "narix_replication_ha_persistence_handoffs_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                persistence.handoff_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_adopt_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                persistence.adopt_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_enable_total{{proxy=\"{}\",result=\"success\"}} {}\n",
                escape_label(&proxy.name),
                persistence.enable_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_enable_total{{proxy=\"{}\",result=\"failure\"}} {}\n",
                escape_label(&proxy.name),
                persistence.enable_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_disable_total{{proxy=\"{}\",result=\"success\"}} {}\n",
                escape_label(&proxy.name),
                persistence.disable_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_disable_total{{proxy=\"{}\",result=\"failure\"}} {}\n",
                escape_label(&proxy.name),
                persistence.disable_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_snapshot_total{{proxy=\"{}\",result=\"success\"}} {}\n",
                escape_label(&proxy.name),
                persistence.snapshot_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_snapshot_total{{proxy=\"{}\",result=\"failure\"}} {}\n",
                escape_label(&proxy.name),
                persistence.snapshot_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_verify_total{{proxy=\"{}\",result=\"success\"}} {}\n",
                escape_label(&proxy.name),
                persistence.verify_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_verify_total{{proxy=\"{}\",result=\"failure\"}} {}\n",
                escape_label(&proxy.name),
                persistence.verify_failure_count
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_verify_duration_micros{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                persistence.last_verify_duration_micros
            ));
            out.push_str(&format!(
                "narix_replication_ha_persistence_protection_total{{proxy=\"{}\"}} {}\n",
                escape_label(&proxy.name),
                persistence.protection_count
            ));
            for node in &replication_ha.nodes {
                out.push_str(&format!(
                    "narix_replication_ha_node_role{{proxy=\"{}\",node=\"{}\",role=\"{}\"}} 1\n",
                    escape_label(&proxy.name),
                    escape_label(&node.client_addr),
                    escape_label(&node.role)
                ));
                out.push_str(&format!(
                    "narix_replication_ha_node_probe_latency_micros{{proxy=\"{}\",node=\"{}\"}} {}\n",
                    escape_label(&proxy.name),
                    escape_label(&node.client_addr),
                    node.probe_latency_micros
                ));
                if let Some(replica_lag_seconds) = node.replica_lag_seconds {
                    out.push_str(&format!(
                        "narix_replication_ha_replica_lag_seconds{{proxy=\"{}\",node=\"{}\"}} {}\n",
                        escape_label(&proxy.name),
                        escape_label(&node.client_addr),
                        replica_lag_seconds
                    ));
                }
            }
            if replication_ha.last_error.is_some() {
                out.push_str(&format!(
                    "narix_replication_ha_last_error{{proxy=\"{}\"}} 1\n",
                    escape_label(&proxy.name)
                ));
            }
        }
    }
    for draining in &snapshot.draining_generations {
        let _ = writeln!(
            out,
            "narix_proxy_draining_connections{{proxy=\"{}\",generation=\"{}\"}} {}",
            escape_label(&draining.proxy),
            draining.generation,
            draining.active_connections
        );
    }
    out
}

/// 业务作用: 保存一次管理端抓取时的 Narix 进程资源快照，供 Prometheus 按节点观察 CPU、内存、线程和文件描述符。
///
/// 设计原因: 这些指标只在 `/metrics` 请求到达时读取，不在代理数据面增加计数、系统调用或锁竞争。
#[derive(Debug, Default)]
struct ProcessMetricsSnapshot {
    cpu_user_seconds: f64,
    cpu_system_seconds: f64,
    resident_memory_bytes: Option<u64>,
    virtual_memory_bytes: Option<u64>,
    threads: Option<u64>,
    open_fds: Option<u64>,
    max_fds: Option<u64>,
}

/// 业务作用: 读取当前 Narix 进程的累计 CPU 时间和 Linux `/proc` 资源占用。
///
/// 返回: CPU 时间在所有支持平台上通过 `getrusage` 提供；Linux 部署额外返回当前 RSS、虚拟内存、线程和 FD。
fn process_metrics_snapshot() -> ProcessMetricsSnapshot {
    let mut snapshot = ProcessMetricsSnapshot::default();
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();

    // SAFETY: `usage` 指向一块足以容纳 `libc::rusage` 的可写内存；返回 0 时内核已完整初始化该结构。
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
        // SAFETY: 仅在 `getrusage` 返回成功后读取已初始化的数据。
        let usage = unsafe { usage.assume_init() };
        snapshot.cpu_user_seconds = timeval_seconds(usage.ru_utime);
        snapshot.cpu_system_seconds = timeval_seconds(usage.ru_stime);
    }

    populate_platform_process_metrics(&mut snapshot);
    snapshot
}

/// 业务作用: 把 `timeval` 转为 Prometheus counter 使用的秒数。
/// 参数说明:
/// - `value`: `getrusage` 返回的秒和微秒组合值。
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}

/// 业务作用: 在 Linux 生产环境读取 `/proc` 中的当前进程资源，不执行外部命令。
/// 参数说明:
/// - `snapshot`: 接收 Linux 当前进程内存、线程和文件描述符指标的快照。
#[cfg(target_os = "linux")]
fn populate_platform_process_metrics(snapshot: &mut ProcessMetricsSnapshot) {
    if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
        let mut fields = statm.split_whitespace();
        let virtual_pages = fields.next().and_then(|raw| raw.parse::<u64>().ok());
        let resident_pages = fields.next().and_then(|raw| raw.parse::<u64>().ok());
        // SAFETY: `_SC_PAGESIZE` 是无副作用的进程级系统配置查询。
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if let Ok(page_size) = u64::try_from(page_size) {
            snapshot.virtual_memory_bytes =
                virtual_pages.and_then(|pages| pages.checked_mul(page_size));
            snapshot.resident_memory_bytes =
                resident_pages.and_then(|pages| pages.checked_mul(page_size));
        }
    }

    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        snapshot.threads = status.lines().find_map(|line| {
            line.strip_prefix("Threads:")
                .and_then(|raw| raw.trim().parse::<u64>().ok())
        });
    }

    snapshot.open_fds = std::fs::read_dir("/proc/self/fd")
        .ok()
        .and_then(|entries| u64::try_from(entries.filter_map(Result::ok).count()).ok());

    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
    // SAFETY: `limit` 是有效的可写 `rlimit` 缓冲区，成功返回后结构已由内核初始化。
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0 {
        // SAFETY: 仅在 `getrlimit` 成功后读取已初始化的数据。
        snapshot.max_fds = Some(unsafe { limit.assume_init() }.rlim_cur);
    }
}

/// 业务作用: 在 macOS 本地开发环境通过 Mach task API 读取与 Linux 面板语义一致的当前 RSS 和虚拟内存。
/// 参数说明:
/// - `snapshot`: 接收 macOS 当前进程内存和文件描述符指标的快照。
#[cfg(target_os = "macos")]
fn populate_platform_process_metrics(snapshot: &mut ProcessMetricsSnapshot) {
    let mut task_info = std::mem::MaybeUninit::<libc::mach_task_basic_info>::zeroed();
    let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
    #[allow(deprecated)]
    // SAFETY: `mach_task_self` 只读取当前进程预置的 Mach task port。
    let task = unsafe { libc::mach_task_self() };
    // SAFETY: 输出缓冲区和 count 与 `MACH_TASK_BASIC_INFO` flavor 一致；成功返回后结构完整初始化。
    let status = unsafe {
        libc::task_info(
            task,
            libc::MACH_TASK_BASIC_INFO,
            task_info.as_mut_ptr().cast(),
            &mut count,
        )
    };
    if status == libc::KERN_SUCCESS {
        // SAFETY: `task_info` 已成功写入完整 `mach_task_basic_info` 结构。
        let task_info = unsafe { task_info.assume_init() };
        snapshot.resident_memory_bytes = Some(task_info.resident_size);
        snapshot.virtual_memory_bytes = Some(task_info.virtual_size);
    }

    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::zeroed();
    // SAFETY: `limit` 是有效的可写 `rlimit` 缓冲区，成功返回后结构已由内核初始化。
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0 {
        // SAFETY: 仅在 `getrlimit` 成功后读取已初始化的数据。
        snapshot.max_fds = Some(unsafe { limit.assume_init() }.rlim_cur);
    }
}

/// 业务作用: 其他开发平台保留 CPU 指标，同时明确不伪造没有可靠来源的当前资源值。
/// 参数说明:
/// - `_snapshot`: 保留跨平台统一签名的进程资源快照，当前平台不写入额外字段。
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn populate_platform_process_metrics(_snapshot: &mut ProcessMetricsSnapshot) {}

/// 业务作用: 将进程资源快照编码为 Prometheus text exposition 格式。
/// 参数说明:
/// - `out`: 接收 Prometheus 指标文本的输出缓冲区。
/// - `snapshot`: 本次采集得到的 CPU、内存、线程和文件描述符快照。
fn append_process_metrics(out: &mut String, snapshot: &ProcessMetricsSnapshot) {
    out.push_str(
        "# HELP narix_process_cpu_seconds_total Total user and system CPU time spent by the Narix process.\n",
    );
    out.push_str("# TYPE narix_process_cpu_seconds_total counter\n");
    let _ = writeln!(
        out,
        "narix_process_cpu_seconds_total{{mode=\"user\"}} {}",
        snapshot.cpu_user_seconds
    );
    let _ = writeln!(
        out,
        "narix_process_cpu_seconds_total{{mode=\"system\"}} {}",
        snapshot.cpu_system_seconds
    );

    out.push_str(
        "# HELP narix_process_resident_memory_bytes Current resident memory used by the Narix process.\n",
    );
    out.push_str("# TYPE narix_process_resident_memory_bytes gauge\n");
    if let Some(value) = snapshot.resident_memory_bytes {
        let _ = writeln!(out, "narix_process_resident_memory_bytes {value}");
    }

    out.push_str(
        "# HELP narix_process_virtual_memory_bytes Current virtual memory used by the Narix process.\n",
    );
    out.push_str("# TYPE narix_process_virtual_memory_bytes gauge\n");
    if let Some(value) = snapshot.virtual_memory_bytes {
        let _ = writeln!(out, "narix_process_virtual_memory_bytes {value}");
    }

    out.push_str("# HELP narix_process_threads Number of Narix process threads.\n");
    out.push_str("# TYPE narix_process_threads gauge\n");
    if let Some(value) = snapshot.threads {
        let _ = writeln!(out, "narix_process_threads {value}");
    }

    out.push_str("# HELP narix_process_open_fds Number of open file descriptors.\n");
    out.push_str("# TYPE narix_process_open_fds gauge\n");
    if let Some(value) = snapshot.open_fds {
        let _ = writeln!(out, "narix_process_open_fds {value}");
    }

    out.push_str("# HELP narix_process_max_fds Maximum number of open file descriptors.\n");
    out.push_str("# TYPE narix_process_max_fds gauge\n");
    if let Some(value) = snapshot.max_fds {
        let _ = writeln!(out, "narix_process_max_fds {value}");
    }
}

/// 业务作用: 转义 管理端 HTTP API、运行时状态快照和指标采集 的输出字段，避免标签或协议文本破坏下游解析。
/// 参数说明:
/// - `raw`: 尚未结构化的 RESP 或后端字节数据，解析前必须保留原始顺序。
///
// 格式化 escape_label 输出，用于对外呈现管理健康检查、状态查询、指标和 replication HA 运维接口结果。
fn escape_label(raw: &str) -> String {
    raw.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

/// 业务作用: 对受保护管理端点执行统一认证，并在失败时生成 401 响应。
/// 参数说明:
/// - `state`: 共享运行时状态快照或状态容器，用于跨任务读取和更新业务状态。
/// - `headers`: HTTP 请求头集合，用于管理端鉴权和请求上下文判断。
///
fn unauthorized_if_needed(state: &AdminState, headers: &HeaderMap) -> Option<Response> {
    if state.authorize(headers) {
        None
    } else {
        Some(
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"status": "UNAUTHORIZED"})),
            )
                .into_response(),
        )
    }
}

/// 业务作用: 以不依赖首个差异位置的循环比较令牌字节，降低认证时序侧信道。
/// 参数说明:
/// - `left`: 比较操作左侧的实际或候选值，与右侧共同完成一致性判断。
/// - `right`: 比较操作右侧的期望或参考值，与左侧共同完成一致性判断。
///
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    // 循环次数取两侧最大长度，长度差也合并进 diff，避免长度或首个差异位置形成明显的提前返回信号。
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for idx in 0..max_len {
        let a = left.get(idx).copied().unwrap_or_default();
        let b = right.get(idx).copied().unwrap_or_default();
        diff |= usize::from(a ^ b);
    }
    diff == 0
}

/// 业务作用: 生成管理状态和指标使用的当前 Unix 秒时间戳。
/// 参数说明: 无。
// 系统时间异常早于 Unix epoch 时返回零，管理面不能因时间转换错误而 panic。
fn current_unix_second() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}
