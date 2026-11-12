use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::admin::{AdminAuth, AdminState, RuntimeRegistry};
use crate::config::{listen_addresses_conflict, ProxyConfig, ProxyItem, ProxyMode};
use crate::coordinator::runtime::CoordinatorHandle;
use crate::ha::model::parse_nodes as parse_ha_nodes;
use crate::infrastructure::lifecycle::{AdmissionGate, DomainTask};
use crate::proxy;

use super::{
    ApplyConfigRequest, ApplyMode, ApplyOrigin, ConfigDiff, LoadedConfig, MAX_CLUSTER_CONFIG_BYTES,
};

/// 一个正在运行或等待排空的代理配置代次。
struct ManagedProxy {
    item: ProxyItem,
    generation: u64,
    config_generation: u64,
    cluster_config_digest: String,
    controller: Option<proxy::ProxyGenerationController>,
    activation: watch::Sender<bool>,
    ready: Option<oneshot::Receiver<()>>,
    activated: Option<oneshot::Receiver<()>>,
    listener_closed: Option<oneshot::Receiver<()>>,
    shutdown: watch::Sender<bool>,
    join: DomainTask,
}

/// 代理任务退出后发送给运行时 supervisor 的结果。
struct ProxyExit {
    name: String,
    generation: u64,
    result: Result<(), String>,
}

/// 持有全部代理任务，并串行执行新增、修改、删除和失败回滚。
pub struct RuntimeSupervisor;

/// 运行时 supervisor 启动后返回的动态就绪地址订阅和后台任务。
type RuntimeSupervisorHandle = (
    watch::Receiver<Vec<SocketAddr>>,
    oneshot::Receiver<()>,
    JoinHandle<anyhow::Result<()>>,
);

/// 在多代理发布窗口内关闭新连接入口，并且只在明确成功或完整回滚后重新开放。
struct PublicationPause {
    gate: AdmissionGate,
    reopened: bool,
}

impl PublicationPause {
    /// 业务作用: 关闭进程内全部代理监听器的新连接入口。
    ///
    /// 参数说明:
    /// - `gate`: 由运行时 supervisor 与进程停止共同控制、全部监听器订阅的准入门禁。
    ///
    /// 返回: 停止先于发布时拒绝；成功时返回必须在发布或完整回滚后显式重新开放的守卫。
    fn begin(gate: &AdmissionGate) -> Result<Self, String> {
        // 停止和发布共用门禁决定先后，防止异步准备完成后安装一个已经失去发布资格的候选。
        if !gate.begin_publication() {
            return Err(
                "application is stopping; candidate publication is not allowed".to_string(),
            );
        }
        Ok(Self {
            gate: gate.clone(),
            reopened: false,
        })
    }

    /// 业务作用: 在全部代理已经处于同一配置版本后重新开放新连接。
    fn reopen(mut self) {
        self.gate.send_replace(true);
        self.reopened = true;
    }
}

impl Drop for PublicationPause {
    /// 业务作用: 发布过程异常离开且没有完成回滚时保持门禁关闭，禁止暴露混合配置。
    fn drop(&mut self) {
        if !self.reopened {
            warn!("proxy publication gate remains closed because rollback was incomplete");
        }
    }
}

impl RuntimeSupervisor {
    /// 业务作用: 启动首代代理任务和运行时配置事务循环。
    ///
    /// 参数说明:
    /// - `initial`: 启动时已通过配置加载和静态校验的活动版本。
    /// - `admin_state`: 管理面认证策略和代理运行时注册表。
    /// - `coordinator`: 可选的进程级 HA 协调器。
    /// - `apply_rx`: 配置 supervisor 发送的串行预检或发布请求。
    /// - `shutdown`: 进程级关闭信号。
    /// - `admission`: Application 和配置事务共同约束的准入门禁。
    ///
    /// 返回: 最新监听地址订阅、首代完成绑定和激活的通知，以及受监督的后台任务。
    pub fn spawn(
        initial: LoadedConfig,
        admin_state: AdminState,
        coordinator: Option<Arc<CoordinatorHandle>>,
        apply_rx: mpsc::Receiver<ApplyConfigRequest>,
        shutdown: watch::Receiver<bool>,
        admission: AdmissionGate,
    ) -> anyhow::Result<RuntimeSupervisorHandle> {
        // 协调节点必须先从集群日志恢复权威版本，不能让本地旧文件提前创建数据面并以旧摘要
        // 注册 HA 组。单节点仍立即启动首代代理，保持原有启动路径。
        let awaiting_cluster_bootstrap = coordinator.is_some();
        let readiness = if awaiting_cluster_bootstrap {
            Vec::new()
        } else {
            readiness_addresses(initial.config().as_ref())?
        };
        let (readiness_tx, readiness_rx) = watch::channel(readiness);
        let (startup_tx, startup_rx) = oneshot::channel();
        let registry = admin_state.registry();
        let (exit_tx, exit_rx) = mpsc::unbounded_channel();
        let publication_tx = admission;
        let publication_rx = publication_tx.subscribe();
        let mut proxies = HashMap::new();
        let mut next_generation = 1_u64;
        if !awaiting_cluster_bootstrap {
            let initial_cluster_digest = initial.cluster_digest().to_string();
            for item in initial.config().proxies.clone() {
                let managed = spawn_proxy(
                    item.clone(),
                    next_generation,
                    initial_cluster_digest.clone(),
                    registry.clone(),
                    coordinator.clone(),
                    exit_tx.clone(),
                    publication_rx.clone(),
                    true,
                );
                proxies.insert(item.name.clone(), managed);
                next_generation = next_generation.saturating_add(1);
            }
        }
        let task = tokio::spawn(run_runtime_supervisor(
            proxies,
            next_generation,
            awaiting_cluster_bootstrap,
            registry,
            admin_state,
            initial,
            coordinator,
            apply_rx,
            exit_tx,
            exit_rx,
            publication_tx,
            readiness_tx,
            startup_tx,
            shutdown,
        ));
        Ok((readiness_rx, startup_rx, task))
    }
}

/// 业务作用: 运行代理任务退出监控、配置应用和进程关闭循环。
///
/// 参数说明:
/// - `proxies`: 当前活动代理名称到运行代的映射。
/// - `next_generation`: 下一个代理运行代编号。
/// - `awaiting_cluster_bootstrap`: 是否仍在等待首次权威集群版本创建数据面。
/// - `registry`: 管理面代理运行时注册表。
/// - `admin_state`: 可原子替换管理端认证策略的共享状态。
/// - `current_config`: 当前活动配置，用于限制管理监听和进程级不可变字段。
/// - `coordinator`: 可选的进程级 HA 协调器。
/// - `apply_rx`: 配置应用请求接收通道。
/// - `exit_tx`: 新代理任务使用的退出事件发送端。
/// - `exit_rx`: 全部代理任务共享的退出事件接收端。
/// - `publication_gate`: 多代理配置事务统一暂停和开放新连接的进程级门禁。
/// - `readiness_tx`: 每次成功发布后原子替换为最新活动运行代的监听地址集合。
/// - `startup`: 首代监听与运行时准备完成通知，不依赖业务 accept 开放。
/// - `shutdown`: 进程级关闭信号。
///
/// 返回: 正常关闭时返回成功；活动代理意外退出时返回错误并触发进程关闭。
#[allow(clippy::too_many_arguments)]
async fn run_runtime_supervisor(
    mut proxies: HashMap<String, ManagedProxy>,
    mut next_generation: u64,
    mut awaiting_cluster_bootstrap: bool,
    registry: RuntimeRegistry,
    admin_state: AdminState,
    mut current_config: LoadedConfig,
    coordinator: Option<Arc<CoordinatorHandle>>,
    mut apply_rx: mpsc::Receiver<ApplyConfigRequest>,
    exit_tx: mpsc::UnboundedSender<ProxyExit>,
    mut exit_rx: mpsc::UnboundedReceiver<ProxyExit>,
    publication_gate: AdmissionGate,
    readiness_tx: watch::Sender<Vec<SocketAddr>>,
    startup: oneshot::Sender<()>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let mut retiring = HashSet::new();
    let mut retired_drains: Vec<ManagedProxy> = Vec::new();
    let mut failure = None;
    let mut task_health = tokio::time::interval(Duration::from_millis(100));
    task_health.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut startup = Some(startup);
    if !awaiting_cluster_bootstrap {
        let timeout = Duration::from_millis(current_config.config().reload.transaction_timeout_ms);
        // 以监听器和运行代的显式确认作为启动证据，业务准入仍保持关闭。
        for proxy in proxies.values_mut() {
            wait_proxy_ready(proxy, timeout).await?;
            activate_proxy(proxy, timeout).await?;
        }
        if let Some(startup) = startup.take() {
            let _ = startup.send(());
        }
    }
    loop {
        let mut index = 0;
        while index < retired_drains.len() {
            if retired_drains[index].join.is_finished() {
                // 退休不等于成功排空；必须取得等待结果，保留旧代收尾失败与 panic。
                if let Err(error) = stop_proxy(retired_drains.swap_remove(index)).await {
                    failure.get_or_insert(error);
                }
            } else {
                index += 1;
            }
        }
        if failure.is_some() {
            break;
        }
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            _ = task_health.tick() => {
                if !*shutdown.borrow() && proxies.values().any(|proxy| proxy.join.is_finished()) {
                    // panic 无法发送正常退出事件，独立观察任务状态才能及时撤销业务准入。
                    failure = Some(anyhow::anyhow!("active proxy task exited before shutdown"));
                    break;
                }
            }
            Some(request) = apply_rx.recv() => {
                let candidate = request.candidate().clone();
                let mode = request.mode();
                let origin = request.origin();
                let diff = if awaiting_cluster_bootstrap && origin == ApplyOrigin::Cluster {
                    cluster_bootstrap_diff(&candidate, request.diff())
                } else {
                    request.diff().clone()
                };
                let candidate_readiness = readiness_addresses(candidate.config().as_ref())
                    .map_err(|error| format!("{error:#}"));
                let result = match candidate_readiness {
                    Ok(candidate_readiness) => {
                        let result = apply_candidate(
                            &mut proxies,
                            &mut next_generation,
                            &registry,
                            coordinator.clone(),
                            &exit_tx,
                            &mut retiring,
                            &mut retired_drains,
                            &admin_state,
                            &current_config,
                            &candidate,
                            &diff,
                            mode,
                            origin,
                            &publication_gate,
                        ).await;
                        if result.is_ok() && mode == ApplyMode::Publish {
                            current_config = candidate;
                            readiness_tx.send_replace(candidate_readiness);
                            if origin == ApplyOrigin::Cluster {
                                awaiting_cluster_bootstrap = false;
                                if let Some(startup) = startup.take() {
                                    // 首个权威配置已完成监听激活与 HA 绑定，才能解除进程启动等待。
                                    let _ = startup.send(());
                                }
                            }
                        }
                        result
                    }
                    Err(error) => Err(error),
                };
                request.complete(result);
            }
            Some(exit) = exit_rx.recv() => {
                if *shutdown.borrow() {
                    break;
                }
                if retiring.remove(&(exit.name.clone(), exit.generation)) {
                    continue;
                }
                let is_active = proxies
                    .get(&exit.name)
                    .is_some_and(|proxy| proxy.generation == exit.generation);
                if !is_active {
                    continue;
                }
                let result = exit.result
                    .map(|_| "proxy task exited without a shutdown request".to_string())
                    .unwrap_or_else(|error| error);
                failure = Some(anyhow::anyhow!(
                    "proxy `{}` generation {} stopped unexpectedly: {}",
                    exit.name,
                    exit.generation,
                    result
                ));
                break;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            else => break,
        }
    }

    // 失去关键监听或收到停止后先封闭新连接，再同时通知活动与退休运行代排空。
    publication_gate.send_replace(false);
    for proxy in proxies.values().chain(retired_drains.iter()) {
        proxy.shutdown.send_replace(true);
    }
    let mut drains = FuturesUnordered::new();
    for (name, proxy) in proxies {
        drains.push(stop_proxy(proxy));
        registry.unregister_proxy(&name).await;
    }
    for proxy in retired_drains {
        drains.push(stop_proxy(proxy));
    }
    while let Some(result) = drains.next().await {
        if let Err(error) = result {
            warn!(error = %error, "proxy generation stopped with error");
            failure.get_or_insert(error);
        }
    }
    match failure {
        Some(error) => Err(error.context("proxy generation shutdown failed")),
        None => Ok(()),
    }
}

/// 业务作用: 将首次权威集群发布转换为“从空数据面新增全部代理”的运行时差异。
///
/// 参数说明:
/// - `candidate`: 已从协调日志恢复并校验的权威候选配置。
/// - `original`: 候选版本相对本地启动文件的原始差异，用于保留非代理字段变化。
///
/// 返回: 全部候选代理按名称排序后均归为新增项，且不携带任何本地旧代理状态。
fn cluster_bootstrap_diff(candidate: &LoadedConfig, original: &ConfigDiff) -> ConfigDiff {
    let mut added = candidate
        .config()
        .proxies
        .iter()
        .map(|item| item.name.clone())
        .collect::<Vec<_>>();
    added.sort();
    ConfigDiff {
        added,
        updated: Vec::new(),
        removed: Vec::new(),
        unchanged: Vec::new(),
        admin_changed: original.admin_changed,
        coordination_changed: original.coordination_changed,
        process_changed: original.process_changed,
        log_changed: original.log_changed,
        reload_changed: original.reload_changed,
    }
}

/// 业务作用: 预检并按事务方式发布一个候选配置版本。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `next_generation`: 下一个可分配的运行代编号。
/// - `registry`: 管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `exit_tx`: 新任务使用的退出事件发送端。
/// - `retiring`: 正在按预期退出、不能判为进程故障的运行代集合。
/// - `retired_drains`: 仍持有旧连接的退休运行代，始终由进程资源树负责等待或取消。
/// - `admin_state`: 共享管理状态，用于在发布点替换认证策略。
/// - `current_config`: 当前活动配置，用于判断管理 listener 是否可原位更新。
/// - `candidate`: 已完成加载和静态校验的候选配置。
/// - `diff`: 候选版本相对当前活动版本的差异。
/// - `mode`: 只预检还是完整发布。
/// - `origin`: 本地来源或集群配置事务来源。
/// - `publication_gate`: 发布期间统一关闭全部代理新连接入口的进程级门禁。
///
/// 返回: 全部目标代理预检或发布成功时返回成功；失败时尽力恢复旧运行代。
#[allow(clippy::too_many_arguments)]
async fn apply_candidate(
    proxies: &mut HashMap<String, ManagedProxy>,
    next_generation: &mut u64,
    registry: &RuntimeRegistry,
    coordinator: Option<Arc<CoordinatorHandle>>,
    exit_tx: &mpsc::UnboundedSender<ProxyExit>,
    retiring: &mut HashSet<(String, u64)>,
    retired_drains: &mut Vec<ManagedProxy>,
    admin_state: &AdminState,
    current_config: &LoadedConfig,
    candidate: &LoadedConfig,
    diff: &ConfigDiff,
    mode: ApplyMode,
    origin: ApplyOrigin,
    publication_gate: &AdmissionGate,
) -> Result<(), String> {
    // 已经停止时不再准备日志、凭据或发出控制动作，后续由监督器统一排空现有运行代。
    if publication_gate.is_stopping() {
        return Err("application is stopping; candidate preparation is not allowed".to_string());
    }
    let current_config = current_config.config();
    let candidate_config = candidate.config();
    if candidate.cluster_payload().len() > MAX_CLUSTER_CONFIG_BYTES {
        return Err(
            "cluster-managed configuration exceeds the 16 MiB persistence limit".to_string(),
        );
    }
    if candidate.cluster_digest().len() != 64 {
        return Err("cluster-managed configuration digest is invalid".to_string());
    }
    if coordinator.is_some() && origin == ApplyOrigin::Local && mode == ApplyMode::Publish {
        return Err(
            "cluster member configuration must be submitted through the coordination gRPC transaction"
                .to_string(),
        );
    }
    if diff.admin_changed {
        let current_admin = &current_config.admin;
        let candidate_admin = &candidate_config.admin;
        if current_admin.enabled != candidate_admin.enabled
            || current_admin.listen != candidate_admin.listen
        {
            return Err(
                "admin listener enablement or address change requires listener migration"
                    .to_string(),
            );
        }
    }
    if diff.coordination_changed {
        return Err(
            "coordination configuration requires cluster configuration transaction".to_string(),
        );
    }
    if diff.process_changed {
        return Err(
            "process-level configuration change requires an implemented hot-reload adapter"
                .to_string(),
        );
    }
    let mut prepared_log = if diff.log_changed {
        let logging = admin_state.logging().map_err(|error| error.to_string())?;
        let prepared = logging
            .prepare(&candidate_config.log)
            .await
            .map_err(|error| error.to_string())?;
        Some((logging, prepared))
    } else {
        None
    };
    // 日志准备可能等待 I/O；停止期间完成的准备资源只能释放，不能继续进入控制动作。
    if publication_gate.is_stopping() {
        return Err("application stopped during candidate log preparation".to_string());
    }
    let candidate_admin_auth = if diff.admin_changed {
        let admin = &candidate_config.admin;
        Some(if admin.allow_unauthenticated {
            AdminAuth::disabled()
        } else {
            AdminAuth::token(
                admin
                    .token
                    .clone()
                    .ok_or_else(|| "candidate admin token is missing".to_string())?,
            )
        })
    } else {
        None
    };
    if mode == ApplyMode::HaPrecommit {
        if origin != ApplyOrigin::Cluster {
            return Err("HA topology precommit requires a cluster transaction".to_string());
        }
        let candidate_by_name = candidate_config
            .proxies
            .iter()
            .map(|item| (item.name.clone(), item))
            .collect::<HashMap<_, _>>();
        preflight_changed_listeners(proxies, &candidate_by_name, diff).await?;
        return precommit_ha_topology_changes(
            registry,
            current_config.as_ref(),
            candidate_config.as_ref(),
            diff,
        )
        .await;
    }
    if mode == ApplyMode::Publish && origin == ApplyOrigin::Local {
        let candidate_by_name = candidate_config
            .proxies
            .iter()
            .map(|item| (item.name.clone(), item))
            .collect::<HashMap<_, _>>();
        preflight_changed_listeners(proxies, &candidate_by_name, diff).await?;
        precommit_ha_topology_changes(
            registry,
            current_config.as_ref(),
            candidate_config.as_ref(),
            diff,
        )
        .await?;
    }

    let candidate_by_name = candidate_config
        .proxies
        .iter()
        .map(|item| (item.name.clone(), item))
        .collect::<HashMap<_, _>>();
    if !(mode == ApplyMode::Publish && origin == ApplyOrigin::Local) {
        preflight_changed_listeners(proxies, &candidate_by_name, diff).await?;
    }
    preflight_changed_proxy_backends(
        proxies,
        registry,
        current_config.as_ref(),
        &candidate_by_name,
        diff,
    )
    .await?;
    // 后端预检可能阻塞到停止之后，此时保留旧配置并释放候选材料，不申请运行代发布。
    if publication_gate.is_stopping() {
        return Err("application stopped during candidate backend preparation".to_string());
    }
    if mode == ApplyMode::ValidateOnly {
        return Ok(());
    }

    let stable_update_names = diff
        .updated
        .iter()
        .filter(|name| {
            let Some(current) = proxies.get(*name) else {
                return false;
            };
            let Some(candidate) = candidate_by_name.get(*name) else {
                return false;
            };
            current.controller.is_some()
                && candidate.mode != crate::config::ProxyMode::ClusterNodeProxy
                && current.item.bind_addr() == candidate.bind_addr()
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut prepared_stable_updates = Vec::new();
    for name in &stable_update_names {
        let candidate_item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        let generation = take_generation(next_generation);
        let active = proxies
            .get(name)
            .ok_or_else(|| format!("active proxy `{name}` disappeared before update"))?;
        let controller = active.controller.as_ref().ok_or_else(|| {
            format!("proxy `{name}` stable update controller disappeared before prepare")
        })?;
        if let Err(error) = controller
            .prepare(
                (*candidate_item).clone(),
                generation,
                Some(candidate.cluster_digest().to_string()),
            )
            .await
        {
            abort_stable_updates(proxies, &prepared_stable_updates).await;
            return Err(format!(
                "proxy `{name}` stable generation prepare failed: {error}"
            ));
        }
        prepared_stable_updates.push((name.clone(), generation));
    }

    let restart_update_names = diff
        .updated
        .iter()
        .filter(|name| !stable_update_names.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    let ready_timeout = Duration::from_millis(candidate_config.reload.transaction_timeout_ms);
    let retiring_names = restart_update_names
        .iter()
        .chain(diff.removed.iter())
        .cloned()
        .collect::<HashSet<_>>();
    let mut staged = HashMap::new();
    for name in diff.added.iter().chain(restart_update_names.iter()) {
        let item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        if candidate_conflicts_with_retiring_listener(proxies, item, &retiring_names)? {
            continue;
        }
        let generation = take_generation(next_generation);
        let mut managed = spawn_proxy(
            (*item).clone(),
            generation,
            candidate.cluster_digest().to_string(),
            registry.clone(),
            coordinator.clone(),
            exit_tx.clone(),
            publication_gate.subscribe(),
            false,
        );
        if let Err(error) = wait_proxy_ready(&mut managed, ready_timeout).await {
            retiring.insert((name.clone(), generation));
            let _ = stop_proxy(managed).await;
            stop_uninstalled_candidates(staged, retiring).await;
            abort_stable_updates(proxies, &prepared_stable_updates).await;
            return Err(format!(
                "proxy `{name}` candidate generation failed before publication: {error:#}"
            ));
        }
        staged.insert(name.clone(), managed);
    }

    // 日志或管理凭据单独变化也必须取得发布资格；停止先于此处时，撤销全部暂存运行代。
    let mut publication_pause = match PublicationPause::begin(publication_gate) {
        Ok(pause) => Some(pause),
        Err(error) => {
            stop_uninstalled_candidates(staged, retiring).await;
            abort_stable_updates(proxies, &prepared_stable_updates).await;
            return Err(error);
        }
    };
    let mut old_updated = Vec::new();
    // 删除项也必须在候选新增项绑定前停止，否则“删除旧名称并在同一地址新增新名称”
    // 会被仍占用端口的旧 listener 阻塞。失败回滚时统一从 old_updated 恢复。
    for name in restart_update_names.iter().chain(diff.removed.iter()) {
        let mut old = proxies
            .remove(name)
            .ok_or_else(|| format!("active proxy `{name}` disappeared before update"))?;
        retiring.insert((name.clone(), old.generation));
        let old_item = (old.item.clone(), old.cluster_config_digest.clone());
        let close_result = close_proxy_listener(&mut old, ready_timeout).await;
        old_updated.push(old_item);
        retired_drains.push(old);
        if let Err(error) = close_result {
            stop_uninstalled_candidates(staged, retiring).await;
            let restored = rollback_installed(
                proxies,
                Vec::new(),
                old_updated,
                next_generation,
                registry,
                coordinator,
                exit_tx,
                retiring,
                ready_timeout,
                publication_gate,
            )
            .await;
            abort_stable_updates(proxies, &prepared_stable_updates).await;
            if restored {
                if let Some(pause) = publication_pause.take() {
                    pause.reopen();
                }
            }
            return Err(format!(
                "proxy `{name}` old listener did not close during publication: {error:#}"
            ));
        }
    }

    let mut installed = Vec::new();
    for name in diff.added.iter().chain(restart_update_names.iter()) {
        let item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        let (generation, mut managed) = if let Some(managed) = staged.remove(name) {
            (managed.generation, managed)
        } else {
            let generation = take_generation(next_generation);
            let managed = spawn_proxy(
                (*item).clone(),
                generation,
                candidate.cluster_digest().to_string(),
                registry.clone(),
                coordinator.clone(),
                exit_tx.clone(),
                publication_gate.subscribe(),
                false,
            );
            (generation, managed)
        };
        if managed.ready.is_some() {
            if let Err(error) = wait_proxy_ready(&mut managed, ready_timeout).await {
                retiring.insert((name.clone(), generation));
                let _ = stop_proxy(managed).await;
                stop_uninstalled_candidates(staged, retiring).await;
                let restored = rollback_installed(
                    proxies,
                    installed,
                    old_updated,
                    next_generation,
                    registry,
                    coordinator,
                    exit_tx,
                    retiring,
                    ready_timeout,
                    publication_gate,
                )
                .await;
                abort_stable_updates(proxies, &prepared_stable_updates).await;
                if restored {
                    if let Some(pause) = publication_pause.take() {
                        pause.reopen();
                    }
                }
                return Err(format!(
                    "proxy `{name}` candidate generation failed: {error:#}"
                ));
            }
        }
        proxies.insert(name.clone(), managed);
        installed.push(name.clone());
    }
    for name in installed.clone() {
        let managed = proxies
            .get_mut(&name)
            .ok_or_else(|| format!("candidate proxy `{name}` disappeared before activation"))?;
        if let Err(error) = activate_proxy(managed, ready_timeout).await {
            stop_uninstalled_candidates(staged, retiring).await;
            let restored = rollback_installed(
                proxies,
                installed,
                old_updated,
                next_generation,
                registry,
                coordinator,
                exit_tx,
                retiring,
                ready_timeout,
                publication_gate,
            )
            .await;
            abort_stable_updates(proxies, &prepared_stable_updates).await;
            if restored {
                if let Some(pause) = publication_pause.take() {
                    pause.reopen();
                }
            }
            return Err(format!(
                "proxy `{name}` candidate activation failed: {error:#}"
            ));
        }
    }

    let mut committed_stable_updates = Vec::new();
    for (name, generation) in &prepared_stable_updates {
        let candidate_item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        let active = proxies
            .get_mut(name)
            .ok_or_else(|| format!("active proxy `{name}` disappeared before commit"))?;
        let controller = active.controller.as_ref().ok_or_else(|| {
            format!("proxy `{name}` stable update controller disappeared before commit")
        })?;
        if let Err(error) = controller.commit(*generation).await {
            let remaining = prepared_stable_updates
                .iter()
                .filter(|(candidate_name, _)| {
                    !committed_stable_updates
                        .iter()
                        .any(|(committed_name, _, _)| committed_name == candidate_name)
                        && candidate_name != name
                })
                .cloned()
                .collect::<Vec<_>>();
            abort_stable_updates(proxies, &remaining).await;
            let restarted_restored = rollback_installed(
                proxies,
                installed,
                old_updated,
                next_generation,
                registry,
                coordinator,
                exit_tx,
                retiring,
                Duration::from_millis(candidate_config.reload.transaction_timeout_ms),
                publication_gate,
            )
            .await;
            let stable_restored = rollback_committed_stable_updates(
                proxies,
                committed_stable_updates,
                next_generation,
            )
            .await;
            if restarted_restored && stable_restored {
                if let Some(pause) = publication_pause.take() {
                    pause.reopen();
                }
            }
            return Err(format!(
                "proxy `{name}` stable generation commit failed: {error}"
            ));
        }
        committed_stable_updates.push((
            name.clone(),
            active.item.clone(),
            active.cluster_config_digest.clone(),
        ));
        active.item = (*candidate_item).clone();
        active.config_generation = *generation;
        active.cluster_config_digest = candidate.cluster_digest().to_string();
    }

    let ha_binding_result: Result<(), String> = async {
        if coordinator.is_some() {
            for item in &candidate_config.proxies {
                if item.mode != ProxyMode::ReplicationHa {
                    continue;
                }
                let runtime = wait_for_active_ha_runtime(
                    proxies,
                    registry,
                    &item.name,
                    Duration::from_millis(candidate_config.reload.transaction_timeout_ms),
                )
                .await?;
                runtime
                    .refresh_cluster_group_registration(candidate.cluster_digest())
                    .await
                    .map_err(|error| {
                        format!(
                            "proxy `{}` cannot bind HA configuration to committed revision: {error:#}",
                            item.name
                        )
                    })?;
            }
        }
        if let Some((logging, prepared)) = &mut prepared_log {
            // 代理与 HA 绑定准备成功后才安装日志；失败仍进入共同回滚，不能开放半完成的新配置。
            logging.install(prepared).map_err(|error| error.to_string())?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = ha_binding_result {
        let restarted_restored = rollback_installed(
            proxies,
            installed,
            old_updated,
            next_generation,
            registry,
            coordinator,
            exit_tx,
            retiring,
            Duration::from_millis(candidate_config.reload.transaction_timeout_ms),
            publication_gate,
        )
        .await;
        let stable_restored =
            rollback_committed_stable_updates(proxies, committed_stable_updates, next_generation)
                .await;
        if restarted_restored && stable_restored {
            if let Some(pause) = publication_pause.take() {
                pause.reopen();
            }
        }
        return Err(error);
    }

    for name in &diff.removed {
        if let Some(old) = proxies.remove(name) {
            retiring.insert((name.clone(), old.generation));
            if let Err(error) = stop_proxy(old).await {
                warn!(proxy = %name, error = %error, "removed proxy generation drained with error");
            }
        }
        registry.unregister_proxy(name).await;
    }
    if let Some(auth) = candidate_admin_auth {
        admin_state.set_auth(auth);
    }
    if let Some(pause) = publication_pause.take() {
        pause.reopen();
    }
    // 旧 listener 已释放，旧连接在门禁重新开放后继续排空，任务所有权保留到监督器确认退出。
    info!(
        added = ?diff.added,
        updated = ?diff.updated,
        removed = ?diff.removed,
        "proxy configuration generation published"
    );
    Ok(())
}

/// 业务作用: 等待首代或候选 HA 运行时完成后端发现并发布到管理注册表。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代，用于在等待期间识别任务提前退出。
/// - `registry`: 保存已经完成构造并激活的 HA 运行时。
/// - `proxy_name`: 要等待的稳定代理名称。
/// - `timeout`: 允许后端发现和协调组初始化完成的最长时间。
///
/// 返回: 运行时成功注册时返回共享句柄；任务退出或超时时返回明确错误。
async fn wait_for_active_ha_runtime(
    proxies: &HashMap<String, ManagedProxy>,
    registry: &RuntimeRegistry,
    proxy_name: &str,
    timeout: Duration,
) -> Result<Arc<crate::ha::runtime::ReplicationHaRuntime>, String> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(runtime) = registry.replication_ha_runtime(proxy_name).await {
            return Ok(runtime);
        }
        if proxies
            .get(proxy_name)
            .is_none_or(|proxy| proxy.join.is_finished())
        {
            return Err(format!(
                "HA runtime for proxy `{proxy_name}` exited before registration"
            ));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "active HA runtime for proxy `{proxy_name}` is unavailable after publish timeout"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 业务作用: 在成员配置预检前仅由协调领导者执行 HA 节点集合的必要拓扑迁移。
///
/// 参数说明:
/// - `registry`: 提供当前活动 HA 运行时和真实主节点状态的管理注册表。
/// - `current`: 当前已经提交并运行的完整配置。
/// - `candidate`: 即将广播给全部成员的候选配置。
/// - `diff`: 当前与候选之间按代理名称计算的确定性差异。
///
/// 返回: 不需要切主或当前主节点已安全迁移到候选保留节点时返回成功。
async fn precommit_ha_topology_changes(
    registry: &RuntimeRegistry,
    current: &ProxyConfig,
    candidate: &ProxyConfig,
    diff: &ConfigDiff,
) -> Result<(), String> {
    let current_by_name = current
        .proxies
        .iter()
        .map(|item| (item.name.as_str(), item))
        .collect::<HashMap<_, _>>();
    let candidate_by_name = candidate
        .proxies
        .iter()
        .map(|item| (item.name.as_str(), item))
        .collect::<HashMap<_, _>>();
    // 风险检查必须早于切主、持久化停用等预提交副作用。任何被删除或更新的旧
    // replication-ha 运行代都可能丢失内存中的多主证据，不能只拦截节点集合变化。
    for name in diff.removed.iter().chain(&diff.updated) {
        let Some(current_item) = current_by_name.get(name.as_str()) else {
            continue;
        };
        if current_item.mode != ProxyMode::ReplicationHa {
            continue;
        }
        let runtime = registry
            .replication_ha_runtime(name)
            .await
            .ok_or_else(|| format!("active HA runtime for proxy `{name}` is unavailable"))?;
        runtime
            .ensure_configuration_change_has_no_unconfirmed_extra_masters()
            .await
            .map_err(|error| {
                format!(
                    "proxy `{name}` cannot replace its HA runtime while multi-master evidence is unresolved: {error:#}"
                )
            })?;
    }
    // 删除整个 replication-ha proxy 后不会再有新运行时负责清理动态持久化。
    // 因此必须在配置提交前停用全部 owner；失败时保留旧配置和旧运行时。
    for name in &diff.removed {
        let Some(current_item) = current_by_name.get(name.as_str()) else {
            continue;
        };
        if current_item.mode != ProxyMode::ReplicationHa {
            continue;
        }
        let current_ha = current_item
            .replication_ha_config()
            .map_err(|error| format!("{error:#}"))?;
        if !current_ha.persistence.enabled {
            continue;
        }
        let runtime = registry
            .replication_ha_runtime(name)
            .await
            .ok_or_else(|| format!("active HA runtime for proxy `{name}` is unavailable"))?;
        runtime
            .planned_persistence_disable()
            .await
            .map_err(|error| {
                format!(
                    "proxy `{name}` cannot be removed before dynamic persistence is disabled: {error:#}"
                )
            })?;
    }
    for name in &diff.updated {
        let Some(current_item) = current_by_name.get(name.as_str()) else {
            continue;
        };
        let Some(candidate_item) = candidate_by_name.get(name.as_str()) else {
            continue;
        };
        if current_item.mode != ProxyMode::ReplicationHa {
            continue;
        }
        let current_ha = current_item
            .replication_ha_config()
            .map_err(|error| format!("{error:#}"))?;
        if candidate_item.mode != ProxyMode::ReplicationHa {
            if current_ha.persistence.enabled {
                let runtime = registry.replication_ha_runtime(name).await.ok_or_else(|| {
                    format!("active HA runtime for proxy `{name}` is unavailable")
                })?;
                runtime
                    .planned_persistence_disable()
                    .await
                    .map_err(|error| {
                        format!(
                            "proxy `{name}` cannot leave replication-ha mode before dynamic persistence is disabled: {error:#}"
                        )
                    })?;
            }
            continue;
        }
        let candidate_ha = candidate_item
            .replication_ha_config()
            .map_err(|error| format!("{error:#}"))?;
        let nodes_changed = current_item.nodes != candidate_item.nodes;
        let persistence_disabling =
            current_ha.persistence.enabled && !candidate_ha.persistence.enabled;
        if !nodes_changed && !persistence_disabling {
            continue;
        }
        let runtime = registry
            .replication_ha_runtime(name)
            .await
            .ok_or_else(|| format!("active HA runtime for proxy `{name}` is unavailable"))?;
        let current_master = runtime.current_master_addr().await;
        let candidate_nodes = parse_ha_nodes(
            candidate_item
                .nodes
                .as_deref()
                .ok_or_else(|| format!("candidate HA proxy `{name}` has no nodes"))?,
        )
        .map_err(|error| format!("{error:#}"))?;
        if !candidate_nodes
            .iter()
            .any(|node| node.client_addr == current_master)
        {
            let retained = candidate_nodes
                .iter()
                .map(|node| node.client_addr.clone())
                .collect::<Vec<_>>();
            runtime
                .planned_switchover_to_retained(&retained)
                .await
                .map_err(|error| {
                    format!(
                        "proxy `{name}` cannot remove current master `{current_master}` without an eligible retained replica: {error:#}"
                    )
                })?;
        }
        if persistence_disabling {
            runtime
                .planned_persistence_disable()
                .await
                .map_err(|error| format!("{error:#}"))?;
        } else if nodes_changed
            && current_ha.persistence.enabled
            && candidate_ha.persistence.enabled
        {
            let retained = candidate_nodes
                .iter()
                .map(|node| node.client_addr.clone())
                .collect::<Vec<_>>();
            runtime
                .planned_persistence_handoff_to_retained(&retained)
                .await
                .map_err(|error| format!("{error:#}"))?;
        }
    }
    Ok(())
}

/// 业务作用: 丢弃本事务已经完成构造、但尚未发布到稳定 listener 的候选运行代。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `prepared`: 已在对应 listener 内暂存的代理名称和候选代次。
async fn abort_stable_updates(proxies: &HashMap<String, ManagedProxy>, prepared: &[(String, u64)]) {
    for (name, generation) in prepared {
        let Some(active) = proxies.get(name) else {
            warn!(
                proxy = %name,
                "stable listener disappeared while aborting prepared configuration"
            );
            continue;
        };
        let Some(controller) = &active.controller else {
            warn!(
                proxy = %name,
                "stable listener controller disappeared while aborting prepared configuration"
            );
            continue;
        };
        if let Err(error) = controller.abort(*generation).await {
            warn!(
                proxy = %name,
                error = %error,
                "failed to abort prepared stable proxy generation"
            );
        }
    }
}

/// 业务作用: 将本事务已经提交到稳定 listener 的运行代按反序切回旧配置。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `committed`: 已发布代理名称、旧配置和旧全局摘要。
/// - `next_generation`: 回滚运行代也必须使用新的单调递增编号。
async fn rollback_committed_stable_updates(
    proxies: &mut HashMap<String, ManagedProxy>,
    mut committed: Vec<(String, ProxyItem, String)>,
    next_generation: &mut u64,
) -> bool {
    let mut restored = true;
    committed.reverse();
    for (name, old_item, old_cluster_config_digest) in committed {
        let Some(active) = proxies.get_mut(&name) else {
            warn!(proxy = %name, "stable listener disappeared during configuration rollback");
            restored = false;
            continue;
        };
        let Some(controller) = &active.controller else {
            warn!(proxy = %name, "stable listener controller disappeared during configuration rollback");
            restored = false;
            continue;
        };
        let generation = take_generation(next_generation);
        let prepared = controller
            .prepare(
                old_item.clone(),
                generation,
                Some(old_cluster_config_digest.clone()),
            )
            .await;
        if let Err(error) = prepared {
            warn!(proxy = %name, error = %error, "failed to prepare old stable proxy generation");
            restored = false;
            continue;
        }
        match controller.commit(generation).await {
            Ok(()) => {
                active.item = old_item;
                active.config_generation = generation;
                active.cluster_config_digest = old_cluster_config_digest;
            }
            Err(error) => {
                warn!(proxy = %name, error = %error, "failed to restore old stable proxy generation");
                restored = false;
            }
        }
    }
    restored
}

/// 业务作用: 对全部新增和修改代理执行后端预检。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `registry`: 提供当前 HA 运行代确认的主节点，用于识别删主前的合法过渡态。
/// - `current_config`: 当前活动配置，用于区分同名代理是否持续处于托管复制 HA 模式。
/// - `candidate_by_name`: 候选代理名称到配置项的映射。
/// - `diff`: 当前活动版本与候选版本差异。
///
/// 返回: 全部新增或修改代理的后端控制面可用时返回成功。
async fn preflight_changed_proxy_backends(
    proxies: &HashMap<String, ManagedProxy>,
    registry: &RuntimeRegistry,
    current_config: &ProxyConfig,
    candidate_by_name: &HashMap<String, &ProxyItem>,
    diff: &ConfigDiff,
) -> Result<(), String> {
    let current_by_name = current_config
        .proxies
        .iter()
        .map(|item| (item.name.as_str(), item))
        .collect::<HashMap<_, _>>();
    for name in diff.added.iter().chain(diff.updated.iter()) {
        let item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        let current_replication_ha = proxies.contains_key(name)
            && current_by_name
                .get(name.as_str())
                .is_some_and(|current| current.mode == ProxyMode::ReplicationHa);
        if item.mode == ProxyMode::ReplicationHa && current_replication_ha {
            let runtime = registry
                .replication_ha_runtime(name)
                .await
                .ok_or_else(|| format!("active HA runtime for proxy `{name}` is unavailable"))?;
            let current_master = runtime.current_master_node().await;
            proxy::preflight_replication_ha_transition(item, &current_master)
                .await
                .map_err(|error| format!("{error:#}"))?;
        } else {
            proxy::preflight_proxy_item(item)
                .await
                .map_err(|error| format!("{error:#}"))?;
        }
    }
    Ok(())
}

/// 业务作用: 对全部新增和修改代理执行新监听地址的无副作用绑定预检。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `candidate_by_name`: 候选代理名称到配置项的映射。
/// - `diff`: 当前活动版本与候选版本差异。
///
/// 返回: 所有当前未占用的新监听地址均可绑定时返回成功。
async fn preflight_changed_listeners(
    proxies: &HashMap<String, ManagedProxy>,
    candidate_by_name: &HashMap<String, &ProxyItem>,
    diff: &ConfigDiff,
) -> Result<(), String> {
    let active_binds = active_bind_owners(proxies)?;
    for name in diff.added.iter().chain(diff.updated.iter()) {
        let item = candidate_by_name
            .get(name)
            .ok_or_else(|| format!("candidate proxy `{name}` is missing"))?;
        for bind in item.listen_addrs().map_err(|error| format!("{error:#}"))? {
            let mut conflicts = Vec::new();
            for (owner, active_bind) in &active_binds {
                if listen_addresses_conflict(active_bind, &bind)
                    .map_err(|error| format!("{error:#}"))?
                {
                    conflicts.push(owner);
                }
            }
            if !conflicts.is_empty()
                && conflicts.iter().all(|owner| {
                    *owner == name || diff.removed.contains(*owner) || diff.updated.contains(*owner)
                })
            {
                continue;
            }
            let listener = TcpListener::bind(&bind).await.map_err(|error| {
                format!("proxy `{name}` cannot prepare listener `{bind}`: {error}")
            })?;
            drop(listener);
        }
    }
    Ok(())
}

/// 业务作用: 判断候选 listener 是否必须等待本事务中的旧 listener 先释放地址。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `candidate`: 要在发布前尝试绑定的候选代理配置。
/// - `retiring_names`: 本事务会停止或删除的旧代理名称集合。
///
/// 返回: 候选地址与任一待退役 listener 冲突时返回真。
fn candidate_conflicts_with_retiring_listener(
    proxies: &HashMap<String, ManagedProxy>,
    candidate: &ProxyItem,
    retiring_names: &HashSet<String>,
) -> Result<bool, String> {
    let active_binds = active_bind_owners(proxies)?;
    for bind in candidate
        .listen_addrs()
        .map_err(|error| format!("{error:#}"))?
    {
        for (owner, active_bind) in &active_binds {
            if retiring_names.contains(owner)
                && listen_addresses_conflict(active_bind, &bind)
                    .map_err(|error| format!("{error:#}"))?
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// 业务作用: 构造当前活动监听地址到代理名称的映射。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
///
/// 返回: 地址解析成功时返回代理名称与监听地址列表，否则返回配置错误。
fn active_bind_owners(
    proxies: &HashMap<String, ManagedProxy>,
) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    for (name, proxy) in proxies {
        for bind in proxy
            .item
            .listen_addrs()
            .map_err(|error| format!("{error:#}"))?
        {
            out.push((name.clone(), bind));
        }
    }
    Ok(out)
}

/// 业务作用: 停止尚未装入活动映射的候选运行代，并把退出标记为预期行为。
///
/// 参数说明:
/// - `candidates`: 已绑定但尚未发布的候选名称和运行代。
/// - `retiring`: 正常退出运行代集合。
async fn stop_uninstalled_candidates(
    candidates: HashMap<String, ManagedProxy>,
    retiring: &mut HashSet<(String, u64)>,
) {
    for (name, candidate) in candidates {
        retiring.insert((name, candidate.generation));
        let _ = stop_proxy(candidate).await;
    }
}

/// 业务作用: 在候选运行代启动失败后停止已安装候选，并尽力恢复全部旧配置。
///
/// 参数说明:
/// - `proxies`: 当前活动代理运行代映射。
/// - `installed`: 本事务已经成功启动的候选代理名称。
/// - `old_updated`: 更新前被停止的旧代理配置和全局配置摘要。
/// - `next_generation`: 下一个可分配的运行代编号。
/// - `registry`: 管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `exit_tx`: 新任务使用的退出事件发送端。
/// - `retiring`: 正常退出运行代集合。
/// - `ready_timeout`: 恢复旧代理等待监听就绪的最长时间。
/// - `publication_gate`: 恢复出的旧代理继续订阅的进程级发布门禁。
///
/// 返回: 全部旧代理均重新启动并进入就绪状态时返回 `true`。
#[allow(clippy::too_many_arguments)]
async fn rollback_installed(
    proxies: &mut HashMap<String, ManagedProxy>,
    installed: Vec<String>,
    old_updated: Vec<(ProxyItem, String)>,
    next_generation: &mut u64,
    registry: &RuntimeRegistry,
    coordinator: Option<Arc<CoordinatorHandle>>,
    exit_tx: &mpsc::UnboundedSender<ProxyExit>,
    retiring: &mut HashSet<(String, u64)>,
    ready_timeout: Duration,
    publication_gate: &AdmissionGate,
) -> bool {
    let mut restored_all = true;
    for name in installed {
        if let Some(candidate) = proxies.remove(&name) {
            retiring.insert((name.clone(), candidate.generation));
            let _ = stop_proxy(candidate).await;
        }
        registry.unregister_proxy(&name).await;
    }
    for (item, cluster_config_digest) in old_updated {
        // 旧 listener 关闭超时的运行代可能仍短暂占用端口;先做有界等待吸收滞留,
        // 避免把一次瞬时端口占用放大成"回滚失败、门禁保持关闭待人工"。
        wait_for_listen_addrs_bindable(&item, ready_timeout.min(Duration::from_secs(1))).await;
        let generation = take_generation(next_generation);
        let mut restored = spawn_proxy(
            item.clone(),
            generation,
            cluster_config_digest,
            registry.clone(),
            coordinator.clone(),
            exit_tx.clone(),
            publication_gate.subscribe(),
            true,
        );
        let restored_ready = wait_proxy_ready(&mut restored, ready_timeout).await;
        let restored_active = match restored_ready {
            Ok(()) => activate_proxy(&mut restored, ready_timeout).await,
            Err(error) => Err(error),
        };
        match restored_active {
            Ok(()) => {
                proxies.insert(item.name.clone(), restored);
            }
            Err(error) => {
                restored_all = false;
                retiring.insert((item.name.clone(), generation));
                let _ = stop_proxy(restored).await;
                warn!(
                    proxy = %item.name,
                    error = %error,
                    "failed to restore old proxy generation after reload rollback"
                );
            }
        }
    }
    restored_all
}

/// 业务作用: 启动一个独立代理运行代，并在退出时发送包含代次的事件。
///
/// 参数说明:
/// - `item`: 当前运行代使用的完整代理配置。
/// - `generation`: 当前代理名称下单调递增的运行代编号。
/// - `cluster_config_digest`: 当前运行代所依据的全局集群配置摘要。
/// - `registry`: 管理面运行时注册表。
/// - `coordinator`: 可选进程级 HA 协调器。
/// - `exit_tx`: 代理任务退出事件发送端。
/// - `publication_guard`: 多代理配置事务统一暂停新连接的进程级门禁订阅。
/// - `start_active`: listener 绑定后立即激活，还是等待 supervisor 到达发布点。
///
/// 返回: 可由运行时 supervisor 停止和等待的代理句柄。
#[allow(clippy::too_many_arguments)]
fn spawn_proxy(
    item: ProxyItem,
    generation: u64,
    cluster_config_digest: String,
    registry: RuntimeRegistry,
    coordinator: Option<Arc<CoordinatorHandle>>,
    exit_tx: mpsc::UnboundedSender<ProxyExit>,
    publication_guard: watch::Receiver<bool>,
    start_active: bool,
) -> ManagedProxy {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (activation_tx, activation_rx) = watch::channel(start_active);
    let (ready_tx, ready_rx) = oneshot::channel();
    let (activated_tx, activated_rx) = oneshot::channel();
    let (listener_closed_tx, listener_closed_rx) = oneshot::channel();
    let name = item.name.clone();
    let task_item = item.clone();
    let task_name = name.clone();
    let task_cluster_config_digest = cluster_config_digest.clone();
    let (controller, updates) = if item.mode == crate::config::ProxyMode::ClusterNodeProxy {
        (None, None)
    } else {
        let (controller, updates) = proxy::proxy_generation_channel();
        (Some(controller), Some(updates))
    };
    let join = tokio::spawn(async move {
        let result = match updates {
            Some(updates) => {
                proxy::serve_reloadable_listener_with_registry_coordinator_and_publication_guard(
                    task_item,
                    generation,
                    shutdown_rx,
                    Some(registry),
                    coordinator,
                    Some(task_cluster_config_digest.clone()),
                    updates,
                    publication_guard,
                    activation_rx,
                    ready_tx,
                    activated_tx,
                    listener_closed_tx,
                )
                .await
            }
            None => {
                proxy::serve_listener_with_registry_coordinator_and_publication_guard(
                    task_item,
                    generation,
                    shutdown_rx,
                    Some(registry),
                    coordinator,
                    Some(task_cluster_config_digest.clone()),
                    publication_guard,
                    activation_rx,
                    ready_tx,
                    activated_tx,
                    listener_closed_tx,
                )
                .await
            }
        };
        let event_result = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("{error:#}"));
        if let Err(error) = &event_result {
            warn!(
                proxy = %task_name,
                generation,
                error = %error,
                "proxy generation stopped before runtime supervisor processed its exit"
            );
        }
        let _ = exit_tx.send(ProxyExit {
            name: task_name,
            generation,
            result: event_result,
        });
        result
    });
    ManagedProxy {
        item,
        generation,
        config_generation: generation,
        cluster_config_digest,
        controller,
        activation: activation_tx,
        ready: Some(ready_rx),
        activated: Some(activated_rx),
        listener_closed: Some(listener_closed_rx),
        shutdown: shutdown_tx,
        join: join.into(),
    }
}

/// 业务作用: 等待一个代理运行代显式报告全部监听地址已经绑定。
///
/// 参数说明:
/// - `proxy`: 刚启动且仍可能提前失败的代理运行代。
/// - `timeout`: 全部监听地址进入可连接状态的最长时间。
///
/// 返回: 全部 listener 完成绑定时返回成功；任务提前退出或超时时返回错误。
async fn wait_proxy_ready(proxy: &mut ManagedProxy, timeout: Duration) -> anyhow::Result<()> {
    let ready = proxy
        .ready
        .take()
        .with_context(|| format!("proxy `{}` readiness was already consumed", proxy.item.name))?;
    match tokio::time::timeout(timeout, ready).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => anyhow::bail!(
            "proxy `{}` exited before listeners became ready",
            proxy.item.name
        ),
        Err(_) => anyhow::bail!(
            "proxy `{}` listeners did not become ready before timeout",
            proxy.item.name
        ),
    }
}

/// 业务作用: 打开候选运行代的本地激活门禁，并等待运行时完成注册和接收任务安装。
///
/// 参数说明:
/// - `proxy`: 已完成 listener 绑定、尚未对业务发布的候选运行代。
/// - `timeout`: 候选完成运行时激活的最长等待时间。
///
/// 返回: 运行时和 accept 任务已经安装时返回成功；任务退出或超时时返回错误。
async fn activate_proxy(proxy: &mut ManagedProxy, timeout: Duration) -> anyhow::Result<()> {
    let activated = proxy.activated.take().with_context(|| {
        format!(
            "proxy `{}` activation was already consumed",
            proxy.item.name
        )
    })?;
    proxy.activation.send_replace(true);
    match tokio::time::timeout(timeout, activated).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => anyhow::bail!(
            "proxy `{}` exited before runtime activation completed",
            proxy.item.name
        ),
        Err(_) => anyhow::bail!(
            "proxy `{}` runtime activation did not complete before timeout",
            proxy.item.name
        ),
    }
}

/// 业务作用: 只关闭代理 listener 并等待端口释放，不等待旧连接完成排空。
///
/// 参数说明:
/// - `proxy`: 要退出业务监听、但允许连接任务在重新开门后继续排空的旧运行代。
/// - `timeout`: listener 释放端口的最长等待时间。
///
/// 返回: listener 明确关闭或代理任务已退出时返回成功；等待超时返回错误。
async fn close_proxy_listener(proxy: &mut ManagedProxy, timeout: Duration) -> anyhow::Result<()> {
    let _ = proxy.shutdown.send(true);
    let Some(listener_closed) = proxy.listener_closed.take() else {
        if proxy.join.is_finished() {
            return Ok(());
        }
        anyhow::bail!(
            "proxy `{}` listener-close signal was already consumed",
            proxy.item.name
        );
    };
    match tokio::time::timeout(timeout, listener_closed).await {
        Ok(Ok(())) | Ok(Err(_)) => Ok(()),
        Err(_) => anyhow::bail!(
            "proxy `{}` listener did not close before timeout",
            proxy.item.name
        ),
    }
}

/// 业务作用: 请求一个代理运行代排空并等待任务结束。
///
/// 参数说明:
/// - `proxy`: 要停止的活动或失败候选运行代。
///
/// 返回: 代理正常完成排空时返回成功；任务错误或 join 失败时返回错误。
async fn stop_proxy(proxy: ManagedProxy) -> anyhow::Result<()> {
    let mut proxy = proxy;
    let _ = proxy.shutdown.send(true);
    if let Some(listener_closed) = proxy.listener_closed.take() {
        let _ = listener_closed.await;
    }
    match proxy.join.await {
        Ok(result) => result,
        Err(error) => Err(anyhow::anyhow!("proxy task join failed: {error}")),
    }
}

/// 业务作用: 在回滚重启旧运行代前等待其全部监听地址重新可绑定。
///
/// 只做尽力等待:超时后直接返回,让后续真实 bind 报出明确错误并由既有失败路径处理;
/// 地址解析失败同样交给后续启动路径统一报错,这里不吞错误也不提前失败。
///
/// 参数说明:
/// - `item`: 即将回滚重启的旧代理配置,提供全部业务监听地址。
/// - `timeout`: 全部地址完成释放的最长等待时间。
async fn wait_for_listen_addrs_bindable(item: &ProxyItem, timeout: Duration) {
    let Ok(binds) = item.listen_addrs() else {
        return;
    };
    let deadline = Instant::now() + timeout;
    for bind in binds {
        loop {
            match TcpListener::bind(&bind).await {
                Ok(listener) => {
                    drop(listener);
                    break;
                }
                Err(_) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(_) => break,
            }
        }
    }
}

/// 业务作用: 分配一个单调递增的代理运行代编号。
///
/// 参数说明:
/// - `next_generation`: 保存下一可用编号的计数器。
///
/// 返回: 本次分配的编号。
fn take_generation(next_generation: &mut u64) -> u64 {
    let generation = *next_generation;
    *next_generation = next_generation.saturating_add(1);
    generation
}

/// 业务作用: 展开完整配置中所有代理监听地址，供进程启动就绪检查使用。
///
/// 参数说明:
/// - `config`: 已完成静态校验的活动配置。
///
/// 返回: 所有普通和节点代理监听地址；地址解析失败时返回错误。
fn readiness_addresses(config: &ProxyConfig) -> anyhow::Result<Vec<SocketAddr>> {
    let mut addresses = Vec::new();
    for item in &config.proxies {
        for bind in item.listen_addrs()? {
            addresses.push(bind.parse().with_context(|| {
                format!(
                    "proxy `{}` readiness address `{bind}` is invalid",
                    item.name
                )
            })?);
        }
    }
    Ok(addresses)
}
