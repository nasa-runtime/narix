use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use nasa::application::Application;
use tokio::sync::{mpsc, watch};
use tracing::{info, warn};

use narix::admin;
use narix::coordinator::runtime::StartedCoordinator;
use narix::infrastructure::lifecycle::{
    ensure_clean_exit, supervise, AdmissionGate, CompletionSignal, DomainTask, ManagedShutdown,
    ShutdownControl,
};
use narix::infrastructure::logging::{LogRuntime, ManagedLog};
use narix::reload::{ConfigSource, ConfigSupervisor, LoadedConfig, RuntimeSupervisor};

/// 面向生产环境的高性能 RESP2/3 多 KV 代理。
///
/// 参数由进程入口解析，外部代理配置路径随后交给领域配置加载器处理。
#[derive(Debug, Parser)]
#[command(name = "narix", version)]
#[command(about = "面向生产环境的高性能 RESP2/3 多 KV 代理")]
struct Args {
    /// 可选的代理领域配置文件；优先于 `APP_CONFIG_FILE` 和兼容变量 `NARIX_CONFIG`。
    #[arg(long)]
    config: Option<String>,
}

/// 业务作用: 承载 Application 的运行时，并在统一关闭预算结束后返回进程退出结果。
/// 参数说明: 无。
///
/// 返回: 正常停止且资源排空成功时退出成功；启动或受管运行失败返回非零退出。
fn main() -> anyhow::Result<()> {
    // 定期吸收 I/O 和定时器事件，让响应和控制通知进入运行时。
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .event_interval(47)
        .build()
        .context("create application runtime")?;
    let result = runtime.block_on(run());
    // Application 已完成排空或记录超时，运行时销毁不能再次无期限等待不可取消的文件 I/O。
    // 未完成资源继续作为失败退出；残余阻塞线程由进程结束收回，不能把它们记为排空成功。
    runtime.shutdown_background();
    result
}

/// 业务作用: 完成领域预检后把进程信号、关键任务和资源关闭移交 Application。
/// 参数说明: 无。
///
/// 返回: 初始化、受管运行和资源排空全部成功时返回成功，否则保留启动或关闭错误。
async fn run() -> anyhow::Result<()> {
    let logging = LogRuntime::bootstrap()?;
    let args = Args::parse();
    let source = ConfigSource::from_external(args.config.as_deref());
    let initial = source.load().context("load and validate proxy config")?;
    let configuration = initial.config();
    let runner = narix::infrastructure::lifecycle::application_runner(&configuration)?;
    let exit = runner
        .run(move |application| assemble(application, source, initial, logging))
        .await?;
    for failure in exit.shutdown_failures() {
        tracing::error!(error = %failure, "managed resource shutdown failed");
    }
    ensure_clean_exit(&exit)?;
    Ok(())
}

/// 业务作用: 装配受管领域任务，在集群同步和监听验证完成后允许 Application 发布 Ready。
/// 参数说明:
/// - `application`: 唯一进程生命周期与任务监督对象。
/// - `source`: NARIX 配置事务使用的来源描述。
/// - `initial`: 已完成解析、材料准备及校验的配置候选。
/// - `logging`: 初始化完成的进程日志所有者。
///
/// 返回: 完成所有资源和任务登记后返回，运行期监督由 Application 持续执行。
async fn assemble(
    application: Application,
    source: ConfigSource,
    initial: LoadedConfig,
    logging: Arc<LogRuntime>,
) -> anyhow::Result<()> {
    // 日志先登记，反向资源回收时最后关闭，使任务失败和排空诊断始终可写。
    application.register_managed(ManagedLog(logging.clone()))?;
    let cfg = initial.config();
    // 文件准备可能阻塞，必须先纳入启动取消与资源回收，成功后才创建业务监听和控制任务。
    logging.initialize(&cfg.log).await?;
    let admission = AdmissionGate::new(false);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (apply_tx, apply_rx) = mpsc::channel(1);
    let (reload_handle, reload_task) =
        ConfigSupervisor::spawn(source, initial.clone(), apply_tx, shutdown_rx.clone())?;
    let reload_task = DomainTask::from(reload_task);
    let auth = if cfg.admin.enabled && !cfg.admin.allow_unauthenticated {
        admin::AdminAuth::token(cfg.admin.token.clone().context("admin.token is required")?)
    } else {
        if cfg.admin.enabled {
            warn!("admin HTTP API authentication is disabled");
        }
        admin::AdminAuth::disabled()
    };
    let state = admin::AdminState::ready_with_auth_and_reload(auth, reload_handle.clone());
    state.set_ready(false);
    state.set_logging(logging)?;
    let shutdown = ShutdownControl::new(shutdown_tx, admission.clone(), state.clone());
    application.register_managed(ManagedShutdown(shutdown.clone()))?;
    let (configuration_done, configuration_finished) = watch::channel(false);
    let (runtime_done, runtime_finished) = watch::channel(false);
    let (administration_done, administration_finished) = watch::channel(false);
    supervise(
        &application,
        "configuration",
        reload_task.with_completion(CompletionSignal::new(configuration_done)),
        shutdown.clone(),
    )
    .await?;
    let metrics = state.metrics_source();
    application.register_metrics_source(metrics.cpu_source())?;
    application.register_metrics_source(metrics)?;

    let coordinator = if cfg.coordination.enabled {
        let started = StartedCoordinator::start_with_shutdown_dependencies(
            cfg.coordination.clone(),
            shutdown_rx.clone(),
            vec![
                configuration_finished,
                runtime_finished,
                administration_finished,
            ],
        )
        .await?;
        for (index, task) in started.tasks.into_iter().enumerate() {
            supervise(
                &application,
                format!("coordinator-{index}"),
                task,
                shutdown.clone(),
            )
            .await?;
        }
        Some(started.handle)
    } else {
        None
    };
    if let Some(coordinator) = &coordinator {
        reload_handle.install_coordinator(coordinator.clone());
        coordinator.install_reload_handle(reload_handle).await;
        state.set_coordinator(coordinator.clone());
    }
    let (_readiness, startup, runtime_task) = RuntimeSupervisor::spawn(
        initial.clone(),
        state.clone(),
        coordinator.clone(),
        apply_rx,
        shutdown_rx.clone(),
        admission.clone(),
    )?;
    supervise(
        &application,
        "proxy-runtimes",
        DomainTask::from(runtime_task).with_completion(CompletionSignal::new(runtime_done)),
        shutdown.clone(),
    )
    .await?;
    if let Some(coordinator) = &coordinator {
        // 先恢复集群已提交事实，再允许进程 Ready；Application 本身不能授予 HA 写权。
        coordinator
            .synchronize_startup_cluster_config(
                &initial,
                Duration::from_millis(cfg.reload.transaction_timeout_ms),
            )
            .await?;
    }
    startup
        .await
        .context("proxy runtimes exited before startup completed")?;

    let administration_done = CompletionSignal::new(administration_done);
    if cfg.admin.enabled {
        let listener = tokio::net::TcpListener::bind(&cfg.admin.listen)
            .await
            .context("bind admin listener")?;
        let admin_state = state.clone();
        let control = shutdown.clone();
        application
            .serve_when_ready("administration", move |cancellation| async move {
                let _administration_done = administration_done;
                let serve = admin::serve_listener(listener, admin_state, shutdown_rx);
                tokio::pin!(serve);
                tokio::select! {
                    result = &mut serve => {
                        control.stop();
                        result
                    }
                    _ = cancellation.cancelled() => {
                        control.stop();
                        serve.await
                    }
                }
            })
            .await?;
    } else {
        drop(administration_done);
    }
    let sampling = state.clone();
    application
        .spawn_background("metrics-snapshot", move |cancellation| async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    _ = interval.tick() => {
                        if let Err(error) = sampling.refresh_metrics().await {
                            warn!(error, "metrics snapshot unavailable");
                        }
                    }
                }
            }
        })
        .await?;
    application
        .serve_when_ready("business-admission", move |cancellation| async move {
            let guard = ManagedShutdown(shutdown);
            if !cancellation.is_cancelled() && guard.0.ready() {
                // 所有初始化成功且 Application 已发布 Ready 后才解除进程保护，HA 门禁继续独立生效。
                info!("narix managed services ready");
            }
            cancellation.cancelled().await;
            guard.0.stop();
            Ok(())
        })
        .await?;
    Ok(())
}
