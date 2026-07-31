use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use narix::admin;
use narix::coordinator::runtime::StartedCoordinator;
use narix::reload::{ConfigSource, ConfigSupervisor, RuntimeSupervisor};

/// 面向生产环境的高性能 RESP2/3 多 KV 代理。
///
/// 参数由进程入口解析，外部代理配置路径随后交给领域配置加载器处理。
#[derive(Debug, Parser)]
#[command(name = "narix")]
#[command(about = "面向生产环境的高性能 RESP2/3 多 KV 代理")]
struct Args {
    /// 可选的代理领域配置文件；存在时覆盖 `NARIX_CONFIG` 指定的路径。
    #[arg(long)]
    config: Option<String>,
}

/// 业务作用: 初始化日志与异步执行器，然后运行完整代理生命周期。
///
/// 入口保持为独立二进制，不依赖仓库外的进程框架；配置失败、监听失败和运行期子任务失败
/// 都会沿返回值转成非零退出，关闭信号则进入统一排空路径。
///
/// # 返回
///
/// 完成优雅关闭时返回成功；初始化、配置、监听、信号或后台任务失败时返回带上下文的错误。
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "narix=info,info".to_string()),
        )
        .init();
    run_service().await
}

/// 业务作用: 加载代理配置、绑定监听器，并在收到统一取消请求后完成排空。
///
/// 启动期验证每个配置端口真实可连接，运行期任一子任务提前结束都会触发全局关闭。
/// 所有子任务在返回主错误前均先收到关闭通知并完成 join，避免错误路径遗留监听器。
///
/// # 返回
///
/// 收到关闭信号并完成全部任务回收时返回成功；任一启动或运行阶段失败时返回主错误。
async fn run_service() -> anyhow::Result<()> {
    let args = Args::parse();
    let env_config = std::env::var("NARIX_CONFIG").ok();
    let external_config = args.config.as_deref().or(env_config.as_deref());
    let config_source = ConfigSource::from_external(external_config);
    let initial_config = config_source
        .load()
        .context("load and validate proxy config")?;
    let cfg = initial_config.config();

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (apply_tx, apply_rx) = mpsc::channel(1);
    let (reload_handle, reload_task) = ConfigSupervisor::spawn(
        config_source,
        initial_config.clone(),
        apply_tx,
        shutdown_rx.clone(),
    )
    .context("start configuration supervisor")?;
    let admin_auth = if cfg.admin.enabled {
        if cfg.admin.allow_unauthenticated {
            warn!("admin HTTP API authentication is disabled");
            admin::AdminAuth::disabled()
        } else {
            admin::AdminAuth::token(cfg.admin.token.clone().context("admin.token is required")?)
        }
    } else {
        admin::AdminAuth::disabled()
    };
    let state = admin::AdminState::ready_with_auth_and_reload(admin_auth, reload_handle.clone());

    let mut tasks = vec![reload_task];
    let mut readiness_addresses = Vec::new();
    let coordinator = if cfg.coordination.enabled {
        let started = StartedCoordinator::start(cfg.coordination.clone(), shutdown_rx.clone())
            .await
            .context("start process coordinator")?;
        tasks.extend(started.tasks);
        Some(started.handle)
    } else {
        None
    };
    if let Some(coordinator) = &coordinator {
        reload_handle.install_coordinator(coordinator.clone());
        coordinator
            .install_reload_handle(reload_handle.clone())
            .await;
        state.set_coordinator(coordinator.clone());
    }

    let (proxy_readiness, runtime_task) = RuntimeSupervisor::spawn(
        initial_config.clone(),
        state.clone(),
        coordinator.clone(),
        apply_rx,
        shutdown_rx.clone(),
    )
    .context("start proxy runtime supervisor")?;
    tasks.push(runtime_task);

    if let Some(coordinator) = &coordinator {
        let cluster_sync_timeout = Duration::from_millis(cfg.reload.transaction_timeout_ms.max(1));
        if let Err(error) = coordinator
            .synchronize_startup_cluster_config(&initial_config, cluster_sync_timeout)
            .await
        {
            state.set_ready(false);
            let _ = shutdown_tx.send(true);
            for task in tasks {
                let _ = task.await;
            }
            return Err(error).context("synchronize startup cluster configuration");
        }
    }
    readiness_addresses.extend(proxy_readiness.borrow().iter().copied());

    if cfg.admin.enabled {
        let admin_addr: SocketAddr = cfg.admin.listen.parse().with_context(|| {
            format!(
                "admin.listen `{}` is not a valid socket address",
                cfg.admin.listen
            )
        })?;
        readiness_addresses.push(admin_addr);
        tasks.push(tokio::spawn(admin::serve(
            admin_addr,
            state.clone(),
            shutdown_rx.clone(),
        )));
    }

    let shutdown_signal = wait_for_shutdown_signal();
    tokio::pin!(shutdown_signal);
    let mut primary_error = None;
    let mut shutdown_requested = false;
    // 启动探测和操作系统信号必须同时受理：若部署系统在十秒启动窗口内要求退出，不能等到
    // 所有端口就绪后才开始处理信号。作用域结束后 readiness future 被释放，解除对 tasks 的借用。
    let readiness_result = {
        let readiness = wait_until_listeners_ready(&readiness_addresses, &mut tasks);
        tokio::pin!(readiness);
        tokio::select! {
            result = &mut readiness => Some(result),
            result = &mut shutdown_signal => {
                shutdown_requested = true;
                match result {
                    Ok(signal) => info!(signal, "shutdown signal received while listeners were starting"),
                    Err(err) => primary_error = Some(err.context("wait for shutdown signal")),
                }
                None
            }
        }
    };
    if let Some(result) = readiness_result {
        match result {
            Ok(()) => info!(
                proxies = cfg.proxies.len(),
                admin_enabled = cfg.admin.enabled,
                admin_listen = %cfg.admin.listen,
                admin_allow_unauthenticated = cfg.admin.allow_unauthenticated,
                "narix started"
            ),
            Err(err) => {
                error!(error = %err, "narix listener startup failed");
                primary_error = Some(err);
            }
        }
    }

    if primary_error.is_none() && !shutdown_requested {
        tokio::select! {
            result = &mut shutdown_signal => {
                match result {
                    Ok(signal) => info!(signal, "shutdown signal received"),
                    Err(err) => primary_error = Some(err.context("wait for shutdown signal")),
                }
            }
            res = wait_any_task(&mut tasks) => {
                let err = match res {
                    Ok(()) => anyhow::anyhow!("narix child task exited unexpectedly"),
                    Err(err) => err,
                };
                error!(error = %err, "narix task failed");
                primary_error = Some(err);
            }
        }
    }

    state.set_ready(false);
    let _ = shutdown_tx.send(true);

    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => error!(error = %err, "task stopped with error"),
            Err(err) if err.is_cancelled() => {}
            Err(err) => error!(error = %err, "task join error"),
        }
    }

    info!("narix stopped");
    match primary_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// 业务作用: 等待所有配置 listener 接受本机 TCP 连接，同时监督取消请求和子任务提前退出。
///
/// 探测使用固定十秒上限和短连接超时；通配地址会转换为同地址族的回环地址，避免把
/// 仅用于 bind 的未指定地址当作远端目标。子任务在端口就绪前退出会立即作为启动错误。
///
/// # 参数
///
/// - `addresses`：启动配置展开后的全部监听地址，借用覆盖本次探测过程。
/// - `tasks`：已经启动的管理面、数据面与协调任务，借用用于发现提前退出。
///
/// # 返回
///
/// 所有监听地址均可连接时返回成功；超时或任一子任务提前结束时返回错误。
async fn wait_until_listeners_ready(
    addresses: &[SocketAddr],
    tasks: &mut Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut all_ready = true;
        for address in addresses {
            let probe_ip = if address.ip().is_unspecified() {
                if address.is_ipv4() {
                    IpAddr::V4(Ipv4Addr::LOCALHOST)
                } else {
                    IpAddr::V6(Ipv6Addr::LOCALHOST)
                }
            } else {
                address.ip()
            };
            let probe = SocketAddr::new(probe_ip, address.port());
            let connected = tokio::time::timeout(
                Duration::from_millis(150),
                tokio::net::TcpStream::connect(probe),
            )
            .await
            .is_ok_and(|result| result.is_ok());
            all_ready &= connected;
        }
        if all_ready {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("narix listeners did not become ready within 10 seconds");
        }
        tokio::select! {
            result = wait_any_task(tasks), if !tasks.is_empty() => {
                return match result {
                    Ok(()) => Err(anyhow::anyhow!("narix child task exited during startup")),
                    Err(error) => Err(error),
                };
            }
            _ = tokio::time::sleep(Duration::from_millis(25)) => {}
        }
    }
}

/// 业务作用: 等待进程关闭信号，并把平台差异收敛为稳定的信号名称。
///
/// 类 Unix 系统同时处理终端中断与服务管理器常用的终止信号；其他平台使用异步运行库
/// 提供的通用中断入口。信号注册失败必须返回错误，不能让进程进入无法优雅退出的状态。
///
/// # 返回
///
/// 收到支持的信号时返回其稳定名称；注册或等待失败时返回错误。
#[cfg(unix)]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("register termination signal handler")?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("wait for interrupt signal")?;
            Ok("SIGINT")
        }
        result = terminate.recv() => {
            result.context("termination signal stream closed")?;
            Ok("SIGTERM")
        }
    }
}

/// 业务作用: 等待当前平台支持的通用终端中断信号。
///
/// # 返回
///
/// 收到中断时返回稳定名称；信号注册或等待失败时返回错误。
#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> anyhow::Result<&'static str> {
    tokio::signal::ctrl_c()
        .await
        .context("wait for interrupt signal")?;
    Ok("INTERRUPT")
}

/// 业务作用: 等待任一管理面、数据面或协调任务结束，并从待回收集合移除已消费的句柄。
///
/// `JoinHandle` 的输出只能被取走一次；选择完成项后立即移除对应句柄，保证统一停机阶段只
/// join 尚未完成的任务，不会再次轮询已经返回 `Ready` 的 future。任务 panic 被转换成错误链。
///
/// # 参数
///
/// - `tasks`：仍未完成的受监督子任务集合；成功选出一项后集合长度减少一。
///
/// # 返回
///
/// 返回最先完成任务的业务结果；任务发生 panic 或取消时转换为带上下文的错误。
async fn wait_any_task(
    tasks: &mut Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
) -> anyhow::Result<()> {
    if tasks.is_empty() {
        return Ok(());
    }
    let (res, index, remaining) = futures_util::future::select_all(tasks.iter_mut()).await;
    // 剩余引用仍借用集合；先释放引用，再移除已经消费输出的完成句柄。
    drop(remaining);
    drop(tasks.swap_remove(index));
    match res {
        Ok(inner) => inner,
        Err(err) => Err(anyhow::anyhow!("task join error: {err}")),
    }
}
