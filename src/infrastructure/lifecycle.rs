//! Application 管理任务与退出，进程准入与领域配置发布共用一个监听门禁。

use crate::admin::AdminState;
use anyhow::Context;
use nasa::application::{
    Application, ApplicationError, ApplicationExit, ApplicationFuture, ApplicationInfo,
    ApplicationMode, ApplicationPhase, ApplicationRunner, ComponentId, ConfigSnapshot, ConfigView,
    ManagedResource, ShutdownContext,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

/// 领域资源动态子任务的唯一所有者；任务工厂仅持弱引用，避免任务反向保活自身资源树。
#[derive(Debug)]
pub struct TaskScope {
    state: Arc<std::sync::Mutex<ScopedTaskState>>,
    closing: tokio::sync::Mutex<()>,
}

/// 允许资源在自身生命周期内创建子任务的弱句柄，不承担关闭责任。
#[derive(Debug, Clone)]
pub struct TaskSpawner {
    state: std::sync::Weak<std::sync::Mutex<ScopedTaskState>>,
}

#[derive(Debug, Default)]
struct ScopedTaskState {
    tasks: JoinSet<()>,
    closed: bool,
    failed: bool,
}

struct ScopedTaskDrain {
    state: Arc<std::sync::Mutex<ScopedTaskState>>,
    tasks: JoinSet<()>,
    joined: bool,
}

impl Drop for ScopedTaskDrain {
    /// 业务作用: 等待被取消时保留未排空事实，避免后续空集合被误判为正常关闭。
    /// 参数说明: 无。
    ///
    /// 返回: 未完成等待时标记失败，任务集合析构同时撤销剩余子任务。
    fn drop(&mut self) {
        if !self.joined {
            self.state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .failed = true;
        }
    }
}

impl Default for TaskScope {
    /// 业务作用: 创建独立领域资源的子任务集合，使后续创建和最终关闭由同一所有者串行裁决。
    /// 参数说明: 无。
    ///
    /// 返回: 尚未关闭且没有子任务的作用域。
    fn default() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(ScopedTaskState::default())),
            closing: tokio::sync::Mutex::new(()),
        }
    }
}

impl TaskScope {
    /// 业务作用: 向连接池或控制资源分发创建能力，不转移任务集合的最终关闭责任。
    /// 参数说明: 无。
    ///
    /// 返回: 只在本作用域仍存活且未关闭时接受任务的弱句柄。
    pub fn spawner(&self) -> TaskSpawner {
        TaskSpawner {
            state: Arc::downgrade(&self.state),
        }
    }

    /// 业务作用: 回收已经结束的子任务并保留异常事实，供领域监督及时停止使用失效资源。
    /// 参数说明: 无。
    ///
    /// 返回: 没有子任务 panic 或异常取消时成功；已观察的异常不会因后续查询被清除。
    pub fn check(&self) -> anyhow::Result<()> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reap();
        anyhow::ensure!(
            !state.failed,
            "domain resource child task exited abnormally"
        );
        Ok(())
    }

    /// 业务作用: 封闭子任务入口并等待已接纳的外部事务与补偿完成，不受原请求断开影响。
    /// 参数说明: 无。
    ///
    /// 返回: 全部子任务结束且无任务异常时成功；等待受父级统一关闭 deadline 约束。
    pub async fn finish(&self) -> anyhow::Result<()> {
        self.close(false, true).await
    }

    /// 业务作用: 在业务连接已排空后取消连接池等周期任务，并等待其持有的套接字和材料释放。
    /// 参数说明: 无。
    ///
    /// 返回: 全部取消已完成时成功；先前或取消期间的 panic 保留为失败，不重新分配关闭预算。
    pub async fn cancel_and_wait(&self) -> anyhow::Result<()> {
        self.close(true, true).await
    }

    /// 业务作用: 在下一次控制动作开始前等待已登记的释放完成，保留作用域供后续动作继续使用。
    /// 参数说明: 无。
    ///
    /// 返回: 取得等待权时已接纳的任务全部完成后成功；不封闭创建入口，新增任务由下一次屏障等待。
    pub async fn drain_current(&self) -> anyhow::Result<()> {
        self.close(false, false).await
    }

    /// 业务作用: 串行取得任务集合的等待权，最终关闭时同时封闭创建入口，防止遗漏在途任务。
    /// 参数说明:
    /// - `cancel`: 为真时先取消周期任务；为假时允许已接纳事务自行收口。
    /// - `seal`: 为真时永久封闭创建入口；为假时只等待当前集合。
    ///
    /// 返回: 子任务全部析构后成功；父 future 被取消时剩余集合自动撤销，父级负责记录未排空。
    async fn close(&self, cancel: bool, seal: bool) -> anyhow::Result<()> {
        let _closing = self.closing.lock().await;
        let (tasks, mut failed) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reap();
            state.closed |= seal;
            (std::mem::take(&mut state.tasks), state.failed)
        };
        let mut drain = ScopedTaskDrain {
            state: self.state.clone(),
            tasks,
            joined: false,
        };
        if cancel {
            drain.tasks.abort_all();
        }
        while let Some(result) = drain.tasks.join_next().await {
            if let Err(error) = result {
                failed |= !cancel || !error.is_cancelled();
            }
        }
        drain.joined = true;
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.failed |= failed;
            failed = state.failed;
        }
        anyhow::ensure!(!failed, "domain resource child task exited abnormally");
        Ok(())
    }
}

impl Drop for TaskScope {
    /// 业务作用: 所有者异常离开时立即封闭任务入口并撤销剩余执行，避免任务反向保活资源。
    /// 参数说明: 无。
    ///
    /// 返回: 请求取消尚未结束的子任务；正常路径仍须调用显式等待接口。
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        state.tasks.abort_all();
    }
}

impl ScopedTaskState {
    /// 业务作用: 收取已完成任务的等待结果并保存异常，避免动态任务结果无限积累。
    /// 参数说明: 无。
    ///
    /// 返回: 已完成任务从集合移除；异常事实保留到作用域关闭。
    fn reap(&mut self) {
        while let Some(result) = self.tasks.try_join_next() {
            self.failed |= result.is_err();
        }
    }
}

impl TaskSpawner {
    /// 业务作用: 通过弱句柄检查领域任务异常，不让监督任务反向持有任务集合。
    /// 参数说明: 无。
    ///
    /// 返回: 所有者仍存在且尚未观察到异常时成功；所有者已释放或任务异常时失败。
    pub fn check(&self) -> anyhow::Result<()> {
        let owner = self
            .state
            .upgrade()
            .context("domain resource task scope is unavailable")?;
        let mut state = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reap();
        anyhow::ensure!(
            !state.failed,
            "domain resource child task exited abnormally"
        );
        Ok(())
    }

    /// 业务作用: 将必须成功收口的资源释放交给领域所有者，并保留释放失败的关闭结果。
    /// 参数说明:
    /// - `future`: 需要等待且需要传播失败状态的资源收尾动作。
    ///
    /// 返回: 接纳后由结果通道返回动作结果；失败状态同时留存于作用域，不依赖调用方继续等待。
    pub fn spawn_cleanup<F>(
        &self,
        future: F,
    ) -> anyhow::Result<tokio::sync::oneshot::Receiver<anyhow::Result<()>>>
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let state = self.state.clone();
        self.spawn(async move {
            let result = future.await;
            if result.is_err() {
                if let Some(owner) = state.upgrade() {
                    owner
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .failed = true;
                }
            }
            result
        })
    }

    /// 业务作用: 把新领域任务交给原资源作用域，调用方停止等待不会丢失任务的最终回收责任。
    /// 参数说明:
    /// - `future`: 需独立运行但必须由资源所有者最终等待的动作；不得反向持有 TaskScope。
    ///
    /// 返回: 接纳时返回单次结果通道；作用域已释放或封闭时拒绝，任务不会执行。
    pub fn spawn<F, T>(&self, future: F) -> anyhow::Result<tokio::sync::oneshot::Receiver<T>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let owner = self
            .state
            .upgrade()
            .context("domain resource task scope is unavailable")?;
        let mut state = owner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.reap();
        anyhow::ensure!(!state.closed, "domain resource task scope is closed");
        let (result, receiver) = tokio::sync::oneshot::channel();
        state.tasks.spawn(async move {
            let value = future.await;
            let _ = result.send(value);
        });
        Ok(receiver)
    }
}

/// 组合进程与配置就绪状态的业务准入门禁；停止一旦开始便不可重新开放。
#[derive(Clone)]
pub struct AdmissionGate(Arc<Mutex<AdmissionState>>);

struct AdmissionState {
    process_ready: bool,
    stopping: bool,
    configuration_ready: bool,
    effective: watch::Sender<bool>,
}

impl AdmissionGate {
    /// 业务作用: 创建必须同时满足进程和配置就绪的准入门禁。
    /// 参数说明:
    /// - `process_ready`: 初始进程就绪状态，Application 托管启动时必须为假。
    ///
    /// 返回: 只在生命周期和配置变更时加锁的控制句柄，数据面仍订阅原 watch 门禁。
    pub fn new(process_ready: bool) -> Self {
        let (effective, _) = watch::channel(process_ready);
        Self(Arc::new(Mutex::new(AdmissionState {
            process_ready,
            stopping: false,
            configuration_ready: true,
            effective,
        })))
    }

    /// 业务作用: 发布进程准入状态，配置发布不能绕过应用停止或启动门禁。
    /// 参数说明:
    /// - `ready`: Application 是否允许接受业务连接。
    /// - `admin`: 与业务准入同时更新的管理就绪状态。
    ///
    /// 返回: 成功进入就绪态时为真；停止是不可逆状态，迟到的启动回调不能重新开门。
    fn set_process_ready(&self, ready: bool, admin: &AdminState) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stopping |= !ready;
        state.process_ready = ready && !state.stopping;
        admin.set_ready(state.process_ready);
        state
            .effective
            .send_replace(state.process_ready && state.configuration_ready);
        state.process_ready
    }

    /// 业务作用: 在配置发布和回滚窗口切换领域准入，保持进程停止权威优先。
    /// 参数说明:
    /// - `ready`: 当前代理集合是否已完整发布或回滚。
    ///
    /// 返回: 更新监听器看到的组合门禁。
    pub(crate) fn send_replace(&self, ready: bool) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.configuration_ready = ready;
        state
            .effective
            .send_replace(state.process_ready && state.configuration_ready);
    }

    /// 业务作用: 为候选准备步骤查询不可逆的进程停止状态，避免停止后继续准备新运行代。
    /// 参数说明: 无。
    ///
    /// 返回: 已收到停止请求时为真；启动等待和配置保护本身不等同于停止。
    pub(crate) fn is_stopping(&self) -> bool {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stopping
    }

    /// 业务作用: 将配置发布的开始与进程停止按同一门禁原子排序。
    /// 参数说明: 无。
    ///
    /// 返回: 停止已经生效时拒绝发布；否则关闭业务准入并允许该次发布收口。
    pub(crate) fn begin_publication(&self) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // 停止先获得门禁时，候选必须撤销；已开始的发布仍不能覆盖后续停止状态重新开门。
        if state.stopping {
            return false;
        }
        state.configuration_ready = false;
        state.effective.send_replace(false);
        true
    }

    /// 业务作用: 为监听器提供组合就绪订阅，沿用现有无逐命令锁的转发路径。
    /// 参数说明: 无。
    ///
    /// 返回: 当前有效准入状态的 watch 接收端。
    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .effective
            .subscribe()
    }
}

/// 关联业务门禁、管理状态与领域停止信号，确保关闭先撤销准入再通知任务排空。
#[derive(Clone)]
pub struct ShutdownControl {
    sender: watch::Sender<bool>,
    admission: AdmissionGate,
    admin: AdminState,
    incomplete_drain: Arc<AtomicBool>,
}

impl ShutdownControl {
    /// 业务作用: 在 Application 完成启动后尝试开放业务入口，服从不可逆的停止状态。
    /// 参数说明: 无。
    ///
    /// 返回: 未开始关闭且成功发布管理就绪与业务准入时为真。
    pub fn ready(&self) -> bool {
        self.admission.set_process_ready(true, &self.admin)
    }

    /// 业务作用: 将 Application 取消转换为现有领域任务的统一关闭通知。
    /// 参数说明:
    /// - `sender`: 配置、代理及协调任务共用的停止信号。
    /// - `admission`: 进程与配置组合准入门禁。
    /// - `admin`: 管理端基础就绪状态。
    ///
    /// 返回: 不拥有任何新进程监督循环的领域关闭句柄。
    pub fn new(sender: watch::Sender<bool>, admission: AdmissionGate, admin: AdminState) -> Self {
        Self {
            sender,
            admission,
            admin,
            incomplete_drain: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 业务作用: 在取消或关键任务失败时先撤销业务准入，再通知领域任务排空。
    /// 参数说明: 无。
    ///
    /// 返回: 监听器无法因配置完成而重新开放，已有任务收到关闭信号。
    pub fn stop(&self) {
        // 先关组合门禁，再通知异步任务，防止排空期间的配置回调重新接受新连接。
        self.admission.set_process_ready(false, &self.admin);
        self.sender.send_replace(true);
    }
}

/// Application 持有的领域关闭资源，在启动失败或正常停止时保持准入撤销及排空失败事实。
pub struct ManagedShutdown(pub ShutdownControl);

impl ManagedResource for ManagedShutdown {
    /// 业务作用: 在启动失败或资源关闭时确保领域关闭通知已经发出。
    /// 参数说明:
    /// - `_context`: 应用统一关闭上下文，任务等待由监督器承担。
    ///
    /// 返回: 幂等撤销准入并通知所有领域任务；任务被强制撤销而未完成排空时保留关闭失败。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        self.0.stop();
        Box::pin(async move {
            // 任务已被取消不代表排空成功；资源回收阶段必须保留强制结束的事实。
            if self.0.incomplete_drain.load(Ordering::Acquire) {
                return Err(ApplicationError::new(
                    ComponentId::Application,
                    ApplicationPhase::Stopping,
                    "domain tasks were cancelled before drain completed",
                ));
            }
            Ok(())
        })
    }
}

impl Drop for ManagedShutdown {
    /// 业务作用: 外层启动 future 被撤销时也关闭领域任务，避免只释放句柄而留下监听。
    /// 参数说明: 无。
    ///
    /// 返回: 同步关闭准入并广播停止，不阻塞等待任务。
    fn drop(&mut self) {
        self.0.stop();
    }
}

/// 领域子任务的唯一等待权；外层事务取消时同步取消任务，禁止脱离 Application 资源树。
#[derive(Debug)]
pub struct DomainTask<T = anyhow::Result<()>> {
    handle: JoinHandle<T>,
    completion: Option<CompletionSignal>,
}

/// 领域任务的结束通知；正常结束和父任务取消都释放依赖方，异常事实由原监督链保留。
#[derive(Debug)]
pub struct CompletionSignal(watch::Sender<bool>);

impl CompletionSignal {
    /// 业务作用: 接管领域任务的结束通知，使尚未进入运行体的初始化取消也能解除关闭依赖。
    /// 参数说明:
    /// - `sender`: 此任务独占的结束通知发送端。
    ///
    /// 返回: 析构时发布完成通知的守卫；不表示业务动作成功。
    pub fn new(sender: watch::Sender<bool>) -> Self {
        Self(sender)
    }
}

impl Drop for CompletionSignal {
    /// 业务作用: 让后续关闭阶段得知原任务已交出执行权，避免协调资源先于领域任务释放。
    /// 参数说明: 无。
    ///
    /// 返回: 发布结束通知；正常、失败与强取消仍由 Application 的原始退出结果区分。
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

impl<T> DomainTask<T> {
    /// 业务作用: 判断运行代是否已经退出，允许监督器及时释放退休配置和凭据。
    /// 参数说明: 无。
    ///
    /// 返回: 子任务不再运行时为真。
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    /// 业务作用: 撤销领域子任务的执行权，随后仍由持有者等待资源析构完成。
    /// 参数说明: 无。
    ///
    /// 返回: 请求取消尚未结束的任务；不代表任务已经完成，调用方仍须等待句柄。
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// 业务作用: 给领域任务绑定阶段结束通知，供协调传输在依赖领域排空后才开始关闭。
    /// 参数说明:
    /// - `completion`: 此任务独占的阶段结束通知。
    ///
    /// 返回: 保留原任务等待权的句柄，完成等待或所有者撤销时都会通知依赖方。
    pub fn with_completion(mut self, completion: CompletionSignal) -> Self {
        self.completion = Some(completion);
        self
    }
}

impl<T> From<JoinHandle<T>> for DomainTask<T> {
    /// 业务作用: 接管刚启动的领域任务，使注册失败或启动取消也能回收其执行权。
    /// 参数说明:
    /// - `handle`: 尚未转交其他所有者的任务。
    ///
    /// 返回: 带取消兜底的唯一任务句柄。
    fn from(handle: JoinHandle<T>) -> Self {
        Self {
            handle,
            completion: None,
        }
    }
}

impl<T> Future for DomainTask<T> {
    type Output = Result<T, tokio::task::JoinError>;

    /// 业务作用: 等待领域任务退出，同时保持取消时的资源所有权。
    /// 参数说明:
    /// - `context`: 外层监督器的任务唤醒上下文。
    ///
    /// 返回: 子任务完成时提供运行或 join 错误，否则保持等待。
    fn poll(mut self: Pin<&mut Self>, context: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.handle).poll(context);
        if result.is_ready() {
            self.completion.take();
        }
        result
    }
}

impl<T> Drop for DomainTask<T> {
    /// 业务作用: 所有者被撤销时终止未完成任务，阻止监听器和控制副作用脱管。
    /// 参数说明: 无。
    ///
    /// 返回: 同步请求取消，已完成的任务不受影响。
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct OwnedTask {
    handle: DomainTask,
    shutdown: ShutdownControl,
    joined: bool,
}

impl Drop for OwnedTask {
    /// 业务作用: 在监督 future 被取消时保持子任务所有权，不让 JoinHandle 脱离托管。
    /// 参数说明: 无。
    ///
    /// 返回: 撤销准入并终止未完成的子任务；未取得结束结果时记录排空未完成。
    fn drop(&mut self) {
        // 监督 future 被强制撤销时不会经过 join 分支，不能把子任务随之取消当作正常排空。
        if !self.joined {
            self.shutdown
                .incomplete_drain
                .store(true, Ordering::Release);
        }
        self.shutdown.stop();
    }
}

/// 业务作用: 把已经启动的领域任务移交给 Application，统一处理失败、取消和最终回收。
/// 参数说明:
/// - `application`: 当前唯一进程生命周期对象。
/// - `name`: 关键任务的稳定名称。
/// - `handle`: 待移交所有权的领域任务。
/// - `shutdown`: 失败时同步撤销业务准入的控制句柄。
///
/// 返回: 注册成功后由 Application 监督；注册失败也不会遗留脱管任务。
pub async fn supervise(
    application: &Application,
    name: impl Into<Arc<str>>,
    handle: impl Into<DomainTask>,
    shutdown: ShutdownControl,
) -> anyhow::Result<()> {
    let owned = OwnedTask {
        handle: handle.into(),
        shutdown,
        joined: false,
    };
    application
        .spawn_critical(name, move |cancellation| async move {
            let mut owned = owned;
            tokio::select! {
                result = &mut owned.handle => {
                    owned.joined = true;
                    owned.shutdown.stop();
                    result.context("critical domain task join failed")?
                }
                _ = cancellation.cancelled() => {
                    owned.shutdown.stop();
                    let result = (&mut owned.handle).await;
                    owned.joined = true;
                    result.context("domain task shutdown join failed")?
                }
            }
        })
        .await?;
    Ok(())
}

/// 业务作用: 将 Application 的首次终止原因和资源排空结果共同转换为进程退出条件。
/// 参数说明:
/// - `exit`: 已完成任务与资源回收的应用退出报告。
///
/// 返回: 首次退出码为零且无关闭失败时成功；关闭超时或释放失败不能被正常停止原因掩盖。
pub fn ensure_clean_exit(exit: &ApplicationExit) -> anyhow::Result<()> {
    anyhow::ensure!(
        exit.code() == 0,
        "narix managed lifecycle exited with failure"
    );
    anyhow::ensure!(
        exit.shutdown_failures().is_empty(),
        "narix managed lifecycle stopped with incomplete resource shutdown"
    );
    Ok(())
}

/// 业务作用: 将固定进程装配信息交给唯一 Application Runner，领域配置仍由 NARIX 事务发布。
/// 参数说明:
/// - `configuration`: 决定启动同步与全局排空预算的初始领域配置。
///
/// 返回: 已安装进程信号处理和有界启停策略的 Runner；预算非法时拒绝启动。
pub fn application_runner(
    configuration: &crate::config::ProxyConfig,
) -> anyhow::Result<ApplicationRunner> {
    let startup_timeout = Duration::from_millis(configuration.reload.transaction_timeout_ms)
        .saturating_add(Duration::from_secs(10));
    let shutdown_timeout = Duration::from_millis(
        configuration
            .proxies
            .iter()
            .map(|proxy| proxy.graceful_shutdown_timeout_ms)
            .max()
            .unwrap_or(10_000)
            .max(10_000),
    )
    .saturating_add(Duration::from_secs(5));
    // Application 只接收固定进程装配信息；业务树和凭据仍由领域事务唯一发布。
    let view = Arc::new(ConfigView::new(
        Arc::new(ConfigSnapshot::new(
            1,
            serde_json::json!({"application": {"name": "narix", "mode": "service"}}),
            Vec::new(),
        )),
        Default::default(),
    ));
    let runner = ApplicationRunner::new(
        ApplicationInfo::new("narix", None, ApplicationMode::Service)?,
        view,
    )
    .try_startup_timeout(startup_timeout)?
    .try_shutdown_timeout(shutdown_timeout)?
    .with_process_signals();
    Ok(runner)
}
