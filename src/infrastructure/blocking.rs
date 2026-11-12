/// 业务作用: 在多线程运行时同步处理控制面工作时让出 I/O 工作线程，保持调用作用域内的完成与释放顺序。
/// 参数说明:
/// - `work`: 读取或计算控制状态的同步操作，可以借用当前事务数据；返回前不得发布调用方尚未确认的业务状态。
///
/// 返回: 同步返回操作结果并传播 panic，不生成脱离调用作用域的任务；单线程运行时或普通同步调用直接执行。
pub(crate) fn with_blocking_work<F, R>(work: F) -> R
where
    F: FnOnce() -> R,
{
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        // 运行时可以把其余已排队的任务交给其它线程；同步操作仍在本次调用中完成，
        // 不改变事务门禁持有期，也不允许取消后由独立任务继续发布结果。
        tokio::task::block_in_place(work)
    } else {
        // 单线程运行时不能移交工作线程，保留同步 API 的调用顺序与结果。
        work()
    }
}
