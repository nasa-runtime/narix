//! 日志候选随领域配置事务准备和安装，文件守卫保持到业务任务停止之后。

use anyhow::Context;
use nasa::application::{
    ApplicationError, ApplicationFuture, ApplicationPhase, ComponentId, ManagedResource,
    ShutdownContext,
};
use nasa::log::{LogConfig, LogContext, LogManager, PreparedLogConfig};
use std::sync::{Arc, Mutex};

/// 进程唯一日志管理器，承接配置候选并持有输出资源直到业务收尾完成。
pub struct LogRuntime {
    manager: Mutex<Option<LogManager>>,
}

impl LogRuntime {
    /// 业务作用: 安装进程唯一 subscriber，为配置加载和启动失败提供日志出口。
    /// 参数说明: 无。
    ///
    /// 返回: 由 Application 最后释放的日志管理器；非法初始配置拒绝启动。
    pub fn bootstrap() -> anyhow::Result<Arc<Self>> {
        let configuration = LogConfig {
            level: crate::config::DEFAULT_LOG_LEVEL.to_string(),
            ..LogConfig::default()
        };
        Ok(Arc::new(Self {
            manager: Mutex::new(Some(LogManager::try_bootstrap(Some(&configuration))?)),
        }))
    }

    /// 业务作用: 在阻塞线程上预备过滤器、文件和轮转资源，活动输出不变。
    /// 参数说明:
    /// - `configuration`: 候选配置中完整的日志设置。
    ///
    /// 返回: 尚未安装的候选；失败时保留当前输出并释放新资源。
    pub async fn prepare(
        self: &Arc<Self>,
        configuration: &LogConfig,
    ) -> anyhow::Result<PreparedLogConfig> {
        let runtime = self.clone();
        let configuration = configuration.clone();
        nasa::application::read_config_candidate(move || {
            runtime
                .manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .context("logging is closing")?
                .prepare(&configuration, &LogContext::with_app_name("narix"))
                .map_err(anyhow::Error::from)
        })
        .await
        .context("prepare logging task failed")?
    }

    /// 业务作用: 在领域发布门禁内安装已准备的日志资源，不执行文件读取或旧守卫排空。
    /// 参数说明:
    /// - `prepared`: 本次候选独占的准备对象，调用方在发布窗口之外释放。
    ///
    /// 返回: 成功后后续日志使用新输出；安装失败保留旧输出。
    pub(crate) fn install(&self, prepared: &mut PreparedLogConfig) -> anyhow::Result<()> {
        self.manager
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
            .context("logging is closing")?
            .install(prepared)?;
        Ok(())
    }

    /// 业务作用: 初始化业务装配前完整应用日志设置。
    /// 参数说明:
    /// - `configuration`: 已加载并校验的初始日志配置。
    ///
    /// 返回: 安装成功后允许启动业务任务，失败停止启动。
    pub async fn initialize(self: &Arc<Self>, configuration: &LogConfig) -> anyhow::Result<()> {
        let mut prepared = self.prepare(configuration).await?;
        self.install(&mut prepared)
    }
}

impl Drop for LogRuntime {
    /// 业务作用: 覆盖进入 Application 之前的失败路径，释放仍归当前进程持有的日志资源。
    /// 参数说明: 无。
    ///
    /// 返回: 尚未关闭的文件守卫完成排空；正常受管关闭时不重复执行。
    fn drop(&mut self) {
        if let Some(mut manager) = self
            .manager
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            manager.shutdown();
        }
    }
}

/// Application 最后释放的日志资源，使领域关闭期间的诊断仍能完成输出。
pub struct ManagedLog(pub Arc<LogRuntime>);

impl ManagedResource for ManagedLog {
    /// 业务作用: 业务资源释放后关闭日志 writer 和回收线程，确保关闭诊断仍有输出。
    /// 参数说明:
    /// - `_context`: Application 全局关闭预算，由外层统一执行超时边界。
    ///
    /// 返回: writer 排空完成时成功；锁等待与文件 I/O 都受应用剩余关闭预算监督。
    fn shutdown<'a>(&'a mut self, _context: &'a ShutdownContext) -> ApplicationFuture<'a> {
        let runtime = self.0.clone();
        Box::pin(async move {
            // 候选准备可能仍占有管理器；锁等待也必须离开异步关闭路径，避免阻塞统一超时判定。
            tokio::task::spawn_blocking(move || {
                let manager = runtime
                    .manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(mut manager) = manager {
                    manager.shutdown();
                }
            })
            .await
            .map_err(|error| {
                ApplicationError::with_source(
                    ComponentId::Log,
                    ApplicationPhase::Stopping,
                    "logging drain failed",
                    error,
                )
            })?;
            Ok(())
        })
    }
}
