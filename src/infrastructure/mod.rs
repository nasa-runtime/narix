//! 通用设施在进程装配和配置事务中准备，业务运行代直接持有已经准备的资源。

pub(crate) mod blocking;
pub mod configuration;
pub mod credentials;
pub(crate) mod identity;
pub mod lifecycle;
pub mod logging;
mod metric_descriptors;
pub mod metrics;
pub mod time;
