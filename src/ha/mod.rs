//! 托管复制 HA 的通用模型、后端契约、控制连接和产品适配器。

pub(crate) mod backend;
pub(crate) mod backends;
pub(crate) mod container;
pub(crate) mod control_client;
pub(crate) mod driver;
pub(crate) mod model;
pub(crate) mod persistence;
pub(crate) mod protocol;
pub(crate) mod routing;
pub(crate) mod runtime;
pub(crate) mod topology;
