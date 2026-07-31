//! NARIX 全模式配置热更新的来源监控、差异计算、事务状态和运行时发布。

mod cluster_store;
mod diff;
mod proxy_generation;
mod source;
mod status;
mod supervisor;

/// 集群托管配置正文允许持久化、复制和恢复的最大字节数。
pub(crate) const MAX_CLUSTER_CONFIG_BYTES: usize = 16 * 1024 * 1024;

pub use cluster_store::{ClusterConfigStore, PersistedClusterConfig};
pub use diff::ConfigDiff;
pub use proxy_generation::RuntimeSupervisor;
pub use source::{ConfigSource, LoadedConfig};
pub(crate) use status::ReloadTransactionId;
pub use status::{ReloadPhase, ReloadResultKind, ReloadSnapshot, ReloadState, ReloadTrigger};
pub(crate) use supervisor::ClusterProposalCandidate;
pub use supervisor::{
    ApplyConfigRequest, ApplyMode, ApplyOrigin, ConfigSupervisor, ReloadHandle,
    ReloadOperationResponse,
};
