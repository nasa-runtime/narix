use std::collections::BTreeSet;

use serde::Serialize;

use super::LoadedConfig;

/// 表示两个有效配置版本之间按代理稳定名称计算的业务差异。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigDiff {
    /// 新版本新增的代理项名称。
    pub added: Vec<String>,
    /// 新版本修改的代理项名称。
    pub updated: Vec<String>,
    /// 新版本删除的代理项名称。
    pub removed: Vec<String>,
    /// 两个版本中内容摘要完全相同的代理项名称。
    pub unchanged: Vec<String>,
    /// 管理端配置是否发生变化。
    pub admin_changed: bool,
    /// 进程级协调配置是否发生变化。
    pub coordination_changed: bool,
    /// 日志、热更新策略等其他进程级配置是否发生变化。
    pub process_changed: bool,
    /// 本机日志设置是否发生变化。
    pub log_changed: bool,
    /// 配置来源轮询、防抖和事务时限是否发生变化。
    pub reload_changed: bool,
}

impl ConfigDiff {
    /// 业务作用: 比较当前活动版本和候选版本，生成确定性的代理项差异。
    ///
    /// 参数说明:
    /// - `current`: 当前正在提供业务服务的配置版本。
    /// - `candidate`: 已加载并通过静态校验的候选配置版本。
    ///
    /// 返回: 名称按字典序排列的新增、修改、删除和未变项。
    pub fn between(current: &LoadedConfig, candidate: &LoadedConfig) -> Self {
        let current_names: BTreeSet<_> = current.proxy_digests().keys().cloned().collect();
        let candidate_names: BTreeSet<_> = candidate.proxy_digests().keys().cloned().collect();

        let added = candidate_names
            .difference(&current_names)
            .cloned()
            .collect();
        let removed = current_names
            .difference(&candidate_names)
            .cloned()
            .collect();
        let mut updated = Vec::new();
        let mut unchanged = Vec::new();
        for name in current_names.intersection(&candidate_names) {
            if current.proxy_digests().get(name) == candidate.proxy_digests().get(name) {
                unchanged.push(name.clone());
            } else {
                updated.push(name.clone());
            }
        }

        Self {
            added,
            updated,
            removed,
            unchanged,
            admin_changed: current.admin_digest() != candidate.admin_digest(),
            coordination_changed: current.coordination_digest() != candidate.coordination_digest(),
            process_changed: current.process_digest() != candidate.process_digest(),
            log_changed: current.log_digest() != candidate.log_digest(),
            reload_changed: current.reload_digest() != candidate.reload_digest(),
        }
    }

    /// 业务作用: 判断候选版本是否没有任何运行时可观察变化。
    ///
    /// 返回: 代理项、管理端和协调配置全部未变化时返回 `true`。
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.updated.is_empty()
            && self.removed.is_empty()
            && !self.admin_changed
            && !self.coordination_changed
            && !self.process_changed
            && !self.log_changed
            && !self.reload_changed
    }
}
