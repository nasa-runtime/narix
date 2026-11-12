//! 只在导出侧发布快照，RESP 热路径继续更新原来的原子计数单元。

use super::metric_descriptors::{
    CPU_DESCRIPTOR, CPU_DESCRIPTORS, DESCRIPTORS, STRUCTURED_DESCRIPTORS,
};
use nametrics_core::{LegacyMetricsSource, MetricDescriptor, MetricSample, MetricValue};
use std::sync::{Arc, RwLock};

pub(crate) type PreparedMetricsSnapshot = Result<Arc<Vec<MetricSample>>, &'static str>;

/// 共享最近一次完整结构化指标快照，采集超限时拒绝整个快照而不发布部分指标。
pub struct NarixMetricsSource {
    samples: RwLock<PreparedMetricsSnapshot>,
}

impl Default for NarixMetricsSource {
    /// 业务作用: 建立尚未采样的指标源，不把初始化期间的缺失状态伪装成有效快照。
    /// 参数说明: 无。
    ///
    /// 返回: 首次完整采样前导出明确不可用。
    fn default() -> Self {
        Self {
            samples: RwLock::new(Err("metrics snapshot is not available")),
        }
    }
}

impl NarixMetricsSource {
    /// 业务作用: 为 CPU 累计秒数提供独立文本源，保持小数精度与 Prometheus counter 类型。
    /// 参数说明: 无。
    ///
    /// 返回: 共享本源快照的文本桥；整数 counter 的结构化接口不接收该浮点指标。
    pub fn cpu_source(self: &Arc<Self>) -> Arc<dyn LegacyMetricsSource> {
        Arc::new(CpuMetricsSource(self.clone()))
    }

    /// 业务作用: 在采集任务内校验完整样本预算，使编码和发布只接收完整有效快照。
    /// 参数说明:
    /// - `samples`: 本轮采集的完整样本，包括当前活动标签。
    ///
    /// 返回: 样本满足预算且 CPU 累计秒数有效时生成不可变快照；否则返回固定错误。
    pub(crate) fn prepare(samples: Vec<MetricSample>) -> PreparedMetricsSnapshot {
        // 为 Application 自身指标留出目录余量；只在采集端施加预算，不给命令路径增加判断。
        let valid = samples.iter().all(|sample| {
            sample.name != CPU_DESCRIPTOR.name
                || (matches!(sample.value, MetricValue::Gauge(value) if value.is_finite() && value >= 0.0)
                    && matches!(sample.labels.as_slice(), [("mode", mode)] if mode == "user" || mode == "system"))
        }) && samples.len() <= 90_000
            && samples
                .iter()
                .flat_map(|sample| &sample.labels)
                .all(|(_, value)| value.len() <= 4096)
            && samples
                .iter()
                .flat_map(|sample| &sample.labels)
                .try_fold(0usize, |total, (_, value)| {
                    total
                        .checked_add(value.len())
                        .filter(|total| *total <= 16 * 1024 * 1024)
                })
                .is_some();
        if valid {
            Ok(Arc::new(samples))
        } else {
            Err("metrics snapshot exceeds budget or contains an invalid CPU counter")
        }
    }

    /// 业务作用: 原子发布本轮准备结果，使 Application 和管理出口共享相同指标来源。
    /// 参数说明:
    /// - `next`: 已完成预算校验的完整快照或本轮不可用原因。
    ///
    /// 返回: 有效快照整体发布并移除退休标签；超限结果撤销旧快照，出口拒绝而非截断指标族。
    pub(crate) fn publish(&self, next: PreparedMetricsSnapshot) -> Result<(), &'static str> {
        let result = next.as_ref().map(|_| ()).map_err(|error| *error);
        *self
            .samples
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
        result
    }

    /// 业务作用: 为原管理端路径生成标准 Prometheus 文本。
    /// 参数说明:
    /// - `descriptors`: 本出口拥有的指标族，决定文本与结构化出口的边界。
    ///
    /// 返回: 同一来源的选定指标族编码；尚无有效快照或超限时返回固定错误，不泄露标签。
    pub(crate) fn render(&self, descriptors: &[&MetricDescriptor]) -> Result<String, &'static str> {
        let samples = self
            .samples
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let samples = samples.as_ref().map_err(|error| *error)?;
        let mut output = String::new();
        nametrics_core::atomic::render_snapshot(descriptors, samples, &mut output);
        Ok(output)
    }

    /// 业务作用: 编码同一轮已校验的不可变快照，允许管理请求在发布前完成文本准备。
    /// 参数说明:
    /// - `samples`: 完整快照或预算校验失败原因。
    ///
    /// 返回: 有效快照的标准 Prometheus 文本；无效快照直接拒绝，不分配部分响应。
    pub(crate) fn render_prepared(
        samples: &PreparedMetricsSnapshot,
    ) -> Result<String, &'static str> {
        let samples = samples.as_ref().map_err(|error| *error)?;
        let mut output = String::new();
        nametrics_core::atomic::render_snapshot(DESCRIPTORS, samples, &mut output);
        Ok(output)
    }
}

impl LegacyMetricsSource for NarixMetricsSource {
    /// 业务作用: 提供启动时固定的指标合同，供全局目录检查重名与类型冲突。
    /// 参数说明: 无。
    ///
    /// 返回: 可结构化表达的 NARIX 指标描述符；CPU 累计秒数由独立文本源登记。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        &STRUCTURED_DESCRIPTORS
    }

    /// 业务作用: 向 Application 的结构化导出提供最近一次完整采集。
    /// 参数说明: 无。
    ///
    /// 返回: 当前快照副本；没有采集时为空，不伪造业务状态。
    fn snapshot(&self) -> Option<Vec<MetricSample>> {
        let samples = self
            .samples
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        Some(
            samples
                .map(|samples| {
                    samples
                        .iter()
                        .filter(|sample| sample.name != CPU_DESCRIPTOR.name)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
        )
    }

    /// 业务作用: 用统一编码器输出当前来源，不在锁内执行文本分配。
    /// 参数说明:
    /// - `output`: 需要追加完整指标族的缓冲区。
    ///
    /// 返回: 追加标准 HELP、TYPE 和样本，不修改计数。
    fn render_prometheus(&self, output: &mut String) {
        if let Ok(rendered) = self.render(&STRUCTURED_DESCRIPTORS) {
            output.push_str(&rendered);
        }
    }
}

/// 共享同一快照的浮点累计秒数文本出口，不伪造整数精度或 gauge 语义。
struct CpuMetricsSource(Arc<NarixMetricsSource>);

impl LegacyMetricsSource for CpuMetricsSource {
    /// 业务作用: 将 CPU 指标的唯一 counter 合同登记到统一目录。
    /// 参数说明: 无。
    ///
    /// 返回: 仅包含 CPU 累计秒数的描述符。
    fn descriptors(&self) -> &'static [&'static MetricDescriptor] {
        CPU_DESCRIPTORS
    }

    /// 业务作用: 按 counter 合同导出同一采集轮次的 CPU 浮点秒数。
    /// 参数说明:
    /// - `output`: 接收 CPU 指标的 Prometheus 文本。
    ///
    /// 返回: 有有效快照时追加 CPU 样本；不可用时不伪造零值。
    fn render_prometheus(&self, output: &mut String) {
        if let Ok(rendered) = self.0.render(CPU_DESCRIPTORS) {
            output.push_str(&rendered);
        }
    }
}
