//! 凭据随配置候选准备并与运行代共同持有，认证只借用材料，不回读环境或文件。

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::config::ProxyConfig;
use anyhow::Context;
use nasa::secret::{SecretBytes, SecretSnapshot};
use serde::{Deserialize, Deserializer};

/// UTF-8 凭据共享安全快照，克隆不复制明文，诊断不展示值。
#[derive(Clone)]
pub struct SecretText {
    snapshot: Arc<SecretSnapshot>,
    id: Arc<str>,
}

impl AsRef<str> for SecretText {
    /// 业务作用: 供配置校验借用文本而不复制凭据。
    /// 参数说明: 无。
    ///
    /// 返回: 当前安全快照中的字符串借用。
    fn as_ref(&self) -> &str {
        self
    }
}

impl PartialEq for SecretText {
    /// 业务作用: 比较候选与活动凭据，避免首个差异字节决定比较轮数。
    /// 参数说明:
    /// - `other`: 需要比较的另一份受管凭据。
    ///
    /// 返回: 长度及全部内容相同时为真，不生成明文副本。
    fn eq(&self, other: &Self) -> bool {
        let left = self.as_bytes();
        let right = other.as_bytes();
        let mut difference = left.len() ^ right.len();
        for index in 0..left.len().max(right.len()) {
            difference |= usize::from(
                left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
            );
        }
        difference == 0
    }
}

impl Eq for SecretText {}

impl TryFrom<String> for SecretText {
    type Error = nasa::secret::SecretError;

    /// 业务作用: 将配置中的文本立即转为退出时清零的材料。
    /// 参数说明:
    /// - `text`: 合法 UTF-8 凭据，所有权转交给安全快照。
    ///
    /// 返回: 尚未绑定运行代的受管凭据；超出材料预算时返回脱敏错误并清零已转交字节。
    fn try_from(text: String) -> Result<Self, Self::Error> {
        let mut builder = SecretSnapshot::builder(0);
        builder.insert(Arc::from("value"), SecretBytes::new(text.into_bytes()))?;
        Ok(Self {
            snapshot: Arc::new(builder.build()),
            id: Arc::from("value"),
        })
    }
}

impl<'de> Deserialize<'de> for SecretText {
    /// 业务作用: 在反序列化边界拒绝超大凭据，并移交明文所有权。
    /// 参数说明:
    /// - `deserializer`: 当前配置字段的解析器。
    ///
    /// 返回: 合法文本的安全材料，非法类型或过大输入失败且不包含原值。
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::try_from(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Deref for SecretText {
    type Target = str;
    /// 业务作用: 在认证调用期间借用已经验证过编码的凭据。
    /// 参数说明: 无。
    ///
    /// 返回: 生命周期受快照约束的 UTF-8 文本，不创建明文副本。
    fn deref(&self) -> &str {
        std::str::from_utf8(
            self.snapshot
                .get(&self.id)
                .expect("bound secret exists")
                .expose(),
        )
        .expect("secret originated from UTF-8")
    }
}

impl fmt::Debug for SecretText {
    /// 业务作用: 阻止配置和运行时诊断输出凭据。
    /// 参数说明:
    /// - `formatter`: 诊断输出目标。
    ///
    /// 返回: 固定脱敏标记，不包含值或长度。
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

/// 同一候选中的文件材料只读取一次，摘要和传输准备共享该快照。
#[derive(Debug)]
pub struct Materials {
    snapshot: Arc<SecretSnapshot>,
    files: BTreeMap<PathBuf, Arc<str>>,
}

impl Default for Materials {
    /// 业务作用: 表示尚未完成候选准备的空材料集。
    /// 参数说明: 无。
    ///
    /// 返回: 无文件访问能力的空快照，TLS 使用方必须拒绝缺失材料。
    fn default() -> Self {
        Self {
            snapshot: Arc::new(SecretSnapshot::builder(0).build()),
            files: BTreeMap::new(),
        }
    }
}

impl Materials {
    /// 业务作用: 将所有密码及 TLS 文件绑定到同一候选代，准备失败不改变活动配置。
    /// 参数说明:
    /// - `config`: 尚未发布的强类型配置。
    /// - `paths`: 已去重的 TLS 文件路径。
    ///
    /// 返回: 完整安全快照；读取、预算或身份错误时放弃整份候选。
    pub(crate) fn prepare(
        config: &mut ProxyConfig,
        paths: &[PathBuf],
    ) -> anyhow::Result<Arc<Self>> {
        static GENERATION: AtomicU64 = AtomicU64::new(1);
        let generation = GENERATION
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| anyhow::anyhow!("credential generation exhausted"))?;
        let mut builder = SecretSnapshot::builder(generation);
        let mut files = BTreeMap::new();
        for (index, path) in paths.iter().enumerate() {
            let id: Arc<str> = Arc::from(format!("tls.{index}"));
            let file = std::fs::File::open(path)
                .with_context(|| format!("open credential file `{}`", path.display()))?;
            use std::io::Read;
            let mut bytes = Vec::new();
            let result = file.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes);
            // 先转移安全所有权再检查读取结果，短读失败也必须清除已经进入内存的私钥正文。
            let bytes = SecretBytes::new(bytes);
            result.context("read credential material")?;
            builder.insert(id.clone(), bytes)?;
            files.insert(path.clone(), id);
        }
        let mut entries = Vec::new();
        entries.push(("admin.token".to_string(), &mut config.admin.token));
        for (index, item) in config.proxies.iter_mut().enumerate() {
            entries.push((format!("proxy.{index}.password"), &mut item.password));
            entries.push((
                format!("proxy.{index}.frontend"),
                &mut item.frontend_password,
            ));
            entries.push((
                format!("proxy.{index}.sentinel"),
                &mut item.sentinel_password,
            ));
            if let Some(ha) = &mut item.ha {
                entries.push((
                    format!("proxy.{index}.lease"),
                    &mut ha.coordinator.redis_password,
                ));
            }
        }
        for (id, value) in &entries {
            if let Some(value) = value {
                builder.insert(
                    Arc::from(id.as_str()),
                    SecretBytes::new(value.as_bytes().to_vec()),
                )?;
            }
        }
        let snapshot = Arc::new(builder.build());
        for (id, value) in entries {
            if value.is_some() {
                *value = Some(SecretText {
                    snapshot: snapshot.clone(),
                    id: Arc::from(id),
                });
            }
        }
        let materials = Arc::new(Self { snapshot, files });
        config.coordination.tls.materials = materials.clone();
        for item in &mut config.proxies {
            item.materials = materials.clone();
        }
        Ok(materials)
    }

    /// 业务作用: 为摘要和传输准备借用本候选已读取的文件。
    /// 参数说明:
    /// - `path`: 配置引用的精确路径。
    ///
    /// 返回: 同代材料；缺失时失败，不偷偷回读可能已经变化的文件。
    pub(crate) fn file(&self, path: impl AsRef<Path>) -> anyhow::Result<&[u8]> {
        let id = self
            .files
            .get(path.as_ref())
            .context("TLS material was not prepared for this configuration")?;
        Ok(self
            .snapshot
            .get(id)
            .context("TLS snapshot entry is missing")?
            .expose())
    }
}
