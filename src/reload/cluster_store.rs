use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::MAX_CLUSTER_CONFIG_BYTES;

/// 指向一个已经提交并可在重启时恢复的集群配置版本。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedClusterConfig {
    /// 协调日志确认的单调递增配置版本。
    pub revision: u64,
    /// 规范化配置载荷的 SHA-256 摘要。
    pub digest: String,
    /// 从已提交文件读取并校验通过的规范化配置正文。
    #[serde(skip)]
    pub payload: Vec<u8>,
}

/// 以受限权限、文件同步和原子 rename 持久化集群候选及活动配置。
#[derive(Debug, Clone)]
pub struct ClusterConfigStore {
    root: PathBuf,
}

impl ClusterConfigStore {
    /// 业务作用: 在协调数据目录下创建受保护的配置版本目录。
    ///
    /// 参数说明:
    /// - `coordination_data_dir`: 当前节点独占的协调状态持久化目录。
    ///
    /// 返回: 目录存在且权限收敛为 `0700` 时返回配置存储。
    pub fn open(coordination_data_dir: &Path) -> anyhow::Result<Self> {
        let root = coordination_data_dir.join("config");
        fs::create_dir_all(&root).with_context(|| {
            format!(
                "create cluster configuration directory `{}`",
                root.display()
            )
        })?;
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!(
                "set cluster configuration directory permissions `{}`",
                root.display()
            )
        })?;
        sync_directory(&root)?;
        Ok(Self { root })
    }

    /// 业务作用: 将当前成员尚未提交的候选配置安全写入本地磁盘。
    ///
    /// 参数说明:
    /// - `revision`: 领导节点分配的候选配置版本。
    /// - `digest`: 领导节点广播的规范化载荷摘要。
    /// - `payload`: 通过受保护 gRPC 通道收到的规范化配置正文。
    ///
    /// 返回: 摘要匹配且候选文件完成原子落盘和目录同步时返回成功。
    pub fn prepare(&self, revision: u64, digest: &str, payload: &[u8]) -> anyhow::Result<()> {
        validate_version(revision, digest)?;
        if payload.is_empty() || payload.len() > MAX_CLUSTER_CONFIG_BYTES {
            bail!("cluster configuration payload must be between 1 byte and 16 MiB");
        }
        let actual = sha256_hex(payload);
        if actual != digest {
            bail!("cluster configuration payload digest mismatch for revision {revision}");
        }
        let candidate = self.candidate_path(revision, digest);
        match read_limited(&candidate) {
            Ok(existing) if sha256_hex(&existing) == digest => return Ok(()),
            Ok(_) => bail!(
                "cluster configuration revision {revision} candidate digest does not match its file name"
            ),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error),
        }
        write_atomic(&candidate, payload)?;
        Ok(())
    }

    /// 业务作用: 将已经由协调日志确认的候选版本转成不可误认的已提交文件。
    ///
    /// 参数说明:
    /// - `revision`: 协调日志已经提交的配置版本。
    /// - `digest`: 协调日志已经提交的规范化载荷摘要。
    ///
    /// 返回: 候选存在、内容摘要一致并完成原子 rename 时返回成功。
    pub fn commit(&self, revision: u64, digest: &str) -> anyhow::Result<()> {
        validate_version(revision, digest)?;
        let committed = self.committed_path(revision, digest);
        if committed.exists() {
            let payload = read_limited(&committed)?;
            if sha256_hex(&payload) != digest {
                bail!("persisted committed digest mismatch for revision {revision}");
            }
            return self.remove_candidate_files(revision);
        }
        let candidate = self.candidate_path(revision, digest);
        let payload = read_limited(&candidate)?;
        if sha256_hex(&payload) != digest {
            bail!("persisted candidate digest mismatch for revision {revision}");
        }
        fs::rename(&candidate, &committed).with_context(|| {
            format!(
                "commit cluster configuration `{}` as `{}`",
                candidate.display(),
                committed.display()
            )
        })?;
        self.remove_candidate_files(revision)
    }

    /// 业务作用: 原子更新活动指针，使进程重启时只恢复协调日志已确认的配置。
    ///
    /// 参数说明:
    /// - `revision`: 已经完成本地运行代发布的配置版本。
    /// - `digest`: 已经完成本地运行代发布的配置摘要。
    ///
    /// 返回: 已提交文件存在且摘要一致后，活动指针完成同步时返回成功。
    pub fn activate(&self, revision: u64, digest: &str) -> anyhow::Result<()> {
        validate_version(revision, digest)?;
        let committed = self.committed_path(revision, digest);
        let payload = read_limited(&committed)?;
        if sha256_hex(&payload) != digest {
            bail!("committed cluster configuration digest mismatch for revision {revision}");
        }
        if let Some(active) = self.load_active()? {
            if active.revision > revision {
                bail!(
                    "cannot move active cluster configuration backward from revision {} to {revision}",
                    active.revision
                );
            }
            if active.revision == revision {
                if active.digest != digest {
                    bail!("active cluster configuration revision {revision} has another digest");
                }
                return self.retain_latest_committed(2);
            }
        }
        let pointer = PersistedClusterConfig {
            revision,
            digest: digest.to_string(),
            payload: Vec::new(),
        };
        let bytes = serde_json::to_vec(&pointer)
            .context("serialize active cluster configuration pointer")?;
        write_atomic(&self.root.join("active.json"), &bytes)?;
        self.retain_latest_committed(2)
    }

    /// 业务作用: 删除提交前已经明确中止的候选配置文件。
    ///
    /// 参数说明:
    /// - `revision`: 要放弃的候选配置版本。
    /// - `digest`: 发起中止的事务摘要，必须与磁盘候选正文匹配。
    ///
    /// 返回: 文件不存在或对应事务候选删除并同步成功时返回成功；摘要不符时保留候选。
    pub fn abort(&self, revision: u64, digest: &str) -> anyhow::Result<()> {
        validate_version(revision, digest)?;
        let candidate = self.candidate_path(revision, digest);
        let payload = match read_limited(&candidate) {
            Ok(payload) => payload,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if sha256_hex(&payload) != digest {
            bail!(
                "candidate digest does not match aborted cluster configuration revision {revision}"
            );
        }
        self.remove_candidate_file(revision, digest)
    }

    /// 业务作用: 删除一个已经确认可以清理的候选文件并同步目录项。
    ///
    /// 参数说明:
    /// - `revision`: 候选文件名使用的已校验配置版本。
    /// - `digest`: 候选文件名使用的已校验配置摘要。
    ///
    /// 返回: 文件不存在或删除并同步成功时返回成功。
    fn remove_candidate_file(&self, revision: u64, digest: &str) -> anyhow::Result<()> {
        let candidate = self.candidate_path(revision, digest);
        match fs::remove_file(&candidate) {
            Ok(()) => sync_directory(&self.root),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!(
                    "remove aborted cluster configuration `{}`",
                    candidate.display()
                )
            }),
        }
    }

    /// 业务作用: 删除指定 revision 下已经被提交结果淘汰的全部候选摘要文件。
    ///
    /// 参数说明:
    /// - `revision`: 已经提交、无需再保留其他候选摘要的配置版本。
    ///
    /// 返回: 同 revision 候选全部删除并同步目录项时返回成功。
    fn remove_candidate_files(&self, revision: u64) -> anyhow::Result<()> {
        let prefix = format!("candidate-{revision}-");
        let mut removed = false;
        for entry in fs::read_dir(&self.root).with_context(|| {
            format!(
                "list cluster configuration directory `{}`",
                self.root.display()
            )
        })? {
            let entry = entry.with_context(|| {
                format!(
                    "read cluster configuration directory entry `{}`",
                    self.root.display()
                )
            })?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !name.starts_with(&prefix) || !name.ends_with(".json") {
                continue;
            }
            fs::remove_file(entry.path()).with_context(|| {
                format!(
                    "remove superseded cluster configuration candidate `{}`",
                    entry.path().display()
                )
            })?;
            removed = true;
        }
        if removed {
            sync_directory(&self.root)?;
        }
        Ok(())
    }

    /// 业务作用: 读取并验证当前活动集群配置，供进程重启恢复。
    ///
    /// 返回: 尚未提交任何集群配置时返回 `None`；活动指针或内容损坏时返回错误。
    pub fn load_active(&self) -> anyhow::Result<Option<PersistedClusterConfig>> {
        let pointer_path = self.root.join("active.json");
        let pointer_bytes = match read_limited(&pointer_path) {
            Ok(bytes) => bytes,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound) =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut pointer: PersistedClusterConfig = serde_json::from_slice(&pointer_bytes)
            .context("parse active cluster configuration pointer")?;
        validate_version(pointer.revision, &pointer.digest)?;
        let committed = self.committed_path(pointer.revision, &pointer.digest);
        let payload = read_limited(&committed)?;
        if sha256_hex(&payload) != pointer.digest {
            bail!(
                "active cluster configuration digest mismatch for revision {}",
                pointer.revision
            );
        }
        pointer.payload = payload;
        Ok(Some(pointer))
    }

    /// 业务作用: 读取协调日志指定的已提交配置正文，供提交后崩溃但活动指针尚未更新的节点恢复。
    ///
    /// 参数说明:
    /// - `revision`: 协调状态机已经提交的配置版本。
    /// - `digest`: 协调状态机已经提交的规范化配置摘要。
    ///
    /// 返回: 指定已提交文件存在且摘要匹配时返回完整载荷。
    pub fn load_committed(
        &self,
        revision: u64,
        digest: &str,
    ) -> anyhow::Result<PersistedClusterConfig> {
        validate_version(revision, digest)?;
        let committed = self.committed_path(revision, digest);
        let payload = read_limited(&committed)?;
        if sha256_hex(&payload) != digest {
            bail!("committed cluster configuration digest mismatch for revision {revision}");
        }
        Ok(PersistedClusterConfig {
            revision,
            digest: digest.to_string(),
            payload,
        })
    }

    /// 业务作用: 生成一个候选版本的固定文件路径。
    ///
    /// 参数说明:
    /// - `revision`: 领导节点分配的候选配置版本。
    /// - `digest`: 已校验为规范 SHA-256 的候选载荷摘要。
    ///
    /// 返回: 当前存储目录内不含用户输入片段的候选路径。
    fn candidate_path(&self, revision: u64, digest: &str) -> PathBuf {
        self.root
            .join(format!("candidate-{revision}-{digest}.json"))
    }

    /// 业务作用: 生成一个已提交版本的固定文件路径。
    ///
    /// 参数说明:
    /// - `revision`: 协调日志提交的配置版本。
    /// - `digest`: 已校验为规范 SHA-256 的配置摘要。
    ///
    /// 返回: 当前存储目录内包含版本和摘要的已提交路径。
    fn committed_path(&self, revision: u64, digest: &str) -> PathBuf {
        self.root
            .join(format!("committed-{revision}-{digest}.json"))
    }

    /// 业务作用: 只保留最近若干个已提交版本，候选和活动指针不参与清理。
    ///
    /// 参数说明:
    /// - `keep`: 至少需要保留的已提交版本数量。
    ///
    /// 返回: 旧文件删除并同步完成时返回成功。
    fn retain_latest_committed(&self, keep: usize) -> anyhow::Result<()> {
        let mut committed = fs::read_dir(&self.root)
            .with_context(|| {
                format!(
                    "list cluster configuration directory `{}`",
                    self.root.display()
                )
            })?
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = entry.file_name().into_string().ok()?;
                let revision = name
                    .strip_prefix("committed-")?
                    .split('-')
                    .next()?
                    .parse::<u64>()
                    .ok()?;
                Some((revision, entry.path()))
            })
            .collect::<Vec<_>>();
        committed.sort_by_key(|(revision, _)| *revision);
        let remove_count = committed.len().saturating_sub(keep.max(1));
        for (_, path) in committed.into_iter().take(remove_count) {
            fs::remove_file(&path).with_context(|| {
                format!(
                    "remove old committed cluster configuration `{}`",
                    path.display()
                )
            })?;
        }
        sync_directory(&self.root)
    }
}

/// 业务作用: 校验集群配置版本和摘要可安全用于固定文件名及一致性比较。
///
/// 参数说明:
/// - `revision`: 候选或已提交的配置版本。
/// - `digest`: 规范化载荷 SHA-256 摘要。
///
/// 返回: revision 大于零且摘要为六十四位小写十六进制时返回成功。
fn validate_version(revision: u64, digest: &str) -> anyhow::Result<()> {
    if revision == 0 {
        bail!("cluster configuration revision must be > 0");
    }
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("cluster configuration digest must be lowercase SHA-256");
    }
    Ok(())
}

/// 业务作用: 在目标目录内以受限权限写临时文件，完成文件同步后原子替换目标并同步目录。
///
/// 参数说明:
/// - `path`: 最终活动、候选或元数据文件路径。
/// - `bytes`: 可能包含凭据、不得进入日志的文件正文。
///
/// 返回: 文件内容和目录项都完成同步时返回成功。
fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .context("cluster configuration target has invalid file name")?;
    let temporary = path.with_file_name(format!(".{file_name}.tmp"));
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| {
            format!(
                "create cluster configuration temporary file `{}`",
                temporary.display()
            )
        })?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "atomically publish cluster configuration file `{}`",
            path.display()
        )
    })?;
    let parent = path
        .parent()
        .context("cluster configuration target has no parent directory")?;
    sync_directory(parent)
}

/// 业务作用: 读取大小受限的集群配置文件，防止损坏元数据导致无界内存分配。
///
/// 参数说明:
/// - `path`: 候选、已提交正文或活动指针路径。
///
/// 返回: 不超过十六 MiB 的完整文件内容。
fn read_limited(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut file = File::open(path)
        .with_context(|| format!("open cluster configuration `{}`", path.display()))?;
    let metadata = file.metadata()?;
    if metadata.len() > MAX_CLUSTER_CONFIG_BYTES as u64 {
        bail!(
            "cluster configuration `{}` exceeds size limit",
            path.display()
        );
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

/// 业务作用: 同步配置目录项，确保 rename 和删除在节点异常重启后保持一致。
///
/// 参数说明:
/// - `path`: 需要持久化目录项变更的配置目录。
///
/// 返回: 目录文件描述符完成 `sync_all` 时返回成功。
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    File::open(path)
        .with_context(|| format!("open directory `{}` for sync", path.display()))?
        .sync_all()
        .with_context(|| format!("sync directory `{}`", path.display()))
}

/// 业务作用: 计算规范化配置正文的 SHA-256 小写十六进制摘要。
///
/// 参数说明:
/// - `bytes`: 需要校验的配置正文。
///
/// 返回: 六十四位小写十六进制摘要。
fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
