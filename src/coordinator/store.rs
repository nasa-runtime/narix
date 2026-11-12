use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor, Read, Write};
use std::ops::RangeBounds;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{bail, Context};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, OptionalSend, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::state::{
    CoordinationCommand, CoordinationGroupState, CoordinationResponse, CoordinationState,
};
use super::CoordinatorTypeConfig;

/// 业务作用: 固化首次启动时的集群身份、协议和成员表，后续启动必须逐项一致，防止误改配置造成两个互不兼容的集群同时控制同一后端。
/// 字段说明: 成员按节点编号排序后保存，比较时不受 yml 中书写顺序影响。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoordinatorGenesis {
    /// 首次创建后不可改变的协调集群业务标识。
    pub cluster_id: String,
    /// 首次创建后不可改变的双节点或多数派协议名称。
    pub protocol: String,
    /// 按稳定节点编号保存且首次创建后不可改变的成员地址表。
    pub members: BTreeMap<u64, String>,
    /// 所有成员必须一致的超时、身份表和隔离策略规范摘要。
    #[serde(default)]
    pub configuration_digest: String,
}

/// 业务作用: 保存已经应用到业务状态机的日志位置、成员关系和各托管复制 HA 组状态。
/// 字段说明: 该对象与任期、日志一起原子落盘，重启后不会先暴露尚未持久化的路由。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedStateMachine {
    last_applied_log_id: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    state: CoordinationState,
}

/// 业务作用: 保存可被发送给落后成员的状态机快照及其共识元数据。
/// 字段说明: `data` 是 `CoordinationState` 的稳定 JSON 编码，不包含进程内锁或临时计时器。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

/// 业务作用: 保存双节点同一任期内经过比较后的优胜候选，纳秒值只参与排序，不单独构成领导权。
/// 字段说明: 节点编号和启动实例共同防止编号相同的旧进程复用当前任期身份。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedPairCandidate {
    node_id: u64,
    election_time_ns: u64,
    startup_nonce: String,
}

/// 业务作用: 保存双节点哈希链中的一条已落盘业务记录。
/// 字段说明: 记录同时保存原始领导节点和启动实例；换届后补提旧记录时仍以原始身份生成确定的状态机日志编号。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedPairRecord {
    term: u64,
    sequence: u64,
    leader_node_id: u64,
    leader_startup_nonce: String,
    previous_hash: String,
    record_hash: String,
    payload: Vec<u8>,
}

/// 业务作用: 保存双节点协议的任期、候选、领导者、复制日志和提交水位。
/// 字段说明: 领导者只能在本任期候选已经双方确认后写入；提交水位只能沿连续哈希链递增。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedPairState {
    current_term: u64,
    campaign_winner: Option<PersistedPairCandidate>,
    leader: Option<PersistedPairCandidate>,
    records: BTreeMap<u64, PersistedPairRecord>,
    committed_sequence: u64,
    committed_hash: String,
}

/// 业务作用: 向双节点运行时返回不含可变引用的持久化进度摘要。
/// 字段说明: 摘要同时包含最后落盘和最后提交位置，两者不同表示存在已复制但尚未应用的记录。
#[derive(Debug, Clone)]
pub struct PairStorageStatus {
    /// 已经持久化且后续请求不得回退的当前任期。
    pub current_term: u64,
    /// 当前任期双方比较后记录的优胜候选节点编号。
    pub winner_node_id: Option<u64>,
    /// 优胜候选参与排序的竞选纪元纳秒值。
    pub winner_election_time_ns: Option<u64>,
    /// 优胜候选本次进程实例的不可复用标识。
    pub winner_startup_nonce: Option<String>,
    /// 已经经过双方确认的当前领导节点编号。
    pub leader_node_id: Option<u64>,
    /// 当前领导者本次进程实例的不可复用标识。
    pub leader_startup_nonce: Option<String>,
    /// 本地哈希链最后一条已落盘记录序号。
    pub last_sequence: u64,
    /// 本地最后一条已落盘记录的 SHA-256 哈希。
    pub last_hash: String,
    /// 本地已经应用到业务状态机的连续提交水位。
    pub committed_sequence: u64,
    /// 本地连续提交水位对应记录的 SHA-256 哈希。
    pub committed_hash: String,
}

/// 业务作用: 返回远端竞选请求落盘后的判断结果，服务端据此向请求方声明当前任期优胜者。
/// 字段说明: `accepted` 仅表示请求候选当前胜出且日志一致，不表示请求方可以绕过后续心跳确认直接执行动作。
#[derive(Debug, Clone)]
pub struct PairCampaignDecision {
    /// 请求候选是否胜出且具备继续完成双向确认的前置条件。
    pub accepted: bool,
    /// 处理请求并落盘后的本地双节点进度摘要。
    pub status: PairStorageStatus,
    /// 接受或拒绝竞选请求的明确业务原因。
    pub reason: String,
}

/// 业务作用: 表示协调器在磁盘上的完整一致性镜像，所有修改都通过同一个互斥区和原子替换写入。
/// 字段说明: 投票和日志绝不能跨文件乱序落盘，因此这里故意使用一个小型镜像文件统一串行化。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStoreImage {
    format_version: u32,
    genesis: CoordinatorGenesis,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    logs: BTreeMap<u64, Entry<CoordinatorTypeConfig>>,
    state_machine: PersistedStateMachine,
    current_snapshot: Option<PersistedSnapshot>,
    snapshot_sequence: u64,
    #[serde(default)]
    pair: PersistedPairState,
}

/// 业务作用: 持有磁盘镜像路径和进程内串行化锁，是日志存储与状态机存储共享的唯一事实源。
/// 字段说明: 标准互斥锁只包围短小的内存修改与文件替换，不跨越网络等待，避免异步任务之间出现写入重排。
#[derive(Debug)]
struct StoreCore {
    // 该文件句柄持有目录级排他锁，生命周期覆盖所有日志与状态机句柄。
    // 若不保留句柄，第二个进程可能同时原子替换同一个镜像文件，使各自内存状态互相覆盖。
    _directory_lock: File,
    path: PathBuf,
    image: Mutex<PersistedStoreImage>,
    // 任一同步步骤失败后，磁盘最终状态可能取决于失败发生在重命名前还是目录同步时。
    // 进程必须保持失败关闭，不能继续以无法证明的内存状态授予任期或动作。
    failed: AtomicBool,
}

/// 业务作用: 实现共识日志、投票和提交位置的持久化接口。
/// 字段说明: 克隆该句柄只会增加共享核心引用，不会复制或分叉磁盘状态。
#[derive(Debug, Clone)]
pub struct CoordinatorLogStore {
    core: Arc<StoreCore>,
}

/// 业务作用: 实现业务状态机与快照接口，并向代理数据面提供最近已提交状态的只读副本。
/// 字段说明: 状态应用与日志写入共用同一个存储核心，从而保证崩溃恢复时的顺序一致。
#[derive(Debug, Clone)]
pub struct CoordinatorStateMachineStore {
    core: Arc<StoreCore>,
}

impl CoordinatorLogStore {
    /// 业务作用: 打开或创建协调器持久化镜像，并核对磁盘创世信息与当前静态配置完全一致。
    /// 参数说明:
    /// - `data_dir`: 当前节点独占的协调器数据目录。
    /// - `genesis`: 由集群标识、固定协议和静态成员表生成的预期创世信息。
    ///
    /// 返回: 返回共享同一磁盘核心的日志存储和状态机存储；发现成员或协议被改写时拒绝启动。
    pub fn open(
        data_dir: &Path,
        genesis: CoordinatorGenesis,
    ) -> anyhow::Result<(Self, CoordinatorStateMachineStore)> {
        fs::create_dir_all(data_dir).with_context(|| {
            format!("create coordinator data directory `{}`", data_dir.display())
        })?;
        set_private_permissions(data_dir, 0o700)?;
        let directory_lock = acquire_directory_lock(data_dir)?;
        let path = data_dir.join("coordinator-state.json");
        if path.exists() {
            set_private_permissions(&path, 0o600)?;
        }
        let image = if path.exists() {
            let loaded = read_image(&path)?;
            if loaded.format_version != 3 {
                bail!(
                    "unsupported coordinator storage format `{}` in `{}`",
                    loaded.format_version,
                    path.display()
                );
            }
            if loaded.genesis != genesis {
                bail!(
                    "coordinator genesis mismatch in `{}`; cluster id, protocol and static membership are immutable",
                    path.display()
                );
            }
            loaded
        } else {
            let created = PersistedStoreImage {
                format_version: 3,
                genesis,
                vote: None,
                committed: None,
                last_purged_log_id: None,
                logs: BTreeMap::new(),
                state_machine: PersistedStateMachine::default(),
                current_snapshot: None,
                snapshot_sequence: 0,
                pair: PersistedPairState::default(),
            };
            persist_image(&path, &created)?;
            created
        };
        let core = Arc::new(StoreCore {
            _directory_lock: directory_lock,
            path,
            image: Mutex::new(image),
            failed: AtomicBool::new(false),
        });
        Ok((
            Self { core: core.clone() },
            CoordinatorStateMachineStore { core },
        ))
    }

    /// 业务作用: 获取持久化镜像互斥区，并把锁中毒转换成明确错误而不是继续使用可能不完整的内存状态。
    ///
    /// 返回: 返回镜像保护对象；先前持锁代码异常退出时返回错误。
    fn lock_image(&self) -> anyhow::Result<MutexGuard<'_, PersistedStoreImage>> {
        if self.core.failed.load(Ordering::Acquire) {
            bail!("coordinator storage is fail-closed after a persistence error");
        }
        self.core
            .image
            .lock()
            .map_err(|_| anyhow::anyhow!("coordinator storage mutex is poisoned"))
    }
}

/// 业务作用: 对协调器数据目录取得非阻塞排他锁，拒绝同一节点身份被两个本机进程同时打开。
/// 参数说明:
/// - `data_dir`: 已经创建且应由当前协调器实例独占的数据目录。
///
/// 返回: 成功时返回必须由存储核心持续持有的锁文件句柄；目录已被占用或系统调用失败时返回启动错误。
fn acquire_directory_lock(data_dir: &Path) -> anyhow::Result<File> {
    let lock_path = data_dir.join("coordinator-state.lock");
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&lock_path)
        .with_context(|| format!("open coordinator lock `{}`", lock_path.display()))?;
    set_private_permissions(&lock_path, 0o600)?;

    // 使用非阻塞锁是为了让重复实例在启动阶段立即失败，而不是无限等待并在旧实例退出后
    // 以过期运行参数接管目录。锁附着于文件描述符，StoreCore 析构并关闭句柄时由内核自动释放。
    let result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        let error = io::Error::last_os_error();
        bail!(
            "coordinator data directory `{}` is already in use or cannot be locked: {error}",
            data_dir.display()
        );
    }
    Ok(lock_file)
}

impl CoordinatorStateMachineStore {
    /// 业务作用: 返回最近已经由复制协议提交并持久化的业务状态副本，供路由保护和管理面读取。
    ///
    /// 返回: 返回克隆后的协调状态；先前发生持久化错误或锁异常时返回错误，调用方必须保持失败关闭。
    pub fn read_state(&self) -> anyhow::Result<CoordinationState> {
        // 只读路径同样必须经过 failed 门禁。否则写入在重命名或目录同步阶段失败后，
        // 数据面仍可能从内存克隆旧状态，并误以为持久化权威保持健康。
        let image = self.lock_image()?;
        Ok(image.state_machine.state.clone())
    }

    /// 业务作用: 只读取配置一致性门禁所需的轻量版本头，避免高频守护循环克隆完整配置正文。
    ///
    /// 返回: 最近提交的配置 revision 和摘要；先前持久化失败时返回错误。
    pub fn read_cluster_config_head(&self) -> anyhow::Result<(u64, Option<String>)> {
        let image = self.lock_image()?;
        let state = &image.state_machine.state;
        Ok((state.config_revision, state.config_digest.clone()))
    }

    /// 业务作用: 只读取指定 HA 组的协调状态，避免周期拓扑探测连带克隆完整集群配置正文。
    ///
    /// 参数说明:
    /// - `group_id`: 需要读取的稳定 HA 组标识。
    ///
    /// 返回: HA 组存在时返回其轻量副本；存储失败关闭或锁异常时返回错误。
    pub fn read_group(&self, group_id: &str) -> anyhow::Result<Option<CoordinationGroupState>> {
        let image = self.lock_image()?;
        Ok(image.state_machine.state.groups.get(group_id).cloned())
    }

    /// 业务作用: 仅检查协调状态存储是否仍具备可证明的持久化健康状态。
    ///
    /// 返回: 状态锁可取得且没有发生持久化失败时成功，不克隆业务状态或配置正文。
    pub fn check_healthy(&self) -> anyhow::Result<()> {
        drop(self.lock_image()?);
        Ok(())
    }

    /// 业务作用: 获取状态机共享镜像的互斥保护对象。
    ///
    /// 返回: 返回镜像保护对象；锁异常时返回错误。
    fn lock_image(&self) -> anyhow::Result<MutexGuard<'_, PersistedStoreImage>> {
        if self.core.failed.load(Ordering::Acquire) {
            bail!("coordinator storage is fail-closed after a persistence error");
        }
        self.core
            .image
            .lock()
            .map_err(|_| anyhow::anyhow!("coordinator storage mutex is poisoned"))
    }

    /// 业务作用: 读取双节点协议最后持久化的任期、候选、领导者和日志水位摘要。
    ///
    /// 返回: 返回可跨异步边界传递的状态副本。
    pub fn pair_status(&self) -> anyhow::Result<PairStorageStatus> {
        let image = self.lock_image()?;
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 在远端响应暴露更高任期时先持久化该任期并撤销旧领导身份，确保旧进程不能在下一次心跳前继续写入。
    /// 参数说明:
    /// - `term`: 已由静态对端响应证明存在的更高任期。
    ///
    /// 返回: 返回更新后的持久化摘要；传入任期不高于当前值时保持幂等。
    pub fn pair_observe_term(&self, term: u64) -> anyhow::Result<PairStorageStatus> {
        let mut image = self.lock_image()?;
        if term > image.pair.current_term {
            let previous = image.clone();
            image.pair.current_term = term;
            image.pair.campaign_winner = None;
            image.pair.leader = None;
            persist_or_restore(&self.core, &mut image, previous)?;
        }
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 丢弃仅存在于本节点、且从未越过提交水位的尾部记录，使对端恢复后可以重新进行 2/2 竞选。
    /// 参数说明:
    /// - `peer_last_sequence`: 对端最后落盘位置，必须恰好等于本地提交水位。
    /// - `peer_last_hash`: 对端最后哈希，必须等于本地提交哈希。
    /// - `peer_committed_sequence`: 对端提交水位，必须等于本地提交水位。
    /// - `peer_committed_hash`: 对端提交哈希，必须等于本地提交哈希。
    ///
    /// 返回: 只有双方明确共享同一提交前缀时才截断未提交尾部，否则返回错误并保留现场供人工检查。
    pub fn pair_truncate_uncommitted_tail(
        &self,
        peer_last_sequence: u64,
        peer_last_hash: &str,
        peer_committed_sequence: u64,
        peer_committed_hash: &str,
    ) -> anyhow::Result<PairStorageStatus> {
        let mut image = self.lock_image()?;
        let status = pair_status_from_image(&image);
        if status.last_sequence <= status.committed_sequence {
            return Ok(status);
        }
        if peer_last_sequence != status.committed_sequence
            || peer_committed_sequence != status.committed_sequence
            || peer_last_hash != status.committed_hash
            || peer_committed_hash != status.committed_hash
        {
            bail!("peer does not prove the same committed prefix for safe tail truncation");
        }
        let committed = image.pair.committed_sequence;
        let previous = image.clone();
        image
            .pair
            .records
            .retain(|sequence, _| *sequence <= committed);
        persist_or_restore(&self.core, &mut image, previous)?;
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 在竞选响应证明对端提交水位更高时应用本地已经存在的同哈希记录，使“双方均已落盘但一侧未收到提交通知”的崩溃状态恢复一致。
    /// 参数说明:
    /// - `sequence`: 对端已经提交的位置。
    /// - `record_hash`: 对端已提交位置的哈希。
    ///
    /// 返回: 本地记录连续且目标哈希相同时推进并落盘，否则拒绝把未知内容标记为已提交。
    pub fn pair_recover_commit(
        &self,
        sequence: u64,
        record_hash: &str,
    ) -> anyhow::Result<PairStorageStatus> {
        let mut image = self.lock_image()?;
        if sequence <= image.pair.committed_sequence {
            if sequence == image.pair.committed_sequence && record_hash != image.pair.committed_hash
            {
                bail!("peer committed hash differs at the same pair sequence");
            }
            return Ok(pair_status_from_image(&image));
        }
        let mut replacement = image.clone();
        validate_pair_commit_target(&replacement, sequence, record_hash)?;
        apply_pair_records(&mut replacement, sequence)?;
        persist_replacement(&self.core, &mut image, replacement)?;
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 先行持久化一个更高任期及本地候选信息，再允许向对端发送竞选请求。
    /// 参数说明:
    /// - `node_id`: 本地静态节点编号。
    /// - `election_time_ns`: 本轮竞选纳秒值，只用于双方状态一致后的候选排序。
    /// - `startup_nonce`: 本地当前启动实例标识。
    ///
    /// 返回: 返回新任期的持久化摘要；文件同步失败时不得发送竞选请求。
    pub fn pair_start_campaign(
        &self,
        node_id: u64,
        election_time_ns: u64,
        startup_nonce: String,
    ) -> anyhow::Result<PairStorageStatus> {
        let mut image = self.lock_image()?;
        let previous = image.clone();
        image.pair.current_term = image
            .pair
            .current_term
            .checked_add(1)
            .context("pair term is exhausted")?;
        image.pair.campaign_winner = Some(PersistedPairCandidate {
            node_id,
            election_time_ns,
            startup_nonce,
        });
        image.pair.leader = None;
        persist_or_restore(&self.core, &mut image, previous)?;
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 校验并持久化远端候选；同任期只在双方日志摘要完全一致后选择纳秒值较小的候选。
    /// 参数说明:
    /// - `node_id`: 请求候选节点编号。
    /// - `term`: 请求候选已经持久化的任期。
    /// - `election_time_ns`: 请求候选的纳秒排序值。
    /// - `startup_nonce`: 请求候选启动实例标识。
    /// - `last_sequence`: 请求方最后落盘日志位置。
    /// - `last_hash`: 请求方最后日志哈希。
    /// - `committed_sequence`: 请求方最后提交位置。
    /// - `committed_hash`: 请求方已提交位置对应的哈希。
    ///
    /// 返回: 返回本地持久化后的优胜者和拒绝原因。
    #[allow(clippy::too_many_arguments)]
    pub fn pair_accept_campaign(
        &self,
        node_id: u64,
        term: u64,
        election_time_ns: u64,
        startup_nonce: String,
        last_sequence: u64,
        last_hash: &str,
        committed_sequence: u64,
        committed_hash: &str,
    ) -> anyhow::Result<PairCampaignDecision> {
        let mut image = self.lock_image()?;
        if term < image.pair.current_term {
            return Ok(PairCampaignDecision {
                accepted: false,
                status: pair_status_from_image(&image),
                reason: "campaign term is lower than persisted term".to_string(),
            });
        }
        // 竞选处理可能同时更新任期、截断未提交尾部并追赶提交水位。全部步骤先在副本上完成，
        // 任一哈希或状态机校验失败时正式内存镜像保持不变。
        let mut replacement = image.clone();
        if term > replacement.pair.current_term {
            replacement.pair.current_term = term;
            replacement.pair.campaign_winner = None;
            replacement.pair.leader = None;
        }
        let mut local_status = pair_status_from_image(&replacement);
        // 本地尾部若严格位于双方共同提交点之后，而请求方明确停在该提交点，说明尾部从未获得 2/2 落盘。
        // 只有四个水位和哈希条件全部成立才允许截断，避免网络重排时删除已经被另一侧保存的记录。
        if local_status.last_sequence > local_status.committed_sequence
            && last_sequence == local_status.committed_sequence
            && committed_sequence == local_status.committed_sequence
            && last_hash == local_status.committed_hash
            && committed_hash == local_status.committed_hash
        {
            let committed = replacement.pair.committed_sequence;
            replacement
                .pair
                .records
                .retain(|sequence, _| *sequence <= committed);
            local_status = pair_status_from_image(&replacement);
        }
        if last_sequence != local_status.last_sequence || last_hash != local_status.last_hash {
            persist_replacement(&self.core, &mut image, replacement)?;
            return Ok(PairCampaignDecision {
                accepted: false,
                status: pair_status_from_image(&image),
                reason: format!(
                    "campaign log summary differs: remote last={last_sequence}/{last_hash} committed={committed_sequence}, local last={}/{} committed={}",
                    local_status.last_sequence,
                    local_status.last_hash,
                    local_status.committed_sequence
                ),
            });
        }
        if committed_sequence > local_status.committed_sequence {
            validate_pair_commit_target(&replacement, committed_sequence, committed_hash)?;
            apply_pair_records(&mut replacement, committed_sequence)?;
        } else if committed_sequence < local_status.committed_sequence {
            persist_replacement(&self.core, &mut image, replacement)?;
            return Ok(PairCampaignDecision {
                accepted: false,
                status: local_status,
                reason: "campaign requester must recover the newer committed watermark".to_string(),
            });
        } else if committed_hash != local_status.committed_hash {
            persist_replacement(&self.core, &mut image, replacement)?;
            return Ok(PairCampaignDecision {
                accepted: false,
                status: local_status,
                reason: "campaign committed hash differs at the same sequence".to_string(),
            });
        }
        let incoming = PersistedPairCandidate {
            node_id,
            election_time_ns,
            startup_nonce,
        };
        // 同一任期一旦完成领导确认就不能再改写优胜者，否则延迟到达的旧竞选包可能制造两个领导者。
        // 对已确认领导者自己的幂等竞选可以返回接受，但不会重新生成或替换持久身份。
        if let Some(leader) = replacement.pair.leader.as_ref() {
            let accepted = leader == &incoming;
            persist_replacement(&self.core, &mut image, replacement)?;
            return Ok(PairCampaignDecision {
                accepted,
                status: pair_status_from_image(&image),
                reason: if accepted {
                    String::new()
                } else {
                    "pair term already has a confirmed leader".to_string()
                },
            });
        }
        let winner = match replacement.pair.campaign_winner.take() {
            Some(current) if pair_candidate_key(&current) <= pair_candidate_key(&incoming) => {
                current
            }
            _ => incoming.clone(),
        };
        let accepted = winner == incoming;
        replacement.pair.campaign_winner = Some(winner);
        persist_replacement(&self.core, &mut image, replacement)?;
        Ok(PairCampaignDecision {
            accepted,
            status: pair_status_from_image(&image),
            reason: if accepted {
                String::new()
            } else {
                "another candidate has the smaller deterministic election key".to_string()
            },
        })
    }

    /// 业务作用: 在收到对端确认后把本任期优胜候选持久化为领导者，防止仅凭本地纳秒值自封领导者。
    /// 参数说明:
    /// - `term`: 已经双方确认的任期。
    /// - `node_id`: 预期优胜节点编号。
    /// - `election_time_ns`: 预期优胜候选的纳秒值。
    /// - `startup_nonce`: 预期优胜候选启动实例标识。
    ///
    /// 返回: 身份逐项匹配并同步落盘时返回成功，否则拒绝建立领导权。
    pub fn pair_confirm_leader(
        &self,
        term: u64,
        node_id: u64,
        election_time_ns: u64,
        startup_nonce: &str,
    ) -> anyhow::Result<()> {
        let mut image = self.lock_image()?;
        if term != image.pair.current_term {
            bail!("pair leader term does not match persisted term");
        }
        let Some(winner) = image.pair.campaign_winner.clone() else {
            bail!("pair campaign has no persisted winner");
        };
        if winner.node_id != node_id
            || winner.election_time_ns != election_time_ns
            || winner.startup_nonce != startup_nonce
        {
            bail!("pair leader identity does not match persisted campaign winner");
        }
        let previous = image.clone();
        image.pair.leader = Some(winner);
        persist_or_restore(&self.core, &mut image, previous)
    }

    /// 业务作用: 校验双节点领导者心跳并持久化当前领导身份；较低任期、错误实例或超前提交位置都会被拒绝。
    /// 参数说明:
    /// - `leader_node_id`: 心跳发送者节点编号。
    /// - `term`: 心跳任期。
    /// - `leader_startup_nonce`: 心跳发送者启动实例标识。
    /// - `committed_sequence`: 领导者提交位置。
    /// - `committed_hash`: 领导者提交位置哈希。
    ///
    /// 返回: 返回是否接受心跳以及当前本地摘要；必要时把已复制记录推进到领导者提交位置。
    pub fn pair_accept_heartbeat(
        &self,
        leader_node_id: u64,
        term: u64,
        leader_startup_nonce: &str,
        committed_sequence: u64,
        committed_hash: &str,
    ) -> anyhow::Result<PairCampaignDecision> {
        let mut image = self.lock_image()?;
        if term != image.pair.current_term {
            return Ok(PairCampaignDecision {
                accepted: false,
                status: pair_status_from_image(&image),
                reason: "heartbeat term does not match persisted term".to_string(),
            });
        }
        let Some(winner) = image.pair.campaign_winner.clone() else {
            return Ok(PairCampaignDecision {
                accepted: false,
                status: pair_status_from_image(&image),
                reason: "heartbeat arrived before a confirmed campaign".to_string(),
            });
        };
        if winner.node_id != leader_node_id || winner.startup_nonce != leader_startup_nonce {
            return Ok(PairCampaignDecision {
                accepted: false,
                status: pair_status_from_image(&image),
                reason: "heartbeat identity does not match campaign winner".to_string(),
            });
        }
        let mut replacement = image.clone();
        if committed_sequence > replacement.pair.committed_sequence {
            let Some(record) = replacement.pair.records.get(&committed_sequence) else {
                return Ok(PairCampaignDecision {
                    accepted: false,
                    status: pair_status_from_image(&image),
                    reason: "heartbeat commit position is not present locally".to_string(),
                });
            };
            if record.record_hash != committed_hash {
                return Ok(PairCampaignDecision {
                    accepted: false,
                    status: pair_status_from_image(&image),
                    reason: "heartbeat commit hash differs from local record".to_string(),
                });
            }
            apply_pair_records(&mut replacement, committed_sequence)?;
        }
        replacement.pair.leader = Some(winner);
        persist_replacement(&self.core, &mut image, replacement)?;
        Ok(PairCampaignDecision {
            accepted: true,
            status: pair_status_from_image(&image),
            reason: String::new(),
        })
    }

    /// 业务作用: 校验领导身份、位置和哈希链后持久化一条双节点复制记录。
    /// 参数说明:
    /// - `leader_node_id`: 复制发起者节点编号。
    /// - `term`: 记录所属任期。
    /// - `leader_startup_nonce`: 复制发起者启动实例标识。
    /// - `sequence`: 新记录连续位置。
    /// - `previous_hash`: 调用方声明的上一条记录哈希。
    /// - `record_hash`: 新记录内容哈希。
    /// - `payload`: 协调业务命令 JSON。
    ///
    /// 返回: 文件同步完成后返回最新摘要；任意链路不变量不满足时返回错误。
    #[allow(clippy::too_many_arguments)]
    pub fn pair_append_record(
        &self,
        leader_node_id: u64,
        term: u64,
        leader_startup_nonce: &str,
        sequence: u64,
        previous_hash: &str,
        record_hash: &str,
        payload: Vec<u8>,
    ) -> anyhow::Result<PairStorageStatus> {
        let mut image = self.lock_image()?;
        validate_pair_leader(&image, leader_node_id, term, leader_startup_nonce)?;
        let status = pair_status_from_image(&image);
        let expected_sequence = status
            .last_sequence
            .checked_add(1)
            .context("pair record sequence is exhausted")?;
        if sequence != expected_sequence {
            bail!(
                "pair record sequence `{sequence}` does not follow `{}`",
                status.last_sequence
            );
        }
        if previous_hash != status.last_hash {
            bail!("pair record previous hash does not match local hash chain");
        }
        let expected_hash = persisted_pair_record_hash(
            previous_hash,
            term,
            sequence,
            leader_node_id,
            leader_startup_nonce,
            &payload,
        );
        if record_hash != expected_hash {
            bail!("pair record hash does not match its identity and payload");
        }
        serde_json::from_slice::<CoordinationCommand>(&payload)
            .context("decode pair coordination command before persistence")?;
        let previous = image.clone();
        image.pair.records.insert(
            sequence,
            PersistedPairRecord {
                term,
                sequence,
                leader_node_id,
                leader_startup_nonce: leader_startup_nonce.to_string(),
                previous_hash: previous_hash.to_string(),
                record_hash: record_hash.to_string(),
                payload,
            },
        );
        persist_or_restore(&self.core, &mut image, previous)?;
        Ok(pair_status_from_image(&image))
    }

    /// 业务作用: 将已经在双方落盘的记录推进为已提交状态并应用业务状态机。
    /// 参数说明:
    /// - `leader_node_id`: 提交发起者节点编号。
    /// - `term`: 提交所属任期。
    /// - `leader_startup_nonce`: 提交发起者启动实例标识。
    /// - `sequence`: 需要推进到的提交位置。
    /// - `record_hash`: 目标位置的权威哈希。
    ///
    /// 返回: 返回目标记录应用产生的业务响应；重复提交返回当前路由状态而不重复执行命令。
    pub fn pair_commit_record(
        &self,
        leader_node_id: u64,
        term: u64,
        leader_startup_nonce: &str,
        sequence: u64,
        record_hash: &str,
    ) -> anyhow::Result<CoordinationResponse> {
        let mut image = self.lock_image()?;
        validate_pair_leader(&image, leader_node_id, term, leader_startup_nonce)?;
        let mut replacement = image.clone();
        validate_pair_commit_target(&replacement, sequence, record_hash)?;
        let response = apply_pair_records(&mut replacement, sequence)?;
        persist_replacement(&self.core, &mut image, replacement)?;
        Ok(response)
    }
}

/// 业务作用: 从持久化镜像生成双节点状态摘要，空日志使用空哈希作为创世前序值。
/// 参数说明:
/// - `image`: 当前受锁保护的完整镜像。
///
/// 返回: 返回双节点运行时需要的不可变摘要。
fn pair_status_from_image(image: &PersistedStoreImage) -> PairStorageStatus {
    let last_record = image
        .pair
        .records
        .last_key_value()
        .map(|(_, record)| record);
    PairStorageStatus {
        current_term: image.pair.current_term,
        winner_node_id: image
            .pair
            .campaign_winner
            .as_ref()
            .map(|winner| winner.node_id),
        winner_election_time_ns: image
            .pair
            .campaign_winner
            .as_ref()
            .map(|winner| winner.election_time_ns),
        winner_startup_nonce: image
            .pair
            .campaign_winner
            .as_ref()
            .map(|winner| winner.startup_nonce.clone()),
        leader_node_id: image.pair.leader.as_ref().map(|leader| leader.node_id),
        leader_startup_nonce: image
            .pair
            .leader
            .as_ref()
            .map(|leader| leader.startup_nonce.clone()),
        last_sequence: last_record.map(|record| record.sequence).unwrap_or(0),
        last_hash: last_record
            .map(|record| record.record_hash.clone())
            .unwrap_or_default(),
        committed_sequence: image.pair.committed_sequence,
        committed_hash: image.pair.committed_hash.clone(),
    }
}

/// 业务作用: 构造双节点候选的全序比较键，纳秒相同时依次使用节点编号和启动实例消除平局。
/// 参数说明:
/// - `candidate`: 需要参与同任期排序的候选。
///
/// 返回: 返回可按字典序比较的候选键，较小者优先。
fn pair_candidate_key(candidate: &PersistedPairCandidate) -> (u64, u64, &str) {
    (
        candidate.election_time_ns,
        candidate.node_id,
        candidate.startup_nonce.as_str(),
    )
}

/// 业务作用: 在落盘侧重新计算双节点记录哈希，防止传输损坏或错误实现把任期、领导身份与业务内容不一致的记录接入哈希链。
/// 参数说明:
/// - `previous_hash`: 上一条记录哈希。
/// - `term`: 记录所属任期。
/// - `sequence`: 记录连续位置。
/// - `leader_node_id`: 生成记录的领导节点编号。
/// - `leader_startup_nonce`: 生成记录的领导启动实例。
/// - `payload`: 协调业务命令字节。
///
/// 返回: 返回与发送侧算法完全一致的小写十六进制 SHA-256 摘要。
fn persisted_pair_record_hash(
    previous_hash: &str,
    term: u64,
    sequence: u64,
    leader_node_id: u64,
    leader_startup_nonce: &str,
    payload: &[u8],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(previous_hash.as_bytes());
    hasher.update(term.to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    hasher.update(leader_node_id.to_be_bytes());
    hasher.update(leader_startup_nonce.as_bytes());
    hasher.update(payload);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// 业务作用: 校验双节点写入请求来自当前持久化领导者和同一启动实例。
/// 参数说明:
/// - `image`: 当前受锁保护的持久化镜像。
/// - `leader_node_id`: 请求声明的领导节点编号。
/// - `term`: 请求声明的任期。
/// - `leader_startup_nonce`: 请求声明的领导启动实例。
///
/// 返回: 三项均匹配时返回成功，否则拒绝旧领导者写入。
fn validate_pair_leader(
    image: &PersistedStoreImage,
    leader_node_id: u64,
    term: u64,
    leader_startup_nonce: &str,
) -> anyhow::Result<()> {
    if term != image.pair.current_term {
        bail!("pair request term does not match persisted term");
    }
    let Some(leader) = image.pair.leader.as_ref() else {
        bail!("pair request arrived before leader confirmation");
    };
    if leader.node_id != leader_node_id || leader.startup_nonce != leader_startup_nonce {
        bail!("pair request identity does not match persisted leader");
    }
    Ok(())
}

/// 业务作用: 在推进提交水位前确认目标记录确实存在且哈希一致，把网络声明限制在本地已经稳定落盘的日志范围内。
/// 参数说明:
/// - `image`: 当前受锁保护的持久化镜像。
/// - `sequence`: 需要提交或恢复到的位置。
/// - `record_hash`: 对端声明的目标记录哈希；零位置必须使用空哈希。
///
/// 返回: 目标位置可验证时返回成功，否则拒绝提交未知或不同内容。
fn validate_pair_commit_target(
    image: &PersistedStoreImage,
    sequence: u64,
    record_hash: &str,
) -> anyhow::Result<()> {
    if sequence == 0 {
        if record_hash.is_empty() {
            return Ok(());
        }
        bail!("pair genesis commit hash must be empty");
    }
    let Some(record) = image.pair.records.get(&sequence) else {
        bail!("pair commit record `{sequence}` is not present");
    };
    if record.record_hash != record_hash {
        bail!("pair commit hash differs from persisted record");
    }
    Ok(())
}

/// 业务作用: 连续应用双节点已复制记录直到目标提交位置，并单调推进提交哈希。
/// 参数说明:
/// - `image`: 当前受锁保护且随后会原子落盘的镜像。
/// - `target_sequence`: 本次要推进到的提交位置。
///
/// 返回: 返回目标位置的业务响应；记录不连续或命令损坏时返回错误且不写入磁盘。
fn apply_pair_records(
    image: &mut PersistedStoreImage,
    target_sequence: u64,
) -> anyhow::Result<CoordinationResponse> {
    if target_sequence < image.pair.committed_sequence {
        bail!("pair commit position must not move backwards");
    }
    if target_sequence == image.pair.committed_sequence {
        return Ok(CoordinationResponse {
            accepted: true,
            route_epoch: image
                .state_machine
                .state
                .groups
                .values()
                .map(|group| group.route_epoch)
                .max()
                .unwrap_or(0),
            ..CoordinationResponse::default()
        });
    }
    let records = ((image.pair.committed_sequence + 1)..=target_sequence)
        .map(|sequence| {
            image
                .pair
                .records
                .get(&sequence)
                .cloned()
                .with_context(|| format!("pair commit chain misses sequence `{sequence}`"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut response = CoordinationResponse::default();
    for record in records {
        let command: CoordinationCommand = serde_json::from_slice(&record.payload)
            .with_context(|| format!("decode pair record `{}`", record.sequence))?;
        let log_id = LogId::new(
            CommittedLeaderId::new(record.term, record.leader_node_id),
            record.sequence,
        );
        response = image.state_machine.state.apply(command, log_id);
        image.state_machine.last_applied_log_id = Some(log_id);
        image.pair.committed_sequence = record.sequence;
        image.pair.committed_hash = record.record_hash;
    }
    Ok(response)
}

impl RaftLogReader<CoordinatorTypeConfig> for CoordinatorLogStore {
    /// 业务作用: 按半开区间读取连续共识日志，供领导者复制和节点启动恢复使用。
    /// 参数说明:
    /// - `range`: 需要读取的日志位置范围。
    ///
    /// 返回: 返回范围内按位置升序排列的日志；磁盘状态锁异常时返回存储错误。
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<CoordinatorTypeConfig>>, StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_logs(storage_source(error)))?;
        Ok(image
            .logs
            .iter()
            .filter(|(index, _)| range.contains(*index))
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<CoordinatorTypeConfig> for CoordinatorLogStore {
    type LogReader = Self;

    /// 业务作用: 返回最后清理位置和最后现存日志位置，供共识核心恢复日志边界。
    ///
    /// 返回: 返回满足 `last_purged <= last_log` 的日志状态。
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<CoordinatorTypeConfig>, StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_logs(storage_source(error)))?;
        let last_log_id = image
            .logs
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(image.last_purged_log_id);
        Ok(LogState {
            last_purged_log_id: image.last_purged_log_id,
            last_log_id,
        })
    }

    /// 业务作用: 创建一个共享同一持久化核心的日志读取句柄。
    ///
    /// 返回: 返回可供独立复制任务使用的读取句柄。
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// 业务作用: 在响应投票请求前同步持久化新投票，保证重启后不会在同一任期重复投票。
    /// 参数说明:
    /// - `vote`: 共识核心批准的新投票状态。
    ///
    /// 返回: 仅在原子替换和文件同步完成后返回成功。
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write_vote(storage_source(error)))?;
        let previous = image.clone();
        image.vote = Some(*vote);
        persist_or_restore(&self.core, &mut image, previous)
            .map_err(|error| StorageIOError::write_vote(storage_source(error)))?;
        Ok(())
    }

    /// 业务作用: 读取最后同步落盘的投票状态，供节点恢复任期和投票约束。
    ///
    /// 返回: 返回最后投票；从未投票时返回空值。
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_vote(storage_source(error)))?;
        Ok(image.vote)
    }

    /// 业务作用: 同步保存多数派已确认的提交位置，重启恢复时据此避免暴露未提交日志。
    /// 参数说明:
    /// - `committed`: 最新提交日志位置。
    ///
    /// 返回: 文件同步完成后返回成功。
    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write(storage_source(error)))?;
        let previous = image.clone();
        image.committed = committed;
        persist_or_restore(&self.core, &mut image, previous)
            .map_err(|error| StorageIOError::write(storage_source(error)))?;
        Ok(())
    }

    /// 业务作用: 读取最后持久化的提交位置。
    ///
    /// 返回: 返回最后提交位置；尚无提交时返回空值。
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read(storage_source(error)))?;
        Ok(image.committed)
    }

    /// 业务作用: 连续追加共识日志并同步完整镜像，只有稳定落盘后才通知共识核心复制完成。
    /// 参数说明:
    /// - `entries`: 共识核心按位置升序提供的日志序列。
    /// - `callback`: 文件同步完成通知，领导者依赖该通知计算多数派提交。
    ///
    /// 返回: 追加和同步成功时返回成功；日志存在空洞时返回存储错误。
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<CoordinatorTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<CoordinatorTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write_logs(storage_source(error)))?;
        let previous = image.clone();
        for entry in entries {
            let previous_index = image
                .logs
                .last_key_value()
                .map(|(index, _)| *index)
                .or_else(|| image.last_purged_log_id.map(|log_id| log_id.index));
            let expected = if let Some(previous_index) = previous_index {
                let Some(expected) = previous_index.checked_add(1) else {
                    *image = previous;
                    let message = "coordinator log index is exhausted".to_string();
                    callback.log_io_completed(Err(io::Error::other(message.clone())));
                    return Err(StorageIOError::write_logs(storage_source(message)).into());
                };
                expected
            } else {
                entry.log_id.index
            };
            if entry.log_id.index > expected {
                let message = format!(
                    "coordinator log hole: expected index `{expected}`, got `{}`",
                    entry.log_id.index
                );
                *image = previous;
                callback.log_io_completed(Err(io::Error::other(message.clone())));
                return Err(StorageIOError::write_logs(storage_source(message)).into());
            }
            image.logs.insert(entry.log_id.index, entry);
        }
        if let Err(error) = persist_or_restore(&self.core, &mut image, previous) {
            let message = error.to_string();
            callback.log_io_completed(Err(io::Error::other(message.clone())));
            return Err(StorageIOError::write_logs(storage_source(message)).into());
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// 业务作用: 从冲突日志位置起删除全部后缀，使跟随者能够接受当前领导者的权威日志链。
    /// 参数说明:
    /// - `log_id`: 第一条需要删除的冲突日志位置。
    ///
    /// 返回: 删除并同步成功后返回成功。
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write_logs(storage_source(error)))?;
        let previous = image.clone();
        image.logs.split_off(&log_id.index);
        persist_or_restore(&self.core, &mut image, previous)
            .map_err(|error| StorageIOError::write_logs(storage_source(error)))?;
        Ok(())
    }

    /// 业务作用: 在快照覆盖相应状态后清理指定位置及之前的日志，并持久化清理边界。
    /// 参数说明:
    /// - `log_id`: 已由快照覆盖的最后日志位置。
    ///
    /// 返回: 清理边界和剩余日志同步成功后返回成功。
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write_logs(storage_source(error)))?;
        let previous = image.clone();
        image.last_purged_log_id = Some(log_id);
        image.logs.retain(|index, _| *index > log_id.index);
        persist_or_restore(&self.core, &mut image, previous)
            .map_err(|error| StorageIOError::write_logs(storage_source(error)))?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<CoordinatorTypeConfig> for CoordinatorStateMachineStore {
    /// 业务作用: 从同一互斥时刻复制已提交状态并生成快照，避免元数据与业务内容来自不同日志位置。
    ///
    /// 返回: 返回可传输快照，并在返回前把它同步保存为当前快照。
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<CoordinatorTypeConfig>, StorageError<u64>> {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_state_machine(storage_source(error)))?;
        let previous = image.clone();
        let data = serde_json::to_vec(&image.state_machine.state)
            .map_err(|error| StorageIOError::read_state_machine(storage_source(error)))?;
        image.snapshot_sequence = image.snapshot_sequence.checked_add(1).ok_or_else(|| {
            StorageIOError::write_snapshot(None, storage_source("snapshot sequence is exhausted"))
        })?;
        let snapshot_id = match image.state_machine.last_applied_log_id {
            Some(log_id) => format!(
                "{}-{}-{}",
                log_id.leader_id.term, log_id.index, image.snapshot_sequence
            ),
            None => format!("empty-{}", image.snapshot_sequence),
        };
        let meta = SnapshotMeta {
            last_log_id: image.state_machine.last_applied_log_id,
            last_membership: image.state_machine.last_membership.clone(),
            snapshot_id,
        };
        image.current_snapshot = Some(PersistedSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        persist_or_restore(&self.core, &mut image, previous).map_err(|error| {
            StorageIOError::write_snapshot(Some(meta.signature()), storage_source(error))
        })?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<CoordinatorTypeConfig> for CoordinatorStateMachineStore {
    type SnapshotBuilder = Self;

    /// 业务作用: 返回状态机最后应用位置与成员关系，供启动恢复校验日志和快照边界。
    ///
    /// 返回: 返回状态机应用进度和最后成员配置。
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_state_machine(storage_source(error)))?;
        Ok((
            image.state_machine.last_applied_log_id,
            image.state_machine.last_membership.clone(),
        ))
    }

    /// 业务作用: 按提交顺序应用日志并同步状态机，任何外部动作许可都只能从这里产生。
    /// 参数说明:
    /// - `entries`: 已经由多数派或双节点确认的共识日志序列。
    ///
    /// 返回: 为每条日志返回确定性业务响应；状态同步失败时不向调用方发放令牌。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<CoordinationResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<CoordinatorTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut image = self
            .lock_image()
            .map_err(|error| StorageIOError::write_state_machine(storage_source(error)))?;
        let previous = image.clone();
        let mut responses = Vec::new();
        for entry in entries {
            image.state_machine.last_applied_log_id = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => CoordinationResponse::default(),
                EntryPayload::Normal(command) => {
                    image.state_machine.state.apply(command, entry.log_id)
                }
                EntryPayload::Membership(membership) => {
                    image.state_machine.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership);
                    CoordinationResponse::default()
                }
            };
            responses.push(response);
        }
        persist_or_restore(&self.core, &mut image, previous)
            .map_err(|error| StorageIOError::write_state_machine(storage_source(error)))?;
        Ok(responses)
    }

    /// 业务作用: 创建快照构建句柄；克隆后仍共享同一持久化核心，因此快照元数据与状态不会分叉。
    ///
    /// 返回: 返回快照构建句柄。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// 业务作用: 为接收远端快照创建空的内存游标，共识核心会在安装前完整写入快照字节。
    ///
    /// 返回: 返回可异步读写和定位的空快照缓冲区。
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// 业务作用: 校验并安装领导者发送的完整快照，同时原子替换本地业务状态与当前快照。
    /// 参数说明:
    /// - `meta`: 快照覆盖的日志位置和成员关系。
    /// - `snapshot`: 已接收完成的快照字节。
    ///
    /// 返回: 解码和文件同步均成功后返回成功。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let state: CoordinationState = serde_json::from_slice(&data).map_err(|error| {
            StorageIOError::read_snapshot(Some(meta.signature()), storage_source(error))
        })?;
        let mut image = self.lock_image().map_err(|error| {
            StorageIOError::write_snapshot(Some(meta.signature()), storage_source(error))
        })?;
        let previous = image.clone();
        image.state_machine = PersistedStateMachine {
            last_applied_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            state,
        };
        image.current_snapshot = Some(PersistedSnapshot {
            meta: meta.clone(),
            data,
        });
        persist_or_restore(&self.core, &mut image, previous).map_err(|error| {
            StorageIOError::write_snapshot(Some(meta.signature()), storage_source(error))
        })?;
        Ok(())
    }

    /// 业务作用: 返回当前持久化快照的独立读取游标，供落后节点安装。
    ///
    /// 返回: 存在快照时返回其元数据和字节副本，否则返回空值。
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<CoordinatorTypeConfig>>, StorageError<u64>> {
        let image = self
            .lock_image()
            .map_err(|error| StorageIOError::read_snapshot(None, storage_source(error)))?;
        Ok(image.current_snapshot.as_ref().map(|snapshot| Snapshot {
            meta: snapshot.meta.clone(),
            snapshot: Box::new(Cursor::new(snapshot.data.clone())),
        }))
    }
}

/// 业务作用: 把任意可展示的内部错误转换成共识存储接口要求的输入输出错误，同时保留原始诊断文本。
/// 参数说明:
/// - `error`: 文件、序列化或互斥锁错误。
///
/// 返回: 返回可被共识库统一包装和传播的输入输出错误。
fn storage_source(error: impl std::fmt::Display) -> openraft::AnyError {
    openraft::AnyError::error(error.to_string())
}

/// 业务作用: 从磁盘读取并反序列化完整协调器镜像，启动时不得忽略尾部损坏或使用部分状态。
/// 参数说明:
/// - `path`: 协调器镜像文件路径。
///
/// 返回: 返回完整镜像；读取或 JSON 校验失败时拒绝启动。
fn read_image(path: &Path) -> anyhow::Result<PersistedStoreImage> {
    let mut file =
        File::open(path).with_context(|| format!("open coordinator state `{}`", path.display()))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .with_context(|| format!("read coordinator state `{}`", path.display()))?;
    serde_json::from_slice(&data)
        .with_context(|| format!("decode coordinator state `{}`", path.display()))
}

/// 业务作用: 提交一次受互斥锁保护的内存镜像修改；持久化失败时恢复旧副本并永久关闭本进程存储写入。
/// 参数说明:
/// - `core`: 当前唯一存储核心，提供正式路径和失败关闭标记。
/// - `image`: 已经完成业务校验和内存修改的受锁镜像。
/// - `previous`: 修改前的完整镜像，用于失败回滚。
///
/// 返回: 文件和目录同步全部成功时保留新镜像；失败时恢复旧内存镜像、标记存储不可再用并返回原始错误。
fn persist_or_restore(
    core: &StoreCore,
    image: &mut PersistedStoreImage,
    previous: PersistedStoreImage,
) -> anyhow::Result<()> {
    if let Err(error) = persist_image(&core.path, image) {
        *image = previous;
        core.failed.store(true, Ordering::Release);
        return Err(error)
            .context("coordinator storage entered fail-closed state after persistence failure");
    }
    Ok(())
}

/// 业务作用: 原子提交在独立副本上完成的复杂状态变更，业务校验中途失败时原镜像天然保持不变。
/// 参数说明:
/// - `core`: 当前唯一存储核心。
/// - `image`: 尚未修改的受锁正式内存镜像。
/// - `replacement`: 已完成全部业务校验和状态机应用的新镜像副本。
///
/// 返回: 新副本稳定落盘后才替换正式内存镜像；落盘失败时标记存储失败关闭并保留旧内存状态。
fn persist_replacement(
    core: &StoreCore,
    image: &mut PersistedStoreImage,
    replacement: PersistedStoreImage,
) -> anyhow::Result<()> {
    if let Err(error) = persist_image(&core.path, &replacement) {
        core.failed.store(true, Ordering::Release);
        return Err(error)
            .context("coordinator storage entered fail-closed state after persistence failure");
    }
    *image = replacement;
    Ok(())
}

/// 业务作用: 通过“临时文件写入、文件同步、原子替换、目录同步”四步持久化镜像，避免断电留下半个 JSON 文件。
/// 参数说明:
/// - `path`: 正式镜像文件路径。
/// - `image`: 需要完整落盘的新镜像。
///
/// 返回: 全部同步步骤成功后返回成功；任一步失败都保留旧正式文件作为恢复点。
fn persist_image(path: &Path, image: &PersistedStoreImage) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("coordinator state path has no parent directory")?;
    let temporary = parent.join(".coordinator-state.tmp");
    let data = serde_json::to_vec(image).context("encode coordinator state")?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&temporary)
        .with_context(|| format!("open temporary state `{}`", temporary.display()))?;
    set_private_permissions(&temporary, 0o600)?;
    file.write_all(&data)
        .with_context(|| format!("write temporary state `{}`", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("sync temporary state `{}`", temporary.display()))?;
    drop(file);
    fs::rename(&temporary, path).with_context(|| {
        format!(
            "replace coordinator state `{}` with `{}`",
            path.display(),
            temporary.display()
        )
    })?;
    File::open(parent)
        .with_context(|| format!("open coordinator state directory `{}`", parent.display()))?
        .sync_all()
        .with_context(|| format!("sync coordinator state directory `{}`", parent.display()))?;
    Ok(())
}

/// 业务作用: 把协调目录或状态文件权限收紧到仅进程所属用户可访问。
///
/// 参数说明:
/// - `path`: 已经创建且需要收紧权限的目录或文件。
/// - `mode`: Unix 权限位，目录使用 `0700`，状态和锁文件使用 `0600`。
///
/// 返回: 权限修改成功时返回空结果；无法收紧权限时拒绝继续启动或提交。
fn set_private_permissions(path: &Path, mode: u32) -> anyhow::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set private permissions on `{}`", path.display()))
}
