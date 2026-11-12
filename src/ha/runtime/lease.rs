/// 业务作用: 持有 HA 动作租约和续租任务，析构时把停止续期及 owner 释放交还领域关闭屏障。
#[derive(Debug)]
struct LeaseGuard {
    backend: LeaseBackend,
    owner_id: String,
    lost: Arc<AtomicBool>,
    renew_task: Option<DomainTask<()>>,
    cleanup_tasks: TaskSpawner,
}

impl LeaseGuard {
    /// 业务作用: 把已取得的本地或 Redis 租约组装成带续租任务和失权状态的生命周期守卫。
    /// 参数说明:
    /// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
    /// - `owner_id`: 本次动作已经取得租约时使用的完整随机令牌，用于隔离迟到续期和释放。
    /// - `ttl_ms`: coordinator 租约的有效期毫秒数。
    /// - `renew_interval_ms`: 持锁期间刷新 coordinator 租约的间隔毫秒数。
    /// - `cleanup_tasks`: 租约所属运行代的收尾入口；续期停止和释放由其统一等待。
    ///
    /// 返回: 持有续期任务的守卫；守卫取消后仍由运行代等待续期任务析构。
    fn from_acquired(
        backend: LeaseBackend,
        owner_id: String,
        ttl_ms: u64,
        renew_interval_ms: u64,
        cleanup_tasks: TaskSpawner,
    ) -> Self {
        let renew_backend = backend.clone();
        let renew_owner = owner_id.clone();
        let lost = Arc::new(AtomicBool::new(false));
        let renew_lost = lost.clone();
        let renew_task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(renew_interval_ms));
            let ttl = Duration::from_millis(ttl_ms);
            let mut last_success = Instant::now();
            let mut consecutive_failures = 0u64;
            loop {
                ticker.tick().await;
                match refresh_coordinator_lease(&renew_backend, &renew_owner, ttl_ms).await {
                    Ok(()) => {
                        if consecutive_failures > 0 {
                            warn!(
                                owner = %renew_owner,
                                coordinator = %renew_backend.label(),
                                failures = consecutive_failures,
                                "replication HA failover lease renewal recovered"
                            );
                        }
                        consecutive_failures = 0;
                        last_success = Instant::now();
                    }
                    Err(err) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if last_success.elapsed() >= ttl {
                            renew_lost.store(true, Ordering::Relaxed);
                            warn!(
                                owner = %renew_owner,
                                coordinator = %renew_backend.label(),
                                failures = consecutive_failures,
                                error = %err,
                                "failed to renew replication HA failover lease before TTL expired"
                            );
                            return;
                        }
                        warn!(
                            owner = %renew_owner,
                            coordinator = %renew_backend.label(),
                            failures = consecutive_failures,
                            error = %err,
                            "failed to renew replication HA failover lease; will retry before TTL expires"
                        );
                    }
                }
            }
        });
        Self {
            backend,
            owner_id,
            lost,
            renew_task: Some(renew_task.into()),
            cleanup_tasks,
        }
    }

    /// 业务作用: 按配置后端竞争 HA 动作租约，并在成功后启动周期续租。
    /// 参数说明:
    /// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
    /// - `owner_id`: 稳定实例身份；本函数附加本次动作的独立随机令牌后才开始竞争。
    /// - `ttl_ms`: coordinator 租约的有效期毫秒数。
    /// - `renew_interval_ms`: 持锁期间刷新 coordinator 租约的间隔毫秒数。
    /// - `acquire_timeout_ms`: 等待获得 coordinator 租约的最长毫秒数。
    /// - `cleanup_tasks`: 租约所属运行代的收尾入口，必须晚于所有动作守卫关闭。
    ///
    /// 返回: 在超时内取得租约时返回带续租任务的守卫；忙碌、后端或续租初始化失败时返回错误。
    async fn acquire(
        backend: LeaseBackend,
        owner_id: String,
        ttl_ms: u64,
        renew_interval_ms: u64,
        acquire_timeout_ms: u64,
        cleanup_tasks: TaskSpawner,
    ) -> anyhow::Result<Self> {
        // Redis 超时不能证明已发送命令没有执行；每次动作使用独立身份，迟到释放只能命中原租约。
        let owner_id = new_lease_owner_id(&owner_id)?;
        let started = Instant::now();
        let label = backend.label();
        let timeout = Duration::from_millis(acquire_timeout_ms);
        let mut last_error = None;
        loop {
            match try_acquire_coordinator_lease(&backend, &owner_id, ttl_ms).await {
                Ok(true) => {
                    return Ok(Self::from_acquired(
                        backend,
                        owner_id,
                        ttl_ms,
                        renew_interval_ms,
                        cleanup_tasks,
                    ));
                }
                Ok(false) => {}
                Err(err) => last_error = Some(err),
            }

            if started.elapsed() >= timeout {
                let reason = last_error
                    .map(|err| format!("last error: {err}"))
                    .unwrap_or_else(|| "lease is owned by another proxy".to_string());
                bail!(
                    "timed out after {acquire_timeout_ms}ms acquiring replication HA failover lease `{}`: {reason}",
                    label
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 业务作用: 复验租约守卫未失权且后端 owner 仍匹配，阻止过期动作继续执行副作用。
    /// 参数说明: 无。
    ///
    /// 返回: 本地失权标记未置位且权威后端仍确认当前 owner 时返回成功；否则返回失权错误。
    async fn ensure_active(&self) -> anyhow::Result<()> {
        // 续期任务异常结束同样意味着未来有效期无法保证，不能仅依赖尚未到期的存储值继续动作。
        if self.lost.load(Ordering::Relaxed)
            || self.renew_task.as_ref().is_none_or(DomainTask::is_finished)
        {
            bail!(
                "replication HA lease `{}` renewal failed for `{}`",
                self.backend.label(),
                self.owner_id
            );
        }
        ensure_coordinator_lease_active(&self.backend, &self.owner_id).await
    }
}

/// 业务作用: 为每次租约竞争生成独立控制身份，隔离同一代理前后两次动作的迟到续期和释放。
/// 参数说明:
/// - `instance_owner`: 配置或自动生成的稳定实例身份，用于关联日志与运行状态。
///
/// 返回: 稳定实例前缀加独立随机令牌；随机源不可用时拒绝竞争，不降级复用旧令牌。
fn new_lease_owner_id(instance_owner: &str) -> anyhow::Result<String> {
    Ok(format!("{instance_owner}:{}", instance_nonce()?))
}

impl Drop for LeaseGuard {
    /// 业务作用: 先撤销续期，再将等待及 owner 比较释放登记到运行代，避免续期与释放乱序。
    /// 参数说明: 无。
    ///
    /// 返回: 运行代负责等待收尾；后端不可达时保留告警并依赖租约 TTL，不释放其他 owner 的租约。
    fn drop(&mut self) {
        let Some(renew_task) = self.renew_task.take() else {
            return;
        };
        renew_task.abort();
        let backend = self.backend.clone();
        let owner_id = self.owner_id.clone();
        if let Err(error) = self.cleanup_tasks.spawn_cleanup(async move {
            // 必须确认续期 future 已析构，随后才可删除 owner；异步取消请求本身不构成释放屏障。
            let renewal = match renew_task.await {
                Ok(()) => Ok(()),
                Err(error) if error.is_cancelled() => Ok(()),
                Err(_) => Err(anyhow::anyhow!("HA lease renewal task exited abnormally")),
            };
            let released = match &backend {
                LeaseBackend::LocalFile { path } => release_local_lease(path, &owner_id),
                LeaseBackend::Redis { config } => release_redis_lease(config, &owner_id).await,
            };
            if let Err(error) = released {
                warn!(
                    owner = %owner_id,
                    coordinator = %backend.label(),
                    error = %error,
                    "failed to release replication HA failover lease"
                );
            }
            renewal
        }) {
            warn!(
                owner = %self.owner_id,
                error = %error,
                "HA runtime closed before lease cleanup was accepted"
            );
        }
    }
}

/// 业务作用: 向选定租约后端尝试一次比较并写入式获取。
/// 参数说明:
/// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 本次原子竞争成功返回真，被其它 owner 持有返回假；存储或协议失败时返回错误。
async fn try_acquire_coordinator_lease(
    backend: &LeaseBackend,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<bool> {
    match backend {
        LeaseBackend::LocalFile { path } => try_acquire_local_lease(path, owner_id, ttl_ms),
        LeaseBackend::Redis { config } => try_acquire_redis_lease(config, owner_id, ttl_ms).await,
    }
}

/// 业务作用: 仅在租约仍归属当前 owner 时延长有效期，失权则停止动作。
/// 参数说明:
/// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 租约仍归属当前 owner 并成功延长有效期时返回成功；失权或存储失败时返回错误。
async fn refresh_coordinator_lease(
    backend: &LeaseBackend,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    match backend {
        LeaseBackend::LocalFile { path } => refresh_local_lease(path, owner_id, ttl_ms),
        LeaseBackend::Redis { config } => refresh_redis_lease(config, owner_id, ttl_ms).await,
    }
}

/// 业务作用: 按本地文件或 Redis 后端确认当前 owner 仍持有有效租约。
/// 参数说明:
/// - `backend`: 当前操作对应的后端目标或连接句柄，用于转发命令并归属资源统计。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
///
/// 返回: 所选后端仍确认当前 owner 时返回成功；租约缺失、易主或读取失败时返回错误。
async fn ensure_coordinator_lease_active(
    backend: &LeaseBackend,
    owner_id: &str,
) -> anyhow::Result<()> {
    match backend {
        LeaseBackend::LocalFile { path } => ensure_local_lease_active(path, owner_id),
        LeaseBackend::Redis { config } => ensure_redis_lease_active(config, owner_id).await,
    }
}

/// 业务作用: 保存本地 HA 租约 owner 与绝对过期时间。
#[derive(Debug, Clone)]
struct LocalLease {
    owner_id: String,
    expires_at_ms: u128,
}

/// 业务作用: 持有本地租约 sidecar 排他锁，确保跨进程读改写互斥。
struct LocalLeaseFileLock {
    file: fs::File,
}

impl Drop for LocalLeaseFileLock {
    /// 业务作用: 释放 replication HA 主从探活、受控切换、租约互斥和持久化编排 的作用域资源，确保错误路径和正常路径都能回收计数或文件锁。
    fn drop(&mut self) {
        // 安全依据：`file` 在当前守卫整个生命周期内保持打开，文件描述符来自标准文件对象；
        // 调用只把该描述符传给系统的解锁操作，不构造指针、不转移所有权，也不在之后手工关闭。
        let rc = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        if rc != 0 {
            warn!(
                error = %std::io::Error::last_os_error(),
                "failed to unlock replication HA local lease sidecar"
            );
        }
    }
}

/// 业务作用: 持久化最近切主目标与完成时间，供其它本地实例复核并收敛。
#[derive(Debug, Clone)]
struct LocalFailoverOutcome {
    master: String,
    updated_at_ms: u128,
}

/// 业务作用: 在文件锁保护下读取或接管过期本地租约，并原子写入新 owner。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 本次原子竞争成功返回真，被其它 owner 持有返回假；存储或协议失败时返回错误。
fn try_acquire_local_lease(path: &Path, owner_id: &str, ttl_ms: u64) -> anyhow::Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create replication HA lease directory `{}`",
                parent.display()
            )
        })?;
    }
    let _lock = lock_local_lease_sidecar(path)?;
    let now = now_unix_millis()?;
    if path.exists() {
        match read_local_lease(path) {
            Ok(Some(lease)) if lease.expires_at_ms <= now => {
                fs::remove_file(path).with_context(|| {
                    format!("remove expired replication HA lease `{}`", path.display())
                })?;
            }
            Ok(Some(_)) => return Ok(false),
            Ok(None) => {}
            Err(err)
                if err
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == ErrorKind::NotFound) => {}
            Err(err)
                if is_local_lease_parse_error(&err)
                    && invalid_local_lease_expired(path, ttl_ms)? =>
            {
                warn!(
                    path = %path.display(),
                    error = %err,
                    "removing expired invalid replication HA lease"
                );
                fs::remove_file(path).with_context(|| {
                    format!(
                        "remove expired invalid replication HA lease `{}`",
                        path.display()
                    )
                })?;
            }
            Err(err) if is_local_lease_parse_error(&err) => return Ok(false),
            Err(err) => return Err(err),
        }
    }

    let expires_at_ms = now + u128::from(ttl_ms);
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::AlreadyExists => return Ok(false),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("create replication HA lease `{}`", path.display()));
        }
    };
    file.write_all(render_local_lease(owner_id, expires_at_ms).as_bytes())
        .with_context(|| format!("write replication HA lease `{}`", path.display()))?;
    Ok(true)
}

/// 业务作用: 在文件锁内确认 owner 未变化后刷新本地租约时间。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 文件租约仍归属当前 owner 并完成原子续期时返回成功；失权或文件失败时返回错误。
fn refresh_local_lease(path: &Path, owner_id: &str, ttl_ms: u64) -> anyhow::Result<()> {
    let _lock = lock_local_lease_sidecar(path)?;
    let Some(lease) = read_local_lease(path)? else {
        bail!("replication HA lease `{}` disappeared", path.display());
    };
    if lease.owner_id != owner_id {
        bail!(
            "replication HA lease `{}` is owned by `{}`",
            path.display(),
            lease.owner_id
        );
    }
    let now = now_unix_millis()?;
    if lease.expires_at_ms <= now {
        bail!("replication HA lease `{}` expired", path.display());
    }
    let expires_at_ms = now + u128::from(ttl_ms);
    atomic_write(path, &render_local_lease(owner_id, expires_at_ms))
        .with_context(|| format!("renew replication HA lease `{}`", path.display()))
}

/// 业务作用: 只删除仍属于当前 owner 的本地租约，避免迟到释放误删新持有者。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
///
/// 返回: 租约已安全删除或已不属于当前 owner 时返回成功；文件锁或删除失败时返回错误。
fn release_local_lease(path: &Path, owner_id: &str) -> anyhow::Result<()> {
    let _lock = lock_local_lease_sidecar(path)?;
    let Some(lease) = read_local_lease(path)? else {
        return Ok(());
    };
    if lease.owner_id == owner_id {
        fs::remove_file(path)
            .with_context(|| format!("remove replication HA lease `{}`", path.display()))?;
    }
    Ok(())
}

/// 业务作用: 在文件锁内确认本地租约未过期且 owner 匹配。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
///
/// 返回: 本地租约存在、未过期且 owner 匹配时返回成功；否则返回失权错误。
fn ensure_local_lease_active(path: &Path, owner_id: &str) -> anyhow::Result<()> {
    let _lock = lock_local_lease_sidecar(path)?;
    let Some(lease) = read_local_lease(path)? else {
        bail!("replication HA lease `{}` disappeared", path.display());
    };
    if lease.owner_id != owner_id {
        bail!(
            "replication HA lease `{}` is owned by `{}`",
            path.display(),
            lease.owner_id
        );
    }
    let now = now_unix_millis()?;
    if lease.expires_at_ms <= now {
        bail!("replication HA lease `{}` expired", path.display());
    }
    Ok(())
}

/// 业务作用: 读取仍在有效期内的本地租约 owner；过期、缺失或损坏租约均不授予权威。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
///
/// 返回: 状态存在且有效时返回文本，不存在或过期时返回空；读取、协议或校验失败时返回错误。
fn read_active_local_lease_owner(path: &Path) -> anyhow::Result<Option<String>> {
    let Some(lease) = read_local_lease(path)? else {
        return Ok(None);
    };
    let now = now_unix_millis()?;
    if lease.expires_at_ms <= now {
        return Ok(None);
    }
    Ok(Some(lease.owner_id))
}

/// 业务作用: 以大小上限和严格 JSON 校验读取本地租约文件，区分不存在、损坏与有效状态。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
///
/// 返回: 文件存在且内容合法时返回租约，不存在时返回空；读取、大小或解析异常时返回错误。
fn read_local_lease(path: &Path) -> anyhow::Result<Option<LocalLease>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("read replication HA lease `{}`", path.display()));
        }
    };
    let mut owner_id = None;
    let mut expires_at_ms = None;
    for line in raw.lines() {
        if let Some(value) = line.strip_prefix("owner=") {
            owner_id = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("expires_at_ms=") {
            expires_at_ms = value.parse::<u128>().ok();
        }
    }
    match (owner_id, expires_at_ms) {
        (Some(owner_id), Some(expires_at_ms)) => Ok(Some(LocalLease {
            owner_id,
            expires_at_ms,
        })),
        _ => bail!("replication HA lease `{}` is incomplete", path.display()),
    }
}

/// 业务作用: 把 replication HA 主从探活、受控切换、租约互斥和持久化编排 的内部状态渲染成外部命令、指标、文件内容或日志可读文本。
/// 参数说明:
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `expires_at_ms`: 本地租约过期的 Unix 毫秒时间戳。
///
fn render_local_lease(owner_id: &str, expires_at_ms: u128) -> String {
    format!("owner={owner_id}\nexpires_at_ms={expires_at_ms}\n")
}

/// 业务作用: 对本地租约的旁路锁文件取得进程级排他锁，把“读取旧租约、判断过期、写入新租约”串成单一临界区，避免同机多个代理同时接管。
/// 参数说明:
/// - `path`: 当前 HA 组的租约文件路径；函数据此派生独立锁文件，不锁住会被原子替换的租约文件本身。
///
/// 返回: 成功时返回持续持锁的守卫；目录创建、文件打开或系统加锁失败时返回带路径的错误。
fn lock_local_lease_sidecar(path: &Path) -> anyhow::Result<LocalLeaseFileLock> {
    let lock_path = local_lease_sidecar_path(path);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create replication HA lease sidecar directory `{}`",
                parent.display()
            )
        })?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .with_context(|| {
            format!(
                "open replication HA lease sidecar `{}`",
                lock_path.display()
            )
        })?;
    loop {
        // 安全依据：`file` 在循环和返回守卫的生命周期内保持打开，描述符有效；系统调用只对
        // 该描述符施加排他锁。被信号中断时重试，其他错误原样返回，避免无锁进入租约临界区。
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
        if rc == 0 {
            return Ok(LocalLeaseFileLock { file });
        }
        let err = std::io::Error::last_os_error();
        if err.kind() == ErrorKind::Interrupted {
            continue;
        }
        return Err(err).with_context(|| {
            format!(
                "lock replication HA lease sidecar `{}`",
                lock_path.display()
            )
        });
    }
}

/// 业务作用: 推导本地租约文件对应的锁文件路径。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
///
fn local_lease_sidecar_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.lock", path.to_string_lossy()))
}

/// 业务作用: 根据文件修改时间判断损坏租约是否已超过可安全接管期限。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 损坏租约文件年龄超过 TTL 时返回真；元数据或系统时间读取失败时返回错误。
fn invalid_local_lease_expired(path: &Path, ttl_ms: u64) -> anyhow::Result<bool> {
    let modified = match fs::metadata(path).and_then(|metadata| metadata.modified()) {
        Ok(modified) => modified,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("read replication HA lease metadata `{}`", path.display())
            });
        }
    };
    let modified_ms = unix_millis(modified)?;
    let now = now_unix_millis()?;
    Ok(now.saturating_sub(modified_ms) > u128::from(ttl_ms))
}

/// 业务作用: 识别允许按损坏租约恢复策略处理的 JSON 或 UTF-8 解析错误。
/// 参数说明:
/// - `err`: 错误或原因文本，用于保护态、日志和失败上下文。
///
fn is_local_lease_parse_error(err: &anyhow::Error) -> bool {
    err.to_string().contains(" is incomplete")
}

/// 业务作用: 通过同目录临时文件、刷盘和原子替换持久化租约状态。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `contents`: 租约或状态文件的完整内容，用于解析 owner、期限和故障转移结果。
///
/// 返回: 临时文件落盘并原子替换成功时返回成功；创建、刷盘或重命名失败时返回错误。
fn atomic_write(path: &Path, contents: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create replication HA file directory `{}`",
                parent.display()
            )
        })?;
    }
    for attempt in 0..100u32 {
        let tmp = atomic_temp_path(path, attempt)?;
        let mut file = match OpenOptions::new().write(true).create_new(true).open(&tmp) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("create temporary replication HA file `{}`", tmp.display())
                });
            }
        };
        if let Err(err) = file.write_all(contents.as_bytes()) {
            let _ = fs::remove_file(&tmp);
            return Err(err).with_context(|| {
                format!("write temporary replication HA file `{}`", tmp.display())
            });
        }
        if let Err(err) = file.sync_all() {
            let _ = fs::remove_file(&tmp);
            return Err(err).with_context(|| {
                format!("sync temporary replication HA file `{}`", tmp.display())
            });
        }
        drop(file);
        if let Err(err) = fs::rename(&tmp, path) {
            let _ = fs::remove_file(&tmp);
            return Err(err).with_context(|| {
                format!(
                    "rename temporary replication HA file `{}` to `{}`",
                    tmp.display(),
                    path.display()
                )
            });
        }
        return Ok(());
    }
    bail!(
        "failed to choose temporary replication HA file name for `{}`",
        path.display()
    )
}

/// 业务作用: 为一次原子写生成不与并发进程碰撞的同目录临时文件名。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `attempt`: 当前重试序号，用于退避计算、日志定位和重试上限判断。
///
/// 返回: 父目录和文件名合法时返回同目录唯一临时路径；路径结构非法时返回错误。
fn atomic_temp_path(path: &Path, attempt: u32) -> anyhow::Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("narix-ha-local");
    let now = now_unix_millis()?;
    Ok(path.with_file_name(format!(
        ".{name}.{}.{}.{}.tmp",
        std::process::id(),
        now,
        attempt
    )))
}

/// 业务作用: 原子持久化最近一次本地切主结果，供其它实例在租约竞争期间安全跟随已完成拓扑。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
/// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
///
/// 返回: 切主结果序列化并原子落盘成功时返回成功；序列化或文件系统失败时返回错误。
fn write_local_failover_outcome(path: &Path, master: &str) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create replication HA failover outcome directory `{}`",
                parent.display()
            )
        })?;
    }
    let updated_at_ms = now_unix_millis()?;
    atomic_write(path, &render_local_failover_outcome(master, updated_at_ms))
        .with_context(|| format!("write replication HA failover outcome `{}`", path.display()))
}

/// 业务作用: 在本地租约旁路锁保护下校验 owner，并原子写入本次切主结果。
///
/// 返回: owner 仍持有未过期租约且结果已写入时返回真；租约已转移或过期返回假。
fn write_local_failover_outcome_if_owner(
    lease_path: &Path,
    outcome_path: &Path,
    owner_id: &str,
    master: &str,
) -> anyhow::Result<bool> {
    let _lock = lock_local_lease_sidecar(lease_path)?;
    let Some(lease) = read_local_lease(lease_path)? else {
        return Ok(false);
    };
    let now = now_unix_millis()?;
    if lease.owner_id != owner_id || lease.expires_at_ms <= now {
        return Ok(false);
    }
    write_local_failover_outcome(outcome_path, master)?;
    Ok(true)
}

/// 业务作用: 读取并校验本地切主结果记录，拒绝超限、损坏或与当前配置不匹配的证据。
/// 参数说明:
/// - `path`: 文件路径或 HTTP 路径，决定本次读取、写入或请求的目标位置。
///
/// 返回: 有效记录存在时返回切主结果，不存在时返回空；读取、大小或解析异常时返回错误。
fn read_local_failover_outcome(path: &Path) -> anyhow::Result<Option<LocalFailoverOutcome>> {
    let raw = match fs::read_to_string(path) {
        Ok(raw) => raw,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("read replication HA failover outcome `{}`", path.display())
            });
        }
    };
    let mut master = None;
    let mut updated_at_ms = None;
    for line in raw.lines() {
        if let Some(value) = line.strip_prefix("master=") {
            master = Some(value.to_string());
        } else if let Some(value) = line.strip_prefix("updated_at_ms=") {
            updated_at_ms = value.parse::<u128>().ok();
        }
    }
    Ok(match (master, updated_at_ms) {
        (Some(master), Some(updated_at_ms)) => Some(LocalFailoverOutcome {
            master,
            updated_at_ms,
        }),
        _ => None,
    })
}

/// 业务作用: 把 replication HA 主从探活、受控切换、租约互斥和持久化编排 的内部状态渲染成外部命令、指标、文件内容或日志可读文本。
/// 参数说明:
/// - `master`: 目标 master 节点或地址，用于切换、复制跟随和路由一致性判断。
/// - `updated_at_ms`: 本地租约文件最近更新时间的 Unix 毫秒时间戳。
///
fn render_local_failover_outcome(master: &str, updated_at_ms: u128) -> String {
    format!("master={master}\nupdated_at_ms={updated_at_ms}\n")
}

/// 业务作用: 使用带 owner 和 TTL 的原子 Redis 脚本竞争动作租约。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: 本次原子竞争成功返回真，被其它 owner 持有返回假；存储或协议失败时返回错误。
async fn try_acquire_redis_lease(
    config: &RedisCoordinatorConfig,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<bool> {
    let ttl = ttl_ms.to_string();
    let value = redis_coordinator_command(
        config,
        &[
            b"SET".as_slice(),
            config.key.as_bytes(),
            owner_id.as_bytes(),
            b"NX".as_slice(),
            b"PX".as_slice(),
            ttl.as_bytes(),
        ],
    )
    .await?;
    match value {
        RespValue::Simple(value) if value == b"OK" => Ok(true),
        RespValue::Bulk(Some(value)) if value == b"OK" => Ok(true),
        RespValue::Bulk(None) | RespValue::Null => Ok(false),
        other => bail!(
            "unexpected Redis coordinator SET NX PX response {:?}",
            other
        ),
    }
}

/// 业务作用: 仅当 Redis 租约值仍匹配当前 owner 时续期。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
/// - `ttl_ms`: coordinator 租约的有效期毫秒数。
///
/// 返回: Redis 租约仍匹配当前 owner 并成功续期时返回成功；失权或命令失败时返回错误。
async fn refresh_redis_lease(
    config: &RedisCoordinatorConfig,
    owner_id: &str,
    ttl_ms: u64,
) -> anyhow::Result<()> {
    let ttl = ttl_ms.to_string();
    let value = redis_coordinator_command(
        config,
        &[
            b"EVAL".as_slice(),
            REDIS_RENEW_LEASE_SCRIPT.as_bytes(),
            b"1".as_slice(),
            config.key.as_bytes(),
            owner_id.as_bytes(),
            ttl.as_bytes(),
        ],
    )
    .await?;
    if resp_integer(&value)? == 1 {
        Ok(())
    } else {
        bail!(
            "replication HA Redis coordinator lease `{}` is no longer owned by `{}`",
            config.key,
            owner_id
        )
    }
}

/// 业务作用: 读取 Redis 租约值并确认仍与当前 owner 精确匹配。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
///
/// 返回: Redis 租约值仍匹配当前 owner 时返回成功；缺失、易主或命令失败时返回错误。
async fn ensure_redis_lease_active(
    config: &RedisCoordinatorConfig,
    owner_id: &str,
) -> anyhow::Result<()> {
    let value = redis_coordinator_command(
        config,
        &[
            b"EVAL".as_slice(),
            REDIS_CHECK_LEASE_SCRIPT.as_bytes(),
            b"1".as_slice(),
            config.key.as_bytes(),
            owner_id.as_bytes(),
        ],
    )
    .await?;
    let ttl = resp_integer(&value)?;
    if ttl > 0 {
        Ok(())
    } else {
        bail!(
            "replication HA Redis coordinator lease `{}` is not active for `{}`",
            config.key,
            owner_id
        )
    }
}

/// 业务作用: 仅当 Redis 租约仍匹配当前 owner 时删除租约键。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
/// - `owner_id`: 当前代理持有协调器租约时使用的唯一 owner 标识，用于防止其他实例误续约或误释放。
///
/// 返回: 租约已安全删除或已不属于当前 owner 时返回成功；Redis 命令失败时返回错误。
async fn release_redis_lease(
    config: &RedisCoordinatorConfig,
    owner_id: &str,
) -> anyhow::Result<()> {
    let _ = redis_coordinator_command(
        config,
        &[
            b"EVAL".as_slice(),
            REDIS_RELEASE_LEASE_SCRIPT.as_bytes(),
            b"1".as_slice(),
            config.key.as_bytes(),
            owner_id.as_bytes(),
        ],
    )
    .await?;
    Ok(())
}

/// 业务作用: 查询外部 Redis 中仍有效的租约 owner，供忙碌原因和状态展示使用。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
///
/// 返回: 状态存在且有效时返回文本，不存在或过期时返回空；读取、协议或校验失败时返回错误。
async fn read_redis_lease_owner(config: &RedisCoordinatorConfig) -> anyhow::Result<Option<String>> {
    let value =
        redis_coordinator_command(config, &[b"GET".as_slice(), config.key.as_bytes()]).await?;
    match value {
        RespValue::Bulk(Some(value)) => Ok(Some(String::from_utf8(value)?)),
        RespValue::Bulk(None) | RespValue::Null => Ok(None),
        other => bail!(
            "unexpected Redis coordinator lease GET response {:?}",
            other
        ),
    }
}

/// 业务作用: 仅在 Redis 租约仍属于指定 owner 时，以同一 Lua 脚本写入带 TTL 的切主结果。
///
/// 返回: 租约匹配且结果已写入时返回真；租约已转移或过期返回假。
async fn write_redis_failover_outcome_if_owner(
    config: &RedisCoordinatorConfig,
    owner_id: &str,
    master: &str,
    ttl_ms: u64,
) -> anyhow::Result<bool> {
    let ttl = ttl_ms.to_string();
    let value = redis_coordinator_command(
        config,
        &[
            b"EVAL".as_slice(),
            REDIS_WRITE_OUTCOME_SCRIPT.as_bytes(),
            b"2".as_slice(),
            config.key.as_bytes(),
            config.outcome_key.as_bytes(),
            owner_id.as_bytes(),
            master.as_bytes(),
            ttl.as_bytes(),
        ],
    )
    .await?;
    match value {
        RespValue::Integer(1) => Ok(true),
        RespValue::Integer(0) => Ok(false),
        other => bail!(
            "unexpected Redis coordinator guarded outcome response {:?}",
            other
        ),
    }
}

/// 业务作用: 从外部 Redis 读取并反序列化近期切主结果，供多实例收敛复核。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
///
/// 返回: 状态存在且有效时返回文本，不存在或过期时返回空；读取、协议或校验失败时返回错误。
async fn read_redis_failover_outcome(
    config: &RedisCoordinatorConfig,
) -> anyhow::Result<Option<String>> {
    let value =
        redis_coordinator_command(config, &[b"GET".as_slice(), config.outcome_key.as_bytes()])
            .await?;
    match value {
        RespValue::Bulk(Some(value)) => Ok(Some(String::from_utf8(value)?)),
        RespValue::Bulk(None) | RespValue::Null => Ok(None),
        other => bail!(
            "unexpected Redis coordinator outcome GET response {:?}",
            other
        ),
    }
}

/// 业务作用: 建立受超时约束的协调 Redis 连接，完成认证并执行一条命令。
/// 参数说明:
/// - `config`: 配置对象，提供本次构建、校验或运行所需的参数。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 返回一条完整协调 Redis 响应；连接、认证、超时或协议解析失败时返回错误。
async fn redis_coordinator_command(
    config: &RedisCoordinatorConfig,
    args: &[&[u8]],
) -> anyhow::Result<RespValue> {
    let mut conn = BackendConnection::connect_tracked_with_timeouts(BackendConnectOptions {
        addr: &config.addr,
        username: config.username.as_deref(),
        password: config.password.as_deref(),
        connect_timeout_ms: config.connect_timeout_ms,
        request_timeout_ms: config.request_timeout_ms,
        tcp_keepalive_ms: config.tcp_keepalive_ms,
        response_bytes_limit: None,
        metrics: None,
        tls: None,
    })
    .await?;
    let frames = conn.send_pipeline(&[resp::array(args)]).await?;
    let frame = frames
        .into_iter()
        .next()
        .context("missing Redis coordinator response")?;
    let Some((value, consumed)) = resp::parse_value(&frame)? else {
        bail!("incomplete Redis coordinator response")
    };
    if consumed != frame.len() {
        bail!("Redis coordinator response has trailing bytes")
    }
    if let RespValue::Error(error) = value {
        bail!(
            "Redis coordinator returned error: {}",
            String::from_utf8_lossy(&error)
        );
    }
    Ok(value)
}

/// 业务作用: 把协调 Redis 的 RESP 整数回复解析为租约脚本结果。
/// 参数说明:
/// - `value`: 待写入、解析或比较的业务值。
///
/// 返回: 值符合 RESP integer 语义时返回数值；类型或编码非法时返回错误。
fn resp_integer(value: &RespValue) -> anyhow::Result<i64> {
    match value {
        RespValue::Integer(value) => Ok(*value),
        other => bail!("expected Redis integer response, got {:?}", other),
    }
}
