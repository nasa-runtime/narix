use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{default_application_path, ProxyConfig, ProxyItem};
use crate::infrastructure::credentials::Materials;
use nasa::secret::SecretBytes;

use super::MAX_CLUSTER_CONFIG_BYTES;

/// 表示一次已经完成加载、占位符解析和静态校验的有效配置。
///
/// 摘要包含有效配置树和 TLS 依赖文件内容，但对象不会保存或对外暴露有效配置树，
/// 避免管理状态或调试输出意外泄露密码与证书正文。
#[derive(Clone)]
pub struct LoadedConfig {
    config: Arc<ProxyConfig>,
    digest: Arc<str>,
    proxy_digests: Arc<BTreeMap<String, String>>,
    admin_digest: Arc<str>,
    coordination_digest: Arc<str>,
    process_digest: Arc<str>,
    log_digest: Arc<str>,
    reload_digest: Arc<str>,
    node_bootstrap_digest: Arc<str>,
    cluster_payload: Arc<SecretBytes>,
    cluster_digest: Arc<str>,
    source_paths: Arc<Vec<PathBuf>>,
}

impl LoadedConfig {
    /// 业务作用: 比较本机日志配置，独立于尚未实现热变更的进程字段。
    /// 参数说明: 无。
    ///
    /// 返回: 当前候选日志设置的确定性摘要。
    pub fn log_digest(&self) -> &str {
        &self.log_digest
    }

    /// 业务作用: 返回本次加载得到的强类型配置共享句柄。
    ///
    /// 返回: 调用方只读共享的完整强类型配置。
    pub fn config(&self) -> Arc<ProxyConfig> {
        self.config.clone()
    }

    /// 业务作用: 返回有效配置与 TLS 依赖内容共同生成的确定性摘要。
    ///
    /// 返回: 六十四位小写十六进制 SHA-256 摘要。
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// 业务作用: 返回各代理项按稳定名称索引的确定性摘要。
    ///
    /// 返回: 代理名称到六十四位 SHA-256 摘要的有序映射。
    pub fn proxy_digests(&self) -> &BTreeMap<String, String> {
        &self.proxy_digests
    }

    /// 业务作用: 返回管理端有效配置摘要，用于识别尚不允许热更新的监听变化。
    ///
    /// 返回: 管理端配置对应的六十四位 SHA-256 摘要。
    pub fn admin_digest(&self) -> &str {
        &self.admin_digest
    }

    /// 业务作用: 返回进程级协调配置摘要，用于识别需要协调事务的成员或传输变化。
    ///
    /// 返回: 协调配置与其 TLS 文件共同生成的六十四位 SHA-256 摘要。
    pub fn coordination_digest(&self) -> &str {
        &self.coordination_digest
    }

    /// 业务作用: 返回代理、管理端和协调配置之外的进程级有效配置摘要。
    ///
    /// 返回: 排除已独立管理的日志与热更新策略后，其余进程级字段的六十四位 SHA-256 摘要。
    pub fn process_digest(&self) -> &str {
        &self.process_digest
    }

    /// 业务作用: 返回热更新 supervisor 自身配置的确定性摘要。
    ///
    /// 返回: 轮询、防抖和事务时限配置的六十四位 SHA-256 摘要。
    pub fn reload_digest(&self) -> &str {
        &self.reload_digest
    }

    /// 业务作用: 判断候选配置是否保持本节点不可热迁移的进程身份不变。
    ///
    /// 参数说明:
    /// - `candidate`: 从本节点配置来源加载、即将生成集群提案的候选版本。
    ///
    /// 返回: 协调身份、管理监听和协调 TLS 材料兼容时返回真；业务代理监听主机是
    /// 可热迁移的节点本地覆盖，不参与该进程身份判断。
    pub(crate) fn local_bootstrap_compatible_with(&self, candidate: &LoadedConfig) -> bool {
        self.node_bootstrap_digest == candidate.node_bootstrap_digest
    }

    /// 业务作用: 返回用于集群 gRPC 广播和本地持久化的规范化业务配置载荷。
    ///
    /// 返回: 已排除节点本地身份字段、仍可能包含业务凭据的 JSON 字节；调用方不得记录正文。
    pub(crate) fn cluster_payload(&self) -> &[u8] {
        self.cluster_payload.expose()
    }

    /// 业务作用: 返回规范化集群业务配置载荷的确定性摘要。
    ///
    /// 返回: 全部成员必须逐字一致的六十四位 SHA-256 摘要。
    pub(crate) fn cluster_digest(&self) -> &str {
        &self.cluster_digest
    }

    /// 业务作用: 返回参与本次配置版本计算的全部文件路径。
    ///
    /// 返回: 已排序并去重的基础、profile、外部配置和 TLS 文件路径。
    pub fn source_paths(&self) -> &[PathBuf] {
        &self.source_paths
    }
}

/// 保存可重复加载的配置来源，供启动流程、文件轮询和管理接口共同使用。
#[derive(Debug, Clone)]
pub struct ConfigSource {
    base_file: PathBuf,
    external_file: Option<PathBuf>,
}

impl ConfigSource {
    /// 业务作用: 固定本进程的基础配置与外部覆盖文件，使启动和热更新使用同一来源。
    ///
    /// 参数说明:
    /// - `external_config`: 显式外部配置路径，优先于环境变量；空白值按未配置处理。
    ///
    /// 返回: 按显式路径、`APP_CONFIG_FILE`、兼容变量 `NARIX_CONFIG` 的顺序选择非空路径。
    /// 文件存在性与内容在加载时验证；指定文件不可用时拒绝加载，不回退到低优先级来源。
    pub fn from_external(external_config: Option<&str>) -> Self {
        let external_file = external_config
            .map(str::trim)
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                ["APP_CONFIG_FILE", "NARIX_CONFIG"]
                    .into_iter()
                    .find_map(|name| std::env::var_os(name).filter(|value| !value.is_empty()))
                    .map(PathBuf::from)
            });
        // 路径只在来源创建时选择；后续文件缺失或候选非法不能静默切换到另一配置。
        Self::new(default_application_path(), external_file)
    }

    /// 业务作用: 使用明确的基础配置和外部配置路径创建来源描述。
    ///
    /// 参数说明:
    /// - `base_file`: NARIX 基础配置文件路径。
    /// - `external_file`: 可选的部署配置文件路径。
    ///
    /// 返回: 规范化空路径后的配置来源。
    pub fn new(base_file: PathBuf, external_file: Option<PathBuf>) -> Self {
        let external_file = external_file.filter(|path| !path.as_os_str().is_empty());
        Self {
            base_file,
            external_file,
        }
    }

    /// 业务作用: 重新读取全部配置来源、执行静态校验并计算确定性摘要。
    /// 参数说明: 无。
    ///
    /// 返回: 已校验的配置快照；读取、解析、证书依赖或校验失败时返回带路径上下文的错误。
    /// 多线程运行时在同步读取和解析期间移交其它任务的调度，配置结果仍由调用方串行校验和发布。
    pub fn load(&self) -> anyhow::Result<LoadedConfig> {
        // 文件读取、配置解析和证书摘要都同步完成；移交 I/O 调度后仍在本次调用内
        // 构造候选，避免异步取消留下可独立发布的配置加载任务。
        crate::infrastructure::blocking::with_blocking_work(|| {
            let external = self
                .external_file
                .as_deref()
                .map(|path| {
                    path.to_str()
                        .context("external config path is not valid UTF-8")
                })
                .transpose()?;
            let (config, tree) = ProxyConfig::load_effective_from(&self.base_file, external)
                .context("load effective NARIX configuration")?;
            config.validate().context("validate NARIX configuration")?;
            let paths = config.source_paths.clone();
            build_loaded_config(config, tree, paths)
        })
    }

    /// 业务作用: 使用领导节点广播的规范化集群配置正文构造本节点候选配置。
    ///
    /// 参数说明:
    /// - `payload`: 已通过 gRPC 身份校验和摘要校验的集群托管 JSON 字节。
    ///
    /// 返回: 叠加本节点身份、监听、存储目录和协调 TLS 路径后的完整候选配置。
    pub(crate) fn load_cluster_payload(&self, payload: &[u8]) -> anyhow::Result<LoadedConfig> {
        if payload.len() > MAX_CLUSTER_CONFIG_BYTES {
            anyhow::bail!("cluster configuration payload exceeds 16 MiB");
        }
        let mut managed: Value = serde_json::from_slice(payload)
            .context("parse cluster-managed configuration payload")?;
        if !managed.is_object() {
            anyhow::bail!("cluster-managed configuration must be a JSON object");
        }
        let external = self
            .external_file
            .as_deref()
            .map(|path| {
                path.to_str()
                    .context("external config path is not valid UTF-8")
            })
            .transpose()?;
        let (local_config, local_tree) =
            ProxyConfig::load_effective_from(&self.base_file, external)
                .context("load local bootstrap configuration")?;
        overlay_local_bootstrap(&mut managed, &local_tree)?;
        let config: ProxyConfig = serde_json::from_value(managed.clone())
            .context("deserialize cluster-managed configuration")?;
        config
            .validate()
            .context("validate cluster-managed configuration")?;
        build_loaded_config(config, managed, local_config.source_paths)
    }
}

/// 业务作用: 将领导节点广播配置与本机协调身份及管理监听字段合并成可运行配置。
///
/// 参数说明:
/// - `managed`: 领导节点广播且已经移除节点本地字段的配置树。
/// - `local`: 本节点从受保护引导配置加载的有效配置树。
///
/// 返回: 本地字段存在并成功写入候选配置时返回成功。
fn overlay_local_bootstrap(managed: &mut Value, local: &Value) -> anyhow::Result<()> {
    let local_coordination = local
        .get("coordination")
        .and_then(Value::as_object)
        .context("local bootstrap coordination config must be an object")?;
    let managed_coordination = managed
        .get_mut("coordination")
        .and_then(Value::as_object_mut)
        .context("cluster-managed coordination config must be an object")?;
    for field in ["node_id", "listen", "advertise", "data_dir"] {
        let value = local_coordination
            .get(field)
            .cloned()
            .with_context(|| format!("local bootstrap coordination config is missing `{field}`"))?;
        managed_coordination.insert(field.to_string(), value);
    }
    let managed_tls = managed_coordination
        .entry("tls")
        .or_insert_with(|| Value::Object(Default::default()))
        .as_object_mut()
        .context("cluster-managed coordination TLS config must be an object")?;
    let local_tls = local_coordination.get("tls").and_then(Value::as_object);
    for field in ["certificate_path", "private_key_path", "client_ca_path"] {
        // TLS 关闭时允许最小配置省略整个 tls 对象；强类型配置会应用相同的空值默认值。
        // TLS 开启时静态校验已要求三个路径全部存在，因此这里不会掩盖缺失证书配置。
        let value = local_tls
            .and_then(|tls| tls.get(field))
            .cloned()
            .unwrap_or(Value::Null);
        managed_tls.insert(field.to_string(), value);
    }
    overlay_required_object_field(managed, local, "admin", "listen")?;
    // 日志目录和过滤属于本机运维配置，不将领导者的文件路径发布到其他节点。
    if let Some(object) = managed.as_object_mut() {
        object.insert(
            "log".to_string(),
            local.get("log").cloned().unwrap_or(Value::Null),
        );
    }
    overlay_existing_proxy_hosts(managed, local)?;
    Ok(())
}

/// 业务作用: 从本地引导配置覆盖一个节点独占的对象字段。
///
/// 参数说明:
/// - `managed`: 正在补齐节点本地字段的集群托管树。
/// - `local`: 本节点受保护引导配置的有效树。
/// - `object_name`: 顶层对象名称。
/// - `field_name`: 只能由本节点配置提供的字段名称。
///
/// 返回: 两侧对象存在且本地字段成功复制时返回成功。
fn overlay_required_object_field(
    managed: &mut Value,
    local: &Value,
    object_name: &str,
    field_name: &str,
) -> anyhow::Result<()> {
    let value = local
        .get(object_name)
        .and_then(Value::as_object)
        .and_then(|object| object.get(field_name))
        .cloned()
        .with_context(|| {
            format!("local bootstrap `{object_name}.{field_name}` field is missing")
        })?;
    managed
        .get_mut(object_name)
        .and_then(Value::as_object_mut)
        .with_context(|| format!("cluster-managed `{object_name}` config must be an object"))?
        .insert(field_name.to_string(), value);
    Ok(())
}

/// 业务作用: 为本机已经存在的同名代理补回节点独占的绑定和公布主机。
///
/// 参数说明:
/// - `managed`: 集群托管配置树；业务端口仍由集群统一管理。
/// - `local`: 本节点受保护引导配置，提供主机地址覆盖。
///
/// 返回: 配置树结构有效时返回成功；候选新增代理没有本地覆盖时保留安全默认主机。
fn overlay_existing_proxy_hosts(managed: &mut Value, local: &Value) -> anyhow::Result<()> {
    let local_proxies = local
        .get("proxies")
        .and_then(Value::as_array)
        .context("local bootstrap proxies must be an array")?;
    let local_by_name = local_proxies
        .iter()
        .filter_map(|proxy| {
            Some((
                proxy.get("name")?.as_str()?.to_string(),
                proxy.as_object()?.clone(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let managed_proxies = managed
        .get_mut("proxies")
        .and_then(Value::as_array_mut)
        .context("cluster-managed proxies must be an array")?;
    for managed_proxy in managed_proxies {
        let managed_proxy = managed_proxy
            .as_object_mut()
            .context("cluster-managed proxy item must be an object")?;
        let name = managed_proxy
            .get("name")
            .and_then(Value::as_str)
            .context("cluster-managed proxy item requires string name")?;
        let Some(local_proxy) = local_by_name.get(name) else {
            continue;
        };
        for field in ["listen_host", "cluster_node_advertise_host"] {
            if let Some(value) = local_proxy.get(field) {
                managed_proxy.insert(field.to_string(), value.clone());
            }
        }
        overlay_existing_node_proxy_hosts(managed_proxy, local_proxy)?;
    }
    Ok(())
}

/// 业务作用: 为同一代理项内已经存在的节点代理补回节点独占主机字段。
///
/// 参数说明:
/// - `managed_proxy`: 正在合并本地字段的集群托管代理项。
/// - `local_proxy`: 本节点受保护引导配置中的同名代理项。
///
/// 返回: `node_proxies` 结构有效且所有同后端节点的主机覆盖完成时返回成功。
fn overlay_existing_node_proxy_hosts(
    managed_proxy: &mut serde_json::Map<String, Value>,
    local_proxy: &serde_json::Map<String, Value>,
) -> anyhow::Result<()> {
    let Some(managed_nodes) = managed_proxy
        .get_mut("node_proxies")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };
    let local_nodes = local_proxy
        .get("node_proxies")
        .and_then(Value::as_array)
        .context("local bootstrap node_proxies must be an array")?;
    let local_by_backend = local_nodes
        .iter()
        .filter_map(|node| {
            Some((
                node.get("backend")?.as_str()?.to_string(),
                node.as_object()?.clone(),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    for managed_node in managed_nodes {
        let managed_node = managed_node
            .as_object_mut()
            .context("cluster-managed node proxy item must be an object")?;
        let backend = managed_node
            .get("backend")
            .and_then(Value::as_str)
            .context("cluster-managed node proxy item requires string backend")?;
        let Some(local_node) = local_by_backend.get(backend) else {
            continue;
        };
        for field in ["listen_host", "advertise_host"] {
            if let Some(value) = local_node.get(field) {
                managed_node.insert(field.to_string(), value.clone());
            }
        }
    }
    Ok(())
}

/// 业务作用: 从强类型配置和有效树构造摘要、代理差异索引和集群同步载荷。
///
/// 参数说明:
/// - `config`: 已通过静态校验的完整强类型配置。
/// - `tree`: 与强类型配置逐字段对应的有效配置树。
/// - `source_paths`: 当前节点实际读取的基础、profile 和外部配置路径。
///
/// 返回: 纳入 TLS 依赖内容并可交给运行时事务的配置快照。
fn build_loaded_config(
    mut config: ProxyConfig,
    tree: Value,
    mut source_paths: Vec<PathBuf>,
) -> anyhow::Result<LoadedConfig> {
    let coordination_tls_paths = coordination_tls_dependency_paths(&config);
    let mut dependency_paths = coordination_tls_paths.clone();
    for proxy in &config.proxies {
        dependency_paths.extend(proxy_tls_dependency_paths(proxy));
    }
    dependency_paths.sort();
    dependency_paths.dedup();

    source_paths.extend(dependency_paths.iter().cloned());
    source_paths.sort();
    source_paths.dedup();

    let materials = Materials::prepare(&mut config, &dependency_paths)?;
    for item in &mut config.proxies {
        crate::cluster::backend::prepare_tls(item)?;
    }
    let digest = digest_value_and_files(&tree, &dependency_paths, &materials)?;
    let admin_digest = digest_value(tree.get("admin").unwrap_or(&Value::Null))?;
    let coordination_digest = digest_value_and_files(
        tree.get("coordination").unwrap_or(&Value::Null),
        &coordination_tls_paths,
        &materials,
    )?;
    let process_digest = process_config_digest(&tree)?;
    let reload_digest = digest_value(tree.get("reload").unwrap_or(&Value::Null))?;
    let local_bootstrap = local_bootstrap_tree(&tree);
    let node_bootstrap_digest = digest_value_and_files(
        &node_bootstrap_tree(&local_bootstrap),
        &coordination_tls_paths,
        &materials,
    )?;
    let proxy_digests = proxy_digests(&tree, &config.proxies)?;
    let cluster_tree = cluster_managed_tree(&tree);
    let cluster_payload =
        serde_json::to_vec(&cluster_tree).context("serialize cluster-managed configuration")?;
    let cluster_digest = digest_bytes(&cluster_payload);

    Ok(LoadedConfig {
        config: Arc::new(config),
        digest: Arc::from(digest),
        proxy_digests: Arc::new(proxy_digests),
        admin_digest: Arc::from(admin_digest),
        coordination_digest: Arc::from(coordination_digest),
        process_digest: Arc::from(process_digest),
        log_digest: Arc::from(digest_value(tree.get("log").unwrap_or(&Value::Null))?),
        reload_digest: Arc::from(reload_digest),
        node_bootstrap_digest: Arc::from(node_bootstrap_digest),
        cluster_payload: Arc::new(SecretBytes::new(cluster_payload)),
        cluster_digest: Arc::from(cluster_digest),
        source_paths: Arc::new(source_paths),
    })
}

/// 业务作用: 生成可以在不同 NARIX 节点间逐字广播的集群托管配置树。
///
/// 参数说明:
/// - `tree`: 当前节点完成全部来源合并后的有效配置树。
///
/// 返回: 移除节点编号、管理监听、存储目录和本机 TLS 私钥路径后的配置树。
fn cluster_managed_tree(tree: &Value) -> Value {
    let mut managed = tree.clone();
    if let Some(object) = managed.as_object_mut() {
        object.remove("log");
    }
    if let Some(admin) = managed.get_mut("admin").and_then(Value::as_object_mut) {
        admin.remove("listen");
    }
    if let Some(coordination) = managed
        .get_mut("coordination")
        .and_then(Value::as_object_mut)
    {
        for field in ["node_id", "listen", "advertise", "data_dir"] {
            coordination.remove(field);
        }
        if let Some(tls) = coordination.get_mut("tls").and_then(Value::as_object_mut) {
            for field in ["certificate_path", "private_key_path", "client_ca_path"] {
                tls.remove(field);
            }
        }
    }
    if let Some(proxies) = managed.get_mut("proxies").and_then(Value::as_array_mut) {
        for proxy in proxies {
            let Some(proxy) = proxy.as_object_mut() else {
                continue;
            };
            proxy.remove("listen_host");
            proxy.remove("cluster_node_advertise_host");
            if let Some(nodes) = proxy.get_mut("node_proxies").and_then(Value::as_array_mut) {
                for node in nodes {
                    let Some(node) = node.as_object_mut() else {
                        continue;
                    };
                    node.remove("listen_host");
                    node.remove("advertise_host");
                }
            }
        }
    }
    managed
}

/// 业务作用: 提取只能由每个节点本地引导配置维护的字段，用于区分本地身份变化与代理 TLS 文件轮换。
///
/// 参数说明:
/// - `tree`: 当前节点完成全部来源合并后的有效配置树。
///
/// 返回: 仅包含协调身份、监听主机和本地 TLS 路径的确定性配置树。
fn local_bootstrap_tree(tree: &Value) -> Value {
    let mut local = serde_json::Map::new();
    if let Some(admin) = tree.get("admin").and_then(Value::as_object) {
        let mut local_admin = serde_json::Map::new();
        copy_present_fields(admin, &mut local_admin, &["listen"]);
        local.insert("admin".to_string(), Value::Object(local_admin));
    }
    if let Some(coordination) = tree.get("coordination").and_then(Value::as_object) {
        let mut local_coordination = serde_json::Map::new();
        copy_present_fields(
            coordination,
            &mut local_coordination,
            &["node_id", "listen", "advertise", "data_dir"],
        );
        if let Some(tls) = coordination.get("tls").and_then(Value::as_object) {
            let mut local_tls = serde_json::Map::new();
            copy_present_fields(
                tls,
                &mut local_tls,
                &["certificate_path", "private_key_path", "client_ca_path"],
            );
            local_coordination.insert("tls".to_string(), Value::Object(local_tls));
        }
        local.insert(
            "coordination".to_string(),
            Value::Object(local_coordination),
        );
    }
    let local_proxies = tree
        .get("proxies")
        .and_then(Value::as_array)
        .map(|proxies| {
            proxies
                .iter()
                .filter_map(|proxy| {
                    let proxy = proxy.as_object()?;
                    let mut local_proxy = serde_json::Map::new();
                    copy_present_fields(
                        proxy,
                        &mut local_proxy,
                        &["name", "listen_host", "cluster_node_advertise_host"],
                    );
                    if let Some(nodes) = proxy.get("node_proxies").and_then(Value::as_array) {
                        let local_nodes = nodes
                            .iter()
                            .filter_map(|node| {
                                let node = node.as_object()?;
                                let mut local_node = serde_json::Map::new();
                                copy_present_fields(
                                    node,
                                    &mut local_node,
                                    &["backend", "listen_host", "advertise_host"],
                                );
                                Some(Value::Object(local_node))
                            })
                            .collect();
                        local_proxy.insert("node_proxies".to_string(), Value::Array(local_nodes));
                    }
                    Some(Value::Object(local_proxy))
                })
                .collect()
        })
        .unwrap_or_default();
    local.insert("proxies".to_string(), Value::Array(local_proxies));
    Value::Object(local)
}

/// 业务作用: 从本地引导树中提取整个进程生命周期内不可热迁移的节点身份。
///
/// 参数说明:
/// - `local_bootstrap`: 已仅保留节点本地字段的配置树。
///
/// 返回: 管理监听、协调身份、数据目录和协调 TLS 路径组成的确定性树；
/// 业务代理主机覆盖可通过监听迁移热更新，因此不属于进程级不可变身份。
fn node_bootstrap_tree(local_bootstrap: &Value) -> Value {
    let mut node = local_bootstrap.clone();
    if let Some(object) = node.as_object_mut() {
        object.remove("proxies");
    }
    node
}

/// 业务作用: 从一个配置对象复制明确列出的现有字段。
///
/// 参数说明:
/// - `source`: 提供字段值的有效配置对象。
/// - `target`: 接收字段副本的本地配置对象。
/// - `fields`: 允许复制的字段名称集合。
fn copy_present_fields(
    source: &serde_json::Map<String, Value>,
    target: &mut serde_json::Map<String, Value>,
    fields: &[&str],
) {
    for field in fields {
        if let Some(value) = source.get(*field) {
            target.insert((*field).to_string(), value.clone());
        }
    }
}

/// 业务作用: 计算除代理、管理端、协调、日志和 reload 配置外的进程级有效配置摘要。
///
/// 参数说明:
/// - `tree`: 完成全部来源合并和占位符解析的有效配置树。
///
/// 返回: 不允许热变更的其他进程级字段确定性摘要。
fn process_config_digest(tree: &Value) -> anyhow::Result<String> {
    let mut process_tree = tree.clone();
    if let Some(object) = process_tree.as_object_mut() {
        object.remove("proxies");
        object.remove("admin");
        object.remove("coordination");
        object.remove("reload");
        object.remove("log");
    }
    digest_value(&process_tree)
}

/// 业务作用: 计算有效配置树中每个代理项的独立摘要，并纳入该代理引用的 TLS 文件内容。
///
/// 参数说明:
/// - `tree`: 完成全部来源合并和占位符解析的有效配置树。
/// - `proxies`: 已通过反序列化和静态校验的代理配置列表。
///
/// 返回: 代理名称到摘要的有序映射；有效树与强类型配置不一致时返回错误。
fn proxy_digests(tree: &Value, proxies: &[ProxyItem]) -> anyhow::Result<BTreeMap<String, String>> {
    let proxy_values = tree
        .get("proxies")
        .and_then(Value::as_array)
        .context("effective config proxies must be an array")?;
    let mut values_by_name = BTreeMap::new();
    for value in proxy_values {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .context("effective proxy item requires string name")?;
        values_by_name.insert(name, value);
    }

    let mut out = BTreeMap::new();
    for proxy in proxies {
        let value = values_by_name.get(proxy.name.as_str()).with_context(|| {
            format!(
                "effective config tree does not contain proxy `{}`",
                proxy.name
            )
        })?;
        let paths = proxy_tls_dependency_paths(proxy);
        out.insert(
            proxy.name.clone(),
            digest_value_and_files(value, &paths, &proxy.materials)?,
        );
    }
    Ok(out)
}

/// 业务作用: 收集一个代理运行时实际依赖的后端和 Sentinel TLS 文件。
///
/// 参数说明:
/// - `proxy`: 已通过静态解析的单个代理配置。
///
/// 返回: 已排序并去重的证书、私钥和 CA 文件路径。
fn proxy_tls_dependency_paths(proxy: &ProxyItem) -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    if proxy.backend_tls_enabled {
        extend_optional_path(&mut paths, proxy.backend_tls_ca_path.as_deref());
        extend_optional_path(&mut paths, proxy.backend_tls_certificate_path.as_deref());
        extend_optional_path(&mut paths, proxy.backend_tls_private_key_path.as_deref());
    }
    if proxy.sentinel_tls_enabled {
        extend_optional_path(&mut paths, proxy.sentinel_tls_ca_path.as_deref());
        extend_optional_path(&mut paths, proxy.sentinel_tls_certificate_path.as_deref());
        extend_optional_path(&mut paths, proxy.sentinel_tls_private_key_path.as_deref());
    }
    paths.into_iter().collect()
}

/// 业务作用: 收集进程级协调传输实际依赖的双向 TLS 文件。
///
/// 参数说明:
/// - `config`: 已通过静态解析的完整 NARIX 配置。
///
/// 返回: 已排序并去重的协调证书、私钥和客户端 CA 路径。
fn coordination_tls_dependency_paths(config: &ProxyConfig) -> Vec<PathBuf> {
    let mut paths = BTreeSet::new();
    if config.coordination.tls.enabled {
        extend_optional_path(
            &mut paths,
            config.coordination.tls.certificate_path.as_deref(),
        );
        extend_optional_path(
            &mut paths,
            config.coordination.tls.private_key_path.as_deref(),
        );
        extend_optional_path(
            &mut paths,
            config.coordination.tls.client_ca_path.as_deref(),
        );
    }
    paths.into_iter().collect()
}

/// 业务作用: 将非空配置路径加入有序依赖集合。
///
/// 参数说明:
/// - `paths`: 正在收集的去重路径集合。
/// - `candidate`: 配置中的可选文件路径。
fn extend_optional_path(paths: &mut BTreeSet<PathBuf>, candidate: Option<&str>) {
    if let Some(path) = candidate.filter(|path| !path.trim().is_empty()) {
        paths.insert(PathBuf::from(path));
    }
}

/// 业务作用: 计算一个有效配置片段的确定性 SHA-256 摘要。
///
/// 参数说明:
/// - `value`: 已完成合并和占位符解析的 JSON 配置片段。
///
/// 返回: 六十四位小写十六进制摘要。
fn digest_value(value: &Value) -> anyhow::Result<String> {
    digest_value_and_files(value, &[], &Materials::default())
}

/// 业务作用: 计算配置片段以及所引用文件内容共同形成的确定性摘要。
///
/// 参数说明:
/// - `value`: 已完成合并和占位符解析的 JSON 配置片段。
/// - `paths`: 内容变化必须触发新版本的依赖文件路径。
/// - `materials`: 与运行代共同准备的文件快照，摘要计算不得再次读取文件。
///
/// 返回: 六十四位小写十六进制摘要；依赖文件无法读取时返回明确路径错误。
fn digest_value_and_files(
    value: &Value,
    paths: &[PathBuf],
    materials: &Materials,
) -> anyhow::Result<String> {
    let mut hasher = Sha256::new();
    let encoded = serde_json::to_vec(value).context("serialize effective config for digest")?;
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    for path in paths {
        let bytes = materials.file(path)?;
        let path_text = path.to_string_lossy();
        hasher.update((path_text.len() as u64).to_be_bytes());
        hasher.update(path_text.as_bytes());
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(hex_digest(hasher.finalize().as_slice()))
}

/// 业务作用: 计算任意规范化字节载荷的 SHA-256 摘要。
///
/// 参数说明:
/// - `bytes`: 需要在集群节点间校验逐字一致性的配置载荷。
///
/// 返回: 六十四位小写十六进制摘要。
fn digest_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_digest(hasher.finalize().as_slice())
}

/// 业务作用: 将 SHA-256 原始字节编码为稳定的小写十六进制文本。
///
/// 参数说明:
/// - `bytes`: 摘要算法输出的固定长度原始字节。
///
/// 返回: 每个字节展开为两个字符的十六进制文本。
fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
