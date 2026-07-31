# NARIX 配置参考

README 只保留各模式能够启动并接受运维检查的最小配置。本目录保存可复制的全量 YAML：

| 模式或部署形态 | 全量配置 |
|---|---|
| 固定单后端 | [`configuration/standalone.yml`](configuration/standalone.yml) |
| Sentinel 动态单主 | [`configuration/sentinel.yml`](configuration/sentinel.yml) |
| 标准槽位 Cluster 单入口 | [`configuration/cluster.yml`](configuration/cluster.yml) |
| 标准槽位 Cluster 节点代理 | [`configuration/cluster-node-proxy.yml`](configuration/cluster-node-proxy.yml) |
| replication-ha + Redis 8.2.2 | [`configuration/replication-ha-redis.yml`](configuration/replication-ha-redis.yml) |
| replication-ha + Valkey 9.1.1 | [`configuration/replication-ha-valkey.yml`](configuration/replication-ha-valkey.yml) |
| replication-ha + Redict 7.3.6 | [`configuration/replication-ha-redict.yml`](configuration/replication-ha-redict.yml) |
| replication-ha + KeyDB 6.3.4 | [`configuration/replication-ha-keydb.yml`](configuration/replication-ha-keydb.yml) |
| replication-ha + Kvrocks 2.16.0 | [`configuration/replication-ha-kvrocks.yml`](configuration/replication-ha-kvrocks.yml) |
| replication-ha + Dragonfly 单 NARIX 节点、软隔离自动切换 | [`configuration/replication-ha-dragonfly.yml`](configuration/replication-ha-dragonfly.yml) |
| replication-ha + Dragonfly 三节点内置协调集群 | [`configuration/replication-ha-dragonfly-proxy-cluster.yml`](configuration/replication-ha-dragonfly-proxy-cluster.yml) |

这些文件显式展开当前模式适用的全部字段，并为每个配置项提供中文注释；未针对具体部署调整的字段沿用程序默认值，地址、凭据、证书和生产模板中的容量或策略值属于待替换示例。互斥配置不能同时生效，例如 `ha.coordinator.mode` 只能选择 `none`、`local-file`、`redis` 或 `proxy-cluster` 中的一种；省略该字段时会按 `coordination.enabled` 自动解析（组集群→`proxy-cluster`，单实例→`none`）。内置 [`../zcf/application.yml`](../zcf/application.yml) 只保留安全启动与配置热更新所需的最小字段，不再重复维护全量字段说明。

## 加载与合并

配置优先级从低到高：

```text
1. zcf/application.yml
2. zcf/application-${NARIX_PROFILE}.yml
3. --config 或 NARIX_CONFIG 指定的外部文件
4. 外部文件同目录的 ${文件名}-${NARIX_PROFILE}.yml
5. NARIX_ 字段环境变量
6. ${...} 配置占位符
```

支持 `.yml`、`.yaml`、`.json` 和 `.toml`；没有扩展名时按 YAML 解析。`NARIX_PROFILE` 对应文件不存在时自动跳过。

合并规则：

- 普通对象按字段递归合并。
- 文件显式写出 `proxies[]` 时，该数组按 `name` 表示完整期望集合；缺失名称会被删除。
- 文件显式写出 `node_proxies[]` 时，该数组按 `backend` 表示完整期望集合；缺失后端会被删除。
- 同一个配置来源内不允许出现重复 `name` 或重复 `backend`。
- 高优先级文件省略整个数组时继承低优先级数组；写出数组时，同身份条目继承未覆盖字段。
- 环境变量只做身份或下标定向覆盖，不删除文件层数组中的其他元素。

外部文件可以只写部署差异。内置配置的 `proxies` 为空，不会给外部配置附带隐藏 listener。

## 顶层配置

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启动健康、状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端绑定地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 认证令牌。
  allow_unauthenticated: false # 是否允许管理接口免认证；生产保持 false。

coordination: # NARIX 内置协调控制面配置。
  enabled: false # 当前 standalone 示例不启用协调器。
  peers: [] # 协调器关闭时静态成员表必须为空。

proxies: # 业务代理项列表。
  - name: redis-primary # 代理项唯一名称。
    backend_profile: redis # 后端产品能力档案。
    mode: standalone # 固定单后端模式。
    listen: 7001 # 客户端连接 NARIX 的业务端口。
    host: 127.0.0.1 # 固定后端主机。
    port: 6379 # 固定后端端口。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
```

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `admin` | 启用 | 管理 HTTP 服务。 |
| `coordination` | 禁用 | NARIX 内置协调控制面，可同时承担 HA 动作协调和全模式集群配置同步。 |
| `reload` | 启用 | 文件、SIGHUP、管理接口和集群配置热更新策略。 |
| `proxies` | `[]` | 代理项列表；管理端或配置热更新启用时允许暂时为空。 |

## 管理端

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `admin.enabled` | `true` | 是否启动管理 HTTP listener。 |
| `admin.listen` | `127.0.0.1:18080` | 管理端绑定地址，必须是 `IP:port`。 |
| `admin.token` | 内置拒绝启动的占位值 | 受保护端点使用的 Bearer token 或 `X-Admin-Token`；部署时必须覆盖。 |
| `admin.allow_unauthenticated` | `false` | 允许受保护端点免认证；只应用于本地或可信隔离环境。 |

`admin.enabled=true` 且 `allow_unauthenticated=false` 时必须提供非空 token。内置占位值 `change-me-admin-token` 会被启动校验拒绝。

无需认证的端点：

```text
GET /health
GET /ready
```

其余管理端点均需要认证，包括 `/status`、`/metrics`、`/config/*` 和全部 `/ha/*` 查询或控制接口。

## 配置热更新

```yaml
reload: # 配置来源检查和发布事务参数。
  enabled: true # 启用文件检查、SIGHUP 和管理接口热更新。
  poll_interval_ms: 1000 # 检查配置文件及 TLS 依赖内容的周期。
  stable_observations: 2 # 相同有效摘要连续出现多少次才自动发布。
  transaction_timeout_ms: 60000 # 单次加载、预检、协调和发布事务的时间预算。
```

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `reload.enabled` | `true` | 是否启用热更新入口。 |
| `reload.poll_interval_ms` | `1000` | 文件和证书依赖检查周期，单位毫秒。 |
| `reload.stable_observations` | `2` | 文件自动发布前需要连续观察到的相同有效摘要次数，最小为 2。 |
| `reload.transaction_timeout_ms` | `60000` | 加载、预检、集群协调、listener 切换和运行代发布的事务预算，单位毫秒；有效范围为 `1..=86400000`。旧业务连接在重新开门后后台排空，不占用该事务预算；HA 外部动作开始后不会因调用方断开而异步取消，而是使用各步骤超时安全收口。 |

管理接口：

```text
GET  /config/reload
POST /config/validate
POST /config/reload
```

`validate` 只做加载、强类型校验、监听地址和后端预检，不发布运行代。`reload` 发布多个代理项时，先准备全部候选，再短暂关闭进程级新连接门禁；所有新增、修改和删除项全部成功或完整回滚后才重新开放，因此新连接不会观察到混合配置。成功后新连接使用新运行代，旧连接收到关闭信号并按连接类型排空。`proxies[]`、`node_proxies[]` 和 `replication-ha` 节点集合均按候选期望状态发布，删除项会从运行时注销。

启用 `coordination` 后，集群托管配置由领导节点生成规范化正文和摘要。节点编号、协调监听/通告地址、数据目录、协调 TLS 文件路径、管理监听地址，以及业务监听和节点代理公布地址中的主机部分仍由各节点本地引导配置提供；业务端口、代理项和其余策略在全部成员间保持同一 revision 和摘要。

## 后端档案与模式

`backend_profile` 是运行时能力选择，不是展示标签。它决定 HELLO 产品身份、RESP3 准入、后端握手和拓扑模式校验。

| `backend_profile` | standalone | Sentinel | 标准 cluster / node proxy | replication-ha |
|---|---:|---:|---:|---:|
| `redis` | 是 | 是 | 是 | 是 |
| `valkey` | 是 | 是 | 是 | 是 |
| `dragonfly` | 是 | 否 | 否 | 是 |
| `kvrocks` | 是 | 否 | 是 | 是 |
| `garnet` | 是 | 否 | 是 | 否 |
| `keydb` | 是 | 否 | 是 | 是 |
| `redict` | 是 | 是 | 是 | 是 |
| `pika` | 是 | 否 | 否 | 否 |
| `tendis` | 是 | 否 | 是 | 否 |

各模式必需的后端字段：

| 模式 | 必需字段 | 说明 |
|---|---|---|
| `standalone` | `listen`、`host`、`port` | 固定单后端。 |
| `sentinel` | `listen`、`sentinel_master_name`、`sentinel_nodes` | Sentinel 发现并校验动态主节点。 |
| `cluster` | `listen`、`nodes` | `nodes` 为逗号分隔的标准 Cluster seed。 |
| `cluster-node-proxy` | `node_proxies[]` | 父项不使用 `listen`；每个子项定义自己的 listener。 |
| `replication-ha` | `listen`、`nodes`、`ha` | `backend_profile` 必须是已完成 HA 适配的产品。 |

`replication-ha` 的节点 token 使用：

```text
client_addr@replication_addr
```

多个 token 用逗号分隔。

Redis 8.2.2、Valkey 9.1.1、Redict 7.3.6 和 KeyDB 6.3.4 的计划切换使用原生 `FAILOVER TO`。每个副本必须通过产品支持的 announce 配置向主节点宣布与自身 `replication_addr` 完全对应的地址；主节点未把该地址登记为在线副本时，NARIX 会在执行切换前拒绝事务，不能在多副本拓扑中猜测目标。Kvrocks 2.16.0 使用 `CLIENT PAUSE WRITE`、复制 offset 门禁和 `SLAVEOF` 完成受控主从切换，不宣称具有单命令原子切换语义。

## 代理项通用字段

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `name` | 必填 | 代理项唯一名称，也是日志、指标和配置合并身份。 |
| `backend_profile` | `redis` | 后端能力档案。非 Redis 后端应显式填写。 |
| `mode` | 必填 | `standalone`、`sentinel`、`cluster`、`cluster-node-proxy`、`replication-ha`。 |
| `listen_host` | `0.0.0.0` | 业务 listener 绑定主机。 |
| `listen` | `0` | 除 node proxy 外必须配置为非零端口。 |
| `host` / `port` | `null` | standalone 后端地址。 |
| `nodes` | `null` | Cluster seed 或托管复制 HA 节点清单。 |
| `username` | `null` | 后端 ACL 用户名。 |
| `password` | `null` | 后端认证密码。 |
| `backend_tls_enabled` | `false` | 是否对业务数据节点启用受证书校验的 RESP TLS。 |
| `backend_tls_ca_path` | `null` | 数据节点 CA PEM 文件；启用 TLS 时必填。 |
| `backend_tls_server_name` | `null` | 数据节点 SNI；空值时从实际目标地址提取。 |
| `backend_tls_certificate_path` | `null` | 可选 mTLS 客户端证书，必须与私钥同时配置。 |
| `backend_tls_private_key_path` | `null` | 可选 mTLS 客户端私钥，必须与证书同时配置。 |
| `frontend_password` | `null` | NARIX 校验的前端密码。 |
| `auth_mode` | 自动 | `terminate`、`backend-inject`、`pass-through`。 |
| `allow_unauthenticated` | `false` | 是否显式允许客户端跳过代理侧认证。 |
| `read_only` | `false` | 开启统一只读门禁。未知命令和透明隧道命令 fail-closed。 |

认证模式：

| 模式 | 行为与约束 |
|---|---|
| `terminate` | NARIX 校验 `frontend_password`，再用独立 `username`/`password` 访问后端。 |
| `backend-inject` | NARIX 为后端注入凭据；必须显式设置 `allow_unauthenticated: true`。 |
| `pass-through` | AUTH 原样交给当前连接绑定的后端；必须设置 `allow_unauthenticated: true`。仅适用于 `standalone` 和写数据面使用透明 relay 的 `replication-ha`；后者切主时会关闭连接，由客户端重连后重新认证。Sentinel 和两个 Cluster 模式不支持。 |

未显式设置 `auth_mode` 时，有 `frontend_password` 选择 `terminate`，否则选择 `backend-inject`。

## Sentinel

Sentinel 控制节点只承载拓扑查询；普通命令、pipeline、事务、阻塞命令和 Pub/Sub 都连接 Sentinel 返回且经 NARIX 验证为 master 的业务数据节点。

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `sentinel_master_name` | `null` | Sentinel 监控的服务名，sentinel 模式必填。 |
| `sentinel_nodes` | `null` | 逗号分隔的 Sentinel `host:port`，sentinel 模式必填。 |
| `sentinel_username` | `null` | Sentinel ACL 用户名，与数据节点用户名分离。 |
| `sentinel_password` | `null` | Sentinel 认证密码，与数据节点密码分离。 |
| `sentinel_connect_timeout_ms` | `300` | Sentinel 控制连接和候选角色探测建连超时。 |
| `sentinel_request_timeout_ms` | `1000` | Sentinel 命令和候选角色校验响应超时。 |
| `sentinel_refresh_interval_ms` | `1000` | 后台重新查询当前主节点的周期。 |
| `sentinel_tls_enabled` | `false` | 是否对 Sentinel 控制连接启用 RESP TLS。 |
| `sentinel_tls_ca_path` | `null` | Sentinel CA PEM 文件；启用 TLS 时必填。 |
| `sentinel_tls_server_name` | `null` | Sentinel SNI；空值时从当前控制节点地址提取。 |
| `sentinel_tls_certificate_path` | `null` | 可选 Sentinel mTLS 客户端证书。 |
| `sentinel_tls_private_key_path` | `null` | 可选 Sentinel mTLS 客户端私钥。 |

NARIX 使用 `SENTINEL get-master-addr-by-name` 获取候选地址，再通过 `ROLE` 或 `INFO replication` 确认主角色。只有验证成功的地址才会发布到数据面。切主会创建新代连接资源，旧代不接受新批次；事务、订阅和无限阻塞连接会断开，由业务 SDK 按正常重连流程恢复。

## RESP TLS

业务数据节点 TLS 与 Sentinel 控制连接 TLS 完全独立。两者都强制校验证书链和服务端身份，不提供跳过校验的配置。

- 只需要服务端认证时配置 `*_tls_enabled` 与 `*_tls_ca_path`。
- 证书身份与连接地址不一致时配置 `*_tls_server_name`。
- 双向 TLS 需要同时配置 `*_tls_certificate_path` 与 `*_tls_private_key_path`。
- TLS 总握手时间计入对应 connect timeout。
- 状态接口只展示 TLS 是否开启，不输出证书路径、密钥路径或认证凭据。

## 连接、容量和超时

下表默认值与全量 YAML 一致：

| 字段 | 默认值 | 单位/说明 |
|---|---:|---|
| `dedicated_tunnel_max_active` | `1024` | 专用后端连接活跃上限；`0` 表示不限制。 |
| `dedicated_tunnel_queue_size` | `1024` | 专用连接池满后的等待队列。 |
| `dedicated_tunnel_acquire_timeout_ms` | `1000` | 获取专用连接许可的超时。 |
| `dedicated_tunnel_pool_max_idle` | `1000` | 每个真实后端保留的 clean idle tunnel 数；`0` 关闭复用。 |
| `dedicated_tunnel_pool_idle_timeout_ms` | `30000` | clean tunnel 空闲回收时间；`0` 关闭复用。 |
| `connect_timeout_ms` | `3000` | 后端 TCP 建连超时。 |
| `backend_request_timeout_ms` | `10000` | 普通后端请求超时。 |
| `frontend_auth_timeout_ms` | `10000` | 前端完成 AUTH/HELLO 初始化的超时。 |
| `client_idle_timeout_ms` | `0` | 已认证客户端空闲超时；`0` 不主动关闭。 |
| `tcp_keepalive_ms` | `0` | TCP keepalive 周期；`0` 使用系统行为。 |
| `max_connections` | `65535` | 当前代理项最大前端连接数。 |
| `max_auth_bytes` | `16384` | 认证阶段最大累计输入字节。 |
| `pipeline_max_commands` | `1000` | 单批 pipeline 最大命令数。 |
| `pipeline_max_bytes` | `4194304` | 单批 pipeline 最大输入字节。 |
| `pipeline_backend_max_active` | `5000` | pipeline 后端连接池最大活跃连接。 |
| `pipeline_backend_queue_size` | `4096` | pipeline 池等待队列。 |
| `pipeline_backend_submit_timeout_ms` | `1000` | pipeline 提交等待超时。 |
| `pipeline_backend_idle_timeout_ms` | `60000` | pipeline 后端空闲连接回收时间。 |
| `frame_max_bytes` | `4194304` | 单 RESP frame 上限，不能大于 `pipeline_max_bytes`。 |
| `bulk_max_bytes` | `4194304` | 单 bulk string 上限。 |
| `aggregate_max_concurrency` | `32` | 跨 Cluster 节点聚合并发。 |
| `aggregate_max_items` | `10000` | 聚合类命令最大 item 数。 |
| `pending_response_bytes` | `16777216` | 单前端连接待写响应上限。 |
| `graceful_shutdown_timeout_ms` | `10000` | 优雅退出等待时间。 |
| `cluster_topology_refresh_ms` | `60000` | Cluster 后台拓扑刷新周期。 |

DedicatedTunnel 用于 Pub/Sub、阻塞命令、事务、`SELECT` 及其他连接绑定语义。普通单命令和普通 pipeline 不占用该限额。

## cluster-node-proxy 节点字段

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `cluster_node_advertise_host` | `null` | 全部子节点共享的公布主机。 |
| `node_proxies[].backend` | 必填 | 真实 Cluster 节点 `host:port`，同一父项内唯一。 |
| `node_proxies[].listen` | 必填 | 当前子 listener 端口。 |
| `node_proxies[].advertise_host` | 父级值 | 返回给 Cluster Client 的主机。 |
| `node_proxies[].advertise_port` | `listen` | 返回给 Cluster Client 的端口。 |
| `node_proxies[].listen_host` | 父级值 | 当前子 listener 的绑定主机。 |
| `node_proxies[].aliases` | `[]` | 归一化到该节点代理地址的后端别名。 |

`advertise_host` 或父级 `cluster_node_advertise_host` 至少提供一个。

## 托管复制 HA

`mode: replication-ha` 是六个托管复制产品的统一入口，`backend_profile` 选择产品适配器。当前放行 Redis 8.2.2、Valkey 9.1.1、Dragonfly 1.39.0、Redict 7.3.6、Kvrocks 2.16.0 和 KeyDB 6.3.4。

### 探测与 fencing

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `ha.probe_interval_ms` | `1000` | 全部节点角色和复制状态探测周期。 |
| `ha.probe_connect_timeout_ms` | 继承 `connect_timeout_ms` | HA 角色与复制状态探测的独立建连超时。 |
| `ha.probe_request_timeout_ms` | 继承 `backend_request_timeout_ms` | HA 角色与复制状态探测的独立响应超时。 |
| `ha.master_down_after_failures` | `3` | 连续多少轮主节点失败形成故障判断。 |
| `ha.max_replica_lag` | `0` | 候选最大已知复制延迟秒数；`0` 只关闭数值上限。主库宕机导致的断链副本按断链时长（`master_link_down_since_seconds`，是真实延迟的可证明下界）参与同一门禁与最小延迟排序；全量同步、未知延迟、断链时长不可知或错误上游仍不合格。持久化 owner 门禁不接受任何断链副本。 |
| `ha.auto_failover` | `true` | 是否允许自动故障转移。主故障达到 `master_down_after_failures` 后立即进入保护，再经过协调、fencing、候选选择与提升恢复路由；租约忙、候选不足或动作退避期间都保持保护。设为 `false` 只关闭“NARIX 主动发起切主”，但已确认主故障仍会关闭路由；观测跟随、旧主归队降级、副本漂移修复等拓扑收敛仍然开启。恢复走两条证据通道之一：旧主被探测为新主直接副本（降级证词）时立即跟随；旧主永久死亡无法作证时，唯一接管者连续稳定满同样的 `master_down_after_failures` 轮后按受限跟随通道恢复，无需人工介入。启动/热更新构造期只发现拓扑，不提升副本。 |
| `ha.observer` | `false` | 观察者模式：控制面只探测和跟随，**绝不发送任何后端控制命令**；手动拓扑接口一并拒绝并隐含 `auto_failover=false`。切路由需要证据：旧主被探测为新主直接副本（降级证词）时立即跟随；旧主永久死亡无法作证时，在旧主连败确认宕机且唯一接管者连续稳定满 `master_down_after_failures` 轮后按受限跟随通道恢复。证据不足与双主期间保持 `protected`。启动校验强制不加入协调集群、`coordinator=none`、`persistence.enabled=false`、fencing 为 `manual`。`observer` 不等于数据面只读，旁路只读端点还必须设置 `read_only=true`。 |
| `ha.fencing.mode` | `manual` | `manual`（软隔离，默认）：切主后靠 `REPLICAOF` 把旧主降级为副本，零脚本但存在短暂双写风险。双写窗口上界：单实例部署为旧主复活到被降级的一个探测周期；集群部署下提主前会先等满一个 `route_guard_timeout` 让分区成员断路，残余窗口为分区成员察觉已提交结果前的一个探测周期。`external-command`（强隔离）：切主前运行隔离程序，以零脑裂为目标；安全性取决于程序能否可靠切断旧主写入，需要 `command`。 |
| `ha.fencing.command` | `[]` | 外部隔离程序及独立参数，不经 shell 拼接。 |
| `ha.fencing.timeout_ms` | `10000` | 外部隔离动作超时。 |

`external-command` 的隔离命令是运维填写、直接执行（不经 shell）的可执行程序，操作对象是旧主后端节点（如云 API 关机、iptables 封端口）；退出码即隔离结果，非零/超时视为隔离失败并进入保护态，绝不提升候选。fencing 参数可使用 `{proxy}`、`{client_addr}`、`{replication_addr}`、`{action_token}`。

协调器（`ha.coordinator`）与 fencing 是两个正交机制：**fencing 隔离旧主后端节点**（防旧主残留双写），**协调器串行化多个 NARIX 实例的切主动作**（防多实例同时切）。单实例的 `coordinator: none` 只提供进程内动作串行，不等于旧主已被强隔离；多实例部署同一组后端时必须配 `proxy-cluster` 或独立 `redis` 协调器。`local-file` 只能协调同机进程，不能跨机器。

单实例 + `coordinator: none` 有一条部署红线：**同一组后端在任一时刻只允许存在一个承担控制面的 NARIX 进程**。进程内锁不跨进程，蓝绿发布或热升级若出现新旧进程同时在线的重叠窗口，等价于两套互不感知的控制面。做不到零重叠时,必须为单机重叠场景显式配置 `local-file` 协调器兜底。红线的唯一豁免是 `ha.observer: true` 的观察者节点——它不发送任何后端控制命令,可以与生产控制面共管同一组后端,数量不限。

### 协调器

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `ha.coordinator.mode` | 省略：随 `coordination.enabled` 自动解析 | 省略时：组内置协调集群（`coordination.enabled=true`）自动为 `proxy-cluster`（leader 切主），单实例自动为 `none`（进程内串行）。显式可选 `none`/`local-file`/`redis`/`proxy-cluster`，但组集群时显式非 `proxy-cluster` 会被拒绝。 |
| `ha.coordinator.owner_id` | 自动生成 | local-file/redis 模式的实例稳定身份。 |
| `ha.coordinator.lease_path` | `null` | local-file 租约路径。 |
| `ha.coordinator.group_id` | 省略取代理项名 | proxy-cluster 中同一后端拓扑的稳定组名；省略时默认使用代理项名称（集群内各成员同名代理天然一致且唯一）。 |
| `ha.coordinator.observation_quorum` | `0` | 故障观察票数；`0` 使用静态成员多数派。 |
| `ha.coordinator.observation_window_ms` | `3000` | 相同故障指纹计票窗口。 |
| `ha.coordinator.redis_addr` | `null` | 独立协调 Redis 地址，不能使用被切换的数据源。 |
| `ha.coordinator.redis_username` | `null` | 独立协调 Redis ACL 用户名。 |
| `ha.coordinator.redis_password` | `null` | 独立协调 Redis 密码。 |
| `ha.coordinator.redis_key` | `null` | 同一 HA 组共享的租约键。 |
| `ha.coordinator.redis_connect_timeout_ms` | `3000` | 独立协调 Redis 建连超时。 |
| `ha.coordinator.redis_request_timeout_ms` | `3000` | 独立协调 Redis 请求超时。 |
| `ha.coordinator.lease_ttl_ms` | `15000` | local-file/redis 租约 TTL。 |
| `ha.coordinator.lease_renew_interval_ms` | `5000` | 续租周期，必须小于 TTL。 |
| `ha.coordinator.lease_acquire_timeout_ms` | `3000` | 单轮竞争租约的最长等待。 |

跨机器自动切换不能使用 `local-file`。新部署优先使用 `proxy-cluster`；已经具备独立高可用 Redis 时也可使用 `redis`。

### 动态持久化

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `ha.persistence.enabled` | `false` | 是否由 NARIX 动态分配和迁移持久化责任。 |
| `ha.persistence.fail_closed` | `true` | 无法证明持久化责任安全时关闭业务路由。 |
| `ha.persistence.force_checkpoint_on_handoff` | `true` | owner 交接时是否立即生成产品原生持久化点。 |
| `ha.persistence.min_replica_stable_ms` | `5000` | 副本承担责任前的最短稳定时间。 |
| `ha.persistence.require_zero_lag` | `true` | 是否只接受归一化延迟为零的副本。 |
| `ha.persistence.dragonfly.snapshot_cron` | `*/5 * * * *` | Dragonfly 持久化 owner 的快照周期。 |
| `ha.persistence.dragonfly.dbfilename` | `dump-{role}-{client_addr_safe}-{timestamp}` | Dragonfly 快照文件名模板。 |
| `ha.persistence.dragonfly.dir` | `null` | Dragonfly 可选快照目录；1.39.0 不支持动态修改，非空时要求节点启动值已一致。 |
| `ha.persistence.dragonfly.verify_timeout_ms` | `30000` | Dragonfly 强制快照验证超时。 |
| `ha.persistence.redis.rdb.enabled` | `true` | Redis owner 是否启用周期 RDB。 |
| `ha.persistence.redis.rdb.save_rules[]` | `300/10` | Redis `save` 的秒数与变更次数规则。 |
| `ha.persistence.redis.aof.enabled` | `true` | Redis owner 是否启用 AOF。 |
| `ha.persistence.redis.aof.fsync` | `everysec` | Redis AOF fsync 策略。 |
| `ha.persistence.redis.verify_timeout_ms` | `10000` | Redis 强制 RDB 持久化点验证超时。 |
| `ha.persistence.valkey.*` | 无 | Valkey 使用与 Redis 块相同的 `rdb`、`aof` 和 `verify_timeout_ms` 结构。 |
| `ha.persistence.redict.*` | 无 | Redict 使用与 Redis 块相同的 `rdb`、`aof` 和 `verify_timeout_ms` 结构。 |
| `ha.persistence.keydb.*` | 无 | KeyDB 使用与 Redis 块相同的 RDB/AOF 配置，并以 `LASTSAVE` 验证持久化点。 |

动态持久化只编排责任，不把异步复制变成零数据丢失 WAL。
NARIX 固定先启用并验证新 owner，再停用旧 owner；该安全顺序不可通过配置关闭。Redis-family 和 Dragonfly 的秒级复制心跳在正常采样时会在 0 与 1 之间跳动，适配器结合复制 offset 或扣除一个秒级采样误差后再执行 `require_zero_lag` 门禁。Dragonfly `dir` 非空时，每次采用已有 owner 或迁移新 owner 都会重新读取并校验节点启动值。

Kvrocks 是例外：必须设置 `ha.persistence.enabled: false`，且不能配置任何产品持久化子块。NARIX 只管理其主从拓扑；全部节点继续使用自身 RocksDB/WAL 持久化。

## NARIX 内置协调集群

`coordination` 是进程级配置，同一进程内的 `replication-ha` 项共享。恰好 2 个静态成员固定使用双节点 2/2 协议；至少 3 个成员固定使用多数派协议。

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `enabled` | `false` | 是否启动协调 gRPC 服务。 |
| `cluster_id` | `""` | 全体成员永久一致的集群标识。 |
| `node_id` | `0` | 当前成员稳定正整数编号。 |
| `listen` | `127.0.0.1:19090` | 本地协调监听地址。 |
| `advertise` | `127.0.0.1:19090` | 其他成员访问本节点的地址；端口与 listen 相同。 |
| `data_dir` | `""` | 当前节点独占的任期、日志和快照目录。 |
| `bootstrap_node_id` | `0` | 首次建群成员编号，必须存在于 peers。 |
| `peers` | `[]` | 完整静态成员表，不能按当前存活节点删减。 |
| `heartbeat_interval_ms` | `500` | 领导者心跳周期。 |
| `election_timeout_min_ms` | `1500` | 选举超时下界，必须大于两倍心跳。 |
| `election_timeout_max_ms` | `3000` | 选举超时上界。 |
| `rpc_timeout_ms` | `1000` | 成员间单次 RPC 超时。 |
| `route_guard_timeout_ms` | `5000` | 未确认权威后仍可沿用路由的保护窗口。 |
| `action_timeout_ms` | `30000` | 外部拓扑动作令牌有效期。 |
| `snapshot_log_entries` | `10000` | 多数派模式触发控制日志快照的条目数。 |
| `pair_election_retry_ms` | `500` | 双节点竞选失败重试间隔。 |
| `pair_allow_single_survivor` | `false` | 是否允许可靠隔离对端代理后单存活接管。 |
| `pair_peer_fencing_command` | `[]` | 隔离失联对端 NARIX 的外部命令。 |
| `pair_peer_fencing_timeout_ms` | `5000` | 对端代理隔离超时。 |

`coordination.tls`：

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `enabled` | `false` | 双向 TLS。关闭时全部协调地址只能使用回环 IP。 |
| `certificate_path` | `null` | 当前节点证书链 PEM。 |
| `private_key_path` | `null` | 当前节点私钥 PEM。 |
| `client_ca_path` | `null` | 验证其他成员客户端证书的 CA PEM。 |

TLS 开启后，每个 `peers[]` 还必须提供 `node_id`、`address`、`server_name` 和不带冒号的 64 位叶子证书 DER SHA-256 指纹 `certificate_sha256`。

## 环境变量

管理端字段：

```text
NARIX_ADMIN_ENABLED
NARIX_ADMIN_LISTEN
NARIX_ADMIN_TOKEN
NARIX_ADMIN_ALLOW_UNAUTHENTICATED
```

代理项使用从 0 开始的数组下标，把字段名转换成大写下划线：

```text
proxies[0].backend_profile
-> NARIX_PROXIES_0_BACKEND_PROFILE

proxies[0].pipeline_backend_max_active
-> NARIX_PROXIES_0_PIPELINE_BACKEND_MAX_ACTIVE

proxies[0].backend_tls_enabled
-> NARIX_PROXIES_0_BACKEND_TLS_ENABLED

proxies[0].sentinel_tls_ca_path
-> NARIX_PROXIES_0_SENTINEL_TLS_CA_PATH

proxies[0].node_proxies[1].advertise_port
-> NARIX_PROXIES_0_NODE_PROXIES_1_ADVERTISE_PORT
```

例：

```bash
NARIX_ADMIN_TOKEN='production-admin-token' \
NARIX_PROXIES_0_HOST='192.0.2.21' \
NARIX_PROXIES_0_PORT=6379 \
NARIX_PROXIES_0_PASSWORD='backend-password' \
NARIX_PROXIES_0_FRONTEND_PASSWORD='frontend-password' \
target/release/narix --config=/etc/narix/narix.yml
```

字符串列表可使用 JSON 数组、逗号分隔文本或逐项下标，例如节点代理别名：

```bash
NARIX_PROXIES_0_NODE_PROXIES_0_ALIASES='["redis-a.internal:6379","10.0.0.11:6379"]'
```

`ha.*` 与 `coordination.*` 当前不支持逐字段 `NARIX_` 覆盖。每个协调成员应使用自己的外部 YAML 或 profile 文件；热更新也以完整候选 YAML 为事务边界，避免只覆盖局部 HA 安全门禁。

## 占位符

`${...}` 在所有配置和环境变量合并后解析，点号访问对象，十进制数字访问数组：

```yaml
admin: # 管理端 HTTP 服务配置。
  listen: "${proxies.0.listen_host}:18080" # 复用第一个代理项的监听主机并固定管理端端口。

proxies: # 占位符可以通过数组下标引用其中的标量字段。
  - name: redis-primary # 代理项唯一名称。
    backend_profile: redis # 后端产品能力档案。
    mode: standalone # 固定单后端模式。
    listen_host: 127.0.0.1 # 业务监听主机，也被上方 admin.listen 引用。
    listen: 7001 # 客户端连接 NARIX 的业务端口。
    host: 127.0.0.1 # 固定后端主机。
    port: 6379 # 固定后端端口。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
```

占位符只能引用字符串、数字或布尔值。引用不存在、引用对象/数组/null 或形成循环时，NARIX 拒绝启动。

## 部署前检查

- 替换所有示例 token、密码、证书路径、指纹、主机名和 IP。
- 不把真实密钥提交到仓库；通过外部配置、环境变量或密钥挂载注入。
- 检查业务 listener、管理端和协调端之间没有端口冲突。
- Cluster node proxy 的公布地址必须能从业务客户端访问。
- 核对每个后端的部署版本与 README 支持矩阵完全一致；后端复制语义由固定版本认证保障，不需要使用者自行验证，NARIX 也不在运行时校验版本号。
- 上线前演练部署接线：主故障端到端切换（含负载均衡按 `/ready` 摘挂与告警触达）；配置了 `fencing.mode=external-command` 时在真实基础设施上演练隔离脚本的执行、超时与失败进入保护。
- 结合文件描述符、后端连接上限和内存预算下调示例中的连接容量。
