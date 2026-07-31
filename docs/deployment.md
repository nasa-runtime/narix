# NARIX 部署

## 选择数据面模式

| 模式 | 入口形态 | 适用后端 |
|---|---|---|
| `standalone` | 一个 NARIX listener 对应一个固定后端 | Redis、Valkey、Dragonfly、Kvrocks、Garnet、KeyDB、Redict、Pika、Tendis |
| `sentinel` | 一个 NARIX listener 跟随 Sentinel 已提交的当前主节点 | Redis、Valkey、Redict |
| `cluster` | 一个 NARIX listener 接入完整标准槽位 Cluster | Redis、Valkey、Kvrocks、Garnet、KeyDB、Redict、Tendis |
| `cluster-node-proxy` | 每个真实 Cluster 节点对应一个 NARIX listener | Redis、Valkey、Kvrocks、Garnet、KeyDB、Redict、Tendis |
| `replication-ha` | 一个业务 listener 接入一组受 NARIX 管理的主从节点 | Redis、Valkey、Dragonfly、Redict、Kvrocks、KeyDB |

模式准入是产品级安全上限；正式支持范围以 [README 固定版本验证矩阵](../README.md#已验证的-kv) 为准，不能从配置可启动外推为所有版本和部署形态均已认证。

各模式的可复制全量配置：

- [`configuration/standalone.yml`](configuration/standalone.yml)
- [`configuration/sentinel.yml`](configuration/sentinel.yml)
- [`configuration/cluster.yml`](configuration/cluster.yml)
- [`configuration/cluster-node-proxy.yml`](configuration/cluster-node-proxy.yml)
- [`configuration/replication-ha-redis.yml`](configuration/replication-ha-redis.yml)
- [`configuration/replication-ha-valkey.yml`](configuration/replication-ha-valkey.yml)
- [`configuration/replication-ha-redict.yml`](configuration/replication-ha-redict.yml)
- [`configuration/replication-ha-keydb.yml`](configuration/replication-ha-keydb.yml)
- [`configuration/replication-ha-kvrocks.yml`](configuration/replication-ha-kvrocks.yml)
- [`configuration/replication-ha-dragonfly.yml`](configuration/replication-ha-dragonfly.yml)
- [`configuration/replication-ha-dragonfly-proxy-cluster.yml`](configuration/replication-ha-dragonfly-proxy-cluster.yml)

全部字段、默认值、覆盖规则和环境变量见 [配置参考](configuration.md)。

## 进程与入口

每个 `proxies[]` 配置项独立拥有连接限制、pipeline 池、DedicatedTunnel 池、队列、指标和 `read_only` 策略。不同代理项不共享这些运行时配额。

生产通常使用外部四层入口：

```text
业务客户端
  -> VIP / cloud LB / nginx stream
  -> NARIX 节点 A
  -> NARIX 节点 B
  -> 同一组后端 KV
```

NARIX 不内置业务 VIP。多节点部署需要 Keepalived VIP、云负载均衡、硬件负载均衡或其他 TCP 负载均衡器。

启动：

```bash
RUST_LOG=narix=info target/release/narix --config=/etc/narix/narix.yml
```

负载均衡健康检查：

```text
GET http://<admin-listen>/ready
```

`/health` 只表示进程存活；业务摘挂应以 `/ready` 为准。

## Cluster 部署

`cluster` 是单入口模式，NARIX 负责按槽路由。`nodes` 只需要提供可连接的 seed，但生产建议至少填写多个不同故障域的 seed。

`cluster-node-proxy` 保留 Cluster Client 语义。每个真实节点都必须映射到唯一 listener，并公布客户端实际可达的地址。`aliases` 应覆盖后端可能在 `CLUSTER SLOTS`、`CLUSTER NODES`、`MOVED` 和 `ASK` 中返回的主机名、容器名或内网地址。

Pika/Codis 槽接口不是标准 Redis Cluster 协议，不能用这两个模式；Pika 通过 `standalone` 透明访问其原生命令和槽接口。

## 托管复制 HA 部署

多个 NARIX 节点代理同一组 replication-ha 数据源时，自动故障转移必须串行化：

- `local-file` 只用于同主机验证。
- `redis` 使用独立高可用 Redis 的租约，协调存储不能位于被切换的数据源中。
- `proxy-cluster` 使用 NARIX 内置协调控制面；2 个静态成员固定走 2/2 协议，至少 3 个成员固定走多数派协议。
- 也可关闭 `ha.auto_failover`，由外部控制器或人工执行切换。

单实例部署（`coordinator: none`）由进程内锁串行化切主，有一条硬性红线：**同一组后端任一时刻只允许一个承担控制面的 NARIX 进程在线**。发布方式若存在新旧进程重叠窗口（蓝绿、优雅热替换），必须改配 `local-file` 协调器兜底同机重叠，否则两个进程的控制面互不感知。

红线的唯一豁免是**观察者节点**（`ha.observer: true`）：它不发送任何后端控制命令，只在看到完整收敛证据后跟随生产控制面完成的切主；主故障、证据不足和双主期间保持 `protected`。观察者可与生产控制面共管同一组后端、数量不限。`observer` 只限制控制面，独立只读查询端点还必须设置 `read_only: true`；行为细节见[运维指南](operations.md#观察者节点haobserver)。

新生产部署优先使用至少 3 个 NARIX 成员的 `proxy-cluster`。每个成员使用独立 `node_id`、协调地址、数据目录、证书和业务/管理监听端口；`cluster_id`、`bootstrap_node_id`、完整 `peers` 和 HA `group_id` 必须一致。

2 成员（pair）协议的自动故障转移要求两个协调成员都在线并都观察到主库故障：仅剩单个幸存成员（即使配置了 `pair_allow_single_survivor` 并完成对端隔离）也无法凑齐自动切主的观察票，只能人工执行 `/ha/failover` 或 `/ha/node/{node}/promote`；且对称分区下的安全性完全依赖 `pair_peer_fencing_command` 的跨节点互斥（STONITH）语义。对可用性有自动化要求的部署应直接使用 3 成员。

跨机器协调必须开启双向 TLS，并将 `peers[].node_id` 固定到各成员的叶子证书 SHA-256 指纹。不能按当前存活成员临时删除静态 `peers`。

进程级协调集群同时同步全部数据面模式的热更新配置。运维只需在一个成员修改配置；领导节点在多数派完成落盘和预检后，把 revision、摘要和规范化正文提交到协调日志。三节点允许一个成员离线时继续热更新，离线成员恢复后必须先追平已提交正文才能进入 ready。

各节点只保留以下本地引导差异：节点编号、协调监听/通告地址、数据目录、协调 TLS 路径、管理监听地址，以及业务监听或节点代理公布地址中的主机部分。代理项、业务端口、后端、认证和运行策略属于集群托管层。不要在落后节点上用旧文件执行独立提交；启动门禁会优先恢复协调日志版本。

配置事务协议要求全部协调成员运行相同的 NARIX 配置 schema 版本。滚动升级期间可以逐节点替换程序，但在全部成员完成升级前不要提交热更新；混合 schema 的成员会明确拒绝配置 RPC，不能静默按缺省字段继续提交。静态协调成员表本阶段不支持热更新，成员增删需要按维护窗口修改全部节点本地引导配置并重启；在实现联合成员集合迁移前，不能通过普通配置热更修改 `coordination.peers`。

## Fencing

默认 `ha.fencing.mode: manual` 是软隔离：无需脚本，但旧主恢复到被降级之间可能存在短暂双写窗口，只适合明确接受该风险的环境。以零脑裂为目标的生产自动故障转移必须使用 `ha.fencing.mode: external-command`。NARIX 直接执行配置的程序和独立参数，不经过 shell 字符串拼接。

外部程序负责通过云 API、虚拟机电源、网络 ACL、iptables 或其他可靠基础设施隔离旧主。程序超时、非零退出或动作令牌失效都会阻止切换。仓库不提供云厂商专用 fencing 适配器。

## 持久化责任

`ha.persistence.enabled: true` 的目标是：

- 配置只有 1 个节点时，由当前主节点承担快照；
- 配置 2 个节点时，由 1 个副本承担快照，主节点不持久化；
- 配置至少 3 个节点时，保留 1 个无持久化热备副本，其余合格副本承担快照，主节点不持久化；
- 多节点配置暂时没有合格副本时进入保护，不能把持久化临时回退到主节点。

新 owner 未验证成功前不会关闭旧 owner。该能力是快照责任编排，不是同步复制或零数据丢失保证。

Kvrocks 不使用上述动态责任：必须设置 `ha.persistence.enabled: false`，全部节点保留自身 RocksDB/WAL，NARIX 只收敛一个主节点及其直接副本。

## 上线检查

- 用 `cargo build --release --locked` 构建发布二进制。
- 配置前端认证、管理 token 和最小网络暴露面。
- 根据文件描述符、内存和后端 `maxclients` 调整连接与队列上限。
- Prometheus 逐个抓取所有 NARIX 成员，而不是抓业务 LB。
- 对 Cluster 执行槽迁移、`MOVED`、`ASK` 和节点故障演练。
- 核对 replication-ha 后端部署版本与 README 支持矩阵一致（复制语义由固定版本认证保障，无需自行验证）；随后演练部署接线：主故障端到端切换与告警、`external-command` 隔离脚本、协调集群失权保护、持久化交接的落盘位置与磁盘容量；Kvrocks 改为验证各节点自身磁盘恢复。
- 确认 `/ready`、`/status`、`/metrics` 和 Grafana 的节点维度均正确。
