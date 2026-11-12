# NARIX - NASA Adaptive RESP2/3 Infrastructure eXchange（NASA 自适应 RESP2/3 基础设施交换层）

[English](README.en.md)

NARIX 是使用 Rust 编写、以超高性能为目标的 RESP2/RESP3 前置代理与托管复制 HA 控制面，同时提供面向开发和运维的 readonly 查询入口。它支持多种 KV 后端及五种代理模式，业务客户端通过稳定地址接入，由 NARIX 处理连接、路由、认证和访问策略。

**Dragonfly HA 是 NARIX 最突出的特色模式。** 它在 Dragonfly 原生主从复制之上提供完整的 HA 控制面，持续管理故障探测、受控切主、安全路由、副本拓扑与持久化责任。业务保留普通 Redis 客户端连接方式，无需接入专用 SDK。

围绕 Dragonfly HA，NARIX 持续管理五件事：

- **安全切主**：确认主节点故障后先保护路由，再经过 fencing、复制证据和动作权威门禁提升候选。
- **稳定入口**：切主或保护时关闭旧主连接；客户端重连后进入已确认的新主，控制面无需进入每条命令的转发路径。
- **拓扑收敛**：副本直接跟随当前主节点；级联、错误上游和多主证据触发保护与受控恢复。
- **持久化分工**：多节点稳态由副本承担快照；合格副本充足时分离无快照热备与快照节点，交接先验证新责任再撤下旧责任。
- **多实例协同**：通过租约或内置协调集群串行化控制动作，并以事务方式发布配置。

Dragonfly HA 的普通读写采用一对一透明数据通道，控制探测与业务转发分离；实际吞吐和额外延迟见[性能数据](#吞吐与额外转发延迟)。**HA 不等于零数据丢失**：Dragonfly 的异步复制仍有 RPO 窗口，默认软隔离也不能保证旧主失去写能力；生产强隔离需要可靠的外部 fencing。

除 Dragonfly 外，托管复制 HA 还适配 Redis、Valkey、Redict、KeyDB 和 Kvrocks；standalone、Sentinel、Cluster 单入口与节点代理模式分别服务固定后端、动态主节点和标准槽位集群。一个进程可以运行多个相互隔离的代理监听器；每个 `proxies[]` 项独立管理连接限制、连接资源、只读策略、指标和运行状态。

[Dragonfly HA 最小配置](#replication-ha) · [生产协调集群配置](docs/configuration/replication-ha-dragonfly-proxy-cluster.yml) · [部署指南](docs/deployment.md) · [运维与恢复](docs/operations.md)

NARIX 当前提供五种数据面模式：

- `replication-ha`：托管复制模式，Dragonfly HA 是其特色使用场景；按 `backend_profile` 加载产品适配器，执行探测、保护、fencing 和协同切换，除 Kvrocks 外还可动态编排持久化责任。
- `standalone`：固定单后端透明代理。
- `sentinel`：按 Redis Sentinel 客户端协议发现并校验当前主节点，切主后更新本地连接资源。
- `cluster`：标准槽位 Cluster 的单入口代理，负责拓扑加载、槽位路由、`MOVED`/`ASK` 和跨节点聚合。
- `cluster-node-proxy`：为标准槽位 Cluster 的每个节点提供一对一代理地址，并改写集群拓扑和重定向地址。

## Dragonfly HA：NARIX 的特色模式

Dragonfly 提供数据存储和主从复制，NARIX 的 `replication-ha` 控制面负责持续判断“谁可以接流、谁可以接管、谁承担快照、谁有权修改拓扑”。业务客户端保留普通 Redis 连接方式，切换时由 NARIX 收敛路由并关闭旧代连接，客户端重连后进入当前安全主节点。

```text
        业务客户端（普通 Redis 客户端，零 SDK 改造）
                          │ 稳定接入地址
             ┌────────────▼────────────┐
             │        NARIX 数据面      │ 只路由到唯一已验证主节点
             │   保护模式 fail-closed    │ 切主后主动关闭旧代长连接
             └────────────┬────────────┘
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
          master ◄─── replica      replica
             ▲                         │
             └─────────────────────────┘
          全部副本直连当前主节点；级联漂移进入受控收敛

   NARIX HA 控制面对全部节点持续探测并执行：
   候选延迟门禁 · 故障确认后先断路 · fencing/切换 · 副本拓扑收敛
   动态持久化编排（单节点由主节点承担快照；多节点责任在副本间交接并验证）
   多实例互斥：local-file / Redis 租约 / 内置 2 节点与多数派协调集群
```

| Dragonfly HA 的运行职责 | NARIX 的处理方式 |
|---|---|
| 主节点故障与恢复 | 连续故障确认后关闭路由；验证候选复制状态、取得动作权威并完成 fencing 后切主，结果确认后开放新路由。 |
| 多个代理实例 | 同机文件租约、独立 Redis 租约或内置协调集群互斥控制动作；多机生产优先使用至少三个协调成员。 |
| 不确定拓扑与多主 | 缺少安全证据时保持保护，管理端给出原因；未确认额外主节点不能因暂时不可达而被忽略。 |
| 快照对业务的影响 | 多节点稳态把快照责任放在副本，合格副本充足时保留无快照热备；交接遵守先启用并验证新 owner、再停用旧 owner 的顺序。 |
| 级联与错误上游 | 在控制权威有效时重新分配副本，使其直连当前主节点；失败退避并保持保护。 |
| 运维变更 | 管理接口与自动切主共用安全门禁；配置先校验和准备，再提交发布，失败候选保留原配置。 |
| 业务可观测性 | 提供主从拓扑、保护原因、持久化责任、配置状态、Prometheus 指标和 Grafana 仪表盘。 |

切代和删除先退休旧控制运行代；产品适配器在外部步骤前后复验退休状态、租约或动作令牌，阻止旧句柄继续发起控制动作。已经发送到后端的命令无法撤回，客户端在断连时仍可能面对结果未知，需按业务幂等语义处理。内置协调集群记录 HA 观察、切换结果与持久化所有权，控制结果先提交，再发布安全路由。详细边界见 [运维指南](docs/operations.md) 与 [后端控制能力与部署约束](docs/certification.md)。

## Dragonfly HA 的持久化拓扑

Dragonfly HA 还要管理快照责任。启用 `ha.persistence.enabled: true`（Kvrocks 除外）后，NARIX 还会编排"**谁来承担持久化**"：单节点部署由主节点承担快照；多节点稳态把责任交给合格副本，让主节点不被快照 I/O 拖累；无法确认合格责任节点时按 `fail_closed` 策略保护路由。

推荐生产稳态是 **1 主 + 1 热备 + 至少 1 快照节点**（至少 3 个健康节点）。业务只发往当前主节点，其余节点都**直接跟随主节点**：

```text
                       业务读写
                          │
                          ▼
              ┌────────────────────────┐
              │   当前主节点 (master)    │  稳态关闭周期快照，不被快照 I/O 拖累
              └────────────┬───────────┘
                           │ 全部副本直连主节点（级联漂移进入受控收敛）
           ┌───────────────┼────────────────┐
           ▼               ▼                ▼
     热备 hot-standby   快照节点 1        快照节点 N
     关周期快照         执行周期快照       执行周期快照
     专职故障接管        承担持久化责任     承担持久化责任
```

三个角色由运行时**根据节点实际主从状态、复制延迟和稳定时间动态选择**，`nodes` 只是候选清单——不能把某个地址永久钉成主节点或热备。运行时按当前健康从节点数量在三种拓扑间自动切换：

| 健康主 | 合格从 | 拓扑（状态接口 `topology`） | 说明 |
|---|---|---|---|
| 单节点配置：1 | 0 | `single-master-snapshot` | 只有配置本来就只有一个节点时，主节点承担快照。 |
| 多节点配置：1 | 0 | `none` / `protected` | 多节点拓扑失去全部合格副本时不把持久化责任偷渡回主节点；按 `fail_closed` 保持保护并等待副本恢复。 |
| 1 | 1 | `single-replica-snapshot` | 自动取消独立热备，唯一从节点既做快照 owner 又是故障切换候选；快照 I/O 与接管职责落在同一节点，属可用性降级。 |
| 1 | ≥2 | `hot-standby-replica-snapshots` | 选 1 个稳定低延迟从节点做**无快照热备**（专职接管），其余合格从节点承担快照。这是推荐稳态。 |

节点数建议：**3 个是满足"1 主 + 1 热备 + 1 快照"的最低数量**；若要求任意 1 个节点故障后仍保留这三个角色，建议至少 **4 个节点**（1 主 + 1 热备 + 2 快照）。主节点故障后原热备优先晋升；只有 3 个节点时，在故障节点恢复或补充新节点前会暂时退化为"1 主 + 1 快照从节点"。

安全约束：启用严格持久化策略后，启动阶段会保持保护，直到首次 owner 与后端配置完成验证。交接严格按“启用新 owner → 验证 checkpoint → 停用全部旧 owner → 提交协调结果 → 发布本地拓扑”执行，任一步失败都不会把新拓扑标记为已验证；产品适配器内部每条配置写和验证轮询都会复验动作权威，多配置中途失败会在权威仍有效时恢复调用前配置。已发布拓扑也会持续与后端真实配置复核，外部 `CONFIG`/cron 漂移不会被本地缓存掩盖；`proxy-cluster` 提交的证据覆盖完整 owner 与复制链路，而不是只记录首个 owner。已知旧 owner 缺席时不能假定其已经停用，持久化无法安全收敛且 `fail_closed: true` 时进入保护模式而非静默失去持久化。**Kvrocks 是例外**——每个节点自管 RocksDB/WAL，必须 `ha.persistence.enabled: false`，NARIX 只收敛主从拓扑、不分配热备或快照角色。

## 支持的后端与版本

下表给出普通 RESP 接入的最低支持版本，支持该版本及以上具有相同协议与原生能力的版本。版本下限不表示所有新命令、参数、模块、拓扑和持久化能力都存在；命令语义以目标后端为准。`read_only` 使用 NARIX 的查询白名单，拒绝未知形态和所有修改操作，其范围见 [命令矩阵](command-matrix.md)。

| 后端 | `backend_profile` | 最低 RESP 接入版本 | 模式与原生能力边界 |
|---|---|---|---|
| Redis | `redis` | 6.0.0 及以上 | Sentinel 与标准 Cluster 可用；较新版本的 Function、模块和管理能力不会补到旧后端。 |
| Valkey | `valkey` | 7.2.5 及以上 | Sentinel、标准 Cluster；继承的 7.2.4 标签及已撤回的 8.0.8 不在支持范围。 |
| Dragonfly | `dragonfly` | 1.7.0 及以上 | `replication-ha` 的复制控制要求至少 1.9.0；不准入标准 Cluster 或 Sentinel。1.7.0 需关闭后端远端版本检查，并使用该版本的 `save_schedule` 参数。 |
| Apache Kvrocks | `kvrocks` | 2.10.1 及以上 | 标准 Cluster 和托管复制；各节点自管 RocksDB/WAL，禁止 NARIX 动态持久化编排。 |
| Microsoft Garnet | `garnet` | 2.1.0 及以上 | 标准 Cluster；不准入托管复制 HA 或 Sentinel。Range/Vector 还依赖后端相应模块及运行依赖，不能仅凭命令名存在认定可用。 |
| KeyDB | `keydb` | 5.0.0 及以上 | 标准 Cluster 最低为 5.1.0；原生 Cluster TLS 要求至少 6.2.0 且后端构建启用 TLS。托管复制只允许单主拓扑，禁止 active-replica/multi-master。 |
| Redict | `redict` | 7.3.0 及以上 | Sentinel、标准 Cluster；托管复制与动态持久化仍须具备下述控制能力。 |
| Pika | `pika` | 4.0.4 及以上 | `standalone` 透明接入；Pika/Codis 槽协议不等于标准 Redis Cluster；使用 RESP2。 |
| Tendis | `tendis` | 2.0.6 及以上 | 标准 Cluster 与节点代理；使用 RESP2，不准入 Sentinel 或托管复制 HA。RocksDB 构建变体须与后端数据目录格式相容。 |

托管复制与动态持久化采用独立下限：

| 后端 | 托管复制 HA 最低版本 | 动态持久化最低版本 |
|---|---|---|
| Redis | 6.2.0 及以上 | 7.0.0 及以上 |
| Valkey | 7.2.5 及以上 | 7.2.5 及以上 |
| Dragonfly | 1.9.0 及以上 | 1.15.0 及以上，须满足下述 ACK 周期条件 |
| Redict | 7.3.0 及以上 | 7.3.0 及以上 |
| KeyDB | 6.2.0 及以上，单主复制 | 6.2.0 及以上 |
| Kvrocks | 2.16.0 及以上 | 不提供，节点自管 RocksDB/WAL |

Dragonfly 的副本延迟门禁使用 `master_last_io_seconds_ago` 的秒级 I/O 年龄，并扣除一秒采样粒度；它不等同于字节 offset 差。Dragonfly 1.15.0 使用严格 `ha.persistence.require_zero_lag: true` 时，后端启动须显式设置 `--replication_acks_interval=1000`。该版本默认 3000ms ACK 周期可能让健康空闲链路的年龄达到 2，导致持久化候选被拒绝、路由保持保护；NARIX 不会为取得 Ready 自动放宽此门禁。断链、全量同步、未知复制状态仍不可作为安全候选。

HA 和动态持久化有独立的原生能力要求：Redis-family 的计划切换需要 `FAILOVER TO`，副本分配需要动态复制凭据及 `REPLICAOF`；Redis、Valkey、Redict 的 checkpoint 需要 `INFO persistence` 中的 `rdb_saves`，KeyDB 使用严格推进的 `LASTSAVE`。Dragonfly 动态持久化需要可读写的 `snapshot_cron`、`dbfilename` 以及可验证的 `LASTSAVE`。普通 RESP 版本下限不替代这些要求；能力缺失、权限不足或结果无法确认时，相应动作失败并保留保护。完整合同见 [后端控制能力与部署约束](docs/certification.md)。

原生缺席、占位或错误语义不会由透明代理变成完整功能。例如 Dragonfly 的 `LATENCY` 固定空返回不表示具有延迟诊断；旧 Tendis 的部分管理入口只有占位或无法调用；Garnet 2.1.0 至 2.2.4 的原生 `FAILOVER` 存在双端变为副本的边界，该路径不属于可用切换合同。Garnet 的向量计数与删除后状态也受后端版本限制。具体命令路由和兼容转换见 [命令矩阵](command-matrix.md)。

配置层的模式准入如下：

| 模式 | 可用 `backend_profile` |
|---|---|
| `standalone` | `redis`、`valkey`、`dragonfly`、`kvrocks`、`garnet`、`keydb`、`redict`、`pika`、`tendis` |
| `sentinel` | `redis`、`valkey`、`redict` |
| `cluster` | `redis`、`valkey`、`kvrocks`、`garnet`、`keydb`、`redict`、`tendis` |
| `cluster-node-proxy` | `redis`、`valkey`、`kvrocks`、`garnet`、`keydb`、`redict`、`tendis` |
| `replication-ha` | `redis`、`valkey`、`dragonfly`、`redict`、`kvrocks`、`keydb` |

注意：

- Pika/Codis 槽接口不是标准 Redis Cluster 协议，因此 Pika 使用 `standalone` 透明接入，不能配置为 `cluster` 或 `cluster-node-proxy`。
- `sentinel` 只消费 Sentinel 已提交的主节点拓扑，不发起选主、`REPLICAOF`、fencing 或 Sentinel 配置修改。
- 六个托管复制产品统一使用 `mode: replication-ha`，由 `backend_profile` 选择适配器，不存在产品专属模式入口。
- Kvrocks 的 replication-ha 只管理主从拓扑，必须设置 `ha.persistence.enabled: false`；Redis、Valkey、Redict、KeyDB 和 Dragonfly 可使用产品原生动态持久化。
- 云托管 RESP 服务的 TLS、ACL、网络和故障转移须满足服务商接口及部署约束，不能仅凭 RESP 接入兼容推定可用。
- 命令分类、路由策略和模式限制见 [`command-matrix.md`](command-matrix.md)；版本下限、模式准入和原生能力约束以本节及控制能力合同为准。

## 吞吐与额外转发延迟

以下是指定负载下的**最佳实测**，不是典型值或容量保证。吞吐取完整配对中 NARIX 的最高 QPS；延迟取 NARIX p99 最低的完整配对，同一行的各个分位都来自同一次配对。每种负载至少包含三次有效重复，异常样本按直连、代理和参考路径整组排除。

环境为 Apple M1 Pro（10 核、16 GiB），Docker Linux arm64 虚拟机配置 10 vCPU、约 7.75 GiB 内存。后端、代理、客户端分别固定到互不重叠的两颗 vCPU；后端内存上限 768 MiB，代理和客户端各 512 MiB，NARIX 使用两个 Tokio worker。程序使用 Rust 1.95 的 release 构建，`lto=thin`、`codegen-units=1`。采用同一 Docker 网络内的 TCP、RESP2、持久连接，无 TLS；每次预热 15 秒、测量 60 秒。HA 为一主一从的稳态路由，探测周期 2 秒，自动切换和持久化编排关闭；Dragonfly 后端也关闭快照。

构建列是构建源快照 SHA-256 的前 12 位。当前业务实现的测量构建为 `f13cdca88499`；其他行仅描述各自标注的构建，数据不跨构建合并。吞吐表均为热点 GET、64 字节值，直连列与 NARIX 列来自同一次配对。

| 后端 | 模式 | 连接数 / pipeline | NARIX QPS | 直连 QPS | 构建 |
|---|---|---:|---:|---:|---|
| Redis 6.0.0 | `standalone` | 32 / 16 | 1,425,690 | 1,485,763 | `f13cdca88499` |
| Redis 6.2.0 | `replication-ha` | 32 / 16 | 1,576,003 | 1,513,763 | `b869f93c7fd7` |
| Redis 8.10.3 | `replication-ha` | 32 / 16 | 1,565,762 | 1,516,921 | `b869f93c7fd7` |
| Dragonfly 2.0.2 | `standalone` | 32 / 16 | 1,236,086 | 1,449,221 | `83135bd29311` |
| Dragonfly 2.0.2 | `replication-ha` | 32 / 16 | 1,418,180 | 1,532,165 | `b68c94ee83d3` |
| Dragonfly 2.0.2 | `replication-ha` | 128 / 64 | 1,960,180 | 1,938,193 | `23223f27435c` |

容量负载下各路径达到的速率不同；批量处理与调度也可能使代理路径的容量实测高于直连。高吞吐不代表低尾延迟：上表 128 连接、pipeline 64 的 Dragonfly HA 配对中，NARIX 的绝对 p99 约为 20.19 ms。因此额外延迟使用下面的固定请求速率配对单独描述。

**额外延迟 = 经 NARIX 的命令完成延迟分位数 − 直连的同一分位数**，单位为 **µs**。例如直连 150µs、经代理 160µs，差值就是 10µs。下表均为 Dragonfly 2.0.2、32 连接、pipeline 16、64 字节热点 GET，设定速率为每秒 1,000,000 条命令。

| 模式 | 构建 | p50 差值 | p95 差值 | p99 差值 | p99.5 差值 | p99.9 差值 |
|---|---|---:|---:|---:|---:|---:|
| `replication-ha` | `23223f27435c` | +35.840 | −2.048 | −20.480 | −32.768 | −126.976 |
| `standalone` | `83135bd29311` | +79.872 | +100.352 | +139.264 | +151.552 | +200.704 |
| `standalone` | `f13cdca88499` | +82.944 | +145.408 | +180.224 | +188.416 | +294.912 |

延迟从客户端发出该命令时计到收到对应完整响应，包含网络、后端执行和排队。它不含客户端尚未发出命令时的等待；若从计划发送时刻计时，上述三行 NARIX 的 p99 分别约为 1.71 ms、39.32 ms、28.18 ms。60 秒窗口内，NARIX 分别完成 59,999,568、59,999,472、59,999,440 条命令，仍有 432、528、560 条计划命令未发出，不能称为全部计划需求均已完成。

这些分位差不是逐命令代理内部耗时的分布，负差值也不表示负的内部耗时。直方图桶相对宽度不超过 0.78125%，表中采用桶上界计算差值；pipeline 不用于除算单命令延迟。实际开销随模式、连接数、值大小、负载率和宿主调度变化，不能从最佳 GET 数据推定混合读写、切换期间或所有尾部分位的性能。

## 开发与运维 readonly 查询入口

使用独立的代理项并设置 `read_only: true`，可为开发排查和运维查询提供受限入口。允许的读命令按后端及参数形态分类；写命令、脚本执行、管理修改、复制控制和未知子命令在发往后端前拒绝。同一解析批次只要包含一条不允许的命令，整批返回 `READONLY`，不会先执行其中的读命令；跨批次不提供事务回滚。

readonly 不会授予额外后端权限，也不隐藏查询可见的敏感数据；仍应配置前端认证、后端最小权限和网络隔离。需要旁路跟随现有 HA 控制面时，可同时使用 `ha.observer: true`：observer 不执行后端控制写，业务命令是否只读仍由 `read_only` 决定。

## 构建与启动

需要 Rust 1.94 或更高版本。全部第三方依赖来自 crates.io，使用仓库中的 `Cargo.lock` 固定版本与校验和，不需要本地框架源码。NARIX 以二进制、源码或容器分发，保留 `publish = false`：

```bash
cargo build --release --locked
```

使用外部配置启动：

```bash
target/release/narix --config=/etc/narix/narix.yml
```

也可以通过环境变量指定：

```bash
APP_CONFIG_FILE=/etc/narix/narix.yml target/release/narix
```

外部文件选择优先级为 `--config` → `APP_CONFIG_FILE` → 兼容变量 `NARIX_CONFIG`，未设置或空值继续选择下一项。指定文件不可读或校验失败时拒绝加载，不回退到低优先级文件。进程会在打开业务监听前完成配置解析、强类型校验、代理名称去重和监听地址冲突检查；失败时直接退出。

### Docker 本地构建与启动

准备 `deploy/narix/config/narix.yml`，将业务监听设为 `0.0.0.0:6379`、管理监听设为 `0.0.0.0:18080`，并填写真实后端地址和凭据。创建 `deploy/narix/data`、`deploy/narix/logs`，确保镜像用户 `10001:10001` 可读配置、可写数据和日志目录。

```sh
docker build -f docker/Dockerfile -t narix:1.0.0 .
docker run -d --name narix \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/narix/config:/etc/narix:ro" \
  -v "$PWD/deploy/narix/data:/var/lib/narix" \
  -v "$PWD/deploy/narix/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  narix:1.0.0
```

镜像默认 `APP_CONFIG_FILE=/etc/narix/narix.yml`。配置目录只读，数据与日志保留在宿主机，`/etc/localtime` 只读挂载使容器使用宿主时区；该文件须在 Docker 宿主上存在且可挂载。协调数据路径放在 `/var/lib/narix` 下；每个协调成员使用独立目录。示例发布业务端口，管理端口仅限宿主回环访问。后端在另一个容器时加入双方共用的 Docker 网络。

各代理模式、HA 协调、只读入口和多监听器的完整 `docker run` 示例见 [Docker Hub README](docker/README.md)；挂载、热更和镜像分发说明见[容器部署](docs/container-deployment.md)。

## 最小可运行配置

以下配置只保留启动和基本运维所需字段。示例密码和 token 必须在部署前替换；后端需要 ACL 或密码时再增加 `username`、`password`。

### replication-ha

托管复制 HA 的唯一模式入口，架构与对比见上文[Dragonfly HA 架构](#dragonfly-hanarix-的特色模式)。配置层面的要点：

- `backend_profile` 选择产品适配器，当前放行六个后端产品；产品切换语义差异（Redis 系 `FAILOVER TO`、Dragonfly 先升后降、Kvrocks `CLIENT PAUSE`+`SLAVEOF`）由适配器吸收，管理接口统一为 `/ha/*`。
- `ha` 块是后端无关的控制面配置：探测周期与独立探测超时、故障判定阈值、复制延迟门禁、`auto_failover` 开关、fencing 模式、协调器（`none`/`local-file`/`redis`/`proxy-cluster`）。`coordinator.mode` **省略时自动解析**：组了内置协调集群（`coordination.enabled=true`）自动为 `proxy-cluster`（集群里只有 leader 切主，其余节点只观察）；单实例自动为 `none`（进程内串行）。组集群时显式配非 `proxy-cluster` 会被启动校验拒绝，从源头堵死多实例各自切主的双主风险。
- 产品持久化参数放在 `ha.persistence.<product>` 子块；Kvrocks 必须 `ha.persistence.enabled: false`。
- **自动故障转移默认开启**（`ha.auto_failover: true`）：主节点达到连续失败阈值后先关闭路由，再经过协调、fencing 和候选门禁恢复服务。默认 `fencing.mode: manual` 是无需脚本的软隔离，允许旧主恢复到被降级之间存在短暂双写风险；生产零脑裂目标必须使用经真实基础设施演练的 `external-command`。
- **人工拓扑动作不是绕过安全门禁的后门**：`failover`/`switchover`/`promote`/`replicaof`/`fence` 与自动切主使用同一协调权威；人工 `promote` 也必须先 fencing 旧主。产品适配器在每条角色/复制写命令及等待确认期间持续复验租约或动作令牌，切主结果成功提交后才发布本地新主路由。
- **观察者模式**（`ha.observer: true`）：控制面只探测和跟随，绝不发送后端控制命令，手动拓扑接口一并拒绝；切路由需要证据：旧主降级证词立即跟随，旧主永久死亡时按“确认宕机 + 接管者连续稳定”的受限通道自动恢复；证据不足与双主期间进入保护。`observer` 不限制业务命令，旁路只读端点必须同时配置 `read_only: true`。
- 运维全套接口：`GET /ha/topology|master|replicas`，`POST /ha/failover|switchover`，`POST /ha/node/{node}/promote|replicaof/{master}|fence`。

这是以 Dragonfly 为例的单实例自动切换最小配置，适合本地验证或能接受软隔离风险的单控制面部署。`nodes` 中每项格式为 `客户端地址@复制地址`；接入其他支持的产品时替换 `backend_profile` 和节点地址，并使用下方对应全量 YAML 的产品持久化块：

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启用健康、HA 状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端监听地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 令牌，部署前必须替换。

proxies: # 托管复制 HA 业务代理项列表。
  - name: replication-ha-dragonfly # 代理项唯一名称。
    backend_profile: dragonfly # 选择 Dragonfly 的 replication-ha 适配器。
    mode: replication-ha # 托管复制 HA 的唯一模式入口。
    listen_host: 0.0.0.0 # 业务 listener 绑定地址。
    listen: 7001 # 客户端连接该托管复制代理的端口。
    nodes: 198.51.100.11:6379@198.51.100.11:6379,198.51.100.12:6379@198.51.100.12:6379 # 节点清单，每项为客户端地址@复制地址，使用文档专用地址。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
    ha: # 后端无关的 HA 控制面配置。
      auto_failover: true # 默认即 true：确认主故障后先保护路由，再自动选择并提升候选。
      fencing: # 旧主隔离策略。
        mode: manual # 默认软隔离：切主后靠 REPLICAOF 把旧主降级为副本；零脚本，但存在短暂双写风险。
      coordinator: # 单实例用默认 none（进程内串行化）；多实例部署必须改 proxy-cluster/redis 防双主。
        mode: none
```
> 上面几乎全是默认值——`auto_failover`、`fencing.mode`、`coordinator.mode` 都可省略。默认值解决的是单进程动作串行和自动恢复，不承诺零脑裂。生产强隔离把 `fencing.mode` 改为 `external-command` 并配置隔离程序；跨机器多实例改用 `proxy-cluster` 或独立 Redis 协调器，完整模板见下方。

完整单 NARIX 实例配置：[`docs/configuration/replication-ha-dragonfly.yml`](docs/configuration/replication-ha-dragonfly.yml)

内置协调集群生产模板：[`docs/configuration/replication-ha-dragonfly-proxy-cluster.yml`](docs/configuration/replication-ha-dragonfly-proxy-cluster.yml)

其他产品完整配置：

- [Redis 8.2.2](docs/configuration/replication-ha-redis.yml)
- [Valkey 9.1.1](docs/configuration/replication-ha-valkey.yml)
- [Redict 7.3.6](docs/configuration/replication-ha-redict.yml)
- [KeyDB 6.3.4](docs/configuration/replication-ha-keydb.yml)
- [Kvrocks 2.16.0](docs/configuration/replication-ha-kvrocks.yml)

生产自动切换必须同时配置可靠的 `external-command` fencing 和与部署范围匹配的互斥机制：单实例可使用进程内锁，但必须保证没有进程重叠；跨进程/跨机器部署使用 `proxy-cluster` 或独立 Redis。跨机器新部署优先使用至少三个成员的内置多数派协调；`local-file` 只协调同机进程。

### standalone

固定单后端的透明代理。业务侧看到的仍是一个普通 RESP 端点，NARIX 在中间补齐直连拿不到的治理能力：

- 全命令类别透明透传，前端 pipeline 严格保序；前端是 pipeline 时到后端同样保持 pipeline。
- Pub/Sub、阻塞命令、事务和连接绑定命令自动进入 DedicatedTunnel，独享连接并受活跃上限、排队容量与获取超时约束，慢消费者不会拖垮普通请求的连接池。
- `auth_mode: terminate` 前端认证由 NARIX 终止、后端独立认证，业务侧不再持有后端密码；`backend-inject` 兼容旧内网免认证接入。
- `read_only: true` 时写命令与全部脚本执行入口按失败关闭原则拒绝，只读查询保持可用。
- 每个代理项独立的连接上限、队列、帧/批大小与超时预算，以及独立指标；NARIX 到后端支持受 CA/SNI 校验的 RESP TLS 与可选 mTLS。
- 九个后端产品全部准入（见上方模式准入表）；Pika/Codis 等非标准槽位产品也经由该模式透明接入。

下面以 Redis 为例。接入其他支持的 KV 时，替换 `backend_profile`、`host` 和 `port`：

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启用健康、状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端监听地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 令牌，部署前必须替换。

proxies: # 业务代理项列表。
  - name: redis-primary # 代理项唯一名称。
    backend_profile: redis # 后端产品能力档案。
    mode: standalone # 固定单后端模式。
    listen_host: 0.0.0.0 # 业务 listener 绑定地址。
    listen: 7001 # 客户端连接 NARIX 的端口。
    host: 127.0.0.1 # 固定后端主机。
    port: 6379 # 固定后端端口。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
```

完整配置：[`docs/configuration/standalone.yml`](docs/configuration/standalone.yml)

### sentinel

面向已有 Redis Sentinel 体系的动态单主代理。业务客户端不需要实现 Sentinel 协议，只连 NARIX 一个稳定地址：

- 按 Sentinel 客户端协议查询已提交的当前主节点，并校验候选节点角色确实为 master 后才路由，不盲信 Sentinel 应答。
- 订阅 `+switch-master` 事件即时感知切主，另有周期刷新兜底；切主后更新本地连接资源并执行安全路由，无法确认安全主节点时拒绝新连接而不是乱路由。
- 定位是 Sentinel 拓扑的消费者：不发起选主、不执行 `REPLICAOF`、不做 fencing、不修改 Sentinel 配置——需要 NARIX 亲自管理主从时应使用 `replication-ha`。
- Sentinel 控制连接与业务数据连接的认证、TLS 完全独立配置，互不复用。
- 当前准入 `redis`、`valkey`、`redict`。

业务客户端只连接 NARIX；NARIX 查询 Sentinel、校验候选节点为 master，再把业务请求发送到当前主节点：

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启用健康、状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端监听地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 令牌，部署前必须替换。

proxies: # 业务代理项列表。
  - name: redis-sentinel # 代理项唯一名称。
    backend_profile: redis # Sentinel 模式当前准入 redis、valkey、redict。
    mode: sentinel # Sentinel 动态单主模式。
    listen_host: 0.0.0.0 # 业务 listener 绑定地址。
    listen: 7001 # 客户端连接 NARIX 的端口。
    sentinel_master_name: mymaster # Sentinel 监控的服务名称。
    sentinel_nodes: 192.0.2.11:26379,192.0.2.12:26379,192.0.2.13:26379 # Sentinel 控制节点。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 分别处理前端、Sentinel 和数据节点认证。
```

完整配置：[`docs/configuration/sentinel.yml`](docs/configuration/sentinel.yml)

### cluster

标准槽位 Cluster 的单入口代理：业务客户端把 NARIX 当普通单机 Redis 使用，不需要 Cluster 客户端能力。

- NARIX 负责拓扑加载与周期刷新、按 key 槽位路由、吸收 `MOVED`/`ASK` 重定向；已登记的跨槽聚合命令按槽拆分、并发执行并聚合结果；`MSET`、脚本等要求同槽的命令仍拒绝跨槽请求，pipeline 响应严格保序。
- 适合让不支持 Cluster 协议的旧客户端、框架或语言栈接入集群，由代理处理槽位路由与重定向；故障、超时和不可执行的命令仍会返回给客户端。
- 共享后端通道固定使用 RESP2；`HELLO 3` 明确返回 `NOPROTO`，不虚假宣称 RESP3。会话状态类命令（如 `RESET`）在单入口语义下无法安全映射到具体节点时明确拒绝。
- 配置热更新预检会刷新拓扑并校验 16384 个槽位完整且无冲突后才发布。
- 当前准入 `redis`、`valkey`、`kvrocks`、`garnet`、`keydb`、`redict`、`tendis`。

`nodes` 是标准槽位 Cluster 的 seed 列表：

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启用健康、状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端监听地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 令牌，部署前必须替换。

proxies: # 业务代理项列表。
  - name: redis-cluster # 代理项唯一名称。
    backend_profile: redis # 标准槽位后端能力档案。
    mode: cluster # Cluster 单入口模式。
    listen_host: 0.0.0.0 # 业务 listener 绑定地址。
    listen: 7001 # Cluster Client 连接 NARIX 的端口。
    nodes: 192.0.2.11:6379,192.0.2.12:6379,192.0.2.13:6379 # 逗号分隔的 Cluster seed，使用文档专用地址。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接 NARIX 使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
```

完整配置：[`docs/configuration/cluster.yml`](docs/configuration/cluster.yml)

### cluster-node-proxy

保留 Cluster Client 语义的节点级代理：业务继续使用标准 Cluster 客户端，但每个真实节点都经由一个 NARIX listener 暴露。

- 改写 `CLUSTER SLOTS`、`CLUSTER NODES`、`MOVED` 和 `ASK` 中的节点地址为公布地址，部署侧须确保这些公布地址从客户端网络可达。
- `aliases` 用于归一化后端可能返回的主机名、容器名或内网地址，防止重定向泄漏业务不可达的原始地址。
- 适合客户端位于 NAT/防火墙之外、或需要为每个集群节点独立做连接治理与指标的场景；terminate 模式下同样由 NARIX 终止前端认证。
- 槽位迁移、`MOVED`/`ASK` 与节点故障语义与直连集群一致，由客户端按 Cluster 协议处理。
- 当前准入 `redis`、`valkey`、`kvrocks`、`garnet`、`keydb`、`redict`、`tendis`。

每个真实 Cluster 节点必须有唯一的代理监听端口；`advertise_host`/`advertise_port` 是返回给 Cluster Client 的可达地址：

```yaml
admin: # 管理端 HTTP 服务配置。
  enabled: true # 启用健康、状态和指标接口。
  listen: 127.0.0.1:18080 # 管理端监听地址。
  token: "replace-with-a-long-random-admin-token" # 管理 API 令牌，部署前必须替换。

proxies: # 业务代理项列表。
  - name: redis-cluster-nodes # 代理项唯一名称。
    backend_profile: redis # 标准槽位后端能力档案。
    mode: cluster-node-proxy # Cluster 节点一对一代理模式。
    listen_host: 0.0.0.0 # 子 listener 默认绑定地址。
    cluster_node_advertise_host: narix.example.internal # 返回给 Cluster Client 的统一可达主机。
    frontend_password: "replace-with-a-long-random-client-password" # 客户端连接节点代理使用的密码。
    auth_mode: terminate # NARIX 终止前端认证并独立认证后端。
    node_proxies: # 真实节点与代理 listener 的映射表。
      - backend: 192.0.2.11:6379 # 第一个真实 Cluster 节点，示例使用文档专用地址。
        listen: 17001 # 第一个节点代理监听端口。
        advertise_port: 17001 # 返回给 Cluster Client 的第一个代理端口。
      - backend: 192.0.2.12:6379 # 第二个真实 Cluster 节点。
        listen: 17002 # 第二个节点代理监听端口。
        advertise_port: 17002 # 返回给 Cluster Client 的第二个代理端口。
      - backend: 192.0.2.13:6379 # 第三个真实 Cluster 节点。
        listen: 17003 # 第三个节点代理监听端口。
        advertise_port: 17003 # 返回给 Cluster Client 的第三个代理端口。
```

完整配置：[`docs/configuration/cluster-node-proxy.yml`](docs/configuration/cluster-node-proxy.yml)

## 运维检查

健康和就绪端点不要求 token：

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18080/ready
```

状态与指标接口需要管理 token：

```bash
curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  http://127.0.0.1:18080/status

curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  http://127.0.0.1:18080/metrics
```

负载均衡健康检查应使用 `/ready`，不要使用只表示进程存活的 `/health`。管理端还接受 `X-Admin-Token: <token>`。

业务连接检查：

```bash
redis-cli -h 127.0.0.1 -p 7001 \
  -a replace-with-a-long-random-client-password PING
```

## 配置入口

配置加载优先级从低到高：

```text
zcf/application.yml
同目录 application-${NARIX_PROFILE}.yml
--config、APP_CONFIG_FILE 或 NARIX_CONFIG 选定的外部文件
外部文件同目录的 ${name}-${NARIX_PROFILE}.yml
NARIX_ 字段环境变量
${...} 配置占位符解析
```

支持 YAML、JSON 和 TOML。文件显式写出 `proxies[]` 或 `node_proxies[]` 时，该数组表示完整期望集合：同 `name` 或 `backend` 的条目继承低优先级文件中未覆盖的字段，未出现在高优先级数组中的条目会被删除；省略整个数组才表示全部继承。环境变量仍按身份或下标定向覆盖，不删除其他数组项。

默认开启配置热更新，`reload.poll_interval_ms` 默认 `1000`（1 秒），设置 `10000` 即 10 秒。修改外部配置后，NARIX 会在连续两次读取到相同有效摘要时尝试发布；也可以显式校验或发布。涉及 HA 拓扑、持久化责任或未确认多主证据时，发布仍须通过旧运行代的安全预提交，失败会保留原配置和原运行代：

```bash
curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  -X POST http://127.0.0.1:18080/config/validate

curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  -X POST http://127.0.0.1:18080/config/reload
```

单节点部署直接切换代理运行代。启用进程级协调集群后，只需在任意一个 NARIX 成员修改并提交一次；请求会转交领导节点，以多数派预检和共识提交同步到其他成员。落后成员在追平已提交 revision 前不开放数据面，本地旧文件不能覆盖集群新版本。

更多配置：

- [完整配置说明、认证、覆盖规则与环境变量](docs/configuration.md)
- [standalone 全量 YAML](docs/configuration/standalone.yml)
- [sentinel 全量 YAML](docs/configuration/sentinel.yml)
- [cluster 全量 YAML](docs/configuration/cluster.yml)
- [cluster-node-proxy 全量 YAML](docs/configuration/cluster-node-proxy.yml)
- [replication-ha + Redis 全量 YAML](docs/configuration/replication-ha-redis.yml)
- [replication-ha + Valkey 全量 YAML](docs/configuration/replication-ha-valkey.yml)
- [replication-ha + Redict 全量 YAML](docs/configuration/replication-ha-redict.yml)
- [replication-ha + KeyDB 全量 YAML](docs/configuration/replication-ha-keydb.yml)
- [replication-ha + Kvrocks 全量 YAML](docs/configuration/replication-ha-kvrocks.yml)
- [replication-ha + Dragonfly 全量 YAML](docs/configuration/replication-ha-dragonfly.yml)
- [replication-ha + Dragonfly 内置协调集群 YAML](docs/configuration/replication-ha-dragonfly-proxy-cluster.yml)
- [内置最小安全配置](zcf/application.yml)
- [配置热更新、集群一致性与故障恢复](docs/operations.md)

## 日志与配置事务

默认输出全部 `WARN`、异常 `ERROR` 以及 NARIX 启停、配置生效、控制权、角色与保护态变化等关键 `INFO`。正常请求和成功的周期探测不逐条记录。

```yaml
application:
  name: narix
log:
  level: ${NARIX_LOG_LEVEL:warn,narix=info}
  path: ${NARIX_LOG_PATH:/usr/local/logs/${application.name}}
  max_file_size: 100MB
  total_size_cap: 1GB
  max_history_days: 7
  split_error_file: true
reload:
  enabled: true
  poll_interval_ms: 1000
```

日志目录通过环境变量覆盖，例如 `NARIX_LOG_PATH='/var/log/${application.name}'`；占位符由配置加载器解析为应用名。单文件上限为 100 MB，归档总量上限为 1 GB，保留 7 天。`RUST_LOG` 优先于 `NARIX_LOG_LEVEL`，环境覆盖存在时修改同名 YAML 字段不会改变有效值。

日志级别、目录、格式与轮转策略支持事务热更新。整份候选先解析、校验和准备，任何一步失败都不替换已生效配置；运行代安装失败按事务回滚，回滚不完整时保持业务门禁关闭。成功提交后才采用新的轮询周期。配置来源、节点本地字段及不可热更字段见[配置参考](docs/configuration.md#配置热更新)，日志关闭与丢弃边界见[日志说明](docs/configuration.md#日志)。

## 指标与 Grafana

`GET /metrics` 暴露进程 CPU、RSS、线程、文件描述符、前后端连接、命令、pipeline、DedicatedTunnel、Cluster、Sentinel 和托管复制 HA 指标。仓库内置的监控文件：

| 文件 | 用途 |
|---|---|
| `dashboards/prometheus.yml` | 单个 NARIX 进程抓取模板 |
| `dashboards/prometheus-cluster.yml` | 多个 NARIX 节点逐节点抓取模板 |
| `dashboards/grafana-datasource.yml` | Grafana Prometheus 数据源 |
| `dashboards/grafana-dashboard-provider.yml` | Grafana dashboard provider |
| `dashboards/narix-overview.json` | 支持集群、节点和代理筛选的 NARIX 仪表盘 |

NARIX 集群中每个进程都独立暴露管理端，Prometheus 必须逐节点抓取，不能只抓负载均衡地址。集群内各任务的 `environment` 和 `narix_cluster` 标签保持一致，`narix_node` 必须唯一。详细部署与告警处理见 [运维指南](docs/operations.md)。

## 生产约束

- 生产业务入口建议使用 `auth_mode: terminate`，不要无认证暴露代理端口。
- 管理端默认要求 token；`allow_unauthenticated: true` 仅适用于可信隔离网络。
- 业务 RESP 入口不原生终止 TLS；跨不可信网络时应由受信四层入口、隧道或服务网格终止 TLS。
- NARIX 到业务数据节点支持受 CA/SNI 校验的 RESP TLS；Sentinel 控制连接可独立启用 TLS，两套证书和 ACL 互不复用。
- `coordination.tls` 只保护 NARIX 成员间的 gRPC 控制端口，不能替代业务入口加密。
- replication-ha 达到主故障阈值、观察到未确认新主或双主时会关闭路由；负载均衡必须按 `/ready` 摘挂。
- 生产自动故障转移必须具备可靠 fencing 和与部署范围匹配的协调门禁；不能把不可确认的旧主直接视为已隔离。
- `ha.persistence.enabled: true` 是持久化责任编排，不是零数据丢失 WAL；异步复制仍有 RPO 窗口。
- Kvrocks 必须保持 `ha.persistence.enabled: false`，各节点自己的 RocksDB/WAL、备份和磁盘可靠性由部署侧负责。
- 按文件描述符、后端 `maxclients`、业务 pipeline 和内存预算设置连接、队列与帧上限。
- cluster-node-proxy 的别名应覆盖可能出现在 `CLUSTER SLOTS`、`CLUSTER NODES`、`MOVED` 和 `ASK` 中的后端地址。

## 运行设施与关闭语义

日志由单一管理器控制过滤、文件输出和轮转；配置解析、指标编码、gRPC 传输、凭据材料和时间换算使用 NASA 设施。NARIX 的配置事务负责候选准备、提交和同代激活，Application 负责进程信号、关键任务监督及资源关闭。RESP 稳态转发直接使用当前运行代持有的句柄，不逐命令加载配置或读取证书。

运行期配置补读与候选文件读取由 NASA Application 共用组件承接，阻塞文件 I/O 在线程池执行，异步调用方通过 `.await` 让出执行线程。NARIX 保留路由、HA 权威和跨节点配置提交的领域事务。

同步配置和凭据预检完成后，日志文件准备与业务装配共享启动预算。停止时先撤销业务准入，再停止控制任务和排空连接，最后关闭日志。SIGTERM 与 reload 竞争时不得重新开放已停止的业务门禁；资源排空超时按非零退出，运行时不会在应用关闭预算之外再次无限等待阻塞文件 I/O。详细预算见 [配置参考](docs/configuration.md#进程生命周期)。

## 更多文档

- [配置参考](docs/configuration.md)
- [部署指南](docs/deployment.md)
- [后端控制能力与部署约束](docs/certification.md)
- [运维指南](docs/operations.md)
- [部署交付清单](docs/release-checklist.md)
- [命令支持矩阵](command-matrix.md)

## 许可证

本项目采用 Apache License 2.0，详见 [LICENSE](LICENSE)。
