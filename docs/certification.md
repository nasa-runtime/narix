# 后端控制能力与部署约束

[English](certification.en.md)

普通 RESP 接入、托管复制 HA 和动态持久化分别依赖不同的原生能力。最低 RESP 版本与模式准入见 [README](../README.md#支持的后端与版本)；满足接入版本不表示所有控制动作可用。NARIX 保留原生错误，并通过角色、复制链路和持久化点确认动作效果。

## 产品适配边界

| 后端 | 托管复制 HA 最低版本 | 动态持久化最低版本 |
|---|---|---|
| Redis | 6.2.0 及以上 | 7.0.0 及以上 |
| Valkey | 7.2.5 及以上 | 7.2.5 及以上 |
| Dragonfly | 1.9.0 及以上 | 1.15.0 及以上，须满足下述 ACK 周期条件 |
| Redict | 7.3.0 及以上 | 7.3.0 及以上 |
| KeyDB | 6.2.0 及以上，单主复制 | 6.2.0 及以上 |
| Kvrocks | 2.16.0 及以上 | 不提供，节点自管 RocksDB/WAL |

Dragonfly 的副本延迟门禁使用 `master_last_io_seconds_ago` 的秒级 I/O 年龄，并扣除一秒采样粒度；它不等同于字节 offset 差。Dragonfly 1.15.0 使用严格 `ha.persistence.require_zero_lag: true` 时，后端启动须显式设置 `--replication_acks_interval=1000`。该版本默认 3000ms ACK 周期可能让健康空闲链路的年龄达到 2，导致持久化候选被拒绝、路由保持保护；NARIX 不会为取得 Ready 自动放宽此门禁。断链、全量同步、未知复制状态仍不可作为安全候选。

| 后端 | 主从控制 | 动态持久化 |
|---|---|---|
| Dragonfly | 至少 1.9.0，要求动态 `masterauth`、`REPLICAOF` 和真实复制状态 | 动态 `snapshot_cron`、`dbfilename`；`dir` 只校验；严格推进的 `LASTSAVE` |
| Redis、Valkey、Redict | `INFO replication`、`FAILOVER TO`、`REPLICAOF` 和动态 `masteruser/masterauth` | 动态 `save/appendonly/appendfsync`；`BGSAVE SCHEDULE`；`rdb_saves` 推进及保存成功状态 |
| KeyDB | 单主复制，要求计划切换与动态复制凭据；禁止 active-replica/multi-master | 动态 RDB/AOF；服务端时间边界及严格推进的 `LASTSAVE` |
| Kvrocks | `INFO replication`、`CLIENT PAUSE ... WRITE`、`CLIENT UNPAUSE` 和 `SLAVEOF` | 节点自管 RocksDB/WAL，必须 `ha.persistence.enabled: false` |

Garnet、Pika、Tendis 不准入 `replication-ha`。标准 Cluster 的复制和故障转移由原生集群负责，NARIX 的 Cluster 数据面不提供托管复制控制。

缺少 `FAILOVER TO` 的旧版本不能使用依赖该命令的计划切换；缺少 `rdb_saves` 或动态快照配置的版本不能使用依赖这些证据的持久化编排。能力缺失、权限不足、占位响应和无法复验的结果均不是动作成功。失败时按路由及 `fail_closed` 策略保留保护，不发布未经确认的新拓扑。

## 角色与控制权威

Redis-family 使用角色、上游地址、链路、全量同步状态、延迟和 offset 判断候选资格。主节点公布的副本地址须与 `replication_addr` 相容；`client_addr` 用于代理访问，`replication_addr` 用于后端复制。控制账户须具有相应探测、复制和配置权限，业务前端认证独立配置。

候选不能处于全量同步、未知延迟或错误上游状态。`ha.max_replica_lag: 0` 仅取消已知延迟的数值上限。持久化 owner 必须保持健康复制链路，不能以断链副本替代可验证的责任节点。

同一组后端只允许一个有效写控制面。进程重叠、多主机和多实例部署须使用与范围匹配的协调方式。双成员要求 2/2，三个及以上成员使用多数派，不能根据当前存活节点缩减静态成员表。

故障确认、未确认新主或多主证据会先关闭路由。每条外部写入和验证循环复验租约或动作令牌，失权后停止后续动作。控制结果先提交，再发布本地安全路由；目标缺席时不能私自替换令牌绑定的候选。

`manual` fencing 不提供物理隔离证据。可靠隔离应使用 `external-command`，由基础设施执行器保证隔离效果并拒绝过期令牌。直接子进程被终止不等于其派生的远端操作已撤销。隔离失败、超时或失权时不得提升候选。

`observer` 仅跟随安全拓扑，不执行后端控制写，也不加入写控制面的协调集群；业务只读仍须设置 `read_only: true`。风险记账及恢复条件见 [运维指南](operations.md)。

## 持久化交接

先启用新 owner 并验证 checkpoint，再停用全部旧 owner，之后提交协调结果并发布拓扑。旧 owner 缺席不能等同于停用成功；外部改写的持久化配置也会重新读取并收敛。

`proxy-cluster` 配置事务开始前排空本机普通 HA 控制动作，并从候选准备到激活完成独占本机控制许可；远端已准备候选也冻结旧配置运行代的控制入口。普通收敛和管理动作不能在此期间按旧拓扑重建 owner。该许可只约束执行顺序，每条外部副作用仍需有效领导权和动作令牌；失权不会因持有本机许可而获得豁免。

Redis、Valkey、Redict 要求 `rdb_saves` 相对触发前推进、后台保存结束且结果成功。KeyDB 和 Dragonfly 使用本次调用链的 `LASTSAVE` 标记，要求服务端值严格增加，不能复用历史快照证明本次交接完成。时钟、磁盘、目录权限和备份由部署侧保障。

动态持久化不提供零数据丢失保证，异步复制仍有 RPO 窗口。仅有一个合格副本时，该节点兼任快照和接管职责；两个及以上合格副本才可分离无快照热备与快照节点。

## 版本、镜像与模块

部署应固定实际后端版本、镜像摘要和主机架构，并保持数据目录与存储格式相容。升级须保留本页所列命令、参数、字段和副作用语义；托管服务的权限、网络、TLS 或复制限制不能由代理绕过。

模块只有在后端加载实现及运行依赖后才可用。命令出现在 `COMMAND` 中不表示全部参数与功能成立；NARIX 的路由、兼容转换及明确拒绝见 [命令矩阵](../command-matrix.md)。
