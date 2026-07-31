# 后端逐版本认证手册

托管复制 HA 只对 README 支持矩阵中明确列出的产品与固定版本负责。任何版本升级、共享内核假设变化或新产品接入，都必须按本手册重新认证，不能只在临时进度记录里保留结论。执行环境需要 Docker 与本仓库的 release 构建。

## 1. 认证版本与镜像

| `backend_profile` | 认证版本 | 参考镜像 | 实现边界 |
|---|---:|---|---|
| `dragonfly` | 1.39.0 | `docker.dragonflydb.io/dragonflydb/dragonfly:v1.39.0` | Dragonfly 专用复制与快照适配器 |
| `redis` | 8.2.2 | `redis:8.2.2` | Redis-family 共享内核 |
| `valkey` | 9.1.1 | `valkey/valkey:9.1.1` | Valkey 包装层 + Redis-family 共享内核 |
| `redict` | 7.3.6 | `registry.redict.io/redict:7.3.6` | Redict 包装层 + Redis-family 共享内核 |
| `kvrocks` | 2.16.0 | `apache/kvrocks:2.16.0` | Kvrocks 专用复制适配器 |
| `keydb` | 6.3.4 | `eqalpha/keydb:<architecture>_v6.3.4` | KeyDB 包装层 + Redis-family 共享内核（LASTSAVE 分支） |

镜像 tag 必须与认证版本和运行主机架构完全一致，例如 arm64 使用 `eqalpha/keydb:arm64_v6.3.4`；认证通过后同步更新 README 支持矩阵与本表。

## 2. 共享内核假设复验清单（Redis-family：redis/valkey/redict/keydb）

版本升级时，以下每一条都必须在真实容器上重新确认；任何一条不成立，该版本不得复用共享内核，必须拆出独立适配器或版本分支：

1. `INFO replication` 提供 `role`、`master_host/master_port`、`master_link_status`、`master_sync_in_progress`、`master_last_io_seconds_ago`、`slave_repl_offset`（或 `replica_repl_offset`）与 `master_repl_offset`；主节点的 `slaveN:...state=online` 行含 `ip`/`port` 字段。
2. `FAILOVER TO host port TIMEOUT ms` 存在且超时自动中止；副本可通过 `replica-announce-ip/port` 登记与 `nodes` 中 `replication_addr` 一致的地址。
3. `REPLICAOF NO ONE` / `REPLICAOF host port`、`CONFIG SET masteruser/masterauth` 动态生效。
4. `CONFIG SET save/appendonly/appendfsync` 动态生效且 `CONFIG GET` 回读一致。
5. `BGSAVE SCHEDULE` 被接受；`INFO persistence` 是否提供 `rdb_saves`：
   - 提供（redis/valkey/redict 现认证版本）：走 `rdb_saves` 增长 + `rdb_bgsave_in_progress=0` + `rdb_last_bgsave_status=ok` 验证；
   - 不提供（keydb 6.3.4）：必须确认 `LASTSAVE`/`TIME` 语义可用，走 LASTSAVE 严格增长分支。
6. KeyDB 专项：不得开启 `active-replica`/`multi-master`（与单主状态机冲突，认证矩阵须包含“确认默认关闭”）。

Kvrocks 专项假设：`CONFIG SET slaveof` 不受支持；`CLIENT PAUSE <ms> WRITE`/`CLIENT UNPAUSE` 可用；`INFO replication` 提供 `master_sync_unrecoverable_error` 与 sequence 型 `slave_repl_offset`/`master_repl_offset`；`SLAVEOF` 语义与本手册第 4 节流程一致；持久化保持节点自管（不做第 4～5 条）。

## 3. 环境准备

```bash
cargo build --release --locked
docker network create narix-cert   # 结束后 docker network rm narix-cert
```

容器命名统一为 `narix-cert-<product>-<n>`；每个产品验证结束必须：

```bash
docker rm -f narix-cert-<product>-1 narix-cert-<product>-2 narix-cert-<product>-3
docker ps -a | grep narix-cert   # 必须无残留
```

三节点拓扑示例（以 valkey 为例，其余产品替换镜像与端口；密码统一 `cert-pass`）：

```bash
for i in 1 2 3; do
  docker run -d --name narix-cert-valkey-$i --network narix-cert -p 2740$i:6379 \
    valkey/valkey:9.1.1 valkey-server --requirepass cert-pass --masterauth cert-pass \
    --replica-announce-ip 127.0.0.1 --replica-announce-port 2740$i
done
# 初始复制:2、3 跟随 1(在容器内执行 REPLICAOF)
```

NARIX 配置模板取 `docs/configuration/replication-ha-redis.yml` 为底，替换 `backend_profile`、`nodes`（`127.0.0.1:2740X`）与产品持久化子块；Kvrocks 使用 `ha.persistence.enabled=false`。

## 4. 验证矩阵（每个产品必须全绿）

逐项执行并记录命令、状态快照、关键时延和异常日志；控制动作统一使用管理接口：

1. 一主两从启动：`/ha/topology` 单主、两副本 lag 已知且 serving；数据面 SET/GET。
2. 计划切换 `POST /ha/switchover`：新主生效、旧主跟随新主、第三副本直连新主（级联自动修复）；切换前写入的数据切换后可读。连续执行两次。
3. 自动故障转移（fencing=external-command，隔离脚本用 `docker pause`）：旧主隔离成功后仅提升一个副本；旧主 `docker unpause` 后自动降级并恢复复制。
4. fencing 失败保护：隔离脚本强制退出非零，必须保持 `protected` 且不提升；失败重试按租约周期退避。
5. 动态持久化（Kvrocks 跳过）：owner handoff、`force_checkpoint` 验证（注意第 2 节的 rdb_saves/LASTSAVE 分支）、非 owner 停用、fail_closed 保护；热更新缩容/扩容后持久化拓扑重新收敛。
6. 配置热更新：增删节点、删除当前主节点（先切换到保留副本）、revision 推进；删主失败场景保留旧拓扑。制造未确认额外 master 风险后，仅修改探测周期也必须在任何预提交副作用前拒绝，旧运行代及保护状态保持不变。
7. 坏副本退避：人为使一个副本拒绝 `REPLICAOF`（如错误 masterauth），确认修复进入退避（日志不随探测周期刷）且数据面保护原因可定位。
8. 禁用自动切主：`auto_failover=false` 时主故障达到阈值后必须 `protected`，`/ready` 摘除实例且不提升任何副本；原主恢复为唯一主时自动恢复，或在唯一外部接管者连续稳定达到阈值后仅切换本地路由。多主时必须保持保护，拓扑修复必须全程持有协调权威；风险成员成为新 current 时其它缺席 master 仍阻止 Serving。对永久不可达的风险成员，成功且权威复验通过的 `external-command` fence 可出清风险并恢复 Serving，`manual` fence 不可出清。observer 在额外 master 对本实例不可达后继续保护且控制写计数为零。
9. observer：未确认新主、主故障与双主必须 `protected`，已收敛外部切主可自动跟随，整个过程后端控制命令计数保持为零；另验证 `read_only=true` 才会拒绝业务写命令。
10. 人工动作权威：对 `failover`/`switchover`/`promote`/`replicaof`/`fence` 注入租约/动作令牌丢失，确认后续后端写命令数不增长、路由保持保护且不发布未提交结果。将 fencing 脚本强制置为失败，人工 `promote` 必须在提升目标前返回失败；再让脚本长时间运行并在中途撤销权威，确认直接子进程被终止且基础设施执行器按动作令牌拒绝任何已派生的过期后台任务。
11. 协调崩溃恢复：`proxy-cluster` 在 promotion 成功、`CompleteAction` 提交前杀死领导者；新运行代必须保护启动，并只在重新取得观察票和动作令牌、fencing 旧主后提交真实新主。复验时令牌目标缺席不得改提其它副本。
12. 回归本仓库测试：按 [`../source.md`](../../source.md) 使用 `127.0.0.1` 本地数据源执行 `cargo test --locked --all-targets`，不得因仍指向旧 LAN 地址而跳过真实环境场景；依赖其他固定版本容器的忽略项按本节单独执行并保存结果。

## 5. 结果登记

认证通过后：更新 README 支持矩阵和本手册第 1 节表格，在发布记录中保存关键时延、异常观察与验证证据，并注明“按 docs/certification.md 重新认证”。认证失败或仅部分成立时，禁止发布对应 profile，并在发布记录中列出不成立的假设与复现步骤。
