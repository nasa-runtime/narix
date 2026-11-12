# 运维

[English](operations.en.md)

## 管理端接口

```text
GET /health
GET /ready
GET /status
GET /metrics
GET /config/reload
POST /config/validate
POST /config/reload
GET /ha/topology
GET /ha/master
GET /ha/replicas
POST /ha/failover
POST /ha/switchover
POST /ha/node/{node}/promote
POST /ha/node/{node}/replicaof/{master}
POST /ha/node/{node}/fence
```

除 `GET /health` 和 `GET /ready` 外，管理请求需要携带：

```text
Authorization: Bearer <admin.token>
```

或者：

```text
X-Admin-Token: <admin.token>
```

除非显式配置 `admin.allow_unauthenticated: true`。

Dragonfly 动态持久化会把 `force_checkpoint` 返回的 `LASTSAVE` 令牌原样传给同一次验证，只有令牌严格前进才发布新 owner。KeyDB 同样使用调用链携带的 `LASTSAVE`，但先等待服务端秒边界，再执行 `BGSAVE SCHEDULE`。`SAVE` 引起的单轮探测抖动在 `probe_interval_ms * master_down_after_failures` 宽限内保留已验证拓扑；连续超过宽限才降级持久化责任，避免 checkpoint 自身触发 owner 往返迁移。

`POST /ha/switchover` 在不同后端上保证相同的候选门禁和最终唯一主拓扑，但中间窗口不同：Redis、Valkey、Redict 和 KeyDB 使用原生 `FAILOVER TO`，由旧主协调目标副本接管；Dragonfly 先提升候选，验证成功后再降级旧主，因此可能出现受控的短暂双主窗口；Kvrocks 先暂停旧主写入、确认 RocksDB sequence offset 追平，再通过多条 `SLAVEOF` 命令完成切换。变更期间应暂停非幂等批处理，并以状态接口确认最终主节点。

## 配置热更新

`POST /config/validate` 只校验候选配置，`POST /config/reload` 执行发布，`GET /config/reload` 返回 revision、摘要、差异、最近结果和最近一次集群提交响应时尚未收敛的成员数。文件自动检查与 `SIGHUP` 使用同一串行事务，不会和管理请求交叉提交。

单节点部署会先预检新增或修改的监听器和后端，再准备全部候选运行代。无地址冲突的候选 listener 会先完成真实 bind，但保持在本地 activation 门禁后，不接受业务连接；地址复用候选在发布窗口内等旧 listener 释放后再绑定。进入发布点后，进程级门禁短暂拒绝所有业务监听器的新连接；稳定 listener 切代会先停止并等待旧代 HA、Sentinel、Cluster 控制任务完全退出，再激活新代，防止旧配置继续改写后端；supervisor 确认候选运行时注册和 accept 任务已安装后，才重新开放全局门禁。旧 listener 释放与旧业务连接排空分开，旧连接在门禁重新开放后继续后台排空，不会把最长排空超时叠加到全局发布窗口。旧代控制任务的退出等待同样有界：收敛循环会在关闭信号处取消在途动作。HA 半程动作由下一代幂等收敛；Sentinel 取消刷新时保留最近一次完整校验的安全路由，并由新代重新发现；Cluster 取消刷新时保留最近一次完整发布的槽位拓扑，并由新代重新拉取。三类循环都不得发布半成品状态。管理接口仍持有旧运行时句柄的在途 HA 运维操作，会在旧代退休后被统一拒绝（错误信息含 `retired`）；共享退休令牌也会在产品适配器每条控制命令前后复验，禁止多命令序列继续推进。运维应对新运行代重新发起该操作。回滚不完整时门禁保持关闭，不能让新连接进入混合配置。HA 删除当前主节点时先切换到候选配置保留的健康副本；删除动态持久化 owner 时先把责任迁移到保留节点并验证持久化点；关闭动态持久化时先停用全部后端责任。预绑定阶段的新地址已完成 TCP 绑定，但不会交给业务处理，早到连接会被关闭；负载均衡健康检查应使用业务命令（如 `PING`）而不是纯 TCP 握手判活，避免把未激活的候选 listener 判定为可服务。

删除整个 HA 代理或退出 `replication-ha` 模式与显式关闭动态持久化使用同一安全前置动作：先停用全部已知后端责任。被删除节点或上一版 owner 不可达时无法证明其已经停用，配置事务会在提交前拒绝并继续运行旧配置，不能用“节点已不在候选配置中”替代停用证据。

协调集群中只修改一个成员：

1. 任意成员把候选配置通过 gRPC 转交当前领导节点。领导节点先排空本机普通 HA 控制动作，并持有从 prepare 到激活结束的独占控制许可；普通收敛和管理动作在许可不可用时拒绝进入。许可不能替代领导权、租约或动作令牌的复验。
2. 领导节点与可达成员并发落盘和预检；成员只有在候选准备完成并冻结数据面发布门禁后才确认 prepare。冻结到 activate 或 abort 期间会拒绝新建业务连接，客户端必须启用带退避的自动重连；`reload.transaction_timeout_ms` 同时约束最坏冻结窗口。
3. 达到静态成员多数派后，领导节点执行必要的 HA 安全前置动作，并让可用成员二次预检；事务期间周期守护不能重新开放旧运行代；已 prepare 的远端成员也不能按旧配置发起 HA 控制动作。
4. 二次预检达到多数派后提交 revision、摘要和规范化正文，再由已预检成员并发发布；其他成员保持非 ready，并在恢复或重启后从复制状态机追平。
5. 每个成员把已提交正文写入 `coordination.data_dir/config/`；目录权限为 `0700`，文件权限为 `0600`。提交前明确中止只会按 revision 和摘要清理本事务候选并解除同一冻结；领导节点回调丢失时，冻结最多保留 `reload.transaction_timeout_ms`，到期后清理孤儿候选并按当前共识版本重新判断。

配置事务进入 HA precommit 前以 `reload.transaction_timeout_ms` 作为总预算；一旦 fencing、切主或持久化交接已经开始，领导节点不会因 HTTP/gRPC 调用方超时或断开而取消外部副作用。它会继续执行各自带超时的控制步骤并把事务明确提交或中止，避免出现“先返回 Abort、后异步完成切主”。调用方断开时应通过 `GET /config/reload` 和集群 revision 查询最终状态。

三节点集群允许一个成员掉线时由剩余两个成员完成热更新。掉线成员使用旧本地文件重启时，管理端和数据面不会提前开放；它先从领导者确认最新提交头、等待本地状态追平、写入并发布新 revision。恢复完成后，文件检查会抑制旧源摘要，不能反向提交旧配置。

文件自动热更新遇到瞬时预检或发布失败时，会对同一摘要按 1 秒起步、最高 30 秒的指数退避继续尝试；修改文件内容会立即重置失败次数。该失败重试与集群恢复后的旧源摘要抑制是两套状态，后者在本地旧文件未变前不会自动反向提案。

如果只剩少数派、候选无法通过预检、磁盘同步失败或 HA 拓扑没有安全迁移目标，事务在共识提交前失败并继续运行旧版本。共识提交后发生的成员发布失败不能回滚 revision；失败成员保持非 ready 并由周期收敛任务重试。

周期一致性守卫的正常路径只比较 revision 和 digest，HA 周期探测只读取目标组状态，不复制完整配置正文；只有发现本地运行代落后时才读取正文并执行恢复。

HA 观察和动作申请同时携带运行时绑定的全局 revision 与组配置摘要。配置提交窗口中的旧运行代即使尚未退出，也不能给新版本投票或申请控制动作。同名 HA 代理删除至少一个 revision 后重新加入时会清除旧生命周期的主节点和持久化 owner，再由当前领导者确认新拓扑。

## 只读代理项

`read_only: false` 是默认快速路径。

`read_only: true` 会用 `READONLY` 拒绝写命令。所有 Lua/Function 执行入口（`EVAL`、`EVALSHA`、`EVAL_RO`、`EVALSHA_RO`、`FCALL` 和 `FCALL_RO`）都会被拒绝。`SCRIPT EXISTS` 等只读查询仍然可用；`SCRIPT LOAD`、`SCRIPT FLUSH`、`SCRIPT KILL`、`SCRIPT DEBUG` 以及 `FUNCTION LOAD`、`FUNCTION DELETE`、`FUNCTION FLUSH`、`FUNCTION RESTORE`、`FUNCTION KILL` 等会改变脚本或函数库状态的命令都会被拒绝。未知命令和需要透明隧道的命令也会被拒绝，因为建立透明隧道后代理无法继续可靠解析命令并实施只读门禁。

## DedicatedTunnel 限制

Pub/Sub、阻塞命令、事务和连接绑定命令使用 DedicatedTunnel。每个代理项都应分别设置活跃连接上限、排队容量和获取许可超时；字段含义、默认值及各模式全量 YAML 见 [配置参考](configuration.md)。

监控以下指标：

```text
narix_dedicated_tunnel_active
narix_dedicated_tunnel_queue_waiters
narix_dedicated_tunnel_rejections_total
narix_dedicated_tunnel_acquire_timeouts_total
```

## replication HA 保护模式

当系统检测到无法消除的多主歧义、自动切换所需的隔离失败、Redis 协调租约失效、内置协调集群失去权威、动作令牌过期，或者严格持久化策略无法安全收敛时，replication HA 会进入保护模式。保护模式下，`/ready` 返回非正常状态，新业务连接会收到 Redis 协议错误，而不会被路由到任意主节点。

在线副本未直连当前主节点（例如级联跟随另一副本）也会触发保护：运行时会在协调权威保护下把漂移副本改写为直连主节点，收敛完成并获得新一轮观测后才重新开放路由；收敛失败按 `max(lease_ttl, 10×probe_interval)` 退避，退避期间保持保护且不重复执行复制控制命令。该行为不受 `ha.auto_failover` 约束（属拓扑收敛而非故障切换）。若确需临时保留链式或外部管理的副本，应先把该节点移出 `nodes`，否则单个漂移副本会使整个代理进入保护。

常见原因：

- 存在多个主节点，且当前安全主节点无法确定；
- 自动故障切换所需的隔离失败；
- 自动切换期间协调器租约获取或续约失败；
- 双节点协调失去对端联系，或多数派协调失去法定票数；
- 已探测主节点与协调状态机提交的主节点不一致；
- 主节点故障后所有副本都无法提供可验证的复制证据；断链时长未知、全量同步中、延迟未知或无法确认原上游的副本不能晋升。断链时长已知的副本按该时长作为延迟下界参与门禁；
- fencing、晋升、拓扑收敛或持久化交接期间动作令牌失效；
- `ha.persistence.fail_closed=true` 时持久化编排无法安全收敛。

fencing 与协调器是两个正交的防双主机制：**fencing**（`ha.fencing.mode`）隔离的是旧主后端节点——`manual` 软隔离靠切主后 `REPLICAOF` 把旧主降级为副本（零脚本，存在旧主复活到降级的短暂双写窗口；集群部署下提主前会先等满一个 `route_guard_timeout`，让分区成员的旧路由先断路），`external-command` 切主前跑脚本物理隔离旧主（安全性取决于隔离程序的真实效果及过期令牌拒绝能力）；**协调器**（`ha.coordinator`）串行化多个 NARIX 实例的切主动作。单实例部署 `coordinator: none` + `auto_failover: true` 只提供进程内动作串行，同一组后端任一时刻只允许一个 NARIX 进程在线（蓝绿发布出现新旧进程重叠时必须改配 `local-file` 协调器兜底）；多实例部署同一组后端必须配 `proxy-cluster`（内置共识，零外部依赖）或 `redis` 协调器,否则两个实例会同时切主造成双主。协调器省略时按 `coordination.enabled` 自动解析：组集群自动 `proxy-cluster`（多实例只有 leader 切主），单实例自动 `none`；组集群时显式配非 `proxy-cluster` 会被启动校验拒绝,从源头消除多实例双主。

主故障确认后若没有合格候选（例如唯一副本正在全量同步），数据面进入保护态并在 `lastError` 保留原因，不会把业务流量留在已宕机的旧主上；此时应先恢复合格副本，再等待自动恢复或用 `POST /ha/node/{node}/promote` 指定目标；人工提升也不能绕过候选与 fencing 门禁。主库宕机导致的断链副本按断链时长参与候选门禁,无需人工介入。启动或热更新时若观测不到主节点，监听会以保护态启动并由故障确认机制驱动恢复，构造期不会提升任何副本。

一旦观测到多主，所有协调模式都先关闭路由。当前安全主节点仍明确存在时，进程内锁、local-file 或 Redis 协调模式取得并持续复验租约后才能逐节点降级额外主节点；`proxy-cluster` 只有领导者能在动作令牌下执行收敛，非领导节点保持保护。已经观测为额外 master 的节点会跨探测轮保留为未确认风险：即使它随后对本代理不可达、全量探测只剩当前主，也必须等该节点重新证明为 replica、在受权动作中完成降级验证，或由仍持有动作权威的 `external-command` fencing 成功证明已经物理失去写能力后才能开放路由。`manual` fencing 是软模式 no-op，不构成物理隔离证据，也不会清除风险。若其中一个风险节点后来经旧主降级证词、已提交 outcome、受限跟随或受权运维动作成为新 current，只清除该节点自身的“额外主”风险；其余缺席风险仍在所有 Serving 发布入口的统一门禁阻止开放。observer 执行同样的纯本地风险记账，但仍然不发送任何后端控制写。降级失败、租约/令牌中途失效或结果无法提交时都不得重新开放，并按 `max(lease_ttl, 10×probe_interval)` 退避，避免每轮重复发送 fencing 或复制控制命令。关闭自动故障切换后，当前主节点达到连续失败阈值也会进入保护，`/ready` 摘除实例并拒绝新业务连接；原主恢复且重新成为唯一主节点后自动恢复服务。

未确认额外 master 集合属于运行代内的观测证据，不跨进程重启或候选运行代持久化。配置发布预提交会拒绝删除或替换持有该证据的 HA 运行代，因此普通热更新不能静默清空风险；`POST /config/validate` 仍然只做无状态候选校验，成功不表示该候选允许发布。状态接口显示此类保护原因时，也不得通过进程重启或滚动发布把路由重新投入流量。安全解锁方式是让每个风险节点重新证明为目标主的 replica；对永久不可达节点，只有配置为 `external-command` 时才可调用 `POST /ha/node/{node}/fence`，并以命令成功及动作权威复验通过作为物理隔离证据。`manual` 模式必须先由独立控制面确认所有已报告 master 已隔离或降级，再按受控维护流程重启。重启不会保留该运行代的风险集合，因此重启前必须完成上述隔离或降级确认。

人工拓扑接口与自动切主共用同一权威链：进程内锁、local-file 或 Redis 协调模式在 fencing、提主、复制改写与拓扑收敛前后复验租约，`proxy-cluster` 在同一位置复验动作令牌。Redis 系、Dragonfly 与 Kvrocks 适配器内部的多命令流程也在每条角色/复制写命令前和等待确认循环中复验，不把整个适配器调用误当原子动作；Redis 系与 Dragonfly 的多配置持久化修改若中途失败，会在权威仍有效时恢复调用前配置。`external-command` 运行期间每 50ms 并发复验权威，失权时立即保护并终止直接子进程，不能等脚本超时后才发现旧 owner；隔离脚本若自行派生后台进程，仍必须使用动作令牌在基础设施侧实现幂等与过期拒绝。`POST /ha/node/{node}/promote` 必须先按当前 fencing 模式隔离旧主；隔离失败时目标节点不会被提升。local-file/Redis 协调先在 owner 条件下原子发布 outcome，`proxy-cluster` 先提交 `CompleteAction`，两者都只在提交成功后开放本地新主路由。

启用 `ha.persistence.fail_closed=true` 后，启动阶段会保持保护，直到首次 owner、角色、复制链路与后端配置完成验证。持久化交接的发布点晚于新 owner 验证、旧 owner 全部停用和协调结果提交；上一版已知 owner 即使本轮不可达也不能从停用集合中消失，失败期间管理面仍保留上一版已验证拓扑，不会把半完成结果当成成功。每轮即使本地拓扑缓存命中，也会重新读取后端配置确认 owner/non-owner 职责，发现外部漂移后重新取得权威并收敛。例外仅限当前已验证副本在 `probe_interval_ms × master_down_after_failures` 内短暂错过角色探测：这段有界宽限沿用上一版 owner/link 事实且不发送 `REPLICAOF`，宽限耗尽后恢复严格复验。启用 `force_checkpoint_on_handoff` 时，local-file/Redis 协调模式的进程重启不能仅凭配置外观采纳 owner，而会重新取得租约并生成可验证 checkpoint；`proxy-cluster` 只有在完整 owner/复制链路指纹的 `CompleteAction` 已提交且无在途动作时才允许跟随者采纳远端拓扑。

`auto_failover: false` 只关闭“NARIX 主动发起切主”，**拓扑收敛仍然开启**：副本漂移和已确认安全的单主变化仍会收敛。绕开 NARIX 直接切主时，外部控制面必须先把旧主降为新主的直接副本，再提升新主；NARIX 只有同时看到“唯一新主 + 旧主明确跟随新主”才直接跟随。旧主仅仅不可达并不能证明已隔离，此时 NARIX 达到故障阈值后先保持保护；若开启自动切主，则改走标准协调与 fencing 流程；未开启自动切主（含观察者）且旧主永久死亡时，唯一接管者连续稳定满 `master_down_after_failures` 轮后按受限跟随通道自动恢复，无需人工介入。使用 NARIX 管理接口可由同一控制面编排顺序。

## 观察者节点（ha.observer）

`ha.observer: true` 把该代理变成**纯观察者**：只探测、感应并跟随当前主节点、发布路由，绝不发送任何后端控制命令——不切主、不降级归队旧主、不主动纠正副本漂移、不做持久化编排；`/ha/failover`、`/ha/switchover`、`/ha/node/{node}/promote`、`/ha/node/{node}/replicaof/{master}`、`/ha/node/{node}/fence` 全部拒绝并返回明确错误；隐含 `auto_failover=false`，状态接口呈现 `observer: true`。

用途：生产 NARIX 集群负责切主，另起独立观察者节点（或多个）提供旁路查询。它优先在旧主已明确跟随新主等完整证据出现后切路由；旧主永久死亡无法提供证词时才使用下述受限跟随通道。双主期间始终保护且不干预生产控制面。这是“同一组后端只允许一套控制面”红线的唯一豁免。`observer` 只让 HA 控制面只读，不会拒绝业务写命令；只读查询端点必须同时设置 `read_only: true`。启动校验强制观察者不加入协调集群、`coordinator=none`、关闭持久化编排并保持 `fencing.mode=manual`。

观察者的主节点达到连续失败阈值后关闭路由，并在 `lastError`/`protectionReason` 记录原因。恢复走两条证据通道之一：外部控制面完成切主且旧主被探测为新主直接副本（降级证词）时立即跟随；旧主永久死亡、永远无法归队作证时，在旧主连败确认宕机且唯一接管者连续稳定满同样的 `master_down_after_failures` 轮后按受限跟随通道恢复——两个连续确认都满足才切路由，比构造期发现的单轮采信更严格，且全程仍是零控制写。双主始终保护。冷启动无主时以保护态等待。

部署须符合 [README 的版本与模式合同](../README.md#支持的后端与版本) 和 [后端控制能力约束](certification.md)。NARIX 不在运行时仅凭版本号授予控制权；角色、复制、checkpoint 或命令结果无法确认时，相应动作失败并保持保护。上线前的主故障演练验证的是**部署接线**而非产品语义：网络分区下的保护与恢复、`external-command` 隔离脚本的真实执行与超时行为、负载均衡按 `/ready` 摘挂、告警触达以及恢复时间是否满足预期。`ha.max_replica_lag: 0` 只关闭已知延迟的数值上限：在线副本按真实延迟、主库宕机导致的断链副本按断链时长参与切主门禁；全量同步、未知延迟或跟随错误上游的副本不参选，持久化 owner 门禁不接受任何断链副本。

全部托管复制 HA 产品统一使用 `narix_replication_ha_*` 指标前缀，并以 `proxy`、`node`、`role` 等标签区分代理项和数据节点；不能用产品名指标前缀判断当前适配器，产品类型以状态接口的 `backendProfile` 为准。

配置热更新状态中的 `proxies[].generation` 是当前接收新连接的运行代，`proxies[].drainingGenerations[]` 展示同名代理仍有连接待关闭的旧代。Prometheus 使用 `narix_proxy_generation{proxy}` 展示活动代，使用 `narix_proxy_draining_connections{proxy,generation}` 展示每个旧代真实的剩余连接数；代理项删除后，只要旧连接尚未结束，排空指标仍会保留，归零后自动清理。

Dragonfly 的 `dir` 是启动时固定参数。配置 `ha.persistence.dragonfly.dir` 时，NARIX 只读取并校验节点现值，不发送无效的动态修改命令；不一致会按 `fail_closed` 进入保护态。相同持久化错误只记录一次，并至少退避 5 秒后再探测收敛，避免按 HA 探测周期放大日志和控制命令。

恢复前应先确认安全主节点，再通过管理端接口或外部基础设施执行恢复操作。

CPU 累计秒数 `narix_process_cpu_seconds_total` 保留 Prometheus counter 类型和小数精度，通过 `/metrics` 与统一指标文本出口提供。当前结构化指标接口只支持整数 counter，因此该浮点指标不进入结构化快照；其余指标继续使用结构化快照，不把 CPU 秒数取整或改称 gauge。
