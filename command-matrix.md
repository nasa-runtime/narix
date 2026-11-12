# NARIX 命令矩阵

[English](command-matrix.en.md)

## 说明

本文档描述五种代理模式对 RESP 命令的处理边界。代理模式如下：

- `standalone`：固定后端透明代理。
- `sentinel`：发现并验证当前主节点后，使用透明数据面。
- `replication-ha`：完成受保护的主从拓扑选择后，使用透明数据面。
- `cluster`：也称 cluster-single-entry，对业务侧暴露一个普通 Redis 单节点入口，proxy 内部吸收 Redis Cluster slot 路由和聚合。
- `cluster-node-proxy`：一对一代理 Redis Cluster 每个节点，对业务侧仍暴露一组 proxy 节点地址，适合生产业务使用 Redis Cluster client。

设计原则：

- 透明模式保留后端原生语义；需要解析的命令必须具有明确的 key、会话和路由规则。
- 不支持的命令必须明确拒绝，不能猜 key、错路由或挂死连接。
- pipeline 必须保序。
- frontend 是 pipeline 时，proxy 到后端也必须保持 pipeline。
- 代理接入不需要清空后端数据；普通读写入口会保留 `FLUSHALL`、`FLUSHDB` 等原生命令的实际副作用，应由后端 ACL 限制权限。

后端最低版本与模式准入见 [支持的后端与版本](README.md#支持的后端与版本)。下文的命令类别不代表每个后端都实现其中全部命令；原生缺席、占位、参数限制、模块依赖及 ACL 仍由后端决定。

## 总览

| 命令类别 | standalone / sentinel / replication-ha | cluster | cluster-node-proxy |
| --- | --- | --- | --- |
| `PING` / `ECHO` / `COMMAND` | 透明透传 | 本地响应或显式策略 | 透传并改写必要拓扑 |
| `AUTH` / `HELLO` | 根据 `auth_mode` 处理；业务阶段透明透传 | frontend auth 本地处理 | frontend auth 本地处理，返回 cluster mode |
| `HELLO 3` 后端 RESP3 语义 | terminate 模式会同步后端协议版本 | 明确返回 `NOPROTO`；共享后端 lane 固定使用 RESP2，不能虚假宣称 RESP3 | terminate 模式会同步对应节点后端协议版本 |
| `RESET` | 进入透明隧道并清除后端会话状态；收到 `+RESET` 后应断开重连 | 默认拒绝，不能安全猜测单节点会话 | 进入透明隧道并清除后端会话状态；收到 `+RESET` 后应断开重连 |
| 字符串 | 透明透传；旧 Garnet 的 ETag 写入口按下述条件映射 | 按 key slot 路由 | 透传到客户端选择的 proxy node |
| 哈希 | 透明透传 | 按 key slot 路由 | 透传到客户端选择的 proxy node |
| `HDEL` | 支持 | 支持 | 支持，透传 |
| `DEL` / `UNLINK` | 支持 | 支持跨 slot 拆分聚合 integer | 支持，按 Redis Cluster 原生语义 |
| 列表 | 透明透传 | 单 key/同 slot 支持；阻塞命令拒绝 | 透传；阻塞命令进入隧道 |
| 集合 | 透明透传 | 单 key/同 slot 支持，跨 slot 集合运算默认拒绝 | 透传 |
| 有序集合 | 透明透传 | 单 key/同 slot 支持 | 透传 |
| 流（非阻塞） | 透明透传 | `XADD` / `XRANGE` / `XINFO` / `XREAD` 非阻塞支持 | 透传 |
| `XREAD BLOCK` / `XREADGROUP BLOCK` | 透明透传 | fail-closed | 透明隧道 |
| Pub/Sub | 透明透传 | fail-closed | 透明隧道 |
| 事务 | 透明透传 | 失败关闭 | 透传到客户端选择的节点 |
| Lua / Function | 透明透传 | 解析 `numkeys`，同 slot 路由，跨 slot 拒绝 | 透传 |
| 扫描 / 键查询 / 数据库大小 | 透明透传 | 跨节点聚合，受限流保护 | 透传 |
| Valkey `CLUSTERSCAN` | 透传原生游标；readonly 允许查询 | 按完整游标的哈希标签路由，包括初始游标 `0`；保留原生参数与游标 | 透传并改写 MOVED/ASK 地址 |
| RediSearch `FT.*` | 透明透传 | default-node 显式策略 | 透传 |
| RedisJSON `JSON.*` | 透明透传 | key-first 显式策略；`JSON.MGET` 拆分保序聚合；`JSON.MSET` 校验同 slot | 透传 |
| 未知命令 | 透明透传 | 默认拒绝，不能猜 key | 透传 |

## Garnet ETag 命令

`GETWITHETAG`、`GETIFNOTMATCH`、`SETWITHETAG`、`SETIFMATCH`、`SETIFGREATER`、`DELIFGREATER` 在 Cluster 中按第一个 key 路由。命令能否执行仍取决于目标 Garnet 的原生能力。

对缺少 `SETWITHETAG` 的旧 Garnet，NARIX 可将 RESP 数组形式的 `SETWITHETAG key value [EX seconds | PX milliseconds]` 映射为单条原子 `SET key value [EX seconds | PX milliseconds] WITHETAG`。请求保留在同一后端连接和原有流水线、事务位置，不通过客户端读后写组合实现 ETag。TTL 参数遵循有符号 32 位整数边界。

这一映射仅在后端连接建立时，使用该连接的认证身份，通过 `COMMAND INFO` 确认原生缺少 `SETWITHETAG` 且存在 `GETWITHETAG` 后启用。原生已支持该命令、元数据权限不足或透传认证尚未完成时保留原生行为；后续 `AUTH` 不会自动重新探测。映射后的写入由后端 `SET` 权限约束，`read_only=true` 仍拒绝该写命令。Garnet 的 key 权限能力以其原生 ACL 为准。

参数错误保持无写入副作用及对应事务拒绝阶段，但旧版返回文本可能不同：缺少 key 或 value 时错误中的命令名为 `AUTH`，不会改变认证身份；参数过多时返回 `ERR syntax error`。应用不应依赖这两种错误的命令名称或完整文本。专用连接上的兼容请求受 `frame_max_bytes` 限制；超限、非法帧或连接关闭时不重放未完成的请求。

## Garnet 单 key 迁移

Garnet 的专用连接可将 `MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password]` 的单个非空 key 规范为同一条命令的 `MIGRATE host port "" db timeout ... KEYS key` 形态。迁移、源端删除、COPY/REPLACE 和认证仍由后端执行，保持原有连接与命令顺序，不重试迁移。仅 RESP 数组且选项完整、可识别时转换；显式 KEYS、槽位迁移、未知选项和行式请求保留原生语义。

此能力使用建连时同一次 `COMMAND INFO` 查询确认 MIGRATE 入口可见，适用于 standalone 与 cluster-node-proxy 的专用连接；Cluster 单入口不猜测管理迁移的路由。元数据不可确认或透传认证尚未完成时不启用。普通 GET/SET 转发不增加迁移参数解析；专用连接上的请求受 `frame_max_bytes` 限制。readonly 在转换前拒绝所有 MIGRATE 形态。

Garnet 2.2.5 的目标 key 已存在时，原生 `COPY REPLACE` 可能保留目标旧值或丢失源 key 的过期时间，因此不承诺该形态的完整替换语义。NARIX 不通过先删除目标、重试迁移或事后补写 TTL 来改变这一边界。

## `read_only=true` 统一门禁

只读门禁位于各模式的数据路由之前，五种模式共用同一套命令分类。任何批次只要包含一条无法证明为只读的命令，整批命令都会返回 `READONLY`，不会先执行其中的读命令，也不会把危险命令转发到后端。

| 命令类别 | standalone / sentinel / replication-ha | cluster | cluster-node-proxy |
| --- | --- | --- | --- |
| 已登记的普通读命令 | 放行并转发到当前目标节点 | 放行后按槽位或聚合规则路由 | 放行并转发到对应节点 |
| 写命令 | `READONLY` | `READONLY` | `READONLY` |
| `EVAL`、`EVALSHA`、`EVAL_RO`、`EVALSHA_RO` | `READONLY` | `READONLY` | `READONLY` |
| `FCALL`、`FCALL_RO` | `READONLY` | `READONLY` | `READONLY` |
| `SCRIPT EXISTS` | 放行 | 放行并按集群策略查询 | 放行 |
| `SCRIPT LOAD`、`FLUSH`、`KILL`、`DEBUG` 及其他脚本状态修改入口 | `READONLY` | `READONLY` | `READONLY` |
| `FUNCTION LOAD`、`DELETE`、`FLUSH`、`RESTORE`、`KILL` | `READONLY` | `READONLY` | `READONLY` |
| `SCRIPT HELP`、`FUNCTION LIST`、`DUMP`、`STATS`、`HELP` | 门禁允许，按后端能力响应 | 门禁允许，但未登记集群路由的命令会明确拒绝 | 门禁允许，按后端能力响应 |
| 未知命令、事务、发布订阅、阻塞命令及其他透明隧道入口 | `READONLY` | `READONLY` | `READONLY` |

`_RO` 后缀只代表后端执行限制，不满足本代理“完全禁止用户脚本执行”的策略，因此不会获得例外。`SCRIPT EXISTS` 只读取脚本缓存状态，不执行脚本也不改变缓存，所以继续支持；`SCRIPT LOAD` 会改变脚本缓存，必须拒绝。

`FT.PROFILE <index> SEARCH ...` 和 `FT.PROFILE <index> AGGREGATE ...` 按索引名之后的查询类型通过只读门禁，查询参数仍由后端验证。缺少查询类型或使用未登记类型时返回 `READONLY`；索引名本身不影响只读判定。

搜索查询还支持 `FT.ALIASLIST`、`FT.DICTDUMP`、`FT.GET`、`FT.MGET`、`FT.SUGGET`、`FT.SUGLEN` 和 `FT.HYBRID`。`_FT.CONFIG` 与 `FT.CONFIG` 共用子命令门禁，只允许 `GET`、`HELP`；配置写入、别名变更、词典修改和建议写入继续拒绝。

Kvrocks 的 `FT.SEARCHSQL`、`FT.EXPLAINSQL` 与 `JSON.INFO` 也可用于只读查询。SQL 入口由后端的查询语言解析器处理，不是通用 SQL 写入通道；索引创建、删除和 JSON 数据修改仍在代理门禁拒绝。

只读查询还包括 `EXPIRETIME`、`PEXPIRETIME`、`HTTL`、`HPEXPIRETIME`、`FIELDTTL`、`SUBSTR`、`DIGEST`、`TOUCH`、`SINTERCARD`、`ZINTERCARD`、`XPENDING`、有序集合范围查询别名和 `JSON.ARRINDEX`。`TOUCH` 遵循后端的访问元数据语义，不修改键值或有效期。概率数据结构仅放行已登记的查询：`BF.EXISTS/MEXISTS/INFO`、`CF.EXISTS/MEXISTS/COUNT/INFO`、`CMS.QUERY/INFO` 和 `TOPK.QUERY/COUNT/LIST/INFO`；同族写命令及未知命令仍拒绝。门禁允许不代表后端实现了该命令，路由规则与后端原生能力继续生效。

`SORT_RO`、`GEORADIUS_RO` 和 `GEORADIUSBYMEMBER_RO` 允许查询，但可选参数中的 `STORE`、`STOREDIST` 写入目标仍在路由前拒绝。键名、成员名和查询模式中的相同字节不构成写入选项。

各后端的查询扩展也按完整命令名登记：

| 查询类型 | 允许的命令 |
| --- | --- |
| Pika 槽位查询 | `SLOTSHASHKEY`、`SLOTSINFO`、`SLOTSSCAN`、`SLOTSMGRT-ASYNC-STATUS`；遵循后端槽位配置，不等同于 Redis Cluster 路由 |
| Tendis 管理查询 | `ECHOKEY`、`COMMANDLIST`、`ROCKSPROPERTY`、`BINLOGPOS/START/TIME/META`、`RECYCLESTATUS`、`REPLSTATUS`、`TENDISSTAT`；`SHOW processlist [all]`、`SHOW locks` |
| TDigest 统计 | `TDIGEST.MIN/MAX/QUANTILE/CDF/TRIMMED_MEAN/RANK/REVRANK/BYRANK/BYREVRANK/INFO` |
| 时序 | `TS.GET/INFO/RANGE/REVRANGE/MGET/MRANGE/MREVRANGE/NRANGE/NREVRANGE/QUERYINDEX/QUERYLABELS`；`TS.READ key timestamp [MAX_COUNT count]` |
| 向量集合 | `VCARD`、`VDIM`、`VEMB`、`VGETATTR`、`VINFO`、`VISMEMBER`、`VLINKS`、`VRANDMEMBER`、`VRANGE`、`VSIM` |
| 概率结构信息与导出 | `BF.CARD/DEBUG/SCANDUMP`、`CF.DEBUG/SCANDUMP` |
| 字符串、哈希与集合 | `LCS`、`STRALGO LCS`、`HEXPIRETIME`、`HPTTL`、`SDIFFCARD`、`SUNIONCARD` |
| 数组 | `ARCOUNT`、`ARGET`、`ARGETRANGE`、`ARGREP`、`ARINFO`、`ARLASTITEMS`、`ARLEN`、`ARMGET`、`ARNEXT`、`AROP`、`ARSCAN` |
| KeyDB 嵌套哈希与多键存在性 | `KEYDB.NHGET`、`KEYDB.MEXISTS` |
| Kvrocks 哈希范围与整数集合 | `HRANGEBYLEX`、`SICARD`、`SIEXISTS`、`SIRANGE`、`SIRANGEBYVALUE`、`SIREVRANGE`、`SIREVRANGEBYVALUE` |
| Garnet 有序集合成员有效期 | `ZEXPIRETIME`、`ZPEXPIRETIME`、`ZPTTL`、`ZTTL` |
| Garnet Range Index | `RI.EXISTS`、`RI.GET`、`RI.SCAN`、`RI.RANGE`、`RI.CONFIG`、`RI.METRICS`；后端须启用该类型并具备对应原生依赖 |
| Pika 范围扫描 | `HSCANX`、`PKHRSCANRANGE`、`PKHSCANRANGE`、`PKRSCANRANGE`、`PKSCANRANGE`、`SCANX` |
| Tendis 值版本、位图与有序集合扫描 | `GETVSN`、`HMGETVSN`、`TBITCOUNT`、`TBITPOS`、`TGETBIT`、`ZSCANBYSCORE` |
| Tendis 存储与管理数据查询 | `HSIZE`、`LSIZE`、`SSIZE`、`ZSIZE`、`DBEMPTY`、`ITERALL`、`ITERALLKEYS`、`ADMINGET`；`DUMPX` 会改变连接的数据库选择，不属于 readonly 查询 |
| 运维查询 | `LASTSAVE`、`LOLWUT`；`SLOWLOG GET/LEN/HELP`、`LATENCY LATEST/HISTORY/DOCTOR/GRAPH/HISTOGRAM/HELP`、`MODULE LIST/HELP`、`ACL CAT/GETUSER/LIST/USERS/WHOAMI/HELP`；重置、加载、卸载、权限变更及未知子命令拒绝 |
| Valkey 命令日志查询 | `COMMANDLOG GET/LEN/HELP`；具体日志类别由后端校验，`RESET` 及未知子命令拒绝 |
| Pika 缓存查询 | `CACHE RANDOMKEY`；缓存删除、清空、命中率重置及未知子命令拒绝 |
| Redis 热点与备份查询 | 仅 `backend_profile: redis` 开放 `HOTKEYS GET/HELP`、`BACKUP STATUS/LIST/HELP`，只接受命令名和子命令两个参数；其他后端的同名命令、采样启停、统计重置、备份创建、封存、中止、清理及未知形态在 readonly 入口拒绝 |
| Redis 搜索拓扑查询 | 仅 `backend_profile: redis` 开放无参数 `SEARCH.CLUSTERINFO`；拓扑设置、刷新和带参数形态在 readonly 入口拒绝 |
| Tendis 元数据查询 | 仅 `backend_profile: tendis` 开放 `SYNCVERSION name ? ? v1`、`PFDEBUG ENCODING/DECODE key` 与 `SLOTSEMPTY start end` 的完整参数形态；槽位范围必须满足 `0 <= start <= end < 16384`。版本元数据写入、会改写 HLL 编码的 `GETREG/TODENSE`、槽位删除或压缩、多余参数和未知子命令拒绝；`INCRMETA` 多帧数据流与 `DEXEC` 嵌套执行仍拒绝进入 readonly |
| Kvrocks 存储查询 | `DISK USAGE`、`STATS`、`POLLUPDATES`、`PERFLOG GET/LEN`、`KPROFILE MEMORY STATUS`；日志重置、采样启停、文件导出、存储导入、刷盘及未知管理子命令拒绝 |
| Kvrocks 元数据与命名空间查询 | `KMETADATA`、`NAMESPACE GET/CURRENT`；命名空间及令牌查询按后端认证权限执行，命名空间增删、令牌变更和跨命名空间移动拒绝 |
| Kvrocks 集群查询 | `CLUSTERX MYID/VERSION`，只接受完整查询形态；导入、迁移、改拓扑和清槽操作在 readonly 入口拒绝 |
| Garnet 自定义对象查询 | `CUSTOMOBJECTSCAN key cursor [MATCH pattern] [COUNT count]`；动态注册的命令和事务过程仍不进入 readonly 白名单 |

`STRALGO` 只允许 `LCS` 子命令；数组、嵌套哈希、整数集合、位图及有效期的写入变体继续拒绝，不根据后端命令元数据自动扩大白名单。

`TS.READ` 在只读模式仅接受键、游标及可选的一组 `MAX_COUNT` 参数。包含 `BLOCK`、未知选项或不完整参数时返回 `READONLY`，避免阻塞读取占用共享请求连接；键名本身为 `BLOCK` 不受影响。普通模式中的阻塞读取仍使用专用连接。同族创建、修改、规则变更、删除、向量属性写入及 `LOADCHUNK` 导入继续拒绝。

## 透明数据面（Standalone / Sentinel / Replication HA）

`standalone` 直接使用固定后端；`sentinel` 和 `replication-ha` 先由各自控制面选择当前安全主节点。目标确定后，`standalone` 和 `sentinel` 解析请求以管理普通连接池、只读策略和专用会话，并尽量复用原始字节。`replication-ha` 普通读写使用一对一透明 relay；只读入口才逐批解析命令。各模式不通过无必要的解码、重编码改变原生参数和响应。

主要支持场景：

- frontend auth：未认证拒绝，错误密码拒绝，`HELLO 2 AUTH` 成功。
- `HELLO 3 AUTH` 后端协议同步，`HGETALL` 返回 RESP3 map。
- 字符串：`SET` / `GET` / `TYPE` / `EXPIRE` / `TTL`。
- 计数器：`INCRBY` / `DECRBY`。
- 多键命令：`MSET` / `MGET`。
- 哈希：`HSET` / `HGET` / `HGETALL` / `HSCAN` / `HDEL`。
- 列表：`RPUSH` / `LRANGE` / `LPOP` / `RPOP` / `BLPOP`。
- 集合：`SADD` / `SISMEMBER` / `SMEMBERS` / `SREM`。
- 有序集合：`ZADD` / `ZRANGE WITHSCORES` / `ZSCORE`。
- 流：`XADD` / `XLEN` / `XRANGE` / `XREAD BLOCK`。
- 基数统计：`PFADD` / `PFCOUNT`。
- 地理位置：`GEOADD` / `GEODIST`。
- Lua 脚本：`EVAL`。
- 事务：`MULTI` / `EXEC`。
- 发布/订阅：`SUBSCRIBE` / `PUBLISH` / `UNSUBSCRIBE`。
- 大值受所在转发路径的帧、批次和响应预算约束，详见[容量配置](docs/configuration.md#连接容量和超时)。
- 清理：`DEL`。

透明转发保留后端自身的会话语义，同名命令不保证具有 Redis 的全部行为。Tendis 的 `MULTI`/`EXEC` 不提供 Redis 式排队与原子提交，期间的数据命令仍可能立即执行；它不能作为原子事务使用。只有后端实际提供 `SUBSCRIBE` 等命令时才可建立订阅，缺少这些命令的旧 Tendis 不会由代理补出订阅能力。RESP3 也须由目标后端原生支持；Pika 和 Tendis 使用 RESP2。

## 集群单入口（Cluster-Single-Entry）

`cluster` 模式对业务侧暴露普通 Redis 单节点入口。它必须主动解析命令、提取 key、计算 Redis Cluster slot，并按 slot 路由到后端节点。

主要支持场景：

- frontend auth：未认证拒绝，错误密码拒绝，`HELLO 2 AUTH` 成功。
- `SELECT 0` 兼容成功，`SELECT 1` 明确失败。
- `COMMAND` 返回空数组，避免客户端探测失败。
- `INFO cluster` 本地返回 `cluster_enabled:0`，保证自动探测型客户端按单节点入口连接。
- 字符串 / 计数器 / 哈希 / 列表 / 集合 / 有序集合 / 流 / HLL / 地理位置。
- Hash 删除：`HDEL` 按 key 路由，字段删除与查询遵循目标后端语义。
- 跨 slot `MGET` 拆分聚合并按原始 key 顺序返回。
- 跨 slot `MSET` 返回 `CROSSSLOT`。
- `DEL` 清理多类 key。
- `EXISTS` 跨 slot integer 聚合。
- `SCAN` / `KEYS` / `DBSIZE` 跨节点聚合。
- `SCRIPT LOAD` 广播或显式策略。
- `EVALSHA` 同 slot 路由。
- `EVAL` 跨 slot 返回 `CROSSSLOT`。
- `FT.INFO` 使用已登记的 default-node 策略，模块能力与权限由目标后端决定。
- `JSON.SET` / `JSON.DEBUG MEMORY` 按真实 document key 路由或由后端返回模块错误。
- `JSON.MGET` 按 key 拆成 `JSON.GET` 并按原始 key 顺序聚合；`JSON.MSET` 校验 triplet key 同 slot。
- 大 pipeline 保序。
- 高并发 ordered pipeline 保序。

明确 fail-closed 的场景：

- `MULTI`。
- Pub/Sub，例如 `PUBLISH`。
- Blocking list，例如 `BLPOP`。
- `XREAD BLOCK` / `XREADGROUP BLOCK`。
- `WAIT`。
- 未注册 unknown command。

## 集群节点代理（Cluster-Node-Proxy）

`cluster-node-proxy` 一对一代理 Redis Cluster 每个源节点。业务侧 Redis Cluster client 连接的是 proxy 暴露的 cluster 节点地址。

主要支持场景：

- `HELLO 3 AUTH ...` 返回 cluster mode。
- `HELLO 3 AUTH ...` 后端协议同步，`HGETALL` 返回 RESP3 map。
- `CLUSTER SLOTS` master 地址全部改写为 proxy listener 地址。
- `CLUSTER NODES` 不泄露源 Redis 地址。
- `CLUSTER SHARDS` RESP3 map 能改写地址。
- `MOVED` / `ASK` 错误地址改写为 proxy 地址。
- `READONLY` / `READWRITE` / `ASKING` 绑定当前会话的独占后端连接，后续普通响应继续改写重定向和拓扑地址；连接状态不返回共享池。
- 同节点 pipeline：`SET` / `GET` / `DEL` 严格保序。
- 错节点请求返回指向 proxy 节点的 `MOVED`。
- Pub/Sub 进入透明隧道。
- `BLPOP` 进入透明隧道。
- `XREAD BLOCK` 进入透明隧道。
- 并发 routed pipeline 保持连接内响应顺序。

注意：

- 普通命令不在 proxy 内做 slot 聚合，slot 选择由业务侧 Redis Cluster client 完成。
- proxy 只在必要时解析 request/response：拓扑改写、MOVED/ASK 地址改写、隧道命令识别、请求上限保护。
- 有明确一问一答语义的 `READONLY` / `READWRITE` / `ASKING` 使用独占连接并逐帧处理；订阅、复制流、其他会话命令及未知命令按透明隧道边界执行。已绑定会话转入隧道时沿用原连接和容量许可，避免丢失连接状态。
- Kvrocks 的 `CLUSTER IMPORT` 使用独占透明连接，导入权限不进入共享连接池；`CLUSTERX` 仅标识与配置版本查询进入普通响应路径，迁移和其他控制动作保留透明流语义。
