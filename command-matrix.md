# NARIX 命令矩阵

## 说明

本文档描述五种代理模式对 RESP 命令的处理边界。命令执行路径归纳为三类：

- `standalone`：固定后端透明代理。
- `sentinel`：发现并验证当前主节点后，使用透明数据面。
- `replication-ha`：完成受保护的主从拓扑选择后，使用透明数据面。
- `cluster`：也称 cluster-single-entry，对业务侧暴露一个普通 Redis 单节点入口，proxy 内部吸收 Redis Cluster slot 路由和聚合。
- `cluster-node-proxy`：一对一代理 Redis Cluster 每个节点，对业务侧仍暴露一组 proxy 节点地址，适合生产业务使用 Redis Cluster client。

设计原则：

- 支持的命令必须有测试或明确透传理由。
- 不支持的命令必须明确拒绝，不能猜 key、错路由或挂死连接。
- pipeline 必须保序。
- frontend 是 pipeline 时，proxy 到后端也必须保持 pipeline。
- 每次系统性压测前后都要清空并校验 `DBSIZE=0`。
- 功能测试必须覆盖 `DEL` 和 `HDEL`，防止压测 key/hash field 无序增长。

## 总览

| 命令类别 | standalone / sentinel / replication-ha | cluster | cluster-node-proxy |
| --- | --- | --- | --- |
| `PING` / `ECHO` / `COMMAND` | 透明透传 | 本地响应或显式策略 | 透传并改写必要拓扑 |
| `AUTH` / `HELLO` | 根据 `auth_mode` 处理；业务阶段透明透传 | frontend auth 本地处理 | frontend auth 本地处理，返回 cluster mode |
| `HELLO 3` 后端 RESP3 语义 | terminate 模式会同步后端协议版本 | 明确返回 `NOPROTO`；共享后端 lane 固定使用 RESP2，不能虚假宣称 RESP3 | terminate 模式会同步对应节点后端协议版本 |
| `RESET` | 进入透明隧道并清除后端会话状态；收到 `+RESET` 后应断开重连 | 默认拒绝，不能安全猜测单节点会话 | 进入透明隧道并清除后端会话状态；收到 `+RESET` 后应断开重连 |
| 字符串 | 透明透传 | 按 key slot 路由 | 透传到客户端选择的 proxy node |
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
| RediSearch `FT.*` | 透明透传 | default-node 显式策略 | 透传 |
| RedisJSON `JSON.*` | 透明透传 | key-first 显式策略；`JSON.MGET` 拆分保序聚合；`JSON.MSET` 校验同 slot | 透传 |
| 未知命令 | 透明透传 | 默认拒绝，不能猜 key | 透传 |

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

## 透明数据面（Standalone / Sentinel / Replication HA）

`standalone` 直接使用固定后端；`sentinel` 和 `replication-ha` 先由各自控制面选择当前安全主节点。目标确定后，三种模式共用透明业务数据面，业务阶段不做不必要的 RESP 解码和重编码。

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
- 大批量数据：512KB 值。
- 清理：`DEL`。

## 集群单入口（Cluster-Single-Entry）

`cluster` 模式对业务侧暴露普通 Redis 单节点入口。它必须主动解析命令、提取 key、计算 Redis Cluster slot，并按 slot 路由到后端节点。

主要支持场景：

- frontend auth：未认证拒绝，错误密码拒绝，`HELLO 2 AUTH` 成功。
- `SELECT 0` 兼容成功，`SELECT 1` 明确失败。
- `COMMAND` 返回空数组，避免客户端探测失败。
- `INFO cluster` 本地返回 `cluster_enabled:0`，保证自动探测型客户端按单节点入口连接。
- 字符串 / 计数器 / 哈希 / 列表 / 集合 / 有序集合 / 流 / HLL / 地理位置。
- Hash 删除：`HDEL` 后 `HGET` 必须为空。
- 跨 slot `MGET` 拆分聚合并按原始 key 顺序返回。
- 跨 slot `MSET` 返回 `CROSSSLOT`。
- `DEL` 清理多类 key。
- `EXISTS` 跨 slot integer 聚合。
- `SCAN` / `KEYS` / `DBSIZE` 跨节点聚合。
- `SCRIPT LOAD` 广播或显式策略。
- `EVALSHA` 同 slot 路由。
- `EVAL` 跨 slot 返回 `CROSSSLOT`。
- `FT.INFO` 不允许被 proxy 错误判成未注册命令。
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
- 同节点 pipeline：`SET` / `GET` / `DEL` 严格保序。
- 错节点请求返回指向 proxy 节点的 `MOVED`。
- Pub/Sub 进入透明隧道。
- `BLPOP` 进入透明隧道。
- `XREAD BLOCK` 进入透明隧道。
- 并发 routed pipeline 压测。

注意：

- 普通命令不在 proxy 内做 slot 聚合，slot 选择由业务侧 Redis Cluster client 完成。
- proxy 只在必要时解析 request/response：拓扑改写、MOVED/ASK 地址改写、隧道命令识别、请求上限保护。
- 连接绑定或阻塞命令必须切到透明隧道，不能继续使用固定响应数量模型。
