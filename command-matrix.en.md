# NARIX command matrix

[简体中文](command-matrix.md)

## Scope

The five modes have different routing responsibilities:

- `standalone`: one fixed backend.
- `sentinel`: transparent business access after primary discovery and validation.
- `replication-ha`: transparent business access after protected replication-topology selection.
- `cluster`: a single non-Cluster-facing endpoint with internal slot routing and aggregation.
- `cluster-node-proxy`: one proxy per real node, retaining ordinary Cluster-client behavior.

Transparent modes preserve native semantics. Commands requiring parsing need explicit key, session and routing rules; unsupported operations are rejected instead of guessing keys or hanging. Preserve pipeline response order and backend pipelining. Connecting through NARIX does not require clearing data. Ordinary writing access preserves the real effects of `FLUSHALL` and `FLUSHDB`; restrict them through backend ACLs.

See [backend versions and modes](README.en.md#supported-backends-and-versions). A category below does not mean every backend implements all of its commands. Missing commands, placeholders, argument limits, modules and ACLs remain native constraints.

## Overview

| Category | standalone / sentinel / replication-ha | cluster | cluster-node-proxy |
|---|---|---|---|
| `PING` / `ECHO` / `COMMAND` | Forward | Local response or explicit policy | Forward and rewrite topology where needed |
| `AUTH` / `HELLO` | Apply `auth_mode`; forward business-phase native semantics | Local frontend authentication | Local frontend authentication, reporting cluster mode |
| RESP3 after `HELLO 3` | Terminate mode synchronizes backend protocol | `NOPROTO`; shared lanes use RESP2 | Terminate mode synchronizes that node's protocol |
| `RESET` | Transparent tunnel; reconnect after `+RESET` clears backend session state | Reject; cannot infer one node's session | Transparent tunnel; reconnect after `+RESET` |
| Strings | Forward; conditional older-Garnet ETag translation below | Key-slot routing | Client-selected node |
| Hashes / `HDEL` | Forward | Key-slot routing | Forward |
| `DEL` / `UNLINK` | Forward | Split across slots and sum integers | Native Cluster semantics |
| Lists | Forward | Single-key/same-slot operations; reject blocking | Forward; tunnel blocking commands |
| Sets | Forward | Single-key/same-slot; cross-slot set operations rejected by default | Forward |
| Sorted sets | Forward | Single-key/same-slot | Forward |
| Nonblocking streams | Forward | `XADD`, `XRANGE`, `XINFO`, nonblocking `XREAD` | Forward |
| `XREAD BLOCK` / `XREADGROUP BLOCK` | Forward | Fail closed | Transparent tunnel |
| Pub/Sub | Forward | Fail closed | Transparent tunnel |
| Transactions | Forward | Fail closed | Client-selected node |
| Lua / Function | Forward | Parse `numkeys`; same-slot routing, reject cross-slot | Forward |
| Scan / key queries / database size | Forward | Bounded cross-node aggregation | Forward |
| Valkey `CLUSTERSCAN` | Native cursor; allowed readonly query | Route by the full cursor's hash tag, including initial `0`; preserve arguments/cursor | Forward and rewrite redirects |
| RediSearch `FT.*` | Forward | Explicit default-node policy | Forward |
| RedisJSON `JSON.*` | Forward | Explicit key-first policy; ordered `JSON.MGET` splitting; same-slot `JSON.MSET` | Forward |
| Unknown commands | Forward | Reject; never guess keys | Forward |

## Garnet ETag commands

Cluster routes `GETWITHETAG`, `GETIFNOTMATCH`, `SETWITHETAG`, `SETIFMATCH`, `SETIFGREATER` and `DELIFGREATER` by their first key. Native availability remains a backend requirement.

For older Garnet without `SETWITHETAG`, NARIX can translate RESP-array `SETWITHETAG key value [EX seconds | PX milliseconds]` into one atomic `SET key value [EX seconds | PX milliseconds] WITHETAG`. It keeps the backend connection, pipeline order and transaction position. There is no client-side read/modify/write emulation. TTL arguments follow signed 32-bit bounds.

Translation activates only when connection-time `COMMAND INFO`, under that connection's authenticated identity, confirms missing `SETWITHETAG` and present `GETWITHETAG`. Native support, insufficient metadata permission or incomplete pass-through authentication retains native behavior. Later `AUTH` does not reprobe automatically. Backend `SET` permission governs the translated write; readonly still rejects it. Key permissions remain Garnet's native ACL contract.

Invalid arguments retain no-write side effects and the corresponding transaction rejection phase, although older response text can differ: missing key/value may report command name `AUTH` without changing identity; excess arguments return `ERR syntax error`. Do not depend on those full messages. Dedicated compatibility requests obey `frame_max_bytes`; oversized/invalid input or disconnection never replays unfinished requests.

## Garnet single-key migration

Dedicated connections can normalize `MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password]` with one nonempty key into `MIGRATE host port "" db timeout ... KEYS key`. The backend still owns migration, deletion, COPY/REPLACE and authentication. Preserve connection/order and do not retry. Convert only RESP arrays with complete recognized options; explicit KEYS, slot migration, unknown options and inline commands retain native semantics.

Connection-time `COMMAND INFO` must confirm MIGRATE visibility. This applies to standalone and node-proxy dedicated connections, not guessed administrative routing in single-entry Cluster. Unconfirmed metadata or incomplete pass-through authentication disables translation. Ordinary GET/SET does not parse migration options. Dedicated requests obey `frame_max_bytes`; readonly rejects every MIGRATE form before conversion.

When the destination key exists, Garnet 2.2.5 native `COPY REPLACE` may preserve its old value or lose source expiry. Complete replacement semantics are therefore not guaranteed. NARIX does not change this by deleting the destination first, retrying migration or rewriting TTL afterward.

## Unified `read_only=true` gate

All five modes classify commands before routing. A parsed batch containing any command not proven readonly returns `READONLY` for every command. Reads in that batch are not executed first and dangerous commands never reach the backend.

| Category | All transparent/node-proxy modes | cluster |
|---|---|---|
| Registered ordinary reads | Allow and forward to the selected backend | Allow, then apply slot/aggregation rules |
| Writes | `READONLY` | `READONLY` |
| `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO` | `READONLY` | `READONLY` |
| `FCALL`, `FCALL_RO` | `READONLY` | `READONLY` |
| `SCRIPT EXISTS` | Allow | Allow under cluster query policy |
| `SCRIPT LOAD/FLUSH/KILL/DEBUG` and other script changes | `READONLY` | `READONLY` |
| `FUNCTION LOAD/DELETE/FLUSH/RESTORE/KILL` | `READONLY` | `READONLY` |
| `SCRIPT HELP`, `FUNCTION LIST/DUMP/STATS/HELP` | Gate allows; native backend responds | Gate allows, but unregistered cluster routes still reject |
| Unknown commands, transactions, Pub/Sub, blocking and other tunnel operations | `READONLY` | `READONLY` |

`_RO` denotes a backend restriction, not permission under NARIX's policy of forbidding all user-script execution. `SCRIPT EXISTS` only queries cache state; `SCRIPT LOAD` changes it.

`FT.PROFILE <index> SEARCH ...` and `FT.PROFILE <index> AGGREGATE ...` are classified by the query type following the index name. Missing/unknown query types return `READONLY`; the index name itself does not determine classification. The backend validates query parameters.

Search reads also include `FT.ALIASLIST`, `FT.DICTDUMP`, `FT.GET`, `FT.MGET`, `FT.SUGGET`, `FT.SUGLEN`, `FT.HYBRID`. `_FT.CONFIG` and `FT.CONFIG` allow only `GET` and `HELP`; configuration, alias, dictionary and suggestion writes remain rejected. Kvrocks `FT.SEARCHSQL`, `FT.EXPLAINSQL` and `JSON.INFO` are queries, not general SQL write channels; index and JSON mutations remain blocked.

Other reads include `EXPIRETIME`, `PEXPIRETIME`, `HTTL`, `HPEXPIRETIME`, `FIELDTTL`, `SUBSTR`, `DIGEST`, `TOUCH`, `SINTERCARD`, `ZINTERCARD`, `XPENDING`, sorted-set range aliases and `JSON.ARRINDEX`. `TOUCH` retains native access-metadata behavior without changing values or expiry. Probabilistic structures allow only registered queries: `BF.EXISTS/MEXISTS/INFO`, `CF.EXISTS/MEXISTS/COUNT/INFO`, `CMS.QUERY/INFO`, `TOPK.QUERY/COUNT/LIST/INFO`. Gate admission does not create a native command or a cluster route.

`SORT_RO`, `GEORADIUS_RO` and `GEORADIUSBYMEMBER_RO` are allowed queries, but optional `STORE`/`STOREDIST` destinations are rejected before routing. Identical bytes in keys, members or query patterns are not treated as write options.

| Query category | Allowed forms and boundaries |
|---|---|
| Pika slots | `SLOTSHASHKEY`, `SLOTSINFO`, `SLOTSSCAN`, `SLOTSMGRT-ASYNC-STATUS`; native slot settings, not standard Cluster routing |
| Tendis administration | `ECHOKEY`, `COMMANDLIST`, `ROCKSPROPERTY`, `BINLOGPOS/START/TIME/META`, `RECYCLESTATUS`, `REPLSTATUS`, `TENDISSTAT`; `SHOW processlist [all]`, `SHOW locks` |
| TDigest | `TDIGEST.MIN/MAX/QUANTILE/CDF/TRIMMED_MEAN/RANK/REVRANK/BYRANK/BYREVRANK/INFO` |
| Time series | `TS.GET/INFO/RANGE/REVRANGE/MGET/MRANGE/MREVRANGE/NRANGE/NREVRANGE/QUERYINDEX/QUERYLABELS`; `TS.READ key timestamp [MAX_COUNT count]` |
| Vector sets | `VCARD`, `VDIM`, `VEMB`, `VGETATTR`, `VINFO`, `VISMEMBER`, `VLINKS`, `VRANDMEMBER`, `VRANGE`, `VSIM` |
| Probabilistic information/export | `BF.CARD/DEBUG/SCANDUMP`, `CF.DEBUG/SCANDUMP` |
| Strings, hashes, sets | `LCS`, `STRALGO LCS`, `HEXPIRETIME`, `HPTTL`, `SDIFFCARD`, `SUNIONCARD` |
| Arrays | `ARCOUNT`, `ARGET`, `ARGETRANGE`, `ARGREP`, `ARINFO`, `ARLASTITEMS`, `ARLEN`, `ARMGET`, `ARNEXT`, `AROP`, `ARSCAN` |
| KeyDB nested hashes/existence | `KEYDB.NHGET`, `KEYDB.MEXISTS` |
| Kvrocks hash ranges/integer sets | `HRANGEBYLEX`, `SICARD`, `SIEXISTS`, `SIRANGE`, `SIRANGEBYVALUE`, `SIREVRANGE`, `SIREVRANGEBYVALUE` |
| Garnet sorted-set member expiry | `ZEXPIRETIME`, `ZPEXPIRETIME`, `ZPTTL`, `ZTTL` |
| Garnet Range Index | `RI.EXISTS`, `RI.GET`, `RI.SCAN`, `RI.RANGE`, `RI.CONFIG`, `RI.METRICS`; native type and dependencies required |
| Pika range scans | `HSCANX`, `PKHRSCANRANGE`, `PKHSCANRANGE`, `PKRSCANRANGE`, `PKSCANRANGE`, `SCANX` |
| Tendis versions/bitmaps/sorted-set scans | `GETVSN`, `HMGETVSN`, `TBITCOUNT`, `TBITPOS`, `TGETBIT`, `ZSCANBYSCORE` |
| Tendis storage queries | `HSIZE`, `LSIZE`, `SSIZE`, `ZSIZE`, `DBEMPTY`, `ITERALL`, `ITERALLKEYS`, `ADMINGET`; `DUMPX` changes selected database and is not readonly |
| Operational queries | `LASTSAVE`, `LOLWUT`; `SLOWLOG GET/LEN/HELP`, `LATENCY LATEST/HISTORY/DOCTOR/GRAPH/HISTOGRAM/HELP`, `MODULE LIST/HELP`, `ACL CAT/GETUSER/LIST/USERS/WHOAMI/HELP`; reject resets, loads/unloads, permission changes and unknown subcommands |
| Valkey command logs | `COMMANDLOG GET/LEN/HELP`; native log-category validation; reject `RESET` and unknown forms |
| Pika cache | `CACHE RANDOMKEY`; reject cache deletion/clear, hit-rate reset and unknown forms |
| Redis hot keys/backups | Only profile `redis`: `HOTKEYS GET/HELP`, `BACKUP STATUS/LIST/HELP`, exactly command plus subcommand. Reject other profiles, sampling changes, resets, backup create/seal/abort/cleanup and unknown forms |
| Redis search topology | Only profile `redis`: argument-free `SEARCH.CLUSTERINFO`; reject topology setting/refresh and arguments |
| Tendis metadata | Only profile `tendis`: complete `SYNCVERSION name ? ? v1`, `PFDEBUG ENCODING/DECODE key`, `SLOTSEMPTY start end` with `0 <= start <= end < 16384`. Reject metadata writes, HLL-changing `GETREG/TODENSE`, slot deletion/compaction, excess arguments and unknown forms. `INCRMETA` multi-frame streams and nested `DEXEC` remain disallowed |
| Kvrocks storage | `DISK USAGE`, `STATS`, `POLLUPDATES`, `PERFLOG GET/LEN`, `KPROFILE MEMORY STATUS`; reject resets, sampling changes, file exports, storage imports, flushes and unknown administrative forms |
| Kvrocks metadata/namespaces | `KMETADATA`, `NAMESPACE GET/CURRENT` under native credentials; reject namespace creation/removal, token changes and cross-namespace moves |
| Kvrocks cluster | Complete `CLUSTERX MYID/VERSION` queries only; reject import, migration, topology changes and slot clearing |
| Garnet custom objects | `CUSTOMOBJECTSCAN key cursor [MATCH pattern] [COUNT count]`; dynamically registered commands/transaction procedures are not added to the allowlist |

`STRALGO` permits only `LCS`. Write variants for arrays, nested hashes, integer sets, bitmaps and expiry remain rejected; backend command metadata never automatically expands the allowlist.

Readonly `TS.READ` accepts a key, cursor and optionally one `MAX_COUNT` pair. `BLOCK`, unknown options or incomplete parameters return `READONLY`, avoiding blocking on a shared request connection. A key named `BLOCK` is not an option. Normal blocking reads use dedicated connections. Creation, modification, rules, deletion, vector attributes and `LOADCHUNK` imports remain disallowed.

## Transparent modes

Standalone uses a fixed backend; Sentinel and HA select a safe primary through their controllers. Standalone/Sentinel parse requests for ordinary pools, readonly policy and dedicated sessions while reusing original bytes where possible. Ordinary HA traffic uses one-to-one transparent relay; readonly HA parses each batch. Unnecessary decode/re-encode steps do not alter native arguments or responses.

Supported native scenarios include authenticated `HELLO 2 AUTH`, backend RESP3 synchronization after `HELLO 3 AUTH` and RESP3-map `HGETALL`; strings (`SET/GET/TYPE/EXPIRE/TTL`), counters (`INCRBY/DECRBY`), multi-key operations (`MSET/MGET`), hashes (`HSET/HGET/HGETALL/HSCAN/HDEL`), lists (`RPUSH/LRANGE/LPOP/RPOP/BLPOP`), sets (`SADD/SISMEMBER/SMEMBERS/SREM`), sorted sets (`ZADD/ZRANGE WITHSCORES/ZSCORE`), streams (`XADD/XLEN/XRANGE/XREAD BLOCK`), HLL (`PFADD/PFCOUNT`), geo (`GEOADD/GEODIST`), Lua `EVAL`, transactions `MULTI/EXEC`, subscriptions `SUBSCRIBE/PUBLISH/UNSUBSCRIBE` and `DEL`. Incorrect/missing frontend authentication is rejected. Large values obey the [applicable path budgets](docs/configuration.en.md#connections-capacity-and-timeouts).

Identical command names do not guarantee full Redis semantics. Tendis `MULTI`/`EXEC` does not provide Redis-style queuing and atomic commit; commands may execute immediately, so it cannot be used as an atomic transaction. Older Tendis without subscription commands gains no synthetic Pub/Sub. RESP3 requires native support; Pika and Tendis use RESP2.

## Single-entry Cluster

NARIX parses commands, identifies keys, calculates slots and routes internally. Its supported behavior includes:

- Frontend authentication and `HELLO 2 AUTH`; reject missing/invalid credentials.
- Accept `SELECT 0`, reject `SELECT 1`.
- Return an empty `COMMAND` array and local `INFO cluster` with `cluster_enabled:0` so automatic clients use single-node access.
- Route strings, counters, hashes, lists, sets, sorted sets, streams, HLL and geo by their registered rules. `HDEL` uses its real key; native field semantics remain unchanged.
- Split cross-slot `MGET` and preserve key order; reject cross-slot `MSET`.
- Sum cross-slot `EXISTS`, support multi-key `DEL`, and aggregate `SCAN`, `KEYS`, `DBSIZE` across nodes.
- Apply explicit broadcast/routing policy for `SCRIPT LOAD`, route same-slot `EVALSHA`, and return `CROSSSLOT` for cross-slot `EVAL`.
- Route `FT.INFO` to its registered default node, where module availability/permissions decide the result.
- Route `JSON.SET` and `JSON.DEBUG MEMORY` by the real document key or return the backend module error. Split `JSON.MGET` into ordered `JSON.GET` requests; require `JSON.MSET` triplet keys to share a slot.
- Preserve response order for large/concurrent pipelines.

Fail closed for `MULTI`, Pub/Sub including `PUBLISH`, blocking lists such as `BLPOP`, blocking `XREAD/XREADGROUP`, `WAIT` and unregistered commands.

## Cluster node proxy

Clients see proxy addresses for each real Cluster node and retain native slot selection:

- `HELLO 3 AUTH` reports cluster mode and synchronizes backend RESP3, including map responses such as `HGETALL`.
- Rewrite primary addresses in `CLUSTER SLOTS`, addresses in `CLUSTER NODES`, RESP3 maps in `CLUSTER SHARDS`, and `MOVED`/`ASK` destinations.
- `READONLY`, `READWRITE` and `ASKING` bind a dedicated backend session. Later ordinary responses still rewrite redirects/topology; stateful connections never return to shared pools.
- Preserve same-node `SET/GET/DEL` pipeline order. Wrong-node requests return `MOVED` toward proxy addresses.
- Pub/Sub, `BLPOP` and blocking `XREAD` use transparent tunnels. Concurrent routed pipelines retain per-connection response order.

Ordinary commands do not aggregate slots inside node proxy; the Cluster client chooses them. Parsing is limited to necessary topology/redirect rewriting, tunnel detection and request budgets. One-request/one-response session controls use frame-aware dedicated connections. Subscriptions, replication streams, other session commands and unknown operations use transparent semantics. Transitioning an already-bound session to a tunnel retains its connection and capacity permit.

Kvrocks `CLUSTER IMPORT` uses a dedicated transparent connection so import privileges never enter a shared pool. Only identity/configuration-version `CLUSTERX` queries use ordinary response handling; migration and other controls retain transparent-stream semantics.
