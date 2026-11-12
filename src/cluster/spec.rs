use anyhow::{bail, Context};

use crate::cluster::slot::redis_slot;
use crate::resp;

/// 业务作用: 表示单条 Redis 命令在 Cluster 代理里的执行策略，所有数据面分支都从这里收敛。
/// 分支说明: `Keyless` 处理本地、默认节点或全节点命令；`SingleSlot` 走单 master；`SplitKeys` 拆分并聚合；`Unsupported` fail-closed。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutePlan {
    /// 命令不依赖具体 key 槽位，使用显式无 key 策略。
    Keyless(KeylessPolicy),
    /// 命令涉及的全部 key 位于同一个槽位。
    SingleSlot {
        /// 应接收整条命令的 Redis Cluster 槽位。
        slot: u16,
    },
    /// 命令可按 key 拆分到多个槽位并聚合结果。
    SplitKeys {
        /// 与命令中各 key 顺序对应的槽位列表。
        slots: Vec<u16>,
        /// 子请求完成后的响应聚合方式。
        aggregate: AggregatePolicy,
    },
    /// 代理无法安全表达该命令的集群语义，必须关闭式拒绝。
    Unsupported {
        /// 返回客户端并记录日志的具体拒绝原因。
        reason: String,
    },
}

/// 业务作用: 描述没有 Redis key 或不能从 key 计算 slot 的命令应该落到哪里执行。
/// 分支说明: `Local` 由代理直接响应；`DefaultNode` 只发一个后端节点；`AllNodes` fan-out 到所有 master 后聚合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeylessPolicy {
    /// 完全由代理本地生成响应，不访问后端节点。
    Local,
    /// 只发送到槽位表中的一个稳定默认主节点。
    DefaultNode,
    /// 发送到全部当前主节点并聚合各节点响应。
    AllNodes,
}

/// 业务作用: 描述跨节点拆分命令的回包合并方式，避免每个命令在路由器里散落自定义聚合逻辑。
/// 分支说明: `SumIntegers` 汇总删除/存在数量；`BulkArrayInOrder` 保持 MGET 这类按 key 顺序返回的数组位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatePolicy {
    /// 把所有子请求的整数响应求和。
    SumIntegers,
    /// 按原 key 顺序拼接各子请求的 bulk array 元素。
    BulkArrayInOrder,
}

/// 业务作用: 根据 Redis 命令语义生成 Cluster 路由计划，避免未知命令被错误地按第一个参数当 key 路由。
/// 参数说明:
/// - `args`: Redis 命令参数列表，首个元素是命令名，后续参数按原始客户端顺序保留。
///
/// 返回: 返回明确的路由计划；参数错误或不支持的连接绑定语义会转成可返回客户端的失败计划。
// 计算 route_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
pub fn route_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.is_empty() {
        return Ok(RoutePlan::Unsupported {
            reason: "empty command".to_string(),
        });
    }
    if is_hot_single_key_command(args[0].as_ref()) {
        return single_key_plan(args);
    }
    if eq_ignore_ascii_case(args[0].as_ref(), b"MGET") {
        return split_bulk_array_or_single_slot(args, 1);
    }
    if eq_ignore_ascii_case(args[0].as_ref(), b"MSET")
        || eq_ignore_ascii_case(args[0].as_ref(), b"MSETNX")
    {
        return mset_plan(args);
    }
    let name = resp::command_name(args);
    match name.as_str() {
        name if name.starts_with("FT.") => Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode)),
        name if name.starts_with("JSON.") => json_plan(name, args),
        "PING" | "QUIT" | "COMMAND" | "CLIENT" | "INFO" => {
            Ok(RoutePlan::Keyless(KeylessPolicy::Local))
        }
        "AUTH" | "HELLO" => Ok(RoutePlan::Keyless(KeylessPolicy::Local)),
        "SELECT" => select_plan(args),
        "DBSIZE" | "KEYS" | "SCAN" => Ok(RoutePlan::Keyless(KeylessPolicy::AllNodes)),
        // 原生游标自身承担路由键，包括初始字符串 0；保留哈希标签及尾部参数，让后端校验扫描范围和游标归属。
        "CLUSTERSCAN" => single_key_at(args, 1, "CLUSTERSCAN requires a cursor"),
        "SCRIPT" => script_plan(args),
        "STRALGO" => stralgo_plan(args),
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO" => {
            eval_family_plan(args)
        }
        "MGET" => split_bulk_array_or_single_slot(args, 1),
        "SUNION" | "SINTER" | "SDIFF" => same_slot_multi(args, 1),
        "MSET" | "MSETNX" => mset_plan(args),
        "DEL" | "EXPDEL" | "UNLINK" | "EXISTS" | "TOUCH" => split_sum(args, 1),
        "KEYDB.MEXISTS" => same_slot_multi(args, 1),
        "PFSELFTEST" => Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode)),
        "BLPOP" | "BRPOP" | "BRPOPLPUSH" | "BZPOPMIN" | "BZPOPMAX" | "BLMOVE" | "BLMPOP"
        | "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" | "UNSUBSCRIBE" | "PUNSUBSCRIBE"
        | "SUNSUBSCRIBE" | "PUBLISH" | "SPUBLISH" | "PUBSUB" | "MULTI" | "EXEC" | "DISCARD"
        | "WATCH" | "UNWATCH" | "WAIT" | "WAITAOF" | "LFENCE" | "RESET" | "RREPLAY" => {
            Ok(RoutePlan::Unsupported {
                reason: format!("{name} requires connection-bound cluster semantics"),
            })
        }
        _ => registered_command_plan(&name, args),
    }
}

/// 业务作用: 快速识别常见单 key 命令，让 GET/SET 等热路径直接按第二个参数算 slot。
/// 参数说明:
/// - `name`: 客户端传入的命令名字节，大小写不敏感。
///
/// 返回: 属于明确登记的高频单 key 命令时返回 `true`。
// 判断高频单 key 命令，用于在 SET/GET 等热路径上避免命令名 String 分配。
pub(crate) fn is_hot_single_key_command(name: &[u8]) -> bool {
    matches!(name.len(), 3..=13)
        && [
            b"GET".as_slice(),
            b"SET".as_slice(),
            b"TTL".as_slice(),
            b"PTTL".as_slice(),
            b"TYPE".as_slice(),
            b"HGET".as_slice(),
            b"HSET".as_slice(),
            b"HDEL".as_slice(),
            b"HLEN".as_slice(),
            b"HGETALL".as_slice(),
            b"INCR".as_slice(),
            b"DECR".as_slice(),
            b"INCRBY".as_slice(),
            b"DECRBY".as_slice(),
            b"EXPIRE".as_slice(),
            b"PEXPIRE".as_slice(),
            b"PERSIST".as_slice(),
            b"ZADD".as_slice(),
            b"ZRANGE".as_slice(),
            b"ZSCORE".as_slice(),
            b"ZREM".as_slice(),
            b"XADD".as_slice(),
            b"XLEN".as_slice(),
            b"XRANGE".as_slice(),
        ]
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
}

/// 业务作用: 按 RedisJSON 子命令选择单键、同槽多键、拆分聚合或无键路由策略。
/// 参数说明:
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 json_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn json_plan<S: AsRef<[u8]>>(name: &str, args: &[S]) -> anyhow::Result<RoutePlan> {
    match name {
        "JSON.MGET" => json_mget_plan(args),
        "JSON.MSET" => json_mset_plan(args),
        "JSON.DEBUG" => json_debug_plan(args),
        "JSON.HELP" => Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode)),
        _ => single_key_plan(args),
    }
}

/// 业务作用: 计算 json_mget_plan 路由规则，用于把 JSON.MGET 拆成多条 JSON.GET 并按原始 key 顺序聚合。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 json_mget_plan 路由规则，用于把 JSON.MGET 拆成多条 JSON.GET 并按原始 key 顺序聚合。
fn json_mget_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 3 {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for JSON.MGET".to_string(),
        });
    }
    split_bulk_array_or_single_slot(&args[..args.len() - 1], 1)
}

/// 业务作用: 计算 json_mset_plan 路由规则，用于校验 JSON.MSET triplet 里的所有 key 属于同一 slot。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 json_mset_plan 路由规则，用于校验 JSON.MSET triplet 里的所有 key 属于同一 slot。
fn json_mset_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 4 || !(args.len() - 1).is_multiple_of(3) {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for JSON.MSET".to_string(),
        });
    }
    let mut triplets = args[1..].chunks_exact(3);
    let Some(first) = triplets.next() else {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for JSON.MSET".to_string(),
        });
    };
    let slot = redis_slot(first[0].as_ref());
    if triplets.all(|triplet| redis_slot(triplet[0].as_ref()) == slot) {
        Ok(RoutePlan::SingleSlot { slot })
    } else {
        Ok(RoutePlan::Unsupported {
            reason: "CROSSSLOT keys in request do not hash to the same slot".to_string(),
        })
    }
}

/// 业务作用: 计算 json_debug_plan 路由规则，用于处理 JSON.DEBUG 的 key 位置不是 arg1 的子命令。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 json_debug_plan 路由规则，用于处理 JSON.DEBUG 的 key 位置不是 arg1 的子命令。
fn json_debug_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 2 {
        return Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode));
    }
    if eq_ignore_ascii_case(args[1].as_ref(), b"MEMORY") {
        return single_key_at(args, 2, "JSON.DEBUG MEMORY requires key");
    }
    if eq_ignore_ascii_case(args[1].as_ref(), b"HELP") {
        return Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode));
    }
    Ok(RoutePlan::Unsupported {
        reason: "JSON.DEBUG subcommand is not registered for cluster routing".to_string(),
    })
}

/// 业务作用: 从 Redis 命令规格、读写分类和 cluster 支持策略 的候选集合中选择最安全的目标，避免在关键路径中依赖调用方猜测。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 select_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn select_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() != 2 {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for SELECT".to_string(),
        });
    }
    if args[1].as_ref() == b"0" {
        Ok(RoutePlan::Keyless(KeylessPolicy::Local))
    } else {
        Ok(RoutePlan::Unsupported {
            reason: "Redis Cluster supports database 0 only".to_string(),
        })
    }
}

/// 业务作用: 按 SCRIPT 子命令是否访问键空间决定无键路由或明确拒绝。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 script_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn script_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() >= 2 && eq_ignore_ascii_case(args[1].as_ref(), b"EXISTS") {
        if args.len() < 3 {
            return Ok(RoutePlan::Unsupported {
                reason: "wrong number of arguments for SCRIPT EXISTS".to_string(),
            });
        }
        return Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode));
    }
    if args.len() >= 2 && eq_ignore_ascii_case(args[1].as_ref(), b"LOAD") {
        if args.len() != 3 {
            return Ok(RoutePlan::Unsupported {
                reason: "wrong number of arguments for SCRIPT LOAD".to_string(),
            });
        }
        return Ok(RoutePlan::Keyless(KeylessPolicy::AllNodes));
    }
    Ok(RoutePlan::Unsupported {
        reason: "SCRIPT subcommand is not registered for cluster routing".to_string(),
    })
}

/// 业务作用: 解析 EVAL/EVALSHA 的 NUMKEYS，并要求声明键属于同一 slot。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 eval_family_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn eval_family_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    let name = resp::command_name(args);
    let numkeys_idx = match name.as_str() {
        "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" => 2,
        "FCALL" | "FCALL_RO" => 2,
        _ => unreachable!(),
    };
    let numkeys = parse_numkeys(args, numkeys_idx)?;
    if numkeys == 0 {
        return Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode));
    }
    let first_key = numkeys_idx + 1;
    let Some(key_end) = checked_key_end(args.len(), first_key, numkeys) else {
        return Ok(RoutePlan::Unsupported {
            reason: format!("{name} declares {numkeys} keys but args are missing"),
        });
    };
    same_slot_keys(&args[first_key..key_end])
}

/// 业务作用: 按 KeyDB 6.3.4 的 STRALGO LCS 参数区分无键字符串模式和双键模式。
/// 参数说明:
/// - `args`: 完整 STRALGO 命令参数，用于查找 STRINGS 或 KEYS 子句。
fn stralgo_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    for idx in 1..args.len() {
        if eq_ignore_ascii_case(args[idx].as_ref(), b"STRINGS") {
            return Ok(RoutePlan::Keyless(KeylessPolicy::DefaultNode));
        }
        if eq_ignore_ascii_case(args[idx].as_ref(), b"KEYS") {
            return same_slot_pair(args, idx + 1, idx + 2);
        }
    }
    Ok(RoutePlan::Unsupported {
        reason: "STRALGO requires STRINGS or KEYS".to_string(),
    })
}

/// 业务作用: 根据命令规格注册表的 key 位置和聚合规则生成路由计划。
/// 参数说明:
/// - `name`: 当前代理、模式或运行任务名称，用于唯一标识配置、日志和指标。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 registered_command_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn registered_command_plan<S: AsRef<[u8]>>(name: &str, args: &[S]) -> anyhow::Result<RoutePlan> {
    match name {
        // Garnet ETag 操作只作用于第一个 key，比较值和过期参数不能参与槽位计算。
        "GETWITHETAG" | "GETIFNOTMATCH" | "SETWITHETAG" | "SETIFMATCH" | "SETIFGREATER"
        | "DELIFGREATER" => single_key_plan(args),
        // String / generic key commands.
        "GET" | "GETDEL" | "GETEX" | "GETRANGE" | "GETSET" | "SET" | "SETEX" | "PSETEX"
        | "SETNX" | "SETRANGE" | "STRLEN" | "APPEND" | "INCR" | "INCRBY" | "INCRBYFLOAT"
        | "DECR" | "DECRBY" | "GETBIT" | "SETBIT" | "BITCOUNT" | "BITFIELD" | "BITFIELD_RO"
        | "BITPOS" | "TYPE" | "TTL" | "PTTL" | "PERSIST" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
        | "PEXPIREAT" | "EXPIRETIME" | "PEXPIRETIME" | "DUMP" | "RESTORE" | "RESTORE-ASKING"
        | "EXPIREMEMBER" | "EXPIREMEMBERAT" | "PEXPIREMEMBERAT" | "KEYDB.CRON"
        | "KEYDB.HRENAME" | "KEYDB.MVCCRESTORE" | "KEYDB.NHGET" | "KEYDB.NHSET" => {
            single_key_plan(args)
        }

        // Hash commands.
        "HDEL" | "HEXISTS" | "HGET" | "HGETALL" | "HINCRBY" | "HINCRBYFLOAT" | "HKEYS" | "HLEN"
        | "HMGET" | "HMSET" | "HRANDFIELD" | "HSCAN" | "HSET" | "HSETNX" | "HSTRLEN" | "HVALS" => {
            single_key_plan(args)
        }

        // List commands.
        "LINDEX" | "LINSERT" | "LLEN" | "LMOVE" | "LPOP" | "LPOS" | "LPUSH" | "LPUSHX"
        | "LRANGE" | "LREM" | "LSET" | "LTRIM" | "RPOP" | "RPOPLPUSH" | "RPUSH" | "RPUSHX" => {
            match name {
                "LMOVE" | "RPOPLPUSH" => same_slot_pair(args, 1, 2),
                _ => single_key_plan(args),
            }
        }

        // Set commands.
        "SADD" | "SCARD" | "SISMEMBER" | "SMEMBERS" | "SMISMEMBER" | "SPOP" | "SRANDMEMBER"
        | "SREM" | "SSCAN" => single_key_plan(args),
        "SDIFFSTORE" | "SINTERSTORE" | "SUNIONSTORE" => same_slot_multi(args, 1),
        "SMOVE" => same_slot_pair(args, 1, 2),

        // Sorted-set commands. Redis 6.2+ ZRANGE variants are key-first and pass through unchanged.
        "ZADD" | "ZCARD" | "ZCOUNT" | "ZINCRBY" | "ZLEXCOUNT" | "ZMSCORE" | "ZPOPMAX"
        | "ZPOPMIN" | "ZRANDMEMBER" | "ZRANGE" | "ZRANGEBYLEX" | "ZRANGEBYSCORE" | "ZRANK"
        | "ZREM" | "ZREMRANGEBYLEX" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZREVRANGE"
        | "ZREVRANGEBYLEX" | "ZREVRANGEBYSCORE" | "ZREVRANK" | "ZSCAN" | "ZSCORE" => {
            single_key_plan(args)
        }
        "ZRANGESTORE" => same_slot_pair(args, 1, 2),
        "ZDIFF" | "ZINTER" | "ZUNION" | "ZINTERCARD" => counted_same_slot_keys(args, 1, 2),
        "ZDIFFSTORE" | "ZINTERSTORE" | "ZUNIONSTORE" => counted_same_slot_keys(args, 2, 3),

        // Stream commands with a single stream key.
        "XACK" | "XADD" | "XAUTOCLAIM" | "XCLAIM" | "XDEL" | "XLEN" | "XPENDING" | "XRANGE"
        | "XREVRANGE" | "XTRIM" => single_key_plan(args),
        "XREAD" | "XREADGROUP" => xread_plan(args),
        "XGROUP" => xgroup_plan(args),
        "XINFO" => xinfo_plan(args),

        // HyperLogLog and geo commands.
        "PFADD" => single_key_plan(args),
        "PFDEBUG" => single_key_at(args, 2, "PFDEBUG requires a key"),
        "PFCOUNT" => same_slot_multi(args, 1),
        "PFMERGE" => same_slot_multi(args, 1),
        "GEOADD" | "GEODIST" | "GEOHASH" | "GEOPOS" | "GEORADIUS" | "GEORADIUSBYMEMBER"
        | "GEOSEARCH" => single_key_plan(args),
        "GEOSEARCHSTORE" => same_slot_pair(args, 1, 2),

        // Two-key or destination-plus-source commands.
        "COPY" | "RENAME" | "RENAMENX" => same_slot_pair(args, 1, 2),
        "BITOP" => same_slot_multi(args, 2),

        _ => Ok(RoutePlan::Unsupported {
            reason: format!("command {name} is not registered for cluster routing"),
        }),
    }
}

/// 业务作用: 把第一个参数作为唯一路由键，缺失时返回明确命令错误。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 single_key_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn single_key_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    single_key_at(args, 1, "command has no registered key position")
}

/// 业务作用: 计算 single_key_at 路由规则，用于支持 key 不在 arg1 的命令。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `key_idx`: 命令中当前 key 的序号，用于槽位计算和聚合结果保序。
/// - `missing_reason`: 错误或原因文本，用于保护态、日志和失败上下文。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 single_key_at 路由规则，用于支持 key 不在 arg1 的命令。
fn single_key_at<S: AsRef<[u8]>>(
    args: &[S],
    key_idx: usize,
    missing_reason: &str,
) -> anyhow::Result<RoutePlan> {
    if args.len() < 2 {
        return Ok(RoutePlan::Unsupported {
            reason: missing_reason.to_string(),
        });
    }
    let Some(key) = args.get(key_idx) else {
        return Ok(RoutePlan::Unsupported {
            reason: missing_reason.to_string(),
        });
    };
    Ok(RoutePlan::SingleSlot {
        slot: redis_slot(key.as_ref()),
    })
}

/// 业务作用: 提取两个指定位置的键并验证它们属于同一 slot。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
/// - `second_key`: 多键命令中的第二个 key，用于构造同槽位或跨槽位场景。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 same_slot_pair 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn same_slot_pair<S: AsRef<[u8]>>(
    args: &[S],
    first_key: usize,
    second_key: usize,
) -> anyhow::Result<RoutePlan> {
    if args.len() <= second_key {
        return Ok(RoutePlan::Unsupported {
            reason: "two-key command requires both keys".to_string(),
        });
    }
    same_slot_keys(&args[first_key..=second_key])
}

/// 业务作用: 按 NUMKEYS 声明提取连续键集合并验证同槽与参数边界。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `numkeys_idx`: 命令中 numkeys 参数的位置，用于从不同脚本命令格式提取 key。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 counted_same_slot_keys 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn counted_same_slot_keys<S: AsRef<[u8]>>(
    args: &[S],
    numkeys_idx: usize,
    first_key: usize,
) -> anyhow::Result<RoutePlan> {
    let numkeys = parse_numkeys(args, numkeys_idx)?;
    if numkeys == 0 {
        return Ok(RoutePlan::Unsupported {
            reason: "counted multi-key command requires at least one key".to_string(),
        });
    }
    let Some(key_end) = checked_key_end(args.len(), first_key, numkeys) else {
        return Ok(RoutePlan::Unsupported {
            reason: format!(
                "counted multi-key command declares {numkeys} keys but args are missing"
            ),
        });
    };
    same_slot_keys(&args[first_key..key_end])
}

/// 业务作用: 校验起点和键数量不会越过命令参数边界，并返回键区间终点。
/// 参数说明:
/// - `args_len`: 命令参数总数，用于在读取指定位置前完成数量边界校验。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
/// - `numkeys`: 脚本或函数命令声明的 key 数量，用于确定真实 key 参数边界。
///
fn checked_key_end(args_len: usize, first_key: usize, numkeys: usize) -> Option<usize> {
    let available = args_len.checked_sub(first_key)?;
    if numkeys > available {
        return None;
    }
    first_key.checked_add(numkeys)
}

/// 业务作用: 按 XGROUP 子命令定位 stream key，兼容 HELP 等无键分支。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 xgroup_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn xgroup_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 3 {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for XGROUP".to_string(),
        });
    }
    match String::from_utf8_lossy(args[1].as_ref())
        .to_ascii_uppercase()
        .as_str()
    {
        "CREATE" | "SETID" | "DESTROY" | "CREATECONSUMER" | "DELCONSUMER" => {
            Ok(RoutePlan::SingleSlot {
                slot: redis_slot(args[2].as_ref()),
            })
        }
        _ => Ok(RoutePlan::Unsupported {
            reason: "XGROUP subcommand is not registered for cluster routing".to_string(),
        }),
    }
}

/// 业务作用: 按 XINFO 子命令定位 stream key，兼容 HELP 等无键分支。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 xinfo_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn xinfo_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 3 {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for XINFO".to_string(),
        });
    }
    match String::from_utf8_lossy(args[1].as_ref())
        .to_ascii_uppercase()
        .as_str()
    {
        "STREAM" | "GROUPS" | "CONSUMERS" => Ok(RoutePlan::SingleSlot {
            slot: redis_slot(args[2].as_ref()),
        }),
        _ => Ok(RoutePlan::Unsupported {
            reason: "XINFO subcommand is not registered for cluster routing".to_string(),
        }),
    }
}

/// 业务作用: 解析 STREAMS 分隔符和键数量，要求 XREAD/XREADGROUP 的全部 stream 同槽。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 xread_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn xread_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    let name = resp::command_name(args);
    // 只在 options 区（STREAMS 关键字之前）按位置解析 BLOCK/STREAMS，
    // 否则会把名为 "BLOCK"/"STREAMS" 的 key / group / consumer 误判为选项。
    // XREADGROUP 语法: XREADGROUP GROUP <group> <consumer> [COUNT n] [BLOCK ms] [NOACK] STREAMS key... id...
    // XREAD 语法:      XREAD [COUNT n] [BLOCK ms] STREAMS key... id...
    let is_group = name == "XREADGROUP";
    let mut idx = 1usize;
    if is_group {
        if args.len() < 4 || !eq_ignore_ascii_case(args[1].as_ref(), b"GROUP") {
            return Ok(RoutePlan::Unsupported {
                reason: format!("{name} requires GROUP <group> <consumer>"),
            });
        }
        idx = 4; // 跳过 GROUP <group> <consumer>，其中 <group>/<consumer> 是任意用户字符串
    }
    let mut streams_idx = None;
    while idx < args.len() {
        let token = args[idx].as_ref();
        if eq_ignore_ascii_case(token, b"STREAMS") {
            streams_idx = Some(idx);
            break;
        }
        if eq_ignore_ascii_case(token, b"BLOCK") {
            return Ok(RoutePlan::Unsupported {
                reason: format!("{name} BLOCK requires connection-bound cluster semantics"),
            });
        }
        if eq_ignore_ascii_case(token, b"COUNT") {
            idx += 2; // COUNT <count>
            continue;
        }
        if is_group && eq_ignore_ascii_case(token, b"NOACK") {
            idx += 1;
            continue;
        }
        // options 区出现无法识别的 token：与真实 Redis 一致按 syntax error 处理（fail-closed，绝不误路由）。
        return Ok(RoutePlan::Unsupported {
            reason: format!("{name} has an unexpected token before STREAMS"),
        });
    }
    let Some(streams_idx) = streams_idx else {
        return Ok(RoutePlan::Unsupported {
            reason: format!("{name} requires STREAMS section"),
        });
    };
    let remaining = args.len().saturating_sub(streams_idx + 1);
    if remaining < 2 || !remaining.is_multiple_of(2) {
        return Ok(RoutePlan::Unsupported {
            reason: format!("wrong number of arguments for {name} STREAMS"),
        });
    }
    let key_count = remaining / 2;
    same_slot_keys(&args[streams_idx + 1..streams_idx + 1 + key_count])
}

/// 业务作用: 从指定起点提取全部键并要求同槽执行。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 same_slot_multi 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn same_slot_multi<S: AsRef<[u8]>>(args: &[S], first_key: usize) -> anyhow::Result<RoutePlan> {
    if args.len() <= first_key {
        return Ok(RoutePlan::Unsupported {
            reason: "multi-key command requires at least one key".to_string(),
        });
    }
    same_slot_keys(&args[first_key..])
}

/// 业务作用: 为可跨槽计数命令生成按节点拆分、最终整数求和的路由计划。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 解析 split_sum 输入，用于支撑根据 Redis 命令语义生成 Cluster 路由策略。
fn split_sum<S: AsRef<[u8]>>(args: &[S], first_key: usize) -> anyhow::Result<RoutePlan> {
    if args.len() <= first_key {
        return Ok(RoutePlan::Unsupported {
            reason: "split command requires at least one key".to_string(),
        });
    }
    Ok(RoutePlan::SplitKeys {
        slots: args[first_key..]
            .iter()
            .map(|k| redis_slot(k.as_ref()))
            .collect(),
        aggregate: AggregatePolicy::SumIntegers,
    })
}

/// 业务作用: 为可跨槽批量读取生成按节点拆分、按原 key 顺序合并数组的计划。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 解析 split_bulk_array 输入，用于支撑根据 Redis 命令语义生成 Cluster 路由策略。
fn split_bulk_array<S: AsRef<[u8]>>(args: &[S], first_key: usize) -> anyhow::Result<RoutePlan> {
    if args.len() <= first_key {
        return Ok(RoutePlan::Unsupported {
            reason: "split command requires at least one key".to_string(),
        });
    }
    Ok(RoutePlan::SplitKeys {
        slots: args[first_key..]
            .iter()
            .map(|k| redis_slot(k.as_ref()))
            .collect(),
        aggregate: AggregatePolicy::BulkArrayInOrder,
    })
}

/// 业务作用: 根据键是否同槽选择单节点执行或跨槽拆分后按数组顺序聚合。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `first_key`: 多键命令中的第一个 key，用于建立基准槽位或结果顺序。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
fn split_bulk_array_or_single_slot<S: AsRef<[u8]>>(
    args: &[S],
    first_key: usize,
) -> anyhow::Result<RoutePlan> {
    if args.len() <= first_key {
        return Ok(RoutePlan::Unsupported {
            reason: "split command requires at least one key".to_string(),
        });
    }
    let first_slot = redis_slot(args[first_key].as_ref());
    if args[first_key + 1..]
        .iter()
        .all(|key| redis_slot(key.as_ref()) == first_slot)
    {
        Ok(RoutePlan::SingleSlot { slot: first_slot })
    } else {
        split_bulk_array(args, first_key)
    }
}

/// 业务作用: 校验 MSET 键值对完整，并要求所有键在同一 slot 后单节点执行。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 mset_plan 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn mset_plan<S: AsRef<[u8]>>(args: &[S]) -> anyhow::Result<RoutePlan> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for MSET/MSETNX".to_string(),
        });
    }
    let mut pairs = args[1..].chunks_exact(2);
    let Some(first) = pairs.next() else {
        return Ok(RoutePlan::Unsupported {
            reason: "wrong number of arguments for MSET/MSETNX".to_string(),
        });
    };
    let slot = redis_slot(first[0].as_ref());
    if pairs.all(|pair| redis_slot(pair[0].as_ref()) == slot) {
        Ok(RoutePlan::SingleSlot { slot })
    } else {
        Ok(RoutePlan::Unsupported {
            reason: "CROSSSLOT keys in request do not hash to the same slot".to_string(),
        })
    }
}

/// 业务作用: 计算键集合 slot 并返回单节点计划，空集合或跨槽时拒绝。
/// 参数说明:
/// - `keys`: 本条命令提取出的 key，供槽位一致性校验使用。
///
/// 返回: 参数满足命令规格时返回保序路由计划；缺参、跨槽违规或命令不受支持时返回错误。
// 计算 same_slot_keys 路由规则，用于决定根据 Redis 命令语义生成 Cluster 路由策略的执行路径。
fn same_slot_keys<S: AsRef<[u8]>>(keys: &[S]) -> anyhow::Result<RoutePlan> {
    let first = keys.first().context("same slot requires key")?;
    let slot = redis_slot(first.as_ref());
    if keys.iter().all(|k| redis_slot(k.as_ref()) == slot) {
        Ok(RoutePlan::SingleSlot { slot })
    } else {
        Ok(RoutePlan::Unsupported {
            reason: "CROSSSLOT keys in request do not hash to the same slot".to_string(),
        })
    }
}

/// 业务作用: 解析 Redis 命令规格、读写分类和 cluster 支持策略 需要的输入文本或协议帧，把外部表示转换成可校验的内部结构。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `idx`: 当前元素或参数下标，用于在保持原始顺序的同时定位处理位置。
///
/// 返回: 输入可安全转换且未越界时返回数量；缺失、格式非法或溢出时返回错误。
// 解析 parse_numkeys 输入，用于支撑根据 Redis 命令语义生成 Cluster 路由策略。
fn parse_numkeys<S: AsRef<[u8]>>(args: &[S], idx: usize) -> anyhow::Result<usize> {
    let Some(raw) = args.get(idx) else {
        bail!("missing numkeys");
    };
    let s = std::str::from_utf8(raw.as_ref()).context("numkeys is not utf8")?;
    s.parse().context("numkeys is not integer")
}

/// 业务作用: 对命令参数执行无分配的 ASCII 大小写无关比较。
/// 参数说明:
/// - `left`: 比较操作左侧的实际或候选值，与右侧共同完成一致性判断。
/// - `right`: 比较操作右侧的期望或参考值，与左侧共同完成一致性判断。
///
fn eq_ignore_ascii_case(left: &[u8], right: &[u8]) -> bool {
    left.eq_ignore_ascii_case(right)
}
