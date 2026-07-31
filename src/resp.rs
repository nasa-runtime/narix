use std::ops::Range;

use anyhow::{bail, Context};

// RESP 聚合值来自后端，不能信任其嵌套层数。限制为 128 层既远高于正常 Redis
// 响应所需深度，又能保证递归解析不会被异常后端响应拖到线程栈溢出。
const MAX_RESP_NESTING_DEPTH: usize = 128;

/// 业务作用: 表示从前端缓冲区解析出的单条 Redis 命令，供需要拥有参数内容的路由路径使用。
/// 字段说明: `args` 保留命令参数顺序；`consumed` 标记本条命令占用的字节数，调用方用它切分 pipeline 剩余数据。
#[derive(Debug, Clone)]
// 表示 ParsedCommand 的数据结构，用于解析和编码 Redis RESP 协议帧。
pub struct ParsedCommand {
    /// 按客户端原始顺序拥有每个命令参数的字节内容。
    pub args: Vec<Vec<u8>>,
    /// 当前完整命令在输入缓冲区中占用的字节数。
    pub consumed: usize,
}

/// 业务作用: 表示单条 Redis 命令参数在原始缓冲区中的区间，供热路径少拷贝读取参数。
/// 字段说明: `arg_ranges` 指向原始帧内每个参数的字节区间；`consumed` 用于定位下一条 pipeline 命令。
#[derive(Debug, Clone)]
// 表示 ParsedCommandRanges 的数据结构，用于解析和编码 Redis RESP 协议帧。
pub struct ParsedCommandRanges {
    /// 每个命令参数在原始输入帧中的半开字节范围。
    pub arg_ranges: Vec<Range<usize>>,
    /// 当前完整命令在输入缓冲区中占用的字节数。
    pub consumed: usize,
}

/// 业务作用: 表示需要被理解或改写的 RESP 值，主要用于 Cluster 拓扑响应、管理接口测试和聚合命令结果。
/// 分支说明: 分支覆盖 RESP2 常见类型和代理需要处理的 RESP3 map/set/push/null，未知类型在解析阶段 fail-fast。
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum RespValue {
    /// RESP 简单字符串内容，不包含类型前缀和行结束符。
    Simple(Vec<u8>),
    /// RESP 简单错误内容，不包含类型前缀和行结束符。
    Error(Vec<u8>),
    /// RESP 有符号整数。
    Integer(i64),
    /// RESP bulk string；`None` 表示空 bulk 值。
    Bulk(Option<Vec<u8>>),
    /// RESP array；`None` 表示空 array 值。
    Array(Option<Vec<RespValue>>),
    /// RESP3 map；`None` 表示空 map 值。
    Map(Option<Vec<(RespValue, RespValue)>>),
    /// RESP3 set；`None` 表示空 set 值。
    Set(Option<Vec<RespValue>>),
    /// RESP3 push 消息中的有序元素。
    Push(Vec<RespValue>),
    /// RESP3 独立 null 值。
    Null,
}

/// 业务作用: 从前端缓冲区解析第一条 Redis 命令，支持 RESP array 和 inline command 两种客户端输入形态。
/// 参数说明:
/// - `input`: 前端连接当前已读取但尚未消费的字节缓冲。
///
/// 返回: 完整命令返回参数和消费长度；半包返回 `None`；协议错误返回错误。
// 解析 parse_command 输入，用于支撑解析和编码 Redis RESP 协议帧。
pub fn parse_command(input: &[u8]) -> anyhow::Result<Option<ParsedCommand>> {
    if input.is_empty() {
        return Ok(None);
    }
    match input[0] {
        b'*' => parse_array(input),
        _ => parse_inline(input),
    }
}

/// 业务作用: 解析第一条 Redis 命令但只返回参数区间，避免在高频 SET/GET 和 pipeline 路径复制参数内容。
/// 参数说明:
/// - `input`: 前端连接当前已读取但尚未消费的字节缓冲，返回区间都指向这段缓冲。
///
/// 返回: 完整命令返回参数区间和消费长度；半包返回 `None`；协议错误返回错误。
// 解析 parse_command_ranges 输入，用于支撑解析和编码 Redis RESP 协议帧。
pub fn parse_command_ranges(input: &[u8]) -> anyhow::Result<Option<ParsedCommandRanges>> {
    if input.is_empty() {
        return Ok(None);
    }
    match input[0] {
        b'*' => parse_array_ranges(input),
        _ => parse_inline_ranges(input),
    }
}

/// 业务作用: 计算缓冲区开头一条完整 RESP frame 的字节长度，用于 pipeline 切帧和后端响应读取。
/// 参数说明:
/// - `input`: 以 RESP 类型字节开头的未消费缓冲。
///
/// 返回: 完整帧返回长度；半包返回 `None`；未知类型或长度溢出返回错误。
pub fn frame_len(input: &[u8]) -> anyhow::Result<Option<usize>> {
    frame_len_with_depth(input, MAX_RESP_NESTING_DEPTH)
}

/// 业务作用: 在剩余嵌套预算内计算一条 RESP 帧长度，使不可信后端不能用深层聚合帧耗尽线程栈。
/// 参数说明:
/// - `input`: 以 RESP 类型字节开头的未消费缓冲。
/// - `remaining_depth`: 当前值及其子值还允许进入的聚合层数；标量值不消耗预算。
///
/// 返回: 完整帧返回长度；半包返回 `None`；协议非法、长度溢出或嵌套过深返回错误。
fn frame_len_with_depth(input: &[u8], remaining_depth: usize) -> anyhow::Result<Option<usize>> {
    if input.is_empty() {
        return Ok(None);
    }
    match input[0] {
        // RESP2/3 单行帧：简单字符串、错误、整数、双精度数、空值、布尔值和大整数。
        b'+' | b'-' | b':' | b',' | b'_' | b'#' | b'(' => {
            Ok(find_crlf(input, 0).map(|end| end + 2))
        }
        // 带长度前缀的字节帧：bulk string、verbatim string 和 blob error。
        b'$' | b'=' | b'!' => bulk_frame_len(input),
        // 聚合帧：array、map、set 和 push。
        b'*' | b'%' | b'~' | b'>' => {
            aggregate_frame_len(input, descend_resp_depth(remaining_depth)?)
        }
        // RESP3 attribute 是位于下一条回复之前的 map 型元数据块。
        b'|' => attribute_frame_len(input, descend_resp_depth(remaining_depth)?),
        other => bail!("unknown RESP frame type: {:#04x}", other),
    }
}

/// 业务作用: 计算 RESP3 attribute 及其后被注解真实回复的整体长度，避免把 attribute 和实际回复拆成两条响应。
/// 参数说明:
/// - `input`: 以 `|` 开头的 RESP3 attribute 缓冲。
/// - `remaining_depth`: attribute 子值和被注解值可继续使用的嵌套预算。
///
/// 返回: attribute 加真实回复的完整长度；任一部分半包时返回 `None`。
// 计算 RESP3 attribute 帧长度：attribute 是附着在下一条回复前的 map，
// 完整帧长度 = attribute map 本身 + 其后被注解的那条真实回复。
fn attribute_frame_len(input: &[u8], remaining_depth: usize) -> anyhow::Result<Option<usize>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid attribute length")?;
    if count < 0 {
        bail!("invalid RESP attribute count");
    }
    let elements = (count as usize)
        .checked_mul(2)
        .context("attribute element count overflow")?;
    for _ in 0..elements {
        let Some(len) = frame_len_with_depth(&input[pos..], remaining_depth)? else {
            return Ok(None);
        };
        pos = checked_add(pos, len, "attribute frame length overflow")?;
    }
    // attribute 之后紧跟它所注解的真实回复，一并计入当前帧长度。
    let Some(len) = frame_len_with_depth(&input[pos..], remaining_depth)? else {
        return Ok(None);
    };
    pos = checked_add(pos, len, "attribute frame length overflow")?;
    Ok(Some(pos))
}

/// 业务作用: 把一条完整 RESP 值解析成内部枚举，供需要理解响应结构的拓扑改写和测试断言使用。
/// 参数说明:
/// - `input`: 以 RESP 类型字节开头的未消费缓冲。
///
/// 返回: 完整值返回枚举和消费长度；半包返回 `None`；不支持类型返回错误。
#[allow(dead_code)]
// 解析 parse_value 输入，用于支撑解析和编码 Redis RESP 协议帧。
pub fn parse_value(input: &[u8]) -> anyhow::Result<Option<(RespValue, usize)>> {
    parse_value_with_depth(input, MAX_RESP_NESTING_DEPTH)
}

/// 业务作用: 在剩余嵌套预算内解析 RESP 值，保护拓扑改写和聚合响应路径免受深层递归攻击。
/// 参数说明:
/// - `input`: 以 RESP 类型字节开头的未消费缓冲。
/// - `remaining_depth`: 当前值及其子值还允许进入的聚合层数；标量值不消耗预算。
///
/// 返回: 完整值返回枚举和消费长度；半包返回 `None`；协议非法或嵌套过深返回错误。
fn parse_value_with_depth(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    if input.is_empty() {
        return Ok(None);
    }
    match input[0] {
        b'+' => parse_line_value(input, RespValue::Simple),
        b'-' => parse_line_value(input, RespValue::Error),
        b':' => {
            let Some((line, consumed)) = read_line(input, 1)? else {
                return Ok(None);
            };
            let value: i64 = std::str::from_utf8(line)?.parse()?;
            Ok(Some((RespValue::Integer(value), consumed)))
        }
        b'$' => parse_bulk_value(input),
        b'*' => parse_array_value(input, descend_resp_depth(remaining_depth)?),
        b'%' => parse_map_value(input, descend_resp_depth(remaining_depth)?),
        b'~' => parse_set_value(input, descend_resp_depth(remaining_depth)?),
        b'>' => parse_push_value(input, descend_resp_depth(remaining_depth)?),
        b'_' => parse_null_value(input),
        // RESP3 标量扩展。
        b',' | b'(' => parse_line_value(input, RespValue::Simple), // double / big number: keep textual form
        b'#' => parse_boolean_value(input),
        b'=' => parse_bulk_value(input), // verbatim string: framed like bulk, keep body verbatim
        b'!' => parse_blob_error_value(input),
        b'|' => parse_attribute_value(input, descend_resp_depth(remaining_depth)?),
        other => bail!("unsupported RESP value type: {:#04x}", other),
    }
}

/// 业务作用: 解析 RESP3 boolean，并映射成内部整数表示，简化后续聚合和断言逻辑。
/// 参数说明:
/// - `input`: 以 `#` 开头的 RESP3 boolean 缓冲。
///
/// 返回: 完整合法的 RESP3 boolean 返回枚举和消费长度；半包返回空，非法字面量返回错误。
// 解析 RESP3 boolean（`#t` / `#f`），映射为 Integer(1) / Integer(0)。
fn parse_boolean_value(input: &[u8]) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, consumed)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let value = match line {
        b"t" => 1,
        b"f" => 0,
        other => bail!("invalid RESP boolean payload: {other:?}"),
    };
    Ok(Some((RespValue::Integer(value), consumed)))
}

/// 业务作用: 解析 RESP3 blob error，并映射成内部错误值，保证错误响应仍能参与统一回包处理。
/// 参数说明:
/// - `input`: 以 `!` 开头的 RESP3 blob error 缓冲。
///
/// 返回: 完整 blob error 返回错误枚举和消费长度；半包返回空，长度或终止符非法时返回错误。
// 解析 RESP3 blob error（`!<len>\r\n<bytes>\r\n`），映射为 Error。
fn parse_blob_error_value(input: &[u8]) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, after_len)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let len = parse_isize(line).context("invalid blob error length")?;
    if len < 0 {
        return Ok(Some((RespValue::Error(Vec::new()), after_len)));
    }
    let body_end = checked_add(after_len, len as usize, "blob error frame length overflow")?;
    let end = checked_add(body_end, 2, "blob error frame length overflow")?;
    if input.len() < end {
        return Ok(None);
    }
    if &input[end - 2..end] != b"\r\n" {
        bail!("invalid blob error frame terminator");
    }
    Ok(Some((
        RespValue::Error(input[after_len..end - 2].to_vec()),
        end,
    )))
}

/// 业务作用: 解析 RESP3 attribute，丢弃元数据并返回被注解的真实值，使上层继续按普通响应处理。
/// 参数说明:
/// - `input`: 以 `|` 开头的 RESP3 attribute 缓冲。
/// - `remaining_depth`: attribute 子值和被注解值可继续使用的嵌套预算。
///
/// 返回: 完整 attribute 与其后真实值返回组合结果和消费长度；半包返回空，嵌套或结构非法时返回错误。
// 解析 RESP3 attribute（`|<count>\r\n` + count 对 k/v + 被注解的值），返回其后被注解的那条真实回复，丢弃 attribute 元数据。
fn parse_attribute_value(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid attribute length")?;
    if count < 0 {
        bail!("invalid RESP attribute count");
    }
    let pairs = (count as usize)
        .checked_mul(2)
        .context("attribute element count overflow")?;
    for _ in 0..pairs {
        let Some((_, consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)? else {
            return Ok(None);
        };
        pos = checked_add(pos, consumed, "attribute value length overflow")?;
    }
    let Some((value, consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)? else {
        return Ok(None);
    };
    pos = checked_add(pos, consumed, "attribute value length overflow")?;
    Ok(Some((value, pos)))
}

/// 业务作用: 将内部 RESP 值重新编码成 wire bytes，用于拓扑地址改写后写回客户端。
/// 参数说明:
/// - `value`: 已解析或聚合出的 RESP 值。
///
/// 返回: 可直接写入 TCP 连接的 RESP 编码字节。
// 编码 RESP value，用于改写 Redis Cluster 拓扑响应后重新生成合法 RESP 帧。
pub fn encode_value(value: &RespValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_value_into(value, &mut out);
    out
}

/// 业务作用: 生成 Redis `+OK` 响应，用于本地处理 AUTH、SELECT 0、QUIT 等无需后端参与的命令。
/// 参数说明: 无。
/// 返回: 静态 RESP 简单字符串字节。
pub fn simple_ok() -> &'static [u8] {
    b"+OK\r\n"
}

/// 业务作用: 生成 Redis `+PONG` 响应，用于本地处理无参数 `PING`。
/// 参数说明: 无。
/// 返回: 静态 RESP 简单字符串字节。
pub fn pong() -> &'static [u8] {
    b"+PONG\r\n"
}

/// 业务作用: 将一段二进制内容编码成 RESP bulk string，保留任意非 UTF-8 payload。
/// 参数说明:
/// - `item`: bulk string 的原始字节内容。
///
/// 返回: 可直接写入连接的 `$len\r\npayload\r\n` 字节。
pub fn bulk(item: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
    out
}

/// 业务作用: 生成 RESP2 null bulk，用于不存在的 key、聚合缺失值和兼容旧客户端。
/// 参数说明: 无。
/// 返回: 静态 `$-1` 响应字节。
pub fn null_bulk() -> &'static [u8] {
    b"$-1\r\n"
}

/// 业务作用: 将整数编码成 RESP integer，用于删除数量、DBSIZE、协议版本等数值响应。
/// 参数说明:
/// - `value`: 要写入响应的整数值。
///
/// 返回: 可直接写入连接的 `:<value>` 响应字节。
pub fn integer(value: i64) -> Vec<u8> {
    format!(":{value}\r\n").into_bytes()
}

/// 业务作用: 生成 Redis 认证缺失错误，用于 terminate 模式下首条业务命令未认证的场景。
/// 参数说明: 无。
/// 返回: 静态错误响应字节。
pub fn noauth() -> &'static [u8] {
    b"-NOAUTH Authentication required.\r\n"
}

/// 业务作用: 生成 Redis 密码错误响应，保持客户端对认证失败的标准识别。
/// 参数说明: 无。
/// 返回: 静态错误响应字节。
pub fn wrongpass() -> &'static [u8] {
    b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
}

/// 业务作用: 生成 Redis 标准 `NOPROTO` 错误，用于明确拒绝当前代理模式不能完整兑现的协议版本。
/// 参数说明: 无。
/// 返回: 静态协议版本不受支持错误字节。
pub fn noproto() -> &'static [u8] {
    b"-NOPROTO unsupported protocol version\r\n"
}

/// 业务作用: 生成通用 `-ERR` 响应，供代理层把配置、路由或协议错误返回给客户端。
/// 参数说明:
/// - `msg`: 错误消息正文，不包含 `-ERR` 前缀和 CRLF。
///
/// 返回: 可直接写入连接的错误响应字节。
pub fn err(msg: &str) -> Vec<u8> {
    format!("-ERR {msg}\r\n").into_bytes()
}

/// 业务作用: 生成 Redis Cluster `-CROSSSLOT` 响应，用于拒绝无法安全拆分或跨槽执行的多 key 命令。
/// 参数说明:
/// - `msg`: CROSSSLOT 错误说明正文。
///
/// 返回: 可直接写入连接的错误响应字节。
pub fn crossslot(msg: &str) -> Vec<u8> {
    format!("-CROSSSLOT {msg}\r\n").into_bytes()
}

/// 业务作用: 将命令参数编码成 RESP array of bulk strings，用于向后端发送 Redis 命令。
/// 参数说明:
/// - `items`: 每个数组元素的原始字节内容，函数会逐个编码为 bulk string。
///
/// 返回: 可直接写入后端连接的 RESP array 字节。
pub fn array(items: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
    for item in items {
        out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
        out.extend_from_slice(item);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// 业务作用: 将二进制列表编码成 RESP array of bulk strings，用于 MGET、SCAN、拓扑字段等批量响应。
/// 参数说明:
/// - `items`: 响应数组中的非空 bulk string 列表。
///
/// 返回: 可直接写回客户端的 RESP array 字节。
pub fn bulk_array(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
    for item in items {
        out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
        out.extend_from_slice(item);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// 业务作用: 将可空二进制列表编码成 RESP array，用于保持 MGET 这类命令中缺失 key 的位置。
/// 参数说明:
/// - `items`: 按客户端请求顺序排列的可空 bulk string 列表。
///
/// 返回: 可直接写回客户端的 RESP array 字节。
pub fn bulk_array_nullable(items: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
    for item in items {
        match item {
            Some(item) => {
                out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                out.extend_from_slice(item);
                out.extend_from_slice(b"\r\n");
            }
            None => out.extend_from_slice(null_bulk()),
        }
    }
    out
}

/// 业务作用: 生成 Redis `SCAN` 响应结构，保持 `[cursor, keys]` 的标准两元素数组形态。
/// 参数说明:
/// - `cursor`: 下一轮扫描游标，通常是 Redis 返回游标或代理封装游标。
/// - `items`: 本轮扫描返回的 key 列表。
///
/// 返回: 可直接写回客户端的 SCAN 响应字节。
pub fn scan_response(cursor: &[u8], items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"*2\r\n");
    out.extend_from_slice(&bulk(cursor));
    out.extend_from_slice(&bulk_array(items));
    out
}

/// 业务作用: 生成默认 standalone 模式的 RESP3 `HELLO` 响应，供简单前端认证路径复用。
/// 参数说明: 无。
/// 返回: 静态 RESP3 map 响应字节。
pub fn hello3_response() -> &'static [u8] {
    b"%7\r\n+server\r\n+narix\r\n+version\r\n+1.0.0\r\n+proto\r\n:3\r\n+id\r\n:0\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n"
}

/// 业务作用: 生成指定代理模式的 RESP3 `HELLO` 响应，让 cluster listener 能向客户端声明 cluster 能力。
/// 参数说明:
/// - `mode`: 返回给客户端的 Redis server mode 字符串。
///
/// 返回: RESP3 map 响应字节，`mode` 已经过简单字符串清洗。
// 构造 RESP3 HELLO 响应，用于需要按代理模式返回 cluster/standalone 的认证握手。
pub fn hello3_response_for_mode(mode: &str) -> Vec<u8> {
    hello3_response_for_server_mode("narix", mode)
}

/// 业务作用: 生成带后端产品身份的 RESP3 HELLO 响应。
///
/// `server` 来自 `backend_profile`，用于让产品专属 SDK 识别其实际连接的后端家族；
/// `version` 仍是 NARIX 版本，避免冒充尚未探测的后端精确版本。
///
/// 参数说明:
/// - `server`: HELLO 响应声明的后端产品名称。
/// - `mode`: HELLO 响应声明的 standalone 或 cluster 服务模式。
pub fn hello3_response_for_server_mode(server: &str, mode: &str) -> Vec<u8> {
    let server = sanitize_simple_string(server);
    let mode = sanitize_simple_string(mode);
    format!(
        "%7\r\n+server\r\n+{server}\r\n+version\r\n+1.0.0\r\n+proto\r\n:3\r\n+id\r\n:0\r\n+mode\r\n+{mode}\r\n+role\r\n+master\r\n+modules\r\n*0\r\n"
    )
    .into_bytes()
}

/// 业务作用: 生成默认 standalone 模式的 RESP2 `HELLO` 响应，供只支持 RESP2 的客户端认证后使用。
/// 参数说明: 无。
/// 返回: 静态 RESP2 array 响应字节。
pub fn hello2_response() -> &'static [u8] {
    b"*14\r\n$6\r\nserver\r\n$5\r\nnarix\r\n$7\r\nversion\r\n$5\r\n1.0.0\r\n$5\r\nproto\r\n:2\r\n$2\r\nid\r\n:0\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n"
}

/// 业务作用: 生成指定代理模式的 RESP2 `HELLO` 响应，保持老客户端也能拿到正确 server mode。
/// 参数说明:
/// - `mode`: 返回给客户端的 Redis server mode 字符串。
///
/// 返回: RESP2 array 响应字节，`mode` 已经过 bulk string 编码。
// 构造 RESP2 HELLO 响应，用于需要按代理模式返回 cluster/standalone 的认证握手。
pub fn hello2_response_for_mode(mode: &str) -> Vec<u8> {
    hello2_response_for_server_mode("narix", mode)
}

/// 业务作用: 生成带后端产品身份的 RESP2 HELLO 响应。
///
/// 参数说明:
/// - `server`: HELLO 响应声明的后端产品名称。
/// - `mode`: HELLO 响应声明的 standalone 或 cluster 服务模式。
pub fn hello2_response_for_server_mode(server: &str, mode: &str) -> Vec<u8> {
    let server = sanitize_simple_string(server);
    let mode = sanitize_simple_string(mode);
    let mut out = Vec::new();
    out.extend_from_slice(b"*14\r\n");
    for item in [
        b"server".as_slice(),
        server.as_bytes(),
        b"version".as_slice(),
        b"1.0.0".as_slice(),
        b"proto".as_slice(),
    ] {
        out.extend_from_slice(&bulk(item));
    }
    out.extend_from_slice(&integer(2));
    out.extend_from_slice(&bulk(b"id"));
    out.extend_from_slice(&integer(0));
    out.extend_from_slice(&bulk(b"mode"));
    out.extend_from_slice(&bulk(mode.as_bytes()));
    out.extend_from_slice(&bulk(b"role"));
    out.extend_from_slice(&bulk(b"master"));
    out.extend_from_slice(&bulk(b"modules"));
    out.extend_from_slice(b"*0\r\n");
    out
}

/// 业务作用: 提取并大写 Redis 命令名，供路由规划、认证状态机和本地响应分支做大小写无关匹配。
/// 参数说明:
/// - `args`: Redis 命令参数列表，首个元素应为命令名。
///
/// 返回: 大写命令名；空参数返回空字符串。
pub fn command_name<S: AsRef<[u8]>>(args: &[S]) -> String {
    args.first()
        .map(|v| String::from_utf8_lossy(v.as_ref()).to_ascii_uppercase())
        .unwrap_or_default()
}

/// 业务作用: 解析 RESP array command，并把每个 bulk string 参数复制成独立字节数组。
/// 参数说明:
/// - `input`: 以 `*` 开头的 Redis 命令帧。
///
/// 返回: 完整命令返回参数和消费长度；半包返回 `None`；非 bulk 参数或非法 CRLF 返回错误。
// 解析 parse_array 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_array(input: &[u8]) -> anyhow::Result<Option<ParsedCommand>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count: usize = parse_usize(line).context("invalid array length")?;
    let mut args = Vec::new();

    for _ in 0..count {
        if pos >= input.len() {
            return Ok(None);
        }
        if input[pos] != b'$' {
            bail!("expected bulk string");
        }
        let Some((line, after_len)) = read_line(input, pos + 1)? else {
            return Ok(None);
        };
        let len: isize = parse_isize(line).context("invalid bulk length")?;
        if len < 0 {
            bail!("null bulk string is not valid for command args");
        }
        let len = len as usize;
        let data_start = after_len;
        let data_end = checked_add(data_start, len, "bulk length overflow")?;
        let crlf_end = checked_add(data_end, 2, "bulk length overflow")?;
        if input.len() < crlf_end {
            return Ok(None);
        }
        if &input[data_end..crlf_end] != b"\r\n" {
            bail!("invalid bulk string terminator");
        }
        args.push(input[data_start..data_end].to_vec());
        pos = crlf_end;
    }

    Ok(Some(ParsedCommand {
        args,
        consumed: pos,
    }))
}

/// 业务作用: 解析 RESP array command 并只记录参数区间，供高频路径直接借用原始缓冲区。
/// 参数说明:
/// - `input`: 以 `*` 开头的 Redis 命令帧，参数必须都是非空 bulk string。
///
/// 返回: 完整命令返回参数区间和消费长度；半包返回 `None`；非法 bulk 或 CRLF 返回错误。
// 解析 parse_array_ranges 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_array_ranges(input: &[u8]) -> anyhow::Result<Option<ParsedCommandRanges>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count: usize = parse_usize(line).context("invalid array length")?;
    let mut arg_ranges = Vec::new();

    for _ in 0..count {
        if pos >= input.len() {
            return Ok(None);
        }
        if input[pos] != b'$' {
            bail!("expected bulk string");
        }
        let Some((line, after_len)) = read_line(input, pos + 1)? else {
            return Ok(None);
        };
        let len: isize = parse_isize(line).context("invalid bulk length")?;
        if len < 0 {
            bail!("null bulk string is not valid for command args");
        }
        let len = len as usize;
        let data_start = after_len;
        let data_end = checked_add(data_start, len, "bulk length overflow")?;
        let crlf_end = checked_add(data_end, 2, "bulk length overflow")?;
        if input.len() < crlf_end {
            return Ok(None);
        }
        if &input[data_end..crlf_end] != b"\r\n" {
            bail!("invalid bulk string terminator");
        }
        arg_ranges.push(data_start..data_end);
        pos = crlf_end;
    }

    Ok(Some(ParsedCommandRanges {
        arg_ranges,
        consumed: pos,
    }))
}

/// 业务作用: 解析 inline command，并按空格或制表符拆分参数，兼容手工 telnet 风格输入。
/// 参数说明:
/// - `input`: 一行以 CRLF 或 LF 结束的 inline command 缓冲。
///
/// 返回: 完整命令返回参数和消费长度；没有完整行结束符时返回 `None`。
// 解析 parse_inline 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_inline(input: &[u8]) -> anyhow::Result<Option<ParsedCommand>> {
    let line_end = find_inline_line_end(input);
    reject_nul_in_line_prefix(input, 0, line_end.map(|(end, _)| end))?;
    let Some((end, consumed)) = line_end else {
        return Ok(None);
    };
    let line = &input[..end];
    let args = line
        .split(|b| *b == b' ' || *b == b'\t')
        .filter(|part| !part.is_empty())
        .map(|part| part.to_vec())
        .collect();
    Ok(Some(ParsedCommand { args, consumed }))
}

/// 业务作用: 解析 inline command 的参数区间，减少普通命令分类时的临时分配。
/// 参数说明:
/// - `input`: 一行以 CRLF 或 LF 结束的 inline command 缓冲。
///
/// 返回: 完整命令返回参数区间和消费长度；没有完整行结束符时返回 `None`。
// 解析 parse_inline_ranges 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_inline_ranges(input: &[u8]) -> anyhow::Result<Option<ParsedCommandRanges>> {
    let line_end = find_inline_line_end(input);
    reject_nul_in_line_prefix(input, 0, line_end.map(|(end, _)| end))?;
    let Some((end, consumed)) = line_end else {
        return Ok(None);
    };
    let mut arg_ranges = Vec::new();
    let mut pos = 0;
    while pos < end {
        while pos < end && (input[pos] == b' ' || input[pos] == b'\t') {
            pos += 1;
        }
        let start = pos;
        while pos < end && input[pos] != b' ' && input[pos] != b'\t' {
            pos += 1;
        }
        if start < pos {
            arg_ranges.push(start..pos);
        }
    }
    Ok(Some(ParsedCommandRanges {
        arg_ranges,
        consumed,
    }))
}

/// 业务作用: 查找 inline 命令行边界，同时兼容 Redis 常见 CRLF 和 Dragonfly 接受的单 LF 输入。
/// 参数说明:
/// - `input`: 尚未完成解析的前端协议缓冲区。
///
/// 返回: 行内容结束位置及包含结束符的总消费长度；输入仍是半包时返回 `None`。
fn find_inline_line_end(input: &[u8]) -> Option<(usize, usize)> {
    let newline = input.iter().position(|byte| *byte == b'\n')?;
    if newline > 0 && input[newline - 1] == b'\r' {
        Some((newline - 1, newline + 1))
    } else {
        Some((newline, newline + 1))
    }
}

/// 业务作用: 从指定位置读取一行 RESP 长度或标量内容，不包含末尾 CRLF。
/// 参数说明:
/// - `input`: 待扫描的 RESP 缓冲。
/// - `start`: 行内容起始下标，通常跳过类型字节或长度前缀。
///
/// 返回: 找到 CRLF 时返回行内容和下一段起始下标；未找到时返回 `None`。
fn read_line(input: &[u8], start: usize) -> anyhow::Result<Option<(&[u8], usize)>> {
    let end = find_crlf(input, start);
    reject_nul_in_line_prefix(input, start, end)?;
    let Some(end) = end else {
        return Ok(None);
    };
    Ok(Some((&input[start..end], end + 2)))
}

/// 业务作用: 在 RESP 头部或 inline 命令中一旦出现 NUL 就立即拒绝，即使对端尚未发送 CRLF。
///
/// Redis 与 Valkey 会立即关闭协议失步的客户端。如果这里继续等待 CRLF，
/// `*\0`、`$\0` 这类输入就能长期占用前端连接，并让缓冲区增长到 pipeline 上限。
///
/// 参数说明:
/// - `input`: 当前前端协议缓冲区。
/// - `start`: 本行内容的起始下标。
/// - `line_end`: 已找到的行尾下标；半包时为空并检查当前全部前缀。
fn reject_nul_in_line_prefix(
    input: &[u8],
    start: usize,
    line_end: Option<usize>,
) -> anyhow::Result<()> {
    let end = line_end.unwrap_or(input.len());
    let Some(line_prefix) = input.get(start..end) else {
        return Ok(());
    };
    if line_prefix.contains(&0) {
        bail!("invalid NUL byte in request");
    }
    Ok(())
}

/// 业务作用: 计算 bulk/verbatim/blob-error 这类长度前缀帧的完整字节数。
/// 参数说明:
/// - `input`: 以 `$`、`=` 或 `!` 开头的 RESP 缓冲。
///
/// 返回: 完整帧返回长度；半包返回 `None`；长度溢出或 CRLF 错误返回错误。
fn bulk_frame_len(input: &[u8]) -> anyhow::Result<Option<usize>> {
    let Some((line, after_len)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let len = parse_isize(line).context("invalid bulk length")?;
    if len < 0 {
        return Ok(Some(after_len));
    }
    let body_end = checked_add(after_len, len as usize, "bulk frame length overflow")?;
    let end = checked_add(body_end, 2, "bulk frame length overflow")?;
    if input.len() < end {
        return Ok(None);
    }
    if &input[end - 2..end] != b"\r\n" {
        bail!("invalid bulk frame terminator");
    }
    Ok(Some(end))
}

/// 业务作用: 解析一行型 RESP 值，并用调用方提供的构造器映射成内部值。
/// 参数说明:
/// - `input`: 以一行型 RESP 类型字节开头的缓冲。
/// - `build`: 将行内容字节转换成具体 `RespValue` 分支的构造函数。
///
/// 返回: 完整行返回解析值和消费长度；没有 CRLF 时返回 `None`。
// 解析 parse_line_value 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_line_value(
    input: &[u8],
    build: impl FnOnce(Vec<u8>) -> RespValue,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, consumed)) = read_line(input, 1)? else {
        return Ok(None);
    };
    Ok(Some((build(line.to_vec()), consumed)))
}

/// 业务作用: 解析 RESP bulk string，保留 null bulk 与空字符串的区别。
/// 参数说明:
/// - `input`: 以 `$` 或兼容 bulk 形态类型开头的缓冲。
///
/// 返回: 完整 bulk 返回 `RespValue::Bulk` 和消费长度；半包返回 `None`。
// 解析 parse_bulk_value 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_bulk_value(input: &[u8]) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, after_len)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let len = parse_isize(line).context("invalid bulk length")?;
    if len < 0 {
        return Ok(Some((RespValue::Bulk(None), after_len)));
    }
    let body_end = checked_add(after_len, len as usize, "bulk frame length overflow")?;
    let end = checked_add(body_end, 2, "bulk frame length overflow")?;
    if input.len() < end {
        return Ok(None);
    }
    if &input[end - 2..end] != b"\r\n" {
        bail!("invalid bulk frame terminator");
    }
    Ok(Some((
        RespValue::Bulk(Some(input[after_len..end - 2].to_vec())),
        end,
    )))
}

/// 业务作用: 递归解析 RESP array，保留 null array 和普通 array 的协议差异。
/// 参数说明:
/// - `input`: 以 `*` 开头的 RESP array 缓冲。
/// - `remaining_depth`: 数组元素可继续使用的嵌套预算。
///
/// 返回: 完整 array 返回内部数组值；任一子元素半包时返回 `None`。
// 解析 parse_array_value 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_array_value(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid array length")?;
    if count < 0 {
        return Ok(Some((RespValue::Array(None), pos)));
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let Some((value, consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)?
        else {
            return Ok(None);
        };
        values.push(value);
        pos += consumed;
    }
    Ok(Some((RespValue::Array(Some(values)), pos)))
}

/// 业务作用: 递归解析 RESP3 map，用于读取和改写 RESP3 Cluster 拓扑响应。
/// 参数说明:
/// - `input`: 以 `%` 开头的 RESP3 map 缓冲。
/// - `remaining_depth`: map 的键和值可继续使用的嵌套预算。
///
/// 返回: 完整 map 返回 key/value 对列表；任一子元素半包时返回 `None`。
// 解析 parse_map_value 输入，用于支撑 RESP3 map 类型的 Cluster 拓扑响应改写。
fn parse_map_value(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid map length")?;
    if count < 0 {
        return Ok(Some((RespValue::Map(None), pos)));
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let Some((key, key_consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)?
        else {
            return Ok(None);
        };
        pos += key_consumed;
        let Some((value, value_consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)?
        else {
            return Ok(None);
        };
        pos += value_consumed;
        values.push((key, value));
    }
    Ok(Some((RespValue::Map(Some(values)), pos)))
}

/// 业务作用: 递归解析 RESP3 set，用于保持 RESP3 响应读取和测试校验的类型信息。
/// 参数说明:
/// - `input`: 以 `~` 开头的 RESP3 set 缓冲。
/// - `remaining_depth`: set 元素可继续使用的嵌套预算。
///
/// 返回: 完整 set 返回元素列表；任一子元素半包时返回 `None`。
// 解析 parse_set_value 输入，用于支撑 RESP3 set 类型的响应读取和测试校验。
fn parse_set_value(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid set length")?;
    if count < 0 {
        return Ok(Some((RespValue::Set(None), pos)));
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let Some((value, consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)?
        else {
            return Ok(None);
        };
        values.push(value);
        pos += consumed;
    }
    Ok(Some((RespValue::Set(Some(values)), pos)))
}

/// 业务作用: 递归解析 RESP3 push，用于 Pub/Sub 或服务端主动推送帧的测试和转发辅助判断。
/// 参数说明:
/// - `input`: 以 `>` 开头的 RESP3 push 缓冲。
/// - `remaining_depth`: push 元素可继续使用的嵌套预算。
///
/// 返回: 完整 push 返回元素列表；负长度会作为协议错误拒绝。
// 解析 parse_push_value 输入，用于支撑 RESP3 Pub/Sub push 帧和客户端主动推送消息的校验。
fn parse_push_value(
    input: &[u8],
    remaining_depth: usize,
) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid push length")?;
    if count < 0 {
        bail!("invalid RESP3 push length");
    }
    let mut values = Vec::new();
    for _ in 0..count {
        let Some((value, consumed)) = parse_value_with_depth(&input[pos..], remaining_depth)?
        else {
            return Ok(None);
        };
        values.push(value);
        pos += consumed;
    }
    Ok(Some((RespValue::Push(values), pos)))
}

/// 业务作用: 解析 RESP3 null，避免把 `_` 类型误当成 RESP2 null bulk。
/// 参数说明:
/// - `input`: 以 `_` 开头的 RESP3 null 缓冲。
///
/// 返回: 合法 null 返回 `RespValue::Null` 和消费长度；payload 非空返回错误。
// 解析 parse_null_value 输入，用于支撑 RESP3 null 类型的 Cluster 拓扑响应改写。
fn parse_null_value(input: &[u8]) -> anyhow::Result<Option<(RespValue, usize)>> {
    let Some((line, consumed)) = read_line(input, 1)? else {
        return Ok(None);
    };
    if !line.is_empty() {
        bail!("invalid RESP3 null frame");
    }
    Ok(Some((RespValue::Null, consumed)))
}

/// 业务作用: 递归编码内部 RESP 值到既有输出缓冲，避免多层聚合响应反复分配临时 Vec。
/// 参数说明:
/// - `value`: 待编码的内部 RESP 值。
/// - `out`: 调用方提供的输出缓冲，函数会追加编码结果。
///
// 编码 encode_value_into 输出，用于支撑 RESP value 的递归编码。
fn encode_value_into(value: &RespValue, out: &mut Vec<u8>) {
    match value {
        RespValue::Simple(bytes) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Error(bytes) => {
            out.extend_from_slice(b"-");
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Integer(value) => {
            out.extend_from_slice(format!(":{value}\r\n").as_bytes());
        }
        RespValue::Bulk(Some(bytes)) => {
            out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
            out.extend_from_slice(bytes);
            out.extend_from_slice(b"\r\n");
        }
        RespValue::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        RespValue::Array(Some(values)) => {
            out.extend_from_slice(format!("*{}\r\n", values.len()).as_bytes());
            for value in values {
                encode_value_into(value, out);
            }
        }
        RespValue::Array(None) => out.extend_from_slice(b"*-1\r\n"),
        RespValue::Map(Some(values)) => {
            out.extend_from_slice(format!("%{}\r\n", values.len()).as_bytes());
            for (key, value) in values {
                encode_value_into(key, out);
                encode_value_into(value, out);
            }
        }
        RespValue::Map(None) => out.extend_from_slice(b"%-1\r\n"),
        RespValue::Set(Some(values)) => {
            out.extend_from_slice(format!("~{}\r\n", values.len()).as_bytes());
            for value in values {
                encode_value_into(value, out);
            }
        }
        RespValue::Set(None) => out.extend_from_slice(b"~-1\r\n"),
        RespValue::Push(values) => {
            out.extend_from_slice(format!(">{}\r\n", values.len()).as_bytes());
            for value in values {
                encode_value_into(value, out);
            }
        }
        RespValue::Null => out.extend_from_slice(b"_\r\n"),
    }
}

/// 业务作用: 计算 array/map/set/push 聚合帧长度，递归确认所有子帧完整后再允许上层消费。
/// 参数说明:
/// - `input`: 以聚合类型字节开头的 RESP 缓冲。
/// - `remaining_depth`: 聚合元素可继续使用的嵌套预算。
///
/// 返回: 完整聚合帧返回总长度；任一子帧半包返回 `None`；map 元素数溢出返回错误。
fn aggregate_frame_len(input: &[u8], remaining_depth: usize) -> anyhow::Result<Option<usize>> {
    let Some((line, mut pos)) = read_line(input, 1)? else {
        return Ok(None);
    };
    let count = parse_isize(line).context("invalid aggregate length")?;
    if count < 0 {
        return Ok(Some(pos));
    }
    let count = count as usize;
    let elements = if input[0] == b'%' {
        count
            .checked_mul(2)
            .context("aggregate element count overflow")?
    } else {
        count
    };
    for _ in 0..elements {
        let Some(len) = frame_len_with_depth(&input[pos..], remaining_depth)? else {
            return Ok(None);
        };
        pos += len;
    }
    Ok(Some(pos))
}

/// 业务作用: 为进入一层 RESP 聚合值扣减递归预算，在真正递归前阻断超深输入。
/// 参数说明:
/// - `remaining_depth`: 当前调用尚可使用的聚合层数。
///
/// 返回: 可传给子值的剩余预算；预算耗尽时返回明确协议错误。
fn descend_resp_depth(remaining_depth: usize) -> anyhow::Result<usize> {
    remaining_depth
        .checked_sub(1)
        .with_context(|| format!("RESP nesting depth exceeds limit of {MAX_RESP_NESTING_DEPTH}"))
}

/// 业务作用: 在缓冲区中查找 RESP 行结束符，作为所有行型解析器的基础边界判断。
/// 参数说明:
/// - `input`: 待扫描的协议缓冲。
/// - `start`: 开始查找 CRLF 的下标。
///
/// 返回: 找到时返回 `\r` 所在下标；未找到或 `start` 越界时返回 `None`。
fn find_crlf(input: &[u8], start: usize) -> Option<usize> {
    input
        .get(start..)?
        .windows(2)
        .position(|w| w == b"\r\n")
        .map(|idx| start + idx)
}

/// 业务作用: 将 RESP 长度行解析成无符号整数，用于 array 元素数和 command 参数个数。
/// 参数说明:
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 成功返回整数；非 UTF-8 或非数字返回错误。
// 解析 parse_usize 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_usize(bytes: &[u8]) -> anyhow::Result<usize> {
    Ok(std::str::from_utf8(bytes)?.parse()?)
}

/// 业务作用: 将 RESP 长度行解析成有符号整数，用于识别 null bulk、null array 等负长度语义。
/// 参数说明:
/// - `bytes`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
/// 返回: 成功返回整数；非 UTF-8 或非数字返回错误。
// 解析 parse_isize 输入，用于支撑解析和编码 Redis RESP 协议帧。
fn parse_isize(bytes: &[u8]) -> anyhow::Result<isize> {
    Ok(std::str::from_utf8(bytes)?.parse()?)
}

/// 业务作用: 对帧偏移做溢出保护加法，防止恶意长度字段导致 usize 回绕后越界切片。
/// 参数说明:
/// - `left`: 当前偏移或起始长度。
/// - `right`: 需要增加的帧长度。
/// - `message`: 溢出时附加到错误里的上下文说明。
///
/// 返回: 加法成功返回新偏移；溢出返回错误。
fn checked_add(left: usize, right: usize, message: &str) -> anyhow::Result<usize> {
    left.checked_add(right).context(message.to_string())
}

/// 业务作用: 清理 RESP simple string payload，避免动态 mode 字段插入 CRLF 破坏 HELLO 响应结构。
/// 参数说明:
/// - `value`: 准备写入 simple string 的文本。
///
/// 返回: 去除 CR 和 LF 后的文本。
// 清理 RESP simple string 内容，用于避免 HELLO mode 中出现 CRLF 破坏协议帧。
fn sanitize_simple_string(value: &str) -> String {
    value
        .chars()
        .filter(|ch| *ch != '\r' && *ch != '\n')
        .collect()
}
