use anyhow::{bail, Context};
use std::io::Write;

use crate::resp;

/// 业务作用: 定位一条完整后端请求，兼容标准 RESP 数组和行式请求的边界。
/// 参数说明:
/// - `input`: 当前连接尚未发送的客户端字节。
///
/// 返回: 完整请求返回占用字节数；半包返回 None；非法 RESP 返回错误。
pub(crate) fn request_len(input: &[u8]) -> anyhow::Result<Option<usize>> {
    if input.first() == Some(&b'*') {
        resp::frame_len(input)
    } else {
        Ok(input
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|end| end + 1))
    }
}

/// 业务作用: 在 Garnet 兼容路径识别指定命令，普通数组请求不分配参数集合。
/// 参数说明:
/// - `frame`: 已确认完整的单条客户端请求。
/// - `expected`: 需要识别的完整命令名，按 ASCII 大小写无关匹配。
///
/// 返回: 数组中的命令名匹配时返回 true；行式或其他命令返回 false。
fn request_name_is(frame: &[u8], expected: &[u8]) -> anyhow::Result<bool> {
    if frame.first() != Some(&b'*') {
        // Garnet 的行式入口只识别无参数 PING/QUIT；不能把本应拒绝的行式写请求变成有效写入。
        return Ok(false);
    }
    let Some(array_end) = frame.windows(2).position(|pair| pair == b"\r\n") else {
        return Ok(false);
    };
    let first = &frame[array_end + 2..];
    if first.first() != Some(&b'$') {
        return Ok(false);
    }
    let Some(length_end) = first.windows(2).position(|pair| pair == b"\r\n") else {
        return Ok(false);
    };
    let length = std::str::from_utf8(&first[1..length_end])?.parse::<usize>()?;
    Ok(length == expected.len()
        && first
            .get(length_end + 2..length_end + 2 + length)
            .is_some_and(|name| name.eq_ignore_ascii_case(expected)))
}

/// 业务作用: 把新 ETag 写命令转成旧版单条 SET，保留后端原子性及事务队列中的执行位置。
/// 参数说明:
/// - `frame`: 已确认命令名为 SETWITHETAG 的完整请求。
/// - `output`: 已分配的流水线输出，直接追加原参数字节以避免复制大 value 到中间集合。
///
/// 返回: 追加等价旧命令；缺参触发后端事务中止，其余非法参数保持执行期失败，错误文本沿用原生命令。
fn append_etag_alias(frame: &[u8], output: &mut Vec<u8>) -> anyhow::Result<()> {
    let header_end = frame
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .context("missing array header")?;
    let count = std::str::from_utf8(&frame[1..header_end])?.parse::<usize>()?;
    if count < 3 {
        // AUTH 的无参数请求在身份变更前拒绝，且不依赖用户额外的读命令权限；同一事务必须在排队阶段中止，防止 EXEC 执行先前排队的写入。
        output.extend_from_slice(b"*1\r\n$4\r\nAUTH\r\n");
        return Ok(());
    }
    if count > 5 {
        // 参数过多在原生新命令中属于执行期失败；保留真实 key 和 value，让事务仍按原顺序执行其他有效请求。
        let name_start = header_end + 2;
        let key_start =
            name_start + resp::frame_len(&frame[name_start..])?.context("incomplete ETag name")?;
        let value_start =
            key_start + resp::frame_len(&frame[key_start..])?.context("incomplete ETag key")?;
        let value_end = value_start
            + resp::frame_len(&frame[value_start..])?.context("incomplete ETag value")?;
        if frame[key_start] != b'$' || frame[value_start] != b'$' {
            bail!("ETag key and value must be bulk strings");
        }
        output.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
        output.extend_from_slice(&frame[key_start..value_end]);
        output.extend_from_slice(b"$2\r\nNX\r\n$2\r\nNX\r\n");
        return Ok(());
    }
    let command = resp::parse_command_ranges(frame)?.context("incomplete ETag request")?;
    let args = &command.arg_ranges;
    // 新命令只允许 EX/PX；直接拼接任意选项会把本应拒绝的 NX、GET 等请求变成有效写入。
    if args.len() > 3
        && !frame[args[3].clone()].eq_ignore_ascii_case(b"EX")
        && !frame[args[3].clone()].eq_ignore_ascii_case(b"PX")
    {
        output.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
        output.extend_from_slice(&frame[args[0].end + 2..args[2].end + 2]);
        output.extend_from_slice(b"$2\r\nNX\r\n$2\r\nNX\r\n");
        return Ok(());
    }
    // 显式限定新命令的 32 位过期值边界，不能依赖后端各版本 SET 的整数解析范围。
    if args.len() == 5
        && std::str::from_utf8(&frame[args[4].clone()])
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
            .is_none()
    {
        output.extend_from_slice(b"*5\r\n$3\r\nSET\r\n");
        output.extend_from_slice(&frame[args[0].end + 2..args[2].end + 2]);
        output.extend_from_slice(b"$2\r\nEX\r\n$1\r\n?\r\n");
        return Ok(());
    }
    output.extend_from_slice(b"*");
    output.push(b'0' + count as u8 + 1);
    output.extend_from_slice(b"\r\n$3\r\nSET\r\n");
    output.extend_from_slice(&frame[args[0].end + 2..]);
    output.extend_from_slice(b"$8\r\nWITHETAG\r\n");
    Ok(())
}

/// 业务作用: 在写入后端之前一次性改写旧 Garnet 的 ETag 别名，不重排或重放流水线命令。
/// 参数说明:
/// - `input`: 完整请求组成的连续流水线，不能包含末尾半包。
///
/// 返回: 存在别名时返回改写字节；全为原生命令时返回 None 且不复制请求；协议错误拒绝整批。
pub(crate) fn rewrite_garnet_pipeline(input: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
    rewrite_garnet_relay_pipeline(input, true, false)
}

/// 业务作用: 仅识别可等价转换为单元素 KEYS 列表的 Garnet 迁移请求，避免改变未知选项和多 key 形态的拒绝语义。
/// 参数说明:
/// - `frame`: 命令名已确认为 MIGRATE 的完整 RESP 数组。
///
/// 返回: 单个非空 key 且所有选项完整时返回参数边界；其余形态保持原始字节，协议非法时返回错误。
fn single_key_migrate_arguments(frame: &[u8]) -> anyhow::Result<Option<resp::ParsedCommandRanges>> {
    let command = resp::parse_command_ranges(frame)?.context("incomplete MIGRATE request")?;
    let args = &command.arg_ranges;
    if args.len() < 6 || args[3].is_empty() {
        return Ok(None);
    }
    let mut index = 6;
    while index < args.len() {
        let option = &frame[args[index].clone()];
        let width =
            if option.eq_ignore_ascii_case(b"COPY") || option.eq_ignore_ascii_case(b"REPLACE") {
                1
            } else if option.eq_ignore_ascii_case(b"AUTH") {
                2
            } else if option.eq_ignore_ascii_case(b"AUTH2") {
                3
            } else {
                // KEYS、槽位迁移和未知扩展不能在代理推断为单 key 请求，避免扩大实际迁移范围。
                return Ok(None);
            };
        if args.len() - index < width {
            return Ok(None);
        }
        index += width;
    }
    Ok(Some(command))
}

/// 业务作用: 使用同一条 MIGRATE 请求表达单 key 迁移，让后端按 KEYS 路径建立槽位范围并执行原生搬迁。
/// 参数说明:
/// - `frame`: 已通过单 key 形态校验的原始数组。
/// - `command`: 与原始数组对应的参数边界。
/// - `output`: 流水线输出；key、地址、超时和认证字节按原样追加。
///
/// 返回: 追加等价请求，保留命令权限、COPY/REPLACE、认证和原有执行顺序；不读取数据、不重试写入。
fn append_single_key_migrate(
    frame: &[u8],
    command: &resp::ParsedCommandRanges,
    output: &mut Vec<u8>,
) -> anyhow::Result<()> {
    let args = &command.arg_ranges;
    let header_end = frame
        .windows(2)
        .position(|pair| pair == b"\r\n")
        .context("missing MIGRATE array header")?;
    write!(output, "*{}\r\n", args.len() + 2)?;
    output.extend_from_slice(&frame[header_end + 2..args[2].end + 2]);
    output.extend_from_slice(b"$0\r\n\r\n");
    output.extend_from_slice(&frame[args[3].end + 2..]);
    output.extend_from_slice(b"$4\r\nKEYS\r\n");
    output.extend_from_slice(&frame[args[2].end + 2..args[3].end + 2]);
    Ok(())
}

/// 业务作用: 在 Garnet 专用连接发送前应用已确认的命令形态兼容，保持流水线、事务和权限检查所在的后端连接。
/// 参数说明:
/// - `input`: 完整请求组成的流水线，不包含末尾半包。
/// - `legacy_etag`: 当前连接是否确认需要旧 ETag 别名。
/// - `single_key_migrate`: 当前 Garnet 连接是否确认 MIGRATE 入口可见并启用单 key 的 KEYS 形态。
///
/// 返回: 只有需要转换时分配输出；普通请求返回 None，非法协议拒绝整批，不重放已发送内容。
pub(crate) fn rewrite_garnet_relay_pipeline(
    input: &[u8],
    legacy_etag: bool,
    single_key_migrate: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    let mut output: Option<Vec<u8>> = None;
    let mut start = 0;
    let mut copied = 0;
    while start < input.len() {
        let length = request_len(&input[start..])?.context("incomplete compatibility pipeline")?;
        if length == 0 {
            bail!("empty compatibility frame");
        }
        let end = start + length;
        let frame = &input[start..end];
        if legacy_etag && request_name_is(frame, b"SETWITHETAG")? {
            let bytes =
                output.get_or_insert_with(|| Vec::with_capacity(input.len().saturating_add(16)));
            bytes.extend_from_slice(&input[copied..start]);
            append_etag_alias(frame, bytes)?;
            copied = end;
        } else if single_key_migrate && request_name_is(frame, b"MIGRATE")? {
            if let Some(command) = single_key_migrate_arguments(frame)? {
                let bytes = output
                    .get_or_insert_with(|| Vec::with_capacity(input.len().saturating_add(24)));
                bytes.extend_from_slice(&input[copied..start]);
                append_single_key_migrate(frame, &command, bytes)?;
                copied = end;
            }
        }
        start = end;
    }
    if let Some(bytes) = output.as_mut() {
        bytes.extend_from_slice(&input[copied..]);
    }
    Ok(output)
}
