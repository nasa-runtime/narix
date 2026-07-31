use anyhow::{bail, Context};

use crate::ha::model::format_host_port;
use crate::resp::{self, RespValue};

/// 业务作用: 从冒号分隔的 INFO 行读取字符串字段。
///
/// 参数:
/// - `info`: RESP 兼容后端返回的 INFO 文本。
/// - `field`: 要读取的字段名称。
pub(crate) fn parse_info_string(info: &str, field: &str) -> Option<String> {
    info.lines()
        .map(str::trim)
        .filter_map(|line| line.split_once(':'))
        .find_map(|(name, value)| (name == field).then(|| value.trim().to_string()))
}

/// 业务作用: 从冒号分隔的 INFO 行读取无符号整数字段。
///
/// 参数:
/// - `info`: RESP 兼容后端返回的 INFO 文本。
/// - `field`: 要读取的字段名称。
pub(crate) fn parse_info_u64(info: &str, field: &str) -> Option<u64> {
    parse_info_string(info, field)?.parse().ok()
}

/// 业务作用: 从复制 INFO 文本提取副本当前跟随的上游地址。
///
/// 参数:
/// - `info`: RESP 兼容后端返回的 replication 信息文本。
pub(crate) fn parse_replica_master_addr(info: &str) -> Option<String> {
    let host = parse_info_string(info, "master_host")?;
    let port = parse_info_string(info, "master_port")?;
    Some(format_host_port(&host, &port))
}

/// 业务作用: 从 RESP 响应读取 INFO 文本。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
pub(crate) fn parse_bulk_string(frame: &[u8]) -> anyhow::Result<String> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty backend response")?;
    if consumed != frame.len() {
        bail!("backend response has trailing bytes");
    }
    match value {
        RespValue::Bulk(Some(bytes)) | RespValue::Simple(bytes) => Ok(String::from_utf8(bytes)?),
        _ => bail!("expected backend bulk string response"),
    }
}

/// 业务作用: 从 RESP 整数响应读取非负值。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
/// - `command`: 生成错误上下文的命令名称。
pub(crate) fn parse_integer_response(frame: &[u8], command: &str) -> anyhow::Result<u64> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty integer response")?;
    if consumed != frame.len() {
        bail!("{command} response has trailing bytes");
    }
    match value {
        RespValue::Integer(value) => {
            u64::try_from(value).with_context(|| format!("{command} returned a negative value"))
        }
        other => bail!("{command} expected integer response, got {:?}", other),
    }
}

/// 业务作用: 从 Redis 兼容 `TIME` 数组读取服务端纪元秒。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
pub(crate) fn parse_time_seconds(frame: &[u8]) -> anyhow::Result<u64> {
    let (value, consumed) = resp::parse_value(frame)?.context("empty TIME response")?;
    if consumed != frame.len() {
        bail!("TIME response has trailing bytes");
    }
    let RespValue::Array(Some(items)) = value else {
        bail!("TIME expected array response");
    };
    let seconds = items.first().context("TIME response is missing seconds")?;
    match seconds {
        RespValue::Bulk(Some(bytes)) | RespValue::Simple(bytes) => std::str::from_utf8(bytes)
            .context("TIME seconds are not UTF-8")?
            .parse::<u64>()
            .context("TIME seconds are not an unsigned integer"),
        RespValue::Integer(value) => {
            u64::try_from(*value).context("TIME returned negative seconds")
        }
        other => bail!("TIME seconds are not a string or integer: {:?}", other),
    }
}

/// 业务作用: 从 RESP2/3 `CONFIG GET` 响应读取配置值。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
pub(crate) fn parse_config_get_value(frame: &[u8]) -> anyhow::Result<String> {
    let (value, consumed) =
        resp::parse_value(frame)?.context("empty backend CONFIG GET response")?;
    if consumed != frame.len() {
        bail!("backend CONFIG GET response has trailing bytes");
    }
    let RespValue::Array(Some(items)) = value else {
        bail!("expected backend CONFIG GET array response");
    };
    if items.len() != 2 {
        bail!(
            "backend CONFIG GET response expected 2 items, got {}",
            items.len()
        );
    }
    match &items[1] {
        RespValue::Bulk(Some(bytes)) | RespValue::Simple(bytes) => {
            Ok(String::from_utf8(bytes.clone())?)
        }
        RespValue::Bulk(None) | RespValue::Null => Ok(String::new()),
        other => bail!("backend CONFIG GET value is not a string: {:?}", other),
    }
}

/// 业务作用: 校验后端返回标准 `+OK`。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
pub(crate) fn ensure_ok(frame: &[u8]) -> anyhow::Result<()> {
    if frame.starts_with(b"+OK") {
        Ok(())
    } else {
        bail!(
            "expected backend +OK, got {}",
            String::from_utf8_lossy(frame).trim()
        )
    }
}

/// 业务作用: 校验控制命令没有返回错误或空值。
///
/// 参数:
/// - `frame`: 后端返回的完整 RESP 帧。
pub(crate) fn ensure_accepted_response(frame: &[u8]) -> anyhow::Result<()> {
    let Some((value, consumed)) = resp::parse_value(frame)? else {
        bail!("incomplete backend response")
    };
    if consumed != frame.len() {
        bail!("backend response has trailing bytes");
    }
    match value {
        RespValue::Error(error) => {
            bail!(
                "backend returned error: {}",
                String::from_utf8_lossy(&error)
            )
        }
        RespValue::Bulk(None) | RespValue::Null => bail!("backend returned nil response"),
        _ => Ok(()),
    }
}
