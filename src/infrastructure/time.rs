//! 墙钟只提供协议时间和展示时间；租约权威仍由协调状态与单调截止时间决定。

use anyhow::Context;
use nasa::date::clock::{SystemClock, UtcClock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// 业务作用: 集中读取 NASA 系统墙钟，不引入动态分派或同步锁。
/// 参数说明: 无。
///
/// 返回: 可因系统校时回拨的 UTC 时刻，不授予控制权威。
#[inline]
pub(crate) fn wall_now() -> SystemTime {
    UtcClock::now(&SystemClock)
}

/// 业务作用: 严格转换协议和租约记录的纪元时间，拒绝无效系统时间。
/// 参数说明:
/// - `time`: 待转换的墙钟时刻。
///
/// 返回: 非负纪元毫秒；早于纪元时失败，不能伪造有效租约时间。
#[inline]
pub(crate) fn unix_millis(time: SystemTime) -> anyhow::Result<u128> {
    Ok(time
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_millis())
}

/// 业务作用: 为协议字段读取有效纪元毫秒，保留失败语义。
/// 参数说明: 无。
///
/// 返回: 饱和到 u64 的时间；早于纪元时失败。
#[inline]
pub(crate) fn unix_millis_u64() -> anyhow::Result<u64> {
    Ok(u64::try_from(unix_millis(wall_now())?).unwrap_or(u64::MAX))
}

/// 业务作用: 提供只用于运维展示的时间，不参与租约授权。
/// 参数说明: 无。
///
/// 返回: 有效纪元毫秒；系统时间异常或超出展示范围时为零。
#[inline]
pub(crate) fn display_millis() -> u64 {
    unix_millis(wall_now())
        .ok()
        .and_then(|value| value.try_into().ok())
        .unwrap_or(0)
}

/// 业务作用: 将请求预算转换为非零协议毫秒，避免截断产生无期限请求。
/// 参数说明:
/// - `duration`: 已确定的请求预算。
///
/// 返回: 至少一毫秒，溢出时饱和。
#[inline]
pub(crate) fn duration_millis(duration: Duration) -> u64 {
    duration_millis_saturating(duration).max(1)
}

/// 业务作用: 将单调时长转换为毫秒状态字段，保留零值并避免极端时长回绕。
/// 参数说明:
/// - `duration`: 已测量的时长或允许为零的时间窗口。
///
/// 返回: 截断亚毫秒精度后的毫秒数，超出 u64 时饱和；不隐式赋予非零请求预算。
#[inline]
pub(crate) fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// 业务作用: 统一数据面、连接池和控制面耗时指标的微秒换算，保持原有截断与饱和语义。
/// 参数说明:
/// - `duration`: 由单调计时得到的耗时。
///
/// 返回: 截断亚微秒精度后的微秒数，超出 u64 时饱和；不分配内存或读取墙钟。
#[inline]
pub(crate) fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// 业务作用: 读取用于持久化记录的纪元毫秒，不掩盖系统时间异常。
/// 参数说明: 无。
///
/// 返回: 有效墙钟毫秒，早于纪元时失败。
#[inline]
pub(crate) fn now_millis() -> anyhow::Result<u128> {
    unix_millis(wall_now())
}

/// 业务作用: 为实例标识和候选排序读取高分辨率墙钟值，不作为租约权威。
/// 参数说明: 无。
///
/// 返回: 非负纪元纳秒，系统时间异常时失败。
#[inline]
pub(crate) fn unix_nanos() -> anyhow::Result<u128> {
    Ok(wall_now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?
        .as_nanos())
}

/// 业务作用: 为候选全序比较提供固定宽度的墙钟字段。
/// 参数说明: 无。
///
/// 返回: 饱和到 u64 的纪元纳秒，不提供时间单调性保证。
#[inline]
pub(crate) fn unix_nanos_u64() -> anyhow::Result<u64> {
    Ok(u64::try_from(unix_nanos()?).unwrap_or(u64::MAX))
}

/// 业务作用: 为管理状态提供不参与控制决策的纪元秒数。
/// 参数说明: 无。
///
/// 返回: 当前秒数；系统时间异常时为零。
#[inline]
pub(crate) fn display_seconds() -> u64 {
    wall_now()
        .duration_since(UNIX_EPOCH)
        .map(|time| time.as_secs())
        .unwrap_or_default()
}

/// 业务作用: 将单调耗时转换为运维微秒指标，避免系统校时污染时延。
/// 参数说明:
/// - `started`: 操作开始的单调时刻。
///
/// 返回: 饱和到 u64 的微秒耗时；保留 Instant 的直接取时路径。
#[inline]
pub(crate) fn elapsed_micros(started: Instant) -> u64 {
    duration_micros(started.elapsed())
}
