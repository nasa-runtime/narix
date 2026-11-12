//! 启动实例身份独立于墙钟精度、系统校时和进程编号复用。

/// 业务作用: 为运行代和协调启动实例生成不可预测的身份，避免旧实例请求复用新实例的控制身份。
/// 参数说明: 无。
///
/// 返回: 由系统安全随机源生成的 256 位小写十六进制标识；随机源不可用时拒绝创建实例身份。
pub(crate) fn instance_nonce() -> anyhow::Result<String> {
    let mut random = [0u8; 32];
    // 身份碰撞会使旧租约释放或迟到协议请求命中新实例，随机源失败时不能退回时间戳或固定值。
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut random)
        .map_err(|_| anyhow::anyhow!("system randomness is unavailable for instance identity"))?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut nonce = String::with_capacity(random.len() * 2);
    for byte in random {
        nonce.push(HEX[usize::from(byte >> 4)] as char);
        nonce.push(HEX[usize::from(byte & 15)] as char);
    }
    Ok(nonce)
}
