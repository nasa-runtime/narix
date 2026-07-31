const SLOT_COUNT: u16 = 16_384;
const CRC16_TABLE: [u16; 256] = build_crc16_table();

/// 业务作用: 按 Redis Cluster hash tag 和 CRC16 规则计算键所属槽位。
/// 参数说明:
/// - `key`: 当前命令、租约或配置项使用的唯一键，用于定位明确的数据或状态。
///
pub fn redis_slot(key: &[u8]) -> u16 {
    let key = hash_key(key);
    crc16(key) % SLOT_COUNT
}

/// 业务作用: 提取有效花括号 hash tag；不存在时使用完整键参与哈希。
/// 参数说明:
/// - `key`: 当前命令、租约或配置项使用的唯一键，用于定位明确的数据或状态。
///
pub fn hash_key(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|b| *b == b'{') else {
        return key;
    };
    let rest = &key[open + 1..];
    let Some(close_rel) = rest.iter().position(|b| *b == b'}') else {
        return key;
    };
    if close_rel == 0 {
        return key;
    }
    &rest[..close_rel]
}

/// 业务作用: 使用预计算表计算 Redis Cluster 兼容的 CRC16 校验值。
/// 参数说明:
/// - `data`: 协议帧或字节缓冲区，用于 RESP 解析、写回或大小限制校验。
///
fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for byte in data {
        let idx = ((crc >> 8) ^ (*byte as u16)) as usize;
        crc = (crc << 8) ^ CRC16_TABLE[idx];
    }
    crc
}

/// 业务作用: 预计算 Redis Cluster 使用的 CRC16 查表，确保运行时槽位计算与服务端算法一致且不逐位重复运算。
/// 参数说明: 无。
// 构建 CRC16 查表数据，用于避免每个 key 逐 bit 计算 Redis Cluster 槽位。
const fn build_crc16_table() -> [u16; 256] {
    let mut table = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut bit = 0;
        while bit < 8 {
            if (crc & 0x8000) != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}
