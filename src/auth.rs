use crate::resp;

/// 业务作用: 表示前端认证解析结果，供连接入口决定写回成功响应、拒绝客户端或继续等待真实业务命令。
/// 分支说明: `Authenticated` 携带要写回客户端的握手响应和可选协议版本；`Rejected` 携带 Redis 风格错误；`NotAuthCommand` 表示首条命令不是认证命令。
pub enum FrontendAuth {
    /// 当前帧是合法认证命令，代理已完成验证并生成应答。
    Authenticated {
        /// 返回客户端的认证成功响应。
        response: Vec<u8>,
        /// `HELLO` 认证同时请求的 RESP 协议版本；普通 `AUTH` 为空。
        protocol_version: Option<u8>,
    },
    /// 当前帧是认证命令，但凭据、参数或协议版本不符合要求。
    Rejected {
        /// 返回客户端的标准认证或参数错误响应。
        response: Vec<u8>,
    },
    /// 当前帧不是前端认证命令，应交回正常命令处理路径。
    NotAuthCommand,
}

/// 业务作用: 按默认 standalone 握手语义校验前端 `AUTH` 或 `HELLO AUTH`，用于 terminate 鉴权模式的入口认证。
/// 参数说明:
/// - `args`: 已解析出的 Redis 命令参数，必须保留客户端原始参数顺序。
/// - `expected_password`: 配置中的前端密码，只用于本地比较，不会继续透传到后端。
///
/// 返回: 返回认证分支；协议参数错误会转换成可写回客户端的 Redis 错误响应。
// 无显式模式信息的调用方沿用 standalone 握手身份，实际比较和 RESP 响应仍复用统一实现。
pub fn handle_frontend_auth(
    args: &[Vec<u8>],
    expected_password: &str,
) -> anyhow::Result<FrontendAuth> {
    handle_frontend_auth_with_mode(args, expected_password, "standalone")
}

/// 业务作用: 校验前端认证命令，并在 `HELLO` 成功时按代理模式生成 `standalone` 或 `cluster` 握手响应。
/// 参数说明:
/// - `args`: 已解析出的 Redis 命令参数，函数只识别 `AUTH` 和 `HELLO`。
/// - `expected_password`: 配置中的前端密码，用常量时间比较避免按首个差异字节提前返回。
/// - `hello_mode`: 写入 `HELLO` 响应的 server mode，cluster-node-proxy 依赖它让客户端进入正确拓扑流程。
///
/// 返回: 返回认证状态；非认证命令返回 `NotAuthCommand`，由调用方决定是否继续或报 NOAUTH。
// 该入口允许 RESP3，只有受限数据面才改用 protocol_policy 入口关闭 RESP3。
pub fn handle_frontend_auth_with_mode(
    args: &[Vec<u8>],
    expected_password: &str,
    hello_mode: &str,
) -> anyhow::Result<FrontendAuth> {
    handle_frontend_auth_with_protocol_policy(args, expected_password, hello_mode, true)
}

/// 业务作用: 校验前端认证命令，并限制当前数据面能够完整兑现的 RESP 协议版本。
/// 参数说明:
/// - `args`: 已解析出的 Redis 命令参数，函数只识别 `AUTH` 和 `HELLO`。
/// - `expected_password`: 配置中的前端密码，用常量时间比较避免按首个差异字节提前返回。
/// - `hello_mode`: 写入 `HELLO` 响应的 server mode。
/// - `allow_resp3`: 当前数据面能否保证后续所有响应遵守 RESP3；不能保证时必须拒绝 `HELLO 3`。
///
/// 返回: 返回认证状态；不受支持的协议版本返回 `NOPROTO`，不会先宣称协商成功再降级。
pub fn handle_frontend_auth_with_protocol_policy(
    args: &[Vec<u8>],
    expected_password: &str,
    hello_mode: &str,
    allow_resp3: bool,
) -> anyhow::Result<FrontendAuth> {
    handle_frontend_auth_with_backend_profile(
        args,
        expected_password,
        "narix",
        hello_mode,
        allow_resp3,
        false,
    )
}

/// 业务作用: 校验前端认证，并按后端兼容档案生成 HELLO 产品身份。
///
/// `hello3_downgrade_to_resp2` 只用于 Pika 这类接受 `HELLO 3`、但在响应中明确
/// 返回 `proto=2` 的后端；协商结果也必须保持为 RESP2，不能把后续响应误解码为 RESP3。
///
/// 参数说明:
/// - `args`: 已解析的 `AUTH` 或 `HELLO` 命令参数。
/// - `expected_password`: 配置的前端认证密码。
/// - `hello_server`: HELLO 响应声明的后端产品身份。
/// - `hello_mode`: HELLO 响应声明的服务模式。
/// - `allow_resp3`: 当前数据面是否完整支持 RESP3 会话。
/// - `hello3_downgrade_to_resp2`: 是否接受 HELLO 3 请求但明确保持 RESP2。
pub fn handle_frontend_auth_with_backend_profile(
    args: &[Vec<u8>],
    expected_password: &str,
    hello_server: &str,
    hello_mode: &str,
    allow_resp3: bool,
    hello3_downgrade_to_resp2: bool,
) -> anyhow::Result<FrontendAuth> {
    let name = resp::command_name(args);
    match name.as_str() {
        "AUTH" => {
            if args.len() != 2 && args.len() != 3 {
                return Ok(FrontendAuth::Rejected {
                    response: resp::err("wrong number of arguments for 'auth' command"),
                });
            }
            let password = args.last().expect("len checked");
            if constant_time_eq(password.as_slice(), expected_password.as_bytes()) {
                Ok(FrontendAuth::Authenticated {
                    response: resp::simple_ok().to_vec(),
                    protocol_version: None,
                })
            } else {
                Ok(FrontendAuth::Rejected {
                    response: resp::wrongpass().to_vec(),
                })
            }
        }
        "HELLO" => handle_hello(
            args,
            expected_password,
            hello_server,
            hello_mode,
            allow_resp3,
            hello3_downgrade_to_resp2,
        ),
        _ => Ok(FrontendAuth::NotAuthCommand),
    }
}

/// 业务作用: 解析 `HELLO 2|3 AUTH <user> <pass> [SETNAME <name>]`，同时确定前端会话的 RESP 协议版本。
/// 参数说明:
/// - `args`: HELLO 命令完整参数，`args[1]` 必须是协议版本。
/// - `expected_password`: 配置中的前端密码，只有 `AUTH` 子选项匹配后才允许通过。
/// - `hello_server`: HELLO 响应声明的后端产品身份。
/// - `hello_mode`: 返回给客户端的代理模式字段，保证客户端看到的模式与 listener 行为一致。
/// - `allow_resp3`: 当前数据面是否能在握手后持续提供 RESP3 语义。
/// - `hello3_downgrade_to_resp2`: 是否复刻后端接受版本 3 请求、实际保持 RESP2 的降级语义。
///
/// 返回: 返回认证状态和协议版本；不支持的 HELLO 选项会返回明确错误而不是透传到后端。
// 必须先完成 AUTH 子句校验再返回 HELLO 元数据，不能让协议协商绕过 terminate 鉴权。
fn handle_hello(
    args: &[Vec<u8>],
    expected_password: &str,
    hello_server: &str,
    hello_mode: &str,
    allow_resp3: bool,
    hello3_downgrade_to_resp2: bool,
) -> anyhow::Result<FrontendAuth> {
    if args.len() < 2 {
        return Ok(FrontendAuth::Rejected {
            response: resp::err("wrong number of arguments for 'hello' command"),
        });
    }

    let (protocol_version, hello_response) = match args[1].as_slice() {
        b"2" => (
            2,
            resp::hello2_response_for_server_mode(hello_server, hello_mode),
        ),
        b"3" if allow_resp3 => (
            3,
            resp::hello3_response_for_server_mode(hello_server, hello_mode),
        ),
        b"3" if hello3_downgrade_to_resp2 => (
            2,
            resp::hello2_response_for_server_mode(hello_server, hello_mode),
        ),
        b"3" => {
            return Ok(FrontendAuth::Rejected {
                response: resp::noproto().to_vec(),
            });
        }
        _ => {
            return Ok(FrontendAuth::Rejected {
                response: resp::noproto().to_vec(),
            });
        }
    };

    let mut idx = 2;
    let mut authenticated = false;
    while idx < args.len() {
        let token = String::from_utf8_lossy(&args[idx]).to_ascii_uppercase();
        match token.as_str() {
            "AUTH" => {
                if idx + 2 >= args.len() {
                    return Ok(FrontendAuth::Rejected {
                        response: resp::err("wrong number of arguments for 'hello auth'"),
                    });
                }
                let password = &args[idx + 2];
                authenticated = constant_time_eq(password.as_slice(), expected_password.as_bytes());
                idx += 3;
            }
            "SETNAME" => {
                if idx + 1 >= args.len() {
                    return Ok(FrontendAuth::Rejected {
                        response: resp::err("wrong number of arguments for 'hello setname'"),
                    });
                }
                idx += 2;
            }
            _ => {
                return Ok(FrontendAuth::Rejected {
                    response: resp::err(&format!("unsupported HELLO option `{token}`")),
                });
            }
        }
    }

    if authenticated {
        Ok(FrontendAuth::Authenticated {
            response: hello_response,
            protocol_version: Some(protocol_version),
        })
    } else {
        Ok(FrontendAuth::Rejected {
            response: resp::wrongpass().to_vec(),
        })
    }
}

/// 业务作用: 对密码字节做固定轮数比较，避免比较时间随第一个不同字节或长度差异明显变化。
/// 参数说明:
/// - `left`: 客户端提交的密码字节。
/// - `right`: 配置中期望的密码字节。
///
/// 返回: 两侧长度和内容完全一致时返回 `true`。
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for idx in 0..max_len {
        let a = left.get(idx).copied().unwrap_or_default();
        let b = right.get(idx).copied().unwrap_or_default();
        diff |= usize::from(a ^ b);
    }
    diff == 0
}
