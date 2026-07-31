use crate::backend::BackendProfile;
use crate::cluster::plan::{PipelinePlan, PlannedRoute};
use crate::cluster::spec::KeylessPolicy;
use crate::resp;

/// 业务作用: 为无需访问后端的 Cluster 命令生成本地 RESP 响应。
/// 参数说明:
/// - `backend_profile`: 决定本地 HELLO 等响应使用的后端产品兼容语义。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `route`: 命令解析得到的路由类型，决定单节点、分片、聚合或拒绝路径。
///
pub fn local_response<S: AsRef<[u8]>>(
    backend_profile: BackendProfile,
    args: &[S],
    route: &PlannedRoute,
) -> Option<Vec<u8>> {
    match route {
        PlannedRoute::LocalError(reason) => {
            if reason.contains("CROSSSLOT") {
                Some(resp::crossslot(reason))
            } else {
                Some(resp::err(reason))
            }
        }
        PlannedRoute::Keyless(KeylessPolicy::Local) => local_keyless(backend_profile, args),
        _ => None,
    }
}

/// 业务作用: 生成与输入命令槽位一一对应的本地响应集合。
/// 参数说明:
/// - `backend_profile`: 决定每条本地响应使用的后端产品兼容语义。
/// - `commands`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
/// - `plan`: 单条命令的路由计划，描述目标节点、聚合方式和响应位置。
///
pub fn local_response_slots<S: AsRef<[u8]>>(
    backend_profile: BackendProfile,
    commands: &[Vec<S>],
    plan: &PipelinePlan,
) -> Vec<Option<Vec<u8>>> {
    plan.entries
        .iter()
        .map(|entry| local_response(backend_profile, &commands[entry.sequence], &entry.route))
        .collect()
}

/// 业务作用: 处理无路由键命令的本地兼容响应或明确拒绝。
/// 参数说明:
/// - `backend_profile`: 决定无键命令本地响应使用的产品语义。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
fn local_keyless<S: AsRef<[u8]>>(backend_profile: BackendProfile, args: &[S]) -> Option<Vec<u8>> {
    let name = resp::command_name(args);
    match name.as_str() {
        "PING" => {
            if args.len() == 2 {
                Some(resp::bulk(args[1].as_ref()))
            } else if args.len() == 1 {
                Some(resp::pong().to_vec())
            } else {
                Some(resp::err("wrong number of arguments for 'ping' command"))
            }
        }
        "AUTH" | "SELECT" => Some(resp::simple_ok().to_vec()),
        "HELLO" => Some(hello_response(backend_profile, args)),
        "CLIENT" => Some(client_response(args)),
        "COMMAND" => Some(resp::array(&[])),
        "INFO" => Some(info_response(args)),
        "QUIT" => Some(resp::simple_ok().to_vec()),
        _ => None,
    }
}

/// 业务作用: 生成 INFO 本地响应，用于让 cluster-single-entry 对自动探测型客户端保持单节点外观。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
// 生成 INFO 本地响应，用于让 cluster-single-entry 对自动探测型客户端保持单节点外观。
fn info_response<S: AsRef<[u8]>>(args: &[S]) -> Vec<u8> {
    if args.len() > 2 {
        return resp::err("wrong number of arguments for 'info' command");
    }
    if args.len() == 2 && !args[1].as_ref().eq_ignore_ascii_case(b"cluster") {
        return resp::err("only INFO cluster is supported by cluster proxy");
    }
    resp::bulk(b"# Cluster\r\ncluster_enabled:0\r\n")
}

/// 业务作用: 按协商协议和后端产品身份生成 Cluster 入口 HELLO 响应。
/// 参数说明:
/// - `backend_profile`: 决定 HELLO 响应中的产品身份。
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
fn hello_response<S: AsRef<[u8]>>(backend_profile: BackendProfile, args: &[S]) -> Vec<u8> {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hello' command");
    }
    let response = match args[1].as_ref() {
        b"2" => {
            resp::hello2_response_for_server_mode(backend_profile.hello_server_name(), "standalone")
        }
        // Cluster 单入口的数据面使用共享 RESP2 后端 lane。若接受 HELLO 3，后续 map/set
        // 等聚合响应仍可能以 RESP2 返回，会让客户端按错误类型解码，因此在入口明确拒绝。
        b"3" => return resp::noproto().to_vec(),
        _ => return resp::noproto().to_vec(),
    };

    let mut idx = 2;
    while idx < args.len() {
        let token = String::from_utf8_lossy(args[idx].as_ref()).to_ascii_uppercase();
        match token.as_str() {
            "AUTH" => {
                if idx + 2 >= args.len() {
                    return resp::err("wrong number of arguments for 'hello auth'");
                }
                idx += 3;
            }
            "SETNAME" => {
                if idx + 1 >= args.len() {
                    return resp::err("wrong number of arguments for 'hello setname'");
                }
                idx += 2;
            }
            _ => return resp::err(&format!("unsupported HELLO option `{token}`")),
        }
    }
    response
}

/// 业务作用: 生成 CLIENT 子命令在代理本地终止时的兼容响应。
/// 参数说明:
/// - `args`: Redis 命令或外部命令参数，调用方按 RESP 或系统命令语义传入。
///
fn client_response<S: AsRef<[u8]>>(args: &[S]) -> Vec<u8> {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'client' command");
    }
    let subcommand = String::from_utf8_lossy(args[1].as_ref()).to_ascii_uppercase();
    match subcommand.as_str() {
        "SETNAME" => {
            if args.len() == 3 {
                resp::simple_ok().to_vec()
            } else {
                resp::err("wrong number of arguments for 'client setname' command")
            }
        }
        "GETNAME" => {
            if args.len() == 2 {
                resp::null_bulk().to_vec()
            } else {
                resp::err("wrong number of arguments for 'client getname' command")
            }
        }
        "ID" => {
            if args.len() == 2 {
                resp::integer(0)
            } else {
                resp::err("wrong number of arguments for 'client id' command")
            }
        }
        "SETINFO" => {
            if args.len() != 4 {
                resp::err("wrong number of arguments for 'client setinfo' command")
            } else if args[2].as_ref().eq_ignore_ascii_case(b"LIB-NAME")
                || args[2].as_ref().eq_ignore_ascii_case(b"LIB-VER")
            {
                resp::simple_ok().to_vec()
            } else {
                resp::err("unsupported CLIENT SETINFO option; expected LIB-NAME or LIB-VER")
            }
        }
        _ => resp::err(&format!(
            "CLIENT {subcommand} is not supported by cluster proxy"
        )),
    }
}
