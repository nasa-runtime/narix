use std::io;

/// 业务作用: 使用项目固定的协议编译器生成协调器 gRPC 客户端和服务端代码，避免部署主机是否预装编译器影响构建结果。
/// 参数说明: 无。
/// 返回: 协议文件解析或代码生成失败时返回输入输出错误，使构建立即停止而不是留下不完整接口。
fn main() -> io::Result<()> {
    nagrpc_build::compile("proto/coordinator.proto").map_err(io::Error::other)
}
