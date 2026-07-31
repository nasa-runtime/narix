use std::io;

/// 业务作用: 使用项目固定的协议编译器生成协调器 gRPC 客户端和服务端代码，避免部署主机是否预装编译器影响构建结果。
/// 参数说明: 无。
/// 返回: 协议文件解析或代码生成失败时返回输入输出错误，使构建立即停止而不是留下不完整接口。
fn main() -> io::Result<()> {
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .map_err(|error| io::Error::other(format!("定位协议编译器失败: {error}")))?;
    let mut prost_config = tonic_prost_build::Config::new();
    prost_config.protoc_executable(protoc);
    tonic_prost_build::configure().compile_with_config(
        prost_config,
        &["proto/coordinator.proto"],
        &["proto"],
    )
}
