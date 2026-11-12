# 当前能力与合同

[English](CHANGELOG.en.md)

NARIX 提供 RESP2/RESP3 前置代理、托管复制 HA 控制面，以及面向开发和运维的 readonly 查询入口。Dragonfly HA 是其特色模式，包含受控切主、安全路由、副本收敛和持久化责任编排。

- [运行架构与后端版本边界](README.md)说明五种代理模式、独立的 HA 与持久化下限及原生能力约束。
- [Dragonfly HA 特色模式](README.md#dragonfly-hanarix-的特色模式)说明控制权威、数据路由和持久化交接如何协作。
- [配置参考](docs/configuration.md)说明配置来源、环境变量、认证、运行设施和超时预算。
- [运维指南](docs/operations.md)说明保护状态、动作权威、持久化责任与事务配置发布。
- [命令矩阵](command-matrix.md)说明命令路由、连接语义和只读拒绝边界。

代码沿革由 Git 提交记录维护；上述入口描述当前可用合同。
