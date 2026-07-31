# 安全策略

## 支持版本

当前支持版本为 `1.0.0`。安全修复进入当前维护分支，并在完成回归、只读门禁和协调安全检查后发布修订版本。

## 问题报告

请私下向项目维护者报告安全问题。以下问题不得提交公开 issue：

- 认证绕过；
- `read_only` 绕过；
- 可能写入错误后端数据源的命令路由；
- 故障转移脑裂或 fencing 绕过；
- 日志或指标泄露密钥；
- 无界缓冲区、任务或连接导致的拒绝服务路径。

## 安全默认值

- 管理 API 默认要求 token；只有可信测试网络可显式设置 `admin.allow_unauthenticated: true`。
- `ha.auto_failover` 默认开启，配套安全边界：单实例由进程内锁串行且同一组后端只允许一个承担控制面的 NARIX 进程在线（`ha.observer: true` 的观察者节点是唯一豁免——它不发送任何后端控制命令）；多实例必须配置协调器（组内置协调集群时强制 `proxy-cluster`）；软隔离在集群部署提主前等满一个 `route_guard_timeout`；需要零脑裂窗口时配置 `fencing.mode=external-command`。
- `ha.coordinator.mode=local-file` 仅用于同机进程互斥；跨机器部署优先使用启用双向 TLS 的 `proxy-cluster` 多数派协调，也可以在确有独立高可用 Redis 时使用 `redis` 租约协调器。
- `read_only=true` 会拒绝写命令，并按失败关闭原则拒绝未知命令或隧道绑定命令。
