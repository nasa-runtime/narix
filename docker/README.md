# NARIX

[English](https://github.com/nasa-runtime/narix/blob/release/docker/README.en.md)

**高性能 RESP2/RESP3 多后端代理，突出能力是 Dragonfly HA。** NARIX 为 Redis、Dragonfly、Valkey 等后端提供稳定业务入口，支持固定后端、Sentinel、Cluster、节点代理和托管复制 HA；同一进程可运行多个隔离的代理项。

Dragonfly HA 将主路由、故障切换、副本收敛和快照责任纳入一个控制面：先验证控制权和隔离条件，再执行切主，确认角色与复制状态后开放路由。多节点持久化可以让主节点专注服务、热备保持无快照负担、其它合格副本承担快照。至少三个 NARIX 协调成员可通过多数派串行化动作；跨主机使用 mTLS 和固定成员身份。

复制仍然是异步的，HA 不保证零数据丢失。生产自动故障转移需要可靠的外部 fencing；默认 `manual` 不能提供物理隔离保证。NARIX 不内置业务 VIP，也不把 Dragonfly 变成标准 Redis Cluster。完整[架构、版本下限与安全边界](https://github.com/nasa-runtime/narix/blob/release/README.md)适用于容器部署。

## 镜像

| 项目 | 值 |
| --- | --- |
| Docker Hub | `nasaruntime/narix` |
| 版本标签 | `1.0.0`；`latest` 随发布更新，固定内容使用 digest |
| 目标平台 | `linux/amd64`、`linux/arm64` |
| 用户 | `10001:10001`，非 root |
| 工作目录 / 程序 | `/app` / `/app/narix` |
| 许可证 | Apache-2.0 |

[Dockerfile](https://github.com/nasa-runtime/narix/blob/release/docker/Dockerfile) 使用固定 digest 的 Rust/Debian 多阶段构建，保留吞吐导向的 release 优化、thin LTO 和单 codegen unit。运行层包含程序、CA 证书、许可证和引导配置；不包含编译器、源码或依赖缓存。

需要镜像仓库已提供所选标签，或先在各 Docker 宿主从本仓库构建同名本地镜像：

```sh
docker build -f docker/Dockerfile -t nasaruntime/narix:1.0.0 .
```

## 通用准备

下列命令运行的是 **NARIX**，连接已经部署好的后端。选择需要的场景，不要原样同时启动占用相同端口的示例。准备 `deploy/<场景>/config/narix.yml`、`data` 和 `logs` 目录。配置须允许 UID 10001 读取，数据和日志须允许 UID/GID `10001:10001` 写入；凭据只对该用户或受控组开放。仅调整专用部署目录的所有权。

镜像默认 `APP_CONFIG_FILE=/etc/narix/narix.yml`，文件日志默认写入 `/usr/local/logs/narix`。每个示例均保留配置、状态、日志与宿主时区挂载。只读时区挂载要求 Docker 宿主存在 `/etc/localtime`；Docker Desktop 还须允许访问挂载来源。以下路径按 `application.name: narix` 设置。协调数据放在 `/var/lib/narix` 下，证书放在 `/etc/narix/tls` 下。每个协调成员使用不同的数据目录，不能共享或丢弃共识状态。

首次创建共用容器网络，已存在时省略：

```sh
docker network create narix-net
```

后端容器接入同一网络，或使用可达的外部地址。容器内 `127.0.0.1` 不是宿主机。各 YAML 的管理监听设为 `0.0.0.0:18080`，业务监听主机设为 `0.0.0.0`；除特别说明外，业务端口统一改为 `6379`，替换模板中的 `7001`。填写真实后端地址、前端密码与管理 token。下方业务端口映射面向宿主网卡，应使用受信网卡或防火墙限制来源；管理端口只映射到宿主回环地址。

外部文件优先级为 `--config` → `APP_CONFIG_FILE` → `NARIX_CONFIG`；只用兼容变量时，应通过 `-e APP_CONFIG_FILE=` 清空镜像默认入口。挂载整个配置目录，保证同目录原子 rename 的更新可见；单文件挂载和 Kubernetes `subPath` 可能保留旧文件。热更默认每秒观察，无效候选保留原配置；环境变量变化需要重建容器。

## 各场景启动命令

### 固定 Redis / Valkey 等后端

使用 [standalone.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/standalone.yml)，设置 `mode: standalone`、相应 `backend_profile`、`host` 和 `port`。

```sh
docker run -d --name narix-standalone --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/standalone/config:/etc/narix:ro" \
  -v "$PWD/deploy/standalone/data:/var/lib/narix" \
  -v "$PWD/deploy/standalone/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### 固定 Dragonfly 后端

同样使用 [standalone.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/standalone.yml)，设 `backend_profile: dragonfly`、`mode: standalone`，指向一个固定数据节点。此场景不执行 HA 控制动作。

```sh
docker run -d --name narix-dragonfly --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/dragonfly/config:/etc/narix:ro" \
  -v "$PWD/deploy/dragonfly/data:/var/lib/narix" \
  -v "$PWD/deploy/dragonfly/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### Sentinel 动态主节点

使用 [sentinel.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/sentinel.yml)，填写 `sentinel_nodes`、`sentinel_master_name` 及 Sentinel / 数据节点各自的认证信息。NARIX 必须能访问所有 Sentinel 和可能成为主节点的地址；本容器无需映射 Sentinel 的 26379 端口。

```sh
docker run -d --name narix-sentinel --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/sentinel/config:/etc/narix:ro" \
  -v "$PWD/deploy/sentinel/data:/var/lib/narix" \
  -v "$PWD/deploy/sentinel/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### 标准 Cluster 单入口

使用 [cluster.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/cluster.yml)，填写种子 `nodes`。NARIX 必须能访问发现的全部节点；一个业务映射由 NARIX 完成槽位路由。此模式不适用于 Dragonfly。

```sh
docker run -d --name narix-cluster --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/cluster/config:/etc/narix:ro" \
  -v "$PWD/deploy/cluster/data:/var/lib/narix" \
  -v "$PWD/deploy/cluster/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### Cluster 每节点代理

使用 [cluster-node-proxy.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/cluster-node-proxy.yml)，保留示例 `node_proxies[].listen` 为 17001–17003。`cluster_node_advertise_host` 必须是客户端可达的宿主地址；各 `advertise_port` 与宿主端口映射一致。真实拓扑有更多节点时，每个节点都要配置并映射对应端口。

```sh
docker run -d --name narix-node-proxy --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 17001-17003:17001-17003 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/node-proxy/config:/etc/narix:ro" \
  -v "$PWD/deploy/node-proxy/data:/var/lib/narix" \
  -v "$PWD/deploy/node-proxy/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### Dragonfly HA：单控制实例

使用 [replication-ha-dragonfly.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/replication-ha-dragonfly.yml)，设 `coordination.enabled: false`、`ha.coordinator.mode: none`，清空 `owner_id`、`lease_path` 等其它协调方式字段。该拓扑只能运行一个控制进程。填写全部数据节点的 client / replication 地址与可靠 fencing 配置。启用快照编排时，快照磁盘必须挂在 Dragonfly 自己的容器中。Redis / Valkey / Redict / KeyDB / Kvrocks HA 可用同一命令形态，更换对应产品配置；Kvrocks 必须关闭 NARIX 持久化编排。

```sh
docker run -d --name narix-ha --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/ha/config:/etc/narix:ro" \
  -v "$PWD/deploy/ha/data:/var/lib/narix" \
  -v "$PWD/deploy/ha/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### Dragonfly HA：同机文件租约

两份配置设置 `ha.coordinator.mode: local-file`、不同的 `owner_id`、相同的 `lease_path: /var/lib/narix/leases/dragonfly.lease`，保持 `coordination.enabled: false`。先创建 UID 10001 可写的 `deploy/shared-leases`。两个容器必须在同一宿主机，共享同一个支持文件锁的租约目录；不要用不同目录或跨机文件系统冒充共享锁。数据与日志目录仍各自独立。

```sh
docker run -d --name narix-ha-1 --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 7001:6379 -p 127.0.0.1:18081:18080 \
  -v "$PWD/deploy/local-file-1/config:/etc/narix:ro" \
  -v "$PWD/deploy/local-file-1/data:/var/lib/narix" \
  -v "$PWD/deploy/local-file-1/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  -v "$PWD/deploy/shared-leases:/var/lib/narix/leases" \
  nasaruntime/narix:1.0.0
```

```sh
docker run -d --name narix-ha-2 --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 7002:6379 -p 127.0.0.1:18082:18080 \
  -v "$PWD/deploy/local-file-2/config:/etc/narix:ro" \
  -v "$PWD/deploy/local-file-2/data:/var/lib/narix" \
  -v "$PWD/deploy/local-file-2/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  -v "$PWD/deploy/shared-leases:/var/lib/narix/leases" \
  nasaruntime/narix:1.0.0
```

### Dragonfly HA：独立 Redis 租约

设置 `ha.coordinator.mode: redis`，填写独立租约服务的 `redis_addr`、认证与同组共用的 `redis_key`，每个实例使用不同 `owner_id`；清空 `lease_path`，关闭内置协调。租约 Redis 不能是本组被管理的数据节点。其它宿主可运行同形态命令，但配置身份与数据目录必须独立；该方案不提供内置共识日志。

```sh
docker run -d --name narix-ha-redis-lease --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/redis-lease/config:/etc/narix:ro" \
  -v "$PWD/deploy/redis-lease/data:/var/lib/narix" \
  -v "$PWD/deploy/redis-lease/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### Dragonfly HA：三节点协调集群

从 [replication-ha-dragonfly-proxy-cluster.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/replication-ha-dragonfly-proxy-cluster.yml) 准备三份配置。以下三条命令分别在三个宿主机运行，各主机创建自己的 `narix-net`。成员 `node_id`、本机证书与数据目录不同；`cluster_id`、成员表、bootstrap 身份与 HA group 保持一致。`advertise` / `peers[].address` 使用其它宿主可达的地址和 19090 端口；跨主机启用 mTLS、真实指纹与证书名称。`coordination.data_dir` 位于 `/var/lib/narix` 下，不能放在容器临时层。配置目录同时承载 `tls/`，无需再挂单个证书文件。仅向受信成员开放 19090；业务入口由外部四层负载均衡汇聚。

```sh
docker run -d --name narix-1 --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 -p 19090:19090 \
  -v "$PWD/deploy/coordination-1/config:/etc/narix:ro" \
  -v "$PWD/deploy/coordination-1/data:/var/lib/narix" \
  -v "$PWD/deploy/coordination-1/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

```sh
docker run -d --name narix-2 --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 -p 19090:19090 \
  -v "$PWD/deploy/coordination-2/config:/etc/narix:ro" \
  -v "$PWD/deploy/coordination-2/data:/var/lib/narix" \
  -v "$PWD/deploy/coordination-2/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

```sh
docker run -d --name narix-3 --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 -p 19090:19090 \
  -v "$PWD/deploy/coordination-3/config:/etc/narix:ro" \
  -v "$PWD/deploy/coordination-3/data:/var/lib/narix" \
  -v "$PWD/deploy/coordination-3/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### 只读查询 / HA 旁路观察

在相应模式配置中设 `read_only: true`。若已有控制面，只观察 Dragonfly HA 时再设 `ha.observer: true`，禁止该实例执行控制写；只读业务门禁和控制观察模式是两个独立设置。保留认证与最小权限。示例以宿主 6380 提供查询入口。

```sh
docker run -d --name narix-readonly --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6380:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/readonly/config:/etc/narix:ro" \
  -v "$PWD/deploy/readonly/data:/var/lib/narix" \
  -v "$PWD/deploy/readonly/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

### 一个进程承载多个代理项

在同一 YAML 的 `proxies` 中配置不同名称与监听端口。此例三个代理项分别监听 6379、6380、6381；分别填写各自后端与访问策略。每个真实 listener 都须发布，管理端只需一个映射。

```sh
docker run -d --name narix-multi --network narix-net \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379-6381:6379-6381 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/multi/config:/etc/narix:ro" \
  -v "$PWD/deploy/multi/data:/var/lib/narix" \
  -v "$PWD/deploy/multi/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  nasaruntime/narix:1.0.0
```

## 挂载与运维选项

| 挂载 | 用途 |
| --- | --- |
| `/etc/narix:ro` | 完整 YAML / TLS 目录，支持原子替换文件 |
| `/var/lib/narix` | 协调任期、日志与状态快照；YAML 路径需位于该目录下 |
| `/usr/local/logs/narix` | 文件日志，容器重建后保留 |
| `/etc/localtime:ro` | 使用宿主时区 |
| 共用租约目录 | 仅 `local-file` 需要，同宿主控制实例共享 |
| fencing 程序与凭据 | 使用外部隔离命令时，按配置路径额外提供只读挂载 |

挂载不会改写 YAML 路径。租约路径若不在 `/var/lib/narix` 下，必须额外挂载对应可写目录。Dragonfly 快照和其它后端数据必须挂载到**数据容器**，NARIX 状态卷不能替代；NARIX 的 checkpoint 验证并不对应另一个本地数据备份目录。改变应用名或 `log.path` 后需同步调整日志挂载。覆盖目录可加 `-e NARIX_LOG_PATH=/usr/local/logs/narix`；显式 `-e NARIX_LOG_PATH=` 才是仅输出控制台。

日志默认单文件 100MB、归档总量 1GB、保留 7 天，不逐条输出正常请求。Docker stdout 可统一配置容量，或加 `--log-driver json-file --log-opt max-size=10m --log-opt max-file=3`。按需增加 `--read-only --cap-drop ALL --security-opt no-new-privileges`；仍须满足全部配置写入路径与外部 fencing 程序要求。Linux 宿主网络可替换桥接网络与 `-p` 参数，此时 listener 直接绑定宿主，必须避免地址冲突。

## 健康与关闭

存活探针使用 `GET /health`，业务接流判断使用 `GET /ready`。状态、指标与配置接口需要 Bearer token。保护管理与协调接口；镜像不预设固定探针命令，也不自动映射未声明的协调或业务端口。

SIGTERM 先关闭准入再排空。70 秒停止宽限期应大于 `max(10000, graceful_shutdown_timeout_ms 最大值) + 5000` 毫秒并留出余量，业务预算增加时同步调大。升级保留状态卷和稳定成员身份；配置、TLS 与 HA 异常仍须遵守对应保护门禁。

更多内容：[Container deployment](https://github.com/nasa-runtime/narix/blob/release/docs/container-deployment.md) · [Configuration](https://github.com/nasa-runtime/narix/blob/release/docs/configuration.md) · [Operations](https://github.com/nasa-runtime/narix/blob/release/docs/operations.md) · [Source](https://github.com/nasa-runtime/narix)
