# 容器部署与镜像分发

[English](container-deployment.en.md)

NARIX 镜像命名为 `nasaruntime/narix`，版本标签为 `1.0.0`。Docker Hub 展示内容见 [镜像概览](../docker/README.md)。版本标签用于固定发布，digest 用于固定内容；`latest` 可以变化。发布目标平台为 `linux/amd64` 和 `linux/arm64`。

## 配置与启动

镜像工作目录是 `/app`，程序是 `/app/narix`，UID/GID 为 `10001:10001`。随镜像提供的 `zcf/application.yml` 是基础配置，不含可直接投入业务的代理项与凭据；镜像默认 `APP_CONFIG_FILE=/etc/narix/narix.yml`，可用环境变量覆盖为其它部署文件。`--config` 优先于该变量，`NARIX_CONFIG` 保留为低优先级兼容入口；单独使用时须清空镜像的 `APP_CONFIG_FILE`。

下面是固定 Redis 后端的最小文件。将地址与凭据改为实际值；Dragonfly HA 应从[完整 HA 配置](configuration/replication-ha-dragonfly.yml)或[协调集群配置](configuration/replication-ha-dragonfly-proxy-cluster.yml)开始。

```yaml
application:
  name: narix
admin:
  listen: 0.0.0.0:18080
  token: replace-with-a-long-random-admin-token
reload:
  enabled: true
  poll_interval_ms: 1000
proxies:
  - name: redis-primary
    backend_profile: redis
    mode: standalone
    listen_host: 0.0.0.0
    listen: 6379
    host: redis
    port: 6379
    auth_mode: terminate
    frontend_password: replace-with-a-long-random-client-password
```

若后端要求认证，同时设置 `username`、`password`。示例 `redis` 必须能在容器网络内解析；连接其它容器时先创建专用 Docker 网络，将后端与 NARIX 都接入该网络。连接宿主机时使用部署环境实际支持的地址，不使用容器回环地址。

把配置保存为 `deploy/standalone/config/narix.yml`，使用 [Docker Hub README 中对应场景的启动命令](../docker/README.md#各场景启动命令)。示例发布业务端口，管理端口仅向宿主回环地址开放；业务访问应按受控网络入口限制。标准 Cluster 的节点地址、节点代理公布地址和重定向地址都必须对对应客户端可达，单个端口映射不能代替拓扑规划。

挂载整个配置目录，目录允许 UID 10001 遍历、文件允许其读取。配置可能含密码，应限制为该用户或受控组可读，不能写进镜像、构建参数或标签。使用同目录原子 rename 更新配置；Docker 单文件 bind mount 与 Kubernetes `subPath` 可能保留旧 inode，不能用于该热更方式。

文件路径启动时固定。文件缺失、坏 YAML、类型错误、非法轮询周期或资源准备失败会拒绝整份候选并保留原配置。改变进程环境变量或 `APP_CONFIG_FILE` 路径需要重建容器；修改未被环境覆盖的字段可经文件轮询、SIGHUP 或带认证的 `/config/reload` 生效。复杂 HA 变更还必须通过领域安全门禁，详见[配置参考](configuration.md)。

## 日志、状态与卷

默认保留文件日志挂载，显式 `NARIX_LOG_PATH=` 才切换为仅输出控制台。文件日志使用 `NARIX_LOG_PATH='/usr/local/logs/${application.name}'`，并将专用可写目录挂载到 `/usr/local/logs/narix`；不要让宿主 shell 提前展开 `${application.name}`。例如宿主日志目录 `/srv/logs/narix` 可作为该卷的宿主路径。容器看到的是容器内路径，不能直接使用未挂载的宿主绝对路径。

日志卷与数据卷应由运行用户或受控组拥有，不要递归更改共享宿主目录的所有权。文件轮转默认 100MB、归档总量 1GB、保留 7 天。运行平台还应限制 stdout 日志容量。

| 用途 | 挂载约束 |
| --- | --- |
| 部署配置、证书、私钥 | 整个目录只读，UID 10001 可读 |
| 文件日志 | 示例保留独立可写目录 |
| `coordination.data_dir` | 每个协调成员独立持久卷，重启后保留 |
| `ha.coordinator.lease_path` | 按实际协调文件路径提供可写挂载；只用于同机协调 |
| `/etc/localtime` | 宿主时区文件只读挂载，源文件必须存在 |
| Dragonfly 快照 | 挂在 Dragonfly 数据容器中，NARIX 仅编排责任 |

checkpoint 指数据节点的原生持久化点，其数据盘属于后端容器；NARIX 的协调快照与日志保存在 `coordination.data_dir`。启动前按实际 YAML 核对文件路径，不能只给日志目录写权限。多控制实例共享同组后端时必须有共同协调机制；`coordinator: none` 只允许一个控制进程在线。生产多机优先至少三个固定成员的 `proxy-cluster`，跨机启用 mTLS 与证书身份绑定，另开放配置指定的协调端口。可靠外部 fencing 所需程序、证书或受控代理必须在运行环境中可用；镜像不内置云厂商隔离工具。

## 健康与关闭

存活探针使用 `/health`，就绪探针与负载均衡使用 `/ready`。`/status`、`/metrics` 和配置接口需要 Bearer token。TCP 接通只证明监听存在，不证明 HA 写路由安全。镜像不预置依赖固定管理地址或额外 HTTP 客户端的 HEALTHCHECK，由部署平台按实际监听设置探针。

默认空代理的关闭预算为 15 秒；非空配置为 `max(10000, 所有 graceful_shutdown_timeout_ms 的最大值) + 5000` 毫秒。`--stop-timeout 70` 或 Kubernetes `terminationGracePeriodSeconds: 70` 应覆盖该预算并留余量，业务超时更长时同步调大。SIGTERM 先停止准入再排空，超时或资源关闭失败为非零退出，不能当作正常完成。

## 构建与发布到 Docker Hub

仓库提供 `docker-publish.yml`，在 GitHub Release 发布完成后上传镜像。设置仓库变量 `DOCKERHUB_USERNAME` 与密钥 `DOCKERHUB_TOKEN`，从 `release` 发起工作流，`release_ci_run_id` 填该 release 提交上成功完成的 CI 编号。工作流复验版本 tag 与两个分支，使用原生 runner 分别构建和启动 amd64 / arm64 镜像，核对归档身份后上传同一份内容；先发布版本清单和完整 Docker Hub README，再移动 `latest`。已有版本若内容不同会拒绝覆盖。



从仓库根目录构建本机架构镜像：

```sh
docker build --file docker/Dockerfile --tag narix:1.0.0 .
docker run --rm narix:1.0.0 --version
```

Dockerfile 固定基础镜像 digest，使用 `Cargo.lock` 从 crates.io 获取依赖，标准 release 优化保持 thin LTO、单 codegen unit，随后裁剪符号。运行层不携带构建缓存。`.dockerignore` 只允许产品构建材料进入上下文，宿主配置和凭据不得放入这些路径。

对外发布顺序是：提交并合并目标分支、推送、远端 CI 全部成功、核对分支与版本 tag 的提交 SHA、获得明确发布授权，再上传镜像。不能在工作区仍有未提交产品变化时发布。`master`、`release` 和版本 tag 应指向同一发布提交；CI 的 SHA 必须与镜像 `org.opencontainers.image.revision` 一致。

取得上述 SHA 后，使用具有该仓库写权限的 Docker Hub 账户登录，将它写入 `SOURCE_REVISION`。不要把访问 token 放在命令参数、文件或构建上下文中。

```sh
docker login
export SOURCE_REVISION=replace-with-the-full-verified-git-sha
docker buildx build --file docker/Dockerfile \
  --platform linux/amd64,linux/arm64 \
  --build-arg APP_VERSION=1.0.0 \
  --build-arg SOURCE_REVISION="$SOURCE_REVISION" \
  --tag nasaruntime/narix:1.0.0 \
  --provenance=true --sbom=true --push .
docker buildx imagetools inspect nasaruntime/narix:1.0.0
```

发布账户须确认两种架构都有对应运行镜像，分别核对启动、配置加载、标签、许可证和平台运行条件；不能用一个架构的运行结果声称两个都已通过。启用 provenance/SBOM 时清单可能附带证明描述符，它们不是运行平台。版本标签若已存在且 digest 不同，应停止并选择新的补丁版本，不覆盖既有发布。只有版本内容核对无误后才移动 `latest`：

```sh
docker buildx imagetools create \
  --tag nasaruntime/narix:latest nasaruntime/narix:1.0.0
docker buildx imagetools inspect nasaruntime/narix:latest
```

在 Docker Hub 仓库说明中使用 [docker/README.md](../docker/README.md) 或 [英文版](../docker/README.en.md)，正文保持不超过 25000 UTF-8 字节。短说明可用 `High-performance RESP2/RESP3 proxy featuring Dragonfly HA`。上传后回读仓库说明、版本清单、两个平台的标签与 digest，确认来源链接、配置入口与镜像实际内容一致。以上命令是发布入口，不代表镜像已经上传。
