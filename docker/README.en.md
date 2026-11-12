# NARIX

[简体中文](https://github.com/nasa-runtime/narix/blob/release/docker/README.md)

**A high-performance RESP2/RESP3 proxy for multiple backends, featuring Dragonfly HA.** NARIX provides stable client endpoints for Redis, Dragonfly, Valkey and other backends, with standalone, Sentinel, Cluster, node-proxy and managed replication HA modes. Each process can host multiple isolated proxy entries.

Dragonfly HA combines primary routing, failover, replica convergence and snapshot responsibility. It verifies authority and fencing prerequisites before promotion, then opens routes only after role and replication evidence is sufficient. Multi-node persistence can keep snapshots off the primary and a hot standby while assigning them to other eligible replicas. Three or more NARIX coordinator members serialize actions through a majority; cross-host coordination uses mTLS and fixed member identities.

Replication remains asynchronous: HA does not guarantee zero data loss. Production automatic failover needs reliable external fencing; the default `manual` mode does not physically isolate nodes. NARIX does not provide a business VIP or turn Dragonfly into a standard Redis Cluster. The full [architecture, minimum versions and safety boundaries](https://github.com/nasa-runtime/narix/blob/release/README.en.md) apply to container deployments.

## Image

| Item | Value |
| --- | --- |
| Docker Hub | `nasaruntime/narix` |
| Version tag | `1.0.0`; `latest` follows releases, while a digest pins content |
| Target platforms | `linux/amd64`, `linux/arm64` |
| User | Non-root `10001:10001` |
| Working directory / program | `/app` / `/app/narix` |
| License | Apache-2.0 |

The [Dockerfile](https://github.com/nasa-runtime/narix/blob/release/docker/Dockerfile) uses digest-pinned Rust/Debian multi-stage builds with throughput-oriented release optimization, thin LTO and one codegen unit. The runtime contains the binary, CA certificates, license and bootstrap configuration, without a compiler, source tree or dependency cache.

Use a tag available in the image registry, or first build a local image with the same name on each Docker host:

```sh
docker build -f docker/Dockerfile -t nasaruntime/narix:1.0.0 .
```

## Shared preparation

All commands below run **NARIX**, connecting to already deployed backends. Choose one scenario; do not start overlapping examples unchanged. Prepare `deploy/<scenario>/config/narix.yml`, `data` and `logs`. Configuration must be readable by UID 10001; data and logs must be writable by UID/GID `10001:10001`. Restrict secrets to that user or a controlled group. Only adjust ownership of dedicated deployment directories.

The image defaults to `APP_CONFIG_FILE=/etc/narix/narix.yml`; file logs default to `/usr/local/logs/narix`. Every example mounts configuration, state, logs and the host time zone. The read-only `/etc/localtime` source must exist on the Docker host. Docker Desktop must allow access to bind-mount sources. Keep `application.name: narix` for these paths. Put coordination storage under `/var/lib/narix` and certificates under `/etc/narix/tls`. Each coordinator member owns a different state directory; never share or discard its consensus state.

Create a shared container network once (skip if it already exists):

```sh
docker network create narix-net
```

Attach backend containers to this network or supply reachable external addresses. Container `127.0.0.1` is not the host. In each YAML, set administration to `0.0.0.0:18080`; set business listeners to `0.0.0.0`. Unless stated otherwise, use business port `6379`, replacing templates' `7001`. Set real backend addresses, a frontend password and an administration token. Business mappings below accept connections on host interfaces; use a trusted interface or firewall. Administration is bound to host loopback.

External-file priority is `--config` → `APP_CONFIG_FILE` → `NARIX_CONFIG`. To use the compatibility variable alone, clear the image's `APP_CONFIG_FILE` with `-e APP_CONFIG_FILE=`. Mount the whole configuration directory so atomic rename updates remain visible; single-file mounts and Kubernetes `subPath` can retain old files. Polling defaults to one second. Invalid candidates preserve the active configuration. Environment changes require container recreation.

## Commands by scenario

### Fixed Redis / Valkey and other backends

Use [standalone.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/standalone.yml) with `mode: standalone`, the appropriate `backend_profile`, `host` and `port`.

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

### Fixed Dragonfly backend

Use [standalone.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/standalone.yml) with `backend_profile: dragonfly` and `mode: standalone`, targeting a fixed data node. This scenario performs no HA control actions.

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

### Sentinel primary discovery

Use [sentinel.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/sentinel.yml) and configure `sentinel_nodes`, `sentinel_master_name`, and separate Sentinel/data-node credentials. NARIX must reach all Sentinels and potential primaries. Do not publish Sentinel port 26379 from this container.

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

### Standard Cluster, single entry point

Use [cluster.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/cluster.yml) with seed `nodes`. NARIX must reach every discovered node and routes slots through one published business port. This mode does not apply to Dragonfly.

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

### Cluster per-node proxy

Use [cluster-node-proxy.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/cluster-node-proxy.yml), keeping example `node_proxies[].listen` ports 17001–17003. Set `cluster_node_advertise_host` to a client-reachable host address and match each `advertise_port` to its published host port. Map every node in a larger topology.

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

### Dragonfly HA: single controller

Use [replication-ha-dragonfly.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/replication-ha-dragonfly.yml) with `coordination.enabled: false` and `ha.coordinator.mode: none`; clear fields for other coordination modes such as `owner_id` and `lease_path`. Only one controller may manage this topology. Supply all data-node client/replication addresses and reliable fencing configuration. Snapshot storage belongs on Dragonfly containers. Redis / Valkey / Redict / KeyDB / Kvrocks HA use the same command shape with their own templates; disable NARIX persistence orchestration for Kvrocks.

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

### Dragonfly HA: same-host file lease

Set `ha.coordinator.mode: local-file`, distinct `owner_id` values and the same `lease_path: /var/lib/narix/leases/dragonfly.lease`; keep `coordination.enabled: false`. Create `deploy/shared-leases` writable by UID 10001. Both containers must run on the same host and share the same lock-capable lease directory. Different directories or a cross-host filesystem do not provide this locking contract. State and log directories remain separate.

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

### Dragonfly HA: independent Redis lease

Set `ha.coordinator.mode: redis`, configure a separate lease service through `redis_addr`, credentials and a shared group `redis_key`, and give each instance a distinct `owner_id`. Clear `lease_path` and disable built-in coordination. The lease Redis must not be a data node managed by this HA group. Other hosts can use the same command shape with distinct identities and state directories. This option does not supply the built-in consensus log.

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

### Dragonfly HA: three-member coordination cluster

Prepare three configurations from [replication-ha-dragonfly-proxy-cluster.yml](https://github.com/nasa-runtime/narix/blob/release/docs/configuration/replication-ha-dragonfly-proxy-cluster.yml). Run the following commands on three separate hosts, each with its own `narix-net`. Each member has a distinct `node_id`, certificate and state directory; cluster ID, membership, bootstrap identity and HA group agree. Set `advertise` / `peers[].address` to addresses reachable from the other hosts on port 19090. Enable mTLS with real fingerprints and certificate names. Keep `coordination.data_dir` under `/var/lib/narix`, not in the container layer. The configuration mount includes `tls/`; no individual certificate mounts are needed. Restrict 19090 to trusted members and aggregate business endpoints with an external layer-4 load balancer.

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

### Readonly queries / HA observation

Set `read_only: true` in the chosen mode. When following an existing HA controller, also set `ha.observer: true` to prevent this instance from issuing control writes. Readonly business access and control-plane observation are independent settings. Retain authentication and least privilege. This example exposes queries on host port 6380.

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

### Multiple proxies in one process

Configure uniquely named entries in one YAML `proxies` list, listening on 6379, 6380 and 6381 with their own backends and access policies. Publish every listener; administration needs one mapping.

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

## Mounts and operational options

| Mount | Purpose |
| --- | --- |
| `/etc/narix:ro` | Complete YAML/TLS directory; atomic replacement remains visible |
| `/var/lib/narix` | Persistent coordination terms, log and state snapshots; configure paths beneath it |
| `/usr/local/logs/narix` | File logs, retained across container recreation |
| `/etc/localtime:ro` | Host time zone |
| Shared lease directory | Required only for `local-file`, shared by same-host controllers |
| Fencing programs and credentials | Add explicit read-only mounts at the command's configured paths when needed |

A mount does not rewrite YAML paths. A lease path outside `/var/lib/narix` needs its own writable mount. Dragonfly snapshots and other backend data must have volumes on their **data containers**; the NARIX state volume cannot replace them. NARIX's checkpoint verification is not a separate local data-backup directory. If the application name or `log.path` changes, adjust the log mount. To override the file location, use `-e NARIX_LOG_PATH=/usr/local/logs/narix`; `-e NARIX_LOG_PATH=` explicitly selects console-only output.

File rotation defaults to 100MB per file, 1GB of archives and seven days. Normal commands do not produce per-request logs. Bound Docker stdout globally or add `--log-driver json-file --log-opt max-size=10m --log-opt max-file=3`. Optional hardening is `--read-only --cap-drop ALL --security-opt no-new-privileges`; all configured writes and external fencing requirements must still be satisfied. Linux host networking can replace bridge networking and `-p` flags, but listeners then bind directly to the host and must not conflict.

## Health and shutdown

Use `GET /health` for liveness and `GET /ready` for traffic admission. Status, metrics and configuration endpoints require a Bearer token. Protect administration and coordination interfaces. The image does not hard-code a health command or publish unspecified coordination/listener ports.

SIGTERM closes admission before draining. The 70-second stop grace must exceed `max(10000, largest graceful_shutdown_timeout_ms) + 5000` milliseconds with margin; increase it when application budgets grow. Preserve state volumes across upgrades and keep a stable member identity. Configuration, TLS and HA failures retain the applicable protection gates.

More: [Container deployment](https://github.com/nasa-runtime/narix/blob/release/docs/container-deployment.en.md) · [Configuration](https://github.com/nasa-runtime/narix/blob/release/docs/configuration.en.md) · [Operations](https://github.com/nasa-runtime/narix/blob/release/docs/operations.en.md) · [Source](https://github.com/nasa-runtime/narix)
