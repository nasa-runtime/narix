# NARIX - NASA Adaptive RESP2/3 Infrastructure eXchange

[简体中文](README.md)

NARIX is a Rust RESP2/RESP3 proxy and managed replication HA control plane designed for very high throughput and low forwarding overhead. It supports multiple KV backends and five proxy modes, with a readonly query endpoint for development and operations. Clients use a stable address while NARIX handles connections, routing, authentication and access policy.

**Dragonfly HA is NARIX's standout mode.** Built on Dragonfly's native replication, it continuously manages failure detection, controlled promotion, safe routing, replica topology and persistence responsibility. Applications keep their ordinary Redis client connections; no dedicated SDK is required.

For Dragonfly HA, NARIX manages five responsibilities:

- **Safe failover:** protect routing after confirming a primary failure, then apply candidate, authority and fencing gates before promotion.
- **Stable access:** close connections to the old primary when routing changes or becomes protected. Reconnecting clients reach the confirmed primary; control decisions stay outside the per-command forwarding path.
- **Topology convergence:** replicas follow the current primary directly. Cascading replication, incorrect upstreams and multiple-primary evidence trigger protection and controlled recovery.
- **Persistence placement:** replicas own snapshots in a stable multi-node deployment. With enough eligible replicas, separate a snapshot-free hot standby from snapshot owners; verify the new responsibility before retiring the old one.
- **Coordinated instances:** leases or the built-in coordination cluster serialize control actions and publish configuration transactionally.

Ordinary Dragonfly HA traffic uses one-to-one transparent connections, separate from control probes. See [performance data](#throughput-and-added-forwarding-latency) for measured throughput and overhead. **HA does not guarantee zero data loss:** asynchronous replication retains an RPO window, and default soft fencing does not prove that the old primary can no longer write. Strong production isolation requires reliable external fencing.

Managed replication HA also supports Redis, Valkey, Redict, KeyDB and Kvrocks. Standalone, Sentinel, single-entry Cluster and node-proxy modes serve fixed backends, dynamically discovered primaries and standard slot-based clusters. One process can host multiple isolated listeners; each `proxies[]` entry owns its connection limits, connection resources, readonly policy, metrics and runtime state.

[Dragonfly HA configuration](#replication-ha) · [Production coordination template](docs/configuration/replication-ha-dragonfly-proxy-cluster.yml) · [Deployment](docs/deployment.en.md) · [Operations and recovery](docs/operations.en.md)

The five data-plane modes are:

- `replication-ha`: managed replication, with Dragonfly HA as a distinctive use case. `backend_profile` selects the adapter for probing, protection, fencing and coordinated switching. Dynamic persistence orchestration is available except for Kvrocks.
- `standalone`: transparent access to one fixed backend.
- `sentinel`: discover and validate the current primary through the Redis Sentinel client protocol, replacing local connection resources after a switch.
- `cluster`: one entry point for a standard slot-based Cluster, including topology loading, slot routing, `MOVED`/`ASK` handling and cross-node aggregation.
- `cluster-node-proxy`: one proxy address per real Cluster node, rewriting topology and redirection addresses.

## Dragonfly HA: the standout mode

Dragonfly owns storage and replication. NARIX's `replication-ha` control plane continuously decides which node may serve traffic, which replica may take over, which nodes own snapshots and which controller may change topology. Clients reconnect through the same entry point after NARIX converges routing and closes old-generation connections.

```text
                    Ordinary Redis clients
                            | stable address
                   +--------v---------+
                   | NARIX data plane |  Confirmed primary only
                   |    fail-closed   |  Close old connections on change
                   +--------+---------+
                            |
                         primary
                         ^     ^
                         |     |
                      replica replica
                  Every replica follows the primary directly

Control plane:
  candidate lag gates -> protect routing -> fencing/switching -> convergence
  persistence ownership -> checkpoint verification -> safe handoff
  controller exclusion: local-file / Redis lease / pair or majority cluster
```

| Dragonfly HA responsibility | NARIX behavior |
|---|---|
| Primary failure and recovery | Confirm repeated failures and close routing; validate replication evidence, obtain authority and complete fencing before switching. Reopen only after confirming the result. |
| Multiple proxy instances | Serialize control actions using a same-host file lease, independent Redis lease or built-in coordination cluster. Prefer at least three coordination members across hosts. |
| Uncertain topology or multiple primaries | Remain protected and expose the reason. An unconfirmed additional primary is not forgotten merely because it becomes unreachable. |
| Snapshot impact | Place snapshots on replicas in steady state, retaining a snapshot-free hot standby when enough replicas qualify. Enable and verify new owners before disabling old owners. |
| Cascades and incorrect upstreams | Reassign replicas to follow the primary directly while control authority remains valid. Back off and retain protection on failure. |
| Operational changes | Manual and automatic actions share safety gates. Parse, validate and prepare configuration before committing and publishing; rejected candidates retain the current configuration. |
| Observability | Expose topology, protection reasons, persistence responsibility, configuration status, Prometheus metrics and a Grafana dashboard. |

Replacement or deletion retires the old control generation first. Adapters recheck retirement, leases or action tokens around external steps so old handles cannot initiate further control actions. Commands already sent to a backend cannot be recalled; a disconnected client may still face an unknown outcome and must apply its own idempotency policy. The coordination cluster records HA observations, switching results and persistence ownership. Commit control results before publishing safe routing. See [operations](docs/operations.en.md) and [backend control requirements](docs/certification.en.md).

## Dragonfly HA persistence topology

With `ha.persistence.enabled: true`, NARIX assigns persistence responsibility. A configured single-node deployment snapshots on its primary. A stable multi-node deployment assigns eligible replicas so snapshot I/O stays off the primary. If no eligible responsibility can be confirmed, `fail_closed` determines routing protection. Kvrocks uses a different storage contract described below.

The recommended steady state is **one primary, one hot standby and at least one snapshot node**, requiring at least three healthy nodes. Business traffic goes to the primary; every other node follows it directly:

```text
                         Business traffic
                                |
                                v
                         Current primary
                     Periodic snapshots disabled
                                ^
                +---------------+----------------+
                |               |                |
          Hot standby      Snapshot node 1  Snapshot node N
          No snapshots     Periodic snapshots on eligible replicas
          Preferred takeover candidate
```

Roles are chosen from actual replication roles, lag and stability. `nodes` lists candidates; it does not permanently assign primary or standby roles.

| Healthy primary | Eligible replicas | Status `topology` | Behavior |
|---|---|---|---|
| Single-node configuration: 1 | 0 | `single-master-snapshot` | The primary owns snapshots only when the configuration itself contains one node. |
| Multi-node configuration: 1 | 0 | `none` / `protected` | Do not silently move persistence back to the primary. Apply `fail_closed` and await replica recovery. |
| 1 | 1 | `single-replica-snapshot` | The only replica owns snapshots and is also a takeover candidate. Snapshot I/O and takeover share one node, reducing availability isolation. |
| 1 | ≥2 | `hot-standby-replica-snapshots` | Select one stable replica with low lag as a snapshot-free hot standby; other eligible replicas own snapshots. |

Three nodes are the minimum for all three roles. To retain them after any one node fails, use at least four nodes: one primary, one hot standby and two snapshot replicas. The standby is preferred for promotion. With only three nodes, the remaining deployment temporarily becomes one primary plus one snapshot replica until recovery or replacement.

Strict persistence policy keeps startup protected until ownership and backend configuration are verified. Handoff follows **enable new owners → verify checkpoints → disable all old owners → commit the coordinated result → publish local topology**. No incomplete handoff is marked verified. Each configuration write and verification poll rechecks action authority; a partially applied multi-setting change restores the previous settings only while authority remains valid. Published ownership is checked against actual backend settings, including external `CONFIG` or cron changes. Coordination evidence covers the complete owner set and replication links. An absent old owner is not assumed disabled; unsafe convergence with `fail_closed: true` protects routing. **Kvrocks must use `ha.persistence.enabled: false`:** each node owns its RocksDB/WAL, and NARIX converges replication topology without assigning snapshot or hot-standby roles.

## Supported backends and versions

These are minimum versions for ordinary RESP access. Later versions must retain the required native protocol and capabilities. A version floor does not imply support for every newer command, argument, module, topology or persistence feature. Backend semantics remain authoritative. NARIX's readonly allowlist rejects unknown forms and modifying operations; see the [command matrix](command-matrix.en.md).

| Backend | `backend_profile` | Minimum RESP version | Mode and native capability limits |
|---|---|---|---|
| Redis | `redis` | 6.0.0 and later | Sentinel and standard Cluster. Newer Functions, modules and administrative capabilities are not added to older servers. |
| Valkey | `valkey` | 7.2.5 and later | Sentinel and standard Cluster. The inherited 7.2.4 tag and withdrawn 8.0.8 are excluded. |
| Dragonfly | `dragonfly` | 1.7.0 and later | HA control requires at least 1.9.0. Standard Cluster and Sentinel are not admitted. For 1.7.0, disable remote version checks and use that version's `save_schedule` option. |
| Apache Kvrocks | `kvrocks` | 2.10.1 and later | Standard Cluster and managed replication. Each node manages RocksDB/WAL; dynamic persistence orchestration is prohibited. |
| Microsoft Garnet | `garnet` | 2.1.0 and later | Standard Cluster; no managed replication HA or Sentinel. Range/Vector capabilities also require native modules and runtime dependencies. Command-name presence alone is insufficient. |
| KeyDB | `keydb` | 5.0.0 and later | Standard Cluster requires 5.1.0. Native Cluster TLS requires at least 6.2.0 and a TLS-enabled build. Managed replication permits only a single-primary topology, not active-replica/multi-master. |
| Redict | `redict` | 7.3.0 and later | Sentinel and standard Cluster. HA and dynamic persistence also require the control capabilities below. |
| Pika | `pika` | 4.0.4 and later | Transparent `standalone` access using RESP2. Pika/Codis slot interfaces are not standard Redis Cluster. |
| Tendis | `tendis` | 2.0.6 and later | Standard Cluster and node proxy using RESP2; no Sentinel or managed replication HA. RocksDB build variants must match the data-directory format. |

HA and dynamic persistence have separate minimum versions:

| Backend | Minimum managed replication HA version | Minimum dynamic persistence version |
|---|---|---|
| Redis | 6.2.0 and later | 7.0.0 and later |
| Valkey | 7.2.5 and later | 7.2.5 and later |
| Dragonfly | 1.9.0 and later | 1.15.0 and later, subject to the ACK condition below |
| Redict | 7.3.0 and later | 7.3.0 and later |
| KeyDB | 6.2.0 and later, single primary | 6.2.0 and later |
| Kvrocks | 2.16.0 and later | Not provided; each node manages RocksDB/WAL |

Dragonfly's replica-lag gate uses the second-granularity I/O age `master_last_io_seconds_ago`, subtracting one second for sampling granularity. This is not a byte-offset difference. With Dragonfly 1.15.0 and strict `ha.persistence.require_zero_lag: true`, start the backend with `--replication_acks_interval=1000`. Its default 3000 ms ACK interval can make a healthy idle link report age 2, rejecting persistence candidates and keeping routing protected. NARIX does not relax this gate to become Ready. Disconnected, synchronizing or unknown replication states remain ineligible.

Redis-family planned switches require `FAILOVER TO`; replica assignment requires dynamic replication credentials and `REPLICAOF`. Redis, Valkey and Redict checkpoints require `INFO persistence`'s `rdb_saves`; KeyDB requires strictly advancing `LASTSAVE`. Dragonfly persistence requires readable/writable `snapshot_cron`, `dbfilename` and verifiable `LASTSAVE`. Missing capabilities, insufficient permissions or unconfirmed results fail the action and preserve applicable protection. See [backend control requirements](docs/certification.en.md).

Transparent proxying does not turn absent commands or placeholder responses into complete features. Dragonfly's fixed empty `LATENCY` response is not latency diagnostics. Some older Tendis administrative endpoints are placeholders or unavailable. Garnet 2.1.0–2.2.4 native `FAILOVER` can leave both sides as replicas, so that path is outside the usable switching contract. Garnet vector counts and post-deletion state also depend on the native version. See the [command matrix](command-matrix.en.md) for routing and compatibility translations.

| Mode | Admitted `backend_profile` values |
|---|---|
| `standalone` | `redis`, `valkey`, `dragonfly`, `kvrocks`, `garnet`, `keydb`, `redict`, `pika`, `tendis` |
| `sentinel` | `redis`, `valkey`, `redict` |
| `cluster` | `redis`, `valkey`, `kvrocks`, `garnet`, `keydb`, `redict`, `tendis` |
| `cluster-node-proxy` | `redis`, `valkey`, `kvrocks`, `garnet`, `keydb`, `redict`, `tendis` |
| `replication-ha` | `redis`, `valkey`, `dragonfly`, `redict`, `kvrocks`, `keydb` |

Pika uses `standalone`, not standard Cluster modes. Sentinel consumes committed primary topology; it does not elect a primary, issue `REPLICAOF`, fence nodes or modify Sentinel configuration. All six managed replication products use `mode: replication-ha`, selected by `backend_profile`. Hosted RESP services must also satisfy provider-specific TLS, ACL, networking and failover constraints.

## Throughput and added forwarding latency

These are **best observed measurements** for the stated workloads, not typical values or capacity guarantees. Throughput selects the complete paired repetition with the highest NARIX QPS. Latency selects the pair with the lowest NARIX p99, keeping all quantiles in a row from that same pair. Each workload includes at least three valid repetitions; anomalous direct, proxy and reference samples are excluded as a complete matched group.

The host is an Apple M1 Pro with 10 cores and 16 GiB RAM. The Docker Linux arm64 VM has 10 vCPUs and approximately 7.75 GiB RAM. Backend, proxy and client each use two separate, non-overlapping vCPUs. Memory limits are 768 MiB for the backend and 512 MiB each for proxy and client; NARIX uses two Tokio workers. Builds use Rust 1.95 release mode, `lto=thin`, `codegen-units=1`. Traffic uses TCP, RESP2 and persistent connections within one Docker network, without TLS; each repetition has 15 seconds of warm-up and 60 seconds of measurement. HA uses steady-state routing with one primary and one replica, a two-second probe interval, automatic failover disabled and persistence orchestration disabled. Dragonfly snapshots are also disabled.

Build identifiers are the first 12 hexadecimal digits of the source snapshot SHA-256. The measured build for the current business implementation is `f13cdca88499`; other rows describe only their stated builds. Results are not merged across builds. All throughput rows use hot-key GET with 64-byte values; direct and NARIX values in each row belong to one pair.

| Backend | Mode | Connections / pipeline | NARIX QPS | Direct QPS | Build |
|---|---|---:|---:|---:|---|
| Redis 6.0.0 | `standalone` | 32 / 16 | 1,425,690 | 1,485,763 | `f13cdca88499` |
| Redis 6.2.0 | `replication-ha` | 32 / 16 | 1,576,003 | 1,513,763 | `b869f93c7fd7` |
| Redis 8.10.3 | `replication-ha` | 32 / 16 | 1,565,762 | 1,516,921 | `b869f93c7fd7` |
| Dragonfly 2.0.2 | `standalone` | 32 / 16 | 1,236,086 | 1,449,221 | `83135bd29311` |
| Dragonfly 2.0.2 | `replication-ha` | 32 / 16 | 1,418,180 | 1,532,165 | `b68c94ee83d3` |
| Dragonfly 2.0.2 | `replication-ha` | 128 / 64 | 1,960,180 | 1,938,193 | `23223f27435c` |

Capacity workloads reach different rates on each path. Batching and scheduling can also yield a higher observed proxy capacity than direct access. High throughput does not imply low tail latency: NARIX's absolute p99 in the Dragonfly HA 128-connection, pipeline-64 pair is approximately 20.19 ms. Added latency is therefore reported separately at a fixed offered rate.

**Added latency = the NARIX command-completion quantile − the corresponding direct quantile**, in **µs**. Direct access at 150 µs and proxy access at 160 µs means 10 µs added latency. All rows below use Dragonfly 2.0.2, 32 connections, pipeline 16, hot-key GET with 64-byte values and an offered rate of 1,000,000 commands per second.

| Mode | Build | p50 delta | p95 delta | p99 delta | p99.5 delta | p99.9 delta |
|---|---|---:|---:|---:|---:|---:|
| `replication-ha` | `23223f27435c` | +35.840 | −2.048 | −20.480 | −32.768 | −126.976 |
| `standalone` | `83135bd29311` | +79.872 | +100.352 | +139.264 | +151.552 | +200.704 |
| `standalone` | `f13cdca88499` | +82.944 | +145.408 | +180.224 | +188.416 | +294.912 |

Latency runs from client command issuance to receipt of its complete response, including networking, backend execution and queueing. It excludes waiting before issuance. Measured from the scheduled send time instead, NARIX p99 for these three rows is approximately 1.71 ms, 39.32 ms and 28.18 ms. In 60 seconds, NARIX completes 59,999,568, 59,999,472 and 59,999,440 commands respectively; 432, 528 and 560 scheduled commands remain unsent. The full scheduled demand is therefore not completed.

Differences between quantiles are not a distribution of per-command proxy-internal processing time. Negative deltas do not imply negative internal time. Histogram relative bucket width is at most 0.78125%, and deltas use bucket upper bounds. Do not divide latency by pipeline depth. Overhead depends on mode, connection count, value size, load and host scheduling; best GET results cannot establish mixed-workload, switching-period or all-tail performance.

## Readonly access for development and operations

Configure a separate proxy entry with `read_only: true`. Allowed reads are classified by backend and argument form. Writes, script execution, administrative changes, replication control and unknown subcommands are rejected before reaching the backend. If any command in a parsed batch is disallowed, the entire batch returns `READONLY`; reads in that batch are not executed first. There is no transactional rollback across batches.

Readonly access does not grant extra backend privileges or conceal sensitive query results. Retain frontend authentication, least-privilege backend credentials and network isolation. To follow an existing HA control plane from a side endpoint, also use `ha.observer: true`: observers issue no backend control writes, while `read_only` separately governs business commands.

## Build and start

Use Rust 1.94 or newer. All third-party dependencies come from crates.io; `Cargo.lock` fixes their versions and checksums. No local framework checkout is required. NARIX is distributed as a binary, source archive or container and retains `publish = false`.

```bash
cargo build --release --locked
```

Start with an external configuration:

```bash
target/release/narix --config=/etc/narix/narix.yml
```

Or select it through the environment:

```bash
APP_CONFIG_FILE=/etc/narix/narix.yml target/release/narix
```

External-file selection follows `--config` → `APP_CONFIG_FILE` → the compatibility variable `NARIX_CONFIG`. Unset or empty values fall through. An unreadable or invalid selected file fails loading without falling back to a lower-priority file. Parsing, typed validation, duplicate proxy-name checks and listener-conflict checks finish before business listeners open. Failure exits the process.

### Docker local build and startup

Prepare `deploy/narix/config/narix.yml` with the business listener at `0.0.0.0:6379`, administration at `0.0.0.0:18080`, and real backend addresses and credentials. Create `deploy/narix/data` and `deploy/narix/logs`. Image user `10001:10001` must be able to read configuration and write data and logs.

```sh
docker build -f docker/Dockerfile -t narix:1.0.1 .
docker run -d --name narix \
  --restart unless-stopped --stop-timeout 70 \
  -p 6379:6379 -p 127.0.0.1:18080:18080 \
  -v "$PWD/deploy/narix/config:/etc/narix:ro" \
  -v "$PWD/deploy/narix/data:/var/lib/narix" \
  -v "$PWD/deploy/narix/logs:/usr/local/logs/narix" \
  -v /etc/localtime:/etc/localtime:ro \
  narix:1.0.1
```

The image defaults to `APP_CONFIG_FILE=/etc/narix/narix.yml`. Configuration is read-only; data and logs persist on the host. The read-only `/etc/localtime` mount uses the host time zone; this file must exist and be mountable on the Docker host. Keep coordination storage under `/var/lib/narix`, with a separate directory for each member. The business port is published; administration is limited to host loopback. If the backend is another container, attach both to the same Docker network.

For complete `docker run` examples covering every proxy mode, HA coordination, readonly access and multiple listeners, see the [Docker Hub README](docker/README.en.md). Mounts, reload and image distribution are described in [container deployment](docs/container-deployment.en.md).

## Minimal configurations

Replace every example password and token before deployment. Add backend `username` and `password` when the backend requires them. The examples use identical keys and values in both language editions.

### replication-ha

This is the single mode name for managed replication. See the [Dragonfly HA architecture](#dragonfly-ha-the-standout-mode).

- `backend_profile` selects one of six product adapters. Redis-family `FAILOVER TO`, Dragonfly promotion followed by demotion, and Kvrocks `CLIENT PAUSE` plus `SLAVEOF` share `/ha/*` management endpoints while retaining their native switching differences.
- `ha` configures probes and independent probe timeouts, failure thresholds, replication-lag gates, `auto_failover`, fencing and coordination (`none`, `local-file`, `redis`, `proxy-cluster`). An omitted `coordinator.mode` resolves to `proxy-cluster` when `coordination.enabled=true`, otherwise `none`. Explicitly selecting another coordinator inside the built-in cluster is rejected at startup.
- Product persistence settings belong under `ha.persistence.<product>`. Kvrocks requires `ha.persistence.enabled: false`.
- **Automatic failover defaults to enabled**, `ha.auto_failover: true`. Confirmed primary failure protects routing before coordination, fencing and candidate promotion. Default `fencing.mode: manual` is soft isolation: the old primary may accept writes before it is demoted. A production goal of preventing split brain requires reliable `external-command` fencing exercised on the real infrastructure.
- **Manual topology actions share the same safety gates.** `failover`, `switchover`, `promote`, `replicaof` and `fence` require the same authority as automatic actions. Manual promotion also fences the old primary first. Adapters recheck authority around role/replication writes and while waiting for confirmation. Commit switching results before opening the new route.
- **Observer mode**, `ha.observer: true`, only probes and follows. It issues no backend control writes and rejects manual topology changes. Confirmed old-primary demotion permits immediate following; a permanently unavailable old primary requires confirmed failure plus a continuously stable successor. Insufficient evidence or multiple primaries keeps routing protected. Use `read_only: true` separately for a readonly business endpoint.
- Management endpoints include `GET /ha/topology`, `/ha/master`, `/ha/replicas`; `POST /ha/failover`, `/ha/switchover`, `/ha/node/{node}/promote`, `/ha/node/{node}/replicaof/{master}` and `/ha/node/{node}/fence`.

This minimal Dragonfly example uses one NARIX instance with automatic failover and soft isolation. It is suitable for local use or a single control plane that explicitly accepts the soft-fencing risk. Each `nodes` item is `client_addr@replication_addr`:

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"

proxies:
  - name: replication-ha-dragonfly
    backend_profile: dragonfly
    mode: replication-ha
    listen_host: 0.0.0.0
    listen: 7001
    nodes: 198.51.100.11:6379@198.51.100.11:6379,198.51.100.12:6379@198.51.100.12:6379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
    ha:
      auto_failover: true
      fencing:
        mode: manual
      coordinator:
        mode: none
```

`auto_failover`, `fencing.mode` and `coordinator.mode` above can be omitted to use their defaults. These defaults serialize actions within one process and enable recovery; they do not guarantee freedom from split brain. For strong isolation, configure `external-command` and its executable. Across hosts, use `proxy-cluster` or an independent Redis coordinator.

Full templates:

- [Dragonfly, one NARIX instance](docs/configuration/replication-ha-dragonfly.yml)
- [Dragonfly with the built-in coordination cluster](docs/configuration/replication-ha-dragonfly-proxy-cluster.yml)
- [Redis 8.2.2](docs/configuration/replication-ha-redis.yml)
- [Valkey 9.1.1](docs/configuration/replication-ha-valkey.yml)
- [Redict 7.3.6](docs/configuration/replication-ha-redict.yml)
- [KeyDB 6.3.4](docs/configuration/replication-ha-keydb.yml)
- [Kvrocks 2.16.0](docs/configuration/replication-ha-kvrocks.yml)

Production automatic failover requires both reliable fencing and coordination matching the deployment scope. An in-process lock requires strictly non-overlapping processes. Use `proxy-cluster` or independent Redis across processes/hosts; prefer at least three coordination members for new multi-host deployments. `local-file` coordinates only processes on the same host.

### standalone

A transparent proxy to one fixed backend:

- Preserve native command semantics and pipeline response order, keeping pipelined frontend traffic pipelined to the backend.
- Pub/Sub, blocking commands, transactions and connection-bound commands use DedicatedTunnel, with independent active limits, queue capacity and acquisition timeouts.
- `auth_mode: terminate` authenticates clients at NARIX and separately authenticates the backend. `backend-inject` supports explicitly unauthenticated internal access.
- `read_only: true` rejects writes and every script execution entry point.
- Each proxy owns connection, queue, parsed-frame/batch and timeout budgets plus metrics. Backend RESP TLS validates CA/SNI and optionally uses mTLS.
- All nine backend profiles are admitted, including Pika/Codis through their native interfaces.

Replace `backend_profile`, `host` and `port` to select another supported backend:

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"

proxies:
  - name: redis-primary
    backend_profile: redis
    mode: standalone
    listen_host: 0.0.0.0
    listen: 7001
    host: 127.0.0.1
    port: 6379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
```

[Full standalone configuration](docs/configuration/standalone.yml)

### sentinel

A dynamic single-primary endpoint for an existing Sentinel deployment:

- Query committed Sentinel topology, then verify that the candidate is actually a primary before routing.
- Subscribe to `+switch-master` and refresh periodically. Replace connection resources after a switch; reject new traffic when no safe primary can be confirmed.
- Consume Sentinel topology without initiating elections, `REPLICAOF`, fencing or Sentinel configuration changes. Use `replication-ha` when NARIX should control replication itself.
- Configure Sentinel control authentication/TLS separately from business data connections.
- Admit `redis`, `valkey` and `redict`.

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"

proxies:
  - name: redis-sentinel
    backend_profile: redis
    mode: sentinel
    listen_host: 0.0.0.0
    listen: 7001
    sentinel_master_name: mymaster
    sentinel_nodes: 192.0.2.11:26379,192.0.2.12:26379,192.0.2.13:26379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
```

[Full Sentinel configuration](docs/configuration/sentinel.yml)

### cluster

A single endpoint for a standard slot-based Cluster. Applications can use an ordinary non-Cluster Redis client:

- Load and refresh topology, route by key slot and handle `MOVED`/`ASK`. Registered cross-slot aggregates split and combine results in order; `MSET`, scripts and other same-slot operations still reject cross-slot requests.
- Handle topology routing on the client's behalf while returning backend failures, timeouts and unsupported operations explicitly.
- Shared backend channels use RESP2. `HELLO 3` returns `NOPROTO`; unmappable session operations such as `RESET` are explicitly rejected.
- Configuration preflight refreshes topology and requires all 16384 slots without conflicts before publication.
- Admit `redis`, `valkey`, `kvrocks`, `garnet`, `keydb`, `redict` and `tendis`.

`nodes` lists standard Cluster seeds:

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"

proxies:
  - name: redis-cluster
    backend_profile: redis
    mode: cluster
    listen_host: 0.0.0.0
    listen: 7001
    nodes: 192.0.2.11:6379,192.0.2.12:6379,192.0.2.13:6379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
```

[Full Cluster configuration](docs/configuration/cluster.yml)

### cluster-node-proxy

One NARIX listener per real Cluster node, retaining standard Cluster-client behavior:

- Rewrite addresses in `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED` and `ASK`. Operators must make the advertised proxy addresses reachable from the client network.
- Use `aliases` for backend hostnames, container names and internal addresses that may appear in topology or redirects.
- Apply connection governance and metrics per node, including frontend authentication in terminate mode; suitable for NAT/firewall deployments.
- Clients retain native slot migration, redirection and node-failure handling.
- Admit `redis`, `valkey`, `kvrocks`, `garnet`, `keydb`, `redict` and `tendis`.

Map each real node to a unique listener. `advertise_host`/`advertise_port` define the address returned to clients:

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"

proxies:
  - name: redis-cluster-nodes
    backend_profile: redis
    mode: cluster-node-proxy
    listen_host: 0.0.0.0
    cluster_node_advertise_host: narix.example.internal
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
    node_proxies:
      - backend: 192.0.2.11:6379
        listen: 17001
        advertise_port: 17001
      - backend: 192.0.2.12:6379
        listen: 17002
        advertise_port: 17002
      - backend: 192.0.2.13:6379
        listen: 17003
        advertise_port: 17003
```

[Full node-proxy configuration](docs/configuration/cluster-node-proxy.yml)

## Operational checks

Health and readiness do not require a token:

```bash
curl -fsS http://127.0.0.1:18080/health
curl -fsS http://127.0.0.1:18080/ready
```

Status and metrics require the administration token:

```bash
curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  http://127.0.0.1:18080/status

curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  http://127.0.0.1:18080/metrics
```

Load balancers should use `/ready`; `/health` only indicates process liveness. The administration service also accepts `X-Admin-Token: <token>`.

Check a business connection:

```bash
redis-cli -h 127.0.0.1 -p 7001 \
  -a replace-with-a-long-random-client-password PING
```

## Configuration sources

Precedence, from lowest to highest:

```text
zcf/application.yml
application-${NARIX_PROFILE}.yml in the same directory
external file selected by --config, APP_CONFIG_FILE or NARIX_CONFIG
${name}-${NARIX_PROFILE}.yml beside the external file
NARIX_ field environment variables
${...} placeholder resolution
```

YAML, JSON and TOML are supported. An explicitly provided `proxies[]` or `node_proxies[]` array describes the entire desired set. Entries matching `name` or `backend` inherit unspecified fields from lower-priority files; entries absent from the higher-priority array are removed. Omitting the array inherits it entirely. Environment overrides target identities or indices without deleting other entries.

Hot reload is enabled by default. `reload.poll_interval_ms` defaults to `1000` (one second); `10000` means ten seconds. Automatic publication requires two consecutive reads with the same effective digest. Validation and publication can also be requested explicitly. HA topology, persistence responsibility and unresolved multiple-primary evidence must pass the active generation's safety precommit; rejection retains the existing configuration and generation:

```bash
curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  -X POST http://127.0.0.1:18080/config/validate

curl -fsS \
  -H 'Authorization: Bearer replace-with-a-long-random-admin-token' \
  -X POST http://127.0.0.1:18080/config/reload
```

A single node switches its local runtime generation. With the process-level coordination cluster enabled, submit once on any member; the leader coordinates majority preflight and consensus commit. Lagging members cannot open their data plane until they reach the committed revision, and stale local files cannot overwrite the committed cluster configuration.

See the [configuration reference](docs/configuration.en.md) for authentication, merging, environment overrides and every template; the [built-in configuration](zcf/application.yml) supplies minimal safe defaults. [Operations](docs/operations.en.md) covers distributed reload and recovery.

## Logging and configuration transactions

Defaults retain every `WARN`, exceptional `ERROR` and key NARIX `INFO` events for startup/shutdown, configuration application, authority, role and protection changes. Ordinary requests and successful periodic probes are not logged individually.

```yaml
application:
  name: narix
log:
  level: ${NARIX_LOG_LEVEL:warn,narix=info}
  path: ${NARIX_LOG_PATH:/usr/local/logs/${application.name}}
  max_file_size: 100MB
  total_size_cap: 1GB
  max_history_days: 7
  split_error_file: true
reload:
  enabled: true
  poll_interval_ms: 1000
```

Override the directory with an environment variable such as `NARIX_LOG_PATH='/var/log/${application.name}'`; the loader resolves the application-name placeholder. Configuration sets a 100 MB file limit, 1 GB archive cap and seven-day retention. `RUST_LOG` takes precedence over `NARIX_LOG_LEVEL`; environment overrides prevent edits to the same YAML fields from changing the effective value.

Level, directory, format and rotation settings support transactional reload. Parse, validate and prepare the complete candidate before replacing active configuration. Installation failures roll back the generation; incomplete rollback keeps business admission closed. A new polling interval is adopted only after successful commit. See [reload configuration](docs/configuration.en.md#configuration-reload) for sources, node-local fields and restart-only settings, and [logging](docs/configuration.en.md#logging) for shutdown and loss boundaries.

## Metrics and Grafana

`GET /metrics` exposes process CPU, RSS, threads, file descriptors, frontend/backend connections, commands, pipeline, DedicatedTunnel, Cluster, Sentinel and managed replication HA metrics.

| File | Purpose |
|---|---|
| `dashboards/prometheus.yml` | Scrape one NARIX process |
| `dashboards/prometheus-cluster.yml` | Scrape each member of a NARIX cluster |
| `dashboards/grafana-datasource.yml` | Grafana Prometheus data source |
| `dashboards/grafana-dashboard-provider.yml` | Grafana dashboard provider |
| `dashboards/narix-overview.json` | Dashboard with cluster, node and proxy selectors |

Each process exposes its own administration service. Prometheus must scrape each member rather than only the load-balancer address. Keep `environment` and `narix_cluster` labels consistent and `narix_node` unique. See [operations](docs/operations.en.md).

## Production constraints

- Prefer `auth_mode: terminate` and authenticated business listeners.
- Administration requires a token by default; `allow_unauthenticated: true` is only for trusted isolated networks.
- Business RESP listeners do not terminate TLS natively. Use a trusted layer-four entry point, tunnel or service mesh across untrusted networks.
- Backend RESP TLS validates CA/SNI; Sentinel control TLS and ACLs are independent of data connections.
- `coordination.tls` protects inter-member gRPC, not business ingress.
- Confirmed primary failure, an unconfirmed new primary or multiple-primary evidence closes HA routing. Load balancers must use `/ready`.
- Automatic failover requires reliable fencing and coordination matching the deployment scope. Unreachable does not mean isolated.
- Persistence orchestration does not turn asynchronous replication into zero-data-loss WAL; an RPO window remains.
- Kvrocks must keep `ha.persistence.enabled: false`; operators own per-node RocksDB/WAL, backups and disk reliability.
- Size connections, queues and frame limits for file descriptors, backend `maxclients`, pipeline workload and memory. Apply limits according to the [actual forwarding path](docs/configuration.en.md#connections-capacity-and-timeouts).
- Node-proxy aliases should cover addresses returned by `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED` and `ASK`.

## Runtime facilities and shutdown

One log manager owns filtering, file output and rotation. NASA facilities provide configuration parsing, metrics encoding, gRPC transport, credential material and time conversion. NARIX owns candidate preparation, commit and activation of the same generation; Application owns signals, critical-task supervision and resource shutdown. Steady-state RESP forwarding uses handles held by the active generation without loading configuration or reading certificates per command.

NASA Application's shared components perform runtime configuration polling and candidate reads. Blocking file I/O runs in a blocking thread pool; asynchronous callers `.await` it. NARIX retains domain authority over routing, HA and cross-node configuration commit.

After synchronous configuration and credential preflight, log-resource preparation and business assembly share a startup budget. Shutdown revokes admission, stops control tasks, drains connections and closes logging last. A reload racing with SIGTERM cannot reopen stopped admission. Drain timeouts produce a nonzero exit; runtime teardown does not wait indefinitely beyond the application shutdown budget for blocking file I/O. See [lifecycle budgets](docs/configuration.en.md#process-lifecycle).

## Further documentation

- [Configuration reference](docs/configuration.en.md)
- [Deployment](docs/deployment.en.md)
- [Backend control capabilities and constraints](docs/certification.en.md)
- [Operations](docs/operations.en.md)
- [Deployment checklist](docs/release-checklist.en.md)
- [Command matrix](command-matrix.en.md)
- [Contributing](CONTRIBUTING.en.md)
- [Security](SECURITY.en.md)
- [Container deployment](docs/container-deployment.en.md)
- [Docker Hub overview](docker/README.en.md)

## License

Licensed under Apache License 2.0. See [LICENSE](LICENSE).
