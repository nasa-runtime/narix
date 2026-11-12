# NARIX configuration reference

[简体中文](configuration.md)

README contains minimal configurations. Full mode-specific YAML templates are available here:

| Mode or deployment | Full configuration |
|---|---|
| Fixed backend | [standalone](configuration/standalone.yml) |
| Sentinel primary discovery | [sentinel](configuration/sentinel.yml) |
| Single-entry standard Cluster | [cluster](configuration/cluster.yml) |
| Standard Cluster node proxy | [cluster-node-proxy](configuration/cluster-node-proxy.yml) |
| HA with Redis 8.2.2 | [replication-ha-redis](configuration/replication-ha-redis.yml) |
| HA with Valkey 9.1.1 | [replication-ha-valkey](configuration/replication-ha-valkey.yml) |
| HA with Redict 7.3.6 | [replication-ha-redict](configuration/replication-ha-redict.yml) |
| HA with KeyDB 6.3.4 | [replication-ha-keydb](configuration/replication-ha-keydb.yml) |
| HA with Kvrocks 2.16.0 | [replication-ha-kvrocks](configuration/replication-ha-kvrocks.yml) |
| Dragonfly, one NARIX instance with soft fencing | [replication-ha-dragonfly](configuration/replication-ha-dragonfly.yml) |
| Dragonfly, three-member coordination cluster | [replication-ha-dragonfly-proxy-cluster](configuration/replication-ha-dragonfly-proxy-cluster.yml) |

Templates expand proxy, administration and coordination fields; logging and application name inherit built-in configuration. YAML comments remain in Chinese and field names are identical. Addresses, credentials, certificates and deployment-specific budgets/policies are examples to replace. Other fields use program defaults. Mutually exclusive settings cannot apply simultaneously: `ha.coordinator.mode` selects `none`, `local-file`, `redis` or `proxy-cluster`. When omitted, it resolves to `proxy-cluster` with coordination enabled, otherwise `none`. The [built-in configuration](../zcf/application.yml) contains minimal safe startup and reload settings.

## Loading and merging

Precedence from lowest to highest:

```text
1. zcf/application.yml
2. zcf/application-${NARIX_PROFILE}.yml
3. external file selected by --config, APP_CONFIG_FILE or NARIX_CONFIG
4. ${filename}-${NARIX_PROFILE}.yml beside the external file
5. NARIX_ field environment variables
6. ${...} placeholders
```

Accepted extensions are `.yml`, `.yaml`, `.json` and `.toml`; files without an extension are parsed as YAML. Missing profile files are skipped.

NASA `naml` parses and resolves configuration. Limits are 16 MiB per source, 1 MiB per string, tree depth 64, 100,000 nodes and 16 MiB of cumulative placeholder expansion. Duplicate object keys, missing/cyclic references, ambiguous paths, unknown YAML tags and oversized inputs are rejected. Errors identify sources and fields without including configuration bodies. Ordinary `${proxies.0.name}` references use array indices; whole-value references still produce strings. A leading `$` outside a placeholder retains its literal meaning. Environment input accepts only valid UTF-8 `NARIX_*` and `RUST_LOG` values.

Merge rules:

- Objects merge recursively by field.
- Explicit `proxies[]` arrays describe the complete desired set keyed by `name`; absent names are deleted.
- Explicit `node_proxies[]` arrays describe the complete desired set keyed by `backend`; absent backends are deleted.
- One source cannot repeat a `name` or `backend` identity.
- Omitting an array inherits the lower-priority array. Providing it preserves unspecified fields only for matching identities.
- Environment overrides target an identity or index without deleting other array elements.

External files may contain only deployment differences. Built-in `proxies` is empty and adds no hidden listener.

External-file selection follows `--config` → `APP_CONFIG_FILE` → the compatibility variable `NARIX_CONFIG`; unset or empty values fall through. Only the selected external file and its profile are loaded. The path is fixed at startup and shared by polling, SIGHUP and administration requests. Missing, unreadable or invalid selected files reject the candidate instead of selecting another source. Edit file contents for hot reload; changing the environment-selected path requires a process restart. `ProxyConfig::load` follows the same rules; `ProxyConfig::load_from` uses the caller's explicit sources.

## Top-level configuration

```yaml
admin:
  enabled: true
  listen: 127.0.0.1:18080
  token: "replace-with-a-long-random-admin-token"
  allow_unauthenticated: false

coordination:
  enabled: false
  peers: []

proxies:
  - name: redis-primary
    backend_profile: redis
    mode: standalone
    listen: 7001
    host: 127.0.0.1
    port: 6379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
```

| Field | Default | Meaning |
|---|---|---|
| `admin` | Enabled | Administration HTTP service. |
| `coordination` | Disabled | Built-in control plane for HA coordination and cluster-wide configuration synchronization in all modes. |
| `reload` | Enabled | File, SIGHUP, administration and cluster reload policy. |
| `proxies` | `[]` | Proxy entries; an empty waiting state requires administration and reload enabled. |

## Coordination RPC capacity and deadlines

With `coordination` enabled, `coordination.grpc` declares deployment ceilings, effective message limits and aggregate budgets. Omitting it uses these defaults; admission limits do not preallocate the corresponding amount of memory:

```yaml
coordination:
  grpc:
    message_size_ceiling_bytes: 268435456
    unary_timeout_ceiling_ms: 86400000
    max_message_bytes: 201326592
    unary_timeout_ms: 86400000
    max_inflight_message_bytes: 805306368
    managed_memory_budget_bytes: 2147483648
```

`message_size_ceiling_bytes` and `unary_timeout_ceiling_ms` bound the effective `max_message_bytes` and `unary_timeout_ms`. The framework also validates gRPC frame lengths, monotonic-clock representation, inflight messages, connection concurrency and managed memory accounting. Invalid combinations fail before opening a listener. A shorter caller deadline still takes precedence; a timeout does not prove that the remote operation had no effect.

These fields are fixed at startup. Reload changes reject the entire candidate and preserve the active configuration. Default budgets retain the existing cluster genesis digest. Custom budgets enter the cluster identity and must agree across members. Existing state directories cannot switch directly to different budgets: plan a migration to a new coordination cluster, retaining the original consensus data.

## Logging

One NASA log manager applies levels, file output and rotation:

```yaml
application:
  name: narix
log:
  level: ${NARIX_LOG_LEVEL:warn,narix=info}
  path: ${NARIX_LOG_PATH:/usr/local/logs/${application.name}}
  max_file_size: 100MB
  total_size_cap: 1GB
  max_history_days: 7
  clean_history_on_start: true
  split_error_file: true
  color: false
```

Built-in defaults write to `/usr/local/logs/narix`, which must be writable by the runtime user. An explicitly empty `NARIX_LOG_PATH`, `log.path: ""` or `log.path: null` selects console-only output. `max_file_size` limits one file; `total_size_cap` limits archives; `max_history_days` controls archive retention. `split_error_file` enables separate error output, while `color: false` disables ANSI color in files.

Level precedence is `RUST_LOG` → `NARIX_LOG_LEVEL` → file `log.level`. Default filtering, `warn,narix=info`, retains warnings/errors and key NARIX events: startup/shutdown, configuration application, authority, role, routing and protection changes. Normal requests, successful routine probes, pool recycling and individual forwarding completion do not produce logs. Connection failures and unexpected task exits are `ERROR`. Logging supports environment defaults, nested references and explicit empty strings; ordinary business fields retain their existing configuration-reference rules.

Logging is node-local. Level, path, pattern, file size, archive cap, retention, startup cleanup, error separation and color can reload. With `reload.enabled`, edit an actually loaded YAML file and wait for `reload.poll_interval_ms` and `reload.stable_observations`, or send SIGHUP or authenticated `POST /config/reload`. Inspect `GET /config/reload` for version and failure reason. Log paths are not broadcast to other cluster members.

Environment values take precedence over YAML. Remove `RUST_LOG` and `NARIX_LOG_LEVEL` before startup if YAML should control level changes; the same principle applies to other fields. Changing another shell's environment does not modify the running process. Edit an unoverridden YAML field or restart with new environment values.

Reload first validates filters and formatting, then prepares files. Installation occurs inside the configuration publication gate before the new generation opens. Invalid candidates or preparation failure preserve current output and configuration. Initial preparation starts after Application owns signals and resources. Shutdown stops business tasks before draining log writers. File opening or draining cannot extend the application shutdown budget indefinitely; incomplete shutdown exits nonzero.

Each file output queues at most 128000 buffers, allocating according to actual backlog. Individual buffers have no separate byte limit. Producers do not wait for disk: a full queue or stopped worker drops the current buffer. A guard waits up to 100 ms to enqueue ordered shutdown and up to one second for a flush attempt. Shutdown handoff is not proof of durable logging; write/flush failure or timeout can lose logs. These limits are separate from Application task/resource budgets.

## Process lifecycle

After synchronous configuration and credential preflight, Application owns log initialization, control services, business listeners and Ready publication. Startup budget is `reload.transaction_timeout_ms + 10000` milliseconds. Shutdown budget is `max(10000, maximum proxy graceful_shutdown_timeout_ms) + 5000` milliseconds; with no proxies it is 15000 milliseconds. Resources share the total budget and may receive shorter derived budgets.

SIGTERM or critical-task failure first revokes business admission and broadcasts shutdown, then drains managed tasks and resources. Late preflight or reload completion cannot reopen admission. Successful drain exits 0; critical-task failure, unfinished forcibly cancelled tasks or resource timeout exits nonzero with the failed phase logged. Tokio teardown does not add an unlimited wait for uncancellable file I/O after application shutdown.

Nonempty business configuration can run with `admin.enabled: false`. Empty `proxies` is permitted only with both administration and reload enabled. Process Ready does not grant HA write authority; each generation must independently establish safe backend routing.

## Administration

| Field | Default | Meaning |
|---|---|---|
| `admin.enabled` | `true` | Start the HTTP listener. |
| `admin.listen` | `127.0.0.1:18080` | Bind address, in `IP:port` form. |
| `admin.token` | Built-in startup-rejected placeholder | Bearer token or `X-Admin-Token`; replace before deployment. |
| `admin.allow_unauthenticated` | `false` | Allow protected endpoints without authentication only in local/trusted isolated environments. |

With administration enabled and unauthenticated access disabled, a nonempty token is required. The built-in `change-me-admin-token` is rejected.

Only these endpoints are unauthenticated:

```text
GET /health
GET /ready
```

All others require authentication, including `/status`, `/metrics`, `/config/*` and all `/ha/*` queries/actions.

`/metrics` and Application share the same collection snapshot; command processing continues to update atomics directly. CPU time retains fractional precision through a separate Prometheus counter text source. The current integer-counter structured interface omits this one metric; other metrics use the structured source. A NARIX snapshot permits at most 90,000 samples, 4 KiB per label value and 16 MiB of combined label values. Exceeding a limit rejects the entire snapshot with HTTP 503 instead of truncating families or retaining obsolete labels. Removed proxies disappear from subsequent snapshots after their old generations drain.

## Configuration reload

```yaml
reload:
  enabled: true
  poll_interval_ms: 1000
  stable_observations: 2
  transaction_timeout_ms: 60000
```

| Field | Default | Meaning |
|---|---:|---|
| `reload.enabled` | `true` | Enable reload entry points. |
| `reload.poll_interval_ms` | `1000` | File and certificate dependency check interval, milliseconds. |
| `reload.stable_observations` | `2` | Consecutive identical effective digests required before automatic publication; minimum 2. |
| `reload.transaction_timeout_ms` | `60000` | Budget for loading, preflight, coordination, listener switching and publication, in milliseconds; range `1..=86400000`. Old connections drain in the background after admission reopens, outside this transaction budget. Once HA external actions start, caller disconnection does not cancel them asynchronously; individual step timeouts govern safe completion. |

The polling interval defaults to one second and accepts one millisecond through 365 days; `10000` means ten seconds. NASA Application's shared configuration component owns timing and blocking-read isolation, while NARIX's configuration transaction governs validation, routing, HA and cluster commit. A successful commit changes the interval; invalid intervals reject the complete candidate and retain the active configuration. Larger intervals delay discovery of configuration/certificate changes; stable-observation requirements still apply.

```text
GET  /config/reload
POST /config/validate
POST /config/reload
```

`validate` loads, performs typed/listener/backend preflight and does not publish. Reload prepares every candidate, then briefly closes process-wide new-connection admission. All additions, modifications and removals must succeed or roll back completely before reopening, preventing new connections from observing mixed configuration. New connections use the new generation; old connections receive shutdown and drain according to connection type. Proxy, node-proxy and HA-node collections publish the candidate's desired set; deleted entries are unregistered.

Independent Redis lease preflight verifies `ha.coordinator.redis_username/redis_password` and reads both the lease key and its `:outcome` key. Connection, authentication or read failure rejects the candidate. Preflight does not acquire, renew or release a lease and grants no control authority; switching still requires a valid lease, backend evidence and fencing.

With `coordination` enabled, the leader creates normalized managed configuration and its digest. Member ID, coordination listen/advertise addresses, data directory, coordination TLS paths, administration listen address and the host parts of business/node-proxy addresses stay in each member's local bootstrap. Business ports, proxy entries and other managed policies share one revision and digest.

### Reload boundaries and sources

Logging and administration authentication are node-local and are not propagated in cluster configuration. Ordinary reload cannot migrate administration listener enablement/address, application metadata, static coordination membership, coordination addresses or TLS identities; these changes reject the complete candidate and require a planned restart. Business proxies, backend credentials, policies and listeners still require their applicable safety precommit and resource preparation.

NARIX exposes local files, profiles, environment overrides and committed coordination-cluster configuration as sources. NASA Application's general transaction also supports Nacos, but this executable does not expose Nacos connection settings. Deployments using a configuration center should materialize complete candidates into the external file and use the same validation/publication flow. Framework source support does not imply an enabled NARIX entry point.

Runtime candidate reads, material preflight and log-resource preparation run in the blocking pool, with asynchronous callers awaiting completion. A reader may construct a candidate but cannot publish it independently. Cancelling a wait cannot forcibly interrupt synchronous file I/O already in progress; source, shutdown and publication authority must be rechecked after completion. Invalid YAML, fields, backend preflight or preparation failure leaves current configuration and polling unchanged.

## Backend profiles and modes

`backend_profile` selects real capabilities: HELLO identity, RESP3 admission, backend handshake and topology-mode validation.

| `backend_profile` | standalone | Sentinel | cluster / node proxy | replication-ha |
|---|---|---|---|---|
| `redis` | Yes | Yes | Yes | Yes |
| `valkey` | Yes | Yes | Yes | Yes |
| `dragonfly` | Yes | No | No | Yes |
| `kvrocks` | Yes | No | Yes | Yes |
| `garnet` | Yes | No | Yes | No |
| `keydb` | Yes | No | Yes | Yes |
| `redict` | Yes | Yes | Yes | Yes |
| `pika` | Yes | No | No | No |
| `tendis` | Yes | No | Yes | No |

| Mode | Required backend fields | Meaning |
|---|---|---|
| `standalone` | `listen`, `host`, `port` | One fixed backend. |
| `sentinel` | `listen`, `sentinel_master_name`, `sentinel_nodes` | Discover and validate a dynamic primary. |
| `cluster` | `listen`, `nodes` | Comma-separated standard Cluster seeds. |
| `cluster-node-proxy` | `node_proxies[]` | Parent has no business `listen`; each child defines a listener. |
| `replication-ha` | `listen`, `nodes`, `ha` | Use an admitted profile. `ha: {}` selects defaults; production should explicitly configure fencing and coordination. |

HA `nodes` is a comma-separated list of `client_addr@replication_addr` tokens.

Redis, Valkey, Redict and KeyDB planned switching uses native `FAILOVER TO` and the [HA version floors](certification.en.md#product-adaptation-boundaries). Each replica must announce an address matching its `replication_addr`. If the primary does not list it as an online replica, preflight rejects the switch rather than guessing a target. Kvrocks 2.16.0 uses `CLIENT PAUSE WRITE`, offset gates and `SLAVEOF`; this is not a single atomic command.

## Common proxy fields

| Field | Default | Meaning |
|---|---|---|
| `name` | Required | Unique identity for logs, metrics and merging. |
| `backend_profile` | `redis` | Explicitly select non-Redis profiles. |
| `mode` | Required | `standalone`, `sentinel`, `cluster`, `cluster-node-proxy`, `replication-ha`. |
| `listen_host` | `0.0.0.0` | Business bind host. |
| `listen` | `0` | Must be nonzero except for the parent of a node proxy. |
| `host` / `port` | `null` | Standalone backend. |
| `nodes` | `null` | Cluster seeds or HA node list. |
| `username` | `null` | Backend ACL username. |
| `password` | `null` | Backend password. |
| `backend_tls_enabled` | `false` | Enable certificate-validated backend RESP TLS. |
| `backend_tls_ca_path` | `null` | Required CA PEM when TLS is enabled. |
| `backend_tls_server_name` | `null` | SNI; otherwise derived from the actual destination. |
| `backend_tls_certificate_path` | `null` | Optional mTLS certificate, paired with its private key. |
| `backend_tls_private_key_path` | `null` | Optional mTLS private key, paired with its certificate. |
| `frontend_password` | `null` | Password validated by NARIX. |
| `auth_mode` | Automatic | `terminate`, `backend-inject`, `pass-through`. |
| `allow_unauthenticated` | `false` | Explicitly permit clients to skip proxy authentication. |
| `read_only` | `false` | Unified readonly gate; unknown and transparent-tunnel commands fail closed. |

| Authentication mode | Behavior |
|---|---|
| `terminate` | Validate `frontend_password`, then use independent backend `username`/`password`. |
| `backend-inject` | Inject backend credentials; requires `allow_unauthenticated: true`. |
| `pass-through` | Forward AUTH on the connection-bound backend; requires `allow_unauthenticated: true`. Supported only for `standalone` and the transparent writing data plane of `replication-ha`. HA switches close connections so clients reconnect and authenticate again. Not supported by Sentinel or either Cluster mode. |

If omitted, `auth_mode` is `terminate` when a frontend password exists, otherwise `backend-inject`.

## Sentinel

Sentinel connections carry topology queries only. Ordinary commands, pipelines, transactions, blocking commands and Pub/Sub connect to the discovered and validated business primary.

| Field | Default | Meaning |
|---|---|---|
| `sentinel_master_name` | `null` | Required monitored service name. |
| `sentinel_nodes` | `null` | Required comma-separated `host:port` control endpoints. |
| `sentinel_username` | `null` | Independent Sentinel ACL username. |
| `sentinel_password` | `null` | Independent Sentinel password. |
| `sentinel_connect_timeout_ms` | `300` | Control/candidate-role connection timeout. |
| `sentinel_request_timeout_ms` | `1000` | Control/candidate-role response timeout. |
| `sentinel_refresh_interval_ms` | `1000` | Background primary refresh period. |
| `sentinel_tls_enabled` | `false` | Enable validated RESP TLS for Sentinel. |
| `sentinel_tls_ca_path` | `null` | Required CA PEM for Sentinel TLS. |
| `sentinel_tls_server_name` | `null` | SNI; otherwise derived from the selected control endpoint. |
| `sentinel_tls_certificate_path` | `null` | Optional mTLS certificate. |
| `sentinel_tls_private_key_path` | `null` | Optional mTLS private key. |

NARIX queries `SENTINEL get-master-addr-by-name`, then checks `ROLE` or `INFO replication`. Only a validated primary is published. Switching creates a new connection-resource generation; the old generation accepts no new batches. Transactions, subscriptions and unbounded blocking connections disconnect and must reconnect.

Transient discovery failure retains an existing route that has not reported business connection failure. No discovered primary, or failed business connection plus failed rediscovery, protects routing. Recovery revalidates the primary and increments the route generation even if the address is unchanged. Connection-bound sessions cannot cross the protection interval on old resources.

## RESP TLS

Business-backend and Sentinel TLS configurations are independent. Both validate certificate chains and server identity; there is no skip-verification option.

Materials are parsed and shared per configuration generation; ordinary connections retain TLS session resumption. An internal TLS alert during handshake permits one full-handshake retry using the same trust roots, server name and client identity. Both attempts share the connection timeout. Certificate or identity failures do not retry. No AUTH or business command has been sent at that point, so business requests are not replayed. Pools reuse established connections; steady-state commands do not reparse certificates or rehandshake.

- Server authentication requires `*_tls_enabled` and `*_tls_ca_path`.
- Set `*_tls_server_name` when certificate identity differs from the connection address.
- mTLS requires both `*_tls_certificate_path` and `*_tls_private_key_path`.
- Handshake time counts toward connect timeout.
- Status shows whether TLS is enabled, not certificate paths, private-key paths or credentials.

## Connections, capacity and timeouts

Defaults match the full YAML templates:

| Field | Default | Meaning |
|---|---:|---|
| `dedicated_tunnel_max_active` | `1024` | Active dedicated backend connections; `0` is unlimited. |
| `dedicated_tunnel_queue_size` | `1024` | Wait queue after dedicated capacity is exhausted. |
| `dedicated_tunnel_acquire_timeout_ms` | `1000` | Dedicated permit acquisition timeout. |
| `dedicated_tunnel_pool_max_idle` | `1000` | Clean idle tunnels per real backend; `0` disables reuse. |
| `dedicated_tunnel_pool_idle_timeout_ms` | `30000` | Clean idle lifetime; `0` disables reuse. |
| `connect_timeout_ms` | `3000` | Backend TCP connect timeout. |
| `backend_request_timeout_ms` | `10000` | Ordinary backend request timeout. |
| `frontend_auth_timeout_ms` | `10000` | Frontend AUTH/HELLO initialization deadline. |
| `client_idle_timeout_ms` | `0` | Authenticated client idle timeout; `0` disables proactive close. |
| `tcp_keepalive_ms` | `0` | TCP keepalive period; `0` uses system behavior. |
| `max_connections` | `65535` | Frontend connection limit for one proxy. |
| `max_auth_bytes` | `16384` | Maximum cumulative authentication input bytes. |
| `pipeline_max_commands` | `1000` | Commands per parsed batch. |
| `pipeline_max_bytes` | `4194304` | Input bytes per parsed batch. |
| `pipeline_backend_max_active` | `5000` | Active pipeline-pool connections. |
| `pipeline_backend_queue_size` | `4096` | Pipeline-pool wait queue. |
| `pipeline_backend_submit_timeout_ms` | `1000` | Pipeline submission wait deadline. |
| `pipeline_backend_idle_timeout_ms` | `60000` | Pipeline backend idle lifetime. |
| `frame_max_bytes` | `4194304` | One parsed RESP frame; cannot exceed `pipeline_max_bytes`. |
| `bulk_max_bytes` | `4194304` | One bulk string. |
| `aggregate_max_concurrency` | `32` | Concurrent Cluster aggregation subtasks. |
| `aggregate_max_items` | `10000` | Aggregation item limit. |
| `pending_response_bytes` | `16777216` | Pending response bytes per frontend. |
| `graceful_shutdown_timeout_ms` | `10000` | Graceful connection/task shutdown wait. |
| `cluster_topology_refresh_ms` | `60000` | Background Cluster topology refresh. |

DedicatedTunnel handles connection-bound semantics in parsed modes: Pub/Sub, blocking commands, transactions, `SELECT` and similar commands. Ordinary commands and pipelines do not consume this limit.

The `replication-ha` writing data plane (`read_only: false`) opens one backend connection per frontend and transparently relays bytes. It does not use the ordinary pipeline or DedicatedTunnel pools; `max_connections` is its main connection-count gate. Readonly HA also binds a separate backend connection but parses each batch for the readonly gate. `pipeline_backend_max_active: 5000` and `dedicated_tunnel_pool_max_idle: 1000` limit their respective pools, not all modes' backend connections. Transparent relay does not parse each business frame, so parsed-frame, batch and per-request timeout settings are not per-command guarantees on that path.

## Cluster node-proxy fields

| Field | Default | Meaning |
|---|---|---|
| `cluster_node_advertise_host` | `null` | Shared advertised host for all child listeners. |
| `node_proxies[].backend` | Required | Real Cluster `host:port`, unique within the parent. |
| `node_proxies[].listen` | Required | Child listener port. |
| `node_proxies[].advertise_host` | Parent value | Host returned to Cluster clients. |
| `node_proxies[].advertise_port` | `listen` | Port returned to clients. |
| `node_proxies[].listen_host` | Parent value | Child bind host. |
| `node_proxies[].aliases` | `[]` | Backend aliases normalized to this proxy address. |

Provide `advertise_host` or parent `cluster_node_advertise_host`.

## Managed replication HA

All six supported products use `mode: replication-ha`; `backend_profile` selects the adapter. HA and persistence have independent [capability/version requirements](certification.en.md#product-adaptation-boundaries). Actual command, role and replication evidence governs control, not version numbers alone.

### Probing and fencing

| Field | Default | Meaning |
|---|---|---|
| `ha.probe_interval_ms` | `1000` | Probe all node roles and replication state. |
| `ha.probe_connect_timeout_ms` | Inherit `connect_timeout_ms` | Independent HA probe connect timeout. |
| `ha.probe_request_timeout_ms` | Inherit `backend_request_timeout_ms` | Independent HA probe response timeout. |
| `ha.master_down_after_failures` | `3` | Consecutive failed primary observations before failure confirmation. |
| `ha.max_replica_lag` | `0` | Maximum known candidate lag in seconds; `0` removes only the numerical cap. A disconnected replica after primary failure uses known `master_link_down_since_seconds` as a provable lower bound for gates and minimum-lag ranking. Unknown lag/disconnection age, full synchronization or wrong upstream remains ineligible. Persistence owners cannot be disconnected. |
| `ha.auto_failover` | `true` | Confirmed primary failure protects routing before coordination, fencing and promotion. Lease contention, missing candidates and backoff retain protection. `false` disables NARIX-initiated promotion, not observation, old-primary demotion or replica convergence. Follow immediately with confirmed demotion evidence; if the old primary is permanently absent, require a unique successor stable for `master_down_after_failures` observations. Construction only discovers topology, never promotes. |
| `ha.observer` | `false` | Probe/follow without any backend control writes; reject manual topology actions and imply `auto_failover=false`. Require old-primary demotion evidence, or confirmed old-primary failure plus a unique successor stable for the same threshold. Insufficient evidence or multiple primaries protects routing. Startup requires no coordination cluster, `coordinator=none`, persistence disabled and manual fencing. Business readonly access separately requires `read_only=true`. |
| `ha.fencing.mode` | `manual` | Soft fencing demotes the old primary with `REPLICAOF` after switching and permits a dual-write risk. The window depends on probing, network, execution and recovery; one probe period is not a hard upper bound. Cluster route-guard waiting cannot replace physical isolation. `external-command` isolates before promotion; safety depends on actually preventing old-primary writes and requires `command`. |
| `ha.fencing.command` | `[]` | Executable and separate arguments, without shell interpolation. |
| `ha.fencing.timeout_ms` | `10000` | External fencing deadline. |

The executable acts on the old backend node, for example through a cloud power API or network ACL. A nonzero exit or timeout means fencing failure and prevents promotion. Argument placeholders are `{proxy}`, `{client_addr}`, `{replication_addr}` and `{action_token}`.

Fencing isolates the old backend; coordination serializes controllers. `coordinator: none` only serializes one process and proves no physical isolation. Multiple hosts require `proxy-cluster` or independent Redis. `local-file` covers same-host overlap only. Never run overlapping controllers with `none`, including blue/green replacement. Observers are the only exception because they never issue control writes.

In `proxy-cluster`, if a committed new primary fails before every proxy follows it, proxies without a healthy primary still synchronize that committed failure target. They close routing before accumulating observations for the target. Subsequent recovery still requires observation votes, action authority, fencing, roles and persistence verification; a commit alone never opens routing.

### Coordinators

| Field | Default | Meaning |
|---|---|---|
| `ha.coordinator.mode` | Resolve from `coordination.enabled` | Built-in cluster: `proxy-cluster`; otherwise `none`. Explicit `none`/`local-file`/`redis`/`proxy-cluster` is allowed only when consistent with cluster enablement. |
| `ha.coordinator.owner_id` | Generated | Stable instance identity for file/Redis modes, using secure randomness rather than clock/PID uniqueness. Each lease attempt adds an independent token; renewal, release and outcome publication compare the full token. Random-source failure prevents generation/lease creation. Do not reuse explicit identities across active instances. |
| `ha.coordinator.lease_path` | `null` | File lease path. |
| `ha.coordinator.group_id` | Proxy name | Stable HA group identity within `proxy-cluster`. |
| `ha.coordinator.observation_quorum` | `0` | Required failure observations; `0` uses static-member majority. |
| `ha.coordinator.observation_window_ms` | `3000` | Voting window for an identical failure fingerprint. |
| `ha.coordinator.redis_addr` | `null` | Independent Redis; never the controlled data source. |
| `ha.coordinator.redis_username` | `null` | Coordinator Redis ACL username. |
| `ha.coordinator.redis_password` | `null` | Coordinator Redis password. |
| `ha.coordinator.redis_key` | `null` | Shared lease key for the HA group. |
| `ha.coordinator.redis_connect_timeout_ms` | `3000` | Coordinator connection timeout. |
| `ha.coordinator.redis_request_timeout_ms` | `3000` | Coordinator request timeout. |
| `ha.coordinator.lease_ttl_ms` | `15000` | File/Redis lease TTL. |
| `ha.coordinator.lease_renew_interval_ms` | `5000` | Renewal interval; must be below TTL. |
| `ha.coordinator.lease_acquire_timeout_ms` | `3000` | Maximum wait for one lease attempt. |

Prefer `proxy-cluster` for new multi-host deployments; independent highly available Redis is another option. `local-file` cannot coordinate across hosts.

### Dynamic persistence

| Field | Default | Meaning |
|---|---|---|
| `ha.persistence.enabled` | `false` | Assign and migrate persistence responsibility. |
| `ha.persistence.fail_closed` | `true` | Protect routing when responsibility cannot be proven safe. |
| `ha.persistence.force_checkpoint_on_handoff` | `true` | Generate a native checkpoint during handoff. |
| `ha.persistence.min_replica_stable_ms` | `5000` | Minimum stable time before a replica owns persistence. |
| `ha.persistence.require_zero_lag` | `true` | Require normalized zero lag. |
| `ha.persistence.dragonfly.snapshot_cron` | `*/5 * * * *` | Snapshot schedule for owners. |
| `ha.persistence.dragonfly.dbfilename` | `dump-{role}-{client_addr_safe}-{timestamp}` | Snapshot filename template. |
| `ha.persistence.dragonfly.dir` | `null` | Validate startup directory without dynamically modifying it; each responsibility node must match. |
| `ha.persistence.dragonfly.verify_timeout_ms` | `30000` | Forced snapshot verification deadline. |
| `ha.persistence.redis.rdb.enabled` | `true` | Periodic RDB for owners. |
| `ha.persistence.redis.rdb.save_rules[]` | `300/10` | Seconds/change-count `save` rules. |
| `ha.persistence.redis.aof.enabled` | `true` | AOF for owners. |
| `ha.persistence.redis.aof.fsync` | `everysec` | AOF fsync policy. |
| `ha.persistence.redis.verify_timeout_ms` | `10000` | Forced RDB verification deadline. |
| `ha.persistence.valkey.*` | None | Same `rdb`, `aof`, `verify_timeout_ms` structure as Redis. |
| `ha.persistence.redict.*` | None | Same structure as Redis. |
| `ha.persistence.keydb.*` | None | Same RDB/AOF structure, using `LASTSAVE` for checkpoints. |

Persistence orchestration does not make asynchronous replication zero-data-loss WAL. Always enable and verify new owners before disabling old owners; this ordering is not configurable. Second-granularity Redis-family/Dragonfly heartbeat ages normally alternate between 0 and 1; adapters combine offset evidence or subtract one second of sampling granularity before the zero-lag gate. A nonempty Dragonfly `dir` is reread and checked whenever existing owners are adopted or new owners assigned.

Kvrocks must disable persistence orchestration and cannot specify any product persistence sub-block. NARIX controls only replication topology; all nodes retain their own RocksDB/WAL.

Dragonfly uses `master_last_io_seconds_ago`, a second-granularity I/O age with one second subtracted for sampling granularity; it is not byte-offset lag. Dragonfly 1.15.0 with strict zero-lag ownership must explicitly use `--replication_acks_interval=1000`; its default 3000 ms may report age 2 even on an idle healthy link and keep routing protected. Disconnected, synchronizing or unknown states remain ineligible; NARIX does not relax the gate to become Ready.

## Built-in coordination cluster

Process-level `coordination` is shared by HA entries. Exactly two static members use 2/2; at least three use majority consensus.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Start coordination gRPC. |
| `cluster_id` | `""` | Permanent shared cluster identity. |
| `node_id` | `0` | Stable positive member ID. |
| `listen` | `127.0.0.1:19090` | Local bind address. |
| `advertise` | `127.0.0.1:19090` | Address reachable by peers; port matches listen. |
| `data_dir` | `""` | Exclusive local term/log/snapshot directory. |
| `bootstrap_node_id` | `0` | Initial bootstrap member, included in peers. |
| `peers` | `[]` | Complete static membership, never reduced to survivors. |
| `heartbeat_interval_ms` | `500` | Leader heartbeat period. |
| `election_timeout_min_ms` | `1500` | Election lower bound; greater than twice heartbeat. |
| `election_timeout_max_ms` | `3000` | Election upper bound. |
| `rpc_timeout_ms` | `1000` | Ordinary member RPC and snapshot-install timeout; positive and below election minimum. Configuration phases have a separate transaction budget. |
| `route_guard_timeout_ms` | `5000` | Route retention window without confirmed authority. |
| `action_timeout_ms` | `30000` | External topology-action token lifetime. |
| `snapshot_log_entries` | `10000` | Log-entry threshold for a majority-mode control snapshot. |
| `pair_election_retry_ms` | `500` | Pair election retry interval. |
| `pair_allow_single_survivor` | `false` | Permit a survivor only after reliable peer isolation. |
| `pair_peer_fencing_command` | `[]` | External command to isolate the lost NARIX peer. |
| `pair_peer_fencing_timeout_ms` | `5000` | Peer fencing timeout. |

Before an external HA action in majority mode, the leader obtains linearizable majority confirmation within `route_guard_timeout_ms`, then commits the action token. Leader/term changes reject the request. Confirmation age starts at request initiation; slow responses cannot extend validity. This check runs for control actions, not per RESP command.

Managed configuration bodies are limited to 16 MiB, distinct from snapshot/RPC size. Snapshots also contain state/log positions, and JSON byte representation expands the envelope. Large configurations require enough heartbeat, RPC, election and transaction time for serialization, replication and disk synchronization. Increasing transport capacity does not extend deadlines. Snapshot installation uses `rpc_timeout_ms`; replication also uses consensus heartbeat budgets. Timeout keeps lagging members protected and never counts partial installation as synchronization.

Fields under `coordination.tls`:

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Mutual TLS; without it, all coordination addresses must be loopback IPs. |
| `certificate_path` | `null` | Local certificate-chain PEM. |
| `private_key_path` | `null` | Local private-key PEM. |
| `client_ca_path` | `null` | CA PEM for validating peers' client certificates. |

TLS peers require `node_id`, `address`, `server_name` and a 64-digit SHA-256 leaf-certificate DER fingerprint, `certificate_sha256`, without colons.

At bind time the certificate chain must already be valid and have at least one second remaining. Short-lived identities under one day are supported. Handshakes validate chains, lifetime, server names and member fingerprints; not-yet-valid or expired certificates cannot open coordination service.

Coordination TLS and static identities initialize together and cannot reload. Replacing bytes at the same path is also rejected; active channels keep their original snapshot. Plan a restart with consistent static membership identities. A local certificate/fingerprint mismatch rejects startup.

## Environment variables

Administration variables:

```text
NARIX_ADMIN_ENABLED
NARIX_ADMIN_LISTEN
NARIX_ADMIN_TOKEN
NARIX_ADMIN_ALLOW_UNAUTHENTICATED
```

Proxy indices start at zero; uppercase field names with underscores:

```text
proxies[0].backend_profile -> NARIX_PROXIES_0_BACKEND_PROFILE
proxies[0].pipeline_backend_max_active -> NARIX_PROXIES_0_PIPELINE_BACKEND_MAX_ACTIVE
proxies[0].backend_tls_enabled -> NARIX_PROXIES_0_BACKEND_TLS_ENABLED
proxies[0].sentinel_tls_ca_path -> NARIX_PROXIES_0_SENTINEL_TLS_CA_PATH
proxies[0].node_proxies[1].advertise_port -> NARIX_PROXIES_0_NODE_PROXIES_1_ADVERTISE_PORT
```

```bash
NARIX_ADMIN_TOKEN='production-admin-token' \
NARIX_PROXIES_0_HOST='192.0.2.21' \
NARIX_PROXIES_0_PORT=6379 \
NARIX_PROXIES_0_PASSWORD='backend-password' \
NARIX_PROXIES_0_FRONTEND_PASSWORD='frontend-password' \
target/release/narix --config=/etc/narix/narix.yml
```

String lists accept JSON arrays, comma-separated text or indexed items:

```bash
NARIX_PROXIES_0_NODE_PROXIES_0_ALIASES='["redis-a.internal:6379","10.0.0.11:6379"]'
```

`ha.*` and `coordination.*` do not support per-field `NARIX_` overrides. Give each member an external YAML/profile file. Complete candidate YAML remains the transaction boundary, avoiding isolated overrides of HA safety gates.

## Placeholders

Resolve `${...}` after file merging and environment overrides. Dots access objects and decimal components access arrays:

```yaml
admin:
  listen: "${proxies.0.listen_host}:18080"

proxies:
  - name: redis-primary
    backend_profile: redis
    mode: standalone
    listen_host: 127.0.0.1
    listen: 7001
    host: 127.0.0.1
    port: 6379
    frontend_password: "replace-with-a-long-random-client-password"
    auth_mode: terminate
```

References may resolve only strings, numbers or booleans. Missing references, object/array/null targets and cycles reject startup.

## Before deployment

- Replace example tokens, passwords, certificate paths/fingerprints, hostnames and IPs.
- Keep real secrets out of the repository; use external configuration, environment input or secret mounts.
- Check conflicts between business, administration and coordination ports.
- Ensure advertised node-proxy addresses are reachable by clients.
- Confirm backend versions, modes and native capabilities against README. Version numbers never authorize missing capabilities; failed controls or insufficient evidence preserve protection.
- Exercise deployment wiring: primary failure, load-balancer `/ready` removal, alerts, and real external fencing success/timeout/failure behavior.
- Reduce template capacity values according to file descriptors, backend limits and memory.
