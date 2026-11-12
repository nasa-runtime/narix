# Deploying NARIX

[简体中文](deployment.md)

NARIX supports multiple KV backends and proxy modes. In its standout Dragonfly HA mode, data nodes own storage and replication while NARIX owns safe primary routing, failover, replica convergence and persistence responsibility. Select `backend_profile: dragonfly` and `mode: replication-ha`; see the [Dragonfly HA architecture](../README.en.md#dragonfly-ha-the-standout-mode).

## Choose a data-plane mode

| Mode | Entry shape | Backends |
|---|---|---|
| `replication-ha` | One business listener for a managed replication group | Redis, Valkey, Dragonfly, Redict, Kvrocks, KeyDB |
| `standalone` | One listener for one fixed backend | Redis, Valkey, Dragonfly, Kvrocks, Garnet, KeyDB, Redict, Pika, Tendis |
| `sentinel` | One listener following the committed Sentinel primary | Redis, Valkey, Redict |
| `cluster` | One listener for a complete standard slot-based Cluster | Redis, Valkey, Kvrocks, Garnet, KeyDB, Redict, Tendis |
| `cluster-node-proxy` | One listener per real Cluster node | Redis, Valkey, Kvrocks, Garnet, KeyDB, Redict, Tendis |

Mode admission is a product-level safety boundary. Deployments must also satisfy [minimum versions and native capabilities](../README.en.md#supported-backends-and-versions); successful configuration parsing does not establish backend control capabilities.

Full templates:

- [standalone](configuration/standalone.yml)
- [sentinel](configuration/sentinel.yml)
- [cluster](configuration/cluster.yml)
- [cluster-node-proxy](configuration/cluster-node-proxy.yml)
- [HA / Redis](configuration/replication-ha-redis.yml)
- [HA / Valkey](configuration/replication-ha-valkey.yml)
- [HA / Redict](configuration/replication-ha-redict.yml)
- [HA / KeyDB](configuration/replication-ha-keydb.yml)
- [HA / Kvrocks](configuration/replication-ha-kvrocks.yml)
- [HA / Dragonfly](configuration/replication-ha-dragonfly.yml)
- [HA / Dragonfly coordination cluster](configuration/replication-ha-dragonfly-proxy-cluster.yml)

See [configuration](configuration.en.md) for all fields, defaults, overrides and environment variables.

## Processes and ingress

Each `proxies[]` entry owns connection limits, metrics and readonly policy. Parsed modes also isolate pipeline pools, DedicatedTunnel pools and queues per entry. Ordinary Dragonfly HA traffic uses one-to-one transparent connections rather than those pools; see [capacity by forwarding path](configuration.en.md#connections-capacity-and-timeouts).

Production generally uses an external layer-four entry point:

```text
Business clients
  -> VIP / cloud LB / nginx stream
  -> NARIX node A or node B
  -> the same backend KV group
```

NARIX does not provide a business VIP. Use Keepalived, a cloud/hardware load balancer or another TCP load balancer.

```bash
target/release/narix --config=/etc/narix/narix.yml
```

Use `GET http://<admin-listen>/ready` for load-balancer admission. `/health` indicates only process liveness.

## Cluster deployments

`cluster` is a single-entry mode with proxy-side slot routing. `nodes` only needs reachable seeds; production should use several seeds across failure domains.

`cluster-node-proxy` retains Cluster-client semantics. Map each real node to a unique listener and advertise an address clients can reach. Include aliases for names, container addresses and internal endpoints that may appear in `CLUSTER SLOTS`, `CLUSTER NODES`, `MOVED` or `ASK`.

Pika/Codis slot interfaces are not standard Redis Cluster and cannot use these modes. Access Pika native commands/slot interfaces through `standalone`.

## Managed replication HA deployments

Controllers sharing a backend group must serialize automatic failover:

- `local-file` covers processes on one host only.
- `redis` requires an independent highly available Redis; never place coordination storage inside the controlled data source.
- `proxy-cluster` uses built-in coordination. Two static members use 2/2; at least three use majority consensus.
- Automatic failover may be disabled with `ha.auto_failover`, leaving switching to an external controller or operator.

With `coordinator: none`, an in-process lock serializes actions, and **only one control-plane NARIX process may be active for the backend group**. Blue/green or graceful replacement with process overlap requires same-host `local-file` coordination or a suitable distributed coordinator.

The only exception is an **observer**, `ha.observer: true`. It sends no backend control commands, follows only with sufficient convergence evidence and remains protected during primary failure, insufficient evidence or multiple-primary states. Any number can accompany the production control plane. Observer limits control behavior; a side readonly endpoint also requires `read_only: true`. See [observer operation](operations.en.md#observer-nodes-haobserver).

For new multi-host deployments, prefer at least three `proxy-cluster` members. Each needs its own `node_id`, coordination address, data directory, certificates and business/administration ports. All members must agree on `cluster_id`, `bootstrap_node_id`, complete `peers` and HA `group_id`.

Automatic failover in a two-member pair requires both members online and observing primary failure. A single survivor cannot form the observation quorum even with `pair_allow_single_survivor` and completed peer fencing; use manual `/ha/failover` or `/ha/node/{node}/promote`. Symmetric-partition safety depends entirely on cross-node exclusion enforced by `pair_peer_fencing_command` (STONITH). Prefer three members when automatic availability is required.

Across hosts, enable mutual TLS and bind each `peers[].node_id` to the corresponding leaf-certificate SHA-256 fingerprint. Do not remove absent peers from static membership.

The process-level coordination cluster synchronizes configuration for all data modes. Edit one member; the leader commits revision, digest and normalized body after majority persistence and preflight. A three-member cluster can update with one member offline. A returning member must catch up with the committed body before Ready.

Only member ID, coordination listen/advertise addresses, data directory, coordination TLS paths, administration listen address, and host parts of business/node-proxy addresses remain local bootstrap differences. Proxy entries, business ports, backends, authentication and managed policies belong to cluster configuration. Do not independently submit a stale file on a lagging member; startup restores the coordination-log revision first.

Every member must use the same NARIX configuration schema version for configuration transactions. During rolling executable replacement, wait until all members are upgraded before submitting reloads. Mixed schemas explicitly reject configuration RPCs instead of filling unknown fields with defaults. Static membership does not hot-reload; change bootstrap configuration consistently during a maintenance window and restart. Ordinary reload cannot change `coordination.peers`.

## Fencing

Default `ha.fencing.mode: manual` is soft isolation without an executable. The old primary may briefly accept writes after recovery until demotion; use it only when that risk is accepted. Production automatic failover targeting no split brain needs `external-command`. NARIX executes a program with separate arguments, not a shell-assembled command.

The executable must actually isolate the old primary through infrastructure such as cloud APIs, power control or network ACLs. Timeout, nonzero exit or lost action authority prevents switching. No cloud-specific fencing adapters are bundled.

## Persistence responsibility

With `ha.persistence.enabled: true`:

- A configuration containing one node snapshots on that primary.
- With two nodes, one replica owns snapshots and the primary does not persist.
- With at least three nodes, retain one snapshot-free hot standby and place snapshots on other eligible replicas.
- A multi-node deployment with no eligible replica remains protected rather than falling back to primary persistence.

Old owners are not disabled until new owners are verified. This orchestrates snapshot responsibility; it is neither synchronous replication nor a zero-data-loss guarantee.

Kvrocks must disable this orchestration. Every node retains RocksDB/WAL; NARIX converges one primary and its direct replicas.

## Before opening traffic

- Build with `cargo build --release --locked`.
- Configure frontend authentication, an administration token and minimal network exposure.
- Size connections/queues for file descriptors, memory and backend `maxclients`.
- Scrape each member individually with Prometheus, not the business load-balancer address.
- Exercise Cluster migration, `MOVED`, `ASK` and node failure.
- Confirm HA backend versions and native replication/persistence requirements. Exercise deployment wiring: primary failure, alerts, real external fencing, protection after authority loss, checkpoint placement and disk capacity. For Kvrocks, verify each node's disk recovery instead.
- Confirm `/ready`, `/status`, `/metrics` and Grafana correctly identify each node.
