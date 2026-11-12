# Backend control capabilities and deployment constraints

[简体中文](certification.md)

Ordinary RESP access, managed replication HA and dynamic persistence require different native capabilities. See [supported versions and modes](../README.en.md#supported-backends-and-versions); meeting the RESP version floor does not make every control action available. NARIX preserves native errors and confirms effects through roles, replication links and persistence checkpoints.

## Product adaptation boundaries

| Backend | Minimum managed replication HA version | Minimum dynamic persistence version |
|---|---|---|
| Redis | 6.2.0 and later | 7.0.0 and later |
| Valkey | 7.2.5 and later | 7.2.5 and later |
| Dragonfly | 1.9.0 and later | 1.15.0 and later, with the ACK condition below |
| Redict | 7.3.0 and later | 7.3.0 and later |
| KeyDB | 6.2.0 and later, single primary | 6.2.0 and later |
| Kvrocks | 2.16.0 and later | Not provided; nodes manage RocksDB/WAL |

Dragonfly's lag gate uses second-granularity I/O age from `master_last_io_seconds_ago`, subtracting one second for sampling granularity; it is not a byte-offset difference. For Dragonfly 1.15.0 with strict `ha.persistence.require_zero_lag: true`, explicitly start with `--replication_acks_interval=1000`. Its default 3000 ms ACK interval can make a healthy idle link report age 2, rejecting persistence candidates and protecting routing. NARIX does not relax this gate merely to become Ready. Disconnected, synchronizing and unknown states remain unsafe candidates.

| Backend | Replication control requirements | Dynamic persistence requirements |
|---|---|---|
| Dragonfly | At least 1.9.0; dynamic `masterauth`, `REPLICAOF` and actual replication state | Dynamic `snapshot_cron` and `dbfilename`; validate `dir` without modifying it; strictly advancing `LASTSAVE` |
| Redis, Valkey, Redict | `INFO replication`, `FAILOVER TO`, `REPLICAOF`, dynamic `masteruser/masterauth` | Dynamic `save/appendonly/appendfsync`; `BGSAVE SCHEDULE`; advancing `rdb_saves` and successful save status |
| KeyDB | Single-primary replication, planned switching and dynamic replication credentials; no active-replica/multi-master | Dynamic RDB/AOF; backend time boundary and strictly advancing `LASTSAVE` |
| Kvrocks | `INFO replication`, `CLIENT PAUSE ... WRITE`, `CLIENT UNPAUSE`, `SLAVEOF` | Per-node RocksDB/WAL; require `ha.persistence.enabled: false` |

Garnet, Pika and Tendis are not admitted to `replication-ha`. Native Cluster owns its replication and failover; NARIX's Cluster data plane does not provide managed replication control.

Older servers without `FAILOVER TO` cannot perform planned switches requiring it. Servers without `rdb_saves` or dynamic snapshot settings cannot perform orchestration requiring that evidence. Missing capabilities, insufficient permissions, placeholders and unverifiable results are not successful actions. Preserve protection according to routing and `fail_closed`; never publish an unconfirmed topology.

## Roles and control authority

Redis-family candidate eligibility uses role, upstream address, link state, synchronization state, lag and offset. A primary's advertised replica addresses must match `replication_addr`. `client_addr` is for proxy access; `replication_addr` is for backend replication. Control credentials need probing, replication and configuration permissions; frontend business authentication is separate.

Candidates cannot be synchronizing, have unknown lag or follow the wrong upstream. `ha.max_replica_lag: 0` removes only the numerical limit for known lag. Persistence owners must have a healthy replication link; a disconnected replica is not verifiable responsibility.

Only one effective writing control plane may manage a backend group. Process overlap and multiple hosts or instances require coordination matching that scope. Two members require 2/2; three or more use a majority. Never shrink static membership to the currently surviving nodes.

Confirmed failure, an unconfirmed new primary or multiple-primary evidence closes routing first. Every external write and verification loop rechecks a lease or action token; losing authority stops further actions. Commit results before publishing local safe routes. An absent token-bound target cannot be silently replaced.

`manual` fencing is not physical-isolation evidence. Reliable isolation uses `external-command` with infrastructure that enforces the effect and rejects stale tokens. Terminating the direct child does not prove that remote operations it launched were cancelled. Failed isolation, timeout or lost authority must prevent promotion.

An `observer` only follows safe topology, issues no backend control writes and does not join the writing control plane's coordination cluster. Business readonly access still requires `read_only: true`. See [operations](operations.en.md) for risk tracking and recovery.

## Persistence handoff

Enable new owners and verify checkpoints, disable all old owners, commit the coordinated result, then publish topology. Missing old owners do not count as successfully disabled. Externally modified persistence settings are reread and converged.

A `proxy-cluster` configuration transaction first drains ordinary local HA actions, then holds exclusive local control permission from candidate preparation through activation. Remote prepared candidates also freeze old-generation control entry points. Ordinary convergence and manual actions cannot rebuild owners from the old topology during that interval. This permission orders execution only: every external side effect still requires valid leadership and an action token.

Redis, Valkey and Redict require `rdb_saves` to advance from its pre-trigger value, the background save to finish and its status to indicate success. KeyDB and Dragonfly carry a `LASTSAVE` marker through the current call chain and require strict advancement; historical snapshots cannot prove the current handoff. Operators own clock, disk, directory permissions and backups.

Dynamic persistence does not guarantee zero data loss; asynchronous replication retains an RPO window. With one eligible replica, it owns snapshots and takeover responsibility. At least two eligible replicas are needed to separate a snapshot-free hot standby from snapshot owners.

## Versions, images and modules

Pin backend versions, image digests and architecture, retaining compatible data directories and storage formats. Upgrades must preserve the commands, arguments, fields and side effects required here. A proxy cannot bypass hosted-service permissions, network/TLS restrictions or replication limits.

Modules require native implementation and runtime dependencies. Presence in `COMMAND` does not establish every argument or feature. See the [command matrix](../command-matrix.en.md) for routing, compatibility translations and explicit rejection.
