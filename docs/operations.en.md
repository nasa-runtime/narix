# Operations

[简体中文](operations.md)

## Administration endpoints

```text
GET /health
GET /ready
GET /status
GET /metrics
GET /config/reload
POST /config/validate
POST /config/reload
GET /ha/topology
GET /ha/master
GET /ha/replicas
POST /ha/failover
POST /ha/switchover
POST /ha/node/{node}/promote
POST /ha/node/{node}/replicaof/{master}
POST /ha/node/{node}/fence
```

Except `/health` and `/ready`, requests need `Authorization: Bearer <admin.token>` or `X-Admin-Token: <token>`, unless `admin.allow_unauthenticated: true` is explicitly configured.

Dragonfly persistence passes the `LASTSAVE` token returned by `force_checkpoint` unchanged into verification for the same call; only a strictly advancing marker can establish new ownership. KeyDB likewise carries `LASTSAVE`, first waiting for a backend second boundary and then issuing `BGSAVE SCHEDULE`. A short probe interruption caused by `SAVE` retains verified topology within `probe_interval_ms * master_down_after_failures`; exceeding that grace period degrades persistence responsibility. This prevents the checkpoint itself from repeatedly moving owners.

`POST /ha/switchover` shares candidate gates and a final unique-primary requirement, but native intermediate states differ. Redis, Valkey, Redict and KeyDB use `FAILOVER TO` coordinated by the old primary. Dragonfly promotes the candidate, verifies it, then demotes the old primary, allowing a controlled intermediate dual-primary interval. Kvrocks pauses old-primary writes, confirms the RocksDB sequence offset is caught up and executes multiple `SLAVEOF` steps. Pause non-idempotent bulk work during changes and confirm final primary status.

## Configuration reload

`POST /config/validate` checks a candidate without publishing. `POST /config/reload` publishes; `GET /config/reload` exposes revision, digest, differences, recent result and the number of members still unconverged when the most recent cluster commit returned. File checks, SIGHUP and management requests use the same serialized transaction.

Single-node publication prepares all candidate generations after listener/backend preflight. Listeners without address conflicts bind in advance but remain behind activation admission. Reused addresses bind only after old listeners release them. Publication briefly rejects new connections on all business listeners. A stable listener stops and waits for old HA, Sentinel or Cluster control tasks before activating the replacement, preventing old configuration from continuing control writes. Global admission reopens only after the supervisor confirms candidate registration and accept-task installation.

Listener release is separate from old business-connection drain: old connections finish in the background after admission reopens, so their maximum drain time is not added to the global publication window. Old control-task shutdown is also bounded. Convergence loops cancel in-flight actions on shutdown without publishing partial state. HA partial actions are handled by the next generation's idempotent convergence; Sentinel retains its last fully validated safe route until rediscovery; Cluster retains its last complete slot map until refresh.

In-flight management calls holding an old HA handle are rejected after retirement, with `retired` in the error. Shared retirement tokens are also checked around each adapter control command so a multi-command sequence cannot continue. Reissue the operation against the new generation. Incomplete rollback keeps admission closed.

Deleting a current HA primary first switches to a healthy replica retained by the candidate. Removing a persistence owner first moves responsibility to retained nodes and verifies a checkpoint. Disabling orchestration first disables all backend responsibilities. Candidate listeners may already accept TCP establishment, but business handling is not active and early connections are closed. Use business-level readiness, such as `PING`, rather than TCP establishment alone for a candidate listener.

Deleting an entire HA proxy or leaving `replication-ha` has the same prerequisite as disabling dynamic persistence: disable every known backend responsibility. An unreachable removed node or prior owner cannot be assumed disabled. Reject before commit and retain the old configuration rather than treating omission from the candidate as proof.

For a coordination cluster, edit only one member:

1. Any member forwards the candidate to the leader through gRPC. The leader drains ordinary local HA actions and holds exclusive local control permission from preparation through activation. Ordinary convergence and manual actions cannot enter while it is held. Leadership, lease and token checks still apply.
2. The leader and reachable members persist and preflight concurrently. A member confirms prepare only after preparation and freezing its publication gate. From prepare to activate/abort, new business connections are rejected. Clients need reconnect with backoff; `reload.transaction_timeout_ms` bounds the freeze window.
3. After a static majority prepares, the leader performs required HA safety precommit and asks available members to preflight again. Periodic guards cannot reopen the old generation. Remote prepared members cannot issue control actions using old configuration.
4. A majority of second preflights permits committing revision, digest and normalized body. Prepared members publish concurrently; others remain not ready and recover from the replicated state machine.
5. Members write committed bodies under `coordination.data_dir/config/`, with directory permissions `0700` and file permissions `0600`. Explicit precommit abort removes only that transaction's revision/digest candidate and its freeze. A lost leader callback leaves a freeze for at most the transaction timeout; orphan cleanup then reevaluates readiness against current consensus.

Before HA precommit, the transaction timeout bounds the whole operation. Once fencing, switching or persistence handoff starts, HTTP/gRPC caller timeout or disconnection does not cancel external side effects. The leader completes individually bounded steps and explicitly commits or aborts, preventing an abort response followed by an uncontrolled late switch. Query configuration status and cluster revision after disconnection.

A three-member cluster updates with one offline member. A stale member restarting from an old file cannot open administration or business service early: it confirms the leader's latest head, catches up, writes and publishes the committed revision first. File checks then suppress the old source digest so it cannot propose stale configuration automatically.

Transient file-driven preflight/publication failure retries the same digest with exponential backoff from one second to 30 seconds. Changed file content resets failures immediately. This is separate from stale-source suppression after cluster recovery, which persists until the local old file changes.

Before consensus commit, minority availability, failed preflight, disk synchronization failure or no safe HA migration target rejects the candidate and retains the old revision. After commit, a member's activation failure cannot roll back the revision; it stays not ready and retries convergence.

Periodic consistency checks compare revision/digest only. HA probes read the target group state rather than copying complete configuration. Only detected lag triggers body loading and recovery.

HA observations and action requests carry their bound global revision and group digest. An old generation cannot vote or acquire actions for a new revision during publication. If an HA proxy name is removed for at least one revision before reintroduction, its former primary/owner lifecycle is cleared; the current leader must confirm the new topology.

## Readonly proxy entries

`read_only: false` is the default fast path. `read_only: true` returns `READONLY` for writes and all Lua/Function execution: `EVAL`, `EVALSHA`, `EVAL_RO`, `EVALSHA_RO`, `FCALL`, `FCALL_RO`. Queries such as `SCRIPT EXISTS` remain available. Script/Function state modifications are rejected, including `SCRIPT LOAD/FLUSH/KILL/DEBUG` and `FUNCTION LOAD/DELETE/FLUSH/RESTORE/KILL`. Unknown and transparent-tunnel commands also fail closed because opaque forwarding cannot enforce per-command readonly policy.

## DedicatedTunnel limits

Pub/Sub, blocking commands, transactions and connection-bound operations use DedicatedTunnel in parsed forwarding modes. Configure active limits, queue capacity and permit timeout per proxy; see [capacity settings](configuration.en.md#connections-capacity-and-timeouts) for defaults and path applicability.

Monitor:

```text
narix_dedicated_tunnel_active
narix_dedicated_tunnel_queue_waiters
narix_dedicated_tunnel_rejections_total
narix_dedicated_tunnel_acquire_timeouts_total
```

## Replication HA protection

Unresolved multiple-primary ambiguity, failed required fencing, lost Redis lease, lost built-in coordination authority, expired action tokens or unsafe strict persistence convergence can protect HA routing. `/ready` becomes unhealthy and new business connections receive a Redis-protocol error instead of being sent to an arbitrary primary.

An online replica not directly following the current primary, including a cascading replica, also triggers protection. Under valid coordination authority, NARIX reassigns it directly to the primary, then requires fresh observations before reopening. Failed convergence backs off by `max(lease_ttl, 10×probe_interval)`, remaining protected without repeatedly issuing replication commands. This is topology convergence and remains active with `ha.auto_failover: false`. To temporarily retain an externally managed or cascading replica, remove it through a valid `nodes` change; otherwise one drifting replica can protect the whole proxy.

Common protection reasons include:

- Multiple primaries without a confirmed safe primary.
- Failed isolation required for automatic switching.
- Failed lease acquisition/renewal during failover.
- Loss of a pair peer or majority authority.
- Observed primary disagreeing with the committed primary.
- No replica with usable replication evidence after primary failure. Unknown disconnection duration, full synchronization, unknown lag or unconfirmed upstream excludes a candidate. Known disconnection age is a lower bound for the lag gate.
- Lost action authority during fencing, promotion, convergence or persistence handoff.
- Unsafe persistence convergence with `ha.persistence.fail_closed=true`.

Fencing (`ha.fencing.mode`) and coordination (`ha.coordinator`) have different responsibilities. `manual` fencing is a soft no-op followed by `REPLICAOF` demotion; old-primary recovery before demotion can permit dual writes. Cluster deployments wait a full `route_guard_timeout` interval before promotion so partitioned members close old routing. `external-command` physically isolates the old backend before promotion; its safety depends on real infrastructure effects and stale-token rejection. Coordination serializes NARIX control actions. `none` only covers one process, while multi-instance deployments need suitable `proxy-cluster`, independent Redis or same-host `local-file` coordination. With `none`, exactly one control-plane process may be online for a backend group; same-host blue/green overlap requires explicit `local-file` coordination. Omitted mode follows `coordination.enabled`; an inconsistent explicit mode is rejected.

After confirmed primary failure without an eligible candidate, such as during full synchronization, protect the data plane and retain `lastError`. Restore an eligible replica, then await automatic recovery or explicitly select it with `/ha/node/{node}/promote`. Manual promotion cannot bypass eligibility or fencing. Replicas disconnected because the primary failed participate using known disconnection age. Startup/reload with no visible primary constructs a protected listener and lets failure confirmation drive recovery; construction itself never promotes.

Every coordinator mode closes routing on multiple-primary evidence. If the current safe primary remains known, in-process/file/Redis coordination must obtain and continuously recheck authority before demoting additional primaries. Only the `proxy-cluster` leader can converge them under an action token; followers stay protected. Additional-primary risks survive probe rounds even when a node disappears. Reopening requires that each risk node proves replica status, completes authorized demotion verification, or is physically fenced by successful `external-command` under still-valid authority. Manual fencing is not that evidence.

If a risk node becomes the accepted current primary through demotion evidence, committed outcome, restricted following or authorized management, only its own additional-primary risk is cleared. Other absent risks still block every Serving publication path. Observers keep the same local risk accounting without control writes. Failed demotion, lost authority or uncommitted results prevent reopening and use `max(lease_ttl, 10×probe_interval)` backoff. Disabling automatic failover still protects confirmed primary failure; the original primary's return as the unique primary can restore service.

The additional-primary set is runtime-generation evidence, not durable state across restart or replacement. Configuration precommit refuses to remove/replace a generation holding such evidence, so reload cannot silently clear it. Stateless `POST /config/validate` success does not imply publication is permitted. Do not restart or roll a protected process back into traffic to erase this evidence. Restore every risk node as a replica of the target primary. For permanently unreachable nodes, only successful `/ha/node/{node}/fence` with `external-command` and authority revalidation supplies physical-isolation evidence. In manual mode, an independent controller must confirm isolation/demotion before a controlled restart. Restart does not preserve the risk set.

Manual actions and automatic failover share authority checks around fencing, promotion, replication writes and convergence. Adapters recheck each native command and confirmation loop, not just the outer method. Redis-family and Dragonfly persistence updates restore earlier settings on partial failure only while authority remains valid. During `external-command`, authority is rechecked concurrently every 50 ms; loss immediately protects routing and terminates the direct child. Detached infrastructure work must independently enforce idempotency and token expiry. Promotion first fences the old primary and refuses promotion if that fails. File/Redis outcomes publish atomically under the owner condition; `proxy-cluster` commits `CompleteAction`. Only committed success opens the new local route.

Strict persistence startup stays protected until owners, roles, links and settings are verified. Publication follows new-owner verification, complete old-owner disablement and coordinated commit. Previously known owners remain in the disablement set even if absent; status retains the last verified topology during failure. Each round rereads actual owner/non-owner settings, even on a local topology-cache hit, and reacquires authority to converge drift. A verified replica may briefly miss role probes within `probe_interval_ms × master_down_after_failures`; that bounded grace retains prior owner/link evidence without sending `REPLICAOF`, then returns to strict verification. With `force_checkpoint_on_handoff`, file/Redis process restart reacquires authority and creates a verifiable checkpoint rather than adopting owners from settings alone. Cluster followers adopt remote persistence only with a committed complete owner/link fingerprint in `CompleteAction` and no active action.

`auto_failover: false` disables NARIX-initiated primary promotion, not topology convergence. For an external switch, demote the old primary to follow the intended new primary before promoting that new primary. NARIX immediately follows only with a unique new primary plus explicit old-primary following evidence. Mere old-primary unreachability is not isolation proof. Confirmed failure protects first; automatic failover, if enabled, uses normal coordination/fencing. Without automatic promotion, including observers, a permanently absent old primary permits restricted following only after a unique successor remains stable for `master_down_after_failures` observations. NARIX management APIs can coordinate the sequence through one control plane.

## Observer nodes (`ha.observer`)

`ha.observer: true` probes and follows topology but never issues backend control commands: no promotion, old-primary demotion, replica reassignment or persistence orchestration. All manual topology endpoints listed above reject requests. It implies `auto_failover=false`, and status exposes `observer: true`.

A production controller can coexist with separate observer query endpoints. Confirmed old-primary demotion permits immediate following; permanently absent old primaries use the restricted path. Multiple primaries always protect routing without interfering with production control. This is the only exception to one writing control plane per backend group. Business commands are not automatically readonly: also set `read_only: true`. Startup requires no coordination-cluster participation, `coordinator=none`, persistence disabled and `fencing.mode=manual`.

Consecutive primary failures close routing and populate `lastError`/`protectionReason`. Recovery requires either an old primary observed as the new primary's direct replica, or confirmed old-primary failure plus a unique successor stable for the same failure threshold. Both consecutive confirmations are necessary for the restricted path, stronger than a single construction-time observation. No-primary cold start remains protected; observers make no control writes throughout.

Deploy according to [version/mode limits](../README.en.md#supported-backends-and-versions) and [native control requirements](certification.en.md). Version numbers alone grant no authority. Missing role, replication, checkpoint or command evidence fails the action and preserves protection. Deployment exercises should verify partition behavior, real fencing effects/timeouts, `/ready` removal, alerts and recovery time. `ha.max_replica_lag: 0` removes only the numerical cap; full synchronization, unknown lag or wrong upstream remains ineligible, and persistence owners never accept disconnected replicas.

All managed replication products use `narix_replication_ha_*` metric names with labels such as `proxy`, `node`, `role`. Use status `backendProfile` to identify the adapter, not a product-specific metric prefix.

Reload status `proxies[].generation` identifies the generation accepting new connections; `drainingGenerations[]` lists older generations still draining. Metrics use `narix_proxy_generation{proxy}` and `narix_proxy_draining_connections{proxy,generation}`. Deleted proxies retain drain metrics until actual remaining connections reach zero.

Dragonfly `dir` is fixed at backend startup. NARIX reads and validates nonempty `ha.persistence.dragonfly.dir`, never attempts an unsupported dynamic change and applies `fail_closed` on mismatch. Identical persistence failures log once and back off at least five seconds before another convergence attempt.

Before recovery, confirm the safe primary, then use administration endpoints or infrastructure controls to perform the necessary actions.

The CPU time metric `narix_process_cpu_seconds_total` preserves fractional seconds and the Prometheus counter type at `/metrics` and the unified text exporter. The current structured metrics interface supports integer counters only, so this floating-point counter is omitted from structured snapshots. Other metrics remain structured; CPU seconds are neither rounded nor reclassified as a gauge.
