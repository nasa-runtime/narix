# Deployment delivery checklist

[简体中文](release-checklist.md)

Deployments must satisfy the [version and mode contract](../README.en.md#supported-backends-and-versions), [backend control requirements](certification.en.md) and [configuration reference](configuration.en.md).

## Build and runtime materials

- Use complete source and locked crates.io dependencies. Retain Apache-2.0 and applicable third-party licenses.
- Source distributions retain `Cargo.toml`, `Cargo.lock`, build scripts, protocol files, source, bootstrap configuration and documentation. This project retains `publish = false`; NARIX is not uploaded to crates.io.
- Validate external configuration, node-local paths and permissions. File selection follows `--config` → `APP_CONFIG_FILE` → `NARIX_CONFIG`; a selected invalid path must not silently fall back.
- Ensure the log directory is writable and capacity, retention and shutdown budgets match resources. Empty or null paths select console-only output.
- Configure at least one business proxy. Empty `proxies` is only a waiting-for-configuration state with administration and reload both enabled.

## Access and authentication

- Use a strong administration token and restrict network access. Configure frontend authentication and least-privilege backend credentials independently.
- Terminate business-ingress TLS at trusted infrastructure across untrusted networks. Configure backend, Sentinel and coordination gRPC certificates separately.
- Use `read_only: true` for readonly access. `observer` limits control writes, not business commands.
- Size connection, pipeline, tunnel, queue, frame and timeout budgets for file descriptors, backend maxclients and memory.
- Cover topology and redirect addresses in node-proxy mappings. Do not configure Pika/Codis interfaces as standard Cluster.

## HA and persistence

- For Dragonfly, use `backend_profile: dragonfly` and `mode: replication-ha`. Prefer at least three coordination members across hosts; plan data nodes as primary, snapshot-free hot standby and snapshot replicas.
- Allow only one effective writing control plane per backend group. Match coordination to deployment scope; use mTLS and member certificate fingerprints across hosts.
- Retain the complete static membership. Insufficient 2/2 or majority authority keeps protection active; do not temporarily remove absent members.
- Fencing executables must provide real isolation and reject expired actions. `manual` does not prove physical isolation.
- Use `/ready` for load-balancer admission. Monitor roles, replication, protection reasons, leases and action state. TCP establishment alone is not business readiness.
- Enable and verify new owners, disable old owners, commit and then publish. Absence, failed writes or unconfirmed checkpoints preserve protection according to `fail_closed`.
- Kvrocks keeps per-node RocksDB/WAL with NARIX persistence orchestration disabled. Other products still require correct directories, backup/recovery procedures and disk capacity.
- Do not restart or replace configuration to clear evidence of additional primaries before isolation or demotion. Absence is not isolation evidence.

## Configuration changes and shutdown

- Publish reloads only through the configuration transaction. Prepare same-generation logging, credentials, routes and listeners before activation; never open mixed state after failure.
- Align application metadata and managed configuration across members. Keep node-local logging, certificates, addresses and directories local. Mixed executable versions do not automatically share management or timeout semantics.
- Failed local activation of committed configuration remains protected and recovers the committed state; do not roll back consensus facts independently.
- Revoke admission, stop control tasks, drain connections and close logs last. Nonzero exit or shutdown timeout is not normal drain completion.
- Automatic restart policies should distinguish configuration, startup and runtime failure. Preserve role, coordination and persistence facts during maintenance for safe recovery.
