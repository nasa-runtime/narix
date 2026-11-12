# Security policy

[简体中文](SECURITY.md)

## Supported version

The supported version is `1.0.0`; security updates apply to the maintained branch. Current configuration and operations contracts define authentication, readonly rejection and coordination authority.

## Reporting

Report security issues privately to the maintainers. Do not disclose the following in public issues:

- Authentication bypasses.
- `read_only` bypasses.
- Routing that can write to the wrong backend data source.
- Split brain during failover or bypassed fencing.
- Secrets exposed through logs or metrics.
- Denial-of-service paths caused by unbounded buffers, tasks or connections.

## Security defaults

- Administration APIs require a token. Set `admin.allow_unauthenticated: true` only for local or trusted isolated networks.
- `ha.auto_failover` defaults to enabled. A single instance serializes actions with an in-process lock, and only one control-plane process may be active for a backend group. `ha.observer: true` is the sole exception because observers issue no backend control commands. Multiple active controllers require coordination; the built-in cluster requires `proxy-cluster`. Soft fencing waits a full `route_guard_timeout` before promotion in cluster deployments. Use reliable `fencing.mode=external-command` when physical isolation of the old primary is required.
- `ha.coordinator.mode=local-file` coordinates only same-host processes. Across hosts, prefer majority `proxy-cluster` coordination with mutual TLS; an independent highly available Redis (`ha.coordinator.mode=redis`) can also provide leases.
- `read_only=true` rejects writes and fails closed for unknown commands or tunnel-bound commands.
