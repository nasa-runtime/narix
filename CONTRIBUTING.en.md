# Contributing

[简体中文](CONTRIBUTING.md)

## Development checks

Before submitting changes, run formatting, compilation and static checks appropriate to the change. See [Build and start](README.en.md#build-and-start). README and configuration documentation define the runtime architecture and mode boundaries.

Maintainers perform functional, stability and security checks appropriate to the change before distributing artifacts.

## Change requirements

- Keep changes within the intended mode: standalone, sentinel, cluster, cluster-node-proxy or replication-ha.
- Never silently route unknown Cluster commands by guessing key positions.
- Preserve frontend pipeline response order.
- Preserve isolation between proxy entries for limits, pools, queues, metrics and readonly policy.
- Do not introduce a path around `read_only=true`. Unknown commands and commands requiring a transparent tunnel must fail closed in readonly mode.
- New HA management endpoints and backend side-effect paths must use the runtime retirement gate: `ensure_not_retired` and the shared retirement token in the control client. Include coverage for retirement races. Old generations must not gain a new path to continue modifying backends after replacement.
