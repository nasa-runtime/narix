# Current capabilities and contracts

[简体中文](CHANGELOG.md)

NARIX provides a RESP2/RESP3 proxy, a managed replication HA control plane and a readonly query endpoint for development and operations. Dragonfly HA is its standout mode, with controlled promotion, safe routing, replica convergence and persistence responsibility management.

- [Architecture and backend version limits](README.en.md) describes the five proxy modes, separate HA and persistence version floors, and native capability requirements.
- [Dragonfly HA](README.en.md#dragonfly-ha-the-standout-mode) explains the relationship between control authority, data routing and persistence handoff.
- [Configuration](docs/configuration.en.md) covers sources, environment variables, authentication, runtime facilities and time budgets.
- [Operations](docs/operations.en.md) covers protected states, authority, persistence responsibility and transactional publication.
- [Command matrix](command-matrix.en.md) describes routing, session semantics and readonly rejection.

Git commits retain code history. These documents describe the current supported contract.
