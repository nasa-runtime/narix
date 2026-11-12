# Container deployment and image distribution

[简体中文](container-deployment.md)

The image repository is `nasaruntime/narix`, with version tag `1.0.0`. See the [Docker Hub overview](../docker/README.en.md). A version tag selects a release; a digest pins content, while `latest` may change. Target platforms are `linux/amd64` and `linux/arm64`.

## Configuration and startup

The image runs `/app/narix` from `/app` as UID/GID `10001:10001`. Its bundled `zcf/application.yml` contains bootstrap defaults without usable business credentials or proxy entries. The image defaults to `APP_CONFIG_FILE=/etc/narix/narix.yml`; override the environment variable to select a different deployment file. `--config` overrides it; `NARIX_CONFIG` remains a lower-priority compatibility input; clear the image's `APP_CONFIG_FILE` to use it alone.

This minimal example targets a fixed Redis backend. Replace the address and credentials. For Dragonfly HA, start with the [complete HA configuration](configuration/replication-ha-dragonfly.yml) or [coordinator-cluster configuration](configuration/replication-ha-dragonfly-proxy-cluster.yml).

```yaml
application:
  name: narix
admin:
  listen: 0.0.0.0:18080
  token: replace-with-a-long-random-admin-token
reload:
  enabled: true
  poll_interval_ms: 1000
proxies:
  - name: redis-primary
    backend_profile: redis
    mode: standalone
    listen_host: 0.0.0.0
    listen: 6379
    host: redis
    port: 6379
    auth_mode: terminate
    frontend_password: replace-with-a-long-random-client-password
```

Add `username` and `password` if the backend requires authentication. The example hostname `redis` must resolve inside the container network. For another container, attach both backend and NARIX to a dedicated Docker network. For a host service, use an address supported by the deployment environment, not the container loopback address.

Save the file as `deploy/standalone/config/narix.yml` and use the appropriate [scenario command](../docker/README.en.md#commands-by-scenario). Business ports are published, while administration is limited to host loopback; restrict business access to a controlled network. Cluster node addresses, node-proxy advertisements and redirects must be reachable by their respective clients. A single port mapping does not replace topology planning.

Mount the complete configuration directory, with traversal and read permission for UID 10001. Files may contain passwords: restrict access to the runtime user or a controlled group, and never embed them in image layers, build arguments or labels. Update files through atomic rename within the same directory. Single-file bind mounts and Kubernetes `subPath` can retain the old inode and are unsuitable for this reload method.

The selected path is fixed at startup. Missing files, malformed YAML, invalid types or polling intervals, and failed resource preparation reject the entire candidate while preserving the active configuration. Environment or `APP_CONFIG_FILE` path changes require container recreation. Unshadowed fields can reload through polling, SIGHUP or authenticated `/config/reload` requests. HA changes also pass domain safety gates; see the [configuration reference](configuration.en.md).

## Logs, state and volumes

Examples retain the file-log mount; explicitly setting `NARIX_LOG_PATH=` selects console-only logging. For files, set `NARIX_LOG_PATH='/usr/local/logs/${application.name}'` and mount a dedicated writable directory at `/usr/local/logs/narix`. Do not let the host shell expand `${application.name}`. For example, `/srv/logs/narix` can be the host side of this mount. The process sees container paths, not unmounted host absolute paths.

Dedicated log and state volumes should belong to the runtime user or a controlled group. Do not recursively change ownership of shared host directories. Defaults are 100MB per log file, 1GB of archives and seven days of retention. Also bound the platform's stdout logs.

| Purpose | Mount requirement |
| --- | --- |
| Deployment configuration, certificates and keys | Read-only directory, readable by UID 10001 |
| File logs | Dedicated writable directory retained in the examples |
| `coordination.data_dir` | Independent persistent volume for each member |
| `ha.coordinator.lease_path` | Writable mount for the configured coordination file; same-host coordination only |
| `/etc/localtime` | Read-only host time-zone file; the source must exist |
| Dragonfly snapshots | Storage on Dragonfly data containers; NARIX only orchestrates responsibility |

Checkpoints belong to backend-native persistence and its data-container storage. NARIX coordination snapshots and logs live in `coordination.data_dir`. Check all configured paths before starting; log-directory access alone is insufficient. Multiple controllers managing the same backends require shared coordination. `coordinator: none` allows only one active control process. Prefer at least three fixed `proxy-cluster` members for multi-host production, with mTLS and certificate identity binding; expose the configured coordination port separately. Programs, credentials or controlled agents required for reliable fencing must exist in the runtime environment. The image does not bundle cloud-specific fencing tools.

## Health and shutdown

Use `/health` for liveness and `/ready` for readiness and load balancing. `/status`, `/metrics` and configuration APIs require a Bearer token. A TCP connection proves a listener exists, not that an HA write route is safe. The image does not hard-code a Docker HEALTHCHECK that assumes an administration address or an extra HTTP client; configure platform probes for the actual listener.

Shutdown budget is 15 seconds for an empty proxy list, or `max(10000, largest graceful_shutdown_timeout_ms) + 5000` milliseconds otherwise. `--stop-timeout 70` or Kubernetes `terminationGracePeriodSeconds: 70` must cover the application budget with margin; increase both for longer business drain timeouts. SIGTERM closes admission before draining. A timeout or resource-shutdown failure produces a nonzero exit and is not successful completion.

## Build and publish to Docker Hub

Build for the host architecture from the repository root:

```sh
docker build --file docker/Dockerfile --tag narix:1.0.0 .
docker run --rm narix:1.0.0 --version
```

The Dockerfile pins base-image digests, resolves dependencies from crates.io using `Cargo.lock`, retains standard release optimization with thin LTO and one codegen unit, then strips symbols. The runtime excludes build caches. `.dockerignore` admits only product build inputs; never place deployment credentials in these paths.

Public distribution follows this order: commit and merge the target branch, push, wait for all remote CI checks, verify branch and version-tag SHAs, obtain explicit publication authorization, then upload. Do not publish uncommitted product changes. `master`, `release` and the version tag should identify the same release commit. The CI SHA must match the image's `org.opencontainers.image.revision`.

Log in with an account authorized to write the image repository, and place the verified SHA in `SOURCE_REVISION`. Do not put access tokens in command arguments, files or the build context.

```sh
docker login
export SOURCE_REVISION=replace-with-the-full-verified-git-sha
docker buildx build --file docker/Dockerfile \
  --platform linux/amd64,linux/arm64 \
  --build-arg APP_VERSION=1.0.0 \
  --build-arg SOURCE_REVISION="$SOURCE_REVISION" \
  --tag nasaruntime/narix:1.0.0 \
  --provenance=true --sbom=true --push .
docker buildx imagetools inspect nasaruntime/narix:1.0.0
```

Verify both platform images, including startup, configuration loading, labels, license and runtime requirements. A successful run on one architecture does not establish the other. Provenance/SBOM can add attestation descriptors that are not runnable platforms. If a version tag already exists with different content, stop and choose a new patch version instead of replacing it. Move `latest` only after validating the versioned image:

```sh
docker buildx imagetools create \
  --tag nasaruntime/narix:latest nasaruntime/narix:1.0.0
docker buildx imagetools inspect nasaruntime/narix:latest
```

Use [docker/README.en.md](../docker/README.en.md) or the [Chinese overview](../docker/README.md) as the Docker Hub description, within 25000 UTF-8 bytes. Suggested short description: `High-performance RESP2/RESP3 proxy featuring Dragonfly HA`. Read back the repository description, version manifest, platform labels and digests after upload, checking source links and configuration instructions against the actual image. These commands describe publication; they do not assert that an image has already been uploaded.
