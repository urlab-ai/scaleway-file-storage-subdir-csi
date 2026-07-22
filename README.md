# Scaleway File Storage Subdirectory CSI Driver

This project is building a Kubernetes CSI driver that exposes isolated logical
RWX volumes as subdirectories of a small, explicit pool of existing Scaleway
File Storage filesystems.

This is a community project and is not an official Scaleway product. Created by
URLab and released under the MIT license.

## Development status

The repository is under active development and is not yet a qualified public
production release. Its controller, node, provider, mount, recovery, and
operator implementations are wired for code review and controlled local or
staging qualification. The normative behavior and safety contract are defined in
[`docs/SPECIFICATION.md`](docs/SPECIFICATION.md).

Supporting review and operations material:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md)
- [`docs/ALERTS.md`](docs/ALERTS.md)

The public source, module, CSI, and artifact identities are frozen as:

- `github.com/urlab-ai/scaleway-file-storage-subdir-csi`;
- `file-storage-subdir.csi.urlab.ai`;
- `ghcr.io/urlab-ai/scaleway-file-storage-subdir-csi`;
- `oci://ghcr.io/urlab-ai/charts/scaleway-sfs-subdir-csi`.

The frozen candidates `v0.1.0-rc.1` through `v0.1.0-rc.10` are superseded and
must not be promoted. The seventh candidate reached real Kapsule provisioning,
installed the chart, and created its first logical volume, then proved that
Scaleway File Storage `virtiofs` rejects directory
`renameat2(RENAME_NOREPLACE)` and exposed an incorrect PVC-count expression in
the smoke harness. The eighth candidate then proved that the narrowly scoped
descriptor-relative compatibility path safely resumes a prepared `Deleting`
allocation and completes the real archive, but review before the ten-PVC
scenario found a second copy of the same incorrect count expression. Both
count sites and the regression test are now corrected. The public rc.10
artifacts were not production-qualified; review of the deeper E2E runner found
and fixed its ambient-kubeconfig, nil-stdin, and premature-qualification-gate
defects before a billable release-candidate run. The next candidate will be
`v0.1.0-rc.11`; publication remains blocked until that exact candidate has
concrete Linux, kind, CSI, Helm, Kapsule, and final-cleanup evidence.
`POP2-HM-2C-16G` is the sole proposed commercial type for the first controlled
run because it is the lowest-priced currently documented type with two File
Storage slots. It is not supported or advertised until retained real-provider
evidence is complete.

`TEST-TYPE-1` in the development values is a synthetic validation fixture, not
a supported Scaleway Instance type. A release must replace it with the sorted
real-E2E-qualified list embedded in both binaries and recorded in their checked
identity sidecars. Runtime startup rejects any production Helm list that differs
from that binary identity, even if the live API reports a positive attachment
limit.

The Helm chart now renders the intended controller, node plugin, CSI sidecars,
RBAC, CSIDriver, StorageClasses, probes, metrics endpoint, and exact mount
hostPaths for policy review. Its default values are deliberately synthetic and
the source chart rejects `release.mode=production`. Release preparation promotes
an exact copy with the frozen CSI identity and immutable driver/sidecar digests;
rendering the source development chart is not an installation procedure.

The driver executable validates the closed controller/node flag set, loads the
exact Helm-rendered runtime projection, and assembles the production CSI,
Kubernetes, Scaleway, mount, leadership, recovery, metrics, and admin runtime
adapters. The versioned `csi-admin` surface includes checkpoint export/restore,
manual GC, upgrade preflight, audited target-parent decommission, and safe
uninstall. The repository also contains a disposable, development-only `kind`
endpoint and chart-install harness that exercises real kubelet bind mounts,
sidecar registration, PVC lifecycle, controller/node restarts, and cleanup
without contacting Scaleway. This functional completeness does not replace the
privileged Linux, Kapsule, real `virtiofs`, supported-version, cost-cleanup, and
exact release-artifact evidence required for the eventual release candidate.

## Safety model

The driver is designed to fail closed when parent ownership, immutable volume
mapping, provider attachment state, mount identity, or destructive path safety
cannot be proven. PVC sizes are logical reservations used for pool accounting;
they are not hard filesystem quotas in v1.

## Development

Prerequisites currently include Go 1.26, Helm 3.18, and `jq` for validating the
closed runtime JSON rendered by Helm.

The in-cluster v1 binaries target Linux `amd64` and Linux `arm64`. Both are
cross-compiled in CI; destructive filesystem behavior still requires the
privileged Linux and real `virtiofs` release suites defined by the specification.
Nodes must expose `statx(STATX_MNT_ID_UNIQUE)` (normally Linux 6.8 or a kernel
with that primitive backported), `open_tree`, `move_mount`, `mount_setattr`,
`fsopen`, `fsconfig`, and `fsmount`.
The runtime security profile must allow those syscalls plus procfs mount-FD
identity reads. Startup exercises the complete protocol inside the chart's
private, non-propagated mount-quarantine `emptyDir`; it does not wait until the
first cleanup to discover an incompatible kernel. The driver deliberately
refuses reusable mountinfo IDs or pathname-only checks as authority to unmount
or roll back a target.

The v1 mount-safety threat model covers kubelet, CSI concurrency, process
crashes, retries, and cooperating driver generations. An unrelated process
with node-root mount privileges is outside that model: root already has direct
authority to replace or unmount every workload mount on the node.

```bash
make fmt-check
make test
make test-race
make test-csi-sanity
make vet
make test-linux-cross-compile
make test-release-binaries
make test-release-manifest
make test-install-preflight
make helm-lint
make helm-test
make docker-build
make test-kind
```

`make helm-test` renders the chart, checks its security and ownership policy,
and proves that unsafe cross-field values are rejected. It does not contact a
Kubernetes cluster.

A future release installation must run
[`hack/install-preflight.sh`](hack/install-preflight.sh) before Helm. The
operator-side command verifies effective privileged Pod admission with a
non-persistent server-side dry-run, checks the required external Secret key
names selected by the Helm values without printing values, and reads the exact Kapsule cluster to require
the cluster-level `scw-filestorage-csi` tag, matching Project, and region. A tag
applied only to the node pool is not sufficient. See the
[operations guide](docs/OPERATIONS.md#release-installation-prerequisites) for
the complete invocation.

`make test-kind` downloads the checksum-pinned kind v0.32.0 binary when needed,
creates one disposable Kubernetes 1.35 cluster, builds the separate
`Dockerfile.kind` image, and always removes the cluster. It performs no
Scaleway API call. `make test-linux-privileged` is a separate Linux-root gate
for a private mount namespace and is not runnable on macOS.

Release tooling keeps the human tag separate from CSI identity. For example,
`RELEASE_TAG=v1.2.3 VERSION=1.2.3` names artifacts with `v1.2.3` while both
binaries report strict unprefixed SemVer `1.2.3`. A complete Git object ID and
canonical UTC build timestamp are also mandatory; development placeholders
cannot produce release artifacts. The same deterministic build emits SHA-256
checksums, an SPDX 2.3 SBOM, and unsigned SLSA provenance subjects for all four
binaries and their identity/module sidecars. Repository coordinates are an
explicit required input; the tool never invents a public URL, and local output
identifies the local evidence generator rather than claiming a CI builder.
The repository intentionally contains only one GitHub Actions workflow:
[`ci.yaml`](.github/workflows/ci.yaml). It runs local source, CSI, Linux, Helm,
container-build, and disposable `kind` checks. It has no Scaleway credentials,
does not call a Scaleway API, and does not publish release artifacts. Release
preparation and publication remain explicit operator actions in v1; this keeps
the automation small and prevents a repository workflow from provisioning or
deleting billable cloud resources.
`make test-release-manifest` independently proves that a release-candidate chart
can render only the closed driver/sidecar set by immutable digest. The public
coordinates above are fixed; the tool still never publishes artifacts by
itself.

Before using a published operator binary, download its matching release
checksum manifest and every file listed by that manifest into an otherwise
empty directory, verify the whole manifest, and only then inspect the binary's
embedded version. For example:

```bash
sha256sum --check checksums_v1.2.3.txt
./csi-admin_v1.2.3_linux_amd64 version
```

On macOS, use `shasum -a 256 --check checksums_v1.2.3.txt`. The manifest also
covers the generated SBOM and unsigned provenance statement. Checksums and
unsigned subjects do not replace signed attestation or verification that the
Git commit, chart, driver image, sidecar digests, and `csi-admin` version all
belong to the same qualified release.

No real Scaleway resource may be created, changed, detached, resized, stopped,
or deleted without an explicit approved E2E plan immediately before the action.
No GitHub Actions workflow invokes the live runner. Real qualification is an
operator-controlled command run only from a dedicated test environment after
the immediate approval described below.
The development-only `scaleway-e2e-plan` and `scaleway-e2e-cleanup --dry-run`
commands remain non-authorizing review tools. The separate
`scaleway-e2e-run` command is dry-run by default, requires the complete run ID
for live execution or cleanup, journals exact IDs, and fails closed on any
ownership or cleanup ambiguity. Its existence is not cloud evidence; see [the
operations guide](docs/OPERATIONS.md#real-e2e-planning-execution-and-cleanup).
Cleanup refuses a missing retained ledger and accepts Kubernetes/Helm cleanup
preconditions only from a completed structured `csi-admin` safe-uninstall
audit. It never converts an unavailable API or absent local file into success.
The `base` profile can execute the fixed, explicitly non-qualifying first smoke
matrix: two nodes, two product-minimum 25 GB parents, ten logical PVCs,
cross-node RWX, isolation and archive, controller replacement, provider
attachment inventory bounded to the two parents and two nodes, safe uninstall,
and exact cleanup. Its evidence contains
`releaseQualified=false` and is rejected by release promotion. The
`release-candidate` profile executes only after every scenario in the bounded
production matrix has structured proof validation; any remaining blocker makes
it refuse before credentials or provider mutation. That implementation
interlock is currently clear; this authorizes a controlled qualification run,
not release promotion. The run includes the exact 100-PVC multiplex check and
a 20-minute checksum correctness soak across controller and node-plugin
restarts. `--cleanup-only` remains
available for an already approved retained run.
