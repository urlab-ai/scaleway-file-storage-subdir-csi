# AGENTS.md - Scaleway SFS Subdirectory CSI

## 1. Mission

This repository contains a community Kubernetes CSI driver for provisioning many
logical RWX PersistentVolumes as isolated subdirectories inside a small pool of
existing Scaleway File Storage filesystems.

This is production infrastructure software. It is not a proof of concept, an
MVP, or an experimental code sample. A defect can make workloads unavailable or
damage user data. Work as a senior storage and Kubernetes engineer and keep the
implementation production-grade from the first release.

The project is created by URLab, released under the MIT license, and designed to
remain reusable outside URLab. Do not introduce private URLab code, product
assumptions, paths, domains, registries, or credentials.

## 2. Normative Specification

[`docs/SPECIFICATION.md`](docs/SPECIFICATION.md) is the single source of truth
for behavior, architecture, safety invariants, supported scope, testing, release
requirements, and operational procedures.

Before changing code:

1. Read the complete relevant sections of the specification.
2. Identify the invariants and failure modes affected by the change.
3. Implement the smallest complete solution that satisfies those requirements.
4. Add or update tests that prove the required behavior and failure handling.
5. Re-read the affected specification sections before considering the work done.

The specification and implementation must never knowingly diverge. When a
design, behavior, public contract, failure policy, dependency constraint, or
operational procedure changes, update `docs/SPECIFICATION.md` in the same
logical change. Do not quietly make the code the new source of truth.

If implementation evidence shows that a specified design is unsafe, impossible,
or needlessly complex, stop and explain the conflict. Propose the simplest
production-grade correction and update the specification before or together
with the implementation. Never weaken a safety invariant merely to make a test
pass.

## 3. Engineering Principles

Apply these rules in this order:

1. Data safety and correctness.
2. Robustness and explicit failure handling.
3. Kubernetes and CSI conformance.
4. Simplicity and maintainability.
5. Performance and bounded resource usage.
6. Developer and operator experience.

Keep it simple. Production-grade does not mean adding layers, services, or
abstractions without a demonstrated need. V1 deliberately uses one controller,
ConfigMap-backed allocation records, existing parent filesystems, standard CSI
sidecars, and explicit operator workflows. Do not add controller HA, a CRD, a
database, an automatic parent manager, or a separate control plane unless the
specification is deliberately revised with concrete evidence and tests.

Prefer established Kubernetes, CSI, Go, and Scaleway SDK patterns. Study the
official Scaleway File Storage CSI driver at the reference pinned by the
specification for provider API usage, but do not copy behavior that conflicts
with this driver's subdirectory model or safety contract. Preserve upstream
licenses and notices for any copied code.

## 4. Go and Code Organization

- Use Go and the Go version pinned by the repository.
- Keep packages focused by responsibility: CSI services, provider access,
  mounting, path safety, allocation state, ownership state, coordination,
  recovery, and operator commands must remain clearly separated.
- Avoid large catch-all files and generic `utils` or `helpers` packages.
- Keep interfaces narrow. Introduce an abstraction only when it supports a real
  boundary, deterministic testing, or more than one concrete implementation.
- Pass `context.Context` through I/O and blocking operations. Honor cancellation
  and deadlines while waiting for locks, Kubernetes, Scaleway, mounts, or polls.
- Use typed values and explicit validation for IDs, paths, states, sizes, and
  durable records. Do not use maps of unvalidated strings for domain state.
- Wrap errors with operation and resource context without exposing secrets.
- Never swallow an error or convert an ambiguous result into success.
- Use bounded polling with cancellation, deadlines, backoff, and jitter. Do not
  use fixed sleeps as readiness contracts.
- Keep memory, goroutine counts, work queues, metric labels, and API calls
  bounded by the scale envelope in the specification.
- Preserve the specified lock order and crash-recovery write order. Do not add a
  new lock or durable transition without analyzing cancellation, deadlocks,
  process crashes, and one-sided writes.

Run `gofmt` on every Go change. Exported APIs require idiomatic Go documentation.
Package documentation is mandatory. Avoid unnecessary public symbols.

## 5. In-Code Documentation

All code, comments, docstrings, identifiers, errors, logs, tests, examples, and
documentation must be written in English.

Documentation in code is a first-class requirement:

- document every package's responsibility and trust boundary;
- document exported symbols and durable schemas;
- explain why non-obvious safety checks, write ordering, locking, fencing,
  `fsync`, mount validation, and fail-closed decisions exist;
- document assumptions inherited from CSI, Kubernetes, Linux, `virtiofs`, and
  the pinned Scaleway SDK;
- place comments next to the code whose safety they explain;
- keep comments synchronized when behavior changes.

Be generous with explanations of intent and invariants, but do not narrate
obvious syntax. A future maintainer must be able to understand why removing a
check is dangerous without reconstructing the entire design history.

## 6. Storage Safety Rules

The following are non-negotiable summaries; the specification remains
authoritative:

- Never infer ownership from a directory name alone.
- Never mutate or delete a path unless allocation state, ownership state,
  mapping identity, parent identity, and mount identity satisfy the exact
  specified contract.
- Never follow symlinks or cross mount boundaries during destructive traversal.
- Never unmount a foreign, aliased, replaced, or stacked mount.
- Never mark a filesystem transition complete before its required durability
  barriers succeed.
- Never interpret an unavailable, forbidden, stale, timed-out, or ambiguous read
  as absence.
- Never clear a published-node fence without the specified normal-node evidence
  or conclusive provider fencing.
- Never detach a parent during normal logical-volume unpublish.
- Never reuse a logical volume name or remove permanent tombstones in v1.
- Existing healthy mounts must not be torn down merely because the controller or
  Scaleway API is temporarily unavailable.

Destructive code requires focused unit tests, crash-point tests, and real Linux
mount/filesystem tests where fakes cannot prove kernel behavior.

## 7. Security

- Never commit credentials, kubeconfigs, tokens, project IDs, private endpoints,
  or real resource identifiers.
- Keep Scaleway credentials out of the node plugin and every container that does
  not call authenticated Scaleway APIs.
- Use least-privilege Kubernetes RBAC and the narrowest available Scaleway IAM
  permissions documented by the specification.
- Keep privileged containers, host paths, mount propagation, and Linux
  capabilities limited to the exact mount responsibilities that require them.
- Treat all CSI requests, StorageClass parameters, Kubernetes objects, provider
  responses, filesystem entries, and restored metadata as untrusted input.
- Do not log Secret values, credentials, raw tokens, or workload data.
- Pin release dependencies, images, sidecars, and build artifacts. Do not use
  `latest` or mutable tag-only production references.

## 8. Testing Requirements

Every behavior change requires tests at the lowest useful layer and at every
integration boundary whose behavior cannot be proven below it.

The expected validation stack includes:

- focused unit tests for pure state, validation, hashing, capacity, and error
  mapping;
- fake Kubernetes, Scaleway, clock, and mounter tests for deterministic retries,
  ambiguity, cancellation, and crash windows;
- `go test -race ./...` for concurrency-sensitive code;
- pinned Kubernetes CSI sanity tests against the controller and node sockets;
- Helm lint, schema, rendering, RBAC, security-context, and immutable-image
  tests;
- privileged Linux mount-namespace and filesystem tests for mount identity,
  stacked mounts, symlink races, exact unmount, and durability barriers;
- local `kind` integration tests for Kubernetes wiring and fake-provider flows;
- tightly controlled real Scaleway Kapsule tests for provider and `virtiofs`
  behavior that fakes cannot establish.

Tests must be deterministic, isolated, idempotent where applicable, and explicit
about cleanup. Do not weaken assertions, skip required suites, or replace kernel
or cloud evidence with mocks merely to obtain a green build. A flaky test is a
defect to diagnose, not a reason to add blind retries or sleeps.

At minimum, normal development must run the relevant subset of:

```bash
gofmt -l .
go test ./...
go test -race ./...
go vet ./...
golangci-lint run
helm lint charts/scaleway-sfs-subdir-csi
```

Run the broader gates defined in `docs/SPECIFICATION.md` before claiming release
readiness. If a required check cannot run locally, state exactly what was not
run and why.

## 9. Real Scaleway Tests and Cost Control

Real cloud tests create billable resources and can damage unrelated resources.
An agent must obtain explicit user approval immediately before creating,
resizing, stopping, detaching, or deleting real Scaleway resources, even when
credentials are already available.

Real E2E tooling must enforce all of the following:

- use a dedicated test Project, never a production Project;
- require explicit project ID, region, unique run ID, and non-empty resource
  prefix on every invocation;
- use only release-qualified region and Instance types;
- create the smallest cluster and node pool that can prove the scenario;
- create the minimum parent filesystem size supported by the tested product;
- print the planned resources, estimated hourly cost, destructive operations,
  and cleanup command before creation;
- require a deliberate confirmation flag in addition to cloud credentials;
- tag and name every created resource with the unique run ID and ownership;
- record whether each resource was created by the run or reused;
- never delete a reused, pre-existing, untagged, or ambiguously owned resource;
- support dry-run and idempotent standalone cleanup;
- use exact IDs for cleanup and refuse broad name, Project, or region deletion;
- clean resources after successful tests and after controlled failures;
- print a final inventory proving that run-owned billable resources were
  removed;
- emit the cleanup command and surviving exact resource IDs if automatic cleanup
  cannot complete.

Do not leave a cluster running between test sessions for convenience. Prefer a
small ephemeral cluster, execute the planned scenario once, retain the test
evidence, and destroy the run-owned resources immediately. Reuse is acceptable
only inside one explicitly approved test session when it materially reduces
cost and does not weaken isolation or cleanup proof.

Never run destructive fault-injection scenarios in a production cluster. Tests
that stop or delete Instances, exercise detach, or validate recovery require a
disposable cluster and disposable tagged parents.

## 10. Developer and Operator Experience

- Keep the standard development path documented and reproducible.
- Prefer a small Makefile or focused scripts over long undocumented command
  sequences. Scripts must use strict shell mode and resolve paths from their own
  location.
- Validate configuration before cloud or filesystem mutation and return clear,
  actionable errors.
- Provide dry-run for destructive operator workflows.
- Keep logs structured and useful for diagnosing one operation without exposing
  unbounded values as metric labels.
- Keep Helm defaults safe. Development shortcuts must be explicit and impossible
  to confuse with production support.
- Keep `csi-admin` versioned, checksum-verifiable, and protocol-compatible with
  the matching driver release.
- Update README, examples, troubleshooting, and operations procedures whenever a
  user-visible workflow changes.

## 11. Dependencies and Supply Chain

- Add dependencies only when the standard library or an existing focused
  dependency is insufficient.
- Prefer mature, maintained upstream libraries used by Kubernetes or Scaleway.
- Pin and review dependency versions. Record deliberate compatibility changes.
- Keep generated files reproducible and clearly identified.
- Release images and binaries must be reproducible enough to publish checksums,
  SBOMs, and provenance as required by the specification.
- Preserve license attribution and verify compatibility with MIT distribution.

## 12. Git and Change Discipline

- Keep each change narrowly scoped and reviewable.
- Do not mix unrelated refactors with functional work.
- Never rewrite or discard user changes without explicit approval.
- Do not commit generated credentials, local kubeconfigs, test outputs, or cloud
  inventories containing sensitive identifiers.
- Use clear English commit messages describing the behavior and reason.
- Treat changes to durable schemas, CSI identity, volume handles, context,
  StorageClasses, Helm values, Lease identity, and recovery procedures as
  compatibility changes requiring specification updates and migration analysis.

## 13. Definition of Done

A task is complete only when:

- implementation and `docs/SPECIFICATION.md` agree;
- the solution is complete, production-grade, and no more complex than needed;
- success, failure, retry, cancellation, crash, and idempotency paths are
  handled explicitly;
- relevant unit, race, CSI, Helm, Linux, integration, and cloud tests pass;
- real-cloud cost and cleanup evidence exists when real resources were used;
- code and operational decisions are documented in English;
- security, RBAC, credentials, mount privileges, and path safety were reviewed;
- no required test was silently skipped;
- the final change can be understood and operated by someone who did not author
  it.
