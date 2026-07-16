SHELL := /bin/sh

GO ?= go
HELM ?= helm
JQ ?= jq
GOLANGCI_LINT ?= golangci-lint
VERSION ?= 0.0.0-dev
RELEASE_TAG ?=
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
REPOSITORY_URL ?=
LDFLAGS := -s -w \
	-X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Version=$(VERSION) \
	-X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.Commit=$(COMMIT) \
	-X github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version.BuildDate=$(BUILD_DATE)

.PHONY: all build release-binaries test-release-binaries test-release-manifest verify test test-race test-csi-sanity test-e2e-safety test-install-preflight test-kind test-linux-privileged test-linux-cross-compile vet fmt-check lint helm-lint helm-test docker-build clean

all: build

build:
	mkdir -p bin
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/scaleway-sfs-subdir-csi ./cmd/scaleway-sfs-subdir-csi
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o bin/csi-admin ./cmd/csi-admin

release-binaries:
	GO=$(GO) RELEASE_TAG=$(RELEASE_TAG) VERSION=$(VERSION) COMMIT=$(COMMIT) BUILD_DATE=$(BUILD_DATE) REPOSITORY_URL=$(REPOSITORY_URL) ./hack/build-release-binaries.sh

test-release-binaries:
	GO=$(GO) ./hack/verify-release-binaries.sh

test-release-manifest:
	HELM=$(HELM) ./hack/verify-release-manifest.sh

verify: fmt-check test test-race test-e2e-safety test-install-preflight vet test-linux-cross-compile test-release-binaries test-release-manifest helm-test

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

# Keep this verbose so CI evidence records the pinned conformance version and
# every controller/node sanity case rather than only a package-level PASS.
test-csi-sanity:
	$(GO) list -m -f 'CSI sanity module: {{.Path}} {{.Version}}' github.com/kubernetes-csi/csi-test/v5
	$(GO) test -count=1 -v ./internal/csisanity

# This focused target verifies only the local, non-mutating real-E2E planning
# and cleanup trust boundaries. It does not run or replace the real cloud suite.
test-e2e-safety:
	$(GO) test ./internal/e2eplan ./internal/e2ecleanup ./internal/e2erunner ./internal/releasequalification ./hack/scaleway-e2e-plan ./hack/scaleway-e2e-cleanup ./hack/scaleway-e2e-run ./hack/release-qualification
	sh -n ./hack/run-kapsule-e2e.sh

# Exercises the read-only/server-dry-run installation gate against deterministic
# kubectl and Scaleway CLI boundaries. It never contacts a real cluster.
test-install-preflight:
	JQ=$(JQ) ./hack/verify-install-preflight.sh

# Creates and removes one local disposable kind cluster. The chart explicitly
# switches to a fake binary that is absent from production release images.
test-kind:
	HELM=$(HELM) ./hack/verify-kind.sh

# This target must run on Linux as root (normally in the dedicated CI step).
# The test creates only a private mount namespace and disposable tmpfs/bind
# mounts; it does not contact Kubernetes or Scaleway.
test-linux-privileged:
	@test "$$(uname -s)" = Linux || { echo "test-linux-privileged requires Linux" >&2; exit 2; }
	@test "$$(id -u)" = 0 || { echo "test-linux-privileged requires root/CAP_SYS_ADMIN" >&2; exit 2; }
	SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 $(GO) test -count=1 -run '^TestPrivilegedKernelMountNamespace$$' ./pkg/mount
	SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 $(GO) test -count=1 -run '^TestPrivilegedNodeServiceKernelLifecycle$$' ./pkg/driver
	SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 $(GO) test -count=1 -run '^TestPrivilegedOSLifecycleRejectsNestedMount$$' ./pkg/safety
	SFS_SUBDIR_PRIVILEGED_LINUX_TEST=1 $(GO) test -count=1 -run '^TestPrivilegedOSDurableFSLateNestedMount$$' ./pkg/safety
	$(GO) test -count=1 -run '^TestLinuxOSDurableFSBarrierRestart$$' ./pkg/safety
	$(GO) test -count=1 -run '^TestLinuxOSLifecycleBarrierRestart$$' ./pkg/safety
	$(GO) test -count=1 ./pkg/safety

test-linux-cross-compile:
	@set -eu; tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
		GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/safety-linux-amd64.test" ./pkg/safety; \
		GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/safety-linux-arm64.test" ./pkg/safety; \
		GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/mount-linux-amd64.test" ./pkg/mount; \
		GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/mount-linux-arm64.test" ./pkg/mount; \
		GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/driver-linux-amd64.test" ./pkg/driver; \
		GOOS=linux GOARCH=arm64 CGO_ENABLED=0 $(GO) test -c -o "$$tmp/driver-linux-arm64.test" ./pkg/driver

vet:
	$(GO) vet ./...

fmt-check:
	@test -z "$$($(GO)fmt -l .)" || { $(GO)fmt -l .; exit 1; }

lint:
	$(GOLANGCI_LINT) run

helm-lint:
	$(HELM) lint charts/scaleway-sfs-subdir-csi

helm-test:
	HELM=$(HELM) ./hack/verify-helm.sh

docker-build:
	docker build --file Dockerfile --tag scaleway-sfs-subdir-csi:verify .

clean:
	rm -rf bin coverage dist
