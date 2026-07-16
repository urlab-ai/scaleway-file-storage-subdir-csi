package admincli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const testRequestID = "11111111-1111-4111-8111-111111111111"

type fakeWireClient struct {
	handshake      admin.HandshakeResponse
	handshakeErr   error
	handshakeCalls int
	result         json.RawMessage
	executeErr     error
	executeCalls   int
	executed       admin.Command
	requestID      string
	payload        json.RawMessage
}

func (client *fakeWireClient) Handshake(context.Context) (admin.HandshakeResponse, error) {
	client.handshakeCalls++
	return client.handshake, client.handshakeErr
}

func (client *fakeWireClient) Execute(_ context.Context, command admin.Command, requestID string, payload json.RawMessage) (json.RawMessage, error) {
	client.executeCalls++
	client.executed = command
	client.requestID = requestID
	client.payload = append(json.RawMessage(nil), payload...)
	return append(json.RawMessage(nil), client.result...), client.executeErr
}

type partialWriter struct {
	maximum int
	buffer  bytes.Buffer
}

func (writer *partialWriter) Write(value []byte) (int, error) {
	if len(value) > writer.maximum {
		value = value[:writer.maximum]
	}
	return writer.buffer.Write(value)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestParseInvocationCoversClosedLocalCommandSet(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		command   admin.Command
		requestID string
	}{
		{name: "handshake", args: []string{"local", "handshake"}, command: admin.CommandHandshake},
		{name: "checkpoint prepare", args: []string{"local", "checkpoint", "prepare", "--request-id=" + testRequestID}, command: admin.CommandCheckpointPrepare, requestID: testRequestID},
		{name: "checkpoint resume", args: []string{"local", "checkpoint", "resume", "--request-id=" + testRequestID}, command: admin.CommandCheckpointResume, requestID: testRequestID},
		{name: "decommission inspect", args: []string{"local", "decommission", "inspect", "--request-id=" + testRequestID, "--parent-filesystem-id=22222222-2222-4222-8222-222222222222"}, command: admin.CommandDecommissionInspect, requestID: testRequestID},
		{name: "decommission prepare", args: []string{"local", "decommission", "prepare", "--request-id=" + testRequestID, "--parent-filesystem-id=22222222-2222-4222-8222-222222222222"}, command: admin.CommandDecommissionPrepare, requestID: testRequestID},
		{name: "decommission quiesce", args: []string{"local", "decommission", "quiesce", "--request-id=" + testRequestID, "--parent-filesystem-id=22222222-2222-4222-8222-222222222222"}, command: admin.CommandDecommissionQuiesce, requestID: testRequestID},
		{name: "decommission cleanup", args: []string{"local", "decommission", "cleanup", "--request-id=" + testRequestID, "--parent-filesystem-id=22222222-2222-4222-8222-222222222222"}, command: admin.CommandDecommissionCleanup, requestID: testRequestID},
		{name: "decommission release", args: []string{"local", "decommission", "release", "--request-id=" + testRequestID, "--parent-filesystem-id=22222222-2222-4222-8222-222222222222"}, command: admin.CommandDecommissionRelease, requestID: testRequestID},
		{name: "GC", args: []string{"local", "gc", "submit", "--request-id=" + testRequestID, "--logical-volume-id=lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--mode=dry-run", "--expected-state=Archived"}, command: admin.CommandGCSubmit, requestID: testRequestID},
		{name: "upgrade", args: []string{"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-file=/tmp/candidate.json"}, command: admin.CommandUpgradePreflight, requestID: testRequestID},
		{name: "upgrade stdin", args: []string{"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-stdin=true"}, command: admin.CommandUpgradePreflight, requestID: testRequestID},
		{name: "uninstall inspect", args: []string{"local", "uninstall", "inspect", "--request-id=" + testRequestID}, command: admin.CommandUninstallInspect, requestID: testRequestID},
		{name: "uninstall prepare", args: []string{"local", "uninstall", "prepare", "--request-id=" + testRequestID}, command: admin.CommandUninstallPrepare, requestID: testRequestID},
		{name: "uninstall quiesce", args: []string{"local", "uninstall", "quiesce", "--request-id=" + testRequestID}, command: admin.CommandUninstallQuiesce, requestID: testRequestID},
		{name: "uninstall cleanup", args: []string{"local", "uninstall", "cleanup", "--request-id=" + testRequestID}, command: admin.CommandUninstallCleanup, requestID: testRequestID},
		{name: "uninstall release", args: []string{"local", "uninstall", "release", "--request-id=" + testRequestID}, command: admin.CommandUninstallRelease, requestID: testRequestID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := parseInvocation(test.args)
			if err != nil {
				t.Fatalf("parseInvocation() error = %v", err)
			}
			if parsed.command != test.command || parsed.requestID != test.requestID {
				t.Fatalf("parseInvocation() = %#v", parsed)
			}
			if parsed.endpoint != "unix:///run/scaleway-sfs-subdir-csi/admin.sock" || parsed.timeout != defaultLocalTimeout {
				t.Fatalf("parseInvocation() endpoint/timeout = %q/%s", parsed.endpoint, parsed.timeout)
			}
		})
	}
}

func TestParseInvocationValidatesBeforeTransport(t *testing.T) {
	invalid := [][]string{
		nil,
		{"checkpoint", "prepare"},
		{"local"},
		{"local", "unknown"},
		{"local", "handshake", "extra"},
		{"local", "checkpoint", "prepare"},
		{"local", "checkpoint", "other", "--request-id=" + testRequestID},
		{"local", "decommission", "inspect", "--request-id=" + testRequestID},
		{"local", "decommission", "prepare", "--request-id=" + testRequestID, "--parent-filesystem-id=bad/id"},
		{"local", "gc", "submit", "--request-id=" + testRequestID, "--logical-volume-id=bad", "--mode=dry-run", "--expected-state=Archived"},
		{"local", "gc", "submit", "--request-id=" + testRequestID, "--logical-volume-id=lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--mode=execute", "--expected-state=Deleted"},
		{"local", "upgrade", "preflight", "--request-id=" + testRequestID},
		{"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-stdin=false"},
		{"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-stdin=true", "--candidate-file=/tmp/candidate.json"},
		{"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-file=relative.json"},
		{"local", "--unknown=true", "handshake"},
		{"local", "-timeout=30s", "handshake"},
		{"local", "--timeout=30s", "--timeout=45s", "handshake"},
		{"local", "--timeout=999ms", "handshake"},
		{"local", "--timeout=6m", "handshake"},
		{"local", "checkpoint", "prepare", "--request-id=" + testRequestID, "--request-id=" + testRequestID},
		{"local", "checkpoint", "prepare", "-request-id=" + testRequestID},
		{"local", "uninstall", "prepare"},
		{"local", "uninstall", "unknown", "--request-id=" + testRequestID},
	}
	for _, args := range invalid {
		if _, err := parseInvocation(args); err == nil || ExitCode(err) != 2 {
			t.Errorf("parseInvocation(%q) error/exit = %v/%d", args, err, ExitCode(err))
		}
	}
}

func TestLocalSocketPathRejectsNonUnixAndAmbiguousEndpoints(t *testing.T) {
	for _, endpoint := range []string{
		"tcp://127.0.0.1:9000", "unix://relative", "unix:///", "unix:///tmp/../admin.sock",
		"unix:///tmp/admin.sock?query", "unix:///tmp/admin.sock#fragment", "unix:///tmp/admin.sock\nother",
	} {
		if _, err := localSocketPath(endpoint); err == nil {
			t.Errorf("localSocketPath(%q) error = nil", endpoint)
		}
	}
	if value, err := localSocketPath("unix:///tmp/admin.sock"); err != nil || value != "/tmp/admin.sock" {
		t.Fatalf("localSocketPath(valid) = %q, %v", value, err)
	}
}

func TestRunHandshakeUsesFixedProtocolAndCanonicalOutput(t *testing.T) {
	client := &fakeWireClient{handshake: admin.HandshakeResponse{
		DriverVersion: "1.2.3", ProtocolMajor: 1, MinimumMinor: 0, MaximumMinor: 0,
	}}
	var gotPath, gotVersion string
	var gotProtocol admin.ProtocolVersion
	var gotTimeout time.Duration
	factory := func(path, version string, protocol admin.ProtocolVersion, timeout time.Duration) (wireClient, error) {
		gotPath, gotVersion, gotProtocol, gotTimeout = path, version, protocol, timeout
		return client, nil
	}
	var output bytes.Buffer
	err := run(context.Background(), []string{
		"local", "--endpoint=unix:///tmp/driver-admin.sock", "--timeout=45s", "handshake",
	}, &output, "1.2.0", factory)
	if err != nil {
		t.Fatalf("run(handshake) error = %v", err)
	}
	if gotPath != "/tmp/driver-admin.sock" || gotVersion != "1.2.0" || gotProtocol != (admin.ProtocolVersion{Major: 1, Minor: 0}) || gotTimeout != 45*time.Second {
		t.Fatalf("factory inputs = %q/%q/%#v/%s", gotPath, gotVersion, gotProtocol, gotTimeout)
	}
	want := "{\"driverVersion\":\"1.2.3\",\"protocolMajor\":1,\"minimumMinor\":0,\"maximumMinor\":0}\n"
	if output.String() != want {
		t.Fatalf("handshake output = %q, want %q", output.String(), want)
	}
}

func TestRunGCEmitsExactResultAndTypedPayload(t *testing.T) {
	client := &fakeWireClient{result: json.RawMessage(`{"completed":false}`)}
	var output bytes.Buffer
	err := run(context.Background(), []string{
		"local", "gc", "submit", "--request-id=" + testRequestID,
		"--logical-volume-id=lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "--mode=dry-run", "--expected-state=Archived",
	}, &output, "1.0.0", func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatalf("run(GC) error = %v", err)
	}
	if output.String() != "{\"completed\":false}\n" || client.executeCalls != 1 || client.executed != admin.CommandGCSubmit || client.requestID != testRequestID {
		t.Fatalf("GC output/call = %q/%d/%q/%q", output.String(), client.executeCalls, client.executed, client.requestID)
	}
	var payload admin.GCCommandPayload
	if err := json.Unmarshal(client.payload, &payload); err != nil {
		t.Fatalf("decode GC payload: %v", err)
	}
	if payload.LogicalVolumeID != "lv-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || payload.Mode != "dry-run" || payload.ExpectedState != volume.StateArchived {
		t.Fatalf("GC payload = %#v", payload)
	}
}

func TestRunUpgradeValidatesCandidateBeforeExecute(t *testing.T) {
	candidate := validUpgradeCandidate()
	encoded, err := canonicaljson.Marshal(candidate)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	filename := filepath.Join(t.TempDir(), "candidate.json")
	if err := os.WriteFile(filename, encoded, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	client := &fakeWireClient{result: json.RawMessage(`{"accepted":true}`)}
	var output bytes.Buffer
	err = run(context.Background(), []string{
		"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-file=" + filename,
	}, &output, "1.0.0", func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatalf("run(upgrade) error = %v", err)
	}
	if client.executed != admin.CommandUpgradePreflight || output.String() != "{\"accepted\":true}\n" {
		t.Fatalf("upgrade call/output = %q/%q", client.executed, output.String())
	}
	var payload admin.UpgradePreflightPayload
	if err := json.Unmarshal(client.payload, &payload); err != nil || payload.Candidate.DriverName != candidate.DriverName {
		t.Fatalf("upgrade payload/error = %#v/%v", payload, err)
	}

	unknown := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"unknown":true}`)...)
	if err := os.WriteFile(filename, unknown, 0o600); err != nil {
		t.Fatalf("WriteFile(unknown) error = %v", err)
	}
	client.executeCalls = 0
	err = run(context.Background(), []string{
		"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-file=" + filename,
	}, io.Discard, "1.0.0", func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		return client, nil
	})
	if err == nil || client.executeCalls != 0 {
		t.Fatalf("run(invalid upgrade) error/calls = %v/%d", err, client.executeCalls)
	}
}

func TestRunUpgradeAcceptsOnlyValidatedBoundedOrchestratorStdin(t *testing.T) {
	candidate := validUpgradeCandidate()
	payload, err := canonicaljson.Marshal(admin.UpgradePreflightPayload{Candidate: candidate})
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	client := &fakeWireClient{result: json.RawMessage(`{"accepted":true}`)}
	var output bytes.Buffer
	err = runWithInput(context.Background(), []string{
		"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-stdin=true",
	}, bytes.NewReader(payload), &output, "1.0.0", func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		return client, nil
	})
	if err != nil {
		t.Fatalf("runWithInput(upgrade) error = %v", err)
	}
	if client.executed != admin.CommandUpgradePreflight || !bytes.Equal(client.payload, payload) || output.String() != "{\"accepted\":true}\n" {
		t.Fatalf("stdin upgrade call/payload/output = %q/%s/%q", client.executed, client.payload, output.String())
	}
	if err := runWithInput(context.Background(), []string{
		"local", "upgrade", "preflight", "--request-id=" + testRequestID, "--candidate-stdin=true",
	}, strings.NewReader(`{"candidate":{"unknown":true}}`), io.Discard, "1.0.0", func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		return client, nil
	}); err == nil {
		t.Fatal("runWithInput(invalid streamed candidate) error = nil")
	}
}

func TestReadUpgradePayloadRejectsSymlinkAndNonRegularInput(t *testing.T) {
	candidate := validUpgradeCandidate()
	encoded, err := canonicaljson.Marshal(candidate)
	if err != nil {
		t.Fatalf("canonicaljson.Marshal() error = %v", err)
	}
	directory := t.TempDir()
	target := filepath.Join(directory, "candidate.json")
	if err := os.WriteFile(target, encoded, 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	symlink := filepath.Join(directory, "candidate-link.json")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatalf("os.Symlink() error = %v", err)
	}
	if _, err := readUpgradePayload(context.Background(), symlink); err == nil {
		t.Fatal("readUpgradePayload(symlink) error = nil")
	}
	if _, err := readUpgradePayload(context.Background(), directory); err == nil {
		t.Fatal("readUpgradePayload(directory) error = nil")
	}
}

func TestParseInvocationAcceptsSeparateClosedFlagValues(t *testing.T) {
	parsed, err := parseInvocation([]string{
		"local", "--timeout", "45s", "checkpoint", "prepare", "--request-id", testRequestID,
	})
	if err != nil {
		t.Fatalf("parseInvocation() error = %v", err)
	}
	if parsed.timeout != 45*time.Second || parsed.requestID != testRequestID || parsed.command != admin.CommandCheckpointPrepare {
		t.Fatalf("parseInvocation() = %#v", parsed)
	}
}

func TestRunRejectsInvalidBoundaryInputsWithoutFactoryOrOutput(t *testing.T) {
	calls := 0
	factory := func(string, string, admin.ProtocolVersion, time.Duration) (wireClient, error) {
		calls++
		return &fakeWireClient{}, nil
	}
	for _, args := range [][]string{
		{"local", "--endpoint=tcp://127.0.0.1:9000", "handshake"},
		{"local", "--endpoint=unix:///tmp/../admin.sock", "handshake"},
	} {
		var output bytes.Buffer
		err := run(context.Background(), args, &output, "1.0.0", factory)
		if err == nil || ExitCode(err) != 2 || output.Len() != 0 {
			t.Fatalf("run(%q) error/exit/output = %v/%d/%q", args, err, ExitCode(err), output.String())
		}
	}
	if calls != 0 {
		t.Fatalf("invalid endpoints constructed %d clients", calls)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(canceled, []string{"local", "handshake"}, io.Discard, "1.0.0", factory); !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("run(canceled) error/calls = %v/%d", err, calls)
	}
	//nolint:staticcheck // This case deliberately verifies the public nil-context guard.
	if err := run(nil, []string{"local", "handshake"}, io.Discard, "1.0.0", factory); err == nil {
		t.Fatal("run(nil context) error = nil")
	}
	if err := run(context.Background(), []string{"local", "handshake"}, nil, "1.0.0", factory); err == nil {
		t.Fatal("run(nil output) error = nil")
	}
}

func TestWriteResultHandlesShortAndInvalidWriters(t *testing.T) {
	partial := &partialWriter{maximum: 2}
	if err := writeResult(partial, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("writeResult(short writes) error = %v", err)
	}
	if partial.buffer.String() != "{\"ok\":true}\n" {
		t.Fatalf("short writer output = %q", partial.buffer.String())
	}
	if err := writeResult(zeroWriter{}, []byte(`{}`)); err == nil {
		t.Fatal("writeResult(zero writer) error = nil")
	}
	if err := writeResult(io.Discard, nil); err == nil {
		t.Fatal("writeResult(empty) error = nil")
	}
}

func TestExitCodeSeparatesUsageAndRuntimeErrors(t *testing.T) {
	if ExitCode(nil) != 0 || ExitCode(usage(errors.New("bad args"))) != 2 || ExitCode(errors.New("remote failure")) != 1 {
		t.Fatalf("ExitCode values = %d/%d/%d", ExitCode(nil), ExitCode(usage(errors.New("bad args"))), ExitCode(errors.New("remote failure")))
	}
}

func TestUsageDocumentsClosedCommandsAndPrivateUninstallPhases(t *testing.T) {
	usage := Usage()
	for _, required := range []string{"csi-admin version", "checkpoint prepare", "gc submit", "upgrade preflight", "uninstall inspect", "private primitives"} {
		if !strings.Contains(usage, required) {
			t.Errorf("Usage() does not contain %q", required)
		}
	}
}

func validUpgradeCandidate() admin.UpgradeCandidate {
	return admin.UpgradeCandidate{
		DriverName:                    "file-storage-subdir.csi.urlab.ai",
		InstallationIDHash:            "sha256:" + strings.Repeat("a", 64),
		ActiveClusterUID:              "22222222-2222-4222-8222-222222222222",
		LeadershipLeaseName:           volume.LeadershipLeaseNameV1,
		ReadableAllocationSchemas:     []string{"1"},
		ReadableOwnershipSchemas:      []string{"1"},
		WrittenAllocationSchema:       "1",
		WrittenOwnershipSchema:        "1",
		CandidateNodeConfigGeneration: strings.Repeat("c", 64),
		Parents: []admin.UpgradeParentMapping{{
			ParentFilesystemID: "33333333-3333-4333-8333-333333333333",
			PoolName:           "standard",
			BasePathHash:       "bp-" + strings.Repeat("b", 32),
		}},
	}
}
