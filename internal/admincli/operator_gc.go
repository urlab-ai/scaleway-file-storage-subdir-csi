package admincli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const defaultOperatorGCTimeout = 30 * time.Minute

type operatorGCInvocation struct {
	namespace       string
	release         string
	requestID       string
	logicalVolumeID string
	mode            string
	expectedState   volume.AllocationState
	kubeconfig      string
	context         string
	timeout         time.Duration
}

func parseOperatorGC(args []string) (operatorGCInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorGCInvocation{}, usage(err)
	}
	if len(args) < 2 || args[0] != "gc" || args[1] != "submit" {
		return operatorGCInvocation{}, usage(fmt.Errorf("gc requires submit"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "logical-volume-id": {},
		"mode": {}, "expected-state": {}, "kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorGCInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorGCInvocation{}, usage(fmt.Errorf("gc submit does not accept positional arguments"))
	}
	for _, required := range []string{"namespace", "release", "request-id", "logical-volume-id", "mode", "expected-state"} {
		if _, present := values[required]; !present {
			return operatorGCInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorGCInvocation{}, usage(fmt.Errorf("GC namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorGCInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorGCInvocation{}, usage(fmt.Errorf("GC request ID: %w", err))
	}
	payload := admin.GCCommandPayload{
		LogicalVolumeID: values["logical-volume-id"], Mode: values["mode"],
		ExpectedState: volume.AllocationState(values["expected-state"]),
	}
	if err := payload.Validate(); err != nil {
		return operatorGCInvocation{}, usage(fmt.Errorf("GC request: %w", err))
	}
	timeout := defaultOperatorGCTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorGCInvocation{}, usage(fmt.Errorf("GC timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > 2*time.Hour {
		return operatorGCInvocation{}, usage(fmt.Errorf("GC timeout must be between 1 minute and 2 hours"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorGCInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorGCInvocation{
		namespace: values["namespace"], release: values["release"], requestID: values["request-id"],
		logicalVolumeID: payload.LogicalVolumeID, mode: payload.Mode, expectedState: payload.ExpectedState,
		kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorGC(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	parsed, err := parseOperatorGC(args)
	if err != nil {
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, parsed.timeout)
	defer cancel()
	client, kubectl, err := newCallerKubernetesClient(parsed.kubeconfig, parsed.context, buildVersion)
	if err != nil {
		return err
	}
	discovery := &kubernetesUninstallBackend{client: client, namespace: parsed.namespace, release: parsed.release}
	pod, err := discovery.singleActivePod(operationCtx, "controller")
	if err != nil {
		return err
	}
	executor := &kubectlPodAdminExecutor{binary: kubectl, kubeconfig: parsed.kubeconfig, context: parsed.context}
	result, err := executor.run(operationCtx, parsed.namespace, pod.Name, []string{
		"local", "--timeout=5m", "gc", "submit", "--request-id=" + parsed.requestID,
		"--logical-volume-id=" + parsed.logicalVolumeID, "--mode=" + parsed.mode,
		"--expected-state=" + string(parsed.expectedState),
	}, nil)
	if err != nil {
		return err
	}
	var audit admin.GCCommandResult
	if err := strictjson.Decode(result, &audit); err != nil {
		return fmt.Errorf("decode operator GC result: %w", err)
	}
	if err := audit.Validate(); err != nil {
		return fmt.Errorf("validate operator GC result: %w", err)
	}
	if audit.RequestID != parsed.requestID || audit.LogicalVolumeID != parsed.logicalVolumeID || audit.Mode != parsed.mode || audit.PreviousState != parsed.expectedState {
		return fmt.Errorf("operator GC result differs from submitted request")
	}
	return writeResult(stdout, []byte(strings.TrimSpace(string(result))))
}
