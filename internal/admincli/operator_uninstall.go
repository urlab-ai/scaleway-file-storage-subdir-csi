package admincli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"scaleway-sfs-subdir-csi/internal/canonicaljson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

const defaultOperatorUninstallTimeout = 30 * time.Minute

type operatorUninstallInvocation struct {
	namespace  string
	release    string
	requestID  string
	mode       admin.UninstallMode
	kubeconfig string
	context    string
	timeout    time.Duration
}

func parseOperatorUninstall(args []string) (operatorUninstallInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorUninstallInvocation{}, usage(err)
	}
	if len(args) == 0 || args[0] != "uninstall" {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("expected uninstall operator command"))
	}
	if len(args) < 2 || args[1] != "prepare" {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall requires prepare"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "mode": {},
		"kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorUninstallInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall prepare does not accept positional arguments"))
	}
	for _, required := range []string{"namespace", "release", "request-id", "mode"} {
		if _, present := values[required]; !present {
			return operatorUninstallInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall request ID: %w", err))
	}
	mode := admin.UninstallMode(values["mode"])
	if mode != admin.UninstallDryRun && mode != admin.UninstallExecute {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall mode %q is unsupported", mode))
	}
	timeout := defaultOperatorUninstallTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > 2*time.Hour {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("uninstall timeout must be between 1 minute and 2 hours"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorUninstallInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorUninstallInvocation{
		namespace: values["namespace"], release: values["release"], requestID: values["request-id"],
		mode: mode, kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorUninstall(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	parsed, err := parseOperatorUninstall(args)
	if err != nil {
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, parsed.timeout)
	defer cancel()
	backend, err := newKubernetesUninstallBackend(operationCtx, parsed, buildVersion)
	if err != nil {
		return err
	}
	coordinator, err := admin.NewUninstallCoordinator(backend)
	if err != nil {
		return err
	}
	request := admin.MutationRequest{
		RequestID: parsed.requestID, AdminVersion: buildVersion,
		Protocol: admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
	}
	result, err := coordinator.Prepare(operationCtx, request, parsed.mode)
	if err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode safe-uninstall result: %w", err)
	}
	return writeResult(stdout, encoded)
}
