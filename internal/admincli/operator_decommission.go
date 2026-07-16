package admincli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const defaultOperatorDecommissionTimeout = 30 * time.Minute

type operatorDecommissionInvocation struct {
	namespace          string
	release            string
	requestID          string
	parentFilesystemID string
	mode               admin.DecommissionMode
	kubeconfig         string
	context            string
	timeout            time.Duration
}

func parseOperatorDecommission(args []string) (operatorDecommissionInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorDecommissionInvocation{}, usage(err)
	}
	if len(args) < 2 || args[0] != "decommission" || args[1] != "prepare" {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission requires prepare"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "parent-filesystem-id": {}, "mode": {},
		"kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorDecommissionInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission prepare does not accept positional arguments"))
	}
	for _, required := range []string{"namespace", "release", "request-id", "parent-filesystem-id", "mode"} {
		if _, present := values[required]; !present {
			return operatorDecommissionInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission request ID: %w", err))
	}
	if err := volume.ValidateParentFilesystemID(values["parent-filesystem-id"]); err != nil {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission parent: %w", err))
	}
	mode := admin.DecommissionMode(values["mode"])
	if mode != admin.DecommissionDryRun && mode != admin.DecommissionExecute {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission mode %q is unsupported", mode))
	}
	timeout := defaultOperatorDecommissionTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > 2*time.Hour {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("decommission timeout must be between 1 minute and 2 hours"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorDecommissionInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorDecommissionInvocation{
		namespace: values["namespace"], release: values["release"], requestID: values["request-id"],
		parentFilesystemID: values["parent-filesystem-id"], mode: mode,
		kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorDecommission(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	parsed, err := parseOperatorDecommission(args)
	if err != nil {
		return err
	}
	operationCtx, cancel := context.WithTimeout(ctx, parsed.timeout)
	defer cancel()
	backend, err := newKubernetesDecommissionBackend(operationCtx, parsed, buildVersion)
	if err != nil {
		return err
	}
	coordinator, err := admin.NewDecommissionCoordinator(backend)
	if err != nil {
		return err
	}
	request := admin.MutationRequest{
		RequestID: parsed.requestID, AdminVersion: buildVersion,
		Protocol: admin.ProtocolVersion{Major: admin.ProtocolMajorV1, Minor: admin.ProtocolMinorV1},
	}
	result, err := coordinator.Prepare(operationCtx, request, parsed.parentFilesystemID, parsed.mode)
	if err != nil {
		return err
	}
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode parent decommission result: %w", err)
	}
	return writeResult(stdout, encoded)
}
