package admincli

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const defaultOperatorUpgradeTimeout = 10 * time.Minute

type operatorUpgradeInvocation struct {
	namespace     string
	release       string
	requestID     string
	candidateFile string
	kubeconfig    string
	context       string
	timeout       time.Duration
}

func parseOperatorUpgrade(args []string) (operatorUpgradeInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorUpgradeInvocation{}, usage(err)
	}
	if len(args) < 2 || args[0] != "upgrade" || args[1] != "preflight" {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade requires preflight"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "candidate-file": {},
		"kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorUpgradeInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade preflight does not accept positional arguments"))
	}
	for _, required := range []string{"namespace", "release", "request-id", "candidate-file"} {
		if _, present := values[required]; !present {
			return operatorUpgradeInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade request ID: %w", err))
	}
	if err := validateCandidateFilePath(values["candidate-file"]); err != nil {
		return operatorUpgradeInvocation{}, usage(err)
	}
	timeout := defaultOperatorUpgradeTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > 30*time.Minute {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("upgrade timeout must be between 1 and 30 minutes"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorUpgradeInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorUpgradeInvocation{
		namespace: values["namespace"], release: values["release"], requestID: values["request-id"],
		candidateFile: values["candidate-file"], kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorUpgrade(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	parsed, err := parseOperatorUpgrade(args)
	if err != nil {
		return err
	}
	// Validate and freeze the exact local inode before any cluster access. The
	// canonical payload, rather than an operator path, crosses kubectl stdin.
	payload, err := readUpgradePayload(ctx, parsed.candidateFile)
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
		"local", "--timeout=5m", "upgrade", "preflight", "--request-id=" + parsed.requestID,
		"--candidate-stdin=true",
	}, payload)
	if err != nil {
		return err
	}
	var audit admin.UpgradePreflightResult
	if err := strictjson.Decode(result, &audit); err != nil {
		return fmt.Errorf("decode operator upgrade preflight result: %w", err)
	}
	if err := audit.Validate(); err != nil {
		return fmt.Errorf("validate operator upgrade preflight result: %w", err)
	}
	var sent admin.UpgradePreflightPayload
	if err := strictjson.Decode(payload, &sent); err != nil {
		return err
	}
	if audit.RequestID != parsed.requestID || audit.CandidateNodeConfigGeneration != sent.Candidate.CandidateNodeConfigGeneration {
		return fmt.Errorf("operator upgrade result differs from request or candidate generation")
	}
	return writeResult(stdout, []byte(strings.TrimSpace(string(result))))
}
