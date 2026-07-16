package admincli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/canonicaljson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/admin"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/recovery"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/volume"
)

const defaultOperatorCheckpointTimeout = 30 * time.Minute

type operatorCheckpointInvocation struct {
	phase      string
	namespace  string
	release    string
	requestID  string
	outputFile string
	kubeconfig string
	context    string
	timeout    time.Duration
}

type operatorCheckpointPrepareResult struct {
	SchemaVersion string                        `json:"schemaVersion"`
	RequestID     string                        `json:"requestID"`
	OutputFile    string                        `json:"outputFile"`
	Receipt       admin.CheckpointExportReceipt `json:"receipt"`
}

func parseOperatorCheckpoint(args []string) (operatorCheckpointInvocation, error) {
	if err := validateArguments(args); err != nil {
		return operatorCheckpointInvocation{}, usage(err)
	}
	if len(args) < 2 || args[0] != "checkpoint" || (args[1] != "prepare" && args[1] != "resume") {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint requires prepare or resume"))
	}
	values, remaining, err := parseLeadingFlags(args[2:], map[string]struct{}{
		"namespace": {}, "release": {}, "request-id": {}, "output-file": {},
		"kubeconfig": {}, "context": {}, "timeout": {},
	})
	if err != nil {
		return operatorCheckpointInvocation{}, usage(err)
	}
	if len(remaining) != 0 {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint %s does not accept positional arguments", args[1]))
	}
	for _, required := range []string{"namespace", "release", "request-id"} {
		if _, present := values[required]; !present {
			return operatorCheckpointInvocation{}, usage(fmt.Errorf("required admin flag --%s is missing", required))
		}
	}
	if args[1] == "prepare" {
		if _, present := values["output-file"]; !present {
			return operatorCheckpointInvocation{}, usage(fmt.Errorf("required admin flag --output-file is missing"))
		}
		if err := validateCheckpointOutputPath(values["output-file"]); err != nil {
			return operatorCheckpointInvocation{}, usage(err)
		}
	} else if _, present := values["output-file"]; present {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint resume does not accept --output-file"))
	}
	if problems := validation.IsDNS1123Label(values["namespace"]); len(problems) != 0 {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint namespace is invalid: %s", strings.Join(problems, "; ")))
	}
	if problems := validation.IsDNS1123Label(values["release"]); len(problems) != 0 {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("helm release name is invalid: %s", strings.Join(problems, "; ")))
	}
	if err := volume.ValidateOperationID(values["request-id"]); err != nil {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint request ID: %w", err))
	}
	timeout := defaultOperatorCheckpointTimeout
	if value, present := values["timeout"]; present {
		timeout, err = time.ParseDuration(value)
		if err != nil {
			return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint timeout is invalid: %w", err))
		}
	}
	if timeout < time.Minute || timeout > time.Hour {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("checkpoint timeout must be between 1 minute and 1 hour"))
	}
	kubeconfig := values["kubeconfig"]
	if kubeconfig != "" && (!filepath.IsAbs(kubeconfig) || filepath.Clean(kubeconfig) != kubeconfig || kubeconfig == string(filepath.Separator) || strings.ContainsAny(kubeconfig, "\x00\r\n")) {
		return operatorCheckpointInvocation{}, usage(fmt.Errorf("kubeconfig must be a clean absolute non-root path"))
	}
	return operatorCheckpointInvocation{
		phase: args[1], namespace: values["namespace"], release: values["release"],
		requestID: values["request-id"], outputFile: values["output-file"],
		kubeconfig: kubeconfig, context: values["context"], timeout: timeout,
	}, nil
}

func runOperatorCheckpoint(ctx context.Context, args []string, stdout io.Writer, buildVersion string) error {
	if len(args) >= 2 && args[0] == "checkpoint" && args[1] == "restore" {
		return runOperatorCheckpointRestore(ctx, args, stdout, buildVersion)
	}
	parsed, err := parseOperatorCheckpoint(args)
	if err != nil {
		return err
	}
	if parsed.phase == "prepare" {
		// Refuse an existing destination before cluster access. The final hard
		// link also uses no-replace semantics to close the later race.
		if err := requireAbsentCheckpointOutput(parsed.outputFile); err != nil {
			return err
		}
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
	if parsed.phase == "resume" {
		result, err := executor.run(operationCtx, parsed.namespace, pod.Name, []string{
			"local", "--timeout=5m", "checkpoint", "resume", "--request-id=" + parsed.requestID,
		}, nil)
		if err != nil {
			return err
		}
		var resumed admin.CheckpointResumeResult
		if err := strictjson.Decode(result, &resumed); err != nil {
			return fmt.Errorf("decode checkpoint resume result: %w", err)
		}
		if err := resumed.Validate(); err != nil {
			return fmt.Errorf("validate checkpoint resume result: %w", err)
		}
		if resumed.RequestID != parsed.requestID {
			return fmt.Errorf("checkpoint resume result differs from request")
		}
		return writeResult(stdout, []byte(strings.TrimSpace(string(result))))
	}

	preparedBytes, err := executor.run(operationCtx, parsed.namespace, pod.Name, []string{
		"local", "--timeout=5m", "checkpoint", "prepare", "--request-id=" + parsed.requestID,
	}, nil)
	if err != nil {
		return err
	}
	var prepared admin.CheckpointPrepareResult
	if err := strictjson.Decode(preparedBytes, &prepared); err != nil {
		return fmt.Errorf("decode checkpoint prepare result: %w", err)
	}
	if err := prepared.Validate(); err != nil {
		return fmt.Errorf("validate checkpoint prepare result: %w", err)
	}
	if prepared.RequestID != parsed.requestID {
		return fmt.Errorf("checkpoint prepare result differs from request")
	}
	ticketBytes, err := recovery.EncodeCheckpointTicket(prepared.Ticket)
	if err != nil {
		return err
	}
	temporary, err := createCheckpointArchiveTemp(parsed.outputFile)
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keepTemporary := false
	defer func() {
		_ = temporary.Close()
		if !keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	digest := sha256.New()
	if err := executor.stream(operationCtx, parsed.namespace, pod.Name, []string{
		"local", "--timeout=" + parsed.timeout.String(), "checkpoint", "export",
		"--request-id=" + parsed.requestID, "--ticket-stdin=true",
	}, ticketBytes, io.MultiWriter(temporary, digest)); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("fsync checkpoint archive temporary file: %w", err)
	}
	stat, err := temporary.Stat()
	if err != nil {
		return fmt.Errorf("stat checkpoint archive temporary file: %w", err)
	}
	if stat.Size() <= 0 {
		return fmt.Errorf("checkpoint export produced an empty archive")
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close checkpoint archive temporary file: %w", err)
	}
	if err := publishCheckpointArchive(temporaryPath, parsed.outputFile); err != nil {
		return err
	}
	keepTemporary = true // publishCheckpointArchive removed the temporary name.
	receipt := admin.CheckpointExportReceipt{
		RequestID: parsed.requestID, ManifestSHA256: prepared.Ticket.ManifestSHA256,
		ArchiveSHA256: "sha256:" + hex.EncodeToString(digest.Sum(nil)), ArchiveBytes: uint64(stat.Size()),
		ArchiveFormat: admin.CheckpointExportArchiveFormatV1,
	}
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("validate checkpoint export receipt: %w", err)
	}
	result := operatorCheckpointPrepareResult{
		SchemaVersion: volume.SchemaVersionV1, RequestID: parsed.requestID,
		OutputFile: parsed.outputFile, Receipt: receipt,
	}
	encoded, err := canonicaljson.Marshal(result)
	if err != nil {
		return err
	}
	return writeResult(stdout, encoded)
}

func validateCheckpointOutputPath(outputPath string) error {
	if outputPath == "" || !filepath.IsAbs(outputPath) || filepath.Clean(outputPath) != outputPath || outputPath == string(filepath.Separator) || strings.ContainsAny(outputPath, "\x00\r\n") {
		return fmt.Errorf("checkpoint output file must be a clean absolute non-root path")
	}
	if filepath.Base(outputPath) == "." || filepath.Base(outputPath) == string(filepath.Separator) {
		return fmt.Errorf("checkpoint output file name is invalid")
	}
	return nil
}

func requireAbsentCheckpointOutput(outputPath string) error {
	if err := validateCheckpointOutputPath(outputPath); err != nil {
		return err
	}
	if _, err := os.Lstat(outputPath); err == nil {
		return fmt.Errorf("checkpoint output file %q already exists", outputPath)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect checkpoint output file: %w", err)
	}
	parent := filepath.Dir(outputPath)
	info, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("inspect checkpoint output directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("checkpoint output parent must be an existing non-symlink directory")
	}
	return nil
}

func createCheckpointArchiveTemp(outputPath string) (*os.File, error) {
	if err := requireAbsentCheckpointOutput(outputPath); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(filepath.Dir(outputPath), "."+filepath.Base(outputPath)+".tmp-")
	if err != nil {
		return nil, fmt.Errorf("create checkpoint archive temporary file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, fmt.Errorf("set checkpoint archive temporary permissions: %w", err)
	}
	return file, nil
}

func publishCheckpointArchive(temporaryPath, outputPath string) (returnErr error) {
	if err := os.Link(temporaryPath, outputPath); err != nil {
		return fmt.Errorf("publish checkpoint archive without replacement: %w", err)
	}
	parent, err := os.Open(filepath.Dir(outputPath))
	if err != nil {
		return fmt.Errorf("open checkpoint output directory for durability: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, parent.Close()) }()
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("fsync checkpoint output directory after publish: %w", err)
	}
	if err := os.Remove(temporaryPath); err != nil {
		return fmt.Errorf("remove published checkpoint temporary name: %w", err)
	}
	if err := parent.Sync(); err != nil {
		return fmt.Errorf("fsync checkpoint output directory after temporary removal: %w", err)
	}
	return nil
}
