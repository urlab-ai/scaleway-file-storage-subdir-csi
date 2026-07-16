package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"scaleway-sfs-subdir-csi/internal/strictjson"
	"scaleway-sfs-subdir-csi/pkg/admin"
	"scaleway-sfs-subdir-csi/pkg/volume"
)

func runValidateUninstallResult(arguments []string) error {
	flags := flag.NewFlagSet("validate-uninstall-result", flag.ContinueOnError)
	var path, requestID, parentA, parentB string
	flags.StringVar(&path, "file", "", "completed uninstall result file")
	flags.StringVar(&requestID, "request-id", "", "exact run-scoped uninstall request ID")
	flags.StringVar(&parentA, "parent-a", "", "first exact parent ID")
	flags.StringVar(&parentB, "parent-b", "", "second exact parent ID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 || path == "" || requestID == "" || parentA == "" || parentB == "" {
		return fmt.Errorf("uninstall result file, request ID, and two parent IDs are required")
	}
	if path == string(filepath.Separator) || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsAny(path, "\x00\r\n") {
		return fmt.Errorf("uninstall result path must be a clean absolute non-root path")
	}
	if err := volume.ValidateOperationID(requestID); err != nil {
		return fmt.Errorf("uninstall request ID: %w", err)
	}
	expectedParents := []string{parentA, parentB}
	for _, parentID := range expectedParents {
		if err := volume.ValidateInstallationID(parentID); err != nil {
			return fmt.Errorf("uninstall parent ID: %w", err)
		}
	}
	slices.Sort(expectedParents)
	if expectedParents[0] == expectedParents[1] {
		return fmt.Errorf("uninstall parent IDs must be distinct")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumRequestBytes {
		return fmt.Errorf("uninstall result must be an exact regular file of 1 to %d bytes: %w", maximumRequestBytes, err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return validateUninstallResult(encoded, requestID, expectedParents)
}

func validateUninstallResult(encoded []byte, requestID string, expectedParents []string) error {
	var result admin.UninstallPrepareResult
	if err := strictjson.Decode(encoded, &result); err != nil {
		return fmt.Errorf("decode completed safe-uninstall result: %w", err)
	}
	if result.RequestID != requestID || result.Mode != admin.UninstallExecute || !result.Ready || !result.Completed || len(result.Blockers) != 0 || result.Audit == nil {
		return fmt.Errorf("safe-uninstall result is incomplete or belongs to another request")
	}
	if result.Plan.LeaseName != volume.LeadershipLeaseNameV1 || !slices.Equal(result.Plan.ParentFilesystemIDs, expectedParents) {
		return fmt.Errorf("safe-uninstall plan differs from the exact run parents or Lease")
	}
	if err := result.Audit.Validate(); err != nil {
		return fmt.Errorf("validate safe-uninstall audit: %w", err)
	}
	planNodeIDs := make([]string, 0, len(result.Plan.NodeTargets))
	for _, target := range result.Plan.NodeTargets {
		planNodeIDs = append(planNodeIDs, target.NodeID)
	}
	slices.Sort(planNodeIDs)
	if result.Audit.RequestID != requestID || !slices.Equal(result.Audit.ParentFilesystemIDs, expectedParents) ||
		!slices.Equal(result.Audit.CheckedNodeIDs, planNodeIDs) ||
		result.Audit.ChartVersion != result.Plan.ChartVersion || result.Audit.DriverVersion != result.Plan.DriverVersion ||
		result.Audit.AdminVersion != result.Plan.AdminVersion || result.Audit.LeaseName != result.Plan.LeaseName ||
		result.Audit.NodeParentMountRoot != result.Plan.NodeParentMountRoot || result.Audit.ControllerParentMountRoot != result.Plan.ControllerParentMountRoot {
		return fmt.Errorf("safe-uninstall audit differs from the exact request plan")
	}
	return nil
}
