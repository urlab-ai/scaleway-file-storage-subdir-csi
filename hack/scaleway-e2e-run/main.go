// Command scaleway-e2e-run executes the retained real-Kapsule qualification
// plan. Dry-run is the default and never constructs a credentialed client.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2ecleanup"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2eplan"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/e2erunner"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/strictjson"
)

const maximumRequestBytes = 1 << 20

func main() {
	if len(os.Args) > 1 && os.Args[1] == "validate-uninstall-result" {
		if err := runValidateUninstallResult(os.Args[2:]); err != nil {
			fail(err)
		}
		return
	}
	var input, confirmedRunID string
	var execute, dryRun, cleanupOnly bool
	flags := flag.NewFlagSet("scaleway-e2e-run", flag.ContinueOnError)
	flags.StringVar(&input, "input", "", "absolute closed execution request JSON")
	flags.BoolVar(&dryRun, "dry-run", false, "render the non-authorizing plan (default)")
	flags.BoolVar(&execute, "execute", false, "create and mutate the exact approved run resources")
	flags.BoolVar(&cleanupOnly, "cleanup-only", false, "resume exact-ID cleanup for the approved run")
	flags.StringVar(&confirmedRunID, "confirm-run-id", "", "complete approved run ID required with --execute or --cleanup-only")
	if err := flags.Parse(os.Args[1:]); err != nil || flags.NArg() != 0 || input == "" || (dryRun && (execute || cleanupOnly)) || (execute && cleanupOnly) {
		fmt.Fprintln(os.Stderr, "usage: scaleway-e2e-run --input=/absolute/request.json [--dry-run | --execute --confirm-run-id=<uuid> | --cleanup-only --confirm-run-id=<uuid>]")
		os.Exit(2)
	}
	request, err := readRequest(input)
	if err != nil {
		fail(err)
	}
	plan, err := e2eplan.Build(request.Plan)
	if err != nil {
		fail(err)
	}
	encodedPlan, err := e2eplan.Encode(plan)
	if err != nil {
		fail(err)
	}
	fmt.Println(string(encodedPlan))
	if !execute && !cleanupOnly {
		fmt.Fprintln(os.Stderr, "dry-run only: no credentials loaded and no Scaleway or Kubernetes API called")
		return
	}
	if confirmedRunID != plan.RunID {
		fail(fmt.Errorf("--confirm-run-id must equal the complete planned run ID"))
	}
	if execute {
		// Fail before constructing the credentialed backend while the checked-in
		// scenario matrix still contains smoke-only probes.
		if err := e2erunner.RequireReleaseQualificationReady(); err != nil {
			fail(err)
		}
	}
	fmt.Fprintf(os.Stderr, "approved execution: project=%s region=%s run=%s estimated-hourly-cost-eur=%s\n", plan.ProjectID, plan.Region, plan.RunID, plan.EstimatedHourlyCostEUR)
	backend, err := newScalewayBackend(request, plan)
	if err != nil {
		fail(err)
	}
	deadline, _ := time.ParseDuration(request.ScenarioDeadline)
	ctx, cancel := context.WithTimeout(context.Background(), deadline+45*time.Minute)
	defer cancel()
	if cleanupOnly {
		inventory, readErr := backend.readInventory()
		if readErr != nil {
			if os.IsNotExist(readErr) {
				fail(fmt.Errorf("refuse cleanup without the fsynced run inventory at %q", plan.CleanupInventoryPath))
			}
			fail(readErr)
		}
		final, cleanupErr := backend.Cleanup(ctx, request, inventory)
		if cleanupErr != nil {
			fail(cleanupErr)
		}
		if final.Phase != e2ecleanup.PhaseComplete {
			fail(fmt.Errorf("cleanup ended without a complete exact-ID inventory"))
		}
		fmt.Println(plan.CleanupInventoryPath)
		return
	}
	if _, err := os.Lstat(plan.CleanupInventoryPath); err == nil {
		fail(fmt.Errorf("a retained inventory already exists; run --cleanup-only before starting another execution"))
	} else if !os.IsNotExist(err) {
		fail(err)
	}
	evidence, err := e2erunner.Execute(ctx, request, true, confirmedRunID, backend, time.Now)
	if err != nil {
		fail(err)
	}
	encoded, err := e2erunner.EncodeEvidence(evidence)
	if err != nil {
		fail(err)
	}
	output := filepath.Join(request.Plan.EvidenceDirectory, "kapsule-evidence-"+request.Plan.RunID+".json")
	if err := writeExactFile(output, append(encoded, '\n'), 0o600); err != nil {
		fail(err)
	}
	fmt.Println(output)
}

func readRequest(path string) (e2erunner.Request, error) {
	if path == "" || path == string(filepath.Separator) || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return e2erunner.Request{}, fmt.Errorf("runner input must be a clean absolute non-root path")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximumRequestBytes {
		return e2erunner.Request{}, fmt.Errorf("runner input must be an exact regular file of 1 to %d bytes", maximumRequestBytes)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return e2erunner.Request{}, err
	}
	var request e2erunner.Request
	if err := strictjson.Decode(encoded, &request); err != nil {
		return e2erunner.Request{}, err
	}
	return request, request.Validate()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "scaleway-e2e-run:", err)
	os.Exit(1)
}
