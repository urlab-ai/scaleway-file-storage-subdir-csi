package e2eplan

import (
	"slices"
	"strings"
	"testing"
	"time"
)

const testRunID = "11111111-1111-4111-8111-111111111111"

func validRequest() Request {
	return Request{
		SchemaVersion:          SchemaVersionV1,
		Profile:                ProfileBase,
		RunID:                  testRunID,
		ProjectID:              "22222222-2222-4222-8222-222222222222",
		Region:                 "fr-par",
		ResourcePrefix:         "sfs-e2e-" + testRunID,
		EvidenceDirectory:      "/tmp/sfs-e2e-evidence",
		Cluster:                ClusterRequest{Disposition: ClusterCreate},
		NodePool:               NodePoolRequest{Count: 2, CommercialType: "TEST-TYPE-1"},
		Parents:                ParentRequest{Count: 2, SizeBytes: 25_000_000_000},
		EstimatedHourlyCostEUR: "1.250000",
		CostSource:             "operator-reviewed Scaleway pricing snapshot 2026-07-13",
		ProviderReview: ProviderReview{
			ObservedAt: "2026-07-13T12:00:00Z", ProductStatus: "ga",
			ProductStatusSource: "operator-reviewed Scaleway File Storage product page",
			PublicBetaAccepted:  false, FileStorageQuotaRemaining: 2,
			QuotaSource: "operator-reviewed dedicated test Project quota",
		},
		Artifacts: Artifacts{
			GitCommit:       strings.Repeat("a", 40),
			CandidateDigest: "sha256:" + strings.Repeat("c", 64),
			ChartDigest:     "sha256:" + strings.Repeat("b", 64),
			Images: []ImageDigest{
				{Name: "livenessprobe", Reference: "registry.example/liveness@sha256:" + strings.Repeat("1", 64)},
				{Name: "driver", Reference: "registry.example/driver@sha256:" + strings.Repeat("2", 64)},
				{Name: "external-provisioner", Reference: "registry.example/provisioner@sha256:" + strings.Repeat("3", 64)},
				{Name: "csi-node-driver-registrar", Reference: "registry.example/registrar@sha256:" + strings.Repeat("4", 64)},
				{Name: "external-attacher", Reference: "registry.example/attacher@sha256:" + strings.Repeat("5", 64)},
			},
		},
	}
}

func TestBuildProducesNonAuthorizingRunOwnedPlan(t *testing.T) {
	plan, err := Build(validRequest())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if !plan.DryRun || plan.MutationAuthorized || !plan.RequiresImmediateApproval {
		t.Fatalf("plan authority = dryRun %t, authorized %t, approval %t", plan.DryRun, plan.MutationAuthorized, plan.RequiresImmediateApproval)
	}
	if !plan.Cluster.CreatedByRun || !plan.Cluster.DeleteOnCleanup || plan.OwnershipTag != "sfs-subdir-e2e-run="+testRunID {
		t.Fatalf("plan ownership = %#v / %q", plan.Cluster, plan.OwnershipTag)
	}
	if !plan.NodePool.CreatedByRun || !plan.NodePool.DeleteOnCleanup {
		t.Fatalf("node pool ownership = %#v", plan.NodePool)
	}
	if plan.DisposableInstance != nil {
		t.Fatalf("base profile disposable instance = %#v", plan.DisposableInstance)
	}
	if len(plan.PlannedResources) != 4 || plan.PlannedResources[3].Count != 2 || !plan.PlannedResources[3].DeleteOnCleanup {
		t.Fatalf("planned resources = %#v", plan.PlannedResources)
	}
	if !slices.EqualFunc(plan.Artifacts.Images, []ImageDigest{
		{Name: "csi-node-driver-registrar"}, {Name: "driver"}, {Name: "external-attacher"},
		{Name: "external-provisioner"}, {Name: "livenessprobe"},
	}, func(left, right ImageDigest) bool { return left.Name == right.Name }) {
		t.Fatalf("sorted images = %#v", plan.Artifacts.Images)
	}
	encoded, err := Encode(plan)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"mutationAuthorized":false`) || !strings.Contains(string(encoded), testRunID) {
		t.Fatalf("encoded plan = %s", encoded)
	}
}

func TestBuildNeverMarksReusedClusterForDeletion(t *testing.T) {
	request := validRequest()
	request.Profile = ProfileReleaseCandidate
	request.Parents.SizeBytes = 100_000_000_000
	request.Cluster = ClusterRequest{Disposition: ClusterReuse, ExistingID: "33333333-3333-4333-8333-333333333333"}
	plan, err := Build(request)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if plan.Cluster.CreatedByRun || plan.Cluster.DeleteOnCleanup || plan.PlannedResources[0].DeleteOnCleanup {
		t.Fatalf("reused cluster deletion authority = cluster %#v resources %#v", plan.Cluster, plan.PlannedResources)
	}
	if !plan.NodePool.CreatedByRun || !plan.NodePool.DeleteOnCleanup || !plan.PlannedResources[1].DeleteOnCleanup || !plan.PlannedResources[2].DeleteOnCleanup {
		t.Fatalf("run-owned node pool is not scheduled for cleanup: nodePool %#v resources %#v", plan.NodePool, plan.PlannedResources)
	}
	for _, operation := range plan.DestructiveOperations {
		if strings.Contains(operation, "delete the exact run-owned ephemeral cluster") {
			t.Fatalf("reused cluster plan contains cluster deletion: %q", operation)
		}
	}
}

func TestBuildReleaseCandidatePlansOneRunOwnedDisposableInstance(t *testing.T) {
	request := validRequest()
	request.Profile = ProfileReleaseCandidate
	request.Parents.SizeBytes = 100_000_000_000
	plan, err := Build(request)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(plan.PlannedResources) != 5 {
		t.Fatalf("planned resources = %#v", plan.PlannedResources)
	}
	instance := plan.PlannedResources[4]
	if instance.Kind != "disposable-instance" || instance.Count != 1 || !instance.CreatedByRun || !instance.DeleteOnCleanup {
		t.Fatalf("disposable instance plan = %#v", instance)
	}
	if plan.DisposableInstance == nil || plan.DisposableInstance.Count != 1 || plan.DisposableInstance.CommercialType != request.NodePool.CommercialType || !plan.DisposableInstance.CreatedByRun || !plan.DisposableInstance.DeleteOnCleanup {
		t.Fatalf("disposable instance detail = %#v", plan.DisposableInstance)
	}
}

func TestRequestValidationRejectsUnsafeOrIncompletePlans(t *testing.T) {
	tests := map[string]func(*Request){
		"schema":           func(request *Request) { request.SchemaVersion = "2" },
		"profile":          func(request *Request) { request.Profile = "future" },
		"run ID":           func(request *Request) { request.RunID = "run" },
		"project":          func(request *Request) { request.ProjectID = "project" },
		"region":           func(request *Request) { request.Region = "nl-ams" },
		"prefix ownership": func(request *Request) { request.ResourcePrefix = "sfs-e2e-other" },
		"evidence path":    func(request *Request) { request.EvidenceDirectory = "relative" },
		"created with ID": func(request *Request) {
			request.Cluster.ExistingID = "33333333-3333-4333-8333-333333333333"
		},
		"reuse without ID": func(request *Request) { request.Cluster = ClusterRequest{Disposition: ClusterReuse} },
		"reused base cluster": func(request *Request) {
			request.Cluster = ClusterRequest{Disposition: ClusterReuse, ExistingID: "33333333-3333-4333-8333-333333333333"}
		},
		"one node":               func(request *Request) { request.NodePool.Count = 1 },
		"three-node base smoke":  func(request *Request) { request.NodePool.Count = 3 },
		"commercial type":        func(request *Request) { request.NodePool.CommercialType = "bad type" },
		"one parent":             func(request *Request) { request.Parents.Count = 1 },
		"nonminimum base parent": func(request *Request) { request.Parents.SizeBytes = 100_000_000_000 },
		"release parent without growth step": func(request *Request) {
			request.Profile = ProfileReleaseCandidate
		},
		"zero parent size":       func(request *Request) { request.Parents.SizeBytes = 0 },
		"zero cost":              func(request *Request) { request.EstimatedHourlyCostEUR = "0" },
		"exponent cost":          func(request *Request) { request.EstimatedHourlyCostEUR = "1e3" },
		"multiline source":       func(request *Request) { request.CostSource = "line one\nline two" },
		"stale beta status":      func(request *Request) { request.ProviderReview.ProductStatus = "public-beta" },
		"stale beta acceptance":  func(request *Request) { request.ProviderReview.PublicBetaAccepted = true },
		"insufficient quota":     func(request *Request) { request.ProviderReview.FileStorageQuotaRemaining = 1 },
		"bad provider timestamp": func(request *Request) { request.ProviderReview.ObservedAt = time.Now().String() },
		"short commit":           func(request *Request) { request.Artifacts.GitCommit = "abc" },
		"chart tag":              func(request *Request) { request.Artifacts.ChartDigest = "v1.0.0" },
		"mutable image": func(request *Request) {
			request.Artifacts.Images[0].Reference = "registry.example/liveness:latest"
		},
		"missing image":   func(request *Request) { request.Artifacts.Images = request.Artifacts.Images[:4] },
		"duplicate image": func(request *Request) { request.Artifacts.Images[0].Name = "driver" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := validRequest()
			request.Artifacts.Images = slices.Clone(request.Artifacts.Images)
			mutate(&request)
			if _, err := Build(request); err == nil {
				t.Fatal("Build() error = nil")
			}
		})
	}
}

func TestEncodeRejectsPlanWithMutationAuthority(t *testing.T) {
	plan, err := Build(validRequest())
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	plan.MutationAuthorized = true
	if _, err := Encode(plan); err == nil {
		t.Fatal("Encode(authorizing plan) error = nil")
	}
}
