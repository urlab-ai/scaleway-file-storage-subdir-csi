package driverapp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"scaleway-sfs-subdir-csi/internal/clock"
	"scaleway-sfs-subdir-csi/pkg/driver"
	"scaleway-sfs-subdir-csi/pkg/observability"
)

func TestKubernetesParentEventRecorderEmitsBoundedDegradationAndRecovery(t *testing.T) {
	client := fake.NewClientset()
	manual := clock.NewManual(time.Date(2026, 7, 13, 18, 0, 0, 0, time.UTC))
	recorder, err := newKubernetesParentEventRecorder(
		client.CoreV1().Events("driver-system"), manual, "driver-system", "controller-a",
		"11111111-1111-4111-8111-111111111111",
	)
	if err != nil {
		t.Fatalf("newKubernetesParentEventRecorder() error = %v", err)
	}
	parent := observability.ParentRef{Pool: "standard", Parent: "22222222-2222-4222-8222-222222222222"}
	longError := errors.New(strings.Repeat("é\n", 800))
	if err := recorder.RecordParentTransition(
		context.Background(), parent, observability.ParentConditionAvailable,
		observability.ParentConditionUnknown, driver.ParentDegradationProviderRead, longError,
	); err != nil {
		t.Fatalf("RecordParentTransition(degraded) error = %v", err)
	}
	events, err := client.CoreV1().Events("driver-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list degradation events: %v", err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("degradation events = %#v", events.Items)
	}
	if events.Items[0].Reason != "ParentDegraded" || events.Items[0].Type != "Warning" || len(events.Items[0].Message) > maxParentEventMessageBytes || !utf8.ValidString(events.Items[0].Message) || strings.ContainsAny(events.Items[0].Message, "\r\n") {
		t.Fatalf("degradation event = %#v", events.Items[0])
	}
	// The client-go fake does not implement API-server GenerateName. Remove the
	// first object so the recovery create can exercise the same production path.
	if err := client.CoreV1().Events("driver-system").Delete(context.Background(), events.Items[0].Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete fake degradation event: %v", err)
	}
	if err := recorder.RecordParentTransition(
		context.Background(), parent, observability.ParentConditionUnknown,
		observability.ParentConditionAvailable, "", nil,
	); err != nil {
		t.Fatalf("RecordParentTransition(recovered) error = %v", err)
	}
	events, err = client.CoreV1().Events("driver-system").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events.Items) != 1 {
		t.Fatalf("events = %#v", events.Items)
	}
	if events.Items[0].Reason != "ParentRecovered" || events.Items[0].Type != "Normal" {
		t.Fatalf("recovery event = %#v", events.Items[0])
	}
}
