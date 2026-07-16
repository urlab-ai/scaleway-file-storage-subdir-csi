package driverapp

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/clock"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/driver"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/observability"
)

const maxParentEventMessageBytes = 1024

type controllerParentEventRecorder interface {
	RecordParentTransition(ctx context.Context, parent observability.ParentRef, previous, current observability.ParentCondition, reason driver.ParentDegradationReason, degradationErr error) error
}

type kubernetesParentEventRecorder struct {
	events    typedcorev1.EventInterface
	clock     clock.Clock
	namespace string
	podName   string
	podUID    types.UID
}

func newKubernetesParentEventRecorder(events typedcorev1.EventInterface, operationClock clock.Clock, namespace, podName, podUID string) (*kubernetesParentEventRecorder, error) {
	if events == nil || operationClock == nil {
		return nil, fmt.Errorf("parent event recorder dependency is nil")
	}
	if namespace == "" || podName == "" || podUID == "" || strings.ContainsAny(namespace+podName+podUID, "\x00\r\n") {
		return nil, fmt.Errorf("parent event recorder Pod identity is incomplete or invalid")
	}
	return &kubernetesParentEventRecorder{
		events: events, clock: operationClock, namespace: namespace,
		podName: podName, podUID: types.UID(podUID),
	}, nil
}

func (recorder *kubernetesParentEventRecorder) RecordParentTransition(ctx context.Context, parent observability.ParentRef, previous, current observability.ParentCondition, reason driver.ParentDegradationReason, degradationErr error) error {
	eventType := corev1.EventTypeWarning
	eventReason := "ParentDegraded"
	message := fmt.Sprintf("File Storage parent %s in pool %s changed from %s to %s", parent.Parent, parent.Pool, transitionCondition(previous), current)
	if current == observability.ParentConditionAvailable {
		eventType = corev1.EventTypeNormal
		eventReason = "ParentRecovered"
	} else {
		if reason != "" {
			message += fmt.Sprintf(" (%s)", reason)
		}
		if degradationErr != nil {
			message += ": " + degradationErr.Error()
		}
	}
	message = boundedEventMessage(message)
	now := metav1.NewTime(recorder.clock.Now())
	_, err := recorder.events.Create(ctx, &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{GenerateName: "sfs-subdir-parent-", Namespace: recorder.namespace},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1", Kind: "Pod", Namespace: recorder.namespace,
			Name: recorder.podName, UID: recorder.podUID,
		},
		Reason: eventReason, Message: message, Type: eventType,
		Source:         corev1.EventSource{Component: "scaleway-sfs-subdir-csi-controller", Host: recorder.podName},
		FirstTimestamp: now, LastTimestamp: now, Count: 1,
		Action: "ParentHealthChanged", ReportingController: "scaleway-sfs-subdir-csi-controller",
		ReportingInstance: recorder.podName,
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create Kubernetes %s event for parent %q: %w", eventReason, parent.Parent, err)
	}
	return nil
}

func transitionCondition(condition observability.ParentCondition) string {
	if condition == "" {
		return "unobserved"
	}
	return string(condition)
}

func boundedEventMessage(message string) string {
	message = boundedLogValue(message)
	if len(message) <= maxParentEventMessageBytes {
		return message
	}
	message = message[:maxParentEventMessageBytes]
	for !utf8.ValidString(message) {
		message = message[:len(message)-1]
	}
	return message
}
