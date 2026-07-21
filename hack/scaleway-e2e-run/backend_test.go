package main

import (
	"testing"

	k8sapi "github.com/scaleway/scaleway-sdk-go/api/k8s/v1"
)

func TestCreatableClusterTypeAvailability(t *testing.T) {
	tests := []struct {
		name         string
		availability k8sapi.ClusterTypeAvailability
		want         bool
	}{
		{name: "available", availability: k8sapi.ClusterTypeAvailabilityAvailable, want: true},
		{name: "scarce", availability: k8sapi.ClusterTypeAvailabilityScarce, want: true},
		{name: "shortage", availability: k8sapi.ClusterTypeAvailabilityShortage, want: false},
		{name: "unknown", availability: k8sapi.ClusterTypeAvailability("future-value"), want: false},
		{name: "missing", availability: "", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := creatableClusterTypeAvailability(test.availability); got != test.want {
				t.Fatalf("creatableClusterTypeAvailability(%q) = %t, want %t", test.availability, got, test.want)
			}
		})
	}
}
