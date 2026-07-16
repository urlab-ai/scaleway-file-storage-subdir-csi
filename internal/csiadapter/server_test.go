package csiadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"scaleway-sfs-subdir-csi/pkg/config"
	"scaleway-sfs-subdir-csi/pkg/observability"
)

type recordedCSIObservation struct {
	operation observability.CSIOperation
	code      observability.RPCCode
	duration  time.Duration
	calls     int
	err       error
}

func (observation *recordedCSIObservation) ObserveCSI(operation observability.CSIOperation, code observability.RPCCode, duration time.Duration) error {
	observation.operation = operation
	observation.code = code
	observation.duration = duration
	observation.calls++
	return observation.err
}

func TestCSIMetricsUnaryInterceptorRecordsBoundedCompletion(t *testing.T) {
	observer := &recordedCSIObservation{}
	failures := 0
	interceptor := csiMetricsUnaryInterceptor(config.ComponentController, observer, func(error) { failures++ })
	handlerErr := status.Error(codes.Unavailable, "provider temporarily unavailable")
	response := &struct{ value string }{value: "unchanged"}

	returnedResponse, returnedErr := interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Controller/CreateVolume",
	}, func(context.Context, any) (any, error) {
		return response, handlerErr
	})

	if returnedResponse != response || returnedErr != handlerErr {
		t.Fatalf("interceptor result = (%p, %v), want unchanged (%p, %v)", returnedResponse, returnedErr, response, handlerErr)
	}
	if observer.calls != 1 || observer.operation != observability.CSICreateVolume || observer.code != observability.CodeUnavailable {
		t.Fatalf("observation = %#v, want one CreateVolume/Unavailable completion", observer)
	}
	if observer.duration < 0 {
		t.Fatalf("observation duration = %v, want non-negative", observer.duration)
	}
	if failures != 0 {
		t.Fatalf("failure reports = %d, want 0", failures)
	}
}

func TestCSIMetricsUnaryInterceptorReportsObservationFailureOutOfBand(t *testing.T) {
	metricErr := errors.New("metrics registry failure")
	observer := &recordedCSIObservation{err: metricErr}
	var reported error
	interceptor := csiMetricsUnaryInterceptor(config.ComponentNode, observer, func(err error) { reported = err })
	response := &struct{}{}

	returnedResponse, returnedErr := interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{
		FullMethod: "/csi.v1.Node/NodePublishVolume",
	}, func(context.Context, any) (any, error) {
		return response, nil
	})

	if returnedResponse != response || returnedErr != nil {
		t.Fatalf("interceptor result = (%p, %v), want unchanged (%p, nil)", returnedResponse, returnedErr, response)
	}
	if !errors.Is(reported, metricErr) {
		t.Fatalf("reported error = %v, want metrics failure", reported)
	}
}

func TestCSIMetricsUnaryInterceptorSkipsUnknownAndForeignMethods(t *testing.T) {
	for _, test := range []struct {
		name      string
		component config.Component
		method    string
	}{
		{name: "unknown", component: config.ComponentController, method: "/csi.v1.Controller/FutureMethod"},
		{name: "node method on controller", component: config.ComponentController, method: "/csi.v1.Node/NodeGetInfo"},
		{name: "controller method on node", component: config.ComponentNode, method: "/csi.v1.Controller/CreateVolume"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observer := &recordedCSIObservation{}
			handlerCalls := 0
			interceptor := csiMetricsUnaryInterceptor(test.component, observer, func(error) {
				t.Fatal("unexpected observation failure")
			})
			_, err := interceptor(context.Background(), struct{}{}, &grpc.UnaryServerInfo{FullMethod: test.method}, func(context.Context, any) (any, error) {
				handlerCalls++
				return nil, nil
			})
			if err != nil || handlerCalls != 1 || observer.calls != 0 {
				t.Fatalf("result err=%v handlerCalls=%d observerCalls=%d, want nil/1/0", err, handlerCalls, observer.calls)
			}
		})
	}
}

func TestObservableRPCCodeIsClosed(t *testing.T) {
	tests := map[codes.Code]observability.RPCCode{
		codes.OK: observability.CodeOK, codes.Canceled: observability.CodeCanceled,
		codes.Unknown: observability.CodeUnknown, codes.InvalidArgument: observability.CodeInvalidArgument,
		codes.DeadlineExceeded: observability.CodeDeadlineExceeded, codes.NotFound: observability.CodeNotFound,
		codes.AlreadyExists: observability.CodeAlreadyExists, codes.PermissionDenied: observability.CodePermissionDenied,
		codes.ResourceExhausted: observability.CodeResourceExhausted, codes.FailedPrecondition: observability.CodeFailedPrecondition,
		codes.Aborted: observability.CodeAborted, codes.OutOfRange: observability.CodeOutOfRange,
		codes.Unimplemented: observability.CodeUnimplemented, codes.Internal: observability.CodeInternal,
		codes.Unavailable: observability.CodeUnavailable, codes.DataLoss: observability.CodeDataLoss,
		codes.Unauthenticated: observability.CodeUnauthenticated, codes.Code(99): observability.CodeUnknown,
	}
	for input, want := range tests {
		if got := observableRPCCode(input); got != want {
			t.Errorf("observableRPCCode(%v) = %q, want %q", input, got, want)
		}
	}
}
