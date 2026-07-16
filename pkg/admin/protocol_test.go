package admin

import "testing"

func TestNegotiateRequiresSameMajorAndDeclaredMinorRange(t *testing.T) {
	response := HandshakeResponse{DriverVersion: "1.2.0", ProtocolMajor: 1, MinimumMinor: 1, MaximumMinor: 3}
	for _, minor := range []uint32{1, 2, 3} {
		request := HandshakeRequest{AdminVersion: "1.2.0", Protocol: ProtocolVersion{Major: 1, Minor: minor}}
		if err := Negotiate(request, response); err != nil {
			t.Fatalf("Negotiate(minor %d) error = %v", minor, err)
		}
	}
	for name, request := range map[string]HandshakeRequest{
		"older minor": {AdminVersion: "1.0.0", Protocol: ProtocolVersion{Major: 1, Minor: 0}},
		"newer minor": {AdminVersion: "1.4.0", Protocol: ProtocolVersion{Major: 1, Minor: 4}},
		"other major": {AdminVersion: "2.0.0", Protocol: ProtocolVersion{Major: 2, Minor: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Negotiate(request, response); err == nil {
				t.Fatal("Negotiate() error = nil")
			}
		})
	}
}

func TestMutationRequestRequiresUUIDAndBuildIdentity(t *testing.T) {
	request := MutationRequest{
		RequestID:    "11111111-1111-4111-8111-111111111111",
		AdminVersion: "1.0.0", Protocol: ProtocolVersion{Major: ProtocolMajorV1, Minor: ProtocolMinorV1},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	request.RequestID = "operator-chosen-name"
	if err := request.Validate(); err == nil {
		t.Fatal("Validate(invalid request ID) error = nil")
	}
	request.RequestID = "11111111-1111-4111-8111-111111111111"
	request.AdminVersion = "1.0.0\nforged"
	if err := request.Validate(); err == nil {
		t.Fatal("Validate(multiline admin version) error = nil")
	}
}
