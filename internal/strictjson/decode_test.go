package strictjson

import (
	"strings"
	"testing"
)

type testRecord struct {
	Name string `json:"name"`
}

func TestDecodeAcceptsOneClosedObject(t *testing.T) {
	var record testRecord
	if err := Decode([]byte(`{"name":"volume"}`), &record); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if record.Name != "volume" {
		t.Fatalf("Name = %q, want volume", record.Name)
	}
}

func TestDecodeRejectsAmbiguousOrOpenInput(t *testing.T) {
	tests := map[string][]byte{
		"duplicate root key":   []byte(`{"name":"first","name":"second"}`),
		"duplicate nested key": []byte(`{"name":"first","nested":{"id":1,"id":2}}`),
		"unknown field":        []byte(`{"name":"volume","state":"Ready"}`),
		"trailing value":       []byte(`{"name":"volume"} {"name":"other"}`),
		"invalid UTF-8":        {'{', '"', 'n', 'a', 'm', 'e', '"', ':', '"', 0xff, '"', '}'},
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			var record testRecord
			if err := Decode(input, &record); err == nil {
				t.Fatal("Decode() error = nil, want rejection")
			}
		})
	}
}

func TestDecodeReportsDuplicateKey(t *testing.T) {
	var record testRecord
	err := Decode([]byte(`{"name":"first","name":"second"}`), &record)
	if err == nil || !strings.Contains(err.Error(), `duplicate key "name"`) {
		t.Fatalf("Decode() error = %v, want duplicate key diagnostic", err)
	}
}

func TestDecodeRejectsExcessiveNestingBeforeTypedAllocation(t *testing.T) {
	input := strings.Repeat("[", maxNestingDepth+1) + "null" + strings.Repeat("]", maxNestingDepth+1)
	var destination any
	err := Decode([]byte(input), &destination)
	if err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("Decode(excessive nesting) error = %v", err)
	}
}

func TestDecodeOpenAcceptsUnknownFieldsButRejectsDuplicateKeys(t *testing.T) {
	var destination struct {
		ID string `json:"id"`
	}
	if err := DecodeOpen([]byte(`{"id":"one","future":{"nested":true}}`), &destination); err != nil || destination.ID != "one" {
		t.Fatalf("DecodeOpen() = %#v, %v", destination, err)
	}
	if err := DecodeOpen([]byte(`{"id":"one","future":{"x":1,"x":2}}`), &destination); err == nil {
		t.Fatal("DecodeOpen(duplicate) error = nil")
	}
}
