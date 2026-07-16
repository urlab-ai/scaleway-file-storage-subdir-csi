package driverapp

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestBoundedLogValueFlattensAndBoundsUntrustedText(t *testing.T) {
	value := boundedLogValue(strings.Repeat("é\n", maxStructuredLogValueBytes))
	if len(value) > maxStructuredLogValueBytes || !utf8.ValidString(value) {
		t.Fatalf("bounded value length/UTF-8 = %d/%v", len(value), utf8.ValidString(value))
	}
	if strings.ContainsAny(value, "\x00\r\n") {
		t.Fatalf("bounded value contains record separator: %q", value)
	}
}
