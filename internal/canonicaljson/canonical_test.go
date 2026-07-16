package canonicaljson

import "testing"

func TestMarshalSortsKeysAndPreservesUTF8JSONSemantics(t *testing.T) {
	got, err := Marshal(map[string]any{
		"z": "<parent>",
		"a": uint64(42),
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	const want = `{"a":42,"z":"<parent>"}`
	if string(got) != want {
		t.Fatalf("Marshal() = %s, want %s", got, want)
	}
}
