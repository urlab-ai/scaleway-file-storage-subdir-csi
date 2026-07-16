package canonicaljson

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Marshal returns one deterministic JSON value without a trailing newline.
func Marshal(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}

	encoded := buffer.Bytes()
	if len(encoded) == 0 || encoded[len(encoded)-1] != '\n' {
		return nil, fmt.Errorf("encode canonical JSON: encoder omitted terminator")
	}
	return bytes.Clone(encoded[:len(encoded)-1]), nil
}
