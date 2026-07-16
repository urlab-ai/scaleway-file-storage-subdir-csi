package strictjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"unicode/utf8"
)

const maxNestingDepth = 100

// Decode decodes one closed-schema JSON value into destination.
func Decode(data []byte, destination any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("decode strict JSON: input is not valid UTF-8")
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode strict JSON: %w", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	return nil
}

// DecodeOpen decodes one provider-owned JSON value while accepting fields
// added by a compatible upstream API. It still enforces valid UTF-8, rejects
// duplicate keys at every depth, and requires exactly one top-level value.
func DecodeOpen(data []byte, destination any) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("decode open JSON: input is not valid UTF-8")
	}
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("decode open JSON: %w", err)
	}
	return requireEOF(decoder)
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := inspectValue(decoder, "$", 0); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func inspectValue(decoder *json.Decoder, path string, depth int) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode strict JSON at %s: %w", path, err)
	}

	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if depth >= maxNestingDepth {
		return fmt.Errorf("decode strict JSON at %s: nesting exceeds %d levels", path, maxNestingDepth)
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("decode strict JSON object at %s: %w", path, err)
			}
			name, ok := nameToken.(string)
			if !ok {
				return fmt.Errorf("decode strict JSON object at %s: key is not a string", path)
			}
			if _, exists := seen[name]; exists {
				return fmt.Errorf("decode strict JSON at %s: duplicate key %q", path, name)
			}
			seen[name] = struct{}{}
			if err := inspectValue(decoder, path+"."+name, depth+1); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode strict JSON object at %s: %w", path, err)
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("decode strict JSON object at %s: unexpected closing token %v", path, closing)
		}
	case '[':
		index := 0
		for decoder.More() {
			if err := inspectValue(decoder, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
				return err
			}
			index++
		}
		closing, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode strict JSON array at %s: %w", path, err)
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("decode strict JSON array at %s: unexpected closing token %v", path, closing)
		}
	default:
		return fmt.Errorf("decode strict JSON at %s: unexpected delimiter %q", path, delimiter)
	}

	return nil
}

func requireEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return fmt.Errorf("decode strict JSON trailing data: %w", err)
	}
	return fmt.Errorf("decode strict JSON: multiple top-level values")
}
