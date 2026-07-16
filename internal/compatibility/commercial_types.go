package compatibility

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const (
	// MaxCommercialTypes bounds the release-tested Instance matrix.
	MaxCommercialTypes = 64
	// MaxCommercialTypeBytes bounds one exact Scaleway commercial type.
	MaxCommercialTypeBytes = 64
)

var commercialTypePattern = regexp.MustCompile(`^[A-Za-z0-9](?:[-A-Za-z0-9._]*[A-Za-z0-9])?$`)

// ValidateCommercialTypes requires one canonical sorted, unique, non-empty
// release allowlist. Sorting is part of the durable build/chart comparison and
// prevents two byte representations from claiming the same compatibility set.
func ValidateCommercialTypes(values []string) error {
	if len(values) == 0 || len(values) > MaxCommercialTypes {
		return fmt.Errorf("commercial type allowlist must contain 1 to %d entries", MaxCommercialTypes)
	}
	for index, value := range values {
		if len(value) == 0 || len(value) > MaxCommercialTypeBytes || !commercialTypePattern.MatchString(value) {
			return fmt.Errorf("commercial type %d must contain 1 to %d safe ASCII bytes", index, MaxCommercialTypeBytes)
		}
		if index > 0 && strings.Compare(values[index-1], value) >= 0 {
			return fmt.Errorf("commercial type allowlist must be strictly sorted and unique")
		}
	}
	return nil
}

// ParseCommercialTypes parses the canonical comma-separated build value.
func ParseCommercialTypes(value string) ([]string, error) {
	if value == "" {
		return nil, fmt.Errorf("linked commercial type allowlist is empty")
	}
	values := strings.Split(value, ",")
	if err := ValidateCommercialTypes(values); err != nil {
		return nil, err
	}
	return slices.Clone(values), nil
}

// EncodeCommercialTypes returns the canonical comma-separated linker value.
func EncodeCommercialTypes(values []string) (string, error) {
	if err := ValidateCommercialTypes(values); err != nil {
		return "", err
	}
	return strings.Join(values, ","), nil
}
