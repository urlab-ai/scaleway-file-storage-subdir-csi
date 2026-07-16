package pool

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrArithmeticOverflow marks a capacity result that cannot be represented
	// in the v1 unsigned 64-bit byte counters.
	ErrArithmeticOverflow = errors.New("capacity arithmetic overflow")
	// ErrInvalidStatFS marks a kernel observation that cannot safely drive a
	// physical placement decision.
	ErrInvalidStatFS = errors.New("invalid statfs observation")
)

// Ratio is a positive exact decimal maxLogicalOvercommitRatio.
type Ratio struct {
	numerator   uint64
	denominator uint64
}

// ParseRatio parses a positive, non-exponent decimal without floating point.
func ParseRatio(value string) (Ratio, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return Ratio{}, fmt.Errorf("ratio must be a non-empty canonical decimal")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" {
		return Ratio{}, fmt.Errorf("ratio %q is not a decimal", value)
	}
	if len(parts[0]) > 1 && parts[0][0] == '0' {
		return Ratio{}, fmt.Errorf("ratio %q has a non-canonical leading zero", value)
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
		if fraction == "" {
			return Ratio{}, fmt.Errorf("ratio %q has an empty fractional part", value)
		}
	}
	if len(parts[0])+len(fraction) > 18 {
		return Ratio{}, fmt.Errorf("ratio %q exceeds 18 decimal digits", value)
	}
	for _, digit := range parts[0] + fraction {
		if digit < '0' || digit > '9' {
			return Ratio{}, fmt.Errorf("ratio %q contains a non-decimal character", value)
		}
	}

	numeratorText := strings.TrimLeft(parts[0]+fraction, "0")
	if numeratorText == "" {
		return Ratio{}, fmt.Errorf("ratio must be greater than zero")
	}
	numerator, err := strconv.ParseUint(numeratorText, 10, 64)
	if err != nil {
		return Ratio{}, fmt.Errorf("parse ratio numerator: %w", err)
	}
	denominator := uint64(1)
	for range len(fraction) {
		if denominator > math.MaxUint64/10 {
			return Ratio{}, fmt.Errorf("ratio denominator: %w", ErrArithmeticOverflow)
		}
		denominator *= 10
	}
	divisor := greatestCommonDivisor(numerator, denominator)
	return Ratio{numerator: numerator / divisor, denominator: denominator / divisor}, nil
}

// Validate rejects the zero value and malformed internal ratios.
func (ratio Ratio) Validate() error {
	if ratio.numerator == 0 || ratio.denominator == 0 {
		return fmt.Errorf("ratio must be greater than zero")
	}
	return nil
}

// String returns the exact reduced rational representation for diagnostics.
func (ratio Ratio) String() string {
	if ratio.denominator == 0 {
		return "invalid"
	}
	if ratio.denominator == 1 {
		return strconv.FormatUint(ratio.numerator, 10)
	}
	return strconv.FormatUint(ratio.numerator, 10) + "/" + strconv.FormatUint(ratio.denominator, 10)
}

// Capacity is the exact logical accounting result for one parent.
type Capacity struct {
	ObservedSizeBytes     uint64
	ReserveBytes          uint64
	UsableBytes           uint64
	LogicalCapacityBytes  uint64
	LogicalAllocatedBytes uint64
	LogicalAvailableBytes uint64
}

// CalculateCapacity applies the normative reserve and overcommit formula.
func CalculateCapacity(observedSize, minFreeBytes uint64, minFreePercent uint32, ratio Ratio, allocations []uint64) (Capacity, error) {
	if observedSize == 0 {
		return Capacity{}, fmt.Errorf("observed parent size must be greater than zero")
	}
	if minFreePercent > 100 {
		return Capacity{}, fmt.Errorf("minimum free percent %d is outside [0,100]", minFreePercent)
	}
	if err := ratio.Validate(); err != nil {
		return Capacity{}, err
	}
	percentageReserve, err := ceilPercent(observedSize, minFreePercent)
	if err != nil {
		return Capacity{}, err
	}
	reserve := max(minFreeBytes, percentageReserve)
	usable := uint64(0)
	if observedSize > reserve {
		usable = observedSize - reserve
	}
	logicalCapacity, err := floorMultiplyRatio(usable, ratio)
	if err != nil {
		return Capacity{}, err
	}
	allocated := uint64(0)
	for _, allocation := range allocations {
		if allocation > math.MaxUint64-allocated {
			return Capacity{}, fmt.Errorf("sum logical allocations: %w", ErrArithmeticOverflow)
		}
		allocated += allocation
	}
	available := uint64(0)
	if logicalCapacity > allocated {
		available = logicalCapacity - allocated
	}
	return Capacity{
		ObservedSizeBytes:     observedSize,
		ReserveBytes:          reserve,
		UsableBytes:           usable,
		LogicalCapacityBytes:  logicalCapacity,
		LogicalAllocatedBytes: allocated,
		LogicalAvailableBytes: available,
	}, nil
}

// StatFSSample is the raw Linux information required for placement.
type StatFSSample struct {
	BlockSizeBytes  int64
	AvailableBlocks int64
	ObservedAt      time.Time
}

// PhysicalSpace is a validated statfs and safety-threshold calculation.
type PhysicalSpace struct {
	ActualAvailableBytes         uint64
	PhysicalSafetyThresholdBytes uint64
	PostRequestAvailableBytes    uint64
	ObservedAt                   time.Time
}

// CheckPhysicalSpace validates f_bavail*f_bsize and the post-request reserve.
func CheckPhysicalSpace(sample StatFSSample, observedSize, requested, minFreeBytes uint64, minFreePercent uint32) (PhysicalSpace, error) {
	space, err := MeasurePhysicalSpace(sample, observedSize, minFreeBytes, minFreePercent)
	if err != nil {
		return PhysicalSpace{}, err
	}
	if requested > space.ActualAvailableBytes {
		return PhysicalSpace{}, fmt.Errorf("requested bytes %d exceed actual available bytes %d: %w", requested, space.ActualAvailableBytes, ErrInsufficientPhysicalSpace)
	}
	space.PostRequestAvailableBytes = space.ActualAvailableBytes - requested
	if space.PostRequestAvailableBytes < space.PhysicalSafetyThresholdBytes {
		return space, fmt.Errorf("post-request available bytes %d are below safety threshold %d: %w", space.PostRequestAvailableBytes, space.PhysicalSafetyThresholdBytes, ErrInsufficientPhysicalSpace)
	}
	return space, nil
}

// MeasurePhysicalSpace validates one raw statfs sample and computes the current
// unprivileged-writer free bytes and safety threshold without applying a future
// allocation. This permits monitoring to publish a valid below-threshold state
// while placement still rejects it through CheckPhysicalSpace.
func MeasurePhysicalSpace(sample StatFSSample, observedSize, minFreeBytes uint64, minFreePercent uint32) (PhysicalSpace, error) {
	if sample.BlockSizeBytes <= 0 {
		return PhysicalSpace{}, fmt.Errorf("%w: block size %d must be positive", ErrInvalidStatFS, sample.BlockSizeBytes)
	}
	if sample.AvailableBlocks < 0 {
		return PhysicalSpace{}, fmt.Errorf("%w: available block count %d is negative", ErrInvalidStatFS, sample.AvailableBlocks)
	}
	if observedSize == 0 {
		return PhysicalSpace{}, fmt.Errorf("%w: observed parent size is zero", ErrInvalidStatFS)
	}
	if minFreePercent > 100 {
		return PhysicalSpace{}, fmt.Errorf("minimum free percent %d is outside [0,100]", minFreePercent)
	}
	blockSize := uint64(sample.BlockSizeBytes)
	availableBlocks := uint64(sample.AvailableBlocks)
	if availableBlocks != 0 && blockSize > math.MaxUint64/availableBlocks {
		return PhysicalSpace{}, fmt.Errorf("%w: available byte multiplication: %w", ErrInvalidStatFS, ErrArithmeticOverflow)
	}
	available := availableBlocks * blockSize
	if available > observedSize {
		return PhysicalSpace{}, fmt.Errorf("%w: available bytes %d exceed observed size %d", ErrInvalidStatFS, available, observedSize)
	}
	percentageThreshold, err := ceilPercent(observedSize, minFreePercent)
	if err != nil {
		return PhysicalSpace{}, err
	}
	threshold := max(minFreeBytes, percentageThreshold)
	space := PhysicalSpace{
		ActualAvailableBytes:         available,
		PhysicalSafetyThresholdBytes: threshold,
		PostRequestAvailableBytes:    available,
		ObservedAt:                   sample.ObservedAt,
	}
	return space, nil
}

func ceilPercent(value uint64, percent uint32) (uint64, error) {
	if percent > 100 {
		return 0, fmt.Errorf("percent %d is outside [0,100]", percent)
	}
	whole := value / 100
	remainder := value % 100
	first := whole * uint64(percent)
	partial := remainder * uint64(percent)
	second := partial / 100
	if partial%100 != 0 {
		second++
	}
	if first > math.MaxUint64-second {
		return 0, fmt.Errorf("percentage reserve: %w", ErrArithmeticOverflow)
	}
	return first + second, nil
}

func floorMultiplyRatio(value uint64, ratio Ratio) (uint64, error) {
	high, low := bits.Mul64(value, ratio.numerator)
	if high >= ratio.denominator {
		return 0, fmt.Errorf("logical capacity: %w", ErrArithmeticOverflow)
	}
	quotient, _ := bits.Div64(high, low, ratio.denominator)
	return quotient, nil
}

func greatestCommonDivisor(left, right uint64) uint64 {
	for right != 0 {
		left, right = right, left%right
	}
	return left
}
