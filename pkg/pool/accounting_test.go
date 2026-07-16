package pool

import (
	"errors"
	"math"
	"testing"
	"time"
)

func testRatio(t *testing.T, value string) Ratio {
	t.Helper()
	ratio, err := ParseRatio(value)
	if err != nil {
		t.Fatalf("ParseRatio(%q) error = %v", value, err)
	}
	return ratio
}

func TestCalculateCapacityUsesLargerReserveAndExactRatio(t *testing.T) {
	got, err := CalculateCapacity(1000, 100, 5, testRatio(t, "1.5"), []uint64{100, 200})
	if err != nil {
		t.Fatalf("CalculateCapacity() error = %v", err)
	}
	want := Capacity{
		ObservedSizeBytes:     1000,
		ReserveBytes:          100,
		UsableBytes:           900,
		LogicalCapacityBytes:  1350,
		LogicalAllocatedBytes: 300,
		LogicalAvailableBytes: 1050,
	}
	if got != want {
		t.Fatalf("CalculateCapacity() = %#v, want %#v", got, want)
	}
}

func TestCalculateCapacityRoundsPercentageUp(t *testing.T) {
	got, err := CalculateCapacity(101, 0, 5, testRatio(t, "1.0"), nil)
	if err != nil {
		t.Fatalf("CalculateCapacity() error = %v", err)
	}
	if got.ReserveBytes != 6 || got.UsableBytes != 95 {
		t.Fatalf("CalculateCapacity() reserve/usable = %d/%d, want 6/95", got.ReserveBytes, got.UsableBytes)
	}
}

func TestCalculateCapacityRejectsOverflow(t *testing.T) {
	if _, err := CalculateCapacity(math.MaxUint64, 0, 0, testRatio(t, "2"), nil); !errors.Is(err, ErrArithmeticOverflow) {
		t.Fatalf("CalculateCapacity(ratio overflow) error = %v", err)
	}
	if _, err := CalculateCapacity(100, 0, 0, testRatio(t, "1"), []uint64{math.MaxUint64, 1}); !errors.Is(err, ErrArithmeticOverflow) {
		t.Fatalf("CalculateCapacity(allocation overflow) error = %v", err)
	}
}

func TestCheckPhysicalSpaceBoundaryEquality(t *testing.T) {
	sample := StatFSSample{BlockSizeBytes: 4, AvailableBlocks: 25, ObservedAt: time.Unix(1, 0)}
	space, err := CheckPhysicalSpace(sample, 100, 90, 10, 5)
	if err != nil {
		t.Fatalf("CheckPhysicalSpace(equality) error = %v", err)
	}
	if space.ActualAvailableBytes != 100 || space.PhysicalSafetyThresholdBytes != 10 || space.PostRequestAvailableBytes != 10 {
		t.Fatalf("CheckPhysicalSpace(equality) = %#v", space)
	}
	if _, err := CheckPhysicalSpace(sample, 100, 91, 10, 5); err == nil {
		t.Fatal("CheckPhysicalSpace(one byte below threshold) error = nil")
	}
}

func TestMeasurePhysicalSpacePreservesValidBelowThresholdObservation(t *testing.T) {
	sample := StatFSSample{BlockSizeBytes: 1, AvailableBlocks: 9, ObservedAt: time.Unix(1, 0)}
	space, err := MeasurePhysicalSpace(sample, 100, 10, 5)
	if err != nil {
		t.Fatalf("MeasurePhysicalSpace() error = %v", err)
	}
	if space.ActualAvailableBytes != 9 || space.PhysicalSafetyThresholdBytes != 10 || space.PostRequestAvailableBytes != 9 {
		t.Fatalf("MeasurePhysicalSpace() = %#v", space)
	}
	if _, err := CheckPhysicalSpace(sample, 100, 0, 10, 5); err == nil {
		t.Fatal("CheckPhysicalSpace(below threshold) error = nil")
	}
}

func TestCheckPhysicalSpaceRejectsInvalidKernelObservations(t *testing.T) {
	tests := map[string]StatFSSample{
		"zero block size":         {BlockSizeBytes: 0, AvailableBlocks: 1},
		"negative block size":     {BlockSizeBytes: -1, AvailableBlocks: 1},
		"negative blocks":         {BlockSizeBytes: 1, AvailableBlocks: -1},
		"multiplication overflow": {BlockSizeBytes: math.MaxInt64, AvailableBlocks: 3},
	}
	for name, sample := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := CheckPhysicalSpace(sample, math.MaxUint64, 0, 0, 0); !errors.Is(err, ErrInvalidStatFS) {
				t.Fatalf("CheckPhysicalSpace() error = %v, want ErrInvalidStatFS", err)
			}
		})
	}
	if _, err := CheckPhysicalSpace(StatFSSample{BlockSizeBytes: 4, AvailableBlocks: 26}, 100, 0, 0, 0); !errors.Is(err, ErrInvalidStatFS) {
		t.Fatalf("CheckPhysicalSpace(available > size) error = %v", err)
	}
}

func TestParseRatioRejectsNonCanonicalOrUnsafeValues(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "1e2", "1.", ".5", " 1", "01.0", "00.1", "18446744073709551616", "0.000000000000000001"} {
		if _, err := ParseRatio(value); err == nil {
			t.Errorf("ParseRatio(%q) error = nil", value)
		}
	}
	if _, err := ParseRatio("0.00000000000000001"); err != nil {
		t.Fatalf("ParseRatio(18 digits) error = %v", err)
	}
}
