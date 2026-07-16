package clock

import (
	"testing"
	"time"
)

func TestManualTimerFiresOnlyAfterAdvance(t *testing.T) {
	initial := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	clock := NewManual(initial)
	timer := clock.NewTimer(time.Minute)
	clock.Advance(59 * time.Second)
	select {
	case <-timer.C():
		t.Fatal("timer fired early")
	default:
	}
	clock.Advance(time.Second)
	select {
	case got := <-timer.C():
		if !got.Equal(initial.Add(time.Minute)) {
			t.Fatalf("timer fired at %s, want %s", got, initial.Add(time.Minute))
		}
	default:
		t.Fatal("timer did not fire")
	}
}

func TestManualTimerStop(t *testing.T) {
	clock := NewManual(time.Unix(0, 0))
	timer := clock.NewTimer(time.Second)
	if !timer.Stop() || timer.Stop() {
		t.Fatal("Stop() did not report first/second call correctly")
	}
	clock.Advance(time.Hour)
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}
}
