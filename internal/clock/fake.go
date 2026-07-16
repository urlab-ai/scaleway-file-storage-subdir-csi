package clock

import (
	"sync"
	"time"
)

// Manual is a deterministic clock whose timers fire only through Advance.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	clock   *Manual
	when    time.Time
	channel chan time.Time
	stopped bool
	fired   bool
}

// NewManual returns a clock at the supplied initial instant.
func NewManual(initial time.Time) *Manual {
	return &Manual{now: initial, timers: make(map[*manualTimer]struct{})}
}

// Now returns the current manual instant.
func (clock *Manual) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

// NewTimer registers a one-shot timer. Non-positive durations fire on the next
// scheduler opportunity without requiring Advance.
func (clock *Manual) NewTimer(duration time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{
		clock:   clock,
		when:    clock.now.Add(duration),
		channel: make(chan time.Time, 1),
	}
	clock.timers[timer] = struct{}{}
	if duration <= 0 {
		timer.fired = true
		timer.channel <- clock.now
		delete(clock.timers, timer)
	}
	return timer
}

// Advance moves time forward and fires every timer now due.
func (clock *Manual) Advance(duration time.Duration) {
	if duration < 0 {
		return
	}
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	for timer := range clock.timers {
		if !timer.stopped && !timer.fired && !timer.when.After(clock.now) {
			timer.fired = true
			timer.channel <- timer.when
			delete(clock.timers, timer)
		}
	}
}

func (timer *manualTimer) C() <-chan time.Time { return timer.channel }

func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	if timer.stopped || timer.fired {
		return false
	}
	timer.stopped = true
	delete(timer.clock.timers, timer)
	return true
}
