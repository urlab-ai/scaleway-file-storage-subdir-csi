package clock

import "time"

// Timer is the subset of time.Timer required by bounded polling.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

// Clock creates timers and returns a coherent current time.
type Clock interface {
	Now() time.Time
	NewTimer(duration time.Duration) Timer
}

// Real uses the process monotonic clock.
type Real struct{}

// Now returns the current time.
func (Real) Now() time.Time { return time.Now() }

// NewTimer creates a standard library timer.
func (Real) NewTimer(duration time.Duration) Timer { return realTimer{Timer: time.NewTimer(duration)} }

type realTimer struct{ *time.Timer }

func (timer realTimer) C() <-chan time.Time { return timer.Timer.C }
