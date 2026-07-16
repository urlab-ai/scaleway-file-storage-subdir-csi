package pool

import "context"

// StatFSSampler reads the unprivileged-writer free-space fields from one
// already-validated mounted parent root.
type StatFSSampler interface {
	Sample(ctx context.Context, parentRoot string) (StatFSSample, error)
}
