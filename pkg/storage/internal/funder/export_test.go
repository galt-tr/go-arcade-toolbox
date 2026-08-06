package funder

import "context"

// WithBackoffForTest overrides the contention retry backoff so tests can
// exercise the retry loop without real sleeps. Test-only.
func WithBackoffForTest(fn func(ctx context.Context, attempt int)) Option {
	return func(f *Funder) { f.backoff = fn }
}
