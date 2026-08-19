// Package stress scales the concurrency tests for soak runs.
//
// Every concurrency test in this module runs a fixed number of workers for a
// fixed number of rounds. That is the right default — the suite has to stay
// fast enough to run on every push — but it means a rare interleaving is caught
// only by luck, and the properties these tests guard (no coin claimed twice, no
// input released twice) are exactly the kind that fail once in ten thousand
// attempts rather than once in twelve.
//
// [Scale] multiplies those counts by ARCADE_STRESS so the same tests become a
// soak run without a second copy of the code:
//
//	go test ./...                                   # unchanged, factor 1
//	ARCADE_STRESS=20 go test -race -run Concurrent ./...
//
// Why an environment variable rather than the alternatives:
//
//   - testing.Short() is an opt-OUT for doing less work. The need here is the
//     opposite — opt IN to more — and overloading -short to mean "and also run
//     20x harder when absent" would make the default run the slow one.
//   - A build tag would fragment the suite: the stress and non-stress variants
//     would compile separately, so the soak run would stop exercising the same
//     code the normal run does, which is the one property it must keep.
//
// The factor composes with -race and with the testcontainers harness unchanged:
// it only scales loop bounds, so a scaled run reuses the same container and the
// same per-test schema isolation.
package stress

import (
	"os"
	"strconv"
	"sync"
)

// EnvVar is the environment variable holding the stress multiplier.
const EnvVar = "ARCADE_STRESS"

var (
	once   sync.Once
	factor int
)

// Factor reports the configured multiplier: the value of ARCADE_STRESS when it
// parses as an integer >= 1, and 1 otherwise. An unset, empty, malformed, zero
// or negative value all mean "no scaling" — a soak knob must never be able to
// scale a test DOWN to zero iterations, because a test that runs zero rounds
// passes while asserting nothing.
func Factor() int {
	once.Do(func() { factor = parseFactor(os.Getenv(EnvVar)) })
	return factor
}

// parseFactor turns the raw environment value into a multiplier. Split out from
// [Factor] so it is testable without a subprocess: Factor caches in a sync.Once
// (a hot loop calling Scale must not stat the environment every iteration), and
// making the cache resettable purely to test it would weaken the thing it exists
// for.
func parseFactor(raw string) int {
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 1
	}
	return n
}

// Scale multiplies n by [Factor], for worker counts, round counts and pool
// sizes. n <= 0 is returned unchanged so a caller's own "disabled" sentinel
// survives scaling.
func Scale(n int) int {
	if n <= 0 {
		return n
	}
	return n * Factor()
}
