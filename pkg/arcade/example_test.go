package arcade_test

import (
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
)

// Terminal statuses are final outcomes that a later, lower-priority update must
// not overwrite. There are exactly four: REJECTED, DOUBLE_SPEND_ATTEMPTED,
// MINED and IMMUTABLE.
func ExampleStatus_IsTerminal() {
	fmt.Println(arcade.StatusMined.IsTerminal())
	fmt.Println(arcade.StatusRejected.IsTerminal())
	fmt.Println(arcade.StatusSeenOnNetwork.IsTerminal())
	// Output:
	// true
	// true
	// false
}

// CanSupersede encodes Arcade's status lattice: it reports whether a record
// currently in status prev may be updated to s. This is what makes an
// async-rejected transaction recoverable — a peer accepting it after another
// rejected it is not a contradiction.
func ExampleStatus_CanSupersede() {
	// MINED -> IMMUTABLE is the only transition allowed to leave a terminal state.
	fmt.Println(arcade.StatusImmutable.CanSupersede(arcade.StatusMined))

	// A later SEEN_ON_NETWORK may recover a previously-REJECTED transaction.
	fmt.Println(arcade.StatusSeenOnNetwork.CanSupersede(arcade.StatusRejected))

	// But nothing lower-priority may clobber a confirmed (MINED) transaction.
	fmt.Println(arcade.StatusRejected.CanSupersede(arcade.StatusMined))
	// Output:
	// true
	// true
	// false
}
