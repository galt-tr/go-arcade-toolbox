package tsgenerated

import (
	_ "embed"
)

//go:embed create_action_result.json
var createActionResultJSON string

// CreateActionResultJSON returns the embedded create_action_result.json fixture contents.
func CreateActionResultJSON() string {
	return createActionResultJSON
}
