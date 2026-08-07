package defs

import "fmt"

// ChainTracks configures the ChainTracks headers client.
//
// In the arcade-only posture ChainTracks is served by the Arcade gateway under the
// /chaintracks/v2 path (same host+port as the broadcaster), so there is no
// embedded/remote mode toggle. When URL is left empty it is derived from the Arcade
// URL by WalletServices (see network_endpoints.go); an explicit URL always wins.
//
// The go-chaintracks HTTP client treats URL as a base and requests /tip,
// /header/height/{n}, etc. under it.
type ChainTracks struct {
	Enabled bool   `mapstructure:"enabled"`
	URL     string `mapstructure:"url"`
}

// Validate checks if the ChainTracks configuration is valid.
func (c *ChainTracks) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.URL == "" {
		return fmt.Errorf("chaintracks is enabled but url is empty")
	}

	return nil
}
