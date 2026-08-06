package defs

import (
	"fmt"
)

const (
	// DefaultGetBeefMaxDepth is the maximum depth for GetBEEF requests
	DefaultGetBeefMaxDepth = 100
)

// Service names
const (
	ChaintracksServiceName = "Chaintracks"
)

// WalletServices is a struct that has options for wallet services.
//
// In the arcade-only posture the only external services are Arcade (broadcast /
// status oracle) and ChainTracks (block headers). Both run inside the arcade
// deployment; ChainTracks and the Arcade SSE events URL are derived from the
// Arcade URL when not set explicitly (see deriveEndpoints).
type WalletServices struct {
	Chain           BSVNetwork `mapstructure:"-"`
	GetBeefMaxDepth uint       `mapstructure:"get_beef_max_depth"`

	Arcade      Arcade      `mapstructure:"arcade"`
	ChainTracks ChainTracks `mapstructure:"chaintracks"`
}

// deriveEndpoints fills in the Arcade SSE events URL and the ChainTracks URL from
// the Arcade URL when they are not set explicitly. Arcade SSE events run on port
// 8082 and ChainTracks runs on port 8083 (path /chaintracks/v2) inside arcade
// deployments. Explicit configuration always takes precedence.
func (ws *WalletServices) deriveEndpoints() {
	if ws.Arcade.URL == "" {
		return
	}
	if ws.Arcade.EventsURL == "" {
		ws.Arcade.EventsURL = eventsURLFromArcade(ws.Arcade.URL)
	}
	if ws.ChainTracks.URL == "" {
		ws.ChainTracks.URL = chaintracksURLFromArcade(ws.Arcade.URL)
	}
}

// Validate checks the validity of the WalletServices struct
func (ws *WalletServices) Validate() error {
	var err error

	if ws.Chain == "" {
		return fmt.Errorf("chain is required")
	}

	// Reject unknown networks up front: a typo'd chain (e.g. "mian") would
	// otherwise fall through to a silently inert config with every service
	// disabled instead of failing validation.
	if err = ws.Chain.Validate(); err != nil {
		return fmt.Errorf("invalid chain: %w", err)
	}

	// Derive the Arcade events / ChainTracks URLs from the Arcade URL before
	// validating so explicit values win and empty ones get sensible defaults.
	ws.deriveEndpoints()

	// tstn endpoints are private and supplied at runtime via environment variables.
	// Fail fast with an actionable message when they are missing, instead of surfacing
	// a generic "arcade url is empty" later on.
	if ws.Chain == NetworkTSTN {
		if ws.Arcade.URL == "" {
			return fmt.Errorf("tstn network requires %s to be set in the environment", EnvTstnArcadeURL)
		}
		if ws.ChainTracks.Enabled && ws.ChainTracks.URL == "" {
			return fmt.Errorf("tstn network requires %s or %s to be set in the environment", EnvTstnChaintracksURL, EnvTstnArcadeURL)
		}
	}

	if err = ws.Arcade.Validate(); err != nil {
		return fmt.Errorf("invalid Arcade config: %w", err)
	}

	if err = ws.ChainTracks.Validate(); err != nil {
		return fmt.Errorf("invalid Chaintracks config: %w", err)
	}

	return nil
}

// DefaultServicesConfig returns a default configuration for wallet services
func DefaultServicesConfig(chain BSVNetwork) WalletServices {
	ep := endpointsForChain(chain)

	cfg := WalletServices{
		Chain: chain,
		Arcade: Arcade{
			Enabled: ep.arcadeEnabled,
			// on networks without an Arcade default the URL is left empty on purpose:
			// flipping Enabled without a URL must not silently hit mainnet - Validate()
			// then forces an explicit URL.
			URL:               ep.arcadeURL,
			FullStatusUpdates: true,
			CircuitBreaker: ArcadeCircuitBreaker{
				FailureThreshold:           3,
				HealthProbeIntervalSeconds: 30,
			},
		},
		ChainTracks: ChainTracks{
			Enabled: ep.chaintracksEnabled,
			URL:     ep.chaintracksURL,
		},
		GetBeefMaxDepth: DefaultGetBeefMaxDepth,
	}

	// Derive the Arcade events / ChainTracks URLs from the Arcade host (ports
	// 8082 / 8083) for any field left empty by endpointsForChain.
	cfg.deriveEndpoints()

	return cfg
}
