package defs

import (
	"net/url"
)

// Arcade gateway path layout. A public arcade gateway fronts the broadcaster, its
// SSE events stream and ChainTracks on ONE host+port (typically 443), routed by path:
//
//	<arcade-base>                       Arcade broadcast/status API (the configured URL)
//	<arcade-base>/events                Arcade SSE events stream
//	<arcade-base>/chaintracks/v2/...    ChainTracks headers API
//
// So the events and ChainTracks URLs are derived from the arcade base with its
// scheme+host+port PRESERVED (never a separate :8082/:8083 port — those are internal
// deployment ports, not what a public gateway exposes). An operator running a split
// deployment sets explicit events_url / chaintracks url, which always win.
const (
	// chainTracksPath is the base path ChainTracks is served under on the arcade
	// gateway. The go-chaintracks client appends /tip, /header/... beneath it.
	chainTracksPath = "/chaintracks/v2"
)

// networkEndpoints holds the per-network default service endpoints resolved by
// DefaultServicesConfig. It centralizes the "which services run on which network"
// policy for the arcade-only posture: every network is served by Arcade (broadcast
// + status) and ChainTracks (headers), so enabling one enables the other.
//
// Summary of the policy:
//
//	network  Arcade                       ChainTracks
//	main     arcade-v2-us-1               on (derived from arcade host)
//	test     off                          off
//	ttn      arcade-v2-ttn-us-1           on (derived from arcade host)
//	tstn     $TSTN_ARCADE_URL             on ($TSTN_CHAINTRACKS_URL or derived from arcade host)
//
// The SSE events URL (Arcade) and the ChainTracks URL are derived from the Arcade
// base (same scheme+host+port, ChainTracks adding /chaintracks/v2) by
// WalletServices.deriveEndpoints when left empty, so an explicit configuration
// always wins over the derived defaults.
type networkEndpoints struct {
	arcadeEnabled bool
	arcadeURL     string

	chaintracksEnabled bool
	// chaintracksURL is an explicit ChainTracks URL. When empty it is derived from
	// the Arcade host by WalletServices.deriveEndpoints.
	chaintracksURL string
}

// endpointsForChain returns the default service endpoints for the given network.
//
// For tstn the Arcade URL comes from the environment (TSTN_ARCADE_URL) and an
// optional explicit ChainTracks URL from TSTN_CHAINTRACKS_URL; both are left empty
// when unset, and WalletServices.Validate then surfaces an actionable error pointing
// at the required environment variables.
func endpointsForChain(chain BSVNetwork) networkEndpoints {
	switch chain {
	case NetworkMainnet:
		return networkEndpoints{
			arcadeEnabled:      true,
			arcadeURL:          ArcadeURL,
			chaintracksEnabled: true,
		}
	case NetworkTTN:
		return networkEndpoints{
			arcadeEnabled:      true,
			arcadeURL:          ArcadeTTNURL,
			chaintracksEnabled: true,
		}
	case NetworkTSTN:
		return networkEndpoints{
			arcadeEnabled:      true,
			arcadeURL:          TstnArcadeURL(),
			chaintracksEnabled: true,
			// explicit override only; the arcade-derived fallback is applied by
			// WalletServices.deriveEndpoints when this is empty.
			chaintracksURL: readEnv(EnvTstnChaintracksURL),
		}
	case NetworkTestnet:
		fallthrough
	default:
		return networkEndpoints{
			arcadeEnabled:      false,
			chaintracksEnabled: false,
		}
	}
}

// eventsURLFromArcade derives the Arcade SSE events base URL from the Arcade URL:
// the arcade base unchanged (same scheme+host+port). The arcade client appends
// "/events" beneath it, matching the public gateway layout (<arcade-base>/events).
func eventsURLFromArcade(arcadeURL string) string {
	return withPath(arcadeURL, "")
}

// chaintracksURLFromArcade derives the ChainTracks base URL from the Arcade URL:
// the arcade base (same scheme+host+port) with the /chaintracks/v2 path. The
// go-chaintracks client appends /tip, /header/... beneath it.
func chaintracksURLFromArcade(arcadeURL string) string {
	return withPath(arcadeURL, chainTracksPath)
}

// withPath rebuilds base with the given path, PRESERVING the scheme, host and any
// explicit port (a public arcade gateway serves every service on one port by path;
// the internal :8082/:8083 deployment ports are not what it exposes). It returns ""
// when base is empty or has no parseable host - the caller then leaves the derived
// field empty; Arcade.Validate (via validateArcadeURL) is what rejects a malformed
// Arcade URL with an actionable error.
func withPath(base, path string) string {
	if base == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	u.Path = path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}
