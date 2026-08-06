package defs

import (
	"net"
	"net/url"
)

// Arcade deployment port/path layout. Inside an arcade deployment the broadcaster,
// its SSE events stream and ChainTracks share a host but listen on distinct ports:
//
//	<arcade-host>          Arcade broadcast/status API (from the configured URL)
//	<arcade-host>:8082     Arcade SSE /events stream
//	<arcade-host>:8083/chaintracks/v2   ChainTracks headers API
const (
	// arcadeEventsPort is the port the Arcade SSE events stream listens on.
	arcadeEventsPort = "8082"
	// chainTracksPort is the port ChainTracks listens on inside arcade deployments.
	chainTracksPort = "8083"
	// chainTracksPath is the base path ChainTracks is served under. The
	// go-chaintracks client appends /tip, /header/... beneath it.
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
// host (ports 8082 / 8083) by WalletServices.deriveEndpoints when left empty, so an
// explicit configuration always wins over the derived defaults.
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
// the same scheme+host with the events port (8082).
func eventsURLFromArcade(arcadeURL string) string {
	return withPortAndPath(arcadeURL, arcadeEventsPort, "")
}

// chaintracksURLFromArcade derives the ChainTracks base URL from the Arcade URL:
// the same scheme+host with the ChainTracks port (8083) and the /chaintracks/v2 path.
func chaintracksURLFromArcade(arcadeURL string) string {
	return withPortAndPath(arcadeURL, chainTracksPort, chainTracksPath)
}

// withPortAndPath rebuilds base with the given port and path, preserving the
// scheme and replacing any explicit port on the host. It returns "" when base
// is empty or has no parseable host - the caller then leaves the derived field
// empty; Arcade.Validate (via validateArcadeURL) is what rejects a malformed
// Arcade URL with an actionable error.
func withPortAndPath(base, port, path string) string {
	if base == "" {
		return ""
	}
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	u.Host = net.JoinHostPort(u.Hostname(), port)
	u.Path = path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}
