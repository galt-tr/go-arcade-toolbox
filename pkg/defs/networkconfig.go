package defs

import (
	"fmt"
	"os"
	"strings"
)

// Runtime service-endpoint configuration for the tstn (Teranode Scaling Test Net) network.
//
// Unlike main, test and ttn, the tstn service endpoints are not public and must not be
// hardcoded in this (public) source tree. They are supplied at runtime through environment
// variables:
//
//	TSTN_ARCADE_URL       Arcade broadcaster endpoint base. Also the fallback host for
//	                      ChainTracks when TSTN_CHAINTRACKS_URL is unset
//	                      (${TSTN_ARCADE_URL} host, port 8083, path /chaintracks/v2).
//	TSTN_CHAINTRACKS_URL  ChainTracks service base URL.
//
// tstn runs only Arcade (broadcast + merkle proofs) and ChainTracks (headers); there is no
// block-explorer service for tstn.
const (
	// EnvTstnArcadeURL is the environment variable holding the tstn Arcade endpoint base.
	EnvTstnArcadeURL = "TSTN_ARCADE_URL"
	// EnvTstnChaintracksURL is the environment variable holding the tstn ChainTracks endpoint.
	EnvTstnChaintracksURL = "TSTN_CHAINTRACKS_URL"
)

// readEnv returns the trimmed value of the named environment variable, or "" when unset/blank.
func readEnv(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

// TstnArcadeURL returns the Arcade broadcaster endpoint for tstn from TSTN_ARCADE_URL,
// or "" when the variable is unset.
func TstnArcadeURL() string {
	return readEnv(EnvTstnArcadeURL)
}

// TstnChaintracksURL returns the ChainTracks service base URL for tstn. It falls back to
// the Arcade host (port 8083, path /chaintracks/v2) when TSTN_CHAINTRACKS_URL is unset
// (mirroring the derivation used for ttn/main), and returns an error when neither variable
// is configured.
func TstnChaintracksURL() (string, error) {
	if explicit := readEnv(EnvTstnChaintracksURL); explicit != "" {
		return explicit, nil
	}
	if arcade := TstnArcadeURL(); arcade != "" {
		return chaintracksURLFromArcade(arcade), nil
	}
	return "", fmt.Errorf(
		"tstn network requires a ChainTracks URL: set %s (or %s) in the environment",
		EnvTstnChaintracksURL, EnvTstnArcadeURL,
	)
}
