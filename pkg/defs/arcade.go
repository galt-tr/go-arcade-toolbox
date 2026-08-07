package defs

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ArcadeServiceName is the service name used in queues/results for the Arcade broadcaster.
const ArcadeServiceName = "Arcade"

// ArcadeURL is the default mainnet Arcade instance.
const ArcadeURL = "https://arcade-v2-us-1.bsvblockchain.tech"

// ArcadeTTNURL is the default Arcade instance for the public Teranode Test Net (ttn).
// The same host+port also serves ChainTracks (path /chaintracks/v2) and the SSE events
// stream (path /events); those URLs are derived from this base by DefaultServicesConfig.
const ArcadeTTNURL = "https://arcade-v2-ttn-us-1.bsvblockchain.tech"

// ArcadeCircuitBreaker configures when the wallet fails over away from Arcade.
type ArcadeCircuitBreaker struct {
	// FailureThreshold is the number of consecutive transport failures that opens the circuit.
	FailureThreshold uint `mapstructure:"failure_threshold"`
	// HealthProbeIntervalSeconds is how often /health is probed while the circuit is open.
	HealthProbeIntervalSeconds uint `mapstructure:"health_probe_interval_seconds"`
}

// ArcadeInstance identifies a single Arcade deployment. Multiple instances are
// reserved for a future high-availability (HA) mode; today at most one instance
// may be configured.
type ArcadeInstance struct {
	// URL is the base URL of the Arcade instance.
	URL string `mapstructure:"url"`
	// EventsURL is the base URL for the SSE /events endpoint of this instance.
	EventsURL string `mapstructure:"events_url"`
	// CallbackToken scopes webhooks and the SSE stream to this wallet instance.
	CallbackToken string `mapstructure:"callback_token"`
}

// Arcade is the configuration for the Arcade broadcaster (primary broadcast path).
type Arcade struct {
	Enabled bool `mapstructure:"enabled"`
	// URL is the base URL of the Arcade instance.
	URL string `mapstructure:"url"`
	// EventsURL is the base URL for the SSE /events endpoint; defaults to URL.
	EventsURL string `mapstructure:"events_url"`
	// CallbackToken scopes webhooks and the SSE stream to this wallet instance.
	// When empty it is derived from the wallet identity key at wiring time.
	CallbackToken string `mapstructure:"callback_token"`
	// CallbackURL is an optional public webhook endpoint (X-CallbackUrl).
	CallbackURL string `mapstructure:"callback_url"`
	// FullStatusUpdates requests every status transition (X-FullStatusUpdates).
	FullStatusUpdates bool `mapstructure:"full_status_updates"`

	CircuitBreaker ArcadeCircuitBreaker `mapstructure:"circuit_breaker"`

	// Instances is reserved for a future HA mode; at most one instance is
	// supported today (see Validate).
	Instances []ArcadeInstance `mapstructure:"instances"`
}

// Validate checks the Arcade configuration and defaults EventsURL to URL.
func (a *Arcade) Validate() error {
	if len(a.Instances) > 1 {
		return fmt.Errorf("multiple arcade instances (HA) are not yet supported")
	}
	if !a.Enabled {
		return nil
	}
	if a.URL == "" {
		return fmt.Errorf("arcade is enabled but url is empty")
	}
	if err := validateArcadeURL(a.URL); err != nil {
		return err
	}
	if err := validateExternalCallbackURL(a.CallbackURL); err != nil {
		return fmt.Errorf("invalid callback URL: %w", err)
	}
	if a.EventsURL == "" {
		a.EventsURL = a.URL
	}
	return nil
}

// validateArcadeURL checks that the Arcade base URL is parseable, uses an
// http/https scheme and has a non-empty host — the shape the events/ChainTracks
// endpoint derivation (same base + /events, + /chaintracks/v2) relies on. Without
// this check a scheme-less URL would silently yield empty derived endpoints.
func validateArcadeURL(arcadeURL string) error {
	u, err := url.Parse(arcadeURL)
	if err != nil {
		return fmt.Errorf("invalid arcade url: %s - %w", arcadeURL, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid arcade url: %s - it should start with http:// or https://", arcadeURL)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("invalid arcade url: %s - it must include a host", arcadeURL)
	}
	return nil
}

// validateExternalCallbackURL checks that a callback URL (when set) uses an
// http/https scheme and does not point at the local network.
func validateExternalCallbackURL(callbackURLString string) error {
	if callbackURLString == "" {
		return nil
	}

	callbackURL, err := url.Parse(callbackURLString)
	if err != nil {
		return fmt.Errorf("invalid callback URL: %s - %w", callbackURLString, err)
	}

	schema := callbackURL.Scheme
	if schema != "http" && schema != "https" {
		return fmt.Errorf("invalid callback URL: %s - it should start with http:// or https://", callbackURLString)
	}

	hostname := callbackURL.Hostname()

	if isLocalNetworkHost(hostname) {
		return fmt.Errorf("invalid callback host: %s - must be a valid external URL - not a localhost", hostname)
	}

	return nil
}

func isLocalNetworkHost(hostname string) bool {
	if strings.Contains(hostname, "localhost") {
		return true
	}

	ip := net.ParseIP(hostname)
	if ip != nil {
		// RFC 1918 / RFC 5735 well-known private and reserved CIDR ranges used
		// for local-network detection — these are not hardcoded service addresses.
		_, private10, _ := net.ParseCIDR("10.0.0.0/8")
		_, private172, _ := net.ParseCIDR("172.16.0.0/12")
		_, private192, _ := net.ParseCIDR("192.168.0.0/16")
		_, loopback, _ := net.ParseCIDR("127.0.0.0/8")
		_, linkLocal, _ := net.ParseCIDR("169.254.0.0/16")

		return private10.Contains(ip) || private172.Contains(ip) || private192.Contains(ip) || loopback.Contains(ip) || linkLocal.Contains(ip)
	}

	return false
}
