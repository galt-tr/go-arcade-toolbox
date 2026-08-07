package defs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
)

func TestDefaultServicesConfigPerNetwork(t *testing.T) {
	t.Run("main enables Arcade+ChainTracks with arcade-derived endpoints", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkMainnet)

		require.True(t, cfg.Arcade.Enabled)
		require.Equal(t, defs.ArcadeURL, cfg.Arcade.URL)
		// SSE events derive from the arcade base unchanged (client appends /events).
		require.Equal(t, "https://arcade-v2-us-1.bsvblockchain.tech", cfg.Arcade.EventsURL)
		require.True(t, cfg.ChainTracks.Enabled)
		// ChainTracks derives from the arcade base under /chaintracks/v2 (no port).
		require.Equal(t, "https://arcade-v2-us-1.bsvblockchain.tech/chaintracks/v2", cfg.ChainTracks.URL)

		require.NoError(t, cfg.Validate())
	})

	t.Run("test disables Arcade and ChainTracks", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkTestnet)

		require.False(t, cfg.Arcade.Enabled)
		require.Empty(t, cfg.Arcade.URL)
		require.Empty(t, cfg.Arcade.EventsURL)
		require.False(t, cfg.ChainTracks.Enabled)
		require.Empty(t, cfg.ChainTracks.URL)

		require.NoError(t, cfg.Validate())
	})

	t.Run("ttn points Arcade+ChainTracks at the public ttn arcade host", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkTTN)

		require.True(t, cfg.Arcade.Enabled)
		require.Equal(t, defs.ArcadeTTNURL, cfg.Arcade.URL)
		require.Equal(t, "https://arcade-v2-ttn-us-1.bsvblockchain.tech", cfg.Arcade.EventsURL)
		require.True(t, cfg.ChainTracks.Enabled)
		require.Equal(t, "https://arcade-v2-ttn-us-1.bsvblockchain.tech/chaintracks/v2", cfg.ChainTracks.URL)

		require.NoError(t, cfg.Validate())
	})

	t.Run("tstn reads endpoints from env and derives the rest", func(t *testing.T) {
		t.Setenv(defs.EnvTstnArcadeURL, "https://arcade.example.tstn")
		t.Setenv(defs.EnvTstnChaintracksURL, "")

		cfg := defs.DefaultServicesConfig(defs.NetworkTSTN)

		require.True(t, cfg.Arcade.Enabled)
		require.Equal(t, "https://arcade.example.tstn", cfg.Arcade.URL)
		require.Equal(t, "https://arcade.example.tstn", cfg.Arcade.EventsURL)
		require.True(t, cfg.ChainTracks.Enabled)
		// chaintracks falls back to the arcade host when TSTN_CHAINTRACKS_URL is unset.
		require.Equal(t, "https://arcade.example.tstn/chaintracks/v2", cfg.ChainTracks.URL)

		require.NoError(t, cfg.Validate())
	})

	t.Run("tstn honors an explicit TSTN_CHAINTRACKS_URL", func(t *testing.T) {
		t.Setenv(defs.EnvTstnArcadeURL, "https://arcade.example.tstn")
		t.Setenv(defs.EnvTstnChaintracksURL, "https://ct.example.tstn/v1")

		cfg := defs.DefaultServicesConfig(defs.NetworkTSTN)

		require.Equal(t, "https://ct.example.tstn/v1", cfg.ChainTracks.URL)
		require.NoError(t, cfg.Validate())
	})

	t.Run("tstn without env vars fails validation with an actionable message", func(t *testing.T) {
		t.Setenv(defs.EnvTstnArcadeURL, "")
		t.Setenv(defs.EnvTstnChaintracksURL, "")

		cfg := defs.DefaultServicesConfig(defs.NetworkTSTN)

		require.Empty(t, cfg.Arcade.URL)

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), defs.EnvTstnArcadeURL)
	})

	t.Run("explicit endpoints always override arcade derivation", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkMainnet)
		cfg.Arcade.EventsURL = "https://events.custom.example:9000"
		cfg.ChainTracks.URL = "https://ct.custom.example/v9"

		require.NoError(t, cfg.Validate())
		require.Equal(t, "https://events.custom.example:9000", cfg.Arcade.EventsURL)
		require.Equal(t, "https://ct.custom.example/v9", cfg.ChainTracks.URL)
	})

	t.Run("derivation preserves an explicit port on the arcade URL", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkMainnet)
		cfg.Arcade.URL = "https://arcade.custom.example:9999"
		cfg.Arcade.EventsURL = ""
		cfg.ChainTracks.URL = ""

		require.NoError(t, cfg.Validate())
		// A gateway on a non-standard port serves every service on THAT port by path,
		// so the port is preserved (not swapped for an internal :8082/:8083).
		require.Equal(t, "https://arcade.custom.example:9999", cfg.Arcade.EventsURL)
		require.Equal(t, "https://arcade.custom.example:9999/chaintracks/v2", cfg.ChainTracks.URL)
	})
}

func TestWalletServicesValidateChain(t *testing.T) {
	t.Run("empty chain is rejected", func(t *testing.T) {
		cfg := defs.WalletServices{}

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "chain is required")
	})

	t.Run("typo'd network fails validation instead of yielding an inert config", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig("mian")

		// endpointsForChain treats an unknown chain like testnet (everything off),
		// so without the chain check this config would validate silently.
		require.False(t, cfg.Arcade.Enabled)
		require.False(t, cfg.ChainTracks.Enabled)

		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid chain")
		require.Contains(t, err.Error(), "unsupported BSV network")
	})
}

func TestWalletServicesValidateMalformedArcadeURL(t *testing.T) {
	t.Run("scheme-less arcade URL fails with ChainTracks enabled", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkMainnet)
		cfg.Arcade.URL = "arcade.example.com"
		cfg.Arcade.EventsURL = ""
		cfg.ChainTracks.URL = ""

		// then: the error is attributed to the Arcade URL, not to ChainTracks.
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid Arcade config")
		require.Contains(t, err.Error(), "invalid arcade url")
		require.Contains(t, err.Error(), "arcade.example.com")
	})

	t.Run("scheme-less arcade URL fails with ChainTracks disabled", func(t *testing.T) {
		cfg := defs.DefaultServicesConfig(defs.NetworkMainnet)
		cfg.Arcade.URL = "arcade.example.com"
		cfg.Arcade.EventsURL = ""
		cfg.ChainTracks = defs.ChainTracks{Enabled: false}

		// then: validation must not pass with a broken (empty) EventsURL.
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid arcade url")
		require.Contains(t, err.Error(), "arcade.example.com")
	})
}
