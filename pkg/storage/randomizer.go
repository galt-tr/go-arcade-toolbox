package storage

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// defaultRandomizer is a crypto/rand-backed [wdk.Randomizer], used for
// derivation prefixes/suffixes, references and batch ids.
type defaultRandomizer struct{}

func newDefaultRandomizer() *defaultRandomizer { return &defaultRandomizer{} }

var _ wdk.Randomizer = (*defaultRandomizer)(nil)

// Base64 returns a base64-encoded string of length random bytes.
func (defaultRandomizer) Base64(length uint64) (string, error) {
	b, err := randomBytes(length)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// Bytes returns length random bytes.
func (defaultRandomizer) Bytes(length uint64) ([]byte, error) {
	return randomBytes(length)
}

// Shuffle randomizes the order of n elements via swap.
func (defaultRandomizer) Shuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			// crypto/rand should not fail; degrade to no swap for this index.
			continue
		}
		j := int(binary.LittleEndian.Uint64(buf[:]) % uint64(i+1)) //nolint:gosec // i+1 > 0
		swap(i, j)
	}
}

// Uint64 returns a uniform random value in [0, maxVal). Returns 0 when maxVal is 0.
func (defaultRandomizer) Uint64(maxVal uint64) uint64 {
	if maxVal == 0 {
		return 0
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(buf[:]) % maxVal
}

func randomBytes(length uint64) ([]byte, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return nil, fmt.Errorf("storage: read random bytes: %w", err)
	}
	return b, nil
}
