package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// migratedVersion is the opaque version string Migrate returns (goose applies
// only pending versions; there is no single numeric schema version to surface).
const migratedVersion = "migrated"

// Migrate brings both stores' schemas up to date and seeds the settings row.
// It records storageName/storageIdentityKey and returns an opaque version tag.
func (p *Provider) Migrate(ctx context.Context, storageName, storageIdentityKey string) (string, error) {
	p.trace(ctx, "Migrate")
	if err := p.meta.Migrate(ctx); err != nil {
		return "", fmt.Errorf("storage: migrate metastore: %w", err)
	}
	// The utxostore ran its own migrations at construction; in Mode B a
	// separate backend may need no migration. Nothing else to do here.

	if storageName != "" {
		p.storageName = storageName
	}
	p.storageIdentityKey = storageIdentityKey

	settings := wdk.TableSettings{
		StorageIdentityKey: storageIdentityKey,
		StorageName:        p.storageName,
		CreatedAt:          p.now(),
		UpdatedAt:          p.now(),
		Chain:              p.network,
		DbType:             p.dbType(),
		MaxOutputScript:    DefaultMaxOutputScript,
	}
	raw, err := json.Marshal(settings)
	if err != nil {
		return "", fmt.Errorf("storage: marshal settings: %w", err)
	}
	if err := p.meta.KeyValue().Set(ctx, settingsKey, raw); err != nil {
		return "", fmt.Errorf("storage: persist settings: %w", err)
	}
	return migratedVersion, nil
}

// MakeAvailable returns the stored settings, making the storage available to
// the caller. It falls back to settings derived from configuration when no
// settings row has been persisted yet.
func (p *Provider) MakeAvailable(ctx context.Context) (*wdk.TableSettings, error) {
	p.trace(ctx, "MakeAvailable")
	raw, found, err := p.meta.KeyValue().Get(ctx, settingsKey)
	if err != nil {
		return nil, fmt.Errorf("storage: read settings: %w", err)
	}
	if found {
		var settings wdk.TableSettings
		if err := json.Unmarshal(raw, &settings); err != nil {
			return nil, fmt.Errorf("storage: decode settings: %w", err)
		}
		return &settings, nil
	}
	// No persisted settings: derive from configuration.
	return &wdk.TableSettings{
		StorageIdentityKey: p.storageIdentityKey,
		StorageName:        p.storageName,
		CreatedAt:          p.now(),
		UpdatedAt:          p.now(),
		Chain:              p.network,
		DbType:             p.dbType(),
		MaxOutputScript:    DefaultMaxOutputScript,
	}, nil
}
