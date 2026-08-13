package storage

import (
	"context"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// SetActive updates the active-storage identity key for the authenticated user.
func (p *Provider) SetActive(ctx context.Context, auth wdk.AuthID, newActiveStorageIdentityKey string) error {
	p.trace(ctx, "SetActive")
	userID, err := p.userID(auth)
	if err != nil {
		return err
	}
	if err := p.meta.Users().SetActiveStorage(ctx, userID, newActiveStorageIdentityKey); err != nil {
		return fmt.Errorf("storage: set active storage: %w", err)
	}
	return nil
}

// FindOrInsertUser returns the user for identityKey, inserting a new one (and
// seeding its default baskets) when absent. No auth: called during onboarding.
func (p *Provider) FindOrInsertUser(ctx context.Context, identityKey string) (*wdk.FindOrInsertUserResponse, error) {
	p.trace(ctx, "FindOrInsertUser")

	var (
		user   *wdk.TableUser
		wasNew bool
	)
	err := p.meta.Do(ctx, func(ctx context.Context) error {
		u, isNew, err := p.meta.Users().FindOrInsertUser(ctx, identityKey)
		if err != nil {
			return err
		}
		user, wasNew = u, isNew
		if isNew {
			// Seed default baskets and default the active storage to this store.
			if err := p.seedBaskets(ctx, u.UserID); err != nil {
				return err
			}
			if p.storageIdentityKey != "" && u.ActiveStorage == "" {
				if err := p.meta.Users().SetActiveStorage(ctx, u.UserID, p.storageIdentityKey); err != nil {
					return err
				}
				u.ActiveStorage = p.storageIdentityKey
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: find or insert user: %w", err)
	}
	return &wdk.FindOrInsertUserResponse{User: *user, IsNew: wasNew}, nil
}
