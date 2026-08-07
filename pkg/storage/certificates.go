package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// InsertCertificateAuth stores a certificate for the authenticated user. The
// certificate's Subject must be the caller's own identity key.
func (p *Provider) InsertCertificateAuth(ctx context.Context, auth wdk.AuthID, certificate *wdk.TableCertificateX) (uint, error) {
	p.trace(ctx, "InsertCertificateAuth")
	userID, err := p.userID(auth)
	if err != nil {
		return 0, err
	}
	if certificate == nil {
		return 0, fmt.Errorf("storage: nil certificate")
	}
	if string(certificate.Subject) != auth.IdentityKey {
		return 0, fmt.Errorf("storage: certificate subject must be the caller: %w", ErrAuthorization)
	}
	cert := *certificate
	cert.UserID = userID
	id, err := p.meta.Certificates().Insert(ctx, cert)
	if err != nil {
		return 0, fmt.Errorf("storage: insert certificate: %w", err)
	}
	return id, nil
}

// RelinquishCertificate soft-deletes a certificate for the authenticated user.
func (p *Provider) RelinquishCertificate(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishCertificateArgs) error {
	p.trace(ctx, "RelinquishCertificate")
	userID, err := p.userID(auth)
	if err != nil {
		return err
	}
	if err := p.meta.Certificates().SoftDelete(ctx, userID,
		string(args.Type), string(args.SerialNumber), string(args.Certifier)); err != nil {
		return fmt.Errorf("storage: relinquish certificate: %w", err)
	}
	return nil
}

// ListCertificates returns the user's certificates matching the filters.
func (p *Provider) ListCertificates(ctx context.Context, auth wdk.AuthID, args wdk.ListCertificatesArgs) (*wdk.ListCertificatesResult, error) {
	p.trace(ctx, "ListCertificates")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}
	certs, err := p.meta.Certificates().ListForUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("storage: list certificates: %w", err)
	}

	typeSet := stringSet(args.Types)
	certifierSet := stringSet(args.Certifiers)

	results := make([]*wdk.CertificateResult, 0, len(certs))
	for i := range certs {
		c := &certs[i]
		if len(typeSet) > 0 {
			if _, ok := typeSet[string(c.Type)]; !ok {
				continue
			}
		}
		if len(certifierSet) > 0 {
			if _, ok := certifierSet[string(c.Certifier)]; !ok {
				continue
			}
		}
		if args.SerialNumber != nil && string(*args.SerialNumber) != string(c.SerialNumber) {
			continue
		}
		if args.Subject != nil && string(*args.Subject) != string(c.Subject) {
			continue
		}

		res, err := p.certificateResult(ctx, c)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}

	return &wdk.ListCertificatesResult{
		TotalCertificates: primitives.PositiveInteger(len(results)), //nolint:gosec // non-negative
		Certificates:      results,
	}, nil
}

// certificateResult builds a CertificateResult (with fields) from a base row.
func (p *Provider) certificateResult(ctx context.Context, c *wdk.TableCertificate) (*wdk.CertificateResult, error) {
	fields := wdk.WalletCertificateFieldMap{}
	full, found, err := p.meta.Certificates().FindByID(ctx, c.CertificateID)
	if err != nil {
		return nil, fmt.Errorf("storage: load certificate fields: %w", err)
	}
	if found {
		for _, f := range full.Fields {
			if f == nil {
				continue
			}
			fields[primitives.StringUnder50Bytes(f.FieldName)] = f.FieldValue
		}
	}
	res := &wdk.CertificateResult{
		WalletCertificate: wdk.WalletCertificate{
			Type:               c.Type,
			Subject:            c.Subject,
			SerialNumber:       c.SerialNumber,
			Certifier:          c.Certifier,
			RevocationOutpoint: c.RevocationOutpoint,
			Signature:          c.Signature,
			Fields:             fields,
		},
		Keyring: wdk.KeyringMap{},
	}
	if c.Verifier != nil {
		res.Verifier = wdk.VerifierString(string(*c.Verifier))
	}
	return res, nil
}

func stringSet[T ~string](in []T) map[string]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(in))
	for _, v := range in {
		out[string(v)] = struct{}{}
	}
	return out
}
