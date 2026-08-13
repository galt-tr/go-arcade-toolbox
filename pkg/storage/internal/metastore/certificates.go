package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// CertificatesRepo persists user certificates and their fields.
type CertificatesRepo struct{ s *Store }

// Certificates returns the certificates repository.
func (s *Store) Certificates() CertificatesRepo { return CertificatesRepo{s} }

const certCols = "certificate_id, user_id, type, serial_number, certifier, subject, " +
	"verifier, revocation_outpoint, signature, is_deleted, created_at, updated_at"

func (r CertificatesRepo) scan(sc rowScanner) (*wdk.TableCertificate, error) {
	var (
		c         wdk.TableCertificate
		id        int64
		userID    int64
		typ       string
		serial    string
		certifier string
		subject   string
		verifier  sql.NullString
		revoke    string
		signature string
		isDeleted boolScan
		createdAt tsScan
		updatedAt tsScan
	)
	dest := []any{
		&id, &userID, &typ, &serial, &certifier, &subject,
		&verifier, &revoke, &signature, r.s.boolDest(&isDeleted),
		r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt),
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	c.CertificateID = uint(id) //nolint:gosec // DB identity id, always positive.
	c.UserID = int(userID)
	c.Type = primitives.Base64String(typ)
	c.SerialNumber = primitives.Base64String(serial)
	c.Certifier = primitives.PubKeyHex(certifier)
	c.Subject = primitives.PubKeyHex(subject)
	if v := nullStr(verifier); v != nil {
		pk := primitives.PubKeyHex(*v)
		c.Verifier = &pk
	}
	c.RevocationOutpoint = primitives.OutpointString(revoke)
	c.Signature = primitives.HexString(signature)
	c.IsDeleted = r.s.boolGet(isDeleted)
	c.CreatedAt = r.s.tsTime(createdAt)
	c.UpdatedAt = r.s.tsTime(updatedAt)
	return &c, nil
}

// Insert creates a certificate and its fields, returning the generated id. Runs
// in one transaction.
func (r CertificatesRepo) Insert(ctx context.Context, cert wdk.TableCertificateX) (uint, error) {
	var id uint
	err := r.s.inTx(ctx, func(ctx context.Context) error {
		now := r.s.encTime(r.s.now())
		var verifier any
		if cert.Verifier != nil {
			verifier = string(*cert.Verifier)
		}
		q := r.s.rebind(
			`INSERT INTO certificates
			 (user_id, type, serial_number, certifier, subject, verifier, revocation_outpoint, signature, is_deleted, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 RETURNING certificate_id`)
		var got int64
		if err := r.s.execer(ctx).QueryRowContext(ctx, q,
			cert.UserID, string(cert.Type), string(cert.SerialNumber), string(cert.Certifier),
			string(cert.Subject), verifier, string(cert.RevocationOutpoint), string(cert.Signature),
			r.s.boolVal(cert.IsDeleted), now, now,
		).Scan(&got); err != nil {
			return fmt.Errorf("metastore: insert certificate: %w", err)
		}
		id = uint(got) //nolint:gosec // DB identity id, always positive.

		for _, f := range cert.Fields {
			if f == nil {
				continue
			}
			fq := r.s.rebind(
				`INSERT INTO certificate_fields
				 (certificate_id, user_id, field_name, field_value, master_key, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?)
				 ON CONFLICT (certificate_id, field_name) DO NOTHING`)
			if _, err := r.s.execer(ctx).ExecContext(ctx, fq,
				id, cert.UserID, f.FieldName, f.FieldValue, string(f.MasterKey), now, now,
			); err != nil {
				return fmt.Errorf("metastore: insert certificate field: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// FindByID returns the certificate (with its fields) for id, or found=false when
// absent.
func (r CertificatesRepo) FindByID(ctx context.Context, id uint) (*wdk.TableCertificateX, bool, error) {
	q := r.s.rebind("SELECT " + certCols + " FROM certificates WHERE certificate_id = ?")
	base, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find certificate: %w", err)
	}
	fields, err := r.fields(ctx, id)
	if err != nil {
		return nil, false, err
	}
	return &wdk.TableCertificateX{TableCertificate: *base, Fields: fields}, true, nil
}

func (r CertificatesRepo) fields(ctx context.Context, certificateID uint) ([]*wdk.TableCertificateField, error) {
	q := r.s.rebind(
		`SELECT user_id, field_name, field_value, master_key, created_at, updated_at
		 FROM certificate_fields WHERE certificate_id = ? ORDER BY field_name ASC`)
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, certificateID)
	if err != nil {
		return nil, fmt.Errorf("metastore: certificate fields: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*wdk.TableCertificateField
	for rows.Next() {
		var (
			f         wdk.TableCertificateField
			userID    int64
			masterKey string
			createdAt tsScan
			updatedAt tsScan
		)
		if err := rows.Scan(&userID, &f.FieldName, &f.FieldValue, &masterKey,
			r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt)); err != nil {
			return nil, err
		}
		f.UserID = int(userID)
		f.CertificateID = certificateID
		f.MasterKey = primitives.Base64String(masterKey)
		f.CreatedAt = r.s.tsTime(createdAt)
		f.UpdatedAt = r.s.tsTime(updatedAt)
		out = append(out, &f)
	}
	return out, rows.Err()
}

// ListForUser returns a user's non-deleted certificates (without fields),
// newest first.
func (r CertificatesRepo) ListForUser(ctx context.Context, userID int) ([]wdk.TableCertificate, error) {
	q := r.s.rebind("SELECT " + certCols + " FROM certificates WHERE user_id = ? AND is_deleted = ? ORDER BY certificate_id DESC")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, userID, r.s.boolVal(false))
	if err != nil {
		return nil, fmt.Errorf("metastore: list certificates: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []wdk.TableCertificate
	for rows.Next() {
		c, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// SoftDelete marks a certificate deleted (and bumps updated_at so a since-based
// sync notices the relinquish). Returns an error when the certificate is absent
// or already deleted.
func (r CertificatesRepo) SoftDelete(ctx context.Context, userID int, certType, serialNumber, certifier string) error {
	q := r.s.rebind(
		`UPDATE certificates SET is_deleted = ?, updated_at = ?
		 WHERE user_id = ? AND type = ? AND serial_number = ? AND certifier = ? AND is_deleted = ?`)
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		r.s.boolVal(true), r.s.encTime(r.s.now()), userID, certType, serialNumber, certifier, r.s.boolVal(false))
	if err != nil {
		return fmt.Errorf("metastore: soft delete certificate: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: soft delete certificate (user %d, type %s, serial %s): %w",
			userID, certType, serialNumber, ErrNotFound)
	}
	return nil
}
