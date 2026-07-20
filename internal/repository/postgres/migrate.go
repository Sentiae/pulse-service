package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	migratepg "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"gorm.io/gorm"

	"github.com/sentiae/pulse-service/migrations"
)

// RunMigrations applies the embedded golang-migrate SQL migrations
// (migrations/NNNN_*.up.sql) against the connected database (CLAUDE.md §24,
// mirrors ops-service). This is the authoritative schema source — it replaces
// the old GORM AutoMigrate + ApplyPreMigrate + ApplyRLSObjects boot path (D-178).
// migrate.ErrNoChange is not an error (an already-current DB is a clean no-op).
//
// MUST be called under the OWNER connection (pulse_service_owner): the baseline
// alters table RLS state and transfers resolver-function ownership to
// pulse_service_system, which the NOBYPASSRLS app role cannot do.
//
// Returns the schema version now current and whether anything was applied.
func RunMigrations(db *gorm.DB) (version uint, applied bool, err error) {
	sqlDB, err := db.DB()
	if err != nil {
		return 0, false, fmt.Errorf("migrate: unwrap sql.DB: %w", err)
	}

	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return 0, false, fmt.Errorf("migrate: open embedded source: %w", err)
	}

	// Pin ONE connection explicitly (WithConnection) instead of WithInstance on the
	// pool. WithInstance checks out + PINS the pool's only conn for the driver's
	// lifetime; on the small OWNER pool used at the RLS flip (D-070) that risks the
	// 1-conn deadlock (fleet gotcha). With an explicit pinned conn, m.Close()
	// releases ONLY that conn + the source, and the caller closes the short-lived
	// owner pool afterwards.
	ctx := context.Background()
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("migrate: pin conn: %w", err)
	}
	driver, err := migratepg.WithConnection(ctx, conn, &migratepg.Config{})
	if err != nil {
		_ = conn.Close()
		return 0, false, fmt.Errorf("migrate: init postgres driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		_ = conn.Close()
		return 0, false, fmt.Errorf("migrate: init: %w", err)
	}
	defer func() { _, _ = m.Close() }() // closes the pinned conn + source only

	applied = true
	if err := m.Up(); err != nil {
		if !errors.Is(err, migrate.ErrNoChange) {
			return 0, false, fmt.Errorf("migrate: up: %w", err)
		}
		applied = false
	}
	version, dirty, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return 0, applied, fmt.Errorf("migrate: read version: %w", err)
	}
	if dirty {
		return version, applied, fmt.Errorf("migrate: schema version %d is dirty — manual repair required", version)
	}
	return version, applied, nil
}
