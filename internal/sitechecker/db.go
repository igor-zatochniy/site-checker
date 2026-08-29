package sitechecker

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const databaseRollbackTimeout = 2 * time.Second

//go:embed migrations/*.sql
var migrationFiles embed.FS

var ErrDatabaseSchemaIncompatible = errors.New("database schema is incompatible with this build")

func OpenPostgresPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return OpenPostgresPoolWithTimeout(ctx, databaseURL, defaultDatabaseOperationTimeout)
}

func OpenPostgresPoolWithTimeout(ctx context.Context, databaseURL string, timeout time.Duration) (*pgxpool.Pool, error) {
	ctx, cancel := databaseTimeoutContext(ctx, timeout, defaultDatabaseOperationTimeout)
	defer cancel()

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		var parseErr *pgconn.ParseConfigError
		if errors.As(err, &parseErr) {
			return nil, errors.New("invalid PostgreSQL connection string")
		}
		return nil, err
	}
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 1
	poolConfig.MaxConnLifetime = time.Hour
	poolConfig.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	return RunMigrationsWithTimeout(ctx, pool, defaultDatabaseMigrationTimeout)
}

func RunMigrationsWithTimeout(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	ctx, cancel := databaseTimeoutContext(ctx, timeout, defaultDatabaseMigrationTimeout)
	defer cancel()

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			rollbackTransaction(ctx, tx)
		}
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('site_checker_migrations'))"); err != nil {
		return err
	}

	names, err := embeddedMigrationNames()
	if err != nil {
		return err
	}

	for _, name := range names {
		version := strings.TrimSuffix(name, filepath.Ext(name))
		applied, err := migrationApplied(ctx, tx, version)
		if err != nil {
			return err
		}
		if applied {
			continue
		}

		sqlBytes, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		if err := applyMigration(ctx, tx, version, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

func VerifyPostgresSchema(ctx context.Context, pool *pgxpool.Pool) error {
	return VerifyPostgresSchemaWithTimeout(ctx, pool, defaultDatabaseOperationTimeout)
}

func VerifyPostgresSchemaWithTimeout(ctx context.Context, pool *pgxpool.Pool, timeout time.Duration) error {
	ctx, cancel := databaseTimeoutContext(ctx, timeout, defaultDatabaseOperationTimeout)
	defer cancel()

	names, err := embeddedMigrationNames()
	if err != nil {
		return err
	}

	rows, err := pool.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "42P01" {
			return fmt.Errorf("%w: migration metadata is missing", ErrDatabaseSchemaIncompatible)
		}
		return fmt.Errorf("read schema migration metadata: %w", err)
	}
	defer rows.Close()

	applied := make(map[string]struct{}, len(names))
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return fmt.Errorf("scan schema migration metadata: %w", err)
		}
		applied[version] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema migration metadata: %w", err)
	}

	missing := make([]string, 0)
	for _, name := range names {
		version := strings.TrimSuffix(name, filepath.Ext(name))
		if _, ok := applied[version]; !ok {
			missing = append(missing, version)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%w: missing migrations: %s", ErrDatabaseSchemaIncompatible, strings.Join(missing, ", "))
	}
	return nil
}

func embeddedMigrationNames() ([]string, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	if len(names) == 0 {
		return nil, errors.New("no embedded database migrations found")
	}
	return names, nil
}

func databaseTimeoutContext(parent context.Context, timeout, fallback time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = fallback
	}
	return context.WithTimeout(parent, timeout)
}

func rollbackTransaction(parent context.Context, tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), databaseRollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func migrationApplied(ctx context.Context, tx pgx.Tx, version string) (bool, error) {
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`); err != nil {
		return false, err
	}

	var exists bool
	err := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)", version).Scan(&exists)
	return exists, err
}

func applyMigration(ctx context.Context, tx pgx.Tx, version, sql string) error {
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", version); err != nil {
		return err
	}
	return nil
}
