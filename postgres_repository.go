package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresMonitorRepository struct {
	pool   *pgxpool.Pool
	policy *NetworkPolicy
}

func NewPostgresMonitorRepository(pool *pgxpool.Pool, policy *NetworkPolicy) *PostgresMonitorRepository {
	return &PostgresMonitorRepository{pool: pool, policy: policy}
}

func (r *PostgresMonitorRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *PostgresMonitorRepository) Count(ctx context.Context) (int, error) {
	var total int
	err := r.pool.QueryRow(ctx, "SELECT count(*) FROM monitors").Scan(&total)
	return total, err
}

func (r *PostgresMonitorRepository) Create(ctx context.Context, input MonitorInput) (Monitor, error) {
	if err := validateMonitorInput(input, r.policy); err != nil {
		return Monitor{}, err
	}

	now := time.Now().UTC()
	enabled := true
	if input.Enabled != nil {
		enabled = *input.Enabled
	}
	status := monitorStatusActive
	if !enabled {
		status = monitorStatusDisabled
	}

	monitor := Monitor{
		ID:              newMonitorID(),
		URL:             input.URL,
		IntervalSeconds: input.IntervalSeconds,
		TimeoutSeconds:  input.TimeoutSeconds,
		ExpectedStatus:  input.ExpectedStatus,
		Status:          status,
		Enabled:         enabled,
		NextCheckAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	row := r.pool.QueryRow(ctx, `
		INSERT INTO monitors (
			id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, monitor.ID, monitor.URL, monitor.IntervalSeconds, monitor.TimeoutSeconds, monitor.ExpectedStatus,
		monitor.Status, monitor.Enabled, monitor.NextCheckAt, monitor.CreatedAt, monitor.UpdatedAt)

	created, err := scanMonitor(row)
	if isUniqueViolation(err) {
		return Monitor{}, ErrMonitorExists
	}
	return created, err
}

func (r *PostgresMonitorRepository) List(ctx context.Context, offset, limit int) ([]Monitor, int, error) {
	total, err := r.Count(ctx)
	if err != nil {
		return nil, 0, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
		FROM monitors
		ORDER BY created_at, id
		OFFSET $1 LIMIT $2
	`, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	monitors, err := scanMonitors(rows)
	return monitors, total, err
}

func (r *PostgresMonitorRepository) Get(ctx context.Context, id string) (Monitor, error) {
	monitor, err := scanMonitor(r.pool.QueryRow(ctx, `
		SELECT id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
		FROM monitors
		WHERE id = $1
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	return monitor, err
}

func (r *PostgresMonitorRepository) Update(ctx context.Context, id string, patch MonitorPatch) (Monitor, error) {
	current, err := r.Get(ctx, id)
	if err != nil {
		return Monitor{}, err
	}

	updated := current
	if patch.URL != nil {
		updated.URL = *patch.URL
	}
	if patch.IntervalSeconds != nil {
		updated.IntervalSeconds = *patch.IntervalSeconds
	}
	if patch.TimeoutSeconds != nil {
		updated.TimeoutSeconds = *patch.TimeoutSeconds
	}
	if patch.ExpectedStatus != nil {
		updated.ExpectedStatus = *patch.ExpectedStatus
	}
	if patch.Enabled != nil {
		updated.Enabled = *patch.Enabled
	}
	if err := validateMonitorInput(MonitorInput{
		URL:             updated.URL,
		IntervalSeconds: updated.IntervalSeconds,
		TimeoutSeconds:  updated.TimeoutSeconds,
		ExpectedStatus:  updated.ExpectedStatus,
		Enabled:         &updated.Enabled,
	}, r.policy); err != nil {
		return Monitor{}, err
	}

	now := time.Now().UTC()
	updated.UpdatedAt = now
	if updated.Enabled {
		updated.Status = monitorStatusActive
		if updated.NextCheckAt.IsZero() || updated.NextCheckAt.Before(now) {
			updated.NextCheckAt = now
		}
	} else {
		updated.Status = monitorStatusDisabled
	}

	row := r.pool.QueryRow(ctx, `
		UPDATE monitors
		SET url = $2,
			interval_seconds = $3,
			timeout_seconds = $4,
			expected_status = $5,
			status = $6,
			enabled = $7,
			next_check_at = $8,
			updated_at = $9,
			pending = CASE WHEN $7 THEN pending ELSE false END,
			pending_since = CASE WHEN $7 THEN pending_since ELSE NULL END,
			pending_job_id = CASE WHEN $7 THEN pending_job_id ELSE NULL END
		WHERE id = $1
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, id, updated.URL, updated.IntervalSeconds, updated.TimeoutSeconds, updated.ExpectedStatus,
		updated.Status, updated.Enabled, updated.NextCheckAt, updated.UpdatedAt)

	monitor, err := scanMonitor(row)
	if isUniqueViolation(err) {
		return Monitor{}, ErrMonitorExists
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	return monitor, err
}

func (r *PostgresMonitorRepository) Delete(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx, "DELETE FROM monitors WHERE id = $1", id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrMonitorNotFound
	}
	return nil
}

func (r *PostgresMonitorRepository) ClaimDue(ctx context.Context, limit int, now time.Time, leaseTimeout time.Duration) ([]Monitor, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	leaseCutoff := now.UTC()
	canReclaim := leaseTimeout > 0
	if leaseTimeout > 0 {
		leaseCutoff = now.UTC().Add(-leaseTimeout)
	}

	rows, err := tx.Query(ctx, `
		WITH due AS (
			SELECT id
			FROM monitors
			WHERE enabled = true
				AND next_check_at <= $2::timestamptz
				AND (
					pending = false
					OR (
						$4::boolean
						AND pending = true
						AND (
							(pending_since IS NULL AND updated_at < $3::timestamptz)
							OR (pending_since IS NOT NULL AND pending_since < $3::timestamptz)
						)
					)
				)
			ORDER BY next_check_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE monitors m
		SET pending = true,
			pending_since = NULL,
			pending_job_id = NULL,
			updated_at = $2::timestamptz
		FROM due
		WHERE m.id = due.id
		RETURNING m.id, m.url, m.interval_seconds, m.timeout_seconds, m.expected_status,
			m.status, m.enabled, m.next_check_at, m.created_at, m.updated_at,
			m.last_status_code, m.last_latency_ms, m.last_checked_at, m.last_error
	`, limit, now.UTC(), leaseCutoff, canReclaim)
	if err != nil {
		return nil, err
	}
	monitors, err := scanMonitors(rows)
	if err != nil {
		return nil, err
	}
	for _, monitor := range monitors {
		if _, err := tx.Exec(ctx, `
			UPDATE monitors
			SET pending_job_id = $2
			WHERE id = $1
		`, monitor.ID, NewCheckJobID(monitor.ID, monitor.NextCheckAt)); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return monitors, nil
}

func (r *PostgresMonitorRepository) MarkProcessing(ctx context.Context, id, jobID string, now time.Time, leaseTimeout time.Duration) error {
	leaseCutoff := now.UTC()
	canReclaim := leaseTimeout > 0
	if leaseTimeout > 0 {
		leaseCutoff = now.UTC().Add(-leaseTimeout)
	}

	tag, err := r.pool.Exec(ctx, `
		UPDATE monitors
		SET pending = true,
			pending_since = $3::timestamptz,
			updated_at = $3::timestamptz
		WHERE id = $1
			AND pending = true
			AND pending_job_id = $2
			AND (
				pending_since IS NULL
				OR ($5::boolean AND pending_since < $4::timestamptz)
			)
	`, id, jobID, now.UTC(), leaseCutoff, canReclaim)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var (
		pending      bool
		pendingJobID sql.NullString
		pendingSince sql.NullTime
	)
	err = r.pool.QueryRow(ctx, `
		SELECT pending, pending_job_id, pending_since
		FROM monitors
		WHERE id = $1
	`, id).Scan(&pending, &pendingJobID, &pendingSince)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrMonitorNotFound
	}
	if err != nil {
		return err
	}
	if !pending || !pendingJobID.Valid || pendingJobID.String != jobID {
		return ErrStaleJob
	}
	return ErrJobAlreadyProcessing
}

func (r *PostgresMonitorRepository) MarkQueued(ctx context.Context, id, jobID string, now time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE monitors
		SET pending = true,
			pending_since = NULL,
			pending_job_id = $2,
			updated_at = $3::timestamptz
		WHERE id = $1
			AND pending = true
			AND pending_job_id = $2
	`, id, jobID, now.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM monitors WHERE id = $1)", id).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrMonitorNotFound
	}
	return ErrStaleJob
}

func (r *PostgresMonitorRepository) FailProcessing(ctx context.Context, id, jobID string, now, nextCheckAt time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE monitors
		SET pending = false,
			pending_since = NULL,
			pending_job_id = NULL,
			next_check_at = $3::timestamptz,
			updated_at = $4::timestamptz
		WHERE id = $1
			AND pending = true
			AND pending_job_id = $2
	`, id, jobID, nextCheckAt.UTC(), now.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := r.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM monitors WHERE id = $1)", id).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrMonitorNotFound
	}
	return ErrStaleJob
}

func (r *PostgresMonitorRepository) AddCheck(ctx context.Context, record CheckRecord, alertPolicy AlertPolicy) (Monitor, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Monitor{}, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO check_results (
			id, job_id, monitor_id, status_code, latency_ms, error, success, checked_at
		)
		VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8)
		ON CONFLICT (job_id) DO NOTHING
	`, record.ID, record.JobID, record.MonitorID, record.StatusCode, record.LatencyMS,
		record.Error, record.Success, record.CheckedAt.UTC())
	if err != nil {
		return Monitor{}, err
	}
	if tag.RowsAffected() == 0 {
		if err := completePendingJob(ctx, tx, record.MonitorID, record.JobID); err != nil {
			return Monitor{}, err
		}
		return Monitor{}, ErrDuplicateJob
	}

	monitor, err := scanMonitor(tx.QueryRow(ctx, `
		UPDATE monitors
		SET last_status_code = $2,
			last_latency_ms = $3,
			last_checked_at = $4::timestamptz,
			last_error = $5,
			next_check_at = $4::timestamptz + (interval_seconds * interval '1 second'),
			updated_at = $6::timestamptz,
			pending = false,
			pending_since = NULL,
			pending_job_id = NULL
		WHERE id = $1
			AND (NULLIF($7, '') IS NULL OR pending_job_id = $7)
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, record.MonitorID, record.StatusCode, record.LatencyMS, record.CheckedAt.UTC(), record.Error, time.Now().UTC(), record.JobID))
	if errors.Is(err, pgx.ErrNoRows) {
		var monitorExists bool
		if existsErr := tx.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM monitors WHERE id = $1)", record.MonitorID).Scan(&monitorExists); existsErr != nil {
			return Monitor{}, existsErr
		}
		if !monitorExists {
			return Monitor{}, ErrMonitorNotFound
		}
		return Monitor{}, ErrStaleJob
	}
	if err != nil {
		return Monitor{}, err
	}

	if err := upsertIncidentAndAlert(ctx, tx, record, monitor, alertPolicy); err != nil {
		return Monitor{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Monitor{}, err
	}
	return monitor, nil
}

func (r *PostgresMonitorRepository) AddManualCheck(ctx context.Context, record CheckRecord, alertPolicy AlertPolicy) (Monitor, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Monitor{}, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO check_results (
			id, job_id, monitor_id, status_code, latency_ms, error, success, checked_at
		)
		VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8)
		ON CONFLICT (job_id) DO NOTHING
	`, record.ID, record.JobID, record.MonitorID, record.StatusCode, record.LatencyMS,
		record.Error, record.Success, record.CheckedAt.UTC())
	if err != nil {
		return Monitor{}, err
	}
	if tag.RowsAffected() == 0 {
		return Monitor{}, ErrDuplicateJob
	}

	monitor, err := scanMonitor(tx.QueryRow(ctx, `
		UPDATE monitors
		SET last_status_code = $2,
			last_latency_ms = $3,
			last_checked_at = $4::timestamptz,
			last_error = $5,
			updated_at = $6::timestamptz
		WHERE id = $1
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, record.MonitorID, record.StatusCode, record.LatencyMS, record.CheckedAt.UTC(), record.Error, time.Now().UTC()))
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	if err != nil {
		return Monitor{}, err
	}

	if err := upsertIncidentAndAlert(ctx, tx, record, monitor, alertPolicy); err != nil {
		return Monitor{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Monitor{}, err
	}
	return monitor, nil
}

func (r *PostgresMonitorRepository) CompleteWithoutRecord(ctx context.Context, id string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE monitors
		SET pending = false,
			pending_since = NULL,
			pending_job_id = NULL
		WHERE id = $1
	`, id)
	return err
}

func completePendingJob(ctx context.Context, tx pgx.Tx, monitorID, jobID string) error {
	if jobID == "" {
		_, err := tx.Exec(ctx, `
			UPDATE monitors
			SET pending = false,
				pending_since = NULL,
				pending_job_id = NULL
			WHERE id = $1
		`, monitorID)
		return err
	}
	_, err := tx.Exec(ctx, `
		UPDATE monitors
		SET pending = false,
			pending_since = NULL,
			pending_job_id = NULL
		WHERE id = $1
			AND pending_job_id = $2
	`, monitorID, jobID)
	return err
}

func (r *PostgresMonitorRepository) ListChecks(ctx context.Context, id string, offset, limit int) ([]CheckRecord, int, error) {
	if _, err := r.Get(ctx, id); err != nil {
		return nil, 0, err
	}

	var total int
	if err := r.pool.QueryRow(ctx, "SELECT count(*) FROM check_results WHERE monitor_id = $1", id).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, COALESCE(job_id, ''), monitor_id, status_code, latency_ms, error, success, checked_at
		FROM check_results
		WHERE monitor_id = $1
		ORDER BY checked_at DESC, id DESC
		OFFSET $2 LIMIT $3
	`, id, offset, limit)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	records, err := scanCheckRecords(rows)
	return records, total, err
}

func (r *PostgresMonitorRepository) Stats(ctx context.Context, id string) (MonitorStats, error) {
	monitor, err := r.Get(ctx, id)
	if err != nil {
		return MonitorStats{}, err
	}

	stats := MonitorStats{
		MonitorID:      id,
		LastCheckedAt:  monitor.LastCheckedAt,
		LastStatusCode: monitor.LastStatusCode,
	}
	err = r.pool.QueryRow(ctx, `
		SELECT count(*),
			count(*) FILTER (WHERE success = true),
			count(*) FILTER (WHERE success = false),
			COALESCE(avg(latency_ms), 0)
		FROM check_results
		WHERE monitor_id = $1
	`, id).Scan(&stats.ChecksTotal, &stats.SuccessfulChecks, &stats.FailedChecks, &stats.AverageLatencyMS)
	if err != nil {
		return MonitorStats{}, err
	}
	if stats.ChecksTotal > 0 {
		stats.UptimePercent = float64(stats.SuccessfulChecks) / float64(stats.ChecksTotal) * 100
	}

	rows, err := r.pool.Query(ctx, `
		SELECT success
		FROM check_results
		WHERE monitor_id = $1
		ORDER BY checked_at DESC, id DESC
		LIMIT 500
	`, id)
	if err != nil {
		return MonitorStats{}, err
	}
	defer rows.Close()

	for rows.Next() {
		var success bool
		if err := rows.Scan(&success); err != nil {
			return MonitorStats{}, err
		}
		if success {
			break
		}
		stats.ConsecutiveFailure++
	}
	return stats, rows.Err()
}

func (r *PostgresMonitorRepository) ListIncidents(ctx context.Context, status string, offset, limit int) ([]Incident, int, error) {
	var (
		rows  pgx.Rows
		err   error
		total int
	)
	if status == "" {
		if err := r.pool.QueryRow(ctx, "SELECT count(*) FROM incidents").Scan(&total); err != nil {
			return nil, 0, err
		}
		rows, err = r.pool.Query(ctx, `
			SELECT id, monitor_id, status, failure_count, first_failure_at, last_failure_at,
				resolved_at, last_error, last_alerted_at, alert_count, created_at, updated_at
			FROM incidents
			ORDER BY created_at DESC, id DESC
			OFFSET $1 LIMIT $2
		`, offset, limit)
	} else {
		if err := r.pool.QueryRow(ctx, "SELECT count(*) FROM incidents WHERE status = $1", status).Scan(&total); err != nil {
			return nil, 0, err
		}
		rows, err = r.pool.Query(ctx, `
			SELECT id, monitor_id, status, failure_count, first_failure_at, last_failure_at,
				resolved_at, last_error, last_alerted_at, alert_count, created_at, updated_at
			FROM incidents
			WHERE status = $1
			ORDER BY created_at DESC, id DESC
			OFFSET $2 LIMIT $3
		`, status, offset, limit)
	}
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	incidents, err := scanIncidents(rows)
	return incidents, total, err
}

func (r *PostgresMonitorRepository) ClaimAlerts(ctx context.Context, limit int, now time.Time, leaseTimeout time.Duration) ([]AlertOutboxEvent, error) {
	leaseToken := newID("lease")
	lockedUntil := now.UTC().Add(leaseTimeout)
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM alert_outbox
			WHERE (status = 'pending' AND available_at <= $2::timestamptz)
				OR (status = 'processing' AND locked_until <= $2::timestamptz)
			ORDER BY available_at, created_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE alert_outbox AS outbox
		SET status = 'processing',
			attempt_count = outbox.attempt_count + 1,
			lease_token = $3,
			locked_until = $4::timestamptz,
			updated_at = $2::timestamptz
		FROM candidates
		WHERE outbox.id = candidates.id
		RETURNING outbox.id, outbox.idempotency_key, outbox.incident_id,
			outbox.monitor_id, outbox.payload, outbox.attempt_count, outbox.lease_token
	`, limit, now.UTC(), leaseToken, lockedUntil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]AlertOutboxEvent, 0, limit)
	for rows.Next() {
		var (
			event       AlertOutboxEvent
			payloadJSON []byte
		)
		if err := rows.Scan(
			&event.ID,
			&event.IdempotencyKey,
			&event.IncidentID,
			&event.MonitorID,
			&payloadJSON,
			&event.AttemptCount,
			&event.LeaseToken,
		); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payloadJSON, &event.Payload); err != nil {
			return nil, fmt.Errorf("decode alert outbox payload %s: %w", event.ID, err)
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *PostgresMonitorRepository) MarkAlertDelivered(ctx context.Context, id, leaseToken string, deliveredAt time.Time) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET status = 'delivered',
			delivered_at = $3::timestamptz,
			lease_token = NULL,
			locked_until = NULL,
			last_error = '',
			updated_at = $3::timestamptz
		WHERE id = $1
			AND status = 'processing'
			AND lease_token = $2
	`, id, leaseToken, deliveredAt.UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleAlertLease
	}
	return nil
}

func (r *PostgresMonitorRepository) MarkAlertFailed(ctx context.Context, id, leaseToken, lastError string, availableAt time.Time, dead bool) error {
	status := "pending"
	if dead {
		status = "dead"
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET status = $3,
			available_at = $4::timestamptz,
			lease_token = NULL,
			locked_until = NULL,
			last_error = $5,
			updated_at = now()
		WHERE id = $1
			AND status = 'processing'
			AND lease_token = $2
	`, id, leaseToken, status, availableAt.UTC(), truncateAlertError(lastError))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleAlertLease
	}
	return nil
}

func truncateAlertError(value string) string {
	const maxAlertErrorBytes = 4096
	if len(value) <= maxAlertErrorBytes {
		return value
	}
	return value[:maxAlertErrorBytes]
}

type pgxScanner interface {
	Scan(dest ...any) error
}

func scanMonitor(row pgxScanner) (Monitor, error) {
	var (
		monitor    Monitor
		statusCode sql.NullInt64
		latencyMS  sql.NullInt64
		checkedAt  sql.NullTime
		lastError  string
	)
	err := row.Scan(
		&monitor.ID,
		&monitor.URL,
		&monitor.IntervalSeconds,
		&monitor.TimeoutSeconds,
		&monitor.ExpectedStatus,
		&monitor.Status,
		&monitor.Enabled,
		&monitor.NextCheckAt,
		&monitor.CreatedAt,
		&monitor.UpdatedAt,
		&statusCode,
		&latencyMS,
		&checkedAt,
		&lastError,
	)
	if err != nil {
		return Monitor{}, err
	}
	if statusCode.Valid {
		monitor.LastStatusCode = int(statusCode.Int64)
	}
	if latencyMS.Valid {
		monitor.LastLatencyMS = latencyMS.Int64
	}
	if checkedAt.Valid {
		monitor.LastCheckedAt = checkedAt.Time
	}
	monitor.LastError = lastError
	return monitor, nil
}

func scanMonitors(rows pgx.Rows) ([]Monitor, error) {
	defer rows.Close()
	var monitors []Monitor
	for rows.Next() {
		monitor, err := scanMonitor(rows)
		if err != nil {
			return nil, err
		}
		monitors = append(monitors, monitor)
	}
	return monitors, rows.Err()
}

func scanCheckRecords(rows pgx.Rows) ([]CheckRecord, error) {
	var records []CheckRecord
	for rows.Next() {
		var record CheckRecord
		if err := rows.Scan(
			&record.ID,
			&record.JobID,
			&record.MonitorID,
			&record.StatusCode,
			&record.LatencyMS,
			&record.Error,
			&record.Success,
			&record.CheckedAt,
		); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func scanIncidents(rows pgx.Rows) ([]Incident, error) {
	var incidents []Incident
	for rows.Next() {
		incident, err := scanIncident(rows)
		if err != nil {
			return nil, err
		}
		incidents = append(incidents, incident)
	}
	return incidents, rows.Err()
}

func scanIncident(row pgxScanner) (Incident, error) {
	var (
		incident    Incident
		resolvedAt  sql.NullTime
		lastAlerted sql.NullTime
	)
	err := row.Scan(
		&incident.ID,
		&incident.MonitorID,
		&incident.Status,
		&incident.FailureCount,
		&incident.FirstFailureAt,
		&incident.LastFailureAt,
		&resolvedAt,
		&incident.LastError,
		&lastAlerted,
		&incident.AlertCount,
		&incident.CreatedAt,
		&incident.UpdatedAt,
	)
	if err != nil {
		return Incident{}, err
	}
	if resolvedAt.Valid {
		incident.ResolvedAt = resolvedAt.Time
	}
	if lastAlerted.Valid {
		incident.LastAlertedAt = lastAlerted.Time
	}
	return incident, nil
}

func upsertIncidentAndAlert(ctx context.Context, tx pgx.Tx, record CheckRecord, monitor Monitor, alertPolicy AlertPolicy) error {
	now := time.Now().UTC()
	if record.Success {
		_, err := tx.Exec(ctx, `
		UPDATE incidents
		SET status = $2,
			resolved_at = $3::timestamptz,
			updated_at = $4::timestamptz
			WHERE monitor_id = $1
				AND status = $5
		`, record.MonitorID, incidentStatusResolved, record.CheckedAt.UTC(), now, incidentStatusOpen)
		return err
	}

	lastError := record.Error
	if lastError == "" {
		lastError = fmt.Sprintf("unexpected status code %d", record.StatusCode)
	}

	var (
		incidentID   string
		failureCount int
		lastAlerted  sql.NullTime
	)
	err := tx.QueryRow(ctx, `
		INSERT INTO incidents (
			id, monitor_id, status, failure_count, first_failure_at, last_failure_at,
			last_error, created_at, updated_at
		)
		VALUES ($1, $2, $3, 1, $4::timestamptz, $4::timestamptz, $5, $6::timestamptz, $6::timestamptz)
		ON CONFLICT (monitor_id) WHERE status = 'open'
		DO UPDATE SET
			failure_count = incidents.failure_count + 1,
			last_failure_at = EXCLUDED.last_failure_at,
			last_error = EXCLUDED.last_error,
			updated_at = EXCLUDED.updated_at
		RETURNING id, failure_count, last_alerted_at
	`, newID("inc"), record.MonitorID, incidentStatusOpen, record.CheckedAt.UTC(), lastError, now).Scan(
		&incidentID,
		&failureCount,
		&lastAlerted,
	)
	if err != nil {
		return err
	}
	if !alertPolicy.Enabled || failureCount < alertPolicy.FailureThreshold {
		return nil
	}
	if lastAlerted.Valid && now.Sub(lastAlerted.Time) < alertPolicy.Cooldown {
		return nil
	}

	payload := AlertPayload{
		EventType:           alertEventIncidentFailure,
		IncidentID:          incidentID,
		MonitorID:           record.MonitorID,
		URL:                 monitor.URL,
		StatusCode:          record.StatusCode,
		Error:               lastError,
		ConsecutiveFailures: failureCount,
		CheckedAt:           record.CheckedAt.UTC(),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal alert outbox payload: %w", err)
	}
	idempotencyKey := incidentID + ":failure:" + strconv.Itoa(failureCount)
	tag, err := tx.Exec(ctx, `
		INSERT INTO alert_outbox (
			id, idempotency_key, incident_id, monitor_id, event_type, payload,
			status, available_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, 'pending', $7::timestamptz, $7::timestamptz, $7::timestamptz)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, newID("alert"), idempotencyKey, incidentID, record.MonitorID, alertEventIncidentFailure, payloadJSON, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE incidents
		SET last_alerted_at = $2::timestamptz,
			alert_count = alert_count + 1,
			updated_at = $2::timestamptz
		WHERE id = $1
	`, incidentID, now)
	return err
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
