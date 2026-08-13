package sitechecker

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
	}

	row := r.pool.QueryRow(ctx, `
		WITH db_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS now
		)
		INSERT INTO monitors (
			id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at
		)
		SELECT $1, $2, $3, $4, $5, $6, $7, db_clock.now, db_clock.now, db_clock.now
		FROM db_clock
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, monitor.ID, monitor.URL, monitor.IntervalSeconds, monitor.TimeoutSeconds, monitor.ExpectedStatus,
		monitor.Status, monitor.Enabled)

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
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Monitor{}, err
	}
	defer tx.Rollback(ctx)

	current, err := scanMonitor(tx.QueryRow(ctx, `
		SELECT id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
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

	now, err := databaseTime(ctx, tx)
	if err != nil {
		return Monitor{}, err
	}
	checkSemanticsChanged := updated.URL != current.URL ||
		updated.TimeoutSeconds != current.TimeoutSeconds ||
		updated.ExpectedStatus != current.ExpectedStatus
	updated.UpdatedAt = now
	if updated.Enabled {
		updated.Status = monitorStatusActive
		if checkSemanticsChanged || updated.NextCheckAt.IsZero() || updated.NextCheckAt.Before(now) {
			updated.NextCheckAt = now
		}
	} else {
		updated.Status = monitorStatusDisabled
	}

	row := tx.QueryRow(ctx, `
		UPDATE monitors
		SET url = $2,
			interval_seconds = $3,
			timeout_seconds = $4,
			expected_status = $5,
			status = $6,
			enabled = $7,
			next_check_at = $8,
			updated_at = $9,
			last_status_code = CASE WHEN $10 THEN NULL ELSE last_status_code END,
			last_latency_ms = CASE WHEN $10 THEN NULL ELSE last_latency_ms END,
			last_checked_at = CASE WHEN $10 THEN NULL ELSE last_checked_at END,
			last_error = CASE WHEN $10 THEN '' ELSE last_error END
		WHERE id = $1
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, id, updated.URL, updated.IntervalSeconds, updated.TimeoutSeconds, updated.ExpectedStatus,
		updated.Status, updated.Enabled, updated.NextCheckAt, updated.UpdatedAt, checkSemanticsChanged)

	monitor, err := scanMonitor(row)
	if isUniqueViolation(err) {
		return Monitor{}, ErrMonitorExists
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	if err != nil {
		return Monitor{}, err
	}
	if !updated.Enabled || checkSemanticsChanged {
		jobError := "monitor configuration changed"
		if !updated.Enabled {
			jobError = "monitor disabled"
		}
		if _, err := tx.Exec(ctx, `
			UPDATE check_jobs
			SET status = 'dead',
				completed_at = $2::timestamptz,
				processing_started_at = NULL,
				lease_expires_at = NULL,
				lease_token = NULL,
				last_error = $3,
				updated_at = $2::timestamptz
			WHERE monitor_id = $1
				AND status IN ('scheduled', 'queued', 'processing', 'failed')
		`, id, now, jobError); err != nil {
			return Monitor{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Monitor{}, err
	}
	return monitor, nil
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

func (r *PostgresMonitorRepository) CreateManualJob(ctx context.Context, id string) (CheckJobRecord, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return CheckJobRecord{}, false, err
	}
	defer tx.Rollback(ctx)
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return CheckJobRecord{}, false, err
	}

	var monitorID string
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&monitorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return CheckJobRecord{}, false, ErrMonitorNotFound
	}
	if err != nil {
		return CheckJobRecord{}, false, err
	}

	existing, err := scanCheckJob(tx.QueryRow(ctx, `
		SELECT id, monitor_id, kind, scheduled_for, status, attempt,
			available_at, published_at, processing_started_at,
			lease_expires_at, lease_token, completed_at, last_error, created_at, updated_at
		FROM check_jobs
		WHERE monitor_id = $1
			AND status IN ('scheduled', 'queued', 'processing', 'failed')
		LIMIT 1
	`, id))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return CheckJobRecord{}, false, err
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return CheckJobRecord{}, false, err
	}

	jobID := newID("job")
	job, err := scanCheckJob(tx.QueryRow(ctx, `
		INSERT INTO check_jobs (
			id, monitor_id, kind, scheduled_for, status, attempt,
			available_at, created_at, updated_at
		)
		VALUES ($1, $2, 'manual', $3::timestamptz, 'scheduled', 0,
			$3::timestamptz, $3::timestamptz, $3::timestamptz)
		RETURNING id, monitor_id, kind, scheduled_for, status, attempt,
			available_at, published_at, processing_started_at,
			lease_expires_at, lease_token, completed_at, last_error, created_at, updated_at
	`, jobID, id, now))
	if err != nil {
		return CheckJobRecord{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CheckJobRecord{}, false, err
	}
	return job, true, nil
}

func (r *PostgresMonitorRepository) ClaimDueJobs(ctx context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]CheckJobRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	if leaseTimeout <= 0 {
		leaseTimeout = time.Minute
	}
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxJobAttempts
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	leaseExpiresAt := now.Add(leaseTimeout)

	if _, err := tx.Exec(ctx, `
		WITH exhausted AS (
			UPDATE check_jobs
			SET status = 'dead',
				processing_started_at = NULL,
				lease_expires_at = NULL,
				lease_token = NULL,
				completed_at = $1::timestamptz,
				last_error = CASE status
					WHEN 'processing' THEN 'processing lease expired; maximum attempts reached'
					WHEN 'queued' THEN 'queued delivery lease expired; maximum attempts reached'
					ELSE 'maximum processing attempts reached'
				END,
				updated_at = $1::timestamptz
			WHERE attempt >= $2
				AND (
					(status = 'processing' AND lease_expires_at <= $1::timestamptz)
					OR (status = 'queued' AND published_at <= $3::timestamptz)
					OR (status = 'failed' AND available_at <= $1::timestamptz)
					OR (status = 'scheduled' AND lease_expires_at <= $1::timestamptz)
				)
			RETURNING monitor_id, kind
		)
		UPDATE monitors AS monitor
		SET next_check_at = $1::timestamptz + (monitor.interval_seconds * interval '1 second'),
			updated_at = $1::timestamptz
		FROM exhausted
		WHERE monitor.id = exhausted.monitor_id
			AND exhausted.kind = 'scheduled'
	`, now, maxAttempts, now.Add(-leaseTimeout)); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE check_jobs
		SET status = 'failed',
			available_at = $1::timestamptz,
			processing_started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			last_error = 'processing lease expired',
			updated_at = $1::timestamptz
		WHERE status = 'processing'
			AND lease_expires_at <= $1::timestamptz
			AND attempt < $2
	`, now, maxAttempts); err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE check_jobs
		SET status = 'failed',
			available_at = $1::timestamptz,
			lease_expires_at = NULL,
			last_error = 'queued delivery lease expired',
			updated_at = $1::timestamptz
		WHERE status = 'queued'
			AND published_at <= $2::timestamptz
			AND attempt < $3
	`, now, now.Add(-leaseTimeout), maxAttempts); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, `
		WITH claimable AS (
			SELECT id
			FROM check_jobs
			WHERE status IN ('scheduled', 'failed')
				AND available_at <= $2::timestamptz
				AND (lease_expires_at IS NULL OR lease_expires_at <= $2::timestamptz)
				AND attempt < $4
			ORDER BY available_at, scheduled_for, id
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE check_jobs j
		SET status = 'scheduled',
			lease_expires_at = $3::timestamptz,
			updated_at = $2::timestamptz
		FROM claimable
		WHERE j.id = claimable.id
		RETURNING j.id, j.monitor_id, j.kind, j.scheduled_for, j.status, j.attempt,
			j.available_at, j.published_at, j.processing_started_at,
			j.lease_expires_at, j.lease_token, j.completed_at, j.last_error, j.created_at, j.updated_at
	`, limit, now, leaseExpiresAt, maxAttempts)
	if err != nil {
		return nil, err
	}
	jobs, err := scanCheckJobs(rows)
	if err != nil {
		return nil, err
	}
	rows.Close()

	remaining := limit - len(jobs)
	if remaining > 0 {
		monitorRows, err := tx.Query(ctx, `
			SELECT m.id, m.next_check_at
			FROM monitors m
			WHERE m.enabled = true
				AND m.next_check_at <= $2::timestamptz
				AND NOT EXISTS (
					SELECT 1
					FROM check_jobs j
					WHERE j.monitor_id = m.id
						AND j.status IN ('scheduled', 'queued', 'processing', 'failed')
				)
			ORDER BY m.next_check_at, m.id
			FOR UPDATE OF m SKIP LOCKED
			LIMIT $1
		`, remaining, now)
		if err != nil {
			return nil, err
		}
		type dueMonitor struct {
			id          string
			nextCheckAt time.Time
		}
		due := make([]dueMonitor, 0, remaining)
		for monitorRows.Next() {
			var monitor dueMonitor
			if err := monitorRows.Scan(&monitor.id, &monitor.nextCheckAt); err != nil {
				monitorRows.Close()
				return nil, err
			}
			due = append(due, monitor)
		}
		if err := monitorRows.Err(); err != nil {
			monitorRows.Close()
			return nil, err
		}
		monitorRows.Close()

		for _, monitor := range due {
			jobID := NewCheckJobID(monitor.id, monitor.nextCheckAt)
			job, err := scanCheckJob(tx.QueryRow(ctx, `
				INSERT INTO check_jobs (
					id, monitor_id, kind, scheduled_for, status, attempt, available_at,
					lease_expires_at, created_at, updated_at
				)
				VALUES ($1, $2, 'scheduled', $3::timestamptz, 'scheduled', 0, $4::timestamptz,
					$5::timestamptz, $4::timestamptz, $4::timestamptz)
				RETURNING id, monitor_id, kind, scheduled_for, status, attempt,
					available_at, published_at, processing_started_at,
					lease_expires_at, lease_token, completed_at, last_error, created_at, updated_at
			`, jobID, monitor.id, monitor.nextCheckAt.UTC(), now, leaseExpiresAt))
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, job)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (r *PostgresMonitorRepository) GetCheckJob(ctx context.Context, jobID string) (CheckJobRecord, error) {
	job, err := scanCheckJob(r.pool.QueryRow(ctx, `
		SELECT id, monitor_id, kind, scheduled_for, status, attempt,
			available_at, published_at, processing_started_at,
			lease_expires_at, lease_token, completed_at, last_error, created_at, updated_at
		FROM check_jobs
		WHERE id = $1
	`, jobID))
	if errors.Is(err, pgx.ErrNoRows) {
		return CheckJobRecord{}, ErrStaleJob
	}
	return job, err
}

func (r *PostgresMonitorRepository) MarkJobPublished(ctx context.Context, id, jobID string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE check_jobs
		SET status = 'queued',
			published_at = clock_timestamp(),
			lease_expires_at = NULL,
			updated_at = clock_timestamp()
		WHERE id = $2
			AND monitor_id = $1
			AND status = 'scheduled'
	`, id, jobID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return r.acceptAdvancedJobState(ctx, id, jobID)
}

func (r *PostgresMonitorRepository) ReleaseJobPublish(ctx context.Context, id, jobID, lastError string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE check_jobs
		SET lease_expires_at = NULL,
			last_error = $3,
			updated_at = clock_timestamp()
		WHERE id = $2
			AND monitor_id = $1
			AND status = 'scheduled'
	`, id, jobID, lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return r.acceptAdvancedJobState(ctx, id, jobID)
}

func (r *PostgresMonitorRepository) MarkJobProcessing(ctx context.Context, id, jobID string, attempt int, leaseTimeout time.Duration) (ProcessingLease, error) {
	if leaseTimeout <= 0 {
		leaseTimeout = time.Minute
	}
	leaseToken := newID("lease")
	attempt = max(attempt, 1)

	var persistedAttempt int
	err := r.pool.QueryRow(ctx, `
		WITH db_clock AS MATERIALIZED (
			SELECT clock_timestamp() AS now
		)
		UPDATE check_jobs
		SET status = 'processing',
			attempt = $3,
			processing_started_at = db_clock.now,
			lease_expires_at = db_clock.now + ($4::bigint * interval '1 microsecond'),
			lease_token = $5,
			updated_at = db_clock.now
		FROM db_clock
		WHERE id = $2
			AND monitor_id = $1
			AND (
				(status IN ('scheduled', 'queued') AND $3 > attempt)
				OR (
					status = 'processing'
					AND lease_expires_at <= db_clock.now
					AND $3 >= attempt
				)
			)
		RETURNING attempt
	`, id, jobID, attempt, leaseTimeout.Microseconds(), leaseToken).Scan(&persistedAttempt)
	if err == nil {
		return ProcessingLease{JobID: jobID, MonitorID: id, Attempt: persistedAttempt, LeaseToken: leaseToken}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ProcessingLease{}, err
	}

	status, err := r.checkJobStatus(ctx, id, jobID)
	if err != nil {
		return ProcessingLease{}, err
	}
	if status == checkJobStatusProcessing {
		return ProcessingLease{}, ErrJobAlreadyProcessing
	}
	return ProcessingLease{}, ErrStaleJob
}

func (r *PostgresMonitorRepository) MarkJobFailed(ctx context.Context, lease ProcessingLease, lastError string, retryDelay time.Duration) error {
	retryDelay = max(retryDelay, 0)
	tag, err := r.pool.Exec(ctx, `
		UPDATE check_jobs
		SET status = 'failed',
			available_at = clock_timestamp() + ($6::bigint * interval '1 microsecond'),
			processing_started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			last_error = $5,
			updated_at = clock_timestamp()
		WHERE id = $1
			AND monitor_id = $2
			AND status = 'processing'
			AND attempt = $3
			AND lease_token = $4
	`, lease.JobID, lease.MonitorID, lease.Attempt, lease.LeaseToken, lastError, retryDelay.Microseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return r.classifyInactiveJob(ctx, lease.MonitorID, lease.JobID)
}

func (r *PostgresMonitorRepository) MarkJobDead(ctx context.Context, lease ProcessingLease, lastError string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return err
	}

	var monitorID string
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`, lease.MonitorID).Scan(&monitorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrMonitorNotFound
	}
	if err != nil {
		return err
	}

	var kind string
	err = tx.QueryRow(ctx, `
		UPDATE check_jobs
		SET status = 'dead',
			processing_started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			completed_at = $6::timestamptz,
			last_error = $5,
			updated_at = $6::timestamptz
		WHERE id = $1
			AND monitor_id = $2
			AND status = 'processing'
			AND attempt = $3
			AND lease_token = $4
		RETURNING kind
	`, lease.JobID, lease.MonitorID, lease.Attempt, lease.LeaseToken, lastError, now.UTC()).Scan(&kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return classifyInactiveJobQuery(ctx, tx, lease.MonitorID, lease.JobID)
	}
	if err != nil {
		return err
	}
	if kind == checkJobKindScheduled {
		tag, err := tx.Exec(ctx, `
		UPDATE monitors
		SET next_check_at = $2::timestamptz + (interval_seconds * interval '1 second'),
			updated_at = $2::timestamptz
		WHERE id = $1
		`, lease.MonitorID, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrMonitorNotFound
		}
	}
	return tx.Commit(ctx)
}

func (r *PostgresMonitorRepository) AddCheck(ctx context.Context, record CheckRecord, lease ProcessingLease, alertPolicy AlertPolicy) (Monitor, error) {
	if record.JobID == "" {
		return Monitor{}, ErrJobIDRequired
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return Monitor{}, err
	}
	defer tx.Rollback(ctx)

	var monitorID string
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM monitors
		WHERE id = $1
		FOR UPDATE
	`, record.MonitorID).Scan(&monitorID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	if err != nil {
		return Monitor{}, err
	}

	var (
		status     string
		attempt    int
		leaseToken sql.NullString
		kind       string
	)
	err = tx.QueryRow(ctx, `
			SELECT status, attempt, lease_token, kind
			FROM check_jobs
			WHERE id = $1
				AND monitor_id = $2
			FOR UPDATE
	`, record.JobID, record.MonitorID).Scan(&status, &attempt, &leaseToken, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrStaleJob
	}
	if err != nil {
		return Monitor{}, err
	}
	if status == checkJobStatusCompleted {
		return Monitor{}, ErrDuplicateJob
	}
	if status != checkJobStatusProcessing {
		return Monitor{}, ErrStaleJob
	}
	if attempt != lease.Attempt || !leaseToken.Valid || leaseToken.String == "" || leaseToken.String != lease.LeaseToken ||
		lease.JobID != record.JobID || lease.MonitorID != record.MonitorID {
		return Monitor{}, ErrStaleJob
	}
	updatedAt, err := databaseTime(ctx, tx)
	if err != nil {
		return Monitor{}, err
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO check_results (
			id, job_id, monitor_id, status_code, latency_ms, error, success, checked_at, recorded_at
		)
		VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (job_id) DO NOTHING
	`, record.ID, record.JobID, record.MonitorID, record.StatusCode, record.LatencyMS,
		record.Error, record.Success, record.CheckedAt.UTC(), updatedAt)
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
			last_checked_at = $5::timestamptz,
			last_error = $4,
			next_check_at = CASE
				WHEN $6 = 'scheduled' THEN $5::timestamptz + (interval_seconds * interval '1 second')
				ELSE next_check_at
			END,
			updated_at = $5::timestamptz
		WHERE id = $1
		RETURNING id, url, interval_seconds, timeout_seconds, expected_status,
			status, enabled, next_check_at, created_at, updated_at,
			last_status_code, last_latency_ms, last_checked_at, last_error
	`, record.MonitorID, record.StatusCode, record.LatencyMS, record.Error, updatedAt, kind))
	if errors.Is(err, pgx.ErrNoRows) {
		return Monitor{}, ErrMonitorNotFound
	}
	if err != nil {
		return Monitor{}, err
	}

	tag, err = tx.Exec(ctx, `
			UPDATE check_jobs
			SET status = 'completed',
				processing_started_at = NULL,
				lease_expires_at = NULL,
				lease_token = NULL,
				completed_at = $5::timestamptz,
				last_error = '',
				updated_at = $5::timestamptz
			WHERE id = $1
				AND monitor_id = $2
				AND status = 'processing'
				AND attempt = $3
				AND lease_token = $4
		`, record.JobID, record.MonitorID, lease.Attempt, lease.LeaseToken, updatedAt)
	if err != nil {
		return Monitor{}, err
	}
	if tag.RowsAffected() != 1 {
		return Monitor{}, ErrStaleJob
	}

	if err := upsertIncidentAndAlert(ctx, tx, record, monitor, alertPolicy, updatedAt); err != nil {
		return Monitor{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Monitor{}, err
	}
	return monitor, nil
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
		ORDER BY recorded_at DESC, id DESC
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

	err = r.pool.QueryRow(ctx, `
		SELECT COALESCE((
			SELECT failure_count
			FROM incidents
			WHERE monitor_id = $1
				AND status = 'open'
			LIMIT 1
		), 0)
	`, id).Scan(&stats.ConsecutiveFailure)
	return stats, err
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

func (r *PostgresMonitorRepository) ClaimAlerts(ctx context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]AlertOutboxEvent, error) {
	if limit <= 0 {
		return nil, nil
	}
	if leaseTimeout <= 0 {
		leaseTimeout = defaultAlertLeaseTimeout
	}
	if maxAttempts <= 0 {
		maxAttempts = defaultAlertMaxAttempts
	}
	leaseToken := newID("lease")
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	lockedUntil := now.Add(leaseTimeout)

	if _, err := tx.Exec(ctx, `
		UPDATE alert_outbox
		SET status = 'dead',
			lease_token = NULL,
			locked_until = NULL,
			last_error = CASE
				WHEN last_error = '' THEN 'maximum delivery attempts reached after lease expiry'
				ELSE last_error
			END,
			updated_at = $1::timestamptz
		WHERE attempt_count >= $2
			AND (
				status = 'pending'
				OR (status = 'processing' AND locked_until <= $1::timestamptz)
			)
	`, now, maxAttempts); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, `
		WITH candidates AS (
			SELECT id
			FROM alert_outbox
			WHERE attempt_count < $5
				AND (
					(status = 'pending' AND available_at <= $2::timestamptz)
					OR (status = 'processing' AND locked_until <= $2::timestamptz)
				)
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
	`, limit, now, leaseToken, lockedUntil, maxAttempts)
	if err != nil {
		return nil, err
	}
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
			rows.Close()
			return nil, err
		}
		if err := json.Unmarshal(payloadJSON, &event.Payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode alert outbox payload %s: %w", event.ID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return events, nil
}

func (r *PostgresMonitorRepository) MarkAlertDelivered(ctx context.Context, id, leaseToken string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET status = 'delivered',
			delivered_at = clock_timestamp(),
			lease_token = NULL,
			locked_until = NULL,
			last_error = '',
			updated_at = clock_timestamp()
		WHERE id = $1
			AND status = 'processing'
			AND lease_token = $2
	`, id, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStaleAlertLease
	}
	return nil
}

func (r *PostgresMonitorRepository) MarkAlertFailed(ctx context.Context, id, leaseToken, lastError string, retryDelay time.Duration, dead bool) error {
	status := "pending"
	if dead {
		status = "dead"
	}
	retryDelay = max(retryDelay, 0)
	tag, err := r.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET status = $3,
			available_at = clock_timestamp() + ($4::bigint * interval '1 microsecond'),
			lease_token = NULL,
			locked_until = NULL,
			last_error = $5,
			updated_at = now()
		WHERE id = $1
			AND status = 'processing'
			AND lease_token = $2
	`, id, leaseToken, status, retryDelay.Microseconds(), truncateAlertError(lastError))
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

type pgxQueryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *PostgresMonitorRepository) checkJobStatus(ctx context.Context, monitorID, jobID string) (string, error) {
	var status string
	err := r.pool.QueryRow(ctx, `
		SELECT status
		FROM check_jobs
		WHERE id = $1
			AND monitor_id = $2
	`, jobID, monitorID).Scan(&status)
	if err == nil {
		return status, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	return "", classifyInactiveJobQuery(ctx, r.pool, monitorID, jobID)
}

func (r *PostgresMonitorRepository) acceptAdvancedJobState(ctx context.Context, monitorID, jobID string) error {
	_, err := r.checkJobStatus(ctx, monitorID, jobID)
	return err
}

func (r *PostgresMonitorRepository) classifyInactiveJob(ctx context.Context, monitorID, jobID string) error {
	return classifyInactiveJobQuery(ctx, r.pool, monitorID, jobID)
}

func classifyInactiveJobQuery(ctx context.Context, queryer pgxQueryRower, monitorID, jobID string) error {
	var (
		monitorExists bool
		jobExists     bool
	)
	err := queryer.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM monitors WHERE id = $1),
			EXISTS (SELECT 1 FROM check_jobs WHERE id = $2 AND monitor_id = $1)
	`, monitorID, jobID).Scan(&monitorExists, &jobExists)
	if err != nil {
		return err
	}
	if !monitorExists {
		return ErrMonitorNotFound
	}
	if !jobExists {
		return ErrStaleJob
	}
	return ErrStaleJob
}

type pgxScanner interface {
	Scan(dest ...any) error
}

func scanCheckJob(row pgxScanner) (CheckJobRecord, error) {
	var (
		job            CheckJobRecord
		publishedAt    sql.NullTime
		processingAt   sql.NullTime
		leaseExpiresAt sql.NullTime
		leaseToken     sql.NullString
		completedAt    sql.NullTime
	)
	err := row.Scan(
		&job.ID,
		&job.MonitorID,
		&job.Kind,
		&job.ScheduledFor,
		&job.Status,
		&job.Attempt,
		&job.AvailableAt,
		&publishedAt,
		&processingAt,
		&leaseExpiresAt,
		&leaseToken,
		&completedAt,
		&job.LastError,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return CheckJobRecord{}, err
	}
	if publishedAt.Valid {
		job.PublishedAt = publishedAt.Time
	}
	if processingAt.Valid {
		job.ProcessingStartedAt = processingAt.Time
	}
	if leaseExpiresAt.Valid {
		job.LeaseExpiresAt = leaseExpiresAt.Time
	}
	if leaseToken.Valid {
		job.LeaseToken = leaseToken.String
	}
	if completedAt.Valid {
		job.CompletedAt = completedAt.Time
	}
	return job, nil
}

func scanCheckJobs(rows pgx.Rows) ([]CheckJobRecord, error) {
	defer rows.Close()
	var jobs []CheckJobRecord
	for rows.Next() {
		job, err := scanCheckJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
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

func upsertIncidentAndAlert(ctx context.Context, tx pgx.Tx, record CheckRecord, monitor Monitor, alertPolicy AlertPolicy, now time.Time) error {
	if record.Success {
		_, err := tx.Exec(ctx, `
		UPDATE incidents
		SET status = $2,
			resolved_at = $3::timestamptz,
			updated_at = $3::timestamptz
			WHERE monitor_id = $1
				AND status = $4
		`, record.MonitorID, incidentStatusResolved, now, incidentStatusOpen)
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
	`, newID("inc"), record.MonitorID, incidentStatusOpen, now, lastError, now).Scan(
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

func databaseTime(ctx context.Context, querier pgxQueryRower) (time.Time, error) {
	var now time.Time
	if err := querier.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return time.Time{}, err
	}
	return now.UTC(), nil
}
