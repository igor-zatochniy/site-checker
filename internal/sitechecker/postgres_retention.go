package sitechecker

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (r *PostgresMonitorRepository) DeleteExpiredData(ctx context.Context, _ time.Time, policy RetentionPolicy) (RetentionResult, error) {
	if policy.BatchSize < 1 {
		return RetentionResult{}, fmt.Errorf("retention batch size must be positive")
	}
	if policy.CheckResults <= 0 || policy.CheckJobs <= 0 || policy.AlertOutbox <= 0 || policy.ResolvedIncident <= 0 {
		return RetentionResult{}, fmt.Errorf("retention durations must be positive")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return RetentionResult{}, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return RetentionResult{}, err
	}

	var result RetentionResult
	result.CheckResults, err = deleteRetentionBatch(ctx, tx, `
		WITH expired AS (
			SELECT id FROM check_results
			WHERE checked_at < $1
			ORDER BY checked_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM check_results AS target
		USING expired
		WHERE target.id = expired.id
	`, now.Add(-policy.CheckResults), policy.BatchSize)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete expired check results: %w", err)
	}

	result.CheckJobs, err = deleteRetentionBatch(ctx, tx, `
		WITH expired AS (
			SELECT id FROM check_jobs
			WHERE status IN ('completed', 'dead')
				AND updated_at < $1
			ORDER BY updated_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM check_jobs AS target
		USING expired
		WHERE target.id = expired.id
	`, now.Add(-policy.CheckJobs), policy.BatchSize)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete expired check jobs: %w", err)
	}

	result.AlertOutbox, err = deleteRetentionBatch(ctx, tx, `
		WITH expired AS (
			SELECT id FROM alert_outbox
			WHERE status IN ('delivered', 'dead')
				AND updated_at < $1
			ORDER BY updated_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM alert_outbox AS target
		USING expired
		WHERE target.id = expired.id
	`, now.Add(-policy.AlertOutbox), policy.BatchSize)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete expired alert outbox entries: %w", err)
	}

	result.ResolvedIncidents, err = deleteRetentionBatch(ctx, tx, `
		WITH expired AS (
			SELECT incident.id
			FROM incidents AS incident
			WHERE incident.status = 'resolved'
				AND incident.resolved_at < $1
				AND NOT EXISTS (
					SELECT 1 FROM alert_outbox AS outbox
					WHERE outbox.incident_id = incident.id
				)
			ORDER BY incident.resolved_at, incident.id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM incidents AS target
		USING expired
		WHERE target.id = expired.id
	`, now.Add(-policy.ResolvedIncident), policy.BatchSize)
	if err != nil {
		return RetentionResult{}, fmt.Errorf("delete expired resolved incidents: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return RetentionResult{}, err
	}
	return result, nil
}

func deleteRetentionBatch(ctx context.Context, tx pgx.Tx, query string, cutoff time.Time, batchSize int) (int64, error) {
	tag, err := tx.Exec(ctx, query, cutoff.UTC(), batchSize)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
