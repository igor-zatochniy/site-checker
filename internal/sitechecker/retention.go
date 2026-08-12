package sitechecker

import (
	"context"
	"log/slog"
	"time"
)

type RetentionPolicy struct {
	CheckResults     time.Duration
	CheckJobs        time.Duration
	AlertOutbox      time.Duration
	ResolvedIncident time.Duration
	BatchSize        int
}

type RetentionResult struct {
	CheckResults      int64
	CheckJobs         int64
	AlertOutbox       int64
	ResolvedIncidents int64
}

type RetentionRepository interface {
	DeleteExpiredData(ctx context.Context, now time.Time, policy RetentionPolicy) (RetentionResult, error)
}

func RunRetention(ctx context.Context, repo RetentionRepository, cfg Config, logger *slog.Logger) {
	policy := RetentionPolicy{
		CheckResults:     cfg.CheckResultsRetention,
		CheckJobs:        cfg.CheckJobsRetention,
		AlertOutbox:      cfg.AlertOutboxRetention,
		ResolvedIncident: cfg.ResolvedIncidentRetention,
		BatchSize:        cfg.RetentionBatchSize,
	}
	ticker := time.NewTicker(cfg.RetentionInterval)
	defer ticker.Stop()

	for {
		result, err := repo.DeleteExpiredData(ctx, time.Now().UTC(), policy)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Warn("Data retention pass failed", "error", err)
		} else if result.CheckResults+result.CheckJobs+result.AlertOutbox+result.ResolvedIncidents > 0 {
			logger.Info("Expired data deleted",
				"check_results", result.CheckResults,
				"check_jobs", result.CheckJobs,
				"alert_outbox", result.AlertOutbox,
				"resolved_incidents", result.ResolvedIncidents,
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
