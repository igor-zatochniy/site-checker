package sitechecker

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type MonitorService struct {
	repo        MonitorRepository
	checker     *Checker
	metrics     *Metrics
	alertPolicy AlertPolicy
	logger      *slog.Logger
}

func NewMonitorService(repo MonitorRepository, checker *Checker, metrics *Metrics, alertPolicy AlertPolicy, logger *slog.Logger) *MonitorService {
	return &MonitorService{
		repo:        repo,
		checker:     checker,
		metrics:     metrics,
		alertPolicy: alertPolicy,
		logger:      logger,
	}
}

func (s *MonitorService) Count(ctx context.Context) (int, error) {
	return s.repo.Count(ctx)
}

func (s *MonitorService) Create(ctx context.Context, input MonitorInput) (Monitor, error) {
	monitor, err := s.repo.Create(ctx, input)
	if err != nil {
		return Monitor{}, err
	}
	s.updateTotalLinks(ctx)
	return monitor, nil
}

func (s *MonitorService) List(ctx context.Context, offset, limit int) ([]Monitor, int, error) {
	return s.repo.List(ctx, offset, limit)
}

func (s *MonitorService) Get(ctx context.Context, id string) (Monitor, error) {
	return s.repo.Get(ctx, id)
}

func (s *MonitorService) Update(ctx context.Context, id string, patch MonitorPatch) (Monitor, error) {
	return s.repo.Update(ctx, id, patch)
}

func (s *MonitorService) Delete(ctx context.Context, id string) error {
	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}
	s.updateTotalLinks(ctx)
	return nil
}

func (s *MonitorService) ListChecks(ctx context.Context, id string, offset, limit int) ([]CheckRecord, int, error) {
	return s.repo.ListChecks(ctx, id, offset, limit)
}

func (s *MonitorService) Stats(ctx context.Context, id string) (MonitorStats, error) {
	return s.repo.Stats(ctx, id)
}

func (s *MonitorService) ListIncidents(ctx context.Context, status string, offset, limit int) ([]Incident, int, error) {
	return s.repo.ListIncidents(ctx, status, offset, limit)
}

func (s *MonitorService) ClaimDueJobs(ctx context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]CheckJobRecord, error) {
	return s.repo.ClaimDueJobs(ctx, limit, leaseTimeout, maxAttempts)
}

func (s *MonitorService) QueueManualCheck(ctx context.Context, id string) (ManualCheckJobReceipt, error) {
	job, created, err := s.repo.CreateManualJob(ctx, id)
	if err != nil {
		return ManualCheckJobReceipt{}, err
	}
	return ManualCheckJobReceipt{
		JobID:        job.ID,
		MonitorID:    job.MonitorID,
		Kind:         job.Kind,
		Status:       job.Status,
		CreatedAt:    job.CreatedAt,
		Deduplicated: !created,
	}, nil
}

func (s *MonitorService) MarkJobPublished(ctx context.Context, id, jobID string) error {
	return s.repo.MarkJobPublished(ctx, id, jobID)
}

func (s *MonitorService) ReleaseJobPublish(ctx context.Context, id, jobID, lastError string) error {
	return s.repo.ReleaseJobPublish(ctx, id, jobID, lastError)
}

func (s *MonitorService) MarkProcessing(ctx context.Context, id, jobID string, attempt int, leaseTimeout time.Duration) (ProcessingLease, error) {
	return s.repo.MarkJobProcessing(ctx, id, jobID, attempt, leaseTimeout)
}

func (s *MonitorService) MarkJobFailed(ctx context.Context, lease ProcessingLease, lastError string, retryDelay time.Duration) error {
	return s.repo.MarkJobFailed(ctx, lease, lastError, retryDelay)
}

func (s *MonitorService) FailProcessing(ctx context.Context, lease ProcessingLease, lastError string) error {
	return s.repo.MarkJobDead(ctx, lease, lastError)
}

func (s *MonitorService) StoreCheckResult(ctx context.Context, record CheckRecord, result CheckResult, lease ProcessingLease) error {
	monitor, err := s.repo.AddCheck(ctx, record, lease, s.alertPolicy)
	if err != nil {
		if errors.Is(err, ErrDuplicateJob) {
			return nil
		}
		return err
	}

	s.metrics.RecordResult(result, monitor.LastCheckedAt)
	return nil
}

func (s *MonitorService) updateTotalLinks(ctx context.Context) {
	total, err := s.repo.Count(ctx)
	if err != nil {
		s.logger.Warn("Failed to update total monitor metric", "error", err)
		return
	}
	s.metrics.SetTotalLinks(total)
}
