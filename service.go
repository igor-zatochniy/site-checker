package main

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type MonitorService struct {
	repo    MonitorRepository
	checker *Checker
	metrics *Metrics
	alerts  *AlertManager
	logger  *slog.Logger
}

func NewMonitorService(repo MonitorRepository, checker *Checker, metrics *Metrics, alerts *AlertManager, logger *slog.Logger) *MonitorService {
	return &MonitorService{
		repo:    repo,
		checker: checker,
		metrics: metrics,
		alerts:  alerts,
		logger:  logger,
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

func (s *MonitorService) ClaimDue(ctx context.Context, limit int, now time.Time, leaseTimeout time.Duration) ([]Monitor, error) {
	return s.repo.ClaimDue(ctx, limit, now, leaseTimeout)
}

func (s *MonitorService) CompleteWithoutRecord(ctx context.Context, id string) error {
	return s.repo.CompleteWithoutRecord(ctx, id)
}

func (s *MonitorService) RunManualCheck(ctx context.Context, id string) (CheckRecord, error) {
	monitor, err := s.repo.Get(ctx, id)
	if err != nil {
		return CheckRecord{}, err
	}

	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(monitor.TimeoutSeconds)*time.Second)
	defer cancel()

	result := s.checker.CheckMonitor(checkCtx, monitor)
	record := CheckRecordFromResult(result)
	record.JobID = newID("manual")
	if err := s.StoreCheckResult(ctx, record, result); err != nil {
		return CheckRecord{}, err
	}
	return record, nil
}

func (s *MonitorService) StoreCheckResult(ctx context.Context, record CheckRecord, result CheckResult) error {
	if _, err := s.repo.AddCheck(ctx, record); err != nil {
		if errors.Is(err, ErrDuplicateJob) {
			return nil
		}
		return err
	}

	s.metrics.RecordResult(result)
	if s.alerts != nil {
		s.alerts.Handle(ctx, result)
	}
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
