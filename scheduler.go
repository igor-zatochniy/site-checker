package main

import (
	"context"
	"log/slog"
	"time"
)

type Scheduler struct {
	links        []string
	jobs         chan<- CheckJob
	results      <-chan CheckResult
	interval     time.Duration
	metrics      *Metrics
	logger       *slog.Logger
	onResult     func(CheckResult)
	pending      map[string]struct{}
	nextDue      map[string]time.Time
	cursor       int
	nextSequence uint64
}

func NewScheduler(
	links []string,
	jobs chan<- CheckJob,
	results <-chan CheckResult,
	interval time.Duration,
	metrics *Metrics,
	logger *slog.Logger,
	onResult func(CheckResult),
) *Scheduler {
	nextDue := make(map[string]time.Time, len(links))
	now := time.Now()
	for _, link := range links {
		nextDue[link] = now
	}

	return &Scheduler{
		links:    links,
		jobs:     jobs,
		results:  results,
		interval: interval,
		metrics:  metrics,
		logger:   logger,
		onResult: onResult,
		pending:  make(map[string]struct{}, len(links)),
		nextDue:  nextDue,
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.tickInterval())
	defer ticker.Stop()

	s.enqueueDue(time.Now())
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Scheduler stopped", "pending", len(s.pending))
			return
		case result := <-s.results:
			delete(s.pending, result.URL)
			s.nextDue[result.URL] = time.Now().Add(s.interval)
			if s.onResult != nil {
				s.onResult(result)
			}
			s.enqueueDue(time.Now())
		case now := <-ticker.C:
			s.enqueueDue(now)
		}
	}
}

func (s *Scheduler) tickInterval() time.Duration {
	if s.interval < 30*time.Second {
		return s.interval
	}
	return 30 * time.Second
}

func (s *Scheduler) enqueueDue(now time.Time) {
	if len(s.links) == 0 {
		return
	}

	for attempts := 0; attempts < len(s.links); attempts++ {
		link := s.links[s.cursor]
		s.cursor = (s.cursor + 1) % len(s.links)

		if _, exists := s.pending[link]; exists {
			continue
		}
		if dueAt, exists := s.nextDue[link]; exists && dueAt.After(now) {
			continue
		}

		job := CheckJob{
			URL:        link,
			Sequence:   s.nextSequence,
			EnqueuedAt: now,
		}

		select {
		case s.jobs <- job:
			s.pending[link] = struct{}{}
			s.nextSequence++
			s.metrics.RecordScheduled()
		default:
			s.metrics.RecordSkipped()
			return
		}
	}
}
