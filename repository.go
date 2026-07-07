package main

import (
	"context"
	"errors"
	"time"
)

type MonitorRepository interface {
	Ping(ctx context.Context) error
	Count(ctx context.Context) (int, error)
	Create(ctx context.Context, input MonitorInput) (Monitor, error)
	List(ctx context.Context, offset, limit int) ([]Monitor, int, error)
	Get(ctx context.Context, id string) (Monitor, error)
	Update(ctx context.Context, id string, patch MonitorPatch) (Monitor, error)
	Delete(ctx context.Context, id string) error
	ClaimDue(ctx context.Context, limit int, now time.Time, leaseTimeout time.Duration) ([]Monitor, error)
	MarkProcessing(ctx context.Context, id, jobID string, now time.Time, leaseTimeout time.Duration) error
	AddCheck(ctx context.Context, record CheckRecord, alertPolicy AlertPolicy) (Monitor, error)
	CompleteWithoutRecord(ctx context.Context, id string) error
	ListChecks(ctx context.Context, id string, offset, limit int) ([]CheckRecord, int, error)
	Stats(ctx context.Context, id string) (MonitorStats, error)
	ListIncidents(ctx context.Context, status string, offset, limit int) ([]Incident, int, error)
}

type InMemoryMonitorRepository struct {
	store *MonitorStore
}

func NewInMemoryMonitorRepository(policy *NetworkPolicy) *InMemoryMonitorRepository {
	return &InMemoryMonitorRepository{store: NewMonitorStore(policy)}
}

func NewInMemoryMonitorRepositoryFromStore(store *MonitorStore) *InMemoryMonitorRepository {
	return &InMemoryMonitorRepository{store: store}
}

func (r *InMemoryMonitorRepository) Count(_ context.Context) (int, error) {
	return r.store.Count(), nil
}

func (r *InMemoryMonitorRepository) Ping(context.Context) error {
	return nil
}

func (r *InMemoryMonitorRepository) Create(_ context.Context, input MonitorInput) (Monitor, error) {
	return r.store.Create(input)
}

func (r *InMemoryMonitorRepository) List(_ context.Context, offset, limit int) ([]Monitor, int, error) {
	monitors, total := r.store.List(offset, limit)
	return monitors, total, nil
}

func (r *InMemoryMonitorRepository) Get(_ context.Context, id string) (Monitor, error) {
	return r.store.Get(id)
}

func (r *InMemoryMonitorRepository) Update(_ context.Context, id string, patch MonitorPatch) (Monitor, error) {
	return r.store.Update(id, patch)
}

func (r *InMemoryMonitorRepository) Delete(_ context.Context, id string) error {
	return r.store.Delete(id)
}

func (r *InMemoryMonitorRepository) ClaimDue(_ context.Context, limit int, now time.Time, leaseTimeout time.Duration) ([]Monitor, error) {
	return r.store.ClaimDueWithLease(limit, now, leaseTimeout), nil
}

func (r *InMemoryMonitorRepository) MarkProcessing(_ context.Context, id, jobID string, now time.Time, leaseTimeout time.Duration) error {
	return r.store.MarkProcessing(id, jobID, now, leaseTimeout)
}

func (r *InMemoryMonitorRepository) AddCheck(_ context.Context, record CheckRecord, _ AlertPolicy) (Monitor, error) {
	return r.store.AddCheck(record)
}

func (r *InMemoryMonitorRepository) CompleteWithoutRecord(_ context.Context, id string) error {
	r.store.CompleteWithoutRecord(id)
	return nil
}

func (r *InMemoryMonitorRepository) ListChecks(_ context.Context, id string, offset, limit int) ([]CheckRecord, int, error) {
	return r.store.ListChecks(id, offset, limit)
}

func (r *InMemoryMonitorRepository) Stats(_ context.Context, id string) (MonitorStats, error) {
	return r.store.Stats(id)
}

func (r *InMemoryMonitorRepository) ListIncidents(_ context.Context, status string, offset, limit int) ([]Incident, int, error) {
	incidents, total := r.store.ListIncidents(status, offset, limit)
	return incidents, total, nil
}

func SeedRepository(ctx context.Context, repo MonitorRepository, links []string, cfg Config) error {
	for _, link := range links {
		_, err := repo.Create(ctx, MonitorInput{
			URL:             link,
			IntervalSeconds: int(cfg.CheckInterval.Seconds()),
			TimeoutSeconds:  int(cfg.HTTPTimeout.Seconds()),
			ExpectedStatus:  200,
		})
		if err != nil && !errors.Is(err, ErrMonitorExists) {
			return err
		}
	}
	return nil
}
