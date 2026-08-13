package sitechecker

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
	CreateManualJob(ctx context.Context, id string) (CheckJobRecord, bool, error)
	ClaimDueJobs(ctx context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]CheckJobRecord, error)
	MarkJobPublished(ctx context.Context, id, jobID string) error
	ReleaseJobPublish(ctx context.Context, id, jobID, lastError string) error
	MarkJobProcessing(ctx context.Context, id, jobID string, attempt int, leaseTimeout time.Duration) (ProcessingLease, error)
	MarkJobFailed(ctx context.Context, lease ProcessingLease, lastError string, retryDelay time.Duration) error
	MarkJobDead(ctx context.Context, lease ProcessingLease, lastError string) error
	AddCheck(ctx context.Context, record CheckRecord, lease ProcessingLease, alertPolicy AlertPolicy) (Monitor, error)
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

func (r *InMemoryMonitorRepository) CreateManualJob(_ context.Context, id string) (CheckJobRecord, bool, error) {
	return r.store.CreateManualJob(id, time.Now().UTC())
}

func (r *InMemoryMonitorRepository) ClaimDueJobs(_ context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]CheckJobRecord, error) {
	return r.store.ClaimDueJobsWithMaxAttempts(limit, time.Now().UTC(), leaseTimeout, maxAttempts), nil
}

func (r *InMemoryMonitorRepository) MarkJobPublished(_ context.Context, id, jobID string) error {
	return r.store.MarkJobPublished(id, jobID, time.Now().UTC())
}

func (r *InMemoryMonitorRepository) ReleaseJobPublish(_ context.Context, id, jobID, lastError string) error {
	return r.store.ReleaseJobPublish(id, jobID, lastError, time.Now().UTC())
}

func (r *InMemoryMonitorRepository) MarkJobProcessing(_ context.Context, id, jobID string, attempt int, leaseTimeout time.Duration) (ProcessingLease, error) {
	return r.store.MarkJobProcessing(id, jobID, attempt, time.Now().UTC(), leaseTimeout)
}

func (r *InMemoryMonitorRepository) MarkJobFailed(_ context.Context, lease ProcessingLease, lastError string, retryDelay time.Duration) error {
	now := time.Now().UTC()
	return r.store.MarkJobFailed(lease, lastError, now, now.Add(max(retryDelay, 0)))
}

func (r *InMemoryMonitorRepository) MarkJobDead(_ context.Context, lease ProcessingLease, lastError string) error {
	monitor, err := r.store.Get(lease.MonitorID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	return r.store.MarkJobDead(lease, lastError, now, now.Add(time.Duration(monitor.IntervalSeconds)*time.Second))
}

func (r *InMemoryMonitorRepository) AddCheck(_ context.Context, record CheckRecord, lease ProcessingLease, _ AlertPolicy) (Monitor, error) {
	return r.store.AddCheck(record, lease)
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
			ExpectedStatus:  cfg.ExpectedStatus.PreferredStatus(),
		})
		if err != nil && !errors.Is(err, ErrMonitorExists) {
			return err
		}
	}
	return nil
}
