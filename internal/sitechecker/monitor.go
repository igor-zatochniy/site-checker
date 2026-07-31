package sitechecker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

var (
	ErrMonitorNotFound      = errors.New("monitor not found")
	ErrMonitorExists        = errors.New("monitor already exists")
	ErrInvalidMonitor       = errors.New("invalid monitor")
	ErrJobIDRequired        = errors.New("check job id is required")
	ErrDuplicateJob         = errors.New("check job already processed")
	ErrStaleJob             = errors.New("check job is no longer active")
	ErrJobAlreadyProcessing = errors.New("check job is already processing")
)

const (
	monitorStatusActive      = "active"
	monitorStatusDisabled    = "disabled"
	incidentStatusOpen       = "open"
	incidentStatusResolved   = "resolved"
	checkJobStatusScheduled  = "scheduled"
	checkJobStatusQueued     = "queued"
	checkJobStatusProcessing = "processing"
	checkJobStatusCompleted  = "completed"
	checkJobStatusFailed     = "failed"
	checkJobStatusDead       = "dead"
	checkJobKindScheduled    = "scheduled"
	checkJobKindManual       = "manual"
	minMonitorTimeoutSeconds = 1
	maxMonitorTimeoutSeconds = 60
	maxChecksPerMonitor      = 500
	maxProcessedJobIDs       = 10_000
	maxRetainedCheckJobs     = 10_000
)

type Monitor struct {
	ID              string    `json:"id"`
	URL             string    `json:"url"`
	IntervalSeconds int       `json:"interval_seconds"`
	TimeoutSeconds  int       `json:"timeout_seconds"`
	ExpectedStatus  int       `json:"expected_status"`
	Status          string    `json:"status"`
	Enabled         bool      `json:"enabled"`
	NextCheckAt     time.Time `json:"next_check_at"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	LastStatusCode  int       `json:"last_status_code,omitempty"`
	LastLatencyMS   int64     `json:"last_latency_ms,omitempty"`
	LastCheckedAt   time.Time `json:"last_checked_at,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
}

type CheckRecord struct {
	ID         string    `json:"id"`
	JobID      string    `json:"job_id,omitempty"`
	MonitorID  string    `json:"monitor_id"`
	StatusCode int       `json:"status_code"`
	LatencyMS  int64     `json:"latency_ms"`
	Error      string    `json:"error,omitempty"`
	Success    bool      `json:"success"`
	CheckedAt  time.Time `json:"checked_at"`
}

type CheckJobRecord struct {
	ID                  string
	MonitorID           string
	Kind                string
	ScheduledFor        time.Time
	Status              string
	Attempt             int
	AvailableAt         time.Time
	PublishedAt         time.Time
	ProcessingStartedAt time.Time
	LeaseExpiresAt      time.Time
	LeaseToken          string
	CompletedAt         time.Time
	LastError           string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type ProcessingLease struct {
	JobID      string
	MonitorID  string
	Attempt    int
	LeaseToken string
}

type ManualCheckJobReceipt struct {
	JobID        string    `json:"job_id"`
	MonitorID    string    `json:"monitor_id"`
	Kind         string    `json:"kind"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	Deduplicated bool      `json:"deduplicated"`
}

type Incident struct {
	ID             string    `json:"id"`
	MonitorID      string    `json:"monitor_id"`
	Status         string    `json:"status"`
	FailureCount   int       `json:"failure_count"`
	FirstFailureAt time.Time `json:"first_failure_at"`
	LastFailureAt  time.Time `json:"last_failure_at"`
	ResolvedAt     time.Time `json:"resolved_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	LastAlertedAt  time.Time `json:"last_alerted_at,omitempty"`
	AlertCount     int       `json:"alert_count"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type MonitorStats struct {
	MonitorID          string    `json:"monitor_id"`
	UptimePercent      float64   `json:"uptime_percent"`
	AverageLatencyMS   float64   `json:"average_latency_ms"`
	ChecksTotal        int       `json:"checks_total"`
	SuccessfulChecks   int       `json:"successful_checks"`
	FailedChecks       int       `json:"failed_checks"`
	LastCheckedAt      time.Time `json:"last_checked_at,omitempty"`
	LastStatusCode     int       `json:"last_status_code,omitempty"`
	ConsecutiveFailure int       `json:"consecutive_failures"`
}

type MonitorInput struct {
	URL             string `json:"url"`
	IntervalSeconds int    `json:"interval_seconds"`
	TimeoutSeconds  int    `json:"timeout_seconds"`
	ExpectedStatus  int    `json:"expected_status"`
	Enabled         *bool  `json:"enabled,omitempty"`
}

type MonitorPatch struct {
	URL             *string `json:"url,omitempty"`
	IntervalSeconds *int    `json:"interval_seconds,omitempty"`
	TimeoutSeconds  *int    `json:"timeout_seconds,omitempty"`
	ExpectedStatus  *int    `json:"expected_status,omitempty"`
	Enabled         *bool   `json:"enabled,omitempty"`
}

type MonitorStore struct {
	mu                    sync.RWMutex
	policy                *NetworkPolicy
	byID                  map[string]Monitor
	byURL                 map[string]string
	checks                map[string][]CheckRecord
	checkJobs             map[string]CheckJobRecord
	activeJobByMonitor    map[string]string
	terminalJobOrder      []string
	terminalJobHead       int
	incidents             map[string]Incident
	openIncidentByMonitor map[string]string
	processedJobs         map[string]struct{}
	processedJobOrder     []string
	processedJobHead      int
}

func NewMonitorStore(policy *NetworkPolicy) *MonitorStore {
	return &MonitorStore{
		policy:                policy,
		byID:                  make(map[string]Monitor),
		byURL:                 make(map[string]string),
		checks:                make(map[string][]CheckRecord),
		checkJobs:             make(map[string]CheckJobRecord),
		activeJobByMonitor:    make(map[string]string),
		terminalJobOrder:      make([]string, 0, maxRetainedCheckJobs),
		incidents:             make(map[string]Incident),
		openIncidentByMonitor: make(map[string]string),
		processedJobs:         make(map[string]struct{}),
		processedJobOrder:     make([]string, 0, maxProcessedJobIDs),
	}
}

func SeedMonitorStore(links []string, cfg Config, policy *NetworkPolicy) (*MonitorStore, error) {
	store := NewMonitorStore(policy)
	for _, link := range links {
		_, err := store.Create(MonitorInput{
			URL:             link,
			IntervalSeconds: int(cfg.CheckInterval.Seconds()),
			TimeoutSeconds:  int(cfg.HTTPTimeout.Seconds()),
			ExpectedStatus:  200,
		})
		if err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *MonitorStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byID)
}

func (s *MonitorStore) Create(input MonitorInput) (Monitor, error) {
	if err := validateMonitorInput(input, s.policy); err != nil {
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

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byURL[monitor.URL]; exists {
		return Monitor{}, ErrMonitorExists
	}

	s.byID[monitor.ID] = monitor
	s.byURL[monitor.URL] = monitor.ID
	return monitor, nil
}

func (s *MonitorStore) List(offset, limit int) ([]Monitor, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	monitors := make([]Monitor, 0, len(s.byID))
	for _, monitor := range s.byID {
		monitors = append(monitors, monitor)
	}
	slices.SortFunc(monitors, func(a, b Monitor) int {
		if a.CreatedAt.Equal(b.CreatedAt) {
			return compareString(a.ID, b.ID)
		}
		if a.CreatedAt.Before(b.CreatedAt) {
			return -1
		}
		return 1
	})

	total := len(monitors)
	if offset > total {
		return []Monitor{}, total
	}
	end := min(offset+limit, total)
	return append([]Monitor(nil), monitors[offset:end]...), total
}

func (s *MonitorStore) Get(id string) (Monitor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	monitor, exists := s.byID[id]
	if !exists {
		return Monitor{}, ErrMonitorNotFound
	}
	return monitor, nil
}

func (s *MonitorStore) Update(id string, patch MonitorPatch) (Monitor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	monitor, exists := s.byID[id]
	if !exists {
		return Monitor{}, ErrMonitorNotFound
	}

	updated := monitor
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
	}, s.policy); err != nil {
		return Monitor{}, err
	}

	if existingID, exists := s.byURL[updated.URL]; exists && existingID != id {
		return Monitor{}, ErrMonitorExists
	}
	if updated.URL != monitor.URL {
		delete(s.byURL, monitor.URL)
		s.byURL[updated.URL] = id
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
		s.finishActiveJobLocked(id, checkJobStatusDead, "monitor disabled", now)
	}

	s.byID[id] = updated
	return updated, nil
}

func (s *MonitorStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	monitor, exists := s.byID[id]
	if !exists {
		return ErrMonitorNotFound
	}
	delete(s.byID, id)
	delete(s.byURL, monitor.URL)
	delete(s.checks, id)
	if jobID, exists := s.activeJobByMonitor[id]; exists {
		delete(s.checkJobs, jobID)
		delete(s.activeJobByMonitor, id)
	}
	for jobID, job := range s.checkJobs {
		if job.MonitorID == id {
			delete(s.checkJobs, jobID)
		}
	}
	delete(s.openIncidentByMonitor, id)
	for incidentID, incident := range s.incidents {
		if incident.MonitorID == id {
			delete(s.incidents, incidentID)
		}
	}
	return nil
}

func (s *MonitorStore) ClaimDueJobs(limit int, now time.Time, leaseTimeout time.Duration) []CheckJobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	now = now.UTC()
	if limit <= 0 {
		return nil
	}
	if leaseTimeout <= 0 {
		leaseTimeout = time.Minute
	}

	for jobID, job := range s.checkJobs {
		switch {
		case job.Status == checkJobStatusProcessing && !job.LeaseExpiresAt.IsZero() && !job.LeaseExpiresAt.After(now):
			job.LastError = "processing lease expired"
		case job.Status == checkJobStatusQueued && !job.PublishedAt.IsZero() && !job.PublishedAt.Add(leaseTimeout).After(now):
			job.LastError = "queued delivery lease expired"
		default:
			continue
		}
		job.Status = checkJobStatusFailed
		job.AvailableAt = now
		job.ProcessingStartedAt = time.Time{}
		job.LeaseExpiresAt = time.Time{}
		job.LeaseToken = ""
		job.UpdatedAt = now
		s.checkJobs[jobID] = job
	}

	candidates := make([]CheckJobRecord, 0, len(s.checkJobs))
	for _, job := range s.checkJobs {
		if job.Status != checkJobStatusScheduled && job.Status != checkJobStatusFailed {
			continue
		}
		if job.AvailableAt.After(now) {
			continue
		}
		if !job.LeaseExpiresAt.IsZero() && job.LeaseExpiresAt.After(now) {
			continue
		}
		candidates = append(candidates, job)
	}
	slices.SortFunc(candidates, func(left, right CheckJobRecord) int {
		if order := left.AvailableAt.Compare(right.AvailableAt); order != 0 {
			return order
		}
		if order := left.ScheduledFor.Compare(right.ScheduledFor); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})

	claimed := make([]CheckJobRecord, 0, limit)
	for _, job := range candidates {
		if len(claimed) >= limit {
			break
		}
		job.Status = checkJobStatusScheduled
		job.LeaseExpiresAt = now.Add(leaseTimeout)
		job.UpdatedAt = now
		s.checkJobs[job.ID] = job
		claimed = append(claimed, job)
	}
	if len(claimed) >= limit {
		return claimed
	}

	monitors := make([]Monitor, 0, len(s.byID))
	for _, monitor := range s.byID {
		monitors = append(monitors, monitor)
	}
	slices.SortFunc(monitors, func(left, right Monitor) int {
		if order := left.NextCheckAt.Compare(right.NextCheckAt); order != 0 {
			return order
		}
		return strings.Compare(left.ID, right.ID)
	})

	for _, monitor := range monitors {
		if len(claimed) >= limit {
			break
		}
		if !monitor.Enabled || monitor.NextCheckAt.After(now) {
			continue
		}
		if _, exists := s.activeJobByMonitor[monitor.ID]; exists {
			continue
		}

		job := CheckJobRecord{
			ID:             NewCheckJobID(monitor.ID, monitor.NextCheckAt),
			MonitorID:      monitor.ID,
			Kind:           checkJobKindScheduled,
			ScheduledFor:   monitor.NextCheckAt.UTC(),
			Status:         checkJobStatusScheduled,
			AvailableAt:    now,
			LeaseExpiresAt: now.Add(leaseTimeout),
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		s.checkJobs[job.ID] = job
		s.activeJobByMonitor[monitor.ID] = job.ID
		claimed = append(claimed, job)
	}
	return claimed
}

func (s *MonitorStore) CreateManualJob(id string, now time.Time) (CheckJobRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.byID[id]; !exists {
		return CheckJobRecord{}, false, ErrMonitorNotFound
	}
	if jobID, exists := s.activeJobByMonitor[id]; exists {
		job, jobExists := s.checkJobs[jobID]
		if jobExists {
			return job, false, nil
		}
		delete(s.activeJobByMonitor, id)
	}

	now = now.UTC()
	job := CheckJobRecord{
		ID:           newID("job"),
		MonitorID:    id,
		Kind:         checkJobKindManual,
		ScheduledFor: now,
		Status:       checkJobStatusScheduled,
		AvailableAt:  now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	s.checkJobs[job.ID] = job
	s.activeJobByMonitor[id] = job.ID
	return job, true, nil
}

func (s *MonitorStore) MarkJobPublished(id, jobID string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, err := s.activeJobLocked(id, jobID)
	if err != nil {
		if existing, exists := s.checkJobs[jobID]; exists && existing.MonitorID == id {
			return nil
		}
		return err
	}
	if job.Status != checkJobStatusScheduled {
		return nil
	}
	now = now.UTC()
	job.Status = checkJobStatusQueued
	job.PublishedAt = now
	job.LeaseExpiresAt = time.Time{}
	job.UpdatedAt = now
	s.checkJobs[jobID] = job
	return nil
}

func (s *MonitorStore) ReleaseJobPublish(id, jobID, lastError string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, err := s.activeJobLocked(id, jobID)
	if err != nil {
		return err
	}
	if job.Status != checkJobStatusScheduled {
		return nil
	}
	job.LeaseExpiresAt = time.Time{}
	job.LastError = lastError
	job.UpdatedAt = now.UTC()
	s.checkJobs[jobID] = job
	return nil
}

func (s *MonitorStore) MarkJobProcessing(id, jobID string, attempt int, now time.Time, leaseTimeout time.Duration) (ProcessingLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, err := s.activeJobLocked(id, jobID)
	if err != nil {
		return ProcessingLease{}, err
	}
	now = now.UTC()
	switch job.Status {
	case checkJobStatusScheduled, checkJobStatusQueued:
		if attempt <= job.Attempt {
			return ProcessingLease{}, ErrStaleJob
		}
	case checkJobStatusProcessing:
		if job.LeaseExpiresAt.IsZero() || job.LeaseExpiresAt.After(now) {
			return ProcessingLease{}, ErrJobAlreadyProcessing
		}
		if attempt < job.Attempt {
			return ProcessingLease{}, ErrStaleJob
		}
	default:
		return ProcessingLease{}, ErrStaleJob
	}
	if leaseTimeout <= 0 {
		leaseTimeout = time.Minute
	}
	job.Status = checkJobStatusProcessing
	job.Attempt = max(job.Attempt, max(attempt, 1))
	job.ProcessingStartedAt = now
	job.LeaseExpiresAt = now.Add(leaseTimeout)
	job.LeaseToken = newID("lease")
	job.UpdatedAt = now
	s.checkJobs[jobID] = job
	return ProcessingLease{JobID: jobID, MonitorID: id, Attempt: job.Attempt, LeaseToken: job.LeaseToken}, nil
}

func (s *MonitorStore) MarkJobFailed(lease ProcessingLease, lastError string, now, retryAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, err := s.activeJobLocked(lease.MonitorID, lease.JobID)
	if err != nil {
		return err
	}
	if !jobOwnsLease(job, lease) {
		return ErrStaleJob
	}
	retryAt = retryAt.UTC()
	now = now.UTC()
	job.Status = checkJobStatusFailed
	job.AvailableAt = retryAt
	job.ProcessingStartedAt = time.Time{}
	job.LeaseExpiresAt = time.Time{}
	job.LeaseToken = ""
	job.LastError = lastError
	job.UpdatedAt = now
	s.checkJobs[lease.JobID] = job
	return nil
}

func (s *MonitorStore) MarkJobDead(lease ProcessingLease, lastError string, now, nextCheckAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	monitor, exists := s.byID[lease.MonitorID]
	if !exists {
		return ErrMonitorNotFound
	}

	job, err := s.activeJobLocked(lease.MonitorID, lease.JobID)
	if err != nil {
		return err
	}
	if !jobOwnsLease(job, lease) {
		return ErrStaleJob
	}

	now = now.UTC()
	job.Status = checkJobStatusDead
	job.CompletedAt = now
	job.ProcessingStartedAt = time.Time{}
	job.LeaseExpiresAt = time.Time{}
	job.LeaseToken = ""
	job.LastError = lastError
	job.UpdatedAt = now
	s.checkJobs[lease.JobID] = job
	delete(s.activeJobByMonitor, lease.MonitorID)
	s.rememberTerminalJobLocked(lease.JobID)
	if job.Kind == checkJobKindScheduled {
		monitor.NextCheckAt = nextCheckAt.UTC()
		monitor.UpdatedAt = now
		s.byID[lease.MonitorID] = monitor
	}
	return nil
}

func jobOwnsLease(job CheckJobRecord, lease ProcessingLease) bool {
	return job.Status == checkJobStatusProcessing &&
		job.ID == lease.JobID &&
		job.MonitorID == lease.MonitorID &&
		job.Attempt == lease.Attempt &&
		job.LeaseToken != "" &&
		job.LeaseToken == lease.LeaseToken
}

func (s *MonitorStore) CheckJob(jobID string) (CheckJobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.checkJobs[jobID]
	if !exists {
		return CheckJobRecord{}, ErrStaleJob
	}
	return job, nil
}

func (s *MonitorStore) activeJobLocked(monitorID, jobID string) (CheckJobRecord, error) {
	if _, exists := s.byID[monitorID]; !exists {
		return CheckJobRecord{}, ErrMonitorNotFound
	}
	activeJobID, exists := s.activeJobByMonitor[monitorID]
	if !exists || activeJobID != jobID {
		return CheckJobRecord{}, ErrStaleJob
	}
	job, exists := s.checkJobs[jobID]
	if !exists || job.MonitorID != monitorID {
		return CheckJobRecord{}, ErrStaleJob
	}
	return job, nil
}

func (s *MonitorStore) finishActiveJobLocked(monitorID, status, lastError string, now time.Time) {
	jobID, exists := s.activeJobByMonitor[monitorID]
	if !exists {
		return
	}
	job, exists := s.checkJobs[jobID]
	if !exists {
		delete(s.activeJobByMonitor, monitorID)
		return
	}
	now = now.UTC()
	job.Status = status
	job.LastError = lastError
	job.ProcessingStartedAt = time.Time{}
	job.LeaseExpiresAt = time.Time{}
	job.LeaseToken = ""
	job.UpdatedAt = now
	if status == checkJobStatusCompleted || status == checkJobStatusDead {
		job.CompletedAt = now
		delete(s.activeJobByMonitor, monitorID)
		s.checkJobs[jobID] = job
		s.rememberTerminalJobLocked(jobID)
		return
	} else {
		job.AvailableAt = now
	}
	s.checkJobs[jobID] = job
}

func (s *MonitorStore) AddCheck(record CheckRecord, lease ProcessingLease) (Monitor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	monitor, exists := s.byID[record.MonitorID]
	if !exists {
		return Monitor{}, ErrMonitorNotFound
	}
	if record.JobID == "" {
		return Monitor{}, ErrJobIDRequired
	}
	job, exists := s.checkJobs[record.JobID]
	if !exists || job.MonitorID != record.MonitorID {
		return Monitor{}, ErrStaleJob
	}
	if job.Status == checkJobStatusCompleted {
		return monitor, ErrDuplicateJob
	}
	if !jobOwnsLease(job, lease) {
		return Monitor{}, ErrStaleJob
	}
	if _, exists := s.processedJobs[record.JobID]; exists {
		return monitor, ErrDuplicateJob
	}
	s.rememberProcessedJobLocked(record.JobID)

	now := time.Now().UTC()
	job.Status = checkJobStatusCompleted
	job.CompletedAt = now
	job.ProcessingStartedAt = time.Time{}
	job.LeaseExpiresAt = time.Time{}
	job.LeaseToken = ""
	job.LastError = ""
	job.UpdatedAt = now
	s.checkJobs[record.JobID] = job
	delete(s.activeJobByMonitor, record.MonitorID)
	s.rememberTerminalJobLocked(record.JobID)
	monitor.LastStatusCode = record.StatusCode
	monitor.LastLatencyMS = record.LatencyMS
	monitor.LastCheckedAt = record.CheckedAt
	monitor.LastError = record.Error
	if job.Kind == checkJobKindScheduled {
		monitor.NextCheckAt = now.Add(time.Duration(monitor.IntervalSeconds) * time.Second)
	}
	monitor.UpdatedAt = now
	s.byID[record.MonitorID] = monitor

	records := append(s.checks[record.MonitorID], record)
	if len(records) > maxChecksPerMonitor {
		records = records[len(records)-maxChecksPerMonitor:]
	}
	s.checks[record.MonitorID] = records
	s.updateIncident(record, now)
	return monitor, nil
}

func (s *MonitorStore) rememberProcessedJobLocked(jobID string) {
	if _, exists := s.processedJobs[jobID]; exists {
		return
	}
	s.processedJobs[jobID] = struct{}{}
	s.processedJobOrder = append(s.processedJobOrder, jobID)

	for len(s.processedJobs) > maxProcessedJobIDs {
		oldest := s.processedJobOrder[s.processedJobHead]
		delete(s.processedJobs, oldest)
		s.processedJobHead++
	}
	if s.processedJobHead > maxProcessedJobIDs && s.processedJobHead*2 >= len(s.processedJobOrder) {
		s.processedJobOrder = append([]string(nil), s.processedJobOrder[s.processedJobHead:]...)
		s.processedJobHead = 0
	}
}

func (s *MonitorStore) rememberTerminalJobLocked(jobID string) {
	s.terminalJobOrder = append(s.terminalJobOrder, jobID)
	for len(s.terminalJobOrder)-s.terminalJobHead > maxRetainedCheckJobs {
		oldest := s.terminalJobOrder[s.terminalJobHead]
		s.terminalJobHead++
		job, exists := s.checkJobs[oldest]
		if exists && (job.Status == checkJobStatusCompleted || job.Status == checkJobStatusDead) {
			delete(s.checkJobs, oldest)
		}
	}
	if s.terminalJobHead > maxRetainedCheckJobs && s.terminalJobHead*2 >= len(s.terminalJobOrder) {
		s.terminalJobOrder = append([]string(nil), s.terminalJobOrder[s.terminalJobHead:]...)
		s.terminalJobHead = 0
	}
}

func (s *MonitorStore) updateIncident(record CheckRecord, now time.Time) {
	if record.Success {
		incidentID, exists := s.openIncidentByMonitor[record.MonitorID]
		if !exists {
			return
		}
		incident := s.incidents[incidentID]
		incident.Status = incidentStatusResolved
		incident.ResolvedAt = record.CheckedAt
		incident.UpdatedAt = now
		s.incidents[incidentID] = incident
		delete(s.openIncidentByMonitor, record.MonitorID)
		return
	}

	lastError := record.Error
	if lastError == "" {
		lastError = fmt.Sprintf("unexpected status code %d", record.StatusCode)
	}

	if incidentID, exists := s.openIncidentByMonitor[record.MonitorID]; exists {
		incident := s.incidents[incidentID]
		incident.FailureCount++
		incident.LastFailureAt = record.CheckedAt
		incident.LastError = lastError
		incident.UpdatedAt = now
		s.incidents[incidentID] = incident
		return
	}

	incident := Incident{
		ID:             newID("inc"),
		MonitorID:      record.MonitorID,
		Status:         incidentStatusOpen,
		FailureCount:   1,
		FirstFailureAt: record.CheckedAt,
		LastFailureAt:  record.CheckedAt,
		LastError:      lastError,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	s.incidents[incident.ID] = incident
	s.openIncidentByMonitor[record.MonitorID] = incident.ID
}

func (s *MonitorStore) ListChecks(id string, offset, limit int) ([]CheckRecord, int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, exists := s.byID[id]; !exists {
		return nil, 0, ErrMonitorNotFound
	}
	records := append([]CheckRecord(nil), s.checks[id]...)
	slices.SortFunc(records, func(a, b CheckRecord) int {
		if a.CheckedAt.Equal(b.CheckedAt) {
			return compareString(a.ID, b.ID)
		}
		if a.CheckedAt.After(b.CheckedAt) {
			return -1
		}
		return 1
	})

	total := len(records)
	if offset > total {
		return []CheckRecord{}, total, nil
	}
	end := min(offset+limit, total)
	return records[offset:end], total, nil
}

func (s *MonitorStore) ListIncidents(status string, offset, limit int) ([]Incident, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	incidents := make([]Incident, 0, len(s.incidents))
	for _, incident := range s.incidents {
		if status != "" && incident.Status != status {
			continue
		}
		incidents = append(incidents, incident)
	}
	slices.SortFunc(incidents, func(a, b Incident) int {
		if a.CreatedAt.Equal(b.CreatedAt) {
			return compareString(a.ID, b.ID)
		}
		if a.CreatedAt.After(b.CreatedAt) {
			return -1
		}
		return 1
	})

	total := len(incidents)
	if offset > total {
		return []Incident{}, total
	}
	end := min(offset+limit, total)
	return append([]Incident(nil), incidents[offset:end]...), total
}

func (s *MonitorStore) Stats(id string) (MonitorStats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	monitor, exists := s.byID[id]
	if !exists {
		return MonitorStats{}, ErrMonitorNotFound
	}

	records := s.checks[id]
	stats := MonitorStats{
		MonitorID:      id,
		ChecksTotal:    len(records),
		LastCheckedAt:  monitor.LastCheckedAt,
		LastStatusCode: monitor.LastStatusCode,
	}
	if len(records) == 0 {
		return stats, nil
	}

	var latencyTotal int64
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		if record.Success {
			stats.SuccessfulChecks++
		} else {
			stats.FailedChecks++
			if stats.ConsecutiveFailure == len(records)-1-i {
				stats.ConsecutiveFailure++
			}
		}
		latencyTotal += record.LatencyMS
	}
	stats.UptimePercent = float64(stats.SuccessfulChecks) / float64(len(records)) * 100
	stats.AverageLatencyMS = float64(latencyTotal) / float64(len(records))
	return stats, nil
}

func validateMonitorInput(input MonitorInput, policy *NetworkPolicy) error {
	if input.URL == "" {
		return fmt.Errorf("%w: url is required", ErrInvalidMonitor)
	}
	if _, err := url.ParseRequestURI(input.URL); err != nil {
		return fmt.Errorf("%w: url is invalid", ErrInvalidMonitor)
	}
	if err := policy.ValidateURL(input.URL); err != nil {
		return fmt.Errorf("%w: url is not allowed: %v", ErrInvalidMonitor, err)
	}
	if input.IntervalSeconds < 30 || input.IntervalSeconds > 86400 {
		return fmt.Errorf("%w: interval_seconds must be between 30 and 86400", ErrInvalidMonitor)
	}
	if input.TimeoutSeconds < minMonitorTimeoutSeconds || input.TimeoutSeconds > maxMonitorTimeoutSeconds {
		return fmt.Errorf("%w: timeout_seconds must be between 1 and 60", ErrInvalidMonitor)
	}
	if input.ExpectedStatus < 100 || input.ExpectedStatus > 599 {
		return fmt.Errorf("%w: expected_status must be between 100 and 599", ErrInvalidMonitor)
	}
	return nil
}

func newMonitorID() string {
	return newID("mon")
}

func newID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(bytes[:])
}

func compareString(a, b string) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
