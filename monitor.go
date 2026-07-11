package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sync"
	"time"
)

var (
	ErrMonitorNotFound = errors.New("monitor not found")
	ErrMonitorExists   = errors.New("monitor already exists")
	ErrDuplicateJob    = errors.New("check job already processed")
)

const (
	monitorStatusActive    = "active"
	monitorStatusDisabled  = "disabled"
	incidentStatusOpen     = "open"
	incidentStatusResolved = "resolved"
	maxChecksPerMonitor    = 500
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

type Incident struct {
	ID             string    `json:"id"`
	MonitorID      string    `json:"monitor_id"`
	Status         string    `json:"status"`
	FailureCount   int       `json:"failure_count"`
	FirstFailureAt time.Time `json:"first_failure_at"`
	LastFailureAt  time.Time `json:"last_failure_at"`
	ResolvedAt     time.Time `json:"resolved_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
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
	pending               map[string]time.Time
	incidents             map[string]Incident
	openIncidentByMonitor map[string]string
	processedJobs         map[string]struct{}
}

func NewMonitorStore(policy *NetworkPolicy) *MonitorStore {
	return &MonitorStore{
		policy:                policy,
		byID:                  make(map[string]Monitor),
		byURL:                 make(map[string]string),
		checks:                make(map[string][]CheckRecord),
		pending:               make(map[string]time.Time),
		incidents:             make(map[string]Incident),
		openIncidentByMonitor: make(map[string]string),
		processedJobs:         make(map[string]struct{}),
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
		delete(s.pending, id)
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
	delete(s.pending, id)
	delete(s.openIncidentByMonitor, id)
	for incidentID, incident := range s.incidents {
		if incident.MonitorID == id {
			delete(s.incidents, incidentID)
		}
	}
	return nil
}

func (s *MonitorStore) ClaimDue(limit int, now time.Time) []Monitor {
	return s.ClaimDueWithLease(limit, now, 0)
}

func (s *MonitorStore) ClaimDueWithLease(limit int, now time.Time, leaseTimeout time.Duration) []Monitor {
	s.mu.Lock()
	defer s.mu.Unlock()

	due := make([]Monitor, 0, limit)
	for id, monitor := range s.byID {
		if len(due) >= limit {
			break
		}
		if !monitor.Enabled || monitor.NextCheckAt.After(now) {
			continue
		}
		if pendingSince, exists := s.pending[id]; exists {
			if leaseTimeout <= 0 || now.Sub(pendingSince) < leaseTimeout {
				continue
			}
		}

		s.pending[id] = now
		due = append(due, monitor)
	}
	return due
}

func (s *MonitorStore) AddCheck(record CheckRecord) (Monitor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	monitor, exists := s.byID[record.MonitorID]
	if !exists {
		return Monitor{}, ErrMonitorNotFound
	}
	if record.JobID != "" {
		if _, exists := s.processedJobs[record.JobID]; exists {
			delete(s.pending, record.MonitorID)
			return monitor, ErrDuplicateJob
		}
		s.processedJobs[record.JobID] = struct{}{}
	}

	now := time.Now().UTC()
	delete(s.pending, record.MonitorID)
	monitor.LastStatusCode = record.StatusCode
	monitor.LastLatencyMS = record.LatencyMS
	monitor.LastCheckedAt = record.CheckedAt
	monitor.LastError = record.Error
	monitor.NextCheckAt = now.Add(time.Duration(monitor.IntervalSeconds) * time.Second)
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

func (s *MonitorStore) CompleteWithoutRecord(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, id)
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
		return fmt.Errorf("url is required")
	}
	if _, err := url.ParseRequestURI(input.URL); err != nil {
		return fmt.Errorf("url is invalid")
	}
	if err := policy.ValidateURL(input.URL); err != nil {
		return fmt.Errorf("url is not allowed: %w", err)
	}
	if input.IntervalSeconds < 30 || input.IntervalSeconds > 86400 {
		return fmt.Errorf("interval_seconds must be between 30 and 86400")
	}
	if input.TimeoutSeconds < 1 || input.TimeoutSeconds > 60 {
		return fmt.Errorf("timeout_seconds must be between 1 and 60")
	}
	if input.ExpectedStatus < 100 || input.ExpectedStatus > 599 {
		return fmt.Errorf("expected_status must be between 100 and 599")
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
