package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type Metrics struct {
	mu                   sync.RWMutex
	startedAt            time.Time
	version              string
	commit               string
	buildDate            string
	totalLinks           int
	checksTotal          uint64
	healthyTotal         uint64
	unhealthyTotal       uint64
	checkErrorsTotal     uint64
	jobsScheduledTotal   uint64
	jobsSkippedTotal     uint64
	alertsDeliveredTotal uint64
	alertFailuresTotal   uint64
	alertsDeadTotal      uint64
	durationSumSeconds   float64
	durationCount        uint64
	lastCheckAt          time.Time
	lastSuccessAt        time.Time
	statusByURL          map[string]int
	upByURL              map[string]bool
	consecutiveFailures  map[string]int
	dependencyUp         map[string]bool
}

type MetricsSnapshot struct {
	StartedAt            time.Time
	Version              string
	Commit               string
	BuildDate            string
	TotalLinks           int
	ChecksTotal          uint64
	HealthyTotal         uint64
	UnhealthyTotal       uint64
	CheckErrorsTotal     uint64
	JobsScheduledTotal   uint64
	JobsSkippedTotal     uint64
	AlertsDeliveredTotal uint64
	AlertFailuresTotal   uint64
	AlertsDeadTotal      uint64
	DurationSumSeconds   float64
	DurationCount        uint64
	LastCheckAt          time.Time
	LastSuccessAt        time.Time
	StatusByURL          map[string]int
	UpByURL              map[string]bool
	ConsecutiveFailures  map[string]int
	DependencyUp         map[string]bool
}

func NewMetrics(version, commit, buildDate string, totalLinks int) *Metrics {
	return &Metrics{
		startedAt:           time.Now(),
		version:             version,
		commit:              commit,
		buildDate:           buildDate,
		totalLinks:          totalLinks,
		statusByURL:         make(map[string]int, totalLinks),
		upByURL:             make(map[string]bool, totalLinks),
		consecutiveFailures: make(map[string]int, totalLinks),
		dependencyUp:        make(map[string]bool),
	}
}

func (m *Metrics) SetTotalLinks(totalLinks int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.totalLinks = totalLinks
}

func (m *Metrics) RecordScheduled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsScheduledTotal++
}

func (m *Metrics) RecordSkipped() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobsSkippedTotal++
}

func (m *Metrics) RecordAlertDelivered() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertsDeliveredTotal++
}

func (m *Metrics) RecordAlertFailure(dead bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alertFailuresTotal++
	if dead {
		m.alertsDeadTotal++
	}
}

func (m *Metrics) SetDependencyUp(name string, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dependencyUp[name] = up
}

func (m *Metrics) RecordResult(result CheckResult) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checksTotal++
	m.lastCheckAt = result.CheckedAt
	m.durationSumSeconds += result.Duration.Seconds()
	m.durationCount++
	m.statusByURL[result.URL] = result.StatusCode
	m.upByURL[result.URL] = result.Healthy

	if result.Healthy {
		m.healthyTotal++
		m.lastSuccessAt = result.CheckedAt
		m.consecutiveFailures[result.URL] = 0
		return
	}

	m.unhealthyTotal++
	m.consecutiveFailures[result.URL]++
	if result.Error != "" {
		m.checkErrorsTotal++
	}
}

func (m *Metrics) ConsecutiveFailures(url string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.consecutiveFailures[url]
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return MetricsSnapshot{
		StartedAt:            m.startedAt,
		Version:              m.version,
		Commit:               m.commit,
		BuildDate:            m.buildDate,
		TotalLinks:           m.totalLinks,
		ChecksTotal:          m.checksTotal,
		HealthyTotal:         m.healthyTotal,
		UnhealthyTotal:       m.unhealthyTotal,
		CheckErrorsTotal:     m.checkErrorsTotal,
		JobsScheduledTotal:   m.jobsScheduledTotal,
		JobsSkippedTotal:     m.jobsSkippedTotal,
		AlertsDeliveredTotal: m.alertsDeliveredTotal,
		AlertFailuresTotal:   m.alertFailuresTotal,
		AlertsDeadTotal:      m.alertsDeadTotal,
		DurationSumSeconds:   m.durationSumSeconds,
		DurationCount:        m.durationCount,
		LastCheckAt:          m.lastCheckAt,
		LastSuccessAt:        m.lastSuccessAt,
		StatusByURL:          cloneIntMap(m.statusByURL),
		UpByURL:              cloneBoolMap(m.upByURL),
		ConsecutiveFailures:  cloneIntMap(m.consecutiveFailures),
		DependencyUp:         cloneBoolMap(m.dependencyUp),
	}
}

func (m *Metrics) Prometheus() string {
	snapshot := m.Snapshot()
	var builder strings.Builder

	writeMetric := func(name, help, metricType string, value any) {
		builder.WriteString("# HELP ")
		builder.WriteString(name)
		builder.WriteByte(' ')
		builder.WriteString(help)
		builder.WriteByte('\n')
		builder.WriteString("# TYPE ")
		builder.WriteString(name)
		builder.WriteByte(' ')
		builder.WriteString(metricType)
		builder.WriteByte('\n')
		builder.WriteString(name)
		builder.WriteByte(' ')
		builder.WriteString(fmt.Sprint(value))
		builder.WriteByte('\n')
	}

	builder.WriteString(`# HELP site_checker_build_info Build metadata.` + "\n")
	builder.WriteString(`# TYPE site_checker_build_info gauge` + "\n")
	builder.WriteString(`site_checker_build_info{version="`)
	builder.WriteString(escapeLabelValue(snapshot.Version))
	builder.WriteString(`",commit="`)
	builder.WriteString(escapeLabelValue(snapshot.Commit))
	builder.WriteString(`",build_date="`)
	builder.WriteString(escapeLabelValue(snapshot.BuildDate))
	builder.WriteString(`"} 1` + "\n")

	writeMetric("site_checker_links_total", "Number of configured URLs.", "gauge", snapshot.TotalLinks)
	writeMetric("site_checker_checks_total", "Total completed checks.", "counter", snapshot.ChecksTotal)
	writeMetric("site_checker_checks_healthy_total", "Total checks matching the expected status policy.", "counter", snapshot.HealthyTotal)
	writeMetric("site_checker_checks_unhealthy_total", "Total checks outside the expected status policy.", "counter", snapshot.UnhealthyTotal)
	writeMetric("site_checker_check_errors_total", "Total checks that returned an error.", "counter", snapshot.CheckErrorsTotal)
	writeMetric("site_checker_jobs_scheduled_total", "Total jobs accepted by the scheduler.", "counter", snapshot.JobsScheduledTotal)
	writeMetric("site_checker_jobs_skipped_total", "Total scheduler attempts skipped because the queue was full.", "counter", snapshot.JobsSkippedTotal)
	writeMetric("site_checker_alerts_delivered_total", "Total webhook alerts delivered successfully.", "counter", snapshot.AlertsDeliveredTotal)
	writeMetric("site_checker_alert_delivery_failures_total", "Total failed webhook alert delivery attempts.", "counter", snapshot.AlertFailuresTotal)
	writeMetric("site_checker_alerts_dead_total", "Total webhook alerts exhausted after retry attempts.", "counter", snapshot.AlertsDeadTotal)
	writeMetric("site_checker_check_duration_seconds_sum", "Total check duration in seconds.", "counter", snapshot.DurationSumSeconds)
	writeMetric("site_checker_check_duration_seconds_count", "Number of observed check durations.", "counter", snapshot.DurationCount)
	writeMetric("site_checker_started_timestamp_seconds", "Unix timestamp when the process started.", "gauge", unixSeconds(snapshot.StartedAt))
	writeMetric("site_checker_last_check_timestamp_seconds", "Unix timestamp of the last completed check.", "gauge", unixSeconds(snapshot.LastCheckAt))
	writeMetric("site_checker_last_success_timestamp_seconds", "Unix timestamp of the last successful check.", "gauge", unixSeconds(snapshot.LastSuccessAt))

	urls := make([]string, 0, len(snapshot.UpByURL))
	for url := range snapshot.UpByURL {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	builder.WriteString("# HELP site_checker_site_up Last known site health by URL.\n")
	builder.WriteString("# TYPE site_checker_site_up gauge\n")
	builder.WriteString("# HELP site_checker_site_status_code Last observed HTTP status code by URL.\n")
	builder.WriteString("# TYPE site_checker_site_status_code gauge\n")
	builder.WriteString("# HELP site_checker_site_consecutive_failures Consecutive failed checks by URL.\n")
	builder.WriteString("# TYPE site_checker_site_consecutive_failures gauge\n")
	for _, url := range urls {
		upValue := 0
		if snapshot.UpByURL[url] {
			upValue = 1
		}
		label := `url="` + escapeLabelValue(url) + `"`
		builder.WriteString("site_checker_site_up{")
		builder.WriteString(label)
		builder.WriteString("} ")
		builder.WriteString(fmt.Sprint(upValue))
		builder.WriteByte('\n')
		builder.WriteString("site_checker_site_status_code{")
		builder.WriteString(label)
		builder.WriteString("} ")
		builder.WriteString(fmt.Sprint(snapshot.StatusByURL[url]))
		builder.WriteByte('\n')
		builder.WriteString("site_checker_site_consecutive_failures{")
		builder.WriteString(label)
		builder.WriteString("} ")
		builder.WriteString(fmt.Sprint(snapshot.ConsecutiveFailures[url]))
		builder.WriteByte('\n')
	}

	dependencies := make([]string, 0, len(snapshot.DependencyUp))
	for dependency := range snapshot.DependencyUp {
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	builder.WriteString("# HELP site_checker_dependency_up Last known dependency readiness by dependency name.\n")
	builder.WriteString("# TYPE site_checker_dependency_up gauge\n")
	for _, dependency := range dependencies {
		upValue := 0
		if snapshot.DependencyUp[dependency] {
			upValue = 1
		}
		builder.WriteString(`site_checker_dependency_up{dependency="`)
		builder.WriteString(escapeLabelValue(dependency))
		builder.WriteString(`"} `)
		builder.WriteString(fmt.Sprint(upValue))
		builder.WriteByte('\n')
	}

	return builder.String()
}

func cloneIntMap(input map[string]int) map[string]int {
	output := make(map[string]int, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneBoolMap(input map[string]bool) map[string]bool {
	output := make(map[string]bool, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func unixSeconds(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.Unix()
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
