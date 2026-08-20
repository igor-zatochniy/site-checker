package sitechecker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestMetricsUseBoundedMonitorIDsWithoutExposingURLs(t *testing.T) {
	metrics := NewMetrics("test", "commit", "date", 0)
	recordedAt := time.Now().UTC()
	metrics.RecordResult(CheckResult{
		MonitorID:  "mon_1",
		URL:        "https://example.com/health?token=secret",
		Healthy:    true,
		StatusCode: 200,
		Duration:   time.Millisecond,
		CheckedAt:  time.Now().UTC(),
	}, recordedAt)

	output := metrics.Prometheus()
	if strings.Contains(output, "token=secret") || strings.Contains(output, "https://example.com") {
		t.Fatalf("metrics exposed a monitored URL: %s", output)
	}
	if !strings.Contains(output, `monitor_id="mon_1"`) {
		t.Fatalf("metrics do not contain the monitor ID label: %s", output)
	}

	for index := 0; index < maxTrackedMetricMonitors+5; index++ {
		metrics.RecordResult(CheckResult{
			MonitorID:  fmt.Sprintf("mon_%d", index+2),
			Healthy:    true,
			StatusCode: 200,
			Duration:   time.Millisecond,
			CheckedAt:  time.Now().UTC(),
		}, time.Now().UTC())
	}

	snapshot := metrics.Snapshot()
	if len(snapshot.UpByMonitor) != maxTrackedMetricMonitors {
		t.Fatalf("tracked monitors = %d, want %d", len(snapshot.UpByMonitor), maxTrackedMetricMonitors)
	}
	if _, exists := snapshot.UpByMonitor["mon_1"]; exists {
		t.Fatal("oldest monitor metric was not evicted")
	}
}

func TestMetricsUseAuthoritativeRecordedTime(t *testing.T) {
	metrics := NewMetrics("test", "commit", "date", 0)
	recordedAt := time.Now().UTC()
	metrics.RecordResult(CheckResult{
		MonitorID: "mon_1",
		Healthy:   true,
		CheckedAt: recordedAt.Add(24 * time.Hour),
	}, recordedAt)

	snapshot := metrics.Snapshot()
	if !snapshot.LastCheckAt.Equal(recordedAt) || !snapshot.LastSuccessAt.Equal(recordedAt) {
		t.Fatalf("metric timestamps = check:%s success:%s, want %s", snapshot.LastCheckAt, snapshot.LastSuccessAt, recordedAt)
	}
}

func TestMonitorServiceClearsPerMonitorMetrics(t *testing.T) {
	cfg := testCheckerConfig(t)
	policy := NewNetworkPolicy(cfg)
	metrics := NewMetricsForRole("test", "commit", "date", 0, "all")
	repo := NewInMemoryMonitorRepository(policy)
	service := NewMonitorService(repo, nil, metrics, AlertPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	createMonitor := func(url string) Monitor {
		t.Helper()
		monitor, err := service.Create(context.Background(), MonitorInput{
			URL:             url,
			IntervalSeconds: 60,
			TimeoutSeconds:  5,
			ExpectedStatus:  200,
		})
		if err != nil {
			t.Fatal(err)
		}
		metrics.RecordResult(CheckResult{
			MonitorID:  monitor.ID,
			StatusCode: 503,
			Healthy:    false,
		}, time.Now().UTC())
		return monitor
	}

	deleted := createMonitor("https://delete.example.com")
	if err := service.Delete(context.Background(), deleted.ID); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.Prometheus(), `monitor_id="`+deleted.ID+`"`) {
		t.Fatal("deleted monitor is still exported")
	}

	updated := createMonitor("https://update.example.com")
	disabled := false
	if _, err := service.Update(context.Background(), updated.ID, MonitorPatch{Enabled: &disabled}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.Prometheus(), `monitor_id="`+updated.ID+`"`) {
		t.Fatal("disabled monitor is still exported")
	}

	semanticallyUpdated := createMonitor("https://semantics.example.com")
	expectedStatus := 204
	if _, err := service.Update(context.Background(), semanticallyUpdated.ID, MonitorPatch{ExpectedStatus: &expectedStatus}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(metrics.Prometheus(), `monitor_id="`+semanticallyUpdated.ID+`"`) {
		t.Fatal("semantically updated monitor is still exported with its previous state")
	}
}

func TestSplitRoleMetricsDoNotExportProcessLocalMonitorState(t *testing.T) {
	workerA := NewMetricsForRole("test", "commit", "date", 0, "worker")
	workerB := NewMetricsForRole("test", "commit", "date", 0, "worker")
	now := time.Now().UTC()
	workerA.RecordResult(CheckResult{MonitorID: "mon_shared", StatusCode: 503}, now)
	workerB.RecordResult(CheckResult{MonitorID: "mon_shared", StatusCode: 200, Healthy: true}, now)

	for name, metrics := range map[string]*Metrics{"worker A": workerA, "worker B": workerB} {
		output := metrics.Prometheus()
		for _, forbidden := range []string{
			"site_checker_site_up",
			"site_checker_site_status_code",
			"site_checker_site_consecutive_failures",
			`monitor_id="mon_shared"`,
		} {
			if strings.Contains(output, forbidden) {
				t.Fatalf("%s exported process-local monitor state %q", name, forbidden)
			}
		}
		if !strings.Contains(output, "site_checker_checks_total 1") {
			t.Fatalf("%s did not export aggregatable process counters", name)
		}
		if snapshot := metrics.Snapshot(); len(snapshot.UpByMonitor) != 0 {
			t.Fatalf("%s retained disabled per-monitor state: %+v", name, snapshot.UpByMonitor)
		}
	}
}
