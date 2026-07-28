package sitechecker

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMetricsUseBoundedMonitorIDsWithoutExposingURLs(t *testing.T) {
	metrics := NewMetrics("test", "commit", "date", 0)
	metrics.RecordResult(CheckResult{
		MonitorID:  "mon_1",
		URL:        "https://example.com/health?token=secret",
		Healthy:    true,
		StatusCode: 200,
		Duration:   time.Millisecond,
		CheckedAt:  time.Now().UTC(),
	})

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
		})
	}

	snapshot := metrics.Snapshot()
	if len(snapshot.UpByMonitor) != maxTrackedMetricMonitors {
		t.Fatalf("tracked monitors = %d, want %d", len(snapshot.UpByMonitor), maxTrackedMetricMonitors)
	}
	if _, exists := snapshot.UpByMonitor["mon_1"]; exists {
		t.Fatal("oldest monitor metric was not evicted")
	}
}
