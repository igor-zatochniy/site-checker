package main

import (
	"testing"
	"time"
)

func TestMonitorStoreClaimDueAvoidsDuplicates(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	first := store.ClaimDue(10, time.Now().UTC())
	if len(first) != 1 {
		t.Fatalf("first claim len = %d, want 1", len(first))
	}
	second := store.ClaimDue(10, time.Now().UTC())
	if len(second) != 0 {
		t.Fatalf("second claim len = %d, want 0", len(second))
	}

	_, err = store.AddCheck(CheckRecord{
		ID:        "check_1",
		MonitorID: monitor.ID,
		Success:   true,
		CheckedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMonitorStoreReclaimsStalePendingMonitor(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	first := store.ClaimDueWithLease(10, now, time.Minute)
	if len(first) != 1 || first[0].ID != monitor.ID {
		t.Fatalf("first claim = %+v, want monitor %s", first, monitor.ID)
	}

	second := store.ClaimDueWithLease(10, now.Add(30*time.Second), time.Minute)
	if len(second) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(second))
	}

	third := store.ClaimDueWithLease(10, now.Add(2*time.Minute), time.Minute)
	if len(third) != 1 || third[0].ID != monitor.ID {
		t.Fatalf("third claim = %+v, want reclaimed monitor %s", third, monitor.ID)
	}
}

func TestMonitorStoreStats(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	for _, record := range []CheckRecord{
		{ID: "check_1", MonitorID: monitor.ID, Success: true, LatencyMS: 100, CheckedAt: now},
		{ID: "check_2", MonitorID: monitor.ID, Success: false, LatencyMS: 200, CheckedAt: now.Add(time.Second)},
	} {
		if _, err := store.AddCheck(record); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := store.Stats(monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChecksTotal != 2 {
		t.Fatalf("ChecksTotal = %d, want 2", stats.ChecksTotal)
	}
	if stats.UptimePercent != 50 {
		t.Fatalf("UptimePercent = %f, want 50", stats.UptimePercent)
	}
	if stats.AverageLatencyMS != 150 {
		t.Fatalf("AverageLatencyMS = %f, want 150", stats.AverageLatencyMS)
	}
}
