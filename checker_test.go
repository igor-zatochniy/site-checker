package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testCheckerConfig(t testing.TB) Config {
	t.Helper()
	policy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	return Config{
		HTTPTimeout:    time.Second,
		MaxBodyBytes:   5,
		MaxHeaderBytes: 1024,
		ExpectedStatus: policy,
		UserAgent:      "site-checker-test",
	}
}

func TestCheckerLimitsBodyAndMarksHealthyStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "site-checker-test" {
			t.Fatalf("User-Agent = %q", got)
		}
		_, _ = w.Write([]byte("0123456789"))
	}))
	defer server.Close()

	checker := NewChecker(server.Client(), testCheckerConfig(t), nil)
	result := checker.Check(t.Context(), CheckJob{URL: server.URL})

	if !result.Healthy {
		t.Fatalf("result.Healthy = false, error = %q", result.Error)
	}
	if result.BytesRead != 5 {
		t.Fatalf("BytesRead = %d, want 5", result.BytesRead)
	}
	if result.Duration <= 0 {
		t.Fatalf("Duration = %s, want positive", result.Duration)
	}
}

func TestCheckerMarksUnexpectedStatusUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	checker := NewChecker(server.Client(), testCheckerConfig(t), nil)
	result := checker.Check(t.Context(), CheckJob{URL: server.URL})

	if result.Healthy {
		t.Fatal("result.Healthy = true, want false")
	}
	if result.StatusCode != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want 500", result.StatusCode)
	}
}
