package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCheckerWorkerPool(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	for _, workers := range []int{10, 50, 100} {
		b.Run("workers_"+itoa(workers), func(b *testing.B) {
			cfg := testCheckerConfig(b)
			cfg.WorkerCount = workers
			cfg.MaxBodyBytes = 1024
			checker := NewChecker(server.Client(), cfg, nil)
			jobs := make(chan CheckJob, workers)
			results := make(chan CheckResult, workers)

			for i := 0; i < workers; i++ {
				go RunWorker(b.Context(), i+1, jobs, results, checker, discardLogger())
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				jobs <- CheckJob{URL: server.URL, EnqueuedAt: time.Now()}
				<-results
			}
		})
	}
}

func itoa(value int) string {
	return strconv.Itoa(value)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
