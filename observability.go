package main

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"time"
)

func NewObservabilityServer(addr string, cfg Config, metrics *Metrics, registrars ...func(*http.ServeMux)) *http.Server {
	mux := http.NewServeMux()
	for _, register := range registrars {
		if register != nil {
			register(mux)
		}
	}
	if cfg.EnablePprof {
		registerPprof(mux)
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
			"time":   time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		snapshot := metrics.Snapshot()
		now := time.Now()
		if snapshot.LastCheckAt.IsZero() {
			if now.Sub(snapshot.StartedAt) <= cfg.StartupGracePeriod {
				writeJSON(w, http.StatusOK, map[string]any{"status": "warming_up"})
				return
			}
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "reason": "no checks completed"})
			return
		}
		if now.Sub(snapshot.LastCheckAt) > cfg.ReadinessStaleAfter {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_ready", "reason": "checks are stale"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status":        "ready",
			"last_check_at": snapshot.LastCheckAt.UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(metrics.Prometheus()))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    int(cfg.MaxHeaderBytes),
	}
}

func registerPprof(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
