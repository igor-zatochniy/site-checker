package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"
)

type ReadinessDependency struct {
	Name  string
	Check func(ctx context.Context) error
}

func NewObservabilityServer(addr string, cfg Config, metrics *Metrics, registrars ...func(*http.ServeMux)) *http.Server {
	return NewObservabilityServerWithDependencies(addr, cfg, metrics, nil, registrars...)
}

func NewObservabilityServerWithDependencies(addr string, cfg Config, metrics *Metrics, dependencies []ReadinessDependency, registrars ...func(*http.ServeMux)) *http.Server {
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
		if dependencyErrors := checkReadinessDependencies(r.Context(), metrics, dependencies); len(dependencyErrors) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status":       "not_ready",
				"dependencies": dependencyErrors,
			})
			return
		}

		if cfg.AppRole != "" && cfg.AppRole != "all" {
			writeJSON(w, http.StatusOK, map[string]any{
				"status": "ready",
				"role":   cfg.AppRole,
			})
			return
		}

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

func checkReadinessDependencies(ctx context.Context, metrics *Metrics, dependencies []ReadinessDependency) map[string]string {
	if len(dependencies) == 0 {
		return nil
	}

	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	failures := make(map[string]string)
	for _, dependency := range dependencies {
		if dependency.Name == "" || dependency.Check == nil {
			continue
		}
		err := dependency.Check(checkCtx)
		metrics.SetDependencyUp(dependency.Name, err == nil)
		if err != nil {
			failures[dependency.Name] = fmt.Sprintf("%v", err)
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return failures
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
