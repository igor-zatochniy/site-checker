package sitechecker

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

const (
	defaultHTTPWriteTimeout = 10 * time.Second
	httpResponseWriteMargin = 5 * time.Second
)

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

		response := map[string]any{"status": "ready"}
		if cfg.AppRole != "" {
			response["role"] = cfg.AppRole
		}
		writeJSON(w, http.StatusOK, response)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(metrics.Prometheus()))
	})

	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      responseWriteTimeout(cfg),
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    int(cfg.MaxHeaderBytes),
	}
}

func responseWriteTimeout(cfg Config) time.Duration {
	requestTimeout := cfg.DatabaseOperationTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultDatabaseOperationTimeout
	}
	return max(defaultHTTPWriteTimeout, requestTimeout+httpResponseWriteMargin)
}

func checkReadinessDependencies(ctx context.Context, metrics *Metrics, dependencies []ReadinessDependency) map[string]string {
	if len(dependencies) == 0 {
		return nil
	}

	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	type dependencyResult struct {
		name string
		err  error
	}
	results := make(chan dependencyResult, len(dependencies))
	pending := 0
	for _, dependency := range dependencies {
		if dependency.Name == "" || dependency.Check == nil {
			continue
		}
		pending++
		go func(dependency ReadinessDependency) {
			err := checkCtx.Err()
			if err == nil {
				err = dependency.Check(checkCtx)
			}
			results <- dependencyResult{name: dependency.Name, err: err}
		}(dependency)
	}

	failures := make(map[string]string)
	for range pending {
		result := <-results
		metrics.SetDependencyUp(result.name, result.err == nil)
		if result.err != nil {
			failures[result.name] = fmt.Sprintf("%v", result.err)
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
