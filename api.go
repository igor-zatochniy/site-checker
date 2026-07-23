package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

type APIHandler struct {
	service *MonitorService
	apiKey  string
	logger  *slog.Logger
}

func NewAPIHandler(service *MonitorService, apiKey string, logger *slog.Logger) *APIHandler {
	return &APIHandler{
		service: service,
		apiKey:  apiKey,
		logger:  logger,
	}
}

func (h *APIHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/monitors", h.requireAPIKey(h.createMonitor))
	mux.HandleFunc("GET /api/v1/monitors", h.requireAPIKey(h.listMonitors))
	mux.HandleFunc("GET /api/v1/monitors/{id}", h.requireAPIKey(h.getMonitor))
	mux.HandleFunc("PATCH /api/v1/monitors/{id}", h.requireAPIKey(h.updateMonitor))
	mux.HandleFunc("DELETE /api/v1/monitors/{id}", h.requireAPIKey(h.deleteMonitor))
	mux.HandleFunc("GET /api/v1/monitors/{id}/checks", h.requireAPIKey(h.listChecks))
	mux.HandleFunc("POST /api/v1/monitors/{id}/check", h.requireAPIKey(h.runManualCheck))
	mux.HandleFunc("GET /api/v1/monitors/{id}/stats", h.requireAPIKey(h.getStats))
	mux.HandleFunc("GET /api/v1/incidents", h.requireAPIKey(h.listIncidents))
}

func (h *APIHandler) createMonitor(w http.ResponseWriter, r *http.Request) {
	var input MonitorInput
	if err := decodeJSON(r, &input); err != nil {
		h.writeRequestError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON", err)
		return
	}

	monitor, err := h.service.Create(r.Context(), input)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, monitor)
}

func (h *APIHandler) listMonitors(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		h.writeRequestError(w, r, http.StatusBadRequest, "invalid_pagination", "pagination parameters are invalid", err)
		return
	}

	items, total, err := h.service.List(r.Context(), offset, limit)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
}

func (h *APIHandler) getMonitor(w http.ResponseWriter, r *http.Request) {
	monitor, err := h.service.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *APIHandler) updateMonitor(w http.ResponseWriter, r *http.Request) {
	var patch MonitorPatch
	if err := decodeJSON(r, &patch); err != nil {
		h.writeRequestError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON", err)
		return
	}

	monitor, err := h.service.Update(r.Context(), r.PathValue("id"), patch)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *APIHandler) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	err := h.service.Delete(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *APIHandler) listChecks(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		h.writeRequestError(w, r, http.StatusBadRequest, "invalid_pagination", "pagination parameters are invalid", err)
		return
	}

	items, total, err := h.service.ListChecks(r.Context(), r.PathValue("id"), offset, limit)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
}

func (h *APIHandler) runManualCheck(w http.ResponseWriter, r *http.Request) {
	record, err := h.service.RunManualCheck(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (h *APIHandler) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.service.Stats(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h *APIHandler) listIncidents(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		h.writeRequestError(w, r, http.StatusBadRequest, "invalid_pagination", "pagination parameters are invalid", err)
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	items, total, err := h.service.ListIncidents(r.Context(), status, offset, limit)
	if err != nil {
		h.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
}

func (h *APIHandler) requireAPIKey(next http.HandlerFunc) http.HandlerFunc {
	if h.apiKey == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if h.validAPIKey(r) {
			next(w, r)
			return
		}
		writeAPIError(w, http.StatusUnauthorized, "unauthorized", "valid API key is required")
	}
}

func (h *APIHandler) validAPIKey(r *http.Request) bool {
	candidates := []string{
		strings.TrimSpace(r.Header.Get("X-API-Key")),
		bearerToken(r.Header.Get("Authorization")),
	}
	for _, candidate := range candidates {
		if candidate == "" || len(candidate) != len(h.apiKey) {
			continue
		}
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(h.apiKey)) == 1 {
			return true
		}
	}
	return false
}

func (h *APIHandler) writeRequestError(w http.ResponseWriter, r *http.Request, statusCode int, code, message string, err error) {
	h.logAPIError(r, statusCode, code, err)
	writeAPIError(w, statusCode, code, message)
}

func (h *APIHandler) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	statusCode, code, message := mapAPIError(err)
	h.logAPIError(r, statusCode, code, err)
	writeAPIError(w, statusCode, code, message)
}

func mapAPIError(err error) (int, string, string) {
	switch {
	case errors.Is(err, ErrMonitorNotFound):
		return http.StatusNotFound, "monitor_not_found", "monitor not found"
	case errors.Is(err, ErrMonitorExists):
		return http.StatusConflict, "monitor_exists", "monitor already exists"
	case errors.Is(err, ErrInvalidMonitor):
		return http.StatusBadRequest, "invalid_monitor", "monitor request is invalid"
	case errors.Is(err, ErrStaleJob):
		return http.StatusConflict, "stale_check_job", "check job is no longer active"
	case errors.Is(err, ErrJobAlreadyProcessing):
		return http.StatusConflict, "check_job_processing", "check job is already processing"
	case errors.Is(err, ErrQueueFull), errors.Is(err, ErrQueueConsumerClosed):
		return http.StatusServiceUnavailable, "queue_unavailable", "check queue is unavailable"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, "service_unavailable", "service is temporarily unavailable"
	default:
		return http.StatusInternalServerError, "internal_error", "internal server error"
	}
}

func (h *APIHandler) logAPIError(r *http.Request, statusCode int, code string, err error) {
	if err == nil || h.logger == nil {
		return
	}
	args := []any{
		"method", r.Method,
		"path", r.URL.Path,
		"status", statusCode,
		"code", code,
		"error", err,
	}
	if statusCode >= http.StatusInternalServerError {
		h.logger.Error("API request failed", args...)
		return
	}
	h.logger.Warn("API request rejected", args...)
}

func decodeJSON(r *http.Request, target any) error {
	if r.Body == nil {
		return fmt.Errorf("request body is required")
	}
	defer r.Body.Close()

	decoder := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("request body must contain one JSON document")
	}
	return nil
}

func pagination(r *http.Request) (int, int, error) {
	offset, err := parseNonNegativeInt(r.URL.Query().Get("offset"), 0)
	if err != nil {
		return 0, 0, err
	}
	limit, err := parseNonNegativeInt(r.URL.Query().Get("limit"), defaultPageLimit)
	if err != nil {
		return 0, 0, err
	}
	if limit == 0 || limit > maxPageLimit {
		return 0, 0, fmt.Errorf("limit must be between 1 and %d", maxPageLimit)
	}
	return offset, limit, nil
}

func parseNonNegativeInt(raw string, defaultValue int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("value must be a non-negative integer")
	}
	return value, nil
}

func bearerToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	scheme, token, ok := strings.Cut(raw, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return ""
	}
	return strings.TrimSpace(token)
}

func writeAPIError(w http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(w, statusCode, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
