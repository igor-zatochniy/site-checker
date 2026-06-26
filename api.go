package main

import (
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
		writeAPIError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	monitor, err := h.service.Create(r.Context(), input)
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, monitor)
}

func (h *APIHandler) listMonitors(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}

	items, total, err := h.service.List(r.Context(), offset, limit)
	if err != nil {
		h.writeStoreError(w, err)
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
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *APIHandler) updateMonitor(w http.ResponseWriter, r *http.Request) {
	var patch MonitorPatch
	if err := decodeJSON(r, &patch); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	monitor, err := h.service.Update(r.Context(), r.PathValue("id"), patch)
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *APIHandler) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	err := h.service.Delete(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *APIHandler) listChecks(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}

	items, total, err := h.service.ListChecks(r.Context(), r.PathValue("id"), offset, limit)
	if err != nil {
		h.writeStoreError(w, err)
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
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, record)
}

func (h *APIHandler) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.service.Stats(r.Context(), r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h *APIHandler) listIncidents(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}

	status := strings.TrimSpace(r.URL.Query().Get("status"))
	items, total, err := h.service.ListIncidents(r.Context(), status, offset, limit)
	if err != nil {
		h.writeStoreError(w, err)
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

func (h *APIHandler) writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrMonitorNotFound):
		writeAPIError(w, http.StatusNotFound, "monitor_not_found", "monitor not found")
	case errors.Is(err, ErrMonitorExists):
		writeAPIError(w, http.StatusConflict, "monitor_exists", "monitor already exists")
	default:
		writeAPIError(w, http.StatusBadRequest, "invalid_monitor", err.Error())
	}
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
