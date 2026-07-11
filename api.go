package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPageLimit = 50
	maxPageLimit     = 100
)

type APIHandler struct {
	store        *MonitorStore
	checker      *Checker
	metrics      *Metrics
	alerts       *AlertManager
	logger       *slog.Logger
	totalUpdater func()
}

func NewAPIHandler(store *MonitorStore, checker *Checker, metrics *Metrics, alerts *AlertManager, logger *slog.Logger) *APIHandler {
	handler := &APIHandler{
		store:   store,
		checker: checker,
		metrics: metrics,
		alerts:  alerts,
		logger:  logger,
	}
	handler.totalUpdater = func() {
		metrics.SetTotalLinks(store.Count())
	}
	return handler
}

func (h *APIHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/monitors", h.createMonitor)
	mux.HandleFunc("GET /api/v1/monitors", h.listMonitors)
	mux.HandleFunc("GET /api/v1/monitors/{id}", h.getMonitor)
	mux.HandleFunc("PATCH /api/v1/monitors/{id}", h.updateMonitor)
	mux.HandleFunc("DELETE /api/v1/monitors/{id}", h.deleteMonitor)
	mux.HandleFunc("GET /api/v1/monitors/{id}/checks", h.listChecks)
	mux.HandleFunc("POST /api/v1/monitors/{id}/check", h.runManualCheck)
	mux.HandleFunc("GET /api/v1/monitors/{id}/stats", h.getStats)
}

func (h *APIHandler) createMonitor(w http.ResponseWriter, r *http.Request) {
	var input MonitorInput
	if err := decodeJSON(r, &input); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	monitor, err := h.store.Create(input)
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	h.totalUpdater()
	writeJSON(w, http.StatusCreated, monitor)
}

func (h *APIHandler) listMonitors(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}

	items, total := h.store.List(offset, limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"items":  items,
		"total":  total,
		"offset": offset,
		"limit":  limit,
	})
}

func (h *APIHandler) getMonitor(w http.ResponseWriter, r *http.Request) {
	monitor, err := h.store.Get(r.PathValue("id"))
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

	monitor, err := h.store.Update(r.PathValue("id"), patch)
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, monitor)
}

func (h *APIHandler) deleteMonitor(w http.ResponseWriter, r *http.Request) {
	err := h.store.Delete(r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	h.totalUpdater()
	w.WriteHeader(http.StatusNoContent)
}

func (h *APIHandler) listChecks(w http.ResponseWriter, r *http.Request) {
	offset, limit, err := pagination(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_pagination", err.Error())
		return
	}

	items, total, err := h.store.ListChecks(r.PathValue("id"), offset, limit)
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
	monitor, err := h.store.Get(r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(monitor.TimeoutSeconds)*time.Second)
	defer cancel()

	result := h.checker.CheckMonitor(ctx, monitor)
	record := CheckRecordFromResult(result)
	if _, err := h.store.AddCheck(record); err != nil {
		h.writeStoreError(w, err)
		return
	}

	h.metrics.RecordResult(result)
	if h.alerts != nil {
		h.alerts.Handle(r.Context(), result)
	}
	writeJSON(w, http.StatusAccepted, record)
}

func (h *APIHandler) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.Stats(r.PathValue("id"))
	if err != nil {
		h.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
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

func writeAPIError(w http.ResponseWriter, statusCode int, code, message string) {
	writeJSON(w, statusCode, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
