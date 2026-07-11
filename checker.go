package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type CheckJob struct {
	URL        string
	Sequence   uint64
	EnqueuedAt time.Time
}

type CheckResult struct {
	MonitorID  string
	URL        string
	Sequence   uint64
	Healthy    bool
	StatusCode int
	Duration   time.Duration
	BytesRead  int64
	Error      string
	CheckedAt  time.Time
}

type Checker struct {
	client *http.Client
	cfg    Config
}

func NewChecker(client *http.Client, cfg Config, _ *Metrics) *Checker {
	return &Checker{client: client, cfg: cfg}
}

func (c *Checker) Check(ctx context.Context, job CheckJob) (result CheckResult) {
	startedAt := time.Now()
	result = CheckResult{
		URL:       job.URL,
		Sequence:  job.Sequence,
		CheckedAt: startedAt,
	}
	defer func() {
		duration := time.Since(startedAt)
		if duration <= 0 {
			duration = time.Nanosecond
		}
		result.Duration = duration
		result.CheckedAt = time.Now().UTC()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, job.URL, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent)
	req.Header.Set("Accept", "*/*")

	resp, err := c.client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer resp.Body.Close()

	result.StatusCode = resp.StatusCode
	bytesRead, readErr := io.Copy(io.Discard, io.LimitReader(resp.Body, c.cfg.MaxBodyBytes))
	result.BytesRead = bytesRead

	if readErr != nil {
		result.Error = readErr.Error()
		return result
	}
	if !c.cfg.ExpectedStatus.Allows(resp.StatusCode) {
		result.Error = fmt.Sprintf("unexpected status %d", resp.StatusCode)
		return result
	}

	result.Healthy = true
	return result
}

func (c *Checker) CheckMonitor(ctx context.Context, monitor Monitor) CheckResult {
	policy, err := ParseStatusPolicy(strconv.Itoa(monitor.ExpectedStatus))
	if err != nil {
		return CheckResult{
			URL:       monitor.URL,
			Error:     err.Error(),
			CheckedAt: time.Now().UTC(),
		}
	}

	cfg := c.cfg
	cfg.ExpectedStatus = policy
	cfg.UserAgent = c.cfg.UserAgent
	checker := Checker{client: c.client, cfg: cfg}
	result := checker.Check(ctx, CheckJob{URL: monitor.URL})
	result.MonitorID = monitor.ID
	result.Sequence = 0
	return result
}

func CheckRecordFromResult(result CheckResult) CheckRecord {
	return CheckRecord{
		ID:         newID("check"),
		MonitorID:  result.MonitorID,
		StatusCode: result.StatusCode,
		LatencyMS:  result.Duration.Milliseconds(),
		Error:      result.Error,
		Success:    result.Healthy,
		CheckedAt:  result.CheckedAt.UTC(),
	}
}

func RunWorker(ctx context.Context, workerID int, jobs <-chan CheckJob, results chan<- CheckResult, checker *Checker, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}

			result := checker.Check(ctx, job)
			if result.Error != "" && ctx.Err() == nil {
				logger.Warn("Site check failed", "worker", workerID, "url", result.URL, "error", result.Error)
			}

			select {
			case results <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}
