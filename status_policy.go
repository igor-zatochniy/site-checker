package main

import (
	"fmt"
	"strconv"
	"strings"
)

type StatusPolicy struct {
	ranges []StatusRange
	raw    string
}

type StatusRange struct {
	Min int
	Max int
}

func ParseStatusPolicy(raw string) (StatusPolicy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return StatusPolicy{}, fmt.Errorf("policy is empty")
	}

	parts := strings.Split(raw, ",")
	ranges := make([]StatusRange, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return StatusPolicy{}, fmt.Errorf("empty status item")
		}

		statusRange, err := parseStatusRange(part)
		if err != nil {
			return StatusPolicy{}, err
		}
		ranges = append(ranges, statusRange)
	}

	return StatusPolicy{ranges: ranges, raw: raw}, nil
}

func parseStatusRange(raw string) (StatusRange, error) {
	if !strings.Contains(raw, "-") {
		code, err := parseHTTPStatus(raw)
		if err != nil {
			return StatusRange{}, err
		}
		return StatusRange{Min: code, Max: code}, nil
	}

	parts := strings.Split(raw, "-")
	if len(parts) != 2 {
		return StatusRange{}, fmt.Errorf("invalid status range %q", raw)
	}

	minCode, err := parseHTTPStatus(parts[0])
	if err != nil {
		return StatusRange{}, err
	}
	maxCode, err := parseHTTPStatus(parts[1])
	if err != nil {
		return StatusRange{}, err
	}
	if minCode > maxCode {
		return StatusRange{}, fmt.Errorf("invalid descending status range %q", raw)
	}
	return StatusRange{Min: minCode, Max: maxCode}, nil
}

func parseHTTPStatus(raw string) (int, error) {
	code, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("status %q must be numeric", raw)
	}
	if code < 100 || code > 599 {
		return 0, fmt.Errorf("status %d is outside 100-599", code)
	}
	return code, nil
}

func (p StatusPolicy) Allows(statusCode int) bool {
	for _, statusRange := range p.ranges {
		if statusCode >= statusRange.Min && statusCode <= statusRange.Max {
			return true
		}
	}
	return false
}

func (p StatusPolicy) String() string {
	return p.raw
}
