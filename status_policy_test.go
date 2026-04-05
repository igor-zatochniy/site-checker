package main

import "testing"

func TestStatusPolicyAllowsConfiguredRanges(t *testing.T) {
	policy, err := ParseStatusPolicy("200-204,301,302")
	if err != nil {
		t.Fatalf("ParseStatusPolicy returned error: %v", err)
	}

	tests := map[int]bool{
		199: false,
		200: true,
		204: true,
		205: false,
		301: true,
		302: true,
		500: false,
	}
	for status, want := range tests {
		if got := policy.Allows(status); got != want {
			t.Fatalf("Allows(%d) = %v, want %v", status, got, want)
		}
	}
}

func TestStatusPolicyRejectsInvalidInput(t *testing.T) {
	for _, raw := range []string{"", "abc", "99", "600", "300-200", "200-"} {
		if _, err := ParseStatusPolicy(raw); err == nil {
			t.Fatalf("ParseStatusPolicy(%q) returned nil error", raw)
		}
	}
}
