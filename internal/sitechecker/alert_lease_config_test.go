package sitechecker

import (
	"strings"
	"testing"
	"time"
)

func TestLoadConfigAlertLeaseFinalizationMargin(t *testing.T) {
	for _, tc := range []struct {
		name, lease, delivery, database string
		valid                           bool
	}{
		{"equal_timeouts", "5s", "5s", "15s", false},
		{"http_margin_only", "10s", "5s", "15s", false},
		{"missing_claim_budget", "25s", "5s", "15s", false},
		{"missing_safety_margin", "35s", "5s", "15s", false},
		{"below_boundary", "39.999999999s", "5s", "15s", false},
		{"exact_boundary", "40s", "5s", "15s", true},
		{"default_lease", "1m", "5s", "15s", true},
		{"larger_database_budget", "1m", "5s", "30s", false},
		{"larger_database_budget_valid", "70s", "5s", "30s", true},
		{"long_delivery", "95s", "1m", "15s", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cleanConfigEnv(t)
			t.Setenv("ALERT_LEASE_TIMEOUT", tc.lease)
			t.Setenv("ALERT_DELIVERY_TIMEOUT", tc.delivery)
			t.Setenv("DATABASE_OPERATION_TIMEOUT", tc.database)
			_, err := LoadConfig()
			if tc.valid && err != nil {
				t.Fatalf("valid lease budgets rejected: %v", err)
			}
			if !tc.valid && (err == nil || !strings.Contains(err.Error(), "ALERT_LEASE_TIMEOUT")) {
				t.Fatalf("error=%v, want alert lease budget validation", err)
			}
		})
	}
	if minimum := minimumAlertLeaseTimeout(defaultAlertDeliveryTimeout, defaultDatabaseOperationTimeout); defaultAlertLeaseTimeout < minimum {
		t.Fatalf("default lease=%s, minimum=%s", defaultAlertLeaseTimeout, minimum)
	}
	if defaultAlertLeaseTimeout != time.Minute {
		t.Fatalf("default alert lease=%s, want 1m", defaultAlertLeaseTimeout)
	}
}
