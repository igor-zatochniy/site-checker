package sitechecker

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOpenPostgresPoolRedactsParseConfigSecrets(t *testing.T) {
	testCases := []struct {
		name        string
		databaseURL string
	}{
		{
			name:        "invalid connect timeout",
			databaseURL: "postgres://user:DB_SECRET@host/site_checker?sslpassword=TLS_SECRET&connect_timeout=BAD",
		},
		{
			name:        "invalid pool setting",
			databaseURL: "postgres://user:DB_SECRET@host/site_checker?sslpassword=TLS_SECRET&pool_max_conns=BAD",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := OpenPostgresPool(context.Background(), testCase.databaseURL)
			if err == nil {
				t.Fatal("OpenPostgresPool returned nil error for invalid connection string")
			}
			for _, secret := range []string{"DB_SECRET", "TLS_SECRET", testCase.databaseURL} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("OpenPostgresPool error leaked connection data: %v", err)
				}
			}
			if err.Error() != "invalid PostgreSQL connection string" {
				t.Fatalf("OpenPostgresPool error = %q, want safe static message", err)
			}
		})
	}
}

func TestPostgresRepositoryOperationContextUsesConfiguredTimeout(t *testing.T) {
	repository := NewPostgresMonitorRepositoryWithTimeout(nil, nil, 100*time.Millisecond)
	started := time.Now()
	ctx, cancel := repository.operationContext(context.Background())
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("operation context has no deadline")
	}
	if got := deadline.Sub(started); got < 50*time.Millisecond || got > 150*time.Millisecond {
		t.Fatalf("operation context timeout = %s, want approximately 100ms", got)
	}

	parent, parentCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer parentCancel()
	parentStarted := time.Now()
	child, childCancel := repository.operationContext(parent)
	defer childCancel()
	childDeadline, ok := child.Deadline()
	if !ok {
		t.Fatal("operation context lost the caller deadline")
	}
	if got := childDeadline.Sub(parentStarted); got < 0 || got > 50*time.Millisecond {
		t.Fatalf("operation context deadline = %s, want caller deadline to take precedence", got)
	}
}
