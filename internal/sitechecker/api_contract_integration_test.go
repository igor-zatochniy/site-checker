//go:build integration

package sitechecker

import (
	"context"
	"testing"
	"time"
)

func TestAPIEmptyListsMatchOpenAPIPostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	repo := publicationTestRepository(t, ctx)
	assertEmptyListResponses(t, ctx, repo)
}
