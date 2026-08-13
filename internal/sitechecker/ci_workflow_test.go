package sitechecker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseImageWaitsForIntegrationAndReusesScannedImage(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	workflowPath := filepath.Join(workingDirectory, "..", "..", ".github", "workflows", "ci.yml")
	content, err := os.ReadFile(workflowPath)
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(content)
	dockerStart := strings.Index(workflow, "\n  docker:\n")
	integrationStart := strings.Index(workflow, "\n  integration:\n")
	if dockerStart < 0 || integrationStart <= dockerStart {
		t.Fatal("cannot locate docker and integration jobs")
	}
	dockerJob := workflow[dockerStart:integrationStart]
	if !strings.Contains(dockerJob, "needs: integration") {
		t.Fatal("docker release job does not wait for integration tests")
	}
	if count := strings.Count(dockerJob, "uses: docker/build-push-action@v6"); count != 1 {
		t.Fatalf("docker job builds the image %d times, want exactly once", count)
	}
	if !strings.Contains(dockerJob, "docker tag site-checker:ci") ||
		!strings.Contains(dockerJob, "docker push \"${IMAGE_NAME}:${RELEASE_TAG}\"") {
		t.Fatal("release does not publish the already scanned local image")
	}
}
