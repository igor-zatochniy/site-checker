package sitechecker

import (
	"os"
	"path/filepath"
	"regexp"
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
	if count := strings.Count(dockerJob, "uses: docker/build-push-action@"); count != 1 {
		t.Fatalf("docker job builds the image %d times, want exactly once", count)
	}
	if !strings.Contains(dockerJob, "docker tag site-checker:ci") ||
		!strings.Contains(dockerJob, "docker push \"${IMAGE_NAME}:${RELEASE_TAG}\"") {
		t.Fatal("release does not publish the already scanned local image")
	}
	globalSection := workflow[:strings.Index(workflow, "\njobs:\n")]
	if strings.Contains(globalSection, "packages: write") || strings.Contains(globalSection, "contents: write") {
		t.Fatal("workflow grants release write permissions globally")
	}
	if !strings.Contains(dockerJob, "permissions:\n      contents: write\n      packages: write") {
		t.Fatal("docker release job does not declare scoped package write permission")
	}
	if !strings.Contains(dockerJob, "refusing to overwrite it") ||
		!strings.Contains(dockerJob, "gh release create") ||
		!strings.Contains(dockerJob, "site-checker-checksums-${RELEASE_TAG}.txt") ||
		!strings.Contains(dockerJob, "site-checker-sbom-${RELEASE_TAG}.spdx.json") {
		t.Fatal("release job does not preserve immutable manifests, SBOM, and checksums in a GitHub release")
	}
	if !strings.Contains(workflow, "run: go mod verify") || !strings.Contains(workflow, "run: go mod tidy -diff") {
		t.Fatal("CI does not verify module integrity and tidiness")
	}
	for _, line := range strings.Split(workflow, "\n") {
		assertActionPinned(t, line)
	}
	pagesContent, err := os.ReadFile(filepath.Join(workingDirectory, "..", "..", ".github", "workflows", "pages.yml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(pagesContent), "\n") {
		assertActionPinned(t, line)
	}
}

func TestKubernetesSourceImagesUseOneReleaseTag(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Join(workingDirectory, "..", "..")
	baseKustomization := readTestFile(t, filepath.Join(repositoryRoot, "deploy", "kubernetes", "kustomization.yaml"))
	releaseTagMatch := regexp.MustCompile(`(?m)^\s*newTag:\s*(v[0-9]+\.[0-9]+\.[0-9]+)\s*$`).FindStringSubmatch(baseKustomization)
	if len(releaseTagMatch) != 2 {
		t.Fatal("Kubernetes base does not contain one semantic release tag")
	}
	releaseTag := releaseTagMatch[1]

	deploymentPaths := []string{
		filepath.Join(repositoryRoot, "deploy", "kubernetes", "api-deployment.yaml"),
		filepath.Join(repositoryRoot, "deploy", "kubernetes", "scheduler-deployment.yaml"),
		filepath.Join(repositoryRoot, "deploy", "kubernetes", "worker-deployment.yaml"),
		filepath.Join(repositoryRoot, "deploy", "kubernetes", "alerts", "deployment.yaml"),
	}
	imageReference := "image: ghcr.io/igor-zatochniy/site-checker:" + releaseTag
	for _, path := range deploymentPaths {
		if !strings.Contains(readTestFile(t, path), imageReference) {
			t.Fatalf("%s is not aligned with Kubernetes release tag %s", path, releaseTag)
		}
	}
	alertsKustomization := readTestFile(t, filepath.Join(repositoryRoot, "deploy", "kubernetes", "alerts", "kustomization.yaml"))
	if !regexp.MustCompile(`(?m)^\s*newTag:\s*` + regexp.QuoteMeta(releaseTag) + `\s*$`).MatchString(alertsKustomization) {
		t.Fatalf("alerts kustomization is not aligned with Kubernetes release tag %s", releaseTag)
	}
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func assertActionPinned(t *testing.T, line string) {
	t.Helper()
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "uses:") {
		return
	}
	parts := strings.SplitN(line, "@", 2)
	if len(parts) != 2 {
		t.Fatalf("action has no immutable ref: %s", line)
	}
	ref := strings.Fields(parts[1])[0]
	if len(ref) != 40 {
		t.Fatalf("action is not pinned to a commit SHA: %s", line)
	}
}

func TestGoToolchainAndDockerBasesArePinned(t *testing.T) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot := filepath.Join(workingDirectory, "..", "..")
	moduleContent, err := os.ReadFile(filepath.Join(repositoryRoot, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	versionMatch := regexp.MustCompile(`(?m)^toolchain go([0-9]+\.[0-9]+\.[0-9]+)$`).FindStringSubmatch(string(moduleContent))
	if len(versionMatch) != 2 {
		t.Fatal("go.mod does not pin an exact Go toolchain")
	}
	version := versionMatch[1]

	dockerContent, err := os.ReadFile(filepath.Join(repositoryRoot, "Dockerfile"))
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(dockerContent)
	if !strings.Contains(dockerfile, "ARG GO_VERSION="+version) {
		t.Fatalf("Dockerfile Go version is not aligned with toolchain go%s", version)
	}
	digestArguments := regexp.MustCompile(`(?m)^ARG (?:GO_IMAGE_DIGEST|ALPINE_IMAGE_DIGEST)=sha256:[0-9a-f]{64}$`).FindAllString(dockerfile, -1)
	if len(digestArguments) != 2 ||
		!strings.Contains(dockerfile, "@${GO_IMAGE_DIGEST} AS builder") ||
		!strings.Contains(dockerfile, "@${ALPINE_IMAGE_DIGEST}") {
		t.Fatal("Docker builder and runtime bases are not pinned to image digests")
	}

	workflowContent, err := os.ReadFile(filepath.Join(repositoryRoot, ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(workflowContent), `GO_VERSION: "`+version+`"`) {
		t.Fatalf("CI Go version is not aligned with toolchain go%s", version)
	}
}
