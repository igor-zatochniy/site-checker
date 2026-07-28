package sitechecker

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestEmbeddedRuntimeAssetsMatchPublicFiles(t *testing.T) {
	assertEmbeddedFileMatches(t, filepath.Join("..", "..", "api", "openapi.yaml"), openAPISpec)

	rootEntries, err := os.ReadDir(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("read public migrations: %v", err)
	}
	embeddedEntries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}

	rootNames := sqlMigrationNames(rootEntries)
	embeddedNames := sqlMigrationNames(embeddedEntries)
	if !slices.Equal(rootNames, embeddedNames) {
		t.Fatalf("embedded migrations differ from public migrations: public=%v embedded=%v", rootNames, embeddedNames)
	}

	for _, name := range rootNames {
		publicPath := filepath.Join("..", "..", "migrations", name)
		publicBytes, err := os.ReadFile(publicPath)
		if err != nil {
			t.Fatalf("read public migration %s: %v", name, err)
		}
		embeddedBytes, err := migrationFiles.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatalf("read embedded migration %s: %v", name, err)
		}
		if !bytes.Equal(publicBytes, embeddedBytes) {
			t.Fatalf("embedded migration %s differs from public migration", name)
		}
	}
}

func assertEmbeddedFileMatches(t *testing.T, publicPath string, embeddedBytes []byte) {
	t.Helper()

	publicBytes, err := os.ReadFile(publicPath)
	if err != nil {
		t.Fatalf("read public file %s: %v", publicPath, err)
	}
	if !bytes.Equal(publicBytes, embeddedBytes) {
		t.Fatalf("embedded file differs from public file %s", publicPath)
	}
}

func sqlMigrationNames(entries []fs.DirEntry) []string {
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	slices.Sort(names)
	return names
}
