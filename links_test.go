package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadLinksFromTextFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "urls.txt")
	content := "# comment\nhttps://example.com\n\nhttps://example.com\nhttps://openai.com\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	links, err := LoadLinks(path, nil)
	if err != nil {
		t.Fatalf("LoadLinks returned error: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("len(links) = %d, want 2", len(links))
	}
}

func TestLoadLinksFromJSONFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "urls.json")
	content := `["https://example.com","https://openai.com"]`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	links, err := LoadLinks(path, nil)
	if err != nil {
		t.Fatalf("LoadLinks returned error: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("len(links) = %d, want 2", len(links))
	}
}
