package sitechecker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadLinksWithoutPathAndFallbackReturnsEmpty(t *testing.T) {
	links, err := LoadLinks("", nil)
	if err != nil {
		t.Fatalf("LoadLinks returned error: %v", err)
	}
	if len(links) != 0 {
		t.Fatalf("len(links) = %d, want 0", len(links))
	}
}

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

func TestLoadSeedLinksUsesDemoDefaultOnlyWhenEnabled(t *testing.T) {
	links, err := LoadSeedLinks(Config{})
	if err != nil {
		t.Fatalf("LoadSeedLinks returned error: %v", err)
	}
	if len(links) != 0 {
		t.Fatalf("len(links) = %d, want 0", len(links))
	}

	links, err = LoadSeedLinks(Config{SeedDefaultLinks: true})
	if err != nil {
		t.Fatalf("LoadSeedLinks with defaults returned error: %v", err)
	}
	if len(links) == 0 {
		t.Fatal("default demo links were not loaded")
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

func TestValidateLinksDoesNotExposeURLSecrets(t *testing.T) {
	const (
		secretPath     = "SECRET_PATH"
		secretQuery    = "SECRET_QUERY"
		secretFragment = "SECRET_FRAGMENT"
	)
	policy := NewNetworkPolicy(Config{})
	link := "https://localhost/" + secretPath + "?token=" + secretQuery + "#" + secretFragment

	err := ValidateLinks([]string{link}, policy)
	if err == nil {
		t.Fatal("ValidateLinks returned nil for localhost URL")
	}
	for _, secret := range []string{secretPath, secretQuery, secretFragment} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("validation error leaked URL secret %q: %v", secret, err)
		}
	}
}
