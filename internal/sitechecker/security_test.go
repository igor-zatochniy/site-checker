package sitechecker

import (
	"net/http"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

func TestNetworkPolicyBlocksPrivateAndMetadataAddresses(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{80: {}, 443: {}}})

	blocked := []string{
		"http://127.0.0.1",
		"http://10.0.0.1",
		"http://172.16.0.1",
		"http://192.168.1.1",
		"http://169.254.169.254",
		"http://[::1]",
		"http://localhost",
		"http://service.localhost",
	}
	for _, raw := range blocked {
		if err := policy.ValidateURL(raw); err == nil {
			t.Fatalf("ValidateURL(%q) returned nil error", raw)
		}
	}
}

func TestNetworkPolicyAllowsPublicHTTPAndHTTPS(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{80: {}, 443: {}}})

	for _, raw := range []string{"https://example.com", "http://1.1.1.1"} {
		if err := policy.ValidateURL(raw); err != nil {
			t.Fatalf("ValidateURL(%q) returned error: %v", raw, err)
		}
	}
}

func TestNetworkPolicyBlocksUnexpectedPortsAndSchemes(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{80: {}, 443: {}}})

	for _, raw := range []string{"https://example.com:8443", "file:///etc/passwd", "https://user@example.com"} {
		if err := policy.ValidateURL(raw); err == nil {
			t.Fatalf("ValidateURL(%q) returned nil error", raw)
		}
	}
}

func TestCheckRedirectBlocksUnsafeTargets(t *testing.T) {
	policy := NewNetworkPolicy(Config{
		MaxRedirects: 3,
		AllowedPorts: map[int]struct{}{
			80:  {},
			443: {},
		},
	})
	target, err := url.Parse("http://169.254.169.254/latest/meta-data")
	if err != nil {
		t.Fatal(err)
	}

	err = policy.CheckRedirect(&http.Request{URL: target}, []*http.Request{{}})
	if err == nil {
		t.Fatal("CheckRedirect returned nil error for metadata IP")
	}
}

func TestNetworkPolicyIPClassification(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{80: {}, 443: {}}})

	if policy.IsAllowedIP(netip.MustParseAddr("169.254.169.254")) {
		t.Fatal("metadata IP is allowed")
	}
	if !policy.IsAllowedIP(netip.MustParseAddr("1.1.1.1")) {
		t.Fatal("public IP is blocked")
	}
}

func TestCheckHTTPClientUsesPerMonitorContextTimeout(t *testing.T) {
	cfg := Config{
		HTTPTimeout: 5 * time.Second,
		WorkerCount: 1,
		AllowedPorts: map[int]struct{}{
			80:  {},
			443: {},
		},
	}
	client := NewCheckHTTPClient(cfg, NewNetworkPolicy(cfg))
	if client.Timeout != 0 {
		t.Fatalf("check client timeout = %s, want zero so monitor context owns the timeout", client.Timeout)
	}
}

func TestNetworkPolicyRejectsProxyOutsidePrivateNetworkTrust(t *testing.T) {
	policy := NewNetworkPolicy(Config{
		AllowProxyEnv: true,
		AllowedPorts:  map[int]struct{}{80: {}, 443: {}},
	})
	req, err := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := policy.Proxy(req); err == nil {
		t.Fatal("Proxy returned nil error outside private-network trust mode")
	}
}
