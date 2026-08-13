package sitechecker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"testing"
	"time"
)

func TestNetworkPolicyPinsValidatedDNSAnswerForDial(t *testing.T) {
	resolver := &sequenceIPResolver{answers: [][]netip.Addr{
		{netip.MustParseAddr("93.184.216.34")},
		{netip.MustParseAddr("127.0.0.1")},
	}}
	dialer := &recordingNetworkDialer{}
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{80: {}}})
	policy.resolver = resolver
	policy.dialer = dialer

	conn, err := policy.DialContext(t.Context(), "tcp", "rebind.example:80")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if resolver.calls != 1 {
		t.Fatalf("DNS lookups during dial = %d, want exactly 1", resolver.calls)
	}
	if dialer.address != "93.184.216.34:80" {
		t.Fatalf("dial address = %q, want validated public IP literal", dialer.address)
	}
	if _, err := policy.ResolveAllowedIPs(t.Context(), "rebind.example"); err == nil {
		t.Fatal("second rebinding answer to loopback was accepted")
	}
}

func TestNetworkPolicyFiltersPrivateAddressesFromMultiAAnswer(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{443: {}}})
	policy.resolver = &sequenceIPResolver{answers: [][]netip.Addr{{
		netip.MustParseAddr("127.0.0.1"),
		netip.MustParseAddr("93.184.216.34"),
		netip.MustParseAddr("169.254.169.254"),
	}}}
	dialer := &recordingNetworkDialer{}
	policy.dialer = dialer

	conn, err := policy.DialContext(t.Context(), "tcp", "multi-a.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if dialer.address != "93.184.216.34:443" {
		t.Fatalf("dial address = %q, want only allowed public address", dialer.address)
	}
}

func TestNetworkPolicyFallsBackAfterPerAddressTimeout(t *testing.T) {
	policy := NewNetworkPolicy(Config{AllowedPorts: map[int]struct{}{443: {}}})
	policy.resolver = &sequenceIPResolver{answers: [][]netip.Addr{{
		netip.MustParseAddr("93.184.216.34"),
		netip.MustParseAddr("1.1.1.1"),
	}}}
	dialer := &timeoutThenSuccessDialer{}
	policy.dialer = dialer

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	conn, err := policy.DialContext(ctx, "tcp", "multi-a.example:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if ctx.Err() != nil {
		t.Fatalf("parent context expired before fallback succeeded: %v", ctx.Err())
	}
	if len(dialer.addresses) != 2 || dialer.addresses[1] != "1.1.1.1:443" {
		t.Fatalf("dial attempts = %v, want timed-out first IP then healthy second IP", dialer.addresses)
	}
}

func TestRedirectHostnameIsResolvedThroughSSRFPolicy(t *testing.T) {
	policy := NewNetworkPolicy(Config{MaxRedirects: 3, AllowedPorts: map[int]struct{}{80: {}}})
	policy.resolver = &sequenceIPResolver{answers: [][]netip.Addr{{netip.MustParseAddr("169.254.169.254")}}}
	redirectURL, err := url.Parse("http://redirect-to-metadata.example/latest")
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.CheckRedirect(&http.Request{URL: redirectURL}, []*http.Request{{}}); err != nil {
		t.Fatalf("syntactically valid redirect hostname was rejected before DNS: %v", err)
	}
	if _, err := policy.DialContext(t.Context(), "tcp", "redirect-to-metadata.example:80"); err == nil {
		t.Fatal("redirect hostname resolving to metadata address was dialed")
	}
}

type sequenceIPResolver struct {
	answers [][]netip.Addr
	calls   int
}

func (r *sequenceIPResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	if r.calls >= len(r.answers) {
		return nil, fmt.Errorf("unexpected DNS lookup %d", r.calls+1)
	}
	answer := append([]netip.Addr(nil), r.answers[r.calls]...)
	r.calls++
	return answer, nil
}

type recordingNetworkDialer struct {
	address string
}

func (d *recordingNetworkDialer) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	d.address = address
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

type timeoutThenSuccessDialer struct {
	addresses []string
}

func (d *timeoutThenSuccessDialer) DialContext(ctx context.Context, _, address string) (net.Conn, error) {
	d.addresses = append(d.addresses, address)
	if len(d.addresses) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

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

func TestCheckRedirectAllowsExactlyConfiguredRedirectCount(t *testing.T) {
	target, err := url.Parse("https://example.com/next")
	if err != nil {
		t.Fatal(err)
	}
	request := &http.Request{URL: target}

	policy := NewNetworkPolicy(Config{
		MaxRedirects: 1,
		AllowedPorts: map[int]struct{}{443: {}},
	})
	if err := policy.CheckRedirect(request, []*http.Request{{}}); err != nil {
		t.Fatalf("first redirect was rejected: %v", err)
	}
	if err := policy.CheckRedirect(request, []*http.Request{{}, {}}); err == nil {
		t.Fatal("second redirect was allowed with MAX_REDIRECTS=1")
	}

	policy = NewNetworkPolicy(Config{
		MaxRedirects: 0,
		AllowedPorts: map[int]struct{}{443: {}},
	})
	if err := policy.CheckRedirect(request, []*http.Request{{}}); err == nil {
		t.Fatal("redirect was allowed with MAX_REDIRECTS=0")
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
