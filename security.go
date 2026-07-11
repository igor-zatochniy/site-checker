package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type NetworkPolicy struct {
	allowPrivateNetworks bool
	allowProxyEnv        bool
	maxRedirects         int
	allowedPorts         map[int]struct{}
	resolver             *net.Resolver
	dialer               *net.Dialer
}

func NewNetworkPolicy(cfg Config) *NetworkPolicy {
	allowedPorts := cfg.AllowedPorts
	if len(allowedPorts) == 0 {
		allowedPorts = map[int]struct{}{80: {}, 443: {}}
	}

	return &NetworkPolicy{
		allowPrivateNetworks: cfg.AllowPrivateNetworks,
		allowProxyEnv:        cfg.AllowProxyEnv,
		maxRedirects:         cfg.MaxRedirects,
		allowedPorts:         allowedPorts,
		resolver:             net.DefaultResolver,
		dialer: &net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		},
	}
}

func NewSecureTransport(cfg Config, policy *NetworkPolicy) *http.Transport {
	return &http.Transport{
		Proxy:                  policy.Proxy,
		DialContext:            policy.DialContext,
		ForceAttemptHTTP2:      true,
		MaxIdleConns:           100,
		MaxIdleConnsPerHost:    cfg.WorkerCount + 2,
		IdleConnTimeout:        90 * time.Second,
		TLSHandshakeTimeout:    10 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: cfg.MaxHeaderBytes,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}
}

func (p *NetworkPolicy) ValidateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	return p.ValidateParsedURL(parsed)
}

func (p *NetworkPolicy) ValidateParsedURL(parsed *url.URL) error {
	if parsed == nil {
		return fmt.Errorf("URL is empty")
	}

	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return fmt.Errorf("scheme %q is not allowed", parsed.Scheme)
	}
	if parsed.User != nil {
		return fmt.Errorf("userinfo is not allowed")
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("host is required")
	}
	normalizedHost := strings.TrimSuffix(strings.ToLower(host), ".")
	if normalizedHost == "localhost" || strings.HasSuffix(normalizedHost, ".localhost") {
		return fmt.Errorf("localhost names are not allowed")
	}
	if strings.Contains(host, "%") {
		return fmt.Errorf("zone identifiers are not allowed")
	}
	port, err := portForURL(parsed)
	if err != nil {
		return err
	}
	if !p.IsAllowedPort(port) {
		return fmt.Errorf("port %d is not allowed", port)
	}

	if ip, err := netip.ParseAddr(host); err == nil {
		if !p.IsAllowedIP(ip) {
			return fmt.Errorf("IP address %s is not allowed", ip)
		}
	}
	return nil
}

func (p *NetworkPolicy) CheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= p.maxRedirects {
		return fmt.Errorf("stopped after %d redirects", p.maxRedirects)
	}
	if err := p.ValidateParsedURL(req.URL); err != nil {
		return fmt.Errorf("blocked redirect to %s: %w", req.URL.Redacted(), err)
	}
	return nil
}

func (p *NetworkPolicy) Proxy(req *http.Request) (*url.URL, error) {
	if !p.allowProxyEnv {
		return nil, nil
	}

	proxyURL, err := http.ProxyFromEnvironment(req)
	if err != nil || proxyURL == nil {
		return proxyURL, err
	}
	if err := p.ValidateParsedURL(proxyURL); err != nil {
		return nil, fmt.Errorf("blocked proxy URL: %w", err)
	}
	return proxyURL, nil
}

func (p *NetworkPolicy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, fmt.Errorf("network %q is not allowed", network)
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return nil, fmt.Errorf("port is invalid")
	}
	if !p.IsAllowedPort(portNumber) {
		return nil, fmt.Errorf("port %d is not allowed", portNumber)
	}
	if strings.Contains(host, "%") {
		return nil, fmt.Errorf("zone identifiers are not allowed")
	}

	ips, err := p.ResolveAllowedIPs(ctx, host)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, ip := range ips {
		conn, err := p.dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no allowed addresses for %s", host)
}

func (p *NetworkPolicy) ResolveAllowedIPs(ctx context.Context, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		ip = ip.Unmap()
		if !p.IsAllowedIP(ip) {
			return nil, fmt.Errorf("IP address %s is not allowed", ip)
		}
		return []netip.Addr{ip}, nil
	}

	ips, err := p.resolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}

	allowed := make([]netip.Addr, 0, len(ips))
	seen := make(map[netip.Addr]struct{}, len(ips))
	for _, ip := range ips {
		ip = ip.Unmap()
		if _, exists := seen[ip]; exists {
			continue
		}
		seen[ip] = struct{}{}
		if p.IsAllowedIP(ip) {
			allowed = append(allowed, ip)
		}
	}
	if len(allowed) == 0 {
		return nil, fmt.Errorf("host %s resolved only to blocked addresses", host)
	}
	return allowed, nil
}

func (p *NetworkPolicy) IsAllowedIP(ip netip.Addr) bool {
	if !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() {
		return false
	}
	if p.allowPrivateNetworks {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return false
	}
	for _, prefix := range blockedNetworkPrefixes {
		if prefix.Contains(ip) {
			return false
		}
	}
	return true
}

func (p *NetworkPolicy) IsAllowedPort(port int) bool {
	_, exists := p.allowedPorts[port]
	return exists
}

func portForURL(parsed *url.URL) (int, error) {
	if parsed.Port() != "" {
		port, err := strconv.Atoi(parsed.Port())
		if err != nil || port < 1 || port > 65535 {
			return 0, fmt.Errorf("port is invalid")
		}
		return port, nil
	}

	switch strings.ToLower(parsed.Scheme) {
	case "http":
		return 80, nil
	case "https":
		return 443, nil
	default:
		return 0, fmt.Errorf("scheme %q is not allowed", parsed.Scheme)
	}
}

var blockedNetworkPrefixes = []netip.Prefix{
	mustPrefix("0.0.0.0/8"),
	mustPrefix("100.64.0.0/10"),
	mustPrefix("192.0.0.0/24"),
	mustPrefix("192.0.2.0/24"),
	mustPrefix("198.18.0.0/15"),
	mustPrefix("198.51.100.0/24"),
	mustPrefix("203.0.113.0/24"),
	mustPrefix("224.0.0.0/4"),
	mustPrefix("240.0.0.0/4"),
	mustPrefix("::/128"),
	mustPrefix("2001:db8::/32"),
	mustPrefix("ff00::/8"),
}

func mustPrefix(raw string) netip.Prefix {
	prefix, err := netip.ParsePrefix(raw)
	if err != nil {
		panic(err)
	}
	return prefix
}
