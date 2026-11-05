package collector

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	"whatsapp-client/internal/archive"
)

func trustedMediaHost(host string) bool {
	host = strings.ToLower(host)
	for _, domain := range []string{"whatsapp.net", "whatsapp.com", "fbcdn.net"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

type mediaGuard struct{ transport http.RoundTripper }
type mediaFileContextKey struct{}

func (g mediaGuard) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodGet || request.URL.Scheme != "https" || request.URL.User != nil || !trustedMediaHost(request.URL.Hostname()) || (request.URL.Port() != "" && request.URL.Port() != "443") {
		return nil, errors.New("untrusted media endpoint")
	}
	// The pinned downloader can move to another media host after an integrity
	// failure without resetting its file. Every full GET must start empty so a
	// retry cannot append a second ciphertext and hide the first integrity error.
	if file, ok := request.Context().Value(mediaFileContextKey{}).(archive.AttachmentFile); ok {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		if err := file.Truncate(0); err != nil {
			return nil, err
		}
	}
	return g.transport.RoundTrip(request)
}

func publicMediaIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	// Shared carrier space is not classified as private by netip.
	return !netip.MustParsePrefix("100.64.0.0/10").Contains(ip)
}

func newMediaHTTPClient() (*http.Client, *http.Transport) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Media URLs are constructed by the library from authenticated server hosts.
	// Do not let proxy environment variables or redirects expand that boundary.
	transport.Proxy = nil
	transport.MaxIdleConns = 2
	transport.MaxIdleConnsPerHost = 1
	transport.MaxConnsPerHost = 1
	transport.MaxResponseHeaderBytes = 64 * 1024
	transport.ResponseHeaderTimeout = 15 * time.Second
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || port != "443" || !trustedMediaHost(host) {
			return nil, errors.New("untrusted media endpoint")
		}
		addresses, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, errors.New("media endpoint unavailable")
		}
		dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
		for _, ip := range addresses {
			if !publicMediaIP(ip) {
				continue
			}
			connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return connection, nil
			}
		}
		return nil, errors.New("media endpoint unavailable")
	}
	return &http.Client{
		Transport: mediaGuard{transport}, Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("media redirects are disabled") },
	}, transport
}
