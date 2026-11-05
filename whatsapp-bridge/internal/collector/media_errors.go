package collector

import (
	"context"
	"errors"
	"net"
	"strings"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow"
)

// attachmentErrorReason returns a closed vocabulary, never raw error text.
func attachmentErrorReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, whatsmeow.ErrInvalidMediaHMAC):
		return "invalid_hmac"
	case errors.Is(err, whatsmeow.ErrInvalidMediaEncSHA256):
		return "invalid_encrypted_hash"
	case errors.Is(err, whatsmeow.ErrInvalidMediaSHA256), errors.Is(err, whatsmeow.ErrInvalidUnencryptedMediaSHA256):
		return "invalid_plaintext_hash"
	case errors.Is(err, archive.ErrNotAllowed), errors.Is(err, archive.ErrSuspended), errors.Is(err, archive.ErrClosed), errors.Is(err, archive.ErrAttachmentUnavailable):
		return "policy_or_stale"
	}
	var httpError whatsmeow.DownloadHTTPError
	if errors.As(err, &httpError) && httpError.Response != nil {
		switch status := httpError.Response.StatusCode; {
		case status == 403:
			return "http_403"
		case status == 404:
			return "http_404"
		case status == 410:
			return "http_410"
		case status >= 400 && status < 500:
			return "http_4xx"
		case status >= 500 && status < 600:
			return "http_5xx"
		default:
			return "http_other"
		}
	}
	// These constants originate in the bounded local cache and guarded transport.
	// Matching them does not disclose any URL, identifier, path, or secret value.
	message := err.Error()
	if strings.Contains(message, "attachment exceeds configured size limit") {
		return "write_limit"
	}
	if strings.Contains(message, "attachment seek exceeds bounds") {
		return "seek_limit"
	}
	if strings.Contains(message, "attachment truncate exceeds bounds") {
		return "truncate_limit"
	}
	if strings.Contains(message, "attachment size does not match descriptor") {
		return "plaintext_size"
	}
	if strings.Contains(message, "attachment integrity check failed") {
		return "cache_integrity"
	}
	if strings.Contains(message, "attachment cache budget is exhausted") {
		return "cache_budget"
	}
	if strings.Contains(message, "untrusted media endpoint") || strings.Contains(message, "media redirects are disabled") {
		return "untrusted_endpoint"
	}
	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return "timeout"
		}
		return "network_transport"
	}
	return "unavailable"
}
