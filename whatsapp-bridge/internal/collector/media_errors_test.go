package collector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"go.mau.fi/whatsmeow"
	"whatsapp-client/internal/archive"
)

func TestAttachmentErrorsUseSanitizedClosedVocabulary(t *testing.T) {
	for _, test := range []struct {
		err    error
		reason string
	}{
		{context.DeadlineExceeded, "timeout"}, {context.Canceled, "cancelled"},
		{whatsmeow.ErrInvalidMediaHMAC, "invalid_hmac"}, {whatsmeow.ErrInvalidMediaEncSHA256, "invalid_encrypted_hash"},
		{whatsmeow.ErrInvalidMediaSHA256, "invalid_plaintext_hash"}, {archive.ErrNotAllowed, "policy_or_stale"},
		{whatsmeow.DownloadHTTPError{Response: &http.Response{StatusCode: 410}}, "http_410"},
		{errors.New("attachment exceeds configured size limit"), "write_limit"},
		{errors.New("attachment seek exceeds bounds"), "seek_limit"},
		{errors.New("attachment truncate exceeds bounds"), "truncate_limit"},
		{errors.New("attachment size does not match descriptor"), "plaintext_size"},
		{errors.New("attachment integrity check failed"), "cache_integrity"},
		{errors.New("untrusted media endpoint"), "untrusted_endpoint"},
		{errors.New("private identifier and secret URL marker"), "unavailable"},
	} {
		err := fmt.Errorf("https://private.invalid/secret-token: %w", test.err)
		if actual := attachmentErrorReason(err); actual != test.reason {
			t.Fatalf("expected fixed reason %s, got %s", test.reason, actual)
		}
	}
}
