package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"
)

func cachedAttachment(t *testing.T, store *archive.Store, id, kind, mimeType string, data []byte) {
	t.Helper()
	if err := store.Ingest([]archive.Message{{ID: id, ChatJID: testChat, Sender: testChat, MediaType: kind, Timestamp: time.Now().Unix()}}); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	desc := archive.AttachmentDescriptor{DirectPath: "/v/file?token=private-download-marker", MediaKey: bytes.Repeat([]byte{7}, 32), FileSHA256: hash[:], FileEncSHA256: bytes.Repeat([]byte{8}, 32), FileLength: int64(len(data)), MediaType: kind, MIMEType: mimeType, Filename: "staff-file.bin"}
	if err := store.RegisterAttachment(testChat, id, desc); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingAttachments(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if job.MessageID == id {
			if err := store.CacheAttachment(context.Background(), job, func(ctx context.Context, f archive.AttachmentFile) error { _, err := f.Write(data); return err }); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("attachment not queued")
}

func attachmentGET(h http.Handler, path, token, byteRange string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil)
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if byteRange != "" {
		r.Header.Set("Range", byteRange)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestAttachmentHTTPAuthRangeAndRevocation(t *testing.T) {
	h, store := fixture(t, testSource{})
	payload := []byte("original spreadsheet bytes")
	cachedAttachment(t, store, "sheet", "document", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", payload)
	path := attachmentPath(testChat, "sheet")
	if w := attachmentGET(h, path, "", ""); w.Code != http.StatusUnauthorized || bytes.Contains(w.Body.Bytes(), payload) {
		t.Fatal("unauthenticated attachment access")
	}
	w := attachmentGET(h, path, testToken, "")
	if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), payload) {
		t.Fatalf("download: %d %s", w.Code, w.Body.String())
	}
	if !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("unsafe download headers")
	}
	w = attachmentGET(h, path, testToken, "bytes=9-19")
	if w.Code != http.StatusPartialContent || !bytes.Equal(w.Body.Bytes(), payload[9:20]) {
		t.Fatalf("range: %d %q", w.Code, w.Body.String())
	}
	if w := attachmentGET(h, path, testToken, "bytes=0-1,4-5"); w.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatal("multipart range accepted")
	}
	if w := attachmentGET(h, path+"&message_id=other", testToken, ""); w.Code != http.StatusBadRequest {
		t.Fatal("duplicate query accepted")
	}
	if err := store.ApplyConfig(config.Default()); err != nil {
		t.Fatal(err)
	}
	w = attachmentGET(h, path, testToken, "")
	if w.Code != http.StatusNotFound || bytes.Contains(w.Body.Bytes(), payload) {
		t.Fatal("removed attachment remained available")
	}
}

func TestAttachmentToolsAndResourcesExposeBytesWithoutPrivateRefs(t *testing.T) {
	h, store := fixture(t, testSource{})
	// Signature is sufficient for sniffing; the server never decodes the image.
	payload := append([]byte("\x89PNG\r\n\x1a\n"), bytes.Repeat([]byte{0}, 64)...)
	cachedAttachment(t, store, "photo", "image", "image/png", payload)
	w := request(h, "/mcp", testToken, call("get_attachment", map[string]any{"chat_jid": testChat, "message_id": "photo"}), nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"type":"image"`) || !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString(payload)) {
		t.Fatalf("inline image missing: %s", w.Body.String())
	}
	for _, secret := range []string{"private-download-marker", "media_key", "direct_path", "file_enc_sha256"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("private reference leaked: %s", secret)
		}
	}
	w = request(h, "/mcp", testToken, call("get_attachment", map[string]any{"chat_jid": testChat, "message_id": "photo", "metadata_only": true}), nil)
	if strings.Contains(w.Body.String(), `"type":"image"`) {
		t.Fatal("metadata-only returned image")
	}
	w = request(h, "/mcp", testToken, call("read_attachment", map[string]any{"chat_jid": testChat, "message_id": "photo", "offset": 8, "length": 10}), nil)
	var result struct {
		Result struct{ StructuredContent AttachmentChunk }
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	chunk := result.Result.StructuredContent
	if chunk.Data != base64.StdEncoding.EncodeToString(payload[8:18]) || chunk.NextOffset != 18 || chunk.EOF {
		t.Fatalf("invalid chunk: %s", w.Body.String())
	}
	resourceBody, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "resources/read", "params": map[string]string{"uri": attachmentURI(testChat, "photo")}})
	w = request(h, "/mcp", testToken, string(resourceBody), nil)
	if !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString(payload)) {
		t.Fatalf("resource bytes missing: %s", w.Body.String())
	}
	store.Suspend()
	for _, body := range []string{string(resourceBody), call("get_attachment", map[string]any{"chat_jid": testChat, "message_id": "photo"}), call("read_attachment", map[string]any{"chat_jid": testChat, "message_id": "photo"})} {
		w = request(h, "/mcp", testToken, body, nil)
		if strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString(payload)) || !strings.Contains(w.Body.String(), "unavailable") {
			t.Fatalf("suspended read: %s", w.Body.String())
		}
	}
}

func TestAttachmentInputBoundsAndURIs(t *testing.T) {
	h, _ := fixture(t, testSource{})
	for _, args := range []map[string]any{
		{"chat_jid": testChat, "message_id": "fixture", "offset": -1},
		{"chat_jid": testChat, "message_id": "fixture", "length": attachmentChunkLimit + 1},
		{"chat_jid": "*", "message_id": "fixture"},
	} {
		w := request(h, "/mcp", testToken, call("read_attachment", args), nil)
		if !strings.Contains(w.Body.String(), "error") && !strings.Contains(w.Body.String(), "isError") {
			t.Fatalf("invalid input accepted: %s", w.Body.String())
		}
	}
	for _, uri := range []string{"file:///etc/passwd", "whatsapp-attachment:///../secret", "whatsapp-attachment:///a/b/c", attachmentURI(testChat, "fixture") + "?anything"} {
		if _, _, err := parseAttachmentURI(uri); err == nil {
			t.Fatalf("invalid URI accepted %q", uri)
		}
	}
	chat, id, err := parseAttachmentURI(attachmentURI(testChat, "message/with?delimiters"))
	if err != nil || chat != testChat || id != "message/with?delimiters" {
		t.Fatal("URI roundtrip failed")
	}
}
