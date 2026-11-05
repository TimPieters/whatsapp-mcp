package service

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/collector"
	"whatsapp-client/internal/config"
)

const testToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
const testChat = "15551234567@s.whatsapp.net"

type testSource struct {
	entered chan struct{}
	release chan struct{}
}

func (s testSource) Status() collector.Status {
	if s.entered != nil {
		s.entered <- struct{}{}
		<-s.release
	}
	return collector.Status{Connected: true, Paired: true}
}

func fixture(t *testing.T, source StatusSource) (http.Handler, *archive.Store) {
	t.Helper()
	cfg := config.Default()
	cfg.AllowedChats = []config.Chat{{JID: testChat, Label: "Chosen chat"}}
	store, err := archive.Open(filepath.Join(t.TempDir(), "archive.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	if err := store.Ingest([]archive.Message{{ID: "fixture", ChatJID: testChat, Sender: testChat, Text: "approved-private-marker", Timestamp: time.Now().Unix()}}); err != nil {
		t.Fatal(err)
	}
	return New(store, source, testToken, "test"), store
}

func request(h http.Handler, path, token, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "http://localhost"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	r.Header.Set("MCP-Protocol-Version", "2025-06-18")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func call(name string, arguments any) string {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{"name": name, "arguments": arguments}})
	return string(b)
}

func TestAuthorizationCoversEveryPathAndRejectsQueryToken(t *testing.T) {
	h, _ := fixture(t, testSource{})
	for _, path := range []string{"/mcp", "/qr", "/pair", "/admin", "/health", "/mcp?token=" + testToken} {
		for _, token := range []string{"", "wrong"} {
			w := request(h, path, token, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, nil)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("%s token=%q status=%d", path, token, w.Code)
			}
			if strings.Contains(w.Body.String(), "private-marker") {
				t.Fatal("unauthenticated response leaked content")
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("missing no-store")
			}
		}
	}
	for _, path := range []string{"/qr", "/pair", "/admin", "/reload", "/token"} {
		w := request(h, path, testToken, `{}`, nil)
		if w.Code != http.StatusNotFound {
			t.Fatalf("authenticated token exposed unexpected admin route %s: %d", path, w.Code)
		}
	}
}

func TestOnlyReadToolsAndCurrentChatPolicyAreExposed(t *testing.T) {
	h, store := fixture(t, testSource{})
	w := request(h, "/mcp", testToken, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", w.Code, w.Body.String())
	}
	var list struct {
		Result struct{ Tools []struct{ Name string } }
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"whatsapp_status": true, "list_chats": true, "list_messages": true, "search_messages": true}
	if len(list.Result.Tools) != len(want) {
		t.Fatalf("unexpected tool count: %s", w.Body.String())
	}
	for _, tool := range list.Result.Tools {
		if !want[tool.Name] {
			t.Fatalf("unexpected tool %q", tool.Name)
		}
	}
	w = request(h, "/mcp", testToken, call("list_messages", map[string]any{"chat_jid": testChat}), nil)
	if !strings.Contains(w.Body.String(), "approved-private-marker") {
		t.Fatalf("approved read failed: %s", w.Body.String())
	}
	for _, name := range []string{"send_message", "send_text", "pair", "reload_config", "import_history"} {
		w = request(h, "/mcp", testToken, call(name, map[string]any{}), nil)
		if !strings.Contains(w.Body.String(), "error") || strings.Contains(w.Body.String(), "approved-private-marker") {
			t.Fatalf("unsafe tool response for %s: %s", name, w.Body.String())
		}
	}
	w = request(h, "/mcp", testToken, call("list_messages", map[string]any{"chat_jid": "15559999999@s.whatsapp.net"}), nil)
	if !strings.Contains(w.Body.String(), "chat is not approved") || strings.Contains(w.Body.String(), "approved-private-marker") {
		t.Fatalf("unapproved read: %s", w.Body.String())
	}
	cfg := config.Default()
	if err := store.ApplyConfig(cfg); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{call("list_messages", map[string]any{"chat_jid": testChat}), call("search_messages", map[string]any{"query": "private-marker"}), call("list_chats", map[string]any{})} {
		w = request(h, "/mcp", testToken, body, nil)
		if strings.Contains(w.Body.String(), "approved-private-marker") || strings.Contains(w.Body.String(), "Chosen chat") {
			t.Fatalf("removed chat leaked: %s", w.Body.String())
		}
	}
	store.Suspend()
	w = request(h, "/mcp", testToken, call("whatsapp_status", map[string]any{}), nil)
	if !strings.Contains(w.Body.String(), "archive access is unavailable") {
		t.Fatalf("suspended archive remained readable: %s", w.Body.String())
	}
}

func TestRequestBodyAndCrossOriginAreRejected(t *testing.T) {
	h, _ := fixture(t, testSource{})
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/list","padding":"` + strings.Repeat("a", 65*1024) + `"}`
	w := request(h, "/mcp", testToken, body, nil)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize status=%d body=%s", w.Code, w.Body.String())
	}
	w = request(h, "/mcp", testToken, call("list_chats", map[string]any{}), map[string]string{"Origin": "https://attacker.example", "Sec-Fetch-Site": "cross-site"})
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-origin status=%d", w.Code)
	}
	if bytes.Contains(w.Body.Bytes(), []byte("approved-private-marker")) {
		t.Fatal("cross-origin content leak")
	}
}

func TestAuthenticatedConcurrencyIsBounded(t *testing.T) {
	source := testSource{entered: make(chan struct{}, 4), release: make(chan struct{})}
	h, _ := fixture(t, source)
	var workers sync.WaitGroup
	defer workers.Wait()
	defer close(source.release)
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			request(h, "/mcp", testToken, call("whatsapp_status", map[string]any{}), nil)
		}()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-source.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("requests did not reach status source")
		}
	}
	w := request(h, "/mcp", testToken, call("whatsapp_status", map[string]any{}), nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("fifth concurrent request status=%d", w.Code)
	}
}
