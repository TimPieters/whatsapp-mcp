package migration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"
)

const approvedChat = "123@s.whatsapp.net"
const deniedChat = "456@s.whatsapp.net"

func testConfig() config.Config {
	c := config.Default()
	c.AllowedChats = []config.Chat{{JID: approvedChat}}
	return c
}

func testArchive(t *testing.T) (*archive.Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "archive.db")
	s, err := archive.Open(path, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestExportSelectsApprovedContentAndDoesNotWriteSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "original.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE messages (id TEXT,chat_jid TEXT,sender TEXT,content TEXT,timestamp TIMESTAMP,is_from_me BOOLEAN,media_type TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Truncate(time.Second)
	for _, row := range []struct{ id, jid, text string }{
		{"a", approvedChat, "APPROVED_EXPORT_LITERAL"},
		{"b", deniedChat, "DENIED_EXPORT_LITERAL_4163"},
	} {
		if _, err := db.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?)`, row.id, row.jid, "123", row.text, stamp.Format("2006-01-02 15:04:05.999999999-07:00"), false, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	r, err := ExportUpstream(context.Background(), path, testConfig(), &output)
	if err != nil {
		t.Fatal(err)
	}
	if r.Exported != 1 || r.Skipped != 0 || !strings.Contains(output.String(), "APPROVED_EXPORT_LITERAL") || strings.Contains(output.String(), "DENIED_EXPORT_LITERAL_4163") {
		t.Fatalf("bad filtered export: %+v", r)
	}
	var msg archive.Message
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Timestamp != stamp.Unix() {
		t.Fatalf("timestamp: got %d want %d", msg.Timestamp, stamp.Unix())
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("source database changed")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("source sidecars created")
	}
	store, _ := testArchive(t)
	r, err = ImportJSONL(context.Background(), bytes.NewReader(output.Bytes()), store)
	if err != nil || r.Imported != 1 {
		t.Fatalf("round trip: %+v %v", r, err)
	}
}

func jsonLine(t *testing.T, jid, text string) string {
	t.Helper()
	data, err := json.Marshal(archive.Message{ID: "a", ChatJID: jid, Sender: "123", Text: text, Timestamp: time.Now().Unix()})
	if err != nil {
		t.Fatal(err)
	}
	return string(data) + "\n"
}

func TestImportDeniedRecordsNeverReachDisk(t *testing.T) {
	s, path := testArchive(t)
	secret := "DENIED_IMPORT_LITERAL_189645"
	input := jsonLine(t, deniedChat, secret) + jsonLine(t, approvedChat, "approved")
	r, err := ImportJSONL(context.Background(), strings.NewReader(input), s)
	if err != nil || r.Imported != 1 || r.Skipped != 1 {
		t.Fatalf("import: %+v %v", r, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(secret)) {
		t.Fatal("denied payload persisted")
	}
}

func TestImportRejectsMalformedLineAndCountsCommittedPrefix(t *testing.T) {
	for _, bad := range []string{
		`{"unknown":"PRIVATE_LITERAL"}`, `[]`, `{} {}`,
		`{"id":"x","id":"y"}`, `{"ID":"x"}`, `{"id":null}`,
		strings.Repeat("x", MaxLineBytes+1),
	} {
		s, _ := testArchive(t)
		r, err := ImportJSONL(context.Background(), strings.NewReader(jsonLine(t, approvedChat, "prefix")+bad+"\n"), s)
		if err == nil || !strings.Contains(err.Error(), "line 2") || strings.Contains(err.Error(), "PRIVATE_LITERAL") {
			t.Fatalf("expected content-free line error, got %v", err)
		}
		if r.Imported != 1 {
			t.Fatalf("prefix count: %+v", r)
		}
		stats, err := s.Stats()
		if err != nil || stats.Messages != 1 {
			t.Fatalf("committed prefix: %+v %v", stats, err)
		}
	}
}

func TestTimestampFormats(t *testing.T) {
	want := time.Date(2026, 10, 30, 15, 4, 5, 0, time.UTC).Unix()
	for _, stamp := range []string{
		"2026-10-30T15:04:05Z", "2026-10-30 16:04:05+01:00",
		"2026-10-30 16:04:05 +0100 CET", "2026-10-30 15:04:05",
	} {
		got, ok := parseTimestamp(stamp)
		if !ok || got != want {
			t.Fatalf("timestamp %q: %d %v", stamp, got, ok)
		}
	}
}

func TestExportDenyAllAndMissingSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	if _, err := ExportUpstream(context.Background(), path, testConfig(), &bytes.Buffer{}); err == nil {
		t.Fatal("missing source accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("missing source was created")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE unrelated (value TEXT)"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	var output bytes.Buffer
	report, err := ExportUpstream(context.Background(), path, config.Default(), &output)
	if err != nil || report.Exported != 0 || output.Len() != 0 {
		t.Fatalf("empty policy queried message contents: %+v %v", report, err)
	}
}

func TestImportRejectsUnavailableArchiveAndCancelledContext(t *testing.T) {
	s, _ := testArchive(t)
	s.Suspend()
	if _, err := ImportJSONL(context.Background(), strings.NewReader(""), s); err == nil {
		t.Fatal("suspended archive accepted import")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ImportJSONL(ctx, strings.NewReader(""), s); err != context.Canceled {
		t.Fatalf("cancelled import: %v", err)
	}
}
