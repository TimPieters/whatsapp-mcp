package migration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
)

func referenceFixture() mediaReference {
	return mediaReference{DirectPath: "/v/t62.7118-24/fixture.enc?token=fixture", MediaKey: bytes.Repeat([]byte{1}, 32), FileSHA256: bytes.Repeat([]byte{2}, 32), FileEncSHA256: bytes.Repeat([]byte{3}, 32), FileLength: 1024, MediaType: "document", Filename: "Budget.xlsx", MIMEType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
}

func TestSelectedMediaExportAndImportWithoutNetwork(t *testing.T) {
	database := filepath.Join(t.TempDir(), "source.db")
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE messages(id TEXT,chat_jid TEXT,sender TEXT,content TEXT,timestamp INTEGER,is_from_me BOOLEAN,media_type TEXT,filename TEXT,url TEXT,media_key BLOB,file_sha256 BLOB,file_enc_sha256 BLOB,file_length INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	ref := referenceFixture()
	deniedKey := bytes.Repeat([]byte{99}, 32)
	for _, row := range []struct {
		id, chat, url string
		key           []byte
	}{
		{"approved", approvedChat, "https://mmg.whatsapp.net" + ref.DirectPath, ref.MediaKey},
		{"denied", deniedChat, "https://mmg.whatsapp.net/v/DENIED_MEDIA_PATH.enc?secret=not-exported", deniedKey},
		{"unsafe-url", approvedChat, "https://127.0.0.1/private", ref.MediaKey},
		{"incomplete", approvedChat, "https://mmg.whatsapp.net/v/incomplete.enc?x=1", []byte{1}},
	} {
		_, err = db.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, row.id, row.chat, "123", "Workbook", time.Now().Unix(), false, "document", ref.Filename, row.url, row.key, ref.FileSHA256, ref.FileEncSHA256, ref.FileLength)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	report, err := ExportUpstream(context.Background(), database, testConfig(), &output)
	if err != nil {
		t.Fatal(err)
	}
	if report.Exported != 3 || report.AttachmentRefs != 1 || report.AttachmentRefsSkipped != 2 {
		t.Fatalf("unexpected export report %+v", report)
	}
	for _, excluded := range []string{"DENIED_MEDIA_PATH", base64.StdEncoding.EncodeToString(deniedKey), "https://", "127.0.0.1"} {
		if strings.Contains(output.String(), excluded) {
			t.Fatalf("unsafe or unapproved metadata exported: %s", excluded)
		}
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	for _, line := range lines {
		m, raw, err := decodeRecord([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		if m.ID == "approved" {
			got, err := decodeReference(raw, "document")
			if err != nil {
				t.Fatal(err)
			}
			if got.DirectPath != ref.DirectPath || !bytes.Equal(got.MediaKey, ref.MediaKey) || got.MIMEType != ref.MIMEType {
				t.Fatalf("descriptor round trip mismatch %+v", got)
			}
		} else if len(raw) != 0 {
			t.Fatalf("invalid media retained descriptor for %s", m.ID)
		}
	}
	store, _ := testArchive(t)
	report, err = ImportJSONL(context.Background(), bytes.NewReader(output.Bytes()), store)
	if err != nil || report.Imported != 3 || report.AttachmentRefs != 1 {
		t.Fatalf("import report %+v err=%v", report, err)
	}
	jobs, err := store.PendingAttachments(5)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "approved" || jobs[0].Descriptor.DirectPath != ref.DirectPath || !bytes.Equal(jobs[0].Descriptor.MediaKey, ref.MediaKey) {
		t.Fatalf("imported descriptor unavailable: %+v %v", jobs, err)
	}
	public, err := store.ListMessages(approvedChat, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{ref.DirectPath, base64.StdEncoding.EncodeToString(ref.MediaKey), "media_key", "file_enc_sha256"} {
		if bytes.Contains(data, []byte(private)) {
			t.Fatalf("private descriptor leaked in message results: %s", private)
		}
	}
}

func TestImportPreservesDescriptorAboveCurrentLimitAsOversized(t *testing.T) {
	cfg := testConfig()
	cfg.MaxAttachmentBytes = 512
	store, err := archive.Open(filepath.Join(t.TempDir(), "archive.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ref := referenceFixture()
	m := exportRecord{Message: archive.Message{ID: "oversized", ChatJID: approvedChat, Sender: "123", MediaType: "document", Timestamp: time.Now().Unix()}, Reference: &ref}
	data, _ := json.Marshal(m)
	report, err := ImportJSONL(context.Background(), bytes.NewReader(append(data, '\n')), store)
	if err != nil || report.Imported != 1 || report.AttachmentRefs != 1 {
		t.Fatalf("oversized metadata import %+v %v", report, err)
	}
	info, err := store.GetAttachmentInfo(approvedChat, "oversized")
	if err != nil || info.Status != "oversized" {
		t.Fatalf("oversized metadata lost: %+v %v", info, err)
	}
	if jobs, err := store.PendingAttachments(1); err != nil || len(jobs) != 0 {
		t.Fatalf("oversized attachment became eligible: %+v %v", jobs, err)
	}
	cfg.MaxAttachmentBytes = 2048
	if err := store.ApplyConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if jobs, err := store.PendingAttachments(1); err != nil || len(jobs) != 1 || !bytes.Equal(jobs[0].Descriptor.MediaKey, ref.MediaKey) {
		t.Fatalf("raised limit did not recover descriptor: %+v %v", jobs, err)
	}
}

func TestMediaPathsRejectArbitraryHostsAndAuthority(t *testing.T) {
	for _, value := range []string{
		"http://mmg.whatsapp.net/v/a.enc", "https://evil.example/v/a.enc", "https://whatsapp.net.evil.example/v/a.enc", "https://127.0.0.1/v/a.enc", "https://user@mmg.whatsapp.net/v/a.enc", "https://mmg.whatsapp.net:8443/v/a.enc", "https://mmg.whatsapp.net/v/a.enc#fragment", "https://mmg.whatsapp.net/v/%2e%2e/private", "https://mmg.whatsapp.net/private", "https://mmg.whatsapp.net/v/%5cevil", "https://mmg.whatsapp.net/v/%0aevil",
	} {
		if _, ok := directPathFromURL(value); ok {
			t.Fatalf("accepted unsafe source URL %q", value)
		}
	}
	for _, value := range []string{"//evil.example/v/a", "https://evil.example/v/a", "/v/../a", "/v/%2e%2e/a", "/v/a#fragment", "/v/a\\b", "/v/a\nb", "/v/%0ab", "/mms/./file", "/other/file"} {
		if validDirectPath(value) {
			t.Fatalf("accepted unsafe path %q", value)
		}
	}
	for _, value := range []string{"https://mmg.whatsapp.net/v/a.enc?token=1", "https://media.fbcdn.net/mms/a.enc?token=1"} {
		if _, ok := directPathFromURL(value); !ok {
			t.Fatalf("rejected legitimate CDN path %q", value)
		}
	}
}

func TestDuplicateImportKeyUsesLastAttachmentStateWithinBatch(t *testing.T) {
	ref := referenceFixture()
	first := exportRecord{Message: archive.Message{ID: "duplicate", ChatJID: approvedChat, Sender: "123", Text: "old", MediaType: "document", Timestamp: time.Now().Unix()}, Reference: &ref}
	last := first
	last.Reference = nil
	last.Text = "replacement without credentials"
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(last)
	store, _ := testArchive(t)
	input := append(append(append(a, '\n'), b...), '\n')
	report, err := ImportJSONL(context.Background(), bytes.NewReader(input), store)
	if err != nil || report.Imported != 2 || report.AttachmentRefs != 0 {
		t.Fatalf("duplicate import report %+v %v", report, err)
	}
	if _, err := store.GetAttachmentInfo(approvedChat, "duplicate"); err == nil {
		t.Fatal("earlier duplicate descriptor survived text-only replacement")
	}
	rows, err := store.ListMessages(approvedChat, 0, 5)
	if err != nil || len(rows) != 1 || rows[0].Text != last.Text {
		t.Fatalf("last duplicate did not win: %+v %v", rows, err)
	}
}

func TestMalformedPrivateReferencesFailWithoutLeakingAndDeniedRefsAreIgnored(t *testing.T) {
	ref := referenceFixture()
	for _, mutate := range []func(*mediaReference){
		func(r *mediaReference) { r.DirectPath = "https://127.0.0.1/PRIVATE_REFERENCE_LITERAL" },
		func(r *mediaReference) { r.Filename = "../PRIVATE_REFERENCE_LITERAL.xlsx" },
		func(r *mediaReference) { r.MediaKey = []byte("PRIVATE_REFERENCE_LITERAL") },
		func(r *mediaReference) { r.MediaType = "image" },
		func(r *mediaReference) { r.FileLength = -1 },
	} {
		bad := ref
		mutate(&bad)
		msg := map[string]any{"id": "media", "chat_jid": approvedChat, "sender": "123", "media_type": "document", "timestamp": time.Now().Unix(), "attachment_ref": bad}
		data, _ := json.Marshal(msg)
		store, _ := testArchive(t)
		_, err := ImportJSONL(context.Background(), bytes.NewReader(append(data, '\n')), store)
		if err == nil || strings.Contains(err.Error(), "PRIVATE_REFERENCE_LITERAL") {
			t.Fatalf("invalid descriptor error %v", err)
		}
		msg["chat_jid"] = deniedChat
		data, _ = json.Marshal(msg)
		report, err := ImportJSONL(context.Background(), bytes.NewReader(append(data, '\n')), store)
		if err != nil || report.Imported != 0 || report.Skipped != 1 {
			t.Fatalf("denied descriptor was interpreted %+v %v", report, err)
		}
	}
	data, _ := json.Marshal(ref)
	for _, bad := range []string{strings.TrimSuffix(string(data), "}") + `,"media_key":"duplicate"}`, strings.TrimSuffix(string(data), "}") + `,"url":"https://evil.example"}`, `{"direct_path":null}`} {
		if _, err := decodeReference([]byte(bad), "document"); err == nil {
			t.Fatal("accepted ambiguous private descriptor")
		}
	}
}
