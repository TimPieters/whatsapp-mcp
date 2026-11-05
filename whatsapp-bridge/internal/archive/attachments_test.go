package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"whatsapp-client/internal/config"
)

func syntheticDescriptor(payload []byte) AttachmentDescriptor {
	hash := sha256.Sum256(payload)
	return AttachmentDescriptor{DirectPath: "/v/synthetic?token=PRIVATE_ATTACHMENT_REFERENCE", MediaKey: bytes.Repeat([]byte{7}, 32), FileSHA256: hash[:], FileEncSHA256: bytes.Repeat([]byte{8}, 32), FileLength: int64(len(payload)), MediaType: "document", MIMEType: "application/octet-stream", Filename: "synthetic.xlsx"}
}

func seedAttachment(t *testing.T, s *Store, id string, payload []byte) AttachmentJob {
	t.Helper()
	m := message(id, approved, "")
	m.MediaType = "document"
	if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{{ChatJID: approved, MessageID: id, Descriptor: syntheticDescriptor(payload)}}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(32)
	if err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs {
		if job.MessageID == id {
			return job
		}
	}
	t.Fatal("attachment was not queued")
	return AttachmentJob{}
}

func cacheSynthetic(t *testing.T, s *Store, id string, payload []byte) {
	t.Helper()
	job := seedAttachment(t, s, id, payload)
	if err := s.CacheAttachment(context.Background(), job, func(_ context.Context, file AttachmentFile) error { _, err := file.Write(payload); return err }); err != nil {
		t.Fatal(err)
	}
}

func assertCacheEmpty(t *testing.T, s *Store) {
	t.Helper()
	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("cache retained %d entries", len(entries))
	}
}

func TestAttachmentApprovalRetentionAndPrivateMetadata(t *testing.T) {
	s, path := openTestStore(t, approvedConfig())
	denied := message("denied", excluded, "EXCLUDED_ATTACHMENT_BODY")
	denied.MediaType = "document"
	desc := syntheticDescriptor([]byte("synthetic bytes"))
	desc.DirectPath = "/v/EXCLUDED_ATTACHMENT_REFERENCE"
	old := message("old", approved, "")
	old.MediaType = "document"
	old.Timestamp -= 100 * 86400
	if err := s.IngestWithAttachments([]Message{denied, old}, []AttachmentRecord{{ChatJID: excluded, MessageID: denied.ID, Descriptor: desc}, {ChatJID: approved, MessageID: old.ID, Descriptor: desc}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterAttachment(excluded, denied.ID, desc); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("denied descriptor: %v", err)
	}
	if err := s.RegisterAttachment(approved, old.ID, desc); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("expired descriptor: %v", err)
	}
	assertAbsentOnDisk(t, path, "EXCLUDED_ATTACHMENT_REFERENCE")
	assertAbsentOnDisk(t, path, "EXCLUDED_ATTACHMENT_BODY")
	payload := []byte("synthetic spreadsheet bytes")
	cacheSynthetic(t, s, "approved", payload)
	got, err := s.ListMessages(approved, 0, 10)
	if err != nil || len(got) != 1 || got[0].Attachment == nil || got[0].Attachment.Status != "ready" {
		t.Fatalf("public metadata: %+v %v", got, err)
	}
	public, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(public, []byte("PRIVATE_ATTACHMENT_REFERENCE")) || bytes.Contains(public, []byte("media_key")) || bytes.Contains(public, []byte("direct_path")) {
		t.Fatal("public messages exposed private attachment credentials")
	}
	if err := s.WithAttachment(approved, "approved", func(info AttachmentInfo, r io.ReadSeeker) error {
		data, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if !bytes.Equal(data, payload) || info.Filename != "synthetic.xlsx" {
			t.Fatal("incorrect cached bytes or filename")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyConfig(configWithoutChats()); err != nil {
		t.Fatal(err)
	}
	if err := s.WithAttachment(approved, "approved", func(AttachmentInfo, io.ReadSeeker) error { t.Fatal("revoked cache was read"); return nil }); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("revoked read: %v", err)
	}
	assertCacheEmpty(t, s)
	assertAbsentOnDisk(t, path, "PRIVATE_ATTACHMENT_REFERENCE")
}

func configWithoutChats() config.Config { c := approvedConfig(); c.AllowedChats = nil; return c }

func TestAttachmentInflightRevocationClosesAndDiscards(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	job := seedAttachment(t, s, "inflight", []byte("synthetic payload"))
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.CacheAttachment(context.Background(), job, func(ctx context.Context, f AttachmentFile) error {
			if _, err := f.Write([]byte("synthetic")); err != nil {
				return err
			}
			close(started)
			<-ctx.Done()
			if _, err := f.Write([]byte("forbidden after revoke")); err == nil {
				return errors.New("revoked writer remained active")
			}
			return ctx.Err()
		})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("download did not start")
	}
	if err := s.ApplyConfig(configWithoutChats()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("revoked download committed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("revoked download did not cancel")
	}
	assertCacheEmpty(t, s)
	if err := s.CacheAttachment(context.Background(), job, func(context.Context, AttachmentFile) error { t.Fatal("stale job started"); return nil }); err == nil {
		t.Fatal("stale job accepted")
	}
}

func TestAttachmentOversizedRefsSurviveLimitChanges(t *testing.T) {
	c := approvedConfig()
	c.MaxAttachmentBytes = 4
	c.AttachmentCacheBytes = 256
	s, _ := openTestStore(t, c)
	payload := []byte("synthetic")
	m := message("large", approved, "")
	m.MediaType = "document"
	if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{{ChatJID: approved, MessageID: m.ID, Descriptor: syntheticDescriptor(payload)}}); err != nil {
		t.Fatal(err)
	}
	info, err := s.GetAttachmentInfo(approved, m.ID)
	if err != nil || info.Status != "oversized" {
		t.Fatalf("oversized metadata: %+v %v", info, err)
	}
	if jobs, err := s.PendingAttachments(1); err != nil || len(jobs) != 0 {
		t.Fatalf("oversized download queued: %+v %v", jobs, err)
	}
	c.MaxAttachmentBytes = 128
	if err := s.ApplyConfig(c); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("raised limit did not queue preserved ref: %+v %v", jobs, err)
	}
	if err := s.CacheAttachment(context.Background(), jobs[0], func(_ context.Context, f AttachmentFile) error { _, err := f.Write(payload); return err }); err != nil {
		t.Fatal(err)
	}
	c.MaxAttachmentBytes = 4
	if err := s.ApplyConfig(c); err != nil {
		t.Fatal(err)
	}
	assertCacheEmpty(t, s)
	info, err = s.GetAttachmentInfo(approved, m.ID)
	if err != nil || info.Status != "oversized" {
		t.Fatalf("shrunk limit removed metadata: %+v %v", info, err)
	}
}

func TestAttachmentLRUEvictionAndExplicitRequeue(t *testing.T) {
	c := approvedConfig()
	c.MaxAttachmentBytes = 128
	c.AttachmentCacheBytes = 400
	s, _ := openTestStore(t, c)
	payload := bytes.Repeat([]byte("x"), 128)
	cacheSynthetic(t, s, "first", payload)
	cacheSynthetic(t, s, "second", payload)
	if err := s.WithAttachment(approved, "first", func(AttachmentInfo, io.ReadSeeker) error { return nil }); err != nil {
		t.Fatal(err)
	}
	cacheSynthetic(t, s, "third", payload)
	for id, want := range map[string]string{"first": "ready", "second": "evicted", "third": "ready"} {
		info, err := s.GetAttachmentInfo(approved, id)
		if err != nil || info.Status != want {
			t.Fatalf("%s status %+v %v", id, info, err)
		}
	}
	if jobs, err := s.PendingAttachments(32); err != nil || len(jobs) != 0 {
		t.Fatal("evicted content was automatically queued again")
	}
	if err := s.RequestAttachment(approved, "second"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "second" {
		t.Fatalf("explicit requeue: %+v %v", jobs, err)
	}
	var used int64
	if err := s.db.QueryRow("SELECT COALESCE(SUM(length),0) FROM attachments WHERE status='ready'").Scan(&used); err != nil {
		t.Fatal(err)
	}
	if used > c.AttachmentCacheBytes {
		t.Fatal("cache quota exceeded")
	}
}

func TestAttachmentFailureBoundsAndMissingCacheRecovery(t *testing.T) {
	c := approvedConfig()
	c.MaxAttachmentBytes = 128
	c.AttachmentCacheBytes = 400
	s, _ := openTestStore(t, c)
	job := seedAttachment(t, s, "bounded", []byte("small"))
	if err := s.CacheAttachment(context.Background(), job, func(_ context.Context, f AttachmentFile) error {
		if _, err := f.Write(bytes.Repeat([]byte("x"), int(job.Descriptor.FileLength+65))); err == nil {
			return errors.New("write limit was not enforced")
		}
		return errors.New("synthetic download failed")
	}); err == nil {
		t.Fatal("oversized download succeeded")
	}
	assertCacheEmpty(t, s)
	info, err := s.GetAttachmentInfo(approved, job.MessageID)
	if err != nil || info.Status != "failed" {
		t.Fatalf("failed status: %+v %v", info, err)
	}
	if jobs, err := s.PendingAttachments(1); err != nil || len(jobs) != 0 {
		t.Fatal("failure backoff was not enforced")
	}
	cacheSynthetic(t, s, "missing", []byte("payload"))
	var name string
	if err := s.db.QueryRow("SELECT cache_name FROM attachments WHERE message_id='missing'").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(s.cacheDir, name)); err != nil {
		t.Fatal(err)
	}
	if err := s.WithAttachment(approved, "missing", func(AttachmentInfo, io.ReadSeeker) error { t.Fatal("missing file was read"); return nil }); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("missing file read: %v", err)
	}
	info, err = s.GetAttachmentInfo(approved, "missing")
	if err != nil || info.Status != "evicted" {
		t.Fatalf("missing status: %+v %v", info, err)
	}
	if err := s.RequestAttachment(approved, "missing"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "missing" {
		t.Fatalf("missing cache requeue: %+v %v", jobs, err)
	}
}

func TestAttachmentRestartReplacementAndPathValidation(t *testing.T) {
	c := approvedConfig()
	s, path := openTestStore(t, c)
	cacheSynthetic(t, s, "ready", []byte("synthetic ready"))
	seedAttachment(t, s, "interrupted", []byte("synthetic pending"))
	if err := os.WriteFile(filepath.Join(s.cacheDir, ".download-orphan"), []byte("synthetic orphan"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	jobs, err := reopened.PendingAttachments(1)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "interrupted" {
		t.Fatalf("restart pending: %+v %v", jobs, err)
	}
	if _, err := os.Stat(filepath.Join(reopened.cacheDir, ".download-orphan")); !os.IsNotExist(err) {
		t.Fatal("restart retained orphan bytes")
	}
	replacement := message("ready", approved, "replacement text")
	if err := reopened.Ingest([]Message{replacement}); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.GetAttachmentInfo(approved, "ready"); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("replacement retained old attachment: %v", err)
	}
	for _, badPath := range []string{"https://example.test/file", "//example.test/file", "/v/../secret", "/v/%2e%2e/secret", "/v/\\secret", "/v/file\r\n"} {
		d := syntheticDescriptor([]byte("x"))
		d.DirectPath = badPath
		if err := ValidateAttachmentDescriptor(d, 128); err == nil {
			t.Errorf("accepted path %q", badPath)
		}
	}
	for _, filename := range []string{"../secret", "..\\secret", "C:secret", "line\nname", strings.Repeat("x", 256)} {
		d := syntheticDescriptor([]byte("x"))
		d.Filename = filename
		if err := ValidateAttachmentDescriptor(d, 128); err == nil {
			t.Errorf("accepted filename %q", filename)
		}
	}
}

func TestAttachmentSameTypeReplacementAndIdenticalReplay(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	payload := []byte("original synthetic document")
	cacheSynthetic(t, s, "same", payload)
	m := message("same", approved, "updated caption")
	m.MediaType = "document"
	ref := AttachmentRecord{ChatJID: approved, MessageID: m.ID, Descriptor: syntheticDescriptor(payload)}
	if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{ref}); err != nil {
		t.Fatal(err)
	}
	info, err := s.GetAttachmentInfo(approved, m.ID)
	if err != nil || info.Status != "ready" {
		t.Fatalf("identical replay discarded cached bytes: %+v %v", info, err)
	}
	ref.Descriptor.MediaKey = nil
	if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{ref}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAttachmentInfo(approved, m.ID); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("invalid replacement retained stale bytes: %v", err)
	}
	assertCacheEmpty(t, s)
	cacheSynthetic(t, s, "same", payload)
	if err := s.Ingest([]Message{m}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAttachmentInfo(approved, m.ID); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("descriptor-free replacement retained stale bytes: %v", err)
	}
	assertCacheEmpty(t, s)
}

func TestAttachmentRequestPriorityAndEarlyFailureRecovery(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	var messages []Message
	var refs []AttachmentRecord
	for i, id := range []string{"oldest", "recent", "newest"} {
		m := message(id, approved, "")
		m.MediaType = "document"
		m.Timestamp -= int64(30 - i)
		messages = append(messages, m)
		refs = append(refs, AttachmentRecord{ChatJID: approved, MessageID: id, Descriptor: syntheticDescriptor([]byte("x"))})
	}
	if err := s.IngestWithAttachments(messages, refs); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestAttachment(approved, "oldest"); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "oldest" {
		t.Fatalf("explicit request priority: %+v %v", jobs, err)
	}
	if err := s.CacheAttachment(context.Background(), jobs[0], nil); err == nil {
		t.Fatal("nil download callback accepted")
	}
	info, err := s.GetAttachmentInfo(approved, "oldest")
	if err != nil || info.Status != "failed" {
		t.Fatalf("claim remained stuck: %+v %v", info, err)
	}
	jobs, err = s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 || jobs[0].MessageID != "newest" {
		t.Fatalf("new upload priority: %+v %v", jobs, err)
	}
}

func TestAttachmentCachePurgedWithRetentionAndMessageCap(t *testing.T) {
	c := approvedConfig()
	c.MaxMessagesPerChat = 1
	s, _ := openTestStore(t, c)
	cacheSynthetic(t, s, "old", []byte("synthetic old bytes"))
	m := message("new", approved, "new message")
	m.Timestamp++
	if err := s.Ingest([]Message{m}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAttachmentInfo(approved, "old"); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("capped message attachment survived: %v", err)
	}
	assertCacheEmpty(t, s)
	cacheSynthetic(t, s, "new", []byte("synthetic expired bytes"))
	if _, err := s.db.Exec("UPDATE messages SET timestamp=? WHERE id='new'", time.Now().Unix()-100*86400); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetAttachmentInfo(approved, "new"); !errors.Is(err, ErrAttachmentUnavailable) {
		t.Fatalf("expired attachment exposed: %v", err)
	}
	if err := s.Maintain(); err != nil {
		t.Fatal(err)
	}
	assertCacheEmpty(t, s)
}
