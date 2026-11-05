package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestRestoreExactAttachmentsPreservesVerifiedBytes(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("local-%d", i)
		payload := []byte(fmt.Sprintf("synthetic local original %d", i))
		m := message(id, approved, "")
		m.MediaType = "document"
		if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{{ChatJID: approved, MessageID: id, Descriptor: syntheticDescriptor(payload)}}); err != nil {
			t.Fatal(err)
		}
		if err := s.RestoreAttachment(context.Background(), approved, id, bytes.NewReader(payload)); err != nil {
			t.Fatal(err)
		}
		if err := s.RestoreAttachment(context.Background(), approved, id, bytes.NewReader([]byte("must not replace ready bytes"))); err != nil {
			t.Fatal(err)
		}
		if err := s.WithAttachment(approved, id, func(info AttachmentInfo, reader io.ReadSeeker) error {
			actual, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			if info.Status != "ready" || !bytes.Equal(actual, payload) {
				return errors.New("restored bytes differ")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if jobs, err := s.PendingAttachments(32); err != nil || len(jobs) != 0 {
		t.Fatalf("restoration left unexpected jobs: %d %v", len(jobs), err)
	}
}

func TestRestoreRejectsWrongHashSizeAndDeniedChat(t *testing.T) {
	s, path := openTestStore(t, approvedConfig())
	original := []byte("original bytes expected")
	for _, payload := range [][]byte{bytes.Repeat([]byte("x"), len(original)), []byte("short"), bytes.Repeat([]byte("x"), 200)} {
		id := fmt.Sprintf("invalid-%d", len(payload))
		m := message(id, approved, "")
		m.MediaType = "document"
		if err := s.IngestWithAttachments([]Message{m}, []AttachmentRecord{{ChatJID: approved, MessageID: id, Descriptor: syntheticDescriptor(original)}}); err != nil {
			t.Fatal(err)
		}
		if err := s.RestoreAttachment(context.Background(), approved, id, bytes.NewReader(payload)); err == nil {
			t.Fatal("invalid local payload became ready")
		}
		info, err := s.GetAttachmentInfo(approved, id)
		if err != nil || info.Status == "ready" {
			t.Fatalf("invalid restoration status: %+v %v", info, err)
		}
	}
	secret := "DENIED_LOCAL_RESTORE_LITERAL"
	reader := &trackingRestoreReader{Reader: bytes.NewReader([]byte(secret))}
	if err := s.RestoreAttachment(context.Background(), excluded, "unknown", reader); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("denied local restore: %v", err)
	}
	if reader.reads != 0 {
		t.Fatal("denied source was read")
	}
	assertAbsentOnDisk(t, path, secret)
	assertCacheEmpty(t, s)
}

type trackingRestoreReader struct {
	*bytes.Reader
	reads int
}

func (r *trackingRestoreReader) Read(p []byte) (int, error) { r.reads++; return r.Reader.Read(p) }
