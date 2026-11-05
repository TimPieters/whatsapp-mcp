package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"path/filepath"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"
)

// generatedAttachment supplies synthetic bytes without a file-sized allocation.
type generatedAttachment struct{}

func (generatedAttachment) Read(buffer []byte) (int, error) {
	for i := range buffer {
		buffer[i] = 'x'
	}
	return len(buffer), nil
}

// BenchmarkStreamingAttachment measures bounded cache streaming and readback.
// The download callback is synthetic; no network or transport cryptography runs.
// Measure process RSS separately from the Go allocation counters.
func BenchmarkStreamingAttachment(b *testing.B) {
	const size int64 = 32 << 20
	const chat = "1234567890@s.whatsapp.net"
	buffer := make([]byte, 32*1024)
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, io.LimitReader(generatedAttachment{}, size), buffer); err != nil {
		b.Fatal(err)
	}
	descriptor := archive.AttachmentDescriptor{
		DirectPath: "/v/synthetic.enc?fixture=1", MediaType: "document",
		MediaKey: bytes.Repeat([]byte{1}, 32), FileEncSHA256: bytes.Repeat([]byte{2}, 32),
		FileSHA256: hash.Sum(nil), FileLength: size,
		MIMEType: "application/octet-stream", Filename: "synthetic.bin",
	}
	cfg := config.Default()
	cfg.AllowedChats = []config.Chat{{JID: chat, Label: "Synthetic attachment"}}
	cfg.MaxAttachmentBytes = size
	cfg.AttachmentCacheBytes = size + 64
	b.ReportAllocs()
	b.SetBytes(size)
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		dir := b.TempDir()
		store, err := archive.Open(filepath.Join(dir, "archive.db"), cfg)
		if err != nil {
			b.Fatal(err)
		}
		row := archive.Message{ID: "synthetic-attachment", ChatJID: chat, Sender: chat, MediaType: "document", Timestamp: time.Now().Unix()}
		if err := store.IngestWithAttachments([]archive.Message{row}, []archive.AttachmentRecord{{ChatJID: chat, MessageID: row.ID, Descriptor: descriptor}}); err != nil {
			store.Close()
			b.Fatal(err)
		}
		jobs, err := store.PendingAttachments(1)
		if err != nil || len(jobs) != 1 {
			store.Close()
			b.Fatalf("expected one approved download job, got %d, error %v", len(jobs), err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err = store.CacheAttachment(ctx, jobs[0], func(ctx context.Context, file archive.AttachmentFile) error {
			n, copyErr := io.CopyBuffer(file, io.LimitReader(generatedAttachment{}, size), buffer)
			if copyErr == nil && n != size {
				return io.ErrShortWrite
			}
			return copyErr
		})
		cancel()
		if err != nil {
			store.Close()
			b.Fatal(err)
		}
		info, err := store.GetAttachmentInfo(chat, row.ID)
		if err != nil || info.Status != "ready" || info.Size != size {
			store.Close()
			b.Fatalf("unexpected cached attachment: %+v, error %v", info, err)
		}
		err = store.WithAttachment(chat, row.ID, func(info archive.AttachmentInfo, reader io.ReadSeeker) error {
			n, copyErr := io.Copy(io.Discard, reader)
			if copyErr == nil && n != size {
				return io.ErrUnexpectedEOF
			}
			return copyErr
		})
		closeErr := store.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
	}
	b.ReportMetric(float64(size), "cached_bytes")
	b.ReportMetric(float64(size), "streamed_download_bytes")
	b.ReportMetric(float64(size), "readback_bytes")
}
