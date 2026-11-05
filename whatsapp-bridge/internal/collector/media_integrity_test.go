package collector

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type fixtureMediaTransport struct{ body []byte }

type retryFixtureMediaTransport struct {
	bodies [][]byte
	calls  int
}

func (f *retryFixtureMediaTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	index := f.calls
	f.calls++
	return (fixtureMediaTransport{f.bodies[index]}).RoundTrip(request)
}

func (f fixtureMediaTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Header: make(http.Header), Request: request,
		ContentLength: int64(len(f.body)), Body: io.NopCloser(bytes.NewReader(f.body)),
	}, nil
}

func encryptedMediaFixture(t *testing.T, plaintext, key []byte) []byte {
	t.Helper()
	expanded := hkdfutil.SHA256(key, nil, []byte(whatsmeow.MediaDocument), 112)
	block, err := aes.NewCipher(expanded[16:48])
	if err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	encrypted := append(append([]byte(nil), plaintext...), bytes.Repeat([]byte{byte(padding)}, padding)...)
	cipher.NewCBCEncrypter(block, expanded[:16]).CryptBlocks(encrypted, encrypted)
	mac := hmac.New(sha256.New, expanded[48:80])
	mac.Write(expanded[:16])
	mac.Write(encrypted)
	return append(encrypted, mac.Sum(nil)[:10]...)
}

func TestPinnedLibraryStreamingIntegrityWithSyntheticHTTP(t *testing.T) {
	plaintext := []byte("Synthetic approved document content.")
	key := bytes.Repeat([]byte{3}, 32)
	original := encryptedMediaFixture(t, plaintext, key)
	for _, name := range []string{"valid", "encrypted-hash", "hmac", "plaintext-hash"} {
		t.Run(name, func(t *testing.T) {
			body := append([]byte(nil), original...)
			plainHash := sha256.Sum256(plaintext)
			encHash := sha256.Sum256(original)
			var expected error
			switch name {
			case "encrypted-hash":
				body[0] ^= 1
				expected = whatsmeow.ErrInvalidMediaEncSHA256
			case "hmac":
				body[len(body)-1] ^= 1
				encHash = sha256.Sum256(body)
				expected = whatsmeow.ErrInvalidMediaHMAC
			case "plaintext-hash":
				plainHash[0] ^= 1
				expected = whatsmeow.ErrInvalidMediaSHA256
			}
			cfg := config.Default()
			cfg.AllowedChats = []config.Chat{{JID: allowedChat, Label: "Synthetic document"}}
			archiveStore, err := archive.Open(filepath.Join(t.TempDir(), "archive.db"), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer archiveStore.Close()
			d := archive.AttachmentDescriptor{
				DirectPath: "/v/synthetic.enc?fixture=1", MediaType: "document", MIMEType: "application/octet-stream",
				Filename: "synthetic.bin", FileLength: int64(len(plaintext)), MediaKey: key,
				FileSHA256: plainHash[:], FileEncSHA256: encHash[:],
			}
			row := archive.Message{ID: "synthetic-media", ChatJID: allowedChat, Sender: allowedChat, Timestamp: time.Now().Unix(), MediaType: "document"}
			if err := archiveStore.IngestWithAttachments([]archive.Message{row}, []archive.AttachmentRecord{{ChatJID: allowedChat, MessageID: row.ID, Descriptor: d}}); err != nil {
				t.Fatal(err)
			}
			jobs, err := archiveStore.PendingAttachments(1)
			if err != nil || len(jobs) != 1 {
				t.Fatalf("expected one media job: %v", err)
			}
			client := whatsmeow.NewClient(store.NoopDevice, waLog.Noop)
			client.SetMediaHTTPClient(&http.Client{Transport: mediaGuard{fixtureMediaTransport{body}}})
			err = archiveStore.CacheAttachment(context.Background(), jobs[0], func(ctx context.Context, file archive.AttachmentFile) error {
				// Test-only entry point exercises the pinned streaming decrypt path
				// without a real account or media-connection negotiation. Production
				// calls the public DownloadToFile method instead.
				return client.DangerousInternals().DownloadAndDecryptToFile(ctx, "https://mmg.whatsapp.net/v/synthetic.enc", key, whatsmeow.MediaDocument, encHash[:], plainHash[:], file)
			})
			if expected != nil {
				if !errors.Is(err, expected) {
					t.Fatalf("expected integrity rejection %v, got %v", expected, err)
				}
				if info, _ := archiveStore.GetAttachmentInfo(allowedChat, row.ID); info.Status == "ready" {
					t.Fatal("integrity failure published cached bytes")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := archiveStore.WithAttachment(allowedChat, row.ID, func(info archive.AttachmentInfo, reader io.ReadSeeker) error {
				actual, err := io.ReadAll(reader)
				if err == nil && !bytes.Equal(actual, plaintext) {
					t.Fatal("streaming decryption produced incorrect plaintext")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMediaHostRetryResetsBoundedCiphertextFile(t *testing.T) {
	plaintext := []byte("Synthetic retry fixture.")
	key := bytes.Repeat([]byte{3}, 32)
	encrypted := encryptedMediaFixture(t, plaintext, key)
	wrong := append([]byte(nil), encrypted...)
	wrong[0] ^= 1
	plainHash, encHash := sha256.Sum256(plaintext), sha256.Sum256(encrypted)
	cfg := config.Default()
	cfg.AllowedChats = []config.Chat{{JID: allowedChat, Label: "Synthetic"}}
	s, err := archive.Open(filepath.Join(t.TempDir(), "archive.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := archive.AttachmentDescriptor{DirectPath: "/v/synthetic.enc?fixture=1", MediaType: "document", FileLength: int64(len(plaintext)), MediaKey: key, FileSHA256: plainHash[:], FileEncSHA256: encHash[:]}
	row := archive.Message{ID: "host-retry", ChatJID: allowedChat, Sender: allowedChat, Timestamp: time.Now().Unix(), MediaType: "document"}
	if err := s.IngestWithAttachments([]archive.Message{row}, []archive.AttachmentRecord{{ChatJID: allowedChat, MessageID: row.ID, Descriptor: d}}); err != nil {
		t.Fatal(err)
	}
	jobs, err := s.PendingAttachments(1)
	if err != nil || len(jobs) != 1 {
		t.Fatal("missing retry fixture job")
	}
	transport := &retryFixtureMediaTransport{bodies: [][]byte{wrong, encrypted}}
	client := whatsmeow.NewClient(store.NoopDevice, waLog.Noop)
	client.SetMediaHTTPClient(&http.Client{Transport: mediaGuard{transport}})
	err = s.CacheAttachment(context.Background(), jobs[0], func(ctx context.Context, file archive.AttachmentFile) error {
		ctx = context.WithValue(ctx, mediaFileContextKey{}, file)
		first := client.DangerousInternals().DownloadAndDecryptToFile(ctx, "https://mmg.whatsapp.net/v/first", key, whatsmeow.MediaDocument, encHash[:], plainHash[:], file)
		if !errors.Is(first, whatsmeow.ErrInvalidMediaEncSHA256) {
			t.Fatal("first host did not reject corrupt ciphertext")
		}
		return client.DangerousInternals().DownloadAndDecryptToFile(ctx, "https://media.cdn.whatsapp.net/v/second", key, whatsmeow.MediaDocument, encHash[:], plainHash[:], file)
	})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := s.GetAttachmentInfo(allowedChat, row.ID); err != nil || info.Status != "ready" || transport.calls != 2 {
		t.Fatal("second verified host was not cached")
	}
}
