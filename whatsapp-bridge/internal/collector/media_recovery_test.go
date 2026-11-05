package collector

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/proto"
)

const recoveryChat = "120363000000000001@g.us"

type fakeRecoveryStore struct {
	*fakeAttachmentStore
	candidates []archive.AttachmentRecovery
	replaced   []string
	claims     int
}

func (s *fakeRecoveryStore) ClaimAttachmentRecoveries(limit int) ([]archive.AttachmentRecovery, error) {
	s.claims++
	if len(s.candidates) == 0 {
		return nil, nil
	}
	row := s.candidates[0]
	s.candidates = s.candidates[1:]
	return []archive.AttachmentRecovery{row}, nil
}

func (s *fakeRecoveryStore) ReplaceRecoveredAttachment(row archive.AttachmentRecovery, path string) error {
	s.replaced = append(s.replaced, path)
	return nil
}

func (s *fakeRecoveryStore) WithAttachmentRecovery(row archive.AttachmentRecovery, send func() error) error {
	if !s.allowed[row.ChatJID] {
		return archive.ErrNotAllowed
	}
	return send()
}

func recoveryCollector(t *testing.T) (*Collector, *fakeRecoveryStore, archive.AttachmentRecovery) {
	t.Helper()
	s := &fakeRecoveryStore{fakeAttachmentStore: &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{recoveryChat: true}}}}
	c, _ := testCollector(s.fakeArchive)
	c.attachments = s
	c.dataDir = t.TempDir()
	c.mediaWake = make(chan struct{}, 1)
	row := archive.AttachmentRecovery{ChatJID: recoveryChat, MessageID: "synthetic-recovery-id", Ref: "private-old-ref", Sender: "120363000000000001", Descriptor: mediaDescriptor()}
	return c, s, row
}

func retryResponse(t *testing.T, row archive.AttachmentRecovery, stanza, path string) *events.MediaRetry {
	t.Helper()
	payload, err := proto.Marshal(&waMmsRetry.MediaRetryNotification{StanzaID: proto.String(stanza), DirectPath: proto.String(path), Result: waMmsRetry.MediaRetryNotification_SUCCESS.Enum()})
	if err != nil {
		t.Fatal(err)
	}
	key := hkdfutil.SHA256(row.Descriptor.MediaKey, nil, []byte("WhatsApp Media Retry Notification"), 32)
	iv := bytes.Repeat([]byte{1}, 12)
	ciphertext, err := gcmutil.Encrypt(key, iv, payload, []byte(row.MessageID))
	if err != nil {
		t.Fatal(err)
	}
	chat, _ := types.ParseJID(row.ChatJID)
	return &events.MediaRetry{ChatID: chat, MessageID: row.MessageID, FromMe: row.FromMe, IV: iv, Ciphertext: ciphertext}
}

func TestMediaRecoveryNeverFabricatesLegacyGroupParticipant(t *testing.T) {
	c, s, row := recoveryCollector(t)
	requests := 0
	c.sendMediaRetry = func(ctx context.Context, info *types.MessageInfo, key []byte) error {
		requests++
		if info.Chat.String() != recoveryChat || info.ID != row.MessageID || info.IsFromMe || info.IsGroup || !info.Sender.IsEmpty() {
			t.Fatal("unknown legacy participant was fabricated")
		}
		return nil
	}
	c.sendOneMediaRecovery(context.Background(), row)
	if requests != 1 {
		t.Fatal("expected one own-phone protocol request")
	}
	c.handleMediaRetry(retryResponse(t, row, row.MessageID, "/o1/v/refreshed.enc?fixture=1"))
	if len(s.replaced) != 1 || s.replaced[0] != "/o1/v/refreshed.enc?fixture=1" {
		t.Fatal("authenticated media refresh not applied")
	}
	if c.recoveryStats["refreshed"] != 1 || len(c.mediaWake) != 1 {
		t.Fatal("refreshed media not queued")
	}
	data, err := os.ReadFile(filepath.Join(c.dataDir, "media-recovery.status.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), row.MessageID) || strings.Contains(string(data), recoveryChat) || strings.Contains(string(data), "private-old-ref") {
		t.Fatal("recovery status leaked private identifiers")
	}
}

func TestMediaRecoveryRejectsMismatchedResponseAndRevokedPolicy(t *testing.T) {
	for _, kind := range []string{"stanza", "ciphertext", "revoked", "path"} {
		t.Run(kind, func(t *testing.T) {
			c, s, row := recoveryCollector(t)
			c.sendMediaRetry = func(context.Context, *types.MessageInfo, []byte) error { return nil }
			c.sendOneMediaRecovery(context.Background(), row)
			stanza, path := row.MessageID, "/v/refreshed.enc?fixture=1"
			if kind == "stanza" {
				stanza = "unrelated-message"
			}
			if kind == "path" {
				path = "https://evil.invalid/private"
			}
			event := retryResponse(t, row, stanza, path)
			if kind == "ciphertext" {
				event.Ciphertext[0] ^= 1
			}
			if kind == "revoked" {
				s.allowed[recoveryChat] = false
			}
			c.handleMediaRetry(event)
			if len(s.replaced) != 0 {
				t.Fatal("untrusted or revoked media refresh applied")
			}
		})
	}
}

func TestMediaRecoveryMarkerIsOwnerOnlyAndClaimedOnce(t *testing.T) {
	c, s, row := recoveryCollector(t)
	s.candidates = []archive.AttachmentRecovery{row}
	c.sendMediaRetry = func(context.Context, *types.MessageInfo, []byte) error { return nil }
	request := filepath.Join(c.dataDir, MediaRecoveryRequestName)
	if err := os.WriteFile(request, []byte(MediaRecoveryRequestContent), 0600); err != nil {
		t.Fatal(err)
	}
	c.processMediaRecoveryRequest(context.Background())
	if c.recoveryStats["requested"] != 1 {
		t.Fatal("private recovery request not processed")
	}
	if _, err := os.Lstat(request); !os.IsNotExist(err) {
		t.Fatal("owner request was not consumed")
	}
	if _, err := os.Lstat(filepath.Join(c.dataDir, "media-recovery.processing")); !os.IsNotExist(err) {
		t.Fatal("processing marker remained")
	}
	claims := s.claims
	c.processMediaRecoveryRequest(context.Background())
	if s.claims != claims {
		t.Fatal("marker replayed without owner request")
	}
	if err := os.WriteFile(request, []byte("untrusted-content"), 0600); err != nil {
		t.Fatal(err)
	}
	c.processMediaRecoveryRequest(context.Background())
	if s.claims != claims {
		t.Fatal("invalid marker reached recovery claims")
	}
}

func TestMediaRecoveryPhoneUnavailableIsReportedWithoutDownload(t *testing.T) {
	c, s, row := recoveryCollector(t)
	c.sendMediaRetry = func(context.Context, *types.MessageInfo, []byte) error { return nil }
	c.sendOneMediaRecovery(context.Background(), row)
	chat, _ := types.ParseJID(recoveryChat)
	c.handleMediaRetry(&events.MediaRetry{ChatID: chat, MessageID: row.MessageID, Error: &events.MediaRetryError{Code: 2}})
	if c.recoveryStats["not_available_on_phone"] != 1 || len(s.replaced) != 0 {
		t.Fatal("phone unavailability not handled")
	}
}
