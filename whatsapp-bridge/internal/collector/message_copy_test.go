package collector

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type fakeMessageCopyStore struct {
	*fakeRecoveryStore
	requestID string
	copies    int
}

func (s *fakeMessageCopyStore) ClaimAttachmentMessageCopies(limit int) ([]archive.AttachmentRecovery, error) {
	return s.ClaimAttachmentRecoveries(limit)
}
func (s *fakeMessageCopyStore) WithAttachmentMessageCopy(row archive.AttachmentRecovery, send func() error) error {
	return s.WithAttachmentRecovery(row, send)
}
func (s *fakeMessageCopyStore) RegisterAttachmentMessageCopy(row archive.AttachmentRecovery, id string) error {
	s.requestID = id
	return nil
}
func (s *fakeMessageCopyStore) ReplaceAttachmentMessageCopy(row archive.AttachmentRecovery, id string, d archive.AttachmentDescriptor) error {
	if id != s.requestID || d.FileLength != row.Descriptor.FileLength || d.MediaType != row.Descriptor.MediaType || !bytes.Equal(d.FileSHA256, row.Descriptor.FileSHA256) {
		return errors.New("copy fingerprint mismatch")
	}
	s.copies++
	return nil
}

func messageCopyCollector(t *testing.T) (*Collector, *fakeMessageCopyStore, archive.AttachmentRecovery) {
	t.Helper()
	c, old, row := recoveryCollector(t)
	s := &fakeMessageCopyStore{fakeRecoveryStore: old}
	c.attachments = s
	c.client = whatsmeow.NewClient(store.NoopDevice, waLog.Noop)
	return c, s, row
}

func copyEvent(row archive.AttachmentRecovery, requestID string) *events.Message {
	chat, _ := types.ParseJID(row.ChatJID)
	return &events.Message{
		UnavailableRequestID: requestID,
		Info:                 types.MessageInfo{ID: row.MessageID, MessageSource: types.MessageSource{Chat: chat, IsFromMe: row.FromMe}},
		Message:              documentMessage(row.Descriptor),
	}
}

func TestExactMessageCopyBindsPeerRequestAndEarlyResponse(t *testing.T) {
	c, s, row := messageCopyCollector(t)
	c.sendMessageCopy = func(ctx context.Context, message *waE2E.Message) (string, error) {
		protocol := message.GetProtocolMessage()
		requests := protocol.GetPeerDataOperationRequestMessage().GetPlaceholderMessageResendRequest()
		if protocol.GetType() != waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE || len(requests) != 1 {
			t.Fatal("not a bounded peer placeholder request")
		}
		key := requests[0].GetMessageKey()
		if key.GetRemoteJID() != row.ChatJID || key.GetID() != row.MessageID || key.GetFromMe() || key.Participant != nil {
			t.Fatal("exact legacy group key or direction changed")
		}
		// An own-phone response can arrive before the send's server ACK returns.
		c.handleAttachmentMessageCopy(copyEvent(row, "peer-copy-1"))
		if s.copies != 0 {
			t.Fatal("early unregistered response was applied")
		}
		return "peer-copy-1", nil
	}
	c.sendOneMessageCopy(context.Background(), row)
	if s.copies != 1 || s.requestID != "peer-copy-1" || c.copyStats["refreshed"] != 1 {
		t.Fatal("registered early copy did not refresh exact attachment")
	}
	if len(s.batches) != 0 {
		t.Fatal("peer response leaked into normal message ingestion")
	}
}

func TestUnsolicitedAndMismatchedPeerCopiesNeverIngest(t *testing.T) {
	for _, kind := range []string{"unsolicited", "request-id", "chat", "message-id", "direction", "plaintext"} {
		t.Run(kind, func(t *testing.T) {
			c, s, row := messageCopyCollector(t)
			c.sendMessageCopy = func(context.Context, *waE2E.Message) (string, error) { return "peer-copy-1", nil }
			if kind != "unsolicited" {
				c.sendOneMessageCopy(context.Background(), row)
			}
			event := copyEvent(row, "peer-copy-1")
			switch kind {
			case "request-id":
				event.UnavailableRequestID = "unrelated-request"
			case "chat":
				event.Info.Chat, _ = types.ParseJID(allowedChat)
			case "message-id":
				event.Info.ID = "unrelated-message"
			case "direction":
				event.Info.IsFromMe = true
			case "plaintext":
				event.Message.DocumentMessage.FileSHA256 = bytes.Repeat([]byte{9}, 32)
			}
			c.handleMessage(event)
			if s.copies != 0 || len(s.batches) != 0 {
				t.Fatal("uncorrelated peer copy was accepted")
			}
		})
	}
}
