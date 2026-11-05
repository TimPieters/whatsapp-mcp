package collector

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

const allowedChat = "1234567890@s.whatsapp.net"
const rejectedChat = "9999999999@s.whatsapp.net"

type fakeArchive struct {
	allowed map[string]bool
	batches [][]archive.Message
	err     error
}

func (s *fakeArchive) IsAllowed(jid string) bool { return s.allowed[jid] }
func (s *fakeArchive) Ingest(messages []archive.Message) error {
	s.batches = append(s.batches, append([]archive.Message(nil), messages...))
	return s.err
}

func testCollector(store *fakeArchive) (*Collector, *bytes.Buffer) {
	output := &bytes.Buffer{}
	return &Collector{archive: store, logger: slog.New(slog.NewTextHandler(output, nil))}, output
}

func live(chat, body string) *events.Message {
	jid, _ := types.ParseJID(chat)
	return &events.Message{
		Info: types.MessageInfo{
			MessageSource: types.MessageSource{Chat: jid, Sender: jid},
			ID:            "synthetic-message", Timestamp: time.Unix(1700000000, 0),
		},
		Message: &waE2E.Message{Conversation: proto.String(body)},
	}
}

func TestLiveAdmissionAndPrivateLogs(t *testing.T) {
	s := &fakeArchive{allowed: map[string]bool{allowedChat: true}}
	c, logs := testCollector(s)
	c.handleEvent(live(rejectedChat, "rejected-content-marker"))
	if len(s.batches) != 0 || logs.Len() != 0 {
		t.Fatal("rejected chat reached archive or logs")
	}
	s.err = errors.New("sensitive-database-error-marker")
	c.handleEvent(live(allowedChat, "approved-content-marker"))
	if len(s.batches) != 1 || s.batches[0][0].Text != "approved-content-marker" {
		t.Fatal("approved message was not archived")
	}
	for _, private := range []string{allowedChat, rejectedChat, "content-marker", "synthetic-message", "sensitive-database-error-marker"} {
		if bytes.Contains(logs.Bytes(), []byte(private)) {
			t.Fatal("private data appeared in logs")
		}
	}
}

func historyMessage(chat string, i int, message *waE2E.Message) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
		Key:     &waCommon.MessageKey{RemoteJID: proto.String(chat), ID: proto.String(fmt.Sprintf("m-%d", i))},
		Message: message, MessageTimestamp: proto.Uint64(1700000000),
		PushName: proto.String("ignored-name-marker"),
	}}
}

func TestHistoryAdmissionAndBoundedBatches(t *testing.T) {
	s := &fakeArchive{allowed: map[string]bool{allowedChat: true}}
	c, logs := testCollector(s)
	approved := &waHistorySync.Conversation{ID: proto.String(allowedChat)}
	for i := 0; i < 205; i++ {
		approved.Messages = append(approved.Messages, historyMessage(allowedChat, i, &waE2E.Message{
			EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
				DocumentWithCaptionMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{
					DocumentMessage: &waE2E.DocumentMessage{Caption: proto.String("approved caption")},
				}},
			}},
		}))
	}
	// A mismatched embedded chat must never be imported under an approved envelope.
	approved.Messages = append(approved.Messages, historyMessage(rejectedChat, 206, &waE2E.Message{Conversation: proto.String("rejected")}))
	c.handleEvent(&events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{
		{ID: proto.String(rejectedChat), Messages: []*waHistorySync.HistorySyncMsg{historyMessage(rejectedChat, 1, &waE2E.Message{Conversation: proto.String("rejected")})}},
		approved,
	}}})
	if len(s.batches) != 3 || len(s.batches[0]) != 100 || len(s.batches[1]) != 100 || len(s.batches[2]) != 5 {
		t.Fatalf("unexpected history batch sizes: %v", s.batches)
	}
	for _, batch := range s.batches {
		for _, row := range batch {
			if row.ChatJID != allowedChat || row.Text != "approved caption" || row.MediaType != "document" {
				t.Fatal("unexpected archived history content")
			}
		}
	}
	if logs.Len() != 0 {
		t.Fatal("history content should not be logged")
	}
}

func TestViewOnceSkippedIncludingUnwrappedLiveEvents(t *testing.T) {
	s := &fakeArchive{allowed: map[string]bool{allowedChat: true}}
	c, _ := testCollector(s)
	event := live(allowedChat, "private")
	event.IsViewOnce = true
	c.handleEvent(event)
	if len(s.batches) != 0 {
		t.Fatal("unwrapped view-once event was archived")
	}
	for _, msg := range []*waE2E.Message{
		{ViewOnceMessage: &waE2E.FutureProofMessage{Message: event.Message}},
		{ViewOnceMessageV2: &waE2E.FutureProofMessage{Message: event.Message}},
		{EphemeralMessage: &waE2E.FutureProofMessage{Message: &waE2E.Message{ViewOnceMessageV2Extension: &waE2E.FutureProofMessage{Message: event.Message}}}},
	} {
		if _, _, ok := extract(msg); ok {
			t.Fatal("wrapped view-once content was extracted")
		}
	}
}

func TestExtractionIsBoundedAndIgnoresStructuredPrivateData(t *testing.T) {
	cycle := &waE2E.Message{}
	cycle.EphemeralMessage = &waE2E.FutureProofMessage{Message: cycle}
	for _, message := range []*waE2E.Message{nil, cycle, {ContactMessage: &waE2E.ContactMessage{DisplayName: proto.String("ignored")}}} {
		if _, _, ok := extract(message); ok {
			t.Fatal("unsupported payload was extracted")
		}
	}
	text, media, ok := extract(&waE2E.Message{DeviceSentMessage: &waE2E.DeviceSentMessage{Message: &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{Text: proto.String("text")}}}})
	if !ok || text != "text" || media != "" {
		t.Fatal("device-sent text was not extracted")
	}
}
