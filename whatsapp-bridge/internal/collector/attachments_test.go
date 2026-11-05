package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type fakeAttachmentStore struct {
	*fakeArchive
	records []archive.AttachmentRecord
	jobs    []archive.AttachmentJob
	cached  [][]byte
}

func (s *fakeAttachmentStore) IngestWithAttachments(messages []archive.Message, records []archive.AttachmentRecord) error {
	if err := s.Ingest(messages); err != nil {
		return err
	}
	s.records = append(s.records, records...)
	return nil
}

func (s *fakeAttachmentStore) PendingAttachments(limit int) ([]archive.AttachmentJob, error) {
	if len(s.jobs) == 0 {
		return nil, nil
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	return []archive.AttachmentJob{job}, nil
}

func (s *fakeAttachmentStore) CacheAttachment(ctx context.Context, job archive.AttachmentJob, download func(context.Context, archive.AttachmentFile) error) error {
	f, err := os.CreateTemp("", "attachment-test-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = download(ctx, f); err != nil {
		return err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(f)
	if err == nil {
		s.cached = append(s.cached, data)
	}
	return err
}

func mediaDescriptor() archive.AttachmentDescriptor {
	hash := sha256.Sum256([]byte("fixture"))
	return archive.AttachmentDescriptor{
		DirectPath: "/v/media.enc?auth=private-marker", MediaKey: bytes.Repeat([]byte{3}, 32),
		FileSHA256: hash[:], FileEncSHA256: bytes.Repeat([]byte{4}, 32),
		FileLength: 7, MediaType: "document", Filename: "example.xlsx",
		MIMEType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
	}
}

func documentMessage(d archive.AttachmentDescriptor) *waE2E.Message {
	return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
		DirectPath: proto.String(d.DirectPath), MediaKey: d.MediaKey,
		FileSHA256: d.FileSHA256, FileEncSHA256: d.FileEncSHA256,
		FileLength: proto.Uint64(uint64(d.FileLength)), Mimetype: proto.String(d.MIMEType),
		FileName: proto.String(d.Filename), Caption: proto.String("approved caption"),
	}}
}

func TestAttachmentAdmissionIsAtomicAndNeverDownloadsInHandler(t *testing.T) {
	s := &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{allowedChat: true}}}
	c, logs := testCollector(s.fakeArchive)
	c.attachments = s
	c.mediaWake = make(chan struct{}, 1)
	downloads := 0
	c.download = func(context.Context, archive.AttachmentDescriptor, archive.AttachmentFile) error {
		downloads++
		return nil
	}
	d := mediaDescriptor()
	event := live(rejectedChat, "")
	event.Message = documentMessage(d)
	c.handleEvent(event)
	if len(s.batches) != 0 || len(s.records) != 0 {
		t.Fatal("rejected attachment reached storage")
	}
	event.Info.Chat, _ = types.ParseJID(allowedChat)
	c.handleEvent(event)
	if len(s.batches) != 1 || len(s.records) != 1 || s.records[0].Descriptor.Filename != "example.xlsx" {
		t.Fatal("approved attachment not stored with message")
	}
	if downloads != 0 {
		t.Fatal("event handler made a network operation")
	}
	d.MediaKey[0] = 9
	if s.records[0].Descriptor.MediaKey[0] != 3 {
		t.Fatal("descriptor retains mutable protobuf key slice")
	}
	if len(c.mediaWake) != 1 || logs.Len() != 0 {
		t.Fatal("prefetch wake or private logging unexpected")
	}
	// Quoted media belongs to context, not to this message's attachment.
	quoted := live(allowedChat, "text")
	quoted.Message = &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String("text"), ContextInfo: &waE2E.ContextInfo{QuotedMessage: documentMessage(mediaDescriptor())},
	}}
	c.handleEvent(quoted)
	if len(s.records) != 1 {
		t.Fatal("quoted media descriptor was archived")
	}
	event.IsViewOnce = true
	c.handleEvent(event)
	if len(s.records) != 1 {
		t.Fatal("view-once media descriptor was archived")
	}
}

func TestInvalidAttachmentMetadataKeepsText(t *testing.T) {
	s := &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{allowedChat: true}}}
	c, _ := testCollector(s.fakeArchive)
	c.attachments = s
	d := mediaDescriptor()
	d.DirectPath = "https://example.invalid/private"
	event := live(allowedChat, "")
	event.Message = documentMessage(d)
	c.handleEvent(event)
	if len(s.records) != 0 || len(s.batches) != 1 || s.batches[0][0].Text != "approved caption" {
		t.Fatal("bad attachment metadata dropped valid text")
	}
}

func TestPrefetchUsesClaimedJobsWithoutNetworkAndRechecksApproval(t *testing.T) {
	s := &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{allowedChat: true}}}
	c, logs := testCollector(s.fakeArchive)
	c.attachments = s
	s.jobs = []archive.AttachmentJob{{ChatJID: allowedChat, Descriptor: mediaDescriptor()}, {ChatJID: rejectedChat, Descriptor: mediaDescriptor()}}
	calls := 0
	c.download = func(ctx context.Context, d archive.AttachmentDescriptor, file archive.AttachmentFile) error {
		calls++
		_, err := file.Write([]byte("fixture"))
		return err
	}
	c.prefetchAvailable(context.Background())
	if calls != 1 || len(s.cached) != 1 || string(s.cached[0]) != "fixture" {
		t.Fatal("prefetch did not enforce current approval")
	}
	for _, secret := range []string{allowedChat, rejectedChat, "private-marker", "example.xlsx"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("attachment log leaked private data")
		}
	}
}

func TestPrefetchShutdownCancelsDownload(t *testing.T) {
	s := &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{allowedChat: true}}}
	c, _ := testCollector(s.fakeArchive)
	c.attachments = s
	s.jobs = []archive.AttachmentJob{{ChatJID: allowedChat, Descriptor: mediaDescriptor()}}
	started := make(chan struct{})
	c.download = func(ctx context.Context, d archive.AttachmentDescriptor, file archive.AttachmentFile) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.prefetchLoop(ctx); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("download blocked shutdown")
	}
	if len(s.cached) != 0 {
		t.Fatal("cancelled media was cached")
	}
}

type fakeRoundTripper struct{ calls int }

func (f *fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	f.calls++
	return nil, errors.New("synthetic transport")
}

func TestMediaTransportRejectsExternalEndpointsWithoutNetwork(t *testing.T) {
	fake := &fakeRoundTripper{}
	guard := mediaGuard{fake}
	for _, endpoint := range []string{"http://mmg.whatsapp.net/file", "https://127.0.0.1/file", "https://mmg.whatsapp.net.evil.invalid/file", "https://mmg.whatsapp.net:444/file", "https://secret@mmg.whatsapp.net/file"} {
		request, _ := http.NewRequest(http.MethodGet, endpoint, nil)
		if _, err := guard.RoundTrip(request); err == nil {
			t.Fatal("unsafe endpoint accepted")
		}
	}
	if fake.calls != 0 {
		t.Fatal("rejected endpoint reached network transport")
	}
	for _, host := range []string{"mmg.whatsapp.net", "media.cdn.whatsapp.net", "video.xx.fbcdn.net", "web.whatsapp.com"} {
		request, _ := http.NewRequest(http.MethodGet, "https://"+host+"/v/file", nil)
		guard.RoundTrip(request)
	}
	if fake.calls != 4 {
		t.Fatal("trusted CDN host was rejected")
	}
	for _, ip := range []string{"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.0.1", "169.254.169.254", "100.64.0.1", "::1", "fd00::1", "::ffff:127.0.0.1"} {
		if publicMediaIP(netip.MustParseAddr(ip)) {
			t.Fatal("nonpublic media address accepted")
		}
	}
	if !publicMediaIP(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("public media address rejected")
	}
	client, transport := newMediaHTTPClient()
	defer transport.CloseIdleConnections()
	if transport.Proxy != nil || client.CheckRedirect(nil, nil) == nil {
		t.Fatal("media proxy/redirect policy not enforced")
	}
}

func TestMediaDescriptorRejectsUnsafePathsAndMissingIntegrity(t *testing.T) {
	for _, path := range []string{"https://example.invalid/file", "//example.invalid/file", "/a/../file", "/a/%2e%2e/file", "/a/%0a/file", "/a\\file", "/v/file#fragment"} {
		d := mediaDescriptor()
		d.DirectPath = path
		if validMediaDescriptor(d) {
			t.Fatal("unsafe direct path accepted")
		}
	}
	d := mediaDescriptor()
	d.FileEncSHA256 = nil
	if validMediaDescriptor(d) {
		t.Fatal("missing encrypted integrity accepted")
	}
	if !validMediaDescriptor(mediaDescriptor()) {
		t.Fatal("valid descriptor rejected")
	}
}

func TestMediaDownloadQuerySeparatorDoesNotChangePersistedPath(t *testing.T) {
	for _, path := range []string{"/v/fixture.enc", "/o1/v/fixture.enc?token=fixture"} {
		d := mediaDescriptor()
		d.DirectPath = path
		requestPath := (downloadable{d}).GetDirectPath()
		full, err := url.Parse("https://mmg.whatsapp.net" + requestPath + "&hash=fixture&mms-type=document")
		if err != nil || full.Query().Get("hash") != "fixture" || strings.Contains(full.Path, "&hash=") {
			t.Fatal("media query was appended to the filename")
		}
		if d.DirectPath != path {
			t.Fatal("download normalization changed stored descriptor")
		}
		if strings.Contains(path, "?") && requestPath != path {
			t.Fatal("existing authenticated query changed")
		}
	}
}

func TestHistoryAttachmentsOnlyFromApprovedEnvelope(t *testing.T) {
	s := &fakeAttachmentStore{fakeArchive: &fakeArchive{allowed: map[string]bool{allowedChat: true}}}
	c, _ := testCollector(s.fakeArchive)
	c.attachments = s
	wrapped := &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: documentMessage(mediaDescriptor())}}
	c.handleEvent(&events.HistorySync{Data: &waHistorySync.HistorySync{Conversations: []*waHistorySync.Conversation{
		{ID: proto.String(rejectedChat), Messages: []*waHistorySync.HistorySyncMsg{historyMessage(rejectedChat, 1, wrapped)}},
		{ID: proto.String(allowedChat), Messages: []*waHistorySync.HistorySyncMsg{historyMessage(allowedChat, 2, wrapped)}},
	}}})
	if len(s.records) != 1 || s.records[0].ChatJID != allowedChat || s.records[0].Descriptor.MediaType != "document" {
		t.Fatal("history attachment admission failed")
	}
}
