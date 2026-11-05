package collector

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow/types/events"
)

const MessageCopyRequestName = "message-copy.request"
const MessageCopyRequestContent = "recover-message-copies-v1\n"

type messageCopyStore interface {
	ClaimAttachmentMessageCopies(int) ([]archive.AttachmentRecovery, error)
	WithAttachmentMessageCopy(archive.AttachmentRecovery, func() error) error
	RegisterAttachmentMessageCopy(archive.AttachmentRecovery, string) error
	ReplaceAttachmentMessageCopy(archive.AttachmentRecovery, string, archive.AttachmentDescriptor) error
}

var _ messageCopyStore = (*archive.Store)(nil)

type pendingMessageCopy struct {
	candidate archive.AttachmentRecovery
	requestID string
	early     *events.Message
	expires   time.Time
}

func (c *Collector) processMessageCopyRequest(ctx context.Context) {
	store, ok := c.attachments.(messageCopyStore)
	if !ok || c.client == nil || !c.client.IsLoggedIn() || c.sendMessageCopy == nil || c.dataDir == "" {
		return
	}
	path := filepath.Join(c.dataDir, MessageCopyRequestName)
	info, err := os.Lstat(path)
	if err != nil {
		return
	}
	if !info.Mode().IsRegular() || info.Size() > 64 || (runtime.GOOS != "windows" && info.Mode().Perm()&0077 != 0) {
		c.logger.Warn("invalid private message-copy request")
		return
	}
	processing := filepath.Join(c.dataDir, "message-copy.processing")
	if _, err := os.Lstat(processing); !os.IsNotExist(err) {
		return
	}
	if err := os.Rename(path, processing); err != nil {
		return
	}
	defer os.Remove(processing)
	f, err := os.Open(processing)
	if err != nil {
		return
	}
	data, err := io.ReadAll(io.LimitReader(f, 65))
	f.Close()
	if err != nil || string(data) != MessageCopyRequestContent {
		c.logger.Warn("invalid private message-copy request")
		return
	}
	for index := range 32 {
		if ctx.Err() != nil {
			return
		}
		if index > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
		rows, err := store.ClaimAttachmentMessageCopies(1)
		if err != nil {
			c.logger.Warn("private message-copy recovery could not start")
			return
		}
		if len(rows) == 0 {
			return
		}
		c.sendOneMessageCopy(ctx, rows[0])
	}
}

func (c *Collector) sendOneMessageCopy(ctx context.Context, candidate archive.AttachmentRecovery) {
	store, ok := c.attachments.(messageCopyStore)
	if !ok || c.client == nil || c.sendMessageCopy == nil || !c.archive.IsAllowed(candidate.ChatJID) {
		return
	}
	info, err := c.recoveryMessageInfo(ctx, candidate)
	if err != nil {
		c.copyResult("ineligible")
		return
	}
	request := c.client.BuildUnavailableMessageRequest(info.Chat, info.Sender, info.ID)
	key := request.GetProtocolMessage().GetPeerDataOperationRequestMessage().GetPlaceholderMessageResendRequest()[0].MessageKey
	key.FromMe = &candidate.FromMe
	if info.Sender.IsEmpty() {
		key.Participant = nil
	}
	mapKey := recoveryKey(candidate.ChatJID, candidate.MessageID)
	c.copyMu.Lock()
	if c.messageCopies == nil {
		c.messageCopies = make(map[string]pendingMessageCopy)
	}
	if len(c.messageCopies) >= 32 {
		c.copyMu.Unlock()
		c.copyResult("capacity_rejected")
		return
	}
	c.messageCopies[mapKey] = pendingMessageCopy{candidate: candidate, expires: time.Now().Add(5 * time.Minute)}
	c.copyMu.Unlock()
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	requestID := ""
	err = store.WithAttachmentMessageCopy(candidate, func() error {
		c.copyResult("requested")
		var sendErr error
		requestID, sendErr = c.sendMessageCopy(requestCtx, request)
		return sendErr
	})
	cancel()
	if err == nil {
		err = store.RegisterAttachmentMessageCopy(candidate, requestID)
	}
	if err != nil {
		c.copyMu.Lock()
		delete(c.messageCopies, mapKey)
		c.copyMu.Unlock()
		c.copyResult("request_failed")
		return
	}
	c.copyMu.Lock()
	pending, exists := c.messageCopies[mapKey]
	if exists {
		pending.requestID = requestID
		c.messageCopies[mapKey] = pending
	}
	early := pending.early
	c.copyMu.Unlock()
	if exists && early != nil {
		c.handleAttachmentMessageCopy(early)
	}
}

func (c *Collector) handleAttachmentMessageCopy(event *events.Message) {
	if event == nil || event.UnavailableRequestID == "" {
		return
	}
	chat := event.Info.Chat.ToNonAD().String()
	if !c.archive.IsAllowed(chat) {
		return
	}
	mapKey := recoveryKey(chat, event.Info.ID)
	c.copyMu.Lock()
	pending, ok := c.messageCopies[mapKey]
	if !ok || event.Info.IsFromMe != pending.candidate.FromMe || time.Now().After(pending.expires) {
		c.copyMu.Unlock()
		return
	}
	if pending.requestID == "" {
		// At most one early response pointer per exact requested message is held
		// until SendPeerMessage returns the ID used for durable correlation.
		if pending.early == nil {
			pending.early = event
			c.messageCopies[mapKey] = pending
		}
		c.copyMu.Unlock()
		return
	}
	if pending.requestID != event.UnavailableRequestID {
		c.copyMu.Unlock()
		return
	}
	delete(c.messageCopies, mapKey)
	c.copyMu.Unlock()
	if event.IsViewOnce || event.IsViewOnceV2 || event.IsViewOnceV2Extension {
		c.copyResult("response_rejected")
		return
	}
	raw := event.RawMessage
	if raw == nil {
		raw = event.Message
	}
	message := contentMessage(raw)
	if message == nil {
		c.copyResult("response_rejected")
		return
	}
	kind := ""
	switch {
	case message.ImageMessage != nil:
		kind = "image"
	case message.VideoMessage != nil:
		kind = "video"
	case message.AudioMessage != nil:
		kind = "audio"
	case message.DocumentMessage != nil:
		kind = "document"
	case message.StickerMessage != nil:
		kind = "sticker"
	}
	refs := attachmentRecord(archive.Message{ID: event.Info.ID, ChatJID: chat, MediaType: kind}, raw)
	if len(refs) != 1 {
		c.copyResult("response_invalid")
		return
	}
	store, ok := c.attachments.(messageCopyStore)
	if !ok || store.ReplaceAttachmentMessageCopy(pending.candidate, event.UnavailableRequestID, refs[0].Descriptor) != nil {
		c.copyResult("response_rejected")
		return
	}
	c.copyResult("refreshed")
	c.wakePrefetch()
}

func (c *Collector) expireMessageCopies() {
	c.copyMu.Lock()
	expired := 0
	for key, pending := range c.messageCopies {
		if time.Now().After(pending.expires) {
			delete(c.messageCopies, key)
			expired++
		}
	}
	c.copyMu.Unlock()
	for range expired {
		c.copyResult("timed_out")
	}
}

func (c *Collector) copyResult(result string) {
	c.copyMu.Lock()
	defer c.copyMu.Unlock()
	if c.copyStats == nil {
		c.copyStats = make(map[string]int)
	}
	c.copyStats[result]++
	if c.dataDir == "" {
		return
	}
	data, err := json.Marshal(struct {
		Counts    map[string]int `json:"counts"`
		Pending   int            `json:"pending"`
		UpdatedAt int64          `json:"updated_at"`
	}{c.copyStats, len(c.messageCopies), time.Now().Unix()})
	if err != nil {
		return
	}
	path := filepath.Join(c.dataDir, "message-copy.status.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, append(data, '\n'), 0600) == nil {
		_ = os.Rename(tmp, path)
	}
}
