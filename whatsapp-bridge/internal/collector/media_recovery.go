package collector

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waMmsRetry"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const MediaRecoveryRequestName = "media-recovery.request"
const MediaRecoveryRequestContent = "recover-failed-v1\n"

type recoveryStore interface {
	ClaimAttachmentRecoveries(int) ([]archive.AttachmentRecovery, error)
	ReplaceRecoveredAttachment(archive.AttachmentRecovery, string) error
	WithAttachmentRecovery(archive.AttachmentRecovery, func() error) error
}

var _ recoveryStore = (*archive.Store)(nil)

type pendingRecovery struct {
	candidate archive.AttachmentRecovery
	sender    types.JID
	expires   time.Time
}

func recoveryKey(chat, id string) string { return chat + "\x00" + id }

func (c *Collector) processMediaRecoveryRequest(ctx context.Context) {
	store, ok := c.attachments.(recoveryStore)
	if !ok || c.sendMediaRetry == nil || c.dataDir == "" || (c.client != nil && !c.client.IsLoggedIn()) {
		return
	}
	request := filepath.Join(c.dataDir, MediaRecoveryRequestName)
	stat, err := os.Lstat(request)
	if err != nil {
		return
	}
	if !stat.Mode().IsRegular() || stat.Size() > 64 || (runtime.GOOS != "windows" && stat.Mode().Perm()&0077 != 0) {
		c.logger.Warn("invalid private media recovery request")
		return
	}
	processing := filepath.Join(c.dataDir, "media-recovery.processing")
	if _, err := os.Lstat(processing); !os.IsNotExist(err) {
		return
	}
	if err := os.Rename(request, processing); err != nil {
		return
	}
	defer os.Remove(processing)
	f, err := os.Open(processing)
	if err != nil {
		return
	}
	content, err := io.ReadAll(io.LimitReader(f, 65))
	f.Close()
	if err != nil || string(content) != MediaRecoveryRequestContent {
		c.logger.Warn("invalid private media recovery request")
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
		candidates, err := store.ClaimAttachmentRecoveries(1)
		if err != nil {
			c.logger.Warn("private media recovery could not start")
			return
		}
		if len(candidates) == 0 {
			return
		}
		c.sendOneMediaRecovery(ctx, candidates[0])
	}
}

func (c *Collector) sendOneMediaRecovery(ctx context.Context, candidate archive.AttachmentRecovery) {
	if !c.archive.IsAllowed(candidate.ChatJID) || !validMediaDescriptor(candidate.Descriptor) {
		c.recoveryResult("ineligible")
		return
	}
	info, err := c.recoveryMessageInfo(ctx, candidate)
	if err != nil {
		c.recoveryResult("sender_unresolved")
		return
	}
	key := recoveryKey(candidate.ChatJID, candidate.MessageID)
	c.recoveryMu.Lock()
	if c.recoveries == nil {
		c.recoveries = make(map[string]pendingRecovery)
	}
	if len(c.recoveries) >= 32 {
		c.recoveryMu.Unlock()
		c.recoveryResult("capacity_rejected")
		return
	}
	c.recoveries[key] = pendingRecovery{candidate: candidate, sender: info.Sender, expires: time.Now().Add(5 * time.Minute)}
	c.recoveryMu.Unlock()
	requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	store, ok := c.attachments.(recoveryStore)
	if !ok {
		cancel()
		return
	}
	err = store.WithAttachmentRecovery(candidate, func() error {
		c.recoveryResult("requested")
		return c.sendMediaRetry(requestCtx, info, candidate.Descriptor.MediaKey)
	})
	cancel()
	if err != nil {
		c.recoveryMu.Lock()
		delete(c.recoveries, key)
		c.recoveryMu.Unlock()
		c.recoveryResult("request_failed")
	}
}

func (c *Collector) recoveryMessageInfo(ctx context.Context, candidate archive.AttachmentRecovery) (*types.MessageInfo, error) {
	if !config.ValidJID(candidate.ChatJID) || candidate.MessageID == "" {
		return nil, errors.New("invalid recovery envelope")
	}
	chat, err := types.ParseJID(candidate.ChatJID)
	if err != nil {
		return nil, errors.New("invalid recovery chat")
	}
	info := &types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, IsFromMe: candidate.FromMe}, ID: candidate.MessageID}
	if chat.Server != types.GroupServer {
		return info, nil
	}
	if candidate.FromMe && c.client != nil {
		info.Sender = c.client.Store.GetJID().ToNonAD()
	} else if config.ValidJID(candidate.Sender) && candidate.Sender != candidate.ChatJID {
		info.Sender, err = types.ParseJID(candidate.Sender)
	}
	if err != nil {
		return nil, errors.New("invalid recovery sender")
	}
	if info.Sender.Server != types.DefaultUserServer && info.Sender.Server != types.HiddenUserServer {
		info.Sender = types.EmptyJID
	}
	// Some legacy archives replaced a missing group participant with the group
	// number. Do not invent a participant identity. Omitting that optional rmr
	// attribute lets the owner phone try an exact chat/message-key lookup.
	// IsGroup controls only that attribute in SendMediaRetryReceipt; Chat remains
	// the exact approved group JID. The phone may reject incomplete legacy keys.
	info.IsGroup = !info.Sender.IsEmpty()
	return info, nil
}

func (c *Collector) handleMediaRetry(event *events.MediaRetry) {
	if event == nil {
		return
	}
	chat := event.ChatID.ToNonAD().String()
	key := recoveryKey(chat, event.MessageID)
	c.recoveryMu.Lock()
	pending, ok := c.recoveries[key]
	if ok {
		delete(c.recoveries, key)
	}
	c.recoveryMu.Unlock()
	if !ok {
		return
	}
	if time.Now().After(pending.expires) || !c.archive.IsAllowed(chat) || event.FromMe != pending.candidate.FromMe || (!pending.sender.IsEmpty() && !event.SenderID.IsEmpty() && event.SenderID.ToNonAD() != pending.sender.ToNonAD()) {
		c.recoveryResult("response_rejected")
		return
	}
	notification, err := whatsmeow.DecryptMediaRetryNotification(event, pending.candidate.Descriptor.MediaKey)
	if errors.Is(err, whatsmeow.ErrMediaNotAvailableOnPhone) {
		c.recoveryResult("not_available_on_phone")
		return
	}
	// The protobuf stanza field is optional. The event ID is already authenticated
	// as AES-GCM additional data; reject a contradictory explicit stanza value.
	if err != nil || (notification.GetStanzaID() != "" && notification.GetStanzaID() != event.MessageID) {
		c.recoveryResult("response_invalid")
		return
	}
	if notification.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
		c.recoveryResult("phone_rejected")
		return
	}
	descriptor := pending.candidate.Descriptor
	descriptor.DirectPath = notification.GetDirectPath()
	if !validMediaDescriptor(descriptor) {
		c.recoveryResult("response_invalid")
		return
	}
	store, ok := c.attachments.(recoveryStore)
	if !ok || store.ReplaceRecoveredAttachment(pending.candidate, descriptor.DirectPath) != nil {
		c.recoveryResult("response_rejected")
		return
	}
	c.recoveryResult("refreshed")
	c.wakePrefetch()
}

func (c *Collector) expireMediaRecoveries() {
	c.recoveryMu.Lock()
	expired := 0
	for key, pending := range c.recoveries {
		if time.Now().After(pending.expires) {
			delete(c.recoveries, key)
			expired++
		}
	}
	c.recoveryMu.Unlock()
	for range expired {
		c.recoveryResult("timed_out")
	}
}

func (c *Collector) recoveryResult(result string) {
	c.recoveryMu.Lock()
	defer c.recoveryMu.Unlock()
	if c.recoveryStats == nil {
		c.recoveryStats = make(map[string]int)
	}
	c.recoveryStats[result]++
	if c.dataDir == "" {
		return
	}
	data, err := json.Marshal(struct {
		Counts    map[string]int `json:"counts"`
		Pending   int            `json:"pending"`
		UpdatedAt int64          `json:"updated_at"`
	}{c.recoveryStats, len(c.recoveries), time.Now().Unix()})
	if err != nil {
		return
	}
	path := filepath.Join(c.dataDir, "media-recovery.status.json")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0600); err == nil {
		_ = os.Rename(tmp, path)
	}
}
