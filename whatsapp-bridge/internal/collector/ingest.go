package collector

import (
	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const historyBatchSize = 100

func (c *Collector) handleMessage(event *events.Message) {
	if event == nil {
		return
	}
	chat := event.Info.Chat.ToNonAD().String()
	// Admission precedes content extraction and all archive operations.
	if !c.archive.IsAllowed(chat) {
		return
	}
	if event.IsViewOnce || event.IsViewOnceV2 || event.IsViewOnceV2Extension {
		return
	}
	message := event.RawMessage
	if message == nil {
		message = event.Message
	}
	text, media, ok := extract(message)
	if !ok {
		return
	}
	row := archive.Message{
		ID: event.Info.ID, ChatJID: chat,
		Sender: event.Info.Sender.ToNonAD().String(), Timestamp: event.Info.Timestamp.Unix(),
		Text: text, MediaType: media, FromMe: event.Info.IsFromMe,
	}
	if err := c.ingest([]archive.Message{row}, attachmentRecord(row, message)); err != nil {
		c.logger.Warn("approved message could not be archived")
	}
}

func (c *Collector) handleHistory(event *events.HistorySync) {
	if event == nil || event.Data == nil {
		return
	}
	batch := make([]archive.Message, 0, historyBatchSize)
	refs := make([]archive.AttachmentRecord, 0, historyBatchSize)
	flush := func() {
		if len(batch) > 0 {
			if err := c.ingest(batch, refs); err != nil {
				c.logger.Warn("approved history batch could not be archived")
			}
			clear(batch)
			batch = batch[:0]
			clear(refs)
			refs = refs[:0]
		}
	}
	for _, conversation := range event.Data.GetConversations() {
		jid, err := types.ParseJID(conversation.GetID())
		if err != nil {
			continue
		}
		chat := jid.ToNonAD().String()
		// Never parse messages, participants, names or captions in rejected chats.
		if !c.archive.IsAllowed(chat) {
			continue
		}
		for _, history := range conversation.GetMessages() {
			// Recheck during large syncs so configuration changes apply promptly.
			if !c.archive.IsAllowed(chat) {
				break
			}
			web := history.GetMessage()
			key := web.GetKey()
			if key == nil {
				continue
			}
			if remote := key.GetRemoteJID(); remote != "" {
				remoteJID, err := types.ParseJID(remote)
				if err != nil || remoteJID.ToNonAD().String() != chat {
					continue
				}
			}
			text, media, ok := extract(web.GetMessage())
			if !ok {
				continue
			}
			// Do not resolve contact names or invoke library history parsing, which
			// also extracts push names. Sender identifiers come from the envelope.
			sender := chat
			if jid.Server == types.GroupServer {
				sender = web.GetParticipant()
				if sender == "" {
					sender = key.GetParticipant()
				}
			}
			if key.GetFromMe() {
				// No account identifier is needed for the owner; FromMe is explicit.
				sender = ""
			}
			row := archive.Message{
				ID: key.GetID(), ChatJID: chat, Sender: sender,
				Timestamp: int64(web.GetMessageTimestamp()), FromMe: key.GetFromMe(),
				Text: text, MediaType: media,
			}
			batch = append(batch, row)
			refs = append(refs, attachmentRecord(row, web.GetMessage())...)
			if len(batch) == historyBatchSize {
				flush()
			}
		}
	}
	flush()
}

// extract ignores view-once payloads, unsupported structured data, and metadata.
// Wrappers are bounded to avoid traversing malformed, cyclic synthetic objects.
func extract(message *waE2E.Message) (text, media string, ok bool) {
	message = contentMessage(message)
	if message == nil {
		return "", "", false
	}
	switch {
	case message.Conversation != nil:
		return message.GetConversation(), "", message.GetConversation() != ""
	case message.ExtendedTextMessage != nil:
		return message.GetExtendedTextMessage().GetText(), "", message.GetExtendedTextMessage().GetText() != ""
	case message.ImageMessage != nil:
		return message.GetImageMessage().GetCaption(), "image", true
	case message.VideoMessage != nil:
		return message.GetVideoMessage().GetCaption(), "video", true
	case message.DocumentMessage != nil:
		return message.GetDocumentMessage().GetCaption(), "document", true
	case message.AudioMessage != nil:
		return "", "audio", true
	case message.StickerMessage != nil:
		return "", "sticker", true
	default:
		return "", "", false
	}
}

func contentMessage(message *waE2E.Message) *waE2E.Message {
	for depth := 0; message != nil && depth < 8; depth++ {
		if message.ViewOnceMessage != nil || message.ViewOnceMessageV2 != nil || message.ViewOnceMessageV2Extension != nil {
			return nil
		}
		switch {
		case message.DeviceSentMessage != nil:
			message = message.GetDeviceSentMessage().GetMessage()
		case message.EphemeralMessage != nil:
			message = message.GetEphemeralMessage().GetMessage()
		case message.DocumentWithCaptionMessage != nil:
			message = message.GetDocumentWithCaptionMessage().GetMessage()
		case message.EditedMessage != nil:
			message = message.GetEditedMessage().GetMessage()
		default:
			return message
		}
	}
	return nil
}
