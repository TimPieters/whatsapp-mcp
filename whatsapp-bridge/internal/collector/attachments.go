package collector

import (
	"context"
	"errors"
	"math"
	"net/url"
	"strings"
	"time"
	"unicode"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

type attachmentStore interface {
	IngestWithAttachments([]archive.Message, []archive.AttachmentRecord) error
	PendingAttachments(int) ([]archive.AttachmentJob, error)
	CacheAttachment(context.Context, archive.AttachmentJob, func(context.Context, archive.AttachmentFile) error) error
}

type mediaDownload func(context.Context, archive.AttachmentDescriptor, archive.AttachmentFile) error

// downloadable exposes only the authenticated direct path and integrity fields.
// No imported absolute URL is passed to the network library.
type downloadable struct{ archive.AttachmentDescriptor }

func (d downloadable) GetDirectPath() string {
	// The pinned library appends media query fields with '&'. Keep a queryless
	// authenticated path intact while supplying the required separator.
	if !strings.Contains(d.DirectPath, "?") {
		return d.DirectPath + "?"
	}
	return d.DirectPath
}
func (d downloadable) GetMediaKey() []byte      { return d.MediaKey }
func (d downloadable) GetFileSHA256() []byte    { return d.FileSHA256 }
func (d downloadable) GetFileEncSHA256() []byte { return d.FileEncSHA256 }
func (d downloadable) GetMediaType() whatsmeow.MediaType {
	switch d.MediaType {
	case "image", "sticker":
		return whatsmeow.MediaImage
	case "video":
		return whatsmeow.MediaVideo
	case "audio":
		return whatsmeow.MediaAudio
	case "document":
		return whatsmeow.MediaDocument
	default:
		return ""
	}
}

type mediaEnvelope interface {
	whatsmeow.DownloadableMessage
	GetFileLength() uint64
	GetMimetype() string
}

func attachmentRecord(row archive.Message, raw *waE2E.Message) []archive.AttachmentRecord {
	message := contentMessage(raw)
	if message == nil {
		return nil
	}
	var media mediaEnvelope
	var filename string
	switch row.MediaType {
	case "image":
		if message.ImageMessage == nil {
			return nil
		}
		media = message.ImageMessage
	case "video":
		if message.VideoMessage == nil {
			return nil
		}
		media = message.VideoMessage
	case "audio":
		if message.AudioMessage == nil {
			return nil
		}
		media = message.AudioMessage
	case "document":
		if message.DocumentMessage == nil {
			return nil
		}
		media = message.DocumentMessage
		filename = message.GetDocumentMessage().GetFileName()
	case "sticker":
		if message.StickerMessage == nil {
			return nil
		}
		media = message.StickerMessage
	default:
		return nil
	}
	if media.GetFileLength() > math.MaxInt64 {
		return nil
	}
	descriptor := archive.AttachmentDescriptor{
		DirectPath: media.GetDirectPath(), MediaType: row.MediaType,
		MediaKey:      media.GetMediaKey(),
		FileSHA256:    media.GetFileSHA256(),
		FileEncSHA256: media.GetFileEncSHA256(),
		FileLength:    int64(media.GetFileLength()), MIMEType: media.GetMimetype(), Filename: filename,
	}
	if !validMediaDescriptor(descriptor) {
		return nil
	}
	descriptor.MediaKey = append([]byte(nil), descriptor.MediaKey...)
	descriptor.FileSHA256 = append([]byte(nil), descriptor.FileSHA256...)
	descriptor.FileEncSHA256 = append([]byte(nil), descriptor.FileEncSHA256...)
	return []archive.AttachmentRecord{{ChatJID: row.ChatJID, MessageID: row.ID, Descriptor: descriptor}}
}

func validMediaDescriptor(d archive.AttachmentDescriptor) bool {
	if archive.ValidateAttachmentDescriptor(d, 128<<20) != nil {
		return false
	}
	u, err := url.Parse(d.DirectPath)
	if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" {
		return false
	}
	for _, r := range u.Path {
		if unicode.IsControl(r) || r == '\\' {
			return false
		}
	}
	return true
}

func (c *Collector) ingest(rows []archive.Message, refs []archive.AttachmentRecord) error {
	if c.attachments == nil {
		return c.archive.Ingest(rows)
	}
	if err := c.attachments.IngestWithAttachments(rows, refs); err != nil {
		return err
	}
	if len(refs) != 0 {
		c.wakePrefetch()
	}
	return nil
}

func (c *Collector) wakePrefetch() {
	select {
	case c.mediaWake <- struct{}{}:
	default:
	}
}

// WakeAttachments asks the background worker to scan persisted eligible jobs.
// It performs no network transfer and accepts no attachment credentials.
func (c *Collector) WakeAttachments() { c.wakePrefetch() }

func (c *Collector) prefetchLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		c.expireMediaRecoveries()
		c.processMediaRecoveryRequest(ctx)
		c.expireMessageCopies()
		c.processMessageCopyRequest(ctx)
		c.prefetchAvailable(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-c.mediaWake:
		}
	}
}

func (c *Collector) prefetchAvailable(ctx context.Context) {
	if c.attachments == nil || c.download == nil {
		return
	}
	for ctx.Err() == nil {
		if c.client != nil && !c.client.IsLoggedIn() {
			return
		}
		jobs, err := c.attachments.PendingAttachments(1)
		if err != nil || len(jobs) == 0 {
			return
		}
		job := jobs[0]
		downloadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = c.attachments.CacheAttachment(downloadCtx, job, func(ctx context.Context, file archive.AttachmentFile) error {
			if !c.archive.IsAllowed(job.ChatJID) || !validMediaDescriptor(job.Descriptor) {
				return errors.New("attachment is not eligible")
			}
			return c.download(ctx, job.Descriptor, file)
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			c.logger.Warn("attachment prefetch could not complete", "reason", attachmentErrorReason(err))
		}
	}
}
