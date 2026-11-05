// Package collector receives messages and stores only explicitly allowed chats.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"whatsapp-client/internal/archive"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	_ "modernc.org/sqlite"
	"rsc.io/qr"
)

// Status contains no account identifiers or message content.
type Status struct {
	Connected bool `json:"connected"`
	Paired    bool `json:"paired"`
}

type messageStore interface {
	IsAllowed(string) bool
	Ingest([]archive.Message) error
}

// Collector receives approved messages and prefetches their bounded attachments.
// It has no application operation for sending messages.
type Collector struct {
	client      *whatsmeow.Client
	session     *sqlstore.Container
	archive     messageStore
	logger      *slog.Logger
	paired      atomic.Bool
	handlerID   uint32
	attachments attachmentStore
	download    mediaDownload
	mediaWake   chan struct{}
	mediaHTTP   *http.Transport

	lifecycle sync.Mutex
	started   bool
	closed    bool
	cancel    context.CancelFunc
	workers   sync.WaitGroup
	events    sync.Mutex

	qrMu      sync.RWMutex
	qrPNG     []byte
	qrExpires time.Time
}

// New opens private protocol state separately from the searchable archive.
func New(dataDir string, archiveStore *archive.Store, logger *slog.Logger) (*Collector, error) {
	if archiveStore == nil {
		return nil, errors.New("archive store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, errors.New("could not create private session directory")
	}
	path, err := filepath.Abs(filepath.Join(dataDir, "session.db"))
	if err != nil {
		return nil, errors.New("could not resolve private session path")
	}
	// Create with restrictive permissions before SQLite opens the file.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("could not open private session file")
	}
	if err = f.Chmod(0600); err != nil {
		_ = f.Close()
		return nil, errors.New("could not protect private session file")
	}
	if err = f.Close(); err != nil {
		return nil, errors.New("could not close private session file")
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(path)}
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	q := u.Query()
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "busy_timeout(5000)")
	u.RawQuery = q.Encode()
	session, err := sqlstore.New(context.Background(), "sqlite", u.String(), waLog.Noop)
	if err != nil {
		return nil, errors.New("could not initialize private protocol state")
	}
	device, err := session.GetFirstDevice(context.Background())
	if err != nil {
		_ = session.Close()
		return nil, errors.New("could not load linked-device state")
	}
	private := &privateContainer{inner: session}
	private.minimize(device)
	client := whatsmeow.NewClient(device, waLog.Noop)
	client.EnableDecryptedEventBuffer = false
	client.UseRetryMessageStore = false
	client.AutomaticMessageRerequestFromPhone = false
	// Skip account-wide history downloads. Targeted backfill requires a separate
	// explicit operation; already-delivered history is still filtered defensively.
	client.ManualHistorySyncDownload = true
	// Read receipts require explicit MarkRead calls in this library version.
	// This package never calls that API or any user-message sending API.
	c := &Collector{client: client, session: session, archive: archiveStore, logger: logger}
	c.attachments = archiveStore
	c.mediaWake = make(chan struct{}, 1)
	mediaClient, transport := newMediaHTTPClient()
	client.SetMediaHTTPClient(mediaClient)
	c.mediaHTTP = transport
	c.download = func(ctx context.Context, descriptor archive.AttachmentDescriptor, file archive.AttachmentFile) error {
		return client.DownloadToFile(ctx, downloadable{descriptor}, file)
	}
	c.paired.Store(device.ID != nil)
	c.handlerID = client.AddEventHandler(c.handleEvent)
	return c, nil
}

// Connect initiates the connection without waiting for QR pairing to finish.
func (c *Collector) Connect(ctx context.Context) error {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.closed {
		return errors.New("collector is closed")
	}
	if c.started {
		return errors.New("collector connection already initiated")
	}
	runCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	if !c.paired.Load() {
		codes, err := c.client.GetQRChannel(runCtx)
		if err != nil {
			cancel()
			return errors.New("could not initialize pairing")
		}
		c.workers.Add(1)
		go func() {
			defer c.workers.Done()
			defer c.clearQR()
			for {
				select {
				case <-runCtx.Done():
					return
				case item, ok := <-codes:
					if !ok {
						return
					}
					if item.Event != whatsmeow.QRChannelEventCode {
						c.clearQR()
						continue
					}
					code, err := qr.Encode(item.Code, qr.M)
					if err != nil {
						c.logger.Warn("pairing image could not be generated")
						continue
					}
					c.qrMu.Lock()
					c.qrPNG = code.PNG()
					c.qrExpires = time.Now().Add(item.Timeout)
					c.qrMu.Unlock()
				}
			}
		}()
	}
	if err := c.client.ConnectContext(runCtx); err != nil {
		cancel()
		c.clearQR()
		return errors.New("could not initiate linked-device connection")
	}
	c.started = true
	c.workers.Add(1)
	go func() {
		defer c.workers.Done()
		c.prefetchLoop(runCtx)
	}()
	return nil
}

func (c *Collector) Status() Status {
	return Status{Connected: c.client.IsConnected() && c.client.IsLoggedIn(), Paired: c.paired.Load()}
}

// PairingPNG returns a copy of the current, unexpired in-memory pairing image.
func (c *Collector) PairingPNG() []byte {
	c.qrMu.RLock()
	defer c.qrMu.RUnlock()
	if time.Now().After(c.qrExpires) {
		return nil
	}
	return append([]byte(nil), c.qrPNG...)
}

func (c *Collector) clearQR() {
	c.qrMu.Lock()
	clear(c.qrPNG)
	c.qrPNG = nil
	c.qrExpires = time.Time{}
	c.qrMu.Unlock()
}

// Close disconnects transport and releases the private session database.
func (c *Collector) Close() {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	if c.cancel != nil {
		c.cancel()
	}
	c.client.Disconnect()
	c.client.RemoveEventHandler(c.handlerID)
	c.workers.Wait()
	if c.mediaHTTP != nil {
		c.mediaHTTP.CloseIdleConnections()
	}
	c.clearQR()
	c.events.Lock()
	defer c.events.Unlock()
	if err := c.session.Close(); err != nil {
		c.logger.Warn("private protocol state could not be closed")
	}
}

// privateContainer reapplies nonpersistent stores after first-pair initialization.
// The library's SQL container replaces all stores during the first device save.
type privateContainer struct{ inner *sqlstore.Container }

func (p *privateContainer) minimize(device *store.Device) {
	noop := &store.NoopStore{}
	device.Contacts = noop
	device.ChatSettings = noop
	device.MsgSecrets = noop
	device.PrivacyTokens = noop
	device.EventBuffer = noop
	device.Container = p
}

func (p *privateContainer) PutDevice(ctx context.Context, device *store.Device) error {
	err := p.inner.PutDevice(ctx, device)
	p.minimize(device)
	return err
}

func (p *privateContainer) DeleteDevice(ctx context.Context, device *store.Device) error {
	return p.inner.DeleteDevice(ctx, device)
}

func (c *Collector) handleEvent(raw any) {
	c.events.Lock()
	defer c.events.Unlock()
	switch event := raw.(type) {
	case *events.Message:
		c.handleMessage(event)
	case *events.HistorySync:
		c.handleHistory(event)
	case *events.PairSuccess:
		c.paired.Store(true)
		c.clearQR()
	case *events.LoggedOut:
		c.paired.Store(false)
		c.clearQR()
	case *events.Connected:
		c.wakePrefetch()
	}
}
