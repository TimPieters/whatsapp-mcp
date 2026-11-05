package collector

import (
	"context"
	"database/sql"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func TestPrivateSessionInitializationAndFirstPairing(t *testing.T) {
	dir := t.TempDir()
	archiveStore, err := archive.Open(filepath.Join(dir, "archive.db"), config.Default())
	if err != nil {
		t.Fatal(err)
	}
	defer archiveStore.Close()
	c, err := New(dir, archiveStore, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.Status() != (Status{}) {
		t.Fatal("fresh session unexpectedly connected or paired")
	}
	if c.client.EnableDecryptedEventBuffer || c.client.UseRetryMessageStore || c.client.AutomaticMessageRerequestFromPhone || !c.client.ManualHistorySyncDownload {
		t.Fatal("private transport settings were not applied")
	}
	device := c.client.Store
	jid := types.NewJID("1234567890", types.DefaultUserServer)
	device.ID = &jid
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details: []byte("synthetic"), AccountSignature: make([]byte, 64),
		AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64),
	}
	// First pairing initializes SQL stores inside PutDevice. The wrapper must
	// replace those stores before Save returns, rather than on a later event.
	if err := device.Save(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := device.Contacts.(*store.NoopStore); !ok {
		t.Fatal("first pairing restored contact persistence")
	}
	if _, ok := device.MsgSecrets.(*store.NoopStore); !ok {
		t.Fatal("first pairing restored message-secret persistence")
	}
	if _, ok := device.ChatSettings.(*store.NoopStore); !ok {
		t.Fatal("first pairing restored chat-setting persistence")
	}
	if _, ok := device.EventBuffer.(*store.NoopStore); !ok {
		t.Fatal("first pairing restored plaintext-event persistence")
	}
	if _, ok := device.Container.(*privateContainer); !ok {
		t.Fatal("first pairing removed private container wrapper")
	}
	if err := device.Contacts.PutContactName(context.Background(), jid, "discarded", "discarded"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "session.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var contacts int
	if err := db.QueryRow("SELECT COUNT(*) FROM whatsmeow_contacts").Scan(&contacts); err != nil {
		t.Fatal(err)
	}
	if contacts != 0 {
		t.Fatal("contact names reached protocol database")
	}
}

func TestPairingImageIsCopiedAndExpires(t *testing.T) {
	c := &Collector{qrPNG: []byte{1, 2, 3}, qrExpires: time.Now().Add(time.Minute)}
	image := c.PairingPNG()
	image[0] = 9
	if c.PairingPNG()[0] != 1 {
		t.Fatal("pairing response exposed mutable internal image")
	}
	c.qrExpires = time.Now().Add(-time.Second)
	if c.PairingPNG() != nil {
		t.Fatal("expired pairing image was returned")
	}
	c.clearQR()
	if c.qrPNG != nil {
		t.Fatal("pairing image remained after clear")
	}
}

func TestCollectorDoesNotCallUserWriteOrMediaDownloadAPIs(t *testing.T) {
	for _, filename := range []string{"collector.go", "ingest.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filename, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch selector.Sel.Name {
			case "SendMessage", "SendPresence", "SendChatPresence", "MarkRead", "BuildReaction", "Download", "DownloadAny":
				t.Errorf("collector calls forbidden application API %s", selector.Sel.Name)
			}
			return true
		})
	}
}
