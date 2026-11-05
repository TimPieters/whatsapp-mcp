// whatsapp-mcp runs a selected-chat, read-only archive and MCP service.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gofrs/flock"
	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/collector"
	"whatsapp-client/internal/config"
	"whatsapp-client/internal/migration"
	"whatsapp-client/internal/service"
)

var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: whatsapp-mcp <init|serve|allow|deny|qr|recover-attachments|recover-message-copies|restore-attachment|export-upstream|import|version> [options]")
	}
	if args[0] == "version" {
		fmt.Println(version)
		return nil
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	configPath := fs.String("config", "config.json", "configuration file")
	dataDir := fs.String("data-dir", "data", "private state directory, for init")
	tokenPath := fs.String("token-file", "token", "bearer-token file, for init")
	jid := fs.String("jid", "", "exact chat identifier, for allow or deny")
	messageID := fs.String("message-id", "", "exact message identifier, for restore-attachment")
	label := fs.String("label", "", "local label, for allow")
	source := fs.String("source", "", "migration source file")
	output := fs.String("output", "", "migration or QR output file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	switch args[0] {
	case "init":
		return initialize(*configPath, *dataDir, *tokenPath)
	case "allow", "deny":
		return changeAllowed(*configPath, *jid, *label, args[0] == "allow")
	case "serve":
		return serve(*configPath)
	case "restore-attachment":
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		if *source == "" || *jid == "" || *messageID == "" {
			return errors.New("--source, --jid and --message-id are required")
		}
		in, err := os.Open(*source)
		if err != nil {
			return errors.New("cannot open local attachment source")
		}
		defer in.Close()
		info, err := in.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > cfg.MaxAttachmentBytes {
			return errors.New("local source is not a bounded regular file")
		}
		lock, err := lockState(cfg.DataDir)
		if err != nil {
			return err
		}
		defer lock.Unlock()
		store, err := archive.Open(filepath.Join(cfg.DataDir, "archive.db"), cfg)
		if err != nil {
			return err
		}
		defer store.Close()
		if err := store.RestoreAttachment(context.Background(), *jid, *messageID, in); err != nil {
			return errors.New("local attachment restore failed approval, availability, size or integrity checks")
		}
		fmt.Println("Attachment verified and available in the approved cache.")
		return nil
	case "recover-attachments", "recover-message-copies":
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		if len(cfg.AllowedChats) == 0 {
			return errors.New("approve chats before requesting attachment recovery")
		}
		requestName, requestContent, processingName := collector.MediaRecoveryRequestName, collector.MediaRecoveryRequestContent, "media-recovery.processing"
		if args[0] == "recover-message-copies" {
			requestName, requestContent, processingName = collector.MessageCopyRequestName, collector.MessageCopyRequestContent, "message-copy.processing"
		}
		if _, err := os.Lstat(filepath.Join(cfg.DataDir, processingName)); err == nil {
			return errors.New("an attachment recovery request is already processing")
		} else if !errors.Is(err, os.ErrNotExist) {
			return errors.New("cannot inspect the private recovery state")
		}
		if err := writeNew(filepath.Join(cfg.DataDir, requestName), []byte(requestContent)); err != nil {
			return errors.New("cannot queue recovery; run as the state owner and check for an existing request")
		}
		fmt.Println("Queued bounded owner-phone recovery for eligible failed attachments. The connected service will process it; files absent from the phone may remain unavailable.")
		return nil
	case "qr":
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		if *output == "" {
			return errors.New("--output is required")
		}
		png, err := os.ReadFile(filepath.Join(cfg.DataDir, "pairing.png"))
		if err != nil {
			return errors.New("no active pairing QR; start the service and run this as the state owner")
		}
		if len(png) > 128*1024 {
			return errors.New("invalid pairing image")
		}
		return writeNew(*output, png)
	case "export-upstream":
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		if *source == "" || *output == "" {
			return errors.New("--source and --output are required")
		}
		out, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		report, exportErr := migration.ExportUpstream(context.Background(), *source, cfg, out)
		closeErr := out.Close()
		if exportErr != nil {
			_ = os.Remove(*output)
			return exportErr
		}
		if closeErr != nil {
			return closeErr
		}
		return json.NewEncoder(os.Stdout).Encode(report)
	case "import":
		cfg, err := loadConfig(*configPath)
		if err != nil {
			return err
		}
		if *source == "" {
			return errors.New("--source is required")
		}
		lock, err := lockState(cfg.DataDir)
		if err != nil {
			return err
		}
		defer lock.Unlock()
		store, err := archive.Open(filepath.Join(cfg.DataDir, "archive.db"), cfg)
		if err != nil {
			return err
		}
		defer store.Close()
		in, err := os.Open(*source)
		if err != nil {
			return err
		}
		defer in.Close()
		report, importErr := migration.ImportJSONL(context.Background(), in, store)
		_ = json.NewEncoder(os.Stdout).Encode(report)
		return importErr
	default:
		return errors.New("unknown command")
	}
}

func loadConfig(path string) (config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		return cfg, errors.New("configuration is missing or invalid")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return cfg, err
	}
	base := filepath.Dir(absolute)
	if !filepath.IsAbs(cfg.DataDir) {
		cfg.DataDir = filepath.Join(base, cfg.DataDir)
	}
	if cfg.TokenFile != "" && !filepath.IsAbs(cfg.TokenFile) {
		cfg.TokenFile = filepath.Join(base, cfg.TokenFile)
	}
	return cfg, nil
}

func writeNew(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func initialize(path, data, token string) error {
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return errors.New("configuration already exists or cannot be checked")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	cfg := config.Default()
	cfg.AllowedChats = []config.Chat{}
	cfg.DataDir, err = filepath.Abs(data)
	if err != nil {
		return err
	}
	cfg.TokenFile, err = filepath.Abs(token)
	if err != nil {
		return err
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(abs), 0750); err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(cfg.TokenFile), 0750); err != nil {
		return err
	}
	if err = writeNew(cfg.TokenFile, []byte(hex.EncodeToString(secret)+"\n")); err != nil {
		return err
	}
	encoded, _ := json.MarshalIndent(cfg, "", "  ")
	if err = writeNew(abs, append(encoded, '\n')); err != nil {
		return err
	}
	fmt.Println("Created configuration with an empty allowlist and a private token file.")
	return nil
}

func changeAllowed(path, jid, label string, add bool) error {
	cfg, err := config.Load(path)
	if err != nil {
		return errors.New("configuration is missing or invalid")
	}
	if !config.ValidJID(jid) {
		return errors.New("an exact supported --jid is required")
	}
	selected := make([]config.Chat, 0, len(cfg.AllowedChats)+1)
	for _, chat := range cfg.AllowedChats {
		if chat.JID != jid {
			selected = append(selected, chat)
		}
	}
	if add {
		selected = append(selected, config.Chat{JID: jid, Label: label})
	}
	cfg.AllowedChats = selected
	if err = cfg.Validate(); err != nil {
		return err
	}
	encoded, _ := json.MarshalIndent(cfg, "", "  ")
	// Preserve ownership/group. A reader observing an incomplete edit suspends.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		return err
	}
	_, err = f.Write(append(encoded, '\n'))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Println("Updated allowlist. A running service checks for changes every five seconds; an active read may finish first.")
	return nil
}

func lockState(dir string) (*flock.Flock, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if runtime.GOOS != "windows" && st.Mode().Perm()&0077 != 0 {
		return nil, errors.New("state directory must have owner-only permissions (0700)")
	}
	lock := flock.New(filepath.Join(dir, "service.lock"))
	ok, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("state is in use; stop the service before running an import")
	}
	return lock, nil
}

func serve(path string) error {
	if os.Getenv("GOMEMLIMIT") == "" {
		debug.SetMemoryLimit(256 << 20)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		return err
	}
	host, _, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("listen must use a literal loopback address; use an authenticated private proxy for remote clients")
	}
	token, err := readToken(cfg.TokenFile)
	if err != nil {
		return err
	}
	lock, err := lockState(cfg.DataDir)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	// A crash may leave expired pairing material. Never reuse it after restart.
	if err = removePairingFiles(cfg.DataDir); err != nil {
		return err
	}
	if err = removeStaleRecoveryProcessing(cfg.DataDir); err != nil {
		return err
	}
	store, err := archive.Open(filepath.Join(cfg.DataDir, "archive.db"), cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	source, err := collector.New(cfg.DataDir, store, logger)
	if err != nil {
		return err
	}
	defer source.Close()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	handler := service.New(store, source, token, version)
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
	var workers sync.WaitGroup
	workers.Add(1)
	go func() { defer workers.Done(); watch(ctx, path, cfg, store, source, logger) }()
	defer func() {
		cancel()
		shutdownCtx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		server.Shutdown(shutdownCtx)
		workers.Wait()
		_ = removePairingFiles(cfg.DataDir)
	}()
	if err = source.Connect(ctx); err != nil {
		return errors.New("could not connect the message collector")
	}
	logger.Info("read-only service started", "listen", cfg.Listen)
	select {
	case <-ctx.Done():
		return nil
	case err = <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func readToken(path string) (string, error) {
	if path == "" {
		return "", errors.New("token_file is required")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("cannot read token_file")
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil || len(b) > 1024 {
		return "", errors.New("invalid token file")
	}
	token := strings.TrimSpace(string(b))
	if len(token) < 32 || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("token must contain at least 32 non-whitespace characters")
	}
	return token, nil
}

func watch(ctx context.Context, path string, initial config.Config, store *archive.Store, source *collector.Collector, logger *slog.Logger) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	last, _ := json.Marshal(initial)
	var lastQR []byte
	counter := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			png := source.PairingPNG()
			qrPath := filepath.Join(initial.DataDir, "pairing.png")
			if !bytes.Equal(png, lastQR) {
				var qrErr error
				if len(png) == 0 {
					qrErr = removePairingFiles(initial.DataDir)
				} else {
					tmp := qrPath + ".tmp"
					qrErr = os.WriteFile(tmp, png, 0600)
					if qrErr == nil {
						qrErr = os.Rename(tmp, qrPath)
					}
				}
				if qrErr == nil {
					lastQR = append(lastQR[:0], png...)
				}
			}
			counter++
			if counter%5 != 0 {
				continue
			}
			next, err := loadConfig(path)
			if err != nil || next.Listen != initial.Listen || next.DataDir != initial.DataDir || next.TokenFile != initial.TokenFile {
				store.Suspend()
				last = nil
				continue
			}
			encoded, _ := json.Marshal(next)
			if !bytes.Equal(last, encoded) {
				if err = store.ApplyConfig(next); err != nil {
					logger.Error("configuration reload failed; archive suspended")
					continue
				}
				last = encoded
				logger.Info("archive policy reloaded")
			}
			if counter%3600 == 0 {
				if err = store.Maintain(); err != nil {
					logger.Error("archive maintenance failed")
				}
			}
		}
	}
}

func removePairingFiles(dir string) error {
	for _, name := range []string{"pairing.png", "pairing.png.tmp"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return errors.New("could not remove stale pairing material")
		}
	}
	return nil
}

// Call only after the exclusive daemon state lock is held. The archive's durable
// per-file attempt flags remain intact; a crash must not cause repeated receipts.
func removeStaleRecoveryProcessing(dir string) error {
	for _, name := range []string{"media-recovery.processing", "message-copy.processing"} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.New("could not clear stale private recovery processing marker")
		}
	}
	return nil
}
