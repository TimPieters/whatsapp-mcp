package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"whatsapp-client/internal/collector"
)

func TestRecoveryCommandIsPrivateAndDoesNotOverwriteRequests(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"data_dir":".","allowed_chats":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"recover-attachments", "--config", path}); err == nil {
		t.Fatal("empty allowlist queued recovery")
	}
	if err := changeAllowed(path, "15551234567@s.whatsapp.net", "Example", true); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"recover-attachments", "--config", path}); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, collector.MediaRecoveryRequestName)
	data, err := os.ReadFile(marker)
	if err != nil || string(data) != collector.MediaRecoveryRequestContent {
		t.Fatal("invalid recovery request")
	}
	if err := run([]string{"recover-attachments", "--config", path}); err == nil {
		t.Fatal("duplicate recovery request accepted")
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "media-recovery.processing"), []byte("active"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"recover-attachments", "--config", path}); err == nil {
		t.Fatal("processing request bypassed")
	}
}

func TestRecoveryStartupCleanupPreservesPendingRequest(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{collector.MediaRecoveryRequestName, "media-recovery.processing"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(collector.MediaRecoveryRequestContent), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := removeStaleRecoveryProcessing(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "media-recovery.processing")); !os.IsNotExist(err) {
		t.Fatal("stale processing marker remains")
	}
	if _, err := os.Stat(filepath.Join(dir, collector.MediaRecoveryRequestName)); err != nil {
		t.Fatal("pending request removed")
	}
}

func TestTokenFileRejectsEmptyShortOversizedAndWhitespace(t *testing.T) {
	for _, input := range []string{"", "short", strings.Repeat("a", 1025), strings.Repeat("a", 32) + " " + strings.Repeat("b", 32), strings.Repeat("a", 32) + "\n" + strings.Repeat("b", 32)} {
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readToken(path); err == nil {
			t.Fatalf("accepted invalid token length %d", len(input))
		}
	}
	path := filepath.Join(t.TempDir(), "token")
	want := strings.Repeat("a", 64)
	if err := os.WriteFile(path, []byte(want+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readToken(path); err != nil || got != want {
		t.Fatalf("valid generated token got=%q err=%v", got, err)
	}
}

func TestStateLockExcludesOtherProcessesAndReleases(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	first, err := lockState(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Unlock() })
	if second, err := lockState(dir); err == nil {
		second.Unlock()
		t.Fatal("second writer acquired state lock")
	}
	if err := first.Unlock(); err != nil {
		t.Fatal(err)
	}
	second, err := lockState(dir)
	if err != nil {
		t.Fatalf("released lock not reacquired: %v", err)
	}
	second.Unlock()
}

func TestConfigPathsResolveAgainstConfigAndAllowEditPreservesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"data_dir":"private-data","token_file":"private-token","allowed_chats":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := changeAllowed(path, "15551234567@s.whatsapp.net", "Approved", true); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"data_dir": "private-data"`) {
		t.Fatal("allow edit changed path interpretation")
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != filepath.Join(dir, "private-data") || cfg.TokenFile != filepath.Join(dir, "private-token") {
		t.Fatalf("unexpected resolved paths %+v", cfg)
	}
	if err := changeAllowed(path, "15551234567@s.whatsapp.net", "", false); err != nil {
		t.Fatal(err)
	}
	cfg, err = loadConfig(path)
	if err != nil || len(cfg.AllowedChats) != 0 {
		t.Fatalf("deny did not remove approval: %+v %v", cfg.AllowedChats, err)
	}
}

func TestNoSendOrPairAdminCommandExists(t *testing.T) {
	for _, name := range []string{"send", "send-message", "pair", "reload"} {
		if err := run([]string{name}); err == nil {
			t.Fatalf("unexpected command %s", name)
		}
	}
}
