package archive

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"whatsapp-client/internal/config"
)

const approved = "123@s.whatsapp.net"
const excluded = "456@s.whatsapp.net"

func approvedConfig() config.Config {
	c := config.Default()
	c.AllowedChats = []config.Chat{{JID: approved, Label: "Chosen chat"}}
	return c
}

func openTestStore(t *testing.T, c config.Config) (*Store, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "archive.db")
	s, err := Open(p, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, p
}

func message(id, jid, text string) Message {
	return Message{ID: id, ChatJID: jid, Sender: jid, Text: text, Timestamp: time.Now().Unix()}
}

func assertAbsentOnDisk(t *testing.T, path, literal string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(path), entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(literal)) {
			t.Fatalf("literal remains in %s", entry.Name())
		}
	}
}

func TestExcludedContentNeverReachesDatabase(t *testing.T) {
	s, path := openTestStore(t, approvedConfig())
	secret := "EXCLUDED_LITERAL_8e31b752"
	if err := s.Ingest([]Message{message("a", approved, "approved text"), message("b", excluded, secret)}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Search("text", "", 100)
	if err != nil || len(got) != 1 {
		t.Fatalf("search: %v %+v", err, got)
	}
	if _, err := s.ListMessages(excluded, 0, 10); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("excluded query: %v", err)
	}
	if _, err := s.Search(secret, excluded, 10); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("excluded search: %v", err)
	}
	stats, err := s.Stats()
	if err != nil || stats.Messages != 1 || stats.Chats != 1 {
		t.Fatalf("stats: %v %+v", err, stats)
	}
	assertAbsentOnDisk(t, path, secret)
}

func TestDefaultDenyAndAliases(t *testing.T) {
	s, path := openTestStore(t, config.Config{})
	secret := "DEFAULT_DENY_0d3c1c"
	if err := s.Ingest([]Message{message("a", approved, secret)}); err != nil {
		t.Fatal(err)
	}
	if s.IsAllowed(approved) {
		t.Fatal("empty allowlist admitted chat")
	}
	if err := s.ApplyConfig(approvedConfig()); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"123@lid", "123:2@s.whatsapp.net", "123@g.us", "+123@s.whatsapp.net", "123@s.whatsapp.net "} {
		if s.IsAllowed(alias) {
			t.Errorf("alias bypass: %q", alias)
		}
		if err := s.Ingest([]Message{message("alias", alias, secret)}); err != nil {
			t.Fatal(err)
		}
	}
	assertAbsentOnDisk(t, path, secret)
}

func TestInvalidPolicySuspendsWithoutDeleting(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	if err := s.Ingest([]Message{message("a", approved, "retained")}); err != nil {
		t.Fatal(err)
	}
	bad := approvedConfig()
	bad.AllowedChats = []config.Chat{{JID: "*"}}
	if err := s.ApplyConfig(bad); err == nil {
		t.Fatal("invalid policy accepted")
	}
	if s.IsAllowed(approved) {
		t.Fatal("suspended store admits chat")
	}
	if _, err := s.Search("retained", "", 1); !errors.Is(err, ErrSuspended) {
		t.Fatalf("search: %v", err)
	}
	if _, err := s.ListChats(1); !errors.Is(err, ErrSuspended) {
		t.Fatalf("chats: %v", err)
	}
	if _, err := s.Stats(); !errors.Is(err, ErrSuspended) {
		t.Fatalf("stats: %v", err)
	}
	if err := s.Ingest([]Message{message("b", approved, "new")}); !errors.Is(err, ErrSuspended) {
		t.Fatalf("ingest: %v", err)
	}
	if err := s.ApplyConfig(approvedConfig()); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListMessages(approved, 0, 10)
	if err != nil || len(got) != 1 || got[0].Text != "retained" {
		t.Fatalf("recovery: %v %+v", err, got)
	}
	s.Suspend()
	if _, err := s.ListMessages(approved, 0, 10); !errors.Is(err, ErrSuspended) {
		t.Fatalf("explicit suspension: %v", err)
	}
}

func TestRemovalPurgesQueriesAndDisk(t *testing.T) {
	c := approvedConfig()
	c.AllowedChats = append(c.AllowedChats, config.Chat{JID: excluded, Label: "Second"})
	s, path := openTestStore(t, c)
	secret := "REMOVED_LITERAL_6d3a9fdd"
	if err := s.Ingest([]Message{message("a", approved, "kept"), message("b", excluded, secret)}); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplyConfig(approvedConfig()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ListMessages(excluded, 0, 10); !errors.Is(err, ErrNotAllowed) {
		t.Fatalf("removed chat: %v", err)
	}
	got, err := s.Search(secret, "", 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("removed search: %v %+v", err, got)
	}
	assertAbsentOnDisk(t, path, secret)
	if err := s.ApplyConfig(c); err != nil {
		t.Fatal(err)
	}
	got, err = s.ListMessages(excluded, 0, 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("removed history resurrected: %v %+v", err, got)
	}
}

func TestRemovalSerializesWithConcurrentIngestion(t *testing.T) {
	s, path := openTestStore(t, approvedConfig())
	secret := "RACE_REMOVED_LITERAL_94201"
	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 25; i++ {
				if err := s.Ingest([]Message{message(fmt.Sprintf("%d-%d", worker, i), approved, secret)}); err != nil {
					t.Errorf("ingest: %v", err)
				}
			}
		}(worker)
	}
	close(start)
	if err := s.ApplyConfig(config.Default()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	got, err := s.Search(secret, "", 10)
	if err != nil || len(got) != 0 {
		t.Fatalf("race leaked data: %v %+v", err, got)
	}
	assertAbsentOnDisk(t, path, secret)
}

func TestRetentionStorageAndPageBounds(t *testing.T) {
	c := approvedConfig()
	c.RetentionDays = 2
	c.MaxMessagesPerChat = 2
	c.MaxResults = 1
	s, _ := openTestStore(t, c)
	msgs := []Message{}
	for i := 0; i < 5; i++ {
		m := message(fmt.Sprint(i), approved, "retained text")
		m.Timestamp -= int64(i * 86400)
		msgs = append(msgs, m)
	}
	if err := s.Ingest(msgs); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Stats()
	if err != nil || stats.Messages != 2 {
		t.Fatalf("bounded stats: %v %+v", err, stats)
	}
	for _, limit := range []int{-1, 0, 999999} {
		got, err := s.ListMessages(approved, 0, limit)
		if err != nil || len(got) != 1 || got[0].ID != "0" {
			t.Fatalf("bounded results: %v %+v", err, got)
		}
	}
	c.MaxMessagesPerChat = 1
	if err := s.ApplyConfig(c); err != nil {
		t.Fatal(err)
	}
	stats, err = s.Stats()
	if err != nil || stats.Messages != 1 {
		t.Fatalf("apply cap: %v %+v", err, stats)
	}
}

func TestLiteralSearchAndPayloadBounds(t *testing.T) {
	s, _ := openTestStore(t, approvedConfig())
	if err := s.Ingest([]Message{message("a", approved, "100%_done\\file"), message("b", approved, "ordinary")}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Search("%_done\\", "", 100)
	if err != nil || len(got) != 1 || got[0].ID != "a" {
		t.Fatalf("literal LIKE: %v %+v", err, got)
	}
	if err := s.Ingest([]Message{message("c", approved, strings.Repeat("x", MaxTextBytes+1))}); err == nil {
		t.Fatal("oversized text accepted")
	}
	badSender := message("d", approved, "text")
	badSender.Sender = "Contact full name"
	if err := s.Ingest([]Message{badSender}); err == nil {
		t.Fatal("contact name accepted as sender")
	}
	if _, err := s.Search(strings.Repeat("q", 1025), "", 1); err == nil {
		t.Fatal("oversized query accepted")
	}
}

func TestStartupEnforcesChangedPolicyAndRetention(t *testing.T) {
	c := approvedConfig()
	c.RetentionDays = 10
	c.AllowedChats = append(c.AllowedChats, config.Chat{JID: excluded})
	s, path := openTestStore(t, c)
	old := message("old", approved, "EXPIRED_STARTUP_LITERAL_98631")
	old.Timestamp -= 3 * 86400
	removed := message("removed", excluded, "REVOKED_STARTUP_LITERAL_14628")
	if err := s.Ingest([]Message{old, removed, message("new", approved, "kept")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	c = approvedConfig()
	c.RetentionDays = 1
	reopened, err := Open(path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	stats, err := reopened.Stats()
	if err != nil || stats.Chats != 1 || stats.Messages != 1 {
		t.Fatalf("startup enforcement: %v %+v", err, stats)
	}
	assertAbsentOnDisk(t, path, old.Text)
	assertAbsentOnDisk(t, path, removed.Text)
}

func TestScheduledMaintenanceExpiresInactiveChats(t *testing.T) {
	c := approvedConfig()
	c.AllowedChats = append(c.AllowedChats, config.Chat{JID: excluded})
	s, _ := openTestStore(t, c)
	// Represent a row that aged out while its chat was inactive.
	_, err := s.db.Exec(`INSERT INTO messages VALUES(?,?,?,?,?,?,?)`, "inactive", excluded, "456", "expired", "", time.Now().Unix()-100*86400, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ingest([]Message{message("active", approved, "live")}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid=?", excluded).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("live ingestion performed unrelated-chat maintenance")
	}
	if err := s.Maintain(); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT COUNT(*) FROM messages WHERE chat_jid=?", excluded).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("scheduled maintenance retained expired inactive history")
	}
}
