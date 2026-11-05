// Package archive stores and searches messages from explicitly approved chats.
package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
	"whatsapp-client/internal/config"
)

var (
	ErrSuspended  = errors.New("archive access is suspended")
	ErrNotAllowed = errors.New("chat is not approved")
	ErrClosed     = errors.New("archive is closed")
)

const MaxTextBytes = 64 * 1024

type Message struct {
	ID         string          `json:"id"`
	ChatJID    string          `json:"chat_jid"`
	Sender     string          `json:"sender"`
	Text       string          `json:"text"`
	MediaType  string          `json:"media_type,omitempty"`
	Timestamp  int64           `json:"timestamp"`
	FromMe     bool            `json:"from_me"`
	Attachment *AttachmentInfo `json:"attachment,omitempty"`
}

type Chat struct {
	JID           string `json:"jid"`
	Label         string `json:"label"`
	MessageCount  int    `json:"message_count"`
	LastMessageAt int64  `json:"last_message_at"`
}

type Stats struct {
	Chats           int   `json:"chats"`
	Messages        int   `json:"messages"`
	OldestMessageAt int64 `json:"oldest_message_at"`
	LastMessageAt   int64 `json:"last_message_at"`
}

// Store serializes policy updates, reads, and writes. Returning from ApplyConfig
// means no operation using the previous policy can still complete afterward.
type Store struct {
	mu         sync.Mutex
	db         *sql.DB
	cfg        config.Config
	allowed    map[string]string
	suspended  bool
	closed     bool
	cacheDir   string
	generation uint64
	flights    map[string]*attachmentFlight
}

func Open(dbPath string, cfg config.Config) (*Store, error) {
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dbPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0700); err != nil {
		return nil, err
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	u.RawQuery = "_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)&_pragma=secure_delete(1)"
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, suspended: true, cacheDir: filepath.Join(filepath.Dir(abs), "attachments"), flights: make(map[string]*attachmentFlight)}
	fail := func(err error) (*Store, error) { db.Close(); return nil, err }
	for _, stmt := range []string{
		"PRAGMA journal_mode=DELETE", "PRAGMA secure_delete=ON", "PRAGMA temp_store=MEMORY", "PRAGMA cache_size=-4096",
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT NOT NULL, chat_jid TEXT NOT NULL, sender TEXT NOT NULL,
			text TEXT NOT NULL, media_type TEXT NOT NULL, timestamp INTEGER NOT NULL,
			from_me INTEGER NOT NULL, PRIMARY KEY(chat_jid,id))`,
		"CREATE INDEX IF NOT EXISTS messages_chat_time ON messages(chat_jid,timestamp DESC,id DESC)",
		"CREATE INDEX IF NOT EXISTS messages_time ON messages(timestamp)",
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fail(err)
		}
	}
	if err := os.Chmod(abs, 0600); err != nil {
		return fail(err)
	}
	if err := s.initAttachments(); err != nil {
		return fail(err)
	}
	if err := s.ApplyConfig(cfg); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *Store) check() error {
	if s.closed {
		return ErrClosed
	}
	if s.suspended {
		return ErrSuspended
	}
	return nil
}

func (s *Store) Suspend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.suspended = true
	s.invalidateAttachmentsLocked()
}

func (s *Store) IsAllowed(jid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.check() != nil {
		return false
	}
	_, ok := s.allowed[jid]
	return ok
}

func (s *Store) ApplyConfig(cfg config.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.suspended = true
	s.invalidateAttachmentsLocked()
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}
	allowed := make(map[string]string, len(cfg.AllowedChats))
	for _, chat := range cfg.AllowedChats {
		allowed[chat.JID] = chat.Label
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query("SELECT DISTINCT chat_jid FROM messages")
	if err != nil {
		return err
	}
	var removed []string
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			rows.Close()
			return err
		}
		if _, ok := allowed[jid]; !ok {
			removed = append(removed, jid)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, jid := range removed {
		if _, err := tx.Exec("DELETE FROM messages WHERE chat_jid=?", jid); err != nil {
			return err
		}
	}
	if err := cleanup(tx, cfg, time.Now().Unix()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Compact after policy application, including startup, so deleted data from
	// previous limits or approval sets does not remain in unused database pages.
	if _, err := s.db.Exec("VACUUM"); err != nil {
		return fmt.Errorf("compact archive: %w", err)
	}
	s.cfg = cfg
	s.allowed = allowed
	if err := s.reconcileAttachmentsLocked(); err != nil {
		return err
	}
	s.suspended = false
	return nil
}

func cleanup(tx *sql.Tx, cfg config.Config, now int64) error {
	cutoff := now - int64(cfg.RetentionDays)*86400
	if _, err := tx.Exec("DELETE FROM messages WHERE timestamp < ?", cutoff); err != nil {
		return err
	}
	for _, chat := range cfg.AllowedChats {
		if _, err := capChat(tx, chat.JID, cfg.MaxMessagesPerChat); err != nil {
			return err
		}
	}
	return nil
}

func capChat(tx *sql.Tx, jid string, maxMessages int) (int64, error) {
	result, err := tx.Exec(`DELETE FROM messages WHERE chat_jid=? AND id IN (
		SELECT id FROM messages WHERE chat_jid=?
		ORDER BY timestamp DESC,id DESC LIMIT -1 OFFSET ?)`, jid, jid, maxMessages)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Maintain removes expired rows and checks storage caps without compacting the
// whole database. Call periodically to expire history in inactive chats.
func (s *Store) Maintain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := cleanup(tx, s.cfg, time.Now().Unix()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.reconcileAttachmentsLocked()
}

func (s *Store) Ingest(messages []Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ingestLocked(messages, nil)
}

func (s *Store) IngestWithAttachments(messages []Message, refs []AttachmentRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(refs) > 1000 {
		return errors.New("attachment batch exceeds 1000 records")
	}
	return s.ingestLocked(messages, refs)
}

func (s *Store) ingestLocked(messages []Message, refs []AttachmentRecord) error {
	if err := s.check(); err != nil {
		return err
	}
	if len(messages) > 1000 {
		return errors.New("ingestion batch exceeds 1000 messages")
	}
	now := time.Now().Unix()
	cutoff := now - int64(s.cfg.RetentionDays)*86400
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	touched := make(map[string]bool)
	pruned := false
	type attachmentKey struct{ jid, id string }
	incoming := make(map[attachmentKey]AttachmentDescriptor, len(refs))
	for _, ref := range refs {
		if _, ok := s.allowed[ref.ChatJID]; ok && ValidateAttachmentDescriptor(ref.Descriptor, MaxSupportedAttachmentBytes) == nil {
			incoming[attachmentKey{ref.ChatJID, ref.MessageID}] = ref.Descriptor
		}
	}
	for _, msg := range messages {
		// Reject before binding any unapproved message field to SQLite.
		if _, ok := s.allowed[msg.ChatJID]; !ok {
			continue
		}
		touched[msg.ChatJID] = true
		if msg.Timestamp < cutoff || msg.Timestamp > now+300 {
			continue
		}
		if !validSender(msg.Sender) {
			return errors.New("sender must be a canonical identity")
		}
		if msg.ID == "" || len(msg.ID) > 256 || len(msg.Sender) > 128 || len(msg.MediaType) > 64 || len(msg.Text) > MaxTextBytes || !utf8.ValidString(msg.Text) {
			return errors.New("approved message exceeds storage bounds or contains invalid text")
		}
		if msg.Text == "" && msg.MediaType == "" {
			continue
		}
		if _, err := tx.Exec(`INSERT INTO messages(id,chat_jid,sender,text,media_type,timestamp,from_me)
			VALUES(?,?,?,?,?,?,?) ON CONFLICT(chat_jid,id) DO UPDATE SET
			sender=excluded.sender,text=excluded.text,media_type=excluded.media_type,timestamp=excluded.timestamp,from_me=excluded.from_me`,
			msg.ID, msg.ChatJID, msg.Sender, msg.Text, msg.MediaType, msg.Timestamp, msg.FromMe); err != nil {
			return err
		}
		deleteSQL := "DELETE FROM attachments WHERE chat_jid=? AND message_id=?"
		deleteArgs := []any{msg.ChatJID, msg.ID}
		if descriptor, ok := incoming[attachmentKey{msg.ChatJID, msg.ID}]; ok && descriptor.MediaType == msg.MediaType {
			deleteSQL += " AND kind<>?"
			deleteArgs = append(deleteArgs, msg.MediaType)
		}
		result, err := tx.Exec(deleteSQL, deleteArgs...)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			pruned = true
		}
	}
	for jid := range touched {
		result, err := tx.Exec("DELETE FROM messages WHERE chat_jid=? AND timestamp<?", jid, cutoff)
		if err != nil {
			return err
		}
		if n, _ := result.RowsAffected(); n > 0 {
			pruned = true
		}
		if n, err := capChat(tx, jid, s.cfg.MaxMessagesPerChat); err != nil {
			return err
		} else if n > 0 {
			pruned = true
		}
	}
	for _, ref := range refs {
		if _, ok := s.allowed[ref.ChatJID]; !ok {
			continue
		}
		if ValidateAttachmentDescriptor(ref.Descriptor, MaxSupportedAttachmentBytes) != nil {
			continue
		}
		if err := s.registerAttachmentLocked(tx, ref.ChatJID, ref.MessageID, ref.Descriptor); err != nil {
			if errors.Is(err, ErrAttachmentUnavailable) {
				continue
			}
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	if pruned || len(refs) > 0 {
		return s.cleanOrphanCacheLocked()
	}
	return nil
}

func validSender(sender string) bool {
	if sender == "" || config.ValidJID(sender) {
		return true
	}
	if len(sender) > 128 {
		return false
	}
	for _, r := range sender {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func (s *Store) limit(n int) int {
	if n <= 0 || n > s.cfg.MaxResults {
		return s.cfg.MaxResults
	}
	return n
}

func (s *Store) ListChats(limit int) ([]Chat, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT chat_jid,COUNT(*),MAX(timestamp) FROM messages
		WHERE timestamp>=? GROUP BY chat_jid ORDER BY MAX(timestamp) DESC,chat_jid LIMIT ?`, time.Now().Unix()-int64(s.cfg.RetentionDays)*86400, s.limit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Chat{}
	for rows.Next() {
		var c Chat
		if err := rows.Scan(&c.JID, &c.MessageCount, &c.LastMessageAt); err != nil {
			return nil, err
		}
		label, ok := s.allowed[c.JID]
		if !ok {
			continue
		}
		c.Label = label
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) ListMessages(jid string, before int64, limit int) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if _, ok := s.allowed[jid]; !ok {
		return nil, ErrNotAllowed
	}
	if before <= 0 {
		before = time.Now().Unix() + 301
	}
	return s.queryMessages(`SELECT id,chat_jid,sender,text,media_type,timestamp,from_me FROM messages
		WHERE chat_jid=? AND timestamp<? AND timestamp>=? ORDER BY timestamp DESC,id DESC LIMIT ?`,
		jid, before, time.Now().Unix()-int64(s.cfg.RetentionDays)*86400, s.limit(limit))
}

func (s *Store) Search(query, jid string, limit int) ([]Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if len(query) == 0 || len(query) > 1024 || !utf8.ValidString(query) {
		return nil, errors.New("query must contain 1 to 1024 bytes of valid text")
	}
	if jid != "" {
		if _, ok := s.allowed[jid]; !ok {
			return nil, ErrNotAllowed
		}
	}
	q := `SELECT id,chat_jid,sender,text,media_type,timestamp,from_me FROM messages WHERE text LIKE ? ESCAPE '\' AND timestamp>=?`
	pattern := "%" + strings.NewReplacer("\\", "\\\\", "%", "\\%", "_", "\\_").Replace(query) + "%"
	args := []any{pattern, time.Now().Unix() - int64(s.cfg.RetentionDays)*86400}
	if jid != "" {
		q += " AND chat_jid=?"
		args = append(args, jid)
	}
	q += " ORDER BY timestamp DESC,id DESC LIMIT ?"
	args = append(args, s.limit(limit))
	return s.queryMessages(q, args...)
}

// queryMessages is called with mu held and checks every returned chat again.
func (s *Store) queryMessages(query string, args ...any) ([]Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Message{}
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.ChatJID, &m.Sender, &m.Text, &m.MediaType, &m.Timestamp, &m.FromMe); err != nil {
			return nil, err
		}
		if _, ok := s.allowed[m.ChatJID]; ok {
			out = append(out, m)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	for i := range out {
		if info, err := s.attachmentInfoLocked(out[i].ChatJID, out[i].ID); err == nil {
			out[i].Attachment = &info
		}
	}
	return out, nil
}

func (s *Store) Stats() (Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return Stats{}, err
	}
	var out Stats
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT chat_jid,COUNT(*),MIN(timestamp),MAX(timestamp) FROM messages WHERE timestamp>=? GROUP BY chat_jid`, time.Now().Unix()-int64(s.cfg.RetentionDays)*86400)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var jid string
		var count int
		var oldest, last int64
		if err := rows.Scan(&jid, &count, &oldest, &last); err != nil {
			return Stats{}, err
		}
		if _, ok := s.allowed[jid]; !ok {
			continue
		}
		out.Chats++
		out.Messages += count
		if out.OldestMessageAt == 0 || oldest < out.OldestMessageAt {
			out.OldestMessageAt = oldest
		}
		if last > out.LastMessageAt {
			out.LastMessageAt = last
		}
	}
	return out, rows.Err()
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.suspended = true
	s.invalidateAttachmentsLocked()
	return s.db.Close()
}
