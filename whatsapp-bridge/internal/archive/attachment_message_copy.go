package archive

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"time"
)

var copyRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

func (s *Store) initAttachmentMessageCopy() error {
	for _, column := range []struct{ name, definition string }{
		{"message_copy_attempted", "INTEGER NOT NULL DEFAULT 0"},
		{"message_copy_send_started", "INTEGER NOT NULL DEFAULT 0"},
		{"message_copy_request_id", "TEXT NOT NULL DEFAULT ''"},
		{"message_copy_request_ref", "TEXT NOT NULL DEFAULT ''"},
		{"message_copy_generation", "INTEGER NOT NULL DEFAULT 0"},
	} {
		var found int
		if err := s.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('attachments') WHERE name=?", column.name).Scan(&found); err != nil {
			return err
		}
		if found == 0 {
			if _, err := s.db.Exec("ALTER TABLE attachments ADD COLUMN " + column.name + " " + column.definition); err != nil {
				return err
			}
		}
	}
	return nil
}

// ClaimAttachmentMessageCopies selects only exact approved failed attachments.
// This durable, bounded attempt is separate from media-path retry receipts.
func (s *Store) ClaimAttachmentMessageCopies(limit int) ([]AttachmentRecovery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 32 {
		limit = 32
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	result := []AttachmentRecovery{}
	for _, chat := range s.cfg.AllowedChats {
		if len(result) == limit {
			break
		}
		rows, err := tx.Query(`SELECT a.message_id,a.ref,a.descriptor,m.sender,m.from_me FROM attachments a
			JOIN messages m ON m.chat_jid=a.chat_jid AND m.id=a.message_id
			WHERE a.chat_jid=? AND a.status='failed' AND a.message_copy_attempted=0 AND a.length<=?
			AND m.timestamp>=? ORDER BY m.timestamp DESC,a.ref LIMIT ?`, chat.JID, s.cfg.MaxAttachmentBytes, time.Now().Unix()-int64(s.cfg.RetentionDays)*86400, limit-len(result))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			candidate := AttachmentRecovery{ChatJID: chat.JID, Generation: s.generation}
			var descriptor []byte
			if err := rows.Scan(&candidate.MessageID, &candidate.Ref, &descriptor, &candidate.Sender, &candidate.FromMe); err != nil {
				rows.Close()
				return nil, err
			}
			if json.Unmarshal(descriptor, &candidate.Descriptor) != nil {
				rows.Close()
				return nil, errors.New("invalid stored attachment copy descriptor")
			}
			if ValidateAttachmentDescriptor(candidate.Descriptor, s.cfg.MaxAttachmentBytes) != nil {
				continue
			}
			result = append(result, candidate)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	for _, candidate := range result {
		if _, err := tx.Exec(`UPDATE attachments SET message_copy_attempted=1,message_copy_send_started=0,
		message_copy_request_id='',message_copy_request_ref=ref,message_copy_generation=? WHERE ref=?`, candidate.Generation, candidate.Ref); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) currentAttachmentMessageCopyLocked(candidate AttachmentRecovery) (AttachmentDescriptor, string, string, int, error) {
	var d AttachmentDescriptor
	if err := s.check(); err != nil {
		return d, "", "", 0, err
	}
	if candidate.Generation != s.generation {
		return d, "", "", 0, ErrAttachmentUnavailable
	}
	if err := s.retainedLocked(s.db, candidate.ChatJID, candidate.MessageID); err != nil {
		return d, "", "", 0, err
	}
	var ref, status, sender, requestID, requestRef string
	var fromMe bool
	var attempted, started int
	var generation uint64
	var descriptor []byte
	err := s.db.QueryRow(`SELECT a.ref,a.status,a.descriptor,a.message_copy_attempted,a.message_copy_send_started,
		a.message_copy_request_id,a.message_copy_request_ref,a.message_copy_generation,m.sender,m.from_me
		FROM attachments a JOIN messages m ON m.chat_jid=a.chat_jid AND m.id=a.message_id
		WHERE a.chat_jid=? AND a.message_id=?`, candidate.ChatJID, candidate.MessageID).Scan(&ref, &status, &descriptor, &attempted, &started, &requestID, &requestRef, &generation, &sender, &fromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return d, "", "", 0, ErrAttachmentUnavailable
	}
	if err != nil {
		return d, "", "", 0, err
	}
	claimed, err := json.Marshal(candidate.Descriptor)
	if err != nil || ref != candidate.Ref || requestRef != ref || generation != candidate.Generation || attempted != 1 || sender != candidate.Sender || fromMe != candidate.FromMe || !equalBytes(descriptor, claimed) {
		return d, "", "", 0, ErrAttachmentUnavailable
	}
	if json.Unmarshal(descriptor, &d) != nil || ValidateAttachmentDescriptor(d, s.cfg.MaxAttachmentBytes) != nil {
		return d, "", "", 0, ErrAttachmentUnavailable
	}
	return d, status, requestID, started, nil
}

// WithAttachmentMessageCopy holds authorization through a bounded protocol
// write. The callback must not call archive methods and must enforce a timeout.
func (s *Store) WithAttachmentMessageCopy(candidate AttachmentRecovery, send func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if send == nil {
		return errors.New("message copy callback is required")
	}
	_, status, requestID, started, err := s.currentAttachmentMessageCopyLocked(candidate)
	if err != nil {
		return err
	}
	if started != 0 || requestID != "" {
		return ErrAttachmentUnavailable
	}
	switch status {
	case "failed", "pending", "downloading":
	default:
		return ErrAttachmentUnavailable
	}
	if _, err := s.db.Exec("UPDATE attachments SET message_copy_send_started=1 WHERE ref=?", candidate.Ref); err != nil {
		return err
	}
	return send()
}

// RegisterAttachmentMessageCopy binds the returned protocol request identifier
// to its exact claim. Uncorrelated peer responses cannot refresh credentials.
func (s *Store) RegisterAttachmentMessageCopy(candidate AttachmentRecovery, requestID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !copyRequestIDPattern.MatchString(requestID) {
		return errors.New("invalid message copy request identifier")
	}
	_, status, current, started, err := s.currentAttachmentMessageCopyLocked(candidate)
	if err != nil {
		return err
	}
	if started != 1 || (current != "" && current != requestID) {
		return ErrAttachmentUnavailable
	}
	switch status {
	case "failed", "pending", "downloading", "ready":
	default:
		return ErrAttachmentUnavailable
	}
	_, err = s.db.Exec("UPDATE attachments SET message_copy_request_id=? WHERE ref=?", requestID, candidate.Ref)
	return err
}

// ReplaceAttachmentMessageCopy accepts only an authenticated owner-device copy
// correlated by the collector. Plaintext identity, media type, and size remain
// immutable; only media transport credentials may change.
func (s *Store) ReplaceAttachmentMessageCopy(candidate AttachmentRecovery, requestID string, replacement AttachmentDescriptor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, status, expected, started, err := s.currentAttachmentMessageCopyLocked(candidate)
	if err != nil {
		return err
	}
	if started != 1 || expected == "" || requestID != expected {
		return ErrAttachmentUnavailable
	}
	if status == "ready" {
		return nil
	}
	switch status {
	case "failed", "pending", "downloading":
	default:
		return ErrAttachmentUnavailable
	}
	if ValidateAttachmentDescriptor(replacement, s.cfg.MaxAttachmentBytes) != nil || replacement.MediaType != current.MediaType || replacement.FileLength != current.FileLength || !equalBytes(replacement.FileSHA256, current.FileSHA256) {
		return errors.New("message copy does not match the retained attachment")
	}
	current.DirectPath = replacement.DirectPath
	current.MediaKey = append([]byte(nil), replacement.MediaKey...)
	current.FileEncSHA256 = append([]byte(nil), replacement.FileEncSHA256...)
	updated, err := json.Marshal(current)
	if err != nil {
		return err
	}
	ref, err := randomRef()
	if err != nil {
		return err
	}
	if flight := s.flights[candidate.Ref]; flight != nil {
		flight.cancel()
		flight.file.Close()
		delete(s.flights, candidate.Ref)
		if err := os.Remove(flight.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	_, err = s.db.Exec(`UPDATE attachments SET ref=?,descriptor=?,status='pending',cache_name='',attempts=0,retry_after=0,last_access=? WHERE ref=?`, ref, updated, time.Now().UnixNano(), candidate.Ref)
	return err
}
