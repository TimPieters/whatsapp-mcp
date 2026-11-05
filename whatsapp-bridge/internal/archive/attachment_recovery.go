package archive

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"time"
)

// AttachmentRecovery is private protocol state for one explicitly claimed
// owner-device retry. It must never be exposed by public tools or logs.
type AttachmentRecovery struct {
	ChatJID, MessageID, Ref, Sender string
	FromMe                          bool
	Descriptor                      AttachmentDescriptor
	Generation                      uint64
}

func (s *Store) initAttachmentRecovery() error {
	var found int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('attachments') WHERE name='phone_retry_attempted'").Scan(&found); err != nil {
		return err
	}
	if found == 0 {
		_, err := s.db.Exec("ALTER TABLE attachments ADD COLUMN phone_retry_attempted INTEGER NOT NULL DEFAULT 0")
		if err != nil {
			return err
		}
	}
	return s.initAttachmentMessageCopy()
}

// ClaimAttachmentRecoveries authorizes at most one owner-device retry per
// stored file. A claim survives restarts even if the request cannot be sent.
// Regular pending, ready, oversized, and evicted entries are never selected.
func (s *Store) ClaimAttachmentRecoveries(limit int) ([]AttachmentRecovery, error) {
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
		rows, err := tx.Query(`SELECT a.message_id,a.ref,a.descriptor,m.sender,m.from_me
			FROM attachments a JOIN messages m ON m.chat_jid=a.chat_jid AND m.id=a.message_id
			WHERE a.chat_jid=? AND a.status='failed' AND a.phone_retry_attempted=0
			AND a.length<=? AND m.timestamp>=? ORDER BY m.timestamp DESC,a.ref LIMIT ?`, chat.JID, s.cfg.MaxAttachmentBytes, time.Now().Unix()-int64(s.cfg.RetentionDays)*86400, limit-len(result))
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
			if err := json.Unmarshal(descriptor, &candidate.Descriptor); err != nil {
				rows.Close()
				return nil, errors.New("invalid stored recovery descriptor")
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
		if _, err := tx.Exec("UPDATE attachments SET phone_retry_attempted=1 WHERE ref=?", candidate.Ref); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) currentRecoveryLocked(candidate AttachmentRecovery) (AttachmentDescriptor, string, error) {
	var current AttachmentDescriptor
	if err := s.check(); err != nil {
		return current, "", err
	}
	if candidate.Generation != s.generation {
		return current, "", ErrAttachmentUnavailable
	}
	if err := s.retainedLocked(s.db, candidate.ChatJID, candidate.MessageID); err != nil {
		return current, "", err
	}
	var ref, status, sender string
	var fromMe bool
	var attempted int
	var descriptor []byte
	err := s.db.QueryRow(`SELECT a.ref,a.status,a.descriptor,a.phone_retry_attempted,m.sender,m.from_me
		FROM attachments a JOIN messages m ON m.chat_jid=a.chat_jid AND m.id=a.message_id
		WHERE a.chat_jid=? AND a.message_id=?`, candidate.ChatJID, candidate.MessageID).Scan(&ref, &status, &descriptor, &attempted, &sender, &fromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return current, "", ErrAttachmentUnavailable
	}
	if err != nil {
		return current, "", err
	}
	claimed, err := json.Marshal(candidate.Descriptor)
	if err != nil {
		return current, "", ErrAttachmentUnavailable
	}
	if ref != candidate.Ref || attempted != 1 || sender != candidate.Sender || fromMe != candidate.FromMe || !equalBytes(descriptor, claimed) {
		return current, "", ErrAttachmentUnavailable
	}
	if err := json.Unmarshal(descriptor, &current); err != nil {
		return current, "", ErrAttachmentUnavailable
	}
	if ValidateAttachmentDescriptor(current, s.cfg.MaxAttachmentBytes) != nil {
		return current, "", ErrAttachmentUnavailable
	}
	return current, status, nil
}

// WithAttachmentRecovery serializes the actual owner-device protocol write
// with policy changes. The callback must enforce a short network timeout and
// must not call archive methods while this authorization lock is held.
func (s *Store) WithAttachmentRecovery(candidate AttachmentRecovery, send func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if send == nil {
		return errors.New("recovery callback is required")
	}
	_, status, err := s.currentRecoveryLocked(candidate)
	if err != nil {
		return err
	}
	switch status {
	case "failed", "pending", "downloading":
	default:
		return ErrAttachmentUnavailable
	}
	return send()
}

// ReplaceRecoveredAttachment changes only the authenticated download path.
// A concurrent normal download may have requeued this exact claimed reference;
// replacing it cancels that older flight. Ready bytes are preserved unchanged.
func (s *Store) ReplaceRecoveredAttachment(candidate AttachmentRecovery, newDirectPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, status, err := s.currentRecoveryLocked(candidate)
	if err != nil {
		return err
	}
	if status == "ready" {
		return nil
	}
	switch status {
	case "failed", "pending", "downloading":
	default:
		return ErrAttachmentUnavailable
	}
	current.DirectPath = newDirectPath
	if err := ValidateAttachmentDescriptor(current, s.cfg.MaxAttachmentBytes); err != nil {
		return errors.New("invalid recovered attachment path")
	}
	updated, err := json.Marshal(current)
	if err != nil {
		return err
	}
	newRef, err := randomRef()
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
	_, err = s.db.Exec(`UPDATE attachments SET ref=?,descriptor=?,status='pending',cache_name='',attempts=0,retry_after=0,last_access=?
		WHERE ref=?`, newRef, updated, time.Now().UnixNano(), candidate.Ref)
	return err
}
