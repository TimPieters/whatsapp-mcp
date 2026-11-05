package archive

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// RestoreAttachment restores one existing attachment from a local byte stream.
// Its retained approved descriptor remains authoritative for size and SHA-256.
// This administrative operation never claims unrelated pending downloads.
func (s *Store) RestoreAttachment(ctx context.Context, jid, id string, source io.Reader) error {
	if source == nil {
		return errors.New("an attachment source is required")
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := s.check(); err != nil {
		s.mu.Unlock()
		return err
	}
	if err := s.retainedLocked(s.db, jid, id); err != nil {
		s.mu.Unlock()
		return err
	}
	var ref, status, name string
	var raw []byte
	err := s.db.QueryRow("SELECT ref,status,cache_name,descriptor FROM attachments WHERE chat_jid=? AND message_id=?", jid, id).Scan(&ref, &status, &name, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		s.mu.Unlock()
		return ErrAttachmentUnavailable
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	var descriptor AttachmentDescriptor
	if json.Unmarshal(raw, &descriptor) != nil || ValidateAttachmentDescriptor(descriptor, s.cfg.MaxAttachmentBytes) != nil {
		s.mu.Unlock()
		return ErrAttachmentUnavailable
	}
	if status == "ready" {
		valid, err := s.validCachedAttachmentLocked(ctx, name, descriptor)
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if valid {
			s.mu.Unlock()
			return nil
		}
		// A broken ready entry has no bytes to preserve. Requeue this exact ref
		// and remove the invalid cached copy before attempting the local restore.
		if cacheNamePattern.MatchString(name) {
			if err := os.Remove(filepath.Join(s.cacheDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.mu.Unlock()
				return err
			}
		}
		status = "pending"
	}
	if status != "failed" && status != "pending" {
		s.mu.Unlock()
		return ErrAttachmentUnavailable
	}
	if flight := s.flights[ref]; flight != nil {
		flight.cancel()
		flight.file.Close()
		delete(s.flights, ref)
		if err := os.Remove(flight.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.mu.Unlock()
			return err
		}
	}
	if _, err := s.db.Exec("UPDATE attachments SET status='downloading',cache_name='',attempts=attempts+1 WHERE ref=?", ref); err != nil {
		s.mu.Unlock()
		return err
	}
	job := AttachmentJob{ChatJID: jid, MessageID: id, Ref: ref, Descriptor: descriptor, Generation: s.generation}
	s.mu.Unlock()
	return s.CacheAttachment(ctx, job, func(downloadCtx context.Context, file AttachmentFile) error {
		_, err := io.CopyBuffer(file, contextAttachmentReader{ctx: downloadCtx, reader: source}, make([]byte, 32*1024))
		return err
	})
}

func (s *Store) validCachedAttachmentLocked(ctx context.Context, name string, d AttachmentDescriptor) (bool, error) {
	if !cacheNamePattern.MatchString(name) {
		return false, nil
	}
	full := filepath.Join(s.cacheDir, name)
	info, err := os.Lstat(full)
	if err != nil || !info.Mode().IsRegular() || info.Size() != d.FileLength {
		return false, nil
	}
	file, err := os.Open(full)
	if err != nil {
		return false, nil
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, contextAttachmentReader{ctx: ctx, reader: io.LimitReader(file, d.FileLength+1)}, make([]byte, 32*1024)); err != nil {
		return false, err
	}
	return equalBytes(hash.Sum(nil), d.FileSHA256), nil
}

type contextAttachmentReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextAttachmentReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
