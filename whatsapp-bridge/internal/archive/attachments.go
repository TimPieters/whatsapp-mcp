package archive

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrAttachmentUnavailable = errors.New("attachment is not available")

const MaxSupportedAttachmentBytes int64 = 128 << 20

// AttachmentDescriptor contains private download credentials. Never include it
// in a message response, log, or public resource description.
type AttachmentDescriptor struct {
	DirectPath    string `json:"direct_path"`
	MediaKey      []byte `json:"media_key"`
	FileSHA256    []byte `json:"file_sha256"`
	FileEncSHA256 []byte `json:"file_enc_sha256"`
	FileLength    int64  `json:"file_length"`
	MediaType     string `json:"media_type"`
	MIMEType      string `json:"mime_type"`
	Filename      string `json:"filename"`
}

type AttachmentInfo struct {
	Status    string `json:"status"`
	MediaType string `json:"media_type"`
	MIMEType  string `json:"mime_type"`
	Filename  string `json:"filename"`
	Size      int64  `json:"size"`
}

type AttachmentRecord struct {
	ChatJID, MessageID string
	Descriptor         AttachmentDescriptor
}
type AttachmentJob struct {
	ChatJID, MessageID, Ref string
	Generation              uint64
	Descriptor              AttachmentDescriptor
}

// AttachmentFile supports streaming encrypted download and in-place decryption.
// The implementation bounds every write, seek, and truncate operation.
type AttachmentFile interface {
	io.Reader
	io.Writer
	io.Seeker
	io.ReaderAt
	io.WriterAt
	Truncate(int64) error
	Stat() (os.FileInfo, error)
}

type attachmentFlight struct {
	cancel        context.CancelFunc
	file          *os.File
	path, jid, id string
	reserved      int64
	generation    uint64
}
type sqlRunner interface {
	Exec(string, ...any) (sql.Result, error)
	QueryRow(string, ...any) *sql.Row
}

var cacheNamePattern = regexp.MustCompile(`^[a-f0-9]{32}\.bin$`)

func ValidateAttachmentDescriptor(d AttachmentDescriptor, maxBytes int64) error {
	if d.FileLength < 1 || d.FileLength > maxBytes || len(d.MediaKey) != 32 || len(d.FileSHA256) != 32 || len(d.FileEncSHA256) != 32 {
		return errors.New("invalid attachment size or integrity fields")
	}
	switch d.MediaType {
	case "image", "audio", "video", "document", "sticker":
	default:
		return errors.New("unsupported attachment type")
	}
	if len(d.DirectPath) > 4096 || !strings.HasPrefix(d.DirectPath, "/") || strings.HasPrefix(d.DirectPath, "//") || strings.Contains(d.DirectPath, "\\") {
		return errors.New("invalid attachment download path")
	}
	u, err := url.Parse(d.DirectPath)
	if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || path.Clean(u.Path) != u.Path {
		return errors.New("invalid attachment download path")
	}
	for _, r := range d.DirectPath {
		if unicode.IsControl(r) {
			return errors.New("invalid attachment download path")
		}
	}
	if len(d.Filename) > 255 || !utf8.ValidString(d.Filename) || strings.ContainsAny(d.Filename, "/\\:") || d.Filename == "." || d.Filename == ".." {
		return errors.New("invalid attachment filename")
	}
	for _, r := range d.Filename {
		if unicode.IsControl(r) {
			return errors.New("invalid attachment filename")
		}
	}
	if len(d.MIMEType) > 255 {
		return errors.New("invalid attachment MIME type")
	}
	for _, r := range d.MIMEType {
		if unicode.IsControl(r) {
			return errors.New("invalid attachment MIME type")
		}
	}
	if d.MIMEType != "" {
		if _, _, err := mime.ParseMediaType(d.MIMEType); err != nil {
			return errors.New("invalid attachment MIME type")
		}
	}
	return nil
}

func (s *Store) initAttachments() error {
	if err := os.MkdirAll(s.cacheDir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(s.cacheDir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("attachment cache must be a private directory")
	}
	if err := os.Chmod(s.cacheDir, 0700); err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE TABLE IF NOT EXISTS attachments(
		chat_jid TEXT NOT NULL,message_id TEXT NOT NULL,ref TEXT NOT NULL UNIQUE,
		descriptor BLOB NOT NULL,kind TEXT NOT NULL,mime TEXT NOT NULL,filename TEXT NOT NULL,
		length INTEGER NOT NULL,status TEXT NOT NULL,cache_name TEXT NOT NULL DEFAULT '',
		last_access INTEGER NOT NULL DEFAULT 0,retry_after INTEGER NOT NULL DEFAULT 0,attempts INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(chat_jid,message_id),FOREIGN KEY(chat_jid,message_id) REFERENCES messages(chat_jid,id) ON DELETE CASCADE)`)
	if err != nil {
		return err
	}
	return s.initAttachmentRecovery()
}

func (s *Store) invalidateAttachmentsLocked() {
	s.generation++
	for ref, flight := range s.flights {
		flight.cancel()
		flight.file.Close()
		flight.reserved = 0
		delete(s.flights, ref)
	}
}

// ValidateAttachment checks the supported descriptor format. Descriptors above
// the configured download limit remain private metadata with oversized status.
func (s *Store) ValidateAttachment(d AttachmentDescriptor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	return ValidateAttachmentDescriptor(d, MaxSupportedAttachmentBytes)
}

func (s *Store) retainedLocked(q sqlRunner, jid, id string) error {
	if _, ok := s.allowed[jid]; !ok {
		return ErrNotAllowed
	}
	var stamp int64
	if err := q.QueryRow("SELECT timestamp FROM messages WHERE chat_jid=? AND id=?", jid, id).Scan(&stamp); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrAttachmentUnavailable
		}
		return err
	}
	if stamp < time.Now().Unix()-int64(s.cfg.RetentionDays)*86400 {
		return ErrAttachmentUnavailable
	}
	return nil
}

func (s *Store) registerAttachmentLocked(q sqlRunner, jid, id string, d AttachmentDescriptor) error {
	if err := s.retainedLocked(q, jid, id); err != nil {
		return err
	}
	if err := ValidateAttachmentDescriptor(d, MaxSupportedAttachmentBytes); err != nil {
		return err
	}
	var kind string
	if err := q.QueryRow("SELECT media_type FROM messages WHERE chat_jid=? AND id=?", jid, id).Scan(&kind); err != nil {
		return err
	}
	if kind != d.MediaType {
		return ErrAttachmentUnavailable
	}
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	var previous []byte
	err = q.QueryRow("SELECT descriptor FROM attachments WHERE chat_jid=? AND message_id=?", jid, id).Scan(&previous)
	if err == nil && string(previous) == string(data) {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	preservePhoneAttempt := false
	preserveMessageCopy := false
	if len(previous) > 0 {
		var old AttachmentDescriptor
		if json.Unmarshal(previous, &old) == nil {
			preservePhoneAttempt = old.FileLength == d.FileLength && old.MediaType == d.MediaType && equalBytes(old.MediaKey, d.MediaKey) && equalBytes(old.FileSHA256, d.FileSHA256) && equalBytes(old.FileEncSHA256, d.FileEncSHA256)
			preserveMessageCopy = old.FileLength == d.FileLength && old.MediaType == d.MediaType && equalBytes(old.FileSHA256, d.FileSHA256)
		}
	}
	ref, err := randomRef()
	if err != nil {
		return err
	}
	status := "pending"
	if d.FileLength > s.cfg.MaxAttachmentBytes {
		status = "oversized"
	}
	_, err = q.Exec(`INSERT INTO attachments(chat_jid,message_id,ref,descriptor,kind,mime,filename,length,status) VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(chat_jid,message_id) DO UPDATE SET ref=excluded.ref,descriptor=excluded.descriptor,kind=excluded.kind,mime=excluded.mime,filename=excluded.filename,length=excluded.length,status=excluded.status,cache_name='',last_access=0,retry_after=0,attempts=0,
		phone_retry_attempted=CASE WHEN ? THEN phone_retry_attempted ELSE 0 END,
		message_copy_attempted=CASE WHEN ? THEN message_copy_attempted ELSE 0 END,
		message_copy_send_started=CASE WHEN ? THEN message_copy_send_started ELSE 0 END,
		message_copy_request_id=CASE WHEN ? THEN message_copy_request_id ELSE '' END,
		message_copy_request_ref=CASE WHEN ? THEN message_copy_request_ref ELSE '' END,
		message_copy_generation=CASE WHEN ? THEN message_copy_generation ELSE 0 END`, jid, id, ref, data, d.MediaType, d.MIMEType, d.Filename, d.FileLength, status, preservePhoneAttempt, preserveMessageCopy, preserveMessageCopy, preserveMessageCopy, preserveMessageCopy, preserveMessageCopy)
	return err
}

func (s *Store) RegisterAttachment(jid, id string, d AttachmentDescriptor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if err := s.registerAttachmentLocked(s.db, jid, id, d); err != nil {
		return err
	}
	return s.cleanOrphanCacheLocked()
}

func (s *Store) attachmentInfoLocked(jid, id string) (AttachmentInfo, error) {
	var info AttachmentInfo
	if err := s.retainedLocked(s.db, jid, id); err != nil {
		return info, err
	}
	err := s.db.QueryRow("SELECT status,kind,mime,filename,length FROM attachments WHERE chat_jid=? AND message_id=?", jid, id).Scan(&info.Status, &info.MediaType, &info.MIMEType, &info.Filename, &info.Size)
	if errors.Is(err, sql.ErrNoRows) {
		return info, ErrAttachmentUnavailable
	}
	return info, err
}

func (s *Store) GetAttachmentInfo(jid, id string) (AttachmentInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return AttachmentInfo{}, err
	}
	return s.attachmentInfoLocked(jid, id)
}

func (s *Store) RequestAttachment(jid, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return err
	}
	if err := s.retainedLocked(s.db, jid, id); err != nil {
		return err
	}
	result, err := s.db.Exec(`UPDATE attachments SET last_access=?,status='pending',attempts=CASE WHEN status IN ('evicted','failed') THEN 0 ELSE attempts END WHERE chat_jid=? AND message_id=? AND status IN ('pending','evicted','failed') AND retry_after<=?`, time.Now().UnixNano(), jid, id, time.Now().Unix())
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		if _, err := s.attachmentInfoLocked(jid, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) PendingAttachments(limit int) ([]AttachmentJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 32 {
		limit = 1
	}
	rows, err := s.db.Query(`SELECT a.chat_jid,a.message_id,a.ref,a.descriptor FROM attachments a JOIN messages m ON m.chat_jid=a.chat_jid AND m.id=a.message_id
		WHERE a.status IN ('pending','failed') AND a.attempts<3 AND a.retry_after<=? AND m.timestamp>=? ORDER BY a.last_access DESC,m.timestamp DESC,a.ref LIMIT ?`, time.Now().Unix(), time.Now().Unix()-int64(s.cfg.RetentionDays)*86400, limit)
	if err != nil {
		return nil, err
	}
	jobs := []AttachmentJob{}
	for rows.Next() {
		var j AttachmentJob
		var data []byte
		if err := rows.Scan(&j.ChatJID, &j.MessageID, &j.Ref, &data); err != nil {
			rows.Close()
			return nil, err
		}
		if _, ok := s.allowed[j.ChatJID]; !ok {
			continue
		}
		if err := json.Unmarshal(data, &j.Descriptor); err != nil {
			rows.Close()
			return nil, errors.New("invalid stored attachment descriptor")
		}
		if err := ValidateAttachmentDescriptor(j.Descriptor, s.cfg.MaxAttachmentBytes); err != nil {
			continue
		}
		j.Generation = s.generation
		jobs = append(jobs, j)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, j := range jobs {
		if _, err := s.db.Exec("UPDATE attachments SET status='downloading',attempts=attempts+1 WHERE ref=?", j.Ref); err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (s *Store) verifyJobLocked(j AttachmentJob) error {
	if err := s.check(); err != nil {
		return err
	}
	if j.Generation != s.generation {
		return ErrAttachmentUnavailable
	}
	if err := s.retainedLocked(s.db, j.ChatJID, j.MessageID); err != nil {
		return err
	}
	var ref, status string
	var descriptor []byte
	if err := s.db.QueryRow("SELECT ref,status,descriptor FROM attachments WHERE chat_jid=? AND message_id=?", j.ChatJID, j.MessageID).Scan(&ref, &status, &descriptor); err != nil {
		return ErrAttachmentUnavailable
	}
	if ref != j.Ref || status != "downloading" {
		return ErrAttachmentUnavailable
	}
	data, err := json.Marshal(j.Descriptor)
	if err != nil || string(data) != string(descriptor) {
		return ErrAttachmentUnavailable
	}
	return nil
}

func (s *Store) CacheAttachment(ctx context.Context, j AttachmentJob, download func(context.Context, AttachmentFile) error) (resultErr error) {
	s.mu.Lock()
	if err := s.verifyJobLocked(j); err != nil {
		s.mu.Unlock()
		return err
	}
	if _, exists := s.flights[j.Ref]; exists {
		s.mu.Unlock()
		return ErrAttachmentUnavailable
	}
	failStart := func(err error) error { s.failAttachmentJobLocked(j); s.mu.Unlock(); return err }
	if download == nil {
		return failStart(errors.New("download callback is required"))
	}
	if err := ctx.Err(); err != nil {
		return failStart(err)
	}
	if err := ValidateAttachmentDescriptor(j.Descriptor, s.cfg.MaxAttachmentBytes); err != nil {
		return failStart(err)
	}
	if err := s.evictCacheLocked(j.Descriptor.FileLength + 64); err != nil {
		return failStart(err)
	}
	file, err := os.CreateTemp(s.cacheDir, ".download-*")
	if err != nil {
		return failStart(err)
	}
	jobCtx, cancel := context.WithCancel(ctx)
	flight := &attachmentFlight{cancel: cancel, file: file, path: file.Name(), jid: j.ChatJID, id: j.MessageID, reserved: j.Descriptor.FileLength + 64, generation: j.Generation}
	s.flights[j.Ref] = flight
	bounded := &boundedAttachmentFile{file: file, store: s, job: j, limit: j.Descriptor.FileLength + 64, ctx: jobCtx}
	s.mu.Unlock()
	err = download(jobCtx, bounded)
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		cancel()
		if s.flights[j.Ref] == flight {
			delete(s.flights, j.Ref)
		}
		file.Close()
		os.Remove(file.Name())
		if resultErr != nil {
			s.failAttachmentJobLocked(j)
		}
	}()
	if err == nil {
		err = s.verifyJobLocked(j)
	}
	if err == nil {
		err = jobCtx.Err()
	}
	if err == nil {
		info, statErr := file.Stat()
		if statErr != nil {
			err = statErr
		} else if info.Size() != j.Descriptor.FileLength || info.Size() > s.cfg.MaxAttachmentBytes {
			err = errors.New("attachment size does not match descriptor")
		}
	}
	if err == nil {
		if _, seekErr := file.Seek(0, io.SeekStart); seekErr != nil {
			err = seekErr
		} else {
			hash := sha256.New()
			if _, copyErr := io.Copy(hash, file); copyErr != nil {
				err = copyErr
			} else if !equalBytes(hash.Sum(nil), j.Descriptor.FileSHA256) {
				err = errors.New("attachment integrity check failed")
			}
		}
	}
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	name := j.Ref + ".bin"
	if err := os.Rename(file.Name(), filepath.Join(s.cacheDir, name)); err != nil {
		return err
	}
	if _, err := s.db.Exec("UPDATE attachments SET status='ready',cache_name=?,last_access=?,retry_after=0 WHERE ref=?", name, time.Now().UnixNano(), j.Ref); err != nil {
		os.Remove(filepath.Join(s.cacheDir, name))
		return err
	}
	return nil
}

func (s *Store) failAttachmentJobLocked(j AttachmentJob) {
	if s.verifyJobLocked(j) == nil {
		_, _ = s.db.Exec("UPDATE attachments SET status='failed',retry_after=? WHERE ref=?", time.Now().Unix()+60, j.Ref)
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}

func (s *Store) WithAttachment(jid, id string, read func(AttachmentInfo, io.ReadSeeker) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if read == nil {
		return errors.New("read callback is required")
	}
	if err := s.check(); err != nil {
		return err
	}
	info, err := s.attachmentInfoLocked(jid, id)
	if err != nil {
		return err
	}
	if info.Status != "ready" {
		return ErrAttachmentUnavailable
	}
	var name string
	if err := s.db.QueryRow("SELECT cache_name FROM attachments WHERE chat_jid=? AND message_id=?", jid, id).Scan(&name); err != nil {
		return err
	}
	if !cacheNamePattern.MatchString(name) {
		s.markAttachmentEvictedLocked(jid, id)
		return ErrAttachmentUnavailable
	}
	full := filepath.Join(s.cacheDir, name)
	stat, err := os.Lstat(full)
	if err != nil || !stat.Mode().IsRegular() || stat.Size() != info.Size {
		s.markAttachmentEvictedLocked(jid, id)
		return ErrAttachmentUnavailable
	}
	file, err := os.Open(full)
	if err != nil {
		s.markAttachmentEvictedLocked(jid, id)
		return ErrAttachmentUnavailable
	}
	defer file.Close()
	if _, err := s.db.Exec("UPDATE attachments SET last_access=? WHERE chat_jid=? AND message_id=?", time.Now().UnixNano(), jid, id); err != nil {
		return err
	}
	return read(info, file)
}

func (s *Store) markAttachmentEvictedLocked(jid, id string) {
	_, _ = s.db.Exec("UPDATE attachments SET status='evicted',cache_name='',retry_after=0 WHERE chat_jid=? AND message_id=?", jid, id)
}

func (s *Store) evictCacheLocked(reserve int64) error {
	for {
		var used int64
		if err := s.db.QueryRow("SELECT COALESCE(SUM(length),0) FROM attachments WHERE status='ready'").Scan(&used); err != nil {
			return err
		}
		for _, f := range s.flights {
			used += f.reserved
		}
		if used+reserve <= s.cfg.AttachmentCacheBytes {
			return nil
		}
		var ref, name string
		if err := s.db.QueryRow("SELECT ref,cache_name FROM attachments WHERE status='ready' ORDER BY last_access,ref LIMIT 1").Scan(&ref, &name); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errors.New("attachment cache budget is exhausted")
			}
			return err
		}
		if !cacheNamePattern.MatchString(name) {
			return errors.New("invalid cache entry")
		}
		if err := os.Remove(filepath.Join(s.cacheDir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := s.db.Exec("UPDATE attachments SET status='evicted',cache_name='' WHERE ref=?", ref); err != nil {
			return err
		}
	}
}

func (s *Store) reconcileAttachmentsLocked() error {
	if _, err := s.db.Exec("UPDATE attachments SET status='oversized',cache_name='' WHERE length>?", s.cfg.MaxAttachmentBytes); err != nil {
		return err
	}
	if _, err := s.db.Exec("UPDATE attachments SET status='pending',attempts=0,retry_after=0 WHERE status='oversized' AND length<=?", s.cfg.MaxAttachmentBytes); err != nil {
		return err
	}
	rows, err := s.db.Query("SELECT ref,status,cache_name,length FROM attachments")
	if err != nil {
		return err
	}
	type entry struct {
		ref, status, name string
		size              int64
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ref, &e.status, &e.name, &e.size); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.status == "downloading" {
			if f := s.flights[e.ref]; f == nil || f.generation != s.generation {
				if _, err := s.db.Exec("UPDATE attachments SET status='pending',retry_after=0,attempts=MAX(attempts-1,0) WHERE ref=?", e.ref); err != nil {
					return err
				}
			}
		}
		if e.status == "ready" {
			var info os.FileInfo
			var err error
			if cacheNamePattern.MatchString(e.name) {
				info, err = os.Lstat(filepath.Join(s.cacheDir, e.name))
			} else {
				err = ErrAttachmentUnavailable
			}
			if err != nil || !info.Mode().IsRegular() || info.Size() != e.size {
				if _, err := s.db.Exec("UPDATE attachments SET status='evicted',cache_name='' WHERE ref=?", e.ref); err != nil {
					return err
				}
			}
		}
	}
	if err := s.cleanOrphanCacheLocked(); err != nil {
		return err
	}
	return s.evictCacheLocked(0)
}

func (s *Store) cleanOrphanCacheLocked() error {
	rows, err := s.db.Query("SELECT cache_name FROM attachments WHERE status='ready'")
	if err != nil {
		return err
	}
	keep := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return err
		}
		if cacheNamePattern.MatchString(name) {
			keep[name] = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for ref, f := range s.flights {
		var currentRef string
		_ = s.db.QueryRow("SELECT ref FROM attachments WHERE chat_jid=? AND message_id=?", f.jid, f.id).Scan(&currentRef)
		if currentRef == ref && f.generation == s.generation && s.retainedLocked(s.db, f.jid, f.id) == nil {
			keep[filepath.Base(f.path)] = true
		} else {
			f.cancel()
			f.file.Close()
			f.reserved = 0
			delete(s.flights, ref)
		}
	}
	entries, err := os.ReadDir(s.cacheDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if keep[entry.Name()] {
			continue
		}
		if entry.IsDir() {
			return errors.New("unexpected directory in attachment cache")
		}
		if err := os.Remove(filepath.Join(s.cacheDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func randomRef() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

type boundedAttachmentFile struct {
	file  *os.File
	store *Store
	job   AttachmentJob
	limit int64
	ctx   context.Context
}

func (f *boundedAttachmentFile) check() error {
	if err := f.ctx.Err(); err != nil {
		return err
	}
	return f.store.verifyJobLocked(f.job)
}
func (f *boundedAttachmentFile) Read(p []byte) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}
func (f *boundedAttachmentFile) ReadAt(p []byte, off int64) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}
func (f *boundedAttachmentFile) Write(p []byte) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	off, err := f.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if off < 0 || int64(len(p)) > f.limit-off {
		return 0, errors.New("attachment exceeds configured size limit")
	}
	return f.file.Write(p)
}
func (f *boundedAttachmentFile) WriteAt(p []byte, off int64) (int, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if off < 0 || int64(len(p)) > f.limit-off {
		return 0, errors.New("attachment exceeds configured size limit")
	}
	return f.file.WriteAt(p, off)
}
func (f *boundedAttachmentFile) Seek(off int64, whence int) (int64, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	var base int64
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		var err error
		base, err = f.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
	case io.SeekEnd:
		info, err := f.file.Stat()
		if err != nil {
			return 0, err
		}
		base = info.Size()
	default:
		return 0, errors.New("invalid seek")
	}
	if off < -base || off > f.limit-base {
		return 0, errors.New("attachment seek exceeds bounds")
	}
	return f.file.Seek(off, whence)
}
func (f *boundedAttachmentFile) Truncate(size int64) error {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if size < 0 || size > f.limit {
		return errors.New("attachment truncate exceeds bounds")
	}
	return f.file.Truncate(size)
}
func (f *boundedAttachmentFile) Stat() (os.FileInfo, error) {
	f.store.mu.Lock()
	defer f.store.mu.Unlock()
	if err := f.check(); err != nil {
		return nil, err
	}
	return f.file.Stat()
}
