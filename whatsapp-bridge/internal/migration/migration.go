// Package migration moves explicitly approved history through a bounded JSONL
// stream. It never copies provider contacts or session credentials.
package migration

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	_ "modernc.org/sqlite"
	"whatsapp-client/internal/archive"
	"whatsapp-client/internal/config"
)

const MaxLineBytes = 128 * 1024

type Report struct {
	Exported int `json:"exported"`
	// AttachmentRefs counts complete private download descriptors, not binaries.
	AttachmentRefs        int `json:"attachment_refs"`
	AttachmentRefsSkipped int `json:"attachment_refs_skipped"`
	// Imported counts valid approved records submitted successfully. Duplicate
	// replacement, retention, and storage caps can reduce the retained row count.
	Imported int `json:"imported"`
	Skipped  int `json:"skipped"`
}

// ExportUpstream reads a stopped database or consistent copy of the original
// message store. The immutable read-only connection never creates sidecars;
// callers must not use a source that is being modified concurrently.
func ExportUpstream(ctx context.Context, sourceDB string, cfg config.Config, out io.Writer) (Report, error) {
	var report Report
	if out == nil {
		return report, errors.New("an export writer is required")
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return report, err
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	abs, err := filepath.Abs(sourceDB)
	if err != nil {
		return report, errors.New("invalid source database path")
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	if filepath.VolumeName(abs) != "" && !strings.HasPrefix(u.Path, "/") {
		u.Path = "/" + u.Path
	}
	u.RawQuery = "mode=ro&immutable=1&_pragma=query_only(1)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return report, errors.New("cannot open source database read-only")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		return report, errors.New("cannot open source database read-only")
	}
	now := time.Now().Unix()
	cutoff := now - int64(cfg.RetentionDays)*86400
	mediaColumns, err := sourceMediaColumns(ctx, db)
	if err != nil {
		return report, err
	}
	for _, chat := range cfg.AllowedChats {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		// Only approved chat identifiers are bound. CASE keeps malformed large
		// fields out of the Go heap while still counting those rows as skipped.
		rows, err := db.QueryContext(ctx, `SELECT
			CASE WHEN length(CAST(id AS BLOB))<=256 THEN id END,
			CASE WHEN length(CAST(COALESCE(sender,'') AS BLOB))<=128 THEN COALESCE(sender,'') END,
			CASE WHEN length(CAST(content AS BLOB))<=65536 THEN content END,
			CASE WHEN length(CAST(timestamp AS BLOB))<=128 THEN CAST(timestamp AS TEXT) END,
			CAST(COALESCE(is_from_me,0) AS INTEGER),
			CASE WHEN length(CAST(COALESCE(media_type,'') AS BLOB))<=64 THEN COALESCE(media_type,'') END, `+mediaColumns+`
			FROM messages WHERE chat_jid=? ORDER BY timestamp DESC,id DESC LIMIT ?`, chat.JID, cfg.MaxMessagesPerChat)
		if err != nil {
			return report, errors.New("cannot read the source message schema")
		}
		for rows.Next() {
			if err := ctx.Err(); err != nil {
				rows.Close()
				return report, err
			}
			var id, sender, text, stamp, media sql.NullString
			var mediaRow sourceMedia
			var fromMe int
			if err := rows.Scan(&id, &sender, &text, &stamp, &fromMe, &media, &mediaRow.directPath, &mediaRow.url, &mediaRow.filename, &mediaRow.mime, &mediaRow.key, &mediaRow.hash, &mediaRow.encHash, &mediaRow.length); err != nil {
				rows.Close()
				return report, errors.New("cannot decode a source message row")
			}
			if !id.Valid || !sender.Valid || !text.Valid || !stamp.Valid || !media.Valid {
				report.Skipped++
				continue
			}
			ts, ok := parseTimestamp(stamp.String)
			m := archive.Message{ID: id.String, ChatJID: chat.JID, Sender: sender.String, Text: text.String, MediaType: media.String, Timestamp: ts, FromMe: fromMe != 0}
			if !ok || ts < cutoff || ts > now+300 || !validMessage(m) {
				report.Skipped++
				continue
			}
			record := exportRecord{Message: m}
			if m.MediaType != "" {
				if ref, ok := mediaRow.reference(m.MediaType, archive.MaxSupportedAttachmentBytes); ok {
					record.Reference = ref
				} else {
					report.AttachmentRefsSkipped++
				}
			}
			data, err := json.Marshal(record)
			if err != nil {
				rows.Close()
				return report, errors.New("cannot encode an export record")
			}
			if len(data) > MaxLineBytes {
				report.Skipped++
				continue
			}
			data = append(data, '\n')
			if n, err := out.Write(data); err != nil {
				rows.Close()
				return report, errors.New("cannot write export record")
			} else if n != len(data) {
				rows.Close()
				return report, io.ErrShortWrite
			}
			report.Exported++
			if record.Reference != nil {
				report.AttachmentRefs++
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			if ctx.Err() != nil {
				return report, ctx.Err()
			}
			return report, errors.New("source message query failed")
		}
	}
	return report, nil
}

// ImportJSONL accepts one strict Message object per line. Valid earlier lines
// are committed before a malformed-line error is returned. Policy is checked
// again by the archive when each batch is written.
func ImportJSONL(ctx context.Context, in io.Reader, store *archive.Store) (Report, error) {
	var report Report
	if in == nil || store == nil {
		return report, errors.New("an input reader and archive are required")
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if err := store.Ingest(nil); err != nil {
		return report, errors.New("archive is unavailable for import")
	}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), MaxLineBytes+2)
	batch := make([]archive.Message, 0, 100)
	refs := make(map[referenceKey]pendingReference)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		attachments := make([]archive.AttachmentRecord, 0, len(refs))
		for _, item := range refs {
			attachments = append(attachments, archive.AttachmentRecord{ChatJID: item.chat, MessageID: item.id, Descriptor: item.ref.descriptor()})
		}
		if err := store.IngestWithAttachments(batch, attachments); err != nil {
			return errors.New("archive rejected an import batch")
		}
		// Counts describe accepted input. Retention or a narrower concurrent policy
		// can discard rows and their descriptors before they are retained.
		report.AttachmentRefs += len(refs)
		report.Imported += len(batch)
		batch = batch[:0]
		clear(refs)
		return nil
	}
	line := 0
	fail := func(message string) (Report, error) {
		if err := flush(); err != nil {
			return report, fmt.Errorf("line %d: %s", line, err.Error())
		}
		return report, fmt.Errorf("line %d: %s", line, message)
	}
	for scanner.Scan() {
		line++
		if err := ctx.Err(); err != nil {
			return report, err
		}
		data := bytes.TrimSpace(scanner.Bytes())
		if len(scanner.Bytes()) > MaxLineBytes {
			return fail("record exceeds 128 KiB")
		}
		if len(data) == 0 {
			report.Skipped++
			continue
		}
		if data[0] != '{' {
			return fail("expected one message object")
		}
		m, rawRef, err := decodeRecord(data)
		if err != nil {
			return fail("invalid message JSON")
		}
		if !store.IsAllowed(m.ChatJID) {
			report.Skipped++
			continue
		}
		if !validMessage(m) {
			return fail("invalid or oversized message fields")
		}
		var ref *mediaReference
		if len(rawRef) > 0 {
			ref, err = decodeReference(rawRef, m.MediaType)
			if err != nil {
				return fail("invalid attachment descriptor")
			}
			if err := store.ValidateAttachment(ref.descriptor()); err != nil {
				return fail("invalid attachment descriptor or unavailable archive")
			}
		}
		// Duplicate message keys use the last valid input record, including an
		// explicit absence of attachment credentials. An earlier descriptor in
		// this same batch must not survive a later text-only replacement.
		key := referenceKey{m.ChatJID, m.ID}
		delete(refs, key)
		if ref != nil {
			refs[key] = pendingReference{chat: m.ChatJID, id: m.ID, ref: *ref}
		}
		batch = append(batch, m)
		if len(batch) == 100 {
			if err := flush(); err != nil {
				return report, fmt.Errorf("line %d: %s", line, err.Error())
			}
		}
	}
	if err := scanner.Err(); err != nil {
		line++
		return fail("cannot read a complete record or record exceeds 128 KiB")
	}
	if err := flush(); err != nil {
		return report, fmt.Errorf("line %d: %s", line, err.Error())
	}
	return report, nil
}

func decodeRecord(data []byte) (archive.Message, json.RawMessage, error) {
	var m archive.Message
	var ref json.RawMessage
	if !utf8.Valid(data) {
		return m, nil, errors.New("invalid JSON")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if token, err := dec.Token(); err != nil || token != json.Delim('{') {
		return m, nil, errors.New("invalid JSON")
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return m, nil, errors.New("invalid JSON")
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return m, nil, errors.New("invalid JSON")
		}
		switch key {
		case "id", "chat_jid", "sender", "text", "media_type", "timestamp", "from_me", "attachment_ref":
		default:
			return m, nil, errors.New("invalid JSON")
		}
		seen[key] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return m, nil, errors.New("invalid JSON")
		}
		if key == "attachment_ref" {
			ref = append(json.RawMessage(nil), value...)
		}
	}
	if _, err := dec.Token(); err != nil {
		return m, nil, errors.New("invalid JSON")
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return m, nil, errors.New("invalid JSON")
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, nil, errors.New("invalid JSON")
	}
	return m, ref, nil
}

func validMessage(m archive.Message) bool {
	if !config.ValidJID(m.ChatJID) || m.ID == "" || len(m.ID) > 256 || len(m.Sender) > 128 || len(m.Text) > archive.MaxTextBytes || len(m.MediaType) > 64 || m.Timestamp <= 0 {
		return false
	}
	if m.Text == "" && m.MediaType == "" {
		return false
	}
	if !utf8.ValidString(m.ID) || !utf8.ValidString(m.Text) || !utf8.ValidString(m.MediaType) {
		return false
	}
	if m.Sender == "" || config.ValidJID(m.Sender) {
		return true
	}
	for _, r := range m.Sender {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func parseTimestamp(value string) (int64, bool) {
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		if n > 100000000000 {
			n /= 1000
		}
		return n, n > 0
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02T15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
	} {
		if stamp, err := time.Parse(layout, value); err == nil {
			return stamp.Unix(), stamp.Unix() > 0
		}
	}
	return 0, false
}
