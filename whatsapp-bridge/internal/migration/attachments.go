package migration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"whatsapp-client/internal/archive"
)

// Only owner-controlled migration files contain these private download keys.
// MCP message results expose an independent summary, never this structure.
type mediaReference struct {
	DirectPath    string `json:"direct_path"`
	MediaKey      []byte `json:"media_key"`
	FileSHA256    []byte `json:"file_sha256"`
	FileEncSHA256 []byte `json:"file_enc_sha256"`
	FileLength    int64  `json:"file_length"`
	MediaType     string `json:"media_type"`
	MIMEType      string `json:"mime_type,omitempty"`
	Filename      string `json:"filename,omitempty"`
}

type exportRecord struct {
	archive.Message
	Reference *mediaReference `json:"attachment_ref,omitempty"`
}

type pendingReference struct {
	chat, id string
	ref      mediaReference
}

type referenceKey struct{ chat, id string }
type sourceMedia struct {
	directPath, url, filename, mime sql.NullString
	key, hash, encHash              []byte
	length                          sql.NullInt64
}

func sourceMediaColumns(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(messages)")
	if err != nil {
		return "", errors.New("cannot inspect source message schema")
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notnull, &defaultValue, &pk); err != nil {
			return "", errors.New("cannot inspect source message schema")
		}
		columns[strings.ToLower(name)] = true
	}
	if rows.Err() != nil {
		return "", errors.New("cannot inspect source message schema")
	}
	// Names below are fixed literals, never source-controlled SQL identifiers.
	text := func(name string, max int) string {
		if !columns[name] {
			return "NULL"
		}
		return "CASE WHEN length(CAST(" + name + " AS BLOB))<=" + strconv.Itoa(max) + " THEN " + name + " END"
	}
	blob := func(name string) string {
		if !columns[name] {
			return "NULL"
		}
		return "CASE WHEN typeof(" + name + ")='blob' AND length(" + name + ")=32 THEN " + name + " END"
	}
	length := "NULL"
	if columns["file_length"] {
		length = "CASE WHEN typeof(file_length)='integer' AND file_length>0 THEN file_length END"
	}
	return strings.Join([]string{text("direct_path", 4096), text("url", 8192), text("filename", 255), text("mime_type", 128), blob("media_key"), blob("file_sha256"), blob("file_enc_sha256"), length}, ","), nil
}

func (s sourceMedia) reference(mediaType string, maxBytes int64) (*mediaReference, bool) {
	direct := s.directPath.String
	if direct == "" && s.url.Valid {
		var ok bool
		direct, ok = directPathFromURL(s.url.String)
		if !ok {
			return nil, false
		}
	}
	r := &mediaReference{DirectPath: direct, MediaKey: s.key, FileSHA256: s.hash, FileEncSHA256: s.encHash, FileLength: s.length.Int64, MediaType: mediaType, MIMEType: s.mime.String, Filename: s.filename.String}
	if r.MIMEType == "" {
		r.MIMEType = inferMIME(mediaType, r.Filename)
	}
	return r, validReference(*r, mediaType) && archive.ValidateAttachmentDescriptor(r.descriptor(), maxBytes) == nil
}

func directPathFromURL(value string) (string, bool) {
	if len(value) > 8192 || strings.IndexFunc(value, unicode.IsControl) >= 0 || strings.Contains(value, "\\") {
		return "", false
	}
	u, err := url.Parse(value)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Host == "" || u.Opaque != "" || (u.Port() != "" && u.Port() != "443") {
		return "", false
	}
	host := strings.ToLower(u.Hostname())
	trusted := false
	for _, domain := range []string{"whatsapp.net", "whatsapp.com", "fbcdn.net"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			trusted = true
			break
		}
	}
	if !trusted {
		return "", false
	}
	direct := u.EscapedPath()
	if u.RawQuery != "" {
		direct += "?" + u.RawQuery
	}
	return direct, validDirectPath(direct)
}

func validDirectPath(value string) bool {
	if len(value) == 0 || len(value) > 4096 || strings.Contains(value, "\\") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	u, err := url.Parse(value)
	if err != nil || u.IsAbs() || u.Host != "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return false
	}
	if !strings.HasPrefix(u.Path, "/v/") && !strings.HasPrefix(u.Path, "/mms/") && !strings.HasPrefix(u.Path, "/o1/v/") {
		return false
	}
	if strings.Contains(u.Path, "\\") || strings.IndexFunc(u.Path, unicode.IsControl) >= 0 {
		return false
	}
	for _, part := range strings.Split(u.Path, "/") {
		if part == ".." || part == "." {
			return false
		}
	}
	return true
}

func validReference(r mediaReference, mediaType string) bool {
	if r.MediaType != mediaType || !validDirectPath(r.DirectPath) || len(r.MediaKey) != 32 || len(r.FileSHA256) != 32 || len(r.FileEncSHA256) != 32 || r.FileLength <= 0 {
		return false
	}
	switch mediaType {
	case "image", "video", "audio", "document":
	default:
		return false
	}
	if len(r.Filename) > 255 || len(r.MIMEType) > 128 || !utf8.ValidString(r.Filename) || !utf8.ValidString(r.MIMEType) {
		return false
	}
	if strings.ContainsAny(r.Filename, "/\\") || r.Filename == "." || r.Filename == ".." || strings.IndexFunc(r.Filename, unicode.IsControl) >= 0 || strings.IndexFunc(r.MIMEType, unicode.IsControl) >= 0 {
		return false
	}
	return archive.ValidateAttachmentDescriptor(r.descriptor(), archive.MaxSupportedAttachmentBytes) == nil
}

func decodeReference(data []byte, mediaType string) (*mediaReference, error) {
	var r mediaReference
	dec := json.NewDecoder(bytes.NewReader(data))
	if token, err := dec.Token(); err != nil || token != json.Delim('{') {
		return nil, errors.New("invalid attachment descriptor")
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, errors.New("invalid attachment descriptor")
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return nil, errors.New("invalid attachment descriptor")
		}
		seen[key] = true
		switch key {
		case "direct_path", "media_key", "file_sha256", "file_enc_sha256", "file_length", "media_type", "mime_type", "filename":
		default:
			return nil, errors.New("invalid attachment descriptor")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("invalid attachment descriptor")
		}
	}
	if _, err := dec.Token(); err != nil {
		return nil, errors.New("invalid attachment descriptor")
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("invalid attachment descriptor")
	}
	if err := json.Unmarshal(data, &r); err != nil || !validReference(r, mediaType) {
		return nil, errors.New("invalid attachment descriptor")
	}
	return &r, nil
}

func (r mediaReference) descriptor() archive.AttachmentDescriptor {
	return archive.AttachmentDescriptor{DirectPath: r.DirectPath, MediaKey: r.MediaKey, FileSHA256: r.FileSHA256, FileEncSHA256: r.FileEncSHA256, FileLength: r.FileLength, MediaType: r.MediaType, MIMEType: r.MIMEType, Filename: r.Filename}
}

func inferMIME(mediaType, filename string) string {
	switch strings.ToLower(path.Ext(filename)) {
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".csv":
		return "text/csv"
	case ".pdf":
		return "application/pdf"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".png":
		return "image/png"
	case ".webp":
		return "image/webp"
	}
	switch mediaType {
	case "image":
		return "image/jpeg"
	case "video":
		return "video/mp4"
	case "audio":
		return "audio/ogg"
	default:
		return "application/octet-stream"
	}
}
