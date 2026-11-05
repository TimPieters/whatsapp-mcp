// Package config validates the local archive configuration.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
)

type Chat struct {
	JID   string `json:"jid"`
	Label string `json:"label"`
}

type Config struct {
	Listen             string `json:"listen"`
	DataDir            string `json:"data_dir"`
	TokenFile          string `json:"token_file"`
	AllowedChats       []Chat `json:"allowed_chats"`
	RetentionDays      int    `json:"retention_days"`
	MaxMessagesPerChat int    `json:"max_messages_per_chat"`
	MaxResults         int    `json:"max_results"`
}

func Default() Config {
	return Config{Listen: "127.0.0.1:8787", DataDir: "data", RetentionDays: 90, MaxMessagesPerChat: 10000, MaxResults: 100}
}

// WithDefaults fills omitted settings. An omitted allowlist remains empty.
func (c Config) WithDefaults() Config {
	d := Default()
	if c.Listen == "" {
		c.Listen = d.Listen
	}
	if c.DataDir == "" {
		c.DataDir = d.DataDir
	}
	if c.RetentionDays == 0 {
		c.RetentionDays = d.RetentionDays
	}
	if c.MaxMessagesPerChat == 0 {
		c.MaxMessagesPerChat = d.MaxMessagesPerChat
	}
	if c.MaxResults == 0 {
		c.MaxResults = d.MaxResults
	}
	return c
}

var jidPattern = regexp.MustCompile(`^(?:[0-9]+@(?:s\.whatsapp\.net|lid)|[0-9]+(?:-[0-9]+)?@g\.us)$`)

// ValidJID accepts exact direct, linked-identity, and group identifiers only.
// Device suffixes, wildcards, whitespace, and alternate spellings are rejected.
func ValidJID(jid string) bool { return len(jid) <= 128 && jidPattern.MatchString(jid) }

func (c Config) Validate() error {
	if _, port, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen must be a host:port address")
	} else if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("listen port must be between 1 and 65535")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data_dir must not be empty")
	}
	if c.RetentionDays < 1 || c.RetentionDays > 36500 {
		return fmt.Errorf("retention_days must be between 1 and 36500")
	}
	if c.MaxMessagesPerChat < 1 || c.MaxMessagesPerChat > 1000000 {
		return fmt.Errorf("max_messages_per_chat must be between 1 and 1000000")
	}
	if c.MaxResults < 1 || c.MaxResults > 1000 {
		return fmt.Errorf("max_results must be between 1 and 1000")
	}
	if len(c.AllowedChats) > 10000 {
		return fmt.Errorf("allowed_chats exceeds 10000 entries")
	}
	seen := make(map[string]bool, len(c.AllowedChats))
	for i, chat := range c.AllowedChats {
		if !ValidJID(chat.JID) {
			return fmt.Errorf("allowed_chats[%d].jid is not an exact supported chat identifier", i)
		}
		if seen[chat.JID] {
			return fmt.Errorf("allowed_chats contains a duplicate identifier")
		}
		seen[chat.JID] = true
		if len(chat.Label) > 256 {
			return fmt.Errorf("allowed_chats[%d].label exceeds 256 bytes", i)
		}
	}
	return nil
}

func Load(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 2*1024*1024+1))
	if err != nil {
		return Config{}, err
	}
	if len(data) > 2*1024*1024 {
		return Config{}, fmt.Errorf("configuration exceeds 2 MiB")
	}
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return Config{}, fmt.Errorf("configuration must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	c := Default()
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("configuration must contain one JSON object")
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
