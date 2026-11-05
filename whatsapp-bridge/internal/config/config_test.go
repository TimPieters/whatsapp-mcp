package config

import (
	"os"
	"path/filepath"
	"testing"
)

func loadJSON(t *testing.T, data string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestStrictConfigurationAndDefaults(t *testing.T) {
	c, err := loadJSON(t, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != "127.0.0.1:8787" || c.RetentionDays != 90 || c.MaxMessagesPerChat != 10000 || c.MaxResults != 100 || len(c.AllowedChats) != 0 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	for _, input := range []string{
		`null`, `[]`, `{"unknown":true}`, `{} {}`,
		`{"retention_days":0}`, `{"max_results":1001}`,
		`{"allowed_chats":[{"jid":"*"}]}`,
		`{"allowed_chats":[{"jid":"123@s.whatsapp.net","extra":true}]}`,
		`{"allowed_chats":[{"jid":"123@lid"},{"jid":"123@lid"}]}`,
	} {
		if _, err := loadJSON(t, input); err == nil {
			t.Errorf("accepted invalid configuration %s", input)
		}
	}
}

func TestJIDsAreExact(t *testing.T) {
	for _, jid := range []string{"123@s.whatsapp.net", "123@lid", "123@g.us", "123-456@g.us"} {
		if !ValidJID(jid) {
			t.Errorf("rejected %q", jid)
		}
	}
	for _, jid := range []string{"*", "123@broadcast", "status@broadcast", "123:2@s.whatsapp.net", "+123@s.whatsapp.net", "123@S.WHATSAPP.NET", " 123@lid", "123@lid ", "123-@g.us", "abc@g.us", "123@s.whatsapp.net\n"} {
		if ValidJID(jid) {
			t.Errorf("accepted %q", jid)
		}
	}
}
