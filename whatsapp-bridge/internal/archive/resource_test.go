package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"whatsapp-client/internal/config"
)

// BenchmarkBoundedImport exercises local storage with generated data only.
// Heap samples describe the Go runtime; measure process RSS separately.
func BenchmarkBoundedImport(b *testing.B) {
	const records = 20000
	const batchSize = 100
	const textBytes = 1024
	chats := []string{"123@s.whatsapp.net", "456@s.whatsapp.net"}
	cfg := config.Default()
	cfg.AllowedChats = []config.Chat{{JID: chats[0], Label: "Synthetic first"}, {JID: chats[1], Label: "Synthetic second"}}
	cfg.MaxMessagesPerChat = records / len(chats)
	b.SetBytes(records * textBytes)
	var peakHeap, peakSys uint64
	var dbBytes int64
	for iteration := 0; iteration < b.N; iteration++ {
		path := filepath.Join(b.TempDir(), "synthetic.db")
		store, err := Open(path, cfg)
		if err != nil {
			b.Fatal(err)
		}
		now := time.Now().Unix()
		for offset := 0; offset < records; offset += batchSize {
			batch := make([]Message, 0, batchSize)
			for i := offset; i < offset+batchSize; i++ {
				prefix := fmt.Sprintf("Generated local message %05d. ", i)
				batch = append(batch, Message{
					ID:        fmt.Sprintf("synthetic-%05d", i),
					ChatJID:   chats[i%len(chats)],
					Sender:    chats[i%len(chats)],
					Text:      prefix + strings.Repeat("x", textBytes-len(prefix)),
					Timestamp: now - int64(records-i),
				})
			}
			if err := store.Ingest(batch); err != nil {
				store.Close()
				b.Fatal(err)
			}
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			if mem.HeapAlloc > peakHeap {
				peakHeap = mem.HeapAlloc
			}
			if mem.Sys > peakSys {
				peakSys = mem.Sys
			}
		}
		stats, err := store.Stats()
		if err != nil || stats.Messages != records || stats.Chats != len(chats) {
			store.Close()
			b.Fatalf("retained rows: got %+v, error %v", stats, err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			b.Fatal(err)
		}
		dbBytes = info.Size()
	}
	b.ReportMetric(records, "retained_rows")
	b.ReportMetric(float64(dbBytes), "db_bytes")
	b.ReportMetric(float64(peakHeap), "sampled_heap_peak_bytes")
	b.ReportMetric(float64(peakSys), "sampled_runtime_sys_bytes")
}
