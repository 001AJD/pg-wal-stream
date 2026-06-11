package localfilesink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/logger"
)

func TestLocalFileSinkWritesJSONLRecord(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	acker := &recordingAcker{}
	sink := NewLocalFileSink(logger.NewNopLogger(), destinationDir, acker)

	event := cdc.EncodedEvent{
		Data: []byte(`{"operation":"update","schema":"public","table":"domains","lsn":"0/2","commit_lsn":"0/3","columns":{"blob":{"binary":"aGVsbG8="},"body":{"unchanged_toasted":true},"name":"example.com","status":null}}` + "\n"),
		LSN:  "0/2",
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write event: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	records := readJSONLLines(t, filepath.Join(destinationDir, defaultJSONLFileName))
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}

	record := records[0]
	if record["operation"] != "update" {
		t.Fatalf("operation = %v, want update", record["operation"])
	}
	if record["schema"] != "public" || record["table"] != "domains" {
		t.Fatalf("relation = %v.%v, want public.domains", record["schema"], record["table"])
	}
	if record["lsn"] != "0/2" || record["commit_lsn"] != "0/3" {
		t.Fatalf("lsn fields = %v/%v, want 0/2/0/3", record["lsn"], record["commit_lsn"])
	}

	columns, ok := record["columns"].(map[string]any)
	if !ok {
		t.Fatalf("columns has type %T, want map[string]any", record["columns"])
	}
	if columns["name"] != "example.com" {
		t.Fatalf("name = %v, want example.com", columns["name"])
	}
	if columns["status"] != nil {
		t.Fatalf("status = %v, want nil", columns["status"])
	}
	if got := columns["blob"].(map[string]any)["binary"]; got != "aGVsbG8=" {
		t.Fatalf("blob binary = %v, want aGVsbG8=", got)
	}
	if got := columns["body"].(map[string]any)["unchanged_toasted"]; got != true {
		t.Fatalf("body unchanged_toasted = %v, want true", got)
	}
	if got := acker.LSNs(); len(got) != 1 || got[0] != "0/2" {
		t.Fatalf("acked LSNs = %v, want [0/2]", got)
	}
}

func TestLocalFileSinkAppendsRecords(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	sink := NewLocalFileSink(logger.NewNopLogger(), destinationDir, nil)
	event := cdc.EncodedEvent{Data: []byte(`{"operation":"insert","columns":{}}` + "\n"), LSN: "0/1"}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write first event: %v", err)
	}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write second event: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	records := readJSONLLines(t, filepath.Join(destinationDir, defaultJSONLFileName))
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
}

func TestLocalFileSinkReturnsErrorForInvalidDestination(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "destination")
	if err := os.WriteFile(filePath, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("create destination file: %v", err)
	}

	sink := NewLocalFileSink(logger.NewNopLogger(), filePath, nil)
	event := cdc.EncodedEvent{Data: []byte("{}\n"), LSN: "0/1"}

	// Since Write is async, we might need a small delay or try multiple times
	// to see the worker error propagated back to Write.
	// Or just call Close and check the error.

	_ = sink.Write(context.Background(), event)

	// Wait a bit for worker to fail
	time.Sleep(10 * time.Millisecond)

	err := sink.Write(context.Background(), event)
	if err == nil {
		// Try Close
		err = sink.Close()
	}

	if err == nil {
		t.Fatal("write/close error = nil, want error")
	}
}

func readJSONLLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl file: %v", err)
	}
	defer file.Close()

	var records []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("unmarshal jsonl record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl file: %v", err)
	}

	return records
}

type recordingAcker struct {
	mu   sync.Mutex
	lsns []string
}

func (a *recordingAcker) Acknowledge(lsn string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lsns = append(a.lsns, lsn)
}

func (a *recordingAcker) LSNs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	lsns := make([]string, len(a.lsns))
	copy(lsns, a.lsns)
	return lsns
}
