package sink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

func TestLocalFileSinkWritesJSONLRecord(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	sink := NewLocalFileSink(destinationDir)

	event := cdc.Event{
		Operation: cdc.OperationUpdate,
		Schema:    "public",
		Table:     "domains",
		LSN:       "0/2",
		CommitLSN: "0/3",
		Columns: map[string]cdc.Value{
			"name":   {Text: "example.com"},
			"status": {Null: true},
			"blob":   {Binary: []byte("hello")},
			"body":   {UnchangedToasted: true},
		},
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write event: %v", err)
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
}

func TestLocalFileSinkAppendsRecords(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	sink := NewLocalFileSink(destinationDir)
	event := cdc.Event{Operation: cdc.OperationInsert, Columns: map[string]cdc.Value{}}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write first event: %v", err)
	}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write second event: %v", err)
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

	sink := NewLocalFileSink(filePath)
	err := sink.Write(context.Background(), cdc.Event{})
	if err == nil {
		t.Fatal("write error = nil, want error")
	}
}

func TestHandlerWritesToAllSinks(t *testing.T) {
	first := &recordingSink{}
	second := &recordingSink{}
	handler := NewHandler(first, second)

	event := cdc.Event{Operation: cdc.OperationDelete}
	if err := handler.Handle(context.Background(), event); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	if len(first.events) != 1 || len(second.events) != 1 {
		t.Fatalf("sink event counts = %d/%d, want 1/1", len(first.events), len(second.events))
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

type recordingSink struct {
	events []cdc.Event
}

func (s *recordingSink) Write(_ context.Context, event cdc.Event) error {
	s.events = append(s.events, event)
	return nil
}
