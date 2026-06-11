package jsonl

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

func TestEncoderEncodesEventAsJSONL(t *testing.T) {
	encoder := NewEncoder()
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

	data, err := encoder.Encode(event)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	if !strings.HasSuffix(string(data), "\n") {
		t.Fatalf("encoded event does not end with newline: %q", string(data))
	}

	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("unmarshal encoded event: %v", err)
	}

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
