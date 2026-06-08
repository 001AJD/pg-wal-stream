package sink

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

const defaultJSONLFileName = "events.jsonl"

type LocalFileSink struct {
	destinationDir string
	fileName       string
	mu             sync.Mutex
}

func NewLocalFileSink(destinationDir string) *LocalFileSink {
	return &LocalFileSink{
		destinationDir: destinationDir,
		fileName:       defaultJSONLFileName,
	}
}

func (s *LocalFileSink) Write(ctx context.Context, event cdc.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.destinationDir, 0o755); err != nil {
		return fmt.Errorf("create destination directory: %w", err)
	}

	filePath := filepath.Join(s.destinationDir, s.fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open jsonl file: %w", err)
	}
	defer file.Close()

	if err := json.NewEncoder(file).Encode(jsonRecord(event)); err != nil {
		return fmt.Errorf("write jsonl record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync jsonl file: %w", err)
	}

	return nil
}

type eventRecord struct {
	Operation string         `json:"operation"`
	Schema    string         `json:"schema"`
	Table     string         `json:"table"`
	LSN       string         `json:"lsn"`
	CommitLSN string         `json:"commit_lsn"`
	Columns   map[string]any `json:"columns"`
}

func jsonRecord(event cdc.Event) eventRecord {
	columns := make(map[string]any, len(event.Columns))
	for name, value := range event.Columns {
		columns[name] = jsonValue(value)
	}

	return eventRecord{
		Operation: string(event.Operation),
		Schema:    event.Schema,
		Table:     event.Table,
		LSN:       event.LSN,
		CommitLSN: event.CommitLSN,
		Columns:   columns,
	}
}

func jsonValue(value cdc.Value) any {
	switch {
	case value.Null:
		return nil
	case value.UnchangedToasted:
		return map[string]bool{"unchanged_toasted": true}
	case len(value.Binary) > 0:
		return map[string]string{"binary": base64.StdEncoding.EncodeToString(value.Binary)}
	default:
		return value.Text
	}
}
