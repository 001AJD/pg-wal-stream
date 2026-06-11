package jsonl

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

type Encoder struct{}

func NewEncoder() *Encoder {
	return &Encoder{}
}

func (e *Encoder) Encode(event cdc.Event) ([]byte, error) {
	data, err := json.Marshal(jsonRecord(event))
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	return append(data, '\n'), nil
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
