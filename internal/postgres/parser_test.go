package postgres

import (
	"errors"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/jackc/pglogrepl"
)

func TestParserConvertsInsertUpdateDeleteMessages(t *testing.T) {
	p := newParser()
	relation := relationMessage()

	if events, err := p.parse(relation, "0/1"); err != nil {
		t.Fatalf("parse relation: %v", err)
	} else if len(events) != 0 {
		t.Fatalf("relation produced %d events, want 0", len(events))
	}

	tests := []struct {
		name      string
		message   pglogrepl.Message
		operation cdc.Operation
	}{
		{
			name: "insert",
			message: &pglogrepl.InsertMessage{
				RelationID: relation.RelationID,
				Tuple:      tuple("example.com", nil),
			},
			operation: cdc.OperationInsert,
		},
		{
			name: "update",
			message: &pglogrepl.UpdateMessage{
				RelationID: relation.RelationID,
				NewTuple:   tuple("updated.com", []byte("active")),
			},
			operation: cdc.OperationUpdate,
		},
		{
			name: "delete",
			message: &pglogrepl.DeleteMessage{
				RelationID: relation.RelationID,
				OldTuple:   tuple("deleted.com", []byte("inactive")),
			},
			operation: cdc.OperationDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events, err := p.parse(tt.message, "0/2")
			if err != nil {
				t.Fatalf("parse %s: %v", tt.name, err)
			}
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}

			event := events[0]
			if event.Operation != tt.operation {
				t.Fatalf("operation = %s, want %s", event.Operation, tt.operation)
			}
			if event.Schema != "public" || event.Table != "domains" {
				t.Fatalf("relation = %s.%s, want public.domains", event.Schema, event.Table)
			}
			if event.LSN != "0/2" {
				t.Fatalf("lsn = %s, want 0/2", event.LSN)
			}
			if _, ok := event.Columns["name"]; !ok {
				t.Fatal("missing name column")
			}
		})
	}
}

func TestParserPreservesNullValues(t *testing.T) {
	p := newParser()
	relation := relationMessage()
	if _, err := p.parse(relation, "0/1"); err != nil {
		t.Fatalf("parse relation: %v", err)
	}

	events, err := p.parse(&pglogrepl.InsertMessage{
		RelationID: relation.RelationID,
		Tuple:      tuple("example.com", nil),
	}, "0/2")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}

	value := events[0].Columns["status"]
	if !value.Null {
		t.Fatalf("status null = false, want true")
	}
	if value.Text != "" {
		t.Fatalf("status text = %q, want empty", value.Text)
	}
}

func TestParserReturnsRelationErrorWhenRelationIsMissing(t *testing.T) {
	p := newParser()

	_, err := p.parse(&pglogrepl.InsertMessage{
		RelationID: 99,
		Tuple:      tuple("example.com", nil),
	}, "0/2")
	if !errors.Is(err, ErrRelationNotFound) {
		t.Fatalf("error = %v, want ErrRelationNotFound", err)
	}
}

func relationMessage() *pglogrepl.RelationMessage {
	return &pglogrepl.RelationMessage{
		RelationID:   42,
		Namespace:    "public",
		RelationName: "domains",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "name"},
			{Name: "status"},
		},
	}
}

func tuple(name string, status []byte) *pglogrepl.TupleData {
	statusColumn := &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
	if status != nil {
		statusColumn = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: status}
	}

	return &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte(name)},
			statusColumn,
		},
	}
}
