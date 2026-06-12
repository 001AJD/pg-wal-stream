package postgres

import (
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/jackc/pglogrepl"
)

type parser struct {
	relations map[uint32]*pglogrepl.RelationMessage
}

func newParser() *parser {
	return &parser{
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}
}

// Reset clears the cached relation metadata. Must be called on reconnect
// because Postgres may not re-send RelationMessages for unchanged tables,
// and any DDL changes during disconnection would make stale entries dangerous.
func (p *parser) Reset() {
	p.relations = make(map[uint32]*pglogrepl.RelationMessage)
}

func (p *parser) parse(msg pglogrepl.Message, lsn string) ([]cdc.Event, error) {
	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		p.relations[msg.RelationID] = msg
		return nil, nil
	case *pglogrepl.InsertMessage:
		return p.parseTuple(cdc.OperationInsert, msg.RelationID, msg.Tuple, lsn)
	case *pglogrepl.UpdateMessage:
		return p.parseTuple(cdc.OperationUpdate, msg.RelationID, msg.NewTuple, lsn)
	case *pglogrepl.DeleteMessage:
		return p.parseTuple(cdc.OperationDelete, msg.RelationID, msg.OldTuple, lsn)
	default:
		return nil, nil
	}
}

func (p *parser) parseTuple(operation cdc.Operation, relationID uint32, tuple *pglogrepl.TupleData, lsn string) ([]cdc.Event, error) {
	relation, ok := p.relations[relationID]
	if !ok {
		return nil, fmt.Errorf("%w: relation id %d", ErrRelationNotFound, relationID)
	}

	columns, err := tupleColumns(relation, tuple)
	if err != nil {
		return nil, err
	}

	return []cdc.Event{
		{
			Operation: operation,
			Schema:    relation.Namespace,
			Table:     relation.RelationName,
			LSN:       lsn,
			Columns:   columns,
		},
	}, nil
}

func tupleColumns(relation *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]cdc.Value, error) {
	if tuple == nil {
		return nil, fmt.Errorf("tuple data is nil for relation %s.%s", relation.Namespace, relation.RelationName)
	}
	if len(tuple.Columns) > len(relation.Columns) {
		return nil, fmt.Errorf(
			"tuple has %d columns but relation %s.%s has %d",
			len(tuple.Columns),
			relation.Namespace,
			relation.RelationName,
			len(relation.Columns),
		)
	}

	columns := make(map[string]cdc.Value, len(tuple.Columns))
	for i, col := range tuple.Columns {
		name := relation.Columns[i].Name
		columns[name] = tupleValue(col)
	}

	return columns, nil
}

func tupleValue(col *pglogrepl.TupleDataColumn) cdc.Value {
	switch col.DataType {
	case pglogrepl.TupleDataTypeNull:
		return cdc.Value{Null: true}
	case pglogrepl.TupleDataTypeToast:
		return cdc.Value{UnchangedToasted: true}
	case pglogrepl.TupleDataTypeBinary:
		data := make([]byte, len(col.Data))
		copy(data, col.Data)
		return cdc.Value{Binary: data}
	default:
		return cdc.Value{Text: string(col.Data)}
	}
}
