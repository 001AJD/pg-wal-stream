package cdc

type Operation string

const (
	OperationInsert Operation = "insert"
	OperationUpdate Operation = "update"
	OperationDelete Operation = "delete"
)

type Value struct {
	Text             string
	Null             bool
	Binary           []byte
	UnchangedToasted bool
}

type Event struct {
	Operation Operation
	Schema    string
	Table     string
	LSN       string
	CommitLSN string
	Columns   map[string]Value
}

type EncodedEvent struct {
	Data []byte
	LSN  string
}

type Acker interface {
	Acknowledge(lsn string)
}
