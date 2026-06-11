package dispatcher

import (
	"context"
	"errors"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/encoder/jsonl"
	"github.com/001ajd/change-data-capture/internal/sink"
)

func TestSinkDispatcherDispatchesToSinkHandler(t *testing.T) {
	target := &recordingSink{}
	dispatcher := NewSinkDispatcher(sink.NewHandler(jsonl.NewEncoder(), target))

	event := cdc.Event{Operation: cdc.OperationInsert}
	if err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatalf("dispatch event: %v", err)
	}

	if len(target.events) != 1 {
		t.Fatalf("events = %d, want 1", len(target.events))
	}
}

func TestSinkDispatcherReturnsSinkError(t *testing.T) {
	sinkErr := errors.New("sink failed")
	dispatcher := NewSinkDispatcher(sink.NewHandler(jsonl.NewEncoder(), &failingSink{err: sinkErr}))

	err := dispatcher.Dispatch(context.Background(), cdc.Event{})
	if !errors.Is(err, sinkErr) {
		t.Fatalf("error = %v, want sink error", err)
	}
}

type recordingSink struct {
	events []cdc.EncodedEvent
}

func (s *recordingSink) Write(_ context.Context, event cdc.EncodedEvent) error {
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) Close() error { return nil }

type failingSink struct {
	err error
}

func (s *failingSink) Write(context.Context, cdc.EncodedEvent) error {
	return s.err
}

func (s *failingSink) Close() error { return nil }
