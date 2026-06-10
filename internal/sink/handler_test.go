package sink

import (
	"context"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

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

type recordingSink struct {
	events []cdc.Event
}

func (s *recordingSink) Write(_ context.Context, event cdc.Event) error {
	s.events = append(s.events, event)
	return nil
}

func (s *recordingSink) Close() error { return nil }
