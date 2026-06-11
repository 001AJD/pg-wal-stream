package sink

import (
	"context"
	"errors"
	"testing"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

func TestHandlerWritesToAllSinks(t *testing.T) {
	first := &recordingSink{}
	second := &recordingSink{}
	encoder := &recordingEncoder{data: []byte("{}\n")}
	handler := NewHandler(encoder, nil, first, second)

	event := cdc.Event{Operation: cdc.OperationDelete, LSN: "0/2"}
	if err := handler.Handle(context.Background(), event); err != nil {
		t.Fatalf("handle event: %v", err)
	}

	if len(first.events) != 1 || len(second.events) != 1 {
		t.Fatalf("sink event counts = %d/%d, want 1/1", len(first.events), len(second.events))
	}
	if string(first.events[0].Data) != "{}\n" || first.events[0].LSN != "0/2" {
		t.Fatalf("first event = %#v, want encoded bytes and LSN 0/2", first.events[0])
	}
	if encoder.calls != 1 {
		t.Fatalf("encoder calls = %d, want 1", encoder.calls)
	}
}

func TestHandlerReturnsEncoderError(t *testing.T) {
	encoderErr := errors.New("encode failed")
	target := &recordingSink{}
	handler := NewHandler(&recordingEncoder{err: encoderErr}, nil, target)

	err := handler.Handle(context.Background(), cdc.Event{})
	if !errors.Is(err, encoderErr) {
		t.Fatalf("error = %v, want encoder error", err)
	}
	if len(target.events) != 0 {
		t.Fatalf("sink event count = %d, want 0", len(target.events))
	}
}

func TestHandlerReturnsSinkError(t *testing.T) {
	sinkErr := errors.New("sink failed")
	handler := NewHandler(&recordingEncoder{data: []byte("{}\n")}, nil, &failingSink{err: sinkErr})

	err := handler.Handle(context.Background(), cdc.Event{})
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

type recordingEncoder struct {
	data  []byte
	err   error
	calls int
}

func (e *recordingEncoder) Encode(cdc.Event) ([]byte, error) {
	e.calls++
	if e.err != nil {
		return nil, e.err
	}
	return e.data, nil
}
