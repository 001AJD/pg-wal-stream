package sink

import (
	"context"
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

type Sink interface {
	Write(ctx context.Context, event cdc.EncodedEvent) error
	Close() error
}

type Encoder interface {
	Encode(event cdc.Event) ([]byte, error)
}

type Handler struct {
	encoder Encoder
	sinks   []Sink
}

func NewHandler(encoder Encoder, sinks ...Sink) *Handler {
	return &Handler{encoder: encoder, sinks: sinks}
}

func (h *Handler) Handle(ctx context.Context, event cdc.Event) error {
	data, err := h.encoder.Encode(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	encoded := cdc.EncodedEvent{
		Data: data,
		LSN:  event.LSN,
	}

	for i, sink := range h.sinks {
		if err := sink.Write(ctx, encoded); err != nil {
			return fmt.Errorf("write to sink %d: %w", i, err)
		}
	}
	return nil
}

func (h *Handler) Close() error {
	for i, sink := range h.sinks {
		if err := sink.Close(); err != nil {
			return fmt.Errorf("close sink %d: %w", i, err)
		}
	}
	return nil
}
