package sink

import (
	"context"
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
)

type Sink interface {
	Write(ctx context.Context, event cdc.Event) error
}

type Handler struct {
	sinks []Sink
}

func NewHandler(sinks ...Sink) *Handler {
	return &Handler{sinks: sinks}
}

func (h *Handler) Handle(ctx context.Context, event cdc.Event) error {
	for i, sink := range h.sinks {
		if err := sink.Write(ctx, event); err != nil {
			return fmt.Errorf("write to sink %d: %w", i, err)
		}
	}
	return nil
}
