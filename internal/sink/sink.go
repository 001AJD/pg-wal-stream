package sink

import (
	"context"
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
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
	metrics *metrics.Metrics
}

func NewHandler(encoder Encoder, m *metrics.Metrics, sinks ...Sink) *Handler {
	return &Handler{encoder: encoder, sinks: sinks, metrics: m}
}

func (h *Handler) Handle(ctx context.Context, event cdc.Event) error {
	if h.metrics != nil {
		switch event.Operation {
		case cdc.OperationInsert:
			h.metrics.TotalInserts.Inc()
		case cdc.OperationUpdate:
			h.metrics.TotalUpdates.Inc()
		case cdc.OperationDelete:
			h.metrics.TotalDeletes.Inc()
		}
		h.metrics.EventsPerTable.WithLabelValues(event.Schema, event.Table, string(event.Operation)).Inc()
	}

	data, err := h.encoder.Encode(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	encoded := cdc.EncodedEvent{
		Data:   data,
		LSN:    event.LSN,
		Schema: event.Schema,
		Table:  event.Table,
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
