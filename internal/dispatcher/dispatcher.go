package dispatcher

import (
	"context"
	"log"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/sink"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, event cdc.Event) error
}

type LoggingDispatcher struct{}

func NewLoggingDispatcher() *LoggingDispatcher {
	return &LoggingDispatcher{}
}

func (d *LoggingDispatcher) Dispatch(_ context.Context, event cdc.Event) error {
	log.Printf(
		"cdc event operation=%s table=%s.%s lsn=%s columns=%d",
		event.Operation,
		event.Schema,
		event.Table,
		event.LSN,
		len(event.Columns),
	)
	return nil
}

type SinkDispatcher struct {
	handler *sink.Handler
}

func NewSinkDispatcher(handler *sink.Handler) *SinkDispatcher {
	return &SinkDispatcher{handler: handler}
}

func (d *SinkDispatcher) Dispatch(ctx context.Context, event cdc.Event) error {
	return d.handler.Handle(ctx, event)
}
