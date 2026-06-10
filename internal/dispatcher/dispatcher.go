package dispatcher

import (
	"context"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/sink"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, event cdc.Event) error
	Close() error
}

type LoggingDispatcher struct {
	logger logger.Logger
}

func NewLoggingDispatcher(l logger.Logger) *LoggingDispatcher {
	return &LoggingDispatcher{logger: l}
}

func (d *LoggingDispatcher) Dispatch(_ context.Context, event cdc.Event) error {
	d.logger.Info(
		"cdc event received",
		"operation", event.Operation,
		"table", event.Schema+"."+event.Table,
		"lsn", event.LSN,
		"columns", len(event.Columns),
	)
	return nil
}

func (d *LoggingDispatcher) Close() error {
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

func (d *SinkDispatcher) Close() error {
	return d.handler.Close()
}
