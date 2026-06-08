package dispatcher

import (
	"context"
	"log"

	"github.com/001ajd/change-data-capture/internal/cdc"
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
