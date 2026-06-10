package main

import (
	"context"
	"os"
	"time"

	appconfig "github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/postgres"
	"github.com/001ajd/change-data-capture/internal/sink"
	"github.com/jackc/pglogrepl"
)

// entry point. The execution starts here
func main() {
	ctx := context.Background()

	// Postgres database configuration + The sink configuration
	// The publication, replication slot and replication level = logical should be already set before running this.
	config := appconfig.Config{
		LogLevel: "debug",
		Postgres: appconfig.Postgres{
			ConnString:            "host=localhost port=5432 user=replicator password=secret dbname=domains replication=database",
			SlotName:              "slot_domain_cdc",
			StartLSN:              "0/D7D7A90",
			PublicationNames:      []string{"pub_domain_cdc"},
			StandbyStatusInterval: 10 * time.Second,
		},
		Sink: appconfig.Sink{
			Type: appconfig.SinkTypeLocalFile,
			LocalFile: appconfig.LocalFileSink{
				DestinationDir: "./destination",
			},
		},
	}

	l := logger.NewDefaultLogger(config.LogLevel)

	startLSN, err := pglogrepl.ParseLSN(config.Postgres.StartLSN)
	if err != nil {
		l.Error("failed to parse start lsn", "error", err, "lsn", config.Postgres.StartLSN)
		os.Exit(1)
	}

	tracker := postgres.NewLSNTracker(startLSN)

	configuredSink, err := sink.NewFromConfig(l, config.Sink, tracker)
	if err != nil {
		l.Error("failed to configure sink", "error", err)
		os.Exit(1)
	}

	sinkHandler := sink.NewHandler(configuredSink)
	defer sinkHandler.Close()
	streamer := postgres.NewStreamer(l, config.Postgres, dispatcher.NewSinkDispatcher(sinkHandler), tracker)
	if err := streamer.Run(ctx); err != nil {
		l.Error("streamer error", "error", err)
		os.Exit(1)
	}
}
