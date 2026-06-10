package main

import (
	"context"
	"log"
	"time"

	appconfig "github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
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

	startLSN, err := pglogrepl.ParseLSN(config.Postgres.StartLSN)
	if err != nil {
		log.Fatalf("parse start lsn: %v", err)
	}

	tracker := postgres.NewLSNTracker(startLSN)

	configuredSink, err := sink.NewFromConfig(config.Sink, tracker)
	if err != nil {
		log.Fatal(err)
	}

	sinkHandler := sink.NewHandler(configuredSink)
	defer sinkHandler.Close()
	streamer := postgres.NewStreamer(config.Postgres, dispatcher.NewSinkDispatcher(sinkHandler), tracker)
	if err := streamer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
