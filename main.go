package main

import (
	"context"
	"log"
	"time"

	appconfig "github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/postgres"
	"github.com/001ajd/change-data-capture/internal/sink"
)

func main() {
	ctx := context.Background()

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
				DestinationDir: "destination",
			},
		},
	}

	configuredSink, err := sink.NewFromConfig(config.Sink)
	if err != nil {
		log.Fatal(err)
	}

	sinkHandler := sink.NewHandler(configuredSink)
	streamer := postgres.NewStreamer(config.Postgres, dispatcher.NewSinkDispatcher(sinkHandler))
	if err := streamer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
