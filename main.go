package main

import (
	"context"
	"log"
	"time"

	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/postgres"
)

func main() {
	ctx := context.Background()

	config := postgres.Config{
		ConnString:            "host=localhost port=5432 user=replicator password=secret dbname=domains replication=database",
		SlotName:              "slot_domain_cdc",
		StartLSN:              "0/D7D7A90",
		PublicationNames:      []string{"pub_domain_cdc"},
		StandbyStatusInterval: 10 * time.Second,
	}

	streamer := postgres.NewStreamer(config, dispatcher.NewLoggingDispatcher())
	if err := streamer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
