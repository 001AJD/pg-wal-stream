package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	appconfig "github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/encoder/jsonl"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/observability"
	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
	"github.com/001ajd/change-data-capture/internal/postgres"
	"github.com/001ajd/change-data-capture/internal/sink"
	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus"
)

// entry point. The execution starts here
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Postgres database configuration + The sink configuration
	// The publication, replication slot and replication level logical should be already set in postgres before running this.
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

	// Observability setup
	reg := prometheus.DefaultRegisterer
	m := metrics.NewMetrics(reg)
	h := health.NewRegistry()

	obsServer := observability.NewServer(":9090", h)
	go func() {
		l.Info("starting observability server", "addr", ":9090")
		if err := obsServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.Error("observability server failed", "error", err)
		}
	}()

	startLSN, err := pglogrepl.ParseLSN(config.Postgres.StartLSN)
	if err != nil {
		l.Error("failed to parse start lsn", "error", err, "lsn", config.Postgres.StartLSN)
		os.Exit(1)
	}

	tracker := postgres.NewLSNTracker(startLSN, m)

	configuredSink, err := sink.NewFromConfig(l, config.Sink, tracker, m, h)
	if err != nil {
		l.Error("failed to configure sink", "error", err)
		os.Exit(1)
	}

	sinkHandler := sink.NewHandler(jsonl.NewEncoder(), m, configuredSink)
	defer sinkHandler.Close()
	streamer := postgres.NewStreamer(l, config.Postgres, dispatcher.NewSinkDispatcher(sinkHandler), tracker, m, h)

	// Start graceful shutdown for observability server
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := obsServer.Shutdown(shutdownCtx); err != nil {
			l.Error("failed to shutdown observability server", "error", err)
		}
	}()

	if err := streamer.Run(ctx); err != nil {
		l.Error("streamer error", "error", err)
		os.Exit(1)
	}
}
