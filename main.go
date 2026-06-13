package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
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
	configPath := flag.String("config", "config.yaml", "Path to the configuration file")
	initFlag := flag.Bool("init", false, "Initialize a default config.example.yaml file")
	flag.Parse()

	if flag.NArg() > 0 {
		*configPath = flag.Arg(0)
	}

	if *initFlag {
		err := appconfig.GenerateExample("config.example.yaml")
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to generate example config: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Successfully generated config.example.yaml")
		os.Exit(0)
	}

	config, err := appconfig.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config (use -init to generate an example): %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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

	// Recover flushed LSN from the configured sink's state (if supported).
	// Falls back to the StartLSN from the config if no prior state exists.
	lsnString := config.Postgres.StartLSN
	recoveredLSN, err := sink.RecoverFlushedLSN(config.Sink)
	if err != nil {
		l.Error("failed to recover flushed LSN from sink", "error", err)
		os.Exit(1)
	}
	if recoveredLSN != "" {
		lsnString = recoveredLSN
		l.Info("state file found, using LSN from it", "lsn", recoveredLSN)
	} else {
		l.Info("state file not found or unsupported by sink, using config start LSN", "lsn", lsnString)
	}

	startLSN, err := pglogrepl.ParseLSN(lsnString)
	if err != nil {
		l.Error("failed to parse start lsn", "error", err, "lsn", lsnString)
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
