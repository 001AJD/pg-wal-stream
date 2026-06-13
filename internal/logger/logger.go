package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"

	slogmulti "github.com/samber/slog-multi"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// Logger is a generic logging interface that can be implemented by various logging libraries.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	With(args ...any) Logger
}

// SlogLogger is an implementation of the Logger interface using slog.
type SlogLogger struct {
	inner *slog.Logger
}

// NewSlogLogger creates a new SlogLogger with a JSON handler and specified level.
func NewSlogLogger(level string) *SlogLogger {
	var slogLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: slogLevel,
	}

	stdoutHandler := slog.NewJSONHandler(os.Stdout, opts)

	exporter, err := otlploghttp.New(context.Background(),
		otlploghttp.WithEndpoint("localhost:4318"),
		otlploghttp.WithInsecure(),
	)
	if err == nil {
		res := resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName("pg-wal-stream"),
		)
		provider := log.NewLoggerProvider(
			log.WithProcessor(log.NewBatchProcessor(exporter)),
			log.WithResource(res),
		)

		otelHandler := otelslog.NewHandler("pg-wal-stream",
			otelslog.WithLoggerProvider(provider),
		)

		combinedHandler := slogmulti.Fanout(stdoutHandler, otelHandler)

		return &SlogLogger{
			inner: slog.New(combinedHandler),
		}
	}

	return &SlogLogger{
		inner: slog.New(stdoutHandler),
	}
}

func (l *SlogLogger) Debug(msg string, args ...any) {
	l.inner.Debug(msg, args...)
}

func (l *SlogLogger) Info(msg string, args ...any) {
	l.inner.Info(msg, args...)
}

func (l *SlogLogger) Warn(msg string, args ...any) {
	l.inner.Warn(msg, args...)
}

func (l *SlogLogger) Error(msg string, args ...any) {
	l.inner.Error(msg, args...)
}

func (l *SlogLogger) With(args ...any) Logger {
	return &SlogLogger{
		inner: l.inner.With(args...),
	}
}

// NewDefaultLogger returns a default logger instance with the given level.
func NewDefaultLogger(level string) Logger {
	return NewSlogLogger(level)
}

// Default returns a default logger instance with Info level.
func Default() Logger {
	return NewSlogLogger("info")
}

// NewNopLogger returns a logger that does nothing. Useful for tests.
type NopLogger struct{}

func (n *NopLogger) Debug(msg string, args ...any) {}
func (n *NopLogger) Info(msg string, args ...any)  {}
func (n *NopLogger) Warn(msg string, args ...any)  {}
func (n *NopLogger) Error(msg string, args ...any) {}
func (n *NopLogger) With(args ...any) Logger       { return n }

func NewNopLogger() Logger {
	return &NopLogger{}
}
