package sink

import (
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
	"github.com/001ajd/change-data-capture/internal/sink/localfilesink"
)

func NewFromConfig(l logger.Logger, cfg config.Sink, acker cdc.Acker, m *metrics.Metrics, h *health.Registry) (Sink, error) {
	switch cfg.Type {
	case config.SinkTypeLocalFile:
		if cfg.LocalFile.DestinationDir == "" {
			return nil, fmt.Errorf("localfile sink destination directory is required")
		}
		if cfg.LocalFile.DbName == "" {
			return nil, fmt.Errorf("localfile sink database name is required")
		}
		return localfilesink.NewLocalFileSink(l, cfg.LocalFile, acker, m, h)
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Type)
	}
}

// RecoverFlushedLSN attempts to recover the last flushed LSN from the configured sink.
// If the sink does not support recovery or no state is found, it returns an empty string.
func RecoverFlushedLSN(cfg config.Sink) (string, error) {
	switch cfg.Type {
	case config.SinkTypeLocalFile:
		return localfilesink.RecoverFlushedLSN(cfg.LocalFile.DestinationDir)
	default:
		return "", nil
	}
}
