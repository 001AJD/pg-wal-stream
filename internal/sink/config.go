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
		return localfilesink.NewLocalFileSink(l, cfg.LocalFile.DestinationDir, acker, m, h), nil
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Type)
	}
}
