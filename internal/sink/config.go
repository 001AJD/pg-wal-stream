package sink

import (
	"fmt"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/sink/localfilesink"
)

func NewFromConfig(cfg config.Sink, acker cdc.Acker) (Sink, error) {
	switch cfg.Type {
	case config.SinkTypeLocalFile:
		if cfg.LocalFile.DestinationDir == "" {
			return nil, fmt.Errorf("localfile sink destination directory is required")
		}
		return localfilesink.NewLocalFileSink(cfg.LocalFile.DestinationDir, acker), nil
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Type)
	}
}
