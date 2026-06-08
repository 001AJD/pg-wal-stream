package sink

import (
	"fmt"

	"github.com/001ajd/change-data-capture/internal/config"
)

func NewFromConfig(cfg config.Sink) (Sink, error) {
	switch cfg.Type {
	case config.SinkTypeLocalFile:
		if cfg.LocalFile.DestinationDir == "" {
			return nil, fmt.Errorf("localfile sink destination directory is required")
		}
		return NewLocalFileSink(cfg.LocalFile.DestinationDir), nil
	default:
		return nil, fmt.Errorf("unsupported sink type %q", cfg.Type)
	}
}
