package sink

import (
	"testing"

	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/sink/localfilesink"
)

func TestNewFromConfigCreatesLocalFileSink(t *testing.T) {
	target, err := NewFromConfig(logger.NewNopLogger(), config.Sink{
		Type: config.SinkTypeLocalFile,
		LocalFile: config.LocalFileSink{
			DestinationDir: t.TempDir(),
			MaxFileSize:    100 * 1024 * 1024,
			DbName:         "testdb",
		},
	}, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink from config: %v", err)
	}

	if _, ok := target.(*localfilesink.LocalFileSink); !ok {
		t.Fatalf("sink has type %T, want *localfilesink.LocalFileSink", target)
	}
}

func TestNewFromConfigReturnsErrorForUnsupportedSinkType(t *testing.T) {
	_, err := NewFromConfig(logger.NewNopLogger(), config.Sink{Type: "unknown"}, nil, nil, nil)
	if err == nil {
		t.Fatal("error = nil, want unsupported sink type error")
	}
}

func TestNewFromConfigReturnsErrorForMissingLocalFileDestination(t *testing.T) {
	_, err := NewFromConfig(logger.NewNopLogger(), config.Sink{
		Type: config.SinkTypeLocalFile,
		LocalFile: config.LocalFileSink{
			DbName: "testdb",
		},
	}, nil, nil, nil)
	if err == nil {
		t.Fatal("error = nil, want missing destination error")
	}
}

func TestNewFromConfigReturnsErrorForMissingDbName(t *testing.T) {
	_, err := NewFromConfig(logger.NewNopLogger(), config.Sink{
		Type: config.SinkTypeLocalFile,
		LocalFile: config.LocalFileSink{
			DestinationDir: t.TempDir(),
		},
	}, nil, nil, nil)
	if err == nil {
		t.Fatal("error = nil, want missing db name error")
	}
}
