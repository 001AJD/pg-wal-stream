package sink

import (
	"testing"

	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/sink/localfilesink"
)

func TestNewFromConfigCreatesLocalFileSink(t *testing.T) {
	target, err := NewFromConfig(config.Sink{
		Type: config.SinkTypeLocalFile,
		LocalFile: config.LocalFileSink{
			DestinationDir: "destination",
		},
	}, nil)
	if err != nil {
		t.Fatalf("new sink from config: %v", err)
	}

	if _, ok := target.(*localfilesink.LocalFileSink); !ok {
		t.Fatalf("sink has type %T, want *localfilesink.LocalFileSink", target)
	}
}

func TestNewFromConfigReturnsErrorForUnsupportedSinkType(t *testing.T) {
	_, err := NewFromConfig(config.Sink{Type: "unknown"}, nil)
	if err == nil {
		t.Fatal("error = nil, want unsupported sink type error")
	}
}

func TestNewFromConfigReturnsErrorForMissingLocalFileDestination(t *testing.T) {
	_, err := NewFromConfig(config.Sink{Type: config.SinkTypeLocalFile}, nil)
	if err == nil {
		t.Fatal("error = nil, want missing destination error")
	}
}
