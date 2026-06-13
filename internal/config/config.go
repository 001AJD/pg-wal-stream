package config

import "time"

const SinkTypeLocalFile = "localfile"

type Config struct {
	LogLevel string
	Postgres Postgres
	Sink     Sink
}

type Postgres struct {
	ConnString            string
	SlotName              string
	StartLSN              string
	PublicationNames      []string
	StandbyStatusInterval time.Duration
	MaxReconnectAttempts  int           // 0 = unlimited retries
	ReconnectBaseDelay    time.Duration // base delay for exponential backoff (default: 1s)
	ReconnectMaxDelay     time.Duration // maximum delay cap (default: 60s)
}

type Sink struct {
	Type      string
	LocalFile LocalFileSink
}

type LocalFileSink struct {
	DestinationDir string
	MaxFileSize    int64  // max segment file size in bytes; 0 = use default (200 MiB)
	DbName         string // database name used in segment file naming
}
