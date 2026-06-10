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
}

type Sink struct {
	Type      string
	LocalFile LocalFileSink
}

type LocalFileSink struct {
	DestinationDir string
}
