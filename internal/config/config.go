package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const SinkTypeLocalFile = "localfile"

type Config struct {
	LogLevel string   `yaml:"log_level"`
	Postgres Postgres `yaml:"postgres"`
	Sink     Sink     `yaml:"sink"`
}

type Postgres struct {
	ConnString            string        `yaml:"conn_string"`
	SlotName              string        `yaml:"slot_name"`
	StartLSN              string        `yaml:"start_lsn"`
	PublicationNames      []string      `yaml:"publication_names"`
	StandbyStatusInterval time.Duration `yaml:"standby_status_interval"`
	MaxReconnectAttempts  int           `yaml:"max_reconnect_attempts"` // 0 = unlimited retries
	ReconnectBaseDelay    time.Duration `yaml:"reconnect_base_delay"`   // base delay for exponential backoff (default: 1s)
	ReconnectMaxDelay     time.Duration `yaml:"reconnect_max_delay"`    // maximum delay cap (default: 60s)
}

type Sink struct {
	Type      string        `yaml:"type"`
	LocalFile LocalFileSink `yaml:"local_file"`
}

type LocalFileSink struct {
	DestinationDir string `yaml:"destination_dir"`
	MaxFileSize    int64  `yaml:"max_file_size"` // max segment file size in bytes; 0 = use default (200 MiB)
	DbName         string `yaml:"db_name"`       // database name used in segment file naming
}

// Load reads the YAML configuration file from the given path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &cfg, nil
}

// GenerateExample generates a default config file at the specified path.
func GenerateExample(path string) error {
	cfg := Config{
		LogLevel: "debug",
		Postgres: Postgres{
			ConnString:            "host=localhost port=5432 user=replicator password=secret dbname=domains replication=database",
			SlotName:              "slot_domain_cdc",
			StartLSN:              "0/0",
			PublicationNames:      []string{"pub_domain_cdc"},
			StandbyStatusInterval: 10 * time.Second,
			MaxReconnectAttempts:  0,
			ReconnectBaseDelay:    1 * time.Second,
			ReconnectMaxDelay:     60 * time.Second,
		},
		Sink: Sink{
			Type: SinkTypeLocalFile,
			LocalFile: LocalFileSink{
				DestinationDir: "/data/destination",
				MaxFileSize:    200 * 1024 * 1024, // 200 MiB
				DbName:         "domains",
			},
		},
	}

	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal example config: %w", err)
	}

	return os.WriteFile(path, data, 0644)
}
