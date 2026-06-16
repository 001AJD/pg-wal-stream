package cli

import (
	"flag"
)

// Options holds all parsed command-line configurations.
type Options struct {
	ConfigPath string
	Init       bool
}

// Parser defines the contract for any CLI parsing implementation.
type Parser interface {
	Parse(args []string) (*Options, error)
}

// DefaultParser implements Parser using Go's standard flag package.
type DefaultParser struct{}

// NewDefaultParser creates a new instance of DefaultParser.
func NewDefaultParser() *DefaultParser {
	return &DefaultParser{}
}

// Parse implements the Parser interface.
func (p *DefaultParser) Parse(args []string) (*Options, error) {
	// Use a new FlagSet instead of the global flag variables for testability
	fs := flag.NewFlagSet("pg-wal-stream", flag.ContinueOnError)

	configPath := fs.String("config", "config.yaml", "Path to the configuration file")
	initFlag := fs.Bool("init", false, "Initialize a default config.example.yaml file")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Fallback: Use the first positional argument as the config path if provided
	if fs.NArg() > 0 {
		*configPath = fs.Arg(0)
	}

	return &Options{
		ConfigPath: *configPath,
		Init:       *initFlag,
	}, nil
}
