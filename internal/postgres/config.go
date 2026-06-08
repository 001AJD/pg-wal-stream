package postgres

import "time"

type Config struct {
	ConnString            string
	SlotName              string
	StartLSN              string
	PublicationNames      []string
	StandbyStatusInterval time.Duration
}

func (c Config) standbyStatusInterval() time.Duration {
	if c.StandbyStatusInterval > 0 {
		return c.StandbyStatusInterval
	}
	return 10 * time.Second
}
