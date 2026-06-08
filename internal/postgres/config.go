package postgres

import "time"

func standbyStatusInterval(interval time.Duration) time.Duration {
	if interval > 0 {
		return interval
	}
	return 10 * time.Second
}
