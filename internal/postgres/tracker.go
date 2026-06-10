package postgres

import (
	"sync"

	"github.com/jackc/pglogrepl"
)

type LSNTracker struct {
	mu         sync.RWMutex
	flushedLSN pglogrepl.LSN
}

func NewLSNTracker(startLSN pglogrepl.LSN) *LSNTracker {
	return &LSNTracker{
		flushedLSN: startLSN,
	}
}

func (t *LSNTracker) Acknowledge(lsnStr string) {
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if lsn > t.flushedLSN {
		t.flushedLSN = lsn
	}
}

func (t *LSNTracker) GetFlushed() pglogrepl.LSN {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.flushedLSN
}
