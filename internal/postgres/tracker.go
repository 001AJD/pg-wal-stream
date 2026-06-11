package postgres

import (
	"sync"

	"github.com/001ajd/change-data-capture/internal/observability/metrics"
	"github.com/jackc/pglogrepl"
)

type LSNTracker struct {
	mu         sync.RWMutex
	flushedLSN pglogrepl.LSN
	metrics    *metrics.Metrics
}

func NewLSNTracker(startLSN pglogrepl.LSN, m *metrics.Metrics) *LSNTracker {
	return &LSNTracker{
		flushedLSN: startLSN,
		metrics:    m,
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
		if t.metrics != nil {
			t.metrics.LastCommittedLSN.Set(float64(uint64(lsn)))
		}
	}
}

func (t *LSNTracker) GetFlushed() pglogrepl.LSN {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.flushedLSN
}
