package localfilesink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
)

const (
	defaultJSONLFileName = "events.jsonl"
	defaultBufferSize    = 1000
)

type eventBatchItem struct {
	data []byte
	lsn  string
}

type LocalFileSink struct {
	destinationDir string
	fileName       string
	acker          cdc.Acker
	logger         logger.Logger
	metrics        *metrics.Metrics

	eventChan chan eventBatchItem
	err       atomic.Value // holds error
	wg        sync.WaitGroup
	closed    atomic.Bool
}

func NewLocalFileSink(l logger.Logger, destinationDir string, acker cdc.Acker, m *metrics.Metrics, h *health.Registry) *LocalFileSink {
	s := &LocalFileSink{
		destinationDir: destinationDir,
		fileName:       defaultJSONLFileName,
		acker:          acker,
		logger:         l.With("sink", "localfile", "dir", destinationDir),
		metrics:        m,
		eventChan:      make(chan eventBatchItem, defaultBufferSize),
	}

	if h != nil {
		h.AddReadinessCheck("local_file_sink", health.CheckerFunc(func(ctx context.Context) error {
			if err := s.checkWorkerError(); err != nil {
				return err
			}
			return nil
		}))
	}

	s.wg.Add(1)
	go s.worker()

	return s
}

// Writes (appends) the data to the destination file.
func (s *LocalFileSink) Write(ctx context.Context, event cdc.EncodedEvent) error {
	if s.closed.Load() {
		return fmt.Errorf("sink is closed")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := s.checkWorkerError(); err != nil {
		return fmt.Errorf("worker error: %w", err)
	}

	item := eventBatchItem{
		data: event.Data,
		lsn:  event.LSN,
	}

	select {
	case s.eventChan <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Closes the event channel
func (s *LocalFileSink) Close() error {
	if s.closed.Swap(true) {
		return nil
	}

	close(s.eventChan)
	s.wg.Wait()

	return s.checkWorkerError()
}

func (s *LocalFileSink) checkWorkerError() error {
	if err := s.err.Load(); err != nil {
		return err.(error)
	}
	return nil
}

func (s *LocalFileSink) worker() {
	defer s.wg.Done()

	s.logger.Info("starting local file sink worker")

	if err := os.MkdirAll(s.destinationDir, 0o755); err != nil {
		err = fmt.Errorf("create destination directory: %w", err)
		s.logger.Error("failed to create destination directory", "error", err)
		s.err.Store(err)
		return
	}

	filePath := filepath.Join(s.destinationDir, s.fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		err = fmt.Errorf("open jsonl file: %w", err)
		s.logger.Error("failed to open jsonl file", "error", err, "path", filePath)
		s.err.Store(err)
		return
	}
	defer file.Close()

	s.logger.Info("opened jsonl file for writing", "path", filePath)

	for item := range s.eventChan {
		writeStart := time.Now()
		if _, err := file.Write(item.data); err != nil {
			err = fmt.Errorf("write jsonl record: %w", err)
			s.logger.Error("failed to write jsonl record", "error", err)
			s.err.Store(err)
			if s.metrics != nil {
				s.metrics.EventsFailedTotal.Inc()
			}
			return
		}
		if s.metrics != nil {
			s.metrics.SinkWriteLatency.Observe(time.Since(writeStart).Seconds())
		}

		syncStart := time.Now()
		if err := file.Sync(); err != nil {
			err = fmt.Errorf("sync jsonl file: %w", err)
			s.logger.Error("failed to sync jsonl file", "error", err)
			s.err.Store(err)
			if s.metrics != nil {
				s.metrics.EventsFailedTotal.Inc()
			}
			return
		}

		if s.metrics != nil {
			s.metrics.EventsWrittenTotal.Inc()
			s.metrics.BatchFlushDuration.Observe(time.Since(syncStart).Seconds())
		}

		if s.acker != nil {
			s.acker.Acknowledge(item.lsn)
		}
	}

	s.logger.Info("local file sink worker stopped")
}
