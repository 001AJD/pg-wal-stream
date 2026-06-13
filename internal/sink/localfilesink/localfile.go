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
	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
)

const (
	defaultBufferSize               = 1000
	defaultMaxFileSize        int64 = 200 * 1024 * 1024 // 200 MiB
	statePersistWriteInterval       = 100                // persist state every N writes
)

type eventBatchItem struct {
	data   []byte
	lsn    string
	schema string
	table  string
}

// tableWriter manages a single open segment file for one db.schema.table stream.
type tableWriter struct {
	file        *os.File
	segmentInfo *SegmentInfo
	key         string // "db.schema.table"
}

type LocalFileSink struct {
	destinationDir string
	dbName         string
	maxFileSize    int64
	acker          cdc.Acker
	logger         logger.Logger
	metrics        *metrics.Metrics

	eventChan chan eventBatchItem
	err       atomic.Value // holds error
	wg        sync.WaitGroup
	closed    atomic.Bool

	// segment management (worker-goroutine-only, no mutex needed)
	currentDateDir string
	state          *SegmentState
	writers        map[string]*tableWriter
	writesSinceStateSync int
}

// NewLocalFileSink creates a new segmented local file sink.
// It returns an error if the destination directory cannot be created or
// if the segment state cannot be loaded from a previous run.
func NewLocalFileSink(l logger.Logger, cfg config.LocalFileSink, acker cdc.Acker, m *metrics.Metrics, h *health.Registry) (*LocalFileSink, error) {
	maxFileSize := cfg.MaxFileSize
	if maxFileSize <= 0 {
		maxFileSize = defaultMaxFileSize
	}

	s := &LocalFileSink{
		destinationDir: cfg.DestinationDir,
		dbName:         cfg.DbName,
		maxFileSize:    maxFileSize,
		acker:          acker,
		logger:         l.With("sink", "localfile", "dir", cfg.DestinationDir),
		metrics:        m,
		eventChan:      make(chan eventBatchItem, defaultBufferSize),
		writers:        make(map[string]*tableWriter),
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

	return s, nil
}

// Writes (appends) the data to the appropriate segment file.
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
		data:   event.Data,
		lsn:    event.LSN,
		schema: event.Schema,
		table:  event.Table,
	}

	select {
	case s.eventChan <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Closes the event channel and waits for the worker to finish.
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

	// Attempt recovery from previous run.
	if err := s.recoverState(); err != nil {
		err = fmt.Errorf("recover segment state: %w", err)
		s.logger.Error("failed to recover segment state", "error", err)
		s.err.Store(err)
		return
	}

	for item := range s.eventChan {
		if err := s.processEvent(item); err != nil {
			s.logger.Error("failed to process event", "error", err)
			s.err.Store(err)
			if s.metrics != nil {
				s.metrics.EventsFailedTotal.Inc()
			}
			return
		}
	}

	// Persist final state and close all writers on shutdown.
	s.closeAllWriters()
	s.logger.Info("local file sink worker stopped")
}

// recoverState loads segment state from the most recent date directory
// and re-opens segment files for appending.
func (s *LocalFileSink) recoverState() error {
	recentDir, err := findMostRecentDateDir(s.destinationDir)
	if err != nil {
		return err
	}

	if recentDir == "" {
		// No previous date directory — start fresh.
		s.currentDateDir = todayDateDir()
		s.state = newSegmentState()
		s.logger.Info("no previous state found, starting fresh", "dateDir", s.currentDateDir)
		return nil
	}

	dateDirPath := filepath.Join(s.destinationDir, recentDir)
	state, err := loadSegmentState(dateDirPath)
	if err != nil {
		return err
	}

	if state == nil {
		// Date dir exists but no state file — start fresh in that dir.
		s.currentDateDir = recentDir
		s.state = newSegmentState()
		s.logger.Info("date dir exists but no state, starting fresh", "dateDir", recentDir)
		return nil
	}

	s.currentDateDir = recentDir
	s.state = state

	// Re-open existing segment files.
	for key, info := range state.Segments {
		fileName := segmentFileName(key, info)
		filePath := filepath.Join(dateDirPath, fileName)

		// Stat the file to get actual size (handles stale state).
		fi, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				// Segment file is gone — advance to next segment.
				info.Segment++
				info.CurrentSize = 0
				s.logger.Warn("segment file missing during recovery, advancing",
					"key", key, "segment", info.Segment)
				continue
			}
			return fmt.Errorf("stat segment file %s: %w", filePath, err)
		}

		// Trust actual disk size over persisted state.
		info.CurrentSize = fi.Size()

		// Check if the recovered file needs rotation.
		if info.CurrentSize >= s.maxFileSize {
			info.Segment++
			info.CurrentSize = 0
			fileName = segmentFileName(key, info)
			filePath = filepath.Join(dateDirPath, fileName)
		}

		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("reopen segment file %s: %w", filePath, err)
		}

		s.writers[key] = &tableWriter{
			file:        file,
			segmentInfo: info,
			key:         key,
		}

		s.logger.Info("recovered segment",
			"key", key,
			"segment", info.Segment,
			"size", info.CurrentSize,
			"file", fileName,
		)
	}

	return nil
}

// processEvent handles a single event: ensures the correct date directory,
// gets or creates a writer, checks rotation, writes data, and acks.
func (s *LocalFileSink) processEvent(item eventBatchItem) error {
	// Check for date rollover.
	today := todayDateDir()
	if today != s.currentDateDir {
		s.logger.Info("date rollover detected", "from", s.currentDateDir, "to", today)
		s.closeAllWriters()
		s.currentDateDir = today
		s.state = newSegmentState()
	}

	key := fmt.Sprintf("%s.%s.%s", s.dbName, item.schema, item.table)

	// Ensure date directory exists.
	dateDirPath := filepath.Join(s.destinationDir, s.currentDateDir)
	if err := os.MkdirAll(dateDirPath, 0o755); err != nil {
		return fmt.Errorf("create date directory %s: %w", dateDirPath, err)
	}

	tw, err := s.getOrCreateWriter(key)
	if err != nil {
		return fmt.Errorf("get writer for %s: %w", key, err)
	}

	// Check if rotation is needed before writing.
	tw, err = s.checkAndRotate(tw, len(item.data))
	if err != nil {
		return fmt.Errorf("check and rotate for %s: %w", key, err)
	}

	writeStart := time.Now()
	if _, err := tw.file.Write(item.data); err != nil {
		return fmt.Errorf("write to segment: %w", err)
	}
	if s.metrics != nil {
		s.metrics.SinkWriteLatency.Observe(time.Since(writeStart).Seconds())
	}

	syncStart := time.Now()
	if err := tw.file.Sync(); err != nil {
		return fmt.Errorf("sync segment file: %w", err)
	}
	if s.metrics != nil {
		s.metrics.EventsWrittenTotal.Inc()
		s.metrics.BatchFlushDuration.Observe(time.Since(syncStart).Seconds())
	}

	tw.segmentInfo.CurrentSize += int64(len(item.data))

	if s.acker != nil {
		s.acker.Acknowledge(item.lsn)
	}
	s.state.FlushedLSN = item.lsn

	// Debounced state persistence.
	s.writesSinceStateSync++
	if s.writesSinceStateSync >= statePersistWriteInterval {
		if err := s.persistCurrentState(); err != nil {
			return fmt.Errorf("persist state: %w", err)
		}
		s.writesSinceStateSync = 0
	}

	return nil
}

// getOrCreateWriter returns the tableWriter for the given key, creating a new
// segment file if one doesn't exist yet.
func (s *LocalFileSink) getOrCreateWriter(key string) (*tableWriter, error) {
	if tw, ok := s.writers[key]; ok {
		return tw, nil
	}

	info, exists := s.state.Segments[key]
	if !exists {
		info = &SegmentInfo{
			Timestamp:   time.Now().Unix(),
			Segment:     1,
			CurrentSize: 0,
		}
		s.state.Segments[key] = info
	}

	fileName := segmentFileName(key, info)
	dateDirPath := filepath.Join(s.destinationDir, s.currentDateDir)
	filePath := filepath.Join(dateDirPath, fileName)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open segment file %s: %w", filePath, err)
	}

	tw := &tableWriter{
		file:        file,
		segmentInfo: info,
		key:         key,
	}

	s.writers[key] = tw
	s.logger.Info("opened new segment file", "key", key, "file", fileName)

	return tw, nil
}

// checkAndRotate checks if writing dataSize bytes would exceed maxFileSize.
// If so, it closes the current segment, increments the segment number, opens
// a new file, and persists the state. Returns the (possibly new) tableWriter.
func (s *LocalFileSink) checkAndRotate(tw *tableWriter, dataSize int) (*tableWriter, error) {
	if tw.segmentInfo.CurrentSize+int64(dataSize) <= s.maxFileSize {
		return tw, nil // no rotation needed
	}

	// Close current segment.
	if err := tw.file.Close(); err != nil {
		return nil, fmt.Errorf("close segment: %w", err)
	}

	// Advance to next segment.
	tw.segmentInfo.Segment++
	tw.segmentInfo.CurrentSize = 0

	// Open new segment file.
	fileName := segmentFileName(tw.key, tw.segmentInfo)
	dateDirPath := filepath.Join(s.destinationDir, s.currentDateDir)
	filePath := filepath.Join(dateDirPath, fileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open new segment: %w", err)
	}
	tw.file = file

	// Persist state immediately on rotation.
	if err := s.persistCurrentState(); err != nil {
		return nil, fmt.Errorf("persist state after rotation: %w", err)
	}

	s.logger.Info("rotated to new segment",
		"key", tw.key,
		"segment", tw.segmentInfo.Segment,
		"file", fileName,
	)

	return tw, nil
}

// segmentFileName generates the file name for a segment.
// Format: {db}.{schema}.{table}.{timestamp}.{segment}.jsonl
func segmentFileName(key string, info *SegmentInfo) string {
	return fmt.Sprintf("%s.%d.%04d.jsonl", key, info.Timestamp, info.Segment)
}

// persistCurrentState atomically writes the current segment state to disk.
func (s *LocalFileSink) persistCurrentState() error {
	dateDirPath := filepath.Join(s.destinationDir, s.currentDateDir)
	if err := os.MkdirAll(dateDirPath, 0o755); err != nil {
		return fmt.Errorf("ensure date dir for state: %w", err)
	}
	return persistSegmentState(dateDirPath, s.state)
}

// closeAllWriters closes all open segment file handles and persists state.
func (s *LocalFileSink) closeAllWriters() {
	for key, tw := range s.writers {
		if err := tw.file.Close(); err != nil {
			s.logger.Error("failed to close segment file", "key", key, "error", err)
		}
	}
	s.writers = make(map[string]*tableWriter)

	if err := s.persistCurrentState(); err != nil {
		s.logger.Error("failed to persist state on close", "error", err)
	}
	s.writesSinceStateSync = 0
}
