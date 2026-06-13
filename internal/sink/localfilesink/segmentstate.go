package localfilesink

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	segmentStateFileName = ".segment_state.json"
	dateDirFormat        = "02-01-2006"
)

// SegmentInfo tracks the current segment for a single table stream.
type SegmentInfo struct {
	Timestamp   int64 `json:"timestamp"`    // unix timestamp when the stream was created
	Segment     int   `json:"segment"`      // current segment number (1-based)
	CurrentSize int64 `json:"current_size"` // bytes written to current segment
}

// SegmentState is the top-level persisted state.
type SegmentState struct {
	FlushedLSN string                  `json:"flushed_lsn"` // last durably acked LSN (e.g. "0/D7DE228")
	Segments   map[string]*SegmentInfo `json:"segments"`     // key: "db.schema.table"
}

// newSegmentState returns an empty SegmentState ready for use.
func newSegmentState() *SegmentState {
	return &SegmentState{
		Segments: make(map[string]*SegmentInfo),
	}
}

// loadSegmentState reads and parses the segment state from a date directory.
// Returns nil state and nil error if the file does not exist.
func loadSegmentState(dateDir string) (*SegmentState, error) {
	path := filepath.Join(dateDir, segmentStateFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read segment state: %w", err)
	}

	var state SegmentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("unmarshal segment state: %w", err)
	}

	if state.Segments == nil {
		state.Segments = make(map[string]*SegmentInfo)
	}

	return &state, nil
}

// persistSegmentState atomically writes the segment state to the date directory
// using write-to-temp-then-rename to prevent corruption on crash.
func persistSegmentState(dateDir string, state *SegmentState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal segment state: %w", err)
	}

	path := filepath.Join(dateDir, segmentStateFileName)
	tmpPath := path + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("write temp segment state: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename segment state: %w", err)
	}

	return nil
}

// findMostRecentDateDir finds the most recent date-based subdirectory inside
// destinationDir. Returns ("", nil) if no date directories exist.
func findMostRecentDateDir(destinationDir string) (string, error) {
	entries, err := os.ReadDir(destinationDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read destination dir: %w", err)
	}

	var dateDirs []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Validate that the directory name matches our date format.
		if _, err := time.Parse(dateDirFormat, entry.Name()); err == nil {
			dateDirs = append(dateDirs, entry.Name())
		}
	}

	if len(dateDirs) == 0 {
		return "", nil
	}

	// Sort lexicographically in dd-MM-yyyy format won't sort chronologically,
	// so parse and sort by actual time.
	sort.Slice(dateDirs, func(i, j int) bool {
		ti, _ := time.Parse(dateDirFormat, dateDirs[i])
		tj, _ := time.Parse(dateDirFormat, dateDirs[j])
		return ti.Before(tj)
	})

	return dateDirs[len(dateDirs)-1], nil
}

// todayDateDir returns today's date formatted as a directory name.
func todayDateDir() string {
	return time.Now().Format(dateDirFormat)
}

// RecoverFlushedLSN reads the flushed LSN from the most recent date directory's
// segment state file. Returns "" if no state exists or no LSN was persisted.
// This is intended to be called before the sink is created, so the caller can
// seed the LSN tracker with the recovered value.
func RecoverFlushedLSN(destinationDir string) (string, error) {
	recentDir, err := findMostRecentDateDir(destinationDir)
	if err != nil {
		return "", fmt.Errorf("find recent date dir: %w", err)
	}
	if recentDir == "" {
		return "", nil
	}

	dateDirPath := filepath.Join(destinationDir, recentDir)
	state, err := loadSegmentState(dateDirPath)
	if err != nil {
		return "", fmt.Errorf("load segment state: %w", err)
	}
	if state == nil {
		return "", nil
	}

	return state.FlushedLSN, nil
}
