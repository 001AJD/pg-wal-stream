package localfilesink

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/001ajd/change-data-capture/internal/cdc"
	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/logger"
)

func testConfig(destinationDir string) config.LocalFileSink {
	return config.LocalFileSink{
		DestinationDir: destinationDir,
		MaxFileSize:    200 * 1024 * 1024,
		DbName:         "domains",
	}
}

func TestLocalFileSinkWritesJSONLRecord(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	acker := &recordingAcker{}
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, acker, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"update","schema":"public","table":"users","lsn":"0/2","commit_lsn":"0/3","columns":{"name":"example.com","status":null}}` + "\n"),
		LSN:    "0/2",
		Schema: "public",
		Table:  "users",
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write event: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	// Find the written file inside the date directory.
	files := findJSONLFiles(t, destinationDir)
	if len(files) != 1 {
		t.Fatalf("jsonl files = %d, want 1", len(files))
	}

	records := readJSONLLines(t, files[0])
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}

	record := records[0]
	if record["operation"] != "update" {
		t.Fatalf("operation = %v, want update", record["operation"])
	}
	if record["schema"] != "public" || record["table"] != "users" {
		t.Fatalf("relation = %v.%v, want public.users", record["schema"], record["table"])
	}
	if got := acker.LSNs(); len(got) != 1 || got[0] != "0/2" {
		t.Fatalf("acked LSNs = %v, want [0/2]", got)
	}
}

func TestLocalFileSinkAppendsRecords(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","columns":{}}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write first event: %v", err)
	}
	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write second event: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	files := findJSONLFiles(t, destinationDir)
	if len(files) != 1 {
		t.Fatalf("jsonl files = %d, want 1", len(files))
	}

	records := readJSONLLines(t, files[0])
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
}

func TestLocalFileSinkReturnsErrorForInvalidDestination(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "destination")
	if err := os.WriteFile(filePath, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("create destination file: %v", err)
	}

	cfg := testConfig(filePath)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte("{}\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	_ = sink.Write(context.Background(), event)

	// Wait a bit for worker to fail.
	time.Sleep(10 * time.Millisecond)

	writeErr := sink.Write(context.Background(), event)
	if writeErr == nil {
		writeErr = sink.Close()
	}

	if writeErr == nil {
		t.Fatal("write/close error = nil, want error")
	}
}

func TestSegmentRotationOnMaxFileSize(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := config.LocalFileSink{
		DestinationDir: destinationDir,
		MaxFileSize:    100, // 100 bytes to trigger rotation quickly
		DbName:         "domains",
	}
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	// Each event is ~40 bytes. After 3 events (~120 bytes), we should
	// have rotated to a second segment.
	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","columns":{"id":1}}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	for i := 0; i < 5; i++ {
		if err := sink.Write(context.Background(), event); err != nil {
			t.Fatalf("write event %d: %v", i, err)
		}
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	files := findJSONLFiles(t, destinationDir)
	if len(files) < 2 {
		t.Fatalf("jsonl files = %d, want >= 2 (rotation should have occurred)", len(files))
	}

	// All files should follow the naming convention.
	for _, f := range files {
		name := filepath.Base(f)
		if !strings.HasPrefix(name, "domains.public.users.") || !strings.HasSuffix(name, ".jsonl") {
			t.Fatalf("unexpected file name: %s", name)
		}
	}
}

func TestSegmentFileNamingConvention(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert"}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "orders",
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write event: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}

	files := findJSONLFiles(t, destinationDir)
	if len(files) != 1 {
		t.Fatalf("jsonl files = %d, want 1", len(files))
	}

	name := filepath.Base(files[0])
	// Expected format: domains.public.orders.{timestamp}.0001.jsonl
	if !strings.HasPrefix(name, "domains.public.orders.") {
		t.Fatalf("file name %q does not start with 'domains.public.orders.'", name)
	}
	if !strings.HasSuffix(name, ".0001.jsonl") {
		t.Fatalf("file name %q does not end with '.0001.jsonl'", name)
	}

	// Verify it's inside a date directory.
	dateDir := filepath.Base(filepath.Dir(files[0]))
	today := time.Now().Format(dateDirFormat)
	if dateDir != today {
		t.Fatalf("date dir = %q, want %q", dateDir, today)
	}
}

func TestCrashRecoveryResumesLastSegment(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)

	// First run: write some events and close.
	sink1, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 1): %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","run":1}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	if err := sink1.Write(context.Background(), event); err != nil {
		t.Fatalf("write (run 1): %v", err)
	}
	if err := sink1.Close(); err != nil {
		t.Fatalf("close (run 1): %v", err)
	}

	files1 := findJSONLFiles(t, destinationDir)
	if len(files1) != 1 {
		t.Fatalf("run 1: jsonl files = %d, want 1", len(files1))
	}

	// Second run: recover and write more events.
	sink2, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 2): %v", err)
	}

	event2 := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","run":2}` + "\n"),
		LSN:    "0/2",
		Schema: "public",
		Table:  "users",
	}

	if err := sink2.Write(context.Background(), event2); err != nil {
		t.Fatalf("write (run 2): %v", err)
	}
	if err := sink2.Close(); err != nil {
		t.Fatalf("close (run 2): %v", err)
	}

	// Should still be one file (resumed same segment).
	files2 := findJSONLFiles(t, destinationDir)
	if len(files2) != 1 {
		t.Fatalf("run 2: jsonl files = %d, want 1 (should resume)", len(files2))
	}

	// File should have 2 records total.
	records := readJSONLLines(t, files2[0])
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
}

func TestCrashRecoveryWithStaleState(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := config.LocalFileSink{
		DestinationDir: destinationDir,
		MaxFileSize:    100, // small limit
		DbName:         "domains",
	}

	// First run: write events that bring the file close to the limit.
	sink1, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 1): %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","columns":{"id":1}}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	// Write enough to fill one segment.
	for i := 0; i < 3; i++ {
		if err := sink1.Write(context.Background(), event); err != nil {
			t.Fatalf("write (run 1): %v", err)
		}
	}
	if err := sink1.Close(); err != nil {
		t.Fatalf("close (run 1): %v", err)
	}

	// Second run: recovery should stat the file and use actual size.
	sink2, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 2): %v", err)
	}

	if err := sink2.Write(context.Background(), event); err != nil {
		t.Fatalf("write (run 2): %v", err)
	}
	if err := sink2.Close(); err != nil {
		t.Fatalf("close (run 2): %v", err)
	}

	// Should have created multiple segments.
	files := findJSONLFiles(t, destinationDir)
	if len(files) < 2 {
		t.Fatalf("jsonl files = %d, want >= 2", len(files))
	}
}

func TestMultipleTablesGetSeparateFiles(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	usersEvent := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","table":"users"}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}
	ordersEvent := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","table":"orders"}` + "\n"),
		LSN:    "0/2",
		Schema: "public",
		Table:  "orders",
	}

	if err := sink.Write(context.Background(), usersEvent); err != nil {
		t.Fatalf("write users: %v", err)
	}
	if err := sink.Write(context.Background(), ordersEvent); err != nil {
		t.Fatalf("write orders: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files := findJSONLFiles(t, destinationDir)
	if len(files) != 2 {
		t.Fatalf("jsonl files = %d, want 2 (one per table)", len(files))
	}

	hasUsers, hasOrders := false, false
	for _, f := range files {
		name := filepath.Base(f)
		if strings.HasPrefix(name, "domains.public.users.") {
			hasUsers = true
		}
		if strings.HasPrefix(name, "domains.public.orders.") {
			hasOrders = true
		}
	}
	if !hasUsers || !hasOrders {
		t.Fatalf("expected files for both users and orders, got: %v", files)
	}
}

func TestMaxFileSizeDefaultsTo200MiB(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := config.LocalFileSink{
		DestinationDir: destinationDir,
		MaxFileSize:    0, // should default
		DbName:         "domains",
	}
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer sink.Close()

	if sink.maxFileSize != defaultMaxFileSize {
		t.Fatalf("maxFileSize = %d, want %d", sink.maxFileSize, defaultMaxFileSize)
	}
}

func TestSegmentStateInsideDateDir(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert"}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	if err := sink.Write(context.Background(), event); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	today := time.Now().Format(dateDirFormat)
	statePath := filepath.Join(destinationDir, today, segmentStateFileName)
	if _, err := os.Stat(statePath); err != nil {
		t.Fatalf("segment state file not found at %s: %v", statePath, err)
	}

	// Verify state content.
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}

	var state SegmentState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}

	info, ok := state.Segments["domains.public.users"]
	if !ok {
		t.Fatal("segment state missing entry for domains.public.users")
	}
	if info.Segment != 1 {
		t.Fatalf("segment = %d, want 1", info.Segment)
	}
}

func TestCrashRecoveryUsesCheckAndRotate(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := config.LocalFileSink{
		DestinationDir: destinationDir,
		MaxFileSize:    80, // very small
		DbName:         "domains",
	}

	// First run: write just enough to fill the segment.
	sink1, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 1): %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"operation":"insert","columns":{"id":1}}` + "\n"),
		LSN:    "0/1",
		Schema: "public",
		Table:  "users",
	}

	// Write 2 events (~42 bytes each = ~84 bytes total, exceeding 80 byte limit)
	if err := sink1.Write(context.Background(), event); err != nil {
		t.Fatalf("write 1 (run 1): %v", err)
	}
	if err := sink1.Write(context.Background(), event); err != nil {
		t.Fatalf("write 2 (run 1): %v", err)
	}
	if err := sink1.Close(); err != nil {
		t.Fatalf("close (run 1): %v", err)
	}

	filesAfterRun1 := findJSONLFiles(t, destinationDir)

	// Second run: recovery should detect the full segment and rotate.
	sink2, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 2): %v", err)
	}

	if err := sink2.Write(context.Background(), event); err != nil {
		t.Fatalf("write (run 2): %v", err)
	}
	if err := sink2.Close(); err != nil {
		t.Fatalf("close (run 2): %v", err)
	}

	filesAfterRun2 := findJSONLFiles(t, destinationDir)
	if len(filesAfterRun2) <= len(filesAfterRun1) {
		t.Fatalf("expected more files after run 2 (rotation on recovery), got %d <= %d",
			len(filesAfterRun2), len(filesAfterRun1))
	}
}

func TestFlushedLSNPersistedInState(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)
	sink, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}

	events := []cdc.EncodedEvent{
		{Data: []byte(`{"op":"insert"}` + "\n"), LSN: "0/1", Schema: "public", Table: "users"},
		{Data: []byte(`{"op":"insert"}` + "\n"), LSN: "0/2", Schema: "public", Table: "users"},
		{Data: []byte(`{"op":"insert"}` + "\n"), LSN: "0/3", Schema: "public", Table: "users"},
	}

	for _, e := range events {
		if err := sink.Write(context.Background(), e); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Read the state file and verify the flushed LSN.
	today := time.Now().Format(dateDirFormat)
	statePath := filepath.Join(destinationDir, today, segmentStateFileName)
	data, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("read state: %v", err)
	}

	var state SegmentState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal state: %v", err)
	}

	if state.FlushedLSN != "0/3" {
		t.Fatalf("flushed LSN = %q, want %q", state.FlushedLSN, "0/3")
	}
}

func TestRecoverFlushedLSNFromPreviousRun(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")
	cfg := testConfig(destinationDir)

	// First run: write events.
	sink1, err := NewLocalFileSink(logger.NewNopLogger(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("new sink (run 1): %v", err)
	}

	event := cdc.EncodedEvent{
		Data:   []byte(`{"op":"insert"}` + "\n"),
		LSN:    "0/ABCD",
		Schema: "public",
		Table:  "users",
	}

	if err := sink1.Write(context.Background(), event); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Recover the flushed LSN.
	recovered, err := RecoverFlushedLSN(destinationDir)
	if err != nil {
		t.Fatalf("recover flushed LSN: %v", err)
	}
	if recovered != "0/ABCD" {
		t.Fatalf("recovered LSN = %q, want %q", recovered, "0/ABCD")
	}
}

func TestRecoverFlushedLSNReturnsEmptyWhenNoState(t *testing.T) {
	destinationDir := filepath.Join(t.TempDir(), "destination")

	recovered, err := RecoverFlushedLSN(destinationDir)
	if err != nil {
		t.Fatalf("recover flushed LSN: %v", err)
	}
	if recovered != "" {
		t.Fatalf("recovered LSN = %q, want empty", recovered)
	}
}

// --- Helpers ---

func readJSONLLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl file: %v", err)
	}
	defer file.Close()

	var records []map[string]any
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("unmarshal jsonl record: %v", err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl file: %v", err)
	}

	return records
}

func findJSONLFiles(t *testing.T, root string) []string {
	t.Helper()

	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".jsonl") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk destination dir: %v", err)
	}
	return files
}

type recordingAcker struct {
	mu   sync.Mutex
	lsns []string
}

func (a *recordingAcker) Acknowledge(lsn string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lsns = append(a.lsns, lsn)
}

func (a *recordingAcker) LSNs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	lsns := make([]string, len(a.lsns))
	copy(lsns, a.lsns)
	return lsns
}
