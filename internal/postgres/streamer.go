package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/logger"
	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/001ajd/change-data-capture/internal/observability/metrics"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Streamer struct {
	config     config.Postgres
	dispatcher dispatcher.Dispatcher
	parser     *parser
	tracker    *LSNTracker
	logger     logger.Logger
	metrics    *metrics.Metrics
	health     *health.Registry

	connMu sync.RWMutex
	conn   *pgconn.PgConn

	lastRateUpdate time.Time
	bytesReceived  uint64

	lastHeartbeat atomic.Pointer[time.Time]
}

func NewStreamer(l logger.Logger, config config.Postgres, dispatcher dispatcher.Dispatcher, tracker *LSNTracker, m *metrics.Metrics, h *health.Registry) *Streamer {
	now := time.Now()
	s := &Streamer{
		config:         config,
		dispatcher:     dispatcher,
		parser:         newParser(),
		tracker:        tracker,
		logger:         l.With("module", "streamer"),
		metrics:        m,
		health:         h,
		lastRateUpdate: now,
	}
	s.lastHeartbeat.Store(&now)

	if h != nil {
		h.AddReadinessCheck("postgres_replication", health.CheckerFunc(func(ctx context.Context) error {
			s.connMu.RLock()
			defer s.connMu.RUnlock()
			if s.conn == nil {
				return fmt.Errorf("postgres connection is nil")
			}
			// We avoid calling s.conn.Ping(ctx) here because the connection is busy
			// with the replication stream. Liveness check handles active health.
			return nil
		}))

		h.AddLivenessCheck("streamer_loop", health.CheckerFunc(func(ctx context.Context) error {
			last := s.lastHeartbeat.Load()
			if last == nil {
				return fmt.Errorf("no heartbeat recorded")
			}
			threshold := standbyStatusInterval(s.config.StandbyStatusInterval) * 3
			if time.Since(*last) > threshold {
				return fmt.Errorf("last heartbeat too old: %v", time.Since(*last))
			}
			return nil
		}))
	}

	return s
}

func (s *Streamer) Run(ctx context.Context) error {
	bo := newBackoff(s.config.ReconnectBaseDelay, s.config.ReconnectMaxDelay)
	attempts := 0

	for {
		err := s.connectAndStream(ctx)

		// Graceful shutdown: context was cancelled.
		if err == nil || errors.Is(err, context.Canceled) ||
			strings.Contains(err.Error(), "context canceled") {

			s.logger.Info("shutdown signal received, starting graceful shutdown")
			if closeErr := s.dispatcher.Close(); closeErr != nil {
				s.logger.Error("failed to close dispatcher", "error", closeErr)
			}
			finalLSN := s.tracker.GetFlushed()
			s.logger.Info("graceful shutdown complete", "final_lsn", finalLSN.String())
			return nil
		}

		// Non-retryable error: bail immediately.
		if !isRetryableError(err) {
			s.logger.Error("non-retryable error, shutting down", "error", err)
			return err
		}

		// Check attempt limit (0 = unlimited).
		attempts++
		if s.config.MaxReconnectAttempts > 0 &&
			attempts >= s.config.MaxReconnectAttempts {
			s.logger.Error("max reconnect attempts reached",
				"attempts", attempts, "error", err)
			return fmt.Errorf("max reconnect attempts (%d) reached: %w",
				s.config.MaxReconnectAttempts, err)
		}

		s.logger.Warn("connection lost, will reconnect",
			"error", err, "attempt", attempts)

		// Invalidate stale relation cache.
		s.parser.Reset()

		// Increment reconnect metric.
		if s.metrics != nil {
			s.metrics.ReconnectCount.Inc()
		}

		// Keep heartbeat alive during reconnection to avoid
		// false-positive liveness probe failures.
		now := time.Now()
		s.lastHeartbeat.Store(&now)

		// Wait with exponential backoff.
		if waitErr := bo.Wait(ctx); waitErr != nil {
			return nil // context cancelled during backoff — graceful exit
		}
	}
}

// connectAndStream establishes a single connection to Postgres, starts
// replication from the last flushed LSN, and runs the receive loop.
// It returns when the connection is lost or the context is cancelled.
func (s *Streamer) connectAndStream(ctx context.Context) error {
	s.logger.Info("connecting to postgres", "conn_string", s.config.ConnString)
	conn, err := pgconn.Connect(ctx, s.config.ConnString)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}

	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()

	defer func() {
		s.connMu.Lock()
		s.conn = nil
		s.connMu.Unlock()
		conn.Close(context.Background())
	}()

	s.logger.Info("pinging postgres")
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	// Resume from last flushed LSN — not the initial config StartLSN.
	// This ensures we pick up exactly where we left off after a reconnect.
	startLSN := s.tracker.GetFlushed()

	s.logger.Info("starting replication", "slot_name", s.config.SlotName, "start_lsn", startLSN.String())
	if err := pglogrepl.StartReplication(ctx, conn, s.config.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: s.pluginArgs(),
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	return s.receiveLoop(ctx, conn, startLSN)
}

// isRetryableError returns true for connection-level errors that warrant
// a reconnection attempt (as opposed to fatal configuration errors).
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Context cancellation is a graceful shutdown, not retryable.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// pgconn wraps connection-level failures; check for non-retryable PG errors.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "28": // invalid_authorization_specification
			return false
		case "3D": // invalid_catalog_name (wrong DB)
			return false
		}
	}
	// Everything else (network I/O, EOF, connection reset) is retryable.
	return true
}

func (s *Streamer) receiveLoop(ctx context.Context, conn *pgconn.PgConn, lastLSN pglogrepl.LSN) error {
	statusInterval := standbyStatusInterval(s.config.StandbyStatusInterval)
	nextStatusTime := time.Now().Add(statusInterval)

	for {
		now := time.Now()
		s.lastHeartbeat.Store(&now)

		if err := ctx.Err(); err != nil {
			return err
		}

		if now.After(nextStatusTime) || now.Equal(nextStatusTime) {
			if err := s.sendStandbyStatus(ctx, conn, lastLSN, s.tracker.GetFlushed(), false); err != nil {
				return err
			}
			nextStatusTime = time.Now().Add(statusInterval)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStatusTime)
		msg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if receiveCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
				// The status update will be sent at the top of the next loop iteration.
				continue
			}
			s.logger.Warn("replication connection error", "error", err, "last_received_lsn", lastLSN.String(), "last_flushed_lsn", s.tracker.GetFlushed().String())
			return fmt.Errorf("receive replication message: %w", err)
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		if len(copyData.Data) == 0 {
			continue
		}

		if s.metrics != nil {
			s.bytesReceived += uint64(len(copyData.Data))
			now := time.Now()
			if now.Sub(s.lastRateUpdate) >= time.Second {
				duration := now.Sub(s.lastRateUpdate).Seconds()
				rate := float64(s.bytesReceived) / duration
				s.metrics.WalReceiveRate.Set(rate)
				s.bytesReceived = 0
				s.lastRateUpdate = now
			}
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				s.logger.Error("failed to parse primary keepalive message", "error", err)
				return err
			}
			s.logger.Debug("primary keepalive message received", "server_lsn", pkm.ServerWALEnd.String(), "reply_requested", pkm.ReplyRequested)

			if s.metrics != nil {
				s.metrics.CurrentReceivedLSN.Set(float64(uint64(pkm.ServerWALEnd)))
				lagBytes := uint64(0)
				if pkm.ServerWALEnd > lastLSN {
					lagBytes = uint64(pkm.ServerWALEnd - lastLSN)
				}
				s.metrics.ReplicationLagBytes.Set(float64(lagBytes))
				s.metrics.ReplicationLagSeconds.Set(time.Since(pkm.ServerTime).Seconds())
			}

			if pkm.ServerWALEnd > lastLSN {
				lastLSN = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				if err := s.sendStandbyStatus(ctx, conn, lastLSN, s.tracker.GetFlushed(), true); err != nil {
					return err
				}
				nextStatusTime = time.Now().Add(statusInterval)
			}
		case pglogrepl.XLogDataByteID:
			nextLSN, err := s.handleXLogData(ctx, copyData.Data[1:])
			if err != nil {
				return err
			}
			if nextLSN > lastLSN {
				lastLSN = nextLSN
			}
		}
	}
}

func (s *Streamer) handleXLogData(ctx context.Context, data []byte) (pglogrepl.LSN, error) {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		return 0, fmt.Errorf("parse xlog data: %w", err)
	}

	if s.metrics != nil {
		s.metrics.CurrentReceivedLSN.Set(float64(uint64(xld.WALStart)))
		s.metrics.ReplicationLagSeconds.Set(time.Since(xld.ServerTime).Seconds())
	}

	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return 0, fmt.Errorf("parse logical replication message: %w", err)
	}

	events, err := s.parser.parse(logicalMsg, xld.WALStart.String())
	if err != nil {
		return 0, err
	}

	for _, event := range events {
		if err := s.dispatcher.Dispatch(ctx, event); err != nil {
			return 0, fmt.Errorf("dispatch cdc event: %w", err)
		}
	}

	return pglogrepl.LSN(uint64(xld.WALStart) + uint64(len(xld.WALData))), nil
}

// sendStandbyStatus reports replication progress to PostgreSQL.
// writeLSN is the latest LSN received from the WAL stream.
// flushLSN is the latest LSN durably written by the sink (acknowledged).
// Separating these two tells PostgreSQL the client is alive and receiving
// data even when the sink is lagging behind, preventing wal_sender_timeout.
func (s *Streamer) sendStandbyStatus(ctx context.Context, conn *pgconn.PgConn, writeLSN, flushLSN pglogrepl.LSN, replyRequested bool) error {
	s.logger.Debug("sending standby status update", "write_lsn", writeLSN.String(), "flush_lsn", flushLSN.String(), "reply_requested", replyRequested)
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: writeLSN,
		WALFlushPosition: flushLSN,
		WALApplyPosition: flushLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	}); err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	s.logger.Info("standby status sent successfully to postgres!")
	return nil
}

func (s *Streamer) pluginArgs() []string {
	args := []string{"proto_version '1'"}
	if len(s.config.PublicationNames) > 0 {
		args = append(args, fmt.Sprintf("publication_names '%s'", strings.Join(s.config.PublicationNames, ",")))
	}
	return args
}
