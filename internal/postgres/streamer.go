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
	s.logger.Info("connecting to postgres", "conn_string", s.config.ConnString)
	conn, err := pgconn.Connect(ctx, s.config.ConnString)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(context.Background())

	s.connMu.Lock()
	s.conn = conn
	s.connMu.Unlock()

	s.logger.Info("pinging postgres")
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	startLSN, err := pglogrepl.ParseLSN(s.config.StartLSN)
	if err != nil {
		return fmt.Errorf("parse start lsn: %w", err)
	}

	s.logger.Info("starting replication", "slot_name", s.config.SlotName, "start_lsn", startLSN.String())
	if err := pglogrepl.StartReplication(ctx, conn, s.config.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: s.pluginArgs(),
	}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}

	err = s.receiveLoop(ctx, conn, startLSN)
	if err != nil && (errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled")) {
		s.logger.Info("shutdown signal received, starting graceful shutdown")

		if err := s.dispatcher.Close(); err != nil {
			s.logger.Error("failed to close dispatcher", "error", err)
		}

		finalLSN := s.tracker.GetFlushed()
		s.logger.Info("sending final standby status update", "lsn", finalLSN.String())
		if err := s.sendStandbyStatus(context.Background(), conn, finalLSN, true); err != nil {
			s.logger.Error("failed to send final standby status", "error", err)
		}

		s.logger.Info("graceful shutdown complete")
		return nil
	}

	return err
}

func (s *Streamer) receiveLoop(ctx context.Context, conn *pgconn.PgConn, lastLSN pglogrepl.LSN) error {
	statusInterval := standbyStatusInterval(s.config.StandbyStatusInterval)

	for {
		now := time.Now()
		s.lastHeartbeat.Store(&now)

		if err := ctx.Err(); err != nil {
			return err
		}

		receiveCtx, cancel := context.WithDeadline(ctx, time.Now().Add(statusInterval))
		msg, err := conn.ReceiveMessage(receiveCtx)
		cancel()

		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if err := s.sendStandbyStatus(ctx, conn, s.tracker.GetFlushed(), false); err != nil {
					return err
				}
				continue
			}
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
				if err := s.sendStandbyStatus(ctx, conn, s.tracker.GetFlushed(), true); err != nil {
					return err
				}
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

func (s *Streamer) sendStandbyStatus(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN, replyRequested bool) error {
	s.logger.Debug("sending standby status update", "lsn", lsn.String(), "reply_requested", replyRequested)
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	}); err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	return nil
}

func (s *Streamer) pluginArgs() []string {
	args := []string{"proto_version '1'"}
	if len(s.config.PublicationNames) > 0 {
		args = append(args, fmt.Sprintf("publication_names '%s'", strings.Join(s.config.PublicationNames, ",")))
	}
	return args
}
