package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/001ajd/change-data-capture/internal/config"
	"github.com/001ajd/change-data-capture/internal/dispatcher"
	"github.com/001ajd/change-data-capture/internal/logger"
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
}

func NewStreamer(l logger.Logger, config config.Postgres, dispatcher dispatcher.Dispatcher, tracker *LSNTracker) *Streamer {
	return &Streamer{
		config:     config,
		dispatcher: dispatcher,
		parser:     newParser(),
		tracker:    tracker,
		logger:     l.With("module", "streamer"),
	}
}

func (s *Streamer) Run(ctx context.Context) error {
	s.logger.Info("connecting to postgres", "conn_string", s.config.ConnString)
	conn, err := pgconn.Connect(ctx, s.config.ConnString)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer conn.Close(ctx)

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

	return s.receiveLoop(ctx, conn, startLSN)
}

func (s *Streamer) receiveLoop(ctx context.Context, conn *pgconn.PgConn, lastLSN pglogrepl.LSN) error {
	statusInterval := standbyStatusInterval(s.config.StandbyStatusInterval)

	for {
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

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			replyRequested, serverLSN, err := parseKeepalive(copyData.Data[1:])
			if err != nil {
				s.logger.Error("failed to parse primary keepalive message", "error", err)
				return err
			}
			s.logger.Debug("primary keepalive message received", "server_lsn", serverLSN.String(), "reply_requested", replyRequested)
			if serverLSN > lastLSN {
				lastLSN = serverLSN
			}
			if replyRequested {
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

func parseKeepalive(data []byte) (bool, pglogrepl.LSN, error) {
	message, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		return false, 0, fmt.Errorf("parse primary keepalive message: %w", err)
	}
	return message.ReplyRequested, message.ServerWALEnd, nil
}
