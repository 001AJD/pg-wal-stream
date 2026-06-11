package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	// Replication Metrics
	CurrentReceivedLSN    prometheus.Gauge
	LastCommittedLSN      prometheus.Gauge
	ReplicationLagBytes   prometheus.Gauge
	ReplicationLagSeconds prometheus.Gauge
	WalReceiveRate        prometheus.Gauge
	ReconnectCount        prometheus.Counter
	KeepaliveLatency      prometheus.Histogram

	// Sink Metrics
	EventsWrittenTotal prometheus.Counter
	EventsFailedTotal  prometheus.Counter
	SinkWriteLatency   prometheus.Histogram
	BatchFlushDuration prometheus.Histogram

	// CDC Metrics
	TotalInserts   prometheus.Counter
	TotalUpdates   prometheus.Counter
	TotalDeletes   prometheus.Counter
	EventsPerTable *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	factory := promauto.With(reg)

	return &Metrics{
		CurrentReceivedLSN: factory.NewGauge(prometheus.GaugeOpts{
			Name: "pg_wal_stream_current_received_lsn",
			Help: "Current LSN received from Postgres",
		}),
		LastCommittedLSN: factory.NewGauge(prometheus.GaugeOpts{
			Name: "pg_wal_stream_last_committed_lsn",
			Help: "Last LSN committed to the sink",
		}),
		ReplicationLagBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "pg_wal_stream_replication_lag_bytes",
			Help: "Replication lag in bytes",
		}),
		ReplicationLagSeconds: factory.NewGauge(prometheus.GaugeOpts{
			Name: "pg_wal_stream_replication_lag_seconds",
			Help: "Replication lag in seconds",
		}),
		WalReceiveRate: factory.NewGauge(prometheus.GaugeOpts{
			Name: "pg_wal_stream_wal_receive_rate_bytes_per_second",
			Help: "WAL receive rate in bytes per second",
		}),
		ReconnectCount: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_reconnect_total",
			Help: "Total number of reconnections to Postgres",
		}),
		KeepaliveLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "pg_wal_stream_keepalive_latency_seconds",
			Help:    "Latency of keepalive messages",
			Buckets: prometheus.DefBuckets,
		}),

		EventsWrittenTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_sink_events_written_total",
			Help: "Total events written by the sink",
		}),
		EventsFailedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_sink_events_failed_total",
			Help: "Total events that failed to be written by the sink",
		}),
		SinkWriteLatency: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "pg_wal_stream_sink_write_latency_seconds",
			Help:    "Latency of sink write operations",
			Buckets: prometheus.DefBuckets,
		}),
		BatchFlushDuration: factory.NewHistogram(prometheus.HistogramOpts{
			Name:    "pg_wal_stream_sink_batch_flush_duration_seconds",
			Help:    "Duration of batch flush operations",
			Buckets: prometheus.DefBuckets,
		}),

		TotalInserts: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_cdc_inserts_total",
			Help: "Total number of insert events",
		}),
		TotalUpdates: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_cdc_updates_total",
			Help: "Total number of update events",
		}),
		TotalDeletes: factory.NewCounter(prometheus.CounterOpts{
			Name: "pg_wal_stream_cdc_deletes_total",
			Help: "Total number of delete events",
		}),
		EventsPerTable: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "pg_wal_stream_cdc_table_events_total",
			Help: "Total events per table and operation",
		}, []string{"schema", "table", "operation"}),
	}
}
