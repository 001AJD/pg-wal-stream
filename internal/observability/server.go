package observability

import (
	"context"
	"net/http"

	"github.com/001ajd/change-data-capture/internal/observability/health"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	httpServer *http.Server
}

func NewServer(addr string, healthRegistry *health.Registry) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/ready", healthRegistry.HandleReady)
	mux.HandleFunc("/health/live", healthRegistry.HandleLive)

	return &Server{
		httpServer: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpServer.Shutdown(ctx)
}
