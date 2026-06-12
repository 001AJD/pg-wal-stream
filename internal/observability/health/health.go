package health

import (
	"context"
	"fmt"
	"net/http"
	"sync"
)

type Checker interface {
	Check(ctx context.Context) error
}

type CheckerFunc func(ctx context.Context) error

func (f CheckerFunc) Check(ctx context.Context) error {
	return f(ctx)
}

type Registry struct {
	mu                sync.RWMutex
	readinessCheckers map[string]Checker
	livenessCheckers  map[string]Checker
}

func NewRegistry() *Registry {
	return &Registry{
		readinessCheckers: make(map[string]Checker),
		livenessCheckers:  make(map[string]Checker),
	}
}

func (r *Registry) AddReadinessCheck(name string, c Checker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readinessCheckers[name] = c
}

func (r *Registry) AddLivenessCheck(name string, c Checker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.livenessCheckers[name] = c
}

func (r *Registry) HandleReady(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx := req.Context()
	for name, checker := range r.readinessCheckers {
		if err := checker.Check(ctx); err != nil {
			http.Error(w, fmt.Sprintf("readiness check failed: %s: %v", name, err), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (r *Registry) HandleLive(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ctx := req.Context()
	for name, checker := range r.livenessCheckers {
		if err := checker.Check(ctx); err != nil {
			http.Error(w, fmt.Sprintf("liveness check failed: %s: %v", name, err), http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
