package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryableError_Nil(t *testing.T) {
	if isRetryableError(nil) {
		t.Error("nil error should not be retryable")
	}
}

func TestIsRetryableError_ContextCanceled(t *testing.T) {
	if isRetryableError(context.Canceled) {
		t.Error("context.Canceled should not be retryable")
	}
}

func TestIsRetryableError_WrappedContextCanceled(t *testing.T) {
	err := fmt.Errorf("receive message: %w", context.Canceled)
	if isRetryableError(err) {
		t.Error("wrapped context.Canceled should not be retryable")
	}
}

func TestIsRetryableError_AuthFailure(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "28P01"} // invalid_password
	if isRetryableError(pgErr) {
		t.Error("auth failure (28P01) should not be retryable")
	}
}

func TestIsRetryableError_InvalidCatalog(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "3D000"} // invalid_catalog_name
	if isRetryableError(pgErr) {
		t.Error("invalid catalog (3D000) should not be retryable")
	}
}

func TestIsRetryableError_NetworkError(t *testing.T) {
	err := errors.New("read tcp: connection reset by peer")
	if !isRetryableError(err) {
		t.Error("network error should be retryable")
	}
}

func TestIsRetryableError_IOError(t *testing.T) {
	err := fmt.Errorf("receive replication message: %w", errors.New("EOF"))
	if !isRetryableError(err) {
		t.Error("EOF error should be retryable")
	}
}

func TestIsRetryableError_RetryablePgError(t *testing.T) {
	// Class 08 = connection_exception — should be retryable
	pgErr := &pgconn.PgError{Code: "08006"} // connection_failure
	if !isRetryableError(pgErr) {
		t.Error("connection_failure (08006) should be retryable")
	}
}

func TestParserReset(t *testing.T) {
	p := newParser()

	// Simulate cached relation messages.
	p.relations[1] = nil
	p.relations[2] = nil

	if len(p.relations) != 2 {
		t.Fatalf("expected 2 relations, got %d", len(p.relations))
	}

	p.Reset()

	if len(p.relations) != 0 {
		t.Errorf("expected 0 relations after Reset(), got %d", len(p.relations))
	}
}

func TestDeadlineDetectionWithWrappedError(t *testing.T) {
	// Simulate pgconn wrapping the deadline error in a way that
	// errors.Is(err, context.DeadlineExceeded) returns false.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	time.Sleep(5 * time.Millisecond) // let the deadline expire

	wrappedErr := fmt.Errorf("receive message failed: %w",
		fmt.Errorf("context deadline exceeded"))

	// Old check — fails (this is the bug).
	if errors.Is(wrappedErr, context.DeadlineExceeded) {
		t.Fatal("double-wrapped string error should NOT match errors.Is")
	}

	// New check — succeeds.
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatal("expired context should report DeadlineExceeded")
	}
}
