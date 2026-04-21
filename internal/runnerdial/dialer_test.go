package runnerdial

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type stubZitiDialer struct{}

func (stubZitiDialer) DialContext(context.Context, string) (edge.Conn, error) {
	return nil, errors.New("unused")
}

func (stubZitiDialer) NotifyAuthFailure(context.Context) {}

func TestDialerCachesConnections(t *testing.T) {
	ctx := context.Background()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
	})

	var conns []*grpc.ClientConn
	dialCalls := 0
	dialer := NewDialer(stubZitiDialer{})
	dialer.dialFunc = func(ctx context.Context, runnerID string) (*grpc.ClientConn, error) {
		dialCalls++
		conn, err := grpc.NewClient(
			"passthrough:///test",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.Dial()
			}),
		)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
		return conn, nil
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	if _, err := dialer.Dial(ctx, "runner-a"); err != nil {
		t.Fatalf("dial runner-a: %v", err)
	}
	firstConn := dialer.conns["runner-a"]
	if firstConn == nil {
		t.Fatal("expected cached connection")
	}
	if _, err := dialer.Dial(ctx, "runner-a"); err != nil {
		t.Fatalf("dial runner-a again: %v", err)
	}
	secondConn := dialer.conns["runner-a"]
	if dialCalls != 1 {
		t.Fatalf("expected 1 dial call, got %d", dialCalls)
	}
	if firstConn != secondConn {
		t.Fatal("expected cached connection to be reused")
	}
}

func TestDialerEvictsBadConnections(t *testing.T) {
	ctx := context.Background()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
	})

	var conns []*grpc.ClientConn
	dialCalls := 0
	dialer := NewDialer(stubZitiDialer{})
	dialer.dialFunc = func(ctx context.Context, runnerID string) (*grpc.ClientConn, error) {
		dialCalls++
		conn, err := grpc.NewClient(
			"passthrough:///test",
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.Dial()
			}),
		)
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
		return conn, nil
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	})

	if _, err := dialer.Dial(ctx, "runner-a"); err != nil {
		t.Fatalf("dial runner-a: %v", err)
	}
	firstConn := dialer.conns["runner-a"]
	if firstConn == nil {
		t.Fatal("expected cached connection")
	}
	if err := firstConn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	if _, err := dialer.Dial(ctx, "runner-a"); err != nil {
		t.Fatalf("dial runner-a again: %v", err)
	}
	secondConn := dialer.conns["runner-a"]
	if dialCalls != 2 {
		t.Fatalf("expected 2 dial calls, got %d", dialCalls)
	}
	if firstConn == secondConn {
		t.Fatal("expected new connection after eviction")
	}
}

func TestDialerErrorsOnMissingRunnerID(t *testing.T) {
	dialer := NewDialer(stubZitiDialer{})
	if _, err := dialer.Dial(context.Background(), ""); err == nil {
		t.Fatal("expected error for missing runner id")
	}
}

func TestIsNoTerminators(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil",
			err:      nil,
			expected: false,
		},
		{
			name:     "has no terminators",
			err:      errors.New("service runner-a has no terminators"),
			expected: true,
		},
		{
			name:     "no terminators",
			err:      errors.New("no terminators available"),
			expected: true,
		},
		{
			name:     "other",
			err:      errors.New("other"),
			expected: false,
		},
	}

	for _, tt := range tests {
		if got := IsNoTerminators(tt.err); got != tt.expected {
			t.Fatalf("%s: expected %v, got %v", tt.name, tt.expected, got)
		}
	}
}

func TestIsAuthFailure(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil",
			err:      nil,
			expected: false,
		},
		{
			name:     "invalid auth",
			err:      errors.New("INVALID_AUTH: session expired"),
			expected: true,
		},
		{
			name:     "no apiSession",
			err:      errors.New("no apiSession for identity"),
			expected: true,
		},
		{
			name:     "other",
			err:      errors.New("other"),
			expected: false,
		},
	}

	for _, tt := range tests {
		if got := isAuthFailure(tt.err); got != tt.expected {
			t.Fatalf("%s: expected %v, got %v", tt.name, tt.expected, got)
		}
	}
}

func TestDialZitiWithRetryAuthFailureWaits(t *testing.T) {
	dialer := &authFailureDialer{
		dialErrors:    []error{errors.New("INVALID_AUTH: session expired"), nil},
		notifyStarted: make(chan struct{}, 1),
		notifyRelease: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := dialZitiWithRetry(ctx, dialer, "service-a")
		errCh <- err
	}()

	select {
	case <-dialer.notifyStarted:
	case <-time.After(500 * time.Millisecond):
		close(dialer.notifyRelease)
		t.Fatal("expected NotifyAuthFailure call")
	}
	if dialer.dialCalls.Load() != 1 {
		close(dialer.notifyRelease)
		t.Fatalf("expected 1 dial before release, got %d", dialer.dialCalls.Load())
	}
	close(dialer.notifyRelease)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("dialZitiWithRetry: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected dialZitiWithRetry to finish")
	}

	if dialer.notifyCalls.Load() != 1 {
		t.Fatalf("expected 1 notify call, got %d", dialer.notifyCalls.Load())
	}
	if dialer.dialCalls.Load() != 2 {
		t.Fatalf("expected 2 dial attempts, got %d", dialer.dialCalls.Load())
	}
}

func TestDialZitiWithRetryStopsOnNoTerminators(t *testing.T) {
	dialer := &noTerminatorsDialer{dialError: errors.New("service runner-a has no terminators")}

	_, err := dialZitiWithRetry(context.Background(), dialer, "service-a")
	if err == nil {
		t.Fatal("expected error")
	}
	if dialer.dialCalls.Load() != 1 {
		t.Fatalf("expected 1 dial attempt, got %d", dialer.dialCalls.Load())
	}
	if dialer.notifyCalls.Load() != 0 {
		t.Fatalf("expected 0 notify calls, got %d", dialer.notifyCalls.Load())
	}
}

type authFailureDialer struct {
	dialErrors    []error
	dialCalls     atomic.Int32
	notifyCalls   atomic.Int32
	notifyStarted chan struct{}
	notifyRelease chan struct{}
}

func (a *authFailureDialer) DialContext(context.Context, string) (edge.Conn, error) {
	a.dialCalls.Add(1)
	if len(a.dialErrors) == 0 {
		return nil, nil
	}
	err := a.dialErrors[0]
	a.dialErrors = a.dialErrors[1:]
	return nil, err
}

func (a *authFailureDialer) NotifyAuthFailure(ctx context.Context) {
	a.notifyCalls.Add(1)
	if a.notifyStarted != nil {
		a.notifyStarted <- struct{}{}
	}
	select {
	case <-ctx.Done():
		return
	case <-a.notifyRelease:
	}
}

type noTerminatorsDialer struct {
	dialCalls   atomic.Int32
	notifyCalls atomic.Int32
	dialError   error
}

func (n *noTerminatorsDialer) DialContext(context.Context, string) (edge.Conn, error) {
	n.dialCalls.Add(1)
	return nil, n.dialError
}

func (n *noTerminatorsDialer) NotifyAuthFailure(context.Context) {
	n.notifyCalls.Add(1)
}
