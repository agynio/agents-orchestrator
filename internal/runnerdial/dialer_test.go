package runnerdial

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/openziti/sdk-golang/ziti/edge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type stubZitiDialer struct{}

func (stubZitiDialer) DialContext(context.Context, string) (edge.Conn, error) {
	return nil, errors.New("unused")
}

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
