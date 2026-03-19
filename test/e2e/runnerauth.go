//go:build e2e

package e2e

import (
	"testing"

	"github.com/agynio/agents-orchestrator/internal/runnerauth"
	"google.golang.org/grpc"
)

func dialRunnerGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	unaryInterceptor, streamInterceptor, err := runnerauth.NewClientInterceptors(runnerSharedSecret)
	if err != nil {
		t.Fatalf("create runner auth interceptors: %v", err)
	}
	return dialGRPC(t, addr,
		grpc.WithUnaryInterceptor(unaryInterceptor),
		grpc.WithStreamInterceptor(streamInterceptor),
	)
}
