//go:build e2e

package e2e

import (
	"testing"

	"google.golang.org/grpc"
)

func dialRunnerGRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	return dialGRPC(t, addr)
}
