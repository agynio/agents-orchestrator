//go:build e2e

package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	tracingv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/tracing/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	tracingAvailable         bool
	tracingUnavailableReason string
)

func TestMain(m *testing.M) {
	tracingAvailable, tracingUnavailableReason = checkTracingAvailability()
	if !tracingAvailable {
		reason := strings.TrimSpace(tracingUnavailableReason)
		if reason == "" {
			reason = "unknown reason"
		}
		log.Printf("tracing e2e disabled: %s", reason)
	}
	os.Exit(m.Run())
}

func checkTracingAvailability() (bool, string) {
	addr := strings.TrimSpace(tracingAddr)
	if addr == "" {
		return false, "TRACING_ADDRESS is empty"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	traceID, err := runTraceCanary(ctx, addr)
	if err != nil {
		return false, fmt.Sprintf("export canary span: %v", err)
	}

	conn, err := dialGRPCForCheck(ctx, addr)
	if err != nil {
		return false, fmt.Sprintf("dial tracing %s: %v", addr, err)
	}
	defer conn.Close()

	queryClient := tracingv1.NewTracingServiceClient(conn)
	pollCtx, cancelPoll := context.WithTimeout(ctx, 20*time.Second)
	defer cancelPoll()

	err = pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		resp, err := queryClient.GetTrace(ctx, &tracingv1.GetTraceRequest{TraceId: traceID})
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("trace not found")
			}
			return fmt.Errorf("get trace: %w", err)
		}
		if len(flattenSpans(resp.GetResourceSpans())) == 0 {
			return fmt.Errorf("trace has no spans")
		}
		return nil
	})
	if err != nil {
		return false, fmt.Sprintf("tracing ingest check failed: %v", err)
	}

	return true, ""
}

func dialGRPCForCheck(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func runTraceCanary(ctx context.Context, addr string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "go", "run", "./tracecanary")
	cmd.Env = append(os.Environ(), fmt.Sprintf("TRACING_ADDRESS=%s", addr))
	output, err := cmd.CombinedOutput()
	if err != nil {
		reason := strings.TrimSpace(string(output))
		if reason == "" {
			reason = err.Error()
		}
		return nil, fmt.Errorf("trace canary failed: %s", reason)
	}
	traceHex := strings.TrimSpace(string(output))
	if traceHex == "" {
		return nil, fmt.Errorf("trace canary returned empty trace id")
	}
	traceID, err := hex.DecodeString(traceHex)
	if err != nil {
		return nil, fmt.Errorf("decode trace id %q: %w", traceHex, err)
	}
	return traceID, nil
}
