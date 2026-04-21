//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
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
	e2eSkipReason            string
	tracingAvailable         bool
	tracingUnavailableReason string
	tracingSkipReason        string
)

func TestMain(m *testing.M) {
	e2eSkipReason = skipE2EReason()
	if e2eSkipReason != "" {
		log.Printf("e2e skipped: %s", e2eSkipReason)
		os.Exit(0)
	}

	tracingSkipReason = skipTracingReason()
	if tracingSkipReason != "" {
		log.Printf("tracing e2e skipped: %s", tracingSkipReason)
		os.Exit(m.Run())
	}

	tracingAvailable, tracingUnavailableReason = checkTracingAvailability()
	if !tracingAvailable {
		reason := strings.TrimSpace(tracingUnavailableReason)
		if reason == "" {
			reason = "unknown reason"
		}
		log.Printf("tracing e2e unavailable: %s", reason)
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
		if isCanaryAuthFailure(err) {
			ok, reason := checkTracingQueryConnectivity(ctx, addr)
			return ok, reason
		}
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

func checkTracingQueryConnectivity(ctx context.Context, addr string) (bool, string) {
	conn, err := dialGRPCForCheck(ctx, addr)
	if err != nil {
		return false, fmt.Sprintf("dial tracing %s: %v", addr, err)
	}
	defer conn.Close()

	traceID, err := randomTraceID()
	if err != nil {
		return false, err.Error()
	}
	queryClient := tracingv1.NewTracingServiceClient(conn)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	_, err = queryClient.GetTrace(queryCtx, &tracingv1.GetTraceRequest{TraceId: traceID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return true, ""
		}
		return false, fmt.Sprintf("get trace: %v", err)
	}

	return true, ""
}

func dialGRPCForCheck(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func runTraceCanary(ctx context.Context, addr string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "go", "run", "./tracecanary")
	cmd.Env = append(os.Environ(), fmt.Sprintf("TRACING_ADDRESS=%s", addr))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		reason := strings.TrimSpace(stderr.String())
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

func isCanaryAuthFailure(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "source identity missing") || strings.Contains(message, "unauthenticated")
}

func randomTraceID() ([]byte, error) {
	traceID := make([]byte, 16)
	if _, err := rand.Read(traceID); err != nil {
		return nil, fmt.Errorf("generate trace id: %w", err)
	}
	return traceID, nil
}

func skipTracingReason() string {
	if value, ok := os.LookupEnv("SKIP_TRACING_E2E"); ok {
		if shouldSkipTracing(value) {
			trimmed := strings.TrimSpace(value)
			if trimmed == "" {
				return "SKIP_TRACING_E2E set"
			}
			return fmt.Sprintf("SKIP_TRACING_E2E=%s", trimmed)
		}
	}
	return ""
}

func skipE2EReason() string {
	if strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) == "" {
		return "KUBERNETES_SERVICE_HOST not set"
	}
	serviceAccountPaths := []string{
		"/var/run/secrets/kubernetes.io/serviceaccount/token",
		"/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
		"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
	}
	for _, path := range serviceAccountPaths {
		if _, err := os.Stat(path); err != nil {
			return fmt.Sprintf("service account file missing: %s", path)
		}
	}
	return ""
}

func shouldSkipTracing(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return true
	}
	parsed, err := strconv.ParseBool(trimmed)
	if err != nil {
		return true
	}
	return parsed
}
