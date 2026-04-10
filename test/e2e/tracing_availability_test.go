//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	tracingv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/tracing/v1"
	"github.com/google/uuid"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
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

	conn, err := dialGRPCForCheck(ctx, addr)
	if err != nil {
		return false, fmt.Sprintf("dial tracing %s: %v", addr, err)
	}
	defer conn.Close()

	traceID, err := randomBytes(16)
	if err != nil {
		return false, fmt.Sprintf("generate trace id: %v", err)
	}
	spanID, err := randomBytes(8)
	if err != nil {
		return false, fmt.Sprintf("generate span id: %v", err)
	}

	threadID := uuid.NewString()
	now := time.Now()
	exportReq := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key: "agyn.thread.id",
							Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{StringValue: threadID},
							},
						},
					},
				},
				ScopeSpans: []*tracev1.ScopeSpans{
					{
						Spans: []*tracev1.Span{
							{
								TraceId:           traceID,
								SpanId:            spanID,
								Name:              "e2e.tracing.canary",
								Kind:              tracev1.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: uint64(now.UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(5 * time.Millisecond).UnixNano()),
							},
						},
					},
				},
			},
		},
	}

	exportCtx, cancelExport := context.WithTimeout(ctx, 10*time.Second)
	_, err = collectortracev1.NewTraceServiceClient(conn).Export(exportCtx, exportReq)
	cancelExport()
	if err != nil {
		return false, fmt.Sprintf("export canary span: %v", err)
	}

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

func randomBytes(size int) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}
