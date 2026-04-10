package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dialTimeout   = 10 * time.Second
	exportTimeout = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	addr := strings.TrimSpace(os.Getenv("TRACING_ADDRESS"))
	if addr == "" {
		return fmt.Errorf("TRACING_ADDRESS is empty")
	}
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("dial tracing %s: %w", addr, err)
	}
	defer conn.Close()

	traceID, err := randomBytes(16)
	if err != nil {
		return fmt.Errorf("generate trace id: %w", err)
	}
	spanID, err := randomBytes(8)
	if err != nil {
		return fmt.Errorf("generate span id: %w", err)
	}
	threadID, err := randomBytes(8)
	if err != nil {
		return fmt.Errorf("generate thread id: %w", err)
	}
	threadIDHex := hex.EncodeToString(threadID)

	now := time.Now()
	exportReq := &collectortracev1.ExportTraceServiceRequest{
		ResourceSpans: []*tracev1.ResourceSpans{
			{
				Resource: &resourcev1.Resource{
					Attributes: []*commonv1.KeyValue{
						{
							Key: "agyn.thread.id",
							Value: &commonv1.AnyValue{
								Value: &commonv1.AnyValue_StringValue{StringValue: threadIDHex},
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

	exportCtx, cancelExport := context.WithTimeout(context.Background(), exportTimeout)
	defer cancelExport()
	_, err = collectortracev1.NewTraceServiceClient(conn).Export(exportCtx, exportReq)
	if err != nil {
		return fmt.Errorf("export canary span: %w", err)
	}

	fmt.Fprint(os.Stdout, hex.EncodeToString(traceID))
	return nil
}

func randomBytes(size int) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}
