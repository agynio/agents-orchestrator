//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	tracingv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/tracing/v1"
	commonv1 "github.com/agynio/agents-orchestrator/.gen/go/opentelemetry/proto/common/v1"
	tracev1 "github.com/agynio/agents-orchestrator/.gen/go/opentelemetry/proto/trace/v1"
)

func newTracingClient(t *testing.T) tracingv1.TracingServiceClient {
	t.Helper()
	conn := dialGRPC(t, tracingAddr)
	return tracingv1.NewTracingServiceClient(conn)
}

func discoverTraceID(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	threadID string,
	startTimeMinNs uint64,
) []byte {
	t.Helper()

	pollCtx, cancel := context.WithTimeout(ctx, tracingDiscoverTimeout)
	defer cancel()

	var traceID []byte
	err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		resp, err := client.ListSpans(ctx, &tracingv1.ListSpansRequest{
			Filter: &tracingv1.SpanFilter{
				StartTimeMin: startTimeMinNs,
				Names:        []string{"invocation.message"},
			},
			PageSize: 100,
		})
		if err != nil {
			return fmt.Errorf("list spans: %w", err)
		}
		for _, resourceSpan := range resp.GetResourceSpans() {
			if !resourceHasThreadID(resourceSpan, threadID) {
				continue
			}
			for _, span := range spansFromResource(resourceSpan) {
				if len(span.GetTraceId()) == 0 {
					continue
				}
				traceID = span.GetTraceId()
				return nil
			}
		}
		return fmt.Errorf("trace id not found")
	})
	if err != nil {
		t.Fatalf("discover trace id: %v", err)
	}
	return traceID
}

func assertTraceSummary(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	traceID []byte,
	expectedCounts map[string]int64,
	expectedTotal int64,
) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, tracingSummaryTimeout)
	defer cancel()

	err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		resp, err := client.GetTraceSummary(ctx, &tracingv1.GetTraceSummaryRequest{TraceId: traceID})
		if err != nil {
			return fmt.Errorf("get trace summary: %w", err)
		}
		counts := resp.GetCountsByName()
		for name, expected := range expectedCounts {
			if counts[name] != expected {
				return fmt.Errorf("expected %s count %d, got %d", name, expected, counts[name])
			}
		}
		if resp.GetTotalSpans() != expectedTotal {
			return fmt.Errorf("expected total spans %d, got %d", expectedTotal, resp.GetTotalSpans())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("trace summary: %v", err)
	}
}

func assertSpanAttributes(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	traceID []byte,
	spanName string,
	expectedAttrs map[string]string,
) map[string]string {
	t.Helper()

	spans := traceSpans(t, ctx, client, traceID)
	for _, span := range spans {
		if span.GetName() != spanName {
			continue
		}
		attrs := attributesToMap(span.GetAttributes())
		for key, expected := range expectedAttrs {
			value, ok := attrs[key]
			if !ok {
				t.Fatalf("span %s missing attribute %s", spanName, key)
			}
			if value != expected {
				t.Fatalf("span %s attribute %s expected %q, got %q", spanName, key, expected, value)
			}
		}
		return attrs
	}
	t.Fatalf("span %s not found", spanName)
	return nil
}

func traceSpans(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	traceID []byte,
) []*tracev1.Span {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	resp, err := client.GetTrace(callCtx, &tracingv1.GetTraceRequest{TraceId: traceID})
	if err != nil {
		t.Fatalf("get trace: %v", err)
	}
	return flattenSpans(resp.GetResourceSpans())
}

func flattenSpans(resourceSpans []*tracev1.ResourceSpans) []*tracev1.Span {
	spans := make([]*tracev1.Span, 0, len(resourceSpans))
	for _, resourceSpan := range resourceSpans {
		spans = append(spans, spansFromResource(resourceSpan)...)
	}
	return spans
}

func spansFromResource(resourceSpan *tracev1.ResourceSpans) []*tracev1.Span {
	if resourceSpan == nil {
		return nil
	}
	spans := make([]*tracev1.Span, 0, len(resourceSpan.GetScopeSpans()))
	for _, scopeSpan := range resourceSpan.GetScopeSpans() {
		spans = append(spans, scopeSpan.GetSpans()...)
	}
	return spans
}

func resourceHasThreadID(resourceSpans *tracev1.ResourceSpans, threadID string) bool {
	if resourceSpans == nil {
		return false
	}
	resource := resourceSpans.GetResource()
	if resource == nil {
		return false
	}
	attrs := attributesToMap(resource.GetAttributes())
	value, ok := attrs["agyn.thread.id"]
	return ok && value == threadID
}

func attributesToMap(attrs []*commonv1.KeyValue) map[string]string {
	values := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		if attr == nil {
			continue
		}
		value, ok := attributeStringValue(attr.GetValue())
		if !ok {
			continue
		}
		values[attr.GetKey()] = value
	}
	return values
}

func attributeStringValue(value *commonv1.AnyValue) (string, bool) {
	if value == nil {
		return "", false
	}
	switch typed := value.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return typed.StringValue, true
	default:
		return "", false
	}
}
