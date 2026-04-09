//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
)

func TestAgentSimpleHelloProducesTrace(t *testing.T) {
	expectedResponse := "Hi! How are you?"
	result := runFullPipelineMessageResponse(t, testLLMEndpointAgn, agnInitImage, "hi", expectedResponse)

	ctx := context.Background()
	tracingClient := newTracingClient(t)
	traceID := discoverTraceID(t, ctx, tracingClient, result.threadID, result.startTimeMinNs)
	assertTraceSummary(t, ctx, tracingClient, traceID, map[string]int64{
		"invocation.message": 1,
		"llm.call":           1,
	}, 2)

	assertSpanAttributes(t, ctx, tracingClient, traceID, "invocation.message", map[string]string{
		"agyn.message.text": "hi",
		"agyn.message.role": "user",
		"agyn.message.kind": "source",
	})
	llmAttrs := assertSpanAttributes(t, ctx, tracingClient, traceID, "llm.call", map[string]string{
		"gen_ai.system":          "openai",
		"agyn.llm.response_text": expectedResponse,
	})
	modelName, ok := llmAttrs["gen_ai.request.model"]
	if !ok || strings.TrimSpace(modelName) == "" {
		t.Fatal("expected gen_ai.request.model to be set")
	}
}

func TestAgentMCPToolsProducesTrace(t *testing.T) {
	result := runMCPToolsE2E(t, testLLMEndpointAgn, agnInitImage)

	ctx := context.Background()
	tracingClient := newTracingClient(t)
	traceID := discoverTraceID(t, ctx, tracingClient, result.threadID, result.startTimeMinNs)
	assertTraceSummary(t, ctx, tracingClient, traceID, map[string]int64{
		"invocation.message": 1,
		"llm.call":           2,
		"tool.execution":     2,
	}, 5)

	spans := traceSpans(t, ctx, tracingClient, traceID)
	foundCreate := false
	foundList := false
	for _, span := range spans {
		if span.GetName() != "tool.execution" {
			continue
		}
		attrs := attributesToMap(span.GetAttributes())
		toolName := attrs["agyn.tool.name"]
		if strings.Contains(toolName, "create_entities") {
			foundCreate = true
		}
		if strings.Contains(toolName, "list_directory") {
			foundList = true
		}
	}
	if !foundCreate {
		t.Fatal("expected tool.execution span for create_entities")
	}
	if !foundList {
		t.Fatal("expected tool.execution span for list_directory")
	}
}
