//go:build e2e

package e2e

import (
	"context"
	"strings"
	"testing"
)

func TestAgentSimpleHelloProducesTrace(t *testing.T) {
	requireTracingAvailable(t)
	expectedResponse := "Hi! How are you?"
	result := runFullPipelineMessageResponse(t, testLLMEndpointAgn, agnInitImage, "hi", expectedResponse)

	ctx := context.Background()
	tracingClient := newTracingClient(t)
	traceID := discoverTraceID(t, ctx, tracingClient, result.threadID, result.startTimeMinNs, result.messageText)
	assertTraceSummary(t, ctx, tracingClient, traceID, map[string]int64{
		"invocation.message": 1,
		"llm.call":           1,
	}, 2, result.threadID)

	assertSpanAttributes(t, ctx, tracingClient, traceID, "invocation.message", map[string]string{
		"agyn.message.text": result.messageText,
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
	requireTracingAvailable(t)
	result := runMCPToolsE2E(t, testLLMEndpointAgn, agnInitImage)

	ctx := context.Background()
	tracingClient := newTracingClient(t)
	traceID := discoverTraceID(t, ctx, tracingClient, result.threadID, result.startTimeMinNs, result.messageText)
	expectedCounts := map[string]spanCountRange{
		"invocation.message": {min: 1, max: 1},
		"llm.call":           {min: 2, max: 3},
		"tool.execution":     {min: 2, max: 2},
	}
	expectedTotal := spanCountRange{min: 5, max: 6}
	optionalCounts := map[string]spanCountRange{
		"invocation.message": {min: 0, max: 1},
		"llm.call":           {min: 2, max: 3},
		"tool.execution":     {min: 1, max: 2},
	}
	traceSummaryErr := waitForTraceSummaryRange(ctx, tracingClient, traceID, expectedCounts, expectedTotal)
	selectedTraceID := traceID
	if traceSummaryErr != nil {
		invocationTraceIDs, err := traceIDsForSpanName(ctx, tracingClient, result.threadID, result.messageText, result.startTimeMinNs, "invocation.message")
		if err != nil {
			t.Fatalf("list invocation.message spans: %v", err)
		}
		llmTraceIDs, err := traceIDsForSpanName(ctx, tracingClient, result.threadID, "", result.startTimeMinNs, "llm.call")
		if err != nil {
			t.Fatalf("list llm.call spans: %v", err)
		}
		toolTraceIDs, err := traceIDsForSpanName(ctx, tracingClient, result.threadID, "", result.startTimeMinNs, "tool.execution")
		if err != nil {
			t.Fatalf("list tool.execution spans: %v", err)
		}
		if len(llmTraceIDs) == 0 {
			t.Fatalf("llm.call spans missing: %v", traceSummaryErr)
		}
		if len(toolTraceIDs) == 0 {
			t.Fatalf("tool.execution spans missing: %v", traceSummaryErr)
		}
		countRanges := expectedCounts
		if len(invocationTraceIDs) == 0 {
			t.Logf("invocation.message spans missing: %v", traceSummaryErr)
			countRanges = optionalCounts
		}
		sharedTraceIDs := make(traceIDSet)
		for id := range invocationTraceIDs {
			if _, ok := llmTraceIDs[id]; !ok {
				continue
			}
			if _, ok := toolTraceIDs[id]; !ok {
				continue
			}
			sharedTraceIDs[id] = struct{}{}
		}
		if len(sharedTraceIDs) > 0 {
			selectedTraceID = decodeTraceID(t, sortedTraceIDs(sharedTraceIDs)[0])
			assertTraceSummaryRange(t, ctx, tracingClient, selectedTraceID, expectedCounts, expectedTotal, result.threadID)
		} else {
			t.Logf(
				"trace mismatch: invocation.message=%v llm.call=%v tool.execution=%v (summary err: %v)",
				sortedTraceIDs(invocationTraceIDs),
				sortedTraceIDs(llmTraceIDs),
				sortedTraceIDs(toolTraceIDs),
				traceSummaryErr,
			)
			assertRange := func(name string, count int64, expected spanCountRange) {
				t.Helper()
				if count < expected.min || count > expected.max {
					if expected.min == expected.max {
						t.Fatalf("expected %s count %d, got %d", name, expected.min, count)
					}
					t.Fatalf("expected %s count %d-%d, got %d", name, expected.min, expected.max, count)
				}
			}
			invocationCount, err := countSpansForThread(ctx, tracingClient, result.threadID, result.messageText, result.startTimeMinNs, "invocation.message")
			if err != nil {
				t.Fatalf("count invocation.message spans: %v", err)
			}
			llmCount, err := countSpansForThread(ctx, tracingClient, result.threadID, "", result.startTimeMinNs, "llm.call")
			if err != nil {
				t.Fatalf("count llm.call spans: %v", err)
			}
			toolCount, err := countSpansForThread(ctx, tracingClient, result.threadID, "", result.startTimeMinNs, "tool.execution")
			if err != nil {
				t.Fatalf("count tool.execution spans: %v", err)
			}
			assertRange("invocation.message", invocationCount, countRanges["invocation.message"])
			assertRange("llm.call", llmCount, countRanges["llm.call"])
			assertRange("tool.execution", toolCount, countRanges["tool.execution"])

			selectedTraceID = decodeTraceID(t, sortedTraceIDs(toolTraceIDs)[0])
		}
	}

	spans := traceSpans(t, ctx, tracingClient, selectedTraceID)
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
