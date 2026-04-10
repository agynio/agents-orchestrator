//go:build e2e

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	tracingv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/tracing/v1"
	commonv1 "github.com/agynio/agents-orchestrator/.gen/go/opentelemetry/proto/common/v1"
	tracev1 "github.com/agynio/agents-orchestrator/.gen/go/opentelemetry/proto/trace/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func requireTracingAvailable(t *testing.T) {
	t.Helper()
	if tracingAvailable {
		return
	}
	if tracingSkipReason != "" {
		t.Skipf("tracing e2e skipped: %s", tracingSkipReason)
	}
	reason := strings.TrimSpace(tracingUnavailableReason)
	if reason == "" {
		reason = "tracing ingest check failed"
	}
	t.Fatalf("tracing ingest unavailable: %s", reason)
}

func newTracingClient(t *testing.T) tracingv1.TracingServiceClient {
	t.Helper()
	conn := dialGRPC(t, tracingAddr)
	return tracingv1.NewTracingServiceClient(conn)
}

var traceSearchSpanNames = []string{
	"invocation.message",
	"tool.execution",
	"llm.call",
}

func discoverTraceID(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	threadID string,
	startTimeMinNs uint64,
	messageText string,
) []byte {
	t.Helper()
	searchStartTimeMinNs := startTimeMinNs
	if tracingStartTimeBuffer > 0 {
		bufferNs := uint64(tracingStartTimeBuffer.Nanoseconds())
		if searchStartTimeMinNs > bufferNs {
			searchStartTimeMinNs -= bufferNs
		} else {
			searchStartTimeMinNs = 0
		}
	}
	messageText = strings.TrimSpace(messageText)

	pollCtx, cancel := context.WithTimeout(ctx, tracingDiscoverTimeout)
	defer cancel()

	var traceID []byte
	err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		var err error
		traceID, err = findTraceID(ctx, client, threadID, messageText, searchStartTimeMinNs)
		if err != nil {
			return err
		}
		if len(traceID) > 0 {
			return nil
		}
		return fmt.Errorf("trace id not found")
	})
	if err != nil {
		logTraceSearchDiagnostics(t, client, searchStartTimeMinNs, messageText)
		logTracingDiagnostics(t, threadID)
		t.Fatalf("discover trace id: %v", err)
	}
	return traceID
}

func findTraceID(
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	threadID string,
	messageText string,
	startTimeMinNs uint64,
) ([]byte, error) {
	for _, spanName := range traceSearchSpanNames {
		traceID, err := listTraceIDForSpanName(ctx, client, threadID, messageText, startTimeMinNs, spanName)
		if err != nil {
			return nil, err
		}
		if len(traceID) > 0 {
			return traceID, nil
		}
	}
	return nil, nil
}

func listTraceIDForSpanName(
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	threadID string,
	messageText string,
	startTimeMinNs uint64,
	spanName string,
) ([]byte, error) {
	pageToken := ""
	for {
		resp, err := client.ListSpans(ctx, &tracingv1.ListSpansRequest{
			Filter: &tracingv1.SpanFilter{
				StartTimeMin: startTimeMinNs,
				Names:        []string{spanName},
			},
			PageSize:  100,
			PageToken: pageToken,
			OrderBy:   tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_DESC,
		})
		if err != nil {
			return nil, fmt.Errorf("list spans %s: %w", spanName, err)
		}
		if traceID := traceIDFromResourceSpans(resp.GetResourceSpans(), threadID); len(traceID) > 0 {
			return traceID, nil
		}
		if spanName == "invocation.message" {
			if traceID := traceIDFromMessageText(resp.GetResourceSpans(), messageText); len(traceID) > 0 {
				return traceID, nil
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil, nil
}

func assertTraceSummary(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	traceID []byte,
	expectedCounts map[string]int64,
	expectedTotal int64,
	threadID string,
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
		if threadID != "" {
			logTracingDiagnostics(t, threadID)
		}
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

func traceIDFromResourceSpans(resourceSpans []*tracev1.ResourceSpans, threadID string) []byte {
	for _, resourceSpan := range resourceSpans {
		spans := spansFromResource(resourceSpan)
		if resourceHasThreadID(resourceSpan, threadID) {
			if traceID := traceIDFromSpans(spans); len(traceID) > 0 {
				return traceID
			}
			continue
		}
		for _, span := range spans {
			if !spanHasThreadID(span, threadID) {
				continue
			}
			if len(span.GetTraceId()) == 0 {
				continue
			}
			return span.GetTraceId()
		}
	}
	return nil
}

func traceIDFromSpans(spans []*tracev1.Span) []byte {
	for _, span := range spans {
		if len(span.GetTraceId()) == 0 {
			continue
		}
		return span.GetTraceId()
	}
	return nil
}

func traceIDFromMessageText(resourceSpans []*tracev1.ResourceSpans, messageText string) []byte {
	if strings.TrimSpace(messageText) == "" {
		return nil
	}
	for _, resourceSpan := range resourceSpans {
		for _, span := range spansFromResource(resourceSpan) {
			attrs := attributesToMap(span.GetAttributes())
			value, ok := attrs["agyn.message.text"]
			if !ok {
				continue
			}
			if !messageTextMatches(value, messageText) {
				continue
			}
			if len(span.GetTraceId()) == 0 {
				continue
			}
			return span.GetTraceId()
		}
	}
	return nil
}

func messageTextMatches(value string, messageText string) bool {
	trimmedValue := strings.TrimSpace(value)
	trimmedMessage := strings.TrimSpace(messageText)
	if trimmedValue == "" || trimmedMessage == "" {
		return false
	}
	if trimmedValue == trimmedMessage {
		return true
	}
	if len(trimmedValue) < len(trimmedMessage) {
		return strings.HasPrefix(trimmedMessage, trimmedValue)
	}
	return strings.HasPrefix(trimmedValue, trimmedMessage)
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

func spanHasThreadID(span *tracev1.Span, threadID string) bool {
	if span == nil {
		return false
	}
	attrs := attributesToMap(span.GetAttributes())
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

func logTraceSearchDiagnostics(
	t *testing.T,
	client tracingv1.TracingServiceClient,
	startTimeMinNs uint64,
	messageText string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if strings.TrimSpace(messageText) != "" {
		t.Logf("diagnostics: trace search message=%s", truncateLogLine(messageText))
	}
	logSpanSamples(t, ctx, client, startTimeMinNs, []string{"invocation.message"}, "invocation.message")
	logSpanSamples(t, ctx, client, startTimeMinNs, nil, "all-spans")
	logTracingStackDiagnostics(t)
}

func logSpanSamples(
	t *testing.T,
	ctx context.Context,
	client tracingv1.TracingServiceClient,
	startTimeMinNs uint64,
	spanNames []string,
	label string,
) {
	t.Helper()
	filter := &tracingv1.SpanFilter{StartTimeMin: startTimeMinNs}
	if len(spanNames) > 0 {
		filter.Names = spanNames
	}
	resp, err := client.ListSpans(ctx, &tracingv1.ListSpansRequest{
		Filter:   filter,
		PageSize: 10,
		OrderBy:  tracingv1.ListSpansOrderBy_LIST_SPANS_ORDER_BY_START_TIME_DESC,
	})
	if err != nil {
		t.Logf("diagnostics: list spans %s error: %v", label, err)
		return
	}
	samples := 0
	for _, resourceSpan := range resp.GetResourceSpans() {
		resourceAttrs := attributesToMap(resourceSpan.GetResource().GetAttributes())
		resourceThreadID := resourceAttrs["agyn.thread.id"]
		resourceService := resourceAttrs["service.name"]
		for _, span := range spansFromResource(resourceSpan) {
			spanAttrs := attributesToMap(span.GetAttributes())
			spanThreadID := spanAttrs["agyn.thread.id"]
			message := spanAttrs["agyn.message.text"]
			toolName := spanAttrs["agyn.tool.name"]
			t.Logf(
				"diagnostics: span_sample label=%s name=%s trace=%x resource_thread=%s span_thread=%s service=%s message=%s tool=%s",
				label,
				span.GetName(),
				span.GetTraceId(),
				resourceThreadID,
				spanThreadID,
				resourceService,
				truncateLogLine(message),
				toolName,
			)
			samples++
			if samples >= 5 {
				return
			}
		}
	}
	if samples == 0 {
		t.Logf("diagnostics: no spans found for %s", label)
	}
}

func logTracingDiagnostics(t *testing.T, threadID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	clientset := kubeClientset(t)
	namespace := workloadNamespace(t)
	selector := fmt.Sprintf("%s=%s,%s=%s", labelManagedBy, managedByValue, labelThreadID, threadID)
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Logf("diagnostics: list workload pods: %v", err)
		return
	}
	if len(pods.Items) == 0 {
		t.Logf("diagnostics: no workload pods found for thread %s", threadID)
		return
	}
	for _, pod := range pods.Items {
		logWorkloadPodDiagnosticsFromPod(t, ctx, pod)
	}
}

func readWorkloadLogs(t *testing.T, ctx context.Context, namespace, podName, containerName string) {
	t.Helper()
	tail := int64(50)
	request := kubeClientset(t).CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  containerName,
		TailLines:  &tail,
		Timestamps: true,
	})
	stream, err := request.Stream(ctx)
	if err != nil {
		t.Logf("diagnostics: pod=%s container=%s log error: %v", podName, containerName, err)
		return
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lines := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		t.Logf("diagnostics: pod=%s container=%s log=%s", podName, containerName, truncateLogLine(line))
		lines++
		if lines >= 5 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		t.Logf("diagnostics: pod=%s container=%s log scan error: %v", podName, containerName, err)
	}
}

func logTracingStackDiagnostics(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	namespace := currentNamespace(t)
	clientset := kubeClientset(t)
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Logf("diagnostics: list tracing pods: %v", err)
		return
	}
	found := 0
	for _, pod := range pods.Items {
		if !isTracingPod(pod) {
			continue
		}
		t.Logf("diagnostics: tracing pod=%s", pod.Name)
		for _, container := range pod.Spec.Containers {
			t.Logf("diagnostics: tracing pod=%s container=%s", pod.Name, container.Name)
			readWorkloadLogs(t, ctx, namespace, pod.Name, container.Name)
		}
		found++
		if found >= 2 {
			break
		}
	}
	if found == 0 {
		t.Log("diagnostics: no tracing pods found")
	}
}

func isTracingPod(pod corev1.Pod) bool {
	name := strings.ToLower(pod.Name)
	if strings.Contains(name, "tracing") || strings.Contains(name, "tempo") || strings.Contains(name, "otel") || strings.Contains(name, "collector") {
		return true
	}
	for key, value := range pod.Labels {
		labelKey := strings.ToLower(key)
		labelValue := strings.ToLower(value)
		if strings.Contains(labelKey, "tracing") || strings.Contains(labelValue, "tracing") {
			return true
		}
		if strings.Contains(labelKey, "otel") || strings.Contains(labelValue, "otel") {
			return true
		}
		if strings.Contains(labelKey, "tempo") || strings.Contains(labelValue, "tempo") {
			return true
		}
		if strings.Contains(labelKey, "collector") || strings.Contains(labelValue, "collector") {
			return true
		}
	}
	return false
}

func truncateLogLine(line string) string {
	if line == "" {
		return line
	}
	lineRunes := []rune(line)
	if len(lineRunes) <= 200 {
		return line
	}
	return string(lineRunes[:200])
}

func logWorkloadPodDiagnostics(t *testing.T, ctx context.Context, workloadID string) {
	t.Helper()
	namespace := workloadNamespace(t)
	podName := fmt.Sprintf("workload-%s", workloadID)
	clientset := kubeClientset(t)
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Logf("diagnostics: pod=%s get error: %v", podName, err)
		return
	}
	logWorkloadPodDiagnosticsFromPod(t, ctx, *pod)
}

func logWorkloadPodDiagnosticsFromPod(t *testing.T, ctx context.Context, pod corev1.Pod) {
	t.Helper()
	logWorkloadPodStatus(t, pod)
	namespace := pod.Namespace
	if namespace == "" {
		namespace = workloadNamespace(t)
	}
	logWorkloadPodEvents(t, ctx, namespace, pod.Name)
	logWorkloadContainerLogs(t, ctx, namespace, pod)
}

func logWorkloadPodStatus(t *testing.T, pod corev1.Pod) {
	t.Helper()
	message := truncateLogLine(pod.Status.Message)
	if message == "" {
		message = "-"
	}
	t.Logf(
		"diagnostics: pod=%s phase=%s reason=%s message=%s",
		pod.Name,
		pod.Status.Phase,
		pod.Status.Reason,
		message,
	)
	for _, condition := range pod.Status.Conditions {
		conditionMessage := truncateLogLine(condition.Message)
		if conditionMessage == "" {
			conditionMessage = "-"
		}
		t.Logf(
			"diagnostics: pod=%s condition=%s status=%s reason=%s message=%s",
			pod.Name,
			condition.Type,
			condition.Status,
			condition.Reason,
			conditionMessage,
		)
	}
	for _, status := range pod.Status.InitContainerStatuses {
		logContainerStatus(t, pod.Name, "init", status)
	}
	for _, status := range pod.Status.ContainerStatuses {
		logContainerStatus(t, pod.Name, "container", status)
	}
}

func logContainerStatus(t *testing.T, podName, kind string, status corev1.ContainerStatus) {
	t.Helper()
	state, reason, message, exitCode := summarizeContainerState(status.State)
	if message == "" {
		message = "-"
	}
	t.Logf(
		"diagnostics: pod=%s %s=%s ready=%t restarts=%d state=%s reason=%s exit=%d message=%s",
		podName,
		kind,
		status.Name,
		status.Ready,
		status.RestartCount,
		state,
		reason,
		exitCode,
		truncateLogLine(message),
	)
	if status.LastTerminationState.Terminated != nil {
		last := status.LastTerminationState.Terminated
		lastMessage := truncateLogLine(last.Message)
		if lastMessage == "" {
			lastMessage = "-"
		}
		t.Logf(
			"diagnostics: pod=%s %s=%s last_exit=%d last_reason=%s last_message=%s",
			podName,
			kind,
			status.Name,
			last.ExitCode,
			last.Reason,
			lastMessage,
		)
	}
}

func summarizeContainerState(state corev1.ContainerState) (string, string, string, int32) {
	switch {
	case state.Running != nil:
		return "running", "", "", 0
	case state.Waiting != nil:
		return "waiting", state.Waiting.Reason, state.Waiting.Message, 0
	case state.Terminated != nil:
		return "terminated", state.Terminated.Reason, state.Terminated.Message, state.Terminated.ExitCode
	default:
		return "unknown", "", "", 0
	}
}

func logWorkloadPodEvents(t *testing.T, ctx context.Context, namespace, podName string) {
	t.Helper()
	clientset := kubeClientset(t)
	fieldSelector := fmt.Sprintf("involvedObject.name=%s", podName)
	events, err := clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{FieldSelector: fieldSelector})
	if err != nil {
		t.Logf("diagnostics: pod=%s events error: %v", podName, err)
		return
	}
	if len(events.Items) == 0 {
		t.Logf("diagnostics: pod=%s no events", podName)
		return
	}
	sort.Slice(events.Items, func(i, j int) bool {
		return eventTimestamp(events.Items[i]).Before(eventTimestamp(events.Items[j]))
	})
	start := len(events.Items) - 5
	if start < 0 {
		start = 0
	}
	for _, event := range events.Items[start:] {
		eventMessage := truncateLogLine(event.Message)
		if eventMessage == "" {
			eventMessage = "-"
		}
		t.Logf(
			"diagnostics: pod=%s event=%s reason=%s type=%s message=%s",
			podName,
			event.Name,
			event.Reason,
			event.Type,
			eventMessage,
		)
	}
}

func eventTimestamp(event corev1.Event) time.Time {
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if !event.FirstTimestamp.IsZero() {
		return event.FirstTimestamp.Time
	}
	return time.Time{}
}

func logWorkloadContainerLogs(t *testing.T, ctx context.Context, namespace string, pod corev1.Pod) {
	t.Helper()
	containers := workloadContainerNames(pod)
	if len(containers) == 0 {
		t.Logf("diagnostics: pod=%s no agent/mcp containers found", pod.Name)
		return
	}
	for _, container := range containers {
		t.Logf("diagnostics: workload pod=%s container=%s", pod.Name, container)
		readWorkloadLogs(t, ctx, namespace, pod.Name, container)
	}
}

func workloadContainerNames(pod corev1.Pod) []string {
	containers := make([]string, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		name := container.Name
		if strings.HasPrefix(name, "agent-") || strings.HasPrefix(name, "mcp-") {
			containers = append(containers, name)
		}
	}
	return containers
}
