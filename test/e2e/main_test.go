//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	clientexec "k8s.io/client-go/util/exec"
)

const (
	pollInterval = 2 * time.Second
	testTimeout  = 120 * time.Second

	tracingDiscoverTimeout = 2 * time.Minute
	tracingSummaryTimeout  = 2 * time.Minute
	tracingStartTimeBuffer = 30 * time.Second

	testLLMEndpointCodex = "https://testllm.dev/v1/org/agynio/suite/codex/responses"
	testLLMEndpointAgn   = "https://testllm.dev/v1/org/agynio/suite/agn/responses"

	labelManagedBy = "managed-by"
	labelAgentID   = "agent-id"
	labelThreadID  = "thread-id"
	managedByValue = "agents-orchestrator"
)

var (
	agentsAddr     = envOrDefault("AGENTS_ADDRESS", "agents:50051")
	threadsAddr    = envOrDefault("THREADS_ADDRESS", "threads:50051")
	llmAddr        = envOrDefault("LLM_ADDRESS", "llm:50051")
	usersAddr      = envOrDefault("USERS_ADDRESS", "users:50051")
	orgsAddr       = envOrDefault("ORGANIZATIONS_ADDRESS", "tenants:50051")
	runnerAddr     = envOrDefault("RUNNER_ADDRESS", "k8s-runner:50051")
	secretsAddr    = envOrDefault("SECRETS_ADDRESS", "secrets:50051")
	tracingAddr    = envOrDefault("TRACING_ADDRESS", "tracing:50051")
	codexInitImage = envOrDefault("CODEX_INIT_IMAGE", "ghcr.io/agynio/agent-init-codex:mcp-host-20260412-3")
	agnInitImage   = envOrDefault("AGN_INIT_IMAGE", "ghcr.io/agynio/agent-init-agn:mcp-host-20260412-6")
)

type pipelineRun struct {
	threadID       string
	startTimeMinNs uint64
	agentResponse  string
	messageText    string
}

func envOrDefault(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

// pollUntil retries check at interval until it returns nil or ctx expires.
func pollUntil(ctx context.Context, interval time.Duration, check func(ctx context.Context) error) error {
	lastErr := check(ctx)
	if lastErr == nil {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("poll timed out: last error: %w", lastErr)
		case <-ticker.C:
			if err := check(ctx); err == nil {
				return nil
			} else {
				lastErr = err
			}
		}
	}
}

// newUserID returns a random UUID to use as a fake user participant.
func newUserID() string {
	return uuid.New().String()
}

func createLLMProvider(t *testing.T, ctx context.Context, client llmv1.LLMServiceClient, endpoint, orgID string) *llmv1.LLMProvider {
	t.Helper()
	resp, err := client.CreateLLMProvider(ctx, &llmv1.CreateLLMProviderRequest{
		Endpoint:       endpoint,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
		Token:          "test-token",
		OrganizationId: orgID,
	})
	if err != nil {
		t.Fatalf("create llm provider: %v", err)
	}
	provider := resp.GetProvider()
	if provider == nil || provider.GetMeta() == nil {
		t.Fatal("create llm provider: nil response")
	}
	return provider
}

func createModel(t *testing.T, ctx context.Context, client llmv1.LLMServiceClient, name, providerID, remoteName, orgID string) *llmv1.Model {
	t.Helper()
	resp, err := client.CreateModel(ctx, &llmv1.CreateModelRequest{
		Name:           name,
		LlmProviderId:  providerID,
		RemoteName:     remoteName,
		OrganizationId: orgID,
	})
	if err != nil {
		t.Fatalf("create model %q: %v", name, err)
	}
	model := resp.GetModel()
	if model == nil || model.GetMeta() == nil {
		t.Fatal("create model: nil response")
	}
	return model
}

// --- Setup Helpers ---

func createAgent(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, name, model, organizationID, initImage string) *agentsv1.Agent {
	t.Helper()
	resp, err := client.CreateAgent(ctx, &agentsv1.CreateAgentRequest{
		Name:           name,
		Role:           "assistant",
		Model:          model,
		Image:          "alpine:3.21",
		InitImage:      initImage,
		OrganizationId: organizationID,
	})
	if err != nil {
		t.Fatalf("create agent %q: %v", name, err)
	}
	agent := resp.GetAgent()
	if agent == nil || agent.GetMeta() == nil {
		t.Fatal("create agent: nil response")
	}
	return agent
}

func deleteAgent(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, agentID string) {
	t.Helper()
	_, err := client.DeleteAgent(ctx, &agentsv1.DeleteAgentRequest{Id: agentID})
	if err != nil {
		t.Logf("cleanup: delete agent %s: %v", agentID, err)
	}
}

func createAgentEnv(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, agentID, name, value string) *agentsv1.Env {
	t.Helper()
	resp, err := client.CreateEnv(ctx, &agentsv1.CreateEnvRequest{
		Name:   name,
		Target: &agentsv1.CreateEnvRequest_AgentId{AgentId: agentID},
		Source: &agentsv1.CreateEnvRequest_Value{Value: value},
	})
	if err != nil {
		t.Fatalf("create agent env %q: %v", name, err)
	}
	env := resp.GetEnv()
	if env == nil || env.GetMeta() == nil {
		t.Fatal("create agent env: nil response")
	}
	return env
}

func createImagePullSecret(
	t *testing.T,
	ctx context.Context,
	client secretsv1.SecretsServiceClient,
	description string,
	registry string,
	username string,
	password string,
	orgID string,
) *secretsv1.ImagePullSecret {
	t.Helper()
	resp, err := client.CreateImagePullSecret(ctx, &secretsv1.CreateImagePullSecretRequest{
		Description:    description,
		Registry:       registry,
		Username:       username,
		Source:         &secretsv1.CreateImagePullSecretRequest_Value{Value: password},
		OrganizationId: orgID,
	})
	if err != nil {
		t.Fatalf("create image pull secret %q: %v", description, err)
	}
	secret := resp.GetImagePullSecret()
	if secret == nil || secret.GetMeta() == nil {
		t.Fatal("create image pull secret: nil response")
	}
	return secret
}

func deleteImagePullSecret(t *testing.T, ctx context.Context, client secretsv1.SecretsServiceClient, id string) {
	t.Helper()
	_, err := client.DeleteImagePullSecret(ctx, &secretsv1.DeleteImagePullSecretRequest{Id: id})
	if err != nil {
		t.Logf("cleanup: delete image pull secret %s: %v", id, err)
	}
}

func createImagePullSecretAttachment(
	t *testing.T,
	ctx context.Context,
	client agentsv1.AgentsServiceClient,
	imagePullSecretID string,
	agentID string,
) *agentsv1.ImagePullSecretAttachment {
	t.Helper()
	resp, err := client.CreateImagePullSecretAttachment(ctx, &agentsv1.CreateImagePullSecretAttachmentRequest{
		ImagePullSecretId: imagePullSecretID,
		Target:            &agentsv1.CreateImagePullSecretAttachmentRequest_AgentId{AgentId: agentID},
	})
	if err != nil {
		t.Fatalf("create image pull secret attachment: %v", err)
	}
	attachment := resp.GetImagePullSecretAttachment()
	if attachment == nil || attachment.GetMeta() == nil {
		t.Fatal("create image pull secret attachment: nil response")
	}
	return attachment
}

func deleteImagePullSecretAttachment(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, id string) {
	t.Helper()
	_, err := client.DeleteImagePullSecretAttachment(ctx, &agentsv1.DeleteImagePullSecretAttachmentRequest{Id: id})
	if err != nil {
		t.Logf("cleanup: delete image pull secret attachment %s: %v", id, err)
	}
}

func createMCP(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, agentID, name, image, command string) *agentsv1.Mcp {
	t.Helper()
	resp, err := client.CreateMcp(ctx, &agentsv1.CreateMcpRequest{
		AgentId: agentID,
		Name:    name,
		Image:   image,
		Command: command,
	})
	if err != nil {
		t.Fatalf("create mcp %q: %v", name, err)
	}
	mcp := resp.GetMcp()
	if mcp == nil || mcp.GetMeta() == nil {
		t.Fatal("create mcp: nil response")
	}
	return mcp
}

func deleteMCP(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, mcpID string) {
	t.Helper()
	_, err := client.DeleteMcp(ctx, &agentsv1.DeleteMcpRequest{Id: mcpID})
	if err != nil {
		t.Logf("cleanup: delete mcp %s: %v", mcpID, err)
	}
}

func createMCPEnv(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, mcpID, name, value string) *agentsv1.Env {
	t.Helper()
	resp, err := client.CreateEnv(ctx, &agentsv1.CreateEnvRequest{
		Name:   name,
		Target: &agentsv1.CreateEnvRequest_McpId{McpId: mcpID},
		Source: &agentsv1.CreateEnvRequest_Value{Value: value},
	})
	if err != nil {
		t.Fatalf("create mcp env %q: %v", name, err)
	}
	env := resp.GetEnv()
	if env == nil || env.GetMeta() == nil {
		t.Fatal("create mcp env: nil response")
	}
	return env
}

func createThread(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, participantIDs []string) *threadsv1.Thread {
	t.Helper()
	resp, err := client.CreateThread(ctx, &threadsv1.CreateThreadRequest{
		ParticipantIds: participantIDs,
	})
	if err != nil {
		t.Fatalf("create thread: %v", err)
	}
	thread := resp.GetThread()
	if thread == nil {
		t.Fatal("create thread: nil response")
	}
	return thread
}

func archiveThread(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, threadID string) {
	t.Helper()
	_, err := client.ArchiveThread(ctx, &threadsv1.ArchiveThreadRequest{ThreadId: threadID})
	if err != nil {
		t.Logf("cleanup: archive thread %s: %v", threadID, err)
	}
}

func sendMessage(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, threadID, senderID, body string) *threadsv1.Message {
	t.Helper()
	resp, err := client.SendMessage(ctx, &threadsv1.SendMessageRequest{
		ThreadId: threadID,
		SenderId: senderID,
		Body:     body,
	})
	if err != nil {
		t.Fatalf("send message on thread %s: %v", threadID, err)
	}
	msg := resp.GetMessage()
	if msg == nil {
		t.Fatal("send message: nil response")
	}
	return msg
}

func messageCreatedAt(t *testing.T, msg *threadsv1.Message) time.Time {
	t.Helper()
	if msg == nil {
		t.Fatal("message is nil")
	}
	createdAt := msg.GetCreatedAt()
	if createdAt == nil {
		t.Fatal("message created_at is nil")
	}
	return createdAt.AsTime()
}

func messageStartTimeMinNs(t *testing.T, msg *threadsv1.Message) uint64 {
	t.Helper()
	createdAt := messageCreatedAt(t, msg)
	return uint64(createdAt.UnixNano())
}

func ackMessages(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, participantID string, messageIDs []string) {
	t.Helper()
	_, err := client.AckMessages(ctx, &threadsv1.AckMessagesRequest{
		ParticipantId: participantID,
		MessageIds:    messageIDs,
	})
	if err != nil {
		t.Fatalf("ack messages for %s: %v", participantID, err)
	}
}

type podExecResult struct {
	stdout   string
	stderr   string
	exitCode int
}

func kubeRestConfig(t *testing.T) *rest.Config {
	t.Helper()
	config, err := rest.InClusterConfig()
	if err != nil {
		t.Fatalf("load in-cluster config: %v", err)
	}
	return config
}

func kubeClientset(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	clientset, err := kubernetes.NewForConfig(kubeRestConfig(t))
	if err != nil {
		t.Fatalf("create kubernetes clientset: %v", err)
	}
	return clientset
}

func execPodCommand(
	t *testing.T,
	ctx context.Context,
	namespace string,
	podName string,
	containerName string,
	command []string,
) podExecResult {
	t.Helper()
	config := kubeRestConfig(t)
	req := kubeClientset(t).CoreV1().RESTClient().Post().
		Namespace(namespace).
		Resource("pods").
		Name(podName).
		SubResource("exec")
	req.VersionedParams(&corev1.PodExecOptions{
		Container: containerName,
		Command:   command,
		Stdin:     false,
		Stdout:    true,
		Stderr:    true,
		TTY:       false,
	}, scheme.ParameterCodec)

	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		t.Fatalf("create exec pod=%s container=%s: %v", podName, containerName, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
		Tty:    false,
	})
	exitCode := 0
	if err != nil {
		var exitErr clientexec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitStatus()
		} else {
			t.Fatalf("exec pod=%s container=%s: %v", podName, containerName, err)
		}
	}

	return podExecResult{stdout: stdout.String(), stderr: stderr.String(), exitCode: exitCode}
}

func currentNamespace(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		t.Fatalf("read namespace: %v", err)
	}
	namespace := strings.TrimSpace(string(data))
	if namespace == "" {
		t.Fatal("namespace is empty")
	}
	return namespace
}

func workloadNamespace(t *testing.T) string {
	t.Helper()
	return envOrDefault("WORKLOAD_NAMESPACE", "agyn-workloads")
}

// --- Verification Helpers ---

func pollForAgentResponse(
	t *testing.T,
	ctx context.Context,
	threadsClient threadsv1.ThreadsServiceClient,
	runnerClient runnerv1.RunnerServiceClient,
	threadID string,
	agentID string,
	labels map[string]string,
	minCreatedAt time.Time,
	expectedBody string,
) (string, error) {
	t.Helper()
	truncateBody := func(body string) string {
		if body == "" {
			return body
		}
		bodyRunes := []rune(body)
		if len(bodyRunes) <= 200 {
			return body
		}
		return string(bodyRunes[:200])
	}
	truncateID := func(id string) string {
		if len(id) <= 8 {
			return id
		}
		return id[:8]
	}
	formatCreatedAt := func(msg *threadsv1.Message) string {
		createdAt := msg.GetCreatedAt()
		if createdAt == nil {
			return "-"
		}
		return createdAt.AsTime().Format(time.RFC3339Nano)
	}
	messageMatches := func(msg *threadsv1.Message) bool {
		if msg.GetSenderId() != agentID {
			return false
		}
		if !minCreatedAt.IsZero() {
			createdAt := msg.GetCreatedAt()
			if createdAt == nil {
				return false
			}
			if createdAt.AsTime().Before(minCreatedAt) {
				return false
			}
		}
		if expectedBody != "" && msg.GetBody() != expectedBody {
			return false
		}
		return true
	}

	agentBody := ""
	pollCount := 0
	err := pollUntil(ctx, pollInterval, func(ctx context.Context) error {
		pollCount++
		logDiagnostics := pollCount%10 == 0
		resp, err := threadsClient.GetMessages(ctx, &threadsv1.GetMessagesRequest{
			ThreadId: threadID,
			PageSize: 50,
		})
		if err != nil {
			return fmt.Errorf("get messages: %w", err)
		}
		agentMessage := ""
		for _, msg := range resp.GetMessages() {
			if logDiagnostics {
				t.Logf(
					"diagnostics: message id=%s sender=%s created_at=%s body=%s",
					truncateID(msg.GetId()),
					msg.GetSenderId(),
					formatCreatedAt(msg),
					truncateBody(msg.GetBody()),
				)
			}
			if agentMessage == "" && messageMatches(msg) {
				agentMessage = msg.GetBody()
			}
		}
		if agentMessage != "" {
			agentBody = agentMessage
			return nil
		}
		if logDiagnostics {
			ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
			if err != nil {
				t.Logf("diagnostics: find workloads: %v", err)
			} else if len(ids) == 0 {
				t.Log("diagnostics: no workloads found")
			} else {
				t.Logf("diagnostics: workloads=%v", ids)
				for _, workloadID := range ids {
					inspect, err := runnerClient.InspectWorkload(ctx, &runnerv1.InspectWorkloadRequest{WorkloadId: workloadID})
					if err != nil {
						t.Logf("diagnostics: workload=%s inspect error: %v", workloadID, err)
						continue
					}
					t.Logf("diagnostics: workload=%s state_status=%s state_running=%t", workloadID, inspect.GetStateStatus(), inspect.GetStateRunning())
					logsCtx, cancelLogs := context.WithTimeout(ctx, 2*time.Second)
					logWorkloadPodDiagnostics(t, logsCtx, workloadID)
					cancelLogs()
				}
			}
		}
		return fmt.Errorf("agent response not found")
	})
	if err != nil {
		return "", err
	}
	return agentBody, nil
}

func waitForMcpSidecarsReady(
	t *testing.T,
	ctx context.Context,
	runnerClient runnerv1.RunnerServiceClient,
	labels map[string]string,
) error {
	t.Helper()
	pollCount := 0
	return pollUntil(ctx, pollInterval, func(ctx context.Context) error {
		pollCount++
		logDiagnostics := pollCount%5 == 0
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return fmt.Errorf("no workloads found")
		}
		for _, workloadID := range ids {
			ready, err := workloadMcpSidecarsReady(t, ctx, workloadID)
			if err != nil {
				return err
			}
			if !ready {
				if logDiagnostics {
					logWorkloadPodDiagnostics(t, ctx, workloadID)
				}
				return fmt.Errorf("mcp sidecars not ready for workload %s", workloadID)
			}
		}
		return nil
	})
}

func workloadMcpSidecarsReady(t *testing.T, ctx context.Context, workloadID string) (bool, error) {
	t.Helper()
	namespace := workloadNamespace(t)
	podName := fmt.Sprintf("workload-%s", workloadID)
	clientset := kubeClientset(t)
	pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return false, fmt.Errorf("get pod %s: %w", podName, err)
	}
	containers := mcpContainerNames(pod)
	if len(containers) == 0 {
		return false, fmt.Errorf("pod %s has no mcp containers", pod.Name)
	}
	for _, container := range containers {
		ready, err := containerLogsContain(t, ctx, namespace, pod.Name, container, []string{
			"Listening on port",
			"StreamableHttp endpoint:",
		})
		if err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}
	}
	return true, nil
}

func mcpContainerNames(pod *corev1.Pod) []string {
	if pod == nil {
		return nil
	}
	containers := make([]string, 0, len(pod.Spec.Containers))
	for _, container := range pod.Spec.Containers {
		name := container.Name
		if strings.HasPrefix(name, "mcp-") {
			containers = append(containers, name)
		}
	}
	return containers
}

func agentContainerName(pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	for _, container := range pod.Spec.Containers {
		if strings.HasPrefix(container.Name, "agent-") {
			return container.Name, nil
		}
	}
	return "", fmt.Errorf("pod %s has no agent container", pod.Name)
}

func containerReady(pod *corev1.Pod, containerName string) bool {
	if pod == nil {
		return false
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == containerName {
			return status.Ready
		}
	}
	return false
}

func containerLogsContain(
	t *testing.T,
	ctx context.Context,
	namespace,
	podName,
	containerName string,
	needles []string,
) (bool, error) {
	tailLines := int64(200)
	request := kubeClientset(t).CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  containerName,
		TailLines:  &tailLines,
		Timestamps: true,
	})
	stream, err := request.Stream(ctx)
	if err != nil {
		return false, fmt.Errorf("get logs pod=%s container=%s: %w", podName, containerName, err)
	}
	defer stream.Close()

	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		for _, needle := range needles {
			if strings.Contains(line, needle) {
				return true, nil
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan logs pod=%s container=%s: %w", podName, containerName, err)
	}
	return false, nil
}

func findWorkloadsByLabels(ctx context.Context, client runnerv1.RunnerServiceClient, labels map[string]string) ([]string, error) {
	resp, err := client.FindWorkloadsByLabels(ctx, &runnerv1.FindWorkloadsByLabelsRequest{
		Labels: labels,
		All:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("find workloads: %w", err)
	}
	return resp.GetTargetIds(), nil
}

func getWorkloadLabels(ctx context.Context, client runnerv1.RunnerServiceClient, workloadID string) (map[string]string, error) {
	resp, err := client.GetWorkloadLabels(ctx, &runnerv1.GetWorkloadLabelsRequest{
		WorkloadId: workloadID,
	})
	if err != nil {
		return nil, fmt.Errorf("get labels for %s: %w", workloadID, err)
	}
	return resp.GetLabels(), nil
}

// --- Teardown Helpers ---

func cleanupWorkload(t *testing.T, ctx context.Context, client runnerv1.RunnerServiceClient, workloadID string) {
	t.Helper()
	_, err := client.StopWorkload(ctx, &runnerv1.StopWorkloadRequest{
		WorkloadId: workloadID,
		TimeoutSec: 10,
	})
	if err != nil {
		t.Logf("cleanup: stop workload %s: %v", workloadID, err)
	}
	_, err = client.RemoveWorkload(ctx, &runnerv1.RemoveWorkloadRequest{
		WorkloadId:    workloadID,
		Force:         true,
		RemoveVolumes: true,
	})
	if err != nil {
		t.Logf("cleanup: remove workload %s: %v", workloadID, err)
	}
}

func assertLabel(t *testing.T, labels map[string]string, key, expected string) {
	t.Helper()
	value, ok := labels[key]
	if !ok {
		t.Fatalf("missing label %s", key)
	}
	if value != expected {
		t.Fatalf("expected label %s=%q, got %q", key, expected, value)
	}
}
