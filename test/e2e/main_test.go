//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	pollInterval = 2 * time.Second
	testTimeout  = 120 * time.Second

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
	codexInitImage = envOrDefault("CODEX_INIT_IMAGE", "ghcr.io/agynio/agent-init-codex:latest")
	agnInitImage   = envOrDefault("AGN_INIT_IMAGE", "ghcr.io/agynio/agent-init-agn:latest")
)

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

func kubeClientset(t *testing.T) *kubernetes.Clientset {
	t.Helper()
	config, err := rest.InClusterConfig()
	if err != nil {
		t.Fatalf("load in-cluster config: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create kubernetes clientset: %v", err)
	}
	return clientset
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
				t.Logf("diagnostics: message sender=%s body=%s", msg.GetSenderId(), truncateBody(msg.GetBody()))
			}
			if agentMessage == "" && msg.GetSenderId() == agentID {
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
					stream, err := runnerClient.StreamWorkloadLogs(logsCtx, &runnerv1.StreamWorkloadLogsRequest{
						WorkloadId: workloadID,
						Tail:       50,
						Stdout:     true,
						Stderr:     true,
						Timestamps: true,
					})
					if err != nil {
						t.Logf("diagnostics: workload=%s stream logs error: %v", workloadID, err)
						cancelLogs()
						continue
					}

					logLines := 0
					for logLines < 5 {
						resp, err := stream.Recv()
						if err != nil {
							if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
								break
							}
							t.Logf("diagnostics: workload=%s log stream recv error: %v", workloadID, err)
							break
						}
						if resp.GetError() != nil {
							t.Logf("diagnostics: workload=%s log stream error: %s", workloadID, resp.GetError().GetMessage())
							break
						}
						if resp.GetEnd() != nil {
							break
						}
						chunk := resp.GetChunk()
						if chunk == nil {
							continue
						}
						data := strings.TrimSpace(string(chunk.GetData()))
						if data == "" {
							continue
						}
						for _, line := range strings.Split(data, "\n") {
							line = strings.TrimSpace(line)
							if line == "" {
								continue
							}
							t.Logf("diagnostics: workload=%s log=%s", workloadID, truncateBody(line))
							logLines++
							if logLines >= 5 {
								break
							}
						}
					}
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
