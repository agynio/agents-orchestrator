//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pollInterval = 2 * time.Second
	testTimeout  = 120 * time.Second

	testOrganizationID = "33333333-3333-3333-3333-333333333333"

	labelManagedBy = "managed-by"
	labelAgentID   = "agent-id"
	labelThreadID  = "thread-id"
	managedByValue = "agents-orchestrator"
)

var (
	agentsAddr   = envOrDefault("AGENTS_ADDRESS", "agents:50051")
	threadsAddr  = envOrDefault("THREADS_ADDRESS", "threads:50051")
	identityAddr = envOrDefault("IDENTITY_ADDRESS", "identity:50051")
	runnerAddr   = envOrDefault("RUNNER_ADDRESS", "k8s-runner:50051")
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

// dialGRPC creates an insecure gRPC connection. The test fails immediately on error.
func dialGRPC(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	options = append(options, opts...)
	conn, err := grpc.DialContext(ctx, addr, options...)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
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

func registerIdentity(t *testing.T, ctx context.Context, client identityv1.IdentityServiceClient, identityID string) {
	t.Helper()
	_, err := client.RegisterIdentity(ctx, &identityv1.RegisterIdentityRequest{
		IdentityId:   identityID,
		IdentityType: identityv1.IdentityType_IDENTITY_TYPE_USER,
	})
	if err != nil {
		t.Fatalf("register identity %s: %v", identityID, err)
	}
}

// --- Setup Helpers ---

func createAgent(t *testing.T, ctx context.Context, client agentsv1.AgentsServiceClient, name string) *agentsv1.Agent {
	t.Helper()
	modelValue := uuid.New().String()
	resp, err := client.CreateAgent(ctx, &agentsv1.CreateAgentRequest{
		Name:           name,
		Role:           "assistant",
		Model:          modelValue,
		Image:          "alpine:3.21",
		OrganizationId: testOrganizationID,
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

// --- Verification Helpers ---

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
