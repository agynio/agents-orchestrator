//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
)

func TestMCPToolsE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)
	usersConn := dialGRPC(t, usersAddr)
	orgsConn := dialGRPC(t, orgsAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	identityClient := identityv1.NewIdentityServiceClient(dialGRPC(t, identityAddr))
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	usersClient := usersv1.NewUsersServiceClient(usersConn)
	orgsClient := organizationsv1.NewOrganizationsServiceClient(orgsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	identityID := resolveOrCreateUser(t, ctx, usersClient)
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)
	registerIdentity(t, ctx, identityClient, identityID)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpoint, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "mcp-tools-test", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, "e2e-mcp-tools-"+uuid.NewString(), modelID, orgID)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)
	registerAgentIdentity(t, ctx, identityClient, agentID)
	memoryMCP := createMCP(
		t,
		ctx,
		agentsClient,
		agentID,
		"memory",
		"node:22-slim",
		`npx -y supergateway --stdio "npx -y @modelcontextprotocol/server-memory" --outputTransport streamableHttp --port $MCP_PORT --streamableHttpPath /mcp`,
	)
	memoryMcpID := memoryMCP.GetMeta().GetId()
	if memoryMcpID == "" {
		t.Fatal("create memory mcp: missing id")
	}
	t.Cleanup(func() { deleteMCP(t, ctx, agentsClient, memoryMcpID) })
	createMCPEnv(t, ctx, agentsClient, memoryMcpID, "MEMORY_FILE_PATH", "/tmp/memory.json")

	filesystemMCP := createMCP(
		t,
		ctx,
		agentsClient,
		agentID,
		"filesystem",
		"node:22-slim",
		`mkdir -p /test-data && printf 'hello' > /test-data/hello.txt && npx -y supergateway --stdio "npx -y @modelcontextprotocol/server-filesystem /test-data" --outputTransport streamableHttp --port $MCP_PORT --streamableHttpPath /mcp`,
	)
	filesystemMcpID := filesystemMCP.GetMeta().GetId()
	if filesystemMcpID == "" {
		t.Fatal("create filesystem mcp: missing id")
	}
	t.Cleanup(func() { deleteMCP(t, ctx, agentsClient, filesystemMcpID) })

	thread := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	sendMessage(t, ctx, threadsClient, threadID, identityID, "Create an entity called test_project of type project with observation 'A test project', then list files in /test-data")
	t.Logf("test setup complete: agentID=%s threadID=%s memoryMcpID=%s filesystemMcpID=%s", agentID, threadID, memoryMcpID, filesystemMcpID)

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
	}
	t.Cleanup(func() {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			t.Logf("cleanup: find workloads: %v", err)
			return
		}
		for _, workloadID := range ids {
			cleanupWorkload(t, ctx, runnerClient, workloadID)
		}
	})

	pollCtx, pollCancel := context.WithTimeout(ctx, 7*time.Minute)
	defer pollCancel()
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
	if err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
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
	}); err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}

	expected := "I've created the entity 'test_project' (type: project) with the observation 'A test project'. The /test-data directory contains one file: hello.txt."
	if agentBody != expected {
		t.Fatalf("expected agent response %q, got %q", expected, agentBody)
	}
}
