//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
)

func TestThreadsSendShell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)
	usersConn := dialGRPC(t, usersAddr)
	orgsConn := dialGRPC(t, orgsAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	usersClient := usersv1.NewUsersServiceClient(usersConn)
	orgsClient := organizationsv1.NewOrganizationsServiceClient(orgsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	identityID := resolveOrCreateUser(t, ctx, usersClient)
	threadsCtx := withIdentity(ctx, identityID)
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpointAgn, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "shell-threads-send", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-threads-send-%s", uuid.NewString()), modelID, orgID, agnInitImage)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	thread := createThread(t, threadsCtx, threadsClient, orgID, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, threadsCtx, threadsClient, threadID) })

	sentMessage := sendMessage(t, threadsCtx, threadsClient, threadID, identityID, "Send me an intermediate update then reply")
	sentMessageTime := messageCreatedAt(t, sentMessage)
	startTimeMinNs := messageStartTimeMinNs(t, sentMessage)

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

	pollCtx, pollCancel := context.WithTimeout(threadsCtx, 5*time.Minute)
	defer pollCancel()
	expectedBodies := []string{"Thinking", "Done thinking. Here is my reply."}
	agentMessages, err := pollForAgentMessages(t, pollCtx, threadsClient, runnerClient, threadID, agentID, labels, sentMessageTime, expectedBodies)
	if err != nil {
		logShellToolExecutionDiagnostics(t, startTimeMinNs, orgID, threadID)
		t.Fatalf("wait for agent messages: %v", err)
	}
	if len(agentMessages) != len(expectedBodies) {
		t.Fatalf("expected %d agent messages, got %d", len(expectedBodies), len(agentMessages))
	}
	for index, msg := range agentMessages {
		body := msg.GetBody()
		if body != expectedBodies[index] {
			t.Fatalf("expected agent message %d body %q, got %q", index, expectedBodies[index], body)
		}
	}
}

func pollForAgentMessages(
	t *testing.T,
	ctx context.Context,
	threadsClient threadsv1.ThreadsServiceClient,
	runnerClient runnerv1.RunnerServiceClient,
	threadID string,
	agentID string,
	labels map[string]string,
	minCreatedAt time.Time,
	expectedBodies []string,
) ([]*threadsv1.Message, error) {
	t.Helper()
	if len(expectedBodies) == 0 {
		return nil, fmt.Errorf("expected bodies list is empty")
	}
	var agentMessages []*threadsv1.Message
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
		filtered := make([]*threadsv1.Message, 0, len(expectedBodies))
		for _, msg := range resp.GetMessages() {
			if logDiagnostics {
				logMessageDiagnostics(t, msg)
			}
			if msg.GetSenderId() != agentID {
				continue
			}
			createdAt := msg.GetCreatedAt()
			if createdAt == nil {
				return fmt.Errorf("message %s missing created_at", msg.GetId())
			}
			if !minCreatedAt.IsZero() && createdAt.AsTime().Before(minCreatedAt) {
				continue
			}
			filtered = append(filtered, msg)
		}
		sort.Slice(filtered, func(i, j int) bool {
			return messageCreatedAt(t, filtered[i]).Before(messageCreatedAt(t, filtered[j]))
		})
		matched := make([]*threadsv1.Message, 0, len(expectedBodies))
		expectedIndex := 0
		for _, msg := range filtered {
			if expectedIndex >= len(expectedBodies) {
				break
			}
			if msg.GetBody() == expectedBodies[expectedIndex] {
				matched = append(matched, msg)
				expectedIndex++
			}
		}
		if expectedIndex == len(expectedBodies) {
			agentMessages = matched
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
		return fmt.Errorf("expected %d agent messages, matched %d", len(expectedBodies), expectedIndex)
	})
	if err != nil {
		return nil, err
	}
	return agentMessages, nil
}
