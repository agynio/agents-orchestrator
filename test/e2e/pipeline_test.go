//go:build e2e

package e2e

import (
	"context"
	"fmt"
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

func TestFullPipelineMessageResponse(t *testing.T) {
	runFullPipelineMessageResponse(t, testLLMEndpointCodex, codexInitImage, "hello", "Hi! How are you?", createLLMProvider)
}

func TestFullPipelineAgnMessageResponse(t *testing.T) {
	runFullPipelineMessageResponse(t, testLLMEndpointAgn, agnInitImage, "hi", "Hi! How are you?", createLLMProvider)
}

func TestFullPipelineClaudeMessageResponse(t *testing.T) {
	runFullPipelineMessageResponse(t, testLLMEndpointClaude, claudeInitImage, "hello", "Hi! How are you?", createLLMProviderAnthropic)
}

func TestClaudeRestoreSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
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
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createLLMProviderAnthropic(t, ctx, llmClient, testLLMEndpointClaude, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-state", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-claude-restore-%s", uuid.NewString()), modelID, orgID, claudeInitImage)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	thread := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

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

	firstMessage := sendMessage(t, ctx, threadsClient, threadID, identityID, "hello")
	firstMessageTime := messageCreatedAt(t, firstMessage)

	firstPollCtx, firstPollCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer firstPollCancel()
	firstBody, err := pollForAgentResponse(
		t,
		firstPollCtx,
		threadsClient,
		runnerClient,
		threadID,
		agentID,
		labels,
		firstMessageTime,
		"Hi! How are you?",
	)
	if err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}
	if firstBody != "Hi! How are you?" {
		t.Fatalf("expected agent response %q, got %q", "Hi! How are you?", firstBody)
	}

	secondMessage := sendMessage(t, ctx, threadsClient, threadID, identityID, "fine")
	secondMessageTime := messageCreatedAt(t, secondMessage)

	secondPollCtx, secondPollCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer secondPollCancel()
	secondBody := ""
	if err := pollUntil(secondPollCtx, pollInterval, func(ctx context.Context) error {
		resp, err := threadsClient.GetMessages(ctx, &threadsv1.GetMessagesRequest{
			ThreadId: threadID,
			PageSize: 50,
		})
		if err != nil {
			return fmt.Errorf("get messages: %w", err)
		}
		agentMessages := 0
		for _, msg := range resp.GetMessages() {
			if msg.GetSenderId() != agentID {
				continue
			}
			agentMessages++
			if agentMessages == 2 {
				createdAt := msg.GetCreatedAt()
				if createdAt == nil || createdAt.AsTime().Before(secondMessageTime) {
					return fmt.Errorf("agent response not found")
				}
				secondBody = msg.GetBody()
				return nil
			}
		}
		return fmt.Errorf("agent response not found")
	}); err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}
	if secondBody != "How can I help you?" {
		t.Fatalf("expected agent response %q, got %q", "How can I help you?", secondBody)
	}
}

func runFullPipelineMessageResponse(
	t *testing.T,
	llmEndpoint,
	initImage,
	message,
	expectedResponse string,
	createProvider llmProviderCreator,
) pipelineRun {
	t.Helper()

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
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createProvider(t, ctx, llmClient, llmEndpoint, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-pipeline-%s", uuid.NewString()), modelID, orgID, initImage)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	thread := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	sentMessage := sendMessage(t, ctx, threadsClient, threadID, identityID, message)
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

	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer pollCancel()
	agentBody, err := pollForAgentResponse(t, pollCtx, threadsClient, runnerClient, threadID, agentID, labels, sentMessageTime, expectedResponse)
	if err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}
	if agentBody != expectedResponse {
		t.Fatalf("expected agent response %q, got %q", expectedResponse, agentBody)
	}

	return pipelineRun{
		threadID:       threadID,
		startTimeMinNs: startTimeMinNs,
		agentResponse:  agentBody,
		messageText:    message,
	}
}
