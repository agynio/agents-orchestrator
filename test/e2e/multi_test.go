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

func TestMultipleAgentsSeparateThreads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpoint, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agentA := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-multi-a-%s", uuid.NewString()), modelID, orgID)
	agentB := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-multi-b-%s", uuid.NewString()), modelID, orgID)
	agentAID := agentA.GetMeta().GetId()
	agentBID := agentB.GetMeta().GetId()
	if agentAID == "" || agentBID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentAID) })
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentBID) })
	createAgentEnv(t, ctx, agentsClient, agentAID, "LLM_API_TOKEN", token)
	createAgentEnv(t, ctx, agentsClient, agentBID, "LLM_API_TOKEN", token)

	threadA := createThread(t, ctx, threadsClient, []string{identityID, agentAID})
	threadB := createThread(t, ctx, threadsClient, []string{identityID, agentBID})
	threadAID := threadA.GetId()
	threadBID := threadB.GetId()
	if threadAID == "" || threadBID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadAID) })
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadBID) })

	sendMessage(t, ctx, threadsClient, threadAID, identityID, "multi agent message a")
	sendMessage(t, ctx, threadsClient, threadBID, identityID, "multi agent message b")

	labelsA := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentAID,
		labelThreadID:  threadAID,
	}
	labelsB := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentBID,
		labelThreadID:  threadBID,
	}

	workloadAID := ""
	workloadBID := ""
	t.Cleanup(func() {
		if workloadAID != "" {
			cleanupWorkload(t, ctx, runnerClient, workloadAID)
		}
	})
	t.Cleanup(func() {
		if workloadBID != "" {
			cleanupWorkload(t, ctx, runnerClient, workloadBID)
		}
	})

	pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancel()
	if err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labelsA)
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return fmt.Errorf("expected 1 workload for agent A, got %d", len(ids))
		}
		workloadAID = ids[0]
		return nil
	}); err != nil {
		t.Fatalf("wait for workload A: %v", err)
	}

	pollCtxB, pollCancelB := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancelB()
	if err := pollUntil(pollCtxB, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labelsB)
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return fmt.Errorf("expected 1 workload for agent B, got %d", len(ids))
		}
		workloadBID = ids[0]
		return nil
	}); err != nil {
		t.Fatalf("wait for workload B: %v", err)
	}

	if workloadAID == workloadBID {
		t.Fatalf("expected distinct workloads, got %s", workloadAID)
	}

	labelsRespA, err := getWorkloadLabels(ctx, runnerClient, workloadAID)
	if err != nil {
		t.Fatalf("get labels for workload A: %v", err)
	}
	labelsRespB, err := getWorkloadLabels(ctx, runnerClient, workloadBID)
	if err != nil {
		t.Fatalf("get labels for workload B: %v", err)
	}
	assertLabel(t, labelsRespA, labelManagedBy, managedByValue)
	assertLabel(t, labelsRespA, labelAgentID, agentAID)
	assertLabel(t, labelsRespA, labelThreadID, threadAID)
	assertLabel(t, labelsRespB, labelManagedBy, managedByValue)
	assertLabel(t, labelsRespB, labelAgentID, agentBID)
	assertLabel(t, labelsRespB, labelThreadID, threadBID)
}

func TestSameAgentMultipleThreads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
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

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpoint, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-multi-thread-%s", uuid.NewString()), modelID, orgID)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	threadA := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadB := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadAID := threadA.GetId()
	threadBID := threadB.GetId()
	if threadAID == "" || threadBID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadAID) })
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadBID) })

	sendMessage(t, ctx, threadsClient, threadAID, identityID, "multi thread message a")
	sendMessage(t, ctx, threadsClient, threadBID, identityID, "multi thread message b")

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
	}

	workloadIDs := []string{}
	t.Cleanup(func() {
		for _, workloadID := range workloadIDs {
			cleanupWorkload(t, ctx, runnerClient, workloadID)
		}
	})

	pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancel()
	if err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			return err
		}
		if len(ids) != 2 {
			return fmt.Errorf("expected 2 workloads, got %d", len(ids))
		}
		workloadIDs = ids
		return nil
	}); err != nil {
		t.Fatalf("wait for workloads: %v", err)
	}

	threadIDs := map[string]bool{
		threadAID: true,
		threadBID: true,
	}
	foundThreads := map[string]bool{}
	for _, workloadID := range workloadIDs {
		labelsResp, err := getWorkloadLabels(ctx, runnerClient, workloadID)
		if err != nil {
			t.Fatalf("get labels for workload %s: %v", workloadID, err)
		}
		assertLabel(t, labelsResp, labelManagedBy, managedByValue)
		assertLabel(t, labelsResp, labelAgentID, agentID)
		threadID := labelsResp[labelThreadID]
		if !threadIDs[threadID] {
			t.Fatalf("unexpected thread id label %q", threadID)
		}
		foundThreads[threadID] = true
	}
	if len(foundThreads) != 2 {
		t.Fatalf("expected workloads for two threads, got %d", len(foundThreads))
	}
}
