//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
)

func TestNoDuplicateWorkloads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	identityClient := identityv1.NewIdentityServiceClient(dialGRPC(t, identityAddr))
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpoint, testOrganizationID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", testOrganizationID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-nodup-%s", uuid.NewString()), modelID)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	registerAgentIdentity(t, ctx, identityClient, agentID)

	userID := newUserID()
	registerIdentity(t, ctx, identityClient, userID)
	thread := createThread(t, ctx, threadsClient, []string{userID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	for i := 1; i <= 3; i++ {
		sendMessage(t, ctx, threadsClient, threadID, userID, fmt.Sprintf("msg %d", i))
	}

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
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
		if len(ids) < 1 {
			return fmt.Errorf("expected at least 1 workload, got %d", len(ids))
		}
		workloadIDs = ids
		return nil
	}); err != nil {
		t.Fatalf("wait for workload: %v", err)
	}

	time.Sleep(15 * time.Second)
	ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
	if err != nil {
		t.Fatalf("find workloads: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected 1 workload after delay, got %d", len(ids))
	}
	workloadIDs = ids
}
