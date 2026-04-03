//go:build e2e && short_idle

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

func TestWorkloadStopsAfterIdleTimeout(t *testing.T) {
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

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-idle-%s", uuid.NewString()), modelID)
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

	message := sendMessage(t, ctx, threadsClient, threadID, userID, "e2e idle message")
	messageID := message.GetId()
	if messageID == "" {
		t.Fatal("send message: missing id")
	}

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
	}

	workloadID := ""
	t.Cleanup(func() {
		if workloadID == "" {
			return
		}
		cleanupWorkload(t, ctx, runnerClient, workloadID)
	})

	pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancel()
	if err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return fmt.Errorf("expected 1 workload, got %d", len(ids))
		}
		workloadID = ids[0]
		return nil
	}); err != nil {
		t.Fatalf("wait for workload: %v", err)
	}

	ackMessages(t, ctx, threadsClient, agentID, []string{messageID})

	idleCtx, idleCancel := context.WithTimeout(ctx, 50*time.Second)
	defer idleCancel()
	if err := pollUntil(idleCtx, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			return err
		}
		if len(ids) != 0 {
			return fmt.Errorf("expected 0 workloads, got %d", len(ids))
		}
		return nil
	}); err != nil {
		t.Fatalf("wait for workload stop: %v", err)
	}
}
