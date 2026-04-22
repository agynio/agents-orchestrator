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
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
)

func TestWorkloadStopsAfterIdleTimeout(t *testing.T) {
	idleTestTimeout := 8 * time.Minute
	workloadWaitTimeout := 90 * time.Second
	agentResponseTimeout := 2 * time.Minute
	unackedDrainTimeout := 2 * time.Minute
	idleStopTimeout := 180 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), idleTestTimeout)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)
	runnersConn := dialGRPC(t, runnersAddr)
	usersConn := dialGRPC(t, usersAddr)
	orgsConn := dialGRPC(t, orgsAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	usersClient := usersv1.NewUsersServiceClient(usersConn)
	orgsClient := organizationsv1.NewOrganizationsServiceClient(orgsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
	runnersClient := runnersv1.NewRunnersServiceClient(runnersConn)

	identityID := resolveOrCreateUser(t, ctx, usersClient)
	threadsCtx := withIdentity(ctx, identityID)
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpointCodex, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	idleTimeout := "15s"
	agent := createAgentWithIdleTimeout(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-idle-%s", uuid.NewString()), modelID, orgID, codexInitImage, idleTimeout)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	agentThreadsCtx := withIdentity(ctx, agentID)
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	agentInfoResp, err := agentsClient.GetAgent(ctx, &agentsv1.GetAgentRequest{Id: agentID})
	if err != nil {
		t.Fatalf("get agent %s: %v", agentID, err)
	}
	agentInfo := agentInfoResp.GetAgent()
	if agentInfo == nil || agentInfo.GetMeta() == nil {
		t.Fatal("get agent: nil response")
	}
	if agentInfo.IdleTimeout == nil {
		t.Logf("diagnostics: agent idle_timeout is nil (value=%q)", agentInfo.GetIdleTimeout())
	} else {
		t.Logf("diagnostics: agent idle_timeout=%q", agentInfo.GetIdleTimeout())
	}
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	thread := createThread(t, threadsCtx, threadsClient, orgID, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, threadsCtx, threadsClient, threadID) })

	message := sendMessage(t, threadsCtx, threadsClient, threadID, identityID, "hello")
	messageID := message.GetId()
	if messageID == "" {
		t.Fatal("send message: missing id")
	}
	sentMessageTime := messageCreatedAt(t, message)

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

	pollCtx, pollCancel := context.WithTimeout(ctx, workloadWaitTimeout)
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

	responseCtx, responseCancel := context.WithTimeout(threadsCtx, agentResponseTimeout)
	defer responseCancel()
	if _, err := pollForAgentResponse(t, responseCtx, threadsClient, runnerClient, threadID, agentID, labels, sentMessageTime, ""); err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}

	ackAllUnackedMessages(t, agentThreadsCtx, threadsClient, agentID)
	unackedCtx, unackedCancel := context.WithTimeout(agentThreadsCtx, unackedDrainTimeout)
	defer unackedCancel()
	if err := pollUntil(unackedCtx, pollInterval, func(ctx context.Context) error {
		messageIDs, err := listUnackedMessageIDs(ctx, threadsClient, agentID)
		if err != nil {
			return err
		}
		if len(messageIDs) == 0 {
			return nil
		}
		ackMessages(t, ctx, threadsClient, agentID, messageIDs)
		return fmt.Errorf("expected 0 unacked messages, got %d", len(messageIDs))
	}); err != nil {
		t.Fatalf("wait for unacked messages to drain: %v", err)
	}

	idleCtx, idleCancel := context.WithTimeout(ctx, idleStopTimeout)
	defer idleCancel()
	if err := pollUntil(idleCtx, pollInterval, func(ctx context.Context) error {
		logRunnersWorkloadState(t, ctx, runnersClient, workloadID)
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			return err
		}
		if len(ids) != 0 {
			return fmt.Errorf("expected 0 workloads, got %d", len(ids))
		}
		return nil
	}); err != nil {
		diagCtx, cancelDiag := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelDiag()
		logRunnersWorkloadState(t, diagCtx, runnersClient, workloadID)
		t.Fatalf("wait for workload stop: %v", err)
	}
}

func logRunnersWorkloadState(t *testing.T, ctx context.Context, client runnersv1.RunnersServiceClient, workloadID string) {
	t.Helper()
	if workloadID == "" {
		t.Log("diagnostics: runners workload id is empty")
		return
	}
	resp, err := client.GetWorkload(ctx, &runnersv1.GetWorkloadRequest{Id: workloadID})
	if err != nil {
		t.Logf("diagnostics: runners get workload %s: %v", workloadID, err)
		return
	}
	workload := resp.GetWorkload()
	if workload == nil {
		t.Logf("diagnostics: runners workload %s missing in response", workloadID)
		return
	}
	now := time.Now()
	createdAt := "-"
	meta := workload.GetMeta()
	if meta != nil {
		created := meta.GetCreatedAt()
		if created != nil {
			createdAt = created.AsTime().Format(time.RFC3339Nano)
		}
	}
	lastActivityAt := "-"
	lastActivityAge := "-"
	if lastActivity := workload.GetLastActivityAt(); lastActivity != nil {
		lastActivityTime := lastActivity.AsTime()
		lastActivityAt = lastActivityTime.Format(time.RFC3339Nano)
		lastActivityAge = now.Sub(lastActivityTime).String()
	}

	t.Logf(
		"diagnostics: runners workload=%s status=%s created_at=%s last_activity_at=%s now-last_activity_at=%s",
		workloadID,
		workload.GetStatus().String(),
		createdAt,
		lastActivityAt,
		lastActivityAge,
	)
}
