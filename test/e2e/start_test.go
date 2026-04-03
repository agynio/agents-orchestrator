//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
)

func TestWorkloadStartsOnUnackedMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	identityConn := dialGRPC(t, identityAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	identityClient := identityv1.NewIdentityServiceClient(identityConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-test-agent-start-%s", uuid.NewString()))
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })

	userID := registerUserIdentity(t, ctx, identityClient)
	thread := createThread(t, ctx, threadsClient, []string{userID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	_ = sendMessage(t, ctx, threadsClient, threadID, userID, "e2e test message")

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
	}

	pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancel()
	workloadID := ""
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

	t.Cleanup(func() { cleanupWorkload(t, ctx, runnerClient, workloadID) })

	labelsResp, err := getWorkloadLabels(ctx, runnerClient, workloadID)
	if err != nil {
		t.Fatalf("get workload labels: %v", err)
	}
	assertLabel(t, labelsResp, labelManagedBy, managedByValue)
	assertLabel(t, labelsResp, labelAgentID, agentID)
	assertLabel(t, labelsResp, labelThreadID, threadID)
}
