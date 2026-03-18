//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	teamsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/teams/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/google/uuid"
)

func TestNoDuplicateWorkloads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	teamsConn := dialGRPC(t, teamsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialGRPC(t, runnerAddr)

	teamsClient := teamsv1.NewTeamsServiceClient(teamsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)

	agent := createAgent(t, ctx, teamsClient, fmt.Sprintf("e2e-test-agent-nodup-%s", uuid.NewString()))
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, teamsClient, agentID) })

	userID := newUserID()
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
