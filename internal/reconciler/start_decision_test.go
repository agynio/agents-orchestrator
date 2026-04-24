package reconciler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestShouldStartWorkloadUsesRunnerIdentityContext(t *testing.T) {
	ctx := context.Background()
	threadID := uuid.New()
	agentID := uuid.MustParse(testAgentID)
	listCalls := 0

	runners := &fakeRunnersClient{
		listWorkloadsByThread: func(ctx context.Context, req *runnersv1.ListWorkloadsByThreadRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
			listCalls++
			if req.GetThreadId() != threadID.String() {
				return nil, errors.New("unexpected thread id")
			}
			metadataValues, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				return nil, errors.New("missing outgoing metadata")
			}
			identityValues := metadataValues.Get(identityMetadataKey)
			if len(identityValues) != 1 || identityValues[0] != agentID.String() {
				return nil, fmt.Errorf("unexpected identity metadata: %v", identityValues)
			}
			return &runnersv1.ListWorkloadsByThreadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{Runners: runners})
	shouldStart, err := reconciler.shouldStartWorkload(ctx, AgentThread{AgentID: agentID, ThreadID: threadID}, time.Now().UTC(), map[uuid.UUID]time.Time{}, nil)
	if err != nil {
		t.Fatalf("should start workload: %v", err)
	}
	if !shouldStart {
		t.Fatal("expected start decision to allow workload")
	}
	if listCalls == 0 {
		t.Fatal("expected list workloads calls")
	}
}
