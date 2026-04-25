package reconciler

import (
	"context"
	"errors"
	"testing"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestListActiveWorkloadsFallbacksToOrgIdentityOnPermissionDenied(t *testing.T) {
	ctx := context.Background()
	workloadID := "workload-1"
	callCount := 0

	runners := &fakeRunnersClient{
		listWorkloads: func(ctx context.Context, req *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			callCount++
			if req.GetOrganizationId() != testOrganizationID {
				return nil, errors.New("unexpected organization id")
			}
			identityID, err := identityIDFromContext(ctx)
			if err != nil {
				return nil, err
			}
			switch callCount {
			case 1:
				if identityID != testClusterAdminIdentityID {
					return nil, errors.New("expected cluster admin identity")
				}
				return nil, status.Error(codes.PermissionDenied, "denied")
			case 2:
				if identityID != testAgentID {
					return nil, errors.New("expected agent identity")
				}
				return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{{Meta: &runnersv1.EntityMeta{Id: workloadID}}}}, nil
			default:
				return nil, errors.New("unexpected list workloads call")
			}
		},
	}

	reconciler := newTestReconciler(Config{Runners: runners})
	workloads, err := reconciler.listActiveWorkloads(ctx, map[string]string{testOrganizationID: testAgentID})
	if err != nil {
		t.Fatalf("list active workloads: %v", err)
	}
	if callCount != 2 {
		t.Fatalf("expected 2 list workload calls, got %d", callCount)
	}
	if len(workloads) != 1 {
		t.Fatalf("expected 1 workload, got %d", len(workloads))
	}
}

func identityIDFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return "", errors.New("missing outgoing metadata")
	}
	values := md.Get(identityMetadataKey)
	if len(values) != 1 {
		return "", errors.New("missing identity metadata")
	}
	return values[0], nil
}
