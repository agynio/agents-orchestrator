package reconciler

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	groupsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/groups/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// groupsClientFunc answers ListMemberGroups with a function, so a test can act
// between a role pass's listing and its patch.
type groupsClientFunc func(context.Context, *groupsv1.ListMemberGroupsRequest, ...grpc.CallOption) (*groupsv1.ListMemberGroupsResponse, error)

func (f groupsClientFunc) ListMemberGroups(ctx context.Context, req *groupsv1.ListMemberGroupsRequest, opts ...grpc.CallOption) (*groupsv1.ListMemberGroupsResponse, error) {
	return f(ctx, req, opts...)
}

// recordingRolePatches records every patch and answers it with the error
// failFor returns for its identity, nil for success.
func recordingRolePatches(patched *[]string, failFor func(identityID string) error) func(context.Context, *zitimgmtv1.PatchIdentityRoleAttributesRequest, ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
	return func(_ context.Context, req *zitimgmtv1.PatchIdentityRoleAttributesRequest, _ ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error) {
		*patched = append(*patched, req.GetZitiIdentityId())
		if err := failFor(req.GetZitiIdentityId()); err != nil {
			return nil, err
		}
		return &zitimgmtv1.PatchIdentityRoleAttributesResponse{}, nil
	}
}

func liveWorkloadsOf(workloads ...*runnersv1.Workload) *fakeRunnersClient {
	return &fakeRunnersClient{listWorkloads: func(context.Context, *runnersv1.ListWorkloadsRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
		return &runnersv1.ListWorkloadsResponse{Workloads: workloads}, nil
	}}
}

func oneGroup() *fakeGroupsClient {
	return &fakeGroupsClient{groupsByOrg: map[string][]*groupsv1.Group{testOrganizationID: {{Meta: &groupsv1.EntityMeta{Id: "group-a"}}}}}
}

// What happened on 2026-10-31: the main loop's role pass listed a hibernated
// workload as live, and before its patch the workload reconciliation loop --
// running beside it -- found the runner no longer held the workload, failed it
// with snapshot_lost and deleted its identity. The patch answered NotFound,
// and that ended the cycle. Here the reconciliation pass runs inside the role
// pass's Groups call, which sits between its listing and its patch.
func TestRolePassToleratesAnIdentityDeletedByWorkloadReconciliation(t *testing.T) {
	h := newReconcileRowHarness(t, runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATED, false, runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED, nil)
	identityID := h.workload.GetZitiIdentityId()
	patched := []string{}
	h.zitiMgmt.patchIdentityRoleAttributes = recordingRolePatches(&patched, func(id string) error {
		if slices.Contains(h.deleted, id) {
			return status.Error(codes.NotFound, "ziti identity not found")
		}
		return nil
	})
	reconciled := false
	h.reconciler.groups = groupsClientFunc(func(context.Context, *groupsv1.ListMemberGroupsRequest, ...grpc.CallOption) (*groupsv1.ListMemberGroupsResponse, error) {
		if !reconciled {
			reconciled = true
			h.reconcile(t)
		}
		return &groupsv1.ListMemberGroupsResponse{}, nil
	})

	if err := h.reconciler.ReconcileAllAgentGroupRoles(context.Background()); err != nil {
		t.Fatalf("ReconcileAllAgentGroupRoles: %v", err)
	}
	if update := h.singleUpdate(t); update.GetFailureReason() != runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_SNAPSHOT_LOST {
		t.Fatalf("expected the workload failed with snapshot_lost, got %s", update.GetFailureReason())
	}
	assertStringSet(t, h.deleted, []string{identityID})
	assertStringSet(t, patched, []string{identityID})
}

// One workload whose patch fails is that workload's problem: the pass patches
// the rest and the cycle goes on. The next pass retries it.
func TestRolePassPatchesEveryWorkloadWhenOneFails(t *testing.T) {
	agentID := uuid.MustParse(testAgentID)
	broken := idleAgentWorkload(agentID, uuid.New(), "runner-1", time.Now().UTC())
	healthy := idleAgentWorkload(agentID, uuid.New(), "runner-1", time.Now().UTC())
	patched := []string{}
	zitiMgmt := &fakeZitiMgmtClient{patchIdentityRoleAttributes: recordingRolePatches(&patched, func(identityID string) error {
		if identityID == broken.GetZitiIdentityId() {
			return status.Error(codes.Unavailable, "controller unavailable")
		}
		return nil
	})}
	reconciler := newTestReconciler(Config{Runners: liveWorkloadsOf(broken, healthy), ZitiMgmt: zitiMgmt, Groups: oneGroup()})

	if err := reconciler.ReconcileAllAgentGroupRoles(context.Background()); err != nil {
		t.Fatalf("ReconcileAllAgentGroupRoles: %v", err)
	}
	assertStringSet(t, patched, []string{broken.GetZitiIdentityId(), healthy.GetZitiIdentityId()})
}

// An identity that is gone has nothing to patch, so a membership event about
// its agent is done rather than redelivered.
func TestGroupMembershipEventIsDoneWhenAWorkloadIdentityIsGone(t *testing.T) {
	agentID := uuid.New()
	gone := idleAgentWorkload(agentID, uuid.New(), "runner-1", time.Now().UTC())
	patched := []string{}
	zitiMgmt := &fakeZitiMgmtClient{patchIdentityRoleAttributes: recordingRolePatches(&patched, func(string) error {
		return status.Error(codes.NotFound, "ziti identity not found")
	})}
	reconciler := newTestReconciler(Config{Runners: liveWorkloadsOf(gone), ZitiMgmt: zitiMgmt, Groups: oneGroup()})
	payload := mustMarshal(t, &groupsv1.GroupMembershipAddedEvent{
		GroupId:    "group-a",
		MemberType: groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
		MemberId:   agentID.String(),
	})

	if err := reconciler.HandleGroupMembershipEvent(context.Background(), groupMembershipAddedSubject, payload); err != nil {
		t.Fatalf("expected the event handled, got %v", err)
	}
	assertStringSet(t, patched, []string{gone.GetZitiIdentityId()})
}

// A failed patch still gets the event redelivered, but not before every other
// live workload of the agent has had its own.
func TestGroupMembershipEventPatchesEveryWorkloadBeforeReportingAFailure(t *testing.T) {
	agentID := uuid.New()
	broken := idleAgentWorkload(agentID, uuid.New(), "runner-1", time.Now().UTC())
	healthy := idleAgentWorkload(agentID, uuid.New(), "runner-1", time.Now().UTC())
	patched := []string{}
	zitiMgmt := &fakeZitiMgmtClient{patchIdentityRoleAttributes: recordingRolePatches(&patched, func(identityID string) error {
		if identityID == broken.GetZitiIdentityId() {
			return status.Error(codes.Unavailable, "controller unavailable")
		}
		return nil
	})}
	reconciler := newTestReconciler(Config{Runners: liveWorkloadsOf(broken, healthy), ZitiMgmt: zitiMgmt, Groups: oneGroup()})
	payload := mustMarshal(t, &groupsv1.GroupMembershipAddedEvent{
		GroupId:    "group-a",
		MemberType: groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
		MemberId:   agentID.String(),
	})

	err := reconciler.HandleGroupMembershipEvent(context.Background(), groupMembershipAddedSubject, payload)
	if err == nil {
		t.Fatal("expected the failure reported, so the event is redelivered")
	}
	if !strings.Contains(err.Error(), broken.GetMeta().GetId()) {
		t.Fatalf("expected the error to name workload %s, got %v", broken.GetMeta().GetId(), err)
	}
	assertStringSet(t, patched, []string{broken.GetZitiIdentityId(), healthy.GetZitiIdentityId()})
}
