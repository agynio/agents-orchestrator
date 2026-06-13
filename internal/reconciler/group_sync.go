package reconciler

import (
	"context"
	"fmt"
	"sort"
	"time"

	groupsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/groups/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var (
	groupMembershipRetryInitial = time.Second
	groupMembershipRetryMax     = 30 * time.Second
)

const (
	groupMembershipAddedSubject   = "agyn.groups.membership.added"
	groupMembershipRemovedSubject = "agyn.groups.membership.removed"
	groupRoleAttributePrefix      = "group-"
	groupMembershipPageSize       = int32(100)
	groupWorkloadPageSize         = int32(100)
)

type zitiIdentityPatcher interface {
	PatchIdentityRoleAttributes(context.Context, *zitimgmtv1.PatchIdentityRoleAttributesRequest, ...grpc.CallOption) (*zitimgmtv1.PatchIdentityRoleAttributesResponse, error)
}

type groupsClient interface {
	ListMemberGroups(context.Context, *groupsv1.ListMemberGroupsRequest, ...grpc.CallOption) (*groupsv1.ListMemberGroupsResponse, error)
}

func groupRoleAttribute(groupID string) string {
	return groupRoleAttributePrefix + groupID
}

func (r *Reconciler) agentGroupRoleAttributes(ctx context.Context, agentID uuid.UUID, organizationID string) ([]string, error) {
	if r.groups == nil {
		return nil, nil
	}
	groups, err := r.listAgentGroups(ctx, agentID, organizationID)
	if err != nil {
		return nil, err
	}
	roleAttributes := make([]string, 0, len(groups))
	seen := map[string]struct{}{}
	for _, group := range groups {
		groupID := group.GetMeta().GetId()
		if groupID == "" {
			return nil, fmt.Errorf("groups list returned group without id")
		}
		attribute := groupRoleAttribute(groupID)
		if _, ok := seen[attribute]; ok {
			continue
		}
		seen[attribute] = struct{}{}
		roleAttributes = append(roleAttributes, attribute)
	}
	sort.Strings(roleAttributes)
	return roleAttributes, nil
}

func (r *Reconciler) listAgentGroups(ctx context.Context, agentID uuid.UUID, organizationID string) ([]*groupsv1.Group, error) {
	groups := []*groupsv1.Group{}
	pageToken := ""
	for {
		response, err := r.groups.ListMemberGroups(ctx, &groupsv1.ListMemberGroupsRequest{
			MemberType:     groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT,
			MemberId:       agentID.String(),
			OrganizationId: organizationID,
			PageSize:       groupMembershipPageSize,
			PageToken:      pageToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list agent groups: %w", err)
		}
		groups = append(groups, response.GetGroups()...)
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			return groups, nil
		}
	}
}

func (r *Reconciler) HandleGroupMembershipEvent(ctx context.Context, subject string, data []byte) error {
	switch subject {
	case groupMembershipAddedSubject:
		event := &groupsv1.GroupMembershipAddedEvent{}
		if err := proto.Unmarshal(data, event); err != nil {
			return fmt.Errorf("unmarshal group membership added event: %w", err)
		}
		return r.handleAgentMembershipChange(ctx, event.GetMemberType(), event.GetMemberId(), event.GetGroupId())
	case groupMembershipRemovedSubject:
		event := &groupsv1.GroupMembershipRemovedEvent{}
		if err := proto.Unmarshal(data, event); err != nil {
			return fmt.Errorf("unmarshal group membership removed event: %w", err)
		}
		return r.handleAgentMembershipChange(ctx, event.GetMemberType(), event.GetMemberId(), event.GetGroupId())
	default:
		return nil
	}
}

func (r *Reconciler) handleAgentMembershipChange(ctx context.Context, memberType groupsv1.GroupMemberType, memberID string, groupID string) error {
	if memberType != groupsv1.GroupMemberType_GROUP_MEMBER_TYPE_AGENT {
		return nil
	}
	agentID, err := uuid.Parse(memberID)
	if err != nil {
		return fmt.Errorf("parse group membership member id: %w", err)
	}
	candidateRemoveAttributes := []string{}
	if groupID != "" {
		candidateRemoveAttributes = append(candidateRemoveAttributes, groupRoleAttribute(groupID))
	}
	return r.patchLiveAgentWorkloadGroupRoles(ctx, agentID, candidateRemoveAttributes)
}

func (r *Reconciler) patchLiveAgentWorkloadGroupRoles(ctx context.Context, agentID uuid.UUID, candidateRemoveAttributes []string) error {
	workloads, err := r.listLiveAgentWorkloads(ctx, agentID)
	if err != nil {
		return err
	}
	for _, workload := range workloads {
		if err := r.patchWorkloadToCurrentGroupRoles(ctx, workload, candidateRemoveAttributes); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) listLiveAgentWorkloads(ctx context.Context, agentID uuid.UUID) ([]*runnersv1.Workload, error) {
	workloads := []*runnersv1.Workload{}
	pageToken := ""
	statuses := []runnersv1.WorkloadStatus{
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
	}
	for {
		response, err := r.runners.ListWorkloads(runnersContext(ctx), &runnersv1.ListWorkloadsRequest{
			PageSize:  groupWorkloadPageSize,
			PageToken: pageToken,
			Filter: &runnersv1.ListWorkloadsFilter{
				AgentIdIn: []string{agentID.String()},
				StatusIn:  statuses,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("list live agent workloads: %w", err)
		}
		workloads = append(workloads, response.GetWorkloads()...)
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			return workloads, nil
		}
	}
}

func (r *Reconciler) ReconcileAllAgentGroupRoles(ctx context.Context) error {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return err
	}
	workloads, err := r.listActiveWorkloads(ctx, orgIdentities)
	if err != nil {
		return err
	}
	for _, workload := range workloads {
		if err := r.patchWorkloadToCurrentGroupRoles(ctx, workload, nil); err != nil {
			return err
		}
	}
	return nil
}

func (r *Reconciler) patchWorkloadToCurrentGroupRoles(ctx context.Context, workload *runnersv1.Workload, candidateRemoveAttributes []string) error {
	if workload == nil {
		return fmt.Errorf("workload is nil")
	}
	zitiIdentityID := workload.GetZitiIdentityId()
	if zitiIdentityID == "" {
		return nil
	}
	agentID, err := uuid.Parse(workload.GetAgentId())
	if err != nil {
		return fmt.Errorf("parse workload agent id: %w", err)
	}
	desiredAttributes, err := r.agentGroupRoleAttributes(ctx, agentID, workload.GetOrganizationId())
	if err != nil {
		return err
	}
	_, err = r.zitiPatcher.PatchIdentityRoleAttributes(ctx, &zitimgmtv1.PatchIdentityRoleAttributesRequest{
		ZitiIdentityId: zitiIdentityID,
		Add:            desiredAttributes,
		Remove:         staleCandidateAttributes(desiredAttributes, candidateRemoveAttributes),
	})
	if err != nil {
		return fmt.Errorf("patch agent workload role attributes: %w", err)
	}
	return nil
}

func staleCandidateAttributes(desiredAttributes []string, candidateAttributes []string) []string {
	desired := make(map[string]struct{}, len(desiredAttributes))
	for _, attr := range desiredAttributes {
		desired[attr] = struct{}{}
	}
	remove := []string{}
	seen := map[string]struct{}{}
	for _, attr := range candidateAttributes {
		if _, ok := desired[attr]; ok {
			continue
		}
		if _, ok := seen[attr]; ok {
			continue
		}
		seen[attr] = struct{}{}
		remove = append(remove, attr)
	}
	sort.Strings(remove)
	return remove
}
