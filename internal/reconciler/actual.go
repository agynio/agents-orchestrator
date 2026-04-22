package reconciler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const activeWorkloadPageSize int32 = 100

func (r *Reconciler) fetchActual(ctx context.Context) ([]*runnersv1.Workload, error) {
	tracked, err := r.listActiveWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	actual := make([]*runnersv1.Workload, 0, len(tracked))
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			log.Printf("reconciler: warn: workload %s missing runner id", workload.GetMeta().GetId())
			continue
		}
		actual = append(actual, workload)
	}
	return actual, nil
}

func (r *Reconciler) listActiveWorkloads(ctx context.Context) ([]*runnersv1.Workload, error) {
	orgAgents, err := r.listOrganizationAgents(ctx)
	if err != nil {
		return nil, err
	}
	active := []*runnersv1.Workload{}
	if len(orgAgents) == 0 {
		return active, nil
	}
	for _, orgAgent := range orgAgents {
		callCtx := r.agentContext(ctx, orgAgent.agentID)
		orgIDCopy := orgAgent.orgID
		pageToken := ""
		for {
			resp, err := r.runners.ListWorkloads(callCtx, &runnersv1.ListWorkloadsRequest{
				PageSize:       activeWorkloadPageSize,
				PageToken:      pageToken,
				OrganizationId: &orgIDCopy,
				Statuses: []runnersv1.WorkloadStatus{
					runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
					runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
					runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPING,
				},
			})
			if err != nil {
				return nil, fmt.Errorf("list workloads: %w", err)
			}
			for _, workload := range resp.GetWorkloads() {
				if workload == nil {
					return nil, fmt.Errorf("workload is nil")
				}
				meta := workload.GetMeta()
				if meta == nil {
					return nil, fmt.Errorf("workload meta missing")
				}
				if meta.GetId() == "" {
					return nil, fmt.Errorf("workload meta id missing")
				}
				active = append(active, workload)
			}
			pageToken = resp.GetNextPageToken()
			if pageToken == "" {
				break
			}
		}
	}
	return active, nil
}

type organizationAgent struct {
	orgID   string
	agentID uuid.UUID
}

func (r *Reconciler) listOrganizationAgents(ctx context.Context) ([]organizationAgent, error) {
	agents, err := r.listAgents(ctx)
	if err != nil {
		return nil, err
	}
	orgAgents := make(map[string]uuid.UUID)
	for _, agent := range agents {
		if agent == nil {
			return nil, fmt.Errorf("agent is nil")
		}
		agentID := strings.TrimSpace(agent.GetMeta().GetId())
		if agentID == "" {
			return nil, fmt.Errorf("agent meta id missing")
		}
		parsedAgentID, err := uuidutil.ParseUUID(agentID, "agent.meta.id")
		if err != nil {
			return nil, err
		}
		orgID := strings.TrimSpace(agent.GetOrganizationId())
		if orgID == "" {
			return nil, fmt.Errorf("agent organization_id missing")
		}
		parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent.organization_id")
		if err != nil {
			return nil, err
		}
		orgIDValue := parsedOrgID.String()
		if _, ok := orgAgents[orgIDValue]; ok {
			continue
		}
		orgAgents[orgIDValue] = parsedAgentID
	}
	orgIDs := make([]string, 0, len(orgAgents))
	for orgID := range orgAgents {
		orgIDs = append(orgIDs, orgID)
	}
	sort.Strings(orgIDs)
	result := make([]organizationAgent, 0, len(orgIDs))
	for _, orgID := range orgIDs {
		result = append(result, organizationAgent{
			orgID:   orgID,
			agentID: orgAgents[orgID],
		})
	}
	return result, nil
}
