package reconciler

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
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
	orgIDs, err := r.listOrganizationIDs(ctx)
	if err != nil {
		return nil, err
	}
	active := []*runnersv1.Workload{}
	if len(orgIDs) == 0 {
		return active, nil
	}
	callCtx := r.serviceContext(ctx)
	for _, orgID := range orgIDs {
		orgIDCopy := orgID
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

func (r *Reconciler) listOrganizationIDs(ctx context.Context) ([]string, error) {
	agents, err := r.listAgents(ctx)
	if err != nil {
		return nil, err
	}
	orgIDs := make(map[string]struct{})
	for _, agent := range agents {
		if agent == nil {
			return nil, fmt.Errorf("agent is nil")
		}
		orgID := strings.TrimSpace(agent.GetOrganizationId())
		if orgID == "" {
			return nil, fmt.Errorf("agent organization_id missing")
		}
		parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent.organization_id")
		if err != nil {
			return nil, err
		}
		orgIDs[parsedOrgID.String()] = struct{}{}
	}
	result := make([]string, 0, len(orgIDs))
	for orgID := range orgIDs {
		result = append(result, orgID)
	}
	sort.Strings(result)
	return result, nil
}
