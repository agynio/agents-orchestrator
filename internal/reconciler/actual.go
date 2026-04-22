package reconciler

import (
	"context"
	"fmt"
	"log"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

const activeWorkloadPageSize int32 = 100

func (r *Reconciler) fetchActual(ctx context.Context) ([]*runnersv1.Workload, error) {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return nil, err
	}
	tracked, err := r.listActiveWorkloads(ctx, orgIdentities)
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

func (r *Reconciler) listActiveWorkloads(ctx context.Context, orgIdentities map[string]string) ([]*runnersv1.Workload, error) {
	active := []*runnersv1.Workload{}
	if len(orgIdentities) == 0 {
		return active, nil
	}
	callCtx, err := r.serviceContext(ctx)
	if err != nil {
		return nil, err
	}
	for orgID := range orgIdentities {
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
