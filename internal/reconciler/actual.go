package reconciler

import (
	"context"
	"fmt"
	"log"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
)

const activeWorkloadPageSize int32 = 100

func (r *Reconciler) fetchActual(ctx context.Context) ([]*runnersv1.Workload, error) {
	tracked, err := r.listActiveWorkloads(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := r.runner.FindWorkloadsByLabels(ctx, &runnerv1.FindWorkloadsByLabelsRequest{
		Labels: map[string]string{assembler.LabelManagedBy: assembler.ManagedByValue},
		All:    true,
	})
	if err != nil {
		return nil, err
	}
	running := make(map[string]struct{}, len(resp.GetTargetIds()))
	for _, id := range resp.GetTargetIds() {
		running[id] = struct{}{}
	}
	actual := make([]*runnersv1.Workload, 0, len(tracked))
	for _, workload := range tracked {
		workloadID := workload.GetMeta().GetId()
		if _, ok := running[workloadID]; ok {
			actual = append(actual, workload)
			continue
		}
		if _, err := r.runners.DeleteWorkload(ctx, &runnersv1.DeleteWorkloadRequest{Id: workloadID}); err != nil {
			log.Printf("reconciler: warn: delete stale workload %s: %v", workloadID, err)
			continue
		}
		log.Printf("reconciler: warn: removed stale workload %s", workloadID)
		if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
			if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
				log.Printf("reconciler: warn: delete ziti identity %s for stale workload %s: %v", workload.GetZitiIdentityId(), workloadID, err)
			}
		}
	}
	return actual, nil
}

func (r *Reconciler) listActiveWorkloads(ctx context.Context) ([]*runnersv1.Workload, error) {
	active := []*runnersv1.Workload{}
	pageToken := ""
	for {
		resp, err := r.runners.ListWorkloads(ctx, &runnersv1.ListWorkloadsRequest{
			PageSize:  activeWorkloadPageSize,
			PageToken: pageToken,
			Statuses: []runnersv1.WorkloadStatus{
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
				runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
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
	return active, nil
}
