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
	byRunner := make(map[string][]*runnersv1.Workload)
	for _, workload := range tracked {
		runnerID := workload.GetRunnerId()
		if runnerID == "" {
			r.removeStaleWorkload(ctx, workload, "missing runner id")
			continue
		}
		byRunner[runnerID] = append(byRunner[runnerID], workload)
	}
	actual := make([]*runnersv1.Workload, 0, len(tracked))
	for runnerID, workloads := range byRunner {
		runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
		if err != nil {
			log.Printf("reconciler: warn: dial runner %s: %v", runnerID, err)
			continue
		}
		resp, err := runnerClient.FindWorkloadsByLabels(ctx, &runnerv1.FindWorkloadsByLabelsRequest{
			Labels: map[string]string{assembler.LabelManagedBy: assembler.ManagedByValue},
			All:    true,
		})
		if err != nil {
			log.Printf("reconciler: warn: list workloads for runner %s: %v", runnerID, err)
			continue
		}
		running := make(map[string]struct{}, len(resp.GetTargetIds()))
		for _, id := range resp.GetTargetIds() {
			running[id] = struct{}{}
		}
		for _, workload := range workloads {
			workloadID := workload.GetMeta().GetId()
			if _, ok := running[workloadID]; ok {
				actual = append(actual, workload)
				continue
			}
			r.removeStaleWorkload(ctx, workload, "missing on runner")
		}
	}
	return actual, nil
}

func (r *Reconciler) removeStaleWorkloads(ctx context.Context, workloads []*runnersv1.Workload, reason string) {
	for _, workload := range workloads {
		r.removeStaleWorkload(ctx, workload, reason)
	}
}

func (r *Reconciler) removeStaleWorkload(ctx context.Context, workload *runnersv1.Workload, reason string) {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		log.Printf("reconciler: warn: stale workload missing id (%s)", reason)
		return
	}
	if _, err := r.runners.DeleteWorkload(ctx, &runnersv1.DeleteWorkloadRequest{Id: workloadID}); err != nil {
		log.Printf("reconciler: warn: delete stale workload %s after %s: %v", workloadID, reason, err)
		return
	}
	log.Printf("reconciler: warn: removed stale workload %s (%s)", workloadID, reason)
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
			log.Printf("reconciler: warn: delete ziti identity %s for stale workload %s: %v", workload.GetZitiIdentityId(), workloadID, err)
		}
	}
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
