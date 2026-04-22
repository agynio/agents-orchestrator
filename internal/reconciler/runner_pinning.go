package reconciler

import (
	"context"
	"fmt"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const volumesByThreadPageSize int32 = 100

func (r *Reconciler) pinnedRunnerForThread(ctx context.Context, threadID string) (string, error) {
	if threadID == "" {
		return "", fmt.Errorf("thread id missing")
	}
	callCtx := r.serviceContext(ctx)
	pageToken := ""
	runnerID := ""
	for {
		resp, err := r.runners.ListVolumesByThread(callCtx, &runnersv1.ListVolumesByThreadRequest{
			ThreadId:  threadID,
			PageSize:  volumesByThreadPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return "", fmt.Errorf("list volumes for thread %s: %w", threadID, err)
		}
		for _, volume := range resp.GetVolumes() {
			if volume == nil {
				return "", fmt.Errorf("volume is nil")
			}
			meta := volume.GetMeta()
			if meta == nil || meta.GetId() == "" {
				return "", fmt.Errorf("volume meta missing")
			}
			if !isPinnedVolumeStatus(volume.GetStatus()) {
				continue
			}
			volumeRunnerID := volume.GetRunnerId()
			if volumeRunnerID == "" {
				return "", fmt.Errorf("volume %s missing runner id", meta.GetId())
			}
			if runnerID == "" {
				runnerID = volumeRunnerID
				continue
			}
			if runnerID != volumeRunnerID {
				return "", fmt.Errorf("thread %s volumes span runners %s and %s", threadID, runnerID, volumeRunnerID)
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return runnerID, nil
}

func isPinnedVolumeStatus(status runnersv1.VolumeStatus) bool {
	switch status {
	case runnersv1.VolumeStatus_VOLUME_STATUS_PROVISIONING,
		runnersv1.VolumeStatus_VOLUME_STATUS_ACTIVE,
		runnersv1.VolumeStatus_VOLUME_STATUS_DEPROVISIONING:
		return true
	default:
		return false
	}
}

func (r *Reconciler) getRunnerIfEnrolled(ctx context.Context, runnerID string) (*runnersv1.Runner, bool, error) {
	if runnerID == "" {
		return nil, false, fmt.Errorf("runner id missing")
	}
	callCtx := r.serviceContext(ctx)
	resp, err := r.runners.GetRunner(callCtx, &runnersv1.GetRunnerRequest{Id: runnerID})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get runner %s: %w", runnerID, err)
	}
	runner := resp.GetRunner()
	if runner == nil {
		return nil, false, fmt.Errorf("runner %s missing", runnerID)
	}
	meta := runner.GetMeta()
	if meta == nil || meta.GetId() == "" {
		return nil, false, fmt.Errorf("runner %s missing meta", runnerID)
	}
	if runner.GetStatus() != runnersv1.RunnerStatus_RUNNER_STATUS_ENROLLED {
		return runner, false, nil
	}
	return runner, true, nil
}
