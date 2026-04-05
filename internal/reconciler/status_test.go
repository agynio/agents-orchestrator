package reconciler

import (
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

func TestWorkloadStatusFromInspect(t *testing.T) {
	cases := []struct {
		name    string
		resp    *runnerv1.InspectWorkloadResponse
		want    runnersv1.WorkloadStatus
		wantErr bool
	}{
		{
			name: "running flag wins",
			resp: &runnerv1.InspectWorkloadResponse{
				StateRunning: true,
				StateStatus:  dockerStateExited,
			},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		},
		{
			name: "status running",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateRunning},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		},
		{
			name: "status created",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateCreated},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		},
		{
			name: "status restarting",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateRestarting},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		},
		{
			name: "status starting",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateStarting},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		},
		{
			name: "status paused",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStatePaused},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		},
		{
			name: "phase pending",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: k8sPhasePending},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		},
		{
			name: "phase succeeded",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: k8sPhaseSucceeded},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name: "phase failed",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: k8sPhaseFailed},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		},
		{
			name:    "phase unknown",
			resp:    &runnerv1.InspectWorkloadResponse{StateStatus: k8sPhaseUnknown},
			want:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			wantErr: true,
		},
		{
			name: "status exited",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateExited},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name: "status dead",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateDead},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name: "status removing",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateRemoving},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name: "status stopped",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateStopped},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name: "status empty",
			resp: &runnerv1.InspectWorkloadResponse{StateStatus: ""},
			want: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name:    "nil response",
			resp:    nil,
			want:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			wantErr: true,
		},
		{
			name:    "status unknown",
			resp:    &runnerv1.InspectWorkloadResponse{StateStatus: "missing"},
			want:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			wantErr: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := workloadStatusFromInspect(testCase.resp)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("expected %v, got %v", testCase.want, got)
			}
		})
	}
}
