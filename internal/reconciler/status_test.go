package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
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

func TestUpdateWorkloadStatusesCleansUpTerminal(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	calls := []string{}
	runner := &fakeRunnerClient{
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			calls = append(calls, "inspect")
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{StateStatus: k8sPhaseSucceeded}, nil
		},
		stopWorkload: func(_ context.Context, req *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			calls = append(calls, "stop")
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}
	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			calls = append(calls, "dial")
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}
	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			calls = append(calls, "list")
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING},
			}}, nil
		},
		updateWorkloadStatus: func(_ context.Context, req *runnersv1.UpdateWorkloadStatusRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error) {
			calls = append(calls, "update")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED {
				return nil, errors.New("unexpected workload status")
			}
			return &runnersv1.UpdateWorkloadStatusResponse{}, nil
		},
		deleteWorkload: func(_ context.Context, req *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			calls = append(calls, "delete")
			if req.GetId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
	})

	if err := reconciler.updateWorkloadStatuses(ctx); err != nil {
		t.Fatalf("update workload statuses: %v", err)
	}

	expectedCalls := []string{"list", "dial", "inspect", "update", "stop", "delete"}
	if !reflect.DeepEqual(calls, expectedCalls) {
		t.Fatalf("unexpected call order: %v", calls)
	}
}

func TestUpdateWorkloadStatusesSkipsCleanupForNonTerminal(t *testing.T) {
	ctx := context.Background()
	runnerID := "runner-1"
	workloadID := "workload-1"

	stopCalled := false
	deleteCalled := false
	updateCalled := false

	runner := &fakeRunnerClient{
		inspectWorkload: func(_ context.Context, req *runnerv1.InspectWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.InspectWorkloadResponse, error) {
			if req.GetWorkloadId() != workloadID {
				return nil, errors.New("unexpected workload id")
			}
			return &runnerv1.InspectWorkloadResponse{StateStatus: dockerStateRunning}, nil
		},
		stopWorkload: func(_ context.Context, _ *runnerv1.StopWorkloadRequest, _ ...grpc.CallOption) (*runnerv1.StopWorkloadResponse, error) {
			stopCalled = true
			return &runnerv1.StopWorkloadResponse{}, nil
		},
	}

	runnerDialer := &fakeRunnerDialer{
		dial: func(_ context.Context, id string) (runnerv1.RunnerServiceClient, error) {
			if id != runnerID {
				return nil, errors.New("unexpected runner id")
			}
			return runner, nil
		},
	}

	runners := &fakeRunnersClient{
		listWorkloads: func(_ context.Context, _ *runnersv1.ListWorkloadsRequest, _ ...grpc.CallOption) (*runnersv1.ListWorkloadsResponse, error) {
			return &runnersv1.ListWorkloadsResponse{Workloads: []*runnersv1.Workload{
				{Meta: &runnersv1.EntityMeta{Id: workloadID}, RunnerId: runnerID, Status: runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING},
			}}, nil
		},
		updateWorkloadStatus: func(_ context.Context, req *runnersv1.UpdateWorkloadStatusRequest, _ ...grpc.CallOption) (*runnersv1.UpdateWorkloadStatusResponse, error) {
			updateCalled = true
			if req.GetStatus() != runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING {
				return nil, errors.New("unexpected workload status")
			}
			return &runnersv1.UpdateWorkloadStatusResponse{}, nil
		},
		deleteWorkload: func(_ context.Context, _ *runnersv1.DeleteWorkloadRequest, _ ...grpc.CallOption) (*runnersv1.DeleteWorkloadResponse, error) {
			deleteCalled = true
			return &runnersv1.DeleteWorkloadResponse{}, nil
		},
	}

	reconciler := newTestReconciler(Config{
		RunnerDialer: runnerDialer,
		Runners:      runners,
	})

	if err := reconciler.updateWorkloadStatuses(ctx); err != nil {
		t.Fatalf("update workload statuses: %v", err)
	}
	if !updateCalled {
		t.Fatal("expected update workload status call")
	}
	if stopCalled {
		t.Fatal("expected no stop workload call")
	}
	if deleteCalled {
		t.Fatal("expected no delete workload call")
	}
}
