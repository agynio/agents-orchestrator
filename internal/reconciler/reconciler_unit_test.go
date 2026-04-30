package reconciler

import (
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRunnerStatus(t *testing.T) {
	cases := []struct {
		name    string
		input   runnerv1.WorkloadStatus
		want    runnersv1.WorkloadStatus
		wantErr bool
	}{
		{
			name:    "unspecified",
			input:   runnerv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			want:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			wantErr: true,
		},
		{
			name:  "starting",
			input: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
			want:  runnersv1.WorkloadStatus_WORKLOAD_STATUS_STARTING,
		},
		{
			name:  "running",
			input: runnerv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
			want:  runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING,
		},
		{
			name:  "stopped",
			input: runnerv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
			want:  runnersv1.WorkloadStatus_WORKLOAD_STATUS_STOPPED,
		},
		{
			name:  "failed",
			input: runnerv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
			want:  runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED,
		},
		{
			name:    "unknown",
			input:   runnerv1.WorkloadStatus(99),
			want:    runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED,
			wantErr: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := runnerStatus(testCase.input)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != testCase.want {
				t.Fatalf("expected %v, got %v", testCase.want, got)
			}
		})
	}
}

func TestBuildContainers(t *testing.T) {
	t.Run("nil containers", func(t *testing.T) {
		request := &runnerv1.StartWorkloadRequest{}
		resp := &runnerv1.StartWorkloadResponse{}
		containers := buildContainers(request, resp)
		if containers != nil {
			t.Fatalf("expected nil containers, got %v", containers)
		}
	})

	t.Run("populates containers", func(t *testing.T) {
		request := &runnerv1.StartWorkloadRequest{
			Main: &runnerv1.ContainerSpec{
				Name:  "main",
				Image: "main-image",
			},
			Sidecars: []*runnerv1.ContainerSpec{
				{
					Name:  "sidecar-a",
					Image: "sidecar-image",
				},
			},
		}
		resp := &runnerv1.StartWorkloadResponse{
			Containers: &runnerv1.WorkloadContainers{
				Main: "main-id",
				Sidecars: []*runnerv1.SidecarInstance{
					{Name: "sidecar-a", Id: "sidecar-1"},
					{Name: "sidecar-b", Id: "sidecar-2"},
					nil,
					{Name: "sidecar-c", Id: ""},
				},
			},
		}

		containers := buildContainers(request, resp)
		if len(containers) != 3 {
			t.Fatalf("expected 3 containers, got %d", len(containers))
		}

		main := containers[0]
		if main.GetContainerId() != "main-id" {
			t.Fatalf("unexpected main container id: %s", main.GetContainerId())
		}
		if main.GetName() != "main" {
			t.Fatalf("unexpected main container name: %s", main.GetName())
		}
		if main.GetImage() != "main-image" {
			t.Fatalf("unexpected main container image: %s", main.GetImage())
		}
		if main.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
			t.Fatalf("unexpected main container role: %v", main.GetRole())
		}
		if main.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
			t.Fatalf("unexpected main container status: %v", main.GetStatus())
		}

		sidecarA := containers[1]
		if sidecarA.GetContainerId() != "sidecar-1" {
			t.Fatalf("unexpected sidecar-a container id: %s", sidecarA.GetContainerId())
		}
		if sidecarA.GetName() != "sidecar-a" {
			t.Fatalf("unexpected sidecar-a container name: %s", sidecarA.GetName())
		}
		if sidecarA.GetImage() != "sidecar-image" {
			t.Fatalf("unexpected sidecar-a container image: %s", sidecarA.GetImage())
		}
		if sidecarA.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
			t.Fatalf("unexpected sidecar-a container role: %v", sidecarA.GetRole())
		}
		if sidecarA.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
			t.Fatalf("unexpected sidecar-a container status: %v", sidecarA.GetStatus())
		}

		sidecarB := containers[2]
		if sidecarB.GetContainerId() != "sidecar-2" {
			t.Fatalf("unexpected sidecar-b container id: %s", sidecarB.GetContainerId())
		}
		if sidecarB.GetName() != "sidecar-b" {
			t.Fatalf("unexpected sidecar-b container name: %s", sidecarB.GetName())
		}
		if sidecarB.GetImage() != "" {
			t.Fatalf("expected empty sidecar-b image, got %s", sidecarB.GetImage())
		}
		if sidecarB.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
			t.Fatalf("unexpected sidecar-b container role: %v", sidecarB.GetRole())
		}
		if sidecarB.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
			t.Fatalf("unexpected sidecar-b container status: %v", sidecarB.GetStatus())
		}
	})
}

func TestMapRunnerContainers(t *testing.T) {
	start := timestamppb.New(time.Date(2024, time.January, 2, 3, 4, 5, 0, time.UTC))
	finish := timestamppb.New(time.Date(2024, time.January, 2, 4, 5, 6, 0, time.UTC))
	reason := "Completed"
	message := "done"
	exitCode := int32(0)

	containers, err := mapRunnerContainers([]*runnerv1.WorkloadContainer{
		{
			ContainerId:  "init-id",
			Name:         "init",
			Role:         runnerv1.ContainerRole_CONTAINER_ROLE_INIT,
			Image:        "init-image",
			Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_TERMINATED,
			Reason:       &reason,
			Message:      &message,
			ExitCode:     &exitCode,
			RestartCount: 2,
			StartedAt:    start,
			FinishedAt:   finish,
		},
		{
			ContainerId:  "main-id",
			Name:         "main",
			Role:         runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
			Image:        "main-image",
			Status:       runnerv1.ContainerStatus_CONTAINER_STATUS_RUNNING,
			RestartCount: 1,
			StartedAt:    start,
		},
		{
			ContainerId: "sidecar-id",
			Name:        "sidecar",
			Role:        runnerv1.ContainerRole_CONTAINER_ROLE_SIDECAR,
			Image:       "sidecar-image",
			Status:      runnerv1.ContainerStatus_CONTAINER_STATUS_WAITING,
		},
	})
	if err != nil {
		t.Fatalf("map containers: %v", err)
	}
	if len(containers) != 3 {
		t.Fatalf("expected 3 containers, got %d", len(containers))
	}

	initContainer := containers[0]
	if initContainer.GetContainerId() != "init-id" {
		t.Fatalf("unexpected init container id: %s", initContainer.GetContainerId())
	}
	if initContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_INIT {
		t.Fatalf("unexpected init container role: %v", initContainer.GetRole())
	}
	if initContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_TERMINATED {
		t.Fatalf("unexpected init container status: %v", initContainer.GetStatus())
	}
	if initContainer.GetReason() != reason || initContainer.Reason == nil {
		t.Fatalf("unexpected init container reason: %v", initContainer.GetReason())
	}
	if initContainer.GetMessage() != message || initContainer.Message == nil {
		t.Fatalf("unexpected init container message: %v", initContainer.GetMessage())
	}
	if initContainer.GetExitCode() != exitCode || initContainer.ExitCode == nil {
		t.Fatalf("unexpected init container exit code: %v", initContainer.GetExitCode())
	}
	if initContainer.GetRestartCount() != 2 {
		t.Fatalf("unexpected init restart count: %v", initContainer.GetRestartCount())
	}
	if initContainer.GetStartedAt().AsTime() != start.AsTime() {
		t.Fatalf("unexpected init started_at")
	}
	if initContainer.GetFinishedAt().AsTime() != finish.AsTime() {
		t.Fatalf("unexpected init finished_at")
	}

	mainContainer := containers[1]
	if mainContainer.GetName() != "main" {
		t.Fatalf("unexpected main container name: %s", mainContainer.GetName())
	}
	if mainContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_MAIN {
		t.Fatalf("unexpected main container role: %v", mainContainer.GetRole())
	}
	if mainContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_RUNNING {
		t.Fatalf("unexpected main container status: %v", mainContainer.GetStatus())
	}
	if mainContainer.GetRestartCount() != 1 {
		t.Fatalf("unexpected main restart count: %v", mainContainer.GetRestartCount())
	}

	sidecarContainer := containers[2]
	if sidecarContainer.GetRole() != runnersv1.ContainerRole_CONTAINER_ROLE_SIDECAR {
		t.Fatalf("unexpected sidecar container role: %v", sidecarContainer.GetRole())
	}
	if sidecarContainer.GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
		t.Fatalf("unexpected sidecar container status: %v", sidecarContainer.GetStatus())
	}
}

func TestMapRunnerContainersUnspecifiedStatus(t *testing.T) {
	containers, err := mapRunnerContainers([]*runnerv1.WorkloadContainer{
		{
			ContainerId: "main-id",
			Name:        "main",
			Role:        runnerv1.ContainerRole_CONTAINER_ROLE_MAIN,
			Image:       "main-image",
			Status:      runnerv1.ContainerStatus_CONTAINER_STATUS_UNSPECIFIED,
		},
	})
	if err != nil {
		t.Fatalf("map containers: %v", err)
	}
	if len(containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(containers))
	}
	if containers[0].GetStatus() != runnersv1.ContainerStatus_CONTAINER_STATUS_WAITING {
		t.Fatalf("unexpected container status: %v", containers[0].GetStatus())
	}
}
