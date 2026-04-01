package reconciler

import (
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
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
