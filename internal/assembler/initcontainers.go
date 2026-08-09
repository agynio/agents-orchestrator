package assembler

import (
	"strings"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
)

// Three images are injected into a workload, from two different places, because
// they change for different reasons: the two platform binaries ship with the
// chart and are injected unconditionally, while the agent runtime comes from the
// environment and is what decides which agent CLI an agent runs.
//
// They write disjoint paths in the shared volume, so they compose without
// coordinating beyond that layout.
const (
	agyndCLIInitName    = "agynd-cli-init"
	agynCLIInitName     = "agyn-cli-init"
	agentRuntimeInit    = "agent-runtime"
)

// platformInitContainers builds the two chart-pinned init containers every
// workload gets. They are not proxied: the proxy is itself a platform component
// and cannot serve the containers a workload needs before it is reachable.
func (a *Assembler) platformInitContainers() ([]*runnerv1.ContainerSpec, error) {
	agyndImage := strings.TrimSpace(a.cfg.AgyndCLIInitImage)
	agynImage := strings.TrimSpace(a.cfg.AgynCLIInitImage)
	if agyndImage == "" || agynImage == "" {
		// Not yet configured: the caller falls back to the agent's own init
		// image, which is the pre-split behaviour.
		return nil, nil
	}

	containers := make([]*runnerv1.ContainerSpec, 0, 2)
	for _, spec := range []struct {
		name  string
		image string
	}{
		{agyndCLIInitName, agyndImage},
		{agynCLIInitName, agynImage},
	} {
		container := &runnerv1.ContainerSpec{
			Image: spec.image,
			Name:  spec.name,
			Mounts: []*runnerv1.VolumeMount{
				{Volume: agynBinVolumeName, MountPath: agynBinMountPath},
			},
		}
		applyEgressCA(container, a.egressCACert)
		containers = append(containers, container)
	}
	return containers, nil
}

// agentRuntimeInitContainer builds the init container supplying the agent CLI.
// Its config.json tells agynd which CLI to spawn; the orchestrator reads
// neither, treating the image as an opaque reference the catalog resolves.
func (a *Assembler) agentRuntimeInitContainer(image string) *runnerv1.ContainerSpec {
	if strings.TrimSpace(image) == "" {
		return nil
	}
	container := &runnerv1.ContainerSpec{
		Image: image,
		Name:  agentRuntimeInit,
		Mounts: []*runnerv1.VolumeMount{
			{Volume: agynBinVolumeName, MountPath: agynBinMountPath},
		},
	}
	applyEgressCA(container, a.egressCACert)
	return container
}

