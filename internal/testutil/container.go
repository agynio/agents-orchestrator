package testutil

import runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"

func FindInitContainer(containers []*runnerv1.ContainerSpec, name string) *runnerv1.ContainerSpec {
	for _, container := range containers {
		if container.GetName() == name {
			return container
		}
	}
	return nil
}
