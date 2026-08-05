package assembler

import (
	"testing"

	"github.com/agynio/agents-orchestrator/internal/config"
)

func assemblerWithInitImages(agynd, agyn string) *Assembler {
	return &Assembler{cfg: &config.Config{AgyndCLIInitImage: agynd, AgynCLIInitImage: agyn}}
}

// Both platform images are injected into every workload, in the order they
// write into the shared volume.
func TestPlatformInitContainers(t *testing.T) {
	a := assemblerWithInitImages("ghcr.io/agynio/agynd-cli-init:1", "ghcr.io/agynio/agyn-cli-init:1")

	containers, err := a.platformInitContainers()
	if err != nil {
		t.Fatalf("platformInitContainers: %v", err)
	}
	if len(containers) != 2 {
		t.Fatalf("got %d containers, want 2", len(containers))
	}
	if containers[0].GetName() != agyndCLIInitName || containers[1].GetName() != agynCLIInitName {
		t.Fatalf("names = %q, %q", containers[0].GetName(), containers[1].GetName())
	}
	for _, container := range containers {
		mounts := container.GetMounts()
		if len(mounts) != 1 || mounts[0].GetMountPath() != agynBinMountPath {
			t.Fatalf("%s does not mount the shared volume", container.GetName())
		}
	}
}

// Unconfigured, the caller falls back to the agent's own init image rather than
// producing a workload with no binaries.
func TestPlatformInitContainersAreAbsentUntilConfigured(t *testing.T) {
	containers, err := assemblerWithInitImages("", "").platformInitContainers()
	if err != nil {
		t.Fatalf("platformInitContainers: %v", err)
	}
	if len(containers) != 0 {
		t.Fatalf("got %d containers, want none", len(containers))
	}
}

func TestAgentRuntimeInitContainer(t *testing.T) {
	a := assemblerWithInitImages("a", "b")

	if container := a.agentRuntimeInitContainer(""); container != nil {
		t.Fatal("a workspace-only environment gets no agent runtime container")
	}

	container := a.agentRuntimeInitContainer("proxy.example/acme/runtime-codex:1.0.0")
	if container == nil || container.GetName() != agentRuntimeInit {
		t.Fatalf("container = %+v", container)
	}
	if container.GetImage() != "proxy.example/acme/runtime-codex:1.0.0" {
		t.Fatalf("image = %q, want the proxy reference unchanged", container.GetImage())
	}
}

func TestLegacyInitContainerRequiresAnImage(t *testing.T) {
	a := assemblerWithInitImages("", "")

	if _, err := a.legacyInitContainer(""); err == nil {
		t.Fatal("expected an empty init image to be refused")
	}
	container, err := a.legacyInitContainer("ghcr.io/agynio/agent-init-codex:latest")
	if err != nil {
		t.Fatalf("legacyInitContainer: %v", err)
	}
	if container.GetName() != legacyAgentInitName {
		t.Fatalf("name = %q", container.GetName())
	}
}
