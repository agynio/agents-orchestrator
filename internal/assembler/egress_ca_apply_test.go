package assembler

import (
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
)

// A container env entry replaces the image's ENV PATH rather than extending it,
// so setting one here loses whatever the image installed -- the devcontainer's
// nix profile directories among them. Every route to /agyn/bin prepends inside
// the container, where the image's PATH is what $PATH holds.
func TestWorkloadEnvLeavesPathToTheImage(t *testing.T) {
	for _, env := range appendEgressCAEnvVars(nil) {
		if env.GetName() == "PATH" {
			t.Fatalf("PATH = %q, want the image's own to survive", env.GetValue())
		}
	}
}

// A workload that sets its own PATH keeps it: the platform no longer has a
// value of its own to prefer over it.
func TestWorkloadPathPassesThrough(t *testing.T) {
	envs := appendEgressCAEnvVars([]*runnerv1.EnvVar{{Name: "PATH", Value: "/only/this"}})
	var count int
	for _, env := range envs {
		if env.GetName() != "PATH" {
			continue
		}
		count++
		if env.GetValue() != "/only/this" {
			t.Fatalf("PATH = %q, want the workload's own value", env.GetValue())
		}
	}
	if count != 1 {
		t.Fatalf("PATH appears %d times", count)
	}
}

// Claude Code refuses to run with bypassed permissions as root, and a sandbox
// shell inherits the container spec rather than anything agynd assembled.
func TestAppendEgressCAEnvVarsMarksTheContainerASandbox(t *testing.T) {
	envs := appendEgressCAEnvVars(nil)
	for _, env := range envs {
		if env.GetName() == "IS_SANDBOX" {
			if env.GetValue() != "1" {
				t.Fatalf("IS_SANDBOX = %q, want 1", env.GetValue())
			}
			return
		}
	}
	t.Fatalf("IS_SANDBOX not set: %v", envs)
}
