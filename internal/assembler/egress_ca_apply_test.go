package assembler

import (
	"strings"
	"testing"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
)

// A sandbox shell comes from the runner's Exec and inherits the container
// spec's environment, so PATH has to be there rather than in anything agynd
// assembles for a subprocess it never spawns in holder mode.
func TestWorkloadPathIncludesThePlatformBinaries(t *testing.T) {
	envs := appendEgressCAEnvVars(nil)
	var path string
	for _, env := range envs {
		if env.GetName() == "PATH" {
			path = env.GetValue()
		}
	}
	if path == "" {
		t.Fatal("PATH is not set on the container")
	}
	if !strings.HasPrefix(path, agynBinDir+":") {
		t.Fatalf("PATH = %q, want the platform directory first", path)
	}
	if !strings.Contains(path, "/usr/bin") {
		t.Fatalf("PATH = %q, want the default login set after it", path)
	}
}

// The platform's value wins, the same way SSL_CERT_FILE does: a workload that
// set its own PATH would otherwise leave the agent CLI unreachable, which is
// the failure this exists to prevent.
func TestWorkloadPathWinsOverAnEarlierValue(t *testing.T) {
	envs := appendEgressCAEnvVars([]*runnerv1.EnvVar{{Name: "PATH", Value: "/only/this"}})
	var count int
	for _, env := range envs {
		if env.GetName() != "PATH" {
			continue
		}
		count++
		if !strings.HasPrefix(env.GetValue(), agynBinDir+":") {
			t.Fatalf("PATH = %q", env.GetValue())
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
