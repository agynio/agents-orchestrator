package reconciler

import (
	"context"
	"io"
	"log"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
)

// Where each shell was, recorded immediately before a planned stop.
//
// Here because nowhere else can be. The value is only needed once the container
// is gone, and the last moment it can be read is the moment before it goes. A
// browser cannot take it: a reload, a closed lid and a dropped network all end
// a connection without giving the client a turn. And no client is necessarily
// present at all -- an idle stop happens precisely because nothing has been
// attached for a while.

const (
	// The list is one short command against a socket on the same host; a stop
	// must not wait on a container that has stopped answering.
	shellSnapshotTimeout = 8 * time.Second

	// Mirrors what agynd starts and the Terminal Proxy attaches to. Named here
	// rather than shared because a constant crossing three repositories to say
	// "the socket" is worse than three lines that each say it plainly.
	shellSnapshotCommand = `TMUX_TMPDIR=/agyn/run /agyn/bin/tmux -L agyn ls -F '#{session_name}	#{pane_current_path}' 2>/dev/null || true`

	workloadPodPrefix = "workload-"
)

// snapshotShellDirectories records each shell's working directory on the
// sandbox's layouts.
//
// Best-effort throughout: it is bounded by a short timeout, and every failure
// is logged and swallowed. A sandbox that lost its directories reopens its tabs
// where they were last recorded, or at the image's default; a sandbox that
// failed to stop because a bookkeeping read timed out would be a worse trade.
func (r *Reconciler) snapshotShellDirectories(ctx context.Context, workload *runnersv1.Workload) {
	if workload == nil || !workload.GetPersistentShells() {
		return
	}
	sandboxID := strings.TrimSpace(workload.GetOwnerId())
	if sandboxID == "" || workload.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, shellSnapshotTimeout)
	defer cancel()

	output, err := r.execShellList(ctx, workload)
	if err != nil {
		log.Printf("reconciler: shell snapshot for sandbox %s: %v", sandboxID, err)
		return
	}
	directories := parseShellDirectories(output)
	if len(directories) == 0 {
		return
	}

	if _, err := r.agents.SetSandboxLayoutDirectories(ctx, &agentsv1.SetSandboxLayoutDirectoriesRequest{
		SandboxId:   sandboxID,
		Directories: directories,
	}); err != nil {
		log.Printf("reconciler: record shell directories for sandbox %s: %v", sandboxID, err)
	}
}

func (r *Reconciler) execShellList(ctx context.Context, workload *runnersv1.Workload) (string, error) {
	runnerClient, err := r.runnerDialer.Dial(ctx, workload.GetRunnerId())
	if err != nil {
		return "", err
	}
	stream, err := runnerClient.Exec(ctx)
	if err != nil {
		return "", err
	}

	target := strings.TrimSpace(workload.GetInstanceId())
	if target == "" {
		target = strings.TrimSpace(workload.GetMeta().GetId())
	}
	if !strings.HasPrefix(target, workloadPodPrefix) {
		target = workloadPodPrefix + target
	}

	if err := stream.Send(&runnerv1.ExecRequest{Msg: &runnerv1.ExecRequest_Start{Start: &runnerv1.ExecStartRequest{
		TargetId:     target,
		CommandShell: shellSnapshotCommand,
		// No PTY: this is a machine reading a list, and a PTY would merge
		// stderr into it and translate line endings.
		Options: &runnerv1.ExecOptions{Tty: false, SeparateStderr: true},
	}}}); err != nil {
		return "", err
	}
	_ = stream.CloseSend()

	var out strings.Builder
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out.String(), err
		}
		switch msg := event.Event.(type) {
		case *runnerv1.ExecResponse_Stdout:
			out.Write(msg.Stdout.GetData())
		case *runnerv1.ExecResponse_Exit:
			return out.String(), nil
		}
	}
	return out.String(), nil
}

// parseShellDirectories reads the tab-separated `name<TAB>path` lines tmux was
// asked for. A line missing either half is skipped rather than guessed at: a
// directory recorded wrongly reopens a tab somewhere the person never was.
func parseShellDirectories(output string) []*agentsv1.ShellDirectory {
	var directories []*agentsv1.ShellDirectory
	for _, line := range strings.Split(output, "\n") {
		name, path, ok := strings.Cut(strings.TrimRight(line, "\r"), "\t")
		if !ok {
			continue
		}
		name, path = strings.TrimSpace(name), strings.TrimSpace(path)
		if name == "" || !strings.HasPrefix(path, "/") {
			continue
		}
		directories = append(directories, &agentsv1.ShellDirectory{ShellId: name, Cwd: path})
	}
	return directories
}
