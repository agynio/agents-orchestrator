package reconciler

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	exposev1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/expose/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// On a runner that advertises the hibernate capability an idle workload is
// neither left running until its idle timeout nor stopped. It is hibernated:
// paused and persisted by the runner, so that it occupies no compute, and
// resumed by the next inbox item with every process where it was. The runner
// owns the mechanism; this service owns the decision in both directions,
// because only it can see that an inbox is empty, which is the one fact that
// makes a pause safe -- an agent that is quiet because it is waiting on a tool
// call is not idle.

const (
	// hibernateCapability is the catalog entry a runner reports when it
	// implements HibernateWorkload and WakeWorkload. No agent declares it; the
	// Orchestrator uses it on every workload there.
	hibernateCapability = "hibernate"
	// hibernateRetry is the least time between hibernation attempts on one
	// workload after a failed one. The idle timeout remains the stop of last
	// resort meanwhile.
	hibernateRetry = time.Minute
	// wakeAttempts is how many consecutive WakeWorkload calls one hibernated
	// workload gets before it is failed and its instance cold-starts on its
	// volumes.
	wakeAttempts = 3

	exposurePageSize int32 = 100
)

// hibernationState is the little this service remembers between cycles: when a
// workload may next be put to sleep after a failed attempt, and how many wakes
// it has been given. Both are bookkeeping about calls this process made, so
// they live here rather than on the record; a restart forgets them, which costs
// one early retry and nothing else. Every method tolerates a nil receiver so a
// Reconciler built without New still decides.
type hibernationState struct {
	mu         sync.Mutex
	retryAfter map[string]time.Time
	wakes      map[string]int
}

func newHibernationState() *hibernationState {
	return &hibernationState{retryAfter: map[string]time.Time{}, wakes: map[string]int{}}
}

// mayHibernate reports whether a workload is outside the retry window a failed
// attempt opened.
func (s *hibernationState) mayHibernate(workloadID string, now time.Time) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	retryAt, ok := s.retryAfter[workloadID]
	if !ok {
		return true
	}
	if now.Before(retryAt) {
		return false
	}
	delete(s.retryAfter, workloadID)
	return true
}

func (s *hibernationState) hibernateFailed(workloadID string, now time.Time) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retryAfter[workloadID] = now.Add(hibernateRetry)
}

// wakeAttempted counts one more WakeWorkload call and returns the total.
func (s *hibernationState) wakeAttempted(workloadID string) int {
	if s == nil {
		return 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.wakes[workloadID]++
	return s.wakes[workloadID]
}

func (s *hibernationState) wakeCount(workloadID string) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wakes[workloadID]
}

// forget drops what was remembered about a workload: it is awake, asleep for
// good, or gone.
func (s *hibernationState) forget(workloadID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.retryAfter, workloadID)
	delete(s.wakes, workloadID)
}

// runnerHibernates reports whether a runner advertises the hibernate
// capability. The cache, when given, holds the answer for the rest of a cycle:
// the question is asked once per idle workload and the runner record does not
// change within a cycle.
func (r *Reconciler) runnerHibernates(ctx context.Context, runnerID string, cache map[string]bool) (bool, error) {
	runnerID = strings.TrimSpace(runnerID)
	if runnerID == "" {
		return false, fmt.Errorf("runner id missing")
	}
	if cache != nil {
		if hibernates, ok := cache[runnerID]; ok {
			return hibernates, nil
		}
	}
	resp, err := r.runners.GetRunner(ctx, &runnersv1.GetRunnerRequest{Id: runnerID})
	if err != nil {
		return false, fmt.Errorf("get runner %s: %w", runnerID, err)
	}
	hibernates := slices.Contains(resp.GetRunner().GetCapabilities(), hibernateCapability)
	if cache != nil {
		cache[runnerID] = hibernates
	}
	return hibernates, nil
}

type hibernateOutcome int

const (
	// hibernatedWorkload: the runner holds the persisted state.
	hibernatedWorkload hibernateOutcome = iota
	// hibernateDeferred: a recent attempt failed; the next waits hibernateRetry.
	hibernateDeferred
	// hibernateKeptAwakeByExposure: an exposed port is a standing request to be
	// reachable, and a frozen guest is not. The idle timeout governs the
	// workload meanwhile, as on a runner that cannot hibernate.
	hibernateKeptAwakeByExposure
	// hibernateKeptAwakeByInbox: an item landed since the decision was made.
	hibernateKeptAwakeByInbox
)

// hibernateIdleWorkload is the main loop's hibernation of an agent workload the
// decision selected from its record: on a capable runner, through
// hibernateWorkload. It reports whether the workload went to sleep.
func (r *Reconciler) hibernateIdleWorkload(ctx context.Context, workload *runnersv1.Workload, capable map[string]bool, now time.Time) (bool, error) {
	hibernates, err := r.runnerHibernates(ctx, workload.GetRunnerId(), capable)
	if err != nil {
		return false, err
	}
	if !hibernates {
		return false, nil
	}
	outcome, err := r.hibernateWorkload(ctx, workload, now)
	if err != nil {
		return false, err
	}
	return outcome == hibernatedWorkload, nil
}

// hibernateWorkload puts a workload to sleep: UpdateWorkload(hibernating), the
// runner's HibernateWorkload, then the pull credential revoked -- nothing pulls
// while frozen, and a credential valid for the days a workload may sleep would
// be the one grant to outlive its activity. The caller has settled what the
// record alone can say (the runner's capability, the agent's state, the class's
// policy); this makes the checks only a call can make, the inbox one
// immediately before the runner call. A failure leaves the record running and
// nothing terminal written; the next attempt waits hibernateRetry, and the
// idle timeout remains the stop of last resort.
func (r *Reconciler) hibernateWorkload(ctx context.Context, workload *runnersv1.Workload, now time.Time) (hibernateOutcome, error) {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return 0, fmt.Errorf("workload missing id")
	}
	instanceID := normalizeRunnerWorkloadID(workload.GetInstanceId())
	if instanceID == "" {
		return 0, fmt.Errorf("workload %s missing instance id", workloadID)
	}
	runnerID := workload.GetRunnerId()
	if runnerID == "" {
		return 0, fmt.Errorf("workload %s missing runner id", workloadID)
	}
	if !r.hibernation.mayHibernate(workloadID, now) {
		return hibernateDeferred, nil
	}
	exposed, err := r.workloadHasActiveExposure(ctx, workloadID)
	if err != nil {
		return 0, err
	}
	if exposed {
		return hibernateKeptAwakeByExposure, nil
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return 0, fmt.Errorf("dial runner %s for workload %s: %w", runnerID, workloadID, err)
	}
	// Immediately before the runner call. A check, not a lock: an item landing
	// between here and the pause costs one wake directly after it, never an
	// item -- it is in the inbox either way, and the workload comes back
	// holding everything it had. A sandbox has no inbox.
	if workload.GetOwnerKind() != runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		waiting, err := r.instanceHasUnackedItems(ctx, workloadAgentInstanceID(workload))
		if err != nil {
			return 0, err
		}
		if waiting {
			return hibernateKeptAwakeByInbox, nil
		}
	}
	hibernating := runnersv1.WorkloadStatus_WORKLOAD_STATUS_HIBERNATING
	if _, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: workloadID, Status: &hibernating}); err != nil {
		r.hibernation.hibernateFailed(workloadID, now)
		return 0, fmt.Errorf("update workload %s to hibernating: %w", workloadID, err)
	}
	// Before the pause, for the stop that may eventually follow: a hibernated
	// sandbox that reaches its TTL is stopped without ever being thawed, and a
	// frozen container cannot be read.
	r.snapshotShellDirectories(ctx, workload)
	if err := r.hibernateRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
		r.hibernation.hibernateFailed(workloadID, now)
		// The runner could not persist it, so the record says what is true.
		running := runnersv1.WorkloadStatus_WORKLOAD_STATUS_RUNNING
		if _, revertErr := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: workloadID, Status: &running}); revertErr != nil {
			log.Printf("reconciler: revert workload %s to running after failed hibernation: %v", workloadID, revertErr)
		}
		return 0, fmt.Errorf("hibernate workload %s: %w", workloadID, err)
	}
	workload.Status = hibernating
	r.hibernation.forget(workloadID)
	r.revokePullCredential(ctx, workloadID)
	return hibernatedWorkload, nil
}

// workloadHasActiveExposure reports whether a port exposure stands on the
// workload. A provisioning one counts: it is the same request, not yet served.
// Without an Expose client there is nothing to ask and nothing standing.
func (r *Reconciler) workloadHasActiveExposure(ctx context.Context, workloadID string) (bool, error) {
	if r.expose == nil {
		return false, nil
	}
	pageToken := ""
	for {
		resp, err := r.expose.ListExposures(r.platformContext(ctx), &exposev1.ListExposuresRequest{
			WorkloadId: workloadID,
			PageSize:   exposurePageSize,
			PageToken:  pageToken,
		})
		if err != nil {
			return false, fmt.Errorf("list exposures for workload %s: %w", workloadID, err)
		}
		for _, exposure := range resp.GetExposures() {
			switch exposure.GetStatus() {
			case exposev1.ExposureStatus_EXPOSURE_STATUS_PROVISIONING,
				exposev1.ExposureStatus_EXPOSURE_STATUS_ACTIVE:
				return true, nil
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			return false, nil
		}
	}
}

// instanceHasUnackedItems is the inbox check, read as the platform: Agents
// admits cluster admin on the inbox reads for exactly this.
func (r *Reconciler) instanceHasUnackedItems(ctx context.Context, agentInstanceID string) (bool, error) {
	agentInstanceID = strings.TrimSpace(agentInstanceID)
	if agentInstanceID == "" {
		return false, fmt.Errorf("agent instance id missing")
	}
	resp, err := r.agents.GetUnackedInboxCount(r.platformContext(ctx), &agentsv1.GetUnackedInboxCountRequest{AgentInstanceId: agentInstanceID})
	if err != nil {
		return false, fmt.Errorf("count unacked inbox items for agent instance %s: %w", agentInstanceID, err)
	}
	return resp.GetCount() > 0, nil
}

// wakeWorkload resumes a hibernated workload where it left off: a fresh pull
// credential, UpdateWorkload(waking), the runner's WakeWorkload. The wake is
// complete when the runner reports running; reconciliation records it if the
// report is missed. WakeWorkload failing fails the workload (wake_failed), so
// that the next tick cold-starts the instance on its volumes -- one failure,
// one short backoff.
func (r *Reconciler) wakeWorkload(ctx context.Context, workload *runnersv1.Workload) error {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return fmt.Errorf("workload missing id")
	}
	instanceID := normalizeRunnerWorkloadID(workload.GetInstanceId())
	if instanceID == "" {
		return fmt.Errorf("workload %s missing instance id", workloadID)
	}
	runnerID := workload.GetRunnerId()
	if runnerID == "" {
		return fmt.Errorf("workload %s missing runner id", workloadID)
	}
	runnerClient, err := r.runnerDialer.Dial(ctx, runnerID)
	if err != nil {
		return fmt.Errorf("dial runner %s for workload %s: %w", runnerID, workloadID, err)
	}
	credentials, err := r.wakeCredentials(ctx, workload)
	if err != nil {
		return fmt.Errorf("mint pull credential for the wake of workload %s: %w", workloadID, err)
	}
	waking := runnersv1.WorkloadStatus_WORKLOAD_STATUS_WAKING
	if _, err := r.runners.UpdateWorkload(ctx, &runnersv1.UpdateWorkloadRequest{Id: workloadID, Status: &waking}); err != nil {
		return fmt.Errorf("update workload %s to waking: %w", workloadID, err)
	}
	workload.Status = waking
	r.hibernation.wakeAttempted(workloadID)
	if err := r.wakeRunnerWorkload(ctx, runnerClient, instanceID, credentials); err != nil {
		r.failWake(ctx, runnerClient, workload, instanceID, err.Error())
		return fmt.Errorf("wake workload %s: %w", workloadID, err)
	}
	return nil
}

// failWake records a wake that did not take and releases what the workload
// held: the record goes terminal with wake_failed, the runner discards the
// persisted state, and the identity and the fresh credential go with it. The
// ordinary Start Decision cold-starts the instance on its volumes next.
func (r *Reconciler) failWake(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, workload *runnersv1.Workload, instanceID string, message string) {
	workloadID := workload.GetMeta().GetId()
	if workloadID == "" {
		return
	}
	r.markWorkloadFailed(ctx, workloadID, stringPtr(instanceID), runnersv1.WorkloadFailureReason_WORKLOAD_FAILURE_REASON_WAKE_FAILED, message, nil)
	workload.Status = runnersv1.WorkloadStatus_WORKLOAD_STATUS_FAILED
	if runnerClient != nil && instanceID != "" {
		if err := r.stopRunnerWorkload(ctx, runnerClient, instanceID); err != nil {
			log.Printf("reconciler: discard workload %s on runner after failed wake: %v", workloadID, err)
		}
	}
	r.revokePullCredential(ctx, workloadID)
	if r.zitiMgmt != nil && workload.GetZitiIdentityId() != "" {
		if err := r.deleteIdentity(ctx, workload.GetZitiIdentityId()); err != nil {
			log.Printf("reconciler: delete ziti identity %s after failed wake of workload %s: %v", workload.GetZitiIdentityId(), workloadID, err)
		}
	}
	r.hibernation.forget(workloadID)
}

// wakeCredentials mints the credential a wake carries. The one the workload
// started with was revoked when it went to sleep; this one covers the same
// images, resolved again from the class or the sandbox's environment rather
// than kept for the days a workload may sleep.
func (r *Reconciler) wakeCredentials(ctx context.Context, workload *runnersv1.Workload) ([]*runnerv1.ImagePullCredential, error) {
	if r.imageProxy == nil || r.imageProxyHost == "" || r.assembler == nil {
		return nil, nil
	}
	workloadID := workload.GetMeta().GetId()
	grant, err := r.wakeImageGrant(ctx, workload)
	if err != nil {
		return nil, err
	}
	return r.mintPullCredentialFor(ctx, workloadID, grant.OrganizationID, grant.ImageIDs)
}

func (r *Reconciler) wakeImageGrant(ctx context.Context, workload *runnersv1.Workload) (*assembler.ImageGrant, error) {
	if workload.GetOwnerKind() == runnersv1.RuntimeOwnerKind_RUNTIME_OWNER_KIND_SANDBOX {
		sandboxID := strings.TrimSpace(workload.GetOwnerId())
		if sandboxID == "" {
			return nil, fmt.Errorf("sandbox workload %s owner id missing", workload.GetMeta().GetId())
		}
		resp, err := r.agents.GetSandbox(ctx, &agentsv1.GetSandboxRequest{Ref: &agentsv1.GetSandboxRequest_Id{Id: sandboxID}})
		if err != nil {
			return nil, fmt.Errorf("get sandbox %s: %w", sandboxID, err)
		}
		return r.assembler.SandboxImageGrant(ctx, resp.GetSandbox())
	}
	agentID, err := uuidutil.ParseUUID(workloadAgentClassID(workload), "workload.agent_class_id")
	if err != nil {
		return nil, err
	}
	return r.assembler.AgentImageGrant(ctx, agentID)
}

func (r *Reconciler) hibernateRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string) error {
	return callRunnerWorkload(instanceID, func(id string) error {
		_, err := runnerClient.HibernateWorkload(ctx, &runnerv1.HibernateWorkloadRequest{WorkloadId: id})
		return err
	})
}

func (r *Reconciler) wakeRunnerWorkload(ctx context.Context, runnerClient runnerv1.RunnerServiceClient, instanceID string, credentials []*runnerv1.ImagePullCredential) error {
	return callRunnerWorkload(instanceID, func(id string) error {
		_, err := runnerClient.WakeWorkload(ctx, &runnerv1.WakeWorkloadRequest{WorkloadId: id, ImagePullCredentials: credentials})
		return err
	})
}

// callRunnerWorkload addresses a workload the way the stop does: by the id the
// runner handed back, and by the prefixed pod name when that one is not found.
// Not found both ways is the first error -- the runner does not hold it.
func callRunnerWorkload(instanceID string, call func(id string) error) error {
	err := call(instanceID)
	if err == nil || status.Code(err) != codes.NotFound {
		return err
	}
	if _, parseErr := uuid.Parse(instanceID); parseErr != nil {
		return err
	}
	prefixedErr := call(runnerWorkloadPrefix + instanceID)
	if prefixedErr == nil {
		return nil
	}
	if status.Code(prefixedErr) != codes.NotFound {
		return prefixedErr
	}
	return err
}

// stopHibernatedIfInstanceGone stops a sleeping workload nothing will ever
// wake: its instance is paused or terminated, or no longer exists. The stop
// flow discards the persisted state and keeps the volumes, as any stop does.
func (r *Reconciler) stopHibernatedIfInstanceGone(ctx context.Context, workload *runnersv1.Workload) bool {
	agentInstanceID := strings.TrimSpace(workloadAgentInstanceID(workload))
	if agentInstanceID == "" {
		return false
	}
	resp, err := r.agents.GetInstance(ctx, &agentsv1.GetInstanceRequest{Id: agentInstanceID})
	switch {
	case err != nil && status.Code(err) == codes.NotFound:
	case err != nil:
		log.Printf("reconciler: get agent instance %s behind hibernated workload %s: %v", agentInstanceID, workload.GetMeta().GetId(), err)
		return false
	default:
		switch resp.GetInstance().GetState() {
		case agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_PAUSED,
			agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_TERMINATED:
		default:
			return false
		}
	}
	log.Printf("reconciler: stopping hibernated workload %s: agent instance %s will not wake it", workload.GetMeta().GetId(), agentInstanceID)
	r.stopWorkload(ctx, workload)
	return true
}
