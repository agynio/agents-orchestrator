package assembler

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const (
	sandboxWorkspaceVolumeName = "workspace"
	SandboxWorkspaceMountPath  = "/workspace"
	SandboxHolderMode          = "holder"
)

type SandboxAssembleResult struct {
	Request        *runnerv1.StartWorkloadRequest
	OrganizationID string
	EnvironmentID  uuid.UUID
	OwnerID        uuid.UUID
	RunnerID       string
	// Flavor names the catalog entry the workload is allocated from, and is
	// what compute is billed by.
	Flavor string
	// GrantedImageIDs are the catalog images this sandbox may pull. The
	// credential is minted against them once the workload id exists.
	GrantedImageIDs        []string
	WorkspaceVolumeID      string
	WorkspaceSizeGB        string
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
}

func (a *Assembler) AssembleSandbox(ctx context.Context, sandbox *agentsv1.Sandbox, workspaceVolumeID string) (*SandboxAssembleResult, error) {
	if sandbox == nil {
		return nil, fmt.Errorf("sandbox missing")
	}
	sandboxID, err := sandboxUUID(sandbox)
	if err != nil {
		return nil, err
	}
	environmentID, err := uuidutil.ParseUUID(sandbox.GetEnvironmentId(), "sandbox.environment_id")
	if err != nil {
		return nil, err
	}
	ownerID, err := uuidutil.ParseUUID(sandbox.GetOwnerId(), "sandbox.owner_id")
	if err != nil {
		return nil, err
	}
	workspaceVolumeID = strings.TrimSpace(workspaceVolumeID)
	if workspaceVolumeID == "" {
		return nil, fmt.Errorf("sandbox %s workspace volume id missing", sandboxID.String())
	}
	environment, err := a.fetchEnvironment(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	flavor, err := a.resolveFlavor(ctx, environment.GetRunnerId(), environment.GetFlavor())
	if err != nil {
		return nil, err
	}
	resolver := newEnvResolver(a.secrets)
	environmentEnvs, err := a.listEnvs(ctx, &agentsv1.ListEnvsRequest{EnvironmentId: environmentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list environment envs: %w", err)
	}
	environmentEnvVars, err := resolver.ResolveEnvVars(ctx, environmentEnvs)
	if err != nil {
		return nil, fmt.Errorf("resolve environment envs: %w", err)
	}
	allocatedCPU, allocatedRAM, err := flavorAllocatedResources(flavor)
	if err != nil {
		return nil, err
	}

	rewriter := newImageRewriter(a.images, a.organizations, a.cfg.ImageProxyHost)
	mainImage := environment.GetImage()
	agentRuntimeImage := ""
	// A catalog reference resolves only through the Image Proxy, which is the
	// only path a workload's images are pulled by. Skipping the lookup when it
	// is unconfigured silently dropped the environment's agent runtime, and the
	// sandbox came up with no agent CLI and nothing said so.
	if environment.GetWorkspaceImageId() != "" {
		if !rewriter.enabled() {
			return nil, fmt.Errorf("environment %s names a workspace image but the image proxy is not configured", environmentID)
		}
		mainImage, err = rewriter.Rewrite(ctx, environment.GetWorkspaceImageId(), environment.GetWorkspaceImageTag())
		if err != nil {
			return nil, fmt.Errorf("environment %s workspace image: %w", environmentID, err)
		}
	}
	if environment.GetAgentRuntimeImageId() != "" {
		if !rewriter.enabled() {
			return nil, fmt.Errorf("environment %s names an agent runtime image but the image proxy is not configured", environmentID)
		}
		agentRuntimeImage, err = rewriter.Rewrite(ctx, environment.GetAgentRuntimeImageId(), environment.GetAgentRuntimeImageTag())
		if err != nil {
			return nil, fmt.Errorf("environment %s agent runtime image: %w", environmentID, err)
		}
	}

	mainEnv := mergeEnvVars(a.baseSandboxEnvVars(sandbox, environment), environmentEnvVars, fmt.Sprintf("sandbox %s", sandboxID.String()))
	mainEnv = appendEgressCAEnvVars(mainEnv)
	main := &runnerv1.ContainerSpec{
		Image:            mainImage,
		Name:             fmt.Sprintf("sandbox-%s", sandboxID.String()[:8]),
		Cmd:              []string{agynBinBinaryPath},
		Env:              mainEnv,
		WorkingDir:       SandboxWorkspaceMountPath,
		Mounts:           []*runnerv1.VolumeMount{{Volume: sandboxWorkspaceVolumeName, MountPath: SandboxWorkspaceMountPath}, {Volume: agynBinVolumeName, MountPath: agynBinMountPath}},
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}
	// The two platform init containers go into a sandbox too, which is what
	// makes agyn available inside a plain one. The environment's agent runtime
	// follows when it names one, so a sandbox carries the same tooling as an
	// agent running there.
	initContainers, err := a.platformInitContainers()
	if err != nil {
		return nil, err
	}
	if runtimeInit := a.agentRuntimeInitContainer(agentRuntimeImage); runtimeInit != nil {
		initContainers = append(initContainers, runtimeInit)
	} else if len(initContainers) == 0 {
		legacy, err := a.legacyInitContainer(a.cfg.SandboxInitImage)
		if err != nil {
			return nil, fmt.Errorf("sandbox: %w", err)
		}
		initContainers = append(initContainers, legacy)
	}
	volumes := []*runnerv1.VolumeSpec{
		{
			Name:           sandboxWorkspaceVolumeName,
			Kind:           runnerv1.VolumeKind_VOLUME_KIND_NAMED,
			PersistentName: sandboxWorkspacePersistentName(sandboxID),
			Labels: map[string]string{
				LabelManagedBy:      ManagedByValue,
				LabelSandboxID:      sandboxID.String(),
				LabelSandboxOwnerID: ownerID.String(),
				LabelEnvironmentID:  environmentID.String(),
				LabelVolumeKey:      workspaceVolumeID,
			},
		},
		{
			Name: agynBinVolumeName,
			Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
		},
	}
	if a.cfg.ZitiEnabled {
		if _, err := gatewayHost(a.cfg.AgentGatewayAddress); err != nil {
			return nil, err
		}
		llmProxyTarget, err := zitiServiceWaitTarget(a.cfg.AgentLLMBaseURL)
		if err != nil {
			return nil, err
		}
		zitiEnroll := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       ZitiEnrollContainerName,
			Cmd:        buildZitiEnrollCommand(a.cfg.ZitiEnrollmentDNSUpstream, a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort, a.cfg.ZitiRuntimeControllerResolveHost, a.cfg.ZitiRuntimeControllerPort),
			Entrypoint: zitiEnrollEntrypoint,
			Env:        zitiEnrollEnvVars(a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort),
			Mounts:     []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
		}
		zitiSidecar := &runnerv1.ContainerSpec{
			Image:                a.cfg.ZitiSidecarImage,
			Name:                 ZitiSidecarContainerName,
			Cmd:                  buildZitiSidecarCommand(a.cfg.WorkloadDNSUpstream),
			Entrypoint:           zitiSidecarEntrypoint,
			Env:                  zitiSidecarEnvVars(a.cfg.WorkloadDNSUpstream),
			Mounts:               []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
			RequiredCapabilities: []string{zitiRequiredCapabilityNetAdmin},
			AdditionalProperties: map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways},
		}
		zitiGatewayWait := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       zitiGatewayWaitContainerName,
			Entrypoint: zitiSidecarEntrypoint,
			Cmd:        buildZitiGatewayWaitCommand(a.cfg.AgentGatewayAddress, a.cfg.WorkloadDNSUpstream),
		}
		zitiServiceWait := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       zitiServiceWaitContainerName,
			Entrypoint: zitiSidecarEntrypoint,
			Cmd:        buildZitiServiceWaitCommand(llmProxyTarget, a.cfg.WorkloadDNSUpstream),
		}
		applyEgressCA(zitiEnroll, a.egressCACert)
		applyEgressCA(zitiSidecar, a.egressCACert)
		applyEgressCA(zitiGatewayWait, a.egressCACert)
		applyEgressCA(zitiServiceWait, a.egressCACert)
		// Ziti runs before the binaries land, so the agyn-bin init containers
		// follow it rather than being replaced by it.
		initContainers = append([]*runnerv1.ContainerSpec{zitiEnroll, zitiSidecar, zitiGatewayWait, zitiServiceWait}, initContainers...)
		volumes = append(volumes, &runnerv1.VolumeSpec{Name: zitiIdentityVolumeName, Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })
	request := &runnerv1.StartWorkloadRequest{
		Main:           main,
		Volumes:        volumes,
		InitContainers: initContainers,
		InlineFiles:    a.inlineFiles(),
		AdditionalProperties: map[string]string{
			LabelKeyPrefix + LabelManagedBy:      ManagedByValue,
			LabelKeyPrefix + LabelSandboxID:      sandboxID.String(),
			LabelKeyPrefix + LabelSandboxOwnerID: ownerID.String(),
			LabelKeyPrefix + LabelEnvironmentID:  environmentID.String(),
		},
	}
	if a.cfg.ZitiEnabled {
		request.DnsConfig = &runnerv1.DnsConfig{
			Nameservers: []string{zitiDNSNameserver, a.cfg.WorkloadDNSUpstream},
			Searches:    []string{zitiDNSSearchService, zitiDNSSearchCluster},
		}
	}
	return &SandboxAssembleResult{
		Request:                request,
		OrganizationID:         sandbox.GetOrganizationId(),
		EnvironmentID:          environmentID,
		OwnerID:                ownerID,
		RunnerID:               flavor.GetRunnerId(),
		Flavor:                 flavor.GetName(),
		GrantedImageIDs:        rewriter.GrantedImageIDs(),
		WorkspaceVolumeID:      workspaceVolumeID,
		WorkspaceSizeGB:        a.cfg.SandboxWorkspaceSizeGB,
		AllocatedCPUMillicores: allocatedCPU,
		AllocatedRAMBytes:      allocatedRAM,
	}, nil
}

func sandboxUUID(sandbox *agentsv1.Sandbox) (uuid.UUID, error) {
	if sandbox == nil || sandbox.GetMeta() == nil {
		return uuid.Nil, fmt.Errorf("sandbox meta missing")
	}
	return uuidutil.ParseUUID(sandbox.GetMeta().GetId(), "sandbox.meta.id")
}

func sandboxWorkspacePersistentName(sandboxID uuid.UUID) string {
	return "sandbox-ws-" + sandboxID.String()
}

func (a *Assembler) fetchEnvironment(ctx context.Context, environmentID uuid.UUID) (*agentsv1.Environment, error) {
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := a.agents.GetEnvironment(rctx, &agentsv1.GetEnvironmentRequest{Id: environmentID.String()})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("get environment %s: %w", environmentID.String(), err)
	}
	environment := resp.GetEnvironment()
	if environment == nil {
		return nil, fmt.Errorf("environment %s missing", environmentID.String())
	}
	if environment.GetImage() == "" && environment.GetWorkspaceImageId() == "" {
		return nil, fmt.Errorf("environment %s names no workspace image", environmentID.String())
	}
	if environment.GetRunnerId() == "" {
		return nil, fmt.Errorf("environment %s runner_id is required", environmentID.String())
	}
	return environment, nil
}

// resolveFlavor looks the environment's flavor name up in the runner's reported
// catalog. The name is late-bound by design, so a name that is not in the
// catalog is a scheduling failure the standard retry policy covers rather than
// a permanent error — the runner may report it on its next startup.
func (a *Assembler) resolveFlavor(ctx context.Context, runnerID, flavorName string) (*runnersv1.Flavor, error) {
	runnerID = strings.TrimSpace(runnerID)
	if runnerID == "" {
		return nil, fmt.Errorf("runner id missing")
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := a.runners.ListFlavors(rctx, &runnersv1.ListFlavorsRequest{
		RunnerId: &runnerID,
		// A deprecated entry still resolves and schedules; the flag only steers
		// new references away from it.
		IncludeDeprecated: proto.Bool(true),
	})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("list flavors for runner %s: %w", runnerID, err)
	}

	flavorName = strings.TrimSpace(flavorName)
	var defaultFlavor *runnersv1.Flavor
	for _, flavor := range resp.GetFlavors() {
		if flavor == nil {
			continue
		}
		if flavorName != "" && flavor.GetName() == flavorName {
			return flavor, nil
		}
		if flavor.GetDefault() {
			defaultFlavor = flavor
		}
	}

	// An environment naming no flavor takes the runner's default.
	if flavorName == "" {
		if defaultFlavor == nil {
			return nil, fmt.Errorf("runner %s reports no default flavor", runnerID)
		}
		return defaultFlavor, nil
	}
	return nil, fmt.Errorf("runner %s reports no flavor named %q", runnerID, flavorName)
}

func flavorAllocatedResources(flavor *runnersv1.Flavor) (int32, int64, error) {
	if flavor == nil {
		return 0, 0, fmt.Errorf("flavor missing")
	}
	resources := flavor.GetResources()
	if resources == nil {
		return 0, 0, nil
	}
	cpu, err := parseCPUMillicores(resources.GetRequestsCpu(), fmt.Sprintf("flavor %s", flavor.GetMeta().GetId()))
	if err != nil {
		return 0, 0, err
	}
	ram, err := parseRAMBytes(resources.GetRequestsMemory(), fmt.Sprintf("flavor %s", flavor.GetMeta().GetId()))
	if err != nil {
		return 0, 0, err
	}
	if cpu > int64(^uint32(0)>>1) {
		return 0, 0, fmt.Errorf("allocated cpu millicores overflow: %d", cpu)
	}
	return int32(cpu), ram, nil
}

func (a *Assembler) baseSandboxEnvVars(sandbox *agentsv1.Sandbox, environment *agentsv1.Environment) []*runnerv1.EnvVar {
	gatewayURL := buildGatewayURL(a.cfg.AgentGatewayAddress)
	vars := []*runnerv1.EnvVar{
		{Name: "AGYND_MODE", Value: SandboxHolderMode},
		{Name: "SANDBOX_ID", Value: sandbox.GetMeta().GetId()},
		{Name: "SANDBOX_NAME", Value: sandbox.GetName()},
		{Name: "SANDBOX_OWNER_ID", Value: sandbox.GetOwnerId()},
		{Name: "ENVIRONMENT_ID", Value: environment.GetMeta().GetId()},
		{Name: "ENVIRONMENT_NAME", Value: environment.GetName()},
		{Name: "WORKSPACE_DIR", Value: SandboxWorkspaceMountPath},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "AGYN_GATEWAY_URL", Value: gatewayURL},
		{Name: "LLM_BASE_URL", Value: a.cfg.AgentLLMBaseURL},
	}
	if a.cfg.AgentTracingAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "TRACING_ADDRESS", Value: a.cfg.AgentTracingAddress})
		vars = append(vars, &runnerv1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://localhost:4317"})
	}
	return vars
}
