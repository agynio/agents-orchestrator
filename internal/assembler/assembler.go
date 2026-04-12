package assembler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const (
	listPageSize                   int32 = 100
	rpcTimeout                           = 10 * time.Second
	agynBinVolumeName                    = "agyn-bin"
	agynBinMountPath                     = "/agyn-bin"
	agynBinBinaryPath                    = "/agyn-bin/agynd"
	agentWorkspaceDir                    = "/tmp"
	agentHomeDir                         = "/root"
	mcpBasePort                          = 8100
	ZitiSidecarInitContainerName         = "ziti-sidecar"
	zitiIdentityVolumeName               = "ziti-identity"
	zitiIdentityMountPath                = "/netfoundry"
	zitiDNSNameserver                    = "127.0.0.1"
	zitiSidecarCommand                   = "tproxy"
	zitiRequiredCapabilityNetAdmin       = "NET_ADMIN"
	zitiRestartPolicyKey                 = "restart_policy"
	zitiRestartPolicyAlways              = "Always"
	zitiDNSSearchService                 = "svc.cluster.local"
	zitiDNSSearchCluster                 = "cluster.local"
)

type Assembler struct {
	agents  agentsv1.AgentsServiceClient
	secrets secretsv1.SecretsServiceClient
	cfg     *config.Config
}

type AssembleResult struct {
	Request           *runnerv1.StartWorkloadRequest
	OrganizationID    string
	RunnerLabels      map[string]string
	PersistentVolumes []PersistentVolumeInfo
}

type PersistentVolumeInfo struct {
	ID     uuid.UUID
	Volume *agentsv1.Volume
	Spec   *runnerv1.VolumeSpec
}

func New(agents agentsv1.AgentsServiceClient, secrets secretsv1.SecretsServiceClient, cfg *config.Config) *Assembler {
	return &Assembler{agents: agents, secrets: secrets, cfg: cfg}
}

func (a *Assembler) Assemble(ctx context.Context, agentID, threadID uuid.UUID) (*AssembleResult, error) {
	agent, err := a.fetchAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	runnerLabels := agentRunnerLabels(agent)

	resolver := newEnvResolver(a.secrets)
	volumeResolver := newVolumeResolver(a.agents, agentID)
	imagePullResolver := newImagePullResolver(a.secrets)

	agentEnvs, err := a.listEnvs(ctx, &agentsv1.ListEnvsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent envs: %w", err)
	}
	agentEnvVars, err := resolver.ResolveEnvVars(ctx, agentEnvs)
	if err != nil {
		return nil, fmt.Errorf("resolve agent envs: %w", err)
	}

	agentScripts, err := a.listInitScripts(ctx, &agentsv1.ListInitScriptsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent init scripts: %w", err)
	}
	agentInitScript := concatInitScripts(agentScripts)

	skills, err := a.listSkills(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list skills: %w", err)
	}
	skillsJSON, err := buildSkillsJSON(skills)
	if err != nil {
		return nil, fmt.Errorf("encode skills: %w", err)
	}

	agentAttachments, err := a.listVolumeAttachments(ctx, &agentsv1.ListVolumeAttachmentsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent volume attachments: %w", err)
	}
	agentMounts, err := volumeResolver.mountsFor(ctx, agentAttachments)
	if err != nil {
		return nil, fmt.Errorf("resolve agent mounts: %w", err)
	}
	agentImagePullAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent image pull secret attachments: %w", err)
	}
	if err := imagePullResolver.Resolve(ctx, agentImagePullAttachments); err != nil {
		return nil, fmt.Errorf("resolve agent image pull secrets: %w", err)
	}

	mainEnv := a.baseAgentEnvVars(ctx, agent, agentID, threadID, skillsJSON, agentInitScript)
	mainEnv = append(mainEnv, agentEnvVars...)

	initImage := agent.GetInitImage()
	if initImage == "" {
		return nil, fmt.Errorf("agent %s: init_image is required", agentID)
	}

	mainMounts := append([]*runnerv1.VolumeMount{}, agentMounts...)
	mainMounts = append(mainMounts, &runnerv1.VolumeMount{Volume: agynBinVolumeName, MountPath: agynBinMountPath})
	main := &runnerv1.ContainerSpec{
		Image:  agent.GetImage(),
		Name:   fmt.Sprintf("agent-%s-%s", agentID.String()[:8], threadID.String()[:8]),
		Cmd:    []string{agynBinBinaryPath},
		Env:    mainEnv,
		Mounts: mainMounts,
	}

	initContainer := &runnerv1.ContainerSpec{
		Image: initImage,
		Name:  "agent-init",
		Mounts: []*runnerv1.VolumeMount{
			{Volume: agynBinVolumeName, MountPath: agynBinMountPath},
		},
	}
	initContainers := []*runnerv1.ContainerSpec{initContainer}
	if a.cfg.ZitiEnabled {
		initContainers = append(initContainers, &runnerv1.ContainerSpec{
			Image:                a.cfg.ZitiSidecarImage,
			Name:                 ZitiSidecarInitContainerName,
			Cmd:                  []string{zitiSidecarCommand},
			Mounts:               []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
			RequiredCapabilities: []string{zitiRequiredCapabilityNetAdmin},
			AdditionalProperties: map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways},
		})
	}

	mcps, err := a.listMcps(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list mcps: %w", err)
	}
	log.Printf("assembler: agent %s: found %d MCP servers", agentID, len(mcps))
	mcpAssignments, err := assignMcpPorts(mcps)
	if err != nil {
		return nil, fmt.Errorf("assign mcp ports: %w", err)
	}
	hooks, err := a.listHooks(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list hooks: %w", err)
	}
	hookAssignments, err := assignHooks(hooks)
	if err != nil {
		return nil, fmt.Errorf("assign hooks: %w", err)
	}
	for _, assignment := range mcpAssignments {
		mcpAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{McpId: assignment.id})
		if err != nil {
			return nil, fmt.Errorf("list mcp image pull secret attachments: %w", err)
		}
		if err := imagePullResolver.Resolve(ctx, mcpAttachments); err != nil {
			return nil, fmt.Errorf("resolve mcp image pull secrets: %w", err)
		}
	}
	for _, assignment := range hookAssignments {
		hookAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{HookId: assignment.id.String()})
		if err != nil {
			return nil, fmt.Errorf("list hook image pull secret attachments: %w", err)
		}
		if err := imagePullResolver.Resolve(ctx, hookAttachments); err != nil {
			return nil, fmt.Errorf("resolve hook image pull secrets: %w", err)
		}
	}

	sidecars := make([]*runnerv1.ContainerSpec, 0, len(mcpAssignments)+len(hookAssignments))
	mcpServers := make([]string, 0, len(mcpAssignments))
	for _, assignment := range mcpAssignments {
		sidecar, err := a.buildMcpSidecar(ctx, resolver, volumeResolver, assignment.mcp, assignment.port)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
		mcpServers = append(mcpServers, fmt.Sprintf("%s:%d", assignment.name, assignment.port))
	}
	for _, assignment := range hookAssignments {
		sidecar, err := a.buildHookSidecar(ctx, resolver, volumeResolver, assignment)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
	}
	if len(mcpServers) > 0 {
		main.Env = append(main.Env, &runnerv1.EnvVar{Name: "AGENT_MCP_SERVERS", Value: strings.Join(mcpServers, ",")})
	}
	imagePullCredentials, err := imagePullResolver.Credentials()
	if err != nil {
		return nil, fmt.Errorf("image pull credentials: %w", err)
	}

	agynBinVolume := &runnerv1.VolumeSpec{
		Name: agynBinVolumeName,
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	volumes := append(volumeResolver.Specs(), agynBinVolume)
	if a.cfg.ZitiEnabled {
		volumes = append(volumes, &runnerv1.VolumeSpec{
			Name: zitiIdentityVolumeName,
			Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
		})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })

	request := &runnerv1.StartWorkloadRequest{
		Main:                 main,
		Sidecars:             sidecars,
		Volumes:              volumes,
		InitContainers:       initContainers,
		ImagePullCredentials: imagePullCredentials,
		AdditionalProperties: map[string]string{
			LabelKeyPrefix + LabelManagedBy: ManagedByValue,
			LabelKeyPrefix + LabelAgentID:   agentID.String(),
			LabelKeyPrefix + LabelThreadID:  threadID.String(),
		},
	}
	if a.cfg.ZitiEnabled {
		request.DnsConfig = &runnerv1.DnsConfig{
			Nameservers: []string{zitiDNSNameserver, a.cfg.ClusterDNS},
			Searches:    []string{zitiDNSSearchService, zitiDNSSearchCluster},
		}
	}
	persistentVolumes, err := volumeResolver.PersistentVolumes()
	if err != nil {
		return nil, err
	}
	return &AssembleResult{
		Request:           request,
		OrganizationID:    agent.GetOrganizationId(),
		RunnerLabels:      runnerLabels,
		PersistentVolumes: persistentVolumes,
	}, nil
}

func agentRunnerLabels(agent *agentsv1.Agent) map[string]string {
	if agent == nil {
		return nil
	}
	// TODO: Add runner_labels to Agent proto and return agent.GetRunnerLabels().
	return nil
}

func (a *Assembler) fetchAgent(ctx context.Context, agentID uuid.UUID) (*agentsv1.Agent, error) {
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := a.agents.GetAgent(rctx, &agentsv1.GetAgentRequest{Id: agentID.String()})
	cancel()
	if err != nil {
		return nil, err
	}
	agent := resp.GetAgent()
	if agent == nil {
		return nil, fmt.Errorf("agent response missing")
	}
	meta := agent.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("agent meta missing")
	}
	metaID, err := uuidutil.ParseUUID(meta.GetId(), "agent.meta.id")
	if err != nil {
		return nil, err
	}
	if metaID != agentID {
		return nil, fmt.Errorf("agent id mismatch: %s", metaID.String())
	}
	if agent.GetOrganizationId() == "" {
		return nil, fmt.Errorf("agent organization id missing")
	}
	return agent, nil
}

func (a *Assembler) listMcps(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Mcp, error) {
	resp := []*agentsv1.Mcp{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListMcps(rctx, &agentsv1.ListMcpsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetMcps()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listHooks(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Hook, error) {
	resp := []*agentsv1.Hook{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListHooks(rctx, &agentsv1.ListHooksRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetHooks()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listSkills(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Skill, error) {
	resp := []*agentsv1.Skill{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListSkills(rctx, &agentsv1.ListSkillsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetSkills()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest) ([]*agentsv1.Env, error) {
	resp := []*agentsv1.Env{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListEnvs(rctx, &agentsv1.ListEnvsRequest{
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetEnvs()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listInitScripts(ctx context.Context, req *agentsv1.ListInitScriptsRequest) ([]*agentsv1.InitScript, error) {
	resp := []*agentsv1.InitScript{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListInitScripts(rctx, &agentsv1.ListInitScriptsRequest{
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetInitScripts()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest) ([]*agentsv1.VolumeAttachment, error) {
	resp := []*agentsv1.VolumeAttachment{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListVolumeAttachments(rctx, &agentsv1.ListVolumeAttachmentsRequest{
			VolumeId:  req.GetVolumeId(),
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetVolumeAttachments()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listImagePullSecretAttachments(ctx context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest) ([]*agentsv1.ImagePullSecretAttachment, error) {
	resp := []*agentsv1.ImagePullSecretAttachment{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListImagePullSecretAttachments(rctx, &agentsv1.ListImagePullSecretAttachmentsRequest{
			ImagePullSecretId: req.GetImagePullSecretId(),
			AgentId:           req.GetAgentId(),
			McpId:             req.GetMcpId(),
			HookId:            req.GetHookId(),
			PageSize:          listPageSize,
			PageToken:         token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetImagePullSecretAttachments()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

type mcpAssignment struct {
	mcp  *agentsv1.Mcp
	id   string
	name string
	port int
}

type hookAssignment struct {
	hook *agentsv1.Hook
	id   uuid.UUID
}

func assignMcpPorts(mcps []*agentsv1.Mcp) ([]mcpAssignment, error) {
	assignments := make([]mcpAssignment, 0, len(mcps))
	for _, mcp := range mcps {
		if mcp == nil {
			return nil, fmt.Errorf("mcp is nil")
		}
		meta := mcp.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("mcp meta missing")
		}
		id := meta.GetId()
		if id == "" {
			return nil, fmt.Errorf("mcp meta id missing")
		}
		name := mcp.GetName()
		if name == "" {
			return nil, fmt.Errorf("mcp name missing")
		}
		assignments = append(assignments, mcpAssignment{mcp: mcp, id: id, name: name})
	}
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].id < assignments[j].id
	})
	for i := range assignments {
		assignments[i].port = mcpBasePort + i
	}
	return assignments, nil
}

func assignHooks(hooks []*agentsv1.Hook) ([]hookAssignment, error) {
	assignments := make([]hookAssignment, 0, len(hooks))
	for _, hook := range hooks {
		if hook == nil {
			return nil, fmt.Errorf("hook is nil")
		}
		meta := hook.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("hook meta missing")
		}
		hookID, err := uuidutil.ParseUUID(meta.GetId(), "hook.meta.id")
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, hookAssignment{hook: hook, id: hookID})
	}
	return assignments, nil
}

func (a *Assembler) buildMcpSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, mcp *agentsv1.Mcp, port int) (*runnerv1.ContainerSpec, error) {
	if mcp == nil {
		return nil, fmt.Errorf("mcp is nil")
	}
	meta := mcp.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("mcp meta missing")
	}
	mcpID, err := uuidutil.ParseUUID(meta.GetId(), "mcp.meta.id")
	if err != nil {
		return nil, err
	}
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{McpId: mcpID.String()},
		&agentsv1.ListInitScriptsRequest{McpId: mcpID.String()},
		&agentsv1.ListVolumeAttachmentsRequest{McpId: mcpID.String()},
	)
	if err != nil {
		return nil, err
	}
	envVars = append(envVars, &runnerv1.EnvVar{Name: "MCP_PORT", Value: strconv.Itoa(port)})
	envVars = append(envVars, &runnerv1.EnvVar{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress})
	return &runnerv1.ContainerSpec{
		Image:  mcp.GetImage(),
		Name:   fmt.Sprintf("mcp-%s", mcpID.String()[:8]),
		Cmd:    []string{"/bin/sh", "-c", mcp.GetCommand()},
		Env:    envVars,
		Mounts: mounts,
	}, nil
}

func (a *Assembler) buildHookSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, assignment hookAssignment) (*runnerv1.ContainerSpec, error) {
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{HookId: assignment.id.String()},
		&agentsv1.ListInitScriptsRequest{HookId: assignment.id.String()},
		&agentsv1.ListVolumeAttachmentsRequest{HookId: assignment.id.String()},
	)
	if err != nil {
		return nil, err
	}
	return &runnerv1.ContainerSpec{
		Image:  assignment.hook.GetImage(),
		Name:   fmt.Sprintf("hook-%s", assignment.id.String()[:8]),
		Cmd:    []string{"/bin/sh", "-c", assignment.hook.GetFunction()},
		Env:    envVars,
		Mounts: mounts,
	}, nil
}

func (a *Assembler) baseAgentEnvVars(ctx context.Context, agent *agentsv1.Agent, agentID, threadID uuid.UUID, skillsJSON, initScript string) []*runnerv1.EnvVar {
	vars := []*runnerv1.EnvVar{
		{Name: "AGENT_ID", Value: agentID.String()},
		{Name: "AGENT_NAME", Value: agent.GetName()},
		{Name: "AGENT_ROLE", Value: agent.GetRole()},
		{Name: "AGENT_MODEL", Value: agent.GetModel()},
		{Name: "AGENT_CONFIG", Value: agent.GetConfiguration()},
		{Name: "THREAD_ID", Value: threadID.String()},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "LLM_BASE_URL", Value: a.cfg.AgentLLMBaseURL},
		{Name: "WORKSPACE_DIR", Value: agentWorkspaceDir},
		{Name: "HOME", Value: agentHomeDir},
		{Name: "AGENT_SKILLS", Value: skillsJSON},
	}
	if a.cfg.AgentTracingAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "TRACING_ADDRESS", Value: a.cfg.AgentTracingAddress})
	}
	if initScript != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "INIT_SCRIPT", Value: initScript})
	}
	return vars
}

type skillPayload struct {
	Name string `json:"name"`
	Body string `json:"body"`
}

func buildSkillsJSON(skills []*agentsv1.Skill) (string, error) {
	payload := make([]skillPayload, len(skills))
	for i, skill := range skills {
		payload[i] = skillPayload{Name: skill.GetName(), Body: skill.GetBody()}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func concatInitScripts(scripts []*agentsv1.InitScript) string {
	if len(scripts) == 0 {
		return ""
	}
	sorted := append([]*agentsv1.InitScript(nil), scripts...)
	sort.SliceStable(sorted, func(i, j int) bool {
		itime := initScriptTime(sorted[i])
		jtime := initScriptTime(sorted[j])
		if itime.Equal(jtime) {
			return initScriptID(sorted[i]) < initScriptID(sorted[j])
		}
		return itime.Before(jtime)
	})
	var builder strings.Builder
	for i, script := range sorted {
		if i > 0 {
			builder.WriteString("\n")
		}
		builder.WriteString(script.GetScript())
	}
	return builder.String()
}

func initScriptTime(script *agentsv1.InitScript) time.Time {
	if script == nil {
		return time.Time{}
	}
	meta := script.GetMeta()
	if meta == nil {
		return time.Time{}
	}
	if meta.GetCreatedAt() == nil {
		return time.Time{}
	}
	return meta.GetCreatedAt().AsTime()
}

func initScriptID(script *agentsv1.InitScript) string {
	if script == nil {
		return ""
	}
	meta := script.GetMeta()
	if meta == nil {
		return ""
	}
	return meta.GetId()
}

type volumeResolver struct {
	agents  agentsv1.AgentsServiceClient
	agentID uuid.UUID
	cache   map[string]*agentsv1.Volume
	specs   map[string]*runnerv1.VolumeSpec
}

func newVolumeResolver(agents agentsv1.AgentsServiceClient, agentID uuid.UUID) *volumeResolver {
	return &volumeResolver{
		agents:  agents,
		agentID: agentID,
		cache:   map[string]*agentsv1.Volume{},
		specs:   map[string]*runnerv1.VolumeSpec{},
	}
}

func (v *volumeResolver) mountsFor(ctx context.Context, attachments []*agentsv1.VolumeAttachment) ([]*runnerv1.VolumeMount, error) {
	mounts := make([]*runnerv1.VolumeMount, 0, len(attachments))
	for _, attachment := range attachments {
		if attachment == nil {
			return nil, fmt.Errorf("volume attachment is nil")
		}
		volumeIDRaw := attachment.GetVolumeId()
		volumeID, err := uuidutil.ParseUUID(volumeIDRaw, "volume_attachment.volume_id")
		if err != nil {
			return nil, err
		}
		volume, err := v.getVolume(ctx, volumeID)
		if err != nil {
			return nil, err
		}
		mountPath := volume.GetMountPath()
		if mountPath == "" {
			return nil, fmt.Errorf("volume %s mount_path is empty", volumeID.String())
		}
		spec := v.ensureSpec(volumeID, volume)
		mounts = append(mounts, &runnerv1.VolumeMount{Volume: spec.Name, MountPath: mountPath})
	}
	return mounts, nil
}

func (v *volumeResolver) Specs() []*runnerv1.VolumeSpec {
	if len(v.specs) == 0 {
		return nil
	}
	specs := make([]*runnerv1.VolumeSpec, 0, len(v.specs))
	for _, spec := range v.specs {
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

func (v *volumeResolver) PersistentVolumes() ([]PersistentVolumeInfo, error) {
	if len(v.specs) == 0 {
		return nil, nil
	}
	volumes := make([]PersistentVolumeInfo, 0, len(v.specs))
	for volumeID, spec := range v.specs {
		if spec == nil {
			return nil, fmt.Errorf("volume spec missing for %s", volumeID)
		}
		volume := v.cache[volumeID]
		if volume == nil {
			return nil, fmt.Errorf("volume %s missing", volumeID)
		}
		if !volume.GetPersistent() {
			continue
		}
		parsedID, err := uuidutil.ParseUUID(volumeID, "volume.id")
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, PersistentVolumeInfo{ID: parsedID, Volume: volume, Spec: spec})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].ID.String() < volumes[j].ID.String() })
	return volumes, nil
}

func (v *volumeResolver) getVolume(ctx context.Context, volumeID uuid.UUID) (*agentsv1.Volume, error) {
	key := volumeID.String()
	if cached, ok := v.cache[key]; ok {
		return cached, nil
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := v.agents.GetVolume(rctx, &agentsv1.GetVolumeRequest{Id: key})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("get volume %s: %w", key, err)
	}
	volume := resp.GetVolume()
	if volume == nil {
		return nil, fmt.Errorf("volume %s missing", key)
	}
	v.cache[key] = volume
	return volume, nil
}

func (a *Assembler) resolveSidecarResources(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, envReq *agentsv1.ListEnvsRequest, initReq *agentsv1.ListInitScriptsRequest, attachmentReq *agentsv1.ListVolumeAttachmentsRequest) ([]*runnerv1.EnvVar, []*runnerv1.VolumeMount, error) {
	vars, err := a.listEnvs(ctx, envReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar envs: %w", err)
	}
	envVars, err := resolver.ResolveEnvVars(ctx, vars)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar envs: %w", err)
	}
	scripts, err := a.listInitScripts(ctx, initReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar init scripts: %w", err)
	}
	initScript := concatInitScripts(scripts)
	if initScript != "" {
		envVars = append(envVars, &runnerv1.EnvVar{Name: "INIT_SCRIPT", Value: initScript})
	}
	attachments, err := a.listVolumeAttachments(ctx, attachmentReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar volume attachments: %w", err)
	}
	mounts, err := volumeResolver.mountsFor(ctx, attachments)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar mounts: %w", err)
	}
	return envVars, mounts, nil
}

func (v *volumeResolver) ensureSpec(volumeID uuid.UUID, volume *agentsv1.Volume) *runnerv1.VolumeSpec {
	key := volumeID.String()
	if spec, ok := v.specs[key]; ok {
		return spec
	}
	shortVolume := key[:8]
	spec := &runnerv1.VolumeSpec{
		Name: fmt.Sprintf("vol-%s", shortVolume),
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	if volume.GetPersistent() {
		spec.Kind = runnerv1.VolumeKind_VOLUME_KIND_NAMED
		spec.PersistentName = fmt.Sprintf("agent-%s-%s", v.agentID.String()[:8], shortVolume)
	}
	v.specs[key] = spec
	return spec
}
