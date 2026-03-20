package assembler

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const listPageSize int32 = 100

type Assembler struct {
	agents  agentsv1.AgentsServiceClient
	secrets secretsv1.SecretsServiceClient
	cfg     *config.Config
}

func New(agents agentsv1.AgentsServiceClient, secrets secretsv1.SecretsServiceClient, cfg *config.Config) *Assembler {
	return &Assembler{agents: agents, secrets: secrets, cfg: cfg}
}

func (a *Assembler) Assemble(ctx context.Context, agentID, threadID uuid.UUID) (*runnerv1.StartWorkloadRequest, error) {
	agent, err := a.fetchAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}

	resolver := newEnvResolver(a.secrets)
	volumeResolver := newVolumeResolver(a.agents, agentID)

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

	labelsJSON, err := json.Marshal(map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelAgentID:   agentID.String(),
		LabelThreadID:  threadID.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("encode labels: %w", err)
	}

	mainEnv := baseAgentEnvVars(a.cfg, agent, agentID, threadID, skillsJSON, agentInitScript)
	mainEnv = append(mainEnv, agentEnvVars...)

	image := agent.GetImage()
	if image == "" {
		image = a.cfg.DefaultAgentImage
	}
	main := &runnerv1.ContainerSpec{
		Image:  image,
		Name:   fmt.Sprintf("agent-%s-%s", agentID.String()[:8], threadID.String()[:8]),
		Cmd:    []string{"/bin/sh", "-c", "exec sleep infinity"},
		Env:    mainEnv,
		Mounts: agentMounts,
		AdditionalProperties: map[string]string{
			"labels_json": string(labelsJSON),
		},
	}

	mcps, err := a.listMcps(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list mcps: %w", err)
	}
	hooks, err := a.listHooks(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list hooks: %w", err)
	}

	sidecars := make([]*runnerv1.ContainerSpec, 0, len(mcps)+len(hooks))
	for _, mcp := range mcps {
		sidecar, err := a.buildMcpSidecar(ctx, resolver, volumeResolver, mcp)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
	}
	for _, hook := range hooks {
		sidecar, err := a.buildHookSidecar(ctx, resolver, volumeResolver, hook)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
	}

	return &runnerv1.StartWorkloadRequest{
		Main:     main,
		Sidecars: sidecars,
		Volumes:  volumeResolver.Specs(),
	}, nil
}

func (a *Assembler) fetchAgent(ctx context.Context, agentID uuid.UUID) (*agentsv1.Agent, error) {
	resp, err := a.agents.GetAgent(ctx, &agentsv1.GetAgentRequest{Id: agentID.String()})
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
	return agent, nil
}

func (a *Assembler) listMcps(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Mcp, error) {
	resp := []*agentsv1.Mcp{}
	token := ""
	for {
		page, err := a.agents.ListMcps(ctx, &agentsv1.ListMcpsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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
		page, err := a.agents.ListHooks(ctx, &agentsv1.ListHooksRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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
		page, err := a.agents.ListSkills(ctx, &agentsv1.ListSkillsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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
		page, err := a.agents.ListEnvs(ctx, &agentsv1.ListEnvsRequest{
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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
		page, err := a.agents.ListInitScripts(ctx, &agentsv1.ListInitScriptsRequest{
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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
		page, err := a.agents.ListVolumeAttachments(ctx, &agentsv1.ListVolumeAttachmentsRequest{
			VolumeId:  req.GetVolumeId(),
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
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

func (a *Assembler) buildMcpSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, mcp *agentsv1.Mcp) (*runnerv1.ContainerSpec, error) {
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
	return &runnerv1.ContainerSpec{
		Image:  mcp.GetImage(),
		Name:   fmt.Sprintf("mcp-%s", mcpID.String()[:8]),
		Cmd:    []string{"/bin/sh", "-c", mcp.GetCommand()},
		Env:    envVars,
		Mounts: mounts,
	}, nil
}

func (a *Assembler) buildHookSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, hook *agentsv1.Hook) (*runnerv1.ContainerSpec, error) {
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
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{HookId: hookID.String()},
		&agentsv1.ListInitScriptsRequest{HookId: hookID.String()},
		&agentsv1.ListVolumeAttachmentsRequest{HookId: hookID.String()},
	)
	if err != nil {
		return nil, err
	}
	return &runnerv1.ContainerSpec{
		Image:  hook.GetImage(),
		Name:   fmt.Sprintf("hook-%s", hookID.String()[:8]),
		Cmd:    []string{"/bin/sh", "-c", hook.GetFunction()},
		Env:    envVars,
		Mounts: mounts,
	}, nil
}

func baseAgentEnvVars(cfg *config.Config, agent *agentsv1.Agent, agentID, threadID uuid.UUID, skillsJSON, initScript string) []*runnerv1.EnvVar {
	vars := []*runnerv1.EnvVar{
		{Name: "AGENT_ID", Value: agentID.String()},
		{Name: "AGENT_NAME", Value: agent.GetName()},
		{Name: "AGENT_ROLE", Value: agent.GetRole()},
		{Name: "AGENT_MODEL", Value: agent.GetModel()},
		{Name: "AGENT_CONFIG", Value: agent.GetConfiguration()},
		{Name: "THREAD_ID", Value: threadID.String()},
		{Name: "THREADS_ADDRESS", Value: cfg.AgentThreadsAddress},
		{Name: "NOTIFICATIONS_ADDRESS", Value: cfg.AgentNotificationsAddress},
		{Name: "AGENT_SKILLS", Value: skillsJSON},
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

func (v *volumeResolver) getVolume(ctx context.Context, volumeID uuid.UUID) (*agentsv1.Volume, error) {
	key := volumeID.String()
	if cached, ok := v.cache[key]; ok {
		return cached, nil
	}
	resp, err := v.agents.GetVolume(ctx, &agentsv1.GetVolumeRequest{Id: key})
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
