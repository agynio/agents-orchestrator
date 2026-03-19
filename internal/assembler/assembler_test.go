package assembler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	teamsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/teams/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssemblerMainContainer(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &teamsv1.Agent{
		Meta:          &teamsv1.EntityMeta{Id: agentID.String()},
		Name:          "assistant",
		Role:          "ops",
		Model:         "gpt-test",
		Description:   "test agent",
		Configuration: "{\"mode\":\"test\"}",
	}

	skills := []*teamsv1.Skill{{Name: "skill-a", Body: "do-a"}}

	teamsClient := &fakeTeamsClient{
		getAgent: func(_ context.Context, req *teamsv1.GetAgentRequest, _ ...grpc.CallOption) (*teamsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &teamsv1.GetAgentResponse{Agent: agent}, nil
		},
		listSkills: func(_ context.Context, req *teamsv1.ListSkillsRequest, _ ...grpc.CallOption) (*teamsv1.ListSkillsResponse, error) {
			if req.GetAgentId() != agentID.String() {
				return nil, errors.New("unexpected skills agent id")
			}
			return &teamsv1.ListSkillsResponse{Skills: skills}, nil
		},
		listEnvs: func(_ context.Context, req *teamsv1.ListEnvsRequest, _ ...grpc.CallOption) (*teamsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &teamsv1.ListEnvsResponse{Envs: []*teamsv1.Env{
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, Name: "CUSTOM_ENV", Source: &teamsv1.Env_Value{Value: "custom"}},
				}}, nil
			}
			return &teamsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, req *teamsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*teamsv1.ListInitScriptsResponse, error) {
			if req.GetAgentId() != agentID.String() {
				return &teamsv1.ListInitScriptsResponse{}, nil
			}
			return &teamsv1.ListInitScriptsResponse{InitScripts: []*teamsv1.InitScript{
				{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, Script: "echo ready"},
			}}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *teamsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*teamsv1.ListVolumeAttachmentsResponse, error) {
			return &teamsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *teamsv1.ListMcpsRequest, _ ...grpc.CallOption) (*teamsv1.ListMcpsResponse, error) {
			return &teamsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *teamsv1.ListHooksRequest, _ ...grpc.CallOption) (*teamsv1.ListHooksResponse, error) {
			return &teamsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		DefaultAgentImage:         "default-image",
		AgentThreadsAddress:       "threads:50051",
		AgentNotificationsAddress: "notifications:50052",
	}

	assembler := New(teamsClient, &fakeSecretsClient{}, &cfg)
	request, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if request.Main == nil {
		t.Fatal("expected main container")
	}
	if request.Main.Image != cfg.DefaultAgentImage {
		t.Fatalf("expected default image %q, got %q", cfg.DefaultAgentImage, request.Main.Image)
	}
	expectedName := "agent-" + agentID.String()[:8] + "-" + threadID.String()[:8]
	if request.Main.Name != expectedName {
		t.Fatalf("expected main name %q, got %q", expectedName, request.Main.Name)
	}
	expectedCmd := []string{"/bin/sh", "-c", "exec sleep infinity"}
	if !equalStringSlice(request.Main.Cmd, expectedCmd) {
		t.Fatalf("unexpected main cmd: %+v", request.Main.Cmd)
	}
	labelsJSON := request.Main.AdditionalProperties["labels_json"]
	if labelsJSON == "" {
		t.Fatal("expected labels_json")
	}
	labels := map[string]string{}
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		t.Fatalf("unmarshal labels: %v", err)
	}
	expectedLabels := map[string]string{
		LabelManagedBy: ManagedByValue,
		LabelAgentID:   agentID.String(),
		LabelThreadID:  threadID.String(),
	}
	if !equalStringMap(labels, expectedLabels) {
		t.Fatalf("expected labels %+v, got %+v", expectedLabels, labels)
	}
	envs := envMap(request.Main.Env)
	assertEnv(t, envs, "AGENT_ID", agentID.String())
	assertEnv(t, envs, "AGENT_NAME", agent.GetName())
	assertEnv(t, envs, "AGENT_ROLE", agent.GetRole())
	assertEnv(t, envs, "AGENT_MODEL", agent.GetModel())
	assertEnv(t, envs, "AGENT_CONFIG", agent.GetConfiguration())
	assertEnv(t, envs, "THREAD_ID", threadID.String())
	assertEnv(t, envs, "THREADS_ADDRESS", cfg.AgentThreadsAddress)
	assertEnv(t, envs, "NOTIFICATIONS_ADDRESS", cfg.AgentNotificationsAddress)
	assertEnv(t, envs, "CUSTOM_ENV", "custom")
	assertEnv(t, envs, "INIT_SCRIPT", "echo ready")
	var parsedSkills []skillPayload
	if err := json.Unmarshal([]byte(envs["AGENT_SKILLS"]), &parsedSkills); err != nil {
		t.Fatalf("unmarshal skills: %v", err)
	}
	if len(parsedSkills) != 1 || parsedSkills[0].Name != "skill-a" || parsedSkills[0].Body != "do-a" {
		t.Fatalf("unexpected skills payload: %+v", parsedSkills)
	}
}

func TestAssemblerResolvesSecretEnv(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	resolveCalls := 0
	secretsClient := &fakeSecretsClient{
		resolveSecret: func(_ context.Context, req *secretsv1.ResolveSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
			resolveCalls++
			if req.GetId() != "secret-1" {
				return nil, errors.New("unexpected secret id")
			}
			return &secretsv1.ResolveSecretResponse{Value: "resolved"}, nil
		},
	}

	teamsClient := &fakeTeamsClient{
		getAgent: func(_ context.Context, _ *teamsv1.GetAgentRequest, _ ...grpc.CallOption) (*teamsv1.GetAgentResponse, error) {
			return &teamsv1.GetAgentResponse{Agent: &teamsv1.Agent{Meta: &teamsv1.EntityMeta{Id: agentID.String()}}}, nil
		},
		listSkills: func(_ context.Context, _ *teamsv1.ListSkillsRequest, _ ...grpc.CallOption) (*teamsv1.ListSkillsResponse, error) {
			return &teamsv1.ListSkillsResponse{}, nil
		},
		listEnvs: func(_ context.Context, req *teamsv1.ListEnvsRequest, _ ...grpc.CallOption) (*teamsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &teamsv1.ListEnvsResponse{Envs: []*teamsv1.Env{
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV", Source: &teamsv1.Env_SecretId{SecretId: "secret-1"}},
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV_TWO", Source: &teamsv1.Env_SecretId{SecretId: "secret-1"}},
				}}, nil
			}
			return &teamsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, _ *teamsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*teamsv1.ListInitScriptsResponse, error) {
			return &teamsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *teamsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*teamsv1.ListVolumeAttachmentsResponse, error) {
			return &teamsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *teamsv1.ListMcpsRequest, _ ...grpc.CallOption) (*teamsv1.ListMcpsResponse, error) {
			return &teamsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *teamsv1.ListHooksRequest, _ ...grpc.CallOption) (*teamsv1.ListHooksResponse, error) {
			return &teamsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(teamsClient, secretsClient, &config.Config{})
	request, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	envs := envMap(request.Main.Env)
	assertEnv(t, envs, "SECRET_ENV", "resolved")
	assertEnv(t, envs, "SECRET_ENV_TWO", "resolved")
	if resolveCalls != 1 {
		t.Fatalf("expected resolve to be cached, got %d calls", resolveCalls)
	}
}

func TestAssemblerBuildsMcpSidecarAndVolumes(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()
	volumeID := uuid.New()

	teamsClient := &fakeTeamsClient{
		getAgent: func(_ context.Context, _ *teamsv1.GetAgentRequest, _ ...grpc.CallOption) (*teamsv1.GetAgentResponse, error) {
			return &teamsv1.GetAgentResponse{Agent: &teamsv1.Agent{Meta: &teamsv1.EntityMeta{Id: agentID.String()}}}, nil
		},
		listSkills: func(_ context.Context, _ *teamsv1.ListSkillsRequest, _ ...grpc.CallOption) (*teamsv1.ListSkillsResponse, error) {
			return &teamsv1.ListSkillsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *teamsv1.ListMcpsRequest, _ ...grpc.CallOption) (*teamsv1.ListMcpsResponse, error) {
			return &teamsv1.ListMcpsResponse{Mcps: []*teamsv1.Mcp{
				{Meta: &teamsv1.EntityMeta{Id: mcpID.String()}, Image: "mcp-image", Command: "run-mcp"},
			}}, nil
		},
		listEnvs: func(_ context.Context, req *teamsv1.ListEnvsRequest, _ ...grpc.CallOption) (*teamsv1.ListEnvsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &teamsv1.ListEnvsResponse{Envs: []*teamsv1.Env{
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_ENV", Source: &teamsv1.Env_Value{Value: "enabled"}},
				}}, nil
			}
			return &teamsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, req *teamsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*teamsv1.ListInitScriptsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &teamsv1.ListInitScriptsResponse{InitScripts: []*teamsv1.InitScript{
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString(), CreatedAt: timestamppb.New(time.Unix(5, 0))}, Script: "echo mcp"},
				}}, nil
			}
			return &teamsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, req *teamsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*teamsv1.ListVolumeAttachmentsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &teamsv1.ListVolumeAttachmentsResponse{VolumeAttachments: []*teamsv1.VolumeAttachment{
					{Meta: &teamsv1.EntityMeta{Id: uuid.NewString()}, VolumeId: volumeID.String()},
				}}, nil
			}
			return &teamsv1.ListVolumeAttachmentsResponse{}, nil
		},
		getVolume: func(_ context.Context, req *teamsv1.GetVolumeRequest, _ ...grpc.CallOption) (*teamsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &teamsv1.GetVolumeResponse{Volume: &teamsv1.Volume{
				Meta:       &teamsv1.EntityMeta{Id: volumeID.String()},
				Persistent: true,
				MountPath:  "/data",
			}}, nil
		},
		listHooks: func(_ context.Context, _ *teamsv1.ListHooksRequest, _ ...grpc.CallOption) (*teamsv1.ListHooksResponse, error) {
			return &teamsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(teamsClient, &fakeSecretsClient{}, &config.Config{})
	request, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if len(request.Sidecars) != 1 {
		t.Fatalf("expected 1 sidecar, got %d", len(request.Sidecars))
	}
	sidecar := request.Sidecars[0]
	if sidecar.Image != "mcp-image" {
		t.Fatalf("expected sidecar image mcp-image, got %q", sidecar.Image)
	}
	if sidecar.Name != "mcp-"+mcpID.String()[:8] {
		t.Fatalf("unexpected sidecar name: %q", sidecar.Name)
	}
	expectedCmd := []string{"/bin/sh", "-c", "run-mcp"}
	if !equalStringSlice(sidecar.Cmd, expectedCmd) {
		t.Fatalf("unexpected sidecar cmd: %+v", sidecar.Cmd)
	}
	if len(sidecar.Mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(sidecar.Mounts))
	}
	if len(request.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(request.Volumes))
	}
	volumeSpec := request.Volumes[0]
	expectedName := "vol-" + volumeID.String()[:8]
	if volumeSpec.Name != expectedName {
		t.Fatalf("expected volume name %q, got %q", expectedName, volumeSpec.Name)
	}
	if volumeSpec.Kind != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
		t.Fatalf("expected named volume, got %v", volumeSpec.Kind)
	}
	expectedPersistent := "agent-" + agentID.String()[:8] + "-" + volumeID.String()[:8]
	if volumeSpec.PersistentName != expectedPersistent {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistent, volumeSpec.PersistentName)
	}
	mount := sidecar.Mounts[0]
	if mount.Volume != expectedName {
		t.Fatalf("expected mount volume %q, got %q", expectedName, mount.Volume)
	}
	if mount.MountPath != "/data" {
		t.Fatalf("expected mount path /data, got %q", mount.MountPath)
	}
	envs := envMap(sidecar.Env)
	assertEnv(t, envs, "MCP_ENV", "enabled")
	assertEnv(t, envs, "INIT_SCRIPT", "echo mcp")
}

func envMap(envs []*runnerv1.EnvVar) map[string]string {
	result := make(map[string]string, len(envs))
	for _, env := range envs {
		if env == nil {
			continue
		}
		result[env.Name] = env.Value
	}
	return result
}

func assertEnv(t *testing.T, envs map[string]string, name, expected string) {
	t.Helper()
	value, ok := envs[name]
	if !ok {
		t.Fatalf("missing env %s", name)
	}
	if value != expected {
		t.Fatalf("expected env %s=%q, got %q", name, expected, value)
	}
}

func equalStringMap(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func equalStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i, value := range left {
		if right[i] != value {
			return false
		}
	}
	return true
}

type fakeTeamsClient struct {
	getAgent              func(context.Context, *teamsv1.GetAgentRequest, ...grpc.CallOption) (*teamsv1.GetAgentResponse, error)
	listSkills            func(context.Context, *teamsv1.ListSkillsRequest, ...grpc.CallOption) (*teamsv1.ListSkillsResponse, error)
	listEnvs              func(context.Context, *teamsv1.ListEnvsRequest, ...grpc.CallOption) (*teamsv1.ListEnvsResponse, error)
	listInitScripts       func(context.Context, *teamsv1.ListInitScriptsRequest, ...grpc.CallOption) (*teamsv1.ListInitScriptsResponse, error)
	listVolumeAttachments func(context.Context, *teamsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*teamsv1.ListVolumeAttachmentsResponse, error)
	listMcps              func(context.Context, *teamsv1.ListMcpsRequest, ...grpc.CallOption) (*teamsv1.ListMcpsResponse, error)
	listHooks             func(context.Context, *teamsv1.ListHooksRequest, ...grpc.CallOption) (*teamsv1.ListHooksResponse, error)
	getVolume             func(context.Context, *teamsv1.GetVolumeRequest, ...grpc.CallOption) (*teamsv1.GetVolumeResponse, error)
}

var errNotImplemented = errors.New("not implemented")

func (f *fakeTeamsClient) CreateAgent(context.Context, *teamsv1.CreateAgentRequest, ...grpc.CallOption) (*teamsv1.CreateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetAgent(ctx context.Context, req *teamsv1.GetAgentRequest, opts ...grpc.CallOption) (*teamsv1.GetAgentResponse, error) {
	if f.getAgent != nil {
		return f.getAgent(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateAgent(context.Context, *teamsv1.UpdateAgentRequest, ...grpc.CallOption) (*teamsv1.UpdateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteAgent(context.Context, *teamsv1.DeleteAgentRequest, ...grpc.CallOption) (*teamsv1.DeleteAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListAgents(context.Context, *teamsv1.ListAgentsRequest, ...grpc.CallOption) (*teamsv1.ListAgentsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateVolume(context.Context, *teamsv1.CreateVolumeRequest, ...grpc.CallOption) (*teamsv1.CreateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetVolume(ctx context.Context, req *teamsv1.GetVolumeRequest, opts ...grpc.CallOption) (*teamsv1.GetVolumeResponse, error) {
	if f.getVolume != nil {
		return f.getVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateVolume(context.Context, *teamsv1.UpdateVolumeRequest, ...grpc.CallOption) (*teamsv1.UpdateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteVolume(context.Context, *teamsv1.DeleteVolumeRequest, ...grpc.CallOption) (*teamsv1.DeleteVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListVolumes(context.Context, *teamsv1.ListVolumesRequest, ...grpc.CallOption) (*teamsv1.ListVolumesResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateVolumeAttachment(context.Context, *teamsv1.CreateVolumeAttachmentRequest, ...grpc.CallOption) (*teamsv1.CreateVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetVolumeAttachment(context.Context, *teamsv1.GetVolumeAttachmentRequest, ...grpc.CallOption) (*teamsv1.GetVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteVolumeAttachment(context.Context, *teamsv1.DeleteVolumeAttachmentRequest, ...grpc.CallOption) (*teamsv1.DeleteVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListVolumeAttachments(ctx context.Context, req *teamsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*teamsv1.ListVolumeAttachmentsResponse, error) {
	if f.listVolumeAttachments != nil {
		return f.listVolumeAttachments(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateMcp(context.Context, *teamsv1.CreateMcpRequest, ...grpc.CallOption) (*teamsv1.CreateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetMcp(context.Context, *teamsv1.GetMcpRequest, ...grpc.CallOption) (*teamsv1.GetMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateMcp(context.Context, *teamsv1.UpdateMcpRequest, ...grpc.CallOption) (*teamsv1.UpdateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteMcp(context.Context, *teamsv1.DeleteMcpRequest, ...grpc.CallOption) (*teamsv1.DeleteMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListMcps(ctx context.Context, req *teamsv1.ListMcpsRequest, opts ...grpc.CallOption) (*teamsv1.ListMcpsResponse, error) {
	if f.listMcps != nil {
		return f.listMcps(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateSkill(context.Context, *teamsv1.CreateSkillRequest, ...grpc.CallOption) (*teamsv1.CreateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetSkill(context.Context, *teamsv1.GetSkillRequest, ...grpc.CallOption) (*teamsv1.GetSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateSkill(context.Context, *teamsv1.UpdateSkillRequest, ...grpc.CallOption) (*teamsv1.UpdateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteSkill(context.Context, *teamsv1.DeleteSkillRequest, ...grpc.CallOption) (*teamsv1.DeleteSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListSkills(ctx context.Context, req *teamsv1.ListSkillsRequest, opts ...grpc.CallOption) (*teamsv1.ListSkillsResponse, error) {
	if f.listSkills != nil {
		return f.listSkills(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateHook(context.Context, *teamsv1.CreateHookRequest, ...grpc.CallOption) (*teamsv1.CreateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetHook(context.Context, *teamsv1.GetHookRequest, ...grpc.CallOption) (*teamsv1.GetHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateHook(context.Context, *teamsv1.UpdateHookRequest, ...grpc.CallOption) (*teamsv1.UpdateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteHook(context.Context, *teamsv1.DeleteHookRequest, ...grpc.CallOption) (*teamsv1.DeleteHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListHooks(ctx context.Context, req *teamsv1.ListHooksRequest, opts ...grpc.CallOption) (*teamsv1.ListHooksResponse, error) {
	if f.listHooks != nil {
		return f.listHooks(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateEnv(context.Context, *teamsv1.CreateEnvRequest, ...grpc.CallOption) (*teamsv1.CreateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetEnv(context.Context, *teamsv1.GetEnvRequest, ...grpc.CallOption) (*teamsv1.GetEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateEnv(context.Context, *teamsv1.UpdateEnvRequest, ...grpc.CallOption) (*teamsv1.UpdateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteEnv(context.Context, *teamsv1.DeleteEnvRequest, ...grpc.CallOption) (*teamsv1.DeleteEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListEnvs(ctx context.Context, req *teamsv1.ListEnvsRequest, opts ...grpc.CallOption) (*teamsv1.ListEnvsResponse, error) {
	if f.listEnvs != nil {
		return f.listEnvs(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) CreateInitScript(context.Context, *teamsv1.CreateInitScriptRequest, ...grpc.CallOption) (*teamsv1.CreateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) GetInitScript(context.Context, *teamsv1.GetInitScriptRequest, ...grpc.CallOption) (*teamsv1.GetInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) UpdateInitScript(context.Context, *teamsv1.UpdateInitScriptRequest, ...grpc.CallOption) (*teamsv1.UpdateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) DeleteInitScript(context.Context, *teamsv1.DeleteInitScriptRequest, ...grpc.CallOption) (*teamsv1.DeleteInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeTeamsClient) ListInitScripts(ctx context.Context, req *teamsv1.ListInitScriptsRequest, opts ...grpc.CallOption) (*teamsv1.ListInitScriptsResponse, error) {
	if f.listInitScripts != nil {
		return f.listInitScripts(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

type fakeSecretsClient struct {
	resolveSecret func(context.Context, *secretsv1.ResolveSecretRequest, ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error)
}

func (f *fakeSecretsClient) CreateSecretProvider(context.Context, *secretsv1.CreateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.CreateSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) GetSecretProvider(context.Context, *secretsv1.GetSecretProviderRequest, ...grpc.CallOption) (*secretsv1.GetSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) UpdateSecretProvider(context.Context, *secretsv1.UpdateSecretProviderRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) DeleteSecretProvider(context.Context, *secretsv1.DeleteSecretProviderRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretProviderResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ListSecretProviders(context.Context, *secretsv1.ListSecretProvidersRequest, ...grpc.CallOption) (*secretsv1.ListSecretProvidersResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) CreateSecret(context.Context, *secretsv1.CreateSecretRequest, ...grpc.CallOption) (*secretsv1.CreateSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) GetSecret(context.Context, *secretsv1.GetSecretRequest, ...grpc.CallOption) (*secretsv1.GetSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) UpdateSecret(context.Context, *secretsv1.UpdateSecretRequest, ...grpc.CallOption) (*secretsv1.UpdateSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) DeleteSecret(context.Context, *secretsv1.DeleteSecretRequest, ...grpc.CallOption) (*secretsv1.DeleteSecretResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ListSecrets(context.Context, *secretsv1.ListSecretsRequest, ...grpc.CallOption) (*secretsv1.ListSecretsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeSecretsClient) ResolveSecret(ctx context.Context, req *secretsv1.ResolveSecretRequest, opts ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
	if f.resolveSecret != nil {
		return f.resolveSecret(ctx, req, opts...)
	}
	return nil, errNotImplemented
}
