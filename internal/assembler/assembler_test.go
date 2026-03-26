package assembler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssemblerMainContainer(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Name:           "assistant",
		Role:           "ops",
		Model:          "gpt-test",
		Image:          "agent-image",
		Description:    "test agent",
		Configuration:  "{\"mode\":\"test\"}",
	}

	skills := []*agentsv1.Skill{{Name: "skill-a", Body: "do-a"}}

	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		listSkills: func(_ context.Context, req *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			if req.GetAgentId() != agentID.String() {
				return nil, errors.New("unexpected skills agent id")
			}
			return &agentsv1.ListSkillsResponse{Skills: skills}, nil
		},
		listEnvs: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "CUSTOM_ENV", Source: &agentsv1.Env_Value{Value: "custom"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, req *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			if req.GetAgentId() != agentID.String() {
				return &agentsv1.ListInitScriptsResponse{}, nil
			}
			return &agentsv1.ListInitScriptsResponse{InitScripts: []*agentsv1.InitScript{
				{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Script: "echo ready"},
			}}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}

	assembler := New(agentsClient, &fakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.OrganizationID != agent.GetOrganizationId() {
		t.Fatalf("expected organization id %q, got %q", agent.GetOrganizationId(), result.OrganizationID)
	}
	request := result.Request
	if request.Main == nil {
		t.Fatal("expected main container")
	}
	if request.Main.Image != agent.GetImage() {
		t.Fatalf("expected agent image %q, got %q", agent.GetImage(), request.Main.Image)
	}
	expectedName := "agent-" + agentID.String()[:8] + "-" + threadID.String()[:8]
	if request.Main.Name != expectedName {
		t.Fatalf("expected main name %q, got %q", expectedName, request.Main.Name)
	}
	expectedCmd := []string{agynBinBinaryPath}
	if !equalStringSlice(request.Main.Cmd, expectedCmd) {
		t.Fatalf("unexpected main cmd: %+v", request.Main.Cmd)
	}
	if len(request.Main.Mounts) != 1 {
		t.Fatalf("expected 1 mount, got %d", len(request.Main.Mounts))
	}
	if request.Main.Mounts[0].Volume != agynBinVolumeName {
		t.Fatalf("expected agyn-bin mount volume, got %q", request.Main.Mounts[0].Volume)
	}
	if request.Main.Mounts[0].MountPath != agynBinMountPath {
		t.Fatalf("expected agyn-bin mount path %q, got %q", agynBinMountPath, request.Main.Mounts[0].MountPath)
	}
	if len(request.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(request.Volumes))
	}
	if request.Volumes[0].Name != agynBinVolumeName {
		t.Fatalf("expected agyn-bin volume name, got %q", request.Volumes[0].Name)
	}
	if request.Volumes[0].Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", request.Volumes[0].Kind)
	}
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
	initContainer := request.InitContainers[0]
	if initContainer.Image != cfg.DefaultInitImage {
		t.Fatalf("expected init container image %q, got %q", cfg.DefaultInitImage, initContainer.Image)
	}
	if initContainer.Name != "agent-init" {
		t.Fatalf("expected init container name agent-init, got %q", initContainer.Name)
	}
	if len(initContainer.Mounts) != 1 {
		t.Fatalf("expected 1 init container mount, got %d", len(initContainer.Mounts))
	}
	if initContainer.Mounts[0].Volume != agynBinVolumeName {
		t.Fatalf("expected init container agyn-bin volume, got %q", initContainer.Mounts[0].Volume)
	}
	if initContainer.Mounts[0].MountPath != agynBinMountPath {
		t.Fatalf("expected init container agyn-bin mount path %q, got %q", agynBinMountPath, initContainer.Mounts[0].MountPath)
	}
	labels := request.AdditionalProperties
	if len(labels) == 0 {
		t.Fatal("expected labels in request additional properties")
	}
	expectedLabels := map[string]string{
		LabelKeyPrefix + LabelManagedBy: ManagedByValue,
		LabelKeyPrefix + LabelAgentID:   agentID.String(),
		LabelKeyPrefix + LabelThreadID:  threadID.String(),
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
	assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
	assertEnv(t, envs, "LLM_BASE_URL", cfg.AgentLLMBaseURL)
	assertEnv(t, envs, "WORKSPACE_DIR", agentWorkspaceDir)
	assertEnv(t, envs, "HOME", agentHomeDir)
	if _, ok := envs["MODEL_OVERRIDE"]; ok {
		t.Fatal("expected MODEL_OVERRIDE to be absent")
	}
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

func TestAssemblerAddsZitiSidecar(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:  &agentsv1.EntityMeta{Id: agentID.String()},
		Image: "agent-image",
	}

	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		listSkills: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		listEnvs: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
		ZitiEnabled:         true,
		ZitiSidecarImage:    "ziti-image",
		ClusterDNS:          "10.43.0.10",
	}

	assembler := New(agentsClient, &fakeSecretsClient{}, &cfg)
	request, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if request.DnsConfig == nil {
		t.Fatal("expected dns config")
	}
	expectedNameservers := []string{zitiDNSNameserver, cfg.ClusterDNS}
	if !equalStringSlice(request.DnsConfig.Nameservers, expectedNameservers) {
		t.Fatalf("expected dns nameservers %+v, got %+v", expectedNameservers, request.DnsConfig.Nameservers)
	}
	expectedSearches := []string{zitiDNSSearchService, zitiDNSSearchCluster}
	if !equalStringSlice(request.DnsConfig.Searches, expectedSearches) {
		t.Fatalf("expected dns searches %+v, got %+v", expectedSearches, request.DnsConfig.Searches)
	}
	if len(request.InitContainers) != 2 {
		t.Fatalf("expected 2 init containers, got %d", len(request.InitContainers))
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, "agent-init")
	if initContainer == nil {
		t.Fatal("expected agent-init container")
	}
	zitiInit := testutil.FindInitContainer(request.InitContainers, ZitiSidecarInitContainerName)
	if zitiInit == nil {
		t.Fatal("expected ziti-sidecar init container")
	}
	if zitiInit.Image != cfg.ZitiSidecarImage {
		t.Fatalf("expected ziti sidecar image %q, got %q", cfg.ZitiSidecarImage, zitiInit.Image)
	}
	expectedCmd := []string{zitiSidecarCommand}
	if !equalStringSlice(zitiInit.Cmd, expectedCmd) {
		t.Fatalf("expected ziti sidecar cmd %+v, got %+v", expectedCmd, zitiInit.Cmd)
	}
	if !equalStringSlice(zitiInit.RequiredCapabilities, []string{zitiRequiredCapabilityNetAdmin}) {
		t.Fatalf("expected ziti sidecar capabilities %+v, got %+v", []string{zitiRequiredCapabilityNetAdmin}, zitiInit.RequiredCapabilities)
	}
	expectedProperties := map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways}
	if !equalStringMap(zitiInit.AdditionalProperties, expectedProperties) {
		t.Fatalf("expected ziti sidecar properties %+v, got %+v", expectedProperties, zitiInit.AdditionalProperties)
	}
	if len(zitiInit.Mounts) != 1 {
		t.Fatalf("expected 1 ziti sidecar mount, got %d", len(zitiInit.Mounts))
	}
	zitiMount := zitiInit.Mounts[0]
	if zitiMount.Volume != zitiIdentityVolumeName {
		t.Fatalf("expected ziti mount volume %q, got %q", zitiIdentityVolumeName, zitiMount.Volume)
	}
	if zitiMount.MountPath != zitiIdentityMountPath {
		t.Fatalf("expected ziti mount path %q, got %q", zitiIdentityMountPath, zitiMount.MountPath)
	}
	if len(request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(request.Volumes))
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
	}
	zitiIdentityVolume := findVolumeSpec(request.Volumes, zitiIdentityVolumeName)
	if zitiIdentityVolume == nil {
		t.Fatalf("expected %s volume", zitiIdentityVolumeName)
	}
	if zitiIdentityVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected ziti identity volume kind ephemeral, got %v", zitiIdentityVolume.Kind)
	}
}

func TestAssemblerZitiDefaultsFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("THREADS_ADDRESS", "")
	t.Setenv("NOTIFICATIONS_ADDRESS", "")
	t.Setenv("AGENTS_ADDRESS", "")
	t.Setenv("SECRETS_ADDRESS", "")
	t.Setenv("RUNNER_ADDRESS", "")
	t.Setenv("ZITI_ENABLED", "true")
	t.Setenv("ZITI_MANAGEMENT_ADDRESS", "")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_SIDECAR_IMAGE", "")
	t.Setenv("CLUSTER_DNS", "")
	t.Setenv("DEFAULT_INIT_IMAGE", "init-image")
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
	t.Setenv("AGENT_MODEL_OVERRIDE", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("IDLE_TIMEOUT", "")
	t.Setenv("STOP_TIMEOUT_SEC", "")
	t.Setenv("LEASE_NAME", "")
	t.Setenv("LEASE_NAMESPACE", "")

	cfg, err := config.FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}

	agentID := uuid.New()
	threadID := uuid.New()
	agent := &agentsv1.Agent{
		Name:          "assistant",
		Role:          "ops",
		Model:         "gpt-test",
		Configuration: "{}",
	}

	envs := envMap(baseAgentEnvVars(&cfg, agent, agentID, threadID, "[]", ""))
	assertEnv(t, envs, "GATEWAY_ADDRESS", "gateway.ziti:443")
	assertEnv(t, envs, "LLM_BASE_URL", "http://llm-proxy.ziti/v1")
}

func TestAssemblerModelOverrideEnv(t *testing.T) {
	agentID := uuid.New()
	threadID := uuid.New()

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
		AgentModelOverride:  "override-model",
	}

	agent := &agentsv1.Agent{
		Name:          "assistant",
		Role:          "ops",
		Model:         "gpt-test",
		Configuration: "{}",
	}

	envs := envMap(baseAgentEnvVars(&cfg, agent, agentID, threadID, "[]", ""))
	assertEnv(t, envs, "MODEL_OVERRIDE", "override-model")
}

func TestAssemblerInitImageOverride(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
	}

	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		listSkills: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		listEnvs: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}

	assembler := New(agentsClient, &fakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
	initContainer := request.InitContainers[0]
	if initContainer.Image != agent.GetInitImage() {
		t.Fatalf("expected init image %q, got %q", agent.GetInitImage(), initContainer.Image)
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

	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}}, nil
		},
		listSkills: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		listEnvs: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV_TWO", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		listHooks: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
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

	agentsClient := &fakeAgentsClient{
		getAgent: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image"}}, nil
		},
		listSkills: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		listMcps: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Image: "mcp-image", Command: "run-mcp"},
			}}, nil
		},
		listEnvs: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_ENV", Source: &agentsv1.Env_Value{Value: "enabled"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		listInitScripts: func(_ context.Context, req *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListInitScriptsResponse{InitScripts: []*agentsv1.InitScript{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString(), CreatedAt: timestamppb.New(time.Unix(5, 0))}, Script: "echo mcp"},
				}}, nil
			}
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		listVolumeAttachments: func(_ context.Context, req *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListVolumeAttachmentsResponse{VolumeAttachments: []*agentsv1.VolumeAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, VolumeId: volumeID.String()},
				}}, nil
			}
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		getVolume: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{
				Meta:       &agentsv1.EntityMeta{Id: volumeID.String()},
				Persistent: true,
				MountPath:  "/data",
			}}, nil
		},
		listHooks: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, &fakeSecretsClient{}, &config.Config{
		DefaultInitImage:    "default-init-image",
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
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
	if len(request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(request.Volumes))
	}
	expectedName := "vol-" + volumeID.String()[:8]
	volumeSpec := findVolumeSpec(request.Volumes, expectedName)
	if volumeSpec == nil {
		t.Fatalf("expected volume %q", expectedName)
	}
	if volumeSpec.Kind != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
		t.Fatalf("expected named volume, got %v", volumeSpec.Kind)
	}
	expectedPersistent := "agent-" + agentID.String()[:8] + "-" + volumeID.String()[:8]
	if volumeSpec.PersistentName != expectedPersistent {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistent, volumeSpec.PersistentName)
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
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

func findVolumeSpec(volumes []*runnerv1.VolumeSpec, name string) *runnerv1.VolumeSpec {
	for _, volume := range volumes {
		if volume.GetName() == name {
			return volume
		}
	}
	return nil
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

type fakeAgentsClient struct {
	getAgent              func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	listSkills            func(context.Context, *agentsv1.ListSkillsRequest, ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error)
	listEnvs              func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error)
	listInitScripts       func(context.Context, *agentsv1.ListInitScriptsRequest, ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error)
	listVolumeAttachments func(context.Context, *agentsv1.ListVolumeAttachmentsRequest, ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error)
	listMcps              func(context.Context, *agentsv1.ListMcpsRequest, ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error)
	listHooks             func(context.Context, *agentsv1.ListHooksRequest, ...grpc.CallOption) (*agentsv1.ListHooksResponse, error)
	getVolume             func(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error)
}

var errNotImplemented = errors.New("not implemented")

func (f *fakeAgentsClient) CreateAgent(context.Context, *agentsv1.CreateAgentRequest, ...grpc.CallOption) (*agentsv1.CreateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.getAgent != nil {
		return f.getAgent(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateAgent(context.Context, *agentsv1.UpdateAgentRequest, ...grpc.CallOption) (*agentsv1.UpdateAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteAgent(context.Context, *agentsv1.DeleteAgentRequest, ...grpc.CallOption) (*agentsv1.DeleteAgentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListAgents(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateVolume(context.Context, *agentsv1.CreateVolumeRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetVolume(ctx context.Context, req *agentsv1.GetVolumeRequest, opts ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	if f.getVolume != nil {
		return f.getVolume(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateVolume(context.Context, *agentsv1.UpdateVolumeRequest, ...grpc.CallOption) (*agentsv1.UpdateVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteVolume(context.Context, *agentsv1.DeleteVolumeRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListVolumes(context.Context, *agentsv1.ListVolumesRequest, ...grpc.CallOption) (*agentsv1.ListVolumesResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateVolumeAttachment(context.Context, *agentsv1.CreateVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.CreateVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetVolumeAttachment(context.Context, *agentsv1.GetVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.GetVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteVolumeAttachment(context.Context, *agentsv1.DeleteVolumeAttachmentRequest, ...grpc.CallOption) (*agentsv1.DeleteVolumeAttachmentResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest, opts ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
	if f.listVolumeAttachments != nil {
		return f.listVolumeAttachments(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateMcp(context.Context, *agentsv1.CreateMcpRequest, ...grpc.CallOption) (*agentsv1.CreateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetMcp(context.Context, *agentsv1.GetMcpRequest, ...grpc.CallOption) (*agentsv1.GetMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateMcp(context.Context, *agentsv1.UpdateMcpRequest, ...grpc.CallOption) (*agentsv1.UpdateMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteMcp(context.Context, *agentsv1.DeleteMcpRequest, ...grpc.CallOption) (*agentsv1.DeleteMcpResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListMcps(ctx context.Context, req *agentsv1.ListMcpsRequest, opts ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
	if f.listMcps != nil {
		return f.listMcps(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateSkill(context.Context, *agentsv1.CreateSkillRequest, ...grpc.CallOption) (*agentsv1.CreateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetSkill(context.Context, *agentsv1.GetSkillRequest, ...grpc.CallOption) (*agentsv1.GetSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateSkill(context.Context, *agentsv1.UpdateSkillRequest, ...grpc.CallOption) (*agentsv1.UpdateSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteSkill(context.Context, *agentsv1.DeleteSkillRequest, ...grpc.CallOption) (*agentsv1.DeleteSkillResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListSkills(ctx context.Context, req *agentsv1.ListSkillsRequest, opts ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
	if f.listSkills != nil {
		return f.listSkills(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateHook(context.Context, *agentsv1.CreateHookRequest, ...grpc.CallOption) (*agentsv1.CreateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetHook(context.Context, *agentsv1.GetHookRequest, ...grpc.CallOption) (*agentsv1.GetHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateHook(context.Context, *agentsv1.UpdateHookRequest, ...grpc.CallOption) (*agentsv1.UpdateHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteHook(context.Context, *agentsv1.DeleteHookRequest, ...grpc.CallOption) (*agentsv1.DeleteHookResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListHooks(ctx context.Context, req *agentsv1.ListHooksRequest, opts ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
	if f.listHooks != nil {
		return f.listHooks(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateEnv(context.Context, *agentsv1.CreateEnvRequest, ...grpc.CallOption) (*agentsv1.CreateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetEnv(context.Context, *agentsv1.GetEnvRequest, ...grpc.CallOption) (*agentsv1.GetEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateEnv(context.Context, *agentsv1.UpdateEnvRequest, ...grpc.CallOption) (*agentsv1.UpdateEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteEnv(context.Context, *agentsv1.DeleteEnvRequest, ...grpc.CallOption) (*agentsv1.DeleteEnvResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest, opts ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
	if f.listEnvs != nil {
		return f.listEnvs(ctx, req, opts...)
	}
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) CreateInitScript(context.Context, *agentsv1.CreateInitScriptRequest, ...grpc.CallOption) (*agentsv1.CreateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetInitScript(context.Context, *agentsv1.GetInitScriptRequest, ...grpc.CallOption) (*agentsv1.GetInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateInitScript(context.Context, *agentsv1.UpdateInitScriptRequest, ...grpc.CallOption) (*agentsv1.UpdateInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) DeleteInitScript(context.Context, *agentsv1.DeleteInitScriptRequest, ...grpc.CallOption) (*agentsv1.DeleteInitScriptResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) ListInitScripts(ctx context.Context, req *agentsv1.ListInitScriptsRequest, opts ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
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
