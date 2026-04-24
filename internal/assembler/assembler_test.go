package assembler

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const testServiceAccountNamespace = "platform"

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithServiceAccountNamespace(m, testServiceAccountNamespace))
}

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
		InitImage:      "agent-init-image",
		Description:    "test agent",
		Configuration:  "{\"mode\":\"test\"}",
		Capabilities:   []string{"privileged", "dind"},
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "CUSTOM_ENV", Source: &agentsv1.Env_Value{Value: "custom"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "AGENT_NAME", Source: &agentsv1.Env_Value{Value: "override"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "WORKSPACE_DIR", Source: &agentsv1.Env_Value{Value: "/override"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "HOME", Source: &agentsv1.Env_Value{Value: "/override-home"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentTracingAddress: "tracing:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.OrganizationID != agent.GetOrganizationId() {
		t.Fatalf("expected organization id %q, got %q", agent.GetOrganizationId(), result.OrganizationID)
	}
	if len(result.RunnerLabels) != 0 {
		t.Fatalf("expected no runner labels, got %v", result.RunnerLabels)
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
	agynBinMount := findVolumeMount(request.Main, agynBinVolumeName)
	if agynBinMount == nil {
		t.Fatalf("expected agyn-bin mount")
	}
	if agynBinMount.MountPath != agynBinMountPath {
		t.Fatalf("expected agyn-bin mount path %q, got %q", agynBinMountPath, agynBinMount.MountPath)
	}
	if len(request.Volumes) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(request.Volumes))
	}
	agynBinVolume := findVolumeSpec(request.Volumes, agynBinVolumeName)
	if agynBinVolume == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if agynBinVolume.Kind != runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL {
		t.Fatalf("expected agyn-bin volume kind ephemeral, got %v", agynBinVolume.Kind)
	}
	if request.ImagePullCredentials != nil {
		t.Fatalf("expected no image pull credentials, got %+v", request.ImagePullCredentials)
	}
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, "agent-init")
	if initContainer == nil {
		t.Fatal("expected agent-init init container")
	}
	if initContainer.Image != agent.GetInitImage() {
		t.Fatalf("expected init container image %q, got %q", agent.GetInitImage(), initContainer.Image)
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
	if !equalStringSlice(request.Capabilities, agent.GetCapabilities()) {
		t.Fatalf("expected capabilities %+v, got %+v", agent.GetCapabilities(), request.Capabilities)
	}
	envs := envMap(request.Main.Env)
	assertEnv(t, envs, "AGENT_ID", agentID.String())
	assertEnv(t, envs, "AGENT_NAME", agent.GetName())
	assertEnv(t, envs, "AGENT_ROLE", agent.GetRole())
	assertEnv(t, envs, "AGENT_MODEL", agent.GetModel())
	assertEnv(t, envs, "AGENT_CONFIG", agent.GetConfiguration())
	assertEnv(t, envs, "THREAD_ID", threadID.String())
	assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
	assertEnv(t, envs, "LLM_BASE_URL", cfg.AgentLLMBaseURL)
	assertEnv(t, envs, TokenCountingEnvVar, buildTokenCountingAddress(testServiceAccountNamespace))
	assertEnv(t, envs, "TRACING_ADDRESS", cfg.AgentTracingAddress)
	assertEnv(t, envs, "WORKSPACE_DIR", "/override")
	assertEnv(t, envs, "HOME", "/override-home")
	assertEnv(t, envs, "CUSTOM_ENV", "custom")
	if _, ok := envs["INIT_SCRIPT"]; ok {
		t.Fatal("expected INIT_SCRIPT to be absent")
	}
	if _, ok := envs["AGENT_SKILLS"]; ok {
		t.Fatal("expected AGENT_SKILLS to be absent")
	}
}

func TestAssemblerReusesWorkspaceMount(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	volumeID := uuid.New()
	workspacePath := "/workspace"

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, req *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListVolumeAttachmentsResponse{VolumeAttachments: []*agentsv1.VolumeAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, VolumeId: volumeID.String()},
				}}, nil
			}
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{
				Meta:      &agentsv1.EntityMeta{Id: volumeID.String()},
				MountPath: workspacePath,
			}}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	workspaceVolumeName := "vol-" + volumeID.String()[:8]
	if len(request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(request.Volumes))
	}
	if findVolumeSpec(request.Volumes, agynBinVolumeName) == nil {
		t.Fatalf("expected %s volume", agynBinVolumeName)
	}
	if findVolumeSpec(request.Volumes, workspaceVolumeName) == nil {
		t.Fatalf("expected %s volume", workspaceVolumeName)
	}
	workspaceMount := findMountByPath(request.Main.Mounts, workspacePath)
	if workspaceMount == nil {
		t.Fatalf("expected main workspace mount")
	}
	if workspaceMount.Volume != workspaceVolumeName {
		t.Fatalf("expected workspace volume %q, got %q", workspaceVolumeName, workspaceMount.Volume)
	}
	if countMountsByPath(request.Main.Mounts, workspacePath) != 1 {
		t.Fatalf("expected one workspace mount, got %d", countMountsByPath(request.Main.Mounts, workspacePath))
	}
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
}

func TestAssemblerTokenCountingOverride(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	customAddress := "token-counting.custom.svc.cluster.local:50051"

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Name:           "assistant",
		Role:           "ops",
		Model:          "gpt-test",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
		Configuration:  "{}",
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() != agentID.String() {
				return &agentsv1.ListEnvsResponse{}, nil
			}
			return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
				{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: TokenCountingEnvVar, Source: &agentsv1.Env_Value{Value: customAddress}},
			}}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	envs := envMap(result.Request.Main.Env)
	assertEnv(t, envs, TokenCountingEnvVar, customAddress)
}

func TestAssemblerAddsZitiSidecar(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		InitImage:      "agent-init-image",
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
		ZitiEnabled:         true,
		ZitiSidecarImage:    "ziti-image",
		ClusterDNS:          "10.43.0.10",
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.OrganizationID != agent.GetOrganizationId() {
		t.Fatalf("expected organization id %q, got %q", agent.GetOrganizationId(), result.OrganizationID)
	}
	if len(result.RunnerLabels) != 0 {
		t.Fatalf("expected no runner labels, got %v", result.RunnerLabels)
	}
	request := result.Request
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
	if len(request.InitContainers) != 3 {
		t.Fatalf("expected 3 init containers, got %d", len(request.InitContainers))
	}
	if request.InitContainers[0].GetName() != ZitiSidecarContainerName {
		t.Fatalf("expected %s to be first init container", ZitiSidecarContainerName)
	}
	if request.InitContainers[1].GetName() != zitiGatewayWaitContainerName {
		t.Fatalf("expected %s to be second init container", zitiGatewayWaitContainerName)
	}
	if request.InitContainers[2].GetName() != "agent-init" {
		t.Fatalf("expected agent-init to be third init container")
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, "agent-init")
	if initContainer == nil {
		t.Fatal("expected agent-init container")
	}
	if len(request.Sidecars) != 0 {
		t.Fatalf("expected 0 sidecars, got %d", len(request.Sidecars))
	}
	zitiSidecar := testutil.FindInitContainer(request.InitContainers, ZitiSidecarContainerName)
	if zitiSidecar == nil {
		t.Fatal("expected ziti-sidecar init container")
	}
	if zitiSidecar.Image != cfg.ZitiSidecarImage {
		t.Fatalf("expected ziti sidecar image %q, got %q", cfg.ZitiSidecarImage, zitiSidecar.Image)
	}
	expectedCmd := []string{zitiSidecarCommand, "--dnsUpstream", fmt.Sprintf("udp://%s:53", cfg.ClusterDNS)}
	if !equalStringSlice(zitiSidecar.Cmd, expectedCmd) {
		t.Fatalf("expected ziti sidecar cmd %+v, got %+v", expectedCmd, zitiSidecar.Cmd)
	}
	if !equalStringSlice(zitiSidecar.RequiredCapabilities, []string{zitiRequiredCapabilityNetAdmin}) {
		t.Fatalf("expected ziti sidecar capabilities %+v, got %+v", []string{zitiRequiredCapabilityNetAdmin}, zitiSidecar.RequiredCapabilities)
	}
	if len(zitiSidecar.Env) != 1 {
		t.Fatalf("expected 1 ziti sidecar env, got %d", len(zitiSidecar.Env))
	}
	zitiEnv := envMap(zitiSidecar.Env)
	if zitiEnv[ZitiIdentityBasenameEnvVar] != ZitiIdentityBasename {
		t.Fatalf("expected ziti sidecar basename %q, got %q", ZitiIdentityBasename, zitiEnv[ZitiIdentityBasenameEnvVar])
	}
	expectedProperties := map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways}
	if !equalStringMap(zitiSidecar.AdditionalProperties, expectedProperties) {
		t.Fatalf("expected ziti sidecar properties %+v, got %+v", expectedProperties, zitiSidecar.AdditionalProperties)
	}
	if len(zitiSidecar.Mounts) != 1 {
		t.Fatalf("expected 1 ziti sidecar mount, got %d", len(zitiSidecar.Mounts))
	}
	zitiMount := zitiSidecar.Mounts[0]
	if zitiMount.Volume != zitiIdentityVolumeName {
		t.Fatalf("expected ziti mount volume %q, got %q", zitiIdentityVolumeName, zitiMount.Volume)
	}
	if zitiMount.MountPath != zitiIdentityMountPath {
		t.Fatalf("expected ziti mount path %q, got %q", zitiIdentityMountPath, zitiMount.MountPath)
	}
	zitiGatewayWait := testutil.FindInitContainer(request.InitContainers, zitiGatewayWaitContainerName)
	if zitiGatewayWait == nil {
		t.Fatal("expected ziti-gateway-wait init container")
	}
	if zitiGatewayWait.Image != zitiGatewayWaitImage {
		t.Fatalf("expected ziti gateway wait image %q, got %q", zitiGatewayWaitImage, zitiGatewayWait.Image)
	}
	gatewayHost, _, err := net.SplitHostPort(cfg.AgentGatewayAddress)
	if err != nil {
		t.Fatalf("parse gateway host: %v", err)
	}
	expectedWaitCmd := buildZitiGatewayWaitCommand(gatewayHost)
	if !equalStringSlice(zitiGatewayWait.Cmd, expectedWaitCmd) {
		t.Fatalf("expected ziti gateway wait cmd %+v, got %+v", expectedWaitCmd, zitiGatewayWait.Cmd)
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
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
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

	assembler := New(&testutil.FakeAgentsClient{}, &testutil.FakeSecretsClient{}, &cfg)
	envs := envMap(assembler.baseAgentEnvVars(agent, agentID, threadID))
	assertEnv(t, envs, "GATEWAY_ADDRESS", "gateway.ziti:443")
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://gateway.ziti:443")
	assertEnv(t, envs, "LLM_BASE_URL", "http://llm-proxy.ziti/v1")
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

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	if len(request.InitContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(request.InitContainers))
	}
	initContainer := testutil.FindInitContainer(request.InitContainers, "agent-init")
	if initContainer == nil {
		t.Fatal("expected agent-init container")
	}
	if initContainer.Image != agent.GetInitImage() {
		t.Fatalf("expected init image %q, got %q", agent.GetInitImage(), initContainer.Image)
	}
}

func TestAssemblerErrorsOnEmptyInitImage(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agent := &agentsv1.Agent{
		Meta:           &agentsv1.EntityMeta{Id: agentID.String()},
		OrganizationId: "org-1",
		Image:          "agent-image",
		InitImage:      "",
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: agent}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, cfg)
	_, err := assembler.Assemble(ctx, agentID, threadID)
	if err == nil {
		t.Fatal("expected error for empty init image")
	}
	if !strings.Contains(err.Error(), "init_image is required") {
		t.Fatalf("expected init_image required error, got %q", err.Error())
	}
}

func TestAssemblerResolvesSecretEnv(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	resolveCalls := 0
	secretsClient := &testutil.FakeSecretsClient{
		ResolveSecretFunc: func(_ context.Context, req *secretsv1.ResolveSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
			resolveCalls++
			if req.GetId() != "secret-1" {
				return nil, errors.New("unexpected secret id")
			}
			return &secretsv1.ResolveSecretResponse{Value: "resolved"}, nil
		},
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetAgentId() == agentID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV_TWO", Source: &agentsv1.Env_SecretId{SecretId: "secret-1"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
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

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "test-mcp", Image: "mcp-image", Command: "run-mcp"},
			}}, nil
		},
		ListEnvsFunc: func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_ENV", Source: &agentsv1.Env_Value{Value: "enabled"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "MCP_PORT", Source: &agentsv1.Env_Value{Value: "9090"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "GATEWAY_ADDRESS", Source: &agentsv1.Env_Value{Value: "user-gateway"}},
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "AGYN_GATEWAY_URL", Source: &agentsv1.Env_Value{Value: "http://user-gateway"}},
				}}, nil
			}
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, req *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			if req.GetMcpId() == mcpID.String() {
				return &agentsv1.ListVolumeAttachmentsResponse{VolumeAttachments: []*agentsv1.VolumeAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, VolumeId: volumeID.String()},
				}}, nil
			}
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{
				Meta:       &agentsv1.EntityMeta{Id: volumeID.String()},
				Persistent: true,
				MountPath:  "/data",
			}}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, cfg)
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
	expectedPersistent := "pv-" + threadID.String()[:12] + "-" + volumeID.String()[:12]
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
	assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
	assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
}

func TestAssemblerSharesPersistentVolumeAcrossContainers(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadA := uuid.New()
	threadB := uuid.New()
	mcpID := uuid.New()
	volumeID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "shared", Image: "mcp-image", Command: "run-mcp"},
			}}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, req *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			switch {
			case req.GetAgentId() == agentID.String(), req.GetMcpId() == mcpID.String():
				return &agentsv1.ListVolumeAttachmentsResponse{VolumeAttachments: []*agentsv1.VolumeAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, VolumeId: volumeID.String()},
				}}, nil
			default:
				return &agentsv1.ListVolumeAttachmentsResponse{}, nil
			}
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		GetVolumeFunc: func(_ context.Context, req *agentsv1.GetVolumeRequest, _ ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
			if req.GetId() != volumeID.String() {
				return nil, errors.New("unexpected volume id")
			}
			return &agentsv1.GetVolumeResponse{Volume: &agentsv1.Volume{
				Meta:       &agentsv1.EntityMeta{Id: volumeID.String()},
				Persistent: true,
				MountPath:  "/data",
			}}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})

	resultA, err := assembler.Assemble(ctx, agentID, threadA)
	if err != nil {
		t.Fatalf("assemble thread A: %v", err)
	}
	volumeName := "vol-" + volumeID.String()[:8]
	if len(resultA.Request.Volumes) != 2 {
		t.Fatalf("expected 2 volumes, got %d", len(resultA.Request.Volumes))
	}
	volumeSpecA := findVolumeSpec(resultA.Request.Volumes, volumeName)
	if volumeSpecA == nil {
		t.Fatalf("expected volume %q", volumeName)
	}
	expectedPersistentA := "pv-" + threadA.String()[:12] + "-" + volumeID.String()[:12]
	if volumeSpecA.PersistentName != expectedPersistentA {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistentA, volumeSpecA.PersistentName)
	}
	mainMount := findVolumeMount(resultA.Request.Main, volumeName)
	if mainMount == nil {
		t.Fatalf("expected main mount for %q", volumeName)
	}
	if mainMount.MountPath != "/data" {
		t.Fatalf("expected main mount path /data, got %q", mainMount.MountPath)
	}
	if len(resultA.Request.Sidecars) != 1 {
		t.Fatalf("expected 1 sidecar, got %d", len(resultA.Request.Sidecars))
	}
	sidecarMount := findVolumeMount(resultA.Request.Sidecars[0], volumeName)
	if sidecarMount == nil {
		t.Fatalf("expected sidecar mount for %q", volumeName)
	}
	if sidecarMount.MountPath != "/data" {
		t.Fatalf("expected sidecar mount path /data, got %q", sidecarMount.MountPath)
	}

	resultB, err := assembler.Assemble(ctx, agentID, threadB)
	if err != nil {
		t.Fatalf("assemble thread B: %v", err)
	}
	volumeSpecB := findVolumeSpec(resultB.Request.Volumes, volumeName)
	if volumeSpecB == nil {
		t.Fatalf("expected volume %q", volumeName)
	}
	expectedPersistentB := "pv-" + threadB.String()[:12] + "-" + volumeID.String()[:12]
	if volumeSpecB.PersistentName != expectedPersistentB {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistentB, volumeSpecB.PersistentName)
	}
	if volumeSpecA.PersistentName == volumeSpecB.PersistentName {
		t.Fatalf("expected different persistent names, got %q", volumeSpecA.PersistentName)
	}
}

func TestAssemblerMcpPortAllocation(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	lowID := "11111111-1111-1111-1111-111111111111"
	highID := "22222222-2222-2222-2222-222222222222"

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{
				{Meta: &agentsv1.EntityMeta{Id: highID}, Name: "filesystem", Image: "fs-image", Command: "run-fs"},
				{Meta: &agentsv1.EntityMeta{Id: lowID}, Name: "memory", Image: "mem-image", Command: "run-mem"},
			}}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	cfg := &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	}
	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, cfg)
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	request := result.Request
	if len(request.Sidecars) != 2 {
		t.Fatalf("expected 2 sidecars, got %d", len(request.Sidecars))
	}
	mainEnvs := envMap(request.Main.Env)
	expectedServers := fmt.Sprintf("%s:%d,%s:%d", "memory", mcpBasePort, "filesystem", mcpBasePort+1)
	assertEnv(t, mainEnvs, "AGENT_MCP_SERVERS", expectedServers)

	ports := map[string]string{}
	for _, sidecar := range request.Sidecars {
		envs := envMap(sidecar.Env)
		port, ok := envs["MCP_PORT"]
		if !ok {
			t.Fatalf("missing MCP_PORT for sidecar %s", sidecar.Name)
		}
		assertEnv(t, envs, "GATEWAY_ADDRESS", cfg.AgentGatewayAddress)
		assertEnv(t, envs, "AGYN_GATEWAY_URL", "http://"+cfg.AgentGatewayAddress)
		ports[sidecar.Name] = port
	}
	expectedMemoryName := "mcp-" + lowID[:8]
	expectedFilesystemName := "mcp-" + highID[:8]
	if ports[expectedMemoryName] != fmt.Sprintf("%d", mcpBasePort) {
		t.Fatalf("expected %s MCP_PORT %d, got %q", expectedMemoryName, mcpBasePort, ports[expectedMemoryName])
	}
	if ports[expectedFilesystemName] != fmt.Sprintf("%d", mcpBasePort+1) {
		t.Fatalf("expected %s MCP_PORT %d, got %q", expectedFilesystemName, mcpBasePort+1, ports[expectedFilesystemName])
	}
}

func TestAssemblerNoMcpsNoAgentMcpServersEnv(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	envs := envMap(result.Request.Main.Env)
	if _, ok := envs["AGENT_MCP_SERVERS"]; ok {
		t.Fatal("expected AGENT_MCP_SERVERS to be absent")
	}
}

func TestAssemblerImagePullCredentials(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				return nil, errors.New("unexpected agent id")
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			switch {
			case req.GetAgentId() == agentID.String():
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, ImagePullSecretId: "secret-b", Target: &agentsv1.ImagePullSecretAttachment_AgentId{AgentId: agentID.String()}},
				}}, nil
			case req.GetMcpId() == mcpID.String():
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, ImagePullSecretId: "secret-a", Target: &agentsv1.ImagePullSecretAttachment_McpId{McpId: mcpID.String()}},
				}}, nil
			default:
				return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
			}
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "test", Image: "mcp-image", Command: "run"}}}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	secretsClient := &testutil.FakeSecretsClient{
		ResolveImagePullSecretFunc: func(_ context.Context, req *secretsv1.ResolveImagePullSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error) {
			switch req.GetId() {
			case "secret-a":
				return &secretsv1.ResolveImagePullSecretResponse{Registry: "registry-a", Username: "user-a", Password: "pass-a"}, nil
			case "secret-b":
				return &secretsv1.ResolveImagePullSecretResponse{Registry: "registry-b", Username: "user-b", Password: "pass-b"}, nil
			default:
				return nil, errors.New("unexpected image pull secret id")
			}
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	creds := result.Request.ImagePullCredentials
	if len(creds) != 2 {
		t.Fatalf("expected 2 credentials, got %d", len(creds))
	}
	if creds[0].GetRegistry() != "registry-a" || creds[0].GetUsername() != "user-a" || creds[0].GetPassword() != "pass-a" {
		t.Fatalf("unexpected first credential: %+v", creds[0])
	}
	if creds[1].GetRegistry() != "registry-b" || creds[1].GetUsername() != "user-b" || creds[1].GetPassword() != "pass-b" {
		t.Fatalf("unexpected second credential: %+v", creds[1])
	}
}

func TestAssemblerImagePullCredentialsCaching(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()
	hookID := uuid.New()

	resolveCalls := 0
	secretsClient := &testutil.FakeSecretsClient{
		ResolveImagePullSecretFunc: func(_ context.Context, req *secretsv1.ResolveImagePullSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error) {
			resolveCalls++
			if req.GetId() != "secret-1" {
				return nil, errors.New("unexpected image pull secret id")
			}
			return &secretsv1.ResolveImagePullSecretResponse{Registry: "registry", Username: "user", Password: "pass"}, nil
		},
	}

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			attachment := &agentsv1.ImagePullSecretAttachment{
				Meta:              &agentsv1.EntityMeta{Id: uuid.NewString()},
				ImagePullSecretId: "secret-1",
			}
			switch {
			case req.GetAgentId() == agentID.String():
				attachment.Target = &agentsv1.ImagePullSecretAttachment_AgentId{AgentId: agentID.String()}
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{attachment}}, nil
			case req.GetMcpId() == mcpID.String():
				attachment.Target = &agentsv1.ImagePullSecretAttachment_McpId{McpId: mcpID.String()}
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{attachment}}, nil
			case req.GetHookId() == hookID.String():
				attachment.Target = &agentsv1.ImagePullSecretAttachment_HookId{HookId: hookID.String()}
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{attachment}}, nil
			default:
				return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
			}
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "cache", Image: "mcp-image", Command: "run"}}}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{Hooks: []*agentsv1.Hook{{Meta: &agentsv1.EntityMeta{Id: hookID.String()}, Image: "hook-image", Function: "exec"}}}, nil
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if resolveCalls != 1 {
		t.Fatalf("expected 1 resolve call, got %d", resolveCalls)
	}
	creds := result.Request.ImagePullCredentials
	if len(creds) != 1 {
		t.Fatalf("expected 1 credential, got %d", len(creds))
	}
	if creds[0].GetRegistry() != "registry" || creds[0].GetUsername() != "user" || creds[0].GetPassword() != "pass" {
		t.Fatalf("unexpected credential: %+v", creds[0])
	}
}

func TestAssemblerImagePullCredentialsRegistryConflict(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	mcpID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			switch {
			case req.GetAgentId() == agentID.String():
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, ImagePullSecretId: "secret-a", Target: &agentsv1.ImagePullSecretAttachment_AgentId{AgentId: agentID.String()}},
				}}, nil
			case req.GetMcpId() == mcpID.String():
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, ImagePullSecretId: "secret-b", Target: &agentsv1.ImagePullSecretAttachment_McpId{McpId: mcpID.String()}},
				}}, nil
			default:
				return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
			}
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{Mcps: []*agentsv1.Mcp{{Meta: &agentsv1.EntityMeta{Id: mcpID.String()}, Name: "test", Image: "mcp-image", Command: "run"}}}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	secretsClient := &testutil.FakeSecretsClient{
		ResolveImagePullSecretFunc: func(_ context.Context, _ *secretsv1.ResolveImagePullSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error) {
			return &secretsv1.ResolveImagePullSecretResponse{Registry: "registry", Username: "user", Password: "pass"}, nil
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	_, err := assembler.Assemble(ctx, agentID, threadID)
	if err == nil {
		t.Fatal("expected registry conflict error")
	}
	expected := fmt.Sprintf("image pull credentials: registry conflict: registry %q is targeted by image pull secrets %s and %s", "registry", "secret-a", "secret-b")
	if err.Error() != expected {
		t.Fatalf("expected error %q, got %q", expected, err.Error())
	}
}

func TestAssemblerNoImagePullSecretAttachments(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{}, nil
		},
	}

	assembler := New(agentsClient, &testutil.FakeSecretsClient{}, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if result.Request.ImagePullCredentials != nil {
		t.Fatalf("expected no image pull credentials, got %+v", result.Request.ImagePullCredentials)
	}
}

func TestAssemblerImagePullCredentialsHookOnly(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	threadID := uuid.New()
	hookID := uuid.New()

	agentsClient := &testutil.FakeAgentsClient{
		GetAgentFunc: func(_ context.Context, _ *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: agentID.String()}, OrganizationId: "org-1", Image: "agent-image", InitImage: "agent-init-image"}}, nil
		},
		ListSkillsFunc: func(_ context.Context, _ *agentsv1.ListSkillsRequest, _ ...grpc.CallOption) (*agentsv1.ListSkillsResponse, error) {
			return &agentsv1.ListSkillsResponse{}, nil
		},
		ListEnvsFunc: func(_ context.Context, _ *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
		ListInitScriptsFunc: func(_ context.Context, _ *agentsv1.ListInitScriptsRequest, _ ...grpc.CallOption) (*agentsv1.ListInitScriptsResponse, error) {
			return &agentsv1.ListInitScriptsResponse{}, nil
		},
		ListVolumeAttachmentsFunc: func(_ context.Context, _ *agentsv1.ListVolumeAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListVolumeAttachmentsResponse, error) {
			return &agentsv1.ListVolumeAttachmentsResponse{}, nil
		},
		ListImagePullSecretAttachmentsFunc: func(_ context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest, _ ...grpc.CallOption) (*agentsv1.ListImagePullSecretAttachmentsResponse, error) {
			if req.GetHookId() == hookID.String() {
				return &agentsv1.ListImagePullSecretAttachmentsResponse{ImagePullSecretAttachments: []*agentsv1.ImagePullSecretAttachment{
					{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, ImagePullSecretId: "secret-1", Target: &agentsv1.ImagePullSecretAttachment_HookId{HookId: hookID.String()}},
				}}, nil
			}
			return &agentsv1.ListImagePullSecretAttachmentsResponse{}, nil
		},
		ListMcpsFunc: func(_ context.Context, _ *agentsv1.ListMcpsRequest, _ ...grpc.CallOption) (*agentsv1.ListMcpsResponse, error) {
			return &agentsv1.ListMcpsResponse{}, nil
		},
		ListHooksFunc: func(_ context.Context, _ *agentsv1.ListHooksRequest, _ ...grpc.CallOption) (*agentsv1.ListHooksResponse, error) {
			return &agentsv1.ListHooksResponse{Hooks: []*agentsv1.Hook{{Meta: &agentsv1.EntityMeta{Id: hookID.String()}, Image: "hook-image", Function: "exec"}}}, nil
		},
	}

	secretsClient := &testutil.FakeSecretsClient{
		ResolveImagePullSecretFunc: func(_ context.Context, req *secretsv1.ResolveImagePullSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveImagePullSecretResponse, error) {
			if req.GetId() != "secret-1" {
				return nil, errors.New("unexpected image pull secret id")
			}
			return &secretsv1.ResolveImagePullSecretResponse{Registry: "registry", Username: "user", Password: "pass"}, nil
		},
	}

	assembler := New(agentsClient, secretsClient, &config.Config{
		AgentGatewayAddress: "gateway:50051",
		AgentLLMBaseURL:     "http://llm:8080/v1",
	})
	result, err := assembler.Assemble(ctx, agentID, threadID)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	creds := result.Request.ImagePullCredentials
	if len(creds) != 1 {
		t.Fatalf("expected 1 credential, got %d", len(creds))
	}
	if creds[0].GetRegistry() != "registry" || creds[0].GetUsername() != "user" || creds[0].GetPassword() != "pass" {
		t.Fatalf("unexpected credential: %+v", creds[0])
	}
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

func findVolumeMount(container *runnerv1.ContainerSpec, volumeName string) *runnerv1.VolumeMount {
	if container == nil {
		return nil
	}
	for _, mount := range container.Mounts {
		if mount != nil && mount.GetVolume() == volumeName {
			return mount
		}
	}
	return nil
}

func findMountByPath(mounts []*runnerv1.VolumeMount, path string) *runnerv1.VolumeMount {
	for _, mount := range mounts {
		if mount != nil && mount.GetMountPath() == path {
			return mount
		}
	}
	return nil
}

func countMountsByPath(mounts []*runnerv1.VolumeMount, path string) int {
	count := 0
	for _, mount := range mounts {
		if mount != nil && mount.GetMountPath() == path {
			count++
		}
	}
	return count
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
