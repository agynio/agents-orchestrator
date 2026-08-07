package assembler

import (
	"context"
	"errors"
	"strings"
	"testing"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const (
	testSandboxImage      = "sandbox-image"
	testSandboxInitImage  = "sandbox-init-image"
	testSandboxFlavor     = "ram-2gb"
	testSandboxRunnerID   = "runner-1"
	testSandboxSizeGB     = "10"
	testSandboxEnvName    = "sandbox-env"
	testSandboxWorkspace  = "/workspace"
	testSandboxGatewayURL = "gateway:50051"
)

type fakeRunnersClient struct {
	listFlavors func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error)
}

func (f *fakeRunnersClient) ListFlavors(ctx context.Context, req *runnersv1.ListFlavorsRequest, opts ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
	if f.listFlavors != nil {
		return f.listFlavors(ctx, req, opts...)
	}
	return nil, errors.New("not implemented")
}

type sandboxFixture struct {
	sandbox           *agentsv1.Sandbox
	sandboxID         uuid.UUID
	environmentID     uuid.UUID
	ownerID           uuid.UUID
	workspaceVolumeID string
	agents            *testutil.FakeAgentsClient
	runners           *fakeRunnersClient
	secrets           *testutil.FakeSecretsClient
	cfg               *config.Config
	egressCACert      []byte
}

func newSandboxFixture() *sandboxFixture {
	sandboxID := uuid.New()
	environmentID := uuid.New()
	ownerID := uuid.New()
	fixture := &sandboxFixture{
		sandboxID:         sandboxID,
		environmentID:     environmentID,
		ownerID:           ownerID,
		workspaceVolumeID: uuid.NewString(),
		secrets:           &testutil.FakeSecretsClient{},
		cfg: &config.Config{
			AgentGatewayAddress:    testSandboxGatewayURL,
			AgentLLMBaseURL:        "http://llm:8080/v1",
			AgentTracingAddress:    "tracing:50051",
			SandboxInitImage:       testSandboxInitImage,
			SandboxWorkspaceSizeGB: testSandboxSizeGB,
		},
	}
	fixture.sandbox = &agentsv1.Sandbox{
		Meta:          &agentsv1.EntityMeta{Id: sandboxID.String()},
		Name:          "brave-otter",
		EnvironmentId: environmentID.String(),
		OwnerId:       ownerID.String(),
		Status:        agentsv1.SandboxStatus_SANDBOX_STATUS_STARTING,

		OrganizationId: "org-1",
	}
	fixture.agents = &testutil.FakeAgentsClient{
		GetEnvironmentFunc: func(_ context.Context, req *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
			if req.GetId() != environmentID.String() {
				return nil, errors.New("unexpected environment id")
			}
			return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
				Meta:           &agentsv1.EntityMeta{Id: environmentID.String()},
				OrganizationId: "org-1",
				Name:           testSandboxEnvName,
				Image:          testSandboxImage,
				RunnerId:       testSandboxRunnerID,
				Flavor:         testSandboxFlavor,
			}}, nil
		},
		ListEnvsFunc: func(context.Context, *agentsv1.ListEnvsRequest, ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
			return &agentsv1.ListEnvsResponse{}, nil
		},
	}
	fixture.runners = &fakeRunnersClient{
		listFlavors: func(_ context.Context, req *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			if req.GetRunnerId() != testSandboxRunnerID {
				return nil, errors.New("unexpected runner id")
			}
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{
					RunnerId:  testSandboxRunnerID,
					Name:      "other",
					Default:   true,
					Resources: &runnersv1.ComputeResources{RequestsCpu: "100m", RequestsMemory: "256Mi"},
				},
				{
					RunnerId:  testSandboxRunnerID,
					Name:      testSandboxFlavor,
					Resources: &runnersv1.ComputeResources{RequestsCpu: "500m", RequestsMemory: "1Gi"},
				},
			}}, nil
		},
	}
	return fixture
}

func (f *sandboxFixture) assemble(t *testing.T) *SandboxAssembleResult {
	t.Helper()
	assembler := NewWithRunnersAndEgressCA(f.agents, f.runners, f.secrets, f.cfg, f.egressCACert)
	result, err := assembler.AssembleSandbox(context.Background(), f.sandbox)
	if err != nil {
		t.Fatalf("assemble sandbox: %v", err)
	}
	return result
}

func TestAssembleSandboxUsesEnvironmentImageAndFlavor(t *testing.T) {
	fixture := newSandboxFixture()
	result := fixture.assemble(t)

	if result.RunnerID != testSandboxRunnerID {
		t.Fatalf("expected runner %q, got %q", testSandboxRunnerID, result.RunnerID)
	}
	// The resolved flavor name is carried out of assembly, not discarded:
	// it is written to the workload record and is what compute bills by.
	if result.Flavor != testSandboxFlavor {
		t.Fatalf("expected flavor %q, got %q", testSandboxFlavor, result.Flavor)
	}
	if result.OrganizationID != "org-1" {
		t.Fatalf("unexpected organization id %q", result.OrganizationID)
	}
	if result.EnvironmentID != fixture.environmentID {
		t.Fatalf("unexpected environment id %s", result.EnvironmentID)
	}
	if result.OwnerID != fixture.ownerID {
		t.Fatalf("unexpected owner id %s", result.OwnerID)
	}
	if result.WorkspaceVolumeID != fixture.workspaceVolumeID {
		t.Fatalf("unexpected workspace volume id %q", result.WorkspaceVolumeID)
	}
	if result.WorkspaceSizeGB != testSandboxSizeGB {
		t.Fatalf("unexpected workspace size %q", result.WorkspaceSizeGB)
	}
	if result.AllocatedCPUMillicores != 500 {
		t.Fatalf("unexpected allocated cpu %d", result.AllocatedCPUMillicores)
	}
	if result.AllocatedRAMBytes != 1<<30 {
		t.Fatalf("unexpected allocated ram %d", result.AllocatedRAMBytes)
	}
	main := result.Request.GetMain()
	if main == nil {
		t.Fatal("expected main container")
	}
	if main.GetImage() != testSandboxImage {
		t.Fatalf("expected environment image %q, got %q", testSandboxImage, main.GetImage())
	}
	expectedName := "sandbox-" + fixture.sandboxID.String()[:8]
	if main.GetName() != expectedName {
		t.Fatalf("expected main name %q, got %q", expectedName, main.GetName())
	}
	properties := result.Request.GetAdditionalProperties()
	if properties[LabelKeyPrefix+LabelSandboxID] != fixture.sandboxID.String() {
		t.Fatalf("unexpected sandbox id label: %v", properties)
	}
	if properties[LabelKeyPrefix+LabelSandboxOwnerID] != fixture.ownerID.String() {
		t.Fatalf("unexpected sandbox owner label: %v", properties)
	}
	if properties[LabelKeyPrefix+LabelEnvironmentID] != fixture.environmentID.String() {
		t.Fatalf("unexpected environment label: %v", properties)
	}
}

func TestAssembleSandboxRunsInitContainerAndHolderCommand(t *testing.T) {
	fixture := newSandboxFixture()
	result := fixture.assemble(t)

	initContainers := result.Request.GetInitContainers()
	if len(initContainers) != 1 {
		t.Fatalf("expected 1 init container, got %d", len(initContainers))
	}
	initContainer := initContainers[0]
	if initContainer.GetImage() != testSandboxInitImage {
		t.Fatalf("expected init image %q, got %q", testSandboxInitImage, initContainer.GetImage())
	}
	initMount := findVolumeMount(initContainer, agynBinVolumeName)
	if initMount == nil || initMount.GetMountPath() != agynBinMountPath {
		t.Fatalf("expected agyn-bin mount on the init container, got %v", initContainer.GetMounts())
	}
	if findVolumeSpec(result.Request.GetVolumes(), agynBinVolumeName) == nil {
		t.Fatalf("expected agyn-bin volume, got %v", result.Request.GetVolumes())
	}
	main := result.Request.GetMain()
	if !equalStringSlice(main.GetCmd(), []string{agynBinBinaryPath}) {
		t.Fatalf("unexpected main cmd %v", main.GetCmd())
	}
	mainBinMount := findVolumeMount(main, agynBinVolumeName)
	if mainBinMount == nil || mainBinMount.GetMountPath() != agynBinMountPath {
		t.Fatalf("expected agyn-bin mount on the main container, got %v", main.GetMounts())
	}
}

func TestAssembleSandboxSetsHolderMode(t *testing.T) {
	fixture := newSandboxFixture()
	result := fixture.assemble(t)

	envs := envMap(result.Request.GetMain().GetEnv())
	assertEnv(t, envs, "AGYND_MODE", SandboxHolderMode)
	assertEnv(t, envs, "SANDBOX_ID", fixture.sandboxID.String())
	assertEnv(t, envs, "SANDBOX_NAME", "brave-otter")
	assertEnv(t, envs, "SANDBOX_OWNER_ID", fixture.ownerID.String())
	assertEnv(t, envs, "ENVIRONMENT_ID", fixture.environmentID.String())
	assertEnv(t, envs, "ENVIRONMENT_NAME", testSandboxEnvName)
	assertEnv(t, envs, "WORKSPACE_DIR", testSandboxWorkspace)
}

func TestAssembleSandboxSetsNoAgentPlatformEnv(t *testing.T) {
	fixture := newSandboxFixture()
	result := fixture.assemble(t)

	for name := range envMap(result.Request.GetMain().GetEnv()) {
		if strings.HasPrefix(name, "AGENT_") {
			t.Fatalf("unexpected agent platform env %s on a sandbox workload", name)
		}
	}
}

func TestAssembleSandboxMountsPersistentWorkspace(t *testing.T) {
	fixture := newSandboxFixture()
	result := fixture.assemble(t)

	main := result.Request.GetMain()
	if main.GetWorkingDir() != SandboxWorkspaceMountPath {
		t.Fatalf("expected working dir %q, got %q", SandboxWorkspaceMountPath, main.GetWorkingDir())
	}
	workspaceMount := findVolumeMount(main, sandboxWorkspaceVolumeName)
	if workspaceMount == nil {
		t.Fatalf("expected workspace mount, got %v", main.GetMounts())
	}
	if workspaceMount.GetMountPath() != SandboxWorkspaceMountPath {
		t.Fatalf("expected workspace mount path %q, got %q", SandboxWorkspaceMountPath, workspaceMount.GetMountPath())
	}
	workspaceVolume := findVolumeSpec(result.Request.GetVolumes(), sandboxWorkspaceVolumeName)
	if workspaceVolume == nil {
		t.Fatalf("expected workspace volume, got %v", result.Request.GetVolumes())
	}
	if workspaceVolume.GetKind() != runnerv1.VolumeKind_VOLUME_KIND_NAMED {
		t.Fatalf("expected a named workspace volume, got %v", workspaceVolume.GetKind())
	}
	// The deterministic persistent name is what lets the workspace survive idle
	// stops and reconnects.
	expectedPersistentName := "sandbox-ws-" + fixture.sandboxID.String()
	if workspaceVolume.GetPersistentName() != expectedPersistentName {
		t.Fatalf("expected persistent name %q, got %q", expectedPersistentName, workspaceVolume.GetPersistentName())
	}
	labels := workspaceVolume.GetLabels()
	if labels[LabelSandboxID] != fixture.sandboxID.String() {
		t.Fatalf("unexpected workspace sandbox label: %v", labels)
	}
	if labels[LabelSandboxOwnerID] != fixture.ownerID.String() {
		t.Fatalf("unexpected workspace owner label: %v", labels)
	}
	if labels[LabelVolumeKey] != fixture.workspaceVolumeID {
		t.Fatalf("unexpected workspace volume key label: %v", labels)
	}
}

func TestAssembleSandboxInjectsEnvironmentEnvsAndSecrets(t *testing.T) {
	fixture := newSandboxFixture()
	secretID := uuid.NewString()
	fixture.agents.ListEnvsFunc = func(_ context.Context, req *agentsv1.ListEnvsRequest, _ ...grpc.CallOption) (*agentsv1.ListEnvsResponse, error) {
		if req.GetEnvironmentId() != fixture.environmentID.String() {
			return nil, errors.New("expected environment-scoped env listing")
		}
		return &agentsv1.ListEnvsResponse{Envs: []*agentsv1.Env{
			{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "PLAIN_ENV", Source: &agentsv1.Env_Value{Value: "plain"}},
			{Meta: &agentsv1.EntityMeta{Id: uuid.NewString()}, Name: "SECRET_ENV", Source: &agentsv1.Env_SecretId{SecretId: secretID}},
		}}, nil
	}
	fixture.secrets.ResolveSecretFunc = func(_ context.Context, req *secretsv1.ResolveSecretRequest, _ ...grpc.CallOption) (*secretsv1.ResolveSecretResponse, error) {
		if req.GetId() != secretID {
			return nil, errors.New("unexpected secret id")
		}
		return &secretsv1.ResolveSecretResponse{Value: "resolved-secret"}, nil
	}

	result := fixture.assemble(t)
	envs := envMap(result.Request.GetMain().GetEnv())
	assertEnv(t, envs, "PLAIN_ENV", "plain")
	assertEnv(t, envs, "SECRET_ENV", "resolved-secret")
}

func TestAssembleSandboxDistributesEgressCA(t *testing.T) {
	fixture := newSandboxFixture()
	fixture.egressCACert = []byte("egress-ca-cert")
	result := fixture.assemble(t)

	inlineFiles := result.Request.GetInlineFiles()
	// Contains, not equals: the inline file is the public roots with the egress
	// CA appended; see EgressCABundle.
	if !strings.Contains(string(inlineFiles[egressCACertPath]), "egress-ca-cert") {
		t.Fatalf("expected the egress CA inline file, got %v", inlineFiles)
	}
	main := result.Request.GetMain()
	if len(main.GetInlineFileMounts()) != 1 || main.GetInlineFileMounts()[0].GetPath() != egressCACertPath {
		t.Fatalf("expected the egress CA mounted into the main container, got %v", main.GetInlineFileMounts())
	}
	envs := envMap(main.GetEnv())
	assertEnv(t, envs, "SSL_CERT_FILE", egressCACertPath)
	assertEnv(t, envs, "REQUESTS_CA_BUNDLE", egressCACertPath)
	assertEnv(t, envs, "NODE_EXTRA_CA_CERTS", egressCACertPath)
	assertEnv(t, envs, "CURL_CA_BUNDLE", egressCACertPath)
	assertEnv(t, envs, "SSL_CERT_DIR", egressCACertDir)

	initContainer := result.Request.GetInitContainers()[0]
	if len(initContainer.GetInlineFileMounts()) != 1 || initContainer.GetInlineFileMounts()[0].GetPath() != egressCACertPath {
		t.Fatalf("expected the egress CA mounted into the init container, got %v", initContainer.GetInlineFileMounts())
	}
	initEnvs := envMap(initContainer.GetEnv())
	assertEnv(t, initEnvs, "SSL_CERT_FILE", egressCACertPath)
}

func TestResolveFlavorFallsBackToRunnerDefault(t *testing.T) {
	// An environment naming no flavor takes whatever the runner marks default.
	assembler := &Assembler{runners: &fakeRunnersClient{
		listFlavors: func(_ context.Context, _ *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{RunnerId: testSandboxRunnerID, Name: "ram-4gb"},
				{RunnerId: testSandboxRunnerID, Name: "ram-2gb", Default: true},
			}}, nil
		},
	}}

	flavor, err := assembler.resolveFlavor(context.Background(), testSandboxRunnerID, "")
	if err != nil {
		t.Fatalf("resolve flavor: %v", err)
	}
	if flavor.GetName() != "ram-2gb" {
		t.Fatalf("expected the default flavor, got %q", flavor.GetName())
	}
}

func TestResolveFlavorFailsWhenNameIsNotReported(t *testing.T) {
	// Late binding means an unknown name is a scheduling failure the retry
	// policy covers, not a silent fallback to some other size.
	assembler := &Assembler{runners: &fakeRunnersClient{
		listFlavors: func(_ context.Context, _ *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{RunnerId: testSandboxRunnerID, Name: "ram-2gb", Default: true},
			}}, nil
		},
	}}

	_, err := assembler.resolveFlavor(context.Background(), testSandboxRunnerID, "ram-64gb")
	if err == nil || !strings.Contains(err.Error(), "ram-64gb") {
		t.Fatalf("expected the unknown name to be reported, got %v", err)
	}
}

func TestResolveFlavorFailsWhenRunnerHasNoDefault(t *testing.T) {
	assembler := &Assembler{runners: &fakeRunnersClient{
		listFlavors: func(_ context.Context, _ *runnersv1.ListFlavorsRequest, _ ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{
				{RunnerId: testSandboxRunnerID, Name: "ram-2gb"},
			}}, nil
		},
	}}

	_, err := assembler.resolveFlavor(context.Background(), testSandboxRunnerID, "")
	if err == nil || !strings.Contains(err.Error(), "no default flavor") {
		t.Fatalf("expected a missing default to be reported, got %v", err)
	}
}
