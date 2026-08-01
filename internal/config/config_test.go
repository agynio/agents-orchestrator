package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

const defaultZitiSidecarImage = "openziti/ziti-tunnel:1.6.15"

func TestFromEnvDefaultsNonZiti(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENABLED", "false")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.ZitiEnabled {
		t.Fatal("expected ZitiEnabled to be false")
	}
	if cfg.AgentGatewayAddress != "gateway:8080" {
		t.Fatalf("expected gateway address %q, got %q", "gateway:8080", cfg.AgentGatewayAddress)
	}
	if cfg.AgentTracingAddress != "tracing:50051" {
		t.Fatalf("expected tracing address %q, got %q", "tracing:50051", cfg.AgentTracingAddress)
	}
	if cfg.AgentLLMBaseURL != "http://llm-proxy-llm-proxy.platform.svc.cluster.local:8080/v1" {
		t.Fatalf("expected llm base url %q, got %q", "http://llm-proxy-llm-proxy.platform.svc.cluster.local:8080/v1", cfg.AgentLLMBaseURL)
	}
	if cfg.ZitiSidecarImage != defaultZitiSidecarImage {
		t.Fatalf("expected ziti sidecar image %q, got %q", defaultZitiSidecarImage, cfg.ZitiSidecarImage)
	}
	if cfg.ZitiEnrollmentTimeout != 2*time.Minute {
		t.Fatalf("expected ziti enrollment timeout %q, got %q", 2*time.Minute, cfg.ZitiEnrollmentTimeout)
	}
	if cfg.IdleTimeout != 5*time.Minute {
		t.Fatalf("expected idle timeout %q, got %q", 5*time.Minute, cfg.IdleTimeout)
	}
	if cfg.MeteringServiceAddress != "metering:50051" {
		t.Fatalf("expected metering service address %q, got %q", "metering:50051", cfg.MeteringServiceAddress)
	}
	if cfg.MeteringSampleInterval != time.Minute {
		t.Fatalf("expected metering sample interval %q, got %q", time.Minute, cfg.MeteringSampleInterval)
	}
	if cfg.GroupSyncEnabled {
		t.Fatal("expected group sync to be disabled")
	}
	if cfg.GroupsAddress != "groups:50051" {
		t.Fatalf("expected groups address %q, got %q", "groups:50051", cfg.GroupsAddress)
	}
	if cfg.NATSURL != "" {
		t.Fatalf("expected empty nats url, got %q", cfg.NATSURL)
	}
	if cfg.WorkloadReconcileInterval != time.Minute {
		t.Fatalf("expected workload reconcile interval %q, got %q", time.Minute, cfg.WorkloadReconcileInterval)
	}
}

func TestZitiSidecarImageUsesOfficialImage(t *testing.T) {
	configSource, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	chartValues, err := os.ReadFile("../../charts/agents-orchestrator/values.yaml")
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	devspaceConfig, err := os.ReadFile("../../devspace.yaml")
	if err != nil {
		t.Fatalf("read devspace config: %v", err)
	}
	for name, content := range map[string]string{
		"config.go":     string(configSource),
		"values.yaml":   string(chartValues),
		"devspace.yaml": string(devspaceConfig),
	} {
		if !strings.Contains(content, defaultZitiSidecarImage) {
			t.Fatalf("expected %s to use official ziti sidecar image %q", name, defaultZitiSidecarImage)
		}
	}
}

func TestZitiWorkflowKeepsSourceOfTruthRefsAndDnsValidation(t *testing.T) {
	e2e, err := os.ReadFile("../../.github/workflows/e2e.yml")
	if err != nil {
		t.Fatalf("read E2E workflow: %v", err)
	}
	e2eWorkflow := string(e2e)
	for _, expected := range []string{
		// Bootstrap no longer provisions this workflow: the VM does, and it
		// carries its own platform version rather than a ref to build from.
		"agynio/e2e/.github/actions/provision-vm@main",
		"K8S_RUNNER_REF: main",
		"github.event_name == 'workflow_dispatch' && inputs.k8s_runner_ref || env.K8S_RUNNER_REF",
		"name: Patch workload Ziti DNS runtime target",
		"current_router_target=",
		"kubectl get configmap ziti-workload-dns",
		"ziti.agyn.dev workload DNS from ",
		"to ziti-controller-client",
		"ziti-router.agyn.dev from ",
		"to ziti-router-edge",
		"dnsPolicy: None",
		"timeout 10 nc -vz -w 5 ziti-router.agyn.dev 2496",
		"name: Verify stock sidecar runtime DNS path",
		"image: openziti/ziti-tunnel:1.6.15",
		"bash -c '</dev/tcp/ziti.agyn.dev/2496'",
		"name: Verify gateway Ziti service binding",
		"gateway listening on ziti service gateway",
		"name: Verify llm-proxy Ziti service binding",
		"llm-proxy listening on ziti service llm-proxy",
	} {
		if !strings.Contains(e2eWorkflow, expected) {
			t.Fatalf("expected E2E workflow to contain %q", expected)
		}
	}
	for _, forbidden := range []string{
		"K8S_RUNNER_REF: noa/issue-73",
		// Bootstrap is deprecated; provisioning must not come back to it.
		"agynio/bootstrap/.github/actions/provision",
		"name: Build Ziti sidecar image",
		"build/ziti-tunnel-x509/Dockerfile",
		"k3d image import",
		"kubectl patch application gateway",
		"kubectl set env",
		"kubectl patch application llm-proxy",
	} {
		if strings.Contains(e2eWorkflow, forbidden) {
			t.Fatalf("expected E2E workflow not to contain %q", forbidden)
		}
	}
}

func TestFromEnvDefaultsZiti(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENABLED", "true")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if !cfg.ZitiEnabled {
		t.Fatal("expected ZitiEnabled to be true")
	}
	if cfg.AgentGatewayAddress != "gateway.ziti:443" {
		t.Fatalf("expected gateway address %q, got %q", "gateway.ziti:443", cfg.AgentGatewayAddress)
	}
	if cfg.AgentTracingAddress != "tracing.ziti:443" {
		t.Fatalf("expected tracing address %q, got %q", "tracing.ziti:443", cfg.AgentTracingAddress)
	}
	if cfg.AgentLLMBaseURL != "http://llm-proxy.ziti/v1" {
		t.Fatalf("expected llm base url %q, got %q", "http://llm-proxy.ziti/v1", cfg.AgentLLMBaseURL)
	}
	if cfg.ZitiEnrollmentTimeout != 2*time.Minute {
		t.Fatalf("expected ziti enrollment timeout %q, got %q", 2*time.Minute, cfg.ZitiEnrollmentTimeout)
	}
	if cfg.WorkloadDNSUpstream != "10.43.0.10" {
		t.Fatalf("expected workload DNS upstream %q, got %q", "10.43.0.10", cfg.WorkloadDNSUpstream)
	}
	if cfg.ZitiEnrollmentDNSUpstream != "10.43.0.10" {
		t.Fatalf("expected ziti enrollment DNS upstream %q, got %q", "10.43.0.10", cfg.ZitiEnrollmentDNSUpstream)
	}
	if cfg.ZitiEnrollmentControllerResolveHost != "ziti-controller-client.ziti.svc.cluster.local" {
		t.Fatalf("expected ziti enrollment controller resolve host %q, got %q", "ziti-controller-client.ziti.svc.cluster.local", cfg.ZitiEnrollmentControllerResolveHost)
	}
	if cfg.ZitiEnrollmentControllerPort != "2496" {
		t.Fatalf("expected ziti enrollment controller port %q, got %q", "2496", cfg.ZitiEnrollmentControllerPort)
	}
	if cfg.ZitiRuntimeControllerResolveHost != "ziti-controller-client.ziti.svc.cluster.local" {
		t.Fatalf("expected ziti runtime controller resolve host %q, got %q", "ziti-controller-client.ziti.svc.cluster.local", cfg.ZitiRuntimeControllerResolveHost)
	}
	if cfg.ZitiRuntimeControllerPort != "2496" {
		t.Fatalf("expected ziti runtime controller port %q, got %q", "2496", cfg.ZitiRuntimeControllerPort)
	}
}

func TestFromEnvWorkloadDNSUpstream(t *testing.T) {
	tests := []struct {
		name           string
		workloadDNS    string
		clusterDNS     string
		enrollmentDNS  string
		expected       string
		expectedEnroll string
	}{
		{
			name:           "default",
			expected:       "10.43.0.10",
			expectedEnroll: "10.43.0.10",
		},
		{
			name:           "deprecated cluster dns fallback",
			clusterDNS:     "10.43.0.20",
			expected:       "10.43.0.20",
			expectedEnroll: "10.43.0.20",
		},
		{
			name:           "workload dns upstream takes precedence",
			workloadDNS:    "10.43.0.30",
			clusterDNS:     "10.43.0.20",
			expected:       "10.43.0.30",
			expectedEnroll: "10.43.0.20",
		},
		{
			name:           "enrollment dns upstream takes precedence",
			workloadDNS:    "10.43.0.30",
			clusterDNS:     "10.43.0.20",
			enrollmentDNS:  "10.43.0.40",
			expected:       "10.43.0.30",
			expectedEnroll: "10.43.0.40",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("WORKLOAD_DNS_UPSTREAM", tt.workloadDNS)
			t.Setenv("CLUSTER_DNS", tt.clusterDNS)
			t.Setenv("ZITI_ENROLLMENT_DNS_UPSTREAM", tt.enrollmentDNS)

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv: %v", err)
			}
			if cfg.WorkloadDNSUpstream != tt.expected {
				t.Fatalf("expected workload DNS upstream %q, got %q", tt.expected, cfg.WorkloadDNSUpstream)
			}
			if cfg.ZitiEnrollmentDNSUpstream != tt.expectedEnroll {
				t.Fatalf("expected ziti enrollment DNS upstream %q, got %q", tt.expectedEnroll, cfg.ZitiEnrollmentDNSUpstream)
			}
		})
	}
}

func TestFromEnvAgentTracingAddress(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("AGENT_TRACING_ADDRESS", "tracing:50051")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.AgentTracingAddress != "tracing:50051" {
		t.Fatalf("expected tracing address %q, got %q", "tracing:50051", cfg.AgentTracingAddress)
	}
}

func TestChartLeavesWorkloadAddressesToConfigDefaults(t *testing.T) {
	values, err := os.ReadFile("../../charts/agents-orchestrator/values.yaml")
	if err != nil {
		t.Fatalf("read chart values: %v", err)
	}
	chartValues := string(values)
	for _, forbidden := range []string{
		"name: AGENT_GATEWAY_ADDRESS\n    value: \"gateway:8080\"",
		"name: AGENT_TRACING_ADDRESS\n    value: \"tracing:50051\"",
	} {
		if strings.Contains(chartValues, forbidden) {
			t.Fatalf("expected chart not to pin non-Ziti workload address %q", forbidden)
		}
	}
	for _, expected := range []string{
		"name: AGENT_GATEWAY_ADDRESS\n    value: \"\"",
		"name: AGENT_TRACING_ADDRESS\n    value: \"\"",
	} {
		if !strings.Contains(chartValues, expected) {
			t.Fatalf("expected chart to leave workload address empty for config defaults: %q", expected)
		}
	}
}

func setBaseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("THREADS_ADDRESS", "")
	t.Setenv("NOTIFICATIONS_ADDRESS", "")
	t.Setenv("AGENTS_ADDRESS", "")
	t.Setenv("SECRETS_ADDRESS", "")
	t.Setenv("RUNNER_ADDRESS", "")
	t.Setenv("RUNNERS_ADDRESS", "")
	t.Setenv("METERING_SERVICE_ADDRESS", "")
	t.Setenv("METERING_SAMPLE_INTERVAL", "")
	t.Setenv("ZITI_MANAGEMENT_ADDRESS", "")
	t.Setenv("GROUP_SYNC_ENABLED", "")
	t.Setenv("GROUPS_ADDRESS", "")
	t.Setenv("NATS_URL", "")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_ENROLLMENT_TIMEOUT", "")
	t.Setenv("ZITI_SIDECAR_IMAGE", "")
	t.Setenv("WORKLOAD_DNS_UPSTREAM", "")
	t.Setenv("CLUSTER_DNS", "")
	t.Setenv("ZITI_ENROLLMENT_DNS_UPSTREAM", "")
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST", "")
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_PORT", "")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_RESOLVE_HOST", "")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_PORT", "")
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_TRACING_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
	t.Setenv("AGYND_AGENTS_DIRECT_ADDRESS", "")
	t.Setenv("AGYND_RUNNERS_DIRECT_ADDRESS", "")
	t.Setenv("SANDBOX_INIT_IMAGE", "")
	t.Setenv("SANDBOX_WORKSPACE_SIZE_GB", "")
	t.Setenv("SANDBOX_RECONCILE_ORGANIZATION_IDS", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("WORKLOAD_RECONCILE_INTERVAL", "")
	t.Setenv("IDLE_TIMEOUT", "")
	t.Setenv("STOP_TIMEOUT_SEC", "")
	t.Setenv("LEASE_NAME", "")
	t.Setenv("LEASE_NAMESPACE", "")
	t.Setenv("EGRESS_CA_NAMESPACE", "")
}

func TestFromEnvAgyndDirectAddresses(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("AGYND_AGENTS_DIRECT_ADDRESS", "10.42.0.10:50051")
	t.Setenv("AGYND_RUNNERS_DIRECT_ADDRESS", "10.42.0.11:50051")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.AgyndAgentsDirectAddress != "10.42.0.10:50051" {
		t.Fatalf("expected agents direct address, got %q", cfg.AgyndAgentsDirectAddress)
	}
	if cfg.AgyndRunnersDirectAddress != "10.42.0.11:50051" {
		t.Fatalf("expected runners direct address, got %q", cfg.AgyndRunnersDirectAddress)
	}
}

func TestFromEnvGroupSyncConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("GROUP_SYNC_ENABLED", "true")
	t.Setenv("GROUPS_ADDRESS", "groups.internal:50051")
	t.Setenv("NATS_URL", "nats://nats:4222")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if !cfg.GroupSyncEnabled {
		t.Fatal("expected group sync to be enabled")
	}
	if cfg.GroupsAddress != "groups.internal:50051" {
		t.Fatalf("expected groups address %q, got %q", "groups.internal:50051", cfg.GroupsAddress)
	}
	if cfg.NATSURL != "nats://nats:4222" {
		t.Fatalf("expected nats url %q, got %q", "nats://nats:4222", cfg.NATSURL)
	}
}

func TestFromEnvGroupSyncEnabledInvalid(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("GROUP_SYNC_ENABLED", "not-bool")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected GROUP_SYNC_ENABLED parse error")
	}
}

func TestFromEnvZitiRuntimeController(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST", "ziti-controller-client.ziti.svc.cluster.local")
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_PORT", "2496")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_RESOLVE_HOST", "istio-ingressgateway.istio-gateway.svc.cluster.local")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_PORT", "443")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.ZitiRuntimeControllerResolveHost != "istio-ingressgateway.istio-gateway.svc.cluster.local" {
		t.Fatalf("expected runtime controller resolve host, got %q", cfg.ZitiRuntimeControllerResolveHost)
	}
	if cfg.ZitiRuntimeControllerPort != "443" {
		t.Fatalf("expected runtime controller port %q, got %q", "443", cfg.ZitiRuntimeControllerPort)
	}
	if cfg.ZitiEnrollmentControllerResolveHost != "ziti-controller-client.ziti.svc.cluster.local" {
		t.Fatalf("expected enrollment controller resolve host, got %q", cfg.ZitiEnrollmentControllerResolveHost)
	}
	if cfg.ZitiEnrollmentControllerPort != "2496" {
		t.Fatalf("expected enrollment controller port %q, got %q", "2496", cfg.ZitiEnrollmentControllerPort)
	}
}

func TestFromEnvZitiControllerDefaults(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST", "")
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_PORT", "")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_RESOLVE_HOST", "")
	t.Setenv("ZITI_RUNTIME_CONTROLLER_PORT", "")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.ZitiEnrollmentControllerResolveHost != "ziti-controller-client.ziti.svc.cluster.local" {
		t.Fatalf("expected default enrollment controller resolve host, got %q", cfg.ZitiEnrollmentControllerResolveHost)
	}
	if cfg.ZitiEnrollmentControllerPort != "2496" {
		t.Fatalf("expected default enrollment controller port, got %q", cfg.ZitiEnrollmentControllerPort)
	}
	if cfg.ZitiRuntimeControllerResolveHost != "ziti-controller-client.ziti.svc.cluster.local" {
		t.Fatalf("expected default runtime controller resolve host, got %q", cfg.ZitiRuntimeControllerResolveHost)
	}
	if cfg.ZitiRuntimeControllerPort != "2496" {
		t.Fatalf("expected default runtime controller port, got %q", cfg.ZitiRuntimeControllerPort)
	}
}

func TestFromEnvZitiEnrollmentControllerPortInvalid(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_ENROLLMENT_CONTROLLER_PORT", "not-a-port")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected ZITI_ENROLLMENT_CONTROLLER_PORT parse error")
	}
}

func TestFromEnvZitiRuntimeControllerPortInvalid(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("ZITI_RUNTIME_CONTROLLER_PORT", "not-a-port")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected ZITI_RUNTIME_CONTROLLER_PORT parse error")
	}
}

func TestFromEnvSandboxReconcileOrganizationIDs(t *testing.T) {
	setBaseEnv(t)
	first := "11111111-1111-1111-1111-111111111111"
	second := "22222222-2222-2222-2222-222222222222"
	t.Setenv("SANDBOX_RECONCILE_ORGANIZATION_IDS", first+", "+second)

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if len(cfg.SandboxReconcileOrganizationIDs) != 2 {
		t.Fatalf("expected 2 organization ids, got %d", len(cfg.SandboxReconcileOrganizationIDs))
	}
	if cfg.SandboxReconcileOrganizationIDs[0] != first || cfg.SandboxReconcileOrganizationIDs[1] != second {
		t.Fatalf("unexpected organization ids: %v", cfg.SandboxReconcileOrganizationIDs)
	}
}

func TestFromEnvSandboxReconcileOrganizationIDsInvalid(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("SANDBOX_RECONCILE_ORGANIZATION_IDS", "not-a-uuid")

	_, err := FromEnv()
	if err == nil {
		t.Fatal("expected SANDBOX_RECONCILE_ORGANIZATION_IDS parse error")
	}
}
