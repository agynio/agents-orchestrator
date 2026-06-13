package config

import (
	"testing"
	"time"
)

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
	if cfg.ZitiSidecarImage != "openziti/ziti-tunnel:2.0.0-pre8" {
		t.Fatalf("expected ziti sidecar image %q, got %q", "openziti/ziti-tunnel:2.0.0-pre8", cfg.ZitiSidecarImage)
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
}

func TestFromEnvWorkloadDNSUpstream(t *testing.T) {
	tests := []struct {
		name        string
		workloadDNS string
		clusterDNS  string
		expected    string
	}{
		{
			name:     "default",
			expected: "10.43.0.10",
		},
		{
			name:       "deprecated cluster dns fallback",
			clusterDNS: "10.43.0.20",
			expected:   "10.43.0.20",
		},
		{
			name:        "workload dns upstream takes precedence",
			workloadDNS: "10.43.0.30",
			clusterDNS:  "10.43.0.20",
			expected:    "10.43.0.30",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setBaseEnv(t)
			t.Setenv("WORKLOAD_DNS_UPSTREAM", tt.workloadDNS)
			t.Setenv("CLUSTER_DNS", tt.clusterDNS)

			cfg, err := FromEnv()
			if err != nil {
				t.Fatalf("FromEnv: %v", err)
			}
			if cfg.WorkloadDNSUpstream != tt.expected {
				t.Fatalf("expected workload DNS upstream %q, got %q", tt.expected, cfg.WorkloadDNSUpstream)
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
	t.Setenv("GROUPS_ADDRESS", "")
	t.Setenv("NATS_URL", "")
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_ENROLLMENT_TIMEOUT", "")
	t.Setenv("ZITI_SIDECAR_IMAGE", "")
	t.Setenv("WORKLOAD_DNS_UPSTREAM", "")
	t.Setenv("CLUSTER_DNS", "")
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_TRACING_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("WORKLOAD_RECONCILE_INTERVAL", "")
	t.Setenv("IDLE_TIMEOUT", "")
	t.Setenv("STOP_TIMEOUT_SEC", "")
	t.Setenv("LEASE_NAME", "")
	t.Setenv("LEASE_NAMESPACE", "")
	t.Setenv("EGRESS_CA_NAMESPACE", "")
}

func TestFromEnvGroupSyncConfig(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("GROUPS_ADDRESS", "groups.internal:50051")
	t.Setenv("NATS_URL", "nats://nats:4222")

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if cfg.GroupsAddress != "groups.internal:50051" {
		t.Fatalf("expected groups address %q, got %q", "groups.internal:50051", cfg.GroupsAddress)
	}
	if cfg.NATSURL != "nats://nats:4222" {
		t.Fatalf("expected nats url %q, got %q", "nats://nats:4222", cfg.NATSURL)
	}
}
