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
	if cfg.AgentLLMBaseURL != "http://llm-proxy.ziti/v1" {
		t.Fatalf("expected llm base url %q, got %q", "http://llm-proxy.ziti/v1", cfg.AgentLLMBaseURL)
	}
	if cfg.ZitiEnrollmentTimeout != 2*time.Minute {
		t.Fatalf("expected ziti enrollment timeout %q, got %q", 2*time.Minute, cfg.ZitiEnrollmentTimeout)
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
	t.Setenv("ZITI_LEASE_RENEWAL_INTERVAL", "")
	t.Setenv("ZITI_ENROLLMENT_TIMEOUT", "")
	t.Setenv("ZITI_SIDECAR_IMAGE", "")
	t.Setenv("CLUSTER_DNS", "")
	t.Setenv("AGENT_GATEWAY_ADDRESS", "")
	t.Setenv("AGENT_TRACING_ADDRESS", "")
	t.Setenv("AGENT_LLM_BASE_URL", "")
	t.Setenv("POLL_INTERVAL", "")
	t.Setenv("IDLE_TIMEOUT", "")
	t.Setenv("STOP_TIMEOUT_SEC", "")
	t.Setenv("LEASE_NAME", "")
	t.Setenv("LEASE_NAMESPACE", "")
}
