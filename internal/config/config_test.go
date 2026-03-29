package config

import "testing"

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
	if cfg.AgentLLMBaseURL != "http://llm-proxy:8080/v1" {
		t.Fatalf("expected llm base url %q, got %q", "http://llm-proxy:8080/v1", cfg.AgentLLMBaseURL)
	}
	if cfg.ZitiSidecarImage != "openziti/ziti-tunnel:1.6.14" {
		t.Fatalf("expected ziti sidecar image %q, got %q", "openziti/ziti-tunnel:1.6.14", cfg.ZitiSidecarImage)
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
	if cfg.AgentLLMBaseURL != "https://llm-proxy.ziti/v1" {
		t.Fatalf("expected llm base url %q, got %q", "https://llm-proxy.ziti/v1", cfg.AgentLLMBaseURL)
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
}
