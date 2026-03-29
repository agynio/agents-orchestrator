package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL              string
	ThreadsAddress           string
	NotificationsAddress     string
	AgentsAddress            string
	SecretsAddress           string
	RunnerAddress            string
	ZitiEnabled              bool
	ZitiManagementAddress    string
	ZitiLeaseRenewalInterval time.Duration
	ZitiSidecarImage         string
	ClusterDNS               string
	DefaultInitImage         string
	AgentGatewayAddress      string
	AgentLLMBaseURL          string
	AgentModelOverride       string
	PollInterval             time.Duration
	IdleTimeout              time.Duration
	StopTimeoutSec           uint32
	LeaseName                string
	LeaseNamespace           string
}

func FromEnv() (Config, error) {
	cfg := Config{}
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be set")
	}
	cfg.ThreadsAddress = os.Getenv("THREADS_ADDRESS")
	if cfg.ThreadsAddress == "" {
		cfg.ThreadsAddress = "threads:50051"
	}
	cfg.NotificationsAddress = os.Getenv("NOTIFICATIONS_ADDRESS")
	if cfg.NotificationsAddress == "" {
		cfg.NotificationsAddress = "notifications:50051"
	}
	cfg.AgentsAddress = os.Getenv("AGENTS_ADDRESS")
	if cfg.AgentsAddress == "" {
		cfg.AgentsAddress = "agents:50051"
	}
	cfg.SecretsAddress = os.Getenv("SECRETS_ADDRESS")
	if cfg.SecretsAddress == "" {
		cfg.SecretsAddress = "secrets:50051"
	}
	cfg.RunnerAddress = os.Getenv("RUNNER_ADDRESS")
	if cfg.RunnerAddress == "" {
		cfg.RunnerAddress = "k8s-runner:50051"
	}
	zitiEnabled := os.Getenv("ZITI_ENABLED")
	if zitiEnabled != "" {
		parsed, err := strconv.ParseBool(zitiEnabled)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENABLED: %w", err)
		}
		cfg.ZitiEnabled = parsed
	}
	cfg.AgentGatewayAddress = os.Getenv("AGENT_GATEWAY_ADDRESS")
	if cfg.AgentGatewayAddress == "" {
		if cfg.ZitiEnabled {
			cfg.AgentGatewayAddress = "gateway.ziti:443"
		} else {
			cfg.AgentGatewayAddress = "gateway:8080"
		}
	}
	cfg.AgentLLMBaseURL = os.Getenv("AGENT_LLM_BASE_URL")
	if cfg.AgentLLMBaseURL == "" {
		if cfg.ZitiEnabled {
			cfg.AgentLLMBaseURL = "https://llm-proxy.ziti/v1"
		} else {
			cfg.AgentLLMBaseURL = "http://llm-proxy:8080/v1"
		}
	}
	cfg.ZitiManagementAddress = os.Getenv("ZITI_MANAGEMENT_ADDRESS")
	if cfg.ZitiManagementAddress == "" {
		cfg.ZitiManagementAddress = "ziti-management:50051"
	}
	zitiLeaseRenewalInterval := os.Getenv("ZITI_LEASE_RENEWAL_INTERVAL")
	if zitiLeaseRenewalInterval == "" {
		cfg.ZitiLeaseRenewalInterval = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(zitiLeaseRenewalInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_LEASE_RENEWAL_INTERVAL: %w", err)
		}
		cfg.ZitiLeaseRenewalInterval = parsed
	}
	if cfg.ZitiLeaseRenewalInterval <= 0 {
		return Config{}, fmt.Errorf("ZITI_LEASE_RENEWAL_INTERVAL must be greater than 0")
	}
	cfg.ZitiSidecarImage = os.Getenv("ZITI_SIDECAR_IMAGE")
	if cfg.ZitiSidecarImage == "" {
		cfg.ZitiSidecarImage = "openziti/ziti-tunnel:1.6.14"
	}
	cfg.ClusterDNS = os.Getenv("CLUSTER_DNS")
	if cfg.ClusterDNS == "" {
		cfg.ClusterDNS = "10.43.0.10"
	}
	cfg.DefaultInitImage = os.Getenv("DEFAULT_INIT_IMAGE")
	if cfg.DefaultInitImage == "" {
		return Config{}, fmt.Errorf("DEFAULT_INIT_IMAGE must be set")
	}
	cfg.AgentModelOverride = os.Getenv("AGENT_MODEL_OVERRIDE")

	pollInterval := os.Getenv("POLL_INTERVAL")
	if pollInterval == "" {
		cfg.PollInterval = 30 * time.Second
	} else {
		parsed, err := time.ParseDuration(pollInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse POLL_INTERVAL: %w", err)
		}
		cfg.PollInterval = parsed
	}

	idleTimeout := os.Getenv("IDLE_TIMEOUT")
	if idleTimeout == "" {
		cfg.IdleTimeout = 10 * time.Minute
	} else {
		parsed, err := time.ParseDuration(idleTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse IDLE_TIMEOUT: %w", err)
		}
		cfg.IdleTimeout = parsed
	}

	stopTimeout := os.Getenv("STOP_TIMEOUT_SEC")
	if stopTimeout == "" {
		cfg.StopTimeoutSec = 30
	} else {
		parsed, err := strconv.ParseUint(stopTimeout, 10, 32)
		if err != nil {
			return Config{}, fmt.Errorf("parse STOP_TIMEOUT_SEC: %w", err)
		}
		cfg.StopTimeoutSec = uint32(parsed)
	}

	cfg.LeaseName = os.Getenv("LEASE_NAME")
	if cfg.LeaseName == "" {
		cfg.LeaseName = "agents-orchestrator"
	}
	cfg.LeaseNamespace = os.Getenv("LEASE_NAMESPACE")
	return cfg, nil
}
