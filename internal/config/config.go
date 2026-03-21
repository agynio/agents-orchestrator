package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL               string
	ThreadsAddress            string
	NotificationsAddress      string
	AgentsAddress             string
	SecretsAddress            string
	ZitiIdentityFile          string
	ZitiManagementAddress     string
	DefaultAgentImage         string
	AgentThreadsAddress       string
	AgentNotificationsAddress string
	PollInterval              time.Duration
	IdleTimeout               time.Duration
	StopTimeoutSec            uint32
	LeaseName                 string
	LeaseNamespace            string
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
	cfg.ZitiIdentityFile = os.Getenv("ZITI_IDENTITY_FILE")
	if cfg.ZitiIdentityFile == "" {
		return Config{}, fmt.Errorf("ZITI_IDENTITY_FILE must be set")
	}
	cfg.ZitiManagementAddress = os.Getenv("ZITI_MANAGEMENT_ADDRESS")
	if cfg.ZitiManagementAddress == "" {
		cfg.ZitiManagementAddress = "ziti-management:50051"
	}
	cfg.DefaultAgentImage = os.Getenv("DEFAULT_AGENT_IMAGE")
	if cfg.DefaultAgentImage == "" {
		return Config{}, fmt.Errorf("DEFAULT_AGENT_IMAGE must be set")
	}
	cfg.AgentThreadsAddress = os.Getenv("AGENT_THREADS_ADDRESS")
	if cfg.AgentThreadsAddress == "" {
		cfg.AgentThreadsAddress = "threads:50051"
	}
	cfg.AgentNotificationsAddress = os.Getenv("AGENT_NOTIFICATIONS_ADDRESS")
	if cfg.AgentNotificationsAddress == "" {
		cfg.AgentNotificationsAddress = "notifications:50051"
	}

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
