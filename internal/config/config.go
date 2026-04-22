package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

type Config struct {
	ThreadsAddress            string
	NotificationsAddress      string
	AgentsAddress             string
	SecretsAddress            string
	RunnerAddress             string
	RunnersAddress            string
	MeteringServiceAddress    string
	ServiceIdentityID         uuid.UUID
	MeteringSampleInterval    time.Duration
	ZitiEnabled               bool
	ZitiManagementAddress     string
	ZitiLeaseRenewalInterval  time.Duration
	ZitiEnrollmentTimeout     time.Duration
	ZitiSidecarImage          string
	ClusterDNS                string
	AgentGatewayAddress       string
	AgentTracingAddress       string
	AgentLLMBaseURL           string
	PollInterval              time.Duration
	WorkloadReconcileInterval time.Duration
	IdleTimeout               time.Duration
	StopTimeoutSec            uint32
	LeaseName                 string
	LeaseNamespace            string
}

func FromEnv() (Config, error) {
	cfg := Config{}
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
	cfg.RunnersAddress = os.Getenv("RUNNERS_ADDRESS")
	if cfg.RunnersAddress == "" {
		cfg.RunnersAddress = "runners:50051"
	}
	cfg.MeteringServiceAddress = os.Getenv("METERING_SERVICE_ADDRESS")
	if cfg.MeteringServiceAddress == "" {
		cfg.MeteringServiceAddress = "metering:50051"
	}
	serviceIdentityID := strings.TrimSpace(os.Getenv("SERVICE_IDENTITY_ID"))
	if serviceIdentityID == "" {
		return Config{}, fmt.Errorf("SERVICE_IDENTITY_ID is required")
	}
	parsedIdentityID, err := uuidutil.ParseUUID(serviceIdentityID, "SERVICE_IDENTITY_ID")
	if err != nil {
		return Config{}, err
	}
	cfg.ServiceIdentityID = parsedIdentityID
	meteringSampleInterval := os.Getenv("METERING_SAMPLE_INTERVAL")
	if meteringSampleInterval == "" {
		cfg.MeteringSampleInterval = time.Minute
	} else {
		parsed, err := time.ParseDuration(meteringSampleInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse METERING_SAMPLE_INTERVAL: %w", err)
		}
		cfg.MeteringSampleInterval = parsed
	}
	if cfg.MeteringSampleInterval <= 0 {
		return Config{}, fmt.Errorf("METERING_SAMPLE_INTERVAL must be greater than 0")
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
	cfg.AgentTracingAddress = os.Getenv("AGENT_TRACING_ADDRESS")
	if cfg.AgentTracingAddress == "" {
		if cfg.ZitiEnabled {
			cfg.AgentTracingAddress = "tracing.ziti:443"
		} else {
			cfg.AgentTracingAddress = "tracing:50051"
		}
	}
	cfg.AgentLLMBaseURL = os.Getenv("AGENT_LLM_BASE_URL")
	if cfg.AgentLLMBaseURL == "" {
		if cfg.ZitiEnabled {
			cfg.AgentLLMBaseURL = "http://llm-proxy.ziti/v1"
		} else {
			cfg.AgentLLMBaseURL = "http://llm-proxy-llm-proxy.platform.svc.cluster.local:8080/v1"
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
	zitiEnrollmentTimeout := os.Getenv("ZITI_ENROLLMENT_TIMEOUT")
	if zitiEnrollmentTimeout == "" {
		cfg.ZitiEnrollmentTimeout = 2 * time.Minute
	} else {
		parsed, err := time.ParseDuration(zitiEnrollmentTimeout)
		if err != nil {
			return Config{}, fmt.Errorf("parse ZITI_ENROLLMENT_TIMEOUT: %w", err)
		}
		cfg.ZitiEnrollmentTimeout = parsed
	}
	if cfg.ZitiEnrollmentTimeout <= 0 {
		return Config{}, fmt.Errorf("ZITI_ENROLLMENT_TIMEOUT must be greater than 0")
	}
	cfg.ZitiSidecarImage = os.Getenv("ZITI_SIDECAR_IMAGE")
	if cfg.ZitiSidecarImage == "" {
		cfg.ZitiSidecarImage = "openziti/ziti-tunnel:2.0.0-pre8"
	}
	cfg.ClusterDNS = os.Getenv("CLUSTER_DNS")
	if cfg.ClusterDNS == "" {
		cfg.ClusterDNS = "10.43.0.10"
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

	workloadReconcileInterval := os.Getenv("WORKLOAD_RECONCILE_INTERVAL")
	if workloadReconcileInterval == "" {
		cfg.WorkloadReconcileInterval = time.Minute
	} else {
		parsed, err := time.ParseDuration(workloadReconcileInterval)
		if err != nil {
			return Config{}, fmt.Errorf("parse WORKLOAD_RECONCILE_INTERVAL: %w", err)
		}
		cfg.WorkloadReconcileInterval = parsed
	}
	if cfg.WorkloadReconcileInterval <= 0 {
		return Config{}, fmt.Errorf("WORKLOAD_RECONCILE_INTERVAL must be greater than 0")
	}

	idleTimeout := os.Getenv("IDLE_TIMEOUT")
	if idleTimeout == "" {
		cfg.IdleTimeout = 5 * time.Minute
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
