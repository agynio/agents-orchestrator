package assembler

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const (
	listPageSize                              int32 = 100
	rpcTimeout                                      = 10 * time.Second
	agynBinVolumeName                               = "agyn-bin"
	agynBinMountPath                                = "/agyn-bin"
	agynBinBinaryPath                               = "/agyn-bin/agynd"
	mcpBasePort                                     = 8100
	ZitiEnrollContainerName                         = "ziti-enroll"
	ZitiSidecarContainerName                        = "ziti-sidecar"
	zitiIdentityVolumeName                          = "ziti-identity"
	zitiIdentityMountPath                           = "/netfoundry"
	ZitiIdentityBasename                            = "agent"
	ZitiEnrollmentTokenEnvVar                       = "ZITI_ENROLL_TOKEN"
	ZitiIdentityBasenameEnvVar                      = "ZITI_IDENTITY_BASENAME"
	ZitiIdentityDirEnvVar                           = "ZITI_IDENTITY_DIR"
	ZitiEnrollmentControllerResolveHostEnvVar       = "ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST"
	ZitiEnrollmentControllerPortEnvVar              = "ZITI_ENROLLMENT_CONTROLLER_PORT"
	egressCACertPath                                = "/etc/agyn/egress-ca/ca.crt"
	egressCACertDir                                 = "/etc/agyn/egress-ca"
	zitiDNSNameserver                               = "127.0.0.1"
	zitiEnrollEntrypoint                            = "/usr/bin/bash"
	zitiSidecarEntrypoint                           = "/usr/bin/bash"
	zitiSidecarBinaryPath                           = "/usr/local/bin/ziti"
	zitiSidecarCommand                              = "tunnel"
	zitiSidecarMode                                 = "tproxy"
	zitiSidecarServicePollRate                      = "1"
	zitiEnrollScript                                = `workload_dns_upstream="$1"
workload_dns_nameserver="$2"
enrollment_controller_resolve_host="$3"
enrollment_controller_port_override="$4"
runtime_controller_resolve_host="$5"
runtime_controller_port_override="$6"
identity_dir="${ZITI_IDENTITY_DIR}"
identity_basename="${ZITI_IDENTITY_BASENAME}"
identity_file="${identity_dir}/${identity_basename}.json"
jwt_file="${identity_dir}/${identity_basename}.jwt"
ziti_controller_cert="${identity_dir}/controller-ca.pem"
ziti_tls_ca_cert="${identity_dir}/controller-tls-ca.pem"
ziti_key_file="${identity_dir}/${identity_basename}.key"
ziti_csr_file="${identity_dir}/${identity_basename}.csr"
ziti_cert_file="${identity_dir}/${identity_basename}.crt"
resolv_file="${ZITI_RESOLV_CONF:-/etc/resolv.conf}"
hosts_file="${ZITI_HOSTS_FILE:-/etc/hosts}"

printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"
mkdir -p "${identity_dir}"

if [[ ! -s "${identity_file}" ]]; then
  if [[ -z "${ZITI_ENROLL_TOKEN}" ]]; then
    echo "ZITI_ENROLL_TOKEN is required" >&2
    exit 1
  fi
  if [[ -n "${ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST:-}" ]]; then
    enrollment_controller_resolve_host="${ZITI_ENROLLMENT_CONTROLLER_RESOLVE_HOST}"
  fi
  if [[ -n "${ZITI_ENROLLMENT_CONTROLLER_PORT:-}" ]]; then
    enrollment_controller_port_override="${ZITI_ENROLLMENT_CONTROLLER_PORT}"
  fi
  printf '%s\n' "${ZITI_ENROLL_TOKEN}" > "${jwt_file}"

  jwt_payload="${ZITI_ENROLL_TOKEN#*.}"
  jwt_payload="${jwt_payload%%.*}"
  jwt_payload="$(printf '%s' "${jwt_payload}" | tr '_-' '/+')"
  case $(( ${#jwt_payload} % 4 )) in
    2) jwt_payload="${jwt_payload}==" ;;
    3) jwt_payload="${jwt_payload}=" ;;
  esac
  jwt_payload_json="$(printf '%s' "${jwt_payload}" | base64 -d 2>/dev/null || true)"
  ziti_controller_host="$(printf '%s' "${jwt_payload_json}" | sed -nE 's/.*"iss"[[:space:]]*:[[:space:]]*"https?:\/\/([^"\/:]+).*/\1/p' | head -n 1)"
  if [[ -n "${ziti_controller_host}" ]]; then
    awk -v host="${ziti_controller_host}" '($1 ~ /^(127\.|::1$)/) { for (i = 2; i <= NF; i++) if ($i == host) next } { print }' "${hosts_file}" > "${hosts_file}.tmp"
    cat "${hosts_file}.tmp" > "${hosts_file}"
    rm -f "${hosts_file}.tmp"
  fi
  printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"

  jwt_payload_file="${identity_dir}/${identity_basename}.payload.json"
  printf '%s\n' "${jwt_payload_json}" > "${jwt_payload_file}"
  ziti_controller_url="$(jq -r '.iss // empty' "${jwt_payload_file}")"
  ziti_enrollment_method="$(jq -r '.em // empty' "${jwt_payload_file}")"
  ziti_enrollment_token_id="$(jq -r '.jti // empty' "${jwt_payload_file}")"
  ziti_identity_subject="$(jq -r '.sub // empty' "${jwt_payload_file}")"
  if [[ -z "${ziti_controller_url}" || -z "${ziti_enrollment_method}" || -z "${ziti_enrollment_token_id}" || -z "${ziti_identity_subject}" ]]; then
    echo "ZITI_ENROLL_TOKEN is missing required iss, em, jti, or sub claims" >&2
    exit 1
  fi

  ziti_controller_hostport="$(printf '%s\n' "${ziti_controller_url}" | sed -nE 's#^https?://([^/]+).*#\1#p')"
  ziti_controller_host="${ziti_controller_hostport%%:*}"
  ziti_controller_port="${ziti_controller_hostport##*:}"
  if [[ "${ziti_controller_port}" == "${ziti_controller_hostport}" ]]; then
    ziti_controller_port="443"
  fi
  ziti_enrollment_controller_port="${ziti_controller_port}"
  if [[ -n "${enrollment_controller_port_override}" ]]; then
    ziti_enrollment_controller_port="${enrollment_controller_port_override}"
  fi
  ziti_enrollment_resolve_host="${ziti_controller_host}"
  if [[ -n "${enrollment_controller_resolve_host}" ]]; then
    ziti_enrollment_resolve_host="${enrollment_controller_resolve_host}"
  fi
  ziti_enrollment_controller_ip="$(getent ahostsv4 "${ziti_enrollment_resolve_host}" 2>/dev/null | awk '$2 == "STREAM" { print $1; exit }' || true)"
  if [[ -z "${ziti_enrollment_controller_ip}" ]]; then
    ziti_enrollment_controller_ip="$(awk -v host="${ziti_enrollment_resolve_host}" '{ for (i = 2; i <= NF; i++) if ($i == host) { print $1; exit } }' "${hosts_file}")"
  fi
  if [[ -z "${ziti_enrollment_controller_ip}" ]]; then
    echo "expected resolved controller address for ${ziti_enrollment_resolve_host}" >&2
    exit 1
  fi
  ziti_runtime_controller_host="${ziti_controller_host}"
  ziti_runtime_controller_port="${ziti_controller_port}"
  if [[ -n "${runtime_controller_port_override}" ]]; then
    ziti_runtime_controller_port="${runtime_controller_port_override}"
  fi
  ziti_enroll_url="https://${ziti_controller_host}:${ziti_enrollment_controller_port}/edge/client/v1/enroll?method=${ziti_enrollment_method}&token=${ziti_enrollment_token_id}"

  openssl s_client -showcerts -servername "${ziti_controller_host}" -connect "${ziti_enrollment_controller_ip}:${ziti_enrollment_controller_port}" </dev/null 2>/dev/null | awk '/BEGIN CERTIFICATE/,/END CERTIFICATE/ { print }' > "${ziti_controller_cert}"
  if [[ ! -s "${ziti_controller_cert}" ]]; then
    echo "expected controller certificate from ${ziti_controller_hostport}" >&2
    exit 1
  fi
  cat "${ziti_controller_cert}" > "${ziti_tls_ca_cert}"
  if [[ -s "${SSL_CERT_FILE:-}" ]]; then
    cat "${SSL_CERT_FILE}" >> "${ziti_tls_ca_cert}"
  fi
  openssl ecparam -name secp384r1 -genkey -noout -out "${ziti_key_file}"
  openssl req -new -key "${ziti_key_file}" -subj "/C=US/O=NetFoundry/CN=${ziti_identity_subject}" -out "${ziti_csr_file}"
  enroll_response="$(curl --fail-with-body --show-error --silent --cacert "${ziti_tls_ca_cert}" --resolve "${ziti_controller_host}:${ziti_enrollment_controller_port}:${ziti_enrollment_controller_ip}" -H 'content-type: application/x-pem-file' --data-binary "@${ziti_csr_file}" "${ziti_enroll_url}")"
  if printf '%s' "${enroll_response}" | jq -e . >/dev/null 2>&1; then
    printf '%s' "${enroll_response}" | jq -r '.data.cert // empty' > "${ziti_cert_file}"
  else
    printf '%s' "${enroll_response}" > "${ziti_cert_file}"
  fi
  if [[ ! -s "${ziti_cert_file}" ]]; then
    echo "expected certificate in ziti enrollment response" >&2
    exit 1
  fi
  jq -n --arg ztAPI "https://${ziti_runtime_controller_host}:${ziti_runtime_controller_port}/edge/client/v1" --arg cert "pem:$(cat "${ziti_cert_file}")" --arg key "pem:$(cat "${ziti_key_file}")" --arg ca "pem:$(cat "${ziti_tls_ca_cert}")" '{ztAPI: $ztAPI, id: {cert: $cert, key: $key, ca: $ca}}' > "${identity_file}"
fi

if [[ ! -s "${identity_file}" ]]; then
  echo "expected identity file ${identity_file}" >&2
  exit 1
fi

if [[ -n "${runtime_controller_resolve_host}" ]]; then
  if [[ ! -s "${ziti_tls_ca_cert}" ]]; then
    echo "expected controller CA bundle ${ziti_tls_ca_cert}" >&2
    exit 1
  fi
  ziti_runtime_controller_ip="$(getent ahostsv4 "${runtime_controller_resolve_host}" 2>/dev/null | awk '$2 == "STREAM" { print $1; exit }' || true)"
  if [[ -z "${ziti_runtime_controller_ip}" ]]; then
    echo "expected resolved runtime controller address for ${runtime_controller_resolve_host}" >&2
    exit 1
  fi
  ziti_runtime_controller_url="$(jq -r '.ztAPI // empty' "${identity_file}")"
  ziti_runtime_controller_hostport="$(printf '%s\n' "${ziti_runtime_controller_url}" | sed -nE 's#^https?://([^/]+).*#\1#p')"
  if [[ -z "${ziti_runtime_controller_hostport}" ]]; then
    echo "expected runtime controller endpoint in ${identity_file}" >&2
    exit 1
  fi
  ziti_runtime_controller_host="${ziti_runtime_controller_hostport%%:*}"
  ziti_runtime_controller_port="${ziti_runtime_controller_hostport##*:}"
  if [[ "${ziti_runtime_controller_port}" == "${ziti_runtime_controller_hostport}" ]]; then
    ziti_runtime_controller_port="443"
  fi
  if [[ -n "${runtime_controller_port_override}" ]]; then
    ziti_runtime_controller_port="${runtime_controller_port_override}"
    jq --arg ztAPI "https://${ziti_runtime_controller_host}:${ziti_runtime_controller_port}/edge/client/v1" '.ztAPI = $ztAPI' "${identity_file}" > "${identity_file}.tmp"
    cat "${identity_file}.tmp" > "${identity_file}"
    rm -f "${identity_file}.tmp"
  fi
fi

printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_nameserver}" > "${resolv_file}"`
	zitiSidecarScript = `workload_dns_upstream="$1"
runtime_controller_resolve_host="$2"
runtime_controller_port_override="$3"
enrollment_controller_resolve_host="$4"
runtime_controller_dns_upstream="${workload_dns_upstream}"
identity_file="${ZITI_IDENTITY_DIR}/${ZITI_IDENTITY_BASENAME}.json"
hosts_file="${ZITI_HOSTS_FILE:-/etc/hosts}"
resolv_file="${ZITI_RESOLV_CONF:-/etc/resolv.conf}"
if [[ -n "${runtime_controller_resolve_host}" ]]; then
  printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${runtime_controller_dns_upstream}" > "${resolv_file}"
  ziti_runtime_controller_url="$(jq -r '.ztAPI // empty' "${identity_file}")"
  ziti_runtime_controller_hostport="$(printf '%s\n' "${ziti_runtime_controller_url}" | sed -nE 's#^https?://([^/]+).*#\1#p')"
  if [[ -z "${ziti_runtime_controller_hostport}" ]]; then
    echo "expected runtime controller endpoint in ${identity_file}" >&2
    exit 1
  fi
  ziti_runtime_controller_host="${ziti_runtime_controller_hostport%%:*}"
  ziti_runtime_controller_port="${ziti_runtime_controller_hostport##:*}"
  if [[ "${ziti_runtime_controller_port}" == "${ziti_runtime_controller_hostport}" ]]; then
    ziti_runtime_controller_port="443"
  fi
  if [[ -n "${runtime_controller_port_override}" ]]; then
    ziti_runtime_controller_port="${runtime_controller_port_override}"
  fi
  ziti_runtime_controller_ip="$(getent ahostsv4 "${runtime_controller_resolve_host}" 2>/dev/null | awk '$2 == "STREAM" { print $1; exit }' || true)"
  if [[ -z "${ziti_runtime_controller_ip}" ]]; then
    echo "expected resolved runtime controller address for ${runtime_controller_resolve_host}" >&2
    exit 1
  fi
  printf 'nameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "${workload_dns_upstream}" > "${resolv_file}"
  getent hosts "${ziti_runtime_controller_host}" || true
fi
printf 'nameserver %s\nnameserver %s\nsearch svc.cluster.local cluster.local\noptions ndots:5\n' "127.0.0.1" "${workload_dns_upstream}" > "${resolv_file}"
export GODEBUG="${GODEBUG:+${GODEBUG},}netdns=cgo"
exec "${ZITI_SIDECAR_BINARY}" "${ZITI_SIDECAR_COMMAND}" "${ZITI_SIDECAR_MODE}" --identity "${identity_file}" --dnsUpstream "udp://${workload_dns_upstream}:53" --dnsUpstream "tcp://${workload_dns_upstream}:53" --svcPollRate "${ZITI_SIDECAR_SERVICE_POLL_RATE}"`
	zitiRequiredCapabilityNetAdmin = "NET_ADMIN"
	zitiRestartPolicyKey           = "restart_policy"
	zitiRestartPolicyAlways        = "Always"
	zitiDNSSearchService           = "svc.cluster.local"
	zitiDNSSearchCluster           = "cluster.local"
	zitiGatewayWaitContainerName   = "ziti-gateway-wait"
	zitiGatewayWaitImage           = "busybox:1.37.0"
	zitiGatewayWaitTimeoutSeconds  = 60
)

var reservedEnvNames = map[string]struct{}{
	"AGENT_ID":                    {},
	"AGENT_NAME":                  {},
	"AGENT_ROLE":                  {},
	"AGENT_MODEL":                 {},
	"AGENT_CONFIG":                {},
	"THREAD_ID":                   {},
	"WORKLOAD_ID":                 {},
	"GATEWAY_ADDRESS":             {},
	"AGYN_GATEWAY_URL":            {},
	"LLM_BASE_URL":                {},
	"TRACING_ADDRESS":             {},
	"OTEL_EXPORTER_OTLP_ENDPOINT": {},
	"SSL_CERT_FILE":               {},
	"REQUESTS_CA_BUNDLE":          {},
	"NODE_EXTRA_CA_CERTS":         {},
	"CURL_CA_BUNDLE":              {},
	"SSL_CERT_DIR":                {},
	"AGENT_MCP_SERVERS":           {},
	"MCP_PORT":                    {},
	ZitiEnrollmentTokenEnvVar:     {},
	ZitiIdentityBasenameEnvVar:    {},
	ZitiIdentityDirEnvVar:         {},
}

type Assembler struct {
	agents       agentsv1.AgentsServiceClient
	secrets      secretsv1.SecretsServiceClient
	cfg          *config.Config
	egressCACert []byte
}

type AssembleResult struct {
	Request                *runnerv1.StartWorkloadRequest
	OrganizationID         string
	RunnerLabels           map[string]string
	PersistentVolumes      []PersistentVolumeInfo
	AllocatedCPUMillicores int32
	AllocatedRAMBytes      int64
}

type PersistentVolumeInfo struct {
	ID     uuid.UUID
	Volume *agentsv1.Volume
	Spec   *runnerv1.VolumeSpec
}

func New(agents agentsv1.AgentsServiceClient, secrets secretsv1.SecretsServiceClient, cfg *config.Config) *Assembler {
	return NewWithEgressCA(agents, secrets, cfg, nil)
}

func NewWithEgressCA(agents agentsv1.AgentsServiceClient, secrets secretsv1.SecretsServiceClient, cfg *config.Config, egressCACert []byte) *Assembler {
	return &Assembler{agents: agents, secrets: secrets, cfg: cfg, egressCACert: append([]byte(nil), egressCACert...)}
}

func (a *Assembler) Assemble(ctx context.Context, agentID, threadID uuid.UUID) (*AssembleResult, error) {
	agent, err := a.fetchAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	runnerLabels := agentRunnerLabels(agent)

	resolver := newEnvResolver(a.secrets)
	volumeResolver := newVolumeResolver(a.agents, threadID)
	imagePullResolver := newImagePullResolver(a.secrets)

	agentEnvs, err := a.listEnvs(ctx, &agentsv1.ListEnvsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent envs: %w", err)
	}
	agentEnvVars, err := resolver.ResolveEnvVars(ctx, agentEnvs)
	if err != nil {
		return nil, fmt.Errorf("resolve agent envs: %w", err)
	}

	agentAttachments, err := a.listVolumeAttachments(ctx, &agentsv1.ListVolumeAttachmentsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent volume attachments: %w", err)
	}
	agentMounts, err := volumeResolver.mountsFor(ctx, agentAttachments)
	if err != nil {
		return nil, fmt.Errorf("resolve agent mounts: %w", err)
	}
	agentImagePullAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{AgentId: agentID.String()})
	if err != nil {
		return nil, fmt.Errorf("list agent image pull secret attachments: %w", err)
	}
	if err := imagePullResolver.Resolve(ctx, agentImagePullAttachments); err != nil {
		return nil, fmt.Errorf("resolve agent image pull secrets: %w", err)
	}

	mainEnv := mergeEnvVars(a.baseAgentEnvVars(agent, agentID, threadID), agentEnvVars, fmt.Sprintf("agent %s", agentID.String()))
	mainEnv = appendEgressCAEnvVars(mainEnv)

	initImage := agent.GetInitImage()
	if initImage == "" {
		return nil, fmt.Errorf("agent %s: init_image is required", agentID)
	}

	mainMounts := append([]*runnerv1.VolumeMount{}, agentMounts...)
	mainMounts = append(mainMounts, &runnerv1.VolumeMount{Volume: agynBinVolumeName, MountPath: agynBinMountPath})
	main := &runnerv1.ContainerSpec{
		Image:            agent.GetImage(),
		Name:             fmt.Sprintf("agent-%s-%s", agentID.String()[:8], threadID.String()[:8]),
		Cmd:              []string{agynBinBinaryPath},
		Env:              mainEnv,
		Mounts:           mainMounts,
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}
	initContainer := &runnerv1.ContainerSpec{
		Image: initImage,
		Name:  "agent-init",
		Mounts: []*runnerv1.VolumeMount{
			{Volume: agynBinVolumeName, MountPath: agynBinMountPath},
		},
	}
	applyEgressCA(initContainer, a.egressCACert)
	initContainers := []*runnerv1.ContainerSpec{initContainer}
	if a.cfg.ZitiEnabled {
		gatewayHostname, err := gatewayHost(a.cfg.AgentGatewayAddress)
		if err != nil {
			return nil, err
		}
		zitiEnroll := &runnerv1.ContainerSpec{
			Image:      a.cfg.ZitiSidecarImage,
			Name:       ZitiEnrollContainerName,
			Cmd:        buildZitiEnrollCommand(a.cfg.ZitiEnrollmentDNSUpstream, a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort, a.cfg.ZitiRuntimeControllerResolveHost, a.cfg.ZitiRuntimeControllerPort),
			Entrypoint: zitiEnrollEntrypoint,
			Env:        zitiEnrollEnvVars(a.cfg.ZitiEnrollmentControllerResolveHost, a.cfg.ZitiEnrollmentControllerPort),
			Mounts:     []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
		}
		zitiSidecar := &runnerv1.ContainerSpec{
			Image:                a.cfg.ZitiSidecarImage,
			Name:                 ZitiSidecarContainerName,
			Cmd:                  buildZitiSidecarCommand(a.cfg.WorkloadDNSUpstream, a.cfg.ZitiRuntimeControllerResolveHost, a.cfg.ZitiRuntimeControllerPort, a.cfg.ZitiEnrollmentControllerResolveHost),
			Entrypoint:           zitiSidecarEntrypoint,
			Env:                  zitiSidecarEnvVars(a.cfg.WorkloadDNSUpstream, a.cfg.ZitiEnrollmentDNSUpstream, a.cfg.ZitiRuntimeControllerResolveHost),
			Mounts:               []*runnerv1.VolumeMount{{Volume: zitiIdentityVolumeName, MountPath: zitiIdentityMountPath}},
			RequiredCapabilities: []string{zitiRequiredCapabilityNetAdmin},
			// k8s-runner maps restart_policy=Always on init containers to
			// Kubernetes restartable init containers. This lets the tunnel stay
			// up while Kubernetes continues to later init containers and main.
			AdditionalProperties: map[string]string{zitiRestartPolicyKey: zitiRestartPolicyAlways},
		}
		zitiGatewayWait := &runnerv1.ContainerSpec{
			Image: zitiGatewayWaitImage,
			Name:  zitiGatewayWaitContainerName,
			Cmd:   buildZitiGatewayWaitCommand(gatewayHostname),
		}
		applyEgressCA(zitiEnroll, a.egressCACert)
		applyEgressCA(zitiSidecar, a.egressCACert)
		applyEgressCA(zitiGatewayWait, a.egressCACert)
		initContainers = []*runnerv1.ContainerSpec{zitiEnroll, zitiSidecar, zitiGatewayWait, initContainer}
	}

	mcps, err := a.listMcps(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list mcps: %w", err)
	}
	log.Printf("assembler: agent %s: found %d MCP servers", agentID, len(mcps))
	mcpAssignments, err := assignMcpPorts(mcps)
	if err != nil {
		return nil, fmt.Errorf("assign mcp ports: %w", err)
	}
	hooks, err := a.listHooks(ctx, agentID)
	if err != nil {
		return nil, fmt.Errorf("list hooks: %w", err)
	}
	hookAssignments, err := assignHooks(hooks)
	if err != nil {
		return nil, fmt.Errorf("assign hooks: %w", err)
	}
	allocatedCPU, allocatedRAM, err := sumAllocatedResources(agent, mcpAssignments, hookAssignments)
	if err != nil {
		return nil, err
	}
	for _, assignment := range mcpAssignments {
		mcpAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{McpId: assignment.id})
		if err != nil {
			return nil, fmt.Errorf("list mcp image pull secret attachments: %w", err)
		}
		if err := imagePullResolver.Resolve(ctx, mcpAttachments); err != nil {
			return nil, fmt.Errorf("resolve mcp image pull secrets: %w", err)
		}
	}
	for _, assignment := range hookAssignments {
		hookAttachments, err := a.listImagePullSecretAttachments(ctx, &agentsv1.ListImagePullSecretAttachmentsRequest{HookId: assignment.id.String()})
		if err != nil {
			return nil, fmt.Errorf("list hook image pull secret attachments: %w", err)
		}
		if err := imagePullResolver.Resolve(ctx, hookAttachments); err != nil {
			return nil, fmt.Errorf("resolve hook image pull secrets: %w", err)
		}
	}

	sidecarCapacity := len(mcpAssignments) + len(hookAssignments)
	sidecars := make([]*runnerv1.ContainerSpec, 0, sidecarCapacity)
	mcpServers := make([]string, 0, len(mcpAssignments))
	for _, assignment := range mcpAssignments {
		sidecar, err := a.buildMcpSidecar(ctx, resolver, volumeResolver, assignment.mcp, assignment.port)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
		mcpServers = append(mcpServers, fmt.Sprintf("%s:%d", assignment.name, assignment.port))
	}
	for _, assignment := range hookAssignments {
		sidecar, err := a.buildHookSidecar(ctx, resolver, volumeResolver, assignment)
		if err != nil {
			return nil, err
		}
		sidecars = append(sidecars, sidecar)
	}
	if len(mcpServers) > 0 {
		main.Env = appendPlatformEnvVar(main.Env, &runnerv1.EnvVar{Name: "AGENT_MCP_SERVERS", Value: strings.Join(mcpServers, ",")})
	}
	imagePullCredentials, err := imagePullResolver.Credentials()
	if err != nil {
		return nil, fmt.Errorf("image pull credentials: %w", err)
	}

	agynBinVolume := &runnerv1.VolumeSpec{
		Name: agynBinVolumeName,
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	volumes := append(volumeResolver.Specs(), agynBinVolume)
	if a.cfg.ZitiEnabled {
		volumes = append(volumes, &runnerv1.VolumeSpec{
			Name: zitiIdentityVolumeName,
			Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
		})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].Name < volumes[j].Name })

	request := &runnerv1.StartWorkloadRequest{
		Main:                 main,
		Sidecars:             sidecars,
		Volumes:              volumes,
		InitContainers:       initContainers,
		ImagePullCredentials: imagePullCredentials,
		Capabilities:         append([]string(nil), agent.GetCapabilities()...),
		InlineFiles:          a.inlineFiles(),
		AdditionalProperties: map[string]string{
			LabelKeyPrefix + LabelManagedBy: ManagedByValue,
			LabelKeyPrefix + LabelAgentID:   agentID.String(),
			LabelKeyPrefix + LabelThreadID:  threadID.String(),
		},
	}
	if a.cfg.ZitiEnabled {
		request.DnsConfig = &runnerv1.DnsConfig{
			Nameservers: []string{zitiDNSNameserver, a.cfg.WorkloadDNSUpstream},
			Searches:    []string{zitiDNSSearchService, zitiDNSSearchCluster},
		}
	}
	persistentVolumes, err := volumeResolver.PersistentVolumes()
	if err != nil {
		return nil, err
	}
	return &AssembleResult{
		Request:                request,
		OrganizationID:         agent.GetOrganizationId(),
		RunnerLabels:           runnerLabels,
		PersistentVolumes:      persistentVolumes,
		AllocatedCPUMillicores: allocatedCPU,
		AllocatedRAMBytes:      allocatedRAM,
	}, nil
}

func zitiEnvVars() []*runnerv1.EnvVar {
	return []*runnerv1.EnvVar{
		{Name: ZitiIdentityBasenameEnvVar, Value: ZitiIdentityBasename},
		{Name: ZitiIdentityDirEnvVar, Value: zitiIdentityMountPath},
	}
}

func zitiEnrollEnvVars(enrollmentControllerResolveHost string, enrollmentControllerPort string) []*runnerv1.EnvVar {
	envVars := zitiEnvVars()
	envVars = append(envVars,
		&runnerv1.EnvVar{Name: ZitiEnrollmentControllerResolveHostEnvVar, Value: enrollmentControllerResolveHost},
		&runnerv1.EnvVar{Name: ZitiEnrollmentControllerPortEnvVar, Value: enrollmentControllerPort},
	)
	return envVars
}

func zitiSidecarEnvVars(workloadDNSUpstream string, zitiEnrollmentDNSUpstream string, runtimeControllerResolveHost string) []*runnerv1.EnvVar {
	envVars := zitiEnvVars()
	envVars = append(envVars,
		&runnerv1.EnvVar{Name: "WORKLOAD_DNS_UPSTREAM", Value: workloadDNSUpstream},
		&runnerv1.EnvVar{Name: "ZITI_DNS_UPSTREAM", Value: zitiEnrollmentDNSUpstream},
		&runnerv1.EnvVar{Name: "ZITI_CTRL_ADVERTISED_ADDRESS", Value: runtimeControllerResolveHost},
		&runnerv1.EnvVar{Name: "ZITI_SIDECAR_BINARY", Value: zitiSidecarBinaryPath},
		&runnerv1.EnvVar{Name: "ZITI_SIDECAR_COMMAND", Value: zitiSidecarCommand},
		&runnerv1.EnvVar{Name: "ZITI_SIDECAR_MODE", Value: zitiSidecarMode},
		&runnerv1.EnvVar{Name: "ZITI_SIDECAR_SERVICE_POLL_RATE", Value: zitiSidecarServicePollRate},
	)
	return envVars
}

func buildZitiEnrollCommand(workloadDNSUpstream string, enrollmentControllerResolveHost string, enrollmentControllerPort string, runtimeControllerResolveHost string, runtimeControllerPort string) []string {
	return []string{
		"-ec",
		zitiEnrollScript,
		ZitiEnrollContainerName,
		workloadDNSUpstream,
		zitiDNSNameserver,
		enrollmentControllerResolveHost,
		enrollmentControllerPort,
		runtimeControllerResolveHost,
		runtimeControllerPort,
	}
}

func buildZitiSidecarCommand(workloadDNSUpstream string, runtimeControllerResolveHost string, runtimeControllerPort string, enrollmentControllerResolveHost string) []string {
	return []string{
		"-ec",
		zitiSidecarScript,
		ZitiSidecarContainerName,
		workloadDNSUpstream,
		runtimeControllerResolveHost,
		runtimeControllerPort,
		enrollmentControllerResolveHost,
	}
}

func agentRunnerLabels(agent *agentsv1.Agent) map[string]string {
	if agent == nil {
		return nil
	}
	// TODO: Add runner_labels to Agent proto and return agent.GetRunnerLabels().
	return nil
}

func (a *Assembler) fetchAgent(ctx context.Context, agentID uuid.UUID) (*agentsv1.Agent, error) {
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := a.agents.GetAgent(rctx, &agentsv1.GetAgentRequest{Id: agentID.String()})
	cancel()
	if err != nil {
		return nil, err
	}
	agent := resp.GetAgent()
	if agent == nil {
		return nil, fmt.Errorf("agent response missing")
	}
	meta := agent.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("agent meta missing")
	}
	metaID, err := uuidutil.ParseUUID(meta.GetId(), "agent.meta.id")
	if err != nil {
		return nil, err
	}
	if metaID != agentID {
		return nil, fmt.Errorf("agent id mismatch: %s", metaID.String())
	}
	if agent.GetOrganizationId() == "" {
		return nil, fmt.Errorf("agent organization id missing")
	}
	return agent, nil
}

func (a *Assembler) listMcps(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Mcp, error) {
	resp := []*agentsv1.Mcp{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListMcps(rctx, &agentsv1.ListMcpsRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetMcps()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listHooks(ctx context.Context, agentID uuid.UUID) ([]*agentsv1.Hook, error) {
	resp := []*agentsv1.Hook{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListHooks(rctx, &agentsv1.ListHooksRequest{
			AgentId:   agentID.String(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetHooks()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listEnvs(ctx context.Context, req *agentsv1.ListEnvsRequest) ([]*agentsv1.Env, error) {
	resp := []*agentsv1.Env{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListEnvs(rctx, &agentsv1.ListEnvsRequest{
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetEnvs()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listVolumeAttachments(ctx context.Context, req *agentsv1.ListVolumeAttachmentsRequest) ([]*agentsv1.VolumeAttachment, error) {
	resp := []*agentsv1.VolumeAttachment{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListVolumeAttachments(rctx, &agentsv1.ListVolumeAttachmentsRequest{
			VolumeId:  req.GetVolumeId(),
			AgentId:   req.GetAgentId(),
			McpId:     req.GetMcpId(),
			HookId:    req.GetHookId(),
			PageSize:  listPageSize,
			PageToken: token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetVolumeAttachments()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (a *Assembler) listImagePullSecretAttachments(ctx context.Context, req *agentsv1.ListImagePullSecretAttachmentsRequest) ([]*agentsv1.ImagePullSecretAttachment, error) {
	resp := []*agentsv1.ImagePullSecretAttachment{}
	token := ""
	for {
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		page, err := a.agents.ListImagePullSecretAttachments(rctx, &agentsv1.ListImagePullSecretAttachmentsRequest{
			ImagePullSecretId: req.GetImagePullSecretId(),
			AgentId:           req.GetAgentId(),
			McpId:             req.GetMcpId(),
			HookId:            req.GetHookId(),
			PageSize:          listPageSize,
			PageToken:         token,
		})
		cancel()
		if err != nil {
			return nil, err
		}
		resp = append(resp, page.GetImagePullSecretAttachments()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

type mcpAssignment struct {
	mcp  *agentsv1.Mcp
	id   string
	name string
	port int
}

type hookAssignment struct {
	hook *agentsv1.Hook
	id   uuid.UUID
}

func assignMcpPorts(mcps []*agentsv1.Mcp) ([]mcpAssignment, error) {
	assignments := make([]mcpAssignment, 0, len(mcps))
	for _, mcp := range mcps {
		if mcp == nil {
			return nil, fmt.Errorf("mcp is nil")
		}
		meta := mcp.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("mcp meta missing")
		}
		id := meta.GetId()
		if id == "" {
			return nil, fmt.Errorf("mcp meta id missing")
		}
		name := mcp.GetName()
		if name == "" {
			return nil, fmt.Errorf("mcp name missing")
		}
		assignments = append(assignments, mcpAssignment{mcp: mcp, id: id, name: name})
	}
	sort.Slice(assignments, func(i, j int) bool {
		return assignments[i].id < assignments[j].id
	})
	for i := range assignments {
		assignments[i].port = mcpBasePort + i
	}
	return assignments, nil
}

func assignHooks(hooks []*agentsv1.Hook) ([]hookAssignment, error) {
	assignments := make([]hookAssignment, 0, len(hooks))
	for _, hook := range hooks {
		if hook == nil {
			return nil, fmt.Errorf("hook is nil")
		}
		meta := hook.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("hook meta missing")
		}
		hookID, err := uuidutil.ParseUUID(meta.GetId(), "hook.meta.id")
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, hookAssignment{hook: hook, id: hookID})
	}
	return assignments, nil
}

func (a *Assembler) buildMcpSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, mcp *agentsv1.Mcp, port int) (*runnerv1.ContainerSpec, error) {
	if mcp == nil {
		return nil, fmt.Errorf("mcp is nil")
	}
	meta := mcp.GetMeta()
	if meta == nil {
		return nil, fmt.Errorf("mcp meta missing")
	}
	mcpID, err := uuidutil.ParseUUID(meta.GetId(), "mcp.meta.id")
	if err != nil {
		return nil, err
	}
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{McpId: mcpID.String()},
		&agentsv1.ListVolumeAttachmentsRequest{McpId: mcpID.String()},
	)
	if err != nil {
		return nil, err
	}
	gatewayURL := buildGatewayURL(a.cfg.AgentGatewayAddress)
	envVars = mergeEnvVars([]*runnerv1.EnvVar{
		{Name: "MCP_PORT", Value: strconv.Itoa(port)},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "AGYN_GATEWAY_URL", Value: gatewayURL},
	}, envVars, fmt.Sprintf("mcp %s", mcpID.String()))
	envVars = appendEgressCAEnvVars(envVars)
	return &runnerv1.ContainerSpec{
		Image:            mcp.GetImage(),
		Name:             fmt.Sprintf("mcp-%s", mcpID.String()[:8]),
		Cmd:              []string{"/bin/sh", "-c", mcp.GetCommand()},
		Env:              envVars,
		Mounts:           mounts,
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}, nil
}

func (a *Assembler) buildHookSidecar(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, assignment hookAssignment) (*runnerv1.ContainerSpec, error) {
	envVars, mounts, err := a.resolveSidecarResources(
		ctx,
		resolver,
		volumeResolver,
		&agentsv1.ListEnvsRequest{HookId: assignment.id.String()},
		&agentsv1.ListVolumeAttachmentsRequest{HookId: assignment.id.String()},
	)
	if err != nil {
		return nil, err
	}
	envVars = mergeEnvVars(nil, envVars, fmt.Sprintf("hook %s", assignment.id.String()))
	envVars = appendEgressCAEnvVars(envVars)
	return &runnerv1.ContainerSpec{
		Image:            assignment.hook.GetImage(),
		Name:             fmt.Sprintf("hook-%s", assignment.id.String()[:8]),
		Cmd:              []string{"/bin/sh", "-c", assignment.hook.GetFunction()},
		Env:              envVars,
		Mounts:           mounts,
		InlineFileMounts: egressCAInlineFileMounts(a.egressCACert),
	}, nil
}

func buildGatewayURL(address string) string {
	if strings.Contains(address, "://") {
		return address
	}
	return "http://" + address
}

func gatewayHost(address string) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("parse gateway host from %q: %w", address, err)
	}
	if host == "" {
		return "", fmt.Errorf("gateway host missing from %q", address)
	}
	return host, nil
}

func buildZitiGatewayWaitCommand(host string) []string {
	script := fmt.Sprintf(
		"i=0; while [ $i -lt %d ]; do nslookup %s %s >/dev/null 2>&1 && exit 0; i=$((i+1)); sleep 1; done; echo \"timeout waiting for %s\" >&2; exit 1",
		zitiGatewayWaitTimeoutSeconds,
		host,
		zitiDNSNameserver,
		host,
	)
	return []string{"/bin/sh", "-c", script}
}

func mergeEnvVars(platformEnv, userEnv []*runnerv1.EnvVar, owner string) []*runnerv1.EnvVar {
	merged := make([]*runnerv1.EnvVar, 0, len(platformEnv)+len(userEnv))
	seen := make(map[string]struct{}, len(platformEnv)+len(userEnv))
	for _, env := range platformEnv {
		name := env.Name
		if _, ok := seen[name]; ok {
			continue
		}
		merged = append(merged, env)
		seen[name] = struct{}{}
	}
	for _, env := range userEnv {
		name := env.Name
		if _, ok := reservedEnvNames[name]; ok {
			log.Printf("assembler: warn: dropping reserved env %s for %s", name, owner)
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		merged = append(merged, env)
		seen[name] = struct{}{}
	}
	return merged
}

func appendPlatformEnvVar(envs []*runnerv1.EnvVar, env *runnerv1.EnvVar) []*runnerv1.EnvVar {
	result := make([]*runnerv1.EnvVar, 0, len(envs)+1)
	for _, existing := range envs {
		if existing.Name == env.Name {
			continue
		}
		result = append(result, existing)
	}
	result = append(result, env)
	return result
}

func (a *Assembler) baseAgentEnvVars(agent *agentsv1.Agent, agentID, threadID uuid.UUID) []*runnerv1.EnvVar {
	gatewayURL := buildGatewayURL(a.cfg.AgentGatewayAddress)
	vars := []*runnerv1.EnvVar{
		{Name: "AGENT_ID", Value: agentID.String()},
		{Name: "AGENT_NAME", Value: agent.GetName()},
		{Name: "AGENT_ROLE", Value: agent.GetRole()},
		{Name: "AGENT_MODEL", Value: agent.GetModel()},
		{Name: "AGENT_CONFIG", Value: agent.GetConfiguration()},
		{Name: "THREAD_ID", Value: threadID.String()},
		{Name: "GATEWAY_ADDRESS", Value: a.cfg.AgentGatewayAddress},
		{Name: "AGYN_GATEWAY_URL", Value: gatewayURL},
		{Name: "LLM_BASE_URL", Value: a.cfg.AgentLLMBaseURL},
	}
	if a.cfg.AgentTracingAddress != "" {
		vars = append(vars, &runnerv1.EnvVar{Name: "TRACING_ADDRESS", Value: a.cfg.AgentTracingAddress})
		vars = append(vars, &runnerv1.EnvVar{Name: "OTEL_EXPORTER_OTLP_ENDPOINT", Value: "http://localhost:4317"})
	}
	return vars
}

type volumeResolver struct {
	agents   agentsv1.AgentsServiceClient
	threadID uuid.UUID
	cache    map[string]*agentsv1.Volume
	specs    map[string]*runnerv1.VolumeSpec
}

func newVolumeResolver(agents agentsv1.AgentsServiceClient, threadID uuid.UUID) *volumeResolver {
	return &volumeResolver{
		agents:   agents,
		threadID: threadID,
		cache:    map[string]*agentsv1.Volume{},
		specs:    map[string]*runnerv1.VolumeSpec{},
	}
}

func (v *volumeResolver) mountsFor(ctx context.Context, attachments []*agentsv1.VolumeAttachment) ([]*runnerv1.VolumeMount, error) {
	mounts := make([]*runnerv1.VolumeMount, 0, len(attachments))
	for _, attachment := range attachments {
		if attachment == nil {
			return nil, fmt.Errorf("volume attachment is nil")
		}
		volumeIDRaw := attachment.GetVolumeId()
		volumeID, err := uuidutil.ParseUUID(volumeIDRaw, "volume_attachment.volume_id")
		if err != nil {
			return nil, err
		}
		volume, err := v.getVolume(ctx, volumeID)
		if err != nil {
			return nil, err
		}
		mountPath := volume.GetMountPath()
		if mountPath == "" {
			return nil, fmt.Errorf("volume %s mount_path is empty", volumeID.String())
		}
		spec := v.ensureSpec(volumeID, volume)
		mounts = append(mounts, &runnerv1.VolumeMount{Volume: spec.Name, MountPath: mountPath})
	}
	return mounts, nil
}

func (v *volumeResolver) Specs() []*runnerv1.VolumeSpec {
	if len(v.specs) == 0 {
		return nil
	}
	specs := make([]*runnerv1.VolumeSpec, 0, len(v.specs))
	for _, spec := range v.specs {
		specs = append(specs, spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

func (v *volumeResolver) PersistentVolumes() ([]PersistentVolumeInfo, error) {
	if len(v.specs) == 0 {
		return nil, nil
	}
	volumes := make([]PersistentVolumeInfo, 0, len(v.specs))
	for volumeID, spec := range v.specs {
		if spec == nil {
			return nil, fmt.Errorf("volume spec missing for %s", volumeID)
		}
		volume := v.cache[volumeID]
		if volume == nil {
			return nil, fmt.Errorf("volume %s missing", volumeID)
		}
		if !volume.GetPersistent() {
			continue
		}
		parsedID, err := uuidutil.ParseUUID(volumeID, "volume.id")
		if err != nil {
			return nil, err
		}
		volumes = append(volumes, PersistentVolumeInfo{ID: parsedID, Volume: volume, Spec: spec})
	}
	sort.Slice(volumes, func(i, j int) bool { return volumes[i].ID.String() < volumes[j].ID.String() })
	return volumes, nil
}

func (v *volumeResolver) getVolume(ctx context.Context, volumeID uuid.UUID) (*agentsv1.Volume, error) {
	key := volumeID.String()
	if cached, ok := v.cache[key]; ok {
		return cached, nil
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	resp, err := v.agents.GetVolume(rctx, &agentsv1.GetVolumeRequest{Id: key})
	cancel()
	if err != nil {
		return nil, fmt.Errorf("get volume %s: %w", key, err)
	}
	volume := resp.GetVolume()
	if volume == nil {
		return nil, fmt.Errorf("volume %s missing", key)
	}
	v.cache[key] = volume
	return volume, nil
}

func (a *Assembler) resolveSidecarResources(ctx context.Context, resolver *envResolver, volumeResolver *volumeResolver, envReq *agentsv1.ListEnvsRequest, attachmentReq *agentsv1.ListVolumeAttachmentsRequest) ([]*runnerv1.EnvVar, []*runnerv1.VolumeMount, error) {
	vars, err := a.listEnvs(ctx, envReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar envs: %w", err)
	}
	envVars, err := resolver.ResolveEnvVars(ctx, vars)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar envs: %w", err)
	}
	attachments, err := a.listVolumeAttachments(ctx, attachmentReq)
	if err != nil {
		return nil, nil, fmt.Errorf("list sidecar volume attachments: %w", err)
	}
	mounts, err := volumeResolver.mountsFor(ctx, attachments)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve sidecar mounts: %w", err)
	}
	return envVars, mounts, nil
}

func (v *volumeResolver) ensureSpec(volumeID uuid.UUID, volume *agentsv1.Volume) *runnerv1.VolumeSpec {
	key := volumeID.String()
	if spec, ok := v.specs[key]; ok {
		return spec
	}
	shortVolume := key[:8]
	spec := &runnerv1.VolumeSpec{
		Name: fmt.Sprintf("vol-%s", shortVolume),
		Kind: runnerv1.VolumeKind_VOLUME_KIND_EPHEMERAL,
	}
	if volume.GetPersistent() {
		spec.Kind = runnerv1.VolumeKind_VOLUME_KIND_NAMED
		threadPrefix := v.threadID.String()[:12]
		volumePrefix := key[:12]
		spec.PersistentName = fmt.Sprintf("pv-%s-%s", threadPrefix, volumePrefix)
	}
	v.specs[key] = spec
	return spec
}
