package assembler

import (
	"os"

	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
)

// systemCACertPath is the host's public root bundle. It is merged with the
// Egress CA into the workload trust file: SSL_CERT_FILE / CURL_CA_BUNDLE /
// REQUESTS_CA_BUNDLE *replace* the system store, so pointing them at the Egress
// CA alone leaves workloads unable to verify the real certs of destinations the
// egress passes through (only intercepted, Egress-CA-signed leaves would
// verify). Overridable in tests.
var systemCACertPath = "/etc/ssl/certs/ca-certificates.crt"

// egressCABundle returns the bytes written to egressCACertPath: the host public
// roots followed by the Egress CA. Falls back to the Egress CA alone if the host
// bundle is unavailable.
func egressCABundle(cert []byte) []byte {
	roots, err := os.ReadFile(systemCACertPath)
	if err != nil || len(roots) == 0 {
		return append([]byte(nil), cert...)
	}
	out := make([]byte, 0, len(roots)+len(cert)+1)
	out = append(out, roots...)
	if roots[len(roots)-1] != '\n' {
		out = append(out, '\n')
	}
	return append(out, cert...)
}

func appendEgressCAEnvVars(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	for _, env := range egressCAEnvVars() {
		envs = appendPlatformEnvVar(envs, env)
	}
	return envs
}

func egressCAEnvVars() []*runnerv1.EnvVar {
	return []*runnerv1.EnvVar{
		{Name: "SSL_CERT_FILE", Value: egressCACertPath},
		{Name: "REQUESTS_CA_BUNDLE", Value: egressCACertPath},
		{Name: "NODE_EXTRA_CA_CERTS", Value: egressCACertPath},
		{Name: "CURL_CA_BUNDLE", Value: egressCACertPath},
		{Name: "SSL_CERT_DIR", Value: egressCACertDir},
	}
}

func egressCAInlineFiles(cert []byte) map[string][]byte {
	if len(cert) == 0 {
		return nil
	}
	return map[string][]byte{egressCACertPath: egressCABundle(cert)}
}

func egressCAInlineFileMounts(cert []byte) []*runnerv1.InlineFileMount {
	if len(cert) == 0 {
		return nil
	}
	return []*runnerv1.InlineFileMount{{Path: egressCACertPath}}
}

func applyEgressCA(container *runnerv1.ContainerSpec, cert []byte) {
	if container == nil {
		panic("container is nil")
	}
	container.Env = appendEgressCAEnvVars(container.GetEnv())
	container.InlineFileMounts = append(container.GetInlineFileMounts(), egressCAInlineFileMounts(cert)...)
}
