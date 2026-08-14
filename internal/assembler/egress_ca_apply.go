package assembler

import runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"

func appendEgressCAEnvVars(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	for _, env := range egressCAEnvVars() {
		envs = appendPlatformEnvVar(envs, env)
	}
	return appendWorkloadSandboxEnvVar(envs)
}

// appendWorkloadSandboxEnvVar marks the container as the isolation boundary the
// agent CLI is already inside.
//
// Claude Code refuses to run with bypassed permissions as root -- the container
// runs as root, and the refusal is fatal rather than a downgrade. agynd sets
// this for the subprocess it spawns, which covers an agent but not a sandbox,
// where the shell comes from the runner's Exec against the pod and inherits the
// container spec's environment instead. The claim is true either way: the
// container is the sandbox, and the permission prompt it suppresses guards a
// developer's own machine, not this one.
func appendWorkloadSandboxEnvVar(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	return appendPlatformEnvVar(envs, &runnerv1.EnvVar{Name: "IS_SANDBOX", Value: "1"})
}

// PATH is deliberately not set here. A container env entry replaces the image's
// ENV PATH outright -- Kubernetes offers no way to prepend to it -- so setting
// one discards whatever the image put there, and an image that installs into a
// profile directory loses it. Every route to /agyn/bin prepends inside the
// container instead, where $PATH is the image's: agynd for the subprocess it
// spawns, the tmux configuration for a persistent shell, the Terminal Proxy for
// a session. Machine-invoked commands name their binary by absolute path.

func egressCAEnvVars() []*runnerv1.EnvVar {
	return []*runnerv1.EnvVar{
		{Name: "SSL_CERT_FILE", Value: egressCACertPath},
		{Name: "REQUESTS_CA_BUNDLE", Value: egressCACertPath},
		{Name: "NODE_EXTRA_CA_CERTS", Value: egressCACertPath},
		{Name: "CURL_CA_BUNDLE", Value: egressCACertPath},
		{Name: "SSL_CERT_DIR", Value: egressCACertDir},
	}
}

// egressCAInlineFiles writes the bundle, not the bare certificate: the env vars
// above name it as the trust store, and a store containing only the egress CA
// vouches for nothing the egress gateway does not terminate.
func egressCAInlineFiles(cert []byte) map[string][]byte {
	bundle := EgressCABundle(cert)
	if len(bundle) == 0 {
		return nil
	}
	return map[string][]byte{egressCACertPath: bundle}
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

func (a *Assembler) inlineFiles() map[string][]byte {
	return egressCAInlineFiles(a.egressCACert)
}
