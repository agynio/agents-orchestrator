package assembler

import runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"

func appendEgressCAEnvVars(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	for _, env := range egressCAEnvVars() {
		envs = appendPlatformEnvVar(envs, env)
	}
	envs = appendWorkloadPathEnvVar(envs)
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

// appendWorkloadPathEnvVar puts the platform's binaries on PATH for everything
// that runs in the container, including a shell nobody here started.
//
// agynd prepends this for the subprocess it spawns, which covers an agent but
// not a sandbox: holder mode spawns nothing, and an interactive session comes
// from the runner's Exec against the pod, inheriting the container spec's
// environment rather than any process's. Without it a person at the shell finds
// agyn, agynd and the agent CLI present on disk and none of them on PATH.
func appendWorkloadPathEnvVar(envs []*runnerv1.EnvVar) []*runnerv1.EnvVar {
	return appendPlatformEnvVar(envs, &runnerv1.EnvVar{
		Name: "PATH",
		// The image's own PATH is not knowable here, so the default login set
		// is spelled out after the platform's own directory.
		Value: agynBinDir + ":/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	})
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
