package assembler

import runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"

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
	return map[string][]byte{egressCACertPath: append([]byte(nil), cert...)}
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

func applyZitiEnrollmentCA(container *runnerv1.ContainerSpec, cert []byte) {
	if len(cert) == 0 {
		return
	}
	container.Env = appendPlatformEnvVar(container.GetEnv(), &runnerv1.EnvVar{Name: "ZITI_ENROLLMENT_CA_FILE", Value: zitiEnrollmentCAFilePath})
	container.InlineFileMounts = append(container.GetInlineFileMounts(), &runnerv1.InlineFileMount{Path: zitiEnrollmentCAFilePath})
}

func (a *Assembler) inlineFiles() map[string][]byte {
	files := egressCAInlineFiles(a.egressCACert)
	if !a.cfg.ZitiEnabled || len(a.egressCACert) == 0 {
		return files
	}
	if files == nil {
		files = map[string][]byte{}
	}
	files[zitiEnrollmentCAFilePath] = append([]byte(nil), a.egressCACert...)
	return files
}
