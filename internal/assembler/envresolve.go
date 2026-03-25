package assembler

import (
	"context"
	"fmt"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
)

type envResolver struct {
	secrets secretsv1.SecretsServiceClient
	cache   map[string]string
}

func newEnvResolver(secrets secretsv1.SecretsServiceClient) *envResolver {
	return &envResolver{secrets: secrets, cache: map[string]string{}}
}

func (r *envResolver) ResolveEnvVars(ctx context.Context, envs []*agentsv1.Env) ([]*runnerv1.EnvVar, error) {
	vars := make([]*runnerv1.EnvVar, 0, len(envs))
	for _, env := range envs {
		if env == nil {
			return nil, fmt.Errorf("env is nil")
		}
		name := env.GetName()
		if name == "" {
			return nil, fmt.Errorf("env %s name is empty", envID(env))
		}
		value, err := r.resolveValue(ctx, env)
		if err != nil {
			return nil, err
		}
		vars = append(vars, &runnerv1.EnvVar{Name: name, Value: value})
	}
	return vars, nil
}

func (r *envResolver) resolveValue(ctx context.Context, env *agentsv1.Env) (string, error) {
	switch source := env.GetSource().(type) {
	case *agentsv1.Env_Value:
		return source.Value, nil
	case *agentsv1.Env_SecretId:
		if source.SecretId == "" {
			return "", fmt.Errorf("env %s secret_id is empty", envID(env))
		}
		if cached, ok := r.cache[source.SecretId]; ok {
			return cached, nil
		}
		rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := r.secrets.ResolveSecret(rctx, &secretsv1.ResolveSecretRequest{Id: source.SecretId})
		cancel()
		if err != nil {
			return "", fmt.Errorf("resolve secret %s: %w", source.SecretId, err)
		}
		value := resp.GetValue()
		r.cache[source.SecretId] = value
		return value, nil
	default:
		return "", fmt.Errorf("env %s has no value or secret_id", envID(env))
	}
}

func envID(env *agentsv1.Env) string {
	if env == nil {
		return ""
	}
	meta := env.GetMeta()
	if meta == nil {
		return ""
	}
	return meta.GetId()
}
