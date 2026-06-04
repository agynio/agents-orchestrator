package assembler

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

const (
	egressCASecretName = "egress-ca"
	egressCASecretKey  = "tls.crt"
)

type secretGetter interface {
	Get(ctx context.Context, namespace string, name string) (*corev1.Secret, error)
}

func LoadEgressCACertificate(ctx context.Context, client secretGetter, namespace string) ([]byte, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client is required")
	}
	if namespace == "" {
		return nil, fmt.Errorf("egress CA namespace is required")
	}
	secret, err := client.Get(ctx, namespace, egressCASecretName)
	if err != nil {
		return nil, fmt.Errorf("get %s secret: %w", egressCASecretName, err)
	}
	if secret == nil {
		return nil, fmt.Errorf("%s secret missing", egressCASecretName)
	}
	cert := secret.Data[egressCASecretKey]
	if len(cert) == 0 {
		return nil, fmt.Errorf("%s secret missing %s", egressCASecretName, egressCASecretKey)
	}
	return append([]byte(nil), cert...), nil
}
