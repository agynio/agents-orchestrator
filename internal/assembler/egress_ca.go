package assembler

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	corev1 "k8s.io/api/core/v1"
)

const (
	egressCASecretName = "egress-ca"
	egressCASecretKey  = "tls.crt"
)

// systemRootsPath is this image's own trust store. The bundle handed to a
// workload is built from it, so pointing SSL_CERT_FILE at that bundle adds the
// egress CA to the public roots instead of replacing them. A variable so the
// tests can point it somewhere predictable.
var systemRootsPath = "/etc/ssl/certs/ca-certificates.crt"

// EgressCABundle returns the certificate bundle to install in every workload:
// the public roots this image trusts, with the egress CA appended.
//
// The egress CA alone is not a trust store. Installed as one it broke every
// connection egress does not intercept -- an agent could not reach the ziti
// controller or its own MCP sidecars, because a store holding one private CA
// vouches for nothing else. Traffic the egress gateway does terminate is still
// trusted, because its CA is in here too.
func EgressCABundle(cert []byte) []byte {
	if len(cert) == 0 {
		return nil
	}
	roots, err := os.ReadFile(systemRootsPath)
	if err != nil {
		// Better a store that trusts only the egress CA than no egress at all,
		// but say so: everything else this workload dials will fail to verify.
		log.Printf("assembler: read system roots %s: %v; workloads get the egress CA alone", systemRootsPath, err)
		return append([]byte(nil), cert...)
	}
	bundle := make([]byte, 0, len(roots)+len(cert)+1)
	bundle = append(bundle, bytes.TrimRight(roots, "\n")...)
	bundle = append(bundle, '\n')
	return append(bundle, cert...)
}

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
