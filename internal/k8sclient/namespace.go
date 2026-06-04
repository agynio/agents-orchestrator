package k8sclient

import (
	"fmt"
	"os"
	"strings"
)

const NamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

func ResolveNamespace(configuredNamespace, purpose string) (string, error) {
	namespace := strings.TrimSpace(configuredNamespace)
	if namespace != "" {
		return namespace, nil
	}
	value, err := os.ReadFile(NamespacePath)
	if err != nil {
		return "", fmt.Errorf("read %s namespace: %w", purpose, err)
	}
	namespace = strings.TrimSpace(string(value))
	if namespace == "" {
		return "", fmt.Errorf("%s namespace is empty", purpose)
	}
	return namespace, nil
}
