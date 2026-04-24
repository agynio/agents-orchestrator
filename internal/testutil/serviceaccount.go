package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const NamespacePathEnvVar = "AGENTS_ORCHESTRATOR_NAMESPACE_PATH"

func RunWithServiceAccountNamespace(m *testing.M, namespace string) int {
	tempDir, err := os.MkdirTemp("", "agents-orchestrator-namespace-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup service account namespace dir: %v\n", err)
		return 1
	}
	path := filepath.Join(tempDir, "namespace")
	if err := os.WriteFile(path, []byte(namespace), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "setup service account namespace: %v\n", err)
		return 1
	}
	originalPath, hadOriginal := os.LookupEnv(NamespacePathEnvVar)
	if err := os.Setenv(NamespacePathEnvVar, path); err != nil {
		fmt.Fprintf(os.Stderr, "set namespace path env: %v\n", err)
		return 1
	}
	exitCode := m.Run()
	if hadOriginal {
		if err := os.Setenv(NamespacePathEnvVar, originalPath); err != nil {
			fmt.Fprintf(os.Stderr, "restore namespace path env: %v\n", err)
		}
	} else {
		if err := os.Unsetenv(NamespacePathEnvVar); err != nil {
			fmt.Fprintf(os.Stderr, "unset namespace path env: %v\n", err)
		}
	}
	if err := os.RemoveAll(tempDir); err != nil {
		fmt.Fprintf(os.Stderr, "cleanup service account namespace dir: %v\n", err)
	}
	return exitCode
}
