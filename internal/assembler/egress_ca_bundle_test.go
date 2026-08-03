package assembler

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The egress CA is not a trust store. Installed as one it broke every
// connection egress does not intercept: ziti enrolment could not verify the
// controller, and codex refused to start because its MCP sidecar could not
// either. The bundle has to keep the public roots and add the CA to them.
func TestEgressCABundleKeepsTheSystemRoots(t *testing.T) {
	roots := "-----BEGIN CERTIFICATE-----\nroot-one\n-----END CERTIFICATE-----\n"
	cert := []byte("-----BEGIN CERTIFICATE-----\negress\n-----END CERTIFICATE-----\n")
	withSystemRoots(t, roots)

	bundle := string(EgressCABundle(cert))
	if !strings.Contains(bundle, "root-one") {
		t.Fatalf("expected the system roots to survive:\n%s", bundle)
	}
	if !strings.Contains(bundle, "egress") {
		t.Fatalf("expected the egress CA to be present:\n%s", bundle)
	}
	if strings.Count(bundle, "BEGIN CERTIFICATE") != 2 {
		t.Fatalf("expected both certificates, got:\n%s", bundle)
	}
}

// Callers use an empty result to mean "mount nothing", so an absent CA must not
// produce a bundle of bare system roots.
func TestEgressCABundleIsEmptyWithoutACertificate(t *testing.T) {
	withSystemRoots(t, "-----BEGIN CERTIFICATE-----\nroot\n-----END CERTIFICATE-----\n")
	if bundle := EgressCABundle(nil); bundle != nil {
		t.Fatalf("expected no bundle, got %q", bundle)
	}
}

// Losing the roots is worse than losing egress, but it must not lose the CA.
func TestEgressCABundleFallsBackToTheCertificateAlone(t *testing.T) {
	withSystemRootsPath(t, filepath.Join(t.TempDir(), "absent.crt"))
	cert := []byte("-----BEGIN CERTIFICATE-----\negress\n-----END CERTIFICATE-----\n")

	if bundle := EgressCABundle(cert); !bytes.Equal(bundle, cert) {
		t.Fatalf("expected the certificate alone, got %q", bundle)
	}
}

// The inline file is what actually reaches the container, so it carries the
// bundle rather than the bare certificate.
func TestEgressCAInlineFilesCarryTheBundle(t *testing.T) {
	withSystemRoots(t, "-----BEGIN CERTIFICATE-----\nroot-one\n-----END CERTIFICATE-----\n")
	cert := []byte("-----BEGIN CERTIFICATE-----\negress\n-----END CERTIFICATE-----\n")

	files := egressCAInlineFiles(cert)
	content, ok := files[egressCACertPath]
	if !ok {
		t.Fatalf("expected an inline file at %s, got %v", egressCACertPath, files)
	}
	if !strings.Contains(string(content), "root-one") {
		t.Fatalf("expected the system roots in the inline file:\n%s", content)
	}
}

func withSystemRoots(t *testing.T, content string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca-certificates.crt")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write system roots: %v", err)
	}
	withSystemRootsPath(t, path)
}

func withSystemRootsPath(t *testing.T, path string) {
	t.Helper()
	original := systemRootsPath
	systemRootsPath = path
	t.Cleanup(func() { systemRootsPath = original })
}
