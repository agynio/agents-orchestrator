package config

import (
	"os"
	"strings"
	"testing"
)

func TestZitiSidecarImageBuildDisablesDynamicOIDC(t *testing.T) {
	dockerfile, err := os.ReadFile("../../build/ziti-tunnel-x509/Dockerfile")
	if err != nil {
		t.Fatalf("read ziti sidecar Dockerfile: %v", err)
	}
	content := string(dockerfile)
	for _, expected := range []string{
		"ARG ZITI_VERSION=2.0.0",
		"FROM openziti/ziti-tunnel:${ZITI_VERSION}",
		"rootPrivateContext.(*ziti.ContextImpl).CtrlClt.SetUseOidc(false)",
		"github.com/openziti/ziti/v2/common/version.Version=v${ZITI_VERSION}",
		"go build -trimpath",
	} {
		if !strings.Contains(content, expected) {
			t.Fatalf("expected ziti sidecar Dockerfile to contain %q, got %q", expected, content)
		}
	}
	for _, forbidden := range []string{"grep -abo", "dd of=", "OIDC_AUTH"} {
		if strings.Contains(content, forbidden) {
			t.Fatalf("expected ziti sidecar Dockerfile not to use binary patching marker %q, got %q", forbidden, content)
		}
	}
}
