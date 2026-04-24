package reconciler

import (
	"os"
	"testing"

	"github.com/agynio/agents-orchestrator/internal/testutil"
)

const testServiceAccountNamespace = "platform"

func TestMain(m *testing.M) {
	os.Exit(testutil.RunWithServiceAccountNamespace(m, testServiceAccountNamespace))
}
