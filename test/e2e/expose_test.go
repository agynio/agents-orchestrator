//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type exposureOutput struct {
	ID     string `json:"id"`
	Port   int32  `json:"port"`
	URL    string `json:"url"`
	Status string `json:"status"`
}

func TestAgentExposeListExec(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)
	usersConn := dialGRPC(t, usersAddr)
	orgsConn := dialGRPC(t, orgsAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	usersClient := usersv1.NewUsersServiceClient(usersConn)
	orgsClient := organizationsv1.NewOrganizationsServiceClient(orgsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
	exposeInitImage := envOrDefault("AGN_EXPOSE_INIT_IMAGE", "ghcr.io/agynio/agent-init-agn:0.4.4")

	identityID := resolveOrCreateUser(t, ctx, usersClient)
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpointAgn, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-expose-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	agent := createAgent(t, ctx, agentsClient, fmt.Sprintf("e2e-expose-%s", uuid.NewString()), modelID, orgID, exposeInitImage)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	thread := createThread(t, ctx, threadsClient, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	labels := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
	}
	t.Cleanup(func() {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labels)
		if err != nil {
			t.Logf("cleanup: find workloads: %v", err)
			return
		}
		for _, workloadID := range ids {
			cleanupWorkload(t, ctx, runnerClient, workloadID)
		}
	})

	expectedResponse := "Hi! How are you?"
	sentMessage := sendMessage(t, ctx, threadsClient, threadID, identityID, "hi")
	sentMessageTime := messageCreatedAt(t, sentMessage)

	pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer pollCancel()
	agentBody, err := pollForAgentResponse(t, pollCtx, threadsClient, runnerClient, threadID, agentID, labels, sentMessageTime, expectedResponse)
	if err != nil {
		t.Fatalf("wait for agent response: %v", err)
	}
	if agentBody != expectedResponse {
		t.Fatalf("expected agent response %q, got %q", expectedResponse, agentBody)
	}

	workloadIDs, err := findWorkloadsByLabels(ctx, runnerClient, labels)
	if err != nil {
		t.Fatalf("find workloads: %v", err)
	}
	if len(workloadIDs) == 0 {
		t.Fatal("expected workload id")
	}

	execCtx, execCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer execCancel()
	podName, containerName, err := waitForWorkloadAgentContainerReady(t, execCtx, workloadIDs[0])
	if err != nil {
		t.Fatalf("wait for agent container: %v", err)
	}

	port := int32(15000 + time.Now().UnixNano()%10000)
	addResult := execPodCommand(t, execCtx, workloadNamespace(t), podName, containerName, []string{
		"/agyn-bin/cli/agyn",
		"expose",
		"add",
		fmt.Sprintf("%d", port),
		"--output",
		"json",
	})
	if addResult.exitCode != 0 {
		t.Fatalf("expected expose add exit code 0, got %d stdout=%q stderr=%q", addResult.exitCode, addResult.stdout, addResult.stderr)
	}
	var addOutput exposureOutput
	if err := json.Unmarshal([]byte(addResult.stdout), &addOutput); err != nil {
		t.Fatalf("decode expose add output: %v stdout=%q", err, addResult.stdout)
	}
	if addOutput.ID == "" {
		t.Fatal("expose add output missing id")
	}
	if addOutput.URL == "" {
		t.Fatal("expose add output missing url")
	}
	if addOutput.Port != port {
		t.Fatalf("expected expose add port %d, got %d", port, addOutput.Port)
	}
	if addOutput.Status != "active" {
		t.Fatalf("expected expose add status active, got %q", addOutput.Status)
	}

	listResult := execPodCommand(t, execCtx, workloadNamespace(t), podName, containerName, []string{
		"/agyn-bin/cli/agyn",
		"expose",
		"list",
		"--output",
		"json",
	})
	if listResult.exitCode != 0 {
		t.Fatalf("expected expose list exit code 0, got %d stdout=%q stderr=%q", listResult.exitCode, listResult.stdout, listResult.stderr)
	}
	var listOutputs []exposureOutput
	if err := json.Unmarshal([]byte(listResult.stdout), &listOutputs); err != nil {
		t.Fatalf("decode expose list output: %v stdout=%q", err, listResult.stdout)
	}
	foundPort := false
	for _, exposure := range listOutputs {
		if exposure.Port == port {
			foundPort = true
			break
		}
	}
	if !foundPort {
		t.Fatalf("expected expose list to include port %d", port)
	}

	duplicateResult := execPodCommand(t, execCtx, workloadNamespace(t), podName, containerName, []string{
		"/agyn-bin/cli/agyn",
		"expose",
		"add",
		fmt.Sprintf("%d", port),
		"--output",
		"json",
	})
	if duplicateResult.exitCode == 0 {
		t.Fatalf("expected expose add duplicate exit code non-zero, got 0 stdout=%q stderr=%q", duplicateResult.stdout, duplicateResult.stderr)
	}
	if !strings.Contains(duplicateResult.stderr, "already_exists") {
		t.Fatalf("expected expose add duplicate stderr to include already_exists, got %q", duplicateResult.stderr)
	}

	removeResult := execPodCommand(t, execCtx, workloadNamespace(t), podName, containerName, []string{
		"/agyn-bin/cli/agyn",
		"expose",
		"remove",
		fmt.Sprintf("%d", port),
	})
	if removeResult.exitCode != 0 {
		t.Fatalf("expected expose remove exit code 0, got %d stdout=%q stderr=%q", removeResult.exitCode, removeResult.stdout, removeResult.stderr)
	}

	listAfterResult := execPodCommand(t, execCtx, workloadNamespace(t), podName, containerName, []string{
		"/agyn-bin/cli/agyn",
		"expose",
		"list",
		"--output",
		"json",
	})
	if listAfterResult.exitCode != 0 {
		t.Fatalf("expected expose list after remove exit code 0, got %d stdout=%q stderr=%q", listAfterResult.exitCode, listAfterResult.stdout, listAfterResult.stderr)
	}
	var listAfterOutputs []exposureOutput
	if err := json.Unmarshal([]byte(listAfterResult.stdout), &listAfterOutputs); err != nil {
		t.Fatalf("decode expose list after remove output: %v stdout=%q", err, listAfterResult.stdout)
	}
	for _, exposure := range listAfterOutputs {
		if exposure.Port == port {
			t.Fatalf("expected expose list after remove to exclude port %d", port)
		}
	}
}

func waitForWorkloadAgentContainerReady(t *testing.T, ctx context.Context, workloadID string) (string, string, error) {
	t.Helper()
	namespace := workloadNamespace(t)
	podName := fmt.Sprintf("workload-%s", workloadID)
	clientset := kubeClientset(t)
	containerName := ""
	err := pollUntil(ctx, pollInterval, func(ctx context.Context) error {
		pod, err := clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("get pod %s: %w", podName, err)
		}
		if pod.Status.Phase != corev1.PodRunning {
			return fmt.Errorf("pod %s not running", pod.Name)
		}
		name, err := agentContainerName(pod)
		if err != nil {
			return err
		}
		if !containerReady(pod, name) {
			return fmt.Errorf("container %s not ready", name)
		}
		containerName = name
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return podName, containerName, nil
}
