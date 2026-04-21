//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	llmv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/llm/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	secretsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/secrets/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	testImagePullRegistry = "registry.example.com"
	testImagePullUsername = "e2e-user"
	testImagePullPassword = "e2e-password"
)

func TestImagePullSecretAttachedToPod(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	agentsConn := dialGRPC(t, agentsAddr)
	threadsConn := dialGRPC(t, threadsAddr)
	runnerConn := dialRunnerGRPC(t, runnerAddr)
	usersConn := dialGRPC(t, usersAddr)
	orgsConn := dialGRPC(t, orgsAddr)
	secretsConn := dialGRPC(t, secretsAddr)

	agentsClient := agentsv1.NewAgentsServiceClient(agentsConn)
	threadsClient := threadsv1.NewThreadsServiceClient(threadsConn)
	llmConn := dialGRPC(t, llmAddr)
	llmClient := llmv1.NewLLMServiceClient(llmConn)
	usersClient := usersv1.NewUsersServiceClient(usersConn)
	orgsClient := organizationsv1.NewOrganizationsServiceClient(orgsConn)
	runnerClient := runnerv1.NewRunnerServiceClient(runnerConn)
	secretsClient := secretsv1.NewSecretsServiceClient(secretsConn)

	identityID := resolveOrCreateUser(t, ctx, usersClient)
	token := createAPIToken(t, ctx, usersClient, identityID)
	orgID := createTestOrganization(t, ctx, orgsClient, identityID)

	provider := createLLMProvider(t, ctx, llmClient, testLLMEndpointCodex, orgID)
	providerID := provider.GetMeta().GetId()
	if providerID == "" {
		t.Fatal("create llm provider: missing id")
	}
	model := createModel(t, ctx, llmClient, "e2e-model-"+uuid.NewString(), providerID, "simple-hello", orgID)
	modelID := model.GetMeta().GetId()
	if modelID == "" {
		t.Fatal("create model: missing id")
	}

	imagePullSecret := createImagePullSecret(
		t,
		ctx,
		secretsClient,
		fmt.Sprintf("e2e-image-pull-%s", uuid.NewString()),
		testImagePullRegistry,
		testImagePullUsername,
		testImagePullPassword,
		orgID,
	)
	imagePullSecretID := imagePullSecret.GetMeta().GetId()
	if imagePullSecretID == "" {
		t.Fatal("create image pull secret: missing id")
	}
	t.Cleanup(func() { deleteImagePullSecret(t, ctx, secretsClient, imagePullSecretID) })

	agent := createAgent(t, ctx, agentsClient, "e2e-image-pull-"+uuid.NewString(), modelID, orgID, codexInitImage)
	agentID := agent.GetMeta().GetId()
	if agentID == "" {
		t.Fatal("create agent: missing id")
	}
	t.Cleanup(func() { deleteAgent(t, ctx, agentsClient, agentID) })
	createAgentEnv(t, ctx, agentsClient, agentID, "LLM_API_TOKEN", token)

	attachment := createImagePullSecretAttachment(t, ctx, agentsClient, imagePullSecretID, agentID)
	attachmentID := attachment.GetMeta().GetId()
	if attachmentID == "" {
		t.Fatal("create image pull secret attachment: missing id")
	}
	t.Cleanup(func() { deleteImagePullSecretAttachment(t, ctx, agentsClient, attachmentID) })

	thread := createThread(t, ctx, threadsClient, orgID, []string{identityID, agentID})
	threadID := thread.GetId()
	if threadID == "" {
		t.Fatal("create thread: missing id")
	}
	t.Cleanup(func() { archiveThread(t, ctx, threadsClient, threadID) })

	sendMessage(t, ctx, threadsClient, threadID, identityID, "e2e image pull secret")

	labelsMap := map[string]string{
		labelManagedBy: managedByValue,
		labelAgentID:   agentID,
		labelThreadID:  threadID,
	}

	workloadID := ""
	t.Cleanup(func() {
		if workloadID == "" {
			return
		}
		cleanupWorkload(t, ctx, runnerClient, workloadID)
	})

	pollCtx, pollCancel := context.WithTimeout(ctx, 90*time.Second)
	defer pollCancel()
	if err := pollUntil(pollCtx, pollInterval, func(ctx context.Context) error {
		ids, err := findWorkloadsByLabels(ctx, runnerClient, labelsMap)
		if err != nil {
			return err
		}
		if len(ids) != 1 {
			return fmt.Errorf("expected 1 workload, got %d", len(ids))
		}
		workloadID = ids[0]
		return nil
	}); err != nil {
		t.Fatalf("wait for workload: %v", err)
	}

	clientset := kubeClientset(t)
	namespace := workloadNamespace(t)
	labelSelector := labels.Set(labelsMap).String()

	var workloadPod *corev1.Pod
	podCtx, podCancel := context.WithTimeout(ctx, 90*time.Second)
	defer podCancel()
	if err := pollUntil(podCtx, pollInterval, func(ctx context.Context) error {
		pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return fmt.Errorf("list pods: %w", err)
		}
		if len(pods.Items) == 0 {
			return fmt.Errorf("expected at least 1 pod, got 0")
		}
		for i := range pods.Items {
			if len(pods.Items[i].Spec.ImagePullSecrets) > 0 {
				workloadPod = &pods.Items[i]
				return nil
			}
		}
		return fmt.Errorf("pod image pull secrets not set")
	}); err != nil {
		t.Fatalf("wait for pod image pull secrets: %v", err)
	}
	secretName := workloadPod.Spec.ImagePullSecrets[0].Name
	if secretName == "" {
		t.Fatal("image pull secret name missing")
	}
	var secret *corev1.Secret
	secretCtx, secretCancel := context.WithTimeout(ctx, 90*time.Second)
	defer secretCancel()
	if err := pollUntil(secretCtx, pollInterval, func(ctx context.Context) error {
		fetched, err := clientset.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		secret = fetched
		return nil
	}); err != nil {
		t.Fatalf("wait for image pull secret %s: %v", secretName, err)
	}
	if secret.Type != corev1.SecretTypeDockerConfigJson {
		t.Fatalf("expected secret type %s, got %s", corev1.SecretTypeDockerConfigJson, secret.Type)
	}
	configJSON, ok := secret.Data[corev1.DockerConfigJsonKey]
	if !ok {
		t.Fatalf("missing %s in secret data", corev1.DockerConfigJsonKey)
	}

	var config struct {
		Auths map[string]struct {
			Username string `json:"username"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(configJSON, &config); err != nil {
		t.Fatalf("parse docker config json: %v", err)
	}
	auth, ok := config.Auths[testImagePullRegistry]
	if !ok {
		t.Fatalf("missing auths entry for %s", testImagePullRegistry)
	}
	if auth.Username != testImagePullUsername {
		t.Fatalf("expected username %q, got %q", testImagePullUsername, auth.Username)
	}
}
