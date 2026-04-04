//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	usersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/users/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	setupTimeout = 30 * time.Second
	apiTokenName = "e2e-orchestrator"
)

func withIdentity(ctx context.Context, identityID string) context.Context {
	md := metadata.New(map[string]string{"x-identity-id": identityID})
	return metadata.NewOutgoingContext(ctx, md)
}

func resolveOrCreateUser(t *testing.T, ctx context.Context, client usersv1.UsersServiceClient) string {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	subject := fmt.Sprintf("e2e-orchestrator-%s", uuid.NewString())
	name := fmt.Sprintf("E2E Orchestrator %s", subject)
	email := fmt.Sprintf("%s@test.local", subject)

	resp, err := client.ResolveOrCreateUser(callCtx, &usersv1.ResolveOrCreateUserRequest{
		OidcSubject: subject,
		Name:        name,
		Email:       email,
	})
	if err != nil {
		t.Fatalf("resolve user %s: %v", subject, err)
	}
	if resp == nil || resp.GetUser() == nil || resp.GetUser().GetMeta() == nil {
		t.Fatalf("resolve user %s: missing user metadata", subject)
	}
	identityID := strings.TrimSpace(resp.GetUser().GetMeta().GetId())
	if identityID == "" {
		t.Fatalf("resolve user %s: identity id missing", subject)
	}
	return identityID
}

func createAPIToken(t *testing.T, ctx context.Context, client usersv1.UsersServiceClient, identityID string) string {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	callCtx = withIdentity(callCtx, identityID)
	resp, err := client.CreateAPIToken(callCtx, &usersv1.CreateAPITokenRequest{Name: apiTokenName})
	if err != nil {
		t.Fatalf("create api token: %v", err)
	}
	if resp == nil || resp.GetToken() == nil {
		t.Fatal("create api token: missing response")
	}
	token := strings.TrimSpace(resp.GetPlaintextToken())
	if token == "" {
		t.Fatal("create api token: plaintext token missing")
	}
	return token
}

func createTestOrganization(t *testing.T, ctx context.Context, client organizationsv1.OrganizationsServiceClient, identityID string) string {
	t.Helper()
	callCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()

	name := fmt.Sprintf("e2e-orchestrator-org-%s", uuid.NewString())
	callCtx = withIdentity(callCtx, identityID)
	resp, err := client.CreateOrganization(callCtx, &organizationsv1.CreateOrganizationRequest{Name: name})
	if err != nil {
		t.Fatalf("create organization %s: %v", name, err)
	}
	if resp == nil || resp.GetOrganization() == nil {
		t.Fatalf("create organization %s: missing organization", name)
	}
	orgID := strings.TrimSpace(resp.GetOrganization().GetId())
	if orgID == "" {
		t.Fatalf("create organization %s: id missing", name)
	}
	return orgID
}
