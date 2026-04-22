package reconciler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const testServiceIdentityID = "7e7d8b2c-6a5c-4c3f-9f5e-8d5b9a7c2f91"

func TestAgentAndServiceIdentityContexts(t *testing.T) {
	agentID := uuid.New()
	reconciler := New(Config{ServiceIdentityID: testServiceIdentityID})

	agentCtx, err := reconciler.agentContext(context.Background(), agentID)
	if err != nil {
		t.Fatalf("agent context: %v", err)
	}
	assertIdentityMetadata(t, agentCtx, agentID.String(), identityTypeAgent)

	serviceCtx, err := reconciler.serviceContext(agentCtx)
	if err != nil {
		t.Fatalf("service context: %v", err)
	}
	assertIdentityMetadata(t, serviceCtx, testServiceIdentityID, "")
}

func TestWithIdentityOverridesExisting(t *testing.T) {
	base := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		identityMetadataKey, "first",
		identityMetadataKey, "second",
		identityTypeMetadataKey, identityTypeAgent,
	))

	ctx, err := withIdentity(base, testServiceIdentityID, "", false)
	if err != nil {
		t.Fatalf("with identity: %v", err)
	}
	assertIdentityMetadata(t, ctx, testServiceIdentityID, "")
}

func assertIdentityMetadata(t *testing.T, ctx context.Context, expectedID, expectedType string) {
	t.Helper()
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatalf("expected outgoing metadata")
	}
	identityValues := md.Get(identityMetadataKey)
	if len(identityValues) != 1 {
		t.Fatalf("expected single identity value, got %d", len(identityValues))
	}
	if identityValues[0] != expectedID {
		t.Fatalf("expected identity id %q, got %q", expectedID, identityValues[0])
	}
	typeValues := md.Get(identityTypeMetadataKey)
	if expectedType == "" {
		if len(typeValues) != 0 {
			t.Fatalf("expected no identity type, got %v", typeValues)
		}
		return
	}
	if len(typeValues) != 1 {
		t.Fatalf("expected single identity type, got %d", len(typeValues))
	}
	if typeValues[0] != expectedType {
		t.Fatalf("expected identity type %q, got %q", expectedType, typeValues[0])
	}
}
