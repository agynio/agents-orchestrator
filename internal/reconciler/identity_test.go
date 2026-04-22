package reconciler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

var testServiceIdentityID = uuid.MustParse("7e7d8b2c-6a5c-4c3f-9f5e-8d5b9a7c2f91")

func TestAgentAndServiceIdentityContexts(t *testing.T) {
	agentID := uuid.New()
	reconciler := New(Config{ServiceIdentityID: testServiceIdentityID})

	agentCtx := reconciler.agentContext(context.Background(), agentID)
	assertIdentityMetadata(t, agentCtx, agentID.String(), identityTypeAgent)

	serviceCtx := reconciler.serviceContext(agentCtx)
	assertIdentityMetadata(t, serviceCtx, testServiceIdentityID.String(), "")
}

func TestWithIdentityOverridesExisting(t *testing.T) {
	base := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(
		identityMetadataKey, "first",
		identityMetadataKey, "second",
		identityTypeMetadataKey, identityTypeAgent,
	))

	ctx := withIdentity(base, testServiceIdentityID.String(), "", false)
	assertIdentityMetadata(t, ctx, testServiceIdentityID.String(), "")
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
