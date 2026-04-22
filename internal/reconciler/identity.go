package reconciler

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	identityMetadataKey     = "x-identity-id"
	identityTypeMetadataKey = "x-identity-type"
	identityTypeAgent       = "agent"
)

func (r *Reconciler) agentContext(ctx context.Context, agentID uuid.UUID) context.Context {
	return withIdentity(ctx, agentID.String(), identityTypeAgent, true)
}

func (r *Reconciler) serviceContext(ctx context.Context) context.Context {
	return withIdentity(ctx, r.serviceIdentityID.String(), "", false)
}

func withIdentity(ctx context.Context, identityID, identityType string, setType bool) context.Context {
	var md metadata.MD
	if current, ok := metadata.FromOutgoingContext(ctx); ok {
		md = current.Copy()
	} else {
		md = metadata.MD{}
	}
	md.Set(identityMetadataKey, identityID)
	if setType {
		md.Set(identityTypeMetadataKey, identityType)
	} else {
		md.Delete(identityTypeMetadataKey)
	}
	return metadata.NewOutgoingContext(ctx, md)
}
