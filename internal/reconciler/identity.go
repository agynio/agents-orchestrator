package reconciler

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	identityMetadataKey     = "x-identity-id"
	identityTypeMetadataKey = "x-identity-type"
	identityTypeAgent       = "agent"
)

func (r *Reconciler) agentContext(ctx context.Context, agentID uuid.UUID) (context.Context, error) {
	return withIdentity(ctx, agentID.String(), identityTypeAgent, true)
}

func (r *Reconciler) serviceContext(ctx context.Context) (context.Context, error) {
	return withIdentity(ctx, r.serviceIdentityID, "", false)
}

func withIdentity(ctx context.Context, identityID, identityType string, setType bool) (context.Context, error) {
	trimmedID := strings.TrimSpace(identityID)
	if trimmedID == "" {
		return nil, fmt.Errorf("identity id missing")
	}
	var md metadata.MD
	if current, ok := metadata.FromOutgoingContext(ctx); ok {
		md = current.Copy()
	} else {
		md = metadata.MD{}
	}
	md.Set(identityMetadataKey, trimmedID)
	if setType {
		trimmedType := strings.TrimSpace(identityType)
		if trimmedType == "" {
			return nil, fmt.Errorf("identity type missing")
		}
		md.Set(identityTypeMetadataKey, trimmedType)
	} else {
		md.Delete(identityTypeMetadataKey)
	}
	return metadata.NewOutgoingContext(ctx, md), nil
}
