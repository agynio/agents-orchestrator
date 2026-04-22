package reconciler

import (
	"context"
	"fmt"
	"log"

	identityv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/identity/v1"
	zitimgmtv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/ziti_management/v1"
)

// managedIdentityPageSize bounds each list call to keep pagination reasonable.
const managedIdentityPageSize int32 = 100

func (r *Reconciler) reconcileOrphanIdentities(ctx context.Context) error {
	orgIdentities, err := r.agentIdentityByOrg(ctx)
	if err != nil {
		return err
	}
	tracked, err := r.listActiveWorkloads(ctx, orgIdentities)
	if err != nil {
		return err
	}
	active := make(map[string]struct{}, len(tracked))
	for _, workload := range tracked {
		zitiIdentityID := workload.GetZitiIdentityId()
		if zitiIdentityID == "" {
			continue
		}
		active[zitiIdentityID] = struct{}{}
	}

	pageToken := ""
	var deleteErr error
	for {
		resp, err := r.zitiMgmt.ListManagedIdentities(ctx, &zitimgmtv1.ListManagedIdentitiesRequest{
			IdentityType: identityv1.IdentityType_IDENTITY_TYPE_AGENT,
			PageSize:     managedIdentityPageSize,
			PageToken:    pageToken,
		})
		if err != nil {
			return fmt.Errorf("list managed identities: %w", err)
		}
		for _, identity := range resp.GetIdentities() {
			if identity == nil {
				return fmt.Errorf("managed identity is nil")
			}
			identityID := identity.GetZitiIdentityId()
			if identityID == "" {
				return fmt.Errorf("managed identity missing ziti_identity_id")
			}
			if _, ok := active[identityID]; ok {
				continue
			}
			if err := r.deleteIdentity(ctx, identityID); err != nil {
				log.Printf("reconciler: delete orphan ziti identity %s: %v", identityID, err)
				if deleteErr == nil {
					deleteErr = fmt.Errorf("delete orphan ziti identity %s: %w", identityID, err)
				}
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return deleteErr
}
