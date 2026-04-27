package reconciler

import (
	"context"
	"fmt"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerselect"
)

const runnerPageSize int32 = 100

func (r *Reconciler) selectRunner(ctx context.Context, organizationID string, runnerLabels map[string]string, requiredCapabilities []string) (*runnersv1.Runner, error) {
	runners, err := r.listRunners(ctx, organizationID)
	if err != nil {
		return nil, err
	}
	return runnerselect.Select(runners, organizationID, runnerLabels, requiredCapabilities)
}

func (r *Reconciler) listRunners(ctx context.Context, organizationID string) ([]*runnersv1.Runner, error) {
	if organizationID == "" {
		return nil, fmt.Errorf("organization id missing")
	}
	orgID := organizationID
	var runners []*runnersv1.Runner
	pageToken := ""
	for {
		resp, err := r.runners.ListRunners(runnersContext(ctx), &runnersv1.ListRunnersRequest{
			PageSize:       runnerPageSize,
			PageToken:      pageToken,
			OrganizationId: &orgID,
		})
		if err != nil {
			return nil, fmt.Errorf("list runners: %w", err)
		}
		runners = append(runners, resp.GetRunners()...)
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return runners, nil
}

func (r *Reconciler) listRunnersByOrg(ctx context.Context, orgIdentities map[string]string) ([]*runnersv1.Runner, error) {
	if len(orgIdentities) == 0 {
		return nil, nil
	}
	var runners []*runnersv1.Runner
	for orgID := range orgIdentities {
		orgRunners, err := r.listRunners(ctx, orgID)
		if err != nil {
			return nil, err
		}
		runners = append(runners, orgRunners...)
	}
	return runners, nil
}
