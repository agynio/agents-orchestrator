package reconciler

import (
	"context"
	"fmt"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerselect"
)

const runnerPageSize int32 = 100

func (r *Reconciler) selectRunner(ctx context.Context, organizationID string, runnerLabels map[string]string, requiredCapabilities []string) (*runnersv1.Runner, error) {
	runners, err := r.listRunners(ctx)
	if err != nil {
		return nil, err
	}
	return runnerselect.Select(runners, organizationID, runnerLabels, requiredCapabilities)
}

func (r *Reconciler) listRunners(ctx context.Context) ([]*runnersv1.Runner, error) {
	var runners []*runnersv1.Runner
	callCtx := r.serviceContext(ctx)
	pageToken := ""
	for {
		// Service identity can list all runners without org scoping.
		resp, err := r.runners.ListRunners(callCtx, &runnersv1.ListRunnersRequest{
			PageSize:  runnerPageSize,
			PageToken: pageToken,
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
