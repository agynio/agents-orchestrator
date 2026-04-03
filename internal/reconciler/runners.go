package reconciler

import (
	"context"
	"fmt"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/runnerselect"
)

const runnerPageSize int32 = 100

func (r *Reconciler) selectRunner(ctx context.Context, organizationID string, runnerLabels map[string]string) (*runnersv1.Runner, error) {
	runners, err := r.listRunners(ctx)
	if err != nil {
		return nil, err
	}
	return runnerselect.Select(runners, organizationID, runnerLabels)
}

func (r *Reconciler) listRunners(ctx context.Context) ([]*runnersv1.Runner, error) {
	var runners []*runnersv1.Runner
	pageToken := ""
	for {
		resp, err := r.runners.ListRunners(ctx, &runnersv1.ListRunnersRequest{
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
