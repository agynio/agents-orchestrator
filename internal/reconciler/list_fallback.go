package reconciler

import (
	"context"

	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (r *Reconciler) listWorkloadsWithFallback(adminCtx, orgCtx context.Context, useOrgIdentity *bool, req *runnersv1.ListWorkloadsRequest) (*runnersv1.ListWorkloadsResponse, error) {
	ctx := adminCtx
	if *useOrgIdentity {
		ctx = orgCtx
	}
	resp, err := r.runners.ListWorkloads(ctx, req)
	if err != nil && !*useOrgIdentity && isPermissionDenied(err) {
		*useOrgIdentity = true
		resp, err = r.runners.ListWorkloads(orgCtx, req)
	}
	return resp, err
}

func (r *Reconciler) listVolumesWithFallback(adminCtx, orgCtx context.Context, useOrgIdentity *bool, req *runnersv1.ListVolumesRequest) (*runnersv1.ListVolumesResponse, error) {
	ctx := adminCtx
	if *useOrgIdentity {
		ctx = orgCtx
	}
	resp, err := r.runners.ListVolumes(ctx, req)
	if err != nil && !*useOrgIdentity && isPermissionDenied(err) {
		*useOrgIdentity = true
		resp, err = r.runners.ListVolumes(orgCtx, req)
	}
	return resp, err
}

func isPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	statusInfo, ok := status.FromError(err)
	if !ok {
		return false
	}
	return statusInfo.Code() == codes.PermissionDenied
}
