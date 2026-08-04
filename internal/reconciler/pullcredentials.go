package reconciler

import (
	"context"
	"fmt"
	"log"

	imageproxyv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/image_proxy/v1"
	runnerv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runner/v1"
	"github.com/agynio/agents-orchestrator/internal/assembler"
	"google.golang.org/grpc"
)

// ImageProxyClient is the credential lifecycle behind the image proxy.
type ImageProxyClient interface {
	MintPullCredential(ctx context.Context, req *imageproxyv1.MintPullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error)
	RevokePullCredential(ctx context.Context, req *imageproxyv1.RevokePullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error)
}

// mintPullCredential issues the credential the runner writes into the
// workload's dockerconfigjson secret. One credential covers the whole Pod:
// every catalog image resolves to the same proxy host, the auths key selects
// which credential, and the request path selects which image.
func (r *Reconciler) mintPullCredential(ctx context.Context, workloadID string, assembled *assembler.AssembleResult) ([]*runnerv1.ImagePullCredential, error) {
	if r.imageProxy == nil || r.imageProxyHost == "" || len(assembled.GrantedImageIDs) == 0 {
		return nil, nil
	}
	minted, err := r.imageProxy.MintPullCredential(ctx, &imageproxyv1.MintPullCredentialRequest{
		WorkloadId:     workloadID,
		ImageIds:       assembled.GrantedImageIDs,
		OrganizationId: assembled.OrganizationID,
	})
	if err != nil {
		return nil, fmt.Errorf("mint pull credential for workload %s: %w", workloadID, err)
	}
	return []*runnerv1.ImagePullCredential{{
		Registry: r.imageProxyHost,
		Username: minted.GetUsername(),
		Password: minted.GetPassword(),
	}}, nil
}

// revokePullCredential is called on workload stop, alongside the OpenZiti
// identity delete: both are per-workload grants that outlive nothing. A missed
// revocation is bounded by the credential's TTL rather than left open.
func (r *Reconciler) revokePullCredential(ctx context.Context, workloadID string) {
	if r.imageProxy == nil || workloadID == "" {
		return
	}
	if _, err := r.imageProxy.RevokePullCredential(ctx, &imageproxyv1.RevokePullCredentialRequest{WorkloadId: workloadID}); err != nil {
		// Bounded by the TTL, so a failure here is worth reporting but not
		// worth failing the stop over.
		log.Printf("reconciler: revoke pull credential for workload %s: %v", workloadID, err)
	}
}

// mintSandboxPullCredential is the sandbox counterpart: same grant, same
// scoping, different assemble result.
func (r *Reconciler) mintSandboxPullCredential(ctx context.Context, workloadID string, assembled *assembler.SandboxAssembleResult) ([]*runnerv1.ImagePullCredential, error) {
	if r.imageProxy == nil || r.imageProxyHost == "" || len(assembled.GrantedImageIDs) == 0 {
		return nil, nil
	}
	minted, err := r.imageProxy.MintPullCredential(ctx, &imageproxyv1.MintPullCredentialRequest{
		WorkloadId:     workloadID,
		ImageIds:       assembled.GrantedImageIDs,
		OrganizationId: assembled.OrganizationID,
	})
	if err != nil {
		return nil, fmt.Errorf("mint pull credential for sandbox workload %s: %w", workloadID, err)
	}
	return []*runnerv1.ImagePullCredential{{
		Registry: r.imageProxyHost,
		Username: minted.GetUsername(),
		Password: minted.GetPassword(),
	}}, nil
}
