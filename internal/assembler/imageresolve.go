package assembler

import (
	"context"
	"fmt"
	"strings"

	imageproxyv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/image_proxy/v1"
	imagesv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/images/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	"google.golang.org/grpc"
)

// Every catalog image in a workload spec is rewritten to the image proxy:
//
//	<proxy-host>/<org-slug>/<image-name>:<tag>
//
// The path names the Image record, not the upstream repository - two records may
// point at the same upstream with different credentials, and a reference naming
// the upstream could not say which credential to use. No registry address and no
// upstream credential reaches the workload cluster.
//
// Platform containers are deliberately outside this: agynd-cli-init,
// agyn-cli-init and the Ziti sidecar are chart-pinned and pulled from a public
// registry, because the proxy cannot serve the components a workload needs
// before the proxy itself is reachable.

type ImagesClient interface {
	GetImage(ctx context.Context, req *imagesv1.GetImageRequest, opts ...grpc.CallOption) (*imagesv1.GetImageResponse, error)
}

type OrganizationsClient interface {
	GetOrganization(ctx context.Context, req *organizationsv1.GetOrganizationRequest, opts ...grpc.CallOption) (*organizationsv1.GetOrganizationResponse, error)
}

type ImageProxyClient interface {
	MintPullCredential(ctx context.Context, req *imageproxyv1.MintPullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.MintPullCredentialResponse, error)
	RevokePullCredential(ctx context.Context, req *imageproxyv1.RevokePullCredentialRequest, opts ...grpc.CallOption) (*imageproxyv1.RevokePullCredentialResponse, error)
}

// imageRewriter turns catalog references into proxy references and collects the
// image ids a workload must be granted.
type imageRewriter struct {
	images        ImagesClient
	organizations OrganizationsClient
	proxyHost     string

	slugs    map[string]string
	imageIDs []string
}

func newImageRewriter(images ImagesClient, organizations OrganizationsClient, proxyHost string) *imageRewriter {
	return &imageRewriter{
		images:        images,
		organizations: organizations,
		proxyHost:     strings.TrimSpace(proxyHost),
		slugs:         map[string]string{},
	}
}

// enabled reports whether references should be rewritten at all. Without a
// proxy host or a catalog client the spec keeps whatever image it already
// carried, which is the pre-proxy behaviour.
func (r *imageRewriter) enabled() bool {
	return r.proxyHost != "" && r.images != nil && r.organizations != nil
}

// Rewrite resolves one catalog reference to a proxy reference and records the
// grant it needs.
func (r *imageRewriter) Rewrite(ctx context.Context, imageID, tag string) (string, error) {
	if imageID == "" || tag == "" {
		return "", fmt.Errorf("image reference needs both an id and a tag")
	}

	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	image, err := r.images.GetImage(rctx, &imagesv1.GetImageRequest{Id: imageID})
	cancel()
	if err != nil {
		return "", fmt.Errorf("resolve image %s: %w", imageID, err)
	}

	slug, err := r.organizationSlug(ctx, image.GetImage().GetOrganizationId())
	if err != nil {
		return "", err
	}

	r.imageIDs = append(r.imageIDs, imageID)
	return fmt.Sprintf("%s/%s/%s:%s", r.proxyHost, slug, image.GetImage().GetName(), tag), nil
}

// organizationSlug caches per assembly: a workload's images often share an
// owning organization, and the slug is the same for all of them.
func (r *imageRewriter) organizationSlug(ctx context.Context, organizationID string) (string, error) {
	if slug, ok := r.slugs[organizationID]; ok {
		return slug, nil
	}
	rctx, cancel := context.WithTimeout(ctx, rpcTimeout)
	organization, err := r.organizations.GetOrganization(rctx, &organizationsv1.GetOrganizationRequest{Id: organizationID})
	cancel()
	if err != nil {
		return "", fmt.Errorf("resolve organization %s: %w", organizationID, err)
	}
	slug := organization.GetOrganization().GetSlug()
	if slug == "" {
		return "", fmt.Errorf("organization %s has no slug, so its images have no proxy reference", organizationID)
	}
	r.slugs[organizationID] = slug
	return slug, nil
}

// GrantedImageIDs is what the workload's pull credential is scoped to.
func (r *imageRewriter) GrantedImageIDs() []string {
	return r.imageIDs
}
