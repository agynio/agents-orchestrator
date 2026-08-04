package assembler

import (
	"context"
	"testing"

	imagesv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/images/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	"google.golang.org/grpc"
)

type fakeImages struct {
	image *imagesv1.Image
	calls int
}

func (f *fakeImages) GetImage(context.Context, *imagesv1.GetImageRequest, ...grpc.CallOption) (*imagesv1.GetImageResponse, error) {
	f.calls++
	return &imagesv1.GetImageResponse{Image: f.image}, nil
}

type fakeOrganizations struct {
	slug  string
	calls int
}

func (f *fakeOrganizations) GetOrganization(context.Context, *organizationsv1.GetOrganizationRequest, ...grpc.CallOption) (*organizationsv1.GetOrganizationResponse, error) {
	f.calls++
	return &organizationsv1.GetOrganizationResponse{
		Organization: &organizationsv1.Organization{Id: "org-1", Slug: f.slug},
	}, nil
}

func catalogImage() *imagesv1.Image {
	return &imagesv1.Image{
		Meta:           &imagesv1.EntityMeta{Id: "image-1"},
		OrganizationId: "org-1",
		Name:           "devcontainer-go",
		// The upstream repository is present on the record and must not appear
		// in the rewritten reference.
		Repository: "ghcr.io/agynio/devcontainer-go",
	}
}

// The reference names the Image record - organization slug and image name -
// not the upstream repository.
func TestRewriteProducesAProxyReference(t *testing.T) {
	images := &fakeImages{image: catalogImage()}
	organizations := &fakeOrganizations{slug: "acme"}
	rewriter := newImageRewriter(images, organizations, "registry.agyn.dev")

	reference, err := rewriter.Rewrite(context.Background(), "image-1", "1.2.3")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if reference != "registry.agyn.dev/acme/devcontainer-go:1.2.3" {
		t.Fatalf("reference = %q", reference)
	}
	if got := rewriter.GrantedImageIDs(); len(got) != 1 || got[0] != "image-1" {
		t.Fatalf("granted = %v", got)
	}
}

// A pull issues one request per blob, all naming the same organization.
func TestRewriteCachesTheOrganizationSlug(t *testing.T) {
	organizations := &fakeOrganizations{slug: "acme"}
	rewriter := newImageRewriter(&fakeImages{image: catalogImage()}, organizations, "registry.agyn.dev")

	for i := 0; i < 3; i++ {
		if _, err := rewriter.Rewrite(context.Background(), "image-1", "1.2.3"); err != nil {
			t.Fatalf("Rewrite: %v", err)
		}
	}
	if organizations.calls != 1 {
		t.Fatalf("resolved the slug %d times, want 1", organizations.calls)
	}
}

// Without a slug there is no reference scheme, and silently emitting an
// upstream address would defeat the point of the proxy.
func TestRewriteFailsWithoutASlug(t *testing.T) {
	rewriter := newImageRewriter(&fakeImages{image: catalogImage()}, &fakeOrganizations{slug: ""}, "registry.agyn.dev")

	if _, err := rewriter.Rewrite(context.Background(), "image-1", "1.2.3"); err == nil {
		t.Fatal("expected an organization with no slug to be refused")
	}
}

func TestRewriteNeedsBothHalvesOfAReference(t *testing.T) {
	rewriter := newImageRewriter(&fakeImages{image: catalogImage()}, &fakeOrganizations{slug: "acme"}, "registry.agyn.dev")

	if _, err := rewriter.Rewrite(context.Background(), "image-1", ""); err == nil {
		t.Fatal("expected an id with no tag to be refused")
	}
}

// Unconfigured, the spec keeps whatever reference it already carried.
func TestRewriterIsDisabledWithoutAProxyHost(t *testing.T) {
	if newImageRewriter(&fakeImages{}, &fakeOrganizations{}, "").enabled() {
		t.Fatal("expected no proxy host to disable rewriting")
	}
	if newImageRewriter(nil, nil, "registry.agyn.dev").enabled() {
		t.Fatal("expected missing clients to disable rewriting")
	}
}
