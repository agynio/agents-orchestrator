package assembler

import (
	"context"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	runnersv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/runners/v1"
	"github.com/agynio/agents-orchestrator/internal/config"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"

	imagesv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/images/v1"
	organizationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/organizations/v1"
	"google.golang.org/grpc"
)

// The catalog an environment's runtime image resolves through. Every workload
// needs one now that naming no runtime is refused, so these stand in for the
// two services the rewriter reads.
type fakeImagesClient struct {
	name           string
	organizationID string
}

func (f *fakeImagesClient) GetImage(_ context.Context, req *imagesv1.GetImageRequest, _ ...grpc.CallOption) (*imagesv1.GetImageResponse, error) {
	return &imagesv1.GetImageResponse{Image: &imagesv1.Image{
		Meta:           &imagesv1.EntityMeta{Id: req.GetId()},
		Name:           f.name,
		OrganizationId: f.organizationID,
	}}, nil
}

type fakeOrganizationsClient struct {
	slug string
}

func (f *fakeOrganizationsClient) GetOrganization(_ context.Context, req *organizationsv1.GetOrganizationRequest, _ ...grpc.CallOption) (*organizationsv1.GetOrganizationResponse, error) {
	return &organizationsv1.GetOrganizationResponse{Organization: &organizationsv1.Organization{
		Id:   req.GetId(),
		Slug: f.slug,
	}}, nil
}

const (
	testCatalogProxyHost   = "registry.agyn.test"
	testCatalogOrgSlug     = "org-one"
	testRuntimeImageName   = "agent-runtime"
	testRuntimeImageTag    = "1.0.0"
	testCatalogRunnerID    = "11111111-1111-1111-1111-111111111111"
	testRuntimeImageID     = "22222222-2222-2222-2222-222222222222"
	testResolvedRuntimeRef = testCatalogProxyHost + "/" + testCatalogOrgSlug + "/" + testRuntimeImageName + ":" + testRuntimeImageTag
)

// withCatalog wires an assembler to resolve the runtime image an environment
// names. Assembly refuses a workload without one, so a test that assembles at
// all needs this.
func withCatalog(a *Assembler, organizationID string) *Assembler {
	return a.WithCatalog(
		&fakeImagesClient{name: testRuntimeImageName, organizationID: organizationID},
		&fakeOrganizationsClient{slug: testCatalogOrgSlug},
		nil,
	)
}

// withRuntimeEnvironment gives an agent the environment assembly now requires:
// one naming a runtime image, resolved through the catalog. The environment
// carries the agent's own image so a test asserting on the main container still
// sees what it set.
func withRuntimeEnvironment(agent *agentsv1.Agent, client *testutil.FakeAgentsClient, cfg *config.Config) {
	environmentID := uuid.NewString()
	agent.EnvironmentId = environmentID
	cfg.ImageProxyHost = testCatalogProxyHost
	client.GetEnvironmentFunc = func(_ context.Context, _ *agentsv1.GetEnvironmentRequest, _ ...grpc.CallOption) (*agentsv1.GetEnvironmentResponse, error) {
		return &agentsv1.GetEnvironmentResponse{Environment: &agentsv1.Environment{
			Meta:                 &agentsv1.EntityMeta{Id: environmentID},
			OrganizationId:       agent.GetOrganizationId(),
			Image:                agent.GetImage(),
			RunnerId:             testCatalogRunnerID,
			AgentRuntimeImageId:  testRuntimeImageID,
			AgentRuntimeImageTag: testRuntimeImageTag,
		}}, nil
	}
}

// runnersWithDefaultFlavor serves the one flavor an environment's runner needs.
// Naming no flavor takes the runner's default, which is what these tests want:
// they assert on containers, not on placement.
func runnersWithDefaultFlavor() *fakeRunnersClient {
	return &fakeRunnersClient{
		listFlavors: func(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
			return &runnersv1.ListFlavorsResponse{Flavors: []*runnersv1.Flavor{{
				Name:    "default",
				Default: true,
				Resources: &runnersv1.ComputeResources{
					RequestsCpu:    "500m",
					RequestsMemory: "512Mi",
				},
			}}}, nil
		},
	}
}
