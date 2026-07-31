package reconciler

import (
	"context"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeAgentsClient struct {
	testutil.FakeAgentsClient
	listAgents           func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error)
	listInstances        func(context.Context, *agentsv1.ListInstancesRequest, ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error)
	getUnackedInboxItems func(context.Context, *agentsv1.GetUnackedInboxItemsRequest, ...grpc.CallOption) (*agentsv1.GetUnackedInboxItemsResponse, error)
	getAgent             func(context.Context, *agentsv1.GetAgentRequest, ...grpc.CallOption) (*agentsv1.GetAgentResponse, error)
	pauseInstance        func(context.Context, *agentsv1.PauseInstanceRequest, ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error)
}

type fakeThreadsClient struct {
	degradeThread func(context.Context, *threadsv1.DegradeThreadRequest, ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error)
}

func (f *fakeThreadsClient) CreateThread(context.Context, *threadsv1.CreateThreadRequest, ...grpc.CallOption) (*threadsv1.CreateThreadResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) ArchiveThread(context.Context, *threadsv1.ArchiveThreadRequest, ...grpc.CallOption) (*threadsv1.ArchiveThreadResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) DegradeThread(ctx context.Context, req *threadsv1.DegradeThreadRequest, opts ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error) {
	if f.degradeThread != nil {
		return f.degradeThread(ctx, req, opts...)
	}
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) AddParticipant(context.Context, *threadsv1.AddParticipantRequest, ...grpc.CallOption) (*threadsv1.AddParticipantResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) SendMessage(context.Context, *threadsv1.SendMessageRequest, ...grpc.CallOption) (*threadsv1.SendMessageResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetThreads(context.Context, *threadsv1.GetThreadsRequest, ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) ListOrganizationThreads(context.Context, *threadsv1.ListOrganizationThreadsRequest, ...grpc.CallOption) (*threadsv1.ListOrganizationThreadsResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetThread(context.Context, *threadsv1.GetThreadRequest, ...grpc.CallOption) (*threadsv1.GetThreadResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetOrganizationThreads(context.Context, *threadsv1.GetOrganizationThreadsRequest, ...grpc.CallOption) (*threadsv1.GetOrganizationThreadsResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetMessages(context.Context, *threadsv1.GetMessagesRequest, ...grpc.CallOption) (*threadsv1.GetMessagesResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetUnackedMessages(context.Context, *threadsv1.GetUnackedMessagesRequest, ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) GetUnackedMessageCounts(context.Context, *threadsv1.GetUnackedMessageCountsRequest, ...grpc.CallOption) (*threadsv1.GetUnackedMessageCountsResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) AckMessages(context.Context, *threadsv1.AckMessagesRequest, ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeAgentsClient) ListAgents(ctx context.Context, req *agentsv1.ListAgentsRequest, opts ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	if f.listAgents != nil {
		return f.listAgents(ctx, req, opts...)
	}
	return f.FakeAgentsClient.ListAgents(ctx, req, opts...)
}

func (f *fakeAgentsClient) ListInstances(ctx context.Context, req *agentsv1.ListInstancesRequest, opts ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error) {
	if f.listInstances != nil {
		return f.listInstances(ctx, req, opts...)
	}
	return f.FakeAgentsClient.ListInstances(ctx, req, opts...)
}

func (f *fakeAgentsClient) GetUnackedInboxItems(ctx context.Context, req *agentsv1.GetUnackedInboxItemsRequest, opts ...grpc.CallOption) (*agentsv1.GetUnackedInboxItemsResponse, error) {
	if f.getUnackedInboxItems != nil {
		return f.getUnackedInboxItems(ctx, req, opts...)
	}
	return f.FakeAgentsClient.GetUnackedInboxItems(ctx, req, opts...)
}

func (f *fakeAgentsClient) GetAgent(ctx context.Context, req *agentsv1.GetAgentRequest, opts ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
	if f.getAgent != nil {
		return f.getAgent(ctx, req, opts...)
	}
	return f.FakeAgentsClient.GetAgent(ctx, req, opts...)
}

func (f *fakeAgentsClient) PauseInstance(ctx context.Context, req *agentsv1.PauseInstanceRequest, opts ...grpc.CallOption) (*agentsv1.PauseInstanceResponse, error) {
	if f.pauseInstance != nil {
		return f.pauseInstance(ctx, req, opts...)
	}
	return f.FakeAgentsClient.PauseInstance(ctx, req, opts...)
}

func TestFetchDesiredListsActiveInstancesWithUnackedInbox(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	instanceID := uuid.New()
	orgID := uuid.New()
	updatedAt := time.Now().UTC()
	threadID := uuid.New()
	var listReq *agentsv1.ListInstancesRequest
	var inboxReq *agentsv1.GetUnackedInboxItemsRequest
	agents := &fakeAgentsClient{
		listInstances: func(_ context.Context, req *agentsv1.ListInstancesRequest, _ ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error) {
			listReq = req
			return &agentsv1.ListInstancesResponse{Instances: []*agentsv1.AgentInstance{{
				Meta:           &agentsv1.EntityMeta{Id: instanceID.String()},
				AgentId:        agentID.String(),
				OrganizationId: orgID.String(),
			}}}, nil
		},
		getUnackedInboxItems: func(ctx context.Context, req *agentsv1.GetUnackedInboxItemsRequest, _ ...grpc.CallOption) (*agentsv1.GetUnackedInboxItemsResponse, error) {
			inboxReq = req
			metadataValues, _ := metadata.FromOutgoingContext(ctx)
			identityValues := metadataValues.Get(identityMetadataKey)
			if len(identityValues) != 1 || identityValues[0] != instanceID.String() {
				t.Fatalf("unexpected identity metadata: %v", identityValues)
			}
			return &agentsv1.GetUnackedInboxItemsResponse{Items: []*agentsv1.InboxItem{{ThreadId: stringPtr(threadID.String())}}}, nil
		},
		getAgent: func(_ context.Context, req *agentsv1.GetAgentRequest, _ ...grpc.CallOption) (*agentsv1.GetAgentResponse, error) {
			if req.GetId() != agentID.String() {
				t.Fatalf("unexpected agent id: %s", req.GetId())
			}
			return &agentsv1.GetAgentResponse{Agent: &agentsv1.Agent{
				Meta:           &agentsv1.EntityMeta{Id: agentID.String(), UpdatedAt: timestamppb.New(updatedAt)},
				OrganizationId: orgID.String(),
			}}, nil
		},
	}
	reconciler := &Reconciler{agents: agents, idle: time.Hour}

	desired, idleTimeouts, agentUpdatedAt, err := reconciler.fetchDesired(ctx)
	if err != nil {
		t.Fatalf("fetch desired: %v", err)
	}
	expected := []AgentInstanceTarget{{AgentID: agentID, AgentInstanceID: instanceID, OrganizationID: orgID, ThreadID: threadID}}
	if !reflect.DeepEqual(desired, expected) {
		t.Fatalf("unexpected desired: %#v", desired)
	}
	if listReq == nil || listReq.GetHasUnacked() != true || !reflect.DeepEqual(listReq.GetStateIn(), []agentsv1.AgentInstanceState{agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE}) {
		t.Fatalf("unexpected list instances request: %#v", listReq)
	}
	if inboxReq == nil || inboxReq.GetAgentInstanceId() != instanceID.String() || inboxReq.GetPageSize() != 1 {
		t.Fatalf("unexpected get unacked inbox request: %#v", inboxReq)
	}
	if idleTimeouts[agentID] != time.Hour {
		t.Fatalf("unexpected idle timeout: %v", idleTimeouts[agentID])
	}
	if !agentUpdatedAt[agentID].Equal(updatedAt) {
		t.Fatalf("unexpected updated_at: %v", agentUpdatedAt[agentID])
	}
}

func (f *fakeAgentsClient) ListSandboxes(context.Context, *agentsv1.ListSandboxesRequest, ...grpc.CallOption) (*agentsv1.ListSandboxesResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func (f *fakeAgentsClient) DeleteSandbox(context.Context, *agentsv1.DeleteSandboxRequest, ...grpc.CallOption) (*agentsv1.DeleteSandboxResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func TestListActiveInstancesWithUnackedInboxPaginates(t *testing.T) {
	ctx := context.Background()
	firstInstanceID := uuid.New()
	secondInstanceID := uuid.New()
	var requests []*agentsv1.ListInstancesRequest
	agents := &fakeAgentsClient{
		listInstances: func(_ context.Context, req *agentsv1.ListInstancesRequest, _ ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error) {
			requests = append(requests, req)
			switch req.GetPageToken() {
			case "":
				return &agentsv1.ListInstancesResponse{
					Instances:     []*agentsv1.AgentInstance{{Meta: &agentsv1.EntityMeta{Id: firstInstanceID.String()}}},
					NextPageToken: "next",
				}, nil
			case "next":
				return &agentsv1.ListInstancesResponse{
					Instances: []*agentsv1.AgentInstance{{Meta: &agentsv1.EntityMeta{Id: secondInstanceID.String()}}},
				}, nil
			default:
				t.Fatalf("unexpected page token: %s", req.GetPageToken())
				return nil, nil
			}
		},
	}
	reconciler := &Reconciler{agents: agents}

	instances, err := reconciler.listActiveInstancesWithUnackedInbox(ctx)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
	if len(requests) != 2 {
		t.Fatalf("expected 2 page requests, got %d", len(requests))
	}
	for _, req := range requests {
		if req.GetHasUnacked() != true {
			t.Fatalf("expected has_unacked=true, got %#v", req)
		}
		expectedStateIn := []agentsv1.AgentInstanceState{agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE}
		if !reflect.DeepEqual(req.GetStateIn(), expectedStateIn) {
			t.Fatalf("unexpected state_in: %#v", req.GetStateIn())
		}
	}
	if requests[0].GetPageToken() != "" || requests[1].GetPageToken() != "next" {
		t.Fatalf("unexpected page tokens: %#v %#v", requests[0], requests[1])
	}
}

func (f *fakeAgentsClient) GetSandbox(context.Context, *agentsv1.GetSandboxRequest, ...grpc.CallOption) (*agentsv1.GetSandboxResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) UpdateSandboxRuntimeState(context.Context, *agentsv1.UpdateSandboxRuntimeStateRequest, ...grpc.CallOption) (*agentsv1.UpdateSandboxRuntimeStateResponse, error) {
	return nil, errNotImplemented
}

func (f *fakeAgentsClient) GetVolume(context.Context, *agentsv1.GetVolumeRequest, ...grpc.CallOption) (*agentsv1.GetVolumeResponse, error) {
	return nil, errNotImplemented
}
