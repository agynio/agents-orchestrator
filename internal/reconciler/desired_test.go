package reconciler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeAgentsClient struct {
	testutil.FakeAgentsClient
	listAgents func(context.Context, *agentsv1.ListAgentsRequest, ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error)
}

func (f *fakeAgentsClient) ListAgents(ctx context.Context, req *agentsv1.ListAgentsRequest, opts ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
	if f.listAgents != nil {
		return f.listAgents(ctx, req, opts...)
	}
	return nil, testutil.ErrNotImplemented
}

type fakeThreadsClient struct {
	getThreads         func(context.Context, *threadsv1.GetThreadsRequest, ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error)
	getUnackedMessages func(context.Context, *threadsv1.GetUnackedMessagesRequest, ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error)
	degradeThread      func(context.Context, *threadsv1.DegradeThreadRequest, ...grpc.CallOption) (*threadsv1.DegradeThreadResponse, error)
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

func (f *fakeThreadsClient) GetThreads(ctx context.Context, req *threadsv1.GetThreadsRequest, opts ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
	if f.getThreads != nil {
		return f.getThreads(ctx, req, opts...)
	}
	return nil, testutil.ErrNotImplemented
}
func (f *fakeThreadsClient) ListOrganizationThreads(ctx context.Context, req *threadsv1.ListOrganizationThreadsRequest, opts ...grpc.CallOption) (*threadsv1.ListOrganizationThreadsResponse, error) {
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

func (f *fakeThreadsClient) GetUnackedMessages(ctx context.Context, req *threadsv1.GetUnackedMessagesRequest, opts ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
	if f.getUnackedMessages != nil {
		return f.getUnackedMessages(ctx, req, opts...)
	}
	return nil, testutil.ErrNotImplemented
}

func (f *fakeThreadsClient) AckMessages(context.Context, *threadsv1.AckMessagesRequest, ...grpc.CallOption) (*threadsv1.AckMessagesResponse, error) {
	return nil, testutil.ErrNotImplemented
}

func TestFetchDesiredSkipsPassiveThreads(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	activeThreadID := uuid.New()
	passiveThreadID := uuid.New()
	otherParticipantID := uuid.New()

	agents := &fakeAgentsClient{
		listAgents: func(_ context.Context, _ *agentsv1.ListAgentsRequest, _ ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
			updatedAt := timestamppb.New(time.Now().UTC())
			return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{
				{Meta: &agentsv1.EntityMeta{Id: agentID.String(), UpdatedAt: updatedAt}},
			}}, nil
		},
	}

	threads := &fakeThreadsClient{
		getUnackedMessages: func(_ context.Context, req *threadsv1.GetUnackedMessagesRequest, _ ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
			if req.GetParticipantId() != agentID.String() {
				return nil, errors.New("unexpected participant id")
			}
			return &threadsv1.GetUnackedMessagesResponse{Messages: []*threadsv1.Message{
				{Id: uuid.NewString(), ThreadId: activeThreadID.String()},
				{Id: uuid.NewString(), ThreadId: passiveThreadID.String()},
			}}, nil
		},
		getThreads: func(_ context.Context, req *threadsv1.GetThreadsRequest, _ ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
			if req.GetParticipantId() != agentID.String() {
				return nil, errors.New("unexpected participant id")
			}
			return &threadsv1.GetThreadsResponse{Threads: []*threadsv1.Thread{
				{
					Id: activeThreadID.String(),
					Participants: []*threadsv1.Participant{
						{Id: agentID.String(), Passive: false},
						{Id: otherParticipantID.String(), Passive: false},
					},
				},
				{
					Id: passiveThreadID.String(),
					Participants: []*threadsv1.Participant{
						{Id: agentID.String(), Passive: true},
						{Id: otherParticipantID.String(), Passive: false},
					},
				},
			}}, nil
		},
	}

	reconciler := New(Config{Agents: agents, Threads: threads})
	result, _, _, err := reconciler.fetchDesired(ctx)
	if err != nil {
		t.Fatalf("fetch desired: %v", err)
	}

	expected := []AgentThread{{AgentID: agentID, ThreadID: activeThreadID}}
	sortAgentThreads(result)
	sortAgentThreads(expected)

	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("expected %+v, got %+v", expected, result)
	}
}

func TestFetchDesiredSkipsPassiveLookupWithoutMessages(t *testing.T) {
	ctx := context.Background()
	agentID := uuid.New()
	getThreadsCalled := false

	agents := &fakeAgentsClient{
		listAgents: func(_ context.Context, _ *agentsv1.ListAgentsRequest, _ ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
			updatedAt := timestamppb.New(time.Now().UTC())
			return &agentsv1.ListAgentsResponse{Agents: []*agentsv1.Agent{
				{Meta: &agentsv1.EntityMeta{Id: agentID.String(), UpdatedAt: updatedAt}},
			}}, nil
		},
	}

	threads := &fakeThreadsClient{
		getUnackedMessages: func(_ context.Context, req *threadsv1.GetUnackedMessagesRequest, _ ...grpc.CallOption) (*threadsv1.GetUnackedMessagesResponse, error) {
			if req.GetParticipantId() != agentID.String() {
				return nil, errors.New("unexpected participant id")
			}
			return &threadsv1.GetUnackedMessagesResponse{}, nil
		},
		getThreads: func(context.Context, *threadsv1.GetThreadsRequest, ...grpc.CallOption) (*threadsv1.GetThreadsResponse, error) {
			getThreadsCalled = true
			return nil, errors.New("unexpected get threads")
		},
	}

	reconciler := New(Config{Agents: agents, Threads: threads})
	result, _, _, err := reconciler.fetchDesired(ctx)
	if err != nil {
		t.Fatalf("fetch desired: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected no desired workloads, got %+v", result)
	}
	if getThreadsCalled {
		t.Fatalf("expected no get threads call")
	}
}
