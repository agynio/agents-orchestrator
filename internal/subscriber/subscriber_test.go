package subscriber

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"github.com/agynio/agents-orchestrator/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestSubscriberWakeOnMessageCreated(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "11111111-1111-1111-1111-111111111111"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, "")
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs, 1)

	responses <- messageEnvelope("message.created")
	waitForAck(t, ack, 1)

	select {
	case <-harness.subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberWakeOnAgentUpdated(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "22222222-2222-2222-2222-222222222222"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, "")
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs, 1)

	responses <- messageEnvelope("agent.updated")
	waitForAck(t, ack, 1)

	select {
	case <-harness.subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberIgnoresOtherEvents(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "33333333-3333-3333-3333-333333333333"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, "")
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs, 1)

	responses <- messageEnvelope("thread.updated")
	waitForAck(t, ack, 1)

	select {
	case <-harness.subscriber.Wake():
		t.Fatal("unexpected wake signal")
	case <-time.After(200 * time.Millisecond):
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberCoalescesWake(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 2)
	ack := make(chan struct{}, 2)
	agentID := "44444444-4444-4444-4444-444444444444"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, "")
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs, 1)

	responses <- messageEnvelope("message.created")
	responses <- messageEnvelope("message.created")
	waitForAck(t, ack, 2)

	select {
	case <-harness.subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}

	select {
	case <-harness.subscriber.Wake():
		t.Fatal("expected wake to be coalesced")
	case <-time.After(200 * time.Millisecond):
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberSubscribesWithRooms(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "55555555-5555-5555-5555-555555555555"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, "")
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs, 1)
	expected := []string{"agent:" + agentID, "thread_participant:" + agentID}
	if !reflect.DeepEqual(req.GetRooms(), expected) {
		t.Fatalf("expected rooms %v, got %v", expected, req.GetRooms())
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberAddsServiceTokenMetadata(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "88888888-8888-8888-8888-888888888888"
	token := "service-token"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, token)
	defer harness.cancel()

	waitForSubscribe(t, harness.subscribeReqs, 1)
	gotToken := waitForSubscribeToken(t, harness.subscribeTokens, 1)
	if gotToken != token {
		t.Fatalf("expected service token %q, got %q", token, gotToken)
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberResubscribesOnAgentChange(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse)
	ack := make(chan struct{}, 1)
	agentID := "66666666-6666-6666-6666-666666666666"
	updatedAgentID := "77777777-7777-7777-7777-777777777777"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, 10*time.Millisecond, "")
	defer harness.cancel()

	firstReq := waitForSubscribe(t, harness.subscribeReqs, 1)
	firstExpected := []string{"agent:" + agentID, "thread_participant:" + agentID}
	if !reflect.DeepEqual(firstReq.GetRooms(), firstExpected) {
		t.Fatalf("expected initial rooms %v, got %v", firstExpected, firstReq.GetRooms())
	}

	harness.store.set([]*agentsv1.Agent{agentFixture(agentID), agentFixture(updatedAgentID)})
	secondReq := waitForSubscribe(t, harness.subscribeReqs, 1)
	secondExpected := []string{
		"agent:" + agentID,
		"agent:" + updatedAgentID,
		"thread_participant:" + agentID,
		"thread_participant:" + updatedAgentID,
	}
	if !reflect.DeepEqual(secondReq.GetRooms(), secondExpected) {
		t.Fatalf("expected updated rooms %v, got %v", secondExpected, secondReq.GetRooms())
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func newSubscriberHarness(t *testing.T, responses chan *notificationsv1.SubscribeResponse, ack chan struct{}, initialAgents []*agentsv1.Agent, refreshInterval time.Duration, serviceToken string) *subscriberHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store := &agentStore{agents: initialAgents}
	agentsClient := &testutil.FakeAgentsClient{ListAgentsFunc: func(ctx context.Context, req *agentsv1.ListAgentsRequest, opts ...grpc.CallOption) (*agentsv1.ListAgentsResponse, error) {
		return &agentsv1.ListAgentsResponse{Agents: store.list()}, nil
	}}
	subscribeReqs := make(chan *notificationsv1.SubscribeRequest, 4)
	subscribeTokens := make(chan string, 4)
	client := &fakeNotificationsClient{subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
		token := ""
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			values := md.Get(serviceTokenMetadataKey)
			if len(values) > 0 {
				token = values[0]
			}
		}
		subscribeTokens <- token
		subscribeReqs <- req
		return &fakeSubscribeStream{
			fakeClientStream: fakeClientStream{ctx: ctx},
			responses:        responses,
			ack:              ack,
		}, nil
	}}
	subscriber := New(client, agentsClient, WithServiceToken(serviceToken))
	subscriber.roomRefreshInterval = refreshInterval
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Run(ctx)
	}()
	return &subscriberHarness{
		subscriber:      subscriber,
		cancel:          cancel,
		done:            done,
		store:           store,
		subscribeReqs:   subscribeReqs,
		subscribeTokens: subscribeTokens,
	}
}

func messageEnvelope(event string) *notificationsv1.SubscribeResponse {
	return &notificationsv1.SubscribeResponse{
		Envelope: &notificationsv1.NotificationEnvelope{Event: event},
	}
}

func waitForSubscribe(t *testing.T, subscribeReqs <-chan *notificationsv1.SubscribeRequest, count int) *notificationsv1.SubscribeRequest {
	t.Helper()
	var req *notificationsv1.SubscribeRequest
	for i := 0; i < count; i++ {
		select {
		case req = <-subscribeReqs:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for subscribe %d", i)
		}
	}
	return req
}

func waitForSubscribeToken(t *testing.T, tokens <-chan string, count int) string {
	t.Helper()
	var token string
	for i := 0; i < count; i++ {
		select {
		case token = <-tokens:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for subscribe token %d", i)
		}
	}
	return token
}

func waitForAck(t *testing.T, ack <-chan struct{}, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		select {
		case <-ack:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for ack %d", i)
		}
	}
}

type fakeNotificationsClient struct {
	subscribe func(context.Context, *notificationsv1.SubscribeRequest, ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error)
}

func (f *fakeNotificationsClient) Subscribe(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
	return f.subscribe(ctx, req, opts...)
}

func (f *fakeNotificationsClient) Publish(ctx context.Context, req *notificationsv1.PublishRequest, opts ...grpc.CallOption) (*notificationsv1.PublishResponse, error) {
	return nil, errors.New("not implemented")
}

type fakeSubscribeStream struct {
	fakeClientStream
	responses <-chan *notificationsv1.SubscribeResponse
	ack       chan<- struct{}
}

func (f *fakeSubscribeStream) Recv() (*notificationsv1.SubscribeResponse, error) {
	select {
	case <-f.Context().Done():
		return nil, f.Context().Err()
	case resp, ok := <-f.responses:
		if !ok {
			return nil, io.EOF
		}
		if f.ack != nil {
			f.ack <- struct{}{}
		}
		return resp, nil
	}
}

type fakeClientStream struct {
	ctx context.Context
}

func (f fakeClientStream) Header() (metadata.MD, error) { return nil, nil }

func (f fakeClientStream) Trailer() metadata.MD { return nil }

func (f fakeClientStream) CloseSend() error { return nil }

func (f fakeClientStream) Context() context.Context { return f.ctx }

func (f fakeClientStream) SendMsg(any) error { return nil }

func (f fakeClientStream) RecvMsg(any) error { return nil }

type agentStore struct {
	mu     sync.RWMutex
	agents []*agentsv1.Agent
}

func (s *agentStore) list() []*agentsv1.Agent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	agents := make([]*agentsv1.Agent, len(s.agents))
	copy(agents, s.agents)
	return agents
}

func (s *agentStore) set(agents []*agentsv1.Agent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents = agents
}

type subscriberHarness struct {
	subscriber      *Subscriber
	cancel          context.CancelFunc
	done            chan error
	store           *agentStore
	subscribeReqs   chan *notificationsv1.SubscribeRequest
	subscribeTokens chan string
}

func agentFixture(id string) *agentsv1.Agent {
	return &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: id}}
}
