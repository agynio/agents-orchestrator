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
	instanceID := "11111111-1111-1111-1111-111111111111"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, time.Hour)
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

func TestSubscriberWakeOnInstanceUpdated(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	instanceID := "22222222-2222-2222-2222-222222222222"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, time.Hour)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs, 1)

	responses <- messageEnvelope("instance.updated")
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
	instanceID := "33333333-3333-3333-3333-333333333333"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, time.Hour)
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
	instanceID := "44444444-4444-4444-4444-444444444444"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, time.Hour)
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
	instanceID := "55555555-5555-5555-5555-555555555555"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, time.Hour)
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs, 1)
	expected := []string{"agent_instance:" + instanceID, "instance_inbox:" + instanceID}
	if !reflect.DeepEqual(req.req.GetRooms(), expected) {
		t.Fatalf("expected rooms %v, got %v", expected, req.req.GetRooms())
	}
	assertSubscribeIdentity(t, req, instanceID)

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberResubscribesOnInstanceChange(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse)
	ack := make(chan struct{}, 1)
	instanceID := "66666666-6666-6666-6666-666666666666"
	updatedInstanceID := "77777777-7777-7777-7777-777777777777"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.AgentInstance{instanceFixture(instanceID)}, 10*time.Millisecond)
	defer harness.cancel()

	firstReq := waitForSubscribe(t, harness.subscribeReqs, 1)
	firstExpected := []string{"agent_instance:" + instanceID, "instance_inbox:" + instanceID}
	if !reflect.DeepEqual(firstReq.req.GetRooms(), firstExpected) {
		t.Fatalf("expected initial rooms %v, got %v", firstExpected, firstReq.req.GetRooms())
	}

	harness.store.set([]*agentsv1.AgentInstance{instanceFixture(instanceID), instanceFixture(updatedInstanceID)})
	secondReqs := waitForSubscribeRequests(t, harness.subscribeReqs, 2)
	assertSubscribeRequestForIdentity(t, secondReqs, instanceID)
	assertSubscribeRequestForIdentity(t, secondReqs, updatedInstanceID)

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func newSubscriberHarness(t *testing.T, responses chan *notificationsv1.SubscribeResponse, ack chan struct{}, initialInstances []*agentsv1.AgentInstance, refreshInterval time.Duration) *subscriberHarness {
	t.Helper()
	return newSubscriberHarnessWithSandboxOrgs(t, responses, ack, initialAgents, refreshInterval, nil)
}

func newSubscriberHarnessWithSandboxOrgs(t *testing.T, responses chan *notificationsv1.SubscribeResponse, ack chan struct{}, initialAgents []*agentsv1.Agent, refreshInterval time.Duration, sandboxOrgIDs []string) *subscriberHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	store := &instanceStore{instances: initialInstances}
	agentsClient := &testutil.FakeAgentsClient{ListInstancesFunc: func(ctx context.Context, req *agentsv1.ListInstancesRequest, opts ...grpc.CallOption) (*agentsv1.ListInstancesResponse, error) {
		return &agentsv1.ListInstancesResponse{Instances: store.list()}, nil
	}}
	subscribeReqs := make(chan subscribeCall, 4)
	client := &fakeNotificationsClient{subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
		call := subscribeCall{req: req}
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			values := md.Get(identityMetadataKey)
			if len(values) > 0 {
				call.identityID = values[0]
			}
		}
		subscribeReqs <- call
		return &fakeSubscribeStream{
			fakeClientStream: fakeClientStream{ctx: ctx},
			responses:        responses,
			ack:              ack,
		}, nil
	}}
	subscriber := NewWithSandboxOrganizations(client, agentsClient, sandboxOrgIDs)
	subscriber.roomRefreshInterval = refreshInterval
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Run(ctx)
	}()
	return &subscriberHarness{
		subscriber:    subscriber,
		cancel:        cancel,
		done:          done,
		store:         store,
		subscribeReqs: subscribeReqs,
	}
}

func messageEnvelope(event string) *notificationsv1.SubscribeResponse {
	return &notificationsv1.SubscribeResponse{
		Envelope: &notificationsv1.NotificationEnvelope{Event: event},
	}
}

type subscribeCall struct {
	req        *notificationsv1.SubscribeRequest
	identityID string
}

func waitForSubscribe(t *testing.T, subscribeReqs <-chan subscribeCall, count int) subscribeCall {
	t.Helper()
	reqs := waitForSubscribeRequests(t, subscribeReqs, count)
	return reqs[len(reqs)-1]
}

func waitForSubscribeRequests(t *testing.T, subscribeReqs <-chan subscribeCall, count int) []subscribeCall {
	t.Helper()
	reqs := make([]subscribeCall, 0, count)
	for i := 0; i < count; i++ {
		select {
		case req := <-subscribeReqs:
			reqs = append(reqs, req)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timeout waiting for subscribe %d", i)
		}
	}
	return reqs
}

func assertSubscribeRequestForIdentity(t *testing.T, reqs []subscribeCall, instanceID string) {
	t.Helper()
	expected := []string{"agent_instance:" + instanceID, "instance_inbox:" + instanceID}
	for _, req := range reqs {
		if reflect.DeepEqual(req.req.GetRooms(), expected) {
			assertSubscribeIdentity(t, req, instanceID)
			return
		}
	}
	t.Fatalf("expected subscribe request for instance %s in %+v", instanceID, reqs)
}

func assertSubscribeIdentity(t *testing.T, req subscribeCall, instanceID string) {
	t.Helper()
	if req.identityID != instanceID {
		t.Fatalf("expected subscribe identity %s, got %q", instanceID, req.identityID)
	}
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

type instanceStore struct {
	mu        sync.RWMutex
	instances []*agentsv1.AgentInstance
}

func (s *instanceStore) list() []*agentsv1.AgentInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	instances := make([]*agentsv1.AgentInstance, len(s.instances))
	copy(instances, s.instances)
	return instances
}

func (s *instanceStore) set(instances []*agentsv1.AgentInstance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances = instances
}

type subscriberHarness struct {
	subscriber    *Subscriber
	cancel        context.CancelFunc
	done          chan error
	store         *instanceStore
	subscribeReqs chan subscribeCall
}

func instanceFixture(id string) *agentsv1.AgentInstance {
	return &agentsv1.AgentInstance{Meta: &agentsv1.EntityMeta{Id: id}}
}

func TestSubscriberWakesSandboxOnSandboxUpdated(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "88888888-8888-8888-8888-888888888888"
	orgID := "99999999-9999-9999-9999-999999999999"
	harness := newSubscriberHarness(t, responses, ack, []*agentsv1.Agent{agentInOrgFixture(agentID, orgID)}, time.Hour)
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs, 1)
	expected := []string{"agent:" + agentID, "sandbox_org:" + orgID, "thread_participant:" + agentID}
	if !reflect.DeepEqual(req.req.GetRooms(), expected) {
		t.Fatalf("expected rooms %v, got %v", expected, req.req.GetRooms())
	}

	responses <- messageEnvelope("sandbox.updated")
	waitForAck(t, ack, 1)

	select {
	case <-harness.subscriber.SandboxWake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected sandbox wake signal")
	}
	select {
	case <-harness.subscriber.Wake():
		t.Fatal("sandbox.updated must not wake the agent loop")
	case <-time.After(200 * time.Millisecond):
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberSubscribesConfiguredSandboxOrganizations(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	agentID := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	configuredOrgID := "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	harness := newSubscriberHarnessWithSandboxOrgs(t, responses, ack, []*agentsv1.Agent{agentFixture(agentID)}, time.Hour, []string{configuredOrgID})
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs, 1)
	expected := []string{"agent:" + agentID, "sandbox_org:" + configuredOrgID, "thread_participant:" + agentID}
	if !reflect.DeepEqual(req.req.GetRooms(), expected) {
		t.Fatalf("expected rooms %v, got %v", expected, req.req.GetRooms())
	}

	harness.cancel()
	if err := <-harness.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func agentInOrgFixture(id, organizationID string) *agentsv1.Agent {
	return &agentsv1.Agent{Meta: &agentsv1.EntityMeta{Id: id}, OrganizationId: organizationID}
}
