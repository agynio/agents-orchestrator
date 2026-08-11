package subscriber

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// testPlatformIdentityID stands for the identity Identity registers from
// configuration and grants cluster admin.
var testPlatformIdentityID = uuid.MustParse("a3c1e9d2-7f4b-5e1a-9c3d-2b8f6a4e7d10")

func TestSubscriberWakeOnMessageCreated(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("message.created")
	waitForAck(t, harness.ack, 1)

	harness.expectWake(t)
	harness.stop(t)
}

func TestSubscriberWakeOnInstanceUpdated(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("instance.updated")
	waitForAck(t, harness.ack, 1)

	harness.expectWake(t)
	harness.stop(t)
}

func TestSubscriberIgnoresOtherEvents(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("thread.updated")
	waitForAck(t, harness.ack, 1)

	select {
	case <-harness.subscriber.Wake():
		t.Fatal("unexpected wake signal")
	case <-time.After(200 * time.Millisecond):
	}
	harness.stop(t)
}

func TestSubscriberCoalescesWake(t *testing.T) {
	harness := newSubscriberHarness(t, 2)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("message.created")
	harness.responses <- messageEnvelope("message.created")
	waitForAck(t, harness.ack, 2)

	harness.expectWake(t)
	select {
	case <-harness.subscriber.Wake():
		t.Fatal("expected wake to be coalesced")
	case <-time.After(200 * time.Millisecond):
	}
	harness.stop(t)
}

// A workload backs either kind of owner and the event does not say which, so
// the report the runner sends wakes both loops.
func TestSubscriberWakesBothLoopsOnWorkloadStatusChanged(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("workload.status_changed")
	waitForAck(t, harness.ack, 1)

	harness.expectWake(t)
	select {
	case <-harness.subscriber.SandboxWake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected sandbox wake signal")
	}
	harness.stop(t)
}

func TestSubscriberWakesSandboxOnSandboxUpdated(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()
	waitForSubscribe(t, harness.subscribeReqs)

	harness.responses <- messageEnvelope("sandbox.updated")
	waitForAck(t, harness.ack, 1)

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
	harness.stop(t)
}

// The rooms are named outright rather than derived from the instances and
// sandboxes that exist. A derived set cannot cover the organization whose first
// sandbox is the very event that would have announced it.
func TestSubscriberSubscribesToConstantRooms(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs)
	expected := []string{"agent_instances", "sandboxes", "workloads"}
	if !reflect.DeepEqual(req.req.GetRooms(), expected) {
		t.Fatalf("expected rooms %v, got %v", expected, req.req.GetRooms())
	}
	harness.stop(t)
}

// The Orchestrator used to name an agent instance as itself so that instance's
// rooms would admit it. It says what it is now, and Notifications settles that
// against the cluster admin relation behind it.
func TestSubscriberSubscribesAsThePlatform(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs)
	if req.identityType != platformIdentityType {
		t.Fatalf("expected identity type %q, got %q", platformIdentityType, req.identityType)
	}
	if req.identityID != testPlatformIdentityID.String() {
		t.Fatalf("expected identity %s, got %q", testPlatformIdentityID, req.identityID)
	}
	harness.stop(t)
}

// Reading what an instance was sent is the instance's business. The wake the
// Orchestrator needs reaches the cluster-wide room, so it has no reason to hold
// an inbox and must not ask for one.
func TestSubscriberNeverSubscribesToAnInbox(t *testing.T) {
	harness := newSubscriberHarness(t, 1)
	defer harness.cancel()

	req := waitForSubscribe(t, harness.subscribeReqs)
	for _, room := range req.req.GetRooms() {
		if strings.HasPrefix(room, "instance_inbox") {
			t.Fatalf("subscribed to an inbox room: %q", room)
		}
	}
	harness.stop(t)
}

// A refused Subscribe is retried rather than abandoned, and the retry is paced.
func TestSubscriberRetriesARefusedSubscribe(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse)
	var mu sync.Mutex
	calls := 0

	client := &fakeNotificationsClient{subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
		mu.Lock()
		calls++
		refuse := calls < 3
		mu.Unlock()
		if refuse {
			return nil, status.Error(codes.PermissionDenied, "permission denied")
		}
		return &fakeSubscribeStream{fakeClientStream: fakeClientStream{ctx: ctx}, responses: responses}, nil
	}}

	subscriber := New(client, testPlatformIdentityID)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- subscriber.Run(ctx) }()

	select {
	case responses <- messageEnvelope("message.created"):
	case <-time.After(10 * time.Second):
		t.Fatal("subscriber never established a stream after the refusals")
	}
	select {
	case <-subscriber.Wake():
	case <-time.After(time.Second):
		t.Fatal("expected wake signal")
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

type subscriberHarness struct {
	subscriber    *Subscriber
	cancel        context.CancelFunc
	done          chan error
	responses     chan *notificationsv1.SubscribeResponse
	ack           chan struct{}
	subscribeReqs chan subscribeCall
}

func newSubscriberHarness(t *testing.T, buffer int) *subscriberHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	responses := make(chan *notificationsv1.SubscribeResponse, buffer)
	ack := make(chan struct{}, buffer)
	subscribeReqs := make(chan subscribeCall, 4)
	client := &fakeNotificationsClient{subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
		call := subscribeCall{req: req}
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			if values := md.Get(identityMetadataKey); len(values) > 0 {
				call.identityID = values[0]
			}
			if types := md.Get(identityTypeMetadataKey); len(types) > 0 {
				call.identityType = types[0]
			}
		}
		subscribeReqs <- call
		return &fakeSubscribeStream{
			fakeClientStream: fakeClientStream{ctx: ctx},
			responses:        responses,
			ack:              ack,
		}, nil
	}}
	subscriber := New(client, testPlatformIdentityID)
	done := make(chan error, 1)
	go func() { done <- subscriber.Run(ctx) }()
	return &subscriberHarness{
		subscriber:    subscriber,
		cancel:        cancel,
		done:          done,
		responses:     responses,
		ack:           ack,
		subscribeReqs: subscribeReqs,
	}
}

func (h *subscriberHarness) expectWake(t *testing.T) {
	t.Helper()
	select {
	case <-h.subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}
}

func (h *subscriberHarness) stop(t *testing.T) {
	t.Helper()
	h.cancel()
	if err := <-h.done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func messageEnvelope(event string) *notificationsv1.SubscribeResponse {
	return &notificationsv1.SubscribeResponse{
		Envelope: &notificationsv1.NotificationEnvelope{Event: event},
	}
}

type subscribeCall struct {
	req          *notificationsv1.SubscribeRequest
	identityID   string
	identityType string
}

func waitForSubscribe(t *testing.T, subscribeReqs <-chan subscribeCall) subscribeCall {
	t.Helper()
	select {
	case req := <-subscribeReqs:
		return req
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for subscribe")
	}
	return subscribeCall{}
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
