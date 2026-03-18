package subscriber

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestSubscriberWakeOnMessageCreated(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	subscriber, cancel, done := newSubscriber(t, responses, ack)
	defer cancel()

	responses <- messageEnvelope("message.created")
	waitForAck(t, ack, 1)

	select {
	case <-subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberIgnoresOtherEvents(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 1)
	ack := make(chan struct{}, 1)
	subscriber, cancel, done := newSubscriber(t, responses, ack)
	defer cancel()

	responses <- messageEnvelope("thread.updated")
	waitForAck(t, ack, 1)

	select {
	case <-subscriber.Wake():
		t.Fatal("unexpected wake signal")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func TestSubscriberCoalescesWake(t *testing.T) {
	responses := make(chan *notificationsv1.SubscribeResponse, 2)
	ack := make(chan struct{}, 2)
	subscriber, cancel, done := newSubscriber(t, responses, ack)
	defer cancel()

	responses <- messageEnvelope("message.created")
	responses <- messageEnvelope("message.created")
	waitForAck(t, ack, 2)

	select {
	case <-subscriber.Wake():
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected wake signal")
	}

	select {
	case <-subscriber.Wake():
		t.Fatal("expected wake to be coalesced")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected run error: %v", err)
	}
}

func newSubscriber(t *testing.T, responses chan *notificationsv1.SubscribeResponse, ack chan struct{}) (*Subscriber, context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := &fakeNotificationsClient{subscribe: func(ctx context.Context, req *notificationsv1.SubscribeRequest, opts ...grpc.CallOption) (notificationsv1.NotificationsService_SubscribeClient, error) {
		return &fakeSubscribeStream{
			fakeClientStream: fakeClientStream{ctx: ctx},
			responses:        responses,
			ack:              ack,
		}, nil
	}}
	subscriber := New(client)
	done := make(chan error, 1)
	go func() {
		done <- subscriber.Run(ctx)
	}()
	return subscriber, cancel, done
}

func messageEnvelope(event string) *notificationsv1.SubscribeResponse {
	return &notificationsv1.SubscribeResponse{
		Envelope: &notificationsv1.NotificationEnvelope{Event: event},
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
