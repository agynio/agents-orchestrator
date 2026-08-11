package subscriber

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	messageCreatedEvent  = "message.created"
	instanceUpdatedEvent = "instance.updated"
	sandboxUpdatedEvent  = "sandbox.updated"
	// The runner reporting what it saw, rather than the platform having asked.
	workloadStatusChangedEvent = "workload.status_changed"
	identityMetadataKey        = "x-identity-id"
	identityTypeMetadataKey    = "x-identity-type"
	// The Orchestrator subscribes as itself. It reaches Notifications over the
	// mesh rather than through the Gateway, so it states its own identity, and
	// Notifications settles the claim against admin on cluster:global rather
	// than taking the header for it.
	platformIdentityType = "platform"
)

// rooms is the whole subscription, and it is a constant.
//
// It used to be derived: one room per active instance, plus one per
// organization that had a sandbox, rebuilt every thirty seconds from a full
// listing of both. That cannot be made correct. The Orchestrator reconciles
// every instance and sandbox in the cluster, and the event announcing one in an
// organization it had not yet enumerated went to a room nobody held -- these
// are Redis pub/sub, so it was simply dropped, and the sandbox waited for the
// reconcile tick instead. Naming the rooms outright removes the listing, the
// re-derivation, the resubscribe on every change, and the gap.
var rooms = []string{"agent_instances", "sandboxes", "workloads"}

type Subscriber struct {
	client      notificationsv1.NotificationsServiceClient
	identityID  uuid.UUID
	wake        chan struct{}
	sandboxWake chan struct{}
}

// New builds the subscriber. identityID is the platform identity this process
// runs as -- the one Identity registers from configuration and grants cluster
// admin -- and the subscription is made as it.
func New(client notificationsv1.NotificationsServiceClient, identityID uuid.UUID) *Subscriber {
	return &Subscriber{
		client:      client,
		identityID:  identityID,
		wake:        make(chan struct{}, 1),
		sandboxWake: make(chan struct{}, 1),
	}
}

func (s *Subscriber) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := s.runSubscription(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Printf("subscriber: %v", err)
		}
		// Paced whether or not that was an error: a stream closed cleanly the
		// moment it opens is still a reconnect loop, and reconnecting with no
		// delay at all would spin on it.
		if waitErr := waitWithBackoff(ctx, backoff); waitErr != nil {
			return waitErr
		}
		if err != nil {
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second
	}
}

func (s *Subscriber) runSubscription(ctx context.Context) error {
	streamCtx := metadata.AppendToOutgoingContext(ctx,
		identityMetadataKey, s.identityID.String(),
		identityTypeMetadataKey, platformIdentityType,
	)
	stream, err := s.client.Subscribe(streamCtx, &notificationsv1.SubscribeRequest{Rooms: rooms})
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("subscriber: stream closed")
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("stream recv: %w", err)
		}
		envelope := resp.GetEnvelope()
		if envelope == nil {
			continue
		}
		switch envelope.GetEvent() {
		case messageCreatedEvent, instanceUpdatedEvent:
			select {
			case s.wake <- struct{}{}:
			default:
			}
		case sandboxUpdatedEvent:
			select {
			case s.sandboxWake <- struct{}{}:
			default:
			}
		// A workload backs an agent instance or a sandbox, and the event does
		// not say which, so both loops are woken. Each is a cheap no-op when the
		// workload was not its own.
		case workloadStatusChangedEvent:
			select {
			case s.wake <- struct{}{}:
			default:
			}
			select {
			case s.sandboxWake <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Subscriber) Wake() <-chan struct{} {
	return s.wake
}

// SandboxWake signals sandbox.updated events so the sandbox reconcile loop can
// react to connect/stop/delete without waiting for its poll interval.
func (s *Subscriber) SandboxWake() <-chan struct{} {
	return s.sandboxWake
}

func waitWithBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return time.Second
	}
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}
