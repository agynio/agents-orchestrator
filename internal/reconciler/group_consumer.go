package reconciler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

const groupMembershipConsumerName = "agents-orchestrator-group-sync"

type groupMembershipSubscription interface {
	Unsubscribe() error
}

type groupMembershipSubscriber func(context.Context) (groupMembershipSubscription, error)

func (r *Reconciler) StartGroupMembershipConsumerLoop(ctx context.Context, natsURL string) {
	r.StartGroupMembershipConsumerLoopWithSubscriber(ctx, func(ctx context.Context) (groupMembershipSubscription, error) {
		return r.subscribeGroupMembershipEvents(ctx, natsURL)
	})
}

func (r *Reconciler) StartGroupMembershipConsumerLoopWithSubscriber(ctx context.Context, subscribe groupMembershipSubscriber) {
	go func() {
		backoff := groupMembershipRetryInitial
		for {
			if ctx.Err() != nil {
				return
			}
			subscription, err := subscribe(ctx)
			if err != nil {
				log.Printf("reconciler: group membership consumer subscribe failed: %v", err)
				if !sleepWithContext(ctx, backoff) {
					return
				}
				backoff *= 2
				if backoff > groupMembershipRetryMax {
					backoff = groupMembershipRetryMax
				}
				continue
			}
			backoff = groupMembershipRetryInitial
			<-ctx.Done()
			if subscription != nil {
				if err := subscription.Unsubscribe(); err != nil {
					log.Printf("reconciler: group membership consumer unsubscribe failed: %v", err)
				}
			}
			return
		}
	}()
}

func (r *Reconciler) subscribeGroupMembershipEvents(ctx context.Context, natsURL string) (groupMembershipSubscription, error) {
	if natsURL == "" {
		return nil, errors.New("nats url is empty")
	}
	conn, err := nats.Connect(natsURL)
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	subscription, err := js.QueueSubscribe(
		"agyn.groups.membership.*",
		groupMembershipConsumerName,
		func(msg *nats.Msg) {
			if err := r.HandleGroupMembershipEvent(ctx, msg.Subject, msg.Data); err != nil {
				log.Printf("reconciler: group membership event failed subject=%s: %v", msg.Subject, err)
				_ = msg.Nak()
				return
			}
			_ = msg.Ack()
		},
		nats.Durable(groupMembershipConsumerName),
		nats.ManualAck(),
		nats.AckExplicit(),
	)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("subscribe group membership events: %w", err)
	}
	return &natsGroupMembershipSubscription{conn: conn, sub: subscription}, nil
}

type natsGroupMembershipSubscription struct {
	conn *nats.Conn
	sub  *nats.Subscription
}

func (s *natsGroupMembershipSubscription) Unsubscribe() error {
	var err error
	if s.sub != nil {
		err = s.sub.Unsubscribe()
	}
	if s.conn != nil {
		s.conn.Close()
	}
	return err
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
