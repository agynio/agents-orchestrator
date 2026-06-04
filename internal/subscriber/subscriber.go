package subscriber

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
)

const (
	messageCreatedEvent               = "message.created"
	agentUpdatedEvent                 = "agent.updated"
	agentRoomPrefix                   = "agent:"
	threadParticipantRoomPrefix       = "thread_participant:"
	identityMetadataKey               = "x-identity-id"
	listAgentsPageSize          int32 = 100
	defaultRoomRefreshInterval        = 30 * time.Second
)

type roomSubscription struct {
	identityID uuid.UUID
	rooms      []string
}

type Subscriber struct {
	client              notificationsv1.NotificationsServiceClient
	agents              agentsv1.AgentsServiceClient
	wake                chan struct{}
	roomRefreshInterval time.Duration
}

func New(client notificationsv1.NotificationsServiceClient, agents agentsv1.AgentsServiceClient) *Subscriber {
	return &Subscriber{
		client:              client,
		agents:              agents,
		wake:                make(chan struct{}, 1),
		roomRefreshInterval: defaultRoomRefreshInterval,
	}
}

func (s *Subscriber) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		subscriptions, fingerprint, err := s.buildRoomSubscriptions(ctx)
		if err != nil {
			log.Printf("subscriber: build rooms failed: %v", err)
			if err := waitWithBackoff(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff)
			continue
		}

		runCtx, cancel := context.WithCancel(ctx)
		roomsUpdated := make(chan struct{})
		go s.watchRooms(runCtx, fingerprint, roomsUpdated, cancel)

		err = s.runSubscriptions(runCtx, subscriptions, roomsUpdated)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errRoomsChanged) {
			log.Printf("subscriber: agent rooms changed, resubscribing")
			backoff = time.Second
			continue
		}
		if err != nil {
			log.Printf("subscriber: subscriptions failed: %v", err)
			if err := waitWithBackoff(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second
	}
}

var errRoomsChanged = errors.New("rooms changed")

func (s *Subscriber) runSubscriptions(ctx context.Context, subscriptions []roomSubscription, roomsUpdated <-chan struct{}) error {
	errCh := make(chan error, len(subscriptions))
	for _, subscription := range subscriptions {
		subscription := subscription
		go func() {
			errCh <- s.runSubscription(ctx, subscription)
		}()
	}

	remaining := len(subscriptions)
	for remaining > 0 {
		select {
		case <-roomsUpdated:
			return errRoomsChanged
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			remaining--
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Subscriber) runSubscription(ctx context.Context, subscription roomSubscription) error {
	streamCtx := metadata.AppendToOutgoingContext(ctx, identityMetadataKey, subscription.identityID.String())
	stream, err := s.client.Subscribe(streamCtx, &notificationsv1.SubscribeRequest{Rooms: subscription.rooms})
	if err != nil {
		return fmt.Errorf("subscribe as agent %s: %w", subscription.identityID.String(), err)
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
			return fmt.Errorf("stream recv for agent %s: %w", subscription.identityID.String(), err)
		}
		envelope := resp.GetEnvelope()
		if envelope == nil {
			continue
		}
		switch envelope.GetEvent() {
		case messageCreatedEvent, agentUpdatedEvent:
			select {
			case s.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Subscriber) buildRoomSubscriptions(ctx context.Context) ([]roomSubscription, string, error) {
	if s.agents == nil {
		return nil, "", errors.New("agents client not configured")
	}
	roomsByIdentity := map[uuid.UUID]map[string]struct{}{}
	pageToken := ""
	for {
		resp, err := s.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  listAgentsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return nil, "", fmt.Errorf("list agents: %w", err)
		}
		for _, agent := range resp.GetAgents() {
			if agent == nil {
				return nil, "", fmt.Errorf("agent is nil")
			}
			meta := agent.GetMeta()
			if meta == nil {
				return nil, "", fmt.Errorf("agent meta missing")
			}
			agentID := strings.TrimSpace(meta.GetId())
			parsedAgentID, err := uuidutil.ParseUUID(agentID, "agent.meta.id")
			if err != nil {
				return nil, "", err
			}
			rooms := roomsByIdentity[parsedAgentID]
			if rooms == nil {
				rooms = map[string]struct{}{}
				roomsByIdentity[parsedAgentID] = rooms
			}
			agentID = parsedAgentID.String()
			rooms[agentRoomPrefix+agentID] = struct{}{}
			rooms[threadParticipantRoomPrefix+agentID] = struct{}{}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(roomsByIdentity) == 0 {
		return nil, "", fmt.Errorf("no agent rooms available")
	}

	identityIDs := make([]uuid.UUID, 0, len(roomsByIdentity))
	for identityID := range roomsByIdentity {
		identityIDs = append(identityIDs, identityID)
	}
	sort.Slice(identityIDs, func(i, j int) bool { return identityIDs[i].String() < identityIDs[j].String() })

	subscriptions := make([]roomSubscription, 0, len(identityIDs))
	fingerprints := make([]string, 0, len(identityIDs))
	for _, identityID := range identityIDs {
		rooms := sortedRooms(roomsByIdentity[identityID])
		fingerprint := identityID.String() + ":" + strings.Join(rooms, ",")
		subscriptions = append(subscriptions, roomSubscription{
			identityID: identityID,
			rooms:      rooms,
		})
		fingerprints = append(fingerprints, fingerprint)
	}
	return subscriptions, strings.Join(fingerprints, "|"), nil
}

func sortedRooms(rooms map[string]struct{}) []string {
	ordered := make([]string, 0, len(rooms))
	for room := range rooms {
		ordered = append(ordered, room)
	}
	sort.Strings(ordered)
	return ordered
}

func (s *Subscriber) watchRooms(ctx context.Context, fingerprint string, updated chan<- struct{}, cancel context.CancelFunc) {
	if s.roomRefreshInterval <= 0 {
		return
	}
	ticker := time.NewTicker(s.roomRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, nextFingerprint, err := s.buildRoomSubscriptions(ctx)
			if err != nil {
				log.Printf("subscriber: refresh rooms failed: %v", err)
				continue
			}
			if nextFingerprint != fingerprint {
				close(updated)
				cancel()
				return
			}
		}
	}
}

func (s *Subscriber) Wake() <-chan struct{} {
	return s.wake
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
