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
)

const (
	messageCreatedEvent               = "message.created"
	agentUpdatedEvent                 = "agent.updated"
	agentRoomPrefix                   = "agent:"
	threadParticipantRoomPrefix       = "thread_participant:"
	listAgentsPageSize          int32 = 100
	defaultRoomRefreshInterval        = 30 * time.Second
)

type roomSnapshot struct {
	rooms       []string
	fingerprint string
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
		snapshot, err := s.buildRoomSnapshot(ctx)
		if err != nil {
			log.Printf("subscriber: build rooms failed: %v", err)
			if err := waitWithBackoff(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff)
			continue
		}
		streamCtx, cancel := context.WithCancel(ctx)
		stream, err := s.client.Subscribe(streamCtx, &notificationsv1.SubscribeRequest{Rooms: snapshot.rooms})
		if err != nil {
			cancel()
			log.Printf("subscriber: subscribe failed: %v", err)
			if err := waitWithBackoff(ctx, backoff); err != nil {
				return err
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = time.Second

		roomsUpdated := make(chan struct{})
		watchCtx, watchCancel := context.WithCancel(streamCtx)
		go s.watchRooms(watchCtx, snapshot, roomsUpdated, cancel)

		for {
			resp, err := stream.Recv()
			if err != nil {
				watchCancel()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				roomsChanged := false
				select {
				case <-roomsUpdated:
					log.Printf("subscriber: agent rooms changed, resubscribing")
					roomsChanged = true
				default:
				}
				if roomsChanged {
					break
				}
				if errors.Is(err, io.EOF) {
					log.Printf("subscriber: stream closed")
				} else {
					log.Printf("subscriber: stream recv failed: %v", err)
				}
				if err := waitWithBackoff(ctx, backoff); err != nil {
					return err
				}
				backoff = nextBackoff(backoff)
				break
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
}

func (s *Subscriber) buildRoomSnapshot(ctx context.Context) (roomSnapshot, error) {
	if s.agents == nil {
		return roomSnapshot{}, errors.New("agents client not configured")
	}
	rooms := make(map[string]struct{})
	pageToken := ""
	for {
		resp, err := s.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  listAgentsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return roomSnapshot{}, fmt.Errorf("list agents: %w", err)
		}
		for _, agent := range resp.GetAgents() {
			if agent == nil {
				return roomSnapshot{}, fmt.Errorf("agent is nil")
			}
			meta := agent.GetMeta()
			if meta == nil {
				return roomSnapshot{}, fmt.Errorf("agent meta missing")
			}
			agentID := strings.TrimSpace(meta.GetId())
			parsedAgentID, err := uuidutil.ParseUUID(agentID, "agent.meta.id")
			if err != nil {
				return roomSnapshot{}, err
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
	if len(rooms) == 0 {
		return roomSnapshot{}, fmt.Errorf("no agent rooms available")
	}
	ordered := make([]string, 0, len(rooms))
	for room := range rooms {
		ordered = append(ordered, room)
	}
	sort.Strings(ordered)
	return roomSnapshot{
		rooms:       ordered,
		fingerprint: strings.Join(ordered, "|"),
	}, nil
}

func (s *Subscriber) watchRooms(ctx context.Context, snapshot roomSnapshot, updated chan<- struct{}, cancel context.CancelFunc) {
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
			next, err := s.buildRoomSnapshot(ctx)
			if err != nil {
				log.Printf("subscriber: refresh rooms failed: %v", err)
				continue
			}
			if next.fingerprint != snapshot.fingerprint {
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
