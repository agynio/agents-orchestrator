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
<<<<<<< HEAD
	messageCreatedEvent               = "message.created"
	agentUpdatedEvent                 = "agent.updated"
	sandboxUpdatedEvent               = "sandbox.updated"
	agentRoomPrefix                   = "agent:"
	threadParticipantRoomPrefix       = "thread_participant:"
	sandboxOrgRoomPrefix              = "sandbox_org:"
	identityMetadataKey               = "x-identity-id"
	listAgentsPageSize          int32 = 100
	defaultRoomRefreshInterval        = 30 * time.Second
=======
	messageCreatedEvent              = "message.created"
	instanceUpdatedEvent             = "instance.updated"
	agentInstanceRoomPrefix          = "agent_instance:"
	instanceInboxRoomPrefix          = "instance_inbox:"
	identityMetadataKey              = "x-identity-id"
	listInstancesPageSize      int32 = 100
	defaultRoomRefreshInterval       = 30 * time.Second
>>>>>>> 368846f (fix(orchestrator): subscribe to instance inbox rooms)
)

type roomSubscription struct {
	identityID uuid.UUID
	rooms      []string
}

type Subscriber struct {
	client              notificationsv1.NotificationsServiceClient
	agents              agentsClient
	wake                chan struct{}
	sandboxWake         chan struct{}
	sandboxOrgIDs       []string
	roomRefreshInterval time.Duration
}

func New(client notificationsv1.NotificationsServiceClient, agents agentsClient) *Subscriber {
	return NewWithSandboxOrganizations(client, agents, nil)
}

// NewWithSandboxOrganizations additionally watches the org-level sandbox rooms
// of organizations that carry no agent, which the agent listing cannot surface.
func NewWithSandboxOrganizations(client notificationsv1.NotificationsServiceClient, agents agentsClient, sandboxOrganizationIDs []string) *Subscriber {
	return &Subscriber{
		client:              client,
		agents:              agents,
		wake:                make(chan struct{}, 1),
		sandboxWake:         make(chan struct{}, 1),
		sandboxOrgIDs:       append([]string(nil), sandboxOrganizationIDs...),
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
			log.Printf("subscriber: agent instance rooms changed, resubscribing")
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
		return fmt.Errorf("subscribe as agent instance %s: %w", subscription.identityID.String(), err)
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
			return fmt.Errorf("stream recv for agent instance %s: %w", subscription.identityID.String(), err)
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
		}
	}
}

func (s *Subscriber) buildRoomSubscriptions(ctx context.Context) ([]roomSubscription, string, error) {
	if s.agents == nil {
		return nil, "", errors.New("agents client not configured")
	}
	roomsByIdentity := map[uuid.UUID]map[string]struct{}{}
	sandboxOrgIdentities := map[string]uuid.UUID{}
	pageToken := ""
	for {
		resp, err := s.agents.ListInstances(ctx, &agentsv1.ListInstancesRequest{
			PageSize:  listInstancesPageSize,
			PageToken: pageToken,
			StateIn:   []agentsv1.AgentInstanceState{agentsv1.AgentInstanceState_AGENT_INSTANCE_STATE_ACTIVE},
		})
		if err != nil {
			return nil, "", fmt.Errorf("list agent instances: %w", err)
		}
		for _, instance := range resp.GetInstances() {
			if instance == nil {
				return nil, "", fmt.Errorf("agent instance is nil")
			}
			meta := instance.GetMeta()
			if meta == nil {
				return nil, "", fmt.Errorf("agent instance meta missing")
			}
			instanceID := strings.TrimSpace(meta.GetId())
			parsedInstanceID, err := uuidutil.ParseUUID(instanceID, "agent_instance.meta.id")
			if err != nil {
				return nil, "", err
			}
			rooms := roomsByIdentity[parsedInstanceID]
			if rooms == nil {
				rooms = map[string]struct{}{}
				roomsByIdentity[parsedInstanceID] = rooms
			}
<<<<<<< HEAD
			agentID = parsedAgentID.String()
			rooms[agentRoomPrefix+agentID] = struct{}{}
			rooms[threadParticipantRoomPrefix+agentID] = struct{}{}
			orgID := strings.TrimSpace(agent.GetOrganizationId())
			if orgID == "" {
				continue
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent.organization_id")
			if err != nil {
				return nil, "", err
			}
			if _, ok := sandboxOrgIdentities[parsedOrgID.String()]; !ok {
				sandboxOrgIdentities[parsedOrgID.String()] = parsedAgentID
			}
=======
			instanceID = parsedInstanceID.String()
			rooms[agentInstanceRoomPrefix+instanceID] = struct{}{}
			rooms[instanceInboxRoomPrefix+instanceID] = struct{}{}
>>>>>>> 368846f (fix(orchestrator): subscribe to instance inbox rooms)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(roomsByIdentity) == 0 {
		return nil, "", fmt.Errorf("no agent instance rooms available")
	}
	if err := s.applySandboxOrgRooms(roomsByIdentity, sandboxOrgIdentities); err != nil {
		return nil, "", err
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

// applySandboxOrgRooms attaches the org-level sandbox room of every organization
// the reconciler covers to one subscription per organization, so sandbox status
// changes wake the sandbox loop without waiting for the next poll tick.
func (s *Subscriber) applySandboxOrgRooms(roomsByIdentity map[uuid.UUID]map[string]struct{}, sandboxOrgIdentities map[string]uuid.UUID) error {
	for _, configuredOrgID := range s.sandboxOrgIDs {
		parsedOrgID, err := uuidutil.ParseUUID(strings.TrimSpace(configuredOrgID), "sandbox_reconcile.organization_id")
		if err != nil {
			return err
		}
		if _, ok := sandboxOrgIdentities[parsedOrgID.String()]; ok {
			continue
		}
		// The organization has no agent to borrow an identity from; the org-level
		// sandbox room is not identity-scoped, so any known identity can watch it.
		sandboxOrgIdentities[parsedOrgID.String()] = lowestIdentity(roomsByIdentity)
	}
	for orgID, identityID := range sandboxOrgIdentities {
		rooms, ok := roomsByIdentity[identityID]
		if !ok {
			continue
		}
		rooms[sandboxOrgRoomPrefix+orgID] = struct{}{}
	}
	return nil
}

func lowestIdentity(roomsByIdentity map[uuid.UUID]map[string]struct{}) uuid.UUID {
	var lowest uuid.UUID
	for identityID := range roomsByIdentity {
		if lowest == uuid.Nil || identityID.String() < lowest.String() {
			lowest = identityID
		}
	}
	return lowest
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
