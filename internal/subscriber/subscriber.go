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
	messageCreatedEvent     = "message.created"
	instanceUpdatedEvent    = "instance.updated"
	sandboxUpdatedEvent     = "sandbox.updated"
	agentInstanceRoomPrefix = "agent_instance:"
	instanceInboxRoomPrefix = "instance_inbox:"
	sandboxOrgRoomPrefix    = "sandbox_org:"
	identityMetadataKey     = "x-identity-id"
	// Notifications settles an instance's own rooms by identity, and reads the
	// type as well: only an agent_instance may subscribe to an instance inbox.
	// The Gateway attaches this for callers that arrive through it; this
	// subscriber reaches Notifications over the mesh and has to say so itself.
	identityTypeMetadataKey          = "x-identity-type"
	agentInstanceIdentityType        = "agent_instance"
	listInstancesPageSize      int32 = 100
	defaultRoomRefreshInterval       = 30 * time.Second
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
	roomRefreshInterval time.Duration
}

func New(client notificationsv1.NotificationsServiceClient, agents agentsClient) *Subscriber {
	return NewWithSandboxOrganizations(client, agents)
}

// NewWithSandboxOrganizations additionally watches the org-level sandbox rooms
// of organizations that carry no agent, which the agent listing cannot surface.
func NewWithSandboxOrganizations(client notificationsv1.NotificationsServiceClient, agents agentsClient) *Subscriber {
	return &Subscriber{
		client:              client,
		agents:              agents,
		wake:                make(chan struct{}, 1),
		sandboxWake:         make(chan struct{}, 1),
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

// runSubscriptions holds every room until the room set changes or the context
// ends. Each subscription reconnects on its own: one instance the platform can
// no longer resolve used to end the whole set, and every wake delivered while
// the rest were being rebuilt was lost.
func (s *Subscriber) runSubscriptions(ctx context.Context, subscriptions []roomSubscription, roomsUpdated <-chan struct{}) error {
	for _, subscription := range subscriptions {
		subscription := subscription
		go s.keepSubscribed(ctx, subscription)
	}

	select {
	case <-roomsUpdated:
		return errRoomsChanged
	case <-ctx.Done():
		return ctx.Err()
	}
}

// keepSubscribed reconnects a single subscription until its context ends.
func (s *Subscriber) keepSubscribed(ctx context.Context, subscription roomSubscription) {
	backoff := time.Second
	for {
		err := s.runSubscription(ctx, subscription)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("subscriber: %v", err)
		}
		if waitWithBackoff(ctx, backoff) != nil {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func (s *Subscriber) runSubscription(ctx context.Context, subscription roomSubscription) error {
	streamCtx := metadata.AppendToOutgoingContext(ctx,
		identityMetadataKey, subscription.identityID.String(),
		identityTypeMetadataKey, agentInstanceIdentityType,
	)
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
			instanceID = parsedInstanceID.String()
			rooms[agentInstanceRoomPrefix+instanceID] = struct{}{}
			rooms[instanceInboxRoomPrefix+instanceID] = struct{}{}
			// Sandbox rooms are per organization, so one instance identity per
			// org is elected to hold the subscription.
			orgID := strings.TrimSpace(instance.GetOrganizationId())
			if orgID == "" {
				continue
			}
			parsedOrgID, err := uuidutil.ParseUUID(orgID, "agent_instance.organization_id")
			if err != nil {
				return nil, "", err
			}
			if _, ok := sandboxOrgIdentities[parsedOrgID.String()]; !ok {
				sandboxOrgIdentities[parsedOrgID.String()] = parsedInstanceID
			}
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(roomsByIdentity) == 0 {
		return nil, "", fmt.Errorf("no agent instance rooms available")
	}
	sandboxOrgRooms, err := s.sandboxOrgRooms(roomsByIdentity, sandboxOrgIdentities)
	if err != nil {
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
	// Kept out of the subscriptions above rather than folded into them.
	// Notifications refuses a whole Subscribe when any one room is
	// unauthorized, and an instance identity cannot hold can_list_sandboxes --
	// it is a member of its organization, and that room wants the owner. Bundled
	// together, the sandbox room took the instance's own inbox down with it and
	// no delivery ever woke the reconciler.
	for _, identityID := range sortedIdentities(sandboxOrgRooms) {
		rooms := sortedRooms(sandboxOrgRooms[identityID])
		subscriptions = append(subscriptions, roomSubscription{
			identityID: identityID,
			rooms:      rooms,
		})
		fingerprints = append(fingerprints, identityID.String()+":"+strings.Join(rooms, ","))
	}
	return subscriptions, strings.Join(fingerprints, "|"), nil
}

func sortedIdentities(roomsByIdentity map[uuid.UUID]map[string]struct{}) []uuid.UUID {
	identityIDs := make([]uuid.UUID, 0, len(roomsByIdentity))
	for identityID := range roomsByIdentity {
		identityIDs = append(identityIDs, identityID)
	}
	sort.Slice(identityIDs, func(i, j int) bool { return identityIDs[i].String() < identityIDs[j].String() })
	return identityIDs
}

// sandboxOrgRooms elects one identity per organization to watch its org-level
// sandbox room, so sandbox status changes wake the sandbox loop without waiting
// for the next poll tick. The result is subscribed separately from the instance
// rooms; see the caller.
func (s *Subscriber) sandboxOrgRooms(roomsByIdentity map[uuid.UUID]map[string]struct{}, sandboxOrgIdentities map[string]uuid.UUID) (map[uuid.UUID]map[string]struct{}, error) {
	roomsByElected := map[uuid.UUID]map[string]struct{}{}
	for orgID, identityID := range sandboxOrgIdentities {
		if _, ok := roomsByIdentity[identityID]; !ok {
			continue
		}
		rooms, ok := roomsByElected[identityID]
		if !ok {
			rooms = map[string]struct{}{}
			roomsByElected[identityID] = rooms
		}
		rooms[sandboxOrgRoomPrefix+orgID] = struct{}{}
	}
	return roomsByElected, nil
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
