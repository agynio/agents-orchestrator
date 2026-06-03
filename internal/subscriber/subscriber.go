package subscriber

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	notificationsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/notifications/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	messageCreatedEvent               = "message.created"
	agentUpdatedEvent                 = "agent.updated"
	agentRoomPrefix                   = "agent:"
	threadParticipantRoomPrefix       = "thread_participant:"
	listAgentsPageSize          int32 = 100
	defaultRoomRefreshInterval        = 30 * time.Second
	streamErrorSummaryInterval        = time.Minute
	roomSnapshotSampleSize            = 3
	agentsServiceName                 = "agents"
	notificationsServiceName          = "notifications"
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
	notificationsTarget string
	agentsTarget        string
	streamErrors        *repeatedErrorLimiter
}

func New(client notificationsv1.NotificationsServiceClient, agents agentsv1.AgentsServiceClient) *Subscriber {
	return &Subscriber{
		client:              client,
		agents:              agents,
		wake:                make(chan struct{}, 1),
		roomRefreshInterval: defaultRoomRefreshInterval,
		streamErrors:        newRepeatedErrorLimiter(streamErrorSummaryInterval),
	}
}

func (s *Subscriber) Wake() <-chan struct{} {
	return s.wake
}

func (s *Subscriber) SetServiceTargets(notificationsTarget, agentsTarget string) {
	s.notificationsTarget = strings.TrimSpace(notificationsTarget)
	s.agentsTarget = strings.TrimSpace(agentsTarget)
}

func (s *Subscriber) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		snapshot, err := s.buildRoomSnapshot(ctx)
		if err != nil {
			log.Printf(
				"subscriber: rooms build failed phase=rooms.build %s backoff=%s err=%v",
				formatServiceTarget(agentsServiceName, s.agentsTarget),
				backoff,
				err,
			)
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
			s.logStreamError("Subscribe", snapshot, backoff, err)
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
					log.Printf(
						"subscriber: rooms changed, resubscribing phase=rooms.refresh %s previous_%s",
						formatServiceTarget(agentsServiceName, s.agentsTarget),
						formatRoomSnapshot(snapshot),
					)
					roomsChanged = true
				default:
				}
				if roomsChanged {
					break
				}
				if errors.Is(err, io.EOF) {
					log.Printf(
						"subscriber: stream closed phase=Recv %s %s backoff=%s",
						formatServiceTarget(notificationsServiceName, s.notificationsTarget),
						formatRoomSnapshot(snapshot),
						backoff,
					)
				} else {
					s.logStreamError("Recv", snapshot, backoff, err)
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
		fingerprint: fingerprintRooms(ordered),
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
				log.Printf(
					"subscriber: rooms refresh failed phase=rooms.refresh %s previous_%s err=%v",
					formatServiceTarget(agentsServiceName, s.agentsTarget),
					formatRoomSnapshot(snapshot),
					err,
				)
				continue
			}
			if next.fingerprint != snapshot.fingerprint {
				log.Printf(
					"subscriber: rooms refresh detected change phase=rooms.refresh %s previous_%s current_%s",
					formatServiceTarget(agentsServiceName, s.agentsTarget),
					formatRoomSnapshot(snapshot),
					formatRoomSnapshot(next),
				)
				close(updated)
				cancel()
				return
			}
		}
	}
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

func (s *Subscriber) logStreamError(phase string, snapshot roomSnapshot, backoff time.Duration, err error) {
	code, desc := grpcErrorDetails(err)
	key := strings.Join([]string{phase, notificationsServiceName, s.notificationsTarget, snapshot.fingerprint, code, desc}, "\x00")
	decision := s.streamErrors.Record(key)
	if decision.First {
		log.Printf(
			"subscriber: stream error phase=%s %s %s grpc_code=%s grpc_desc=%q backoff=%s err=%v",
			phase,
			formatServiceTarget(notificationsServiceName, s.notificationsTarget),
			formatRoomSnapshot(snapshot),
			code,
			desc,
			backoff,
			err,
		)
		return
	}
	if decision.Summary {
		log.Printf(
			"subscriber: stream error repeated phase=%s %s %s grpc_code=%s grpc_desc=%q backoff=%s suppressed=%d",
			phase,
			formatServiceTarget(notificationsServiceName, s.notificationsTarget),
			formatRoomSnapshot(snapshot),
			code,
			desc,
			backoff,
			decision.Suppressed,
		)
	}
}

func grpcErrorDetails(err error) (string, string) {
	statusErr, ok := status.FromError(err)
	if !ok {
		return "NonGRPC", err.Error()
	}
	code := statusErr.Code()
	if code == codes.OK {
		return "NonGRPC", err.Error()
	}
	return code.String(), statusErr.Message()
}

func formatServiceTarget(serviceName, target string) string {
	if target == "" {
		return fmt.Sprintf("service=%s target=unknown", serviceName)
	}
	return fmt.Sprintf("service=%s target=%s", serviceName, target)
}

func formatRoomSnapshot(snapshot roomSnapshot) string {
	agentRooms := 0
	threadParticipantRooms := 0
	for _, room := range snapshot.rooms {
		switch {
		case strings.HasPrefix(room, agentRoomPrefix):
			agentRooms++
		case strings.HasPrefix(room, threadParticipantRoomPrefix):
			threadParticipantRooms++
		}
	}
	otherRooms := len(snapshot.rooms) - agentRooms - threadParticipantRooms
	sampleLimit := roomSnapshotSampleSize
	if len(snapshot.rooms) < sampleLimit {
		sampleLimit = len(snapshot.rooms)
	}
	return fmt.Sprintf(
		"rooms_count=%d agent_rooms=%d thread_participant_rooms=%d other_rooms=%d fingerprint=%s sample=%q",
		len(snapshot.rooms),
		agentRooms,
		threadParticipantRooms,
		otherRooms,
		snapshot.fingerprint,
		snapshot.rooms[:sampleLimit],
	)
}

func fingerprintRooms(rooms []string) string {
	hash := sha256.New()
	for _, room := range rooms {
		_, _ = hash.Write([]byte(room))
		_, _ = hash.Write([]byte{0})
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

type repeatedErrorLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	now      func() time.Time
	entries  map[string]repeatedErrorEntry
}

type repeatedErrorEntry struct {
	lastLogged time.Time
	suppressed int
}

type repeatedErrorDecision struct {
	First      bool
	Summary    bool
	Suppressed int
}

func newRepeatedErrorLimiter(interval time.Duration) *repeatedErrorLimiter {
	return &repeatedErrorLimiter{
		interval: interval,
		now:      time.Now,
		entries:  make(map[string]repeatedErrorEntry),
	}
}

func (l *repeatedErrorLimiter) Record(key string) repeatedErrorDecision {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	entry, ok := l.entries[key]
	if !ok {
		l.entries[key] = repeatedErrorEntry{lastLogged: now}
		return repeatedErrorDecision{First: true}
	}
	entry.suppressed++
	if now.Sub(entry.lastLogged) >= l.interval {
		decision := repeatedErrorDecision{Summary: true, Suppressed: entry.suppressed}
		entry.lastLogged = now
		entry.suppressed = 0
		l.entries[key] = entry
		return decision
	}
	l.entries[key] = entry
	return repeatedErrorDecision{}
}
