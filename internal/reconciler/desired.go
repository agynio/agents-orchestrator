package reconciler

import (
	"context"
	"fmt"

	agentsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/agents/v1"
	threadsv1 "github.com/agynio/agents-orchestrator/.gen/go/agynio/api/threads/v1"
	"github.com/agynio/agents-orchestrator/internal/uuidutil"
	"github.com/google/uuid"
)

const desiredPageSize int32 = 100

type AgentThread struct {
	AgentID  uuid.UUID
	ThreadID uuid.UUID
}

func (r *Reconciler) fetchDesired(ctx context.Context) ([]AgentThread, error) {
	agents, err := r.listAgents(ctx)
	if err != nil {
		return nil, err
	}
	unique := make(map[AgentThread]struct{})
	for _, agent := range agents {
		if agent == nil {
			return nil, fmt.Errorf("agent is nil")
		}
		meta := agent.GetMeta()
		if meta == nil {
			return nil, fmt.Errorf("agent meta missing")
		}
		agentID, err := uuidutil.ParseUUID(meta.GetId(), "agent.meta.id")
		if err != nil {
			return nil, err
		}
		threadIDs, err := r.listUnackedThreads(ctx, agentID)
		if err != nil {
			return nil, err
		}
		if len(threadIDs) == 0 {
			continue
		}
		passiveThreads, err := r.fetchPassiveThreads(ctx, agentID)
		if err != nil {
			return nil, err
		}
		for _, threadID := range threadIDs {
			if _, ok := passiveThreads[threadID]; ok {
				continue
			}
			unique[AgentThread{AgentID: agentID, ThreadID: threadID}] = struct{}{}
		}
	}
	result := make([]AgentThread, 0, len(unique))
	for key := range unique {
		result = append(result, key)
	}
	return result, nil
}

func (r *Reconciler) listAgents(ctx context.Context) ([]*agentsv1.Agent, error) {
	resp := []*agentsv1.Agent{}
	token := ""
	for {
		page, err := r.agents.ListAgents(ctx, &agentsv1.ListAgentsRequest{
			PageSize:  desiredPageSize,
			PageToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("list agents: %w", err)
		}
		resp = append(resp, page.GetAgents()...)
		token = page.GetNextPageToken()
		if token == "" {
			return resp, nil
		}
	}
}

func (r *Reconciler) listUnackedThreads(ctx context.Context, agentID uuid.UUID) ([]uuid.UUID, error) {
	threadIDs := make([]uuid.UUID, 0)
	token := ""
	for {
		page, err := r.threads.GetUnackedMessages(ctx, &threadsv1.GetUnackedMessagesRequest{
			ParticipantId: agentID.String(),
			PageSize:      desiredPageSize,
			PageToken:     token,
		})
		if err != nil {
			return nil, fmt.Errorf("get unacked messages for agent %s: %w", agentID.String(), err)
		}
		for _, message := range page.GetMessages() {
			threadID, err := uuidutil.ParseUUID(message.GetThreadId(), "message.thread_id")
			if err != nil {
				return nil, err
			}
			threadIDs = append(threadIDs, threadID)
		}
		token = page.GetNextPageToken()
		if token == "" {
			return threadIDs, nil
		}
	}
}

func (r *Reconciler) fetchPassiveThreads(ctx context.Context, agentID uuid.UUID) (map[uuid.UUID]struct{}, error) {
	passiveThreads := make(map[uuid.UUID]struct{})
	token := ""
	agentIDString := agentID.String()
	for {
		page, err := r.threads.GetThreads(ctx, &threadsv1.GetThreadsRequest{
			ParticipantId: agentIDString,
			PageSize:      desiredPageSize,
			PageToken:     token,
		})
		if err != nil {
			return nil, fmt.Errorf("get threads for agent %s: %w", agentIDString, err)
		}
		for _, thread := range page.GetThreads() {
			if thread == nil {
				return nil, fmt.Errorf("thread is nil")
			}
			threadID, err := uuidutil.ParseUUID(thread.GetId(), "thread.id")
			if err != nil {
				return nil, err
			}
			participant, err := findThreadParticipant(thread, agentID, threadID)
			if err != nil {
				return nil, err
			}
			if participant.GetPassive() {
				passiveThreads[threadID] = struct{}{}
			}
		}
		token = page.GetNextPageToken()
		if token == "" {
			return passiveThreads, nil
		}
	}
}

func findThreadParticipant(thread *threadsv1.Thread, agentID uuid.UUID, threadID uuid.UUID) (*threadsv1.Participant, error) {
	participants := thread.GetParticipants()
	if len(participants) == 0 {
		return nil, fmt.Errorf("thread %s has no participants", threadID.String())
	}
	for _, participant := range participants {
		if participant == nil {
			return nil, fmt.Errorf("thread %s has nil participant", threadID.String())
		}
		participantID, err := uuidutil.ParseUUID(participant.GetId(), "participant.id")
		if err != nil {
			return nil, err
		}
		if participantID == agentID {
			return participant, nil
		}
	}
	return nil, fmt.Errorf("thread %s missing participant %s", threadID.String(), agentID.String())
}
