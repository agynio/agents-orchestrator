const fs = require('fs');

function replace(path, marker, original, replacement) {
  const text = fs.readFileSync(path, 'utf8');
  if (text.includes(marker)) {
    return;
  }
  if (!text.includes(original)) {
    throw new Error(`${path}: expected block not found`);
  }
  fs.writeFileSync(path, text.replace(original, replacement));
}

replace(
  'internal/config/config.go',
  'AgentInstanceID uuid.UUID',
  [
    'type Config struct {',
    '\tMode           string',
    '\tAgentID        uuid.UUID',
    '\tGatewayAddress string',
    '\tTracingAddress string',
    '\tThreadID       string',
  ].join('\n'),
  [
    'type Config struct {',
    '\tMode            string',
    '\tAgentID         uuid.UUID',
    '\tAgentInstanceID uuid.UUID',
    '\tGatewayAddress  string',
    '\tTracingAddress  string',
    '\tThreadID        string',
  ].join('\n'),
);
replace(
  'internal/config/config.go',
  'AGENT_INSTANCE_ID',
  [
    '\tthreadID := strings.TrimSpace(os.Getenv("THREAD_ID"))',
    '\tthreadUUID, err := uuidutil.ParseUUID(threadID, "THREAD_ID")',
    '\tif err != nil {',
    '\t\treturn Config{}, err',
    '\t}',
    '\tthreadID = threadUUID.String()',
    '\tworkloadID := strings.TrimSpace(os.Getenv("WORKLOAD_ID"))',
  ].join('\n'),
  [
    '\tagentInstanceID, err := uuidutil.ParseUUID(strings.TrimSpace(os.Getenv("AGENT_INSTANCE_ID")), "AGENT_INSTANCE_ID")',
    '\tif err != nil {',
    '\t\treturn Config{}, err',
    '\t}',
    '\tthreadID := strings.TrimSpace(os.Getenv("THREAD_ID"))',
    '\tif threadID != "" {',
    '\t\tthreadUUID, err := uuidutil.ParseUUID(threadID, "THREAD_ID")',
    '\t\tif err != nil {',
    '\t\t\treturn Config{}, err',
    '\t\t}',
    '\t\tthreadID = threadUUID.String()',
    '\t}',
    '\tworkloadID := strings.TrimSpace(os.Getenv("WORKLOAD_ID"))',
  ].join('\n'),
);
replace(
  'internal/config/config.go',
  'AgentInstanceID: agentInstanceID',
  [
    '\t\tMode:           mode,',
    '\t\tAgentID:        agentID,',
    '\t\tGatewayAddress: gatewayAddress,',
    '\t\tTracingAddress: tracingAddress,',
    '\t\tThreadID:       threadID,',
  ].join('\n'),
  [
    '\t\tMode:            mode,',
    '\t\tAgentID:         agentID,',
    '\t\tAgentInstanceID: agentInstanceID,',
    '\t\tGatewayAddress:  gatewayAddress,',
    '\t\tTracingAddress:  tracingAddress,',
    '\t\tThreadID:        threadID,',
  ].join('\n'),
);

replace(
  'internal/platform/threads.go',
  'InboxItemID string',
  ['type Message struct {', '\tID        string'].join('\n'),
  ['type Message struct {', '\tID          string', '\tInboxItemID string'].join('\n'),
);
replace(
  'internal/platform/consumer.go',
  'agentsInboxClient',
  ['type Consumer struct {', '\tthreads        *Threads'].join('\n'),
  ['type Consumer struct {', '\tthreads        *Threads', '\tagents         agentsInboxClient'].join('\n'),
);
replace(
  'internal/platform/consumer.go',
  'type agentsInboxClient interface',
  'type PageFetchError struct {',
  [
    'type agentsInboxClient interface {',
    '\tGetUnackedInboxItems(ctx context.Context, agentInstanceID string, pageSize int32, pageToken string) ([]Message, string, error)',
    '}',
    '',
    'type PageFetchError struct {',
  ].join('\n'),
);
replace(
  'internal/platform/consumer.go',
  'func NewInboxConsumer',
  [
    'func NewConsumer(threads *Threads, pageSize int32, requestTimeout time.Duration) *Consumer {',
    '\treturn &Consumer{threads: threads, pageSize: pageSize, requestTimeout: requestTimeout}',
    '}',
  ].join('\n'),
  [
    'func NewConsumer(threads *Threads, pageSize int32, requestTimeout time.Duration) *Consumer {',
    '\treturn &Consumer{threads: threads, pageSize: pageSize, requestTimeout: requestTimeout}',
    '}',
    '',
    'func NewInboxConsumer(agents agentsInboxClient, pageSize int32, requestTimeout time.Duration) *Consumer {',
    '\treturn &Consumer{agents: agents, pageSize: pageSize, requestTimeout: requestTimeout}',
    '}',
  ].join('\n'),
);
replace(
  'internal/platform/consumer.go',
  'c.getUnackedMessages',
  '\t\tmessages, nextToken, err := c.threads.GetUnackedMessages(pageCtx, participantID, threadID, c.pageSize, pageToken)',
  '\t\tmessages, nextToken, err := c.getUnackedMessages(pageCtx, participantID, threadID, pageToken)',
);
const consumerPath = 'internal/platform/consumer.go';
let consumerText = fs.readFileSync(consumerPath, 'utf8');
if (!consumerText.includes('func (c *Consumer) getUnackedMessages')) {
  consumerText += [
    '',
    'func (c *Consumer) getUnackedMessages(ctx context.Context, participantID string, threadID string, pageToken string) ([]Message, string, error) {',
    '\tif c.agents != nil {',
    '\t\treturn c.agents.GetUnackedInboxItems(ctx, participantID, c.pageSize, pageToken)',
    '\t}',
    '\treturn c.threads.GetUnackedMessages(ctx, participantID, threadID, c.pageSize, pageToken)',
    '}',
  ].join('\n');
  fs.writeFileSync(consumerPath, consumerText);
}

fs.writeFileSync('internal/platform/agents.go', `package platform

import (
\t"context"
\t"fmt"

\tagentsv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/agents/v1"
\tgatewayv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/gateway/v1"
)

type Agents struct {
\tclient gatewayv1.AgentsGatewayClient
}

func NewAgents(client gatewayv1.AgentsGatewayClient) *Agents {
\treturn &Agents{client: client}
}

func (a *Agents) GetUnackedInboxItems(ctx context.Context, agentInstanceID string, pageSize int32, pageToken string) ([]Message, string, error) {
\tif agentInstanceID == "" {
\t\treturn nil, "", fmt.Errorf("agent instance id is required")
\t}
\tresp, err := a.client.GetUnackedInboxItems(ctx, &agentsv1.GetUnackedInboxItemsRequest{
\t\tAgentInstanceId: agentInstanceID,
\t\tPageSize:        pageSize,
\t\tPageToken:       pageToken,
\t})
\tif err != nil {
\t\treturn nil, "", fmt.Errorf("get unacked inbox items: %w", err)
\t}
\tmessages := make([]Message, 0, len(resp.GetItems()))
\tfor _, item := range resp.GetItems() {
\t\tparsed, err := inboxItemFromProto(item)
\t\tif err != nil {
\t\t\treturn nil, "", err
\t\t}
\t\tmessages = append(messages, parsed)
\t}
\treturn messages, resp.GetNextPageToken(), nil
}

func (a *Agents) AckInboxItems(ctx context.Context, agentInstanceID string, itemIDs []string) error {
\tif agentInstanceID == "" {
\t\treturn fmt.Errorf("agent instance id is required")
\t}
\tif len(itemIDs) == 0 {
\t\treturn fmt.Errorf("item ids are required")
\t}
\tfor _, id := range itemIDs {
\t\tif id == "" {
\t\t\treturn fmt.Errorf("item id is required")
\t\t}
\t}
\t_, err := a.client.AckInboxItems(ctx, &agentsv1.AckInboxItemsRequest{
\t\tAgentInstanceId: agentInstanceID,
\t\tItemIds:         append([]string{}, itemIDs...),
\t})
\tif err != nil {
\t\treturn fmt.Errorf("ack inbox items: %w", err)
\t}
\treturn nil
}

func inboxItemFromProto(item *agentsv1.InboxItem) (Message, error) {
\tif item == nil {
\t\treturn Message{}, fmt.Errorf("inbox item is nil")
\t}
\tid := item.GetId()
\tif id == "" {
\t\treturn Message{}, fmt.Errorf("inbox item.id is required")
\t}
\tmessageID := item.GetMessageId()
\tif messageID == "" {
\t\tmessageID = id
\t}
\tthreadID := item.GetThreadId()
\tif threadID == "" {
\t\treturn Message{}, fmt.Errorf("inbox item.thread_id is required")
\t}
\tsenderID := item.GetSenderId()
\tif senderID == "" {
\t\treturn Message{}, fmt.Errorf("inbox item.sender_id is required")
\t}
\tcreatedAt := item.GetAcceptedAt()
\tif createdAt == nil {
\t\treturn Message{}, fmt.Errorf("inbox item.accepted_at is required")
\t}
\tfileIDs := append([]string{}, item.GetFileIds()...)
\tif item.GetBody() == "" && len(fileIDs) == 0 {
\t\treturn Message{}, fmt.Errorf("inbox item body or file ids are required")
\t}
\treturn Message{
\t\tID:          messageID,
\t\tInboxItemID: id,
\t\tThreadID:    threadID,
\t\tSenderID:    senderID,
\t\tBody:        item.GetBody(),
\t\tFileIDs:     fileIDs,
\t\tCreatedAt:   createdAt.AsTime(),
\t}, nil
}
`);

replace(
  'internal/platform/notifications.go',
  'agentInstanceSelfRoom',
  'const threadParticipantSelfRoom = "thread_participant:me"',
  ['const threadParticipantSelfRoom = "thread_participant:me"', 'const agentInstanceSelfRoom = "agent_instance:me"'].join('\n'),
);
replace(
  'internal/platform/notifications.go',
  '[]string{threadParticipantSelfRoom, agentInstanceSelfRoom}',
  ['\trequest := &notificationsv1.SubscribeRequest{', '\t\tRooms: []string{threadParticipantSelfRoom},', '\t}'].join('\n'),
  ['\trequest := &notificationsv1.SubscribeRequest{', '\t\tRooms: []string{threadParticipantSelfRoom, agentInstanceSelfRoom},', '\t}'].join('\n'),
);
replace(
  'internal/subscriber/subscriber.go',
  's.threadID != "" && (!ok || payloadThreadID != s.threadID)',
  ['\t\t\tif !ok || payloadThreadID != s.threadID {', '\t\t\t\tcontinue', '\t\t\t}'].join('\n'),
  ['\t\t\tif s.threadID != "" && (!ok || payloadThreadID != s.threadID) {', '\t\t\t\tcontinue', '\t\t\t}'].join('\n'),
);

replace(
  'internal/tracingproxy/proxy.go',
  'threadMu',
  ['\tthreadID   string', '\tworkloadID string', '\tmessageMu  sync.RWMutex'].join('\n'),
  ['\tthreadMu   sync.RWMutex', '\tthreadID   string', '\tworkloadID string', '\tmessageMu  sync.RWMutex'].join('\n'),
);
replace(
  'internal/tracingproxy/proxy.go',
  'threadID := p.threadIDValue()',
  ['\tif p.threadID != "" {', '\t\tinjectThreadID(req, p.threadID)', '\t}'].join('\n'),
  ['\tif threadID := p.threadIDValue(); threadID != "" {', '\t\tinjectThreadID(req, threadID)', '\t}'].join('\n'),
);
replace(
  'internal/tracingproxy/proxy.go',
  'func (p *Proxy) SetThreadID',
  ['func (p *Proxy) SetMessageID(messageID string) {', '\tp.messageMu.Lock()', '\tp.messageID = messageID', '\tp.messageMu.Unlock()', '}'].join('\n'),
  [
    'func (p *Proxy) SetThreadID(threadID string) {',
    '\tp.threadMu.Lock()',
    '\tp.threadID = threadID',
    '\tp.threadMu.Unlock()',
    '}',
    '',
    'func (p *Proxy) threadIDValue() string {',
    '\tp.threadMu.RLock()',
    '\tdefer p.threadMu.RUnlock()',
    '\treturn p.threadID',
    '}',
    '',
    'func (p *Proxy) SetMessageID(messageID string) {',
    '\tp.messageMu.Lock()',
    '\tp.messageID = messageID',
    '\tp.messageMu.Unlock()',
    '}',
  ].join('\n'),
);

replace(
  'internal/daemon/daemon.go',
  '"github.com/google/uuid"',
  '\tcodex "github.com/agynio/codex-sdk-go"',
  ['\tcodex "github.com/agynio/codex-sdk-go"', '\t"github.com/google/uuid"'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'agentInbox    *platform.Agents',
  ['\tthreads       *platform.Threads', '\tagents        gatewayv1.AgentsGatewayClient', '\trunners       runnersClient'].join('\n'),
  ['\tthreads       *platform.Threads', '\tagents        gatewayv1.AgentsGatewayClient', '\tagentInbox    *platform.Agents', '\trunners       runnersClient'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'agentInbox    *platform.Agents\n\trunners       *platform.Runners',
  ['\tthreads       *platform.Threads', '\tnotifications *platform.Notifications', '\tagents        gatewayv1.AgentsGatewayClient', '\trunners       *platform.Runners'].join('\n'),
  ['\tthreads       *platform.Threads', '\tnotifications *platform.Notifications', '\tagents        gatewayv1.AgentsGatewayClient', '\tagentInbox    *platform.Agents', '\trunners       *platform.Runners'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'agentInboxClient := platform.NewAgents',
  ['\tthreadsClient := platform.NewThreads(threadsGateway)', '\tnotificationsClient := platform.NewNotifications(notificationsGateway)', '\trunnersClient := platform.NewRunners(runnersGateway)'].join('\n'),
  ['\tthreadsClient := platform.NewThreads(threadsGateway)', '\tnotificationsClient := platform.NewNotifications(notificationsGateway)', '\tagentInboxClient := platform.NewAgents(agentsClient)', '\trunnersClient := platform.NewRunners(runnersGateway)'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'agentInbox:    agentInboxClient',
  ['\t\tnotifications: notificationsClient,', '\t\tagents:        agentsClient,', '\t\trunners:       runnersClient,'].join('\n'),
  ['\t\tnotifications: notificationsClient,', '\t\tagents:        agentsClient,', '\t\tagentInbox:    agentInboxClient,', '\t\trunners:       runnersClient,'].join('\n'),
);
for (const path of ['internal/daemon/daemon.go', 'internal/daemon/agn.go', 'internal/daemon/claude.go']) {
  replace(
    path,
    'platform.NewInboxConsumer',
    ['\t\tsubscriber:   subscriber.New(setup.notifications, cfg.ThreadID),', '\t\tconsumer:     platform.NewConsumer(setup.threads, pageSize, pageTimeout),'].join('\n'),
    ['\t\tsubscriber:   subscriber.New(setup.notifications, cfg.ThreadID),', '\t\tconsumer:     platform.NewInboxConsumer(setup.agentInbox, pageSize, pageTimeout),'].join('\n'),
  );
}
replace(
  'internal/daemon/daemon.go',
  'participantID := d.cfg.AgentID.String()',
  ['\tif err := d.consumer.Sync(ctx, d.cfg.AgentID.String(), d.cfg.ThreadID, func(message platform.Message) error {', '\t\tif d.tracingProxy != nil {', '\t\t\td.tracingProxy.SetMessageID(message.ID)', '\t\t}'].join('\n'),
  ['\tparticipantID := d.cfg.AgentID.String()', '\tif d.cfg.AgentInstanceID != uuid.Nil {', '\t\tparticipantID = d.cfg.AgentInstanceID.String()', '\t}', '\tif err := d.consumer.Sync(ctx, participantID, d.cfg.ThreadID, func(message platform.Message) error {', '\t\tif d.tracingProxy != nil {', '\t\t\td.tracingProxy.SetThreadID(message.ThreadID)', '\t\t\td.tracingProxy.SetMessageID(message.ID)', '\t\t}'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'participantID, d.cfg.ThreadID, pageFetchErr',
  'fmt.Errorf("sync unacked messages for participant %s thread %s: %w", d.cfg.AgentID.String(), d.cfg.ThreadID, pageFetchErr)',
  'fmt.Errorf("sync unacked messages for participant %s thread %s: %w", participantID, d.cfg.ThreadID, pageFetchErr)',
);
replace(
  'internal/daemon/daemon.go',
  'senderID := d.cfg.AgentID.String()',
  ['\tpublishCtx, cancel := context.WithTimeout(ctx, messagePublishTimeout)', '\t_, err := d.threads.SendMessage(publishCtx, threadID, d.cfg.AgentID.String(), response, nil)'].join('\n'),
  ['\tpublishCtx, cancel := context.WithTimeout(ctx, messagePublishTimeout)', '\tsenderID := d.cfg.AgentID.String()', '\tif d.cfg.AgentInstanceID != uuid.Nil {', '\t\tsenderID = d.cfg.AgentInstanceID.String()', '\t}', '\t_, err := d.threads.SendMessage(publishCtx, threadID, senderID, response, nil)'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'message.InboxItemID != ""',
  ['\tackCtx, cancel := context.WithTimeout(ctx, messageAckTimeout)', '\terr := d.threads.AckMessages(ackCtx, d.cfg.AgentID.String(), []string{message.ID})'].join('\n'),
  ['\tackCtx, cancel := context.WithTimeout(ctx, messageAckTimeout)', '\tvar err error', '\tif message.InboxItemID != "" {', '\t\terr = d.agentInbox.AckInboxItems(ackCtx, d.cfg.AgentInstanceID.String(), []string{message.InboxItemID})', '\t} else {', '\t\terr = d.threads.AckMessages(ackCtx, d.cfg.AgentID.String(), []string{message.ID})', '\t}'].join('\n'),
);
