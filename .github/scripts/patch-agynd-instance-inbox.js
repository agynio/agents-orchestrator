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


fs.writeFileSync('internal/platform/grpc.go', `package platform

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func DialGateway(address string) (*grpc.ClientConn, error) {
	return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func DialKubernetesService(address string) (*grpc.ClientConn, error) {
	if strings.TrimSpace(address) == "" {
		return nil, fmt.Errorf("address is required")
	}
	return grpc.NewClient(
		"passthrough:///"+address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialKubernetesService),
	)
}

func dialKubernetesService(ctx context.Context, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{}
	if net.ParseIP(host) != nil {
		return dialer.DialContext(ctx, "tcp", address)
	}
	resolver := kubernetesResolver()
	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	var lastErr error
	for _, ip := range ips {
		conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.IP.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func kubernetesResolver() *net.Resolver {
	server := clusterDNSServer()
	if server == "" {
		return net.DefaultResolver
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network string, address string) (net.Conn, error) {
			dialer := &net.Dialer{}
			return dialer.DialContext(ctx, "udp", net.JoinHostPort(server, "53"))
		},
	}
}

func clusterDNSServer() string {
	contents, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(contents), "\\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "nameserver" {
			continue
		}
		ip := net.ParseIP(fields[1])
		if ip == nil || ip.IsLoopback() {
			continue
		}
		return ip.String()
	}
	return ""
}
`);

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
	"context"
	"fmt"
	"strings"

	agentsv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/agents/v1"
	"google.golang.org/grpc/metadata"
)

type Agents struct {
	client     agentsv1.AgentsServiceClient
	identityID string
}

func NewAgents(client agentsv1.AgentsServiceClient, identityID string) *Agents {
	identityID = strings.TrimSpace(identityID)
	if identityID == "" {
		panic("identity id is required")
	}
	return &Agents{client: client, identityID: identityID}
}

func (a *Agents) GetUnackedInboxItems(ctx context.Context, agentInstanceID string, pageSize int32, pageToken string) ([]Message, string, error) {
	if agentInstanceID == "" {
		return nil, "", fmt.Errorf("agent instance id is required")
	}
	resp, err := a.client.GetUnackedInboxItems(a.authContext(ctx), &agentsv1.GetUnackedInboxItemsRequest{
		AgentInstanceId: agentInstanceID,
		PageSize:        pageSize,
		PageToken:       pageToken,
	})
	if err != nil {
		return nil, "", fmt.Errorf("get unacked inbox items: %w", err)
	}
	messages := make([]Message, 0, len(resp.GetItems()))
	for _, item := range resp.GetItems() {
		parsed, err := inboxItemFromProto(item)
		if err != nil {
			return nil, "", err
		}
		messages = append(messages, parsed)
	}
	return messages, resp.GetNextPageToken(), nil
}

func (a *Agents) AckInboxItems(ctx context.Context, agentInstanceID string, itemIDs []string) error {
	if agentInstanceID == "" {
		return fmt.Errorf("agent instance id is required")
	}
	if len(itemIDs) == 0 {
		return fmt.Errorf("item ids are required")
	}
	for _, id := range itemIDs {
		if id == "" {
			return fmt.Errorf("item id is required")
		}
	}
	_, err := a.client.AckInboxItems(a.authContext(ctx), &agentsv1.AckInboxItemsRequest{
		AgentInstanceId: agentInstanceID,
		ItemIds:         append([]string{}, itemIDs...),
	})
	if err != nil {
		return fmt.Errorf("ack inbox items: %w", err)
	}
	return nil
}

func (a *Agents) authContext(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-identity-id", a.identityID)
}

func inboxItemFromProto(item *agentsv1.InboxItem) (Message, error) {
	if item == nil {
		return Message{}, fmt.Errorf("inbox item is nil")
	}
	id := item.GetId()
	if id == "" {
		return Message{}, fmt.Errorf("inbox item.id is required")
	}
	messageID := item.GetMessageId()
	if messageID == "" {
		messageID = id
	}
	threadID := item.GetThreadId()
	if threadID == "" {
		return Message{}, fmt.Errorf("inbox item.thread_id is required")
	}
	senderID := item.GetSenderId()
	if senderID == "" {
		return Message{}, fmt.Errorf("inbox item.sender_id is required")
	}
	createdAt := item.GetAcceptedAt()
	if createdAt == nil {
		return Message{}, fmt.Errorf("inbox item.accepted_at is required")
	}
	fileIDs := append([]string{}, item.GetFileIds()...)
	if item.GetBody() == "" && len(fileIDs) == 0 {
		return Message{}, fmt.Errorf("inbox item body or file ids are required")
	}
	return Message{
		ID:          messageID,
		InboxItemID: id,
		ThreadID:    threadID,
		SenderID:    senderID,
		Body:        item.GetBody(),
		FileIDs:     fileIDs,
		CreatedAt:   createdAt.AsTime(),
	}, nil
}
`);


fs.writeFileSync('internal/platform/runners.go', `package platform

import (
	"context"
	"fmt"
	"strings"
	"time"

	gatewayv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/gateway/v1"
	runnersv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/runners/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Workload struct {
	ID        string
	AgentID   string
	Status    runnersv1.WorkloadStatus
	CreatedAt time.Time
	RemovedAt *time.Time
}

type workloadTouchClient interface {
	TouchWorkload(ctx context.Context, in *runnersv1.TouchWorkloadRequest, opts ...grpc.CallOption) (*runnersv1.TouchWorkloadResponse, error)
}

type Runners struct {
	listClient  gatewayv1.RunnersGatewayClient
	touchClient workloadTouchClient
	identityID  string
}

func NewRunners(client gatewayv1.RunnersGatewayClient) *Runners {
	return &Runners{listClient: client, touchClient: client}
}

func NewRunnersWithTouchClient(listClient gatewayv1.RunnersGatewayClient, touchClient workloadTouchClient, identityID string) *Runners {
	identityID = strings.TrimSpace(identityID)
	if identityID == "" {
		panic("identity id is required")
	}
	return &Runners{listClient: listClient, touchClient: touchClient, identityID: identityID}
}

func (r *Runners) ListWorkloadsByThread(ctx context.Context, threadID string, pageSize int32, pageToken string) ([]Workload, string, error) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil, "", fmt.Errorf("thread id is required")
	}
	resp, err := r.listClient.ListWorkloadsByThread(ctx, &runnersv1.ListWorkloadsByThreadRequest{
		ThreadId:  threadID,
		PageSize:  pageSize,
		PageToken: pageToken,
	})
	if err != nil {
		return nil, "", fmt.Errorf("list workloads by thread: %w", err)
	}
	workloads := make([]Workload, 0, len(resp.GetWorkloads()))
	for _, workload := range resp.GetWorkloads() {
		converted, err := workloadFromProto(workload)
		if err != nil {
			return nil, "", err
		}
		workloads = append(workloads, converted)
	}
	return workloads, resp.GetNextPageToken(), nil
}

func (r *Runners) TouchWorkload(ctx context.Context, workloadID string) error {
	workloadID = strings.TrimSpace(workloadID)
	if workloadID == "" {
		return fmt.Errorf("workload id is required")
	}
	if r.identityID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-identity-id", r.identityID)
	}
	_, err := r.touchClient.TouchWorkload(ctx, &runnersv1.TouchWorkloadRequest{Id: workloadID})
	if err != nil {
		return fmt.Errorf("touch workload: %w", err)
	}
	return nil
}

func workloadFromProto(workload *runnersv1.Workload) (Workload, error) {
	if workload == nil {
		return Workload{}, fmt.Errorf("workload is required")
	}
	meta := workload.GetMeta()
	if meta == nil {
		return Workload{}, fmt.Errorf("workload meta is required")
	}
	workloadID := strings.TrimSpace(meta.GetId())
	if workloadID == "" {
		return Workload{}, fmt.Errorf("workload id is required")
	}
	agentID := strings.TrimSpace(workload.GetAgentId())
	if agentID == "" {
		return Workload{}, fmt.Errorf("workload agent id is required")
	}
	status := workload.GetStatus()
	if status == runnersv1.WorkloadStatus_WORKLOAD_STATUS_UNSPECIFIED {
		return Workload{}, fmt.Errorf("workload status is required")
	}
	createdAt := meta.GetCreatedAt()
	if createdAt == nil {
		return Workload{}, fmt.Errorf("workload created at is required")
	}
	createdAtTime := createdAt.AsTime()
	var removedAt *time.Time
	if workload.RemovedAt != nil {
		removedTime := workload.GetRemovedAt().AsTime()
		removedAt = &removedTime
	}

	return Workload{
		ID:        workloadID,
		AgentID:   agentID,
		Status:    status,
		CreatedAt: createdAtTime,
		RemovedAt: removedAt,
	}, nil
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
  'runnersv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/runners/v1"',
  '\tgatewayv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/gateway/v1"',
  ['\tgatewayv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/gateway/v1"', '\trunnersv1 "github.com/agynio/agynd-cli/.gen/go/agynio/api/runners/v1"'].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'type platformConns []platformConn',
  ['type platformConn interface {', '\tClose() error', '}'].join('\n'),
  [
    'type platformConn interface {',
    '\tClose() error',
    '}',
    '',
    'type platformConns []platformConn',
    '',
    'func (conns platformConns) Close() error {',
    '\tvar closeErr error',
    '\tfor _, conn := range conns {',
    '\t\tif err := conn.Close(); err != nil && closeErr == nil {',
    '\t\t\tcloseErr = err',
    '\t\t}',
    '\t}',
    '\treturn closeErr',
    '}',
  ].join('\n'),
);
replace(
  'internal/daemon/daemon.go',
  'agentsService := agentsv1.NewAgentsServiceClient',
  [
    '\trunnersGateway := gatewayv1.NewRunnersGatewayClient(gatewayConn)',
    '',
    '\tthreadsClient := platform.NewThreads(threadsGateway)',
  ].join('\n'),
  [
    '\trunnersGateway := gatewayv1.NewRunnersGatewayClient(gatewayConn)',
    '',
    '\tagentsConn, err := platform.DialKubernetesService("agents.platform.svc.cluster.local:50051")',
    '\tif err != nil {',
    '\t\t_ = gatewayConn.Close()',
    '\t\treturn nil, config.Config{}, fmt.Errorf("dial agents service agents.platform.svc.cluster.local:50051: %w", err)',
    '\t}',
    '\trunnersConn, err := platform.DialKubernetesService("runners.platform.svc.cluster.local:50051")',
    '\tif err != nil {',
    '\t\t_ = gatewayConn.Close()',
    '\t\t_ = agentsConn.Close()',
    '\t\treturn nil, config.Config{}, fmt.Errorf("dial runners service runners.platform.svc.cluster.local:50051: %w", err)',
    '\t}',
    '\tcloseConns := func() {',
    '\t\t_ = gatewayConn.Close()',
    '\t\t_ = agentsConn.Close()',
    '\t\t_ = runnersConn.Close()',
    '\t}',
    '\tagentsService := agentsv1.NewAgentsServiceClient(agentsConn)',
    '\trunnersService := runnersv1.NewRunnersServiceClient(runnersConn)',
    '',
    '\tthreadsClient := platform.NewThreads(threadsGateway)',
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
  ['\tthreadsClient := platform.NewThreads(threadsGateway)', '\tnotificationsClient := platform.NewNotifications(notificationsGateway)', '\tagentInboxClient := platform.NewAgents(agentsService, cfg.AgentInstanceID.String())', '\trunnersClient := platform.NewRunnersWithTouchClient(runnersGateway, runnersService, cfg.AgentInstanceID.String())'].join('\n'),
);
const daemonPath = 'internal/daemon/daemon.go';
let daemonText = fs.readFileSync(daemonPath, 'utf8');
if (daemonText.includes('closeConns := func()')) {
  daemonText = daemonText.replaceAll('\t\t_ = gatewayConn.Close()\n\t\treturn nil, config.Config{}, fmt.Errorf("get agent:', '\t\tcloseConns()\n\t\treturn nil, config.Config{}, fmt.Errorf("get agent:');
  daemonText = daemonText.replaceAll('\t\t_ = gatewayConn.Close()\n\t\treturn nil, config.Config{}, fmt.Errorf("agent not found")', '\t\tcloseConns()\n\t\treturn nil, config.Config{}, fmt.Errorf("agent not found")');
  daemonText = daemonText.replaceAll('\t\t_ = gatewayConn.Close()\n\t\treturn nil, config.Config{}, fmt.Errorf("list skills:', '\t\tcloseConns()\n\t\treturn nil, config.Config{}, fmt.Errorf("list skills:');
  daemonText = daemonText.replaceAll('\t\t_ = gatewayConn.Close()\n\t\treturn nil, config.Config{}, fmt.Errorf("list MCPs:', '\t\tcloseConns()\n\t\treturn nil, config.Config{}, fmt.Errorf("list MCPs:');
  daemonText = daemonText.replaceAll('\t\t_ = gatewayConn.Close()\n\t\treturn nil, config.Config{}, err', '\t\tcloseConns()\n\t\treturn nil, config.Config{}, err');
  daemonText = daemonText.replace('\t\tgatewayConn:   gatewayConn,', '\t\tgatewayConn:   platformConns{gatewayConn, agentsConn, runnersConn},');
  fs.writeFileSync(daemonPath, daemonText);
}

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
