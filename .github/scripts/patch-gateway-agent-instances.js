const fs = require('fs');

function replaceOnce(path, label, original, replacement) {
  let text = fs.readFileSync(path, 'utf8');
  if (text.includes(replacement)) {
    return;
  }
  if (!text.includes(original)) {
    throw new Error(label + ' block not found');
  }
  text = text.replace(original, replacement);
  fs.writeFileSync(path, text);
}

replaceOnce(
  'internal/identity/identity.go',
  'gateway agent instance identity type constant',
  [
    '\tIdentityTypeUser    IdentityType = "user"',
    '\tIdentityTypeAgent   IdentityType = "agent"',
    '\tIdentityTypeApp     IdentityType = "app"',
    '\tIdentityTypeRunner  IdentityType = "runner"',
    '\tIdentityTypeSandbox IdentityType = "sandbox"',
  ].join('\n'),
  [
    '\tIdentityTypeUser          IdentityType = "user"',
    '\tIdentityTypeAgent         IdentityType = "agent"',
    '\tIdentityTypeAgentInstance IdentityType = "agent_instance"',
    '\tIdentityTypeApp           IdentityType = "app"',
    '\tIdentityTypeRunner        IdentityType = "runner"',
    '\tIdentityTypeSandbox       IdentityType = "sandbox"',
  ].join('\n'),
);

replaceOnce(
  'internal/identity/identity.go',
  'gateway agent instance identity type parser',
  [
    '\tcase string(IdentityTypeAgent):',
    '\t\treturn IdentityTypeAgent, nil',
    '\tcase string(IdentityTypeApp):',
  ].join('\n'),
  [
    '\tcase string(IdentityTypeAgent):',
    '\t\treturn IdentityTypeAgent, nil',
    '\tcase string(IdentityTypeAgentInstance):',
    '\t\treturn IdentityTypeAgentInstance, nil',
    '\tcase string(IdentityTypeApp):',
  ].join('\n'),
);

replaceOnce(
  'internal/zitimgmtclient/client.go',
  'gateway ziti managed agent instance identity mapping',
  [
    '\tworkloadID := strings.TrimSpace(response.GetWorkloadId())',
    '\tif identityType == identity.IdentityTypeAgent && workloadID == "" {',
    '\t\treturn identity.ResolvedIdentity{}, fmt.Errorf("workload id missing")',
    '\t}',
    '',
    '\treturn identity.ResolvedIdentity{',
    '\t\tIdentityID:   identityID,',
    '\t\tIdentityType: identityType,',
    '\t\tWorkloadID:   workloadID,',
    '\t}, nil',
  ].join('\n'),
  [
    '\tworkloadID := strings.TrimSpace(response.GetWorkloadId())',
    '\tif identityType == identity.IdentityTypeAgent || identityType == identity.IdentityTypeAgentInstance {',
    '\t\tif workloadID == "" {',
    '\t\t\treturn identity.ResolvedIdentity{}, fmt.Errorf("workload id missing")',
    '\t\t}',
    '\t}',
    '\tif identityType == identity.IdentityTypeAgent && workloadID != "" {',
    '\t\tidentityType = identity.IdentityTypeAgentInstance',
    '\t}',
    '',
    '\treturn identity.ResolvedIdentity{',
    '\t\tIdentityID:   identityID,',
    '\t\tIdentityType: identityType,',
    '\t\tWorkloadID:   workloadID,',
    '\t}, nil',
  ].join('\n'),
);

replaceOnce(
  'internal/zitimgmtclient/client.go',
  'gateway ziti agent instance identity type enum',
  [
    '\tcase identityv1.IdentityType_IDENTITY_TYPE_AGENT:',
    '\t\treturn identity.IdentityTypeAgent, nil',
    '\tcase identityv1.IdentityType_IDENTITY_TYPE_RUNNER:',
  ].join('\n'),
  [
    '\tcase identityv1.IdentityType_IDENTITY_TYPE_AGENT:',
    '\t\treturn identity.IdentityTypeAgent, nil',
    '\tcase identityv1.IdentityType_IDENTITY_TYPE_AGENT_INSTANCE:',
    '\t\treturn identity.IdentityTypeAgentInstance, nil',
    '\tcase identityv1.IdentityType_IDENTITY_TYPE_RUNNER:',
  ].join('\n'),
);

replaceOnce(
  'internal/identity/identity_test.go',
  'gateway agent instance identity type test',
  [
    'func TestParseIdentityTypeApp(t *testing.T) {',
    '\tidentityType, err := ParseIdentityType("app")',
    '\tif err != nil {',
    '\t\tt.Fatalf("unexpected error: %v", err)',
    '\t}',
    '\tif identityType != IdentityTypeApp {',
    '\t\tt.Fatalf("unexpected identity type: %s", identityType)',
    '\t}',
    '}',
  ].join('\n'),
  [
    'func TestParseIdentityTypeApp(t *testing.T) {',
    '\tidentityType, err := ParseIdentityType("app")',
    '\tif err != nil {',
    '\t\tt.Fatalf("unexpected error: %v", err)',
    '\t}',
    '\tif identityType != IdentityTypeApp {',
    '\t\tt.Fatalf("unexpected identity type: %s", identityType)',
    '\t}',
    '}',
    '',
    'func TestParseIdentityTypeAgentInstance(t *testing.T) {',
    '\tidentityType, err := ParseIdentityType("agent_instance")',
    '\tif err != nil {',
    '\t\tt.Fatalf("unexpected error: %v", err)',
    '\t}',
    '\tif identityType != IdentityTypeAgentInstance {',
    '\t\tt.Fatalf("unexpected identity type: %s", identityType)',
    '\t}',
    '}',
  ].join('\n'),
);

const zitiClientTestPath = 'internal/zitimgmtclient/client_test.go';
if (!fs.existsSync(zitiClientTestPath)) {
  fs.writeFileSync(zitiClientTestPath, `package zitimgmtclient

import (
	"context"
	"net"
	"testing"

	"github.com/agynio/gateway/internal/identity"
	identityv1 "github.com/agynio/gateway/gen/agynio/api/identity/v1"
	zitimgmtv1 "github.com/agynio/gateway/gen/agynio/api/ziti_management/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestResolveIdentityMapsAgentWorkloadToAgentInstanceCaller(t *testing.T) {
	agentInstanceID := uuid.NewString()
	workloadID := uuid.NewString()
	server := grpc.NewServer()
	zitimgmtv1.RegisterZitiManagementServiceServer(server, &resolveIdentityServer{
		response: &zitimgmtv1.ResolveIdentityResponse{
			IdentityId:   agentInstanceID,
			IdentityType: identityv1.IdentityType_IDENTITY_TYPE_AGENT,
			WorkloadId:   &workloadID,
		},
	})
	listener := bufconn.Listen(1024 * 1024)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Errorf("serve ziti management: %v", err)
		}
	}()
	defer server.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial ziti management: %v", err)
	}
	defer conn.Close()

	client := &Client{conn: conn, client: zitimgmtv1.NewZitiManagementServiceClient(conn)}
	resolved, err := client.ResolveIdentity(context.Background(), "ziti-identity")
	if err != nil {
		t.Fatalf("ResolveIdentity failed: %v", err)
	}
	if resolved.IdentityID != agentInstanceID {
		t.Fatalf("expected identity id %s, got %s", agentInstanceID, resolved.IdentityID)
	}
	if resolved.IdentityType != identity.IdentityTypeAgentInstance {
		t.Fatalf("expected identity type %s, got %s", identity.IdentityTypeAgentInstance, resolved.IdentityType)
	}
	if resolved.WorkloadID != workloadID {
		t.Fatalf("expected workload id %s, got %s", workloadID, resolved.WorkloadID)
	}
}

type resolveIdentityServer struct {
	zitimgmtv1.UnimplementedZitiManagementServiceServer
	response *zitimgmtv1.ResolveIdentityResponse
}

func (s *resolveIdentityServer) ResolveIdentity(context.Context, *zitimgmtv1.ResolveIdentityRequest) (*zitimgmtv1.ResolveIdentityResponse, error) {
	return s.response, nil
}
`);
}


replaceOnce(
  'internal/gateway/threads.go',
  'gateway threads downstream identity context',
  [
    '	resp, err := g.threads.CreateThread(ctx, req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) ArchiveThread(ctx context.Context, req *connect.Request[threadsv1.ArchiveThreadRequest]) (*connect.Response[threadsv1.ArchiveThreadResponse], error) {',
    '	resp, err := g.threads.ArchiveThread(ctx, req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) AddParticipant(ctx context.Context, req *connect.Request[threadsv1.AddParticipantRequest]) (*connect.Response[threadsv1.AddParticipantResponse], error) {',
    '	resp, err := g.threads.AddParticipant(ctx, req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) SendMessage(ctx context.Context, req *connect.Request[threadsv1.SendMessageRequest]) (*connect.Response[threadsv1.SendMessageResponse], error) {',
    '	resp, err := g.threads.SendMessage(ctx, req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
  ].join('\n'),
  [
    '	resp, err := g.threads.CreateThread(downstreamContext(ctx), req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) ArchiveThread(ctx context.Context, req *connect.Request[threadsv1.ArchiveThreadRequest]) (*connect.Response[threadsv1.ArchiveThreadResponse], error) {',
    '	resp, err := g.threads.ArchiveThread(downstreamContext(ctx), req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) AddParticipant(ctx context.Context, req *connect.Request[threadsv1.AddParticipantRequest]) (*connect.Response[threadsv1.AddParticipantResponse], error) {',
    '	resp, err := g.threads.AddParticipant(downstreamContext(ctx), req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
    '',
    'func (g *ThreadsGateway) SendMessage(ctx context.Context, req *connect.Request[threadsv1.SendMessageRequest]) (*connect.Response[threadsv1.SendMessageResponse], error) {',
    '	resp, err := g.threads.SendMessage(downstreamContext(ctx), req.Msg)',
    '	if err != nil {',
    '		return nil, toConnectError(err)',
    '	}',
    '	return connect.NewResponse(resp), nil',
    '}',
  ].join('\n'),
);

replaceOnce(
  'cmd/gateway/main.go',
  'gateway ziti listen options',
  [
    '\t\tlistenerFactory := func(zitiCtx ziti.Context) (net.Listener, error) {',
    '\t\t\treturn zitiCtx.ListenWithOptions(serviceName, ziti.DefaultListenOptions())',
    '\t\t}',
  ].join('\n'),
  [
    '\t\tlistenerFactory := func(zitiCtx ziti.Context) (net.Listener, error) {',
    '\t\t\tlistenOptions := ziti.DefaultListenOptions()',
    '\t\t\tlistenOptions.WaitForNEstablishedListeners = 1',
    '\t\t\tlistenOptions.ConnectTimeout = config.ZitiEnrollmentTimeout',
    '\t\t\treturn zitiCtx.ListenWithOptions(serviceName, listenOptions)',
    '\t\t}',
  ].join('\n'),
);

replaceOnce(
  'internal/zitimanager/manager.go',
  'gateway ziti enrollment retry',
  [
    '\tvar identityID string',
    '\tvar identityJSON []byte',
    '\tif err := retryWithBackoff(enrollmentCtx, "ziti enrollment", func(attemptCtx context.Context) error {',
    '\t\tvar requestErr error',
    '\t\tidentityID, identityJSON, requestErr = m.mgmtClient.RequestServiceIdentity(attemptCtx, m.serviceType)',
    '\t\treturn requestErr',
    '\t}); err != nil {',
    '\t\treturn err',
    '\t}',
    '',
    '\tzitiConfig := &ziti.Config{}',
    '\tif err := json.Unmarshal(identityJSON, zitiConfig); err != nil {',
    '\t\treturn fmt.Errorf("failed to parse ziti identity: %w", err)',
    '\t}',
    '',
    '\tzitiCtx, err := newZitiContext(zitiConfig)',
    '\tif err != nil {',
    '\t\treturn fmt.Errorf("failed to create ziti context: %w", err)',
    '\t}',
    '',
    '\tlistener, err := m.listenerFactory(zitiCtx)',
    '\tif err != nil {',
    '\t\tzitiCtx.Close()',
    '\t\treturn err',
    '\t}',
  ].join('\n'),
  [
    '\tvar identityID string',
    '\tvar zitiCtx ziti.Context',
    '\tvar listener net.Listener',
    '\tif err := retryWithBackoff(enrollmentCtx, "ziti enrollment", func(attemptCtx context.Context) error {',
    '\t\tvar identityJSON []byte',
    '\t\tvar requestErr error',
    '\t\tidentityID, identityJSON, requestErr = m.mgmtClient.RequestServiceIdentity(attemptCtx, m.serviceType)',
    '\t\tif requestErr != nil {',
    '\t\t\treturn requestErr',
    '\t\t}',
    '',
    '\t\tzitiConfig := &ziti.Config{}',
    '\t\tif err := json.Unmarshal(identityJSON, zitiConfig); err != nil {',
    '\t\t\treturn fmt.Errorf("failed to parse ziti identity: %w", err)',
    '\t\t}',
    '',
    '\t\tzitiCtx, requestErr = newZitiContext(zitiConfig)',
    '\t\tif requestErr != nil {',
    '\t\t\treturn fmt.Errorf("failed to create ziti context: %w", requestErr)',
    '\t\t}',
    '',
    '\t\tlistener, requestErr = m.listenerFactory(zitiCtx)',
    '\t\tif requestErr != nil {',
    '\t\t\tzitiCtx.Close()',
    '\t\t\tzitiCtx = nil',
    '\t\t\treturn status.Error(codes.Unavailable, fmt.Sprintf("listen on ziti service: %v", requestErr))',
    '\t\t}',
    '',
    '\t\treturn nil',
    '\t}); err != nil {',
    '\t\treturn err',
    '\t}',
  ].join('\n'),
);

function appendMethods(path, receiver, serviceField, packageAlias, methods) {
  let text = fs.readFileSync(path, 'utf8').trimEnd();
  const additions = [];
  for (const method of methods) {
    const name = method[0];
    const request = method[1];
    const response = method[2];
    if (text.includes(`func (${receiver}) ${name}`)) {
      continue;
    }
    additions.push([
      '',
      `func (${receiver}) ${name}(ctx context.Context, req *connect.Request[${packageAlias}.${request}]) (*connect.Response[${packageAlias}.${response}], error) {`,
      `\tresp, err := ${receiver.split(' ')[0]}.${serviceField}.${name}(downstreamContext(ctx), req.Msg)`,
      '\tif err != nil {',
      '\t\treturn nil, toConnectError(err)',
      '\t}',
      '\treturn connect.NewResponse(resp), nil',
      '}',
    ].join('\n'));
  }
  if (additions.length === 0) {
    return;
  }
  fs.writeFileSync(path, `${text}\n${additions.join('\n')}\n`);
}

appendMethods('internal/gateway/agents.go', 'g *Gateway', 'agents', 'agentsv1', [
  ['CreateEnvironment', 'CreateEnvironmentRequest', 'CreateEnvironmentResponse'],
  ['GetEnvironment', 'GetEnvironmentRequest', 'GetEnvironmentResponse'],
  ['UpdateEnvironment', 'UpdateEnvironmentRequest', 'UpdateEnvironmentResponse'],
  ['DeleteEnvironment', 'DeleteEnvironmentRequest', 'DeleteEnvironmentResponse'],
  ['ListEnvironments', 'ListEnvironmentsRequest', 'ListEnvironmentsResponse'],
  ['CreateSandbox', 'CreateSandboxRequest', 'CreateSandboxResponse'],
  ['GetSandbox', 'GetSandboxRequest', 'GetSandboxResponse'],
  ['ListSandboxes', 'ListSandboxesRequest', 'ListSandboxesResponse'],
  ['StopSandbox', 'StopSandboxRequest', 'StopSandboxResponse'],
  ['DeleteSandbox', 'DeleteSandboxRequest', 'DeleteSandboxResponse'],
  ['EnsureSandboxRunning', 'EnsureSandboxRunningRequest', 'EnsureSandboxRunningResponse'],
  ['CreateInstance', 'CreateInstanceRequest', 'CreateInstanceResponse'],
  ['GetInstance', 'GetInstanceRequest', 'GetInstanceResponse'],
  ['ListInstances', 'ListInstancesRequest', 'ListInstancesResponse'],
  ['PauseInstance', 'PauseInstanceRequest', 'PauseInstanceResponse'],
  ['ResumeInstance', 'ResumeInstanceRequest', 'ResumeInstanceResponse'],
  ['DeleteInstance', 'DeleteInstanceRequest', 'DeleteInstanceResponse'],
  ['WriteInboxItem', 'WriteInboxItemRequest', 'WriteInboxItemResponse'],
  ['GetUnackedInboxItems', 'GetUnackedInboxItemsRequest', 'GetUnackedInboxItemsResponse'],
  ['AckInboxItems', 'AckInboxItemsRequest', 'AckInboxItemsResponse'],
  ['GetUnackedInboxCount', 'GetUnackedInboxCountRequest', 'GetUnackedInboxCountResponse'],
]);

appendMethods('internal/gateway/runners.go', 'g *RunnersGateway', 'runners', 'runnersv1', [
  ['CreateFlavor', 'CreateFlavorRequest', 'CreateFlavorResponse'],
  ['GetFlavor', 'GetFlavorRequest', 'GetFlavorResponse'],
  ['UpdateFlavor', 'UpdateFlavorRequest', 'UpdateFlavorResponse'],
  ['DeleteFlavor', 'DeleteFlavorRequest', 'DeleteFlavorResponse'],
  ['ListFlavors', 'ListFlavorsRequest', 'ListFlavorsResponse'],
  ['ListWorkloadsByAgentInstance', 'ListWorkloadsByAgentInstanceRequest', 'ListWorkloadsByAgentInstanceResponse'],
  ['ListVolumesByAgentInstance', 'ListVolumesByAgentInstanceRequest', 'ListVolumesByAgentInstanceResponse'],
]);
