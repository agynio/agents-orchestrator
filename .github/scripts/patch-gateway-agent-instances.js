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
    '\tIdentityTypeUser   IdentityType = "user"',
    '\tIdentityTypeAgent  IdentityType = "agent"',
    '\tIdentityTypeApp    IdentityType = "app"',
    '\tIdentityTypeRunner IdentityType = "runner"',
  ].join('\n'),
  [
    '\tIdentityTypeUser          IdentityType = "user"',
    '\tIdentityTypeAgent         IdentityType = "agent"',
    '\tIdentityTypeAgentInstance IdentityType = "agent_instance"',
    '\tIdentityTypeApp           IdentityType = "app"',
    '\tIdentityTypeRunner        IdentityType = "runner"',
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
    '\t\tidentityID = workloadID',
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
