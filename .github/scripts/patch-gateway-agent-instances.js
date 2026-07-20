const fs = require('fs');

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
