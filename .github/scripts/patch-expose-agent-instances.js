const fs = require('fs');

function replace(path, marker, before, after) {
  let text = fs.readFileSync(path, 'utf8');
  if (text.includes(after)) {
    return;
  }
  if (!text.includes(before)) {
    throw new Error(`${marker} not found in ${path}`);
  }
  text = text.replace(before, after);
  fs.writeFileSync(path, text);
}

replace(
  'internal/server/server_test.go',
  'mock ziti sandbox identity method',
  `func (m *mockZitiMgmt) CreateAgentIdentity(context.Context, *zitimanagementv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockZitiMgmt) CreateAgentIdentity(context.Context, *zitimanagementv1.CreateAgentIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateSandboxIdentity(context.Context, *zitimanagementv1.CreateSandboxIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateSandboxIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock runners flavor methods',
  `func (m *mockRunners) EnrollRunner(context.Context, *runnersv1.EnrollRunnerRequest, ...grpc.CallOption) (*runnersv1.EnrollRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockRunners) EnrollRunner(context.Context, *runnersv1.EnrollRunnerRequest, ...grpc.CallOption) (*runnersv1.EnrollRunnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) CreateFlavor(context.Context, *runnersv1.CreateFlavorRequest, ...grpc.CallOption) (*runnersv1.CreateFlavorResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) GetFlavor(context.Context, *runnersv1.GetFlavorRequest, ...grpc.CallOption) (*runnersv1.GetFlavorResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) UpdateFlavor(context.Context, *runnersv1.UpdateFlavorRequest, ...grpc.CallOption) (*runnersv1.UpdateFlavorResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) DeleteFlavor(context.Context, *runnersv1.DeleteFlavorRequest, ...grpc.CallOption) (*runnersv1.DeleteFlavorResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListFlavors(context.Context, *runnersv1.ListFlavorsRequest, ...grpc.CallOption) (*runnersv1.ListFlavorsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock runners agent instance list method',
  `func (m *mockRunners) ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockRunners) ListWorkloadsByThread(context.Context, *runnersv1.ListWorkloadsByThreadRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListWorkloadsByAgentInstance(context.Context, *runnersv1.ListWorkloadsByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListWorkloadsByAgentInstanceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock runners volume agent instance list method',
  `func (m *mockRunners) ListVolumesByThread(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockRunners) ListVolumesByThread(context.Context, *runnersv1.ListVolumesByThreadRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByThreadResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockRunners) ListVolumesByAgentInstance(context.Context, *runnersv1.ListVolumesByAgentInstanceRequest, ...grpc.CallOption) (*runnersv1.ListVolumesByAgentInstanceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock ziti tunnel identity methods',
  `func (m *mockZitiMgmt) DeleteDeviceIdentity(context.Context, *zitimanagementv1.DeleteDeviceIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockZitiMgmt) DeleteDeviceIdentity(context.Context, *zitimanagementv1.DeleteDeviceIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteDeviceIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) CreateTunnelIdentity(context.Context, *zitimanagementv1.CreateTunnelIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.CreateTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) DeleteTunnelIdentity(context.Context, *zitimanagementv1.DeleteTunnelIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock ziti post-tunnel methods',
  `func (m *mockZitiMgmt) DeleteTunnelIdentity(context.Context, *zitimanagementv1.DeleteTunnelIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (m *mockZitiMgmt) DeleteTunnelIdentity(context.Context, *zitimanagementv1.DeleteTunnelIdentityRequest, ...grpc.CallOption) (*zitimanagementv1.DeleteTunnelIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) PatchIdentityRoleAttributes(context.Context, *zitimanagementv1.PatchIdentityRoleAttributesRequest, ...grpc.CallOption) (*zitimanagementv1.PatchIdentityRoleAttributesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) GetIdentityLiveness(context.Context, *zitimanagementv1.GetIdentityLivenessRequest, ...grpc.CallOption) (*zitimanagementv1.GetIdentityLivenessResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListServicesByTag(context.Context, *zitimanagementv1.ListServicesByTagRequest, ...grpc.CallOption) (*zitimanagementv1.ListServicesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListIdentitiesByTag(context.Context, *zitimanagementv1.ListIdentitiesByTagRequest, ...grpc.CallOption) (*zitimanagementv1.ListIdentitiesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListServicePoliciesByTag(context.Context, *zitimanagementv1.ListServicePoliciesByTagRequest, ...grpc.CallOption) (*zitimanagementv1.ListServicePoliciesByTagResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) UpdateService(context.Context, *zitimanagementv1.UpdateServiceRequest, ...grpc.CallOption) (*zitimanagementv1.UpdateServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock ziti service read methods',
  `func (m *mockZitiMgmt) CreateService(ctx context.Context, req *zitimanagementv1.CreateServiceRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	if m.createService == nil {
		return nil, errors.New("not implemented")
	}
	return m.createService(ctx, req)
}
`,
  `func (m *mockZitiMgmt) CreateService(ctx context.Context, req *zitimanagementv1.CreateServiceRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServiceResponse, error) {
	if m.createService == nil {
		return nil, errors.New("not implemented")
	}
	return m.createService(ctx, req)
}

func (m *mockZitiMgmt) GetService(context.Context, *zitimanagementv1.GetServiceRequest, ...grpc.CallOption) (*zitimanagementv1.GetServiceResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListServices(context.Context, *zitimanagementv1.ListServicesRequest, ...grpc.CallOption) (*zitimanagementv1.ListServicesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server_test.go',
  'mock ziti service policy read methods',
  `func (m *mockZitiMgmt) CreateServicePolicy(ctx context.Context, req *zitimanagementv1.CreateServicePolicyRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServicePolicyResponse, error) {
	if m.createServicePolicy == nil {
		return nil, errors.New("not implemented")
	}
	return m.createServicePolicy(ctx, req)
}
`,
  `func (m *mockZitiMgmt) CreateServicePolicy(ctx context.Context, req *zitimanagementv1.CreateServicePolicyRequest, _ ...grpc.CallOption) (*zitimanagementv1.CreateServicePolicyResponse, error) {
	if m.createServicePolicy == nil {
		return nil, errors.New("not implemented")
	}
	return m.createServicePolicy(ctx, req)
}

func (m *mockZitiMgmt) GetServicePolicy(context.Context, *zitimanagementv1.GetServicePolicyRequest, ...grpc.CallOption) (*zitimanagementv1.GetServicePolicyResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (m *mockZitiMgmt) ListServicePolicies(context.Context, *zitimanagementv1.ListServicePoliciesRequest, ...grpc.CallOption) (*zitimanagementv1.ListServicePoliciesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/server.go',
  'add exposure self check',
  `		if err := requireAgentSelf(caller, agentIDValue); err != nil {
			return nil, err
		}
		agentID = agentIDValue`,
  `		if err := requireWorkloadCaller(caller, workload, agentIDValue); err != nil {
			return nil, err
		}
		agentID = agentIDValue`,
);

replace(
  'internal/server/server.go',
  'remove exposure caller match',
  '	allowed, err := agentMatchesWorkload(caller, agentID)',
  '	allowed, err := callerMatchesWorkload(caller, workload, agentID)',
);

replace(
  'internal/server/server.go',
  'list exposure caller match',
  '	allowed, err := agentMatchesWorkload(caller, agentID)',
  '	allowed, err := callerMatchesWorkload(caller, workload, agentID)',
);

replace(
  'internal/server/server.go',
  'workload caller helpers',
  `func agentMatchesWorkload(caller exposureCaller, agentID uuid.UUID) (bool, error) {
	if caller.identity.identityType != identityTypeAgent {
		return false, nil
	}
	callerID, err := parseIdentityUUID(caller.identity.identityID)
	if err != nil {
		return false, err
	}
	return callerID == agentID, nil
}

func requireAgentSelf(caller exposureCaller, agentID uuid.UUID) error {
	allowed, err := agentMatchesWorkload(caller, agentID)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "agent id does not match workload")
	}
	return nil
}`,
  `func agentMatchesWorkload(caller exposureCaller, agentID uuid.UUID) (bool, error) {
	if caller.identity.identityType != identityTypeAgent {
		return false, nil
	}
	callerID, err := parseIdentityUUID(caller.identity.identityID)
	if err != nil {
		return false, err
	}
	return callerID == agentID, nil
}

func callerMatchesWorkload(caller exposureCaller, workload *runnersv1.Workload, agentID uuid.UUID) (bool, error) {
	if matched, err := agentMatchesWorkload(caller, agentID); err != nil || matched {
		return matched, err
	}
	if caller.identity.identityType != identityTypeAgent {
		return false, nil
	}
	callerID, err := parseIdentityUUID(caller.identity.identityID)
	if err != nil {
		return false, err
	}
	workloadAgentInstanceID := strings.TrimSpace(workload.GetAgentInstanceId())
	if workloadAgentInstanceID == "" {
		workloadAgentInstanceID = strings.TrimSpace(workload.GetOwnerId())
	}
	if workloadAgentInstanceID == "" {
		return false, nil
	}
	parsedAgentInstanceID, err := parseUUID(workloadAgentInstanceID, "workload.agent_instance_id")
	if err != nil {
		return false, status.Errorf(codes.Internal, "workload agent_instance_id invalid: %v", err)
	}
	return callerID == parsedAgentInstanceID, nil
}

func requireWorkloadCaller(caller exposureCaller, workload *runnersv1.Workload, agentID uuid.UUID) error {
	allowed, err := callerMatchesWorkload(caller, workload, agentID)
	if err != nil {
		return err
	}
	if !allowed {
		return status.Error(codes.PermissionDenied, "agent id does not match workload")
	}
	return nil
}`,
);

const testPath = 'internal/server/server_test.go';
let testText = fs.readFileSync(testPath, 'utf8');
const insertBefore = 'func TestAddExposureHappyPath(t *testing.T) {';
if (!testText.includes('func TestAddExposureAllowsAgentInstanceOwner')) {
  const testCase = `func TestAddExposureAllowsAgentInstanceOwner(t *testing.T) {
	workloadID := uuid.New()
	agentID := uuid.New()
	agentInstanceID := uuid.New()
	orgID := uuid.New()
	ctx := contextWithAgentIdentity(agentInstanceID, workloadID)

	storeMock := &mockStore{}
	storeMock.createExposure = func(context.Context, store.Exposure) error { return nil }
	storeMock.updateExposureProvisioned = func(context.Context, uuid.UUID, store.ExposureResourceIDs) error { return nil }
	storeMock.getExposure = func(context.Context, uuid.UUID) (store.Exposure, error) {
		return store.Exposure{
			ID: uuid.New(), WorkloadID: workloadID, AgentID: agentID, Port: 8080,
			OpenZitiServiceID: "svc-id", OpenZitiBindPolicyID: "bind-id",
			OpenZitiDialPolicyID: "dial-id", URL: "https://exposure.example.test",
			Status: store.ExposureStatusActive,
		}, nil
	}

	zitiMock := &mockZitiMgmt{}
	zitiMock.createService = func(context.Context, *zitimanagementv1.CreateServiceRequest) (*zitimanagementv1.CreateServiceResponse, error) {
		return &zitimanagementv1.CreateServiceResponse{ZitiServiceId: "svc-id"}, nil
	}
	zitiMock.createServicePolicy = func(context.Context, *zitimanagementv1.CreateServicePolicyRequest) (*zitimanagementv1.CreateServicePolicyResponse, error) {
		return &zitimanagementv1.CreateServicePolicyResponse{ZitiServicePolicyId: uuid.NewString()}, nil
	}

	runnersMock := &mockRunners{getWorkload: func(ctx context.Context, req *runnersv1.GetWorkloadRequest) (*runnersv1.GetWorkloadResponse, error) {
		if req.GetId() != workloadID.String() {
			return nil, fmt.Errorf("unexpected workload id %s", req.GetId())
		}
		assertOutgoingIdentity(t, ctx, agentInstanceID.String(), string(identityTypeAgent), workloadID.String())
		return &runnersv1.GetWorkloadResponse{Workload: &runnersv1.Workload{
			AgentId:         agentID.String(),
			OwnerId:         agentInstanceID.String(),
			OrganizationId:  orgID.String(),
		}}, nil
	}}

	svc := New(storeMock, zitiMock, runnersMock, defaultAuthz())
	if _, err := svc.AddExposure(ctx, &exposev1.AddExposureRequest{Port: 8080}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

`;
  testText = testText.replace(insertBefore, testCase + insertBefore);
  fs.writeFileSync(testPath, testText);
}
