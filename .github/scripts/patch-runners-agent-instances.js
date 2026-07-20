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
  'internal/server/runners_test.go',
  'fake ziti sandbox identity method',
  `func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimanagementv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
  `func (f fakeZitiManagementClient) CreateAgentIdentity(ctx context.Context, req *zitimanagementv1.CreateAgentIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateAgentIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f fakeZitiManagementClient) CreateSandboxIdentity(ctx context.Context, req *zitimanagementv1.CreateSandboxIdentityRequest, opts ...grpc.CallOption) (*zitimanagementv1.CreateSandboxIdentityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
`,
);

replace(
  'internal/server/workloads.go',
  'CreateWorkload authorization owner',
  `	statusValue, err := workloadStatusToString(req.GetStatus())`,
  `	agentInstanceAuthorizationID := (*uuid.UUID)(nil)
	if req.AgentInstanceId != nil {
		agentInstanceAuthorizationID = ownerID
	}

	statusValue, err := workloadStatusToString(req.GetStatus())`,
);

replace(
  'internal/server/workloads.go',
  'CreateWorkload authorization call',
  'if err := s.writeWorkloadAuthorization(ctx, id, organizationID, agentID); err != nil {',
  'if err := s.writeWorkloadAuthorization(ctx, id, organizationID, agentID, agentInstanceAuthorizationID); err != nil {',
);

replace(
  'internal/server/workloads.go',
  'writeWorkloadAuthorization signature',
  `func (s *Server) writeWorkloadAuthorization(ctx context.Context, workloadID, organizationID uuid.UUID, agentID *uuid.UUID) error {
	tuples := workloadAuthorizationTuples(workloadID, organizationID, agentID)`,
  `func (s *Server) writeWorkloadAuthorization(ctx context.Context, workloadID, organizationID uuid.UUID, agentID *uuid.UUID, ownerID *uuid.UUID) error {
	tuples := workloadAuthorizationTuples(workloadID, organizationID, agentID, ownerID)`,
);

replace(
  'internal/server/workloads.go',
  'workloadAuthorizationTuples signature',
  'func workloadAuthorizationTuples(workloadID, organizationID uuid.UUID, agentID *uuid.UUID) []*authorizationv1.TupleKey {',
  'func workloadAuthorizationTuples(workloadID, organizationID uuid.UUID, agentID *uuid.UUID, ownerID *uuid.UUID) []*authorizationv1.TupleKey {',
);

replace(
  'internal/server/workloads.go',
  'owner authorization tuple',
  `	if agentID != nil {
		tuples = append(tuples, &authorizationv1.TupleKey{
			User:     identityObject(*agentID),
			Relation: workloadOwnerAgentRelation,
			Object:   object,
		})
	}
	return tuples`,
  `	if agentID != nil {
		tuples = append(tuples, &authorizationv1.TupleKey{
			User:     identityObject(*agentID),
			Relation: workloadOwnerAgentRelation,
			Object:   object,
		})
	}
	if ownerID != nil && (agentID == nil || *ownerID != *agentID) {
		tuples = append(tuples, &authorizationv1.TupleKey{
			User:     identityObject(*ownerID),
			Relation: workloadOwnerAgentRelation,
			Object:   object,
		})
	}
	return tuples`,
);

replace(
  'internal/server/workloads_test.go',
  'assert authorization writes signature',
  'func assertWorkloadAuthorizationWrites(t *testing.T, req *authorizationv1.WriteRequest, workloadID, organizationID, agentID uuid.UUID) {',
  'func assertWorkloadAuthorizationWrites(t *testing.T, req *authorizationv1.WriteRequest, workloadID, organizationID, agentID uuid.UUID, ownerID ...uuid.UUID) {',
);

replace(
  'internal/server/workloads_test.go',
  'assert owner authorization expected tuple',
  `	if len(writes) != len(expected) {
		t.Fatalf("expected %d authorization writes, got %d", len(expected), len(writes))
	}`,
  `	if len(ownerID) > 0 && ownerID[0] != agentID {
		expected = append(expected, &authorizationv1.TupleKey{
			User:     identityObject(ownerID[0]),
			Relation: workloadOwnerAgentRelation,
			Object:   workloadObject(workloadID),
		})
	}
	if len(writes) != len(expected) {
		t.Fatalf("expected %d authorization writes, got %d", len(expected), len(writes))
	}`,
);

{
  const path = 'internal/server/workloads_test.go';
  let text = fs.readFileSync(path, 'utf8');
  const fn = 'func TestCreateWorkloadMapsAgentInstanceIDToOwnerID';
  const start = text.indexOf(fn);
  if (start === -1) {
    throw new Error('agent instance owner test not found in internal/server/workloads_test.go');
  }
  const end = text.indexOf('func TestCreateWorkloadRejectsMismatchedAgentInstanceID', start);
  if (end === -1) {
    throw new Error('agent instance owner test end not found in internal/server/workloads_test.go');
  }
  const before = text.slice(0, start);
  let body = text.slice(start, end);
  const after = text.slice(end);
  body = body.replace(
    'assertWorkloadAuthorizationWrites(t, gotWriteReq, workloadID, organizationID, agentID)',
    'assertWorkloadAuthorizationWrites(t, gotWriteReq, workloadID, organizationID, agentID, agentInstanceID)',
  );
  fs.writeFileSync(path, before + body + after);
}
