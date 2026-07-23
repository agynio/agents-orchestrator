const fs = require('fs');

function replaceOnce(path, label, original, replacement) {
  let text = fs.readFileSync(path, 'utf8');
  if (text.includes(replacement)) {
    return;
  }
  if (!text.includes(original)) {
    throw new Error(`${label} block not found`);
  }
  text = text.replace(original, replacement);
  fs.writeFileSync(path, text);
}

replaceOnce(
  'internal/server/server.go',
  'threads agent instance participant send authorization',
  [
    '\tif err := s.requireAllowed(ctx, identityID, "can_write", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String())); err != nil {',
    '\t\treturn nil, err',
    '\t}',
    '',
    '\tthread, err := s.store.GetThread(ctx, threadID)',
    '\tif err != nil {',
    '\t\treturn nil, toStatusError(err)',
    '\t}',
  ].join('\n'),
  [
    '\tthread, err := s.store.GetThread(ctx, threadID)',
    '\tif err != nil {',
    '\t\treturn nil, toStatusError(err)',
    '\t}',
    '\tallowed, err := s.checkAllowed(ctx, identityID, "can_write", fmt.Sprintf("%s%s", threadObjectPrefix, threadID.String()))',
    '\tif err != nil {',
    '\t\treturn nil, err',
    '\t}',
    '\tif !allowed && !agentInstanceIsThreadParticipant(ctx, thread, identityID) {',
    '\t\treturn nil, status.Error(codes.PermissionDenied, "permission denied")',
    '\t}',
  ].join('\n'),
);

replaceOnce(
  'internal/server/server.go',
  'threads agent instance participant helper',
  [
    'func (s *Server) deliveryRecipients(ctx context.Context, participants []store.Participant, senderID uuid.UUID) ([]uuid.UUID, []uuid.UUID, error) {',
  ].join('\n'),
  [
    'func agentInstanceIsThreadParticipant(ctx context.Context, thread store.Thread, identityID uuid.UUID) bool {',
    '\tif !isAgentInstanceIdentity(ctx) {',
    '\t\treturn false',
    '\t}',
    '\tfor _, participant := range thread.Participants {',
    '\t\tif participant.ID == identityID {',
    '\t\t\treturn true',
    '\t\t}',
    '\t}',
    '\treturn false',
    '}',
    '',
    'func (s *Server) deliveryRecipients(ctx context.Context, participants []store.Participant, senderID uuid.UUID) ([]uuid.UUID, []uuid.UUID, error) {',
  ].join('\n'),
);

replaceOnce(
  'internal/server/server.go',
  'threads agent instance identity helper',
  [
    'func isAgentIdentity(ctx context.Context) bool {',
    '\tmd, ok := metadata.FromIncomingContext(ctx)',
    '\tif !ok {',
    '\t\treturn false',
    '\t}',
    '\tidentityType := metadataValue(md, identityTypeMetadataKey)',
    '\treturn strings.EqualFold(identityType, agentIdentityType) || strings.EqualFold(identityType, agentInstanceIdentityType)',
    '}',
  ].join('\n'),
  [
    'func isAgentIdentity(ctx context.Context) bool {',
    '\tmd, ok := metadata.FromIncomingContext(ctx)',
    '\tif !ok {',
    '\t\treturn false',
    '\t}',
    '\tidentityType := metadataValue(md, identityTypeMetadataKey)',
    '\treturn strings.EqualFold(identityType, agentIdentityType) || strings.EqualFold(identityType, agentInstanceIdentityType)',
    '}',
    '',
    'func isAgentInstanceIdentity(ctx context.Context) bool {',
    '\tmd, ok := metadata.FromIncomingContext(ctx)',
    '\tif !ok {',
    '\t\treturn false',
    '\t}',
    '\tidentityType := metadataValue(md, identityTypeMetadataKey)',
    '\treturn strings.EqualFold(identityType, agentInstanceIdentityType)',
    '}',
  ].join('\n'),
);

replaceOnce(
  'internal/server/server_test.go',
  'threads send denied get thread expectation',
  [
    '\tstoreStub := &stubThreadStore{',
    '\t\tt: t,',
    '\t\tsendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {',
    '\t\t\tstoreCalled = true',
    '\t\t\treturn store.SendMessageResult{}, nil',
    '\t\t},',
    '\t}',
  ].join('\n'),
  [
    '\tstoreStub := &stubThreadStore{',
    '\t\tt: t,',
    '\t\tgetThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {',
    '\t\t\tif id != threadID {',
    '\t\t\t\tt.Fatalf("expected thread %s, got %s", threadID, id)',
    '\t\t\t}',
    '\t\t\treturn store.Thread{ID: threadID, Participants: []store.Participant{{ID: uuid.New(), JoinedAt: time.Now().UTC(), Passive: false}}}, nil',
    '\t\t},',
    '\t\tsendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {',
    '\t\t\tstoreCalled = true',
    '\t\t\treturn store.SendMessageResult{}, nil',
    '\t\t},',
    '\t}',
  ].join('\n'),
);

const testPath = 'internal/server/server_test.go';
let testText = fs.readFileSync(testPath, 'utf8');
if (!testText.includes('func TestSendMessageAllowsAgentInstanceParticipantWhenAuthorizationLags(')) {
  const marker = 'func TestSendMessageRecordsUsageWithThreadOrganization(t *testing.T) {';
  if (!testText.includes(marker)) {
    throw new Error('threads send usage test marker not found');
  }
  const addition = [
    'func TestSendMessageAllowsAgentInstanceParticipantWhenAuthorizationLags(t *testing.T) {',
    '\tthreadID := uuid.New()',
    '\tmessageID := uuid.New()',
    '\tidentityID := uuid.New()',
    '\trecipientID := uuid.New()',
    '\tnow := time.Now().UTC()',
    '\tstoreCalled := false',
    '',
    '\tstoreStub := &stubThreadStore{',
    '\t\tt: t,',
    '\t\tgetThreadFn: func(ctx context.Context, id uuid.UUID) (store.Thread, error) {',
    '\t\t\tif id != threadID {',
    '\t\t\t\tt.Fatalf("expected thread %s, got %s", threadID, id)',
    '\t\t\t}',
    '\t\t\treturn store.Thread{ID: threadID, Participants: []store.Participant{',
    '\t\t\t\t{ID: identityID, JoinedAt: now, Passive: false},',
    '\t\t\t\t{ID: recipientID, JoinedAt: now, Passive: false},',
    '\t\t\t}}, nil',
    '\t\t},',
    '\t\tsendMessageFn: func(ctx context.Context, threadArg, senderArg uuid.UUID, body string, fileIDs []uuid.UUID, messageRecipientIDs []uuid.UUID, agentInstanceRecipientIDs []uuid.UUID) (store.SendMessageResult, error) {',
    '\t\t\tstoreCalled = true',
    '\t\t\tif threadArg != threadID {',
    '\t\t\t\tt.Fatalf("expected thread %s, got %s", threadID, threadArg)',
    '\t\t\t}',
    '\t\t\tif senderArg != identityID {',
    '\t\t\t\tt.Fatalf("expected sender %s, got %s", identityID, senderArg)',
    '\t\t\t}',
    '\t\t\treturn store.SendMessageResult{Message: store.Message{ID: messageID, ThreadID: threadID, SenderID: identityID, Body: body, CreatedAt: now}}, nil',
    '\t\t},',
    '\t}',
    '\tauthStub := &stubAuthorizationService{',
    '\t\tt: t,',
    '\t\tcheckFn: func(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {',
    '\t\t\treturn &authorizationv1.CheckResponse{Allowed: false}, nil',
    '\t\t},',
    '\t}',
    '',
    '\tsrv := New(storeStub, &stubNotifier{t: t}, authStub, &stubIdentityResolver{t: t}, nil, nil)',
    '\tctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(',
    '\t\t"x-identity-id", identityID.String(),',
    '\t\t"x-identity-type", agentInstanceIdentityType,',
    '\t))',
    '\t_, err := srv.SendMessage(ctx, &threadsv1.SendMessageRequest{ThreadId: threadID.String(), SenderId: identityID.String(), Body: "hi"})',
    '\tif err != nil {',
    '\t\tt.Fatalf("SendMessage returned error: %v", err)',
    '\t}',
    '\tif !storeCalled {',
    '\t\tt.Fatal("expected SendMessage to be called")',
    '\t}',
    '}',
    '',
  ].join('\n');
  testText = testText.replace(marker, addition + marker);
  fs.writeFileSync(testPath, testText);
}
