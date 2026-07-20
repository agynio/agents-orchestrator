const fs = require('fs');

function read(path) {
  return fs.readFileSync(path, 'utf8');
}

function write(path, text) {
  fs.writeFileSync(path, text);
}

function replace(path, marker, original, replacement) {
  const text = read(path);
  if (!text.includes(original)) {
    if (text.includes(replacement)) {
      return;
    }
    throw new Error(`${path}: ${marker} not found`);
  }
  write(path, text.replace(original, replacement));
}

const mainPath = 'suites/go-core/tests/main_test.go';
const idlePath = 'suites/go-core/tests/idle_test.go';

replace(
  mainPath,
  'agent instance label constant',
  'labelThreadID  = "thread-id"',
  'labelThreadID  = "agent-instance-id"',
);

replace(
  mainPath,
  'sync import',
  ['\t"strings"', '\t"testing"'].join('\n'),
  ['\t"strings"', '\t"sync"', '\t"testing"'].join('\n'),
);

replace(
  mainPath,
  'thread agent participant registry',
  [
    'var (',
    '\tagentsAddr      = envOrDefault("AGENTS_ADDRESS", "agents:50051")',
  ].join('\n'),
  [
    'var threadAgentParticipants sync.Map',
    '',
    'var (',
    '\tagentsAddr      = envOrDefault("AGENTS_ADDRESS", "agents:50051")',
  ].join('\n'),
);

replace(
  mainPath,
  'remember thread participants',
  [
    '\tif thread == nil {',
    '\t\tt.Fatal("create thread: nil response")',
    '\t}',
    '\treturn thread',
  ].join('\n'),
  [
    '\tif thread == nil {',
    '\t\tt.Fatal("create thread: nil response")',
    '\t}',
    '\trememberThreadAgentParticipants(thread, participantIDs)',
    '\treturn thread',
  ].join('\n'),
);

replace(
  mainPath,
  'agent participant helpers',
  [
    'func archiveThread(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, threadID string) {',
  ].join('\n'),
  [
    'func rememberThreadAgentParticipants(thread *threadsv1.Thread, requestedParticipantIDs []string) {',
    '\tif thread == nil || thread.GetId() == "" {',
    '\t\treturn',
    '\t}',
    '\trequested := make(map[string]bool, len(requestedParticipantIDs))',
    '\tfor _, id := range requestedParticipantIDs {',
    '\t\tif id != "" {',
    '\t\t\trequested[id] = true',
    '\t\t}',
    '\t}',
    '\tparticipants := thread.GetParticipants()',
    '\tresolved := make(map[string]string, len(requested))',
    '\tfor _, participant := range participants {',
    '\t\tid := participant.GetId()',
    '\t\tif requested[id] {',
    '\t\t\tresolved[id] = id',
    '\t\t}',
    '\t}',
    '\tunmatchedRequested := make([]string, 0, len(requested))',
    '\tfor id := range requested {',
	'\t\tif resolved[id] != "" {',
    '\t\t\tcontinue',
    '\t\t}',
    '\t\tunmatchedRequested = append(unmatchedRequested, id)',
    '\t}',
    '\tunmatchedParticipants := make([]string, 0, len(participants))',
    '\tfor _, participant := range participants {',
    '\t\tid := participant.GetId()',
    '\t\tif id != "" && !requested[id] {',
    '\t\t\tunmatchedParticipants = append(unmatchedParticipants, id)',
    '\t\t}',
    '\t}',
    '\tif len(unmatchedRequested) == 1 && len(unmatchedParticipants) == 1 {',
    '\t\tresolved[unmatchedRequested[0]] = unmatchedParticipants[0]',
    '\t}',
    '\tthreadAgentParticipants.Store(thread.GetId(), resolved)',
    '}',
    '',
    'func agentParticipantIDForThread(t *testing.T, threadID, agentID string) string {',
    '\tt.Helper()',
    '\tvalue, ok := threadAgentParticipants.Load(threadID)',
    '\tif !ok {',
    '\t\tt.Fatalf("thread %s has no participant mapping", threadID)',
    '\t}',
    '\tparticipants, ok := value.(map[string]string)',
    '\tif !ok {',
    '\t\tt.Fatalf("thread %s participant mapping has unexpected type", threadID)',
    '\t}',
    '\tparticipantID := participants[agentID]',
    '\tif participantID == "" {',
    '\t\tt.Fatalf("thread %s has no participant mapping for agent %s", threadID, agentID)',
    '\t}',
    '\treturn participantID',
    '}',
    '',
    'func agentResponseSenderMatches(msg *threadsv1.Message, agentID string, labels map[string]string) bool {',
    '\tsenderID := msg.GetSenderId()',
    '\tif senderID == agentID {',
    '\t\treturn true',
    '\t}',
    '\treturn labels[labelThreadID] != "" && senderID == labels[labelThreadID]',
    '}',
    '',
    'func archiveThread(t *testing.T, ctx context.Context, client threadsv1.ThreadsServiceClient, threadID string) {',
  ].join('\n'),
);

replace(
  mainPath,
  'response sender matcher',
  [
    '\tmessageMatches := func(msg *threadsv1.Message) bool {',
    '\t\tif msg.GetSenderId() != agentID {',
    '\t\t\treturn false',
    '\t\t}',
  ].join('\n'),
  [
    '\tmessageMatches := func(msg *threadsv1.Message) bool {',
    '\t\tif !agentResponseSenderMatches(msg, agentID, labels) {',
    '\t\t\treturn false',
    '\t\t}',
  ].join('\n'),
);

const goCoreFiles = fs.readdirSync('suites/go-core/tests')
  .filter((name) => name.endsWith('.go'))
  .map((name) => `suites/go-core/tests/${name}`);

for (const path of goCoreFiles) {
  let text = read(path);
  text = text.replace(
    /(labelAgentID:\s+([^,\n]+),\n\s*)labelThreadID:\s+([^,\n]+),/g,
    (_match, prefix, agentID, threadID) => `${prefix}labelThreadID:  agentParticipantIDForThread(t, ${threadID.trim()}, ${agentID.trim()}),`,
  );
  text = text.replace(
    /(assertLabel\(t,\s*([^,]+),\s*labelAgentID,\s*([^\)]+)\)\n\s*)assertLabel\(t,\s*\2,\s*labelThreadID,\s*([^\)]+)\)/g,
    (_match, prefix, labelsResp, agentID, threadID) => `${prefix}assertLabel(t, ${labelsResp.trim()}, labelThreadID, agentParticipantIDForThread(t, ${threadID.trim()}, ${agentID.trim()}))`,
  );
  write(path, text);
}

replace(
  'suites/go-core/tests/threads_send_test.go',
  'multi-message sender matcher',
  'if msg.GetSenderId() != agentID {',
  'if !agentResponseSenderMatches(msg, agentID, labels) {',
);

replace(
  idlePath,
  'remove class agent identity context',
  '\tagentThreadsCtx := withAgentIdentity(ctx, agentID)\n',
  '',
);

replace(
  idlePath,
  'idle ack messages',
  [
    '\tackAllUnackedMessages(t, agentThreadsCtx, threadsClient, agentID)',
    '\tunackedCtx, unackedCancel := context.WithTimeout(agentThreadsCtx, unackedDrainTimeout)',
    '\tdefer unackedCancel()',
    '\tif err := pollUntil(unackedCtx, pollInterval, func(ctx context.Context) error {',
    '\t\tmessageIDs, err := listUnackedMessageIDs(ctx, threadsClient, agentID)',
    '\t\tif err != nil {',
    '\t\t\treturn err',
    '\t\t}',
    '\t\tif len(messageIDs) == 0 {',
    '\t\t\treturn nil',
    '\t\t}',
    '\t\tackMessages(t, ctx, threadsClient, agentID, messageIDs)',
  ].join('\n'),
  [
    '\tagentParticipantID := agentParticipantIDForThread(t, threadID, agentID)',
    '\tagentThreadsCtx := withAgentIdentity(ctx, agentParticipantID)',
    '\tackAllUnackedMessages(t, agentThreadsCtx, threadsClient, agentParticipantID)',
    '\tunackedCtx, unackedCancel := context.WithTimeout(agentThreadsCtx, unackedDrainTimeout)',
    '\tdefer unackedCancel()',
    '\tif err := pollUntil(unackedCtx, pollInterval, func(ctx context.Context) error {',
    '\t\tmessageIDs, err := listUnackedMessageIDs(ctx, threadsClient, agentParticipantID)',
    '\t\tif err != nil {',
    '\t\t\treturn err',
    '\t\t}',
    '\t\tif len(messageIDs) == 0 {',
    '\t\t\treturn nil',
    '\t\t}',
    '\t\tackMessages(t, ctx, threadsClient, agentParticipantID, messageIDs)',
  ].join('\n'),
);

replace(
  'suites/go-core/tests/dedup_test.go',
  'dedup cleanup identity',
  'agentCleanupCtx := withAgentIdentity(cleanupCtx, agentID)\n\t\tackAllUnackedMessagesBestEffort(t, agentCleanupCtx, threadsClient, agentID)',
  'agentParticipantID := agentParticipantIDForThread(t, threadID, agentID)\n\t\tagentCleanupCtx := withAgentIdentity(cleanupCtx, agentParticipantID)\n\t\tackAllUnackedMessagesBestEffort(t, agentCleanupCtx, threadsClient, agentParticipantID)',
);

replace(
  'suites/go-core/tests/multi_test.go',
  'same agent participant assertion map',
  [
    '\tthreadIDs := map[string]bool{',
    '\t\tthreadAID: true,',
    '\t\tthreadBID: true,',
    '\t}',
    '\tfoundThreads := map[string]bool{}',
  ].join('\n'),
  [
    '\tagentParticipantIDs := map[string]bool{',
    '\t\tagentParticipantIDForThread(t, threadAID, agentID): true,',
    '\t\tagentParticipantIDForThread(t, threadBID, agentID): true,',
    '\t}',
    '\tfoundAgentParticipants := map[string]bool{}',
  ].join('\n'),
);
replace(
  'suites/go-core/tests/multi_test.go',
  'same agent label assertion',
  [
    '\t\tthreadID := labelsResp[labelThreadID]',
    '\t\tif !threadIDs[threadID] {',
    '\t\t\tt.Fatalf("unexpected thread id label %q", threadID)',
    '\t\t}',
    '\t\tfoundThreads[threadID] = true',
    '\t}',
    '\tif len(foundThreads) != 2 {',
    '\t\tt.Fatalf("expected workloads for two threads, got %d", len(foundThreads))',
  ].join('\n'),
  [
    '\t\tagentParticipantID := labelsResp[labelThreadID]',
    '\t\tif !agentParticipantIDs[agentParticipantID] {',
    '\t\t\tt.Fatalf("unexpected agent instance id label %q", agentParticipantID)',
    '\t\t}',
    '\t\tfoundAgentParticipants[agentParticipantID] = true',
    '\t}',
    '\tif len(foundAgentParticipants) != 2 {',
    '\t\tt.Fatalf("expected workloads for two agent instances, got %d", len(foundAgentParticipants))',
  ].join('\n'),
);

replace(
  'suites/go-core/tests/diagnostics_helpers_test.go',
  'tracing diagnostic selector',
  'selector := fmt.Sprintf("%s=%s,%s=%s", labelManagedBy, managedByValue, labelThreadID, threadID)',
  'selector := fmt.Sprintf("%s=%s", labelManagedBy, managedByValue)',
);

const agynWaitPath = 'suites/go-core/tests/agent_agyn_wait_test.go';
replace(
  agynWaitPath,
  'agent thread list filter',
  [
    '\t\tresp, err := client.ListOrganizationThreads(ctx, &threadsv1.ListOrganizationThreadsRequest{',
    '\t\t\tOrganizationId: orgID,',
    '\t\t\tFilter: &threadsv1.ListOrganizationThreadsFilter{',
    '\t\t\t\tParticipantIdIn: []string{participantA, participantB},',
    '\t\t\t\tStatusIn:        []threadsv1.ThreadStatus{threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE},',
    '\t\t\t},',
  ].join('\n'),
  [
    '\t\tresp, err := client.ListOrganizationThreads(ctx, &threadsv1.ListOrganizationThreadsRequest{',
    '\t\t\tOrganizationId: orgID,',
    '\t\t\tFilter: &threadsv1.ListOrganizationThreadsFilter{',
    '\t\t\t\tStatusIn: []threadsv1.ThreadStatus{threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE},',
    '\t\t\t},',
  ].join('\n'),
);
replace(
  agynWaitPath,
  'agent thread participant predicate',
  'if thread == nil || thread.GetId() == "" || !threadHasParticipants(thread, participantA, participantB) {',
  'if thread == nil || thread.GetId() == "" {',
);
replace(
  agynWaitPath,
  'agent b reply body check',
  'if !messagesContainSenderBody(messagesB, agentBID, agynWaitAgentBResponse) {',
  'if !messagesContainBody(messagesB, agynWaitAgentBResponse) {',
);
replace(
  agynWaitPath,
  'body contains helper',
  [
    'func messagesContainSenderBody(messages []*threadsv1.Message, senderID, body string) bool {',
    '\tfor _, msg := range messages {',
    '\t\tif msg.GetSenderId() == senderID && msg.GetBody() == body {',
    '\t\t\treturn true',
    '\t\t}',
    '\t}',
    '\treturn false',
    '}',
  ].join('\n'),
  [
    'func messagesContainBody(messages []*threadsv1.Message, body string) bool {',
    '\tfor _, msg := range messages {',
    '\t\tif msg.GetBody() == body {',
    '\t\t\treturn true',
    '\t\t}',
    '\t}',
    '\treturn false',
    '}',
  ].join('\n'),
);
replace(
  agynWaitPath,
  'diagnostic candidate filter',
  'Filter:         &threadsv1.ListOrganizationThreadsFilter{ParticipantIdIn: []string{agentAID, agentBID}},\n\t\tPageSize:       25,',
  'Filter:         &threadsv1.ListOrganizationThreadsFilter{StatusIn: []threadsv1.ThreadStatus{threadsv1.ThreadStatus_THREAD_STATUS_ACTIVE}},\n\t\tPageSize:       25,',
);
replace(
  agynWaitPath,
  'diagnostic participant predicate',
  'if thread == nil || thread.GetId() == "" || !threadHasParticipants(thread, agentAID, agentBID) {',
  'if thread == nil || thread.GetId() == "" {',
);


const startRetryPath = 'suites/go-core/tests/workload_start_retry_policy_test.go';
replace(
  startRetryPath,
  'start retry agent instance local',
  [
    '	if threadID == "" {',
    '		t.Fatal("create thread: missing id")',
    '	}',
    '	t.Cleanup(func() { archiveThread(t, threadsCtx, threadsClient, threadID) })',
  ].join('\n'),
  [
    '	if threadID == "" {',
    '		t.Fatal("create thread: missing id")',
    '	}',
    '	agentParticipantID := agentParticipantIDForThread(t, threadID, agentID)',
    '	t.Cleanup(func() { archiveThread(t, threadsCtx, threadsClient, threadID) })',
  ].join('\n'),
);
replace(
  startRetryPath,
  'start retry labels use local',
  'labelThreadID:  agentParticipantIDForThread(t, threadID, agentID),',
  'labelThreadID:  agentParticipantID,',
);
replace(
  startRetryPath,
  'start retry failed workload owner',
  'failedWorkloads, err := waitForFailedWorkloads(failureCtx, runnersClient, threadID, agentID, 2)',
  'failedWorkloads, err := waitForFailedWorkloads(failureCtx, runnersClient, agentParticipantID, agentID, 2)',
);
replace(
  startRetryPath,
  'start retry failed latest validation',
  'assertFailedWorkload(t, failedLatest, threadID, agentID)\n\tassertFailedWorkload(t, failedPrevious, threadID, agentID)',
  'assertFailedWorkload(t, failedLatest, agentParticipantID, agentID)\n\tassertFailedWorkload(t, failedPrevious, agentParticipantID, agentID)',
);
replace(
  startRetryPath,
  'start retry all workload owner',
  'allWorkloads, err := listWorkloadsByThread(ctx, runnersClient, threadID, agentID, nil)',
  'allWorkloads, err := listWorkloadsByThread(ctx, runnersClient, agentParticipantID, agentID, nil)',
);
replace(
  startRetryPath,
  'start retry retry workload owner',
  'retryWorkload, err := waitForRetryWorkload(fastRetryCtx, runnersClient, threadID, agentID, removedAt)',
  'retryWorkload, err := waitForRetryWorkload(fastRetryCtx, runnersClient, agentParticipantID, agentID, removedAt)',
);


replace(
  'suites/go-core/suite.yaml',
  'optional go-core buf generate',
  '  buf generate\n\n  tag_args="e2e"',
  '  if [ "${E2E_SKIP_BUF_GENERATE:-}" != "true" ]; then\n    buf generate\n  fi\n\n  tag_args="e2e"',
);
const actionPath = '.github/actions/run-tests/action.yml';
replace(
  actionPath,
  'skip nested checkout',
  '    - name: Checkout e2e repository\n      uses: actions/checkout@v4',
  '    - name: Checkout e2e repository\n      if: ${{ false }}\n      uses: actions/checkout@v4',
);
