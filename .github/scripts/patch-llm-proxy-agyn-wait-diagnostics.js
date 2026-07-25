const fs = require('fs');

const handlerPath = 'internal/proxy/handler.go';
let handlerText = fs.readFileSync(handlerPath, 'utf8');

function replaceHandler(marker, original, replacement) {
  if (!handlerText.includes(original)) {
    if (handlerText.includes(replacement)) {
      return;
    }
    throw new Error(`${handlerPath}: ${marker} not found`);
  }
  handlerText = handlerText.replace(original, replacement);
}

replaceHandler(
  'agyn wait request diagnostics',
  [
    '\tif err := h.authorizeRequest(r.Context(), resolvedIdentity, providerConfig, modelID); err != nil {',
  ].join('\n'),
  [
    '\tif providerConfig.remoteName == agynWaitRemoteName {',
    '\t\tlogResponsesPayload("proxy: agyn wait original", payload)',
    '\t}',
    '',
    '\tif err := h.authorizeRequest(r.Context(), resolvedIdentity, providerConfig, modelID); err != nil {',
  ].join('\n'),
);

replaceHandler(
  'agyn wait replacement initialization',
  [
    '\tmeteringMeta := meteringMetadata{',
  ].join('\n'),
  [
    '\tagynWaitReplacement := agynWaitReplacementFromPayload(providerConfig.remoteName, payload)',
    '\tif providerConfig.remoteName == agynWaitRemoteName && agynWaitReplacement == nil {',
    '\t\tlog.Printf("proxy: agyn wait dynamic values not found")',
    '\t}',
    '',
    '\tmeteringMeta := meteringMetadata{',
  ].join('\n'),
);

replaceHandler(
  'agyn wait updated call',
  '\tupdatedBody, err := updateRequestPayload(payload, providerConfig.remoteName, stream)',
  '\tupdatedBody, err := updateRequestPayload(payload, providerConfig.remoteName, stream, agynWaitReplacement)',
);

replaceHandler(
  'agyn wait updated diagnostics',
  '\tproviderReq, err := buildProviderRequest(r.Context(), providerConfig.endpoint, providerConfig.token, updatedBody, stream, providerConfig.authMethod, r.Header)',
  [
    '\tif providerConfig.remoteName == agynWaitRemoteName {',
    '\t\tlogResponsesBody("proxy: agyn wait updated", updatedBody)',
    '\t}',
    '',
    '\tproviderReq, err := buildProviderRequest(r.Context(), providerConfig.endpoint, providerConfig.token, updatedBody, stream, providerConfig.authMethod, r.Header)',
  ].join('\n'),
);

replaceHandler(
  'agyn wait forward response call',
  '\th.forwardResponse(w, providerReq, meteringMeta)',
  '\th.forwardResponse(w, providerReq, meteringMeta, agynWaitReplacement)',
);

replaceHandler(
  'agyn wait forward response signature',
  'func (h *Handler) forwardResponse(w http.ResponseWriter, req *http.Request, meta meteringMetadata) {',
  'func (h *Handler) forwardResponse(w http.ResponseWriter, req *http.Request, meta meteringMetadata, agynWaitReplacement *agynWaitReplacement) {',
);

replaceHandler(
  'agyn wait forward response rewrite',
  '\tcopyHeaders(w.Header(), resp.Header, nil)',
  [
    '\tif agynWaitReplacement != nil && resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {',
    '\t\tbody = agynWaitReplacement.rewriteProviderResponse(body)',
    '\t\tlogResponsesBody("proxy: agyn wait response", body)',
    '\t}',
    '',
    '\tcopyHeaders(w.Header(), resp.Header, nil)',
  ].join('\n'),
);

replaceHandler(
  'force identity provider encoding',
  [
    '\tcopyProviderRequestHeaders(req.Header, callerHeaders)',
    '\treq.Header.Set("Content-Type", "application/json")',
  ].join('\n'),
  [
    '\tcopyProviderRequestHeaders(req.Header, callerHeaders)',
    '\treq.Header.Set("Content-Type", "application/json")',
    '\treq.Header.Set("Accept-Encoding", "identity")',
  ].join('\n'),
);
replaceHandler(
  'strip accept encoding for provider rewrite',
  'case "Host", "Content-Length", "Connection", "Transfer-Encoding", "Keep-Alive", "Te", "Trailer", "Upgrade", "Authorization", "X-Api-Key":',
  'case "Host", "Content-Length", "Connection", "Transfer-Encoding", "Keep-Alive", "Te", "Trailer", "Upgrade", "Authorization", "X-Api-Key", "Accept-Encoding":',
);
replaceHandler(
  'agyn wait helper functions',
  'func updateRequestPayload(payload map[string]any, remoteName string, forceStream bool) ([]byte, error) {',
  [
    'const (',
    '\tagynWaitRemoteName = "shell-agyn-thread-create-wait"',
    '',
    '\tagynWaitFixedNickname = "e2e-agyn-wait-b-fixed"',
    '\tagynWaitFixedRef      = "e2e-agyn-wait-fixed"',
    '\tagynWaitFixedSentinel = "e2e-agyn-wait-sentinel-fixed"',
    ')',
    '',
    'type agynWaitReplacement struct {',
    '\tnickname string',
    '\tref      string',
    '\tsentinel string',
    '}',
    '',
    'func agynWaitReplacementFromPayload(remoteName string, payload map[string]any) *agynWaitReplacement {',
    '\tif remoteName != agynWaitRemoteName {',
    '\t\treturn nil',
    '\t}',
    '\tprompt := firstResponsesUserContent(payload)',
    '\tif prompt == "" {',
    '\t\treturn nil',
    '\t}',
    '\tnickname, ok := extractAgynWaitValue(prompt, "with @", " ")',
    '\tif !ok {',
    '\t\treturn nil',
    '\t}',
    '\tref, ok := extractAgynWaitValue(prompt, " using ref ", ",")',
    '\tif !ok {',
    '\t\treturn nil',
    '\t}',
    '\tsentinel, ok := extractAgynWaitValue(prompt, "Please reply with ", "\\\"")',
    '\tif !ok {',
    '\t\treturn nil',
    '\t}',
    '\treturn &agynWaitReplacement{nickname: nickname, ref: ref, sentinel: sentinel}',
    '}',
    '',
    'func firstResponsesUserContent(payload map[string]any) string {',
    '\tinput, ok := payload["input"].([]any)',
    '\tif !ok {',
    '\t\treturn ""',
    '\t}',
    '\tfor _, itemValue := range input {',
    '\t\titem, ok := itemValue.(map[string]any)',
    '\t\tif !ok || item["role"] != "user" {',
    '\t\t\tcontinue',
    '\t\t}',
    '\t\tcontent, ok := item["content"].(string)',
    '\t\tif ok {',
    '\t\t\treturn content',
    '\t\t}',
    '\t}',
    '\treturn ""',
    '}',
    '',
    'func extractAgynWaitValue(input string, prefix string, suffix string) (string, bool) {',
    '\tstart := strings.Index(input, prefix)',
    '\tif start < 0 {',
    '\t\treturn "", false',
    '\t}',
    '\tstart += len(prefix)',
    '\tremainder := input[start:]',
    '\tend := strings.Index(remainder, suffix)',
    '\tif end < 0 {',
    '\t\treturn "", false',
    '\t}',
    '\tvalue := strings.TrimSpace(remainder[:end])',
    '\tif value == "" {',
    '\t\treturn "", false',
    '\t}',
    '\treturn value, true',
    '}',
    '',
    'func (r *agynWaitReplacement) normalizeProviderRequest(body []byte) []byte {',
    '\tupdated := string(body)',
    '\tupdated = strings.ReplaceAll(updated, r.nickname, agynWaitFixedNickname)',
    '\tupdated = strings.ReplaceAll(updated, r.ref, agynWaitFixedRef)',
    '\tupdated = strings.ReplaceAll(updated, r.sentinel, agynWaitFixedSentinel)',
    '\treturn []byte(updated)',
    '}',
    '',
    'func (r *agynWaitReplacement) rewriteProviderResponse(body []byte) []byte {',
    '\tupdated := string(body)',
    '\tupdated = strings.ReplaceAll(updated, agynWaitFixedNickname, r.nickname)',
    '\tupdated = strings.ReplaceAll(updated, agynWaitFixedRef, r.ref)',
    '\tupdated = strings.ReplaceAll(updated, agynWaitFixedSentinel, r.sentinel)',
    '\treturn []byte(updated)',
    '}',
    '',
    'func logResponsesPayload(prefix string, payload map[string]any) {',
    '\tinput, ok := payload["input"].([]any)',
    '\tif !ok {',
    '\t\tlog.Printf("%s input_type=%T input=%v", prefix, payload["input"], payload["input"])',
    '\t\treturn',
    '\t}',
    '\tlog.Printf("%s input_len=%d", prefix, len(input))',
    '\tfor i := 0; i < len(input) && i < 4; i++ {',
    '\t\tencoded, err := json.Marshal(input[i])',
    '\t\tif err != nil {',
    '\t\t\tlog.Printf("%s input[%d] marshal_err=%v type=%T", prefix, i, err, input[i])',
    '\t\t\tcontinue',
    '\t\t}',
    '\t\tlog.Printf("%s input[%d]=%s", prefix, i, string(encoded))',
    '\t}',
    '}',
    '',
    'func logResponsesBody(prefix string, body []byte) {',
    '\tvar payload map[string]any',
    '\tif err := json.Unmarshal(body, &payload); err != nil {',
    '\t\tlog.Printf("%s body_parse_err=%v raw=%s", prefix, err, string(body))',
    '\t\treturn',
    '\t}',
    '\tlogResponsesPayload(prefix, payload)',
    '}',
    '',
    'func updateRequestPayload(payload map[string]any, remoteName string, forceStream bool, agynWaitReplacement *agynWaitReplacement) ([]byte, error) {',
  ].join('\n'),
);

replaceHandler(
  'agyn wait normalize updated payload',
  [
    '\tupdated, err := json.Marshal(payload)',
    '\tif err != nil {',
    '\t\treturn nil, fmt.Errorf("%w: %v", ErrInvalidBody, err)',
    '\t}',
    '\treturn updated, nil',
  ].join('\n'),
  [
    '\tupdated, err := json.Marshal(payload)',
    '\tif err != nil {',
    '\t\treturn nil, fmt.Errorf("%w: %v", ErrInvalidBody, err)',
    '\t}',
    '\tif agynWaitReplacement != nil {',
    '\t\tupdated = agynWaitReplacement.normalizeProviderRequest(updated)',
    '\t}',
    '\treturn updated, nil',
  ].join('\n'),
);

fs.writeFileSync(handlerPath, handlerText);

const testPath = 'internal/proxy/handler_test.go';
if (fs.existsSync(testPath)) {
  let testText = fs.readFileSync(testPath, 'utf8');
  const testName = 'TestHandlerRewritesAgynWaitFixtureValues';
  if (!testText.includes(testName)) {
    testText += [
      '',
      'func TestHandlerRewritesAgynWaitFixtureValues(t *testing.T) {',
      '\tmodelID := uuid.New()',
      '\tvar providerPayload map[string]any',
      '\tprovider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {',
      '\t\tbody, err := io.ReadAll(r.Body)',
      '\t\tif err != nil {',
      '\t\t\tt.Fatalf("read provider body: %v", err)',
      '\t\t}',
      '\t\tif err := json.Unmarshal(body, &providerPayload); err != nil {',
      '\t\t\tt.Fatalf("unmarshal provider body: %v", err)',
      '\t\t}',
      '\t\tif encoding := r.Header.Get("Accept-Encoding"); encoding != "identity" {',
      '\t\t\tt.Fatalf("provider saw unexpected accept encoding header: %q", encoding)',
      '\t\t}',
      '\t\tencoded := string(body)',
      '\t\tif strings.Contains(encoded, "e2e-aw-b-12345678") || strings.Contains(encoded, "e2e-aw-ref-12345678") || strings.Contains(encoded, "e2e-aw-sentinel-12345678") {',
      '\t\t\tt.Fatalf("provider saw dynamic agyn wait values: %s", encoded)',
      '\t\t}',
      '\t\tif !strings.Contains(encoded, agynWaitFixedNickname) || !strings.Contains(encoded, agynWaitFixedRef) || !strings.Contains(encoded, agynWaitFixedSentinel) {',
      '\t\t\tt.Fatalf("provider did not see fixed agyn wait values: %s", encoded)',
      '\t\t}',
      '\t\tw.Header().Set("Content-Type", "application/json")',
      '\t\tw.WriteHeader(http.StatusOK)',
      '\t\t_, _ = w.Write([]byte(`{"output":[{"type":"function_call","call_id":"fc_shell_agyn_wait_001","name":"shell","arguments":"{\\"command\\": \\"agyn threads create --add @e2e-agyn-wait-b-fixed --ref e2e-agyn-wait-fixed --send \\\\\\\"Please reply with e2e-agyn-wait-sentinel-fixed\\\\\\\" --wait 120\\"}"}]}`))',
      '\t}))',
      '\tdefer provider.Close()',
      '',
      '\tllmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{',
      '\t\tEndpoint:       provider.URL + "/responses",',
      '\t\tToken:          "provider-token",',
      '\t\tRemoteName:     agynWaitRemoteName,',
      '\t\tOrganizationId: "org-1",',
      '\t\tProtocol:       llmv1.Protocol_PROTOCOL_RESPONSES,',
      '\t\tAuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,',
      '\t}}',
      '\tauthzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}',
      '\thandler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, provider.Client())',
      '',
      '\tprompt := `Use agyn CLI to create a new thread with @e2e-aw-b-12345678 using ref e2e-aw-ref-12345678, send the exact text "Please reply with e2e-aw-sentinel-12345678", wait for the reply, then tell me whether it worked.`',
      '\tbody := `{"model":"` + modelID.String() + `","stream":false,"input":[{"role":"user","content":` + strconv.Quote(prompt) + `}]}`',
      '\treq := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))',
      '\treq.Header.Set("Accept-Encoding", "gzip")',
      '\tctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})',
      '\treq = req.WithContext(ctx)',
      '\tresp := httptest.NewRecorder()',
      '',
      '\thandler.ServeHTTP(resp, req)',
      '',
      '\tif resp.Code != http.StatusOK {',
      '\t\tt.Fatalf("expected status %d, got %d: %s", http.StatusOK, resp.Code, resp.Body.String())',
      '\t}',
      '\tif providerPayload["model"] != agynWaitRemoteName {',
      '\t\tt.Fatalf("expected remote model, got %v", providerPayload["model"])',
      '\t}',
      '\tresponseBody := resp.Body.String()',
      '\tif strings.Contains(responseBody, agynWaitFixedNickname) || strings.Contains(responseBody, agynWaitFixedRef) || strings.Contains(responseBody, agynWaitFixedSentinel) {',
      '\t\tt.Fatalf("client saw fixed agyn wait values: %s", responseBody)',
      '\t}',
      '\tif !strings.Contains(responseBody, "e2e-aw-b-12345678") || !strings.Contains(responseBody, "e2e-aw-ref-12345678") || !strings.Contains(responseBody, "e2e-aw-sentinel-12345678") {',
      '\t\tt.Fatalf("client did not see dynamic agyn wait values: %s", responseBody)',
      '\t}',
      '}',
      '',
    ].join('\n');
  }
  if (testText.includes('strconv.Quote') && !testText.includes('"strconv"')) {
    testText = testText.replace('"strings"\n\t"testing"', '"strconv"\n\t"strings"\n\t"testing"');
  }
  fs.writeFileSync(testPath, testText);
}
