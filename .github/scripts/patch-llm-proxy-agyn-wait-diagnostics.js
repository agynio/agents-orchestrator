const fs = require('fs');

const path = 'internal/proxy/handler.go';
let text = fs.readFileSync(path, 'utf8');

function replace(marker, original, replacement) {
  if (!text.includes(original)) {
    if (text.includes(replacement)) {
      return;
    }
    throw new Error(`${marker} not found`);
  }
  text = text.replace(original, replacement);
}

replace(
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

replace(
  'agyn wait updated diagnostics',
  [
    '\tproviderReq, err := buildProviderRequest(r.Context(), providerConfig.endpoint, providerConfig.token, updatedBody, stream, providerConfig.authMethod, r.Header)',
  ].join('\n'),
  [
    '\tif providerConfig.remoteName == agynWaitRemoteName {',
    '\t\tlogResponsesBody("proxy: agyn wait updated", updatedBody)',
    '\t}',
    '',
    '\tproviderReq, err := buildProviderRequest(r.Context(), providerConfig.endpoint, providerConfig.token, updatedBody, stream, providerConfig.authMethod, r.Header)',
  ].join('\n'),
);

replace(
  'agyn wait helper functions',
  [
    'func updateRequestPayload(payload map[string]any, remoteName string, forceStream bool) ([]byte, error) {',
  ].join('\n'),
  [
    'const agynWaitRemoteName = "shell-agyn-thread-create-wait"',
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
    'func updateRequestPayload(payload map[string]any, remoteName string, forceStream bool) ([]byte, error) {',
  ].join('\n'),
);

fs.writeFileSync(path, text);
