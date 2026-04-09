{{- define "service-base.renderEnv" -}}
{{- $env := concat (.Values.env | default (list)) (.Values.extraEnvVars | default (list)) -}}
{{- $hasTracing := false -}}
{{- range $env }}
{{- if and (hasKey . "name") (eq .name "AGENT_TRACING_ADDRESS") -}}
{{- $hasTracing = true -}}
{{- end -}}
{{- end -}}
{{- if not $hasTracing -}}
{{- $env = append $env (dict "name" "AGENT_TRACING_ADDRESS" "value" (.Values.agentTracingAddress | default "")) -}}
{{- end -}}
{{- if $env }}
env:
{{ toYaml $env | nindent 2 }}
{{- end }}
{{- end -}}
