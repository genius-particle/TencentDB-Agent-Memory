{{- define "tdai-memory.proxyYamlTmpl" -}}
server:
  host: 0.0.0.0
  port: 8096
  forwardTimeoutMs: 600000
upstream:
  url: "__PROXY_UPSTREAM_URL__"
  apiKey: "__PROXY_UPSTREAM_API_KEY__"
log:
  file: ""
  level: info
  backend: console
tdai:
  enabled: true
  endpoint: {{ include "tdai-memory.coreUrl" . | quote }}
  apiKey: ""
  serviceId: default
  memory:
    enabled: true
    inject: true
    writeL0: true
    recallL1: true
    injectL2L3: true
skill:
  endpoint: {{ include "tdai-memory.coreUrl" . | quote }}
  serviceToken: ""
  timeoutMs: {{ .Values.proxy.skillTimeoutMs }}
auth:
  enabled: true
  url: {{ include "tdai-memory.coreUrl" . | quote }}
  timeoutMs: {{ .Values.proxy.authTimeoutMs }}
sessionInit:
  enabled: true
  maxRetries: 3
  injectAgentContext: true
  injectTaskContext: true
  headerAutoSelect:
    enabled: true
    teamHeader: "x-team-id"
    agentHeader: "x-agent-id"
    taskHeader: "x-task-id"
    onMismatch: "form"
costGuard:
  enabled: false
injection:
  enabled: true
  injectors:
    - skill
    - knowledge
    - tdai-memory
  externalGatewayUrl: {{ include "tdai-memory.public.proxyUrl" . | quote }}
redis:
  enabled: false
storage:
  enabled: true
  backend: sqlite
{{- end }}
