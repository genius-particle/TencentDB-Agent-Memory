{{- define "tdai-memory.gatewayYaml" -}}
deployMode: standalone
stateBackend: {{ .Values.stateBackend | quote }}
server:
  port: 8420
  host: 0.0.0.0
data:
  baseDir: /data/tdai-memory
llm:
  baseUrl: ""
  apiKey: ""
  model: ""
  maxTokens: 32000
  timeoutMs: 300000
memory:
  promptMode: chat
  capture:
    enabled: true
  extraction:
    enabled: true
    enableDedup: true
    maxMemoriesPerSession: 20
  persona:
    triggerEveryN: 50
    maxScenes: 15
  pipeline:
    everyNConversations: 5
    enableWarmup: true
    l1IdleTimeoutSeconds: 600
    l2DelayAfterL1Seconds: 90
    l2MinIntervalSeconds: 900
    l2MaxIntervalSeconds: 3600
  recall:
    enabled: true
    maxResults: 5
    scoreThreshold: 0.3
    strategy: hybrid
    timeoutMs: 5000
  storeBackend: {{ .Values.store.mode | quote }}
  embedding:
    provider: {{ .Values.embedding.provider | quote }}
    baseUrl: {{ .Values.embedding.baseUrl | quote }}
    apiKey: "${EMBEDDING_API_KEY}"
    model: {{ .Values.embedding.model | quote }}
    dimensions: {{ .Values.embedding.dimensions }}
    sendDimensions: {{ .Values.embedding.sendDimensions }}
skill:
  enabled: true
  routing:
    mode: bm25
    searchTopK: 20
  extraction:
    enabled: true
    maxIterations: 16
    queue:
      backend: local
      keyPrefix: tdai
      resultTtlSeconds: 86400
      lockTtlMs: 600000
      maxRetries: 2
      retryBackoffsMs: [5000, 15000]
  resources:
    maxResourceSizeBytes: 5000000
scanner:
  intervalMs: {{ .Values.scanner.intervalMs }}
{{- if eq .Values.store.mode "postgres" }}
metadata:
  store:
    backend: postgres
{{- end }}
{{- end }}
