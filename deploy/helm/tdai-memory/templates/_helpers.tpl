{{- define "tdai-memory.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "tdai-memory.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.labels" -}}
helm.sh/chart: {{ include "tdai-memory.chart" . }}
{{ include "tdai-memory.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: tencentdb-agent-memory
{{- end }}

{{- define "tdai-memory.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tdai-memory.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "tdai-memory.core.fullname" -}}
{{- printf "%s-core" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.hub.fullname" -}}
{{- printf "%s-hub" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.proxy.fullname" -}}
{{- printf "%s-proxy" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.postgres.fullname" -}}
{{- printf "%s-postgres" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.minio.fullname" -}}
{{- printf "%s-minio" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.redis.fullname" -}}
{{- printf "%s-redis" (include "tdai-memory.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "tdai-memory.useRedisState" -}}
{{- if eq .Values.stateBackend "redis" }}yes{{- end -}}
{{- end }}

{{- define "tdai-memory.redisHost" -}}
{{- if .Values.redis.host }}
{{- .Values.redis.host }}
{{- else if .Values.redis.deploy }}
{{- include "tdai-memory.redis.fullname" . }}
{{- else }}
{{- required "redis.host is required when stateBackend=redis and redis.deploy=false" .Values.redis.host }}
{{- end }}
{{- end }}

{{- define "tdai-memory.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else }}
{{- printf "%s-secrets" (include "tdai-memory.fullname" .) }}
{{- end }}
{{- end }}

{{- define "tdai-memory.image.core" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.core.repository .Values.image.tag }}
{{- end }}

{{- define "tdai-memory.image.hub" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.hub.repository .Values.image.tag }}
{{- end }}

{{- define "tdai-memory.image.proxy" -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.proxy.repository .Values.image.tag }}
{{- end }}

{{- define "tdai-memory.openStack" -}}
{{- eq .Values.store.mode "postgres" }}
{{- end }}

{{- define "tdai-memory.databaseUrl" -}}
{{- if .Values.postgres.url }}
{{- .Values.postgres.url }}
{{- else if .Values.postgres.deploy }}
{{- printf "postgres://%s:%s@%s:%v/%s" .Values.postgres.auth.username (.Values.postgres.auth.password | urlquery) (include "tdai-memory.postgres.fullname" .) .Values.postgres.service.port .Values.postgres.auth.database }}
{{- else }}
{{- required "postgres.url is required when store.mode=postgres and postgres.deploy=false" .Values.postgres.url }}
{{- end }}
{{- end }}

{{- define "tdai-memory.useS3" -}}
{{- or .Values.minio.deploy .Values.minio.endpoint }}
{{- end }}

{{- define "tdai-memory.s3Endpoint" -}}
{{- if .Values.minio.endpoint }}
{{- .Values.minio.endpoint }}
{{- else if .Values.minio.deploy }}
{{- printf "http://%s:%v" (include "tdai-memory.minio.fullname" .) .Values.minio.service.apiPort }}
{{- else }}
{{- required "minio.endpoint is required when minio.deploy=false but S3 is selected" .Values.minio.endpoint }}
{{- end }}
{{- end }}

{{- define "tdai-memory.coreUrl" -}}
{{- printf "http://%s:%v" (include "tdai-memory.core.fullname" .) .Values.core.service.port }}
{{- end }}

{{- define "tdai-memory.public.proxyUrl" -}}
{{- if .Values.public.proxyUrl }}
{{- .Values.public.proxyUrl }}
{{- else if .Values.public.host }}
{{- $port := .Values.proxy.service.port }}
{{- if and (eq .Values.proxy.service.type "NodePort") .Values.proxy.service.nodePort }}
{{- $port = .Values.proxy.service.nodePort }}
{{- end }}
{{- printf "http://%s:%v" .Values.public.host $port }}
{{- else }}
{{- printf "http://%s:%v" (include "tdai-memory.proxy.fullname" .) .Values.proxy.service.port }}
{{- end }}
{{- end }}

{{- define "tdai-memory.public.knowledgeUrl" -}}
{{- if .Values.public.knowledgeUrl }}
{{- .Values.public.knowledgeUrl }}
{{- else if .Values.public.host }}
{{- $port := .Values.hub.service.knowledgePort }}
{{- if and (eq .Values.hub.service.type "NodePort") .Values.hub.service.knowledgeNodePort }}
{{- $port = .Values.hub.service.knowledgeNodePort }}
{{- end }}
{{- printf "http://%s:%v/v3" .Values.public.host $port }}
{{- else }}
{{- printf "http://%s:%v/v3" (include "tdai-memory.hub.fullname" .) .Values.hub.service.knowledgePort }}
{{- end }}
{{- end }}
