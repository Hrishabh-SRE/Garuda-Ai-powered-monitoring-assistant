{{- define "garuda-am-bridge.name" -}}
garuda-am-bridge
{{- end -}}

{{- define "garuda-am-bridge.labels" -}}
app.kubernetes.io/name: {{ include "garuda-am-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: garuda
app.kubernetes.io/component: alertmanager-bridge
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "garuda-am-bridge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "garuda-am-bridge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
