{{- define "soft-drain.fullname" -}}
{{- if contains "soft-drain" .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-soft-drain" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "soft-drain.labels" -}}
app.kubernetes.io/name: soft-drain
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "soft-drain.selectorLabels" -}}
app.kubernetes.io/name: soft-drain
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
