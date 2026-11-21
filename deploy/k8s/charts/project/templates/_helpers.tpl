{{/* Labels that never change with the chart version: selectors and the
     immutable volumeClaimTemplates (C-2). */}}
{{- define "project.stableLabels" -}}
app.kubernetes.io/part-of: airflow-project-one
{{- end }}

{{/* Common labels. */}}
{{- define "project.labels" -}}
app.kubernetes.io/part-of: airflow-project-one
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/* The project image, repository:tag. */}}
{{- define "project.image" -}}
{{ required "image.repository is required" .Values.image.repository }}:{{ required "image.tag is required" .Values.image.tag }}
{{- end }}

{{/* Pod security context of the pods running the project image (uid 50000). */}}
{{- define "project.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: {{ .Values.podSecurity.runAsUser }}
runAsGroup: {{ .Values.podSecurity.runAsGroup }}
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{/* Container security context shared by every container of this chart. */}}
{{- define "project.containerSecurityContext" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
{{- end }}
