{{- define "cache-csi-driver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cache-csi-driver.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "cache-csi-driver.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cache-csi-driver.labels" -}}
helm.sh/chart: {{ include "cache-csi-driver.chart" . }}
{{ include "cache-csi-driver.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "cache-csi-driver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cache-csi-driver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cache-csi-driver.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cache-csi-driver.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "cache-csi-driver.validatePressureSettings" -}}
{{- if and .Values.pressure.allowPodEviction (not .Values.rbac.createPodEvictions) -}}
{{- fail "pressure.allowPodEviction requires rbac.createPodEvictions" -}}
{{- end -}}
{{- if and .Values.rbac.createPodEvictions (not .Values.pressure.allowPodEviction) -}}
{{- fail "rbac.createPodEvictions requires pressure.allowPodEviction" -}}
{{- end -}}
{{- if and .Values.pressure.allowForceDelete (not .Values.pressure.allowPodEviction) -}}
{{- fail "pressure.allowForceDelete requires pressure.allowPodEviction" -}}
{{- end -}}
{{- if and .Values.pressure.allowForceDelete (not .Values.rbac.deletePodsAtCriticalPressure) -}}
{{- fail "pressure.allowForceDelete requires rbac.deletePodsAtCriticalPressure" -}}
{{- end -}}
{{- if and .Values.rbac.deletePodsAtCriticalPressure (not .Values.pressure.allowForceDelete) -}}
{{- fail "rbac.deletePodsAtCriticalPressure requires pressure.allowForceDelete" -}}
{{- end -}}
{{- end -}}
