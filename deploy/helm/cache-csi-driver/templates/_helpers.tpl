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

{{- define "cache-csi-driver.resourceName" -}}
{{- $suffix := .suffix -}}
{{- $prefixLength := int (sub 62 (len $suffix)) -}}
{{- $prefix := include "cache-csi-driver.fullname" .root | trunc $prefixLength | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix $suffix | trimSuffix "-" -}}
{{- end -}}

{{- define "cache-csi-driver.healthControllerServiceAccountName" -}}
{{- include "cache-csi-driver.resourceName" (dict "root" . "suffix" "health-controller") -}}
{{- end -}}
