#!/usr/bin/env bash
set -euo pipefail

cluster_name="cache-csi-api-validation-$$"
context="kind-$cluster_name"
tmp_dir="$(mktemp -d)"
ready_label='cache.csi.walnuts.dev/ready'
test_label='e2e.test.walnuts.dev/no-csi-driver'
probe_driver='scheduler-probe.csi.walnuts.dev'

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]] && kind get clusters 2>/dev/null | grep -qx "$cluster_name"; then
    kubectl --context "$context" get pods -A -o wide >&2 || true
    kubectl --context "$context" get events -A --sort-by=.lastTimestamp >&2 || true
    kubectl --context "$context" get validatingadmissionpolicies,mutatingadmissionpolicies -o yaml >&2 || true
    kubectl --context "$context" get validatingadmissionpolicybindings,mutatingadmissionpolicybindings -o yaml >&2 || true
  fi
  kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT

cat > "$tmp_dir/values.yaml" <<EOF
nodeSelector:
  $test_label: never-schedule-driver
healthController:
  replicaCount: 0
  nodeSelector:
    $test_label: never-schedule-controller
EOF

cat > "$tmp_dir/old-object-policy.yaml" <<'EOF'
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: cache-csi-e2e-old-object-typecheck
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
      - apiGroups: [""]
        apiVersions: ["v1"]
        operations: ["UPDATE"]
        resources: ["pods"]
        scope: Namespaced
  validations:
    - expression: >-
        !has(oldObject.spec.nodeName) ||
        !has(object.spec.nodeName) ||
        object.spec.nodeName == oldObject.spec.nodeName
      message: Pod nodeName cannot be cleared after scheduling.
EOF

cat > "$tmp_dir/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
EOF

kind create cluster \
  --name "$cluster_name" \
  --image kindest/node:v1.37.0 \
  --config "$tmp_dir/kind.yaml" \
  --wait 3m
kubectl --context "$context" wait --for=condition=Ready nodes --all --timeout=2m

helm template cache-csi-driver deploy/helm/cache-csi-driver \
  --namespace kube-system \
  --include-crds \
  --kube-version 1.37.0 \
  --values "$tmp_dir/values.yaml" > "$tmp_dir/chart.yaml"

# Install the CRD before strict API discovery validates the complete chart.
kubectl --context "$context" apply -f deploy/helm/cache-csi-driver/crds/cache.storage.walnuts.dev_cacheclasses.yaml
kubectl --context "$context" wait --for=condition=Established crd/cacheclasses.cache.storage.walnuts.dev --timeout=1m
kubectl --context "$context" apply --server-side --dry-run=server --validate=strict -f "$tmp_dir/chart.yaml"
kubectl --context "$context" apply -f "$tmp_dir/chart.yaml"
kubectl --context "$context" apply -f "$tmp_dir/old-object-policy.yaml"

wait_for_policy_typechecking() {
  local kind_name="$1" status
  for _ in $(seq 1 60); do
    status="$(kubectl --context "$context" get "$kind_name" -o go-template='{{range .items}}{{.metadata.generation}}{{"\t"}}{{.status.observedGeneration}}{{"\t"}}{{printf "%#v" .status.typeChecking}}{{"\t"}}{{range .status.typeChecking.expressionWarnings}}{{.fieldRef}}{{": "}}{{.warning}}{{end}}{{if not .status.typeChecking.expressionWarnings}}OK{{end}}{{"\n"}}{{end}}')"
    if [[ -n "$status" ]] && printf '%s\n' "$status" | awk -F '\t' 'NF != 4 || $1 != $2 || $3 == "<nil>" || $3 == "<no value>" || $4 != "OK" { failed = 1 } END { exit failed }'; then
      return
    fi
    sleep 1
  done
  kubectl --context "$context" get "$kind_name" -o yaml >&2
  return 1
}

wait_for_policy_typechecking validatingadmissionpolicies

if [[ "$(kubectl --context "$context" get csidriver cache.csi.walnuts.dev -o go-template='{{.spec.preventPodSchedulingIfMissing}}')" != true ]]; then
  echo "CSIDriver does not enable preventPodSchedulingIfMissing" >&2
  exit 1
fi

kubectl --context "$context" label namespace default cache.csi.walnuts.dev/allow-use=true --overwrite

readiness_selector=""
for _ in $(seq 1 60); do
  readiness_selector="$(kubectl --context "$context" create --dry-run=server -f - -o go-template='{{with .spec.nodeSelector}}{{index . "cache.csi.walnuts.dev/ready"}}{{" "}}{{index . "kubernetes.io/os"}}{{end}}' <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: cache-api-validation-positive
  namespace: default
spec:
  nodeSelector:
    kubernetes.io/os: linux
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10.1
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: api-validation-positive
          maxBytes: 1Mi
EOF
  2>/dev/null || true)"
  if [[ "$readiness_selector" == "true linux" ]]; then
    break
  fi
  sleep 1
done
if [[ "$readiness_selector" != "true linux" ]]; then
  echo "MutatingAdmissionPolicy did not add the ready selector while preserving the existing selector" >&2
  kubectl --context "$context" get mutatingadmissionpolicybindings cache-csi-driver-cache-readiness -o yaml >&2 || true
  exit 1
fi

for invalid_quantity in not-a-quantity 0 -1; do
  rejected=false
  for _ in $(seq 1 60); do
    if ! kubectl --context "$context" create --dry-run=server -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-api-validation-invalid-size
  namespace: default
spec:
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10.1
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: api-validation-invalid-size
          maxBytes: "$invalid_quantity"
EOF
    then
      rejected=true
      break
    fi
    sleep 1
  done
  if [[ "$rejected" != true ]]; then
    echo "Admission accepted invalid maxBytes quantity $invalid_quantity after waiting for policy activation" >&2
    kubectl --context "$context" get validatingadmissionpolicybindings -o yaml >&2 || true
    exit 1
  fi
done

if kubectl --context "$context" create --dry-run=server -f - >/dev/null 2>&1 <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: cache-api-validation-node-name
  namespace: default
spec:
  nodeName: must-be-rejected
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10.1
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: api-validation-node-name
EOF
then
  echo "Admission accepted a Cache CSI Pod with spec.nodeName on CREATE" >&2
  exit 1
fi

kubectl --context "$context" apply -f - <<EOF
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: $probe_driver
spec:
  attachRequired: false
  volumeLifecycleModes:
    - Persistent
  preventPodSchedulingIfMissing: true
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: cache-scheduler-missing-driver
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  csi:
    driver: $probe_driver
    volumeHandle: cache-scheduler-missing-driver
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cache-scheduler-missing-driver
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: ""
  volumeName: cache-scheduler-missing-driver
  resources:
    requests:
      storage: 1Gi
EOF
kubectl --context "$context" wait --for=jsonpath='{.status.phase}'=Bound pvc/cache-scheduler-missing-driver --namespace default --timeout=1m
if kubectl --context "$context" get csinodes -o jsonpath='{range .items[*]}{range .spec.drivers[*]}{.name}{"\n"}{end}{end}' | grep -Fxq "$probe_driver"; then
  echo "The scheduler probe CSI driver unexpectedly registered on a Node" >&2
  exit 1
fi

kubectl --context "$context" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-driver-missing-scheduling
  namespace: default
spec:
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10.1
  volumes:
    - name: cache
      persistentVolumeClaim:
        claimName: cache-scheduler-missing-driver
EOF
for _ in $(seq 1 30); do
  assigned_node="$(kubectl --context "$context" get pod cache-driver-missing-scheduling -o jsonpath='{.spec.nodeName}')"
  if [[ -n "$assigned_node" ]]; then
    echo "The default scheduler assigned a Cache CSI Pod to a Node without the registered driver" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$(kubectl --context "$context" get pod cache-driver-missing-scheduling -o jsonpath='{.status.phase}')" != Pending ]]; then
  echo "The Cache CSI Pod without a registered driver was not left Pending" >&2
  exit 1
fi
driver_scheduling_events="$(kubectl --context "$context" get events --namespace default --field-selector involvedObject.name=cache-driver-missing-scheduling -o go-template='{{range .items}}{{.reason}}{{"\t"}}{{.message}}{{"\n"}}{{end}}')"
if ! grep -Fq FailedScheduling <<<"$driver_scheduling_events" || ! grep -Fq "$probe_driver" <<<"$driver_scheduling_events"; then
  echo "The default scheduler did not identify the missing CSI driver as the scheduling failure" >&2
  printf '%s\n' "$driver_scheduling_events" >&2
  exit 1
fi

mapfile -t nodes < <(kubectl --context "$context" get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
if [[ ${#nodes[@]} -ne 2 ]]; then
  echo "Expected two Kind Nodes for cache readiness scheduling validation" >&2
  exit 1
fi
unavailable_node="${nodes[0]}"
healthy_node="${nodes[1]}"
kubectl --context "$context" label node "$unavailable_node" "$ready_label=true" --overwrite
kubectl --context "$context" label node "$unavailable_node" "$ready_label-"
kubectl --context "$context" apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: cache-inline-readiness-scheduling
  namespace: default
spec:
  containers:
    - name: pause
      image: registry.k8s.io/pause:3.10.1
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: inline-readiness-scheduling
EOF
inline_selector="$(kubectl --context "$context" get pod cache-inline-readiness-scheduling -o go-template='{{with .spec.nodeSelector}}{{index . "cache.csi.walnuts.dev/ready"}}{{end}}')"
if [[ "$inline_selector" != true ]]; then
  echo "MutatingAdmissionPolicy did not add the cache-ready selector to the inline CSI Pod" >&2
  exit 1
fi
sleep 10
if [[ -n "$(kubectl --context "$context" get pod cache-inline-readiness-scheduling -o jsonpath='{.spec.nodeName}')" ]]; then
  echo "The default scheduler assigned an inline cache Pod while no Node was labeled cache-ready" >&2
  exit 1
fi
kubectl --context "$context" label node "$healthy_node" "$ready_label=true" --overwrite
for _ in $(seq 1 30); do
  assigned_node="$(kubectl --context "$context" get pod cache-inline-readiness-scheduling -o jsonpath='{.spec.nodeName}')"
  if [[ -n "$assigned_node" ]]; then
    break
  fi
  sleep 1
done
if [[ "$assigned_node" != "$healthy_node" ]]; then
  echo "The inline cache Pod was not assigned to the only cache-ready Node" >&2
  kubectl --context "$context" describe pod cache-inline-readiness-scheduling --namespace default >&2 || true
  exit 1
fi

echo "Kubernetes 1.37 API validation, Admission CEL, and missing-driver scheduling checks passed"
