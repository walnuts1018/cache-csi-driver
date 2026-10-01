#!/usr/bin/env bash
set -euo pipefail

cluster_name="cache-csi-lifecycle-$$"
image_tag="e2e-$$"
image="cache-csi-driver:$image_tag"
app_image="busybox:1.37.0"
tmp_dir="$(mktemp -d)"
selector='app.kubernetes.io/name=cache-csi-driver,app.kubernetes.io/instance=cache-csi-driver,app.kubernetes.io/component=node'
daemonset_selector='app.kubernetes.io/name=cache-csi-driver,app.kubernetes.io/instance=cache-csi-driver'
test_node_label='e2e.test.walnuts.dev/cache-node'
failed_node=""
readonly_node=""

cleanup() {
  status=$?
  if [[ $status -ne 0 ]] && kind get clusters 2>/dev/null | grep -qx "$cluster_name"; then
    kubectl --context "kind-$cluster_name" get pods -A -o wide >&2 || true
    kubectl --context "kind-$cluster_name" get events -A --sort-by=.lastTimestamp >&2 || true
    for pod in $(kubectl --context "kind-$cluster_name" get pods --namespace kube-system -l "$selector" -o name 2>/dev/null || true); do
      kubectl --context "kind-$cluster_name" logs --namespace kube-system "$pod" -c cache-csi-node >&2 || true
    done
  fi
  if [[ -n "$failed_node" ]]; then
    docker start "$failed_node" >/dev/null 2>&1 || true
  fi
  if [[ -n "$readonly_node" ]]; then
    docker exec "$readonly_node" mount -o remount,rw /var/lib/cache-csi >/dev/null 2>&1 || true
  fi
  kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT

cat > "$tmp_dir/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF

kind create cluster \
  --name "$cluster_name" \
  --image kindest/node:v1.37.0 \
  --config "$tmp_dir/kind.yaml" \
  --wait 5m
context="kind-$cluster_name"
kubectl --context "$context" wait --for=condition=Ready nodes --all --timeout=3m
while IFS= read -r node_container; do
  docker exec "$node_container" mkdir -p /var/lib/cache-csi
  docker exec "$node_container" mount -t tmpfs -o size=512m,mode=0700 cache-csi-e2e /var/lib/cache-csi
done < <(kind get nodes --name "$cluster_name")

go_arch="$(go env GOARCH)"
docker build --platform "linux/$go_arch" --provenance=false --tag "$image" .
docker save --output "$tmp_dir/kind-images.tar" "$image"
kind load image-archive "$tmp_dir/kind-images.tar" --name "$cluster_name"

helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 2m

helm template cache-csi-driver deploy/helm/cache-csi-driver \
  --namespace kube-system \
  --include-crds \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  | kubectl --context "$context" apply --dry-run=server --validate=strict -f -
wait_for_admission_policies() {
  local status
  for _ in $(seq 1 60); do
    status="$(kubectl --context "$context" get validatingadmissionpolicies -o=go-template='{{range .items}}{{.metadata.generation}}{{"\t"}}{{.status.observedGeneration}}{{"\t"}}{{printf "%#v" .status.typeChecking}}{{"\t"}}{{range .status.typeChecking.expressionWarnings}}{{.fieldRef}}{{": "}}{{.warning}}{{end}}{{"\n"}}{{end}}')"
    if [[ -n "$status" ]] && printf '%s\n' "$status" | awk -F '\t' 'NF != 4 || $1 != $2 || $3 == "<nil>" || $4 != "" { failed = 1 } END { exit failed }'; then
      return
    fi
    sleep 1
  done
  kubectl --context "$context" get validatingadmissionpolicies -o yaml >&2
  return 1
}
wait_for_admission_policies
if [[ "$(kubectl --context "$context" get csidriver cache.csi.walnuts.dev -o go-template='{{.spec.preventPodSchedulingIfMissing}}')" != true ]]; then
  echo "CSIDriver does not enable preventPodSchedulingIfMissing" >&2
  exit 1
fi

kubectl --context "$context" label namespace default cache.csi.walnuts.dev/allow-use=true --overwrite
kubectl --context "$context" apply -f examples/cacheclass-default.yaml
kubectl --context "$context" create serviceaccount isolated

mapfile -t workers < <(kubectl --context "$context" get nodes -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
if [[ ${#workers[@]} -ne 2 ]]; then
  echo "Expected two Kind worker nodes, found ${#workers[@]}" >&2
  exit 1
fi
node_a="${workers[0]}"
node_b="${workers[1]}"
kubectl --context "$context" label node "$node_a" "$test_node_label=$node_a" --overwrite
kubectl --context "$context" label node "$node_b" "$test_node_label=$node_b" --overwrite

admission_result="$(kubectl --context "$context" create --dry-run=server -f - -o go-template='{{index .spec.nodeSelector "cache.csi.walnuts.dev/ready"}} {{.spec.schedulerName}}' <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-admission-positive
  namespace: default
spec:
  schedulerName: custom-scheduler
  nodeSelector:
    kubernetes.io/os: linux
  containers:
    - name: check
      image: $app_image
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: admission-positive
          maxBytes: 1Mi
EOF
)"
if [[ "$admission_result" != "true custom-scheduler" ]]; then
  echo "Cache Admission did not preserve schedulerName and add the cache readiness selector: $admission_result" >&2
  exit 1
fi

if kubectl --context "$context" create --dry-run=server -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-admission-node-name
  namespace: default
spec:
  nodeName: $node_a
  containers:
    - name: check
      image: $app_image
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: admission-node-name
EOF
then
  echo "Cache Admission accepted spec.nodeName on Pod CREATE" >&2
  exit 1
fi

if kubectl --context "$context" create --dry-run=server -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-admission-invalid-size
  namespace: default
spec:
  containers:
    - name: check
      image: $app_image
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: admission-invalid-size
          maxBytes: not-a-quantity
EOF
then
  echo "Cache Admission accepted malformed maxBytes" >&2
  exit 1
fi

if kubectl --context "$context" create --dry-run=server -f - >/dev/null 2>&1 <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: cache-admission-zero-size
  namespace: default
spec:
  containers:
    - name: check
      image: $app_image
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: admission-zero-size
          maxBytes: "0"
EOF
then
  echo "Cache Admission accepted zero maxBytes" >&2
  exit 1
fi

wait_for_cache_ready() {
  local node="$1" ready
  for _ in $(seq 1 180); do
    ready="$(kubectl --context "$context" get node "$node" -o go-template='{{index .metadata.labels "cache.csi.walnuts.dev/ready"}}' 2>/dev/null || true)"
    if [[ "$ready" == true ]]; then
      return
    fi
    sleep 1
  done
  echo "Cache CSI did not mark node $node ready within 3 minutes" >&2
  kubectl --context "$context" get node "$node" --show-labels >&2 || true
  exit 1
}

wait_for_cache_not_ready() {
  local node="$1" ready
  for _ in $(seq 1 60); do
    ready="$(kubectl --context "$context" get node "$node" -o go-template='{{index .metadata.labels "cache.csi.walnuts.dev/ready"}}' 2>/dev/null || true)"
    if [[ "$ready" != true ]]; then
      return
    fi
    sleep 1
  done
  echo "Cache CSI kept node $node ready after its plugin became unavailable" >&2
  kubectl --context "$context" get node "$node" --show-labels >&2 || true
  exit 1
}

wait_for_cache_ready "$node_a"
wait_for_cache_ready "$node_b"

write_pod() {
  local name="$1" node="$2" service_account="$3" cache_key="${4:-lifecycle-e2e}"
  cat > "$tmp_dir/$name.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: default
spec:
  nodeSelector:
    $test_node_label: $node
  serviceAccountName: $service_account
  restartPolicy: Never
  containers:
    - name: check
      image: $app_image
      imagePullPolicy: IfNotPresent
      command: ["sh", "-c", "if [ -f /cache/marker ]; then printf 'HIT' > /tmp/cache-result; else printf 'MISS' > /tmp/cache-result; printf 'cached' > /cache/marker; fi; sleep 3600"]
      volumeMounts:
        - name: cache
          mountPath: /cache
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: $cache_key
EOF
}

start_pod() {
  local name="$1" node="$2" service_account="$3" cache_key="${4:-lifecycle-e2e}"
  write_pod "$name" "$node" "$service_account" "$cache_key"
  kubectl --context "$context" apply -f "$tmp_dir/$name.yaml"
  kubectl --context "$context" wait --for=condition=Ready "pod/$name" --namespace default --timeout=3m
}

restart_plugin() {
  local node="$1" plugin_pod
  plugin_pod="$(kubectl --context "$context" get pods --namespace kube-system -l "$selector" --field-selector "spec.nodeName=$node" -o jsonpath='{.items[0].metadata.name}')"
  kubectl --context "$context" delete pod --namespace kube-system "$plugin_pod" --wait=true --timeout=2m
  kubectl --context "$context" wait --for=condition=Ready pod --namespace kube-system -l "$selector" --field-selector "spec.nodeName=$node" --timeout=3m
}

expect_result() {
  local name="$1" expected="$2" actual
  for _ in $(seq 1 30); do
    actual="$(kubectl --context "$context" exec --namespace default "$name" -- cat /tmp/cache-result 2>/dev/null || true)"
    if [[ "$actual" == "$expected" ]]; then
      return
    fi
    sleep 1
  done
  echo "Pod $name reported cache result '$actual', expected '$expected':" >&2
  printf '%s\n' "$actual" >&2
  exit 1
}

delete_pod() {
  kubectl --context "$context" delete pod --namespace default "$1" --wait=true --timeout=2m
}

wait_for_replacement_pod() {
  local previous_pod="$1" failed_node="$2" pod node phase
  for _ in $(seq 1 360); do
    pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-node-failure -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | awk -v previous="$previous_pod" '$0 != previous && NF { print; exit }')"
    if [[ -n "$pod" ]]; then
      node="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
      phase="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      if [[ -n "$node" && "$node" != "$failed_node" && "$phase" == Running ]]; then
        if kubectl --context "$context" wait --for=condition=Ready "pod/$pod" --namespace default --timeout=1s >/dev/null 2>&1; then
          printf '%s\n' "$pod"
          return
        fi
      fi
    fi
    sleep 1
  done
  echo "A replacement Pod was not ready on a different worker within 6 minutes" >&2
  kubectl --context "$context" get pods --namespace default -l app=cache-node-failure -o wide >&2 || true
  exit 1
}

wait_for_health_replacement() {
  local previous_pod="$1" unavailable_node="$2" pod node phase
  for _ in $(seq 1 180); do
    pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-node-health -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | awk -v previous="$previous_pod" '$0 != previous && NF { print; exit }')"
    if [[ -n "$pod" ]]; then
      node="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
      phase="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      if [[ -n "$node" && "$node" != "$unavailable_node" && "$phase" == Running ]]; then
        if kubectl --context "$context" wait --for=condition=Ready "pod/$pod" --namespace default --timeout=1s >/dev/null 2>&1; then
          printf '%s\n' "$pod"
          return
        fi
      fi
    fi
    sleep 1
  done
  echo "The cache workload was not replaced on a healthy worker within 3 minutes" >&2
  kubectl --context "$context" get pods --namespace default -l app=cache-node-health -o wide >&2 || true
  exit 1
}

wait_for_managed_replacement() {
  local label_selector="$1" previous_pod="$2" unavailable_node="$3" pod node phase
  for _ in $(seq 1 180); do
    pod="$(kubectl --context "$context" get pods --namespace default -l "$label_selector" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | awk -v previous="$previous_pod" '$0 != previous && NF { print; exit }')"
    if [[ -n "$pod" ]]; then
      node="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
      phase="$(kubectl --context "$context" get pod --namespace default "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      if [[ -n "$node" && "$node" != "$unavailable_node" && "$phase" == Running ]]; then
        if kubectl --context "$context" wait --for=condition=Ready "pod/$pod" --namespace default --timeout=1s >/dev/null 2>&1; then
          printf '%s\n' "$pod"
          return
        fi
      fi
    fi
    sleep 1
  done
  echo "The health controller did not relocate a managed Pending cache Pod to a healthy Node" >&2
  kubectl --context "$context" get pods --namespace default -l "$label_selector" -o wide >&2 || true
  exit 1
}

wait_for_node_not_ready() {
  local node="$1" condition
  for _ in $(seq 1 180); do
    condition="$(kubectl --context "$context" get node "$node" -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    if [[ "$condition" == False || "$condition" == Unknown ]]; then
      return
    fi
    sleep 1
  done
  echo "Node $node did not become NotReady within 3 minutes" >&2
  exit 1
}

wait_for_pressure_config() {
  local node="$1" args
  for _ in $(seq 1 180); do
    args="$(kubectl --context "$context" get pods --namespace kube-system -l "$selector" --field-selector "spec.nodeName=$node" -o jsonpath='{.items[0].spec.containers[0].args}' 2>/dev/null || true)"
    if [[ "$args" == *"--pressure-high-free-percent=100"* && "$args" == *"--pressure-low-free-percent=99"* ]]; then
      return
    fi
    sleep 1
  done
  echo "Node plugin did not receive the pressure test configuration on $node" >&2
  kubectl --context "$context" get pods --namespace kube-system -l "$selector" --field-selector "spec.nodeName=$node" -o wide >&2 || true
  exit 1
}

if [[ "$(kubectl --context "$context" auth can-i list cacheclasses.cache.storage.walnuts.dev --as=system:serviceaccount:kube-system:cache-csi-driver)" != yes ]]; then
  echo "The node plugin ServiceAccount cannot read CacheClasses" >&2
  exit 1
fi
if [[ "$(kubectl --context "$context" auth can-i patch nodes --as=system:serviceaccount:kube-system:cache-csi-driver)" != no ]]; then
  echo "The node plugin ServiceAccount can patch Nodes" >&2
  exit 1
fi
if [[ "$(kubectl --context "$context" auth can-i create pods/eviction --namespace kube-system --as=system:serviceaccount:kube-system:cache-csi-driver)" != no ]]; then
  echo "The node plugin ServiceAccount can evict Pods" >&2
  exit 1
fi
helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 5m
restart_plugin "$node_a"

start_pod pod-a "$node_a" default
expect_result pod-a MISS
delete_pod pod-a

start_pod pod-b "$node_a" default
expect_result pod-b HIT

restart_plugin "$node_a"
kubectl --context "$context" exec --namespace default pod-b -- cat /cache/marker | grep -Fxq cached
delete_pod pod-b

start_pod pod-after-restart "$node_a" default
expect_result pod-after-restart HIT
delete_pod pod-after-restart

node_container="$(kind get nodes --name "$cluster_name" | awk -v node="$node_a" '$0 == node { print; exit }')"
cache_metadata="$(docker exec "$node_container" sh -c "find /var/lib/cache-csi -maxdepth 4 -type f -name .cache-csi.json -printf '%T@ %p\\n' | sort -nr | head -n1 | cut -d' ' -f2-")"
if [[ -z "$cache_metadata" ]]; then
  echo "Could not find cache metadata on Node $node_a" >&2
  exit 1
fi
docker exec "$node_container" sh -c "printf '%s' '{broken' > '$cache_metadata'"
start_pod pod-after-metadata-corruption "$node_a" default
expect_result pod-after-metadata-corruption MISS
delete_pod pod-after-metadata-corruption

start_pod pod-corrupt-mounted-old "$node_a" default lifecycle-corrupt-mounted
expect_result pod-corrupt-mounted-old MISS
mounted_metadata="$(docker exec "$node_container" sh -c "find /var/lib/cache-csi -maxdepth 4 -type f -name .cache-csi.json -printf '%T@ %p\\n' | sort -nr | head -n1 | cut -d' ' -f2-")"
if [[ -z "$mounted_metadata" ]]; then
  echo "Could not find metadata for the mounted cache object on Node $node_a" >&2
  exit 1
fi
docker exec "$node_container" sh -c "printf '%s' '{broken' > '$mounted_metadata'"
start_pod pod-corrupt-mounted-new "$node_a" default lifecycle-corrupt-mounted
expect_result pod-corrupt-mounted-new MISS
if ! kubectl --context "$context" wait --for=condition=Ready pod/pod-corrupt-mounted-old --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Quarantining a mounted cache object disrupted its existing Pod" >&2
  exit 1
fi
kubectl --context "$context" exec --namespace default pod-corrupt-mounted-old -- cat /cache/marker | grep -Fxq cached
delete_pod pod-corrupt-mounted-new
delete_pod pod-corrupt-mounted-old

start_pod pod-other-node "$node_b" default
expect_result pod-other-node MISS
delete_pod pod-other-node

start_pod pod-other-service-account "$node_a" isolated
expect_result pod-other-service-account MISS
delete_pod pod-other-service-account

cat > "$tmp_dir/node-health-deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cache-node-health
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cache-node-health
  template:
    metadata:
      labels:
        app: cache-node-health
    spec:
      serviceAccountName: default
      containers:
        - name: check
          image: $app_image
          imagePullPolicy: IfNotPresent
          command: ["sh", "-c", "sleep 3600"]
          volumeMounts:
            - name: cache
              mountPath: /cache
      volumes:
        - name: cache
          csi:
            driver: cache.csi.walnuts.dev
            volumeAttributes:
              cacheClass: default
              cacheKey: lifecycle-node-health
EOF
kubectl --context "$context" apply -f "$tmp_dir/node-health-deployment.yaml"
kubectl --context "$context" rollout status deployment/cache-node-health --namespace default --timeout=3m
health_pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-node-health -o jsonpath='{.items[0].metadata.name}')"
health_node="$(kubectl --context "$context" get pod --namespace default "$health_pod" -o jsonpath='{.spec.nodeName}')"
health_node_container="$(kind get nodes --name "$cluster_name" | awk -v node="$health_node" '$0 == node { print; exit }')"
if [[ -z "$health_node_container" ]]; then
  echo "Could not find the Kind container for Node $health_node" >&2
  exit 1
fi
start_pod pod-pressure-unused "$health_node" default lifecycle-pressure-unused
expect_result pod-pressure-unused MISS
delete_pod pod-pressure-unused
unused_cache_metadata="$(docker exec "$health_node_container" sh -c "find /var/lib/cache-csi -mindepth 2 -maxdepth 2 -type f -name .cache-csi.json -printf '%T@ %p\\n' | sort -nr | head -n1 | cut -d' ' -f2-")"
if [[ -z "$unused_cache_metadata" ]]; then
  echo "Could not find metadata for the unused pressure cache on Node $health_node" >&2
  exit 1
fi
start_pod pod-plugin-unavailable-existing "$health_node" default
docker exec "$health_node_container" sh -c 'dd if=/dev/zero of=/var/lib/cache-csi/.pressure-e2e bs=1M count=460 conv=fsync status=none'
driver_daemonset="$(kubectl --context "$context" get daemonsets --namespace kube-system -l "$daemonset_selector" -o jsonpath='{.items[0].metadata.name}')"
kubectl --context "$context" patch daemonset "$driver_daemonset" --namespace kube-system --type=json --patch '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pressure-high-free-percent=100"},{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--pressure-low-free-percent=99"}]'
wait_for_pressure_config "$health_node"
wait_for_cache_not_ready "$health_node"
sleep 8
if ! kubectl --context "$context" wait --for=condition=Ready "pod/$health_pod" --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Filesystem pressure evicted an established cache workload" >&2
  exit 1
fi
if ! kubectl --context "$context" wait --for=condition=Ready pod/pod-plugin-unavailable-existing --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Filesystem pressure evicted an unmanaged cache Pod" >&2
  exit 1
fi
if docker exec "$health_node_container" test -e "$unused_cache_metadata"; then
  echo "Pressure reclaim left an unused cache at its canonical path" >&2
  exit 1
fi
docker exec "$health_node_container" rm -f /var/lib/cache-csi/.pressure-e2e
kubectl --context "$context" patch daemonset "$driver_daemonset" --namespace kube-system --type=json --patch '[{"op":"remove","path":"/spec/template/spec/containers/0/args/15"},{"op":"remove","path":"/spec/template/spec/containers/0/args/14"}]'
kubectl --context "$context" rollout status daemonset "$driver_daemonset" --namespace kube-system --timeout=3m
wait_for_cache_ready "$health_node"
wait_for_cache_ready "$node_a"
wait_for_cache_ready "$node_b"

if [[ "$health_node" == "$node_a" ]]; then
  healthy_node="$node_b"
else
  healthy_node="$node_a"
fi
daemonset_node_selector="$(printf '{\"kubernetes.io/os\":\"linux\",\"%s\":\"%s\"}' "$test_node_label" "$healthy_node")"
helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  --set-json "nodeSelector=$daemonset_node_selector" \
  --wait \
  --timeout 3m
wait_for_cache_ready "$healthy_node"
for _ in $(seq 1 60); do
  remaining_plugin="$(kubectl --context "$context" get pods --namespace kube-system -l "$selector" --field-selector "spec.nodeName=$health_node" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$remaining_plugin" ]]; then
    break
  fi
  sleep 1
done
if [[ -n "$remaining_plugin" ]]; then
  echo "Node plugin remained scheduled on $health_node after restricting the DaemonSet" >&2
  exit 1
fi
wait_for_cache_not_ready "$health_node"
if ! kubectl --context "$context" wait --for=condition=Ready "pod/$health_pod" --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Plugin disappearance disrupted an established cache workload" >&2
  exit 1
fi
if ! kubectl --context "$context" wait --for=condition=Ready pod/pod-plugin-unavailable-existing --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Plugin disappearance disrupted an established bare cache Pod" >&2
  exit 1
fi
write_pod pod-plugin-unavailable-pending "$health_node" default lifecycle-plugin-unavailable
kubectl --context "$context" apply -f "$tmp_dir/pod-plugin-unavailable-pending.yaml"
sleep 5
pending_node="$(kubectl --context "$context" get pod pod-plugin-unavailable-pending --namespace default -o jsonpath='{.spec.nodeName}')"
if [[ -n "$pending_node" ]]; then
  echo "The default scheduler bound a cache Pod to Node $health_node after its plugin disappeared" >&2
  exit 1
fi
delete_pod pod-plugin-unavailable-pending
helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 5m
wait_for_cache_ready "$health_node"

cat > "$tmp_dir/cache-node-health-pdb.yaml" <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: cache-node-health
  namespace: default
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: cache-node-health
EOF
kubectl --context "$context" apply -f "$tmp_dir/cache-node-health-pdb.yaml"
pdb_healthy=""
pdb_disruptions=""
for _ in $(seq 1 30); do
  pdb_healthy="$(kubectl --context "$context" get pdb cache-node-health --namespace default -o jsonpath='{.status.currentHealthy}' 2>/dev/null || true)"
  pdb_disruptions="$(kubectl --context "$context" get pdb cache-node-health --namespace default -o jsonpath='{.status.disruptionsAllowed}' 2>/dev/null || true)"
  if [[ "$pdb_healthy" == 1 && "$pdb_disruptions" == 0 ]]; then
    break
  fi
  sleep 1
done
if [[ "$pdb_healthy" != 1 || "$pdb_disruptions" != 0 ]]; then
  echo "PodDisruptionBudget did not reach currentHealthy=1 and disruptionsAllowed=0" >&2
  kubectl --context "$context" get pdb cache-node-health --namespace default -o yaml >&2 || true
  exit 1
fi

health_controller_deployment=cache-csi-driver-health-controller
kubectl --context "$context" scale deployment "$health_controller_deployment" --namespace kube-system --replicas=0
for _ in $(seq 1 60); do
  health_controller_pods="$(kubectl --context "$context" get pods --namespace kube-system -l app.kubernetes.io/component=health-controller -o name)"
  if [[ -z "$health_controller_pods" ]]; then
    break
  fi
  sleep 1
done
if [[ -n "$health_controller_pods" ]]; then
  echo "Health controller Pods did not stop before the scheduling race test" >&2
  exit 1
fi
docker exec "$health_node_container" mount -o remount,ro /var/lib/cache-csi
readonly_node="$health_node_container"
kubectl --context "$context" label node "$healthy_node" "$test_node_label-" --overwrite
kubectl --context "$context" label node "$health_node" "$test_node_label=race-target" --overwrite
cat > "$tmp_dir/deployment-readonly-scheduling-race.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cache-readonly-scheduling-race
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cache-readonly-scheduling-race
  template:
    metadata:
      labels:
        app: cache-readonly-scheduling-race
    spec:
      serviceAccountName: default
      nodeSelector:
        $test_node_label: race-target
      containers:
        - name: check
          image: $app_image
          imagePullPolicy: IfNotPresent
          command: ["sh", "-c", "if [ -f /cache/marker ]; then printf 'HIT' > /tmp/cache-result; else printf 'MISS' > /tmp/cache-result; printf 'cached' > /cache/marker; fi; sleep 3600"]
          volumeMounts:
            - name: cache
              mountPath: /cache
      volumes:
        - name: cache
          csi:
            driver: cache.csi.walnuts.dev
            volumeAttributes:
              cacheClass: default
              cacheKey: lifecycle-readonly-scheduling-race
EOF
kubectl --context "$context" apply -f "$tmp_dir/deployment-readonly-scheduling-race.yaml"
race_pod=""
race_node=""
for _ in $(seq 1 30); do
  race_pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-readonly-scheduling-race -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [[ -z "$race_pod" ]]; then
    sleep 1
    continue
  fi
  race_node="$(kubectl --context "$context" get pod "$race_pod" --namespace default -o jsonpath='{.spec.nodeName}')"
  if [[ "$race_node" == "$health_node" ]]; then
    break
  fi
  sleep 1
done
if [[ "$race_node" != "$health_node" ]]; then
  echo "The managed scheduling race Pod did not bind to the Node whose ready label was stale" >&2
  kubectl --context "$context" get pods --namespace default -l app=cache-readonly-scheduling-race -o wide >&2 || true
  exit 1
fi
mount_failure=""
for _ in $(seq 1 60); do
  mount_failure="$(kubectl --context "$context" get events --namespace default --field-selector "involvedObject.kind=Pod,involvedObject.name=$race_pod" -o jsonpath='{range .items[*]}{.reason}{"\t"}{.message}{"\n"}{end}')"
  if grep -Fq 'FailedMount' <<<"$mount_failure"; then
    break
  fi
  sleep 1
done
if ! grep -Fq 'FailedMount' <<<"$mount_failure"; then
  echo "NodePublish did not fail after the cache root became read-only while the Node label was stale" >&2
  kubectl --context "$context" describe pod "$race_pod" --namespace default >&2 || true
  exit 1
fi
kubectl --context "$context" label node "$health_node" "$test_node_label-" --overwrite
kubectl --context "$context" label node "$healthy_node" "$test_node_label=race-target" --overwrite
kubectl --context "$context" scale deployment "$health_controller_deployment" --namespace kube-system --replicas=2
kubectl --context "$context" rollout status deployment "$health_controller_deployment" --namespace kube-system --timeout=3m
wait_for_cache_not_ready "$health_node"
race_replacement="$(wait_for_managed_replacement app=cache-readonly-scheduling-race "$race_pod" "$health_node")"
expect_result "$race_replacement" MISS
sleep 8
current_health_pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-node-health -o jsonpath='{.items[0].metadata.name}')"
if [[ "$current_health_pod" != "$health_pod" ]] || ! kubectl --context "$context" wait --for=condition=Ready "pod/$health_pod" --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "A PodDisruptionBudget did not prevent eviction while the cache root was read-only" >&2
  kubectl --context "$context" get pods,pdb --namespace default -l app=cache-node-health -o wide >&2 || true
  exit 1
fi
if ! kubectl --context "$context" wait --for=condition=Ready pod/pod-plugin-unavailable-existing --namespace default --timeout=1s >/dev/null 2>&1; then
  echo "Health controller evicted an unmanaged Pod from an unavailable Node" >&2
  kubectl --context "$context" get pods --namespace default -o wide >&2 || true
  exit 1
fi
kubectl --context "$context" delete pdb cache-node-health --namespace default --wait=true --timeout=2m
health_replacement="$(wait_for_health_replacement "$health_pod" "$health_node")"
health_replacement_node="$(kubectl --context "$context" get pod --namespace default "$health_replacement" -o jsonpath='{.spec.nodeName}')"
if [[ "$health_replacement_node" == "$health_node" ]]; then
  echo "Replacement Pod was scheduled back to the read-only cache Node $health_node" >&2
  exit 1
fi
docker exec "$health_node_container" mount -o remount,rw /var/lib/cache-csi
readonly_node=""
wait_for_cache_ready "$health_node"
delete_pod pod-plugin-unavailable-existing
kubectl --context "$context" delete deployment cache-readonly-scheduling-race --namespace default --wait=true --timeout=2m
kubectl --context "$context" delete deployment cache-node-health --namespace default --wait=true --timeout=2m
echo "Read-only cache root removed readiness, respected PDB and unmanaged Pod ownership, and recovered automatically on $health_node"

cat > "$tmp_dir/node-failure-deployment.yaml" <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cache-node-failure
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cache-node-failure
  template:
    metadata:
      labels:
        app: cache-node-failure
    spec:
      serviceAccountName: default
      tolerations:
        - key: node.kubernetes.io/not-ready
          operator: Exists
          effect: NoExecute
          tolerationSeconds: 5
        - key: node.kubernetes.io/unreachable
          operator: Exists
          effect: NoExecute
          tolerationSeconds: 5
      containers:
        - name: check
          image: $app_image
          imagePullPolicy: IfNotPresent
          command: ["sh", "-c", "if [ -f /cache/marker ]; then printf 'HIT' > /tmp/cache-result; else printf 'MISS' > /tmp/cache-result; printf 'cached' > /cache/marker; fi; sleep 3600"]
          volumeMounts:
            - name: cache
              mountPath: /cache
      volumes:
        - name: cache
          csi:
            driver: cache.csi.walnuts.dev
            volumeAttributes:
              cacheClass: default
              cacheKey: lifecycle-node-failure
EOF
kubectl --context "$context" apply -f "$tmp_dir/node-failure-deployment.yaml"
kubectl --context "$context" rollout status deployment/cache-node-failure --namespace default --timeout=3m
failure_pod="$(kubectl --context "$context" get pods --namespace default -l app=cache-node-failure -o jsonpath='{.items[0].metadata.name}')"
failure_node="$(kubectl --context "$context" get pod --namespace default "$failure_pod" -o jsonpath='{.spec.nodeName}')"
if [[ "$failure_node" != "$node_a" && "$failure_node" != "$node_b" ]]; then
  echo "Scheduler placed the Deployment Pod on unexpected node $failure_node" >&2
  exit 1
fi
expect_result "$failure_pod" MISS
docker kill "$failure_node" >/dev/null
failed_node="$failure_node"
wait_for_node_not_ready "$failure_node"
replacement_pod="$(wait_for_replacement_pod "$failure_pod" "$failure_node")"
expect_result "$replacement_pod" MISS
replacement_node="$(kubectl --context "$context" get pod --namespace default "$replacement_pod" -o jsonpath='{.spec.nodeName}')"
if [[ "$replacement_node" == "$failure_node" ]]; then
  echo "Replacement Pod was scheduled back to failed node $failure_node" >&2
  exit 1
fi
docker start "$failure_node" >/dev/null
failed_node=""
echo "Scheduler-managed replacement after node failure passed on $replacement_node"

echo "Kind cache lifecycle E2E passed on Kubernetes 1.37.0"
