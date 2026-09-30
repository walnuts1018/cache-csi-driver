#!/usr/bin/env bash
set -euo pipefail

cluster_name="cache-csi-lifecycle-$$"
image_tag="e2e-$$"
image="cache-csi-driver:$image_tag"
app_image="busybox:1.37.0"
tmp_dir="$(mktemp -d)"
selector='app.kubernetes.io/name=cache-csi-driver,app.kubernetes.io/instance=cache-csi-driver'
failed_node=""

cleanup() {
  status=$?
  if [[ $status -ne 0 ]] && kind get clusters 2>/dev/null | grep -qx "$cluster_name"; then
    kubectl --context "kind-$cluster_name" get pods -A -o wide >&2 || true
    kubectl --context "kind-$cluster_name" get events -A --sort-by=.lastTimestamp >&2 || true
  fi
  if [[ -n "$failed_node" ]]; then
    docker start "$failed_node" >/dev/null 2>&1 || true
  fi
  kind delete cluster --name "$cluster_name" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
  exit "$status"
}
trap cleanup EXIT

kind create cluster \
  --name "$cluster_name" \
  --image kindest/node:v1.37.0 \
  --config hack/kind-lifecycle.yaml \
  --wait 5m
context="kind-$cluster_name"
kubectl --context "$context" wait --for=condition=Ready nodes --all --timeout=3m

go_arch="$(go env GOARCH)"
docker build --platform "linux/$go_arch" --provenance=false --tag "$image" .
docker pull "$app_image"
docker save --output "$tmp_dir/kind-images.tar" "$image" "$app_image"
kind load image-archive "$tmp_dir/kind-images.tar" --name "$cluster_name"

helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag="$image_tag" \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 5m

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

write_pod() {
  local name="$1" node="$2" service_account="$3"
  cat > "$tmp_dir/$name.yaml" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: default
spec:
  nodeName: $node
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
          cacheKey: lifecycle-e2e
EOF
}

start_pod() {
  local name="$1" node="$2" service_account="$3"
  write_pod "$name" "$node" "$service_account"
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

role_binding=cache-csi-driver-cacheclass-reader
kubectl --context "$context" delete clusterrolebinding "$role_binding"
if [[ "$(kubectl --context "$context" auth can-i list cacheclasses.cache.storage.walnuts.dev --as=system:serviceaccount:kube-system:cache-csi-driver)" != no ]]; then
  echo "The node plugin ServiceAccount still has permission to list CacheClasses after removing its ClusterRoleBinding" >&2
  exit 1
fi
restart_plugin "$node_a"
start_pod pod-informer-cold-start "$node_a" default
expect_result pod-informer-cold-start MISS
delete_pod pod-informer-cold-start
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

start_pod pod-other-node "$node_b" default
expect_result pod-other-node MISS
delete_pod pod-other-node

start_pod pod-other-service-account "$node_a" isolated
expect_result pod-other-service-account MISS
delete_pod pod-other-service-account

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
