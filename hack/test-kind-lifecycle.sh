#!/usr/bin/env bash
set -euo pipefail

cluster_name="cache-csi-lifecycle-$$"
image="cache-csi-driver:e2e"
app_image="busybox:1.37.0"
tmp_dir="$(mktemp -d)"
selector='app.kubernetes.io/name=cache-csi-driver,app.kubernetes.io/instance=cache-csi-driver'

cleanup() {
  status=$?
  if [[ $status -ne 0 ]] && kind get clusters 2>/dev/null | grep -qx "$cluster_name"; then
    kubectl --context "kind-$cluster_name" get pods -A -o wide >&2 || true
    kubectl --context "kind-$cluster_name" get events -A --sort-by=.lastTimestamp >&2 || true
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

docker build --tag "$image" .
docker pull "$app_image"
kind load docker-image "$image" "$app_image" --name "$cluster_name"

helm upgrade --install cache-csi-driver deploy/helm/cache-csi-driver \
  --kube-context "$context" \
  --namespace kube-system \
  --set image.repository=cache-csi-driver \
  --set image.tag=e2e \
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
      command: ["sh", "-c", "if [ -f /cache/marker ]; then printf 'HIT:%s' \"$(cat /cache/marker)\" > /tmp/cache-result; else printf 'MISS' > /tmp/cache-result; printf '%s' '$name' > /cache/marker; fi; sleep 3600"]
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
  --set image.tag=e2e \
  --set image.pullPolicy=IfNotPresent \
  --wait \
  --timeout 5m
restart_plugin "$node_a"

start_pod pod-a "$node_a" default
expect_result pod-a MISS
delete_pod pod-a

start_pod pod-b "$node_a" default
expect_result pod-b HIT:pod-a

restart_plugin "$node_a"
kubectl --context "$context" exec --namespace default pod-b -- cat /cache/marker | grep -Fxq pod-a
delete_pod pod-b

start_pod pod-after-restart "$node_a" default
expect_result pod-after-restart HIT:pod-a
delete_pod pod-after-restart

start_pod pod-other-node "$node_b" default
expect_result pod-other-node MISS
delete_pod pod-other-node

start_pod pod-other-service-account "$node_a" isolated
expect_result pod-other-service-account MISS
delete_pod pod-other-service-account

echo "Kind cache lifecycle E2E passed on Kubernetes 1.37.0"
