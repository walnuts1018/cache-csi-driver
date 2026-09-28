# Cache CSI Driver

Cache CSI DriverはKubernetesのCSI inline ephemeral volumeとしてNode-local cache directoryを提供します。Podのvolume leaseはPod削除時に解除しますが、cache dataはNode上に残り、同じ`CacheClass`と`cacheKey`を指定する後続Podから再利用できます。

このdriverはKubernetes 1.37以降を対象とします。CSI inline volumeはbest-effort cacheであり、データの永続性、Node間共有、容量の予約を提供しません。Node障害、容量圧迫、CacheClassの保持期限、またはdriverのGCによりcacheはいつでも破棄されます。アプリケーションはcache miss後にデータを再生成できる必要があります。

## インストール

Helm OCI chartを使ってインストールします。

```sh
helm install cache-csi-driver \
  oci://ghcr.io/walnuts1018/charts/cache-csi-driver \
  --version 0.1.0 \
  --namespace kube-system
```

このchartは`CacheClass` CRD、`CSIDriver`、read-onlyのClusterRole、ServiceAccount、Node DaemonSetをインストールします。Node pluginはmount system callを使うためprivileged containerとして動作します。cacheは各Nodeの`/var/lib/cache-csi`に保存されます。保存先を変更する場合はHelm valuesの`cacheRootDir`を設定してください。この値はNode上のhostPathとpluginの引数の両方に反映されます。

Kubernetes APIからPod namespaceやCacheClassを解決できない場合、または通常cacheのmountに失敗した場合は、volume固有で共有されないfallback directoryを使います。fallbackデータはNode上の`/run/cache-csi/fallback`に配置します。標準的なLinuxでは`/run`はtmpfsですが、すべてのNode構成で保証されるわけではありません。fallbackデータをメモリ上に保つ場合は、`fallbackRootDir`をNode上のtmpfs内のパスに設定してください。この値はNode上のhostPathとpluginの引数の両方に反映されます。

`preventPodSchedulingIfMissing`を有効にしているため、CSI pluginが登録されていないNodeへのPod配置を防ぎます。Cluster Autoscalerを使う場合は、CSI node-aware schedulingを有効にしてください。

## CacheClassとPod volume

CacheClassはCluster-scopedです。例のCacheClassを適用します。

```sh
kubectl apply -f examples/cacheclass-default.yaml
```

PodではCSI inline volumeにCacheClass名とアプリケーション固有のcache keyを指定します。

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: cache-example
  namespace: default
spec:
  containers:
    - name: app
      image: busybox:1.37
      command: ["sh", "-c", "mkdir -p /cache && date > /cache/last-start && sleep 3600"]
      volumeMounts:
        - name: cache
          mountPath: /cache
  volumes:
    - name: cache
      csi:
        driver: cache.csi.walnuts.dev
        volumeAttributes:
          cacheClass: default
          cacheKey: application-model-v3
```

アプリケーションのPod templateを更新するときも、同じcacheを再利用する場合は`cacheKey`を維持してください。互換性のないcache formatへ変更するときはkeyか`CacheClass.spec.schemaVersion`を更新してください。Pod namespaceのUID、CacheClassのUID、cache key、schema versionから内部identityを作るため、namespaceやCacheClassを削除して作り直した場合に古いcacheを別のobjectとして誤って再利用しません。

サンプルは[examples](examples)にあります。

## 設定

主なHelm valuesは次の通りです。

| Value | Default | 説明 |
| --- | --- | --- |
| `image.repository` | `ghcr.io/walnuts1018/cache-csi-driver` | Node plugin image |
| `image.tag` | ChartのappVersion | Node plugin image tag |
| `driverName` | `cache.csi.walnuts.dev` | CSI driver name |
| `cacheRootDir` | `/var/lib/cache-csi` | Node上のcache directory |
| `fallbackRootDir` | `/run/cache-csi/fallback` | Node上のfallback専用volume directory。tmpfsを使う場合はtmpfs内のNode pathを指定 |
| `kubeletRootDir` | `/var/lib/kubelet` | kubeletのroot directory |
| `gcInterval` | `30s` | Node-local cache GC interval |
| `csiDriver.preventPodSchedulingIfMissing` | `true` | CSI pluginが未登録のNodeへの配置を防止 |
| `rbac.createPodEvictions` | `true` | `evictRunning`用のPod Eviction権限 |
| `nodeSelector` | `kubernetes.io/os: linux` | DaemonSetを配置するNode |

`CacheClass.spec.maxBytes`は、各cache identityに対してvolume側が要求できるquota上限です。`quota.enabled`と`backend: xfs-project`が必要で、volume attributeの`maxBytes`がこの上限を超える場合はmount要求を拒否します。volume attributeに`maxBytes`がない場合は`quota.defaultMaxBytes`を設定していればその値を使い、未設定なら`spec.maxBytes`を使います。quotaを有効にするCacheClassには`spec.maxBytes`または`quota.defaultMaxBytes`を設定してください。`examples/cacheclass-xfs-project.yaml`と`examples/pod-inline-cache-xfs-project.yaml`に設定例があります。

現在、class全体の集約byte上限はありません。`pressure`はNode-local cache filesystemの空き容量とinodeに対するGC水位を設定し、実際のfilesystem使用量に応じてcacheを回収します。`retention`は未使用cacheの保持期間、`crashRecovery`はNode再起動後に未完了だったgenerationの扱いです。`noExec`を有効にするとcache volumeを`noexec`でmountします。

`evictRunning`を有効にしたCacheClassでは、pressure GCが未使用cacheだけで不足すると、そのcacheを利用中のPodへKubernetes Eviction APIを要求します。PodDisruptionBudgetにより要求が拒否される場合は、Podとactive cacheを維持します。Node pluginのClusterRoleは全namespaceの`pods/eviction`作成だけを追加し、Podの直接削除権限は持ちません。active Pod evictionを使わない場合は`rbac.createPodEvictions=false`にできます。

`quota.enabled`を使うCacheClassでは`backend: xfs-project`を指定し、`cacheRootDir`がproject quota有効のXFS filesystem上にあることを確認してください。Node imageには`xfs_quota`を含めていますが、host filesystemのmount optionやquota設定は管理者が行います。

## 開発とリリース

開発ツールはmiseで管理します。主なtaskは`mise run build`、`mise run test`、`mise run lint:chart`、`mise run package:chart`です。Helm chartとCRDの定義は`deploy/helm/cache-csi-driver`と`config/crd/bases`にあります。

リリースはGitHub Actionsの`Release` workflowを手動実行し、SemVer形式のtagを指定します。workflowはテストとchart lintを行い、linux/amd64およびlinux/arm64のNode plugin image、Helm OCI chartをGHCRへ公開し、imageとchartのprovenance attestationを生成してGitHub Releaseを作成します。
