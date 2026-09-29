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

このchartは`CacheClass` CRD、`CSIDriver`、ValidatingAdmissionPolicy、ClusterRole、ServiceAccount、Node DaemonSetをインストールします。ValidatingAdmissionPolicyは、namespaceに`cache.csi.walnuts.dev/allow-use=true`ラベルがない場合にCache CSI volumeを使うPodを拒否します。CacheClassを利用させるnamespaceには、namespaceラベルを変更できる利用者を管理した上で次のようにラベルを付けてください。

```sh
kubectl label namespace default cache.csi.walnuts.dev/allow-use=true
```

Helm valuesの`admissionPolicy.enabled=false`でpolicyを無効化する場合は、同等の利用制限を別のAdmission設定で行ってください。ClusterRoleはNamespace、CacheClass、Node上のPodとServiceAccountの参照権限を持ち、既定で`pods/eviction`の作成権限を持ちます。Pod informerは現在のNodeに割り当てられたPodだけをwatchします。Node pluginはmount system callを使うためprivileged containerとして動作し、mount属性付きmountの伝播にLinux kernel 5.12以降が必要です。cacheは各Nodeの`/var/lib/cache-csi`に保存されます。保存先を変更する場合はHelm valuesの`cacheRootDir`を設定してください。この値はNode上のhostPathとpluginの引数の両方に反映され、`kubeletRootDir`と重複できません。

CacheClass、Namespace、Node上のPod、ServiceAccountはNode pluginのInformer cacheから参照します。initial sync完了後は、API serverの一時障害中もcacheにあるClassを利用できます。cold start直後でInformer cacheが同期していない要求は`Unavailable`となり、kubeletのretryで再試行されます。通常cacheのmetadata異常、共有競合、quota設定失敗、pressure中の新規cache publishでは、volume IDごとに分離したfallback objectへ切り替えます。fallbackはquota有効時も使用でき、volume attributeの`maxBytes`、解決済みCacheClassの有効上限、`fallbackVolumeMaxBytes`のうち小さい値をgenerationごとのtmpfs上限にします。複数fallback volumeの予約容量の合計は`fallbackMaxBytes`を超えません。fallback容量が尽きた場合は`ResourceExhausted`を返します。CacheClassを解決できない場合はCacheClass固有の既定上限を確認できないため、指定されたvolume attributeの`maxBytes`とNode側のfallback上限で制限します。fallback leaseとgenerationは`fallbackRootDir`配下のStore metadataで管理し、last release時はgeneration tmpfsをunmountしてからobjectをtrashへdetachします。fallback mountには常に`nodev`と`nosuid`を付け、`noExec`に応じて`noexec`を付けます。fallback用tmpfsのmount伝播に`Bidirectional`を使います。

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
| `cacheRootDir` | `/var/lib/cache-csi` | Node上のcache directory |
| `fallbackRootDir` | `/run/cache-csi/fallback` | Node上のfallback Store root。plugin起動時にこのpathへ上限付きtmpfsをmount |
| `fallbackMaxBytes` | `1Gi` | 全fallback volumeの容量予約上限 |
| `fallbackVolumeMaxBytes` | `128Mi` | fallback volumeごとの既定上限 |
| `kubeletRootDir` | `/var/lib/kubelet` | kubeletのroot directory |
| `gcInterval` | `30s` | retention-based cache GC scan interval |
| `pressure.highFreePercent` | `25` | cache filesystemの空き容量GC終了水位 |
| `pressure.lowFreePercent` | `20` | cache filesystemの空き容量GC開始水位 |
| `pressure.highInodeFreePercent` | `15` | cache filesystemの空きinode GC終了水位 |
| `pressure.lowInodeFreePercent` | `10` | cache filesystemの空きinode GC開始水位 |
| `projectIDRange.start` | `2000000000` | XFS project quota用に予約するproject ID範囲の開始値 |
| `projectIDRange.count` | `1000000` | XFS project quota用に予約するproject IDの個数 |
| `csiDriver.preventPodSchedulingIfMissing` | `true` | CSI pluginが未登録のNodeへの配置を防止 |
| `admissionPolicy.enabled` | `true` | 許可ラベルのないnamespaceでのCache CSI利用を拒否 |
| `rbac.createPodEvictions` | `true` | `evictRunning`用のPod Eviction権限 |
| `nodeSelector` | `kubernetes.io/os: linux` | DaemonSetを配置するNode |

`CacheClass.spec.maxBytes`は、各cache identityに対してvolume側が要求できるquota上限です。`quota.enabled`と`backend: xfs-project`が必要で、volume attributeの`maxBytes`がこの上限を超える場合はmount要求を拒否します。volume attributeに`maxBytes`がない場合は`quota.defaultMaxBytes`を設定していればその値を使い、未設定なら`spec.maxBytes`を使います。quotaを有効にするCacheClassには`spec.maxBytes`または`quota.defaultMaxBytes`を設定してください。`examples/cacheclass-xfs-project.yaml`と`examples/pod-inline-cache-xfs-project.yaml`に設定例があります。

pressure watermarksはCacheClassごとではなく、`cacheRootDir`が属する単一filesystem全体に適用します。Node pluginは`gcInterval`とは別に3秒ごとに空き容量とinodeを確認し、開始水位を下回ると未使用cacheを物理削除して終了水位までの回復を試みます。pressure中は、新しいCSI leaseによる通常cacheのpublishを止めてbounded fallbackへ切り替えます。既にmount済みのPodによる書き込みは継続するため、`directory` backendだけではNode filesystemを満杯から守るhard limitを保証できません。cache専用filesystemと`xfs-project` quotaを推奨します。`retention`は未使用cacheの保持期間で、省略時はAPI serverのdefaultである`72h`です。明示的な`0s`ではretentionを理由にした回収を無効にしますが、pressure時の回収対象にはなります。`crashRecovery`はNode再起動後に未完了だったgenerationの扱いです。`noExec`を有効にするとcache volumeを`noexec`でmountします。

`evictRunning`を有効にしたCacheClassでは、未使用cacheを回収した後もglobal pressureが続く場合、そのcacheを利用中のPodへKubernetes Eviction APIでbest-effortの退去要求を出します。PodDisruptionBudgetにより拒否される場合やPodが退去しない場合があり、cacheの回収は保証されません。driverはPodを強制削除しません。Node pluginのClusterRoleは全namespaceの`pods/eviction`作成だけを追加し、Podの直接削除権限は持ちません。active Pod evictionを使わない場合は`rbac.createPodEvictions=false`にできます。

`sharingPolicy`の既定値は`Exclusive`です。`Shared`を明示する場合、同じ`cacheKey`を使うPod同士で書き込みを調整し、cache実装が複数プロセスからの同時アクセスに対応している必要があります。再利用したファイルのmodeによってはUID/GIDが異なるPodから書き込めないため、実効UID/GIDも揃えてください。`scope`の既定値`ServiceAccount`ではServiceAccount UIDをcache identityに含めます。`scope: Namespace`を指定すると同一Namespace内のServiceAccount間で同じ`cacheKey`を共有するため、Namespace内のworkloadが同じ信頼境界にある場合に限って使ってください。

`CacheClass`のpolicyはgeneration作成時にsnapshotとして保存します。変更した`retention`などは次のpublish時に反映され、既にidleなcacheには次のpublishまで反映されません。active generationのquotaは変更せず、異なるquotaを要求するpublishはbounded fallbackへ切り替えます。

SELinux enforcing環境は現時点でサポート対象外です。`CSIDriver.spec.seLinuxMount`は`false`で、driverはSELinux mount contextを適用せず、SELinux relabelの動作もintegration testしていません。`Shared` cacheを異なるSELinux contextのPod間で使わないでください。

`quota.enabled`を使うCacheClassでは`backend: xfs-project`を指定し、`cacheRootDir`がproject quota有効のXFS filesystem上にあることを確認してください。Node imageには`xfs_quota`を含めていますが、host filesystemのmount optionやquota設定は管理者が行います。`projectIDRange`はこのNode plugin専用に予約した範囲へ変更し、同じfilesystem上の他用途のproject IDと重複しないようにしてください。

## 開発とリリース

開発ツールはmiseで管理します。主なtaskは`mise run build`、`mise run test`、`mise run lint:chart`、`mise run manifests`、`mise run package:chart`です。`mise run manifests`はAPI markerからDeepCopyとCRDを生成し、生成したCRDをHelm chartへコピーします。CRDのauthoritative copyは`config/crd/bases`です。

リリースはGitHub Actionsの`Release` workflowを手動実行し、SemVer形式のtagを指定します。workflowはテストとchart lintを行い、linux/amd64およびlinux/arm64のNode plugin image、Helm OCI chartをGHCRへ公開し、imageとchartのprovenance attestationを生成してGitHub Releaseを作成します。
