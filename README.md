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

Helm valuesの`admissionPolicy.cacheClassAllowlist.enabled=true`を指定すると、namespace annotation `cache.storage.walnuts.dev/allowed-cache-classes`に列挙されたCacheClassだけを利用できます。値はカンマ区切りです。annotation keyはHelm valuesで変更できます。`admissionPolicy.enabled=false`でpolicyを無効化する場合は、同等の利用制限を別のAdmission設定で行ってください。ClusterRoleはNamespaceとServiceAccountに`get`、CacheClassに`get/list/watch`の権限を持ち、`rbac.createPodEvictions=true`を指定した場合に`pods/eviction`の作成権限を追加します。Podの直接削除権限は`rbac.deletePodsAtCriticalPressure=true`を指定した場合だけ追加されます。ServiceAccount tokenの自動mountは無効で、Kubernetes API tokenはdriver containerだけにprojected volumeとして渡します。Node pluginはmount system callを使うためprivileged containerとして動作し、mount属性付きmountの伝播にLinux kernel 5.12以降が必要です。cacheは各Nodeの`/var/lib/cache-csi`に保存されます。保存先を変更する場合はHelm valuesの`cacheRootDir`を設定してください。この値はNode上のhostPathとpluginの引数の両方に反映され、`kubeletRootDir`と重複できません。

CacheClassは各Node pluginのdynamic informer cacheから参照します。PodのNamespace、name、UID、ServiceAccount.nameはCSIの`podInfoOnMount`から取得します。NamespaceとServiceAccountのUIDはNodePublishの解決時にKubernetes APIへGETし、同一objectへの同時GETだけsingleflightで共有します。成功したUIDをTTL cacheへ保存しないため、一時的なAPI障害でidentityを確認できない場合に古いUIDを使わず、要求はbounded fallbackへ切り替わります。NamespaceまたはServiceAccountが存在しない場合やAPI accessが拒否された場合は設定・権限エラーとしてpublishを拒否します。CacheClass informerのcold start中はbounded fallbackを使います。NamespaceとServiceAccountのwatchは行わず、CacheClass informerのwatchだけがNode数に比例します。各publishではNamespaceとServiceAccount scopeのCacheClassに限りServiceAccountのUIDをAPIへ確認するため、大規模クラスタではpublish量とAPI serverのGET負荷を考慮してください。CSI socketは通常Storeのindex構築とlease recoveryより先に利用可能になり、復旧中の新規publish要求にはbounded fallbackを使います。通常cacheのmetadata異常、共有競合、quota設定失敗、pressure中の新規cache publishでも、volume IDごとに分離したfallback objectへ切り替えます。fallbackはCacheClass policyを適用しないbounded disposable scratchです。volume attributeの`maxBytes`とNode側の`fallbackVolumeMaxBytes`のうち小さい値をgenerationごとのtmpfs上限にし、複数fallback volumeの予約容量の合計は`fallbackMaxBytes`を超えません。fallback容量が尽きた場合は`ResourceExhausted`を返します。CacheClassを解決できない場合も同じNode側の上限で制限します。fallback leaseとgenerationは`fallbackRootDir`配下のStore metadataで管理し、last release時はgeneration tmpfsをunmountしてからobjectをtrashへdetachします。fallback mountには常に`nodev`、`nosuid`、`noexec`を付けます。fallback用tmpfsのmount伝播に`Bidirectional`を使います。

NodePublishのtarget pathは`internal/kubeletcompat`でKubernetes 1.37のinline CSI target path形式を検証します。このpath layoutはCSIの公開契約ではなくkubeletの実装詳細です。サポート対象のKubernetes versionを更新するときは、kubelet実装と対応fixtureを確認して更新してください。

`preventPodSchedulingIfMissing`は既定で無効です。有効にするとCSI pluginが登録されていないNodeへのPod配置を防ぎますが、Cluster Autoscalerでは`--enable-csi-node-aware-scheduling=true`を設定した場合に有効化を推奨します。Karpenterなど他のautoscalerではCSI-aware schedulingの対応状況を確認してから有効にしてください。

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
| `metrics.port` | `9807` | Prometheus metrics endpointのport |
| `kubeletRootDir` | `/var/lib/kubelet` | kubeletのroot directory |
| `gcInterval` | `30s` | retention-based cache GC scan interval |
| `pressure.highFreePercent` | `25` | cache filesystemの空き容量GC終了水位 |
| `pressure.lowFreePercent` | `20` | cache filesystemの空き容量GC開始水位 |
| `pressure.criticalFreePercent` | `0` | 強制削除を許可する空き容量の危険水位。`0`では無効 |
| `pressure.highInodeFreePercent` | `15` | cache filesystemの空きinode GC終了水位 |
| `pressure.lowInodeFreePercent` | `10` | cache filesystemの空きinode GC開始水位 |
| `pressure.criticalInodeFreePercent` | `0` | 強制削除を許可する空きinodeの危険水位。`0`では無効 |
| `driver.extraArgs` | `[]` | `--allow-force-delete`や`--require-cache-root-mountpoint`などのNode plugin追加引数 |
| `projectIDRange.start` | `2000000000` | XFS project quota用に予約するproject ID範囲の開始値 |
| `projectIDRange.count` | `1000000` | XFS project quota用に予約するproject IDの個数 |
| `csiDriver.preventPodSchedulingIfMissing` | `false` | CSI pluginが未登録のNodeへの配置を防止。CSI-aware schedulingに対応したautoscalerでのみ有効化 |
| `admissionPolicy.enabled` | `true` | 許可ラベルのないnamespaceでのCache CSI利用を拒否 |
| `admissionPolicy.cacheClassAllowlist.enabled` | `false` | namespace annotationで使用可能なCacheClassを制限 |
| `admissionPolicy.cacheClassAllowlist.annotationKey` | `cache.storage.walnuts.dev/allowed-cache-classes` | CacheClass allowlistを保持するnamespace annotation |
| `rbac.createPodEvictions` | `false` | `pressurePolicy: Evict`用のPod Eviction権限 |
| `rbac.deletePodsAtCriticalPressure` | `false` | critical pressure時に直接Pod削除を許可 |
| `nodeSelector` | `kubernetes.io/os: linux` | DaemonSetを配置するNode |

Node pluginは`metrics.port`で`/metrics`を公開し、PodにはPrometheus scrape annotationを既定で付けます。metricsにはpublish結果、fallback原因、pressure状態、cache object数、degraded object数、fallback予約容量、trash削除数、Pod eviction結果、recovery時間を含みます。予期しないprimary backend障害が起きると`cache_csi_backend_degraded`を`1`にし、CSI storage healthもDegradedとして報告します。この状態はplugin processの起動中は保持され、別cacheのpublish成功では解除されません。

各cache identityの`.cache-csi.json`がlease、generation、policyの正本です。`.project-ids.json`はmetadataから再構築できるproject ID予約indexであり、metadata commit後にindexの永続化が失敗してもCSI operationは成功します。その場合はCSI storage healthをDegradedとして報告し、background collectorが永続化を再試行します。indexが同期するまではproject IDの新規割り当てを止めます。

`CacheClass.spec.storage.maxBytes`は、各cache identityに対してvolume側が要求できるquota上限です。`storage.backend: xfs-project`を指定し、volume attributeの`maxBytes`がこの上限を超える場合はmount要求を拒否します。volume attributeに`maxBytes`がない場合は`storage.defaultMaxBytes`を使い、未設定なら`storage.maxBytes`を使います。`xfs-project` backendにはどちらかの上限が必要です。`directory` backendではquota上限を指定できません。`examples/cacheclass-xfs-project.yaml`と`examples/pod-inline-cache-xfs-project.yaml`に設定例があります。

pressure watermarksはCacheClassごとではなく、各Nodeの`cacheRootDir`が属するfilesystem全体に適用します。同じfilesystem上の他用途のデータ増加でもCache CSIのGCとPod evictionが始まるため、cache専用filesystemを推奨します。`--require-cache-root-mountpoint=true`を`driver.extraArgs`に指定すると、`cacheRootDir`が親directoryと異なるmount IDを持つfilesystem mount pointであることを起動時に検証します。この検証はfilesystemがCache CSI専用であることまでは保証しません。Node pluginは`gcInterval`とは別に3秒ごとに空き容量とinodeを確認し、開始水位を下回ると未使用cacheを物理削除して終了水位までの回復を試みます。pressure中は、新しいCSI leaseによる通常cacheのpublishを止めてbounded fallbackへ切り替えます。既にmount済みのPodによる書き込みは継続するため、`directory` backendだけではfilesystemを満杯から守るhard limitを保証できません。`xfs-project` quotaとcache専用filesystemを推奨します。`retention`は未使用cacheの保持期間で、省略時はAPI serverのdefaultである`72h`です。明示的な`0s`ではretentionを理由にした回収を無効にしますが、pressure時の回収対象にはなります。`crashRecovery`はNode再起動後に未完了だったgenerationの扱いです。`noExec`を有効にするとcache volumeを`noexec`でmountします。

`CacheClass.spec.pressurePolicy`の既定値`UnusedOnly`ではactive Podを終了させません。`Evict`は未使用cacheを回収した後もglobal pressureが続く場合にKubernetes Eviction APIで退去を要求します。利用するにはHelm valuesの`rbac.createPodEvictions=true`が必要です。Eviction APIはPodDisruptionBudgetを尊重するため、拒否された場合やPodが退去しない場合にcacheの回収は保証されません。`ForceDelete`も通常pressure時には同じEviction APIを使うため、`rbac.createPodEvictions=true`が必要です。`pressure.criticalFreePercent`または`pressure.criticalInodeFreePercent`で設定した、開始水位より低い危険水位を下回り、さらにNode pluginの`--allow-force-delete=true`が明示された場合だけUID precondition付きのPod Deleteをgrace period 0で要求します。直接削除はPodDisruptionBudgetを迂回します。既定の危険水位は`0`で無効であり、`--allow-force-delete`とforce delete RBACも既定で無効です。直接削除を有効にする場合はHelm valuesの`rbac.deletePodsAtCriticalPressure=true`と`driver.extraArgs: ["--allow-force-delete=true"]`の両方を明示してください。`--allow-force-delete`が無効の場合は、critical pressureでもPodDisruptionBudgetを尊重するEvictionへ切り替わります。必要なPodへのtermination要求より先にcache generationを退役させ、後続Podには空の新世代を割り当てます。退役世代は最後のleaseが解放されるまで保持します。

`sharingPolicy`の既定値は`Exclusive`です。`Shared`を明示する場合、同じ`cacheKey`を使うPod同士で書き込みを調整し、cache実装が複数プロセスからの同時アクセスに対応している必要があります。再利用したファイルのmodeによってはUID/GIDが異なるPodから書き込めないため、実効UID/GIDも揃えてください。`scope`はcache identityのcollisionと分離の範囲を決める設定であり、認可やsecurity boundaryではありません。既定の`ServiceAccount` scopeではServiceAccount UIDごとにidentityが分かれますが、Pod作成権限を持つ利用者が任意の`serviceAccountName`を指定できる環境では、これだけでtenant間の認可は保証されません。tenantごとの認可が必要な場合は、Podの`spec.serviceAccountName`を許可された値に制限するAdmission policyなどを別途設定してください。`scope: Namespace`は同一Namespace内のすべてのServiceAccountで同じidentityを使うため、意図的にその範囲でcacheを共有する場合に指定します。

generation作成時の有効policyはmetadataにsnapshotとして保存し、そのhashも記録します。identityに属するすべてのgenerationのleaseがある間はcurrent generationのsnapshotを維持します。CacheClass specの変更後は、identityのleaseがすべてなくなった後の次のpublishでhashを比較し、idle generationを回収して新policyのgenerationへ移行します。新policyが適用されるまで既存leaseが使うgenerationのretention、sharing、pressure、crash recovery、quotaは変わりません。quotaが異なるpublishはidentity内のleaseが残る間fallbackへ切り替わります。`schemaVersion`はアプリケーションのcache format互換性を変更するときに更新してください。

NodeGetVolumeStatsとCSIの`GET_VOLUME_STATS` capabilityはadvertiseしません。directory backendではvolumeごとの正確なcapacityとavailableを取得できず、recursive size scanはlarge cacheでpressure pathに影響するためです。filesystem全体のcapacityをvolume単位の値として返すことはせず、cache usage semanticsを定義できた段階で実装します。

SELinux enforcing環境は現時点でサポート対象外です。`CSIDriver.spec.seLinuxMount`は`false`で、driverはSELinux mount contextを適用せず、SELinux relabelの動作もintegration testしていません。`Shared` cacheを異なるSELinux contextのPod間で使わないでください。

`storage.backend: xfs-project`を使うCacheClassでは、`cacheRootDir`がproject quota有効のXFS filesystem上にあることを確認してください。Node imageには`xfs_quota`を含めていますが、host filesystemのmount optionやquota設定は管理者が行います。`projectIDRange`はこのNode plugin専用に予約した範囲へ変更し、同じfilesystem上の他用途のproject IDと重複しないようにしてください。

## 開発とリリース

開発ツールはmiseで管理します。主なtaskは`mise run build`、`mise run test`、`mise run lint:chart`、`mise run manifests`、`mise run package:chart`です。`mise run manifests`はAPI markerからDeepCopyとCRDを生成し、生成したCRDをHelm chartへコピーします。CRDのauthoritative copyは`config/crd/bases`です。

リリースはGitHub Actionsの`Release` workflowを手動実行し、SemVer形式のtagを指定します。workflowはテストとchart lintを行い、linux/amd64およびlinux/arm64のNode plugin image、Helm OCI chartをGHCRへ公開し、imageとchartのprovenance attestationを生成してGitHub Releaseを作成します。
