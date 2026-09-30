# Cache CSI Driver

Cache CSI DriverはKubernetesのCSI inline ephemeral volumeとしてNode-local cache directoryを提供します。`NodePublishVolume`が成功した場合、そのNode上で要求された意味論を満たすcache generationを利用できます。Podのvolume leaseはPod削除時に解除しますが、cache dataはNode上に残り、同じ`CacheClass`と`cacheKey`を指定する後続Podから再利用できます。

このdriverはKubernetes 1.37以降を対象とします。CSI inline volumeはbest-effort cacheであり、データの永続性、Node間共有、容量の予約を提供しません。Node障害、容量圧迫、CacheClassの保持期限、またはdriverのGCによりcacheはいつでも破棄されます。アプリケーションはcache miss後にデータを再生成できる必要があります。

## インストール

Helm OCI chartを使ってインストールします。

```sh
helm install cache-csi-driver \
  oci://ghcr.io/walnuts1018/charts/cache-csi-driver \
  --version 0.1.0 \
  --namespace kube-system
```

このchartは`CacheClass` CRD、`CSIDriver`、MutatingAdmissionPolicy、ValidatingAdmissionPolicy、RBAC、ServiceAccount、Node DaemonSet、health-controller Deployment、Cache CSI専用schedulerをインストールします。ValidatingAdmissionPolicyは、namespaceに`cache.csi.walnuts.dev/allow-use=true`ラベルがない場合にCache CSI volumeを使うPodを拒否します。CacheClassを利用させるnamespaceには、namespaceラベルを変更できる利用者を管理した上で次のようにラベルを付けてください。

```sh
kubectl label namespace default cache.csi.walnuts.dev/allow-use=true
```

Admission policyは必須です。Pod作成時にCache CSI inline ephemeral volumeを検証し、`cacheClass`と`cacheKey`を必須にして`maxBytes`以外の未知属性を拒否します。`maxBytes`を指定した場合は正のKubernetes quantityである必要があります。MutatingAdmissionPolicyはCache CSI Podへ`cache.csi.walnuts.dev/ready: "true"`の`nodeSelector`と`schedulerName: cache-csi-scheduler`を追加します。既存selectorに同じkeyの別値がある場合、`spec.nodeName`が指定されている場合、または別のschedulerを指定した場合はValidatingAdmissionPolicyがPodを拒否します。helm chartが配置するscheduler extenderはinformer cacheにあるNode health Leaseの更新時刻をPod schedulingごとに確認します。LeaseがstaleまたはUnavailableの場合やextenderへ接続できない場合、schedulerはCache CSI Podをbindしません。health-controllerはLease informerからNode labelを投影します。Helm valuesの`admissionPolicy.cacheClassAllowlist.enabled=true`を指定すると、namespace annotation `cache.storage.walnuts.dev/allowed-cache-classes`に列挙されたCacheClassだけを利用できます。値はカンマ区切りで、annotation keyはHelm valuesから変更できます。Node DaemonSetのServiceAccount token自動mountは無効で、Node plugin containerだけにprojected tokenを渡します。health-controllerと専用schedulerはAPI access用ServiceAccount tokenを自動mountします。Node pluginはmount system callを使うためprivilegedかつ`hostPID`有効のcontainerとして動作し、mount属性付きmountの伝播にLinux kernel 5.12以降が必要です。cacheは各Nodeの`/var/lib/cache-csi`に保存されます。保存先を変更する場合はHelm valuesの`cacheRootDir`を設定してください。この値はNode上のhostPathとpluginの引数の両方に反映され、`kubeletRootDir`と重複できません。

CacheClassは各Node pluginのdynamic informer cacheから参照します。PodのNamespace、name、UID、ServiceAccount.nameはCSIの`podInfoOnMount`から取得します。NamespaceとServiceAccountのUIDはNodePublishの解決時にKubernetes APIへGETし、同一objectへの同時GETだけsingleflightで共有します。成功したUIDをTTL cacheへ保存しないため、一時的なAPI障害でidentityを確認できない場合に古いUIDを使わず、NodePublishはエラーになります。NamespaceまたはServiceAccountが存在しない場合やAPI accessが拒否された場合も、設定・権限エラーとしてpublishを拒否します。CacheClass informerのcold start中や通常Storeのlease recovery中も、NodePublishは要求されたcacheを確保できないためエラーになります。NamespaceとServiceAccountのwatchは行わず、CacheClass informerのwatchだけがNode数に比例します。各publishではNamespaceとServiceAccount scopeのCacheClassに限りServiceAccountのUIDをAPIへ確認するため、大規模クラスタではpublish量とAPI serverのGET負荷を考慮してください。

cache object単体のmetadataまたはgeneration構造に破損を検出した場合、Node pluginはそのobjectを再利用対象からatomicにquarantineし、空のgenerationを作成します。この処理が完了すればcache missとしてNodePublishを成功させます。cache rootのread-only化、filesystem I/O error、容量枯渇、安全なdirectory作成・rename・fsyncの失敗、必要なquota機構の障害など、Node全体がcacheを提供できない場合はNodePublishを失敗させ、要求されたcacheが使えると報告しません。要求したquotaに達したcacheは通常どおり`ENOSPC`になります。

Node pluginはNamespace内で`app.kubernetes.io/component=node-health`ラベルを持つLeaseを管理します。Lease annotation `cache.csi.walnuts.dev/node-name`はNode名、`cache.csi.walnuts.dev/state`は`Starting`、`Recovering`、`Ready`、`Degraded`、`Unavailable`のいずれか、`cache.csi.walnuts.dev/reason`は状態の理由、`cache.csi.walnuts.dev/evict`はPod退避の要求を示します。health-controllerは更新期限内のLeaseにある`Ready`または`Degraded`だけをNode labelへ投影します。Leaseがない、更新期限を過ぎている、または状態が`Starting`、`Recovering`、`Unavailable`の場合はNode labelを削除します。`Degraded`は新しいcache generationを正常に提供できる状態です。stale Leaseは新規Schedulingを止めますが、既存PodをEvictする理由にはしません。health-controllerはfreshな`Unavailable` Leaseが`cache.csi.walnuts.dev/evict=true`を示す場合に限り、Deployment、StatefulSet、Job、ReplicationControllerが管理するCache CSI PodへEvictionを要求します。裸PodとDaemonSet Podは自動退避しません。

Node pluginはlease、metadata、generationのStore操作を完了できない場合、該当するStore conditionを`Unavailable`のまま保持します。filesystemの書込みprobeだけではStore操作の回復を確認できないため、このconditionは自動解除しません。管理者は原因を解消してNode pluginを再起動し、起動時recovery完了後にNodeが`Ready`へ戻ることを確認してください。

cache filesystemのpressure時はunused cacheの回収を試みます。回収に失敗しNodeが安全なcacheを提供できなくなった場合、Node pluginはLease stateを`Unavailable`にし、annotation `cache.csi.walnuts.dev/evict=true`を設定します。health-controllerはNode labelを削除し、該当Node上でCache CSI inline volumeを使うPodにKubernetes Eviction APIを呼び出します。Eviction APIはPodDisruptionBudgetを尊重するため、PDBが退避を拒否した場合はPodがNode上に残ることがあります。health-controllerはPodを直接削除せず、PDBを迂回しません。driver recoveryだけが必要な間は`Recovering`で新規配置を止め、既存mountが正常で退避要求がなければ既存Podはそのまま動作します。

NodePublishとNodeUnpublishのtarget pathはkubelet root配下の`pods/<podUID>/volumes/kubernetes.io~csi/<volumeName>/mount`形式に限定し、NodePublishでは`podInfoOnMount`のPod UIDとの一致を検証します。既存path componentのsymbolic link traversalも拒否します。この検証はKubernetes 1.37.1のCSI mounterが生成するinline volume pathに対応しています。CSI socketはkubeletとrootだけがアクセスできるようにしてください。

`cache.csi.walnuts.dev/ready=true`のNode selectorは候補を絞る補助条件です。専用scheduler extenderはcandidate Nodeごとにfresh Leaseを確認し、health-controllerが停止してNode labelが古く残った場合もstale LeaseのNodeを選びません。scheduler extenderのHTTP failureは無視しない設定のため、extenderやLease informerが利用できない間、Cache CSI Podは`Pending`になります。Cluster AutoscalerやKarpenterを使う場合は、custom schedulerとNode templateのlabel要件を確認してください。Node templateにready labelを含めてもscheduler extenderはfresh Leaseを要求します。クラスター管理者がchartのAdmission policyまたはcustom schedulerを削除・置換した場合、このScheduling保証はなくなります。

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
| `storage.backend` | `directory` | Node全体で使う`directory`または`xfs-project` backend |
| `storage.requireRootMountpoint` | `true` | host上でcache rootが独立したmount pointか検証 |
| `metrics.port` | `9807` | Prometheus metrics endpointのport |
| `kubeletRootDir` | `/var/lib/kubelet` | kubeletのroot directory |
| `gcInterval` | `30s` | retention-based cache GC scan interval |
| `pressure.highFreePercent` | `25` | cache filesystemの空き容量GC終了水位 |
| `pressure.lowFreePercent` | `20` | cache filesystemの空き容量GC開始水位 |
| `pressure.highInodeFreePercent` | `15` | cache filesystemの空きinode GC終了水位 |
| `pressure.lowInodeFreePercent` | `10` | cache filesystemの空きinode GC開始水位 |
| `driver.extraArgs` | `[]` | Node pluginに渡す追加引数 |
| `projectIDRange.start` | `2000000000` | XFS project quota用に予約するproject ID範囲の開始値 |
| `projectIDRange.count` | `1000000` | XFS project quota用に予約するproject IDの個数 |
| `admissionPolicy.cacheClassAllowlist.enabled` | `false` | namespace annotationで使用可能なCacheClassを制限 |
| `admissionPolicy.cacheClassAllowlist.annotationKey` | `cache.storage.walnuts.dev/allowed-cache-classes` | CacheClass allowlistを保持するnamespace annotation |
| `nodeSelector` | `kubernetes.io/os: linux` | DaemonSetを配置するNode |
| `healthController.resources` | requests `25m`/`64Mi`, limits `250m`/`256Mi` | health-controller Deploymentのresources |
| `healthController.nodeSelector` | `kubernetes.io/os: linux` | health-controller Deploymentを配置するNode |
| `healthController.tolerations` | `operator: Exists` | health-controller Deploymentのtolerations |
| `scheduler.replicaCount` | `2` | 専用scheduler Deploymentのreplica数 |
| `scheduler.image` | `registry.k8s.io/kube-scheduler:v1.37.1` | Cache CSI専用scheduler image |

Node pluginは`metrics.port`で`/metrics`を公開し、Node DaemonSet PodにはPrometheus scrape annotationを既定で付けます。Node plugin readiness probeは同じportの`/readyz`を確認します。health-controllerはLeaseとNodeのinformer cacheを利用してNode labelを管理し、専用scheduler extenderもLease informer cacheで各Nodeの最新healthを確認します。

各cache identityの`.cache-csi.json`がlease、generation、policyの正本です。`.project-ids.json`はmetadataから再構築できるproject ID予約indexであり、registryが見つからない場合はmetadataとtrash entryから再構築します。metadata commit後にindexの永続化が失敗してもCSI operationは成功し、CSI storage healthをDegradedとして報告してbackground collectorが永続化を再試行します。registryの内容が壊れて予約状態を特定できない場合は、既存project IDの誤再利用を防ぐためXFS project quotaの新規割り当てを停止し、registryを自動上書きしません。

`storage.backend`はCacheClassではなくNode plugin全体に適用します。`CacheClass.spec.storage.maxBytes`は、各cache identityが要求できる最大quotaです。`storage.backend: xfs-project`では各CacheClassに`maxBytes`か`defaultMaxBytes`が必要です。volume attributeの`maxBytes`がCacheClassの上限を超える場合はmount要求を拒否します。volume attributeに`maxBytes`がない場合は`storage.defaultMaxBytes`を使い、未設定なら`storage.maxBytes`を使います。`directory` backendではper-cache quotaを提供しないため、cache rootを専用または容量制限済みfilesystem上に配置してください。`examples/cacheclass-xfs-project.yaml`と`examples/pod-inline-cache-xfs-project.yaml`に設定例があります。

pressure watermarksはCacheClassごとではなく、各Nodeの`cacheRootDir`が属するfilesystem全体に適用します。同じfilesystem上の他用途のデータ増加でもCache CSIのGCとNode health stateの変更が始まるため、専用または容量制限済みfilesystemを使ってください。`storage.requireRootMountpoint=true`はNode pluginが`hostPID`でホストのmount namespaceを参照し、`cacheRootDir`と親directoryのmount IDが異なることを起動時に検証します。この検証はfilesystemがCache CSI専用であることまでは保証しません。軽量なpressure monitorは`gcInterval`とは別に3秒ごとに空き容量とinodeを確認し、回収workerへ処理を依頼します。retention GC、degraded recovery、pressure reclaimは別workerで実行するため、大量のtrash削除中もwatermark監視を継続します。開始水位を下回るとunused cacheの物理削除を行い、終了水位までの回復を試みます。回収後も安全なcacheを提供できない場合、Node pluginはhealth Leaseを`Unavailable`にします。Leaseがfreshで退避要求がある場合、health-controllerはNodeからreplacement controller管理PodをPodDisruptionBudgetを尊重してEvictします。EvictionがPDBに拒否された場合、PodはNode上に残ることがあります。既にmount済みのPodの書き込みは継続するため、`directory` backendだけではfilesystem全体が満杯になる前に必ず回収できるとは限りません。`xfs-project` quotaとcache専用filesystemを推奨します。個別cacheのquota到達はそのcacheだけの`ENOSPC`であり、Node health stateや他のPodの配置には影響しません。`retention`は未使用cacheの保持期間で、省略時はAPI serverのdefaultである`72h`です。明示的な`0s`ではretentionを理由にした回収を無効にしますが、pressure時の回収対象にはなります。unclean shutdown後にdirtyなgenerationは常に破棄し、空のgenerationを作成します。アプリケーション内部の論理的なcache corruptionをCSI driverは検知できないため、cache miss後に再生成できる形式をアプリケーションが維持してください。`noExec`を有効にするとcache volumeを`noexec`でmountします。

`sharingPolicy`の既定値は`Exclusive`です。`Shared`を明示する場合、同じ`cacheKey`を使うPod同士で書き込みを調整し、cache実装が複数プロセスからの同時アクセスに対応している必要があります。再利用したファイルのmodeによってはUID/GIDが異なるPodから書き込めないため、実効UID/GIDも揃えてください。`scope`はcache identityのcollisionと分離の範囲を決める設定であり、認可やsecurity boundaryではありません。既定の`ServiceAccount` scopeではServiceAccount UIDごとにidentityが分かれますが、Pod作成権限を持つ利用者が任意の`serviceAccountName`を指定できる環境では、これだけでtenant間の認可は保証されません。tenantごとの認可が必要な場合は、Podの`spec.serviceAccountName`を許可された値に制限するAdmission policyなどを別途設定してください。`scope: Namespace`は同一Namespace内のすべてのServiceAccountで同じidentityを使うため、意図的にその範囲でcacheを共有する場合に指定します。

generation作成時の有効policyはmetadataにsnapshotとして保存し、そのhashも記録します。identityに属するすべてのgenerationのleaseがある間はcurrent generationのsnapshotを維持します。CacheClass specの変更後は、identityのleaseがすべてなくなった後の次のpublishでhashを比較し、idle generationを回収して新policyのgenerationへ移行します。新policyが適用されるまで既存leaseが使うgenerationのretention、sharing、pressure、quotaは変わりません。active leaseが残る間にquotaの異なるpublish要求がある場合は、その要求を失敗させます。`schemaVersion`はアプリケーションのcache format互換性を変更するときに更新してください。

NodeGetVolumeStatsとCSIの`GET_VOLUME_STATS` capabilityはadvertiseしません。directory backendではvolumeごとの正確なcapacityとavailableを取得できず、recursive size scanはlarge cacheでpressure pathに影響するためです。filesystem全体のcapacityをvolume単位の値として返すことはせず、cache usage semanticsを定義できた段階で実装します。

SELinux enforcing環境は現時点でサポート対象外です。`CSIDriver.spec.seLinuxMount`は`false`で、driverはSELinux mount contextを適用せず、SELinux relabelの動作もintegration testしていません。`Shared` cacheを異なるSELinux contextのPod間で使わないでください。

`storage.backend: xfs-project`を使う場合は、`cacheRootDir`がproject quota有効のXFS filesystem上にあることを確認してください。Node imageには`xfs_quota`を含めていますが、host filesystemのmount optionやquota設定は管理者が行います。`projectIDRange`はこのNode plugin専用に予約した範囲へ変更し、同じfilesystem上の他用途のproject IDと重複しないようにしてください。CSI inline volumeはKubernetesのstorage capacity-aware schedulingやPodの`ephemeral-storage` limitによる容量予約の対象ではないため、schedulerはdirectory backendの保存先容量を判断できません。

## 開発とリリース

開発ツールはmiseで管理します。主なtaskは`mise run build`、`mise run test`、`mise run lint:chart`、`mise run manifests`、`mise run package:chart`です。`mise run manifests`はAPI markerからDeepCopyとCRDを生成し、生成したCRDをHelm chartへコピーします。CRDのauthoritative copyは`config/crd/bases`です。

リリースはGitHub Actionsの`Release` workflowを手動実行し、SemVer形式のtagを指定します。workflowはテストとchart lintを行い、linux/amd64およびlinux/arm64のNode plugin image、Helm OCI chartをGHCRへ公開し、imageとchartのprovenance attestationを生成してGitHub Releaseを作成します。
