かなり良い設計だと思います。しかも今回の意味論は、通常の PV/PVC より **CSI Inline Ephemeral Volume のほうが本質的に近い**です。

2026年9月・Kubernetes 1.37 を前提にするなら、私なら「Cache CSI」を次のように設計します。

> **Kubernetes から見える Volume 自体は ephemeral。  
> しかし CSI Driver 内部には Node-local の cache pool があり、Pod の Volume はその cache の一時的な view / lease にする。**

これなら「Pod が消えても同じ Node ならキャッシュを再利用する」「Node が死んだら何も待たず別 Node で空キャッシュから開始」「DiskPressure 前にキャッシュから捨てる」という、PV では表現しづらい意味論をかなり自然に実現できます。

なお厳密には `emptyDir` は**コンテナ再起動では消えず、Pod の lifetime が終わると消える**ものです。今回問題にしているのは Pod replacement / rescheduling 時ですね。:chatgpt-content-reference{index="0"}

## 推奨アーキテクチャ

構造としてはこうします。

```text
                       Kubernetes API
                             │
                 CSIDriver / CacheClass CRD
                             │
                             ▼
 Node A
 ┌─────────────────────────────────────────────────────────┐
 │ kubelet                                                 │
 │   │                                                     │
 │   │ NodePublishVolume                                   │
 │   ▼                                                     │
 │ cache-csi-node  (DaemonSet / privileged)                │
 │   │                                                     │
 │   ├── cache metadata DB                                 │
 │   ├── pressure / quota manager                          │
 │   ├── GC / LRU                                          │
 │   └── CSI Node service                                  │
 │                                                         │
 │ /var/lib/cache-csi/                                     │
 │   └── pool/                                             │
 │       ├── 81ac.../                                      │
 │       │    ├── generation-17/                           │
 │       │    └── metadata                                 │
 │       └── f20b.../                                      │
 │                                                         │
 │                         bind mount                      │
 │ cache generation ─────────────────────► Pod:/cache      │
 └─────────────────────────────────────────────────────────┘
```

重要なのは、

```text
CSI volume ID != cache identity
```

にすることです。

Pod が作り直されれば inline CSI volume の ID は変わります。それでも、

```text
namespace
+ CacheClass
+ application supplied cacheKey
+ cache schema version
```

などから内部の `cache identity` を作れば、同じ Node 上に以前のデータが残っている場合だけ再利用できます。

CSI inline ephemeral volume は Kubernetes 1.25 から Stable で、まさに「Pod が Node に配置された後、その Node 上で CSI driver が作るローカル volume」を想定した機能です。inline CSI については容量を scheduler が考慮しないことも仕様として明記されています。:chatgpt-content-reference{index="1"}

---

# 1. PV/PVC を作らないのが重要

例えば Pod 側はこんな感じです。

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: app
spec:
  containers:
    - name: app
      image: example/app
      volumeMounts:
        - name: cache
          mountPath: /cache

  volumes:
    - name: cache
      csi:
        driver: cache.csi.example.com
        volumeAttributes:
          cacheClass: default
          cacheKey: model-cache-v4
          maxBytes: 20Gi
```

`CSIDriver` は例えば、

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: cache.csi.example.com
spec:
  attachRequired: false
  podInfoOnMount: true

  volumeLifecycleModes:
    - Ephemeral

  preventPodSchedulingIfMissing: true
```

という構成です。

`podInfoOnMount: true` にすると `NodePublishVolume()` に Pod name / namespace / UID / ephemeral flag が渡ります。ephemeral flag 自体は Kubernetes 1.16 から存在します。:chatgpt-content-reference{index="2"}

`attachRequired: false` なので external-attacher は不要です。そして pure inline CSI にするなら基本的には Controller Service すら不要で、

```text
Identity service
Node service
```

だけの **Node-only CSI driver** にできます。

つまり Longhorn とはかなり性格の違う、非常に小さい CSI driver になります。

---

# 2. Pod が消えても cache は消さない

ここが一番面白いところです。

普通の CSI volume なら、

```text
NodePublishVolume
      ↓
使用
      ↓
NodeUnpublishVolume
      ↓
volume を破棄
```

ですが、Cache CSI では

```text
NodePublishVolume
      ↓
cache object に lease を張る
      ↓
bind mount
      ↓
使用
      ↓
NodeUnpublishVolume
      ↓
bind mount を外す
      ↓
lease だけ削除
      ↓
cache object 自体は残す
```

とします。

つまり Kubernetes 上の ephemeral volume と、内部の cache object は別物です。

例えば、

```text
/cache-pool/
  sha256(namespaceUID + class + cacheKey)/
    metadata.json
    generation-42/
```

を残します。

同じ Deployment の replacement Pod が同じ Node に来て、

```yaml
cacheKey: model-cache-v4
```

を指定すれば、

```text
generation-42
```

を再利用できます。

別 Node に行った場合、

```text
Node B:
  model-cache-v4 が存在する？
       │
       ├─ yes → reuse
       └─ no  → empty generation を作成
```

です。

これなら **Node-locality は scheduling constraint になりません**。

---

# 3. Node が死んでも volume recovery を待たない

普通の Local PV の最大の問題がここです。

```text
PV
 ↓
NodeAffinity = Node A
 ↓
Node A unavailable
 ↓
Pod Pending
```

になり得ます。

今回の方式にはそもそも PV がありません。

Node A が死んだら、

```text
Node A
 cache = 80 GiB
      × lost

Deployment / StatefulSet
      ↓
replacement Pod
      ↓
Node B
      ↓
NodePublishVolume()
      ↓
cache miss
      ↓
empty cache
```

で終了です。

特別な failover 処理すら要りません。

一つだけ Kubernetes の基本的な性質として注意が必要で、**既存の Pod object 自体が別 Node に移動するわけではありません**。Deployment / ReplicaSet / StatefulSet / Job などの controller が replacement Pod を生成することで再配置されます。

Node A があとで復旧した場合、残された cache は orphan 扱いにして cache agent が GC します。

この方式では

```text
「Node A にある cache を削除できるまで待つ」
```

ことは絶対にしません。

Node が物理的に死んでいる以上、その瞬間にディスクから bytes を消すことは不可能です。保証するのは、

> **cache identity としては即座に lost と扱い、他 Node の起動を一切妨げない**

という意味論です。

---

# 4. DiskPressure 時は kubelet ではなく Cache CSI が先に削除する

ここも重要です。

CSI inline ephemeral volume は、`emptyDir` と違って kubelet の通常の Pod `ephemeral-storage` accounting に完全に乗る仕組みではありません。

また kubelet が node pressure として見る filesystem は、

```text
nodefs
imagefs
containerfs
```

に限定されています。別の `/mnt/cache` filesystem を作っても、それを勝手に第四の disk pressure resource として監視してはくれません。Kubernetes 1.37 でもこの点は同じです。:chatgpt-content-reference{index="3"}

したがって `cache-csi-node` 自身が、

```go
unix.Statfs()
```

などで cache filesystem の

```text
available bytes
available inodes
```

を監視します。

例えば設定を、

```yaml
apiVersion: cache.storage.example.com/v1alpha1
kind: CacheClass
metadata:
  name: default
spec:
  highWatermark:
    freePercent: 25
    inodeFreePercent: 15

  lowWatermark:
    freePercent: 20
    inodeFreePercent: 10

  maxCacheBytes: 500Gi

  retention: 72h
```

のようにしておき、

```text
free > 25%
    通常

20% < free < 25%
    新規 cache の増加を抑える

free < 20%
    GC 開始
       ↓
    25% まで回復したら終了
```

とするとよいです。

Kubelet の既定 hard threshold は現在、

```text
nodefs.available   < 10%
imagefs.available  < 15%
nodefs.inodesFree  < 5%
imagefs.inodesFree < 5%
```

なので、それよりかなり前に cache を捨てられます。kubelet の eviction check は既定で 10 秒周期です。:chatgpt-content-reference{index="4"}

つまり、

```text
Normal workload data
       ↑ 大事

Container images

Application cache
       ↓ 一番最初に捨てる
```

という意図した reclaim hierarchy を実現できます。

---

# 5. GC は generation 単位にする

ユーザーの要件にある、

> 「中途半端に残らないように完全に削除」

には少し技術的な落とし穴があります。

例えば cache が、

```text
/cache/
  a
  b
  c
  ...
```

という普通の directory で、動いているアプリから見える状態で

```bash
rm -rf /cache/*
```

すると、削除途中の状態が見えてしまいます。

なので generation を使います。

```text
cache/
  generation-40/
  generation-41/
  generation-42/
```

metadata は、

```text
current = generation-42
```

とします。

invalidate すると、

```text
generation-42  ─────────► trash/
generation-43  ← empty
current = generation-43
```

という切り替えにします。

これで**論理的には atomic invalidate**できます。

ただし、ここでもう一つ Linux mount の重要な制約があります。

### bind mount 済みの running Pod は generation を差し替えられない

例えば、

```text
/cache/current → generation-42
```

を bind mount したあと、ホスト側で

```text
current → generation-43
```

に変えても、running container の mount は古い inode / mount を参照し続けます。

Kubernetes/CSI の `requiresRepublish` を使っても、running container の既存 mount point を新しい backing mount にすり替える用途には使えません。

なので「**active cache まで瞬時に完全削除する**」なら三つの選択肢があります。

| 方式 | I/O性能 | 実装 | running Pod からの atomic clear |
| --- | ---: | ---: | ---: |
| 普通の directory + bind mount | ◎ | ◎ | × |
| cache invalidate → Pod 再作成 | ◎ | ○ | ◎ |
| FUSE filesystem | △〜○ | △ | ◎ |

私は **通常時は directory、極端な DiskPressure では Pod を再作成する**方式をまず実装します。

例えば、

```text
1. generation を INVALID にする
2. workload Pod を terminate
3. NodeUnpublishVolume
4. generation-42 を丸ごと削除
5. replacement Pod
6. empty generation-43 を mount
```

です。

これならアプリケーションが

```text
cache の半分だけ消えている
```

状態を見ることがありません。

「cache purge のためだけに Pod restart は絶対したくない」という要件があるなら、FUSE を backing interface にする価値があります。

---

# 6. 物理的にも「丸ごと一発で捨てたい」なら backend を分ける

実際には数百万ファイルの

```bash
rm -rf cache/
```

はかなり重いです。

なので CacheClass に backend を持たせるのも面白いです。

```yaml
spec:
  backend: directory
```

のほか、

```yaml
spec:
  backend: loop
```

を用意して、

```text
cache-abc.img
   ↓ loop device
ext4
   ↓
Pod
```

とします。

cache 全体を破棄するとき、

```text
umount
losetup -d
unlink cache-abc.img
```

で済みます。

大量の inode を一個ずつ走査するよりずっと扱いやすいケースがあります。

あるいは Node filesystem が対応しているなら、

```text
btrfs subvolume
ZFS dataset
```

を cache generation に使うのも非常に相性がいいです。

実装初期なら私は、

```text
directory + XFS/ext4 project quota
```

を default にして、

```text
loop filesystem
```

を「巨大な小ファイル cache 向け」の option にします。

---

# 7. cache ごとに hard quota も持たせる

cache GC だけでは、

```text
一個の cache が数秒で 500 GiB 書いた
```

ような場合には間に合いません。

そこで cache object 単位に quota を持たせます。

例えば XFS/ext4 の project quota を使って、

```text
cache A → project 1001 → 20 GiB
cache B → project 1002 → 100 GiB
```

とします。

これなら非常に軽量です。

ただし、

```yaml
maxBytes: 20Gi
```

を **20 GiB の reservation** として扱ってはいけません。

今回の cache は best-effort なので、

```text
maxBytes = ceiling
```

です。

容量が 20 GiB 空いていなくても NodePublishVolume をなるべく成功させます。

CSI inline ephemeral volume は scheduler が storage capacity を考慮しないことが仕様上明示されており、「volume creation が失敗しにくい driver 用」とされています。:chatgpt-content-reference{index="5"}

したがって driver の思想も、

```text
「20Gi要求されたけど17Giしかない」
    ↓
ResourceExhausted
    ↓
Pod ContainerCreating
```

ではなく、

```text
empty cache は mount する
       +
最大容量は available capacity に合わせて縮小

```

の方が今回の意味論に合っています。

---

# 8. crash 後の「壊れた cache」を再利用しない仕組み

cache は消えてもよいですが、

> **壊れた cache を再利用する**

のは、空 cache より危険です。

なので generation metadata を、

```text
state:
  clean
  dirty
  invalid
```

くらいにします。

NodePublish 時に、

```text
clean
  ↓
dirty
```

にし、正常な NodeUnpublish 時に必要なら `clean` に戻します。

Node reboot / kernel panic / power failure 後、

```text
dirty generation
```

が発見されたら、

```text
cacheClass.crashSafe == false
    ↓
discard
```

でいいでしょう。

例えば package cache や thumbnail cache は捨てる。

一方 RocksDB や SQLite のようにアプリ側が crash recovery 可能なキャッシュなら、

```yaml
crashRecovery: reuse
```

を許可してもよいです。

---

# 9. cacheKey はかなり重要

CSI の `volume_id` や Pod UID を cache key にしてはいけません。

例えば、

```yaml
volumeAttributes:
  cacheClass: default
  cacheKey: picca-ai-models-v7
```

のようにユーザーが明示します。

内部では例えば、

```text
SHA256(
    namespace UID
  + cacheClass UID
  + cacheKey
  + cacheSchemaVersion
)
```

にするといいです。

Namespace **name ではなく UID** にしておけば、

```text
namespace foo 削除
      ↓
別ユーザーが namespace foo 作成
```

したときに cache が漏れません。

`volumeAttributes` はユーザーから直接渡せるため、Kubernetes のドキュメントも「管理者だけが指定すべき設定を inline volume attributes として公開してはいけない」と明示しています。Admission で制限するのが前提です。:chatgpt-content-reference{index="6"}

なので、

```yaml
cacheClass: trusted-default
cacheKey: ...
```

くらいだけをユーザー入力にして、

```text
hostPath
filesystem
mountOptions
project ID
quota policy
GC priority policy
```

などは `CacheClass` CRD から引くべきです。

特に `cacheKey` をそのまま pathname にするのはやめて hash 化します。

---

# 10. Kubernetes 1.37 の新機能をどう使うか

ここが今回かなり面白いです。

### `preventPodSchedulingIfMissing`

1.37 では `VolumeLimitScaling` が Beta / default-on になり、`CSIDriver.spec.preventPodSchedulingIfMissing=true` を指定できます。

これを使えば、

```text
cache-csi-node がまだ登録されていない Node
```

へ Cache CSI 使用 Pod が scheduler に配置されるのを防げます。:chatgpt-content-reference{index="7"}

今回かなり有用です。

CSI node DaemonSet の起動 race で、

```text
Pod scheduled
↓
driver not registered
↓
MountVolume failed
```

になるのを抑えられます。

Cluster Autoscaler を使う場合は CSI-aware scheduling も有効にする必要があります。:chatgpt-content-reference{index="8"}

---

### `VolumeBindMountOptions`

Kubernetes 1.37 で新しく Alpha になった機能です。

```yaml
volumeMounts:
  - name: cache
    mountPath: /cache
    bindMountOptions:
      - nodev
      - nosuid
      - noexec
```

が使えます。

feature gate:

```text
VolumeBindMountOptions=true
```

が必要です。:chatgpt-content-reference{index="9"}

cache は多くの場合、

```text
nodev
nosuid
```

との相性が非常に良いです。

ただし、

```text
Go build cache
npm cache
JIT cache
compiler cache
```

などでは cached executable を直接実行する可能性があるため、`noexec` は `CacheClass` ごとに判断した方がいいでしょう。

---

### `CSIVolumeHealth`

1.37 では CSI Volume Health が新しい形式で Alpha として整備されています。

Node plugin が、

```text
NodeGetVolumeHealth
NodeGetStorageHealth
```

を実装すると kubelet が、

```text
Pod.status.volumeHealth
CSINode.status.storageHealth
```

に health を書けます。Controller 側には `ControllerGetVolumeHealth` / `ControllerListVolumeHealth` もあります。:chatgpt-content-reference{index="10"}

今回なら、

```text
cache pool filesystem became read-only
quota subsystem broken
metadata DB corrupted
cache disk disappeared
```

などを、

```text
StorageDegraded
StorageUnreachable
```

として出せます。

ただし Kubernetes は health status を**表示するだけで、自動 remediation / failover はしません**。:chatgpt-content-reference{index="11"}

また、

```text
「この cache generation を DiskPressure で意図的に捨てた」
```

のは health failure ではありません。

これは正常な cache eviction として metrics に出すだけでいいです。

現在最新の CSI spec は **v1.13.0（2026-07-27）**で、ここに controller-side VolumeHealth RPC も追加されています。:chatgpt-content-reference{index="12"}

---

# 11. `CSIStorageCapacity` は main design には使わない

一見、

```text
CSIStorageCapacity
```

がぴったりに見えます。

しかし **CSI inline ephemeral volume は storage capacity scheduling の対象外**です。Kubernetes の仕様にも明記されています。:chatgpt-content-reference{index="13"}

これは今回むしろ良いことです。

cache は optional なので、

```text
cache に 20GiB 空きがない
   ↓
Pod 自体を scheduling できない
```

にしたくありません。

したがってメインモードは、

```text
inline CSI
+
best-effort capacity
```

が正解だと思います。

---

# 12. ただし第二モードとして Generic Ephemeral Volume を持つ価値はある

「cache は optional だけど、**20 GiB 確保できなければ起動したくない**」という workload も将来的にはあるかもしれません。

その場合だけ、

```yaml
volumes:
  - name: cache
    ephemeral:
      volumeClaimTemplate:
        spec:
          storageClassName: cache-local
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 20Gi
```

という Generic Ephemeral Volume モードを用意します。

Generic Ephemeral Volume は Kubernetes 1.23 から Stable です。:chatgpt-content-reference{index="14"}

StorageClass は、

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: cache-local
provisioner: cache.csi.example.com
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
```

にします。

すると、

```text
CSIStorageCapacity
```

を使えます。

これは 1.24 から Stable です。:chatgpt-content-reference{index="15"}

さらに Kubernetes 1.37 では、

```text
StorageCapacityScoring
```

が Beta / default-on になりました。

scheduler の `VolumeBinding` plugin が `CSIStorageCapacity` を使って、dynamic provisioning についても node の空き容量を scoring できます。:chatgpt-content-reference{index="16"}

つまりこの mode なら、

```text
Node A: 400 GiB free
Node B: 30 GiB free
Node C: 5 GiB free
```

のような状況で capacity-aware scheduling ができます。

ただ私は、

```text
Mode A: Cache
    CSI inline ephemeral
    capacity は best effort

Mode B: Scratch
    generic ephemeral PVC
    requested capacity guaranteed
```

と**別の意味論として分ける**のを勧めます。

今回ユーザーが最初に説明した用途は完全に Mode A です。

---

# 13. warm cache を scheduler が優先する機能も後から作れる

さらに発展させるなら、

```text
Node A: cache foo = warm
Node B: cache foo = none
Node C: cache foo = warm
```

という情報を、

```yaml
CacheNodeState
```

のような CRD に node agent が publish できます。

そして kube-scheduler の Score plugin を実装して、

```text
warm cache があれば +score
```

します。

重要なのは、

```text
required affinity
```

にしないことです。

つまり、

```text
cache hit
    → Node A を好む

cache miss
    → Node B でも普通に起動できる
```

です。

Node label に、

```text
cache.example.com/key-xxxxxxxx=true
```

を大量に作るのは label cardinality が爆発するので避けた方がよいでしょう。

これは CSI 自体とは分離した scheduler plugin にするのがきれいです。

---

# 14. latest DRA をあえて使わない理由

Kubernetes 1.37 の Dynamic Resource Allocation 系にも、容量を consumable resource としてモデル化するかなり強力な機能があります。

ただ、

```text
20Gi cache capacity を resource claim
```

にしてしまうと、

```text
cache が確保できない
=
Pod を起動できない
```

という意味論になります。

今回の

> キャッシュはあれば高速、なくても正しく動作

とは逆方向なので、**最新だからといって DRA を使わない**方がよいです。

---

# 15. runtime 依存性

この設計の core 部分は、

```text
kubelet
  ↓ CSI gRPC
CSI node plugin
  ↓ mount(2)
host filesystem
```

なので、containerd / CRI-O 固有機能はほぼ使いません。

したがって、

> **CSI inline cache の core functionality に特別な container runtime minimum version はない**

と考えてよいです。

Kubernetes 1.37 自体は CRI v1 runtime が必要です。現在の containerd の Kubernetes compatibility matrix では 1.37 に推奨されているのは **containerd 2.3.0+ または 2.4.0+** です。:chatgpt-content-reference{index="17"}

CRI-O なら Kubernetes と minor を揃えるのが基本で、現在 **CRI-O 1.37.1** が最新です。:chatgpt-content-reference{index="18"}

ただし例外が `VolumeBindMountOptions` です。

これは container runtime が CRI の

```text
Mount.mount_options
```

をサポートし、その feature を advertise している必要があります。Kubernetes 1.37 は runtime が対応を advertise していなければ、この option を使う Pod を拒否します。したがってここは「何バージョン以上」というより**CRI capability を runtime に問い合わせる設計**になっています。:chatgpt-content-reference{index="19"}

また `containerfs` の separate disk pressure accounting は、Kubernetes 1.37 時点では **CRI-O 1.29+ のみ**が対応しています。とはいえ Cache CSI 自身が cache backing filesystem を `statfs` する設計なら、この機能に依存する必要はありません。:chatgpt-content-reference{index="20"}

---

# バージョンをまとめると

| 機能 | Kubernetes | 今回の用途 |
| --- | ---: | --- |
| CSI Inline Ephemeral Volume | **1.25 GA** | **メイン方式** |
| Generic Ephemeral Volume | **1.23 GA** | capacity保証版 |
| CSIStorageCapacity | **1.24 GA** | generic ephemeral版 |
| `podInfoOnMount` / ephemeral context | 1.16〜 | cache owner識別 |
| `StorageCapacityScoring` | 1.33 Alpha → **1.37 Beta/default-on** | capacity保証版のNode選択 |
| `VolumeLimitScaling` | 1.35 Alpha → **1.37 Beta/default-on** | CSI driver未登録Node回避等 |
| `preventPodSchedulingIfMissing` | **1.37 Beta系** | **使いたい** |
| `VolumeBindMountOptions` | **1.37 Alpha/off** | `nodev/nosuid/noexec` |
| 新 `CSIVolumeHealth` | **1.37 Alpha/off** | optionalなhealth reporting |
| CSI spec | **v1.13.0 最新** | 実装ターゲット |
| containerd for K8s 1.37 | **2.3.0+ / 2.4.0+ 推奨** | runtime |
| CRI-O | **1.37.x 推奨** | runtime |
| `containerfs` accounting | CRI-O **1.29+** | optional | :chatgpt-content-reference{index="21"}

## 私なら最初のバージョンはこうする

最初から Controller CSI / PV / scheduler plugin まで全部作らず、まず **Node-only CSI Driver** として作るのがかなりきれいです。

必要な部品は一つの DaemonSet にまとめられます。

```text
cache-csi-node
 ├─ CSI Identity service
 ├─ CSI Node service
 ├─ cache metadata manager
 ├─ quota manager
 ├─ pressure watcher
 ├─ LRU/TTL GC
 └─ orphan reconciler
```

CSI RPC の中心はほぼ、

```text
NodeGetCapabilities
NodePublishVolume
NodeUnpublishVolume
NodeGetVolumeStats       optional
NodeGetVolumeHealth      1.37 Alpha optional
NodeGetStorageHealth     1.37 Alpha optional
```

だけです。

そして cache lifecycle を、

```text
                 Pod lifetime
                      │
         ┌────────────┴─────────────┐
         │                          │
      volume                     volume
       lease                      lease
         │                          │
         └───────┐        ┌────────┘
                 ▼        ▼

          Node-local cache object
                 │
            generation
                 │
          ┌──────┴────────┐
          │               │
        TTL GC        pressure GC
```

という **「volume と cache の lifecycle の分離」**にする。

これがこの CSI の最も重要なアイデアだと思います。

特に Longhorn のような Storage Engine を経由せず、Node の ext4/XFS を直接使うので、replication・network I/O・engine process といった persistent storage 用コストも全部ありません。

そしてこの設計なら、Cache CSI の保証を非常に明快にできます。

> **キャッシュの存在・容量・継続性は一切保証しない。マウントポイントだけは提供する。Node-local の過去データが安全に再利用可能なら再利用する。いつでも全データを失いうる。データ損失によって workload の scheduling / recovery を阻害してはならない。**

これは普通の CSI Storage Driver とはかなり違いますが、まさに欲しい abstraction だと思います。

さらに詰めるなら、次は **`NodePublishVolume/NodeUnpublishVolume` の完全な状態遷移、CacheClass CRD、ディスク上の metadata layout、crash recovery/GC のアルゴリズムまで含めて、実際に Go で実装できるレベルの設計**に落とすと面白いです。
