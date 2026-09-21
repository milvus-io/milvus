# QueryNode 多副本节点分配算法

节点均衡、稳定性优先于加载成本。实现与 [Balancer](balancer_design.md) 和 [Cache](balancer_cache.md) 对接。当前接口没有节点容量，第一版采用同规格 QueryNode、相同 collection 副本需求模型。

## 1. 规划对象与不变量

独立规划域为 `(CollectionID, ResourceGroup)`。N 是该 RG 内 Alive 且非 Stopping 的 QueryNode 数，R 是该 collection 在该 RG 的期望 replica 数。不同 collection 可以使用相同节点，不引入全局独占节点。

R > 0 时统一使用以下配额规则（N 可以小于 R）：

- 每个节点在本规划域只归属一个目标 replica；每个 replica 的所有 vchannel 使用同一个目标节点集合。
- 每个 replica 分到 floor(N/R) 或 ceil(N/R) 个节点，所有 N 个节点都在目标中；分配了节点不意味着必须往每个节点加载 segment。
- 对已有唯一归属，优先最小化仍存活且仍属于本域的节点的归属变化数，然后启发式降低加载行数。
- 目标约束新视图；旧视图可暂时在目标之外，直到安全替换和清理完成。

按最新决策，N < R 时不再长期共用节点维持所有 replica。此时 floor(N/R)=0，恰有 N 个 replica 获得一个节点，其余 R-N 个 replica 配额为 0，暂时停用。期望 LoadConfig、ReplicaID 和 RG 归属不变，节点恢复后自动补齐。Active 表示有非空目标节点集合，不代表已经 Serving；对外必须区分 Desired、Active、Serving 数量。

已持有健康节点的副本优先保留，继续使用第 4 节 gain 配额算法，不按 ReplicaID 截断。候选配额的保留收益和原有配额稳定性相同、且尚未冻结目标时，可优先保留完整可服务副本，再以稳定散列决胜。视图普通进度不触发重新竞选 Active 集合。资源不足时不轮换公平性，避免健康副本反复被停用。

配额为 0 是目标状态，不要求立即销毁旧视图。旧副本经 Draining 到 Suspended：先保证保留副本能够承接相应 shard 的服务，再沿现有视图生命周期释放被停用副本。仍可服务且是某个 shard 唯一覆盖的旧视图需暂时保留，不能因缩减 Active 集合制造停机；无法服务的旧视图无须等待这种屏障。允许这段交接期间临时共用节点。

N = 0 时没有非空数据的可行新分配，保留已有生命周期状态并等待节点事件/重试；不产生非法目标。R = 0 时回收目标，清理残留视图。

可行性仍要求目标节点能够加载数据。现有接口没有容量准入能力，不能仅凭节点数量保证内存足够。特别是 N < R 时每个 Active replica 只有一个目标节点，如果完整副本装不下，min(N,R) 只是节点数量给出的上限，不能保证这些副本最终可服务。当前算法不自动根据内存失败进一步缩减 Active 数量；那需要另外的容量模型。加载失败不应使目标反复跳动，也不能因此先删掉仍可服务的旧 Up。

## 2. 状态和接口

DefaultBalancePolicy 在其互斥锁保护下持有 LayoutManager，跨 reconcile 保留目标；Balancer 的 reconcileMu 串行化规划与执行，cache 只保存事实。

```go
type CollectionLayout struct {
    // Fingerprint 只包含 replica/RG 意图和 eligible topology；
    // 不包含普通 DataVersion、行数和 Ready/Up 进度。
    Fingerprint LayoutFingerprint
    Owner map[NodeID]ReplicaID
    NodesByReplica map[ReplicaID][]NodeID // 空集合表示目标 Suspended
}
```

概念接口：`EnsureLayout(collection, eligibleNodes, previous, facts) -> (layout, changedNodes, affectedReplicas)`。先完整构造再替换目标，apply 部分失败不回滚目标。目标先于本轮新 AddPreparing 可见给后续 reconcile，但不是已加载事实；实际 cache 仍只由 runtime 同步 hook 更新。

cache 需提供小粒度事实：每 shard 的 Up/Preparing/保留引用节点集合，node -> replica 的引用汇总，以及去重的 node -> compatible resource 索引。资源复用必须有兼容性证据，不能只看 SegmentID。当前 SegmentDataView 只有 ID、PartitionID、RowNum，merged ShardStats 也不能完整表达资源兼容身份和 Dropping 引用，需要在内部接口补齐或保守地判定为不可复用。

本轮用对象局部 immutable Get，不要求全局快照。并发变化由下一轮事件修复，目标在下一轮比较 fingerprint。RG 失效事件必须覆盖该 RG 的 desired collections，不能只通知已有视图使用该节点的 shards。

## 3. 保留可用目标

如果 fingerprint 相同且目标不变量成立，直接复用目标，不扫描 segment。

fingerprint 变化时，保留仍 eligible 且 replica 仍在本域的 owner。新节点、被删除 replica 的节点、从其他 RG 进入的节点加入 Unowned。离开本域的节点剔除。之前已经发出 Preparing 的目标仍以 prior target 为基准，不能被旧 Up 拉回。

首次启动/恢复没有目标时：

1. 没有跨 replica 占用冲突的节点，以实际 Up 归属作为 seed；仅有健康 Preparing 时可用它作为 seed。
2. 冲突节点按保留服务中 shard-node 关系数、Preparing 关系数的字典序选一个暂定 owner，再用 collection/replica/node 的稳定散列打破平局。
3. 没有引用的节点为 Unowned；退役 replica 的引用不产生新 owner。
4. 下面的配额修复生成均衡目标，并在恢复后的进程内冻结。

首次恢复存在冲突时 seed 是启发式，不能宣称相对所有历史视图全局最少迁移。明确的最少迁移保证适用于已有唯一目标归属；所有实际旧视图都继续受生命周期保护，不会因 seed 选择被直接撤销。

## 4. 配额：先精确确定最少节点转移

令 `b = N / R`，`k = N % R`。必须恰有 k 个 replica 获得 b+1，其余 b。

`c[r]` 为清理非法 owner 后，r 当前拥有的节点数；Unowned 不计入 c。对于候选 quota：

```
Retained(quota) = sum_r min(c[r], quota[r])
Moved(quota)    = sum_r max(c[r] - quota[r], 0)
```

给 r 额外一个名额带来的保留收益恰好为：

```
gain[r] = min(c[r], b+1) - min(c[r], b) // 0 或 1
```

按 `(gain 降序, 原先是否多一份配额优先, 完整可服务优先, 稳定散列)` 选 k 个 replica。第一项精确最大化 Retained、最小化 Moved；第二项稳定余数归属。加载成本阶段不能推翻已经满足这些更高优先级的决策。无需搜索全部 quota 组合。N < R 时该规则自然选出有节点的 Active replica，其余副本 quota=0；后者仍在 desired config 中。

随后计算：

```
excess[r]  = max(c[r] - quota[r], 0)
deficit[r] = max(quota[r] - c[r], 0)
M          = sum(excess)
```

第一步分配全部 Unowned，第二步从 excess replica 向 deficit replica 转移恰好 M 个节点。每个既有节点最多转移一次。由于本域所有节点对所有 replica 都合法，填补 deficit 不会因候选约束死锁。

N 个节点中只有 `sum(c)` 个已有合法 owner，因此 `sum(deficit) = len(Unowned) + M`。这既是可构造性检查，也是最少迁移的证明：每个 donor 至少要交出 excess 个节点，算法正好交出这些节点。

## 5. 最后优化加载成本：有预算的确定性贪心

节点均衡和最少转移数量已确定。只在可行的 Unowned 分配和 donor -> receiver 转移中比较加载成本，不为加载成本额外交换已经稳定的节点。

### 5.1 按资源覆盖估价

同一个资源按兼容的 materialization identity 区分，RowNum 为原始行数。共享且兼容的已加载资源不区分 view/replica 所有者。已进入释放、兼容性未知、失败的资源不能算保证可复用；正在加载的资源只有能保证请求合并时才算一次加载。

对暂定目标建立稀疏覆盖计数：

```
cover[r,s] = r 的目标节点中，持有兼容资源 s 的节点数
Missing(T) = sum_active_r sum_desired_s rows(s) * [cover[r,s] == 0]
```

不会建立 `N * R * S` 矩阵。当前同 collection 的 replica 需求相同，遍历去重的 node-resource 稀疏索引即可建立 coverage。

移交节点 n 从 a 到 b 的即时边际估计为：

```
loss(n,a) = sum_{s on n} rows(s) * [cover[a,s] == 1]
gain(n,b) = sum_{s on n} rows(s) * [cover[b,s] == 0]
delta    = loss(n,a) - gain(n,b)
```

没有 owner 的节点 loss 为 0。这是目标集合缺失资源行数的变化，不是该节点的总行数，也不是最终真实 I/O 的精确值。节点集合内部的负载调整仍可能需要额外加载。

### 5.2 可实现的选择过程

第一版使用两轮固定评分贪心，避免动态失效堆导致难以界定的重复资源遍历：

1. 对 Unowned -> deficit replica 构造候选边，基于当前 coverage 算 `-gain`，按 `(cost, stable hash, IDs)` 排序；顺序接受尚未分配且目标未满的边。分配完全部 Unowned。
2. 更新一次 coverage。对所有 donor 节点 -> 仍 deficit 的 replica 构造候选边，算 `loss-gain` 并排序。顺序接受未转移、donor 还有 excess、receiver 还有 deficit 的边，直到转移 M 次。
3. 某条边被接受后不重新排序所有边；固定评分可能过时，因此明确只保证加载成本启发式，不保证 Missing 全局最优。结构约束保证所有 deficit 最终填满，与成本排序无关。
4. 构造最终 coverage 和 Missing。可做一次有界改良：交换两个已转移节点的目的 replica；或用同一 donor 的一个保留节点替换其已转出的节点。只接受 quota 不变、相对原 owner 的总变化数不增加、精确 Missing 严格下降的方案。不触碰不存在迁移需求的稳定布局。
5. 改良的候选数和资源访问数均设硬上限；一次交换用涉及节点资源的计数差量计算精确收益，先计算再提交。到预算就停止，当前方案仍完整可用。第一版也可关闭这一步，功能正确性不依赖它。

若预估候选边数或资源访问量超过本次成本优化预算，直接使用稳定顺序分配 Unowned，再选择 donor 节点填 deficit；不产生半个目标、不延迟必要迁移。预算只影响最低优先级的加载成本，不影响均衡、最少转移和可用性。可从内部固定预算开始，经 benchmark 调整；不为此引入新的用户配置。

复杂度：基础 quota/repair 为 `O(N + R log R)`（稳定节点有序索引可复用，否则加 `N log N`）。启用全候选评分时为 `O(F*R + N*R*log(N*R))`，临时内存 `O(F + N*R)`，F 是本 collection 在本 RG 的去重可复用 node-resource 关系数。候选生成/资源访问预算限制这部分额外工作；不扫描其他 collection。资源 coverage 建立本身也属于预算，预算不足时不建立它。

## 6. 接入 shard 批量规划

```
WaitForReady()
takePending()
pin object-local planning inputs
for affected collection/RG:
    layout = EnsureLayout(...)
    add shards affected by changed layout
classify shards against layout
Must first, descending rows, shared projectedRows
allocate each shard within NodesByReplica[replica]
Apply batch through existing lifecycle
```

所有 shard 候选仍先完成整个 batch 的规划，再 Apply；不改成每 collection 立即提交。

目标变更时，用 per-shard footprint 找出引用已转出节点的 shards，作为 Must。新增候选节点会影响相关 replica 的所有 shards 的可选负载优化；也需要重新考虑此前不可分配或无视图的 shards，不能只依赖现有引用索引。普通进度事件只重算脏 shard，复用 collection layout。

当前 `hasPreparing -> None` 必须细化：健康且允许完成的 Preparing 不重复创建；引用 lost/ineligible 节点的 Preparing 必须能替换。健康但不属于新目标的 Preparing 可先完成，然后通过后续 Must 迁移进入稳定目标，不能永远被 early return 屏蔽。

classify 在“desired 存在但没有 Up -> Must”之前处理 quota=0：没有旧视图则 NoOp；仍有旧视图则受服务交接屏障约束地 RequestRelease。它不是分配失败，不能加入普通失败重试队列，更不能下一轮因 LoadConfig 仍包含该 replica 而重新 AddPreparing。停用状态由 Balancer 内部的目标节点集合表达，不写回 LoadConfigStore，也不通知 CollectionLoadManager。配额重新变正后触发其所有 desired shards，沿正常 Preparing/Up 流程恢复服务。

Balancer 只输出视图 Prepare/Release 和必要的重试，不维护或发布另一份 discovery 集合。实际可服务状态应由 QueryView 生命周期反映；服务发现订阅与下发的生产 wiring 留到后续，不作为本次 Balance 执行的前置条件。在途请求保护和视图租约继续由既有生命周期承担。

跨 replica 复用查询改成 node/resource 索引；生命周期、assignment equality 仍按 ShardID。共享 projectedRows 保持当前逻辑行数口径，不能只把实际统计改成物理去重而继续减去完整 shard 贡献。本版只给已就绪、版本完全匹配的资源复用收益，不预设正在加载的请求能够合并。新目标跨副本分离，且一个 segment 只属于一个 vchannel，因此同一 collection 的本轮新分配不会跨 shard 重复申请同一 node/resource。

## 7. 应用与收敛

- AddPreparing 同步发布 cache，再进入异步加载/同步流程；目标未变时下一轮继续相同节点集合。
- 新 Up 前保留旧 Up，允许目标节点被其他 replica 的旧视图同时引用；不能等对方释放后才允许获取，否则无空闲节点时可能互等。
- 新 Up 后旧视图沿既有 Down/Dropping 流程退出。服务布局进入目标，与物理引用完成清理分开统计；Dropping 仍可能占用资源，不能用 PendingRows=0 判定隔离完成。
- Load/DataView 的普通资源变动不重选 owner；replica/RG/topology 变化才修复目标。目标节点行数忽高忽低不会触发归属抖动。
- Suspended 恢复 Active 时先产生目标，再逐 shard 加载、达到 Up 后恢复服务。加载失败保留旧的可服务视图，重试同一目标；不会因一次失败无限在多个布局间跳转。
- 前提是拓扑/意图最终稳定、目标资源可加载、重试和清理最终推进。在这些前提下固定目标 + Must 迁移 + 旧引用清理给出最终物理隔离。

## 8. 验证矩阵

配额和稳定性：任意 N/R 的 floor/ceil、所有节点唯一归属、最少转移下界；6 节点 4/2 -> 3/3 恰一次转移；5 节点 3/2 保持不变；新增第六节点分给较小副本而不转移旧节点；缩容、replica 增减、RG 移动；恢复时多 replica 交叉占用。

成本：跨 replica 复用；同一资源多份只计一次覆盖；移出最后两份资源时固定评分误差与有界改良；不同 manifest/加载需求不可误判兼容；优化预算为 0 仍正确完成目标；大 RowNum 不被饱和归一化抹平。

生命周期：节点不足时 Active=min(N,R)、其余 Suspended、desired 不变；不轮换健康 Active；停用副本不反复 Prepare/Retry；最后服务覆盖保护；停用不反向更新加载配置管理；节点恢复后原 ReplicaID 自动补齐；无备用节点交叉迁移；partial Apply；目标外健康 Preparing 与丢节点 Preparing；掉电恢复；Dropping 延迟清理；新增节点即使无旧视图也能触发相关 collection；普通进度不扫描完整资源集。

性能：独立测 layout fast path、纯配额修复、资源估价预算上限，区分原有 shard allocator 的复杂度。本设计不宣称修复现有 `CurrentRows` 扫描所有节点及每 shard 复制 projectedRows 的开销。


实现选择：资源复用以 `(PartitionID, SegmentID, DataVersion, LoadInfoVersion)` 完全一致且已就绪为充分条件。跨 DataVersion 的兼容资源暂不计入复用收益；这是保守估计，避免缺少 materialization 身份时错误复用。第一版不启用可选的局部改良。目标布局由默认策略的专用 LayoutManager 保存，Plan 串行化；自定义策略接口保持不变。

视图与 cache 的组件级实现不新增 RPC 或 Replica 节点列表。目标以 immutable layout 对象保留，不另加持久化 generation 或 discovery revision。
