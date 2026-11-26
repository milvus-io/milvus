# Shard Split on QueryView 设计（第 5 版，2026-11-13）

## 1. 状态、范围与结论

本次于 2026-11-26 完善第 5 版方案。源码核对基线为六个 PR 的集成提交 `1bad020dc76f1f51b26d78cbb2d407359365191d`。本文同步该实现的路由、幂等重试、改挂提交、版本分页回收、快照和主备准入约束，并记录六个 PR 在本轮的实施范围。[基线实施稿](https://github.com/milvus-io/milvus/blob/1bad020dc76f/docs/design-docs/design_docs/20261113-shard_split_on_queryview_r5.md)

单条可复制的 SplitShard 决定逻辑切分。RootCoord 发布新的写路由，目标本地写入在读代际准入通过后开放。旧行暂由源 shard 服务，源数据持有者订阅目标删除流。相关查询固定读时间点，各集群独立完成后台清洗、整体改挂和源退休。

本轮接入 QueryCoord 权威下的 Proxy 读拓扑缓存、完整发布身份、全请求规划屏障及流式输出重试边界，并增加 TEXT/LOB 清洗与改挂元数据回归。BM25 排序另做对照实验。PChannel 保留既有空闲/分配数量排序，速率采样及自动扩容闭环仍属后续工作。广播锁缩短按第 13.1 节的前提推进，具备持久消费确认和完整生命周期保护后才改变释放时机。

R5 已实现集合模数、逐 shard 余数集合及 O(1) 内存槽位表。跨切分 AutoID 继续按原始行偏移取模确定分片，改挂保留 attempt ID 条件提交，DataView 页按创建它的版本回收。主备继续保留真实源 Fence 凭据和全拓扑父任务退休准入。

本文中注明“后续实施”“实验”的内容不应视为已实现功能。第 22 节区分历史验证和本轮证据，第 23 节给出六个 PR 的归属；构建和真实集群验收须绑定各自的源码提交。

## 2. 目标与基本规则

目标是降低单个 shard 的写入和查询压力，使数据量较大的 shard 能在线二分；写切换不等待全量数据清洗、加载或改挂。后台任务可以耗时，切换后的正确性不能依赖它们很快完成。

一个源 shard S 被切为 T1、T2。S 只持有切换前的数据；目标接收切换后的写。每行在一个服务代际中只有一个 shard 负责返回。删除必须能到达持有该行的节点，无论删除写入的是 S 还是目标。

逻辑状态只通过可复制 WAL 消息改变。段、manifest、放置、DataView、服务代际、清洗进度和退休进度属于本集群物理派生状态，各集群独立恢复和推进。跨集群不能共享段 ID、节点 ID、本地文件路径或本地时间戳坐标。

SplitShard 形成 fence 后不可取消，只能向前完成。超时不能删除任务、解除必要历史保护，或恢复向 S 写入。故障应表现为等待、可重试错误或明确的准入拒绝，不能静默漏行或重复计数。

## 3. 概念与状态归属

### 3.1 两套 shard 列表

写路由列表由 RootCoord 的 collection 元数据决定。切分后 S 退出写路由，T1/T2 加入。

读服务列表由本集群已发布的 serving generation 决定。窗口中必须包含 S；不能用写路由列表替代读列表。

RootCoord 永久保留 S 的逻辑谱系记录，直到 collection 删除；本集群是否还服务 S、是否已经退休，不回写 RootCoord。

### 3.2 三个服务代际

- g0：切分前的列表，包含 S，不含本次 T1/T2。
- g1：包含 S、T1、T2；S 的 view 带目标删除流谱系。
- g2：本次改挂完成后的列表，包含 T1/T2，不含 S。

以上只列出本次受影响的 shard，collection 的其他 shard 始终保留。

DataView 持有 shard-set epoch；声明目标和删除源各推进一次。普通 flush、manifest 更新及段内 compaction 不推进 shard-set epoch。schema version、DataVersion 和 shard-set epoch 各有独立含义。

服务发布记录绑定完整 shard 列表、epoch、schema、load 配置及相应 view。协调器只在所需 replica 的所有 shard 都实际 Up 后发布。一个请求不能混合不同 epoch 的子结果。

### 3.3 时间戳

- T_switch：本集群 S 上 SplitShard 的时间戳，业务 DML 的终止边界。
- G_i：本集群目标 T_i 的创世时间戳，必须大于 T_switch。
- through_i：目标所在 PChannel 的完整发布前缀。
- X_i@B：目标在读时间点 B 上对应的删除前沿。
- F_i：本次收尾在目标上固定的历史折叠边界。
- M_i：目标已持久化的删除物化覆盖位置。
- C_s,i：段基线已持久覆盖目标流删除的位置。

同一集群 TSO 的时间戳可以跨 PChannel 比较；每个 PChannel 的完成前缀独立，必须分别等待。跨集群时间戳不可互换；谱系中的切换点和各个目标的创世点使用接收集群本地 WAL 坐标。

T_switch 不是 S 的最后一条本地控制消息。S 此后仍可能接收本地不复制的 schema 转发、flush 或退休消息；它只是最后的业务写边界。

### 3.4 读拓扑缓存的权威和身份

QueryCoord 继续负责本集群 serving generation 的发布。Proxy 缓存该发布结果，RootCoord 负责逻辑写路由及谱系。g1/g2 的可服务状态绑定本集群副本、加载范围和 schema，复制接收端使用自己的发布结果。

缓存按本地集群和 collection ID 隔离。缓存项包含加载配置版本、读 schema 版本、shard-set epoch、完整服务分片列表和谱系，作为不可变对象整体替换。复用现有 GetQueryViewLoadInfo 的 load version 和服务分片信息，并在同一发布快照中补齐读 schema 身份。本轮已增加持久 `ServingGeneration`，`GetQueryViewLoadInfo` 的同一快照返回 load/schema/epoch/shards；`GetQueryPlan` 携带 load/schema 身份，SN 在新计划准入时精确匹配。Release 后再次 Load 即使 shard-set epoch 相同，也必须通过加载版本区分。恢复和缓存回填使用精确版本身份，避免迟到响应覆盖当前发布。

## 4. 路由与物理通道

### 4.1 主键 residue 路由

使用与现有写路径完全相同的主键 hash。路由由 modulus M 和每个目标拥有的 residue 集合决定；重写与 Insert/Delete 必须复用同一 hash 实现。

若源拥有多个 residue，保持 M 不变并分配给两个目标。若只拥有一个 residue r，则将 M 翻倍，目标分别获得 r、r+M；其他 shard 的 residue 等价展开，不移动其主键归属。

陈旧 proxy 写向 S 时得到 SHARD_FENCED，刷新路由后只重试确定未成功的行。事务 body 的 Expired 可在 split 源转换为 fence：producer 等待所有 body 成功后才发送 Commit，因此此时尚未提交。Commit 找不到 session 则不能推断零提交，split fence 和普通 TimeTick 清理都可能删除已提交 session；在 split 源返回终止的 INVALID_TRANSACTION_STATE（提交结果未知），保留原有 Invalid 状态，不投影为 fence、Expired 或 CAS 重试。此边界上确实被 abort 的 Commit 也可能明确失败，以避免相对更新重复应用。已有 idempotency 窗口及跨 fence 的重复写检测必须保留。

### 4.2 PChannel 放置与扩容

当前 shard manager 在一个 PChannel 上按 collection ID 保留一份注册。首版保持该约束：目标使用未承载过本 collection 的 PChannel；历史源退休后仍不直接复用其位置，避免旧副本和恢复状态冲突。

基线已经有动态增加 PChannel 的链路：ConfigChannelProvider 发现新通道，Balancer 调用 AddPChannels，持久化并分配 SN、打开 WAL。默认 16 是初始配置，不是系统固定上限。

Split 的前置条件是两个目标 PChannel 已经完成必要的创建、分配和可用性检查。通道不足时，先扩容并等待可用，再重新规划；不能在 S fence 后才发现 topic 无法创建。自动扩容触发、预算及运维入口是配套工作，现有 Split 规划器本身不能被描述为已自动创建通道。

当前 R5 的复制配置校验已支持通道列表仅在末尾扩展，各集群保持相同数量且既有通道索引映射保持原位，并为新增映射建立 CDC 任务。split 使用新位置前仍需验证复制配置与目标 WAL 就绪。此能力按实际代码核实，自动扩容触发、配额预算和全站就绪闭环属于第 4.5 节的后续实施。[配置校验](https://github.com/milvus-io/milvus/blob/1bad020dc76f/pkg/util/replicateutil/config_validator.go#L272)

### 4.3 数量边界

设初始 shard 数为 N，完成二分次数为 K。当前可写 shard 为 N+K，历史源为 K，累计创建 shard 为 N+2K。按当前不复用历史 PChannel 的规则，这个 collection 需要 N+2K 个曾使用的位置。

固定可用 PChannel 为 P 时，最多支持 floor((P-N)/2) 次二分；动态增加 P 后该限制扩大。不同 collection 可以共享 PChannel，不能把全库累计 shard 数直接当作 topic 数。

当前 MaxModulus=32768。初始 modulus 为 N 时，Mmax=N×2^floor(log2(32768/N))。理论最大存活 shard 为 Mmax，最大二分次数为 Mmax-N，累计创建 shard 为 2×Mmax-N；还应满足节点、元数据及发布容量等实际约束。

例如 N=8 时，理论最大存活 shard 为 32768、二分 32760 次、累计创建 65528 个 shard。只沿同一条热点分支不断二分，可能在其他分支仍很粗时先达到 modulus 翻倍深度上限；此时不能继续二分该单 residue。

底层上限同时取决于 MQ 的 topic/partition 配额、broker 与 SN 资源、连接、恢复时间及元数据容量，不能只写为统一的 topic 数上限。首版不以解除 MaxModulus 或支持任意 shard 数为目标。

### 4.4 路由表与 AutoID 幂等的实施约束

持久化的 `routing_modulus` 和逐 shard `buckets` 编译为 `routing.Table`。所有余数恰好归属一个 shard，Proxy 用 `slots[hash(pk) % M]` 查找。路由表及其 vchannel 列表从同一个缓存快照取得。多余数分裂保持 M，单余数分裂翻倍 M，并等价展开其他 shard 的余数。[路由实现](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/util/routing/routing.go)

带幂等键的 AutoID 使用原始 `rowOffset % M` 确定所属 shard，再分配主键 hash 符合该归属的 ID。这样未分裂 collection 的 `rowOffset % N` 语义在首次及连续分裂时保持稳定。改成 `hash(rowOffset) % M` 会在首次升级时改变部分未分裂 shard 的行归属。两个初始 shard 下，offset 为 1 时旧归属为奇数桶，而当前 Int64 hash 得到 1392991556，归属变成偶数桶。

带键重试继续探测相关已封口源的幂等窗口，按原始 offset 恢复首次提交的 ID，仅为确定未提交的行分配和发送后续写。源窗口在约定 TTL 内的恢复与可达性需要覆盖源退休、节点迁移和连续分裂。当前方案采用已有源窗口探测路径，若后续迁移窗口至目标，应另行定义完整记录、继承链、恢复与 GC 协议。[AutoID 与重试实现](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/proxy/dml/task_insert_idempotency_autoid.go) · [源窗口探测](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/proxy/dml/split_fence_idempotency.go)

### 4.5 目标 PChannel 的负载择优

现有分配器已按分配数量排序，空闲通道自然优先；本轮没有新增速率采样器。后续速率择优仍在既有分配器中为 split 选择目标。先按第 4.2 节过滤同集合曾用通道、复制不可用通道及尚未确认的读写 WAL，再在合格集合里优先选择空闲通道，随后考虑可比较的近期写入速率和已有分配负载。两个目标必须落在不同 PChannel。负载排序只影响性能，不能放宽隔离、复制或就绪条件。

复用分配器已有负载信息。引入写入速率时需明确采样窗口、刷新时间和来源，缺失或过期数据按未知处理，使用已有负载排序，不将其当作零流量。沿现有分配器排序和锁内重检，实际注册后更新负载信息。不同 collection 可以共享 PChannel，空闲优先属于软偏好。规划取消、超时或扩容部分完成后重新检查已有可用通道，避免遗留额外容量预留。

候选不足时，在 fence 前、split 广播资源锁外完成容量检查、扩容申请和就绪等待，复制配置更新也在该锁外完成。之后重新获取 collection 资源锁校验路由与目标。MQ 配额、授权、预创建 topic 或 WAL 打开失败时返回可识别错误，源继续正常服务。已有动态通道发现链路负责创建与分配，自动发起扩容及预算控制属于后续实施。

低流量放置减少 Summary pin 对共享 PChannel 的影响。容量预算仍需计入重写窗口内的新增 Summary、重写文件和目标 L0。运行中流量上升会增大保留量，应报告实际使用和等待年龄。源墓碑与同集合旧 PChannel 的复用继续受注册及恢复状态约束，回收证明需覆盖陈旧请求、WAL 重放、复制和幂等窗口。

## 5. 全局不变式

1. S 的业务行和删除时间戳不超过 T_switch；目标的事件在创世之后，创世大于 T_switch。
2. 同一个 serving generation 内，一段及其行只有一个逻辑服务归属；所有权提交前全部归 S，提交后全部按目标归属，准备中的批次不可见。
3. 窗口读使用同一个 B；持有 S 行的节点在回答前，已应用每个目标流上截至 B 的全部相关删除。
4. 每个目标 through_i 覆盖 B；单个通道的完成不能证明其他通道完成。
5. 所有重放必需的历史由持久层保护；内存缓存淘汰不等于允许 GC 删除历史。
6. 旧代际仍有合法引用时，其 DataView、段基线和必要历史继续有效。
7. adoption 的准备、提交、规范化均可在任意持久化边界崩溃后幂等恢复。
8. 物化、段基线覆盖、查询可见前沿和历史保留下限是不同证明，不可相互替代。

## 6. 切分前规划与准入

在 collection 的广播资源锁下，重新读取 collection 及路由，检查源仍可切、没有冲突的本地 split、import/restore 等任务，目标通道满足放置要求，所需节点理解本协议。

检查必须覆盖 prepare 记录、DataView 单个对象/分块根、最终发布事务和控制消息的编码上限。移除“必须在一笔事务里写完所有段”的人为规模限制，但不能移除实际存储容量检查。准入失败必须发生在 fence 之前。

可以在广播前本地 ManualFlush 以减少残余。该操作不复制，不是正确性条件；备集群的残余量可与主集群不同。

默认不自动触发；版本/能力准入未建立可靠发布边界前保持关闭。触发阈值必须处于实现和验收支持的范围内，不能配置一个默认永远被容量准入拒绝的目标并宣称支持该规模。

## 7. 单条 SplitShard 与窗口建立

### 7.1 消息内容及顺序

消息携带源/目标 VChannel、split task ID、路由 post-image、schema、partition 等创世所需逻辑信息，不携带 primary 的段 ID、节点 ID或本地状态。

广播到源、两个目标及 CChannel。源副本先持久化，再以其本地时间戳作为其余副本的 barrier，确保目标创世高于本地 T_switch。复制接收端也必须保证该顺序；仅依靠不同 PChannel 的独立 CDC 流不够。

### 7.2 源与目标

源在 append 的串行化边界安装 DML fence，封存所有 growing 段，后续 Insert/Delete/CreateSegment 不能越过 fence；事务和 Commit 的锁顺序保证不能提交一笔穿越 fence 的旧事务。append 结果不确定时不能解除 fence，恢复决定实际持久状态。

无持久幂等键的单条 DML 遇到源 WAL 应答丢失时，返回结果未知，停止自动重发。明确被 Fence 拒绝后，路由刷新及后续 WAL 尝试共用首次拒绝时设置的绝对截止时间。Insert 部分落盘后再遇 schema 变化时，返回部分写入状态、已持久行的索引与主键。Go SDK 据此停止整批重试。AutoID Partial Upsert 在 CAS 证明缺少新目的地时，刷新路由并重新执行 Strong 读。

源残余段正常 flush，仍属于 S。其 SN 在残余服务期间从 T_switch 起订阅目标删除流。

目标副本作为创世，复用 CreateCollection 的注册、schema、函数运行时、恢复模块和 MVCC 初始化机制，目标持久记录首次创世的 task、源和目标 VChannel。只有三个标识全部相同的重放才可接受，append 结果未知及节点重启时继续检查这组标识。

### 7.3 回调及发布

DataCoord 先声明目标 DataView shard，记录谱系与历史 pin，并暂停目标 L0 compaction。在 pin 建立之前的目标最终 flush/compaction 发布必须 fail closed，不能提前推进所需历史下限。

RootCoord 随后一次发布写路由 post-image，记录源谱系并使 proxy 路由缓存失效。QueryView 协调器以 DataView 的 shard 集合建立 g1，而非只看 RootCoord 的写列表。

源消费 ACK 证明 fence 封存的残余 L1 和所需 L0 工作完成。原 Split 回调在此之前保留 collection 资源锁。写路由和 g1 准备不应等待该残余 ACK 才开始。

主集群目标 DML 的本地准入在 g1 已发布、旧 g0 已按约定退出服务后开放。复制追加不能被本地读准备进度拒绝；该豁免不意味着 g0 可以无条件返回旧结果。

## 8. 完整读协议

### 8.1 fence 到 g1 的边界

fence 前新请求走普通 g0。fence 后，无论 primary 还是 secondary，源不能再为无谱系 g0 发放新计划。g1 尚未可用时返回可重试的 view invalidated/not-ready，由客户端在原 deadline 内等待或重新规划，不降级保证时间戳。

计划解析 MVCC 前后均检查 fence，避免一个正在等 barrier 的 g0 请求穿过切换。只有在 fence 前完成 MVCC 固定并取得有效计划的请求，才能按其原快照完成；它不能临时改为更新的时间点。

这明确接受短暂读不可用。撤回第 4.1 版“备集群消息到 g1 之间所有读都可成功使用 T_switch”的描述；它不满足任意 guarantee/session 下限。

### 8.2 统一 B

proxy 对每个谱系组采样目标 through 及 Strong 最小前缀。Session 取请求 guarantee 与参与目标 Strong 下限的较大值，Strong 使用参与目标的 Strong 下限，Bounded/Customized 保留请求 guarantee，Eventually 无额外下限；固定时间点操作保留已有时间点。

分组发布或重建 DataView 移除已发布的物理段时，在受影响的 shard 上持久化发布时的 TSO 作为固定快照的保守下界。接管时所有目标继承源的下界，包括重写后没有剩余行的目标。SN 在 Phase 1 用请求的 B 校验该下界。失效的游标返回 ViewInvalidated。

非固定请求选择 B=max(一致性下限,min(through_i))。各目标以 barrier_mvcc=B 做 Phase 1，分别等待其完整前缀覆盖 B，返回该点的执行前沿及 X_i@B。

S 的自身业务前沿固定为 T_switch，Phase 2 同时携带共同 B 和每条目标流的 X_i@B。QN sealed 与 SN 残余均先等待本流及全部谱系删除应用位置，再以 B 作为引擎读时间戳。

强一致性的原理是每条目标流都完整覆盖 B；晚于 B 的删除即使已应用也由引擎时间戳可见性过滤。upsert 的删除和新行在其事务时间点上应同时可见或同时不可见。

“Eventually 不主动等待未来下限”不等于没有物理追赶等待；新节点、慢订阅、迁移或网络故障仍可能导致查询等待或超时。

### 8.3 代际与请求重试

一个请求固定完整 shard 列表、加载版本、读 schema 和 shard-set epoch。缓存填充从 QueryCoord 的同一发布快照取得这些字段，SN 的 GetQueryPlan 精确校验请求身份。普通 flush 或 manifest 更新可产生新的物理 view，语义上的服务代际仍按其各自版本定义。

已用每个 queryclient 内按 collection ID 索引的不可变 serving generation 缓存替换 split 查询的逐请求 Coordinator 拓扑 RPC，LRU 容量为 1024。正常命中只访问缓存和查询节点。首次访问、发布失效或精确匹配失败时刷新，合并同一 collection 的并发刷新，并用本地缓存项身份隔离迟到响应。容量淘汰只控制内存，服务端身份校验决定是否可服务；没有以 TTL 替代身份校验。

g0/g1/g2 发布、schema 或加载范围变化、Release/Load、Drop 后，旧身份必须失效。现有 Proxy collection/database 缓存失效入口同时清理 serving 缓存与 load probe；g1/g2 的异步发布没有新增逐次推送通知，依靠 SN 的精确匹配和源 fence 检查触发缓存刷新。通知只加快收敛，不承担安全证明。请求一旦观察到 split，后续较旧的元数据响应不得把它退回 g0。写路由可用于检查服务列表覆盖，查询的参与分片由已发布读拓扑决定。

Coordinator 不可达时，已有缓存项仍需通过节点的全部身份和服务有效性检查。缓存缺失或服务端拒绝后，刷新失败沿请求原 deadline 返回。尚未发布的拓扑在原 deadline 内退避重试，避免将暂时缺少 serving generation 错判为可执行的空分片集合。

Search、Query、统计、表达式删除的读阶段、search requery 和 iterator 均使用同一身份解析规则。固定时间点保持原 B。requery 继承其初始结果的计划和 B，资源失效时显式失败。长期 iterator 只在真实资源保留契约内继续，固定时间戳字段本身不构成保留承诺。

### 8.3.1 全请求规划屏障

已将带服务发布身份或 split 谱系的请求执行拆为以下步骤，保持第 8.2 节的前缀证明。未切分且无发布身份的路径保留既有执行策略。

1. 从同一缓存项取得分片列表、加载身份、schema、epoch 和谱系，解析所需字段及 partition 范围。
2. 按第 8.2 节为谱系组固定 B。明确给定的快照原样保留，操作要求集合级固定时间点时全部参与分片使用该点。普通无谱系分片保持相应操作的一致性语义。
3. 目标先以 `barrier_mvcc=B` 完成 Phase 1，分别证明完整前缀覆盖 B，返回 `X_i@B`。源计划固定自身业务前沿为 T_switch，并绑定该组 B 和全部目标删除前沿。
4. 等本请求全部参与分片规划成功，校验加载身份、schema、epoch、查询范围及计划中的资源描述。任何失败都会取消本轮 RPC，丢弃计划与暂存结果，释放实际持有的引用，在允许重试时重新规划整单。
5. 屏障通过后才向执行节点发起 Phase 2。源 SN/QN 等待对应目标删除前沿后以 B 读取，目标按其在 B 上的计划读取。Phase 1 已返回的内联统计结果仅暂存，两阶段搜索中需要执行节点参与的准备步骤也在屏障之后启动。

`fanOutShards` 为请求创建统一屏障，目标接受有效计划后先发布 `OnPlan` 以放行源规划，再到达屏障；源和普通兄弟分片也必须到达。Search、Query、内部流式归并、统计与两阶段搜索共用此入口。GetQueryPlan 的 lease 仍只保护已有生命周期，屏障不延长资源寿命。Phase 2 继续验证计划有效性，旧 view 被抢占、过期或节点恢复后按下一节处理。

### 8.3.2 流式输出与局部重试

对 unary 请求，在向客户端返回前，Proxy 可以取消执行并清空本轮全部归并状态，再整请求重试。对流式请求，重试条件同时取决于客户端输出和内部 reducer 是否已消费不可撤回的结果。

本轮对通过规划屏障的请求关闭执行阶段的 shard 局部重规划和首个流式接收前的局部 restart；错误返回整请求重试边界。batch/setup 由 queryclient 重试，内部流的接收失败由现有 Proxy 在关闭本轮流并清空归并状态后决定是否重试。重试保留失败 worker/replica 信息，身份失配才驱逐 serving 缓存，传输故障不驱逐有效拓扑。

将来若开放同 shard 局部重试，需同时满足原 collection、加载身份、schema、epoch、B 和查询范围一致，目标副本仍可执行完整计划，失败 shard 的本轮输出可完整撤回或尚未进入不可回滚的归并。重试前取消并等待旧 shard 尝试退出，或以明确的尝试身份拒绝旧回调，再清空对应归并状态。仅有相同 epoch 和 B 无法证明重试安全。

首版在首个批次进入不可撤回的发送阶段、或归并器已经消费无法撤回的部分结果后，遇到无法继续的节点或计划故障即终止该流，返回明确失败。发送报错可能已部分送达，不能由此推断客户端尚未收到数据。实现精确续传后才能扩大局部重试范围，续传需有稳定排序、继续位置和去重证明。所有重试共用原 deadline。

### 8.4 g2 和未加载 collection

g2 就绪后新请求只读目标，旧 g1 停止发放计划。允许的旧计划在已有资源引用和失效规则内结束；其退出是后续历史释放的前置条件。

collection 未加载时，不为写准入强制创建读 view。确认旧 view 已退出后允许目标写，后续 Load 直接构建该集群当前完整服务代际。

## 9. 删除订阅与有界重放

### 9.1 源行的删除投递

QN 中 S 的每段在 S 流上从自己的基线游标注册，并在每条目标流上从 T_switch 注册。删除通过已有 ApplyTransform 和主键匹配作用到段；不引入查询引擎按 shard 逐行过滤。

SN 的未交接残余同样订阅目标 TransformLog，记录每流成功应用位置；WaitMVCCVisible 增加谱系等待。订阅断开、换主、恢复和 partition refresh 不可漏应用删除。

### 9.2 历史保留与内存缓存分离

持久 Summary 保存从 T_switch 起的必要 Delete 历史。逻辑历史 pin 决定 GC 下限；内存缓存起点只描述节点当前缓存的页。固定 pin 不再强制节点把从 T_switch 起的全部事件留在 RAM。

每个 QN/SN 设置节点级缓存预算、流级队列预算和重放并发预算，按驻留数据量核算并预留解码/单页工作集。缓存淘汰只能针对仍可从持久历史补读的内容；尚未持久覆盖的尾部不能被静默丢弃。

新段或 partition refresh 需要的旧内容缺失时，按自身基线分页从 Summary 重放，应用一页、释放一页。每个物理段/流只保留必要游标和有界工作集，避免对每个 view 重复缓存同一份完整历史。

重放边界捕获和实时后缀注册保持现有 Summary 无缺口衔接语义。成功应用位置必须在该事件全部内容应用成功后才推进；未携带 Delete 的进度信号也必须排在相应前缀之后。

慢订阅实时队列溢出时终止/重建该订阅并从最后成功应用位置补读，不能无限积压，不能阻塞 WAL 主消费链，也不能继续报告未应用的前沿。补读所需持久前缀尚未就绪时等待；查询沿原 deadline 超时，不跳过删除。

SN 残余的 partition refresh 也使用同一重放机制，逐步替代无限增长的 applyLog。旧残余全部交接且引用退出后停止订阅；新并入段必须先补齐所需位置才可服务。

### 9.3 恢复与 GC

SN 恢复、变更所有者及 Summary GC 必须在接受可能依赖历史的 view 之前重建保留要求；首次 split 目标物化或前沿推进也不能越过尚未建立的 pin。不能仅依赖 QN 在线连接作为历史保护。

节点重启丢失内存缓存和应用游标后，可从段的持久基线重新重放。应用要求幂等；不能仅凭进程内“已应用”游标释放持久历史。

有界缓存只限制额外日志缓冲/队列，不保证整个引擎总内存恒定。段内删除索引、持久历史容量及重放时长仍随实际工作量变化。现有保守保留整个 collection 历史的实现，不等于本方案的精确 GC 已完成。

## 10. 源清洗

源禁写后，它自己接收的 Delete 是有限集合。fence 前已接受而未完成物化的记录仍可能随后形成 S 的 L0；残余 ACK 完成后，此集合封闭。

先用普通源 L0 compaction 将该集合折入源段。完成后，源每段覆盖 S 的全部必要业务删除。清洗任务随后按 partition、主键 residue 重写源段，产出仍归 S，并带 SplitTarget 标记。

重写不依赖目标的新写是否停止，也不在本阶段折叠目标删除；产出继续以 T_switch 为目标历史重放起点。因此目标持续写不会改变清洗的有限输入集合。

输入保护、输出准备和 compaction 提交复用现有框架；任务失败不改输入可见性，输出对象在发布前不可服务，重试及孤儿 GC 保持现有规范。索引、BM25/统计、manifest 和 TEXT 等附属数据必须逐类验证，不能仅验证行分桶。本轮 split compactor 复用 LOBCompactionContext 对 TEXT 强制重写：按源 manifest 的真实 namespace 解码，按目标 partition namespace 生成独立 LOB，覆盖 canonical、legacy namespace 及 inline-only 源。目标不依赖源 LOB 的存活；已有参数生成路径将 TEXT 输出设为 V3。改挂继续通过 revision clone 保留统计、字段逻辑大小 manifest 与已完成索引 inventory，并验证安装、恢复和清理后的读取。

目标 L0 compaction 在 adoption/g1 退出之前暂停，防止删除只折进目标现有段后丢掉 L0，而源行仍未获得持久删除基线。

## 11. 分批准备、整体提交的 adoption

### 11.1 为什么不能只拦 QueryView

直接分批修改 InsertChannel，即使暂不推送 QueryView，DataView 重算、恢复、compaction 选择、统计与资源 RPC 仍可能读到部分迁移状态。因此 prepare 阶段必须对所有正常读者保持旧逻辑归属；QueryView 发布门禁只是其中一层。

### 11.2 持久记录

新增本地 adoption 标识，绑定 split task、collection、源、目标、源段集合版本及准备状态。段归属清单按大小分块，记录段 ID、partition、目标、manifest/基线身份及目标流起始游标；批次不可变或以明确身份覆盖，重复写不得产生重复迁移。

轻量根记录保存批次数、集合校验信息和状态。单个记录、批次及最终事务都受存储请求大小限制。若完整 DataView 超过单值上限，需将成员列表分块，以小根发布，不能在最后重新汇总成一个超限 blob。

状态为 Preparing → Prepared → Committed → Cleaning → Done。Committed 是唯一生效点；Cleaning 只是已提交归属的规范化，不是第二次切换。

### 11.3 准备与并发

进入 Preparing 前确认源残余完成、源 L0 折叠、全部健康源段已标记且没有占用它们的 compaction。固定这些输入及基线，暂停替换它们的任务。

prepare 批次写入暂存归属，不覆盖当前生效 InsertChannel。schema、drop/truncate、partition 删除等会改变集合的操作，与 adoption 提交串行化；可通过 collection 本地生命周期 guard 覆盖这些回调。锁顺序统一为既有广播回调入口到本地 guard；后台 adoption 持 guard 时不嵌套申请广播资源锁。

目标 DML、flush 和正常 DataView/QueryView 更新继续。不能因为 S 的 adoption 准备较慢而冻结 collection 的全部 view 发布。

### 11.4 整体发布

所有批次齐全且身份/覆盖校验通过后置为 Prepared。提交短临界区基于最新 DataView 合并改挂，保留准备期间目标的新段、manifest 更新及其他合法变化；移除 S，推进 shard-set epoch 一次。

以小事务原子发布新所有权/DataView 根和 Committed 状态。提交前所有段有效归属仍为 S；提交后所有段有效归属全部取目标。失败不暴露半份结果；结果不确定时读回提交记录，而非盲目撤销。

所有 SegmentMeta 消费者必须采用一致的有效归属解析规则：未提交的 pending 记录不可见，已提交但尚未规范化的段由 adoption 记录提供有效归属。后台分批更新规范字段后，只有证明没有消费者仍依赖 overlay 时才能回收准备记录。不能引入两个可各自决定归属的权威。

恢复时先恢复 committed overlay，再启动 projector、调度和 GC。任务状态落后于提交记录时，以已提交事实继续整理；不能把已提交段回放为旧归属。

### 11.5 g2 建立

以 S 的 resident placement 为种子构建目标 g2。QN 物理段按 ID/基线共享；每个 view 的流注册按其归属独立，不能破坏仍在服务的 g1 注册。

每个所需 replica 全部 Up 后发布 g2，停止 g1 发放新计划，等待既定失效/引用退出完成。旧 g1 的资源查询以被引用的精确历史 DataView、已提交的段改挂记录及受保留的实际资源验证归属；目标普通 compaction 替换该段后，SN 冷启动仍可重建旧 g1 的 BM25 IDF。

### 11.6 当前实现及优化边界

当前 `Meta.AdoptSplit` 已在持久提交后一次发布更新后的内存 SegmentInfo。常规读取直接取得一致的有效归属，后台规范化按段加锁，写入当前有效元数据，保护期间发生的 manifest 更新。恢复先在 Catalog 层还原已提交归属，再开放调度和查询。[内存发布和规范化](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/datacoord/meta/adopt_split.go)

保留 Prepared 根的 attempt ID 比较及结果未知时的确认屏障。即使回读暂时看到 Prepared，旧事务也可能稍后到达。必须先确认 Committed，或通过条件状态变更使旧事务无法再提交，才能释放内存发布屏障并启动新尝试。此规则同时适用于 leader 重启和客户端取消。[提交恢复](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/metastore/kv/datacoord/split_adoption.go)

分页根引用本版本专属页，历史版本满足引用退出条件后按版本前缀回收。准备失败的未发布页和写成功丢响应的页也纳入故障验收，清理必须与活跃准备 owner 串行化，避免删除尚可提交的数据。按版本回收已发布页的证据只覆盖其实际回收路径。

## 12. 有限历史收尾与退休

### 12.1 固定 F_i

g2 发布且 g1 退出服务后，在每个目标追加本地不复制的 flush 边界，记录其 F_i。它是后台收尾，不是第二条逻辑切分消息。重复尝试使用已持久化边界；append 成功但记录失败时可以安全重发，不能因此提前完成。

### 12.2 完成证明

固定并记录 F_i 后恢复目标 L0 调度，使其可以执行所需折叠。物化进度到达 F_i 后，每次检查只对仍阻挡覆盖的目标及分区补调度，晚到的小 L0 可在关闭普通自动 compaction 的情况下完成折叠。此时 g1 已退出，源段已全部有效归属目标，不再有窗口内提前丢弃历史的问题；历史 pin 继续保留到证明完成。

等待目标持久化 M_i 覆盖 F_i，证明截至 F_i 的删除都已可靠物化；再由 L0 compaction 为相关仍存活的段基线建立 C_s,i 覆盖 F_i 的证明。没有命中行或完全没有 Delete，也必须可以记录覆盖，不能要求生成实际非空删除文件才前进。

跨 F_i 的 L0 输入只要含有必要历史就仍需处理；不能仅凭文件结束位置较新而跳过旧删除。F_i 以后创建、且能证明不依赖旧删除的基线不必机械补做旧工作。

完成条件不要求目标“没有任何 L0”。F_i 之后持续产生的新删除和 L0 继续普通处理，不延长本次 split 的完成边界。

### 12.3 释放与退休

当旧代际退出、相关基线覆盖固定边界且证明均已持久化后，释放本次 split 的历史 pin。正常 DataView 最小基线、其他 view、其他任务或复制的保留要求继续生效；释放一个 pin 不等于立即删除所有旧日志。

目标普通 L0 调度继续运行，并逐步解除已经不再需要的保护。最后向 S 追加本地不复制的 RetireSplitShard，验证 collection/split 身份，置本地 SPLITTED，释放查询 runtime 与订阅，持久化任务退休。

保留最小源元数据用于历史回放过滤和陈旧写 fence；不重新将 S 加入路由，也不修改 RootCoord 的逻辑谱系。退休或规范化失败均继续幂等重试。

## 13. DDL 与生命周期并发

Split 原回调持有 collection 广播资源锁直到残余完成。源 flush、物化或节点故障可能使其无限等待，阻塞后续相关 DDL、Drop/Release，并影响需要冲突集群锁的操作。这一可用性限制本轮接受；不能把 timeout 当作完成，或直接提前释放锁。

需要暴露残余等待年龄、阻塞阶段及故障原因，运维恢复源 SN/对象存储/物化后继续。允许 Drop 越过残余并拒绝迟到 flush 的 tombstone 协议不属于本版。

schema 变更沿原 AlterCollection 消息和回调执行。PrepareSchema 前向本集群尚未退休的源本地追加不复制的 SchemaChange，使其观察新 schema；语义重试不能无限增加重复历史。schema handoff 与 adoption/退休用相同本地生命周期顺序串行化。

若 adoption 已提交、g2 尚未发布时发生协调器重启，恢复必须能先完成旧 schema 上的 g2，再推进待执行 ALTER，避免 pending schema 阻止 g2、而 ALTER 又等待 g2 的环形依赖。

DropCollection 清理整个 collection，包含源、目标及本地准备/任务状态。DropPartition/Truncate 在 collection 级移除源相关段，同时处理 preparing adoption 的输入失效；进入该回调前已有残余 ACK 保证。不能只广播到写列表后假设源自然消失。

CreatePartition/Flush 主要作用于目标；源读 partition 选择仍须与本集群 load 配置一致。Release 后可停止读资源，但不能丢失恢复和后续 Load 所需的切分事实。

### 13.1 缩短广播锁的后续设计

当前实现仍按本节的残余完成条件释放广播锁。优化目标是将源残余排空交给持久 split 任务，使无冲突操作更早继续。优化实施需同时完成以下条件。

源排空阶段可由现有 `WindowOpen + SourceAcked=false` 表达，确认后进入既有 ResidualFlushed 和后续重写阶段，复用同一持久任务权威。

1. 写路由 post-image、目标创世身份、split owner、谱系和历史保护均已持久化。已加载集合的 g1 完整发布并停止新 g0 计划准入。未加载集合按第 8.4 节证明旧 view 已退出。升主后的真实源 Fence 凭据继续独立约束目标本地 DML，补写副本可以推进广播及 g1 收敛，广播完成不得反过来等待无法补造的真实源凭据。
2. `SourceAcked` 仅由源消费完成确认设置，绑定 collection、task、源和本地 fence 位置。确认覆盖 fence 封存的 L1 与必要 L0 的最终 DataCoord 发布。WAL append ACK 只证明消息追加，分区 checkpoint 或文件已上传都不能替代该完成条件。
3. 源消费 ACK 在广播已转 TOMBSTONE 后仍需写入现存 split owner。主站普通 ACK 与复制源 ACK 都覆盖这一时序。当前普通主站的完成后 ACK 会被忽略，实施必须修正该入口。广播完成或回收后，ACK 接收还能从持久 split owner 验证身份并更新进度。SN 只有在进度持久化得到确认后才能释放消息完成引用，失败按既有 ACK 机制重试。
4. 以持久 owner 记录可恢复的等待状态。ACK 先到、回调先到、Coordinator 或 SN 任意重启都能收敛。后台清洗仅在 SourceAcked 成立后获取封闭输入，复制真实源 Fence 的凭据继续单独保存。
5. 释放前完成各生命周期入口的准入及锁顺序核查。入口读取持久阶段并在现有资源锁和本地 lifecycle guard 内重检，已经进入 WAL 的回调继续按消息顺序恢复。后台持 guard 时不等待广播资源锁。g1 发布和消费 ACK 持久化路径不得取得被等待方持有的 lifecycle guard。跨集群复制回调覆盖相同状态检查。

释放条件与剩余动作采用如下约束。

| 操作 | 源残余尚未完成时的设计要求 |
| --- | --- |
| 目标 DML、flush、正常 view 更新 | 按本地写准入继续，保留源和目标的历史保护 |
| 源清洗与 adoption | 等待持久 SourceAcked 后进入封闭输入阶段 |
| schema、DropPartition、Truncate、import、snapshot | 源排空阶段的本地新请求在广播前、持有对应资源锁时检查持久任务，依赖残余完成的请求返回明确可重试的未就绪错误。已经入 WAL 的消息和复制回调按消息顺序恢复，不能因本地进度落后直接丢弃。ResidualFlushed 后恢复原有 schema 转发和生命周期处理，import、snapshot 仍执行第 15 节的独立限制 |
| CreatePartition 与索引变更 | 逐入口核对源读 partition 范围、schema 和重写输出的索引身份，确认生命周期串行化后才在源排空期放行 |
| Release/Load | 可撤销读加载义务，保留 split 事实、历史保护及后续 Load 的恢复依据 |
| DropCollection | 覆盖源、目标和任务。若要越过残余等待，先设计并验证持久 Drop tombstone，使迟到 ACK、flush、改挂提交均无法复活 owner 或段 |
| FlushAll、复制配置变更及升主 | 保留原有全局锁及 WAL 顺序，复核活动 split、复制身份与目标写准入。集群操作自身的 flush 仍可能等待真实存储完成 |

SN 消费 ACK 还按消息 ResourceKeys 保持顺序，失败的残余工作可能继续阻塞相冲突的消费确认。Coordinator 提前释放资源锁减少该层等待，集群操作仍受真实消费前缀和各自完成条件约束。

上述条件通过故障验收后再移除回调中的残余等待。每个真实源确认、迟到提交和 Drop 竞态都需有恢复证据。切换耗时分别记录源 fence、目标创世、g1 就绪和残余持久化，按实测分位数报告。

## 14. 主备复制与切分准入

### 14.1 保留的协议方向

SplitShard 及后续目标 DML/DDL 可复制；接收端重命名源、目标、路由和 append-first 信息，并以本地 WAL 坐标执行 source-first fence/genesis。

清洗、adoption、F_i、段/manifest、placement、服务代际和源退休为本地物理派生状态。schema 转发和退休带 _ur，不向其他集群传播。复制追加不受目标本地 QueryView 准入门禁阻止。

强制升主可以补写缺失的 SplitShard 副本，使本地广播及读代际继续收敛。DataCoord 从持久广播任务首个复制副本（含控制通道）的复制头记录原主集群。只有本地源消费者确认了原主复制来的真实源 Fence，才额外记录原主集群、源 PChannel、原始 WAL 消息 ID 及时间戳。源 PChannel 的复制流按序持久化，Fence 的消费确认因此覆盖此前的源流前缀。补写的源副本在 WAL 头中永久标记，后续转发给第三个站点也保持该标记。

接纳受影响 target 的首笔本地 DML 需要同时满足两个条件：本地完整 split record 已记录，复制来源与真实源证明相符；本地目标已具备当前读服务条件。原主发起的任务在完整记录后沿用读服务准入，无需额外等待源残余 ACK。缺失源尾部时，新主可完成广播并继续普通复制，受影响目标保持写入拒绝。已落盘真实源 Fence 的迟到消费确认可在广播完成后补齐凭据。未补齐的父任务不能证明其目标可以再次切分。

这描述单次 split 的设计方向，不构成完整 CDC 支持声明。空闲 secondary 的 barrier、角色切换后的目标准入和所有恢复边界均需独立验证。

### 14.2 子切分的全拓扑退休准入

主集群已完成 S→T1/T2 后，可能继续 T1→U1/U2，而备集群仍保留 S 的旧行并只订阅 T1/T2。之后写到 U1/U2 的删除无法自然到达 S；当前单层谱系分组也拒绝一个 shard 同时为祖先目标与后继源。

发起子切分时，在共享集群配置锁和 collection 广播资源锁内检查父任务。主站检查本地任务，按当前复制配置的 PChannel 槽位转换源 VChannel，并向每个远端成员并发查询持久退休记录。成员身份、拓扑指纹、collection、父 task 和源均须吻合。复制来的父任务还须具备真实源 Fence 凭据。拓扑指纹包含成员、PChannel 映射及复制方向，允许各站独立配置连接地址和凭据。

成员缺席、超时、持有旧配置、任务尚未退休或缺少凭据时，推迟当前子切分。目标普通 DML 及复制继续按既有准入执行。下一次申请重新取证，首次切分无需跨站探测。

## 15. 功能与兼容性边界

首版针对主键 hash 路由。namespace 按其他键放置，不得按 PK residue 猜测重写；需要独立方案。

TEXT 的 LOB 引用、manifest base 和重写语义尚未闭合，首版保持准入拒绝；其他字段、索引及统计必须有实际覆盖。不是“Go 编译通过”就证明附属数据能迁移。

residue collection 的 import 路由目前未实现。现有实现即使 adoption 后也可能继续拒绝；要解除拒绝必须明确 import 输出和提交按 residue 分配，不能默认 g2 自动恢复能力。

snapshot/restore 与窗口内再次切分保持保护。CDC 子切分还要满足第 14 节的全拓扑退休条件。

窗口结束后的 snapshot 必须保存当前写 shard 的 routing modulus、ShardBy 和逐 shard residue，不能只保存 shard 数。snapshot 的 schema、properties、routing 从 collection 资源锁内的同一个 RootCoord immutable view 取得。restore 复用复制段的确定性旧 channel→新 channel 映射来重命名 routing，把完整 post-image 放入原 RestoreSnapshot WAL recipe，与 CollectionRestoring 元数据一次创建持久化，不复制旧集群的 split lineage 或再发一次路由更新。回调重放和重启保持相同 owner；恢复后的 collection 可以继续 split。无 routing 字段的旧快照继续按原 modulo-N 语义恢复。含 residue 的新快照需要理解这些字段的恢复端；旧版本忽略新增 JSON 字段不能证明恢复兼容。

RestoreSnapshot 当前是本集群的 `_ur` 消息。对显式 residue 路由快照，在持有广播的 SharedCluster 锁后、准备目标 collection 之前读取最新复制配置；已配置跨集群拓扑或多个 cluster 则明确拒绝，不能创建仅存在于 Primary 的用户 collection。配置读取失败原样传播，不能当作未配置复制。快照 pin 继续保护读取，拒绝时沿既有失败清理释放；旧无 routing 快照的既有行为不在本次扩展范围。

新消息、元数据字段、有效归属解析、epoch 和多流执行都需要所有参与角色兼容。版本门禁须选择真实可辨认的发布版本或能力边界；相同 beta 版本号不能证明节点理解本协议。rollback 到旧节点不在未经验证时承诺。

默认关闭自动触发；关闭触发不应停止已进入 WAL 的任务恢复。首次上线需明确支持的数据规模、最大窗口、资源预算和操作限制。

## 16. 故障恢复矩阵

- 源 fence append 返回不确定：保持 fence；广播和 WAL 恢复决定实际持久状态，不重新接纳旧写。
- 目标创世重放：同一 split 幂等；身份冲突 fail closed，不创建部分可写状态。
- 回调重启：重入声明、路由和残余等待；历史保护在消费发布前恢复。
- 源 SN 在残余交接前重启：从 WAL/基线恢复残余，以持久历史补齐目标删除后服务。
- 目标 SN 更换所有者：等待重新建立完整前缀和无缺口流；失效计划重新规划，不能跨本地通道坐标猜测进度。
- QN 重启或缓存淘汰：从持久段基线分页重放；不依赖旧进程缓存。
- 清洗失败：输入仍有效，重试任务，不暴露未提交输出。
- adoption 准备中重启：旧归属继续有效，恢复批次进度；不能从已写批次推断整体完成。
- adoption 提交成功但应答丢失：读回根/commit 标记；继续 g2 或整理，不回滚归属、不重复推进 epoch。
- g2 未就绪：保留合法旧代际及资源，重试构建；不能只因 adoption durable 就释放历史。
- 持续 Delete：F_i 固定，新 L0 不阻塞旧边界完成。
- 无 Delete/空结果：生成持久覆盖证明，允许完成收尾。
- pin 释放或退休状态写失败：重复执行已证明安全的步骤；以持久事实恢复。
- 对象存储或元数据故障：保留可恢复状态，查询在原 deadline 内等待/失败；不释放尚无证明的历史。

## 17. 代码改动范围

RootCoord/model/routing：SplitShard 逻辑 post-image、源谱系、residue 路由；本地 schema 转发仍沿原 WAL 回调。

Streaming WAL：源事务 fence、目标创世、source-first 接收顺序、恢复、目标写准入、retire；SN 残余删除订阅和分页追赶。

DataCoord/meta/catalog：任务状态、源清洗、目标 L0 保护、adoption 分块准备/commit/规范化、统一有效归属解析、固定 F_i 及覆盖证明。

DataView：空 shard 声明、完整所有权提交、epoch、必要时分块持久化；持续目标更新与 adoption 的合并及恢复。

QueryView 协调器：完整代际发布、schema/load/epoch 一致性、placement 复用、旧代际退出与生命周期调度。

QN/TransformLog：多流注册、物理段共享、有界缓存和分页 replay、每流应用游标、历史 view 的资源验证。

Proxy/queryclient：写 fence/idempotency 重试、读列表和 epoch、统一 B、整请求重试及固定时间点操作语义。

PChannel/CDC：复用现有动态新增能力，补齐可用性和扩容准入；子切分按当前拓扑检查所有成员的持久退休进度。

## 18. 验证与验收

先审计值的构造来源和所有重写路径，再逐个真实失败模式端到端追踪或注入。测试需覆盖以下行为，不能只验证 happy path。

- 切换前旧行在目标 upsert/delete，验证无重返、漏行、重复及 count/BM25/聚合异常；主键 int64/varchar 和 partition 场景均覆盖。
- Strong/Session/Bounded/Eventually/Customized；未确认事务阻挡 through；空闲通道 barrier；fence 与规划/Commit 并发。
- g0→g1 和 g1→g2 中节点故障、schema 变化、Release/Load、整请求重试；固定时间点不被静默替换。
- admission 关闭、恢复、未加载场景及复制豁免；错误类别和重试层级正确，不能让明确拒绝与不确定提交混淆。
- adoption 每批写前后、Prepared、commit 前后、规范化每批、task 状态落后时崩溃；持续目标 flush 不能丢失；任何版本里每段只归一个 shard。
- 超过原事务 op 数和总 payload 的源能完成；单块、单值及最终根的请求大小都受控。
- 长时间持续 Delete、慢 compaction、缓存超预算、订阅溢出、QN 重启、partition refresh；内存工作集受控且无缺口。
- 固定 F_i 之后继续持续 Delete，验证退休最终完成；空历史、跨边界 L0、空匹配也能取得覆盖证明。
- PChannel 扩容成功/失败/尚未可用、进程恢复、预创建 topic 模式、MQ 配额失败；fence 前准入不破坏原写路径。
- 所需时保留旧 generation 的历史资源、索引和 BM25；GC 不因当前归属规范化而回收旧引用。

Go 测试使用 -tags dynamic,test、-gcflags="all=-N -l"、-count=1。还需真实原生库、对象存储及独立集群测试；stub 环境不能证明引擎删除可见性。CDC 验收需实际验证源流断流升主、备站落后时延迟子切分以及追平后的放行。

### 18.1 本轮新增验收用例

| 范围 | 必测时序与判定 |
| --- | --- |
| 读拓扑缓存 | g0→g1→g2、失效通知丢失、并发填充、旧刷新迟到、Coordinator 重启/不可达、同名删建、Release/Load、partition 和 schema 变化。缓存命中无逐请求 Coordinator 拓扑 RPC，已撤销资格的旧身份不能取得新计划，先前合法固定的调用按引用契约完成或明确失效 |
| 统一读点 | 目标完整前缀分别为 120 与 100，时间戳 110 的 upsert 尚未完整发布。Eventually 选 B=100，更新尚不可见。Strong/Session 下限或固定快照要求 B=120 时，等待慢目标覆盖 120，再验证旧行删除与新行可见性同时成立，否则失败。另测固定 B 早于保留基线 |
| 全请求屏障 | 一个 shard 规划超时、目标规划后源失效、g2 抢占、lease 到期。屏障前 Phase 2 调用数为零，失败释放本轮全部计划 |
| 流式重试 | 首批前失败、reducer 已消费但客户端尚未输出、首批后失败、同 epoch 换副本、跨代失效。验证无重复批次、无混代、无静默改变 B |
| AutoID 幂等 | offset=1 的首次切分反例、未分裂兄弟 shard 已提交丢响应、单行/跨子 shard 批次、连续切分、TTL 内源重启/迁移/退休，原始 offset 和首次 PK 精确复原 |
| PChannel | 空闲、冷热混合、缺少负载样本、并发选择、只就绪一个目标、MQ 配额和 WAL 打开失败、规划取消后重试、CDC 通道映射变更。失败发生在 fence 前，原写入持续可用，无遗留容量预留 |
| 改挂及分页 | 提交延迟到回读之后、提交成功丢响应、leader 重启、prepare 期间目标 flush/manifest 更新、未发布孤儿页与已发布版本 GC，验证无半改挂、无新段丢失、无活跃页误删 |
| 广播锁优化 | 普通主站 late ACK、复制源 ACK、广播完成/回收后 ACK、持久进度写失败、Drop 与迟到 flush/ACK、准入检查到 append 间的竞态、复制 DDL 先于本地残余完成、Release/Load、FlushAll、升主并发。补写广播可完成，缺真实源凭据时目标持续拒绝本地写，验证无永久等待和已删除状态复活 |
| 主备 | 正常复制后升主、源 Fence 之前断流后升主且目标拒写、备站父任务未退休时阻止子切分及追平后放行，继续在两个独立集群验证 |

测试记录绑定源码提交、二进制、原生依赖及故障注入点。已通过的历史测试仅证明其对应实现，新增优化按本表重新执行相关用例。

### 18.2 BM25 排序对照实验

当前 IDF Oracle 使用每个 vchannel 最近准备成功的 sealed 基线与已应用 growing 增量。统计未按请求 DataVersion 或 B 回溯，Delete 也不直接逐条扣减该统计。Phase 1 固定请求的 IDF/avgdl，Proxy 按各 shard 返回的分数归并。统一 B 证明行可见性，BM25 统计身份需另行记录。[BM25 优化入口](https://github.com/milvus-io/milvus/blob/1bad020dc76f/internal/streamingnode/server/wal/vchannel/queryresource/global_optimizer.go)

实验分三组执行。

1. 固定 D0 数据、查询集、分析器、BM25 参数、partition/load 范围和搜索参数，停写后分别测 g0、g1、g2，隔离拓扑变化。记录 flush/compaction 状态、各 shard 的 N、df、avgdl 和实际统计版本。
2. 未切分对照集合与切分集合重放相同 D1 新增量及 Delete/Upsert 序列，在相同逻辑检查点比较。构造同文本、同词频和长度的旧行/新行配对，以不同 PK 覆盖 S、T1、T2 和未切分兄弟 shard。覆盖稀有词、常见词、多词查询、长短文本、空目标和两个目标写入严重不均衡。
3. g2 与按最终 residue 归属构造的静态对照集合比较，控制相同数据和物化状态。继续覆盖 flush、清洗、adoption、节点重启及 Release/Load。对照路由应精确匹配最终余数归属。

正确性门槛包括同一数据快照无漏行、重复和删除重返，统计贡献符合当前模型且无重复计入，计数合法、`0 <= df <= N`、分数有限。用记录的统计离线重算候选分数，预先定义浮点容差，同分按等价组比较。排序质量报告 Top-K overlap、配对分数比、排名变化和新行入选率，有标注语料时增加 NDCG@K。质量门槛在实验结果和业务要求明确后定案，既有 BM25 功能 E2E 的通过记录不能替代该实验。

本轮先实施实验。`BM25CorpusSeed` 作为实验候选，需证明收益后再定实现。源基线分别叠加两个目标的不同增量会产生不同统计，评估必须记录这种差异。seed 若进入实现，需绑定字段/函数签名、partition 范围、统计版本和消费截止点，持久化完整性校验及引用保护。空载或 lazy 初始化源需定义可重建来源，容量按词项数量预算。增量重放、growing→sealed、源 compaction 与 adoption 都要避免重复贡献。g2 加入旧行真实统计时须原子移除对应 seed，并验证 g1/g2 并存及恢复语义。

## 19. 可观测性与运维

记录每个任务的逻辑阶段、准备批次、发布 epoch、等待 replica、源残余等待年龄、固定 F_i、物化/基线覆盖差距、pin 原因和退休重试原因。

提供节点/流的缓存预算与占用、历史 replay 字节/耗时、gap 补读、慢订阅、持久历史容量及窗口时长；指标维度遵守现有规范，详细段列表进入可查询诊断信息而非无限高基数标签。

运维可区分“写尚未放开”“g1 服务但后台清洗慢”“adoption 已提交但 g2 未 Up”“历史折叠慢”“只剩规范化清理”。禁止提供未经协议支持的强制解除 fence、丢弃 pin 或跳过批次操作。

新增设计需要观测读拓扑缓存命中/刷新/过期拒绝、全请求规划等待、重规划次数、流式失败前的输出状态，以及缓存身份和服务发布身份的差异。记录 PChannel 选择时的资格、负载数据时间和就绪等待。锁优化分别记录广播完成与源消费完成的时间及未确认 ACK。BM25 实验记录统计模型和查询参数，以便复现实验差异。沿用现有低基数指标规范，collection/task/view 明细放入诊断记录。

## 20. 相对第 4.1 版的关键变化

- 把“一次 DataView 改挂”明确为整体可见性的原子发布，段准备和规范化可以分批，不要求一笔 etcd 事务承载全部段。
- 区分源的有限 L0 和目标的持续新 L0；收尾固定 F_i，不等目标完全空闲。
- 明确已有动态 PChannel 代码及路由 MaxModulus，撤回将默认 16 当作总体硬上限的解读。
- 将持久历史保留与内存缓存解绑，给出分页追赶、实时 gap 恢复和有界工作集方案。
- 保留残余 flush 持锁的已接受可用性限制。
- 明确 fence 后拒绝新 g0、整请求重试、固定时间点及旧代际保留边界。
- CDC 子切分增加全拓扑退休准入，落后成员继续完成自己的接管和退休。


## 21. R5 实施说明与仍需保留的边界

实现使用内容寻址的 256 KiB 元数据页和小根记录；只有已经切分的 DataView 启用分页写入，未切分 collection 保留原有持久化编码。已切分 DataView 的页归创建它的 streaming/compact 版本所有。历史版本进入 GC 后，一次 KV 事务删除该版本的根及专属页前缀，其他活跃版本继续保有自己的页。

每次准备生成唯一 attempt ID；提交事务比较该次 Prepared 根，原子写入 Committed 根和 DataView 根。提交结果不确定且 KV 读回失败时，持续确认并保留内存发布屏障。仅仅读到 Prepared 不能证明旧请求不会稍后提交：必须以相同 Prepared 根为条件，将其变为 Preparing，以隔离迟到提交后才能返回失败。若原提交先赢，则恢复 Committed；若隔离应答丢失，则继续读回。下一次准备使用新的 attempt ID，避免相同 move 集合重建后旧事务再次满足条件。请求取消不代表事务未提交。这一安全策略可能在元数据长时间不可用时延迟优雅关闭，不能宣称取消请求即可解除等待。

严格历史订阅增加请求及应答的能力确认；服务端不确认时客户端 fail closed。启用依赖严格补读的新 QN/客户端前，应先升级 SN，不能认为新字段被旧服务端忽略仍具有同样保证。

节点及流缓存、队列按预算限制；单条逻辑 TransformLog Entry 的解码/同步应用仍有瞬时工作集。事务可能把多个 Delete 合并为一个 Entry，不能为已接受的有效记录增加一个更低且不可恢复的重放大小上限。因此当前界为缓存/队列预算加上并发逻辑 Entry 的瞬时工作集，而不是整个进程内存严格等于缓存预算。按 PK 分片的 wire 协议不是本轮实现。

窗口内源清洗及源替换任务的 TTL 过滤使用冻结截止点。固定时间点请求早于当前 view 的保留基线或谱系切换点时明确失效，不提升 B；这不构成任意历史 iterator 的跨分页快照保留服务。

自动触发和未确定的版本门禁继续关闭；CDC 子切分遵循 §14.2 的全拓扑退休约束。原生引擎、实际对象存储和真实集群/CDC 的验收结论必须与 Go 回归区分。

## 22. R6 端到端验证后的约束补全

空 Summary 必须持久化第一次未知历史前缀，包括有效的零起点。原始边界发布前，global checkpoint 保持在安全位置。重启沿持久起点恢复，真实 GC watermark 继续严格检查。目标创世身份可证明其创世之前没有目标业务消息，所需历史仍由持久覆盖及保留状态共同证明。

WAL growing base 的初始 C 来自真实 CreateSegment 消息的 TimeTick，经已有 AllocSegment 生命周期持久化。首次 data pack 尚未写出 StartPosition 时，L0 筛选可使用这个显式下界。正式旧数据的零值和非 streaming 请求继续按真实兼容要求处理，重试保留已有更高 C。

存活 Preparing/Up QueryView 的读 schema 引用参加 schema GC 保留集。注册和 GC 使用同一个 owner 锁，恢复先重建引用再启后台清理。DropCollection 的加载义务撤销在等待 split lifecycle guard 前完成，后续物理及元数据清理继续受 guard 保护。

Snapshot 等待 collection 资源锁期间可能完成 split、rename 或 move。取得锁后重新读取 immutable collection view，并核验所锁 DB/name 仍匹配，广播 channel 列表从锁内快照构建。

此前 R6 验证轮记录了累计 54 个独立 E2E 场景，运行分布在多个修复版本。随后 R5 评审修复轮记录了 24 个独立真实进程场景通过，其中 3 个是双集群主备场景。两轮存在覆盖重叠，结果按各自提交和二进制记录。R6 是验证轮名称，本设计版本仍为 R5。[基线文档中的历史验证说明](https://github.com/milvus-io/milvus/blob/1bad020dc76f/docs/design-docs/design_docs/20261113-shard_split_on_queryview_r5.md)

最近评审修复记录位于 `/home/zc/test/scratch/shard-split-review-20261119/TEST-REPORT.md`。其集成提交为 `1bad020dc76f`。主备覆盖正常复制后升主、源流在 Fence 前被截断后的目标拒写、备站父任务落后时阻止子切分及追平后的放行。验收环境以报告列出的两套独立 Milvus、CDC、存储和测试门禁为准。

本轮已接入完整发布身份、serving 缓存、全请求规划屏障、流式重试约束及 TEXT 清洗修复。Go 回归、真实原生存储测试和真实进程/CDC 结果分别记录；旧提交的通过结果不代替最新提交验收。PChannel 速率采样、自动扩容闭环、广播锁提前释放和 BM25 seed 均未实现。生产启用、最大规模、长期压力和长期 iterator 保留各有独立验收要求。


## 23. 本轮完善项的实施归属

六个 PR 保持原有层次。跨层接线及完整缓存身份放在最后一层，以保证之前五层可独立编译；全请求屏障本身放在查询层。广播锁优化在其恢复条件齐备后接入，BM25 seed 根据实验决定。

| PR | 本轮设计的关联工作 | 状态 |
| --- | --- | --- |
| #53366，WAL 协议 | 保留 source-first、源 fence 和创世顺序；同步 master WAL 切换接口及 Go 测试入口 | 已接入；广播 ACK 锁时机保持原方案 |
| #53093，写路径 | 保留 residue 路由、原始 offset 分桶、源幂等窗口探测及未知提交边界 | 已接入，随新 master 回归 |
| #53095，协调与任务 | 保留目标通道数量排序、现有扩容准入及持久任务 | 已接入；写速率排序与锁优化待实施 |
| #53097，段改挂 | 保留 attempt CAS 和版本专属分页；修复 TEXT/LOB 清洗并验证改挂元数据 | 已接入；原生存储及崩溃恢复回归通过 |
| #53096，查询与删除重放 | 全请求规划屏障、流式谱系读点及禁止屏障后局部重规划 | 已接入；完整 queryclient race 回归通过 |
| #52604，代际发布与退休 | load/schema/epoch 同一发布快照、SN 精确校验、serving 缓存与整请求重试；退休 runner 和 Drop gate 清理 | 已接入；最终集成及真实进程验收另记录 |

新接口复用现有 QueryView 与 load 版本表达，确需增加字段时直接更新六层未合并草稿及生成代码。正式协议的既有兼容要求按真实使用方保留。上线以实际完成的构建、原生 E2E、主备故障和资源边界结果为依据。

本轮对照资料为 [在线分片分裂架构设计](https://zilliverse.feishu.cn/docx/B6jGdwr1XooPtQxGxnbcGrmzngd)。实施验收按第 18 节执行，实验结果不替代未验证的资源及规模边界。
