# Shard Split on QueryView 设计（第 5 版，2026-11-13）

## 1. 状态、范围与结论

本文替代第 4.1 版，整合代码评审及本轮讨论后的完整方案。代码基线为 master 30c7552ed8；已有六层初版实现为 qv-l6 / 67687fb9dd。本文描述目标设计，不能据此认定初版代码已经实现或验证了所有机制。本文为实现目标；r5 实施分支为 qv-r5，具体完成范围以代码及验证记录为准。

核心路线保持不变：单条可复制的 SplitShard 决定逻辑切分；写路由立即切到两个目标；旧行暂时由源 shard 服务；源数据持有者订阅目标删除流；一次相关查询使用同一个读时间点；各集群本地完成后台清洗、整体改挂和源退休。

本轮收敛的事项：改挂分批准备、整体发布；源 L0 有限，目标持续产生的新 L0 不阻塞历史收尾；利用现有动态 PChannel 能力扩容；删除历史依赖持久 Summary，内存只做有界缓存；接受残余 flush 阶段的 DDL 锁等待；fence 后拒绝新 g0 计划，保留一致性并接受短暂读重试。

CDC 场景下，子切分等待当前拓扑每个成员确认父任务退休。强制升主时，目标本地写需要原主真实源 Fence 的复制凭据。第 14 节给出两项准入的恢复条件；生产复制开关默认关闭。

首版支持主键 hash 路由的内部 collection。namespace、TEXT 重写、residue collection 的 import、snapshot/restore 等未闭合能力见第 15 节。查询引擎不增加按行 shard 过滤，不增加第二条依赖物理清洗完成的可复制切换消息。

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

## 4. 路由与物理通道

### 4.1 主键 residue 路由

使用与现有写路径完全相同的主键 hash。路由由 modulus M 和每个目标拥有的 residue 集合决定；重写与 Insert/Delete 必须复用同一 hash 实现。

若源拥有多个 residue，保持 M 不变并分配给两个目标。若只拥有一个 residue r，则将 M 翻倍，目标分别获得 r、r+M；其他 shard 的 residue 等价展开，不移动其主键归属。

陈旧 proxy 写向 S 时得到 SHARD_FENCED，刷新路由后只重试确定未成功的行。事务 body 的 Expired 可在 split 源转换为 fence：producer 等待所有 body 成功后才发送 Commit，因此此时尚未提交。Commit 找不到 session 则不能推断零提交，split fence 和普通 TimeTick 清理都可能删除已提交 session；在 split 源返回终止的 INVALID_TRANSACTION_STATE（提交结果未知），保留原有 Invalid 状态，不投影为 fence、Expired 或 CAS 重试。此边界上确实被 abort 的 Commit 也可能明确失败，以避免相对更新重复应用。已有 idempotency 窗口及跨 fence 的重复写检测必须保留。

### 4.2 PChannel 放置与扩容

当前 shard manager 在一个 PChannel 上按 collection ID 保留一份注册。首版保持该约束：目标使用未承载过本 collection 的 PChannel；历史源退休后仍不直接复用其位置，避免旧副本和恢复状态冲突。

基线已经有动态增加 PChannel 的链路：ConfigChannelProvider 发现新通道，Balancer 调用 AddPChannels，持久化并分配 SN、打开 WAL。默认 16 是初始配置，不是系统固定上限。

Split 的前置条件是两个目标 PChannel 已经完成必要的创建、分配和可用性检查。通道不足时，先扩容并等待可用，再重新规划；不能在 S fence 后才发现 topic 无法创建。自动扩容触发、预算及运维入口是配套工作，现有 Split 规划器本身不能被描述为已自动创建通道。

复制拓扑还需各集群追加通道列表、保留既有索引映射并完成复制配置更新；仅主集群本地加通道不代表 CDC 已可用。这部分和第 14 节一起验收。

### 4.3 数量边界

设初始 shard 数为 N，完成二分次数为 K。当前可写 shard 为 N+K，历史源为 K，累计创建 shard 为 N+2K。按当前不复用历史 PChannel 的规则，这个 collection 需要 N+2K 个曾使用的位置。

固定可用 PChannel 为 P 时，最多支持 floor((P-N)/2) 次二分；动态增加 P 后该限制扩大。不同 collection 可以共享 PChannel，不能把全库累计 shard 数直接当作 topic 数。

当前 MaxModulus=32768。初始 modulus 为 N 时，Mmax=N×2^floor(log2(32768/N))。理论最大存活 shard 为 Mmax，最大二分次数为 Mmax-N，累计创建 shard 为 2×Mmax-N；还应满足节点、元数据及发布容量等实际约束。

例如 N=8 时，理论最大存活 shard 为 32768、二分 32760 次、累计创建 65528 个 shard。只沿同一条热点分支不断二分，可能在其他分支仍很粗时先达到 modulus 翻倍深度上限；此时不能继续二分该单 residue。

底层上限同时取决于 MQ 的 topic/partition 配额、broker 与 SN 资源、连接、恢复时间及元数据容量，不能只写为统一的 topic 数上限。首版不以解除 MaxModulus 或支持任意 shard 数为目标。

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

一个请求固定完整 shard 列表、epoch 和 schema。任一 shard 无法精确匹配时，丢弃本轮所有部分结果，刷新 serving generation 后整请求重试；不能只重试后把两代结果拼起来。

可缓存不可变 serving generation，依赖 SN 的精确 epoch 校验检测过期并刷新。缓存不能使已退出的 view 重新接受请求；不能在协调器失败时回退为写路由列表。初版每请求拉取的路径可以先保留，缓存是后续优化。

search requery、统计、表达式删除和 iterator 都必须保持选定的 schema/时间点及其原有语义。跨分页快照若需要继续存活，须有相应引用或保留契约；若已无法执行，返回明确的快照/游标失效，不能静默换 B。不能仅因固定时间点字段存在就宣称 iterator 的跨代保留已经实现。

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

输入保护、输出准备和 compaction 提交复用现有框架；任务失败不改输入可见性，输出对象在发布前不可服务，重试及孤儿 GC 保持现有规范。索引、BM25/统计、manifest 和 TEXT 等附属数据必须逐类验证，不能仅验证行分桶。

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

## 19. 可观测性与运维

记录每个任务的逻辑阶段、准备批次、发布 epoch、等待 replica、源残余等待年龄、固定 F_i、物化/基线覆盖差距、pin 原因和退休重试原因。

提供节点/流的缓存预算与占用、历史 replay 字节/耗时、gap 补读、慢订阅、持久历史容量及窗口时长；指标维度遵守现有规范，详细段列表进入可查询诊断信息而非无限高基数标签。

运维可区分“写尚未放开”“g1 服务但后台清洗慢”“adoption 已提交但 g2 未 Up”“历史折叠慢”“只剩规范化清理”。禁止提供未经协议支持的强制解除 fence、丢弃 pin 或跳过批次操作。

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

空 Summary 必须持久化第一次未知历史前缀，包括有效的零起点；原始边界发布前，global checkpoint 不能继续越过它。重启不能用更晚 checkpoint 重建更晚的初始 floor。真实 GC watermark 仍严格检查，已经丢失旧起点的草稿状态不能凭空修复。

WAL growing base 的初始 C 来自真实 CreateSegment 消息的 TimeTick，经已有 AllocSegment 生命周期持久化。首次 data pack 尚未写出 StartPosition 时，L0 筛选可使用这个显式下界；legacy 零值和非 streaming 请求继续保守处理，重试不能覆盖已有更高 C。

存活 Preparing/Up QueryView 的读 schema 引用必须参加 schema GC 保留集，不能仅依赖 segment 引用。注册和 GC 使用同一个 owner 锁，恢复先重建引用再启后台清理。DropCollection 的加载义务撤销应在等待 split lifecycle guard 前完成，后续物理及元数据清理继续受 guard 保护。

Snapshot 等 collection 资源锁期间可能完成 split、rename 或 move。取得锁后重新读取 immutable collection view，并核验所锁 DB/name 仍匹配；构造广播的 channel 列表不得沿用锁前版本。

R6 累计 54 个独立 e2e 场景取得通过记录：功能/查询/实际物理合并 9、恢复/历史/存储故障 22、准入/DDL/TTL/快照恢复 18、MQ/独立集群 3、大 moves/未加载并发 2。真实环境包含精确 30c7552 原生库、MinIO、RocksMQ/Pulsar/Kafka，以及 2 SN/2 QN/2 replica 独立进程集群。39 个主要场景在最终两次构建中复测；14 个额外恢复阶段沿用较早修复版本证据，最后 CDC 保护增加一个独立场景。记录按实际源码、binary SHA、native 和运行目录区分，不声称 54 项均在同一个二进制重跑。

验证包含持续全 DML 下有限 F 退休、同 PK 并发更新的 search/requery 一致性、1100 项真实两页 adoption 的 Prepared 崩溃恢复、单条 19 MB 历史回放与队列溢出、S3 503、提交成功丢响应、Pulsar topic quota，以及不对称三 shard 快照恢复后删写和再次 split。六项问题有真实 e2e 红绿证据；Snapshot 锁前代际竞态另有确定性 service-entry 红绿回归，不冒充窄竞态的真实网络注入。

这些结果不代表生产部署、CDC 数据复制、旧 reader 恢复新 residue 快照、任意历史 iterator 保留、最大规模或长期压力已验收。修复前实验 split 快照缺失 routing 的需要重新生成；生产默认开关和未声明的版本门禁继续关闭。
