# Index contracts

本目录定义索引的查询、构建、加载和 growing 发布接口。配套的持久化接口见
[`storage/artifact/`](../../storage/artifact/)，growing 实现约束见
[`index/growing/README.md`](../growing/README.md)。接口语义应在代码注释或组件 README 中完整说明。

文件按职责放在 `query/`、`build/`、`growing/`；静态加载函数对及 builder registry
定义在根目录的 `Registry.h`。目录组织不增加 namespace 层级，接口仍位于
`milvus::index`。

## 生命周期与所有权

| 对象 | 职责 | 所有权与使用方式 |
|---|---|---|
| `IIndexReaderBase` + 查询 mixin | 查询已打开的索引 | load entry/artifact 返回 `unique_ptr`；消费者借用接口，并保留对应 pin |
| `IArtifactBuilder<Input>` | 对完整输入执行一次同步构建 | `Build(input) &&` 返回完成的 Artifact，之后不再使用 builder |
| `storage::Artifact` | 持有构建产物并暴露序列化接口 | 具体模式是否支持导出由 family 决定；仅 Text 与内存向量产物额外支持消费式转换为 Reader |
| `LoaderEntry` | 从元数据派生 caps，并从持久化数据打开 reader | registry 按值保存静态函数对；不创建无状态 loader 对象 |
| `IGrowingIndex` + `IAppendable<Batch>` | 长期接收增量输入，发布固定读记录 | Segment 唯一持有 owner；查询持有 `GrowingIndexSnapshotPin`；加载边界用 `Flush` 强制发布已构建 owner 的已接受前缀 |

一次性原地构建不是 growing append。查询接口是 reader 对象上的视图，不是每次查询创建的代理，
也不把 reader 的共享所有权交给消费者。sealed 缓存与 inventory 的接入状态见
[`segcore/indexing/README.md`](../../segcore/indexing/README.md) 及对应实现中的 TODO。

## 查询接口（`query/`）

| 文件 | 语义 |
|---|---|
| `IIndexReaderBase.h` | 类型擦除基类、坐标域、数量和 reader 自描述 |
| `ReaderCaps.h` | 加载期元数据推导的单个索引能力；选择路径前不 pin |
| `IScalarPredicateReader.h` | 点查与范围查询，输入字符串为借用的 `string_view` |
| `INullReader.h` | 独立于点查的 null 查询；空间等标量类型也可提供，不单设 caps 位 |
| `IPatternMatchReader.h` | 精确字符串模式匹配及 typed reader 的 string-view-only CRTP adapter；具体 literal 的代价判断在 pin 后进行 |
| `ITextMatchReader.h` | 分词全文查询；独立 text artifact 不隐含 `INullReader` |
| `INgramReader.h` | ngram 候选超集；消费者读取原值并精确验证 |
| `ISpatialReader.h` | MBR 候选超集；精确几何关系及距离由消费者验证 |
| `IScalarValueReader.h` | 反查；`Lookup` 返回拥有数据，`Gather` 回调可短暂借用视图 |
| `IJsonIndexReader.h` | path/cast 路由到普通谓词 reader，而非新的谓词语义 |
| `IVectorReader.h` | 统一向量 reader：搜索、取值、metadata、nullable mapping、refine 与 embedding-list |

标量实现类非虚继承 `IIndexReaderBase` 和支持的纯查询 mixin；mixin 不继承该基类。
向量使用同一模式：`IVectorReader` 是纯查询 mixin，具体 `VectorIndexReader` 同时继承
`IIndexReaderBase` 与 `IVectorReader`。pin 后只做一次基类到所需查询接口的转换，
后续查询无需知道具体 family。
能力缺失通过元数据或空接口结果表达；加载失败不能伪装成能力缺失。

`ReaderCaps` 是每个 inventory entry 的描述，不能把不同索引的位 OR 成一个不存在的组合。
路径选择使用缓存的元数据；打开后 `Caps()` 用于一致性校验。literal 长度和 FM 的 count-first
代价护栏依赖查询输入，因此仍在 pin 后询问对应查询接口。

## 值、坐标与 JSON

- 谓词位图中 1 表示命中，尺寸等于 reader 的 `Count()`。这条约定不描述向量搜索结果的形状。
- nested 索引的 `CoordDomain()` 为 `Element`，`Count()` 数元素而非行；索引不持有列 offsets，
  也不把元素命中折叠为行。元素到行的投影属于执行层，offsets 来自列而非 reader。
- 投影位置由查询语义决定。相关 struct 谓词必须先在同一元素坐标上组合，再折叠到行；分别
  折叠会错误地允许不同元素满足两侧。非相关的 `contains(1) AND contains(2)` 则允许不同元素，
  因此每个谓词先独立折叠到行再组合。`NOT contains(1)` 表示 `not exists i: x[i] == 1`，
  不能改写成 `exists i: x[i] != 1`。
- 多层 nested 投影需要逐层组合 offsets，并在每个 nullable 层保留 validity；`CoordDomain`
  只区分行与元素，不编码嵌套深度。这是共享 projection 后续整理必须遵守的约束，不表示当前
  所有执行路径已经普遍支持多层投影。
- 输入视图仅在调用期间借用。`owned_t<string_view>` 为 `string`，其余为 `T`；压缩字典可能在
  栈上重建原值，因此 `Lookup` 不能返回悬空视图。
- `JsonResolvedReader` 可以拥有临时接口视图或借用子 reader，但使用期间仍须 pin 住父 reader。
  `CastTypesOf(path)` 为空表示不支持该形状，非空但该路径所有行都缺失时 `Exists` 返回全零位图。
- `CompareOp`、`PatternOp`、`SpatialOp` 使用本地枚举。plan 或引擎枚举在边界转换。
  JSON 路由使用 `JsonCastType`，不能擅自扩大成所有 `DataType` 都受支持。

## 构建、加载与依赖边界

`IArtifactBuilder<Input>` 按完整输入类型模板化，只有一次同步 `Build(input) && -> Artifact`。
调用方准备并保留输入，不向 builder 暴露 cursor、远端读取或 replay 协议：

- `ScalarBuildInput<T>` 借用稳定的 typed batches，values 按逻辑行对齐，包括 null 行。
  validity 可直接借用 packed/expanded view，空 view 表示全有效，读取时须判断是否为空。
  字符串/数组的传递后备数据与 batch 数组都须保留到 Build 结束；不要求再复制为连续标量数组。
- `VectorBuildInput<T>` 借用一次物化的完整 tensor/sparse rows，并交付逻辑/物理行数、parent
  validity、可选 embedding offsets 和额外标量分类。有效空列表与 null 行不能混淆。
  `T` 用于引擎分派；实际 span 元素在 dense 时为 `T`，在 sparse 时为
  `knowhere::sparse::SparseRow<float>`，不能把 sparse 类型标签作为行对象复制。
- `PreparedVectorBuildFiles<T>` 交付完整 raw 文件和可选 sidecar；调用方保留文件到 Build
  返回或抛错，构建产物自己的输出 staging 独立存活。scalar-info 的未交付、已交付但无文件、
  实际路径三态保持区分。

`BuilderInputSpec` 构造后稳定，只声明实际所需的 `side_inputs`；默认实现返回空集合，内存与磁盘
向量 builder 根据配置/引擎需求覆盖它。额外字段的读取须按引擎能力预检，其值通过具体输入类型
交付。Hybrid 在同一次 Build 内统计基数并让选中的 builder 消费
同一完整输入；不要求调用方重放，不缓存第二份完整原始列。产物记录选型，持久化 selector
解析出 concrete family 后再 lookup 对应 load entry。
Builder 和返回的 Artifact 不保留借用输入；独立构建出的索引状态和后备文件可以正常共享。

`LoaderEntry::open` 指向 family provider 的静态函数，直接返回
`unique_ptr<IIndexReaderBase>`。加载只产生 Reader，不提供加载后
重新发布的 Artifact。所有 Artifact 都保留 `Serialize` 接口，具体构建模式是否支持持久化由
family 决定；Text 的 RAM interim 模式没有 directory-backed 产物，不能导出。仅 Text 与内存向量
Artifact 实现 `IReaderConvertible`，通过消费式 `IntoReader() &&` 转移已构建状态，
`IReaderConvertible::FromArtifact` 负责能力检查。仅当产物模式支持
持久化且调用方同时需要持久化和直接查询时，才必须先 serialize/publish，再消费 Artifact；
消费成功或失败后都不能重试转换或序列化。
能力缺失返回 Unsupported，不自动 serialize/load，不增加隐藏 IO。持久化产物仍通过
load entry → Reader 打开。

- Builder/Reader 不接收 Segment、executor、列 cursor 或 FileManagerContext；调用方完成输入
  IO 与物化，load provider/Artifact 通过 `FileSource`/`FileSink` 打开或序列化，上传由构建服务编排。
- `FileSink::PutMeta` / `FileSource::GetMeta` 保留 JSON 的 bool/integer/array/string 类型。
- `LoadOptions::params` 是单次运行参数，不由 storage 持久化或解释。
- `IIndexReaderBase::CellByteSize()` 在原生计账可用时报告已打开对象实际拥有的资源：堆结构计入
  memory，mmap/文件后备字节计入 file。暂时无法取得原生计账的 family 可以显式记录全零为
  unavailable sentinel；调用方须保留其 translator/segment 旧估算，不能把 sentinel 当成实测零，
  也不能按每个 pin 重复收费。加载前准入估算仍由 translator、`IndexLoadResource` 与
  `LoadOptions::estimated_bytes` 传递，不能冒充 reader 的实测值。
- index contracts 和 family 不直接依赖缓存实现；L1 artifact 边界复用
  `cachinglayer::ResourceUsage`。向量接口可使用 knowhere 类型，共享/标量接口不增加直接 knowhere include；
  `common/Types.h` 的既有传递依赖仍需单独整理。
- JSON shredding、列 zone map、元素 offsets 和 segment 级能力聚合不属于索引查询契约。
- IGrowingIndex 的初始化、append、commit 和读记录发布不消费 Artifact；发布记录由
  IGrowingIndex 自己管理。

## Growing 发布与输入约束

- Segment 唯一持有 IGrowingIndex；每个发布记录唯一持有 const reader。记录固定 Reader::Count、CoveredRowEnd、validity/offset mapping 和依赖生命周期，但 `const` 不自动保证底层引擎不可变或 Add/Search 并发安全。GrowingIndexSnapshotPin 内部保留记录生命周期，不向消费者暴露共享拥有指针。
- GrowingIndexSet 在 PinSnapshot 前调用 owner 的 CommitIfNeeded，使 interval writer 在查询到来时提交已到期的最后一批；同步发布型实现可使用默认 no-op。提交错误原样传播，不退回旧 pin 掩盖失败。`Flush` 是更强的同步边界：已构建 owner 成功返回时，所有已接受行必须已进入发布记录；尚未达到 family 构建阈值的 owner 可以保持空 pin，由消费者走 raw fallback，Flush 不为满足统一接口而强制提前构建。Knowhere 首次 cold Build 失败也只在从未发布 engine 且完整 raw 仍可查询/重试时允许安全空 pin；已构建 engine 的 Add 失败不属于该例外。PinSnapshot 本身只固定已发布的 reader 与 CoveredRowEnd，不提供独立 watermark getter。pin 可复制/移动，移出后为空；Reader 要求非空 pin，返回的引用与能力指针不得越过 pin 生命周期。空 pin 的覆盖边界为 0。
- CoveredRowEnd 表示完整的 Segment 行前缀，不是 Reader::Count、本次元素数量或最大 reserved offset。null 行计入覆盖；乱序完成不能越过空洞。
- 公共基类在一把短锁下发布/获取整个记录，旧记录在锁外释放，已有 pin 保留其 reader 与依赖。分配或发布检查失败不替换当前记录；若 Knowhere Add 已成功，重试只发布已接受状态，不能再次 Add。
- Tantivy 与 R-Tree 发布不可变引擎视图。Knowhere 采用单活引擎继续 Add/Search；旧 pin 的 ANN 命中允许随引擎推进而变化，但其 Count、CoveredRowEnd、validity/offset mapping 和依赖生命周期不得变化。所有向量 search/iterator 必须在生成 candidate/top-k 前，把逻辑上界限制为 `min(query-visible row end, pin.CoveredRowEnd())` 并转换成同代 physical prefix；value lookup、raw refine 与 deferred iterator 使用同一边界，deferred 路径保留原 pin。
- 写侧负责串行 append/commit 或满足 family 的并发协议。单活 Knowhere engine 还必须由引擎保证 Add/Search 并发安全；已构建 engine 的 Add 失败 terminal-poison owner、传播原错误且不推进 feed/ACK，也不能重放该输入。首次 cold Build 在完整 raw 仍保留且从未发布 engine 时可以安全退回 raw；这项例外不适用于 Add。
- IAppendable<Batch> 是独立的输入 mixin。ScalarBatch<T>、TextBatch、VectorBatch<T> 是调用期间借用的平坦数据视图；不包含查询能力。不为新增 Reader 能力定义新的 Growing 接口。嵌套输入需显式 offsets/validity 视图。
- Append 接受输入与发布是两件事；提交节奏只属于写侧。未覆盖尾部、查询可见性、缺少查询能力时的 fallback 均由消费者处理。
- 获取 pin 的调用需要 owner 生命周期保护；获取后的 pin 可以独立存活。

## 生产接入状态

Growing text、R-Tree 与支持的 Knowhere interim vector 已接入 Segment
owner/feed/snapshot pin；普通 scalar 未启用。Vector pin 固定逻辑/物理前缀和依赖，
不固定共享 live engine 的 ANN 命中；生产 owner、typed pending feed、search/raw-data 与 deferred
iterator 消费者代码已经迁移，但尚未完成全量构建与基础 e2e 验证。
ngram/JSON flat 的尾部处理策略须由消费者明确；标量 growing 接入时还需消除表达式按
`size_per_chunk_` 假设造成的边界问题。向量 reader 的原生精确计账仍是 follow-up；当前 sealed
加载保留 translator 的准入估算，growing 保留 segment 级估算，二者都不能冒充 reader 实测计账。
向量能力描述仍需后续补齐。
