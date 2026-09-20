# Index contracts

本目录定义索引的查询、构建、加载和 growing 发布接口。持久化边界见
[`storage/artifact/`](../../storage/artifact/)，growing 实现约束见
[`index/growing/README.md`](../growing/README.md)。`query/`、`build/`、`growing/`
只按职责分目录，不增加 namespace 层级；接口位于 `milvus::index`。Loader 与 builder registry
定义在本目录根部的 `Registry.h` 和 `Registry.cpp`。

## 生命周期与所有权

| 对象 | 职责 | 所有权与生命周期 |
|---|---|---|
| `IIndexReaderBase` + 查询 mixin | 查询一个已经打开的索引 | loader 或 Artifact 转换返回 `unique_ptr`；消费者借用查询接口，并让对应 pin 存活 |
| `IArtifactBuilder<Input>` | 对调用方物化的完整输入执行一次同步构建 | `Build(input) &&` 消费 builder 并返回完成的 Artifact；builder 和 Artifact 均不得保留借用输入 |
| `storage::Artifact` | 持有一次构建的结果 | 暴露 `Serialize`；具体状态与目标 storage generation 是否可序列化由 family 定义 |
| `LoaderEntry` | 派生 metadata-only caps，并从持久化数据打开 reader | registry 按值保存静态函数对，不创建无状态 loader 对象 |
| `IGrowingIndex` + `IAppendable<Batch>` | 接受增量输入并发布可 pin 的读记录 | Segment 唯一持有 owner；`GrowingIndexSnapshotPin` 保留一个发布记录及其依赖 |

一次性 sealed 构建、持久化加载和 growing append 是三个独立生命周期。查询能力是 reader
对象上的 mixin，不把 reader 的共享所有权交给消费者。

## 查询接口（`query/`）

| 文件 | 语义 |
|---|---|
| `IIndexReaderBase.h` | 类型擦除基类、坐标域、数量、值类型和资源自描述 |
| `ReaderCaps.h` | 单个 inventory entry 的 metadata-only 能力描述 |
| `IScalarPredicateReader.h` | 点查与范围查询；字符串输入为调用期间借用的 `string_view` |
| `INullReader.h` | 独立于点查的 null 查询；不单设 caps 位 |
| `IPatternMatchReader.h` | 字符串模式匹配及 typed reader 的 adapter；精确性与每次调用的代价护栏分开表达 |
| `ITextMatchReader.h` | 分词全文查询；支持 text match 不自动表示支持 null 查询 |
| `INgramReader.h` | ngram 候选超集；消费者读取原值并精确验证 |
| `ISpatialReader.h` | MBR 候选超集；消费者用原始 geometry 验证精确关系或距离 |
| `IScalarValueReader.h` | 反查；`Lookup` 返回拥有数据，`Gather` 回调可短暂借用视图 |
| `IJsonIndexReader.h` | 把 path/cast 路由到普通 reader，不定义新的谓词语义 |
| `IVectorReader.h` | 向量搜索、取值、metadata、nullable mapping、refine 与 embedding-list 查询 |

具体 reader 非虚继承 `IIndexReaderBase` 和它实际支持的纯查询 mixin；mixin 不继承基类。
消费者 pin 一次后转换到所需接口，后续查询不依赖具体 family。能力缺失通过 metadata、空 resolve
结果或明确的 Unsupported 表达；打开失败不能伪装成能力缺失。

`ReaderCaps` 只描述一个 entry，不能把多个索引的 bit OR 成不存在的 reader。执行路径先使用
metadata 派生的 caps，打开后再与 `reader.Caps()` 校验。literal 长度等依赖查询输入的判断在
pin 后调用对应接口完成。

`PatternMatchReaderAdapter<Derived, T>` 可被 typed reader 无条件继承，但主模板是空类；只有
`T = std::string_view` 的特化继承 `IPatternMatchReader`。因此数值实例即使出现在同一个模板继承
列表中，也不暴露 pattern-match 能力。

## 位图、NULL 与覆盖边界

- 标量查询位图中 1 表示命中，位图尺寸必须恰好等于 reader 的 `Count()`，坐标由
  `CoordDomain()` 决定。该约定不描述向量搜索结果的形状。
- `INullReader::IsNull()` 和 `IsNotNull()` 只回答同一个 reader 的精确 `Count()` 与坐标域；
  reader 不接收消费者的 active row count，也不为自身域外的行合成 validity。
- 谓词结果用 `(data, valid)` 表示三值逻辑；`UNKNOWN` 是 `(0, 0)`。逻辑 `NOT` 只能翻转
  valid 行的 data，不能把 `UNKNOWN` 变成命中。
- growing reader 的 `CoveredRowEnd()` 是 Segment 行坐标中的完整前缀 `[0, covered)`，与
  `Reader::Count()` 独立。null 行计入 coverage；nested reader 的 element count、已完成的最大
  offset 或累计 append 数都不能代替 coverage。
- 消费者负责把 reader 结果拼入查询可见区间 `[0, active)`。对 `[covered, active)`，只有存在
  语义等价的 raw evaluator 且原始数据可读时，才能精确计算 tail 的 data 和 validity；否则该
  tail 必须保持 `UNKNOWN (data=0, valid=0)`。候选型操作可以临时把 tail 置为全 1 超集，但仅限
  后续必然对这些行执行 raw 精确验证的路径，不能直接输出；validity 在任何情况下都不能补 true。
  并非每种索引操作都有 raw fallback。
- `IS NULL`、`IS NOT NULL` 和它们外层的 `NOT` 同样遵守该规则。索引未覆盖不等于字段为 NULL
  或非 NULL；没有精确 raw null evaluator 时，未覆盖行仍为 `UNKNOWN`，三值 `NOT` 后也仍为
  `UNKNOWN`。

## 坐标、值与 JSON

- nested 索引的 `CoordDomain()` 为 `Element`，`Count()` 数元素而非行。索引不持有列 offsets，
  也不把元素命中折叠为行；执行层使用列 offsets 完成投影并保留各 nullable 层的 validity。
- `CoordDomain` 只编码 `Row`/`Element`，不编码 nested 深度。多层投影必须逐层组合列 offsets 和
  validity；缺少任一层投影上下文时必须明确拒绝该路径。
- 投影位置由查询语义决定。相关 struct 谓词先在同一元素坐标组合，再折叠到行；分别折叠会允许
  不同元素错误地满足两侧。非相关的 `contains(1) AND contains(2)` 允许不同元素满足，因此各自
  折叠后再组合。`NOT contains(1)` 是 `not exists i: x[i] == 1`，不是
  `exists i: x[i] != 1`。
- 输入视图只在调用期间借用。`owned_t<string_view>` 是 `string`，其余为 `T`；压缩结构可能在
  调用栈上重建值，因此 `Lookup` 不能返回悬空视图。
- `JsonResolvedReader` 可以拥有临时 reader 视图或借用子 reader，但两种形式都要求父 reader
  的 pin 存活。`CastTypesOf(path)` 为空表示不支持该形状；非空且路径在所有行都不存在时，
  `Exists` 返回全零位图。
- `CompareOp`、`PatternOp`、`SpatialOp` 使用 contract 本地枚举，plan 或引擎枚举在边界转换。
  JSON 路由使用 `JsonCastType`，不表示所有 `DataType` 都可用。

## 构建、Artifact 与加载

`IArtifactBuilder<Input>` 按完整输入的物理形状模板化。调用方在读取稳定的
`BuilderInputSpec` 后一次性物化输入；builder 可以在同步 `Build` 内多次遍历，但不能要求 cursor、
远端读取或 replay 协议。

- `ScalarBuildInput<T>` 借用稳定的 typed batches；values 与逻辑行对齐并包含 null 行。空
  validity view 表示全有效，不能被下标访问。字符串和数组的传递后备存储须存活到 Build 结束。
- `VectorBuildInput<T>` 借用完整的 dense tensor 或 sparse rows，并明确逻辑行数、物理行数、
  parent validity、可选 embedding offsets 与额外标量分类。有效空列表与 null 行必须可区分。
  `T` 是 registry/引擎分派标签；sparse span 的元素是 owning `SparseRow`。
- `PreparedVectorBuildFiles<T>` 借用完整 raw 文件与可选 sidecar。调用方保持输入文件稳定直到
  Build 返回或抛错；Artifact 的输出 staging 独立拥有。`scalar_info_path` 区分未交付、已交付但
  无文件、以及实际文件路径三种状态。

Hybrid 在一次 Build 内选择 concrete family，并让该 builder 消费同一个完整输入；调用方无需
重放或保存第二份完整列。Artifact 记录 selector，加载端先解析 selector，再查找 concrete loader。

`storage::Artifact::Serialize(FileSink&)` 只把逻辑 named entries 或本地文件交给 sink；sink 负责
transport、切片、命名与 publication metadata，上传编排属于调用方。接口存在不代表任意 Artifact
状态都支持任意 storage generation；不支持的组合必须明确失败。`IReaderConvertible` 是可选的
消费式能力：`FromArtifact` 接管 Artifact 后检查能力并调用 `IntoReader() &&`，不隐式执行
serialize/load 或其他 IO。调用方同时需要持久化和直接查询时，必须先完成持久化，再消费 Artifact。

`LoaderEntry::open` 通过 `FileSource` 独立打开持久化数据并返回
`unique_ptr<IIndexReaderBase>`；loader 不依赖 builder 或原 Artifact。`FileSource` 负责把逻辑 entry
解析为 buffer、本地文件或 file-backed handle。`PutMeta`/`GetMeta` 保留 JSON 类型；
`LoadOptions::params` 是单次打开参数，不由 storage 持久化或解释。

Builder/Reader 不接收 Segment、executor、列 cursor 或 `FileManagerContext`。JSON shredding、列
zone map、元素 offsets 和 segment 级能力聚合也不属于索引查询 contract。向量 contract 可以使用
knowhere 类型，共享/标量 contract 不增加 knowhere 依赖。

`CellByteSize()` 描述打开 reader 所拥有的 heap 与 file-backed 资源。实现若使用全零 unavailable
sentinel 或约定的 post-load estimate，必须在其契约中明确；调用方不能把 sentinel 当作实测零，
也不能按每个 pin 重复计费。`LoadOptions::estimated_bytes` 等加载前准入估算与 reader 打开后的
所有权计账是两个独立值。

## Registry 与扩展规则

- `LoaderRegistry` 以 family key 保存 `{derive_caps, open}` 静态函数对。`derive_caps` 只读加载
  metadata，不打开 payload；未知 family 返回空 entry。
- `BuilderRegistry<Input>` 按完整输入类型隔离 factory。factory 立即解析自己的 typed 参数；
  registry 不解释 family 参数，也不擦除输入形状。未知 family 或不支持该输入形状时返回空指针。
- 新 family 在自己的实现 translation unit 注册 builder/loader，并保证该 translation unit 进入最终
  链接产物。loader 派生的 caps 必须与打开后的 `Reader::Caps()` 一致。
- 新查询能力应作为独立 mixin；只有路径选择需要在打开前识别的能力才加入 `ReaderCaps`。实现必须
  同时声明结果是 exact 还是 candidate superset，以及由哪一层完成精确验证。
- Artifact 的可序列化 generation、可直接转换能力和失败语义由 concrete state 显式定义，不从
  family 名称推断，也不通过隐藏 IO 补齐。

## Growing 发布协议

- `IAppendable<Batch>` 是独立的输入 mixin。`ScalarBatch<T>`、`TextBatch`、`VectorBatch<T>` 是
  调用期间借用的平坦视图，`Append` 返回前必须完成消费或复制。嵌套输入需要显式 offsets/validity。
- Append 成功表示输入已接受，不表示一定已经发布。写侧负责串行化 append/commit 或满足 family
  的并发协议；查询正确性只依赖发布记录与 coverage，不依赖具体 family 的提交节奏。
- 每个发布记录唯一拥有一个 const reader，并原子固定 `Reader::Count()`、`CoveredRowEnd()`、
  validity/offset mapping 和依赖生命周期。`const` 不自动保证底层引擎不可变或 Add/Search 并发安全。
- `PinSnapshot()` 只 pin 已发布记录。空 pin 的 coverage 为 0；非空 pin 可复制或移动，移出后为空。
  reader 引用和 query-interface 指针不能越过 pin 生命周期。获取 pin 时 owner 必须仍受保护，获取后
  pin 可独立存活。
- coverage 单调不减且不能跨越行号空洞。发布构造、分配或校验失败不得替换当前记录；旧记录在
  发布锁外释放，已有 pin 继续保留其 reader 与依赖。
- `CommitIfNeeded()` 给 interval writer 一个查询前提交点；错误原样传播，不能用旧 pin 掩盖。
  `Flush()` 是强边界：已经构建的 owner 成功返回时，所有已接受行必须进入发布记录；尚未达到
  family 构建阈值的 owner 可以保持空 pin。
- Knowhere 向量首次 cold build 只有在从未发布 engine 且完整 raw source 仍可查询和重试时，才可
  保留空 pin。engine 建成后的 Add 失败必须传播且不得把失败输入发布；若 Add 已成功而 publication
  失败，重试只能发布已接受状态，不能再次 Add。
- 不可变引擎的发布记录固定引擎视图。共享单活 Add/Search 引擎的记录可以观察到后续 ANN 状态，
  但每个 pin 的逻辑/物理前缀、Count、coverage、mapping 和依赖仍必须固定，且引擎负责并发安全。
- growing 向量 ANN 只有在 snapshot coverage 包含完整 query-visible 行前缀时才可使用；否则整次
  查询走 raw vector fallback，不能把 ANN 前缀与未覆盖 tail 拼接。使用 ANN 时，search、iterator、
  value lookup 和 raw refine 都限制在同一 query-visible 前缀并保留同一个 pin。
- growing 标量/文本/空间消费者按照“位图、NULL 与覆盖边界”一节合并已覆盖前缀和未覆盖 tail。
  缺少对应 raw evaluator 时 fail closed 为 `UNKNOWN`，不能假定所有能力都有 raw fallback。
- growing 初始化、append、commit 和发布不消费 Artifact；发布记录由 `IGrowingIndex` 管理。
