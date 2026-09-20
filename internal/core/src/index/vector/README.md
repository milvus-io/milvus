# Vector indexes

本目录按查询、构建、产物和加载拆分向量索引对象。共享协议见
[`contracts/README.md`](../contracts/README.md)，增量写入见
[`growing/README.md`](../growing/README.md)。拆分职责本身不要求改变 knowhere 算法或持久化格式；
这不代表尚未接通的实现已经完成行为或性能验证。

## 文件与职责

| 文件组 | 职责 |
|---|---|
| `KnowhereEngine` | 持有 native engine、完整 backing owner 及实际类型、metric、dim、physical/embedding-list 状态 |
| `VectorIndexValidDataUtils` | nullable 行有效位图的编解码，以及把它发布进 knowhere IdMap（#50524，映射本身由 knowhere 持有） |
| `contracts/query/IVectorReader.h` | 统一向量查询接口，包含搜索、取值、metadata、nullable、refine 与 embedding-list 操作 |
| `VectorIndexReader` | 非模板统一 reader；运行时仅区分 memory/disk 搜索外壳与 DiskANN beamwidth，物理类型只在取值 leaf 分派 |
| `VectorMemBuilder` | `IArtifactBuilder<VectorBuildInput<T>>`，一次接受完整输入，调用方持有 tensor 及 side inputs |
| `VectorDiskBuilder` | `IArtifactBuilder<PreparedVectorBuildFiles<T>>`，由 knowhere 读取已准备的完整文件 |
| `VectorMemArtifact` | BinarySet 具名逻辑条目；支持消费已构建 engine/validity 直接生成 reader |
| `VectorDiskArtifact` | 仅按路径序列化大文件；查询必须经 `VectorDiskLoader` 打开持久化产物 |
| `VectorMemLoader` | materialize 与 mmap 两种打开方式 |
| `VectorDiskLoader` | 磁盘索引加载及流式后端对接 |
| `VectorLoadUtils` / `VectorParamUtils` | 共用整数语法解析，不合并 mem/disk 各自的缺失值、别名与类型策略 |
| `VectorReaderUtils` | 共用 dense 与 embedding-list 取回流程；不持有 engine 或 reader |
| `storage::LocalDirectory` | 直接持有 loader/builder 新建的 mmap 或 disk 子目录，不拥有配置的 parent |
| `RangeSearchParams` | 向量专用的 range-search 参数准备，隔离共享 scalar helper 的依赖 |
| `VectorFamilies` | 注册无状态 loader 和已接通的 typed builder |

## 查询与生命周期

`VectorIndexReader` 同时继承 `IIndexReaderBase` 与纯查询 mixin `IVectorReader`，并按值持有
engine/validity。
`KnowhereEngine` 内的 backing owner 先于 native handle 声明，使 native node 在 mmap 文件或
FileManager generation 的最终 owner 之前析构。
搜索只接收 `VectorSearchParams` 的参数、metric、topk、trace；可见性过滤、逻辑/物理坐标转换、
元素到行的聚合和结果处理由消费者完成。

`IVectorReader` 同时提供 metric/dim/knowhere 类型及 iterator 参数准备、借用的 nullable offsets、
距离重算和 embedding-list 取值。统一接口不代表每个后端或物理类型都支持所有操作；运行时检查
和原 Unsupported 路径仍是实际能力边界。
`CoordDomain` 保持 Row：VECTOR_ARRAY 的 element-level search 是单次查询模式，knowhere 返回的 element ID
由消费者结合该查询的 array offsets 转成 `(row, element)`，不把 reader 的 inventory 坐标永久改成 Element。

Growing reader 在发布时固定物理 Count、nullable mapping 和搜索默认参数。Search/Range 在调用 knowhere
前用非空 bitset 长度限制可见物理前缀；更短的查询可见前缀仍由消费者提供。共享 live engine 允许
Add 改变近似搜索的遍历，但旧 reader 不会返回其固定前缀之后的 ID，这属于逻辑前缀 pin，不能表述为
底层 ANN engine 的物理不可变快照。

`Iterators` 返回的 knowhere iterator 不携带 reader pin，而且借用传入 bitset；延后消费的 merge iterator
还可能保存 reader offset mapping 的裸指针。Growing 消费者必须把同一个 `GrowingIndexSnapshotPin` 和
物化后的 prefix bitset 与结果一起保活，直到 iterator 消费结束；不能仅靠 iterator 的共享句柄认定生命周期安全。

## 持久化与 IO

- 内存 Builder 接收完整 tensor、parent validity、可选 embedding offsets 和标量分类组。
  调用方按 `InputSpec().side_inputs` 预检并交付实际数据；生产输入由物化器持有 compact tensor，
  Builder 在同步构建期间借用输入，不在其内部再存一份 raw 缓冲。
- DiskANN 的完整输入文件由调用方保留到同步 Build 结束；输出 staging 单独持有。
  输入文件与有效性元数据不因 Artifact/Reader 后续存活而被借用。
- 内存 artifact 写逻辑条目；切片、组装和传输命名属于 source/sink。当前不实现 packed V3 vector 格式。
- mmap 加载将有序 engine 条目流式拼接到本地文件。embedding-list sidecar 保持独立文件，
  validity/empty-list 元数据单独解析。FileSource 仅在打开期间借用，reader 必须保留其后备文件。
- DiskANN 大文件按路径流式处理；仅 V1/V2 DiskFiles index source 可创建 disk-engine handle。
  loader 先用 LocalFiles handle 探测能力，stream-load 后端再改用限制在 engine inventory 内的
  RemoteStreams handle；reader 保留最终选中的 handle 及其 FileManager 和本地 generation。
- `IArtifactBuilder` 以 `Build(input)` 返回完成的 Artifact，Artifact 负责序列化，构建服务编排上传和
  `FileSink::Finish()`。只有 `VectorMemArtifact` 提供一次性消费转换：engine 与 validity 被移动到独立 reader；
  `VectorDiskArtifact` 不提供该能力，只能先序列化，再由 `VectorDiskLoader` 打开。
  本地 staging 的清理由实际持有者负责，不能删除仍被 reader 使用的文件。

## 已知未完成项

- 完整输入与 typed builder 注册已在源码接通；本轮接口合并未编译或测试，运行与性能尚未验证。
  当前不支持的多额外字段和 VECTOR_ARRAY 额外标量字段组合仍会被拒绝。
- `ReaderCaps` 缺少向量 raw-value/refine 能力位；默认 false 查询位和 exact=true 不能替代完整能力判断。
- `IVectorReader` 同时提供 dense/sparse getter，磁盘后端的 sparse retrieval 仍返回 Unsupported。
- reader 的 `MemoryUsage` 和 `CellByteSize` 返回显式记录的 unavailable-zero sentinel，不表示
  knowhere resident/file footprint 实测为零。`LoadOptions::estimated_bytes` 与
  `VectorLoadResource`/`IndexLoadResource` 继续负责预加载准入；live sealed translator 和 growing
  segment 的既有估算与安全系数不变。按 backend 精确拆分原生 memory/file 属于 follow-up，
  不阻塞当前生产接线验证。
- Growing owner 的初始 Build、后续 Add、发布和 consumer 路由已完成静态接线，本轮未编译或测试。发布记录固定
  reader Count、mapping 和 CoveredRowEnd；它与后续 Add 共享 live engine，因此只承诺上述逻辑前缀，
  不承诺近似 ANN 遍历结果是物理稳定快照。

迁移 TODO 中引用的旧 VectorIndex/VectorMemIndex/VectorDiskIndex 文件位于历史提交 `e255009e01`，
可通过 `git show e255009e01:internal/core/src/index/<file>` 查阅；上述本地说明不依赖迁移设计文档。
