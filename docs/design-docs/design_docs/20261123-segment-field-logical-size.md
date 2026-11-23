# Storage V3 段内各字段的逻辑大小

本设计基于 `milvus-io/milvus` 的 `master` 提交 `163abc5e40`。Go V3 flush、Import V3 和 compaction 为每个实际输出段按 `FieldID` 记录已持久化的插入值。统计随该段的 manifest 和行数一起发布到 `SegmentInfo.stats`。后续可以用这些数据计算查询涉及的字段规模。查询价格、QueryNode direct flush 和 V1/V2 写入不在本期范围内。

段收到删除日志时，旧值仍在段内，字段大小也仍包含这些值。Compaction 实际滤除删除行或 TTL 过期行后，新段只统计保留的行。改变压缩、列组或分批方式，对同一组值应得到相同结果。**在相同输入、路由和 LOB 策略下，计量新增的 LOB 正文读取请求数和读取字节数都必须为零。**

## 计量单位

每个已知字段有两个无符号计数。`payload_bytes` 记录实际字段值的载荷字节，`validity_bits` 记录逻辑上可空的位置数。汇总同一字段在多个批次或段内的大小时，先对两个计数分别求和，再算 `sum(payload_bytes) + ceil(sum(validity_bits) / 8)`。按批次分别将有效位取整，会让相同数据因分批不同而得到不同结果。

| 类型 | 每个非 NULL 值的 `payload_bytes` |
| --- | --- |
| `Bool`、`Int8` | 1 B |
| `Int16` | 2 B |
| `Int32`、`Float` | 4 B |
| `Int64`、`Double`、`Timestamptz` | 8 B |
| `FloatVector` | 维度为 `d` 时占 `4d` B。 |
| `Float16Vector`、`BFloat16Vector` | `2d` B |
| `Int8Vector` | `d` B |
| `BinaryVector` | `ceil(d/8)` B |
| `SparseFloatVector`，含 BM25 输出 | 每个实际条目 8 B，由 4 B 维度编号和 4 B 浮点值构成。`{2: 0.5, 100000: 1.0}` 和 `{7: 0.8}` 两行合计 24 B。 |
| `VarChar`、旧版 `String`、`Text` | UTF-8 正文字节数。「猫」占 3 B，空串占 0 B。 |
| `JSON`，含动态字段 `$meta` | 实际写入的 JSON 文本字节，含其中的空白和转义。 |
| `Geometry` | 实际 WKB 字节数。 |
| `Array`、嵌套数组、`ArrayOfVector` | 按实际叶子元素递归累加。V3 将 `ScalarField` 编码为二进制列时也使用元素的逻辑宽度。 |
| `StructArray` | 每个写出的叶子按自己的 ID 计量。父字段 ID 的载荷为 0 B，仅承载共享的父行有效位。 |

每个 NULL 值的载荷为零。可空字段即使全列均非 NULL，每行也计一个有效位。非 NULL 数组的可空元素另计有效位，NULL 父值没有子元素位置。例如，可空 `Array<Int16?>` 中的 `[7, NULL]`、`[]`、`NULL` 三行共 2 B 载荷和 5 个有效位。主键、RowID、时间戳、动态字段、函数输入、已物化函数输出和已写入的默认值均按各自字段 ID 计量。仅读取时补齐的默认值没有条目。计量排除 Arrow offsets、缓冲区容量、对齐、protobuf 标签、列组包装、压缩、索引和统计文件。[字段计量](../../../internal/storage/field_logical_size.go) · [数组与向量编码](../../../internal/storage/serde.go)

## 元数据与精确性

`Statistics.field_logical_sizes` 使用协议字段号 17。`FieldLogicalSizes` 含 `version`、`row_count` 和以 schema `FieldID` 为键的 `fields` 映射。`FieldLogicalSize` 含 `uint64 payload_bytes` 和 `uint64 validity_bits`。版本 1 的每个条目都必须覆盖同一次提交的全部 `row_count` 行，权威行数来自段记录。[协议定义](../../../pkg/proto/data_coord.proto)

DataCoord 将已提交的 manifest 基目录和修订号记在 `SegmentInfo.field_logical_sizes_manifest`（字段号 44）。基目录必须非空，修订号必须大于零。恢复时核对基目录、修订号和段行数。旧版 DataCoord 可能保留不认识的 protobuf 字段，却在行数相同的情况下替换正文。再次升级时，标识缺失、无法解析或不匹配都会使旧统计失效。导入、快照复制和 CDC 在目标端将统计绑定到本地段的 manifest。[目录恢复](../../../internal/datacoord/meta/meta.go) · [manifest 标识](../../../internal/datacoord/meta/field_logical_sizes.go)

| 元数据 | 如何读取 |
| --- | --- |
| 版本和行数匹配，字段 ID 有条目 | 该字段总量准确，包括显式 `{0, 0}`。 |
| 版本和行数匹配，字段 ID 无条目 | 该字段大小未知。其他字段仍可有准确总量。 |
| 子消息缺失、版本无法识别或行数不匹配 | 这份快照不能用于计算字段大小。 |

追加两个非空批次时，只累加两侧都有准确条目的字段。一个段有 100 行历史数据且没有计量，再写入已计量的 20 行，不能用后 20 行代表全部 120 行。零行同步保留已有总量。每个计数单独检查溢出。`StatisticsCollector` 的 `Clone` 和 `Publish` 返回独立的数据，失败尝试不能改动已确认的统计。[累计算法](../../../internal/storage/field_logical_size_snapshot.go) · [统计收集器](../../../internal/storage/statistics.go)

## 写入和发布

| 路径 | 取得字段值和提交统计的位置 |
| --- | --- |
| Go V3 flush | `packedRecordBatchWriter` 或 `packedTextBatchWriter` 计量投影后的 Arrow 列。写入和关闭成功后，`BulkPackWriterV3` 取得批次计数。manifest 提交成功后，`finalizeStats` 才在收集器副本上调用 `DigestWithFieldSizes`。同步任务收到 DataCoord 确认后安装副本。[V3 flush](../../../internal/flushcommon/syncmgr/pack_writer_v3.go) · [收集器封装](../../../internal/flushcommon/metacache/segment_stats.go) |
| WAL L1 检查点 | `flushPack.PreviousRows` 从 `durableMeta` 捕获上一次确认的行数。当前数据块的行数随后计入本次上报的 `Meta.Stat.ModifiedRows`。恢复收集器要用前者，否则第二次 flush 会把当前批次也算进历史前缀。`pendingFlushChunk.persisted` 保存已写出的 manifest 和累计统计，确认超时后重发同一结果。恢复跳过检查点已覆盖的 TimeTick。`CommitL1Segment` 传递最终累计统计。[段视图](../../../internal/streamingnode/server/wal/vchannel/segment/view.go) · [WAL 写入器](../../../internal/streamingnode/server/wal/vchannel/segment/pack_writer.go) · [提交与恢复](../../../internal/streamingnode/server/wal/vchannel/segment/lifecycle_writer.go) |
| Import V3 | `executeImportPlan` 只从成功关闭、生成最终 manifest 的 writer 获取字段总量，随 `ImportTaskV3Result.statistics` 提交。中间重分片和归并文件不进入结果。DataCoord 核对 manifest 和结果行数，然后保存段统计。[导入执行](../../../internal/datanode/importv3/import_plan_executor.go) · [结果提交](../../../internal/datacoord/bulkimport/result.go) |
| 完整 compaction | 每个实际输出 writer 独立累计。Mix、归并排序和聚簇先过滤删除及过期行，再按行实际到达的段计量。排序、namespace 和完整 schema 重写从各自的最终输出获取快照。列组轮换和文件 flush 不改变段归属。`CompactionSegment.stats` 携带该段总量。[段写入器](../../../internal/datanode/compactor/segment_writer.go) · [排序](../../../internal/datanode/compactor/sort_compaction.go) · [输出统计](../../../internal/datanode/compactor/stats.go) |
| 原地物化字段 | 部分写入器只接收 `outputFields`。`SetFieldLogicalSizeSchema` 提供目标 schema 的父子关系，以原有物理列序计量叶子和父行有效位。字段号 18 的 `field_logical_size_update_ids` 列出全部受影响 ID。DataCoord 用准确值替换对应条目，移除已改写但大小未知的条目，保留其他列。只提升 schema 版本、只更新删除日志或写 L0 时，原段的插入字段总量继续有效。[原地物化](../../../internal/datanode/compactor/bump_schema_version_compactor.go) · [字段合并](../../../internal/datacoord/meta/stats_delta.go) |

## TEXT 与外置 LOB

`Text` 是字段类型。LOB 是存储长正文的方式。默认内联阈值为 65,536 B。内联引用为 `0x00` 加正文。外置引用固定为 24 B，由 `0x01`、3 B 填充、16 B 文件 UUID 和 4 B 文件内行号组成。文件内行号无法给出正文长度。此实现沿用 `TextLobRef` 和现有编码。[落盘格式](20260407-text_lob_storage.md) · [阈值和策略](../../../internal/compaction/lob_compaction.go)

**新写入、导入和 `REWRITE_ALL`。** `milvus-storage` 的 `LobColumnWriter` 在原有写入流程中已经按列累加 `total_bytes`，包括内联和外置正文。`SegmentWriter` 成功关闭后、销毁句柄前，Go 通过 `loon_segment_writer_get_text_column_stats` 一次取回各列统计，按 `total_entries == rows_written` 核对覆盖范围。Go 根据目标 schema 计算有效位。Milvus 的 CMake 对固定版本 `95215819ea9094f7b96be0f9503346dfee0b62e5` 应用随仓库提供的补丁。[固定依赖和补丁](../../../internal/core/thirdparty/milvus-storage/CMakeLists.txt) · [Go FFI](../../../internal/storagev2/packed/segment_writer_ffi.go)

**`REUSE_ALL`。** 写入器从 Arrow 有效位判断 NULL，再按引用首字节识别内联值并减去标志字节。23 B 内联正文和外置引用编码后都长 24 B，不能凭长度区分。若输出包含外置引用，只有在源字段已物化、源快照准确，而且源列的全部行和值原样进入同一个实际输出段时，才可继承该列总量。归并排序记录每个实际弹出并追加的源行，成功写入后才归入输出段。一个输出可同时继承某输入的完整列，并加上另一输入中可直接计量的内联值。两组行必须互不重叠。只要仍有无法计量的外置值，就省略这个输出段的这个字段。[输入来源和继承](../../../internal/datanode/compactor/text_size_lineage.go) · [归并排序](../../../internal/storage/sort.go)

策略选择沿用原流程。普通聚簇和预计多输出的 Mix 强制重写。预计至多一个输出的 Mix 按字段计算空洞率 `1 − ΣValidRows / ΣTotalRows`，分母按 LOB 文件路径去重。默认低于 30% 时复用，达到 30% 时重写。分母为零时复用。排序和 schema 重写还要检查输入与输出的分区存储目录，跨目录时重写。预计输出数量只参与策略选择，继承使用实际行去向。文件字节数和存活比例都不能证明剩余正文的大小。[Mix 策略](../../../internal/datanode/compactor/mix_compactor.go) · [LOB 决策](../../../internal/compaction/lob_compaction.go)

## DataCoord、恢复与 CDC

- Flush 提交在同次元数据更新中处理 manifest、检查点、行数和累计统计。旧报告不能覆盖新检查点。同一提交中两个报告的已知值冲突时，保留已提交结果并返回错误。完整 compaction 安装实际输出的绝对统计。原地 compaction 保留未改字段，并沿用 schema 或 manifest 的并发校验。[Flush 提交](../../../internal/datacoord/services.go) · [覆盖范围与重放](../../../internal/datacoord/meta/field_logical_sizes.go) · [Compaction 提交](../../../internal/datacoord/meta/meta.go)
- 旧写入器追加未计量的行后，原字段总量失去完整覆盖。原地更新可从列组确认受影响的字段时，只使这些字段失效。Spark 提交预构建 manifest、外部刷新替换整个段，或旧排序任务采用可过滤行的新 manifest 时，清空无法证明仍然准确的字段大小。[外部回填](../../../internal/datacoord/services_commit_backfill.go) · [外部刷新](../../../internal/datacoord/externalrefresh/task.go) · [旧排序](../../../internal/datacoord/stats/task.go)
- 快照通过 `SegmentDescription`、Avro `ManifestEntry`、`CopySegmentSource`、`CopySegmentResult` 传递可选统计。Avro 用 8 B 大端无符号数保存载荷和有效位。只有 V3 insert manifest、行集和字段身份匹配时，目标段才继承可验证的条目。旧快照缺少计量时，复制结果为未知。[快照格式](../../../internal/snapshotio/snapshot.go) · [复制提交](../../../internal/datacoord/copysegment/task.go)
- CDC 在每个集群写出本地段时派生统计。两端可以使用不同的段 ID 和切分方式。在相同的逻辑行集上比较某个字段前，要先确认两端都完整覆盖该字段。[集群复制](../../agent_guides/streaming-system/replication/replicate.md)

## 验收样例

| 主键 | `name` | `body` 载荷 | Compaction 结果 |
| --- | --- | ---: | --- |
| 1 | `猫` | 1 B | 段 A |
| 2 | 空串 | 65,536 B | 段 B |
| 3 | NULL | 90,000 B | 按删除记录滤除 |
| 4 | `ab` | 7 B | 按 TTL 滤除 |

这四行通过 Go V3 flush 写入一个段后，`name` 有 **5 B 载荷和 4 个有效位**，`body` 有 **155,544 B 载荷**。把相同数据按一行和三行写到两个段，每个字段的两项计数分别求和，结果与单段完全一致。Compaction 滤除第 3、4 行后，段 A 的 `name` 和 `body` 载荷分别为 **3 B、1 B**，段 B 的两列分别为 **显式 0 B、65,536 B**。[跨段 flush 用例](../../../internal/flushcommon/syncmgr/pack_writer_v3_test.go) · [删除与 TTL 用例](../../../internal/datanode/compactor/clustering_compactor_storage_v3_test.go)

另一个用例把相同三行写成一个 gzip 列组、一个批次，以及两个 Zstandard 列组、两个批次。用例从实际 Parquet 文件页脚核对压缩编码。两次输出的字符串主键都是 5 B，可空字符串都是 4,096 B 载荷和 3 个有效位。[压缩和列组用例](../../../internal/storage/record_writer_test.go)

同目录 Mix 的另一组输入有两条外置正文，长度为 65,536 B 和 90,000 B，源列总量为 **155,536 B**。完整保留时，输出继承了全部 155,536 B。删除较长正文后，输出省略了 `body`，仍发布已知主键等字段。九个覆盖 Mix、归并、排序和 schema 重写的复用用例均在 LOB 正文不可读时通过。[复用测试](../../../internal/datanode/compactor/clustering_compactor_storage_v3_test.go) · [V3 写入器测试](../../../internal/storage/record_writer_test.go)

Go 回归测试使用 `-tags dynamic,test -gcflags="all=-N -l" -count=1`。V3 flush 在写入、关闭、统计文件、删除日志、统计完成后及 manifest 提交处分别注入过故障，重试结果均只计入成功的 3 行。WAL L1 检查点确认超时后重发同一份统计，从该检查点恢复并封段，最终两行的主键为 16 B，向量为 64 B。普通导入和备份逐行导入各经过三个物理片段和中间归并，最终 manifest 均只计入三行。[检查点用例](../../../internal/streamingnode/server/wal/vchannel/segment/pack_writer_v3_test.go) · [失败重试用例](../../../internal/flushcommon/syncmgr/task_v3_retry_test.go) · [导入用例](../../../internal/datanode/importv3/import_plan_executor_test.go)

- **DataCoord 提交。** 两个 flush RPC 用例使用独立的本地 etcd 和 MinIO。对仍在写入的段，用旧检查点封段会返回可重试错误。DataView 发布故障恢复后，段统计与新行数一同生效。同一 manifest 重放时，已知的零与非零值冲突会返回错误。空 TEXT 段重放封存请求后仍为 `Dropped`。[Flush RPC 用例](../../../internal/datacoord/services_test.go)
- **旧版本往返。** 本地另用官方基线的 Go protobuf 绑定读取新版 `SegmentInfo` 并重新序列化。旧绑定保留了自己无法识别的两个字段。用新版 DataCoord 重新加载目录时，同一修订恢复了显式零和非零计数。旧绑定只改 manifest 修订号、保持行数相同后，新版清除了旧计量，同时保留原有文件大小统计。[目录恢复用例](../../../internal/datacoord/meta/field_logical_sizes_test.go)
- **快照与列替换。** 快照导出只携带可核实的计量。引用式与自包含 V6 快照落盘后均保留了显式零和非零计数。直接发布和分阶段发布的复制任务遇到同一修订的统计冲突时，均保留已安装的目标。定向用例也验证了 Spark 替换索引字段、外部刷新和旧排序后的统计失效。[快照导出用例](../../../internal/datacoord/handler_test.go) · [快照落盘用例](../../../internal/snapshotio/storage/snapshot_data_test.go) · [复制重放用例](../../../internal/datacoord/copysegment/task_test.go)

`internal/storage` 分批完成回归。使用本地文件系统、MinIO 和 Azurite 的一轮通过了 520 个顶层测试和 2,794 个子测试。Azure 连接失败用例和两个 GCS 用例另行通过，GCS 使用本地 `fake-gcs-server`。两个既有用例跳过，分别是 512 MiB 大值写入和需要外部 TLS 凭据的测试。

## 本地开销实测

在 Apple M5 arm64 上使用本地文件系统、`GOMAXPROCS=2` 和上述 Go 编译参数。两侧使用同一版 `milvus_core`、各自版本的 `milvus-storage`、相同的输入和参数。按基线、实施版、实施版、基线的顺序运行，每个版本各取六个样本。下表给出中位耗时和最小至最大值。

| 路径和计时范围 | 官方 master | 加入字段计量后 |
| --- | ---: | ---: |
| Go V3 flush，4,096 行，从 `BulkPackWriterV3.Write` 到 manifest 提交 | 12.23 ms（11.91–12.68） | 12.42 ms（11.64–13.21） |
| V3 Mix，100 万行，从 `Compact` 进入到返回 | 337.81 ms（333.63–365.01） | 333.73 ms（328.42–344.69） |

Flush 每个样本写 15 次。输入含两个列组、128 维 float 向量和八分之一为 NULL 的字符串。Mix 每个样本处理一个输入段并生成一个输出段，主键为 Int64，向量为 64 维二进制，使用 `mergeSplit`，该样本没有删除行。两种操作的样本范围均有重叠，这组数据无法判定稳定的加速或减速。[Mix 基准](../../../internal/datanode/compactor/mix_compactor_bench_test.go)

- **LOB 正文 I/O。** 相同正文、内联阈值和策略下，新写入在两版中均读取 0 次、0 B。正常 `REWRITE_ALL` 均读取 1 次、20,376 B。两条路径的新增正文读取都是 0 次、0 B。[原生写入与重写用例](../../../internal/storagev2/packed/segment_writer_ffi_test.go)
- **内存计量循环。** 4,096 行，三列分别为 Int64 主键、可空字符串和 128 维 float 向量。字符串全有效时，原列组计量为 116 ns/批，加入字段计量后为 941 ns/批。八分之一字符串为 NULL 时，分别为 115 ns/批和 31.95 µs/批。两种输入均新增 912 B、13 次 Go 分配/批。此项只计内存中的列循环。[微基准](../../../internal/storage/field_logical_size_benchmark_test.go)
- **段元数据。** 实际序列化含 100 万行的 `SegmentInfo`，每列有 800 万 B 载荷和 100 万个有效位，字段 ID 从 100 起。连同本例的 manifest 标识，4、16、64、256 列分别增加 118、300、1,056、4,128 B。具体大小随字段 ID、计数及 manifest 路径长度变化。[元数据协议](../../../pkg/proto/data_coord.proto)

这些耗时和正文读取数来自本地写入器。远端对象存储的服务端请求数、在线集群吞吐及双集群 CDC 对账尚未实测。
