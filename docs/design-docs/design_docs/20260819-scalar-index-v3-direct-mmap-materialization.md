# Scalar Index V3 直接写入 mmap 的可行性讨论

- **创建时间：** 2026-08-19
- **状态：** 讨论稿，非最终方案
- **涉及模块：** Segcore、Scalar Index V3、Milvus Storage、AWS CRT S3 Client

## 一、结论先行

对于明文 Scalar Index V3 Entry，storage 层可以把远端数据直接读取到调用方提供的可写文件映射区域。这样可以去掉 Milvus 侧的下载中间 buffer，以及随后把 buffer 写入本地文件的 `pwrite` 或内存复制。

但是，“可以直接下载到 mmap”与“这块 mmap 可以直接作为最终索引结构”是两件不同的事：

1. **下载目标是否可以是 mmap：** CRT 能否把 Entry 的字节直接写进预先分配的 writable mmap？
2. **mmap 是否就是最终表示：** 索引能否直接查询这些字节，而不再做格式转换或复制？

目前的总体结论是：

- 几乎所有明文 Entry 都可以把 writable mmap 作为直接下载目标。
- Sort、Marisa 和 FMIndex 的主要数据可以直接查询下载后的 mmap。
- Tantivy 和 RTree 可以直接使用下载完成的 staging files，但必须等整个目录中的必需文件全部完成后才能打开。
- Bitmap 是主要例外：当前 V3 持久化的是普通 Roaring 序列化格式，加载时还要转换成 frozen Roaring mmap 格式。
- `valid_bitset`、`null_offset`、`non_exist_offsets` 等辅助数据通常仍然会变成最终 heap 结构。

因此，更准确的描述是：

> 明文 V3 下，绝大多数大 Entry 可以由 CRT 直接写入最终 mmap 或最终 staging file；但不能把所有索引都称为“完全零拷贝加载”。

## 二、本次讨论范围

本文先按以下边界讨论：

- 只考虑 Scalar Index V3 packed file。
- 只考虑明文 Entry。
- 只考虑原生 AWS CRT S3 路径，并假定后续默认开启 CRT。
- 对支持 mmap 的索引，假定 mmap load 已开启。
- 第一阶段继续同步读取 Footer、Entry Directory 和 `__meta__`。
- catalog inspection 异步化以及非 CRT 路径留到后续单独讨论。

加密 Entry 不在第一阶段范围内。加密场景通常需要 ciphertext source buffer 和独立的 plaintext target；除非后续把解密接口改成可以直接流式写入最终目标，否则不能直接复用明文的零中间 buffer 路径。

## 三、当前路径与目标路径

当前不少索引最终也是 mmap-backed，但加载过程通常仍然经过 Milvus 中间 buffer 或 `FileWriter`：

```text
当前路径

remote read
    → Milvus slice buffer
    → FileWriter / pwrite
    → local file
    → read-only mmap
```

明文 CRT 场景的目标路径是：

```text
目标路径

创建目标文件
    → ftruncate 到最终 mapping 大小
    → mmap(PROT_READ | PROT_WRITE, MAP_SHARED)
    → ReadAtAsyncInto(remote_offset,
                      entry_bytes,
                      mapping + target_offset)
    → 校验实际读取字节数
    → 扫描目标区域并计算 Entry CRC
    → EntryFinalize
    → 解除 writable mapping
    → 以只读方式 remap 或打开最终索引
```

`RemoteInputStream` 已经提供了调用方传入目标地址的异步读取接口：

- [`RemoteInputStream::ReadAtAsyncInto`](../../../internal/core/src/storage/RemoteInputStream.h#L41)
- [native async dispatch](../../../internal/core/src/storage/RemoteInputStream.cpp#L161)

目标 mapping、backing file 和相关 `EntryState` 必须一直存活到异步读取成功或失败，并且所有 callback 完全 drain。

对于需要对齐或 parser padding 的格式，可以在 mmap 前直接把新文件 `ftruncate` 到包含 padding 的最终大小。新扩展的文件区域由文件系统补零，因此不需要另外分配 padding buffer 再写一遍。

## 四、各索引可行性汇总

| 索引类型 | CRT 直接写入 mmap | 下载字节能否直接作为最终表示 | 说明 |
|---|---:|---:|---|
| 数值/布尔 `ScalarIndexSort` | 可以 | 可以 | `index_data` 和持久化的 `idx_to_offsets` 都是平坦、可直接寻址的布局；`valid_bitset` 仍是 heap bitmap。 |
| `StringIndexSort` | 可以 | 可以 | 序列化后的字符串和 posting 布局可由 `StringIndexSortMmapImpl` 原地解析；row offset 也可单独 mmap。 |
| `StringIndexMarisa` | 可以 | 可以 | Trie、string IDs 和持久化 CSR 都可以直接做 file-backed view；两个 CSR Entry 需要提前确定不重叠的目标 offset。 |
| `FMIndex` | 可以 | 可以 | flat blob 直接交给 `FMIndex::LoadView`；null bitmap 和部分派生结构仍在 heap。 |
| Tantivy inverted index | 可以 | 可以，作为最终文件 | 每个 Tantivy 文件单独作为目标；只有全部必需文件 READY 后才能打开 Tantivy reader。 |
| TextMatch / JsonFlat | 可以 | 同 Tantivy | 两者复用 Tantivy 的持久化与加载实现。 |
| Ngram inverted index | 可以 | 同 Tantivy | Tantivy 文件直接落位；`avg_row_size` 是小型标量元数据。 |
| RTree | 可以 | 可以，作为最终文件 | 全部文件完成后再创建并加载 `RTreeIndexWrapper`。 |
| Bitmap，Roaring 模式 | 可以写 staging mmap | **不可以** | 普通 Roaring 序列化格式还要转换为对齐的 frozen Roaring 格式。 |
| Bitmap，bitset 模式 | 可以写 source mmap | **不可以** | 持久化表示最终要反序列化成 heap bitset。 |
| Hybrid | 取决于内部索引 | 取决于内部索引 | Hybrid 只记录内部索引类型，实际加载委托给内部索引。 |
| JSON / Array / Nested wrapper | 取决于基础索引 | 取决于基础索引 | wrapper 自己的 offset vector 和 existence bitmap 仍是 heap 结构。 |

## 五、逐类分析

### 5.1 ScalarIndexSort

V3 持久化三个 Entry：

- `index_data`
- `idx_to_offsets`
- `valid_bitset`

`index_data` 是 `IndexStructure<T>` 数组。在 mmap 模式下，`setup_data_pointers()` 直接把查询指针指向这块 mapping。`idx_to_offsets` 也可以直接引用自己的 mapping。

相关代码：

- [V3 Entry 布局](../../../internal/core/src/index/ScalarIndexSort.cpp#L696)
- [mmap data pointers](../../../internal/core/src/index/ScalarIndexSort.h#L247)
- [持久化 offset mapping](../../../internal/core/src/index/ScalarIndexSort.cpp#L848)

`valid_bitset` 当前会复制进 `TargetBitmap`。这个复制并不是下载过程必需的：只要先校验 Entry 大小和内部布局，CRT 可以直接写入已经分配好的 bitmap storage。但该 allocation 属于最终内存，仍然要计入 Load Memory Planner。

旧 V3 文件如果没有持久化 `idx_to_offsets` 和 `valid_bitset`，加载后仍然需要重建它们。

### 5.2 StringIndexSort

`index_data` Entry 包含 string table、string offsets 和 posting lists。`StringIndexSortMmapImpl::MmapAndParse()` 只是在 mapping 中建立若干 view，因此持久化字节本身可以作为最终表示。

相关代码：

- [V3 Entries](../../../internal/core/src/index/StringIndexSort.cpp#L579)
- [当前 mmap materialization](../../../internal/core/src/index/StringIndexSort.cpp#L671)
- [原地解析 mmap](../../../internal/core/src/index/StringIndexSort.cpp#L1680)

和数值 Sort 一样，压缩后的 validity bitmap 会变成 heap `TargetBitmap`。旧文件还可能需要重建 row-to-sorted-offset mapping。

### 5.3 StringIndexMarisa

V3 持久化：

- `MARISA_TRIE_INDEX`
- `MARISA_STR_IDS`
- `MARISA_CSR_INDEX`
- `MARISA_CSR_OFFSETS`

Trie 文件通过 `trie_.mmap()` 打开；string IDs 直接引用 mapping；两个 CSR Entry 当前会顺序写入同一个文件，然后由两个指针分别引用。

相关代码：

- [V3 Entries](../../../internal/core/src/index/StringIndexMarisa.cpp#L847)
- [Trie 和 string-ID mapping](../../../internal/core/src/index/StringIndexMarisa.cpp#L895)
- [CSR 文件布局](../../../internal/core/src/index/StringIndexMarisa.cpp#L1021)

因此，Plan 阶段必须在两个异步读取启动前确定 CSR 的目标位置：

```text
CSR file

[0, csr_index_bytes)
    ← MARISA_CSR_INDEX

[csr_index_bytes, csr_index_bytes + csr_offsets_bytes)
    ← MARISA_CSR_OFFSETS
```

旧文件如果没有持久化 CSR Entry，仍然需要执行 `fill_offsets()`，不能做到完全 direct-to-final。

### 5.4 FMIndex

大的 `FMINDEX_BLOB_FILE_NAME` Entry 是 flat blob。mmap 路径会调用 `fmindex::FMIndex::LoadView()`，后续查询直接引用 blob 中的数据。

相关代码：

- [V3 blob 与 null bitmap](../../../internal/core/src/index/FMIndex.cpp#L428)
- [mmap 与 `LoadView`](../../../internal/core/src/index/FMIndex.cpp#L565)

CRT 可以直接把 blob 写进最终的 padded mapping。不过，`LoadView()` 仍可能在 heap 中建立少量 rank/search 派生结构；压缩后的 null bitmap 也会被解包成最终 `TargetBitmap`。

### 5.5 Tantivy、TextMatch、JsonFlat 和 Ngram

Tantivy V3 把 Tantivy directory 中的每个文件保存为独立 Entry。这些文件不需要 Milvus 侧进行格式转换。Materializer 可以预先创建并 mmap 每个目标文件，然后分别提交对应 Entry range 的 CRT read。

只有全部必需 Entry 都通过 CRC 校验后，才能创建 `TantivyIndexWrapper` 并打开 staging directory。

相关代码：

- [Tantivy V3 Entries 与加载](../../../internal/core/src/index/InvertedIndexTantivy.cpp#L876)
- [`JsonFlatIndex` 继承关系](../../../internal/core/src/index/JsonFlatIndex.h#L728)
- [Ngram 辅助 Entry](../../../internal/core/src/index/NgramInvertedIndex.cpp#L219)

`index_null_offset`、JSON `non_exist_offsets`、existence bitmap 和 Ngram metadata 是小型最终 heap 结构，而不是 Tantivy mmap data。

### 5.6 RTree

RTree 同样把每个本地索引文件保存为一个 V3 Entry。这些文件可以直接作为 CRT target。只有全部文件完成并通过校验后，才能打开 `RTreeIndexWrapper`。

相关代码：

- [RTree V3 Entries](../../../internal/core/src/index/RTreeIndex.cpp#L641)
- [staging files 与 wrapper open](../../../internal/core/src/index/RTreeIndex.cpp#L693)

null-offset vector 仍然属于最终 heap memory。

### 5.7 BitmapIndex：主要例外

V3 的 `BITMAP_INDEX_DATA` Entry 保存 key 和普通 Roaring bitmap 序列化数据。当前 mmap loader 会：

1. 暂存原始 Entry。
2. 逐个解析普通 Roaring bitmap。
3. 计算 frozen size。
4. 把对齐后的 frozen representation 写入第二个文件。
5. 在最终 mapping 上构造 `Roaring::frozenView`。

相关代码：

- [V3 序列化 Entry](../../../internal/core/src/index/BitmapIndex.cpp#L1421)
- [normal-to-frozen 转换](../../../internal/core/src/index/BitmapIndex.cpp#L568)
- [当前 staging 路径](../../../internal/core/src/index/BitmapIndex.cpp#L1448)

直接 CRT-to-mmap 可以消除原始 Entry 的下载 buffer，但无法消除：

- raw staging mapping
- frozen output mapping
- 每个 bitmap 的对齐 conversion buffer
- conversion CPU 和本地写入

以后可以考虑修改 V3 格式，直接持久化 frozen bitmap bytes 及其 key/offset directory。这样 Bitmap 才能 direct-to-final，但会引入格式迁移和兼容性问题。

### 5.8 Hybrid 和 JSON wrappers

Hybrid 会选择内部 Sort、Bitmap、Marisa 或 Tantivy 实现，然后委托实际 Entry load。因此它能否 direct-to-final 完全取决于所选内部索引。

相关代码：

- [Hybrid delegation](../../../internal/core/src/index/HybridScalarIndex.cpp#L440)
- [JSON wrapper 辅助数据](../../../internal/core/src/index/JsonScalarIndexWrapper.h#L161)
- [JSON Hybrid 辅助数据](../../../internal/core/src/index/JsonHybridScalarIndex.h#L154)

JSON wrapper 还会加载 `non_exist_offsets`，并在 heap 中构造 `exists_bitset`。

## 六、建议的 EntryTarget 模型

公共 materialization 层至少要支持三类目标：

```cpp
using EntryTarget = std::variant<
    MemoryEntryTarget,
    MmapFileRegionTarget,
    StagingMmapTarget>;
```

含义分别是：

- `MemoryEntryTarget`：最终目标就是预先分配的 heap memory。
- `MmapFileRegionTarget`：Entry 可直接写入最终文件或最终文件中的指定区域。
- `StagingMmapTarget`：Entry 可以直接下载到 mmap，但后续还要执行索引特有的格式转换，例如 Bitmap。

`MmapFileRegionTarget` 至少需要描述：

```cpp
struct MmapFileRegionTarget {
    std::string path;
    size_t file_size;
    size_t target_offset;
    size_t target_bytes;

    // fd 和 writable mapping 必须存活到 EntryFinalize/drain 完成。
};
```

这既能表示 Tantivy/RTree 的 one-entry-per-file，也能表示 Marisa CSR 这种多个 Entry 写入同一个文件不同区域的布局。

## 七、CRT 请求粒度与 Milvus Slice

CRT 会在一次 range request 内部继续拆分传输，但这些内部 chunk、buffer 和完成事件对 Milvus 不可见，不能参与 Milvus 的全局 budget admission。因此，CRT 拆块不能替代 Milvus Slice。

Milvus 仍要在 Plan 阶段把明文 Entry 拆成 Slice。每个 Slice 调用一次：

```text
ReadAtAsyncInto(
    entry.remote_offset + slice.entry_offset,
    slice.remote_bytes,
    target_base + slice.target_offset)
```

Slice 直接写入预先分配的非重叠目标 region，不再创建 Milvus 下载 buffer。Slice 在这里有三个职责：

1. 作为 `AcquireAsync(slice.admission_bytes)` 的准入单位。
2. 在不同 Entry 之间 round-robin，避免先完整读完大 Entry A 才开始 B。
3. 提供可取消、可 drain、可限制 `max_inflight_slices` 的任务边界。

准入仍有以下限制：

- `downloadMemoryUsageWindow` 属于单个 CRT meta request，不是所有 Entry 共享的进程级全局 quota。
- 即使 CRT client 暴露 `memoryLimitBytes` 一类 client 级配置，Scalar Index load 也拿不到每次请求实时占用多少、何时释放的 lease 语义；它无法接入 Milvus Load Memory Planner 做逐操作的精确准入。
- Tantivy/RTree 可能包含很多文件，不能一次无上限地提交全部 Slice read。
- 必须保留内部 `max_inflight_slices`，用于限制 request、connection 和 callback 压力。
- `slice.admission_bytes` 默认可以按 range bytes 保守计费，但它不是 CRT 真实 allocation；`max_inflight_slices` 也只限制操作数。

这里必须接受一个限制：Milvus Slice 可以约束自己提交的 range 大小和并发数，却仍看不到 CRT 内部 chunk、buffer 和请求级峰值。因此 Slice admission 是必要的保守控制，但不能证明或保证 CRT 临时内存被限制在某个精确字节 budget 内。

## 八、CRC 与两级 Finalize

每个明文 Slice 完成后，可以扫描它写入的目标 region 并计算 plaintext CRC：

```text
Slice ReadAtAsyncInto 完成
    → 校验返回字节数
    → CRC32C(target_address + slice.target_offset, slice.target_bytes)
    → 保存 slice_crcs[seq]
    → 最后一个 Slice 按 seq 执行 CRC combine
    → 与 expected Entry CRC 比较
    → FinishTarget
    → 标记 Entry READY
```

这会对每个 Slice 的目标 region 多做一次顺序内存扫描，但不分配 Milvus download buffer。Slice 可以乱序完成，EntryFinalize 必须按 `seq` combine CRC。

如果多个 Entry 共用一个目标文件，例如 Marisa CSR，那么必须等该文件所有 Entry 都完成并通过校验后，才能执行 target finish。

IndexFinalize 仍然必须与 EntryFinalize 分开：

```text
全部异步读取 drain
    → 如果失败，重新抛出 first error
    → 确认全部必需 Entry READY
    → 关闭 writable mappings
    → 以只读方式 remap 或打开 staging directory
    → 构造最终 Index
    → 成功后才发布
```

## 九、内存账本

对于直接写入最终目标的 Entry，Milvus 可以不再分配完整 Entry/Slice 下载中间 buffer。这个结论只描述 Milvus 自己删除掉的 allocation，**不能写成“download transient memory 为 0”**：CRT 内部仍会使用临时内存，而且当前 Milvus load budget 无法精确计量和控制它。

需要区分以下账本：

| 内存类型 | 示例 | 负责计账的位置 |
|---|---|---|
| 最终 heap memory | validity/null bitmap、offset vector、FM 派生结构 | Load Memory Planner |
| 最终 mmap/file data | Sort data、FM blob、Tantivy/RTree files | mmap/disk load planning 与 OS page-cache policy |
| 格式转换 transient | Bitmap frozen conversion buffer | Milvus transient/finalization budget |
| CRT 内部内存 | range request buffer、request bookkeeping | 当前对 Milvus Planner 不透明；client/request 配置不提供逐 load lease |
| Kernel memory | dirty mmap pages、page cache | OS/cgroup memory accounting |

因此，本方案不能依靠把 Milvus download transient budget 配成 0 来保证内存安全。direct path 虽然不再分配 Milvus slice buffer，仍应按 Slice range bytes 申请一个保守的 admission lease，用它约束已提交的 range 总量；但这个 lease 是代理 charge，不是 CRT 实际 allocation。CRT 临时内存仍无法被当前 budget 精确计量；在补齐可观测、可计量、可准入的接口之前，本方案没有 CRT 内存 hard bound。

当前格式下，Bitmap Finalize 仍然需要非零的 transformation allowance。

## 十、取消、失败与清理

Direct-to-mmap 不改变失败处理的基本语义：

1. 保存 first error。
2. 停止准入新的 Entry read。
3. 请求取消已经发出的 CRT operation。
4. drain 所有已发出的 operation 和 callback。
5. mapping、fd 和 `IndexLoadState` 必须存活到 drain 完成。
6. 失败时解除 writable mapping，并删除 staging files/directory。
7. 不发布任何半成品 Index。

在同一进程内，已完成的 mmap 写入对随后建立的 mapping 可见，因此仅为了读可见性不需要 `msync/fsync`。只有本地 cache 存在明确的 crash durability 要求时，才需要把 `msync/fsync` 放进加载路径。

即使不要求 durability，也应该在索引库打开最终文件前关闭 writable mapping，避免 final reader 与写入阶段生命周期重叠。

## 十一、需要一起讨论的问题

1. 是否要调整 Bitmap V3 持久化格式，直接保存 frozen representation？
2. `valid_bitset`、null/non-exist offsets 等小 Entry，是直接写入最终 heap，还是继续走简单 buffered path？
3. 在 CRT 临时内存不可精确计量和准入的前提下，是接受 Slice range bytes 作为保守 charge，还是要求 milvus-storage 后续暴露可观测、可 acquire/release 的真实字节级接口？
4. dirty mmap 和 page cache 是否应该纳入现有 Load Memory Planner，还是单独统计？
5. 本地 index cache 是否有 crash durability 要求？如果没有，`msync/fsync` 可以不进入关键路径。
6. 旧 V3 文件缺少 Sort/Marisa 辅助 Entry 的兼容路径需要保留多久？
7. 第一阶段是否只选择一个最简单的 direct-to-final index 验证公共 pipeline，再扩展到多文件索引？

## 十二、建议的第一阶段实现边界

1. catalog inspection 保持同步。
2. 只支持明文 CRT read。
3. 新增带显式生命周期管理的 writable mmap-region target。
4. 先接 `ScalarIndexSort` 或 FMIndex，它们的大 Entry 已经是最终 mmap 表示。
5. 再接 Tantivy/RTree 多文件 staging。
6. Bitmap 先保留现有 frozen conversion，只把下载部分改为 direct-to-staging。
7. encryption、异步 catalog inspection 和 non-CRT fallback 后续单独处理。

## 十三、截至 2026-08-21 的落地状态

当前分支已经按讨论后的收敛方案完成第一阶段实现：

- Field Data 与 Scalar Index 共用一个物理 `PriorityThreadPoolExecutor`；HIGH/LOW 只是同一线程池中的逻辑优先级。旧 `AsyncLoadExecutor` 已删除，没有新增 materialize/disk 专用 executor。
- Footer、V3 Entry Directory 和 `__meta__` 仍同步检查，并生成只读 `IndexEntryCatalog`。
- 公共 pipeline 已拆成 `PlanLoad()`、`MaterializeIndexAsync()`、EntryFinalize 和 `FinalizeLoad()`。
- materializer 会跨 Entry round-robin 准入 Slice，限制 `max_inflight_slices`，直接调用 native caller-owned async read 写入最终 heap/mmap region，乱序完成后按 Slice 顺序 combine CRC。
- 在创建 staging file 或发出 read 前，公共校验会拒绝重复 Entry、Slice 空洞/越界，以及不同 Entry 的 heap/mmap 目标区间重叠。
- first error 会停止新准入并触发 sibling cancellation；全部已发出的 read drain 后才释放 lease 和目标生命周期，失败时删除未提交的 staging file/directory。
- 已接入 direct path：`ScalarIndexSort`、`StringIndexSort`、`StringIndexMarisa`、`FMIndex`、Tantivy inverted index（包括当前不增加额外 V3 Entry 的 TextMatch/JsonFlat）以及 RTree。
- 第一阶段明确保留同步 fallback：Bitmap raw-to-frozen、带额外 `avg_row_size` Entry 的 Ngram、带 `non_exist_offsets` Entry 的 `JsonScalarIndexWrapper`、加密 Entry，以及不支持 native caller-owned read 的 storage 路径。

这里的 Slice admission 仍只是保守代理值，不是 CRT 临时内存的 hard bound；实现没有把 direct path 描述成 transient memory 为 0。

一句话总结：

> 索引类型负责决定每个 Entry 的最终目标和必要的格式转换；公共 CRT materializer 只负责有界准入、直接落位、完整校验、取消 drain 和失败清理。
