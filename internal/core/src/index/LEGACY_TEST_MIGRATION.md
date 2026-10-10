# Index C++ UT 迁移现状

本清单按测试目的和 edge case，而不是测试名或输入数据是否相同，划分原有 C++ UT 与当前 index contract 测试。范围包括原有 index/indexbuilder 用例、从 exec/common/unittest 迁出的纯索引用例、storage 和跨层 consumer 用例，以及当前 `index_tests` 的合同覆盖。一个旧文件可以同时出现在第一类和第三类；同一旧用例兼有合同结果和退役内存表示断言时，也按断言目的分别归类。表中“旧用例已删除”指原 TEST/TYPED_TEST 宏已从旧 target 移除；“文件保留”只说明其中还有别的用例。

`internal/core/unittest/CMakeLists.txt` 当前给 `index_tests` 列出 40 个 index/indexbuilder UT 源、6 个 support 源和 `init_gtest.cpp`。原来纳入迁移判定的 22 个旧 index/indexbuilder 测试文件中，20 个文件已删除，只剩 `SkipIndexTest.cpp` 和 `skipindex_stats/SkipIndexStatsTest.cpp`；它们测试 skip-index statistics，仍由 `all_tests` 收集。旧测试迁移后的其他模块测试也由 `all_tests` 的源文件 glob 收集。本轮已重新构建并完整运行 `index_tests`：13,124 个注册用例中 13,123 通过、1 个因当前 DiskANN backend 不支持附加 scalar side input 而跳过。

## 一、已经迁移到 index_tests

本类包括新 suite 已覆盖其行为目的和边界的旧用例。表中的原用例均已删除；如果原文件仍在，其剩余用例列在第三类。新 suite 的具体参数、数据集和方法边界见 [contract 测试说明](contracts/TESTING.md)。

| 原文件与已迁移的旧用例 / edge | 当前 index_tests 覆盖 | 旧用例已删除；旧文件状态 |
|---|---|---|
| `indexbuilder/ScalarIndexCreatorTest.cpp`：`FMIndexParamsRejectInvalidValuesWithInputCode`、`FMIndexParamsMatchGoLeadingPlusValidation`；错误 JSON 类型、整数边界和前导 `+`；typed `Constructor`/`Codec` 的 bool、int8/16/32/64、float、double、string 构建和持久化目的 | `FmIndexBuilderTest` 的参数拒绝与边界用例；`ArtifactBuilderTest.ArtifactAndReaderOutliveBorrowedInput` 的 scalar lifecycle 矩阵覆盖八种类型的真实 registry/build/serialize/load，Sorted 和 Marisa artifact suites 覆盖相应持久化格式 | 是；文件已删除 |
| `index/BoolIndexTest.cpp`：bool Count/In/NotIn、null、build/load/codec | Bitmap reader/artifact、`ScalarPredicateReaderTest`、`NullReaderTest` | 是；文件已删除 |
| `index/ScalarIndexTest.cpp`：typed scalar Count/In/NotIn/Range/Reverse/Codec、V3 错误分类、resource inspection、旧 hybrid load 行为 | scalar/value/null/pattern suites、`LoadResourceTest`、`PackedIndexLoadTest`、`HybridIndexBuilderTest` | 是；文件已删除 |
| `index/StringIndexTest.cpp`：Marisa build/query/null/pattern/codec 与 heap/mmap native direct entry read | Marisa reader/artifact、shared predicate/value/pattern/null suites、`ScalarIndexV3AsyncTest.NativeDirectEntryReadsByFamily` | 是；文件已删除 |
| `index/StringIndexSortTest.cpp`：Sorted string build/query/reverse lookup/pattern/null/serialization，packed direct read、validity allocation，以及越界 posting row id | Sorted reader/artifact、shared predicate/value/pattern/null suites、`ScalarIndexV3AsyncTest`、`SortedIndexArtifactTest.StringPostingRowBeyondCountIsRejected` | 是；文件已删除 |
| `index/ScalarIndexSortTest.cpp`：Sorted scalar In/Range、heap/mmap direct read、mmap validity 单次计量 | Sorted/predicate suites、`ScalarIndexV3AsyncTest`、`SortedIndexReaderTest.MmapAccountsValidityWordOnce` | 是；文件已删除 |
| `index/BitmapIndexTest.cpp`：V1–V6 predicate/null/pattern 矩阵、异步 planned read、65-row validity allocation、Roaring scratch、五种 CRC 有效的损坏 payload | Bitmap reader/artifact、shared predicate/null/pattern suites、`ScalarIndexV3AsyncTest.BitmapUsesBufferedPlannedReads`、`ScalarIndexV3ResourceTest.BitmapFrozenScratchIsBoundedForRunPayloads`、`BitmapIndexArtifactTest.MalformedPostingsWithValidPackedCrcAreRejected` | 是；文件已删除 |
| `index/HybridScalarIndexTest.cpp`：Hybrid 基础 predicate/null、child selector 和资源估算；Tantivy 65-row validity | Hybrid builder、shared reader suites、`ScalarIndexV3ResourceTest.TantivyValidityCrossesWordBoundaryOnce` | 是；文件已删除 |
| `index/InvertedIndexTest.cpp`：普通查询、pattern/escaping/NUL、heap/mmap direct read、切片 null-offset sidecar | Inverted/pattern suites、`ScalarIndexV3AsyncTest` 的 Inverted profile、`LegacyIndexLoadTest.InvertedSlicedNullOffsetsPreserveRows` | 是；文件已删除 |
| `index/NgramInvertedIndexTest.cpp`：Ngram 基础/JSON/UTF-8/escaping/candidate 语义与异步 direct read | `NgramReaderTest`、`NgramIndexReaderTest`、`ScalarIndexV3AsyncTest` 的 Ngram profile | 是；文件已删除 |
| `index/TextMatchIndexTest.cpp`：Match/Phrase/Fuzzy、writer budget 常量、切片 null-offset sidecar | `TextMatchReaderTest`、`TextIndexArtifactTest.WriterBudgetConstantsMatchBuildAndGrowingModes`、`LegacyIndexLoadTest.TextSlicedNullOffsetsPreserveAllNullRows` | 是；文件已删除 |
| `index/JsonIndexTest.cpp`：只有 non-existent 或只有 null offset 的 JSON 切片 sidecar | `LegacyIndexLoadTest.JsonProjectedSlicedOffsetsRemainDistinct` | 是；文件已删除 |
| `index/JsonPathIndexTest.cpp`：JSON cast/path/Exists/NULL/predicate、projected Hybrid 高低基数与 invalid row 排除、旧 load/factory 路径 | `JsonIndexReaderTest`、`JsonProjectedHybridIndexBuilderTest`、packed/legacy load suites | 是；文件已删除 |
| `index/JsonFlatIndexTest.cpp`：`ExecutorReusesMaterializedFieldValidity` 的 field-null、resolved-null 和 string NotIn 结果 | `JsonIndexReaderTest` 的对应真实 reader 用例 | 是；文件已删除 |
| `index/FMIndexTest.cpp`：普通 Match cost guard、有效空字符串 zero-token、library load view/heap/doc locator、重复 occurrence、随机字节 oracle、LIKE 候选完整性、heap/mmap direct read 和 null bitmap 尾部位 | `PatternMatchReaderTest`、`FmIndexLibraryTest`、`FmIndexReaderTest`、`ScalarIndexV3AsyncTest` 的 FM 用例 | 是；文件已删除 |
| `index/BitmapIndexArrayTest.cpp`：ArrayRows 多 key In/NotIn、NULL parent 在 valid element 前、int8/int16 stride、nested element 查询/持久化/资源 | `ScalarPredicateReaderTest.ArrayRowsMultiKeyMembershipAndNullMask`、`MaterializerArrayTest` 的 NULL 顺序和窄整数用例，以及 nested reader/artifact suites | 是；文件已删除 |
| `index/RTreeIndexWrapperTest.cpp`：基本 build/load/query、invalid WKB、archive open/write/flush 与远端 read 错误 | `RTreeIndexArtifactTest` 的 roundtrip/错误注入、`RTreeIndexReaderTest` 的 MBR candidate 用例 | 是；文件已删除 |
| `index/RTreeIndexTest.cpp`：空间 candidate、null 与空 payload、切片 sidecar、direct read、连续 append 和并发 pinned query；`Build_Upload_Load`、`Load_WithFileNamesOnly`、`Build_WithInvalidWKB_Upload_Load`、`Build_VariousGeometries` 的纯 build/load 目的 | `RTreeIndexReaderTest`、`RTreeIndexArtifactTest`、`LegacyIndexLoadTest.RTreeSlicedNullOffsetsPreserveCandidates`、`ScalarIndexV3AsyncTest`、`GrowingIndexContractTest` | 是；文件已删除 |
| `index/UtilsTest.cpp`：六个 `SetBitset*` edge（连续 block、内部 hole、乱序/重复、越界 growing offset、逐 bit oracle、空/null 输入） | `IndexUtilsTest` 六个对应用例 | 是；文件已删除 |
| `exec/expression/ExprArithMiscTest.cpp`：`BitmapIndexTest.PatternMatchTest` 的纯 Bitmap prefix/inner/postfix 查询 | `PatternMatchReaderTest` 的真实 reader 用例 | 是；文件保留 |
| `common/BitmapTest.cpp`：`Bitmap.Naive` 的 float Sorted `< 0` 与 `(-1, 1]` 区间 | `ScalarPredicateReaderTest.MatchesExpectedOffsets` 的 float oracle/端点/负零用例 | 是；文件已删除 |
| `unittest/test_indexing.cpp`：`Indexing.Naive` IVFPQ interim、`Indexing.Iterator` IVFFLAT_CC iterator、`IndexTest.BuildAndQuery/Mmap/GetVector` family 矩阵、两种 sparse 空行、零物理 embedding list，以及 DiskANN 参数/float16/bfloat16/尾部空 parent list | `VectorFamilyTest`、`VectorFamilyEdgeTest`、`VectorReaderContractTest.EmptyValidEmbeddingListsHaveLogicalIdsWithoutVectors`、`VectorDiskFamilyTest` | 是；文件已删除 |

Vector family 矩阵分别覆盖 FAISS_IDMAP、FAISS_IVFPQ、FAISS_IVFFLAT、FAISS_IVFSQ8、FAISS_BIN_IVFFLAT、FAISS_BIN_IDMAP、SPARSE_INVERTED_INDEX、SPARSE_WAND、HNSW；DISKANN 的 float/float16/bfloat16 用例仅在 `BUILD_DISK_ANN` 开启时注册。Mmap 保留 native capability gate 和适用的平台限制；原 `GetVector_EmptySparseVector` 中没有断言的非 sparse 参数实例已删除。

## 二、需要进入 index_tests，但目前尚未覆盖

当前没有已识别且仍待迁移的旧测试行为或 edge。此前新增的非空 `InputSpec.side_inputs` 合同缺口已由真实 HNSW builder 的 scalar side input build/load 用例覆盖；DiskANN 对应场景保留能力判断，当前 backend 不支持附加 scalar 时跳过。第三类的 consumer、storage 或退役 API 用例不属于本类。

## 三、不需要迁移到 index_tests

### 归属各模块 `all_tests` 的测试

这些用例检查 build 编排、storage、segment、expression、query、skip-index statistics 等跨层行为，超出 index reader/builder 合同。本轮按实际职责重写或移动了第 1–14、16–18 项；第 15 项保留在 skip-index 源码旁，因为文件名对应当前受测类型；第 19 项的四个 storage 文件也已位于正确模块。除第 15、16、19 项注明的保留文件外，表中旧文件均已删除。新 `*Test.cpp` 通过 `all_tests` 源文件 glob 收集。

| 编号 | 原文件 / 用例 | 归属与现状 |
|---:|---|---|
| 1 | `indexbuilder/ScalarIndexCreatorTest.cpp`：`CreateTextMatchIndexForTextField`、`EmptyNestedIndexBuildSkipsPersistence`、`EmptyRawIndexBuildSkipsPersistence` | 已重写为 `indexbuilder/BuildSessionTest.cpp`，由 `all_tests` 的源文件 glob 收集，平台过滤也改为新文件名。TEXT 用真实 `BuildIndexInfo`→`IndexBuildCapiAdapter`→V1 binlog→`BuildSession`→V3 发布→Text loader 路径，检查 Text family/namespace、schema analyzer 和 extra info，并通过 standard/whitespace 的不同命中检验 analyzer 交付。空 nested ARRAY 保留 Sorted/Bitmap × Int32/String × engine V1/V3、8 个有效空 parent 行与真实 binlog/materializer/builder；空 scalar 保留 Sorted/Bitmap，并覆盖 V1/V3。两类空结果均检查重复 `Publish` 的零统计、wire stats，以及构建/发布前后远端目录没有新增条目。直接 raw Build、BinarySet Serialize 入口已退役，不再重建；旧文件已删除。 |
| 2 | `index/ScalarIndexTest.cpp`：取消优先级、finalization 不提交、global switch、local-file worker | 移到 `segcore/storagev2translator/PackedScalarLoadRoutingTest.cpp`，用当前 packed load 路径覆盖取消、切换和单 worker；旧文件已删除。 |
| 3 | `index/StringIndexTest.cpp`：本地 chunk 目录重建、`BaseIndexCodec` | 移到 `storage/ScalarArtifactTransportTest.cpp`，覆盖 Marisa 发布/mmap 目录和 packed codec 查询；旧文件已删除。 |
| 4 | `index/HybridScalarIndexTest.cpp`：resource/admission/translator 与缺失 binlog | 资源 admission 移到 `storage/ScalarLoadAdmissionTest.cpp`；legacy/nested metadata 路由移到 `segcore/storagev1translator/ScalarIndexTranslatorTest.cpp`；缺失行的 null/default 与 nested factory 路由移到 `indexbuilder/ScalarBuildSessionTest.cpp`；旧文件已删除。 |
| 5 | `index/InvertedIndexTest.cpp`：缺失 binlog 前缀、benchmark | 当前 binlog→BuildSession→publish→load 路径移到 `indexbuilder/InvertedBuildSessionTest.cpp`，分别覆盖 null/default 前缀；两个性能对比用例移到 `index/scalar/inverted/InvertedIndexBenchmarkTest.cpp`；旧文件已删除。 |
| 6 | `index/NgramInvertedIndexTest.cpp`：非 LIKE expression、JSON fallback、benchmark | 移到 `exec/expression/NgramExpressionTest.cpp` 与 `index/scalar/ngram/NgramBenchmarkTest.cpp`；前者覆盖普通表达式路径和 JSON 投影 fallback；旧文件已删除。 |
| 7 | `index/TextMatchIndexTest.cpp`：tokenizer 参数、上传/translator/异步 load、LOB/FieldData、Growing、ExprResCache | 分别移到 `common/TokenizerParamsTest.cpp`、`storage/TextArtifactPublicationTest.cpp`、`segcore/storagev1translator/TextIndexTranslatorTest.cpp`、`segcore/SegmentTextFieldLoadTest.cpp`、`segcore/SegmentTextMatchTest.cpp` 和 `exec/expression/TextMatchCacheTest.cpp`；旧文件已删除。 |
| 8 | `index/JsonIndexTest.cpp`：`TestJsonContains`、`TestJsonCast` | 移到 `exec/expression/JsonProjectedExpressionTest.cpp`，覆盖 contains 的索引/原值路径与 string→double cast；旧文件已删除。 |
| 9 | `index/JsonFlatIndexTest.cpp`：contains、三值逻辑、raw-array fallback、validity、batch execution | 移到 `exec/expression/JsonFlatExpressionTest.cpp`，按真实 expression/segment 语义覆盖；旧文件已删除。 |
| 10 | `index/FMIndexTest.cpp`：`ExecutorPath*`、expression fallback/recheck/batch/segment-offset | 移到 `exec/expression/FmIndexExpressionTest.cpp`，覆盖拒绝的操作回退、候选精确复核、跨 batch/chunk 与 null/offset；旧文件已删除。 |
| 11 | `index/RTreeIndexTest.cpp`：InsertData/FileManager/上传加载、GIS/segment/cache/refine | producer 和 publish/load 移到 `indexbuilder/RTreeBuildSessionTest.cpp`；GIS 精确复核、候选缺口、损坏 WKB 与 geometry cache 移到 `exec/expression/GISIndexRefinementTest.cpp`；旧文件已删除。 |
| 12 | `index/BitmapIndexArrayTest.cpp`：encrypted admission、factory wiring、legacy hybrid metadata、`ArrayOffsetsSealedTest` | 分别移到 `storage/ScalarLoadAdmissionTest.cpp`、`indexbuilder/ScalarBuildSessionTest.cpp`、`segcore/storagev1translator/ScalarIndexTranslatorTest.cpp` 和 `common/ArrayOffsetsSealedTest.cpp`。空数组范围与重复构建/析构循环保留；旧文件已删除。 |
| 13 | `index/UtilsTest.cpp`：`TestGetValueFromConfig`、`TestGetValueFromConfigWithoutTypeCheck` | 移到 `index/ConfigParamsTest.cpp`，文件名明确为参数解析；旧文件已删除。 |
| 14 | `index/InvertedIndexArrayTest.cpp`：ContainsAny/ContainsAll/Equal、nested validity、NotEqual prefilter | 移到 `exec/expression/ArrayIndexExpressionTest.cpp`，保留八种类型矩阵与 row-level prefilter 行为；旧文件已删除。 |
| 15 | `index/SkipIndexTest.cpp`、`index/skipindex_stats/SkipIndexStatsTest.cpp`：两个完整文件 | 原文件保留，位置及名称对应当前 `SkipIndex` / skip-index statistics 类型。包括 zone map、column statistics、Arrow/Bloom/ngram metrics 与 fail-open；column metrics generation 用例位于 `SkipIndexTest.cpp`。 |
| 16 | `exec/expression/ExprArithMiscTest.cpp`：`ExprTest`/`Expr` 算术、逻辑、NULL 与 segment 用例 | 原文件保留在 expression 目录，保留八个有断言的执行用例并改为表达意图的名称；Marisa 列比较独立到 `exec/expression/MarisaCompareExpressionTest.cpp`，增加双列 nullable；无断言的 benchmark/skip 用例已删除。 |
| 17 | `unittest/test_indexing.cpp`：`BinaryBruteForce`、四个 `Build*FromBinlog`、`Indexing.IndexStats`、七个 `ReduceApplyRefinedOrder` | binary search 移到 `query/SearchBruteForceBinaryTest.cpp`；binlog producer 与真实发布加载移到 `indexbuilder/VectorArrayBuildSessionTest.cpp`、`VectorBuildSessionRoundTripTest.cpp`；旧 `IndexStats` 的文件顺序/大小和 `mem_size` 目的由 `indexbuilder/ArtifactStatsCapiProjectionTest.cpp` 在当前 `ArtifactStats`→C API 投影上覆盖，还检查重复文件名、空列表、零字节及大于 32 位的大小；结果重排移到 `segcore/reduce/ReduceRefinedOrderTest.cpp`；旧文件已删除。 |
| 18 | `unittest/test_index_wrapper.cpp`：IndexFactory/VecIndexCreator adapter、真实 chunk manager 的 sliced mmap | 用当前 BuildSession/loader 重写到 `indexbuilder/VectorBuildSessionRoundTripTest.cpp`，包括 sliced mmap validity；旧文件已删除。 |
| 19 | `storage/FileWriterTest.cpp`、`storage/LegacyIndexFileIOTest.cpp`、`storage/artifact/FileSinkTest.cpp`、`storage/artifact/LocalDirectoryTest.cpp`：四个完整文件 | 原文件保留在 storage，名称与位置对应 storage I/O、direct read/admission、文件生命周期、目录所有权及 fd/mmap RAII。 |

其余纳入 C++ UT 扫描的 Exec、Plan、Segment、C ABI、common、storage、mmap、query、clustering、futures 等测试，按各自模块/consumer 合同保留在原 target；`index_runtime_acceptance_test` 和 `BitsetTest` 有独立 target。它们不因文件名出现 index、load、artifact 或异步操作而转入 `index_tests`。具体源文件与 target 归属以当前 CMake 清单为准。

### 随退役接口或死代码删除、无需重建的测试

| 编号 | 已删除的旧用例 | 不迁移的原因 |
|---:|---|---|
| 20 | `JsonFlatIndexTest.TestInApply`、`TestInApplyCallback` | 当前 `IJsonIndexReader`/`IScalarPredicateReader` 无 `InApply` callback |
| 21 | `RTreeIndexWrapperTest.FinishReportsBinaryCloseFailure`、旧半完成树 mutation/write-priority mutex 用例 | 当前 builder 没有相同 close 注入、半完成树或旧锁；可达的 open/write/flush/read 错误已在第一类覆盖 |
| 22 | `RTreeIndexTest` 的 sparse/out-of-order `AddGeometry(row_offset)` 五例与一般 engine `bad_alloc` 分类例 | 当前 `Append(row_begin,batch)` 要求连续 offset；当前 reader 没有旧 wrapper 的一般 `bad_alloc` 分类语义 |
| 23 | `InvertedIndexTest.SealedAllValidDoesNotRetainValidityBitmap`、`TextMatchIndexTest` 的旧 `FinalizeSealed`/`ValidityBitmapByteSize` 用例，以及 `JsonFlatIndexTest.ExecutorReusesMaterializedFieldValidity` 中的表示断言 | 旧内存表示已退役；仍需要的 null/NotIn 行为已在第一类覆盖 |
| 24 | `UtilsTest` 的两个 `AssembleIndexDataCodec` slice 用例、`JsonIndexTest.TestSlicedOffsetFilesLoadIndependently` | 仅测试已删除、无生产调用的拼接 helper |
| 25 | `JsonPathIndexTest` 的旧 IndexFactory/Load/Finalize wrapper-only 用例、`ScalarIndexTest` 中旧 `HasRawData` 表示用例 | 旧 API 不属于当前合同；有意义的 reader/loader 行为已在第一类覆盖 |
| 26 | `test_indexing.cpp` 的非 sparse `GetVector_EmptySparseVector` 参数实例 | 测试主体没有断言，不形成行为覆盖 |
| 27 | `TypedScalarIndexCreatorTest.Dummy` 与旧 `Constructor`/`Codec` 的 creator/BinarySet wrapper 调用形状 | `Dummy` 只打印类型和参数，没有断言；旧 `CreateScalarIndex`、直接 dataset Build 与 creator Load/Serialize 接口已退役。八种标量类型的有效构建/持久化目的已由第一类的当前合同 lifecycle 矩阵覆盖，无需重建旧 wrapper。 |

## 当前验证与构建事项

- 默认配置的 `index_tests` 已完整运行：13,124 个注册用例，13,123 通过、1 个 DiskANN side input 能力跳过。`BUILD_DISK_ANN` 等条件编译路径和其他平台配置尚无同等验证。
- 原 22 个 index/indexbuilder 旧测试文件只剩两份 skip-index 文件；`test_indexing.cpp` 与 `test_index_wrapper.cpp` 已删除，CMake 显式引用已移除。第三类的新 `*Test.cpp` 已由 `all_tests` 的 glob 收集。
- 测试迁移阶段，`all_tests` 已成功编译链接，旧 index API 的测试消费方已迁移。当时排除本地环境中 AWS SDK/CRT 双实例初始化失败的三个 S3/MinIO 鉴权 death test、Azure 请求取消的七个测试，以及 packed text 内存估算不足的一个用例后，运行 7,706 个测试：7,697 通过、9 个按现有条件跳过、0 失败，另有 24 个 disabled。本轮已加入每个 scalar reader 的 2 KiB 常驻内存预留，针对 Text translator 的目标测试已通过；上述完整 suite 结果早于本轮生产修复，本轮未重跑完整 `index_tests` 或 `all_tests`。环境排除项不计入通过结论；本地链接同时载入 OpenSSL 1.1 和 3，环境问题仍需独立排查。
- `TextIndexArtifactTest.WriterBudgetConstantsMatchBuildAndGrowingModes` 只断言 500 MiB/15 MiB 常量。运行时 writer budget 生效与否原旧用例也未检验，因此不是遗漏的旧测试迁移。
