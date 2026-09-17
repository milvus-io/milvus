# 最终 index_tests 验证摘要

日期：2026-09-15  
工作区：`/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor`  
配置：Release，关闭 ASan  
日志根目录：`/tmp/segcore-index-test-run-20260915-041720`

## 构建和列举

最终仅源码格式化差异使用以下命令编译：

```sh
cmake --build cmake_build --target index_tests -j6
```

`build-14.log` 和 `build-14.time.log` 记录退出状态 0、2.29 秒 wall time、`NgramReaderTest.cpp` 的编译和 `unittest/index_tests` 的最终链接。`configure-5.log` 记录前置原地 configure 的退出状态 0。运行时验证后的最终源码变更仅是由已配置 `/usr/bin/clang-format-12` 选择的空白格式化；未改变测试期望或行为。

新鲜的格式化前 binary 使用以下命令显式列举：

```sh
INDEX_TEST_BINARY=/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor/cmake_build/unittest/index_tests \
  scripts/run_index_unittest.sh --gtest_list_tests
```

`list-3.log` 包含 12,524 个唯一测试名。相对于 `/tmp/segcore-index-test-run-20260914-230828/index-tests-cleanup.xml` 的 7,960 测试基线 XML，精确保留 7,954 个名称。移除的六个 `ScalarValueInt64Test.LookupMatchesInput/*_RepeatedNullable` 参数在 `18-lookup-case-mapping.md` 中按 profile 映射为 12 个更强的混合有效性和重复值观察。

## 运行时结果

默认未过滤进程无法完成。以下命令到达第一个锚定 JsonFlat regex，并在写入 XML 前以 134 退出：

```sh
TMPDIR=/tmp/segcore-index-test-run-20260915-041720/full-run-2-tmp \
INDEX_TEST_BINARY=/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor/cmake_build/unittest/index_tests \
scripts/run_index_unittest.sh --gtest_color=no \
  --gtest_output=xml:/tmp/segcore-index-test-run-20260915-041720/index-tests-full-2.xml
```

证据：`full-run-2.log`、`full-run-2.time.log`；wall time 15.66 秒。失败是 Tantivy JSON regex 路径中的 Rust 不可展开 panic，因此不存在 `index-tests-full-2.xml`。

随后在独立进程中执行全部四个受影响 profile 参数。`crash-0.status` 至 `crash-3.status` 均包含 134；相邻日志识别精确测试和同一 `Err(NoEmpty)` panic。其 wall time 分别为 0.83、0.86、0.85 和 0.87 秒。

其余 12,520 个测试使用仅包含这四个精确名称的负 filter 执行：

```sh
TMPDIR=/tmp/segcore-index-test-run-20260915-041720/full-run-remaining-tmp \
INDEX_TEST_BINARY=/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor/cmake_build/unittest/index_tests \
scripts/run_index_unittest.sh --gtest_color=no \
  --gtest_filter=-JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNull_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7Mmap_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNullMmap_JsonEmployees_RegexPattern \
  --gtest_output=xml:/tmp/segcore-index-test-run-20260915-041720/index-tests-remaining.xml
```

`index-tests-remaining.xml` 和 `full-run-remaining.time.log` 记录执行 12,520 个、通过 12,306 个、214 个断言失败、0 个禁用、0 个错误且无进程崩溃。GTest time 为 174.849 秒；wall time 为 175.73 秒。四个隔离进程额外增加 3.41 wall 秒。由于契约失败仍可见，shell 以 1 退出。

跨越列举、剩余运行 XML 和隔离崩溃日志：

| 结果 | 数量 |
|---|---:|
| 通过 | 12,306 |
| 断言失败 | 214 |
| 进程崩溃 | 4 |
| 禁用/跳过 | 0 |
| **已列举和执行** | **12,524** |

12,520 个 XML 名称和四个隔离崩溃名称的并集精确等于 12,524 名称的列举，没有重叠、缺失名称或额外名称。root 独立重复此集合比较。

基线的 144 个失败按精确 GTest 名称保留：全部 50 个 Pattern 和 94 个 ScalarPredicate 失败仍存在，没有旧失败名称转绿，也没有保留的旧通过名称新失败。70 个新增断言失败和四个崩溃均已完整分类：

| 根因 | 断言失败 | 崩溃 |
|---|---:|---:|
| Marisa embedded-NUL predicate/pattern/value behavior | 82 | 0 |
| Inverted embedded-NUL scalar keyword queries | 18 | 0 |
| Inverted signed-zero semantics | 52 | 0 |
| JsonFlat cast vocabulary versus Resolve | 8 | 0 |
| JsonFlat escaped JSON Pointer routing | 8 | 0 |
| JsonFlat `LessEqual` bound flag | 4 | 0 |
| JsonFlat embedded-NUL keyword query | 4 | 0 |
| JsonFlat bool range rejection | 32 | 0 |
| Projected Inverted embedded-NUL query | 4 | 0 |
| Corrupt Text/Inverted Tantivy payload error code | 2 | 0 |
| Anchored JsonFlat regex Rust FFI abort | 0 | 4 |
| **总计** | **214** | **4** |

未削弱任何期望位图、错误 code、regex 或后端适用性以得到这些结果。本仅测试任务未修复生产代码。

问题证据维护在：

- `19-reader-contract-issues.md`，用于原始 scalar/pattern 失败。
- `23-remaining-scalar-json-issues.md`，用于 JSON 和 ScalarValue 发现。
- `24-specialized-reader-issues.md`，用于持久化 Tantivy 错误分类和 JsonFlat regex abort。
- `25-storage-artifact-issues.md`，用于工具阻塞后有意未执行的、源码确认的缺失 slice 构造问题。

每个调用的命令均记录在 `commands.txt`。更早的编译/configuration 实验和第一次 12,880 测试运行时仍保留在同一日志根目录下，且不作为最终结果证据。

## 各套件结果

表格将四个隔离 JsonFlat 崩溃加入剩余运行 XML 中的 624 个 JsonReader 用例。`suite-outcomes.tsv` 以机器可读形式包含相同数据。

| 套件 | 测试数 | 通过 | 断言失败 | 崩溃 | 禁用 | 错误 | XML 中的 GTest 秒数 |
|---|---:|---:|---:|---:|---:|---:|---:|
| `RegistryTest` | 8 | 8 | 0 | 0 | 0 | 0 | 0.001 |
| `ReaderConvertibleTest` | 7 | 7 | 0 | 0 | 0 | 0 | 0.107 |
| `JsonResolvedReaderTest` | 1 | 1 | 0 | 0 | 0 | 0 | 0.000 |
| `BitmapIndexArtifactTest` | 10 | 10 | 0 | 0 | 0 | 0 | 0.005 |
| `BitmapIndexReaderTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.000 |
| `FmIndexArtifactTest` | 11 | 11 | 0 | 0 | 0 | 0 | 0.011 |
| `FmIndexReaderTest` | 1 | 1 | 0 | 0 | 0 | 0 | 0.000 |
| `HybridIndexBuilderTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.000 |
| `InvertedIndexArtifactTest` | 9 | 8 | 1 | 0 | 0 | 0 | 0.405 |
| `InvertedIndexReaderTest` | 2 | 2 | 0 | 0 | 0 | 0 | 0.000 |
| `MarisaIndexArtifactTest` | 10 | 10 | 0 | 0 | 0 | 0 | 0.015 |
| `MarisaIndexReaderTest` | 1 | 1 | 0 | 0 | 0 | 0 | 0.000 |
| `SortedIndexArtifactTest` | 12 | 12 | 0 | 0 | 0 | 0 | 0.002 |
| `SortedIndexReaderTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.000 |
| `RTreeIndexArtifactTest` | 5 | 5 | 0 | 0 | 0 | 0 | 0.003 |
| `TextIndexArtifactTest` | 7 | 6 | 1 | 0 | 0 | 0 | 0.196 |
| `NamedBufferSinkTest` | 9 | 9 | 0 | 0 | 0 | 0 | 0.002 |
| `NamedBufferSourceTest` | 16 | 16 | 0 | 0 | 0 | 0 | 0.012 |
| `LocalDirectoryTest` | 7 | 7 | 0 | 0 | 0 | 0 | 0.004 |
| `LocalEntryGuardTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.001 |
| `MappedRegionGuardTest` | 1 | 1 | 0 | 0 | 0 | 0 | 0.000 |
| `FileDescriptorGuardTest` | 1 | 1 | 0 | 0 | 0 | 0 | 0.000 |
| `ScalarProfiles/ProductionRegistryTest` | 430 | 430 | 0 | 0 | 0 | 0 | 1.355 |
| `ScalarAndArrayBuilders/ArtifactBuilderTest` | 262 | 262 | 0 | 0 | 0 | 0 | 3.007 |
| `ScalarReaders/IndexReaderTest` | 240 | 240 | 0 | 0 | 0 | 0 | 3.232 |
| `JsonReaders/JsonIndexReaderTest` | 628 | 564 | 60 | 4 | 0 | 0 | 16.577 |
| `NgramBackends/NgramReaderTest` | 988 | 988 | 0 | 0 | 0 | 0 | 35.515 |
| `ScalarReaders/NullReaderTest` | 672 | 672 | 0 | 0 | 0 | 0 | 10.930 |
| `ScalarReaders/PatternMatchReaderTest` | 2372 | 2322 | 50 | 0 | 0 | 0 | 30.805 |
| `ScalarReaders/ScalarPredicateReaderTest` | 5570 | 5476 | 94 | 0 | 0 | 0 | 57.641 |
| `ScalarReaders/ScalarValueReaderTest` | 826 | 818 | 8 | 0 | 0 | 0 | 0.335 |
| `SpatialBackends/SpatialReaderTest` | 22 | 22 | 0 | 0 | 0 | 0 | 0.017 |
| `TextBackends/TextMatchReaderTest` | 345 | 345 | 0 | 0 | 0 | 0 | 14.303 |
| `HeapAndMmap/FmIndexV3OwnershipTest` | 2 | 2 | 0 | 0 | 0 | 0 | 0.003 |
| `RequiredParts/FmIndexMissingPartTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.001 |
| `HybridCases/HybridIndexBuilderLifecycleTest` | 13 | 13 | 0 | 0 | 0 | 0 | 0.183 |
| `RequiredEntries/MarisaMissingEntryTest` | 2 | 2 | 0 | 0 | 0 | 0 | 0.003 |
| `CsrParts/MarisaIncompleteCsrTest` | 4 | 4 | 0 | 0 | 0 | 0 | 0.010 |
| `NgramHeap/NgramIndexReaderTest` | 3 | 3 | 0 | 0 | 0 | 0 | 0.138 |
| `RTreeHeap/RTreeIndexReaderTest` | 8 | 8 | 0 | 0 | 0 | 0 | 0.008 |
| `RTreeProfiles/RTreeIndexBuildTest` | 4 | 4 | 0 | 0 | 0 | 0 | 0.000 |
