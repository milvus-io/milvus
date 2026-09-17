# IndexTestCase 迁移运行记录

> 本文件仅记录本地开发验证，不提交，也不由 README 或其他最终文档引用。

- 日期：2026-09-15
- 工作区：`/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor`
- 源码 HEAD：`5646b9fadd4e9511761d79d4efad57b0796c125a`
- 配置：Release，`WITH_ASAN=OFF`，`USE_ASAN=OFF`，Ninja，`-j6`
- 本轮日志根目录：`/tmp/segcore-index-test-run-20260915-185344`
- 旧结果根目录：`/tmp/segcore-index-test-run-20260915-041720`

## 构建和列举

第一次执行：

```sh
cmake --build cmake_build --target index_tests -j6
```

Ninja 在 `36/38` 后没有存活的编译器进程，只剩一个 defunct shell；该次执行被中止并保存为 `build-attempt1.log`、`build-attempt1.status` 和 `build-stall-process.txt`。这是与旧运行相同的构建工具僵死，不是编译错误，也没有据此修改测试源码。

随后原地重新配置并重新构建：

```sh
cmake -S internal/core -B cmake_build
cmake --build cmake_build --target index_tests -j6
```

`configure.status` 为 0，wall time 为 11.81 秒；最终 `build.status` 为 0，wall time 为 2.33 秒。最终构建证据是 `configure.log`、`configure.time.log`、`build.log` 和 `build.time.log`。

使用最终 binary 列举：

```sh
INDEX_TEST_BINARY=/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor/cmake_build/unittest/index_tests \
  scripts/run_index_unittest.sh --gtest_list_tests
```

`list.status` 为 0，wall time 为 0.80 秒。`list.log` 和 `current-test-names.txt` 包含 12,525 个唯一测试名；`list-duplicates.txt` 为空。

## 运行方式

四个已知会令进程中止的 JsonFlat regex 参数从主进程排除。主进程执行 12,521 个测试，并把结果写入 `main.xml`：

```sh
TMPDIR=/tmp/segcore-index-test-run-20260915-185344/main-tmp \
INDEX_TEST_BINARY=/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor/cmake_build/unittest/index_tests \
scripts/run_index_unittest.sh --gtest_color=no \
  --gtest_filter=-JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNull_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7Mmap_JsonEmployees_RegexPattern:JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNullMmap_JsonEmployees_RegexPattern \
  --gtest_output=xml:/tmp/segcore-index-test-run-20260915-185344/main.xml
```

`main.status` 为 1，原因是 214 个保留的契约断言失败；进程正常写出完整 XML。GTest time 为 189.455 秒，wall time 为 190.27 秒。

四个排除名称分别在独立进程中执行：

- `JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7_JsonEmployees_RegexPattern`
- `JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNull_JsonEmployees_RegexPattern`
- `JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7Mmap_JsonEmployees_RegexPattern`
- `JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7NonNullMmap_JsonEmployees_RegexPattern`

`crash-0.status` 至 `crash-3.status` 均为 134。四份 `.time.log` 均记录 signal 6；GNU time 同时打印的 `Exit status: 0` 是信号终止时的歧义，不作为进程结果。四份运行日志均到达对应的 `[ RUN ]`，并在 `regex_query.rs:96` 记录 `unwrap()` 收到 `Err(NoEmpty)` 后 non-unwinding abort。wall time 依次为 0.95、0.90、0.86 和 0.85 秒。

主进程与四个隔离进程的 wall time 合计 193.83 秒，不含配置、构建、列举和编排间隔。

## 最终结果

`main.xml` 含 12,521 个唯一名称；它与四个隔离名称没有交集，并集精确等于列举的 12,525 个名称。没有缺失、额外、重复、禁用或跳过的测试。

| 结果 | 数量 |
|---|---:|
| 通过 | 12,307 |
| 断言失败 | 214 |
| 进程崩溃 | 4 |
| 禁用/跳过 | 0 |
| **列举并执行** | **12,525** |

## 与旧基线逐名比较

旧基线来自 `37-2026-09-15-scalar-contract-run-summary.md`、旧 `list-3.log`、`index-tests-remaining.xml` 和四份旧 crash 日志。机器清单为：

- `/tmp/segcore-index-test-old-baseline.json`：旧 12,524 个名称及逐名 outcome；
- `/tmp/segcore-index-test-old-registered.txt`：旧注册名称；
- `/tmp/segcore-index-test-old-failures.tsv`：旧 214 个断言失败及类别；
- `/tmp/segcore-index-test-old-crashes.txt`：旧四个 crash 名称；
- `/tmp/segcore-index-test-run-comparison.json`：本轮逐名差分结果。

比较结果：

- 旧 12,524 个注册名全部保留；删除 0 个；
- 唯一新增名称为 `ScalarBuilders/ArtifactBuilderFailureTest.RejectsInvalidInput/FmIndexVarcharNonNull_NullVsEmptyString_NonNullableRejectsNullInput`，本轮通过；
- 旧 12,306 个通过名称全部继续通过；
- 旧 214 个断言失败名称全部继续失败，没有旧失败消失，也没有新的断言失败；
- 旧四个进程崩溃名称全部仍以 status 134、signal 6 崩溃；
- outcome 类型没有变化，新增失败为 0。

还对 214 条 failure message 做了内容比较。40 条逐字相同；其余 174 条只改变首行断言源码位置，例如公共断言迁入 `AssertHelpers.h` 或源文件行号因迁移而移动。移除首行 `path:line` 后，214 条消息正文全部逐字相同；actual、expected、error code 和断言表达式没有变化。因此不存在相同测试名改为框架错误而被错误归入旧失败的情况。

## 保留的生产问题

本轮 214 个断言失败和四个进程崩溃与旧基线逐名、逐原因一致，没有发现新的生产问题：

| 类别 | 断言失败 | 崩溃 | 既有证据 |
|---|---:|---:|---|
| Marisa 嵌入 NUL 的谓词、模式和值读取行为 | 82 | 0 | `19-reader-contract-issues.md`、`21-scalar-predicate-issues.md`、`22-pattern-match-issues.md`、`23-remaining-scalar-json-issues.md` |
| Inverted 嵌入 NUL 的标量关键词查询 | 18 | 0 | `19-reader-contract-issues.md`、`21-scalar-predicate-issues.md` |
| Inverted 有符号零语义 | 52 | 0 | `19-reader-contract-issues.md`、`21-scalar-predicate-issues.md` |
| JsonFlat 类型转换词汇表与 Resolve 不一致 | 8 | 0 | `23-remaining-scalar-json-issues.md` |
| JsonFlat 转义 JSON Pointer 路由 | 8 | 0 | `23-remaining-scalar-json-issues.md` |
| JsonFlat `LessEqual` 边界标志 | 4 | 0 | `23-remaining-scalar-json-issues.md` |
| JsonFlat 嵌入 NUL 的关键词查询 | 4 | 0 | `23-remaining-scalar-json-issues.md` |
| JsonFlat 拒绝 bool 区间查询 | 32 | 0 | `23-remaining-scalar-json-issues.md` |
| 投影 Inverted 嵌入 NUL 的查询 | 4 | 0 | `23-remaining-scalar-json-issues.md` |
| 损坏的 Text/Inverted Tantivy payload 错误码 | 2 | 0 | `24-specialized-reader-issues.md` |
| 带锚点的 JsonFlat regex 导致 Rust FFI 中止 | 0 | 4 | `24-specialized-reader-issues.md` |
| **总计** | **214** | **4** | |

本轮验证阶段没有为编译或运行修改测试，也没有修改生产代码、错误码或正确期望。唯一构建恢复操作是原地重新配置后重试相同的构建目标。
