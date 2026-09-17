# index_tests 构建和运行时摘要

## 配置

- 工作区：`/home/zilliz/.orca/workspaces/milvus/SegcoreRefactor`
- Generator/构建：Ninja、Release、开启 PCH、关闭 unity、关闭 ASan、关闭 coverage、开启 disk ANN
- 编译器缓存：现有 sccache/ccache launcher
- 构建并行度：12 CPU 主机上为 6
- 聚焦 target：`index_tests`；允许按需构建生产依赖
- 无生产源码变更、commit、push 或 PR

## 命令和退出码

初始 configure，退出码 0：

```bash
CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -S internal/core -B cmake_build -G Ninja -DBUILD_UNIT_TEST=ON
```

target 构建：

```bash
cmake --build cmake_build --target index_tests --parallel 6
```

- 第一次构建在 `ScalarValueReaderTest.cpp:56` 以 1 退出；`build.log`。
- 局部名称生成器修复成功编译和链接；`build-2.log`。
- 链接隔离诊断和中间重链接保留在 `build-3.log` 至 `build-7.log`。
- 所需的 Inverted 元数据/config-generic Folly 更新成功构建；`build-8.log`。
- 源码格式化后，最终增量构建以 0 退出；`build-final.log`。

移除过时缓存 `INDEX_TEST_FOLLY_LIBRARY` 后的最终 configure，退出码 0：

```bash
CMAKE_POLICY_VERSION_MINIMUM=3.5 cmake -U INDEX_TEST_FOLLY_LIBRARY -S internal/core -B cmake_build -G Ninja -DBUILD_UNIT_TEST=ON
```

`INDEX_TEST_FOLLY_LIBRARY` 不存在于 `CMakeCache.txt`；当前配置 Lookup 无需缓存的 Release 路径即成功。见 `configure-final.log`。

最终列举，退出码 0：

```bash
scripts/run_index_unittest.sh --gtest_color=no --gtest_list_tests
```

`list-final.log` 包含 7,960 个唯一名称：5,570 个 Scalar predicate、2,372 个 Pattern、六个 Lookup 和 12 个族守卫。

最终实测清理的完整运行，因保留的生产断言失败而以 1 退出：

```bash
TMPDIR=/tmp/segcore-index-test-run-20260914-230828/cleanup-tmp \
  scripts/run_index_unittest.sh \
  --gtest_color=no \
  --gtest_output=xml:/tmp/segcore-index-test-run-20260914-230828/index-tests-cleanup.xml
```

完整输出为 `full-run-cleanup.log`；GTest XML 为 `index-tests-cleanup.xml`。

## 最终结果

| 套件 | 测试数 | 通过 | 失败 | 跳过/错误 | 时间（秒） |
|---|---:|---:|---:|---:|---:|
| BitmapIndexReaderTest | 3 | 3 | 0 | 0 | 0.004 |
| FmIndexReaderTest | 1 | 1 | 0 | 0 | 0 |
| HybridIndexBuilderTest | 2 | 2 | 0 | 0 | 0 |
| InvertedIndexReaderTest | 2 | 2 | 0 | 0 | 0 |
| MarisaIndexReaderTest | 1 | 1 | 0 | 0 | 0 |
| SortedIndexReaderTest | 3 | 3 | 0 | 0 | 0 |
| ScalarReaders/PatternMatchReaderTest | 2,372 | 2,322 | 50 | 0 | 31.129 |
| ScalarReaders/ScalarPredicateReaderTest | 5,570 | 5,476 | 94 | 0 | 58.684 |
| ScalarReaders/ScalarValueInt64Test | 6 | 6 | 0 | 0 | 0.001 |
| **Total** | **7,960** | **7,816** | **144** | **0** | **89.821** |

从 XML 开始时间戳至 XML 发布时间为 90.019 秒。未发生进程崩溃。失败名称集合与前一轮最终源码运行完全匹配（`index-tests-second.xml`，91.074 秒）。

失败分类：

- 74 个 Marisa 嵌入 NUL 结果不匹配：50 个 Pattern 和 24 个 Scalar。
- 18 个 Inverted VARCHAR 嵌入 NUL 查询边界不匹配。
- 52 个 Inverted FLOAT/DOUBLE 符号零不匹配。

精确源码追踪和运行时状态位于 `19-reader-contract-issues.md`。正确的长度感知和 C++ 数值期望保持有效。

## 测试/构建接线修正

- Replaced an unparenthesized structured-binding comma in the GTest Lookup name macro with `std::get<0/1>`, preserving names and cases.
- Added required `field_id = 101` builder metadata to direct Inverted profiles and Hybrid profiles that can select Inverted. This removed all 2,046 setup failures from the first full run.
- Linked the focused executable to the self-contained `libmilvus_core.so` runtime image plus the directly used Folly shared library while retaining compile usage through `COMPILE_ONLY`. This avoids inheriting `milvus_core`'s broad PUBLIC static archive graph and eliminated teardown corruption caused by duplicate global objects. The final NEEDED set is core, Folly, and system runtime libraries; only 26 benign weak RTTI/vtable symbols overlap with milvus-storage.
- Applied clang-format-12 to the new owned support/guard/Lookup sources and the one authorized Pattern block. Final formatter dry-run and `git diff --check` pass.

## 清理

最终运行后，隔离临时根目录包含零个子条目。未留下已知后端临时模式、工作区 core dump 或新的 apport 条目。有意保留证据目录和空 scratch 祖先目录。详情、副作用前缀、所有权、宽泛 `/tmp` 比较和限制位于 `36-2026-09-14-reader-contract-cleanup-summary.md`。
