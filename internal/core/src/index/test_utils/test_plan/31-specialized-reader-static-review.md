# 任务 3 专用契约静态审查

快照：2026-09-15 源码阶段，固定 master
`a876f471053edb2f68a06a9894afee9810ea7906`。审查了
`TextMatchReaderTest.cpp`, `NgramReaderTest.cpp`, `SpatialReaderTest.cpp`,
`NgramIndexReaderTest.cpp`, `RTreeIndexReaderTest.cpp`,
`TextAndCandidateDataSets.cpp` 和覆盖清单。未运行编译、测试列表或测试命令。

## 当前发现

不存在剩余的可操作 Task 3 源码发现。

此前普通缺口已通过针对 `青铜` 和 `黄金` 的 Jieba PhraseMatch 用例、min-gram 2 中文后缀以及全部五个 min-gram 3 UTF-8 拒绝用例关闭。

## 公开契约边界

- 每个公开 Ngram 观察先调用 `CanHandle`。成功调用要求 Count 大小的结果，证明输出是初始 mask 的子集，并要求每个手动枚举的精确命中仍存在于初始 mask 中。没有公开用例要求特定 false positive。`scalar/ngram/NgramIndexReaderTest.cpp` 的三个用例仅为 `NgramVarcharMin2Max4Heap` 固定有代表性的当前 Phase-1 位图，包括稀疏 AND 合并。
- 每个公开 Spatial 观察使用有效查询 geometry，并要求包含全部手动枚举精确命中的 Count 大小候选超集。公开读取器套件不包含资源计数、无效查询 fallback 和空构建拒绝。`scalar/spatial/RTreeIndexReaderTest.cpp` 在 `SpatialRTreeHeap` 上固定四个有代表性的 RTree 位图，检查全部四个已命名 RTree profile 的当前 heap 计数，并为每个 profile 提供独立精确 `DataIsEmpty` 构建用例。
- Text Match/Phrase/Fuzzy 手动 offset 与每个惰性 Text 数据集保持对齐，包括打包多批 validity、全有效/全空/零行、standard 和 Jieba analyzer。

## 注册与静态计数

两个具体族文件存在于 `INDEX_TEST_FILES`。每个描述符经由 `ReaderObservationCases` 或显式 `FilterParam` 实例展开，因此每个选定后端组合仍是具有惰性数据生成的独立 GTest。

最终 Task 3 计数为 166 个描述符 / 1,370 个参数：Text 39/345、公开 Ngram 108/988、具体 Ngram 3/3、公开 Spatial 10/22，以及具体 RTree reader/build 6/12。

五个已审查 C++ 源及其 CMake 注册均通过 `clang-format --dry-run --Werror` 和 `git diff --check`。在集中式构建/运行门禁开启前，Analyzer 分词、候选引擎输出、RTree 执行、持久化和清理仍仅作静态验证。
