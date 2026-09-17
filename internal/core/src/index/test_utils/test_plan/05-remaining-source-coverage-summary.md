# 标量/索引契约源码覆盖摘要

日期：2026-09-15

证据基线：固定的 master
`a876f471053edb2f68a06a9894afee9810ea7906`, PR #64
`b7cfc84da8ad03ca3b33daca4242a73cacfa614c`, PR #65
`f672fd596774bb25f47a138a5ce59983932954d3`, and integration snapshot
`af90d325473284279ae680b14ddde1f9b9c82464`.

本工作起于基于源码的统计。最终由框架负责的集中构建和 GTest 列表报告 12,524 个唯一测试。下文汇总集中运行时证据；保留的红测仍启用，作为契约发现。

## 静态套件矩阵

| 套件 | 静态源码数量 | 覆盖台账 | 独立审查 |
|---|---:|---|---|
| ScalarPredicateReader | 506 个描述符 / 5,570 个参数 | `10-scalar-predicate-coverage.md` | `27-scalar-predicate-static-review.md` |
| PatternMatchReader | 207 个描述符 / 2,372 个参数 | `11-pattern-match-coverage.md` | `28-pattern-match-static-review.md` |
| NullReader、ScalarValueReader、IndexReader | 161 个描述符 / 1,738 个参数 | `12-core-reader-coverage.md` | `30-core-reader-static-review.md` |
| Text、Ngram、Spatial 公开契约和具体系列回归 | 166 个描述符 / 1,370 个参数 | `13-specialized-reader-coverage.md` | `31-specialized-reader-static-review.md` |
| JsonIndexReader / JsonResolvedReader | 133 个描述符 / 628 个参数，加 1 个局部所有权测试 = 629 | `14-json-reader-coverage.md` | `32-json-reader-static-review.md` |
| Registry、构建器/转换、系列产物和本地存储生命周期 | 835 个 GTest，含 2 个已有 Hybrid 守卫；净新增 833 个 | `15-index-lifecycle-coverage.md`、`16-storage-artifact-coverage.md`、`17-marisa-fm-artifact-coverage.md` | `33-index-lifecycle-static-review.md`、`34-marisa-fm-artifact-static-review.md` |
| 共享惰性数据/后端配置/工厂/驱动/产物 IO | 430 个中心后端配置；是支持代码，非独立套件计数 | `15-index-lifecycle-coverage.md` | `29-remaining-framework-static-review.md` |

最终契约台账合计 12,514 个 GTest/参数。此前二进制包含 7,960 个测试。6 个已有 int64 Lookup 测试由新的 `ScalarValueReaderTest` 矩阵替代，不能重复计入。最终净增 4,564 个，整个二进制共 12,524 个测试：`7,960 - 6 + 1,738 + 1,370 + 629 + 833`。这与最终集中 GTest 列表一致。与契约台账总数相差的 10 个测试，是这些台账之外已有的非 Hybrid 系列守卫范围。最终的 430 配置目录和缩减的 JSON/生命周期展开，取代了此前 12,880 的中间预测。

任务 3 的 1,370 个参数分为 Text 39/345、公开 Ngram 108/988、具体 Ngram 3/3、公开 Spatial 10/22，以及具体 RTree 读取/构建 6/12。公开候选集契约只断言文档化的超集/AND 语义；固定的 Phase-1 Ngram 和 MBR/fallback 结果隔离在具名的中心 Ngram/RTree 后端配置中。

任务 5 的 835 个测试包括 Registry 438、ArtifactBuilder 262、Consume 7、Bitmap/Sorted/Inverted/Hybrid/Text/RTree 产物 59、Marisa/FM 产物 32、NamedBuffer/LocalDirectory 37。`InvertedIndexArtifactTest.cpp` 现有 9 个测试，包括独立的空/保留文件清单和越界 null-offset 拒绝。

## Master 映射归属

- ScalarPredicateReader 负责 bool/整数/浮点/VARCHAR 值、可空分布、批次、高基数、无穷值和每个兼容精确读取器系列中的直接有类型成员、一元和区间期望结果。
- PatternMatchReader 负责六个字符串索引系列的直接 Match、Prefix、Postfix、Inner、Regex 结果，LIKE 通配符/转义语法、Unicode、空/长/嵌入 NUL 数据、null 处理、优化路由和 Unsupported 行为。
- 核心读取器套件负责 Count、ValueType、坐标域、具体加载器 caps/接口对应关系、NullReader、有类型的 Lookup/Gather、返回位图独立性及读取器/值所有权。
- TextMatchReader 负责 Match/Phrase/Fuzzy、标准和 Jieba 分词器、min-should-match/slop/edit 边界、可空/多批/零行数据及持久化/RAM 引擎后端配置。
- NgramReader 负责 CanHandle、Count 大小的 Phase-1 候选超集、AND 合并、精确命中保留、全部 PatternOps、UTF-8 字符边界、重叠/转义用例和 JSON 投影字符串。具名 Ngram 系列测试保留 3 个当前具体候选输出。
- SpatialReader 负责全部 8 个 SpatialOps 的手工枚举必需真命中、Count/null 元数据、无效/空存储 WKB 和调用方扩展的 DWithin 输入。具名 RTree 测试负责当前 MBR 输出、invalid-query fallback、堆资源统计和 4 个后端配置的空构建拒绝。
- JsonIndexReader 负责 JsonFlat 有类型谓词/模式视图、CastTypesOf、Resolve/Exists 路由、投影 scalar/ARRAY/Ngram 子项、JSON-pointer 错误、三态 null/missing/value 语义和已解析子项所有权。
- 任务 5 负责 registry 唯一性/路由、借用输入和转换生命周期、Hybrid 选择器、V1/V2 和 V3 产物布局、公开元数据关系、必需部件/payload 损坏、NamedBuffer source/sink、原子 materialization、LocalDirectory 和资源守卫。

## 保留的生产发现

- `21-scalar-predicate-issues.md`：长度感知的 Marisa 和 Inverted 嵌入 NUL 谓词失败，以及 Inverted 正负零排序。期望仍符合契约，受影响后端未跳过。
- `22-pattern-match-issues.md`：运行时确认的 50 个 Marisa 嵌入 NUL Pattern 失败，以及当前 malformed-regex 错误分类。
- `23-remaining-scalar-json-issues.md`：JsonFlat 声明的 cast 类型并非其 Resolve 路径始终提供，错误处理转义 JSON Pointer 路由和一个字符串边界标志，丢失嵌入 NUL 关键字查询长度，并暴露 Tantivy 拒绝的 bool 有序范围。正确契约期望仍启用。
- `25-storage-artifact-issues.md`：已声明但缺失的物理 slice 可到达未经检查的 `Assemble` 解引用。进程隔离测试两次被执行工具阻塞，按 root 指示未重试或绕过。有效的独立手工 slice 和受控损坏元数据仍受覆盖。
- `24-specialized-reader-issues.md`：损坏的持久化 Text 和 Inverted Tantivy 文件通过存在性探测，随后绑定加载失败被呈现为 `UnexpectedError`，而非保留的 `DataFormatBroken` 产物契约。

## 明确排除与暂缓

- #66 vector 索引、growing 发布和 Segment 所有的投影不在本标量/本地产物阶段。
- #67 executor 规划、精确 Ngram/FM/spatial 第二阶段精炼、表达式缓存/融合、materializer/translator 服务、FileManager 和 ChunkManager 远程传输、取消及上传路径行为仍属消费者/服务范围。
- FM general-LIKE 候选/复核行为不是精确 PatternMatchReader 操作；直接 Unsupported 行为和受支持字面量操作仍受测试。
- NaN 谓词排序/相等性仍暂缓，因为未找到共享产品契约。已覆盖定义的正/负无穷行为。
- 无效 Lookup/Gather offset 及 null-pointer/count 组合未由 ScalarValueReader 规定，未固定到当前断言。
- 无界 fuzzing、benchmark、OOM/bad-allocation 注入、强制进程终止、并发/饥饿、远程缺失对象矩阵、私有 codec 内部和历史兼容性矩阵需要专用机制，不属于普通声明式契约用例。

## 源码阶段验收

所有负责的 C++ 源码都已通过 `clang-format --dry-run --Werror` 和范围化 `git diff --check`。共享框架、核心、JSON、专用、生命周期、存储和 Marisa/FM 范围均已独立静态审查，无剩余可操作源码发现。

## 集中运行时证据

- 最终 GTest 列表：12,524 个唯一名称
  (`/tmp/segcore-index-test-run-20260915-041720/list-3-summary.txt`).
- 未中止的运行执行了 12,520 个测试，保留 214 个失败，无 disabled 测试
  (`/tmp/segcore-index-test-run-20260915-041720/index-tests-remaining.xml`,
  `/tmp/segcore-index-test-run-20260915-041720/full-run-remaining.status`, and
  `/tmp/segcore-index-test-run-20260915-041720/full-run-remaining.time.log`).
  它保留全部 144 个已知基线失败，未在保留的基线测试中引入失败，并暴露 70 个新契约失败
  (`/tmp/segcore-index-test-run-20260915-041720/final-baseline-summary.txt`).
- 4 个被排除的 JsonFlat anchored-regex 参数均独立运行，均以 134 退出；其 `crash-{0..3}.log`、状态、时间和清理清单在同一运行目录。它们是生产中止，不是 disabled 测试。
- 框架负责的集中入口将在发布后为 `37-2026-09-15-scalar-contract-run-summary.md`。
