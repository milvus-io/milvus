# 剩余核心契约静态审查

日期：2026-09-15

仅基于源码审查，参照 `06-remaining-core-contract-inventory.md`、冻结的根计划、公开的 `IndexReaderBase`、`ReaderCaps`、`NullReader` 和 `ScalarValueReader` 头文件，以及固定 master `a876f471053edb2f68a06a9894afee9810ea7906`。

文件：

- `contracts/query/NullReaderTest.cpp`
- `contracts/query/ScalarValueReaderTest.cpp`
- `contracts/query/IndexReaderTest.cpp`
- 回调所用的共享 `ReaderTestDriver.h` 和 `ScalarReaderFactory.cpp` 路径

不存在剩余的可操作缺口。

- Null 用例覆盖全部八种普通类型；混合、缺失、全空和存在的全有效 validity；单批和多批；穿插的空批；打包的 63/64 边界；有效空字符串与空值空字符串；Element 域；精确补集和返回位图独立性。
- Value 用例覆盖全部支持类型和能力选择的族；有效/空/重复/高基数/非有限值；嵌入 NUL 和自有字符串；后续查询及读取器销毁后的 Lookup 生命周期；Gather 的空/单个/置换/重复/空值/全有效/全空/多批/空批/Element 请求。回调 `i` 被视为输出位置，每个位置均访问一次，无效值不解引用，借用字符串在回调中复制。
- Reader 元数据用例覆盖 Row 和 Element profile、后端提供的 Count/域/值类型、能力到接口的对应、独立 NullReader 可用性、候选/嵌套精确性和稳定的非负资源报告。
- 最初怀疑缺少显式 caps 比较并非缺陷。`ReaderBackend::Open` 覆盖 Consume 和 Serialize 两种模式；它在已解析族及产物/加载元数据完成后从实际具体 loader 推导 caps，两个分支均调用 `ValidateReader`。其 `CapsEqual` 在返回读取器前比较全部十个字段。在 `IndexReaderTest` 中重复 `DeriveCaps` 可能使用不同 Hybrid/JSON 元数据，因而有意避免。
- 正向 Text/Ngram/Spatial/JSON 查询路由和候选语义仍归属其专门套件。共享基础断言在其中复用；这符合所有权边界，不是遗漏 Task 2。

本次审查未运行构建或测试命令。三个文件均通过格式化 dry-run。
