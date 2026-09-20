# Scalar indexes

本目录实现 sealed scalar index family。`IArtifactBuilder`、Artifact、Loader 和 Reader
分别负责完整输入构建、构建结果持有与序列化、持久化产物打开和查询；公共代码通过无状态 helper
与 family 内部模板复用，不建立跨 family 的有状态 builder/reader 基类。

## 构建、持久化与 Reader

sealed `IArtifactBuilder` 一次接受完整的 `ScalarBuildInput<T>` 并返回完成的 Artifact。
`ScalarBuildInput<T>` 借用稳定的 typed batches：values 与逻辑行对齐并包含 null 行，空 validity
view 表示全有效，字符串等变长值的后备存储须存活到 `Build` 返回。builder 可在同步调用内重复遍历，
但 builder 和 Artifact 都不得保留借用输入。

Artifact 拥有构建结果并通过 `Serialize` 输出；Loader 从持久化产物独立打开 Reader；Reader 拥有查询
所需的引擎、映射、后备文件和计账状态。`hybrid/` 在同一次 `Build` 中探测基数，按字段形状、
nested 状态、版本和低/高基数配置选择 concrete family，再把同一完整输入交给该 family；Hybrid
Artifact 记录 selector。平台 AUTOINDEX 配置不属于这里的 Hybrid 策略。

`TextIndexArtifact` 实现可选的消费式 Reader 转换，把完成的 Tantivy engine、null 状态以及需要的
directory owner 移交给 `TextIndexReader`。转换会消费 Artifact；Hybrid 和 JSON projected Artifact
只包装选择或投影的序列化状态，不透传该能力。

同一 family 的数值与字符串 reader 可以共享按值类型模板化的查询流程，并统一继承
`PatternMatchReaderAdapter<Derived, T>`。adapter 主模板为空；只有 `T = std::string_view` 的特化
继承 `IPatternMatchReader`，因此数值实例不暴露该接口，`Caps()` 必须与实际继承一致。Bitmap 在
最终 concrete reader 接入 adapter，Inverted 与 Sorted 在各自 typed reader 接入。

Sorted 使用统一的 `SortedIndexReader<T>`；数值 pair 与字符串 dictionary/posting 的布局、搜索和
计费分别封装在 type-specific storage view。Bitmap posting key 使用 `owned_t<T>`，字符串查找使用
透明比较；`Lookup` 返回拥有的值，`Gather` 的 `string_view` 只在同步回调期间有效。

## V3 加载

V3 Artifact 直接写入 `IndexEntryWriter`。读取直接使用 `IndexEntryReader` 或
`AsyncIndexEntryReader`；`FileSink` / `FileSource` 只处理 legacy 文件。
每个 Loader 的 `PlanPacked` 校验 directory 和 metadata 并指定最终内存/文件目标，
`FinishPacked` 从已完成的目标创建 Reader。同步和异步入口共用这两步。
`PackedIndexLoad` 负责读取、CRC、取消后的 drain、目标提交和失败清理；异步文件准备与
文件型 engine 初始化在 LocalFileIOPool 执行。文件目标提交前由 plan 负责清理，成功后
由 Reader 的映射/目录 owner 维持生命周期。无文件目标的初始化沿用 async executor。

## Family 边界

- Marisa、FM、Text、Ngram 和 RTree 分别拥有 trie、FM、全文、候选和空间算法对象，不共享有状态
  concrete Reader 基类。
- NGRAM 的 `NgramIndexBuilder<T>` 接受 scalar `string_view` 与 `JsonProjectedString` 两种完整输入。
  两种实例共享 writer core；scalar validity 与 JSON 的 field-null/missing/value 三态各自处理，并有
  独立 registry 入口。
- JSON projected Artifact 包装一个普通 scalar Artifact，并序列化 path、cast、row count 和
  non-exist 状态；加载后由 `JsonPathIndexReader` 路由到内部 typed Reader，外层不直接暴露内部
  predicate/pattern/ngram mixin。
- JsonFlat 的 root 与 path Reader 共享不可变 field state；bool、numeric、string path Reader 分别
  实现布尔范围、跨 int64/double 数值边界和字符串 ownership/pattern routing。

## 参数、存储与资源约束

- `../ParamUtils.h` 按 `nested`、`is_nested`、`is_nested_index` 顺序读取 nested 别名，并拒绝互相
  冲突的值。schema/boundary 值是权威值，进入要求 normalized 参数的 Loader 前会写入 canonical
  runtime keys；Loader 将缺少该内部参数视为契约错误。
- 参数 helper 只负责别名、一致性和基础类型解码。支持类型、required/default、显式 null 语义以及
  字段/元素/值类型关系由各 family 定义。布尔参数统一使用 `GetValueFromConfig<bool>`。
- Bitmap 的 STRING/VARCHAR 判断不包含 TEXT。Sorted 按声明的文件长度完成精确读取，提前 EOF
  是错误；Bitmap 在自己的格式边界报告 posting 与文件大小溢出。
- Tantivy family 分别定义保留文件名、sidecar 集合和各 storage generation 的校验。公共文件枚举
  明确区分普通文件与所有非目录条目；JsonFlat 执行自己的文件集合与 sidecar 冲突校验。
- 正常路径上的显式 close/unlink 失败必须报告；析构和异常展开期间的清理为 best-effort。Reader
  必须让文件映射、directory owner 和其他后备资源覆盖查询对象的完整生命周期，并按依赖逆序销毁。

## 阅读顺序

1. `../ParamUtils.h`：参数别名、一致性检查及规范化整数 `DataType` 解码。
2. family 的 Params 或 Builder/Loader：支持类型、默认值、null 与字段/元素/值类型关系。
3. `ScalarIndexUtils.h`：C++ 标量类型映射、类型匹配、字符串赋值及 validity bitmap 构造。
4. `../../storage/artifact/FileSourceUtils.h`、`LocalFileUtils.h`：文件名、文件枚举、句柄和
   临时文件生命周期。
5. family 的 Builder/Artifact/Loader/Reader：输入校验、算法状态、sidecar、序列化、打开与查询。
