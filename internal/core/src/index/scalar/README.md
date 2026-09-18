# Scalar index helpers

IArtifactBuilder、Loader、Artifact 和 Reader 保持各自职责；公共代码通过函数复用，
不引入共享的具体 builder/reader 基类。

## 构建、持久化与 Reader

sealed IArtifactBuilder 一次接受 `ScalarBuildInput<T>` 并返回完成的 Artifact。调用方持有
稳定的 typed batches 及变长值后备数据；builder 可在调用内重复遍历，但不保存第二份完整原始输入。
`hybrid/` 在同一次 Build 中按已有低/高基数配置选择具体 family，再传递同一输入；不要求调用方重放。
平台 AUTOINDEX 配置不属于这里的 Hybrid 策略命名。

普通 scalar Artifact 只保留序列化所需状态；查询 Reader 由 Loader 打开持久化产物。
sealed local Text Artifact 例外：它公开可选的消费转换，把已完成的 Tantivy engine 及必要的
directory owner 一次性移交给 TextIndexReader。Hybrid 和 JSON projected 包装仅负责选择及序列化，
不透传该能力。

同一 family 内若数值与字符串共享查询生命周期和算法流程，使用一个按值类型模板化的 Reader
外壳；字符串专有的 `IPatternMatchReader` 由公共的 string-view-only CRTP adapter 提供，
数值实例继承空 primary，不具有该接口，`Caps()` 与实际继承一致。Bitmap 在最终算法实现层
接入 adapter，Inverted 与 Sorted 在各自 reader 接入。Sorted 统一为
`SortedIndexReader<T>`：数值 pair 与字符串 dictionary/posting 的布局、读取和计费差异保留在
type-specific storage view 内，不再迫使查询外壳分成两个类。Bitmap 的 posting key 仍为
`owned_t<T>`；字符串查找使用透明比较，`Lookup` 返回拥有的值，`Gather` 的 string_view 只在
同步回调期间有效。

NGRAM 使用 `NgramIndexBuilder<T>` 承接标量 string-view 与 JSON 三态投影两种输入；两种显式
实例共享 writer core，但仍保留各自的 validity/投影状态规则和 registry 入口。

该原则只合并同一算法 family 内重复的 Reader 流程，不建立跨 family 的有状态具体 Reader 基类。
Marisa、FM、Text、Ngram 与 RTree 保留各自的 trie、FM、全文、候选和空间算法对象。JsonFlat
已经把 field state、path、NULL 与资源逻辑收进共享 path-view 基类；bool 使用原生布尔范围，
numeric 同时服务 int64/double 并保留跨数值域的精度边界，string 负责输入 ownership 与 pattern
routing，因此不再用 policy 模板强行合并三类 path view。

## 阅读顺序

1. `../ParamUtils.h`：按顺序处理参数别名，并解码规范化后的整数
   `DataType` 枚举；布尔值统一使用 `GetValueFromConfig<bool>` 的既有转换。
2. 各 family 的 Params 或 Builder/Loader：支持类型、required/default、
   显式 null 行为和字段/元素/值类型关系仍由该 family 决定。
3. `ScalarIndexUtils.h`：C++ 标量类型映射、类型匹配、字符串赋值及 validity bitmap 构造。
4. `../../storage/artifact/FileSourceUtils.h`、`LocalFileUtils.h`：元数据读取、
   文件名检查、文件枚举、文件句柄和临时文件清理。
5. 各 family 的 Builder/Loader/Artifact：调用公共机制后，继续执行各自的
   输入校验、sidecar 处理、引擎打开和序列化流程。

## 不能合并的规则

- 缺失与显式 null 并非所有 family 都等价。布尔值复用
  `GetValueFromConfig<bool>`；不在各 family 增加不同的字符串或数字编码。
- 需要校验一致性的 nested 别名按 `nested`、`is_nested`、`is_nested_index`
  顺序读取。生产加载边界从 schema field name 恢复规范化值，因此旧持久化参数
  缺少这些别名不会失败；进入 family loader 后仍要求该内部参数存在。
- Bitmap 的 STRING/VARCHAR 判断不接受 TEXT，不能替换为包含 TEXT 的公共判断。
- Sorted 的文件读取仍使用原有的单次读取大小和 EOF 错误处理；Bitmap 文件大小
  溢出保留自己的错误信息。
- Tantivy family 的保留文件名、sidecar 集合和 V1/V2/V3 校验不同。公共文件枚举
  明确区分普通文件与所有非目录条目；JsonFlat 的额外校验仍在本地执行。
- 显式 close/unlink 失败仍报告错误，析构期间的清理仍为 best-effort。句柄和
  directory owner 的作用域、引擎销毁顺序及 reader 对后备文件的保留不变。
