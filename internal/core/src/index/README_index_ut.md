# index_tests 测试框架

`index_tests` 用一组集中定义的数据集和后端配置，展开索引读取、过滤和
Artifact 生命周期测试。核心原则是：注册阶段只组合轻量描述符；每个 GTest
执行时才生成数据、构建索引并打开 Reader。这样可以让每个“用例 × 后端”
组合拥有独立名称、独立失败结果和独立资源生命周期，同时避免在参数表中保存
大数据或已构建索引。

## 两个集中目录

数据集由 [ScalarTestData.h](test_utils/ScalarTestData.h) 定义描述符和所有权模型，
在 [ScalarDataSets.cpp](test_utils/ScalarDataSets.cpp) 的 `ScalarDataSets()` 中统一
注册。Text、Ngram、Spatial 和 JSON 的数据可以拆到独立 `.cpp`，但仍通过这个
唯一入口加入同一个 `DataCatalog`。

一个 `ScalarDataSet<T>` 只保存：

- 名称和输入 C++ 类型 `T`；
- 输入形状 `BackendInputShape`，例如普通标量、数组行、嵌套元素、WKB 或 JSON；
- 坐标域 `Domain` 和可选的逻辑值类型；
- 数据是否含 null；
- 一个无参数 `make_data` 生成器。

生成器返回 `ScalarTestData<T>`。它拥有值、有效位图、批次边界和每个数据集需要的
元数据。字符串、数组和 JSON 投影的底层字节也由它拥有，不把悬空
`string_view` 或 `ArrayView` 存进全局目录。`batch_sizes` 只描述切分；
`ScalarTestInput<T>` 在运行时生成借用视图，并保证有效位图子视图保持原来的
位偏移。

最小的数据注册形态如下：

```cpp
catalog.Add<int64_t>({
    .name = "SmallNullable",
    .requires_nullable = true,
    .make_data = [] {
        ScalarTestData<int64_t> data({1, 2, 3});
        data.validity.reset(1);
        return data;
    },
});
```

后端由 [ScalarReaderFactory.h](test_utils/ScalarReaderFactory.h) 定义，在
[ScalarReaderBackends.cpp](test_utils/ScalarReaderBackends.cpp) 的
`ScalarReaderBackends()` 中统一注册。一个 `BackendSpec` 表示一个可单独执行的
构建/打开配置，包括索引族、输入形状、物理字段类型、逻辑值类型、坐标域、
可空性、堆内存/mmap 请求、构建/加载参数和打开方式。Hybrid 或 JSON 包装器
可以声明多个可能的加载器索引族，并用数据集元数据补齐路径等运行参数。

用例不复制后端配置，也不按后端名称实现查询结果。后端目录负责说明“怎样创建
Reader”和“Reader 宣称什么能力”；用例负责输入、操作参数和正确结果。

## 从用例到独立 GTest

[CaseTestDriver.h](test_utils/CaseTestDriver.h) 提供公共 `IndexTestCase<T>`。`T` 是构建输入
类型；其余字段记录数据集、输入形状、坐标域、逻辑值类型、输入生命周期和后端
选择条件。`body` 明确本次测试停在哪个阶段：

- `Query<Op>` 打开 Reader 后执行一次查询，能力由 `Op` 给出；
- `QueryBatch<T>` 在同一个 Reader 上按顺序执行多个具名 `Query<Op>`；
- `Observe<T>` 打开 Reader 后执行观察回调，可指定需要的能力；
- `BuildFails` 只调用构建器并校验错误，不进入包装或 `Open`。

`Query<Op>` 使用 `Op::ValueType` 作为构建输入类型，从 `Op::Args` 取得查询参数，
并通过 `Op::Reader` 调用对应 Reader 接口。
`QueryBatch<T>` 中各 `Op` 可以不同，但 `Op::ValueType` 必须都是 `T`。批内名称
不能为空且不能重复。

用例文件定义小型操作适配器后，可以直接注册公共描述符：

```cpp
cases.Add(IndexTestCase<int64_t>{
    .name = "FindOneAndThree",
    .dataset = "SmallNullable",
    .body = Query<In<int64_t>>{
        .args = {.keys = {1, 3}},
    },
});
```

同一数据集需要核对多个查询时可合并为一个用例：

```cpp
cases.Add(IndexTestCase<int64_t>{
    .name = "Membership",
    .dataset = "HundredThousandRows",
    .body = QueryBatch<int64_t>{
        {"In", Query<In<int64_t>>{.args = {.keys = {7, 31}}}},
        {"NotIn", Query<NotIn<int64_t>>{.args = {.keys = {7, 31}}}},
    },
});
```

`IndexTestCases::Add` 在注册阶段先核对数据集描述符，再取以下条件的交集：

1. C++ 输入类型、输入形状、坐标域和逻辑值类型；
2. 数据是否含 null、后端是否允许 null，以及查询或观察阶段声明的能力；
3. 可选的索引族、精确后端名和 `select_backend` 条件。

批量查询对所有子查询声明的 Reader capability 取交集；只有能执行完整 batch 的
后端才会生成 GTest。操作参数导致的 `Unsupported` 仍由对应 `Query<Op>` 的 policy
和错误期望校验，不作为 capability 缺失跳过。

每个匹配后端生成一个 `FilterParam`，名称为
`backend_dataset_case`。参数化测试体只调用 `GetParam().run()`，因此
GTest 报告中的每一项都是一个确定组合，不在单个测试体内循环多个后端。
注册闭包只保留用例配置、数据集描述符和一个后端配置，不调用 `make_data`；数据集
生成的运行元数据会在执行时传给构建/加载参数补全逻辑。

`InputLifetime::KeepUntilBodyCompletes` 让输入数据和借用视图存活到查询或观察结束；
`ReleaseBeforeBody` 使用相互独立的期望数据和构建输入，并在回调前销毁输入所有者。
`BuildFails` 是同步借用输入，只允许前一种生命周期。
`QueryBatch<T>` 沿用同一规则：前一种生命周期只生成一次数据并 Build/Open 一次，
后一种仍生成独立的期望数据和临时构建输入，但整个 batch 也只 Build/Open 一次。

数据集的 `requires_nullable` 表示生成数据确实含 null；后端的 `nullable` 表示该配置
接受可空输入。普通选择不把前者送入不可空后端。只有明确验证这一矛盾输入的
`BuildFails` 才可显式允许不匹配；数据必须含 null，且索引族、名称和谓词取交集后的
后端必须全部不可空，否则注册失败。这不代表所有索引族承诺相同错误。

## 执行与结果规则

公共 driver 在测试体中生成数据和 `ScalarTestInput`。查询和观察阶段调用
`ReaderBackend::Create` 并共同检查 Count 和坐标域；Observe 阶段额外检查
ValueType、Caps 基本关系和资源统计。随后 Query 适配器与 Observe 回调分别检查
自己需要的能力、动态接口及结果。
简单谓词由 `Op::Oracle` 根据原始数据计算期望；复杂 LIKE、正则或边界数据使用
`ManualHits` 明确列出偏移。实际和期望位图直接整体比较。查询错误只包围
`Op::Run`，并校验精确 `ErrorCode`，不会把构建/加载失败误算成正确的查询拒绝。
batch 在每个子查询执行时加入其稳定名称的 `SCOPED_TRACE`；普通 `EXPECT` 失败后
继续执行后续查询，fatal failure 则停止当前 batch，避免继续使用无效 Reader。

`BuildFails` 先在错误断言外生成数据和输入，并通过 `CreateBuilder` 完成 registry
查找与配置解析；错误断言只包围生产构建器的 `Build`。它不调用 Artifact 包装或
`Open`，因此 fixture、registry 和加载错误不会冒充预期的构建失败。

模式匹配的结果真值仍属于用例；后端目录只额外保存 `ShouldUseForOp` 的路由预期：
可使用、拒绝优化但仍可直接查询、不支持、或依数据选择。一个操作即使不适合优化，
只要契约允许直接查询，仍会验证结果。

Ngram 和 Spatial 返回候选集，不承诺等于最终真值。公共契约用例只按各接口承诺
验证候选超集；只有接收初始掩码的接口，才进一步检查不在掩码外新增位和 AND 收缩。
某个具体实现当前产生的完整第一阶段位图，只能放在命名索引族的回归测试中，
不能作为所有候选 Reader 的共同预期。

[AssertHelpers.h](test_utils/AssertHelpers.h) 统一位图构造与相等、空值和错误码断言，
避免在每个契约文件重复一套比较逻辑。

## Build、Open 与资源生命周期

[ScalarReaderFactory.cpp](test_utils/ScalarReaderFactory.cpp) 把运行过程分成
`Build`、`Open` 和组合入口 `Create`：

1. `make_data` 创建拥有实际字节的输入；`ScalarTestInput` 创建借用 span/view。
2. `Build` 从生产 `BuilderRegistry` 创建构建器。构建器在消费输入并返回
   `ArtifactPtr` 后销毁；输入所有者至少活到 `Build` 返回。
3. `Open` 根据后端配置走 `Serialize` 或 `Consume`。
4. 返回 Reader 前，框架用实际加载器派生的 Caps 检查 Reader，并检查 Domain 和
   ValueType。
5. 查询或观察回调完成后释放 Reader；Reader 持有的映射、文件和目录所有者随之
   释放。最后再释放期望数据。

`Serialize` 路径把 Artifact 写入
[TestArtifactIO.h](test_utils/TestArtifactIO.h) 提供的内存 V3 接收端，再通过读取端
解析实际加载器索引族并打开。`Open` 返回时，原 Artifact、接收端、读取端和内存
传输数据都已退出局部作用域，所以返回的 Reader 必须自行拥有查询所需状态。
需要落盘的加载器由读取端原子写入自己的临时位置。

`Consume` 路径把 `ArtifactPtr` 移交给 `IReaderConvertible::FromArtifact`，不经过序列化；返回的
Reader 接管继续存活所需的产物状态。这条路径用于验证 RAM/可消费 Artifact，而非
模拟持久化加载。

mmap 配置把系统临时目录作为父目录。加载器创建的文件或
[LocalDirectory.h](../storage/artifact/LocalDirectory.h) 子目录由 Reader 内部存储的
RAII 所有者持有：先释放映射和文件句柄，再由最后一个目录所有者删除自己创建的
子目录；父目录不属于测试，不会被删除。`enable_mmap` 表示请求，具体数据布局仍可
选择堆内存，因此需要 mmap 的实现回归会另选能实际触发文件布局的数据。

设置 `ReleaseBeforeBody` 的观察用例会调用两次 `make_data`：一份用于期望，一份用于
Build。Build/Open 完成后立即销毁输入所有者，再执行 Reader 断言，从而检查 Reader
没有继续借用测试输入。需要验证更细所有权行为的用例可以显式 reset Reader 或
Artifact，但普通用例不需要管理临时路径。

## 查阅入口

- [ScalarTestData.h](test_utils/ScalarTestData.h)：数据所有权、输入形状和延迟生成的
  数据集描述符。
- [ScalarReaderFactory.h](test_utils/ScalarReaderFactory.h) /
  [ScalarReaderFactory.cpp](test_utils/ScalarReaderFactory.cpp)：后端描述符、筛选、
  Build/Open/Create。
- [CaseTestDriver.h](test_utils/CaseTestDriver.h)：公共用例配置、阶段、生命周期和展开。
- [AssertHelpers.h](test_utils/AssertHelpers.h)：共享位图、空值和错误码断言。
- [TestArtifactIO.h](test_utils/TestArtifactIO.h) /
  [TestArtifactIO.cpp](test_utils/TestArtifactIO.cpp)：测试用 V3 内存传输和原子
  本地落盘。
