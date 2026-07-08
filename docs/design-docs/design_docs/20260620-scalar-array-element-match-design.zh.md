# 设计文档：数组与 JSON 元素匹配（`MATCH_*` / `element_filter`）

**分支**：`feat/array-json-element-match`
**日期**：2026 年 6–7 月
**范围**：表达式文法 + planparserv2（Go）、segcore（C++）。向 `plan.proto` 以新 tag 增加 `MatchColumnInfo` 定位符；旧 `MatchExpr.struct_name` 保留在 tag 1 并标 `[deprecated]`，proxy 双写、querynode 回退，保证滚动升级线兼容（见 §7）。无 RPC/SDK 改动。

将量化元素匹配能力（`MATCH_ANY/ALL/LEAST/MOST/EXACT` + `element_filter`）在三类
数组形态容器 —— 标量数组、结构体数组、**JSON 数组** —— 上统一起来，并允许标量/JSON
数组以 **nested tantivy 索引**作为元素载体。第 1–5 节描述标量数组核心，第 6 节描述
JSON 数组与 nested 索引扩展。

---

## 1. 概述

### 1.1 动机

当前标量 `ARRAY` 字段只支持**相等 / 成员关系**过滤：
`array_contains`、`array_contains_all`、`array_contains_any`，外加 `array_length`
和按位置访问 `arr[i]`。目前**无法对数组元素表达带量词的比较** —— 例如「存在元素 `> 90`」、
「所有元素 `>= 60`」或「至少 2 个元素 `== 100`」。

结构体数组字段（array-of-struct）通过
`MATCH_ANY/MATCH_ALL/MATCH_LEAST/MATCH_MOST/MATCH_EXACT` 和 `element_filter` 已经支持这一能力，
并用 `$[subField]` 引用结构体元素的子字段。本特性把**同样的带量词元素过滤能力带给普通标量数组**，
从而弥合标量数组与结构体数组之间的功能差距。

示例：

```text
MATCH_ANY(scores, $ > 90)                       # 存在元素 > 90
MATCH_ALL(scores, $ >= 60)                       # 所有元素 >= 60
MATCH_LEAST(scores, $ == 100, threshold=2)       # 至少 2 个元素 == 100
MATCH_MOST(scores, $ > 90, threshold=1)          # 至多 1 个元素 > 90
MATCH_EXACT(scores, $ == 100, threshold=3)       # 恰好 3 个元素 == 100
MATCH_ANY(tags, $ == "x")                         # VarChar 元素数组同样支持
MATCH_ANY(scores, $ > 60 && $ < 90)              # 复合元素谓词
```

### 1.2 关键需求

1. **标量数组上的带量词元素过滤** —— 五个 `MATCH_*` 算子，语义与结构体数组一致。
2. **新的元素自引用 token `$`** —— 引用数组元素本身
   （标量数组没有子字段，因此对其使用 `$[x]` 会被拒绝）。
3. **支持所有标量元素类型** —— Bool / Int8 / Int16 / Int32 / Int64 / Float /
   Double / VarChar。
4. **支持两种 segment 类型** —— sealed（chunked）和 growing。
5. **索引加速** —— 复用已有的嵌套标量索引路径，使数组字段上的倒排索引能加速元素谓词。
6. **无回归** —— 结构体数组 `MATCH_*` 行为与 `array_contains*` /
   `array_length` 不变。`plan.proto` 以新 tag 新增 `MatchColumnInfo` 定位符（`struct_name` 保留在 tag 1 弃用，线兼容见 §7）；无 RPC/SDK 改动。

### 1.3 设计原则

**把标量数组视为只有一个隐式子字段（即元素值本身）的结构体数组。**
在这一视角下，`$` 就是 `$[subField]` 的标量类比，整条已有的结构体数组执行流水线
（行↔元素 offset 映射、元素谓词求值、量词计数、嵌套索引查找）都可以原样复用。
因此本特性是**增量式管道接线** —— 一个新 token、一处元素自引用列解析，
以及为标量数组字段注册元素 offset —— 而非一套新的执行引擎。

---

## 2. 架构概览

### 2.1 数据流

```
expr 字符串 ──► planparserv2 (Go)
                 │  MATCH_*/element_filter(arrayField, <对 $ 的谓词>)
                 │  └─ $  ──► 元素级 ColumnInfo
                 │            { field_id = arrayField, data_type = ElementType,
                 │              is_element_level = true }
                 ▼
            planpb.MatchExpr { column = MatchColumnInfo{field, data_type, nested_path},
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(column)
                 │   │     标量 ARRAY ─► 该字段；struct ─► 第一个数组子字段
                 │   ├─ segment->GetArrayOffsets(field_id)   # IArrayOffsets
                 │   ├─ 行 → 展平后的元素 offset
                 │   ├─ 子谓词 Eval（对 $ 做元素级读取）
                 │   └─ 按行做量词判定：将匹配数与 MatchType/threshold 比较
                 ▼
            行级 bitset
```

### 2.2 各层职责

| 层 | 职责 | 改动 |
|-------|----------------|--------|
| 文法（`Plan.g4`） | 将 `$` 词法识别为 `ElementSelf`；接受其作为表达式及出现在 range 形式中 | 新增 token + 备选式 |
| 解析器（`parser_visitor.go`） | 将 `$` 解析为元素级 `ColumnInfo`；将 `MATCH_*`/`element_filter` 路由到标量数组或结构体数组路径 | 新增解析 + 分支 |
| Plan proto | 定位数组/JSON 目标 | **新增** `MatchColumnInfo` 定位符（tag 5）；`struct_name` 保留在 tag 1 弃用并双写（线兼容，§7） |
| Segcore —— offsets | 为标量 `ARRAY` 字段构建/维护 `IArrayOffsets`（sealed + growing） | 扩展注册逻辑 |
| Segcore —— 解析 | 解析 `MATCH_*` 目标，可为标量数组或结构体数组 | `Schema::ResolveArrayElementField` |
| Segcore —— 执行 | 读取元素值、求值谓词、做量词判定 | **复用**已有的 `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |

### 2.3 本特性**不**改动的内容

- 结构体数组 `MATCH_*` / `element_filter` 的语义与代码路径。
- `array_contains` / `array_contains_all` / `array_contains_any` / `array_length`。
- RPC 或 SDK 线格式 —— client 发的是表达式字符串，不是 plan proto；仅内部 `plan.proto` 的 `MatchExpr` 变更（新增 `MatchColumnInfo` 定位符）。
- 元素值读取路径（`ProcessElementLevelByOffsets`）—— 本就是通用的。

---

## 3. 语法设计

### 3.1 元素自引用 token `$`

`$` 表示 `MATCH_*` / `element_filter` 谓词中当前正在求值的数组元素。
它是结构体数组 `$[subField]` 的标量对应物：

- `$` —— 元素值（标量数组）。
- `$[subField]` —— 结构体元素的子字段（结构体数组）。

互斥性由解析器强制保证：

- 在标量数组 `MATCH_*` 中使用 `$[subField]` → 报错
  *「scalar array element has no sub-fields; use `$` instead of `$[subField]`」*。
- 在任何 `MATCH_*`/`element_filter` 之外裸用 `$` → 报错
  *「`$` 只能用于标量数组字段上的 MATCH_*/element_filter 内部」*。

`$` 同样可用于 range 表达式：`MATCH_ANY(scores, 60 < $ < 90)`。

### 3.2 算子（语义与结构体数组完全一致）

| 算子 | 当满足以下条件时该行命中 |
|----------|-------------------|
| `MATCH_ANY(arr, pred)` | ≥ 1 个元素满足 `pred` |
| `MATCH_ALL(arr, pred)` | 所有元素满足 `pred`（**空数组 = 空真，vacuous true**） |
| `MATCH_LEAST(arr, pred, threshold=N)` | 匹配元素数 ≥ N |
| `MATCH_MOST(arr, pred, threshold=N)` | 匹配元素数 ≤ N |
| `MATCH_EXACT(arr, pred, threshold=N)` | 匹配元素数 == N |

`pred` 是任意对 `$` 的元素级谓词（比较、range、`&&`/`||`、
针对 VarChar 元素的 `like`/正则），与结构体数组谓词对 `$[subField]` 所允许的形式一致。

`element_filter(arr, pred)` 产生**元素级** bitset，专用于元素级向量检索；
它**不是**独立的行级过滤器（行级标量数组过滤使用 `MATCH_*`）。这与已有的结构体数组约定一致。

**容器间差异（有意为之，已用测试钉住）**：

- `element_filter` 只接受物理数组列（标量/结构体数组）—— 它依赖
  `IArrayOffsets`，而 JSON 数组是求值时逐文档解析的，没有元素偏移。JSON 用户用
  `MATCH_ANY(json["path"], pred)` 表达同一语义；解析器拒绝对 JSON 字段使用
  `element_filter`。
- 结构体子字段上的 `array_contains_all` 在解析期脱糖为
  `MATCH_ANY(...) && MATCH_ANY(...)` 合取，因此不能接受模板占位符列表
  （模板值在解析之后才填充；标量/JSON 的 `contains_all` 走运行时
  `JSONContainsExpr` 路径，segcore 没有结构体实现）。空列表在三种容器上
  一律是 **IS NOT NULL 语义**：对任何真实数组（含空 `[]`）空真；对 NULL 行
  UNKNOWN → 排除（pg：`@>` 是 strict 运算，`NULL @> '{}'` 得 NULL 而非 true）；
  对解析不出真实数组的 JSON path 同样排除。结构体脱糖产出与
  `struct_array is not null` 相同的 `NullExpr IsNotNull` plan；标量/JSON 的
  运行时空列表路径逐行求值而非笼统返回 true。`contains_any(x, [])` 对所有行
  保持确定 FALSE（零次比较，pg：`NULL = ANY('{}')` = false）。

---

## 4. 组件设计

### 4.1 文法（`internal/parser/planparserv2/Plan.g4`）

- 新增词法 token `ElementSelf: '$';`。ANTLR 的最长匹配规则保证 `$meta`
  （`Meta`）和 `$[ident]`（`StructSubFieldIdentifier`）不受影响；裸 `$`
  匹配 `ElementSelf`。
- 新增表达式备选式 `| ElementSelf  # ElementSelf`。
- 将 `ElementSelf` 加入 `Range` / `ReverseRange` 标识符集合，
  使 `lo < $ < hi` 可解析。
- 通过 `internal/parser/planparserv2/generate.sh`（ANTLR 4.13.2）重新生成解析器。

### 4.2 解析器（`internal/parser/planparserv2/parser_visitor.go`）

- `getColumnInfoFromElementSelf()` 为 `$` 构建元素级 `planpb.ColumnInfo`：
  `field_id` = 该标量数组字段，`data_type` = `element_type` = 该数组的元素类型，
  `is_element_level = true`（外加 pk/partition/clustering/nullable 标志，以与结构体子字段路径对称）。
- `VisitElementSelf` 将其包装为列表达式（与 `VisitStructSubField` 类似）。
- `parseMatchExpr` 和 `VisitElementFilter` 解析命名字段：
  - 若它是标量 `DataType_Array` 字段 → 在谓词解析期间设置 `currentElementArrayField`
    （使 `$` 可解析）；
  - 否则 → 走**已有的结构体数组路径**（`currentStructArrayField`），
    字段类型校验延迟到 `$[subField]` 解析时（保留既有行为 —— 不做前置拒绝）。
- 嵌套保护（`MATCH_*` 内不能再嵌 `MATCH_*`）同时覆盖两种上下文。
- 线格式：`planpb.MatchExpr.column`（一个 `MatchColumnInfo`）携带目标字段 id/name、
  `data_type`（ArrayOfStruct / Array / JSON）、元素类型与 JSON `nested_path`；
  标量/结构体/JSON 的分派读 `data_type`。旧的 `struct_name` 字符串保留在 tag 1
  （弃用、双写），供旧 querynode 解析结构体目标、旧 proxy 的 plan 回退使用 ——
  一处 `MatchExpr` 线格式变更，无已发布影响（见 §7）。

### 4.3 Segcore —— 标量数组的元素 offset

`MATCH_*` 执行需要目标字段的 `IArrayOffsets`（行 → 展平后元素区间映射）。
结构体数组字段已具备此机制；本特性为标量 `DataType::ARRAY` 字段也注册它。

- **Sealed**（`ChunkedSegmentSealedImpl`）：在数组 offset 注册路径中，当字段是标量
  `DataType::ARRAY`（无 struct 名）时，构建按 `field_id` 索引的
  `ArrayOffsetsSealed`。`ArrayOffsetsSealed::BuildFromSegment`
  本就通过 `ArrayView` 路径（`array_views[i].length()`）逐 chunk 迭代，
  因此无需改动即可用于标量数组。
- **Growing**（`SegmentGrowingImpl`）：为标量数组字段惰性创建 `ArrayOffsetsGrowing`
  （在 init 时，以及在 `Reopen` 时为 schema 演进新增的字段创建），
  记录于 `struct_representative_fields_`，并在插入时按行喂入元素长度
  （`ExtractArrayLengths` / `…FromFieldData`，并显式处理 null 行 → 长度 0）。

### 4.4 Segcore —— 字段解析

`Schema::ResolveArrayElementField(name)` 解析 `MATCH_*` 目标：

- 若存在名为 `name` 的字段且为 `DataType::ARRAY` / `VECTOR_ARRAY` →
  返回它（标量 / 向量数组）；
- 否则 → `GetFirstArrayFieldInStruct(name)`（结构体数组，既有行为）；
- 若 `name` 存在但是非数组类型 → 抛出清晰的错误。

三个执行点 —— `PhyMatchFilterExpr::Eval`（`MatchExpr.cpp`）、
`PhyElementFilterBitsNode` 和 `PhyIterativeElementFilterNode` —— 从
`GetFirstArrayFieldInStruct` 切换为 `ResolveArrayElementField`。下游一切
（元素 offset 转换、经 `ProcessElementLevelByOffsets` 的子谓词求值、量词计数）保持不变。

### 4.5 元素值读取

子谓词的 `ColumnInfo` 是元素级的（`is_element_level = true`，
`data_type` = 元素类型）。`ProcessElementLevelByOffsets`（已存在）
通过 `IArrayOffsets` 将每个展平的元素 offset → `(row, idx)`，并从
`ArrayView`（sealed）/ `Array`（growing）读取标量值 —— 处理
null 元素、VarChar 和空数组。无需新增访问器。

### 4.6 索引

由于子谓词是普通的 `SegmentExpr`，当数组字段上存在嵌套标量索引（Tantivy 倒排 /
`ScalarIndexSort`）且代价启发式选中它时，子谓词会透明地使用该索引 —— 与结构体数组
所用的 `CanUseNestedIndex()` 路径相同。数组标量索引现按 nested（元素级）构建 ——
索引层改动见 §6.2；标量数组字段上的倒排索引即可加速 `MATCH_*` 谓词。

**HYBRID/AUTOINDEX 替换（有意为之）**：`CreateNestedIndex` 只特判 INVERTED 与
BITMAP；其余请求类型 —— 包括数值/varchar 数组 AUTOINDEX 的默认落点 HYBRID ——
一律落入 nested 排序族（`ScalarIndexSort`/`StringIndexSort`）。构建与加载都按
持久化标记走同一路由，内部自洽；`DESCRIBE INDEX` 仍报告请求类型
（HYBRID/AUTOINDEX），物理结构则是 nested 排序索引（§6 的引擎评测显示排序索引
本就赢下标量查询）。HYBRID 的 `bitmap_cardinality_limit` 参数被接受但对
plain array 物理无效。

---

## 5. 测试

### 5.1 解析器（`plan_parser_v2_test.go`）

- `TestScalarArrayMatchAny`：断言生成的 plan（`MatchExpr.struct_name`、
  `match_type`，以及谓词的元素级 `ColumnInfo` 带有数组的元素 `data_type`）。
- `TestScalarArrayMatchVariants`：全部五个 `MATCH_*` + `element_filter`、复合谓词、
  range 形式、Int64 与 VarChar 元素数组，以及负面用例
  （标量数组上用 `$[sub]`；match 之外裸用 `$`；非数组字段；嵌套）。

### 5.2 Segcore

- `test_chunk_segment.ScalarArrayOffsetsBuiltForArrayField`：标量数组
  `IArrayOffsets` 构建（行数、元素总数、每行区间）。
- `ScalarArrayMatchExprTest`（在 **sealed** 与 **growing** 上参数化），
  Int64 与 VarChar 元素数组，覆盖：
  - 全部五个算子（`MATCH_ANY/ALL/LEAST/MOST/EXACT`）；
  - 复合元素谓词（`$ > 60 && $ < 90`、`$ >= 40 && $ <= 100`、
    `$ == "x" || $ == "y"`）和 range 形式（`60 < $ < 90`）；
  - **空数组空真** 边界（空行被 `MATCH_ALL` 命中，被 `MATCH_ANY` 排除）。
  - 每个用例都通过全量召回 Retrieve 断言**精确的**命中行集合
    （既无假阳性**也无**假阴性）。
- 既有的结构体数组 `MatchExprTest` 行为不变；其 growing segment 测试额外增强了全量召回验证。

### 5.3 覆盖率汇总

| 维度 | 覆盖情况 |
|-----------|---------|
| 算子 | ANY、ALL、LEAST、MOST、EXACT（+ element_filter 解析） |
| 元素类型 | Int64、VarChar（解析器另外验证元素类型接线） |
| Segment 类型 | sealed、growing |
| 谓词形式 | 简单比较、复合 `&&`/`||`、三元 range |
| 边界用例 | 空数组（空真）、NULL 行（三值语义：结果为 UNKNOWN/排除，而非空真命中；`[]` 是真空数组、按空真规则保留）、threshold 边界（0、N） |
| 负面用例 | 标量数组上 `$[sub]`、裸 `$`、非数组字段、嵌套 |
| 索引 | 数组标量索引按 nested（元素级）构建（见 §6.2；`internal/core/src/index/` 约 220 行改动：`BitmapIndex`/`ScalarIndexSort`/`StringIndexSort`/`InvertedIndexTantivy` 的 nested 构建路径 + `is_nested` 持久化）；旧的行级索引照常加载并回退到暴力求值 |

---

## 6. JSON 数组与 nested 索引扩展

标量数组设计（§1–5）沿两个维度自然推广，且除目标定位符外不引入新语法：**JSON 数组**
作为第四种 `MATCH_*` 目标（本 PR 中始终暴力求值），以及以 **nested 索引**作为标量数组
的元素载体（JSON 元素级索引是后续步骤，见 §6.2）。

### 6.1 JSON 数组目标

`MATCH_*` 接受由 JSON path 定位的 JSON 数组目标，元素自引用 token `$` 语义一致：

```text
MATCH_ANY(meta["scores"], $ > 90)
MATCH_ALL(meta["tags"],   $ == "vip")
MATCH_LEAST(meta["k"],    $ == 100, threshold=2)
```

- **解析**（`parser_visitor.go`）：`JSONIdentifier` 目标解析为 JSON 字段 + path；
  `MatchColumnInfo` 携带 `(field_id, nested_path)` 而非结构体子字段定位符。
  `element_filter` 内的模板占位符按与标量数组相同的方式填充。
- **元素读取**（segcore）：用 simdjson 打开每行在该 path 上的 JSON 值并按数组迭代，
  每个元素以 `$` 绑定后送入同一 `element_filter` 子表达式求值。
- **缺失/非数组 path 按三值语义处理**（与 §5.3 的 NULL 行条目同一规则）：某行的
  JSON path 未解析出真正的数组时 —— 字段为 NULL、JSON `null`、key 缺失或值不是
  数组 —— 该行为 **UNKNOWN**，而非 false。`MaskJsonNonArrayRows` 同时清掉该行的
  match 位**和** valid 位，因此结果中被排除 —— 在 `NOT` 下同样如此
  （`not MATCH_ANY(...)` 不会把这些行捞回来）。真正的空数组 `[]` 则不同：它是
  *有效的*零元素输入，按空真规则求值（`MATCH_ALL`/`MATCH_MOST` 命中，
  `MATCH_ANY`/`MATCH_LEAST` 不命中）。
- **数值精度**：元素读取走 `at_numeric()` 对齐的类型判定 —— 整型元素**精确按 int64**
  比较（无 2^53 double 截断），uint64/double 按 double。这与行级 `*JSONCompare` 路径
  一致，使 `meta["id"] == 9007199254740993` 在行级与元素级求值结果相同（本 PR 修复，
  由 `JsonNumericTest` 守护）。

### 6.2 nested 索引作为元素载体

标量数组可构建 **nested** 索引：每个数组元素一个索引文档，元素所属 row-id 作为
user doc-id，因此元素级 term/range 谓词通过遍历命中的 element-id、经 `IArrayOffsets`
将每个元素映射回所属行来得到行位图（逐元素循环；按字的位图归约是可能的后续优化，
不在本分支内）。

- **范围 —— 本 PR 仅限标量数组**：nested（元素级）构建仅对普通标量 `ARRAY` 字段
  生效，为 `BitmapIndex`、`ScalarIndexSort` / `StringIndexSort` 与
  `InvertedIndexTantivy` 新增了 nested 构建路径及 `is_nested` 持久化标志。JSON
  `MATCH_*` 始终暴力求值：`MatchExpr.cpp` 将 JSON 目标硬分派到逐行路径，
  `IndexFactory` 仍将 JSON 字段路由到 `CreateJsonIndex`（行级）。JSON 元素级
  索引加速是明确的后续步骤。
- **构建**：标量数组索引按 nested 构建；修复了 nested 构建下 nullable 紧凑缓冲的
  读取。行级数组查询（`array_contains`、`array_length`）在 nested 索引上照常
  工作 —— 表达式框架经 `IArrayOffsets` 把元素级结果转换回行级。
- **版本门控（滚动升级安全）**：普通 `ARRAY` 标量索引**仅当协商出的 scalar index
  engine version >= 5 时**才按 nested 构建并写入**每段（per-segment）持久化的 is_nested_index 标记**。plain array 天然存在两种索引形态（老的行级、新的元素级），且可能出现在同一版本号下，因此加载路由**只看这个自描述标记、从不解读版本号**：标记缺失或为 false（任何版本下构建的存量索引）一律走原 composite 加载器，行级查询照旧，MATCH_* 对其回退爆搜；struct 子字段索引天生 nested、无需区分。版本号仅在 datacoord 为**新**构建任务做集群能力判断时使用一次（协商取全部 querynode 的最小值，滚动升级中老节点绝不会拿到元素级索引）。已知限制：snapshot/restore 的 Avro 清单暂未携带该标记（follow-up）。
  构建期版本取所有 querynode 的*最小值*，因此滚动升级期间任何旧 querynode 都
  不可能被分到元素级索引（否则会把元素位图误读成行位图）。加载时由**构建期
  持久化的版本**决定路由：旧的行级数组索引 —— 包括只有 `HybridScalarIndex`
  能打开其文件布局的 HYBRID/AUTOINDEX —— 确定性地回到构建时所用的 composite
  加载路径，继续服务行级查询；在这类索引上 `MATCH_*` 回退到暴力求值。不强制
  重建。
- **空输入边界**：全 NULL 或全空数组的字段会构建出有效的**空** nested 索引，
  而不是报 `DataIsEmpty`；V3 打包布局中，零字节的 `idx_to_offsets` 条目跳过
  mmap（`mmap(len=0)` 会返回 EINVAL）。
- **AUTOINDEX**：数值数组解析为 HYBRID 并回落到 `ScalarIndexSort`（现同样按
  nested 构建），故排序索引仍是数组默认；仅当字段显式 `INVERTED` 时才用 nested
  tantivy。（nested 元素索引的排序 vs 倒排取舍见 #51055；构建期单段优化见
  #51054。）
- **兼容性**：nested 索引是构建期选择的新物理布局，查询解析读取段实际携带的布局，
  因此升级期间行级/nested 段可共存。

## 7. 兼容性与风险

- **线格式变更（保持滚动升级兼容）**：`MatchExpr` 增加更丰富的 `MatchColumnInfo`
  定位符 —— JSON 数组目标（`MATCH_ANY(meta["scores"], $ > 90)`）必须携带
  `nested_path`，纯字段名装不下。定位符以**新字段**加入
  （`MatchColumnInfo column = 5`），而 `string struct_name = 1` 保留在原 tag 上并标
  `[deprecated]`：`struct_name = 1` 已随结构体数组 MATCH（#46518）进入 master，
  若在同一 tag 上换类型，string 与 message 同为 wire type 2，混部的新旧
  proxy/querynode 之间会**静默错解**（解码器不报错，直接解出垃圾）。proxy
  **双写**两个定位符；C++ 读侧优先 `column`，缺失时回退 `struct_name`（旧 proxy
  只可能发结构体数组目标）。client SDK 不受影响（SDK 发的是表达式字符串，
  不是 plan proto）。无 RPC/SDK 改动。
- **滚动升级**：`MATCH_*` 标量数组/JSON 语法只被升级后的 QueryNode 理解。新 proxy
  把这类 plan 发给未升级的 querynode 会在执行期干净地失败（旧节点按双写的
  `struct_name` 解析，报 "No array field found in struct"）—— 这是完成 querynode
  升级即关闭的过渡窗口。nested 索引的*构建*另由协商的标量索引引擎版本 +
  per-segment 标记控制（见 §6.2）。内部 `nested_index` 构建/加载配置键已列为
  保留字（CreateIndex 拒绝用户设置），版本闸门无法被绕过。
- **降级 / 混合池调度**：构建闸门只保护**新构建**。已建成的 nested 数组索引不可
  由 pre-v5 querynode 加载（HYBRID/AUTOINDEX 因缺 INDEX_TYPE 条目加载即失败；
  INVERTED 会把元素序号当行偏移静默出错）。已加防护的入口：snapshot restore
  在解析出的池版本低于闸门时拒绝携带 `is_nested_index=true` 段索引的快照
  （对照 JSON path 索引的版本检查）。残余窗口 —— nested 索引已存在后把
  querynode 回滚到 v5 以下、或用旧镜像扩容 —— 无法靠加载期检查防护（旧二进制
  早于标记的存在）；支持的操作流程是**先 drop/重建 plain-ARRAY 索引再降级
  querynode**。标记的语义刻意定义为"加载此索引需要 nested 能力（>= v5）的代码"，
  而非"物理为元素级"：struct 子字段索引同样是元素级，但所有已发布二进制都按
  **名字**路由 nested 加载，因此它们保持 marker=false、不受上述防护影响。
- **结构体数组**：未触及 —— 相同的解析器路径（默认分支）与相同的执行；由既有的
  `MatchExpr` 测试套件做回归保护。
- **Schema 演进**：通过 `AlterCollectionSchema` 新增的标量数组字段会在 growing segment
  `Reopen` 时注册其 `IArrayOffsets`。
- **构建说明（与本特性无关）**：原生 macOS/clang-19 构建需要上游
  `rapidjson/cci.20230929` 的 pin（PR #50664）；本特性自身不带任何构建系统改动。
