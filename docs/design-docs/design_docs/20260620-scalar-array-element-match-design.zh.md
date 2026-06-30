# 设计文档：标量数组元素匹配（`MATCH_*` / `element_filter`）

**分支**：`feat/scalar-array-match`
**日期**：2026 年 6 月
**范围**：表达式文法 + planparserv2（Go）、segcore（C++）。无 proto/RPC 改动。

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
   `array_length` 不变；无 proto/RPC/SDK 线格式改动。

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
            planpb.MatchExpr { struct_name = arrayField,   # 复用字段
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(struct_name)
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
| Plan proto | 携带数组字段名 | **复用** `MatchExpr.struct_name`（无 proto 改动） |
| Segcore —— offsets | 为标量 `ARRAY` 字段构建/维护 `IArrayOffsets`（sealed + growing） | 扩展注册逻辑 |
| Segcore —— 解析 | 解析 `MATCH_*` 目标，可为标量数组或结构体数组 | `Schema::ResolveArrayElementField` |
| Segcore —— 执行 | 读取元素值、求值谓词、做量词判定 | **复用**已有的 `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |

### 2.3 本特性**不**改动的内容

- 结构体数组 `MATCH_*` / `element_filter` 的语义与代码路径。
- `array_contains` / `array_contains_all` / `array_contains_any` / `array_length`。
- Plan proto、RPC 或 SDK 线格式（`MatchExpr.struct_name` 原样复用）。
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
- 线格式：`planpb.MatchExpr.struct_name` 携带标量数组字段名；标量 vs 结构体的区分
  在执行期重新推导。**无 proto 改动。**

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
所用的 `CanUseNestedIndex()` 路径相同。无索引代码改动；标量数组字段上的倒排索引
即可加速 `MATCH_*` 谓词。

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
| 边界用例 | 空数组（空真）、threshold 边界（0、N） |
| 负面用例 | 标量数组上 `$[sub]`、裸 `$`、非数组字段、嵌套 |
| 索引 | 复用结构体数组嵌套索引路径（无新增代码） |

---

## 6. 兼容性与风险

- **线兼容性**：无 proto/RPC/SDK 改动；`MatchExpr.struct_name` 被复用来携带数组字段名，
  消息格式不变。
- **滚动升级**：*新的标量数组语法*不被旧 QueryNode 理解。旧 QueryNode 通过
  `GetFirstArrayFieldInStruct`（仅结构体数组）解析 `MatchExpr.struct_name`，
  因此 `MATCH_ANY(scores, $ > 90)` 这样的标量数组名在旧节点上无法解析。
  格式是线兼容的，但语义不兼容 —— 新语法必须在**所有** QueryNode 升级完成后才能使用。
  结构体数组表达式不受影响。未加版本门控：这是用户在升级后主动选用的新查询语法。
- **结构体数组**：未触及 —— 相同的解析器路径（默认分支）与相同的执行；由既有的
  `MatchExpr` 测试套件做回归保护。
- **Schema 演进**：通过 `AlterCollectionSchema` 新增的标量数组字段会在 growing segment
  `Reopen` 时注册其 `IArrayOffsets`。
- **构建说明（与本特性无关）**：原生 macOS/clang-19 构建需要上游
  `rapidjson/cci.20230929` 的 pin（PR #50664）；本特性自身不带任何构建系统改动。
