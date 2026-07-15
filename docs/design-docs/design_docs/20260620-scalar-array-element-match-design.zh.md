# 设计文档：标量与结构体数组元素匹配（`MATCH_*` / `element_filter`）

**分支**：`feat/array-element-match`
**日期**：2026 年 6–7 月
**范围**：表达式文法 + planparserv2（Go）、segcore（C++），仅暴力（brute-force）执行。
向 `plan.proto` 以新 tag 增加 `MatchColumnInfo` 定位符；旧 `MatchExpr.struct_name`
保留在 tag 1 并标 `[deprecated]`，proxy 双写、querynode 回退，保证滚动升级线兼容（见 §7）。
为普通标量 ARRAY 字段引入的**嵌套（元素级）索引布局**（索引构建/加载流水线及其
每 segment 标记）在后续 PR 中落地（§5）。无 RPC/SDK 改动。

将量化元素匹配能力（`MATCH_ANY/ALL/LEAST/MOST/EXACT` + `element_filter`）在两类
物理数组形态容器 —— 标量数组与结构体数组 —— 上统一起来。第 1–4 节描述查询侧设计；
第 5 节是后续嵌套数组索引 PR 的占位；第 6 节测试；第 7 节兼容性；
第 8 节性能权衡。

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
5. **与数组索引正确共存** —— 本 PR 中元素谓词一律对原始数据做暴力求值。
   钉在字段上的**旧式行级**数组索引绝不能用来回答元素级谓词：执行路径守卫
   检测到这种不匹配后回退到暴力求值（§4.6）。让普通标量 ARRAY 索引以嵌套
   （元素级）布局构建 —— 使数组字段上的索引能通过结构体子字段索引已有的同一
   元素级查找加速元素谓词 —— 在后续 PR 中落地（§5）。
6. **无回归** —— 结构体数组 `MATCH_*` 行为与 `array_contains*` /
   `array_length` 不变（除 §2.3 所述一处有意的 NULL 语义修正之外）。`plan.proto`
   以新 tag 新增 `MatchColumnInfo` 定位符（`struct_name` 保留在 tag 1 弃用，
   线兼容见 §7）；无 RPC/SDK 改动。

### 1.3 设计原则

**把标量数组视为只有一个隐式子字段（即元素值本身）的结构体数组。**
在这一视角下，`$` 就是 `$[subField]` 的标量类比，整条已有的结构体数组执行流水线
（行↔元素 offset 映射、元素谓词求值、量词计数、嵌套索引查找）都可以原样复用。
因此查询侧是**增量式管道接线** —— 一个新 token、一处元素自引用列解析，
以及为标量数组字段注册元素 offset —— 而非一套新的执行引擎。
**索引构建侧在本 PR 中不作任何改动**：普通标量 ARRAY 索引切换为嵌套元素级布局
（由显式的每 segment 标记守护，使旧布局与嵌套布局能安全共存）在后续 PR 中落地（§5）。

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
            planpb.MatchExpr { column = MatchColumnInfo{field_id, field_name},
                               predicate, match_type, count }
                 ▼  (CGO / proto)
            segcore (C++)
                 │  PhyMatchFilterExpr::Eval
                 │   ├─ Schema::ResolveArrayElementField(field_name)
                 │   │     标量 ARRAY ─► 该字段；struct ─► 第一个数组子字段
                 │   ├─ segment->GetArrayOffsets(field_id)   # IArrayOffsets
                 │   ├─ 行 → 展平后的元素 offset
                 │   ├─ 子谓词 Eval（对 $ 做元素级读取，
                 │   │   或在已加载嵌套索引时走索引查找）
                 │   └─ 按行做量词判定：将匹配数与 MatchType/threshold 比较
                 ▼
            行级 bitset（NULL 行被掩掉，§3.2/§4.3）
```

### 2.2 各层职责

| 层 | 职责 | 改动 |
|-------|----------------|--------|
| 文法（`Plan.g4`） | 将 `$` 词法识别为 `ElementSelf`；接受其作为表达式及出现在 range 形式中；`threshold` 改为软关键字 | 新增 token + 备选式，删除 `THRESHOLD` token |
| 解析器（`parser_visitor.go`） | 将 `$` 解析为元素级 `ColumnInfo`；将 `MATCH_*`/`element_filter` 路由到标量数组或结构体数组路径；校验谓词形态 | 新增解析 + 分支 + `validateMatchPredicateShape` |
| Plan proto | 定位数组目标 | **新增** `MatchColumnInfo` 定位符（tag 5）；`struct_name` 保留在 tag 1 弃用并双写（线兼容，§7） |
| Segcore —— offsets | 为标量 `ARRAY` 字段构建/维护 `IArrayOffsets`（sealed + growing），含逐行有效性 | 扩展注册逻辑 + 行有效位图 |
| Segcore —— 解析 | 解析 `MATCH_*` 目标，可为标量数组或结构体数组 | `Schema::ResolveArrayElementField` |
| Segcore —— 执行 | 读取元素值、求值谓词、做量词判定 | **复用**已有的 `MatchExpr.cpp` / `ProcessElementLevelByOffsets` |

索引构建/加载流水线（datacoord 构建决策、index worker、索引元数据/snapshot、
`IndexFactory` 加载路由）在本 PR 中不变 —— 移入嵌套数组索引后续 PR（§5）。

### 2.3 本特性**不**改动的内容

- 结构体数组 `MATCH_*` / `element_filter` 的量词语义与代码路径 ——
  但有**一处有意的三值逻辑修正**：早于字段存在的行（schema 演进回填）过去表现为
  全零 offset，即空数组；现在被记录为 **NULL 行**（全 NULL offset），因此
  `MATCH_ALL` 不再空真命中它们，`not array_contains(...)` 也不再包含它们
  （§4.3，由 `MatchTreatsBackfilledRowsAsNull` 钉住）。
- `array_contains` / `array_contains_all` / `array_contains_any` /
  `array_length` 的语义。
- RPC 或 SDK 线格式 —— client 发的是表达式字符串，不是 plan proto；唯一的内部
  proto 变更是 `plan.proto` 的 `MatchExpr` 定位符（不面向 client）。
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

**NULL 与空数组的区别**：NULL 行（数组字段本身为 NULL —— 区别于真实的空数组
`[]`）在**全部五个**算子下均为 UNKNOWN，因而被排除，遵循 SQL 三值逻辑。
特别地，`MATCH_ALL` 把 `[]` 视为空真命中，而把 NULL 行视为不命中。执行层通过在
结果位图上掩掉 NULL 行来强制这一点（`MatchExpr.cpp` 的 `MaskNullRows`，其底层是
`IArrayOffsets` 跟踪的逐行有效性，§4.3）。

`pred` 是任意对 `$` 的元素级谓词（比较、range、`&&`/`||`、
针对 VarChar 元素的 `like`/正则），与结构体数组谓词对 `$[subField]` 所允许的形式一致。
谓词必须真正引用元素本身 —— 见 §4.2 的形态校验。

`element_filter(arr, pred)` 产生**元素级** bitset，专用于元素级向量检索；
它**不是**独立的行级过滤器（行级标量数组过滤使用 `MATCH_*`）。这与已有的结构体数组约定一致。

**容器间差异（有意为之，已用测试钉住）**：

- `element_filter` 只接受物理数组列（标量/结构体数组）—— 它依赖
  `IArrayOffsets`。
- 结构体子字段上的 `array_contains_all` 在解析期脱糖为
  `MATCH_ANY(...) && MATCH_ANY(...)` 合取，因此不能接受模板占位符列表
  （模板值在解析之后才填充，而结构体路径必须在解析期展开列表）。空列表在两种容器上
  一律是 **IS NOT NULL 语义**：对任何真实数组（含空 `[]`）空真；对 NULL 行
  UNKNOWN → 排除（pg：`@>` 是 strict 运算，`NULL @> '{}'` 得 NULL 而非 true）。
  结构体脱糖产出与 `struct_array is not null` 相同的 `NullExpr IsNotNull` plan；
  标量的运行时空列表路径逐行求值而非笼统返回 true。`contains_any(x, [])`
  对所有行保持确定 FALSE（零次比较，pg：`NULL = ANY('{}')` = false）。

---

## 4. 组件设计

### 4.1 文法（`internal/parser/planparserv2/Plan.g4`）

- 新增词法 token `ElementSelf: '$';`。ANTLR 的最长匹配规则保证 `$meta`
  （`Meta`）和 `$[ident]`（`StructSubFieldIdentifier`）不受影响；裸 `$`
  匹配 `ElementSelf`。
- 新增表达式备选式 `| ElementSelf  # ElementSelf`。
- 将 `ElementSelf` 加入 `Range` / `ReverseRange` 标识符集合，
  使 `lo < $ < hi` 可解析。
- **删除**专用的 `THRESHOLD` 词法 token；`threshold` 改为**软关键字**。
  文法接受 `kw=Identifier ASSIGN IntegerConstant`，由 visitor 对标识符文本做
  大小写不敏感校验（`VisitMatchThreshold`：*「expected 'threshold', got '%s'」*）。
  因此名字恰好叫 `threshold` 的集合字段在表达式其他位置仍可作为普通标识符使用
  （旧 token 会遮蔽它）。
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
- **谓词形态校验**（`validateMatchPredicateShape`）对 `MATCH_*` 与
  `element_filter` **同时生效**。segcore 用**元素** offset 求值谓词
  （`PhyMatchFilterExpr::EvalWithOffsets`），谓词中若出现行级列引用，
  会被按元素 offset 索引 —— 产生垃圾结果或越界读。因此校验器在解析期拒绝：
  (a) 任何非元素级的列引用（即不是 MATCH 目标的 `$` / `$[subField]`）——
  *「predicate can only reference array elements ($ or $[subField]);
  row-level field references are not supported」*；
  (b) 常量谓词（`AlwaysTrueExpr`，或完全不引用元素）——
  *「predicate must not be constant; it has to test the array element via $ or
  $[subField]」*。
- 线格式：`planpb.MatchExpr.column`（一个 `MatchColumnInfo`）携带目标字段 id 与
  name。segcore **只按 name 分发** —— `Schema::ResolveArrayElementField(field_name)`，
  标量数组与结构体数组皆然；定位符里的 `field_id` 仅用于日志/诊断。
  旧的 `struct_name` 字符串保留在 tag 1（弃用、双写），供旧 querynode 解析结构体目标、
  旧 proxy 的 plan 回退使用 —— 见 §7。

### 4.3 Segcore —— 标量数组的元素 offset

`MATCH_*` 执行需要目标字段的 `IArrayOffsets`（行 → 展平后元素区间映射）。
结构体数组字段已具备此机制；本特性为标量 `DataType::ARRAY` 字段也注册它，
并给该抽象扩展了**逐行有效性**，使 NULL 行与空数组保持区分（§3.2）。

- **Sealed**（`ChunkedSegmentSealedImpl`）：在数组 offset 注册路径中，当字段是标量
  `DataType::ARRAY`（无 struct 名）时，构建按 `field_id` 索引的
  `ArrayOffsetsSealed`。`ArrayOffsetsSealed::BuildFromSegment`
  仍通过 `ArrayView` 路径（`array_views[i].length()`）逐 chunk 迭代，
  但**现在额外跟踪行有效性**（由字段 valid data 填充的 `row_valid_` 位图）——
  它不再是「无需改动即可用」。schema 演进回填的行（写入该行时字段尚不存在）
  改用 `ArrayOffsetsSealed::BuildAllNulls(row_count)` —— 全 NULL offset ——
  而非过去的全零形态，避免嵌套索引消费方把历史 NULL 误当作 `[]`。
- **Growing**（`SegmentGrowingImpl`）：为标量数组字段惰性创建 `ArrayOffsetsGrowing`
  （在 init 时，以及在 `Reopen` 时为 schema 演进新增的字段创建），
  记录于 `struct_representative_fields_`，并在插入时按行喂入元素长度
  （`ExtractArrayLengths` / `…FromFieldData`）。NULL 行以**长度 `-1` 哨兵值**
  喂入（`SegmentGrowingImpl` 对 nullable 字段强制此约定）并记为无效
  （`ArrayOffsetsGrowing::Insert`：`valid = array_len >= 0`）；空数组长度为 0、
  仍然有效。schema 演进回填的行经 `ArrayOffsetsGrowing::InsertNulls` 记录。
- **行有效性 API**：有效性通过线程安全的
  `IArrayOffsets::AndRowValidBitmap(view, start, count)` 暴露 —— 它在调用方
  持有的位图上按位与掉无效行（对 growing segment 持共享锁），**取代了**先前
  返回裸指针的 `GetRowValidBitmap` 访问器（后者在并发增长下不安全）。
  `MATCH_*` 借此（经 `MatchExpr.cpp` 的 `MaskNullRows`）把 NULL 行从所有量词
  结果中排除。

### 4.4 Segcore —— 字段解析

`Schema::ResolveArrayElementField(name)` **按 name** 解析
`MATCH_*` / `element_filter` 目标：

- 若存在名为 `name` 的字段且为 `DataType::ARRAY` → 返回它（标量数组）；
- 若存在名为 `name` 的字段但为其他任何类型 —— **包括顶层 `VECTOR_ARRAY`** ——
  → `ThrowInfo(ErrorCode::ExprInvalid, "field '{}' (data type {}) does not
  support MATCH_*/element_filter; expected a scalar array or struct array
  field")`。元素级向量检索不经过此解析，因此向量数组字段在这里被拒绝而非接受；
- 否则 → `GetFirstArrayFieldInStruct(name)`（结构体数组，既有行为）。

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

### 4.6 元素谓词的索引使用

由于子谓词是普通的 `SegmentExpr`，它会经由结构体数组所用的同一
`CanUseNestedIndex()` 路径，透明地使用数组字段上 pin 住的标量索引 ——
**前提是该索引是嵌套（元素级）的**。让普通 ARRAY 索引在每 segment 标记
的守护下以嵌套布局构建（含完整的构建→加载链路）在后续 PR 中落地（§5）；
因此在本 PR 中，标量数组上的元素谓词实际总是走暴力求值。

当加载的是**旧式行级**数组索引时，元素级谓词无法使用它：
其倒排表把值映射到*行* id，而元素谓词消费的是*元素* offset。
`UnaryExpr.cpp`、`TermExpr.cpp`、`BinaryRangeExpr.cpp` 与
`BinaryArithOpEvalRangeExpr.h` 中的执行路径守卫会识别这种情况
（`element_level_ && exec_path_ == ScalarIndex && !PinnedIndexIsNested()`）
并回退到暴力求值（`ExprExecPath::RawData`），保证现有行级数组索引继续给出
正确结果。镜像方向的守卫 —— 行级全数组算子落在*嵌套*索引上 ——
随嵌套索引后续 PR 一起交付。

目前仍无量词或选择率感知的路径选择（代价启发式留待后续）。

---

## 5. 嵌套数组索引与每 segment 标记（后续 PR）

面向标量数组元素谓词的嵌套索引加速 —— 普通标量 ARRAY 索引的嵌套（元素级）
构建布局、每 segment 嵌套索引标记及其构建→加载链路、以及由嵌套索引精确回答的
全数组相等 —— 在后续 PR 中落地，届时补全本节。

---

## 6. 测试

### 6.1 解析器（`plan_parser_v2_test.go`）

- `TestScalarArrayMatchAny`：断言生成的 plan（`MatchExpr.column` 的
  `MatchColumnInfo.field_name`、`match_type`，以及谓词的元素级 `ColumnInfo`
  带有数组的元素 `data_type`）。
- `TestScalarArrayMatchVariants`：全部五个 `MATCH_*` + `element_filter`、复合谓词、
  range 形式、Int64 与 VarChar 元素数组，以及负面用例
  （标量数组上用 `$[sub]`；match 之外裸用 `$`；非数组字段；嵌套）。
- `TestMatchPredicateShapeRejected`：`MATCH_*`/`element_filter` 谓词中的行级
  列引用与常量谓词被拒绝（§4.2）。
- `TestExpr_ThresholdSoftKeyword`：`threshold` 既可作 match 参数，也可作普通
  字段名（§4.1）。
- `TestMatchExprStructNameDualWrite` / `TestMatchColumnInfoArrayOnlyShape`：
  定位符双写与形态（§7.1）。
- `TestMatchTemplatePlaceholders` / `TestElementFilterTemplatePlaceholders` /
  `TestElementFilterRejectsJSON` / `TestExpr_StructArrayContainsDesugar`：
  模板值与 §3.2 的容器差异。

### 6.2 Segcore

- `test_chunk_segment.ScalarArrayOffsetsBuiltForArrayField`：标量数组
  `IArrayOffsets` 构建（行数、元素总数、每行区间）。
- `ScalarArrayMatchExprTest`（在 **sealed** 与 **growing** 上参数化），
  Int64 / VarChar / Bool / Float / Double 元素数组，覆盖：
  - 全部五个算子（`MATCH_ANY/ALL/LEAST/MOST/EXACT`）；
  - 复合元素谓词（`$ > 60 && $ < 90`、`$ >= 40 && $ <= 100`、
    `$ == "x" || $ == "y"`）、range 形式（`60 < $ < 90`）、`like` 模式、
    `in` / `not in` 元素谓词；
  - **空数组空真**边界（空行被 `MATCH_ALL` 命中，被 `MATCH_ANY` 排除）以及
    **NULL 行三值语义**用例（`NullableArrayScalarStructAligned`、
    `NullableNotMatchThreeValued`、`AllNullRowsMatchNothing`）。
  - 每个用例都通过全量召回 Retrieve 断言**精确的**命中行集合
    （既无假阳性**也无**假阴性）。
- **NULL 与空数组的回填区分**：`MatchTreatsBackfilledRowsAsNull`
  （`MatchExprTest.cpp`）钉住 §2.3 —— schema 演进后，回填行被 `MATCH_ALL`
  和 `not array_contains(...)` 排除；
  `ScalarArrayMatchNullableIngest.BinlogLoadAndInsertAgree` 钉住 binlog 加载
  与在线插入对 NULL 行记账的一致性。
- **旧式索引回退**：
  `ScalarArrayMatchIndexConsistency.NonNestedIndexFallsBackToBruteForce`
  钉住 §4.6 的执行路径守卫 —— 旧式行级数组索引必须给出与暴力求值完全相同的
  行集合。嵌套索引构建/加载/一致性套件（`ScalarArrayMatchIndex*`）随嵌套
  索引后续 PR 一起交付。
- 既有的结构体数组 `MatchExprTest` 行为不变；其 growing segment 测试额外增强了全量召回验证。

### 6.3 端到端

标记链路的 Go 测试（datacoord 构建决策、元数据往返、snapshot 恢复守卫、
保留参数拒绝）随嵌套索引后续 PR 一起交付。

- 集成测试：`tests/integration/scalararraymatch`（`TestScalarArrayMatch`）——
  标量数组 MATCH 的端到端验证。
- Python client：`test_milvus_client_struct_array_match.py`（结构体数组上的
  MATCH 家族，含 nullable 行、空数组、`not MATCH_*`、脱糖）与更新后的
  `test_milvus_client_struct_array_nullable.py`（NULL 行被所有 MATCH 算子排除；
  真实 `[]` 仍按空真处理）。

### 6.4 覆盖率汇总

| 维度 | 覆盖情况 |
|-----------|---------|
| 算子 | ANY、ALL、LEAST、MOST、EXACT（+ element_filter 解析） |
| 元素类型 | Int64、VarChar（segcore 另覆盖 Bool/Float/Double；解析器另外验证元素类型接线） |
| Segment 类型 | sealed、growing |
| 谓词形式 | 简单比较、复合 `&&`/`||`、三元 range、`like`、`in`/`not in` |
| 边界用例 | 空数组（空真）、NULL 行（三值语义：结果为 UNKNOWN/排除，而非空真命中）、回填行 = NULL（而非 `[]`）、threshold 边界（0、N） |
| 负面用例 | 标量数组上 `$[sub]`、裸 `$`、非数组字段、嵌套、行级/常量谓词 |
| 索引 | 旧式（非嵌套）索引 → 回退暴力求值，且结果与暴力求值断言一致；嵌套索引加速的覆盖随后续 PR 交付 |

全部测试均为**正确性**测试；本 PR 不含性能基准（§8）。

---

## 7. 兼容性与风险

### 7.1 Plan 线格式

- **线格式变更（无已发布影响）**：`MatchExpr` 增加携带目标字段 ID 与名称的
  `MatchColumnInfo` 定位符。
  定位符以**新字段**加入（`MatchColumnInfo column = 5`），而 `string struct_name = 1`
  保留在原 tag 上并标 `[deprecated]`：`struct_name = 1` 已随结构体数组 MATCH（#46518）
  进入 master，若在同一 tag 上换类型，string 与 message 同为 wire type 2，混部的新旧
  proxy/querynode 之间会**静默错解**（解码器不报错，直接解出垃圾）。proxy
  **双写**两个定位符；C++ 读侧（`PlanProto.cpp`）优先 `column`，缺失时回退
  `struct_name`（旧 proxy 只可能发结构体数组目标）。client SDK 不受影响
  （SDK 发的是表达式字符串，不是 plan proto）。无 RPC/SDK 改动。
- **滚动升级（语法）**：`MATCH_*` 标量数组语法只被升级后的 QueryNode 理解。新 proxy
  把这类 plan 发给未升级的 querynode 会在执行期干净地失败（旧节点按双写的
  `struct_name` 解析，报 "No array field found in struct"）—— 这是完成 querynode
  升级即关闭的过渡窗口。
- **双写落日条件**：当**所有受支持的滚动升级路径都能保证 querynode 解析
  `MatchColumnInfo`**（即受支持的最老升级起点已包含本特性）之后，再过一个
  minor 版本即可停止 `struct_name` 双写；在此之前 proxy 持续双写两个定位符，
  C++ 回退逻辑保留。

### 7.2 嵌套索引升级/回滚矩阵（后续 PR）

本 PR 不为普通 ARRAY 字段构建任何嵌套索引，因此尚不存在嵌套索引的升级或回滚
面；完整的升级/回滚矩阵随嵌套索引后续 PR 一起交付（§5）。

### 7.3 行为兼容性

- **结构体数组**：相同的解析器路径（默认分支）与相同的执行；由既有的
  `MatchExpr` 测试套件做回归保护。本 PR 附带一处有意的语义修正（§2.3）：
  schema 演进回填的行现在是 NULL 行而非空数组，因此 `MATCH_ALL` / 取反的
  `array_contains` 不再命中它们 —— 这才是符合 SQL 语义的行为。
- **Schema 演进**：通过 `AlterCollectionSchema` 新增的标量数组字段会在 growing segment
  `Reopen` 时注册其 `IArrayOffsets`；回填行记录为 NULL（§4.3）。

---

## 8. 性能权衡

- **仅暴力求值**：本 PR 中标量数组上的元素谓词一律对原始数据暴力求值 ——
  代价与被寻址行的元素总数线性相关。`ProcessElementLevelByOffsets` 的按连续
  元素段（run）解析（每段只做一次行解析）把常数因子压得较低，但目前尚无索引
  加速。
- **旧式索引回退是「正确性优先于速度」**：当字段上 pin 住的是行级数组索引时，
  元素级守卫（§4.6）会释放 pin 并改为扫描原始数据 —— 有意地比（错误的）索引
  答案更慢。嵌套索引加速、其构建成本权衡（每个元素一条索引项）以及与全数组
  算子的交互，移入后续 PR（§5）。
- **本 PR 无基准测试**：所含测试全部是正确性测试；对暴力路径与未来嵌套构建的
  基准测量是后续工作。
