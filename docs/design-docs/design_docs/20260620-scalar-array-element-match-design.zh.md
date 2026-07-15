# 设计文档：标量与结构体数组元素匹配（`MATCH_*` / `element_filter`）

**分支**：`feat/array-element-match`
**日期**：2026 年 6–7 月
**范围**：表达式文法 + planparserv2（Go）、segcore（C++），以及标量索引构建/加载流水线
（datacoord、index worker、索引元数据、snapshot manifest、C++ `IndexFactory`）。
向 `plan.proto` 以新 tag 增加 `MatchColumnInfo` 定位符；旧 `MatchExpr.struct_name`
保留在 tag 1 并标 `[deprecated]`，proxy 双写、querynode 回退，保证滚动升级线兼容（见 §7）。
为普通标量 ARRAY 字段引入**嵌套（元素级）索引布局**，由每 segment 的
`is_nested_index` 标记控制（见 §5）。无 RPC/SDK 改动。

将量化元素匹配能力（`MATCH_ANY/ALL/LEAST/MOST/EXACT` + `element_filter`）在两类
物理数组形态容器 —— 标量数组与结构体数组 —— 上统一起来。第 1–4 节描述查询侧设计；
第 5 节描述嵌套数组索引及其每 segment 标记；第 6 节测试；第 7 节兼容性与升级/回滚；
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
5. **索引加速** —— 当集群解析出的标量索引引擎版本 ≥ 5 时，普通标量 ARRAY 字段的
   标量索引（BITMAP / INVERTED / sort 族，含 HYBRID 的内部解析）**以嵌套（元素级）
   布局构建**，使数组字段上的索引能通过结构体子字段索引已有的同一元素级查找加速
   元素谓词（§5）。当加载的是旧式行级数组索引时，`MATCH_*` 回退到暴力求值（§4.6）。
6. **无回归** —— 结构体数组 `MATCH_*` 行为与 `array_contains*` /
   `array_length` 不变（除 §2.3 所述一处有意的 NULL 语义修正之外）。`plan.proto`
   以新 tag 新增 `MatchColumnInfo` 定位符（`struct_name` 保留在 tag 1 弃用，
   线兼容见 §7）；无 RPC/SDK 改动。

### 1.3 设计原则

**把标量数组视为只有一个隐式子字段（即元素值本身）的结构体数组。**
在这一视角下，`$` 就是 `$[subField]` 的标量类比，整条已有的结构体数组执行流水线
（行↔元素 offset 映射、元素谓词求值、量词计数、嵌套索引查找）都可以原样复用。
因此查询侧是**增量式管道接线** —— 一个新 token、一处元素自引用列解析，
以及为标量数组字段注册元素 offset —— 而非一套新的执行引擎。与之相对，
**索引构建侧确实发生了变化**：普通标量 ARRAY 索引切换为嵌套元素级布局，
并由显式的每 segment 标记守护，使旧布局与嵌套布局能安全共存（§5）。

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
                 │   │   或在已加载嵌套索引时走索引查找，§5）
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
| Datacoord —— 构建决策 | 为普通 ARRAY 索引构建决定每 segment 的嵌套标记 | `prepareJobRequest` → `typeutil.IsNestedArrayIndex`（§5.3） |
| Index worker（datanode） | 按 clamp 后版本重新推导标记，构建嵌套或旧式索引并回传标记 | `normalizeNestedIndexMarker`（§5.3） |
| 索引元数据 / snapshot | 持久化、分发与恢复标记 | `SegmentIndex.IsNestedIndex`、`querypb.FieldIndexInfo`、Avro manifest v5（§5.3–5.4） |
| Segcore —— 索引加载 | **只按标记**路由嵌套或旧式 loader | `IndexFactory::CreateScalarIndex`（§5.3） |

### 2.3 本特性**不**改动的内容

- 结构体数组 `MATCH_*` / `element_filter` 的量词语义与代码路径 ——
  但有**一处有意的三值逻辑修正**：早于字段存在的行（schema 演进回填）过去表现为
  全零 offset，即空数组；现在被记录为 **NULL 行**（全 NULL offset），因此
  `MATCH_ALL` 不再空真命中它们，`not array_contains(...)` 也不再包含它们
  （§4.3，由 `MatchTreatsBackfilledRowsAsNull` 钉住）。
- `array_contains` / `array_contains_all` / `array_contains_any` /
  `array_length` 的语义。它们在**嵌套**索引上的内部执行方式变了
  （在索引函数内部做元素→行归约，§5.1），但结果完全一致。
- RPC 或 SDK 线格式 —— client 发的是表达式字符串，不是 plan proto；只有内部
  proto 变更（`plan.proto` 的 `MatchExpr` 定位符；`workerpb`/`indexpb`/
  `querypb`/`indexcgopb` 各消息上的 `is_nested_index` 标记 —— 均不面向 client）。
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
**前提是该索引是嵌套（元素级）的**。本 PR 让普通 ARRAY 索引在每 segment 标记
的守护下以嵌套布局构建；布局、标记及其完整的构建→加载链路见 §5。

当加载的是**旧式行级**数组索引（标记缺失/false）时，元素级谓词无法使用它：
其倒排表把值映射到*行* id，而元素谓词消费的是*元素* offset。
`UnaryExpr.cpp`、`TermExpr.cpp`、`BinaryRangeExpr.cpp` 与
`BinaryArithOpEvalRangeExpr.h` 中的执行路径守卫会识别这种情况
（`element_level_ && exec_path_ == ScalarIndex && !PinnedIndexIsNested()`）
并回退到暴力求值（`ExprExecPath::RawData`），保证滚动升级期间结果正确。
镜像方向的守卫 —— 行级全数组算子落在*嵌套*索引上 —— 见 §5.1/§8。

目前仍无量词或选择率感知的路径选择（代价启发式留待后续）。

---

## 5. 嵌套数组索引与每 segment 标记

本节覆盖特性中改动存储侧的那一半：普通标量 ARRAY 索引从行级布局切换为嵌套
（元素级）布局，并由每 segment 的标记 —— 而非版本推断 —— 决定哪个 loader
打开哪些文件。

### 5.1 布局：「嵌套」是什么

**旧式**数组标量索引把 `值 → 行 id` 建立映射（只要任一元素等于该值该行即命中 ——
这是 `array_contains` 唯一需要回答的问题）。**嵌套**索引把 `值 → 元素 id`
建立映射（元素在展平元素流中的位置），这正是 `MATCH_*` / `element_filter`
谓词所消费的形态。

构建路由（`IndexFactory::CreateNestedIndex`）：

| 索引类型 | 嵌套构建方式 |
|------------|--------------|
| INVERTED | Tantivy，**每个元素一个文档**（`InvertedIndexTantivy` 以 `is_nested_index = true` 构建；每个元素以递增的元素 offset 逐个加入） |
| BITMAP | 以元素 id 为键的 `BitmapIndex`（`值 → 元素 id 位图`） |
| 其余（sort 族，含 HYBRID/AUTOINDEX 内部解析） | `CreateNestedIndexScalarIndexSort` —— 每个元素一条 `(值, 元素 offset)` 记录 |

嵌套索引的消费方式：

- **元素级谓词**（`MATCH_*` / `element_filter` 子谓词）直接使用元素 bitset；
  表达式框架经 `IArrayOffsets` 按行切片（`Expr.h` 的 `need_element_slicing`）。
- **行级 contains 类算子**（`array_contains*`）仍可使用索引：
  索引函数在内部把元素命中归约为行命中
  （`ProcessIndexChunksWithRowLevel`，`JsonContainsExpr.cpp`），并经
  `IArrayOffsets::AndRowValidBitmap` 补回逐行 NULL 有效性 —— 嵌套索引只暴露
  元素级有效性，而 NULL 行没有任何元素，否则 `not array_contains(nullable, x)`
  会错误地包含 NULL 行（`Expr.h`，`ProcessIndexChunksImpl`）。
- **行级全数组等值**（`arr == [1,2]`、`!=`）由嵌套索引**精确**服务：按位置
  求交（`ExecArrayEqualForNestedIndex`）—— 行长度 == k 且每个位置 *i* 上
  `v_i` 的元素 bitset 在 `s + i` 位为真（元素区间来自 `IArrayOffsets`，NULL
  行经 `AndRowValidBitmap` 补回）。等值之外的全数组算子、浮点元素等值、目标
  类型不匹配、以及 index-only 加载的 segment 仍走暴力回退
  （`CanServeNestedArrayEquality`，在 `DetermineExecPath` 中一次性判定）——
  见 §8。

旧式与嵌套索引在集群中**长期共存**（旧 segment 保留旧文件）。二者**只**由下述
显式的每 segment 标记区分 —— 绝不在加载期按索引引擎版本推断。

### 5.2 版本门槛：为什么是 5

`pkg/common/common.go` 定义了门槛：

```go
CurrentScalarIndexEngineVersion          = int32(5)
MinScalarIndexVersionForNestedArrayIndex = int32(5)
```

> 普通 ARRAY 字段的标量索引以 NESTED（元素级）布局构建，使
> MATCH_*/element_filter 可以直接消费。版本门槛必须是 5（而非 3/4）：
> 版本 3 与 4 已经被不支持嵌套数组的历史版本上报并持久化过，若以 <=4 作为
> 嵌套构建或嵌套加载路由的依据，会 (a) 让混部集群把元素级倒排表交给旧
> querynode，(b) 把以版本 3/4 持久化的旧式数组索引路由进嵌套 loader。

构建决策使用的**解析后**标量索引引擎版本来自 `ResolveScalarIndexVersion()`
（`internal/datacoord/index_engine_version_manager.go`）：
取**所有已注册 querynode 的当前版本最小值**，再 clamp 到协调者自身支持的范围。
因此只要还有 pre-v5 querynode 注册在集群中，就绝不会开始嵌套构建。

### 5.3 `is_nested_index` 标记及其链路

**唯一事实源**：`typeutil.IsNestedArrayIndex(field, scalarIndexVersion)`
（`pkg/util/typeutil/schema.go`）：

```go
return field.GetDataType() == schemapb.DataType_Array &&
    !IsStructSubField(field.GetName()) &&
    scalarIndexVersion >= common.MinScalarIndexVersionForNestedArrayIndex
```

即*普通 ARRAY 字段 + 非结构体子字段 + 解析版本 ≥ 5*。所有决定或持久化标记的
位置都套用这同一条规则，从而即便存在版本偏斜的对端，也能维持不变式
**「普通 ARRAY 字段上的 v5 标量索引 ⟺ 嵌套」**：

```
datacoord  prepareJobRequest (internal/datacoord/task_index.go)
    │   marker := IsNestedArrayIndex(field, ResolveScalarIndexVersion())
    │   CreateJobRequest{ is_nested_index, current_scalar_index_version }
    ▼
index worker  PreExecute → normalizeNestedIndexMarker
    │   (internal/datanode/index/task_index.go)
    │   按字段 schema + CLAMP 后版本重新推导标记，覆盖请求中的位；
    │   经 workerpb.IndexTaskInfo 回传
    ▼
datacoord  FinishTask (internal/datacoord/index_meta.go)
    │   model.SegmentIndex.IsNestedIndex  ──►  持久化到 etcd
    ▼
分发  querypb.FieldIndexInfo.is_nested_index
    │   （在 internal/datacoord/handler.go 中由 segment 索引元数据填充）
    ▼
querynode 加载  indexcgopb CreateIndexInfo.is_nested_index
    │   （进入 C++ 后成为索引参数 "nested_index" /
    │    CreateIndexInfo.nested_array_index）
    ▼
C++  IndexFactory::CreateScalarIndex (internal/core/src/index/IndexFactory.cpp)
        标记为 true    → CreateNestedIndex（元素级 loader）
        缺失/false     → CreateCompositeScalarIndex（旧式 loader）
```

**worker 为什么要重新推导**（`normalizeNestedIndexMarker`）：标记驱动 cgo
构建路径、回传到任务结果、并随 segment 索引元数据持久化 —— 因此它必须在
**两个偏斜方向**上都正确：

- **旧 datacoord** 的请求 proto 中没有 `is_nested_index` 字段，却仍可能从已
  升级的 querynode 解析出标量版本 5。若直接相信这个缺失的位，会构建出一个
  *打着 v5 标记*的行级索引；之后任何标记重推导（snapshot 恢复的 copy 路径，
  §5.4）都会把它判为嵌套并路由进嵌套 loader —— 静默出错；
- 被 **clamp 到 v5 以下**的 worker 构建的是旧式行级索引，无论请求怎么说，
  都必须回报标记为 false。

**加载只看标记。**缺失/false 意味着走旧式 composite loader —— 包括
HYBRID/AUTOINDEX（其文件布局只有 `HybridScalarIndex` 能打开）—— 因此每个旧
索引都确定性地按其构建时的路径加载，而这类 segment 上的元素级 `MATCH_*`
谓词回退到暴力求值（§4.6 守卫）。

**标记语义 —— 结构体子字段的豁免。**标记的含义是*「加载此索引需要支持嵌套
数组（v5）的代码」*，而**不是***「物理布局是元素级」*。结构体子字段索引
（`parent[sub]`，ARRAY 类型）物理上同样是元素级，但**所有已发布的二进制**
都**按名字**把它们路由到嵌套 loader —— `IndexFactory::CreateScalarIndex` 中
`IsStructSubField(name)` 检查先于标记检查 —— 因此它们在任何版本上都能正确
加载，其标记**必须保持 false**。若把它设为 true，snapshot 恢复的版本守卫
（§5.4）会错误地拒绝把它们放到其实完全能加载它们的 pre-v5 池上。

**保留索引参数。**`nested_index`（`common.NestedIndexKey`）是内部构建/加载
配置键；索引参数会被原样拷贝进该配置，用户若能自行设置就会绕过版本门槛、
使持久化标记与物理布局脱钩。`indexparamcheck.ValidateIndexParams` 在
`IndexParams`、`UserIndexParams`、`TypeParams` 三处一律拒绝它，并在
datacoord 的 `CreateIndex` 与 `AlterIndex`（`internal/datacoord/index_service.go`）、
snapshot 恢复（`internal/datacoord/snapshot_manager.go`）以及 rootcoord 的
schema 变更 DDL 回调
（`internal/rootcoord/ddl_callbacks_alter_collection_schema.go`）处强制执行。

### 5.4 Snapshot、copy 与恢复

- **Manifest**：Avro snapshot manifest 格式版本 5
  （`SnapshotFormatVersion = 5`，`internal/snapshotio/snapshot.go`）在索引文件
  条目上新增 `is_nested_index`（Avro 默认值 `false`，`AvroSchemaV5`）。
  以版本 ≤ 4 写出的 manifest 解码时标记为 false。若在往返中丢失标记，
  嵌套 ARRAY 索引会被当作旧式 composite 索引加载 —— HYBRID/AUTOINDEX 随即因
  缺少 `INDEX_TYPE` 条目而硬失败。
- **恢复期重推导**：`syncVectorScalarIndexes`
  （`internal/datacoord/copy_segment_task.go`）对照目标 collection schema，
  用同一条 `IsNestedArrayIndex(field, 回传版本)` 规则**权威地**重推导标记，
  而不是相信 copy worker 回传的位 —— 旧的（不支持嵌套的）copy worker 会逐字段
  重建索引信息、静默丢掉新字段，却仍回传标量版本 5。仅当目标 schema 中无法
  解析该字段（schema 漂移）时才回退使用 worker 回传值。
- **恢复版本守卫**：`checkNestedArrayIndexVersion`
  （`internal/datacoord/snapshot_manager.go`，在 `RestoreData` 中调用）拒绝把
  含任何嵌套索引的 snapshot 恢复到解析版本 < 5 的 querynode 池上，并给出明确的
  *「please complete the rolling upgrade first」*错误。

### 5.5 内存估算与缓存

- **加载估算**（`ScalarIndexLoadResource`，`IndexFactory.cpp`）：嵌套索引的
  基数是元素数，按行数计算的公式会低估。对设置了 `nested_index` 参数的 ARRAY
  字段，BITMAP 与 sort 族的 mmap **常驻**估算被抬底到索引文件大小
  （`resident_bytes = std::max(resident_bytes, index_size_in_bytes)`）；
  旧式行级数组索引保持按行数的估算（`BitsetBytes(num_rows)` /
  `SortLegacyAuxBytes(num_rows)`）。HYBRID 先解析到其内部索引类型
  （`ResolveHybridInternalIndexType`）再递归估算。
- **逐元素 offset 缓存**：`BitmapIndex` 对嵌套索引跳过 `BuildOffsetCache()`
  （`BitmapIndex.cpp` 中的 `!is_nested_index_` 守卫）：该缓存按**元素**数分配、
  常驻堆内、不在序列化文件中、也不被加载估算计入；`Reverse_Lookup` 回退到
  bitmap map 扫描。
- **表达式结果磁盘缓存**：元素级结果位图（大小 = 元素数，由 `MATCH_*`/
  `element_filter` 子谓词在嵌套索引上产生）跳过磁盘槽位
  （`ExprCache.cpp` 中 `ExprResCacheManager::Put` 拒绝
  `result->size() != active_count`）：每 segment 磁盘槽位大小由首次写入固定，
  大小不匹配会被当作 growing segment 处理 —— 删除文件并永久禁用该 segment 的
  磁盘缓存。元素级结果只留在内存缓存。

### 5.6 加载期自识别（纵深防御）

在标记之外，索引文件本身也约束误路由：

- **BitmapIndex** 在文件元数据中持久化并读取 `is_nested` 标志
  （`BITMAP_INDEX_IS_NESTED_META`）—— 加载时自识别。
- **Tantivy 倒排**读取 `is_nested` 元数据键 —— 同样自识别。
- **sort 族无法自识别** —— 但它**响亮地失败**：把按嵌套构建的 sort 索引经旧式
  路径加载会触发对 `idx_to_offsets` 数据块大小的 `AssertInfo`
  （期望 `total_num_rows_ * sizeof(int32_t)`，实际是按元素数的大小），
  而不是静默返回错误结果。

这对 §7.2 分析的旧 datacoord 升级窗口至关重要。

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
- **嵌套索引构建/加载/一致性**（`MatchExprTest.cpp` 中的
  `ScalarArrayMatchIndex*`）：嵌套 INVERTED 加速 `MATCH_*`
  （`NestedInvertedIndexAccelerates`、`NestedInvertedIndexNullable`），嵌套
  BITMAP 与 sort 族的 nullable 用例（`NestedBitmapIndexNullable`、
  `NestedScalarSortIndexNullable`），index-only segment
  （`IndexOnlyWithoutRawHasNoOffsets`），以及 Int64/VarChar 在倒排与 sort
  索引上的索引-暴力一致性（`*IndexedEqualsBruteForce`）。
  `NonNestedIndexFallsBackToBruteForce` 钉住 §4.6 的旧式索引回退。
  `NestedArrayIndexRawData.NestedIndexesReportNoRawData`
  （`test_element_filter.cpp`）钉住 raw-data 上报。
- 既有的结构体数组 `MatchExprTest` 行为不变；其 growing segment 测试额外增强了全量召回验证。

### 6.3 标记链路（Go）与端到端

- `Test_isNestedArrayIndex`（`internal/datacoord/task_index_test.go`）：
  §5.3 的构建决策规则。
- `TestSegmentIndex_MarshalUnmarshal_IsNestedIndex`
  （`internal/metastore/model`）：etcd 元数据往返。
- `TestSnapshotManager_CheckNestedArrayIndexVersion`
  （`internal/datacoord/snapshot_manager_test.go`）：恢复版本守卫（§5.4）。
- `TestValidateIndexParamsReservedNestedIndexKey`
  （`internal/util/indexparamcheck`）：保留参数拒绝（§5.3）。
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
| Segment 类型 | sealed、growing、index-only（无原始数据） |
| 谓词形式 | 简单比较、复合 `&&`/`||`、三元 range、`like`、`in`/`not in` |
| 边界用例 | 空数组（空真）、NULL 行（三值语义：结果为 UNKNOWN/排除，而非空真命中）、回填行 = NULL（而非 `[]`）、threshold 边界（0、N） |
| 负面用例 | 标量数组上 `$[sub]`、裸 `$`、非数组字段、嵌套、行级/常量谓词、保留参数 `nested_index` |
| 索引 | 嵌套 INVERTED/BITMAP/sort 加速 `MATCH_*` 且结果与暴力求值断言一致；旧式（非嵌套）索引 → 回退暴力求值；标记链路、snapshot 往返与恢复守卫均有单测 |

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

### 7.2 嵌套索引升级/回滚矩阵

| 场景 | 行为 |
|----------|----------|
| **正向滚动升级**（任意顺序） | 安全。构建门槛取所有已注册 querynode 引擎版本的**当前最小值**（§5.2），因此只要还有 pre-v5 querynode 注册，就绝不会构建嵌套索引。既有 etcd 元数据没有标记 → 解码为 false → 旧式 loader；存量索引行为完全不变。 |
| **旧 datacoord + 已升级 querynode/worker**（窗口期） | datacoord 从已升级的 querynode 解析出 v5，但其 proto 没有标记字段。worker 侧的重推导（§5.3）保证**构建仍是嵌套的**且回传正确；但旧 datacoord 的元数据模型会丢掉回传的位，**持久化标记为 false**，且在 datacoord 升级后依旧是 false。加载时嵌套文件落入旧式路径：BITMAP 与 INVERTED **凭文件元数据自识别**、仍正确加载；sort 族**响亮地失败**（§5.6）—— 显式报错而非静默损坏。copy/恢复的重推导（§5.4）可自愈元数据中的标记。 |
| **嵌套索引已存在后回滚/旧镜像扩容** | **无法检测 —— 需要运维纪律。**重新加入集群的 pre-v5 querynode 会丢弃未知的 `is_nested_index` proto 字段，把嵌套文件经行级路径加载；嵌套 INVERTED 随即返回**静默错误的结果**（元素 id 被当作行 id 读取）。**一旦构建过任何嵌套索引，就不要把 querynode 降级到引擎 v5 之前的版本。**如确需回滚：先删除/重建数组字段索引，或先从升级前的 snapshot 恢复。 |

### 7.3 行为兼容性

- **结构体数组**：相同的解析器路径（默认分支）与相同的执行；由既有的
  `MatchExpr` 测试套件做回归保护。本 PR 附带一处有意的语义修正（§2.3）：
  schema 演进回填的行现在是 NULL 行而非空数组，因此 `MATCH_ALL` / 取反的
  `array_contains` 不再命中它们 —— 这才是符合 SQL 语义的行为。
- **Schema 演进**：通过 `AlterCollectionSchema` 新增的标量数组字段会在 growing segment
  `Reopen` 时注册其 `IArrayOffsets`；回填行记录为 NULL（§4.3）。

---

## 8. 性能权衡

- **全数组算子回退（已收窄到非等值算子）**：旧式 `ExecArrayEqualForIndex`
  需要 `值 → 行 id` 的倒排表，而嵌套索引存的是 `值 → 元素 id`，两个 offset
  空间不一致。全数组 **Equal/NotEqual** 现在由**按位置求交的索引路径**
  （`ExecArrayEqualForNestedIndex`）在嵌套索引上精确计算：行长度 == k 且位
  置 *i* 的元素匹配 *v_i*，由按值的元素 bitset（`In()`，每个不同目标值只查
  一次、按 segment 缓存）加 `IArrayOffsets` 行区间求得；NULL 行经
  `AndRowValidBitmap` 排除，与暴力路径完全一致。`UnaryExpr` 的执行路径守卫
  （`!element_level_ && data_type == ARRAY && PinnedIndexIsNested() &&
  !CanServeNestedArrayEquality()` → `RawData`）仍把无法精确服务的形态强制为
  **暴力扫描**：等值之外的全数组算子、浮点元素等值（索引不精确）、目标类型
  不匹配、以及 index-only 加载（没有 `IArrayOffsets`）。`array_contains*`
  **不**受影响：它继续经元素→行归约使用嵌套索引（§5.1）。
- **构建成本随元素扇出增长**：嵌套构建按**元素**逐条建索引（tantivy：每个
  元素一个文档），因此相对旧式行级布局，构建时间与索引体积随平均数组长度增长。
- **内存准入**：嵌套 BITMAP / sort 族的常驻估算被抬底到索引文件大小（§5.5）
  —— 对同样的数据比旧式按行估算更保守，这是有意为之。
- **本 PR 无基准测试**：所含测试全部是正确性测试。上述回退窗口来自对执行路径
  守卫的代码审读，而非实测；对嵌套构建与暴力回退的基准测量是后续工作。
