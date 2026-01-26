# Query Aggregation Test Plan

**Created**: 2026-01-26
**Author**: Claude (milvus-new-feature-test-plan skill)
**Status**: Draft
**PR**: #44394
**Issue**: #36380

---

## Feature Introduction

Query Aggregation功能为Milvus添加了在查询过程中对标量列进行分组（GROUP BY）和聚合计算的能力。用户可以在query操作中对数据进行分组，并对每个分组计算聚合统计信息，包括COUNT、SUM、MIN、MAX、AVG五种聚合函数。

**解决的问题**:
- 支持SQL风格的GROUP BY + 聚合查询，无需将数据导出到客户端进行聚合
- 减少网络传输，在服务端完成聚合计算
- 提供灵活的分组和聚合组合，支持单列或多列分组

**功能概述**:
- 新增 `group_by_fields` 参数，支持单列或多列分组
- 在 `output_fields` 中使用聚合函数语法：`count(field)`, `sum(field)`, `min(field)`, `max(field)`, `avg(field)`
- 支持与过滤表达式 (`expr`) 和限制 (`limit`) 组合使用
- 支持的数据类型：Int8/16/32/64, Float, Double, VarChar, Timestamptz
- 不支持复杂类型：JSON, Array, Vector

---

## Design Doc

- **Issue**: https://github.com/milvus-io/milvus/issues/36380
- **PR**: https://github.com/milvus-io/milvus/pull/44394
- **Design Document**: ~/Downloads/Milvus Aggregation QA Document.pdf
- **Example Code**: pymilvus/examples/query_group_by.py

---

## Dev-self-test

开发自测清单（提测前必须完成）：

- [ ] 代码编译通过，无语法错误
- [ ] C++单元测试全部通过 (test_query_group_by.cpp, test_c_api.cpp)
- [ ] 基本功能验证通过
  - [ ] 单列group by + COUNT/SUM
  - [ ] 多列group by + MIN/MAX
  - [ ] Group by + filter表达式
  - [ ] 聚合函数返回类型正确
- [ ] 参数验证正确 (分组字段必须在output_fields中)
- [ ] 错误处理完善 (不支持的类型报错)
- [ ] 代码review完成
- [ ] Python SDK示例代码测试通过

---

## Goal

### 功能目标

- 验证 Query Aggregation 的正确性和完整性
- 确保所有聚合函数（COUNT/SUM/MIN/MAX/AVG）在所有支持的数据类型上正常工作
- 确保单列和多列GROUP BY功能正确
- 验证与过滤表达式和limit的组合使用
- 确保返回类型符合规范
- 确保不支持的类型正确报错

### 性能目标

**[TODO: 定义具体的性能指标和阈值]**

- 聚合查询延迟: <= 200ms (1000个分组)
- 聚合查询延迟: <= 500ms (10000个分组)
- 吞吐量: >= 500 QPS
- 内存占用: 与数据规模线性增长

### 质量目标

- 功能测试覆盖率: >= 90%
- 聚合结果准确率: 100% (与ground truth对比)
- 无P0/P1 bug遗留

---

## Scope

### In Scope

本次测试包含以下内容：

- **聚合函数**:
  - COUNT: 统计数量
  - SUM: 求和 (数值类型)
  - MIN: 最小值 (所有支持类型)
  - MAX: 最大值 (所有支持类型)
  - AVG: 平均值 (数值类型，实现为SUM+COUNT)

- **Group By场景**:
  - 单列GROUP BY
  - 多列GROUP BY
  - GROUP BY + 单个聚合函数
  - GROUP BY + 多个聚合函数
  - GROUP BY + filter表达式
  - GROUP BY without filter
  - GROUP BY with limit

- **支持的数据类型**:
  - Int8, Int16, Int32, Int64
  - Float, Double
  - VarChar
  - Timestamptz

- **返回类型验证**:
  - 整形SUM → int64
  - 浮点型SUM → double
  - AVG → double
  - MIN/MAX → 原字段类型

- **约束验证**:
  - 分组字段必须在output_fields中
  - 不支持JSON/Array类型
  - 不支持向量字段

### Out of Scope

本次测试不包含以下内容（后续版本或长期计划）：

- JSON/Array字段的聚合
- 向量字段的聚合
- 嵌套聚合 (aggregation of aggregation)
- HAVING子句
- Window函数
- Search + Aggregation组合 (Search不支持aggregation)

### Notes

- **Cloud-only feature**: No
- **Release version**: 2.5.0+
- **Release timeline**: 2026-01 master分支

**[TODO: 确认以下信息]**
- [ ] 确认云上部署计划和时间节点
- [ ] 确认性能基准和SLA要求

---

## Risk

### 进度风险

- 功能复杂度高，涉及多个组件交互 (Proxy, QueryNode, Segcore)
- 大规模数据下的性能优化可能需要额外时间

### 质量风险

- **核心组件修改风险**:
  - 修改了Segcore的查询执行路径 (ExecPlanNodeVisitor, Driver)
  - 新增了大量聚合算子实现 (query-agg目录)
  - 修改了plan.proto定义，涉及server/client兼容性

- **兼容性风险**:
  - 旧版本client不支持此功能
  - Protobuf变更可能影响跨版本兼容性

- **正确性风险**:
  - 聚合结果准确性依赖复杂的算子实现
  - 多列GROUP BY的hash冲突处理
  - Null值的处理逻辑
  - 数值溢出 (SUM)

- **性能风险**:
  - 大规模分组可能导致内存占用过高
  - HashTable性能在高冲突场景下降

### 技术风险

- AVG实现为SUM+COUNT的组合，需要验证精度
- 不同数据类型的MIN/MAX比较逻辑正确性
- VarChar类型的字典序比较
- Timestamptz的时区处理

**[TODO: 补充额外的技术风险]**

---

## Scheduler

| Milestone | Planned Date | Actual Date | Status |
|-----------|--------------|-------------|--------|
| 功能提测 | 2026-01-20 | - | Completed |
| 测试计划完成 | 2026-01-27 | - | In Progress |
| 测试用例开发 | 2026-02-05 | - | Pending |
| 测试用例评审 | 2026-02-10 | - | Pending |
| 测试执行完成 | 2026-02-20 | - | Pending |
| 测试报告完成 | 2026-02-25 | - | Pending |
| 云上发布 | 2026-03-01 | - | Pending |

**[TODO: 与项目组确认具体日期]**

---

## API

### 新增API参数

**Query API新增参数**:

| 参数 | 类型 | 默认值 | 范围 | 描述 |
|-----|------|--------|------|------|
| `group_by_fields` | List[str] | [] | 支持的标量字段名 | 分组字段列表，支持单列或多列 |

**Output Fields聚合函数语法**:

| 聚合函数 | 语法 | 适用类型 | 返回类型 |
|---------|------|---------|---------|
| COUNT | `count(field)` | 所有支持类型 | int64 |
| SUM | `sum(field)` | Int*, Float, Double | int64 (整形) / double (浮点) |
| MIN | `min(field)` | Int*, Float, Double, VarChar | 原字段类型 |
| MAX | `max(field)` | Int*, Float, Double, VarChar | 原字段类型 |
| AVG | `avg(field)` | Int*, Float, Double | double |

### 参数限制

- `group_by_fields`:
  - 必须是标量字段 (Int*, Float, Double, VarChar, Timestamptz)
  - 不支持JSON, Array, Vector字段
  - 所有分组字段必须出现在 `output_fields` 中
  - 支持空列表 (全局聚合，不分组)

- `output_fields`:
  - 必须包含所有 `group_by_fields` 中的字段
  - 支持聚合函数语法: `function(field_name)`
  - 不能同时包含普通字段和聚合字段 (除了group_by字段)

- `limit`:
  - 限制返回的分组数量
  - 默认无限制
  - 最大值: 16384

### 配置开关

无配置开关，功能默认启用。

### RBAC要求

使用Query API的权限要求不变，继承Query操作的权限控制。

---

## Test Scenarios

### 测试策略

本功能测试遵循L0/L1/L2优先级划分：

- **L0**: 1-2个基本功能用例，覆盖最常用场景
- **L1**: 10-15个常用组合用例，覆盖关键数据类型和聚合函数组合
- **L2**: 全面覆盖所有组合，边界和负向测试

### L0 Test Cases (基本功能验证)

#### TC-L0-01: 单列GROUP BY + COUNT/SUM

- **测试目的**: 验证基本的单列分组和聚合功能
- **前置条件**:
  - Collection with Int64, Int16, VarChar, FloatVector fields
  - 插入3000条数据
  - Data flushed and loaded
- **测试步骤**:
  1. 执行query: `group_by_fields=["c1"]`, `output_fields=["c1", "count(c2)", "sum(c3)"]`
  2. 验证返回的分组数量
  3. 验证每个分组的count和sum值正确性
- **预期结果**:
  - 返回7个分组 (c1字段有7个unique值)
  - 每个分组的count(c2)和sum(c3)与ground truth一致
  - 返回类型: count → int64, sum → int64
- **数据类型**: Int64, Int16, VarChar
- **聚合函数**: COUNT, SUM

#### TC-L0-02: 多列GROUP BY + MIN/MAX

- **测试目的**: 验证多列分组和MIN/MAX聚合
- **前置条件**: 同TC-L0-01
- **测试步骤**:
  1. 执行query: `group_by_fields=["c1", "c6"]`, `output_fields=["c1", "c6", "min(c2)", "max(c2)"]`
  2. 验证返回的分组数量 (c1和c6的笛卡尔积)
  3. 验证每个分组的min和max值
- **预期结果**:
  - 返回49个分组 (c1有7个unique值, c6有7个unique值)
  - 每个分组的min(c2)和max(c2)正确
  - 返回类型: min/max → int16 (c2原类型)

### L1 Test Cases (常用组合)

#### TC-L1-01: 单列GROUP BY + 多个聚合函数

- **测试目的**: 验证单个分组字段上计算多个聚合值
- **前置条件**: Collection with multiple numeric fields
- **测试步骤**:
  1. 执行query: `group_by_fields=["c1"]`, `output_fields=["c1", "avg(c2)", "avg(c3)", "avg(c4)"]`
  2. 验证AVG返回类型为double
  3. 验证AVG计算准确性
- **预期结果**:
  - 每个分组返回3个AVG值
  - AVG值精度正确 (与SUM/COUNT计算对比)
  - 返回类型全部为double

#### TC-L1-02: GROUP BY + Filter表达式

- **测试目的**: 验证分组与过滤表达式的组合
- **前置条件**: Collection loaded
- **测试步骤**:
  1. 执行query: `expr="c2 < 10"`, `group_by_fields=["c1"]`, `output_fields=["c1", "count(c2)", "max(c3)"]`
  2. 验证只有c2 < 10的记录参与分组
  3. 验证聚合结果正确性
- **预期结果**:
  - 过滤后的数据正确分组
  - 聚合值基于过滤后的数据集

#### TC-L1-03: GROUP BY without Filter + Limit

- **测试目的**: 验证无过滤条件的分组查询和limit限制
- **前置条件**: Collection loaded
- **测试步骤**:
  1. 执行query: `expr=""`, `group_by_fields=["c1"]`, `output_fields=["c1", "avg(c2)"]`, `limit=10`
  2. 验证返回分组数 <= 10
- **预期结果**:
  - 返回最多10个分组
  - 聚合值正确

#### TC-L1-04: VarChar字段的MIN/MAX

- **测试目的**: 验证字符串类型的MIN/MAX聚合
- **前置条件**: Collection with VarChar fields
- **测试步骤**:
  1. 执行query: `group_by_fields=["c1"]`, `output_fields=["c1", "min(c6)", "max(c6)"]`
  2. 验证字符串字典序比较正确
- **预期结果**:
  - MIN/MAX按字典序排序
  - 返回类型为VarChar

#### TC-L1-05: Timestamptz字段的聚合

- **测试目的**: 验证时间戳类型的分组和聚合
- **前置条件**: Collection with Timestamptz field
- **测试步骤**:
  1. 执行query: `group_by_fields=["ts"]`, `output_fields=["ts", "count(c2)", "max(ts)"]`
  2. 验证时间戳分组正确
- **预期结果**:
  - 按时间戳分组
  - MAX(ts)返回Timestamptz类型

#### TC-L1-06: 不同数值类型的SUM

- **测试目的**: 验证不同数值类型SUM的返回类型
- **前置条件**: Collection with Int32, Int64, Float, Double fields
- **测试步骤**:
  1. 执行query: `sum(c2)` (Int16) → 预期返回int64
  2. 执行query: `sum(c3)` (Int32) → 预期返回int64
  3. 执行query: `sum(c4)` (Double) → 预期返回double
- **预期结果**:
  - 整形SUM返回int64
  - 浮点型SUM返回double

#### TC-L1-07: AVG返回类型验证

- **测试目的**: 验证AVG无论输入类型，返回都是double
- **前置条件**: Collection with multiple numeric types
- **测试步骤**:
  1. 执行query: `avg(c2)` (Int16) → 预期double
  2. 执行query: `avg(c3)` (Int32) → 预期double
  3. 执行query: `avg(c4)` (Double) → 预期double
- **预期结果**:
  - 所有AVG返回double类型

#### TC-L1-08: Nullable字段的聚合

- **测试目的**: 验证nullable字段的聚合行为
- **前置条件**: Collection with nullable scalar fields
- **测试步骤**:
  1. 插入包含null值的数据
  2. 执行query: `group_by_fields=["c1"]`, `output_fields=["c1", "count(nullable_field)", "sum(nullable_field)"]`
  3. 验证null值的处理逻辑
- **预期结果**:
  - COUNT忽略null值（或包含null值，需确认）
  - SUM/AVG忽略null值
  - MIN/MAX忽略null值

#### TC-L1-09: 空结果集的聚合

- **测试目的**: 验证过滤后无匹配记录的聚合行为
- **前置条件**: Collection loaded
- **测试步骤**:
  1. 执行query: `expr="c2 > 10000"`, `group_by_fields=["c1"]`, `output_fields=["c1", "count(c2)"]`
- **预期结果**:
  - 返回空结果集或单行count=0（需确认）

#### TC-L1-10: 全局聚合 (无GROUP BY)

- **测试目的**: 验证不指定group_by_fields的全局聚合
- **前置条件**: Collection loaded
- **测试步骤**:
  1. 执行query: `group_by_fields=[]`, `output_fields=["count(*)", "sum(c2)", "avg(c3)"]`
  2. 验证返回单行全局聚合结果
- **预期结果**:
  - 返回1行结果
  - 包含全局聚合值

### Coverage Matrix

#### 数据类型 × 聚合函数

| Data Type | COUNT | SUM | MIN | MAX | AVG | Notes |
|-----------|-------|-----|-----|-----|-----|-------|
| Int8 | ✅ L1 | ✅ L1 | ✅ L2 | ✅ L2 | ✅ L1 | SUM/AVG返回int64/double |
| Int16 | ✅ L0 | ✅ L0 | ✅ L0 | ✅ L0 | ✅ L1 | |
| Int32 | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L1 | |
| Int64 | ✅ L0 | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L2 | |
| Float | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L1 | SUM/AVG返回double |
| Double | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L1 | ✅ L0 | |
| VarChar | ✅ L1 | ❌ | ✅ L1 | ✅ L1 | ❌ | 字典序比较 |
| Timestamptz | ✅ L1 | ❌ | ✅ L1 | ✅ L1 | ❌ | |

#### Group By场景 × 聚合函数

| Scenario | COUNT | SUM | MIN | MAX | AVG | Priority |
|----------|-------|-----|-----|-----|-----|----------|
| 单列GROUP BY | ✅ | ✅ | ✅ | ✅ | ✅ | L0 |
| 多列GROUP BY | ✅ | ✅ | ✅ | ✅ | ✅ | L0 |
| GROUP BY + filter | ✅ | ✅ | ✅ | ✅ | ✅ | L1 |
| GROUP BY + limit | ✅ | ✅ | ✅ | ✅ | ✅ | L1 |
| 无GROUP BY (全局) | ✅ | ✅ | ✅ | ✅ | ✅ | L1 |
| 多个聚合函数 | ✅ | ✅ | ✅ | ✅ | ✅ | L1 |

### 特殊行为测试

- [ ] **Nullable**: Nullable标量字段的聚合行为 (L1)
- [ ] **Flush State**: Growing segment vs Sealed segment的聚合结果一致性 (L1)
- [ ] **Add Field**: 动态添加的字段支持聚合 (L2)
- [ ] **MMAP**: MMAP启用时的聚合功能 (L2)

### 功能边界测试

- **分组数量**:
  - 1个分组
  - 100个分组
  - 10000个分组
  - 100000+个分组 (性能测试)

- **聚合字段数量**:
  - 1个聚合字段
  - 5个聚合字段
  - 10个聚合字段

- **数值边界**:
  - SUM溢出测试 (int64最大值)
  - MIN/MAX极值 (最小/最大可表示数)
  - AVG精度测试 (小数位数)

- **字符串边界**:
  - VarChar max_length边界
  - 空字符串
  - 特殊字符

### 负向测试

#### TC-NEG-01: 分组字段不在output_fields中

- **测试步骤**: `group_by_fields=["c1"]`, `output_fields=["count(c2)"]` (缺少c1)
- **预期结果**: 报错，提示分组字段必须在output_fields中

#### TC-NEG-02: 不支持的数据类型 - JSON

- **测试步骤**: `group_by_fields=["json_field"]` 或 `output_fields=["sum(json_field)"]`
- **预期结果**: 报错，提示JSON类型不支持聚合

#### TC-NEG-03: 不支持的数据类型 - Array

- **测试步骤**: `group_by_fields=["array_field"]` 或 `output_fields=["sum(array_field)"]`
- **预期结果**: 报错，提示Array类型不支持聚合

#### TC-NEG-04: 不支持的数据类型 - Vector

- **测试步骤**: `group_by_fields=["vector_field"]`
- **预期结果**: 报错，提示Vector类型不支持分组

#### TC-NEG-05: 不支持的聚合函数组合

- **测试步骤**: `output_fields=["sum(varchar_field)"]`
- **预期结果**: 报错，提示VarChar不支持SUM

#### TC-NEG-06: 混合聚合和非聚合字段

- **测试步骤**: `output_fields=["c1", "c2", "count(c3)"]` (c2不是group_by字段)
- **预期结果**: 报错，output_fields只能包含group_by字段和聚合字段

#### TC-NEG-07: 无效的聚合函数语法

- **测试步骤**: `output_fields=["COUNT(c2)"]` (大写), `output_fields=["count c2"]` (缺少括号)
- **预期结果**: 报错，提示语法错误

---

## Configuration

### Feature配置

无配置参数，功能默认启用。

---

## Monitoring

### 监控指标

**[TODO: 确认是否有新增监控指标]**

建议的监控指标：
- `query_aggregation_count`: 聚合查询总数
- `query_aggregation_latency`: 聚合查询延迟分布
- `query_aggregation_groups`: 返回的分组数量分布
- `query_aggregation_error`: 聚合查询错误数

### 测试验证

- [ ] 监控指标正确性验证
- [ ] 监控指标实时性验证
- [ ] 异常场景监控验证

---

## Performance

### 性能基准

| 指标 | 目标值 | 测试方法 |
|------|--------|---------|
| 查询延迟 (100组) | <= 100ms | 压力测试 |
| 查询延迟 (1000组) | <= 200ms | 压力测试 |
| 查询延迟 (10000组) | <= 500ms | 压力测试 |
| 内存占用 | 与分组数线性增长 | 资源监控 |
| 聚合结果准确性 | 100% | Ground truth对比 |

**[TODO: 与产品确认具体性能目标阈值]**

### 对比测试

- [ ] 不同分组数量的性能对比 (100 vs 1000 vs 10000)
- [ ] 单列vs多列GROUP BY的性能对比
- [ ] 不同聚合函数的性能对比
- [ ] Growing vs Sealed segment的性能对比

### 准确性验证

- [ ] 聚合结果与Python/SQL计算对比 (ground truth)
- [ ] 不同数据分布的准确性 (均匀分布、倾斜分布)
- [ ] 大数值的SUM精度验证
- [ ] 浮点数的AVG精度验证

### 性能优化验证

- [ ] HashTable在不同数据分布下的性能
- [ ] 多列GROUP BY的hash策略
- [ ] 内存分配和回收效率

---

## Stability

### 测试场景

- [ ] **长时间运行测试** (24小时+)
  - 持续聚合查询
  - 监控内存/CPU占用趋势
  - 检查内存泄漏
  - 验证结果持续准确

- [ ] **并发压力测试**
  - 多客户端并发聚合查询
  - 不同分组条件并发
  - 混合query+aggregation负载

- [ ] **大规模数据测试**
  - 亿级数据量
  - 10万+分组
  - 长时间聚合计算

---

## Chaos

### 故障注入场景

- [ ] **节点故障**
  - Kill QueryNode (聚合计算节点)
  - 验证自动恢复
  - 验证聚合结果一致性

- [ ] **网络分区**
  - Proxy与QueryNode网络隔离
  - 验证查询失败和重试

- [ ] **磁盘故障**
  - 磁盘满场景
  - 验证错误处理

- [ ] **负载突增**
  - 聚合查询流量突增
  - 验证限流和降级

---

## Rolling Upgrade

### 升级路径测试

- [ ] **逐个组件升级**
  - 旧Proxy + 新QueryNode
  - 新Proxy + 旧QueryNode
  - 验证功能降级 (旧版本不支持)

- [ ] **升级过程中的操作验证**
  - 普通Query操作正常
  - 聚合Query在新节点上正常
  - 旧节点返回unsupported错误

- [ ] **回滚测试**
  - 升级失败回滚
  - 聚合功能自动禁用

---

## Compatibility

### Server-Client版本兼容性

| Server Version | Client Version | Aggregation Support | 测试结果 |
|----------------|----------------|---------------------|---------|
| v2.5.x (新) | v2.4.x (旧) | ❌ 不支持 | Pending |
| v2.4.x (旧) | v2.5.x (新) | ❌ Server不支持 | Pending |
| v2.5.x | v2.5.x | ✅ 支持 | Pending |

### 数据兼容性

- [ ] **Protobuf兼容性**
  - 新增 `group_by_field_ids` 和 `aggregates` 字段
  - 验证旧版本client发送的请求不受影响
  - 验证新字段的序列化/反序列化

- [ ] **Segment格式兼容性**
  - 聚合操作不影响segment数据格式
  - 旧版本segment支持聚合查询

---

## Extensions

### Bulk Insert / Bulk Writer

聚合功能不影响Bulk Insert/Writer，仅影响查询阶段。

### Backup & Restore

- [ ] Backup包含的数据支持聚合查询
- [ ] Restore后的数据聚合查询正常

### CDC

聚合功能不影响CDC，CDC捕获的是原始数据变更。

### RBAC

- [ ] Query权限包含聚合查询权限
- [ ] 没有Query权限的角色无法执行聚合查询
- [ ] 权限控制与普通Query一致

---

## Others

### 易用性评估

- [ ] API设计是否直观 (SQL风格的GROUP BY + 聚合函数)
- [ ] 错误信息是否清晰明确
- [ ] 文档是否完整准确
- [ ] Python SDK示例代码是否易于理解

### 与SQL标准的对比

| SQL Feature | Milvus Support | Notes |
|-------------|----------------|-------|
| GROUP BY single column | ✅ | `group_by_fields=["field"]` |
| GROUP BY multiple columns | ✅ | `group_by_fields=["f1", "f2"]` |
| COUNT(*) | ✅ | `output_fields=["count(*)"]` |
| SUM/AVG/MIN/MAX | ✅ | 标准聚合函数 |
| HAVING | ❌ | 不支持 |
| DISTINCT | ❌ | 可通过GROUP BY实现 |
| Window functions | ❌ | 不支持 |
| Nested aggregation | ❌ | 不支持 |

### 其他注意事项

- 聚合函数语法区分大小写 (必须小写: `count`, `sum`, `min`, `max`, `avg`)
- 分组字段顺序可能影响返回结果顺序
- Limit限制的是分组数量，不是记录数量
- AVG的实现是SUM+COUNT，精度可能受浮点数表示影响

---

## Appendix

### 参考文档

- PR #44394: https://github.com/milvus-io/milvus/pull/44394
- Issue #36380: https://github.com/milvus-io/milvus/issues/36380
- Design Doc: ~/Downloads/Milvus Aggregation QA Document.pdf
- Example Code: pymilvus/examples/query_group_by.py

### 测试环境

- Milvus版本: 2.5.0+ (master branch, 2026-01+)
- 部署方式: Standalone + Cluster
- Python SDK: pymilvus latest

### 测试数据集

- 数据规模: 3000条 (基础测试), 100万+ (性能测试)
- Schema:
  - pk: VarChar (primary key)
  - c1: VarChar (分组字段, 7 unique values)
  - c2: Int16 (聚合字段)
  - c3: Int32 (聚合字段)
  - c4: Double (聚合字段)
  - ts: Timestamptz (分组/聚合字段)
  - c5: FloatVector (dim=8)
  - c6: VarChar (分组字段, 7 unique values)

---

**Last Updated**: 2026-01-26
