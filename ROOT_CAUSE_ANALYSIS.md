# Issue #46656 根因分析与修复方案

## 问题现象

QueryNode 在处理 Growing Segment 的 Insert 操作时，发生 `std::out_of_range` 异常崩溃：

```
panic: unordered_map::at
github.com/milvus-io/milvus/internal/querynodev2/delegator.(*shardDelegator).ProcessInsert
```

特征：
- 发生在 text index (Tantivy) 场景
- 涉及 schema 动态更新
- 错误来源于 `schema_.get_fields().at(fieldId)` 调用

---

## 根本原因

### 1. Schema 引用的生命周期不一致

**问题代码路径**：

```cpp
// SegmentGrowingImpl.h:662
SchemaPtr schema_;  // 可能被 Reopen() 替换

// SegmentGrowingImpl.h:347 - 构造函数
indexing_record_(*schema_, index_meta_, segcore_config_, &insert_record_)

// IndexingRecord 持有引用
const Schema& schema_;  // 绑定到构造时的 Schema 对象
```

**问题流程**：

```
时间线：
T0: SegmentGrowingImpl 构造
    schema_ = shared_ptr<Schema>(version=1, fields={101, 102, 103})
    indexing_record_.schema_ 引用绑定到 version 1
    field_indexings_ 包含 {101, 102, 103}

T1: Schema 升级 (LazyCheckSchema → Reopen)
    std::unique_lock lck(sch_mutex_);
    schema_ = shared_ptr<Schema>(version=2, fields={101, 102, 103, 104})
    // ⚠️ IndexingRecord::schema_ 仍然引用 version 1 的 Schema 对象

T2: Insert 新数据（使用新 schema）
    for (field_id : schema_->get_fields()) {  // 遍历 version 2：{101,102,103,104}
        indexing_record_.AppendingIndex(field_id=104, ...) {
            if (!is_in(104)) return;  // field_indexings_ 中没有 104 ✓

            // 如果没有 is_in 检查，会执行到：
            auto field_meta = schema_.get_fields().at(104);
            //                └─ IndexingRecord::schema_ 是 version 1
            //                └─ ❌ std::out_of_range: key 104 not found!
        }
    }
```

### 2. 原始代码的漏洞

**FieldIndexing.cpp:44-47 (原始版本)**：

```cpp
void IndexingRecord::AppendingIndex(..., FieldId fieldId, ...) {
    if (!is_in(fieldId)) {
        return;
    }
    auto& indexing = field_indexings_.at(fieldId);  // ✓ 安全
    auto type = indexing->get_data_type();
    auto field_raw_data = record.get_data_base(fieldId);
    auto field_meta = schema_.get_fields().at(fieldId);  // ❌ 可能失败！
    //                └─ 如果 is_in 检查被移除或失效，这里会崩溃
}
```

**关键点**：
- `is_in(fieldId)` 检查的是 `field_indexings_` (初始化时创建，不会变)
- `schema_.get_fields().at(fieldId)` 访问的是构造时绑定的 Schema 对象
- 这两个容器在 schema 升级后会**不一致**

### 3. 触发条件

必须同时满足：
1. ✅ Schema 动态升级（`LazyCheckSchema` → `Reopen`）
2. ✅ 新增字段需要索引（vector 或 geometry 字段）
3. ✅ Insert 操作使用新 schema 的字段 ID
4. ✅ 代码中没有 `is_in()` 检查或检查失效

---

## 现有修复方案分析

**当前提交的修复**：

```cpp
std::shared_lock<std::shared_mutex> lock(field_indexings_mutex_);

if (!is_in(fieldId)) {
    return;  // ← 真正起作用的是这里
}

FieldIndexing* indexing_ptr = field_indexings_.at(fieldId).get();
auto field_meta = schema_.get_fields().at(fieldId);  // 因为提前返回，不会执行到
```

**评估**：
- ✅ **有效**：通过 `is_in()` 检查避免访问不存在的字段
- ⚠️ **过度设计**：加锁是不必要的（不存在并发写问题）
- ⚠️ **副作用**：新增字段不会被索引（静默忽略）

**为什么加锁是多余的**：
1. `field_indexings_` 只在构造函数的 `Initialize()` 中写入（单线程）
2. 运行时所有访问都是**纯读操作**
3. C++ 标准保证 `std::map` 和 `std::unordered_map` 的并发读是安全的
4. 真正的问题不是**并发竞态**，而是**版本不一致**

---

## 简洁修复方案

### 方案 1：防御性编程（推荐）

**FieldIndexing.cpp:38-100** 修改：

```cpp
void IndexingRecord::AppendingIndex(int64_t reserved_offset,
                               int64_t size,
                               FieldId fieldId,
                               const DataArray* stream_data,
                               const InsertRecord<false>& record) {
    // 检查字段是否有索引
    if (!is_in(fieldId)) {
        return;
    }

    auto& indexing = field_indexings_.at(fieldId);
    auto type = indexing->get_data_type();

    // 防御性检查：确保字段在当前 schema 中存在
    auto field_it = schema_.get_fields().find(fieldId);
    if (field_it == schema_.get_fields().end()) {
        LOG_WARN(
            "Field {} exists in field_indexings_ but not in schema, "
            "likely due to schema version mismatch. Skipping indexing.",
            fieldId.get());
        return;
    }

    auto field_meta = field_it->second;
    auto field_raw_data = record.get_data_base(fieldId);

    int64_t valid_count = reserved_offset + size;
    if (field_meta.is_nullable() && field_raw_data->is_mapping_storage()) {
        valid_count = field_raw_data->get_valid_count();
    }

    // 执行索引操作
    if (type == DataType::VECTOR_FLOAT &&
        valid_count >= indexing->get_build_threshold()) {
        indexing->AppendSegmentIndexDense(
            reserved_offset, size, field_raw_data,
            stream_data->vectors().float_vector().data().data());
    }
    // ... 其他类型处理
}
```

**修改点**：
1. 移除所有锁（不需要）
2. 使用 `find()` 代替 `at()`，避免抛异常
3. 添加日志记录异常情况
4. 第二个 `AppendingIndex` 重载同样修改

**优点**：
- ✅ 简洁，没有性能开销
- ✅ 防御性强，不会崩溃
- ✅ 有日志可追踪
- ✅ 符合"fail-safe"原则

---

### 方案 2：根本性修复（长期方案）

**问题根源**：IndexingRecord 持有过时的 Schema 引用

**修复方向**：

#### 选项 A：持有 SchemaPtr 而非引用

```cpp
// FieldIndexing.h:500
- const Schema& schema_;
+ SchemaPtr schema_;  // 使用 shared_ptr

// SegmentGrowingImpl::Reopen()
void SegmentGrowingImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(sch_mutex_);
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        schema_ = sch;
        indexing_record_.UpdateSchema(sch);  // 新增方法
    }
}
```

#### 选项 B：在 Reopen 时重建 IndexingRecord

```cpp
void SegmentGrowingImpl::Reopen(SchemaPtr sch) {
    std::unique_lock lck(sch_mutex_);
    if (sch->get_schema_version() > schema_->get_schema_version()) {
        auto absent_fields = sch->AbsentFields(*schema_);
        schema_ = sch;

        // 为新字段创建索引
        for (const auto& field_meta : *absent_fields) {
            if (should_create_index(field_meta)) {
                indexing_record_.AddFieldIndexing(field_meta);
            }
        }
    }
}
```

**评估**：
- ⚠️ 需要大量改动
- ⚠️ 需要完整测试
- ✅ 从根本上解决问题
- ✅ 支持动态 schema 演进

---

## 推荐实施路径

### 短期（v2.6 hotfix）

采用**方案 1**：
- 最小化改动
- 安全可靠
- 性能无损

### 长期（v3.0）

采用**方案 2**：
- 重构 Schema 管理
- 完整支持动态 schema
- 统一版本控制

---

## 修复验证

### 测试场景

1. **Schema 升级场景**
   ```python
   # 创建 collection (schema v1)
   collection.create_index(field="vector")

   # 动态添加字段 (schema v2)
   collection.add_field("new_text_field", enable_match=True)

   # 并发 insert + search
   # 预期：不崩溃，新字段跳过索引
   ```

2. **Tantivy Text Index 场景**
   ```python
   # Issue #46656 原始场景
   collection.create_index(field="text", index_type="INVERTED")
   # 并发 insert + compaction
   ```

3. **并发压力测试**
   ```bash
   # 10 线程并发 insert
   # 预期：无 race condition，无崩溃
   ```

---

## 文件修改清单

### 方案 1（推荐）

| 文件 | 修改内容 | 行数 |
|------|----------|------|
| `internal/core/src/segcore/FieldIndexing.cpp` | 两个 `AppendingIndex` 方法，使用 `find()` 代替 `at()` | ~20 行 |

### 方案 2（长期）

| 文件 | 修改内容 | 行数 |
|------|----------|------|
| `internal/core/src/segcore/FieldIndexing.h` | Schema 引用改为 SchemaPtr | ~5 行 |
| `internal/core/src/segcore/FieldIndexing.cpp` | 添加 UpdateSchema 方法 | ~50 行 |
| `internal/core/src/segcore/SegmentGrowingImpl.cpp` | Reopen 时更新 IndexingRecord | ~20 行 |

---

## 附录：调试建议

如果仍然遇到崩溃，检查以下方面：

1. **编译器 ABI 兼容性**
   ```bash
   # 确保所有模块使用相同的 C++ 标准库
   ldd milvus | grep libstdc++
   ```

2. **内存损坏检测**
   ```bash
   # 使用 AddressSanitizer 编译
   CXXFLAGS="-fsanitize=address" make
   ```

3. **Schema 版本日志**
   ```cpp
   LOG_INFO("AppendingIndex: fieldId={}, schema_version={}, field_exists={}",
            fieldId.get(),
            schema_.get_schema_version(),
            schema_.get_fields().count(fieldId));
   ```

---

**总结**：问题的本质是 Schema 版本演进导致的引用失效，而非并发竞态。简洁的修复是使用防御性检查，避免访问不存在的字段。
