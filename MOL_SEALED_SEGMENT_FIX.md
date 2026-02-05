# MOL Sealed Segment Bug 修复记录

## 日期
2026-02-04

## 修复的 Bug 列表

1. **Bug #1**: `MolChunkWriter::calculate_size` 没有计算行数和大小
2. **Bug #2**: `IsChunkedVariableColumnDataType` 没有包含 MOL 类型

---

## Bug #1: MolChunkWriter::calculate_size

### 问题描述

### 错误现象
当 MOL 类型数据 flush 到 sealed segment 后，重新加载 collection 时出现以下错误：

```
MilvusException: (code=2007, message=show collection failed: At Load:  
=> All columns should have the same number of rows at 
/home/zilliz/xiejh/milvus/internal/core/src/common/ChunkWriter.cpp:918)
```

### 问题影响
1. MOL 类型数据无法正确写入 sealed segment
2. Collection 无法在 flush 后重新加载
3. IVF 索引无法对 MOL 数据生效（一直使用 growing segment 的暴力搜索）

### 复现步骤
1. 创建包含 MOL Function 的 collection
2. 插入 MOL 数据
3. 执行 flush
4. 重新加载 collection → **报错**

## 根因分析

### 错误位置
`/home/zilliz/xiejh/milvus/internal/core/src/common/ChunkWriter.cpp`

### 问题代码
`MolChunkWriter::calculate_size` 函数存在 bug：

```cpp
// 修复前（错误代码）
std::pair<size_t, size_t>
MolChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        // BUG: 循环体内没有任何操作！
        // 没有计算 size
        // 没有更新 row_nums_
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_MOL_PADDING;
    return {size, row_nums_};  // row_nums_ 始终为 0！
}
```

### 问题原因
for 循环中只创建了 `array` 变量，但没有：
1. 遍历数组元素计算字符串大小 (`size`)
2. 累加行数 (`row_nums_`)

导致 `row_nums_` 始终返回 0，而其他字段（如 `mol_fp`）的行数正确，
触发了 ChunkWriter.cpp:918 的检查：
```cpp
if (row_nums != final_row_nums) {
    ThrowInfo(DataTypeInvalid,
              "All columns should have the same number of rows");
}
```

## 修复方案

### 修改文件
`/home/zilliz/xiejh/milvus/internal/core/src/common/ChunkWriter.cpp`

### 修改内容
参考 `StringChunkWriter::calculate_size` 的正确实现，修复 `MolChunkWriter::calculate_size`：

```cpp
// 修复后（正确代码）
std::pair<size_t, size_t>
MolChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
    row_nums_ = 0;
    size_t size = 0;
    for (const auto& data : array_vec) {
        auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
        // 遍历每个 MOL 字符串，累加其大小
        for (int64_t i = 0; i < array->length(); ++i) {
            auto str = array->GetView(i);
            size += str.size();
        }
        // 累加行数
        row_nums_ += array->length();
    }
    if (nullable_) {
        size += (row_nums_ + 7) / 8;
    }
    size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_MOL_PADDING;
    return {size, row_nums_};
}
```

### Diff
```diff
 std::pair<size_t, size_t>
 MolChunkWriter::calculate_size(const arrow::ArrayVector& array_vec) {
     row_nums_ = 0;
     size_t size = 0;
     for (const auto& data : array_vec) {
         auto array = std::dynamic_pointer_cast<arrow::BinaryArray>(data);
+        // 遍历每个 MOL 字符串，累加其大小
+        for (int64_t i = 0; i < array->length(); ++i) {
+            auto str = array->GetView(i);
+            size += str.size();
+        }
+        // 累加行数
+        row_nums_ += array->length();
     }
     if (nullable_) {
         size += (row_nums_ + 7) / 8;
     }
     size += sizeof(uint32_t) * (row_nums_ + 1) + MMAP_MOL_PADDING;
     return {size, row_nums_};
 }
```

## 验证方法

运行验证脚本：
```bash
cd /home/zilliz/xiejh/pymilvus/examples
python3 verify_sealed_segment_mol.py
```

预期结果：
1. Flush 后能成功加载 collection
2. Sealed segment 搜索正常工作
3. 不同 nprobe 值会影响 recall（体现 IVF 索引生效）

## 相关文件

| 文件 | 说明 |
|------|------|
| `internal/core/src/common/ChunkWriter.cpp` | 修复位置 |
| `internal/core/src/common/ChunkWriter.h` | 类定义 |
| `pymilvus/examples/verify_sealed_segment_mol.py` | 验证脚本 |

## 测试建议

1. 单元测试：添加 `MolChunkWriter` 的单元测试
2. 集成测试：测试 MOL 数据的 flush → load → search 流程
3. Recall 测试：验证 sealed segment + IVF 索引的 recall

---

## Bug #2: IsChunkedVariableColumnDataType 缺少 MOL 类型

### 错误现象
修复 Bug #1 后，在 sealed segment 上执行搜索时出现以下错误：

```
MilvusException: (code=65535, message=fail to search on QueryNode: 
ReduceSearchResultsAndFillData failed:  
=> [StorageV2] BulkRawStringAt only supported for ProxyChunkColumn of 
variable length type(except Json) at 
/home/zilliz/xiejh/milvus/internal/core/src/mmap/ChunkedColumnGroup.h:544)
```

### 错误位置
`/home/zilliz/xiejh/milvus/internal/core/src/mmap/ChunkedColumnInterface.h`

### 问题代码
```cpp
// 修复前
static bool
IsChunkedVariableColumnDataType(DataType data_type) {
    return data_type == DataType::STRING ||
           data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
           data_type == DataType::JSON || data_type == DataType::GEOMETRY;
    // 缺少 DataType::MOL
}
```

### 修复方案
```cpp
// 修复后
static bool
IsChunkedVariableColumnDataType(DataType data_type) {
    return data_type == DataType::STRING ||
           data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
           data_type == DataType::JSON || data_type == DataType::GEOMETRY ||
           data_type == DataType::MOL;  // 添加 MOL 类型
}
```

### Diff
```diff
 static bool
 IsChunkedVariableColumnDataType(DataType data_type) {
     return data_type == DataType::STRING ||
            data_type == DataType::VARCHAR || data_type == DataType::TEXT ||
-           data_type == DataType::JSON || data_type == DataType::GEOMETRY;
+           data_type == DataType::JSON || data_type == DataType::GEOMETRY ||
+           data_type == DataType::MOL;
 }
```

---

## 备注

这两个 bug 都是在添加 MOL 类型支持时遗漏了必要的代码：
1. Bug #1: 复制 StringChunkWriter 代码后遗漏了循环体内的逻辑
2. Bug #2: 忘记将 MOL 添加到变长类型列表中

建议检查其他新增数据类型是否存在类似问题。
