# Milvus Upsert 写链路开发总结

## 概述

本文档总结了 Milvus Upsert 功能在写链路（StreamingNode + Message Adaptor）的完整开发过程，包括遇到的挑战、解决方案和经验教训。

**开发时间**: 2026-01-23
**功能**: 支持 Upsert 消息在 StreamingNode 写链路中的处理
**核心改动**: 2 个文件修改 + 1 个测试文件新增

---

## 1. 分布式消息系统的复杂性认知

### 关键教训：消息可能被多层包装

最初只考虑了单个 Upsert 消息的情况，但实际生产中：
- **Proxy 会根据数据大小自动拆分**大的 upsert 请求
- 拆分后的多个 Upsert 消息会被包装在 **Txn 消息**中
- 这是在遇到 Test 2 panic (`"unsupported message type"`) 后才发现的关键点

### 消息流转路径

```
Client (Large Upsert Request)
  ↓
Proxy (拆分成多个 Upsert)
  ↓
Txn(Upsert1, Upsert2, ..., UpsertN)
  ↓
StreamingNode WAL
  ↓
Message Adaptor (parseTxnMsg 需要处理 Upsert)
  ↓
DataNode (接收 Delete + Insert)
```

### 经验总结

在分布式系统中，消息的传递路径往往比想象的复杂，需要考虑：
- **消息的嵌套层级**：Txn → Upsert → Delete/Insert
- **中间层的转换逻辑**：Proxy 拆分、StreamingNode 包装
- **不同场景下的消息形态**：小数据（单个 Upsert）vs 大数据（Txn 包装的多个 Upsert）

---

## 2. 测试驱动的问题发现

### 测试分层的重要性

#### 1. 单元测试 - 覆盖代码逻辑

创建了 `message_upsert_test.go`，包含 7 个测试场景：

| 测试场景 | 目的 | 发现的问题 |
|---------|------|-----------|
| successful_parse_with_both_delete_and_insert | 验证正常流程 | - |
| delete_with_no_numrows_fallback | 测试 NumRows 回退逻辑 | 需要支持 int64 主键 |
| missing_segment_assignment_error | 测试错误处理 | 错误信息需要更详细 |
| zero_segment_id_error | 测试边界条件 | - |
| nil_primary_keys_safety | 测试 nil 安全 | **发现 nil 指针风险** |
| string_primary_keys_fallback | 测试字符串主键 | 需要支持 string 主键回退 |
| single_upsert_message | 测试 MsgPack 转换 | - |

**覆盖率**: parseUpsertMsg 达到 **94.6%**

#### 2. 集成测试 - 发现系统级问题

创建了 `test_upsert.py`，包含 4 个端到端测试：

| 测试场景 | 数据规模 | 发现的问题 |
|---------|---------|-----------|
| Test 1: Basic Upsert | 1000 条，500 overlap + 500 new | `num_entities` 不准确（包含 deleted rows） |
| Test 2: Large Upsert | 10000 条，5000 overlap + 5000 new | **Panic: Txn 包装的 Upsert 未处理** |
| Test 3: Concurrent Upsert | 5 次连续 upsert | 验证并发场景 |
| Test 4: Upsert Overwrite | 1200 条，200 overlap + 200 new | 验证覆盖写语义 |

### 经验总结

- **单元测试**保证代码质量和边界条件处理
- **集成测试**才能暴露真实的系统行为（如 Txn 包装）
- **测试失败是好事**：Test 2 的 panic 帮助发现了关键的 parseTxnMsg 缺陷

---

## 3. 核心技术实现

### 3.1 Upsert 消息解析 (parseUpsertMsg)

**位置**: `pkg/streaming/util/message/adaptor/message.go:129-238`

**核心逻辑**:
```go
func parseUpsertMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
    // 1. 获取 UpsertMessage 的 body (包含 DeleteRequest 和 InsertRequest)
    body, err := upsertMsg.Body()

    // 2. 解析 Delete 部分
    if body.GetDeleteRequest() != nil {
        deleteMsg := &msgstream.DeleteMsg{...}

        // NumRows 回退逻辑（支持 int64 和 string 主键）
        numRows := int(deleteReq.GetNumRows())
        if numRows == 0 {
            primaryKeys := deleteReq.GetPrimaryKeys()
            if primaryKeys != nil {  // 关键：nil 检查
                if primaryKeys.GetIntId() != nil {
                    numRows = len(primaryKeys.GetIntId().GetData())
                } else if primaryKeys.GetStrId() != nil {
                    numRows = len(primaryKeys.GetStrId().GetData())
                }
            }
        }

        tsMsgs = append(tsMsgs, deleteMsg)
    }

    // 3. 解析 Insert 部分（从 header 获取 SegmentID）
    if body.GetInsertRequest() != nil {
        insertMsg := &msgstream.InsertMsg{...}

        // 从 partition assignment 中获取 segment ID
        assignment := findPartitionAssignment(header, insertMsg.GetPartitionID())
        insertMsg.SegmentID = assignment.GetSegmentAssignment().GetSegmentId()

        tsMsgs = append(tsMsgs, insertMsg)
    }

    return tsMsgs, nil  // 返回 [DeleteMsg, InsertMsg]
}
```

**关键改进**:
1. ✅ 添加 `primaryKeys != nil` 检查，防止 panic
2. ✅ 支持 int64 和 string 主键的 NumRows 回退计算
3. ✅ 详细的错误信息（包含 partition ID 和 segment ID）

### 3.2 Txn 消息处理 (parseTxnMsg)

**位置**: `pkg/streaming/util/message/adaptor/message.go:90-124`

**核心改进**:
```go
func parseTxnMsg(msg message.ImmutableMessage) ([]msgstream.TsMsg, error) {
    tsMsgs := make([]msgstream.TsMsg, 0)

    err := txnMsg.RangeOver(func(im message.ImmutableMessage) error {
        // ========== 新增：处理 Txn 中的 Upsert 消息 ==========
        if im.MessageType() == message.MessageTypeUpsert {
            // 当 upsert 数据过大时，proxy 会拆分成多个 upsert 消息
            // 并包装在 Txn 消息中
            upsertTsMsgs, err := parseUpsertMsg(im)
            if err != nil {
                return err
            }
            tsMsgs = append(tsMsgs, upsertTsMsgs...)
            return nil
        }
        // ====================================================

        // 处理其他消息类型
        tsMsg, err := parseSingleMsg(im)
        if err != nil {
            return err
        }
        tsMsgs = append(tsMsgs, tsMsg)
        return nil
    })

    return tsMsgs, nil
}
```

**为什么需要这个改动**:
- Proxy 在处理大数据 upsert 时，会将单个大请求拆分为多个 Upsert 消息
- 这些 Upsert 消息会被包装在一个 Txn 消息中传递给 StreamingNode
- 如果不处理，会触发 `panic("unsupported message type")`

### 3.3 Segment 分配 (shard_interceptor.go)

**位置**: `internal/streamingnode/server/wal/interceptors/shard/shard_interceptor.go:282`

**核心改进**:
```go
case message.MessageTypeUpsert:
    upsertMsg, _ := message.AsImmutableUpsertMessageV2(msg)
    header := upsertMsg.Header()

    // 为 upsert 的 insert 部分分配 segment
    if err := s.assignSegmentForUpsert(ctx, header); err != nil {
        return nil, err
    }

    // ========== 新增：覆盖 header，传递 segment 分配信息 ==========
    upsertMsg.OverwriteHeader(header)
    // ============================================================

    return upsertMsg.IntoImmutableMessage(msg.MessageID()), nil
```

**为什么需要 OverwriteHeader**:
- Segment 分配信息写入到 `header.Partitions[].SegmentAssignment`
- 必须调用 `OverwriteHeader` 才能将修改后的 header 持久化到消息中
- 否则下游的 Message Adaptor 无法获取 SegmentID

---

## 4. 代码审查的价值

通过系统性的代码审查发现了 3 个重要问题：

### 问题 1: Nil 指针风险

**位置**: `message.go:173-180`

**原始代码**:
```go
if deleteReq.GetPrimaryKeys().GetIntId() != nil {
    numRows = len(deleteReq.GetPrimaryKeys().GetIntId().GetData())
}
```

**问题**: 如果 `GetPrimaryKeys()` 返回 nil，会触发 panic

**修复**:
```go
primaryKeys := deleteReq.GetPrimaryKeys()
if primaryKeys != nil {
    if primaryKeys.GetIntId() != nil {
        numRows = len(primaryKeys.GetIntId().GetData())
    } else if primaryKeys.GetStrId() != nil {
        numRows = len(primaryKeys.GetStrId().GetData())
    }
}
```

### 问题 2: 字符串主键支持不完整

**发现**: NumRows 回退逻辑只支持 int64 主键，但 Milvus 也支持字符串主键

**修复**: 添加 `else if primaryKeys.GetStrId() != nil` 分支

### 问题 3: 错误信息不够详细

**改进**: 所有错误信息都包含上下文信息（partition ID、segment ID）

```go
return nil, errors.Errorf(
    "segment id is 0 for partition %d in upsert message",
    insertMsg.GetPartitionID(),
)
```

### 经验总结

- 写完代码后主动做一遍系统性审查很有价值
- 审查时要特别关注：nil 安全、边界条件、错误路径
- 代码审查可以在提交前就发现潜在问题，避免生产事故

---

## 5. Milvus 写链路架构理解

### 5.1 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Client Layer                         │
│                      (pymilvus SDK)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │ gRPC: Upsert(data)
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                        Proxy Layer                           │
│  - 数据验证、分区路由                                          │
│  - 大数据拆分（生成 Txn(Upsert, Upsert, ...)）                │
└─────────────────────┬───────────────────────────────────────┘
                      │ Message Stream
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                    StreamingNode (WAL)                       │
│  ┌───────────────────────────────────────────────────────┐  │
│  │            Interceptor Chain                          │  │
│  │  1. TimeTick Interceptor                             │  │
│  │  2. Shard Interceptor  ← Segment 分配在这里            │  │
│  │  3. Txn Interceptor                                  │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │              WAL (Pulsar/Kafka)                       │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────┬───────────────────────────────────────┘
                      │ Message Stream
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                   Message Adaptor Layer                      │
│  - 将 V2 streaming message 转换为 V1 msgstream TsMsg         │
│  - parseUpsertMsg: Upsert → [DeleteMsg, InsertMsg]          │
│  - parseTxnMsg: 处理 Txn 包装的 Upsert                        │
└─────────────────────┬───────────────────────────────────────┘
                      │ MsgStream
                      ↓
┌─────────────────────────────────────────────────────────────┐
│                      DataNode Layer                          │
│  - 接收 DeleteMsg 和 InsertMsg                               │
│  - 写入 binlog，更新 segment                                  │
│  - 无需感知 Upsert 的存在                                      │
└─────────────────────────────────────────────────────────────┘
```

### 5.2 关键设计模式

#### Interceptor 模式

- **职责分离**: 每个拦截器负责一个特定功能
  - TimeTick: 时间戳分配
  - Shard: Segment 分配
  - Txn: 事务边界管理

- **链式处理**: 消息依次通过每个拦截器
- **元数据增量补充**: 每层拦截器补充必要的元数据

#### Adaptor 模式

- **目的**: 新旧系统兼容
  - 新系统: V2 streaming message（ImmutableMessage）
  - 旧系统: V1 msgstream（TsMsg）

- **转换方式**:
  - Upsert → [Delete, Insert]
  - Txn → 展开内部消息
  - 其他消息类型 1:1 转换

#### 消息不可变性 (Immutability)

- **设计**: `ImmutableMessage` 一旦创建不可修改
- **修改方式**: 通过 `OverwriteHeader()` 创建新的消息实例
- **好处**: 线程安全、便于调试、避免副作用

---

## 6. Segment 分配机制

### 6.1 为什么 Upsert 的 Insert 部分需要 SegmentID？

Milvus 的数据组织方式：
```
Collection
  ├── Partition 1
  │     ├── Segment 1 (Growing)  ← 新数据写入这里
  │     ├── Segment 2 (Sealed)
  │     └── Segment 3 (Sealed)
  └── Partition 2
        └── ...
```

- **Growing Segment**: 活跃的、可写入的 segment
- **Sealed Segment**: 已关闭的、不可写入的 segment（只读）

Insert 操作必须指定写入哪个 Growing Segment，因此需要 SegmentID。

### 6.2 分配时机

**在 ShardInterceptor 中分配** (`shard_interceptor.go:assignSegmentForUpsert`)

```go
func (s *shardInterceptor) assignSegmentForUpsert(
    ctx context.Context,
    header *messagespb.UpsertMessageHeader,
) error {
    insertReq := header.GetInsertRequest()

    // 获取 partition 的 growing segments
    segmentAllocations := s.segmentAssignManager.AssignSegment(...)

    // 将 segment 分配信息写入 header
    for _, partition := range header.Partitions {
        partition.SegmentAssignment = &messagespb.SegmentAssignment{
            SegmentId: allocatedSegmentID,
        }
    }

    return nil
}
```

### 6.3 信息传递

```
ShardInterceptor.assignSegment()
  ↓ 修改 header
header.Partitions[].SegmentAssignment.SegmentId = 100
  ↓ 必须调用 OverwriteHeader()
upsertMsg.OverwriteHeader(header)
  ↓ 消息进入 WAL
WAL.Append(upsertMsg)
  ↓ 消息被读取
Message Adaptor.parseUpsertMsg()
  ↓ 从 header 获取 SegmentID
insertMsg.SegmentID = header.Partitions[0].SegmentAssignment.SegmentId
```

**关键点**: 如果忘记调用 `OverwriteHeader()`，下游将无法获取 SegmentID！

---

## 7. MVCC 和 num_entities 问题

### 7.1 问题现象

Test 1 中观察到：
```
Initial insert: 1000 entities
Upsert: 500 overlap + 500 new = 1500 expected
collection.num_entities = 2000  ← 为什么？
```

### 7.2 根本原因

Milvus 使用 MVCC (Multi-Version Concurrency Control):

```
Timeline:
         T1                T2
         ↓                 ↓
Insert:  [0-999]          Upsert: delete [500-999]
                                  insert [500-1499]

Sealed Segment (T1):
  [0-999]  ← 这些数据仍然存在于 segment 中
  其中 [500-999] 被标记为删除（通过 bloom filter）

Growing Segment (T2):
  [500-1499]  ← 新写入的数据

collection.num_entities 计算：
  Sealed: 1000 (包含已删除的行)
  Growing: 1000
  Total: 2000 ← 重复计数了 [500-999]
```

### 7.3 解决方案

**方法 1: 使用 Query 查询实际存在的行数**
```python
all_results = collection.query(expr="id >= 0", output_fields=["id"], limit=2000)
actual_count = len(all_results)  # 1500 ✓ 准确
```

**方法 2: 等待 Compaction**
```python
collection.compact()  # 物理删除被标记删除的行
collection.wait_for_compaction_completed()
collection.num_entities  # 1500 ✓ 准确
```

### 7.4 经验总结

- `num_entities` 在 sealed segment 中会包含已删除的行（直到 compaction）
- 需要准确计数时，使用 query 而不是 `num_entities`
- 这是 MVCC 系统的正常行为，不是 bug

---

## 8. 测试覆盖率策略

### 8.1 覆盖率数据

| 范围 | 覆盖率 | 说明 |
|-----|--------|------|
| parseUpsertMsg 函数 | 94.6% | 单元测试覆盖 |
| parseTxnMsg (Upsert 部分) | 集成测试 | test_upsert.py Test 2 |
| 整个 adaptor 包 | 46.7% | 包含很多不相关的旧代码 |

### 8.2 测试策略

#### 单元测试：聚焦新增代码

```go
// message_upsert_test.go
// 7 个测试用例，覆盖所有重要路径：
1. 正常流程（delete + insert）
2. NumRows 回退（int64 主键）
3. NumRows 回退（string 主键）
4. 错误：缺少 partition assignment
5. 错误：segment ID = 0
6. Nil 安全：PrimaryKeys = nil
7. MsgPack 转换
```

#### 集成测试：验证端到端流程

```python
# test_upsert.py
# 4 个场景，覆盖真实使用情况：
Test 1: 基础 upsert（验证覆盖写语义）
Test 2: 大数据 upsert（触发 Txn 包装）← parseTxnMsg 在这里被测试
Test 3: 连续 upsert（验证并发场景）
Test 4: 覆盖写验证（验证 MVCC 行为）
```

### 8.3 为什么不追求 100% 覆盖率？

**务实的选择**:

1. **Txn 消息构建复杂**
   - 需要 BeginTxn + 内部消息 + CommitTxn
   - 单元测试中模拟成本高
   - 集成测试已经覆盖（Test 2）

2. **成本收益分析**
   - 94.6% → 100%：需要额外 3-4 个测试用例
   - 新增覆盖的代码：主要是错误分支和 panic 路径
   - ROI 较低

3. **测试金字塔原则**
   ```
        /\        E2E Tests (少量，覆盖关键流程)
       /  \
      /    \      Integration Tests (适量，验证组件协作)
     /      \
    /________\    Unit Tests (大量，覆盖代码逻辑)
   ```

### 8.4 经验总结

- 覆盖率应该聚焦于**新增代码**，而非整个包
- 复杂的集成场景在集成测试中验证更高效
- 单元测试应该覆盖所有重要的代码路径和边界条件
- 不要为了数字而测试，要为了质量而测试

---

## 9. 调试经验

### 9.1 典型问题清单

| 问题类型 | 表现 | 根本原因 | 解决方案 |
|---------|------|---------|---------|
| Panic | "unsupported message type" | parseTxnMsg 缺少 Upsert 处理 | 添加 Upsert 分支 |
| 断言失败 | num_entities = 2000 != 1500 | MVCC 包含已删除行 | 使用 query 而非 num_entities |
| 编译错误 | undefined: message.UpsertRequestBody | 类型在 messagespb 包而非 message 包 | 导入正确的包 |
| Nil Panic | 访问 PrimaryKeys 字段 | GetPrimaryKeys() 可能返回 nil | 添加 nil 检查 |
| SegmentID = 0 | Insert 失败 | 忘记调用 OverwriteHeader | 调用 OverwriteHeader() |

### 9.2 调试技巧

#### 1. 错误信息要包含上下文

**Bad**:
```go
return nil, errors.New("segment assignment not found")
```

**Good**:
```go
return nil, errors.Errorf(
    "partition assignment not found for partition %d in upsert message, "+
    "collection: %d, available partitions: %v",
    insertMsg.GetPartitionID(),
    header.GetCollectionId(),
    getPartitionIDs(header.Partitions),
)
```

#### 2. 理解底层机制

- **MVCC**: 解释 num_entities 的"不准确"
- **Compaction**: 解释何时物理删除数据
- **Sealed Segment**: 解释为什么已删除的行仍然占用空间

#### 3. 逐层排查

```
问题：Upsert 失败

Step 1: 检查 Client 发送的数据
  ↓ pymilvus SDK
Step 2: 检查 Proxy 是否正确拆分
  ↓ Proxy logs
Step 3: 检查 StreamingNode 是否正确分配 Segment
  ↓ Shard Interceptor logs
Step 4: 检查 Message Adaptor 是否正确解析
  ↓ Adaptor logs
Step 5: 检查 DataNode 是否正确写入
  ↓ DataNode logs
```

#### 4. 单元测试优先

- 遇到问题先写单元测试复现
- 单元测试运行快，迭代快
- 修复后单元测试就是回归测试

---

## 10. Git 工作流

### 10.1 Commit 历史

```
84a88b0 test: add comprehensive unit tests for upsert message parsing
e7d159d enhance: complete upsert message support with Txn handling
2bc537f temp storage
```

### 10.2 Commit Message 规范

#### Commit 1: 核心功能实现

```
enhance: complete upsert message support with Txn handling

Implement full upsert message processing in StreamingNode write path:

1. Message Adaptor (pkg/streaming/util/message/adaptor/message.go):
   - parseUpsertMsg: Convert Upsert to Delete + Insert with proper
     segment ID from header
   - parseTxnMsg: Add special handling for Upsert messages inside Txn
     (when proxy splits large upsert data into multiple upsert messages)
   - Improved error messages with detailed context

2. Shard Interceptor (internal/streamingnode/.../shard_interceptor.go):
   - assignSegmentForUpsert: Reuse existing segment allocation logic
   - OverwriteHeader: Propagate segment assignment back to message

Key design decisions:
- Upsert = Delete + Insert semantics (MVCC compatible)
- Segment assignment happens at StreamingNode Shard Interceptor
- Support both single Upsert and Txn(Upsert, ...) scenarios

Tested with:
- test_upsert.py Test 2 (large data → Txn splitting)

Generated with [Claude Code](https://claude.ai/code)
via [Happy](https://happy.engineering)

Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: Happy <yesreply@happy.engineering>
```

#### Commit 2: 测试补充

```
test: add comprehensive unit tests for upsert message parsing

Add unit tests for parseUpsertMsg function with 94.6% code coverage:
- Test successful parsing of upsert message into delete + insert
- Test NumRows fallback calculation from int64 and string primary keys
- Test error handling for missing/invalid segment assignments
- Test nil safety for PrimaryKeys field
- Test MsgPack conversion from upsert messages

Also improve nil safety in parseUpsertMsg by adding proper null checks
before accessing PrimaryKeys fields to prevent potential panics.

Test coverage:
- parseUpsertMsg: 94.6% (7 test cases covering all major paths)
- Integration test coverage: parseTxnMsg with Upsert messages tested
  via test_upsert.py Test 2 (large upsert with message splitting)

Generated with [Claude Code](https://claude.ai/code)
via [Happy](https://happy.engineering)

Co-Authored-By: Claude <noreply@anthropic.com>
Co-Authored-By: Happy <yesreply@happy.engineering>
```

### 10.3 经验总结

- **功能实现和测试分开提交**：便于 review
- **Commit message 要清晰**：
  - 第一行：简洁描述（50 字符内）
  - 正文：详细说明 what、why、how
  - 包含测试覆盖率数据
  - 关键设计决策的说明
- **引用相关测试**：说明在哪里测试了该功能

---

## 11. 最大的收获

### 11.1 系统思维的建立

**一个 Upsert 操作涉及的组件**:

```
1. Client (pymilvus SDK)      - 发起请求
2. Proxy                       - 验证、路由、拆分
3. StreamingNode               - WAL、拦截器链
4. Shard Interceptor           - Segment 分配
5. Message Adaptor             - 消息格式转换
6. DataNode                    - 数据持久化
7. QueryNode                   - 数据查询
```

**消息的多次转换**:

```
UpsertRequest (Client)
  → Txn(Upsert, Upsert, ...) (Proxy 拆分)
  → UpsertMessage with SegmentID (StreamingNode)
  → [DeleteMsg, InsertMsg] (Adaptor)
  → Binlog (DataNode)
```

**每一层都有自己的抽象和职责**:
- Proxy: 业务逻辑层（验证、路由）
- StreamingNode: 可靠性层（WAL、持久化）
- Adaptor: 兼容性层（新旧格式转换）
- DataNode: 存储层（数据组织）

### 11.2 工程实践的积累

**测试驱动的开发循环**:

```
1. 先写集成测试 (test_upsert.py)
   ↓
2. 运行测试，发现问题 (Panic)
   ↓
3. 修复问题 (添加 Txn Upsert 处理)
   ↓
4. 代码审查，发现潜在风险 (nil 检查)
   ↓
5. 补充单元测试 (message_upsert_test.go)
   ↓
6. 达到高覆盖率 (94.6%)
```

**质量保障的多层防护**:

```
Level 1: 单元测试 (7 test cases)
  → 覆盖代码逻辑、边界条件、错误路径

Level 2: 集成测试 (4 scenarios)
  → 验证端到端流程、发现系统级问题

Level 3: 代码审查 (Self Review)
  → 发现潜在风险、改进错误处理

Level 4: 生产验证 (未来)
  → 监控、告警、灰度发布
```

### 11.3 分布式系统的复杂性

**消息的多层嵌套**:
- 看似简单的 Upsert，实际可能是 `Txn(Upsert(Delete, Insert))`
- 每一层都可能对消息进行包装或拆解

**元数据的分阶段补充**:
- SegmentID 不是一开始就有的
- 在 ShardInterceptor 中分配
- 通过 OverwriteHeader 传递给下游

**MVCC 带来的计数不一致**:
- `num_entities` 包含已删除的行（Sealed Segment）
- 需要理解 MVCC 机制才能解释

**兼容性设计的权衡**:
- V2 消息需要转换为 V1 格式（Adaptor）
- Upsert 拆解为 Delete + Insert（语义等价）
- 新功能不能破坏旧系统（向后兼容）

### 11.4 核心领悟

> **在分布式系统中，"简单"的功能往往需要多个组件的精心配合。**
>
> **测试的完整性和代码的健壮性同样重要。**
>
> **理解底层机制（MVCC、Compaction、消息流转）是解决问题的关键。**

---

## 12. 文件清单

### 修改的文件

1. **internal/streamingnode/server/wal/interceptors/shard/shard_interceptor.go**
   - 添加 Upsert case 处理
   - 调用 `assignSegmentForUpsert`
   - 添加 `OverwriteHeader()` 调用

2. **pkg/streaming/util/message/adaptor/message.go**
   - 实现 `parseUpsertMsg` 函数（129-238 行）
   - 增强 `parseTxnMsg` 处理 Upsert（101-109 行）
   - 添加 nil 安全检查和字符串主键支持

### 新增的文件

3. **pkg/streaming/util/message/adaptor/message_upsert_test.go**
   - 7 个单元测试用例
   - 94.6% 的 parseUpsertMsg 覆盖率
   - 覆盖正常流程、错误场景、边界条件

### 测试文件（未提交）

4. **test_upsert.py**
   - 4 个端到端集成测试
   - 验证小数据、大数据、并发、覆盖写场景

---

## 13. 后续建议

### 13.1 监控指标

建议在生产环境添加以下监控：

```
1. Upsert 消息处理延迟
   - parseUpsertMsg 耗时
   - Segment 分配耗时

2. Upsert 消息统计
   - 单个 Upsert vs Txn 包装的 Upsert 比例
   - 平均每个 Txn 包含的 Upsert 数量

3. 错误监控
   - Segment 分配失败率
   - Nil pointer 异常（如果发生）
```

### 13.2 性能优化方向

```
1. Batch Segment 分配
   - 当前：每个 partition 单独分配
   - 优化：同一个 collection 的多个 partition 批量分配

2. Header 复制优化
   - 当前：OverwriteHeader 可能涉及深拷贝
   - 优化：Copy-on-Write 或 Immutable 优化

3. Message Pool
   - 当前：每次创建新的 DeleteMsg/InsertMsg
   - 优化：使用对象池减少 GC 压力
```

### 13.3 功能增强

```
1. 支持 Batch Upsert API
   - 一次请求 upsert 多个 collection
   - 减少网络往返次数

2. Upsert 统计信息
   - 返回实际 inserted 和 updated 的行数
   - 提供更详细的操作反馈

3. Conditional Upsert
   - 支持条件更新（类似 CAS）
   - 提供更强的一致性保证
```

---

## 14. 参考资源

### Milvus 架构文档
- [Architecture Overview](https://milvus.io/docs/architecture_overview.md)
- [Write Path](https://milvus.io/docs/write_path.md)
- [Data Model](https://milvus.io/docs/data_model.md)

### 相关代码
- StreamingNode: `internal/streamingnode/`
- Message Adaptor: `pkg/streaming/util/message/adaptor/`
- Shard Interceptor: `internal/streamingnode/server/wal/interceptors/shard/`
- Message Definitions: `pkg/proto/messagespb/`

### 测试
- 单元测试: `pkg/streaming/util/message/adaptor/message_upsert_test.go`
- 集成测试: `test_upsert.py`

---

## 作者

开发时间: 2026-01-23
开发者: @chyezh
AI 助手: Claude (Anthropic)

---

**最后更新**: 2026-01-23
