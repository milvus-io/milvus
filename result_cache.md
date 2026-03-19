# ExprCache 磁盘持久化设计文档

## 1. 背景与目标

### 1.1 现状

`ExprResCacheManager` 缓存表达式计算结果（bitset），避免对同一 sealed segment 重复计算相同表达式。

- **缓存对象**: 表达式计算产出的 `result` bitset + `valid_result` bitset
- **使用场景**: 仅 sealed segment 上的 MatchExpr（文本/短语匹配），由 `UnaryExpr.cpp` 调用
- **Key**: `(segment_id, signature)`，其中 signature 是表达式的 `ToString()` 字符串
- **Go 侧集成**: segment 删除时通过 CGO 调用 `ExprResCacheEraseSegment` 清理对应缓存

### 1.2 原方案问题

原方案为纯内存 LRU 缓存（TBB `concurrent_unordered_map`），**全局共享** 256MB 容量（单一 atomic 计数器追踪所有 entry）。对于 1M 行 segment，每个 bitset 约 125KB，256MB 全局仅能容纳约 2000 个 cache entry，命中率受限。

### 1.3 新方案目标

1. **两种独立模式** — 通过配置切换 Memory 模式和 Disk 模式（config switch，非同时运行）
2. **不使用 mmap** — Memory 模式使用 malloc；Disk 模式使用 pread/pwrite。彻底消除 page fault 不可预测性
3. **Entry 级淘汰** — Clock 算法（近似 LRU），两种模式均支持，不再整段丢失
4. **可预测延迟** — Get/Put 延迟确定性高，无隐藏 page fault
5. **高效的 Segment 清理** — Segment 释放时 O(1) 删除对应文件/条目

---

## 2. 总体架构

```
ExprResCacheManager (singleton, unified entry point)
  |
  +-- config: mode = "memory" | "disk"
  +-- public: Put / Get / EraseSegment / Clear
  +-- shared: active_count staleness / sig_hash + signature exact match
  |
  +-- Memory Mode
  |     +-- EntryPool (malloc + Clock eviction)
  |     +-- Compressed storage (Roaring/Raw adaptive)
  |     +-- Admission: frequency filter + latency filter
  |     +-- Capacity: maxBytes (e.g. 2GB)
  |
  +-- Disk Mode
        +-- Per-segment DiskSlotFile (pread/pwrite, fixed-size slots)
        +-- No compression, Raw bitset direct store
        +-- Only sealed segments cached (growing segments skipped)
        +-- Clock eviction (in-memory index tracks hotness)
        +-- Admission: latency filter only (no frequency filter)
        +-- Capacity: maxFileSizeBytes per segment
```

### 2.1 核心设计决策

| 设计点 | 决策 | 原因 |
|--------|------|------|
| 模式 | Memory + Disk 双模式，配置切换 | 不同部署场景（RAM-rich vs RAM-constrained）各有最优解 |
| Memory 模式存储 | malloc + Clock eviction | 无 mmap page fault，延迟可预测，entry 级淘汰 |
| Disk 模式存储 | pread/pwrite + fixed-size slots | 无 mmap，系统调用延迟确定性高，slot 对齐便于覆写 |
| Disk 模式文件结构 | Per-segment DiskSlotFile | Segment 删除 = close(fd) + unlink = O(1) |
| Memory 模式压缩 | 自适应（Roaring V2 / RoarInv / Raw） | 稀疏 113x，密度 30%-70% 直接 Raw |
| Disk 模式压缩 | 无压缩，Raw bitset 直存 | 磁盘空间充裕，省 CPU，slot 固定大小 |
| 淘汰 | Clock 算法（两种模式均支持） | 近似 LRU，entry/slot 级淘汰，无需整段丢失 |
| 准入控制 | Memory: 频率 + 延迟；Disk: 仅延迟 | 磁盘空间便宜，不需要频率过滤 |
| Key 匹配 | sig_hash (XXH64) + signature 全字符串比较 | hash 做桶定位，全串比较确保零误判 |
| 重启策略 | 清空旧文件重建 | 无需恢复逻辑，启动快，设计简单 |

---

## 3. 代码结构

```
internal/core/src/exec/expression/
├── ExprCache.h / .cpp          # ExprResCacheManager — 全局管理器，模式分发 (memory/disk)
├── EntryPool.h / .cpp          # Memory 模式：malloc + Clock eviction + 压缩存储
├── DiskSlotFile.h / .cpp       # Disk 模式：per-segment pread/pwrite + fixed-size slots
├── CacheCompressor.h / .cpp    # 自适应压缩/解压（Roaring V2 / RoarInv / Raw，仅 Memory 模式使用）
├── ExprCacheHelper.h           # GetOrCompute / BatchedCachedMixin（上层接口，不感知存储后端）
├── ExprCacheTest.cpp           # 单元测试
└── bench_expr_cache.cpp        # 性能基准测试

pkg/util/paramtable/component_param.go   # 8 个配置项
internal/util/initcore/query_node.go      # CGO 传递配置
internal/core/src/common/init_c.h/.cpp    # CGO 接口
```

### 3.1 类职责

| 类 | 职责 |
|----|------|
| `ExprResCacheManager` | 全局单例。根据 mode 配置分发到 EntryPool 或 DiskSlotFile，准入控制，提供 `Put/Get/EraseSegment/Clear` 外部接口 |
| `EntryPool` | Memory 模式存储后端。malloc 分配 entry，Clock 算法淘汰，HashMap 索引，compressed payload |
| `DiskSlotFile` | Disk 模式存储后端。per-segment 文件，pread/pwrite，fixed-size slots，in-memory index + Clock 淘汰 |
| `CacheCompressor` | 无状态工具类。将 `result + valid` 两个 bitset 压缩为单个 buffer，根据密度自动选择 Roaring V2 / RoarInv / Raw（仅 Memory 模式使用） |
| `FrequencyTracker` | 轻量级频率计数器。16K 槽位的 direct-mapped 计数器数组，用于频率准入判断（仅 Memory 模式使用） |

---

## 4. 文件格式

### 4.1 目录结构

```
<localStorage.path>/expr_cache/<nodeID>/
├── seg_12345678.cache
├── seg_87654321.cache
└── ...
```

缓存路径由 `pathutil.GetPath(pathutil.ExprCachePath, nodeID)` 派生自 `localStorage.path`。文件数量 = 打开过的 segment 数量（通常 1K-10K）。

### 4.2 文件布局

```
File Header (32 bytes, packed):
┌──────────┬─────────┬────────────┬──────────┬─────────────────────────┐
│ magic 4B │ ver 4B  │ seg_id 8B  │ cnt 4B   │ reserved 12B            │
└──────────┴─────────┴────────────┴──────────┴─────────────────────────┘

Entry (变长, append-only):
┌──────────┬──────────┬───────────────┬───────────┬──────────┬──────────────┐
│ sig_hash │ sig_len  │ active_count  │ comp_type │ data_len │ compressed   │
│ 8B       │ 2B       │ 8B            │ 1B        │ 4B       │ variable     │
└──────────┴──────────┴───────────────┴───────────┴──────────┴──────────────┘
```

Entry header = 23 bytes。使用 `#pragma pack(push, 1)` 保证紧凑布局。

### 4.3 字段说明

**File Header (`CacheFileHeader`, 32 bytes):**

| 字段 | 大小 | 说明 |
|------|------|------|
| `magic` | 4B | `0x4D564543` ("MVEC")，用于校验文件格式 |
| `version` | 4B | 格式版本号，当前为 1 |
| `segment_id` | 8B | Segment ID |
| `entry_count` | 4B | Entry 数量 |
| `reserved[3]` | 12B | 预留字段，填 0 |

**Entry Header (`CacheEntryHeader`, 23 bytes):**

| 字段 | 大小 | 说明 |
|------|------|------|
| `sig_hash` | 8B | Expression signature 的 XXH64 哈希 |
| `sig_len` | 2B | Signature 字符串长度（碰撞检测） |
| `active_count` | 8B | 缓存时的活跃行数（过期检测） |
| `comp_type` | 1B | 压缩类型：`1` = Roaring, `2` = RoarInv（取反 Roaring）, `0xFF` = Raw |
| `data_len` | 4B | 压缩数据长度 |
| `compressed_data` | 变长 | 压缩后的 bitset 数据 |

### 4.4 压缩数据内部格式

`CacheCompressor` 将两个 bitset 编码为一个 buffer：

```
[result_bit_count (4B)] [valid_bit_count_with_flag (4B)] [payload]
```

- `result_bit_count`：result bitset 的位数
- `valid_bit_count_with_flag`：valid bitset 位数，最高位 (`kValidAllOnesMask = 0x80000000`) 标记 valid 是否全 1
- 存储 bit count（而非 byte count），确保解码后 bitset 大小精确还原
- payload 取决于 `comp_type`：
  - **Raw (0xFF)**：result + valid 的原始字节直接拼接（valid 全 1 时跳过）
  - **Roaring (1)**：`[result_roaring_size (4B)][result_roaring_bytes][valid_roaring_bytes]`
  - **RoarInv (2)**：同上，但 result 是取反后的 Roaring（解码时再次取反）

---

## 5. Cache Key 设计

**路由层**: `segment_id` → 对应的 `SegmentCacheFile`（per-segment 文件隔离）

**文件内 Key**: `sig_hash` (8B) + `sig_len` (2B)

- `sig_hash` = `XXH64(signature)`，signature 是表达式的 `ToString()` 字符串
- `sig_len` = signature 字符串长度
- in-memory index 仅用 `sig_hash` 做 `unordered_map<uint64_t, EntryLoc>` 查找

**碰撞概率**: sig_hash 碰撞 1/2^64，碰撞且 sig_len 相同 ~1/2^74。单个 segment 内通常只有几百到几千个表达式，实际碰撞不可能发生。读取时若 sig_hash 匹配但 sig_len 不匹配，返回 cache miss。

---

## 6. 过期检测

### 6.1 active_count 校验

每个 entry 存储缓存时的 `active_count`（segment 活跃行数）。Get 时比较存储值与调用方传入的当前 `active_count`，不匹配则返回 cache miss。

这捕获 segment compaction 或行数变更导致的缓存过期。

---

## 7. 核心操作流程

### 7.1 Put（写入）

```
Put(segment_id, signature, result, valid, active_count, eval_duration_us)
  │
  ├─ eval_duration_us < min_eval_duration_us_?
  │   └─ YES → return (计算太快，缓存收益为负)
  │
  ├─ sig_hash = XXH64(signature)
  ├─ sig_len = len(signature)
  │
  ├─ frequency_tracker_.RecordAndCheck(sig_hash, threshold)
  │   └─ 未达阈值 → return (不缓存，开销 < 10ns)
  │
  ├─ GetOrCreateFile(segment_id)
  │   ├─ FindFile: files_mutex_ shared_lock → 查找 (fast path)
  │   └─ 未找到 → files_mutex_ unique_lock → 创建 SegmentCacheFile (slow path)
  │
  ├─ file->Contains(sig_hash)           // shared_lock, 幂等检查
  │   └─ 已存在 → TouchLRU → return
  │
  ├─ CacheCompressor::Compress(...)     // 锁外执行, ~300μs
  │
  ├─ 检查 file_size + entry_size > max_segment_file_size
  │   └─ 超限 → total_size_ -= old_data, file->Reset()
  │
  ├─ file->Append(...)                  // unique_lock, 仅 ~10μs (memcpy)
  ├─ total_size_ += (post_size - pre_size)
  │
  ├─ TouchSegmentLRU(segment_id)
  └─ EvictIfNeeded()
```

**关键优化**: 压缩操作（~300μs）在任何文件锁之外执行。Append 持写锁仅做 memcpy + index 更新（~10μs），大幅降低写锁持有时间。

### 7.2 Get（读取）

```
Get(segment_id, signature, active_count) → (result, valid)
  │
  ├─ sig_hash = XXH64(signature)
  ├─ sig_len = len(signature)
  │
  ├─ FindFile(segment_id)               // files_mutex_ shared_lock
  │   └─ 未找到 → return false (无懒加载)
  │
  ├─ file->Get(sig_hash, sig_len, active_count, ...)  // shared_lock
  │   ├─ index 未命中 → return false
  │   ├─ 边界校验: offset + header + data_len > file_size → return false
  │   ├─ sig_len 不匹配 → return false (hash 碰撞)
  │   ├─ active_count 不匹配 → return false (已过期)
  │   └─ CacheCompressor::Decompress → out_result, out_valid
  │
  ├─ 包装为 shared_ptr<TargetBitmap>
  ├─ TouchSegmentLRU(segment_id)
  └─ return true
```

### 7.3 EraseSegment（删除 Segment）

```
EraseSegment(segment_id)
  │
  ├─ files_mutex_ unique_lock
  │   ├─ total_size_ -= file->GetFileSize()
  │   └─ move out unique_ptr, 从 map 中删除
  │
  ├─ 释放 files_mutex_
  │
  ├─ file.reset()        // 析构: munmap + close(fd)
  ├─ unlink(path)        // 删除磁盘文件, O(1)
  └─ RemoveFromSegmentLRU(segment_id)
```

**关键**: `munmap` 和 `unlink` 在释放 `files_mutex_` 之后执行，避免阻塞其他 segment 的 Get/Put 操作。

### 7.4 Clear（清空全部）

1. 收集所有 segment ID
2. 逐个调用 `EraseSegment`
3. 扫描磁盘目录，删除残留的 `.cache` 文件（处理尚未加载到内存的孤儿文件）

### 7.5 SetDiskConfig（初始化）

```
SetDiskConfig(base_path, max_total_size, max_segment_file_size,
              compression_enabled, admission_threshold, min_eval_duration_us)
  │
  ├─ 保存配置参数（含准入控制参数）
  ├─ frequency_tracker_.Reset()        // 重置频率计数器
  ├─ create_directories(base_path)     // 确保目录存在
  └─ 遍历 base_path，删除所有 .cache 文件  // 清理上次残留
```

**无重启恢复**: 每次启动时清空旧缓存文件，从零开始构建。这简化了设计（无需 LoadIndex 逻辑），启动速度更快，代价是重启后需要重新暖缓存。

---

## 8. 存储后端 (Storage Backends)

> **V2 变更**: V1 使用 mmap 预分配文件（page fault 不可控），V2 彻底移除 mmap，改为两种独立存储后端。

### 8.1 Memory 模式 (EntryPool)

- **存储**: `malloc` 分配 `std::vector<char>` 存放压缩后的 payload
- **索引**: `HashMap<Key, unique_ptr<Entry>>`，Key = `{segment_id, sig_hash}`
- **淘汰**: Clock 算法（`usage_count` 0-5），entry 级淘汰，永不整段丢失
- **压缩**: Roaring V2 / RoarInv / Raw 自适应（同 V1 CacheCompressor）
- **容量**: `maxBytes`（默认 2GB），超限时 Clock sweep 淘汰最冷 entry

```
Put:
  compress(result, valid) → compressed_data
  unique_lock(mutex_)
  while (current_bytes_ + size > max_bytes_) EvictOne()
  entries_[key] = new Entry{..., usage_count=1}
  current_bytes_ += size

Get:
  shared_lock(mutex_)
  entry = entries_.find({seg_id, sig_hash})
  entry.signature == input_signature?  → exact match
  entry.active_count == input?         → staleness check
  entry.usage_count++                  → Clock touch (atomic, no write lock)
  decompress(entry.data) → result     → return
```

### 8.2 Disk 模式 (DiskSlotFile)

- **存储**: per-segment 文件，`pread`/`pwrite` 系统调用，固定大小 slot
- **Slot 大小**: `slot_header_size + ((row_count + 63) / 64) * 8`（创建时确定，同文件内所有 slot 相同）
- **索引**: in-memory `HashMap<sig_hash, SlotMeta>`，signature 存在内存索引中（不存入磁盘 slot）
- **淘汰**: Clock 算法（in-memory index 追踪 `usage_count`），slot 级覆写
- **压缩**: 无压缩，Raw bitset 直存（磁盘空间充裕，省 CPU）
- **限制**: 仅缓存 sealed segment（growing segment row count 变化导致 slot 大小不匹配）

```
File layout:
  [FileHeader 64B][slot_0][slot_1]...[slot_N-1]

  FileHeader: magic(4B) version(4B) segment_id(8B) slot_size(4B) num_slots(4B) used_count(4B) reserved(24B)
  Slot:       sig_hash(8B) active_count(8B) flags(1B) raw_bitset(...)

Put:
  unique_lock(mutex_)
  if all slots used → EvictOne() → get free slot_id
  build slot: [sig_hash][active_count][flags][raw_bitset]
  pwrite(fd_, slot_buf, slot_size_, 64 + slot_id * slot_size_)
  slot_index_[sig_hash] = SlotMeta{slot_id, signature, active_count, usage_count=1}

Get:
  shared_lock(mutex_)
  meta = slot_index_.find(sig_hash)
  meta.signature == input?     → exact match
  meta.active_count == input?  → staleness check
  meta.usage_count++           → Clock touch
  pread(fd_, buf, slot_size_, 64 + slot_id * slot_size_)
  memcpy raw_bitset → out_result  → return (no decompression)
```

### 8.3 两种模式对比

| 维度 | Memory 模式 | Disk 模式 |
|------|------------|----------|
| 存储方式 | malloc (堆内存) | pread/pwrite (文件) |
| 压缩 | Roaring/Raw 自适应 | 无压缩 |
| 淘汰 | Clock (entry 级) | Clock (slot 级) |
| 准入 | 频率 + 延迟 | 仅延迟 |
| Growing segment | 支持 | 不支持 |
| 容量 | maxBytes (默认 2GB) | maxFileSizeBytes/segment (默认 256MB) |
| 延迟特性 | 确定性高 (malloc) | 确定性高 (pread/pwrite) |

### 8.4 崩溃一致性

缓存数据非关键数据，丢失仅影响性能（cache miss → 重新计算表达式），不影响正确性。Memory 模式进程退出即丢失；Disk 模式启动时清空旧文件，从零重建。

---
## 9. 并发设计

### 9.1 锁层次

| 锁 | 类型 | 保护范围 | 粒度 |
|----|------|---------|------|
| `files_mutex_` | `shared_mutex` | `files_` map 的增删查 | 全局 |
| `file->mutex_` | `shared_mutex` | 单个 segment 文件的读写 | per-segment |
| `lru_mutex_` | `mutex` | segment LRU 链表 | 全局 |

**锁顺序不变量**: `files_mutex_` → `file->mutex_` → `lru_mutex_`。严格遵守，防止死锁。

### 9.2 读写并行

`SegmentCacheFile` 使用 `shared_mutex`：

| 操作 | 锁类型 | 持锁时间 | 并发性 |
|------|--------|---------|--------|
| `Get` | shared_lock | ~50μs (含解压) | **多 Get 可并行** |
| `Contains` | shared_lock | ~100ns | 多 Contains 可并行 |
| `Append` | unique_lock | ~10μs (仅 memcpy) | 独占，但很短 |
| `Reset` | unique_lock | ~微秒 | 独占 |

### 9.3 关键并发模式

- **Double-checked locking**: `GetOrCreateFile` 先 shared_lock 查找，miss 后 unique_lock 创建
- **锁外压缩**: Compress ~300μs 不持任何文件锁
- **幂等 Append**: Append 内部再检查 `index_.count`，避免重复写入
- **锁外析构**: EraseSegment 在释放 `files_mutex_` 后才 reset file（munmap + close 可能较慢）
- **Atomic 计数**: `total_size_` 使用 `atomic<uint64_t>` 追踪磁盘使用量

---

## 10. 准入与淘汰策略

### 10.1 成本准入（Cost-Based Admission）

表达式计算如果本身就很快（如简单的整型比较 ~10μs），缓存它反而亏 — 从磁盘读取 + 解压也需要 ~50μs，再加上写入的 IO 开销，净收益为负。

**机制**: 调用方在执行表达式时计时，将耗时通过 `Value.eval_duration_us` 传给 Put。Put 检查该值是否超过最低阈值 `min_eval_duration_us_`（默认 0，即不过滤），低于阈值则跳过缓存。

```
调用方 (UnaryExpr.cpp):
  start = std::chrono::steady_clock::now()
  res = index->MatchQuery(query)           // 表达式实际执行
  valid = index->IsNotNull()
  eval_duration_us = elapsed(start)        // 计算耗时

  value.eval_duration_us = eval_duration_us
  manager.Put(key, value)

Put 内部:
  if (eval_duration_us > 0 && eval_duration_us < min_eval_duration_us_)
      return;    // 计算太快，不值得缓存
```

**成本分析**:

| 操作 | 耗时 |
|------|------|
| 表达式计算（简单整型比较） | ~10-50μs |
| 表达式计算（TextMatch/正则） | ~1-10ms |
| 缓存 Get（解压 + mmap 读取） | ~50μs |
| 缓存 Put（压缩 + 写入） | ~300μs |

当 `eval_duration < 缓存 Get 耗时` 时，命中缓存比重新计算还慢，缓存有害。建议阈值设为 100-200μs（高于 Get 开销，留出净收益空间）。

**不需要改文件格式**: `eval_duration_us` 仅用于 Put 时的准入判断，不写入磁盘，不影响 Get 路径。

**配置**: `min_eval_duration_us` 默认为 0（不过滤，向后兼容）。

### 10.2 频率准入（Frequency Admission）

频率过滤解决另一个问题：偶尔执行一次的 one-off 查询写入磁盘会浪费 IO 和空间，挤出真正的热点数据。

**FrequencyTracker** 在 Put 路径上做轻量级频率检查，只有达到阈值的表达式才真正写入磁盘：

```
counters_: [0][0][3][0][1][0]...[0]    ← 16384 个槽位，每个 1 byte (atomic<uint8_t>)
                 ↑
          sig_hash % 16384 = 2，已出现 3 次

threshold=2 时:
  第 1 次 Put("expr_A"):  counter: 0→1 < 2 → 拒绝 (不缓存)
  第 2 次 Put("expr_A"):  counter: 1→2 >= 2 → 放行 (写入磁盘)
  第 3 次 Put("expr_A"):  走到 Contains() → 已存在，跳过
```

**数据结构**: Direct-mapped 计数器数组，16384 个 `atomic<uint8_t>` 槽位，总共 16KB 固定内存。用 `sig_hash % 16384` 直接索引，不存 key，只存计数值。

**碰撞处理**: 不同 sig_hash 可能映射到同一槽位，共享计数器。最坏情况是某个 one-off 查询"借"到热点的计数被误缓存，浪费几十 KB 磁盘，对比 10GB 总容量可忽略。16384 槽位、1000 个活跃表达式时碰撞率约 6%。

**周期衰减**: 每 10000 次 `RecordAndCheck` 调用，所有计数器除以 2（指数衰减）：

```
热点表达式:   counter 200 → 100 → 50 → 25    // 多轮衰减后仍远超阈值，不受影响
冷门表达式:   counter 2 → 1 → 0              // 自然归零，腾出槽位给新热点
```

衰减用除以 2 而非清零，避免误杀真正的热点。假设 QPS=1000、每查询 5 个表达式，约 2 秒衰减一次。

**性能开销**: 被拦住的 one-off 查询总开销 < 10ns（一次取模 + 一次 atomic 读写），对比完整 Put 路径的 ~300μs 压缩 + ~10μs 写入，可忽略。

**配置**: `admission_threshold` 默认为 1（不过滤，向后兼容），设为 2 即启用频率准入。

### 10.3 准入过滤链

成本准入和频率准入是两个独立的过滤器，串联在 Put 路径上：

```
Put():
  ├─ eval_duration_us < min_threshold?  → return (太快，缓存收益为负)
  ├─ frequency < admission_threshold?   → return (太冷，one-off 查询)
  └─ 通过 → 继续压缩 + 写入磁盘
```

两者各解决一个问题：
- **成本准入**: 过滤"不值得缓存"的表达式（计算本身就很快）
- **频率准入**: 过滤"不经常出现"的表达式（偶尔执行一次的长尾查询）

**Memory 模式**: 一个表达式必须**同时**通过频率准入和延迟准入两个过滤器才会被缓存。
**Disk 模式**: 仅延迟准入（磁盘空间充裕，不需要频率过滤）。

### 10.4 淘汰策略

> **V2 变更**: V1 使用单文件 Reset + Segment 级 LRU 淘汰。V2 改为 Clock 算法做 entry/slot 级淘汰。

**Clock 算法**（两种模式均使用）:
- 每个 entry/slot 有 `usage_count` (0-5)，Get 命中时 atomic 递增
- 淘汰时从 `clock_hand_` 位置扫描：
  - `usage_count > 0` → 递减，跳过（给一次"机会"）
  - `usage_count == 0` → 淘汰
- 最坏情况：所有 entry 都热 → 强制淘汰 clock_hand 位置的 entry

| 模式 | 淘汰粒度 | 容量限制 | 触发条件 |
|------|---------|---------|---------|
| Memory | entry 级 | `maxBytes` (2GB) | `current_bytes_ + size > max_bytes_` |
| Disk | slot 级 | `maxFileSizeBytes` (256MB/segment) | 所有 slot 已满 |

**EraseSegment**:
- Memory 模式: 遍历 `entries_`，删除匹配 `segment_id` 的所有 entry。O(N)
- Disk 模式: `close(fd_)` + `unlink(path)`。O(1)

---

## 11. Go 侧集成

### 11.1 配置传递 (CGO)

```go
// internal/util/initcore/query_node.go

// 1. 设置总开关
C.SetExprResCacheEnable(C.bool(cfg.ExprResCacheEnabled.GetAsBool()))

// 2. 设置兼容性容量参数（disk 模式下未使用）
C.SetExprResCacheCapacityBytes(C.int64_t(cfg.ExprResCacheCapacityBytes.GetAsInt64()))

// 3. 配置 disk 后端（含准入控制参数）
if cfg.ExprResCacheDiskEnabled.GetAsBool() {
    diskPath := pathutil.GetPath(pathutil.ExprCachePath, nodeID)
    C.SetExprResCacheDiskConfig(cDiskPath, maxTotalSize, maxSegSize,
        compressionEnabled, admissionThreshold, minEvalDurationUs)
}
```

### 11.2 Segment 生命周期集成

```go
// internal/querynodev2/segments/segment.go → LocalSegment.Delete()
C.ExprResCacheEraseSegment(C.int64_t(s.ID()))
```

Segment 释放时通过 CGO 调用 C++ 侧清理对应缓存文件。

### 11.3 C++ CGO 接口

```cpp
// internal/core/src/common/init_c.h
void SetExprResCacheEnable(bool val);
void SetExprResCacheDiskConfig(const char* base_path,
                                int64_t max_total_size,
                                int64_t max_segment_file_size,
                                bool compression_enabled,
                                int32_t admission_threshold,
                                int64_t min_eval_duration_us,
                                bool in_memory);

// internal/core/src/segcore/segment_c.cpp
void ExprResCacheEraseSegment(int64_t segment_id);
```

---

## 12. 配置

> **V2 配置变更**: V1 使用扁平化的 `exprCache.*` 配置，V2 改为 `exprCache.mode` + `exprCache.memory.*` / `exprCache.disk.*` 分层结构。

```yaml
queryNode:
  exprCache:
    enabled: false                          # 总开关（默认关闭）
    mode: "memory"                          # "memory" | "disk"

    memory:
      maxBytes: 2147483648                  # 2GB — EntryPool 总内存上限
      compressionEnabled: true              # Roaring/Raw 自适应压缩
      admissionThreshold: 2                 # 频率准入：出现 2+ 次才缓存
      minEvalDurationUs: 100                # 延迟准入：计算 < 100μs 的不缓存

    disk:
      maxFileSizeBytes: 268435456           # 256MB — 单个 DiskSlotFile 上限
      minEvalDurationUs: 200                # 延迟准入：阈值高于 memory（disk I/O 成本）
```

缓存路径由 `localStorage.path` 派生：`<localStorage.path>/expr_cache/<nodeID>/`。

### 12.1 容量估算

**Memory 模式**:
- 1M 行 segment，压缩后 bitset ≈ 20-50KB/entry
- 2GB 容量 ≈ 40K-100K 个表达式（压缩态）

**Disk 模式**:
- 1M 行 segment，raw bitset = 125KB/slot
- 256MB 单文件 ≈ 2000 个 slot/segment
- slot 大小 = ((row_count + 63) / 64) * 8 + slot_header_size

### 12.2 Paramtable 配置项

| Key | 默认值 | 说明 |
|-----|--------|------|
| `queryNode.exprCache.enabled` | `false` | 总开关 |
| `queryNode.exprCache.mode` | `"memory"` | `"memory"` = EntryPool；`"disk"` = DiskSlotFile |
| `queryNode.exprCache.memory.maxBytes` | `2147483648` | Memory 模式总容量（默认 2GB） |
| `queryNode.exprCache.memory.compressionEnabled` | `true` | 自适应压缩开关（关闭则全走 Raw） |
| `queryNode.exprCache.memory.admissionThreshold` | `2` | 频率准入阈值，表达式 Put 达到此次数才缓存 |
| `queryNode.exprCache.memory.minEvalDurationUs` | `100` | 延迟准入阈值（μs），计算耗时低于此值不缓存 |
| `queryNode.exprCache.disk.maxFileSizeBytes` | `268435456` | Disk 模式单 segment 文件最大（默认 256MB） |
| `queryNode.exprCache.disk.minEvalDurationUs` | `200` | Disk 模式延迟准入阈值（μs） |

---
## 13. 外部接口

`ExprResCacheManager` 的外部接口基本不变，仅 `Value` 结构体增加可选的 `eval_duration_us` 字段：

```cpp
struct Value {
    std::shared_ptr<TargetBitmap> result;
    std::shared_ptr<TargetBitmap> valid_result;
    int64_t active_count{0};
    size_t bytes{0};
    int64_t eval_duration_us{0};  // 表达式计算耗时（μs），用于成本准入，0 表示不检查
};

// 写入缓存（Value.eval_duration_us 用于成本准入判断，不写入磁盘）
void Put(const Key& key, const Value& value);

// 读取缓存 (caller 需预设 out_value.active_count)
bool Get(const Key& key, Value& out_value);

// 删除 segment 的所有缓存
size_t EraseSegment(int64_t segment_id);

// 清空所有缓存
void Clear();
```

调用方改动（`UnaryExpr.cpp` 的 MatchExprImpl）：

```cpp
// 计时
auto start = std::chrono::steady_clock::now();
auto res = func(index, query);
auto valid_res = index->IsNotNull();
auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::steady_clock::now() - start).count();

// Put 时传入耗时
exec::ExprResCacheManager::Value v;
v.result = cached_match_res_;
v.valid_result = cached_index_chunk_valid_res_;
v.active_count = active_count_;
v.eval_duration_us = elapsed;
exec::ExprResCacheManager::Instance().Put(key, v);
```

其他调用方：
- `segment.go` 的 LocalSegment.Delete() — CGO 调用 EraseSegment（不受影响）

---

## 13.5 表达式接入状态

### 接入方式

- **Index path**: 通过 `SegmentExpr::ProcessIndexChunksImpl`（`Expr.h`）统一接入，所有使用 `ProcessIndexChunks` 的表达式自动获得缓存能力
- **Stats path**: 各表达式的 `ByStats` 方法独立接入（和 ExistsExpr 同模式）
- **Raw path**: 依赖 `offset_input`/`bitmap_input`，需评估安全性后逐步接入
- **TextMatch/PhraseMatch**: 通过 `ExprCacheHelper::GetOrCompute` 手动接入（最早接入的表达式）

### 接入进度

| 表达式 | Index path | Stats path | Raw path |
|--------|:---:|:---:|:---:|
| **UnaryExpr** (TextMatch/PhraseMatch) | ✅ | — | — |
| **UnaryExpr** (比较/范围) | ✅ | 待接入 | 待接入 |
| **BinaryRangeExpr** | ✅ | 待接入 | 待接入 |
| **BinaryArithOpEvalRangeExpr** | ✅ | 待接入 | 待接入 |
| **TermExpr** (IN) | ✅ | 待接入 | 待接入 |
| **JsonContainsExpr** | ✅ | 待接入 | 待接入 |
| **ExistsExpr** | ✅ | ✅ | — |
| **NullExpr** | 待接入 | — | 待接入 |
| **GISFunctionFilterExpr** | 待接入 | — | 待接入 |
| **CompareExpr** | — | — | 待接入 |

### 不接入的表达式

| 表达式 | 原因 |
|--------|------|
| AlwaysTrueExpr | 计算 < 1μs，不值得缓存 |
| ValueExpr | 常量，无需缓存 |
| LogicalBinaryExpr / LogicalUnaryExpr | 组合节点，子表达式各自缓存 |
| ConjunctExpr / LikeConjunctExpr | 同上 |
| ColumnExpr / CallExpr | 返回非 bitset 数据 |

### Raw path 接入注意事项

Raw path 的结果可能依赖调用方传入的 `offset_input`（指定行）和 `bitmap_input`（跳过行），导致 `(segment_id, signature)` 相同但结果不同。**仅在 `!has_offset_input_ && bitmap_input.empty()` 时缓存安全**。

### 跨路径共享

所有 path 共享同一份缓存。Cache key = `(segment_id, expr.ToString())`，不包含 path 信息。同一表达式不管走 Index/Stats/Raw 哪条路径计算，结果一致，可安全跨路径复用。

---

## 14. 测试覆盖

**CacheCompressor 测试**:
- LZ4RoundTrip, NoCompression, EmptyBitset, LargeBitset, VariousDensities

**SegmentCacheFile 测试**:
- LazyOpen, AppendAndGet, HashCollision, IdempotentPut, ActiveCountValidation
- ResetOnOverflow, ConcurrentReads, ConcurrentReadWrite

**ExprResCacheManager 测试**:
- PutGetBasic, LruEvictionByCapacity, EraseSegment, EnableDisable
- DiskBackendPutGet, DiskBackendEraseSegment, SegmentLRUEviction
- PutGetAcrossSegments, ClearDeletesAllFiles, EnableDisableWithDisk

---

## 15. E2E 性能实测

> **V2 变更**: V2 使用 malloc (Memory) 和 pread/pwrite (Disk)，不再使用 mmap。以下为 V2 实测数据。V1 mmap 数据保留在本节末尾供参考。

测试配置：1M-row bitset，valid=all-ones。测试代码：`bench_expr_cache.cpp`。

### 15.1 V2 Memory 模式性能

Memory 模式使用 EntryPool（malloc + Clock + 压缩）。

| 操作 | 延迟范围 | 说明 |
|------|---------|------|
| **Put** | ~70-130μs | 含 Roaring/Raw 自适应压缩 |
| **Get** | ~53-83μs | 含解压（Roaring decompress / Raw memcpy） |

### 15.2 V2 Disk 模式性能

Disk 模式使用 DiskSlotFile（pread/pwrite，无压缩）。

| 操作 | 延迟范围 | 说明 |
|------|---------|------|
| **Put** | ~56-65μs | pwrite，无压缩，Raw 直存 |
| **Get** | ~23-25μs | pread，无解压，memcpy → out_result |

### 15.3 V1 vs V2 对比

| 指标 | V1 mmap Put | V2 Memory Put | V2 Disk Put |
|------|:---:|:---:|:---:|
| 延迟 (Raw) | ~210μs | ~70-130μs | ~56-65μs |
| 瓶颈 | page fault (~155μs) | 压缩 | pwrite syscall |
| 延迟可预测性 | 低（page fault 随机） | 高 | 高 |

| 指标 | V1 mmap Get | V2 Memory Get | V2 Disk Get |
|------|:---:|:---:|:---:|
| 延迟 | ~16μs | ~53-83μs | ~23-25μs |
| 瓶颈 | memcpy (page cache hot) | decompress | pread syscall |

**关键发现**:
- V2 Disk Put 比 V1 mmap Put 快 **3-4x**（消除 page fault 瓶颈）
- V2 Disk Get 比 V1 mmap Get 略慢（pread syscall vs 直接 memcpy），但延迟更可预测
- V2 Memory Get 包含解压开销（53-83μs），比 V1 的 ~16μs 慢，但提供 entry 级淘汰能力
- V2 Disk 模式 Put + Get 总延迟 ~80-90μs，对比 TextMatch 表达式计算耗时 1-10ms，加速比约 **11-125x**

### 15.4 V1 历史数据（mmap 模式，已废弃）

以下为 V1 mmap 模式的历史实测数据，供参考：

| 密度 | CompType | mmap Put | mmap Get | 匿名 mmap Put | 匿名 mmap Get |
|------|---------|:---:|:---:|:---:|:---:|
| 0.1% | Roaring | 372μs | 13μs | 125μs | 12μs |
| 1% | Roaring | 254μs | 22μs | 236μs | 22μs |
| 5% | Raw | 214μs | 16μs | 129μs | 16μs |
| 50% | Raw | 209μs | 16μs | 124μs | 15μs |
| 99% | RoarInv | 265μs | 25μs | 216μs | 25μs |

### 15.5 场景推荐

| 场景 | 推荐配置 | 理由 |
|------|---------|------|
| **RAM 充足 + 压缩率优先** | `mode=memory` | 自适应压缩，entry 级淘汰，支持 growing segment |
| **RAM 紧张 + 低延迟** | `mode=disk` | 仅 in-memory index 占 RAM，数据在磁盘，Put/Get 均快 |
| **极致 Get 性能** | `mode=disk` | Get ~23μs（无解压），最低读取延迟 |
| **极致容量** | `mode=disk` + 增大 `maxFileSizeBytes` | 磁盘空间充裕 |

### 15.6 吞吐估算

按每次操作延迟计算（单核）：

**Memory 模式**:
- Put 吞吐: ~8K-14K ops/s（70-130μs/op）
- Get 吞吐: ~12K-19K ops/s（53-83μs/op）

**Disk 模式**:
- Put 吞吐: ~15K-18K ops/s（56-65μs/op）
- Get 吞吐: ~40K-43K ops/s（23-25μs/op）

---
## 16. 压缩方案对比与优化

### 16.1 TargetBitmap 底层结构

```cpp
// CustomBitset = Bitset<VectorizedElementWiseBitsetPolicy<uint64_t>, fbvector<uint8_t>>
```

底层是**密集位数组**（dense bitset），以 `uint64_t` 为操作单元，存储在连续 `uint8_t` 缓冲区中。1M 行 segment = 125KB 原始数据。

### 16.2 候选方案说明

> 本节列举设计阶段考察过的所有压缩方案。**最终实现只采用 Roaring V2 + Raw**，
> 经实测对比 LZ4、ZSTD、RLE 都不如 Roaring V2 + Raw 自适应方案。

**LZ4（已弃用）**: 通用字节流压缩器，基于滑动窗口做模式匹配。不理解 bit 语义，对 5%-90% 中间密度数据编码慢且压缩率不优。

**Roaring Bitmap**: 专门为 bitset 设计。将 32-bit 空间按 65536 bit 分区，每个分区（container）根据密度自动选择最优存储：

| Container 类型 | 存储方式 | 适用场景 | 大小 |
|---------------|---------|---------|------|
| **Array** | set-bit 位置列表（每个 2B） | popcount < 4096 | 2B × popcount |
| **Bitmap** | uint64[1024] 密集位图 | popcount >= 4096 | 固定 8KB |
| **Run** | (start, length) 对（每对 4B） | 连续区间多 | 4B × run 数 |

`runOptimize()` 会自动将 bitmap/array container 转为 run container（如果更小）。

Roaring 的关键优势：**分区后每个 container 独立选择最优编码**，不同密度区域各取所长。

**Roaring dense↔roaring 转换优化**:

逐 bit 遍历（naive）极慢（8-15ms）。有两种优化方案：

| 方案 | 编码（bitset→roaring） | 解码（roaring→bitset） |
|------|----------------------|----------------------|
| **V1 (addMany)** | word 级 tzcnt 提取位置 + `addMany` 批量添加 | `roaring_bitmap_to_bitset` + memcpy |
| **V2 (zero-copy)** | 按 container 粒度 popcount → 稠密 memcpy / 稀疏 tzcnt + `ra_append` | 同 V1 |

V2 的核心：稠密 container 直接 memcpy 8KB uint64 数组到 bitmap container，O(1) per container，跳过逐 bit 遍历。

**RLE**: 编码连续相同 bit 的长度。编解码极快但**强依赖空间局部性**，TextMatch 命中行随机分散时压缩率差甚至膨胀。

**ZSTD**: 比 LZ4 多 30-50% 压缩率，但压缩慢 5-10 倍、解压慢 2-3 倍，对缓存场景不值得。

**Raw（不压缩）**: 30%-70% 密度时所有方案都接近或超过原始大小，不压缩省 CPU 是最优解。

### 16.3 实测数据（1M 行单 bitset）

测试环境：CRoaring 3.0.0，LZ4（直接 API 调用），GCC 11，Release 编译。每个数值为 **9 次运行的中位数**，每次迭代用**全新 src/dst 缓冲区**避免 cache reuse 偏差。测试代码：`ExprCacheTest.cpp` 中 `CompressionBenchmark.FullComparison`。

| 密度 | Raw | LZ4 大小 | Roaring 大小 | 大小胜出 | LZ4 enc | V2 enc | LZ4 dec | V2 dec |
|------|-----|---------|-------------|---------|---------|--------|---------|--------|
| 0.1% | 125KB | 5.5KB | **2.2KB** | **Roaring 2.5x** | **29μs** | 62μs | 19μs | **8μs** |
| 1% | 125KB | 33KB | **20KB** | **Roaring 1.6x** | 141μs | **109μs** | 59μs | **13μs** |
| 5% | 125KB | **76KB** | 100KB | **LZ4 1.3x** | 334μs | **185μs** | 46μs | 36μs |
| 10% | 125KB | **109KB** | 126KB | **LZ4 1.2x** | 260μs | **90μs** | 37μs | **19μs** |
| **50%** | 125KB | **125KB** | 131KB | **LZ4** | **15μs** | 91μs | **3μs** | 19μs |
| 90% | 125KB | **109KB** | 129KB | **LZ4 1.2x** | 258μs | **97μs** | 37μs | **22μs** |
| 99% | 125KB | **33KB** | 40KB | **LZ4 1.2x** | **139μs** | 189μs | **58μs** | 75μs |

**关键观察**：

- **50% 随机密度 LZ4 异常快**（enc 15μs / dec 3μs）：LZ4 hash 表对真随机数据 100% miss，触发 **acceleration / skip 模式** —— 不再尝试匹配，退化为 memcpy + 少量 literal 头标记。125KB memcpy @ L2-L3 带宽（~20 GB/s）≈ 6-12μs，加上 LZ4 块管理开销，15μs 合理。
- **5%/10%/90% 是 LZ4 的最差场景**（enc 260-334μs）：既找不到长匹配也不能 skip，hash 表抖动 + 大量短 copy 操作。Roaring V2 编码快 2-3 倍。
- **稀疏（0.1%/1%）和 99% 密度 LZ4 都很快**（29-141μs）：长 0 / 长 1 串帮 LZ4 找到长匹配。
- **Roaring V2 解码几乎全胜**（除 99% 外）—— 因为大部分密度 V2 是 array container（直接读 uint16 位置），LZ4 解码需要逐字节解释 length+copy 指令。
- **Roaring V2 压缩率仅在 ≤1% 密度胜出**，其余都比 LZ4 大（因为退化为 bitmap container）。

> 说明：上面的数据是**纯算法性能**（直接调用 LZ4/Roaring API）。生产 Put 路径还会叠加
> mmap page fault（~150μs）、锁、hash 等开销，详见第 15 节 E2E 性能测试。

### 16.4 关键发现

**压缩率**：
- **≤1% 密度**：Roaring 更小（2.1KB vs 5.5KB，20KB vs 33KB）— Roaring 的甜点
- **≥5% 密度**：LZ4 更小。Roaring 退化为 bitmap container（固定 8KB/container）
- **30%-70% 密度**：所有方案都接近或超过原始大小。这是信息论极限 — 每个 bit 的熵接近 1（`H = -p·log2(p) - (1-p)·log2(1-p) ≈ 1.0`），数据本质上不可压缩

**编码速度**：
- V2 全密度范围 53-266μs，和 LZ4 的 39-332μs 同一量级
- V1（addMany）在稠密时高达 5-6ms，慢 30-60 倍（已被 V2 淘汰）

**解码速度**：两者接近（14-86μs vs 30-287μs），各有胜负

**30%-70% 密度不应缓存**：这类表达式选择性差（选了一半数据），对向量搜索加速有限，且通常计算很快（倒排索引快速返回大量结果）。成本准入（`minEvalDurationUs`）会自动过滤掉大部分这类表达式。

### 16.5 实际方案：Roaring V2 + Raw 自适应（无 LZ4）

经实测对比（详见 16.3），LZ4 在中间密度（5%-90%）下编码慢、压缩率不优，不如 Raw + memcpy + page cache 直存。**最终实现完全去掉了 LZ4**，只保留 Roaring V2 和 Raw 两种路径：

```cpp
// CacheCompressor::Compress 内部
double density = (double)result.count() / result.size();

if (density <= 0.03) {
    // 极稀疏 → Roaring V2（压缩率 60-114x，Get ~13-22μs）
    out_comp_type = kCompTypeRoaring;
    encode_roaring(result);
} else if (density >= 0.97) {
    // 极稠密 → 取反 + Roaring V2（取反后稀疏，Get ~25μs）
    out_comp_type = kCompTypeRoaringInv;
    TargetBitmap inverted(result.size());
    inverted.set();
    inverted -= result;
    encode_roaring(inverted);
} else {
    // 中间地带 (3%-97%) → Raw 直存
    // 对随机分布的中间密度数据，任何压缩都不优于 memcpy
    // 选择性差的过滤（30%-70%）通常被 minEvalDurationUs 过滤掉，不会进入此分支
    out_comp_type = kCompTypeRaw;
    raw_pointers_to_result_and_valid();  // zero-copy scatter-gather
}
```

### 16.6 实际 comp_type 取值

```cpp
// CacheCompressor.h
constexpr uint8_t kCompTypeRoaring    = 1;
constexpr uint8_t kCompTypeRoaringInv = 2;
constexpr uint8_t kCompTypeRaw        = 0xFF;
// kCompTypeLZ4 = 0 已废弃，不再使用（保留常量定义以避免值复用）
```

Milvus 已依赖 CRoaring 3.0.0（`internal/core/conanfile.py`），BitmapIndex 已在使用，无需引入新依赖。

V2 zero-copy 编码的核心实现（直接构造 container）：

```cpp
// 按 65536-bit container 粒度处理
for (size_t c = 0; c < num_containers; ++c) {
    int32_t popcount = popcnt(words + c*1024, 1024);

    if (popcount >= 4096) {
        // 稠密: memcpy uint64[1024] → bitmap container, O(1)
        bitset_container_t* bc = bitset_container_create();
        memcpy(bc->words, words + c*1024, 8192);
        bc->cardinality = popcount;
        ra_append(&r->high_low_container, key, bc, BITSET_CONTAINER_TYPE);
    } else {
        // 稀疏: tzcnt 提取位置 → array container, O(popcount)
        array_container_t* ac = array_container_create_given_capacity(popcount);
        // ... extract bit positions via __builtin_ctzll
        ra_append(&r->high_low_container, key, ac, ARRAY_CONTAINER_TYPE);
    }
}
roar.runOptimize();  // bitmap/array → run container (if smaller)
```

---

## 17. 与其他系统的对比

### 17.1 横向对比

| 维度 | Milvus (ExprResCache) | Elasticsearch (Query Cache) | ClickHouse (ResultCache) | 传统数据库 (MySQL/PG) |
|------|----------------------|----------------------------|--------------------------|----------------------|
| **缓存粒度** | 分段×表达式级。缓存单个表达式在某个 Segment 上的 Bitset | 分片×过滤条件级。缓存过滤条件产生的 Bitset | 查询级。缓存整条 SQL 的最终结果集 | 表级/查询级。整条语句 |
| **存储介质** | 双模式：Memory (malloc) / Disk (pread/pwrite) | 内存 (On-heap)，Lucene 管理 | 纯内存 (LRU) | 内存 (Buffer Pool) |
| **失效机制** | active_count 逻辑版本；Segment 删除则文件删除 | 段不变性 — sealed segment 的缓存永不失效 | TTL + 数据版本，底层表变则全量失效 | 表更新即全量失效（MySQL 8.0 已移除 Query Cache） |
| **压缩方式** | 自适应：Roaring V2（≤3%）/ RoarInv（≥97%）/ Raw（中间） | Roaring Bitmap | 通常不压缩 | 不压缩 |
| **准入策略** | 成本准入 + 频率准入（计算太快或出现次数不够的表达式不缓存） | 频率准入（最近 256 次查询中出现 2+ 次才缓存） | 无 | 无 |

### 17.2 Milvus 的设计优势

**A. 极细粒度的"乐高式"重用**

Milvus 缓存的是**单个表达式 × 单个 Segment** 的 bitset，而不是整条查询的结果。

```
查询 1: A > 10 AND B < 5   → 分别缓存 [A > 10] 和 [B < 5]
查询 2: A > 10 AND C = 1   → [A > 10] 命中缓存，只需计算 [C = 1]
```

ClickHouse 缓存整条 SQL 结果，条件稍有变化（如 `LIMIT 10` → `LIMIT 20`）就全量失效。Milvus 的粒度使得长尾查询的命中率大幅提升。

**B. 双模式灵活部署**

V2 提供 Memory 和 Disk 两种模式，不同部署场景各取所长：

- Memory 模式：malloc + 压缩，RAM 充足时容量大、压缩率高
- Disk 模式：pread/pwrite，RAM 紧张时仅占用少量 in-memory index，数据在磁盘
- 两种模式均消除了 V1 mmap 的 page fault 不可预测性

**C. Sealed Segment 的天然契合**

Sealed segment 的数据不再变化，缓存天然不会过期（只需通过 `active_count` 检测 compaction）。这与 Elasticsearch 的段不变性设计思路一致，但 Milvus 还额外支持了 compaction 后的失效检测。

### 17.3 可借鉴的优化方向

**A. Roaring Bitmap 压缩（借鉴 Elasticsearch/Lucene）**

ES 的 Query Cache 使用 Roaring Bitmap 而非通用压缩。对 ExprCache 的稀疏 bitset（TextMatch 典型命中率 0.1%-10%），Roaring 的压缩率比 LZ4 高 10 倍以上。详见第 16 章分析。

此外，Roaring Bitmap 支持**直接在压缩格式上做位运算**（AND/OR/NOT），可以跳过"解压→运算→压缩"的流程。如果未来 ExprCache 扩展到缓存组合表达式的中间结果，这会是很大的优势。

**B. 频率准入策略（借鉴 Elasticsearch）** — ✅ 已采纳

已实现 `FrequencyTracker`（详见第 10.2 节），使用 16K 槽位的 direct-mapped 计数器数组，配合周期衰减。通过 `admissionThreshold` 配置。

**C. 表达式规范化（借鉴 ClickHouse）**

当前使用 `Expression.ToString()` 作为 Key，存在逻辑等价但字符串不同的情况：

```
"field > 10"   vs  "field > 10.0"   → 逻辑相同，字符串不同，缓存不命中
"10 < field"   vs  "field > 10"     → 逻辑相同，字符串不同，缓存不命中
```

ClickHouse 在生成缓存 Key 前会进行 AST 规范化（Canonicalization）。可以在 `ToString()` 阶段做简单的规范化处理，进一步提高命中率。

**D. 成本感知的准入策略（借鉴 Snowflake）** — ✅ 已采纳

已实现成本准入（详见第 10.1 节），调用方传入 `eval_duration_us`，计算耗时低于 `minEvalDurationUs` 阈值的表达式直接跳过缓存。

---

## 18. 设计总结

| 方面 | 说明 |
|------|------|
| **存储后端** | **V2 双模式：Memory (EntryPool, malloc) / Disk (DiskSlotFile, pread/pwrite)。不再使用 mmap** |
| 模式切换 | `exprCache.mode = "memory" \| "disk"`，配置切换，非同时运行 |
| 容量 | Memory: 2GB (默认)；Disk: 256MB/segment (默认) |
| **Put 性能** | **Memory ~70-130μs / Disk ~56-65μs**（1M-row） |
| **Get 性能** | **Memory ~53-83μs (含解压) / Disk ~23-25μs (无解压)** |
| **压缩** | **Memory: 自适应 Roaring/RoarInv/Raw；Disk: 无压缩 (Raw 直存)** |
| 准入控制 | Memory: 频率准入 + 延迟准入；Disk: 仅延迟准入 |
| 并发 | shared_mutex，多 Get 并行 |
| 淘汰 | Clock 算法（近似 LRU），entry/slot 级淘汰，不再整段丢失 |
| Key 匹配 | sig_hash (XXH64) 定位 + signature 全字符串精确比较，零误判 |
| 重启 | 清空旧文件，从零重建 |
| 崩溃安全 | 缓存丢失 = cache miss，不影响正确性 |
| 接口兼容 | Put/Get/EraseSegment 接口不变，上层 ExprCacheHelper / BatchedCachedMixin 无感知 |
