# Fix for Issue #46656: QueryNode Panic with "unordered_map::at" During ProcessInsert

## Problem Summary
The `IndexingRecord::AppendingIndex()` methods had a TOCTOU (Time-of-Check-Time-of-Use) race condition that caused Milvus to crash with:
- `std::out_of_range` exception when accessing `field_indexings_` map
- `SIGFPE` (division by zero) due to corrupted hash table state
- Occurred during concurrent insert/retrieve operations with text index (Tantivy)

## Root Cause Analysis

### TOCTOU Race Condition
```cpp
// VULNERABLE CODE:
if (!is_in(fieldId)) {  // Check if fieldId exists
    return;
}
// ⚠️ RACE WINDOW: Other threads may modify field_indexings_
auto& indexing = field_indexings_.at(fieldId);  // May throw std::out_of_range
```

### Missing Synchronization
- `field_indexings_mutex_` existed but was never used
- No lock protection when accessing shared containers
- Two data structures (`field_indexings_`, `schema_`) could become inconsistent

## Solution

### 1. Changed mutex type (FieldIndexing.h:507)
```cpp
- std::mutex mutex_;
+ mutable std::shared_mutex field_indexings_mutex_;
```
**Rationale**: Read-write lock allows multiple readers (better concurrency) while still preventing writer conflicts.

### 2. Protected AppendingIndex methods (FieldIndexing.cpp:36-153)

**Key Strategy: Minimal Locking**
- Acquire shared_lock only while accessing `field_indexings_`
- Get pointer to indexing object and copy field metadata
- Release lock immediately before time-consuming operations
- Execute index building/appending without holding lock

```cpp
// Acquire read lock for safe container access
std::shared_lock<std::shared_mutex> lock(field_indexings_mutex_);

if (!is_in(fieldId)) {
    return;
}

FieldIndexing* indexing_ptr = field_indexings_.at(fieldId).get();
auto type = indexing_ptr->get_data_type();
auto field_meta = schema_.get_fields().at(fieldId);

lock.unlock();  // Release lock before time-consuming operations

// Execute expensive index operations without lock
indexing_ptr->AppendSegmentIndexDense(reserved_offset, size, ...);
```

### 3. Protected public accessor methods (FieldIndexing.cpp:849-918)

Implemented thread-safe versions of:
- `is_in()` - Check if field exists
- `get_field_indexing()` - Get indexing object
- `get_vec_field_indexing()` - Get vector indexing with type check
- `SyncDataWithIndex()` - Query sync status
- `HasRawData()` - Check raw data availability
- `GetDataFromIndex()` - Retrieve indexed data

All methods acquire `shared_lock` during container access, then release before executing user code.

## Performance Characteristics

### Lock Contention
- **Read-heavy operations**: Multiple readers can proceed concurrently (no blocking)
- **Write operations**: Limited to initialization phase (minimal contention)
- **Time-consuming operations**: Execute without lock (good parallelism)

### Memory Overhead
- Changed from `std::mutex` to `std::shared_mutex`
- Additional ~40 bytes per segment (negligible impact)

### Latency Impact
- **Best case**: No blocked readers if field_indexings_ is stable (typical)
- **Worst case**: Minimal read-lock contention during concurrent inserts

## Testing Strategy

### Thread Safety Verification
1. Multiple threads calling `AppendingIndex()` concurrently
2. Simultaneous insert + retrieve operations
3. Chaos test scenarios with pod failures

### No Deadlock Risk
- Single lock per IndexingRecord (simple hierarchy)
- No nested locks acquired
- Lock released before calling user code

## Files Modified

| File | Changes |
|------|---------|
| `FieldIndexing.h` | Changed `mutex_` → `shared_mutex`, moved methods to .cpp |
| `FieldIndexing.cpp` | Added lock protection to 8 methods |

## Build & Runtime Validation

✅ **Compilation**: Standard C++17 features, no new dependencies
✅ **Runtime**: `shared_lock` compatible with all C++17 toolchains
✅ **Compatibility**: Backward compatible (internal implementation change only)

## Related Issues
- Original Issue: #46656
- Related Issue: #45590 (similar Tantivy concurrent access issues)
- Related PR: #43499 (dangling reference fix, but didn't address mutex)
