// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "common/EasyAssert.h"
#include "common/Geometry.h"
#include "common/Types.h"
#include "geos_c.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

// Cache key: the OWNING SEGMENT OBJECT (segment_instance_uid) plus the field.
//
// Keying on the logical segment id is not enough, because more than one live
// object can carry the same id -- and a cache entry's lifetime is tied to the
// object that built it, not to the logical segment:
//   * a growing and a sealed twin coexist during handoff (querynode keeps two
//     maps keyed by the same id), and
//   * two sealed instances of different versions coexist while the replaced
//     one is released asynchronously (segments/manager.go:409-441).
// In both cases a shared key means the arriving instance reuses the departing
// instance's entry, and the departing instance's destructor then erases the
// cache the still-serving instance depends on. The cache is only ever built
// during load, so it is never rebuilt: every later GIS predicate silently
// falls back to per-row bulk_subscript + WKB re-parsing, with no error and no
// log. The segment id stays in the key for diagnosability only.
inline std::string
MakeCacheKey(uint64_t segment_instance_uid,
             int64_t segment_id,
             FieldId field_id) {
    return std::to_string(segment_instance_uid) + "_" +
           std::to_string(segment_id) + "_" + std::to_string(field_id.get());
}

// Prefix covering every field cache owned by one segment OBJECT, so a
// destructor erases only the entries that object created.
inline std::string
MakeSegmentCachePrefix(uint64_t segment_instance_uid, int64_t segment_id) {
    return std::to_string(segment_instance_uid) + "_" +
           std::to_string(segment_id) + "_";
}

// Offset-addressed, never-relocating storage for cached geometries.
//
// Layout follows the growing segment's own column storage (ConcurrentVector /
// ThreadSafeChunkVector in mmap/ChunkVector.h): fixed-size chunks that are
// allocated once and never moved, reached through a geometric slab directory
// whose slabs are likewise never moved. Chunk pointers are published with a
// release store, so a reader only ever dereferences storage that was fully
// allocated before it became reachable. (The layout is duplicated rather than
// reused because ChunkVector.h lives under mmap/ and drags in the mmap
// manager and the segcore config; common/ must not depend on either.)
//
// That is the whole point: because a slot's address is fixed for the cache's
// lifetime and its contents are frozen before publication, readers need no
// lock at all. The previous std::vector<Geometry> could not offer this --
// resize() relocated every element, so a reader's `const Geometry*` had to be
// protected by a shared lock held across a whole batch, and those overlapping
// read locks are what starved the growing-segment writer (issue #52191).
//
// Publication is per slot rather than by a single committed-row count,
// because the cache is written at ABSOLUTE segment offsets that can arrive
// out of order (a later batch may land before an earlier one), so there is no
// single prefix that is "the committed part". A slot is either published --
// its Geometry is final and readable forever -- or it is not there yet, which
// readers report exactly like an invalid row: no geometry here, skip it.
//
// Concurrency: one writer at a time (the owner serializes writers on its own
// mutex, which readers never touch) and any number of lock-free readers.
class GeometryChunkStore {
 public:
    // Slot publication states. A slot goes kEmpty -> kReady exactly once.
    static constexpr uint8_t kEmpty = 0;
    static constexpr uint8_t kReady = 1;

    // Slots per chunk. 4096 * (16B Geometry + 1B state) ~= 70KB per chunk, so
    // the fixed-size granularity costs at most that much over-allocation for
    // the last (partial) chunk of a field.
    static constexpr size_t kChunkSize = 4096;

    // One never-moved block of slots. Public only because SlotRef names it;
    // callers outside this class never touch a Chunk.
    struct Chunk {
        Chunk() {
            for (auto& state : states) {
                state.store(kEmpty, std::memory_order_relaxed);
            }
        }

        // States and geometries are kept in separate arrays so a slot costs
        // one byte more than the bare Geometry rather than a padded 24.
        std::atomic<uint8_t> states[kChunkSize];
        Geometry geometries[kChunkSize];
    };

    // A writer's cursor onto one reserved slot. Only the owning writer holds
    // one, and only for the duration of a single AppendDataAt call.
    struct SlotRef {
        Chunk* chunk{nullptr};
        size_t index{0};
    };

    GeometryChunkStore() {
        for (auto& slab : slabs_) {
            slab.store(nullptr, std::memory_order_relaxed);
        }
    }

    ~GeometryChunkStore() {
        Clear();
    }

    GeometryChunkStore(const GeometryChunkStore&) = delete;
    GeometryChunkStore&
    operator=(const GeometryChunkStore&) = delete;

    // Writer only. Makes storage for `offset` exist and returns a cursor onto
    // it. Throws cleanly: a slab or a chunk is published only after it has
    // been fully allocated, so a failed allocation leaves the store exactly as
    // it was and a retry simply allocates again.
    SlotRef
    Reserve(size_t offset) {
        const size_t chunk_index = offset / kChunkSize;
        const int slab = SlabOf(chunk_index);
        AssertInfo(slab < kMaxSlabs,
                   "geometry cache offset {} exceeds the addressable range",
                   offset);

        auto* entries = slabs_[slab].load(std::memory_order_relaxed);
        if (entries == nullptr) {
            // Value-initialized: every chunk pointer starts null, so the slab
            // is safe to read the instant it is published.
            entries = new std::atomic<Chunk*>[SlabEntries(slab)]();
            slabs_[slab].store(entries, std::memory_order_release);
        }

        auto& entry = entries[PosOf(chunk_index, slab)];
        auto* chunk = entry.load(std::memory_order_relaxed);
        if (chunk == nullptr) {
            auto owned = std::make_unique<Chunk>();
            chunk = owned.get();
            entry.store(chunk, std::memory_order_release);
            owned.release();  // ownership transfers to the directory
        }
        return SlotRef{chunk, offset % kChunkSize};
    }

    // Writer only. True once the slot carries a final geometry.
    static bool
    IsPublished(const SlotRef& ref) {
        return ref.chunk->states[ref.index].load(std::memory_order_relaxed) ==
               kReady;
    }

    // Writer only. Moves `geometry` into the slot and publishes it. The move
    // is noexcept and completes before the release store, so a reader either
    // does not see the slot yet or sees a fully constructed geometry -- never
    // anything in between. A published slot is never written again.
    static void
    Publish(const SlotRef& ref, Geometry&& geometry) {
        static_assert(std::is_nothrow_move_assignable_v<Geometry>,
                      "slot publication requires a noexcept move");
        ref.chunk->geometries[ref.index] = std::move(geometry);
        ref.chunk->states[ref.index].store(kReady, std::memory_order_release);
    }

    // Lock-free reader. Returns nullptr for an offset that was never written,
    // that is not published yet, or that holds an invalid (null/corrupt) row
    // -- all three mean the same thing to a caller: no geometry here.
    const Geometry*
    Get(size_t offset) const {
        const size_t chunk_index = offset / kChunkSize;
        const int slab = SlabOf(chunk_index);
        if (slab >= kMaxSlabs) {
            return nullptr;
        }
        const auto* entries = slabs_[slab].load(std::memory_order_acquire);
        if (entries == nullptr) {
            return nullptr;
        }
        const auto* chunk =
            entries[PosOf(chunk_index, slab)].load(std::memory_order_acquire);
        if (chunk == nullptr) {
            return nullptr;
        }
        const size_t index = offset % kChunkSize;
        if (chunk->states[index].load(std::memory_order_acquire) != kReady) {
            return nullptr;
        }
        const Geometry& geometry = chunk->geometries[index];
        return geometry.IsValid() ? &geometry : nullptr;
    }

    // Destroys every chunk (and with it every cached Geometry). Called by the
    // owner while its GEOS context is still alive, so the Geometry destructors
    // still see a live context.
    void
    Clear() {
        for (int slab = 0; slab < kMaxSlabs; ++slab) {
            auto* entries = slabs_[slab].load(std::memory_order_relaxed);
            if (entries == nullptr) {
                continue;
            }
            const size_t count = SlabEntries(slab);
            for (size_t i = 0; i < count; ++i) {
                delete entries[i].load(std::memory_order_relaxed);
            }
            delete[] entries;
            slabs_[slab].store(nullptr, std::memory_order_relaxed);
        }
    }

 private:
    // Slab s holds (kFirstSlab << s) chunk pointers, so the directory grows
    // geometrically and a published slab is never reallocated. 20 slabs
    // address 16 * (2^20 - 1) chunks, i.e. ~6.8e10 rows -- far beyond any
    // segment. Same series as ChunkSlabDirectory in mmap/ChunkVector.h, which
    // documents the shared arithmetic.
    static constexpr int kFirstSlabLog2 = 4;
    static constexpr size_t kFirstSlab = size_t{1} << kFirstSlabLog2;
    static constexpr int kMaxSlabs = 20;

    static int
    SlabOf(size_t chunk_index) {
        const auto scaled = static_cast<uint64_t>(chunk_index) / kFirstSlab + 1;
        return 63 - __builtin_clzll(scaled);
    }

    static size_t
    PosOf(size_t chunk_index, int slab) {
        return chunk_index - kFirstSlab * ((size_t{1} << slab) - 1);
    }

    static size_t
    SlabEntries(int slab) {
        return kFirstSlab << slab;
    }

    std::atomic<std::atomic<Chunk*>*> slabs_[kMaxSlabs];
};

// Offset-addressed Geometry cache that maintains original field data order.
//
// The cache owns its own GEOS context: every cached Geometry is built and
// destroyed with ctx_, so the cache is fully self-contained and its lifetime
// is independent of the segment that populated it. Combined with the manager
// handing out shared_ptr<SimpleGeometryCache>, an in-flight query keeps the
// cache (and its context) alive even if the owning segment is dropped and
// RemoveSegmentCaches() runs concurrently.
//
// Reads are LOCK-FREE and pointers handed out by GetByOffset() stay valid for
// as long as the caller holds its shared_ptr to the cache -- see
// GeometryChunkStore for why. Writers take write_mutex_, which no reader ever
// touches, so a stream of queries can neither block nor be blocked by the
// growing-segment insert path.
class SimpleGeometryCache {
 public:
    // InitGEOSContext translates an allocation failure into a retriable
    // MemAllocateFailed (GEOS_init_r throws bad_alloc on OOM, it never
    // returns nullptr -- see the helper's comment).
    SimpleGeometryCache() : ctx_(InitGEOSContext("geometry cache")) {
    }

    ~SimpleGeometryCache() {
        // Destroy the cached geometries (each calls GEOSGeom_destroy_r(ctx_,
        // ...)) while ctx_ is still alive, then release the context. No lock
        // is taken: the manager only ever hands out shared_ptr, so the last
        // owner runs this and by then no reader can still be walking the
        // store.
        geometries_.Clear();
        if (ctx_ != nullptr) {
            GEOS_finish_r(ctx_);
            ctx_ = nullptr;
        }
    }

    // The cache owns a GEOS context, so it is neither copyable nor movable.
    SimpleGeometryCache(const SimpleGeometryCache&) = delete;
    SimpleGeometryCache&
    operator=(const SimpleGeometryCache&) = delete;

    // Store the WKB for one row at its ABSOLUTE segment offset.
    //
    // Offset-addressed on purpose (this used to be a tail append): readers
    // resolve rows by absolute segment offset (GetByOffset), while a write can
    // throw a retriable MemAllocateFailed mid-batch (TryParseFromWkb on a
    // transient GEOS reader-allocation failure). With a tail append the rows
    // already written before the throw stayed in the vector, so the retried
    // load/insert appended AFTER them and every later row shifted to the wrong
    // offset -- silently returning the wrong geometry. Writing at the reserved
    // absolute offset makes the operation idempotent: a retry addresses the
    // same slots and alignment can never drift, which is also why publishing
    // the cache in the manager map before it is fully populated
    // (GetOrCreateCache) is safe. Slots skipped over by an out-of-order or
    // failed batch stay unpublished and readers skip them; such rows are never
    // acked/readable until their write lands.
    //
    // FIRST WRITER WINS: a slot that is already published is left untouched
    // and this call returns. An absolute segment offset denotes one immutable
    // row, so every write to it carries the same WKB -- the retry above
    // re-derives exactly the bytes that are already cached. Skipping keeps
    // that idempotence while guaranteeing a published Geometry is never
    // mutated, which is what lets readers hold a bare pointer into it with no
    // lock.
    //
    // A row with corrupt (unparseable) WKB is stored as an INVALID entry --
    // GetByOffset() returns nullptr for it and every reader skips it --
    // instead of throwing. Throwing here would make a single corrupt row fail
    // the whole segment load whenever the geometry cache is enabled (the write
    // paths deliberately keep such rows: add_geometry / bulk_load index a
    // placeholder MBR rather than dropping them, so they DO reach the cache).
    // A transient resource failure (reader allocation) still throws a
    // retriable system error via TryParseFromWkb -- that is not bad data. One
    // exception we cannot tell apart: an OOM INSIDE GEOS parsing surfaces as
    // the same nullptr as corrupt WKB and is deliberately classified as bad
    // data here (see the KNOWN LIMIT note on TryParseFromWkb).
    void
    AppendDataAt(size_t absolute_offset, const char* wkb_data, size_t size) {
        // Writer-only mutex: it serializes concurrent writers against each
        // other, and nothing else. Readers never acquire it, so no amount of
        // query traffic can delay this call and no write can stall a query.
        std::lock_guard<std::mutex> lock(write_mutex_);

        auto slot = geometries_.Reserve(absolute_offset);
        if (GeometryChunkStore::IsPublished(slot)) {
            return;  // first writer wins, see the contract above
        }

        if (size == 0 || wkb_data == nullptr) {
            // Null/empty geometry - publish an invalid entry
            PublishAt(slot, absolute_offset, Geometry());
            return;
        }
        Geometry geometry;
        if (!geometry.TryParseFromWkb(ctx_, wkb_data, size)) {
            static std::atomic<int64_t> last_cache_parse_log_us{0};
            if (ShouldLogGeometryThrottled(last_cache_parse_log_us)) {
                LOG_WARN(
                    "unparseable WKB at cache offset {}; caching an invalid "
                    "placeholder entry, readers will skip it (further "
                    "occurrences suppressed briefly)",
                    absolute_offset);
            } else {
                LOG_DEBUG("unparseable WKB at cache offset {}",
                          absolute_offset);
            }
            PublishAt(slot, absolute_offset, Geometry());
            return;
        }
        PublishAt(slot, absolute_offset, std::move(geometry));
    }

    // Get Geometry by absolute segment offset. Lock-free, and the returned
    // pointer stays valid for as long as the caller holds its shared_ptr to
    // this cache (chunks never move and a published slot is never rewritten).
    //
    // An offset with no geometry returns nullptr -- the same "no geometry
    // here, skip the row" answer as an invalid entry -- it must NOT throw. On
    // a growing segment there is a real window where a reader legitimately
    // outruns this cache: SegmentGrowingImpl::Insert feeds the R-Tree
    // (AppendingIndex) before it fills the cache (BuildGeometryCacheForInsert),
    // so a concurrent query whose coarse bitmap is sized by the index's
    // Count() can probe an offset the cache has not reached yet. Those rows
    // are not yet acked, so reporting them as non-matching is correct;
    // throwing a non-retriable UnexpectedError out of a read path is not.
    const Geometry*
    GetByOffset(size_t offset) const {
        return geometries_.Get(offset);
    }

    // Highest absolute offset published so far, plus one. Lock-free, and
    // advisory only: the cache is written at absolute offsets that may arrive
    // out of order, so this is a high-water mark, not a count of populated
    // rows, and it is never used to bound a read (GetByOffset resolves each
    // offset on its own).
    size_t
    Size() const {
        return size_.load(std::memory_order_acquire);
    }

 private:
    // Writer only (called under write_mutex_).
    void
    PublishAt(const GeometryChunkStore::SlotRef& slot,
              size_t absolute_offset,
              Geometry&& geometry) {
        GeometryChunkStore::Publish(slot, std::move(geometry));
        // After the slot, so a reader can never see a high-water mark that
        // outruns the data. Plain load/store: writers are serialized.
        if (absolute_offset >= size_.load(std::memory_order_relaxed)) {
            size_.store(absolute_offset + 1, std::memory_order_release);
        }
    }

    // ctx_ is declared first so it is destroyed last (after geometries_),
    // guaranteeing the Geometry destructors still see a live context.
    GEOSContextHandle_t ctx_{nullptr};  // Context owned by this cache
    std::mutex write_mutex_;            // Writers only; readers never take it
    std::atomic<size_t> size_{0};
    GeometryChunkStore geometries_;  // Chunked storage of Geometry objects
};

// Global cache instance per segment+field
class SimpleGeometryCacheManager {
 public:
    static SimpleGeometryCacheManager&
    Instance() {
        static SimpleGeometryCacheManager instance;
        return instance;
    }

    SimpleGeometryCacheManager() = default;

    // Returns a shared_ptr so callers keep the cache alive for the duration of
    // their use even if RemoveSegmentCaches drops it concurrently.
    std::shared_ptr<SimpleGeometryCache>
    GetOrCreateCache(uint64_t segment_instance_uid,
                     int64_t segment_id,
                     FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_instance_uid, segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return it->second;
        }

        auto cache = std::make_shared<SimpleGeometryCache>();
        caches_.emplace(key, cache);
        return cache;
    }

    std::shared_ptr<SimpleGeometryCache>
    GetCache(uint64_t segment_instance_uid,
             int64_t segment_id,
             FieldId field_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto key = MakeCacheKey(segment_instance_uid, segment_id, field_id);
        auto it = caches_.find(key);
        if (it != caches_.end()) {
            return it->second;
        }
        return nullptr;
    }

    // Remove all caches owned by one segment OBJECT -- called from that
    // object's destructor, and deliberately NOT matching any other live
    // instance that shares the same logical segment id (growing/sealed twin,
    // or an older/newer version of the same sealed segment).
    void
    RemoveSegmentCaches(uint64_t segment_instance_uid, int64_t segment_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto segment_prefix =
            MakeSegmentCachePrefix(segment_instance_uid, segment_id);
        auto it = caches_.begin();
        while (it != caches_.end()) {
            if (it->first.substr(0, segment_prefix.length()) ==
                segment_prefix) {
                it = caches_.erase(it);
            } else {
                ++it;
            }
        }
    }

 private:
    SimpleGeometryCacheManager(const SimpleGeometryCacheManager&) = delete;
    SimpleGeometryCacheManager&
    operator=(const SimpleGeometryCacheManager&) = delete;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<SimpleGeometryCache>>
        caches_;
};

}  // namespace exec

}  // namespace milvus
