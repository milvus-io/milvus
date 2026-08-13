// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <span>
#include <type_traits>

#include "common/FastMem.h"
#include "common/Span.h"
#include "common/TypeTraits.h"
#include "common/Utils.h"
#include "mmap/ChunkData.h"
#include "segcore/SegcoreConfig.h"
#include "storage/MmapManager.h"

namespace milvus {

// A pinned view of one published chunk generation.
//
// Acquired once per read operation and read through for that operation's whole
// lifetime -- including past the end of the call that acquired it, which is
// what the Knowhere brute-force iterators need. `storage` keeps the generation
// alive: clear() swaps the container's collection, so reclamation returns the
// segment's memory immediately while a holder keeps walking the collection it
// pinned. Once acquired, no lock is taken again.
//
// `count` is the chunk count published with that generation. Row visibility is
// NOT this snapshot's job -- the caller's MVCC bound governs which rows inside
// those chunks it may read, and chunks appended after the snapshot are simply
// never addressed.
struct ChunkSnapshot {
    std::shared_ptr<void> storage;
    int64_t count{0};

    bool
    empty() const {
        return count == 0;
    }
};

template <typename Type>
class ChunkVectorBase {
 public:
    virtual ~ChunkVectorBase() = default;

    // Pin the current generation together with its chunk count, so the two
    // cannot come from different generations. This is the ONLY place a read
    // operation synchronises with the writer.
    virtual ChunkSnapshot
    acquire() const = 0;

    // Snapshot-addressed reads. These take no lock: the generation behind
    // `snap` is pinned by the caller and cannot be swapped out under it.
    virtual void*
    get_chunk_data(const ChunkSnapshot& snap, int64_t index) const = 0;
    virtual int64_t
    get_chunk_size(const ChunkSnapshot& snap, int64_t index) const = 0;
    virtual SpanBase
    get_span(const ChunkSnapshot& snap, int64_t chunk_id) const = 0;

    virtual void
    emplace_to_at_least(int64_t chunk_num, int64_t chunk_size) = 0;
    virtual void
    copy_to_chunk(int64_t chunk_id,
                  int64_t offest,
                  const Type* data,
                  int64_t length,
                  const std::optional<CheckDataValid>& check_data_valid) = 0;
    virtual void*
    get_chunk_data(int64_t index) = 0;
    virtual int64_t
    get_chunk_size(int64_t index) = 0;
    virtual int64_t
    get_element_size() = 0;
    virtual int64_t
    get_element_offset(int64_t index) = 0;
    virtual ChunkViewType<Type>
    view_element(int64_t chunk_id, int64_t chunk_offset) = 0;
    int64_t
    size() const {
        return counter_;
    }
    virtual void
    clear() = 0;
    virtual SpanBase
    get_span(int64_t chunk_id) = 0;

    virtual bool
    is_mmap() const = 0;

 protected:
    std::atomic<int64_t> counter_ = 0;
};
template <typename Type>
using ChunkVectorPtr = std::unique_ptr<ChunkVectorBase<Type>>;

// Append-only storage for a ThreadSafeChunkVector's chunks: a FIXED spine of
// atomic slab pointers, where slab s holds (kFirstSlab << s) slots, so the
// directory grows geometrically without ever moving a published slab or
// element. Single writer (serialized by the owner's mutex_), lock-free
// readers: a reader may index any slot below a count it learned together
// with this directory (ChunkSnapshot latches the two under the owner's
// mutex), because a slab is fully allocated before its pointer is published
// and an element is move-constructed into place before counter_ counts it.
//
// Exception contract -- the reason this exists instead of
// tbb::concurrent_vector: oneTBB specifies reserve() as concurrently unsafe
// with every other member (only grow_by / grow_to_at_least / push_back and
// element access compose concurrently), while its concurrently-safe growth
// calls advance size BEFORE allocating storage, so an allocation failure can
// leave counted-but-unconstructed slots behind for a retry to publish. Here
// Reserve() allocates raw slabs and publishes their pointers only on
// success -- a throw changes nothing an observer or a retry can see -- and
// EmplaceBack() is a noexcept move into storage Reserve() already produced.
template <typename ChunkImpl>
class ChunkSlabDirectory {
 public:
    static constexpr int64_t kFirstSlabLog2 = 4;
    static constexpr int64_t kFirstSlab = int64_t{1} << kFirstSlabLog2;
    // Slabs 0..s together hold kFirstSlab * (2^(s+1) - 1) slots; 40 slabs
    // address far more chunks than any segment can hold.
    static constexpr int kMaxSlabs = 40;

    ChunkSlabDirectory() {
        for (auto& slab : slabs_) {
            slab.store(nullptr, std::memory_order_relaxed);
        }
    }

    ~ChunkSlabDirectory() {
        for (int64_t i = 0; i < size_; ++i) {
            (*this)[i].~ChunkImpl();
        }
        std::allocator<ChunkImpl> alloc;
        for (int s = 0; s < kMaxSlabs; ++s) {
            auto* slab = slabs_[s].load(std::memory_order_relaxed);
            if (slab != nullptr) {
                alloc.deallocate(slab, kFirstSlab << s);
            }
        }
    }

    ChunkSlabDirectory(const ChunkSlabDirectory&) = delete;
    ChunkSlabDirectory&
    operator=(const ChunkSlabDirectory&) = delete;

    // Writer only. Ensures raw storage exists for slot indices [0, capacity).
    // Throws cleanly: no pointer is published unless its slab was fully
    // allocated, so a failed call leaves the directory exactly as it was.
    void
    Reserve(int64_t capacity) {
        if (capacity <= 0) {
            return;
        }
        const int last_slab = SlabOf(capacity - 1);
        AssertInfo(last_slab < kMaxSlabs,
                   "chunk directory capacity {} exceeds the addressable "
                   "range ({} slabs)",
                   capacity,
                   kMaxSlabs);
        std::allocator<ChunkImpl> alloc;
        for (int s = 0; s <= last_slab; ++s) {
            if (slabs_[s].load(std::memory_order_relaxed) != nullptr) {
                continue;
            }
            const int64_t entries = kFirstSlab << s;
            slabs_[s].store(alloc.allocate(entries), std::memory_order_release);
        }
    }

    // Writer only; storage for the new slot must have been Reserve()d. The
    // move is noexcept (asserted by the owner), so the element either fully
    // exists below size() or was never counted.
    void
    EmplaceBack(ChunkImpl&& chunk) {
        const int64_t index = size_;
        const int slab = SlabOf(index);
        auto* slot =
            slabs_[slab].load(std::memory_order_relaxed) + PosOf(index, slab);
        ::new (static_cast<void*>(slot)) ChunkImpl(std::move(chunk));
        ++size_;
    }

    ChunkImpl&
    operator[](int64_t index) {
        const int slab = SlabOf(index);
        return slabs_[slab].load(std::memory_order_relaxed)[PosOf(index, slab)];
    }

    const ChunkImpl&
    operator[](int64_t index) const {
        const int slab = SlabOf(index);
        return slabs_[slab].load(std::memory_order_relaxed)[PosOf(index, slab)];
    }

 private:
    // Slot i lives in slab floor(log2(i / kFirstSlab + 1)) because the slabs
    // form a geometric series -- the same layout as GrowingOffsetMapping's
    // ChunkedArray, which documents the shared reasoning.
    static int
    SlabOf(int64_t index) {
        const auto scaled = static_cast<uint64_t>(index) / kFirstSlab + 1;
        return 63 - __builtin_clzll(scaled);
    }

    static int64_t
    PosOf(int64_t index, int slab) {
        return index - kFirstSlab * ((int64_t{1} << slab) - 1);
    }

    std::atomic<ChunkImpl*> slabs_[kMaxSlabs];
    // Writer-only bookkeeping for destruction; readers never consult it --
    // they bound themselves by the count their ChunkSnapshot latched.
    int64_t size_ = 0;
};

template <typename Type,
          typename ChunkImpl = FixedVector<Type>,
          bool IsMmap = false>
class ThreadSafeChunkVector : public ChunkVectorBase<Type> {
 public:
    ThreadSafeChunkVector(
        storage::MmapChunkDescriptorPtr descriptor = nullptr) {
        mmap_descriptor_ = descriptor;
    }

    void
    emplace_to_at_least(int64_t chunk_num, int64_t chunk_size) override {
        std::unique_lock<std::shared_mutex> lck(this->mutex_);
        if (chunk_num <= this->counter_) {
            return;
        }
        // Allocate all raw storage before constructing or publishing
        // anything, and construct each potentially-throwing chunk outside the
        // directory. Reserve() throws cleanly -- a failure changes nothing a
        // reader or a retry can observe -- and the only operation left after
        // it succeeds is a noexcept move into pre-reserved storage, so
        // counter_ remains the exact successfully-constructed prefix.
        vec_->Reserve(chunk_num);
        static_assert(std::is_nothrow_move_constructible_v<ChunkImpl>,
                      "chunk publication requires a noexcept move");
        while (this->counter_ < chunk_num) {
            ChunkImpl chunk = [&]() -> ChunkImpl {
                if constexpr (IsMmap) {
                    return ChunkImpl(chunk_size, mmap_descriptor_);
                } else {
                    return ChunkImpl(chunk_size);
                }
            }();
            vec_->EmplaceBack(std::move(chunk));

            // Keep the uniform-size optimization on the same committed
            // prefix as counter_. A failed reserve or ChunkImpl construction
            // leaves this metadata unchanged; once emplace_back succeeds,
            // the reserved slot and noexcept move make publication complete.
            if (uniform_chunk_size_ == kUniformUnset) {
                uniform_chunk_size_ = chunk_size;
            } else if (uniform_chunk_size_ != chunk_size) {
                uniform_chunk_size_ = kMixedChunkSizes;
            }
            ++this->counter_;
        }
    }

    void
    copy_to_chunk(
        int64_t chunk_id,
        int64_t offset,
        const Type* data,
        int64_t length,
        const std::optional<CheckDataValid>& check_data_valid) override {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        const auto counter = this->counter_.load();
        AssertInfo(chunk_id < counter,
                   fmt::format("index out of range, index={}, counter_={}",
                               chunk_id,
                               counter));
        if constexpr (!IsMmap || !IsVariableType<Type>) {
            auto ptr = (Type*)(*vec_)[chunk_id].data();
            AssertInfo(
                offset + length <= (*vec_)[chunk_id].size(),
                fmt::format(
                    "index out of chunk range, offset={}, length={}, size={}",
                    offset,
                    length,
                    (*vec_)[chunk_id].size()));
            if constexpr (std::is_trivially_copyable_v<Type>) {
                milvus::fastmem::FastMemcpy(
                    ptr + offset, data, length * sizeof(Type));
            } else {
                std::copy_n(data, length, ptr + offset);
            }
        } else {
            (*vec_)[chunk_id].set(data, offset, length, check_data_valid);
        }
    }

    void
    copy_array_rows_to_chunk(int64_t chunk_id,
                             size_t offset,
                             std::span<const ScalarFieldProto* const> rows,
                             const proto::schema::TypeSchema& type) {
        static_assert(std::is_same_v<Type, ArrayValue> && IsMmap,
                      "copy_array_rows_to_chunk is only available for mmap "
                      "ArrayValue chunks");
        std::unique_lock<std::shared_mutex> lck(mutex_);
        const auto counter = this->counter_.load();
        AssertInfo(chunk_id < counter,
                   "nested ARRAY chunk index {} out of range {}",
                   chunk_id,
                   counter);
        (*vec_)[chunk_id].set_rows(rows, offset, type);
    }

    ChunkViewType<Type>
    view_element(int64_t chunk_id, int64_t chunk_offset) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        return view_chunk_element((*vec_)[chunk_id], chunk_offset);
    }

    void*
    get_chunk_data(int64_t index) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        const auto counter = this->counter_.load();
        AssertInfo(
            index < counter,
            fmt::format(
                "index out of range, index={}, counter_={}", index, counter));
        return (*vec_)[index].data();
    }

    int64_t
    get_chunk_size(int64_t index) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        const auto counter = this->counter_.load();
        AssertInfo(
            index < counter,
            fmt::format(
                "index out of range, index={}, counter_={}", index, counter));
        return (*vec_)[index].size();
    }

    void
    clear() override {
        // Allocate BEFORE touching any state. make_shared can throw, and
        // counter_ = 0 paired with the old collection still in place is
        // unrecoverable: num_chunk() reports empty so nothing retries the
        // clear, while emplace_to_at_least() sees a non-empty container and
        // never advances the counter back, so every later chunk access asserts.
        auto replacement = std::make_shared<ChunkSlabDirectory<ChunkImpl>>();
        {
            std::unique_lock<std::shared_mutex> lck(mutex_);
            // Swap the collection instead of clearing it in place: holders of
            // an acquire()d snapshot keep the old chunks (and the buffers
            // brute-force iterators still point into) alive until their last
            // reference dies. Clearing in place would not do -- besides
            // freeing those buffers, destroying elements in place races with
            // the snapshot readers still walking them. Everything else observes the
            // container through this mutex or the atomic counter, so the swap
            // is invisible to them. All stores below are noexcept, so the
            // counter and the collection commit together.
            this->counter_ = 0;
            uniform_chunk_size_ = kUniformUnset;
            vec_.swap(replacement);
        }
        // `replacement` now owns the old collection. Letting it die out here
        // frees the chunks outside the lock -- or not at all yet, if a
        // SearchResult still pins them through a ChunkSnapshot.
    }

    ChunkSnapshot
    acquire() const override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        // Both under one acquisition: a count from a different generation
        // would address chunks the pinned collection does not have.
        return ChunkSnapshot{vec_, this->counter_.load()};
    }

    void*
    get_chunk_data(const ChunkSnapshot& snap, int64_t index) const override {
        return (*pinned(snap))[checked(snap, index)].data();
    }

    int64_t
    get_chunk_size(const ChunkSnapshot& snap, int64_t index) const override {
        return (*pinned(snap))[checked(snap, index)].size();
    }

    SpanBase
    get_span(const ChunkSnapshot& snap, int64_t chunk_id) const override {
        if constexpr (IsMmap && (std::is_same_v<std::string, Type> ||
                                 std::is_same_v<ArrayValue, Type>)) {
            return SpanBase(get_chunk_data(snap, chunk_id),
                            get_chunk_size(snap, chunk_id),
                            sizeof(ChunkViewType<Type>));
        } else {
            return SpanBase(get_chunk_data(snap, chunk_id),
                            get_chunk_size(snap, chunk_id),
                            sizeof(Type));
        }
    }

    int64_t
    get_element_size() override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        if constexpr (IsMmap && (std::is_same_v<std::string, Type> ||
                                 std::is_same_v<ArrayValue, Type>)) {
            return sizeof(ChunkViewType<Type>);
        }
        return sizeof(Type);
    }

    int64_t
    get_element_offset(int64_t index) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        // Every chunk is created with the same size in the only mode that
        // produces more than one of them (ConcurrentVectorImpl passes
        // elements_per_row_ * size_per_chunk_ on every call), so the offset is
        // a multiplication. Summing `index` chunk sizes instead made this
        // O(index), and the expression layer calls it once per chunk --
        // chunk_data_impl -> InsertRecord::get_span_base -- so a scan over one
        // growing segment was O(num_chunks^2). With chunkRows defaulting to
        // 128 that is ~300M iterations on a multi-million-row segment.
        //
        // The sum is kept for the mixed-size case, which stays correct without
        // anyone having to prove that mode unreachable.
        if (uniform_chunk_size_ > 0) {
            return index * uniform_chunk_size_;
        }
        int64_t offset = 0;
        for (int i = 0; i < index; i++) {
            offset += (*vec_)[i].size();
        }
        return offset;
    }

    SpanBase
    get_span(int64_t chunk_id) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        if constexpr (IsMmap && (std::is_same_v<std::string, Type> ||
                                 std::is_same_v<ArrayValue, Type>)) {
            return SpanBase(get_chunk_data(chunk_id),
                            get_chunk_size(chunk_id),
                            sizeof(ChunkViewType<Type>));
        } else {
            return SpanBase(get_chunk_data(chunk_id),
                            get_chunk_size(chunk_id),
                            sizeof(Type));
        }
    }

    bool
    is_mmap() const override {
        return mmap_descriptor_ != nullptr;
    }

 private:
    // The pinned generation, recovered from the type-erased handle. Safe
    // because only this container ever produces a ChunkSnapshot for itself.
    static ChunkSlabDirectory<ChunkImpl>*
    pinned(const ChunkSnapshot& snap) {
        AssertInfo(snap.storage != nullptr,
                   "chunk snapshot was never acquired");
        return static_cast<ChunkSlabDirectory<ChunkImpl>*>(snap.storage.get());
    }

    static int64_t
    checked(const ChunkSnapshot& snap, int64_t index) {
        AssertInfo(index >= 0 && index < snap.count,
                   fmt::format("index out of range, index={}, snapshot "
                               "chunks={}",
                               index,
                               snap.count));
        return index;
    }

    static ChunkViewType<Type>
    view_chunk_element(ChunkImpl& chunk, int64_t chunk_offset) {
        if constexpr (IsMmap) {
            return chunk.view(chunk_offset);
        } else if constexpr (std::is_same_v<std::string, Type>) {
            return std::string_view(chunk[chunk_offset].data(),
                                    chunk[chunk_offset].size());
        } else if constexpr (std::is_same_v<Array, Type>) {
            auto& src = chunk[chunk_offset];
            return ArrayView(const_cast<char*>(src.data()),
                             src.length(),
                             src.byte_size(),
                             src.get_element_type(),
                             src.get_offsets_data());
        } else if constexpr (std::is_same_v<ArrayValue, Type>) {
            return chunk[chunk_offset].View();
        } else if constexpr (std::is_same_v<VectorArray, Type>) {
            auto& src = chunk[chunk_offset];
            return VectorArrayView(const_cast<char*>(src.data()),
                                   src.dim(),
                                   src.length(),
                                   src.byte_size(),
                                   src.get_element_type());
        } else if constexpr (std::is_same_v<Json, Type>) {
            return Json(chunk[chunk_offset].c_str(),
                        chunk[chunk_offset].size());
        } else {
            return chunk[chunk_offset];
        }
    }

    // Common size of every chunk, or kMixedChunkSizes once a chunk of a
    // different size has been appended. Guarded by mutex_, like the container
    // itself -- get_element_offset() already holds it, so making this atomic
    // would only add a memory-model question with no reader to benefit.
    static constexpr int64_t kUniformUnset = 0;
    static constexpr int64_t kMixedChunkSizes = -1;
    int64_t uniform_chunk_size_ = kUniformUnset;

    mutable std::shared_mutex mutex_;
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    // Held via shared_ptr so clear() can swap it while acquire() holders keep
    // the previous collection alive. The shared_ptr itself is only read and
    // written under mutex_.
    //
    // An owned slab directory, NOT std::deque and NOT tbb::concurrent_vector.
    // The snapshot accessors above index this container with NO lock while
    // emplace_to_at_least may be appending. deque cannot support that: it
    // grows by reallocating the node map an unsynchronised operator[] walks,
    // so a reader can dereference a map array the writer already freed.
    // oneTBB's concurrent_vector composes element access with its grow calls,
    // but the one call the writer needs before publishing -- reserve(), which
    // keeps an allocation failure from ever counting unconstructed slots --
    // is specified as concurrently UNSAFE with every other member, element
    // access included; depending on the 2021.9 implementation happening to
    // leave published segments in place is exactly the kind of implicit
    // contract this redesign exists to remove. ChunkSlabDirectory provides
    // the two guarantees readers actually need by construction (nothing a
    // reader can see ever moves; a throw changes nothing a reader can see);
    // see its class comment.
    std::shared_ptr<ChunkSlabDirectory<ChunkImpl>> vec_ =
        std::make_shared<ChunkSlabDirectory<ChunkImpl>>();
};

template <typename Type>
ChunkVectorPtr<Type>
SelectChunkVectorPtr(storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    if constexpr (!IsVariableType<Type>) {
        if (mmap_descriptor != nullptr) {
            return std::make_unique<
                ThreadSafeChunkVector<Type, FixedLengthChunk<Type>, true>>(
                mmap_descriptor);
        } else {
            return std::make_unique<ThreadSafeChunkVector<Type>>();
        }
    } else if constexpr (IsVariableTypeSupportInChunk<Type>) {
        if (mmap_descriptor != nullptr) {
            return std::make_unique<
                ThreadSafeChunkVector<Type, VariableLengthChunk<Type>, true>>(
                mmap_descriptor);
        } else {
            return std::make_unique<ThreadSafeChunkVector<Type>>();
        }
    } else {
        return std::make_unique<ThreadSafeChunkVector<Type>>();
    }
}
}  // namespace milvus
