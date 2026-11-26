// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "index/growing/MutableScalarGrowingIndex.h"

#include <absl/container/btree_map.h>
#include <absl/container/inlined_vector.h>
#include <algorithm>
#include <atomic>
#include <exception>
#include <iterator>
#include <limits>
#include <new>
#include <list>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cachinglayer/Manager.h"
#include "common/EasyAssert.h"
#include "index/SharedValidityBitmap.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"

namespace milvus::index {
namespace {

struct ScalarAllocationCounter {
    void
    Charge(size_t size) {
        if (size == 0)
            return;
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(size), 0});
        bytes.fetch_add(size, std::memory_order_relaxed);
    }
    void
    Refund(size_t size) noexcept {
        if (size == 0)
            return;
        cachinglayer::Manager::GetInstance().RefundLoadedResource(
            {static_cast<int64_t>(size), 0});
        bytes.fetch_sub(size, std::memory_order_relaxed);
    }
    std::atomic<size_t> bytes{0};
};
using ByteCounter = std::shared_ptr<ScalarAllocationCounter>;

// Counts live allocator requests (including spare capacity), not process RSS.
// The counter outlives the owner when a reader still retains its storage.
template <typename T>
struct ScalarAllocator {
    using value_type = T;
    ByteCounter bytes;

    explicit ScalarAllocator(ByteCounter counter) : bytes(std::move(counter)) {
    }
    template <typename U>
    ScalarAllocator(const ScalarAllocator<U>& other) : bytes(other.bytes) {
    }
    T*
    allocate(size_t n) {
        auto* result = std::allocator<T>{}.allocate(n);
        try {
            bytes->Charge(n * sizeof(T));
        } catch (...) {
            std::allocator<T>{}.deallocate(result, n);
            throw;
        }
        return result;
    }
    void
    deallocate(T* p, size_t n) noexcept {
        std::allocator<T>{}.deallocate(p, n);
        bytes->Refund(n * sizeof(T));
    }
    template <typename U>
    bool
    operator==(const ScalarAllocator<U>& other) const noexcept {
        return bytes == other.bytes;
    }
};

struct ScalarMemoryCharge {
    ByteCounter counter;
    size_t bytes;
    ScalarMemoryCharge(ByteCounter counter, size_t bytes)
        : counter(std::move(counter)), bytes(bytes) {
        this->counter->Charge(bytes);
    }
    ~ScalarMemoryCharge() {
        counter->Refund(bytes);
    }
    ScalarMemoryCharge(const ScalarMemoryCharge&) = delete;
    ScalarMemoryCharge&
    operator=(const ScalarMemoryCharge&) = delete;
};

using ScalarString =
    std::basic_string<char, std::char_traits<char>, ScalarAllocator<char>>;

template <typename T>
using ScalarKey =
    std::conditional_t<std::is_same_v<T, std::string_view>, ScalarString, T>;

template <typename T>
struct ScalarLess {
    using is_transparent = void;
    bool
    operator()(const T& a, const T& b) const {
        return a < b;
    }
};

template <typename T>
ScalarKey<T>
OwnScalarKey(const T& value, const ByteCounter& bytes) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return ScalarString(
            value.begin(), value.end(), ScalarAllocator<char>(bytes));
    } else {
        return value;
    }
}

template <typename T>
ReaderCaps
MutableScalarCaps() {
    return {.predicate = true,
            .pattern_match = std::is_same_v<T, std::string_view>};
}

template <typename T>
struct MutableScalarStorage {
    using Offsets = absl::InlinedVector<int64_t, 2, ScalarAllocator<int64_t>>;
    using Words = std::vector<uint64_t, ScalarAllocator<uint64_t>>;
    struct CachedMembership {
        ScalarKey<T> key;
        Words words;
    };
    using Cache =
        std::list<CachedMembership, ScalarAllocator<CachedMembership>>;
    struct Posting {
        explicit Posting(const ByteCounter& bytes)
            : offsets(ScalarAllocator<int64_t>(bytes)) {
        }
        Offsets offsets;
        // Protected by cache_mutex under a shared tree lock, or by the
        // exclusive tree lock. B-tree relocation does not invalidate list IDs.
        mutable size_t hits = 0;
        mutable bool cached = false;
        mutable typename Cache::iterator cache;
    };
    using Pair = std::pair<const ScalarKey<T>, Posting>;
    using Tree = absl::
        btree_map<ScalarKey<T>, Posting, ScalarLess<T>, ScalarAllocator<Pair>>;

    MutableScalarStorage(DataType type,
                         MutableScalarIndexOptions options,
                         ByteCounter bytes)
        : bytes(std::move(bytes)),
          object_charge(this->bytes, sizeof(MutableScalarStorage)),
          type(type),
          options(options),
          tree(ScalarLess<T>{}, ScalarAllocator<Pair>(this->bytes)),
          cache(ScalarAllocator<CachedMembership>(this->bytes)) {
    }

    void
    CheckHealthy() const {
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    // Caller holds tree's exclusive lock, or shared lock plus cache_mutex.
    void
    Evict(typename Cache::iterator item) const {
        const auto entry = tree.find(item->key);
        if (entry != tree.end()) {
            entry->second.cached = false;
        }
        cache_bytes -= item->words.capacity() * sizeof(uint64_t);
        cache.erase(item);
    }

    // Query-local result, latest shared membership. Older publications read
    // only their own prefix; later inserts never change existing memberships.
    void
    Emit(const ScalarKey<T>& key,
         const Posting& posting,
         TargetBitmap& result,
         int64_t count,
         bool point_query) const {
        if (point_query && options.hot_bitmap_bytes != 0) {
            std::lock_guard cache_lock(cache_mutex);
            if (posting.hits < options.hot_query_threshold) {
                ++posting.hits;
            }
            if (!posting.cached &&
                posting.hits >= options.hot_query_threshold &&
                posting.offsets.size() >= options.hot_posting_threshold) {
                const auto words =
                    static_cast<size_t>(posting.offsets.back()) / 64 + 1;
                if (words <= options.hot_bitmap_bytes / sizeof(uint64_t)) {
                    const auto required = words * sizeof(uint64_t);
                    while (!cache.empty() &&
                           cache_bytes > options.hot_bitmap_bytes - required) {
                        Evict(cache.begin());
                    }
                    // No query correctness depends on cache admission. Build
                    // privately; allocation failure leaves canonical postings.
                    try {
                        Words bitmap(
                            words, 0, ScalarAllocator<uint64_t>(bytes));
                        for (const auto offset : posting.offsets) {
                            bitmap[offset / 64] |= uint64_t{1} << (offset % 64);
                        }
                        const auto capacity =
                            bitmap.capacity() * sizeof(uint64_t);
                        if (capacity <=
                            options.hot_bitmap_bytes - cache_bytes) {
                            cache.push_back({OwnScalarKey<T>(key, bytes),
                                             std::move(bitmap)});
                            posting.cache = std::prev(cache.end());
                            posting.cached = true;
                            cache_bytes += capacity;
                        }
                    } catch (const std::bad_alloc&) {
                        // Optional acceleration only; never catches append or
                        // publication failures and never publishes partial data.
                    }
                }
            }
            if (posting.cached) {
                auto item = posting.cache;
                cache.splice(cache.end(), cache, item);
                const auto words = std::min(
                    item->words.size(), (static_cast<size_t>(count) + 63) / 64);
                for (size_t i = 0; i < words; ++i) {
                    auto word = item->words[i];
                    if (i == static_cast<size_t>(count) / 64 && count % 64) {
                        word &= (uint64_t{1} << (count % 64)) - 1;
                    }
                    result.data()[i] |= word;
                }
                return;
            }
        }
        for (const auto offset : posting.offsets) {
            if (offset >= count) {
                break;
            }
            result[offset] = true;
        }
    }

    // Tree, postings, validity and hot cache share one self-charged counter.
    // Segment statistics may report it but must not charge it again.
    ByteCounter bytes;
    ScalarMemoryCharge object_charge;
    const DataType type;
    const MutableScalarIndexOptions options;
    mutable std::shared_mutex mutex;
    Tree tree;
    mutable std::mutex cache_mutex;
    mutable Cache cache;
    mutable size_t cache_bytes = 0;
    SharedValidityBitmap validity;
    std::shared_ptr<ScalarMemoryCharge> validity_charge;
    int64_t accepted_end = 0;
    int64_t published_end = -1;
    // Like a built vector engine, a failed mutating append is terminal. Every
    // reader checks this under the same lock before touching potentially
    // modified backend state. Publication-only failure remains retryable.
    std::exception_ptr failure;
};

template <typename T>
class MutableScalarReader final
    : public IIndexReaderBase,
      public IScalarPredicateReader<T>,
      public INullReader,
      public PatternMatchReaderAdapter<MutableScalarReader<T>, T, true> {
 public:
    MutableScalarReader(std::shared_ptr<MutableScalarStorage<T>> storage,
                        int64_t count,
                        SharedValidityBitmap::Snapshot validity,
                        std::shared_ptr<ScalarMemoryCharge> validity_charge)
        : storage_(std::move(storage)),
          count_(count),
          validity_(std::move(validity)),
          validity_charge_(std::move(validity_charge)) {
    }
    ReaderCaps
    Caps() const override {
        return MutableScalarCaps<T>();
    }
    Domain
    CoordDomain() const override {
        return Domain::Row;
    }
    int64_t
    Count() const override {
        return count_;
    }
    DataType
    ValueType() const override {
        return storage_->type;
    }

    TargetBitmap
    In(size_t n, const T* values) const override {
        AssertInfo(n == 0 || values != nullptr,
                   "mutable scalar membership has null query values");
        std::shared_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        TargetBitmap result(count_, false);
        for (size_t i = 0; i < n; ++i) {
            const auto it = storage_->tree.find(values[i]);
            if (it != storage_->tree.end()) {
                storage_->Emit(it->first, it->second, result, count_, true);
            }
        }
        return result;
    }
    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        auto result = In(n, values);
        result.flip();
        result &= IsNotNull();
        return result;
    }
    TargetBitmap
    Range(const T& value, CompareOp op) const override {
        if (op == CompareOp::Equal)
            return In(1, &value);
        if (op == CompareOp::NotEqual)
            return NotIn(1, &value);
        std::shared_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        const auto& tree = storage_->tree;
        switch (op) {
            case CompareOp::GreaterThan:
                return Interval(tree.upper_bound(value), tree.end());
            case CompareOp::GreaterEqual:
                return Interval(tree.lower_bound(value), tree.end());
            case CompareOp::LessThan:
                return Interval(tree.begin(), tree.lower_bound(value));
            case CompareOp::LessEqual:
                return Interval(tree.begin(), tree.upper_bound(value));
            default:
                ThrowInfo(OpTypeInvalid,
                          "unsupported mutable scalar comparison");
        }
    }
    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
        std::shared_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        if (hi < lo || (!(lo < hi) && (!lo_inc || !hi_inc))) {
            return TargetBitmap(count_, false);
        }
        const auto& tree = storage_->tree;
        return Interval(lo_inc ? tree.lower_bound(lo) : tree.upper_bound(lo),
                        hi_inc ? tree.upper_bound(hi) : tree.lower_bound(hi));
    }
    TargetBitmap
    IsNotNull() const override {
        std::shared_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        return validity_.ToBitmap();
    }
    TargetBitmap
    IsNull() const override {
        auto result = IsNotNull();
        result.flip();
        return result;
    }
    bool
    ShouldUseForOpImpl(PatternOp op, std::string_view) const {
        return op == PatternOp::PrefixMatch;
    }
    TargetBitmap
    PatternMatchImpl(std::string_view pattern, PatternOp op) const {
        if constexpr (std::is_same_v<T, std::string_view>) {
            AssertInfo(op == PatternOp::PrefixMatch,
                       "mutable scalar reader only supports literal prefix");
            std::shared_lock lock(storage_->mutex);
            storage_->CheckHealthy();
            TargetBitmap result(count_, false);
            for (auto it = storage_->tree.lower_bound(pattern);
                 it != storage_->tree.end() &&
                 std::string_view(it->first).starts_with(pattern);
                 ++it) {
                storage_->Emit(it->first, it->second, result, count_, false);
            }
            return result;
        } else {
            ThrowInfo(UnexpectedError,
                      "pattern query on numeric scalar reader");
        }
    }

 private:
    using Iterator = typename MutableScalarStorage<T>::Tree::const_iterator;
    TargetBitmap
    Interval(Iterator first, Iterator last) const {
        TargetBitmap result(count_, false);
        for (auto it = first; it != last; ++it) {
            storage_->Emit(it->first, it->second, result, count_, false);
        }
        return result;
    }
    std::shared_ptr<MutableScalarStorage<T>> storage_;
    const int64_t count_;
    const SharedValidityBitmap::Snapshot validity_;
    const std::shared_ptr<ScalarMemoryCharge> validity_charge_;
};

template <typename T>
class MutableScalarPublication final : public IIndex {
 public:
    explicit MutableScalarPublication(
        std::shared_ptr<MutableScalarStorage<T>> storage)
        : storage_(std::move(storage)),
          count_(storage_->accepted_end),
          validity_(storage_->validity.Freeze()),
          validity_charge_(storage_->validity_charge) {
    }
    IndexMetadata
    Metadata() const override {
        return {MutableScalarCaps<T>(), Domain::Row, storage_->type, count_};
    }
    IIndexReaderBasePtr
    CreateReader(milvus::OpContext*) const override {
        return std::make_unique<MutableScalarReader<T>>(
            storage_, count_, validity_, validity_charge_);
    }
    void
    CancelWarmup() const override {
    }

 private:
    std::shared_ptr<MutableScalarStorage<T>> storage_;
    const int64_t count_;
    const SharedValidityBitmap::Snapshot validity_;
    const std::shared_ptr<ScalarMemoryCharge> validity_charge_;
};

template <typename T>
class MutableScalarGrowingOwner final : public IGrowingIndex,
                                        public IAppendable<ScalarBatch<T>> {
 public:
    MutableScalarGrowingOwner(DataType type,
                              MutableScalarIndexOptions options,
                              ByteCounter bytes)
        : storage_(std::make_shared<MutableScalarStorage<T>>(
              type, options, std::move(bytes))) {
        PublishAccepted();
    }
    DataType
    ValueType() const override {
        return storage_->type;
    }
    std::string
    Family() const override {
        return "MUTABLE_SCALAR_BTREE";
    }
    std::optional<size_t>
    MemoryUsage() const override {
        return storage_->bytes->bytes.load(std::memory_order_relaxed);
    }
    void
    CommitIfNeeded() override {
        // Synchronous appends publish before ACK. Pinning must not wait for an
        // in-flight append; queries check poison under their own shared lock.
        std::unique_lock lock(storage_->mutex, std::try_to_lock);
        if (lock.owns_lock()) {
            storage_->CheckHealthy();
            PublishAccepted();
        }
    }
    void
    Flush() override {
        std::unique_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        PublishAccepted();
    }

    void
    Append(int64_t row_begin, const ScalarBatch<T>& batch) override {
        AssertInfo(row_begin >= 0 &&
                       batch.row_count <=
                           static_cast<size_t>(
                               std::numeric_limits<int64_t>::max() - row_begin),
                   "mutable scalar append row range overflows");
        const auto row_end = row_begin + static_cast<int64_t>(batch.row_count);
        std::unique_lock lock(storage_->mutex);
        storage_->CheckHealthy();
        PublishAccepted();
        if (row_end <= storage_->accepted_end)
            return;
        AssertInfo(row_begin == storage_->accepted_end,
                   "mutable scalar expected row {}, got {}",
                   storage_->accepted_end,
                   row_begin);
        AssertInfo(batch.values != nullptr,
                   "mutable scalar append has null values");
        bool has_null = false;
        if (batch.valid != nullptr) {
            has_null =
                std::find(batch.valid, batch.valid + batch.row_count, false) !=
                batch.valid + batch.row_count;
        }
        storage_->validity.PrepareAppend(row_end, has_null);
        const auto validity_bytes = storage_->validity.Freeze().BufferBytes();
        if (!storage_->validity_charge ||
            storage_->validity_charge->bytes != validity_bytes) {
            storage_->validity_charge = std::make_shared<ScalarMemoryCharge>(
                storage_->bytes, validity_bytes);
        }
        try {
            for (size_t i = 0; i < batch.row_count; ++i) {
                const auto offset = row_begin + static_cast<int64_t>(i);
                if (batch.valid != nullptr && !batch.valid[i]) {
                    storage_->validity.AppendNull(offset);
                    continue;
                }
                auto it = storage_->tree.find(batch.values[i]);
                if (it == storage_->tree.end()) {
                    it = storage_->tree
                             .try_emplace(OwnScalarKey<T>(batch.values[i],
                                                          storage_->bytes),
                                          storage_->bytes)
                             .first;
                }
                auto& posting = it->second;
                posting.offsets.push_back(offset);
                if (posting.cached) {
                    auto item = posting.cache;
                    const auto words = static_cast<size_t>(offset) / 64 + 1;
                    if (words > item->words.capacity()) {
                        storage_->Evict(item);
                    } else {
                        item->words.resize(words, 0);
                        item->words[offset / 64] |= uint64_t{1}
                                                    << (offset % 64);
                    }
                }
            }
            storage_->validity.FinishAppend(row_end);
            storage_->accepted_end = row_end;
        } catch (...) {
            // The mutable backend does not promise rollback after an allocation
            // failure. Fail closed for all readers, like built vector Add.
            storage_->failure = std::current_exception();
            throw;
        }
        // Publication allocation failure must not repeat the accepted append.
        PublishAccepted();
    }

 private:
    void
    PublishAccepted() {
        if (storage_->published_end == storage_->accepted_end)
            return;
        auto publication =
            std::make_shared<MutableScalarPublication<T>>(storage_);
        PublishSnapshot(std::move(publication), storage_->accepted_end);
        storage_->published_end = storage_->accepted_end;
    }
    std::shared_ptr<MutableScalarStorage<T>> storage_;
};

template <typename T>
std::unique_ptr<IGrowingIndex>
MakeMutableScalarOwner(DataType type,
                       const MutableScalarIndexOptions& options) {
    auto bytes = std::make_shared<ScalarAllocationCounter>();
    return std::make_unique<MutableScalarGrowingOwner<T>>(
        type, options, std::move(bytes));
}

}  // namespace

std::unique_ptr<IGrowingIndex>
CreateMutableScalarGrowingIndex(DataType type,
                                const MutableScalarIndexOptions& options) {
    switch (type) {
        case DataType::BOOL:
            return MakeMutableScalarOwner<bool>(type, options);
        case DataType::INT8:
            return MakeMutableScalarOwner<int8_t>(type, options);
        case DataType::INT16:
            return MakeMutableScalarOwner<int16_t>(type, options);
        case DataType::INT32:
            return MakeMutableScalarOwner<int32_t>(type, options);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return MakeMutableScalarOwner<int64_t>(type, options);
        case DataType::VARCHAR:
            return MakeMutableScalarOwner<std::string_view>(type, options);
        default:
            ThrowInfo(
                UnexpectedError, "unsupported mutable scalar type {}", type);
    }
}

}  // namespace milvus::index
