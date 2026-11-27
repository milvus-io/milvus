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
#include <array>
#include <atomic>
#include <exception>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

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
    // VARCHAR exposes the pattern interface for literal PrefixMatch only;
    // ShouldUseForOpImpl rejects LIKE, postfix, substring and regex operations.
    return {.point_and_range = true,
            .pattern_match = std::is_same_v<T, std::string_view>};
}

template <typename T>
struct MutableScalarStorage {
    using Offsets = absl::InlinedVector<uint32_t, 2, ScalarAllocator<uint32_t>>;
    struct Posting {
        explicit Posting(const ByteCounter& bytes)
            : offsets(ScalarAllocator<uint32_t>(bytes)) {
        }
        mutable std::shared_mutex mutex;
        Offsets offsets;
    };
    using PostingPtr = std::shared_ptr<Posting>;
    using Pair = std::pair<const ScalarKey<T>, PostingPtr>;
    using Tree = absl::btree_map<ScalarKey<T>,
                                 PostingPtr,
                                 ScalarLess<T>,
                                 ScalarAllocator<Pair>>;
    MutableScalarStorage(DataType type, ByteCounter bytes)
        : bytes(std::move(bytes)),
          object_charge(this->bytes, sizeof(MutableScalarStorage)),
          type(type),
          tree(ScalarLess<T>{}, ScalarAllocator<Pair>(this->bytes)) {
    }

    void
    CheckHealthy() const {
        if (failed.load(std::memory_order_acquire)) {
            std::rethrow_exception(failure);
        }
    }

    // Published once by the serialized writer; readers acquire the immutable
    // exception through failed, without taking the writer lock.
    void
    RecordFailure() {
        if (failed.load(std::memory_order_relaxed))
            return;
        failure = std::current_exception();
        failed.store(true, std::memory_order_release);
    }

    // Caller owns writer_mutex. It is the only structural writer, so this
    // lookup can overlap tree readers without an additional shared lock.
    Posting&
    GetOrCreatePosting(const T& value) {
        auto hint = tree.lower_bound(value);
        if (hint != tree.end() && !ScalarLess<T>{}(value, hint->first)) {
            return *hint->second;
        }
        auto posting = std::allocate_shared<Posting>(
            ScalarAllocator<Posting>(bytes), bytes);
        auto key = OwnScalarKey<T>(value, bytes);
        std::unique_lock tree_lock(tree_mutex);
        try {
            return *tree.try_emplace(hint, std::move(key), std::move(posting))
                        ->second;
        } catch (...) {
            // Publish poison before a waiting reader can inspect the tree.
            RecordFailure();
            throw;
        }
    }

    size_t
    VisibleCount(const Posting& posting, int64_t count) const {
        std::shared_lock posting_lock(posting.mutex);
        CheckHealthy();
        const auto& offsets = posting.offsets;
        if (offsets.empty() || offsets.front() >= count)
            return 0;
        if (offsets.back() < count)
            return offsets.size();
        return std::lower_bound(offsets.begin(), offsets.end(), count) -
               offsets.begin();
    }

    // Emit the pinned reader's prefix into a query-private result. Later
    // appends cannot change existing offsets.
    void
    Emit(const Posting& posting,
         TargetBitmap& result,
         int64_t count,
         bool matched = true) const {
        std::shared_lock posting_lock(posting.mutex);
        CheckHealthy();
        for (const auto offset : posting.offsets) {
            if (offset >= count) {
                break;
            }
            result[offset] = matched;
        }
    }

    // Tree, postings and validity share one self-charged counter.
    // Segment statistics may report it but must not charge it again.
    ByteCounter bytes;
    ScalarMemoryCharge object_charge;
    const DataType type;
    // Lock order: writer -> tree (release) -> Posting.
    // Readers never hold tree_mutex while emitting or waiting on a Posting.
    mutable std::mutex writer_mutex;
    mutable std::shared_mutex tree_mutex;
    Tree tree;
    SharedValidityBitmap validity;
    std::shared_ptr<ScalarMemoryCharge> validity_charge;
    int64_t accepted_end = 0;
    int64_t published_end = -1;
    // A failed mutating append is terminal. Publication-only failure is
    // retryable; readers see this exception via the release/acquire flag.
    std::exception_ptr failure;
    std::atomic<bool> failed{false};
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
        storage_->CheckHealthy();
        TargetBitmap result(count_, false);
        for (size_t i = 0; i < n; ++i) {
            const Posting* posting = nullptr;
            {
                std::shared_lock lock(storage_->tree_mutex);
                storage_->CheckHealthy();
                const auto it = storage_->tree.find(values[i]);
                if (it != storage_->tree.end())
                    posting = it->second.get();
            }
            if (posting != nullptr)
                storage_->Emit(*posting, result, count_);
        }
        storage_->CheckHealthy();
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
        switch (op) {
            case CompareOp::GreaterThan:
                return EvaluateRange(
                    [&](const auto& tree) { return tree.upper_bound(value); },
                    [&](const auto& key) {
                        return !ScalarLess<T>{}(value, key);
                    },
                    [](const auto& tree) { return tree.end(); },
                    [](const auto&) { return true; });
            case CompareOp::GreaterEqual:
                return EvaluateRange(
                    [&](const auto& tree) { return tree.lower_bound(value); },
                    [&](const auto& key) {
                        return ScalarLess<T>{}(key, value);
                    },
                    [](const auto& tree) { return tree.end(); },
                    [](const auto&) { return true; });
            case CompareOp::LessThan:
                return EvaluateRange(
                    [](const auto& tree) { return tree.begin(); },
                    [](const auto&) { return false; },
                    [&](const auto& tree) { return tree.lower_bound(value); },
                    [&](const auto& key) {
                        return ScalarLess<T>{}(key, value);
                    });
            case CompareOp::LessEqual:
                return EvaluateRange(
                    [](const auto& tree) { return tree.begin(); },
                    [](const auto&) { return false; },
                    [&](const auto& tree) { return tree.upper_bound(value); },
                    [&](const auto& key) {
                        return !ScalarLess<T>{}(value, key);
                    });
            default:
                ThrowInfo(OpTypeInvalid,
                          "unsupported mutable scalar comparison");
        }
    }
    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
        storage_->CheckHealthy();
        if (hi < lo || (!(lo < hi) && (!lo_inc || !hi_inc))) {
            return TargetBitmap(count_, false);
        }
        return EvaluateRange(
            [&](const auto& tree) {
                return lo_inc ? tree.lower_bound(lo) : tree.upper_bound(lo);
            },
            [&](const auto& key) {
                return lo_inc ? ScalarLess<T>{}(key, lo)
                              : !ScalarLess<T>{}(lo, key);
            },
            [&](const auto& tree) {
                return hi_inc ? tree.upper_bound(hi) : tree.lower_bound(hi);
            },
            [&](const auto& key) {
                return hi_inc ? !ScalarLess<T>{}(hi, key)
                              : ScalarLess<T>{}(key, hi);
            });
    }
    TargetBitmap
    IsNotNull() const override {
        storage_->CheckHealthy();
        auto result = validity_.ToBitmap();
        storage_->CheckHealthy();
        return result;
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
            return ScanRange(
                [&](const auto& tree) { return tree.lower_bound(pattern); },
                [&](const auto& key) {
                    return std::string_view(key).starts_with(pattern);
                });
        } else {
            ThrowInfo(UnexpectedError,
                      "pattern query on numeric scalar reader");
        }
    }

 private:
    using Posting = typename MutableScalarStorage<T>::Posting;
    using ResumeKey =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;

    static ResumeKey
    CopyKey(const ScalarKey<T>& key) {
        if constexpr (std::is_same_v<T, std::string_view>) {
            return std::string(std::string_view(key));
        } else {
            return key;
        }
    }

    std::optional<ResumeKey>
    LastKey() const {
        std::shared_lock lock(storage_->tree_mutex);
        storage_->CheckHealthy();
        if (storage_->tree.empty())
            return std::nullopt;
        return CopyKey(storage_->tree.rbegin()->first);
    }

    // Query-local cursor: capture bounded groups of stable Posting addresses,
    // then release the tree lock before counting/emitting. No iterator or key
    // reference escapes a lock interval; the reader retains the storage.
    template <typename Seek, typename InRange>
    class PostingCursor {
     public:
        PostingCursor(MutableScalarStorage<T>& storage,
                      Seek seek,
                      InRange in_range,
                      const std::optional<ResumeKey>& last_key)
            : storage_(storage),
              seek_(std::move(seek)),
              in_range_(std::move(in_range)),
              last_key_(last_key),
              done_(!last_key.has_value()) {
        }

        const Posting*
        Next() {
            if (position_ == size_ && !done_)
                Fill();
            return position_ < size_ ? postings_[position_++] : nullptr;
        }

        void
        Reset() {
            resume_.reset();
            position_ = size_ = 0;
            done_ = !last_key_.has_value();
        }

     private:
        void
        Fill() {
            std::shared_lock lock(storage_.tree_mutex);
            storage_.CheckHealthy();
            const auto& tree = storage_.tree;
            const auto within_range = [&](const auto& key) {
                return !ScalarLess<T>{}(*last_key_, key) && in_range_(key);
            };
            auto it = resume_ ? tree.upper_bound(*resume_) : seek_(tree);
            position_ = size_ = 0;
            while (it != tree.end() && within_range(it->first) &&
                   size_ < postings_.size()) {
                postings_[size_++] = it->second.get();
                ++it;
            }
            done_ = it == tree.end() || !within_range(it->first);
            if (!done_)
                resume_.emplace(CopyKey(std::prev(it)->first));
        }

        MutableScalarStorage<T>& storage_;
        const Seek seek_;
        const InRange in_range_;
        const std::optional<ResumeKey>& last_key_;
        std::optional<ResumeKey> resume_;
        std::array<const Posting*, 256> postings_;
        size_t position_ = 0;
        size_t size_ = 0;
        bool done_;
    };

    template <typename Cursor>
    void
    EmitCursor(Cursor& cursor, TargetBitmap& result, bool matched) const {
        while (const auto* posting = cursor.Next())
            storage_->Emit(*posting, result, count_, matched);
    }

    // Prefix matching retains the positive path. Ordinary ranges can choose
    // the complement below, without changing the result or validity contract.
    template <typename Seek, typename InRange>
    TargetBitmap
    ScanRange(Seek seek, InRange in_range) const {
        const auto last_key = LastKey();
        PostingCursor<Seek, InRange> cursor(
            *storage_, std::move(seek), std::move(in_range), last_key);
        TargetBitmap result(count_, false);
        EmitCursor(cursor, result, true);
        storage_->CheckHealthy();
        return result;
    }

    // Adapt BitmapIndexReader::UnionKeyRange's clear-the-smaller-side rule.
    // Abseil has no maintained row ranks: probe exact pinned-prefix posting
    // counts on the side with the smaller accumulated count until one side
    // finishes below the other's lower bound. This avoids a mutable prefix
    // table, a full-key prepass for selective ranges, or query-sized buffers.
    template <typename SeekLower,
              typename BeforeLower,
              typename SeekUpper,
              typename BeforeUpper>
    TargetBitmap
    EvaluateRange(SeekLower lower,
                  BeforeLower before_lower,
                  SeekUpper upper,
                  BeforeUpper before_upper) const {
        const auto last_key = LastKey();
        const auto make_cursor = [&](auto seek, auto in_range) {
            return PostingCursor<decltype(seek), decltype(in_range)>(
                *storage_, std::move(seek), std::move(in_range), last_key);
        };
        auto inside = make_cursor(lower, before_upper);
        auto left = make_cursor([](const auto& tree) { return tree.begin(); },
                                before_lower);
        auto right = make_cursor(upper, [](const auto&) { return true; });
        const auto next_outside = [&]() -> const Posting* {
            if (const auto* posting = left.Next())
                return posting;
            return right.Next();
        };

        uint64_t inside_rows = 0, outside_rows = 0;
        bool inside_done = false, outside_done = false;
        bool prefer_inside = true;
        bool complement = false;
        for (;;) {
            if (inside_done && inside_rows <= outside_rows)
                break;
            if (outside_done && outside_rows <= inside_rows) {
                complement = true;
                break;
            }
            if (!inside_done &&
                (outside_done || inside_rows < outside_rows ||
                 (inside_rows == outside_rows && prefer_inside))) {
                prefer_inside = false;
                if (const auto* posting = inside.Next())
                    inside_rows += storage_->VisibleCount(*posting, count_);
                else
                    inside_done = true;
            } else {
                prefer_inside = true;
                if (const auto* posting = next_outside())
                    outside_rows += storage_->VisibleCount(*posting, count_);
                else
                    outside_done = true;
            }
        }

        auto result =
            complement ? validity_.ToBitmap() : TargetBitmap(count_, false);
        if (complement) {
            left.Reset();
            right.Reset();
            EmitCursor(left, result, false);
            EmitCursor(right, result, false);
        } else {
            inside.Reset();
            EmitCursor(inside, result, true);
        }
        storage_->CheckHealthy();
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
class MutableScalarGrowingIndex final : public IGrowingIndex,
                                        public IAppendable<ScalarBatch<T>> {
 public:
    MutableScalarGrowingIndex(DataType type, ByteCounter bytes)
        : storage_(std::make_shared<MutableScalarStorage<T>>(
              type, std::move(bytes))) {
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
        // in-flight append; queries independently check the failure flag.
        std::unique_lock lock(storage_->writer_mutex, std::try_to_lock);
        if (lock.owns_lock()) {
            storage_->CheckHealthy();
            PublishAccepted();
        }
    }
    void
    Flush() override {
        std::unique_lock lock(storage_->writer_mutex);
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
        // row_end is exclusive: UINT32_MAX is still a valid stored offset.
        AssertInfo(
            row_end <=
                static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 1,
            "mutable scalar append exceeds uint32 offset capacity");
        std::unique_lock lock(storage_->writer_mutex);
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
                auto& posting = storage_->GetOrCreatePosting(batch.values[i]);
                std::unique_lock posting_lock(posting.mutex);
                try {
                    posting.offsets.push_back(static_cast<uint32_t>(offset));
                } catch (...) {
                    storage_->RecordFailure();
                    throw;
                }
            }
            storage_->validity.FinishAppend(row_end);
            storage_->accepted_end = row_end;
        } catch (...) {
            // The mutable backend does not promise rollback after an allocation
            // failure. Fail closed for all readers, like built vector Add.
            storage_->RecordFailure();
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
MakeMutableScalarGrowingIndex(DataType type) {
    auto bytes = std::make_shared<ScalarAllocationCounter>();
    return std::make_unique<MutableScalarGrowingIndex<T>>(type,
                                                          std::move(bytes));
}

}  // namespace

std::unique_ptr<IGrowingIndex>
CreateMutableScalarGrowingIndex(DataType type) {
    switch (type) {
        case DataType::BOOL:
            return MakeMutableScalarGrowingIndex<bool>(type);
        case DataType::INT8:
            return MakeMutableScalarGrowingIndex<int8_t>(type);
        case DataType::INT16:
            return MakeMutableScalarGrowingIndex<int16_t>(type);
        case DataType::INT32:
            return MakeMutableScalarGrowingIndex<int32_t>(type);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return MakeMutableScalarGrowingIndex<int64_t>(type);
        case DataType::VARCHAR:
            return MakeMutableScalarGrowingIndex<std::string_view>(type);
        default:
            ThrowInfo(
                UnexpectedError, "unsupported mutable scalar type {}", type);
    }
}

}  // namespace milvus::index
