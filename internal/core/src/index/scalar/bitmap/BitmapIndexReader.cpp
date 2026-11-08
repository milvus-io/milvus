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

#include "index/scalar/bitmap/BitmapIndexReader.h"

#include <mutex>

#include <sys/mman.h>
#include <unistd.h>

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <variant>

#include <roaring/roaring.hh>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/scalar/ScalarIndexUtils.h"

namespace milvus::index {
namespace {

void
UnionPosting(TargetBitmap& result, const roaring::Roaring& posting) {
    for (auto coordinate : posting) {
        AssertInfo(coordinate < result.size(),
                   "bitmap posting coordinate {} exceeds count {}",
                   coordinate,
                   result.size());
        result.set(coordinate);
    }
}

void
UnionPosting(TargetBitmap& result, const TargetBitmap& posting) {
    result |= posting;
}

bool
PostingContains(const roaring::Roaring& posting, size_t coordinate) {
    return posting.contains(static_cast<uint32_t>(coordinate));
}

bool
PostingContains(const TargetBitmap& posting, size_t coordinate) {
    return posting[coordinate];
}

template <typename K, typename Posting>
int64_t
PostingMapHeapBytes(const BitmapPostingMap<K, Posting>& postings,
                    bool mapped_roaring_payload) {
    int64_t total = 0;
    for (const auto& [key, posting] : postings) {
        // Approximate libstdc++'s red-black-tree links/color storage. The key
        // and mapped object live in that allocation and are counted
        // separately so frozen Roaring views still charge their heap
        // metadata while their backing bytes are charged as file usage.
        total += 40 + sizeof(K) + sizeof(Posting);
        if constexpr (std::is_same_v<K, std::string>) {
            total += static_cast<int64_t>(key.capacity() + 1);
        }
        if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
            if (!mapped_roaring_payload) {
                // CRoaring exposes no allocator footprint. Keep the existing
                // engine-size approximation for heap postings; frozen views
                // skip it because their exact backing-file size is known.
                total += static_cast<int64_t>(posting.getSizeInBytes());
            }
        } else {
            total += static_cast<int64_t>(posting.size_in_bytes());
        }
    }
    return total;
}

template <typename T, typename Posting>
class BitmapIndexReaderImplCommon;

template <typename T, typename Posting>
class BitmapIndexReaderImpl;

/** @brief Shares prepared bitmap storage across independent request views. */
template <typename T, typename Posting>
class BitmapIndexImpl final
    : public IIndex,
      public std::enable_shared_from_this<BitmapIndexImpl<T, Posting>> {
 public:
    using Storage = BitmapPostingStorage<owned_t<T>, Posting>;

    explicit BitmapIndexImpl(std::shared_ptr<const Storage> storage)
        : storage_(std::move(storage)) {
        AssertInfo(
            std::get<std::shared_ptr<const Storage>>(storage_) != nullptr,
            "bitmap index requires storage");
    }

    BitmapIndexImpl(std::shared_ptr<cachinglayer::CacheSlot<Storage>> slot,
                    IndexMetadata metadata)
        : storage_(CachedStorage{std::move(slot), metadata}) {
        AssertInfo(std::get<CachedStorage>(storage_).slot != nullptr,
                   "bitmap index requires a cache slot");
        AssertInfo(metadata.count && *metadata.count >= 0,
                   "bitmap index requires a non-negative count");
    }

    IndexMetadata
    Metadata() const override {
        if (const auto* cached = std::get_if<CachedStorage>(&storage_)) {
            return cached->metadata;
        }
        const auto& options =
            std::get<std::shared_ptr<const Storage>>(storage_)->options;
        return {ReaderCaps{.predicate = true,
                           .pattern_match = std::is_same_v<T, std::string_view>,
                           .nested = options.nested,
                           .value_lookup = options.value_lookup,
                           .cheap_value_lookup = options.offset_cache,
                           .exact = !options.nested},
                options.nested ? Domain::Element : Domain::Row,
                options.value_type,
                static_cast<int64_t>(options.total_num_rows)};
    }

    IIndexReaderBasePtr
    CreateReader(milvus::OpContext* op_ctx) const override;

    void
    CancelWarmup() const override {
        if (const auto* cached = std::get_if<CachedStorage>(&storage_)) {
            cached->slot->CancelWarmup();
        }
    }

 private:
    friend class BitmapIndexReaderImplCommon<T, Posting>;
    friend class ::milvus::index::JsonPathIndex;

    std::shared_ptr<const Storage>
    PinStorage(milvus::OpContext* op_ctx) const {
        if (const auto* resident =
                std::get_if<std::shared_ptr<const Storage>>(&storage_)) {
            return *resident;
        }
        const auto& cached = std::get<CachedStorage>(storage_);
        std::shared_ptr<const Storage> storage;
        if (const auto* resident = cached.slot->TryBorrowCell(0)) {
            storage = {cached.slot, resident};
        } else {
            auto pin =
                cachinglayer::SemiInlineGet(cached.slot->PinCells(op_ctx, {0}));
            const auto* cell = pin->get_ith_cell(0);
            storage = {std::move(pin), cell};
        }
        AssertInfo(
            storage != nullptr &&
                storage->options.total_num_rows == *cached.metadata.count &&
                storage->options.value_type == cached.metadata.value_type &&
                storage->options.nested == cached.metadata.caps.nested &&
                storage->options.value_lookup ==
                    cached.metadata.caps.value_lookup &&
                storage->options.offset_cache ==
                    cached.metadata.caps.cheap_value_lookup,
            "bitmap storage disagrees with index metadata");
        return storage;
    }

    struct CachedStorage {
        std::shared_ptr<cachinglayer::CacheSlot<Storage>> slot;
        IndexMetadata metadata;
    };
    std::variant<std::shared_ptr<const Storage>, CachedStorage> storage_;
};

template <typename T, typename Posting>
class BitmapIndexReaderImplCommon : public BitmapIndexReader<T> {
 public:
    using StoredT = owned_t<T>;
    using Storage = BitmapPostingStorage<StoredT, Posting>;

    BitmapIndexReaderImplCommon(
        std::shared_ptr<const BitmapIndexImpl<T, Posting>> index,
        milvus::OpContext* op_ctx)
        : index_(std::move(index)), op_ctx_(op_ctx) {
    }

    void
    Prefetch() const override {
        (void)GetStorage();
    }

    ReaderCaps
    Caps() const override {
        return index_->Metadata().caps;
    }

    Domain
    CoordDomain() const override {
        return index_->Metadata().domain;
    }

    int64_t
    Count() const override {
        return *index_->Metadata().count;
    }

    DataType
    ValueType() const override {
        return index_->Metadata().value_type;
    }

    TargetBitmap
    In(size_t n, const T* values) const override {
        const auto& pin = GetStorage();
        return InStorage(*pin, n, values);
    }

    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        auto result = InStorage(storage, n, values);
        result.flip();
        result &= storage.options.valid_bitset;
        return result;
    }

    TargetBitmap
    Range(const T& value, CompareOp op) const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        if (op == CompareOp::Equal) {
            return InStorage(storage, 1, &value);
        }
        if (op == CompareOp::NotEqual) {
            auto result = InStorage(storage, 1, &value);
            result.flip();
            result &= storage.options.valid_bitset;
            return result;
        }

        TargetBitmap result(storage.options.total_num_rows, false);
        auto begin = storage.postings.begin();
        auto end = storage.postings.end();
        switch (op) {
            case CompareOp::LessThan:
                end = storage.postings.lower_bound(value);
                break;
            case CompareOp::LessEqual:
                end = storage.postings.upper_bound(value);
                break;
            case CompareOp::GreaterThan:
                begin = storage.postings.upper_bound(value);
                break;
            case CompareOp::GreaterEqual:
                begin = storage.postings.lower_bound(value);
                break;
            case CompareOp::Equal:
            case CompareOp::NotEqual:
                break;
        }
        for (auto it = begin; it != end; ++it) {
            UnionPosting(result, it->second);
        }
        return result;
    }

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        TargetBitmap result(storage.options.total_num_rows, false);
        if (hi < lo || (lo == hi && !(lo_inc && hi_inc))) {
            return result;
        }
        const auto begin = lo_inc ? storage.postings.lower_bound(lo)
                                  : storage.postings.upper_bound(lo);
        const auto end = hi_inc ? storage.postings.upper_bound(hi)
                                : storage.postings.lower_bound(hi);
        for (auto it = begin; it != end; ++it) {
            UnionPosting(result, it->second);
        }
        return result;
    }

    std::optional<StoredT>
    Lookup(int64_t offset) const override {
        const auto& storage = *GetStorage();
        if (!storage.options.value_lookup) {
            return std::nullopt;
        }
        AssertInfo(offset >= 0 && static_cast<size_t>(offset) <
                                      storage.options.total_num_rows,
                   "bitmap lookup offset {} is outside [0, {})",
                   offset,
                   storage.options.total_num_rows);
        const auto coordinate = static_cast<size_t>(offset);
        if (!storage.options.valid_bitset[coordinate]) {
            return std::nullopt;
        }
        const auto* value = storage.options.offset_cache
                                ? storage.offset_cache[coordinate]
                                : LookupPosting(storage, coordinate);
        if (value == nullptr) {
            return std::nullopt;
        }
        return StoredT(*value);
    }

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t i, const T*, bool valid)>& out)
        const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
                   "bitmap Gather received invalid offsets/count");
        if (!storage.options.value_lookup) {
            for (int64_t i = 0; i < count; ++i) {
                out(i, nullptr, false);
            }
            return;
        }

        auto values = storage.options.offset_cache
                          ? GatherFromOffsetCache(storage, offsets, count)
                          : GatherFromPostings(storage, offsets, count);
        for (int64_t i = 0; i < count; ++i) {
            const auto* value = values[static_cast<size_t>(i)];
            if (value == nullptr) {
                out(i, nullptr, false);
                continue;
            }
            if constexpr (std::is_same_v<T, StoredT>) {
                out(i, value, true);
            } else {
                const T view(*value);
                out(i, &view, true);
            }
        }
    }

    TargetBitmap
    IsNull() const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        auto result = storage.options.valid_bitset.clone();
        result.flip();
        return result;
    }

    TargetBitmap
    IsNotNull() const override {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        return storage.options.valid_bitset.clone();
    }

 protected:
    template <typename Matches>
    TargetBitmap
    MatchPostings(Matches&& matches) const {
        const auto& pin = GetStorage();
        const auto& storage = *pin;
        TargetBitmap result(storage.options.total_num_rows, false);
        for (const auto& [key, posting] : storage.postings) {
            if (matches(key)) {
                UnionPosting(result, posting);
            }
        }
        return result;
    }

    TargetBitmap
    PatternQuery(std::string_view pattern) const {
        LikePatternMatcher matcher{std::string(pattern)};
        return MatchPostings(
            [&matcher](const std::string& value) { return matcher(value); });
    }

 private:
    TargetBitmap
    InStorage(const Storage& storage, size_t n, const T* values) const {
        AssertInfo(n == 0 || values != nullptr,
                   "bitmap In received null values with non-zero count");
        TargetBitmap result(storage.options.total_num_rows, false);
        for (size_t i = 0; i < n; ++i) {
            const auto it = storage.postings.find(values[i]);
            if (it != storage.postings.end()) {
                UnionPosting(result, it->second);
            }
        }
        return result;
    }

    void
    ValidateCoordinate(const Storage& storage,
                       size_t coordinate,
                       std::string_view operation) const {
        AssertInfo(coordinate < storage.options.total_num_rows,
                   "bitmap {} offset {} is outside [0, {})",
                   operation,
                   coordinate,
                   storage.options.total_num_rows);
    }

    const StoredT*
    LookupPosting(const Storage& storage, size_t coordinate) const {
        for (const auto& [key, posting] : storage.postings) {
            if (PostingContains(posting, coordinate)) {
                return &key;
            }
        }
        return nullptr;
    }

    std::vector<const StoredT*>
    GatherFromOffsetCache(const Storage& storage,
                          const int64_t* offsets,
                          int64_t count) const {
        std::vector<const StoredT*> values(static_cast<size_t>(count), nullptr);
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       storage.options.total_num_rows);
            const auto coordinate = static_cast<size_t>(offsets[i]);
            ValidateCoordinate(storage, coordinate, "gather");
            values[static_cast<size_t>(i)] = storage.offset_cache[coordinate];
        }
        return values;
    }

    std::vector<const StoredT*>
    GatherFromPostings(const Storage& storage,
                       const int64_t* offsets,
                       int64_t count) const {
        std::vector<const StoredT*> values(static_cast<size_t>(count), nullptr);
        std::map<size_t, std::vector<int64_t>> wanted;
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       storage.options.total_num_rows);
            const auto coordinate = static_cast<size_t>(offsets[i]);
            ValidateCoordinate(storage, coordinate, "gather");
            if (storage.options.valid_bitset[coordinate]) {
                if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
                    // Sparse output requests must not enumerate a posting
                    // proportional to the segment size. Probe keys in order
                    // and keep the first match, as Lookup does.
                    values[static_cast<size_t>(i)] =
                        LookupPosting(storage, coordinate);
                } else {
                    wanted[coordinate].push_back(i);
                }
            }
        }

        if constexpr (!std::is_same_v<Posting, roaring::Roaring>) {
            for (const auto& [key, posting] : storage.postings) {
                if (wanted.empty()) {
                    break;
                }
                for (auto match = wanted.begin(); match != wanted.end();) {
                    if (!posting[match->first]) {
                        ++match;
                        continue;
                    }
                    for (auto i : match->second) {
                        values[static_cast<size_t>(i)] = &key;
                    }
                    match = wanted.erase(match);
                }
            }
        }
        return values;
    }

    const std::shared_ptr<const Storage>&
    GetStorage() const {
        std::call_once(storage_once_,
                       [this] { storage_ = index_->PinStorage(op_ctx_); });
        return storage_;
    }

    std::shared_ptr<const BitmapIndexImpl<T, Posting>> index_;
    milvus::OpContext* const op_ctx_;
    mutable std::once_flag storage_once_;
    mutable std::shared_ptr<const Storage> storage_;
};

template <typename T, typename Posting>
class BitmapIndexReaderImpl final
    : public BitmapIndexReaderImplCommon<T, Posting>,
      public PatternMatchReaderAdapter<BitmapIndexReaderImpl<T, Posting>, T> {
 public:
    using BitmapIndexReaderImplCommon<T, Posting>::BitmapIndexReaderImplCommon;

 private:
    template <typename Derived, typename U, bool DelegateShouldUseForOp>
    friend class milvus::index::PatternMatchReaderAdapter;

    template <typename U = T,
              std::enable_if_t<std::is_same_v<U, std::string_view>, int> = 0>
    TargetBitmap
    PatternMatchImpl(std::string_view pattern, PatternOp op) const {
        if (op == PatternOp::Match) {
            return this->PatternQuery(pattern);
        }

        const std::string owned(pattern);
        switch (op) {
            case PatternOp::PrefixMatch:
                return this->MatchPostings([&owned](const std::string& value) {
                    return value.size() >= owned.size() &&
                           value.compare(0, owned.size(), owned) == 0;
                });
            case PatternOp::PostfixMatch:
                return this->MatchPostings([&owned](const std::string& value) {
                    return value.size() >= owned.size() &&
                           value.compare(value.size() - owned.size(),
                                         owned.size(),
                                         owned) == 0;
                });
            case PatternOp::InnerMatch:
                return this->MatchPostings([&owned](const std::string& value) {
                    return value.find(owned) != std::string::npos;
                });
            case PatternOp::RegexMatch: {
                PartialRegexMatcher matcher(owned);
                return this->MatchPostings(
                    [&matcher](const std::string& value) {
                        return matcher(value);
                    });
            }
            case PatternOp::Match:
                break;
        }
        return TargetBitmap(static_cast<size_t>(this->Count()), false);
    }
};

template <typename T, typename Posting>
IIndexReaderBasePtr
BitmapIndexImpl<T, Posting>::CreateReader(milvus::OpContext* op_ctx) const {
    return std::make_unique<BitmapIndexReaderImpl<T, Posting>>(
        this->shared_from_this(), op_ctx);
}

}  // namespace

template <typename T, typename Posting>
BitmapPostingStorage<T, Posting>::BitmapPostingStorage(
    BitmapPostingMap<T, Posting> loaded_postings,
    BitmapStorageOptions loaded_options,
    std::shared_ptr<BitmapMmapOwner> owner)
    : mmap_owner(std::move(owner)),
      postings(std::move(loaded_postings)),
      options(std::move(loaded_options)) {
    AssertInfo(options.valid_bitset.size() == options.total_num_rows,
               "bitmap validity size {} does not match row count {}",
               options.valid_bitset.size(),
               options.total_num_rows);
    if (options.value_type == DataType::NONE) {
        options.value_type = CppDataType<bitmap_query_t<T>>();
    }
    options.offset_cache = options.offset_cache && options.value_lookup;
    if (!options.offset_cache) {
        return;
    }
    offset_cache.assign(options.total_num_rows, nullptr);
    for (const auto& [key, posting] : postings) {
        if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
            for (auto coordinate : posting) {
                AssertInfo(coordinate < options.total_num_rows,
                           "bitmap posting coordinate {} exceeds count {}",
                           coordinate,
                           options.total_num_rows);
                offset_cache[coordinate] = &key;
            }
        } else {
            for (size_t coordinate = 0; coordinate < options.total_num_rows;
                 ++coordinate) {
                if (posting[coordinate]) {
                    offset_cache[coordinate] = &key;
                }
            }
        }
    }
}

template <typename T, typename Posting>
cachinglayer::ResourceUsage
BitmapPostingStorage<T, Posting>::CellByteSize() const {
    auto total = static_cast<int64_t>(options.valid_bitset.size_in_bytes());
    total += PostingMapHeapBytes(postings, mmap_owner != nullptr);
    total += static_cast<int64_t>(offset_cache.capacity() * sizeof(const T*));
    if (json_exists) {
        const auto bytes = static_cast<int64_t>(sizeof(TargetBitmap) +
                                                json_exists->size_in_bytes());
        AssertInfo(total <= std::numeric_limits<int64_t>::max() - bytes,
                   "bitmap reader resource size exceeds int64 capacity");
        total += bytes;
    }
    return {
        total,
        mmap_owner == nullptr ? 0 : static_cast<int64_t>(mmap_owner->Size())};
}

BitmapMmapOwner::BitmapMmapOwner(char* data, size_t size, std::string path)
    : data_(data), size_(size), path_(std::move(path)) {
}

BitmapMmapOwner::~BitmapMmapOwner() {
    if (data_ != nullptr && size_ != 0) {
        munmap(data_, size_);
    }
    if (!path_.empty()) {
        unlink(path_.c_str());
    }
}

const char*
BitmapMmapOwner::Data() const {
    return data_;
}

size_t
BitmapMmapOwner::Size() const {
    return size_;
}

template <typename T>
IndexPtr
CreateBitmapIndex(BitmapRoaringPostingMap<T> postings,
                  BitmapStorageOptions options,
                  std::shared_ptr<BitmapMmapOwner> mmap_owner) {
    return std::make_shared<
        BitmapIndexImpl<bitmap_query_t<T>, roaring::Roaring>>(
        std::make_shared<BitmapPostingStorage<T, roaring::Roaring>>(
            std::move(postings), std::move(options), std::move(mmap_owner)));
}

template <typename T>
IndexPtr
CreateBitmapIndex(BitmapBitsetPostingMap<T> postings,
                  BitmapStorageOptions options) {
    return std::make_shared<BitmapIndexImpl<bitmap_query_t<T>, TargetBitmap>>(
        std::make_shared<BitmapPostingStorage<T, TargetBitmap>>(
            std::move(postings), std::move(options)));
}

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(BitmapRoaringPostingMap<T> postings,
                        BitmapStorageOptions options,
                        std::shared_ptr<BitmapMmapOwner> mmap_owner) {
    auto index = CreateBitmapIndex<T>(
        std::move(postings), std::move(options), std::move(mmap_owner));
    auto reader = index->CreateReader(nullptr);
    return std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>(
        static_cast<BitmapIndexReader<bitmap_query_t<T>>*>(reader.release()));
}

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(BitmapBitsetPostingMap<T> postings,
                        BitmapStorageOptions options) {
    auto index = CreateBitmapIndex<T>(std::move(postings), std::move(options));
    auto reader = index->CreateReader(nullptr);
    return std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>(
        static_cast<BitmapIndexReader<bitmap_query_t<T>>*>(reader.release()));
}

template <typename T, typename Posting>
IndexPtr
CreateBitmapIndex(std::unique_ptr<BitmapPostingStorage<T, Posting>> storage,
                  std::optional<JsonProjectedIndexSpec> projection) {
    auto index = std::make_shared<BitmapIndexImpl<bitmap_query_t<T>, Posting>>(
        std::move(storage));
    if (projection) {
        return JsonPathIndex::FromStorage(std::move(index),
                                          std::move(*projection));
    }
    return index;
}

template <typename T, typename Posting>
IndexPtr
CreateBitmapIndex(
    std::shared_ptr<cachinglayer::CacheSlot<BitmapPostingStorage<T, Posting>>>
        slot,
    IndexMetadata metadata,
    std::optional<JsonProjectedIndexSpec> projection) {
    auto index = std::make_shared<BitmapIndexImpl<bitmap_query_t<T>, Posting>>(
        std::move(slot), metadata);
    if (projection) {
        return JsonPathIndex::FromStorage(std::move(index),
                                          std::move(*projection));
    }
    return index;
}

#define INSTANTIATE_BITMAP_STORAGE(T, P)                                      \
    template struct BitmapPostingStorage<T, P>;                               \
    template IndexPtr CreateBitmapIndex<T, P>(                                \
        std::unique_ptr<BitmapPostingStorage<T, P>>,                          \
        std::optional<JsonProjectedIndexSpec>);                               \
    template IndexPtr CreateBitmapIndex<T, P>(                                \
        std::shared_ptr<cachinglayer::CacheSlot<BitmapPostingStorage<T, P>>>, \
        IndexMetadata,                                                        \
        std::optional<JsonProjectedIndexSpec>);

#define INSTANTIATE_BITMAP_READER(T)                                          \
    INSTANTIATE_BITMAP_STORAGE(T, roaring::Roaring)                           \
    INSTANTIATE_BITMAP_STORAGE(T, TargetBitmap)                               \
    template IndexPtr CreateBitmapIndex<T>(BitmapRoaringPostingMap<T>,        \
                                           BitmapStorageOptions,              \
                                           std::shared_ptr<BitmapMmapOwner>); \
    template IndexPtr CreateBitmapIndex<T>(BitmapBitsetPostingMap<T>,         \
                                           BitmapStorageOptions);             \
    template std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>            \
        CreateBitmapIndexReader<T>(BitmapRoaringPostingMap<T>,                \
                                   BitmapStorageOptions,                      \
                                   std::shared_ptr<BitmapMmapOwner>);         \
    template std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>            \
        CreateBitmapIndexReader<T>(BitmapBitsetPostingMap<T>,                 \
                                   BitmapStorageOptions);

INSTANTIATE_BITMAP_READER(bool)
INSTANTIATE_BITMAP_READER(int8_t)
INSTANTIATE_BITMAP_READER(int16_t)
INSTANTIATE_BITMAP_READER(int32_t)
INSTANTIATE_BITMAP_READER(int64_t)
INSTANTIATE_BITMAP_READER(float)
INSTANTIATE_BITMAP_READER(double)
INSTANTIATE_BITMAP_READER(std::string)

#undef INSTANTIATE_BITMAP_READER
#undef INSTANTIATE_BITMAP_STORAGE

}  // namespace milvus::index
