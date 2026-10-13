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
class BitmapIndexReaderImplCommon : public BitmapIndexReader<T> {
 public:
    using StoredT = owned_t<T>;
    using Storage = BitmapPostingStorage<StoredT, Posting>;

    BitmapIndexReaderImplCommon(std::shared_ptr<const Storage> storage,
                                BitmapReaderOptions options)
        : storage_(std::move(storage)), options_(std::move(options)) {
        AssertInfo(storage_ != nullptr,
                   "bitmap reader requires posting storage");
        AssertInfo(options_.valid_bitset.size() == options_.total_num_rows,
                   "bitmap validity size {} does not match row count {}",
                   options_.valid_bitset.size(),
                   options_.total_num_rows);
        if (options_.value_type == DataType::NONE) {
            options_.value_type = CppDataType<T>();
        }
        options_.offset_cache =
            options_.offset_cache && options_.value_lookup;
        if (options_.offset_cache) {
            BuildOffsetCache();
        }
    }

    ReaderCaps
    Caps() const override {
        return ReaderCaps{
            .predicate = true,
            .pattern_match = std::is_same_v<T, std::string_view>,
            .nested = options_.nested,
            .value_lookup = options_.value_lookup,
            .cheap_value_lookup =
                options_.value_lookup && options_.offset_cache,
            .exact = !options_.nested};
    }

    Domain
    CoordDomain() const override {
        return options_.nested ? Domain::Element : Domain::Row;
    }

    int64_t
    Count() const override {
        return static_cast<int64_t>(options_.total_num_rows);
    }

    DataType
    ValueType() const override {
        return options_.value_type;
    }

    int64_t
    MemoryUsage() const override {
        auto total =
            static_cast<int64_t>(options_.valid_bitset.size_in_bytes());
        total += PostingMapHeapBytes(
            storage_->postings, storage_->mmap_owner != nullptr);
        total += static_cast<int64_t>(
            offset_cache_.capacity() *
            sizeof(typename decltype(offset_cache_)::value_type));
        return total;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {MemoryUsage(),
                storage_->mmap_owner == nullptr
                    ? 0
                    : static_cast<int64_t>(storage_->mmap_owner->Size())};
    }

    TargetBitmap
    In(size_t n, const T* values) const override {
        AssertInfo(n == 0 || values != nullptr,
                   "bitmap In received null values with non-zero count");
        TargetBitmap result(options_.total_num_rows, false);
        for (size_t i = 0; i < n; ++i) {
            const auto it = storage_->postings.find(values[i]);
            if (it != storage_->postings.end()) {
                UnionPosting(result, it->second);
            }
        }
        return result;
    }

    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        auto result = In(n, values);
        result.flip();
        result &= options_.valid_bitset;
        return result;
    }

    TargetBitmap
    Range(const T& value, CompareOp op) const override {
        if (op == CompareOp::Equal) {
            return In(1, &value);
        }
        if (op == CompareOp::NotEqual) {
            auto result = In(1, &value);
            result.flip();
            result &= options_.valid_bitset;
            return result;
        }

        TargetBitmap result(options_.total_num_rows, false);
        auto begin = storage_->postings.begin();
        auto end = storage_->postings.end();
        switch (op) {
            case CompareOp::LessThan:
                end = storage_->postings.lower_bound(value);
                break;
            case CompareOp::LessEqual:
                end = storage_->postings.upper_bound(value);
                break;
            case CompareOp::GreaterThan:
                begin = storage_->postings.upper_bound(value);
                break;
            case CompareOp::GreaterEqual:
                begin = storage_->postings.lower_bound(value);
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
    Range(const T& lo,
          bool lo_inc,
          const T& hi,
          bool hi_inc) const override {
        TargetBitmap result(options_.total_num_rows, false);
        if (hi < lo || (lo == hi && !(lo_inc && hi_inc))) {
            return result;
        }
        const auto begin = lo_inc ? storage_->postings.lower_bound(lo)
                                  : storage_->postings.upper_bound(lo);
        const auto end = hi_inc ? storage_->postings.upper_bound(hi)
                                : storage_->postings.lower_bound(hi);
        for (auto it = begin; it != end; ++it) {
            UnionPosting(result, it->second);
        }
        return result;
    }

    std::optional<StoredT>
    Lookup(int64_t offset) const override {
        if (!options_.value_lookup) {
            return std::nullopt;
        }
        AssertInfo(
            offset >= 0 &&
                static_cast<size_t>(offset) < options_.total_num_rows,
            "bitmap lookup offset {} is outside [0, {})",
            offset,
            options_.total_num_rows);
        const auto coordinate = static_cast<size_t>(offset);
        if (!options_.valid_bitset[coordinate]) {
            return std::nullopt;
        }
        const auto* value = options_.offset_cache
                                ? offset_cache_[coordinate]
                                : LookupPosting(coordinate);
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
        AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
                   "bitmap Gather received invalid offsets/count");
        if (!options_.value_lookup) {
            for (int64_t i = 0; i < count; ++i) {
                out(i, nullptr, false);
            }
            return;
        }

        auto values = options_.offset_cache
                          ? GatherFromOffsetCache(offsets, count)
                          : GatherFromPostings(offsets, count);
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
        auto result = options_.valid_bitset.clone();
        result.flip();
        return result;
    }

    TargetBitmap
    IsNotNull() const override {
        return options_.valid_bitset.clone();
    }

 protected:
    template <typename Matches>
    TargetBitmap
    MatchPostings(Matches&& matches) const {
        TargetBitmap result(options_.total_num_rows, false);
        for (const auto& [key, posting] : storage_->postings) {
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
    void
    ValidateCoordinate(size_t coordinate, std::string_view operation) const {
        AssertInfo(coordinate < options_.total_num_rows,
                   "bitmap {} offset {} is outside [0, {})",
                   operation,
                   coordinate,
                   options_.total_num_rows);
    }

    void
    BuildOffsetCache() {
        offset_cache_.assign(options_.total_num_rows, nullptr);
        for (const auto& [key, posting] : storage_->postings) {
            if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
                for (auto coordinate : posting) {
                    AssertInfo(coordinate < options_.total_num_rows,
                               "bitmap posting coordinate {} exceeds count {}",
                               coordinate,
                               options_.total_num_rows);
                    offset_cache_[coordinate] = &key;
                }
            } else {
                for (size_t coordinate = 0;
                     coordinate < options_.total_num_rows;
                     ++coordinate) {
                    if (posting[coordinate]) {
                        offset_cache_[coordinate] = &key;
                    }
                }
            }
        }
    }

    const StoredT*
    LookupPosting(size_t coordinate) const {
        for (const auto& [key, posting] : storage_->postings) {
            if (PostingContains(posting, coordinate)) {
                return &key;
            }
        }
        return nullptr;
    }

    std::vector<const StoredT*>
    GatherFromOffsetCache(const int64_t* offsets, int64_t count) const {
        std::vector<const StoredT*> values(static_cast<size_t>(count), nullptr);
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       options_.total_num_rows);
            const auto coordinate = static_cast<size_t>(offsets[i]);
            ValidateCoordinate(coordinate, "gather");
            values[static_cast<size_t>(i)] = offset_cache_[coordinate];
        }
        return values;
    }

    std::vector<const StoredT*>
    GatherFromPostings(const int64_t* offsets, int64_t count) const {
        std::vector<const StoredT*> values(static_cast<size_t>(count), nullptr);
        std::map<size_t, std::vector<int64_t>> wanted;
        for (int64_t i = 0; i < count; ++i) {
            AssertInfo(offsets[i] >= 0,
                       "bitmap gather offset {} is outside [0, {})",
                       offsets[i],
                       options_.total_num_rows);
            const auto coordinate = static_cast<size_t>(offsets[i]);
            ValidateCoordinate(coordinate, "gather");
            if (options_.valid_bitset[coordinate]) {
                wanted[coordinate].push_back(i);
            }
        }

        for (const auto& [key, posting] : storage_->postings) {
            if (wanted.empty()) {
                break;
            }
            if constexpr (std::is_same_v<Posting, roaring::Roaring>) {
                for (auto coordinate : posting) {
                    auto match = wanted.find(coordinate);
                    if (match == wanted.end()) {
                        continue;
                    }
                    for (auto i : match->second) {
                        values[static_cast<size_t>(i)] = &key;
                    }
                    wanted.erase(match);
                }
            } else {
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

    std::shared_ptr<const Storage> storage_;
    BitmapReaderOptions options_;
    std::vector<const StoredT*> offset_cache_;
};

template <typename T, typename Posting>
class BitmapIndexReaderImpl final
    : public BitmapIndexReaderImplCommon<T, Posting>,
      public PatternMatchReaderAdapter<BitmapIndexReaderImpl<T, Posting>, T> {
 public:
    using BitmapIndexReaderImplCommon<T, Posting>::
        BitmapIndexReaderImplCommon;

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

}  // namespace

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
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(
    BitmapRoaringPostingMap<T> postings,
    BitmapReaderOptions options,
    std::shared_ptr<BitmapMmapOwner> mmap_owner) {
    using Storage = BitmapPostingStorage<T, roaring::Roaring>;
    auto storage = std::make_shared<Storage>(
        Storage{.mmap_owner = std::move(mmap_owner),
                .postings = std::move(postings)});
    return CreateBitmapIndexReader<T>(std::move(storage), std::move(options));
}

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(
    std::shared_ptr<const BitmapPostingStorage<T, roaring::Roaring>> storage,
    BitmapReaderOptions options) {
    using QueryT = bitmap_query_t<T>;
    return std::make_unique<
        BitmapIndexReaderImpl<QueryT, roaring::Roaring>>(std::move(storage),
                                                         std::move(options));
}

template <typename T>
std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>
CreateBitmapIndexReader(BitmapBitsetPostingMap<T> postings,
                        BitmapReaderOptions options) {
    using QueryT = bitmap_query_t<T>;
    using Storage = BitmapPostingStorage<T, TargetBitmap>;
    auto storage = std::make_shared<Storage>(
        Storage{.mmap_owner = nullptr, .postings = std::move(postings)});
    return std::make_unique<BitmapIndexReaderImpl<QueryT, TargetBitmap>>(
        std::move(storage), std::move(options));
}

#define INSTANTIATE_BITMAP_READER(T)                                    \
    template std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>      \
    CreateBitmapIndexReader<T>(BitmapRoaringPostingMap<T>,              \
                               BitmapReaderOptions,                     \
                               std::shared_ptr<BitmapMmapOwner>);        \
    template std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>      \
    CreateBitmapIndexReader<T>(                                        \
        std::shared_ptr<                                                \
            const BitmapPostingStorage<T, roaring::Roaring>>,           \
        BitmapReaderOptions);                                           \
    template std::unique_ptr<BitmapIndexReader<bitmap_query_t<T>>>      \
    CreateBitmapIndexReader<T>(BitmapBitsetPostingMap<T>,               \
                               BitmapReaderOptions);

INSTANTIATE_BITMAP_READER(bool)
INSTANTIATE_BITMAP_READER(int8_t)
INSTANTIATE_BITMAP_READER(int16_t)
INSTANTIATE_BITMAP_READER(int32_t)
INSTANTIATE_BITMAP_READER(int64_t)
INSTANTIATE_BITMAP_READER(float)
INSTANTIATE_BITMAP_READER(double)
INSTANTIATE_BITMAP_READER(std::string)

#undef INSTANTIATE_BITMAP_READER

}  // namespace milvus::index
