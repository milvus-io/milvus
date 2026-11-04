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

#include "index/scalar/inverted/InvertedIndexReader.h"

#include <algorithm>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "common/RegexQuery.h"
#include "common/Utils.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::index {
namespace {

std::string
OwnString(std::string_view value) {
    return value.empty() ? std::string{}
                         : std::string(value.data(), value.size());
}

template <typename T>
bool
CompatibleReaderType(DataType type) {
    if constexpr (std::is_same_v<T, std::string_view>) {
        return IsStringDataType(type);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        return type == DataType::INT64 || type == DataType::TIMESTAMPTZ;
    } else if constexpr (std::is_same_v<T, bool>) {
        return type == DataType::BOOL;
    } else if constexpr (std::is_same_v<T, int8_t>) {
        return type == DataType::INT8;
    } else if constexpr (std::is_same_v<T, int16_t>) {
        return type == DataType::INT16;
    } else if constexpr (std::is_same_v<T, int32_t>) {
        return type == DataType::INT32;
    } else if constexpr (std::is_same_v<T, float>) {
        return type == DataType::FLOAT;
    } else if constexpr (std::is_same_v<T, double>) {
        return type == DataType::DOUBLE;
    }
    return false;
}

int64_t
ToUsageBytes(size_t bytes) {
    if (bytes > static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "inverted reader resource size exceeds int64 domain");
    }
    return static_cast<int64_t>(bytes);
}

bool
RegexCallback(void* context, const uint8_t* term, uintptr_t length) {
    const auto* matcher = static_cast<const PartialRegexMatcher*>(context);
    return (*matcher)(std::string_view(reinterpret_cast<const char*>(term),
                                       static_cast<size_t>(length)));
}

void
AddUsageBytes(size_t& total, size_t bytes) {
    if (bytes > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataFormatBroken, "inverted reader memory size overflows");
    }
    total += bytes;
}

}  // namespace

template <typename T>
InvertedIndexStorage<T>::InvertedIndexStorage(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    bool nested,
    bool mmap,
    size_t engine_bytes,
    size_t engine_path_bytes)
    : directory_(std::move(directory)),
      engine_(std::move(engine)),
      value_type_(value_type),
      nested_(nested),
      mmap_(mmap),
      engine_bytes_(engine_bytes),
      engine_path_bytes_(engine_path_bytes) {
    AssertInfo(engine_ != nullptr, "inverted reader requires an engine");
    AssertInfo(null_offsets != nullptr,
               "inverted reader requires immutable null offsets");
    AssertInfo(CompatibleReaderType<T>(value_type_),
               "inverted reader type {} does not match its interface",
               static_cast<int>(value_type_));
    AssertInfo(!mmap_ || directory_ != nullptr,
               "mmap inverted reader requires a directory owner");
    count_ = engine_->count();
    size_t previous = 0;
    bool first = true;
    for (const auto offset : *null_offsets) {
        if ((!first && offset <= previous) || (!nested_ && offset >= count_)) {
            ThrowInfo(DataFormatBroken,
                      "invalid inverted null offset {} for count {}",
                      offset,
                      count_);
        }
        previous = offset;
        first = false;
    }

    // See the valid_bitmap_ declaration: materialize validity once here so no
    // query replays the offsets, and keep the all-valid case allocation-free.
    if (nested_ || null_offsets->empty()) {
        all_valid_ = true;
    } else {
        valid_bitmap_ = TargetBitmap(count_, true);
        for (const auto offset : *null_offsets) {
            valid_bitmap_.reset(offset);
        }
    }
}

template <typename T>
InvertedIndex<T>::InvertedIndex(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    bool nested,
    bool mmap,
    size_t engine_bytes,
    size_t engine_path_bytes)
    : InvertedIndex(
          std::make_shared<InvertedIndexStorage<T>>(std::move(directory),
                                                    std::move(engine),
                                                    std::move(null_offsets),
                                                    value_type,
                                                    nested,
                                                    mmap,
                                                    engine_bytes,
                                                    engine_path_bytes)) {
}

template <typename T>
InvertedIndex<T>::InvertedIndex(
    std::shared_ptr<const InvertedIndexStorage<T>> storage)
    : storage_(std::move(storage)) {
    AssertInfo(std::get<std::shared_ptr<const InvertedIndexStorage<T>>>(
                   storage_) != nullptr,
               "inverted index requires storage");
}

template <typename T>
InvertedIndex<T>::InvertedIndex(
    std::shared_ptr<cachinglayer::CacheSlot<InvertedIndexStorage<T>>> slot,
    IndexMetadata metadata)
    : storage_(CachedStorage{std::move(slot), metadata}) {
    AssertInfo(std::get<CachedStorage>(storage_).slot != nullptr,
               "inverted index requires a cache slot");
    AssertInfo(!metadata.count || *metadata.count >= 0,
               "inverted index count must not be negative");
}

template <typename T>
std::shared_ptr<const InvertedIndexStorage<T>>
InvertedIndex<T>::PinStorage(milvus::OpContext* op_ctx) const {
    if (const auto* resident =
            std::get_if<std::shared_ptr<const InvertedIndexStorage<T>>>(
                &storage_)) {
        return *resident;
    }
    const auto& cached = std::get<CachedStorage>(storage_);
    std::shared_ptr<const InvertedIndexStorage<T>> storage;
    if (const auto* resident = cached.slot->TryBorrowCell(0)) {
        storage = {cached.slot, resident};
    } else {
        auto pin =
            cachinglayer::SemiInlineGet(cached.slot->PinCells(op_ctx, {0}));
        const auto* cell = pin->get_ith_cell(0);
        storage = {std::move(pin), cell};
    }
    AssertInfo(storage != nullptr &&
                   (!cached.metadata.count ||
                    storage->count_ == *cached.metadata.count) &&
                   storage->value_type_ == cached.metadata.value_type &&
                   storage->nested_ == cached.metadata.caps.nested,
               "inverted storage disagrees with index metadata");
    return storage;
}

template <typename T>
void
InvertedIndex<T>::CancelWarmup() const {
    if (const auto* cached = std::get_if<CachedStorage>(&storage_)) {
        cached->slot->CancelWarmup();
    }
}

template <typename T>
IndexMetadata
InvertedIndex<T>::Metadata() const {
    if (const auto* cached = std::get_if<CachedStorage>(&storage_))
        return cached->metadata;
    const auto& storage =
        *std::get<std::shared_ptr<const InvertedIndexStorage<T>>>(storage_);
    return {ReaderCaps{.predicate = true,
                       .pattern_match = std::is_same_v<T, std::string_view>,
                       .nested = storage.nested_,
                       .exact = !storage.nested_},
            storage.nested_ ? Domain::Element : Domain::Row,
            storage.value_type_,
            static_cast<int64_t>(storage.count_)};
}

template <typename T>
IIndexReaderBasePtr
InvertedIndex<T>::CreateReader(milvus::OpContext* op_ctx) const {
    return IIndexReaderBasePtr(
        new InvertedIndexReader<T>(this->shared_from_this(), op_ctx));
}

template <typename T>
InvertedIndexReader<T>::InvertedIndexReader(
    std::shared_ptr<storage::LocalDirectory> directory,
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
    std::shared_ptr<const std::vector<size_t>> null_offsets,
    DataType value_type,
    bool nested,
    bool mmap,
    size_t engine_bytes,
    size_t engine_path_bytes)
    : InvertedIndexReader(
          std::make_shared<InvertedIndex<T>>(std::move(directory),
                                             std::move(engine),
                                             std::move(null_offsets),
                                             value_type,
                                             nested,
                                             mmap,
                                             engine_bytes,
                                             engine_path_bytes),
          nullptr) {
}

template <typename T>
InvertedIndexReader<T>::InvertedIndexReader(
    std::shared_ptr<const InvertedIndex<T>> index, milvus::OpContext* op_ctx)
    : index_(std::move(index)), op_ctx_(op_ctx) {
}

template <typename T>
int64_t
InvertedIndexStorage<T>::MemoryUsage() const {
    size_t total = sizeof(InvertedIndexStorage<T>);
    AddUsageBytes(total, sizeof(milvus::tantivy::TantivyIndexWrapper));
    AddUsageBytes(total, engine_path_bytes_);
    AddUsageBytes(total, valid_bitmap_.size_in_bytes());
    if (json_exists) {
        AddUsageBytes(total, sizeof(TargetBitmap));
        AddUsageBytes(total, json_exists->size_in_bytes());
    }
    if (directory_ != nullptr) {
        AddUsageBytes(total, directory_->HeapBytes());
    }
    if (!mmap_) {
        AddUsageBytes(total, engine_bytes_);
    }
    return ToUsageBytes(total);
}

template <typename T>
cachinglayer::ResourceUsage
InvertedIndexStorage<T>::CellByteSize() const {
    return {MemoryUsage(), mmap_ ? ToUsageBytes(engine_bytes_) : 0};
}

template <typename T>
const std::shared_ptr<const InvertedIndexStorage<T>>&
InvertedIndexReader<T>::GetStorage() const {
    std::call_once(storage_once_,
                   [this] { storage_ = index_->PinStorage(op_ctx_); });
    return storage_;
}

template <typename T>
InvertedIndexReader<T>::~InvertedIndexReader() = default;

template <typename T>
ReaderCaps
InvertedIndexReader<T>::Caps() const {
    return index_->Metadata().caps;
}

template <typename T>
Domain
InvertedIndexReader<T>::CoordDomain() const {
    return index_->Metadata().domain;
}

template <typename T>
int64_t
InvertedIndexReader<T>::Count() const {
    if (const auto count = index_->Metadata().count)
        return *count;
    return GetStorage()->count_;
}

template <typename T>
DataType
InvertedIndexReader<T>::ValueType() const {
    return index_->Metadata().value_type;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::In(size_t n, const T* values) const {
    const auto& pin = GetStorage();
    return InStorage(*pin, n, values);
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::InStorage(const InvertedIndexStorage<T>& storage,
                                  size_t n,
                                  const T* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "inverted In received null values with non-zero count");
    TargetBitmap result(storage.count_);
    if (n == 0) {
        return result;
    }
    if constexpr (std::is_same_v<T, std::string_view>) {
        std::vector<std::string> owned;
        owned.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            owned.push_back(OwnString(values[i]));
        }
        storage.engine_->terms_query(owned.data(), owned.size(), &result);
    } else {
        storage.engine_->terms_query(values, n, &result);
    }
    return result;
}

template <typename T>
void
InvertedIndexReader<T>::ApplyValidityMask(
    const InvertedIndexStorage<T>& storage, TargetBitmap& bitset) const {
    if (storage.all_valid_) {
        return;
    }
    AssertInfo(storage.valid_bitmap_.size() == bitset.size(),
               "inverted validity bitmap size {} does not match result size "
               "{}",
               storage.valid_bitmap_.size(),
               bitset.size());
    bitset &= storage.valid_bitmap_;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::NotIn(size_t n, const T* values) const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    auto result = InStorage(storage, n, values);
    result.flip();
    ApplyValidityMask(storage, result);
    return result;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::Range(const T& value, CompareOp op) const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    if (op == CompareOp::Equal) {
        return InStorage(storage, 1, &value);
    }
    if (op == CompareOp::NotEqual) {
        auto result = InStorage(storage, 1, &value);
        result.flip();
        ApplyValidityMask(storage, result);
        return result;
    }

    TargetBitmap result(storage.count_);
    if constexpr (std::is_same_v<T, std::string_view>) {
        const auto owned = OwnString(value);
        switch (op) {
            case CompareOp::GreaterThan:
                storage.engine_->lower_bound_range_query(owned, false, &result);
                break;
            case CompareOp::GreaterEqual:
                storage.engine_->lower_bound_range_query(owned, true, &result);
                break;
            case CompareOp::LessThan:
                storage.engine_->upper_bound_range_query(owned, false, &result);
                break;
            case CompareOp::LessEqual:
                storage.engine_->upper_bound_range_query(owned, true, &result);
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "unsupported inverted comparison operator {}",
                          static_cast<int>(op));
        }
    } else {
        switch (op) {
            case CompareOp::GreaterThan:
                storage.engine_->lower_bound_range_query(value, false, &result);
                break;
            case CompareOp::GreaterEqual:
                storage.engine_->lower_bound_range_query(value, true, &result);
                break;
            case CompareOp::LessThan:
                storage.engine_->upper_bound_range_query(value, false, &result);
                break;
            case CompareOp::LessEqual:
                storage.engine_->upper_bound_range_query(value, true, &result);
                break;
            default:
                ThrowInfo(OpTypeInvalid,
                          "unsupported inverted comparison operator {}",
                          static_cast<int>(op));
        }
    }
    return result;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::Range(const T& lo,
                              bool lo_inc,
                              const T& hi,
                              bool hi_inc) const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    TargetBitmap result(storage.count_);
    if constexpr (std::is_same_v<T, std::string_view>) {
        const auto owned_lo = OwnString(lo);
        const auto owned_hi = OwnString(hi);
        storage.engine_->range_query(
            owned_lo, owned_hi, lo_inc, hi_inc, &result);
    } else {
        storage.engine_->range_query(lo, hi, lo_inc, hi_inc, &result);
    }
    return result;
}

template <typename T>
bool
InvertedIndexReader<T>::ShouldUseForOpImpl(PatternOp op,
                                           std::string_view) const {
    switch (op) {
        case PatternOp::Match:
        case PatternOp::PrefixMatch:
            return true;
        case PatternOp::PostfixMatch:
        case PatternOp::InnerMatch:
        case PatternOp::RegexMatch:
            return false;
    }
    return false;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::PatternMatchImpl(std::string_view pattern,
                                         PatternOp op) const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    const auto owned = OwnString(pattern);
    switch (op) {
        case PatternOp::PrefixMatch: {
            TargetBitmap result(storage.count_);
            storage.engine_->prefix_query(owned, &result);
            return result;
        }
        case PatternOp::PostfixMatch:
            return PatternQuery(storage, "%" + EscapeLikePattern(owned));
        case PatternOp::InnerMatch:
            return PatternQuery(storage, "%" + EscapeLikePattern(owned) + "%");
        case PatternOp::Match:
            return PatternQuery(storage, owned);
        case PatternOp::RegexMatch: {
            TargetBitmap result(storage.count_);
            PartialRegexMatcher matcher(owned);
            storage.engine_->regex_match_query(
                &matcher, RegexCallback, &result);
            return result;
        }
    }
    ThrowInfo(OpTypeInvalid,
              "unsupported inverted pattern operator {}",
              static_cast<int>(op));
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::PatternQuery(const InvertedIndexStorage<T>& storage,
                                     std::string_view pattern) const {
    PatternMatchTranslator translator;
    const auto regex = translator(OwnString(pattern));
    TargetBitmap result(storage.count_);
    storage.engine_->regex_query(regex, &result);
    return result;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::IsNull() const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    if (storage.all_valid_) {
        return TargetBitmap(storage.count_);
    }
    auto result = storage.valid_bitmap_.clone();
    result.flip();
    return result;
}

template <typename T>
TargetBitmap
InvertedIndexReader<T>::IsNotNull() const {
    const auto& pin = GetStorage();
    const auto& storage = *pin;
    if (storage.all_valid_) {
        return TargetBitmap(storage.count_, true);
    }
    return storage.valid_bitmap_.clone();
}

#define INSTANTIATE_INVERTED_READER(T)      \
    template class InvertedIndexStorage<T>; \
    template class InvertedIndex<T>;        \
    template class InvertedIndexReader<T>;
INSTANTIATE_INVERTED_READER(bool)
INSTANTIATE_INVERTED_READER(int8_t)
INSTANTIATE_INVERTED_READER(int16_t)
INSTANTIATE_INVERTED_READER(int32_t)
INSTANTIATE_INVERTED_READER(int64_t)
INSTANTIATE_INVERTED_READER(float)
INSTANTIATE_INVERTED_READER(double)
INSTANTIATE_INVERTED_READER(std::string_view)
#undef INSTANTIATE_INVERTED_READER

}  // namespace milvus::index
