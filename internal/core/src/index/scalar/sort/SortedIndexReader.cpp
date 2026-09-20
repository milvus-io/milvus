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

#include "index/scalar/sort/SortedIndexReader.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <sys/mman.h>
#include <type_traits>
#include <unistd.h>
#include <utility>

#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "common/RegexQuery.h"
#include "index/scalar/sort/SortedIndexFormat.h"

namespace milvus::index {
namespace {

int64_t
ToInt64(size_t value) {
    AssertInfo(
        value <= static_cast<size_t>(std::numeric_limits<int64_t>::max()),
        "sorted reader resource size exceeds int64 capacity");
    return static_cast<int64_t>(value);
}

size_t
StringSsoThreshold() {
    static const size_t threshold = std::string().capacity();
    return threshold;
}

uint32_t
ReadUint32(const uint8_t* data) {
    uint32_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

uint64_t
ReadUint64(const uint8_t* data) {
    uint64_t value = 0;
    std::memcpy(&value, data, sizeof(value));
    return value;
}

size_t
CheckedBytes(size_t count, size_t width, std::string_view label) {
    if (width != 0 && count > std::numeric_limits<size_t>::max() / width) {
        ThrowInfo(
            DataFormatBroken, "sorted string {} byte size overflows", label);
    }
    return count * width;
}

void
RequireBytes(size_t offset,
             size_t bytes,
             size_t limit,
             std::string_view label) {
    if (offset > limit || bytes > limit - offset) {
        ThrowInfo(DataFormatBroken, "truncated sorted string {}", label);
    }
}

struct PackedDescriptor {
    uint32_t unique_count{0};
    size_t string_offsets{0};
    size_t string_data{0};
    size_t posting_offsets{0};
    size_t posting_data{0};
};

PackedDescriptor
ValidatePacked(const uint8_t* data, size_t size, size_t total_num_rows) {
    constexpr size_t kMinimum = sizeof(uint32_t) + sizeof(uint64_t);
    if (data == nullptr || size < kMinimum) {
        ThrowInfo(
            DataFormatBroken, "sorted string payload is too small: {}", size);
    }
    const auto magic_offset = size - sizeof(uint64_t);
    const auto magic = ReadUint64(data + magic_offset);
    if (magic != sort_format::kStringMagic) {
        ThrowInfo(
            DataFormatBroken, "invalid sorted string magic code {}", magic);
    }

    PackedDescriptor result;
    result.unique_count = ReadUint32(data);
    if (result.unique_count >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "sorted string unique count {} exceeds int32 domain",
                  result.unique_count);
    }
    result.string_offsets = sizeof(uint32_t);
    const auto offsets_bytes =
        CheckedBytes(result.unique_count, sizeof(uint32_t), "offset table");
    RequireBytes(
        result.string_offsets, offsets_bytes, magic_offset, "string offsets");
    result.string_data = result.string_offsets + offsets_bytes;

    size_t cursor = result.string_data;
    std::string_view previous;
    bool first = true;
    for (uint32_t i = 0; i < result.unique_count; ++i) {
        const auto relative =
            ReadUint32(data + result.string_offsets + i * sizeof(uint32_t));
        if (relative != cursor - result.string_data) {
            ThrowInfo(DataFormatBroken,
                      "invalid sorted string offset {} at value {}",
                      relative,
                      i);
        }
        RequireBytes(cursor, sizeof(uint32_t), magic_offset, "value length");
        const auto length = ReadUint32(data + cursor);
        cursor += sizeof(uint32_t);
        RequireBytes(cursor, length, magic_offset, "value bytes");
        const std::string_view value(
            reinterpret_cast<const char*>(data + cursor), length);
        if (!first && !(previous < value)) {
            ThrowInfo(DataFormatBroken,
                      "sorted string values are not strictly ordered");
        }
        previous = value;
        first = false;
        cursor += length;
    }

    result.posting_offsets = cursor;
    RequireBytes(
        result.posting_offsets, offsets_bytes, magic_offset, "posting offsets");
    result.posting_data = result.posting_offsets + offsets_bytes;
    cursor = result.posting_data;
    for (uint32_t i = 0; i < result.unique_count; ++i) {
        const auto relative =
            ReadUint32(data + result.posting_offsets + i * sizeof(uint32_t));
        if (relative != cursor - result.posting_data) {
            ThrowInfo(DataFormatBroken,
                      "invalid sorted posting offset {} at value {}",
                      relative,
                      i);
        }
        RequireBytes(cursor, sizeof(uint32_t), magic_offset, "posting length");
        const auto count = ReadUint32(data + cursor);
        cursor += sizeof(uint32_t);
        const auto bytes =
            CheckedBytes(count, sizeof(uint32_t), "posting rows");
        RequireBytes(cursor, bytes, magic_offset, "posting rows");
        uint32_t previous_row = 0;
        for (uint32_t j = 0; j < count; ++j) {
            const auto row = ReadUint32(data + cursor + j * sizeof(uint32_t));
            if (static_cast<size_t>(row) >= total_num_rows) {
                ThrowInfo(DataFormatBroken,
                          "sorted string row {} exceeds count {}",
                          row,
                          total_num_rows);
            }
            if (j != 0 && row < previous_row) {
                ThrowInfo(DataFormatBroken,
                          "sorted string posting rows are not ordered");
            }
            previous_row = row;
        }
        cursor += bytes;
    }
    if (cursor != magic_offset) {
        ThrowInfo(DataFormatBroken,
                  "sorted string payload has {} bytes outside its sections",
                  magic_offset > cursor ? magic_offset - cursor
                                        : cursor - magic_offset);
    }
    return result;
}

class HeapStringLayout final : public SortedStringLayout {
 public:
    HeapStringLayout(std::vector<std::string> values,
                     std::vector<std::vector<uint32_t>> postings,
                     size_t total_num_rows)
        : values_(std::move(values)), postings_(std::move(postings)) {
        if (values_.size() != postings_.size() ||
            values_.size() >
                static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            ThrowInfo(DataFormatBroken,
                      "invalid sorted string heap layout sizes");
        }
        for (size_t i = 0; i < values_.size(); ++i) {
            if (i != 0 && !(values_[i - 1] < values_[i])) {
                ThrowInfo(DataFormatBroken,
                          "sorted string heap values are not ordered");
            }
            uint32_t previous = 0;
            for (size_t j = 0; j < postings_[i].size(); ++j) {
                const auto row = postings_[i][j];
                if (static_cast<size_t>(row) >= total_num_rows ||
                    (j != 0 && row < previous)) {
                    ThrowInfo(DataFormatBroken,
                              "invalid sorted string heap posting");
                }
                previous = row;
            }
        }
    }

    size_t
    UniqueCount() const override {
        return values_.size();
    }

    std::string_view
    Value(size_t index) const override {
        AssertInfo(index < values_.size(),
                   "sorted string value index {} is out of range",
                   index);
        return values_[index];
    }

    SortedStringPostingView
    Posting(size_t index) const override {
        AssertInfo(index < postings_.size(),
                   "sorted string posting index {} is out of range",
                   index);
        return {reinterpret_cast<const uint8_t*>(postings_[index].data()),
                postings_[index].size()};
    }

    int64_t
    MemoryUsage() const override {
        size_t total = values_.capacity() * sizeof(std::string) +
                       postings_.capacity() * sizeof(std::vector<uint32_t>);
        for (const auto& value : values_) {
            if (value.capacity() > StringSsoThreshold()) {
                total += value.capacity() + 1;
            }
        }
        for (const auto& posting : postings_) {
            total += posting.capacity() * sizeof(uint32_t);
        }
        return ToInt64(total);
    }

    int64_t
    FileUsage() const override {
        return 0;
    }

 private:
    std::vector<std::string> values_;
    std::vector<std::vector<uint32_t>> postings_;
};

class PackedStringLayout final : public SortedStringLayout {
 public:
    PackedStringLayout(std::shared_ptr<const void> owner,
                       const uint8_t* data,
                       size_t size,
                       size_t memory_bytes,
                       size_t file_bytes,
                       size_t total_num_rows)
        : owner_(std::move(owner)),
          data_(data),
          memory_bytes_(memory_bytes),
          file_bytes_(file_bytes),
          descriptor_(ValidatePacked(data, size, total_num_rows)) {
    }

    size_t
    UniqueCount() const override {
        return descriptor_.unique_count;
    }

    std::string_view
    Value(size_t index) const override {
        AssertInfo(index < descriptor_.unique_count,
                   "sorted string value index {} is out of range",
                   index);
        const auto relative = ReadUint32(data_ + descriptor_.string_offsets +
                                         index * sizeof(uint32_t));
        const auto offset = descriptor_.string_data + relative;
        const auto length = ReadUint32(data_ + offset);
        return {
            reinterpret_cast<const char*>(data_ + offset + sizeof(uint32_t)),
            length};
    }

    SortedStringPostingView
    Posting(size_t index) const override {
        AssertInfo(index < descriptor_.unique_count,
                   "sorted string posting index {} is out of range",
                   index);
        const auto relative = ReadUint32(data_ + descriptor_.posting_offsets +
                                         index * sizeof(uint32_t));
        const auto offset = descriptor_.posting_data + relative;
        const auto count = ReadUint32(data_ + offset);
        return {data_ + offset + sizeof(uint32_t), count};
    }

    int64_t
    MemoryUsage() const override {
        return ToInt64(memory_bytes_);
    }

    int64_t
    FileUsage() const override {
        return ToInt64(file_bytes_);
    }

 private:
    std::shared_ptr<const void> owner_;
    const uint8_t* data_{nullptr};
    size_t memory_bytes_{0};
    size_t file_bytes_{0};
    PackedDescriptor descriptor_;
};

size_t
LowerBound(const SortedStringLayout& layout, std::string_view value) {
    size_t left = 0;
    size_t right = layout.UniqueCount();
    while (left < right) {
        const auto middle = left + (right - left) / 2;
        if (layout.Value(middle) < value) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    return left;
}

size_t
UpperBound(const SortedStringLayout& layout, std::string_view value) {
    size_t left = 0;
    size_t right = layout.UniqueCount();
    while (left < right) {
        const auto middle = left + (right - left) / 2;
        if (layout.Value(middle) <= value) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    return left;
}

std::pair<size_t, size_t>
PrefixRange(const SortedStringLayout& layout, std::string_view prefix) {
    if (prefix.empty()) {
        return {0, layout.UniqueCount()};
    }
    const auto start = LowerBound(layout, prefix);
    std::string next(prefix);
    for (size_t i = next.size(); i > 0; --i) {
        const auto byte = static_cast<unsigned char>(next[i - 1]);
        if (byte != std::numeric_limits<unsigned char>::max()) {
            next[i - 1] = static_cast<char>(byte + 1);
            next.resize(i);
            return {start, LowerBound(layout, next)};
        }
    }
    return {start, layout.UniqueCount()};
}

void
SetPosting(TargetBitmap& result,
           const SortedStringPostingView& posting) {
    for (size_t i = 0; i < posting.size; ++i) {
        const auto row = posting.At(i);
        AssertInfo(static_cast<size_t>(row) < result.size(),
                   "sorted string row {} exceeds count {}",
                   row,
                   result.size());
        result.set(row);
    }
}

void
ClearPosting(TargetBitmap& result,
             const SortedStringPostingView& posting) {
    for (size_t i = 0; i < posting.size; ++i) {
        const auto row = posting.At(i);
        AssertInfo(static_cast<size_t>(row) < result.size(),
                   "sorted string row {} exceeds count {}",
                   row,
                   result.size());
        result.reset(row);
    }
}

}  // namespace

SortedMmapOwner::SortedMmapOwner(char* data,
                                 size_t mapped_size,
                                 size_t logical_size,
                                 std::string path)
    : data_(data),
      mapped_size_(mapped_size),
      logical_size_(logical_size),
      path_(std::move(path)) {
}

SortedMmapOwner::~SortedMmapOwner() {
    if (data_ != nullptr && mapped_size_ != 0) {
        munmap(data_, mapped_size_);
    }
    if (!path_.empty()) {
        unlink(path_.c_str());
    }
}

const uint8_t*
SortedMmapOwner::Data() const {
    return reinterpret_cast<const uint8_t*>(data_);
}

size_t
SortedMmapOwner::MappedSize() const {
    return mapped_size_;
}

size_t
SortedMmapOwner::LogicalSize() const {
    return logical_size_;
}

uint32_t
SortedStringPostingView::At(size_t index) const {
    AssertInfo(
        index < size, "sorted string posting offset {} is out of range", index);
    return ReadUint32(data + index * sizeof(uint32_t));
}

std::vector<int32_t>
SortedStringLayout::BuildOffsets(size_t total_num_rows) const {
    if (UniqueCount() >
        static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        ThrowInfo(DataFormatBroken,
                  "sorted string unique count exceeds int32 domain");
    }
    std::vector<int32_t> result(total_num_rows, -1);
    for (size_t value = 0; value < UniqueCount(); ++value) {
        const auto posting = Posting(value);
        for (size_t i = 0; i < posting.size; ++i) {
            const auto row = posting.At(i);
            if (static_cast<size_t>(row) >= total_num_rows) {
                ThrowInfo(DataFormatBroken,
                          "sorted string row {} exceeds count {}",
                          row,
                          total_num_rows);
            }
            result[row] = static_cast<int32_t>(value);
        }
    }
    return result;
}

std::shared_ptr<const SortedStringLayout>
SortedStringLayout::FromHeap(
    std::vector<std::string> unique_values,
    std::vector<std::vector<uint32_t>> posting_lists,
    size_t total_num_rows) {
    return std::make_shared<HeapStringLayout>(
        std::move(unique_values), std::move(posting_lists), total_num_rows);
}

std::shared_ptr<const SortedStringLayout>
SortedStringLayout::FromPackedHeap(std::vector<uint8_t> packed,
                                  size_t total_num_rows) {
    auto owner = std::make_shared<std::vector<uint8_t>>(std::move(packed));
    return std::make_shared<PackedStringLayout>(owner,
                                                owner->data(),
                                                owner->size(),
                                                owner->capacity(),
                                                0,
                                                total_num_rows);
}

std::shared_ptr<const SortedStringLayout>
SortedStringLayout::FromPackedMmap(
    std::shared_ptr<SortedMmapOwner> owner, size_t total_num_rows) {
    AssertInfo(owner != nullptr, "sorted string mmap owner must not be null");
    return std::make_shared<PackedStringLayout>(owner,
                                                owner->Data(),
                                                owner->LogicalSize(),
                                                0,
                                                owner->MappedSize(),
                                                total_num_rows);
}

namespace sorted_reader_detail {

template <typename T>
SortedStorageView<T>::SortedStorageView(OpenArgs args)
    : data_(std::move(args)) {
    AssertInfo(data_.size == 0 || data_.data != nullptr,
               "sorted reader data pointer is null");
}

template <typename T>
void
SortedStorageView<T>::SetEqual(TargetBitmap& result, const T& value) const {
    if (data_.size == 0) {
        return;
    }
    const auto* begin = data_.data;
    const auto* end = begin + data_.size;
    const auto range = std::equal_range(begin, end, IndexStructure<T>(value));
    for (auto it = range.first; it != range.second; ++it) {
        result.set(static_cast<size_t>(it->idx_));
    }
}

template <typename T>
void
SortedStorageView<T>::ClearEqual(TargetBitmap& result, const T& value) const {
    if (data_.size == 0) {
        return;
    }
    const auto* begin = data_.data;
    const auto* end = begin + data_.size;
    const auto range = std::equal_range(begin, end, IndexStructure<T>(value));
    for (auto it = range.first; it != range.second; ++it) {
        result.reset(static_cast<size_t>(it->idx_));
    }
}

template <typename T>
TargetBitmap
SortedStorageView<T>::Range(size_t count,
                            const TargetBitmap& validity,
                            bool value_lookup,
                            const T& value,
                            CompareOp op) const {
    if (op == CompareOp::Equal) {
        TargetBitmap result(count, false);
        SetEqual(result, value);
        return result;
    }
    if (op == CompareOp::NotEqual) {
        auto result = validity.clone();
        ClearEqual(result, value);
        return result;
    }
    TargetBitmap result(count, false);
    if (data_.size == 0 || ShouldSkip(value, value, op)) {
        return result;
    }
    const auto* begin = data_.data;
    const auto* end = begin + data_.size;
    const IndexStructure<T> target(value);
    auto first = begin;
    auto last = end;
    switch (op) {
        case CompareOp::LessThan:
            last = std::lower_bound(begin, end, target);
            break;
        case CompareOp::LessEqual:
            last = std::upper_bound(begin, end, target);
            break;
        case CompareOp::GreaterThan:
            first = std::upper_bound(begin, end, target);
            break;
        case CompareOp::GreaterEqual:
            first = std::lower_bound(begin, end, target);
            break;
        case CompareOp::Equal:
        case CompareOp::NotEqual:
            break;
    }
    if (value_lookup && static_cast<size_t>(last - first) > count / 2) {
        result = validity.clone();
        for (auto it = begin; it != first; ++it) {
            result.reset(static_cast<size_t>(it->idx_));
        }
        for (auto it = last; it != end; ++it) {
            result.reset(static_cast<size_t>(it->idx_));
        }
    } else {
        for (auto it = first; it != last; ++it) {
            result.set(static_cast<size_t>(it->idx_));
        }
    }
    return result;
}

template <typename T>
TargetBitmap
SortedStorageView<T>::Range(size_t count,
                            const TargetBitmap& validity,
                            bool value_lookup,
                            const T& lo,
                            bool lo_inc,
                            const T& hi,
                            bool hi_inc) const {
    TargetBitmap result(count, false);
    if (data_.size == 0 || hi < lo ||
        (lo == hi && !(lo_inc && hi_inc)) ||
        ShouldSkip(lo, hi, CompareOp::Equal)) {
        return result;
    }
    const auto* begin = data_.data;
    const auto* end = begin + data_.size;
    const auto first =
        lo_inc ? std::lower_bound(begin, end, IndexStructure<T>(lo))
               : std::upper_bound(begin, end, IndexStructure<T>(lo));
    const auto last = hi_inc
                          ? std::upper_bound(begin, end, IndexStructure<T>(hi))
                          : std::lower_bound(begin, end, IndexStructure<T>(hi));
    if (value_lookup && static_cast<size_t>(last - first) > count / 2) {
        result = validity.clone();
        for (auto it = begin; it != first; ++it) {
            result.reset(static_cast<size_t>(it->idx_));
        }
        for (auto it = last; it != end; ++it) {
            result.reset(static_cast<size_t>(it->idx_));
        }
    } else {
        for (auto it = first; it != last; ++it) {
            result.set(static_cast<size_t>(it->idx_));
        }
    }
    return result;
}

template <typename T>
T
SortedStorageView<T>::ValueAt(size_t index) const {
    AssertInfo(index < data_.size,
               "sorted value offset {} exceeds index size {}",
               index,
               data_.size);
    return data_.data[index].a_;
}

template <typename T>
bool
SortedStorageView<T>::ValidReverseIndex(int32_t index) const {
    return index >= 0 && static_cast<size_t>(index) < data_.size;
}

template <typename T>
bool
SortedStorageView<T>::Empty() const {
    return data_.size == 0;
}

template <typename T>
int64_t
SortedStorageView<T>::MemoryUsage() const {
    return ToInt64(data_.data_heap_bytes);
}

template <typename T>
int64_t
SortedStorageView<T>::FileUsage() const {
    return ToInt64(data_.data_file_bytes);
}

template <typename T>
bool
SortedStorageView<T>::ShouldSkip(const T& lower,
                                 const T& upper,
                                 CompareOp op) const {
    if (data_.size == 0) {
        return true;
    }
    const auto& min = data_.data->a_;
    const auto& max = data_.data[data_.size - 1].a_;
    switch (op) {
        case CompareOp::LessThan:
            return upper <= min;
        case CompareOp::LessEqual:
            return upper < min;
        case CompareOp::GreaterThan:
            return lower >= max;
        case CompareOp::GreaterEqual:
            return lower > max;
        case CompareOp::Equal:
            return lower > max || upper < min;
        case CompareOp::NotEqual:
            return false;
    }
    return false;
}

}  // namespace sorted_reader_detail

template <typename T>
SortedIndexReader<T>::SortedIndexReader(OpenArgs args)
    : state_(std::move(args.state)), storage_(std::move(args.storage)) {
    if (state_.value_type == DataType::NONE) {
        if constexpr (std::is_same_v<T, std::string_view>) {
            state_.value_type = DataType::VARCHAR;
        } else {
            state_.value_type = CppDataType<T>(DataType::NONE);
        }
    }
    AssertInfo(state_.idx_to_offsets_size == 0 ||
                   state_.idx_to_offsets != nullptr,
               "sorted reader offset pointer is null");
    AssertInfo(!state_.value_lookup ||
                   state_.idx_to_offsets_size == state_.total_num_rows,
               "sorted reader reverse offsets do not cover every row");
    AssertInfo(state_.valid_bitset != nullptr,
               "sorted reader validity owner is null");
    AssertInfo(state_.valid_bitset->size() == state_.total_num_rows,
               "sorted reader validity size mismatch");
}

template <typename T>
SortedIndexReader<T>::~SortedIndexReader() = default;

template <typename T>
ReaderCaps
SortedIndexReader<T>::Caps() const {
    return ReaderCaps{.predicate = true,
                      .pattern_match = std::is_same_v<T, std::string_view>,
                      .nested = state_.nested,
                      .value_lookup = state_.value_lookup,
                      .cheap_value_lookup = state_.value_lookup,
                      .exact = !state_.nested};
}

template <typename T>
Domain
SortedIndexReader<T>::CoordDomain() const {
    return state_.nested ? Domain::Element : Domain::Row;
}

template <typename T>
int64_t
SortedIndexReader<T>::Count() const {
    return static_cast<int64_t>(state_.total_num_rows);
}

template <typename T>
DataType
SortedIndexReader<T>::ValueType() const {
    return state_.value_type;
}

template <typename T>
int64_t
SortedIndexReader<T>::MemoryUsage() const {
    return ToInt64(state_.valid_bitset->size_in_bytes() +
                   state_.idx_to_offsets_heap_bytes) +
           storage_.MemoryUsage();
}

template <typename T>
cachinglayer::ResourceUsage
SortedIndexReader<T>::CellByteSize() const {
    return {MemoryUsage(),
            ToInt64(state_.idx_to_offsets_file_bytes) +
                storage_.FileUsage()};
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::In(size_t n, const T* values) const {
    AssertInfo(n == 0 || values != nullptr,
               "sorted In received null values with non-zero count");
    TargetBitmap result(state_.total_num_rows, false);
    if constexpr (!std::is_same_v<T, std::string_view>) {
        if (storage_.Empty()) {
            return result;
        }
    }
    for (size_t i = 0; i < n; ++i) {
        storage_.SetEqual(result, values[i]);
    }
    return result;
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::NotIn(size_t n, const T* values) const {
    if constexpr (std::is_same_v<T, std::string_view>) {
        AssertInfo(n == 0 || values != nullptr,
                   "sorted NotIn received null values with non-zero count");
    }
    auto result = state_.valid_bitset->clone();
    if constexpr (!std::is_same_v<T, std::string_view>) {
        if (storage_.Empty()) {
            return result;
        }
        AssertInfo(n == 0 || values != nullptr,
                   "sorted NotIn received null values with non-zero count");
    }
    for (size_t i = 0; i < n; ++i) {
        storage_.ClearEqual(result, values[i]);
    }
    return result;
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::Range(const T& value, CompareOp op) const {
    return storage_.Range(state_.total_num_rows,
                          *state_.valid_bitset,
                          state_.value_lookup,
                          value,
                          op);
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::Range(const T& lo,
                            bool lo_inc,
                            const T& hi,
                            bool hi_inc) const {
    return storage_.Range(state_.total_num_rows,
                          *state_.valid_bitset,
                          state_.value_lookup,
                          lo,
                          lo_inc,
                          hi,
                          hi_inc);
}

template <typename T>
std::optional<owned_t<T>>
SortedIndexReader<T>::Lookup(int64_t offset) const {
    if (!state_.value_lookup) {
        return std::nullopt;
    }
    AssertInfo(
        offset >= 0 && static_cast<size_t>(offset) < state_.total_num_rows,
        "sorted lookup offset {} is outside [0, {})",
        offset,
        state_.total_num_rows);
    const auto row = static_cast<size_t>(offset);
    if (!(*state_.valid_bitset)[row]) {
        return std::nullopt;
    }
    AssertInfo(row < state_.idx_to_offsets_size,
               "sorted offset map does not cover row {}",
               row);
    const auto index = state_.idx_to_offsets[row];
    AssertInfo(storage_.ValidReverseIndex(index),
               "sorted value offset {} is invalid for row {}",
               index,
               row);
    return owned_t<T>(storage_.ValueAt(static_cast<size_t>(index)));
}

template <typename T>
void
SortedIndexReader<T>::Gather(
    const int64_t* offsets,
    int64_t count,
    const std::function<void(int64_t i, const T*, bool valid)>& out) const {
    AssertInfo(count >= 0 && (count == 0 || offsets != nullptr),
               "sorted Gather received invalid offsets/count");
    if (!state_.value_lookup) {
        for (int64_t i = 0; i < count; ++i) {
            out(i, nullptr, false);
        }
        return;
    }
    for (int64_t i = 0; i < count; ++i) {
        const auto row = offsets[i];
        AssertInfo(row >= 0 && static_cast<size_t>(row) < state_.total_num_rows,
                   "sorted gather offset {} is outside [0, {})",
                   row,
                   state_.total_num_rows);
        const auto coordinate = static_cast<size_t>(row);
        if (!(*state_.valid_bitset)[coordinate]) {
            out(i, nullptr, false);
            continue;
        }
        AssertInfo(coordinate < state_.idx_to_offsets_size,
                   "sorted offset map does not cover row {}",
                   row);
        const auto index = state_.idx_to_offsets[coordinate];
        AssertInfo(storage_.ValidReverseIndex(index),
                   "sorted value offset {} is invalid for row {}",
                   index,
                   row);
        const auto value = storage_.ValueAt(static_cast<size_t>(index));
        out(i, &value, true);
    }
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::IsNull() const {
    auto result = state_.valid_bitset->clone();
    result.flip();
    return result;
}

template <typename T>
TargetBitmap
SortedIndexReader<T>::IsNotNull() const {
    return state_.valid_bitset->clone();
}

namespace sorted_reader_detail {

SortedStorageView<std::string_view>::SortedStorageView(OpenArgs args)
    : layout_(std::move(args.layout)) {
    AssertInfo(layout_ != nullptr, "sorted string layout must not be null");
}

void
SortedStorageView<std::string_view>::SetEqual(
    TargetBitmap& result, const std::string_view& value) const {
    const auto index = LowerBound(*layout_, value);
    if (index < layout_->UniqueCount() && layout_->Value(index) == value) {
        SetPosting(result, layout_->Posting(index));
    }
}

void
SortedStorageView<std::string_view>::ClearEqual(
    TargetBitmap& result, const std::string_view& value) const {
    const auto index = LowerBound(*layout_, value);
    if (index < layout_->UniqueCount() && layout_->Value(index) == value) {
        ClearPosting(result, layout_->Posting(index));
    }
}

TargetBitmap
SortedStorageView<std::string_view>::Range(
    size_t count,
    const TargetBitmap& validity,
    bool,
    const std::string_view& value,
    CompareOp op) const {
    if (op == CompareOp::Equal) {
        TargetBitmap result(count, false);
        SetEqual(result, value);
        return result;
    }
    if (op == CompareOp::NotEqual) {
        auto result = validity.clone();
        ClearEqual(result, value);
        return result;
    }
    size_t begin = 0;
    size_t end = layout_->UniqueCount();
    switch (op) {
        case CompareOp::GreaterThan:
            begin = UpperBound(*layout_, value);
            break;
        case CompareOp::GreaterEqual:
            begin = LowerBound(*layout_, value);
            break;
        case CompareOp::LessThan:
            end = LowerBound(*layout_, value);
            break;
        case CompareOp::LessEqual:
            end = UpperBound(*layout_, value);
            break;
        case CompareOp::Equal:
        case CompareOp::NotEqual:
            break;
    }
    TargetBitmap result(count, false);
    for (size_t i = begin; i < end; ++i) {
        SetPosting(result, layout_->Posting(i));
    }
    return result;
}

TargetBitmap
SortedStorageView<std::string_view>::Range(
    size_t count,
    const TargetBitmap&,
    bool,
    const std::string_view& lo,
    bool lo_inc,
    const std::string_view& hi,
    bool hi_inc) const {
    TargetBitmap result(count, false);
    if (hi < lo || (lo == hi && !(lo_inc && hi_inc))) {
        return result;
    }
    const auto begin =
        lo_inc ? LowerBound(*layout_, lo) : UpperBound(*layout_, lo);
    const auto end =
        hi_inc ? UpperBound(*layout_, hi) : LowerBound(*layout_, hi);
    for (size_t i = begin; i < end; ++i) {
        SetPosting(result, layout_->Posting(i));
    }
    return result;
}

std::string_view
SortedStorageView<std::string_view>::ValueAt(size_t index) const {
    return layout_->Value(index);
}

bool
SortedStorageView<std::string_view>::ValidReverseIndex(int32_t index) const {
    return index >= 0 &&
           static_cast<size_t>(index) < layout_->UniqueCount();
}

int64_t
SortedStorageView<std::string_view>::MemoryUsage() const {
    return layout_->MemoryUsage();
}

int64_t
SortedStorageView<std::string_view>::FileUsage() const {
    return layout_->FileUsage();
}

TargetBitmap
SortedStorageView<std::string_view>::PatternMatch(
    size_t count, std::string_view pattern, PatternOp op) const {
    TargetBitmap result(count, false);
    if (op == PatternOp::PrefixMatch) {
        const auto range = PrefixRange(*layout_, pattern);
        for (size_t i = range.first; i < range.second; ++i) {
            SetPosting(result, layout_->Posting(i));
        }
        return result;
    }

    if (op == PatternOp::Match) {
        const std::string owned(pattern);
        const auto prefix = extract_fixed_prefix_from_pattern(owned);
        const auto range = PrefixRange(*layout_, prefix);
        LikePatternMatcher matcher(owned);
        for (size_t i = range.first; i < range.second; ++i) {
            if (matcher(layout_->Value(i))) {
                SetPosting(result, layout_->Posting(i));
            }
        }
        return result;
    }

    if (op == PatternOp::RegexMatch) {
        PartialRegexMatcher matcher{std::string(pattern)};
        for (size_t i = 0; i < layout_->UniqueCount(); ++i) {
            if (matcher(layout_->Value(i))) {
                SetPosting(result, layout_->Posting(i));
            }
        }
        return result;
    }

    if (op != PatternOp::PostfixMatch && op != PatternOp::InnerMatch) {
        ThrowInfo(OpTypeInvalid,
                  "invalid pattern operator for sorted string index");
    }

    for (size_t i = 0; i < layout_->UniqueCount(); ++i) {
        const auto value = layout_->Value(i);
        bool matches = false;
        if (op == PatternOp::PostfixMatch) {
            matches = value.size() >= pattern.size() &&
                      value.substr(value.size() - pattern.size()) == pattern;
        } else if (op == PatternOp::InnerMatch) {
            matches = value.find(pattern) != std::string_view::npos;
        }
        if (matches) {
            SetPosting(result, layout_->Posting(i));
        }
    }
    return result;
}

}  // namespace sorted_reader_detail

#define INSTANTIATE_SORTED_READER(T)               \
    template class sorted_reader_detail::SortedStorageView<T>; \
    template class SortedIndexReader<T>;
INSTANTIATE_SORTED_READER(bool)
INSTANTIATE_SORTED_READER(int8_t)
INSTANTIATE_SORTED_READER(int16_t)
INSTANTIATE_SORTED_READER(int32_t)
INSTANTIATE_SORTED_READER(int64_t)
INSTANTIATE_SORTED_READER(float)
INSTANTIATE_SORTED_READER(double)
#undef INSTANTIATE_SORTED_READER

template class SortedIndexReader<std::string_view>;

}  // namespace milvus::index
