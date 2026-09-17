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

#include "index/scalar/sort/SortedIndexArtifact.h"

#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "index/scalar/sort/SortedIndexFormat.h"
#include "index/scalar/sort/SortedIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

void
CheckedAdd(size_t& total, size_t value) {
    if (value > std::numeric_limits<size_t>::max() - total) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string serialized size overflows size_t");
    }
    total += value;
}

size_t
CheckedMultiply(size_t left, size_t right) {
    if (right != 0 && left > std::numeric_limits<size_t>::max() / right) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string serialized size multiplication overflows");
    }
    return left * right;
}

uint32_t
ToUint32(size_t value, std::string_view label) {
    if (value > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "sorted string {} {} exceeds uint32 domain",
                  label,
                  value);
    }
    return static_cast<uint32_t>(value);
}

template <typename T>
void
WritePod(std::vector<uint8_t>& output, size_t& offset, const T& value) {
    AssertInfo(offset <= output.size() && output.size() - offset >= sizeof(T),
               "sorted string serializer exceeded allocated buffer");
    std::memcpy(output.data() + offset, &value, sizeof(T));
    offset += sizeof(T);
}

std::vector<uint8_t>
SerializeStrings(const SortedStringLayout& layout) {
    const auto unique_count = ToUint32(layout.UniqueCount(), "unique count");

    size_t total = sizeof(uint32_t);
    CheckedAdd(total, CheckedMultiply(layout.UniqueCount(), sizeof(uint32_t)));
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto value = layout.Value(i);
        (void)ToUint32(value.size(), "value length");
        CheckedAdd(total, sizeof(uint32_t));
        CheckedAdd(total, value.size());
    }
    CheckedAdd(total, CheckedMultiply(layout.UniqueCount(), sizeof(uint32_t)));
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto posting = layout.Posting(i);
        (void)ToUint32(posting.size, "posting length");
        CheckedAdd(total, sizeof(uint32_t));
        CheckedAdd(total, CheckedMultiply(posting.size, sizeof(uint32_t)));
    }
    CheckedAdd(total, sizeof(uint64_t));

    std::vector<uint8_t> output(total);
    size_t offset = 0;
    WritePod(output, offset, unique_count);

    const auto string_offsets_start = offset;
    offset += layout.UniqueCount() * sizeof(uint32_t);
    const auto string_data_start = offset;
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto value = layout.Value(i);
        const auto relative =
            ToUint32(offset - string_data_start, "string offset");
        std::memcpy(output.data() + string_offsets_start + i * sizeof(uint32_t),
                    &relative,
                    sizeof(relative));
        const auto length = ToUint32(value.size(), "value length");
        WritePod(output, offset, length);
        if (!value.empty()) {
            std::memcpy(output.data() + offset, value.data(), value.size());
            offset += value.size();
        }
    }

    const auto posting_offsets_start = offset;
    offset += layout.UniqueCount() * sizeof(uint32_t);
    const auto posting_data_start = offset;
    for (size_t i = 0; i < layout.UniqueCount(); ++i) {
        const auto posting = layout.Posting(i);
        const auto relative =
            ToUint32(offset - posting_data_start, "posting offset");
        std::memcpy(
            output.data() + posting_offsets_start + i * sizeof(uint32_t),
            &relative,
            sizeof(relative));
        const auto length = ToUint32(posting.size, "posting length");
        WritePod(output, offset, length);
        for (size_t row = 0; row < posting.size; ++row) {
            WritePod(output, offset, posting.At(row));
        }
    }

    WritePod(output, offset, sort_format::kStringMagic);
    AssertInfo(offset == output.size(),
               "sorted string serialized size mismatch");
    return output;
}

std::vector<uint8_t>
SerializePackedValidity(const TargetBitmap& validity, size_t count) {
    std::vector<uint8_t> output((count + 7) / 8, 0);
    for (size_t i = 0; i < count; ++i) {
        if (validity[i]) {
            output[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return output;
}

}  // namespace

template <typename T>
SortedIndexArtifact<T>::SortedIndexArtifact(std::vector<IndexStructure<T>> data,
                                            TargetBitmap valid_bitset,
                                            std::vector<int32_t> idx_to_offsets,
                                            size_t total_num_rows,
                                            bool nested)
    : data_(std::move(data)),
      valid_bitset_(std::move(valid_bitset)),
      idx_to_offsets_(std::move(idx_to_offsets)),
      total_num_rows_(total_num_rows),
      nested_(nested) {
    static_assert(std::is_trivially_copyable_v<IndexStructure<T>>);
}

template <typename T>
SortedIndexArtifact<T>::~SortedIndexArtifact() = default;

template <typename T>
void
SortedIndexArtifact<T>::Serialize(storage::FileSink& sink) const {
    const auto data_bytes = data_.size() * sizeof(IndexStructure<T>);
    if (sink.Gen() == storage::Generation::V1V2) {
        const auto index_length = data_.size();
        sink.WriteEntry(sort_format::kIndexData, data_.data(), data_bytes);
        sink.WriteEntry(
            sort_format::kIndexLength, &index_length, sizeof(index_length));
        sink.WriteEntry(sort_format::kLegacyNumRows,
                        &total_num_rows_,
                        sizeof(total_num_rows_));
        sink.WriteEntry(
            sort_format::kLegacyNested, &nested_, sizeof(nested_));
        return;
    }

    sink.PutMeta(sort_format::kIndexLength, nlohmann::json(data_.size()));
    sink.PutMeta(sort_format::kNumRows, nlohmann::json(total_num_rows_));
    sink.PutMeta(sort_format::kNested, nlohmann::json(nested_));
    sink.WriteEntry(sort_format::kIndexData, data_.data(), data_bytes);
    sink.WriteEntry(sort_format::kIdxToOffsets,
                    idx_to_offsets_.data(),
                    idx_to_offsets_.size() * sizeof(int32_t));
    sink.WriteEntry(sort_format::kValidBitset,
                    valid_bitset_.data(),
                    valid_bitset_.size_in_bytes());
}

SortedStringIndexArtifact::SortedStringIndexArtifact(
    std::vector<std::string> unique_values,
    std::vector<std::vector<uint32_t>> posting_lists,
    TargetBitmap valid_bitset,
    std::vector<int32_t> idx_to_offsets,
    size_t total_num_rows,
    bool nested)
    : layout_(SortedStringLayout::FromHeap(
          std::move(unique_values),
          std::move(posting_lists),
          total_num_rows)),
      valid_bitset_(std::move(valid_bitset)),
      idx_to_offsets_(std::move(idx_to_offsets)),
      total_num_rows_(total_num_rows),
      nested_(nested) {
    AssertInfo(layout_ != nullptr,
               "sorted string artifact layout is null");
}

SortedStringIndexArtifact::~SortedStringIndexArtifact() = default;

void
SortedStringIndexArtifact::Serialize(storage::FileSink& sink) const {
    const auto packed = SerializeStrings(*layout_);
    const auto validity =
        SerializePackedValidity(valid_bitset_, total_num_rows_);
    if (sink.Gen() == storage::Generation::V1V2) {
        const auto version = sort_format::kStringVersion;
        sink.WriteEntry(sort_format::kVersion, &version, sizeof(version));
        sink.WriteEntry(sort_format::kIndexData, packed.data(), packed.size());
        sink.WriteEntry(sort_format::kLegacyNumRows,
                        &total_num_rows_,
                        sizeof(total_num_rows_));
        sink.WriteEntry(
            sort_format::kValidBitset, validity.data(), validity.size());
        sink.WriteEntry(
            sort_format::kLegacyNested, &nested_, sizeof(nested_));
        return;
    }

    sink.PutMeta(sort_format::kVersion,
                 nlohmann::json(sort_format::kStringVersion));
    sink.PutMeta(sort_format::kNumRows, nlohmann::json(total_num_rows_));
    sink.PutMeta(sort_format::kNested, nlohmann::json(nested_));
    sink.WriteEntry(sort_format::kIndexData, packed.data(), packed.size());
    sink.WriteEntry(
        sort_format::kValidBitset, validity.data(), validity.size());
    sink.WriteEntry(sort_format::kIdxToOffsets,
                    idx_to_offsets_.data(),
                    idx_to_offsets_.size() * sizeof(int32_t));
}

#define INSTANTIATE_SORTED_ARTIFACT(T) template class SortedIndexArtifact<T>;
INSTANTIATE_SORTED_ARTIFACT(bool)
INSTANTIATE_SORTED_ARTIFACT(int8_t)
INSTANTIATE_SORTED_ARTIFACT(int16_t)
INSTANTIATE_SORTED_ARTIFACT(int32_t)
INSTANTIATE_SORTED_ARTIFACT(int64_t)
INSTANTIATE_SORTED_ARTIFACT(float)
INSTANTIATE_SORTED_ARTIFACT(double)
#undef INSTANTIATE_SORTED_ARTIFACT

}  // namespace milvus::index
