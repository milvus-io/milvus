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

#include <cstdint>

#include "common/Chunk.h"

namespace milvus {

// === StringChunk DataView implementations ===

AnyDataView
StringChunk::GetAnyDataView() const {
    return GetAnyDataView(0, row_nums_);
}

AnyDataView
StringChunk::GetAnyDataView(int64_t offset, int64_t length) const {
    AssertInfo(offset >= 0 && offset < row_nums_,
               "Retrieve string data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(length > 0 && length <= row_nums_,
               "Retrieve string data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(offset + length <= row_nums_,
               "Retrieve string data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);

    std::vector<std::string_view> views;
    views.reserve(length);
    auto end_offset = offset + length;
    for (auto i = offset; i < end_offset; i++) {
        views.emplace_back(data_ + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }

    FixedVector<bool> valid_data;
    if (nullable_) {
        valid_data.assign(valid_.begin() + offset, valid_.begin() + end_offset);
    }

    return AnyDataView(std::make_shared<ContiguousDataView<std::string_view>>(
        std::move(views), std::move(valid_data), length));
}

AnyDataView
StringChunk::GetAnyDataView(const FixedVector<int32_t>& offsets) const {
    std::vector<std::string_view> views;
    FixedVector<bool> valid_data;
    size_t size = offsets.size();
    views.reserve(size);
    valid_data.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        views.emplace_back(data_ + offsets_[offsets[i]],
                           offsets_[offsets[i] + 1] - offsets_[offsets[i]]);
        valid_data.emplace_back(IsValid(offsets[i]));
    }

    return AnyDataView(std::make_shared<ContiguousDataView<std::string_view>>(
        std::move(views), std::move(valid_data), size));
}

// === JSONChunk DataView implementations ===

AnyDataView
JSONChunk::GetAnyDataView() const {
    return GetAnyDataView(0, row_nums_);
}

AnyDataView
JSONChunk::GetAnyDataView(int64_t offset, int64_t length) const {
    AssertInfo(offset >= 0 && offset < row_nums_,
               "Retrieve json data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(length > 0 && length <= row_nums_,
               "Retrieve json data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(offset + length <= row_nums_,
               "Retrieve json data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);

    std::vector<std::string_view> views;
    views.reserve(length);
    auto end_offset = offset + length;
    for (auto i = offset; i < end_offset; i++) {
        views.emplace_back(data_ + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }

    FixedVector<bool> valid_data;
    if (nullable_) {
        valid_data.assign(valid_.begin() + offset, valid_.begin() + end_offset);
    }

    return AnyDataView(std::make_shared<ContiguousDataView<std::string_view>>(
        std::move(views), std::move(valid_data), length));
}

AnyDataView
JSONChunk::GetAnyDataView(const FixedVector<int32_t>& offsets) const {
    std::vector<std::string_view> views;
    FixedVector<bool> valid_data;
    size_t size = offsets.size();
    views.reserve(size);
    valid_data.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        views.emplace_back(data_ + offsets_[offsets[i]],
                           offsets_[offsets[i] + 1] - offsets_[offsets[i]]);
        valid_data.emplace_back(IsValid(offsets[i]));
    }

    return AnyDataView(std::make_shared<ContiguousDataView<std::string_view>>(
        std::move(views), std::move(valid_data), size));
}

// === ArrayChunk DataView implementations ===

AnyDataView
ArrayChunk::GetAnyDataView() const {
    return GetAnyDataView(0, row_nums_);
}

AnyDataView
ArrayChunk::GetAnyDataView(int64_t offset, int64_t length) const {
    AssertInfo(offset >= 0 && offset < row_nums_,
               "Retrieve array data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(length > 0 && length <= row_nums_,
               "Retrieve array data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);
    AssertInfo(offset + length <= row_nums_,
               "Retrieve array data view with out-of-bound offset:{}, len:{}, "
               "row_nums:{}",
               offset,
               length,
               row_nums_);

    std::vector<ArrayView> views;
    views.reserve(length);
    auto end_offset = offset + length;
    for (auto i = offset; i < end_offset; i++) {
        views.emplace_back(View(i));
    }

    FixedVector<bool> valid_data;
    if (nullable_) {
        valid_data.assign(valid_.begin() + offset, valid_.begin() + end_offset);
    }

    return AnyDataView(std::make_shared<ContiguousDataView<ArrayView>>(
        std::move(views), std::move(valid_data), length));
}

AnyDataView
ArrayChunk::GetAnyDataView(const FixedVector<int32_t>& offsets) const {
    std::vector<ArrayView> views;
    FixedVector<bool> valid_data;
    size_t size = offsets.size();
    views.reserve(size);
    valid_data.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        views.emplace_back(View(offsets[i]));
        valid_data.emplace_back(IsValid(offsets[i]));
    }

    return AnyDataView(std::make_shared<ContiguousDataView<ArrayView>>(
        std::move(views), std::move(valid_data), size));
}

// === VectorArrayChunk DataView implementations ===

AnyDataView
VectorArrayChunk::GetAnyDataView() const {
    return GetAnyDataView(0, row_nums_);
}

AnyDataView
VectorArrayChunk::GetAnyDataView(int64_t offset, int64_t length) const {
    AssertInfo(
        offset >= 0 && offset < row_nums_,
        "Retrieve vector array data view with out-of-bound offset:{}, len:{}, "
        "row_nums:{}",
        offset,
        length,
        row_nums_);
    AssertInfo(
        length > 0 && length <= row_nums_,
        "Retrieve vector array data view with out-of-bound offset:{}, len:{}, "
        "row_nums:{}",
        offset,
        length,
        row_nums_);
    AssertInfo(
        offset + length <= row_nums_,
        "Retrieve vector array data view with out-of-bound offset:{}, len:{}, "
        "row_nums:{}",
        offset,
        length,
        row_nums_);

    std::vector<VectorArrayView> views;
    views.reserve(length);
    auto end_offset = offset + length;
    for (int64_t i = offset; i < end_offset; i++) {
        views.emplace_back(View(i));
    }

    // VectorArrayChunk does not support null
    return AnyDataView(std::make_shared<ContiguousDataView<VectorArrayView>>(
        std::move(views), nullptr, length, dim_));
}

}  // namespace milvus
