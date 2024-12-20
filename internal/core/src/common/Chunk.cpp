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

#include <sys/mman.h>
#include <cstdint>
#include "common/Array.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/Chunk.h"

namespace milvus {

std::pair<std::vector<std::string_view>, FixedVector<bool>>
StringChunk::StringViews() {
    std::vector<std::string_view> ret;
    ret.reserve(row_nums_);
    for (int i = 0; i < row_nums_; i++) {
        ret.emplace_back(data_ + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }
    return {ret, valid_};
}

std::pair<std::vector<std::string_view>, FixedVector<bool>>
StringChunk::ViewsByOffsets(const FixedVector<int32_t>& offsets) {
    std::vector<std::string_view> ret;
    FixedVector<bool> valid_res;
    size_t size = offsets.size();
    ret.reserve(size);
    valid_res.reserve(size);
    for (auto i = 0; i < size; ++i) {
        ret.emplace_back(data_ + offsets_[offsets[i]],
                         offsets_[offsets[i] + 1] - offsets_[offsets[i]]);
        valid_res.emplace_back(isValid(offsets[i]));
    }
    return {ret, valid_res};
}

void
ArrayChunk::ConstructViews() {
    views_.reserve(row_nums_);

    for (int i = 0; i < row_nums_; ++i) {
        int offset = offsets_lens_[2 * i];
        int next_offset = offsets_lens_[2 * (i + 1)];
        int len = offsets_lens_[2 * i + 1];

        auto data_ptr = data_ + offset;
        auto offsets_len = 0;
        std::vector<uint64_t> element_indices = {};
        if (IsStringDataType(element_type_)) {
            offsets_len = len * sizeof(uint64_t);
            std::vector<uint64_t> tmp(
                reinterpret_cast<uint64_t*>(data_ptr),
                reinterpret_cast<uint64_t*>(data_ptr + offsets_len));
            element_indices = std::move(tmp);
        }
        views_.emplace_back(data_ptr + offsets_len,
                            next_offset - offset - offsets_len,
                            element_type_,
                            std::move(element_indices));
    }
}

SpanBase
ArrayChunk::Span() const {
    return SpanBase(views_.data(),
                    nullable_ ? valid_.data() : nullptr,
                    views_.size(),
                    sizeof(ArrayView));
}

}  // namespace milvus
