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

std::vector<std::string_view>
StringChunk::StringViews() const {
    std::vector<std::string_view> ret;
    for (int i = 0; i < row_nums_ - 1; i++) {
        ret.emplace_back(data_ + offsets_[i], offsets_[i + 1] - offsets_[i]);
    }
    ret.emplace_back(data_ + offsets_[row_nums_ - 1],
                     size_ - MMAP_STRING_PADDING - offsets_[row_nums_ - 1]);
    return ret;
}

void
ArrayChunk::ConstructViews() {
    views_.reserve(row_nums_);

    for (int i = 0; i < row_nums_; ++i) {
        auto data_ptr = data_ + offsets_[i];
        auto next_data_ptr = i == row_nums_ - 1
                                 ? data_ + size_ - MMAP_ARRAY_PADDING
                                 : data_ + offsets_[i + 1];
        auto offsets_len = lens_[i] * sizeof(uint64_t);
        std::vector<uint64_t> element_indices = {};
        if (IsStringDataType(element_type_)) {
            std::vector<uint64_t> tmp(
                reinterpret_cast<uint64_t*>(data_ptr),
                reinterpret_cast<uint64_t*>(data_ptr + offsets_len));
            element_indices = std::move(tmp);
        }
        views_.emplace_back(data_ptr + offsets_len,
                            next_data_ptr - data_ptr - offsets_len,
                            element_type_,
                            std::move(element_indices));
    }
}

SpanBase
ArrayChunk::Span() const {
    return SpanBase(views_.data(), views_.size(), sizeof(ArrayView));
}

}  // namespace milvus
