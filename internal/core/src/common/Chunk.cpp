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
StringChunk::StringViews(std::optional<std::pair<int64_t, int64_t>> offset_len=std::nullopt) {
    auto start_offset = 0;
    auto len = row_nums_;
    if (offset_len.has_value()) {
        start_offset = offset_len->first;
        len = offset_len->second;
        AssertInfo(start_offset >= 0 && start_offset < row_nums_, "Retrieve string views with out-of-bound offset:{}, len:{}, wrong", start_offset, len);
        AssertInfo(len > 0 && len <= row_nums_, "Retrieve string views with out-of-bound offset:{}, len:{}, wrong", start_offset, len);
        AssertInfo(start_offset + len <= row_nums_, "Retrieve string views with out-of-bound offset:{}, len:{}, wrong", start_offset, len);
    }

    std::vector<std::string_view> ret;
    ret.reserve(len);
    auto offset = start_offset;
    for (int i = 0; i < len; i++) {
        ret.emplace_back(data_ + offsets_[offset], offsets_[offset + 1] - offsets_[offset]);
        offset+=1;
    }
    return {ret, FixedVector<bool>(valid_.begin() + start_offset, valid_.begin() + start_offset + len)};
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

}  // namespace milvus
