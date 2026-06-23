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

#pragma once

#include <algorithm>
#include <cstddef>
#include "common/FastMem.h"
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/OffsetMapping.h"
#include "index/VectorIndex.h"

namespace milvus::index {

inline bool
IsValidDataBinary(const std::string& name) {
    return name == VALID_DATA_COUNT_KEY || name == VALID_DATA_KEY;
}

inline std::string
GetIndexFileName(const std::string& file) {
    auto pos = file.find_last_of("/\\");
    if (pos == std::string::npos) {
        return file;
    }
    return file.substr(pos + 1);
}

inline bool
IsValidDataDiskFileSlice(const std::string& file) {
    const auto file_name = GetIndexFileName(file);
    const std::string prefix = std::string(VALID_DATA_KEY) + "_";
    if (file_name.size() <= prefix.size() ||
        file_name.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    return std::all_of(file_name.begin() + prefix.size(),
                       file_name.end(),
                       [](char c) { return c >= '0' && c <= '9'; });
}

inline std::vector<std::string>
FilterValidDataDiskFileSlices(const std::vector<std::string>& files) {
    std::vector<std::string> valid_data_files;
    for (const auto& file : files) {
        if (IsValidDataDiskFileSlice(file)) {
            valid_data_files.emplace_back(file);
        }
    }
    return valid_data_files;
}

inline std::vector<std::string>
GetCacheFilesForDiskIndexLoad(const std::vector<std::string>& index_files,
                              bool load_index_with_stream) {
    return load_index_with_stream ? FilterValidDataDiskFileSlices(index_files)
                                  : index_files;
}

inline bool
ContainsOnlyValidData(const BinarySet& binary_set) {
    if (!binary_set.Contains(VALID_DATA_COUNT_KEY) ||
        !binary_set.Contains(VALID_DATA_KEY)) {
        return false;
    }
    for (const auto& [name, _] : binary_set.binary_map_) {
        if (!IsValidDataBinary(name)) {
            return false;
        }
    }
    return true;
}

inline bool
IsAllNullNullable(const OffsetMapping& offset_mapping) {
    return offset_mapping.IsEnabled() && offset_mapping.GetValidCount() == 0;
}

inline size_t
GetValidDataBitmapSize(size_t count) {
    return (count + 7) / 8;
}

inline uint64_t
ToValidDataCount(size_t count) {
    return static_cast<uint64_t>(count);
}

inline size_t
FromValidDataCount(uint64_t count) {
    AssertInfo(count <= std::numeric_limits<size_t>::max(),
               "nullable vector valid_data count is too large");
    return static_cast<size_t>(count);
}

inline size_t
CountValidDataBitmap(size_t count, const uint8_t* bitmap) {
    size_t valid_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if ((bitmap[i / 8] >> (i % 8)) & 1) {
            ++valid_count;
        }
    }
    return valid_count;
}

inline std::vector<uint8_t>
PackValidDataBitmap(const OffsetMapping& offset_mapping) {
    auto count = static_cast<size_t>(offset_mapping.GetTotalCount());
    std::vector<uint8_t> data(GetValidDataBitmapSize(count), 0);
    for (size_t i = 0; i < count; ++i) {
        if (offset_mapping.IsValid(i)) {
            data[i / 8] |= (1 << (i % 8));
        }
    }
    return data;
}

inline void
BuildValidDataFromBitmap(VectorIndex* vector_index,
                         size_t count,
                         const uint8_t* bitmap) {
    std::unique_ptr<bool[]> valid_data(new bool[count]);
    for (size_t i = 0; i < count; ++i) {
        valid_data[i] = (bitmap[i / 8] >> (i % 8)) & 1;
    }
    vector_index->BuildValidData(valid_data.get(), count);
}

inline void
AppendValidDataToBinarySet(const OffsetMapping& offset_mapping,
                           BinarySet& binary_set) {
    if (!offset_mapping.IsEnabled()) {
        return;
    }

    auto count = static_cast<size_t>(offset_mapping.GetTotalCount());
    auto wire_count = ToValidDataCount(count);
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));

    auto packed_data = PackValidDataBitmap(offset_mapping);
    std::shared_ptr<uint8_t[]> data(new uint8_t[packed_data.size()]);
    if (!packed_data.empty()) {
        milvus::fastmem::FastMemcpy(
            data.get(), packed_data.data(), packed_data.size());
    }
    binary_set.Append(VALID_DATA_KEY, data, packed_data.size());
}

inline bool
LoadValidDataFromBinarySet(const BinarySet& binary_set,
                           VectorIndex* vector_index) {
    bool has_count = binary_set.Contains(VALID_DATA_COUNT_KEY);
    bool has_data = binary_set.Contains(VALID_DATA_KEY);
    if (!has_count && !has_data) {
        return false;
    }
    AssertInfo(has_count && has_data,
               "nullable vector index valid_data files are incomplete");

    auto count_ptr = binary_set.GetByName(VALID_DATA_COUNT_KEY);
    AssertInfo(count_ptr != nullptr && count_ptr->size == sizeof(uint64_t),
               "nullable vector index valid_data count file is invalid");
    uint64_t wire_count = 0;
    milvus::fastmem::FastMemcpy(
        &wire_count, count_ptr->data.get(), sizeof(uint64_t));
    auto count = FromValidDataCount(wire_count);

    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    AssertInfo(
        data_ptr != nullptr && data_ptr->size >= GetValidDataBitmapSize(count),
        "nullable vector index valid_data bitmap file is invalid");
    BuildValidDataFromBitmap(vector_index, count, data_ptr->data.get());
    return true;
}

}  // namespace milvus::index
