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
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "knowhere/binaryset.h"
#include "knowhere/id_map.h"

namespace milvus::index {

constexpr const char* VALID_DATA_KEY = "valid_data";
constexpr const char* VALID_DATA_COUNT_KEY = "valid_data_count";

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

inline size_t
GetValidDataBitmapSize(size_t count) {
    return (count + 7) / 8;
}

inline bool
IsValidInBitmap(const uint8_t* bitmap, size_t offset) {
    return (bitmap[offset / 8] >> (offset % 8)) & 1;
}

inline size_t
CountValidDataBitmap(size_t count, const uint8_t* bitmap) {
    if (bitmap == nullptr) {
        return count;
    }
    size_t valid_count = 0;
    const auto full_bytes = count >> 3;
    const auto full_uint64 = full_bytes >> 3;

    for (size_t i = 0; i < full_uint64; ++i) {
        uint64_t word = 0;
        milvus::fastmem::FastMemcpy(
            &word, bitmap + i * sizeof(uint64_t), sizeof(uint64_t));
        valid_count += static_cast<size_t>(__builtin_popcountll(word));
    }

    auto popcount8 = [](uint8_t x) -> int {
        x = (x & 0x55) + ((x >> 1) & 0x55);
        x = (x & 0x33) + ((x >> 2) & 0x33);
        x = (x & 0x0F) + ((x >> 4) & 0x0F);
        return x;
    };
    const auto* p_uint8 = bitmap + (full_uint64 << 3);
    for (size_t i = (full_uint64 << 3); i < full_bytes; ++i) {
        valid_count += static_cast<size_t>(popcount8(*p_uint8));
        ++p_uint8;
    }

    const auto tail_bits = count & 7;
    if (tail_bits != 0) {
        const auto mask = static_cast<uint8_t>((1U << tail_bits) - 1);
        valid_count +=
            static_cast<size_t>(popcount8(bitmap[full_bytes] & mask));
    }
    return valid_count;
}

inline void
AppendValidDataToBinarySet(size_t count,
                           const uint8_t* bitmap,
                           BinarySet& binary_set) {
    auto wire_count = static_cast<uint64_t>(count);
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));

    const auto bitmap_size = GetValidDataBitmapSize(count);
    std::shared_ptr<uint8_t[]> data(new uint8_t[bitmap_size]);
    if (bitmap_size != 0 && bitmap != nullptr) {
        milvus::fastmem::FastMemcpy(data.get(), bitmap, bitmap_size);
    } else if (bitmap_size != 0) {
        std::fill(data.get(), data.get() + bitmap_size, 0);
    }
    binary_set.Append(VALID_DATA_KEY, data, bitmap_size);
}

inline void
AppendValidDataToBinarySet(const knowhere::IdMap& id_map,
                           BinarySet& binary_set) {
    const auto id_map_snapshot = id_map.GetSnapshot();
    const auto& valid_bitmap = id_map_snapshot.GetValidBitmap();
    if (valid_bitmap.size() == 0) {
        return;
    }
    AppendValidDataToBinarySet(
        valid_bitmap.size(), valid_bitmap.data(), binary_set);
}

inline bool
LoadValidDataFromBinarySet(const BinarySet& binary_set,
                           knowhere::IdMap& id_map) {
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
    auto count = static_cast<size_t>(wire_count);

    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    AssertInfo(
        data_ptr != nullptr && data_ptr->size >= GetValidDataBitmapSize(count),
        "nullable vector index valid_data bitmap file is invalid");
    id_map.SetValidBitmap(data_ptr->data.get(), static_cast<int64_t>(count));
    return true;
}

}  // namespace milvus::index
