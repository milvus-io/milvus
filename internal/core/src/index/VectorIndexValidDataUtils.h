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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "knowhere/dataset.h"

namespace milvus::index {

// Local staging file name used while building disk vector indexes from compact
// raw data. It is not a Milvus index sidecar.
constexpr const char* NULLABLE_VECTOR_VALID_DATA_FILE = "valid_data";
constexpr const char* DISK_INDEX_EXTERNAL_ID_MAP_FILE = "_id_map";

inline std::string
GetIndexFileName(const std::string& file) {
    auto pos = file.find_last_of("/\\");
    if (pos == std::string::npos) {
        return file;
    }
    return file.substr(pos + 1);
}

inline std::vector<std::string>
GetCacheFilesForDiskIndexLoad(const std::vector<std::string>& index_files,
                              bool load_index_with_stream) {
    (void)load_index_with_stream;
    std::vector<std::string> cache_files;
    cache_files.reserve(index_files.size());
    for (const auto& file : index_files) {
        if (GetIndexFileName(file) == DISK_INDEX_EXTERNAL_ID_MAP_FILE) {
            continue;
        }
        cache_files.emplace_back(file);
    }
    return cache_files;
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
    for (size_t i = 0; i < count; ++i) {
        if (IsValidInBitmap(bitmap, i)) {
            ++valid_count;
        }
    }
    return valid_count;
}

inline size_t
CountValidData(const bool* valid_data, size_t count) {
    if (valid_data == nullptr) {
        return count;
    }
    size_t valid_count = 0;
    for (size_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            ++valid_count;
        }
    }
    return valid_count;
}

inline std::vector<uint8_t>
PackValidDataBitmap(const bool* valid_data, size_t count) {
    std::vector<uint8_t> bitmap(GetValidDataBitmapSize(count), 0);
    if (valid_data == nullptr) {
        for (size_t i = 0; i < count; ++i) {
            bitmap[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
        return bitmap;
    }
    for (size_t i = 0; i < count; ++i) {
        if (valid_data[i]) {
            bitmap[i / 8] |= static_cast<uint8_t>(1U << (i % 8));
        }
    }
    return bitmap;
}

inline void
SetDatasetValidBitmap(const knowhere::DataSetPtr& dataset,
                      const uint8_t* valid_bitmap,
                      int64_t total_count) {
    if (dataset == nullptr || valid_bitmap == nullptr || total_count <= 0) {
        return;
    }
    dataset->SetValidBitmap(valid_bitmap, total_count);
}

inline void
SetDatasetValidBitmap(const knowhere::DataSetPtr& dataset,
                      const bool* valid_data,
                      int64_t total_count) {
    if (dataset == nullptr || valid_data == nullptr || total_count <= 0) {
        return;
    }
    auto bitmap =
        PackValidDataBitmap(valid_data, static_cast<size_t>(total_count));
    dataset->SetValidBitmap(bitmap.data(), total_count);
}

}  // namespace milvus::index
