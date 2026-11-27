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
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/index/index.h"

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

inline size_t
GetValidDataBitmapSize(size_t count) {
    return count / 8 + (count % 8 != 0);
}

inline size_t
FromValidDataCount(uint64_t count) {
    AssertInfo(count <= std::numeric_limits<size_t>::max(),
               "nullable vector valid_data count is too large");
    return static_cast<size_t>(count);
}

inline bool
IsAllNullNullable(const knowhere::Index<knowhere::IndexNode>& index) {
    const auto has_validity = index.HasValidity();
    AssertInfo(has_validity.has_value(),
               "failed to inspect nullable validity: {}",
               has_validity.what());
    if (!has_validity.value()) {
        return false;
    }
    const auto count = index.ValidCount();
    AssertInfo(
        count.has_value(), "failed to read valid count: {}", count.what());
    return count.value() == 0;
}

struct ValidDataView {
    bool found = false;
    size_t count = 0;
    const uint8_t* bitmap = nullptr;
};

struct OwnedValidData {
    bool found = false;
    size_t count = 0;
    std::vector<uint8_t> bitmap;

    ValidDataView
    View() const {
        return {found, count, bitmap.data()};
    }
};

inline knowhere::IdMapData
MakeIdMapData(const ValidDataView& valid_data) {
    AssertInfo(valid_data.found, "nullable vector valid_data is empty");
    return knowhere::IdMapData::FromValidBitmap(valid_data.bitmap,
                                                valid_data.count);
}

inline DatasetPtr
GenIdMapDataset(int64_t rows,
                int64_t dim,
                const knowhere::IdMapData& id_map_data,
                bool is_sparse = false) {
    auto dataset = GenDataset(rows, dim, nullptr);
    dataset->SetIdMapData(id_map_data);
    if (is_sparse) {
        dataset->SetIsSparse(true);
    }
    return dataset;
}

inline DatasetPtr
GenIdMapDataset(int64_t rows,
                int64_t dim,
                const ValidDataView& valid_data,
                bool is_sparse = false) {
    auto id_map_data = MakeIdMapData(valid_data);
    return GenIdMapDataset(rows, dim, id_map_data, is_sparse);
}

struct IdMapOnlyBuildPlan {
    bool enabled = false;
    bool empty_embedding_list = false;
};

inline IdMapOnlyBuildPlan
GetIdMapOnlyBuildPlan(const DatasetPtr& dataset, bool is_embedding_list) {
    AssertInfo(dataset != nullptr, "dataset is null");
    if (milvus::GetDatasetRows(dataset) != 0) {
        return {};
    }
    if (is_embedding_list) {
        AssertInfo(dataset->HasIdMapData(),
                   "empty embedding list dataset must provide id map data");
        return {true, true};
    }
    return {dataset->HasIdMapData(), false};
}

inline int64_t
ResolveDatasetOrConfigDim(const DatasetPtr& dataset,
                          const Config& config,
                          const char* context) {
    auto dim = dataset == nullptr ? 0 : dataset->GetDim();
    if (dim > 0) {
        return dim;
    }
    auto config_dim = GetValueFromConfig<int64_t>(config, DIM_KEY);
    AssertInfo(config_dim.has_value() && config_dim.value() > 0,
               "dim is missing when {}",
               context);
    return config_dim.value();
}

inline constexpr const char* ID_MAP_MMAP_DIR = "id_mapping_mmap";

inline knowhere::MappingMmapOptions
NullableMmapOptions(const Config& config, const std::string& prefix) {
    const bool search_ids =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_I2O_MAP).value_or(false);
    const bool retrieval_ids =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_O2I_MAP).value_or(false);
    if (!search_ids && !retrieval_ids) {
        return {};
    }
    AssertInfo(!prefix.empty(),
               "nullable mapping mmap requires a staging parent");
    return {search_ids,
            retrieval_ids,
            (std::filesystem::path(prefix) / ID_MAP_MMAP_DIR).string()};
}

inline knowhere::LoadOptions
MakeNullableLoadInput(const ValidDataView& valid_data,
                      const Config& config = {},
                      const std::string& prefix = {},
                      bool embedding_list = false) {
    if (!valid_data.found) {
        return {.embedding_list = embedding_list};
    }
    return {knowhere::ValidityView{valid_data.bitmap,
                                   GetValidDataBitmapSize(valid_data.count),
                                   static_cast<uint64_t>(valid_data.count)},
            NullableMmapOptions(config, prefix),
            embedding_list};
}

inline std::optional<knowhere::ValidityData>
ExportValidData(const knowhere::Index<knowhere::IndexNode>& index) {
    auto result = index.ExportValidity();
    AssertInfo(result.has_value(),
               "failed to export nullable validity: {}",
               result.what());
    return std::move(result.value());
}

inline void
AppendValidDataToBinarySet(const knowhere::Index<knowhere::IndexNode>& index,
                           BinarySet& binary_set) {
    auto validity = ExportValidData(index);
    if (!validity.has_value()) {
        return;
    }
    auto wire_count = validity->logical_row_count;
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));
    const auto& packed_data = validity->packed_bits;
    std::shared_ptr<uint8_t[]> data(new uint8_t[packed_data.size()]);
    if (!packed_data.empty()) {
        milvus::fastmem::FastMemcpy(
            data.get(), packed_data.data(), packed_data.size());
    }
    binary_set.Append(VALID_DATA_KEY, data, packed_data.size());
}

inline ValidDataView
LoadValidDataViewFromPayload(const uint8_t* count_data,
                             int64_t count_size,
                             const uint8_t* bitmap_data,
                             int64_t bitmap_size) {
    AssertInfo(count_data != nullptr &&
                   count_size == static_cast<int64_t>(sizeof(uint64_t)),
               "nullable vector index valid_data count file is invalid");
    uint64_t wire_count = 0;
    milvus::fastmem::FastMemcpy(&wire_count, count_data, sizeof(uint64_t));
    auto count = FromValidDataCount(wire_count);

    const auto expected_bitmap_size =
        static_cast<int64_t>(GetValidDataBitmapSize(count));
    AssertInfo((bitmap_data != nullptr || expected_bitmap_size == 0) &&
                   bitmap_size >= expected_bitmap_size,
               "nullable vector index valid_data bitmap file is invalid");
    return {true, count, bitmap_data};
}

inline ValidDataView
LoadValidDataViewFromBinarySet(const BinarySet& binary_set) {
    const bool has_count = binary_set.Contains(VALID_DATA_COUNT_KEY);
    const bool has_data = binary_set.Contains(VALID_DATA_KEY);
    if (!has_count && !has_data) {
        return {};
    }
    AssertInfo(has_count && has_data,
               "nullable vector index valid_data files are incomplete");
    auto count_ptr = binary_set.GetByName(VALID_DATA_COUNT_KEY);
    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    AssertInfo(count_ptr != nullptr && data_ptr != nullptr,
               "nullable vector index valid_data files are incomplete");
    return LoadValidDataViewFromPayload(count_ptr->data.get(),
                                        count_ptr->size,
                                        data_ptr->data.get(),
                                        data_ptr->size);
}

inline void
ImportValidData(knowhere::Index<knowhere::IndexNode>& index,
                const knowhere::LoadOptions& options) {
    if (!options.validity.has_value()) {
        return;
    }
    const auto status = index.ImportValidity(*options.validity, options.mmap);
    AssertInfo(status == knowhere::Status::success,
               "failed to import nullable validity: {}",
               KnowhereStatusString(status));
}

}  // namespace milvus::index
