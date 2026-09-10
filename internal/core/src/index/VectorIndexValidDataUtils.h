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
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/VectorIndex.h"
#include "knowhere/id_map.h"
#include "knowhere/index/index_node.h"

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
    return (count + 7) / 8;
}

inline size_t
FromValidDataCount(uint64_t count) {
    AssertInfo(count <= std::numeric_limits<size_t>::max(),
               "nullable vector valid_data count is too large");
    return static_cast<size_t>(count);
}

inline bool
IsAllNullNullable(const knowhere::IdMap& id_map) {
    return !id_map.ValidBitmap().empty() && id_map.InCount() == 0;
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

struct RestoredIdMap {
    bool has_valid_data = false;
    bool all_null_nullable = false;

    bool
    IsAllNullNullable() const {
        return has_valid_data && all_null_nullable;
    }
};

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

inline void
ConfigureIdMapMmap(knowhere::IdMap& id_map,
                   const Config& config,
                   const std::string& local_index_path_prefix) {
    knowhere::IdMapMmapOptions options;
    options.enable_in_to_out_ids =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_I2O_MAP).value_or(false);
    options.enable_out_to_in_ids =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_O2I_MAP).value_or(false);
    options.mmap_dir_path =
        (std::filesystem::path(local_index_path_prefix) / ID_MAP_MMAP_DIR)
            .string();
    if (options.enable_in_to_out_ids || options.enable_out_to_in_ids) {
        if (!id_map.IsEnabled()) {
            id_map.SetType(knowhere::IdMap::Type::SEALED);
        }
        AssertInfo(id_map.type() == knowhere::IdMap::Type::SEALED,
                   "nullable vector id map mmap requires sealed storage");
        // Mmap applies only to derived dense id arrays. The validity bitmap
        // stays heap-backed and is consumed once by AddFromData.
        id_map.ConfigureMmap(std::move(options));
    }
}

inline void
ConfigureIdMapMmapForDataset(knowhere::IdMap& id_map,
                             const DatasetPtr& dataset,
                             const Config& config,
                             const std::string& local_index_path_prefix) {
    if (dataset == nullptr || !dataset->HasIdMapData()) {
        return;
    }
    ConfigureIdMapMmap(id_map, config, local_index_path_prefix);
}

inline DatasetPtr
GenIdMapDatasetFromValidData(knowhere::IdMap& id_map,
                             int64_t rows,
                             int64_t dim,
                             const ValidDataView& valid_data,
                             const Config& config,
                             const std::string& local_index_path_prefix,
                             bool is_sparse = false) {
    if (!valid_data.found) {
        return nullptr;
    }
    if (!id_map.IsEnabled()) {
        id_map.SetType(knowhere::IdMap::Type::SEALED);
    }
    AssertInfo(id_map.type() == knowhere::IdMap::Type::SEALED,
               "nullable sealed vector index requires sealed id map storage");
    ConfigureIdMapMmap(id_map, config, local_index_path_prefix);
    return GenIdMapDataset(rows, dim, valid_data, is_sparse);
}

inline void
FinalizeRestoredIdMap(knowhere::IndexNode* index_node,
                      ErrorCode error_code,
                      const std::string& context) {
    AssertInfo(index_node != nullptr, "index node is null");
    auto stat = index_node->FinalizeIdMap();
    if (stat != knowhere::Status::success) {
        ThrowInfo(error_code,
                  "failed to finalize id map for {}, {}",
                  context,
                  KnowhereStatusString(stat));
    }
}

inline std::vector<uint8_t>
PackValidDataBitmap(const knowhere::IdMap& id_map) {
    const auto& valid_bitmap = id_map.ValidBitmap();
    if (valid_bitmap.empty()) {
        return {};
    }
    const auto bytes = GetValidDataBitmapSize(valid_bitmap.size());
    std::vector<uint8_t> data(bytes, 0);
    milvus::fastmem::FastMemcpy(data.data(), valid_bitmap.data(), bytes);
    return data;
}

inline void
AppendValidDataToBinarySet(const knowhere::IdMap& id_map,
                           BinarySet& binary_set) {
    if (id_map.ValidBitmap().empty()) {
        return;
    }

    auto count = static_cast<size_t>(id_map.ValidBitmap().size());
    auto wire_count = static_cast<uint64_t>(count);
    std::shared_ptr<uint8_t[]> count_buf(new uint8_t[sizeof(uint64_t)]);
    milvus::fastmem::FastMemcpy(count_buf.get(), &wire_count, sizeof(uint64_t));
    binary_set.Append(VALID_DATA_COUNT_KEY, count_buf, sizeof(uint64_t));

    auto packed_data = PackValidDataBitmap(id_map);
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

inline RestoredIdMap
RestoreIdMapFromValidData(knowhere::IdMap& id_map,
                          const ValidDataView& valid_data,
                          const Config* mmap_config = nullptr,
                          const std::string& mmap_path_prefix = {}) {
    if (!valid_data.found) {
        return {};
    }
    if (!id_map.IsEnabled()) {
        id_map.SetType(knowhere::IdMap::Type::SEALED);
    }
    AssertInfo(id_map.type() == knowhere::IdMap::Type::SEALED,
               "nullable sealed vector index requires sealed id map storage");
    if (mmap_config != nullptr) {
        ConfigureIdMapMmap(id_map, *mmap_config, mmap_path_prefix);
    }
    id_map.AddFromData(MakeIdMapData(valid_data));
    return {true, IsAllNullNullable(id_map)};
}

inline RestoredIdMap
RestoreIdMapFromValidDataPayload(knowhere::IdMap& id_map,
                                 const uint8_t* count_data,
                                 int64_t count_size,
                                 const uint8_t* bitmap_data,
                                 int64_t bitmap_size,
                                 const Config* mmap_config = nullptr,
                                 const std::string& mmap_path_prefix = {}) {
    return RestoreIdMapFromValidData(
        id_map,
        LoadValidDataViewFromPayload(
            count_data, count_size, bitmap_data, bitmap_size),
        mmap_config,
        mmap_path_prefix);
}

inline RestoredIdMap
RestoreIdMapFromBinarySet(const BinarySet& binary_set,
                          knowhere::IdMap& id_map) {
    bool has_count = binary_set.Contains(VALID_DATA_COUNT_KEY);
    bool has_data = binary_set.Contains(VALID_DATA_KEY);
    if (!has_count && !has_data) {
        return {};
    }
    AssertInfo(has_count && has_data,
               "nullable vector index valid_data files are incomplete");

    auto count_ptr = binary_set.GetByName(VALID_DATA_COUNT_KEY);
    auto data_ptr = binary_set.GetByName(VALID_DATA_KEY);
    AssertInfo(count_ptr != nullptr && data_ptr != nullptr,
               "nullable vector index valid_data files are incomplete");
    return RestoreIdMapFromValidDataPayload(id_map,
                                            count_ptr->data.get(),
                                            count_ptr->size,
                                            data_ptr->data.get(),
                                            data_ptr->size);
}

}  // namespace milvus::index
