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

#include "index/LoadResource.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/ResourceUsageUtils.h"
#include "index/Utils.h"
#include "index/vector/VectorLoadResource.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "storage/EntryStreamUtils.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PluginLoader.h"

namespace milvus::index {

namespace {

using detail::SaturatingAdd;
using detail::SaturatingMul;

uint64_t
ScalarIndexStreamMemoryOverhead(
    uint64_t index_size_in_bytes,
    int32_t scalar_version,
    bool encrypted,
    bool file_stream,
    const std::optional<storage::EntryStreamLoadInfo>& stream_load_info =
        std::nullopt) {
    if (index_size_in_bytes == 0) {
        return 0;
    }
    if (scalar_version < 3) {
        return index_size_in_bytes;
    }
    size_t total_transient_bytes;
    size_t max_task_transient_bytes;
    if (encrypted && stream_load_info.has_value()) {
        total_transient_bytes = stream_load_info->total_transient_bytes;
        max_task_transient_bytes = stream_load_info->max_task_transient_bytes;
    } else {
        total_transient_bytes =
            storage::EntryStreamTransientBytes(index_size_in_bytes, encrypted);
        max_task_transient_bytes = storage::EntryStreamTransientBytes(
            storage::MaxEntryStreamTaskBytes(), encrypted);
        if (encrypted &&
            storage::TransientMemoryBudget::GetLoadTransientBudget()
                    .CapacityBytes() == 0) {
            return total_transient_bytes;
        }
    }

    if (file_stream && !encrypted) {
        total_transient_bytes = storage::SaturatingMultiply(
            total_transient_bytes, storage::kFileStreamBufferMultiplier);
        max_task_transient_bytes = storage::SaturatingMultiply(
            max_task_transient_bytes, storage::kFileStreamBufferMultiplier);
    }
    if (stream_load_info.has_value()) {
        return total_transient_bytes;
    }
    return storage::EntryStreamMaxTransientBytes(total_transient_bytes,
                                                 max_task_transient_bytes);
}

uint64_t
BitsetBytes(int64_t num_rows) {
    return num_rows <= 0 ? 0 : (static_cast<uint64_t>(num_rows) + 7) / 8;
}

uint64_t
AlignUp(uint64_t size, uint64_t alignment) {
    if (alignment == 0 || size == 0) {
        return size;
    }
    if (size > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
        return std::numeric_limits<uint64_t>::max();
    }
    return ((size + alignment - 1) / alignment) * alignment;
}

uint64_t
BitmapMmapFrozenBufferBytes(int64_t num_rows, uint64_t index_size_in_bytes) {
    constexpr uint64_t kBitmapFrozenAlignment = 32;
    return std::max(AlignUp(BitsetBytes(num_rows), kBitmapFrozenAlignment),
                    index_size_in_bytes);
}

uint64_t
SortLegacyAuxBytes(int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    auto rows = static_cast<uint64_t>(num_rows);
    if (rows > (std::numeric_limits<uint64_t>::max() - BitsetBytes(num_rows)) /
                   sizeof(int32_t)) {
        return std::numeric_limits<uint64_t>::max();
    }
    return rows * sizeof(int32_t) + BitsetBytes(num_rows);
}

uint64_t
MarisaLegacyCsrBytes(int64_t num_rows, uint64_t arrays_per_row) {
    if (num_rows <= 0) {
        return 0;
    }
    auto rows = static_cast<uint64_t>(num_rows);
    auto max_rows =
        (std::numeric_limits<uint64_t>::max() / sizeof(uint32_t) - 1) /
        arrays_per_row;
    if (rows > max_rows) {
        return std::numeric_limits<uint64_t>::max();
    }
    return (arrays_per_row * rows + 1) * sizeof(uint32_t);
}

std::string
FileName(const std::string& path) {
    auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}

IndexType
HybridInternalIndexTypeToIndexType(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::BITMAP:
            return BITMAP_INDEX_TYPE;
        case ScalarIndexType::STLSORT:
            return ASCENDING_SORT;
        case ScalarIndexType::MARISA:
            return MARISA_TRIE;
        case ScalarIndexType::INVERTED:
            return INVERTED_INDEX_TYPE;
        default:
            return "";
    }
}

ScalarIndexType
CheckedHybridInternalIndexType(uint64_t value) {
    if (value > std::numeric_limits<uint8_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "hybrid index type selector is out of range: {}",
                  value);
    }
    const auto type = static_cast<ScalarIndexType>(value);
    if (HybridInternalIndexTypeToIndexType(type).empty()) {
        ThrowInfo(DataFormatBroken,
                  "unsupported hybrid internal index type: {}",
                  value);
    }
    return type;
}

ScalarIndexType
ReadHybridInternalIndexType(const nlohmann::json& value) {
    if (value.is_number_unsigned()) {
        return CheckedHybridInternalIndexType(value.get<uint64_t>());
    }
    if (value.is_number_integer()) {
        const auto signed_value = value.get<int64_t>();
        if (signed_value >= 0) {
            return CheckedHybridInternalIndexType(
                static_cast<uint64_t>(signed_value));
        }
    }
    ThrowInfo(DataFormatBroken,
              "hybrid index type selector must be a non-negative integer");
}

std::optional<ScalarIndexType>
ResolveHybridInternalIndexType(
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context,
    std::optional<storage::EntryStreamLoadInfo>* stream_load_info) {
    if (stream_load_info != nullptr) {
        stream_load_info->reset();
    }
    if (index_files.empty() || !context.Valid()) {
        return std::nullopt;
    }

    storage::MemFileManagerImpl file_manager(context);
    auto type_file =
        std::find_if(index_files.begin(), index_files.end(), [](const auto& f) {
            return FileName(f) == INDEX_TYPE;
        });
    if (type_file != index_files.end()) {
        auto data = file_manager.LoadIndexToMemory(
            {*type_file}, milvus::proto::common::LoadPriority::HIGH);
        BinarySet binary_set;
        AssembleIndexDatas(data, binary_set);
        auto type = binary_set.GetByName(INDEX_TYPE);
        if (type == nullptr || type->size != sizeof(uint8_t) ||
            type->data == nullptr) {
            ThrowInfo(DataFormatBroken, "invalid hybrid index type entry");
        }
        uint8_t value;
        std::memcpy(&value, type->data.get(), sizeof(value));
        return CheckedHybridInternalIndexType(value);
    }

    if (index_files.size() == 1 && context.fs != nullptr) {
        auto input = file_manager.OpenInputStream(index_files.front());
        AssertInfo(input != nullptr,
                   "failed to open packed hybrid index file: {}",
                   index_files.front());
        auto reader = storage::IndexEntryReader::Open(
            input, input->Size(), context.fieldDataMeta.collection_id);
        if (stream_load_info != nullptr) {
            *stream_load_info = reader->GetStreamLoadInfo();
        }
        if (reader->HasMeta(INDEX_TYPE)) {
            return ReadHybridInternalIndexType(
                reader->GetMeta<nlohmann::json>(INDEX_TYPE));
        }
    }
    return std::nullopt;
}

std::optional<storage::EntryStreamLoadInfo>
InspectScalarIndexStreamLoadInfo(const std::vector<std::string>& index_files,
                                 const storage::FileManagerContext& context) {
    if (index_files.size() != 1 || !context.Valid()) {
        return std::nullopt;
    }
    storage::MemFileManagerImpl file_manager(context);
    auto input = file_manager.OpenInputStream(index_files.front());
    AssertInfo(input != nullptr,
               "failed to open packed scalar index file: {}",
               index_files.front());
    return storage::IndexEntryReader::InspectStreamLoadInfo(input,
                                                            input->Size());
}

LoadResourceRequest
ScalarIndexLoadResourceImpl(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::optional<storage::EntryStreamLoadInfo>& stream_load_info) {
    (void)index_version;
    auto config = ParseConfigFromIndexParams(index_params);
    auto type = index_params.find(INDEX_TYPE);
    AssertInfo(type != index_params.end(), "index type is empty");
    const auto& index_type = type->second;
    auto scalar_version =
        GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    auto encrypted_stream =
        scalar_version >= 3 &&
        (stream_load_info.has_value()
             ? stream_load_info->encrypted
             : storage::PluginLoader::GetInstance().getCipherPlugin() !=
                   nullptr);
    auto file_stream = index_type == INVERTED_INDEX_TYPE ||
                       index_type == NGRAM_INDEX_TYPE ||
                       index_type == RTREE_INDEX_TYPE;
    auto stream_memory_overhead =
        ScalarIndexStreamMemoryOverhead(index_size_in_bytes,
                                        scalar_version,
                                        encrypted_stream,
                                        file_stream,
                                        stream_load_info);

    LoadResourceRequest request{};
    if (index_type == ASCENDING_SORT) {
        auto aux = SortLegacyAuxBytes(num_rows);
        if (mmap_enable) {
            request.final_memory_cost = aux;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(aux, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            request.final_memory_cost = SaturatingAdd(index_size_in_bytes, aux);
            request.max_memory_cost = SaturatingAdd(request.final_memory_cost,
                                                    stream_memory_overhead);
        }
        request.has_raw_data = true;
    } else if (index_type == MARISA_TRIE || index_type == MARISA_TRIE_UPPER) {
        if (mmap_enable) {
            auto resident = MarisaLegacyCsrBytes(num_rows, 2);
            auto peak = MarisaLegacyCsrBytes(num_rows, 3);
            request.final_memory_cost = resident;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(peak, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            request.final_memory_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        }
        request.has_raw_data = true;
    } else if (index_type == INVERTED_INDEX_TYPE ||
               index_type == NGRAM_INDEX_TYPE ||
               index_type == RTREE_INDEX_TYPE) {
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = stream_memory_overhead;
        request.max_disk_cost = index_size_in_bytes;
    } else if (index_type == FMINDEX_INDEX_TYPE) {
        if (mmap_enable) {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            request.final_memory_cost = SaturatingMul(2, index_size_in_bytes);
            request.max_memory_cost = request.final_memory_cost;
        }
    } else if (index_type == BITMAP_INDEX_TYPE) {
        if (mmap_enable) {
            auto resident = BitsetBytes(num_rows);
            auto frozen =
                BitmapMmapFrozenBufferBytes(num_rows, index_size_in_bytes);
            request.final_memory_cost = resident;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = SaturatingAdd(
                SaturatingAdd(resident, stream_memory_overhead), frozen);
            request.max_disk_cost = SaturatingMul(2, index_size_in_bytes);
        } else {
            request.final_memory_cost = index_size_in_bytes;
            request.max_memory_cost = std::max(
                SaturatingMul(2, index_size_in_bytes),
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead));
        }
    } else if (index_type == HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = SaturatingMul(2, index_size_in_bytes);
        request.max_disk_cost = index_size_in_bytes;
    } else {
        LOG_ERROR(
            "invalid index type to estimate scalar index load resource: {}",
            index_type);
        return LoadResourceRequest{0, 0, 0, 0, false};
    }
    request.has_raw_data =
        CanUseIndexRawDataForField(field_type, request.has_raw_data);
    return request;
}

}  // namespace

bool
CanUseIndexRawDataForField(DataType field_type, bool has_raw_data) {
    return has_raw_data && field_type != DataType::ARRAY &&
           field_type != DataType::VECTOR_ARRAY && field_type != DataType::JSON;
}

LoadResourceRequest
IndexLoadResource(DataType field_type,
                  DataType element_type,
                  IndexVersion index_version,
                  uint64_t index_size_in_bytes,
                  const std::map<std::string, std::string>& index_params,
                  bool mmap_enable,
                  int64_t num_rows,
                  int64_t dim) {
    return IsVectorDataType(field_type)
               ? VecIndexLoadResource(field_type,
                                      element_type,
                                      index_version,
                                      index_size_in_bytes,
                                      index_params,
                                      mmap_enable,
                                      num_rows,
                                      dim)
               : ScalarIndexLoadResource(field_type,
                                         index_version,
                                         index_size_in_bytes,
                                         index_params,
                                         mmap_enable,
                                         num_rows);
}

LoadResourceRequest
IndexLoadResource(DataType field_type,
                  DataType element_type,
                  IndexVersion index_version,
                  uint64_t index_size_in_bytes,
                  const std::map<std::string, std::string>& index_params,
                  bool mmap_enable,
                  int64_t num_rows,
                  int64_t dim,
                  const std::vector<std::string>& index_files,
                  const storage::FileManagerContext& context,
                  std::optional<storage::EntryStreamLoadInfo>* stream_load_info,
                  bool* use_shared_memory_overhead_group) {
    if (stream_load_info != nullptr) {
        stream_load_info->reset();
    }
    if (use_shared_memory_overhead_group != nullptr) {
        *use_shared_memory_overhead_group = false;
    }
    if (IsVectorDataType(field_type)) {
        return VecIndexLoadResource(field_type,
                                    element_type,
                                    index_version,
                                    index_size_in_bytes,
                                    index_params,
                                    mmap_enable,
                                    num_rows,
                                    dim);
    }
    return ScalarIndexLoadResource(field_type,
                                   index_version,
                                   index_size_in_bytes,
                                   index_params,
                                   mmap_enable,
                                   num_rows,
                                   index_files,
                                   context,
                                   stream_load_info,
                                   use_shared_memory_overhead_group);
}

LoadResourceRequest
ScalarIndexLoadResource(DataType field_type,
                        IndexVersion index_version,
                        uint64_t index_size_in_bytes,
                        const std::map<std::string, std::string>& index_params,
                        bool mmap_enable,
                        int64_t num_rows) {
    return ScalarIndexLoadResourceImpl(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       index_params,
                                       mmap_enable,
                                       num_rows,
                                       std::nullopt);
}

LoadResourceRequest
ScalarIndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context,
    std::optional<storage::EntryStreamLoadInfo>* stream_load_info,
    bool* use_shared_memory_overhead_group) {
    auto type = index_params.find(INDEX_TYPE);
    AssertInfo(type != index_params.end(), "index type is empty");
    auto config = ParseConfigFromIndexParams(index_params);
    auto scalar_version =
        GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);

    std::optional<storage::EntryStreamLoadInfo> inspected;
    std::optional<ScalarIndexType> internal_type;
    if (type->second == HYBRID_INDEX_TYPE) {
        try {
            internal_type = ResolveHybridInternalIndexType(
                index_files, context, &inspected);
        } catch (const std::exception& error) {
            if (scalar_version >= 3 && !inspected.has_value()) {
                inspected =
                    InspectScalarIndexStreamLoadInfo(index_files, context);
            }
            LOG_WARN(
                "failed to resolve hybrid scalar internal index type, "
                "fallback to hybrid estimate: {}",
                error.what());
        }
    } else if (scalar_version >= 3) {
        inspected = InspectScalarIndexStreamLoadInfo(index_files, context);
    }
    if (stream_load_info != nullptr) {
        *stream_load_info = inspected;
    }

    auto resolved_params = index_params;
    if (internal_type.has_value()) {
        auto resolved = HybridInternalIndexTypeToIndexType(*internal_type);
        if (!resolved.empty()) {
            resolved_params[INDEX_TYPE] = resolved;
            LOG_INFO(
                "estimate hybrid scalar index load resource by internal "
                "index type: {}",
                resolved);
        }
    }

    const auto& resolved_type = resolved_params.at(INDEX_TYPE);
    auto shared_group = scalar_version >= 3 &&
                        resolved_type != BITMAP_INDEX_TYPE &&
                        resolved_type != HYBRID_INDEX_TYPE;
    if (use_shared_memory_overhead_group != nullptr) {
        *use_shared_memory_overhead_group = shared_group;
    }
    return ScalarIndexLoadResourceImpl(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       resolved_params,
                                       mmap_enable,
                                       num_rows,
                                       inspected);
}

}  // namespace milvus::index
