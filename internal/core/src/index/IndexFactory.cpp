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

#include "index/IndexFactory.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "index/Index.h"
#include "index/JsonFlatIndex.h"
#include "index/VectorMemIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "index/JsonInvertedIndex.h"
#include "index/NgramInvertedIndex.h"
#include "knowhere/utils.h"

#include "index/VectorDiskIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/HybridScalarIndex.h"
#include "index/RTreeIndex.h"
#include "knowhere/comp/knowhere_check.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "storage/IndexEntryReader.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"

namespace milvus::index {
namespace {

uint64_t
BitsetBytes(int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    return (static_cast<uint64_t>(num_rows) + 7) / 8;
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
    const auto rows = static_cast<uint64_t>(num_rows);
    return SaturatingAdd(SaturatingMultiply(rows, sizeof(int32_t)),
                         BitsetBytes(num_rows));
}

uint64_t
MarisaLegacyCsrBytes(int64_t num_rows, uint64_t arrays_per_row) {
    if (num_rows <= 0) {
        return 0;
    }
    const auto rows = static_cast<uint64_t>(num_rows);
    return SaturatingMultiply(
        SaturatingAdd(SaturatingMultiply(arrays_per_row, rows), uint64_t{1}),
        sizeof(uint32_t));
}

std::string
GetFileName(const std::string& path) {
    const auto pos = path.find_last_of('/');
    return pos == std::string::npos ? path : path.substr(pos + 1);
}

proto::common::LoadPriority
ResolveLoadPriority(const std::map<std::string, std::string>& index_params) {
    // Missing or unrecognized values use the higher-concurrency pool so the
    // loading-memory estimate remains conservative.
    const auto it = index_params.find(LOAD_PRIORITY);
    return it == index_params.end() ? proto::common::LoadPriority::HIGH
                                    : PriorityForLoad(it->second);
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

std::optional<ScalarIndexType>
ResolveHybridInternalIndexType(
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& file_manager_context,
    proto::common::LoadPriority load_priority) {
    if (index_files.empty() || !file_manager_context.Valid()) {
        return std::nullopt;
    }

    storage::MemFileManagerImpl file_manager(file_manager_context);
    auto type_file =
        std::find_if(index_files.begin(), index_files.end(), [](const auto& f) {
            return GetFileName(f) == INDEX_TYPE;
        });
    if (type_file != index_files.end()) {
        auto index_datas = file_manager.LoadIndexToMemory(
            std::vector<std::string>{*type_file}, load_priority);
        BinarySet binary_set;
        AssembleIndexDatas(index_datas, binary_set);
        auto type_buffer = binary_set.GetByName(INDEX_TYPE);
        AssertInfo(
            type_buffer != nullptr && type_buffer->size >= sizeof(uint8_t),
            "index type file not found in hybrid index binary set");
        uint8_t type = 0;
        std::memcpy(&type, type_buffer->data.get(), sizeof(type));
        return static_cast<ScalarIndexType>(type);
    }

    if (index_files.size() == 1 && file_manager_context.fs != nullptr) {
        auto input = file_manager.OpenInputStream(index_files.front());
        AssertInfo(input != nullptr,
                   "failed to open packed hybrid index file: {}",
                   index_files.front());
        auto reader =
            storage::IndexEntryReader::Open(input,
                                            input->Size(),
                                            /*collection_id=*/0,
                                            PriorityForLoad(load_priority));
        AssertInfo(reader != nullptr,
                   "failed to create reader for packed hybrid index file");
        if (reader->HasMeta(INDEX_TYPE)) {
            return static_cast<ScalarIndexType>(
                reader->GetMeta<uint8_t>(INDEX_TYPE));
        }
    }
    return std::nullopt;
}

}  // namespace

namespace {

uint64_t
ValidityBitmapBytes(int64_t num_rows) {
    return AlignUp(BitsetBytes(num_rows), sizeof(uint64_t));
}

}  // namespace

bool
IndexFactory::CanUseIndexRawDataForField(DataType field_type,
                                         bool has_raw_data) {
    // A JSON path index only retains values extracted from one path. It
    // cannot reconstruct the complete JSON field for output or raw-data
    // fallback on another path.
    return has_raw_data && field_type != DataType::JSON;
}

template <typename T>
ScalarIndexPtr<T>
IndexFactory::CreatePrimitiveScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    if (index_type == INVERTED_INDEX_TYPE) {
        assert(create_index_info.tantivy_index_version != 0);
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<T>>(
            create_index_info.tantivy_index_version,
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<T>>(file_manager_context);
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<T>>(
            create_index_info.tantivy_index_version, file_manager_context);
    }
    return CreateScalarIndexSort<T>(file_manager_context);
}

template <>
ScalarIndexPtr<std::string>
IndexFactory::CreatePrimitiveScalarIndex<std::string>(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
#if defined(__linux__) || defined(__APPLE__)
    if (index_type == INVERTED_INDEX_TYPE) {
        assert(create_index_info.tantivy_index_version != 0);
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<std::string>>(
            create_index_info.tantivy_index_version,
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<std::string>>(file_manager_context);
    } else if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<std::string>>(
            create_index_info.tantivy_index_version, file_manager_context);
    } else if (index_type == MARISA_TRIE || index_type == MARISA_TRIE_UPPER) {
        return CreateStringIndexMarisa(file_manager_context);
    } else if (index_type == ASCENDING_SORT) {
        return CreateStringIndexSort(file_manager_context);
    } else {
        ThrowInfo(Unsupported, "unsupported index type: {}", index_type);
    }
#else
    ThrowInfo(Unsupported, "unsupported platform");
#endif
}

LoadResourceRequest
IndexFactory::IndexLoadResource(
    DataType field_type,
    DataType element_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    int64_t dim) {
    if (milvus::IsVectorDataType(field_type)) {
        return VecIndexLoadResource(field_type,
                                    element_type,
                                    index_version,
                                    index_size_in_bytes,
                                    index_params,
                                    mmap_enable,
                                    num_rows,
                                    dim);
    } else {
        return ScalarIndexLoadResource(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       index_params,
                                       mmap_enable,
                                       num_rows);
    }
}

LoadResourceRequest
IndexFactory::IndexLoadResource(
    DataType field_type,
    DataType element_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    int64_t dim,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& file_manager_context) {
    if (milvus::IsVectorDataType(field_type)) {
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
                                   file_manager_context);
}

LoadResourceRequest
IndexFactory::VecIndexLoadResource(
    DataType field_type,
    DataType element_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    int64_t dim) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    auto index_type_it = index_params.find("index_type");
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    const std::string& index_type = index_type_it->second;

    bool mmaped = false;
    if (mmap_enable &&
        knowhere::KnowhereCheck::SupportMmapIndexTypeCheck(index_type)) {
        config["enable_mmap"] = true;
        mmaped = true;
    }

    knowhere::expected<knowhere::Resource> resource;
    uint64_t download_buffer_size_in_bytes = DEFAULT_FIELD_MAX_MEMORY_LIMIT;

    bool has_raw_data = false;
    switch (field_type) {
        case milvus::DataType::VECTOR_BINARY:
            resource = knowhere::IndexStaticFaced<
                knowhere::bin1>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bin1>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp32>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_BFLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::bf16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bf16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            resource = knowhere::IndexStaticFaced<knowhere::sparse_u32_f32>::
                EstimateLoadResource(index_type,
                                     index_version,
                                     index_size_in_bytes,
                                     num_rows,
                                     dim,
                                     config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_INT8:
            resource = knowhere::IndexStaticFaced<
                knowhere::int8>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::int8>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_ARRAY: {
            switch (element_type) {
                case milvus::DataType::VECTOR_FLOAT:
                    resource = knowhere::IndexStaticFaced<knowhere::fp32>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_FLOAT16:
                    resource = knowhere::IndexStaticFaced<knowhere::fp16>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_BFLOAT16:
                    resource = knowhere::IndexStaticFaced<knowhere::bf16>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_BINARY:
                    resource = knowhere::IndexStaticFaced<knowhere::bin1>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_INT8:
                    resource = knowhere::IndexStaticFaced<knowhere::int8>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;

                default:
                    LOG_ERROR(
                        "invalid data type to estimate index load resource: "
                        "field_type {}, element_type {}",
                        field_type,
                        element_type);
                    return LoadResourceRequest{0, 0, 0, 0, true};
            }
            // For VectorArray, has_raw_data is always false as get_vector of index does not provide offsets which
            // is required for reconstructing the raw data
            has_raw_data = false;
            break;
        }
        default:
            LOG_ERROR("invalid data type to estimate index load resource: {}",
                      field_type);
            return LoadResourceRequest{0, 0, 0, 0, true};
    }

    LoadResourceRequest request{};
    const auto& res = resource.value();

    request.has_raw_data = has_raw_data;
    request.final_disk_cost = res.diskCost;
    request.final_memory_cost = res.memoryCost;
    if (knowhere::UseDiskLoad(index_type, index_version) || mmaped) {
        request.max_disk_cost = res.diskCost;
        request.max_memory_cost =
            std::max(res.memoryCost, download_buffer_size_in_bytes);
    } else {
        request.max_disk_cost = 0;
        request.max_memory_cost = 2 * res.memoryCost;
    }
    return request;
}

LoadResourceRequest
IndexFactory::ScalarIndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    auto index_type_it = index_params.find("index_type");
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    const std::string& index_type = index_type_it->second;
    const auto scalar_version =
        GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    const auto load_priority = ResolveLoadPriority(index_params);

    knowhere::expected<knowhere::Resource> resource;

    LoadResourceRequest request{};
    request.has_raw_data = false;

    if (index_type == milvus::index::ASCENDING_SORT) {
        if (scalar_version >= 3) {
            const auto stream_memory_overhead = ScalarIndexStreamMemoryOverhead(
                index_size_in_bytes, scalar_version, load_priority);
            const auto legacy_aux_bytes = SortLegacyAuxBytes(num_rows);
            if (mmap_enable) {
                request.final_memory_cost = legacy_aux_bytes;
                request.final_disk_cost = index_size_in_bytes;
                request.max_memory_cost =
                    SaturatingAdd(legacy_aux_bytes, stream_memory_overhead);
                request.max_disk_cost = index_size_in_bytes;
            } else {
                request.final_memory_cost =
                    SaturatingAdd(index_size_in_bytes, legacy_aux_bytes);
                request.final_disk_cost = 0;
                request.max_memory_cost = SaturatingAdd(
                    request.final_memory_cost, stream_memory_overhead);
                request.max_disk_cost = 0;
            }
        } else {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost =
                SaturatingMultiply(uint64_t{2}, index_size_in_bytes);
            request.max_disk_cost = 0;
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::MARISA_TRIE ||
               index_type == milvus::index::MARISA_TRIE_UPPER) {
        if (scalar_version >= 3) {
            const auto stream_memory_overhead = ScalarIndexStreamMemoryOverhead(
                index_size_in_bytes, scalar_version, load_priority);
            if (mmap_enable) {
                const auto legacy_csr_resident_bytes =
                    MarisaLegacyCsrBytes(num_rows, 2);
                const auto legacy_csr_peak_bytes =
                    MarisaLegacyCsrBytes(num_rows, 3);
                request.final_memory_cost = legacy_csr_resident_bytes;
                request.final_disk_cost = index_size_in_bytes;
                request.max_memory_cost = SaturatingAdd(legacy_csr_peak_bytes,
                                                        stream_memory_overhead);
                request.max_disk_cost = index_size_in_bytes;
            } else {
                request.final_memory_cost = index_size_in_bytes;
                request.final_disk_cost = 0;
                request.max_memory_cost =
                    SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
                request.max_disk_cost = index_size_in_bytes;
            }
        } else {
            if (mmap_enable) {
                request.final_memory_cost = 0;
                request.final_disk_cost = index_size_in_bytes;
                request.max_memory_cost = index_size_in_bytes;
                request.max_disk_cost = index_size_in_bytes;
            } else {
                request.final_memory_cost = index_size_in_bytes;
                request.final_disk_cost = 0;
                request.max_memory_cost =
                    SaturatingMultiply(uint64_t{2}, index_size_in_bytes);
                request.max_disk_cost = index_size_in_bytes;
            }
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::INVERTED_INDEX_TYPE ||
               index_type == milvus::index::NGRAM_INDEX_TYPE) {
        const auto validity_bitmap_bytes = ValidityBitmapBytes(num_rows);
        const auto stream_memory_overhead =
            scalar_version >= 3
                ? ScalarIndexStreamMemoryOverhead(
                      index_size_in_bytes, scalar_version, load_priority)
                : index_size_in_bytes;
        if (mmap_enable) {
            request.final_memory_cost = validity_bitmap_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(stream_memory_overhead, validity_bitmap_bytes);
        } else {
            const auto resident_bytes =
                SaturatingAdd(index_size_in_bytes, validity_bitmap_bytes);
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost =
                std::max(resident_bytes, stream_memory_overhead);
        }
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else if (index_type == milvus::index::RTREE_INDEX_TYPE) {
        request.final_memory_cost = 0;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = index_size_in_bytes;
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else if (index_type == milvus::index::BITMAP_INDEX_TYPE) {
        if (scalar_version >= 3) {
            const auto stream_memory_overhead = ScalarIndexStreamMemoryOverhead(
                index_size_in_bytes, scalar_version, load_priority);
            if (mmap_enable) {
                // TODO: Once bitmap cardinality is exposed to admission
                // estimation, account for the BITSET load mode's per-value
                // resident bitsets here.
                const auto resident_bytes = BitsetBytes(num_rows);
                const auto frozen_buffer_bytes =
                    BitmapMmapFrozenBufferBytes(num_rows, index_size_in_bytes);
                request.final_memory_cost = resident_bytes;
                request.final_disk_cost = index_size_in_bytes;
                request.max_memory_cost = SaturatingAdd(
                    SaturatingAdd(resident_bytes, stream_memory_overhead),
                    frozen_buffer_bytes);
                request.max_disk_cost =
                    SaturatingMultiply(uint64_t{2}, index_size_in_bytes);
            } else {
                request.final_memory_cost = index_size_in_bytes;
                request.final_disk_cost = 0;
                request.max_memory_cost = std::max(
                    SaturatingMultiply(uint64_t{2}, index_size_in_bytes),
                    SaturatingAdd(index_size_in_bytes, stream_memory_overhead));
                request.max_disk_cost = 0;
            }
        } else {
            if (mmap_enable) {
                request.final_memory_cost = 0;
                request.final_disk_cost = index_size_in_bytes;
                request.max_memory_cost = index_size_in_bytes;
                request.max_disk_cost = index_size_in_bytes;
            } else {
                request.final_memory_cost = index_size_in_bytes;
                request.final_disk_cost = 0;
                request.max_memory_cost =
                    SaturatingMultiply(uint64_t{2}, index_size_in_bytes);
                request.max_disk_cost = 0;
            }
        }

        request.has_raw_data = false;
    } else if (index_type == milvus::index::HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = 2 * index_size_in_bytes;
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
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

LoadResourceRequest
IndexFactory::ScalarIndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& file_manager_context) {
    const auto index_type_it = index_params.find(INDEX_TYPE);
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    if (index_type_it->second != HYBRID_INDEX_TYPE) {
        return ScalarIndexLoadResource(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       index_params,
                                       mmap_enable,
                                       num_rows);
    }

    try {
        const auto load_priority = ResolveLoadPriority(index_params);
        const auto internal_index_type = ResolveHybridInternalIndexType(
            index_files, file_manager_context, load_priority);
        if (internal_index_type.has_value()) {
            const auto resolved_index_type =
                HybridInternalIndexTypeToIndexType(internal_index_type.value());
            if (!resolved_index_type.empty()) {
                auto resolved_params = index_params;
                resolved_params[INDEX_TYPE] = resolved_index_type;
                LOG_INFO(
                    "estimate hybrid scalar index load resource by internal "
                    "index type: {}",
                    resolved_index_type);
                return ScalarIndexLoadResource(field_type,
                                               index_version,
                                               index_size_in_bytes,
                                               resolved_params,
                                               mmap_enable,
                                               num_rows);
            }
        }
    } catch (const std::exception& e) {
        LOG_WARN(
            "failed to resolve hybrid scalar internal index type, fallback "
            "to hybrid estimate: {}",
            e.what());
    }

    return ScalarIndexLoadResource(field_type,
                                   index_version,
                                   index_size_in_bytes,
                                   index_params,
                                   mmap_enable,
                                   num_rows);
}

IndexBasePtr
IndexFactory::CreateIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    bool use_build_pool) {
    if (IsVectorDataType(create_index_info.field_type)) {
        return CreateVectorIndex(
            create_index_info, file_manager_context, use_build_pool);
    }

    return CreateScalarIndex(create_index_info, file_manager_context);
}

IndexBasePtr
IndexFactory::CreatePrimitiveScalarIndex(
    DataType data_type,
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    switch (data_type) {
        // create scalar index
        case DataType::BOOL:
            return CreatePrimitiveScalarIndex<bool>(create_index_info,
                                                    file_manager_context);
        case DataType::INT8:
            return CreatePrimitiveScalarIndex<int8_t>(create_index_info,
                                                      file_manager_context);
        case DataType::INT16:
            return CreatePrimitiveScalarIndex<int16_t>(create_index_info,
                                                       file_manager_context);
        case DataType::INT32:
            return CreatePrimitiveScalarIndex<int32_t>(create_index_info,
                                                       file_manager_context);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return CreatePrimitiveScalarIndex<int64_t>(create_index_info,
                                                       file_manager_context);
        case DataType::FLOAT:
            return CreatePrimitiveScalarIndex<float>(create_index_info,
                                                     file_manager_context);
        case DataType::DOUBLE:
            return CreatePrimitiveScalarIndex<double>(create_index_info,
                                                      file_manager_context);

            // create string index
        case DataType::STRING:
        case DataType::VARCHAR: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value());
            }
            return CreatePrimitiveScalarIndex<std::string>(
                create_index_info, file_manager_context);
        }
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("invalid data type to build index: {}", data_type));
    }
}

IndexBasePtr
IndexFactory::CreateCompositeScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    if (index_type == HYBRID_INDEX_TYPE || index_type == BITMAP_INDEX_TYPE ||
        index_type == INVERTED_INDEX_TYPE) {
        auto element_type = static_cast<DataType>(
            file_manager_context.fieldDataMeta.field_schema.element_type());
        return CreatePrimitiveScalarIndex(
            element_type, create_index_info, file_manager_context);
    } else {
        ThrowInfo(
            Unsupported,
            fmt::format("index type: {} for composite scalar not supported now",
                        index_type));
    }
}

IndexBasePtr
IndexFactory::CreateComplexScalarIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    ThrowInfo(Unsupported, "Complex index not supported now");
}

IndexBasePtr
IndexFactory::CreateJsonIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    AssertInfo(create_index_info.index_type == INVERTED_INDEX_TYPE ||
                   create_index_info.index_type == NGRAM_INDEX_TYPE,
               "Invalid index type for json index");

    const auto& cast_dtype = create_index_info.json_cast_type;
    const auto& nested_path = create_index_info.json_path;
    const auto& json_cast_function = create_index_info.json_cast_function;
    switch (cast_dtype.element_type()) {
        case JsonCastType::DataType::BOOL:
            return std::make_unique<index::JsonInvertedIndex<bool>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        case JsonCastType::DataType::DOUBLE:
            return std::make_unique<index::JsonInvertedIndex<double>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        case JsonCastType::DataType::VARCHAR: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value(), nested_path);
            }
            return std::make_unique<index::JsonInvertedIndex<std::string>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        }
        case JsonCastType::DataType::JSON:
            return std::make_unique<JsonFlatIndex>(
                file_manager_context,
                nested_path,
                create_index_info.tantivy_index_version);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", cast_dtype);
    }
}

IndexBasePtr
IndexFactory::CreateGeometryIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    AssertInfo(index_type == RTREE_INDEX_TYPE,
               "Invalid index type for geometry index");
    return std::make_unique<RTreeIndex<std::string>>(file_manager_context);
}

IndexBasePtr
IndexFactory::CreateScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto data_type = create_index_info.field_type;
    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TIMESTAMPTZ:
            return CreatePrimitiveScalarIndex(
                data_type, create_index_info, file_manager_context);
        case DataType::ARRAY: {
            return CreateCompositeScalarIndex(create_index_info,
                                              file_manager_context);
        }
        case DataType::JSON: {
            return CreateJsonIndex(create_index_info, file_manager_context);
        }
        case DataType::GEOMETRY: {
            return CreateGeometryIndex(create_index_info.index_type,
                                       file_manager_context);
        }
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", data_type);
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    bool use_knowhere_build_pool) {
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;
    auto version = create_index_info.index_engine_version;
    // create disk index
    auto data_type = create_index_info.field_type;
    if (knowhere::UseDiskLoad(index_type, version)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<float16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorDiskAnnIndex<bin1>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_SPARSE_U32_F32: {
                return std::make_unique<VectorDiskAnnIndex<sparse_u32_f32>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_ARRAY: {
                auto element_type =
                    static_cast<DataType>(file_manager_context.fieldDataMeta
                                              .field_schema.element_type());
                switch (element_type) {
                    case DataType::VECTOR_FLOAT:
                        return std::make_unique<VectorDiskAnnIndex<float>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_FLOAT16:
                        return std::make_unique<VectorDiskAnnIndex<float16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_BFLOAT16:
                        return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_BINARY:
                        return std::make_unique<VectorDiskAnnIndex<bin1>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_INT8:
                        return std::make_unique<VectorDiskAnnIndex<int8>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    default:
                        ThrowInfo(NotImplemented,
                                  fmt::format("not implemented data type to "
                                              "build disk index: {}",
                                              element_type));
                }
            }
            case DataType::VECTOR_INT8: {
                return std::make_unique<VectorDiskAnnIndex<int8>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            default:
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build disk index: {}",
                                data_type));
        }
    } else {  // create mem index
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorMemIndex<float>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_SPARSE_U32_F32: {
                return std::make_unique<VectorMemIndex<sparse_u32_f32>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorMemIndex<bin1>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorMemIndex<float16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorMemIndex<bfloat16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_INT8: {
                return std::make_unique<VectorMemIndex<int8>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_ARRAY: {
                auto element_type =
                    static_cast<DataType>(file_manager_context.fieldDataMeta
                                              .field_schema.element_type());
                switch (element_type) {
                    case DataType::VECTOR_FLOAT:
                        return std::make_unique<VectorMemIndex<float>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    case DataType::VECTOR_FLOAT16: {
                        return std::make_unique<VectorMemIndex<float16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_BFLOAT16: {
                        return std::make_unique<VectorMemIndex<bfloat16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_BINARY: {
                        return std::make_unique<VectorMemIndex<bin1>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_INT8: {
                        return std::make_unique<VectorMemIndex<int8>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    default:
                        ThrowInfo(NotImplemented,
                                  fmt::format("not implemented data type to "
                                              "build mem index: {}",
                                              element_type));
                }
            }
            default:
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build mem index: {}",
                                data_type));
        }
    }
}
}  // namespace milvus::index
