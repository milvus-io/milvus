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
#include "folly/coro/BlockingWait.h"
#include "storage/AsyncIndexEntryReader.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"
#include "storage/AsyncLoadExecutor.h"
#include "storage/LegacyIndexLoader.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "storage/FileWriter.h"
#include <yaml-cpp/yaml.h>

#include <assert.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/BitmapIndex.h"
#include "index/HybridScalarIndex.h"
#include "index/Index.h"
#include "index/IndexInfo.h"
#include "index/InvertedIndexTantivy.h"
#include "index/TextMatchIndex.h"
#include "index/JsonFlatIndex.h"
#include "index/JsonHybridScalarIndex.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/FMIndex.h"
#include "index/Meta.h"
#include "index/NgramInvertedIndex.h"
#include "index/RTreeIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/StringIndexSort.h"
#include "index/Utils.h"
#include "index/VectorDiskIndex.h"
#include "index/VectorMemIndex.h"
#include "index/VectorIndexValidDataUtils.h"
#include "knowhere/comp/knowhere_check.h"
#include "knowhere/emb_list_utils.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_static.h"
#include "knowhere/operands.h"
#include "knowhere/utils.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/schema.pb.h"
#include "storage/EntryStreamUtils.h"
#include "storage/IndexEntryReader.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/PluginLoader.h"
#include "storage/Types.h"
#include "storage/LoadAdmissionController.h"

namespace milvus::index {

namespace {

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
        total_transient_bytes = milvus::storage::EntryStreamTransientBytes(
            index_size_in_bytes, encrypted);
        max_task_transient_bytes = milvus::storage::EntryStreamTransientBytes(
            milvus::storage::MaxEntryStreamTaskBytes(), encrypted);

        // Without a concrete encrypted directory there is no trustworthy
        // ciphertext task bound. When the runtime budget is disabled, keep
        // the conservative whole-stream fallback instead of applying the
        // executor bound.
        if (encrypted && milvus::storage::LoadAdmissionController::GetInstance()
                                 .CapacityBytes() == 0) {
            return total_transient_bytes;
        }
    }

    if (file_stream && !encrypted) {
        total_transient_bytes = milvus::SaturatingMultiply(
            total_transient_bytes,
            milvus::storage::kFileStreamBufferMultiplier);
        max_task_transient_bytes = milvus::SaturatingMultiply(
            max_task_transient_bytes,
            milvus::storage::kFileStreamBufferMultiplier);
    }

    // File-aware estimates must remain valid when a later reload observes a
    // larger transient budget or executor. Grouped loads are capped by the
    // current Group policy; request-local loads reserve this stable full sum.
    if (stream_load_info.has_value()) {
        return total_transient_bytes;
    }

    return milvus::storage::EntryStreamMaxTransientBytes(
        total_transient_bytes, max_task_transient_bytes);
}

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
ValidityBitmapBytes(int64_t num_rows) {
    return AlignUp(BitsetBytes(num_rows), sizeof(uint64_t));
}

uint64_t
BitmapMmapFrozenBufferBytes(int64_t num_rows, uint64_t index_size_in_bytes) {
    constexpr uint64_t kBitmapFrozenAlignment = 32;
    auto dense_bitmap_bytes =
        AlignUp(BitsetBytes(num_rows), kBitmapFrozenAlignment);
    return std::max(dense_bitmap_bytes, index_size_in_bytes);
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
IdMapMmapDiskCost(const Config& config, int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }

    const auto enable_mmap_o2i =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_O2I_MAP).value_or(false);
    const auto enable_mmap_i2o =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_I2O_MAP).value_or(false);
    if (!enable_mmap_o2i && !enable_mmap_i2o) {
        return 0;
    }

    return milvus::SaturatingMultiply(static_cast<uint64_t>(num_rows),
                                      uint64_t{sizeof(int32_t)});
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

    // Fallback CSR has csr_offsets_ <= num_rows and csr_index_ <= num_rows + 1.
    // During rebuild, fill_offsets also holds a temporary write_pos copy.
    return (arrays_per_row * rows + 1) * sizeof(uint32_t);
}

std::string
GetIndexFileBaseName(const std::string& path) {
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

std::optional<ScalarIndexType>
ResolveHybridInternalIndexType(
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& file_manager_context,
    std::optional<storage::EntryStreamLoadInfo>* stream_load_info = nullptr) {
    if (stream_load_info != nullptr) {
        stream_load_info->reset();
    }
    if (index_files.empty() || !file_manager_context.Valid()) {
        return std::nullopt;
    }

    auto load_priority = milvus::proto::common::LoadPriority::HIGH;
    storage::MemFileManagerImpl file_manager(file_manager_context);

    auto index_type_file =
        std::find_if(index_files.begin(), index_files.end(), [](const auto& f) {
            return GetIndexFileBaseName(f) == INDEX_TYPE;
        });
    if (index_type_file != index_files.end()) {
        auto index_datas = file_manager.LoadIndexToMemory(
            std::vector<std::string>{*index_type_file}, load_priority);
        BinarySet binary_set;
        AssembleIndexDatas(index_datas, binary_set);

        auto index_type_buffer = binary_set.GetByName(INDEX_TYPE);
        AssertInfo(index_type_buffer != nullptr,
                   "index type file not found in hybrid index binary set");
        uint8_t index_type;
        memcpy(&index_type, index_type_buffer->data.get(), sizeof(uint8_t));
        return static_cast<ScalarIndexType>(index_type);
    }

    if (index_files.size() == 1 && file_manager_context.fs != nullptr) {
        auto input = file_manager.OpenInputStream(index_files[0]);
        AssertInfo(input != nullptr,
                   "failed to open packed hybrid index file: {}",
                   index_files[0]);
        auto reader = storage::IndexEntryReader::Open(
            input,
            input->Size(),
            file_manager_context.fieldDataMeta.collection_id);
        AssertInfo(reader != nullptr,
                   "failed to create IndexEntryReader for hybrid index file");
        if (stream_load_info != nullptr) {
            *stream_load_info = reader->GetStreamLoadInfo();
        }
        if (reader->HasMeta(INDEX_TYPE)) {
            return static_cast<ScalarIndexType>(
                reader->GetMeta<uint8_t>(INDEX_TYPE));
        }
    }

    return std::nullopt;
}

std::optional<storage::EntryStreamLoadInfo>
InspectScalarIndexStreamLoadInfo(
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& file_manager_context) {
    if (index_files.size() != 1 || !file_manager_context.Valid()) {
        return std::nullopt;
    }

    storage::MemFileManagerImpl file_manager(file_manager_context);
    auto input = file_manager.OpenInputStream(index_files[0]);
    AssertInfo(input != nullptr,
               "failed to open packed scalar index file: {}",
               index_files[0]);
    return storage::IndexEntryReader::InspectStreamLoadInfo(input,
                                                            input->Size());
}

}  // namespace

bool
IndexFactory::CanUseIndexRawDataForField(DataType field_type,
                                         bool has_raw_data) {
    // ARRAY and JSON indexes only index a projection of the value (array
    // elements / a JSON path) and cannot reconstruct the whole raw value, so
    // their index is never a stand-in for the raw column. This mirrors the
    // segment runtime contract (HasRawDataFromState returns column-based
    // field_data_ready for JSON), so no loader path treats a JSON index as
    // raw-serving and skips its raw column. VECTOR_ARRAY indexes may carry raw
    // vector payloads, but the field column is still needed for struct offsets
    // and parent-row validity.
    return has_raw_data && field_type != DataType::ARRAY &&
           field_type != DataType::VECTOR_ARRAY && field_type != DataType::JSON;
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
        if (create_index_info.is_text_match) {
            auto field_schema = FieldMeta::ParseFrom(
                file_manager_context.fieldDataMeta.field_schema);
            return std::make_unique<TextMatchIndex>(
                file_manager_context,
                create_index_info.tantivy_index_version,
                "milvus_tokenizer",
                field_schema.get_analyzer_params().c_str(),
                create_index_info.analyzer_extra_info.c_str());
        }
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
    const storage::FileManagerContext& file_manager_context,
    std::optional<storage::EntryStreamLoadInfo>* stream_load_info,
    bool* use_shared_memory_overhead_group) {
    if (stream_load_info != nullptr) {
        stream_load_info->reset();
    }
    if (use_shared_memory_overhead_group != nullptr) {
        *use_shared_memory_overhead_group = false;
    }
    if (milvus::IsVectorDataType(field_type)) {
        auto request = VecIndexLoadResource(field_type,
                                            element_type,
                                            index_version,
                                            index_size_in_bytes,
                                            index_params,
                                            mmap_enable,
                                            num_rows,
                                            dim);
        const auto& type = index_params.at(INDEX_TYPE);
        if (index_files.empty() || !file_manager_context.Valid() ||
            knowhere::UseDiskLoad(type, index_version)) {
            return request;
        }
        const bool mmaped =
            mmap_enable &&
            knowhere::KnowhereCheck::SupportMmapIndexTypeCheck(type);
        // Estimate both modes: the switch may change before a cached reload.
        // Memory loads retain the BinarySet; mmap retains only sidecars. Writer
        // buffers and retained metadata are request-owned, outside slice leases.
        auto inspect = [&]() -> folly::coro::Task<uint64_t> {
            uint64_t retained = 0;
            uint64_t scratch = 0;
            for (const auto& file : index_files) {
                auto input = storage::OpenLegacyIndexInput(
                    file_manager_context.chunkManagerPtr,
                    file_manager_context.fs,
                    file);
                const auto info = co_await storage::InspectLegacyIndexFileAsync(
                    *input, proto::common::LoadPriority::HIGH);
                const auto name = GetIndexFileBaseName(file);
                const bool sidecar =
                    name.starts_with(VALID_DATA_KEY) ||
                    name.starts_with(EMPTY_EMB_LIST_OFFSET_KEY) ||
                    name.starts_with(knowhere::meta::EMB_LIST_META);
                if (!mmaped || sidecar) {
                    // Compatibility mmap assembly overlaps nullable codecs and
                    // output; Knowhere also reads embedding metadata into heap
                    // before restoring its strategy. Do not charge main file bytes
                    // as retained heap memory in the mmap path.
                    retained = SaturatingAdd(
                        retained,
                        SaturatingMultiply(uint64_t{info.payload_bytes},
                                           uint64_t{mmaped ? 2 : 1}));
                }
                scratch = std::max(scratch, uint64_t{info.max_transient_bytes});
                if (name == INDEX_FILE_SLICE_META) {
                    retained = SaturatingAdd(
                        retained,
                        SaturatingMultiply(uint64_t{info.payload_bytes},
                                           uint64_t{32}));
                }
            }
            if (mmaped) {
                retained = SaturatingAdd(
                    retained,
                    SaturatingMultiply(
                        uint64_t{storage::FileWriter::MAX_BUFFER_SIZE},
                        uint64_t{element_type == DataType::NONE ? 1 : 3}));
            }
            co_return SaturatingAdd(retained, scratch);
        };
        // Enabled inspection uses the same shared async executor as loading;
        // disabled inspection stays at the synchronous planning boundary.
        const bool use_async_load =
            segcore::storagev2translator::StorageV2AsyncLoadEnabled();
        const auto overhead =
            use_async_load ? folly::coro::blockingWait(inspect().scheduleOn(
                                 storage::ResolveAsyncLoadExecutor(
                                     {}, proto::common::LoadPriority::HIGH)))
                           : folly::coro::blockingWait(inspect());
        request.max_memory_cost =
            std::max(request.max_memory_cost,
                     SaturatingAdd(request.final_memory_cost, overhead));
        return request;
    }
    return ScalarIndexLoadResource(field_type,
                                   index_version,
                                   index_size_in_bytes,
                                   index_params,
                                   mmap_enable,
                                   num_rows,
                                   index_files,
                                   file_manager_context,
                                   stream_load_info,
                                   use_shared_memory_overhead_group);
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
            has_raw_data = knowhere::IndexStaticFaced<
                knowhere::sparse_u32_f32>::HasRawData(index_type,
                                                      index_version,
                                                      config);
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
            auto metric_type = milvus::index::GetMetricTypeFromConfig(config);
            auto is_emb_list_metric =
                knowhere::get_el_metric_type(metric_type).has_value();
            switch (element_type) {
                case milvus::DataType::VECTOR_FLOAT:
                    resource = knowhere::IndexStaticFaced<knowhere::fp32>::
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
                case milvus::DataType::VECTOR_FLOAT16:
                    resource = knowhere::IndexStaticFaced<knowhere::fp16>::
                        EstimateLoadResource(index_type,
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
                    resource = knowhere::IndexStaticFaced<knowhere::bf16>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    has_raw_data =
                        knowhere::IndexStaticFaced<knowhere::bf16>::HasRawData(
                            index_type, index_version, config);
                    break;
                case milvus::DataType::VECTOR_BINARY:
                    resource = knowhere::IndexStaticFaced<knowhere::bin1>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    has_raw_data =
                        knowhere::IndexStaticFaced<knowhere::bin1>::HasRawData(
                            index_type, index_version, config);
                    break;
                case milvus::DataType::VECTOR_INT8:
                    resource = knowhere::IndexStaticFaced<knowhere::int8>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    has_raw_data =
                        knowhere::IndexStaticFaced<knowhere::int8>::HasRawData(
                            index_type, index_version, config);
                    break;

                default:
                    LOG_ERROR(
                        "invalid data type to estimate index load resource: "
                        "field_type {}, element_type {}",
                        field_type,
                        element_type);
                    return LoadResourceRequest{0, 0, 0, 0, true};
            }
            // Non-emb-list VECTOR_ARRAY indexes do not keep embedding-list
            // offsets, so they cannot reconstruct row-level embedding lists.
            if (!is_emb_list_metric) {
                has_raw_data = false;
            }
            break;
        }
        default:
            LOG_ERROR("invalid data type to estimate index load resource: {}",
                      field_type);
            return LoadResourceRequest{0, 0, 0, 0, true};
    }

    LoadResourceRequest request{};
    const auto& res = resource.value();

    request.has_raw_data = CanUseIndexRawDataForField(field_type, has_raw_data);
    request.final_disk_cost = res.diskCost;
    request.final_memory_cost = res.memoryCost;
    if (knowhere::UseDiskLoad(index_type, index_version) || mmaped) {
        request.max_disk_cost = res.diskCost;
        request.max_memory_cost = SaturatingAdd(
            std::max(res.memoryCost, download_buffer_size_in_bytes),
            SaturatingMultiply(
                uint64_t{storage::FileWriter::MAX_BUFFER_SIZE},
                uint64_t{mmaped && element_type != DataType::NONE ? 3 : 1}));
    } else {
        request.max_disk_cost = 0;
        request.max_memory_cost =
            SaturatingMultiply(uint64_t{2}, res.memoryCost);
    }
    if (knowhere::UseDiskLoad(index_type, index_version)) {
        const auto id_map_disk_cost = IdMapMmapDiskCost(config, num_rows);
        request.final_disk_cost =
            milvus::SaturatingAdd(request.final_disk_cost, id_map_disk_cost);
        request.max_disk_cost =
            milvus::SaturatingAdd(request.max_disk_cost, id_map_disk_cost);
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
    return ScalarIndexLoadResourceImpl(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       index_params,
                                       mmap_enable,
                                       num_rows,
                                       std::nullopt);
}

LoadResourceRequest
IndexFactory::ScalarIndexLoadResourceImpl(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::optional<storage::EntryStreamLoadInfo>& stream_load_info) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    auto index_type_it = index_params.find("index_type");
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    const std::string& index_type = index_type_it->second;

    knowhere::expected<knowhere::Resource> resource;
    auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    // File-aware callers use the persisted __edek__ marker. Keep plugin state
    // only as a compatibility fallback for callers without file context.
    auto encrypted_stream =
        scalar_version >= 3 &&
        (stream_load_info.has_value()
             ? stream_load_info->encrypted
             : milvus::storage::PluginLoader::GetInstance().getCipherPlugin() !=
                   nullptr);
    auto file_stream = index_type == milvus::index::INVERTED_INDEX_TYPE ||
                       index_type == milvus::index::NGRAM_INDEX_TYPE ||
                       index_type == milvus::index::RTREE_INDEX_TYPE;
    auto stream_memory_overhead =
        ScalarIndexStreamMemoryOverhead(index_size_in_bytes,
                                        scalar_version,
                                        encrypted_stream,
                                        file_stream,
                                        stream_load_info);

    return ScalarIndexLoadResourceWithOverhead(field_type,
                                               index_size_in_bytes,
                                               index_params,
                                               mmap_enable,
                                               num_rows,
                                               stream_memory_overhead);
}

LoadResourceRequest
IndexFactory::ScalarIndexLoadResourceWithOverhead(
    DataType field_type,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    uint64_t stream_memory_overhead) {
    const auto& index_type = index_params.at("index_type");
    LoadResourceRequest request{};
    request.has_raw_data = false;

    if (index_type == milvus::index::ASCENDING_SORT) {
        // Old V3 sort files do not have idx_to_offsets and valid_bitset
        // entries, so LoadEntries rebuilds them into heap memory.
        auto legacy_aux_bytes = SortLegacyAuxBytes(num_rows);
        if (mmap_enable) {
            // V3 streaming: chunks streamed to disk + mmap. The index data is
            // not heap-resident, but legacy metadata may be heap-resident.
            auto resident_bytes = legacy_aux_bytes;
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(resident_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // V3 streaming: pre-allocate target, stream into it
            request.final_memory_cost =
                SaturatingAdd(index_size_in_bytes, legacy_aux_bytes);
            request.final_disk_cost = 0;
            request.max_memory_cost = SaturatingAdd(request.final_memory_cost,
                                                    stream_memory_overhead);
            request.max_disk_cost = 0;
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::MARISA_TRIE ||
               index_type == milvus::index::MARISA_TRIE_UPPER) {
        if (mmap_enable) {
            // V3 streaming: trie, str_ids, and persisted CSR are mmap'd.
            // Old V3 files do not have CSR entries, so LoadEntries rebuilds
            // CSR into heap vectors. Estimate a conservative legacy upper
            // bound because resource estimation cannot inspect entries here.
            auto legacy_csr_resident_bytes = MarisaLegacyCsrBytes(num_rows, 2);
            auto legacy_csr_peak_bytes = MarisaLegacyCsrBytes(num_rows, 3);
            request.final_memory_cost = legacy_csr_resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(legacy_csr_peak_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // V3 streaming: trie via temp file + read, str_ids pre-allocated
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;  // trie temp file
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::INVERTED_INDEX_TYPE ||
               index_type == milvus::index::NGRAM_INDEX_TYPE) {
        // Sealed nullable Tantivy indexes materialize an immutable validity
        // bitmap on the heap. The estimator cannot know whether a sidecar
        // contains nulls, so reserve the conservative full bitmap.
        auto validity_bitmap_bytes = ValidityBitmapBytes(num_rows);
        if (mmap_enable) {
            request.final_memory_cost = validity_bitmap_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = milvus::SaturatingAdd(
                stream_memory_overhead, validity_bitmap_bytes);
        } else {
            const auto resident_bytes = milvus::SaturatingAdd(
                index_size_in_bytes, validity_bitmap_bytes);
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
        request.max_memory_cost = stream_memory_overhead;
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else if (index_type == milvus::index::FMINDEX_INDEX_TYPE) {
        // FM-index is a single flat blob. A LoadView (mmap) load views every
        // serialized array in place — wavelet words, sampled bitvector,
        // sampled-SA values, doc boundaries; nothing is heap-copied. What the
        // load DOES rebuild on the heap is the rank directories plus small
        // derived vectors and the wrapper's null bitmap. At the default
        // fm_block_bytes=64 the wavelet rank directory is roughly 1/8 of its
        // packed words, so index_size can substantially overstate steady heap
        // usage, especially when per-row document boundaries dominate the
        // blob. Keep index_size as the deliberately conservative pre-load
        // admission/peak bound; after LoadView succeeds FMIndex replaces the
        // cache cell's memory_bytes with its measured resident heap while
        // retaining file_bytes.
        if (mmap_enable) {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost =
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead);
            request.max_disk_cost = index_size_in_bytes;
        } else {
            // Deserialize MOVES the reader entry buffer into owned_blob_ (no
            // intermediate copies), so the resident set is the owned blob (~1x)
            // plus the rebuilt rank directories (~1x blob): both steady and
            // peak are ~2x. (Without the move overload the peak was ~4x from
            // three simultaneous full copies.)
            request.final_memory_cost =
                SaturatingMultiply(index_size_in_bytes, uint64_t{2});
            request.final_disk_cost = 0;
            request.max_memory_cost =
                SaturatingMultiply(index_size_in_bytes, uint64_t{2});
            request.max_disk_cost = 0;
        }
        request.has_raw_data = false;
    } else if (index_type == milvus::index::BITMAP_INDEX_TYPE) {
        if (mmap_enable) {
            // V3 streaming: stream to temp file (mmap'd), then MMapIndexData
            // converts one bitmap at a time to frozen format. The conversion
            // still allocates a per-bitmap heap buffer, so reserve for the
            // largest plausible bitmap in addition to stream buffers.
            auto resident_bytes = BitsetBytes(num_rows);
            auto frozen_buffer_bytes =
                BitmapMmapFrozenBufferBytes(num_rows, index_size_in_bytes);
            request.final_memory_cost = resident_bytes;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = SaturatingAdd(
                SaturatingAdd(resident_bytes, stream_memory_overhead),
                frozen_buffer_bytes);
            request.max_disk_cost = SaturatingMultiply(
                index_size_in_bytes, uint64_t{2});  // temp + final
        } else {
            // V3 streaming: pre-allocate buffer + deserialize
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost = std::max(
                SaturatingMultiply(index_size_in_bytes, uint64_t{2}),
                SaturatingAdd(index_size_in_bytes, stream_memory_overhead));
            request.max_disk_cost = 0;
        }

        request.has_raw_data = false;
    } else if (index_type == milvus::index::HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost =
            SaturatingMultiply(index_size_in_bytes, uint64_t{2});
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

namespace {
folly::coro::Task<std::unique_ptr<storage::AsyncIndexEntryReader>>
InspectAsyncScalarIndex(const std::vector<std::string>& files,
                        const storage::FileManagerContext& context) {
    AssertInfo(files.size() == 1 && context.Valid(),
               "Async scalar load requires one V3 file and a valid context");
    storage::MemFileManagerImpl manager(context);
    auto input = manager.OpenInputStream(files.front());
    AssertInfo(input != nullptr, "Failed to open packed scalar index");
    const auto size = input->Size();
    co_return co_await storage::AsyncIndexEntryReader::Open(
        std::move(input),
        size,
        context.fieldDataMeta.collection_id,
        proto::common::LoadPriority::HIGH,
        {});
}
}  // namespace

AsyncScalarIndexLoadResource
IndexFactory::ScalarIndexAsyncLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context) {
    const auto version =
        GetValueFromConfig<int32_t>(ParseConfigFromIndexParams(index_params),
                                    SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    if (version < 3) {
        return {ScalarIndexLegacyLoadResource(field_type,
                                              index_size,
                                              index_params,
                                              mmap_enable,
                                              num_rows,
                                              index_files,
                                              context,
                                              true),
                std::nullopt};
    }
    auto reader = folly::coro::blockingWait(
        InspectAsyncScalarIndex(index_files, context)
            .scheduleOn(storage::ResolveAsyncLoadExecutor(
                {}, proto::common::LoadPriority::HIGH)));
    const auto& catalog = reader->Catalog();
    auto resolved_params = index_params;
    if (resolved_params.at("index_type") == HYBRID_INDEX_TYPE) {
        auto config = ParseConfigFromIndexParams(index_params);
        config[INDEX_FILES] = index_files;
        const auto type = ResolvePackedHybridIndexType(catalog, config);
        const auto resolved = HybridInternalIndexTypeToIndexType(type);
        AssertInfo(!resolved.empty(),
                   "Unknown async hybrid index type {}",
                   static_cast<int>(type));
        resolved_params["index_type"] = resolved;
    }
    uint64_t total_transient = 0;
    uint64_t max_task = 0;
    for (const auto& entry : catalog.Entries()) {
        if (const auto* encrypted =
                std::get_if<storage::EncryptedEntrySource>(&entry.source)) {
            for (const auto& slice : encrypted->slices) {
                const auto bytes = SaturatingAdd(
                    milvus::SaturatingMultiply(slice.remote_bytes, uint64_t{2}),
                    slice.target_bytes);
                total_transient = SaturatingAdd(total_transient, bytes);
                max_task = std::max(max_task, bytes);
            }
        } else {
            total_transient =
                SaturatingAdd(total_transient, entry.plaintext_size);
            max_task =
                std::max(max_task,
                         std::min<uint64_t>(entry.plaintext_size,
                                            storage::DefaultStreamSliceSize()));
        }
    }
    const auto workers = storage::GetAsyncLoadThreadPoolSize();
    // Bound one load by its materializer's in-flight slice count. Request-local
    // reservations must remain valid even if the byte budget expands later.
    const auto read_peak = std::min(
        total_transient,
        milvus::SaturatingMultiply(max_task, static_cast<uint64_t>(workers)));
    const auto& type = resolved_params.at("index_type");
    uint64_t staging_bytes = 0;
    if ((type == INVERTED_INDEX_TYPE || type == NGRAM_INDEX_TYPE) &&
        catalog.HasEntry(INDEX_NULL_OFFSET_FILE_NAME)) {
        // The whole sidecar survives until FinalizeSealed builds validity.
        staging_bytes = catalog.At(INDEX_NULL_OFFSET_FILE_NAME).plaintext_size;
    } else if (type == BITMAP_INDEX_TYPE) {
        const bool loads_to_mmap =
            mmap_enable && catalog.GetMeta<size_t>(BITMAP_INDEX_LENGTH) >
                               DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND;
        if (!loads_to_mmap) {
            staging_bytes = catalog.At(BITMAP_INDEX_DATA).plaintext_size;
            mmap_enable = false;
        }
        if (catalog.HasEntry(BITMAP_INDEX_VALID_BITSET)) {
            staging_bytes = SaturatingAdd(
                staging_bytes,
                catalog.At(BITMAP_INDEX_VALID_BITSET).plaintext_size);
        }
    }
    if (type == FMINDEX_INDEX_TYPE &&
        catalog.HasEntry(FMINDEX_NULL_BITMAP_FILE_NAME)) {
        staging_bytes = SaturatingAdd(
            staging_bytes,
            catalog.At(FMINDEX_NULL_BITMAP_FILE_NAME).plaintext_size);
    }
    if (type == ASCENDING_SORT && catalog.HasMeta("version") &&
        catalog.HasEntry("valid_bitset")) {
        staging_bytes = SaturatingAdd(
            staging_bytes, catalog.At("valid_bitset").plaintext_size);
    }
    auto request = ScalarIndexLoadResourceWithOverhead(field_type,
                                                       index_size,
                                                       resolved_params,
                                                       mmap_enable,
                                                       num_rows,
                                                       read_peak);
    if (type == RTREE_INDEX_TYPE && catalog.HasEntry("index_null_offset")) {
        request.final_memory_cost =
            catalog.At("index_null_offset").plaintext_size;
    }
    if (field_type == DataType::JSON) {
        const auto non_exist_bytes =
            catalog.HasEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME)
                ? catalog.At(INDEX_NON_EXIST_OFFSET_FILE_NAME).plaintext_size
                : uint64_t{0};
        request.final_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingAdd(non_exist_bytes, ValidityBitmapBytes(num_rows)));
    }
    request.max_memory_cost =
        std::max(request.max_memory_cost,
                 SaturatingAdd(request.final_memory_cost,
                               SaturatingAdd(staging_bytes, read_peak)));
    std::optional<cachinglayer::LoadingOverheadConfig> overhead;
    // Only fold actual, leased slice buffers into the shared resource group.
    // Conversion scratch and whole-entry staging stay in the per-load peak.
    if (staging_bytes == 0 && type != BITMAP_INDEX_TYPE &&
        request.max_memory_cost - request.final_memory_cost <= read_peak) {
        auto memory_group =
            storage::LoadMemoryOverheadController::GetInstance().GetOrCreate(
                milvus::ThreadPools::GetLoadExecutorWorkers());
        AssertInfo(max_task <= static_cast<uint64_t>(
                                   std::numeric_limits<int64_t>::max()),
                   "Async scalar task estimate exceeds resource policy range");
        overhead = cachinglayer::LoadingOverheadConfig{
            cachinglayer::LoadingOverheadGroupBinding{
                std::move(memory_group), static_cast<int64_t>(max_task)},
            std::nullopt};
    }
    return {request, std::move(overhead)};
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
    const storage::FileManagerContext& file_manager_context,
    std::optional<storage::EntryStreamLoadInfo>* stream_load_info,
    bool* use_shared_memory_overhead_group) {
    auto index_type_it = index_params.find("index_type");
    AssertInfo(index_type_it != index_params.end(), "index type is empty");
    std::optional<storage::EntryStreamLoadInfo> inspected_stream_load_info;
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);
    auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    if (scalar_version < 3 && index_type_it->second != FMINDEX_INDEX_TYPE &&
        file_manager_context.Valid() && !index_files.empty()) {
        if (stream_load_info) {
            stream_load_info->reset();
        }
        if (use_shared_memory_overhead_group) {
            *use_shared_memory_overhead_group = false;
        }
        return ScalarIndexLegacyLoadResource(field_type,
                                             index_size_in_bytes,
                                             index_params,
                                             mmap_enable,
                                             num_rows,
                                             index_files,
                                             file_manager_context,
                                             false);
    }
    std::optional<ScalarIndexType> internal_index_type;
    if (index_type_it->second == milvus::index::HYBRID_INDEX_TYPE) {
        try {
            internal_index_type = ResolveHybridInternalIndexType(
                index_files, file_manager_context, &inspected_stream_load_info);
        } catch (std::exception& e) {
            if (scalar_version >= 3 &&
                !inspected_stream_load_info.has_value()) {
                inspected_stream_load_info = InspectScalarIndexStreamLoadInfo(
                    index_files, file_manager_context);
            }
            LOG_WARN(
                "failed to resolve hybrid scalar internal index type, "
                "fallback to hybrid estimate: {}",
                e.what());
        }
    } else if (scalar_version >= 3) {
        inspected_stream_load_info =
            InspectScalarIndexStreamLoadInfo(index_files, file_manager_context);
    }
    if (stream_load_info != nullptr) {
        *stream_load_info = inspected_stream_load_info;
    }

    auto resolved_params = index_params;
    if (internal_index_type.has_value()) {
        auto resolved_index_type =
            HybridInternalIndexTypeToIndexType(internal_index_type.value());
        if (!resolved_index_type.empty()) {
            resolved_params["index_type"] = resolved_index_type;
            LOG_INFO(
                "estimate hybrid scalar index load resource by internal index "
                "type: {}",
                resolved_index_type);
        }
    }

    const auto& resolved_index_type = resolved_params.at("index_type");
    // BITMAP staging and frozen-conversion buffers are allocated by the
    // request itself, outside the entry-stream executor and transient budget.
    // Keep their overhead request-local. An unresolved HYBRID may also select
    // BITMAP at load time, so it must use the same conservative path.
    auto use_shared_group =
        scalar_version >= 3 &&
        resolved_index_type != milvus::index::BITMAP_INDEX_TYPE &&
        resolved_index_type != milvus::index::HYBRID_INDEX_TYPE;
    if (use_shared_memory_overhead_group != nullptr) {
        *use_shared_memory_overhead_group = use_shared_group;
    }

    return ScalarIndexLoadResourceImpl(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       resolved_params,
                                       mmap_enable,
                                       num_rows,
                                       inspected_stream_load_info);
}

LoadResourceRequest
IndexFactory::ScalarIndexLegacyLoadResource(
    DataType field_type,
    uint64_t index_size,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    const std::vector<std::string>& index_files,
    const storage::FileManagerContext& context,
    bool use_async_load) {
    struct Inspection {
        std::string type;
        size_t payload_bytes{0};
        size_t retained_bytes{0};
        size_t max_transient_bytes{0};
        bool disk_files{false};
        bool string_sort{false};
        size_t bitmap_rows{0};
        size_t bitmap_cardinality{0};
    };
    auto inspect = [&]() -> folly::coro::Task<Inspection> {
        Inspection result;
        result.type = index_params.at(INDEX_TYPE);
        storage::MemFileManagerImpl manager(context);
        constexpr auto priority = proto::common::LoadPriority::HIGH;
        if (result.type == HYBRID_INDEX_TYPE) {
            const auto type_file = std::find_if(
                index_files.begin(), index_files.end(), [](const auto& file) {
                    return GetIndexFileBaseName(file) == INDEX_TYPE;
                });
            if (type_file == index_files.end()) {
                ThrowInfo(DataFormatBroken, "Missing legacy Hybrid index_type");
            }
            const std::vector<std::string> type_files{*type_file};
            auto binary =
                co_await manager.LoadIndexBinarySetAsync(type_files, priority);
            auto type = binary.GetByName(INDEX_TYPE);
            if (type == nullptr || type->size != sizeof(uint8_t)) {
                ThrowInfo(DataFormatBroken, "Invalid legacy Hybrid index_type");
            }
            result.type = HybridInternalIndexTypeToIndexType(
                static_cast<ScalarIndexType>(type->data[0]));
            if (result.type.empty()) {
                ThrowInfo(DataFormatBroken,
                          "Unsupported legacy Hybrid index_type");
            }
        }
        result.disk_files = result.type == INVERTED_INDEX_TYPE ||
                            result.type == NGRAM_INDEX_TYPE ||
                            result.type == RTREE_INDEX_TYPE;
        for (const auto& file : index_files) {
            const auto path =
                result.type == RTREE_INDEX_TYPE &&
                        file.find('/') == std::string::npos
                    ? manager.GetRemoteIndexObjectPrefix() + "/" + file
                    : file;
            auto input = storage::OpenLegacyIndexInput(
                context.chunkManagerPtr, context.fs, path);
            const auto info =
                co_await storage::InspectLegacyIndexFileAsync(*input, priority);
            result.payload_bytes =
                SaturatingAdd(result.payload_bytes, info.payload_bytes);
            result.max_transient_bytes =
                std::max(result.max_transient_bytes, info.max_transient_bytes);
            const auto name = GetIndexFileBaseName(file);
            // Legacy string Sort has a version entry; numeric Sort does not.
            result.string_sort |= result.type == ASCENDING_SORT &&
                                  (name == "version" || name == "version_0");
            if (name == INDEX_FILE_SLICE_META) {
                result.max_transient_bytes = std::max(
                    result.max_transient_bytes,
                    SaturatingMultiply(info.payload_bytes, size_t{32}));
            }
            const bool metadata =
                name == INDEX_TYPE || name == INDEX_FILE_SLICE_META ||
                name.starts_with(INDEX_NULL_OFFSET_FILE_NAME) ||
                name.starts_with(INDEX_NON_EXIST_OFFSET_FILE_NAME) ||
                name.starts_with(NGRAM_AVG_ROW_SIZE_FILE_NAME);
            if (!result.disk_files || metadata) {
                result.retained_bytes =
                    SaturatingAdd(result.retained_bytes, info.payload_bytes);
            }
        }
        if (result.type == BITMAP_INDEX_TYPE) {
            auto metadata = co_await manager.LoadIndexBinarySetAsync(
                index_files, priority, {}, BITMAP_INDEX_META);
            const auto entry = metadata.GetByName(BITMAP_INDEX_META);
            if (entry == nullptr) {
                ThrowInfo(DataFormatBroken, "Missing legacy Bitmap metadata");
            }
            try {
                // YAML accepts both the original YAML and later JSON metadata.
                const auto values = YAML::Load(std::string(
                    reinterpret_cast<const char*>(entry->data.get()),
                    entry->size));
                result.bitmap_rows = values[BITMAP_INDEX_NUM_ROWS].as<size_t>();
                result.bitmap_cardinality =
                    values[BITMAP_INDEX_LENGTH].as<size_t>();
            } catch (const YAML::Exception& error) {
                ThrowInfo(DataFormatBroken,
                          "Invalid legacy Bitmap metadata: {}",
                          error.what());
            }
            if (result.bitmap_rows >
                static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
                ThrowInfo(DataFormatBroken,
                          "Legacy Bitmap row count exceeds int64");
            }
        }
        co_return result;
    };
    const auto info = use_async_load
                          ? folly::coro::blockingWait(inspect().scheduleOn(
                                storage::ResolveAsyncLoadExecutor(
                                    {}, proto::common::LoadPriority::HIGH)))
                          : folly::coro::blockingWait(inspect());
    auto resolved_params = index_params;
    resolved_params[INDEX_TYPE] = info.type;
    const uint64_t persisted_bytes =
        std::max<uint64_t>(index_size, info.payload_bytes);
    auto effective_rows = num_rows;
    if (field_type == DataType::ARRAY) {
        // Nested offsets address flattened elements. Serialized bytes bound
        // that count even when the planner only knows segment rows.
        effective_rows = std::max<int64_t>(
            num_rows,
            std::min<uint64_t>(info.payload_bytes,
                               std::numeric_limits<int64_t>::max()));
    }
    if (info.type == BITMAP_INDEX_TYPE) {
        effective_rows = std::max<int64_t>(num_rows, info.bitmap_rows);
    }
    auto request = ScalarIndexLoadResourceWithOverhead(field_type,
                                                       persisted_bytes,
                                                       resolved_params,
                                                       mmap_enable,
                                                       effective_rows,
                                                       0);
    if (info.type == BITMAP_INDEX_TYPE) {
        // Low-cardinality Bitmap expands each posting list into a full bitset,
        // even in mmap mode. This can dwarf a compressed Roaring payload.
        const auto validity = ValidityBitmapBytes(effective_rows);
        const auto config = ParseConfigFromIndexParams(index_params);
        const auto cache =
            GetValueFromConfig<bool>(config, ENABLE_OFFSET_CACHE)
                    .value_or(false)
                ? SaturatingMultiply(static_cast<uint64_t>(effective_rows),
                                     uint64_t{sizeof(void*)})
                : uint64_t{0};
        // Per key: ordered-map node, key, and Roaring/bitset object overhead.
        const auto bookkeeping = SaturatingMultiply(
            static_cast<uint64_t>(info.bitmap_cardinality), uint64_t{128});
        auto resident =
            SaturatingAdd(validity, SaturatingAdd(cache, bookkeeping));
        if (info.bitmap_cardinality <= DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND) {
            resident = SaturatingAdd(
                resident,
                SaturatingMultiply(
                    validity, static_cast<uint64_t>(info.bitmap_cardinality)));
        }
        request.final_memory_cost =
            std::max(request.final_memory_cost, resident);
    }
    if (info.type == ASCENDING_SORT && !mmap_enable &&
        !IsStringDataType(field_type) && field_type != DataType::ARRAY) {
        request.final_memory_cost =
            SaturatingAdd(persisted_bytes, SortLegacyAuxBytes(num_rows));
    }
    if (info.string_sort && !mmap_enable) {
        // MemoryImpl owns string and small-vector objects per distinct value.
        // Each serialized value has at least four uint32 fields/offsets.
        const auto unique_bound =
            std::min<uint64_t>(std::max<int64_t>(0, effective_rows),
                               persisted_bytes / (4 * sizeof(uint32_t)));
        request.final_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingMultiply(
                unique_bound,
                uint64_t{sizeof(std::string) +
                         sizeof(StringIndexSortMemoryImpl::PostingList)}));
    }
    // Legacy mmap retains its metadata in heap. In-memory Marisa also rebuilds
    // CSR arrays; serialized trie bytes alone do not cover that representation.
    if (info.type == MARISA_TRIE || info.type == MARISA_TRIE_UPPER) {
        request.final_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingAdd(
                SaturatingMultiply(
                    static_cast<uint64_t>(std::max<int64_t>(0, effective_rows)),
                    uint64_t{sizeof(int64_t)}),
                MarisaLegacyCsrBytes(effective_rows, 2)));
    }
    if (field_type == DataType::JSON) {
        request.final_memory_cost = SaturatingAdd(
            request.final_memory_cost,
            SaturatingAdd(info.retained_bytes, ValidityBitmapBytes(num_rows)));
    }
    if (info.type == RTREE_INDEX_TYPE) {
        // Boost deserializes the tree into heap; the local archive remains on
        // disk. Reserve node/allocator overhead in addition to persisted bytes.
        request.final_memory_cost =
            std::max(request.final_memory_cost,
                     SaturatingMultiply(persisted_bytes, uint64_t{2}));
    }
    auto transient =
        SaturatingAdd(info.retained_bytes, info.max_transient_bytes);
    if (info.disk_files || mmap_enable || info.type == MARISA_TRIE ||
        info.type == MARISA_TRIE_UPPER) {
        // FileWriter's buffer setting is refreshable before a cache reload.
        transient =
            SaturatingAdd(transient, storage::FileWriter::MAX_BUFFER_SIZE);
    }
    request.max_memory_cost = SaturatingAdd(
        std::max(request.max_memory_cost, request.final_memory_cost),
        transient);
    if (field_type == DataType::ARRAY) {
        request.has_raw_data = false;
    }
    return request;
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
        case DataType::VARCHAR:
        case DataType::TEXT: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value());
            }
            auto& fmindex_params = create_index_info.fmindex_params;
            if (fmindex_params.has_value()) {
                return std::make_unique<FMIndex>(file_manager_context,
                                                 fmindex_params.value());
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

namespace {

template <typename T, typename BaseIndex, typename... Args>
IndexBasePtr
MakeJsonWrapped(const CreateIndexInfo& info,
                const storage::FileManagerContext& ctx,
                Args&&... args) {
    return std::make_unique<JsonScalarIndexWrapper<T, BaseIndex>>(
        info.json_cast_type,
        info.json_path,
        JsonCastFunction::FromString(info.json_cast_function),
        ctx.fieldDataMeta.field_schema,
        ctx,
        std::forward<Args>(args)...);
}

template <typename T>
IndexBasePtr
MakeJsonHybrid(const CreateIndexInfo& info,
               const storage::FileManagerContext& ctx) {
    return std::make_unique<JsonHybridScalarIndex<T>>(
        info.json_cast_type,
        info.json_path,
        JsonCastFunction::FromString(info.json_cast_function),
        ctx.fieldDataMeta.field_schema,
        info.tantivy_index_version,
        ctx);
}

}  // namespace

IndexBasePtr
IndexFactory::CreateJsonIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    const auto& index_type = create_index_info.index_type;
    const auto& cast_dtype = create_index_info.json_cast_type;
    const auto& nested_path = create_index_info.json_path;
    const auto& json_cast_function = create_index_info.json_cast_function;

    // Sort index
    if (index_type == ASCENDING_SORT) {
        switch (cast_dtype.element_type()) {
            case JsonCastType::DataType::DOUBLE:
                return MakeJsonWrapped<double, ScalarIndexSort<double>>(
                    create_index_info, file_manager_context);
            case JsonCastType::DataType::VARCHAR:
                return MakeJsonWrapped<std::string, StringIndexSort>(
                    create_index_info, file_manager_context);
            default:
                ThrowInfo(DataTypeInvalid,
                          "Invalid cast type for JSON sort index: {}",
                          cast_dtype);
        }
    }

    // Bitmap index
    if (index_type == BITMAP_INDEX_TYPE) {
        switch (cast_dtype.element_type()) {
            case JsonCastType::DataType::BOOL:
                return MakeJsonWrapped<bool, BitmapIndex<bool>>(
                    create_index_info, file_manager_context);
            case JsonCastType::DataType::VARCHAR:
                return MakeJsonWrapped<std::string, BitmapIndex<std::string>>(
                    create_index_info, file_manager_context);
            default:
                ThrowInfo(DataTypeInvalid,
                          "Invalid cast type for JSON bitmap index: {}",
                          cast_dtype);
        }
    }

    // Hybrid index
    if (index_type == HYBRID_INDEX_TYPE) {
        switch (cast_dtype.element_type()) {
            case JsonCastType::DataType::BOOL:
                return MakeJsonHybrid<bool>(create_index_info,
                                            file_manager_context);
            case JsonCastType::DataType::DOUBLE:
                return MakeJsonHybrid<double>(create_index_info,
                                              file_manager_context);
            case JsonCastType::DataType::VARCHAR:
                return MakeJsonHybrid<std::string>(create_index_info,
                                                   file_manager_context);
            default:
                ThrowInfo(DataTypeInvalid,
                          "Invalid cast type for JSON hybrid index: {}",
                          cast_dtype);
        }
    }

    // Inverted / NGram (existing paths). FMINDEX is VARCHAR-only in this
    // release — JSON string paths are a follow-up — so it never reaches here.
    AssertInfo(
        index_type == INVERTED_INDEX_TYPE || index_type == NGRAM_INDEX_TYPE,
        "Invalid index type for json index: {}",
        index_type);

    auto tantivy_ver =
        static_cast<uint32_t>(create_index_info.tantivy_index_version);

    switch (cast_dtype.element_type()) {
        case JsonCastType::DataType::BOOL:
            return MakeJsonWrapped<bool, InvertedIndexTantivy<bool>>(
                create_index_info, file_manager_context, tantivy_ver);
        case JsonCastType::DataType::DOUBLE:
            return MakeJsonWrapped<double, InvertedIndexTantivy<double>>(
                create_index_info, file_manager_context, tantivy_ver);
        case JsonCastType::DataType::VARCHAR: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value(), nested_path);
            }
            return MakeJsonWrapped<std::string,
                                   InvertedIndexTantivy<std::string>>(
                create_index_info, file_manager_context, tantivy_ver);
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
IndexFactory::CreateNestedIndex(
    IndexType index_type,
    int32_t tantivy_index_version,
    const storage::FileManagerContext& file_manager_context) {
    if (index_type == INVERTED_INDEX_TYPE) {
        return CreateNestedIndexInverted(tantivy_index_version,
                                         file_manager_context);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return CreateNestedIndexBitmap(file_manager_context);
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return CreateNestedIndexHybrid(tantivy_index_version,
                                       file_manager_context);
    }

    return CreateNestedIndexScalarIndexSort(file_manager_context);
}

IndexBasePtr
IndexFactory::CreateNestedIndexInverted(
    int32_t tantivy_index_version,
    const storage::FileManagerContext& file_manager_context) {
    DataType element_type = static_cast<DataType>(
        file_manager_context.fieldDataMeta.field_schema.element_type());
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<InvertedIndexTantivy<bool>>(
                tantivy_index_version,
                file_manager_context,
                false,  // inverted_index_single_segment
                true,   // user_specified_doc_id
                true);  // is_nested_index
        case DataType::INT8:
            return std::make_unique<InvertedIndexTantivy<int8_t>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::INT16:
            return std::make_unique<InvertedIndexTantivy<int16_t>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::INT32:
            return std::make_unique<InvertedIndexTantivy<int32_t>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::INT64:
            return std::make_unique<InvertedIndexTantivy<int64_t>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::FLOAT:
            return std::make_unique<InvertedIndexTantivy<float>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::DOUBLE:
            return std::make_unique<InvertedIndexTantivy<double>>(
                tantivy_index_version, file_manager_context, false, true, true);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<InvertedIndexTantivy<std::string>>(
                tantivy_index_version, file_manager_context, false, true, true);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", element_type);
    }
}

IndexBasePtr
IndexFactory::CreateNestedIndexBitmap(
    const storage::FileManagerContext& file_manager_context) {
    DataType element_type = static_cast<DataType>(
        file_manager_context.fieldDataMeta.field_schema.element_type());
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<BitmapIndex<bool>>(file_manager_context,
                                                       true);
        case DataType::INT8:
            return std::make_unique<BitmapIndex<int8_t>>(file_manager_context,
                                                         true);
        case DataType::INT16:
            return std::make_unique<BitmapIndex<int16_t>>(file_manager_context,
                                                          true);
        case DataType::INT32:
            return std::make_unique<BitmapIndex<int32_t>>(file_manager_context,
                                                          true);
        case DataType::INT64:
            return std::make_unique<BitmapIndex<int64_t>>(file_manager_context,
                                                          true);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<BitmapIndex<std::string>>(
                file_manager_context, true);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", element_type);
    }
}

IndexBasePtr
IndexFactory::CreateNestedIndexScalarIndexSort(
    const storage::FileManagerContext& file_manager_context) {
    DataType element_type = static_cast<DataType>(
        file_manager_context.fieldDataMeta.field_schema.element_type());
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<ScalarIndexSort<bool>>(file_manager_context,
                                                           true);
        case DataType::INT8:
            return std::make_unique<ScalarIndexSort<int8_t>>(
                file_manager_context, true);
        case DataType::INT16:
            return std::make_unique<ScalarIndexSort<int16_t>>(
                file_manager_context, true);
        case DataType::INT32:
            return std::make_unique<ScalarIndexSort<int32_t>>(
                file_manager_context, true);
        case DataType::INT64:
            return std::make_unique<ScalarIndexSort<int64_t>>(
                file_manager_context, true);
        case DataType::FLOAT:
            return std::make_unique<ScalarIndexSort<float>>(
                file_manager_context, true);
        case DataType::DOUBLE:
            return std::make_unique<ScalarIndexSort<double>>(
                file_manager_context, true);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<StringIndexSort>(file_manager_context,
                                                     true);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", element_type);
    }
}

IndexBasePtr
IndexFactory::CreateNestedIndexHybrid(
    int32_t tantivy_index_version,
    const storage::FileManagerContext& file_manager_context) {
    DataType element_type = static_cast<DataType>(
        file_manager_context.fieldDataMeta.field_schema.element_type());
    switch (element_type) {
        case DataType::BOOL:
            return std::make_unique<HybridScalarIndex<bool>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::INT8:
            return std::make_unique<HybridScalarIndex<int8_t>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::INT16:
            return std::make_unique<HybridScalarIndex<int16_t>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::INT32:
            return std::make_unique<HybridScalarIndex<int32_t>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::INT64:
            return std::make_unique<HybridScalarIndex<int64_t>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::FLOAT:
            return std::make_unique<HybridScalarIndex<float>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::DOUBLE:
            return std::make_unique<HybridScalarIndex<double>>(
                tantivy_index_version, file_manager_context, true);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<HybridScalarIndex<std::string>>(
                tantivy_index_version, file_manager_context, true);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", element_type);
    }
}

IndexBasePtr
IndexFactory::CreateScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto data_type = create_index_info.field_type;

    if (IsStructSubField(create_index_info.field_name)) {
        assert(data_type == DataType::ARRAY);
        return CreateNestedIndex(create_index_info.index_type,
                                 create_index_info.tantivy_index_version,
                                 file_manager_context);
    }

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
        case DataType::TEXT:
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
