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

#include "index/VectorDiskIndex.h"

#include <math.h>
#include <string.h>
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iosfwd>
#include <optional>
#include <stdexcept>
#include <string>

#include "common/Consts.h"
#include "common/FastMem.h"
#include "common/OffsetMapping.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/RangeSearchHelper.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/protobuf_utils.h"
#include "filemanager/FileManager.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/VectorIndexValidDataUtils.h"
#include "knowhere/binaryset.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "opentelemetry/trace/span.h"
#include "opentelemetry/trace/tracer.h"
#include "pb/common.pb.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::index {

#define kSearchListMaxValue1 200    // used if tok <= 20
#define kSearchListMaxValue2 65535  // used for topk > 20
#define kPrepareDim 100
#define kPrepareRows 1

namespace {

constexpr const char* EMPTY_EMB_LIST_OFFSET_KEY = "empty_emb_list_offsets";

struct DiskValidData {
    bool found = false;
    size_t total_count = 0;
    size_t valid_count = 0;
    std::vector<uint8_t> bitmap;
};

struct EmptyEmbListState {
    int64_t dim = 0;
    std::vector<size_t> offsets;
};

class DiskEmptyVectorIterator : public knowhere::IndexNode::iterator {
 public:
    std::pair<int64_t, float>
    Next() override {
        throw std::runtime_error("empty vector iterator has no next result");
    }

    bool
    HasNext() override {
        return false;
    }
};

template <typename LocalChunkManagerPtr>
std::optional<std::vector<size_t>>
ReadDiskEmbListOffsets(const LocalChunkManagerPtr& local_chunk_manager,
                       const std::string& offsets_path) {
    if (!local_chunk_manager->Exist(offsets_path)) {
        return std::nullopt;
    }

    auto file_size = local_chunk_manager->Size(offsets_path);
    AssertInfo(file_size >= sizeof(size_t),
               "embedding list offsets file is too small");
    size_t num_offsets = 0;
    local_chunk_manager->Read(offsets_path, 0, &num_offsets, sizeof(size_t));
    AssertInfo(num_offsets > 0, "embedding list offsets count is invalid");
    AssertInfo(file_size >= sizeof(size_t) + num_offsets * sizeof(size_t),
               "embedding list offsets file payload is too small");

    std::vector<size_t> offsets(num_offsets);
    local_chunk_manager->Read(offsets_path,
                              sizeof(size_t),
                              offsets.data(),
                              num_offsets * sizeof(size_t));
    AssertInfo(offsets.front() == 0, "embedding list offsets must start at 0");
    return offsets;
}

template <typename LocalChunkManagerPtr>
void
WriteDiskEmptyEmbListOffsets(const LocalChunkManagerPtr& local_chunk_manager,
                             const std::string& empty_offsets_path,
                             int64_t dim,
                             const std::vector<size_t>& offsets) {
    AssertInfo(dim > 0, "empty emb_list dim is invalid");
    AssertInfo(!offsets.empty() && offsets.front() == 0,
               "empty emb_list offsets are invalid");
    AssertInfo(offsets.back() == 0,
               "empty emb_list offsets must have no flattened vectors");

    if (!local_chunk_manager->Exist(empty_offsets_path)) {
        local_chunk_manager->CreateFile(empty_offsets_path);
    }

    auto count = ToValidDataCount(offsets.size());
    int64_t write_pos = 0;
    local_chunk_manager->Write(
        empty_offsets_path, write_pos, &dim, sizeof(int64_t));
    write_pos += sizeof(int64_t);
    local_chunk_manager->Write(
        empty_offsets_path, write_pos, &count, sizeof(uint64_t));
    write_pos += sizeof(uint64_t);
    local_chunk_manager->Write(empty_offsets_path,
                               write_pos,
                               const_cast<size_t*>(offsets.data()),
                               offsets.size() * sizeof(size_t));
}

template <typename LocalChunkManagerPtr>
std::optional<EmptyEmbListState>
ReadDiskEmptyEmbListOffsets(const LocalChunkManagerPtr& local_chunk_manager,
                            const std::string& empty_offsets_path) {
    if (!local_chunk_manager->Exist(empty_offsets_path)) {
        return std::nullopt;
    }

    auto file_size = local_chunk_manager->Size(empty_offsets_path);
    AssertInfo(file_size >= sizeof(int64_t) + sizeof(uint64_t),
               "empty emb_list offsets file is too small");

    int64_t read_pos = 0;
    int64_t dim = 0;
    local_chunk_manager->Read(
        empty_offsets_path, read_pos, &dim, sizeof(int64_t));
    read_pos += sizeof(int64_t);

    uint64_t wire_count = 0;
    local_chunk_manager->Read(
        empty_offsets_path, read_pos, &wire_count, sizeof(uint64_t));
    read_pos += sizeof(uint64_t);

    auto count = FromValidDataCount(wire_count);
    AssertInfo(count > 0, "empty emb_list offsets count is invalid");
    AssertInfo(file_size >= read_pos + count * sizeof(size_t),
               "empty emb_list offsets payload is too small");

    std::vector<size_t> offsets(count);
    local_chunk_manager->Read(
        empty_offsets_path, read_pos, offsets.data(), count * sizeof(size_t));
    AssertInfo(offsets.front() == 0, "empty emb_list offsets must start at 0");
    AssertInfo(offsets.back() == 0,
               "empty emb_list offsets must have no flattened vectors");
    return EmptyEmbListState{dim, std::move(offsets)};
}

size_t
GetEmbListNumOffsets(const DatasetPtr& dataset,
                     const size_t* offsets,
                     size_t total_vectors) {
    auto num_queries = dataset->Get<int64_t>(knowhere::meta::NQ);
    AssertInfo(num_queries > 0, "embedding list build query count is missing");
    AssertInfo(offsets[num_queries] == total_vectors,
               "embedding list build offsets are inconsistent with "
               "flattened rows: nq={}, terminal_offset={}, rows={}",
               num_queries,
               offsets[num_queries],
               total_vectors);
    return static_cast<size_t>(num_queries) + 1;
}

template <typename LocalChunkManagerPtr>
DiskValidData
ReadDiskValidData(const LocalChunkManagerPtr& local_chunk_manager,
                  const std::string& valid_data_path) {
    DiskValidData valid_data;
    if (!local_chunk_manager->Exist(valid_data_path)) {
        return valid_data;
    }

    valid_data.found = true;
    auto file_size = local_chunk_manager->Size(valid_data_path);
    AssertInfo(file_size >= sizeof(uint64_t),
               "nullable vector disk valid_data file is too small");
    uint64_t wire_count = 0;
    local_chunk_manager->Read(
        valid_data_path, 0, &wire_count, sizeof(uint64_t));
    valid_data.total_count = FromValidDataCount(wire_count);
    valid_data.bitmap.resize(GetValidDataBitmapSize(valid_data.total_count));
    AssertInfo(file_size >= sizeof(uint64_t) + valid_data.bitmap.size(),
               "nullable vector disk valid_data bitmap file is too small");
    if (!valid_data.bitmap.empty()) {
        local_chunk_manager->Read(valid_data_path,
                                  sizeof(uint64_t),
                                  valid_data.bitmap.data(),
                                  valid_data.bitmap.size());
    }
    valid_data.valid_count =
        CountValidDataBitmap(valid_data.total_count, valid_data.bitmap.data());
    return valid_data;
}

template <typename LocalChunkManagerPtr>
void
WriteDiskValidData(const LocalChunkManagerPtr& local_chunk_manager,
                   const std::string& valid_data_path,
                   const OffsetMapping& offset_mapping) {
    auto total_count = static_cast<size_t>(offset_mapping.GetTotalCount());
    auto wire_count = ToValidDataCount(total_count);
    auto packed_data = PackValidDataBitmap(offset_mapping);
    if (!local_chunk_manager->Exist(valid_data_path)) {
        local_chunk_manager->CreateFile(valid_data_path);
    }
    local_chunk_manager->Write(
        valid_data_path, 0, &wire_count, sizeof(uint64_t));
    if (!packed_data.empty()) {
        local_chunk_manager->Write(valid_data_path,
                                   sizeof(uint64_t),
                                   packed_data.data(),
                                   packed_data.size());
    }
}

}  // namespace

template <typename T>
VectorDiskAnnIndex<T>::VectorDiskAnnIndex(
    DataType elem_type,
    const IndexType& index_type,
    const MetricType& metric_type,
    const IndexVersion& version,
    const storage::FileManagerContext& file_manager_context)
    : VectorIndex(index_type, metric_type), elem_type_(elem_type) {
    CheckMetricTypeSupport<T>(metric_type);
    file_manager_ =
        std::make_shared<storage::DiskFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();

    // As we have guarded dup-load in QueryNode,
    // this assertion failed only if the Milvus rebooted in the same pod,
    // need to remove these files then re-load the segment
    if (local_chunk_manager->Exist(local_index_path_prefix)) {
        local_chunk_manager->RemoveDir(local_index_path_prefix);
    }
    CheckCompatible(version);
    local_chunk_manager->CreateDir(local_index_path_prefix);
    auto diskann_index_pack =
        knowhere::Pack(std::shared_ptr<milvus::FileManager>(file_manager_));
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<T>(
        GetIndexType(), version, diskann_index_pack);
    if (get_index_obj.has_value()) {
        index_ = get_index_obj.value();
    } else {
        auto err = get_index_obj.error();
        if (err == knowhere::Status::invalid_index_error) {
            ThrowInfo(ErrorCode::Unsupported, get_index_obj.what());
        }
        ThrowInfo(ErrorCode::KnowhereError, get_index_obj.what());
    }
}

template <typename T>
void
VectorDiskAnnIndex<T>::Load(const BinarySet& binary_set /* not used */,
                            const Config& config) {
    Load(milvus::tracer::TraceContext{}, config);
}

template <typename T>
void
VectorDiskAnnIndex<T>::Load(milvus::tracer::TraceContext ctx,
                            const Config& config) {
    knowhere::Json load_config = update_load_json(config);

    // start read file span with active scope
    {
        auto read_file_span =
            milvus::tracer::StartSpan("SegCoreReadDiskIndexFile", &ctx);
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
            read_file_nostd_span(read_file_span);
        auto read_scope =
            opentelemetry::trace::Tracer::WithActiveSpan(read_file_nostd_span);
        auto index_files =
            GetValueFromConfig<std::vector<std::string>>(config, "index_files");
        AssertInfo(index_files.has_value(),
                   "index file paths is empty when load disk ann index data");
        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);
        auto cache_files = GetCacheFilesForDiskIndexLoad(
            index_files.value(), index_.LoadIndexWithStream());
        if (!cache_files.empty()) {
            file_manager_->CacheIndexToDisk(cache_files, load_priority);
        }
        read_file_span->End();
    }

    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();

    auto valid_data_path = local_index_path_prefix + "/" + VALID_DATA_KEY;
    auto disk_valid_data =
        ReadDiskValidData(local_chunk_manager, valid_data_path);
    bool all_null_nullable = disk_valid_data.found &&
                             disk_valid_data.total_count > 0 &&
                             disk_valid_data.valid_count == 0;
    auto empty_emb_list_state = ReadDiskEmptyEmbListOffsets(
        local_chunk_manager,
        local_index_path_prefix + "/" + EMPTY_EMB_LIST_OFFSET_KEY);
    if (!all_null_nullable && !empty_emb_list_state.has_value()) {
        // start engine load index span
        auto span_load_engine =
            milvus::tracer::StartSpan("SegCoreEngineLoadDiskIndex", &ctx);
        opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>
            nostd_span_load_engine(span_load_engine);
        auto engine_scope = opentelemetry::trace::Tracer::WithActiveSpan(
            nostd_span_load_engine);
        auto stat = index_.Deserialize(knowhere::BinarySet(), load_config);
        if (stat != knowhere::Status::success)
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to Deserialize index, {}",
                      KnowhereStatusString(stat));
        span_load_engine->End();
        SetDim(index_.Dim());
    } else {
        auto dim = GetValueFromConfig<int64_t>(load_config, DIM_KEY);
        if (dim.has_value()) {
            SetDim(dim.value());
        }
    }
    if (empty_emb_list_state.has_value()) {
        SetDim(empty_emb_list_state->dim);
        empty_emb_list_offsets_ = std::move(empty_emb_list_state->offsets);
    }

    if (disk_valid_data.found) {
        BuildValidDataFromBitmap(
            this, disk_valid_data.total_count, disk_valid_data.bitmap.data());
    }
}

template <typename T>
IndexStatsPtr
VectorDiskAnnIndex<T>::Upload(const Config& config) {
    BinarySet ret;
    const auto& offset_mapping = GetOffsetMapping();
    if (!IsAllNullNullable(offset_mapping) && !IsEmptyEmbListIndex()) {
        auto stat = index_.Serialize(ret);
        if (stat != knowhere::Status::success) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "failed to serialize index, {}",
                      KnowhereStatusString(stat));
        }
    }
    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    return IndexStats::NewFromSizeMap(file_manager_->GetAddedTotalFileSize(),
                                      remote_paths_to_size);
}

template <typename T>
void
VectorDiskAnnIndex<T>::Build(const Config& config) {
    LOG_INFO("start build disk index, build_id: {}",
             config.value("build_id", "unknown"));

    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    knowhere::Json build_config;
    build_config.update(config);

    auto is_embedding_list = (elem_type_ != DataType::NONE);
    Config config_with_emb_list = config;
    config_with_emb_list[EMB_LIST] = is_embedding_list;
    auto local_raw_data_prefix = file_manager_->GetLocalRawDataObjectPrefix();
    auto raw_data_lease =
        file_manager_->AcquireLocalDirWriteLease(local_raw_data_prefix);

    std::string offsets_path;
    // Set offsets path in config for VECTOR_ARRAY
    if (is_embedding_list) {
        offsets_path = local_raw_data_prefix + "offset";
        config_with_emb_list[EMB_LIST_OFFSETS_PATH] = offsets_path;
    }

    // Set valid data path to track nullable vector fields
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    auto index_lease =
        file_manager_->AcquireLocalDirWriteLease(local_index_path_prefix);
    auto valid_data_path = local_index_path_prefix + "/" + VALID_DATA_KEY;
    config_with_emb_list[VALID_DATA_PATH_KEY] = valid_data_path;

    auto local_data_path =
        file_manager_->CacheRawDataToDisk<T>(config_with_emb_list);
    build_config[DISK_ANN_RAW_DATA_PATH] = local_data_path;

    auto disk_valid_data =
        ReadDiskValidData(local_chunk_manager, valid_data_path);
    if (disk_valid_data.found) {
        BuildValidDataFromBitmap(
            this, disk_valid_data.total_count, disk_valid_data.bitmap.data());
        if (disk_valid_data.valid_count == 0) {
            auto dim = GetValueFromConfig<int64_t>(build_config, DIM_KEY);
            if (dim.has_value()) {
                SetDim(dim.value());
            }
            file_manager_->AddFile(valid_data_path);
            file_manager_->RemoveRawDataFiles();
            LOG_INFO("build all-null nullable disk index done, build_id: {}",
                     config.value("build_id", "unknown"));
            return;
        }
    }

    // For VECTOR_ARRAY, verify offsets file exists and pass its path to build_config
    if (is_embedding_list) {
        auto offsets =
            ReadDiskEmbListOffsets(local_chunk_manager, offsets_path);
        if (!offsets.has_value()) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      fmt::format("Embedding list offsets file not found: {}",
                                  offsets_path));
        }
        if (offsets->back() == 0) {
            auto dim = GetValueFromConfig<int64_t>(build_config, DIM_KEY);
            AssertInfo(dim.has_value() && dim.value() > 0,
                       "dim is missing when build empty emb_list disk index");
            SetDim(dim.value());

            auto empty_offsets_path =
                local_index_path_prefix + "/" + EMPTY_EMB_LIST_OFFSET_KEY;
            WriteDiskEmptyEmbListOffsets(local_chunk_manager,
                                         empty_offsets_path,
                                         GetDim(),
                                         offsets.value());
            file_manager_->AddFile(empty_offsets_path);
            if (local_chunk_manager->Exist(valid_data_path)) {
                file_manager_->AddFile(valid_data_path);
            }
            file_manager_->RemoveRawDataFiles();
            empty_emb_list_offsets_ = std::move(offsets.value());
            LOG_INFO("build all-empty emb_list disk index done, build_id: {}",
                     config.value("build_id", "unknown"));
            return;
        }
        build_config[EMB_LIST_OFFSETS_PATH] = offsets_path;
    }

    build_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        auto num_threads = GetValueFromConfig<std::string>(
            build_config, DISK_ANN_BUILD_THREAD_NUM);
        AssertInfo(num_threads.has_value(),
                   "param {} is empty",
                   DISK_ANN_BUILD_THREAD_NUM);
        build_config[DISK_ANN_THREADS_NUM] =
            std::atoi(num_threads.value().c_str());
    }

    auto opt_fields = GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    auto is_partition_key_isolation =
        GetValueFromConfig<bool>(build_config, "partition_key_isolation");
    if (opt_fields.has_value() &&
        index_.IsAdditionalScalarSupported(
            is_partition_key_isolation.value_or(false))) {
        build_config[VEC_OPT_FIELDS_PATH] =
            file_manager_->CacheOptFieldToDisk(config);
        // `partition_key_isolation` is already in the config, so it falls through
        // into the index Build call directly
    }

    build_config.erase(INSERT_FILES_KEY);
    build_config.erase(VEC_OPT_FIELDS);
    auto stat = index_.Build({}, build_config);
    if (stat != knowhere::Status::success)
        ThrowInfo(ErrorCode::IndexBuildError,
                  "failed to build disk index, {}",
                  KnowhereStatusString(stat));

    // Add valid_data file to index if it was created (nullable vector field)
    if (local_chunk_manager->Exist(valid_data_path)) {
        file_manager_->AddFile(valid_data_path);
    }

    file_manager_->RemoveRawDataFiles();

    LOG_INFO("build disk index done, build_id: {}",
             config.value("build_id", "unknown"));
}

template <typename T>
void
VectorDiskAnnIndex<T>::BuildWithDataset(const DatasetPtr& dataset,
                                        const Config& config) {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    knowhere::Json build_config;
    build_config.update(config);

    auto is_embedding_list = (elem_type_ != DataType::NONE);
    build_config[EMB_LIST] = is_embedding_list;

    // set data path
    auto local_raw_data_prefix = file_manager_->GetLocalRawDataObjectPrefix();
    auto raw_data_lease =
        file_manager_->AcquireLocalDirWriteLease(local_raw_data_prefix);
    auto local_data_path = local_raw_data_prefix + "raw_data";
    build_config[DISK_ANN_RAW_DATA_PATH] = local_data_path;

    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    auto index_lease =
        file_manager_->AcquireLocalDirWriteLease(local_index_path_prefix);
    build_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    const auto& offset_mapping = GetOffsetMapping();
    if (HasValidData() && GetValidCount() == 0 &&
        offset_mapping.GetTotalCount() > 0) {
        auto valid_data_path = local_index_path_prefix + "/" + VALID_DATA_KEY;
        WriteDiskValidData(
            local_chunk_manager, valid_data_path, offset_mapping);
        file_manager_->AddFile(valid_data_path);
        auto dim = GetValueFromConfig<int64_t>(build_config, DIM_KEY);
        if (dim.has_value()) {
            SetDim(dim.value());
        }
        file_manager_->RemoveRawDataFiles();
        return;
    }

    if (is_embedding_list && milvus::GetDatasetRows(dataset) == 0) {
        auto offsets =
            dataset->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
        if (offsets == nullptr) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Embedding list offsets is empty when build index");
        }
        auto num_offsets = GetEmbListNumOffsets(dataset, offsets, 0);
        auto empty_offsets =
            std::vector<size_t>(offsets, offsets + num_offsets);
        auto empty_offsets_path =
            local_index_path_prefix + "/" + EMPTY_EMB_LIST_OFFSET_KEY;
        WriteDiskEmptyEmbListOffsets(local_chunk_manager,
                                     empty_offsets_path,
                                     dataset->GetDim(),
                                     empty_offsets);
        file_manager_->AddFile(empty_offsets_path);
        SetDim(dataset->GetDim());
        empty_emb_list_offsets_ = std::move(empty_offsets);
        file_manager_->RemoveRawDataFiles();
        return;
    }

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        auto num_threads = GetValueFromConfig<std::string>(
            build_config, DISK_ANN_BUILD_THREAD_NUM);
        AssertInfo(num_threads.has_value(),
                   "param {} is empty",
                   DISK_ANN_BUILD_THREAD_NUM);
        build_config[DISK_ANN_THREADS_NUM] =
            std::atoi(num_threads.value().c_str());
    }
    if (!local_chunk_manager->Exist(local_data_path)) {
        local_chunk_manager->CreateFile(local_data_path);
    }

    int64_t offset = 0;
    auto num = uint32_t(milvus::GetDatasetRows(dataset));
    local_chunk_manager->Write(local_data_path, offset, &num, sizeof(num));
    offset += sizeof(num);

    auto dim = uint32_t(milvus::GetDatasetDim(dataset));
    local_chunk_manager->Write(local_data_path, offset, &dim, sizeof(dim));
    offset += sizeof(dim);

    size_t data_size = static_cast<size_t>(num) * milvus::GetVecRowSize<T>(dim);
    auto raw_data = const_cast<void*>(milvus::GetDatasetTensor(dataset));
    local_chunk_manager->Write(local_data_path, offset, raw_data, data_size);

    // For VECTOR_ARRAY, write offsets to a separate file and pass the path to knowhere
    if (elem_type_ != DataType::NONE) {
        auto offsets =
            dataset->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
        if (offsets == nullptr) {
            ThrowInfo(ErrorCode::UnexpectedError,
                      "Embedding list offsets is empty when build index");
        }

        // Write offsets to disk file (use same path convention as Build method)
        std::string offsets_path = local_raw_data_prefix + "offset";
        local_chunk_manager->CreateFile(offsets_path);

        size_t total_vectors =
            static_cast<size_t>(milvus::GetDatasetRows(dataset));
        auto num_offsets =
            GetEmbListNumOffsets(dataset, offsets, total_vectors);

        // Write offsets to file
        // Format: [num_offsets (size_t)][offsets_data (size_t array)]
        int64_t write_pos = 0;
        local_chunk_manager->Write(
            offsets_path, write_pos, &num_offsets, sizeof(size_t));
        write_pos += sizeof(size_t);

        local_chunk_manager->Write(
            offsets_path,
            write_pos,
            const_cast<void*>(static_cast<const void*>(offsets)),
            num_offsets * sizeof(size_t));

        build_config[EMB_LIST_OFFSETS_PATH] = offsets_path;
    }

    auto stat = index_.Build({}, build_config);
    if (stat != knowhere::Status::success)
        ThrowInfo(ErrorCode::IndexBuildError,
                  "failed to build index, {}",
                  KnowhereStatusString(stat));

    if (HasValidData()) {
        auto valid_data_path = local_index_path_prefix + "/" + VALID_DATA_KEY;
        WriteDiskValidData(
            local_chunk_manager, valid_data_path, offset_mapping);
        file_manager_->AddFile(valid_data_path);
    }

    file_manager_->RemoveRawDataFiles();

    // TODO ::
    // SetDim(index_->Dim());
}

template <typename T>
void
VectorDiskAnnIndex<T>::Query(const DatasetPtr dataset,
                             const SearchInfo& search_info,
                             const BitsetView& bitset,
                             milvus::OpContext* op_context,
                             SearchResult& search_result) const {
    AssertInfo(GetMetricType() == search_info.metric_type_,
               "Metric type of field index isn't the same with search info");
    auto num_rows = dataset->GetRows();
    auto topk = search_info.topk_;

    knowhere::Json search_config = PrepareSearchParams(search_info);

    const auto& offset_mapping = GetOffsetMapping();
    if (IsAllNullNullable(offset_mapping) || IsEmptyEmbListIndex()) {
        auto offsets =
            dataset->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
        auto num_queries = dataset->GetRows();
        if (offsets != nullptr) {
            num_queries = dataset->Get<int64_t>(knowhere::meta::NQ);
            AssertInfo(num_queries > 0,
                       "embedding list query count is missing");
            auto total_vectors = static_cast<size_t>(dataset->GetRows());
            AssertInfo(
                offsets[num_queries] == total_vectors,
                "embedding list query offsets are inconsistent with flattened "
                "rows: nq={}, terminal_offset={}, rows={}",
                num_queries,
                offsets[num_queries],
                total_vectors);
        }
        auto total_num = num_queries * topk;
        search_result.seg_offsets_.assign(total_num, INVALID_SEG_OFFSET);
        search_result.distances_.assign(total_num, 0.0F);
        search_result.total_nq_ = num_queries;
        search_result.unity_topK_ = topk;
        return;
    }

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        // set search list size
        if (CheckKeyInConfig(search_info.search_params_, DISK_ANN_QUERY_LIST)) {
            search_config[DISK_ANN_SEARCH_LIST_SIZE] =
                search_info.search_params_[DISK_ANN_QUERY_LIST];
        }
        // set beamwidth
        search_config[DISK_ANN_QUERY_BEAMWIDTH] = int(search_beamwidth_);
        // set json reset field, will be removed later
        search_config[DISK_ANN_PQ_CODE_BUDGET] = 0.0;
    }

    // set index prefix, will be removed later
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    search_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    auto final = [&] {
        if (CheckAndUpdateKnowhereRangeSearchParam(
                search_info, topk, GetMetricType(), search_config)) {
            auto res =
                index_.RangeSearch(dataset, search_config, bitset, op_context);
            if (!res.has_value()) {
                ThrowInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to range search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            return ReGenRangeSearchResult(
                res.value(), topk, num_rows, GetMetricType());
        } else {
            auto res =
                index_.Search(dataset, search_config, bitset, op_context);
            if (!res.has_value()) {
                ThrowInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            return res.value();
        }
    }();

    auto ids = final->GetIds();
    // In embedding list query, final->GetRows() can be different from dataset->GetRows().
    auto num_queries = final->GetRows();
    float* distances = const_cast<float*>(final->GetDistance());
    final->SetIsOwner(true);

    auto round_decimal = search_info.round_decimal_;
    auto total_num = num_queries * topk;

    if (round_decimal != -1) {
        const float multiplier = pow(10.0, round_decimal);
        for (int i = 0; i < total_num; i++) {
            distances[i] = std::round(distances[i] * multiplier) / multiplier;
        }
    }
    search_result.seg_offsets_.resize(total_num);
    search_result.distances_.resize(total_num);
    search_result.total_nq_ = num_queries;
    search_result.unity_topK_ = topk;
    milvus::fastmem::FastMemcpy(
        search_result.seg_offsets_.data(),
        ids,
        total_num * sizeof(*search_result.seg_offsets_.data()));
    milvus::fastmem::FastMemcpy(
        search_result.distances_.data(),
        distances,
        total_num * sizeof(*search_result.distances_.data()));
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorDiskAnnIndex<T>::VectorIterators(const DatasetPtr dataset,
                                       const knowhere::Json& conf,
                                       const BitsetView& bitset,
                                       milvus::OpContext* op_context) const {
    auto make_empty_iterators = [](int64_t num_queries) {
        std::vector<knowhere::IndexNode::IteratorPtr> iterators;
        iterators.reserve(num_queries);
        for (int64_t i = 0; i < num_queries; ++i) {
            iterators.emplace_back(std::make_shared<DiskEmptyVectorIterator>());
        }
        return iterators;
    };

    const auto& offset_mapping = GetOffsetMapping();
    if (IsAllNullNullable(offset_mapping) || IsEmptyEmbListIndex()) {
        auto offsets =
            dataset->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
        auto num_queries = dataset->GetRows();
        if (offsets != nullptr) {
            num_queries = dataset->Get<int64_t>(knowhere::meta::NQ);
            AssertInfo(num_queries > 0,
                       "embedding list query count is missing");
            auto total_vectors = static_cast<size_t>(dataset->GetRows());
            AssertInfo(
                offsets[num_queries] == total_vectors,
                "embedding list query offsets are inconsistent with flattened "
                "rows: nq={}, terminal_offset={}, rows={}",
                num_queries,
                offsets[num_queries],
                total_vectors);
        }
        return make_empty_iterators(num_queries);
    }
    return this->index_.AnnIterator(dataset, conf, bitset, false, op_context);
}

template <typename T>
const bool
VectorDiskAnnIndex<T>::HasRawData() const {
    const auto& offset_mapping = GetOffsetMapping();
    if (IsAllNullNullable(offset_mapping) || IsEmptyEmbListIndex()) {
        return true;
    }
    return index_.HasRawData(GetMetricType());
}

template <typename T>
bool
VectorDiskAnnIndex<T>::IsIndexRefineEnabled() const {
    const auto& offset_mapping = GetOffsetMapping();
    if (IsAllNullNullable(offset_mapping) || IsEmptyEmbListIndex()) {
        return false;
    }
    return index_.IsIndexRefineEnabled();
}

template <typename T>
std::vector<uint8_t>
VectorDiskAnnIndex<T>::GetVector(const DatasetPtr dataset) const {
    auto index_type = GetIndexType();
    if (IndexIsSparse(index_type)) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, index is sparse");
    }

    // if dataset is empty, return empty vector
    if (dataset->GetRows() == 0) {
        return {};
    }

    auto res = index_.GetVectorByIds(dataset);
    if (!res.has_value()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to get vector: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    return this->template DecodeVectorByIdsResult<T>(res.value());
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
VectorDiskAnnIndex<T>::GetEmbListByIds(const DatasetPtr dataset,
                                       const std::string& metric_type) const {
    if (dataset->GetRows() == 0) {
        return {{}, {0}};
    }
    if (IsEmptyEmbListIndex()) {
        auto ids = dataset->GetIds();
        auto rows = dataset->GetRows();
        auto emb_list_count =
            static_cast<int64_t>(empty_emb_list_offsets_.size()) - 1;
        for (int64_t i = 0; i < rows; ++i) {
            AssertInfo(ids[i] >= 0 && ids[i] < emb_list_count,
                       "emb list id {} out of range {}",
                       ids[i],
                       emb_list_count);
        }
        return {{}, std::vector<size_t>(rows + 1, 0)};
    }

    auto res = index_.GetEmbListByIds(dataset, metric_type);
    if (!res.has_value()) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to get emb list: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    return this->template DecodeEmbListByIdsResult<T>(res.value());
}

template <typename T>
void
VectorDiskAnnIndex<T>::CleanLocalData() {
    file_manager_->RemoveIndexFiles();
    file_manager_->RemoveRawDataFiles();
}

template <typename T>
inline knowhere::Json
VectorDiskAnnIndex<T>::update_load_json(const Config& config) {
    knowhere::Json load_config;
    load_config.update(config);

    // set data path
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    load_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        // set base info
        load_config[DISK_ANN_PREPARE_WARM_UP] = false;
        load_config[DISK_ANN_PREPARE_USE_BFS_CACHE] = false;

        // set threads number
        auto num_threads = GetValueFromConfig<std::string>(
            load_config, DISK_ANN_LOAD_THREAD_NUM);
        AssertInfo(num_threads.has_value(),
                   "param {} is empty",
                   DISK_ANN_LOAD_THREAD_NUM);
        load_config[DISK_ANN_THREADS_NUM] =
            std::atoi(num_threads.value().c_str());

        // update search_beamwidth
        auto beamwidth = GetValueFromConfig<std::string>(
            load_config, DISK_ANN_QUERY_BEAMWIDTH);
        if (beamwidth.has_value()) {
            search_beamwidth_ = std::atoi(beamwidth.value().c_str());
        }
    }

    if (config.contains(MMAP_FILE_PATH)) {
        load_config.erase(MMAP_FILE_PATH);
        load_config[ENABLE_MMAP] = true;
    }

    return load_config;
}

template <typename T>
knowhere::expected<knowhere::DataSetPtr>
VectorDiskAnnIndex<T>::CalcDistByIDs(const knowhere::DataSetPtr query_dataset,
                                     const BitsetView& bitset,
                                     const int64_t* labels,
                                     size_t labels_len,
                                     bool is_cosine,
                                     milvus::OpContext* op_context) const {
    return index_.CalcDistByIDs(
        query_dataset, bitset, labels, labels_len, is_cosine, op_context);
}

template class VectorDiskAnnIndex<float>;
template class VectorDiskAnnIndex<float16>;
template class VectorDiskAnnIndex<bfloat16>;
template class VectorDiskAnnIndex<bin1>;
template class VectorDiskAnnIndex<sparse_u32_f32>;
template class VectorDiskAnnIndex<int8>;

}  // namespace milvus::index
