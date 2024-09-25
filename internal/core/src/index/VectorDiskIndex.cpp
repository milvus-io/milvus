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

#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "config/ConfigKnowhere.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/Util.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"
#include "indexbuilder/types.h"

namespace milvus::index {

#define kSearchListMaxValue1 200    // used if tok <= 20
#define kSearchListMaxValue2 65535  // used for topk > 20
#define kPrepareDim 100
#define kPrepareRows 1

template <typename T>
VectorDiskAnnIndex<T>::VectorDiskAnnIndex(
    const IndexType& index_type,
    const MetricType& metric_type,
    const IndexVersion& version,
    const storage::FileManagerContext& file_manager_context)
    : VectorIndex(index_type, metric_type) {
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
        knowhere::Pack(std::shared_ptr<knowhere::FileManager>(file_manager_));
    auto get_index_obj = knowhere::IndexFactory::Instance().Create<T>(
        GetIndexType(), version, diskann_index_pack);
    if (get_index_obj.has_value()) {
        index_ = get_index_obj.value();
    } else {
        auto err = get_index_obj.error();
        if (err == knowhere::Status::invalid_index_error) {
            PanicInfo(ErrorCode::Unsupported, get_index_obj.what());
        }
        PanicInfo(ErrorCode::KnowhereError, get_index_obj.what());
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
        auto read_scope =
            milvus::tracer::GetTracer()->WithActiveSpan(read_file_span);
        auto index_files =
            GetValueFromConfig<std::vector<std::string>>(config, "index_files");
        AssertInfo(index_files.has_value(),
                   "index file paths is empty when load disk ann index data");
        file_manager_->CacheIndexToDisk(index_files.value());
        read_file_span->End();
    }

    // start engine load index span
    auto span_load_engine =
        milvus::tracer::StartSpan("SegCoreEngineLoadDiskIndex", &ctx);
    auto engine_scope =
        milvus::tracer::GetTracer()->WithActiveSpan(span_load_engine);
    auto stat = index_.Deserialize(knowhere::BinarySet(), load_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to Deserialize index, " + KnowhereStatusString(stat));
    span_load_engine->End();

    SetDim(index_.Dim());
}

template <typename T>
BinarySet
VectorDiskAnnIndex<T>::Upload(const Config& config) {
    BinarySet ret;
    auto stat = index_.Serialize(ret);
    if (stat != knowhere::Status::success) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to serialize index, " + KnowhereStatusString(stat));
    }
    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

template <typename T>
void
VectorDiskAnnIndex<T>::Build(const Config& config) {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    knowhere::Json build_config;
    build_config.update(config);

    auto segment_id = file_manager_->GetFieldDataMeta().segment_id;
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build disk ann index");
    auto local_data_path =
        file_manager_->CacheRawDataToDisk<T>(insert_files.value());
    build_config[DISK_ANN_RAW_DATA_PATH] = local_data_path;

    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    build_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        auto num_threads = GetValueFromConfig<std::string>(
            build_config, DISK_ANN_BUILD_THREAD_NUM);
        AssertInfo(
            num_threads.has_value(),
            "param " + std::string(DISK_ANN_BUILD_THREAD_NUM) + "is empty");
        build_config[DISK_ANN_THREADS_NUM] =
            std::atoi(num_threads.value().c_str());
    }

    auto opt_fields = GetValueFromConfig<OptFieldT>(config, VEC_OPT_FIELDS);
    if (opt_fields.has_value() && index_.IsAdditionalScalarSupported()) {
        build_config[VEC_OPT_FIELDS_PATH] =
            file_manager_->CacheOptFieldToDisk(opt_fields.value());
        // `partition_key_isolation` is already in the config, so it falls through
        // into the index Build call directly
    }

    build_config.erase("insert_files");
    build_config.erase(VEC_OPT_FIELDS);
    auto stat = index_.Build({}, build_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to build disk index, " + KnowhereStatusString(stat));

    local_chunk_manager->RemoveDir(
        storage::GetSegmentRawDataPathPrefix(local_chunk_manager, segment_id));
}

template <typename T>
void
VectorDiskAnnIndex<T>::BuildWithDataset(const DatasetPtr& dataset,
                                        const Config& config) {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    knowhere::Json build_config;
    build_config.update(config);
    // set data path
    auto segment_id = file_manager_->GetFieldDataMeta().segment_id;
    auto field_id = file_manager_->GetFieldDataMeta().field_id;
    auto local_data_path = storage::GenFieldRawDataPathPrefix(
                               local_chunk_manager, segment_id, field_id) +
                           "raw_data";
    build_config[DISK_ANN_RAW_DATA_PATH] = local_data_path;

    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    build_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    if (GetIndexType() == knowhere::IndexEnum::INDEX_DISKANN) {
        auto num_threads = GetValueFromConfig<std::string>(
            build_config, DISK_ANN_BUILD_THREAD_NUM);
        AssertInfo(
            num_threads.has_value(),
            "param " + std::string(DISK_ANN_BUILD_THREAD_NUM) + "is empty");
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

    auto stat = index_.Build({}, build_config);
    if (stat != knowhere::Status::success)
        PanicInfo(ErrorCode::IndexBuildError,
                  "failed to build index, " + KnowhereStatusString(stat));
    local_chunk_manager->RemoveDir(
        storage::GetSegmentRawDataPathPrefix(local_chunk_manager, segment_id));

    // TODO ::
    // SetDim(index_->Dim());
}

template <typename T>
void
VectorDiskAnnIndex<T>::Query(const DatasetPtr dataset,
                             const SearchInfo& search_info,
                             const BitsetView& bitset,
                             SearchResult& search_result) const {
    AssertInfo(GetMetricType() == search_info.metric_type_,
               "Metric type of field index isn't the same with search info");
    auto num_queries = dataset->GetRows();
    auto topk = search_info.topk_;

    knowhere::Json search_config = PrepareSearchParams(search_info);

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
        auto radius =
            GetValueFromConfig<float>(search_info.search_params_, RADIUS);
        if (radius.has_value()) {
            search_config[RADIUS] = radius.value();
            // `range_search_k` is only used as one of the conditions for iterator early termination.
            // not gurantee to return exactly `range_search_k` results, which may be more or less.
            // set it to -1 will return all results in the range.
            search_config[knowhere::meta::RANGE_SEARCH_K] = topk;
            auto range_filter = GetValueFromConfig<float>(
                search_info.search_params_, RANGE_FILTER);
            if (range_filter.has_value()) {
                search_config[RANGE_FILTER] = range_filter.value();
                CheckRangeSearchParam(search_config[RADIUS],
                                      search_config[RANGE_FILTER],
                                      GetMetricType());
            }

            auto page_retain_order = GetValueFromConfig<bool>(
                search_info.search_params_, PAGE_RETAIN_ORDER);
            if (page_retain_order.has_value()) {
                search_config[knowhere::meta::RETAIN_ITERATOR_ORDER] =
                    page_retain_order.value();
            }

            auto res = index_.RangeSearch(dataset, search_config, bitset);

            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to range search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            return ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
        } else {
            auto res = index_.Search(dataset, search_config, bitset);
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to search: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            return res.value();
        }
    }();

    auto ids = final->GetIds();
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
    std::copy_n(ids, total_num, search_result.seg_offsets_.data());
    std::copy_n(distances, total_num, search_result.distances_.data());
}

template <typename T>
knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
VectorDiskAnnIndex<T>::VectorIterators(const DatasetPtr dataset,
                                       const knowhere::Json& conf,
                                       const BitsetView& bitset) const {
    return this->index_.AnnIterator(dataset, conf, bitset);
}

template <typename T>
const bool
VectorDiskAnnIndex<T>::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

template <typename T>
std::vector<uint8_t>
VectorDiskAnnIndex<T>::GetVector(const DatasetPtr dataset) const {
    auto index_type = GetIndexType();
    if (IndexIsSparse(index_type)) {
        PanicInfo(ErrorCode::UnexpectedError,
                  "failed to get vector, index is sparse");
    }
    auto res = index_.GetVectorByIds(dataset);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to get vector: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size = milvus::GetVecRowSize<T>(dim) * row_num;
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}

template <typename T>
void
VectorDiskAnnIndex<T>::CleanLocalData() {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    local_chunk_manager->RemoveDir(file_manager_->GetLocalIndexObjectPrefix());
    local_chunk_manager->RemoveDir(
        file_manager_->GetLocalRawDataObjectPrefix());
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
        AssertInfo(
            num_threads.has_value(),
            "param " + std::string(DISK_ANN_LOAD_THREAD_NUM) + "is empty");
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

template class VectorDiskAnnIndex<float>;
template class VectorDiskAnnIndex<float16>;
template class VectorDiskAnnIndex<bfloat16>;
template class VectorDiskAnnIndex<bin1>;

}  // namespace milvus::index
