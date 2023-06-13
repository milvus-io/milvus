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

#include "common/Utils.h"
#include "config/ConfigKnowhere.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "storage/LocalChunkManager.h"
#include "storage/Util.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"

namespace milvus::index {

#ifdef BUILD_DISK_ANN

#define kSearchListMaxValue1 200    // used if tok <= 20
#define kSearchListMaxValue2 65535  // used for topk > 20
#define kPrepareDim 100
#define kPrepareRows 1

template <typename T>
VectorDiskAnnIndex<T>::VectorDiskAnnIndex(
    const IndexType& index_type,
    const MetricType& metric_type,
    storage::FileManagerImplPtr file_manager)
    : VectorIndex(index_type, metric_type) {
    file_manager_ =
        std::dynamic_pointer_cast<storage::DiskFileManagerImpl>(file_manager);
    auto& local_chunk_manager = storage::LocalChunkManager::GetInstance();
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();

    // As we have guarded dup-load in QueryNode,
    // this assertion failed only if the Milvus rebooted in the same pod,
    // need to remove these files then re-load the segment
    if (local_chunk_manager.Exist(local_index_path_prefix)) {
        local_chunk_manager.RemoveDir(local_index_path_prefix);
    }

    local_chunk_manager.CreateDir(local_index_path_prefix);
    auto diskann_index_pack =
        knowhere::Pack(std::shared_ptr<knowhere::FileManager>(file_manager));
    index_ = knowhere::IndexFactory::Instance().Create(GetIndexType(),
                                                       diskann_index_pack);
}

template <typename T>
void
VectorDiskAnnIndex<T>::Load(const BinarySet& binary_set /* not used */,
                            const Config& config) {
    knowhere::Json load_config = update_load_json(config);

    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load disk ann index data");
    file_manager_->CacheIndexToDisk(index_files.value());

    // todo : replace by index::load function later
    knowhere::DataSetPtr qs = std::make_unique<knowhere::DataSet>();
    qs->SetRows(kPrepareRows);
    qs->SetDim(kPrepareDim);
    qs->SetIsOwner(true);
    auto query = new T[kPrepareRows * kPrepareDim];
    qs->SetTensor(query);
    index_.Search(*qs, load_config, nullptr);

    SetDim(index_.Dim());
}

template <typename T>
void
VectorDiskAnnIndex<T>::BuildWithDataset(const DatasetPtr& dataset,
                                        const Config& config) {
    auto& local_chunk_manager = storage::LocalChunkManager::GetInstance();
    knowhere::Json build_config;
    build_config.update(config);
    // set data path
    auto segment_id = file_manager_->GetFileDataMeta().segment_id;
    auto field_id = file_manager_->GetFileDataMeta().field_id;
    auto local_data_path =
        storage::GenFieldRawDataPathPrefix(segment_id, field_id) + "raw_data";
    build_config[DISK_ANN_RAW_DATA_PATH] = local_data_path;

    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    build_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    auto num_threads = GetValueFromConfig<std::string>(
        build_config, DISK_ANN_BUILD_THREAD_NUM);
    AssertInfo(num_threads.has_value(),
               "param " + std::string(DISK_ANN_BUILD_THREAD_NUM) + "is empty");
    build_config[DISK_ANN_THREADS_NUM] = std::atoi(num_threads.value().c_str());

    if (!local_chunk_manager.Exist(local_data_path)) {
        local_chunk_manager.CreateFile(local_data_path);
    }

    int64_t offset = 0;
    auto num = uint32_t(milvus::GetDatasetRows(dataset));
    local_chunk_manager.Write(local_data_path, offset, &num, sizeof(num));
    offset += sizeof(num);

    auto dim = uint32_t(milvus::GetDatasetDim(dataset));
    local_chunk_manager.Write(local_data_path, offset, &dim, sizeof(dim));
    offset += sizeof(dim);

    auto data_size = num * dim * sizeof(float);
    auto raw_data = const_cast<void*>(milvus::GetDatasetTensor(dataset));
    local_chunk_manager.Write(local_data_path, offset, raw_data, data_size);

    knowhere::DataSet* ds_ptr = nullptr;
    index_.Build(*ds_ptr, build_config);

    local_chunk_manager.RemoveDir(
        storage::GetSegmentRawDataPathPrefix(segment_id));
    // TODO ::
    // SetDim(index_->Dim());
}

template <typename T>
std::unique_ptr<SearchResult>
VectorDiskAnnIndex<T>::Query(const DatasetPtr dataset,
                             const SearchInfo& search_info,
                             const BitsetView& bitset) {
    AssertInfo(GetMetricType() == search_info.metric_type_,
               "Metric type of field index isn't the same with search info");
    auto num_queries = dataset->GetRows();
    auto topk = search_info.topk_;

    knowhere::Json search_config = search_info.search_params_;

    search_config[knowhere::meta::TOPK] = topk;
    search_config[knowhere::meta::METRIC_TYPE] = GetMetricType();

    // set search list size
    auto search_list_size = GetValueFromConfig<uint32_t>(
        search_info.search_params_, DISK_ANN_QUERY_LIST);
    AssertInfo(search_list_size.has_value(),
               "param " + std::string(DISK_ANN_QUERY_LIST) + "is empty");
    AssertInfo(search_list_size.value() >= topk,
               "search_list should be greater than or equal to topk");
    AssertInfo(
        search_list_size.value() <=
                std::max(uint32_t(topk * 10), uint32_t(kSearchListMaxValue1)) &&
            search_list_size.value() <= uint32_t(kSearchListMaxValue2),
        "search_list should be less than max(topk*10, 200) and less than "
        "65535");
    search_config[DISK_ANN_SEARCH_LIST_SIZE] = search_list_size.value();

    // set beamwidth
    search_config[DISK_ANN_QUERY_BEAMWIDTH] = int(search_beamwidth_);

    // set index prefix, will be removed later
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    search_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    // set json reset field, will be removed later
    search_config[DISK_ANN_PQ_CODE_BUDGET] = 0.0;

    auto final = [&] {
        auto radius =
            GetValueFromConfig<float>(search_info.search_params_, RADIUS);
        if (radius.has_value()) {
            search_config[RADIUS] = radius.value();
            auto range_filter = GetValueFromConfig<float>(
                search_info.search_params_, RANGE_FILTER);
            if (range_filter.has_value()) {
                search_config[RANGE_FILTER] = range_filter.value();
                CheckRangeSearchParam(search_config[RADIUS],
                                      search_config[RANGE_FILTER],
                                      GetMetricType());
            }
            auto res = index_.RangeSearch(*dataset, search_config, bitset);

            if (!res.has_value()) {
                PanicCodeInfo(ErrorCodeEnum::UnexpectedError,
                              "failed to range search, " +
                                  MatchKnowhereError(res.error()));
            }
            return ReGenRangeSearchResult(
                res.value(), topk, num_queries, GetMetricType());
        } else {
            auto res = index_.Search(*dataset, search_config, bitset);
            if (!res.has_value()) {
                PanicCodeInfo(
                    ErrorCodeEnum::UnexpectedError,
                    "failed to search, " + MatchKnowhereError(res.error()));
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
    auto result = std::make_unique<SearchResult>();
    result->seg_offsets_.resize(total_num);
    result->distances_.resize(total_num);
    result->total_nq_ = num_queries;
    result->unity_topK_ = topk;

    std::copy_n(ids, total_num, result->seg_offsets_.data());
    std::copy_n(distances, total_num, result->distances_.data());

    return result;
}

template <typename T>
const bool
VectorDiskAnnIndex<T>::HasRawData() const {
    return index_.HasRawData(GetMetricType());
}

template <typename T>
const std::vector<uint8_t>
VectorDiskAnnIndex<T>::GetVector(const DatasetPtr dataset) const {
    auto res = index_.GetVectorByIds(*dataset);
    if (!res.has_value()) {
        PanicCodeInfo(
            ErrorCodeEnum::UnexpectedError,
            "failed to get vector, " + MatchKnowhereError(res.error()));
    }
    auto index_type = GetIndexType();
    auto tensor = res.value()->GetTensor();
    auto row_num = res.value()->GetRows();
    auto dim = res.value()->GetDim();
    int64_t data_size;
    if (is_in_bin_list(index_type)) {
        data_size = dim / 8 * row_num;
    } else {
        data_size = dim * row_num * sizeof(float);
    }
    std::vector<uint8_t> raw_data;
    raw_data.resize(data_size);
    memcpy(raw_data.data(), tensor, data_size);
    return raw_data;
}

template <typename T>
void
VectorDiskAnnIndex<T>::CleanLocalData() {
    auto& local_chunk_manager = storage::LocalChunkManager::GetInstance();
    local_chunk_manager.RemoveDir(file_manager_->GetLocalIndexObjectPrefix());
    local_chunk_manager.RemoveDir(file_manager_->GetLocalRawDataObjectPrefix());
}

template <typename T>
inline knowhere::Json
VectorDiskAnnIndex<T>::update_load_json(const Config& config) {
    knowhere::Json load_config;
    load_config.update(config);

    // set data path
    auto local_index_path_prefix = file_manager_->GetLocalIndexObjectPrefix();
    load_config[DISK_ANN_PREFIX_PATH] = local_index_path_prefix;

    // set base info
    load_config[DISK_ANN_PREPARE_WARM_UP] = false;
    load_config[DISK_ANN_PREPARE_USE_BFS_CACHE] = false;

    // set threads number
    auto num_threads =
        GetValueFromConfig<std::string>(load_config, DISK_ANN_LOAD_THREAD_NUM);
    AssertInfo(num_threads.has_value(),
               "param " + std::string(DISK_ANN_LOAD_THREAD_NUM) + "is empty");
    load_config[DISK_ANN_THREADS_NUM] = std::atoi(num_threads.value().c_str());

    // update search_beamwidth
    auto beamwidth =
        GetValueFromConfig<std::string>(load_config, DISK_ANN_QUERY_BEAMWIDTH);
    if (beamwidth.has_value()) {
        search_beamwidth_ = std::atoi(beamwidth.value().c_str());
    }

    return load_config;
}

template class VectorDiskAnnIndex<float>;

#endif

}  // namespace milvus::index
