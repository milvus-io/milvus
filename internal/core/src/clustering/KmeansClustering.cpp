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
#include "common/Utils.h"
#include "config/ConfigKnowhere.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/kmeans.h"
#include "clustering/KmeansClustering.h"
#include "segcore/SegcoreConfig.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/Util.h"
#include "common/Consts.h"
#include "common/RangeSearchHelper.h"
#include "clustering/types.h"
#include "clustering/file_utils.h"
#include <random>

namespace milvus::clustering {

template <typename T>
KmeansClustering<T>::KmeansClustering(
    const storage::FileManagerContext& file_manager_context) {
    file_manager_ =
        std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    auto cluster_node_obj =
        knowhere::ClusterFactory::Instance().Create<T>(KMEANS_CLUSTER);
    if (cluster_node_obj.has_value()) {
        cluster_node_ = std::move(cluster_node_obj.value());
    } else {
        auto err = cluster_node_obj.error();
        if (err == knowhere::Status::invalid_cluster_error) {
            throw SegcoreError(ErrorCode::Unsupported, cluster_node_obj.what());
        }
        throw SegcoreError(ErrorCode::KnowhereError, cluster_node_obj.what());
    }
}

template <typename T>
bool
KmeansClustering<T>::FetchSegmentData(uint8_t* buf,
                                      int64_t expected_train_size,
                                      const std::vector<std::string>& files,
                                      const int64_t num_rows,
                                      int64_t& offset) {
    auto field_datas = file_manager_->CacheRawDataToMemory(files);
    int64_t segment_size = 0;
    for (auto& data : field_datas) {
        segment_size += data->Size();
    }
    AssertInfo(segment_size == num_rows * sizeof(T) * dim_,
               "file size consistent, expected: {}, actual: {}",
               num_rows * sizeof(T) * dim_,
               segment_size);
    int64_t fetch_size = expected_train_size - offset;
    bool use_all_segment = fetch_size >= segment_size;
    for (auto& data : field_datas) {
        auto size = std::min(expected_train_size - offset, data->Size());
        if (size <= 0) {
            break;
        }
        std::memcpy(buf + offset, data->Data(), size);
        offset += size;
        data.reset();
    }
    std::vector<FieldDataPtr>().swap(field_datas);
    return use_all_segment;
}

template <typename T>
std::unique_ptr<uint8_t[]>
KmeansClustering<T>::SampleTrainData(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    int64_t expected_train_size) {
    // random sampling to get train data
    std::vector<int32_t> idx(segment_ids.size());
    std::iota(idx.begin(), idx.end(), 0);
    std::shuffle(idx.begin(), idx.end(), std::mt19937());

    int64_t sample_train_num = 0;
    std::unique_ptr<uint8_t[]> buf =
        std::make_unique<uint8_t[]>(expected_train_size);
    int64_t offset = 0;
    for (auto i = 0; i < segment_ids.size(); i++) {
        if (offset == expected_train_size) {
            break;
        }
        int64_t cur_segment_id = segment_ids[idx[i]];
        bool all_segment_used =
            FetchSegmentData(buf.get(),
                             expected_train_size,
                             segment_file_paths.at(cur_segment_id),
                             segment_num_rows.at(cur_segment_id),
                             offset);
        if (all_segment_used) {
            trained_segmentid_to_offset_[cur_segment_id] = i;
            trained_segment_ids_.push_back(cur_segment_id);
        }
    }
    return buf;
}

template <typename T>
milvus::proto::segcore::ClusteringCentroidsStats
KmeansClustering<T>::CentroidsToPB(const T* centroids) {
    milvus::proto::segcore::ClusteringCentroidsStats stats;
    for (int i = 0; i < num_clusters_; i++) {
        milvus::proto::schema::VectorField* vector_field =
            stats.add_centroids();
        vector_field->set_dim(dim_);
        milvus::proto::schema::FloatArray* float_array =
            vector_field->mutable_float_vector();
        for (int j = 0; j < dim_; j++) {
            float_array->add_data(float(centroids[i * dim_ + j]));
        }
    }
    return stats;
}

template <typename T>
std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
KmeansClustering<T>::CentroidIdMappingToPB(
    const uint32_t* centroid_id_mapping,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, int64_t>& num_row_map) {
    auto compute_num_in_centroid = [&](const uint32_t* centroid_id_mapping,
                                       uint64_t start,
                                       uint64_t end) -> std::vector<int64_t> {
        std::vector<int64_t> num_vectors(num_clusters_, 0);
        for (uint64_t i = start; i < end; ++i) {
            num_vectors[centroid_id_mapping[i]]++;
        }
        return num_vectors;
    };
    std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
        stats_arr;
    int64_t cur_offset = 0;
    for (auto segment_id : segment_ids) {
        milvus::proto::segcore::ClusteringCentroidIdMappingStats stats;
        auto num_offset = num_row_map.at(segment_id);
        for (auto j = 0; j < num_offset; j++) {
            stats.add_centroid_id_mapping(centroid_id_mapping[cur_offset + j]);
        }
        auto num_vectors = compute_num_in_centroid(
            centroid_id_mapping, cur_offset, cur_offset + num_offset);
        for (uint64_t j = 0; j < num_clusters_; j++) {
            stats.add_num_in_centroid(num_vectors[j]);
        }
        cur_offset += num_offset;
        stats_arr.emplace_back(stats);
    }
    return stats_arr;
}

template <typename T>
void
KmeansClustering<T>::StreamingAssignandUpload(
    const T* centroids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows) {
    LOG_INFO("start upload");
    int byte_size = centroid_stats_.ByteSizeLong();
    std::shared_ptr<uint8_t[]> data =
        std::shared_ptr<uint8_t[]>(new uint8_t[byte_size]);
    centroid_stats_.SerializeToArray(data.get(), byte_size);
    BinarySet ret;
    ret.Append(std::string(CENTROIDS_NAME), data, byte_size);
    std::unordered_map<std::string, int64_t> remote_paths_to_size;
    LOG_INFO("start upload cluster centroids file");
    AddClusteringResultFiles(file_manager_->GetChunkManager().get(),
                             ret,
                             GetRemoteCentroidsObjectPrefix(),
                             remote_paths_to_size);
    cluster_result_.centroid_path =
        GetRemoteCentroidsObjectPrefix() + "/" + std::string(CENTROIDS_NAME);
    cluster_result_.centroid_file_size =
        remote_paths_to_size.at(cluster_result_.centroid_path);
    remote_paths_to_size.clear();
    LOG_INFO("upload cluster centroids file done");

    auto serializeIdMappingAndUpload =
        [&](const int64_t segment_id,
            milvus::proto::segcore::ClusteringCentroidIdMappingStats&
                id_mapping_pb) {
            BinarySet ret;
            auto byte_size = id_mapping_pb.ByteSizeLong();
            std::shared_ptr<uint8_t[]> data =
                std::shared_ptr<uint8_t[]>(new uint8_t[byte_size]);
            id_mapping_pb.SerializeToArray(data.get(), byte_size);
            ret.Append(std::string(OFFSET_MAPPING_NAME), data, byte_size);
            AddClusteringResultFiles(
                file_manager_->GetChunkManager().get(),
                ret,
                GetRemoteCentroidIdMappingObjectPrefix(segment_id),
                remote_paths_to_size);
        };

    LOG_INFO("start upload cluster id mapping file");
    for (auto& [segment_id, segment_files] : insert_files) {
        // id mapping has been computed, just upload to remote
        if (trained_segmentid_to_offset_.find(segment_id) !=
            trained_segmentid_to_offset_.end()) {
            serializeIdMappingAndUpload(
                segment_id,
                id_mapping_stats_[trained_segmentid_to_offset_.at(segment_id)]);
        } else {  // streaming download raw data, assign id mapping, then upload
            int64_t num_row = num_rows.at(segment_id);
            std::unique_ptr<T[]> buf = std::make_unique<T[]>(num_row * dim_);
            int64_t offset = 0;
            FetchSegmentData(reinterpret_cast<uint8_t*>(buf.get()),
                             INT64_MAX,
                             segment_files,
                             num_row,
                             offset);
            auto dataset = GenDataset(num_row, dim_, buf.release());
            auto res = cluster_node_.Assign(*dataset);
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to kmeans assign: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            res.value()->SetIsOwner(true);
            auto id_mapping =
                reinterpret_cast<const uint32_t*>(res.value()->GetTensor());

            auto id_mapping_pb =
                CentroidIdMappingToPB(id_mapping, {segment_id}, num_rows)[0];
            serializeIdMappingAndUpload(segment_id, id_mapping_pb);
        }
        LOG_INFO("upload segment {} cluster id mapping file done", segment_id);
    }
    LOG_INFO("upload cluster id mapping file done");
    cluster_result_.id_mappings = std::move(remote_paths_to_size);
}

template <typename T>
void
KmeansClustering<T>::Run(const Config& config) {
    auto insert_files = milvus::index::GetValueFromConfig<
        std::map<int64_t, std::vector<std::string>>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file path is empty when kmeans clustering");
    auto num_rows =
        milvus::index::GetValueFromConfig<std::map<int64_t, int64_t>>(
            config, "num_rows");
    AssertInfo(num_rows.has_value(), "num row is empty when kmeans clustering");
    AssertInfo(num_rows.value().size() == insert_files.value().size(),
               "insert files and segment rows not consistent");
    auto segment_size =
        milvus::index::GetValueFromConfig<int64_t>(config, "segment_size");
    AssertInfo(segment_size.has_value(),
               "segment size is empty when kmeans clustering");
    auto train_size =
        milvus::index::GetValueFromConfig<int64_t>(config, "train_size");
    AssertInfo(train_size.has_value(),
               "train size is empty when kmeans clustering");
    auto dim = milvus::index::GetValueFromConfig<int64_t>(config, "dim");
    AssertInfo(dim.has_value(), "dim is empty when kmeans clustering");
    dim_ = dim.value();

    auto data_num = 0;
    std::vector<int64_t> segment_ids;
    for (auto& [segment_id, num_row_each_segment] : num_rows.value()) {
        data_num += num_row_each_segment;
        segment_ids.emplace_back(segment_id);
        AssertInfo(
            insert_files.value().find(segment_id) != insert_files.value().end(),
            "segment id {} not exist in insert files",
            segment_id);
    }

    auto data_size = data_num * dim_ * sizeof(T);
    std::vector<std::string> data_files;
    std::vector<uint64_t> offsets;

    auto train_num = train_size.value() / sizeof(T) / dim_;

    // make train num equal to data num
    if (train_num >= data_num) {
        train_num = data_num;
    }
    auto train_size_final = train_num * dim_ * sizeof(T);

    // if data_num larger than max_train_size, we need to sample to make train data fits in memory
    // otherwise just load all the data for kmeans training
    LOG_INFO("pull and sample {}GB data out of {}GB data",
             train_size_final / 1024.0 / 1024.0 / 1024.0,
             data_size / 1024.0 / 1024.0 / 1024.0);
    auto buf = SampleTrainData(
        segment_ids, insert_files.value(), num_rows.value(), train_size_final);
    LOG_INFO("sample done");

    auto dataset = GenDataset(train_num, dim_, buf.release());

    // get num of clusters by whole data size / segment size
    num_clusters_ = DIV_ROUND_UP(data_size, segment_size.value());
    LOG_INFO("train data num: {}, dim: {}, num_clusters: {}",
             train_num,
             dim_,
             num_clusters_);
    knowhere::Json train_conf;
    train_conf[NUM_CLUSTERS] = num_clusters_;
    // inside knowhere, we will record each kmeans iteration duration
    // return id mapping
    auto res = cluster_node_.Train(*dataset, train_conf);
    if (!res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to kmeans train: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    res.value()->SetIsOwner(true);
    LOG_INFO("kmeans clustering done");
    dataset.reset();  // release train data

    auto centroid_id_mapping =
        reinterpret_cast<const uint32_t*>(res.value()->GetTensor());

    auto centroids_res = cluster_node_.GetCentroids();
    if (!centroids_res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to get centroids: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    // centroids owned by cluster_node_
    centroids_res.value()->SetIsOwner(false);
    auto centroids =
        reinterpret_cast<const T*>(centroids_res.value()->GetTensor());

    centroid_stats_ = CentroidsToPB(centroids);
    id_mapping_stats_ = CentroidIdMappingToPB(
        centroid_id_mapping, trained_segment_ids_, num_rows.value());

    // centroids will be used for knowhere assign
    StreamingAssignandUpload(centroids, insert_files.value(), num_rows.value());
}

template class KmeansClustering<float>;

}  // namespace milvus::clustering
