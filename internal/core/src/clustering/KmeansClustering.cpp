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
#include "knowhere/cluster/cluster_factory.h"
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

KmeansClustering::KmeansClustering(
    const storage::FileManagerContext& file_manager_context) {
    file_manager_ =
        std::make_unique<storage::MemFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
}

template <typename T>
bool
KmeansClustering::FetchSegmentData(uint8_t* buf,
                                   const int64_t expected_train_size,
                                   const std::vector<std::string>& files,
                                   const int64_t num_rows,
                                   const int64_t dim,
                                   int64_t& offset) {
    auto field_datas = file_manager_->CacheRawDataToMemory(files);
    int64_t segment_size = 0;
    for (auto& data : field_datas) {
        segment_size += data->Size();
    }
    AssertInfo(segment_size == num_rows * sizeof(T) * dim,
               "file size consistent, expected: {}, actual: {}",
               num_rows * sizeof(T) * dim,
               segment_size);
    int64_t fetch_size = expected_train_size - offset;
    bool whole_segment_used = fetch_size >= segment_size;
    for (auto& data : field_datas) {
        size_t size = 0;
        if (whole_segment_used) {
            size = data->Size();
        } else {
            size = std::min(expected_train_size - offset, data->Size());
            if (size <= 0) {
                break;
            }
        }
        std::memcpy(buf + offset, data->Data(), size);
        offset += size;
        data.reset();
    }

    return whole_segment_used;
}

template <typename T>
int64_t
KmeansClustering::SampleTrainData(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const int64_t expected_train_size,
    const int64_t dim,
    uint8_t* buf) {
    int64_t trained_segments_num = 0;
    int64_t offset = 0;
    // segment_ids already shuffled, so just pick data by sequence
    for (auto i = 0; i < segment_ids.size(); i++) {
        if (offset == expected_train_size) {
            break;
        }
        int64_t cur_segment_id = segment_ids[i];
        bool whole_segment_used =
            FetchSegmentData<T>(buf,
                                expected_train_size,
                                segment_file_paths.at(cur_segment_id),
                                segment_num_rows.at(cur_segment_id),
                                dim,
                                offset);
        // if whole segment is used for training, we could directly get its vector -> centroid id mapping after kmeans training
        // so we record the num of segments to avoid recomputing id mapping results
        if (whole_segment_used) {
            trained_segments_num++;
        }
    }
    return trained_segments_num;
}

template <typename T>
milvus::proto::segcore::ClusteringCentroidsStats
KmeansClustering::CentroidsToPB(const T* centroids,
                                const int64_t num_clusters,
                                const int64_t dim) {
    milvus::proto::segcore::ClusteringCentroidsStats stats;
    for (auto i = 0; i < num_clusters; i++) {
        milvus::proto::schema::VectorField* vector_field =
            stats.add_centroids();
        vector_field->set_dim(dim);
        milvus::proto::schema::FloatArray* float_array =
            vector_field->mutable_float_vector();
        for (auto j = 0; j < dim; j++) {
            float_array->add_data(float(centroids[i * dim + j]));
        }
    }
    return stats;
}

std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
KmeansClustering::CentroidIdMappingToPB(
    const uint32_t* centroid_id_mapping,
    const std::vector<int64_t>& segment_ids,
    const int64_t trained_segments_num,
    const std::map<int64_t, int64_t>& num_row_map,
    const int64_t num_clusters) {
    auto compute_num_in_centroid = [&](const uint32_t* centroid_id_mapping,
                                       uint64_t start,
                                       uint64_t end) -> std::vector<int64_t> {
        std::vector<int64_t> num_vectors(num_clusters, 0);
        for (uint64_t i = start; i < end; ++i) {
            num_vectors[centroid_id_mapping[i]]++;
        }
        return num_vectors;
    };
    std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>
        stats_arr;
    int64_t cur_offset = 0;
    for (auto i = 0; i < trained_segments_num; i++) {
        milvus::proto::segcore::ClusteringCentroidIdMappingStats stats;
        auto num_offset = num_row_map.at(segment_ids[i]);
        for (auto j = 0; j < num_offset; j++) {
            stats.add_centroid_id_mapping(centroid_id_mapping[cur_offset + j]);
        }
        auto num_vectors = compute_num_in_centroid(
            centroid_id_mapping, cur_offset, cur_offset + num_offset);
        for (uint64_t j = 0; j < num_clusters; j++) {
            stats.add_num_in_centroid(num_vectors[j]);
        }
        cur_offset += num_offset;
        stats_arr.emplace_back(stats);
    }
    return stats_arr;
}

template <typename T>
void
KmeansClustering::StreamingAssignandUpload(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::segcore::ClusteringCentroidsStats& centroid_stats,
    const std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>&
        id_mapping_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const int64_t dim,
    const int64_t trained_segments_num,
    const int64_t num_cluster) {
    LOG_INFO("start upload");
    auto byte_size = centroid_stats.ByteSizeLong();
    std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(byte_size);
    centroid_stats.SerializeToArray(data.get(), byte_size);
    std::unordered_map<std::string, int64_t> remote_paths_to_size;
    LOG_INFO("start upload cluster centroids file");
    AddClusteringResultFiles(
        file_manager_->GetChunkManager().get(),
        data.get(),
        byte_size,
        GetRemoteCentroidsObjectPrefix() + "/" + std::string(CENTROIDS_NAME),
        remote_paths_to_size);
    cluster_result_.centroid_path =
        GetRemoteCentroidsObjectPrefix() + "/" + std::string(CENTROIDS_NAME);
    cluster_result_.centroid_file_size =
        remote_paths_to_size.at(cluster_result_.centroid_path);
    remote_paths_to_size.clear();
    LOG_INFO("upload cluster centroids file done");

    auto serializeIdMappingAndUpload =
        [&](const int64_t segment_id,
            const milvus::proto::segcore::ClusteringCentroidIdMappingStats&
                id_mapping_pb) {
            auto byte_size = id_mapping_pb.ByteSizeLong();
            std::unique_ptr<uint8_t[]> data =
                std::make_unique<uint8_t[]>(byte_size);
            id_mapping_pb.SerializeToArray(data.get(), byte_size);
            AddClusteringResultFiles(
                file_manager_->GetChunkManager().get(),
                data.get(),
                byte_size,
                GetRemoteCentroidIdMappingObjectPrefix(segment_id) + "/" +
                    std::string(OFFSET_MAPPING_NAME),
                remote_paths_to_size);
        };

    LOG_INFO("start upload cluster id mapping file");
    for (size_t i = 0; i < segment_ids.size(); i++) {
        int64_t segment_id = segment_ids[i];
        // id mapping has been computed, just upload to remote
        if (i < trained_segments_num) {
            serializeIdMappingAndUpload(segment_id, id_mapping_stats[i]);
        } else {  // streaming download raw data, assign id mapping, then upload
            int64_t num_row = num_rows.at(segment_id);
            std::unique_ptr<T[]> buf = std::make_unique<T[]>(num_row * dim);
            int64_t offset = 0;
            FetchSegmentData<T>(reinterpret_cast<uint8_t*>(buf.get()),
                                INT64_MAX,
                                insert_files.at(segment_id),
                                dim,
                                num_row,
                                offset);
            auto dataset = GenDataset(num_row, dim, buf.release());
            auto res = cluster_node.Assign(*dataset);
            if (!res.has_value()) {
                PanicInfo(ErrorCode::UnexpectedError,
                          fmt::format("failed to kmeans assign: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            res.value()->SetIsOwner(true);
            auto id_mapping =
                reinterpret_cast<const uint32_t*>(res.value()->GetTensor());

            auto id_mapping_pb = CentroidIdMappingToPB(
                id_mapping, segment_ids, 1, num_rows, num_cluster)[0];
            serializeIdMappingAndUpload(segment_id, id_mapping_pb);
        }
        LOG_INFO("upload segment {} cluster id mapping file done", segment_id);
    }
    LOG_INFO("upload cluster id mapping file done");
    cluster_result_.id_mappings = std::move(remote_paths_to_size);
    is_runned_ = true;
}

template <typename T>
void
KmeansClustering::Run(const Config& config) {
    auto insert_files = milvus::index::GetValueFromConfig<
        std::map<int64_t, std::vector<std::string>>>(config, "insert_files");
    if (!insert_files.has_value()) {
        throw SegcoreError(ErrorCode::ConfigInvalid,
                           "insert file path is empty when kmeans clustering");
    }
    auto num_rows =
        milvus::index::GetValueFromConfig<std::map<int64_t, int64_t>>(
            config, "num_rows");
    if (!num_rows.has_value()) {
        throw SegcoreError(ErrorCode::ConfigInvalid,
                           "num row is empty when kmeans clustering");
    }
    auto num_clusters =
        milvus::index::GetValueFromConfig<int64_t>(config, "num_clusters");
    if (!num_clusters.has_value()) {
        throw SegcoreError(ErrorCode::ConfigInvalid,
                           "num clusters is empty when kmeans clustering");
    }
    auto train_size =
        milvus::index::GetValueFromConfig<int64_t>(config, "train_size");
    if (!train_size.has_value()) {
        throw SegcoreError(ErrorCode::ConfigInvalid,
                           "train size is empty when kmeans clustering");
    }
    auto dim_config = milvus::index::GetValueFromConfig<int64_t>(config, "dim");
    if (!dim_config.has_value()) {
        throw SegcoreError(ErrorCode::ConfigInvalid,
                           "dim is empty when kmeans clustering");
    }
    size_t dim = dim_config.value();

    auto cluster_node_obj =
        knowhere::ClusterFactory::Instance().Create<T>(KMEANS_CLUSTER);
    knowhere::Cluster<knowhere::ClusterNode> cluster_node;
    if (cluster_node_obj.has_value()) {
        cluster_node = std::move(cluster_node_obj.value());
    } else {
        auto err = cluster_node_obj.error();
        if (err == knowhere::Status::invalid_cluster_error) {
            throw SegcoreError(ErrorCode::Unsupported, cluster_node_obj.what());
        }
        throw SegcoreError(ErrorCode::KnowhereError, cluster_node_obj.what());
    }

    size_t data_num = 0;
    std::vector<int64_t> segment_ids;
    for (auto& [segment_id, num_row_each_segment] : num_rows.value()) {
        data_num += num_row_each_segment;
        segment_ids.emplace_back(segment_id);
        AssertInfo(
            insert_files.value().find(segment_id) != insert_files.value().end(),
            "segment id {} not exist in insert files",
            segment_id);
    }
    // random shuffle for sampling
    std::shuffle(segment_ids.begin(), segment_ids.end(), std::mt19937());

    size_t data_size = data_num * dim * sizeof(T);
    std::vector<std::string> data_files;
    std::vector<uint64_t> offsets;

    size_t train_num = train_size.value() / sizeof(T) / dim;

    // make train num equal to data num
    if (train_num >= data_num) {
        train_num = data_num;
    }
    size_t train_size_final = train_num * dim * sizeof(T);

    // if data_num larger than max_train_size, we need to sample to make train data fits in memory
    // otherwise just load all the data for kmeans training
    LOG_INFO("pull and sample {}GB data out of {}GB data",
             train_size_final / 1024.0 / 1024.0 / 1024.0,
             data_size / 1024.0 / 1024.0 / 1024.0);
    auto buf = std::make_unique<uint8_t[]>(train_size_final);
    int64_t trained_segments_num = SampleTrainData<T>(segment_ids,
                                                      insert_files.value(),
                                                      num_rows.value(),
                                                      train_size_final,
                                                      dim,
                                                      buf.get());
    LOG_INFO("sample done");

    auto dataset = GenDataset(train_num, dim, buf.release());

    LOG_INFO("train data num: {}, dim: {}, num_clusters: {}",
             train_num,
             dim,
             num_clusters.value());
    knowhere::Json train_conf;
    train_conf[NUM_CLUSTERS] = num_clusters.value();
    // inside knowhere, we will record each kmeans iteration duration
    // return id mapping
    auto res = cluster_node.Train(*dataset, train_conf);
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

    auto centroids_res = cluster_node.GetCentroids();
    if (!centroids_res.has_value()) {
        PanicInfo(ErrorCode::UnexpectedError,
                  fmt::format("failed to get centroids: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    // centroids owned by cluster_node
    centroids_res.value()->SetIsOwner(false);
    auto centroids =
        reinterpret_cast<const T*>(centroids_res.value()->GetTensor());

    auto centroid_stats =
        CentroidsToPB<T>(centroids, num_clusters.value(), dim);
    auto id_mapping_stats = CentroidIdMappingToPB(centroid_id_mapping,
                                                  segment_ids,
                                                  trained_segments_num,
                                                  num_rows.value(),
                                                  num_clusters.value());
    // upload
    StreamingAssignandUpload<T>(cluster_node,
                                centroid_stats,
                                id_mapping_stats,
                                segment_ids,
                                insert_files.value(),
                                num_rows.value(),
                                dim,
                                trained_segments_num,
                                num_clusters.value());
}

template void
KmeansClustering::StreamingAssignandUpload<float>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::segcore::ClusteringCentroidsStats& centroid_stats,
    const std::vector<milvus::proto::segcore::ClusteringCentroidIdMappingStats>&
        id_mapping_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const int64_t dim,
    const int64_t trained_segments_num,
    const int64_t num_clusters);

template bool
KmeansClustering::FetchSegmentData<float>(uint8_t* buf,
                                          const int64_t expected_train_size,
                                          const std::vector<std::string>& files,
                                          const int64_t num_rows,
                                          const int64_t dim,
                                          int64_t& offset);
template int64_t
KmeansClustering::SampleTrainData<float>(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const int64_t expected_train_size,
    const int64_t dim,
    uint8_t* buf);

template void
KmeansClustering::Run<float>(const Config& config);

template milvus::proto::segcore::ClusteringCentroidsStats
KmeansClustering::CentroidsToPB<float>(const float* centroids,
                                       const int64_t num_clusters,
                                       const int64_t dim);

}  // namespace milvus::clustering
