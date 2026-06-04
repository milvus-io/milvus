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
#include "common/FastMem.h"
#include "clustering/KmeansClustering.h"
#include "common/Tracer.h"
#include "common/Utils.h"
#include "config/ConfigKnowhere.h"
#include "knowhere/cluster/cluster_factory.h"
#include "knowhere/comp/time_recorder.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "clustering/file_utils.h"
#include "storage/ThreadPools.h"

namespace milvus::clustering {

KmeansClustering::KmeansClustering(
    const storage::FileManagerContext& file_manager_context) {
    file_manager_ =
        std::make_unique<storage::MemFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    int64_t collection_id = file_manager_context.fieldDataMeta.collection_id;
    int64_t partition_id = file_manager_context.fieldDataMeta.partition_id;
    msg_header_ = fmt::format(
        "collection: {}, partition: {} ", collection_id, partition_id);
}

template <typename T>
size_t
KmeansClustering::FetchDataFiles(uint8_t* buf, const DatasetPart& part) {
    if (part.files.empty() || part.max_bytes <= 0) {
        return 0;
    }
    size_t fetched = 0;
    auto field_datas = file_manager_->CacheRawDataToMemory(part.storage_config);
    for (auto& data : field_datas) {
        size_t current_size =
            std::min((size_t)part.max_bytes - fetched, (size_t)data->Size());
        std::memcpy(buf + fetched, data->Data(), current_size);
        data.reset();
        fetched += current_size;
        if (fetched >= part.max_bytes) {
            break;
        }
    }
    return fetched;
}

template <typename T>
milvus::proto::clustering::ClusteringCentroidsStats
KmeansClustering::CentroidsToPB(const T* centroids,
                                const int64_t num_clusters,
                                const int64_t dim) {
    milvus::proto::clustering::ClusteringCentroidsStats stats;
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

std::vector<milvus::proto::clustering::ClusteringCentroidIdMappingStats>
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
    std::vector<milvus::proto::clustering::ClusteringCentroidIdMappingStats>
        stats_arr;
    stats_arr.reserve(trained_segments_num);
    int64_t cur_offset = 0;
    for (auto i = 0; i < trained_segments_num; i++) {
        milvus::proto::clustering::ClusteringCentroidIdMappingStats stats;
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
bool
KmeansClustering::IsDataSkew(
    const milvus::proto::clustering::AnalyzeInfo& config,
    const int64_t dim,
    std::vector<int64_t>& num_in_each_centroid) {
    auto min_cluster_ratio = config.min_cluster_ratio();
    auto max_cluster_ratio = config.max_cluster_ratio();
    auto max_cluster_size = config.max_cluster_size();
    std::sort(num_in_each_centroid.begin(), num_in_each_centroid.end());
    size_t avg_size = std::accumulate(num_in_each_centroid.begin(),
                                      num_in_each_centroid.end(),
                                      size_t{0}) /
                      (num_in_each_centroid.size());
    if (num_in_each_centroid.front() <= min_cluster_ratio * avg_size) {
        LOG_INFO(msg_header_ + "minimum cluster too small: {}, avg: {}",
                 num_in_each_centroid.front(),
                 avg_size);
        return true;
    }
    if (num_in_each_centroid.back() >= max_cluster_ratio * avg_size) {
        LOG_INFO(msg_header_ + "maximum cluster too large: {}, avg: {}",
                 num_in_each_centroid.back(),
                 avg_size);
        return true;
    }
    if (num_in_each_centroid.back() * dim * sizeof(T) >= max_cluster_size) {
        LOG_INFO(msg_header_ + "maximum cluster size too large: {}B",
                 num_in_each_centroid.back() * dim * sizeof(T));
        return true;
    }
    return false;
}

template <typename T>
void
KmeansClustering::StreamingAssignandUpload(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<
        milvus::proto::clustering::ClusteringCentroidIdMappingStats>&
        id_mapping_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const int64_t dim,
    const int64_t trained_segments_num,
    const int64_t num_clusters,
    const std::map<std::string, int64_t>& file_sizes_map) {
    // =========================
    // Upload centroids (serial)
    // =========================
    auto byte_size = centroid_stats.ByteSizeLong();
    std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(byte_size);
    centroid_stats.SerializeToArray(data.get(), byte_size);
    std::unordered_map<std::string, int64_t> remote_paths_to_size;
    LOG_INFO(msg_header_ + "start upload cluster centroids file");

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

    LOG_INFO(msg_header_ + "upload cluster centroids file done");
    LOG_INFO(msg_header_ + "start upload cluster id mapping file");
    std::vector<int64_t> num_vectors_each_centroid(num_clusters, 0);

    auto serializeIdMappingAndUpload = [&](int64_t segment_id,
                                           const milvus::proto::clustering::
                                               ClusteringCentroidIdMappingStats&
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

        LOG_INFO(
            msg_header_ +
                "upload segment {} cluster id mapping file with size {} B done",
            segment_id,
            byte_size);
    };

    // ===================================
    // Handle trained segments (serial)
    // ===================================
    for (size_t i = 0; i < trained_segments_num; i++) {
        int64_t segment_id = segment_ids[i];
        serializeIdMappingAndUpload(segment_id, id_mapping_stats[i]);

        for (int64_t j = 0; j < num_clusters; ++j) {
            num_vectors_each_centroid[j] +=
                id_mapping_stats[i].num_in_centroid(j);
        }
    }

    // ===================================
    // Parallel assignment for new segments
    // ===================================

    struct AssignResult {
        int64_t segment_id;
        milvus::proto::clustering::ClusteringCentroidIdMappingStats pb;
        std::vector<int64_t> centroid_counts;
    };

    auto& pool = ThreadPools::GetThreadPool(ThreadPoolPriority::LOW);

    std::vector<std::future<AssignResult>> futures;

    std::mutex assign_mutex;

    size_t total_buffer = config.assign_buffer_size();
    size_t num_threads = std::max<size_t>(pool.GetThreadNum(), 1);
    num_threads = std::min<size_t>(num_threads,
                                   segment_ids.size() - trained_segments_num);
    if (num_threads > 0) {
        size_t per_thread_buffer =
            std::max<size_t>(total_buffer / num_threads, 1 << 20);

        for (size_t i = trained_segments_num; i < segment_ids.size(); i++) {
            int64_t segment_id = segment_ids[i];

            futures.emplace_back(pool.Submit([&, segment_id]() -> AssignResult {
                size_t segment_rows =
                    static_cast<size_t>(num_rows.at(segment_id));
                size_t segment_size = segment_rows * dim * sizeof(T);

                size_t assign_buffer_size =
                    std::min(segment_size, per_thread_buffer);

                auto buf = std::make_unique<uint8_t[]>(assign_buffer_size);

                DatasetIterator<T> assign_iter(DatasetPurpose::ASSIGNMENT,
                                               {segment_id},
                                               insert_files,
                                               num_rows,
                                               file_sizes_map,
                                               dim,
                                               assign_buffer_size,
                                               INT64_MAX,
                                               config.storage_version(),
                                               false);

                std::vector<uint32_t> all_ids;
                all_ids.reserve(segment_rows);

                std::vector<int64_t> local_centroid_counts(num_clusters, 0);

                while (assign_iter.HasNext()) {
                    auto part = assign_iter.Next();
                    size_t fetched = FetchDataFiles<T>(buf.get(), part);

                    if (fetched == 0) {
                        assign_iter.notifyEOF();
                        continue;
                    }

                    auto dataset =
                        GenDataset(fetched / sizeof(T) / dim, dim, buf.get());

                    knowhere::expected<knowhere::DataSetPtr> res;

                    {
                        std::lock_guard<std::mutex> lock(assign_mutex);
                        res = cluster_node.Assign(*dataset);
                    }

                    if (!res.has_value())
                        ThrowInfo(ErrorCode::UnexpectedError,
                                  "failed to kmeans assign");

                    const uint32_t* id_mapping =
                        reinterpret_cast<const uint32_t*>(
                            res.value()->GetTensor());

                    for (int64_t r = 0; r < res.value()->GetRows(); r++) {
                        uint32_t cid = id_mapping[r];
                        all_ids.push_back(cid);
                        local_centroid_counts[cid]++;
                    }

                    LOG_INFO(msg_header_ +
                                 "Assigned {} rows out of {} for segment {}",
                             all_ids.size(),
                             segment_rows,
                             segment_id);
                }

                AssertInfo(all_ids.size() == segment_rows,
                           "assigned rows {} must equal segment rows {}",
                           all_ids.size(),
                           segment_rows);

                auto pb = CentroidIdMappingToPB(
                    all_ids.data(), {segment_id}, 1, num_rows, num_clusters)[0];

                return AssignResult{segment_id,
                                    std::move(pb),
                                    std::move(local_centroid_counts)};
            }));
        }

        // ===================================
        // Collect results (serial merge)
        // ===================================
        for (auto& future : futures) {
            auto result = future.get();

            for (int j = 0; j < num_clusters; ++j) {
                num_vectors_each_centroid[j] += result.centroid_counts[j];

                LOG_INFO(
                    msg_header_ + "After segment {} cluster {} has {} vectors",
                    result.segment_id,
                    j,
                    num_vectors_each_centroid[j]);
            }

            serializeIdMappingAndUpload(result.segment_id, result.pb);
        }
    }
    // ===================================
    // Skew check
    // ===================================
    if (IsDataSkew<T>(config, dim, num_vectors_each_centroid))
        throw SegcoreError(ErrorCode::ClusterSkip,
                           "data skew! skip clustering");

    cluster_result_.id_mappings = std::move(remote_paths_to_size);
    is_runned_ = true;
}

template <typename T>
void
KmeansClustering::Run(const milvus::proto::clustering::AnalyzeInfo& config) {
    std::map<int64_t, std::vector<std::string>> insert_files;
    std::map<int64_t, size_t> segment_num_files;
    if (config.storage_version() != STORAGE_V1) {
        for (auto& [segment_id, segment_insert_files] :
             config.segment_insert_files()) {
            milvus::SegmentInsertFiles groups =
                get_segment_insert_files(segment_insert_files);
            std::vector<std::string> current_segment_files;
            for (int i = 0; i < groups.size(); i++) {
                std::vector<std::string>& group_files = groups[i];
                current_segment_files.insert(current_segment_files.end(),
                                             group_files.begin(),
                                             group_files.end());
            }
            insert_files[segment_id] = current_segment_files;
            segment_num_files[segment_id] = current_segment_files.size();
        }
    } else {
        for (const auto& pair : config.insert_files()) {
            std::vector<std::string> segment_files(
                pair.second.insert_files().begin(),
                pair.second.insert_files().end());
            insert_files[pair.first] = segment_files;
        }
    }
    size_t num_input_segments = insert_files.size();
    std::map<int64_t, int64_t> num_rows(config.num_rows().begin(),
                                        config.num_rows().end());
    auto num_clusters = config.num_clusters();
    AssertInfo(num_clusters > 0, "num clusters must larger than 0");
    auto train_size = config.train_size();
    AssertInfo(train_size > 0, "train size must larger than 0");
    auto dim = config.dim();
    auto min_cluster_ratio = config.min_cluster_ratio();
    AssertInfo(min_cluster_ratio > 0 && min_cluster_ratio < 1,
               "min cluster ratio must larger than 0, less than 1");
    auto max_cluster_ratio = config.max_cluster_ratio();
    AssertInfo(max_cluster_ratio > 1, "max cluster ratio must larger than 1");
    auto max_cluster_size = config.max_cluster_size();
    AssertInfo(max_cluster_size > 0, "max cluster size must larger than 0");

    auto cluster_node_obj =
        knowhere::ClusterFactory::Instance().Create<T>(KMEANS_CLUSTER);
    knowhere::Cluster<knowhere::ClusterNode> cluster_node;
    if (cluster_node_obj.has_value()) {
        cluster_node = std::move(cluster_node_obj.value());
    } else {
        auto err = cluster_node_obj.error();
        if (err == knowhere::Status::invalid_cluster_error) {
            throw SegcoreError(ErrorCode::ClusterSkip, cluster_node_obj.what());
        }
        throw SegcoreError(ErrorCode::KnowhereError, cluster_node_obj.what());
    }

    size_t data_num = 0;
    std::vector<int64_t> segment_ids;
    for (auto& [segment_id, num_row_each_segment] : num_rows) {
        data_num += num_row_each_segment;
        segment_ids.emplace_back(segment_id);
    }
    std::map<std::string, int64_t> file_sizes_map;
    size_t trained_segments_num = 0;
    size_t data_size = data_num * dim * sizeof(T);
    size_t train_buffer_size =
        std::min(data_size, (size_t)config.train_buffer_size());
    AssertInfo(train_buffer_size > 0, "train buffer must be larger than 0");
    size_t train_num = train_size / sizeof(T) / dim;
    if (train_num < num_clusters) {
        LOG_WARN(msg_header_ +
                     "kmeans train num: {} less than num_clusters: {}, skip "
                     "clustering",
                 train_num,
                 num_clusters);
        throw SegcoreError(ErrorCode::ClusterSkip,
                           "sample data num less than num clusters");
    }

    for (auto& [segment_id, segment_files] : insert_files) {
        for (auto& segment_file : segment_files) {
            float segment_file_part =
                (float)1.0 /
                ((float)segment_ids.size() * (float)segment_files.size());
            size_t segment_file_size =
                config.storage_version() != STORAGE_V1
                    ? (size_t)(segment_file_part * (float)train_size)
                    : file_manager_->GetChunkManager()->Size(segment_file);
            if (config.storage_version() != STORAGE_V1) {
                //set a minimum of 32M to allow parallel fetch per file
                segment_file_size =
                    std::max(segment_file_size,
                             (size_t)DEFAULT_INDEX_FILE_SLICE_SIZE * 2);
            }
            LOG_DEBUG(msg_header_ + "File {} part is {} and size is  {}",
                      segment_file,
                      segment_file_part,
                      segment_file_size);
            file_sizes_map[segment_file] = segment_file_size;
        }
    }
    if (train_num < num_clusters) {
        LOG_WARN(msg_header_ +
                     "kmeans train num: {} less than num_clusters: {}, skip "
                     "clustering",
                 train_num,
                 num_clusters);
        throw SegcoreError(ErrorCode::ClusterSkip,
                           "sample data num less than num clusters");
    }
    bool random_sample = true;
    if (train_num >= data_num) {
        train_num = data_num;
        random_sample = false;
        if (train_buffer_size >= data_size) {
            trained_segments_num = segment_ids.size();
        }
    }
    DatasetIterator<T> train_iter(DatasetPurpose::TRAIN,
                                  segment_ids,
                                  insert_files,
                                  num_rows,
                                  file_sizes_map,
                                  dim,
                                  train_buffer_size,
                                  train_num * dim * sizeof(T),
                                  config.storage_version(),
                                  random_sample);
    knowhere::TimeRecorder rc(msg_header_ + "kmeans clustering",
                              2 /* log level: info */);
    auto buf = std::make_unique<uint8_t[]>(train_buffer_size);
    std::vector<uint32_t> train_assignments;
    size_t fetched_so_far = 0, fetched;
    auto train_once = [&](size_t bytes) {
        auto dataset = GenDataset(bytes / sizeof(T) / dim, dim, buf.get());
        auto res = cluster_node.Train(
            *dataset, knowhere::Json{{NUM_CLUSTERS, num_clusters}});
        if (!res.has_value()) {
            ThrowInfo(ErrorCode::UnexpectedError, "failed to kmeans train");
        }

        if (trained_segments_num > 0) {  // full dataset train
            auto centroid_id_mapping =
                reinterpret_cast<const uint32_t*>(res.value()->GetTensor());
            for (int64_t i = 0; i < res.value()->GetRows(); i++) {
                train_assignments.push_back(centroid_id_mapping[i]);
            }
        }
    };
    LOG_DEBUG(
        msg_header_ + "Start training with {} train buffer size on {} vectors",
        train_buffer_size,
        train_num);
    while (train_iter.HasNext()) {
        auto part = train_iter.Next();
        LOG_DEBUG(msg_header_ +
                      "Next iteration with {} max_bytes anf fetched_so_far {}",
                  part.max_bytes,
                  fetched_so_far);
        if (part.max_bytes + fetched_so_far <= train_buffer_size) {
            fetched = FetchDataFiles<T>(buf.get() + fetched_so_far, part);
            if (fetched == 0) {
                train_iter.notifyEOF();
            } else if (fetched < part.max_bytes) {
                train_iter.increaseRemaining(part.max_bytes - fetched);
            }
            fetched_so_far += fetched;
            continue;
        }
        train_once(fetched_so_far);
        fetched_so_far = FetchDataFiles<T>(buf.get(), part);
        if (fetched_so_far == 0) {
            train_iter.notifyEOF();
        } else if (fetched_so_far < part.max_bytes) {
            train_iter.increaseRemaining(part.max_bytes - fetched_so_far);
        }
    }
    //clear last one
    if (fetched_so_far > 0) {
        train_once(fetched_so_far);
    }
    rc.RecordSection("training done");
    buf.reset();
    auto centroids_res = cluster_node.GetCentroids();
    centroids_res.value()->SetIsOwner(false);
    auto centroids =
        reinterpret_cast<const T*>(centroids_res.value()->GetTensor());
    auto centroid_stats = CentroidsToPB<T>(centroids, num_clusters, dim);

    auto id_mapping_stats = CentroidIdMappingToPB(train_assignments.data(),
                                                  segment_ids,
                                                  trained_segments_num,
                                                  num_rows,
                                                  num_clusters);

    StreamingAssignandUpload<T>(cluster_node,
                                config,
                                centroid_stats,
                                id_mapping_stats,
                                segment_ids,
                                insert_files,
                                num_rows,
                                dim,
                                trained_segments_num,
                                num_clusters,
                                file_sizes_map);
    rc.RecordSection("clustering result upload done");
    rc.ElapseFromBegin("clustering done");
}

template void
KmeansClustering::Run<float>(
    const milvus::proto::clustering::AnalyzeInfo& config);
template size_t
KmeansClustering::FetchDataFiles<float>(uint8_t*, const DatasetPart&);
template milvus::proto::clustering::ClusteringCentroidsStats
KmeansClustering::CentroidsToPB<float>(const float*,
                                       const int64_t,
                                       const int64_t);
template bool
KmeansClustering::IsDataSkew<float>(
    const milvus::proto::clustering::AnalyzeInfo&,
    const int64_t,
    std::vector<int64_t>&);
template void
KmeansClustering::KmeansClustering::StreamingAssignandUpload<float>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<
        milvus::proto::clustering::ClusteringCentroidIdMappingStats>&
        id_mapping_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const int64_t dim,
    const int64_t trained_segments_num,
    const int64_t num_clusters,
    const std::map<std::string, int64_t>& file_sizes_map);

}  // namespace milvus::clustering
