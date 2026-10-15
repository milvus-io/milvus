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

#include <string.h>
#include <algorithm>

#include "common/FastMem.h"
#include <cmath>
#include <cstdint>
#include <numeric>
#include <random>
#include <type_traits>
#include <utility>

#include "clustering/KmeansClustering.h"
#include "clustering/file_utils.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "knowhere/cluster/cluster.h"
#include "knowhere/cluster/cluster_factory.h"
#include "knowhere/cluster/cluster_node.h"
#include "knowhere/comp/time_recorder.h"
#include "knowhere/config.h"
#include "knowhere/expected.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/schema.pb.h"
#include "storage/Util.h"
#include <limits>

namespace milvus::clustering {

KmeansClustering::KmeansClustering(
    const storage::FileManagerContext& file_manager_context) {
    loon_ffi_properties_ = file_manager_context.loon_ffi_properties;
    file_manager_ =
        std::make_unique<storage::MemFileManagerImpl>(file_manager_context);
    AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    int64_t collection_id = file_manager_context.fieldDataMeta.collection_id;
    int64_t partition_id = file_manager_context.fieldDataMeta.partition_id;
    msg_header_ = fmt::format(
        "collection: {}, partition: {} ", collection_id, partition_id);
}

// Copy complete decoded vectors, rejecting both short and oversized input.
static int64_t
CopyFieldDatasToBuf(uint8_t* buf,
                    int64_t max_bytes,
                    std::vector<FieldDataPtr>& field_datas,
                    int64_t row_bytes,
                    int64_t& destination_offset) {
    int64_t fetched = 0;
    for (auto& data : field_datas) {
        const auto size = data->Size();
        if (size % row_bytes != 0 || size > max_bytes - fetched) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "analyze input has incomplete vectors or exceeds its row "
                      "count");
        }
        milvus::fastmem::FastMemcpy(
            buf + destination_offset, data->Data(), size);
        fetched += size;
        destination_offset += size;
        data.reset();
    }
    return fetched;
}

template <typename T>
void
KmeansClustering::FetchDataFiles(uint8_t* buf,
                                 const int64_t expected_rows,
                                 const int64_t row_offset,
                                 const std::vector<std::string>& files,
                                 const int64_t dim,
                                 int64_t& offset) {
    AssertInfo(row_offset == 0, "binlog analyze reads complete segments");
    const int64_t expected_bytes = expected_rows * dim * sizeof(T);
    int64_t fetched = 0;
    const auto batch =
        std::max<size_t>(1, DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    for (size_t start = 0; start < files.size(); start += batch) {
        const auto end = std::min(files.size(), start + batch);
        Config config;
        config[INSERT_FILES_KEY] = std::vector<std::string>(
            files.begin() + start, files.begin() + end);
        auto field_datas = file_manager_->CacheRawDataToMemory(config);
        fetched += CopyFieldDatasToBuf(buf,
                                       expected_bytes - fetched,
                                       field_datas,
                                       dim * sizeof(T),
                                       offset);
    }
    if (fetched != expected_bytes) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "analyze input bytes mismatch: expected {}, got {}",
                  expected_bytes,
                  fetched);
    }
}

template <typename T>
void
KmeansClustering::FetchSegmentViaManifest(uint8_t* buf,
                                          const int64_t expected_rows,
                                          const int64_t row_offset,
                                          const std::string& manifest_path,
                                          const Config& base_config,
                                          const int64_t dim,
                                          int64_t& offset) {
    AssertInfo(row_offset == 0, "manifest analyze reads complete segments");
    AssertInfo(loon_ffi_properties_ != nullptr,
               "missing manifest storage properties");
    const int64_t expected_bytes = expected_rows * dim * sizeof(T);
    int64_t fetched = 0;
    // Stream the entire field once. Limiting the reader to metadata's row count
    // would silently accept a manifest containing additional physical rows.
    storage::IterateFieldDataFromManifest(
        manifest_path,
        loon_ffi_properties_,
        file_manager_->GetFieldDataMeta(),
        base_config.at(DATA_TYPE_KEY).get<DataType>(),
        dim,
        base_config.at(ELEMENT_TYPE_KEY).get<DataType>(),
        std::nullopt,
        [&](FieldDataPtr data) {
            std::vector<FieldDataPtr> batch{std::move(data)};
            fetched += CopyFieldDatasToBuf(
                buf, expected_bytes - fetched, batch, dim * sizeof(T), offset);
        });
    if (fetched != expected_bytes) {
        ThrowInfo(ErrorCode::DataFormatBroken,
                  "manifest input bytes mismatch: expected {}, got {}",
                  expected_bytes,
                  fetched);
    }
}

template <typename T>
void
KmeansClustering::SampleTrainData(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const int64_t expected_train_size,
    const int64_t dim,
    const bool random_sample,
    std::mt19937_64& rng,
    uint8_t* buf) {
    AssertInfo(
        dim > 0 && dim <= std::numeric_limits<int64_t>::max() / sizeof(T),
        "invalid vector dimension");
    const int64_t bytes_per_row = dim * sizeof(T);
    AssertInfo(
        expected_train_size >= 0 && expected_train_size % bytes_per_row == 0,
        "training size must contain complete vectors");
    const auto target_sample_rows = expected_train_size / bytes_per_row;
    int64_t total_rows = 0;
    for (const auto segment_id : segment_ids) {
        const auto rows = segment_num_rows.at(segment_id);
        AssertInfo(rows >= 0 &&
                       total_rows <= std::numeric_limits<int64_t>::max() - rows,
                   "invalid segment row count");
        total_rows += rows;
    }
    AssertInfo(target_sample_rows <= total_rows,
               "training budget exceeds input row count");
    AssertInfo(random_sample || target_sample_rows == total_rows,
               "full training must include every input row");
    if (target_sample_rows == 0) {
        return;
    }

    int64_t offset = 0;
    int64_t seen_rows = 0;
    // A single reservoir spans every segment and every decoded batch. After
    // N rows, each source row has probability target_sample_rows / N of being
    // retained. Segment size and physical file boundaries do not affect it.
    auto sample_batch = [&](std::vector<FieldDataPtr>& field_datas) {
        int64_t batch_rows = 0;
        for (auto& data : field_datas) {
            const auto data_size = static_cast<int64_t>(data->Size());
            if (data_size % bytes_per_row != 0) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "sample input does not contain complete vectors");
            }
            const auto rows = data_size / bytes_per_row;
            if (rows > total_rows - seen_rows) {
                ThrowInfo(ErrorCode::DataFormatBroken,
                          "sample input exceeds expected row count");
            }
            const auto* source = reinterpret_cast<const uint8_t*>(data->Data());
            for (int64_t row = 0; row < rows; ++row, ++seen_rows) {
                int64_t slot = seen_rows;
                if (seen_rows >= target_sample_rows) {
                    slot = std::uniform_int_distribution<int64_t>(
                        0, seen_rows)(rng);
                }
                if (slot < target_sample_rows) {
                    milvus::fastmem::FastMemcpy(buf + slot * bytes_per_row,
                                                source + row * bytes_per_row,
                                                bytes_per_row);
                }
            }
            batch_rows += rows;
            data.reset();
        }
        return batch_rows;
    };

    for (const auto cur_segment_id : segment_ids) {
        const auto rows = segment_num_rows.at(cur_segment_id);
        if (rows == 0) {
            continue;
        }

        auto mit = manifest_paths.find(cur_segment_id);
        int64_t fetched_rows = 0;
        if (mit != manifest_paths.end() && !mit->second.empty()) {
            if (!random_sample) {
                FetchSegmentViaManifest<T>(
                    buf, rows, 0, mit->second, base_config, dim, offset);
                continue;
            }
            AssertInfo(loon_ffi_properties_ != nullptr,
                       "missing manifest storage properties");
            storage::IterateFieldDataFromManifest(
                mit->second,
                loon_ffi_properties_,
                file_manager_->GetFieldDataMeta(),
                base_config.at(DATA_TYPE_KEY).get<DataType>(),
                dim,
                base_config.at(ELEMENT_TYPE_KEY).get<DataType>(),
                std::nullopt,
                [&](FieldDataPtr data) {
                    std::vector<FieldDataPtr> batch{std::move(data)};
                    fetched_rows += sample_batch(batch);
                });
        } else {
            auto files = segment_file_paths.at(cur_segment_id);
            storage::SortByPath(files);
            if (!random_sample) {
                FetchDataFiles<T>(buf, rows, 0, files, dim, offset);
                continue;
            }
            const auto batch =
                size_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
            for (size_t start = 0; start < files.size(); start += batch) {
                const auto end = std::min(files.size(), start + batch);
                Config config;
                config[INSERT_FILES_KEY] = std::vector<std::string>(
                    files.begin() + start, files.begin() + end);
                auto field_datas = file_manager_->CacheRawDataToMemory(config);
                fetched_rows += sample_batch(field_datas);
            }
        }
        if (fetched_rows != rows) {
            ThrowInfo(ErrorCode::DataFormatBroken,
                      "segment {} sample input row count inconsistent, "
                      "expected: {}, actual: {}",
                      cur_segment_id,
                      rows,
                      fetched_rows);
        }
    }
    AssertInfo(
        random_sample ? seen_rows == total_rows : offset == expected_train_size,
        "sampled data size differs from training budget");
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
        const auto* centroid = centroids + i * dim;
        for (int64_t j = 0; j < dim; ++j) {
            AssertInfo(std::isfinite(static_cast<float>(centroid[j])),
                       "non-finite centroid value");
        }
        if constexpr (std::is_same_v<T, float>) {
            auto* values = vector_field->mutable_float_vector();
            for (int64_t j = 0; j < dim; ++j) values->add_data(centroid[j]);
        } else if constexpr (std::is_same_v<T, knowhere::fp16>) {
            vector_field->set_float16_vector(centroid, dim * sizeof(T));
        } else {
            static_assert(std::is_same_v<T, knowhere::bf16>);
            vector_field->set_bfloat16_vector(centroid, dim * sizeof(T));
        }
    }
    return stats;
}

milvus::proto::clustering::ClusteringCentroidIdMappingStats
KmeansClustering::CentroidIdMappingWithDistanceToPB(
    const int64_t* centroid_id_mapping,
    const float* distances,
    const int64_t num_rows,
    const int64_t num_clusters) {
    AssertInfo(num_rows == 0 || centroid_id_mapping != nullptr,
               "cluster assignment returned no centroid ids");
    AssertInfo(num_rows == 0 || distances != nullptr,
               "cluster assignment returned no distances");
    AssertInfo(num_rows >= 0, "assignment row count must not be negative");
    AssertInfo(num_clusters > 0, "assignment centroid count must be positive");

    milvus::proto::clustering::ClusteringCentroidIdMappingStats stats;
    std::vector<int64_t> num_vectors(num_clusters, 0);
    for (int64_t row = 0; row < num_rows; ++row) {
        const auto centroid_id = static_cast<int64_t>(centroid_id_mapping[row]);
        AssertInfo(centroid_id >= 0 && centroid_id < num_clusters,
                   "centroid id {} out of range [0, {})",
                   centroid_id,
                   num_clusters);

        const auto distance = distances[row];
        AssertInfo(std::isfinite(distance) && distance >= 0.0f,
                   "invalid distance {} for row {} assigned to centroid {}",
                   distance,
                   row,
                   centroid_id);

        stats.add_centroid_id_mapping(static_cast<uint32_t>(centroid_id));
        stats.add_distance_to_centroid(distance);
        num_vectors[centroid_id]++;
    }
    for (const auto count : num_vectors) {
        stats.add_num_in_centroid(count);
    }
    return stats;
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
                                      int64_t{0}) /
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
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const T* full_training_data,
    const int64_t num_clusters) {
    auto byte_size = centroid_stats.ByteSizeLong();
    std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(byte_size);
    AssertInfo(byte_size <= std::numeric_limits<int>::max() &&
                   centroid_stats.SerializeToArray(data.get(), byte_size),
               "centroid result exceeds protobuf serialization limit");
    std::unordered_map<std::string, int64_t> remote_paths_to_size;
    LOG_INFO(msg_header_ + "start upload cluster centroids file");
    auto centroid_remote_path =
        GetRemoteCentroidsObjectPrefix() + "/" + std::string(CENTROIDS_NAME);
    AddClusteringResultFiles(file_manager_->GetChunkManager().get(),
                             data.get(),
                             byte_size,
                             centroid_remote_path,
                             remote_paths_to_size);
    cluster_result_.centroid_path = std::move(centroid_remote_path);
    cluster_result_.centroid_file_size =
        remote_paths_to_size.at(cluster_result_.centroid_path);
    remote_paths_to_size.clear();
    LOG_INFO(msg_header_ + "upload cluster centroids file done");

    LOG_INFO(msg_header_ + "start upload cluster id mapping file");
    std::vector<int64_t> num_vectors_each_centroid(num_clusters, 0);

    auto serializeIdMappingAndUpload = [&](const int64_t segment_id,
                                           const milvus::proto::clustering::
                                               ClusteringCentroidIdMappingStats&
                                                   id_mapping_pb) {
        auto byte_size = id_mapping_pb.ByteSizeLong();
        std::unique_ptr<uint8_t[]> data =
            std::make_unique<uint8_t[]>(byte_size);
        AssertInfo(byte_size <= std::numeric_limits<int>::max() &&
                       id_mapping_pb.SerializeToArray(data.get(), byte_size),
                   "assignment result exceeds protobuf serialization limit");
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

    int64_t full_data_row_offset = 0;
    for (const auto segment_id : segment_ids) {
        const auto num_row = num_rows.at(segment_id);
        if (num_row == 0) {
            serializeIdMappingAndUpload(segment_id,
                                        CentroidIdMappingWithDistanceToPB(
                                            nullptr, nullptr, 0, num_clusters));
            continue;
        }
        knowhere::DataSetPtr dataset;
        if (full_training_data != nullptr) {
            // Full training data is in segment_ids order. Borrow only this
            // segment's rows; the caller retains the owning training dataset.
            dataset = GenDataset(
                num_row, dim, full_training_data + full_data_row_offset * dim);
            dataset->SetIsOwner(false);
            full_data_row_offset += num_row;
        } else {
            std::unique_ptr<T[]> buf = std::make_unique<T[]>(num_row * dim);
            int64_t offset = 0;
            auto mit = manifest_paths.find(segment_id);
            if (mit != manifest_paths.end() && !mit->second.empty()) {
                FetchSegmentViaManifest<T>(
                    reinterpret_cast<uint8_t*>(buf.get()),
                    num_row,
                    0,
                    mit->second,
                    base_config,
                    dim,
                    offset);
            } else {
                FetchDataFiles<T>(reinterpret_cast<uint8_t*>(buf.get()),
                                  num_row,
                                  0,
                                  insert_files.at(segment_id),
                                  dim,
                                  offset);
            }
            dataset = GenDataset(num_row, dim, buf.release());
            dataset->SetIsOwner(true);
        }
        // Sampling affects training only. Every segment gets a complete
        // assignment and its own row-aligned ID/distance artifact.
        auto res = cluster_node.AssignWithDistance(*dataset, cluster_config);
        if (!res.has_value()) {
            ThrowInfo(KnowhereStatusToErrorCode(res.error()),
                      fmt::format("failed to assign with distance: {}: {}",
                                  KnowhereStatusString(res.error()),
                                  res.what()));
        }
        AssertInfo(res.value() != nullptr, "assignment returned no dataset");
        AssertInfo(res.value()->GetRows() == num_row,
                   "cluster assignment returned {} rows, expected {}",
                   res.value()->GetRows(),
                   num_row);
        const auto* ids = res.value()->GetIds();
        const auto* distances = res.value()->GetDistance();
        auto id_mapping_pb = CentroidIdMappingWithDistanceToPB(
            ids, distances, num_row, num_clusters);
        for (int64_t j = 0; j < num_clusters; ++j) {
            num_vectors_each_centroid[j] += id_mapping_pb.num_in_centroid(j);
        }
        serializeIdMappingAndUpload(segment_id, id_mapping_pb);
    }
    if (IsDataSkew<T>(config, dim, num_vectors_each_centroid)) {
        LOG_INFO(msg_header_ + "data skew! skip clustering");
        // skip clustering, nothing takes affect
        throw SegcoreError(ErrorCode::ClusterSkip,
                           "data skew! skip clustering");
    }
    LOG_INFO(msg_header_ + "upload cluster id mapping file done");
    cluster_result_.id_mappings = std::move(remote_paths_to_size);
    is_runned_ = true;
}

template <typename T>
void
KmeansClustering::Run(const milvus::proto::clustering::AnalyzeInfo& config) {
    // A failed rerun must not expose the previous run's result or partial files.
    is_runned_ = false;
    cluster_result_ = {};
    std::map<int64_t, std::vector<std::string>> insert_files;
    for (const auto& pair : config.insert_files()) {
        std::vector<std::string> segment_files(
            pair.second.insert_files().begin(),
            pair.second.insert_files().end());
        // Sort once before splitting into decode batches. Mapping must consume
        // the same numeric log-ID order, including for sampled training.
        storage::SortByPath(segment_files);
        insert_files[pair.first] = std::move(segment_files);
    }

    std::map<int64_t, std::string> manifest_paths(
        config.manifest_paths().begin(), config.manifest_paths().end());

    // Share the manifest reader and preserve the configured vector element width.
    Config base_config;
    base_config[STORAGE_VERSION_KEY] = int64_t(STORAGE_V3);
    base_config[DATA_TYPE_KEY] =
        static_cast<DataType>(config.field_schema().data_type());
    base_config[ELEMENT_TYPE_KEY] =
        static_cast<DataType>(config.field_schema().element_type());
    base_config[DIM_KEY] = config.dim();

    std::map<int64_t, int64_t> num_rows(config.num_rows().begin(),
                                        config.num_rows().end());
    auto num_clusters = config.num_clusters();
    AssertInfo(num_clusters > 0, "num clusters must larger than 0");
    auto train_size = config.train_size();
    AssertInfo(train_size > 0, "train size must larger than 0");
    auto dim = config.dim();
    AssertInfo(
        dim > 0 && dim <= std::numeric_limits<int64_t>::max() / sizeof(T),
        "invalid analyze dimension");
    const int64_t row_bytes = dim * sizeof(T);
    AssertInfo(num_clusters <= std::numeric_limits<uint32_t>::max(),
               "too many centroids");
    AssertInfo(config.min_cluster_ratio() > 0 &&
                   config.min_cluster_ratio() < 1 &&
                   config.max_cluster_ratio() > 1,
               "invalid clustering skew limits");
    auto max_cluster_size = config.max_cluster_size();
    AssertInfo(max_cluster_size > 0, "max cluster size must larger than 0");

    const auto cluster_type = config.cluster_type().empty()
                                  ? std::string(KMEANS_CLUSTER)
                                  : config.cluster_type();
    auto cluster_node_obj =
        knowhere::ClusterFactory::Instance().Create<T>(cluster_type);
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
        AssertInfo(
            num_row_each_segment >= 0 &&
                num_row_each_segment <=
                    std::numeric_limits<int64_t>::max() / row_bytes - data_num,
            "analyze input size overflows int64");
        data_num += num_row_each_segment;
        segment_ids.emplace_back(segment_id);
        AssertInfo(insert_files.find(segment_id) != insert_files.end() ||
                       manifest_paths.find(segment_id) != manifest_paths.end(),
                   "segment id {} has neither insert files nor manifest",
                   segment_id);
    }

    size_t data_size = data_num * dim * sizeof(T);
    size_t train_num = train_size / sizeof(T) / dim;
    bool random_sample = true;
    // make train num equal to data num
    if (train_num >= data_num) {
        train_num = data_num;
        random_sample =
            false;  // all data are used for training, no need to random sampling
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

    size_t train_size_final = train_num * dim * sizeof(T);
    knowhere::TimeRecorder rc(msg_header_ + "kmeans clustering",
                              2 /* log level: info */);
    // if data_num larger than max_train_size, we need to sample to make train data fits in memory
    // otherwise just load all the data for kmeans training
    LOG_INFO(msg_header_ + "pull and sample {}GB data out of {}GB data",
             train_size_final / 1024.0 / 1024.0 / 1024.0,
             data_size / 1024.0 / 1024.0 / 1024.0);
    auto buf = std::make_unique<T[]>(train_num * dim);
    std::mt19937_64 rng(std::random_device{}());
    SampleTrainData<T>(segment_ids,
                       insert_files,
                       num_rows,
                       manifest_paths,
                       base_config,
                       train_size_final,
                       dim,
                       random_sample,
                       rng,
                       reinterpret_cast<uint8_t*>(buf.get()));
    rc.RecordSection("sample done");

    auto dataset = GenDataset(train_num, dim, buf.release());
    dataset->SetIsOwner(true);

    LOG_INFO(msg_header_ + "train data num: {}, dim: {}, num_clusters: {}",
             train_num,
             dim,
             num_clusters);
    knowhere::Json cluster_config;
    for (const auto& [key, value] : config.cluster_params()) {
        cluster_config[key] = value;
    }
    // Preserve the provider's metric default. Explicit field metric support,
    // including COSINE normalization, is tracked in the plan's G2 TODO.
    cluster_config[DIM_KEY] = dim;
    // num_clusters is computed by Milvus and cannot be overridden by opaque
    // Knowhere parameters.
    cluster_config[NUM_CLUSTERS] = num_clusters;
    // inside knowhere, we will record each kmeans iteration duration
    // return id mapping
    auto res = cluster_node.Train(*dataset, cluster_config);
    if (!res.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(res.error()),
                  fmt::format("failed to kmeans train: {}: {}",
                              KnowhereStatusString(res.error()),
                              res.what()));
    }
    rc.RecordSection("clustering train done");

    auto centroid_stats = [&] {
        auto centroids_res = cluster_node.GetCentroids();
        if (!centroids_res.has_value()) {
            ThrowInfo(KnowhereStatusToErrorCode(centroids_res.error()),
                      fmt::format("failed to get centroids: {}: {}",
                                  KnowhereStatusString(centroids_res.error()),
                                  centroids_res.what()));
        }
        // Preserve the returned dataset's ownership. The protobuf owns a copy,
        // so release the exported dataset before streaming assignment.
        AssertInfo(centroids_res.value() != nullptr &&
                       centroids_res.value()->GetRows() == num_clusters &&
                       centroids_res.value()->GetDim() == dim &&
                       centroids_res.value()->GetTensor() != nullptr,
                   "invalid centroid dataset shape");
        const auto* centroids =
            static_cast<const T*>(centroids_res.value()->GetTensor());
        return CentroidsToPB<T>(centroids, num_clusters, dim);
    }();
    if (random_sample) {
        // Sampled rows are not a complete segment: release them before loading
        // full segments. Otherwise reuse the resident data without re-reading.
        dataset.reset();
    }
    // upload
    StreamingAssignandUpload<T>(
        cluster_node,
        config,
        centroid_stats,
        segment_ids,
        insert_files,
        num_rows,
        manifest_paths,
        base_config,
        cluster_config,
        dim,
        dataset ? static_cast<const T*>(dataset->GetTensor()) : nullptr,
        num_clusters);
    rc.RecordSection("clustering result upload done");
    rc.ElapseFromBegin("clustering done");
}

template void
KmeansClustering::StreamingAssignandUpload<float>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const float* full_training_data,
    const int64_t num_clusters);

template void
KmeansClustering::FetchDataFiles<float>(uint8_t* buf,
                                        const int64_t expected_rows,
                                        const int64_t row_offset,
                                        const std::vector<std::string>& files,
                                        const int64_t dim,
                                        int64_t& offset);

template void
KmeansClustering::FetchSegmentViaManifest<float>(
    uint8_t* buf,
    const int64_t expected_rows,
    const int64_t row_offset,
    const std::string& manifest_path,
    const Config& base_config,
    const int64_t dim,
    int64_t& offset);

template void
KmeansClustering::SampleTrainData<float>(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const int64_t expected_train_size,
    const int64_t dim,
    const bool random_sample,
    std::mt19937_64& rng,
    uint8_t* buf);

template void
KmeansClustering::Run<float>(
    const milvus::proto::clustering::AnalyzeInfo& config);
template void
KmeansClustering::StreamingAssignandUpload<knowhere::fp16>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const knowhere::fp16* full_training_data,
    const int64_t num_clusters);

template void
KmeansClustering::FetchDataFiles<knowhere::fp16>(
    uint8_t* buf,
    const int64_t expected_rows,
    const int64_t row_offset,
    const std::vector<std::string>& files,
    const int64_t dim,
    int64_t& offset);

template void
KmeansClustering::FetchSegmentViaManifest<knowhere::fp16>(
    uint8_t* buf,
    const int64_t expected_rows,
    const int64_t row_offset,
    const std::string& manifest_path,
    const Config& base_config,
    const int64_t dim,
    int64_t& offset);

template void
KmeansClustering::SampleTrainData<knowhere::fp16>(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const int64_t expected_train_size,
    const int64_t dim,
    const bool random_sample,
    std::mt19937_64& rng,
    uint8_t* buf);

template void
KmeansClustering::Run<knowhere::fp16>(
    const milvus::proto::clustering::AnalyzeInfo& config);
template void
KmeansClustering::StreamingAssignandUpload<knowhere::bf16>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const knowhere::bf16* full_training_data,
    const int64_t num_clusters);

template void
KmeansClustering::FetchDataFiles<knowhere::bf16>(
    uint8_t* buf,
    const int64_t expected_rows,
    const int64_t row_offset,
    const std::vector<std::string>& files,
    const int64_t dim,
    int64_t& offset);

template void
KmeansClustering::FetchSegmentViaManifest<knowhere::bf16>(
    uint8_t* buf,
    const int64_t expected_rows,
    const int64_t row_offset,
    const std::string& manifest_path,
    const Config& base_config,
    const int64_t dim,
    int64_t& offset);

template void
KmeansClustering::SampleTrainData<knowhere::bf16>(
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& segment_file_paths,
    const std::map<int64_t, int64_t>& segment_num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const int64_t expected_train_size,
    const int64_t dim,
    const bool random_sample,
    std::mt19937_64& rng,
    uint8_t* buf);

template void
KmeansClustering::Run<knowhere::bf16>(
    const milvus::proto::clustering::AnalyzeInfo& config);

}  // namespace milvus::clustering
