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
#include <ctime>
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

std::map<int64_t, int64_t>
AllocateSegmentSampleRows(const std::vector<int64_t>& segment_ids,
                          const std::map<int64_t, int64_t>& segment_num_rows,
                          int64_t target_sample_rows) {
    AssertInfo(target_sample_rows >= 0,
               "target sample rows must be non-negative");

    std::map<int64_t, int64_t> result;
    std::vector<int64_t> active;
    int64_t total_rows = 0;
    for (const auto segment_id : segment_ids) {
        const auto rows = segment_num_rows.at(segment_id);
        AssertInfo(
            rows >= 0, "segment {} row count must be non-negative", segment_id);
        result[segment_id] = 0;
        if (rows > 0) {
            active.push_back(segment_id);
            AssertInfo(total_rows <= std::numeric_limits<int64_t>::max() - rows,
                       "segment row count sum overflows");
            total_rows += rows;
        }
    }

    auto remaining = std::min(target_sample_rows, total_rows);
    if (remaining > 0 && remaining < static_cast<int64_t>(active.size())) {
        throw SegcoreError(
            ErrorCode::ClusterSkip,
            "sample row budget is too small to cover every non-empty segment");
    }

    while (remaining > 0) {
        AssertInfo(!active.empty(), "no segment capacity remains for sampling");
        const auto share = remaining / static_cast<int64_t>(active.size());
        const auto extra = remaining % static_cast<int64_t>(active.size());
        int64_t allocated = 0;
        std::vector<int64_t> next;
        next.reserve(active.size());
        for (size_t index = 0; index < active.size(); ++index) {
            const auto segment_id = active[index];
            const auto capacity =
                segment_num_rows.at(segment_id) - result.at(segment_id);
            const auto requested =
                share + (static_cast<int64_t>(index) < extra ? 1 : 0);
            const auto rows = std::min(capacity, requested);
            result[segment_id] += rows;
            allocated += rows;
            if (capacity > rows) {
                next.push_back(segment_id);
            }
        }
        AssertInfo(allocated > 0, "failed to allocate segment sample rows");
        remaining -= allocated;
        active = std::move(next);
    }
    return result;
}

// Copy at most max_bytes from field-data payloads into buf. source_offset
// skips a prefix in the source segment; destination_offset appends into the
// shared training buffer.
static int64_t
CopyFieldDatasToBuf(uint8_t* buf,
                    const int64_t max_bytes,
                    std::vector<FieldDataPtr>& field_datas,
                    int64_t& source_offset,
                    int64_t& destination_offset) {
    int64_t fetched = 0;
    for (auto& data : field_datas) {
        const auto data_size = static_cast<int64_t>(data->Size());
        if (source_offset >= data_size) {
            source_offset -= data_size;
            data.reset();
            continue;
        }
        const auto available = data_size - source_offset;
        const auto size = std::min(max_bytes - fetched, available);
        if (size <= 0) {
            break;
        }
        fetched += size;
        const auto* source =
            reinterpret_cast<const uint8_t*>(data->Data()) + source_offset;
        milvus::fastmem::FastMemcpy(buf + destination_offset, source, size);
        destination_offset += size;
        source_offset = 0;
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
    // CacheRawDataToMemory mostly used as pull files from one segment
    // So we could assume memory is always enough for theses cases
    // But in clustering when we sample train data, first pre-allocate the large buffer(size controlled by config) for future knowhere usage
    // And we will have tmp memory usage at pulling stage, pull file(tmp memory) + memcpy to pre-allocated buffer, limit the batch here
    auto batch = size_t(DEFAULT_FIELD_MAX_MEMORY_LIMIT / FILE_SLICE_SIZE);
    int64_t fetched_file_size = 0;
    const auto expected_remote_file_size = expected_rows * dim * sizeof(T);
    int64_t source_offset = row_offset * dim * sizeof(T);

    for (size_t i = 0; i < files.size(); i += batch) {
        if (fetched_file_size >= expected_remote_file_size) {
            break;
        }
        size_t start = i;
        size_t end = std::min(files.size(), i + batch);
        std::vector<std::string> group_files(files.begin() + start,
                                             files.begin() + end);
        Config config;
        config[INSERT_FILES_KEY] = group_files;
        auto field_datas = file_manager_->CacheRawDataToMemory(config);
        fetched_file_size +=
            CopyFieldDatasToBuf(buf,
                                expected_remote_file_size - fetched_file_size,
                                field_datas,
                                source_offset,
                                offset);
    }
    if (!(fetched_file_size == expected_remote_file_size)) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "file size inconsistent, expected: {}, actual: {}",
                  expected_remote_file_size,
                  fetched_file_size);
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
    // base_config already carries STORAGE_VERSION/DATA_TYPE/ELEMENT_TYPE/DIM
    // (built once in Run, mirroring index_c.cpp get_config). Only the manifest
    // varies per segment.
    Config config = base_config;
    config[SEGMENT_MANIFEST_KEY] = manifest_path;
    config[NUM_ROWS_KEY] = expected_rows;
    config[OFFSET_KEY] = row_offset;
    auto field_datas = file_manager_->CacheRawDataToMemory(config);
    const auto expected_remote_file_size = expected_rows * dim * sizeof(T);
    int64_t source_offset = 0;
    int64_t fetched = CopyFieldDatasToBuf(
        buf, expected_remote_file_size, field_datas, source_offset, offset);
    if (!(fetched == expected_remote_file_size)) {
        ThrowInfo(ErrorCode::FileReadFailed,
                  "file size inconsistent, expected: {}, actual: {}",
                  expected_remote_file_size,
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
    uint8_t* buf) {
    int64_t offset = 0;
    const auto bytes_per_row = dim * sizeof(T);
    AssertInfo(bytes_per_row > 0 && expected_train_size % bytes_per_row == 0,
               "training size must contain complete vectors");
    const auto target_sample_rows = expected_train_size / bytes_per_row;
    const auto sample_rows = AllocateSegmentSampleRows(
        segment_ids, segment_num_rows, target_sample_rows);
    std::mt19937 rng(static_cast<unsigned int>(std::time(nullptr)));

    for (const auto cur_segment_id : segment_ids) {
        const auto rows = sample_rows.at(cur_segment_id);
        if (rows == 0) {
            continue;
        }
        const auto segment_rows = segment_num_rows.at(cur_segment_id);
        int64_t row_offset = 0;
        if (random_sample && rows < segment_rows) {
            std::uniform_int_distribution<int64_t> distribution(
                0, segment_rows - rows);
            row_offset = distribution(rng);
        }

        auto mit = manifest_paths.find(cur_segment_id);
        if (mit != manifest_paths.end() && !mit->second.empty()) {
            FetchSegmentViaManifest<T>(
                buf, rows, row_offset, mit->second, base_config, dim, offset);
            continue;
        }
        std::vector<std::string> files = segment_file_paths.at(cur_segment_id);
        std::sort(files.begin(),
                  files.end(),
                  [](const std::string& a, const std::string& b) {
                      return std::stol(a.substr(a.find_last_of("/") + 1)) <
                             std::stol(b.substr(b.find_last_of("/") + 1));
                  });
        FetchDataFiles<T>(buf, rows, row_offset, files, dim, offset);
    }
    AssertInfo(offset == expected_train_size,
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
        if constexpr (std::is_same_v<T, float>) {
            auto* float_array = vector_field->mutable_float_vector();
            for (auto j = 0; j < dim; j++) {
                float_array->add_data(centroid[j]);
            }
        } else if constexpr (std::is_same_v<T, float16>) {
            vector_field->set_float16_vector(std::string(
                reinterpret_cast<const char*>(centroid), dim * sizeof(T)));
        } else if constexpr (std::is_same_v<T, bfloat16>) {
            vector_field->set_bfloat16_vector(std::string(
                reinterpret_cast<const char*>(centroid), dim * sizeof(T)));
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
    AssertInfo(centroid_id_mapping != nullptr,
               "cluster assignment returned no centroid ids");
    AssertInfo(distances != nullptr,
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
    size_t avg_size =
        std::accumulate(
            num_in_each_centroid.begin(), num_in_each_centroid.end(), 0) /
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
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const int64_t trained_segments_num,
    const int64_t num_clusters) {
    auto byte_size = centroid_stats.ByteSizeLong();
    std::unique_ptr<uint8_t[]> data = std::make_unique<uint8_t[]>(byte_size);
    centroid_stats.SerializeToArray(data.get(), byte_size);
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

    for (size_t i = 0; i < segment_ids.size(); i++) {
        int64_t segment_id = segment_ids[i];
        // id mapping has been computed, just upload to remote
        if (i < trained_segments_num) {
            serializeIdMappingAndUpload(segment_id, id_mapping_stats[i]);
            for (int64_t j = 0; j < num_clusters; ++j) {
                num_vectors_each_centroid[j] +=
                    id_mapping_stats[i].num_in_centroid(j);
            }
        } else {  // streaming download raw data, assign id mapping, then upload
            int64_t num_row = num_rows.at(segment_id);
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
            auto dataset = GenDataset(num_row, dim, buf.release());
            dataset->SetIsOwner(true);
            auto res =
                cluster_node.AssignWithDistance(*dataset, cluster_config);
            if (!res.has_value()) {
                ThrowInfo(KnowhereStatusToErrorCode(res.error()),
                          fmt::format("failed to assign with distance: {}: {}",
                                      KnowhereStatusString(res.error()),
                                      res.what()));
            }
            res.value()->SetIsOwner(true);
            AssertInfo(res.value()->GetRows() == num_row,
                       "cluster assignment returned {} rows, expected {}",
                       res.value()->GetRows(),
                       num_row);
            const auto* ids = res.value()->GetIds();
            const auto* distances = res.value()->GetDistance();
            auto id_mapping_pb = CentroidIdMappingWithDistanceToPB(
                ids, distances, num_row, num_clusters);
            for (int64_t j = 0; j < num_clusters; ++j) {
                num_vectors_each_centroid[j] +=
                    id_mapping_pb.num_in_centroid(j);
            }
            serializeIdMappingAndUpload(segment_id, id_mapping_pb);
        }
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
    std::map<int64_t, std::vector<std::string>> insert_files;
    for (const auto& pair : config.insert_files()) {
        std::vector<std::string> segment_files(
            pair.second.insert_files().begin(),
            pair.second.insert_files().end());
        insert_files[pair.first] = std::move(segment_files);
    }

    std::map<int64_t, std::string> manifest_paths(
        config.manifest_paths().begin(), config.manifest_paths().end());

    // Prepared once, mirroring indexbuilder's get_config(). SEGMENT_MANIFEST_KEY
    // is stamped per segment by FetchSegmentViaManifest, because analyze is
    // multi-segment while an index build job covers a single segment.
    // element_type is DataType::NONE for FloatVector; the manifest reader only
    // consults it for array/vector-array types, so NONE is correct here.
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
    auto min_cluster_ratio = config.min_cluster_ratio();
    AssertInfo(min_cluster_ratio > 0 && min_cluster_ratio < 1,
               "min cluster ratio must larger than 0, less than 1");
    auto max_cluster_ratio = config.max_cluster_ratio();
    AssertInfo(max_cluster_ratio > 1, "max cluster ratio must larger than 1");
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
        data_num += num_row_each_segment;
        segment_ids.emplace_back(segment_id);
        AssertInfo(insert_files.find(segment_id) != insert_files.end() ||
                       manifest_paths.find(segment_id) != manifest_paths.end(),
                   "segment id {} has neither insert files nor manifest",
                   segment_id);
    }
    size_t trained_segments_num = 0;

    size_t data_size = data_num * dim * sizeof(T);
    size_t train_num = train_size / sizeof(T) / dim;
    bool random_sample = true;
    // make train num equal to data num
    if (train_num >= data_num) {
        train_num = data_num;
        random_sample =
            false;  // all data are used for training, no need to random sampling
        trained_segments_num = segment_ids.size();
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
    auto buf = std::make_unique<uint8_t[]>(train_size_final);
    SampleTrainData<T>(segment_ids,
                       insert_files,
                       num_rows,
                       manifest_paths,
                       base_config,
                       train_size_final,
                       dim,
                       random_sample,
                       buf.get());
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
    res.value()->SetIsOwner(true);
    rc.RecordSection("clustering train done");

    auto centroids_res = cluster_node.GetCentroids();
    if (!centroids_res.has_value()) {
        ThrowInfo(KnowhereStatusToErrorCode(centroids_res.error()),
                  fmt::format("failed to get centroids: {}: {}",
                              KnowhereStatusString(centroids_res.error()),
                              centroids_res.what()));
    }
    // centroids owned by cluster_node
    centroids_res.value()->SetIsOwner(false);
    auto centroids =
        reinterpret_cast<const T*>(centroids_res.value()->GetTensor());

    auto centroid_stats = CentroidsToPB<T>(centroids, num_clusters, dim);
    std::vector<milvus::proto::clustering::ClusteringCentroidIdMappingStats>
        id_mapping_stats;
    id_mapping_stats.reserve(trained_segments_num);
    if (trained_segments_num > 0) {
        auto assign_res =
            cluster_node.AssignWithDistance(*dataset, cluster_config);
        if (!assign_res.has_value()) {
            ThrowInfo(
                ErrorCode::UnexpectedError,
                fmt::format(
                    "failed to assign training data with distance: {}: {}",
                    KnowhereStatusString(assign_res.error()),
                    assign_res.what()));
        }
        assign_res.value()->SetIsOwner(true);
        AssertInfo(
            assign_res.value()->GetRows() == static_cast<int64_t>(train_num),
            "cluster assignment returned {} rows, expected {}",
            assign_res.value()->GetRows(),
            train_num);
        const auto* centroid_id_mapping = assign_res.value()->GetIds();
        const auto* distances = assign_res.value()->GetDistance();
        AssertInfo(centroid_id_mapping != nullptr,
                   "cluster assignment returned no centroid ids");
        AssertInfo(distances != nullptr,
                   "cluster assignment returned no distances");
        int64_t trained_row_offset = 0;
        for (size_t i = 0; i < trained_segments_num; ++i) {
            const auto segment_rows = num_rows.at(segment_ids[i]);
            id_mapping_stats.emplace_back(CentroidIdMappingWithDistanceToPB(
                centroid_id_mapping + trained_row_offset,
                distances + trained_row_offset,
                segment_rows,
                num_clusters));
            trained_row_offset += segment_rows;
        }
    }
    dataset.reset();
    // upload
    StreamingAssignandUpload<T>(cluster_node,
                                config,
                                centroid_stats,
                                id_mapping_stats,
                                segment_ids,
                                insert_files,
                                num_rows,
                                manifest_paths,
                                base_config,
                                cluster_config,
                                dim,
                                trained_segments_num,
                                num_clusters);
    rc.RecordSection("clustering result upload done");
    rc.ElapseFromBegin("clustering done");
}

template void
KmeansClustering::StreamingAssignandUpload<float>(
    knowhere::Cluster<knowhere::ClusterNode>& cluster_node,
    const milvus::proto::clustering::AnalyzeInfo& config,
    const milvus::proto::clustering::ClusteringCentroidsStats& centroid_stats,
    const std::vector<
        milvus::proto::clustering::ClusteringCentroidIdMappingStats>&
        id_mapping_stats,
    const std::vector<int64_t>& segment_ids,
    const std::map<int64_t, std::vector<std::string>>& insert_files,
    const std::map<int64_t, int64_t>& num_rows,
    const std::map<int64_t, std::string>& manifest_paths,
    const Config& base_config,
    const knowhere::Json& cluster_config,
    const int64_t dim,
    const int64_t trained_segments_num,
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
    uint8_t* buf);

template void
KmeansClustering::Run<float>(
    const milvus::proto::clustering::AnalyzeInfo& config);

template void
KmeansClustering::Run<float16>(
    const milvus::proto::clustering::AnalyzeInfo& config);

template void
KmeansClustering::Run<bfloat16>(
    const milvus::proto::clustering::AnalyzeInfo& config);

template milvus::proto::clustering::ClusteringCentroidsStats
KmeansClustering::CentroidsToPB<float>(const float* centroids,
                                       const int64_t num_clusters,
                                       const int64_t dim);
template bool
KmeansClustering::IsDataSkew<float>(
    const milvus::proto::clustering::AnalyzeInfo& config,
    const int64_t dim,
    std::vector<int64_t>& num_in_each_centroid);

}  // namespace milvus::clustering
