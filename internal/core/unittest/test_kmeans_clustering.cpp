// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <functional>
#include <fstream>
#include <boost/filesystem.hpp>
#include <numeric>
#include <unordered_set>

#include "common/Tracer.h"
#include "common/EasyAssert.h"
#include "index/InvertedIndexTantivy.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "clustering/KmeansClustering.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "index/Meta.h"

using namespace milvus;

void
ReadPBFile(std::string& file_path, google::protobuf::Message& message) {
    std::ifstream infile;
    infile.open(file_path.data(), std::ios_base::binary);
    if (infile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << file_path << " failed, "
                << strerror(errno);
        throw SegcoreError(FileOpenFailed, err_msg.str());
    }

    infile.seekg(0, std::ios::beg);
    if (!message.ParseFromIstream(&infile)) {
        std::stringstream err_msg;
        err_msg << "Error: parse pb file '" << file_path << " failed, "
                << strerror(errno);
        throw SegcoreError(FileReadFailed, err_msg.str());
    }
    infile.close();
}

milvus::proto::clustering::AnalyzeInfo
transforConfigToPB(const Config& config) {
    milvus::proto::clustering::AnalyzeInfo analyze_info;
    analyze_info.set_num_clusters(config["num_clusters"]);
    analyze_info.set_max_cluster_ratio(config["max_cluster_ratio"]);
    analyze_info.set_min_cluster_ratio(config["min_cluster_ratio"]);
    analyze_info.set_max_cluster_size(config["max_cluster_size"]);
    auto& num_rows = *analyze_info.mutable_num_rows();
    for (const auto& [k, v] :
         milvus::index::GetValueFromConfig<std::map<int64_t, int64_t>>(
             config, "num_rows")
             .value()) {
        num_rows[k] = v;
    }
    auto& insert_files = *analyze_info.mutable_insert_files();
    auto insert_files_map = milvus::index::GetValueFromConfig<
                                std::map<int64_t, std::vector<std::string>>>(
                                config, INSERT_FILES_KEY)
                                .value();
    for (const auto& [k, v] : insert_files_map) {
        for (auto i = 0; i < v.size(); i++)
            insert_files[k].add_insert_files(v[i]);
    }
    analyze_info.set_dim(config["dim"]);
    analyze_info.set_train_size(config["train_size"]);
    return analyze_info;
}

// when we skip clustering, nothing uploaded
template <typename T>
void
CheckResultEmpty(const milvus::clustering::KmeansClusteringPtr& clusteringJob,
                 const milvus::storage::ChunkManagerPtr cm,
                 int64_t segment_id,
                 int64_t segment_id2) {
    std::string centroids_path_prefix =
        clusteringJob->GetRemoteCentroidsObjectPrefix();
    std::string centroid_path =
        centroids_path_prefix + "/" + std::string(CENTROIDS_NAME);
    ASSERT_FALSE(cm->Exist(centroid_path));
    std::string offset_mapping_name = std::string(OFFSET_MAPPING_NAME);
    std::string centroid_id_mapping_path =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id) +
        "/" + offset_mapping_name;
    milvus::proto::clustering::ClusteringCentroidIdMappingStats mapping_stats;
    std::string centroid_id_mapping_path2 =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id2) +
        "/" + offset_mapping_name;
    ASSERT_FALSE(cm->Exist(centroid_id_mapping_path));
    ASSERT_FALSE(cm->Exist(centroid_id_mapping_path2));
}

template <typename T>
void
CheckResultCorrectness(
    const milvus::clustering::KmeansClusteringPtr& clusteringJob,
    const milvus::storage::ChunkManagerPtr cm,
    int64_t segment_id,
    int64_t segment_id2,
    int64_t dim,
    int64_t nb,
    int expected_num_clusters,
    bool check_centroids) {
    std::string centroids_path_prefix =
        clusteringJob->GetRemoteCentroidsObjectPrefix();
    std::string centroids_name = std::string(CENTROIDS_NAME);
    std::string centroid_path = centroids_path_prefix + "/" + centroids_name;
    milvus::proto::clustering::ClusteringCentroidsStats stats;
    ReadPBFile(centroid_path, stats);
    std::vector<T> centroids;
    for (const auto& centroid : stats.centroids()) {
        const auto& float_vector = centroid.float_vector();
        for (float value : float_vector.data()) {
            centroids.emplace_back(T(value));
        }
    }
    ASSERT_EQ(centroids.size(), expected_num_clusters * dim);
    std::string offset_mapping_name = std::string(OFFSET_MAPPING_NAME);
    std::string centroid_id_mapping_path =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id) +
        "/" + offset_mapping_name;
    milvus::proto::clustering::ClusteringCentroidIdMappingStats mapping_stats;
    std::string centroid_id_mapping_path2 =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id2) +
        "/" + offset_mapping_name;
    milvus::proto::clustering::ClusteringCentroidIdMappingStats mapping_stats2;
    ReadPBFile(centroid_id_mapping_path, mapping_stats);
    ReadPBFile(centroid_id_mapping_path2, mapping_stats2);

    std::vector<uint32_t> centroid_id_mapping;
    std::vector<int64_t> num_in_centroid;
    for (const auto id : mapping_stats.centroid_id_mapping()) {
        centroid_id_mapping.emplace_back(id);
        ASSERT_TRUE(id < expected_num_clusters);
    }
    ASSERT_EQ(centroid_id_mapping.size(), nb);
    for (const auto num : mapping_stats.num_in_centroid()) {
        num_in_centroid.emplace_back(num);
    }
    ASSERT_EQ(
        std::accumulate(num_in_centroid.begin(), num_in_centroid.end(), 0), nb);
    // second id mapping should be the same with the first one since the segment data is the same
    if (check_centroids) {
        for (int64_t i = 0; i < mapping_stats2.centroid_id_mapping_size();
             i++) {
            ASSERT_EQ(mapping_stats2.centroid_id_mapping(i),
                      centroid_id_mapping[i]);
        }
        for (int64_t i = 0; i < mapping_stats2.num_in_centroid_size(); i++) {
            ASSERT_EQ(mapping_stats2.num_in_centroid(i), num_in_centroid[i]);
        }
    }
    // remove files
    cm->Remove(centroid_path);
    cm->Remove(centroid_id_mapping_path);
    cm->Remove(centroid_id_mapping_path2);
}

template <typename T, DataType dtype>
void
test_run() {
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t segment_id2 = 4;
    int64_t field_id = 101;
    int64_t index_build_id = 1000;
    int64_t index_version = 10000;
    int64_t dim = 100;
    int64_t nb = 10000;

    auto field_meta =
        gen_field_data_meta(collection_id, partition_id, segment_id, field_id);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);

    std::string root_path = "/tmp/test-kmeans-clustering/";
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);

    std::vector<T> data_gen(nb * dim);
    for (int64_t i = 0; i < nb * dim; ++i) {
        data_gen[i] = rand();
    }
    auto field_data = storage::CreateFieldData(dtype, false, dim);
    field_data->FillFieldData(data_gen.data(), data_gen.size() / dim);
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id,
                           log_id);
    };

    auto log_path = get_binlog_path(0);
    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());
    storage::FileManagerContext ctx(field_meta, index_meta, cm);

    std::map<int64_t, std::vector<std::string>> remote_files;
    std::map<int64_t, int64_t> num_rows;
    // two segments
    remote_files[segment_id] = {log_path};
    remote_files[segment_id2] = {log_path};
    num_rows[segment_id] = nb;
    num_rows[segment_id2] = nb;
    Config config;
    config["max_cluster_ratio"] = 10.0;
    config["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    auto clusteringJob = std::make_unique<clustering::KmeansClustering>(ctx);
    // no need to sample train data
    {
        config["min_cluster_ratio"] = 0.01;
        config[INSERT_FILES_KEY] = remote_files;
        config["num_clusters"] = 8;
        config["train_size"] = 25L * 1024 * 1024 * 1024;  // 25GB
        config["dim"] = dim;
        config["num_rows"] = num_rows;
        clusteringJob->Run<T>(transforConfigToPB(config));
        CheckResultCorrectness<T>(clusteringJob,
                                  cm,
                                  segment_id,
                                  segment_id2,
                                  dim,
                                  nb,
                                  config["num_clusters"],
                                  true);
    }
    {
        config["min_cluster_ratio"] = 0.01;
        config[INSERT_FILES_KEY] = remote_files;
        config["num_clusters"] = 200;
        config["train_size"] = 25L * 1024 * 1024 * 1024;  // 25GB
        config["dim"] = dim;
        config["num_rows"] = num_rows;
        clusteringJob->Run<T>(transforConfigToPB(config));
        CheckResultCorrectness<T>(clusteringJob,
                                  cm,
                                  segment_id,
                                  segment_id2,
                                  dim,
                                  nb,
                                  config["num_clusters"],
                                  true);
    }
    // num clusters larger than train num
    {
        EXPECT_THROW(
            try {
                config["min_cluster_ratio"] = 0.01;
                config[INSERT_FILES_KEY] = remote_files;
                config["num_clusters"] = 100000;
                config["train_size"] = 25L * 1024 * 1024 * 1024;  // 25GB
                config["dim"] = dim;
                config["num_rows"] = num_rows;
                clusteringJob->Run<T>(transforConfigToPB(config));
            } catch (SegcoreError& e) {
                ASSERT_EQ(e.get_error_code(), ErrorCode::ClusterSkip);
                CheckResultEmpty<T>(clusteringJob, cm, segment_id, segment_id2);
                throw e;
            },
            SegcoreError);
    }

    // data skew
    {
        EXPECT_THROW(
            try {
                config["min_cluster_ratio"] = 0.98;
                config[INSERT_FILES_KEY] = remote_files;
                config["num_clusters"] = 8;
                config["train_size"] = 25L * 1024 * 1024 * 1024;  // 25GB
                config["dim"] = dim;
                config["num_rows"] = num_rows;
                clusteringJob->Run<T>(transforConfigToPB(config));
            } catch (SegcoreError& e) {
                ASSERT_EQ(e.get_error_code(), ErrorCode::ClusterSkip);
                CheckResultEmpty<T>(clusteringJob, cm, segment_id, segment_id2);
                throw e;
            },
            SegcoreError);
    }

    // need to sample train data case1
    {
        config["min_cluster_ratio"] = 0.01;
        config[INSERT_FILES_KEY] = remote_files;
        config["num_clusters"] = 8;
        config["train_size"] = 1536L * 1024;  // 1.5MB
        config["dim"] = dim;
        config["num_rows"] = num_rows;
        clusteringJob->Run<T>(transforConfigToPB(config));
        CheckResultCorrectness<T>(clusteringJob,
                                  cm,
                                  segment_id,
                                  segment_id2,
                                  dim,
                                  nb,
                                  config["num_clusters"],
                                  true);
    }
    // need to sample train data case2
    {
        config["min_cluster_ratio"] = 0.01;
        config[INSERT_FILES_KEY] = remote_files;
        config["num_clusters"] = 8;
        config["train_size"] = 6L * 1024 * 1024;  // 6MB
        config["dim"] = dim;
        config["num_rows"] = num_rows;
        clusteringJob->Run<T>(transforConfigToPB(config));
        CheckResultCorrectness<T>(clusteringJob,
                                  cm,
                                  segment_id,
                                  segment_id2,
                                  dim,
                                  nb,
                                  config["num_clusters"],
                                  true);
    }
}

TEST(MajorCompaction, Naive) {
    test_run<float, DataType::VECTOR_FLOAT>();
}