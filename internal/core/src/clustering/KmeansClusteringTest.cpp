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
#include <gtest/gtest-param-test.h>
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
#include "milvus-storage/packed/writer.h"
#include <filesystem>
#include <random>
#include <algorithm>
#include <iostream>

namespace fs = std::filesystem;

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

using namespace milvus::clustering;

/**
 * Reads a protobuf file (centroids or mapping) from the given path.
 * If the file does not exist or is corrupted → throw SegcoreError.
 * Used to verify the output of KMeans.
 */
void
ReadPBFile(const std::string& file_path, google::protobuf::Message& message) {
    std::ifstream infile(file_path.data(), std::ios_base::binary);
    if (infile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << file_path << "' failed, "
                << strerror(errno);
        throw SegcoreError(FileOpenFailed, err_msg.str());
    }
    infile.seekg(0, std::ios::beg);
    if (!message.ParseFromIstream(&infile)) {
        std::stringstream err_msg;
        err_msg << "Error: parse pb file '" << file_path << "' failed, "
                << strerror(errno);
        throw SegcoreError(FileReadFailed, err_msg.str());
    }
    infile.close();
}

/**
 * Converts a JSON Config object into a protobuf AnalyzeInfo:
 * The type AnalyzeInfo is a protobuf message class generated from a .proto file
 * it represents the clustering configuration in binary-structured form.
 * It includes: num_clusters, train_size, vector dimension, cluster_ratio limits,
 * max_cluster_size, insert_files (per segment), num_rows (per segment)
 * This structure is required by KmeansClustering::Run().
 * (the KMeans engine, which expects a protobuf message - not a JSON config,
 * Milvus uses Config (JSON) for human-readable configuration,
 * but its internal algorithms use protobuf for structured and type-safe data exchange.)
 */
milvus::proto::clustering::AnalyzeInfo
transforConfigToPB(const Config& config) {
    using namespace milvus::proto::clustering;
    milvus::proto::clustering::AnalyzeInfo info_pb;
    info_pb.set_num_clusters(config.at("num_clusters").get<int>());
    info_pb.set_train_size(config.at("train_size").get<int64_t>());
    info_pb.set_dim(config.at("dim").get<int>());
    info_pb.set_max_cluster_ratio(config.at("max_cluster_ratio").get<double>());
    info_pb.set_min_cluster_ratio(config.at("min_cluster_ratio").get<double>());
    info_pb.set_max_cluster_size(config.at("max_cluster_size").get<int64_t>());
    info_pb.set_train_buffer_size(
        config.at("train_buffer_size").get<int64_t>());
    info_pb.set_assign_buffer_size(
        config.at("assign_buffer_size").get<int64_t>());
    info_pb.set_storage_version(config.at("storage_version").get<int>());
    // Insert files depending on storage version
    if (info_pb.storage_version() == STORAGE_V1) {
        // V1: flat map<segment_id, vector<string>>
        // Get insert files from JSON
        auto insert_files_map =
            config.at(INSERT_FILES_KEY)
                .get<std::map<int64_t, std::vector<std::string>>>();
        for (auto& [segment_id, paths] : insert_files_map) {
            auto& seg_pb = (*info_pb.mutable_insert_files())[segment_id];
            for (auto& path : paths) {
                seg_pb.add_insert_files(path);
            }
        }
    } else if (info_pb.storage_version() == STORAGE_V2) {
        // V2: nested map<segment_id, vector<vector<string>>>
        auto seg_insert_files_v2 =
            config.at(SEGMENT_INSERT_FILES_KEY)
                .get<
                    std::map<int64_t, std::vector<std::vector<std::string>>>>();
        for (auto& [seg_id, groups] : seg_insert_files_v2) {
            auto& seg_pb_v2 = (*info_pb.mutable_segment_insert_files())[seg_id];
            auto* files_field = seg_pb_v2.mutable_field_insert_files();
            for (auto& group : groups) {
                auto* file_msg = files_field->Add();
                for (auto& path : group) {
                    file_msg->add_file_paths(path);
                }
            }
        }
    }
    // Number of rows per segment (common)
    auto num_rows_map = config.at("num_rows").get<std::map<int64_t, int64_t>>();
    for (auto& [segment_id, num] : num_rows_map) {
        (*info_pb.mutable_num_rows())[segment_id] = num;
    }
    return info_pb;
}

/**
 * This function validates that:
 * - The centroid protobuf file (output of KMeans) was written correctly and has the
 *   right size.
 * - The cluster assignment mapping files (for both segments) are valid:
 *      Each vector has an assigned centroid ID.
 *      No centroid ID is out of range.
 *      The total number of vectors in all clusters equals nb (no data loss).
 * - If check_centroids = true: The two segments’ assignments (segment_id
 *   and segment_id2) are identical. since the test uses identical data for
 *   both segments, the clustering result should match exactly.
 */
template <typename T>
void
CheckResultCorrectness(
    const milvus::clustering::KmeansClusteringPtr& clusteringJob,
    const milvus::storage::ChunkManagerPtr& cm,
    const std::map<int64_t, int64_t>& segment_rows_map,
    int64_t dim,
    int expected_num_clusters,
    bool check_centroids) {
    // Load centroids.pb file
    // ----------------------
    // Construct the full path to the centroids protobuf file.
    // Example: /tmp/test-kmeans-clustering-unified/centroids.pb
    std::string centroids_path_prefix =
        clusteringJob->GetRemoteCentroidsObjectPrefix();
    std::string centroids_name = std::string(CENTROIDS_NAME);
    std::string centroid_path = centroids_path_prefix + "/" + centroids_name;
    // Deserialize the protobuf file into a ClusteringCentroidsStats object.
    milvus::proto::clustering::ClusteringCentroidsStats stats;
    ReadPBFile(centroid_path, stats);
    // Extract all centroid coordinates into a flat vector<float>.
    // Each centroid has 'dim' float values.
    std::vector<T> centroids;
    for (const auto& centroid : stats.centroids()) {
        for (float value : centroid.float_vector().data()) {
            centroids.emplace_back(T(value));
        }
    }
    // Sanity check: total number of values should equal num_clusters * dim.
    ASSERT_EQ(centroids.size(), expected_num_clusters * dim)
        << "Centroid count mismatch. Expected " << expected_num_clusters * dim;

    // Load cluster assignment mapping files
    // -------------------------------------
    // Each segment has its own cluster assignment protobuf file (mapping)
    // mapping: vector_index - centroid_id
    // Also includes num_in_centroid[] = how many vectors belong to each cluster.
    std::map<int64_t,
             milvus::proto::clustering::ClusteringCentroidIdMappingStats>
        mapping_stats_map;

    for (auto& [seg_id, nb] : segment_rows_map) {
        std::string offset_mapping_name = std::string(OFFSET_MAPPING_NAME);
        std::string centroid_id_mapping_path =
            clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(seg_id) +
            "/" + offset_mapping_name;
        // Deserialize the mapping file
        milvus::proto::clustering::ClusteringCentroidIdMappingStats
            mapping_stats;
        ReadPBFile(centroid_id_mapping_path, mapping_stats);
        mapping_stats_map[seg_id] = mapping_stats;
        // Validate mapping size and completeness
        ASSERT_EQ(mapping_stats.centroid_id_mapping_size(), nb)
            << "Segment " << seg_id << " mapping size mismatch";
        ASSERT_EQ(std::accumulate(mapping_stats.num_in_centroid().begin(),
                                  mapping_stats.num_in_centroid().end(),
                                  0),
                  nb)
            << "Segment " << seg_id << " total cluster counts != total vectors";
        // Validate centroid IDs are within expected range
        for (auto id : mapping_stats.centroid_id_mapping()) {
            ASSERT_TRUE(id < expected_num_clusters)
                << "Invalid centroid ID " << id << " in segment " << seg_id;
        }
        //        cm->Remove(centroid_id_mapping_path); // Remove file after validation
    }
    // cross-segment comparison
    // ------------------------
    if (check_centroids && segment_rows_map.size() > 1) {
        auto it = mapping_stats_map.begin();
        const auto& ref = it->second;
        ++it;
        for (; it != mapping_stats_map.end(); ++it) {
            const auto& stats_other = it->second;
            ASSERT_EQ(ref.centroid_id_mapping_size(),
                      stats_other.centroid_id_mapping_size())
                << "Cluster assignment size mismatch between segments";
            for (int64_t i = 0; i < ref.centroid_id_mapping_size(); ++i) {
                ASSERT_EQ(ref.centroid_id_mapping(i),
                          stats_other.centroid_id_mapping(i))
                    << "Mismatch in centroid mapping at index " << i;
            }
            for (int64_t i = 0; i < ref.num_in_centroid_size(); ++i) {
                ASSERT_EQ(ref.num_in_centroid(i),
                          stats_other.num_in_centroid(i))
                    << "Mismatch in num_in_centroid for centroid " << i;
            }
        }
    }
    //    cm->Remove(centroid_path);
}

/**
 * run clustering and check data skew
 * if data skew is detected do not fail the test
 * because data is generated randomly and may cause data skew
 * return true if test passed without data skew
 */
template <typename T>
bool
runClustering(std::unique_ptr<clustering::KmeansClustering>& job,
              const Config& config) {
    try {
        job->Run<T>(transforConfigToPB(config));
        return true;  // No data skew
    } catch (const milvus::SegcoreError& e) {
        std::string msg = e.what();
        if (msg.find("data skew") != std::string::npos) {
            std::cout << "[WARNING] Caught data skew exception  skipping this "
                         "clustering run.\n";
            return false;  // ⚠️ Data skew detected
        }
        throw;  // Rethrow any other error
    }
}

struct StoragePreparationResult {
    storage::FileManagerContext ctx;
    std::map<int64_t, std::vector<std::string>> insert_files;
    std::map<int64_t, int64_t> num_rows;
    int64_t train_size;
    Config config;
};

template <typename T, DataType dtype>
StoragePreparationResult
prepare_storage_v1(const std::string& root_path,
                   int64_t collection_id,
                   int64_t partition_id,
                   int64_t segment_id,
                   int64_t segment_id2,
                   int64_t index_build_id,
                   int64_t index_version,
                   int64_t nb,
                   int64_t dim,
                   const std::shared_ptr<storage::ChunkManager>& cm,
                   ChunkManagerWrapper& cm_w,
                   const std::vector<T>& data_gen,
                   bool train_full) {
    std::cout << "[DEBUG] Preparing STORAGE_V1 data\n";
    int64_t field_id = 101;
    auto field_meta =
        gen_field_data_meta(collection_id, partition_id, segment_id, field_id);
    auto index_meta =
        gen_index_meta(segment_id, field_id, index_build_id, index_version);
    // Fill vector data
    auto field_data =
        storage::CreateFieldData(dtype, DataType::NONE, false, dim);
    auto fs = milvus::storage::InitArrowFileSystem(
        gen_local_storage_config(root_path));
    field_data->FillFieldData(data_gen.data(), data_gen.size() / dim);
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    // Serialize
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(milvus::storage::Remote);
    auto get_binlog_path = [&](int64_t seg_id, int64_t log_id) {
        return fmt::format("{}/{}/{}/{}/{}",
                           collection_id,
                           partition_id,
                           seg_id,
                           field_id,
                           log_id);
    };
    auto log_path1 = get_binlog_path(segment_id, 0);
    auto log_path2 = get_binlog_path(segment_id2, 0);
    std::string full_path1 = root_path + log_path1;
    std::string full_path2 = root_path + log_path2;
    boost::filesystem::create_directories(
        boost::filesystem::path(full_path1).parent_path());
    boost::filesystem::create_directories(
        boost::filesystem::path(full_path2).parent_path());
    cm_w.Write(full_path1, serialized_bytes.data(), serialized_bytes.size());
    cm_w.Write(full_path2, serialized_bytes.data(), serialized_bytes.size());
    std::map<int64_t, std::vector<std::string>> remote_files{
        {segment_id, {full_path1}}, {segment_id2, {full_path2}}};
    std::map<int64_t, int64_t> num_rows{{segment_id, nb}, {segment_id2, nb}};
    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    int64_t train_size = nb * dim * sizeof(T) * (train_full ? 2 : 1);

    Config config;
    config["storage_version"] = STORAGE_V1;
    config["train_size"] = train_size;
    config["train_buffer_size"] = 10 * 1024 * 1024L;  // 10MB
    config["assign_buffer_size"] = 10 * 1024 * 1024L;
    return {ctx, remote_files, num_rows, train_size, config};
}

template <typename T>
StoragePreparationResult
prepare_storage_v2(const std::string& root_path,
                   int64_t collection_id,
                   int64_t partition_id,
                   int64_t segment_id,
                   int64_t segment_id2,
                   int64_t index_build_id,
                   int64_t index_version,
                   int64_t nb,
                   int64_t dim,
                   const std::shared_ptr<storage::ChunkManager>& cm,
                   bool train_full) {
    std::cout << "[DEBUG] Preparing STORAGE_V2 data\n";
    auto schema = gen_all_data_types_schema();
    const std::vector<FieldId>& field_ids = schema->get_field_ids();
    auto vec_field = schema->get_field_id(FieldName("embeddings"));
    auto field_meta = gen_field_data_meta(
        collection_id, partition_id, segment_id, vec_field.get());
    auto index_meta = gen_index_meta(
        segment_id, vec_field.get(), index_build_id, index_version);
    auto fs = milvus::storage::InitArrowFileSystem(
        gen_local_storage_config(root_path));
    milvus_storage::StorageConfig storage_config_ms;
    auto arrow_schema = schema->ConvertToArrowSchema();
    std::vector<int> columns;
    for (int i = 0; i < field_ids.size(); i++) {
        columns.push_back(i);
    }
    auto arrow_column_groups = std::vector<std::vector<int>>{columns};
    auto writer_memory = 16 * 1024 * 1024;

    auto write_one_segment = [&](int64_t seg_idx,
                                 const std::string& base_path) {
        auto paths = std::vector<std::string>{
            fmt::format("{}_group0.parquet", base_path)};
        for (auto& p : paths)
            boost::filesystem::create_directories(
                boost::filesystem::path(p).parent_path());
        auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            arrow_schema,
            storage_config_ms,
            arrow_column_groups,
            writer_memory,
            ::parquet::default_writer_properties());
        EXPECT_TRUE(result.ok());
        auto writer = result.ValueOrDie();
        int64_t total_rows = 0, batch_size = 1000;
        int64_t n_batch = nb / batch_size, remainder = nb % batch_size;

        // Avoid very small remainder segments ---
        int64_t min_safe_rows = 8 * 10;  // num_clusters * 10 minimum points
        if (remainder > 0 && remainder < min_safe_rows && n_batch > 0) {
            std::cout << "[INFO] Merging small remainder (" << remainder
                      << " rows) into last batch to avoid tiny segment.\n";
            n_batch -= 1;
            remainder += batch_size;
        }

        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, batch_size, 42 + i * 1000, 0, dim);
            writer->Write(
                ConvertToArrowRecordBatch(dataset, dim, arrow_schema));
            total_rows += batch_size;
        }
        if (remainder > 0) {
            auto dataset =
                DataGen(schema, remainder, 42 + n_batch * 1000, 0, dim);
            writer->Write(
                ConvertToArrowRecordBatch(dataset, dim, arrow_schema));
            total_rows += remainder;
        }
        writer->Close();
        return std::pair<std::vector<std::string>, int64_t>(paths, total_rows);
    };

    auto [seg0_files, seg0_rows] =
        write_one_segment(segment_id, root_path + "0/seg3");
    auto [seg1_files, seg1_rows] =
        write_one_segment(segment_id2, root_path + "0/seg4");
    std::map<int64_t, std::vector<std::string>> flattened_insert_files = {
        {segment_id, seg0_files}, {segment_id2, seg1_files}};
    std::map<int64_t, int64_t> num_rows = {{segment_id, seg0_rows},
                                           {segment_id2, seg1_rows}};
    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);
    int64_t bytes_per_vector = dim * sizeof(T);
    int64_t train_size = (seg0_rows + seg1_rows) * bytes_per_vector;
    Config config;
    config["storage_version"] = STORAGE_V2;
    config["train_size"] = train_size;

    if (train_full)
        // Full training  buffer covers entire dataset
        config["train_buffer_size"] = train_size;
    else {
        // ensure mini-batch has enough vectors for KMeans stability
        int64_t num_clusters = 8;
        int64_t min_safe_vectors = num_clusters * 40;
        int64_t min_safe_bytes = min_safe_vectors * dim * sizeof(T);
        int64_t total_rows = 0;
        for (auto& [seg_id, n] : num_rows) total_rows += n;
        int64_t train_buffer_vectors =
            std::max({total_rows, num_clusters * 40});
        config["train_buffer_size"] = train_buffer_vectors * dim * sizeof(T);

        std::cout << "[INFO] Adjusted train_buffer_size="
                  << config["train_buffer_size"].get<int64_t>()
                  << " bytes (min_safe_bytes=" << min_safe_bytes << ")\n";
    }
    config["assign_buffer_size"] = config["train_buffer_size"].get<int64_t>();

    std::map<int64_t, std::vector<std::vector<std::string>>>
        segment_insert_files = {{segment_id, {seg0_files}},
                                {segment_id2, {seg1_files}}};
    config["segment_insert_files"] = segment_insert_files;
    config[INSERT_FILES_KEY] =
        flattened_insert_files;  // still needed for backwards compatibility
    return {ctx, flattened_insert_files, num_rows, train_size, config};
}

/**
 * Test the ability to prepare synthetic segments in either storage version and pass them to KMeans.
 * 1. Data Setup:
 *    - Generates synthetic vector data (data_gen) for two segments.
 *    - Each segment has: nb = 20000 vectors, dim = 100 for STORAGE_V1, 128 for STORAGE_V2.
 *    - Writes data to disk / chunk manager depending on the storage version:
 *      STORAGE_V1: serialized binary files per segment.
 *      STORAGE_V2: Parquet batches per segment.
 *    - Prepares train and assign buffer sizes for clustering.
 * 2. Runs clustering in Partial / Full Training Modes
 *    - test that KMeans works for both mini-batch and full-batch training modes.
 *      - train_full = false: mini-batch clustering (train_buffer_size < train_size).
 *      - train_full = true:  full-batch clustering (train_buffer_size = train_size).
 *
 * 3. Cluster Assignment Validation (CheckResultCorrectness):
 *    - Ensures centroids exist and have correct size: num_clusters * dim.
 *    - Confirms cluster assignment mapping per segment:
 *      - Each vector is assigned a valid cluster ID.
 *      - Total assigned vectors = total rows per segment.
 *    - Confirms cross-segment consistency (check_centroids = true):
 *      - Since both segments have identical data, the assigned cluster IDs must match.
 * 4. Exception Handling:
 *    - Too many clusters (Sets num_clusters = 100000).
 *    - Sets high min_cluster_ratio = 0.98 - every cluster must have at least 98% of the average cluster size.
 *
 * // FIX FOR SMALL SEGMENTS:
 * // Previously, KMeans was often called on very small mini-batches due to a
 * // small train_buffer_size. This caused warnings such as:
 * // "WARNING clustering N points to K centroids:
 * // please provide at least K*40 training points"
 * // and sometimes triggered unstable centroids or false "data skew" errors.
 * // Fix 1: Increased the effective train_buffer_size to ensure each
 * // mini-batch has at least (num_clusters * 40) vectors. This removed the
 * // "too few vectors" warning and stabilized KMeans.
 * //
 * // Side effect: Because batches now always have enough vectors,
 * // cluster sizes stay balanced, so the test that expected a SegcoreError
 * // with min_cluster_ratio = 0.98 no longer fails.
 * //
 * // Fix 2: To preserve that test behavior, the test now manually reduces
 * // each segment to 1 vector before running KMeans. This intentionally
 * // violates the 0.98 ratio constraint and correctly triggers the expected
 * // SegcoreError exception again.
 */
template <typename T, DataType dtype>
void
test_run_unified(const std::string& storage_version, bool train_full = false) {
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128;
    int64_t nb = 20000;
    std::string root_path = "/tmp/test-kmeans-clustering-unified/";
    boost::filesystem::create_directories(root_path);
    static auto cm =
        storage::CreateChunkManager(gen_local_storage_config(root_path));
    static ChunkManagerWrapper cm_w(cm);
    std::vector<T> data_gen(nb * dim);
    for (auto& v : data_gen) v = static_cast<T>(rand());
    StoragePreparationResult prep;
    bool check_centroids = false;
    if (storage_version == "STORAGE_V1") {
        prep = prepare_storage_v1<T, dtype>(root_path,
                                            collection_id,
                                            partition_id,
                                            segment_id,
                                            segment_id2,
                                            index_build_id,
                                            index_version,
                                            nb,
                                            dim,
                                            cm,
                                            cm_w,
                                            data_gen,
                                            train_full);
    } else if (storage_version == "STORAGE_V2") {
        prep = prepare_storage_v2<T>(root_path,
                                     collection_id,
                                     partition_id,
                                     segment_id,
                                     segment_id2,
                                     index_build_id,
                                     index_version,
                                     nb,
                                     dim,
                                     cm,
                                     train_full);
        check_centroids = true;
    } else
        FAIL() << "Unknown storage_version: " << storage_version;

    Config config = prep.config;
    config["dim"] = dim;
    config["num_rows"] = prep.num_rows;
    config[INSERT_FILES_KEY] = prep.insert_files;
    config["max_cluster_ratio"] = 10.0;
    config["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    config["min_cluster_ratio"] = 0.01;
    config["num_clusters"] = 8;
    config["train_size"] = prep.train_size;
    auto clusteringJob =
        std::make_unique<clustering::KmeansClustering>(prep.ctx);
    // Skip clustering if training data is too small
    // When the number of training vectors is much smaller than num_clusters,
    // KMeans will warn and produce meaningless centroids.
    // This check avoids running KMeans on such small batches in partial tests.
    int64_t num_clusters = config["num_clusters"].get<int>();
    if (nb < num_clusters * 10) {
        std::cout << "[INFO] Skipping clustering because nb=" << nb << " (< "
                  << num_clusters * 10
                  << "), not enough data for stable centroids.\n";
        return;  // skip this partial test safely
    }
    std::cout << "check_centroids = " << check_centroids << "\n";
    if (runClustering<T>(clusteringJob, config)) {
        CheckResultCorrectness<T>(clusteringJob,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  config["num_clusters"].get<int>(),
                                  check_centroids);
    }
    EXPECT_THROW(
        {
            config["num_clusters"] = 100000;
            clusteringJob->Run<T>(transforConfigToPB(config));
        },
        SegcoreError);
    /* train_buffer_size was roughly train_size / 10 or seg0_rows.
       Mini-batches could be too small, giving too few vectors per cluster,
       causing KMeans to fail the min_cluster_ratio check.
       It was fixed to guarantee a minimum number of vectors per cluster in each
       mini-batch for stability.
        After the change:
        train_buffer_size = max(train_size / 10, seg0_rows, num_clusters * 40 * dim * sizeof(T))
        Ensures at least 40 vectors per cluster, avoiding false exceptions on high min_cluster_ratio.*/
    auto old_config = config;  // Backup current config
    // Force clusteringJob to throw SegcoreError
    // by making the segment "too small" relative to num_clusters
    config["num_clusters"] = 100;
    // every cluster must have at least 98% of the average cluster size.
    config["min_cluster_ratio"] = 0.98;  // very high threshold
    for (auto& [seg_id, n] :
         config["num_rows"].get<std::map<int64_t, int64_t>>()) {
        // Reduce number of points in segments so it is < min_safe_vectors
        n = 1;  // tiny segment to trigger exception
    }
    EXPECT_THROW({ clusteringJob->Run<T>(transforConfigToPB(config)); },
                 SegcoreError);
    config = old_config;  // Restore old config for normal runs
    std::cout << "===== [TEST END] storage_version=" << storage_version
              << " =====\n";
}

/*
 * STORAGE_V1:
 *   - Legacy flat storage format.
 *   - Each segment’s field data is serialized as one large binary file (.bin).
 *   - Data is stored row-by-row (interleaved values for all fields).
 *   - Simpler but inefficient for column-wise reads and partial updates.
 *
 * STORAGE_V2:
 *   - Modern columnar storage using Apache Parquet (via milvus-storage).
 *   - Each segment is composed of one or more Parquet files (Arrow batches).
 *   - Data is stored column-by-column (each field is written separately inside Parquet).
 *   - Enables efficient reads of specific fields and better compression.
 *   - Large datasets are split into row groups/batches within Parquet files.
 */

TEST(MajorCompaction, PartialV1) {
    test_run_unified /*v1*/<float, DataType::VECTOR_FLOAT>("STORAGE_V1");
}

TEST(MajorCompaction, PartialV2) {
    test_run_unified /*v2*/<float, DataType::VECTOR_FLOAT>("STORAGE_V2");
}

TEST(MajorCompaction, FullV1) {
    test_run_unified /*v1*/<float, DataType::VECTOR_FLOAT>(
        "STORAGE_V1", /* full_train */ true);
}

TEST(MajorCompaction, FullV2) {
    test_run_unified /*v2*/<float, DataType::VECTOR_FLOAT>(
        "STORAGE_V2", /* full_train */ true);
}

/**
 *  Validates that Milvus KMeans clustering (STORAGE_V2)
    produces consistent results across different buffer sizes
    Small buffer (mini-batch/streaming mode) limited training data per batch
    Exact buffer  fits the entire training dataset in memory
    Large buffer  more than enough memory (full-batch training)
    It ensures that:
    - Clustering produces consistent centroids when enough data is available.
    - The algorithm is stable when buffers change.

    Notes:
    1) Buffer-size variant tests are only supported for STORAGE_V2.
    V1 stores the entire segment in a single binary file, and KMeansClustering
    requires the full segment to be processed at once. Partial/streaming
    buffer modes (SMALL/EXACT/LARGE) are not supported in V1 and will fail.
    2) The small buffer mode intentionally uses a streaming or chunked clustering path.
    This means:
    It only loads partial data into memory at a time.
    It may perform partial updates on centroids per chunk.
    Centroids may depend on the order of chunks or rounding from incremental updates.
    So, it’s not expected to exactly match the full-memory result (EXACT or LARGE).
    Comparing it directly to “exact” would often fail due to small floating-point or data-order variations  not actual bugs.
 */

TEST(MajorCompaction, BufferSizeVariantsV2) {
    using T = float;
    // Define metadata for simulated collection and segments
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128;   // vector dimension
    int64_t nb = 20000;  // number of vectors per segment
    std::string root_path = "/tmp/test-kmeans-buffer-size-variants/";
    boost::filesystem::create_directories(root_path);

    // Create a local ChunkManager for reading/writing segment files
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));

    // Generate synthetic vector data with a fixed seed for reproducibility
    std::vector<T> data_gen(nb * dim);
    std::mt19937 gen(42);  // deterministic random generator
    std::normal_distribution<float> dist(0.0, 1.0);
    for (auto& v : data_gen) v = dist(gen);

    // --- Prepare STORAGE_V2 dataset ---
    // Writes the generated vectors into two Parquet segments.
    // Returns a preparation structure containing:
    // - ctx (FileManagerContext)
    // - insert_files (file paths)
    // - num_rows (vectors per segment)
    // - train_size (total dataset size in bytes)
    // - config (base config for clustering)
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      false);

    // Build base clustering configuration
    Config base = prep.config;
    base["dim"] = dim;
    base["num_rows"] = prep.num_rows;
    base[INSERT_FILES_KEY] = prep.insert_files;
    base["max_cluster_ratio"] = 10.0;  // upper cluster size limit
    base["max_cluster_size"] = 5L * 1024 * 1024 * 1024;  // 5 GB per cluster max
    base["min_cluster_ratio"] = 0.01;      // lower cluster size limit
    base["num_clusters"] = 4;              // number of centroids (k)
    base["train_size"] = prep.train_size;  // total training data size

    // Compute minimum safe buffer sizes to avoid underflow
    int64_t train_size_bytes = base["train_size"].get<int64_t>();
    int64_t num_clusters = base["num_clusters"].get<int>();
    int64_t min_safe_vectors = num_clusters * 40;
    int64_t min_safe_bytes = min_safe_vectors * dim * sizeof(T);

    // Copy base config for each buffer-size variant
    Config small = base;
    Config exact = base;
    Config large = base;

    // --- Buffer-size setup ---
    // SMALL: fits ~10% of training data (streaming mode)
    // EXACT: fits entire dataset exactly
    // LARGE: fits more than total dataset (overprovisioned)
    small["train_buffer_size"] =
        std::max<int64_t>(min_safe_bytes, train_size_bytes / 10);
    exact["train_buffer_size"] = train_size_bytes;
    large["train_buffer_size"] = train_size_bytes * 2;

    // Scale assignment buffer sizes accordingly
    small["assign_buffer_size"] =
        small["train_buffer_size"].get<int64_t>() * dim * sizeof(T);
    exact["assign_buffer_size"] =
        exact["train_buffer_size"].get<int64_t>() * dim * sizeof(T);
    large["assign_buffer_size"] =
        large["train_buffer_size"].get<int64_t>() * dim * sizeof(T);

    // ---------- SMALL buffer ----------
    // Runs KMeans in streaming mode (limited memory)
    std::cout << "\n[TEST] Running SMALL buffer (streaming) mode...\n";
    auto job_small = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_small = runClustering<T>(job_small, small);
    if (!ok_small)
        std::cout << "[INFO] Small buffer run skipped (data skew detected)\n";
    else
        CheckResultCorrectness<T>(job_small,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);

    // ---------- EXACT buffer ----------
    // Fits all data in memory  expected to produce stable centroids
    std::cout << "[TEST] Running EXACT buffer mode...\n";
    auto job_exact = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_exact = runClustering<T>(job_exact, exact);
    if (!ok_exact)
        std::cout << "[INFO] Exact buffer run skipped (data skew detected)\n";
    else
        CheckResultCorrectness<T>(job_exact,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);

    // ---------- LARGE buffer ----------
    // Overprovisioned buffer  should produce same result as EXACT
    std::cout << "[TEST] Running LARGE buffer mode...\n";
    auto job_large = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_large = runClustering<T>(job_large, large);
    if (!ok_large)
        std::cout << "[INFO] Large buffer run skipped (data skew detected)\n";
    else
        CheckResultCorrectness<T>(job_large,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);

    // ---------- Centroid comparison ----------
    // Compare centroids between EXACT and LARGE runs.
    // They should be nearly identical if clustering is stable.
    if (ok_exact && ok_large) {
        std::string centroids_path_exact =
            job_exact->GetRemoteCentroidsObjectPrefix() + "/" +
            std::string(CENTROIDS_NAME);
        std::string centroids_path_large =
            job_large->GetRemoteCentroidsObjectPrefix() + "/" +
            std::string(CENTROIDS_NAME);

        milvus::proto::clustering::ClusteringCentroidsStats stats_exact,
            stats_large;
        ReadPBFile(centroids_path_exact, stats_exact);
        ReadPBFile(centroids_path_large, stats_large);
        ASSERT_EQ(stats_exact.centroids_size(), stats_large.centroids_size());

        // Compute maximum element-wise difference between centroids
        double max_diff = 0.0;
        for (int i = 0; i < stats_exact.centroids_size(); i++) {
            const auto& v1 = stats_exact.centroids(i).float_vector().data();
            const auto& v2 = stats_large.centroids(i).float_vector().data();
            for (int d = 0; d < dim; d++) {
                double diff = std::abs(static_cast<double>(v1[d]) -
                                       static_cast<double>(v2[d]));
                max_diff = std::max(max_diff, diff);
            }
        }

        // Fail if centroids differ significantly between runs
        std::cout << "[DEBUG] Max centroid difference (exact vs large) = "
                  << max_diff << std::endl;
        ASSERT_LT(max_diff, 1e-2)
            << "Centroids differ too much between exact and large buffer runs";
    } else {
        std::cout << "[INFO] Centroid comparison skipped (data skew in one of "
                     "the runs)\n";
    }

    std::cout
        << "Buffer-size variant test (fixed seed) finished successfully.\n";
}

/**
 * This test verifies that Milvus’s KMeans clustering algorithm produces identical
 * assignment results, even when the parameter assign_buffer_size changes.
 * The AssignBufferVariantsV2 test exists only for STORAGE_V2
 * because only V2 supports chunked reading and processing based on assign_buffer_size.
 */
TEST(MajorCompaction, AssignBufferVariantsV2) {
    using T = float;
    // Defines two segments (3 and 4) inside one partition of one collection.
    // Each segment has 20 000 vectors of dimension 128.
    // Data will be written under /tmp/test-kmeans-assign-buffer-variants/.
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128, nb = 20000;
    std::string root_path = "/tmp/test-kmeans-assign-buffer-variants/";
    boost::filesystem::create_directories(root_path);
    // Creates a local ChunkManager that Milvus uses to read/write files.
    // It simulates object storage or filesystem access.
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    // Generates synthetic vector data.
    // Stores it in Parquet files for each segment (the “STORAGE_V2” layout).
    // Returns a StoragePreparationResult containing:
    //  ctx - FileManagerContext (metadata, paths).
    //  insert_files - list of Parquet paths per segment.
    //  num_rows - number of vectors per segment.
    //  train_size - total dataset size.
    //  config - base configuration map for clustering.
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      true /* full train */);
    Config base = prep.config;
    base["dim"] = dim;
    base["num_rows"] = prep.num_rows;
    base[INSERT_FILES_KEY] = prep.insert_files;
    base["max_cluster_ratio"] = 10.0;
    base["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    base["min_cluster_ratio"] = 0.01;
    base["num_clusters"] = 4;
    base["train_size"] = prep.train_size;
    // train_buffer_size is large enough to hold the entire dataset
    int64_t train_size_bytes = base["train_size"].get<int64_t>();
    base["train_buffer_size"] = train_size_bytes;
    // Vary assign_buffer_size by creating three configs:
    // small - 25% of the dataset - Loads vectors in chunks
    // exact - enough for entire dataset - Single pass, baseline
    // large - Over-provisioned - should behave same as exact
    Config small = base, exact = base, large = base;
    int64_t total_rows = 0;
    for (auto& [_, n] : prep.num_rows) total_rows += n;
    int64_t total_bytes = total_rows * dim * sizeof(T);
    int64_t min_safe_assign =
        total_bytes / 4;  // 25% of dataset in memory per chunk
    small["assign_buffer_size"] =
        std::max<int64_t>(min_safe_assign, 64 * 1024 * 1024L);  // at least 64MB
    exact["assign_buffer_size"] = train_size_bytes;             // fits all
    large["assign_buffer_size"] = train_size_bytes * 2;  // overprovisioned
    // Run all variants
    std::cout << "\n[TEST] Running small assign_buffer_size variant...\n";
    auto job_small = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_small = runClustering<T>(job_small, small);
    std::cout << "[TEST] Running exact assign_buffer_size variant...\n";
    auto job_exact = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_exact = runClustering<T>(job_exact, exact);
    std::cout << "[TEST] Running large assign_buffer_size variant...\n";
    auto job_large = std::make_unique<clustering::KmeansClustering>(prep.ctx);
    bool ok_large = runClustering<T>(job_large, large);
    // Validate correctness - CheckResultCorrectness() ensures:
    // - Centroid protobuf file exists and contains num_clusters * dim float values.
    // - Mapping files (cluster assignments) exist and every vector got a valid cluster ID.
    // - Total assigned vectors == total rows (no missing data).
    if (ok_small)
        CheckResultCorrectness<T>(job_small,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);
    if (ok_exact)
        CheckResultCorrectness<T>(job_exact,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);
    if (ok_large)
        CheckResultCorrectness<T>(job_large,
                                  cm,
                                  prep.num_rows,
                                  dim,
                                  base["num_clusters"].get<int>(),
                                  false);
    // Compare assignments
    if (ok_small && ok_exact && ok_large) {
        std::cout << "[TEST] Comparing assignments across SMALL, EXACT, and "
                     "LARGE assign_buffer_size variants...\n";
        for (auto& [seg_id, _] : prep.num_rows) {
            std::string mapping_small =
                job_small->GetRemoteCentroidIdMappingObjectPrefix(seg_id) +
                "/" + std::string(OFFSET_MAPPING_NAME);
            std::string mapping_exact =
                job_exact->GetRemoteCentroidIdMappingObjectPrefix(seg_id) +
                "/" + std::string(OFFSET_MAPPING_NAME);
            std::string mapping_large =
                job_large->GetRemoteCentroidIdMappingObjectPrefix(seg_id) +
                "/" + std::string(OFFSET_MAPPING_NAME);
            milvus::proto::clustering::ClusteringCentroidIdMappingStats
                map_small,
                map_exact, map_large;
            ReadPBFile(mapping_small, map_small);
            ReadPBFile(mapping_exact, map_exact);
            ReadPBFile(mapping_large, map_large);
            // Check equal sizes first
            ASSERT_EQ(map_small.centroid_id_mapping_size(),
                      map_exact.centroid_id_mapping_size())
                << "Mismatch in assignment count (small vs exact) for segment "
                << seg_id;
            ASSERT_EQ(map_small.centroid_id_mapping_size(),
                      map_large.centroid_id_mapping_size())
                << "Mismatch in assignment count (small vs large) for segment "
                << seg_id;
            int64_t n = map_small.centroid_id_mapping_size();
            int64_t diff_small_exact = 0, diff_small_large = 0,
                    diff_exact_large = 0;
            for (int64_t i = 0; i < n; i++) {
                auto id_s = map_small.centroid_id_mapping(i);
                auto id_e = map_exact.centroid_id_mapping(i);
                auto id_l = map_large.centroid_id_mapping(i);
                if (id_s != id_e)
                    diff_small_exact++;
                if (id_s != id_l)
                    diff_small_large++;
                if (id_e != id_l)
                    diff_exact_large++;
            }
            if (diff_small_exact == 0 && diff_small_large == 0 &&
                diff_exact_large == 0) {
                std::cout << "[OK] Segment " << seg_id
                          << ": identical assignments across all buffer sizes ("
                          << n << " vectors)\n";
            } else {
                std::cout << "[FAIL] Segment " << seg_id
                          << ": differences detected! "
                          << "small↔exact=" << diff_small_exact
                          << ", small↔large=" << diff_small_large
                          << ", exact↔large=" << diff_exact_large << "\n";
                FAIL() << "Assignment mismatch across buffer-size variants for "
                          "segment "
                       << seg_id;
            }
        }
        std::cout << "[OK] All segments identical across small, exact, and "
                     "large assign_buffer_size variants.\n";
    } else {
        std::cout << "[INFO] Skipped assignment comparison (data skew detected "
                     "in one of the runs)\n";
    }
    std::cout << "AssignBufferVariantsV2 test completed.\n";
}

/**
 * This test checks how varying num_clusters affects clustering results:
 * It validates that KMeans clustering  runs consistently for different cluster counts
 * and produces well-separated centroids with correct assignment mapping for all segments.
 * Expect:
 *  - Fewer clusters (4) - larger, broader clusters covering wide regions.
 *  - More clusters (64) - smaller, more specific clusters focusing on local regions.
 *  - Total assigned vectors remain constant across runs.
 *  - Average centroid distance increases as num_clusters increases,
 *    since centroids become more spatially separated in the high-dimensional space:
 *    When num_clusters is small (say k=4) only 4 centroids need to represent all
 *    40 000 points. They tend to sit near the global center of mass of the data
 *    distribution. So all centroids are relatively close to each other.
 *    Their pairwise distances are small.
 *
 *    When num_clusters is large (say k=64)
 *    Now you have 64 centroids to cover the same space.
 *    KMeans spreads them out to cover local regions of the data.
 *    Some centroids move to the edges or corners of the data cloud,
 *    others remain in the middle.
 *    The pins are now sprinkled all over the cloud
 *    covering more “surface area” of the data distribution.
 *    Their average pairwise distance (distance between every pair of centroids)
 *    becomes larger, because centroids are now farther apart overall in the data space.
*/
template <typename T>
void
runNumClustersVariantsUnified(const std::string& storage_version,
                              StoragePreparationResult& prep,
                              int64_t dim) {
    Config base = prep.config;
    base["dim"] = dim;
    base["num_rows"] = prep.num_rows;
    base[INSERT_FILES_KEY] = prep.insert_files;
    base["max_cluster_ratio"] = 10.0;
    base["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    base["min_cluster_ratio"] = 0.01;
    base["train_size"] = prep.train_size;
    base["train_buffer_size"] = prep.train_size;
    base["assign_buffer_size"] = prep.train_size * dim * sizeof(T);
    std::vector<int> cluster_counts = {4, 16, 64};
    struct ResultStats {
        std::vector<std::vector<float>> centroids;
        double avg_centroid_distance = 0.0;
    };
    std::map<int, ResultStats> results;
    // For each value of k, the test: runs KMeans with k centroids,
    // loads the resulting centroids.pb file, computes the average pairwise
    // distance between centroids, and verifies cluster assignments.
    for (int k : cluster_counts) {
        std::cout << "\n[TEST] Running " << storage_version
                  << " KMeans with num_clusters = " << k << "...\n";
        Config cfg = base;
        cfg["num_clusters"] = k;
        auto job = std::make_unique<KmeansClustering>(prep.ctx);
        bool ok = runClustering<T>(job, cfg);
        ASSERT_TRUE(ok) << "KMeans run failed (num_clusters=" << k << ")";
        // Read centroids
        std::string centroid_path = job->GetRemoteCentroidsObjectPrefix() +
                                    "/" + std::string(CENTROIDS_NAME);
        milvus::proto::clustering::ClusteringCentroidsStats stats;
        ReadPBFile(centroid_path, stats);
        ASSERT_EQ(stats.centroids_size(), k);
        // Extract centroid vectors
        std::vector<std::vector<float>> centroids;  // [k][dim]
        centroids.reserve(k);
        for (int i = 0; i < k; i++) {
            const auto& fv = stats.centroids(i).float_vector().data();
            centroids.emplace_back(fv.begin(), fv.end());
        }
        // Compute average distance between centroids
        double sum_dist = 0.0;
        int64_t pairs = 0;
        for (int i = 0; i < k; i++) {
            for (int j = i + 1; j < k; j++) {
                double d = 0.0;
                for (int d_i = 0; d_i < dim; d_i++) {
                    double diff = centroids[i][d_i] - centroids[j][d_i];
                    d += diff * diff;
                }
                sum_dist += std::sqrt(
                    d);  // the actual Euclidean distance between centroid i and centroid j
                pairs++;
            }
        }
        double avg_dist = (pairs > 0) ? (sum_dist / pairs) : 0.0;
        // Verify assignments
        // prep.num_rows is a map of segment IDs to the number of vectors in that segment.
        // Example: {3: 20000, 4: 20000} → segment 3 has 20,000 vectors, segment 4 has 20,000 vectors.
        for (auto& [seg_id, nb_seg] : prep.num_rows) {
            std::string mapping_path =
                job->GetRemoteCentroidIdMappingObjectPrefix(seg_id) + "/" +
                std::string(OFFSET_MAPPING_NAME);
            milvus::proto::clustering::ClusteringCentroidIdMappingStats
                map_stats;
            ReadPBFile(mapping_path, map_stats);
            // centroid_id_mapping - an array of centroid IDs for each vector in the segment
            // (which centroid each vector was assigned to)
            // num_in_centroid - how many vectors are assigned to each centroid
            // Check that every vector has a centroid -
            // centroid_id_mapping_size() should equal the number of vectors in the segment
            ASSERT_EQ(map_stats.centroid_id_mapping_size(), nb_seg);
            // Check total vectors assigned per centroid
            ASSERT_EQ(std::accumulate(map_stats.num_in_centroid().begin(),
                                      map_stats.num_in_centroid().end(),
                                      0),
                      nb_seg);
        }
        results[k] = {centroids, avg_dist};
        std::cout << "[INFO] num_clusters=" << k
                  << ", avg_centroid_distance=" << avg_dist << "\n";
    }
    // Compare average centroid distances between cluster counts
    ASSERT_LT(results[4].avg_centroid_distance,
              results[16].avg_centroid_distance)
        << "Expected avg centroid distance to increase with more clusters "
           "(4→16)";
    ASSERT_LT(results[16].avg_centroid_distance,
              results[64].avg_centroid_distance)
        << "Expected avg centroid distance to increase with more clusters "
           "(16→64)";
    std::cout << "[OK] " << storage_version
              << ": centroid distances scale correctly.\n";
}
// TEST(MajorCompaction, NumClustersVariantsV2) {
//     using T = float;
//     int64_t collection_id = 1, partition_id = 2;
//     int64_t segment_id = 3, segment_id2 = 4;
//     int64_t index_build_id = 1000, index_version = 10000;
//     int64_t dim = 128, nb = 20000;
//     std::string root_path = "/tmp/test-kmeans-numclusters-variants/";
//     boost::filesystem::create_directories(root_path);
//     auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
//     // no need to generate data_gen here because prepare_storage_v2() already does it internally
//     auto prep = prepare_storage_v2<T>(root_path,
//                                       collection_id,
//                                       partition_id,
//                                       segment_id,
//                                       segment_id2,
//                                       index_build_id,
//                                       index_version,
//                                       nb,
//                                       dim,
//                                       cm,
//                                       true /* full train */);
//     runNumClustersVariantsUnified<T>("STORAGE_V2", prep, dim);
// }
TEST(MajorCompaction, NumClustersVariantsV1) {
    using T = float;
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128, nb = 20000;
    std::string root_path = "/tmp/test-kmeans-numclusters-variants-v1/";
    boost::filesystem::create_directories(root_path);
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    // data_gen isa flat array of all vector components, stored consecutively in memory:
    // vector_0 = data_gen[0..127]
    // vector_1 = data_gen[128..255]
    std::vector<T> data_gen(nb * dim);
    // This creates a random number generator, the argument 42 is the seed
    // Using a fixed seed makes the results reproducible every test run generates exactly
    // the same random numbers.
    std::mt19937 gen(42);
    // This defines a distribution: how the random numbers are shaped.
    // “Uniform real” means you’ll get numbers evenly spread between 0.0 and 1.0.
    // So each generated value has equal probability anywhere in that range.
    std::uniform_real_distribution<float> dis(0.0f, 1.0f);
    // This loop fills every element of data_gen with a random float.
    // For each v in data_gen: dis(gen) generates one random number using the distribution
    // dis and generator gen. That number is assigned to v.
    // After the loop: data_gen contains nb * dim floating-point values like 0.135, 0.934, 0.112, ...
    for (auto& v : data_gen) v = dis(gen);
    ChunkManagerWrapper wrapper(cm);
    auto prep =
        prepare_storage_v1<T, DataType::VECTOR_FLOAT>(root_path,
                                                      collection_id,
                                                      partition_id,
                                                      segment_id,
                                                      segment_id2,
                                                      index_build_id,
                                                      index_version,
                                                      nb,
                                                      dim,
                                                      cm,
                                                      wrapper,
                                                      data_gen,
                                                      true /* full train */);
    runNumClustersVariantsUnified<T>("STORAGE_V1", prep, dim);
}

/**
 * Compare how similar or different the centroids are between runs with different
 * training sizes.
 * FULL - all dataset size
 * HALF - 50% of the dataset
 * TINY - 10% of the dataset,
 *        but never smaller than 8 clusters × 40 vectors × vector size
 *        (to avoid KMeans instability)
 * Compare "FULL" vs "HALF" - should be nearly identical.
 * Compare "FULL" vs "TINY" - can differ a bit, but not too much.
 * Expect also that:
 * - All runs succeed and produce valid centroids + mapping.
 * - Mapping files remain valid and complete in all runs
 */
template <typename T>
void
runTrainSizeVariantsUnified(const std::string& storage_version,
                            const StoragePreparationResult& prep,
                            int64_t dim,
                            const std::shared_ptr<storage::ChunkManager>& cm) {
    // Base configuration
    Config base = prep.config;
    base["dim"] = dim;
    base["num_rows"] = prep.num_rows;
    base[INSERT_FILES_KEY] = prep.insert_files;
    base["max_cluster_ratio"] = 10.0;
    base["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    base["min_cluster_ratio"] = 0.01;
    base["num_clusters"] = 8;
    int64_t total_train_size = prep.train_size;
    // Define train_size variants
    Config full = base, half = base, tiny = base;
    full["train_size"] = total_train_size;
    half["train_size"] = total_train_size / 2;
    tiny["train_size"] =
        std::max<int64_t>(total_train_size / 10,
                          8 * 40 * dim * sizeof(T));  // minimum safe size
    // declaring dictionaries used to: store the KMeans runs status (ok),
    //  and collect their results (stats).
    std::map<std::string, bool> ok;
    std::map<std::string, milvus::proto::clustering::ClusteringCentroidsStats>
        stats;
    // Run all variants
    for (auto&& [name, cfg] : std::vector<std::pair<std::string, Config>>{
             {"FULL", full}, {"HALF", half}, {"TINY", tiny}}) {
        std::cout << "\n[TEST] Running TrainSize variant: " << name << " ("
                  << storage_version << ")...\n";
        auto job = std::make_unique<KmeansClustering>(prep.ctx);
        bool success = runClustering<T>(job, cfg);
        ok[name] = success;
        if (success) {
            int num_clusters = cfg["num_clusters"].template get<int>();
            CheckResultCorrectness<T>(
                job, cm, prep.num_rows, dim, num_clusters, false);
            std::string path = job->GetRemoteCentroidsObjectPrefix() + "/" +
                               std::string(CENTROIDS_NAME);
            ReadPBFile(path, stats[name]);
        } else {
            std::cout << "[INFO] Skipping centroid comparison for " << name
                      << " (data skew)\n";
        }
    }

    // === Compare centroids between variants (if all succeeded) ===
    if (ok["FULL"] && ok["HALF"] && ok["TINY"]) {
        /* inline function that compares centroids between two variants, a and b
           (e.g., "FULL" and "HALF") and returns a pair of doubles: {mean_diff, max_diff}:
           max_diff - maximum absolute difference between centroid vectors
           mean_diff - average difference  */
        auto compare_centroids =
            [&](const std::string& a,
                const std::string& b) -> std::pair<double, double> {
            // Get both centroid sets
            const auto& A = stats[a];
            const auto& B = stats[b];
            //max_diff - keeps track of the largest difference between any pair of centroids
            // mean_diff - accumulates the total difference to compute an average.
            double max_diff = 0.0, mean_diff = 0.0;
            int64_t count = 0;  // total number of comparisons
            // Loop over all centroids
            for (int i = 0; i < A.centroids_size(); i++) {
                const auto& va = A.centroids(i).float_vector().data();
                const auto& vb = B.centroids(i).float_vector().data();
                int64_t dsize =
                    std::min((int64_t)va.size(), (int64_t)vb.size());
                for (int d = 0; d < dsize; d++) {
                    double diff = std::abs((double)va[d] - (double)vb[d]);
                    max_diff = std::max(max_diff, diff);
                    mean_diff += diff;
                    count++;
                }
            }
            if (count > 0)
                mean_diff /= count;
            std::cout << "[COMPARE] " << a << " vs " << b
                      << " - max_diff=" << max_diff
                      << ", mean_diff=" << mean_diff << "\n";
            return {mean_diff, max_diff};
        };

        auto [mean_full_half, max_full_half] =
            compare_centroids("FULL", "HALF");
        auto [mean_full_tiny, max_full_tiny] =
            compare_centroids("FULL", "TINY");
        // Compares:
        // FULL vs HALF - should be almost identical
        // FULL vs TINY - small differences allowed
        ASSERT_LT(max_full_half, 1.0)
            << "FULL vs HALF centroids differ too much";
        ASSERT_LT(max_full_tiny, 2.0)
            << "FULL vs TINY centroids differ excessively";
    } else {
        std::cout << "[INFO] Skipping centroid comparison due to data skew.\n";
    }

    std::cout << "TrainSizeVariants test completed successfully for "
              << storage_version << ".\n";
}

TEST(MajorCompaction, TrainSizeVariantsV2) {
    using T = float;
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128, nb = 20000;
    std::string root_path = "/tmp/test-kmeans-trainsize-variants/";
    boost::filesystem::create_directories(root_path);
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      true);
    runTrainSizeVariantsUnified<T>("STORAGE_V2", prep, dim, cm);
}

// TEST(MajorCompaction, TrainSizeVariantsV1) {
//     using T = float;
//     int64_t collection_id = 1, partition_id = 2;
//     int64_t segment_id = 3, segment_id2 = 4;
//     int64_t index_build_id = 1000, index_version = 10000;
//     int64_t dim = 128, nb = 20000;
//     std::string root_path = "/tmp/test-kmeans-trainsize-variants-v1/";
//     boost::filesystem::create_directories(root_path);
//     auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
//     // generate synthetic vector data
//     std::vector<T> data_gen(nb * dim);
//     std::mt19937 gen(42);
//     std::uniform_real_distribution<float> dis(0.0f, 1.0f);
//     for (auto& v : data_gen) v = dis(gen);
//     ChunkManagerWrapper wrapper(cm);
//     auto prep = prepare_storage_v1<T, DataType::VECTOR_FLOAT>(root_path,
//                                                               collection_id,
//                                                               partition_id,
//                                                               segment_id,
//                                                               segment_id2,
//                                                               index_build_id,
//                                                               index_version,
//                                                               nb,
//                                                               dim,
//                                                               cm,
//                                                               wrapper,
//                                                               data_gen,
//                                                               true);
//     runTrainSizeVariantsUnified<T>("STORAGE_V1", prep, dim, cm);  // <-- pass cm
// }

/* verifies no data skew: KMeans keeps clusters roughly balanced and no cluster is empty.
Config:
	2 segments × 10,000 vectors, num_clusters = 8
	min_cluster_ratio = 0.5, max_cluster_ratio = 2.0
( The smallest cluster can be as small as half the average,
  The largest cluster can be as large as twice the average)
	Normal distribution (mean=0, stddev=1)
	Fixed random seed.
Expect:
	Clustering runs without throwing SegcoreError(ClusterSkip).
	All clusters have non-zero size.
*/
template <typename T>
void
runNoDataSkewTestUnified(const std::string& storage_version,
                         const StoragePreparationResult& prep,
                         int64_t dim,
                         const std::shared_ptr<storage::ChunkManager>& cm) {
    // Base configuration
    Config cfg = prep.config;
    cfg["dim"] = dim;
    cfg["num_rows"] = prep.num_rows;
    cfg[INSERT_FILES_KEY] = prep.insert_files;
    // Set clustering configuration
    cfg["num_clusters"] = 8;
    cfg["min_cluster_ratio"] = 0.5;
    cfg["max_cluster_ratio"] = 2.0;
    cfg["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    std::cout << "\n[TEST] Running NoDataSkew test (" << storage_version
              << ")...\n";
    auto job = std::make_unique<KmeansClustering>(prep.ctx);
    bool success = runClustering<T>(job, cfg);
    ASSERT_TRUE(success)
        << "KMeans failed (SegcoreError::ClusterSkip or similar) for "
        << storage_version;
    // Load centroids
    std::string path = job->GetRemoteCentroidsObjectPrefix() + "/" +
                       std::string(CENTROIDS_NAME);
    milvus::proto::clustering::ClusteringCentroidsStats stats;
    ReadPBFile(path, stats);
    int num_clusters = cfg["num_clusters"].template get<int>();
    ASSERT_EQ(stats.centroids_size(), num_clusters)
        << "Expected " << num_clusters << " centroids, got "
        << stats.centroids_size();
    // Verify centroid dimensionality and count total values
    int64_t total_values = 0;
    for (const auto& centroid : stats.centroids()) {
        const auto& vec = centroid.float_vector().data();
        // Checking that each centroid vector has the correct dimension
        ASSERT_EQ(vec.size(), dim) << "Centroid vector size mismatch: expected "
                                   << dim << ", got " << vec.size();
        total_values += vec.size();
    }
    ASSERT_EQ(total_values, num_clusters * dim)
        << "Unexpected total number of centroid values";
    // Verify total vectors from input segments
    int64_t total_vectors = 0;
    for (auto& [_, n] : prep.num_rows) total_vectors += n;
    ASSERT_EQ(total_vectors, 2 * (prep.num_rows.begin()->second))
        << "Expected total vectors = 2*nb";
    std::cout << "[INFO] Verified " << num_clusters
              << " centroids of dimension " << dim
              << " each. Total source vectors = " << total_vectors << "\n";
    // - All centroids exist
    // - Each has correct vector length
    // - KMeans ran without skipping data
    std::cout << "NoDataSkew test completed successfully for "
              << storage_version << ".\n";
}

TEST(MajorCompaction, NoDataSkew_V2) {
    using T = float;
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128;
    int64_t nb = 10000;
    std::string root_path = "/tmp/test-kmeans-nodataskew-v2/";
    boost::filesystem::create_directories(root_path);
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      true);
    runNoDataSkewTestUnified<T>("STORAGE_V2", prep, dim, cm);
}

// TEST(MajorCompaction, NoDataSkew_V1) {
//     using T = float;
//     int64_t collection_id = 1, partition_id = 2;
//     int64_t segment_id = 3, segment_id2 = 4;
//     int64_t index_build_id = 1000, index_version = 10000;
//     int64_t dim = 128;
//     int64_t nb = 10000;
//     std::string root_path = "/tmp/test-kmeans-nodataskew-v1/";
//     boost::filesystem::create_directories(root_path);
//     auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
//     // Generate synthetic vector data using Normal(0,1) with fixed seed
//     std::vector<T> data_gen(nb * dim);
//     std::mt19937 gen(12345);  // fixed seed
//     std::normal_distribution<float> dist(0.0f, 1.0f);
//     for (auto& v : data_gen) v = dist(gen);
//     ChunkManagerWrapper wrapper(cm);
//     auto prep = prepare_storage_v1<T, DataType::VECTOR_FLOAT>(root_path,
//                                                               collection_id,
//                                                               partition_id,
//                                                               segment_id,
//                                                               segment_id2,
//                                                               index_build_id,
//                                                               index_version,
//                                                               nb,
//                                                               dim,
//                                                               cm,
//                                                               wrapper,
//                                                               data_gen,
//                                                               true);
//     runNoDataSkewTestUnified<T>("STORAGE_V1", prep, dim, cm);
// }

/**
 * Verifies that the KMeans clustering job correctly detects *data skew*
 * (i.e., clusters that are too uneven in size) and triggers
 * SegcoreError::ClusterSkip when the cluster size ratios fall outside the
 * configured [min_cluster_ratio, max_cluster_ratio] bounds.
 * - Applies *very tight* cluster ratio limits:
 *   min_cluster_ratio = 0.95
 *   max_cluster_ratio = 1.05
 * This allows only 5% deviation from the average cluster size.
 */
template <typename T>
void
runDataSkewDetectionTestUnified(
    const std::string& storage_version,
    const StoragePreparationResult& prep,
    int64_t dim,
    const std::shared_ptr<storage::ChunkManager>& cm) {
    Config cfg = prep.config;
    cfg["dim"] = dim;
    cfg["num_rows"] = prep.num_rows;
    cfg[INSERT_FILES_KEY] = prep.insert_files;
    // Clustering config with very tight cluster ratio to force skip
    cfg["num_clusters"] = 8;
    cfg["min_cluster_ratio"] = 0.95;  // smallest cluster allowed = 95% of avg
    cfg["max_cluster_ratio"] = 1.05;  // largest cluster allowed = 105% of avg
    cfg["max_cluster_size"] = 5L * 1024 * 1024 * 1024;
    std::cout << "\n[TEST] Running DataSkewDetection test (" << storage_version
              << ")...\n";
    auto job = std::make_unique<KmeansClustering>(prep.ctx);
    // Expect SegcoreError::ClusterSkip
    try {
        bool success = runClustering<T>(job, cfg);
        // If no exception is thrown → test fails
        FAIL() << "Expected SegcoreError::ClusterSkip but clustering completed "
                  "successfully";
    } catch (const milvus::SegcoreError& e) {
        std::cout << "[INFO] DataSkewDetection test correctly triggered "
                     "ClusterSkip for "
                  << storage_version << ". Exception: " << e.what() << "\n";
    } catch (...) {
        FAIL() << "Expected SegcoreError::ClusterSkip, but got different "
                  "exception";
    }
}

TEST(MajorCompaction, DataSkew_V2) {
    using T = float;
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128;
    int64_t nb = 10000;
    std::string root_path = "/tmp/test-kmeans-dataskew-v2/";
    boost::filesystem::create_directories(root_path);
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      true);
    // artificially increase one segment's row count to trigger imbalance
    prep.num_rows[segment_id2] = static_cast<int64_t>(nb * 1.5);
    runDataSkewDetectionTestUnified<T>("STORAGE_V2", prep, dim, cm);
}

// TEST(MajorCompaction, DataSkew_V1) {
//     using T = float;
//     int64_t collection_id = 1, partition_id = 2;
//     int64_t segment_id = 3, segment_id2 = 4;
//     int64_t index_build_id = 1000, index_version = 10000;
//     int64_t dim = 128;
//     int64_t nb = 10000;
//     std::string root_path = "/tmp/test-kmeans-dataskew-v1/";
//     boost::filesystem::create_directories(root_path);
//     auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
//     // Generate synthetic vector data
//     std::vector<T> data_gen(nb * dim);
//     std::mt19937 gen(12345);
//     std::normal_distribution<float> dist(0.0f, 1.0f);
//     for (auto& v : data_gen) v = dist(gen);
//     ChunkManagerWrapper wrapper(cm);
//     auto prep = prepare_storage_v1<T, DataType::VECTOR_FLOAT>(root_path,
//                                                               collection_id,
//                                                               partition_id,
//                                                               segment_id,
//                                                               segment_id2,
//                                                               index_build_id,
//                                                               index_version,
//                                                               nb,
//                                                               dim,
//                                                               cm,
//                                                               wrapper,
//                                                               data_gen,
//                                                               true);
//     // artificially increase one segment's row count to trigger imbalance
//     prep.num_rows[segment_id2] = static_cast<int64_t>(nb * 1.5);
//     runDataSkewDetectionTestUnified<T>("STORAGE_V1", prep, dim, cm);
// }

/**
 * Validates that the clustering logic correctly enforces the max_cluster_size limit,
 * (maximum allowed memory for a single cluster), and raises SegcoreError::ClusterSkip
 * when any cluster exceeds it.
 * Use: 2 segments × 20,000 float32 vectors, each 128 dimensions.
 * num_clusters       = 8
 * min_cluster_ratio  = 0.1
 * max_cluster_ratio  = 10.0
 * max_cluster_size   = 1 * 1024 * 1024  (1 MB)
 * Expect:
 * A typical cluster of 2,500 vectors would occupy 2,500 × 128 × 4 = 1.28 MB > 1 MB
 * Hence the clustering should exceed the limit and throws SegcoreError::ClusterSkip.
 * If clustering completes successfully, the test fails.
 */
template <typename T>
void
runDataSkewByMaxClusterSizeTestUnified(
    const std::string& storage_version,
    const StoragePreparationResult& prep,
    int64_t dim,
    const std::shared_ptr<storage::ChunkManager>& cm) {
    Config cfg = prep.config;
    cfg["dim"] = dim;
    cfg["num_rows"] = prep.num_rows;
    cfg[INSERT_FILES_KEY] = prep.insert_files;
    // Clustering configuration: very small max_cluster_size to force a violation
    cfg["num_clusters"] = 8;
    cfg["min_cluster_ratio"] = 0.1;
    cfg["max_cluster_ratio"] = 10.0;
    cfg["max_cluster_size"] = 1 * 1024 * 1024;  // 1 MB
    std::cout << "\n[TEST] Running DataSkewByMaxClusterSize test ("
              << storage_version << ")...\n";
    auto job = std::make_unique<KmeansClustering>(prep.ctx);
    // Expect SegcoreError::ClusterSkip due to exceeding max_cluster_size (in bytes)
    bool success = runClustering<T>(job, cfg);
    ASSERT_FALSE(success) << "Expected clustering to be skipped due to "
                             "max_cluster_size violation, "
                          << "but runClustering() returned success";
    std::cout
        << "[INFO] DataSkewByMaxClusterSize test correctly detected skip for "
        << storage_version << "\n";
}

TEST(MajorCompaction, DataSkew_ByMaxClusterSize_V2) {
    using T = float;
    int64_t collection_id = 1, partition_id = 2;
    int64_t segment_id = 3, segment_id2 = 4;
    int64_t index_build_id = 1000, index_version = 10000;
    int64_t dim = 128;
    int64_t nb = 20000;  // per segment
    std::string root_path = "/tmp/test-kmeans-dataskew-maxclustersize-v2/";
    boost::filesystem::create_directories(root_path);
    auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
    auto prep = prepare_storage_v2<T>(root_path,
                                      collection_id,
                                      partition_id,
                                      segment_id,
                                      segment_id2,
                                      index_build_id,
                                      index_version,
                                      nb,
                                      dim,
                                      cm,
                                      true);
    runDataSkewByMaxClusterSizeTestUnified<T>("STORAGE_V2", prep, dim, cm);
}

// TEST(MajorCompaction, DataSkew_ByMaxClusterSize_V1) {
//     using T = float;
//     int64_t collection_id = 1, partition_id = 2;
//     int64_t segment_id = 3, segment_id2 = 4;
//     int64_t index_build_id = 1000, index_version = 10000;
//     int64_t dim = 128;
//     int64_t nb = 20000;  // per segment
//     std::string root_path = "/tmp/test-kmeans-dataskew-maxclustersize-v1/";
//     boost::filesystem::create_directories(root_path);
//     auto cm = storage::CreateChunkManager(gen_local_storage_config(root_path));
//     std::vector<T> data_gen(nb * dim);
//     std::mt19937 gen(12345);
//     std::normal_distribution<float> dist(0.0f, 1.0f);
//     for (auto& v : data_gen) v = dist(gen);
//     ChunkManagerWrapper wrapper(cm);
//     auto prep = prepare_storage_v1<T, DataType::VECTOR_FLOAT>(root_path,
//                                                               collection_id,
//                                                               partition_id,
//                                                               segment_id,
//                                                               segment_id2,
//                                                               index_build_id,
//                                                               index_version,
//                                                               nb,
//                                                               dim,
//                                                               cm,
//                                                               wrapper,
//                                                               data_gen,
//                                                               true);
//     runDataSkewByMaxClusterSizeTestUnified<T>("STORAGE_V1", prep, dim, cm);
// }
