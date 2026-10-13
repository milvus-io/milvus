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
#include <random>
#include <limits>
#include <string_view>
#include <cmath>
#include <filesystem>
#include <functional>
#include <fstream>
#include <boost/filesystem.hpp>
#include <numeric>
#include <unordered_set>

#include "common/Tracer.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "index/Utils.h"
#include "pb/schema.pb.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "storage/loon_ffi/util.h"
#include "clustering/KmeansClustering.h"
#include "clustering/analyze_c.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "test_utils/Constants.h"
#include "test_utils/ManifestTestUtil.h"
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
    analyze_info.set_cluster_type("CARDINAL_KMEANS");
    (*analyze_info.mutable_cluster_params())["global_train_method"] = "bf";
    (*analyze_info.mutable_cluster_params())["global_assign_method"] = "bf";
    (*analyze_info.mutable_cluster_params())["random_state"] = "42";
    analyze_info.set_num_clusters(config["num_clusters"]);
    analyze_info.set_max_cluster_ratio(config["max_cluster_ratio"]);
    analyze_info.set_min_cluster_ratio(config["min_cluster_ratio"]);
    analyze_info.set_max_cluster_size(config["max_cluster_size"]);
    auto& num_rows = *analyze_info.mutable_num_rows();
    const auto num_rows_map =
        milvus::index::GetValueFromConfig<std::map<int64_t, int64_t>>(
            config, "num_rows")
            .value();
    for (const auto& [k, v] : num_rows_map) {
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

// Skipped jobs must not expose a result. Files uploaded before skew detection
// remain unadopted and are reclaimed by GC; remove this fixture's files here.
template <typename T>
void
CheckResultUnavailable(
    const milvus::clustering::KmeansClusteringPtr& clusteringJob,
    const milvus::storage::ChunkManagerPtr cm,
    int64_t segment_id,
    int64_t segment_id2) {
    std::string centroids_path_prefix =
        clusteringJob->GetRemoteCentroidsObjectPrefix();
    std::string centroid_path =
        centroids_path_prefix + "/" + std::string(CENTROIDS_NAME);
    EXPECT_THROW(clusteringJob->GetClusteringResultMeta(), SegcoreError);
    if (cm->Exist(centroid_path)) {
        cm->Remove(centroid_path);
    }
    std::string offset_mapping_name = std::string(OFFSET_MAPPING_NAME);
    std::string centroid_id_mapping_path =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id) +
        "/" + offset_mapping_name;
    milvus::proto::clustering::ClusteringCentroidIdMappingStats mapping_stats;
    std::string centroid_id_mapping_path2 =
        clusteringJob->GetRemoteCentroidIdMappingObjectPrefix(segment_id2) +
        "/" + offset_mapping_name;
    for (const auto& mapping_path :
         {centroid_id_mapping_path, centroid_id_mapping_path2}) {
        if (cm->Exist(mapping_path)) {
            cm->Remove(mapping_path);
        }
    }
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

    for (const auto* mapping : {&mapping_stats, &mapping_stats2}) {
        ASSERT_EQ(mapping->centroid_id_mapping_size(), nb);
        ASSERT_EQ(mapping->distance_to_centroid_size(), nb);
        ASSERT_EQ(mapping->num_in_centroid_size(), expected_num_clusters);
        std::vector<int64_t> counts(expected_num_clusters, 0);
        for (int64_t row = 0; row < nb; ++row) {
            const auto id = mapping->centroid_id_mapping(row);
            ASSERT_LT(id, expected_num_clusters);
            ++counts[id];
            ASSERT_TRUE(std::isfinite(mapping->distance_to_centroid(row)));
            ASSERT_GE(mapping->distance_to_centroid(row), 0);
        }
        for (int64_t id = 0; id < expected_num_clusters; ++id) {
            ASSERT_EQ(counts[id], mapping->num_in_centroid(id));
        }
    }

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

    std::string root_path = TestLocalPath;
    auto storage_config = gen_local_storage_config(root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    std::vector<T> data_gen(nb * dim);
    for (int64_t i = 0; i < nb * dim; ++i) {
        data_gen[i] = rand();
    }
    auto field_data =
        storage::CreateFieldData(dtype, DataType::NONE, false, dim);
    field_data->FillFieldData(data_gen.data(), data_gen.size() / dim);
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    auto get_binlog_path = [=](int64_t log_id) {
        return fmt::format("{}{}/{}/{}/{}/{}",
                           TestLocalPath,
                           collection_id,
                           partition_id,
                           segment_id,
                           field_id,
                           log_id);
    };

    auto log_path = get_binlog_path(0);
    auto cm_w = ChunkManagerWrapper(cm);
    cm_w.Write(log_path, serialized_bytes.data(), serialized_bytes.size());
    storage::FileManagerContext ctx(field_meta, index_meta, cm, fs);

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
                CheckResultUnavailable<T>(
                    clusteringJob, cm, segment_id, segment_id2);
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
                CheckResultUnavailable<T>(
                    clusteringJob, cm, segment_id, segment_id2);
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

// A StorageV3 segment carries its data in a loon manifest instead of insert
// binlogs: it appears in AnalyzeInfo.manifest_paths and has NO insert_files
// entry. The analyze job must read it through the manifest and still produce
// real centroids covering every row.
TEST(KmeansClusteringTest, ReadFromManifestStorageV3) {
    const int64_t collection_id = 1;
    const int64_t partition_id = 2;
    const int64_t segment_id = 101;
    const int64_t index_build_id = 1000;
    const int64_t index_version = 10000;
    const int64_t dim = 8;
    const int64_t per_batch = 500;
    const int64_t n_batch = 2;
    const int64_t num_rows = per_batch * n_batch;
    const int64_t num_clusters = 2;

    // A single FloatVector field yields exactly one column group. The loon
    // column name is std::to_string(field_id) (Schema::ConvertToLoonArrowSchema),
    // which is the fallback GetFieldDatasFromManifest uses when no
    // storage_column_mapping is set - so vec_fid must be the same value in the
    // schema, in FieldDataMeta.field_id and in AnalyzeInfo.field_schema.
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);

    // Writes real parquet + manifest under <root_path>/<base_path>.
    const std::string root_path = TestLocalPath;
    const std::string base_path = "kmeans_clustering_v3_manifest";
    milvus::test::V3SegmentTestData v3(
        schema, n_batch, per_batch, dim, root_path, base_path, "", per_batch);
    ASSERT_EQ(v3.TotalRows(), num_rows);
    ASSERT_EQ(v3.NumColumnGroups(), 1);

    // The fixture wrote to the local filesystem and ManifestPathJson() returns
    // an absolute base_path. MakeInternalPropertiesFromStorageConfig uses "/"
    // as the local filesystem root, leaving that complete path unchanged.
    // Leaving StorageConfig at its default ("minio") would send the manifest
    // reader to object storage.
    auto storage_config = gen_local_storage_config(root_path);
    ASSERT_EQ(storage_config.storage_type, "local");
    ASSERT_EQ(storage_config.root_path, root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    milvus::proto::clustering::AnalyzeInfo info;
    info.set_cluster_type("CARDINAL_KMEANS");
    (*info.mutable_cluster_params())["random_state"] = "42";
    info.set_collectionid(collection_id);
    info.set_partitionid(partition_id);
    info.set_buildid(index_build_id);
    info.set_version(index_version);
    info.set_dim(dim);
    info.set_num_clusters(num_clusters);
    info.set_min_cluster_ratio(0.01);
    info.set_max_cluster_ratio(10.0);
    info.set_max_cluster_size(int64_t(1) << 30);
    (*info.mutable_num_rows())[segment_id] = num_rows;
    // StorageV3 marker: manifest present, and deliberately no insert_files.
    (*info.mutable_manifest_paths())[segment_id] = v3.ManifestPathJson();
    ASSERT_TRUE(info.insert_files().empty());
    auto* field_schema = info.mutable_field_schema();
    field_schema->set_fieldid(vec_fid.get());
    field_schema->set_data_type(milvus::proto::schema::DataType::FloatVector);
    auto* pb_storage_config = info.mutable_storage_config();
    pb_storage_config->set_storage_type(storage_config.storage_type);
    pb_storage_config->set_root_path(storage_config.root_path);

    // Mirror analyze_c.cpp: the field schema is carried in FieldDataMeta, and
    // the loon properties come from the production storage-config helper.
    milvus::storage::FieldDataMeta field_data_meta{
        collection_id, partition_id, 0, vec_fid.get(), info.field_schema()};
    milvus::storage::IndexMeta index_meta{
        0, vec_fid.get(), index_build_id, index_version};
    milvus::storage::FileManagerContext ctx(
        field_data_meta, index_meta, cm, fs);
    ctx.set_loon_ffi_properties(MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config)));

    // Both full-budget and sampled training must assign every original row.
    for (const std::string method : {"bf", "index"}) {
        SCOPED_TRACE(method);
        (*info.mutable_cluster_params())["global_assign_method"] = method;
        for (const int64_t train_rows : {num_rows, num_rows / 2}) {
            SCOPED_TRACE(train_rows);
            info.set_train_size(train_rows * dim * int64_t(sizeof(float)));
            auto job =
                std::make_unique<milvus::clustering::KmeansClustering>(ctx);
            job->Run<float>(info);

            auto meta = job->GetClusteringResultMeta();
            ASSERT_FALSE(meta.centroid_path.empty());
            ASSERT_GT(meta.centroid_file_size, 0);
            ASSERT_EQ(meta.id_mappings.size(), size_t(1));

            // Centroids must describe real data pulled through the manifest.
            std::string centroid_path = meta.centroid_path;
            milvus::proto::clustering::ClusteringCentroidsStats centroid_stats;
            ReadPBFile(centroid_path, centroid_stats);
            ASSERT_EQ(centroid_stats.centroids_size(), num_clusters);
            bool any_non_zero = false;
            for (const auto& centroid : centroid_stats.centroids()) {
                ASSERT_EQ(centroid.float_vector().data_size(), dim);
                for (float value : centroid.float_vector().data()) {
                    ASSERT_TRUE(std::isfinite(value));
                    if (value != 0.0f) {
                        any_non_zero = true;
                    }
                }
            }
            ASSERT_TRUE(any_non_zero);

            // Every row of the manifest-backed segment must have been read and
            // assigned; this is what fails if the manifest read returns nothing.
            std::string id_mapping_path =
                job->GetRemoteCentroidIdMappingObjectPrefix(segment_id) + "/" +
                std::string(OFFSET_MAPPING_NAME);
            milvus::proto::clustering::ClusteringCentroidIdMappingStats
                mapping_stats;
            ReadPBFile(id_mapping_path, mapping_stats);
            ASSERT_EQ(mapping_stats.centroid_id_mapping_size(), num_rows);
            ASSERT_EQ(mapping_stats.distance_to_centroid_size(), num_rows);
            ASSERT_EQ(mapping_stats.num_in_centroid_size(), num_clusters);
            std::vector<int64_t> histogram(num_clusters, 0);
            for (int64_t batch = 0; batch < n_batch; ++batch) {
                const auto input = milvus::segcore::DataGen(
                    schema, per_batch, 42 + batch * per_batch);
                const auto vectors = input.get_col<float>(vec_fid);
                for (int64_t row = 0; row < per_batch; ++row) {
                    const auto offset = batch * per_batch + row;
                    const auto id = mapping_stats.centroid_id_mapping(offset);
                    ASSERT_LT(id, num_clusters);
                    ++histogram[id];
                    const auto& centroid =
                        centroid_stats.centroids(id).float_vector();
                    double squared_l2 = 0;
                    double squared_norms = 0;
                    for (int64_t d = 0; d < dim; ++d) {
                        const double x = vectors[row * dim + d];
                        const double c = centroid.data(d);
                        squared_l2 += (x - c) * (x - c);
                        squared_norms += x * x + c * c;
                    }
                    const auto distance =
                        mapping_stats.distance_to_centroid(offset);
                    ASSERT_TRUE(std::isfinite(distance));
                    ASSERT_GE(distance, 0);
                    // Index assignment uses provider quantization (RBQ8 by default).
                    // Only the BF path promises the exact FP32 L2 distance.
                    if (method == "bf") {
                        ASSERT_NEAR(distance,
                                    squared_l2,
                                    8 * std::numeric_limits<float>::epsilon() *
                                        squared_norms);
                    }
                }
            }
            for (int64_t id = 0; id < num_clusters; ++id) {
                ASSERT_EQ(mapping_stats.num_in_centroid(id), histogram[id]);
            }

            cm->Remove(centroid_path);
            cm->Remove(id_mapping_path);
        }
    }
    std::filesystem::remove_all(std::filesystem::path(root_path) /
                                std::filesystem::path(base_path));
}

namespace milvus::clustering {

// Exercise the production sampling readers without depending on Knowhere's
// KMEANS Train/Assign registrations. A fixed seed makes row selection repeatable.
class KmeansSamplingTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        const auto* info =
            ::testing::UnitTest::GetInstance()->current_test_info();
        root_path_ = std::string(TestLocalPath) + "kmeans_random_sampling/" +
                     info->test_suite_name() + "/" + info->name() + "/";
        storage_config_ = gen_local_storage_config(root_path_);
        cm_ = storage::CreateChunkManager(storage_config_);
        fs_ = storage::InitArrowFileSystem(storage_config_);
    }

    void
    TearDown() override {
        std::filesystem::remove_all(root_path_);
    }

    storage::FileManagerContext
    Context() {
        return storage::FileManagerContext(gen_field_data_meta(1, 2, 1, 101),
                                           gen_index_meta(1, 101, 1000, 1),
                                           cm_,
                                           fs_);
    }

    template <typename T>
    std::vector<T>
    Sample(KmeansClustering& job,
           const std::map<int64_t, std::vector<std::string>>& files,
           const std::map<int64_t, int64_t>& rows,
           int64_t sample_rows,
           int64_t dim,
           bool random_sample,
           uint64_t seed,
           const std::map<int64_t, std::string>& manifests = {},
           const Config& config = {}) {
        std::vector<int64_t> segment_ids;
        for (const auto& [id, count] : rows) {
            segment_ids.push_back(id);
        }
        std::vector<T> result(sample_rows * dim);
        std::mt19937_64 rng(seed);
        job.SampleTrainData<T>(segment_ids,
                               files,
                               rows,
                               manifests,
                               config,
                               result.size() * sizeof(T),
                               dim,
                               random_sample,
                               rng,
                               reinterpret_cast<uint8_t*>(result.data()));
        return result;
    }

    template <typename T, DataType dtype>
    std::map<int64_t, std::vector<std::string>>
    WriteBinlogs() {
        std::map<int64_t, std::vector<std::string>> files;
        int64_t first_row = 0;
        // Unequal segments and more files than a single fetch batch. Each
        // vector encodes its global row number in all three components.
        for (const auto& [id, count] :
             std::map<int64_t, int64_t>{{1, 40}, {2, 2}}) {
            for (int64_t start = 0; start < count; start += 4) {
                const auto rows = std::min<int64_t>(4, count - start);
                std::vector<T> vectors(rows * 3);
                for (int64_t i = 0; i < rows * 3; ++i) {
                    vectors[i] =
                        T(static_cast<float>((first_row + start) * 3 + i));
                }
                auto data =
                    storage::CreateFieldData(dtype, DataType::NONE, false, 3);
                data->FillFieldData(vectors.data(), rows);
                auto reader = std::make_shared<storage::PayloadReader>(data);
                storage::InsertData insert(reader);
                insert.SetFieldDataMeta(gen_field_data_meta(1, 2, id, 101));
                insert.SetTimestamps(0, 100);
                auto bytes = insert.Serialize(storage::Remote);
                const auto path =
                    fmt::format("{}{}/{}", root_path_, id, start / 4 + 1);
                cm_->Write(path, bytes.data(), bytes.size());
                files[id].push_back(path);
            }
            std::reverse(files[id].begin(), files[id].end());
            first_row += count;
        }
        return files;
    }

    template <typename T, DataType dtype>
    void
    CheckBinlogSampling() {
        const auto files = WriteBinlogs<T, dtype>();
        const std::map<int64_t, int64_t> rows{{1, 40}, {2, 2}, {3, 0}};
        KmeansClustering job(Context());
        const auto sample = Sample<T>(job, files, rows, 8, 3, true, 42);
        std::unordered_set<int64_t> selected;
        for (int64_t i = 0; i < 8; ++i) {
            const auto row = int64_t(float(sample[i * 3])) / 3;
            ASSERT_GE(row, 0);
            ASSERT_LT(row, 42);
            ASSERT_TRUE(selected.insert(row).second);
            for (int64_t d = 0; d < 3; ++d) {
                ASSERT_EQ(float(sample[i * 3 + d]), row * 3 + d);
            }
        }
        // A range sampler can only return one contiguous block per segment.
        std::vector<int64_t> sorted(selected.begin(), selected.end());
        std::sort(sorted.begin(), sorted.end());
        ASSERT_GT(sorted.back() - sorted.front(), 8);

        // A budget smaller than the number of non-empty segments is valid.
        ASSERT_EQ(Sample<T>(job, files, rows, 1, 3, true, 42).size(), 3);
        ASSERT_TRUE(Sample<T>(job, {}, rows, 0, 3, true, 42).empty());
        const auto full = Sample<T>(job, files, rows, 42, 3, false, 42);
        for (int64_t i = 0; i < full.size(); ++i) {
            ASSERT_EQ(float(full[i]), i);
        }

        auto incorrect_rows = rows;
        incorrect_rows[1] += 1;
        EXPECT_THROW(Sample<T>(job, files, incorrect_rows, 8, 3, true, 42),
                     SegcoreError);
        EXPECT_THROW(Sample<T>(job, files, incorrect_rows, 43, 3, false, 42),
                     SegcoreError);
        incorrect_rows[1] -= 2;
        EXPECT_THROW(Sample<T>(job, files, incorrect_rows, 8, 3, true, 42),
                     SegcoreError);
    }

    void
    ReadyResult(KmeansClustering& job) {
        job.cluster_result_.centroid_path = "centroid-generation-2";
        job.cluster_result_.centroid_file_size = 100;
        job.cluster_result_.id_mappings = {{"mapping-generation-2", 200}};
        job.is_runned_ = true;
    }

    auto
    Mapping(KmeansClustering& job,
            const int64_t* ids,
            const float* distances,
            int64_t rows,
            int64_t k) {
        return job.CentroidIdMappingWithDistanceToPB(ids, distances, rows, k);
    }

    std::string root_path_;
    storage::StorageConfig storage_config_;
    storage::ChunkManagerPtr cm_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
};

TEST_F(KmeansSamplingTest, BinlogFloat) {
    CheckBinlogSampling<float, DataType::VECTOR_FLOAT>();
}

TEST_F(KmeansSamplingTest, UploadFailureDoesNotExposeResult) {
    KmeansClustering job(Context());
    Config config;
    config[INSERT_FILES_KEY] = WriteBinlogs<float, DataType::VECTOR_FLOAT>();
    config["num_rows"] = std::map<int64_t, int64_t>{{1, 40}, {2, 2}};
    config["num_clusters"] = 2;
    config["dim"] = 3;
    config["train_size"] = 42 * 3 * sizeof(float);
    config["min_cluster_ratio"] = 0.01;
    config["max_cluster_ratio"] = 10.0;
    config["max_cluster_size"] = int64_t(1) << 30;
    const auto info = transforConfigToPB(config);

    // A directory at the second output's filename causes a real write failure
    // after the centroid and the first segment assignment have been uploaded.
    const auto blocked_path = job.GetRemoteCentroidIdMappingObjectPrefix(2) +
                              "/" + std::string(OFFSET_MAPPING_NAME);
    std::filesystem::create_directories(blocked_path);
    EXPECT_THROW(job.Run<float>(info), SegcoreError);
    EXPECT_TRUE(cm_->Exist(job.GetRemoteCentroidsObjectPrefix() + "/" +
                           std::string(CENTROIDS_NAME)));
    EXPECT_TRUE(cm_->Exist(job.GetRemoteCentroidIdMappingObjectPrefix(1) + "/" +
                           std::string(OFFSET_MAPPING_NAME)));
    EXPECT_THROW(job.GetClusteringResultMeta(), SegcoreError);

    std::filesystem::remove_all(blocked_path);
    job.Run<float>(info);
    EXPECT_EQ(job.GetClusteringResultMeta().id_mappings.size(), 2);
}

TEST_F(KmeansSamplingTest, AssignmentValidation) {
    KmeansClustering job(Context());
    int64_t ids[] = {1, 0, 1};
    float distances[] = {2, 0, 3};
    auto result = Mapping(job, ids, distances, 3, 2);
    EXPECT_EQ(result.num_in_centroid(0), 1);
    EXPECT_EQ(result.num_in_centroid(1), 2);
    EXPECT_EQ(result.distance_to_centroid(2), 3);
    EXPECT_THROW(Mapping(job, nullptr, distances, 3, 2), SegcoreError);
    ids[0] = -1;
    EXPECT_THROW(Mapping(job, ids, distances, 3, 2), SegcoreError);
    ids[0] = 2;
    EXPECT_THROW(Mapping(job, ids, distances, 3, 2), SegcoreError);
    ids[0] = 1;
    distances[0] = std::numeric_limits<float>::quiet_NaN();
    EXPECT_THROW(Mapping(job, ids, distances, 3, 2), SegcoreError);
    EXPECT_EQ(Mapping(job, nullptr, nullptr, 0, 2).num_in_centroid_size(), 2);
}

TEST_F(KmeansSamplingTest, ResultCapacityAndLifetime) {
    KmeansClustering job(Context());
    ReadyResult(job);
    const char* centroid = nullptr;
    int64_t centroid_size = 0;
    const char* mappings[1] = {nullptr};
    int64_t sizes[1] = {0};
    auto status = GetAnalyzeResultMeta(
        &job, &centroid, &centroid_size, mappings, sizes, 0);
    EXPECT_NE(status.error_code, 0);
    free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(centroid, nullptr);
    EXPECT_EQ(mappings[0], nullptr);
    status = GetAnalyzeResultMeta(
        &job, &centroid, &centroid_size, mappings, sizes, 1);
    ASSERT_EQ(status.error_code, 0);
    EXPECT_STREQ(centroid, "centroid-generation-2");
    EXPECT_STREQ(mappings[0], "mapping-generation-2");
    EXPECT_EQ(centroid_size, 100);
    EXPECT_EQ(sizes[0], 200);
    // Exported pointers refer to the job's strings, not a temporary result copy.
    EXPECT_EQ(centroid, job.GetClusteringResultMeta().centroid_path.data());
    EXPECT_EQ(mappings[0],
              job.GetClusteringResultMeta().id_mappings.begin()->first.data());
}

class KmeansSamplingManifestTest
    : public KmeansSamplingTest,
      public ::testing::WithParamInterface<std::pair<int64_t, int64_t>> {};

TEST_P(KmeansSamplingManifestTest, SamplingAndFullTrainingOrder) {
    const auto [dim, per_batch] = GetParam();
    constexpr int64_t batches = 3;
    const int64_t total_rows = batches * per_batch;
    // The large case crosses the 128 MiB decoded window. Sampling all but
    // one row ensures the tail window is represented, catching lost offsets
    // or a reservoir that is restarted at a batch boundary.
    const int64_t sample_rows = dim == 8 ? 8 : total_rows - 1;
    auto schema = std::make_shared<Schema>();
    const auto fid = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    milvus::test::V3SegmentTestData v3(
        schema, batches, per_batch, dim, root_path_, "manifest", "", per_batch);
    milvus::proto::schema::FieldSchema field_schema;
    field_schema.set_fieldid(fid.get());
    field_schema.set_data_type(milvus::proto::schema::DataType::FloatVector);
    storage::FileManagerContext ctx(
        storage::FieldDataMeta{1, 2, 0, fid.get(), field_schema},
        storage::IndexMeta{0, fid.get(), 1000, 1},
        cm_,
        fs_);
    ctx.set_loon_ffi_properties(MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config_)));
    KmeansClustering job(ctx);
    Config config;
    config[STORAGE_VERSION_KEY] = STORAGE_V3;
    config[DATA_TYPE_KEY] = DataType::VECTOR_FLOAT;
    config[ELEMENT_TYPE_KEY] = DataType::NONE;
    config[DIM_KEY] = dim;
    const std::map<int64_t, int64_t> rows{{1, batches * per_batch}};
    const std::map<int64_t, std::string> manifests{{1, v3.ManifestPathJson()}};
    std::vector<float> original;
    for (int64_t batch = 0; batch < batches; ++batch) {
        const auto data =
            milvus::segcore::DataGen(schema, per_batch, 42 + batch * per_batch);
        const auto vectors = data.get_col<float>(fid);
        original.insert(original.end(), vectors.begin(), vectors.end());
    }
    {
        const auto full = Sample<float>(
            job, {}, rows, total_rows, dim, false, 42, manifests, config);
        ASSERT_EQ(full, original);
    }
    std::unordered_map<std::string_view, int64_t> source_rows;
    for (int64_t row = 0; row < total_rows; ++row) {
        ASSERT_TRUE(
            source_rows
                .emplace(std::string_view(reinterpret_cast<const char*>(
                                              original.data() + row * dim),
                                          dim * sizeof(float)),
                         row)
                .second);
    }
    const auto sample = Sample<float>(
        job, {}, rows, sample_rows, dim, true, 42, manifests, config);
    std::unordered_set<int64_t> selected;
    for (int64_t i = 0; i < sample_rows; ++i) {
        const auto found = source_rows.find(std::string_view(
            reinterpret_cast<const char*>(sample.data() + i * dim),
            dim * sizeof(float)));
        ASSERT_NE(found, source_rows.end());
        ASSERT_TRUE(selected.insert(found->second).second);
    }
    std::vector<int64_t> sorted(selected.begin(), selected.end());
    std::sort(sorted.begin(), sorted.end());
    ASSERT_GT(sorted.back() - sorted.front(), 8);
    if (dim == 8) {
        auto truncated_rows = rows;
        truncated_rows[1] -= 1;
        EXPECT_THROW(Sample<float>(job,
                                   {},
                                   truncated_rows,
                                   total_rows - 1,
                                   dim,
                                   false,
                                   42,
                                   manifests,
                                   config),
                     SegcoreError);
        EXPECT_THROW(
            Sample<float>(
                job, {}, truncated_rows, 8, dim, true, 42, manifests, config),
            SegcoreError);
        truncated_rows[1] += 2;
        EXPECT_THROW(
            Sample<float>(
                job, {}, truncated_rows, 8, dim, true, 42, manifests, config),
            SegcoreError);
    } else {
        EXPECT_TRUE(selected.count(total_rows - 1) ||
                    selected.count(total_rows - 2));
    }
}

INSTANTIATE_TEST_SUITE_P(
    StorageV3,
    KmeansSamplingManifestTest,
    ::testing::Values(std::make_pair(int64_t(8), int64_t(7)),
                      std::make_pair(int64_t(4096), int64_t(2732))));

}  // namespace milvus::clustering
