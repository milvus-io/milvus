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
#include "index/InvertedIndexTantivy.h"
#include "pb/schema.pb.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "storage/loon_ffi/util.h"
#include "clustering/KmeansClustering.h"
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
        schema, n_batch, per_batch, dim, root_path, base_path);
    ASSERT_EQ(v3.TotalRows(), num_rows);
    ASSERT_EQ(v3.NumColumnGroups(), 1);

    // The fixture wrote to the LOCAL filesystem, and
    // MakeInternalPropertiesFromStorageConfig copies storage_type/root_path
    // verbatim into the loon properties. So the storage config must be
    // local-typed and rooted at root_path (ManifestPathJson() returns a
    // relative base_path; loon resolves it against PROPERTY_FS_ROOT_PATH).
    // Leaving StorageConfig at its default ("minio") would send the manifest
    // reader to object storage.
    auto storage_config = gen_local_storage_config(root_path);
    ASSERT_EQ(storage_config.storage_type, "local");
    ASSERT_EQ(storage_config.root_path, root_path);
    auto cm = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    milvus::proto::clustering::AnalyzeInfo info;
    info.set_collectionid(collection_id);
    info.set_partitionid(partition_id);
    info.set_buildid(index_build_id);
    info.set_version(index_version);
    info.set_dim(dim);
    info.set_num_clusters(num_clusters);
    // Sized so train_num == data_num; the manifest route forces full-data
    // training regardless (see KmeansClustering::Run).
    info.set_train_size(num_rows * dim * int64_t(sizeof(float)));
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
    // the loon properties come from the storage config through the very
    // production helper under test - not from MakeInternalLocalProperies.
    milvus::storage::FieldDataMeta field_data_meta{
        collection_id, partition_id, 0, vec_fid.get(), info.field_schema()};
    milvus::storage::IndexMeta index_meta{
        0, vec_fid.get(), index_build_id, index_version};
    milvus::storage::FileManagerContext ctx(
        field_data_meta, index_meta, cm, fs);
    ctx.set_loon_ffi_properties(MakeInternalPropertiesFromStorageConfig(
        ToCStorageConfig(storage_config)));

    auto job = std::make_unique<milvus::clustering::KmeansClustering>(ctx);
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
    milvus::proto::clustering::ClusteringCentroidIdMappingStats mapping_stats;
    ReadPBFile(id_mapping_path, mapping_stats);
    ASSERT_EQ(mapping_stats.centroid_id_mapping_size(), num_rows);
    for (const auto id : mapping_stats.centroid_id_mapping()) {
        ASSERT_LT(id, num_clusters);
    }
    ASSERT_EQ(mapping_stats.num_in_centroid_size(), num_clusters);
    int64_t assigned = 0;
    for (const auto num : mapping_stats.num_in_centroid()) {
        assigned += num;
    }
    ASSERT_EQ(assigned, num_rows);

    cm->Remove(centroid_path);
    cm->Remove(id_mapping_path);
    std::filesystem::remove_all(std::filesystem::path(root_path) /
                                std::filesystem::path(base_path));
}
