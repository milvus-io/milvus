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

#include <gtest/gtest.h>
#include <algorithm>
#include <numeric>
#include <vector>
#include <memory>
#include <string>
#include <unordered_map>
#include <iostream>

#include <arrow/record_batch.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>

#include "common/Schema.h"
#include "common/Types.h"
#include "common/VectorArray.h"
#include "common/Consts.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/common/constants.h"
#include "segcore/SegmentSealed.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "pb/schema.pb.h"
#include "knowhere/comp/index_param.h"
#include "index/IndexFactory.h"
#include "index/VectorIndex.h"
#include "storage/Util.h"
#include "storage/ChunkManager.h"
#include "test_utils/storage_test_utils.h"
#include "common/QueryResult.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

const int64_t DIM = 128;

SchemaPtr
GenVectorArrayTestSchema() {
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64);
    schema->AddDebugVectorArrayField(
        "vector_array", DataType::VECTOR_FLOAT, DIM, knowhere::metric::L2);
    schema->AddField(FieldName("ts"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(int64_fid);
    return schema;
}

class TestVectorArrayStorageV2 : public testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = GenVectorArrayTestSchema();
        segment_ = segcore::CreateSealedSegment(
            schema_,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            true);

        // Initialize file system
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = "/tmp/test_vector_array_for_storage_v2";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();

        // Prepare paths and column groups
        std::vector<std::string> paths = {"test_data/0/10000.parquet",
                                          "test_data/101/10001.parquet"};

        // Create directories for the parquet files
        for (const auto& path : paths) {
            auto dir_path = path.substr(0, path.find_last_of('/'));
            auto status = fs->CreateDir(dir_path);
            EXPECT_TRUE(status.ok())
                << "Failed to create directory: " << dir_path;
        }

        std::vector<std::vector<int>> column_groups = {
            {0, 2}, {1}};  // narrow columns and wide columns
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();

        // Create writer
        milvus_storage::PackedRecordBatchWriter writer(
            fs,
            paths,
            schema_->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory);

        // Generate and write data
        int64_t row_count = 0;
        int start_id = 0;

        std::vector<std::string> str_data;
        for (int i = 0; i < test_data_count_ * chunk_num_; i++) {
            str_data.push_back("test" + std::to_string(i));
        }
        std::sort(str_data.begin(), str_data.end());

        fields_ = {
            {"int64", schema_->get_field_id(FieldName("int64"))},
            {"ts", TimestampFieldID},
            {"vector_array", schema_->get_field_id(FieldName("vector_array"))}};

        auto arrow_schema = schema_->ConvertToArrowSchema();
        for (int chunk_id = 0; chunk_id < chunk_num_;
             chunk_id++, start_id += test_data_count_) {
            std::vector<int64_t> test_data(test_data_count_);
            std::iota(test_data.begin(), test_data.end(), start_id);

            // Create arrow arrays for each field
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            for (int i = 0; i < arrow_schema->fields().size(); i++) {
                if (arrow_schema->fields()[i]->type()->id() ==
                    arrow::Type::INT64) {
                    arrow::Int64Builder builder;
                    auto status = builder.AppendValues(test_data.data(),
                                                       test_data_count_);
                    EXPECT_TRUE(status.ok());
                    std::shared_ptr<arrow::Array> array;
                    status = builder.Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                } else {
                    // vector array
                    arrow::BinaryBuilder builder;

                    for (int row = 0; row < test_data_count_; row++) {
                        milvus::proto::schema::VectorField
                            field_float_vector_array;
                        field_float_vector_array.set_dim(DIM);

                        auto data = generate_float_vector(10, DIM);
                        field_float_vector_array.mutable_float_vector()
                            ->mutable_data()
                            ->Add(data.begin(), data.end());

                        std::string serialized_data;
                        bool success =
                            field_float_vector_array.SerializeToString(
                                &serialized_data);
                        EXPECT_TRUE(success);

                        auto status = builder.Append(serialized_data);
                        EXPECT_TRUE(status.ok());
                    }

                    std::shared_ptr<arrow::Array> array;
                    auto status = builder.Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                }
            }

            // Create record batch
            auto record_batch = arrow::RecordBatch::Make(
                schema_->ConvertToArrowSchema(), test_data_count_, arrays);
            row_count += test_data_count_;
            EXPECT_TRUE(writer.Write(record_batch).ok());
        }
        EXPECT_TRUE(writer.Close().ok());

        LoadFieldDataInfo load_info;
        load_info.field_infos.emplace(
            int64_t(0),
            FieldBinlogInfo{
                int64_t(0),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num_ * test_data_count_),
                std::vector<int64_t>(chunk_num_ * test_data_count_ * 4),
                false,
                std::vector<std::string>({paths[0]})});
        load_info.field_infos.emplace(
            int64_t(101),
            FieldBinlogInfo{int64_t(101),
                            static_cast<int64_t>(row_count),
                            std::vector<int64_t>(chunk_num_ * test_data_count_),
                            std::vector<int64_t>(chunk_num_ * test_data_count_ *
                                                 10 * 4 * DIM),
                            false,
                            std::vector<std::string>({paths[1]})});

        load_info.mmap_dir_path = "";
        load_info.storage_version = 2;
        segment_->AddFieldDataInfoForSealed(load_info);
        for (auto& [id, info] : load_info.field_infos) {
            LoadFieldDataInfo load_field_info;
            load_field_info.storage_version = 2;
            load_field_info.mmap_dir_path = "";
            load_field_info.field_infos.emplace(id, info);
            segment_->LoadFieldData(load_field_info);
        }
    }

    void
    TearDown() override {
        // Clean up test data directory
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        auto status = fs->DeleteDir("/tmp/test_vector_array_for_storage_v2");
        ASSERT_TRUE(status.ok());
    }

 protected:
    SchemaPtr schema_;
    segcore::SegmentSealedUPtr segment_;
    int chunk_num_ = 2;
    int test_data_count_ = 100;
    std::unordered_map<std::string, FieldId> fields_;
};

TEST_F(TestVectorArrayStorageV2, BuildEmbListHNSWIndex) {
    ASSERT_NE(segment_, nullptr);
    ASSERT_EQ(segment_->get_row_count(), test_data_count_ * chunk_num_);

    auto vector_array_field_id = fields_["vector_array"];
    ASSERT_TRUE(segment_->HasFieldData(vector_array_field_id));

    // Get the storage v2 parquet file paths that were already written in SetUp
    std::vector<std::string> paths = {"test_data/101/10001.parquet"};

    // Use the existing Arrow file system from SetUp
    auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                  .GetArrowFileSystem();

    // Prepare for index building
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4000;
    int64_t index_version = 4000;

    auto field_meta =
        milvus::segcore::gen_field_meta(collection_id,
                                        partition_id,
                                        segment_id,
                                        vector_array_field_id.get(),
                                        DataType::VECTOR_ARRAY,
                                        DataType::VECTOR_FLOAT,
                                        false);

    auto index_meta = gen_index_meta(
        segment_id, vector_array_field_id.get(), index_build_id, index_version);

    // Create storage config pointing to the test data location
    auto storage_config =
        gen_local_storage_config("/tmp/test_vector_array_for_storage_v2");
    auto cm = CreateChunkManager(storage_config);

    // Create index using storage v2 config
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_ARRAY;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_EMB_LIST_HNSW;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto emb_list_hnsw_index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info,
            storage::FileManagerContext(field_meta, index_meta, cm, fs));

    // Build index with storage v2 configuration
    Config config;
    config[milvus::index::INDEX_TYPE] =
        knowhere::IndexEnum::INDEX_EMB_LIST_HNSW;
    config[knowhere::meta::METRIC_TYPE] = create_index_info.metric_type;
    config[knowhere::indexparam::M] = "16";
    config[knowhere::indexparam::EF] = "10";
    config[DIM_KEY] = DIM;
    config[INDEX_NUM_ROWS_KEY] =
        test_data_count_ * chunk_num_;  // Important: set row count
    config[STORAGE_VERSION_KEY] = 2;    // Use storage v2
    config[DATA_TYPE_KEY] = DataType::VECTOR_ARRAY;

    // For storage v2, we need to provide segment insert files instead of individual binlog files
    milvus::SegmentInsertFiles segment_insert_files;
    segment_insert_files.emplace_back(
        paths);  // Column group with vector array field
    config[SEGMENT_INSERT_FILES_KEY] = segment_insert_files;
    emb_list_hnsw_index->Build(config);

    auto vec_index =
        dynamic_cast<milvus::index::VectorIndex*>(emb_list_hnsw_index.get());

    // Each row has 10 vectors, so total count should be rows * 10
    EXPECT_EQ(vec_index->Count(), test_data_count_ * chunk_num_ * 10);
    EXPECT_EQ(vec_index->GetDim(), DIM);

    {
        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_lims;
        query_vec_lims.push_back(0);
        query_vec_lims.push_back(3);
        query_vec_lims.push_back(10);
        query_dataset->SetLims(query_vec_lims.data());

        auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};
        milvus::SearchInfo searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::L2;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        vec_index->Query(query_dataset, searchInfo, nullptr, result);
        auto ref_result = SearchResultToJson(result);
        std::cout << ref_result.dump(1) << std::endl;
        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
    }
}
