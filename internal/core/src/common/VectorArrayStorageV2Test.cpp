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
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "common/QueryResult.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

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
        auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            schema_->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory,
            ::parquet::default_writer_properties());
        EXPECT_TRUE(result.ok());
        auto writer = result.ValueOrDie();

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
                    // vector array - using ListArray
                    // Get field meta to determine element type
                    auto vector_array_field_id = fields_["vector_array"];
                    auto& field_meta =
                        schema_->operator[](vector_array_field_id);
                    auto element_type = field_meta.get_element_type();

                    // Create appropriate value builder based on element type
                    std::shared_ptr<arrow::ArrayBuilder> value_builder;
                    int byte_width = 0;
                    if (element_type == DataType::VECTOR_FLOAT) {
                        byte_width = DIM * sizeof(float);
                        value_builder =
                            std::make_shared<arrow::FixedSizeBinaryBuilder>(
                                arrow::fixed_size_binary(byte_width));
                    } else {
                        FAIL() << "Unsupported element type for VECTOR_ARRAY "
                                  "in test";
                    }

                    auto list_builder = std::make_shared<arrow::ListBuilder>(
                        arrow::default_memory_pool(), value_builder);

                    for (int row = 0; row < test_data_count_; row++) {
                        // Each row contains 3 vectors of dimension DIM
                        auto status = list_builder->Append();
                        EXPECT_TRUE(status.ok());

                        // Generate 3 vectors for this row
                        auto data = generate_float_vector(3, DIM);
                        auto binary_builder = std::static_pointer_cast<
                            arrow::FixedSizeBinaryBuilder>(value_builder);
                        // Append each vector as a fixed-size binary value
                        for (int vec_idx = 0; vec_idx < 3; vec_idx++) {
                            status = binary_builder->Append(
                                reinterpret_cast<const uint8_t*>(
                                    data.data() + vec_idx * DIM));
                            EXPECT_TRUE(status.ok());
                        }
                    }

                    std::shared_ptr<arrow::Array> array;
                    auto status = list_builder->Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                }
            }

            // Create record batch
            auto record_batch = arrow::RecordBatch::Make(
                schema_->ConvertToArrowSchema(), test_data_count_, arrays);
            row_count += test_data_count_;
            EXPECT_TRUE(writer->Write(record_batch).ok());
        }
        EXPECT_TRUE(writer->Close().ok());

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

        load_info.storage_version = 2;
        segment_->AddFieldDataInfoForSealed(load_info);
        for (auto& [id, info] : load_info.field_infos) {
            LoadFieldDataInfo load_field_info;
            load_field_info.storage_version = 2;
            load_field_info.field_infos.emplace(id, info);
            segment_->LoadFieldData(load_field_info);
        }
    }

    void
    TearDown() override {
        // Clean up test data directory
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        fs->DeleteDir("/tmp/test_vector_array_for_storage_v2");
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
    create_index_info.metric_type = knowhere::metric::MAX_SIM;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto emb_list_hnsw_index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info,
            storage::FileManagerContext(field_meta, index_meta, cm, fs));

    // Build index with storage v2 configuration
    Config config;
    config[milvus::index::INDEX_TYPE] = knowhere::IndexEnum::INDEX_HNSW;
    config[knowhere::meta::METRIC_TYPE] = create_index_info.metric_type;
    config[knowhere::indexparam::M] = "16";
    config[knowhere::indexparam::EF] = "10";
    config[DIM_KEY] = DIM;
    config[INDEX_NUM_ROWS_KEY] =
        test_data_count_ * chunk_num_;  // Important: set row count
    config[STORAGE_VERSION_KEY] = 2;    // Use storage v2
    config[DATA_TYPE_KEY] = DataType::VECTOR_ARRAY;
    config[ELEMENT_TYPE_KEY] = DataType::VECTOR_FLOAT;

    // For storage v2, we need to provide segment insert files instead of individual binlog files
    milvus::SegmentInsertFiles segment_insert_files;
    segment_insert_files.emplace_back(
        paths);  // Column group with vector array field
    config[SEGMENT_INSERT_FILES_KEY] = segment_insert_files;
    emb_list_hnsw_index->Build(config);

    auto vec_index =
        dynamic_cast<milvus::index::VectorIndex*>(emb_list_hnsw_index.get());

    // Each row has 3 vectors, so total count should be rows * 3
    EXPECT_EQ(vec_index->Count(), test_data_count_ * chunk_num_ * 3);
    EXPECT_EQ(vec_index->GetDim(), DIM);

    {
        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_offsets;
        query_vec_offsets.push_back(0);
        query_vec_offsets.push_back(3);
        query_vec_offsets.push_back(10);
        query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                           const_cast<const size_t*>(query_vec_offsets.data()));

        auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};
        milvus::SearchInfo searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::MAX_SIM_IP;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        milvus::OpContext op_context;
        vec_index->Query(
            query_dataset, searchInfo, nullptr, &op_context, result);
        auto ref_result = SearchResultToJson(result);
        std::cout << ref_result.dump(1) << std::endl;
        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
        EXPECT_EQ(op_context.storage_usage.scanned_cold_bytes, 0);
        EXPECT_EQ(op_context.storage_usage.scanned_total_bytes, 0);
    }
}

TEST_F(TestVectorArrayStorageV2, BuildEmbListHNSWIndexWithMmap) {
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
    create_index_info.metric_type = knowhere::metric::MAX_SIM_IP;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto emb_list_hnsw_index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info,
            storage::FileManagerContext(field_meta, index_meta, cm, fs));

    // Build index with storage v2 configuration
    Config config;
    config[milvus::index::INDEX_TYPE] = knowhere::IndexEnum::INDEX_HNSW;
    config[knowhere::meta::METRIC_TYPE] = create_index_info.metric_type;
    config[knowhere::indexparam::M] = "16";
    config[knowhere::indexparam::EF] = "10";
    config[DIM_KEY] = DIM;
    config[INDEX_NUM_ROWS_KEY] =
        test_data_count_ * chunk_num_;  // Important: set row count
    config[STORAGE_VERSION_KEY] = 2;    // Use storage v2
    config[DATA_TYPE_KEY] = DataType::VECTOR_ARRAY;
    config[ELEMENT_TYPE_KEY] = DataType::VECTOR_FLOAT;

    // For storage v2, we need to provide segment insert files instead of individual binlog files
    milvus::SegmentInsertFiles segment_insert_files;
    segment_insert_files.emplace_back(
        paths);  // Column group with vector array field
    config[SEGMENT_INSERT_FILES_KEY] = segment_insert_files;
    emb_list_hnsw_index->Build(config);

    auto create_index_result = emb_list_hnsw_index->Upload();
    emb_list_hnsw_index.reset();
    auto index_files = create_index_result->GetIndexFiles();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);

    auto new_emb_list_hnsw_index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            create_index_info,
            storage::FileManagerContext(field_meta, index_meta, cm, fs));
    milvus::index::VectorIndex* vec_index =
        dynamic_cast<milvus::index::VectorIndex*>(
            new_emb_list_hnsw_index.get());
    // mmap load
    {
        auto load_conf = generate_load_conf(
            knowhere::IndexEnum::INDEX_HNSW, knowhere::metric::MAX_SIM_IP, 0);
        load_conf["index_files"] = index_files;
        load_conf["mmap_filepath"] = "mmap/test_emb_list_index";
        load_conf["emb_list_meta_file_path"] = "mmap/test_index_meta";
        load_conf[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    }
    // search
    {
        // Each row has 3 vectors, so total count should be rows * 3
        EXPECT_EQ(vec_index->Count(), test_data_count_ * chunk_num_ * 3);
        EXPECT_EQ(vec_index->GetDim(), DIM);
        auto vec_num = 10;
        std::vector<float> query_vec = generate_float_vector(vec_num, DIM);
        auto query_dataset =
            knowhere::GenDataSet(vec_num, DIM, query_vec.data());
        std::vector<size_t> query_vec_lims;
        query_vec_lims.push_back(0);
        query_vec_lims.push_back(3);
        query_vec_lims.push_back(10);
        query_dataset->Set(knowhere::meta::EMB_LIST_OFFSET,
                           const_cast<const size_t*>(query_vec_lims.data()));

        auto search_conf = knowhere::Json{{knowhere::indexparam::NPROBE, 10}};
        milvus::SearchInfo searchInfo;
        searchInfo.topk_ = 5;
        searchInfo.metric_type_ = knowhere::metric::MAX_SIM_IP;
        searchInfo.search_params_ = search_conf;
        SearchResult result;
        milvus::OpContext op_context;
        vec_index->Query(
            query_dataset, searchInfo, nullptr, &op_context, result);
        auto ref_result = SearchResultToJson(result);
        std::cout << ref_result.dump(1) << std::endl;
        EXPECT_EQ(result.total_nq_, 2);
        EXPECT_EQ(result.distances_.size(), 2 * searchInfo.topk_);
        EXPECT_EQ(op_context.storage_usage.scanned_cold_bytes, 0);
        EXPECT_EQ(op_context.storage_usage.scanned_total_bytes, 0);
    }
}