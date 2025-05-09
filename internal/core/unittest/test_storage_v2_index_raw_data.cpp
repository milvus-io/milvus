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
#include <boost/filesystem.hpp>

#include "common/Consts.h"
#include "storage/Util.h"
#include "indexbuilder/IndexFactory.h"
#include "index/VectorDiskIndex.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"

#include <folly/Conv.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <cstdint>
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include <memory>
#include <string>
#include <vector>

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::storage;

class StorageV2IndexRawDataTest : public ::testing::Test {
    void
    SetUp() override {
        auto storage_config = gen_local_storage_config(path_);
        fs_ = storage::InitArrowFileSystem(storage_config);
        cm_ = storage::CreateChunkManager(storage_config);
    }

 protected:
    storage::ChunkManagerPtr cm_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::string path_ = "/tmp/test-inverted-index-storage-v2";
    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t index_build_id = 4001;
    int64_t index_version = 4001;
};

TEST_F(StorageV2IndexRawDataTest, TestGetRawData) {
    auto schema = gen_all_data_types_schema();
    auto vec = schema->get_field_id(FieldName("embeddings"));

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    // Write data to storage v2
    auto paths = std::vector<std::string>{path_ + "/19530.parquet",
                                          path_ + "/19531.parquet"};
    auto column_groups = std::vector<std::vector<int>>{
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}, {15}};
    auto writer_memory = 16 * 1024 * 1024;
    auto storage_config = milvus_storage::StorageConfig();
    auto arrow_schema = schema->ConvertToArrowSchema();
    milvus_storage::PackedRecordBatchWriter writer(
        fs_, paths, arrow_schema, storage_config, column_groups, writer_memory);
    int64_t total_rows = 0;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto record_batch =
            ConvertToArrowRecordBatch(dataset, dim, arrow_schema);
        total_rows += record_batch->num_rows();

        EXPECT_TRUE(writer.Write(record_batch).ok());
    }
    EXPECT_TRUE(writer.Close().ok());

    {
        // test memory file manager
        auto int64_field = schema->get_field_id(FieldName("int64"));

        milvus::Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{paths};
        config[STORAGE_VERSION_KEY] = STORAGE_V2;
        config[DATA_TYPE_KEY] = milvus::DataType::INT64;
        config[DIM_KEY] = 0;

        auto field_meta = gen_field_meta(collection_id,
                                         partition_id,
                                         segment_id,
                                         int64_field.get(),
                                         milvus::DataType::INT64,
                                         milvus::DataType::NONE,
                                         false);
        auto index_meta = gen_index_meta(
            segment_id, int64_field.get(), index_build_id, index_version);
        storage::FileManagerContext ctx(field_meta, index_meta, cm_);

        auto index = indexbuilder::IndexFactory::GetInstance().CreateIndex(
            milvus::DataType::INT64, config, ctx);
        index->Build();

        auto create_index_result = index->Upload();
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        auto index_files = create_index_result->GetIndexFiles();
    }

    {
        // test disk file manager
        auto float_field = schema->get_field_id(FieldName("float"));

        milvus::Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{paths};
        config[STORAGE_VERSION_KEY] = STORAGE_V2;
        config[DATA_TYPE_KEY] = milvus::DataType::FLOAT;
        config[DIM_KEY] = 0;

        IndexMeta index_meta = {segment_id,
                                float_field.get(),
                                index_build_id,
                                index_version,
                                "opt_fields",
                                "field_name",
                                milvus::DataType::VECTOR_FLOAT,
                                1};
        int64_t slice_size = milvus::FILE_SLICE_SIZE;
        FieldDataMeta field_data_meta = {
            collection_id, partition_id, segment_id, float_field.get()};
        auto file_manager =
            std::make_shared<milvus::storage::DiskFileManagerImpl>(
                storage::FileManagerContext(field_data_meta, index_meta, cm_));
        auto res = file_manager->CacheRawDataToDisk<float>(config);
        ASSERT_EQ(res, "/tmp/milvus/local_data/raw_datas/3/105/raw_data");
    }

    {
        // test scalar index sort
        auto int32_field = schema->get_field_id(FieldName("int32"));
        milvus::Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{paths};
        config[STORAGE_VERSION_KEY] = STORAGE_V2;
        config[DATA_TYPE_KEY] = milvus::DataType::INT32;
        config[DIM_KEY] = 0;

        IndexMeta index_meta = {segment_id,
                                int32_field.get(),
                                index_build_id,
                                index_version,
                                "opt_fields",
                                "int32",
                                milvus::DataType::INT32,
                                0};
        FieldDataMeta field_data_meta = {
            collection_id, partition_id, segment_id, int32_field.get()};
        auto ctx =
            storage::FileManagerContext(field_data_meta, index_meta, cm_);

        auto int32_index = milvus::index::CreateScalarIndexSort<int32_t>(ctx);
        int32_index->Build(config);
    }

    {
        // test string index marisa
        auto varchar_field = schema->get_field_id(FieldName("varchar"));

        milvus::Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{paths};
        config[STORAGE_VERSION_KEY] = STORAGE_V2;
        config[DATA_TYPE_KEY] = milvus::DataType::VARCHAR;
        config[DIM_KEY] = 0;

        IndexMeta index_meta = {segment_id,
                                varchar_field.get(),
                                index_build_id,
                                index_version,
                                "opt_fields",
                                "varchar",
                                milvus::DataType::VARCHAR,
                                0};
        FieldDataMeta field_data_meta = {
            collection_id, partition_id, segment_id, varchar_field.get()};
        auto ctx =
            storage::FileManagerContext(field_data_meta, index_meta, cm_);

        auto string_index =
            std::make_unique<milvus::index::StringIndexMarisa>(ctx);
        string_index->Build(config);
    }

    {
        // test vector disk ann index
        auto vec_field = schema->get_field_id(FieldName("embeddings"));
        auto index_type = knowhere::IndexEnum::INDEX_DISKANN;

        milvus::Config config;
        config["index_type"] = index_type;
        config[SEGMENT_INSERT_FILES_KEY] =
            std::vector<std::vector<std::string>>{paths};
        config[STORAGE_VERSION_KEY] = STORAGE_V2;
        config[DATA_TYPE_KEY] = milvus::DataType::VECTOR_FLOAT;
        config[DIM_KEY] = 0;
        config[milvus::index::DISK_ANN_BUILD_THREAD_NUM] = std::to_string(2);
        auto metric_type = knowhere::metric::L2;

        IndexMeta index_meta = {segment_id,
                                vec_field.get(),
                                index_build_id,
                                index_version,
                                "opt_fields",
                                "embeddings",
                                milvus::DataType::VECTOR_FLOAT,
                                0};
        FieldDataMeta field_data_meta = {
            collection_id, partition_id, segment_id, vec_field.get()};
        auto ctx =
            storage::FileManagerContext(field_data_meta, index_meta, cm_);

        try {
            auto vec_index =
                std::make_unique<milvus::index::VectorDiskAnnIndex<float>>(
                    index_type, metric_type, 6, ctx);
            vec_index->Build(config);
        } catch (const std::exception& e) {
            std::cout << "Exception: " << e.what() << std::endl;
        }
    }
}
