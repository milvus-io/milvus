// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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
#include <boost/filesystem.hpp>
#include <unordered_set>
#include <memory>
#include <random>

#include "common/Tracer.h"
#include "index/BitmapIndex.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "index/json_stats/JsonKeyStats.h"
#include "common/Json.h"
#include "common/Types.h"

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

int64_t
GenerateRandomInt64(int64_t min, int64_t max) {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());

    std::uniform_int_distribution<int64_t> dist(min, max);
    return dist(gen);
}

static std::vector<milvus::Json>
GenerateJsons(int size) {
    std::vector<Json> jsons;
    std::default_random_engine random(42);
    std::normal_distribution<> distr(0, 1);
    for (int i = 0; i < size; i++) {
        std::string str;
        if (i % 10 < 2) {
            str = R"({"int_shared":)" + std::to_string(random()) +
                  R"(,"double_shared":)" +
                  std::to_string(static_cast<double>(random())) +
                  R"(,"string_shared":")" + std::to_string(random()) +
                  R"(","bool_shared": true)" + R"(, "array_shared": [1,2,3])" +
                  "}";
        } else {
            str = R"({"int":)" + std::to_string(random()) + R"(,"double":)" +
                  std::to_string(static_cast<double>(random())) +
                  R"(,"string":")" + std::to_string(random()) +
                  R"(","bool": true)" + R"(, "array": [1,2,3])" + "}";
        }
        jsons.push_back(milvus::Json(simdjson::padded_string(str)));
    }
    return jsons;
}

class JsonKeyStatsTest : public ::testing::TestWithParam<bool> {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version,
         int64_t size) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);
        field_schema.set_nullable(nullable_);
        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        data_ = std::move(GenerateJsons(size));
        auto field_data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE, nullable_);
        if (nullable_) {
            valid_data.reserve(size_);
            for (size_t i = 0; i < size_; i++) {
                if (i % 2 == 0) {
                    valid_data.push_back(true);
                } else {
                    valid_data.push_back(false);
                }
            }

            int byteSize = (size_ + 7) / 8;
            uint8_t* valid_data_ = new uint8_t[byteSize];
            for (int i = 0; i < size_; i++) {
                bool value = valid_data[i];
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                if (value) {
                    valid_data_[byteIndex] |= (1 << bitIndex);
                } else {
                    valid_data_[byteIndex] &= ~(1 << bitIndex);
                }
            }
            field_data->FillFieldData(
                data_.data(), valid_data_, data_.size(), 0);
            delete[] valid_data_;
        } else {
            field_data->FillFieldData(data_.data(), data_.size());
        }

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    "/tmp/test-jsonkey-stats/",
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager_);
        std::vector<std::string> index_files;

        Config config;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        auto build_index = std::make_shared<JsonKeyStats>(ctx, false);
        build_index->Build(config);

        auto create_index_result = build_index->Upload(config);
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files = create_index_result->GetIndexFiles();

        index::CreateIndexInfo index_info{};
        config["index_files"] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        config[milvus::index::ENABLE_MMAP] = true;
        config[milvus::index::MMAP_FILE_PATH] =
            "/tmp/test-jsonkey-stats/mmap-file";
        index_ = std::make_shared<JsonKeyStats>(ctx, true);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    void
    SetUp() override {
        nullable_ = GetParam();
        type_ = DataType::JSON;
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        int64_t field_id = 101;
        int64_t index_build_id = GenerateRandomInt64(1, 100000);
        int64_t index_version = 1;
        size_ = 1000;  // Use a larger size for better testing
        std::string root_path = "/tmp/test-jsonkey-stats/";

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = "/tmp/test-jsonkey-stats/arrow-fs";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

        Init(collection_id,
             partition_id,
             segment_id,
             field_id,
             index_build_id,
             index_version,
             size_);
    }

    virtual ~JsonKeyStatsTest() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    std::shared_ptr<JsonKeyStats> index_;
    DataType type_;
    bool nullable_;
    size_t size_;
    FixedVector<bool> valid_data;
    std::vector<milvus::Json> data_;
    std::vector<std::string> json_col;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
};

INSTANTIATE_TEST_SUITE_P(JsonKeyStatsTestSuite,
                         JsonKeyStatsTest,
                         ::testing::Values(true, false));

TEST_P(JsonKeyStatsTest, TestBasicOperations) {
    // Test Count
    EXPECT_EQ(index_->Count(), size_);

    // Test Size
    EXPECT_EQ(index_->Size(), size_);

    // Test HasRawData
    EXPECT_FALSE(index_->HasRawData());
}

TEST_P(JsonKeyStatsTest, TestExecuteForSharedData) {
    std::string path = "/int_shared";
    int count = 0;
    index_->ExecuteForSharedData(
        nullptr, path, [&](BsonView bson, uint32_t row_id, uint32_t offset) {
            count++;
        });
    std::cout << "count: " << count << std::endl;
    if (nullable_) {
        EXPECT_EQ(count, 100);
    } else {
        EXPECT_EQ(count, 200);
    }
}

TEST_P(JsonKeyStatsTest, TestExecuteExistsPathForSharedData) {
    std::string path = "/int_shared";
    TargetBitmap bitset(size_);
    TargetBitmapView bitset_view(bitset);
    index_->ExecuteExistsPathForSharedData(path, bitset_view);
    std::cout << "bitset.count(): " << bitset.count() << std::endl;
    auto count = bitset.count();
    if (nullable_) {
        EXPECT_EQ(count, 100);
    } else {
        EXPECT_EQ(count, 200);
    }
}

TEST_P(JsonKeyStatsTest, TestExecutorForGettingValid) {
    std::string path = "/int";
    TargetBitmap valid_res(size_, true);
    TargetBitmapView valid_res_view(valid_res);
    auto shredding_fields = index_->GetShreddingFields(path);
    for (const auto& field : shredding_fields) {
        auto processed_size =
            index_->ExecutorForGettingValid(nullptr, field, valid_res_view);
        EXPECT_EQ(processed_size, size_);
    }
    if (!index_->CanSkipShared(path)) {
        std::cout << "can not skip shared" << std::endl;
        index_->ExecuteExistsPathForSharedData(path, valid_res_view);
    }
    std::cout << "valid_res.count(): " << valid_res.count() << std::endl;
    if (nullable_) {
        EXPECT_EQ(valid_res.count(), 400);
    } else {
        EXPECT_EQ(valid_res.count(), 800);
    }
}

TEST_P(JsonKeyStatsTest, TestExecutorForShreddingData) {
    std::string path = "/int";
    TargetBitmap res(size_);
    TargetBitmap valid_res(size_, true);
    TargetBitmapView res_view(res);
    TargetBitmapView valid_res_view(valid_res);

    auto func = [](const int64_t* data,
                   const bool* valid_data,
                   const int size,
                   TargetBitmapView res,
                   TargetBitmapView valid_res) {
        for (int i = 0; i < size; i++) {
            if (valid_data[i]) {
                res[i] = true;
                valid_res[i] = true;
            }
        }
    };

    auto field_name = *(index_->GetShreddingFields(path).begin());
    std::cout << "field_name: " << field_name << std::endl;
    int processed_size = index_->ExecutorForShreddingData<int64_t>(
        nullptr, field_name, func, nullptr, res_view, valid_res_view);
    std::cout << "processed_size: " << processed_size << std::endl;
    EXPECT_EQ(processed_size, size_);

    if (nullable_) {
        EXPECT_EQ(res.count(), 400);
    } else {
        EXPECT_EQ(res.count(), 800);
    }
}

TEST_P(JsonKeyStatsTest, TestGetShreddingFields) {
    std::string pointer = "/int";
    auto fields = index_->GetShreddingFields(pointer);
    EXPECT_FALSE(fields.empty());

    std::vector<JSONType> types = {JSONType::INT64};
    auto typed_fields = index_->GetShreddingFields(pointer, types);
    EXPECT_FALSE(typed_fields.empty());
}

TEST_P(JsonKeyStatsTest, TestCanSkipShared) {
    std::string path = "/int";
    std::set<JSONType> target_types = {JSONType::INT64};
    EXPECT_TRUE(index_->CanSkipShared(path, target_types));

    target_types = {JSONType::STRING};
    EXPECT_TRUE(index_->CanSkipShared(path, target_types));
}

class JsonKeyStatsUploadLoadTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        collection_id_ = 1;
        partition_id_ = 2;
        segment_id_ = 3;
        field_id_ = 101;
        index_build_id_ = GenerateRandomInt64(1, 100000);
        index_version_ = 10000;
        root_path_ = "/tmp/test-jsonkey-stats-upload-load/";

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path_;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = "/tmp/test-jsonkey-stats-upload-load/arrow-fs";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
    }

    void
    TearDown() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

    void
    InitContext() {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::JSON);
        field_schema.set_nullable(false);
        field_meta_ = storage::FieldDataMeta{collection_id_,
                                             partition_id_,
                                             segment_id_,
                                             field_id_,
                                             field_schema};
        index_meta_ = storage::IndexMeta{
            segment_id_, field_id_, index_build_id_, index_version_};
    }

    void
    PrepareData(const std::vector<std::string>& json_strings) {
        data_.clear();
        for (const auto& str : json_strings) {
            data_.push_back(milvus::Json(simdjson::padded_string(str)));
        }
    }

    void
    BuildAndUpload() {
        auto field_data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE, false);
        field_data->FillFieldData(data_.data(), data_.size());

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta_);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    root_path_,
                                    collection_id_,
                                    partition_id_,
                                    segment_id_,
                                    field_id_,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(
            field_meta_, index_meta_, chunk_manager_);

        Config config;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

        build_index_ = std::make_shared<JsonKeyStats>(ctx, false);
        build_index_->Build(config);

        auto create_index_result = build_index_->Upload(config);
        auto memSize = create_index_result->GetMemSize();
        auto serializedSize = create_index_result->GetSerializedSize();
        ASSERT_GT(memSize, 0);
        ASSERT_GT(serializedSize, 0);
        index_files_ = create_index_result->GetIndexFiles();
    }

    void
    Load() {
        storage::FileManagerContext ctx(
            field_meta_, index_meta_, chunk_manager_);
        Config config;
        config["index_files"] = index_files_;
        config[milvus::index::ENABLE_MMAP] = true;
        config[milvus::index::MMAP_FILE_PATH] =
            "/tmp/test-jsonkey-stats-upload-load/mmap-file";
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        load_index_ = std::make_shared<JsonKeyStats>(ctx, true);
        load_index_->Load(milvus::tracer::TraceContext{}, config);
    }

    void
    VerifyBasicOperations() {
        EXPECT_EQ(load_index_->Count(), data_.size());
        EXPECT_EQ(load_index_->Size(), data_.size());
        EXPECT_FALSE(load_index_->HasRawData());
    }

    void
    VerifyPathInShared(const std::string& path) {
        TargetBitmap bitset(data_.size());
        TargetBitmapView bitset_view(bitset);
        load_index_->ExecuteExistsPathForSharedData(path, bitset_view);
        EXPECT_GT(bitset.size(), 0);
    }

    void
    VerifyPathInShredding(const std::string& path) {
        auto fields = load_index_->GetShreddingFields(path);
        EXPECT_GT(fields.size(), 0);
    }

    void
    VerifyJsonType(const std::string& path, JSONType expected_type) {
        auto type = load_index_->GetShreddingJsonType(path);
        EXPECT_EQ(int(type), int(expected_type));
    }

 public:
    int64_t collection_id_;
    int64_t partition_id_;
    int64_t segment_id_;
    int64_t field_id_;
    int64_t index_build_id_;
    int64_t index_version_;
    std::string root_path_;
    storage::FieldDataMeta field_meta_;
    storage::IndexMeta index_meta_;
    std::vector<milvus::Json> data_;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
    std::shared_ptr<JsonKeyStats> build_index_;
    std::shared_ptr<JsonKeyStats> load_index_;
    std::vector<std::string> index_files_;
};

TEST_F(JsonKeyStatsUploadLoadTest, TestSimpleJson) {
    std::vector<std::string> json_strings = {
        R"({"int": 1, "double": 1.5, "string": "test", "bool": true})",
        R"({"int": 2, "double": 2.5, "string": "test2", "bool": false})",
        R"({"int": 3, "double": 3.5, "string": "test3", "bool": true})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/int");
    VerifyPathInShredding("/double");
    VerifyPathInShredding("/string");
    VerifyPathInShredding("/bool");
    VerifyJsonType("/int_INT64", JSONType::INT64);
    VerifyJsonType("/double_DOUBLE", JSONType::DOUBLE);
    VerifyJsonType("/string_STRING", JSONType::STRING);
    VerifyJsonType("/bool_BOOL", JSONType::BOOL);
}

TEST_F(JsonKeyStatsUploadLoadTest, TestNestedJson) {
    std::vector<std::string> json_strings = {
        R"({"nested": {"int": 1, "double": 1.5}, "array": [1, 2, 3]})",
        R"({"nested": {"int": 2, "double": 2.5}, "array": [4, 5, 6]})",
        R"({"nested": {"int": 3, "double": 3.5}, "array": [7, 8, 9]})"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/nested/int");
    VerifyPathInShredding("/nested/double");
    VerifyPathInShared("/array");
    VerifyJsonType("/nested/int_INT64", JSONType::INT64);
    VerifyJsonType("/nested/double_DOUBLE", JSONType::DOUBLE);
}

TEST_F(JsonKeyStatsUploadLoadTest, TestComplexJson) {
    std::vector<std::string> json_strings = {
        R"({
            "user": {
                "id": 1,
                "name": "John",
                "scores": [85, 90, 95],
                "address": {
                    "city": "New York",
                    "zip": 10001
                }
            },
            "timestamp": 1234567890
        })",
        R"({
            "user": {
                "id": 2,
                "name": "Jane",
                "scores": [88, 92, 98],
                "address": {
                    "city": "Los Angeles",
                    "zip": 90001
                }
            },
            "timestamp": 1234567891
        })"};

    InitContext();
    PrepareData(json_strings);
    BuildAndUpload();
    Load();

    VerifyBasicOperations();
    VerifyPathInShredding("/user/id");
    VerifyPathInShredding("/user/name");
    VerifyPathInShared("/user/scores");
    VerifyPathInShredding("/user/address/city");
    VerifyPathInShredding("/user/address/zip");
    VerifyPathInShredding("/timestamp");
    VerifyJsonType("/user/id_INT64", JSONType::INT64);
    VerifyJsonType("/user/name_STRING", JSONType::STRING);
    VerifyJsonType("/user/address/city_STRING", JSONType::STRING);
    VerifyJsonType("/user/address/zip_INT64", JSONType::INT64);
    VerifyJsonType("/timestamp_INT64", JSONType::INT64);
}