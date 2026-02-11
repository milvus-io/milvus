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

#include <boost/filesystem/operations.hpp>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <simdjson.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/JsonFlatIndex.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "simdjson/padded_string.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;

namespace milvus::test {
struct ChunkManagerWrapper {
    ChunkManagerWrapper(storage::ChunkManagerPtr cm) : cm_(cm) {
    }

    ~ChunkManagerWrapper() {
        for (const auto& file : written_) {
            cm_->Remove(file);
        }

        boost::filesystem::remove_all(cm_->GetRootPath());
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t len) {
        written_.insert(filepath);
        cm_->Write(filepath, buf, len);
    }

    const storage::ChunkManagerPtr cm_;
    std::unordered_set<std::string> written_;
};

class JsonFlatIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        int64_t field_id = 101;
        int64_t index_build_id = 4000;
        int64_t index_version = 4000;

        field_meta_ = milvus::segcore::gen_field_meta(
            collection_id, partition_id, segment_id, field_id, DataType::JSON);
        index_meta_ =
            gen_index_meta(segment_id, field_id, index_build_id, index_version);

        std::string root_path = "/tmp/test-json-flat-index/";
        auto storage_config = gen_local_storage_config(root_path);
        cm_ = storage::CreateChunkManager(storage_config);
        fs_ = storage::InitArrowFileSystem(storage_config);

        json_data_ = {
            R"({"profile": {"name": {"first": "Alice", "last": "Smith", "preferred_name": "Al"}, "team": {"name": "Engineering", "supervisor": {"name": "Bob"}}, "is_active": true, "employee_id": 1001, "skills": ["cpp", "rust", "python"], "scores": [95, 88, 92]}})",
            R"({"profile": {"name": {"first": "Bob", "last": "Johnson", "preferred_name": null}, "team": {"name": "Product", "supervisor": {"name": "Charlie"}}, "is_active": false, "employee_id": 1002, "skills": ["java", "python"], "scores": [85, 90]}})",
            R"({"profile": {"name": {"first": "Charlie", "last": "Williams"}, "team": {"name": "Design", "supervisor": {"name": "Alice"}}, "is_active": true, "employee_id": 1003, "skills": ["python", "javascript"], "scores": [87, 91, 89]}})"};

        // Create field data with JSON values
        auto field_data =
            storage::CreateFieldData(DataType::JSON, DataType::NONE);
        std::vector<Json> json_vec;
        for (const auto& json_str : json_data_) {
            json_vec.push_back(Json(simdjson::padded_string(json_str)));
        }
        field_data->FillFieldData(json_vec.data(), json_vec.size());

        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta_);
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

        log_path_ = get_binlog_path(0);

        cm_w_ = std::make_unique<test::ChunkManagerWrapper>(cm_);
        cm_w_->Write(
            log_path_, serialized_bytes.data(), serialized_bytes.size());

        ctx_ = std::make_unique<storage::FileManagerContext>(
            field_meta_, index_meta_, cm_, fs_);

        // Build index
        Config config;
        config["index_type"] = milvus::index::INVERTED_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path_};
        {
            auto index = std::make_shared<index::JsonFlatIndex>(*ctx_, "");
            index->Build(config);

            auto create_index_result = index->Upload();
            auto memSize = create_index_result->GetMemSize();
            auto serializedSize = create_index_result->GetSerializedSize();
            ASSERT_GT(memSize, 0);
            ASSERT_GT(serializedSize, 0);
            index_files_ = create_index_result->GetIndexFiles();
        }

        // Load index
        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::INVERTED_INDEX_TYPE;
        index_info.field_type = DataType::JSON;

        Config load_config;
        load_config["index_files"] = index_files_;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;

        ctx_->set_for_loading_index(true);
        json_index_ = std::make_shared<index::JsonFlatIndex>(*ctx_, "");
        json_index_->Load(milvus::tracer::TraceContext{}, load_config);

        auto cnt = json_index_->Count();
        ASSERT_EQ(cnt, json_data_.size());
    }

    void
    TearDown() override {
        cm_w_.reset();
        boost::filesystem::remove_all("/tmp/test-json-flat-index/");
    }

    storage::FieldDataMeta field_meta_;
    storage::IndexMeta index_meta_;
    storage::ChunkManagerPtr cm_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::unique_ptr<test::ChunkManagerWrapper> cm_w_;
    std::unique_ptr<storage::FileManagerContext> ctx_;
    std::string log_path_;
    std::vector<std::string> json_data_;
    std::vector<std::string> index_files_;
    std::shared_ptr<milvus::index::IndexBase> json_index_;
};

TEST_F(JsonFlatIndexTest, TestInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);

    std::vector<std::string> names = {"Alice", "Bob"};
    auto result = executor->In(names.size(), names.data());
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice
    ASSERT_TRUE(result[1]);   // Bob
    ASSERT_FALSE(result[2]);  // Charlie
}

TEST_F(JsonFlatIndexTest, TestExistsQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/preferred_name";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    auto result = executor->Exists();
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice
    ASSERT_FALSE(result[1]);  // null
    ASSERT_FALSE(result[2]);  // not exist
}

TEST_F(JsonFlatIndexTest, TestNotInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/team/name";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    std::vector<std::string> teams = {"Engineering", "Product"};
    auto result = executor->NotIn(teams.size(), teams.data());
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_FALSE(result[0]);  // Engineering
    ASSERT_FALSE(result[1]);  // Product
    ASSERT_TRUE(result[2]);   // Design
}

TEST_F(JsonFlatIndexTest, TestRangeQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);

    // Test LessThan
    auto result = executor->Range(std::string("Charlie"), OpType::LessThan);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice < Charlie
    ASSERT_TRUE(result[1]);   // Bob < Charlie
    ASSERT_FALSE(result[2]);  // Charlie = Charlie

    // Test Range between bounds
    auto range_result = executor->Range(std::string("Alice"),
                                        true,  // lower bound inclusive
                                        std::string("Bob"),
                                        true);  // upper bound inclusive
    ASSERT_EQ(range_result.size(), json_data_.size());
    ASSERT_TRUE(range_result[0]);   // Alice in [Alice, Bob]
    ASSERT_TRUE(range_result[1]);   // Bob in [Alice, Bob]
    ASSERT_FALSE(range_result[2]);  // Charlie not in [Alice, Bob]
}

TEST_F(JsonFlatIndexTest, TestPrefixMatchQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    auto result = executor->PrefixMatch("A");
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice starts with A
    ASSERT_FALSE(result[1]);  // Bob doesn't start with A
    ASSERT_FALSE(result[2]);  // Charlie doesn't start with A
}

TEST_F(JsonFlatIndexTest, TestPatternQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    auto result = executor->PatternQuery("[AB].*ice");
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice matches [AB].*ice
    ASSERT_FALSE(result[1]);  // Bob doesn't match [AB].*ice
    ASSERT_FALSE(result[2]);  // Charlie doesn't match [AB].*ice

    // Test another regex pattern
    auto result2 = executor->PatternQuery("B.b");
    ASSERT_EQ(result2.size(), json_data_.size());
    ASSERT_FALSE(result2[0]);  // Alice doesn't match B.b
    ASSERT_TRUE(result2[1]);   // Bob matches B.b
    ASSERT_FALSE(result2[2]);  // Charlie doesn't match B.b
}

TEST_F(JsonFlatIndexTest, TestPatternMatchQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    auto result = executor->PatternMatch("A%e", proto::plan::Match);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice matches A%e
    ASSERT_FALSE(result[1]);  // Bob doesn't match A%e
    ASSERT_FALSE(result[2]);  // Charlie doesn't match A%e
}

TEST_F(JsonFlatIndexTest, TestBooleanInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/is_active";
    auto executor = json_flat_index->create_executor<bool>(json_path);
    bool values[] = {true};
    auto result = executor->In(1, values);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice is active
    ASSERT_FALSE(result[1]);  // Bob is not active
    ASSERT_TRUE(result[2]);   // Charlie is active
}

TEST_F(JsonFlatIndexTest, TestBooleanNotInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/is_active";
    auto executor = json_flat_index->create_executor<bool>(json_path);
    bool values[] = {false};
    auto result = executor->NotIn(1, values);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice is not in [false]
    ASSERT_FALSE(result[1]);  // Bob is in [false]
    ASSERT_TRUE(result[2]);   // Charlie is not in [false]
}

TEST_F(JsonFlatIndexTest, TestInt64InQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/employee_id";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);
    int64_t values[] = {1001, 1002};
    auto result = executor->In(2, values);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice's id is 1001
    ASSERT_TRUE(result[1]);   // Bob's id is 1002
    ASSERT_FALSE(result[2]);  // Charlie's id is 1003
}

TEST_F(JsonFlatIndexTest, TestInt64NotInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/employee_id";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);
    int64_t values[] = {1003};
    auto result = executor->NotIn(1, values);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice's id is not 1003
    ASSERT_TRUE(result[1]);   // Bob's id is not 1003
    ASSERT_FALSE(result[2]);  // Charlie's id is 1003
}

TEST_F(JsonFlatIndexTest, TestInt64RangeQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/employee_id";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);

    // Test LessThan
    auto result = executor->Range(int64_t(1002), OpType::LessThan);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // 1001 < 1002
    ASSERT_FALSE(result[1]);  // 1002 = 1002
    ASSERT_FALSE(result[2]);  // 1003 > 1002

    // Test Range between bounds
    auto range_result = executor->Range(int64_t(1001),  // lower bound
                                        true,           // lower bound inclusive
                                        int64_t(1002),  // upper bound
                                        true);          // upper bound inclusive
    ASSERT_EQ(range_result.size(), json_data_.size());
    ASSERT_TRUE(range_result[0]);   // 1001 in [1001, 1002]
    ASSERT_TRUE(range_result[1]);   // 1002 in [1001, 1002]
    ASSERT_FALSE(range_result[2]);  // 1003 not in [1001, 1002]

    // Test GreaterEqual
    auto ge_result = executor->Range(int64_t(1002), OpType::GreaterEqual);
    ASSERT_EQ(ge_result.size(), json_data_.size());
    ASSERT_FALSE(ge_result[0]);  // 1001 < 1002
    ASSERT_TRUE(ge_result[1]);   // 1002 >= 1002
    ASSERT_TRUE(ge_result[2]);   // 1003 >= 1002
}

TEST_F(JsonFlatIndexTest, TestArrayStringInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/skills";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    std::string values[] = {"cpp", "python"};

    // Test for cpp
    auto result_cpp = executor->In(1, &values[0]);
    ASSERT_EQ(result_cpp.size(), json_data_.size());
    ASSERT_TRUE(result_cpp[0]);   // Alice has cpp
    ASSERT_FALSE(result_cpp[1]);  // Bob doesn't have cpp
    ASSERT_FALSE(result_cpp[2]);  // Charlie doesn't have cpp

    // Test for python
    auto result_python = executor->In(1, &values[1]);
    ASSERT_EQ(result_python.size(), json_data_.size());
    ASSERT_TRUE(result_python[0]);  // Alice has python
    ASSERT_TRUE(result_python[1]);  // Bob has python
    ASSERT_TRUE(result_python[2]);  // Charlie has python
}

TEST_F(JsonFlatIndexTest, TestArrayNumberInQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/scores";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);
    int64_t values[] = {95, 90};

    // Test for score 95
    auto result_95 = executor->In(1, &values[0]);
    ASSERT_EQ(result_95.size(), json_data_.size());
    ASSERT_TRUE(result_95[0]);   // Alice has score 95
    ASSERT_FALSE(result_95[1]);  // Bob doesn't have score 95
    ASSERT_FALSE(result_95[2]);  // Charlie doesn't have score 95

    // Test for score 90
    auto result_90 = executor->In(1, &values[1]);
    ASSERT_EQ(result_90.size(), json_data_.size());
    ASSERT_FALSE(result_90[0]);  // Alice doesn't have score 90
    ASSERT_TRUE(result_90[1]);   // Bob has score 90
    ASSERT_FALSE(result_90[2]);  // Charlie doesn't have score 90
}

TEST_F(JsonFlatIndexTest, TestArrayNumberRangeQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/scores";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);

    // Test scores greater than 90
    auto result = executor->Range(int64_t(90), OpType::GreaterThan);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice has scores > 90 (92, 95)
    ASSERT_FALSE(result[1]);  // Bob doesn't have scores > 90
    ASSERT_TRUE(result[2]);   // Charlie has score 91 > 90

    // Test scores in range [90, 92]
    auto range_result = executor->Range(int64_t(90),  // lower bound
                                        true,         // lower bound inclusive
                                        int64_t(92),  // upper bound
                                        true);        // upper bound inclusive
    ASSERT_EQ(range_result.size(), json_data_.size());
    ASSERT_TRUE(range_result[0]);  // Alice has score 92
    ASSERT_TRUE(range_result[1]);  // Bob has score 90
    ASSERT_TRUE(range_result[2]);  // Charlie has score 91
}

TEST_F(JsonFlatIndexTest, TestInApply) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);

    std::string values[] = {"Alice", "Bob"};
    auto result =
        executor->InApplyFilter(2, values, [](size_t offset) { return true; });
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice
    ASSERT_TRUE(result[1]);   // Bob
    ASSERT_FALSE(result[2]);  // Charlie
}

TEST_F(JsonFlatIndexTest, TestInApplyCallback) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    std::string values[] = {"Alice", "Bob"};
    executor->InApplyCallback(2, values, [](size_t offset) {
        ASSERT_TRUE(offset == 0 || offset == 1);
    });
}

TEST_F(JsonFlatIndexTest, TestQuery) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/employee_id";
    auto executor = json_flat_index->create_executor<int64_t>(json_path);

    auto dataset = std::make_unique<Dataset>();
    dataset->Set(milvus::index::OPERATOR_TYPE,
                 proto::plan::OpType::GreaterThan);
    dataset->Set<int64_t>(milvus::index::RANGE_VALUE, 1001);
    auto result = executor->Query(std::move(dataset));
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_FALSE(result[0]);  // Alice
    ASSERT_TRUE(result[1]);   // Bob
    ASSERT_TRUE(result[2]);   // Charlie
}

class JsonFlatIndexExprTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        json_data_ = {
            R"({"a": 1.0})",
            R"({"a": "abc"})",
            R"({"a": 3.0})",
            R"({"a": true})",
            R"({"a": {"b": 1}})",
            R"({"a": []})",
            R"({"a": ["a", "b"]})",
            R"({"a": null})",  // exists null
            R"(1)",
            R"("abc")",
            R"(1.0)",
            R"(true)",
            R"([1, 2, 3])",
            R"({"a": 1, "b": 2})",
            R"({})",
            R"(null)",
        };

        auto json_index_path = "";

        auto schema = std::make_shared<Schema>();
        auto vec_fid = schema->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
        json_fid_ = schema->AddDebugField("json", DataType::JSON, true);
        schema->set_primary_field_id(i64_fid);

        segment_ = segcore::CreateSealedSegment(schema);
        segcore::LoadIndexInfo load_index_info;

        auto file_manager_ctx = storage::FileManagerContext();
        file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
            milvus::proto::schema::JSON);
        file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(
            json_fid_.get());
        file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
        file_manager_ctx.fieldDataMeta.field_id = json_fid_.get();
        auto index = index::IndexFactory::GetInstance().CreateJsonIndex(
            index::CreateIndexInfo{
                .index_type = index::INVERTED_INDEX_TYPE,
                .json_cast_type = JsonCastType::FromString("JSON"),
                .json_path = json_index_path,
            },
            file_manager_ctx);

        json_index_ = std::unique_ptr<index::JsonFlatIndex>(
            static_cast<index::JsonFlatIndex*>(index.release()));

        auto json_field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
        std::vector<milvus::Json> jsons;
        for (auto& json_str : json_data_) {
            jsons.push_back(milvus::Json(simdjson::padded_string(json_str)));
        }
        json_field->add_json_data(jsons);
        auto json_valid_data = json_field->ValidData();
        json_valid_data[0] = 0xFF;
        json_valid_data[1] = 0xFF;

        json_index_->BuildWithFieldData({json_field});
        json_index_->finish();
        json_index_->create_reader(milvus::index::SetBitsetSealed);

        load_index_info.field_id = json_fid_.get();
        load_index_info.field_type = DataType::JSON;
        load_index_info.index_params = {{JSON_PATH, json_index_path},
                                        {JSON_CAST_TYPE, "JSON"}};
        load_index_info.cache_index =
            CreateTestCacheIndex("", std::move(json_index_));
        segment_->LoadIndex(load_index_info);
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(
            1, 1, 1, json_fid_.get(), {json_field}, cm);
        segment_->LoadFieldData(load_info);
    }

    void
    TearDown() override {
    }

    FieldId json_fid_;
    std::vector<std::string> json_data_;
    std::unique_ptr<index::JsonFlatIndex> json_index_;
    segcore::SegmentSealedUPtr segment_;
};

TEST_F(JsonFlatIndexExprTest, TestUnaryExpr) {
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {""}),
        proto::plan::OpType::GreaterEqual,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 3);
    EXPECT_TRUE(final[8]);
    EXPECT_TRUE(final[10]);
    EXPECT_TRUE(final[12]);
}

TEST_F(JsonFlatIndexExprTest, TestExistsExpr) {
    auto expr = std::make_shared<expr::ExistsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {""}));
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 12);
    EXPECT_FALSE(final[5]);
    EXPECT_FALSE(final[7]);
    EXPECT_FALSE(final[14]);
    EXPECT_FALSE(final[15]);
}
}  // namespace milvus::test
