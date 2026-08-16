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
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprCache.h"
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
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
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

        boost::system::error_code ec;
        boost::filesystem::remove_all(cm_->GetRootPath(), ec);
    }

    void
    Write(const std::string& filepath, void* buf, uint64_t len) {
        written_.insert(filepath);
        cm_->Write(filepath, buf, len);
    }

    const storage::ChunkManagerPtr cm_;
    std::unordered_set<std::string> written_;
};

std::unique_ptr<index::JsonFlatIndex>
BuildInMemoryJsonFlatIndex(const std::vector<std::string>& json_data) {
    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(101);
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = 101;

    index::CreateIndexInfo json_index_info;
    json_index_info.index_type = index::INVERTED_INDEX_TYPE;
    json_index_info.json_cast_type = JsonCastType::FromString("JSON");
    json_index_info.json_path = "";
    auto index = index::IndexFactory::GetInstance().CreateJsonIndex(
        json_index_info, file_manager_ctx);
    auto json_index = std::unique_ptr<index::JsonFlatIndex>(
        static_cast<index::JsonFlatIndex*>(index.release()));

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    jsons.reserve(json_data.size());
    for (const auto& json_str : json_data) {
        jsons.emplace_back(simdjson::padded_string(json_str));
    }
    json_field->add_json_data(jsons);

    auto* valid_data = json_field->ValidData();
    std::fill(valid_data,
              valid_data + json_field->ValidDataSize(),
              static_cast<uint8_t>(0xFF));

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);
    return json_index;
}

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

        std::string root_path = TestLocalPath;
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
            return fmt::format("{}{}/{}/{}/{}/{}",
                               TestLocalPath,
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

            auto create_index_result = index->UploadUnified({});
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
        json_index_->LoadUnified(load_config);

        auto cnt = json_index_->Count();
        ASSERT_EQ(cnt, json_data_.size());
    }

    void
    TearDown() override {
        cm_w_.reset();
        // Cleanup handled by random test path in init_gtest.cpp
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

TEST(JsonFlatIndexExactPathExistsTest, DistinguishesObjectSubpaths) {
    auto json_index = BuildInMemoryJsonFlatIndex({
        R"({"a": 1})",
        R"({"a": [1, 2]})",
        R"({"a": {"b": 1}})",
        R"({"a": []})",
        R"({"a": null})",
        R"({})",
    });

    std::string json_path = "/a";
    auto executor = json_index->create_executor<int64_t>(json_path);

    auto recursive_exists = executor->Exists();
    ASSERT_EQ(recursive_exists.size(), 6);
    EXPECT_TRUE(recursive_exists[0]);
    EXPECT_TRUE(recursive_exists[1]);
    EXPECT_TRUE(recursive_exists[2]);
    EXPECT_FALSE(recursive_exists[3]);
    EXPECT_FALSE(recursive_exists[4]);
    EXPECT_FALSE(recursive_exists[5]);

    auto exact_exists = executor->ExactPathExists();
    ASSERT_EQ(exact_exists.size(), 6);
    EXPECT_TRUE(exact_exists[0]);
    EXPECT_TRUE(exact_exists[1]);
    EXPECT_FALSE(exact_exists[2]);
    EXPECT_FALSE(exact_exists[3]);
    EXPECT_FALSE(exact_exists[4]);
    EXPECT_FALSE(exact_exists[5]);
}

TEST(JsonFlatIndexExactPathExistsTest, FiltersByComparableTypeFamily) {
    auto json_index = BuildInMemoryJsonFlatIndex({
        R"({"a": 1})",
        R"({"a": 1.5})",
        R"({"a": "one"})",
        R"({"a": true})",
        R"({"a": [2, 3]})",
        R"({"a": ["two"]})",
        R"({"a": [false]})",
        R"({"a": {"b": 1}})",
        R"({"a": null})",
        R"({})",
    });

    std::string json_path = "/a";
    auto executor = json_index->create_executor<int64_t>(json_path);

    auto any = executor->ExactPathExists(index::JsonValueType::Any);
    ASSERT_EQ(any.size(), 10);
    EXPECT_EQ(any.count(), 7);
    EXPECT_FALSE(any[7]);
    EXPECT_FALSE(any[8]);
    EXPECT_FALSE(any[9]);

    auto numeric = executor->ExactPathExists(index::JsonValueType::Numeric);
    ASSERT_EQ(numeric.size(), 10);
    EXPECT_TRUE(numeric[0]);
    EXPECT_TRUE(numeric[1]);
    EXPECT_FALSE(numeric[2]);
    EXPECT_FALSE(numeric[3]);
    EXPECT_TRUE(numeric[4]);
    EXPECT_FALSE(numeric[5]);
    EXPECT_FALSE(numeric[6]);
    EXPECT_FALSE(numeric[7]);
    EXPECT_FALSE(numeric[8]);
    EXPECT_FALSE(numeric[9]);

    auto string = executor->ExactPathExists(index::JsonValueType::String);
    ASSERT_EQ(string.size(), 10);
    EXPECT_FALSE(string[0]);
    EXPECT_FALSE(string[1]);
    EXPECT_TRUE(string[2]);
    EXPECT_FALSE(string[3]);
    EXPECT_FALSE(string[4]);
    EXPECT_TRUE(string[5]);
    EXPECT_FALSE(string[6]);
    EXPECT_FALSE(string[7]);
    EXPECT_FALSE(string[8]);
    EXPECT_FALSE(string[9]);

    auto boolean = executor->ExactPathExists(index::JsonValueType::Bool);
    ASSERT_EQ(boolean.size(), 10);
    EXPECT_FALSE(boolean[0]);
    EXPECT_FALSE(boolean[1]);
    EXPECT_FALSE(boolean[2]);
    EXPECT_TRUE(boolean[3]);
    EXPECT_FALSE(boolean[4]);
    EXPECT_FALSE(boolean[5]);
    EXPECT_TRUE(boolean[6]);
    EXPECT_FALSE(boolean[7]);
    EXPECT_FALSE(boolean[8]);
    EXPECT_FALSE(boolean[9]);
}

TEST_F(JsonFlatIndexTest, TestComparableAndFieldValidityMasks) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/preferred_name";
    auto comparable_executor =
        json_flat_index->create_executor<std::string>(json_path);
    auto comparable_mask = comparable_executor->IsNotNull();
    ASSERT_EQ(comparable_mask.size(), json_data_.size());
    ASSERT_TRUE(comparable_mask[0]);
    ASSERT_FALSE(comparable_mask[1]);
    ASSERT_FALSE(comparable_mask[2]);

    auto field_valid_executor =
        json_flat_index->create_executor<std::string>(json_path, false);
    auto field_valid_mask = field_valid_executor->IsNotNull();
    ASSERT_EQ(field_valid_mask.size(), json_data_.size());
    ASSERT_TRUE(field_valid_mask[0]);
    ASSERT_TRUE(field_valid_mask[1]);
    ASSERT_TRUE(field_valid_mask[2]);

    auto field_null_mask = field_valid_executor->IsNull();
    ASSERT_EQ(field_null_mask.size(), json_data_.size());
    ASSERT_FALSE(field_null_mask[0]);
    ASSERT_FALSE(field_null_mask[1]);
    ASSERT_FALSE(field_null_mask[2]);
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

TEST_F(JsonFlatIndexTest, TestLikePatternMatch) {
    auto json_flat_index =
        dynamic_cast<index::JsonFlatIndex*>(json_index_.get());
    ASSERT_NE(json_flat_index, nullptr);

    std::string json_path = "/profile/name/first";
    auto executor = json_flat_index->create_executor<std::string>(json_path);
    auto result = executor->PatternMatch("A%ice", proto::plan::Match);
    ASSERT_EQ(result.size(), json_data_.size());
    ASSERT_TRUE(result[0]);   // Alice matches A%ice
    ASSERT_FALSE(result[1]);  // Bob doesn't match A%ice
    ASSERT_FALSE(result[2]);  // Charlie doesn't match A%ice

    // Test another LIKE pattern
    auto result2 = executor->PatternMatch("B_b", proto::plan::Match);
    ASSERT_EQ(result2.size(), json_data_.size());
    ASSERT_FALSE(result2[0]);  // Alice doesn't match B_b
    ASSERT_TRUE(result2[1]);   // Bob matches B_b
    ASSERT_FALSE(result2[2]);  // Charlie doesn't match B_b
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

TEST(JsonFlatIndexUint64Test, TestNumericQueriesIncludeMixedIntegerTerms) {
    auto json_index = BuildInMemoryJsonFlatIndex({
        R"({"a": -10})",
        R"({"a": 1})",
        R"({"a": 10})",
        R"({"a": 10.5})",
        R"({"a": 9223372036854775808})",
        R"({"a": 18446744073709551615})",
        R"({"a": "1"})",
        R"({})",
    });

    std::string json_path = "/a";
    auto int_executor = json_index->create_executor<int64_t>(json_path);
    auto comparable_mask = int_executor->IsNotNull();
    ASSERT_EQ(comparable_mask.size(), 8);
    EXPECT_TRUE(comparable_mask[0]);
    EXPECT_TRUE(comparable_mask[1]);
    EXPECT_TRUE(comparable_mask[2]);
    EXPECT_TRUE(comparable_mask[3]);
    EXPECT_TRUE(comparable_mask[4]);
    EXPECT_TRUE(comparable_mask[5]);
    EXPECT_FALSE(comparable_mask[6]);
    EXPECT_FALSE(comparable_mask[7]);

    int64_t one = 1;
    auto not_one = int_executor->NotIn(1, &one);
    ASSERT_EQ(not_one.size(), 8);
    EXPECT_TRUE(not_one[0]);
    EXPECT_FALSE(not_one[1]);
    EXPECT_TRUE(not_one[2]);
    EXPECT_TRUE(not_one[3]);
    EXPECT_TRUE(not_one[4]);
    EXPECT_TRUE(not_one[5]);
    EXPECT_FALSE(not_one[6]);
    EXPECT_FALSE(not_one[7]);

    int64_t max_int64 = std::numeric_limits<int64_t>::max();
    auto not_max_int64 = int_executor->NotIn(1, &max_int64);
    ASSERT_EQ(not_max_int64.size(), 8);
    EXPECT_TRUE(not_max_int64[0]);
    EXPECT_TRUE(not_max_int64[1]);
    EXPECT_TRUE(not_max_int64[2]);
    EXPECT_TRUE(not_max_int64[3]);
    EXPECT_TRUE(not_max_int64[4]);
    EXPECT_TRUE(not_max_int64[5]);
    EXPECT_FALSE(not_max_int64[6]);
    EXPECT_FALSE(not_max_int64[7]);

    auto greater_than_nine =
        int_executor->Range(int64_t(9), OpType::GreaterThan);
    ASSERT_EQ(greater_than_nine.size(), 8);
    EXPECT_FALSE(greater_than_nine[0]);
    EXPECT_FALSE(greater_than_nine[1]);
    EXPECT_TRUE(greater_than_nine[2]);
    EXPECT_TRUE(greater_than_nine[3]);
    EXPECT_TRUE(greater_than_nine[4]);
    EXPECT_TRUE(greater_than_nine[5]);
    EXPECT_FALSE(greater_than_nine[6]);
    EXPECT_FALSE(greater_than_nine[7]);

    auto bounded = int_executor->Range(int64_t(0), true, int64_t(9), true);
    ASSERT_EQ(bounded.size(), 8);
    EXPECT_FALSE(bounded[0]);
    EXPECT_TRUE(bounded[1]);
    EXPECT_FALSE(bounded[2]);
    EXPECT_FALSE(bounded[3]);
    EXPECT_FALSE(bounded[4]);
    EXPECT_FALSE(bounded[5]);
    EXPECT_FALSE(bounded[6]);
    EXPECT_FALSE(bounded[7]);

    auto double_executor = json_index->create_executor<double>(json_path);
    auto double_range =
        double_executor->Range(double(9.5), OpType::GreaterThan);
    ASSERT_EQ(double_range.size(), 8);
    EXPECT_FALSE(double_range[0]);
    EXPECT_FALSE(double_range[1]);
    EXPECT_TRUE(double_range[2]);
    EXPECT_TRUE(double_range[3]);
    EXPECT_TRUE(double_range[4]);
    EXPECT_TRUE(double_range[5]);
    EXPECT_FALSE(double_range[6]);
    EXPECT_FALSE(double_range[7]);

    auto double_bounded =
        double_executor->Range(double(-10.5), false, double(10), true);
    ASSERT_EQ(double_bounded.size(), 8);
    EXPECT_TRUE(double_bounded[0]);
    EXPECT_TRUE(double_bounded[1]);
    EXPECT_TRUE(double_bounded[2]);
    EXPECT_FALSE(double_bounded[3]);
    EXPECT_FALSE(double_bounded[4]);
    EXPECT_FALSE(double_bounded[5]);
    EXPECT_FALSE(double_bounded[6]);
    EXPECT_FALSE(double_bounded[7]);

    auto u64_executor = json_index->create_executor<uint64_t>(json_path);
    auto u64_greater_than_nine =
        u64_executor->Range(uint64_t(9), OpType::GreaterThan);
    ASSERT_EQ(u64_greater_than_nine.size(), 8);
    EXPECT_FALSE(u64_greater_than_nine[0]);
    EXPECT_FALSE(u64_greater_than_nine[1]);
    EXPECT_TRUE(u64_greater_than_nine[2]);
    EXPECT_TRUE(u64_greater_than_nine[3]);
    EXPECT_TRUE(u64_greater_than_nine[4]);
    EXPECT_TRUE(u64_greater_than_nine[5]);
    EXPECT_FALSE(u64_greater_than_nine[6]);
    EXPECT_FALSE(u64_greater_than_nine[7]);

    auto u64_bounded =
        u64_executor->Range(uint64_t(0), true, uint64_t(11), true);
    ASSERT_EQ(u64_bounded.size(), 8);
    EXPECT_FALSE(u64_bounded[0]);
    EXPECT_TRUE(u64_bounded[1]);
    EXPECT_TRUE(u64_bounded[2]);
    EXPECT_TRUE(u64_bounded[3]);
    EXPECT_FALSE(u64_bounded[4]);
    EXPECT_FALSE(u64_bounded[5]);
    EXPECT_FALSE(u64_bounded[6]);
    EXPECT_FALSE(u64_bounded[7]);
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
        auto& cache = exec::ExprResCacheManager::Instance();
        cache.Clear();
        exec::ExprResCacheManager::SetEnabled(false);

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
            R"({"a": 9007199254740992})",
            R"({"a": 9007199254740993})",
            R"({"a": 9007199254740994})",
        };

        auto json_index_path = "";

        auto schema = std::make_shared<Schema>();
        schema->AddDebugField(
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
        index::CreateIndexInfo json_index_info;
        json_index_info.index_type = index::INVERTED_INDEX_TYPE;
        json_index_info.json_cast_type = JsonCastType::FromString("JSON");
        json_index_info.json_path = json_index_path;
        auto index = index::IndexFactory::GetInstance().CreateJsonIndex(
            json_index_info, file_manager_ctx);

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
        load_index_info.cache_index = CreateTestCacheIndex(
            "", std::move(json_index_), &observed_index_pin_ctx_);
        segment_->LoadIndex(load_index_info);
        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(
            1, 1, 1, json_fid_.get(), {json_field}, cm);
        segment_->LoadFieldData(load_info);
    }

    void
    TearDown() override {
        auto& cache = exec::ExprResCacheManager::Instance();
        cache.Clear();
        exec::ExprResCacheManager::SetEnabled(false);
    }

    FieldId json_fid_;
    std::vector<std::string> json_data_;
    std::unique_ptr<index::JsonFlatIndex> json_index_;
    segcore::SegmentSealedUPtr segment_;
    OpContext* observed_index_pin_ctx_{nullptr};
};

class JsonFlatIndexContainsExprTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto& cache = exec::ExprResCacheManager::Instance();
        cache.Clear();
        exec::ExprResCacheManager::SetEnabled(false);

        json_data_ = {
            R"({"a": [1, 2]})",
            R"({"a": [2]})",
            R"({"a": 1})",
            R"({"a": 2})",
            R"({"a": {"b": 1}})",
            R"({"a": []})",
            R"({"a": null})",
            R"({})",
            R"({"a": ["x"]})",
            R"({"a": [1]})",
            R"({"a": [9007199254740992]})",
            R"({"a": [9007199254740993]})",
            R"({"a": [9007199254740994]})",
            R"({"a": [9007199254740992.0]})",
        };

        auto schema = std::make_shared<Schema>();
        schema->AddDebugField(
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

        index::CreateIndexInfo json_index_info;
        json_index_info.index_type = index::INVERTED_INDEX_TYPE;
        json_index_info.json_cast_type = JsonCastType::FromString("JSON");
        json_index_info.json_path = "";
        auto index = index::IndexFactory::GetInstance().CreateJsonIndex(
            json_index_info, file_manager_ctx);
        auto json_index = std::unique_ptr<index::JsonFlatIndex>(
            static_cast<index::JsonFlatIndex*>(index.release()));

        json_field_ =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
        std::vector<milvus::Json> jsons;
        for (auto& json_str : json_data_) {
            jsons.emplace_back(simdjson::padded_string(json_str));
        }
        json_field_->add_json_data(jsons);
        auto* valid_data = json_field_->ValidData();
        std::fill(valid_data,
                  valid_data + json_field_->ValidDataSize(),
                  static_cast<uint8_t>(0));
        valid_data[0] = 0xFF;
        valid_data[1] = 0x3D;

        json_index->BuildWithFieldData({json_field_});
        json_index->finish();
        json_index->create_reader(milvus::index::SetBitsetSealed);

        load_index_info.field_id = json_fid_.get();
        load_index_info.field_type = DataType::JSON;
        load_index_info.index_params = {{JSON_PATH, ""},
                                        {JSON_CAST_TYPE, "JSON"}};
        load_index_info.cache_index =
            CreateTestCacheIndex("", std::move(json_index));
        segment_->LoadIndex(load_index_info);

        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto load_info = PrepareSingleFieldInsertBinlog(
            1, 1, 1, json_fid_.get(), {json_field_}, cm);
        segment_->LoadFieldData(load_info);
    }

    void
    TearDown() override {
        auto& cache = exec::ExprResCacheManager::Instance();
        cache.Clear();
        exec::ExprResCacheManager::SetEnabled(false);
    }

    ColumnVectorPtr
    Evaluate(proto::plan::JSONContainsExpr_JSONOp op,
             std::vector<int64_t> values,
             bool negate = false) {
        std::vector<proto::plan::GenericValue> generic_values;
        for (auto value : values) {
            generic_values.emplace_back();
            generic_values.back().set_int64_val(value);
        }
        auto contains_expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
            op,
            true,
            generic_values);
        expr::TypedExprPtr filter_expr = contains_expr;
        if (negate) {
            filter_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, contains_expr);
        }
        return EvalExprInBatches(filter_expr, segment_.get(), json_data_.size())
            .result;
    }

    void
    CheckResult(const ColumnVectorPtr& result,
                const std::vector<bool>& expected_result,
                const std::vector<bool>& expected_valid) {
        ASSERT_EQ(result->size(), expected_result.size());
        ASSERT_EQ(result->size(), expected_valid.size());
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
            if (expected_valid[i]) {
                EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
            }
        }
    }

    FieldId json_fid_;
    std::vector<std::string> json_data_;
    std::shared_ptr<FieldData<milvus::Json>> json_field_;
    segcore::SegmentSealedUPtr segment_;
};

TEST_F(JsonFlatIndexContainsExprTest, UsesExactPathThreeValuedValidity) {
    const std::vector<bool> expected_valid = {true,
                                              true,
                                              true,
                                              true,
                                              false,
                                              false,
                                              false,
                                              false,
                                              true,
                                              false,
                                              true,
                                              true,
                                              true,
                                              true};

    CheckResult(Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1}),
                {true,
                 false,
                 true,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false},
                expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAny, {1, 3}),
        {true,
         false,
         true,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false},
        expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAll, {1, 2}),
        {true,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false,
         false},
        expected_valid);

    CheckResult(
        Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1}, true),
        {false,
         true,
         false,
         true,
         false,
         false,
         false,
         false,
         true,
         false,
         true,
         true,
         true,
         true},
        expected_valid);
}

TEST_F(JsonFlatIndexContainsExprTest, PreservesLargeInt64LiteralPrecision) {
    ExprBatchSizeGuard batch_size_guard(5);
    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    auto contains_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{value});
    EXPECT_TRUE(CanExprExecuteAllAtOnce(
        contains_expr, segment_.get(), json_data_.size()));
    EXPECT_EQ(
        EvalExprBatchSizes(contains_expr, segment_.get(), json_data_.size()),
        (std::vector<int64_t>{5, 5, 4}));

    CheckResult(Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains,
                         {9007199254740993LL}),
                {false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 false,
                 true,
                 false,
                 false},
                {true,
                 true,
                 true,
                 true,
                 false,
                 false,
                 false,
                 false,
                 true,
                 false,
                 true,
                 true,
                 true,
                 true});
}

TEST_F(JsonFlatIndexContainsExprTest,
       ReusesExactPathValidityAcrossLiteralsAndOperators) {
    auto& cache = exec::ExprResCacheManager::Instance();
    exec::CacheConfig config;
    config.mode = exec::CacheMode::Memory;
    config.mem_max_bytes = 1 << 20;
    config.compression_enabled = true;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(cache.SetConfig(config));
    exec::ExprResCacheManager::SetEnabled(true);

    Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {1});
    EXPECT_EQ(cache.GetEntryCount(), 2);

    Evaluate(proto::plan::JSONContainsExpr_JSONOp_Contains, {2});
    EXPECT_EQ(cache.GetEntryCount(), 3);

    Evaluate(proto::plan::JSONContainsExpr_JSONOp_ContainsAny, {1, 3});
    EXPECT_EQ(cache.GetEntryCount(), 4);
}

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

TEST_F(JsonFlatIndexExprTest, TestComparisonUnknowns) {
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto not_equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       not_equal_expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 4);
    EXPECT_TRUE(final[2]);
    EXPECT_TRUE(final[16]);
    EXPECT_TRUE(final[17]);
    EXPECT_TRUE(final[18]);

    auto greater_than_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    auto not_greater_than_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, greater_than_expr);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  not_greater_than_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 2);
    EXPECT_TRUE(final[0]);
    EXPECT_TRUE(final[13]);
}

TEST_F(JsonFlatIndexExprTest, JSONArrayEqualityFallsBackToRawData) {
    proto::plan::GenericValue value;
    auto* array = value.mutable_array_val();
    array->set_same_type(true);
    array->add_array()->set_string_val("a");
    array->add_array()->set_string_val("b");

    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);

    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);
    EXPECT_EQ(observed_index_pin_ctx_, nullptr);
}

TEST_F(JsonFlatIndexExprTest,
       JSONContainsMixedAndArrayLiteralsFallBackToRawData) {
    ExprBatchSizeGuard batch_size_guard(7);
    const auto expect_three_batches = [&](const expr::TypedExprPtr& expr) {
        std::vector<int64_t> batch_sizes;
        EXPECT_NO_THROW(batch_sizes = EvalExprBatchSizes(
                            expr, segment_.get(), json_data_.size()));
        EXPECT_EQ(batch_sizes, (std::vector<int64_t>{7, 7, 5}));
    };

    proto::plan::GenericValue string_value;
    string_value.set_string_val("a");
    proto::plan::GenericValue int_value;
    int_value.set_int64_val(1);

    auto mixed_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        false,
        std::vector<proto::plan::GenericValue>{string_value, int_value});
    expect_three_batches(mixed_expr);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, mixed_expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);

    proto::plan::GenericValue array_value;
    auto* array = array_value.mutable_array_val();
    array->set_same_type(true);
    array->add_array()->set_string_val("a");
    array->add_array()->set_string_val("b");
    auto array_expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::JSONContainsExpr_JSONOp_Contains,
        true,
        std::vector<proto::plan::GenericValue>{array_value});
    expect_three_batches(array_expr);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, array_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    auto in_field_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{string_value},
        true);
    expect_three_batches(in_field_expr);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  in_field_expr);
    final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 1);
    EXPECT_TRUE(final[6]);
}

TEST_F(JsonFlatIndexExprTest, ReusesValidityAcrossLiteralsAndOperators) {
    auto& cache = exec::ExprResCacheManager::Instance();
    exec::CacheConfig config;
    config.mode = exec::CacheMode::Memory;
    config.mem_max_bytes = 1 << 20;
    config.compression_enabled = true;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(cache.SetConfig(config));
    exec::ExprResCacheManager::SetEnabled(true);

    auto evaluate = [&](std::vector<std::string> nested_path,
                        proto::plan::OpType op,
                        proto::plan::GenericValue value) {
        auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid_, DataType::JSON, std::move(nested_path)),
            op,
            value,
            std::vector<proto::plan::GenericValue>());
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           unary_expr);
        return query::ExecuteQueryExpr(
            plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    };
    auto int_value = [](int64_t literal) {
        proto::plan::GenericValue value;
        value.set_int64_val(literal);
        return value;
    };

    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, int_value(1)).count(),
              2);
    EXPECT_EQ(cache.GetEntryCount(), 2);

    exec::ExprResCacheManager::Key artifact_key{
        segment_->get_segment_id(),
        fmt::format("json-flat-validity:v1:field={}:path-length=2:"
                    "path=/a:family={}",
                    json_fid_.get(),
                    static_cast<unsigned int>(index::JsonValueType::Numeric))};
    exec::ExprResCacheManager::Value artifact;
    artifact.active_count = json_data_.size();
    ASSERT_TRUE(cache.Get(artifact_key, artifact));
    EXPECT_EQ(artifact.result->count(), 6);
    EXPECT_EQ(artifact.valid_result->count(), json_data_.size());
    artifact.active_count = json_data_.size() + 1;
    EXPECT_FALSE(cache.Get(artifact_key, artifact));

    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, int_value(3)).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 3);

    EXPECT_EQ(
        evaluate({"a"}, proto::plan::OpType::GreaterThan, int_value(1)).count(),
        4);
    EXPECT_EQ(cache.GetEntryCount(), 4);

    proto::plan::GenericValue string_value;
    string_value.set_string_val("abc");
    EXPECT_EQ(evaluate({"a"}, proto::plan::OpType::Equal, string_value).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 6);

    EXPECT_EQ(evaluate({"b"}, proto::plan::OpType::Equal, int_value(2)).count(),
              1);
    EXPECT_EQ(cache.GetEntryCount(), 8);

    EXPECT_EQ(cache.EraseSegment(segment_->get_segment_id()), 8);
    EXPECT_EQ(cache.GetEntryCount(), 0);
}

TEST_F(JsonFlatIndexExprTest, PreservesLargeInt64LiteralPrecision) {
    ExprBatchSizeGuard batch_size_guard(7);
    const auto evaluate = [&](const expr::TypedExprPtr& expr,
                              bool can_execute_all_at_once) {
        EXPECT_EQ(
            CanExprExecuteAllAtOnce(expr, segment_.get(), json_data_.size()),
            can_execute_all_at_once);
        ExprBatchEvalResult evaluation;
        EXPECT_NO_THROW(evaluation = EvalExprInBatches(
                            expr, segment_.get(), json_data_.size()));
        EXPECT_EQ(evaluation.batch_sizes, (std::vector<int64_t>{7, 7, 5}));
        return evaluation.result;
    };

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);

    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    auto result = evaluate(equal_expr, true);
    TargetBitmapView result_view(result->GetRawData(), result->size());
    TargetBitmapView valid_view(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{value},
        false);
    result = evaluate(term_expr, true);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    result = evaluate(greater_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_FALSE(result_view[17]);
    EXPECT_TRUE(result_view[18]);

    auto between_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        value,
        value,
        true,
        true);
    result = evaluate(between_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(valid_view[16]);
    EXPECT_TRUE(valid_view[17]);
    EXPECT_TRUE(valid_view[18]);
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);

    proto::plan::GenericValue upper_float;
    upper_float.set_float_val(9007199254740994.0);
    auto mixed_lower_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        value,
        upper_float,
        true,
        true);
    result = evaluate(mixed_lower_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_FALSE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_TRUE(result_view[18]);

    proto::plan::GenericValue lower_float;
    lower_float.set_float_val(9007199254740992.0);
    auto mixed_upper_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        lower_float,
        value,
        true,
        true);
    result = evaluate(mixed_upper_expr, false);
    result_view = TargetBitmapView(result->GetRawData(), result->size());
    valid_view = TargetBitmapView(result->GetValidRawData(), result->size());
    EXPECT_TRUE(result_view[16]);
    EXPECT_TRUE(result_view[17]);
    EXPECT_FALSE(result_view[18]);
}

TEST_F(JsonFlatIndexExprTest, EmptyJsonInIsDeterministicForEveryRow) {
    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{},
        false);
    auto check = [&](const expr::TypedExprPtr& filter_expr,
                     bool expected_result) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        auto result = gen_filter_res(
            plan.get(), segment_.get(), json_data_.size(), MAX_TIMESTAMP);
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_TRUE(valid_view[i]) << "row " << i;
            EXPECT_EQ(result_view[i], expected_result) << "row " << i;
        }
    };

    check(term_expr, false);
    check(std::make_shared<expr::LogicalUnaryExpr>(
              expr::LogicalUnaryExpr::OpType::LogicalNot, term_expr),
          true);
}

TEST_F(JsonFlatIndexExprTest, TestExistsExpr) {
    auto expr = std::make_shared<expr::ExistsExpr>(
        expr::ColumnInfo(json_fid_, DataType::JSON, {""}));
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto final = query::ExecuteQueryExpr(
        plan, segment_.get(), json_data_.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 15);
    EXPECT_FALSE(final[5]);
    EXPECT_FALSE(final[7]);
    EXPECT_FALSE(final[14]);
    EXPECT_FALSE(final[15]);
}
}  // namespace milvus::test
