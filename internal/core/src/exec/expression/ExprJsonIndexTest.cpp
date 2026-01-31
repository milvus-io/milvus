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

#include "ExprTestBase.h"

template <typename T>
class JsonIndexTestFixture : public testing::Test {
 public:
    using DataType = T;

    JsonIndexTestFixture() {
        if constexpr (std::is_same_v<T, bool>) {
            schema_data_type = proto::schema::Bool;
            json_path = "/bool";
            lower_bound.set_bool_val(std::numeric_limits<bool>::min());
            upper_bound.set_bool_val(std::numeric_limits<bool>::max());
            cast_type = JsonCastType::FromString("BOOL");
            wrong_type_val.set_int64_val(123);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            schema_data_type = proto::schema::Int64;
            json_path = "/int";
            lower_bound.set_int64_val(std::numeric_limits<int64_t>::min());
            upper_bound.set_int64_val(std::numeric_limits<int64_t>::max());
            cast_type = JsonCastType::FromString("DOUBLE");
            wrong_type_val.set_string_val("123");
        } else if constexpr (std::is_same_v<T, double>) {
            schema_data_type = proto::schema::Double;
            json_path = "/double";
            lower_bound.set_float_val(std::numeric_limits<double>::min());
            upper_bound.set_float_val(std::numeric_limits<double>::max());
            cast_type = JsonCastType::FromString("DOUBLE");
            wrong_type_val.set_string_val("123");
        } else if constexpr (std::is_same_v<T, std::string>) {
            schema_data_type = proto::schema::String;
            json_path = "/string";
            lower_bound.set_string_val("");
            std::string s(1024, '9');
            upper_bound.set_string_val(s);
            cast_type = JsonCastType::FromString("VARCHAR");
            wrong_type_val.set_int64_val(123);
        }
    }
    proto::schema::DataType schema_data_type;
    std::string json_path;
    proto::plan::GenericValue lower_bound;
    proto::plan::GenericValue upper_bound;
    JsonCastType cast_type = JsonCastType::UNKNOWN;

    proto::plan::GenericValue wrong_type_val;
};

using JsonIndexTypes = ::testing::Types<bool, int64_t, double, std::string>;
TYPED_TEST_SUITE(JsonIndexTestFixture, JsonIndexTypes);

TYPED_TEST(JsonIndexTestFixture, TestJsonIndexUnaryExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i32_fid = schema->AddDebugField("age32", DataType::INT32);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    int N = 1000;
    auto raw_data = DataGen(schema, N);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = this->cast_type,
            .json_path = this->json_path,
        },
        file_manager_ctx);

    using json_index_type =
        index::JsonInvertedIndex<typename TestFixture::DataType>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_col = raw_data.get_col<std::string>(json_fid);
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_col) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    // load_index_info.index = std::move(json_index);
    load_index_info.index_params = {
        {JSON_PATH, this->json_path},
        {JSON_CAST_TYPE, this->cast_type.ToString()}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test_cache_index", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::LessEqual,
        this->upper_bound,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N);

    // test for wrong filter type
    unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::LessEqual,
        this->wrong_type_val,
        std::vector<proto::plan::GenericValue>());
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        proto::plan::OpType::GreaterEqual,
        this->lower_bound,
        std::vector<proto::plan::GenericValue>());
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        std::vector<proto::plan::GenericValue>(),
        false);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, term_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 0);

    using DT = std::conditional_t<
        std::is_same_v<typename TestFixture::DataType, std::string>,
        std::string_view,
        typename TestFixture::DataType>;
    std::vector<proto::plan::GenericValue> vals;
    int expect_count = 10;
    if constexpr (std::is_same_v<DT, bool>) {
        proto::plan::GenericValue val;
        val.set_bool_val(true);
        vals.push_back(val);
        val.set_bool_val(false);
        vals.push_back(val);
        expect_count = N;
    } else {
        for (int i = 0; i < expect_count; ++i) {
            proto::plan::GenericValue val;

            auto v = jsons[i].at<DT>(this->json_path).value();
            if constexpr (std::is_same_v<DT, int64_t>) {
                val.set_int64_val(v);
            } else if constexpr (std::is_same_v<DT, double>) {
                val.set_float_val(v);
            } else if constexpr (std::is_same_v<DT, std::string_view>) {
                val.set_string_val(std::string(v));
            } else if constexpr (std::is_same_v<DT, bool>) {
                val.set_bool_val(i % 2 == 0);
            }
            vals.push_back(val);
        }
    }
    term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {this->json_path.substr(1)}),
        vals,
        false);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, term_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);

    EXPECT_EQ(final.count(), expect_count);
    // not expr
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, term_expr);
    plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, not_expr);
    final = ExecuteQueryExpr(plan, seg.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), N - expect_count);
}

TEST(JsonIndexTest, TestJsonNotEqualExpr) {
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("DOUBLE"),
            .json_path = "/a",
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_strs = std::vector<std::string>{
        R"({"a": 1.0})", R"({"a": "abc"})", R"({"a": 3.0})", R"({"a": null})"};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    auto json_field2 =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);
    json_field2->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field, json_field2});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field, json_field2}, cm);
    seg->LoadFieldData(load_info);

    proto::plan::GenericValue val;
    val.set_int64_val(1);
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        val,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto final =
        ExecuteQueryExpr(plan, seg.get(), 2 * json_strs.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 2 * json_strs.size() - 2);
}

class JsonIndexExistsTest : public ::testing::TestWithParam<std::string> {};

INSTANTIATE_TEST_SUITE_P(JsonIndexExistsTestParams,
                         JsonIndexExistsTest,
                         ::testing::Values("/a", ""));

TEST_P(JsonIndexExistsTest, TestExistsExpr) {
    std::vector<std::string> json_strs = {
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

    // bool: exists or not
    std::vector<std::tuple<std::vector<std::string>, bool, uint32_t>>
        test_cases = {
            {{"a"}, true, 0b1111101000000100},
            {{"a", "b"}, true, 0b0000100000000000},
            {{"a"}, false, 0b0000010111111011},
            {{"a", "b"}, false, 0b1111011111111111},
        };

    auto json_index_path = GetParam();

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("DOUBLE"),
            .json_path = json_index_path,
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    for (auto& json_str : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json_str)));
    }
    json_field->add_json_data(jsons);
    auto json_valid_data = json_field->ValidData();
    json_valid_data[0] = 0xFF;
    json_valid_data[1] = 0xFE;

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, json_index_path},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    for (auto& [nested_path, exists, expect] : test_cases) {
        BitsetType expect_res;
        expect_res.resize(json_strs.size());
        for (int i = json_strs.size() - 1; expect > 0; i--) {
            expect_res.set(i, (expect & 1) != 0);
            expect >>= 1;
        }

        std::shared_ptr<expr::ITypeFilterExpr> exists_expr;
        if (exists) {
            exists_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path, true));
        } else {
            auto child_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path, true));
            exists_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
        }
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);
        auto result =
            ExecuteQueryExpr(plan, seg.get(), json_strs.size(), MAX_TIMESTAMP);

        EXPECT_TRUE(result == expect_res);
    }
}

class JsonIndexBinaryExprTest : public testing::TestWithParam<JsonCastType> {};

INSTANTIATE_TEST_SUITE_P(JsonIndexBinaryExprTestParams,
                         JsonIndexBinaryExprTest,
                         testing::Values(JsonCastType::FromString("DOUBLE"),
                                         JsonCastType::FromString("VARCHAR")));

TEST_P(JsonIndexBinaryExprTest, TestBinaryRangeExpr) {
    auto json_strs = std::vector<std::string>{
        R"({"a": 1})",
        R"({"a": 2})",
        R"({"a": 3})",
        R"({"a": 4})",

        R"({"a": 1.0})",
        R"({"a": 2.0})",
        R"({"a": 3.0})",
        R"({"a": 4.0})",

        R"({"a": "1"})",
        R"({"a": "2"})",
        R"({"a": "3"})",
        R"({"a": "4"})",

        R"({"a": null})",
        R"({"a": true})",
        R"({"a": false})",
    };

    auto test_cases = std::vector<std::tuple<std::any,
                                             std::any,
                                             /*lower inclusive*/ bool,
                                             /*upper inclusive*/ bool,
                                             uint32_t>>{
        // Exact match for integer 1 (matches both int 1 and float 1.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(1),
         true,
         true,
         0b1000'1000'0000'000},

        // Range [1, 3] inclusive (matches int 1,2,3 and float 1.0,2.0,3.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         true,
         true,
         0b1110'1110'0000'000},

        // Range (1, 3) exclusive (matches only int 2 and float 2.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         false,
         false,
         0b0100'0100'0000'000},

        // Range [1, 3) left inclusive, right exclusive (matches int 1,2 and float 1.0,2.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         true,
         false,
         0b1100'1100'0000'000},

        // Range (1, 3] left exclusive, right inclusive (matches int 2,3 and float 2.0,3.0)
        {std::make_any<int64_t>(1),
         std::make_any<int64_t>(3),
         false,
         true,
         0b0110'0110'0000'000},

        // Float range test [1.0, 3.0] (matches int 1,2,3 and float 1.0,2.0,3.0)
        {std::make_any<double>(1.0),
         std::make_any<double>(3.0),
         true,
         true,
         0b1110'1110'0000'000},

        // String range test ["1", "3"] (matches string "1","2","3")
        {std::make_any<std::string>("1"),
         std::make_any<std::string>("3"),
         true,
         true,
         0b0000'0000'1110'000},

        // Range that should match nothing
        {std::make_any<int64_t>(10),
         std::make_any<int64_t>(20),
         true,
         true,
         0b0000'0000'0000'000},

        // Range [2, 4] inclusive (matches int 2,3,4 and float 2.0,3.0,4.0)
        {std::make_any<int64_t>(2),
         std::make_any<int64_t>(4),
         true,
         true,
         0b0111'0111'0000'000},

        // Mixed type range test - int to float [1, 3.0]
        // {std::make_any<int64_t>(1),
        //  std::make_any<double>(3.0),
        //  true,
        //  true,
        //  0b1110'1110'0000'000},
    };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    segcore::LoadIndexInfo load_index_info;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = GetParam(),
            .json_path = "/a",
        },
        file_manager_ctx);

    using json_index_type = index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, GetParam().ToString()}};
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    for (auto& [lower, upper, lower_inclusive, upper_inclusive, result] :
         test_cases) {
        proto::plan::GenericValue lower_val;
        proto::plan::GenericValue upper_val;
        if (lower.type() == typeid(int64_t)) {
            lower_val.set_int64_val(std::any_cast<int64_t>(lower));
        } else if (lower.type() == typeid(double)) {
            lower_val.set_float_val(std::any_cast<double>(lower));
        } else if (lower.type() == typeid(std::string)) {
            lower_val.set_string_val(std::any_cast<std::string>(lower));
        }

        if (upper.type() == typeid(int64_t)) {
            upper_val.set_int64_val(std::any_cast<int64_t>(upper));
        } else if (upper.type() == typeid(double)) {
            upper_val.set_float_val(std::any_cast<double>(upper));
        } else if (upper.type() == typeid(std::string)) {
            upper_val.set_string_val(std::any_cast<std::string>(upper));
        }

        BitsetType expect_result;
        expect_result.resize(json_strs.size());
        for (int i = json_strs.size() - 1; result > 0; i--) {
            expect_result.set(i, (result & 0x1) != 0);
            result >>= 1;
        }

        auto binary_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
            lower_val,
            upper_val,
            lower_inclusive,
            upper_inclusive);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           binary_expr);
        auto res =
            ExecuteQueryExpr(plan, seg.get(), json_strs.size(), MAX_TIMESTAMP);
        EXPECT_TRUE(res == expect_result);
    }
}

TEST(JsonNonIndexExistsTest, TestExistsExprSealedNoIndex) {
    std::vector<std::string> json_strs = {
        R"({"a": 1.0})",
        R"({"a": "abc"})",
        R"({"a": 3.0})",
        R"({"a": true})",
        R"({"a": {"b": 1}})",
        R"({"a": []})",
        R"({"a": ["a", "b"]})",
        R"({"a": null})",
        R"(1)",
        R"("abc")",
        R"(1.0)",
        R"(true)",
        R"([1, 2, 3])",
        R"({"a": 1, "b": 2})",
        R"({})",
        R"(null)",
        R"({"a": {}})",
        R"({"a": {"b": {}}})",
        R"({"a": [{}, {}]})",
        R"({"a": [[], []]})",
        R"({"a": [{"b": {}}, {"c": {}}]})",
    };

    // bool: exists or not
    std::vector<std::tuple<std::vector<std::string>, bool, uint32_t>>
        test_cases = {
            {{"a"}, true, 0b111110100000010000000},
            {{"a", "b"}, true, 0b000010000000000000000},
            {{"a"}, false, 0b000001011111101111111},
            {{"a", "b"}, false, 0b111101111111111111111},
        };

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;
    for (auto& json_str : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json_str)));
    }
    json_field->add_json_data(jsons);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    for (auto& [nested_path, exists, expect] : test_cases) {
        BitsetType expect_res;
        expect_res.resize(json_strs.size());
        for (int i = json_strs.size() - 1; expect > 0; i--) {
            expect_res.set(i, (expect & 1) != 0);
            expect >>= 1;
        }

        std::shared_ptr<expr::ITypeFilterExpr> exists_expr;
        if (exists) {
            exists_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path));
        } else {
            auto child_expr = std::make_shared<expr::ExistsExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, nested_path));
            exists_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, child_expr);
        }
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           exists_expr);
        auto result =
            ExecuteQueryExpr(plan, seg.get(), json_strs.size(), MAX_TIMESTAMP);

        EXPECT_TRUE(result == expect_res);
    }
}
