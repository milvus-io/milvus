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
#include "ExprBatchTestUtils.h"

#include <numeric>

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
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddDebugField("age32", DataType::INT32);
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

    // This fixture queries integer JSON through the historical DOUBLE cast.
    // The C++ index type must follow the configured projection, not the data.
    using IndexValueType = std::conditional_t<
        std::is_same_v<typename TestFixture::DataType, int64_t>,
        double,
        typename TestFixture::DataType>;
    using json_index_type = index::JsonInvertedIndex<IndexValueType>;
    ASSERT_NE(dynamic_cast<json_index_type*>(inv_index.get()), nullptr);
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

template <typename T>
class JsonNumericCastTest : public testing::Test {};
using JsonNumericCastTypes =
    testing::Types<int8_t, int16_t, int32_t, int64_t, double>;
TYPED_TEST_SUITE(JsonNumericCastTest, JsonNumericCastTypes);

TYPED_TEST(JsonNumericCastTest,
           IntegerAndDoubleSourcesPreserveProjectionValidity) {
    using T = TypeParam;
    const std::string cast = [] {
        if constexpr (std::is_same_v<T, double>)
            return std::string("DOUBLE");
        return std::string("INT") + std::to_string(sizeof(T) * 8);
    }();
    for (bool double_source : {false, true}) {
        for (const auto& index_type :
             {index::INVERTED_INDEX_TYPE, index::ASCENDING_SORT}) {
            SCOPED_TRACE(cast + "/" + index_type +
                         (double_source ? "/double" : "/int64"));
            auto schema = std::make_shared<Schema>();
            auto fid = schema->AddDebugField("json", DataType::JSON, true);
            auto segment = CreateSealedSegment(schema);
            const std::vector<std::string> tokens =
                double_source
                    ? std::vector<std::string>{"1.0",
                                               "2.0",
                                               "2.5",
                                               "9007199254740992.0",
                                               "9007199254740994.0",
                                               "9223372036854775808.0",
                                               "300.0"}
                    : std::vector<std::string>{"1",
                                               "2",
                                               "3",
                                               "9007199254740992",
                                               "9007199254740993",
                                               "9223372036854775807",
                                               "300"};
            std::vector<milvus::Json> rows;
            for (const auto& token : tokens) {
                rows.emplace_back(
                    simdjson::padded_string("{\"a\":" + token + "}"));
            }
            for (const auto& raw : {R"({"a":"2"})",
                                    R"({"a":null})",
                                    "{}",
                                    R"({"a":1e400})",
                                    R"({"a":2})"}) {
                rows.emplace_back(simdjson::padded_string(std::string(raw)));
            }
            const auto count = rows.size();
            auto field =
                std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
            field->add_json_data(rows);
            std::fill(field->ValidData(),
                      field->ValidData() + field->ValidDataSize(),
                      uint8_t{0xff});
            field->ValidData()[(count - 1) / 8] &= ~(1 << ((count - 1) % 8));

            auto ctx = storage::FileManagerContext();
            ctx.fieldDataMeta.field_schema.set_data_type(proto::schema::JSON);
            ctx.fieldDataMeta.field_schema.set_nullable(true);
            ctx.fieldDataMeta.field_schema.set_fieldid(fid.get());
            ctx.fieldDataMeta.field_id = fid.get();
            auto idx = index::IndexFactory::GetInstance().CreateJsonIndex(
                index::CreateIndexInfo{
                    .index_type = index_type,
                    .json_cast_type = JsonCastType::FromString(cast),
                    .json_path = "/a"},
                ctx);
            ASSERT_NE(dynamic_cast<index::ScalarIndex<T>*>(idx.get()), nullptr);
            dynamic_cast<index::ScalarIndex<T>*>(idx.get())->BuildWithFieldData(
                {field});
            if (auto* inverted =
                    dynamic_cast<index::JsonInvertedIndex<T>*>(idx.get())) {
                inverted->finish();
                inverted->create_reader(index::SetBitsetSealed);
            }
            segcore::LoadIndexInfo load;
            load.field_id = fid.get();
            load.field_type = DataType::JSON;
            load.index_params = {{JSON_PATH, "/a"}, {JSON_CAST_TYPE, cast}};
            load.cache_index =
                CreateTestCacheIndex("numeric_cast_" + cast + index_type +
                                         std::to_string(double_source),
                                     std::move(idx));
            segment->LoadIndex(load);
            auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                          .GetRemoteChunkManager();
            std::vector<int64_t> ids(count);
            std::iota(ids.begin(), ids.end(), 0);
            auto id_field = storage::CreateFieldData(
                DataType::INT64, DataType::NONE, false);
            id_field->FillFieldData(ids.data(), ids.size());
            segment->LoadFieldData(PrepareSingleFieldInsertBinlog(
                1, 1, 1, RowFieldID.get(), {id_field}, cm));
            ASSERT_FALSE(segment->HasFieldData(fid));

            std::vector<bool> valid(count, false);
            valid[0] = valid[1] = true;
            valid[2] = !double_source || std::is_same_v<T, double>;
            valid[3] = valid[4] = sizeof(T) == 8;
            valid[5] =
                std::is_same_v<T, double> || (sizeof(T) == 8 && !double_source);
            valid[6] = sizeof(T) > 1;
            auto check = [&](const expr::TypedExprPtr& predicate,
                             const std::vector<bool>& matches,
                             const std::vector<bool>& validity,
                             bool expect_index = true) {
                SCOPED_TRACE(predicate->ToString());
                if (expect_index) {
                    ASSERT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
                        predicate, segment.get(), count));
                }
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, predicate);
                auto result = milvus::test::gen_filter_res(
                    plan.get(), segment.get(), count, MAX_TIMESTAMP);
                TargetBitmapView bits(result->GetRawData(), count);
                TargetBitmapView nulls(result->GetValidRawData(), count);
                for (size_t i = 0; i < count; ++i) {
                    EXPECT_EQ(nulls[i], validity[i]) << "row " << i;
                    if (validity[i])
                        EXPECT_EQ(bits[i], matches[i]) << "row " << i;
                }
                auto negated = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, predicate);
                auto not_plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, negated);
                auto not_result = milvus::test::gen_filter_res(
                    not_plan.get(), segment.get(), count, MAX_TIMESTAMP);
                TargetBitmapView not_bits(not_result->GetRawData(), count);
                TargetBitmapView not_valid(not_result->GetValidRawData(),
                                           count);
                for (size_t i = 0; i < count; ++i) {
                    EXPECT_EQ(not_valid[i], validity[i]) << "NOT row " << i;
                    EXPECT_EQ(not_bits[i] && not_valid[i],
                              validity[i] && !matches[i])
                        << "NOT row " << i;
                }
            };
            auto value = [](int64_t n) {
                proto::plan::GenericValue v;
                v.set_int64_val(n);
                return v;
            };
            const auto col = expr::ColumnInfo(fid, DataType::JSON, {"a"});
            std::vector<bool> matches(count, false);
            matches[1] = true;
            check(std::make_shared<expr::UnaryRangeFilterExpr>(
                      col,
                      proto::plan::Equal,
                      value(2),
                      std::vector<proto::plan::GenericValue>{}),
                  matches,
                  valid);
            check(std::make_shared<expr::TermFilterExpr>(
                      col,
                      std::vector<proto::plan::GenericValue>{value(2)},
                      false),
                  matches,
                  valid);
            matches[0] = true;
            matches[2] = valid[2];
            check(std::make_shared<expr::BinaryRangeFilterExpr>(
                      col, value(1), value(3), true, true),
                  matches,
                  valid);
            matches.assign(count, false);
            matches[3] = std::is_same_v<T, double>;
            matches[4] = !double_source && sizeof(T) == 8;
            check(std::make_shared<expr::TermFilterExpr>(
                      col,
                      std::vector<proto::plan::GenericValue>{
                          value(9007199254740993LL)},
                      false),
                  matches,
                  valid);
            if constexpr (sizeof(T) < 8) {
                const int64_t overflow =
                    int64_t(std::numeric_limits<T>::max()) + 1;
                matches.assign(count, false);
                check(std::make_shared<expr::TermFilterExpr>(
                          col,
                          std::vector<proto::plan::GenericValue>{
                              value(overflow), value(overflow + 1)},
                          false),
                      matches,
                      valid);
                matches[0] = true;
                check(std::make_shared<expr::TermFilterExpr>(
                          col,
                          std::vector<proto::plan::GenericValue>{
                              value(1), value(overflow)},
                          false),
                      matches,
                      valid);
            }
            if constexpr (std::is_same_v<T, double>) {
                proto::plan::GenericValue fractional;
                fractional.set_float_val(2.5);
                matches.assign(count, false);
                matches[2] = double_source;
                check(std::make_shared<expr::UnaryRangeFilterExpr>(
                          col,
                          proto::plan::Equal,
                          fractional,
                          std::vector<proto::plan::GenericValue>{}),
                      matches,
                      valid);
            }
            // Empty IN has no numeric literal type and uses the raw executor's
            // constant-result path; load its chunk metadata only after all
            // typed-index-only assertions above have run.
            segment->LoadFieldData(PrepareSingleFieldInsertBinlog(
                1, 1, 1, fid.get(), {field}, cm));
            matches.assign(count, false);
            check(std::make_shared<expr::TermFilterExpr>(
                      col, std::vector<proto::plan::GenericValue>{}, false),
                  matches,
                  std::vector<bool>(count, true),
                  false);  // Literal IN [] is constant, without index lookup.
        }
    }
}

TEST(JsonIndexTest, JsonSortLikeUsesIndexWithoutRawJson) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    auto seg = CreateSealedSegment(schema);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto json_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::ASCENDING_SORT,
            .json_cast_type = JsonCastType::FromString("VARCHAR"),
            .json_path = "/s",
        },
        file_manager_ctx);

    const std::vector<std::string> json_strs = {
        R"({"s": "alpha"})",
        R"({"s": "alphabet"})",
        R"({"s": "beta"})",
        R"({"s": "theta"})",
        R"({"s": "alpha"})",
        R"({"other": "alpha"})",
        R"({"s": null})",
        R"({"s": 42})",
    };

    // Load only the system row IDs so the sealed segment has its production
    // row count. The raw JSON field remains deliberately unloaded.
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto cm_w = ChunkManagerWrapper(cm);
    std::vector<int64_t> row_ids(json_strs.size());
    for (size_t i = 0; i < row_ids.size(); ++i) {
        row_ids[i] = i;
    }
    auto row_id_field_data =
        storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
    row_id_field_data->FillFieldData(row_ids.data(), row_ids.size());
    auto row_id_load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, RowFieldID.get(), {row_id_field_data}, cm);
    seg->LoadFieldData(row_id_load_info);

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;
    jsons.reserve(json_strs.size());
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    auto scalar_index =
        dynamic_cast<index::ScalarIndex<std::string>*>(json_index.get());
    ASSERT_NE(scalar_index, nullptr);
    scalar_index->BuildWithFieldData({json_field});

    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/s"},
                                    {JSON_CAST_TYPE, "VARCHAR"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("json_sort_like", std::move(json_index));
    seg->LoadIndex(load_index_info);
    ASSERT_FALSE(seg->HasFieldData(json_fid));

    // Deliberately do not load raw JSON field data. These predicates must be
    // executable from the path index alone in lazy-load scenarios.
    const std::vector<
        std::tuple<proto::plan::OpType, std::string, std::vector<size_t>>>
        test_cases = {
            {proto::plan::OpType::InnerMatch, "pha", {0, 1, 4}},
            {proto::plan::OpType::PostfixMatch, "ta", {2, 3}},
            {proto::plan::OpType::Match, "a_ph%", {0, 1, 4}},
        };

    for (const auto& [op, pattern, matched_rows] : test_cases) {
        proto::plan::GenericValue value;
        value.set_string_val(pattern);
        auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"s"}),
            op,
            value,
            std::vector<proto::plan::GenericValue>());
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           unary_expr);
        auto result = milvus::test::gen_filter_res(
            plan.get(), seg.get(), json_strs.size(), MAX_TIMESTAMP);
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());

        std::vector<bool> expected(json_strs.size(), false);
        for (auto row : matched_rows) {
            expected[row] = true;
        }
        for (size_t i = 0; i < json_strs.size(); ++i) {
            EXPECT_EQ(result_view[i], expected[i])
                << "op " << op << ", row " << i;
            EXPECT_EQ(valid_view[i], i < 5) << "op " << op << ", row " << i;
        }
    }
}

TEST(JsonIndexTest, JsonBinaryRangePathIndexMatchesRawData) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    const std::vector<std::string> json_strs = {
        R"({"n": 1, "s": "alpha"})",
        R"({"n": 2, "s": "beta"})",
        R"({"n": 3.5, "s": "gamma"})",
        R"({"n": "2", "s": 2})",
        R"({"other": 0})",
        R"({"n": null, "s": null})",
        R"({"n": 4, "s": "delta"})",
        R"({"n": 5, "s": "epsilon"})",
        R"({"n": 9007199254740993, "s": "zeta"})",
    };
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    jsons.reserve(json_strs.size());
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    auto* valid_data = json_field->ValidData();
    std::fill(valid_data,
              valid_data + json_field->ValidDataSize(),
              static_cast<uint8_t>(0));
    valid_data[0] = 0b01111111;
    valid_data[1] = 0b00000001;

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto raw_segment = CreateSealedSegment(schema);
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto make_index_segment = [&](const JsonCastType& cast_type,
                                  const std::string& path,
                                  const std::string& cache_key,
                                  bool load_raw_json = false) {
        auto segment = CreateSealedSegment(schema);

        std::vector<int64_t> row_ids(json_strs.size());
        std::iota(row_ids.begin(), row_ids.end(), 0);
        auto row_id_field_data =
            storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
        row_id_field_data->FillFieldData(row_ids.data(), row_ids.size());
        auto row_id_load_info = PrepareSingleFieldInsertBinlog(
            1, 1, 1, RowFieldID.get(), {row_id_field_data}, cm);
        segment->LoadFieldData(row_id_load_info);

        auto file_manager_ctx = storage::FileManagerContext();
        file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
            proto::schema::JSON);
        file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
        file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
        file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
        auto json_index = index::IndexFactory::GetInstance().CreateJsonIndex(
            index::CreateIndexInfo{
                .index_type = index::INVERTED_INDEX_TYPE,
                .json_cast_type = cast_type,
                .json_path = path,
            },
            file_manager_ctx);
        if (cast_type.data_type() == JsonCastType::DataType::DOUBLE) {
            auto* typed_index = dynamic_cast<index::JsonInvertedIndex<double>*>(
                json_index.get());
            AssertInfo(typed_index != nullptr,
                       "expected a DOUBLE JSON path index");
            typed_index->BuildWithFieldData({json_field});
            typed_index->finish();
            typed_index->create_reader(milvus::index::SetBitsetSealed);
        } else {
            auto* typed_index =
                dynamic_cast<index::JsonInvertedIndex<std::string>*>(
                    json_index.get());
            AssertInfo(typed_index != nullptr,
                       "expected a VARCHAR JSON path index");
            typed_index->BuildWithFieldData({json_field});
            typed_index->finish();
            typed_index->create_reader(milvus::index::SetBitsetSealed);
        }

        segcore::LoadIndexInfo load_index_info;
        load_index_info.field_id = json_fid.get();
        load_index_info.field_type = DataType::JSON;
        load_index_info.index_params = {{JSON_PATH, path},
                                        {JSON_CAST_TYPE, cast_type.ToString()}};
        load_index_info.cache_index =
            CreateTestCacheIndex(cache_key, std::move(json_index));
        segment->LoadIndex(load_index_info);
        if (load_raw_json) {
            auto raw_load_info = PrepareSingleFieldInsertBinlog(
                1, 1, 2, json_fid.get(), {json_field}, cm);
            segment->LoadFieldData(raw_load_info);
        } else {
            AssertInfo(!segment->HasFieldData(json_fid),
                       "raw JSON must stay unloaded for this test");
        }
        return segment;
    };

    auto number_index_segment = make_index_segment(
        JsonCastType::FromString("DOUBLE"), "/n", "json_binary_range_number");
    auto string_index_segment = make_index_segment(
        JsonCastType::FromString("VARCHAR"), "/s", "json_binary_range_string");
    // Deliberately index-only: a large-integer bound no longer falls back to
    // a raw scan, so the DOUBLE Path index must answer without raw JSON.
    auto precise_number_segment =
        make_index_segment(JsonCastType::FromString("DOUBLE"),
                           "/n",
                           "json_binary_range_precise_number",
                           false);
    auto evaluate = [&](const expr::TypedExprPtr& filter_expr,
                        const segcore::SegmentInternalInterface* segment,
                        exec::OffsetVector* offsets = nullptr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment, json_strs.size(), MAX_TIMESTAMP, offsets);
    };
    auto expect_same = [&](const ColumnVectorPtr& raw,
                           const ColumnVectorPtr& indexed) {
        ASSERT_EQ(raw->size(), indexed->size());
        TargetBitmapView raw_result(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        TargetBitmapView index_result(indexed->GetRawData(), indexed->size());
        TargetBitmapView index_valid(indexed->GetValidRawData(),
                                     indexed->size());
        for (size_t i = 0; i < raw->size(); ++i) {
            EXPECT_EQ(index_valid[i], raw_valid[i]) << "row " << i;
            EXPECT_EQ(index_result[i], raw_result[i]) << "row " << i;
        }
    };

    proto::plan::GenericValue number_lower;
    number_lower.set_int64_val(2);
    proto::plan::GenericValue number_upper;
    number_upper.set_float_val(4.0);
    auto number_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        number_lower,
        number_upper,
        true,
        true);
    expect_same(evaluate(number_expr, raw_segment.get()),
                evaluate(number_expr, number_index_segment.get()));

    // Keep the indexed segment path-index-only: before the offset index path
    // was supported, evaluation tried to reverse-lookup raw Json values from
    // ScalarIndex<double> and crashed instead of producing candidate results.
    exec::OffsetVector offsets = {7, 2, 4, 1, 3, 5, 6, 0, 2};
    expect_same(evaluate(number_expr, raw_segment.get(), &offsets),
                evaluate(number_expr, number_index_segment.get(), &offsets));

    proto::plan::GenericValue string_lower;
    string_lower.set_string_val("beta");
    proto::plan::GenericValue string_upper;
    string_upper.set_string_val("gamma");
    auto string_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"s"}),
        string_lower,
        string_upper,
        true,
        false);
    expect_same(evaluate(string_expr, raw_segment.get()),
                evaluate(string_expr, string_index_segment.get()));
    expect_same(evaluate(string_expr, raw_segment.get(), &offsets),
                evaluate(string_expr, string_index_segment.get(), &offsets));

    proto::plan::GenericValue precise_lower;
    precise_lower.set_float_val(9007199254740992.0);
    proto::plan::GenericValue precise_upper;
    precise_upper.set_float_val(9007199254740994.0);
    auto precise_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        precise_lower,
        precise_upper,
        false,
        false);
    // Documented Path-index difference. This index uses a DOUBLE projection:
    // row 8 holds 2^53+1, which
    // the index stores as 2^53. The exclusive range (2^53, 2^53+2) therefore
    // excludes it, while a raw scan compares the integer exactly and keeps
    // it. The index answers rather than declining to a raw scan; see
    // docs/agent_guides/json-filtering/cross-path-semantics.md.
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        precise_expr, precise_number_segment.get(), json_strs.size()));
    auto raw_precise = evaluate(precise_expr, raw_segment.get());
    auto indexed_precise = evaluate(precise_expr, precise_number_segment.get());
    TargetBitmapView precise_result(raw_precise->GetRawData(),
                                    raw_precise->size());
    TargetBitmapView precise_valid(raw_precise->GetValidRawData(),
                                   raw_precise->size());
    TargetBitmapView indexed_precise_result(indexed_precise->GetRawData(),
                                            indexed_precise->size());
    TargetBitmapView indexed_precise_valid(indexed_precise->GetValidRawData(),
                                           indexed_precise->size());
    ASSERT_EQ(raw_precise->size(), indexed_precise->size());
    for (size_t i = 0; i + 1 < raw_precise->size(); ++i) {
        EXPECT_EQ(indexed_precise_valid[i], precise_valid[i]) << "row " << i;
        EXPECT_EQ(indexed_precise_result[i], precise_result[i]) << "row " << i;
    }
    EXPECT_TRUE(precise_valid[8]);
    EXPECT_TRUE(precise_result[8]);
    EXPECT_TRUE(indexed_precise_valid[8]);
    EXPECT_FALSE(indexed_precise_result[8])
        << "DOUBLE path index rounds 2^53+1 down to 2^53, which the "
           "exclusive lower bound rejects";
}

TEST(JsonIndexTest, JsonBinaryRangeFlatIndexSupportsOffsetInputWithoutRawJson) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    const std::vector<std::string> json_strs = {
        R"({"n": 1})",
        R"({"n": 2})",
        R"({"n": 3.5})",
        R"({"n": "3"})",
        R"({"other": 4})",
        R"({"n": null})",
        R"({"n": 4})",
        R"({"n": 5})",
    };
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    jsons.reserve(json_strs.size());
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    auto* valid_data = json_field->ValidData();
    std::fill(valid_data,
              valid_data + json_field->ValidDataSize(),
              static_cast<uint8_t>(0));
    valid_data[0] = 0b01111111;

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto raw_segment = CreateSealedSegment(schema);
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    auto flat_segment = CreateSealedSegment(schema);
    std::vector<int64_t> row_ids(json_strs.size());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    auto row_id_field_data =
        storage::CreateFieldData(DataType::INT64, DataType::NONE, false);
    row_id_field_data->FillFieldData(row_ids.data(), row_ids.size());
    auto row_id_load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, RowFieldID.get(), {row_id_field_data}, cm);
    flat_segment->LoadFieldData(row_id_load_info);

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
    auto json_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("JSON"),
            .json_path = "",
        },
        file_manager_ctx);
    auto* flat_index = dynamic_cast<index::JsonFlatIndex*>(json_index.get());
    ASSERT_NE(flat_index, nullptr);
    flat_index->BuildWithFieldData({json_field});
    flat_index->finish();
    flat_index->create_reader(milvus::index::SetBitsetSealed);

    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, ""}, {JSON_CAST_TYPE, "JSON"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("json_binary_range_flat", std::move(json_index));
    flat_segment->LoadIndex(load_index_info);
    ASSERT_FALSE(flat_segment->HasFieldData(json_fid));

    proto::plan::GenericValue lower;
    lower.set_int64_val(2);
    proto::plan::GenericValue upper;
    upper.set_int64_val(4);
    auto range_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        lower,
        upper,
        true,
        true);
    auto evaluate = [&](const segcore::SegmentInternalInterface* segment,
                        exec::OffsetVector* offsets) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           range_expr);
        return milvus::test::gen_filter_res(
            plan.get(), segment, json_strs.size(), MAX_TIMESTAMP, offsets);
    };

    exec::OffsetVector offsets = {6, 2, 4, 1, 3, 5, 7, 0, 2};
    auto raw_result = evaluate(raw_segment.get(), &offsets);
    ColumnVectorPtr flat_result;
    EXPECT_NO_THROW(flat_result = evaluate(flat_segment.get(), &offsets));
    ASSERT_NE(flat_result, nullptr);
    ASSERT_EQ(raw_result->size(), flat_result->size());

    TargetBitmapView raw_values(raw_result->GetRawData(), raw_result->size());
    TargetBitmapView raw_validity(raw_result->GetValidRawData(),
                                  raw_result->size());
    TargetBitmapView flat_values(flat_result->GetRawData(),
                                 flat_result->size());
    TargetBitmapView flat_validity(flat_result->GetValidRawData(),
                                   flat_result->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(flat_values[i], raw_values[i]) << "candidate " << i;
        EXPECT_EQ(flat_validity[i], raw_validity[i]) << "candidate " << i;
    }
}

TEST(JsonIndexTest, EmptyJsonInIsDeterministicForEveryRow) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();

    auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("BOOL"),
            .json_path = "/a",
        },
        file_manager_ctx);
    auto json_index = std::unique_ptr<index::JsonInvertedIndex<bool>>(
        static_cast<index::JsonInvertedIndex<bool>*>(inv_index.release()));

    const std::vector<std::string> json_strs = {R"({"a": true})",
                                                R"({"a": "abc"})",
                                                R"({"b": false})",
                                                R"({"a": null})",
                                                R"({})",
                                                R"({"a": false})"};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    auto* valid_data = json_field->ValidData();
    std::fill(valid_data,
              valid_data + json_field->ValidDataSize(),
              static_cast<uint8_t>(0));
    valid_data[0] = 0b00011111;

    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, "BOOL"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("empty_json_in", std::move(json_index));
    seg->LoadIndex(load_index_info);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{},
        false);
    auto check = [&](const expr::TypedExprPtr& filter_expr,
                     bool expected_result) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        auto result = milvus::test::gen_filter_res(
            plan.get(), seg.get(), json_strs.size(), MAX_TIMESTAMP);
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

// A DOUBLE path index is the projection the user configured, and it answers
// large-integer predicates inside that projection instead of declining to a
// raw scan. 2^53 (row 0) and 2^53+1 (row 1) share one double, so equality,
// IN and BETWEEN report both. JsonRawScanTest below pins the exact integer
// semantics that raw, stats and Flat keep. See
// docs/agent_guides/json-filtering/cross-path-semantics.md, case 8.
TEST(JsonIndexTest, LargeInt64LiteralAliasesInDoublePathIndex) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateSealedSegment(schema);
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
    auto json_index = std::unique_ptr<index::JsonInvertedIndex<double>>(
        static_cast<index::JsonInvertedIndex<double>*>(inv_index.release()));

    const std::vector<std::string> json_strs = {
        R"({"a": 9007199254740992})",
        R"({"a": 9007199254740993})",
        R"({"a": 9007199254740994})",
        R"({"a": 9223372036854775808})"};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, "/a"},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("large_int64", std::move(json_index));
    seg->LoadIndex(load_index_info);

    // Keep this segment index-only. If any large-integer predicate is routed
    // back to RawData, evaluation must fail instead of being masked by a loaded
    // JSON column.
    ASSERT_FALSE(seg->HasFieldData(json_fid));

    const auto evaluate = [&](const expr::TypedExprPtr& expr) {
        EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
            expr, seg.get(), json_strs.size()));
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto result = milvus::test::gen_filter_res(
            plan.get(), seg.get(), json_strs.size(), MAX_TIMESTAMP);
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        std::vector<bool> matches;
        matches.reserve(result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_TRUE(valid_view[i]);
            matches.push_back(result_view[i]);
        }
        return matches;
    };

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    auto result = evaluate(equal_expr);
    EXPECT_TRUE(result[0]) << "2^53 aliases 2^53+1 through double";
    EXPECT_TRUE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{value},
        false);
    result = evaluate(term_expr);
    EXPECT_TRUE(result[0]);
    EXPECT_TRUE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    // A strict lower bound at 2^53 excludes both aliased rows, so this one
    // happens to agree with an exact integer comparison.
    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    result = evaluate(greater_expr);
    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_TRUE(result[2]);
    EXPECT_TRUE(result[3]);

    auto between_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        value,
        value,
        true,
        true);
    result = evaluate(between_expr);
    EXPECT_TRUE(result[0]);
    EXPECT_TRUE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    proto::plan::GenericValue two_to_53;
    two_to_53.set_float_val(9007199254740992.0);
    auto float_term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{two_to_53},
        false);
    result = evaluate(float_term_expr);
    EXPECT_TRUE(result[0]);
    EXPECT_TRUE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    auto float_equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        two_to_53,
        std::vector<proto::plan::GenericValue>());
    result = evaluate(float_equal_expr);
    EXPECT_TRUE(result[0]);
    EXPECT_TRUE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    proto::plan::GenericValue int64_min;
    int64_min.set_int64_val(std::numeric_limits<int64_t>::min());
    auto min_term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{int64_min},
        false);
    result = evaluate(min_term_expr);
    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_FALSE(result[3]);

    proto::plan::GenericValue two_to_63;
    two_to_63.set_float_val(9223372036854775808.0);
    auto uint64_term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{two_to_63},
        false);
    result = evaluate(uint64_term_expr);
    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_FALSE(result[2]);
    EXPECT_TRUE(result[3]);
}

TEST(JsonRawScanTest, EmptyInAndLargeInt64KeepThreeValuedSemantics) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto seg = CreateSealedSegment(schema);

    const std::vector<std::string> json_strs = {R"({"a": 9007199254740992})",
                                                R"({"a": 9007199254740993})",
                                                R"({"a": 9007199254740994})",
                                                R"({"a": 9007199254740992.0})",
                                                R"({"a": "abc"})",
                                                R"({})",
                                                R"({"a": null})",
                                                R"({"a": 9007199254740993})"};
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    std::vector<milvus::Json> jsons;
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);
    auto* valid_data = json_field->ValidData();
    std::fill(valid_data,
              valid_data + json_field->ValidDataSize(),
              static_cast<uint8_t>(0));
    valid_data[0] = 0b01111111;

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), seg.get(), json_strs.size(), MAX_TIMESTAMP);
    };
    auto check = [](const ColumnVectorPtr& result,
                    const std::vector<bool>& expected_result,
                    const std::vector<bool>& expected_valid) {
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
            if (expected_valid[i]) {
                EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
            }
        }
    };

    auto empty_term = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{},
        false);
    check(evaluate(empty_term),
          std::vector<bool>(json_strs.size(), false),
          std::vector<bool>(json_strs.size(), true));
    check(evaluate(std::make_shared<expr::LogicalUnaryExpr>(
              expr::LogicalUnaryExpr::OpType::LogicalNot, empty_term)),
          std::vector<bool>(json_strs.size(), true),
          std::vector<bool>(json_strs.size(), true));

    proto::plan::GenericValue value;
    value.set_int64_val(9007199254740993LL);
    const std::vector<bool> numeric_valid = {
        true, true, true, true, false, false, false, false};
    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::Equal,
        value,
        std::vector<proto::plan::GenericValue>());
    check(evaluate(equal_expr),
          {false, true, false, false, false, false, false, false},
          numeric_valid);

    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        std::vector<proto::plan::GenericValue>{value},
        false);
    check(evaluate(term_expr),
          {false, true, false, false, false, false, false, false},
          numeric_valid);

    auto greater_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>());
    check(evaluate(greater_expr),
          {false, false, true, false, false, false, false, false},
          numeric_valid);

    auto between_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        value,
        value,
        true,
        true);
    check(evaluate(between_expr),
          {false, true, false, false, false, false, false, false},
          numeric_valid);
}

TEST(JsonRawScanTest, NumberErrorIsLimitedToTheAccessedPathOrArrayElement) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    auto seg = CreateSealedSegment(schema);

    const std::vector<std::string> json_strs = {
        R"({"bad":1e400,"ok":7,"target":[1,"x",true,[1,2]]})",
        R"({"bad":1e400,"ok":8,"target":[1e400,[3,4],[1,2],7,"x"]})",
        R"({"bad":1e400,"ok":9,"target":[1e400,[3,4]]})",
    };
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;
    jsons.reserve(json_strs.size());
    for (const auto& json : json_strs) {
        jsons.emplace_back(simdjson::padded_string(json));
    }
    json_field->add_json_data(jsons);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, cm);
    seg->LoadFieldData(load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           filter_expr);
        return milvus::test::gen_filter_res(
            plan.get(), seg.get(), json_strs.size(), MAX_TIMESTAMP);
    };
    auto check = [](const ColumnVectorPtr& result,
                    const std::vector<bool>& expected_result,
                    const std::vector<bool>& expected_valid) {
        ASSERT_EQ(result->size(), expected_result.size());
        ASSERT_EQ(result->size(), expected_valid.size());
        TargetBitmapView result_view(result->GetRawData(), result->size());
        TargetBitmapView valid_view(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < result->size(); ++i) {
            EXPECT_EQ(valid_view[i], expected_valid[i]) << "row " << i;
            EXPECT_EQ(result_view[i], expected_result[i]) << "row " << i;
        }
    };

    proto::plan::GenericValue seven;
    seven.set_int64_val(7);
    proto::plan::GenericValue zero;
    zero.set_int64_val(0);
    proto::plan::GenericValue ten;
    ten.set_int64_val(10);

    auto bad_column = expr::ColumnInfo(json_fid, DataType::JSON, {"bad"});
    check(
        evaluate(std::make_shared<expr::TermFilterExpr>(
            bad_column, std::vector<proto::plan::GenericValue>{seven}, false)),
        {false, false, false},
        {false, false, false});
    check(evaluate(std::make_shared<expr::UnaryRangeFilterExpr>(
              bad_column,
              proto::plan::OpType::GreaterThan,
              seven,
              std::vector<proto::plan::GenericValue>())),
          {false, false, false},
          {false, false, false});
    check(evaluate(std::make_shared<expr::BinaryRangeFilterExpr>(
              bad_column, zero, ten, true, true)),
          {false, false, false},
          {false, false, false});
    check(evaluate(std::make_shared<expr::ExistsExpr>(bad_column)),
          {false, false, false},
          {true, true, true});

    auto ok_column = expr::ColumnInfo(json_fid, DataType::JSON, {"ok"});
    check(evaluate(std::make_shared<expr::TermFilterExpr>(
              ok_column, std::vector<proto::plan::GenericValue>{seven}, false)),
          {true, false, false},
          {true, true, true});
    check(evaluate(std::make_shared<expr::BinaryRangeFilterExpr>(
              ok_column, seven, ten, true, false)),
          {true, true, true},
          {true, true, true});

    proto::plan::GenericValue missing;
    missing.set_string_val("missing");
    auto target_column = expr::ColumnInfo(json_fid, DataType::JSON, {"target"});
    check(evaluate(std::make_shared<expr::JsonContainsExpr>(
              target_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
              false,
              std::vector<proto::plan::GenericValue>{seven, missing})),
          {false, true, false},
          {true, true, true});

    proto::plan::GenericValue x;
    x.set_string_val("x");
    check(evaluate(std::make_shared<expr::JsonContainsExpr>(
              target_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
              false,
              std::vector<proto::plan::GenericValue>{seven, x})),
          {false, true, false},
          {true, true, true});

    proto::plan::GenericValue array_1_2;
    array_1_2.mutable_array_val()->add_array()->set_int64_val(1);
    array_1_2.mutable_array_val()->add_array()->set_int64_val(2);
    proto::plan::GenericValue array_3_4;
    array_3_4.mutable_array_val()->add_array()->set_int64_val(3);
    array_3_4.mutable_array_val()->add_array()->set_int64_val(4);
    proto::plan::GenericValue array_9_9;
    array_9_9.mutable_array_val()->add_array()->set_int64_val(9);
    array_9_9.mutable_array_val()->add_array()->set_int64_val(9);
    check(evaluate(std::make_shared<expr::JsonContainsExpr>(
              target_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
              false,
              std::vector<proto::plan::GenericValue>{array_9_9, array_1_2})),
          {true, true, false},
          {true, true, true});
    check(evaluate(std::make_shared<expr::JsonContainsExpr>(
              target_column,
              proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
              false,
              std::vector<proto::plan::GenericValue>{array_3_4, array_1_2})),
          {false, true, false},
          {true, true, true});
}

TEST(JsonIndexTest, TestJsonNotEqualExpr) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
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
    auto json_strs = std::vector<std::string>{R"({"a": 1.0})",
                                              R"({"a": "abc"})",
                                              R"({"a": 3.0})",
                                              R"({"a": null})",
                                              R"({"b": 2.0})",
                                              R"({"a": 0.0})"};
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
    EXPECT_EQ(final.count(), 4);
    EXPECT_TRUE(final[2]);
    EXPECT_TRUE(final[5]);
    EXPECT_TRUE(final[8]);
    EXPECT_TRUE(final[11]);

    auto greater_than_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::GreaterThan,
        val,
        std::vector<proto::plan::GenericValue>());
    auto not_greater_than_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, greater_than_expr);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  not_greater_than_expr);
    final =
        ExecuteQueryExpr(plan, seg.get(), 2 * json_strs.size(), MAX_TIMESTAMP);
    EXPECT_EQ(final.count(), 4);
    EXPECT_TRUE(final[0]);
    EXPECT_TRUE(final[5]);
    EXPECT_TRUE(final[6]);
    EXPECT_TRUE(final[11]);
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
            {{"a"}, true, 0b1111111000000100},
            {{"a", "b"}, true, 0b0000100000000000},
            {{"a"}, false, 0b0000000111111011},
            {{"a", "b"}, false, 0b1111011111111111},
        };

    auto json_index_path = GetParam();

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
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
    milvus::test::ExprBatchSizeGuard batch_size_guard(7);
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
    schema->AddDebugField(
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

    auto json_index = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = GetParam(),
            .json_path = "/a",
        },
        file_manager_ctx);
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    std::vector<milvus::Json> jsons;

    for (auto& json : json_strs) {
        jsons.push_back(milvus::Json(simdjson::padded_string(json)));
    }
    json_field->add_json_data(jsons);
    if (GetParam().data_type() == JsonCastType::DataType::DOUBLE) {
        auto* typed_index =
            dynamic_cast<index::JsonInvertedIndex<double>*>(json_index.get());
        ASSERT_NE(typed_index, nullptr);
        typed_index->BuildWithFieldData({json_field});
        typed_index->finish();
        typed_index->create_reader(milvus::index::SetBitsetSealed);
    } else {
        auto* typed_index =
            dynamic_cast<index::JsonInvertedIndex<std::string>*>(
                json_index.get());
        ASSERT_NE(typed_index, nullptr);
        typed_index->BuildWithFieldData({json_field});
        typed_index->finish();
        typed_index->create_reader(milvus::index::SetBitsetSealed);
    }

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
        std::vector<int64_t> batch_sizes;
        EXPECT_NO_THROW(batch_sizes = milvus::test::EvalExprBatchSizes(
                            binary_expr, seg.get(), json_strs.size()));
        EXPECT_EQ(batch_sizes, (std::vector<int64_t>{7, 7, 1}));
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
            {{"a"}, true, 0b111111100000010011111},
            {{"a", "b"}, true, 0b000010000000000001000},
            {{"a"}, false, 0b000000011111101100000},
            {{"a", "b"}, false, 0b111101111111111110111},
        };

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
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
