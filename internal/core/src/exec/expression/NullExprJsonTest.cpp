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

namespace {

TEST(JsonNullExprTest, RootNullUsesSourceValidityWithTypedPathIndex) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto segment = CreateSealedSegment(schema);

    // Row 0 is a SQL NULL.  Rows 1-3 are valid JSON values which cannot be
    // projected to DOUBLE; the regression was that the Path index classified
    // all four rows as NULL.
    const std::vector<std::string> json_strs = {
        R"(0)", R"({})", R"({"a": null})", R"({"a": "x"})", R"(1)", R"(2.5)"};
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
    valid_data[0] = 0b00111110;

    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    file_manager_ctx.fieldDataMeta.field_schema.set_nullable(true);
    file_manager_ctx.fieldDataMeta.field_id = json_fid.get();
    auto index_base = index::IndexFactory::GetInstance().CreateJsonIndex(
        index::CreateIndexInfo{
            .index_type = index::INVERTED_INDEX_TYPE,
            .json_cast_type = JsonCastType::FromString("DOUBLE"),
            .json_path = "",
        },
        file_manager_ctx);
    auto json_index = std::unique_ptr<index::JsonInvertedIndex<double>>(
        static_cast<index::JsonInvertedIndex<double>*>(index_base.release()));
    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = json_fid.get();
    load_index_info.field_type = DataType::JSON;
    load_index_info.index_params = {{JSON_PATH, ""},
                                    {JSON_CAST_TYPE, "DOUBLE"}};
    load_index_info.cache_index =
        CreateTestCacheIndex("json_root_null", std::move(json_index));
    segment->LoadIndex(load_index_info);

    auto chunk_manager =
        milvus::storage::RemoteChunkManagerSingleton::GetInstance()
            .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        1, 1, 1, json_fid.get(), {json_field}, chunk_manager);
    segment->LoadFieldData(load_info);

    auto evaluate = [&](proto::plan::NullExpr_NullOp op,
                        exec::OffsetVector* offsets = nullptr) {
        auto null_expr = std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {}, true), op);
        EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
            null_expr, segment.get(), json_strs.size()));
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           null_expr);
        return milvus::test::gen_filter_res(plan.get(),
                                            segment.get(),
                                            json_strs.size(),
                                            MAX_TIMESTAMP,
                                            offsets);
    };

    auto check = [](const ColumnVectorPtr& result,
                    const std::vector<bool>& expected) {
        ASSERT_EQ(result->size(), expected.size());
        TargetBitmapView values(result->GetRawData(), result->size());
        TargetBitmapView validity(result->GetValidRawData(), result->size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_TRUE(validity[i]) << "row " << i;
            EXPECT_EQ(values[i], expected[i]) << "row " << i;
        }
    };

    check(evaluate(proto::plan::NullExpr_NullOp_IsNull),
          {true, false, false, false, false, false});
    check(evaluate(proto::plan::NullExpr_NullOp_IsNotNull),
          {false, true, true, true, true, true});

    exec::OffsetVector offsets = {5, 0, 2, 0, 4};
    check(evaluate(proto::plan::NullExpr_NullOp_IsNull, &offsets),
          {false, true, false, true, false});
    check(evaluate(proto::plan::NullExpr_NullOp_IsNotNull, &offsets),
          {true, false, true, false, true});
}

}  // namespace
