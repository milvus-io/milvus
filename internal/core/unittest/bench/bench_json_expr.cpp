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

#include <benchmark/benchmark.h>
#include <string>
#include <nlohmann/json.hpp>
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "index/JsonInvertedIndex.h"
#include "mmap/Types.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "test_utils/DataGen.h"
#include <segcore/SegmentSealedImpl.h>

using json = nlohmann::json;

class JsonExprBenchmark : public benchmark::Fixture {
 public:
    size_t kTestDataSize;
    milvus::segcore::GeneratedData kTestData;
    std::shared_ptr<milvus::expr::UnaryRangeFilterExpr> kTestExpr;

    void
    SetUp(const ::benchmark::State& state) override {
        kTestDataSize = state.range(0);
        kTestData = milvus::segcore::DataGen(
            []() {
                auto schema = std::make_shared<milvus::Schema>();
                schema->AddField(milvus::FieldName("json_field"),
                                 milvus::FieldId(100),
                                 milvus::DataType::JSON,
                                 false);
                return schema;
            }(),
            kTestDataSize);
        kTestExpr = CreateTestExpr();
    }

 private:
    std::shared_ptr<milvus::expr::UnaryRangeFilterExpr>
    CreateTestExpr() const {
        auto field_id = kTestData.raw_->fields_data().at(0).field_id();
        auto value = milvus::proto::plan::GenericValue();
        value.set_int64_val(1);
        return std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(milvus::FieldId(field_id),
                                     milvus::DataType::JSON,
                                     {"int"},
                                     false),
            milvus::proto::plan::OpType::Equal,
            value,
            std::vector<milvus::proto::plan::GenericValue>());
    }
};

BENCHMARK_DEFINE_F(JsonExprBenchmark, BM_JsonBruteForce)
(benchmark::State& state) {
    auto schema = kTestData.schema_;
    auto segment = milvus::segcore::CreateSealedSegment(schema);

    auto field_data = kTestData.raw_->fields_data().at(0);
    auto field_id = field_data.field_id();

    auto info = milvus::FieldDataInfo(field_id, kTestDataSize, "/tmp/a");
    info.channel->push(milvus::segcore::CreateFieldDataFromDataArray(
        kTestDataSize,
        &field_data,
        schema->get_fields().at(milvus::FieldId(field_id))));
    info.channel->close();
    segment->LoadFieldData(milvus::FieldId(field_id), info);

    auto plan = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, kTestExpr);
    for (auto _ : state) {
        auto final = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), kTestDataSize, milvus::MAX_TIMESTAMP);
    }
}

BENCHMARK_DEFINE_F(JsonExprBenchmark, BM_JsonValueIndex)
(benchmark::State& state) {
    auto schema = kTestData.schema_;
    auto segment = milvus::segcore::CreateSealedSegment(schema);

    auto field_data = kTestData.raw_->fields_data().at(0);
    auto field_id = field_data.field_id();
    auto file_manager_ctx = milvus::storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::JSON);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(100);
    auto inv_index = milvus::index::IndexFactory::GetInstance().CreateJsonIndex(
        milvus::index::INVERTED_INDEX_TYPE,
        milvus::DataType::INT64,
        "/int",
        file_manager_ctx);
    using json_index_type = milvus::index::JsonInvertedIndex<milvus::Json>;
    auto json_index = std::unique_ptr<json_index_type>(
        static_cast<json_index_type*>(inv_index.release()));
    auto json_field = milvus::segcore::CreateFieldDataFromDataArray(
        kTestDataSize,
        &field_data,
        schema->get_fields().at(milvus::FieldId(field_id)));
    json_index->BuildWithFieldData({json_field});
    json_index->finish();
    json_index->create_reader();
    milvus::segcore::LoadIndexInfo load_index_info;
    load_index_info.field_id = field_id;
    load_index_info.field_type = milvus::DataType::INT64;
    load_index_info.index = std::move(json_index);
    load_index_info.index_params = {{JSON_PATH, "/int"}};
    segment->LoadIndex(load_index_info);

    auto json_field_data_info =
        milvus::FieldDataInfo(field_id, kTestDataSize, {json_field});
    segment->LoadFieldData(milvus::FieldId(field_id), json_field_data_info);
    auto plan = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, kTestExpr);
    for (auto _ : state) {
        auto final = milvus::query::ExecuteQueryExpr(
            plan, segment.get(), kTestDataSize, milvus::MAX_TIMESTAMP);
    }
}

BENCHMARK_REGISTER_F(JsonExprBenchmark, BM_JsonBruteForce)
    ->Arg(1000)
    ->Arg(1000000);

BENCHMARK_REGISTER_F(JsonExprBenchmark, BM_JsonValueIndex)
    ->Arg(1000)
    ->Arg(1000000);
