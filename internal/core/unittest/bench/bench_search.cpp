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

#include <cstdint>
#include <benchmark/benchmark.h>
#include <string>
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

static int dim = 768;

const auto schema = []() {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    return schema;
}();

const auto plan = [] {
    const char* raw_plan = R"(vector_anns: <
                                field_id: 100
                                query_info: <
                                  topk: 5
                                  round_decimal: -1
                                  metric_type: "L2"
                                  search_params: "{\"nprobe\": 10}"
                                >
                                placeholder_tag: "$0"
        >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(*schema, plan_str.data(), plan_str.size());
    return plan;
}();
auto ph_group = [] {
    auto num_queries = 10;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    return ph_group;
}();

static void
Search_GrowingIndex(benchmark::State& state) {
    // schema->AddDebugField("age", DataType::FLOAT);

    static int64_t N = 1024 * 32;
    const auto dataset_ = [] {
        auto dataset_ = DataGen(schema, N);
        return dataset_;
    }();

    auto chunk_rows = state.range(1) * 1024;
    auto segconf = SegcoreConfig::default_config();
    segconf.set_chunk_rows(chunk_rows);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"}, {"metric_type", "L2"}, {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(schema->get_field_id(FieldName("fakevec")),
                                  std::move(index_params),
                                  std::move(type_params));
    segconf.set_enable_growing_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {
        {schema->get_field_id(FieldName("fakevec")), fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(226985, std::move(filedMap));

    auto segment = CreateGrowingSegment(schema, metaPtr, -1, segconf);

    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset_.row_ids_.data(),
                    dataset_.timestamps_.data(),
                    dataset_.raw_);

    Timestamp time = 10000000;

    for (auto _ : state) {
        auto qr = segment->Search(plan.get(), ph_group.get(), time);
    }
}

BENCHMARK(Search_GrowingIndex)
    ->MinTime(5)
    ->ArgsProduct({{true, false}, {8, 16, 32}});

static void
Search_Sealed(benchmark::State& state) {
    auto segment = CreateSealedSegment(schema);
    static int64_t N = 1024 * 1024;
    const auto dataset_ = [] {
        auto dataset_ = DataGen(schema, N);
        return dataset_;
    }();
    SealedLoadFieldData(dataset_, *segment);
    auto choice = state.range(0);
    if (choice == 0) {
        // Brute Force
    } else if (choice == 1) {
        // ivf
        auto vec = dataset_.get_col<float>(milvus::FieldId(100));
        auto indexing = GenVecIndexing(N, dim, vec.data());
        segcore::LoadIndexInfo info;
        info.index = std::move(indexing);
        info.field_id = (*schema)[FieldName("fakevec")].get_id().get();
        info.index_params["index_type"] = "IVF";
        info.index_params["metric_type"] = knowhere::metric::L2;
        segment->DropFieldData(milvus::FieldId(100));
        segment->LoadIndex(info);
    }
    Timestamp time = 10000000;
    for (auto _ : state) {
        auto qr = segment->Search(plan.get(), ph_group.get(), time);
    }
}

BENCHMARK(Search_Sealed)->MinTime(5)->Arg(1)->Arg(0);
