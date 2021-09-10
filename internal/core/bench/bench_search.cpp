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
    schema->AddDebugField("fakevec", DataType::VECTOR_FLOAT, dim, MetricType::METRIC_L2);
    return schema;
}();

const auto plan = [] {
    std::string dsl = R"({
        "bool": {
            "must": [
            {
                "vector": {
                    "fakevec": {
                        "metric_type": "L2",
                        "params": {
                            "nprobe": 10
                        },
                        "query": "$0",
                        "topk": 5
                    }
                }
            }
            ]
        }
    })";
    auto plan = CreatePlan(*schema, dsl);
    return plan;
}();
auto ph_group = [] {
    auto num_queries = 10;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024);
    auto ph_group = ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    return ph_group;
}();

static void
Search_SmallIndex(benchmark::State& state) {
    // schema->AddDebugField("age", DataType::FLOAT);

    static int64_t N = 1024 * 32;
    const auto dataset_ = [] {
        auto dataset_ = DataGen(schema, N);
        return dataset_;
    }();

    auto is_small_index = state.range(0);
    auto chunk_size = state.range(1) * 1024;
    auto segconf = SegcoreConfig::default_config();
    segconf.set_size_per_chunk(chunk_size);
    auto segment = CreateGrowingSegment(schema, segconf);
    if (!is_small_index) {
        segment->disable_small_index();
    }
    segment->PreInsert(N);
    ColumnBasedRawData raw_data;
    raw_data.columns_ = dataset_.cols_;
    raw_data.count = N;
    segment->Insert(0, N, dataset_.row_ids_.data(), dataset_.timestamps_.data(), raw_data);

    Timestamp time = 10000000;

    for (auto _ : state) {
        auto qr = segment->Search(plan.get(), *ph_group, time);
    }
}

BENCHMARK(Search_SmallIndex)->MinTime(5)->ArgsProduct({{true, false}, {8, 16, 32}});

static void
Search_Sealed(benchmark::State& state) {
    auto segment = CreateSealedSegment(schema);
    static int64_t N = 1024 * 1024;
    const auto dataset_ = [] {
        auto dataset_ = DataGen(schema, N);
        return dataset_;
    }();
    SealedLoader(dataset_, *segment);
    auto choice = state.range(0);
    if (choice == 0) {
        // Brute Force
    } else if (choice == 1) {
        // ivf
        auto vec = (const float*)dataset_.cols_[0].data();
        auto indexing = GenIndexing(N, dim, vec);
        LoadIndexInfo info;
        info.index = indexing;
        info.field_id = (*schema)[FieldName("fakevec")].get_id().get();
        info.index_params["index_type"] = "IVF";
        info.index_params["index_mode"] = "CPU";
        info.index_params["metric_type"] = MetricTypeToName(MetricType::METRIC_L2);
        segment->LoadIndex(info);
    }
    Timestamp time = 10000000;
    for (auto _ : state) {
        auto qr = segment->Search(plan.get(), *ph_group, time);
    }
}

BENCHMARK(Search_Sealed)->MinTime(5)->Arg(1)->Arg(0);
