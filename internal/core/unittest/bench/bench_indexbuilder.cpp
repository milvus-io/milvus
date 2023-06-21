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
#include <tuple>
#include <map>
#include <google/protobuf/text_format.h>
#include <knowhere/comp/index_param.h>

#include "pb/index_cgo_msg.pb.h"
#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "common/Consts.h"

constexpr int64_t NB = 1000000;

namespace indexcgo = milvus::proto::indexcgo;

auto index_type_collections = [] {
    static std::map<int, knowhere::IndexType> collections{
        {0, knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
    };
    return collections;
}();

auto metric_type_collections = [] {
    static std::map<int, knowhere::MetricType> collections{
        {0, knowhere::metric::L2},
    };
    return collections;
}();

static void
IndexBuilder_build(benchmark::State& state) {
    auto index_type = index_type_collections.at(state.range(0));
    auto metric_type = metric_type_collections.at(state.range(0));

    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    std::tie(type_params, index_params) =
        generate_params(index_type, metric_type);

    milvus::Config config;
    for (auto i = 0; i < type_params.params_size(); ++i) {
        const auto& param = type_params.params(i);
        config[param.key()] = param.value();
    }

    for (auto i = 0; i < index_params.params_size(); ++i) {
        const auto& param = index_params.params(i);
        config[param.key()] = param.value();
    }

    auto is_binary = state.range(2);
    auto dataset = GenDataset(NB, metric_type, is_binary);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(START_USER_FIELDID));
    auto xb_dataset = knowhere::GenDataSet(NB, DIM, xb_data.data());

    for (auto _ : state) {
        auto index = std::make_unique<milvus::indexbuilder::VecIndexCreator>(
            milvus::DataType::VECTOR_FLOAT, config, nullptr);
        index->Build(xb_dataset);
    }
}

static void
IndexBuilder_build_and_codec(benchmark::State& state) {
    auto index_type = index_type_collections.at(state.range(0));
    auto metric_type = metric_type_collections.at(state.range(0));

    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    std::tie(type_params, index_params) =
        generate_params(index_type, metric_type);

    milvus::Config config;
    for (auto i = 0; i < type_params.params_size(); ++i) {
        const auto& param = type_params.params(i);
        config[param.key()] = param.value();
    }

    for (auto i = 0; i < index_params.params_size(); ++i) {
        const auto& param = index_params.params(i);
        config[param.key()] = param.value();
    }

    auto is_binary = state.range(2);
    auto dataset = GenDataset(NB, metric_type, is_binary);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataSet(NB, DIM, xb_data.data());

    for (auto _ : state) {
        auto index = std::make_unique<milvus::indexbuilder::VecIndexCreator>(
            milvus::DataType::VECTOR_FLOAT, config, nullptr);

        index->Build(xb_dataset);
        index->Serialize();
    }
}

// IVF_FLAT, L2, VectorFloat
BENCHMARK(IndexBuilder_build)->Args({0, 0, false});

// IVF_FLAT, L2, VectorFloat
BENCHMARK(IndexBuilder_build_and_codec)->Args({0, 0, false});
