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

#include <tuple>
#include <random>
#include <gtest/gtest.h>

#include "pb/index_cgo_msg.pb.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h"
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/IndexWrapper.h"
#include "indexbuilder/index_c.h"
#include "test_utils/DataGen.h"
#include "faiss/MetricType.h"
#include "index/knowhere/knowhere/index/vector_index/VecIndexFactory.h"

namespace indexcgo = milvus::proto::indexcgo;

constexpr int64_t DIM = 4;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 4;
constexpr auto METRIC_TYPE = milvus::knowhere::Metric::L2;

namespace {
auto
generate_conf(const milvus::knowhere::IndexType& type) {
    if (type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 100},
            // {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    }
    return milvus::knowhere::Config();
}

auto
generate_params() {
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    auto configs = generate_conf(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ);
    for (auto& [key, value] : configs.items()) {
        auto param = index_params.add_params();
        auto value_str = value.is_string() ? value.get<std::string>() : value.dump();
        param->set_key(key);
        param->set_value(value_str);
    }

    return std::make_tuple(type_params, index_params);
}
}  // namespace

TEST(IndexWrapperTest, Constructor) {
    auto [type_params, index_params] = generate_params();
    std::string type_params_str, index_params_str;
    bool ok;

    ok = type_params.SerializeToString(&type_params_str);
    assert(ok);
    ok = index_params.SerializeToString(&index_params_str);
    assert(ok);

    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
}

TEST(IndexWrapperTest, Dim) {
    auto [type_params, index_params] = generate_params();
    std::string type_params_str, index_params_str;
    bool ok;

    ok = type_params.SerializeToString(&type_params_str);
    assert(ok);
    ok = index_params.SerializeToString(&index_params_str);
    assert(ok);

    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    
    ASSERT_EQ(index->dim(), DIM);
}

TEST(IndexWrapperTest, BuildWithoutIds) {
    auto [type_params, index_params] = generate_params();
    std::string type_params_str, index_params_str;
    bool ok;

    ok = type_params.SerializeToString(&type_params_str);
    assert(ok);
    ok = index_params.SerializeToString(&index_params_str);
    assert(ok);

    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    auto schema = std::make_shared<milvus::Schema>();
    schema->AddField("fakevec", milvus::engine::DataType::VECTOR_FLOAT, DIM, faiss::MetricType::METRIC_L2);
    auto dataset = milvus::segcore::DataGen(schema, NB);
    auto xb_data = dataset.get_col<float>(0);

    index->BuildWithoutIds(milvus::knowhere::GenDataset(NB, DIM, xb_data.data()));
}

TEST(IndexWrapperTest, Load) {
    auto type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(type);
    auto conf = generate_conf(type);
    auto schema = std::make_shared<milvus::Schema>();
    schema->AddField("fakevec", milvus::engine::DataType::VECTOR_FLOAT, DIM, faiss::MetricType::METRIC_L2);
    auto dataset = milvus::segcore::DataGen(schema, NB);
    auto xb_data = dataset.get_col<float>(0);
    auto ds = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    index->Train(ds, conf);
    index->AddWithoutIds(ds, conf);
    // std::vector<int64_t> ids(NB);
    // std::iota(ids.begin(), ids.end(), 0); // range(0, NB)
    // auto ds = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_data.data(), ids.data());
    // index->Train(ds, conf);
    // index->Add(ds, conf);
    auto binary_set = index->Serialize(conf);
    auto copy_index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(type);
    copy_index->Load(binary_set);
}

TEST(IndexWrapperTest, Codec) {
    auto [type_params, index_params] = generate_params();
    std::string type_params_str, index_params_str;
    bool ok;

    ok = type_params.SerializeToString(&type_params_str);
    assert(ok);
    ok = index_params.SerializeToString(&index_params_str);
    assert(ok);

    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    auto schema = std::make_shared<milvus::Schema>();
    schema->AddField("fakevec", milvus::engine::DataType::VECTOR_FLOAT, DIM, faiss::MetricType::METRIC_L2);
    auto dataset = milvus::segcore::DataGen(schema, NB);
    auto xb_data = dataset.get_col<float>(0);

    index->BuildWithoutIds(milvus::knowhere::GenDataset(NB, DIM, xb_data.data()));

    auto binary = index->Serialize();
    auto copy_index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    copy_index->Load(binary.data, binary.size);
    ASSERT_EQ(copy_index->dim(), copy_index->dim());
    auto copy_binary = copy_index->Serialize();
    ASSERT_EQ(binary.size, copy_binary.size);
    ASSERT_EQ(strcmp(binary.data, copy_binary.data), 0);
}
