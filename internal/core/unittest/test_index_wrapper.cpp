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
#include <map>
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

constexpr int64_t DIM = 8;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 4;
constexpr auto METRIC_TYPE = milvus::knowhere::Metric::L2;

namespace {
auto
generate_conf(const milvus::knowhere::IndexType& index_type, const milvus::knowhere::MetricType& metric_type) {
    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 100},
            // {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 100},
            // {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
        };
    }
    return milvus::knowhere::Config();
}

auto
generate_params(const milvus::knowhere::IndexType& index_type, const milvus::knowhere::MetricType& metric_type) {
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;

    auto configs = generate_conf(index_type, metric_type);
    for (auto& [key, value] : configs.items()) {
        auto param = index_params.add_params();
        auto value_str = value.is_string() ? value.get<std::string>() : value.dump();
        param->set_key(key);
        param->set_value(value_str);
    }

    auto param = index_params.add_params();
    param->set_key("index_type");
    param->set_value(std::string(index_type));

    return std::make_tuple(type_params, index_params);
}

auto
GenDataset(int64_t N, milvus::knowhere::MetricType metric_type, bool is_binary, int64_t dim = DIM) {
    auto schema = std::make_shared<milvus::Schema>();
    auto faiss_metric_type = milvus::knowhere::GetMetricType(metric_type);
    if (!is_binary) {
        schema->AddField("fakevec", milvus::engine::DataType::VECTOR_FLOAT, dim, faiss_metric_type);
        return milvus::segcore::DataGen(schema, N);
    } else {
        schema->AddField("fakebinvec", milvus::engine::DataType::VECTOR_BINARY, dim, faiss_metric_type);
        return milvus::segcore::DataGen(schema, N);
    }
}
}  // namespace

using Param = std::pair<milvus::knowhere::IndexType, milvus::knowhere::MetricType>;

class IndexWrapperTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        auto param = GetParam();
        index_type = param.first;
        metric_type = param.second;
        std::tie(type_params, index_params) = generate_params(index_type, metric_type);

        std::map<std::string, bool> is_binary_map = {{milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, false},
                                                     {milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, true}};

        is_binary = is_binary_map[index_type];

        bool ok;
        ok = type_params.SerializeToString(&type_params_str);
        assert(ok);
        ok = index_params.SerializeToString(&index_params_str);
        assert(ok);

        auto dataset = GenDataset(NB, metric_type, is_binary);
        if (!is_binary) {
            xb_data = dataset.get_col<float>(0);
            xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
        } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
            xb_bin_data = dataset.get_col<uint8_t>(0);
            ids.resize(NB);
            std::iota(ids.begin(), ids.end(), 0);
            xb_dataset = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_bin_data.data(), ids.data());
        } else {
            xb_bin_data = dataset.get_col<uint8_t>(0);
            xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_bin_data.data());
        }
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type, metric_type;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::string type_params_str, index_params_str;
    bool is_binary;
    milvus::knowhere::DatasetPtr xb_dataset;
    std::vector<float> xb_data;
    std::vector<uint8_t> xb_bin_data;
    std::vector<milvus::knowhere::IDType> ids;
};

TEST(PQ, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto metric_type = milvus::knowhere::Metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
}

TEST(BINFLAT, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::JACCARD;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(0);
    std::vector<milvus::knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_data.data(), ids.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

TEST(BINIDMAP, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
    auto metric_type = milvus::knowhere::Metric::JACCARD;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(0);
    std::vector<milvus::knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_data.data(), ids.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

// TEST(PQWrapper, Build) {
//    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
//    auto metric_type = milvus::knowhere::Metric::L2;
//    indexcgo::TypeParams type_params;
//    indexcgo::IndexParams index_params;
//    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
//    std::string type_params_str, index_params_str;
//    bool ok;
//    ok = type_params.SerializeToString(&type_params_str);
//    assert(ok);
//    ok = index_params.SerializeToString(&index_params_str);
//    assert(ok);
//    auto dataset = GenDataset(NB, metric_type, false);
//    auto xb_data = dataset.get_col<float>(0);
//    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
//}

// TEST(PQCGO, Params) {
//    std::vector<char> type_params;
//    std::vector<char> index_params{10,  10,  10,  5,   110, 98,  105, 116, 115, 18,  1,   56, 10, 17,  10, 11, 109,
//                                   101, 116, 114, 105, 99,  95,  116, 121, 112, 101, 18,  2,  76, 50,  10, 20, 10,
//                                   10,  105, 110, 100, 101, 120, 95,  116, 121, 112, 101, 18, 6,  73,  86, 70, 95,
//                                   80,  81,  10,  8,   10,  3,   100, 105, 109, 18,  1,   56, 10, 12,  10, 5,  110,
//                                   108, 105, 115, 116, 18,  3,   49,  48,  48,  10,  6,   10, 1,  109, 18, 1,  52};
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params.data(), type_params.size(),
//                                                                      index_params.data(), index_params.size());
//
//    auto dim = index->dim();
//    auto dataset = GenDataset(NB, METRIC_TYPE, false, dim);
//    auto xb_data = dataset.get_col<float>(0);
//    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
//    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
//}

// TEST(PQCGOWrapper, Params) {
//    std::vector<char> type_params;
//    std::vector<char> index_params{10,  10,  10,  5,   110, 98,  105, 116, 115, 18,  1,   56, 10, 17,  10, 11, 109,
//                                   101, 116, 114, 105, 99,  95,  116, 121, 112, 101, 18,  2,  76, 50,  10, 20, 10,
//                                   10,  105, 110, 100, 101, 120, 95,  116, 121, 112, 101, 18, 6,  73,  86, 70, 95,
//                                   80,  81,  10,  8,   10,  3,   100, 105, 109, 18,  1,   56, 10, 12,  10, 5,  110,
//                                   108, 105, 115, 116, 18,  3,   49,  48,  48,  10,  6,   10, 1,  109, 18, 1,  52};
//    auto index = CreateIndex(type_params.data(), type_params.size(), index_params.data(), index_params.size());
//    DeleteIndex(index);
//}

// TEST(BinFlatWrapper, Build) {
//    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
//    auto metric_type = milvus::knowhere::Metric::JACCARD;
//    indexcgo::TypeParams type_params;
//    indexcgo::IndexParams index_params;
//    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
//    std::string type_params_str, index_params_str;
//    bool ok;
//    ok = type_params.SerializeToString(&type_params_str);
//    assert(ok);
//    ok = index_params.SerializeToString(&index_params_str);
//    assert(ok);
//    auto dataset = GenDataset(NB, metric_type, true);
//    auto xb_data = dataset.get_col<uint8_t>(0);
//    std::vector<milvus::knowhere::IDType> ids(NB, 0);
//    std::iota(ids.begin(), ids.end(), 0);
//    auto xb_dataset = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_data.data(), ids.data());
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//    ASSERT_ANY_THROW(index->BuildWithoutIds(xb_dataset));
//    ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
//}

// TEST(BinIdMapWrapper, Build) {
//    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
//    auto metric_type = milvus::knowhere::Metric::JACCARD;
//    indexcgo::TypeParams type_params;
//    indexcgo::IndexParams index_params;
//    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
//    std::string type_params_str, index_params_str;
//    bool ok;
//    ok = type_params.SerializeToString(&type_params_str);
//    assert(ok);
//    ok = index_params.SerializeToString(&index_params_str);
//    assert(ok);
//    auto dataset = GenDataset(NB, metric_type, true);
//    auto xb_data = dataset.get_col<uint8_t>(0);
//    std::vector<milvus::knowhere::IDType> ids(NB, 0);
//    std::iota(ids.begin(), ids.end(), 0);
//    auto xb_dataset = milvus::knowhere::GenDatasetWithIds(NB, DIM, xb_data.data(), ids.data());
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
//    ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
//}

INSTANTIATE_TEST_CASE_P(IndexTypeParameters,
                        IndexWrapperTest,
                        ::testing::Values(std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                                                    milvus::knowhere::Metric::L2),
                                          std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                                    milvus::knowhere::Metric::JACCARD),
                                          std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
                                                    milvus::knowhere::Metric::JACCARD)));

// TEST_P(IndexWrapperTest, Constructor) {
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//}

// TEST_P(IndexWrapperTest, Dim) {
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//
//    ASSERT_EQ(index->dim(), DIM);
//}

// TEST_P(IndexWrapperTest, BuildWithoutIds) {
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//
//    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
//        ASSERT_ANY_THROW(index->BuildWithoutIds(xb_dataset));
//    } else {
//        ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
//    }
//}

// TEST_P(IndexWrapperTest, Codec) {
//    auto index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//
//    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT) {
//        ASSERT_ANY_THROW(index->BuildWithoutIds(xb_dataset));
//        ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
//    } else {
//        ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
//    }
//
//    auto binary = index->Serialize();
//    auto copy_index = std::make_unique<milvus::indexbuilder::IndexWrapper>(
//        type_params_str.c_str(), type_params_str.size(), index_params_str.c_str(), index_params_str.size());
//    ASSERT_NO_THROW(copy_index->Load(binary.data, binary.size));
//    ASSERT_EQ(copy_index->dim(), copy_index->dim());
//    auto copy_binary = copy_index->Serialize();
//    ASSERT_EQ(binary.size, copy_binary.size);
//    ASSERT_EQ(strcmp(binary.data, copy_binary.data), 0);
//}
