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
#include <google/protobuf/text_format.h>

#include "pb/index_cgo_msg.pb.h"
#include "index/knowhere/knowhere/index/vector_index/helpers/IndexParameter.h"
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/IndexWrapper.h"
#include "indexbuilder/index_c.h"
#include "test_utils/DataGen.h"
#include "indexbuilder/utils.h"
#include "test_utils/indexbuilder_test_utils.h"

constexpr int64_t NB = 1000;
namespace indexcgo = milvus::proto::indexcgo;

using Param = std::pair<milvus::knowhere::IndexType, milvus::knowhere::MetricType>;

class IndexWrapperTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        auto param = GetParam();
        index_type = param.first;
        metric_type = param.second;
        std::tie(type_params, index_params) = generate_params(index_type, metric_type);

        std::map<std::string, bool> is_binary_map = {
            {milvus::knowhere::IndexEnum::INDEX_FAISS_IDMAP, false},
            {milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, false},
            {milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, false},
            {milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, false},
            {milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, true},
            {milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, true},
#ifdef MILVUS_SUPPORT_SPTAG
            {milvus::knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, false},
            {milvus::knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, false},
#endif
            {milvus::knowhere::IndexEnum::INDEX_HNSW, false},
            {milvus::knowhere::IndexEnum::INDEX_ANNOY, false},
            {milvus::knowhere::IndexEnum::INDEX_RHNSWFlat, false},
            {milvus::knowhere::IndexEnum::INDEX_RHNSWPQ, false},
            {milvus::knowhere::IndexEnum::INDEX_RHNSWSQ, false},
            {milvus::knowhere::IndexEnum::INDEX_NGTPANNG, false},
            {milvus::knowhere::IndexEnum::INDEX_NGTONNG, false},
            {milvus::knowhere::IndexEnum::INDEX_NSG, false},
        };

        is_binary = is_binary_map[index_type];

        bool ok;
        ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
        assert(ok);
        ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
        assert(ok);

        auto dataset = GenDataset(NB, metric_type, is_binary);
        if (!is_binary) {
            xb_data = dataset.get_col<float>(0);
            xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
            xq_data = dataset.get_col<float>(0);
            xq_dataset = milvus::knowhere::GenDataset(NQ, DIM, xq_data.data());
        } else {
            xb_bin_data = dataset.get_col<uint8_t>(0);
            xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_bin_data.data());
            xq_bin_data = dataset.get_col<uint8_t>(0);
            xq_dataset = milvus::knowhere::GenDataset(NQ, DIM, xq_bin_data.data());
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
    milvus::knowhere::DatasetPtr xq_dataset;
    std::vector<float> xq_data;
    std::vector<uint8_t> xq_bin_data;
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

TEST(IVFFLATNM, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
}

TEST(IVFFLATNM, Query) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
    auto bs = index->Serialize(conf);
    auto bptr = std::make_shared<milvus::knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)xb_data.data(), [&](uint8_t*) {});
    bptr->size = DIM * NB * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    index->Load(bs);
    auto xq_data = dataset.get_col<float>(0);
    auto xq_dataset = milvus::knowhere::GenDataset(NQ, DIM, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);
}

TEST(NSG, Query) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_NSG;
    auto metric_type = milvus::knowhere::Metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    index->BuildAll(xb_dataset, conf);
    auto bs = index->Serialize(conf);
    auto bptr = std::make_shared<milvus::knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)xb_data.data(), [&](uint8_t*) {});
    bptr->size = DIM * NB * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    index->Load(bs);
    auto xq_data = dataset.get_col<float>(0);
    auto xq_dataset = milvus::knowhere::GenDataset(NQ, DIM, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);
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
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

void
print_query_result(const std::unique_ptr<milvus::indexbuilder::IndexWrapper::QueryResult>& result) {
    for (auto i = 0; i < result->nq; i++) {
        printf("result of %dth query:\n", i);
        for (auto j = 0; j < result->topk; j++) {
            auto offset = i * result->topk + j;
            printf("id: %ld, distance: %f\n", result->ids[offset], result->distances[offset]);
        }
    }
}

// test for: https://github.com/milvus-io/milvus/issues/6569
TEST(BinIVFFlat, Build_and_Query) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::TANIMOTO;
    auto conf = generate_conf(index_type, metric_type);
    auto topk = 10;
    conf[milvus::knowhere::meta::TOPK] = topk;
    conf[milvus::knowhere::IndexParams::nlist] = 1;
    auto index = milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto nb = 2;
    auto dim = 128;
    auto nq = 10;
    auto dataset = GenDataset(std::max(nq, nb), metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(0);
    std::vector<milvus::knowhere::IDType> ids(nb, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = milvus::knowhere::GenDataset(nb, dim, xb_data.data());
    index->BuildAll(xb_dataset, conf);
    auto xq_data = dataset.get_col<float>(0);
    auto xq_dataset = milvus::knowhere::GenDataset(nq, dim, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);

    auto hit_ids = result->Get<int64_t*>(milvus::knowhere::meta::IDS);
    auto distances = result->Get<float*>(milvus::knowhere::meta::DISTANCE);

    auto query_res = std::make_unique<milvus::indexbuilder::IndexWrapper::QueryResult>();
    query_res->nq = nq;
    query_res->topk = topk;
    query_res->ids.resize(nq * topk);
    query_res->distances.resize(nq * topk);
    memcpy(query_res->ids.data(), hit_ids, sizeof(int64_t) * nq * topk);
    memcpy(query_res->distances.data(), distances, sizeof(float) * nq * topk);

    print_query_result(query_res);
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
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

TEST(PQWrapper, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto metric_type = milvus::knowhere::Metric::L2;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST(IVFFLATNMWrapper, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::L2;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST(IVFFLATNMWrapper, Codec) {
    int64_t flat_nb = 100000;
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::L2;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(flat_nb, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = milvus::knowhere::GenDataset(flat_nb, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));

    auto binary = index->Serialize();
    auto copy_index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(copy_index->Load(binary->data.data(), binary->data.size()));
    ASSERT_EQ(copy_index->dim(), copy_index->dim());
    auto copy_binary = copy_index->Serialize();
}

TEST(BinFlatWrapper, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = milvus::knowhere::Metric::JACCARD;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(0);
    std::vector<milvus::knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
    // ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
}

TEST(BinIdMapWrapper, Build) {
    auto index_type = milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
    auto metric_type = milvus::knowhere::Metric::JACCARD;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(0);
    std::vector<milvus::knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
    // ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
}

INSTANTIATE_TEST_CASE_P(
    IndexTypeParameters,
    IndexWrapperTest,
    ::testing::Values(
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IDMAP, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, milvus::knowhere::Metric::JACCARD),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, milvus::knowhere::Metric::TANIMOTO),
        std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, milvus::knowhere::Metric::JACCARD),
#ifdef MILVUS_SUPPORT_SPTAG
        std::pair(milvus::knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, milvus::knowhere::Metric::L2),
#endif
        std::pair(milvus::knowhere::IndexEnum::INDEX_HNSW, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_ANNOY, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_RHNSWFlat, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_RHNSWPQ, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_RHNSWSQ, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_NGTPANNG, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_NGTONNG, milvus::knowhere::Metric::L2),
        std::pair(milvus::knowhere::IndexEnum::INDEX_NSG, milvus::knowhere::Metric::L2)));

TEST_P(IndexWrapperTest, Constructor) {
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
}

TEST_P(IndexWrapperTest, Dim) {
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_EQ(index->dim(), DIM);
}

TEST_P(IndexWrapperTest, BuildWithoutIds) {
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST_P(IndexWrapperTest, Codec) {
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));

    auto binary = index->Serialize();
    auto copy_index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(copy_index->Load(binary->data.data(), binary->data.size()));
    ASSERT_EQ(copy_index->dim(), copy_index->dim());
    auto copy_binary = copy_index->Serialize();
    if (!milvus::indexbuilder::is_in_nm_list(index_type)) {
        // binary may be not same due to uncertain internal map order
        ASSERT_EQ(binary->data.size(), copy_binary->data.size());
        ASSERT_EQ(binary->data, copy_binary->data);
    }
}

TEST_P(IndexWrapperTest, Query) {
    auto index_wrapper =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    index_wrapper->BuildWithoutIds(xb_dataset);

    std::unique_ptr<milvus::indexbuilder::IndexWrapper::QueryResult> query_result = index_wrapper->Query(xq_dataset);
    ASSERT_EQ(query_result->topk, K);
    ASSERT_EQ(query_result->nq, NQ);
    ASSERT_EQ(query_result->distances.size(), query_result->topk * query_result->nq);
    ASSERT_EQ(query_result->ids.size(), query_result->topk * query_result->nq);
}
