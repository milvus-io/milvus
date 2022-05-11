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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <map>
#include <tuple>
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/ConfAdapterMgr.h>
#include <knowhere/archive/KnowhereConfig.h>

#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/utils.h"
#include "pb/index_cgo_msg.pb.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"

constexpr int64_t NB = 1000;
namespace indexcgo = milvus::proto::indexcgo;

using Param = std::pair<knowhere::IndexType, knowhere::MetricType>;

class IndexWrapperTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        knowhere::KnowhereConfig::SetStatisticsLevel(3);

        auto param = GetParam();
        index_type = param.first;
        metric_type = param.second;
        std::tie(type_params, index_params) = generate_params(index_type, metric_type);

        std::map<knowhere::MetricType, bool> is_binary_map = {
            {knowhere::IndexEnum::INDEX_FAISS_IDMAP, false},
            {knowhere::IndexEnum::INDEX_FAISS_IVFPQ, false},
            {knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, false},
            {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, false},
            {knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, true},
            {knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, true},
#ifdef MILVUS_SUPPORT_SPTAG
            {knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, false},
            {knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, false},
#endif
            {knowhere::IndexEnum::INDEX_HNSW, false},
            {knowhere::IndexEnum::INDEX_ANNOY, false},
            {knowhere::IndexEnum::INDEX_RHNSWFlat, false},
            {knowhere::IndexEnum::INDEX_RHNSWPQ, false},
            {knowhere::IndexEnum::INDEX_RHNSWSQ, false},
#ifdef MILVUS_SUPPORT_NGT
            {knowhere::IndexEnum::INDEX_NGTPANNG, false},
            {knowhere::IndexEnum::INDEX_NGTONNG, false},
#endif
#ifdef MILVUS_SUPPORT_NSG
            {knowhere::IndexEnum::INDEX_NSG, false},
#endif
        };

        is_binary = is_binary_map[index_type];

        bool ok;
        ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
        assert(ok);
        ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
        assert(ok);

        auto dataset = GenDataset(NB, metric_type, is_binary);
        if (!is_binary) {
            xb_data = dataset.get_col<float>(milvus::FieldId(100));
            xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
            xq_data = dataset.get_col<float>(milvus::FieldId(100));
            xq_dataset = knowhere::GenDataset(NQ, DIM, xq_data.data());
        } else {
            xb_bin_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
            xb_dataset = knowhere::GenDataset(NB, DIM, xb_bin_data.data());
            xq_bin_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
            xq_dataset = knowhere::GenDataset(NQ, DIM, xq_bin_data.data());
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
    knowhere::DatasetPtr xb_dataset;
    std::vector<float> xb_data;
    std::vector<uint8_t> xb_bin_data;
    std::vector<knowhere::IDType> ids;
    knowhere::DatasetPtr xq_dataset;
    std::vector<float> xq_data;
    std::vector<uint8_t> xq_bin_data;
};

TEST(PQ, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto metric_type = knowhere::metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
}

TEST(IVFFLATNM, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = knowhere::metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
}

TEST(IVFFLATNM, Query) {
    knowhere::KnowhereConfig::SetStatisticsLevel(3);

    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = knowhere::metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->Train(xb_dataset, conf));
    ASSERT_NO_THROW(index->AddWithoutIds(xb_dataset, conf));
    auto bs = index->Serialize(conf);
    auto bptr = std::make_shared<knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)xb_data.data(), [&](uint8_t*) {});
    bptr->size = DIM * NB * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    index->Load(bs);
    auto xq_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xq_dataset = knowhere::GenDataset(NQ, DIM, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);

    ASSERT_GT(index->Size(), 0);

    auto stats = index->GetStatistics();
    ASSERT_TRUE(stats != nullptr);
    index->ClearStatistics();
}

#ifdef MILVUS_SUPPORT_NSG
TEST(NSG, Query) {
    auto index_type = knowhere::IndexEnum::INDEX_NSG;
    auto metric_type = knowhere::metric::L2;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(0);
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    index->BuildAll(xb_dataset, conf);
    auto bs = index->Serialize(conf);
    auto bptr = std::make_shared<knowhere::Binary>();
    bptr->data = std::shared_ptr<uint8_t[]>((uint8_t*)xb_data.data(), [&](uint8_t*) {});
    bptr->size = DIM * NB * sizeof(float);
    bs.Append(RAW_DATA, bptr);
    index->Load(bs);
    auto xq_data = dataset.get_col<float>(0);
    auto xq_dataset = knowhere::GenDataset(NQ, DIM, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);
}
#endif

TEST(BINFLAT, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = knowhere::metric::JACCARD;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
    std::vector<knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

void
print_query_result(const std::unique_ptr<milvus::indexbuilder::VecIndexCreator::QueryResult>& result) {
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
    knowhere::KnowhereConfig::SetStatisticsLevel(2);

    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = knowhere::metric::TANIMOTO;
    auto conf = generate_conf(index_type, metric_type);
    auto topk = 10;
    knowhere::SetMetaTopk(conf, topk);
    knowhere::SetIndexParamNlist(conf, 1);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto nb = 2;
    auto dim = 128;
    auto nq = 10;
    auto dataset = GenDataset(std::max(nq, nb), metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
    std::vector<knowhere::IDType> ids(nb, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = knowhere::GenDataset(nb, dim, xb_data.data());
    index->BuildAll(xb_dataset, conf);
    auto xq_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xq_dataset = knowhere::GenDataset(nq, dim, xq_data.data());
    auto result = index->Query(xq_dataset, conf, nullptr);

    auto hit_ids = knowhere::GetDatasetIDs(result);
    auto distances = knowhere::GetDatasetDistance(result);

    auto query_res = std::make_unique<milvus::indexbuilder::VecIndexCreator::QueryResult>();
    query_res->nq = nq;
    query_res->topk = topk;
    query_res->ids.resize(nq * topk);
    query_res->distances.resize(nq * topk);
    memcpy(query_res->ids.data(), hit_ids, sizeof(int64_t) * nq * topk);
    memcpy(query_res->distances.data(), distances, sizeof(float) * nq * topk);

    print_query_result(query_res);

    ASSERT_GT(index->Size(), 0);

    auto stats = index->GetStatistics();
    ASSERT_TRUE(stats != nullptr);
    index->ClearStatistics();
}

TEST(BINIDMAP, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
    auto metric_type = knowhere::metric::JACCARD;
    auto conf = generate_conf(index_type, metric_type);
    auto index = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_type);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
    std::vector<knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildAll(xb_dataset, conf));
}

TEST(PQWrapper, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto metric_type = knowhere::metric::L2;
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
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST(IVFFLATNMWrapper, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = knowhere::metric::L2;
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
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST(IVFFLATNMWrapper, Codec) {
    int64_t flat_nb = 100000;
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT;
    auto metric_type = knowhere::metric::L2;
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
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto xb_dataset = knowhere::GenDataset(flat_nb, DIM, xb_data.data());
    auto index_wrapper =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index_wrapper->BuildWithoutIds(xb_dataset));

    auto binary_set = index_wrapper->Serialize();
    auto copy_index_wrapper =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_NO_THROW(copy_index_wrapper->Load(binary_set));
    ASSERT_EQ(copy_index_wrapper->dim(), copy_index_wrapper->dim());

    auto copy_binary_set = copy_index_wrapper->Serialize();
    ASSERT_EQ(binary_set.binary_map_.size(), copy_binary_set.binary_map_.size());

    for (const auto& [k, v] : binary_set.binary_map_) {
        ASSERT_TRUE(copy_binary_set.Contains(k));
    }
}

TEST(BinFlatWrapper, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = knowhere::metric::JACCARD;
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
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
    std::vector<knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
    // ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
}

TEST(BinIdMapWrapper, Build) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP;
    auto metric_type = knowhere::metric::JACCARD;
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
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
    std::vector<knowhere::IDType> ids(NB, 0);
    std::iota(ids.begin(), ids.end(), 0);
    auto xb_dataset = knowhere::GenDataset(NB, DIM, xb_data.data());
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
    // ASSERT_NO_THROW(index->BuildWithIds(xb_dataset));
}

INSTANTIATE_TEST_CASE_P(
    IndexTypeParameters,
    IndexWrapperTest,
    ::testing::Values(std::pair(knowhere::IndexEnum::INDEX_FAISS_IDMAP, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFPQ, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, knowhere::metric::JACCARD),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT, knowhere::metric::TANIMOTO),
                      std::pair(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP, knowhere::metric::JACCARD),
#ifdef MILVUS_SUPPORT_SPTAG
                      std::pair(knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT, knowhere::metric::L2),
#endif
                      std::pair(knowhere::IndexEnum::INDEX_HNSW, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_ANNOY, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_RHNSWFlat, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_RHNSWPQ, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_RHNSWSQ, knowhere::metric::L2)
#ifdef MILVUS_SUPPORT_NGT
                          std::pair(knowhere::IndexEnum::INDEX_NGTPANNG, knowhere::metric::L2),
                      std::pair(knowhere::IndexEnum::INDEX_NGTONNG, knowhere::metric::L2),
#endif
#ifdef MILVUS_SUPPORT_NSG
                      std::pair(knowhere::IndexEnum::INDEX_NSG, knowhere::metric::L2)
#endif
                          ));

TEST_P(IndexWrapperTest, Constructor) {
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
}

TEST_P(IndexWrapperTest, Dim) {
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_EQ(index->dim(), DIM);
}

TEST_P(IndexWrapperTest, BuildWithoutIds) {
    auto index =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
}

TEST_P(IndexWrapperTest, Codec) {
    auto index_wrapper =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_NO_THROW(index_wrapper->BuildWithoutIds(xb_dataset));

    auto binary_set = index_wrapper->Serialize();
    auto copy_index_wrapper =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_NO_THROW(copy_index_wrapper->Load(binary_set));
    ASSERT_EQ(copy_index_wrapper->dim(), copy_index_wrapper->dim());

    auto copy_binary_set = copy_index_wrapper->Serialize();
    ASSERT_EQ(binary_set.binary_map_.size(), copy_binary_set.binary_map_.size());

    for (const auto& [k, v] : binary_set.binary_map_) {
        ASSERT_TRUE(copy_binary_set.Contains(k));
    }
}

TEST_P(IndexWrapperTest, Query) {
    auto index_wrapper =
        std::make_unique<milvus::indexbuilder::VecIndexCreator>(type_params_str.c_str(), index_params_str.c_str());

    index_wrapper->BuildWithoutIds(xb_dataset);

    std::unique_ptr<milvus::indexbuilder::VecIndexCreator::QueryResult> query_result = index_wrapper->Query(xq_dataset);
    ASSERT_EQ(query_result->topk, K);
    ASSERT_EQ(query_result->nq, NQ);
    ASSERT_EQ(query_result->distances.size(), query_result->topk * query_result->nq);
    ASSERT_EQ(query_result->ids.size(), query_result->topk * query_result->nq);
}
