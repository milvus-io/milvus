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
#include "faiss/MetricType.h"
#include "index/knowhere/knowhere/index/vector_index/VecIndexFactory.h"
#include "indexbuilder/utils.h"

namespace indexcgo = milvus::proto::indexcgo;

constexpr int64_t DIM = 8;
constexpr int64_t NB = 10000;
constexpr int64_t NQ = 10;
constexpr int64_t K = 4;
constexpr auto METRIC_TYPE = milvus::knowhere::Metric::L2;
#ifdef MILVUS_GPU_VERSION
int DEVICEID = 0;
#endif

namespace {
auto
generate_conf(const milvus::knowhere::IndexType& index_type, const milvus::knowhere::MetricType& metric_type) {
    if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IDMAP) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ) {
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
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 100},
            // {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
#ifdef MILVUS_GPU_VERSION
            {milvus::knowhere::meta::DEVICEID, DEVICEID},
#endif
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, K},
            {milvus::knowhere::IndexParams::nlist, 100},
            // {milvus::knowhere::IndexParams::nprobe, 4},
            {milvus::knowhere::IndexParams::nbits, 8},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
#ifdef MILVUS_GPU_VERSION
            {milvus::knowhere::meta::DEVICEID, DEVICEID},
#endif
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
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NSG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            {milvus::knowhere::IndexParams::nlist, 163},
            {milvus::knowhere::IndexParams::nprobe, 8},
            {milvus::knowhere::IndexParams::knng, 20},
            {milvus::knowhere::IndexParams::search_length, 40},
            {milvus::knowhere::IndexParams::out_degree, 30},
            {milvus::knowhere::IndexParams::candidate, 100},
            {milvus::knowhere::Metric::TYPE, metric_type},
        };
#ifdef MILVUS_SUPPORT_SPTAG
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_SPTAG_KDT_RNT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_SPTAG_BKT_RNT) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
#endif
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_HNSW) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_ANNOY) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::n_trees, 4},
            {milvus::knowhere::IndexParams::search_k, 100},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWFlat) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWPQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
            {milvus::knowhere::IndexParams::PQM, 8},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_RHNSWSQ) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::IndexParams::M, 16},
            {milvus::knowhere::IndexParams::efConstruction, 200},
            {milvus::knowhere::IndexParams::ef, 200},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NGTPANNG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::IndexParams::edge_size, 10},
            {milvus::knowhere::IndexParams::epsilon, 0.1},
            {milvus::knowhere::IndexParams::max_search_edges, 50},
            {milvus::knowhere::IndexParams::forcedly_pruned_edge_size, 60},
            {milvus::knowhere::IndexParams::selectively_pruned_edge_size, 30},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
        };
    } else if (index_type == milvus::knowhere::IndexEnum::INDEX_NGTONNG) {
        return milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, DIM},
            // {milvus::knowhere::meta::TOPK, 10},
            {milvus::knowhere::Metric::TYPE, metric_type},
            {milvus::knowhere::IndexParams::edge_size, 20},
            {milvus::knowhere::IndexParams::epsilon, 0.1},
            {milvus::knowhere::IndexParams::max_search_edges, 50},
            {milvus::knowhere::IndexParams::outgoing_edge_size, 5},
            {milvus::knowhere::IndexParams::incoming_edge_size, 40},
            {milvus::knowhere::INDEX_FILE_SLICE_SIZE_IN_MEGABYTE, 4},
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
GenDataset(int64_t N, const milvus::knowhere::MetricType& metric_type, bool is_binary, int64_t dim = DIM) {
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
        ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
        assert(ok);
        ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
        assert(ok);

        auto dataset = GenDataset(NB, metric_type, is_binary);
        if (!is_binary) {
            xb_data = dataset.get_col<float>(0);
            xb_dataset = milvus::knowhere::GenDataset(NB, DIM, xb_data.data());
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

    auto binary = index->Serialize();
    auto copy_index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(copy_index->Load(binary.data, binary.size));
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
    ::testing::Values(std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IDMAP, milvus::knowhere::Metric::L2),
                      std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFPQ, milvus::knowhere::Metric::L2),
                      std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, milvus::knowhere::Metric::L2),
                      std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, milvus::knowhere::Metric::L2),
                      std::pair(milvus::knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                                milvus::knowhere::Metric::JACCARD),
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

    if (milvus::indexbuilder::is_in_need_id_list(index_type)) {
        ASSERT_ANY_THROW(index->BuildWithoutIds(xb_dataset));
    } else {
        ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));
    }
}

TEST_P(IndexWrapperTest, Codec) {
    auto index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());

    ASSERT_NO_THROW(index->BuildWithoutIds(xb_dataset));

    auto binary = index->Serialize();
    auto copy_index =
        std::make_unique<milvus::indexbuilder::IndexWrapper>(type_params_str.c_str(), index_params_str.c_str());
    ASSERT_NO_THROW(copy_index->Load(binary.data, binary.size));
    ASSERT_EQ(copy_index->dim(), copy_index->dim());
    auto copy_binary = copy_index->Serialize();
    if (!milvus::indexbuilder::is_in_nm_list(index_type)) {
        // binary may be not same due to uncertain internal map order
        ASSERT_EQ(binary.size, copy_binary.size);
        ASSERT_EQ(strcmp(binary.data, copy_binary.data), 0);
    }
}
