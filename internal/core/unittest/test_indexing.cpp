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

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>

#include <boost/filesystem/operations.hpp>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "arrow/type.h"
#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "index/Index.h"
#include "knowhere/comp/index_param.h"
#include "nlohmann/json.hpp"
#include "query/SearchBruteForce.h"
#include "segcore/reduce/Reduce.h"
#include "index/IndexFactory.h"
#include "common/QueryResult.h"
#include "segcore/Types.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/Timer.h"
#include "storage/Util.h"
#include <boost/filesystem.hpp>

using namespace milvus;
using namespace milvus::segcore;

namespace {
template <int DIM>
auto
generate_data(int N) {
    std::vector<float> raw_data;
    std::vector<uint64_t> timestamps;
    std::vector<int64_t> uids;
    std::default_random_engine er(42);
    std::uniform_real_distribution<> distribution(0.0, 1.0);
    std::default_random_engine ei(42);
    for (int i = 0; i < N; ++i) {
        uids.push_back(10 * N + i);
        timestamps.push_back(0);
        // append vec
        std::vector<float> vec(DIM);
        for (auto& x : vec) {
            x = distribution(er);
        }
        raw_data.insert(raw_data.end(), std::begin(vec), std::end(vec));
    }
    return std::make_tuple(raw_data, timestamps, uids);
}
}  // namespace

SegcoreError
merge_into(int64_t queries,
           int64_t topk,
           float* distances,
           int64_t* uids,
           const float* new_distances,
           const int64_t* new_uids) {
    for (int64_t qn = 0; qn < queries; ++qn) {
        auto base = qn * topk;
        auto src2_dis = distances + base;
        auto src2_uids = uids + base;

        auto src1_dis = new_distances + base;
        auto src1_uids = new_uids + base;

        std::vector<float> buf_dis(topk);
        std::vector<int64_t> buf_uids(topk);

        auto it1 = 0;
        auto it2 = 0;

        for (auto buf = 0; buf < topk; ++buf) {
            if (src1_dis[it1] <= src2_dis[it2]) {
                buf_dis[buf] = src1_dis[it1];
                buf_uids[buf] = src1_uids[it1];
                ++it1;
            } else {
                buf_dis[buf] = src2_dis[it2];
                buf_uids[buf] = src2_uids[it2];
                ++it2;
            }
        }
        std::copy_n(buf_dis.data(), topk, src2_dis);
        std::copy_n(buf_uids.data(), topk, src2_uids);
    }
    return SegcoreError::success();
}

/*
TEST(Indexing, SmartBruteForce) {
    int64_t N = 1000;
    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);

    constexpr int64_t queries = 3;
    auto total_count = queries * K;

    auto raw = (const float*)raw_data.data();
    EXPECT_NE(raw, nullptr);

    auto query_data = raw;

    std::vector<int64_t> final_uids(total_count, -1);
    std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    for (int beg = 0; beg < N; beg += TestChunkSize) {
        std::vector<int64_t> buf_uids(total_count, -1);
        std::vector<float> buf_dis(total_count, std::numeric_limits<float>::max());
        faiss::float_maxheap_array_t buf = {queries, K, buf_uids.data(), buf_dis.data()};
        auto end = beg + TestChunkSize;
        if (end > N) {
            end = N;
        }
        auto nsize = end - beg;
        auto src_data = raw + beg * DIM;

        faiss::knn_L2sqr(query_data, src_data, DIM, queries, nsize, &buf, nullptr);
        for (auto& x : buf_uids) {
            x = uids[x + beg];
        }
        merge_into(queries, K, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
    }

    for (int qn = 0; qn < queries; ++qn) {
        for (int kn = 0; kn < K; ++kn) {
            auto index = qn * K + kn;
            std::cout << final_uids[index] << "->" << final_dis[index] << std::endl;
        }
        std::cout << std::endl;
    }
}
*/
TEST(Indexing, BinaryBruteForce) {
    int64_t N = 100000;
    int64_t num_queries = 10;
    int64_t topk = 5;
    int64_t round_decimal = 3;
    int64_t dim = 8192;
    Config search_params_ = {};
    auto metric_type = knowhere::metric::JACCARD;
    auto result_count = topk * num_queries;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugField(
        "vecbin", DataType::VECTOR_BINARY, dim, metric_type);
    auto i64_fid = schema->AddDebugField("age", DataType::INT64);
    auto dataset = DataGen(schema, N, 10);
    auto bin_vec = dataset.get_col<uint8_t>(vec_fid);
    auto query_data = 1024 * dim / 8 + bin_vec.data();
    query::dataset::SearchDataset search_dataset{
        metric_type,  //
        num_queries,  //
        topk,         //
        round_decimal,
        dim,        //
        query_data  //
    };

    SearchInfo search_info;
    auto index_info = std::map<std::string, std::string>{};
    search_info.topk_ = topk;
    search_info.round_decimal_ = round_decimal;
    search_info.metric_type_ = metric_type;
    auto base_dataset = query::dataset::RawDataset{
        int64_t(0), dim, N, (const void*)bin_vec.data()};
    auto sub_result = query::BruteForceSearch(search_dataset,
                                              base_dataset,
                                              search_info,
                                              index_info,
                                              nullptr,
                                              DataType::VECTOR_BINARY);

    SearchResult sr;
    sr.total_nq_ = num_queries;
    sr.unity_topK_ = topk;
    sr.seg_offsets_ = std::move(sub_result.mutable_seg_offsets());
    sr.distances_ = std::move(sub_result.mutable_distances());

    auto json = SearchResultToJson(sr);
    std::cout << json.dump(2);
#ifdef __linux__
    auto ref = nlohmann::json::parse(R"(
[
  [
    [ "1024->0.000000", "48942->0.642000", "18494->0.644000", "68225->0.644000", "93557->0.644000" ],
    [ "1025->0.000000", "73557->0.641000", "53086->0.643000", "9737->0.643000", "62855->0.644000" ],
    [ "1026->0.000000", "62904->0.644000", "46758->0.644000", "57969->0.645000", "98113->0.646000" ],
    [ "1027->0.000000", "92446->0.638000", "96034->0.640000", "92129->0.644000", "45887->0.644000" ],
    [ "1028->0.000000", "22992->0.643000", "73903->0.644000", "19969->0.645000", "65178->0.645000" ],
    [ "1029->0.000000", "19776->0.641000", "15166->0.642000", "85470->0.642000", "16730->0.643000" ],
    [ "1030->0.000000", "55939->0.640000", "84253->0.643000", "31958->0.644000", "11667->0.646000" ],
    [ "1031->0.000000", "89536->0.637000", "61622->0.638000", "9275->0.639000", "91403->0.640000" ],
    [ "1032->0.000000", "69504->0.642000", "23414->0.644000", "48770->0.645000", "23231->0.645000" ],
    [ "1033->0.000000", "33540->0.636000", "25310->0.640000", "18576->0.640000", "73729->0.642000" ]
  ]
]
)");
#else  // for mac
    auto ref = nlohmann::json::parse(R"(
[
  [
    [ "1024->0.000000", "59169->0.645000", "98548->0.646000", "3356->0.646000", "90373->0.647000" ],
    [ "1025->0.000000", "61245->0.638000", "95271->0.639000", "31087->0.639000", "31549->0.640000" ],
    [ "1026->0.000000", "65225->0.648000", "35750->0.648000", "14971->0.649000", "75385->0.649000" ],
    [ "1027->0.000000", "70158->0.640000", "27076->0.640000", "3407->0.641000", "59527->0.641000" ],
    [ "1028->0.000000", "45757->0.645000", "3356->0.645000", "77230->0.646000", "28690->0.647000" ],
    [ "1029->0.000000", "13291->0.642000", "24960->0.643000", "83770->0.643000", "88244->0.643000" ],
    [ "1030->0.000000", "96807->0.641000", "39920->0.643000", "62943->0.644000", "12603->0.644000" ],
    [ "1031->0.000000", "65769->0.648000", "60493->0.648000", "48738->0.648000", "4353->0.648000" ],
    [ "1032->0.000000", "57827->0.637000", "8213->0.638000", "22221->0.639000", "23328->0.640000" ],
    [ "1033->0.000000", "676->0.645000", "91430->0.646000", "85353->0.646000", "6014->0.646000" ]
  ]
]
)");
#endif
    auto json_str = json.dump(2);
    auto ref_str = ref.dump(2);
    ASSERT_EQ(json_str, ref_str);
}

TEST(Indexing, Naive) {
    constexpr int N = 10000;
    constexpr int TOPK = 10;

    auto [raw_data, timestamps, uids] = generate_data<DIM>(N);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto build_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::meta::DIM, std::to_string(DIM)},
        {knowhere::indexparam::NLIST, "100"},
        {knowhere::indexparam::M, "4"},
        {knowhere::indexparam::NBITS, "8"},
    };

    auto search_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::indexparam::NPROBE, 4},
    };

    std::vector<knowhere::DataSetPtr> datasets;
    std::vector<std::vector<float>> ftrashs;
    auto raw = raw_data.data();
    for (int beg = 0; beg < N; beg += TestChunkSize) {
        auto end = beg + TestChunkSize;
        if (end > N) {
            end = N;
        }
        std::vector<float> ft(raw + DIM * beg, raw + DIM * end);

        auto ds = knowhere::GenDataSet(end - beg, DIM, ft.data());
        datasets.push_back(ds);
        ftrashs.push_back(std::move(ft));
    }

    for (auto& ds : datasets) {
        index->BuildWithDataset(ds, build_conf);
    }

    auto bitmap = BitsetType(N, false);
    // exclude the first
    for (int i = 0; i < N / 2; ++i) {
        bitmap.set(i);
    }

    BitsetView view = bitmap;
    auto query_ds = knowhere::GenDataSet(1, DIM, raw_data.data());

    milvus::SearchInfo searchInfo;
    searchInfo.topk_ = TOPK;
    searchInfo.metric_type_ = knowhere::metric::L2;
    searchInfo.search_params_ = search_conf;
    auto vec_index = dynamic_cast<index::VectorIndex*>(index.get());
    SearchResult result;
    vec_index->Query(query_ds, searchInfo, view, result);

    for (int i = 0; i < TOPK; ++i) {
        ASSERT_FALSE(result.seg_offsets_[i] < N / 2);
    }
}

using Param = std::pair<knowhere::IndexType, knowhere::MetricType>;

class IndexTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        storage_config_ = get_default_local_storage_config();

        auto param = GetParam();
        index_type = param.first;
        metric_type = param.second;

        // try to reduce the test time,
        // but the large dataset is needed for the case below.
        auto test_name = std::string(
            testing::UnitTest::GetInstance()->current_test_info()->name());
        if (test_name == "Mmap" &&
            index_type == knowhere::IndexEnum::INDEX_HNSW) {
            NB = 270000;
        }
        build_conf = generate_build_conf(index_type, metric_type);
        load_conf = generate_load_conf(index_type, metric_type, NB);
        search_conf = generate_search_conf(index_type, metric_type);
        range_search_conf = generate_range_search_conf(index_type, metric_type);

        if (index_type == knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
            index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
            is_sparse = true;
            vec_field_data_type = milvus::DataType::VECTOR_SPARSE_FLOAT;
        } else if (IsBinaryVectorMetricType(metric_type)) {
            is_binary = true;
            vec_field_data_type = milvus::DataType::VECTOR_BINARY;
        } else {
            vec_field_data_type = milvus::DataType::VECTOR_FLOAT;
        }

        auto dataset =
            GenDatasetWithDataType(NB, metric_type, vec_field_data_type);
        if (is_binary) {
            // binary vector
            xb_bin_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
            xb_dataset = knowhere::GenDataSet(NB, DIM, xb_bin_data.data());
            xq_dataset = knowhere::GenDataSet(
                NQ, DIM, xb_bin_data.data() + DIM * query_offset);
        } else if (is_sparse) {
            // sparse vector
            xb_sparse_data =
                dataset.get_col<knowhere::sparse::SparseRow<float>>(
                    milvus::FieldId(100));
            xb_dataset =
                knowhere::GenDataSet(NB, kTestSparseDim, xb_sparse_data.data());
            xb_dataset->SetIsSparse(true);
            xq_dataset = knowhere::GenDataSet(
                NQ, kTestSparseDim, xb_sparse_data.data() + query_offset);
            xq_dataset->SetIsSparse(true);
        } else {
            // float vector
            xb_data = dataset.get_col<float>(milvus::FieldId(100));
            xb_dataset = knowhere::GenDataSet(NB, DIM, xb_data.data());
            xq_dataset = knowhere::GenDataSet(
                NQ, DIM, xb_data.data() + DIM * query_offset);
        }
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type, metric_type;
    bool is_binary = false;
    bool is_sparse = false;
    milvus::Config build_conf;
    milvus::Config load_conf;
    milvus::Config search_conf;
    milvus::Config range_search_conf;
    milvus::DataType vec_field_data_type;
    knowhere::DataSetPtr xb_dataset;
    FixedVector<float> xb_data;
    FixedVector<uint8_t> xb_bin_data;
    FixedVector<knowhere::sparse::SparseRow<float>> xb_sparse_data;
    knowhere::DataSetPtr xq_dataset;
    int64_t query_offset = 100;
    int64_t NB = 3000;  // will be updated to 27000 for mmap+hnsw
    StorageConfig storage_config_;
};

INSTANTIATE_TEST_SUITE_P(
    IndexTypeParameters,
    IndexTest,
    ::testing::Values(
        std::pair(knowhere::IndexEnum::INDEX_FAISS_IDMAP, knowhere::metric::L2),
        std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFPQ, knowhere::metric::L2),
        std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                  knowhere::metric::L2),
        std::pair(knowhere::IndexEnum::INDEX_FAISS_IVFSQ8,
                  knowhere::metric::L2),
        std::pair(knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
                  knowhere::metric::JACCARD),
        std::pair(knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
                  knowhere::metric::JACCARD),
        std::pair(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                  knowhere::metric::IP),
        std::pair(knowhere::IndexEnum::INDEX_SPARSE_WAND, knowhere::metric::IP),
#ifdef BUILD_DISK_ANN
        std::pair(knowhere::IndexEnum::INDEX_DISKANN, knowhere::metric::L2),
#endif
        std::pair(knowhere::IndexEnum::INDEX_HNSW, knowhere::metric::L2)));

TEST(Indexing, Iterator) {
    constexpr int N = 10240;
    constexpr int TOPK = 100;
    constexpr int dim = 128;

    auto [raw_data, timestamps, uids] = generate_data<dim>(N);
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::VECTOR_FLOAT;
    create_index_info.metric_type = knowhere::metric::L2;
    create_index_info.index_type = knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, milvus::storage::FileManagerContext());

    auto build_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::meta::DIM, std::to_string(dim)},
        {knowhere::indexparam::NLIST, "128"},
    };

    auto search_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
        {knowhere::indexparam::NPROBE, 4},
    };

    index->BuildWithDataset(knowhere::GenDataSet(N, dim, raw_data.data()),
                            build_conf);

    auto bitmap = BitsetType(N, false);

    BitsetView view = bitmap;
    auto query_ds = knowhere::GenDataSet(1, dim, raw_data.data());

    milvus::SearchInfo searchInfo;
    searchInfo.topk_ = TOPK;
    searchInfo.metric_type_ = knowhere::metric::L2;
    searchInfo.search_params_ = search_conf;
    auto vec_index = dynamic_cast<index::VectorIndex*>(index.get());

    knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
        kw_iterators = vec_index->VectorIterators(
            query_ds, searchInfo.search_params_, view);
    ASSERT_TRUE(kw_iterators.has_value());
    ASSERT_EQ(kw_iterators.value().size(), 1);
    auto iterator = kw_iterators.value()[0];
    ASSERT_TRUE(iterator->HasNext());
    while (iterator->HasNext()) {
        auto [off, dis] = iterator->Next();
        ASSERT_TRUE(off >= 0);
        ASSERT_TRUE(dis >= 0);
    }
}

TEST_P(IndexTest, BuildAndQuery) {
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = vec_field_data_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    index::IndexBasePtr index;

    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config_);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));
    milvus::index::IndexBasePtr new_index;
    milvus::index::VectorIndex* vec_index = nullptr;

    auto create_index_result = index->Upload();
    index.reset();

    new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());

    auto index_files = create_index_result->GetIndexFiles();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    load_conf = generate_load_conf(index_type, metric_type, 0);
    load_conf["index_files"] = index_files;
    ASSERT_NO_THROW(vec_index->Load(milvus::tracer::TraceContext{}, load_conf));
    EXPECT_EQ(vec_index->Count(), NB);
    if (!is_sparse) {
        EXPECT_EQ(vec_index->GetDim(), DIM);
    }

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = search_conf;
    SearchResult result;
    vec_index->Query(xq_dataset, search_info, nullptr, result);
    EXPECT_EQ(result.total_nq_, NQ);
    EXPECT_EQ(result.unity_topK_, K);
    EXPECT_EQ(result.distances_.size(), NQ * K);
    EXPECT_EQ(result.seg_offsets_.size(), NQ * K);
    if (metric_type == knowhere::metric::L2) {
        // for L2 metric each vector is closest to itself
        for (int i = 0; i < NQ; i++) {
            EXPECT_EQ(result.seg_offsets_[i * K], query_offset + i);
        }
        // for other metrics we can't verify the correctness unless we perform
        // brute force search to get the ground truth.
    }
    if (!is_sparse) {
        // sparse doesn't support range search yet
        search_info.search_params_ = range_search_conf;
        vec_index->Query(xq_dataset, search_info, nullptr, result);
    }
}

TEST_P(IndexTest, Mmap) {
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = vec_field_data_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    index::IndexBasePtr index;

    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config_);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));
    milvus::index::IndexBasePtr new_index;
    milvus::index::VectorIndex* vec_index = nullptr;

    auto create_index_result = index->Upload();
    index.reset();

    new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    if (!new_index->IsMmapSupported()) {
        return;
    }
    vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());

    auto index_files = create_index_result->GetIndexFiles();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    load_conf = generate_load_conf(index_type, metric_type, 0);
    load_conf["index_files"] = index_files;
    load_conf["mmap_filepath"] = "mmap/test_index_mmap_" + index_type;
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    EXPECT_EQ(vec_index->Count(), NB);
    EXPECT_EQ(vec_index->GetDim(), is_sparse ? kTestSparseDim : DIM);

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = search_conf;
    SearchResult result;
    vec_index->Query(xq_dataset, search_info, nullptr, result);
    EXPECT_EQ(result.total_nq_, NQ);
    EXPECT_EQ(result.unity_topK_, K);
    EXPECT_EQ(result.distances_.size(), NQ * K);
    EXPECT_EQ(result.seg_offsets_.size(), NQ * K);
    if (!is_binary) {
        EXPECT_EQ(result.seg_offsets_[0], query_offset);
    }
    search_info.search_params_ = range_search_conf;
    vec_index->Query(xq_dataset, search_info, nullptr, result);
}

TEST_P(IndexTest, GetVector) {
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = vec_field_data_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    index::IndexBasePtr index;

    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config_);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));
    milvus::index::IndexBasePtr new_index;
    milvus::index::VectorIndex* vec_index = nullptr;

    auto create_index_result = index->Upload();
    index.reset();
    auto index_files = create_index_result->GetIndexFiles();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    load_conf = generate_load_conf(index_type, metric_type, 0);
    load_conf["index_files"] = index_files;

    vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    if (!is_sparse) {
        EXPECT_EQ(vec_index->GetDim(), DIM);
    }
    EXPECT_EQ(vec_index->Count(), NB);

    if (!vec_index->HasRawData()) {
        return;
    }

    auto ids_ds = GenRandomIds(NB);
    if (is_binary) {
        auto results = vec_index->GetVector(ids_ds);
        EXPECT_EQ(results.size(), xb_bin_data.size());
        const auto data_bytes = DIM / 8;
        for (size_t i = 0; i < NB; ++i) {
            auto id = ids_ds->GetIds()[i];
            for (size_t j = 0; j < data_bytes; ++j) {
                ASSERT_EQ(results[i * data_bytes + j],
                          xb_bin_data[id * data_bytes + j]);
            }
        }
    } else if (is_sparse) {
        auto sparse_rows = vec_index->GetSparseVector(ids_ds);
        for (size_t i = 0; i < NB; ++i) {
            auto id = ids_ds->GetIds()[i];
            auto& row = sparse_rows[i];
            ASSERT_EQ(row.size(), xb_sparse_data[id].size());
            for (size_t j = 0; j < row.size(); ++j) {
                ASSERT_EQ(row[j].id, xb_sparse_data[id][j].id);
                ASSERT_EQ(row[j].val, xb_sparse_data[id][j].val);
            }
        }
    } else {
        auto results = vec_index->GetVector(ids_ds);
        std::vector<float> result_vectors(results.size() / (sizeof(float)));
        memcpy(result_vectors.data(), results.data(), results.size());
        ASSERT_EQ(result_vectors.size(), xb_data.size());
        for (size_t i = 0; i < NB; ++i) {
            auto id = ids_ds->GetIds()[i];
            for (size_t j = 0; j < DIM; ++j) {
                ASSERT_EQ(result_vectors[i * DIM + j], xb_data[id * DIM + j]);
            }
        }
    }
}

// This ut runs for sparse only. And will not use the default xb_sparse_dataset.
TEST_P(IndexTest, GetVector_EmptySparseVector) {
    if (index_type != knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX &&
        index_type != knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        return;
    }
    NB = 3;

    std::vector<knowhere::sparse::SparseRow<float>> vec;
    vec.reserve(NB);
    vec.emplace_back(2);
    vec[0].set_at(0, 1, 1.0);
    vec[0].set_at(1, 2, 2.0);
    // row1 is an explicit empty row
    vec.emplace_back(0);
    // row2 is an implicit empty row(provided dim has a value of 0)
    vec.emplace_back(1);
    vec[2].set_at(0, 1, 0);

    auto dataset = knowhere::GenDataSet(NB, 3, vec.data());

    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = vec_field_data_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    index::IndexBasePtr index;

    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config_);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    // use custom dataset instead of xb_dataset
    ASSERT_NO_THROW(index->BuildWithDataset(dataset, build_conf));
    milvus::index::IndexBasePtr new_index;
    milvus::index::VectorIndex* vec_index = nullptr;

    auto create_index_result = index->Upload();
    index.reset();
    auto index_files = create_index_result->GetIndexFiles();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    load_conf = generate_load_conf(index_type, metric_type, 0);
    load_conf["index_files"] = index_files;

    vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    EXPECT_EQ(vec_index->Count(), NB);

    auto ids_ds = GenRandomIds(NB);
    auto sparse_rows = vec_index->GetSparseVector(ids_ds);
    for (size_t i = 0; i < NB; ++i) {
        auto id = ids_ds->GetIds()[i];
        auto& row = sparse_rows[i];
        ASSERT_EQ(row.size(), vec[id].size());
        for (size_t j = 0; j < row.size(); ++j) {
            ASSERT_EQ(row[j].id, vec[id][j].id);
            ASSERT_EQ(row[j].val, vec[id][j].val);
        }
    }
}

#ifdef BUILD_DISK_ANN
TEST(Indexing, SearchDiskAnnWithInvalidParam) {
    int64_t NB = 1000;
    IndexType index_type = knowhere::IndexEnum::INDEX_DISKANN;
    MetricType metric_type = knowhere::metric::L2;
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = milvus::DataType::VECTOR_FLOAT;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 100;
    int64_t build_id = 1000;
    int64_t index_version = 1;

    StorageConfig storage_config = get_default_local_storage_config();
    milvus::storage::FieldDataMeta field_data_meta{
        collection_id, partition_id, segment_id, field_id};
    milvus::storage::IndexMeta index_meta{
        segment_id, field_id, build_id, index_version};
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    auto build_conf = Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {knowhere::meta::DIM, std::to_string(DIM)},
        {milvus::index::DISK_ANN_MAX_DEGREE, std::to_string(24)},
        {milvus::index::DISK_ANN_SEARCH_LIST_SIZE, std::to_string(56)},
        {milvus::index::DISK_ANN_PQ_CODE_BUDGET, std::to_string(0.001)},
        {milvus::index::DISK_ANN_BUILD_DRAM_BUDGET, std::to_string(2)},
        {milvus::index::DISK_ANN_BUILD_THREAD_NUM, std::to_string(2)},
    };

    // build disk ann index
    auto dataset = GenDataset(NB, metric_type, false);
    FixedVector<float> xb_data =
        dataset.get_col<float>(milvus::FieldId(field_id));
    knowhere::DataSetPtr xb_dataset =
        knowhere::GenDataSet(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));

    // serialize and load disk index, disk index can only be search after loading for now
    auto create_index_result = index->Upload();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    auto index_files = create_index_result->GetIndexFiles();
    index.reset();

    auto new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    auto vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());
    auto load_conf = generate_load_conf(index_type, metric_type, NB);
    load_conf["index_files"] = index_files;
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    EXPECT_EQ(vec_index->Count(), NB);

    // search disk index with search_list == limit
    int query_offset = 100;
    knowhere::DataSetPtr xq_dataset =
        knowhere::GenDataSet(NQ, DIM, xb_data.data() + DIM * query_offset);

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = milvus::Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {milvus::index::DISK_ANN_QUERY_LIST, K - 1},
    };
    SearchResult result;
    EXPECT_THROW(vec_index->Query(xq_dataset, search_info, nullptr, result),
                 std::runtime_error);
}

TEST(Indexing, SearchDiskAnnWithFloat16) {
    int64_t NB = 1000;
    int64_t NQ = 2;
    int64_t K = 4;
    IndexType index_type = knowhere::IndexEnum::INDEX_DISKANN;
    MetricType metric_type = knowhere::metric::L2;
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = milvus::DataType::VECTOR_FLOAT16;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 100;
    int64_t build_id = 1000;
    int64_t index_version = 1;

    StorageConfig storage_config = get_default_local_storage_config();
    milvus::storage::FieldDataMeta field_data_meta{
        collection_id, partition_id, segment_id, field_id};
    milvus::storage::IndexMeta index_meta{
        segment_id, field_id, build_id, index_version};
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    auto build_conf = Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {knowhere::meta::DIM, std::to_string(DIM)},
        {milvus::index::DISK_ANN_MAX_DEGREE, std::to_string(24)},
        {milvus::index::DISK_ANN_SEARCH_LIST_SIZE, std::to_string(56)},
        {milvus::index::DISK_ANN_PQ_CODE_BUDGET, std::to_string(0.001)},
        {milvus::index::DISK_ANN_BUILD_DRAM_BUDGET, std::to_string(2)},
        {milvus::index::DISK_ANN_BUILD_THREAD_NUM, std::to_string(2)},
    };

    // build disk ann index
    auto dataset = GenDatasetWithDataType(
        NB, metric_type, milvus::DataType::VECTOR_FLOAT16);
    FixedVector<float16> xb_data =
        dataset.get_col<float16>(milvus::FieldId(field_id));
    knowhere::DataSetPtr xb_dataset =
        knowhere::GenDataSet(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));

    // serialize and load disk index, disk index can only be search after loading for now
    auto create_index_result = index->Upload();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    auto index_files = create_index_result->GetIndexFiles();
    index.reset();

    auto new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    auto vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());
    auto load_conf = generate_load_conf<float16>(index_type, metric_type, NB);
    load_conf["index_files"] = index_files;
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    EXPECT_EQ(vec_index->Count(), NB);

    // search disk index with search_list == limit
    int query_offset = 100;
    knowhere::DataSetPtr xq_dataset =
        knowhere::GenDataSet(NQ, DIM, xb_data.data() + DIM * query_offset);

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = milvus::Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {milvus::index::DISK_ANN_QUERY_LIST, K * 2},
    };
    SearchResult result;
    EXPECT_NO_THROW(vec_index->Query(xq_dataset, search_info, nullptr, result));
}

TEST(Indexing, SearchDiskAnnWithBFloat16) {
    int64_t NB = 1000;
    int64_t NQ = 2;
    int64_t K = 4;
    IndexType index_type = knowhere::IndexEnum::INDEX_DISKANN;
    MetricType metric_type = knowhere::metric::L2;
    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.index_type = index_type;
    create_index_info.metric_type = metric_type;
    create_index_info.field_type = milvus::DataType::VECTOR_BFLOAT16;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    int64_t collection_id = 1;
    int64_t partition_id = 2;
    int64_t segment_id = 3;
    int64_t field_id = 100;
    int64_t build_id = 1000;
    int64_t index_version = 1;

    StorageConfig storage_config = get_default_local_storage_config();
    milvus::storage::FieldDataMeta field_data_meta{
        collection_id, partition_id, segment_id, field_id};
    milvus::storage::IndexMeta index_meta{
        segment_id, field_id, build_id, index_version};
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);

    auto build_conf = Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {knowhere::meta::DIM, std::to_string(DIM)},
        {milvus::index::DISK_ANN_MAX_DEGREE, std::to_string(24)},
        {milvus::index::DISK_ANN_SEARCH_LIST_SIZE, std::to_string(56)},
        {milvus::index::DISK_ANN_PQ_CODE_BUDGET, std::to_string(0.001)},
        {milvus::index::DISK_ANN_BUILD_DRAM_BUDGET, std::to_string(2)},
        {milvus::index::DISK_ANN_BUILD_THREAD_NUM, std::to_string(2)},
    };

    // build disk ann index
    auto dataset = GenDatasetWithDataType(
        NB, metric_type, milvus::DataType::VECTOR_BFLOAT16);
    FixedVector<bfloat16> xb_data =
        dataset.get_col<bfloat16>(milvus::FieldId(field_id));
    knowhere::DataSetPtr xb_dataset =
        knowhere::GenDataSet(NB, DIM, xb_data.data());
    ASSERT_NO_THROW(index->BuildWithDataset(xb_dataset, build_conf));

    // serialize and load disk index, disk index can only be search after loading for now
    auto create_index_result = index->Upload();
    auto memSize = create_index_result->GetMemSize();
    auto serializedSize = create_index_result->GetSerializedSize();
    ASSERT_GT(memSize, 0);
    ASSERT_GT(serializedSize, 0);
    auto index_files = create_index_result->GetIndexFiles();
    index.reset();

    auto new_index = milvus::index::IndexFactory::GetInstance().CreateIndex(
        create_index_info, file_manager_context);
    auto vec_index = dynamic_cast<milvus::index::VectorIndex*>(new_index.get());
    auto load_conf = generate_load_conf<bfloat16>(index_type, metric_type, NB);
    load_conf["index_files"] = index_files;
    vec_index->Load(milvus::tracer::TraceContext{}, load_conf);
    EXPECT_EQ(vec_index->Count(), NB);

    // search disk index with search_list == limit
    int query_offset = 100;
    knowhere::DataSetPtr xq_dataset =
        knowhere::GenDataSet(NQ, DIM, xb_data.data() + DIM * query_offset);

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = milvus::Config{
        {knowhere::meta::METRIC_TYPE, metric_type},
        {milvus::index::DISK_ANN_QUERY_LIST, K * 2},
    };
    SearchResult result;
    EXPECT_NO_THROW(vec_index->Query(xq_dataset, search_info, nullptr, result));
}
#endif

TEST(Indexing, IndexStats) {
    using milvus::index::IndexStats;
    using milvus::index::SerializedIndexFileInfo;
    auto result = IndexStats::NewFromSizeMap(
        16, std::map<std::string, size_t>{{"file1", 100}, {"file2", 200}});
    result->AppendSerializedIndexFileInfo(
        SerializedIndexFileInfo{"file3", 300});
    auto files = result->GetIndexFiles();
    ASSERT_EQ(files.size(), 3);
    ASSERT_EQ(files[0], "file1");
    ASSERT_EQ(files[1], "file2");
    ASSERT_EQ(files[2], "file3");
    ASSERT_EQ(result->GetMemSize(), 16);
    ASSERT_EQ(result->GetSerializedSize(), 600);
}