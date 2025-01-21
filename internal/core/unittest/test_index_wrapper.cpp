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

#include "common/Types.h"
#include "indexbuilder/IndexFactory.h"
#include "indexbuilder/VecIndexCreator.h"
#include "common/QueryResult.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::proto;

using Param = std::pair<knowhere::IndexType, knowhere::MetricType>;

class IndexWrapperTest : public ::testing::TestWithParam<Param> {
 protected:
    void
    SetUp() override {
        storage_config_ = get_default_local_storage_config();

        auto param = GetParam();
        index_type = param.first;
        metric_type = param.second;
        std::tie(type_params, index_params) =
            generate_params(index_type, metric_type);

        for (auto i = 0; i < type_params.params_size(); ++i) {
            const auto& p = type_params.params(i);
            config[p.key()] = p.value();
        }

        for (auto i = 0; i < index_params.params_size(); ++i) {
            const auto& p = index_params.params(i);
            config[p.key()] = p.value();
        }

        bool ok;
        ok = google::protobuf::TextFormat::PrintToString(type_params,
                                                         &type_params_str);
        assert(ok);
        ok = google::protobuf::TextFormat::PrintToString(index_params,
                                                         &index_params_str);
        assert(ok);

        search_conf = generate_search_conf(index_type, metric_type);

        std::map<knowhere::MetricType, DataType> index_to_vec_type = {
            {knowhere::IndexEnum::INDEX_FAISS_IDMAP, DataType::VECTOR_FLOAT},
            {knowhere::IndexEnum::INDEX_FAISS_IVFPQ, DataType::VECTOR_FLOAT},
            {knowhere::IndexEnum::INDEX_FAISS_IVFFLAT, DataType::VECTOR_FLOAT},
            {knowhere::IndexEnum::INDEX_FAISS_IVFSQ8, DataType::VECTOR_FLOAT},
            {knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT,
             DataType::VECTOR_BINARY},
            {knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP,
             DataType::VECTOR_BINARY},
            {knowhere::IndexEnum::INDEX_HNSW, DataType::VECTOR_FLOAT},
            {knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
             DataType::VECTOR_SPARSE_FLOAT},
            {knowhere::IndexEnum::INDEX_SPARSE_WAND,
             DataType::VECTOR_SPARSE_FLOAT},
        };

        vec_field_data_type = index_to_vec_type[index_type];
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type, metric_type;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::string type_params_str, index_params_str;
    Config config;
    milvus::Config search_conf;
    DataType vec_field_data_type;
    int64_t query_offset = 1;
    int64_t NB = 10;
    StorageConfig storage_config_;
};

INSTANTIATE_TEST_SUITE_P(
    IndexTypeParameters,
    IndexWrapperTest,
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
        std::pair(knowhere::IndexEnum::INDEX_HNSW, knowhere::metric::L2),
        std::pair(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                  knowhere::metric::IP),
        std::pair(knowhere::IndexEnum::INDEX_SPARSE_WAND,
                  knowhere::metric::IP)));

TEST_P(IndexWrapperTest, BuildAndQuery) {
    milvus::storage::FieldDataMeta field_data_meta{1, 2, 3, 100};
    milvus::storage::IndexMeta index_meta{3, 100, 1000, 1};
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config_);

    storage::FileManagerContext file_manager_context(
        field_data_meta, index_meta, chunk_manager);
    config[milvus::index::INDEX_ENGINE_VERSION] =
        std::to_string(knowhere::Version::GetCurrentVersion().VersionNumber());
    auto index = milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
        vec_field_data_type, config, file_manager_context);
    knowhere::DataSetPtr xb_dataset;
    if (vec_field_data_type == DataType::VECTOR_BINARY) {
        auto dataset = GenFieldData(NB, metric_type, vec_field_data_type);
        auto bin_vecs = dataset.get_col<uint8_t>(milvus::FieldId(100));
        xb_dataset = knowhere::GenDataSet(NB, DIM, bin_vecs.data());
        ASSERT_NO_THROW(index->Build(xb_dataset));
    } else if (vec_field_data_type == DataType::VECTOR_SPARSE_FLOAT) {
        auto dataset = GenFieldData(NB, metric_type, vec_field_data_type);
        auto sparse_vecs = dataset.get_col<knowhere::sparse::SparseRow<float>>(
            milvus::FieldId(100));
        xb_dataset =
            knowhere::GenDataSet(NB, kTestSparseDim, sparse_vecs.data());
        xb_dataset->SetIsSparse(true);
        ASSERT_NO_THROW(index->Build(xb_dataset));
    } else {
        // VECTOR_FLOAT
        auto dataset = GenFieldData(NB, metric_type);
        auto f_vecs = dataset.get_col<float>(milvus::FieldId(100));
        xb_dataset = knowhere::GenDataSet(NB, DIM, f_vecs.data());
        ASSERT_NO_THROW(index->Build(xb_dataset));
    }

    auto binary_set = index->Serialize();
    FixedVector<std::string> index_files;
    for (auto& binary : binary_set.binary_map_) {
        index_files.emplace_back(binary.first);
    }
    config["index_files"] = index_files;
    auto copy_index =
        milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
            vec_field_data_type, config, file_manager_context);
    auto vec_index =
        static_cast<milvus::indexbuilder::VecIndexCreator*>(copy_index.get());
    if (vec_field_data_type != DataType::VECTOR_SPARSE_FLOAT) {
        ASSERT_EQ(vec_index->dim(), DIM);
    }

    ASSERT_NO_THROW(vec_index->Load(binary_set));

    milvus::SearchInfo search_info;
    search_info.topk_ = K;
    search_info.metric_type_ = metric_type;
    search_info.search_params_ = search_conf;
    std::unique_ptr<SearchResult> result;
    if (vec_field_data_type == DataType::VECTOR_FLOAT) {
        auto nb_for_nq = NQ + query_offset;
        auto dataset = GenFieldData(nb_for_nq, metric_type);
        auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
        auto xq_dataset =
            knowhere::GenDataSet(NQ, DIM, xb_data.data() + DIM * query_offset);
        result = vec_index->Query(xq_dataset, search_info, nullptr);
    } else if (vec_field_data_type == DataType::VECTOR_SPARSE_FLOAT) {
        auto dataset = GenFieldData(NQ, metric_type, vec_field_data_type);
        auto xb_data = dataset.get_col<knowhere::sparse::SparseRow<float>>(
            milvus::FieldId(100));
        auto xq_dataset =
            knowhere::GenDataSet(NQ, kTestSparseDim, xb_data.data());
        xq_dataset->SetIsSparse(true);
        result = vec_index->Query(xq_dataset, search_info, nullptr);
    } else {
        auto nb_for_nq = NQ + query_offset;
        auto dataset =
            GenFieldData(nb_for_nq, metric_type, DataType::VECTOR_BINARY);
        auto xb_bin_data = dataset.get_col<uint8_t>(milvus::FieldId(100));
        // offset of binary vector is 8-aligned bit-wise representation.
        auto xq_dataset = knowhere::GenDataSet(
            NQ, DIM, xb_bin_data.data() + ((DIM + 7) / 8) * query_offset);
        result = vec_index->Query(xq_dataset, search_info, nullptr);
    }

    EXPECT_EQ(result->total_nq_, NQ);
    EXPECT_EQ(result->unity_topK_, K);
    EXPECT_EQ(result->distances_.size(), NQ * K);
    EXPECT_EQ(result->seg_offsets_.size(), NQ * K);
    if (vec_field_data_type == DataType::VECTOR_FLOAT) {
        EXPECT_EQ(result->seg_offsets_[0], query_offset);
    }
}
