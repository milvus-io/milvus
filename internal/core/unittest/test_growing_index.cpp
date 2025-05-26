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

#include <gtest/gtest.h>

#include "common/Utils.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
namespace pb = milvus::proto;

using Param = std::tuple</*index type*/ std::string, knowhere::MetricType>;

class GrowingIndexTest : public ::testing::TestWithParam<Param> {
    void
    SetUp() override {
        auto param = GetParam();
        index_type = std::get<0>(param);
        metric_type = std::get<1>(param);
        if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
            index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
            data_type = DataType::VECTOR_FLOAT;
        } else if (index_type ==
                       knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
                   index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
            data_type = DataType::VECTOR_SPARSE_FLOAT;
            is_sparse = true;
        } else {
            ASSERT_TRUE(false);
        }
    }

 protected:
    std::string index_type;
    knowhere::MetricType metric_type;
    DataType data_type;
    bool is_sparse = false;
};

INSTANTIATE_TEST_SUITE_P(
    FloatIndexTypeParameters,
    GrowingIndexTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                          knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC),
        ::testing::Values(knowhere::metric::L2,
                          knowhere::metric::COSINE,
                          knowhere::metric::IP)));

INSTANTIATE_TEST_SUITE_P(
    SparseIndexTypeParameters,
    GrowingIndexTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                          knowhere::IndexEnum::INDEX_SPARSE_WAND),
        ::testing::Values(knowhere::metric::IP)));

TEST_P(GrowingIndexTest, Correctness) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(226985, std::move(filedMap));
    auto segment = CreateGrowingSegment(schema, metaPtr);
    auto segmentImplPtr = dynamic_cast<SegmentGrowingImpl*>(segment.get());

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    if (is_sparse) {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::SparseFloatVector);
    } else {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::FloatVector);
    }
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(102);
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(5);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric_type);
    query_info->set_search_params(R"({"nprobe": 16})");
    auto plan_str = plan_node.SerializeAsString();

    milvus::proto::plan::PlanNode range_query_plan_node;
    auto vector_range_querys = range_query_plan_node.mutable_vector_anns();
    if (is_sparse) {
        vector_range_querys->set_vector_type(
            milvus::proto::plan::VectorType::SparseFloatVector);
    } else {
        vector_range_querys->set_vector_type(
            milvus::proto::plan::VectorType::FloatVector);
    }
    vector_range_querys->set_placeholder_tag("$0");
    vector_range_querys->set_field_id(102);
    auto range_query_info = vector_range_querys->mutable_query_info();
    range_query_info->set_topk(5);
    range_query_info->set_round_decimal(3);
    range_query_info->set_metric_type(metric_type);
    if (PositivelyRelated(metric_type)) {
        range_query_info->set_search_params(
            R"({"nprobe": 10, "radius": 500, "range_filter": 600})");
    } else {
        range_query_info->set_search_params(
            R"({"nprobe": 10, "radius": 600, "range_filter": 500})");
    }
    auto range_plan_str = range_query_plan_node.SerializeAsString();

    int64_t per_batch = 10000;
    int64_t n_batch = 20;
    int64_t top_k = 5;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);
        auto offset = segment->PreInsert(per_batch);
        auto pks = dataset.get_col<int64_t>(pk);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        const VectorBase* field_data = nullptr;
        if (is_sparse) {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::SparseFloatVector>(vec);
        } else {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::FloatVector>(vec);
        }

        auto inserted = (i + 1) * per_batch;
        // once index built, chunk data will be removed.
        // growing index will only be built when num rows reached
        // get_build_threshold(). This value for sparse is 0, thus sparse index
        // will be built since the first chunk. Dense segment buffers the first
        // 2 chunks before building an index in this test case.
        if (!is_sparse && i < 2) {
            EXPECT_EQ(field_data->num_chunk(),
                      upper_div(inserted, field_data->get_size_per_chunk()));
        } else {
            EXPECT_EQ(field_data->num_chunk(), 0);
        }

        auto num_queries = 5;
        auto ph_group_raw =
            is_sparse ? CreateSparseFloatPlaceholderGroup(num_queries)
                      : CreatePlaceholderGroup(num_queries, 128, 1024);

        auto plan = milvus::query::CreateSearchPlanByExpr(
            *schema, plan_str.data(), plan_str.size());
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        Timestamp timestamp = 1000000;
        auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
        EXPECT_EQ(sr->total_nq_, num_queries);
        EXPECT_EQ(sr->unity_topK_, top_k);
        EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
        EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);

        // range search for sparse is not yet supported
        if (is_sparse) {
            continue;
        }
        auto range_plan = milvus::query::CreateSearchPlanByExpr(
            *schema, range_plan_str.data(), range_plan_str.size());
        auto range_ph_group = ParsePlaceholderGroup(
            range_plan.get(), ph_group_raw.SerializeAsString());
        auto range_sr =
            segment->Search(range_plan.get(), range_ph_group.get(), timestamp);
        ASSERT_EQ(range_sr->total_nq_, num_queries);
        EXPECT_EQ(sr->unity_topK_, top_k);
        EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
        EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
        for (int j = 0; j < range_sr->seg_offsets_.size(); j++) {
            if (range_sr->seg_offsets_[j] != -1) {
                EXPECT_TRUE(sr->distances_[j] >= 500.0 &&
                            sr->distances_[j] <= 600.0);
            }
        }
    }
}

TEST_P(GrowingIndexTest, AddWithoutBuildPool) {
    constexpr int N = 1024;
    constexpr int TOPK = 100;
    constexpr int dim = 128;
    constexpr int add_cont = 5;

    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.metric_type = metric_type;
    create_index_info.index_type = index_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(pk);

    auto dataset = DataGen(schema, N);

    auto build_config = generate_build_conf(index_type, metric_type);

    if (data_type == DataType::VECTOR_FLOAT) {
        auto index = std::make_unique<milvus::index::VectorMemIndex<float>>(
            index_type,
            metric_type,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            false,
            milvus::storage::FileManagerContext());
        auto float_data = dataset.get_col<float>(vec);
        index->BuildWithDataset(knowhere::GenDataSet(N, dim, float_data.data()),
                                build_config);
        for (int i = 0; i < add_cont; i++) {
            index->AddWithDataset(
                knowhere::GenDataSet(N, dim, float_data.data()), build_config);
        }
        EXPECT_EQ(index->Count(), (add_cont + 1) * N);
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        auto index = std::make_unique<milvus::index::VectorMemIndex<float16>>(
            index_type,
            metric_type,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            false,
            milvus::storage::FileManagerContext());
        auto float16_data = dataset.get_col<float16>(vec);
        index->BuildWithDataset(
            knowhere::GenDataSet(N, dim, float16_data.data()), build_config);
        for (int i = 0; i < add_cont; i++) {
            index->AddWithDataset(
                knowhere::GenDataSet(N, dim, float16_data.data()),
                build_config);
        }
        EXPECT_EQ(index->Count(), (add_cont + 1) * N);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        auto index = std::make_unique<milvus::index::VectorMemIndex<bfloat16>>(
            index_type,
            metric_type,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            false,
            milvus::storage::FileManagerContext());
        auto bfloat16_data = dataset.get_col<bfloat16>(vec);
        index->BuildWithDataset(
            knowhere::GenDataSet(N, dim, bfloat16_data.data()), build_config);
        for (int i = 0; i < add_cont; i++) {
            index->AddWithDataset(
                knowhere::GenDataSet(N, dim, bfloat16_data.data()),
                build_config);
        }
        EXPECT_EQ(index->Count(), (add_cont + 1) * N);
    } else if (is_sparse) {
        auto index = std::make_unique<milvus::index::VectorMemIndex<float>>(
            index_type,
            metric_type,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            false,
            milvus::storage::FileManagerContext());
        auto sparse_data =
            dataset.get_col<knowhere::sparse::SparseRow<float>>(vec);
        index->BuildWithDataset(
            knowhere::GenDataSet(N, dim, sparse_data.data()), build_config);
        for (int i = 0; i < add_cont; i++) {
            index->AddWithDataset(
                knowhere::GenDataSet(N, dim, sparse_data.data()), build_config);
        }
        EXPECT_EQ(index->Count(), (add_cont + 1) * N);
    } else {
        throw std::invalid_argument("Unsupported data type");
    }
}

TEST_P(GrowingIndexTest, MissIndexMeta) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    auto segment = CreateGrowingSegment(schema, nullptr);
}

TEST_P(GrowingIndexTest, GetVector) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto random = schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    if (data_type == DataType::VECTOR_FLOAT) {
        // GetVector for VECTOR_FLOAT
        int64_t per_batch = 5000;
        int64_t n_batch = 20;
        int64_t dim = 128;
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, per_batch);
            auto fakevec = dataset.get_col<float>(vec);
            auto offset = segment->PreInsert(per_batch);
            segment->Insert(offset,
                            per_batch,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            auto num_inserted = (i + 1) * per_batch;
            auto ids_ds = GenRandomIds(num_inserted);
            auto result =
                segment->bulk_subscript(vec, ids_ds->GetIds(), num_inserted);

            auto vector =
                result.get()->mutable_vectors()->float_vector().data();
            EXPECT_TRUE(vector.size() == num_inserted * dim);
            for (size_t i = 0; i < num_inserted; ++i) {
                auto id = ids_ds->GetIds()[i];
                for (size_t j = 0; j < 128; ++j) {
                    EXPECT_TRUE(vector[i * dim + j] ==
                                fakevec[(id % per_batch) * dim + j]);
                }
            }
        }
    } else if (is_sparse) {
        // GetVector for VECTOR_SPARSE_FLOAT
        int64_t per_batch = 5000;
        int64_t n_batch = 20;
        int64_t dim = 128;
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, per_batch);
            auto fakevec =
                dataset.get_col<knowhere::sparse::SparseRow<float>>(vec);
            auto offset = segment->PreInsert(per_batch);
            segment->Insert(offset,
                            per_batch,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            auto num_inserted = (i + 1) * per_batch;
            auto ids_ds = GenRandomIds(num_inserted);
            auto result =
                segment->bulk_subscript(vec, ids_ds->GetIds(), num_inserted);

            auto vector = result.get()
                              ->mutable_vectors()
                              ->sparse_float_vector()
                              .contents();
            EXPECT_TRUE(result.get()
                            ->mutable_vectors()
                            ->sparse_float_vector()
                            .contents_size() == num_inserted);
            auto sparse_rows = SparseBytesToRows(vector);
            for (size_t i = 0; i < num_inserted; ++i) {
                auto id = ids_ds->GetIds()[i];
                auto actual_row = sparse_rows[i];
                auto expected_row = fakevec[(id % per_batch)];
                EXPECT_TRUE(actual_row.size() == expected_row.size());
                for (size_t j = 0; j < actual_row.size(); ++j) {
                    EXPECT_TRUE(actual_row[j].id == expected_row[j].id);
                    EXPECT_TRUE(actual_row[j].val == expected_row[j].val);
                }
            }
        }
    }
}
