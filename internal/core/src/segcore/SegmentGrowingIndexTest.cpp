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

#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/TypeTraits.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/IndexInfo.h"
#include "index/VectorMemIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/object.h"
#include "knowhere/operands.h"
#include "knowhere/sparse_utils.h"
#include "knowhere/version.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "storage/FileManager.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

using Param = std::tuple<DataType,
                         /*index type*/ std::string,
                         knowhere::MetricType,
                         /*dense vector index type*/ std::optional<std::string>,
                         /*refine type*/ std::optional<std::string>>;

class GrowingIndexTest : public ::testing::TestWithParam<Param> {
    void
    SetUp() override {
        auto param = GetParam();
        data_type = std::get<0>(param);
        index_type = std::get<1>(param);
        metric_type = std::get<2>(param);
        dense_vec_intermin_index_type = std::get<3>(param);
        dense_refine_type = std::get<4>(param);
        if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            is_sparse = true;
            if (metric_type == knowhere::metric::IP) {
                intermin_index_with_raw_data = true;
            } else {
                intermin_index_with_raw_data = false;
            }
        } else {
            if (!dense_vec_intermin_index_type.has_value()) {
                dense_vec_intermin_index_type =
                    knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
            }
            if (dense_vec_intermin_index_type.value() ==
                knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
                intermin_index_with_raw_data = true;
            } else {
                // scann dvr index
                intermin_index_with_raw_data = false;
            }
        }
    }

 protected:
    std::string index_type;
    knowhere::MetricType metric_type;
    DataType data_type;
    std::optional<std::string> dense_vec_intermin_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    bool intermin_index_with_raw_data;
    bool is_sparse = false;
    std::optional<std::string> dense_refine_type = "NONE";
};

INSTANTIATE_TEST_SUITE_P(
    FloatIndexTypeParameters,
    GrowingIndexTest,
    ::testing::Values(
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::L2,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                        std::nullopt),
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                        std::nullopt),
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::L2,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "NONE"),
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "NONE"),
        std::make_tuple(DataType::VECTOR_FLOAT,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::L2,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "FLOAT16")));

INSTANTIATE_TEST_SUITE_P(
    SparseIndexTypeParameters,
    GrowingIndexTest,
    ::testing::Combine(
        ::testing::Values(DataType::VECTOR_SPARSE_U32_F32),
        // VecIndexConfig will convert INDEX_SPARSE_INVERTED_INDEX/
        // INDEX_SPARSE_WAND to INDEX_SPARSE_INVERTED_INDEX_CC/
        // INDEX_SPARSE_WAND_CC, thus no need to use _CC version here.
        ::testing::Values(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                          knowhere::IndexEnum::INDEX_SPARSE_WAND),
        ::testing::Values(
            knowhere::metric::
                IP),  // when metric == IP, growing segment will keep data in intermin index
        ::testing::Values(std::nullopt),
        ::testing::Values(std::nullopt)));

INSTANTIATE_TEST_SUITE_P(
    HalfFloatIndexTypeParameters,
    GrowingIndexTest,
    ::testing::Values(
        std::make_tuple(DataType::VECTOR_FLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                        std::nullopt),
        std::make_tuple(DataType::VECTOR_BFLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC,
                        std::nullopt),
        std::make_tuple(DataType::VECTOR_FLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "NONE"),
        std::make_tuple(DataType::VECTOR_BFLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "NONE"),
        std::make_tuple(DataType::VECTOR_FLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "FLOAT16"),
        std::make_tuple(DataType::VECTOR_BFLOAT16,
                        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                        knowhere::metric::COSINE,
                        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR,
                        "FLOAT16")));

TEST_P(GrowingIndexTest, Correctness) {
    auto dim = 4;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, dim, metric_type);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 1024;
    interim_config.dense_vector_interim_index_type =
        dense_vec_intermin_index_type;
    if (dense_vec_intermin_index_type.has_value() &&
        dense_vec_intermin_index_type.value() ==
            knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        interim_config.nprobe = int(0.4 * config.get_nlist());
        interim_config.sub_dim = 4;
        interim_config.refine_ratio = 4.0F;
        if (dense_refine_type.has_value()) {
            interim_config.refine_quant_type = dense_refine_type.value();
            interim_config.refine_with_quant_flag = false;
        }
    }
    ApplyInterimIndexConfigForTest(interim_config, config);
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
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::Float16Vector);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        vector_anns->set_vector_type(
            milvus::proto::plan::VectorType::BFloat16Vector);
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
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        vector_range_querys->set_vector_type(
            milvus::proto::plan::VectorType::Float16Vector);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        vector_range_querys->set_vector_type(
            milvus::proto::plan::VectorType::BFloat16Vector);
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
    int64_t n_batch = 5;
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
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::Float16Vector>(vec);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::BFloat16Vector>(vec);
        } else {
            field_data = segmentImplPtr->get_insert_record()
                             .get_data<milvus::FloatVector>(vec);
        }

        auto inserted = (i + 1) * per_batch;
        // once index built, chunk data will be removed.
        // growing index will only be built when num rows reached
        // get_build_threshold(). Both sparse and dense segment buffer the first
        // 2 chunks before building an index in this test case.

        if (i < 2 || !intermin_index_with_raw_data) {
            EXPECT_EQ(field_data->num_chunk(),
                      upper_div(inserted, field_data->get_size_per_chunk()));
        } else {
            EXPECT_EQ(field_data->num_chunk(), 0);
        }
        auto num_queries = 5;
        namespace ser = milvus::proto::common;
        ser::PlaceholderGroup ph_group_raw;
        if (is_sparse) {
            ph_group_raw = CreateSparseFloatPlaceholderGroup(num_queries);
        } else if (data_type == DataType::VECTOR_FLOAT16) {
            ph_group_raw = CreatePlaceholderGroup<milvus::Float16Vector>(
                num_queries, dim, 1024);
        } else if (data_type == DataType::VECTOR_BFLOAT16) {
            ph_group_raw = CreatePlaceholderGroup<milvus::BFloat16Vector>(
                num_queries, dim, 1024);
        } else {
            ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024);
        }

        auto plan = milvus::query::CreateSearchPlanByExpr(
            schema, plan_str.data(), plan_str.size());
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
            schema, range_plan_str.data(), range_plan_str.size());
        auto range_ph_group = ParsePlaceholderGroup(
            range_plan.get(), ph_group_raw.SerializeAsString());
        auto range_sr =
            segment->Search(range_plan.get(), range_ph_group.get(), timestamp);
        ASSERT_EQ(range_sr->total_nq_, num_queries);
        for (int j = 0; j < range_sr->seg_offsets_.size(); j++) {
            if (range_sr->seg_offsets_[j] != -1) {
                EXPECT_TRUE(range_sr->distances_[j] >= 500.0 &&
                            range_sr->distances_[j] <= 600.0);
            }
        }
    }
}

TEST_P(GrowingIndexTest, AddWithoutBuildPool) {
    constexpr int N = 1024;
    constexpr int dim = 4;
    constexpr int add_cont = 5;

    milvus::index::CreateIndexInfo create_index_info;
    create_index_info.field_type = data_type;
    create_index_info.metric_type = metric_type;
    create_index_info.index_type = index_type;
    create_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, dim, metric_type);
    schema->set_primary_field_id(pk);

    auto dataset = DataGen(schema, N);

    auto build_config = generate_build_conf(index_type, metric_type);

    if (data_type == DataType::VECTOR_FLOAT) {
        auto index = std::make_unique<milvus::index::VectorMemIndex<float>>(
            DataType::NONE,
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
            DataType::NONE,
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
            DataType::NONE,
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
        // Use the CC (concurrent) variant of sparse index types, since
        // non-CC sparse indices do not support incremental Add() after
        // the initial Build().
        auto cc_index_type =
            (index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND)
                ? knowhere::IndexEnum::INDEX_SPARSE_WAND_CC
                : knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX_CC;
        auto index =
            std::make_unique<milvus::index::VectorMemIndex<sparse_u32_f32>>(
                DataType::NONE,
                cc_index_type,
                metric_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                false,
                milvus::storage::FileManagerContext());
        auto sparse_data =
            dataset
                .get_col<knowhere::sparse::SparseRow<milvus::SparseValueType>>(
                    vec);
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

TEST(GrowingIndexNullableVectorTest,
     ScannDvrRefinerUsesCompactPhysicalVectorIds) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 50;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta field_index_meta(
        vec, std::move(index_params), std::move(type_params));

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 1024;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR;
    interim_config.sub_dim = dim;
    interim_config.refine_ratio = 1.0F;
    interim_config.refine_quant_type = "NONE";
    interim_config.refine_with_quant_flag = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    std::map<FieldId, FieldIndexMeta> field_map = {{vec, field_index_meta}};
    IndexMetaPtr meta =
        std::make_shared<CollectionIndexMeta>(100, std::move(field_map));
    auto segment_growing = CreateGrowingSegment(schema, meta, 1, config);

    std::vector<int64_t> pks(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count, 100);
    for (int64_t i = 0; i < row_count; ++i) {
        pks[i] = i;
        row_ids[i] = i;
    }

    FixedVector<bool> valid_data(row_count);
    std::fill(valid_data.begin(), valid_data.end(), true);
    valid_data[0] = false;

    std::vector<float> compact_vectors((row_count - 1) * dim, 1000.0f);
    // logical row 1 -> physical 0, intentionally far from the query.
    compact_vectors[0] = 1000.0f;
    compact_vectors[1] = 0.0f;
    compact_vectors[2] = 0.0f;
    compact_vectors[3] = 0.0f;
    // logical row 2 -> physical 1, exactly equal to the query.
    compact_vectors[dim] = 0.0f;
    compact_vectors[dim + 1] = 0.0f;
    compact_vectors[dim + 2] = 0.0f;
    compact_vectors[dim + 3] = 0.0f;

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
    auto vec_array = CreateVectorDataArrayFrom(compact_vectors.data(),
                                               valid_data.data(),
                                               row_count,
                                               row_count - 1,
                                               (*schema)[vec]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
    insert_data->set_num_rows(row_count);

    auto reserved_offset = segment_growing->PreInsert(row_count);
    segment_growing->Insert(reserved_offset,
                            row_count,
                            row_ids.data(),
                            timestamps.data(),
                            insert_data.get());

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch(
        "", "embeddings", 5, knowhere::metric::L2, R"({"nprobe": 1})", -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {0.0f, 0.0f, 0.0f, 0.0f};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(1, dim, query.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment_growing->Search(plan.get(), ph_group.get(), 100);

    ASSERT_EQ(result->seg_offsets_.size(), 5);
    ASSERT_EQ(result->distances_.size(), 5);
    bool found_exact_query_row = false;
    for (size_t i = 0; i < result->seg_offsets_.size(); ++i) {
        if (result->seg_offsets_[i] == 2) {
            EXPECT_FLOAT_EQ(result->distances_[i], 0.0f);
            found_exact_query_row = true;
        }
    }
    EXPECT_TRUE(found_exact_query_row);
}

TEST(GrowingIndexNullableVectorTest,
     IndexedSearchUsesTimestampBitsetForAppendedTail) {
    constexpr int64_t dim = 4;
    constexpr int64_t first_batch_rows = 50;
    constexpr int64_t tail_rows = 6;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta field_index_meta(
        vec, std::move(index_params), std::move(type_params));

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 1024;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    ApplyInterimIndexConfigForTest(interim_config, config);

    std::map<FieldId, FieldIndexMeta> field_map = {{vec, field_index_meta}};
    IndexMetaPtr meta =
        std::make_shared<CollectionIndexMeta>(100, std::move(field_map));
    auto segment_growing = CreateGrowingSegment(schema, meta, 1, config);

    auto insert_batch = [&](int64_t row_begin,
                            int64_t row_count,
                            Timestamp ts,
                            const FixedVector<bool>& valid_data,
                            const std::vector<float>& compact_vectors) {
        std::vector<int64_t> pks(row_count);
        std::vector<idx_t> row_ids(row_count);
        std::vector<Timestamp> timestamps(row_count, ts);
        for (int64_t i = 0; i < row_count; ++i) {
            pks[i] = row_begin + i;
            row_ids[i] = row_begin + i;
        }

        auto valid_count = static_cast<int64_t>(
            std::count(valid_data.begin(), valid_data.end(), true));
        auto insert_data = std::make_unique<InsertRecordProto>();
        auto pk_array =
            CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
        auto vec_array = CreateVectorDataArrayFrom(compact_vectors.data(),
                                                   valid_data.data(),
                                                   row_count,
                                                   valid_count,
                                                   (*schema)[vec]);
        insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
        insert_data->set_num_rows(row_count);

        auto reserved_offset = segment_growing->PreInsert(row_count);
        segment_growing->Insert(reserved_offset,
                                row_count,
                                row_ids.data(),
                                timestamps.data(),
                                insert_data.get());
    };

    FixedVector<bool> first_valid(first_batch_rows);
    std::fill(first_valid.begin(), first_valid.end(), true);
    first_valid[0] = false;
    const auto first_valid_count = static_cast<int64_t>(
        std::count(first_valid.begin(), first_valid.end(), true));
    std::vector<float> first_vectors(first_valid_count * dim, 100.0F);
    insert_batch(0, first_batch_rows, 100, first_valid, first_vectors);

    FixedVector<bool> tail_valid(tail_rows);
    std::fill(tail_valid.begin(), tail_valid.end(), true);
    tail_valid[1] = false;
    tail_valid[tail_rows - 1] = false;
    const auto tail_valid_count = static_cast<int64_t>(
        std::count(tail_valid.begin(), tail_valid.end(), true));
    std::vector<float> tail_vectors(tail_valid_count * dim, 200.0F);
    std::fill(tail_vectors.begin(), tail_vectors.begin() + dim, 0.0F);
    insert_batch(first_batch_rows, tail_rows, 200, tail_valid, tail_vectors);

    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);
    ASSERT_TRUE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 5> retrieve_offsets = {
        0,
        1,
        first_batch_rows,
        first_batch_rows + 1,
        first_batch_rows + tail_rows - 1};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    EXPECT_FALSE(retrieve_result->valid_data(0));
    EXPECT_TRUE(retrieve_result->valid_data(1));
    EXPECT_TRUE(retrieve_result->valid_data(2));
    EXPECT_FALSE(retrieve_result->valid_data(3));
    EXPECT_FALSE(retrieve_result->valid_data(4));

    const auto& retrieved_values =
        retrieve_result->vectors().float_vector().data();
    ASSERT_EQ(retrieved_values.size(), 2 * dim);
    for (int64_t d = 0; d < dim; ++d) {
        EXPECT_FLOAT_EQ(retrieved_values[d], 100.0F);
        EXPECT_FLOAT_EQ(retrieved_values[dim + d], 0.0F);
    }

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch(
        "", "embeddings", 3, knowhere::metric::L2, R"({"nprobe": 1})", -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {0.0F, 0.0F, 0.0F, 0.0F};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(1, dim, query.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto old_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(old_snapshot_result->seg_offsets_.size(), 3);
    for (auto offset : old_snapshot_result->seg_offsets_) {
        ASSERT_GE(offset, 0);
        EXPECT_LT(offset, first_batch_rows);
    }

    auto full_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 200);
    bool found_tail = false;
    for (size_t i = 0; i < full_snapshot_result->seg_offsets_.size(); ++i) {
        EXPECT_NE(full_snapshot_result->seg_offsets_[i], first_batch_rows + 1);
        EXPECT_NE(full_snapshot_result->seg_offsets_[i],
                  first_batch_rows + tail_rows - 1);
        if (full_snapshot_result->seg_offsets_[i] == first_batch_rows) {
            EXPECT_FLOAT_EQ(full_snapshot_result->distances_[i], 0.0F);
            found_tail = true;
        }
    }
    EXPECT_TRUE(found_tail);
}

TEST(GrowingIndexNullableVectorTest,
     BruteForceSearchAndGetReturnEmptyForAllNullVector) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 6;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.enable_interim_segment_index = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);

    std::vector<int64_t> pks(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count, 100);
    for (int64_t i = 0; i < row_count; ++i) {
        pks[i] = i;
        row_ids[i] = i;
    }

    FixedVector<bool> valid_data(row_count);
    std::fill(valid_data.begin(), valid_data.end(), false);
    std::vector<float> compact_vectors;

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
    auto vec_array = CreateVectorDataArrayFrom(compact_vectors.data(),
                                               valid_data.data(),
                                               row_count,
                                               0,
                                               (*schema)[vec]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
    insert_data->set_num_rows(row_count);

    auto reserved_offset = segment_growing->PreInsert(row_count);
    segment_growing->Insert(reserved_offset,
                            row_count,
                            row_ids.data(),
                            timestamps.data(),
                            insert_data.get());

    ASSERT_FALSE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 3> retrieve_offsets = {0, 3, row_count - 1};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    for (int i = 0; i < retrieve_result->valid_data_size(); ++i) {
        EXPECT_FALSE(retrieve_result->valid_data(i));
    }
    EXPECT_TRUE(retrieve_result->vectors().float_vector().data().empty());

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch(
        "", "embeddings", 3, knowhere::metric::L2, R"({"nprobe": 1})", -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {0.0F, 0.0F, 0.0F, 0.0F};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(1, dim, query.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(result->seg_offsets_.size(), 3);
    ASSERT_EQ(result->distances_.size(), 3);
    for (size_t i = 0; i < result->seg_offsets_.size(); ++i) {
        EXPECT_EQ(result->seg_offsets_[i], INVALID_SEG_OFFSET);
        EXPECT_FLOAT_EQ(result->distances_[i], 0.0F);
    }
}

TEST(GrowingIndexNullableVectorTest,
     BruteForceSearchUsesLogicalBitsetForNullableVector) {
    constexpr int64_t dim = 4;
    constexpr int64_t first_batch_rows = 10;
    constexpr int64_t tail_rows = 3;
    constexpr int64_t filtered_logical_row = 2;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.enable_interim_segment_index = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);

    auto insert_batch = [&](int64_t row_begin,
                            int64_t row_count,
                            Timestamp ts,
                            const FixedVector<bool>& valid_data,
                            const std::vector<float>& compact_vectors) {
        std::vector<int64_t> pks(row_count);
        std::vector<idx_t> row_ids(row_count);
        std::vector<Timestamp> timestamps(row_count, ts);
        for (int64_t i = 0; i < row_count; ++i) {
            pks[i] = row_begin + i;
            row_ids[i] = row_begin + i;
        }

        auto valid_count = static_cast<int64_t>(
            std::count(valid_data.begin(), valid_data.end(), true));
        auto insert_data = std::make_unique<InsertRecordProto>();
        auto pk_array =
            CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
        auto vec_array = CreateVectorDataArrayFrom(compact_vectors.data(),
                                                   valid_data.data(),
                                                   row_count,
                                                   valid_count,
                                                   (*schema)[vec]);
        insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
        insert_data->set_num_rows(row_count);

        auto reserved_offset = segment_growing->PreInsert(row_count);
        segment_growing->Insert(reserved_offset,
                                row_count,
                                row_ids.data(),
                                timestamps.data(),
                                insert_data.get());
    };

    FixedVector<bool> first_valid(first_batch_rows);
    std::fill(first_valid.begin(), first_valid.end(), true);
    first_valid[0] = false;
    first_valid[4] = false;
    const auto first_valid_count = static_cast<int64_t>(
        std::count(first_valid.begin(), first_valid.end(), true));
    std::vector<float> first_vectors(first_valid_count * dim, 100.0F);
    int64_t physical_row = 0;
    for (int64_t logical_row = 0; logical_row < first_batch_rows;
         ++logical_row) {
        if (!first_valid[logical_row]) {
            continue;
        }
        if (logical_row == filtered_logical_row) {
            std::fill(first_vectors.begin() + physical_row * dim,
                      first_vectors.begin() + (physical_row + 1) * dim,
                      0.0F);
        }
        ++physical_row;
    }
    insert_batch(0, first_batch_rows, 100, first_valid, first_vectors);

    FixedVector<bool> tail_valid(tail_rows);
    std::fill(tail_valid.begin(), tail_valid.end(), true);
    std::vector<float> tail_vectors(tail_rows * dim, 200.0F);
    std::fill(tail_vectors.begin(), tail_vectors.begin() + dim, 0.0F);
    insert_batch(first_batch_rows, tail_rows, 200, tail_valid, tail_vectors);

    ASSERT_FALSE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 5> retrieve_offsets = {0, 2, 4, 10, 12};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    EXPECT_FALSE(retrieve_result->valid_data(0));
    EXPECT_TRUE(retrieve_result->valid_data(1));
    EXPECT_FALSE(retrieve_result->valid_data(2));
    EXPECT_TRUE(retrieve_result->valid_data(3));
    EXPECT_TRUE(retrieve_result->valid_data(4));

    const auto& retrieved_values =
        retrieve_result->vectors().float_vector().data();
    ASSERT_EQ(retrieved_values.size(), 3 * dim);
    for (int64_t i = 0; i < dim; ++i) {
        EXPECT_FLOAT_EQ(retrieved_values[i], 0.0F);
        EXPECT_FLOAT_EQ(retrieved_values[dim + i], 0.0F);
        EXPECT_FLOAT_EQ(retrieved_values[2 * dim + i], 200.0F);
    }

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch("pk != 2",
                                              "embeddings",
                                              3,
                                              knowhere::metric::L2,
                                              R"({"nprobe": 1})",
                                              -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {0.0F, 0.0F, 0.0F, 0.0F};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob(1, dim, query.data());
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto old_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(old_snapshot_result->seg_offsets_.size(), 3);
    for (auto offset : old_snapshot_result->seg_offsets_) {
        ASSERT_GE(offset, 0);
        EXPECT_LT(offset, first_batch_rows);
        EXPECT_NE(offset, filtered_logical_row);
        EXPECT_TRUE(first_valid[offset]);
    }

    auto full_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 200);
    bool found_tail = false;
    for (size_t i = 0; i < full_snapshot_result->seg_offsets_.size(); ++i) {
        EXPECT_NE(full_snapshot_result->seg_offsets_[i], filtered_logical_row);
        if (full_snapshot_result->seg_offsets_[i] == first_batch_rows) {
            EXPECT_FLOAT_EQ(full_snapshot_result->distances_[i], 0.0F);
            found_tail = true;
        }
    }
    EXPECT_TRUE(found_tail);

    auto iterator_plan_str =
        schema_handle.ParseSearchIterator("pk != 2",
                                          "embeddings",
                                          3,
                                          knowhere::metric::L2,
                                          R"({"nprobe": 1})",
                                          3,
                                          "",
                                          std::nullopt,
                                          -1);
    auto iterator_plan = query::CreateSearchPlanByExpr(
        schema, iterator_plan_str.data(), iterator_plan_str.size());
    auto iterator_ph_group = ParsePlaceholderGroup(
        iterator_plan.get(), ph_group_raw.SerializeAsString());

    auto iterator_old_snapshot_result = segment_growing->Search(
        iterator_plan.get(), iterator_ph_group.get(), 100);
    ASSERT_EQ(iterator_old_snapshot_result->seg_offsets_.size(), 3);
    for (auto offset : iterator_old_snapshot_result->seg_offsets_) {
        ASSERT_GE(offset, 0);
        EXPECT_LT(offset, first_batch_rows);
        EXPECT_NE(offset, filtered_logical_row);
        EXPECT_TRUE(first_valid[offset]);
    }

    auto iterator_full_snapshot_result = segment_growing->Search(
        iterator_plan.get(), iterator_ph_group.get(), 200);
    bool iterator_found_tail = false;
    for (size_t i = 0; i < iterator_full_snapshot_result->seg_offsets_.size();
         ++i) {
        EXPECT_NE(iterator_full_snapshot_result->seg_offsets_[i],
                  filtered_logical_row);
        if (iterator_full_snapshot_result->seg_offsets_[i] ==
            first_batch_rows) {
            EXPECT_FLOAT_EQ(iterator_full_snapshot_result->distances_[i], 0.0F);
            iterator_found_tail = true;
        }
    }
    EXPECT_TRUE(iterator_found_tail);
}

TEST(GrowingIndexNullableVectorTest,
     BruteForceSearchUsesLogicalBitsetForNullableEmbList) {
    constexpr int64_t dim = 4;
    constexpr int64_t first_batch_rows = 10;
    constexpr int64_t tail_rows = 3;
    constexpr int64_t filtered_logical_row = 2;

    auto make_emb_list = [](int64_t dim, std::initializer_list<float> values) {
        VectorFieldProto row;
        row.set_dim(dim);
        auto* data = row.mutable_float_vector()->mutable_data();
        data->Add(values.begin(), values.end());
        return row;
    };

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugVectorArrayField("embeddings",
                                                DataType::VECTOR_FLOAT,
                                                dim,
                                                knowhere::metric::MAX_SIM_IP,
                                                true);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.enable_interim_segment_index = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);

    auto insert_batch =
        [&](int64_t row_begin,
            int64_t row_count,
            Timestamp ts,
            const FixedVector<bool>& valid_data,
            const std::vector<VectorFieldProto>& compact_lists) {
            std::vector<int64_t> pks(row_count);
            std::vector<idx_t> row_ids(row_count);
            std::vector<Timestamp> timestamps(row_count, ts);
            for (int64_t i = 0; i < row_count; ++i) {
                pks[i] = row_begin + i;
                row_ids[i] = row_begin + i;
            }

            auto valid_count = static_cast<int64_t>(
                std::count(valid_data.begin(), valid_data.end(), true));
            ASSERT_EQ(valid_count, static_cast<int64_t>(compact_lists.size()));

            auto insert_data = std::make_unique<InsertRecordProto>();
            auto pk_array = CreateDataArrayFrom(
                pks.data(), nullptr, row_count, (*schema)[pk]);
            auto vec_array = CreateVectorDataArrayFrom(compact_lists.data(),
                                                       valid_data.data(),
                                                       row_count,
                                                       valid_count,
                                                       (*schema)[vec]);
            insert_data->mutable_fields_data()->AddAllocated(
                pk_array.release());
            insert_data->mutable_fields_data()->AddAllocated(
                vec_array.release());
            insert_data->set_num_rows(row_count);

            auto reserved_offset = segment_growing->PreInsert(row_count);
            segment_growing->Insert(reserved_offset,
                                    row_count,
                                    row_ids.data(),
                                    timestamps.data(),
                                    insert_data.get());
        };

    FixedVector<bool> first_valid(first_batch_rows);
    std::fill(first_valid.begin(), first_valid.end(), true);
    first_valid[0] = false;
    first_valid[4] = false;

    std::vector<VectorFieldProto> first_lists;
    for (int64_t logical_row = 0; logical_row < first_batch_rows;
         ++logical_row) {
        if (!first_valid[logical_row]) {
            continue;
        }
        if (logical_row == filtered_logical_row) {
            first_lists.push_back(make_emb_list(
                dim, {20.0F, 0.0F, 0.0F, 0.0F, 19.0F, 0.0F, 0.0F, 0.0F}));
        } else {
            first_lists.push_back(make_emb_list(
                dim, {-1.0F, 0.0F, 0.0F, 0.0F, -2.0F, 0.0F, 0.0F, 0.0F}));
        }
    }
    insert_batch(0, first_batch_rows, 100, first_valid, first_lists);

    FixedVector<bool> tail_valid(tail_rows);
    std::fill(tail_valid.begin(), tail_valid.end(), true);
    std::vector<VectorFieldProto> tail_lists;
    tail_lists.push_back(
        make_emb_list(dim, {30.0F, 0.0F, 0.0F, 0.0F, 29.0F, 0.0F, 0.0F, 0.0F}));
    for (int64_t i = 1; i < tail_rows; ++i) {
        tail_lists.push_back(make_emb_list(
            dim, {1.0F, 0.0F, 0.0F, 0.0F, 0.5F, 0.0F, 0.0F, 0.0F}));
    }
    insert_batch(first_batch_rows, tail_rows, 200, tail_valid, tail_lists);

    ASSERT_FALSE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 5> retrieve_offsets = {0, 2, 4, 10, 12};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    EXPECT_FALSE(retrieve_result->valid_data(0));
    EXPECT_TRUE(retrieve_result->valid_data(1));
    EXPECT_FALSE(retrieve_result->valid_data(2));
    EXPECT_TRUE(retrieve_result->valid_data(3));
    EXPECT_TRUE(retrieve_result->valid_data(4));

    const auto& retrieved_lists =
        retrieve_result->vectors().vector_array().data();
    ASSERT_EQ(retrieved_lists.size(), retrieve_offsets.size());
    EXPECT_TRUE(retrieved_lists.Get(0).float_vector().data().empty());
    EXPECT_TRUE(retrieved_lists.Get(2).float_vector().data().empty());

    auto expect_emb_list = [](const VectorFieldProto& row,
                              std::initializer_list<float> expected) {
        const auto& values = row.float_vector().data();
        ASSERT_EQ(values.size(), expected.size());
        size_t i = 0;
        for (auto value : expected) {
            EXPECT_FLOAT_EQ(values.Get(i++), value);
        }
    };
    expect_emb_list(retrieved_lists.Get(1),
                    {20.0F, 0.0F, 0.0F, 0.0F, 19.0F, 0.0F, 0.0F, 0.0F});
    expect_emb_list(retrieved_lists.Get(3),
                    {30.0F, 0.0F, 0.0F, 0.0F, 29.0F, 0.0F, 0.0F, 0.0F});
    expect_emb_list(retrieved_lists.Get(4),
                    {1.0F, 0.0F, 0.0F, 0.0F, 0.5F, 0.0F, 0.0F, 0.0F});

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch("pk != 2",
                                              "embeddings",
                                              3,
                                              knowhere::metric::MAX_SIM_IP,
                                              R"({"nprobe": 1})",
                                              -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {1.0F, 0.0F, 0.0F, 0.0F};
    std::vector<size_t> query_offsets = {0, 1};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
        1, dim, query.data(), query_offsets);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto old_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(old_snapshot_result->seg_offsets_.size(), 3);
    for (auto offset : old_snapshot_result->seg_offsets_) {
        ASSERT_GE(offset, 0);
        EXPECT_LT(offset, first_batch_rows);
        EXPECT_NE(offset, filtered_logical_row);
        EXPECT_TRUE(first_valid[offset]);
    }

    auto full_snapshot_result =
        segment_growing->Search(plan.get(), ph_group.get(), 200);
    bool found_tail = false;
    for (auto offset : full_snapshot_result->seg_offsets_) {
        EXPECT_NE(offset, filtered_logical_row);
        if (offset == first_batch_rows) {
            found_tail = true;
        }
    }
    EXPECT_TRUE(found_tail);
}

TEST(GrowingIndexNullableVectorTest,
     BruteForceSearchAndGetReturnEmptyForAllNullEmbList) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 6;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugVectorArrayField("embeddings",
                                                DataType::VECTOR_FLOAT,
                                                dim,
                                                knowhere::metric::MAX_SIM_IP,
                                                true);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.enable_interim_segment_index = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);

    std::vector<int64_t> pks(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count, 100);
    for (int64_t i = 0; i < row_count; ++i) {
        pks[i] = i;
        row_ids[i] = i;
    }

    FixedVector<bool> valid_data(row_count);
    std::fill(valid_data.begin(), valid_data.end(), false);
    std::vector<VectorFieldProto> compact_lists;

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
    auto vec_array = CreateVectorDataArrayFrom(
        compact_lists.data(), valid_data.data(), row_count, 0, (*schema)[vec]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
    insert_data->set_num_rows(row_count);

    auto reserved_offset = segment_growing->PreInsert(row_count);
    segment_growing->Insert(reserved_offset,
                            row_count,
                            row_ids.data(),
                            timestamps.data(),
                            insert_data.get());

    ASSERT_FALSE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 3> retrieve_offsets = {0, 3, row_count - 1};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    const auto& retrieved_lists =
        retrieve_result->vectors().vector_array().data();
    ASSERT_EQ(retrieved_lists.size(), retrieve_offsets.size());
    for (int i = 0; i < retrieve_result->valid_data_size(); ++i) {
        EXPECT_FALSE(retrieve_result->valid_data(i));
        EXPECT_TRUE(retrieved_lists.Get(i).float_vector().data().empty());
    }

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch("",
                                              "embeddings",
                                              3,
                                              knowhere::metric::MAX_SIM_IP,
                                              R"({"nprobe": 1})",
                                              -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {1.0F, 0.0F, 0.0F, 0.0F};
    std::vector<size_t> query_offsets = {0, 1};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
        1, dim, query.data(), query_offsets);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(result->seg_offsets_.size(), 3);
    ASSERT_EQ(result->distances_.size(), 3);
    for (size_t i = 0; i < result->seg_offsets_.size(); ++i) {
        EXPECT_EQ(result->seg_offsets_[i], INVALID_SEG_OFFSET);
        EXPECT_FLOAT_EQ(result->distances_[i], 0.0F);
    }
}

TEST(GrowingIndexNullableVectorTest,
     BruteForceSearchAndGetKeepValidForEmptyValidEmbList) {
    constexpr int64_t dim = 4;
    constexpr int64_t row_count = 6;

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto vec = schema->AddDebugVectorArrayField("embeddings",
                                                DataType::VECTOR_FLOAT,
                                                dim,
                                                knowhere::metric::MAX_SIM_IP,
                                                true);
    schema->set_primary_field_id(pk);

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.enable_interim_segment_index = false;
    ApplyInterimIndexConfigForTest(interim_config, config);

    auto segment_growing =
        CreateGrowingSegment(schema, empty_index_meta, 1, config);
    auto* segment_impl =
        dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    ASSERT_NE(segment_impl, nullptr);

    std::vector<int64_t> pks(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count, 100);
    for (int64_t i = 0; i < row_count; ++i) {
        pks[i] = i;
        row_ids[i] = i;
    }

    FixedVector<bool> valid_data(row_count);
    std::fill(valid_data.begin(), valid_data.end(), false);
    valid_data[1] = true;
    valid_data[2] = true;
    valid_data[4] = true;

    VectorFieldProto empty_row;
    empty_row.set_dim(dim);
    empty_row.mutable_float_vector();
    std::vector<VectorFieldProto> compact_lists = {
        empty_row,
        empty_row,
        empty_row,
    };

    auto insert_data = std::make_unique<InsertRecordProto>();
    auto pk_array =
        CreateDataArrayFrom(pks.data(), nullptr, row_count, (*schema)[pk]);
    auto vec_array = CreateVectorDataArrayFrom(compact_lists.data(),
                                               valid_data.data(),
                                               row_count,
                                               compact_lists.size(),
                                               (*schema)[vec]);
    insert_data->mutable_fields_data()->AddAllocated(pk_array.release());
    insert_data->mutable_fields_data()->AddAllocated(vec_array.release());
    insert_data->set_num_rows(row_count);

    auto reserved_offset = segment_growing->PreInsert(row_count);
    segment_growing->Insert(reserved_offset,
                            row_count,
                            row_ids.data(),
                            timestamps.data(),
                            insert_data.get());

    ASSERT_FALSE(segment_impl->get_indexing_record().SyncDataWithIndex(vec));

    std::array<int64_t, 4> retrieve_offsets = {0, 1, 3, 4};
    auto retrieve_result = segment_impl->bulk_subscript(
        nullptr, vec, retrieve_offsets.data(), retrieve_offsets.size());
    ASSERT_NE(retrieve_result, nullptr);
    ASSERT_EQ(retrieve_result->valid_data_size(), retrieve_offsets.size());
    EXPECT_FALSE(retrieve_result->valid_data(0));
    EXPECT_TRUE(retrieve_result->valid_data(1));
    EXPECT_FALSE(retrieve_result->valid_data(2));
    EXPECT_TRUE(retrieve_result->valid_data(3));
    const auto& retrieved_lists =
        retrieve_result->vectors().vector_array().data();
    ASSERT_EQ(retrieved_lists.size(), retrieve_offsets.size());
    for (int i = 0; i < retrieve_result->valid_data_size(); ++i) {
        EXPECT_TRUE(retrieved_lists.Get(i).float_vector().data().empty());
    }

    milvus::segcore::ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.ParseSearch("",
                                              "embeddings",
                                              3,
                                              knowhere::metric::MAX_SIM_IP,
                                              R"({"nprobe": 1})",
                                              -1);
    auto plan =
        query::CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    std::array<float, dim> query = {1.0F, 0.0F, 0.0F, 0.0F};
    std::vector<size_t> query_offsets = {0, 1};
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
        1, dim, query.data(), query_offsets);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto result = segment_growing->Search(plan.get(), ph_group.get(), 100);
    ASSERT_EQ(result->seg_offsets_.size(), 3);
    ASSERT_EQ(result->distances_.size(), 3);
    for (size_t i = 0; i < result->seg_offsets_.size(); ++i) {
        EXPECT_EQ(result->seg_offsets_[i], INVALID_SEG_OFFSET);
        EXPECT_FLOAT_EQ(result->distances_[i],
                        std::numeric_limits<float>::min());
    }
}

TEST_P(GrowingIndexTest, MissIndexMeta) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);

    auto dim = 4;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("random", DataType::DOUBLE);
    schema->AddDebugField("embeddings", data_type, dim, metric_type);
    schema->set_primary_field_id(pk);

    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    auto segment = CreateGrowingSegment(schema, nullptr);
}

TEST_P(GrowingIndexTest, GetVector) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);

    auto dim = 4;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->AddDebugField("random", DataType::DOUBLE);
    auto vec = schema->AddDebugField("embeddings", data_type, dim, metric_type);
    schema->set_primary_field_id(pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    if (dense_vec_intermin_index_type.has_value()) {
        config.set_dense_vector_intermin_index_type(
            dense_vec_intermin_index_type.value());
    }
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 5;
    if (data_type == DataType::VECTOR_FLOAT) {
        // GetVector for VECTOR_FLOAT
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
            auto result = segment->bulk_subscript(
                nullptr, vec, ids_ds->GetIds(), num_inserted);

            auto vector =
                result.get()->mutable_vectors()->float_vector().data();
            EXPECT_TRUE(vector.size() == num_inserted * dim);
            for (size_t i = 0; i < num_inserted; ++i) {
                auto id = ids_ds->GetIds()[i];
                for (size_t j = 0; j < dim; ++j) {
                    EXPECT_TRUE(vector[i * dim + j] ==
                                fakevec[(id % per_batch) * dim + j]);
                }
            }
        }
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        // GetVector for VECTOR_FLOAT16
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, per_batch);
            auto fakevec = dataset.get_col<float16>(vec);
            auto offset = segment->PreInsert(per_batch);
            segment->Insert(offset,
                            per_batch,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            auto num_inserted = (i + 1) * per_batch;
            auto ids_ds = GenRandomIds(num_inserted);
            auto result = segment->bulk_subscript(
                nullptr, vec, ids_ds->GetIds(), num_inserted);
            auto vector = result.get()->mutable_vectors()->float16_vector();
            EXPECT_TRUE(vector.size() == num_inserted * dim * sizeof(float16));
            for (size_t i = 0; i < num_inserted; ++i) {
                auto id = ids_ds->GetIds()[i];
                for (size_t j = 0; j < dim; ++j) {
                    EXPECT_TRUE(reinterpret_cast<float16*>(
                                    vector.data())[i * dim + j] ==
                                fakevec[(id % per_batch) * dim + j]);
                }
            }
        }
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        // GetVector for VECTOR_FLOAT16
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, per_batch);
            auto fakevec = dataset.get_col<bfloat16>(vec);
            auto offset = segment->PreInsert(per_batch);
            segment->Insert(offset,
                            per_batch,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            auto num_inserted = (i + 1) * per_batch;
            auto ids_ds = GenRandomIds(num_inserted);
            auto result = segment->bulk_subscript(
                nullptr, vec, ids_ds->GetIds(), num_inserted);

            auto vector = result.get()->mutable_vectors()->bfloat16_vector();
            EXPECT_TRUE(vector.size() == num_inserted * dim * sizeof(bfloat16));
            for (size_t i = 0; i < num_inserted; ++i) {
                auto id = ids_ds->GetIds()[i];
                for (size_t j = 0; j < dim; ++j) {
                    EXPECT_TRUE(reinterpret_cast<bfloat16*>(
                                    vector.data())[i * dim + j] ==
                                fakevec[(id % per_batch) * dim + j]);
                }
            }
        }
    } else if (is_sparse) {
        // GetVector for VECTOR_SPARSE_U32_F32
        for (int64_t i = 0; i < n_batch; i++) {
            auto dataset = DataGen(schema, per_batch);
            auto fakevec = dataset.get_col<
                knowhere::sparse::SparseRow<milvus::SparseValueType>>(vec);
            auto offset = segment->PreInsert(per_batch);
            segment->Insert(offset,
                            per_batch,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            auto num_inserted = (i + 1) * per_batch;
            auto ids_ds = GenRandomIds(num_inserted);
            auto result = segment->bulk_subscript(
                nullptr, vec, ids_ds->GetIds(), num_inserted);

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
