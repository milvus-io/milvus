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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
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
#include "storage/Util.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/indexbuilder_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

// ScopedSegcoreConfigRestore does not cover asyncGrowingBuild, so tests that
// pin the flag restore it themselves.
class ScopedAsyncGrowingBuild {
 public:
    ScopedAsyncGrowingBuild(SegcoreConfig& config, bool value)
        : config_(config),
          previous_(config.get_enable_async_growing_index_build()) {
        config_.set_enable_async_growing_index_build(value);
    }

    ~ScopedAsyncGrowingBuild() {
        config_.set_enable_async_growing_index_build(previous_);
    }

    ScopedAsyncGrowingBuild(const ScopedAsyncGrowingBuild&) = delete;
    ScopedAsyncGrowingBuild&
    operator=(const ScopedAsyncGrowingBuild&) = delete;

 private:
    SegcoreConfig& config_;
    bool previous_;
};

}  // namespace

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
    // This case asserts the synchronous contract "the batch that crosses the
    // build threshold leaves the index fully synchronized, so its chunks are
    // reclaimed before Insert returns". Async first build is covered by
    // GrowingIndexAsyncBuildTest instead.
    ScopedAsyncGrowingBuild sync_build(config, false);
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

class GrowingIndexRawOwnershipTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        pk_ = schema_->AddDebugField("pk", DataType::INT64);
        vec_ = schema_->AddDebugField("embeddings",
                                      DataType::VECTOR_FLOAT,
                                      dim,
                                      knowhere::metric::L2,
                                      true);
        schema_->set_primary_field_id(pk_);

        std::map<std::string, std::string> index_params = {
            {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {"metric_type", knowhere::metric::L2},
            {"nlist", "1"}};
        std::map<std::string, std::string> type_params = {
            {"dim", std::to_string(dim)}};
        FieldIndexMeta field_index_meta(
            vec_, std::move(index_params), std::move(type_params));
        std::map<FieldId, FieldIndexMeta> field_map = {
            {vec_, field_index_meta}};
        meta_ =
            std::make_shared<CollectionIndexMeta>(100, std::move(field_map));

        InterimIndexConfigForTest interim_config;
        interim_config.chunk_rows = 16;
        interim_config.nlist = 1;
        interim_config.nprobe = 1;
        interim_config.dense_vector_interim_index_type =
            knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
        interim_config.sub_dim = dim;
        interim_config.refine_ratio = 1.0F;
        interim_config.refine_quant_type = "NONE";
        interim_config.refine_with_quant_flag = false;
        ApplyInterimIndexConfigForTest(interim_config, config_);
        config_.set_storage_v3_enabled(true);
        config_.set_enable_growing_source_flush(true);
    }

    GeneratedData
    InsertBatch(SegmentGrowing* segment, uint64_t seed) const {
        auto dataset = DataGen(schema_, row_count, seed);
        auto offset = segment->PreInsert(row_count);
        segment->Insert(offset,
                        row_count,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        return dataset;
    }

    FieldDataPtr
    CreateNullableFloatFieldData(const DataArray& data) const {
        std::vector<uint8_t> valid_bitmap((row_count + 7) / 8, 0);
        for (int64_t i = 0; i < row_count; ++i) {
            if (data.valid_data(i)) {
                valid_bitmap[i / 8] |= uint8_t{1} << (i % 8);
            }
        }

        auto field_data = storage::CreateFieldData(
            DataType::VECTOR_FLOAT, DataType::NONE, true, dim, row_count);
        field_data->FillFieldData(data.vectors().float_vector().data().data(),
                                  valid_bitmap.data(),
                                  row_count,
                                  0);
        return field_data;
    }

    void
    AssertNullableFloatDataEqual(const DataArray& actual,
                                 const DataArray& expected) const {
        ASSERT_EQ(actual.valid_data_size(), expected.valid_data_size());
        for (int i = 0; i < actual.valid_data_size(); ++i) {
            EXPECT_EQ(actual.valid_data(i), expected.valid_data(i));
        }

        const auto& actual_values = actual.vectors().float_vector().data();
        const auto& expected_values = expected.vectors().float_vector().data();
        ASSERT_FALSE(expected_values.empty());
        ASSERT_EQ(actual_values.size(), expected_values.size());
        for (int i = 0; i < actual_values.size(); ++i) {
            EXPECT_FLOAT_EQ(actual_values[i], expected_values[i]);
        }
    }

    static constexpr int64_t dim = 4;
    static constexpr int64_t row_count = 100;

    SegcoreConfig& config_ = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore_{config_};
    // Both cases assert raw-data ownership immediately after the insert that
    // crosses the build threshold, which is only defined for the synchronous
    // build path.
    ScopedAsyncGrowingBuild sync_build_{config_, false};
    SchemaPtr schema_;
    FieldId pk_;
    FieldId vec_;
    IndexMetaPtr meta_;
};

TEST_F(GrowingIndexRawOwnershipTest,
       InsertAfterSynchronizationDoesNotRepopulateRawData) {
    auto segment = CreateGrowingSegment(schema_, meta_, 1, config_);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    InsertBatch(segment.get(), 42);
    auto* raw_vector = segment_impl->get_insert_record().get_data_base(vec_);
    ASSERT_EQ(raw_vector->num_chunk(), 0);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(vec_));
    const auto raw_logical_count =
        raw_vector->get_offset_mapping().GetTotalCount();
    ASSERT_EQ(raw_logical_count, row_count);

    auto second_batch = InsertBatch(segment.get(), 43);

    EXPECT_EQ(segment->get_row_count(), 2 * row_count);
    EXPECT_EQ(raw_vector->num_chunk(), 0);
    EXPECT_EQ(raw_vector->get_offset_mapping().GetTotalCount(),
              raw_logical_count);

    std::vector<int64_t> offsets(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        offsets[i] = row_count + i;
    }
    auto actual =
        segment_impl->bulk_subscript(nullptr, vec_, offsets.data(), row_count);
    auto expected = second_batch.get_col(vec_);
    AssertNullableFloatDataEqual(*actual, *expected);
}

TEST_F(GrowingIndexRawOwnershipTest,
       LoadAfterSynchronizationPreservesValidityWithoutRawData) {
    auto segment = CreateGrowingSegment(schema_, meta_, 1, config_);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    InsertBatch(segment.get(), 42);
    auto* raw_vector = segment_impl->get_insert_record().get_data_base(vec_);
    ASSERT_EQ(raw_vector->num_chunk(), 0);
    ASSERT_TRUE(segment_impl->CanReadRawVectorFromIndex(vec_));
    const auto raw_logical_count =
        raw_vector->get_offset_mapping().GetTotalCount();
    ASSERT_EQ(raw_logical_count, row_count);

    auto second_batch = DataGen(schema_, row_count, 44);
    auto expected = second_batch.get_col(vec_);
    auto field_data = CreateNullableFloatFieldData(*expected);
    auto offset = segment->PreInsert(row_count);
    segment_impl->load_field_data_common(
        vec_, offset, {field_data}, pk_, row_count);

    EXPECT_EQ(raw_vector->num_chunk(), 0);
    EXPECT_EQ(raw_vector->get_offset_mapping().GetTotalCount(),
              raw_logical_count);

    std::vector<int64_t> offsets(row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        offsets[i] = row_count + i;
    }
    auto actual =
        segment_impl->bulk_subscript(nullptr, vec_, offsets.data(), row_count);
    AssertNullableFloatDataEqual(*actual, *expected);
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
    // The assertion below requires the index (and its compact physical ids) to
    // serve the very first search after the triggering insert.
    ScopedAsyncGrowingBuild sync_build(config, false);
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

// ---------------------------------------------------------------------------
// Async first build of the growing interim index.
// Spec: docs/superpowers/specs/2026-07-30-growing-index-async-build-design.md
// ---------------------------------------------------------------------------

namespace {

using milvus::segcore::VectorFieldIndexing;

constexpr int64_t kAsyncDim = 16;
// max_index_row_count 226985 with build_ratio 0.1 and nlist 100 gives
// build_threshold = max(22698, 3900) = 22698, i.e. the third 10k batch
// triggers the first build and two more batches must be caught up.
constexpr int64_t kAsyncMaxIndexRowCount = 226985;
// get_build_threshold() for kAsyncMaxIndexRowCount above. The only
// structurally guaranteed catch-up window is
// [kAsyncBuildThreshold, trigger_batch_end): the triggering batch (the one
// whose insert flips kNotBuilt -> kBuilding) has rows below the threshold
// absorbed by the synchronous Phase 1 build, and the rest of that same
// batch can only reach the index via AddRange -> Copy*Rows during catch-up.
// Anything inserted in a *later* batch may instead land there through
// AddBatch* if the background finalize already published by then, so it
// only *probabilistically* exercises the catch-up slice arithmetic.
constexpr int64_t kAsyncBuildThreshold = 22698;

// Clears the process-wide test hook no matter how the test exits.
class ScopedBuildHook {
 public:
    explicit ScopedBuildHook(
        std::function<void(VectorFieldIndexing::GrowingBuildPhase)> hook) {
        VectorFieldIndexing::growing_build_test_hook_ = std::move(hook);
    }

    ~ScopedBuildHook() {
        VectorFieldIndexing::growing_build_test_hook_ = nullptr;
    }

    ScopedBuildHook(const ScopedBuildHook&) = delete;
    ScopedBuildHook&
    operator=(const ScopedBuildHook&) = delete;
};

struct AsyncBuildFixture {
    SchemaPtr schema;
    FieldId pk;
    FieldId vec;
    IndexMetaPtr meta;
    std::string plan_str;
};

AsyncBuildFixture
MakeAsyncBuildFixture(DataType data_type,
                      const std::string& index_type,
                      const knowhere::MetricType& metric,
                      bool nullable,
                      int64_t max_index_row_count = kAsyncMaxIndexRowCount) {
    AsyncBuildFixture fixture;
    fixture.schema = std::make_shared<Schema>();
    fixture.pk = fixture.schema->AddDebugField("pk", DataType::INT64);
    fixture.schema->AddDebugField("random", DataType::DOUBLE);
    fixture.vec = fixture.schema->AddDebugField(
        "embeddings", data_type, kAsyncDim, metric, nullable);
    fixture.schema->set_primary_field_id(fixture.pk);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type}, {"metric_type", metric}, {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(kAsyncDim)}};
    FieldIndexMeta field_index_meta(
        fixture.vec, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> field_map = {
        {fixture.vec, field_index_meta}};
    fixture.meta = std::make_shared<CollectionIndexMeta>(max_index_row_count,
                                                         std::move(field_map));

    milvus::proto::plan::PlanNode plan_node;
    auto vector_anns = plan_node.mutable_vector_anns();
    vector_anns->set_vector_type(
        data_type == DataType::VECTOR_SPARSE_U32_F32
            ? milvus::proto::plan::VectorType::SparseFloatVector
            : milvus::proto::plan::VectorType::FloatVector);
    vector_anns->set_placeholder_tag("$0");
    vector_anns->set_field_id(fixture.vec.get());
    auto query_info = vector_anns->mutable_query_info();
    query_info->set_topk(5);
    query_info->set_round_decimal(3);
    query_info->set_metric_type(metric);
    query_info->set_search_params(R"({"nprobe": 16})");
    fixture.plan_str = plan_node.SerializeAsString();
    return fixture;
}

void
ApplyAsyncBuildConfig(SegcoreConfig& config) {
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 1024;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    ApplyInterimIndexConfigForTest(interim_config, config);
}

bool
WaitSynced(const SegmentGrowingImpl* segment, FieldId vec, int timeout_ms) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
        if (segment->get_indexing_record().SyncDataWithIndex(vec)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return segment->get_indexing_record().SyncDataWithIndex(vec);
}

bool
WaitState(const SegmentGrowingImpl* segment,
          FieldId vec,
          VectorFieldIndexing::GrowingIndexState expected,
          int timeout_ms) {
    for (int waited = 0; waited < timeout_ms; waited += 10) {
        if (segment->get_indexing_record()
                .get_vec_field_indexing(vec)
                .get_growing_index_state() == expected) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return segment->get_indexing_record()
               .get_vec_field_indexing(vec)
               .get_growing_index_state() == expected;
}

int64_t
IndexedRowCount(const SegmentGrowingImpl* segment, FieldId vec) {
    const auto& indexing =
        segment->get_indexing_record().get_vec_field_indexing(vec);
    auto pinned = indexing.get_segment_indexing();
    auto* vec_index = dynamic_cast<index::VectorIndex*>(pinned.get());
    if (vec_index == nullptr) {
        return -1;
    }
    return vec_index->Count();
}

// Inserts one batch and returns the generated data (kept alive by the caller
// when the vectors are needed afterwards). `patch` runs on the generated data
// before it is inserted, so a test can plant a known vector in a known row.
GeneratedData
InsertAsyncBatch(SegmentGrowing* segment,
                 const SchemaPtr& schema,
                 int64_t rows,
                 uint64_t seed,
                 const std::function<void(GeneratedData&)>& patch = nullptr) {
    auto dataset = DataGen(schema, rows, seed);
    if (patch) {
        patch(dataset);
    }
    auto offset = segment->PreInsert(rows);
    segment->Insert(offset,
                    rows,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    return dataset;
}

std::unique_ptr<milvus::query::PlaceholderGroup>
MakeAsyncPlaceholders(DataType data_type,
                      const milvus::query::Plan* plan,
                      int num_queries) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup ph_group_raw;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        ph_group_raw = CreateSparseFloatPlaceholderGroup(num_queries);
    } else {
        ph_group_raw = CreatePlaceholderGroup(num_queries, kAsyncDim, 1024);
    }
    return ParsePlaceholderGroup(plan, ph_group_raw.SerializeAsString());
}

// nq=1 placeholder holding exactly the dim-kAsyncDim vector at `vec`.
std::unique_ptr<milvus::query::PlaceholderGroup>
MakeSingleDensePlaceholder(const milvus::query::Plan* plan, const float* vec) {
    auto raw_group = CreatePlaceholderGroupFromBlob(1, kAsyncDim, vec);
    return ParsePlaceholderGroup(plan, raw_group.SerializeAsString());
}

// nq=1 placeholder holding exactly `row`.
std::unique_ptr<milvus::query::PlaceholderGroup>
MakeSingleSparsePlaceholder(
    const milvus::query::Plan* plan,
    const knowhere::sparse::SparseRow<milvus::SparseValueType>& row) {
    namespace ser = milvus::proto::common;
    ser::PlaceholderGroup raw_group;
    auto* value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::SparseFloatVector);
    value->add_values(row.data(), row.data_byte_size());
    return ParsePlaceholderGroup(plan, raw_group.SerializeAsString());
}

// Sets an atomic flag on every scope exit, including the early return an
// ASSERT_* failure produces. Used to unpark a build thread that a test hook
// is holding: declare it *after* the object whose destructor would otherwise
// block on that thread, so it fires first.
class ScopedFlagOnExit {
 public:
    explicit ScopedFlagOnExit(std::atomic<bool>& flag) : flag_(flag) {
    }

    ~ScopedFlagOnExit() {
        flag_.store(true);
    }

    ScopedFlagOnExit(const ScopedFlagOnExit&) = delete;
    ScopedFlagOnExit&
    operator=(const ScopedFlagOnExit&) = delete;

 private:
    std::atomic<bool>& flag_;
};

}  // namespace

TEST(GrowingIndexAsyncBuildTest, BuildOffInsertPathAndCatchesUp) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/false);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        fixture.schema, fixture.plan_str.data(), fixture.plan_str.size());
    constexpr int num_queries = 5;
    constexpr int top_k = 5;
    auto ph_group =
        MakeAsyncPlaceholders(DataType::VECTOR_FLOAT, plan.get(), num_queries);

    constexpr int64_t per_batch = 10000;
    constexpr int64_t n_batch = 5;
    // The build threshold (kAsyncBuildThreshold == 22698) is crossed by the
    // third batch (index 2, rows [20000, 30000)), so the last batch is
    // *probabilistically* inside the catch-up range: it only exercises
    // CopyDenseRows with from > 0 if the background finalize hasn't already
    // published by the time it's inserted. The triggering batch itself is
    // the structurally guaranteed catch-up window -- see threshold_batch
    // below.
    static_assert(kAsyncBuildThreshold / per_batch < n_batch - 1,
                  "triggering batch must precede the last batch");
    constexpr int64_t kThresholdBatchIdx = kAsyncBuildThreshold / per_batch;
    std::unique_ptr<GeneratedData> last_batch;
    std::unique_ptr<GeneratedData> threshold_batch;
    for (int64_t i = 0; i < n_batch; i++) {
        // DataGen seeds row n of a batch with (batch seed + n), so batches
        // whose seeds are closer together than per_batch share vectors
        // verbatim. Spacing them apart keeps every inserted vector unique,
        // which is what makes the exact-match probes below unambiguous.
        auto dataset = InsertAsyncBatch(
            segment.get(), fixture.schema, per_batch, 42 + i * 2 * per_batch);
        if (i == n_batch - 1) {
            last_batch = std::make_unique<GeneratedData>(std::move(dataset));
        } else if (i == kThresholdBatchIdx) {
            threshold_batch =
                std::make_unique<GeneratedData>(std::move(dataset));
        }
        // Insert must not wait for the build: the segment stays searchable
        // through the whole build/catch-up window.
        auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
        EXPECT_EQ(sr->total_nq_, num_queries);
        EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
        EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
    }

    // The background task eventually catches up and publishes.
    ASSERT_TRUE(WaitSynced(segment_impl, fixture.vec, /*timeout_ms=*/60000));
    EXPECT_EQ(segment_impl->get_indexing_record()
                  .get_vec_field_indexing(fixture.vec)
                  .get_growing_index_state(),
              VectorFieldIndexing::GrowingIndexState::kSynced);
    // No row was dropped between the first build and the locked finalize.
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec), n_batch * per_batch);

    auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);

    // Content check, not just a count: query with the exact vector of a row
    // that was copied during catch-up. A misaligned catch-up slice would put
    // some other row's data at this offset, so the self-match would either
    // land on a different seg_offset or stop being an exact (distance 0) hit.
    ASSERT_NE(last_batch, nullptr);
    constexpr int64_t probe_row = 4237;
    constexpr int64_t probe_offset = (n_batch - 1) * per_batch + probe_row;
    auto probe_vectors = last_batch->get_col<float>(fixture.vec);
    ASSERT_EQ(probe_vectors.size(), per_batch * kAsyncDim);
    auto probe_ph = MakeSingleDensePlaceholder(
        plan.get(), probe_vectors.data() + probe_row * kAsyncDim);
    auto probe_sr = segment->Search(plan.get(), probe_ph.get(), 1000000);
    ASSERT_EQ(probe_sr->total_nq_, 1);
    ASSERT_EQ(probe_sr->seg_offsets_.size(), top_k);
    EXPECT_EQ(probe_sr->seg_offsets_[0], probe_offset);
    EXPECT_NEAR(probe_sr->distances_[0], 0.0f, 1e-5);

    // Second content check, this one at a seg_offset inside the
    // *structurally guaranteed* catch-up window [kAsyncBuildThreshold,
    // threshold-batch end) -- rows here can only have reached the index via
    // AddRange -> CopyDenseRows, unlike the last-batch probe above which
    // depends on winning the race against the background finalize.
    ASSERT_NE(threshold_batch, nullptr);
    constexpr int64_t threshold_probe_offset = 25000;
    static_assert(
        threshold_probe_offset >= kAsyncBuildThreshold &&
            threshold_probe_offset < (kThresholdBatchIdx + 1) * per_batch,
        "threshold probe offset must land inside the structurally "
        "guaranteed catch-up window");
    constexpr int64_t threshold_probe_row =
        threshold_probe_offset - kThresholdBatchIdx * per_batch;
    auto threshold_probe_vectors = threshold_batch->get_col<float>(fixture.vec);
    ASSERT_EQ(threshold_probe_vectors.size(), per_batch * kAsyncDim);
    auto threshold_probe_ph = MakeSingleDensePlaceholder(
        plan.get(),
        threshold_probe_vectors.data() + threshold_probe_row * kAsyncDim);
    auto threshold_probe_sr =
        segment->Search(plan.get(), threshold_probe_ph.get(), 1000000);
    ASSERT_EQ(threshold_probe_sr->total_nq_, 1);
    ASSERT_EQ(threshold_probe_sr->seg_offsets_.size(), top_k);
    EXPECT_EQ(threshold_probe_sr->seg_offsets_[0], threshold_probe_offset);
    EXPECT_NEAR(threshold_probe_sr->distances_[0], 0.0f, 1e-5);

    // A post-sync batch takes the kSynced Add branch; try_remove_chunks only
    // runs on the insert path, so this batch is what reclaims the chunks
    // (IVF_FLAT_CC keeps raw data, so the index owns it now).
    InsertAsyncBatch(
        segment.get(), fixture.schema, per_batch, 42 + 10 * per_batch);
    auto* field_data =
        segment_impl->get_insert_record().get_data<milvus::FloatVector>(
            fixture.vec);
    EXPECT_EQ(field_data->num_chunk(), 0);
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec),
              (n_batch + 1) * per_batch);
    sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
}

TEST(GrowingIndexAsyncBuildTest, BuildFailureDisablesIndexPermanently) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    std::atomic<int> hook_calls{0};
    ScopedBuildHook hook([&](VectorFieldIndexing::GrowingBuildPhase phase) {
        if (phase == VectorFieldIndexing::GrowingBuildPhase::kBeforeBuild) {
            hook_calls.fetch_add(1);
            ThrowInfo(UnexpectedError, "injected build failure");
        }
    });

    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/false);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        fixture.schema, fixture.plan_str.data(), fixture.plan_str.size());
    constexpr int num_queries = 5;
    constexpr int top_k = 5;
    auto ph_group =
        MakeAsyncPlaceholders(DataType::VECTOR_FLOAT, plan.get(), num_queries);

    constexpr int64_t per_batch = 25000;
    for (int64_t i = 0; i < 3; i++) {
        InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 7 + i);
        ASSERT_TRUE(WaitState(segment_impl,
                              fixture.vec,
                              VectorFieldIndexing::GrowingIndexState::kDisabled,
                              /*timeout_ms=*/30000));
    }

    // Exactly one build attempt: kDisabled has no back edge to kNotBuilt.
    EXPECT_EQ(hook_calls.load(), 1);
    // Never synced, and brute-force search keeps working.
    EXPECT_FALSE(
        segment_impl->get_indexing_record().SyncDataWithIndex(fixture.vec));
    auto* field_data =
        segment_impl->get_insert_record().get_data<milvus::FloatVector>(
            fixture.vec);
    EXPECT_GT(field_data->num_chunk(), 0);
    auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->total_nq_, num_queries);
    EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
}

TEST(GrowingIndexAsyncBuildTest, SparseVectorBuildsAsynchronously) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_SPARSE_U32_F32,
                              knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                              knowhere::metric::IP,
                              /*nullable=*/false);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        fixture.schema, fixture.plan_str.data(), fixture.plan_str.size());
    constexpr int num_queries = 5;
    constexpr int top_k = 5;
    auto ph_group = MakeAsyncPlaceholders(
        DataType::VECTOR_SPARSE_U32_F32, plan.get(), num_queries);

    constexpr int64_t per_batch = 10000;
    constexpr int64_t n_batch = 4;
    // Planted in the last batch. The build threshold (kAsyncBuildThreshold ==
    // 22698) is crossed by the third batch (index 2), so this last-batch
    // probe only *probabilistically* lands in the catch-up range: it
    // exercises CopySparseRows with from > 0 only if the background finalize
    // hasn't already published by the time this batch is inserted. Values of
    // 100 on three dims make the row's IP self-similarity (3e4) unreachable
    // for any generated row, whose values are all in [0, 1) -- the top-1 hit
    // is deterministic.
    constexpr int64_t probe_row = 4237;
    constexpr int64_t probe_offset = (n_batch - 1) * per_batch + probe_row;
    knowhere::sparse::SparseRow<milvus::SparseValueType> probe(3);
    probe.set_at(0, 3, 100.0f);
    probe.set_at(1, 17, 100.0f);
    probe.set_at(2, 42, 100.0f);
    auto plant_probe = [&](GeneratedData& dataset) {
        for (auto& field_data : *dataset.raw_->mutable_fields_data()) {
            if (field_data.field_id() != fixture.vec.get()) {
                continue;
            }
            field_data.mutable_vectors()
                ->mutable_sparse_float_vector()
                ->set_contents(
                    probe_row,
                    std::string(static_cast<const char*>(probe.data()),
                                probe.data_byte_size()));
        }
    };

    // Second probe, planted in the triggering batch (index 2) at a seg_offset
    // inside the *structurally guaranteed* catch-up window
    // [kAsyncBuildThreshold, threshold-batch end): the triggering batch never
    // goes through AddBatch*, so this row can only reach the index via
    // AddRange -> CopySparseRows. Distinct dims/values from `probe` so the
    // two planted rows don't collide with each other's top-1 match (zero
    // overlap in sparse ids means zero cross IP contribution).
    constexpr int64_t kThresholdBatchIdx = kAsyncBuildThreshold / per_batch;
    constexpr int64_t threshold_probe_offset = 25000;
    static_assert(
        threshold_probe_offset >= kAsyncBuildThreshold &&
            threshold_probe_offset < (kThresholdBatchIdx + 1) * per_batch,
        "threshold probe offset must land inside the structurally "
        "guaranteed catch-up window");
    static_assert(kThresholdBatchIdx < n_batch - 1,
                  "triggering batch must precede the last batch");
    constexpr int64_t threshold_probe_row =
        threshold_probe_offset - kThresholdBatchIdx * per_batch;
    knowhere::sparse::SparseRow<milvus::SparseValueType> threshold_probe(3);
    threshold_probe.set_at(0, 5, 200.0f);
    threshold_probe.set_at(1, 29, 200.0f);
    threshold_probe.set_at(2, 55, 200.0f);
    auto plant_threshold_probe = [&](GeneratedData& dataset) {
        for (auto& field_data : *dataset.raw_->mutable_fields_data()) {
            if (field_data.field_id() != fixture.vec.get()) {
                continue;
            }
            field_data.mutable_vectors()
                ->mutable_sparse_float_vector()
                ->set_contents(threshold_probe_row,
                               std::string(static_cast<const char*>(
                                               threshold_probe.data()),
                                           threshold_probe.data_byte_size()));
        }
    };

    for (int64_t i = 0; i < n_batch; i++) {
        std::function<void(GeneratedData&)> patch;
        if (i == n_batch - 1) {
            patch = plant_probe;
        } else if (i == kThresholdBatchIdx) {
            patch = plant_threshold_probe;
        }
        InsertAsyncBatch(
            segment.get(), fixture.schema, per_batch, 11 + i, patch);
        auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
        EXPECT_EQ(sr->total_nq_, num_queries);
        EXPECT_EQ(sr->distances_.size(), num_queries * top_k);
    }

    ASSERT_TRUE(WaitSynced(segment_impl, fixture.vec, /*timeout_ms=*/60000));
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec), n_batch * per_batch);
    auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);

    // Content check: the planted row must come back as the top-1 hit at its
    // own offset. A misaligned catch-up slice would move it elsewhere.
    auto probe_ph = MakeSingleSparsePlaceholder(plan.get(), probe);
    auto probe_sr = segment->Search(plan.get(), probe_ph.get(), 1000000);
    ASSERT_EQ(probe_sr->total_nq_, 1);
    ASSERT_GE(probe_sr->seg_offsets_.size(), 1);
    EXPECT_EQ(probe_sr->seg_offsets_[0], probe_offset);

    // Same content check for the structurally-guaranteed threshold-batch
    // probe.
    auto threshold_probe_ph =
        MakeSingleSparsePlaceholder(plan.get(), threshold_probe);
    auto threshold_probe_sr =
        segment->Search(plan.get(), threshold_probe_ph.get(), 1000000);
    ASSERT_EQ(threshold_probe_sr->total_nq_, 1);
    ASSERT_GE(threshold_probe_sr->seg_offsets_.size(), 1);
    EXPECT_EQ(threshold_probe_sr->seg_offsets_[0], threshold_probe_offset);

    // Post-sync batch on the kSynced Add path.
    InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 77);
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec),
              (n_batch + 1) * per_batch);
    sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);
}

TEST(GrowingIndexAsyncBuildTest, NullableVectorBuildsAsynchronously) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    // DataGen marks 50% of rows null, so a 100k max row count (build_threshold
    // 10000 valid rows) puts the trigger on the third 8k batch and leaves two
    // more batches for the catch-up phase.
    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/true,
                              /*max_index_row_count=*/100000);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        fixture.schema, fixture.plan_str.data(), fixture.plan_str.size());
    constexpr int num_queries = 5;
    constexpr int top_k = 5;
    auto ph_group =
        MakeAsyncPlaceholders(DataType::VECTOR_FLOAT, plan.get(), num_queries);

    constexpr int64_t per_batch = 8000;
    constexpr int64_t n_batch = 5;
    for (int64_t i = 0; i < n_batch; i++) {
        InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 31 + i);
        auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
        EXPECT_EQ(sr->total_nq_, num_queries);
    }

    ASSERT_TRUE(WaitSynced(segment_impl, fixture.vec, /*timeout_ms=*/60000));
    auto* raw = segment_impl->get_insert_record().get_data_base(fixture.vec);
    ASSERT_TRUE(raw->is_mapping_storage());
    // DataGen marks exactly every other row null ((i % 100) >= 50).
    constexpr int64_t valid_per_batch = per_batch / 2;
    // Physical rows: only the valid ones reach the index, and every valid row
    // inserted so far was absorbed by the first build plus the catch-up.
    EXPECT_EQ(raw->get_valid_count(), n_batch * valid_per_batch);
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec),
              n_batch * valid_per_batch);
    // Logical rows: UpdateValidData ran up to the raw-data watermark, so the
    // index's own mapping spans every inserted row, nulls included.
    const auto& indexing =
        segment_impl->get_indexing_record().get_vec_field_indexing(fixture.vec);
    auto pinned = indexing.get_segment_indexing();
    auto* vec_index = dynamic_cast<index::VectorIndex*>(pinned.get());
    ASSERT_NE(vec_index, nullptr);
    EXPECT_EQ(vec_index->GetOffsetMapping().GetTotalCount(),
              n_batch * per_batch);

    auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(sr->seg_offsets_.size(), num_queries * top_k);

    // Post-sync batch takes the kSynced Add branch. The index now owns the raw
    // data, so ConcurrentVector stops tracking new rows (see
    // GrowingIndexRawOwnershipTest) -- only the index's counters advance, and
    // they must line up exactly with what the finalize published.
    InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 88);
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec),
              (n_batch + 1) * valid_per_batch);
    EXPECT_EQ(vec_index->GetOffsetMapping().GetTotalCount(),
              (n_batch + 1) * per_batch);
}

// Regression for the kSynced AddBatchDense nullable arm: it derives add_count
// from index_->GetOffsetMapping().GetTotalCount() vs. the incoming batch's
// [reserved_offset, reserved_offset + size) range. A misaligned watermark
// there makes that arithmetic negative (wrapping add_count to a huge value)
// or skips rows silently. NullableVectorBuildsAsynchronously above already
// checks the resulting counters; this test additionally plants a known
// vector at a specific logical row of a batch inserted *after* WaitSynced
// and confirms it comes back as an exact top-1 hit, and that no null row --
// pre- or post-sync -- ever appears in a search result.
TEST(GrowingIndexAsyncBuildTest, NullableCatchupKeepsValidDataConsistent) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    // Same threshold/batch geometry as NullableVectorBuildsAsynchronously:
    // DataGen's default 50% null rate makes build_threshold 10000 valid rows,
    // which the third 8k batch crosses, leaving two more batches to exercise
    // catch-up.
    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/true,
                              /*max_index_row_count=*/100000);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto plan = milvus::query::CreateSearchPlanByExpr(
        fixture.schema, fixture.plan_str.data(), fixture.plan_str.size());
    constexpr int num_queries = 5;
    constexpr int top_k = 5;
    auto ph_group =
        MakeAsyncPlaceholders(DataType::VECTOR_FLOAT, plan.get(), num_queries);

    // Tracks [start, start + size) of every batch inserted so far, so a
    // seg_offset returned by any search can be traced back to the local row
    // index whose nullability DataGen decided ((local % 100) >= 50 is
    // valid, DataGen's default null_percent).
    struct BatchSpan {
        int64_t start;
        int64_t size;
    };
    std::vector<BatchSpan> batches;
    auto record_batch = [&](int64_t size) {
        int64_t start =
            batches.empty() ? 0 : batches.back().start + batches.back().size;
        batches.push_back({start, size});
        return start;
    };
    auto is_valid_offset = [&](int64_t global_offset) {
        for (const auto& b : batches) {
            if (global_offset >= b.start && global_offset < b.start + b.size) {
                return ((global_offset - b.start) % 100) >= 50;
            }
        }
        ADD_FAILURE() << "seg_offset " << global_offset
                      << " is outside every known batch";
        return false;
    };
    // Null rows must never surface in search results: every returned offset
    // must land on a row DataGen marked valid.
    auto assert_no_null_hits = [&](const auto& sr) {
        for (auto off : sr->seg_offsets_) {
            if (off < 0) {
                continue;
            }
            EXPECT_TRUE(is_valid_offset(off))
                << "search returned a seg_offset landing on a null row: "
                << off;
        }
    };

    constexpr int64_t per_batch = 8000;
    constexpr int64_t n_batch = 5;
    for (int64_t i = 0; i < n_batch; i++) {
        InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 31 + i);
        record_batch(per_batch);
        auto sr = segment->Search(plan.get(), ph_group.get(), 1000000);
        EXPECT_EQ(sr->total_nq_, num_queries);
        assert_no_null_hits(sr);
    }

    ASSERT_TRUE(WaitSynced(segment_impl, fixture.vec, /*timeout_ms=*/60000));

    // Search correctness right after sync: null rows still never surface.
    auto synced_sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(synced_sr->seg_offsets_.size(), num_queries * top_k);
    assert_no_null_hits(synced_sr);

    int64_t pre_post_sync_indexed = IndexedRowCount(segment_impl, fixture.vec);

    // Post-sync batch on the kSynced AddBatchDense nullable arm. Local row 50
    // is the first valid row of its 100-block (local rows 0-49 are null
    // under DataGen's default pattern), so it lands at compact physical
    // offset 0 within this batch -- easy to plant a known vector at without
    // disturbing any other row, and immediately preceded by 50 null rows, so
    // finding it exercises "a null row doesn't break the valid rows around
    // it" directly.
    constexpr int64_t post_sync_batch_size = 100;
    constexpr int64_t probe_local_row = 50;
    constexpr int64_t probe_physical_offset = 0;  // no valid rows before it
    std::vector<float> probe_vector(kAsyncDim);
    for (int64_t d = 0; d < kAsyncDim; d++) {
        probe_vector[d] = 100.0f + static_cast<float>(d);
    }
    auto plant_probe = [&](GeneratedData& dataset) {
        for (auto& field_data : *dataset.raw_->mutable_fields_data()) {
            if (field_data.field_id() != fixture.vec.get()) {
                continue;
            }
            auto* floats = field_data.mutable_vectors()
                               ->mutable_float_vector()
                               ->mutable_data();
            for (int64_t d = 0; d < kAsyncDim; d++) {
                floats->Set(probe_physical_offset * kAsyncDim + d,
                            probe_vector[d]);
            }
        }
    };
    int64_t post_sync_start = record_batch(post_sync_batch_size);
    InsertAsyncBatch(
        segment.get(), fixture.schema, post_sync_batch_size, 999, plant_probe);

    // Accounting: the kSynced arm must add exactly this batch's 50 valid
    // rows to the index -- not fewer (skipped rows) and not a negative delta
    // (would wrap add_count to a huge value and either crash inside
    // AddWithDataset or silently corrupt the index).
    constexpr int64_t valid_in_post_sync_batch = post_sync_batch_size / 2;
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec),
              pre_post_sync_indexed + valid_in_post_sync_batch);

    const auto& indexing =
        segment_impl->get_indexing_record().get_vec_field_indexing(fixture.vec);
    auto pinned = indexing.get_segment_indexing();
    auto* vec_index = dynamic_cast<index::VectorIndex*>(pinned.get());
    ASSERT_NE(vec_index, nullptr);
    EXPECT_EQ(vec_index->GetOffsetMapping().GetTotalCount(),
              post_sync_start + post_sync_batch_size);

    // The key content probe: the planted row is found at its exact logical
    // offset with distance 0. A misaligned catch-up/accounting watermark
    // would either miss it, put it at the wrong offset, or (if add_count
    // went negative) crash before reaching this point.
    int64_t probe_offset = post_sync_start + probe_local_row;
    auto probe_ph = MakeSingleDensePlaceholder(plan.get(), probe_vector.data());
    auto probe_sr = segment->Search(plan.get(), probe_ph.get(), 1000000);
    ASSERT_EQ(probe_sr->total_nq_, 1);
    ASSERT_EQ(probe_sr->seg_offsets_.size(), top_k);
    EXPECT_EQ(probe_sr->seg_offsets_[0], probe_offset);
    EXPECT_NEAR(probe_sr->distances_[0], 0.0f, 1e-5);

    // Broad search once more: the post-sync batch's own null rows (its local
    // rows 0-49, immediately preceding the probe row planted above) must not
    // leak into results either.
    auto final_sr = segment->Search(plan.get(), ph_group.get(), 1000000);
    EXPECT_EQ(final_sr->seg_offsets_.size(), num_queries * top_k);
    assert_no_null_hits(final_sr);
}

// Regression for spec §4.2: a batch that lands between the last watermark read
// and the finalize lock must still be absorbed before sync_with_index_ flips.
// Otherwise the next kSynced batch computes a negative source offset.
TEST(GrowingIndexAsyncBuildTest, InsertRacingFinalizeIsAbsorbed) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    std::atomic<bool> finalize_reached{false};
    std::atomic<bool> release_finalize{false};
    ScopedBuildHook hook([&](VectorFieldIndexing::GrowingBuildPhase phase) {
        if (phase != VectorFieldIndexing::GrowingBuildPhase::kBeforeFinalize) {
            return;
        }
        if (finalize_reached.exchange(true)) {
            return;
        }
        // Park just before the finalize lock is taken, so the racing insert
        // below can complete and advance pending_upto_.
        while (!release_finalize.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/false);
    auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
    // Declared after `segment` so it runs BEFORE the segment destructor on
    // every exit path: that destructor joins the build task, which parks in
    // the hook above until release_finalize is set. Without this, any early
    // return (a failing ASSERT_*) would deadlock the test binary.
    ScopedFlagOnExit release_on_exit(release_finalize);
    auto* segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    constexpr int64_t per_batch = 25000;
    InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 5);
    for (int waited = 0; waited < 60000 && !finalize_reached.load();
         waited += 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(finalize_reached.load());
    EXPECT_FALSE(
        segment_impl->get_indexing_record().SyncDataWithIndex(fixture.vec));

    // This insert only bumps the watermark (state is still kBuilding).
    InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 6);
    release_finalize.store(true);

    ASSERT_TRUE(WaitSynced(segment_impl, fixture.vec, /*timeout_ms=*/60000));
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec), 2 * per_batch);
    // Would read out of bounds if the racing batch had been skipped.
    InsertAsyncBatch(segment.get(), fixture.schema, per_batch, 7);
    EXPECT_EQ(IndexedRowCount(segment_impl, fixture.vec), 3 * per_batch);
}

// Spec §4.6: destroying the segment while a build is in flight must not leak,
// use freed memory, or hang.
TEST(GrowingIndexAsyncBuildTest, DestroySegmentDuringBuildIsSafe) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    ScopedAsyncGrowingBuild async_build(config, true);
    ApplyAsyncBuildConfig(config);

    std::atomic<bool> build_started{false};
    ScopedBuildHook hook([&](VectorFieldIndexing::GrowingBuildPhase phase) {
        if (phase == VectorFieldIndexing::GrowingBuildPhase::kBeforeBuild) {
            build_started.store(true);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    });

    auto fixture =
        MakeAsyncBuildFixture(DataType::VECTOR_FLOAT,
                              knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                              knowhere::metric::L2,
                              /*nullable=*/false);
    {
        auto segment = CreateGrowingSegment(fixture.schema, fixture.meta);
        InsertAsyncBatch(segment.get(), fixture.schema, 25000, 13);
        for (int waited = 0; waited < 30000 && !build_started.load();
             waited += 5) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        EXPECT_TRUE(build_started.load());
        // Destructor joins the running task; the raw ConcurrentVector outlives
        // it because IndexingRecord is destroyed before InsertRecord.
    }
    SUCCEED();
}
