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
#include <numeric>

#include "common/Types.h"
#include "common/IndexMeta.h"
#include "knowhere/comp/index_param.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "pb/schema.pb.h"
#include "pb/plan.pb.h"
#include "query/Plan.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/GenExprProto.h"

using namespace milvus::segcore;
using namespace milvus;
namespace pb = milvus::proto;

TEST(Growing, DeleteCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    int64_t c = 10;
    auto offset = 0;

    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    segment->Insert(offset,
                    c,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    Timestamp begin_ts = 100;
    auto tss = GenTss(c, begin_ts);
    auto del_pks = GenPKs(pks.begin(), pks.end());
    auto status = segment->Delete(c, del_pks.get(), tss.data());
    ASSERT_TRUE(status.ok());

    auto cnt = segment->get_deleted_count();
    ASSERT_EQ(cnt, c);
}

TEST(Growing, RealCount) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    int64_t c = 10;
    auto offset = 0;
    auto dataset = DataGen(schema, c);
    auto pks = dataset.get_col<int64_t>(pk);
    segment->Insert(offset,
                    c,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // no delete.
    ASSERT_EQ(c, segment->get_real_count());

    // delete half.
    auto half = c / 2;
    auto del_ids1 = GenPKs(pks.begin(), pks.begin() + half);
    auto del_tss1 = GenTss(half, c);
    auto status = segment->Delete(half, del_ids1.get(), del_tss1.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete duplicate.
    auto del_offset2 = segment->get_deleted_count();
    ASSERT_EQ(del_offset2, half);
    auto del_tss2 = GenTss(half, c + half);
    status = segment->Delete(half, del_ids1.get(), del_tss2.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(c - half, segment->get_real_count());

    // delete all.
    auto del_offset3 = segment->get_deleted_count();
    ASSERT_EQ(del_offset3, half);
    auto del_ids3 = GenPKs(pks.begin(), pks.end());
    auto del_tss3 = GenTss(c, c + half * 2);
    status = segment->Delete(c, del_ids3.get(), del_tss3.data());
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, segment->get_real_count());
}

class GrowingTest
    : public ::testing::TestWithParam<
          std::tuple</*index type*/ std::string, knowhere::MetricType>> {
 public:
    void
    SetUp() override {
        index_type = std::get<0>(GetParam());
        metric_type = std::get<1>(GetParam());
        if (index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
            index_type == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
            data_type = DataType::VECTOR_FLOAT;
        } else if (index_type ==
                       knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
                   index_type == knowhere::IndexEnum::INDEX_SPARSE_WAND) {
            data_type = DataType::VECTOR_SPARSE_U32_F32;
        } else {
            ASSERT_TRUE(false);
        }
    }
    knowhere::MetricType metric_type;
    std::string index_type;
    DataType data_type;
};

INSTANTIATE_TEST_SUITE_P(
    FloatGrowingTest,
    GrowingTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                          knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC),
        ::testing::Values(knowhere::metric::L2,
                          knowhere::metric::IP,
                          knowhere::metric::COSINE)));

INSTANTIATE_TEST_SUITE_P(
    SparseFloatGrowingTest,
    GrowingTest,
    ::testing::Combine(
        ::testing::Values(knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX,
                          knowhere::IndexEnum::INDEX_SPARSE_WAND),
        ::testing::Values(knowhere::metric::IP)));

TEST_P(GrowingTest, FillData) {
    auto schema = std::make_shared<Schema>();
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto timestamptz_field =
        schema->AddDebugField("timestamptz", DataType::TIMESTAMPTZ);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE);
    auto varchar_field = schema->AddDebugField("varchar", DataType::VARCHAR);
    auto json_field = schema->AddDebugField("json", DataType::JSON);
    auto geometry_field = schema->AddDebugField("geometry", DataType::GEOMETRY);
    auto int_array_field =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_field =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_field =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto float_array_field =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto vec = schema->AddDebugField("embeddings", data_type, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", index_type},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto bool_result = segment->bulk_subscript(
            nullptr, bool_field, ids_ds->GetIds(), num_inserted);
        auto int8_result = segment->bulk_subscript(
            nullptr, int8_field, ids_ds->GetIds(), num_inserted);
        auto int16_result = segment->bulk_subscript(
            nullptr, int16_field, ids_ds->GetIds(), num_inserted);
        auto int32_result = segment->bulk_subscript(
            nullptr, int32_field, ids_ds->GetIds(), num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto float_result = segment->bulk_subscript(
            nullptr, float_field, ids_ds->GetIds(), num_inserted);
        auto double_result = segment->bulk_subscript(
            nullptr, double_field, ids_ds->GetIds(), num_inserted);
        auto timestamptz_result = segment->bulk_subscript(
            nullptr, timestamptz_field, ids_ds->GetIds(), num_inserted);
        auto varchar_result = segment->bulk_subscript(
            nullptr, varchar_field, ids_ds->GetIds(), num_inserted);
        auto json_result = segment->bulk_subscript(
            nullptr, json_field, ids_ds->GetIds(), num_inserted);
        auto geometry_result = segment->bulk_subscript(
            nullptr, geometry_field, ids_ds->GetIds(), num_inserted);
        auto int_array_result = segment->bulk_subscript(
            nullptr, int_array_field, ids_ds->GetIds(), num_inserted);
        auto long_array_result = segment->bulk_subscript(
            nullptr, long_array_field, ids_ds->GetIds(), num_inserted);
        auto bool_array_result = segment->bulk_subscript(
            nullptr, bool_array_field, ids_ds->GetIds(), num_inserted);
        auto string_array_result = segment->bulk_subscript(
            nullptr, string_array_field, ids_ds->GetIds(), num_inserted);
        auto double_array_result = segment->bulk_subscript(
            nullptr, double_array_field, ids_ds->GetIds(), num_inserted);
        auto float_array_result = segment->bulk_subscript(
            nullptr, float_array_field, ids_ds->GetIds(), num_inserted);
        auto vec_result = segment->bulk_subscript(
            nullptr, vec, ids_ds->GetIds(), num_inserted);
        // checking result data
        EXPECT_EQ(bool_result->scalars().bool_data().data_size(), num_inserted);
        EXPECT_EQ(int8_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int16_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int32_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(timestamptz_result->scalars().timestamptz_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_result->scalars().float_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_result->scalars().double_data().data_size(),
                  num_inserted);
        EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
                  num_inserted);
        EXPECT_EQ(json_result->scalars().json_data().data_size(), num_inserted);
        EXPECT_EQ(geometry_result->scalars().geometry_data().data_size(),
                  num_inserted);
        if (data_type == DataType::VECTOR_FLOAT) {
            EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
                      num_inserted * dim);
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            EXPECT_EQ(
                vec_result->vectors().sparse_float_vector().contents_size(),
                num_inserted);
        } else {
            ASSERT_TRUE(false);
        }
        EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
                  num_inserted);

        EXPECT_EQ(bool_result->valid_data_size(), 0);
        EXPECT_EQ(int8_result->valid_data_size(), 0);
        EXPECT_EQ(int16_result->valid_data_size(), 0);
        EXPECT_EQ(int32_result->valid_data_size(), 0);
        EXPECT_EQ(int64_result->valid_data_size(), 0);
        EXPECT_EQ(float_result->valid_data_size(), 0);
        EXPECT_EQ(double_result->valid_data_size(), 0);
        EXPECT_EQ(timestamptz_result->valid_data_size(), 0);
        EXPECT_EQ(varchar_result->valid_data_size(), 0);
        EXPECT_EQ(json_result->valid_data_size(), 0);
        EXPECT_EQ(int_array_result->valid_data_size(), 0);
        EXPECT_EQ(long_array_result->valid_data_size(), 0);
        EXPECT_EQ(bool_array_result->valid_data_size(), 0);
        EXPECT_EQ(string_array_result->valid_data_size(), 0);
        EXPECT_EQ(double_array_result->valid_data_size(), 0);
        EXPECT_EQ(float_array_result->valid_data_size(), 0);
    }
}

TEST(Growing, FillNullableData) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::L2;
    auto bool_field = schema->AddDebugField("bool", DataType::BOOL, true);
    auto int8_field = schema->AddDebugField("int8", DataType::INT8, true);
    auto int16_field = schema->AddDebugField("int16", DataType::INT16, true);
    auto int32_field = schema->AddDebugField("int32", DataType::INT32, true);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto float_field = schema->AddDebugField("float", DataType::FLOAT, true);
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE, true);
    auto timestamptz_field =
        schema->AddDebugField("timestamptz", DataType::TIMESTAMPTZ, true);
    auto varchar_field =
        schema->AddDebugField("varchar", DataType::VARCHAR, true);
    auto json_field = schema->AddDebugField("json", DataType::JSON, true);
    auto int_array_field = schema->AddDebugField(
        "int_array", DataType::ARRAY, DataType::INT8, true);
    auto long_array_field = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    auto bool_array_field = schema->AddDebugField(
        "bool_array", DataType::ARRAY, DataType::BOOL, true);
    auto string_array_field = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR, true);
    auto double_array_field = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE, true);
    auto float_array_field = schema->AddDebugField(
        "float_array", DataType::ARRAY, DataType::FLOAT, true);
    auto vec = schema->AddDebugField(
        "embeddings", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params = {
        {"index_type", "IVF_FLAT"},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {{"dim", "128"}};
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto bool_result = segment->bulk_subscript(
            nullptr, bool_field, ids_ds->GetIds(), num_inserted);
        auto int8_result = segment->bulk_subscript(
            nullptr, int8_field, ids_ds->GetIds(), num_inserted);
        auto int16_result = segment->bulk_subscript(
            nullptr, int16_field, ids_ds->GetIds(), num_inserted);
        auto int32_result = segment->bulk_subscript(
            nullptr, int32_field, ids_ds->GetIds(), num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto float_result = segment->bulk_subscript(
            nullptr, float_field, ids_ds->GetIds(), num_inserted);
        auto double_result = segment->bulk_subscript(
            nullptr, double_field, ids_ds->GetIds(), num_inserted);
        auto timestamptz_result = segment->bulk_subscript(
            nullptr, timestamptz_field, ids_ds->GetIds(), num_inserted);
        auto varchar_result = segment->bulk_subscript(
            nullptr, varchar_field, ids_ds->GetIds(), num_inserted);
        auto json_result = segment->bulk_subscript(
            nullptr, json_field, ids_ds->GetIds(), num_inserted);
        auto int_array_result = segment->bulk_subscript(
            nullptr, int_array_field, ids_ds->GetIds(), num_inserted);
        auto long_array_result = segment->bulk_subscript(
            nullptr, long_array_field, ids_ds->GetIds(), num_inserted);
        auto bool_array_result = segment->bulk_subscript(
            nullptr, bool_array_field, ids_ds->GetIds(), num_inserted);
        auto string_array_result = segment->bulk_subscript(
            nullptr, string_array_field, ids_ds->GetIds(), num_inserted);
        auto double_array_result = segment->bulk_subscript(
            nullptr, double_array_field, ids_ds->GetIds(), num_inserted);
        auto float_array_result = segment->bulk_subscript(
            nullptr, float_array_field, ids_ds->GetIds(), num_inserted);
        auto vec_result = segment->bulk_subscript(
            nullptr, vec, ids_ds->GetIds(), num_inserted);

        EXPECT_EQ(bool_result->scalars().bool_data().data_size(), num_inserted);
        EXPECT_EQ(int8_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int16_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int32_result->scalars().int_data().data_size(), num_inserted);
        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_result->scalars().float_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_result->scalars().double_data().data_size(),
                  num_inserted);
        EXPECT_EQ(timestamptz_result->scalars().timestamptz_data().data_size(),
                  num_inserted);
        EXPECT_EQ(varchar_result->scalars().string_data().data_size(),
                  num_inserted);
        EXPECT_EQ(json_result->scalars().json_data().data_size(), num_inserted);
        EXPECT_EQ(vec_result->vectors().float_vector().data_size(),
                  num_inserted * dim);
        EXPECT_EQ(int_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(long_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(string_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(double_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(float_array_result->scalars().array_data().data_size(),
                  num_inserted);
        EXPECT_EQ(bool_result->valid_data_size(), num_inserted);
        EXPECT_EQ(int8_result->valid_data_size(), num_inserted);
        EXPECT_EQ(int16_result->valid_data_size(), num_inserted);
        EXPECT_EQ(int32_result->valid_data_size(), num_inserted);
        EXPECT_EQ(float_result->valid_data_size(), num_inserted);
        EXPECT_EQ(double_result->valid_data_size(), num_inserted);
        EXPECT_EQ(timestamptz_result->valid_data_size(), num_inserted);
        EXPECT_EQ(varchar_result->valid_data_size(), num_inserted);
        EXPECT_EQ(json_result->valid_data_size(), num_inserted);
        EXPECT_EQ(int_array_result->valid_data_size(), num_inserted);
        EXPECT_EQ(long_array_result->valid_data_size(), num_inserted);
        EXPECT_EQ(bool_array_result->valid_data_size(), num_inserted);
        EXPECT_EQ(string_array_result->valid_data_size(), num_inserted);
        EXPECT_EQ(double_array_result->valid_data_size(), num_inserted);
        EXPECT_EQ(float_array_result->valid_data_size(), num_inserted);
    }
}

class GrowingNullableTest : public ::testing::TestWithParam<
                                std::tuple</*data_type*/ DataType,
                                           /*metric_type*/ knowhere::MetricType,
                                           /*index_type*/ std::string,
                                           /*null_percent*/ int,
                                           /*enable_interim_index*/ bool>> {
 public:
    void
    SetUp() override {
        std::tie(data_type,
                 metric_type,
                 index_type,
                 null_percent,
                 enable_interim_index) = GetParam();
    }

    DataType data_type;
    knowhere::MetricType metric_type;
    std::string index_type;
    int null_percent;
    bool enable_interim_index;
};

static std::vector<
    std::tuple<DataType, knowhere::MetricType, std::string, int, bool>>
GenerateGrowingNullableTestParams() {
    std::vector<
        std::tuple<DataType, knowhere::MetricType, std::string, int, bool>>
        params;

    // Dense float vectors with IVF_FLAT
    std::vector<std::tuple<DataType, knowhere::MetricType, std::string>>
        base_configs = {
            {DataType::VECTOR_FLOAT,
             knowhere::metric::L2,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_FLOAT,
             knowhere::metric::IP,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_FLOAT,
             knowhere::metric::COSINE,
             knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {DataType::VECTOR_SPARSE_U32_F32,
             knowhere::metric::IP,
             knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX},
        };

    std::vector<int> null_percents = {0, 20, 100};

    std::vector<bool> interim_index_configs = {true, false};

    for (const auto& [dtype, metric, idx_type] : base_configs) {
        for (int null_pct : null_percents) {
            for (bool enable_interim : interim_index_configs) {
                params.push_back(
                    {dtype, metric, idx_type, null_pct, enable_interim});
            }
        }
    }
    return params;
}

INSTANTIATE_TEST_SUITE_P(
    NullableVectorParameters,
    GrowingNullableTest,
    ::testing::ValuesIn(GenerateGrowingNullableTestParams()));

TEST_P(GrowingNullableTest, SearchAndQueryNullableVectors) {
    using namespace milvus::query;

    bool nullable = true;

    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    int64_t dim = 8;
    auto vec = schema->AddDebugField(
        "embeddings", data_type, dim, metric_type, nullable);
    schema->set_primary_field_id(int64_field);

    std::map<std::string, std::string> index_params;
    std::map<std::string, std::string> type_params;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        index_params = {{"index_type", index_type},
                        {"metric_type", metric_type}};
        type_params = {};
    } else {
        index_params = {{"index_type", index_type},
                        {"metric_type", metric_type},
                        {"nlist", "128"}};
        type_params = {{"dim", std::to_string(dim)}};
    }
    FieldIndexMeta fieldIndexMeta(
        vec, std::move(index_params), std::move(type_params));
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(enable_interim_index);
    // Explicitly set interim index type to avoid contamination from other tests
    config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC);
    std::map<FieldId, FieldIndexMeta> filedMap = {{vec, fieldIndexMeta}};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());

    int64_t batch_size = 2000;
    int64_t num_rounds = 10;
    int64_t topk = 5;
    int64_t num_queries = 2;
    Timestamp timestamp = 10000000;

    // Prepare search plan
    std::string search_params_fmt;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        search_params_fmt = R"(
            vector_anns:<
                field_id: {}
                query_info:<
                    topk: {}
                    round_decimal: 3
                    metric_type: "{}"
                    search_params: "{{\"drop_ratio_search\": 0.1}}"
                >
                placeholder_tag: "$0"
            >
        )";
    } else {
        search_params_fmt = R"(
            vector_anns:<
                field_id: {}
                query_info:<
                    topk: {}
                    round_decimal: 3
                    metric_type: "{}"
                    search_params: "{{\"nprobe\": 10}}"
                >
                placeholder_tag: "$0"
            >
        )";
    }

    auto raw_plan =
        fmt::format(search_params_fmt, vec.get(), topk, metric_type);
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan.c_str());
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    // Create query vectors
    proto::common::PlaceholderGroup ph_group_raw;
    if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        ph_group_raw = CreateSparseFloatPlaceholderGroup(num_queries, 42);
    } else {
        auto query_data = generate_float_vector(num_queries, dim);
        ph_group_raw =
            CreatePlaceholderGroupFromBlob(num_queries, dim, query_data.data());
    }

    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    // Store all inserted data for verification
    // For nullable vectors, data is stored sparsely (only valid vectors)
    // We need a mapping from logical offset to physical offset
    std::vector<float> all_float_vectors;  // Physical storage (only valid)
    std::vector<knowhere::sparse::SparseRow<float>> all_sparse_vectors;
    std::vector<bool> all_valid_data;  // Logical storage (all rows)
    std::vector<int64_t>
        logical_to_physical;  // Maps logical offset to physical

    // Insert data in multiple rounds and test after each round
    for (int64_t round = 0; round < num_rounds; round++) {
        int64_t total_rows = (round + 1) * batch_size;
        int64_t expected_valid_count =
            total_rows - (total_rows * null_percent / 100);

        auto dataset = DataGen(schema,
                               batch_size,
                               42 + round,
                               0,
                               1,
                               10,
                               1,
                               false,
                               true,
                               false,
                               null_percent);

        // Build logical to physical mapping for this batch
        int64_t base_physical = all_float_vectors.size() / dim;
        if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            base_physical = all_sparse_vectors.size();
        }

        auto valid_data_from_dataset = dataset.get_col_valid(vec);
        int64_t physical_idx = base_physical;
        for (size_t i = 0; i < valid_data_from_dataset.size(); i++) {
            if (valid_data_from_dataset[i]) {
                logical_to_physical.push_back(physical_idx);
                physical_idx++;
            } else {
                logical_to_physical.push_back(-1);  // null
            }
        }

        // Get original data directly from proto (sparse storage for nullable)
        // Data is stored sparsely - only valid vectors are in the proto
        if (data_type == DataType::VECTOR_FLOAT) {
            auto field_data = dataset.get_col(vec);
            auto& float_data = field_data->vectors().float_vector().data();
            all_float_vectors.insert(
                all_float_vectors.end(), float_data.begin(), float_data.end());
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            auto field_data = dataset.get_col(vec);
            auto& sparse_array = field_data->vectors().sparse_float_vector();
            for (int i = 0; i < sparse_array.contents_size(); i++) {
                auto& content = sparse_array.contents(i);
                auto row = CopyAndWrapSparseRow(content.data(), content.size());
                all_sparse_vectors.push_back(std::move(row));
            }
        }
        all_valid_data.insert(all_valid_data.end(),
                              valid_data_from_dataset.begin(),
                              valid_data_from_dataset.end());

        auto offset = segment->PreInsert(batch_size);
        segment->Insert(offset,
                        batch_size,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        auto& insert_record = segment->get_insert_record();
        ASSERT_TRUE(insert_record.is_valid_data_exist(vec));

        auto valid_data_ptr = insert_record.get_data_base(vec);
        const auto& valid_data = valid_data_ptr->get_valid_data();

        // Test search
        auto sr =
            segment_growing->Search(plan.get(), ph_group.get(), timestamp);

        ASSERT_EQ(sr->total_nq_, num_queries);
        ASSERT_EQ(sr->unity_topK_, topk);

        if (expected_valid_count == 0) {
            auto total_results = sr->get_total_result_count();
            EXPECT_EQ(total_results, 0)
                << "Round " << round
                << ": 100% null should return 0 results, but got "
                << total_results;
        } else {
            // Verify search results don't contain null vectors
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto seg_offset = sr->seg_offsets_[i];
                if (seg_offset < 0) {
                    continue;
                }
                ASSERT_TRUE(valid_data[seg_offset])
                    << "Round " << round
                    << ": Search returned null vector at offset " << seg_offset;
            }
        }

        auto vec_result = segment->bulk_subscript(
            nullptr, vec, sr->seg_offsets_.data(), sr->seg_offsets_.size());
        ASSERT_TRUE(vec_result != nullptr);

        if (data_type == DataType::VECTOR_FLOAT) {
            auto& float_data = vec_result->vectors().float_vector();
            size_t valid_idx = 0;
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto offset = sr->seg_offsets_[i];
                if (offset < 0) {
                    continue;  // Skip invalid offsets
                }
                auto physical_idx = logical_to_physical[offset];
                for (int d = 0; d < dim; d++) {
                    float expected_val =
                        all_float_vectors[physical_idx * dim + d];
                    float actual_val = float_data.data(valid_idx * dim + d);
                    ASSERT_FLOAT_EQ(expected_val, actual_val)
                        << "Round " << round << ": Mismatch at logical offset "
                        << offset << " dim " << d;
                }
                valid_idx++;
            }
        } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
            auto& sparse_data = vec_result->vectors().sparse_float_vector();
            size_t valid_idx = 0;
            for (size_t i = 0; i < sr->seg_offsets_.size(); i++) {
                auto offset = sr->seg_offsets_[i];
                if (offset < 0) {
                    continue;  // Skip invalid offsets
                }
                auto physical_idx = logical_to_physical[offset];
                auto& content = sparse_data.contents(valid_idx);
                auto retrieved_row =
                    CopyAndWrapSparseRow(content.data(), content.size());
                const auto& expected_row = all_sparse_vectors[physical_idx];
                ASSERT_EQ(retrieved_row.size(), expected_row.size())
                    << "Round " << round
                    << ": Sparse vector size mismatch at logical offset "
                    << offset;
                for (size_t j = 0; j < retrieved_row.size(); j++) {
                    ASSERT_EQ(retrieved_row[j].id, expected_row[j].id)
                        << "Round " << round
                        << ": Sparse vector id mismatch at logical offset "
                        << offset << " element " << j;
                    ASSERT_FLOAT_EQ(retrieved_row[j].val, expected_row[j].val)
                        << "Round " << round
                        << ": Sparse vector val mismatch at logical offset "
                        << offset << " element " << j;
                }
                valid_idx++;
            }
        }
    }
}

TEST_P(GrowingTest, FillVectorArrayData) {
    auto schema = std::make_shared<Schema>();
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector = schema->AddDebugVectorArrayField(
        "array_float_vector", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));
    auto segment_growing = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segment = dynamic_cast<SegmentGrowingImpl*>(segment_growing.get());
    int64_t per_batch = 1000;
    int64_t n_batch = 3;
    int64_t dim = 128;
    for (int64_t i = 0; i < n_batch; i++) {
        auto dataset = DataGen(schema, per_batch);

        auto offset = segment->PreInsert(per_batch);
        segment->Insert(offset,
                        per_batch,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        auto num_inserted = (i + 1) * per_batch;
        auto ids_ds = GenRandomIds(num_inserted);
        auto int64_result = segment->bulk_subscript(
            nullptr, int64_field, ids_ds->GetIds(), num_inserted);
        auto array_float_vector_result = segment->bulk_subscript(
            nullptr, array_float_vector, ids_ds->GetIds(), num_inserted);

        EXPECT_EQ(int64_result->scalars().long_data().data_size(),
                  num_inserted);
        EXPECT_EQ(
            array_float_vector_result->vectors().vector_array().data_size(),
            num_inserted);

        if (i == 0) {
            // Verify vector array data
            auto verify_float_vectors = [](auto arr1, auto arr2) {
                static constexpr float EPSILON = 1e-6;
                EXPECT_EQ(arr1.size(), arr2.size());
                for (int64_t i = 0; i < arr1.size(); ++i) {
                    EXPECT_NEAR(arr1[i], arr2[i], EPSILON);
                }
            };

            auto array_vec_values =
                dataset.get_col<VectorFieldProto>(array_float_vector);
            for (int64_t i = 0; i < per_batch; ++i) {
                auto arrow_array = array_float_vector_result->vectors()
                                       .vector_array()
                                       .data()[i]
                                       .float_vector()
                                       .data();
                auto expected_array =
                    array_vec_values[ids_ds->GetIds()[i]].float_vector().data();
                verify_float_vectors(arrow_array, expected_array);
            }
        }

        EXPECT_EQ(int64_result->valid_data_size(), 0);
        EXPECT_EQ(array_float_vector_result->valid_data_size(), 0);
    }
}

TEST(GrowingTest, LoadVectorArrayData) {
    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::MAX_SIM;
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_float_vector = schema->AddDebugVectorArrayField(
        "array_vec", DataType::VECTOR_FLOAT, 128, metric_type);
    schema->set_primary_field_id(int64_field);

    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);
    std::map<FieldId, FieldIndexMeta> filedMap = {};
    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(filedMap));

    int64_t dataset_size = 1000;
    int64_t dim = 128;
    auto dataset = DataGen(schema, dataset_size);
    auto segment_growing =
        CreateGrowingWithFieldDataLoaded(schema, metaPtr, config, dataset);
    auto segment = segment_growing.get();

    // Verify data
    auto int64_values = dataset.get_col<int64_t>(int64_field);
    auto array_vec_values =
        dataset.get_col<VectorFieldProto>(array_float_vector);

    auto ids_ds = GenRandomIds(dataset_size);
    auto int64_result = segment->bulk_subscript(
        nullptr, int64_field, ids_ds->GetIds(), dataset_size);
    auto array_float_vector_result = segment->bulk_subscript(
        nullptr, array_float_vector, ids_ds->GetIds(), dataset_size);

    EXPECT_EQ(int64_result->scalars().long_data().data_size(), dataset_size);
    EXPECT_EQ(array_float_vector_result->vectors().vector_array().data_size(),
              dataset_size);

    auto verify_float_vectors = [](auto arr1, auto arr2) {
        static constexpr float EPSILON = 1e-6;
        EXPECT_EQ(arr1.size(), arr2.size());
        for (int64_t i = 0; i < arr1.size(); ++i) {
            EXPECT_NEAR(arr1[i], arr2[i], EPSILON);
        }
    };

    for (int64_t i = 0; i < dataset_size; ++i) {
        auto arrow_array = array_float_vector_result->vectors()
                               .vector_array()
                               .data()[i]
                               .float_vector()
                               .data();
        auto expected_array =
            array_vec_values[ids_ds->GetIds()[i]].float_vector().data();
        verify_float_vectors(arrow_array, expected_array);
    }
}

TEST(Growing, TestMaskWithTTLField) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto ttl_fid = schema->AddDebugField("ttl_field", DataType::INT64, false);
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    int64_t test_data_count = 100;

    uint64_t base_ts = 1000000000ULL << 18;
    std::vector<Timestamp> ts_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = base_ts + i;
    }

    std::vector<idx_t> row_ids(test_data_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);

    std::vector<int64_t> pk_data(test_data_count);
    std::iota(pk_data.begin(), pk_data.end(), 0);

    uint64_t base_physical_us = (base_ts >> 18) * 1000;
    std::vector<int64_t> ttl_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        if (i < test_data_count / 2) {
            ttl_data[i] = static_cast<int64_t>(base_physical_us - 10);
        } else {
            ttl_data[i] = static_cast<int64_t>(base_physical_us + 10);
        }
    }

    auto insert_record_proto = std::make_unique<InsertRecordProto>();
    insert_record_proto->set_num_rows(test_data_count);

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(pk_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : pk_data) {
            data->add_data(v);
        }
    }

    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(ttl_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : ttl_data) {
            data->add_data(v);
        }
    }

    auto offset = segment->PreInsert(test_data_count);
    segment->Insert(offset,
                    test_data_count,
                    row_ids.data(),
                    ts_data.data(),
                    insert_record_proto.get());

    BitsetType bitset(test_data_count);
    BitsetTypeView bitset_view(bitset);

    Timestamp query_ts = base_ts + test_data_count;
    segment_impl->mask_with_timestamps(bitset_view, query_ts, 0);

    int expired_count = 0;
    for (int i = 0; i < test_data_count; i++) {
        if (bitset_view[i]) {
            expired_count++;
        }
    }

    EXPECT_EQ(expired_count, test_data_count / 2);
    for (int i = 0; i < test_data_count / 2; i++) {
        EXPECT_TRUE(bitset_view[i]) << "Row " << i << " should be expired";
    }
    for (int i = test_data_count / 2; i < test_data_count; i++) {
        EXPECT_FALSE(bitset_view[i]) << "Row " << i << " should not be expired";
    }
}

// Test TTL field filtering with nullable field for Growing segment
TEST(Growing, TestMaskWithNullableTTLField) {
    // Create schema with nullable TTL field
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto ttl_fid =
        schema->AddDebugField("ttl_field", DataType::INT64, true);  // nullable
    schema->set_primary_field_id(pk_fid);
    schema->set_ttl_field_id(ttl_fid);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto segment_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    int64_t test_data_count = 100;

    // Generate timestamp data
    uint64_t base_ts = 1000000000ULL << 18;
    std::vector<Timestamp> ts_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        ts_data[i] = base_ts + i;
    }

    // Generate row IDs
    std::vector<idx_t> row_ids(test_data_count);
    std::iota(row_ids.begin(), row_ids.end(), 0);

    // Generate PK data
    std::vector<int64_t> pk_data(test_data_count);
    std::iota(pk_data.begin(), pk_data.end(), 0);

    // Generate TTL data with some nulls
    uint64_t base_physical_us = (base_ts >> 18) * 1000;
    std::vector<int64_t> ttl_data(test_data_count);
    std::vector<bool> valid_data(test_data_count);
    for (int i = 0; i < test_data_count; i++) {
        if (i % 4 == 0) {
            // Null value - should not expire
            ttl_data[i] = 0;
            valid_data[i] = false;
        } else if (i % 4 == 1) {
            // Expired
            ttl_data[i] = static_cast<int64_t>(base_physical_us - 10);
            valid_data[i] = true;
        } else {
            // Not expired
            ttl_data[i] = static_cast<int64_t>(base_physical_us + 10);
            valid_data[i] = true;
        }
    }

    // Create insert record proto
    auto insert_record_proto = std::make_unique<InsertRecordProto>();
    insert_record_proto->set_num_rows(test_data_count);

    // Add PK field data
    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(pk_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : pk_data) {
            data->add_data(v);
        }
    }

    // Add nullable TTL field data
    {
        auto field_data = insert_record_proto->add_fields_data();
        field_data->set_field_id(ttl_fid.get());
        field_data->set_type(proto::schema::DataType::Int64);
        auto* scalars = field_data->mutable_scalars();
        auto* data = scalars->mutable_long_data();
        for (auto v : ttl_data) {
            data->add_data(v);
        }
        // Add valid_data for nullable field
        for (auto v : valid_data) {
            field_data->add_valid_data(v);
        }
    }

    // Insert data
    auto offset = segment->PreInsert(test_data_count);
    segment->Insert(offset,
                    test_data_count,
                    row_ids.data(),
                    ts_data.data(),
                    insert_record_proto.get());

    // Test mask_with_timestamps with nullable TTL field
    BitsetType bitset(test_data_count);
    BitsetTypeView bitset_view(bitset);

    Timestamp query_ts = base_ts + test_data_count;
    segment_impl->mask_with_timestamps(bitset_view, query_ts, 0);

    // Verify results:
    // - i % 4 == 0: null, should NOT be expired
    // - i % 4 == 1: expired (TTL < current time)
    // - i % 4 == 2 or 3: not expired
    int expired_count = 0;
    for (int i = 0; i < test_data_count; i++) {
        if (i % 4 == 0) {
            // Null value should not be expired
            EXPECT_FALSE(bitset_view[i])
                << "Row " << i << " (null) should not be expired";
        } else if (i % 4 == 1) {
            // Should be expired
            EXPECT_TRUE(bitset_view[i]) << "Row " << i << " should be expired";
            expired_count++;
        } else {
            // Should not be expired
            EXPECT_FALSE(bitset_view[i])
                << "Row " << i << " should not be expired";
        }
    }

    EXPECT_EQ(expired_count, test_data_count / 4);
}