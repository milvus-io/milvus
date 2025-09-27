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

#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "pb/schema.pb.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

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

TEST(GrowingTest, SearchVectorArray) {
    using namespace milvus::query;

    auto schema = std::make_shared<Schema>();
    auto metric_type = knowhere::metric::MAX_SIM;

    auto dim = 32;

    // Add fields
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    auto array_vec = schema->AddDebugVectorArrayField(
        "array_vec", DataType::VECTOR_FLOAT, dim, metric_type);
    schema->set_primary_field_id(int64_field);

    // Configure segment
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(1024);
    config.set_enable_interim_segment_index(true);

    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_EMB_LIST_HNSW},
        {"metric_type", metric_type},
        {"nlist", "128"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta fieldIndexMeta(
        array_vec, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> fieldMap = {{array_vec, fieldIndexMeta}};

    IndexMetaPtr metaPtr =
        std::make_shared<CollectionIndexMeta>(100000, std::move(fieldMap));
    auto segment = CreateGrowingSegment(schema, metaPtr, 1, config);
    auto segmentImplPtr = dynamic_cast<SegmentGrowingImpl*>(segment.get());

    // Insert data
    int64_t N = 100;
    uint64_t seed = 42;
    int emb_list_len = 5;  // Each row contains 5 vectors
    auto dataset = DataGen(schema, N, seed, 0, 1, emb_list_len);

    auto offset = 0;
    segment->Insert(offset,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // Prepare search query
    int vec_num = 10;  // Total number of query vectors
    std::vector<float> query_vec = generate_float_vector(vec_num, dim);

    // Create query dataset with lims for VectorArray
    std::vector<size_t> query_vec_lims;
    query_vec_lims.push_back(0);  // First query has 3 vectors
    query_vec_lims.push_back(3);
    query_vec_lims.push_back(10);  // Second query has 7 vectors

    // Create search plan
    const char* raw_plan = R"(vector_anns: <
                                  field_id: 101
                                  query_info: <
                                    topk: 5
                                    round_decimal: 3
                                    metric_type: "MAX_SIM"
                                    search_params: "{\"nprobe\": 10}"
                                  >
                                  placeholder_tag: "$0"
      >)";

    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());

    // Use CreatePlaceholderGroupFromBlob for VectorArray
    auto ph_group_raw = CreatePlaceholderGroupFromBlob<EmbListFloatVector>(
        vec_num, dim, query_vec.data(), query_vec_lims);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    // Execute search
    Timestamp timestamp = 10000000;
    auto sr = segment->Search(plan.get(), ph_group.get(), timestamp);
    auto sr_parsed = SearchResultToJson(*sr);
    std::cout << sr_parsed.dump(1) << std::endl;
}
