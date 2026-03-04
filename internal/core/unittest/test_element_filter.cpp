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
#include <stddef.h>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "NamedType/named_type_impl.hpp"
#include "common/ArrayOffsets.h"
#include "common/IndexMeta.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/VectorTrait.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/Meta.h"
#include "index/ScalarIndexSort.h"
#include "index/VectorIndex.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/operands.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "query/PlanNode.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/FileManager.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

// Test parameter: <use_hints, load_index, element_type, metric_type, dim>
using ElementFilterSealedParam =
    std::tuple<bool, bool, DataType, std::string, int>;

class ElementFilterSealed
    : public ::testing::TestWithParam<ElementFilterSealedParam> {
 protected:
    bool
    use_hints() const {
        return std::get<0>(GetParam());
    }
    bool
    load_index() const {
        return std::get<1>(GetParam());
    }
    DataType
    element_type() const {
        return std::get<2>(GetParam());
    }
    std::string
    metric_type() const {
        return std::get<3>(GetParam());
    }
    int
    vec_dim() const {
        return std::get<4>(GetParam());
    }

    // Create placeholder group with element_level = true for element-level search
    // Uses regular vector types (not EmbList), as query is single embedding per query
    proto::common::PlaceholderGroup
    CreatePlaceholderGroupForType(int num_queries, int dim, int seed) {
        if (element_type() == DataType::VECTOR_BINARY) {
            return CreatePlaceholderGroup<milvus::BinaryVector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_FLOAT16) {
            return CreatePlaceholderGroup<milvus::Float16Vector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_BFLOAT16) {
            return CreatePlaceholderGroup<milvus::BFloat16Vector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_INT8) {
            return CreatePlaceholderGroup<milvus::Int8Vector>(
                num_queries, dim, seed, true);
        } else {
            // VECTOR_FLOAT
            return CreatePlaceholderGroup<milvus::FloatVector>(
                num_queries, dim, seed, true);
        }
    }
};

TEST_P(ElementFilterSealed, RangeExpr) {
    bool with_hints = use_hints();
    bool with_load_index = load_index();
    DataType elem_type = element_type();
    std::string metric = metric_type();
    int dim = vec_dim();

    // Step 1: Prepare schema with array field
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField(
        "structA[array_vec]", elem_type, dim, metric);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    int array_len = 3;

    // Step 2: Generate test data
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (size_t row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = row * array_len + elem + 1;
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                }
            }
            break;
        }
    }

    // Step 3: Create sealed segment with field data
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Step 4: Load vector index for element-level search
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);

    // Flatten vector data and build index based on element type
    std::unique_ptr<milvus::index::VectorIndex> indexing;
    std::string actual_metric;

    if (elem_type == DataType::VECTOR_FLOAT) {
        std::vector<float> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& float_vec = array_vec_values[i].float_vector().data();
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = float_vec[j];
            }
        }
        indexing = GenVecIndexing(N * array_len,
                                  dim,
                                  vector_data.data(),
                                  knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_FLOAT16) {
        std::vector<knowhere::fp16> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& fp16_data = array_vec_values[i].float16_vector();
            const knowhere::fp16* src =
                reinterpret_cast<const knowhere::fp16*>(fp16_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingFloat16(N * array_len,
                                         dim,
                                         vector_data.data(),
                                         knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_BFLOAT16) {
        std::vector<knowhere::bf16> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& bf16_data = array_vec_values[i].bfloat16_vector();
            const knowhere::bf16* src =
                reinterpret_cast<const knowhere::bf16*>(bf16_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingBFloat16(N * array_len,
                                          dim,
                                          vector_data.data(),
                                          knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_INT8) {
        std::vector<int8_t> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& int8_data = array_vec_values[i].int8_vector();
            const int8_t* src =
                reinterpret_cast<const int8_t*>(int8_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingInt8(N * array_len,
                                      dim,
                                      vector_data.data(),
                                      knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_BINARY) {
        int byte_dim = (dim + 7) / 8;
        std::vector<uint8_t> vector_data(byte_dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& bin_data = array_vec_values[i].binary_vector();
            const uint8_t* src =
                reinterpret_cast<const uint8_t*>(bin_data.data());
            for (int j = 0; j < array_len * byte_dim; j++) {
                vector_data[i * array_len * byte_dim + j] = src[j];
            }
        }
        indexing =
            GenVecIndexingBinary(N * array_len,
                                 dim,
                                 vector_data.data(),
                                 knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP);
        actual_metric = knowhere::metric::HAMMING;
    }

    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = actual_metric;
    load_index_info.field_type = DataType::VECTOR_ARRAY;
    load_index_info.element_type = elem_type;
    if (with_load_index) {
        segment->LoadIndex(load_index_info);
    }

    int topK = 5;

    // Step 5: Test with element-level filter
    // Query: Search array elements, filter by element_value in (100, 400) and id % 2 == 0
    {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        // Expression: id % 2 == 0 && element_filter(structA, 400 > $[price_array] > 100)
        // binary_range with lower_inclusive=false, upper_inclusive=false means: 100 < x < 400
        std::string expr =
            "id % 2 == 0 && element_filter(structA, 400 > $[price_array] > "
            "100)";

        auto plan_bytes = handle.ParseSearch(
            expr, "structA[array_vec]", topK, metric, search_params, 3);

        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroupForType(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results
        ASSERT_NE(search_result, nullptr);

        // In element-level mode, results should be element indices, not doc offsets
        ASSERT_TRUE(search_result->element_level_);
        ASSERT_FALSE(search_result->element_indices_.empty());
        // Also check seg_offsets_ which stores the doc IDs
        ASSERT_FALSE(search_result->seg_offsets_.empty());
        ASSERT_EQ(search_result->element_indices_.size(),
                  search_result->seg_offsets_.size());

        // Should have topK results per query
        ASSERT_LE(search_result->element_indices_.size(),
                  static_cast<size_t>(topK * num_queries));

        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            int32_t elem_idx = search_result->element_indices_[i];
            float distance = search_result->distances_[i];

            std::cout << "doc_id: " << doc_id << ", element_index: " << elem_idx
                      << ", distance: " << distance << std::endl;

            // Verify the doc_id satisfies the predicate (id % 2 == 0)
            ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                     << " should satisfy (id % 2 == 0)";

            // Verify element value is in range (100, 400)
            // Element value = doc_id * array_len + elem_idx + 1
            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_GT(element_value, 100)
                << "Element value " << element_value << " should be > 100";
            ASSERT_LT(element_value, 400)
                << "Element value " << element_value << " should be < 400";
        }

        // Verify distances are sorted
        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order";
        }
    }
}

TEST_P(ElementFilterSealed, UnaryExpr) {
    bool with_hints = use_hints();
    bool with_load_index = load_index();
    DataType elem_type = element_type();
    std::string metric = metric_type();
    int dim = vec_dim();

    // Step 1: Prepare schema with array field
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField(
        "structA[array_vec]", elem_type, dim, metric);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    int array_len = 3;

    // Step 2: Generate test data
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (size_t row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = row * array_len + elem + 1;
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                }
            }
            break;
        }
    }

    // Step 3: Create sealed segment with field data
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Step 4: Load vector index for element-level search
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);

    // Flatten vector data and build index based on element type
    std::unique_ptr<milvus::index::VectorIndex> indexing;
    std::string actual_metric;

    if (elem_type == DataType::VECTOR_FLOAT) {
        std::vector<float> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& float_vec = array_vec_values[i].float_vector().data();
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = float_vec[j];
            }
        }
        indexing = GenVecIndexing(N * array_len,
                                  dim,
                                  vector_data.data(),
                                  knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_FLOAT16) {
        std::vector<knowhere::fp16> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& fp16_data = array_vec_values[i].float16_vector();
            const knowhere::fp16* src =
                reinterpret_cast<const knowhere::fp16*>(fp16_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingFloat16(N * array_len,
                                         dim,
                                         vector_data.data(),
                                         knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_BFLOAT16) {
        std::vector<knowhere::bf16> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& bf16_data = array_vec_values[i].bfloat16_vector();
            const knowhere::bf16* src =
                reinterpret_cast<const knowhere::bf16*>(bf16_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingBFloat16(N * array_len,
                                          dim,
                                          vector_data.data(),
                                          knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_INT8) {
        std::vector<int8_t> vector_data(dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& int8_data = array_vec_values[i].int8_vector();
            const int8_t* src =
                reinterpret_cast<const int8_t*>(int8_data.data());
            for (int j = 0; j < array_len * dim; j++) {
                vector_data[i * array_len * dim + j] = src[j];
            }
        }
        indexing = GenVecIndexingInt8(N * array_len,
                                      dim,
                                      vector_data.data(),
                                      knowhere::IndexEnum::INDEX_HNSW);
        actual_metric = knowhere::metric::L2;
    } else if (elem_type == DataType::VECTOR_BINARY) {
        int byte_dim = (dim + 7) / 8;
        std::vector<uint8_t> vector_data(byte_dim * N * array_len);
        for (size_t i = 0; i < N; i++) {
            const auto& bin_data = array_vec_values[i].binary_vector();
            const uint8_t* src =
                reinterpret_cast<const uint8_t*>(bin_data.data());
            for (int j = 0; j < array_len * byte_dim; j++) {
                vector_data[i * array_len * byte_dim + j] = src[j];
            }
        }
        indexing =
            GenVecIndexingBinary(N * array_len,
                                 dim,
                                 vector_data.data(),
                                 knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP);
        actual_metric = knowhere::metric::HAMMING;
    }

    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = actual_metric;
    load_index_info.field_type = DataType::VECTOR_ARRAY;
    load_index_info.element_type = elem_type;
    if (with_load_index) {
        segment->LoadIndex(load_index_info);
    }

    int topK = 5;

    // Step 5: Test with element-level filter
    // Query: Search array elements, filter by element_value > 10 and id % 2 == 0
    {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        // Expression: id % 2 == 0 && element_filter(structA, $[price_array] > 10)
        std::string expr =
            "id % 2 == 0 && element_filter(structA, $[price_array] > 10)";

        auto plan_bytes = handle.ParseSearch(
            expr, "structA[array_vec]", topK, metric, search_params, 3);

        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroupForType(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results
        ASSERT_NE(search_result, nullptr);

        // In element-level mode, results should be element indices, not doc offsets
        ASSERT_TRUE(search_result->element_level_);
        ASSERT_FALSE(search_result->element_indices_.empty());
        ASSERT_FALSE(search_result->seg_offsets_.empty());
        ASSERT_EQ(search_result->element_indices_.size(),
                  search_result->seg_offsets_.size());

        ASSERT_LE(search_result->element_indices_.size(),
                  static_cast<size_t>(topK * num_queries));

        std::cout << "Element-level search returned ("
                  << static_cast<int>(elem_type) << "):" << std::endl;
        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            std::cout << "doc_id: " << search_result->seg_offsets_[i]
                      << ", element_index: "
                      << search_result->element_indices_[i]
                      << ", distance: " << search_result->distances_[i]
                      << std::endl;
        }

        // Verify distances are sorted
        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order";
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    ElementFilter,
    ElementFilterSealed,
    ::testing::Values(
        // FloatVector with L2
        std::make_tuple(false, false, DataType::VECTOR_FLOAT, "L2", 4),
        std::make_tuple(false, true, DataType::VECTOR_FLOAT, "L2", 4),
        std::make_tuple(true, false, DataType::VECTOR_FLOAT, "L2", 4),
        std::make_tuple(true, true, DataType::VECTOR_FLOAT, "L2", 4),
        // Float16Vector with L2
        std::make_tuple(false, true, DataType::VECTOR_FLOAT16, "L2", 4),
        std::make_tuple(true, true, DataType::VECTOR_FLOAT16, "L2", 4),
        // BFloat16Vector with L2
        std::make_tuple(false, true, DataType::VECTOR_BFLOAT16, "L2", 4),
        std::make_tuple(true, true, DataType::VECTOR_BFLOAT16, "L2", 4),
        // Int8Vector with L2
        std::make_tuple(false, true, DataType::VECTOR_INT8, "L2", 4),
        std::make_tuple(true, true, DataType::VECTOR_INT8, "L2", 4),
        // BinaryVector with HAMMING (no hints - BIN_FLAT doesn't support iterative filter)
        std::make_tuple(false, true, DataType::VECTOR_BINARY, "HAMMING", 32)),
    [](const ::testing::TestParamInfo<ElementFilterSealedParam>& info) {
        bool with_hints = std::get<0>(info.param);
        bool with_load_index = std::get<1>(info.param);
        DataType elem_type = std::get<2>(info.param);
        std::string metric = std::get<3>(info.param);

        std::string type_name;
        switch (elem_type) {
            case DataType::VECTOR_FLOAT:
                type_name = "Float";
                break;
            case DataType::VECTOR_FLOAT16:
                type_name = "Float16";
                break;
            case DataType::VECTOR_BFLOAT16:
                type_name = "BFloat16";
                break;
            case DataType::VECTOR_INT8:
                type_name = "Int8";
                break;
            case DataType::VECTOR_BINARY:
                type_name = "Binary";
                break;
            default:
                type_name = "Unknown";
        }

        std::string name = type_name + "_" + metric;
        name += with_hints ? "_WithHints" : "_NoHints";
        name += with_load_index ? "_WithIndex" : "_NoIndex";
        return name;
    });

TEST(ElementFilter, GrowingSegmentArrayOffsetsGrowing) {
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_float_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    int array_len = 3;

    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    // Customize int_array data: doc i has elements [i*3+1, i*3+2, i*3+3]
    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (int row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = row * array_len + elem + 1;
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                }
            }
            break;
        }
    }

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);

    // Both fields should share the same ArrayOffsetsGrowing
    auto offsets_vec = growing_impl->GetArrayOffsets(vec_fid);
    auto offsets_int = growing_impl->GetArrayOffsets(int_array_fid);
    ASSERT_NE(offsets_vec, nullptr);
    ASSERT_NE(offsets_int, nullptr);

    // Should point to the same object (shared)
    ASSERT_EQ(offsets_vec, offsets_int)
        << "Fields in same struct should share ArrayOffsetsGrowing";

    // Verify counts
    ASSERT_EQ(offsets_vec->GetRowCount(), N)
        << "Should have " << N << " documents";
    ASSERT_EQ(offsets_vec->GetTotalElementCount(), N * array_len)
        << "Should have " << N * array_len << " total elements";

    for (int64_t doc_id = 0; doc_id < N; ++doc_id) {
        for (int32_t elem_idx = 0; elem_idx < array_len; ++elem_idx) {
            int64_t elem_id = doc_id * array_len + elem_idx;
            auto [mapped_doc, mapped_idx] =
                offsets_vec->ElementIDToRowID(elem_id);

            ASSERT_EQ(mapped_doc, doc_id)
                << "Element " << elem_id << " should map to doc " << doc_id;
            ASSERT_EQ(mapped_idx, elem_idx)
                << "Element " << elem_id << " should have index " << elem_idx;
        }
    }
}

TEST(ElementFilter, GrowingSegmentOutOfOrderInsert) {
    // Test out-of-order Insert handling in ArrayOffsetsGrowing
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_float_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    int array_len = 3;

    // Create growing segment
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    // Simulate out-of-order inserts
    // Insert docs [10-19], [0-9], [20-29]
    auto gen_batch = [&](int64_t start, int64_t count) {
        auto batch = DataGen(schema, count, 42 + start, start, 1, array_len);

        // Customize int_array data
        for (int i = 0; i < batch.raw_->fields_data_size(); i++) {
            auto* field_data = batch.raw_->mutable_fields_data(i);
            if (field_data->field_id() == int_array_fid.get()) {
                field_data->mutable_scalars()
                    ->mutable_array_data()
                    ->mutable_data()
                    ->Clear();

                for (int row = 0; row < count; row++) {
                    int64_t global_row = start + row;
                    auto* array_data = field_data->mutable_scalars()
                                           ->mutable_array_data()
                                           ->mutable_data()
                                           ->Add();

                    for (int elem = 0; elem < array_len; elem++) {
                        int value = global_row * array_len + elem + 1;
                        array_data->mutable_int_data()->mutable_data()->Add(
                            value);
                    }
                }
                break;
            }
        }

        return batch;
    };

    // Insert batch 2 first (docs 10-19) - should be cached
    auto batch2 = gen_batch(10, 10);
    segment->PreInsert(10);
    segment->Insert(
        10, 10, batch2.row_ids_.data(), batch2.timestamps_.data(), batch2.raw_);

    // Insert batch 1 (docs 0-9) - should trigger drain of batch 2
    auto batch1 = gen_batch(0, 10);
    segment->PreInsert(10);
    segment->Insert(
        0, 10, batch1.row_ids_.data(), batch1.timestamps_.data(), batch1.raw_);

    // Insert batch 3 (docs 25-34) - should be cached (gap at 20-24)
    auto batch3 = gen_batch(25, 10);
    segment->PreInsert(10);
    segment->Insert(
        25, 10, batch3.row_ids_.data(), batch3.timestamps_.data(), batch3.raw_);

    // Verify ArrayOffsetsGrowing
    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);

    auto offsets = growing_impl->GetArrayOffsets(vec_fid);
    ASSERT_NE(offsets, nullptr);

    // After inserting docs [0-19] (batch3 cached due to gap), committed count should be 20
    ASSERT_EQ(offsets->GetRowCount(), 20)
        << "Should have committed docs 0-19, batch3 cached";
    ASSERT_EQ(offsets->GetTotalElementCount(), 20 * array_len)
        << "Should have 20 docs worth of elements";

    // Verify mapping for committed docs
    for (int64_t doc_id = 0; doc_id < 20; ++doc_id) {
        for (int32_t elem_idx = 0; elem_idx < array_len; ++elem_idx) {
            int64_t elem_id = doc_id * array_len + elem_idx;
            auto [mapped_doc, mapped_idx] = offsets->ElementIDToRowID(elem_id);

            ASSERT_EQ(mapped_doc, doc_id)
                << "Element " << elem_id << " should map to doc " << doc_id;
            ASSERT_EQ(mapped_idx, elem_idx);
        }
    }
}

// Test parameter for Growing: <use_hints, element_type, metric_type, dim>
using ElementFilterGrowingParam = std::tuple<bool, DataType, std::string, int>;

class ElementFilterGrowing
    : public ::testing::TestWithParam<ElementFilterGrowingParam> {
 protected:
    bool
    use_hints() const {
        return std::get<0>(GetParam());
    }
    DataType
    element_type() const {
        return std::get<1>(GetParam());
    }
    std::string
    metric_type() const {
        return std::get<2>(GetParam());
    }
    int
    vec_dim() const {
        return std::get<3>(GetParam());
    }

    // Create placeholder group with element_level = true for element-level search
    // Uses regular vector types (not EmbList), as query is single embedding per query
    proto::common::PlaceholderGroup
    CreatePlaceholderGroupForType(int num_queries, int dim, int seed) {
        if (element_type() == DataType::VECTOR_BINARY) {
            return CreatePlaceholderGroup<milvus::BinaryVector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_FLOAT16) {
            return CreatePlaceholderGroup<milvus::Float16Vector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_BFLOAT16) {
            return CreatePlaceholderGroup<milvus::BFloat16Vector>(
                num_queries, dim, seed, true);
        } else if (element_type() == DataType::VECTOR_INT8) {
            return CreatePlaceholderGroup<milvus::Int8Vector>(
                num_queries, dim, seed, true);
        } else {
            // VECTOR_FLOAT
            return CreatePlaceholderGroup<milvus::FloatVector>(
                num_queries, dim, seed, true);
        }
    }
};

TEST_P(ElementFilterGrowing, RangeExpr) {
    bool with_hints = use_hints();
    DataType elem_type = element_type();
    std::string metric = metric_type();
    int dim = vec_dim();

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField(
        "structA[array_vec]", elem_type, dim, metric);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    int array_len = 3;

    // Generate test data
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    // Customize int_array data: doc i has elements [i*3+1, i*3+2, i*3+3]
    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (size_t row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = row * array_len + elem + 1;
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                }
            }
            break;
        }
    }

    // Create growing segment and insert data
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    // Verify ArrayOffsetsGrowing was built
    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);
    auto offsets = growing_impl->GetArrayOffsets(vec_fid);
    ASSERT_NE(offsets, nullptr);
    ASSERT_EQ(offsets->GetRowCount(), N);
    ASSERT_EQ(offsets->GetTotalElementCount(), N * array_len);

    int topK = 5;

    // Execute element-level search with iterative filter
    {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        // Expression: id % 2 == 0 && element_filter(structA, 400 > $[price_array] > 100)
        // binary_range with lower_inclusive=false, upper_inclusive=false means: 100 < x < 400
        std::string expr =
            "id % 2 == 0 && element_filter(structA, 400 > $[price_array] > "
            "100)";

        auto plan_bytes = handle.ParseSearch(
            expr, "structA[array_vec]", topK, metric, search_params, 3);

        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroupForType(num_queries, dim, seed);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results
        ASSERT_NE(search_result, nullptr);

        ASSERT_TRUE(search_result->element_level_)
            << "Search should be in element-level mode";
        ASSERT_FALSE(search_result->element_indices_.empty())
            << "Should have element indices";
        ASSERT_FALSE(search_result->seg_offsets_.empty())
            << "Should have doc offsets";
        ASSERT_EQ(search_result->element_indices_.size(),
                  search_result->seg_offsets_.size())
            << "Element indices and doc offsets should match in size";

        ASSERT_LE(search_result->element_indices_.size(),
                  static_cast<size_t>(topK * num_queries))
            << "Should not exceed topK results";

        std::cout << "Growing segment element-level search ("
                  << static_cast<int>(elem_type) << "):" << std::endl;
        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            int32_t elem_idx = search_result->element_indices_[i];
            float distance = search_result->distances_[i];

            std::cout << "  [" << i << "] doc_id=" << doc_id
                      << ", element_index=" << elem_idx
                      << ", distance=" << distance << std::endl;

            ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                     << " should satisfy (id % 2 == 0)";

            ASSERT_GE(elem_idx, 0) << "Element index should be >= 0";
            ASSERT_LT(elem_idx, array_len)
                << "Element index should be < array_len";

            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_GT(element_value, 100)
                << "Element value " << element_value << " should be > 100";
            ASSERT_LT(element_value, 400)
                << "Element value " << element_value << " should be < 400";
        }

        // Verify distances are sorted
        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order";
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    ElementFilter,
    ElementFilterGrowing,
    ::testing::Values(
        // FloatVector with L2
        std::make_tuple(false, DataType::VECTOR_FLOAT, "L2", 4),
        std::make_tuple(true, DataType::VECTOR_FLOAT, "L2", 4),
        // Float16Vector with L2
        std::make_tuple(false, DataType::VECTOR_FLOAT16, "L2", 4),
        std::make_tuple(true, DataType::VECTOR_FLOAT16, "L2", 4),
        // BFloat16Vector with L2
        std::make_tuple(false, DataType::VECTOR_BFLOAT16, "L2", 4),
        std::make_tuple(true, DataType::VECTOR_BFLOAT16, "L2", 4),
        // Int8Vector with L2
        std::make_tuple(false, DataType::VECTOR_INT8, "L2", 4),
        std::make_tuple(true, DataType::VECTOR_INT8, "L2", 4),
        // BinaryVector with HAMMING (no hints - brute force doesn't support iterative filter for binary)
        std::make_tuple(false, DataType::VECTOR_BINARY, "HAMMING", 32)),
    [](const ::testing::TestParamInfo<ElementFilterGrowingParam>& info) {
        bool with_hints = std::get<0>(info.param);
        DataType elem_type = std::get<1>(info.param);
        std::string metric = std::get<2>(info.param);

        std::string type_name;
        switch (elem_type) {
            case DataType::VECTOR_FLOAT:
                type_name = "Float";
                break;
            case DataType::VECTOR_FLOAT16:
                type_name = "Float16";
                break;
            case DataType::VECTOR_BFLOAT16:
                type_name = "BFloat16";
                break;
            case DataType::VECTOR_INT8:
                type_name = "Int8";
                break;
            case DataType::VECTOR_BINARY:
                type_name = "Binary";
                break;
            default:
                type_name = "Unknown";
        }

        std::string name = type_name + "_" + metric;
        name += with_hints ? "_WithHints" : "_NoHints";
        return name;
    });

// Unit tests for ArrayOffsetsGrowing
TEST(ArrayOffsetsGrowing, PurePendingThenDrain) {
    // Test: first insert goes entirely to pending, second insert triggers drain
    ArrayOffsetsGrowing offsets;

    // First insert: rows 2-4, all go to pending (committed_row_count_ = 0)
    std::vector<int32_t> lens1 = {
        3, 2, 4};  // row 2: 3 elems, row 3: 2 elems, row 4: 4 elems
    offsets.Insert(2, lens1.data(), 3);

    ASSERT_EQ(offsets.GetRowCount(), 0) << "No rows should be committed yet";
    ASSERT_EQ(offsets.GetTotalElementCount(), 0)
        << "No elements should exist yet";

    // Second insert: rows 0-1, triggers drain of pending rows 2-4
    std::vector<int32_t> lens2 = {2, 3};  // row 0: 2 elems, row 1: 3 elems
    offsets.Insert(0, lens2.data(), 2);

    ASSERT_EQ(offsets.GetRowCount(), 5) << "All 5 rows should be committed";
    // Total elements: 2 + 3 + 3 + 2 + 4 = 14
    ASSERT_EQ(offsets.GetTotalElementCount(), 14);

    // Verify ElementIDToRowID mapping
    // Row 0: elem 0-1, Row 1: elem 2-4, Row 2: elem 5-7, Row 3: elem 8-9, Row 4: elem 10-13
    std::vector<std::pair<int32_t, int32_t>> expected = {
        {0, 0},
        {0, 1},  // row 0
        {1, 0},
        {1, 1},
        {1, 2},  // row 1
        {2, 0},
        {2, 1},
        {2, 2},  // row 2
        {3, 0},
        {3, 1},  // row 3
        {4, 0},
        {4, 1},
        {4, 2},
        {4, 3}  // row 4
    };

    for (int32_t elem_id = 0; elem_id < 14; ++elem_id) {
        auto [row_id, elem_idx] = offsets.ElementIDToRowID(elem_id);
        ASSERT_EQ(row_id, expected[elem_id].first)
            << "elem_id " << elem_id << " should map to row "
            << expected[elem_id].first;
        ASSERT_EQ(elem_idx, expected[elem_id].second)
            << "elem_id " << elem_id << " should have elem_idx "
            << expected[elem_id].second;
    }
}

TEST(ArrayOffsetsGrowing, ElementIDRangeOfRow) {
    ArrayOffsetsGrowing offsets;

    // Insert 4 rows with varying element counts
    std::vector<int32_t> lens = {3, 0, 2, 5};  // includes empty array
    offsets.Insert(0, lens.data(), 4);

    ASSERT_EQ(offsets.GetRowCount(), 4);
    ASSERT_EQ(offsets.GetTotalElementCount(), 10);  // 3 + 0 + 2 + 5

    // Verify ElementIDRangeOfRow
    auto [start0, end0] = offsets.ElementIDRangeOfRow(0);
    ASSERT_EQ(start0, 0);
    ASSERT_EQ(end0, 3);

    auto [start1, end1] = offsets.ElementIDRangeOfRow(1);
    ASSERT_EQ(start1, 3);
    ASSERT_EQ(end1, 3);  // empty array

    auto [start2, end2] = offsets.ElementIDRangeOfRow(2);
    ASSERT_EQ(start2, 3);
    ASSERT_EQ(end2, 5);

    auto [start3, end3] = offsets.ElementIDRangeOfRow(3);
    ASSERT_EQ(start3, 5);
    ASSERT_EQ(end3, 10);

    // Boundary: row_id == row_count returns (total, total)
    auto [start4, end4] = offsets.ElementIDRangeOfRow(4);
    ASSERT_EQ(start4, 10);
    ASSERT_EQ(end4, 10);
}

TEST(ArrayOffsetsGrowing, MultiplePendingBatches) {
    // Test multiple pending batches being drained in order
    ArrayOffsetsGrowing offsets;

    // Insert row 5 first
    std::vector<int32_t> lens5 = {2};
    offsets.Insert(5, lens5.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 0);

    // Insert row 3
    std::vector<int32_t> lens3 = {3};
    offsets.Insert(3, lens3.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 0);

    // Insert row 1
    std::vector<int32_t> lens1 = {1};
    offsets.Insert(1, lens1.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 0);

    // Insert row 0 - should drain row 1, but not 3 or 5 (gap at 2)
    std::vector<int32_t> lens0 = {2};
    offsets.Insert(0, lens0.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 2) << "Should commit rows 0-1";
    ASSERT_EQ(offsets.GetTotalElementCount(), 3);  // 2 + 1

    // Insert row 2 - should drain rows 3, but not 5 (gap at 4)
    std::vector<int32_t> lens2 = {1};
    offsets.Insert(2, lens2.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 4) << "Should commit rows 0-3";
    ASSERT_EQ(offsets.GetTotalElementCount(), 7);  // 2 + 1 + 1 + 3

    // Insert row 4 - should drain row 5
    std::vector<int32_t> lens4 = {2};
    offsets.Insert(4, lens4.data(), 1);
    ASSERT_EQ(offsets.GetRowCount(), 6) << "Should commit rows 0-5";
    ASSERT_EQ(offsets.GetTotalElementCount(), 11);  // 2 + 1 + 1 + 3 + 2 + 2

    // Verify final mapping
    // Row 0: elem 0-1, Row 1: elem 2, Row 2: elem 3, Row 3: elem 4-6, Row 4: elem 7-8, Row 5: elem 9-10
    auto [r0, i0] = offsets.ElementIDToRowID(0);
    ASSERT_EQ(r0, 0);
    ASSERT_EQ(i0, 0);

    auto [r2, i2] = offsets.ElementIDToRowID(2);
    ASSERT_EQ(r2, 1);
    ASSERT_EQ(i2, 0);

    auto [r4, i4] = offsets.ElementIDToRowID(4);
    ASSERT_EQ(r4, 3);
    ASSERT_EQ(i4, 0);

    auto [r7, i7] = offsets.ElementIDToRowID(7);
    ASSERT_EQ(r7, 4);
    ASSERT_EQ(i7, 0);

    auto [r10, i10] = offsets.ElementIDToRowID(10);
    ASSERT_EQ(r10, 5);
    ASSERT_EQ(i10, 1);
}

enum class NestedIndexType { NONE, STL_SORT, INVERTED };

std::string
NestedIndexTypeToString(NestedIndexType type) {
    switch (type) {
        case NestedIndexType::NONE:
            return "None";
        case NestedIndexType::STL_SORT:
            return "StlSort";
        case NestedIndexType::INVERTED:
            return "Inverted";
        default:
            return "Unknown";
    }
}

// Test fixture for nested index and execution mode testing
// Parameters: (NestedIndexType, bool force_offset_mode)
class ElementFilterNestedIndex
    : public ::testing::TestWithParam<std::tuple<NestedIndexType, bool>> {
 protected:
    NestedIndexType
    nested_index_type() const {
        return std::get<0>(GetParam());
    }

    bool
    force_offset_mode() const {
        return std::get<1>(GetParam());
    }
};

TEST_P(ElementFilterNestedIndex, ExecutionMode) {
    auto index_type = nested_index_type();
    bool offset_mode = force_offset_mode();

    // Step 1: Prepare schema with array field
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_float_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    // Use larger N for better selectivity control
    size_t N = 1000;
    int array_len = 3;

    // Step 2: Generate test data
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    // Customize int_array data: doc i has elements [i*3+1, i*3+2, i*3+3]
    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (int row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = row * array_len + elem + 1;
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                }
            }
            break;
        }
    }

    // Step 3: Create sealed segment with field data
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Step 4: Load vector index
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);
    std::vector<float> vector_data(dim * N * array_len);
    for (int i = 0; i < N; i++) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        for (int j = 0; j < array_len * dim; j++) {
            vector_data[i * array_len * dim + j] = float_vec[j];
        }
    }

    auto indexing = GenVecIndexing(N * array_len,
                                   dim,
                                   vector_data.data(),
                                   knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    load_index_info.field_type = DataType::VECTOR_ARRAY;
    load_index_info.element_type = DataType::VECTOR_FLOAT;
    segment->LoadIndex(load_index_info);

    // Step 5: Build and load nested scalar index if needed
    if (index_type != NestedIndexType::NONE) {
        // Build nested index for int_array field
        std::vector<int32_t> all_elements;
        all_elements.reserve(N * array_len);
        for (int row = 0; row < N; row++) {
            for (int elem = 0; elem < array_len; elem++) {
                all_elements.push_back(row * array_len + elem + 1);
            }
        }

        milvus::index::IndexBasePtr nested_index;
        if (index_type == NestedIndexType::STL_SORT) {
            auto stl_index =
                std::make_unique<milvus::index::ScalarIndexSort<int32_t>>(
                    storage::FileManagerContext(), true /* is_nested */);
            stl_index->Build(all_elements.size(), all_elements.data(), nullptr);
            nested_index = std::move(stl_index);
        } else {
            // INVERTED index - use BuildWithRawDataForUT is not available for int32
            // So we use ScalarIndexSort for now as a workaround
            // In real scenario, inverted index would be loaded from disk
            auto stl_index =
                std::make_unique<milvus::index::ScalarIndexSort<int32_t>>(
                    storage::FileManagerContext(), true /* is_nested */);
            stl_index->Build(all_elements.size(), all_elements.data(), nullptr);
            nested_index = std::move(stl_index);
        }

        // Load nested index to segment
        LoadIndexInfo nested_load_info;
        nested_load_info.field_id = int_array_fid.get();
        nested_load_info.field_type = DataType::ARRAY;
        nested_load_info.element_type = DataType::INT32;
        nested_load_info.index_params["index_type"] =
            index_type == NestedIndexType::STL_SORT
                ? milvus::index::ASCENDING_SORT
                : milvus::index::INVERTED_INDEX_TYPE;
        nested_load_info.cache_index =
            CreateTestCacheIndex("nested_test", std::move(nested_index));
        segment->LoadIndex(nested_load_info);
    }

    int topK = 5;

    // Step 6: Build query plan with appropriate selectivity
    // For offset_mode: use very low selectivity (id == 500 -> 0.1% for N=1000)
    // For full_mode: use high selectivity (id % 2 == 0 -> 50%)
    // Expression: element_filter(structA, 2000 > $[price_array] > 100) && predicate
    // binary_range with lower_inclusive=false, upper_inclusive=false means: 100 < x < 2000
    ScopedSchemaHandle handle(*schema);
    std::string search_params = R"({"ef": 50})";
    std::string expr;
    if (offset_mode) {
        // Very low selectivity: only 1 doc matches (0.1% for N=1000)
        // doc 500's elements are [1501, 1502, 1503] which are in range (100, 2000)
        expr =
            "id == 500 && element_filter(structA, 2000 > $[price_array] > 100)";
    } else {
        // High selectivity: ~50% docs match
        expr =
            "id % 2 == 0 && element_filter(structA, 2000 > $[price_array] > "
            "100)";
    }

    auto plan_bytes = handle.ParseSearch(expr,
                                         "structA[array_float_vec]",
                                         topK,
                                         knowhere::metric::L2,
                                         search_params,
                                         3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto num_queries = 1;
    auto seed = 1024;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, seed, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    // Step 7: Execute search
    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    // Step 8: Verify results
    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->element_level_);

    // Count valid results (doc_id >= 0, -1 means invalid/padding)
    size_t valid_count = 0;
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        if (search_result->seg_offsets_[i] >= 0) {
            valid_count++;
        }
    }

    if (offset_mode) {
        // In offset mode with id == 500, only doc 500 matches
        // doc 500 has 3 elements, all in range (100, 2000): [1501, 1502, 1503]
        ASSERT_GT(valid_count, 0) << "Should have at least one valid result";
        ASSERT_LE(valid_count, 3)
            << "At most 3 elements from doc 500 can match";

        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            if (doc_id >= 0) {
                ASSERT_EQ(doc_id, 500)
                    << "In offset mode, only doc 500 should match";
            }
        }
    } else {
        // In full mode with id % 2 == 0, even docs match
        ASSERT_GT(valid_count, 0) << "Should have valid results";
        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            if (doc_id >= 0) {
                ASSERT_EQ(doc_id % 2, 0)
                    << "Result doc_id " << doc_id << " should be even";
            }
        }
    }

    // Verify element values are in range (100, 2000) for valid results
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];
        if (doc_id >= 0) {
            int32_t elem_idx = search_result->element_indices_[i];
            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_GT(element_value, 100);
            ASSERT_LT(element_value, 2000);
        }
    }

    // Verify distances are sorted (only check valid results)
    float last_distance = -1;
    for (size_t i = 0; i < search_result->distances_.size(); ++i) {
        if (search_result->seg_offsets_[i] >= 0) {
            if (last_distance >= 0) {
                ASSERT_LE(last_distance, search_result->distances_[i]);
            }
            last_distance = search_result->distances_[i];
        }
    }

    std::cout << "Test passed: index_type="
              << NestedIndexTypeToString(index_type)
              << ", offset_mode=" << (offset_mode ? "true" : "false")
              << ", valid_results=" << valid_count << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    ElementFilter,
    ElementFilterNestedIndex,
    ::testing::Combine(::testing::Values(NestedIndexType::NONE,
                                         NestedIndexType::STL_SORT,
                                         NestedIndexType::INVERTED),
                       ::testing::Values(false, true)  // force_offset_mode
                       ),
    [](const ::testing::TestParamInfo<ElementFilterNestedIndex::ParamType>&
           info) {
        auto index_type = std::get<0>(info.param);
        bool offset_mode = std::get<1>(info.param);
        std::string name = NestedIndexTypeToString(index_type);
        name += offset_mode ? "_OffsetMode" : "_FullMode";
        return name;
    });
