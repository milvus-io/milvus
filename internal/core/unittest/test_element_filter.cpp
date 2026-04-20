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
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_set>
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
#include "query/PlanProto.h"
#include "segcore/Collection.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "storage/FileManager.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"
#include "common/Common.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

// Test parameter: <use_hints, load_index, element_type, metric_type, dim>
using ElementFilterSealedParam =
    std::tuple<bool, bool, DataType, std::string, int>;

class ElementFilterSealed
    : public ::testing::TestWithParam<ElementFilterSealedParam> {
 protected:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);
    }
    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

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

 private:
    int64_t saved_batch_size_;
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
    // Query: Search array elements, filter by element_value in (100, 400)
    for (bool with_predicate : {false, true}) {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        std::string base_filter =
            "element_filter(structA, 400 > $[price_array] > 100)";
        std::string expr =
            with_predicate ? "id % 2 == 0 && " + base_filter : base_filter;

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

        // In element-level mode, results should be element indices, not doc
        // offsets
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

            // Verify the doc_id satisfies the predicate (id % 2 == 0) only
            // when predicate is enabled
            if (with_predicate) {
                ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                         << " should satisfy (id % 2 == 0)";
            }

            // Verify element value is in range (100, 400)
            // Element value = doc_id * array_len + elem_idx + 1
            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_GT(element_value, 100)
                << "Element value " << element_value << " should be > 100";
            ASSERT_LT(element_value, 400)
                << "Element value " << element_value << " should be < 400";
        }

        // Verify distances are sorted (ascending for L2)
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
    // Query: Search array elements, filter by element_value > 10
    for (bool with_predicate : {false, true}) {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        std::string base_filter =
            "element_filter(structA, $[price_array] > 10)";
        std::string expr =
            with_predicate ? "id % 2 == 0 && " + base_filter : base_filter;

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

        // In element-level mode, results should be element indices, not doc
        // offsets
        ASSERT_TRUE(search_result->element_level_);
        ASSERT_FALSE(search_result->element_indices_.empty());
        ASSERT_FALSE(search_result->seg_offsets_.empty());
        ASSERT_EQ(search_result->element_indices_.size(),
                  search_result->seg_offsets_.size());

        ASSERT_LE(search_result->element_indices_.size(),
                  static_cast<size_t>(topK * num_queries));

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

TEST(ElementFilter, GrowingSegmentArrayOffsets) {
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

    size_t N = 10000;
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

    // Both fields should share the same ArrayOffsets
    auto offsets_vec = growing_impl->GetArrayOffsets(vec_fid);
    auto offsets_int = growing_impl->GetArrayOffsets(int_array_fid);
    ASSERT_NE(offsets_vec, nullptr);
    ASSERT_NE(offsets_int, nullptr);

    // Should point to the same object (shared)
    ASSERT_EQ(offsets_vec, offsets_int)
        << "Fields in same struct should share ArrayOffsets";

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
    // Test out-of-order Insert handling in GrowingArrayOffsets
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

    // Verify ArrayOffsets
    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);

    auto offsets = growing_impl->GetArrayOffsets(vec_fid);
    ASSERT_NE(offsets, nullptr);

    // After inserting docs [0-19] (batch3 cached due to gap), committed count
    // should be 20
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

TEST(ElementFilter, MultiQueryCollectResults) {
    // Test CollectResults with nq > 1 to cover per-query loop
    auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
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

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Build and load vector index
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);
    std::vector<float> vector_data(dim * N * array_len);
    for (size_t i = 0; i < N; i++) {
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

    int topK = 5;
    int num_queries = 3;

    // Test with_predicate=false to exercise CollectResults with nq > 1
    ScopedSchemaHandle handle(*schema);
    std::string search_params = R"({"ef": 50, "hints": "iterative_filter"})";
    std::string expr = "element_filter(structA, 400 > $[price_array] > 100)";

    auto plan_bytes = handle.ParseSearch(
        expr, "structA[array_vec]", topK, "L2", search_params, 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->element_level_);

    // Result arrays should have nq * topK slots
    ASSERT_EQ(search_result->seg_offsets_.size(), num_queries * topK);
    ASSERT_EQ(search_result->distances_.size(), num_queries * topK);
    ASSERT_EQ(search_result->element_indices_.size(), num_queries * topK);

    // Verify each query's results independently
    for (int q = 0; q < num_queries; q++) {
        int base_idx = q * topK;

        // Count valid results for this query
        int valid_count = 0;
        for (int i = 0; i < topK; i++) {
            if (search_result->seg_offsets_[base_idx + i] !=
                INVALID_SEG_OFFSET) {
                valid_count++;
            }
        }
        ASSERT_GT(valid_count, 0)
            << "Query " << q << " should have at least one result";

        // Verify distances are sorted within this query's range
        for (int i = 1; i < valid_count; i++) {
            ASSERT_LE(search_result->distances_[base_idx + i - 1],
                      search_result->distances_[base_idx + i])
                << "Query " << q << ": distances should be sorted";
        }

        // Verify filter correctness for this query
        for (int i = 0; i < valid_count; i++) {
            int64_t doc_id = search_result->seg_offsets_[base_idx + i];
            int32_t elem_idx = search_result->element_indices_[base_idx + i];
            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_GT(element_value, 100)
                << "Query " << q << ": element value should be > 100";
            ASSERT_LT(element_value, 400)
                << "Query " << q << ": element value should be < 400";
        }
    }

    EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
}

TEST(ElementFilter, CollectResultsWithCosineMetric) {
    // Test CollectResults with large_is_better=true (COSINE metric)
    auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::COSINE);
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

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Build and load vector index with COSINE metric
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);
    std::vector<float> vector_data(dim * N * array_len);
    for (size_t i = 0; i < N; i++) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        for (int j = 0; j < array_len * dim; j++) {
            vector_data[i * array_len * dim + j] = float_vec[j];
        }
    }
    auto indexing = GenVecIndexing(N * array_len,
                                   dim,
                                   vector_data.data(),
                                   knowhere::IndexEnum::INDEX_HNSW,
                                   knowhere::metric::COSINE);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::COSINE;
    load_index_info.field_type = DataType::VECTOR_ARRAY;
    load_index_info.element_type = DataType::VECTOR_FLOAT;
    segment->LoadIndex(load_index_info);

    int topK = 5;

    // with_predicate=false to exercise CollectResults path
    ScopedSchemaHandle handle(*schema);
    std::string search_params = R"({"ef": 50, "hints": "iterative_filter"})";
    std::string expr = "element_filter(structA, 400 > $[price_array] > 100)";

    auto plan_bytes = handle.ParseSearch(
        expr, "structA[array_vec]", topK, "COSINE", search_params, 1);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->element_level_);

    // Count valid results
    int valid_count = 0;
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        if (search_result->seg_offsets_[i] != INVALID_SEG_OFFSET) {
            valid_count++;
        }
    }
    ASSERT_GT(valid_count, 0);

    // For COSINE (large_is_better), distances should be sorted descending
    for (int i = 1; i < valid_count; i++) {
        ASSERT_GE(search_result->distances_[i - 1],
                  search_result->distances_[i])
            << "COSINE distances should be sorted descending";
    }

    // Verify filter correctness
    for (int i = 0; i < valid_count; i++) {
        int64_t doc_id = search_result->seg_offsets_[i];
        int32_t elem_idx = search_result->element_indices_[i];
        int element_value = doc_id * array_len + elem_idx + 1;
        ASSERT_GT(element_value, 100);
        ASSERT_LT(element_value, 400);
    }

    EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
}

// Test parameter for Growing: <use_hints, element_type, metric_type, dim>
using ElementFilterGrowingParam = std::tuple<bool, DataType, std::string, int>;

class ElementFilterGrowing
    : public ::testing::TestWithParam<ElementFilterGrowingParam> {
 protected:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);
    }
    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

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

 private:
    int64_t saved_batch_size_;
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

    // Verify ArrayOffsets was built
    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);
    auto offsets = growing_impl->GetArrayOffsets(vec_fid);
    ASSERT_NE(offsets, nullptr);
    ASSERT_EQ(offsets->GetRowCount(), N);
    ASSERT_EQ(offsets->GetTotalElementCount(), N * array_len);

    int topK = 5;

    // Execute element-level search with iterative filter
    // Query: Search array elements where price_array element in range
    // (100, 400)
    for (bool with_predicate : {false, true}) {
        ScopedSchemaHandle handle(*schema);

        // Build search params with optional hints
        std::string search_params =
            with_hints ? R"({"ef": 50, "hints": "iterative_filter"})"
                       : R"({"ef": 50})";

        std::string base_filter =
            "element_filter(structA, 400 > $[price_array] > 100)";
        std::string expr =
            with_predicate ? "id % 2 == 0 && " + base_filter : base_filter;

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

        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            int32_t elem_idx = search_result->element_indices_[i];

            // Verify the doc_id satisfies the predicate (id % 2 == 0) only
            // when predicate is enabled
            if (with_predicate) {
                ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                         << " should satisfy (id % 2 == 0)";
            }

            ASSERT_GE(elem_idx, 0) << "Element index should be >= 0";
            ASSERT_LT(elem_idx, array_len)
                << "Element index should be < array_len";

            // Verify element value is in range (100, 400)
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

class ElementFilterRetrieve
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);
    }
    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    bool
    is_sealed() const {
        return std::get<0>(GetParam());
    }
    bool
    use_predicate() const {
        return std::get<1>(GetParam());
    }

 private:
    int64_t saved_batch_size_;
};

TEST_P(ElementFilterRetrieve, RangeExpr) {
    bool with_sealed = is_sealed();
    bool with_predicate = use_predicate();

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

    size_t N = 10000;
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

    // Step 3: Create segment (sealed or growing)
    std::shared_ptr<SegmentInterface> segment;
    if (with_sealed) {
        segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    } else {
        auto growing = CreateGrowingSegment(schema, empty_index_meta);
        growing->PreInsert(N);
        growing->Insert(0,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        segment = std::move(growing);
    }

    // Step 4: Build retrieve plan with element-level filter
    // Query: Retrieve docs where price_array element % 3 == 2
    // Data: element_value = doc_id * 3 + elem_idx + 1
    // When elem_idx=1: value = doc_id*3 + 2, so value % 3 == 2
    // This ensures only elem_idx=1 matches for each doc
    {
        proto::plan::PlanNode plan_node;

        // Set up query (not predicates at top level!)
        auto* query = plan_node.mutable_query();
        query->set_is_count(false);
        query->set_limit(100);

        // Build element filter expression under query.predicates
        auto* expr = query->mutable_predicates();
        auto* element_filter = expr->mutable_element_filter_expr();
        element_filter->set_struct_name("structA");

        // Element expression: price_array element % 3 == 2
        auto* element_expr = element_filter->mutable_element_expr();
        auto* arith_expr =
            element_expr->mutable_binary_arith_op_eval_range_expr();

        auto* column_info = arith_expr->mutable_column_info();
        column_info->set_field_id(int_array_fid.get());
        column_info->set_data_type(proto::schema::DataType::Int32);
        column_info->set_element_type(proto::schema::DataType::Int32);
        column_info->set_is_element_level(true);

        arith_expr->set_arith_op(proto::plan::ArithOpType::Mod);
        arith_expr->mutable_right_operand()->set_int64_val(3);
        arith_expr->set_op(proto::plan::OpType::Equal);
        arith_expr->mutable_value()->set_int64_val(2);

        // Add predicate if needed (doc-level filter: id % 2 == 0)
        if (with_predicate) {
            auto* predicate = element_filter->mutable_predicate();
            auto* arith_expr =
                predicate->mutable_binary_arith_op_eval_range_expr();

            auto* pred_column = arith_expr->mutable_column_info();
            pred_column->set_field_id(int64_fid.get());
            pred_column->set_data_type(proto::schema::DataType::Int64);

            arith_expr->set_arith_op(proto::plan::ArithOpType::Mod);
            arith_expr->mutable_right_operand()->set_int64_val(2);
            arith_expr->set_op(proto::plan::OpType::Equal);
            arith_expr->mutable_value()->set_int64_val(0);
        }

        // Add output fields
        plan_node.add_output_field_ids(int64_fid.get());
        plan_node.add_output_field_ids(int_array_fid.get());

        auto parser = ProtoParser(schema);
        auto plan = parser.CreateRetrievePlan(plan_node);

        // Step 5: Execute Retrieve
        int64_t limit = 100;  // Retrieve top 100 element matches
        auto retrieve_results = segment->Retrieve(nullptr,
                                                  plan.get(),
                                                  1L << 63,
                                                  INT64_MAX,
                                                  false,
                                                  folly::CancellationToken(),
                                                  0,
                                                  0);

        // Step 6: Verify results
        ASSERT_NE(retrieve_results, nullptr);

        // Verify element-level flag is set
        ASSERT_TRUE(retrieve_results->element_level())
            << "Retrieve should be in element-level mode";

        // Verify element_indices are populated
        ASSERT_GT(retrieve_results->element_indices_size(), 0)
            << "Should have element indices in element-level retrieve";

        // Verify element_indices match offset size (each doc has its own
        // indices list)
        ASSERT_EQ(retrieve_results->element_indices_size(),
                  retrieve_results->offset_size())
            << "Element indices and offsets should have same size";

        std::cout << "Element-level Retrieve returned "
                  << retrieve_results->offset_size() << " unique docs"
                  << std::endl;

        // Verify each result
        int total_elements = 0;
        for (int i = 0; i < retrieve_results->offset_size(); i++) {
            int64_t doc_id = retrieve_results->offset(i);
            const auto& elem_indices = retrieve_results->element_indices(i);

            // Verify the doc_id satisfies the predicate (id % 2 == 0) only when
            // predicate is enabled
            if (with_predicate) {
                ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                         << " should satisfy (id % 2 == 0)";
            }

            // Verify each element_idx in this doc
            // With filter "value % 3 == 2", only elem_idx=1 should match
            // because value = doc_id*3 + elem_idx + 1
            // elem_idx=0: value % 3 = (doc_id*3 + 1) % 3 = 1
            // elem_idx=1: value % 3 = (doc_id*3 + 2) % 3 = 2  <-- matches
            // elem_idx=2: value % 3 = (doc_id*3 + 3) % 3 = 0
            ASSERT_EQ(elem_indices.indices_size(), 1)
                << "Each doc should have exactly one matching element "
                   "(elem_idx=1)";

            int32_t elem_idx = elem_indices.indices(0);

            // Verify element_idx is valid
            ASSERT_EQ(elem_idx, 1)
                << "Only elem_idx=1 should match the filter (value % 3 == 2)";

            // Verify element value satisfies filter: value % 3 == 2
            int element_value = doc_id * array_len + elem_idx + 1;
            ASSERT_EQ(element_value % 3, 2) << "Element value " << element_value
                                            << " should satisfy value % 3 == 2";

            total_elements += elem_indices.indices_size();
        }
        std::cout << "Total matching elements: " << total_elements << std::endl;

        // Verify that total_elements equals number of docs (1 element per doc)
        ASSERT_EQ(total_elements, retrieve_results->offset_size())
            << "Each doc should have exactly 1 matching element";

        // Verify element-level limit enforcement:
        // find_first_n_element counts elements toward limit, not documents.
        // With limit=100 and 1 element per doc, total_elements <= 100.
        ASSERT_LE(total_elements, 100)
            << "Total matching elements should not exceed limit";

        // Verify fields_data contains the output fields
        ASSERT_EQ(retrieve_results->fields_data_size(), 2)
            << "Should have 2 output fields (id, price_array)";
    }
}

TEST_P(ElementFilterRetrieve, UnaryExpr) {
    bool with_sealed = is_sealed();
    bool with_predicate = use_predicate();

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

    size_t N = 10000;
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

    // Step 3: Create segment (sealed or growing)
    std::shared_ptr<SegmentInterface> segment;
    if (with_sealed) {
        segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    } else {
        auto growing = CreateGrowingSegment(schema, empty_index_meta);
        growing->PreInsert(N);
        growing->Insert(0,
                        N,
                        raw_data.row_ids_.data(),
                        raw_data.timestamps_.data(),
                        raw_data.raw_);
        segment = std::move(growing);
    }

    // Step 4: Build retrieve plan with element-level filter
    // Query: Retrieve docs where price_array elements > 100
    {
        proto::plan::PlanNode plan_node;

        // Set up query (not predicates at top level!)
        auto* query = plan_node.mutable_query();
        query->set_is_count(false);
        query->set_limit(100);

        // Build element filter expression under query.predicates
        auto* expr = query->mutable_predicates();
        auto* element_filter = expr->mutable_element_filter_expr();
        element_filter->set_struct_name("structA");

        // Element expression: price_array element > 100
        auto* element_expr = element_filter->mutable_element_expr();
        auto* unary_range = element_expr->mutable_unary_range_expr();

        auto* column_info = unary_range->mutable_column_info();
        column_info->set_field_id(int_array_fid.get());
        column_info->set_data_type(proto::schema::DataType::Int32);
        column_info->set_element_type(proto::schema::DataType::Int32);
        column_info->set_is_element_level(true);

        unary_range->set_op(proto::plan::OpType::GreaterThan);
        unary_range->mutable_value()->set_int64_val(100);

        // Add predicate if needed (doc-level filter: id % 2 == 0)
        if (with_predicate) {
            auto* predicate = element_filter->mutable_predicate();
            auto* arith_expr =
                predicate->mutable_binary_arith_op_eval_range_expr();

            auto* pred_column = arith_expr->mutable_column_info();
            pred_column->set_field_id(int64_fid.get());
            pred_column->set_data_type(proto::schema::DataType::Int64);

            arith_expr->set_arith_op(proto::plan::ArithOpType::Mod);
            arith_expr->mutable_right_operand()->set_int64_val(2);
            arith_expr->set_op(proto::plan::OpType::Equal);
            arith_expr->mutable_value()->set_int64_val(0);
        }

        // Add output fields
        plan_node.add_output_field_ids(int64_fid.get());
        plan_node.add_output_field_ids(int_array_fid.get());

        auto parser = ProtoParser(schema);
        auto plan = parser.CreateRetrievePlan(plan_node);

        // Step 5: Execute Retrieve
        int64_t limit = 100;  // Retrieve top 100 element matches
        auto retrieve_results = segment->Retrieve(nullptr,
                                                  plan.get(),
                                                  1L << 63,
                                                  INT64_MAX,
                                                  false,
                                                  folly::CancellationToken(),
                                                  0,
                                                  0);

        // Step 6: Verify results
        ASSERT_NE(retrieve_results, nullptr);

        // Verify element-level flag is set
        ASSERT_TRUE(retrieve_results->element_level())
            << "Retrieve should be in element-level mode";

        // Verify element_indices are populated
        ASSERT_GT(retrieve_results->element_indices_size(), 0)
            << "Should have element indices in element-level retrieve";

        // Verify element_indices match offset size (each doc has its own
        // indices list)
        ASSERT_EQ(retrieve_results->element_indices_size(),
                  retrieve_results->offset_size())
            << "Element indices and offsets should have same size";

        std::cout << "Element-level Retrieve (UnaryExpr) returned "
                  << retrieve_results->offset_size() << " unique docs"
                  << std::endl;

        // Verify each result
        int total_elements = 0;
        for (int i = 0; i < retrieve_results->offset_size(); i++) {
            int64_t doc_id = retrieve_results->offset(i);
            const auto& elem_indices = retrieve_results->element_indices(i);

            // Verify the doc_id satisfies the predicate (id % 2 == 0) only when
            // predicate is enabled
            if (with_predicate) {
                ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                         << " should satisfy (id % 2 == 0)";
            }

            // Verify each element_idx in this doc
            ASSERT_GT(elem_indices.indices_size(), 0)
                << "Each doc should have at least one matching element";
            total_elements += elem_indices.indices_size();
            for (int j = 0; j < elem_indices.indices_size(); j++) {
                int32_t elem_idx = elem_indices.indices(j);

                // Verify element_idx is valid
                ASSERT_GE(elem_idx, 0) << "Element index should be >= 0";
                ASSERT_LT(elem_idx, array_len)
                    << "Element index should be < array_len";

                // Verify element value > 100
                // Element value = doc_id * array_len + elem_idx + 1
                int element_value = doc_id * array_len + elem_idx + 1;
                ASSERT_GT(element_value, 100)
                    << "Element value " << element_value << " should be > 100";
            }
        }

        // Verify element-level limit enforcement:
        // find_first_n_element counts elements toward limit, not documents.
        // With limit=100, total matching elements should not exceed 100.
        ASSERT_LE(total_elements, 100)
            << "Total matching elements should not exceed limit";
    }
}

INSTANTIATE_TEST_SUITE_P(
    ElementFilter,
    ElementFilterRetrieve,
    ::testing::Combine(::testing::Bool(),  // with_sealed: true/false
                       ::testing::Bool()   // with_predicate: true/false
                       ),
    [](const ::testing::TestParamInfo<ElementFilterRetrieve::ParamType>& info) {
        bool with_sealed = std::get<0>(info.param);
        bool with_predicate = std::get<1>(info.param);
        std::string name = "";
        name += with_sealed ? "Sealed" : "Growing";
        name += "_";
        name += with_predicate ? "WithPredicate" : "WithoutPredicate";
        return name;
    });

TEST(ElementFilter, RetrieveSortedByPk) {
    // Test find_first_n_element on the is_sorted_by_pk_=true path
    auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

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

    size_t N = 1000;
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

    // Create sealed segment with is_sorted_by_pk=true
    auto segment = CreateSealedSegment(schema,
                                       empty_index_meta,
                                       /*segment_id=*/0,
                                       SegcoreConfig::default_config(),
                                       /*is_sorted_by_pk=*/true);
    LoadGeneratedDataIntoSegment(raw_data, segment.get());

    // Build retrieve plan: element_filter(structA, $[price_array] % 3 == 2)
    proto::plan::PlanNode plan_node;
    auto* query = plan_node.mutable_query();
    query->set_is_count(false);
    query->set_limit(50);

    auto* expr = query->mutable_predicates();
    auto* element_filter = expr->mutable_element_filter_expr();
    element_filter->set_struct_name("structA");

    auto* element_expr = element_filter->mutable_element_expr();
    auto* arith_expr = element_expr->mutable_binary_arith_op_eval_range_expr();
    auto* column_info = arith_expr->mutable_column_info();
    column_info->set_field_id(int_array_fid.get());
    column_info->set_data_type(proto::schema::DataType::Int32);
    column_info->set_element_type(proto::schema::DataType::Int32);
    column_info->set_is_element_level(true);
    arith_expr->set_arith_op(proto::plan::ArithOpType::Mod);
    arith_expr->mutable_right_operand()->set_int64_val(3);
    arith_expr->set_op(proto::plan::OpType::Equal);
    arith_expr->mutable_value()->set_int64_val(2);

    plan_node.add_output_field_ids(int64_fid.get());
    plan_node.add_output_field_ids(int_array_fid.get());

    auto parser = ProtoParser(schema);
    auto plan = parser.CreateRetrievePlan(plan_node);

    auto retrieve_results = segment->Retrieve(nullptr,
                                              plan.get(),
                                              1L << 63,
                                              INT64_MAX,
                                              false,
                                              folly::CancellationToken(),
                                              0,
                                              0);

    ASSERT_NE(retrieve_results, nullptr);
    ASSERT_TRUE(retrieve_results->element_level());
    ASSERT_GT(retrieve_results->element_indices_size(), 0);
    ASSERT_EQ(retrieve_results->element_indices_size(),
              retrieve_results->offset_size());

    // Verify results are in PK order (sorted-by-pk path should return ordered)
    for (int i = 1; i < retrieve_results->offset_size(); i++) {
        ASSERT_LT(retrieve_results->offset(i - 1), retrieve_results->offset(i))
            << "Results should be in ascending PK order for sorted segment";
    }

    // Verify each element satisfies filter (value % 3 == 2 => only elem_idx=1)
    int total_elements = 0;
    for (int i = 0; i < retrieve_results->offset_size(); i++) {
        int64_t doc_id = retrieve_results->offset(i);
        const auto& elem_indices = retrieve_results->element_indices(i);
        ASSERT_EQ(elem_indices.indices_size(), 1);
        ASSERT_EQ(elem_indices.indices(0), 1);
        int element_value = doc_id * array_len + 1 + 1;
        ASSERT_EQ(element_value % 3, 2);
        total_elements += elem_indices.indices_size();
    }

    // Limit=50 counts elements, 1 element per doc => 50 docs
    ASSERT_LE(total_elements, 50);

    EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
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
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);
    }
    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    NestedIndexType
    nested_index_type() const {
        return std::get<0>(GetParam());
    }

    bool
    force_offset_mode() const {
        return std::get<1>(GetParam());
    }

 private:
    int64_t saved_batch_size_;
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

// Test element-level filter combined with group by on sealed segment with index
TEST(ElementFilterGroupBy, SealedWithIndex) {
    int dim = 4;
    size_t N = 500;
    int array_len = 3;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

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

    // Create sealed segment and load field data
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Build and load vector index
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

    int topK = 5;
    int group_size = 4;

    // Execute element-level search with group by primary key
    ScopedSchemaHandle handle(*schema);
    std::string expr =
        "id % 2 == 0 && element_filter(structA, 400 > $[price_array] > 100)";
    std::string search_params = R"({"ef": 50})";

    auto plan_bytes = handle.ParseGroupBySearch(expr,
                                                "structA[array_vec]",
                                                topK,
                                                knowhere::metric::L2,
                                                search_params,
                                                int64_fid.get(),
                                                group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->group_by_values_.has_value())
        << "Group by values should be present";

    // Group by returns row-level results even for element-level search
    ASSERT_FALSE(search_result->element_level_)
        << "Group by should return row-level results";

    auto& group_by_values = search_result->group_by_values_.value();

    ASSERT_LE(search_result->seg_offsets_.size(),
              static_cast<size_t>(topK * group_size))
        << "Should not exceed topK * group_size results";

    std::unordered_map<int64_t, int> group_counts;
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];

        if (i < group_by_values.size() && group_by_values[i].has_value()) {
            if (std::holds_alternative<int64_t>(group_by_values[i].value())) {
                int64_t group_val =
                    std::get<int64_t>(group_by_values[i].value());

                ASSERT_EQ(group_val, doc_id)
                    << "Group by primary key: group value should equal doc_id";

                group_counts[group_val]++;
                ASSERT_LE(group_counts[group_val], group_size)
                    << "Each group should have at most group_size results";
            }
        }

        ASSERT_EQ(doc_id % 2, 0)
            << "Result doc_id " << doc_id << " should satisfy (id % 2 == 0)";
    }

    ASSERT_LE(group_counts.size(), static_cast<size_t>(topK))
        << "Should have at most topK distinct groups";

    for (size_t i = 1; i < search_result->distances_.size(); ++i) {
        ASSERT_LE(search_result->distances_[i - 1],
                  search_result->distances_[i])
            << "Distances should be sorted in ascending order";
    }
}

// Test: element level + group by + growing segment
TEST(ElementFilterGroupBy, GrowingSegment) {
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField(
        "structA[array_vec]", DataType::VECTOR_FLOAT, dim, "L2");
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

    // Create growing segment and insert data
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    int topK = 5;
    int group_size = 4;

    // Execute element-level search with group by primary key
    ScopedSchemaHandle handle(*schema);
    std::string expr =
        "id % 2 == 0 && element_filter(structA, 400 > $[price_array] > 100)";
    std::string search_params = R"({"nprobe": 10})";

    auto plan_bytes = handle.ParseGroupBySearch(expr,
                                                "structA[array_vec]",
                                                topK,
                                                knowhere::metric::L2,
                                                search_params,
                                                int64_fid.get(),
                                                group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->group_by_values_.has_value())
        << "Group by values should be present";

    // Verify element_level_ is false (group by returns row-level results)
    ASSERT_FALSE(search_result->element_level_)
        << "Group by should return row-level results";

    auto& group_by_values = search_result->group_by_values_.value();

    ASSERT_FALSE(search_result->seg_offsets_.empty())
        << "Should have search results";
    ASSERT_LE(search_result->seg_offsets_.size(),
              static_cast<size_t>(topK * group_size))
        << "Should not exceed topK * group_size results";

    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];

        // Verify row-level filter
        ASSERT_EQ(doc_id % 2, 0)
            << "Result doc_id " << doc_id << " should satisfy (id % 2 == 0)";

        // Verify element-level filter: 100 < element_value < 400
        // element_value = doc_id * array_len + elem + 1, elem in [0, array_len)
        // For doc to have any matching element: doc_id * 3 + 1 < 400 and doc_id * 3 + 3 > 100
        // So doc_id should be roughly in range [33, 132]
        ASSERT_GE(doc_id, 33)
            << "doc_id " << doc_id << " should be >= 33 (element filter)";
        ASSERT_LE(doc_id, 133)
            << "doc_id " << doc_id << " should be <= 133 (element filter)";

        if (i < group_by_values.size() && group_by_values[i].has_value()) {
            if (std::holds_alternative<int64_t>(group_by_values[i].value())) {
                int64_t group_val =
                    std::get<int64_t>(group_by_values[i].value());
                ASSERT_EQ(group_val, doc_id)
                    << "Group by primary key: group value should equal doc_id";
            }
        }
    }
}

// Test: normal group by (without element level)
TEST(ElementFilterGroupBy, NormalGroupBy) {
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid =
        schema->AddDebugField("vec", DataType::VECTOR_FLOAT, dim, "L2");
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    auto category_fid = schema->AddDebugField("category", DataType::INT32);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    auto raw_data = DataGen(schema, N, 42);

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Build vector index
    auto vec_values = raw_data.get_col<float>(vec_fid);
    auto indexing = GenVecIndexing(
        N, dim, vec_values.data(), knowhere::IndexEnum::INDEX_HNSW);

    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    load_index_info.field_type = DataType::VECTOR_FLOAT;
    segment->LoadIndex(load_index_info);

    int topK = 5;
    int group_size = 2;

    // Normal search with group by (no element level)
    ScopedSchemaHandle handle(*schema);
    std::string expr = "id % 2 == 0";
    std::string search_params = R"({"ef": 50})";

    auto plan_bytes = handle.ParseGroupBySearch(expr,
                                                "vec",
                                                topK,
                                                knowhere::metric::L2,
                                                search_params,
                                                category_fid.get(),
                                                group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->group_by_values_.has_value())
        << "Group by values should be present";

    // Verify element_level_ is false
    ASSERT_FALSE(search_result->element_level_)
        << "Normal search should not be element-level";

    auto& group_by_values = search_result->group_by_values_.value();

    ASSERT_FALSE(search_result->seg_offsets_.empty())
        << "Should have search results";
    ASSERT_LE(search_result->seg_offsets_.size(),
              static_cast<size_t>(topK * group_size))
        << "Should not exceed topK * group_size results";

    std::unordered_map<int32_t, int> group_counts;
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];

        ASSERT_EQ(doc_id % 2, 0)
            << "Result doc_id " << doc_id << " should satisfy (id % 2 == 0)";

        if (i < group_by_values.size() && group_by_values[i].has_value()) {
            if (std::holds_alternative<int32_t>(group_by_values[i].value())) {
                int32_t group_val =
                    std::get<int32_t>(group_by_values[i].value());
                group_counts[group_val]++;
                ASSERT_LE(group_counts[group_val], group_size)
                    << "Each group should have at most group_size results";
            }
        }
    }

    ASSERT_LE(group_counts.size(), static_cast<size_t>(topK))
        << "Should have at most topK distinct groups";
}

// Test: element-level + group by non-unique field to verify row deduplication.
// When multiple elements from the same document are close to the query vector,
// each document should only appear once in the results (not consume multiple
// group slots).
TEST(ElementFilterGroupBy, DeduplicateRowsInGroup) {
    int dim = 4;
    size_t N = 500;
    int array_len = 3;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    // category field: non-unique, used for group by
    // Assign category = id % 10, so ~50 docs per category
    auto category_fid = schema->AddDebugField("category", DataType::INT32);
    schema->set_primary_field_id(int64_fid);

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
        }
        // Set category = id % 10
        if (field_data->field_id() == category_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_int_data()
                ->mutable_data()
                ->Clear();
            for (int row = 0; row < N; row++) {
                field_data->mutable_scalars()
                    ->mutable_int_data()
                    ->mutable_data()
                    ->Add(row % 10);
            }
        }
    }

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Build and load vector index
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

    int topK = 5;
    int group_size = 3;

    ScopedSchemaHandle handle(*schema);
    std::string expr = "element_filter(structA, 2000 > $[price_array] > 100)";
    std::string search_params = R"({"ef": 50})";

    auto plan_bytes = handle.ParseGroupBySearch(expr,
                                                "structA[array_vec]",
                                                topK,
                                                knowhere::metric::L2,
                                                search_params,
                                                category_fid.get(),
                                                group_size);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto num_queries = 1;
    auto ph_group_raw = CreatePlaceholderGroup(num_queries, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);

    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->group_by_values_.has_value())
        << "Group by values should be present";
    ASSERT_FALSE(search_result->element_level_)
        << "Group by should return row-level results";

    auto& group_by_values = search_result->group_by_values_.value();

    ASSERT_FALSE(search_result->seg_offsets_.empty())
        << "Should have search results";
    ASSERT_LE(search_result->seg_offsets_.size(),
              static_cast<size_t>(topK * group_size))
        << "Should not exceed topK * group_size results";

    // Verify: no duplicate row_offsets in results.
    // Without deduplication, the same document could appear multiple times
    // because different elements from one document may all be close to the
    // query vector.
    std::unordered_set<int64_t> seen_docs;
    std::unordered_map<int32_t, int> group_counts;
    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];

        ASSERT_TRUE(seen_docs.insert(doc_id).second)
            << "Duplicate doc_id " << doc_id
            << " in results: same document should not appear more than once";

        if (i < group_by_values.size() && group_by_values[i].has_value()) {
            if (std::holds_alternative<int32_t>(group_by_values[i].value())) {
                int32_t group_val =
                    std::get<int32_t>(group_by_values[i].value());

                ASSERT_EQ(group_val, doc_id % 10)
                    << "Group value should equal category (doc_id % 10)";

                group_counts[group_val]++;
                ASSERT_LE(group_counts[group_val], group_size)
                    << "Each group should have at most group_size results";
            }
        }
    }

    ASSERT_LE(group_counts.size(), static_cast<size_t>(topK))
        << "Should have at most topK distinct groups";
}

TEST(ElementFilter, SearchWithNestedScalarIndex) {
    auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

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

    size_t N = 200;
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

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Load vector index
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);
    std::vector<float> vector_data(dim * N * array_len);
    for (size_t i = 0; i < N; i++) {
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

    // Build nested scalar index with is_nested=true (the correct behavior
    // after the fix sets field_name so IndexFactory routes to CreateNestedIndex).
    std::vector<int32_t> all_elements;
    all_elements.reserve(N * array_len);
    for (size_t row = 0; row < N; row++) {
        for (int elem = 0; elem < array_len; elem++) {
            all_elements.push_back(row * array_len + elem + 1);
        }
    }

    auto stl_index = std::make_unique<milvus::index::ScalarIndexSort<int32_t>>(
        storage::FileManagerContext(),
        true /* is_nested=true: correct after fix */);
    stl_index->Build(all_elements.size(), all_elements.data(), nullptr);

    LoadIndexInfo nested_load_info;
    nested_load_info.field_id = int_array_fid.get();
    nested_load_info.field_type = DataType::ARRAY;
    nested_load_info.element_type = DataType::INT32;
    nested_load_info.index_params["index_type"] = milvus::index::ASCENDING_SORT;
    nested_load_info.cache_index =
        CreateTestCacheIndex("nested_test", std::move(stl_index));
    segment->LoadIndex(nested_load_info);

    // Build query plan: element_filter with nested index in full mode
    int topK = 5;
    ScopedSchemaHandle handle(*schema);
    std::string search_params = R"({"ef": 50})";
    // High selectivity (~50%) forces full mode evaluation
    std::string expr =
        "id % 2 == 0 && element_filter(structA, 2000 > $[price_array] > 100)";
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

    // With is_nested=true, the index correctly returns element-level results
    // matching the element_filter expression's expectations.
    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->element_level_);

    EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
}

// Regression test for element-level search with multiple chunks in growing
// segments. The bug was that begin_id was set to row_begin (row offset)
// instead of the cumulative element offset, causing wrong row IDs to be
// returned for chunks after the first one.
// See: https://github.com/milvus-io/milvus/issues/48617
TEST(ElementFilter, GrowingMultiChunkElementSearch) {
    int dim = 4;
    int array_len = 3;
    size_t N = 200;

    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField(
        "structA[array_vec]", DataType::VECTOR_FLOAT, dim, "L2");
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    // Customize int_array: doc i has elements [i*3+1, i*3+2, i*3+3]
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
                    array_data->mutable_int_data()->mutable_data()->Add(
                        row * array_len + elem + 1);
                }
            }
            break;
        }
    }

    // Use small chunk_rows=32 to force multiple chunks (200 rows / 32 = 7
    // chunks). The bug only manifests with multiple chunks.
    SegcoreConfig config;
    config.set_chunk_rows(32);

    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    // Verify ArrayOffsets
    auto growing_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing_impl, nullptr);
    auto offsets = growing_impl->GetArrayOffsets(vec_fid);
    ASSERT_NE(offsets, nullptr);
    ASSERT_EQ(offsets->GetRowCount(), N);
    ASSERT_EQ(offsets->GetTotalElementCount(), N * array_len);

    // Element-level search with element_filter that selects elements from
    // rows in later chunks (value > 300 means row >= 100, which is beyond
    // chunk 0-2).
    int topK = 5;
    ScopedSchemaHandle handle(*schema);
    std::string expr =
        "element_filter(structA, $[price_array] > 300 && $[price_array] < 400)";
    auto plan_bytes = handle.ParseSearch(
        expr, "structA[array_vec]", topK, "L2", R"({"ef": 50})", 3);
    auto plan =
        CreateSearchPlanByExpr(schema, plan_bytes.data(), plan_bytes.size());
    ASSERT_NE(plan, nullptr);

    auto ph_group_raw = CreatePlaceholderGroup(1, dim, 1024, true);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    auto search_result = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(search_result, nullptr);
    ASSERT_TRUE(search_result->element_level_);

    for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
        int64_t doc_id = search_result->seg_offsets_[i];
        int32_t elem_idx = search_result->element_indices_[i];
        if (doc_id == INVALID_SEG_OFFSET) {
            continue;
        }

        ASSERT_GE(doc_id, 0);
        ASSERT_LT(doc_id, static_cast<int64_t>(N))
            << "doc_id should be a valid row index";
        ASSERT_GE(elem_idx, 0);
        ASSERT_LT(elem_idx, array_len);

        // Verify the element satisfies the filter: value in (300, 400)
        int element_value = doc_id * array_len + elem_idx + 1;
        ASSERT_GT(element_value, 300)
            << "doc_id=" << doc_id << " elem_idx=" << elem_idx
            << " element_value=" << element_value << " should be > 300";
        ASSERT_LT(element_value, 400)
            << "doc_id=" << doc_id << " elem_idx=" << elem_idx
            << " element_value=" << element_value << " should be < 400";
    }
}

namespace {

constexpr int kElemDim = 4;
constexpr int kElemArrayLen = 3;
constexpr size_t kElemN = 200;
constexpr int kElemTopK = 10;
// Ground-truth target: the query vector equals row kElemTargetDoc's element
// kElemTargetElem, so an exact (BF) search must return it first with
// distance ~0. A mid-range row + non-zero elem index makes "always returns
// the first element" bugs visible.
constexpr int64_t kElemTargetDoc = 5;
constexpr int32_t kElemTargetElem = 1;

struct ElementSearchFixture {
    SchemaPtr schema;
    FieldId vec_fid;
    FieldId int64_fid;
    GeneratedData raw_data;
    std::vector<float> flat_data;   // kN * kArrayLen * kDim
    std::vector<float> query_data;  // kDim
};

inline ElementSearchFixture
MakeElementSearchFixture() {
    ElementSearchFixture f;
    f.schema = std::make_shared<Schema>();
    f.vec_fid = f.schema->AddDebugVectorArrayField(
        "structA[array_vec]", DataType::VECTOR_FLOAT, kElemDim, "L2");
    f.int64_fid = f.schema->AddDebugField("id", DataType::INT64);
    f.schema->set_primary_field_id(f.int64_fid);

    f.raw_data = DataGen(f.schema, kElemN, 42, 0, 1, kElemArrayLen);
    auto array_vec_values = f.raw_data.get_col<VectorFieldProto>(f.vec_fid);
    f.flat_data.resize(array_vec_values.size() * kElemArrayLen * kElemDim);
    for (size_t i = 0; i < array_vec_values.size(); ++i) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        for (int j = 0; j < kElemArrayLen * kElemDim; ++j) {
            f.flat_data[i * kElemArrayLen * kElemDim + j] = float_vec[j];
        }
    }
    const size_t target_off =
        (kElemTargetDoc * kElemArrayLen + kElemTargetElem) * kElemDim;
    f.query_data.assign(f.flat_data.begin() + target_off,
                        f.flat_data.begin() + target_off + kElemDim);
    return f;
}

inline proto::common::PlaceholderGroup
MakeElementLevelPlaceholder(const std::vector<float>& query_data) {
    auto raw = CreatePlaceholderGroupFromBlob<milvus::FloatVector>(
        /*num_queries=*/1, kElemDim, query_data.data());
    raw.mutable_placeholders(0)->set_element_level(true);
    return raw;
}

inline void
LoadElementHnswIndex(SegmentSealed* segment,
                     FieldId vec_fid,
                     const std::vector<float>& flat_data) {
    auto indexing = GenVecIndexing(kElemN * kElemArrayLen,
                                   kElemDim,
                                   flat_data.data(),
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
}

inline void
ExpectElementLevelShape(const milvus::SearchResult& sr) {
    ASSERT_TRUE(sr.element_level_);
    ASSERT_FALSE(sr.seg_offsets_.empty());
    ASSERT_EQ(sr.element_indices_.size(), sr.seg_offsets_.size());
    for (size_t i = 0; i < sr.seg_offsets_.size(); ++i) {
        if (sr.seg_offsets_[i] < 0) {
            continue;
        }
        ASSERT_GE(sr.element_indices_[i], 0);
        ASSERT_LT(sr.element_indices_[i], kElemArrayLen);
    }
}

inline void
ExpectSortedAscending(const milvus::SearchResult& sr) {
    int valid = 0;
    for (size_t i = 0; i < sr.distances_.size(); ++i) {
        if (sr.seg_offsets_[i] < 0) {
            continue;
        }
        ++valid;
        if (i > 0 && sr.seg_offsets_[i - 1] >= 0) {
            ASSERT_LE(sr.distances_[i - 1], sr.distances_[i]);
        }
    }
    ASSERT_GT(valid, 0);
}

inline void
ExpectWithinRadius(const milvus::SearchResult& sr, float radius) {
    for (size_t i = 0; i < sr.seg_offsets_.size(); ++i) {
        if (sr.seg_offsets_[i] < 0) {
            continue;
        }
        ASSERT_LT(sr.distances_[i], radius);
        ASSERT_GE(sr.distances_[i], 0.0f);
    }
}

// Exact path (BF): the target element must sit at index 0 with distance ~0.
inline void
ExpectTopOneIsTarget(const milvus::SearchResult& sr) {
    ASSERT_FALSE(sr.seg_offsets_.empty());
    ASSERT_EQ(sr.seg_offsets_[0], kElemTargetDoc);
    ASSERT_EQ(sr.element_indices_[0], kElemTargetElem);
    ASSERT_NEAR(sr.distances_[0], 0.0f, 1e-5f);
}

// Approximate path (HNSW): the target must appear in the top-K, but may not
// be strictly first.
inline void
ExpectTargetInTopK(const milvus::SearchResult& sr) {
    bool found = false;
    for (size_t i = 0; i < sr.seg_offsets_.size(); ++i) {
        if (sr.seg_offsets_[i] == kElemTargetDoc &&
            sr.element_indices_[i] == kElemTargetElem) {
            found = true;
            EXPECT_NEAR(sr.distances_[i], 0.0f, 1e-3f);
            break;
        }
    }
    ASSERT_TRUE(found) << "Target (doc=" << kElemTargetDoc
                       << ", elem=" << kElemTargetElem
                       << ") not found in approximate search result";
}

}  // namespace

TEST(ElementVectorSearch, GrowingBruteForce_RangeSearch) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateGrowingSegment(f.schema, empty_index_meta);
    segment->PreInsert(kElemN);
    segment->Insert(0,
                    kElemN,
                    f.raw_data.row_ids_.data(),
                    f.raw_data.timestamps_.data(),
                    f.raw_data.raw_);

    ScopedSchemaHandle handle(*f.schema);
    const float radius = 1e6f;
    const std::string search_params =
        R"({"radius": )" + std::to_string(radius) + R"(, "range_filter": 0.0})";
    auto plan_bytes = handle.ParseSearch(
        "", "structA[array_vec]", kElemTopK, "L2", search_params, 3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectWithinRadius(*sr, radius);
    ExpectTopOneIsTarget(*sr);
}

TEST(ElementVectorSearch, GrowingBruteForce_IteratorV2) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateGrowingSegment(f.schema, empty_index_meta);
    segment->PreInsert(kElemN);
    segment->Insert(0,
                    kElemN,
                    f.raw_data.row_ids_.data(),
                    f.raw_data.timestamps_.data(),
                    f.raw_data.raw_);

    ScopedSchemaHandle handle(*f.schema);
    auto plan_bytes =
        handle.ParseSearchIterator("",
                                   "structA[array_vec]",
                                   kElemTopK,
                                   "L2",
                                   R"({"ef": 50})",
                                   static_cast<uint32_t>(kElemTopK),
                                   "",
                                   std::nullopt,
                                   3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectSortedAscending(*sr);
    ExpectTopOneIsTarget(*sr);
}

TEST(ElementVectorSearch, SealedBruteForce_RangeSearch) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateSealedWithFieldDataLoaded(f.schema, f.raw_data);

    ScopedSchemaHandle handle(*f.schema);
    const float radius = 1e6f;
    const std::string search_params =
        R"({"radius": )" + std::to_string(radius) + R"(, "range_filter": 0.0})";
    auto plan_bytes = handle.ParseSearch(
        "", "structA[array_vec]", kElemTopK, "L2", search_params, 3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectWithinRadius(*sr, radius);
    ExpectTopOneIsTarget(*sr);
}

TEST(ElementVectorSearch, SealedBruteForce_IteratorV2) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateSealedWithFieldDataLoaded(f.schema, f.raw_data);

    ScopedSchemaHandle handle(*f.schema);
    auto plan_bytes =
        handle.ParseSearchIterator("",
                                   "structA[array_vec]",
                                   kElemTopK,
                                   "L2",
                                   R"({"ef": 50})",
                                   static_cast<uint32_t>(kElemTopK),
                                   "",
                                   std::nullopt,
                                   3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectSortedAscending(*sr);
    ExpectTopOneIsTarget(*sr);
}

TEST(ElementVectorSearch, SealedIndex_RangeSearch) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateSealedWithFieldDataLoaded(f.schema, f.raw_data);
    LoadElementHnswIndex(segment.get(), f.vec_fid, f.flat_data);

    ScopedSchemaHandle handle(*f.schema);
    const float radius = 1e6f;
    const std::string search_params = R"({"ef": 50, "radius": )" +
                                      std::to_string(radius) +
                                      R"(, "range_filter": 0.0})";
    auto plan_bytes = handle.ParseSearch(
        "", "structA[array_vec]", kElemTopK, "L2", search_params, 3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectWithinRadius(*sr, radius);
    ExpectTargetInTopK(*sr);
}

TEST(ElementVectorSearch, SealedIndex_IteratorV2) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateSealedWithFieldDataLoaded(f.schema, f.raw_data);
    LoadElementHnswIndex(segment.get(), f.vec_fid, f.flat_data);

    ScopedSchemaHandle handle(*f.schema);
    auto plan_bytes =
        handle.ParseSearchIterator("",
                                   "structA[array_vec]",
                                   kElemTopK,
                                   "L2",
                                   R"({"ef": 50})",
                                   static_cast<uint32_t>(kElemTopK),
                                   "",
                                   std::nullopt,
                                   3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectSortedAscending(*sr);
    ExpectTargetInTopK(*sr);
}

// ---- Multi-chunk growing -------------------------------------------------
// Default chunk_rows is 1024, so the 6 tests above fit in one chunk. Here
// we shrink chunk_rows so N=200 spans ~4 chunks, exercising the per-chunk
// flatten loop in SearchOnGrowing.cpp and the chunk merge in
// ChunkMergeIterator.

namespace {

constexpr int64_t kElemChunkRows = 64;

inline SegmentGrowingPtr
CreateMultiChunkGrowingSegment(const ElementSearchFixture& f) {
    auto config = SegcoreConfig::default_config();
    config.set_chunk_rows(kElemChunkRows);
    auto segment = CreateGrowingSegment(f.schema, empty_index_meta, 1, config);
    segment->PreInsert(kElemN);
    segment->Insert(0,
                    kElemN,
                    f.raw_data.row_ids_.data(),
                    f.raw_data.timestamps_.data(),
                    f.raw_data.raw_);
    static_assert(kElemN > kElemChunkRows,
                  "N must exceed chunk_rows to exercise multi-chunk paths");
    return segment;
}

}  // namespace

TEST(ElementVectorSearch, GrowingMultiChunk_RangeSearch) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateMultiChunkGrowingSegment(f);

    ScopedSchemaHandle handle(*f.schema);
    const float radius = 1e6f;
    const std::string search_params =
        R"({"radius": )" + std::to_string(radius) + R"(, "range_filter": 0.0})";
    auto plan_bytes = handle.ParseSearch(
        "", "structA[array_vec]", kElemTopK, "L2", search_params, 3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectWithinRadius(*sr, radius);
    ExpectTopOneIsTarget(*sr);
}

TEST(ElementVectorSearch, GrowingMultiChunk_IteratorV2) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateMultiChunkGrowingSegment(f);

    ScopedSchemaHandle handle(*f.schema);
    auto plan_bytes =
        handle.ParseSearchIterator("",
                                   "structA[array_vec]",
                                   kElemTopK,
                                   "L2",
                                   R"({"ef": 50})",
                                   static_cast<uint32_t>(kElemTopK),
                                   "",
                                   std::nullopt,
                                   3);
    auto plan =
        CreateSearchPlanByExpr(f.schema, plan_bytes.data(), plan_bytes.size());

    auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
    auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
    ASSERT_NE(sr, nullptr);
    ExpectElementLevelShape(*sr);
    ExpectSortedAscending(*sr);
    ExpectTopOneIsTarget(*sr);
}

// ---- Iterator_v2: consecutive NextBatch rounds ---------------------------
// Each search call runs exactly one NextBatch. Chain rounds by feeding the
// previous batch's last distance as last_bound: post-round distances must
// stay strictly greater than that bound (L2, lower-is-better).

TEST(ElementVectorSearch, SealedBruteForce_IteratorV2_MultiBatch) {
    auto f = MakeElementSearchFixture();
    auto segment = CreateSealedWithFieldDataLoaded(f.schema, f.raw_data);

    ScopedSchemaHandle handle(*f.schema);
    std::optional<float> last_bound;
    std::set<std::pair<int64_t, int32_t>> seen;
    const int kRounds = 3;
    for (int round = 0; round < kRounds; ++round) {
        auto plan_bytes =
            handle.ParseSearchIterator("",
                                       "structA[array_vec]",
                                       kElemTopK,
                                       "L2",
                                       R"({"ef": 50})",
                                       static_cast<uint32_t>(kElemTopK),
                                       "",
                                       last_bound,
                                       3);
        auto plan = CreateSearchPlanByExpr(
            f.schema, plan_bytes.data(), plan_bytes.size());
        auto ph_group_raw = MakeElementLevelPlaceholder(f.query_data);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());
        auto sr = segment->Search(plan.get(), ph_group.get(), 1L << 63);
        ASSERT_NE(sr, nullptr);
        ExpectElementLevelShape(*sr);
        ExpectSortedAscending(*sr);

        float round_last = last_bound.value_or(-1.0f);
        for (size_t i = 0; i < sr->seg_offsets_.size(); ++i) {
            if (sr->seg_offsets_[i] < 0) {
                continue;
            }
            if (last_bound.has_value()) {
                ASSERT_GT(sr->distances_[i], *last_bound)
                    << "round " << round
                    << " returned a distance not strictly past last_bound";
            }
            auto key =
                std::make_pair(sr->seg_offsets_[i], sr->element_indices_[i]);
            ASSERT_TRUE(seen.insert(key).second)
                << "iterator returned duplicate (doc=" << key.first
                << ", elem=" << key.second << ") across rounds";
            round_last = sr->distances_[i];
        }
        last_bound = round_last;
    }
    ASSERT_GE(seen.size(), static_cast<size_t>(kElemTopK))
        << "multi-batch iterator should accumulate strictly more results "
        << "than a single batch";
}
