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
#include <memory>
#include <vector>
#include <boost/format.hpp>

#include "common/Schema.h"
#include "common/ArrayOffsets.h"
#include "query/Plan.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

class ElementFilterSealed
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    bool
    use_hints() const {
        return std::get<0>(GetParam());
    }
    bool
    load_index() const {
        return std::get<1>(GetParam());
    }
};

TEST_P(ElementFilterSealed, RangeExpr) {
    bool with_hints = use_hints();
    bool with_load_index = load_index();
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

    // Step 4: Load vector index for element-level search
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);

    // DataGen generates VECTOR_ARRAY with data in float_vector (flattened),
    // not in vector_array (nested structure)
    std::vector<float> vector_data(dim * N * array_len);
    for (int i = 0; i < N; i++) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        // float_vec contains array_len * dim floats
        for (int j = 0; j < array_len * dim; j++) {
            vector_data[i * array_len * dim + j] = float_vec[j];
        }
    }

    // For element-level search, index all elements (N * array_len vectors)
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
    if (with_load_index) {
        segment->LoadIndex(load_index_info);
    }

    int topK = 5;

    // Step 5: Test with element-level filter
    // Query: Search array elements, filter by element_value in (100, 400)
    {
        std::string hints_line =
            with_hints ? R"(hints: "iterative_filter")" : "";
        std::string raw_plan = boost::str(boost::format(R"(vector_anns: <
                                        field_id: %1%
                                        predicates: <
                                          element_filter_expr: <
                                            element_expr: <
                                              binary_range_expr: <
                                                column_info: <
                                                  field_id: %2%
                                                  data_type: Int32
                                                  element_type: Int32
                                                  is_element_level: true
                                                >
                                                lower_inclusive: false
                                                upper_inclusive: false
                                                lower_value: <
                                                  int64_val: 100
                                                >
                                                upper_value: <
                                                  int64_val: 400
                                                >
                                              >
                                            >
                                            predicate: <
                                              binary_arith_op_eval_range_expr: <
                                                column_info: <
                                                  field_id: %3%
                                                  data_type: Int64
                                                >
                                                arith_op: Mod
                                                right_operand: <
                                                  int64_val: 2
                                                >
                                                op: Equal
                                                value: <
                                                  int64_val: 0
                                                >
                                              >
                                            >
                                            struct_name: "structA"
                                          >
                                        >
                                        query_info: <
                                          topk: 5
                                          round_decimal: 3
                                          metric_type: "L2"
                                          %4%
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)") %
                                          vec_fid.get() % int_array_fid.get() %
                                          int64_fid.get() % hints_line);

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        ASSERT_TRUE(ok) << "Failed to parse element-level filter plan";

        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
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
        ASSERT_LE(search_result->element_indices_.size(), topK * num_queries);

        std::cout << "Element-level search returned:" << std::endl;
        for (auto i = 0; i < search_result->seg_offsets_.size(); i++) {
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

    // Step 4: Load vector index for element-level search
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);

    // DataGen generates VECTOR_ARRAY with data in float_vector (flattened),
    // not in vector_array (nested structure)
    std::vector<float> vector_data(dim * N * array_len);
    for (int i = 0; i < N; i++) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        // float_vec contains array_len * dim floats
        for (int j = 0; j < array_len * dim; j++) {
            vector_data[i * array_len * dim + j] = float_vec[j];
        }
    }

    // For element-level search, index all elements (N * array_len vectors)
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
    if (with_load_index) {
        segment->LoadIndex(load_index_info);
    }

    int topK = 5;

    // Step 5: Test with element-level filter
    // Query: Search array elements, filter by element_value < 10
    {
        std::string hints_line =
            with_hints ? R"(hints: "iterative_filter")" : "";
        std::string raw_plan = boost::str(boost::format(R"(vector_anns: <
                                      field_id: %1%
                                      predicates: <
                                        element_filter_expr: <
                                          element_expr: <
                                            unary_range_expr: <
                                              column_info: <
                                                field_id: %2%
                                                data_type: Int32
                                                element_type: Int32
                                                is_element_level: true
                                              >
                                              op: GreaterThan
                                              value: <
                                                int64_val: 10
                                              >
                                            >
                                          >
                                          predicate: <
                                            binary_arith_op_eval_range_expr: <
                                              column_info: <
                                                field_id: %3%
                                                data_type: Int64
                                              >
                                              arith_op: Mod
                                              right_operand: <
                                                int64_val: 2
                                              >
                                              op: Equal
                                              value: <
                                                int64_val: 0
                                              >
                                            >
                                          >
                                          struct_name: "structA"
                                        >
                                      >
                                      query_info: <
                                        topk: 5
                                        round_decimal: 3
                                        metric_type: "L2"
                                        %4%
                                        search_params: "{\"ef\": 50}"
                                      >
                                      placeholder_tag: "$0">)") %
                                          vec_fid.get() % int_array_fid.get() %
                                          int64_fid.get() % hints_line);

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        ASSERT_TRUE(ok) << "Failed to parse element-level filter plan";

        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
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
        ASSERT_LE(search_result->element_indices_.size(), topK * num_queries);

        std::cout << "Element-level search returned:" << std::endl;
        for (auto i = 0; i < search_result->seg_offsets_.size(); i++) {
            std::cout << "doc_id: " << search_result->seg_offsets_[i]
                      << ", element_index: "
                      << search_result->element_indices_[i] << std::endl;
            std::cout << "distance: " << search_result->distances_[i]
                      << std::endl;
        }

        // Verify distances are sorted (ascending for L2)
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
    ::testing::Combine(::testing::Bool(),  // with_hints: true/false
                       ::testing::Bool()   // with_load_index: true/false
                       ),
    [](const ::testing::TestParamInfo<ElementFilterSealed::ParamType>& info) {
        bool with_hints = std::get<0>(info.param);
        bool with_load_index = std::get<1>(info.param);
        std::string name = "";
        name += with_hints ? "WithHints" : "WithoutHints";
        name += "_";
        name += with_load_index ? "WithLoadIndex" : "WithoutLoadIndex";
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

// Parameterized test fixture for GrowingIterativeRangeExpr
class ElementFilterGrowing : public ::testing::TestWithParam<bool> {
 protected:
    bool
    use_hints() const {
        return GetParam();
    }
};

TEST_P(ElementFilterGrowing, RangeExpr) {
    bool with_hints = use_hints();
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
    // Query: Search array elements where (id % 2 == 0) AND (price_array element in range (100, 400))
    {
        std::string hints_line =
            with_hints ? R"(hints: "iterative_filter")" : "";
        std::string raw_plan = boost::str(boost::format(R"(vector_anns: <
                                        field_id: %1%
                                        predicates: <
                                          element_filter_expr: <
                                            element_expr: <
                                              binary_range_expr: <
                                                column_info: <
                                                  field_id: %2%
                                                  data_type: Int32
                                                  element_type: Int32
                                                  is_element_level: true
                                                >
                                                lower_inclusive: false
                                                upper_inclusive: false
                                                lower_value: <
                                                  int64_val: 100
                                                >
                                                upper_value: <
                                                  int64_val: 400
                                                >
                                              >
                                            >
                                            predicate: <
                                              binary_arith_op_eval_range_expr: <
                                                column_info: <
                                                  field_id: %3%
                                                  data_type: Int64
                                                >
                                                arith_op: Mod
                                                right_operand: <
                                                  int64_val: 2
                                                >
                                                op: Equal
                                                value: <
                                                  int64_val: 0
                                                >
                                              >
                                            >
                                            struct_name: "structA"
                                          >
                                        >
                                        query_info: <
                                          topk: 5
                                          round_decimal: 3
                                          metric_type: "L2"
                                          %4%
                                          search_params: "{\"ef\": 50}"
                                        >
                                        placeholder_tag: "$0">)") %
                                          vec_fid.get() % int_array_fid.get() %
                                          int64_fid.get() % hints_line);

        proto::plan::PlanNode plan_node;
        auto ok =
            google::protobuf::TextFormat::ParseFromString(raw_plan, &plan_node);
        ASSERT_TRUE(ok) << "Failed to parse element-level filter plan";

        auto plan = CreateSearchPlanFromPlanNode(schema, plan_node);
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results
        ASSERT_NE(search_result, nullptr);

        // In element-level mode, results should contain element indices
        ASSERT_TRUE(search_result->element_level_)
            << "Search should be in element-level mode";
        ASSERT_FALSE(search_result->element_indices_.empty())
            << "Should have element indices";
        ASSERT_FALSE(search_result->seg_offsets_.empty())
            << "Should have doc offsets";
        ASSERT_EQ(search_result->element_indices_.size(),
                  search_result->seg_offsets_.size())
            << "Element indices and doc offsets should match in size";

        // Should have topK results per query
        ASSERT_LE(search_result->element_indices_.size(), topK * num_queries)
            << "Should not exceed topK results";

        std::cout << "Growing segment element-level search results:"
                  << std::endl;
        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            int32_t elem_idx = search_result->element_indices_[i];
            float distance = search_result->distances_[i];

            std::cout << "  [" << i << "] doc_id=" << doc_id
                      << ", element_index=" << elem_idx
                      << ", distance=" << distance << std::endl;

            // Verify the doc_id satisfies the predicate (id % 2 == 0)
            ASSERT_EQ(doc_id % 2, 0) << "Result doc_id " << doc_id
                                     << " should satisfy (id % 2 == 0)";

            // Verify element_idx is valid
            ASSERT_GE(elem_idx, 0) << "Element index should be >= 0";
            ASSERT_LT(elem_idx, array_len)
                << "Element index should be < array_len";

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

INSTANTIATE_TEST_SUITE_P(
    ElementFilter,
    ElementFilterGrowing,
    ::testing::Bool(),  // with_hints: true/false
    [](const ::testing::TestParamInfo<ElementFilterGrowing::ParamType>& info) {
        bool with_hints = info.param;
        return with_hints ? "WithHints" : "WithoutHints";
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
