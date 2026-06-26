// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License

#include <boost/container/vector.hpp>
#include <boost/cstdint.hpp>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/Array.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/PrometheusClient.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/Utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

class MatchExprTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Set batch size to 100 for testing multiple batches
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        // Create schema with struct array sub-fields
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);

        sub_str_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_str]", DataType::VARCHAR, false);
        sub_int_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_int]", DataType::INT32, false);

        // Generate test data
        GenerateTestData();

        // Create and populate segment
        seg_ = CreateGrowingSegment(schema_, empty_index_meta);
        seg_->PreInsert(N_);
        seg_->Insert(
            0, N_, row_ids_.data(), timestamps_.data(), insert_data_.get());
    }

    void
    TearDown() override {
        // Restore original batch size
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(42);
        std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
        std::uniform_int_distribution<> str_dist(0, 2);
        std::uniform_int_distribution<> int_dist(50, 150);

        insert_data_ = std::make_unique<InsertRecordProto>();

        // Generate vector field
        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data_->mutable_fields_data()->AddAllocated(vec_array.release());

        // Generate id field
        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = i;
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data_->mutable_fields_data()->AddAllocated(id_array.release());

        // Generate struct_array[sub_str]
        sub_str_data_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            for (int j = 0; j < array_len_; ++j) {
                sub_str_data_[i].mutable_string_data()->add_data(
                    str_choices[str_dist(rng)]);
            }
        }
        auto sub_str_array =
            CreateDataArrayFrom(sub_str_data_.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_str_fid_));
        insert_data_->mutable_fields_data()->AddAllocated(
            sub_str_array.release());

        // Generate struct_array[sub_int]
        sub_int_data_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            for (int j = 0; j < array_len_; ++j) {
                sub_int_data_[i].mutable_int_data()->add_data(int_dist(rng));
            }
        }
        auto sub_int_array =
            CreateDataArrayFrom(sub_int_data_.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_int_fid_));
        insert_data_->mutable_fields_data()->AddAllocated(
            sub_int_array.release());

        insert_data_->set_num_rows(N_);

        // Generate row_ids and timestamps
        row_ids_.resize(N_);
        timestamps_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            row_ids_[i] = i;
            timestamps_[i] = i;
        }
    }

    // Count elements matching: sub_str == "aaa" && sub_int > 100
    int
    CountMatchingElements(int64_t row_idx) const {
        int count = 0;
        const auto& str_field = sub_str_data_[row_idx];
        const auto& int_field = sub_int_data_[row_idx];
        for (int j = 0; j < array_len_; ++j) {
            bool str_match = (str_field.string_data().data(j) == "aaa");
            bool int_match = (int_field.int_data().data(j) > 100);
            if (str_match && int_match) {
                ++count;
            }
        }
        return count;
    }

    // Create filter expression with specified match type and count
    std::string
    CreateFilterExpr(const std::string& match_type, int64_t count) {
        // match_type is like "MatchAny", "MatchAll", "MatchLeast", "MatchMost", "MatchExact"
        // Convert to expression format: match_any, match_all, match_least, match_most, match_exact
        std::string predicate = R"($[sub_str] == "aaa" && $[sub_int] > 100)";

        if (match_type == "MatchAny") {
            return "match_any(struct_array, " + predicate + ")";
        } else if (match_type == "MatchAll") {
            return "match_all(struct_array, " + predicate + ")";
        } else if (match_type == "MatchLeast") {
            return "match_least(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchMost") {
            return "match_most(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchExact") {
            return "match_exact(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        }
        return "";
    }

    // Execute search and return results
    std::unique_ptr<SearchResult>
    ExecuteSearch(const std::string& filter_expr) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str =
            schema_handle.ParseSearch(filter_expr,          // expression
                                      "vec",                // vector field name
                                      10,                   // topK
                                      "L2",                 // metric_type
                                      R"({"nprobe": 10})",  // search_params
                                      3                     // round_decimal
            );
        auto plan =
            CreateSearchPlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        auto num_queries = 1;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, 4, 1024);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        return seg_->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // Verify results based on match type
    using VerifyFunc = std::function<bool(
        int match_count, int element_count, int64_t threshold)>;

    void
    VerifyResults(const SearchResult* result,
                  const std::string& match_type_name,
                  int64_t threshold,
                  VerifyFunc verify_func) {
        std::cout << "=== " << match_type_name << " Results ===" << std::endl;
        std::cout << "total_nq: " << result->total_nq_ << std::endl;
        std::cout << "unity_topK: " << result->unity_topK_ << std::endl;
        std::cout << "num_results: " << result->seg_offsets_.size()
                  << std::endl;

        for (int64_t i = 0; i < result->total_nq_; ++i) {
            std::cout << "Query " << i << ":" << std::endl;
            for (int64_t k = 0; k < result->unity_topK_; ++k) {
                int64_t idx = i * result->unity_topK_ + k;
                auto offset = result->seg_offsets_[idx];
                auto distance = result->distances_[idx];

                std::cout << "  [" << k << "] offset=" << offset
                          << ", distance=" << distance;

                if (offset >= 0 && offset < static_cast<int64_t>(N_)) {
                    // Print sub_str array
                    std::cout << ", sub_str=[";
                    const auto& str_field = sub_str_data_[offset];
                    for (int j = 0; j < str_field.string_data().data_size();
                         ++j) {
                        if (j > 0)
                            std::cout << ",";
                        std::cout << str_field.string_data().data(j);
                    }
                    std::cout << "]";

                    // Print sub_int array
                    std::cout << ", sub_int=[";
                    const auto& int_field = sub_int_data_[offset];
                    for (int j = 0; j < int_field.int_data().data_size(); ++j) {
                        if (j > 0)
                            std::cout << ",";
                        std::cout << int_field.int_data().data(j);
                    }
                    std::cout << "]";

                    // Print match_count and verify
                    int match_count = CountMatchingElements(offset);
                    bool expected =
                        verify_func(match_count, array_len_, threshold);
                    std::cout << ", match_count=" << match_count;

                    EXPECT_TRUE(expected)
                        << match_type_name << " failed for row " << offset
                        << ": match_count=" << match_count
                        << ", element_count=" << array_len_
                        << ", threshold=" << threshold;
                }
                std::cout << std::endl;
            }
        }
        std::cout << "==============================" << std::endl;
    }

    // Execute retrieve and return results (full-scan, not top-K limited).
    std::unique_ptr<proto::segcore::RetrieveResults>
    ExecuteRetrieve(const std::string& filter_expr) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str = schema_handle.Parse(filter_expr);
        auto plan =
            CreateRetrievePlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        return seg_->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    }

    // Compute the exact set of rows that should match, directly from the
    // inserted data and the predicate semantics encoded by verify_func.
    std::set<int64_t>
    ComputeExpectedRows(int64_t threshold, VerifyFunc verify_func) const {
        std::set<int64_t> expected;
        for (size_t i = 0; i < N_; ++i) {
            int match_count = CountMatchingElements(static_cast<int64_t>(i));
            if (verify_func(match_count, array_len_, threshold)) {
                expected.insert(static_cast<int64_t>(i));
            }
        }
        return expected;
    }

    // Full-recall verification: issue a Retrieve with the same predicate and
    // assert the returned offset set is EXACTLY the expected matched set
    // (catches both false positives AND false negatives, unlike the top-K
    // search check in VerifyResults).
    void
    VerifyRetrieveRecall(const std::string& match_type_name,
                         const std::string& filter_expr,
                         int64_t threshold,
                         VerifyFunc verify_func) {
        auto result = ExecuteRetrieve(filter_expr);
        ASSERT_NE(result, nullptr);

        auto expected_rows = ComputeExpectedRows(threshold, verify_func);

        std::set<int64_t> actual_rows;
        for (const auto& offset : result->offset()) {
            actual_rows.insert(offset);
        }

        std::vector<int64_t> missing_rows;
        for (auto row : expected_rows) {
            if (actual_rows.find(row) == actual_rows.end()) {
                missing_rows.push_back(row);
            }
        }
        std::vector<int64_t> extra_rows;
        for (auto row : actual_rows) {
            if (expected_rows.find(row) == expected_rows.end()) {
                extra_rows.push_back(row);
            }
        }

        std::cout << "=== " << match_type_name
                  << " Retrieve Recall (Growing) ===" << std::endl;
        std::cout << "Expected rows: " << expected_rows.size()
                  << ", Actual rows: " << actual_rows.size() << std::endl;

        EXPECT_TRUE(missing_rows.empty())
            << match_type_name << " has " << missing_rows.size()
            << " false negatives";
        EXPECT_TRUE(extra_rows.empty())
            << match_type_name << " has " << extra_rows.size()
            << " false positives";
        EXPECT_EQ(expected_rows, actual_rows)
            << match_type_name << " matched-row set mismatch";
    }

    // Member variables
    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId sub_str_fid_;
    FieldId sub_int_fid_;

    std::unique_ptr<InsertRecordProto> insert_data_;
    std::vector<milvus::proto::schema::ScalarField> sub_str_data_;
    std::vector<milvus::proto::schema::ScalarField> sub_int_data_;
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;

    SegmentGrowingPtr seg_;

    static constexpr size_t N_ = 1000;
    static constexpr int array_len_ = 5;
    int64_t saved_batch_size_{0};
};

TEST_F(MatchExprTest, MatchAny) {
    auto filter_expr = CreateFilterExpr("MatchAny", 0);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchAny",
        0,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            // MatchAny: at least one element matches
            return match_count > 0;
        });

    // Full-recall check: assert the Retrieve returns EXACTLY the expected set.
    VerifyRetrieveRecall(
        "MatchAny",
        filter_expr,
        0,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(MatchExprTest, MatchAll) {
    auto filter_expr = CreateFilterExpr("MatchAll", 0);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchAll",
        0,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            // MatchAll: all elements must match
            return match_count == element_count;
        });

    VerifyRetrieveRecall(
        "MatchAll",
        filter_expr,
        0,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(MatchExprTest, MatchLeast) {
    const int64_t threshold = 3;
    auto filter_expr = CreateFilterExpr("MatchLeast", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchLeast(3)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchLeast: at least N elements match
            return match_count >= threshold;
        });

    VerifyRetrieveRecall(
        "MatchLeast(3)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(MatchExprTest, MatchMost) {
    const int64_t threshold = 2;
    auto filter_expr = CreateFilterExpr("MatchMost", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchMost(2)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchMost: at most N elements match
            return match_count <= threshold;
        });

    VerifyRetrieveRecall(
        "MatchMost(2)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(MatchExprTest, MatchExact) {
    const int64_t threshold = 2;
    auto filter_expr = CreateFilterExpr("MatchExact", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchExact(2)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchExact: exactly N elements match
            return match_count == threshold;
        });

    VerifyRetrieveRecall(
        "MatchExact(2)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

// Edge case: MatchLeast with threshold = 1 (equivalent to MatchAny)
TEST_F(MatchExprTest, MatchLeastOne) {
    const int64_t threshold = 1;
    auto filter_expr = CreateFilterExpr("MatchLeast", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchLeast(1)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });

    VerifyRetrieveRecall(
        "MatchLeast(1)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

// Edge case: MatchMost with threshold = 0 (no elements should match)
TEST_F(MatchExprTest, MatchMostZero) {
    const int64_t threshold = 0;
    auto filter_expr = CreateFilterExpr("MatchMost", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchMost(0)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });

    VerifyRetrieveRecall(
        "MatchMost(0)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

// Edge case: MatchExact with threshold = 0 (no elements should match)
TEST_F(MatchExprTest, MatchExactZero) {
    const int64_t threshold = 0;
    auto filter_expr = CreateFilterExpr("MatchExact", threshold);
    auto result = ExecuteSearch(filter_expr);

    VerifyResults(
        result.get(),
        "MatchExact(0)",
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });

    VerifyRetrieveRecall(
        "MatchExact(0)",
        filter_expr,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

TEST(MatchExprZeroElementBatch, MatchAnyTreatsEmptyRowsAsNoMatch) {
    struct BatchSizeGuard {
        int64_t saved;
        ~BatchSizeGuard() {
            EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
        }
    } guard{EXEC_EVAL_EXPR_BATCH_SIZE.load()};
    EXEC_EVAL_EXPR_BATCH_SIZE.store(2);

    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);
    auto sub_int_fid = schema->AddDebugArrayField(
        "struct_array[sub_int]", DataType::INT32, false);

    constexpr int64_t N = 3;
    auto insert_data = std::make_unique<InsertRecordProto>();

    std::vector<int64_t> ids = {0, 1, 2};
    auto id_array = CreateDataArrayFrom(
        ids.data(), nullptr, N, schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    std::vector<milvus::proto::schema::ScalarField> sub_int_data(N);
    sub_int_data[2].mutable_int_data()->add_data(9001);
    auto sub_int_array = CreateDataArrayFrom(
        sub_int_data.data(), nullptr, N, schema->operator[](sub_int_fid));
    insert_data->mutable_fields_data()->AddAllocated(sub_int_array.release());
    insert_data->set_num_rows(N);

    GeneratedData generated_data;
    generated_data.schema_ = schema;
    generated_data.raw_ = insert_data.release();
    for (int64_t i = 0; i < N; ++i) {
        generated_data.row_ids_.push_back(i);
        generated_data.timestamps_.push_back(i);
    }

    auto segment = CreateSealedWithFieldDataLoaded(schema, generated_data);
    ScopedSchemaHandle schema_handle(*schema);
    auto plan_str =
        schema_handle.Parse("match_any(struct_array, $[sub_int] >= 9000)");
    auto plan =
        CreateRetrievePlanByExpr(schema, plan_str.data(), plan_str.size());
    ASSERT_NE(plan, nullptr);

    auto result = segment->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->offset_size(), 1);
    EXPECT_EQ(result->offset(0), 2);
}

class SealedMatchExprTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Set batch size to 100 for testing multiple batches
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        // Create schema with struct array sub-fields
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);

        // Add struct array sub-fields with naming convention: struct_name[sub_field]
        sub_str_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_str]", DataType::VARCHAR, false);
        sub_int_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_int]", DataType::INT32, false);

        // Generate controlled test data (like MatchExprTest)
        GenerateTestData();

        // Create sealed segment with generated data
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);

        // Load nested inverted indexes
        LoadNestedInvertedIndexes();
    }

    void
    TearDown() override {
        // Restore original batch size
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(42);
        std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
        std::uniform_int_distribution<> str_dist(0, 2);
        std::uniform_int_distribution<> int_dist(50, 150);

        auto insert_data = std::make_unique<InsertRecordProto>();

        // Generate vector field
        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        // Generate id field
        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = i;
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        // Generate struct_array[sub_str] with limited choices
        std::vector<milvus::proto::schema::ScalarField> sub_str_data(N_);
        sub_str_arrays_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            boost::container::vector<std::string> arr;
            for (int j = 0; j < array_len_; ++j) {
                std::string val = str_choices[str_dist(rng)];
                sub_str_data[i].mutable_string_data()->add_data(val);
                arr.push_back(val);
            }
            sub_str_arrays_[i] = std::move(arr);
        }
        auto sub_str_array =
            CreateDataArrayFrom(sub_str_data.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_str_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            sub_str_array.release());

        // Generate struct_array[sub_int] with controlled range
        std::vector<milvus::proto::schema::ScalarField> sub_int_data(N_);
        sub_int_arrays_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            boost::container::vector<int32_t> arr;
            for (int j = 0; j < array_len_; ++j) {
                int32_t val = int_dist(rng);
                sub_int_data[i].mutable_int_data()->add_data(val);
                arr.push_back(val);
            }
            sub_int_arrays_[i] = std::move(arr);
        }
        auto sub_int_array =
            CreateDataArrayFrom(sub_int_data.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_int_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            sub_int_array.release());

        insert_data->set_num_rows(N_);

        // Create GeneratedData
        generated_data_.schema_ = schema_;
        generated_data_.raw_ = insert_data.release();
        for (size_t i = 0; i < N_; ++i) {
            generated_data_.row_ids_.push_back(i);
            generated_data_.timestamps_.push_back(i);
        }
    }

    void
    LoadNestedInvertedIndexes() {
        // Load nested index for sub_str field
        {
            auto index =
                std::make_unique<index::InvertedIndexTantivy<std::string>>();
            Config cfg;
            cfg["is_array"] = true;
            cfg["is_nested_index"] = true;
            index->BuildWithRawDataForUT(N_, sub_str_arrays_.data(), cfg);
            LoadIndexInfo info{};
            info.field_id = sub_str_fid_.get();
            info.index_params = GenIndexParams(index.get());
            info.cache_index =
                CreateTestCacheIndex("sub_str", std::move(index));
            seg_->LoadIndex(info);
        }

        // Load nested index for sub_int field
        {
            auto index =
                std::make_unique<index::InvertedIndexTantivy<int32_t>>();
            Config cfg;
            cfg["is_array"] = true;
            cfg["is_nested_index"] = true;
            index->BuildWithRawDataForUT(N_, sub_int_arrays_.data(), cfg);
            LoadIndexInfo info{};
            info.field_id = sub_int_fid_.get();
            info.index_params = GenIndexParams(index.get());
            info.cache_index =
                CreateTestCacheIndex("sub_int", std::move(index));
            seg_->LoadIndex(info);
        }
    }

    // Count elements matching: sub_str == target_str && sub_int > target_int
    int
    CountMatchingElements(int64_t row_idx,
                          const std::string& target_str,
                          int32_t target_int) const {
        int count = 0;
        size_t len = std::min(sub_str_arrays_[row_idx].size(),
                              sub_int_arrays_[row_idx].size());
        for (size_t j = 0; j < len; ++j) {
            bool str_match = (sub_str_arrays_[row_idx][j] == target_str);
            bool int_match = (sub_int_arrays_[row_idx][j] > target_int);
            if (str_match && int_match) {
                ++count;
            }
        }
        return count;
    }

    // Create filter expression with specified match type, count, and target values
    std::string
    CreateSealedFilterExpr(const std::string& match_type,
                           int64_t count,
                           const std::string& target_str,
                           int32_t target_int) {
        std::string predicate = "$[sub_str] == \"" + target_str +
                                "\" && $[sub_int] > " +
                                std::to_string(target_int);

        if (match_type == "MatchAny") {
            return "match_any(struct_array, " + predicate + ")";
        } else if (match_type == "MatchAll") {
            return "match_all(struct_array, " + predicate + ")";
        } else if (match_type == "MatchLeast") {
            return "match_least(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchMost") {
            return "match_most(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchExact") {
            return "match_exact(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        }
        return "";
    }

    // Execute search and return results
    std::unique_ptr<SearchResult>
    ExecuteSealedSearch(const std::string& filter_expr) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str =
            schema_handle.ParseSearch(filter_expr,          // expression
                                      "vec",                // vector field name
                                      10,                   // topK
                                      "L2",                 // metric_type
                                      R"({"nprobe": 10})",  // search_params
                                      3                     // round_decimal
            );
        auto plan =
            CreateSearchPlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        auto num_queries = 1;
        auto ph_group_raw = CreatePlaceholderGroup(num_queries, 4, 1024);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        return seg_->Search(plan.get(), ph_group.get(), 1L << 63);
    }

    // Verify results based on match type
    using VerifyFunc = std::function<bool(
        int match_count, int element_count, int64_t threshold)>;

    void
    VerifySealedResults(const SearchResult* result,
                        const std::string& match_type_name,
                        int64_t threshold,
                        const std::string& target_str,
                        int32_t target_int,
                        VerifyFunc verify_func) {
        std::cout << "=== " << match_type_name
                  << " Results (Sealed) ===" << std::endl;
        std::cout << "total_nq: " << result->total_nq_ << std::endl;
        std::cout << "unity_topK: " << result->unity_topK_ << std::endl;
        std::cout << "num_results: " << result->seg_offsets_.size()
                  << std::endl;
        std::cout << "query: sub_str == \"" << target_str << "\" && sub_int > "
                  << target_int << std::endl;

        for (int64_t i = 0; i < result->total_nq_; ++i) {
            std::cout << "Query " << i << ":" << std::endl;
            for (int64_t k = 0; k < result->unity_topK_; ++k) {
                int64_t idx = i * result->unity_topK_ + k;
                auto offset = result->seg_offsets_[idx];
                auto distance = result->distances_[idx];

                std::cout << "  [" << k << "] offset=" << offset
                          << ", distance=" << distance;

                if (offset >= 0 && offset < static_cast<int64_t>(N_)) {
                    // Print sub_str array
                    std::cout << ", sub_str=[";
                    const auto& str_arr = sub_str_arrays_[offset];
                    for (size_t j = 0; j < str_arr.size(); ++j) {
                        if (j > 0)
                            std::cout << ",";
                        std::cout << str_arr[j];
                    }
                    std::cout << "]";

                    // Print sub_int array
                    std::cout << ", sub_int=[";
                    const auto& int_arr = sub_int_arrays_[offset];
                    for (size_t j = 0; j < int_arr.size(); ++j) {
                        if (j > 0)
                            std::cout << ",";
                        std::cout << int_arr[j];
                    }
                    std::cout << "]";

                    // Print match_count and verify
                    int match_count =
                        CountMatchingElements(offset, target_str, target_int);
                    bool expected =
                        verify_func(match_count, array_len_, threshold);
                    std::cout << ", match_count=" << match_count;

                    EXPECT_TRUE(expected)
                        << match_type_name << " failed for row " << offset
                        << ": match_count=" << match_count
                        << ", element_count=" << array_len_
                        << ", threshold=" << threshold;
                }
                std::cout << std::endl;
            }
        }
        std::cout << "==============================" << std::endl;
    }

    // Create retrieve filter expression - reuses the sealed filter expression
    std::string
    CreateRetrieveFilterExpr(const std::string& match_type,
                             int64_t count,
                             const std::string& target_str,
                             int32_t target_int) {
        // Same expression format as search filter
        return CreateSealedFilterExpr(
            match_type, count, target_str, target_int);
    }

    // Execute retrieve and return results
    std::unique_ptr<proto::segcore::RetrieveResults>
    ExecuteRetrieve(const std::string& filter_expr) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str = schema_handle.Parse(filter_expr);
        auto plan =
            CreateRetrievePlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        return seg_->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    }

    // Compute expected matching rows
    std::set<int64_t>
    ComputeExpectedRows(const std::string& target_str,
                        int32_t target_int,
                        int64_t threshold,
                        VerifyFunc verify_func) {
        std::set<int64_t> expected;
        for (size_t i = 0; i < N_; ++i) {
            int match_count = CountMatchingElements(i, target_str, target_int);
            if (verify_func(match_count, array_len_, threshold)) {
                expected.insert(static_cast<int64_t>(i));
            }
        }
        return expected;
    }

    // Verify retrieve results - check both precision and recall
    void
    VerifyRetrieveResults(const proto::segcore::RetrieveResults* result,
                          const std::string& match_type_name,
                          int64_t threshold,
                          const std::string& target_str,
                          int32_t target_int,
                          VerifyFunc verify_func) {
        // Compute expected rows
        auto expected_rows =
            ComputeExpectedRows(target_str, target_int, threshold, verify_func);

        // Get actual rows from result
        std::set<int64_t> actual_rows;
        for (const auto& offset : result->offset()) {
            actual_rows.insert(offset);
        }

        std::cout << "=== " << match_type_name
                  << " Retrieve Results ===" << std::endl;
        std::cout << "Expected rows: " << expected_rows.size() << std::endl;
        std::cout << "Actual rows: " << actual_rows.size() << std::endl;

        // Check for false negatives (rows that should be returned but weren't)
        std::vector<int64_t> missing_rows;
        for (auto row : expected_rows) {
            if (actual_rows.find(row) == actual_rows.end()) {
                missing_rows.push_back(row);
            }
        }

        // Check for false positives (rows that shouldn't be returned but were)
        std::vector<int64_t> extra_rows;
        for (auto row : actual_rows) {
            if (expected_rows.find(row) == expected_rows.end()) {
                extra_rows.push_back(row);
            }
        }

        if (!missing_rows.empty()) {
            std::cout << "Missing rows (false negatives): ";
            for (size_t i = 0; i < std::min(missing_rows.size(), size_t(10));
                 ++i) {
                std::cout << missing_rows[i] << " ";
            }
            if (missing_rows.size() > 10)
                std::cout << "... (" << missing_rows.size() << " total)";
            std::cout << std::endl;
        }

        if (!extra_rows.empty()) {
            std::cout << "Extra rows (false positives): ";
            for (size_t i = 0; i < std::min(extra_rows.size(), size_t(10));
                 ++i) {
                std::cout << extra_rows[i] << " ";
            }
            if (extra_rows.size() > 10)
                std::cout << "... (" << extra_rows.size() << " total)";
            std::cout << std::endl;
        }

        EXPECT_TRUE(missing_rows.empty())
            << match_type_name << " has " << missing_rows.size()
            << " false negatives";
        EXPECT_TRUE(extra_rows.empty())
            << match_type_name << " has " << extra_rows.size()
            << " false positives";
        EXPECT_EQ(expected_rows.size(), actual_rows.size())
            << match_type_name << " row count mismatch";

        std::cout << "==============================" << std::endl;
    }

    // Member variables
    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId sub_str_fid_;
    FieldId sub_int_fid_;

    std::vector<boost::container::vector<std::string>> sub_str_arrays_;
    std::vector<boost::container::vector<int32_t>> sub_int_arrays_;

    GeneratedData generated_data_;
    SegmentSealedUPtr seg_;

    static constexpr size_t N_ = 1000;
    static constexpr int array_len_ = 5;
    int64_t saved_batch_size_{0};
};

TEST_F(SealedMatchExprTest, MatchAnyWithNestedIndex) {
    // Use fixed query values matching MatchExprTest pattern
    // sub_str has choices {"aaa", "bbb", "ccc"}, each ~1/3 probability
    // sub_int range [50, 150], query > 100 matches ~50%
    // Combined: ~1/6 elements match, with array_len=5, ~0.83 matches per row
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAny",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            // MatchAny: at least one element matches
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTest, MatchAllWithNestedIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAll",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            // MatchAll: all elements must match
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTest, MatchLeastWithNestedIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchLeast(2)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchLeast: at least N elements match
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTest, MatchMostWithNestedIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchMost", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchMost(3)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchMost: at most N elements match
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTest, MatchExactWithNestedIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchExact", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchExact(1)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            // MatchExact: exactly N elements match
            return match_count == threshold;
        });
}

// Test fixture for sealed segment WITHOUT any nested index (brute force scan)
class SealedMatchExprTestNoIndex : public SealedMatchExprTest {
 protected:
    void
    SetUp() override {
        // Set batch size to 100 for testing multiple batches
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        // Create schema with struct array sub-fields
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);

        sub_str_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_str]", DataType::VARCHAR, false);
        sub_int_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_int]", DataType::INT32, false);

        GenerateTestData();

        // Create sealed segment WITHOUT loading any index
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);
        // No LoadNestedInvertedIndexes() call - brute force path
    }
};

TEST_F(SealedMatchExprTestNoIndex, MatchAnyNoIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAny (No Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTestNoIndex, MatchAllNoIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAll (No Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTestNoIndex, MatchLeastNoIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchLeast(2) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTestNoIndex, MatchMostNoIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchMost", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchMost(3) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTestNoIndex, MatchExactNoIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchExact", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchExact(1) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

// Test fixture for sealed segment with PARTIAL index
// (one field has index, another doesn't)
class SealedMatchExprTestPartialIndex : public SealedMatchExprTest {
 protected:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);

        sub_str_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_str]", DataType::VARCHAR, false);
        sub_int_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_int]", DataType::INT32, false);

        GenerateTestData();

        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);

        // Only load index for sub_str field, NOT for sub_int
        LoadPartialIndex();
    }

    void
    LoadPartialIndex() {
        // Only load nested index for sub_str field
        auto index =
            std::make_unique<index::InvertedIndexTantivy<std::string>>();
        Config cfg;
        cfg["is_array"] = true;
        cfg["is_nested_index"] = true;
        index->BuildWithRawDataForUT(N_, sub_str_arrays_.data(), cfg);
        LoadIndexInfo info{};
        info.field_id = sub_str_fid_.get();
        info.index_params = GenIndexParams(index.get());
        info.cache_index = CreateTestCacheIndex("sub_str", std::move(index));
        seg_->LoadIndex(info);
        // sub_int field has NO index - will use brute force
    }
};

TEST_F(SealedMatchExprTestPartialIndex, MatchAnyPartialIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAny (Partial Index: sub_str only)",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, MatchAllPartialIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchAll (Partial Index: sub_str only)",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, MatchLeastPartialIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchLeast(2) (Partial Index: sub_str only)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, MatchMostPartialIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchMost", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchMost(3) (Partial Index: sub_str only)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, MatchExactPartialIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateSealedFilterExpr("MatchExact", threshold, target_str, target_int);
    auto result = ExecuteSealedSearch(filter_expr);

    VerifySealedResults(
        result.get(),
        "MatchExact(1) (Partial Index: sub_str only)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

// ==================== Retrieve Tests ====================
// These tests verify that ALL matching rows are returned (no false negatives)
// and no non-matching rows are returned (no false positives)

TEST_F(SealedMatchExprTest, RetrieveMatchAnyWithIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAny (With Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTest, RetrieveMatchAllWithIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAll (With Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTest, RetrieveMatchLeastWithIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchLeast(2) (With Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTest, RetrieveMatchMostWithIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchMost", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchMost(3) (With Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTest, RetrieveMatchExactWithIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchExact", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchExact(1) (With Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

TEST_F(SealedMatchExprTestNoIndex, RetrieveMatchAnyNoIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAny (No Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTestNoIndex, RetrieveMatchAllNoIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAll (No Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTestNoIndex, RetrieveMatchLeastNoIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchLeast(2) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTestNoIndex, RetrieveMatchMostNoIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchMost", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchMost(3) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTestNoIndex, RetrieveMatchExactNoIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchExact", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchExact(1) (No Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, RetrieveMatchAnyPartialIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAny (Partial Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, RetrieveMatchAllPartialIndex) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAll", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchAll (Partial Index)",
        0,
        target_str,
        target_int,
        [](int match_count, int element_count, int64_t /*threshold*/) {
            return match_count == element_count;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, RetrieveMatchLeastPartialIndex) {
    const int64_t threshold = 2;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchLeast(2) (Partial Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, RetrieveMatchMostPartialIndex) {
    const int64_t threshold = 3;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchMost", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchMost(3) (Partial Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count <= threshold;
        });
}

TEST_F(SealedMatchExprTestPartialIndex, RetrieveMatchExactPartialIndex) {
    const int64_t threshold = 1;
    std::string target_str = "aaa";
    int32_t target_int = 100;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchExact", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    VerifyRetrieveResults(
        result.get(),
        "Retrieve MatchExact(1) (Partial Index)",
        threshold,
        target_str,
        target_int,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count == threshold;
        });
}

// Test combining match expression with other expressions (id % 2 == 0 && match_any)
TEST_F(SealedMatchExprTestNoIndex, MatchWithOtherExpr) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    // Expression: (id % 2 == 0) && match_any(struct_array, $[sub_str] == "aaa" && $[sub_int] > 100)
    std::string predicate = "$[sub_str] == \"" + target_str +
                            "\" && $[sub_int] > " + std::to_string(target_int);
    std::string filter_expr =
        "id % 2 == 0 && match_any(struct_array, " + predicate + ")";

    auto result = ExecuteRetrieve(filter_expr);

    // Verify: all results should have id % 2 == 0 AND match_count > 0
    std::cout << "=== MatchWithOtherExpr (id %% 2 == 0 && MatchAny) ==="
              << std::endl;
    std::cout << "Retrieved " << result->offset_size() << " rows" << std::endl;

    int verified_count = 0;
    for (int i = 0; i < result->offset_size(); ++i) {
        int64_t id = result->offset(i);
        int match_count = CountMatchingElements(id, target_str, target_int);
        bool id_even = (id % 2 == 0);
        bool has_match = (match_count > 0);

        // Both conditions must be true
        EXPECT_TRUE(id_even) << "Row " << id << " should have id %% 2 == 0";
        EXPECT_TRUE(has_match)
            << "Row " << id << " should have match_count > 0";

        if (id_even && has_match) {
            ++verified_count;
        }
    }

    std::cout << "Verified " << verified_count << " rows meet both conditions"
              << std::endl;
    EXPECT_GT(result->offset_size(), 0)
        << "Should have at least some matching rows";
    std::cout << "==============================" << std::endl;
}

TEST_F(SealedMatchExprTestNoIndex, ConjunctSkipMovesMatchChildCursor) {
    std::string target_str = "aaa";
    int32_t target_int = 100;

    // Batch size is 100 in this fixture. The id predicate rejects the first two
    // batches completely, so ConjunctExpr short-circuits and skips MatchExpr.
    std::string predicate = "$[sub_str] == \"" + target_str +
                            "\" && $[sub_int] > " + std::to_string(target_int);
    std::string filter_expr =
        "id > 199 && match_any(struct_array, " + predicate + ")";

    auto result = ExecuteRetrieve(filter_expr);

    std::set<int64_t> expected_rows;
    for (size_t i = 0; i < N_; ++i) {
        if (i > 199 && CountMatchingElements(i, target_str, target_int) > 0) {
            expected_rows.insert(static_cast<int64_t>(i));
        }
    }

    std::set<int64_t> actual_rows;
    for (const auto offset : result->offset()) {
        actual_rows.insert(offset);
    }

    EXPECT_EQ(expected_rows, actual_rows);
}

// ==================== Parameterized Test for Different Int Types ====================
// This tests that int8_t/int16_t/int32_t are correctly handled in ProcessDataChunksForElementLevel

struct IntTypeTestParam {
    DataType int_type;
    std::string type_name;
};

class SealedMatchExprIntTypeTest
    : public ::testing::TestWithParam<IntTypeTestParam> {
 protected:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        auto param = GetParam();
        int_type_ = param.int_type;

        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);

        // Add struct array sub-fields with the parameterized int type
        sub_str_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_str]", DataType::VARCHAR, false);
        sub_int_fid_ = schema_->AddDebugArrayField(
            "struct_array[sub_int]", int_type_, false);

        GenerateTestData();
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);
        // No index loaded - test brute force path (ProcessDataChunksForElementLevel)
    }

    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(42);
        std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
        std::uniform_int_distribution<> str_dist(0, 2);
        // Use small range for int8 compatibility: [-50, 50]
        std::uniform_int_distribution<> int_dist(-50, 50);

        auto insert_data = std::make_unique<InsertRecordProto>();

        // Generate vector field
        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        // Generate id field
        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = i;
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        // Generate struct_array[sub_str]
        std::vector<milvus::proto::schema::ScalarField> sub_str_data(N_);
        sub_str_arrays_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            boost::container::vector<std::string> arr;
            for (int j = 0; j < array_len_; ++j) {
                std::string val = str_choices[str_dist(rng)];
                sub_str_data[i].mutable_string_data()->add_data(val);
                arr.push_back(val);
            }
            sub_str_arrays_[i] = std::move(arr);
        }
        auto sub_str_array =
            CreateDataArrayFrom(sub_str_data.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_str_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            sub_str_array.release());

        // Generate struct_array[sub_int] - store as int32 in proto (will be cast)
        std::vector<milvus::proto::schema::ScalarField> sub_int_data(N_);
        sub_int_arrays_.resize(N_);
        for (size_t i = 0; i < N_; ++i) {
            boost::container::vector<int32_t> arr;
            for (int j = 0; j < array_len_; ++j) {
                int32_t val = int_dist(rng);
                sub_int_data[i].mutable_int_data()->add_data(val);
                arr.push_back(val);
            }
            sub_int_arrays_[i] = std::move(arr);
        }
        auto sub_int_array =
            CreateDataArrayFrom(sub_int_data.data(),
                                nullptr,
                                N_,
                                schema_->operator[](sub_int_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            sub_int_array.release());

        insert_data->set_num_rows(N_);

        generated_data_.schema_ = schema_;
        generated_data_.raw_ = insert_data.release();
        for (size_t i = 0; i < N_; ++i) {
            generated_data_.row_ids_.push_back(i);
            generated_data_.timestamps_.push_back(i);
        }
    }

    int
    CountMatchingElements(int64_t row_idx,
                          const std::string& target_str,
                          int32_t target_int) const {
        int count = 0;
        size_t len = std::min(sub_str_arrays_[row_idx].size(),
                              sub_int_arrays_[row_idx].size());
        for (size_t j = 0; j < len; ++j) {
            bool str_match = (sub_str_arrays_[row_idx][j] == target_str);
            bool int_match = (sub_int_arrays_[row_idx][j] > target_int);
            if (str_match && int_match) {
                ++count;
            }
        }
        return count;
    }

    // Create retrieve filter expression with specified match type, count, and target values
    std::string
    CreateRetrieveFilterExpr(const std::string& match_type,
                             int64_t count,
                             const std::string& target_str,
                             int32_t target_int) {
        std::string predicate = "$[sub_str] == \"" + target_str +
                                "\" && $[sub_int] > " +
                                std::to_string(target_int);

        if (match_type == "MatchAny") {
            return "match_any(struct_array, " + predicate + ")";
        } else if (match_type == "MatchAll") {
            return "match_all(struct_array, " + predicate + ")";
        } else if (match_type == "MatchLeast") {
            return "match_least(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchMost") {
            return "match_most(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        } else if (match_type == "MatchExact") {
            return "match_exact(struct_array, " + predicate +
                   ", threshold=" + std::to_string(count) + ")";
        }
        return "";
    }

    std::unique_ptr<proto::segcore::RetrieveResults>
    ExecuteRetrieve(const std::string& filter_expr) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str = schema_handle.Parse(filter_expr);
        auto plan =
            CreateRetrievePlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        return seg_->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    }

    std::set<int64_t>
    ComputeExpectedRows(const std::string& target_str,
                        int32_t target_int,
                        int64_t threshold,
                        std::function<bool(int, int, int64_t)> verify_func) {
        std::set<int64_t> expected;
        for (size_t i = 0; i < N_; ++i) {
            int match_count = CountMatchingElements(i, target_str, target_int);
            if (verify_func(match_count, array_len_, threshold)) {
                expected.insert(static_cast<int64_t>(i));
            }
        }
        return expected;
    }

    DataType int_type_;
    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId sub_str_fid_;
    FieldId sub_int_fid_;

    std::vector<boost::container::vector<std::string>> sub_str_arrays_;
    std::vector<boost::container::vector<int32_t>> sub_int_arrays_;

    GeneratedData generated_data_;
    SegmentSealedUPtr seg_;

    static constexpr size_t N_ = 1000;
    static constexpr int array_len_ = 5;
    int64_t saved_batch_size_{0};
};

TEST_P(SealedMatchExprIntTypeTest, MatchAnyBruteForce) {
    auto param = GetParam();
    std::string target_str = "aaa";
    int32_t target_int = 0;  // Use 0 as threshold for more matches

    auto filter_expr =
        CreateRetrieveFilterExpr("MatchAny", 0, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    auto expected_rows = ComputeExpectedRows(
        target_str,
        target_int,
        0,
        [](int match_count, int /*element_count*/, int64_t /*threshold*/) {
            return match_count > 0;
        });

    std::set<int64_t> actual_rows;
    for (const auto& offset : result->offset()) {
        actual_rows.insert(offset);
    }

    std::cout << "=== MatchAny BruteForce (" << param.type_name
              << ") ===" << std::endl;
    std::cout << "Expected rows: " << expected_rows.size() << std::endl;
    std::cout << "Actual rows: " << actual_rows.size() << std::endl;

    // Check for mismatches
    std::vector<int64_t> missing_rows;
    for (auto row : expected_rows) {
        if (actual_rows.find(row) == actual_rows.end()) {
            missing_rows.push_back(row);
        }
    }

    std::vector<int64_t> extra_rows;
    for (auto row : actual_rows) {
        if (expected_rows.find(row) == expected_rows.end()) {
            extra_rows.push_back(row);
        }
    }

    if (!missing_rows.empty()) {
        std::cout << "Missing rows (first 10): ";
        for (size_t i = 0; i < std::min(missing_rows.size(), size_t(10)); ++i) {
            std::cout << missing_rows[i] << " ";
        }
        std::cout << std::endl;
    }

    if (!extra_rows.empty()) {
        std::cout << "Extra rows (first 10): ";
        for (size_t i = 0; i < std::min(extra_rows.size(), size_t(10)); ++i) {
            std::cout << extra_rows[i] << " ";
        }
        std::cout << std::endl;
    }

    EXPECT_TRUE(missing_rows.empty())
        << param.type_name << " has " << missing_rows.size()
        << " false negatives";
    EXPECT_TRUE(extra_rows.empty()) << param.type_name << " has "
                                    << extra_rows.size() << " false positives";
    EXPECT_EQ(expected_rows.size(), actual_rows.size())
        << param.type_name << " row count mismatch";

    std::cout << "==============================" << std::endl;
}

TEST_P(SealedMatchExprIntTypeTest, MatchLeastBruteForce) {
    auto param = GetParam();
    std::string target_str = "aaa";
    int32_t target_int = 0;
    int64_t threshold = 2;

    auto filter_expr = CreateRetrieveFilterExpr(
        "MatchLeast", threshold, target_str, target_int);
    auto result = ExecuteRetrieve(filter_expr);

    auto expected_rows = ComputeExpectedRows(
        target_str,
        target_int,
        threshold,
        [](int match_count, int /*element_count*/, int64_t threshold) {
            return match_count >= threshold;
        });

    std::set<int64_t> actual_rows;
    for (const auto& offset : result->offset()) {
        actual_rows.insert(offset);
    }

    std::cout << "=== MatchLeast(2) BruteForce (" << param.type_name
              << ") ===" << std::endl;
    std::cout << "Expected rows: " << expected_rows.size() << std::endl;
    std::cout << "Actual rows: " << actual_rows.size() << std::endl;

    EXPECT_EQ(expected_rows.size(), actual_rows.size())
        << param.type_name << " row count mismatch";

    std::cout << "==============================" << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    IntTypes,
    SealedMatchExprIntTypeTest,
    ::testing::Values(IntTypeTestParam{DataType::INT8, "INT8"},
                      IntTypeTestParam{DataType::INT16, "INT16"},
                      IntTypeTestParam{DataType::INT32, "INT32"}),
    [](const ::testing::TestParamInfo<IntTypeTestParam>& info) {
        return info.param.type_name;
    });

// ============================================================================
// Scalar ARRAY field MATCH_*/element_filter tests.
//
// These exercise MATCH_* over a top-level scalar ARRAY field (not a struct
// array sub-field). The expression syntax uses a bare `$` to refer to the
// element value, e.g. `MATCH_ANY(scores, $ > 90)`. The plan is built through
// the real Go planparserv2 (via ScopedSchemaHandle) and executed with
// Retrieve so we can assert the exact set of matched rows.
//
// Runs for BOTH sealed and growing segments.
// ============================================================================
enum class ScalarArraySegType { kSealed, kGrowing };

class ScalarArrayMatchExprTest
    : public ::testing::TestWithParam<ScalarArraySegType> {
 protected:
    // Build an insert proto holding: pk (INT64) + a scalar Array<Int64>
    // "scores" field with the given per-row contents.
    std::unique_ptr<InsertRecordProto>
    BuildInt64ArrayInsert(const Schema& schema,
                          FieldId pk_fid,
                          FieldId scores_fid,
                          const std::vector<std::vector<int64_t>>& rows) {
        auto insert_data = std::make_unique<InsertRecordProto>();
        const int64_t N = static_cast<int64_t>(rows.size());

        std::vector<int64_t> ids(N);
        std::iota(ids.begin(), ids.end(), 0);
        auto id_array =
            CreateDataArrayFrom(ids.data(), nullptr, N, schema[pk_fid]);
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        std::vector<milvus::proto::schema::ScalarField> scores(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : rows[i]) {
                scores[i].mutable_long_data()->add_data(v);
            }
        }
        auto scores_array =
            CreateDataArrayFrom(scores.data(), nullptr, N, schema[scores_fid]);
        insert_data->mutable_fields_data()->AddAllocated(
            scores_array.release());

        insert_data->set_num_rows(N);
        return insert_data;
    }

    // Create a segment (sealed or growing per param) populated with the insert
    // proto. Returns a SegmentInterface usable for Retrieve.
    std::shared_ptr<SegmentInterface>
    MakeSegment(SchemaPtr schema, std::unique_ptr<InsertRecordProto> insert) {
        const int64_t N = insert->num_rows();
        std::vector<idx_t> row_ids(N);
        std::vector<Timestamp> tss(N);
        for (int64_t i = 0; i < N; ++i) {
            row_ids[i] = i;
            tss[i] = i;
        }

        if (GetParam() == ScalarArraySegType::kGrowing) {
            auto seg = CreateGrowingSegment(schema, empty_index_meta);
            seg->PreInsert(N);
            seg->Insert(0, N, row_ids.data(), tss.data(), insert.get());
            return std::shared_ptr<SegmentInterface>(std::move(seg));
        }

        GeneratedData generated;
        generated.schema_ = schema;
        generated.raw_ = insert.release();
        for (int64_t i = 0; i < N; ++i) {
            generated.row_ids_.push_back(i);
            generated.timestamps_.push_back(i);
        }
        auto seg = CreateSealedWithFieldDataLoaded(schema, generated);
        return std::shared_ptr<SegmentInterface>(std::move(seg));
    }

    // Parse `expr` against `schema`, run Retrieve, and return the matched
    // segment offsets as a set.
    std::set<int64_t>
    RetrieveMatchedRows(SegmentInterface* seg,
                        const Schema& schema,
                        SchemaPtr schema_ptr,
                        const std::string& expr) {
        ScopedSchemaHandle schema_handle(schema);
        auto plan_str = schema_handle.Parse(expr);
        auto plan = CreateRetrievePlanByExpr(
            schema_ptr, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);
        auto result = seg->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
        EXPECT_NE(result, nullptr);
        std::set<int64_t> rows;
        for (const auto& offset : result->offset()) {
            rows.insert(offset);
        }
        return rows;
    }
};

TEST_P(ScalarArrayMatchExprTest, Int64Array) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, false);

    // Rows: [[95,80],[40],[100,100,100],[]]
    std::vector<std::vector<int64_t>> rows = {
        {95, 80}, {40}, {100, 100, 100}, {}};
    auto insert = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(scores, $ > 90) -> rows {0, 2}
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, "MATCH_ANY(scores, $ > 90)"),
              (std::set<int64_t>{0, 2}));

    // MATCH_ALL(scores, $ >= 60): row0 (95,80) all>=60 true;
    // row1 (40) false; row2 (100,100,100) true; row3 [] vacuous true.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, "MATCH_ALL(scores, $ >= 60)"),
              (std::set<int64_t>{0, 2, 3}));

    // MATCH_LEAST(scores, $ == 100, threshold=2) -> only row2 has >=2.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  "MATCH_LEAST(scores, $ == 100, threshold=2)"),
              (std::set<int64_t>{2}));

    // MATCH_EXACT(scores, $ == 100, threshold=3) -> only row2 has exactly 3.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  "MATCH_EXACT(scores, $ == 100, threshold=3)"),
              (std::set<int64_t>{2}));

    // MATCH_MOST(scores, $ > 90, threshold=0): match count <= 0.
    // row0 (95,80) -> 1 match; row1 (40) -> 0; row2 (100,100,100) -> 3;
    // row3 [] -> 0 (empty array). Only rows with 0 matches qualify -> {1, 3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  "MATCH_MOST(scores, $ > 90, threshold=0)"),
              (std::set<int64_t>{1, 3}));

    // Compound element predicate with two `$` references in one predicate.
    // MATCH_ANY(scores, $ > 60 && $ < 90): an element matches when it is in
    // (60, 90). row0 (95,80) -> 80 matches; row1 (40) -> none; row2 (100s) ->
    // none; row3 [] -> none. -> {0}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, "MATCH_ANY(scores, $ > 60 && $ < 90)"),
        (std::set<int64_t>{0}));

    // MATCH_ALL(scores, $ >= 40 && $ <= 100): all elements in [40, 100].
    // row0 (95,80) all in range; row1 (40) in range; row2 (100s) in range;
    // row3 [] vacuously true. -> {0, 1, 2, 3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  "MATCH_ALL(scores, $ >= 40 && $ <= 100)"),
              (std::set<int64_t>{0, 1, 2, 3}));

    // Range form: the grammar accepts `$` inside a ternary range, lowering to a
    // BinaryRangeExpr over the element. MATCH_ANY(scores, 60 < $ < 90) is
    // equivalent to the compound `$ > 60 && $ < 90` above -> {0}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, "MATCH_ANY(scores, 60 < $ < 90)"),
              (std::set<int64_t>{0}));

    // Empty-array edge case made explicit. row3 holds an empty array:
    //   - MATCH_ALL is vacuously true for it (no element violates the
    //     predicate), so row3 IS included.
    //   - MATCH_ANY requires at least one matching element, so an empty array
    //     can never satisfy it and row3 is excluded.
    auto match_all_60 = RetrieveMatchedRows(
        seg.get(), *schema, schema, "MATCH_ALL(scores, $ >= 60)");
    EXPECT_NE(match_all_60.find(3), match_all_60.end())
        << "empty-array row must satisfy MATCH_ALL (vacuous truth)";
    auto match_any_any = RetrieveMatchedRows(
        seg.get(), *schema, schema, "MATCH_ANY(scores, $ >= 0)");
    EXPECT_EQ(match_any_any.find(3), match_any_any.end())
        << "empty-array row must never satisfy MATCH_ANY";

    // NOTE(VERIFY): standalone `element_filter(scores, $ > 90)` is intentionally
    // NOT asserted here. Although planparserv2 parses it, element_filter lowers
    // to an element-level bitset (ElementFilterBitsNode) intended for
    // element-level vector search; it is not a row-level Retrieve predicate (the
    // proxy rejects it as such, see task_search.go). Row-level array filtering
    // must use MATCH_ANY/MATCH_*. Adding a row-set assertion here would encode
    // undefined behavior, so it is deliberately omitted.
}

TEST_P(ScalarArrayMatchExprTest, VarCharArray) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid =
        schema->AddDebugArrayField("tags", DataType::VARCHAR, false);

    // Rows: [["x","y"],["z"],[],["x"]]
    std::vector<std::vector<std::string>> rows = {{"x", "y"}, {"z"}, {}, {"x"}};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto insert = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_array.release());

    std::vector<milvus::proto::schema::ScalarField> tags(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            tags[i].mutable_string_data()->add_data(v);
        }
    }
    auto tags_array = CreateDataArrayFrom(
        tags.data(), nullptr, N, schema->operator[](tags_fid));
    insert->mutable_fields_data()->AddAllocated(tags_array.release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(tags, $ == "x") -> rows {0, 3}
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0, 3}));

    // MATCH_ALL(tags, $ != ""): every element is non-empty.
    // row0 ("x","y") all non-empty; row1 ("z") non-empty; row2 [] vacuously
    // true; row3 ("x") non-empty. -> {0, 1, 2, 3}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, R"(MATCH_ALL(tags, $ != ""))"),
              (std::set<int64_t>{0, 1, 2, 3}));

    // Compound string-element predicate with two `$` references.
    // MATCH_ANY(tags, $ == "x" || $ == "y"): row0 ("x","y") matches;
    // row1 ("z") no; row2 [] no; row3 ("x") matches. -> {0, 3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(tags, $ == "x" || $ == "y"))"),
              (std::set<int64_t>{0, 3}));
}

// Nullable arrays: a scalar array (`tags`, referenced via `$`) and a struct
// array sub-field (`st[val]`, referenced via `$[val]`) must behave IDENTICALLY
// for NULL and empty rows.
//
// CURRENT BEHAVIOR (intentionally NOT yet Postgres-aligned): MatchExpr maps a
// row to its element range purely via the lengths table, and a NULL array row
// is written with length 0 -- so a NULL row is treated exactly like an empty
// array `[]`. Under this behavior MATCH_ALL / MATCH_MOST(0) / MATCH_EXACT(0)
// INCLUDE a NULL row (vacuous truth); MATCH_ANY / MATCH_LEAST(>=1) exclude it.
//
// TODO(scalar-array-null): a follow-up PR will align NULL semantics with
// PostgreSQL three-valued logic -- a NULL array yields UNKNOWN for every
// quantified comparison and is therefore excluded from ALL MATCH_* operators
// (distinct from an empty array, which stays vacuously true for MATCH_ALL).
// When that lands, drop the NULL row (index 1) from the MATCH_ALL / MATCH_MOST /
// MATCH_EXACT expected sets below. The scalar-vs-struct equality assertion holds
// regardless of which semantics is in effect.
TEST_P(ScalarArrayMatchExprTest, NullableArrayScalarStructAligned) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    // Nullable scalar VarChar array, and a nullable struct array sub-field
    // (struct name "st", sub-field "val").
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::VARCHAR, true);

    // Shared layout for both fields. row1 is NULL (valid=false); row3 is an
    // empty array (valid=true). row1 and row3 both have zero elements, so under
    // current behavior they are indistinguishable.
    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {}, {"z"}, {}, {"x"}};
    const std::vector<bool> row_valid = {true, false, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    // Build a string-array DataArray (with the shared null bitmap) for `fid`.
    // `data`/`valid` are consumed synchronously (copied into the proto), so it
    // is safe for them to go out of scope when this returns.
    auto make_str_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (const auto& v : rows[i]) {
                data[i].mutable_string_data()->add_data(v);
            }
        }
        FixedVector<bool> valid(N);
        for (int64_t i = 0; i < N; ++i) {
            valid[i] = row_valid[i];
        }
        return CreateDataArrayFrom(
            data.data(), valid.data(), N, schema->operator[](fid));
    };

    auto insert = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_array.release());
    insert->mutable_fields_data()->AddAllocated(
        make_str_array(tags_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_str_array(st_val_fid).release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    struct Case {
        std::string scalar_expr;  // over `tags` using `$`
        std::string struct_expr;  // over `st`   using `$[val]`
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        // Only rows containing "x" -> {0,4}. NULL(1) and empty(3) excluded.
        {R"(MATCH_ANY(tags, $ == "x"))",
         R"(MATCH_ANY(st, $[val] == "x"))",
         {0, 4}},
        // Every element != "": row0/2/4 have only non-empty elements; NULL(1)
        // and empty(3) are vacuously true under current behavior -> all rows.
        {R"(MATCH_ALL(tags, $ != ""))",
         R"(MATCH_ALL(st, $[val] != ""))",
         {0, 1, 2, 3, 4}},
        // At least one "x": {0,4}.
        {R"(MATCH_LEAST(tags, $ == "x", threshold=1))",
         R"(MATCH_LEAST(st, $[val] == "x", threshold=1))",
         {0, 4}},
        // At most zero "x" -> rows with no "x": {1,2,3} (NULL and empty count 0).
        {R"(MATCH_MOST(tags, $ == "x", threshold=0))",
         R"(MATCH_MOST(st, $[val] == "x", threshold=0))",
         {1, 2, 3}},
        // Exactly zero "x" -> {1,2,3}.
        {R"(MATCH_EXACT(tags, $ == "x", threshold=0))",
         R"(MATCH_EXACT(st, $[val] == "x", threshold=0))",
         {1, 2, 3}},
    };

    for (const auto& c : cases) {
        auto scalar_rows =
            RetrieveMatchedRows(seg.get(), *schema, schema, c.scalar_expr);
        auto struct_rows =
            RetrieveMatchedRows(seg.get(), *schema, schema, c.struct_expr);
        EXPECT_EQ(scalar_rows, c.expected)
            << "scalar expr result mismatch: " << c.scalar_expr;
        EXPECT_EQ(struct_rows, c.expected)
            << "struct expr result mismatch: " << c.struct_expr;
        // Core guarantee: scalar array and struct array agree on NULL/empty.
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence: " << c.scalar_expr << " | "
            << c.struct_expr;
    }
}

INSTANTIATE_TEST_SUITE_P(
    SegTypes,
    ScalarArrayMatchExprTest,
    ::testing::Values(ScalarArraySegType::kSealed,
                      ScalarArraySegType::kGrowing),
    [](const ::testing::TestParamInfo<ScalarArraySegType>& info) {
        return info.param == ScalarArraySegType::kSealed ? "Sealed" : "Growing";
    });

namespace milvus::segcore {
// Test-only accessor for the private FillDefaultValueFields() entry point.
// ApplyLoadDiff() normally calls it for diff.fields_to_fill_default during a
// schema-evolution reopen; this lets a unit test drive it directly without
// constructing a full SegmentLoadInfo proto.
class ScalarArrayFillDefaultTestAccess {
 public:
    static void
    FillDefaultValueFields(ChunkedSegmentSealedImpl* segment,
                           const std::vector<FieldId>& field_ids) {
        segment->FillDefaultValueFields(field_ids);
    }
};
}  // namespace milvus::segcore

// Regression: a nullable scalar ARRAY field added AFTER a segment was sealed is
// materialized through ChunkedSegmentSealedImpl::FillDefaultValueFields() ->
// fill_empty_field() for the pre-existing rows, NOT through the normal load
// path that builds array offsets. Before the fix, EnsureArrayOffsetsForStructField()
// early-returned for non-struct (scalar) arrays, so array_offsets_map_ never got
// an entry; MATCH_* then hit `AssertInfo(array_offsets != nullptr)` and threw.
//
// After the fix the fill path registers an all-zeros offsets table (every
// backfilled row is an empty array), so MATCH_* must run without throwing and
// give correct empty-array semantics: MATCH_ANY excludes every row, MATCH_ALL
// includes every row (vacuous truth). This mirrors the row3 `[]` behavior
// asserted in ScalarArrayMatchExprTest.Int64Array.
TEST(ScalarArrayFillDefaultAfterSeal, MatchTreatsBackfilledRowsAsEmptyArrays) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    // Fields added after creation must be nullable (proxy AddField constraint).
    auto scores_fid = schema->AddDebugArrayField(
        "scores", DataType::INT64, /*nullable=*/true);

    constexpr int64_t N = 4;

    // Build an insert carrying ONLY `id`. `scores` is absent from the binlog,
    // exactly as an old sealed segment whose data predates AddField(scores).
    auto insert = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_array.release());
    insert->set_num_rows(N);

    GeneratedData generated;
    generated.schema_ = schema;
    generated.raw_ = insert.release();
    for (int64_t i = 0; i < N; ++i) {
        generated.row_ids_.push_back(i);
        generated.timestamps_.push_back(i);
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, generated);

    auto* sealed_impl =
        dynamic_cast<milvus::segcore::ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed_impl, nullptr);

    // Sanity: before the fill, the scalar array has no offsets registered.
    ASSERT_EQ(sealed_impl->GetArrayOffsets(scores_fid), nullptr);

    // Drive the schema-evolution fill path for the newly added scalar array.
    milvus::segcore::ScalarArrayFillDefaultTestAccess::FillDefaultValueFields(
        sealed_impl, {scores_fid});

    // The fix must register an all-zeros offsets table for the scalar array.
    auto offsets = sealed_impl->GetArrayOffsets(scores_fid);
    ASSERT_NE(offsets, nullptr)
        << "scalar ARRAY field filled after seal must get array offsets";

    ScopedSchemaHandle schema_handle(*schema);
    auto run = [&](const std::string& expr) {
        auto plan_str = schema_handle.Parse(expr);
        auto plan =
            CreateRetrievePlanByExpr(schema, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);
        auto result = segment->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
        EXPECT_NE(result, nullptr);
        std::set<int64_t> rows;
        for (const auto& offset : result->offset()) {
            rows.insert(offset);
        }
        return rows;
    };

    // MATCH_ANY over empty arrays: no element can satisfy it -> no rows. The key
    // assertion is that this does NOT throw (the pre-fix missing-offsets assert).
    EXPECT_EQ(run("MATCH_ANY(scores, $ > 0)"), (std::set<int64_t>{}));

    // MATCH_ALL over empty arrays: vacuously true for every backfilled row.
    EXPECT_EQ(run("MATCH_ALL(scores, $ >= 60)"),
              (std::set<int64_t>{0, 1, 2, 3}));
}

// Nullable scalar array, two ingestion paths must agree.
//
// A growing segment can receive array data two ways, and MATCH_*/element_filter
// builds its per-row element-offset table differently for each:
//   - "new" data  -> realtime Insert      -> ExtractArrayLengths
//                                             (reads the dense insert proto)
//   - "old" data  -> binlog LoadFieldData  -> ExtractArrayLengthsFromFieldData
//                                             (reads in-memory FieldData, which
//                                              is COMPACT for nullable arrays:
//                                              a NULL row occupies no slot)
//
// The binlog-load path must therefore index the compact buffer by physical
// (valid-only) position, not by logical row index -- otherwise a NULL row that
// precedes valid rows shifts every later read and runs past the buffer end.
// This test loads a nullable Int64 array whose NULLs are at the front/interior
// (rows 0 and 2) through the binlog path, and builds a second growing segment
// with the identical layout through Insert, then asserts both produce the SAME,
// correct MATCH_* row sets.
//
// Behavior asserted is the *current* one (a NULL row is treated like an empty
// array, length 0 -- see ScalarArrayMatchExprTest.NullableArrayScalarStructAligned
// and the TODO(scalar-array-null) there); the point of this test is that the
// load path and the insert path agree and neither corrupts lengths.
TEST(ScalarArrayMatchNullableIngest, BinlogLoadAndInsertAgree) {
    // row0=NULL, row1=[95,80], row2=NULL, row3=[100,100,100], row4=[40]
    const std::vector<std::vector<int64_t>> rows = {
        {}, {95, 80}, {}, {100, 100, 100}, {40}};
    const std::vector<bool> row_valid = {false, true, false, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto make_schema = [&]() {
        auto schema = std::make_shared<Schema>();
        auto pk_fid = schema->AddDebugField("id", DataType::INT64);
        schema->set_primary_field_id(pk_fid);
        auto scores_fid =
            schema->AddDebugArrayField("scores", DataType::INT64, true);
        return std::make_tuple(schema, pk_fid, scores_fid);
    };

    // Parse `expr`, run a full-recall Retrieve, return matched segment offsets.
    auto retrieve_rows = [](SegmentInterface* seg,
                            const Schema& schema,
                            const SchemaPtr& schema_ptr,
                            const std::string& expr) {
        ScopedSchemaHandle schema_handle(schema);
        auto plan_str = schema_handle.Parse(expr);
        auto plan = CreateRetrievePlanByExpr(
            schema_ptr, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);
        auto result = seg->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
        EXPECT_NE(result, nullptr);
        std::set<int64_t> out;
        for (const auto& off : result->offset()) {
            out.insert(off);
        }
        return out;
    };

    // ---- "old" data: build a growing segment via the binlog LoadFieldData path.
    auto [load_schema, load_pk_fid, load_scores_fid] = make_schema();
    auto loaded = CreateGrowingSegment(load_schema, empty_index_meta);
    {
        // Build the same logical layout as a proto InsertRecord (pk + the
        // nullable scores array with its valid bitmap), then load it through the
        // canonical binlog path. LoadGeneratedDataIntoSegment bundles RowID +
        // Timestamp + pk + user fields into a SINGLE LoadFieldDataInfo (matching
        // SegmentGrowingImpl::Load, whose load_field_data_internal asserts the
        // system+pk fields are present together) and converts each field via
        // CreateFieldDataFromDataArray -- the SAME production conversion that
        // materializes a nullable scalar ARRAY FieldData COMPACTLY (a NULL row
        // occupies no slot in the underlying buffer). Hand-building the
        // FieldData with the dense 4-arg FillFieldData does NOT reproduce that
        // compact layout, so ExtractArrayLengthsFromFieldData (which indexes the
        // compact buffer by physical position) would read wrong element lengths.
        auto proto = std::make_unique<InsertRecordProto>();
        std::vector<int64_t> ids(N);
        std::iota(ids.begin(), ids.end(), 0);
        auto id_array = CreateDataArrayFrom(
            ids.data(), nullptr, N, load_schema->operator[](load_pk_fid));
        proto->mutable_fields_data()->AddAllocated(id_array.release());

        std::vector<milvus::proto::schema::ScalarField> scores(N);
        FixedVector<bool> valid(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : rows[i]) {
                scores[i].mutable_long_data()->add_data(v);
            }
            valid[i] = row_valid[i];
        }
        auto scores_array =
            CreateDataArrayFrom(scores.data(),
                                valid.data(),
                                N,
                                load_schema->operator[](load_scores_fid));
        proto->mutable_fields_data()->AddAllocated(scores_array.release());
        proto->set_num_rows(N);

        GeneratedData generated;
        generated.schema_ = load_schema;
        generated.raw_ = proto.release();
        for (int64_t i = 0; i < N; ++i) {
            generated.row_ids_.push_back(i);
            generated.timestamps_.push_back(i);
        }
        LoadGeneratedDataIntoSegment(generated, loaded.get());
    }

    // ---- "new" data: build a growing segment with the same layout via Insert.
    auto [ins_schema, ins_pk_fid, ins_scores_fid] = make_schema();
    auto inserted = CreateGrowingSegment(ins_schema, empty_index_meta);
    {
        auto insert = std::make_unique<InsertRecordProto>();
        std::vector<int64_t> ids(N);
        std::iota(ids.begin(), ids.end(), 0);
        auto id_array = CreateDataArrayFrom(
            ids.data(), nullptr, N, ins_schema->operator[](ins_pk_fid));
        insert->mutable_fields_data()->AddAllocated(id_array.release());

        std::vector<milvus::proto::schema::ScalarField> scores(N);
        FixedVector<bool> valid(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : rows[i]) {
                scores[i].mutable_long_data()->add_data(v);
            }
            valid[i] = row_valid[i];
        }
        auto scores_array = CreateDataArrayFrom(
            scores.data(), valid.data(), N, ins_schema->operator[](ins_scores_fid));
        insert->mutable_fields_data()->AddAllocated(scores_array.release());
        insert->set_num_rows(N);

        std::vector<idx_t> row_ids(N);
        std::vector<Timestamp> tss(N);
        for (int64_t i = 0; i < N; ++i) {
            row_ids[i] = i;
            tss[i] = i;
        }
        inserted->PreInsert(N);
        inserted->Insert(0, N, row_ids.data(), tss.data(), insert.get());
    }

    // Expected MATCH_* row sets under current behavior (NULL == empty, len 0):
    //   $ > 90   : row1(95) and row3(100,100,100)               -> {1, 3}
    //   $ >= 60  : row1 ok; row3 ok; NULL rows 0,2 vacuous true;
    //              row4 (40) fails                                -> {0, 1, 2, 3}
    //   $ < 50   : only row4 (40)                                 -> {4}
    //   == 100, threshold>=2 : only row3 (three 100s)            -> {3}
    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        {"MATCH_ANY(scores, $ > 90)", {1, 3}},
        {"MATCH_ALL(scores, $ >= 60)", {0, 1, 2, 3}},
        {"MATCH_ANY(scores, $ < 50)", {4}},
        {"MATCH_LEAST(scores, $ == 100, threshold=2)", {3}},
    };

    for (const auto& [expr, expected] : cases) {
        auto loaded_rows =
            retrieve_rows(loaded.get(), *load_schema, load_schema, expr);
        auto inserted_rows =
            retrieve_rows(inserted.get(), *ins_schema, ins_schema, expr);
        EXPECT_EQ(loaded_rows, expected)
            << "binlog-load path wrong for: " << expr;
        EXPECT_EQ(inserted_rows, expected)
            << "insert path wrong for: " << expr;
        // The two ingestion paths must agree on a nullable array.
        EXPECT_EQ(loaded_rows, inserted_rows)
            << "load vs insert divergence for: " << expr;
    }
}

namespace {

// Build a sealed segment with pk(INT64) + a scalar VARCHAR Array field `tags`
// holding `rows`. `valid` (if non-empty) marks per-row nullability. When
// `exclude_tags_raw` is true the array field's RAW data is NOT loaded (only the
// system fields + pk are), simulating an "index-only" load.
std::unique_ptr<SegmentSealed>
MakeSealedScalarVarCharArray(const SchemaPtr& schema,
                             FieldId pk_fid,
                             FieldId tags_fid,
                             const std::vector<std::vector<std::string>>& rows,
                             const std::vector<bool>& valid,
                             bool exclude_tags_raw) {
    const int64_t N = static_cast<int64_t>(rows.size());
    auto insert = std::make_unique<InsertRecordProto>();

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_arr =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_arr.release());

    std::vector<milvus::proto::schema::ScalarField> tags(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            tags[i].mutable_string_data()->add_data(v);
        }
    }
    FixedVector<bool> vb;
    const void* valid_ptr = nullptr;
    if (!valid.empty()) {
        vb.resize(N);
        for (int64_t i = 0; i < N; ++i) {
            vb[i] = valid[i];
        }
        valid_ptr = vb.data();
    }
    auto tags_arr = CreateDataArrayFrom(
        tags.data(), valid_ptr, N, schema->operator[](tags_fid));
    insert->mutable_fields_data()->AddAllocated(tags_arr.release());
    insert->set_num_rows(N);

    GeneratedData generated;
    generated.schema_ = schema;
    generated.raw_ = insert.release();
    for (int64_t i = 0; i < N; ++i) {
        generated.row_ids_.push_back(i);
        generated.timestamps_.push_back(i);
    }

    std::vector<int64_t> excluded;
    if (exclude_tags_raw) {
        excluded.push_back(tags_fid.get());
    }
    return CreateSealedWithFieldDataLoaded(schema, generated, false, excluded);
}

// Build + load a nested inverted index over the scalar VARCHAR array `tags_fid`.
void
LoadScalarVarCharArrayNestedIndex(
    SegmentSealed* seg,
    FieldId tags_fid,
    int64_t N,
    const std::vector<std::vector<std::string>>& rows) {
    std::vector<boost::container::vector<std::string>> arrays(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            arrays[i].push_back(v);
        }
    }
    auto index = std::make_unique<index::InvertedIndexTantivy<std::string>>();
    Config cfg;
    cfg["is_array"] = true;
    cfg["is_nested_index"] = true;
    index->BuildWithRawDataForUT(N, arrays.data(), cfg);
    LoadIndexInfo info{};
    info.field_id = tags_fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex("scalar_tags", std::move(index));
    seg->LoadIndex(info);
}

std::set<int64_t>
RetrieveMatchRowsLocal(SegmentInterface* seg,
                       const Schema& schema,
                       const SchemaPtr& schema_ptr,
                       const std::string& expr) {
    ScopedSchemaHandle handle(schema);
    auto plan_str = handle.Parse(expr);
    auto plan =
        CreateRetrievePlanByExpr(schema_ptr, plan_str.data(), plan_str.size());
    auto result = seg->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    std::set<int64_t> out;
    for (const auto& off : result->offset()) {
        out.insert(off);
    }
    return out;
}

}  // namespace

// Case A: scalar ARRAY + nested inverted index, non-nullable. MATCH must use the
// index (acceleration path) and still produce the correct row sets -- the index
// serves the element predicate, IArrayOffsets (built from raw) does the
// per-row quantifier counting.
TEST(ScalarArrayMatchIndex, NestedInvertedIndexAccelerates) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid =
        schema->AddDebugArrayField("tags", DataType::VARCHAR, false);

    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {"z"}, {"x"}, {}, {"y"}};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto seg = MakeSealedScalarVarCharArray(schema,
                                            pk_fid,
                                            tags_fid,
                                            rows,
                                            /*valid=*/{},
                                            /*exclude_tags_raw=*/false);
    LoadScalarVarCharArrayNestedIndex(seg.get(), tags_fid, N, rows);

    EXPECT_EQ(RetrieveMatchRowsLocal(
                  seg.get(), *schema, schema, R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0, 2}));
    // every element != "z": row1 ("z") fails; empty row3 vacuously true.
    EXPECT_EQ(RetrieveMatchRowsLocal(
                  seg.get(), *schema, schema, R"(MATCH_ALL(tags, $ != "z"))"),
              (std::set<int64_t>{0, 2, 3, 4}));
    EXPECT_EQ(
        RetrieveMatchRowsLocal(seg.get(),
                               *schema,
                               schema,
                               R"(MATCH_LEAST(tags, $ == "x", threshold=1))"),
        (std::set<int64_t>{0, 2}));
}

// Case B: scalar ARRAY + nested inverted index, NULLABLE. The index has no
// entries for NULL/empty rows; IArrayOffsets enumerates them (length 0), so the
// vacuous-truth quantifiers stay correct even on the index path.
TEST(ScalarArrayMatchIndex, NestedInvertedIndexNullable) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    // row1 = NULL, row3 = empty []
    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {}, {"z"}, {}, {"x"}};
    const std::vector<bool> valid = {true, false, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto seg = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);
    LoadScalarVarCharArrayNestedIndex(seg.get(), tags_fid, N, rows);

    EXPECT_EQ(RetrieveMatchRowsLocal(
                  seg.get(), *schema, schema, R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0, 4}));
    // every element != "" : NULL(1) and empty(3) are vacuously true (current
    // behavior), all valued rows have non-empty elements.
    EXPECT_EQ(RetrieveMatchRowsLocal(
                  seg.get(), *schema, schema, R"(MATCH_ALL(tags, $ != ""))"),
              (std::set<int64_t>{0, 1, 2, 3, 4}));
}

// Case C: "index-only" -- the array field's nested index is loaded but its RAW
// data is NOT, so no IArrayOffsets is built. MATCH cannot quantify without the
// offsets and currently raises (missing offsets), rather than silently dropping
// rows. This pins the known index-only limitation: an index alone cannot serve
// MATCH because it carries no row<->element mapping and no zero-element rows.
TEST(ScalarArrayMatchIndex, IndexOnlyWithoutRawHasNoOffsets) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {}, {"z"}, {}, {"x"}};
    const std::vector<bool> valid = {true, false, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    // Raw for `tags` is excluded -> load_field_data_common never runs for it ->
    // ArrayOffsetsSealed is never built; only the nested index is present.
    auto seg = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/true);
    LoadScalarVarCharArrayNestedIndex(seg.get(), tags_fid, N, rows);

    // GetArrayOffsets returns null -> MATCH raises instead of returning a wrong
    // (silently null-/empty-dropping) result.
    EXPECT_ANY_THROW({
        RetrieveMatchRowsLocal(
            seg.get(), *schema, schema, R"(MATCH_ANY(tags, $ == "x"))");
    });
}

namespace {

// Sealed segment with pk(INT64) + a scalar INT64 Array field holding `rows`
// (with per-row null `valid`), raw loaded.
std::unique_ptr<SegmentSealed>
MakeSealedScalarInt64Array(const SchemaPtr& schema,
                           FieldId pk_fid,
                           FieldId scores_fid,
                           const std::vector<std::vector<int64_t>>& rows,
                           const std::vector<bool>& valid) {
    const int64_t N = static_cast<int64_t>(rows.size());
    auto insert = std::make_unique<InsertRecordProto>();

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_arr =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_arr.release());

    std::vector<milvus::proto::schema::ScalarField> scores(N);
    for (int64_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            scores[i].mutable_long_data()->add_data(v);
        }
    }
    FixedVector<bool> vb;
    const void* valid_ptr = nullptr;
    if (!valid.empty()) {
        vb.resize(N);
        for (int64_t i = 0; i < N; ++i) {
            vb[i] = valid[i];
        }
        valid_ptr = vb.data();
    }
    auto scores_arr = CreateDataArrayFrom(
        scores.data(), valid_ptr, N, schema->operator[](scores_fid));
    insert->mutable_fields_data()->AddAllocated(scores_arr.release());
    insert->set_num_rows(N);

    GeneratedData generated;
    generated.schema_ = schema;
    generated.raw_ = insert.release();
    for (int64_t i = 0; i < N; ++i) {
        generated.row_ids_.push_back(i);
        generated.timestamps_.push_back(i);
    }
    return CreateSealedWithFieldDataLoaded(schema, generated, false, {});
}

void
LoadScalarInt64ArrayNestedIndex(SegmentSealed* seg,
                                FieldId scores_fid,
                                int64_t N,
                                const std::vector<std::vector<int64_t>>& rows) {
    std::vector<boost::container::vector<int64_t>> arrays(N);
    for (int64_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            arrays[i].push_back(v);
        }
    }
    auto index = std::make_unique<index::InvertedIndexTantivy<int64_t>>();
    Config cfg;
    cfg["is_array"] = true;
    cfg["is_nested_index"] = true;
    index->BuildWithRawDataForUT(N, arrays.data(), cfg);
    LoadIndexInfo info{};
    info.field_id = scores_fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index = CreateTestCacheIndex("scalar_scores", std::move(index));
    seg->LoadIndex(info);
}

}  // namespace

// Differential consistency: the SAME randomized data is loaded into two sealed
// segments -- one with a nested inverted index (DetermineExecPath commits to the
// ScalarIndex path, since a compatible index is present) and one without (the
// RawData / brute-force path). Every MATCH_* expression must produce the
// IDENTICAL row set on both. There are no hand-computed expected sets: the
// brute-force segment is the oracle for the indexed one, so any divergence
// between the index and brute-force code paths fails the test.
TEST(ScalarArrayMatchIndexConsistency, Int64IndexedEqualsBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);

    const int64_t N = 300;
    std::default_random_engine rng(20260625);
    std::uniform_int_distribution<int> len_dist(0, 5);   // 0 => empty arrays
    std::uniform_int_distribution<int> val_dist(0, 9);   // small value domain
    std::uniform_int_distribution<int> null_dist(0, 9);  // ~10% null rows

    std::vector<std::vector<int64_t>> rows(N);
    std::vector<bool> valid(N, true);
    for (int64_t i = 0; i < N; ++i) {
        if (null_dist(rng) == 0) {
            valid[i] = false;
            continue;  // null row: no elements
        }
        int len = len_dist(rng);
        for (int j = 0; j < len; ++j) {
            rows[i].push_back(val_dist(rng));
        }
    }

    auto indexed =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    LoadScalarInt64ArrayNestedIndex(indexed.get(), scores_fid, N, rows);
    auto brute =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);

    const std::vector<std::string> exprs = {
        "MATCH_ANY(scores, $ > 5)",
        "MATCH_ALL(scores, $ >= 0)",
        "MATCH_ALL(scores, $ < 5)",
        "MATCH_ANY(scores, $ == 7)",
        "MATCH_LEAST(scores, $ > 5, threshold=2)",
        "MATCH_MOST(scores, $ == 0, threshold=1)",
        "MATCH_EXACT(scores, $ == 9, threshold=0)",
        "MATCH_EXACT(scores, $ == 9, threshold=2)",
        "MATCH_ANY(scores, $ > 2 && $ < 8)",
        "MATCH_ALL(scores, $ >= 0 && $ <= 9)",
        "MATCH_ANY(scores, 2 < $ < 8)",
        "MATCH_ANY(scores, $ == 1 || $ == 2)",
    };
    for (const auto& e : exprs) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, e);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, e);
        EXPECT_EQ(ri, rb) << "index vs brute-force mismatch for: " << e;
    }
}

TEST(ScalarArrayMatchIndexConsistency, VarCharIndexedEqualsBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    const int64_t N = 300;
    const std::vector<std::string> vocab = {"a", "b", "c", "d", "e"};
    std::default_random_engine rng(7777);
    std::uniform_int_distribution<int> len_dist(0, 5);
    std::uniform_int_distribution<int> val_dist(0, 4);
    std::uniform_int_distribution<int> null_dist(0, 9);

    std::vector<std::vector<std::string>> rows(N);
    std::vector<bool> valid(N, true);
    for (int64_t i = 0; i < N; ++i) {
        if (null_dist(rng) == 0) {
            valid[i] = false;
            continue;
        }
        int len = len_dist(rng);
        for (int j = 0; j < len; ++j) {
            rows[i].push_back(vocab[val_dist(rng)]);
        }
    }

    auto indexed = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);
    LoadScalarVarCharArrayNestedIndex(indexed.get(), tags_fid, N, rows);
    auto brute = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(tags, $ == "a"))",
        R"(MATCH_ALL(tags, $ == "a"))",
        R"(MATCH_ALL(tags, $ != "e"))",
        R"(MATCH_LEAST(tags, $ == "a", threshold=2))",
        R"(MATCH_MOST(tags, $ == "b", threshold=1))",
        R"(MATCH_EXACT(tags, $ == "c", threshold=0))",
        R"(MATCH_EXACT(tags, $ == "c", threshold=2))",
        R"(MATCH_ANY(tags, $ == "a" || $ == "b"))",
    };
    for (const auto& e : exprs) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, e);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, e);
        EXPECT_EQ(ri, rb) << "index vs brute-force mismatch for: " << e;
    }
}

namespace {
// Build + load a NON-nested inverted index over a scalar VarChar array. The
// `is_nested_index` key is deliberately omitted (BuildWithRawDataForUT treats
// the key's presence, not its value, as the nested flag), so this produces a
// legacy-style non-nested array index (IsNestedIndex()==false).
void
LoadScalarVarCharArrayNonNestedIndex(
    SegmentSealed* seg,
    FieldId tags_fid,
    int64_t N,
    const std::vector<std::vector<std::string>>& rows) {
    std::vector<boost::container::vector<std::string>> arrays(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            arrays[i].push_back(v);
        }
    }
    auto index = std::make_unique<index::InvertedIndexTantivy<std::string>>();
    Config cfg;
    cfg["is_array"] = true;  // NOTE: no "is_nested_index" key -> non-nested
    index->BuildWithRawDataForUT(N, arrays.data(), cfg);
    LoadIndexInfo info{};
    info.field_id = tags_fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index =
        CreateTestCacheIndex("scalar_tags_nonnested", std::move(index));
    seg->LoadIndex(info);
}
}  // namespace

// A non-nested (legacy) array index must NOT corrupt MATCH_*: the element-level
// child detects the index is not nested and falls back to brute force, so the
// result equals a segment with no index. (Without the exec-path guard, the
// element-level predicate would feed a row-level non-nested index result into
// element-level logic and could return wrong rows.)
TEST(ScalarArrayMatchIndexConsistency, NonNestedIndexFallsBackToBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    const int64_t N = 300;
    const std::vector<std::string> vocab = {"a", "b", "c", "d", "e"};
    std::default_random_engine rng(31337);
    std::uniform_int_distribution<int> len_dist(0, 5);
    std::uniform_int_distribution<int> val_dist(0, 4);
    std::uniform_int_distribution<int> null_dist(0, 9);

    std::vector<std::vector<std::string>> rows(N);
    std::vector<bool> valid(N, true);
    for (int64_t i = 0; i < N; ++i) {
        if (null_dist(rng) == 0) {
            valid[i] = false;
            continue;
        }
        int len = len_dist(rng);
        for (int j = 0; j < len; ++j) {
            rows[i].push_back(vocab[val_dist(rng)]);
        }
    }

    // Segment with a legacy non-nested array index, plus a brute-force oracle.
    auto with_legacy_index = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);
    LoadScalarVarCharArrayNonNestedIndex(
        with_legacy_index.get(), tags_fid, N, rows);
    auto brute = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(tags, $ == "a"))",
        R"(MATCH_ALL(tags, $ != "e"))",
        R"(MATCH_LEAST(tags, $ == "a", threshold=2))",
        R"(MATCH_MOST(tags, $ == "b", threshold=1))",
        R"(MATCH_EXACT(tags, $ == "c", threshold=0))",
        R"(MATCH_ANY(tags, $ == "a" || $ == "b"))",
    };
    for (const auto& e : exprs) {
        auto ri =
            RetrieveMatchRowsLocal(with_legacy_index.get(), *schema, schema, e);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, e);
        EXPECT_EQ(ri, rb)
            << "non-nested index must fall back to brute force for: " << e;
    }
}
