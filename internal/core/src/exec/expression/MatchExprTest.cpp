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
#include "index/BitmapIndex.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
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
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/Util.h"
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

// Scalar-array counterpart of JsonElementEmptyInNotVacuous: element-level
// `$ in []` on a typed array runs through the typed SetElement exec path
// (distinct from the JSON element extraction), and must likewise yield a
// definite FALSE per element -- MATCH_ALL over a NON-empty row must not
// degenerate into a vacuous true. Unlike JSON, the parser accepts a LONE
// `$ in []` here (the >=1-typed-literal requirement is JSON-only), so both
// the lone and the compound shapes are asserted.
TEST_P(ScalarArrayMatchExprTest, EmptyInElementPredicate) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, false);

    // row0: non-empty array; row1: empty array.
    std::vector<std::vector<int64_t>> rows = {{1, 2, 3}, {}};
    auto insert = BuildInt64ArrayInsert(*schema, pk_fid, scores_fid, rows);
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ALL($ > 1 && $ in []): every element of row0 is a definite false
    // -> row0 excluded; row1 [] vacuously true -> {1}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, "MATCH_ALL(scores, $ > 1 && $ in [])"),
        (std::set<int64_t>{1}));

    // MATCH_ANY($ > 1 && $ in []): nothing can satisfy `$ in []` -> no rows.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, "MATCH_ANY(scores, $ > 1 && $ in [])"),
        (std::set<int64_t>{}));

    // Lone empty-IN forms (parseable on typed arrays).
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, "MATCH_ALL(scores, $ in [])"),
              (std::set<int64_t>{1}));
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, "MATCH_ANY(scores, $ in [])"),
              (std::set<int64_t>{}));

    // Control: `$ not in []` is a definite TRUE per element, so the compound
    // reduces to `$ >= 1`, which every element of row0 satisfies -> {0,1}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  "MATCH_ALL(scores, $ >= 1 && $ not in [])"),
              (std::set<int64_t>{0, 1}));
}

// Nullable arrays: a scalar array (`tags`, referenced via `$`) and a struct
// array sub-field (`st[val]`, referenced via `$[val]`) must produce IDENTICAL
// row sets as each other on the same NULL/empty layout (scalar-vs-struct
// alignment). NULL and empty rows themselves are NOT equivalent -- see below.
//
// THREE-VALUED SEMANTICS (PostgreSQL-aligned): a NULL array row yields UNKNOWN
// for every quantified comparison and is therefore EXCLUDED from ALL MATCH_*
// operators (a filter keeps only `true`; both `false` and UNKNOWN are dropped).
// This is distinct from a genuine empty array `[]`, which is a real (non-null)
// array and keeps vacuous-truth semantics: MATCH_ALL / MATCH_MOST(0) /
// MATCH_EXACT(0) INCLUDE an empty row, while MATCH_ANY / MATCH_LEAST(>=1)
// exclude it. row1 (NULL) is therefore excluded everywhere below; row3 (`[]`)
// still participates vacuously. The scalar-vs-struct equality assertion holds
// under this aligned behavior.
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
        // Every element != "": row0/2/4 have only non-empty elements; empty(3)
        // is vacuously true; NULL(1) is excluded (three-valued) -> {0,2,3,4}.
        {R"(MATCH_ALL(tags, $ != ""))",
         R"(MATCH_ALL(st, $[val] != ""))",
         {0, 2, 3, 4}},
        // At least one "x": {0,4}.
        {R"(MATCH_LEAST(tags, $ == "x", threshold=1))",
         R"(MATCH_LEAST(st, $[val] == "x", threshold=1))",
         {0, 4}},
        // At most zero "x" -> non-null rows with no "x": empty(3), row2 -> {2,3}.
        // NULL(1) excluded (three-valued, not a vacuous match).
        {R"(MATCH_MOST(tags, $ == "x", threshold=0))",
         R"(MATCH_MOST(st, $[val] == "x", threshold=0))",
         {2, 3}},
        // Exactly zero "x" -> {2,3}. NULL(1) excluded.
        {R"(MATCH_EXACT(tags, $ == "x", threshold=0))",
         R"(MATCH_EXACT(st, $[val] == "x", threshold=0))",
         {2, 3}},
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

// Three-valued MATCH under negation. `not MATCH_*` must propagate UNKNOWN:
// a NULL-array row is UNKNOWN for the MATCH, stays UNKNOWN after NOT
// (ThreeValuedLogicOp::Not flips only definite values), and is EXCLUDED by
// the filter -- while an empty [] row is a definite false/true that NOT
// inverts normally. A lost valid bit anywhere in the chain would silently
// flip NULL rows into `not MATCH` results; this is the execution-level lock
// on that behavior for both the scalar-array and struct-array paths.
TEST_P(ScalarArrayMatchExprTest, NullableNotMatchThreeValued) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::VARCHAR, true);

    // Same layout as NullableArrayScalarStructAligned:
    // row0 {"x","y"}, row1 NULL, row2 {"z"}, row3 [] (empty), row4 {"x"}.
    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {}, {"z"}, {}, {"x"}};
    const std::vector<bool> row_valid = {true, false, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

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
        std::string scalar_expr;
        std::string struct_expr;
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        // MATCH_ANY($=="x"): T{0,4} F{2,3} U{1}. NOT keeps definite-false
        // rows {2,3}; NULL(1) stays UNKNOWN and is excluded; empty(3) is a
        // definite false that NOT flips to true.
        {R"(not MATCH_ANY(tags, $ == "x"))",
         R"(not MATCH_ANY(st, $[val] == "x"))",
         {2, 3}},
        // MATCH_MOST(threshold=0, $=="x"): T{2,3} F{0,4} U{1}.
        // NOT -> {0,4}; NULL(1) excluded.
        {R"(not MATCH_MOST(tags, $ == "x", threshold=0))",
         R"(not MATCH_MOST(st, $[val] == "x", threshold=0))",
         {0, 4}},
        // MATCH_ALL($!=""): T{0,2,3,4} (empty vacuous) U{1}.
        // NOT -> nothing definite-false remains; NULL(1) must NOT surface.
        {R"(not MATCH_ALL(tags, $ != ""))",
         R"(not MATCH_ALL(st, $[val] != ""))",
         {}},
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
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence under NOT: " << c.scalar_expr;
    }
}

// LIKE / pattern predicates inside MATCH_* on VARCHAR elements, for BOTH the
// scalar-array (`tags`, bare `$`) and struct-array sub-field (`st[val]`,
// `$[val]`) containers. `like` lowers per wildcard shape to PrefixMatch
// ("ab%"), PostfixMatch ("%bc"), InnerMatch ("%b%") or the general Match op
// ("a%c"); all four must run through the element-level UnaryExpr path with the
// usual quantifier folding and three-valued NULL semantics (NULL row excluded,
// [] row vacuous under MATCH_ALL).
TEST_P(ScalarArrayMatchExprTest, VarCharArrayLikePatterns) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::VARCHAR, true);

    // row1 is NULL; row3 is a genuine empty array.
    const std::vector<std::vector<std::string>> rows = {
        {"abc", "abd"}, {}, {"xyz"}, {}, {"abc", "xyz"}, {"cab"}};
    const std::vector<bool> row_valid = {true, false, true, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

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
        std::string scalar_expr;
        std::string struct_expr;
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        // prefix: elements starting "ab" -> row0 (abc,abd), row4 (abc).
        {R"(MATCH_ANY(tags, $ like "ab%"))",
         R"(MATCH_ANY(st, $[val] like "ab%"))",
         {0, 4}},
        // prefix ALL: row0 all-prefix; empty row3 vacuous; NULL row1 excluded.
        {R"(MATCH_ALL(tags, $ like "ab%"))",
         R"(MATCH_ALL(st, $[val] like "ab%"))",
         {0, 3}},
        // suffix: elements ending "bc" -> "abc" in row0, row4.
        {R"(MATCH_ANY(tags, $ like "%bc"))",
         R"(MATCH_ANY(st, $[val] like "%bc"))",
         {0, 4}},
        // infix: elements containing "b" -> row0, row4 ("abc"), row5 ("cab").
        {R"(MATCH_ANY(tags, $ like "%b%"))",
         R"(MATCH_ANY(st, $[val] like "%b%"))",
         {0, 4, 5}},
        // infix ALL: row0 (abc,abd), row3 vacuous, row5 (cab); row4 has "xyz".
        {R"(MATCH_ALL(tags, $ like "%b%"))",
         R"(MATCH_ALL(st, $[val] like "%b%"))",
         {0, 3, 5}},
        // general pattern (Match op): starts "a" AND ends "c" -> only "abc".
        {R"(MATCH_ANY(tags, $ like "a%c"))",
         R"(MATCH_ANY(st, $[val] like "a%c"))",
         {0, 4}},
        // quantified LIKE: >=2 prefix-matching elements -> only row0.
        {R"(MATCH_LEAST(tags, $ like "ab%", threshold=2))",
         R"(MATCH_LEAST(st, $[val] like "ab%", threshold=2))",
         {0}},
        // NOT over a pattern MATCH keeps three-valued semantics: T{0,4}
        // F{2,3,5} U{1} -> NOT keeps the definite-false rows only.
        {R"(not MATCH_ANY(tags, $ like "ab%"))",
         R"(not MATCH_ANY(st, $[val] like "ab%"))",
         {2, 3, 5}},
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
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence: " << c.scalar_expr;
    }
}

// Non-empty term IN / NOT IN element predicates on INT64 elements, again for
// both the scalar-array and struct-array containers with a shared NULL/empty
// layout. Only the EMPTY in-list was execution-tested before; this pins the
// populated TermElement path (SetElement) plus NOT-of-MATCH over it.
TEST_P(ScalarArrayMatchExprTest, TermInNotInElementPredicate) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::INT64, true);

    // row2 is a genuine empty array; row4 is NULL.
    const std::vector<std::vector<int64_t>> rows = {
        {1, 2, 3}, {5}, {}, {2, 2}, {}};
    const std::vector<bool> row_valid = {true, true, true, true, false};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto make_int_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : rows[i]) {
                data[i].mutable_long_data()->add_data(v);
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
        make_int_array(scores_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_int_array(st_val_fid).release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    struct Case {
        std::string scalar_expr;
        std::string struct_expr;
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        // any element in {2,5}: row0(2), row1(5), row3(2,2).
        {"MATCH_ANY(scores, $ in [2, 5])",
         "MATCH_ANY(st, $[val] in [2, 5])",
         {0, 1, 3}},
        // every element in {2,5}: row1{5}, row2[] vacuous, row3{2,2}.
        {"MATCH_ALL(scores, $ in [2, 5])",
         "MATCH_ALL(st, $[val] in [2, 5])",
         {1, 2, 3}},
        // >=2 elements in {2,5}: only row3 (two 2s); row0 has just one.
        {"MATCH_LEAST(scores, $ in [2, 5], threshold=2)",
         "MATCH_LEAST(st, $[val] in [2, 5], threshold=2)",
         {3}},
        // any element != 2 (NOT IN): row0(1,3), row1(5); row3 all-2s fails;
        // empty row2 can never satisfy ANY.
        {"MATCH_ANY(scores, $ not in [2])",
         "MATCH_ANY(st, $[val] not in [2])",
         {0, 1}},
        // no element equals 2: row1, row2 (vacuous).
        {"MATCH_ALL(scores, $ not in [2])",
         "MATCH_ALL(st, $[val] not in [2])",
         {1, 2}},
        // NOT over term MATCH, three-valued: MATCH_ANY($ in [2,5]) is
        // T{0,1,3} F{2} U{4} -> NOT keeps {2}; NULL row4 must not surface.
        {"not MATCH_ANY(scores, $ in [2, 5])",
         "not MATCH_ANY(st, $[val] in [2, 5])",
         {2}},
        // NOT over NOT-IN MATCH_ALL: T{1,2} F{0,3} U{4} -> {0,3}.
        {"not MATCH_ALL(scores, $ not in [2])",
         "not MATCH_ALL(st, $[val] not in [2])",
         {0, 3}},
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
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence: " << c.scalar_expr;
    }
}

// Struct sub-field inner-predicate variety beyond plain comparisons: ternary
// range (BinaryRangeExpr over the element), modulo arithmetic
// (BinaryArithOpEvalRangeExpr over the element), addition arithmetic, and
// NOT-of-MATCH three-valued behavior over those predicate shapes.
TEST_P(ScalarArrayMatchExprTest, StructSubFieldPredicateVariety) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::INT64, true);

    // row2 is NULL; row3 is a genuine empty array.
    const std::vector<std::vector<int64_t>> rows = {
        {1, 2, 3}, {4, 6}, {}, {}, {7}};
    const std::vector<bool> row_valid = {true, true, false, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto insert = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_array.release());
    {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : rows[i]) {
                data[i].mutable_long_data()->add_data(v);
            }
        }
        FixedVector<bool> valid(N);
        for (int64_t i = 0; i < N; ++i) {
            valid[i] = row_valid[i];
        }
        auto arr = CreateDataArrayFrom(
            data.data(), valid.data(), N, schema->operator[](st_val_fid));
        insert->mutable_fields_data()->AddAllocated(arr.release());
    }
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // Ternary range 1 < $[val] < 5: row0 (2,3), row1 (4).
        {"MATCH_ANY(st, 1 < $[val] < 5)", {0, 1}},
        // ALL in (1,5): row0 has 1, row1 has 6 -> both fail; empty row3
        // vacuous; NULL row2 excluded.
        {"MATCH_ALL(st, 1 < $[val] < 5)", {3}},
        // Modulo arith: even elements -> row0 (2), row1 (4,6).
        {"MATCH_ANY(st, $[val] % 2 == 0)", {0, 1}},
        {"MATCH_ALL(st, $[val] % 2 == 0)", {1, 3}},
        {"MATCH_LEAST(st, $[val] % 2 == 0, threshold=2)", {1}},
        // Addition arith: $+1 == 8 -> only row4 (7).
        {"MATCH_ANY(st, $[val] + 1 == 8)", {4}},
        // NOT over arith MATCH, three-valued: MATCH_ANY(% 2 == 0) is T{0,1}
        // F{3,4} U{2} -> {3,4}; the empty row inverts as a definite value
        // while NULL row2 stays excluded.
        {"not MATCH_ANY(st, $[val] % 2 == 0)", {3, 4}},
        // NOT over MATCH_ALL(% 2 == 0): T{1,3} F{0,4} U{2} -> {0,4}.
        {"not MATCH_ALL(st, $[val] % 2 == 0)", {0, 4}},
    };
    for (const auto& [expr, expected] : cases) {
        EXPECT_EQ(RetrieveMatchedRows(seg.get(), *schema, schema, expr),
                  expected)
            << "struct predicate-variety mismatch: " << expr;
    }
}

// BOOL element arrays through MATCH_* on both the scalar-array and struct
// sub-field containers (only JSON bool elements were execution-tested).
TEST_P(ScalarArrayMatchExprTest, BoolArray) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto flags_fid = schema->AddDebugArrayField("flags", DataType::BOOL, true);
    auto st_flag_fid =
        schema->AddDebugArrayField("st[flag]", DataType::BOOL, true);

    // row2 is NULL; row3 is a genuine empty array.
    const std::vector<std::vector<bool>> rows = {
        {true, false}, {false}, {}, {}, {true, true}};
    const std::vector<bool> row_valid = {true, true, false, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto make_bool_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (bool v : rows[i]) {
                data[i].mutable_bool_data()->add_data(v);
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
        make_bool_array(flags_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_bool_array(st_flag_fid).release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    struct Case {
        std::string scalar_expr;
        std::string struct_expr;
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        {"MATCH_ANY(flags, $ == true)",
         "MATCH_ANY(st, $[flag] == true)",
         {0, 4}},
        // all true: empty row3 vacuous, row4; NULL row2 excluded.
        {"MATCH_ALL(flags, $ == true)",
         "MATCH_ALL(st, $[flag] == true)",
         {3, 4}},
        {"MATCH_ANY(flags, $ == false)",
         "MATCH_ANY(st, $[flag] == false)",
         {0, 1}},
        {"MATCH_ALL(flags, $ != false)",
         "MATCH_ALL(st, $[flag] != false)",
         {3, 4}},
        // exactly two true elements -> only row4.
        {"MATCH_EXACT(flags, $ == true, threshold=2)",
         "MATCH_EXACT(st, $[flag] == true, threshold=2)",
         {4}},
        // zero true elements among non-null rows: row1, empty row3.
        {"MATCH_MOST(flags, $ == true, threshold=0)",
         "MATCH_MOST(st, $[flag] == true, threshold=0)",
         {1, 3}},
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
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence: " << c.scalar_expr;
    }
}

// FLOAT and DOUBLE element arrays through MATCH_*, on the scalar-array
// (both element widths) and struct sub-field (FLOAT) containers. All literal
// values used are exactly representable in binary floating point, so the
// expected sets are exact.
TEST_P(ScalarArrayMatchExprTest, FloatDoubleArray) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto fvals_fid = schema->AddDebugArrayField("fvals", DataType::FLOAT, true);
    auto dvals_fid =
        schema->AddDebugArrayField("dvals", DataType::DOUBLE, true);
    auto st_fv_fid =
        schema->AddDebugArrayField("st[fv]", DataType::FLOAT, true);

    // row2 is NULL; row3 is a genuine empty array.
    const std::vector<std::vector<double>> rows = {
        {1.0, 2.5}, {0.5}, {}, {}, {2.0, 3.5}};
    const std::vector<bool> row_valid = {true, true, false, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto make_float_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (double v : rows[i]) {
                data[i].mutable_float_data()->add_data(static_cast<float>(v));
            }
        }
        FixedVector<bool> valid(N);
        for (int64_t i = 0; i < N; ++i) {
            valid[i] = row_valid[i];
        }
        return CreateDataArrayFrom(
            data.data(), valid.data(), N, schema->operator[](fid));
    };
    auto make_double_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (double v : rows[i]) {
                data[i].mutable_double_data()->add_data(v);
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
        make_float_array(fvals_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_double_array(dvals_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_float_array(st_fv_fid).release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    // (field name, element accessor) pairs for the three containers; every
    // case template is instantiated against each and must agree.
    const std::vector<std::pair<std::string, std::string>> containers = {
        {"fvals", "$"}, {"dvals", "$"}, {"st", "$[fv]"}};
    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // any element > 1.5: row0 (2.5), row4 (2.0, 3.5).
        {"MATCH_ANY({F}, {E} > 1.5)", {0, 4}},
        // all elements > 1.5: empty row3 vacuous, row4; NULL row2 excluded.
        {"MATCH_ALL({F}, {E} > 1.5)", {3, 4}},
        // >=2 elements > 1.5: only row4.
        {"MATCH_LEAST({F}, {E} > 1.5, threshold=2)", {4}},
        // all elements >= 0.5: rows 0,1,4; empty row3 vacuous.
        {"MATCH_ALL({F}, {E} >= 0.5)", {0, 1, 3, 4}},
        // ternary range over float elements: 0.25 < $ < 1.25 -> row0 (1.0),
        // row1 (0.5).
        {"MATCH_ANY({F}, 0.25 < {E} < 1.25)", {0, 1}},
    };

    auto instantiate = [](std::string tmpl,
                          const std::string& field,
                          const std::string& elem) {
        auto replace_all =
            [](std::string& s, const std::string& from, const std::string& to) {
                for (size_t pos = s.find(from); pos != std::string::npos;
                     pos = s.find(from, pos + to.size())) {
                    s.replace(pos, from.size(), to);
                }
            };
        replace_all(tmpl, "{F}", field);
        replace_all(tmpl, "{E}", elem);
        return tmpl;
    };

    for (const auto& [tmpl, expected] : cases) {
        std::set<std::set<int64_t>> distinct_results;
        for (const auto& [field, elem] : containers) {
            auto expr = instantiate(tmpl, field, elem);
            auto rows_out =
                RetrieveMatchedRows(seg.get(), *schema, schema, expr);
            EXPECT_EQ(rows_out, expected)
                << "float/double MATCH mismatch: " << expr;
            distinct_results.insert(rows_out);
        }
        EXPECT_EQ(distinct_results.size(), 1u)
            << "float/double/struct containers diverged for: " << tmpl;
    }
}

// An ALL-NULL array segment (every row's array is NULL) must yield ZERO rows
// for every quantifier: each row is UNKNOWN under three-valued MATCH and a
// filter keeps only definite trues. Covers scalar-array and struct sub-field
// containers, sealed and growing via the suite param.
TEST_P(ScalarArrayMatchExprTest, AllNullRowsMatchNothing) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::INT64, true);

    const int64_t N = 4;
    const std::vector<std::vector<int64_t>> rows(N);  // all zero-element
    const std::vector<bool> row_valid(N, false);      // every row NULL

    auto make_int_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
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
        make_int_array(scores_fid).release());
    insert->mutable_fields_data()->AddAllocated(
        make_int_array(st_val_fid).release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    const std::vector<std::string> exprs = {
        "MATCH_ANY(scores, $ >= 0)",
        "MATCH_ALL(scores, $ >= 0)",
        "MATCH_MOST(scores, $ >= 0, threshold=5)",
        "MATCH_EXACT(scores, $ >= 0, threshold=0)",
        "MATCH_ANY(st, $[val] >= 0)",
        "MATCH_ALL(st, $[val] >= 0)",
    };
    for (const auto& e : exprs) {
        EXPECT_EQ(RetrieveMatchedRows(seg.get(), *schema, schema, e),
                  (std::set<int64_t>{}))
            << "all-NULL segment must match nothing: " << e;
    }
}

// OR / AND composition of two independent MATCH_* expressions over two
// different array fields, exercising the ConjunctExpr folding of two
// quantified children.
TEST_P(ScalarArrayMatchExprTest, OrOfTwoMatchExprs) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, false);
    auto tags_fid =
        schema->AddDebugArrayField("tags", DataType::VARCHAR, false);

    const std::vector<std::vector<int64_t>> score_rows = {
        {95}, {40}, {}, {100}};
    const std::vector<std::vector<std::string>> tag_rows = {
        {"x"}, {"y"}, {"x"}, {}};
    const int64_t N = static_cast<int64_t>(score_rows.size());

    auto insert = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array =
        CreateDataArrayFrom(ids.data(), nullptr, N, schema->operator[](pk_fid));
    insert->mutable_fields_data()->AddAllocated(id_array.release());
    {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (auto v : score_rows[i]) {
                data[i].mutable_long_data()->add_data(v);
            }
        }
        auto arr = CreateDataArrayFrom(
            data.data(), nullptr, N, schema->operator[](scores_fid));
        insert->mutable_fields_data()->AddAllocated(arr.release());
    }
    {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (const auto& v : tag_rows[i]) {
                data[i].mutable_string_data()->add_data(v);
            }
        }
        auto arr = CreateDataArrayFrom(
            data.data(), nullptr, N, schema->operator[](tags_fid));
        insert->mutable_fields_data()->AddAllocated(arr.release());
    }
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(scores,$>90) -> {0,3}; MATCH_ANY(tags,$=="x") -> {0,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(scores, $ > 90) || )"
                                  R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0, 2, 3}));
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(scores, $ > 90) && )"
                                  R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0}));
    // MATCH_ALL(scores,$>90) -> {0,2,3} ([] vacuous); OR {0,2} -> {0,2,3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(scores, $ > 90) || )"
                                  R"(MATCH_ANY(tags, $ == "x"))"),
              (std::set<int64_t>{0, 2, 3}));
}

INSTANTIATE_TEST_SUITE_P(
    SegTypes,
    ScalarArrayMatchExprTest,
    ::testing::Values(ScalarArraySegType::kSealed,
                      ScalarArraySegType::kGrowing),
    [](const ::testing::TestParamInfo<ScalarArraySegType>& info) {
        return info.param == ScalarArraySegType::kSealed ? "Sealed" : "Growing";
    });

// ---------------------------------------------------------------------------
// JSON-array MATCH_* (brute-force EvalJson path).
//
// The MATCH_* target is a JSON field plus a nested path that resolves to an
// array-of-scalars leaf (e.g. json["arr"]); the predicate uses bare `$` for the
// scalar element. EvalJson -> EvalJsonBrute walks the JSON array per row,
// builds a per-element match bitmap, and applies the quantifier with the SAME
// empty/vacuous semantics as scalar/struct arrays (MatchSingleRow /
// MatchEmptyElements):
//   - MATCH_ANY     : >=1 matching element; empty -> false
//   - MATCH_ALL     : every element matches; empty -> true (vacuous)
//   - MATCH_LEAST(N): >=N matches;            empty -> N<=0
//   - MATCH_MOST(N) : <=N matches;            empty -> N>=0
//   - MATCH_EXACT(N): ==N matches;            empty -> N==0
//
// MATCH is three-valued: a row is kept only when the path resolves to a REAL
// JSON array (even an empty `[]`, which stays vacuously true/false per the
// quantifier above). A NULL JSON row, a missing key, a JSON `null`, or a
// non-array value at the path all yield UNKNOWN and are EXCLUDED
// (MaskJsonNonArrayRows clears both the valid and match bits) -- they are NOT
// treated like an empty array. An empty `[]` is a real array and is kept.
// This matches the scalar/struct-array NULL behavior (MaskNullRows). See the
// JsonPathNotArrayExcluded and NullableJsonArrayAtPath tests below.
class JsonArrayMatchExprTest
    : public ::testing::TestWithParam<ScalarArraySegType> {
 protected:
    // Build an insert proto: pk (INT64) + a JSON field holding the given raw
    // JSON document strings, one per row. `valid` (optional, same length as
    // `json_rows`) supplies the null bitmap for the nullable variant; pass
    // empty for an all-valid field.
    std::unique_ptr<InsertRecordProto>
    BuildJsonInsert(const Schema& schema,
                    FieldId pk_fid,
                    FieldId json_fid,
                    const std::vector<std::string>& json_rows,
                    const std::vector<bool>& valid) {
        auto insert_data = std::make_unique<InsertRecordProto>();
        const int64_t N = static_cast<int64_t>(json_rows.size());

        std::vector<int64_t> ids(N);
        std::iota(ids.begin(), ids.end(), 0);
        auto id_array =
            CreateDataArrayFrom(ids.data(), nullptr, N, schema[pk_fid]);
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        // CreateDataArrayFrom dispatches DataType::JSON to a const std::string*
        // backing buffer (the raw JSON document text per row).
        const bool* valid_ptr = nullptr;
        FixedVector<bool> valid_storage;
        if (!valid.empty()) {
            valid_storage.resize(N);
            for (int64_t i = 0; i < N; ++i) {
                valid_storage[i] = valid[i];
            }
            valid_ptr = valid_storage.data();
        }
        auto json_array = CreateDataArrayFrom(
            json_rows.data(), valid_ptr, N, schema[json_fid]);
        insert_data->mutable_fields_data()->AddAllocated(json_array.release());

        insert_data->set_num_rows(N);
        return insert_data;
    }

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

TEST_P(JsonArrayMatchExprTest, IntArrayAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: [95,80]  row1: [40]  row2: [100,100,100]  row3: [] (empty array)
    std::vector<std::string> rows = {
        R"({"arr":[95,80]})",
        R"({"arr":[40]})",
        R"({"arr":[100,100,100]})",
        R"({"arr":[]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(json["arr"], $ > 90): row0(95) & row2(100) -> {0,2}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ > 90))"),
        (std::set<int64_t>{0, 2}));

    // MATCH_ALL(json["arr"], $ >= 60): row0(95,80) & row2(100s) true;
    // row1(40) false; row3 [] vacuously true -> {0,2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 60))"),
        (std::set<int64_t>{0, 2, 3}));

    // MATCH_LEAST(json["arr"], $ == 100, threshold=2): only row2 has >=2 -> {2}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_LEAST(json["arr"], $ == 100, threshold=2))"),
              (std::set<int64_t>{2}));

    // MATCH_EXACT(json["arr"], $ == 100, threshold=3): only row2 has exactly 3.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_EXACT(json["arr"], $ == 100, threshold=3))"),
              (std::set<int64_t>{2}));

    // MATCH_MOST(json["arr"], $ > 90, threshold=0): rows with 0 matches.
    // row0->1, row1->0, row2->3, row3->0 -> {1,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_MOST(json["arr"], $ > 90, threshold=0))"),
        (std::set<int64_t>{1, 3}));

    // Compound element predicate with two `$` references: element in (60,90).
    // row0(80) matches; row1/row2/row3 none -> {0}.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["arr"], $ > 60 && $ < 90))"),
        (std::set<int64_t>{0}));

    // Range form lowers to a BinaryRangeExpr over the element -> same as above.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], 60 < $ < 90))"),
              (std::set<int64_t>{0}));

    // Term form ($ in [...]) lowers to a TermExpr over the element.
    // row0(80) & row2(100) each contain a listed value -> {0,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ in [80, 100]))"),
              (std::set<int64_t>{0, 2}));

    // Arithmetic form ($ % 2 == 0) lowers to a BinaryArithOpEvalRangeExpr.
    // row0(80), row1(40), row2(100) each have an even element -> {0,1,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ % 2 == 0))"),
              (std::set<int64_t>{0, 1, 2}));

    // MATCH_ALL arithmetic: every element even. row1(40) all even;
    // row2(100,100,100) all even; row3 [] vacuously true; row0(95 odd) fails.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["arr"], $ % 2 == 0))"),
              (std::set<int64_t>{1, 2, 3}));

    // Empty-array (row3) vacuous-truth edge case made explicit:
    //   - MATCH_ALL includes the empty row (no element violates the predicate).
    //   - MATCH_ANY can never be satisfied by an empty array.
    auto match_all_60 = RetrieveMatchedRows(
        seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 60))");
    EXPECT_NE(match_all_60.find(3), match_all_60.end())
        << "empty JSON array row must satisfy MATCH_ALL (vacuous truth)";
    auto match_any_ge0 = RetrieveMatchedRows(
        seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ >= 0))");
    EXPECT_EQ(match_any_ge0.find(3), match_any_ge0.end())
        << "empty JSON array row must never satisfy MATCH_ANY";
}

// Bitwise arithmetic over JSON int elements: `($ & m) == v`, `|`, `^`. These
// lower to an element-level BinaryArithOpEvalRangeExpr with a
// BitAnd/BitOr/BitXor op; before this fix the parser accepted them but segcore
// threw "unsupported arith type for JSON element predicate" at execution. The
// element path now mirrors the row-level JSON bitwise semantics (operands
// truncated to int64).
TEST_P(JsonArrayMatchExprTest, IntArrayBitwiseAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: [3,8]  row1: [4]  row2: [1,2,5]  row3: [] (empty array)
    std::vector<std::string> rows = {
        R"({"arr":[3,8]})",
        R"({"arr":[4]})",
        R"({"arr":[1,2,5]})",
        R"({"arr":[]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // BitAnd, MATCH_ANY: ($ & 1) == 1 -> rows with an odd element.
    // row0(3) & row2(1,5) -> {0,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], ($ & 1) == 1))"),
              (std::set<int64_t>{0, 2}));

    // BitAnd, MATCH_ALL: every element odd. row0(8 even), row1(4), row2(2 even)
    // fail; row3 [] vacuously true -> {3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["arr"], ($ & 1) == 1))"),
              (std::set<int64_t>{3}));

    // BitOr, MATCH_ANY: ($ | 1) == 5 -> an element x with x|1==5 (x in {4,5}).
    // row1(4) & row2(5) -> {1,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], ($ | 1) == 5))"),
              (std::set<int64_t>{1, 2}));

    // BitXor, MATCH_ANY: ($ ^ 1) == 2 -> an element x with x^1==2 (x==3).
    // only row0(3) -> {0}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], ($ ^ 1) == 2))"),
              (std::set<int64_t>{0}));
}

TEST_P(JsonArrayMatchExprTest, VarCharArrayAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: ["x","y"]  row1: ["z"]  row2: [] (empty)  row3: ["x"]
    std::vector<std::string> rows = {
        R"({"arr":["x","y"]})",
        R"({"arr":["z"]})",
        R"({"arr":[]})",
        R"({"arr":["x"]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(json["arr"], $ == "x") -> {0,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ == "x"))"),
        (std::set<int64_t>{0, 3}));

    // MATCH_ALL(json["arr"], $ != ""): every element non-empty; row2 [] vacuous.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ != ""))"),
        (std::set<int64_t>{0, 1, 2, 3}));

    // Compound string predicate with two `$` references.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["arr"], $ == "x" || $ == "y"))"),
        (std::set<int64_t>{0, 3}));
}

TEST_P(JsonArrayMatchExprTest, BareRootArray) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // The JSON document itself is the array (empty nested path -> root array).
    // row0: [1,2,3]  row1: [10]  row2: [] (empty)
    std::vector<std::string> rows = {
        R"([1,2,3])",
        R"([10])",
        R"([])",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(json, $ > 5): row0(1,2,3) has no element >5; row1(10) matches;
    // row2 [] never matches MATCH_ANY -> {1}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, R"(MATCH_ANY(json, $ > 5))"),
              (std::set<int64_t>{1}));

    // MATCH_ALL(json, $ >= 1): row0(1,2,3) all >=1; row1(10) >=1;
    // row2 [] vacuous -> {0,1,2}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(), *schema, schema, R"(MATCH_ALL(json, $ >= 1))"),
              (std::set<int64_t>{0, 1, 2}));
}

// Nullable JSON: MATCH is three-valued. A NULL row yields a NULL result and is
// excluded from every quantifier (never a vacuous match), unlike a non-null
// empty array `[]`, which still follows the vacuous-truth rules.
TEST_P(JsonArrayMatchExprTest, NullableJsonArrayAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid =
        schema->AddDebugField("json", DataType::JSON, /*nullable=*/true);

    // row0: [95,80]  row1: NULL  row2: [100,100,100]  row3: [] (empty)  row4: [40]
    std::vector<std::string> rows = {
        R"({"arr":[95,80]})",
        R"({})",  // backing text for the NULL row; valid bit is false below
        R"({"arr":[100,100,100]})",
        R"({"arr":[]})",
        R"({"arr":[40]})",
    };
    std::vector<bool> valid = {true, false, true, true, true};
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(json["arr"], $ > 90): row0(95) & row2(100) -> {0,2}.
    // NULL(1)/empty(3)/row4(40) excluded.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ > 90))"),
        (std::set<int64_t>{0, 2}));

    // MATCH_ALL(json["arr"], $ >= 60): row0 & row2 true; empty(3) vacuously
    // true; NULL(1) excluded (three-valued); row4(40) false -> {0,2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 60))"),
        (std::set<int64_t>{0, 2, 3}));

    // MATCH_LEAST(json["arr"], $ == 100, threshold=2): only row2 -> {2}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_LEAST(json["arr"], $ == 100, threshold=2))"),
              (std::set<int64_t>{2}));

    // MATCH_MOST(json["arr"], $ > 90, threshold=0): rows with 0 matches among
    // non-null rows: empty(3), row4(40) -> {3,4}. NULL(1) excluded.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_MOST(json["arr"], $ > 90, threshold=0))"),
        (std::set<int64_t>{3, 4}));

    // MATCH_EXACT(json["arr"], $ == 100, threshold=0): non-null rows with exactly
    // 0 matches of (==100): row0, empty(3), row4 -> {0,3,4}. NULL(1) excluded.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_EXACT(json["arr"], $ == 100, threshold=0))"),
              (std::set<int64_t>{0, 3, 4}));
}

// Semantic A (JSON three-valued, isolated from field-nullability): the JSON
// field is NON-nullable, so every row's field value is valid. What differs is
// whether the PATH resolves to a real JSON array. Only a genuine array (even
// empty `[]`) evaluates normally; a JSON null at the path, a missing key, or a
// non-array scalar at the path all yield UNKNOWN and are excluded. This is the
// distinction that "field is not NULL" alone cannot make.
TEST_P(JsonArrayMatchExprTest, JsonPathNotArrayExcluded) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    // NON-nullable JSON field: isolates path-not-array logic from field null.
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: real value array   row1: empty array   row2: JSON null at path
    // row3: missing key         row4: non-array scalar at path
    std::vector<std::string> rows = {
        R"({"arr":[1,2,3]})",
        R"({"arr":[]})",
        R"({"arr":null})",
        R"({})",
        R"({"arr":5})",
    };
    auto insert =
        BuildJsonInsert(*schema, pk_fid, json_fid, rows, /*valid=*/{});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ALL(json["arr"], $ >= 0): row0 (1,2,3 all >=0) true; row1 (empty)
    // vacuously true; row2/3/4 have no real array -> UNKNOWN -> excluded -> {0,1}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 0))"),
        (std::set<int64_t>{0, 1}));

    // MATCH_MOST(json["arr"], $ > 0, threshold=0): row0 has 3 matches (>0) ->
    // false; row1 (empty) has 0 matches <= 0 -> vacuously true; row2/3/4 have no
    // real array -> excluded -> {1}. The empty array is INCLUDED while the
    // json-null / missing-path / non-array rows are EXCLUDED.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_MOST(json["arr"], $ > 0, threshold=0))"),
        (std::set<int64_t>{1}));
}

// NOT over JSON MATCH_* must propagate UNKNOWN. On the JSON path the
// UNKNOWN mask comes from MaskJsonNonArrayRows (distinct code from the
// scalar/struct-array path's MaskNullRows, which
// ScalarArrayMatchExprTest.NullableNotMatchThreeValued already locks): a
// NULL JSON row, a JSON null at the path, and a missing key are all UNKNOWN
// for the MATCH, stay UNKNOWN after NOT (ThreeValuedLogicOp::Not flips only
// definite values), and are EXCLUDED by the filter -- while a real empty
// `[]` is a definite true/false that NOT inverts normally. This mirrors
// NullableNotMatchThreeValued on the array path.
TEST_P(JsonArrayMatchExprTest, JsonNotMatchThreeValued) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid =
        schema->AddDebugField("json", DataType::JSON, /*nullable=*/true);

    // row0: [1,2,3] (all > 0)   row1: [] (empty real array)
    // row2: NULL JSON row       row3: JSON null at path
    // row4: missing key         row5: [-1,2] (mixed)
    std::vector<std::string> rows = {
        R"({"arr":[1,2,3]})",
        R"({"arr":[]})",
        R"({})",  // backing text for the NULL row; valid bit is false below
        R"({"arr":null})",
        R"({})",
        R"({"arr":[-1,2]})",
    };
    std::vector<bool> valid = {true, true, false, true, true, true};
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ALL($ > 0): T{0}, T{1} (vacuous), F{5}, UNKNOWN{2,3,4}.
    // NOT keeps definite-false only -> {5}; NULL(2) / json-null(3) /
    // missing-key(4) must NOT surface under negation.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(not MATCH_ALL(json["arr"], $ > 0))"),
        (std::set<int64_t>{5}));

    // MATCH_ANY($ > 0): T{0,5}, F{1}, UNKNOWN{2,3,4}.
    // NOT -> {1}: the empty [] is a definite false that flips to true;
    // UNKNOWN rows stay excluded.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(not MATCH_ANY(json["arr"], $ > 0))"),
        (std::set<int64_t>{1}));

    // MATCH_EXACT($ > 0, threshold=0): T{1} (0 matches), F{0,5},
    // UNKNOWN{2,3,4}. NOT -> {0,5}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(not MATCH_EXACT(json["arr"], $ > 0, threshold=0))"),
              (std::set<int64_t>{0, 5}));
}

// Element-level empty-IN inside MATCH_* on JSON: `$ in []` performs zero
// membership comparisons, so it is a definite FALSE for EVERY element of a
// real array (valid bit kept) -- mirroring row-level empty-IN semantics.
// Regression guard: the empty term set used to fall into the bool-typed
// extraction, which marked every non-bool element INVALID (UNKNOWN); UNKNOWN
// elements are excluded from MATCH_ALL's element count, so a NON-empty row
// like [1,2,3] degenerated to element_count==0 and matched vacuously. A lone
// `$ in []` is rejected by the parser (a JSON MATCH_* predicate needs at
// least one typed literal), so the reachable failing shape is the compound
// `$ > 1 && $ in []`.
TEST_P(JsonArrayMatchExprTest, JsonElementEmptyInNotVacuous) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: real non-empty array   row1: empty array   row2: JSON null at path
    // row3: missing key            row4: non-array scalar at path
    std::vector<std::string> rows = {
        R"({"scores":[1,2,3]})",
        R"({"scores":[]})",
        R"({"scores":null})",
        R"({})",
        R"({"scores":"notarray"})",
    };
    auto insert =
        BuildJsonInsert(*schema, pk_fid, json_fid, rows, /*valid=*/{});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ALL($ > 1 && $ in []): every element of row0 is a definite false
    // (`in []` is false regardless of `$ > 1`), so row0 must NOT match; the
    // empty [] (row1) stays vacuously true; null-at-path / missing-key /
    // non-array (rows 2,3,4) are UNKNOWN and excluded -> exactly {1}.
    // Before the fix row0 wrongly matched (all elements INVALID -> vacuous).
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ALL(json["scores"], $ > 1 && $ in []))"),
        (std::set<int64_t>{1}));

    // MATCH_ANY($ > 1 && $ in []): no element anywhere satisfies `$ in []`,
    // and an empty array can never satisfy ANY -> no rows.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["scores"], $ > 1 && $ in []))"),
        (std::set<int64_t>{}));

    // Control: `$ not in []` is a definite TRUE per element, so the compound
    // reduces to `$ > 1`. MATCH_ALL: row0 fails on element 1 (1 > 1 is
    // false); row1 vacuous -> {1}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_ALL(json["scores"], $ > 1 && $ not in []))"),
              (std::set<int64_t>{1}));

    // Control: `$ >= 1 && $ not in []` holds for ALL of row0's elements, so
    // row0 matches alongside the vacuous row1 -> {0,1}. This proves the
    // not-in-empty elements are definite TRUE (valid), not false/UNKNOWN.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_ALL(json["scores"], $ >= 1 && $ not in []))"),
              (std::set<int64_t>{0, 1}));

    // MATCH_ANY with not-in-empty: row0 has elements 2,3 satisfying `$ > 1`
    // -> {0}; the empty row1 never satisfies ANY.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_ANY(json["scores"], $ > 1 && $ not in []))"),
              (std::set<int64_t>{0}));
}

// Tri-container alignment: ONE segment holding a nullable scalar VarChar
// array (`tags`, element via `$`), a nullable struct array sub-field
// (`st[val]`, element via `$[val]`), and a nullable JSON field (`js`, array
// at js["arr"], element via `$`) that all encode the SAME logical rows:
//   row0 ["x","y"], row1 NULL, row2 ["z"], row3 [] (empty), row4 ["x"].
// Every MATCH_* quantifier must produce IDENTICAL row sets across all three
// container kinds -- an explicit executable equality (the suites above only
// mirror expected constants between the array and JSON tests).
TEST_P(JsonArrayMatchExprTest, TriContainerMatchAligned) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);
    auto st_val_fid =
        schema->AddDebugArrayField("st[val]", DataType::VARCHAR, true);
    auto json_fid =
        schema->AddDebugField("js", DataType::JSON, /*nullable=*/true);

    // Shared logical layout across all three containers.
    const std::vector<std::vector<std::string>> rows = {
        {"x", "y"}, {}, {"z"}, {}, {"x"}};
    const std::vector<bool> row_valid = {true, false, true, true, true};
    const std::vector<std::string> json_rows = {
        R"({"arr":["x","y"]})",
        R"({})",  // backing text for the NULL row; valid bit is false below
        R"({"arr":["z"]})",
        R"({"arr":[]})",
        R"({"arr":["x"]})",
    };
    const int64_t N = static_cast<int64_t>(rows.size());

    FixedVector<bool> valid(N);
    for (int64_t i = 0; i < N; ++i) {
        valid[i] = row_valid[i];
    }

    // Build a string-array DataArray (with the shared null bitmap) for `fid`.
    auto make_str_array = [&](FieldId fid) {
        std::vector<milvus::proto::schema::ScalarField> data(N);
        for (int64_t i = 0; i < N; ++i) {
            for (const auto& v : rows[i]) {
                data[i].mutable_string_data()->add_data(v);
            }
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
    auto json_array = CreateDataArrayFrom(
        json_rows.data(), valid.data(), N, schema->operator[](json_fid));
    insert->mutable_fields_data()->AddAllocated(json_array.release());
    insert->set_num_rows(N);

    auto seg = MakeSegment(schema, std::move(insert));

    struct Case {
        std::string scalar_expr;  // over `tags` using `$`
        std::string struct_expr;  // over `st`   using `$[val]`
        std::string json_expr;    // over `js["arr"]` using `$`
        std::set<int64_t> expected;
    };
    const std::vector<Case> cases = {
        // At least one "x" -> {0,4}. NULL(1) and empty(3) excluded.
        {R"(MATCH_ANY(tags, $ == "x"))",
         R"(MATCH_ANY(st, $[val] == "x"))",
         R"(MATCH_ANY(js["arr"], $ == "x"))",
         {0, 4}},
        // Every element != "": empty(3) vacuously true; NULL(1) excluded
        // (three-valued) -> {0,2,3,4}.
        {R"(MATCH_ALL(tags, $ != ""))",
         R"(MATCH_ALL(st, $[val] != ""))",
         R"(MATCH_ALL(js["arr"], $ != ""))",
         {0, 2, 3, 4}},
        // At least one "x": same as MATCH_ANY -> {0,4}.
        {R"(MATCH_LEAST(tags, $ == "x", threshold=1))",
         R"(MATCH_LEAST(st, $[val] == "x", threshold=1))",
         R"(MATCH_LEAST(js["arr"], $ == "x", threshold=1))",
         {0, 4}},
        // At most zero "x" -> non-null rows with no "x": {2,3}.
        {R"(MATCH_MOST(tags, $ == "x", threshold=0))",
         R"(MATCH_MOST(st, $[val] == "x", threshold=0))",
         R"(MATCH_MOST(js["arr"], $ == "x", threshold=0))",
         {2, 3}},
        // Exactly zero "x" -> {2,3}. NULL(1) excluded.
        {R"(MATCH_EXACT(tags, $ == "x", threshold=0))",
         R"(MATCH_EXACT(st, $[val] == "x", threshold=0))",
         R"(MATCH_EXACT(js["arr"], $ == "x", threshold=0))",
         {2, 3}},
    };

    for (const auto& c : cases) {
        auto scalar_rows =
            RetrieveMatchedRows(seg.get(), *schema, schema, c.scalar_expr);
        auto struct_rows =
            RetrieveMatchedRows(seg.get(), *schema, schema, c.struct_expr);
        auto json_rows_matched =
            RetrieveMatchedRows(seg.get(), *schema, schema, c.json_expr);
        EXPECT_EQ(scalar_rows, c.expected)
            << "scalar expr result mismatch: " << c.scalar_expr;
        EXPECT_EQ(struct_rows, c.expected)
            << "struct expr result mismatch: " << c.struct_expr;
        EXPECT_EQ(json_rows_matched, c.expected)
            << "json expr result mismatch: " << c.json_expr;
        // Core guarantee: all three container kinds agree row-for-row.
        EXPECT_EQ(scalar_rows, struct_rows)
            << "scalar vs struct divergence: " << c.scalar_expr << " | "
            << c.struct_expr;
        EXPECT_EQ(scalar_rows, json_rows_matched)
            << "scalar vs json divergence: " << c.scalar_expr << " | "
            << c.json_expr;
    }
}

TEST_P(JsonArrayMatchExprTest, BoolArrayAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: [true,false]  row1: [false]  row2: [true,true]  row3: []
    std::vector<std::string> rows = {
        R"({"arr":[true,false]})",
        R"({"arr":[false]})",
        R"({"arr":[true,true]})",
        R"({"arr":[]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY($ == true): row0 & row2 -> {0,2}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ == true))"),
        (std::set<int64_t>{0, 2}));
    // MATCH_ALL($ == true): row2 all-true; row3 [] vacuous -> {2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ == true))"),
        (std::set<int64_t>{2, 3}));
    // MATCH_ANY($ == false): row0 & row1 -> {0,1}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ == false))"),
              (std::set<int64_t>{0, 1}));
}

TEST_P(JsonArrayMatchExprTest, DoubleArrayAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0: [1.5,2.5]  row1: [0.5]  row2: [3.5,3.5]  row3: []
    std::vector<std::string> rows = {
        R"({"arr":[1.5,2.5]})",
        R"({"arr":[0.5]})",
        R"({"arr":[3.5,3.5]})",
        R"({"arr":[]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // UnaryRange over float elements: row0(2.5) & row2(3.5) > 2.0 -> {0,2}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ > 2.0))"),
        (std::set<int64_t>{0, 2}));
    // BinaryRange over float elements: row0(1.5,2.5 both in (1,3)) -> {0}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], 1.0 < $ < 3.0))"),
              (std::set<int64_t>{0}));
    // Term over float elements: row0(2.5) & row2(3.5) listed -> {0,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ in [2.5, 3.5]))"),
              (std::set<int64_t>{0, 2}));
    // Integer-operand arithmetic over float elements ($ * 2 yields an integer
    // arith type, so the compare value stays integer): row2(3.5*2=7 > 5) -> {2}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ * 2 > 5))"),
        (std::set<int64_t>{2}));
    // Float-operand arithmetic ($ * 2.0 > 5.0): row2(3.5*2.0=7.0) -> {2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ * 2.0 > 5.0))"),
              (std::set<int64_t>{2}));
    // Modulo uses std::fmod, not int truncation. All elements here are
    // fractional (1.5/2.5/0.5/3.5), so $ % 2 is never 0 -> {} (empty). With the
    // old int-truncation Mod, 2.5 would truncate to 2 and 2%2==0 would falsely
    // match row0.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ % 2 == 0))"),
              (std::set<int64_t>{}));
    // MATCH_ALL over float: every element > 1.0. row0 yes; row1(0.5) no;
    // row2 yes; row3 [] vacuous -> {0,2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ > 1.0))"),
        (std::set<int64_t>{0, 2, 3}));
}

TEST_P(JsonArrayMatchExprTest, RangeMatchVariants) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0:[95,80] row1:[40] row2:[100,100,100] row3:[]
    std::vector<std::string> rows = {
        R"({"arr":[95,80]})",
        R"({"arr":[40]})",
        R"({"arr":[100,100,100]})",
        R"({"arr":[]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ALL(0 < $ < 200): all elements in range; row3 [] vacuous -> {0,1,2,3}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["arr"], 0 < $ < 200))"),
              (std::set<int64_t>{0, 1, 2, 3}));
    // MATCH_LEAST(90 < $ < 110, threshold=2): only row2 (3 in range) -> {2}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_LEAST(json["arr"], 90 < $ < 110, threshold=2))"),
              (std::set<int64_t>{2}));
    // MATCH_EXACT(90 < $ < 110, threshold=1): row0 has exactly 1 (95) -> {0}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_EXACT(json["arr"], 90 < $ < 110, threshold=1))"),
              (std::set<int64_t>{0}));
    // MATCH_MOST(90 < $ < 110, threshold=0): rows with 0 in-range:
    // row1(40) & row3([]) -> {1,3}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_MOST(json["arr"], 90 < $ < 110, threshold=0))"),
              (std::set<int64_t>{1, 3}));
}

TEST_P(JsonArrayMatchExprTest, TermAndArithVariants) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // row0:["x","y"] row1:["z"] row2:[] row3:["x"]
    std::vector<std::string> srows = {
        R"({"arr":["x","y"]})",
        R"({"arr":["z"]})",
        R"({"arr":[]})",
        R"({"arr":["x"]})",
    };
    auto sinsert = BuildJsonInsert(*schema, pk_fid, json_fid, srows, {});
    auto sseg = MakeSegment(schema, std::move(sinsert));
    // String Term MATCH_ANY: row0(x),row1(z),row3(x) each list-member -> {0,1,3}.
    EXPECT_EQ(RetrieveMatchedRows(sseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ in ["x", "z"]))"),
              (std::set<int64_t>{0, 1, 3}));
    // String Term MATCH_ALL: row0(x,y) both in; row2 [] vacuous; row3(x) ->
    // {0,2,3}; row1(z) not in -> excluded.
    EXPECT_EQ(RetrieveMatchedRows(sseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["arr"], $ in ["x", "y"]))"),
              (std::set<int64_t>{0, 2, 3}));

    // Integer arithmetic op coverage (Add/Sub/Mul/Div) on a fresh segment.
    std::vector<std::string> irows = {
        R"({"arr":[95,80]})",
        R"({"arr":[40]})",
        R"({"arr":[100,100,100]})",
        R"({"arr":[]})",
    };
    auto iinsert = BuildJsonInsert(*schema, pk_fid, json_fid, irows, {});
    auto iseg = MakeSegment(schema, std::move(iinsert));
    // Add: $ + 10 >= 105 -> 95+10=105(row0), 100+10=110(row2) -> {0,2}.
    EXPECT_EQ(RetrieveMatchedRows(iseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ + 10 >= 105))"),
              (std::set<int64_t>{0, 2}));
    // Sub: $ - 5 == 35 -> 40-5=35(row1) -> {1}.
    EXPECT_EQ(RetrieveMatchedRows(iseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ - 5 == 35))"),
              (std::set<int64_t>{1}));
    // Mul: $ * 2 == 200 -> 100*2=200(row2) -> {2}.
    EXPECT_EQ(RetrieveMatchedRows(iseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ * 2 == 200))"),
              (std::set<int64_t>{2}));
    // Div (integer): $ / 40 == 1 -> only 40/40=1(row1) -> {1}.
    EXPECT_EQ(RetrieveMatchedRows(iseg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["arr"], $ / 40 == 1))"),
              (std::set<int64_t>{1}));
}

TEST_P(JsonArrayMatchExprTest, MultiLevelPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // Nested array at json["a"]["b"].
    std::vector<std::string> rows = {
        R"({"a":{"b":[1,2,3]}})",
        R"({"a":{"b":[10,20]}})",
        R"({"a":{"b":[]}})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY(json["a"]["b"], $ > 5): row1(10,20) -> {1}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["a"]["b"], $ > 5))"),
        (std::set<int64_t>{1}));
    // MATCH_ALL(json["a"]["b"], $ < 100): all in; row2 [] vacuous -> {0,1,2}.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["a"]["b"], $ < 100))"),
              (std::set<int64_t>{0, 1, 2}));
}

TEST_P(JsonArrayMatchExprTest, MixedTypeAndNullElements) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // Arrays mixing ints with type-mismatched / null elements. Invalid elements
    // (wrong type or JSON null) are skipped by MatchSingleRow's valid bitmap.
    std::vector<std::string> rows = {
        R"({"arr":[95,"x",80]})",
        R"({"arr":[95,null,80]})",
        R"({"arr":["a","b"]})",
        R"({"arr":[null,null]})",
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // MATCH_ANY($ > 90): valid element 95 matches in row0 & row1; rows 2/3 have
    // no valid numeric element -> {0,1}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ > 90))"),
        (std::set<int64_t>{0, 1}));
    // MATCH_ALL($ >= 90): only valid elements counted. row0/row1 have 80 (<90)
    // -> fail. rows 2/3 have 0 valid numeric elements -> vacuously true -> {2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 90))"),
        (std::set<int64_t>{2, 3}));
    // MATCH_LEAST($ >= 80, threshold=2): row0/row1 have 2 valid (95,80) -> {0,1}.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_LEAST(json["arr"], $ >= 80, threshold=2))"),
              (std::set<int64_t>{0, 1}));
}

// Element-level ($) numeric comparisons must preserve int64 exactness beyond
// 2^53, matching the row-level ("normal") JSON path (at_numeric). The old
// element-level path coerced every numeric element AND the int64 literal to
// double, so 2^53 and 2^53+1 collapsed to the same double and became
// indistinguishable. These cases pin the int64-exact behavior for all four
// operator classes (Unary / Range / Term / Arith) and the cross-check against
// the row-level path.
TEST_P(JsonArrayMatchExprTest, Int64PrecisionAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    // 2^53 = 9007199254740992 (representable as double)
    // 2^53+1 = 9007199254740993 (NOT representable; rounds to 2^53 as double)
    // 2^53+2 = 9007199254740994 (representable as double)
    // Each row carries both a scalar "v" and an array "arr" holding the same
    // value(s), so the element-level ($) result can be cross-checked against the
    // row-level (json["v"]) result on identical data.
    std::vector<std::string> rows = {
        R"({"v":9007199254740993,"arr":[9007199254740993]})",  // row0
        R"({"v":9007199254740992,"arr":[9007199254740992]})",  // row1
        R"({"v":9007199254740994,"arr":[9007199254740994]})",  // row2
        // row3: mixed int/float array (regression: float element still valid,
        // compared as double; big-int element stays exact).
        R"({"v":9007199254740993,"arr":[9007199254740993,1.5]})",  // row3
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    // --- Unary Equal: exact int64 must distinguish 2^53 from 2^53+1 ---
    // $ == 9007199254740993 : only rows whose array holds exactly 2^53+1 -> {0,3}.
    // (Old double path also matched row1's 2^53 -> {0,1,3}.)
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["arr"], $ == 9007199254740993))"),
        (std::set<int64_t>{0, 3}));
    // $ == 9007199254740992 : only row1 -> {1}.
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["arr"], $ == 9007199254740992))"),
        (std::set<int64_t>{1}));

    // --- Term ($ in [...]): membership must be int64-exact ---
    // $ in [9007199254740993] : {0,3} only, never row1.
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_ANY(json["arr"], $ in [9007199254740993]))"),
              (std::set<int64_t>{0, 3}));

    // --- Range boundary: $ > 2^53 must exclude 2^53 but include 2^53+1 ---
    // $ > 9007199254740992 : row0(2^53+1), row2(2^53+2), row3(2^53+1) -> {0,2,3}.
    // (Old double path evaluated 2^53+1 as 2^53, so row0/row3 were dropped.)
    EXPECT_EQ(
        RetrieveMatchedRows(seg.get(),
                            *schema,
                            schema,
                            R"(MATCH_ANY(json["arr"], $ > 9007199254740992))"),
        (std::set<int64_t>{0, 2, 3}));
    // Range with both bounds: 9007199254740992 < $ <= 9007199254740994
    // -> row0(2^53+1), row2(2^53+2), row3(2^53+1) -> {0,2,3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(),
            *schema,
            schema,
            R"(MATCH_ANY(json["arr"], 9007199254740992 < $ <= 9007199254740994))"),
        (std::set<int64_t>{0, 2, 3}));

    // --- Arithmetic: $ + 0 == 2^53+1 must be int64-exact -> {0,3}. ---
    EXPECT_EQ(RetrieveMatchedRows(
                  seg.get(),
                  *schema,
                  schema,
                  R"(MATCH_ANY(json["arr"], $ + 0 == 9007199254740993))"),
              (std::set<int64_t>{0, 3}));

    // --- Cross-check element-level ($) vs row-level (json["v"]) on same data.
    // Row-level int64 path is the source of truth; element-level must agree.
    for (const auto& [elem_expr, row_expr] :
         std::vector<std::pair<std::string, std::string>>{
             {R"(MATCH_ANY(json["arr"], $ == 9007199254740993))",
              R"(json["v"] == 9007199254740993)"},
             {R"(MATCH_ANY(json["arr"], $ == 9007199254740992))",
              R"(json["v"] == 9007199254740992)"},
             {R"(MATCH_ANY(json["arr"], $ > 9007199254740992))",
              R"(json["v"] > 9007199254740992)"},
             {R"(MATCH_ANY(json["arr"], $ in [9007199254740993]))",
              R"(json["v"] in [9007199254740993])"},
         }) {
        auto elem = RetrieveMatchedRows(seg.get(), *schema, schema, elem_expr);
        auto row = RetrieveMatchedRows(seg.get(), *schema, schema, row_expr);
        EXPECT_EQ(elem, row)
            << "element-level must match row-level for: " << elem_expr << " vs "
            << row_expr;
    }

    // --- Regression: float literal / float element still behave as double. ---
    // $ == 1.5 (float literal): only the float element in row3 -> {3}.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ANY(json["arr"], $ == 1.5))"),
        (std::set<int64_t>{3}));
    // MATCH_ALL($ >= 1) over row3 [2^53+1, 1.5]: int element exact, float
    // element as double, both >= 1 -> row3 satisfies; all rows satisfy.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(MATCH_ALL(json["arr"], $ >= 1))"),
        (std::set<int64_t>{0, 1, 2, 3}));
}

// LIKE / pattern predicates over JSON string elements (EvalJsonBrute path).
// Same wildcard shapes as the scalar/struct VarCharArrayLikePatterns test:
// prefix "ab%", suffix "%bc", infix "%b%" and the general "a%c" Match op.
// A NULL JSON row is UNKNOWN and excluded; an empty [] stays vacuous under
// MATCH_ALL.
TEST_P(JsonArrayMatchExprTest, StringLikePatterns) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    // Same layout as VarCharArrayLikePatterns: row1 NULL, row3 empty [].
    std::vector<std::string> rows = {
        R"({"tags":["abc","abd"]})",
        R"({})",  // NULL row (valid=false)
        R"({"tags":["xyz"]})",
        R"({"tags":[]})",
        R"({"tags":["abc","xyz"]})",
        R"({"tags":["cab"]})",
    };
    std::vector<bool> valid = {true, false, true, true, true, true};
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // prefix: elements starting "ab" -> row0, row4.
        {R"(MATCH_ANY(json["tags"], $ like "ab%"))", {0, 4}},
        // prefix ALL: row0 all-prefix; empty row3 vacuous; NULL row1 excluded.
        {R"(MATCH_ALL(json["tags"], $ like "ab%"))", {0, 3}},
        // suffix: elements ending "bc" -> "abc" in row0, row4.
        {R"(MATCH_ANY(json["tags"], $ like "%bc"))", {0, 4}},
        // infix: elements containing "b" -> row0, row4, row5 ("cab").
        {R"(MATCH_ANY(json["tags"], $ like "%b%"))", {0, 4, 5}},
        // infix ALL: row0, empty row3 vacuous, row5; row4 has "xyz".
        {R"(MATCH_ALL(json["tags"], $ like "%b%"))", {0, 3, 5}},
        // general Match: starts "a" AND ends "c" -> only "abc".
        {R"(MATCH_ANY(json["tags"], $ like "a%c"))", {0, 4}},
        // quantified LIKE: >=2 prefix-matching elements -> only row0.
        {R"(MATCH_LEAST(json["tags"], $ like "ab%", threshold=2))", {0}},
        // NOT over pattern MATCH, three-valued: T{0,4} F{2,3,5} U{1}.
        {R"(not MATCH_ANY(json["tags"], $ like "ab%"))", {2, 3, 5}},
    };
    for (const auto& [expr, expected] : cases) {
        EXPECT_EQ(RetrieveMatchedRows(seg.get(), *schema, schema, expr),
                  expected)
            << "JSON LIKE mismatch: " << expr;
    }
}

// Non-empty NOT IN (and its NOT-of-MATCH composition) over JSON string
// elements; only IN and the empty in-list were execution-tested before.
TEST_P(JsonArrayMatchExprTest, TermNotInAtPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    // row2 empty [], row3 NULL.
    std::vector<std::string> rows = {
        R"({"arr":["x","y"]})",
        R"({"arr":["z"]})",
        R"({"arr":[]})",
        R"({})",
        R"({"arr":["x"]})",
    };
    std::vector<bool> valid = {true, true, true, false, true};
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // any element not in {"x"}: row0 ("y"), row1 ("z"); row4 all-"x"
        // fails; empty row2 can never satisfy ANY; NULL row3 excluded.
        {R"(MATCH_ANY(json["arr"], $ not in ["x"]))", {0, 1}},
        // no element in {"x"}: row1, empty row2 vacuous.
        {R"(MATCH_ALL(json["arr"], $ not in ["x"]))", {1, 2}},
        // NOT over NOT-IN MATCH_ANY: T{0,1} F{2,4} U{3} -> {2,4}.
        {R"(not MATCH_ANY(json["arr"], $ not in ["x"]))", {2, 4}},
    };
    for (const auto& [expr, expected] : cases) {
        EXPECT_EQ(RetrieveMatchedRows(seg.get(), *schema, schema, expr),
                  expected)
            << "JSON NOT-IN mismatch: " << expr;
    }
}

// Three-level nested path json["a"]["b"]["c"] resolving to an array leaf. A
// row whose document lacks the full path is UNKNOWN and excluded, never a
// vacuous match.
TEST_P(JsonArrayMatchExprTest, ThreeLevelPath) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    std::vector<std::string> rows = {
        R"({"a":{"b":{"c":[1,2,3]}}})",
        R"({"a":{"b":{"c":[10]}}})",
        R"({"a":{"b":{"c":[]}}})",
        R"({"a":{"b":{}}})",  // path missing -> UNKNOWN
    };
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, {});
    auto seg = MakeSegment(schema, std::move(insert));

    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ANY(json["a"]["b"]["c"], $ > 5))"),
              (std::set<int64_t>{1}));
    // MATCH_ALL: rows 0/1 all < 100; empty row2 vacuous; row3 (missing path)
    // stays excluded.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(MATCH_ALL(json["a"]["b"]["c"], $ < 100))"),
              (std::set<int64_t>{0, 1, 2}));
}

// ALL-NULL JSON segment: every row's JSON is NULL, so every quantifier yields
// UNKNOWN for every row and MATCH_* returns zero rows.
TEST_P(JsonArrayMatchExprTest, AllNullRowsMatchNothing) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    std::vector<std::string> rows = {R"({})", R"({})", R"({})", R"({})"};
    std::vector<bool> valid(rows.size(), false);
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(json["arr"], $ > 0))",
        R"(MATCH_ALL(json["arr"], $ > 0))",
        R"(MATCH_MOST(json["arr"], $ > 0, threshold=5))",
    };
    for (const auto& e : exprs) {
        EXPECT_EQ(RetrieveMatchedRows(seg.get(), *schema, schema, e),
                  (std::set<int64_t>{}))
            << "all-NULL JSON segment must match nothing: " << e;
    }
}

// json_contains_all(json["a"], []) has IS-NOT-NULL-like semantics over the
// path: vacuously TRUE for every row whose path holds a real JSON array
// (including the empty []), UNKNOWN for a missing path / non-array value /
// NULL field — mirroring how the non-empty ContainsAll treats such rows
// (pg: '[1,2]'::jsonb @> '[]' is true, but NULL @> '{}' yields NULL).
TEST_P(JsonArrayMatchExprTest, JsonContainsAllEmptyListIsNotNullSemantics) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    std::vector<std::string> rows = {
        R"({"a":[1,2]})",     // row0: real array          -> TRUE
        R"({"a":[]})",        // row1: real EMPTY array    -> still TRUE
        R"({"a":"scalar"})",  // row2: path not an array   -> UNKNOWN
        R"({})",              // row3: path missing        -> UNKNOWN
        R"({"a":[3]})",       // row4: NULL json field     -> UNKNOWN
    };
    std::vector<bool> valid = {true, true, true, true, false};
    auto insert = BuildJsonInsert(*schema, pk_fid, json_fid, rows, valid);
    auto seg = MakeSegment(schema, std::move(insert));

    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(json_contains_all(json["a"], []))"),
        (std::set<int64_t>{0, 1}));
    // UNKNOWN survives negation: bad-path rows and the NULL row stay
    // excluded under NOT, and the two real-array rows are definite TRUE,
    // so nothing qualifies.
    EXPECT_EQ(RetrieveMatchedRows(seg.get(),
                                  *schema,
                                  schema,
                                  R"(not json_contains_all(json["a"], []))"),
              (std::set<int64_t>{}));
    // ContainsAny over an empty candidate set can never match anything.
    EXPECT_EQ(
        RetrieveMatchedRows(
            seg.get(), *schema, schema, R"(json_contains_any(json["a"], []))"),
        (std::set<int64_t>{}));
}

INSTANTIATE_TEST_SUITE_P(
    SegTypes,
    JsonArrayMatchExprTest,
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
// After the fix the fill path registers an all-zeros offsets table, so MATCH_*
// must run without throwing. The backfilled field carries no default value, so
// DefaultValueChunkTranslator materializes every backfilled row as NULL (Arrow
// AppendNulls), NOT as an empty array. Under three-valued MATCH semantics a NULL
// row yields UNKNOWN and is excluded from EVERY quantifier, so both MATCH_ANY
// and MATCH_ALL return no rows here. (An actual non-null empty array `[]` would
// still be vacuously true for MATCH_ALL -- see the row3 case in
// ScalarArrayMatchExprTest.Int64Array.)
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

    // MATCH_ANY over NULL rows: excluded -> no rows. The key assertion is that
    // this does NOT throw (the pre-fix missing-offsets assert).
    EXPECT_EQ(run("MATCH_ANY(scores, $ > 0)"), (std::set<int64_t>{}));

    // MATCH_ALL over NULL rows: three-valued -> UNKNOWN -> excluded (NOT a
    // vacuous match, since these rows are NULL, not empty arrays).
    EXPECT_EQ(run("MATCH_ALL(scores, $ >= 60)"), (std::set<int64_t>{}));
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
// Behavior asserted is the three-valued one (a NULL row yields UNKNOWN and is
// excluded from every MATCH_* -- see
// ScalarArrayMatchExprTest.NullableArrayScalarStructAligned); the point of this
// test is that the load path and the insert path agree and neither corrupts
// lengths.
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
        auto scores_array =
            CreateDataArrayFrom(scores.data(),
                                valid.data(),
                                N,
                                ins_schema->operator[](ins_scores_fid));
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

    // Expected MATCH_* row sets under three-valued semantics (NULL -> UNKNOWN,
    // excluded from every quantifier):
    //   $ > 90   : row1(95) and row3(100,100,100)               -> {1, 3}
    //   $ >= 60  : row1 ok; row3 ok; NULL rows 0,2 excluded;
    //              row4 (40) fails                                -> {1, 3}
    //   $ < 50   : only row4 (40)                                 -> {4}
    //   == 100, threshold>=2 : only row3 (three 100s)            -> {3}
    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        {"MATCH_ANY(scores, $ > 90)", {1, 3}},
        {"MATCH_ALL(scores, $ >= 60)", {1, 3}},
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
        EXPECT_EQ(inserted_rows, expected) << "insert path wrong for: " << expr;
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
// entries for NULL/empty rows. Three-valued MATCH excludes the NULL row while
// the genuine empty `[]` row still participates vacuously -- and this stays
// correct even on the index path.
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
    // every element != "" : empty(3) is vacuously true and all valued rows have
    // non-empty elements; NULL(1) is excluded (three-valued) -> {0,2,3,4}.
    EXPECT_EQ(RetrieveMatchRowsLocal(
                  seg.get(), *schema, schema, R"(MATCH_ALL(tags, $ != ""))"),
              (std::set<int64_t>{0, 2, 3, 4}));
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

namespace {

// Build + load a NESTED bitmap index over the scalar INT64 array `scores_fid`.
// Unlike InvertedIndexTantivy, BitmapIndex has no BuildWithRawDataForUT: its
// nested build path triggers on (field schema is ARRAY && is_nested_index), so
// hand it a FileManagerContext carrying the ARRAY/INT64 field schema plus a
// throwaway local chunk manager (BitmapIndex::Load logs through the file
// manager, so the context must be Valid() even though build / Serialize /
// Load(BinarySet) never touch remote storage). A freshly built BitmapIndex
// finalizes its queryable structures on Serialize/Load, so the segment
// receives the LOADED copy -- matching the production load path.
void
LoadScalarInt64ArrayNestedBitmapIndex(
    SegmentSealed* seg,
    FieldId scores_fid,
    const std::vector<std::vector<int64_t>>& rows,
    const std::vector<bool>& valid) {
    const int64_t N = static_cast<int64_t>(rows.size());

    // Compact nullable ARRAY FieldData: a NULL row occupies no slot.
    std::vector<milvus::proto::schema::ScalarField> scores(N);
    for (int64_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            scores[i].mutable_long_data()->add_data(v);
        }
    }
    std::vector<milvus::Array> array_data;
    array_data.reserve(N);
    for (const auto& s : scores) {
        array_data.emplace_back(s);
    }
    std::vector<uint8_t> valid_bytes((N + 7) / 8, 0);
    for (int64_t i = 0; i < N; ++i) {
        if (valid[i]) {
            valid_bytes[i / 8] |= (1 << (i % 8));
        }
    }
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    field_data->FillFieldData(array_data.data(), valid_bytes.data(), N, 0);

    milvus::proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(milvus::proto::schema::DataType::Array);
    field_schema.set_element_type(milvus::proto::schema::DataType::Int64);
    field_schema.set_nullable(true);
    auto field_meta =
        storage::FieldDataMeta{1, 2, 3, scores_fid.get(), field_schema};
    auto index_meta = storage::IndexMeta{3, scores_fid.get(), 4001, 4001};

    auto root_path = TestLocalPath + "/match_nested_bitmap";
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto built = std::make_unique<index::BitmapIndex<int64_t>>(
        ctx, /*is_nested_index=*/true);
    built->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(built->IsNestedIndex());

    auto binary_set = built->Serialize({});
    auto loaded = std::make_unique<index::BitmapIndex<int64_t>>(
        ctx, /*is_nested_index=*/false);
    loaded->Load(binary_set, {});
    // The persisted nested marker must be restored on load; the segment's
    // pinned scalar index for the field IS this nested bitmap.
    ASSERT_TRUE(loaded->IsNestedIndex());

    LoadIndexInfo info{};
    info.field_id = scores_fid.get();
    info.index_params = GenIndexParams(loaded.get());
    info.cache_index =
        CreateTestCacheIndex("scalar_scores_bitmap", std::move(loaded));
    seg->LoadIndex(info);

    boost::filesystem::remove_all(root_path);
}

}  // namespace

// MATCH execution end-to-end through a nested BITMAP index. The nested-index
// e2e coverage above goes through tantivy/INVERTED only; the exec path is
// index-type agnostic, so a nested BitmapIndex must serve the element
// predicate the same way: bitmap In/Range answers element-level bits,
// IArrayOffsets (built from raw) folds them per row for the quantifier, and
// three-valued NULL semantics hold. All five quantifiers are pinned against
// hand-computed sets AND cross-checked against an identical index-free
// (brute-force) segment.
TEST(ScalarArrayMatchIndex, NestedBitmapIndexNullable) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);

    // Against the element predicate $ > 100:
    //   row0 = NULL, row1 = [] (empty, non-null), row2 = one match,
    //   row3 = all match, row4 = zero match.
    const std::vector<std::vector<int64_t>> rows = {
        {}, {}, {10, 200}, {150, 250}, {10, 20}};
    const std::vector<bool> valid = {false, true, true, true, true};

    auto indexed =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    LoadScalarInt64ArrayNestedBitmapIndex(
        indexed.get(), scores_fid, rows, valid);
    auto brute =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);

    // NULL row0 is excluded from every quantifier (three-valued semantics);
    // empty row1 still participates: vacuously true under MATCH_ALL and zero
    // matching elements under MATCH_MOST/MATCH_EXACT(threshold=0).
    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        {"MATCH_ANY(scores, $ > 100)", {2, 3}},
        {"MATCH_ALL(scores, $ > 100)", {1, 3}},
        {"MATCH_LEAST(scores, $ > 100, threshold=1)", {2, 3}},
        {"MATCH_MOST(scores, $ > 100, threshold=0)", {1, 4}},
        {"MATCH_EXACT(scores, $ > 100, threshold=0)", {1, 4}},
    };
    for (const auto& [expr, expected] : cases) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, expr);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, expr);
        EXPECT_EQ(ri, expected) << "bitmap-index path wrong for: " << expr;
        EXPECT_EQ(rb, expected) << "brute-force path wrong for: " << expr;
        EXPECT_EQ(ri, rb) << "bitmap index vs brute-force mismatch for: "
                          << expr;
    }
}

// array_contains_all(scores, []) has IS-NOT-NULL-like semantics: vacuously
// TRUE for every row holding a real array (including the empty []), UNKNOWN
// for the NULL row — matching the non-empty ContainsAll NULL treatment and
// pg's strict `arr @> '{}'` (NULL @> '{}' yields NULL, not true). Checked on
// a brute-force segment AND on one with a nested scalar index, where the
// empty-set ContainsAll must bypass the index and scan raw data.
TEST(ScalarArrayMatchIndex, ContainsAllEmptyListIsNotNullSemantics) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);

    // row0: real array, row1: real EMPTY array, row2: NULL.
    const std::vector<std::vector<int64_t>> rows = {{10, 20}, {}, {}};
    const std::vector<bool> valid = {true, true, false};

    auto brute =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    auto indexed =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    LoadScalarInt64ArrayNestedBitmapIndex(
        indexed.get(), scores_fid, rows, valid);

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // Real arrays — even the empty one — vacuously contain all zero
        // requested elements; the NULL row is UNKNOWN, not true.
        {"array_contains_all(scores, [])", {0, 1}},
        // UNKNOWN survives negation: the NULL row stays excluded under NOT.
        {"not array_contains_all(scores, [])", {}},
        // ContainsAny over an empty candidate set can never match.
        {"array_contains_any(scores, [])", {}},
    };
    for (const auto& [expr, expected] : cases) {
        EXPECT_EQ(RetrieveMatchRowsLocal(brute.get(), *schema, schema, expr),
                  expected)
            << "brute-force path wrong for: " << expr;
        EXPECT_EQ(RetrieveMatchRowsLocal(indexed.get(), *schema, schema, expr),
                  expected)
            << "indexed-segment path wrong for: " << expr;
    }
}

// LIKE / pattern predicates on a scalar VARCHAR array WITH a nested inverted
// index present. ShouldUseOp routes PrefixMatch ("ab%") and Match ("a%c")
// through the index, while PostfixMatch ("%bc") / InnerMatch ("%b%") are
// forced back to brute force -- both routes must produce the identical,
// hand-computed row sets, and match an index-free oracle segment.
TEST(ScalarArrayMatchIndex, LikePatternsNestedIndexEqualsBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    // Same layout as VarCharArrayLikePatterns: row1 NULL, row3 empty [].
    const std::vector<std::vector<std::string>> rows = {
        {"abc", "abd"}, {}, {"xyz"}, {}, {"abc", "xyz"}, {"cab"}};
    const std::vector<bool> valid = {true, false, true, true, true, true};
    const int64_t N = static_cast<int64_t>(rows.size());

    auto indexed = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);
    LoadScalarVarCharArrayNestedIndex(indexed.get(), tags_fid, N, rows);
    auto brute = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        // prefix -> index path (ShouldUseOp(PrefixMatch) == true).
        {R"(MATCH_ANY(tags, $ like "ab%"))", {0, 4}},
        {R"(MATCH_ALL(tags, $ like "ab%"))", {0, 3}},
        {R"(MATCH_LEAST(tags, $ like "ab%", threshold=2))", {0}},
        // suffix/infix -> brute-force fallback (ShouldUseOp == false).
        {R"(MATCH_ANY(tags, $ like "%bc"))", {0, 4}},
        {R"(MATCH_ANY(tags, $ like "%b%"))", {0, 4, 5}},
        {R"(MATCH_ALL(tags, $ like "%b%"))", {0, 3, 5}},
        // general pattern -> Match op, index path.
        {R"(MATCH_ANY(tags, $ like "a%c"))", {0, 4}},
        // three-valued NOT stays correct with the index present.
        {R"(not MATCH_ANY(tags, $ like "ab%"))", {2, 3, 5}},
    };
    for (const auto& [expr, expected] : cases) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, expr);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, expr);
        EXPECT_EQ(ri, expected) << "indexed LIKE path wrong for: " << expr;
        EXPECT_EQ(rb, expected) << "brute-force LIKE path wrong for: " << expr;
        EXPECT_EQ(ri, rb) << "index vs brute-force LIKE mismatch for: " << expr;
    }
}

namespace {

// Build + load a NESTED ScalarIndexSort (STL_SORT) over the scalar INT64
// array `scores_fid`. Mirrors LoadScalarInt64ArrayNestedBitmapIndex: sort
// indexes have no BuildWithRawDataForUT, so build from ARRAY FieldData with a
// FileManagerContext carrying the ARRAY/INT64 field schema, then Serialize +
// Load a fresh instance so the segment receives the loaded copy (and the
// persisted nested marker is exercised).
void
LoadScalarInt64ArrayNestedSortIndex(
    SegmentSealed* seg,
    FieldId scores_fid,
    const std::vector<std::vector<int64_t>>& rows,
    const std::vector<bool>& valid) {
    const int64_t N = static_cast<int64_t>(rows.size());

    std::vector<milvus::proto::schema::ScalarField> scores(N);
    for (int64_t i = 0; i < N; ++i) {
        for (auto v : rows[i]) {
            scores[i].mutable_long_data()->add_data(v);
        }
    }
    std::vector<milvus::Array> array_data;
    array_data.reserve(N);
    for (const auto& s : scores) {
        array_data.emplace_back(s);
    }
    std::vector<uint8_t> valid_bytes((N + 7) / 8, 0);
    for (int64_t i = 0; i < N; ++i) {
        if (valid[i]) {
            valid_bytes[i / 8] |= (1 << (i % 8));
        }
    }
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    field_data->FillFieldData(array_data.data(), valid_bytes.data(), N, 0);

    milvus::proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(milvus::proto::schema::DataType::Array);
    field_schema.set_element_type(milvus::proto::schema::DataType::Int64);
    field_schema.set_nullable(true);
    auto field_meta =
        storage::FieldDataMeta{1, 2, 3, scores_fid.get(), field_schema};
    auto index_meta = storage::IndexMeta{3, scores_fid.get(), 4002, 4002};

    auto root_path = TestLocalPath + "/match_nested_sort_i64";
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto built = std::make_unique<index::ScalarIndexSort<int64_t>>(
        ctx, /*is_nested_index=*/true);
    built->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(built->IsNestedIndex());

    auto binary_set = built->Serialize({});
    auto loaded = std::make_unique<index::ScalarIndexSort<int64_t>>(
        ctx, /*is_nested_index=*/false);
    loaded->Load(binary_set, {});
    // The persisted nested marker must be restored on load.
    ASSERT_TRUE(loaded->IsNestedIndex());

    LoadIndexInfo info{};
    info.field_id = scores_fid.get();
    info.index_params = GenIndexParams(loaded.get());
    info.cache_index =
        CreateTestCacheIndex("scalar_scores_sort", std::move(loaded));
    seg->LoadIndex(info);

    boost::filesystem::remove_all(root_path);
}

// Build + load a NESTED StringIndexSort over the scalar VARCHAR array
// `tags_fid`, following the same FieldData + Serialize/Load protocol.
void
LoadScalarVarCharArrayNestedStringSortIndex(
    SegmentSealed* seg,
    FieldId tags_fid,
    const std::vector<std::vector<std::string>>& rows,
    const std::vector<bool>& valid) {
    const int64_t N = static_cast<int64_t>(rows.size());

    std::vector<milvus::proto::schema::ScalarField> tags(N);
    for (int64_t i = 0; i < N; ++i) {
        for (const auto& v : rows[i]) {
            tags[i].mutable_string_data()->add_data(v);
        }
    }
    std::vector<milvus::Array> array_data;
    array_data.reserve(N);
    for (const auto& t : tags) {
        array_data.emplace_back(t);
    }
    std::vector<uint8_t> valid_bytes((N + 7) / 8, 0);
    for (int64_t i = 0; i < N; ++i) {
        if (valid[i]) {
            valid_bytes[i / 8] |= (1 << (i % 8));
        }
    }
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    field_data->FillFieldData(array_data.data(), valid_bytes.data(), N, 0);

    milvus::proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(milvus::proto::schema::DataType::Array);
    field_schema.set_element_type(milvus::proto::schema::DataType::VarChar);
    field_schema.set_nullable(true);
    auto field_meta =
        storage::FieldDataMeta{1, 2, 3, tags_fid.get(), field_schema};
    auto index_meta = storage::IndexMeta{3, tags_fid.get(), 4003, 4003};

    auto root_path = TestLocalPath + "/match_nested_sort_str";
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto built =
        std::make_unique<index::StringIndexSort>(ctx, /*is_nested_index=*/true);
    built->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(built->IsNestedIndex());

    auto binary_set = built->Serialize({});
    auto loaded = std::make_unique<index::StringIndexSort>(
        ctx, /*is_nested_index=*/false);
    loaded->Load(binary_set, {});
    ASSERT_TRUE(loaded->IsNestedIndex());

    LoadIndexInfo info{};
    info.field_id = tags_fid.get();
    info.index_params = GenIndexParams(loaded.get());
    info.cache_index =
        CreateTestCacheIndex("scalar_tags_sort", std::move(loaded));
    seg->LoadIndex(info);

    boost::filesystem::remove_all(root_path);
}

}  // namespace

// MATCH execution end-to-end through a nested ScalarIndexSort (STL_SORT).
// IndexFactory routes non-INVERTED/BITMAP nested array index builds to
// CreateNestedIndexScalarIndexSort, but no execution test covered a sort
// index serving the element predicate. Same layout and hand-computed
// expectations as NestedBitmapIndexNullable, cross-checked against an
// index-free brute-force segment.
TEST(ScalarArrayMatchIndex, NestedScalarSortIndexNullable) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);

    // Against the element predicate $ > 100:
    //   row0 = NULL, row1 = [] (empty, non-null), row2 = one match,
    //   row3 = all match, row4 = zero match.
    const std::vector<std::vector<int64_t>> rows = {
        {}, {}, {10, 200}, {150, 250}, {10, 20}};
    const std::vector<bool> valid = {false, true, true, true, true};

    auto indexed =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    LoadScalarInt64ArrayNestedSortIndex(indexed.get(), scores_fid, rows, valid);
    auto brute =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);

    const std::vector<std::pair<std::string, std::set<int64_t>>> cases = {
        {"MATCH_ANY(scores, $ > 100)", {2, 3}},
        {"MATCH_ALL(scores, $ > 100)", {1, 3}},
        {"MATCH_LEAST(scores, $ > 100, threshold=1)", {2, 3}},
        {"MATCH_MOST(scores, $ > 100, threshold=0)", {1, 4}},
        {"MATCH_EXACT(scores, $ > 100, threshold=0)", {1, 4}},
        // Term IN / NOT IN through the sort index as well.
        {"MATCH_ANY(scores, $ in [10, 250])", {2, 3, 4}},
        {"MATCH_ALL(scores, $ not in [10])", {1, 3}},
    };
    for (const auto& [expr, expected] : cases) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, expr);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, expr);
        EXPECT_EQ(ri, expected) << "sort-index path wrong for: " << expr;
        EXPECT_EQ(rb, expected) << "brute-force path wrong for: " << expr;
        EXPECT_EQ(ri, rb) << "sort index vs brute-force mismatch for: " << expr;
    }
}

// Differential consistency for the nested ScalarIndexSort on randomized
// nullable INT64 array data: the brute-force segment is the oracle. Mirrors
// Int64IndexedEqualsBruteForce (nested inverted) for the sort-index engine.
TEST(ScalarArrayMatchIndexConsistency, Int64SortIndexedEqualsBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto scores_fid =
        schema->AddDebugArrayField("scores", DataType::INT64, true);

    const int64_t N = 300;
    std::default_random_engine rng(20260709);
    std::uniform_int_distribution<int> len_dist(0, 5);   // 0 => empty arrays
    std::uniform_int_distribution<int> val_dist(0, 9);   // small value domain
    std::uniform_int_distribution<int> null_dist(0, 9);  // ~10% null rows

    std::vector<std::vector<int64_t>> rows(N);
    std::vector<bool> valid(N, true);
    for (int64_t i = 0; i < N; ++i) {
        if (null_dist(rng) == 0) {
            valid[i] = false;
            continue;
        }
        int len = len_dist(rng);
        for (int j = 0; j < len; ++j) {
            rows[i].push_back(val_dist(rng));
        }
    }

    auto indexed =
        MakeSealedScalarInt64Array(schema, pk_fid, scores_fid, rows, valid);
    LoadScalarInt64ArrayNestedSortIndex(indexed.get(), scores_fid, rows, valid);
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
        "MATCH_ANY(scores, 2 < $ < 8)",
        "MATCH_ANY(scores, $ in [1, 5, 9])",
        "MATCH_ALL(scores, $ not in [0, 9])",
    };
    for (const auto& e : exprs) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, e);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, e);
        EXPECT_EQ(ri, rb) << "sort index vs brute-force mismatch for: " << e;
    }
}

// Differential consistency for the nested StringIndexSort on randomized
// nullable VARCHAR array data, including LIKE patterns (prefix routes to the
// index when supported; suffix/infix fall back to brute force -- either way
// results must equal the index-free oracle).
TEST(ScalarArrayMatchIndexConsistency, VarCharSortIndexedEqualsBruteForce) {
    auto schema = std::make_shared<Schema>();
    auto pk_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk_fid);
    auto tags_fid = schema->AddDebugArrayField("tags", DataType::VARCHAR, true);

    const int64_t N = 300;
    const std::vector<std::string> vocab = {"ab", "ac", "ba", "bc", "ca"};
    std::default_random_engine rng(20260710);
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
    LoadScalarVarCharArrayNestedStringSortIndex(
        indexed.get(), tags_fid, rows, valid);
    auto brute = MakeSealedScalarVarCharArray(
        schema, pk_fid, tags_fid, rows, valid, /*exclude_tags_raw=*/false);

    const std::vector<std::string> exprs = {
        R"(MATCH_ANY(tags, $ == "ab"))",
        R"(MATCH_ALL(tags, $ != "ca"))",
        R"(MATCH_LEAST(tags, $ == "ab", threshold=2))",
        R"(MATCH_MOST(tags, $ == "bc", threshold=1))",
        R"(MATCH_EXACT(tags, $ == "ac", threshold=0))",
        R"(MATCH_ANY(tags, $ in ["ab", "bc"]))",
        R"(MATCH_ALL(tags, $ not in ["ca"]))",
        R"(MATCH_ANY(tags, $ like "a%"))",
        R"(MATCH_ALL(tags, $ like "a%"))",
        R"(MATCH_ANY(tags, $ like "%c"))",
        R"(MATCH_ANY(tags, $ like "%b%"))",
        R"(MATCH_ANY(tags, "a" < $ < "c"))",
    };
    for (const auto& e : exprs) {
        auto ri = RetrieveMatchRowsLocal(indexed.get(), *schema, schema, e);
        auto rb = RetrieveMatchRowsLocal(brute.get(), *schema, schema, e);
        EXPECT_EQ(ri, rb) << "string-sort index vs brute-force mismatch for: "
                          << e;
    }
}
