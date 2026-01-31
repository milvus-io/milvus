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

#include <gtest/gtest.h>
#include <boost/container/vector.hpp>
#include <iostream>
#include <random>
#include <set>
#include <unordered_set>

#include "cachinglayer/Manager.h"
#include "common/Common.h"
#include "common/Schema.h"
#include "index/InvertedIndexTantivy.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
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
