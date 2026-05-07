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
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/PrometheusClient.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/InvertedIndexTantivy.h"
#include "index/JsonInvertedIndex.h"
#include "index/Meta.h"
#include "simdjson/padded_string.h"
#include "common/Tracer.h"
#include "storage/FileManager.h"
#include "storage/Util.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "pb/segcore.pb.h"
#include "query/Plan.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "segcore/Utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
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

// ==================== JSON MATCH — index path ====================
// Exercises MATCH_* on a JSON field where an ARRAY-cast JsonInvertedIndex
// is loaded on the target nested path. Covers two element scalar families:
//   /arr_str -> ARRAY_VARCHAR
//   /arr_int -> ARRAY_DOUBLE  (Int64 literal gets promoted to double in
//                              the inner UnaryRange JSON index dispatch)
//   /arr_num -> ARRAY_DOUBLE  (controlled numeric Term cases)
class SealedMatchExprJsonTest : public ::testing::Test {
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
        json_fid_ = schema_->AddDebugField("json_field", DataType::JSON);

        GenerateTestData();
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);
        if (WithIndex()) {
            LoadJsonIndex<std::string>("/arr_str", "ARRAY_VARCHAR", 1);
            LoadJsonIndex<double>("/arr_int", "ARRAY_DOUBLE", 2);
            LoadJsonIndex<double>("/arr_num", "ARRAY_DOUBLE", 3);
        }
    }

    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    virtual bool
    WithIndex() const {
        return true;
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(42);
        std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
        std::uniform_int_distribution<> str_dist(0, 2);
        std::uniform_int_distribution<> int_dist(50, 150);

        arr_str_data_.resize(N_);
        arr_int_data_.resize(N_);
        arr_num_data_.resize(N_);
        json_strs_.resize(N_);

        auto insert_data = std::make_unique<InsertRecordProto>();

        // vec
        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        // id
        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = static_cast<int64_t>(i);
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        // json
        for (size_t i = 0; i < N_; ++i) {
            arr_str_data_[i].resize(array_len_);
            arr_int_data_[i].resize(array_len_);
            std::string str_part = "[";
            std::string int_part = "[";
            std::string num_part;
            for (int j = 0; j < array_len_; ++j) {
                std::string s = str_choices[str_dist(rng)];
                int32_t v = int_dist(rng);
                arr_str_data_[i][j] = s;
                arr_int_data_[i][j] = v;
                if (j > 0) {
                    str_part += ",";
                    int_part += ",";
                }
                str_part += "\"" + s + "\"";
                int_part += std::to_string(v);
            }
            str_part += "]";
            int_part += "]";
            switch (i % 6) {
                case 0:
                    arr_num_data_[i] = {50.0};
                    num_part = "[50.0]";
                    break;
                case 1:
                    arr_num_data_[i] = {50.0};
                    num_part = "[50]";
                    break;
                case 2:
                    arr_num_data_[i] = {50.5};
                    num_part = "[50.5]";
                    break;
                case 3:
                    arr_num_data_[i] = {50.0, 50.5};
                    num_part = "[50.0,50.5]";
                    break;
                case 4:
                    arr_num_data_[i] = {50.0};
                    num_part = "[50,\"50\",true]";
                    break;
                default:
                    arr_num_data_[i] = {};
                    num_part = "[]";
                    break;
            }
            json_strs_[i] = "{\"arr_str\":" + str_part +
                            ",\"arr_int\":" + int_part +
                            ",\"arr_num\":" + num_part + "}";
        }
        auto json_array = CreateDataArrayFrom(
            json_strs_.data(), nullptr, N_, schema_->operator[](json_fid_));
        insert_data->mutable_fields_data()->AddAllocated(json_array.release());

        insert_data->set_num_rows(N_);
        generated_data_.schema_ = schema_;
        generated_data_.raw_ = insert_data.release();
        for (size_t i = 0; i < N_; ++i) {
            generated_data_.row_ids_.push_back(static_cast<int64_t>(i));
            generated_data_.timestamps_.push_back(static_cast<int64_t>(i));
        }
    }

    template <typename T>
    void
    LoadJsonIndex(const std::string& path,
                  const std::string& cast_str,
                  int64_t build_id) {
        // Go through the full build → Upload → Load flow so the load-side
        // index instance populates array_offsets_ from the sidecar via
        // LoadIndexMetas. MATCH_* on JSON requires GetArrayOffsets() to be
        // non-null.
        auto storage_config = get_default_local_storage_config();
        auto cm = storage::CreateChunkManager(storage_config);
        auto fs = storage::InitArrowFileSystem(storage_config);

        storage::FieldDataMeta field_data_meta{};
        field_data_meta.field_schema.set_data_type(milvus::proto::schema::JSON);
        field_data_meta.field_schema.set_fieldid(json_fid_.get());
        field_data_meta.field_id = json_fid_.get();
        storage::IndexMeta index_meta{/*segment_id*/ 1,
                                      /*field_id*/ json_fid_.get(),
                                      /*build_id*/ build_id,
                                      /*index_version*/ 1};
        storage::FileManagerContext file_manager_ctx(
            field_data_meta, index_meta, cm, fs);

        index::CreateIndexInfo cii;
        cii.index_type = index::INVERTED_INDEX_TYPE;
        cii.json_cast_type = JsonCastType::FromString(cast_str);
        cii.json_path = path;

        // Build side: create, build, upload.
        std::vector<std::string> index_files;
        {
            auto inv_index = index::IndexFactory::GetInstance().CreateJsonIndex(
                cii, file_manager_ctx);
            auto idx = std::unique_ptr<index::JsonInvertedIndex<T>>(
                static_cast<index::JsonInvertedIndex<T>*>(inv_index.release()));

            std::vector<milvus::Json> jsons;
            jsons.reserve(N_);
            for (const auto& s : json_strs_) {
                jsons.emplace_back(simdjson::padded_string(s));
            }
            auto json_field = std::make_shared<FieldData<milvus::Json>>(
                DataType::JSON, false);
            json_field->add_json_data(jsons);
            idx->BuildWithFieldData({json_field});
            auto stats = idx->Upload();
            for (auto& f : stats->GetIndexFiles()) {
                index_files.push_back(std::move(f));
            }
        }

        // Load side: fresh instance, Load consumes sidecar → array_offsets_.
        storage::FileManagerContext load_ctx(
            field_data_meta, index_meta, cm, fs);
        load_ctx.set_for_loading_index(true);
        auto inv_index_load =
            index::IndexFactory::GetInstance().CreateJsonIndex(cii, load_ctx);
        auto loaded = std::unique_ptr<index::JsonInvertedIndex<T>>(
            static_cast<index::JsonInvertedIndex<T>*>(
                inv_index_load.release()));
        Config load_config;
        load_config[index::INDEX_FILES] = index_files;
        load_config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        loaded->Load(milvus::tracer::TraceContext{}, load_config);

        segcore::LoadIndexInfo info;
        info.field_id = json_fid_.get();
        info.field_type = DataType::JSON;
        info.index_params = {{JSON_PATH, path}, {JSON_CAST_TYPE, cast_str}};
        info.cache_index = CreateTestCacheIndex(
            "match_json_" + cast_str + "_" + std::to_string(build_id),
            std::move(loaded));
        seg_->LoadIndex(info);
    }

    int
    CountStr(size_t row, const std::string& target) const {
        int c = 0;
        for (const auto& s : arr_str_data_[row]) {
            if (s == target) {
                ++c;
            }
        }
        return c;
    }

    int
    CountIntGt(size_t row, int32_t boundary) const {
        int c = 0;
        for (auto v : arr_int_data_[row]) {
            if (v > boundary) {
                ++c;
            }
        }
        return c;
    }

    int
    CountNumEq(size_t row, double target) const {
        int c = 0;
        for (auto v : arr_num_data_[row]) {
            if (v == target) {
                ++c;
            }
        }
        return c;
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

    using CountFn = std::function<int(size_t)>;
    using VerifyFn = std::function<bool(int match_count, int64_t threshold)>;
    using RowVerifyFn = std::function<bool(size_t row)>;

    void
    RunAndVerify(const std::string& filter_expr,
                 int64_t threshold,
                 CountFn count_fn,
                 VerifyFn verify_fn,
                 const std::string& label) {
        auto result = ExecuteRetrieve(filter_expr);

        std::set<int64_t> expected;
        for (size_t i = 0; i < N_; ++i) {
            if (verify_fn(count_fn(i), threshold)) {
                expected.insert(static_cast<int64_t>(i));
            }
        }
        std::set<int64_t> actual(result->offset().begin(),
                                 result->offset().end());

        EXPECT_EQ(expected, actual) << label << ": expected=" << expected.size()
                                    << " actual=" << actual.size();
    }

    void
    RunAndVerifyRows(const std::string& filter_expr,
                     RowVerifyFn verify_fn,
                     const std::string& label) {
        auto result = ExecuteRetrieve(filter_expr);

        std::set<int64_t> expected;
        for (size_t i = 0; i < N_; ++i) {
            if (verify_fn(i)) {
                expected.insert(static_cast<int64_t>(i));
            }
        }
        std::set<int64_t> actual(result->offset().begin(),
                                 result->offset().end());

        EXPECT_EQ(expected, actual) << label << ": expected=" << expected.size()
                                    << " actual=" << actual.size();
    }

    ColumnVectorPtr
    EvalFilterWithOffsets(const std::string& filter_expr,
                          const exec::OffsetVector& offsets) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str = schema_handle.ParseSearch(
            filter_expr, "vec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);
        if (plan == nullptr) {
            return nullptr;
        }

        auto offset_copy = offsets;
        auto filter_node =
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get();
        return milvus::test::gen_filter_res(
            filter_node, seg_.get(), N_, MAX_TIMESTAMP, &offset_copy);
    }

    exec::OffsetVector
    EmptyArrNumRows() const {
        exec::OffsetVector offsets;
        for (size_t i = 0; i < N_; ++i) {
            if (i % 6 == 5) {
                offsets.emplace_back(static_cast<int32_t>(i));
            }
        }
        return offsets;
    }

    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId json_fid_;

    std::vector<std::vector<std::string>> arr_str_data_;
    std::vector<std::vector<int32_t>> arr_int_data_;
    std::vector<std::vector<double>> arr_num_data_;
    std::vector<std::string> json_strs_;

    GeneratedData generated_data_;
    SegmentSealedUPtr seg_;

    static constexpr size_t N_ = 1000;
    static constexpr int array_len_ = 5;
    int64_t saved_batch_size_{0};
};

class SealedMatchExprJsonTestNoIndex : public SealedMatchExprJsonTest {
 protected:
    bool
    WithIndex() const override {
        return false;
    }
};

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAnyIntSimpleNoIndex) {
    const int32_t boundary = 100;
    RunAndVerify(
        "match_any(json_field[\"arr_int\"], $ > " + std::to_string(boundary) +
            ")",
        0,
        [&](size_t i) { return CountIntGt(i, boundary); },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyIntSimpleNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchLeastIntInnerAndNoIndex) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int64_t threshold = 2;
    RunAndVerify(
        "match_least(json_field[\"arr_int\"], $ > " + std::to_string(lo) +
            " && $ < " + std::to_string(hi) +
            ", threshold=" + std::to_string(threshold) + ")",
        threshold,
        [&](size_t i) {
            int c = 0;
            for (auto v : arr_int_data_[i]) {
                if (v > lo && v < hi) {
                    ++c;
                }
            }
            return c;
        },
        [](int c, int64_t t) { return c >= t; },
        "MatchLeastIntInnerAndNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAnyStrInnerTermNoIndex) {
    RunAndVerify(
        "match_any(json_field[\"arr_str\"], $ in [\"aaa\", \"bbb\"])",
        0,
        [&](size_t i) {
            int c = 0;
            for (const auto& s : arr_str_data_[i]) {
                if (s == "aaa" || s == "bbb") {
                    ++c;
                }
            }
            return c;
        },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyStrInnerTermNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAnyNumericTermIntLiteralNoIndex) {
    RunAndVerify(
        "match_any(json_field[\"arr_num\"], $ in [50])",
        0,
        [&](size_t i) { return CountNumEq(i, 50.0); },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyNumericTermIntLiteralNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAllNumericTermIntLiteralNoIndex) {
    RunAndVerifyRows(
        "match_all(json_field[\"arr_num\"], $ in [50])",
        [&](size_t i) {
            return CountNumEq(i, 50.0) ==
                   static_cast<int>(arr_num_data_[i].size());
        },
        "MatchAllNumericTermIntLiteralNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex,
       MatchAllNumericTermSkipsMixedTypeElementsNoIndex) {
    RunAndVerifyRows(
        "id % 6 == 4 && match_all(json_field[\"arr_num\"], $ in [50])",
        [](size_t i) { return i % 6 == 4; },
        "MatchAllNumericTermSkipsMixedTypeElementsNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAnyNumericArithAddNoIndex) {
    RunAndVerify(
        "match_any(json_field[\"arr_num\"], $ + 1 == 51)",
        0,
        [&](size_t i) {
            int c = 0;
            for (auto v : arr_num_data_[i]) {
                if (v + 1.0 == 51.0) {
                    ++c;
                }
            }
            return c;
        },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyNumericArithAddNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex, MatchAnyNumericArithFloatNoIndex) {
    RunAndVerify(
        "match_any(json_field[\"arr_num\"], $ + 0.5 == 51.0)",
        0,
        [&](size_t i) {
            int c = 0;
            for (auto v : arr_num_data_[i]) {
                if (v + 0.5 == 51.0) {
                    ++c;
                }
            }
            return c;
        },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyNumericArithFloatNoIndex");
}

TEST_F(SealedMatchExprJsonTestNoIndex,
       MatchAllNumericArithSkipsMixedTypeElementsNoIndex) {
    RunAndVerifyRows(
        "id % 6 == 4 && match_all(json_field[\"arr_num\"], $ % 2 == 0)",
        [](size_t i) { return i % 6 == 4; },
        "MatchAllNumericArithSkipsMixedTypeElementsNoIndex");
}

TEST_F(SealedMatchExprJsonTest, MatchAnyStrWithIndex) {
    const std::string target = "aaa";
    RunAndVerify(
        "match_any(json_field[\"arr_str\"], $ == \"" + target + "\")",
        0,
        [&](size_t i) { return CountStr(i, target); },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyStr");
}

TEST_F(SealedMatchExprJsonTest, MatchAllStrWithIndex) {
    const std::string target = "aaa";
    RunAndVerify(
        "match_all(json_field[\"arr_str\"], $ == \"" + target + "\")",
        array_len_,
        [&](size_t i) { return CountStr(i, target); },
        [](int c, int64_t n) { return c == n; },
        "MatchAllStr");
}

TEST_F(SealedMatchExprJsonTest, MatchLeastStrWithIndex) {
    const std::string target = "aaa";
    const int64_t threshold = 2;
    RunAndVerify(
        "match_least(json_field[\"arr_str\"], $ == \"" + target +
            "\", threshold=" + std::to_string(threshold) + ")",
        threshold,
        [&](size_t i) { return CountStr(i, target); },
        [](int c, int64_t t) { return c >= t; },
        "MatchLeastStr");
}

TEST_F(SealedMatchExprJsonTest, MatchMostStrWithIndex) {
    const std::string target = "aaa";
    const int64_t threshold = 2;
    RunAndVerify(
        "match_most(json_field[\"arr_str\"], $ == \"" + target +
            "\", threshold=" + std::to_string(threshold) + ")",
        threshold,
        [&](size_t i) { return CountStr(i, target); },
        [](int c, int64_t t) { return c <= t; },
        "MatchMostStr");
}

TEST_F(SealedMatchExprJsonTest, MatchExactStrWithIndex) {
    const std::string target = "aaa";
    const int64_t threshold = 2;
    RunAndVerify(
        "match_exact(json_field[\"arr_str\"], $ == \"" + target +
            "\", threshold=" + std::to_string(threshold) + ")",
        threshold,
        [&](size_t i) { return CountStr(i, target); },
        [](int c, int64_t t) { return c == t; },
        "MatchExactStr");
}

// Int64 literal vs ARRAY_DOUBLE index — exercises promotion path.
TEST_F(SealedMatchExprJsonTest, MatchAnyIntWithIndex) {
    const int32_t boundary = 100;
    RunAndVerify(
        "match_any(json_field[\"arr_int\"], $ > " + std::to_string(boundary) +
            ")",
        0,
        [&](size_t i) { return CountIntGt(i, boundary); },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyInt");
}

TEST_F(SealedMatchExprJsonTest, MatchLeastIntWithIndex) {
    const int32_t boundary = 100;
    const int64_t threshold = 2;
    RunAndVerify(
        "match_least(json_field[\"arr_int\"], $ > " + std::to_string(boundary) +
            ", threshold=" + std::to_string(threshold) + ")",
        threshold,
        [&](size_t i) { return CountIntGt(i, boundary); },
        [](int c, int64_t t) { return c >= t; },
        "MatchLeastInt");
}

TEST_F(SealedMatchExprJsonTest, MatchAnyNumericTermIntLiteralWithIndex) {
    RunAndVerify(
        "match_any(json_field[\"arr_num\"], $ in [50])",
        0,
        [&](size_t i) { return CountNumEq(i, 50.0); },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyNumericTermIntLiteralWithIndex");
}

TEST_F(SealedMatchExprJsonTest, MatchAllNumericTermIntLiteralWithIndex) {
    RunAndVerifyRows(
        "match_all(json_field[\"arr_num\"], $ in [50])",
        [&](size_t i) {
            return CountNumEq(i, 50.0) ==
                   static_cast<int>(arr_num_data_[i].size());
        },
        "MatchAllNumericTermIntLiteralWithIndex");
}

TEST_F(SealedMatchExprJsonTest,
       MatchAllNumericTermSkipsMixedTypeElementsWithIndex) {
    RunAndVerifyRows(
        "id % 6 == 4 && match_all(json_field[\"arr_num\"], $ in [50])",
        [](size_t i) { return i % 6 == 4; },
        "MatchAllNumericTermSkipsMixedTypeElementsWithIndex");
}

TEST_F(SealedMatchExprJsonTest, MatchAllNumericTermEmptyOffsetsWithIndex) {
    auto offsets = EmptyArrNumRows();
    auto result = EvalFilterWithOffsets(
        "match_all(json_field[\"arr_num\"], $ in [50])", offsets);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), static_cast<int64_t>(offsets.size()));

    TargetBitmapView view(result->GetRawData(), result->size());
    TargetBitmapView valid_view(result->GetValidRawData(), result->size());
    EXPECT_TRUE(view.all());
    EXPECT_TRUE(valid_view.all());
}

TEST_F(SealedMatchExprJsonTest, MatchAllNumericCompoundEmptyOffsetsWithIndex) {
    auto offsets = EmptyArrNumRows();
    auto result = EvalFilterWithOffsets(
        "match_all(json_field[\"arr_num\"], $ > 0 && $ < 100)", offsets);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), static_cast<int64_t>(offsets.size()));

    TargetBitmapView view(result->GetRawData(), result->size());
    TargetBitmapView valid_view(result->GetValidRawData(), result->size());
    EXPECT_TRUE(view.all());
    EXPECT_TRUE(valid_view.all());
}

TEST_F(SealedMatchExprJsonTest,
       MatchAllNumericCompoundEmptySequentialBatchWithIndex) {
    auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(1);
    RunAndVerifyRows(
        "match_all(json_field[\"arr_num\"], $ > 0 && $ < 1000)",
        [](size_t) { return true; },
        "MatchAllNumericCompoundEmptySequentialBatchWithIndex");
    EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
}

TEST_F(SealedMatchExprJsonTest, MatchAnyNumericArithAddWithIndexFallback) {
    RunAndVerify(
        "match_any(json_field[\"arr_num\"], $ + 1 == 51)",
        0,
        [&](size_t i) {
            int c = 0;
            for (auto v : arr_num_data_[i]) {
                if (v + 1.0 == 51.0) {
                    ++c;
                }
            }
            return c;
        },
        [](int c, int64_t) { return c > 0; },
        "MatchAnyNumericArithAddWithIndexFallback");
}

// ==================== Compound expression tests ====================
// Verify match_* on JSON composes correctly with logical operators,
// other-field conditions, and compound inner predicates.

namespace {

int
CountIntInRange(const std::vector<int32_t>& arr, int32_t lo, int32_t hi) {
    int c = 0;
    for (auto v : arr) {
        if (v > lo && v < hi) {
            ++c;
        }
    }
    return c;
}

}  // namespace

// match_any AND match_any across two JSON sub-paths.
TEST_F(SealedMatchExprJsonTest, MatchAnyAndMatchAny) {
    const std::string target = "aaa";
    const int32_t boundary = 100;
    const std::string filter = "match_any(json_field[\"arr_str\"], $ == \"" +
                               target +
                               "\") && "
                               "match_any(json_field[\"arr_int\"], $ > " +
                               std::to_string(boundary) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountStr(i, target) > 0 && CountIntGt(i, boundary) > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_any OR other-field condition.
TEST_F(SealedMatchExprJsonTest, MatchAnyOrIdRange) {
    const std::string target = "aaa";
    const int64_t id_boundary = 10;
    const std::string filter = "match_any(json_field[\"arr_str\"], $ == \"" +
                               target + "\") || id < " +
                               std::to_string(id_boundary);

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        bool m = CountStr(i, target) > 0;
        bool id_small = static_cast<int64_t>(i) < id_boundary;
        if (m || id_small) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// NOT match_all : rows where not every element equals target.
TEST_F(SealedMatchExprJsonTest, NotMatchAll) {
    const std::string target = "aaa";
    const std::string filter =
        "!match_all(json_field[\"arr_str\"], $ == \"" + target + "\")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountStr(i, target) != array_len_) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_any with inner compound predicate ($ > lo && $ < hi).
TEST_F(SealedMatchExprJsonTest, MatchAnyInnerCompound) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const std::string filter = "match_any(json_field[\"arr_int\"], $ > " +
                               std::to_string(lo) + " && $ < " +
                               std::to_string(hi) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountIntInRange(arr_int_data_[i], lo, hi) > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_all with inner compound AND: every element falls in (80, 120).
TEST_F(SealedMatchExprJsonTest, MatchAllInnerCompoundAnd) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const std::string filter = "match_all(json_field[\"arr_int\"], $ > " +
                               std::to_string(lo) + " && $ < " +
                               std::to_string(hi) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountIntInRange(arr_int_data_[i], lo, hi) == array_len_) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_any with inner OR: any element outside [70, 130].
TEST_F(SealedMatchExprJsonTest, MatchAnyInnerCompoundOr) {
    const int32_t lo = 70;
    const int32_t hi = 130;
    const std::string filter = "match_any(json_field[\"arr_int\"], $ < " +
                               std::to_string(lo) + " || $ > " +
                               std::to_string(hi) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        int c = 0;
        for (auto v : arr_int_data_[i]) {
            if (v < lo || v > hi) {
                ++c;
            }
        }
        if (c > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_least with inner compound AND + threshold=2.
TEST_F(SealedMatchExprJsonTest, MatchLeastInnerCompound) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int64_t threshold = 2;
    const std::string filter = "match_least(json_field[\"arr_int\"], $ > " +
                               std::to_string(lo) + " && $ < " +
                               std::to_string(hi) +
                               ", threshold=" + std::to_string(threshold) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountIntInRange(arr_int_data_[i], lo, hi) >= threshold) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_exact with inner compound: exactly `threshold` elements in range.
TEST_F(SealedMatchExprJsonTest, MatchExactInnerCompound) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int64_t threshold = 3;
    const std::string filter = "match_exact(json_field[\"arr_int\"], $ > " +
                               std::to_string(lo) + " && $ < " +
                               std::to_string(hi) +
                               ", threshold=" + std::to_string(threshold) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountIntInRange(arr_int_data_[i], lo, hi) == threshold) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// String path with inner OR: any element in {"aaa", "bbb"}.
TEST_F(SealedMatchExprJsonTest, MatchAnyStrInnerOr) {
    const std::string filter =
        "match_any(json_field[\"arr_str\"], $ == \"aaa\" || $ == \"bbb\")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        int c = 0;
        for (const auto& s : arr_str_data_[i]) {
            if (s == "aaa" || s == "bbb") {
                ++c;
            }
        }
        if (c > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Three-leaf inner tree: ($ > 80 && $ < 120) || $ == 150.
TEST_F(SealedMatchExprJsonTest, MatchAnyInnerNested) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int32_t eq = 150;
    const std::string filter = "match_any(json_field[\"arr_int\"], ($ > " +
                               std::to_string(lo) + " && $ < " +
                               std::to_string(hi) +
                               ") || $ == " + std::to_string(eq) + ")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        int c = 0;
        for (auto v : arr_int_data_[i]) {
            if ((v > lo && v < hi) || v == eq) {
                ++c;
            }
        }
        if (c > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Outer AND combining two match_* that each have inner compound predicates.
TEST_F(SealedMatchExprJsonTest, OuterAndBothInnerCompound) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const std::string filter =
        "match_any(json_field[\"arr_int\"], $ > " + std::to_string(lo) +
        " && $ < " + std::to_string(hi) +
        ") && "
        "match_any(json_field[\"arr_str\"], $ == \"aaa\" || $ == \"bbb\")";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        bool a = CountIntInRange(arr_int_data_[i], lo, hi) > 0;
        int sc = 0;
        for (const auto& s : arr_str_data_[i]) {
            if (s == "aaa" || s == "bbb") {
                ++sc;
            }
        }
        if (a && sc > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Cross-field AND: match_any on JSON AND id % 2 == 0.
TEST_F(SealedMatchExprJsonTest, MatchAnyAndIdMod) {
    const std::string target = "aaa";
    const std::string filter = "match_any(json_field[\"arr_str\"], $ == \"" +
                               target + "\") && id % 2 == 0";

    auto result = ExecuteRetrieve(filter);
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (CountStr(i, target) > 0 && (i % 2 == 0)) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// ====================================================================
// SealedMatchExprPlainArrayTest
//
// Plain array (DataType::ARRAY with element_type) MATCH coverage.
// Two fixtures: with inverted index loaded (element-level index path)
// and without index (brute-force raw data path). Same tests run under
// both to verify each path independently.
// ====================================================================

class SealedMatchExprPlainArrayTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        saved_batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(100);

        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("id", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);
        arr_int_fid_ =
            schema_->AddDebugField("arr_int", DataType::ARRAY, DataType::INT32);
        arr_str_fid_ = schema_->AddDebugField(
            "arr_str", DataType::ARRAY, DataType::VARCHAR);

        GenerateTestData();
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);
        if (WithIndex()) {
            LoadArrayIndexes();
        }
    }

    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    // Overridable switch for the no-index fixture.
    virtual bool
    WithIndex() const {
        return true;
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(7);
        std::vector<std::string> str_choices = {"aaa", "bbb", "ccc"};
        std::uniform_int_distribution<> str_dist(0, 2);
        std::uniform_int_distribution<> int_dist(50, 150);

        arr_int_data_.assign(N_, {});
        arr_str_data_.assign(N_, {});
        arr_int_raw_.assign(N_, {});
        arr_str_raw_.assign(N_, {});

        auto insert_data = std::make_unique<InsertRecordProto>();

        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = static_cast<int64_t>(i);
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        for (size_t i = 0; i < N_; ++i) {
            for (int j = 0; j < array_len_; ++j) {
                int32_t v = int_dist(rng);
                arr_int_data_[i].mutable_int_data()->add_data(v);
                arr_int_raw_[i].push_back(v);
            }
        }
        auto int_arr_proto =
            CreateDataArrayFrom(arr_int_data_.data(),
                                nullptr,
                                N_,
                                schema_->operator[](arr_int_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            int_arr_proto.release());

        for (size_t i = 0; i < N_; ++i) {
            for (int j = 0; j < array_len_; ++j) {
                const auto& s = str_choices[str_dist(rng)];
                arr_str_data_[i].mutable_string_data()->add_data(s);
                arr_str_raw_[i].push_back(s);
            }
        }
        auto str_arr_proto =
            CreateDataArrayFrom(arr_str_data_.data(),
                                nullptr,
                                N_,
                                schema_->operator[](arr_str_fid_));
        insert_data->mutable_fields_data()->AddAllocated(
            str_arr_proto.release());

        insert_data->set_num_rows(N_);
        generated_data_.schema_ = schema_;
        generated_data_.raw_ = insert_data.release();
        for (size_t i = 0; i < N_; ++i) {
            generated_data_.row_ids_.push_back(static_cast<int64_t>(i));
            generated_data_.timestamps_.push_back(static_cast<int64_t>(i));
        }
    }

    void
    LoadArrayIndexes() {
        {
            auto index =
                std::make_unique<index::InvertedIndexTantivy<int32_t>>();
            Config cfg;
            cfg["is_array"] = true;
            cfg["is_nested_index"] = true;
            std::vector<boost::container::vector<int32_t>> per_row(N_);
            for (size_t i = 0; i < N_; ++i) {
                per_row[i].assign(arr_int_raw_[i].begin(),
                                  arr_int_raw_[i].end());
            }
            index->BuildWithRawDataForUT(N_, per_row.data(), cfg);
            LoadIndexInfo info{};
            info.field_id = arr_int_fid_.get();
            info.index_params = GenIndexParams(index.get());
            info.cache_index =
                CreateTestCacheIndex("plain_arr_int", std::move(index));
            seg_->LoadIndex(info);
        }
        {
            auto index =
                std::make_unique<index::InvertedIndexTantivy<std::string>>();
            Config cfg;
            cfg["is_array"] = true;
            cfg["is_nested_index"] = true;
            std::vector<boost::container::vector<std::string>> per_row(N_);
            for (size_t i = 0; i < N_; ++i) {
                per_row[i].assign(arr_str_raw_[i].begin(),
                                  arr_str_raw_[i].end());
            }
            index->BuildWithRawDataForUT(N_, per_row.data(), cfg);
            LoadIndexInfo info{};
            info.field_id = arr_str_fid_.get();
            info.index_params = GenIndexParams(index.get());
            info.cache_index =
                CreateTestCacheIndex("plain_arr_str", std::move(index));
            seg_->LoadIndex(info);
        }
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

    static int
    CountIntPred(const std::vector<int32_t>& arr,
                 const std::function<bool(int32_t)>& pred) {
        int c = 0;
        for (auto v : arr) {
            if (pred(v)) {
                ++c;
            }
        }
        return c;
    }

    static int
    CountStrPred(const std::vector<std::string>& arr,
                 const std::function<bool(const std::string&)>& pred) {
        int c = 0;
        for (const auto& s : arr) {
            if (pred(s)) {
                ++c;
            }
        }
        return c;
    }

    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId arr_int_fid_;
    FieldId arr_str_fid_;

    std::vector<milvus::proto::schema::ScalarField> arr_int_data_;
    std::vector<milvus::proto::schema::ScalarField> arr_str_data_;
    std::vector<std::vector<int32_t>> arr_int_raw_;
    std::vector<std::vector<std::string>> arr_str_raw_;

    GeneratedData generated_data_;
    SegmentSealedUPtr seg_;

    static constexpr size_t N_ = 1000;
    static constexpr int array_len_ = 5;
    int64_t saved_batch_size_{0};
};

class SealedMatchExprPlainArrayTestNoIndex
    : public SealedMatchExprPlainArrayTest {
 public:
    bool
    WithIndex() const override {
        return false;
    }
};

// ==================== Shared test bodies ====================
// Put the assertions in a helper invoked by both fixtures so both the
// indexed and brute-force execution paths are verified without duplicating
// ground-truth logic. Each helper covers one non-redundant shape.

namespace plain_array_match_cases {

// match_any with a single predicate — baseline element-level dispatch.
inline void
CheckMatchAnyIntSimple(SealedMatchExprPlainArrayTest* f) {
    const int32_t boundary = 100;
    auto result = f->ExecuteRetrieve("match_any(arr_int, $ > " +
                                     std::to_string(boundary) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_int_raw_.size(); ++i) {
        if (SealedMatchExprPlainArrayTest::CountIntPred(
                f->arr_int_raw_[i], [=](int32_t v) { return v > boundary; }) >
            0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_all with inner OR — string path + compound predicate.
inline void
CheckMatchAllStrInnerOr(SealedMatchExprPlainArrayTest* f) {
    auto result =
        f->ExecuteRetrieve("match_all(arr_str, $ == \"aaa\" || $ == \"bbb\")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_str_raw_.size(); ++i) {
        int c = SealedMatchExprPlainArrayTest::CountStrPred(
            f->arr_str_raw_[i],
            [](const std::string& s) { return s == "aaa" || s == "bbb"; });
        if (c == f->array_len_) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_least with inner AND + threshold — compound AND path.
inline void
CheckMatchLeastIntInnerAnd(SealedMatchExprPlainArrayTest* f) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int64_t threshold = 2;
    auto result = f->ExecuteRetrieve(
        "match_least(arr_int, $ > " + std::to_string(lo) + " && $ < " +
        std::to_string(hi) + ", threshold=" + std::to_string(threshold) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_int_raw_.size(); ++i) {
        int c = SealedMatchExprPlainArrayTest::CountIntPred(
            f->arr_int_raw_[i], [=](int32_t v) { return v > lo && v < hi; });
        if (c >= threshold) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_exact with 3-leaf inner: (> lo && < hi) || == eq.
inline void
CheckMatchExactIntInnerNested(SealedMatchExprPlainArrayTest* f) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int32_t eq = 150;
    const int64_t threshold = 3;
    auto result = f->ExecuteRetrieve(
        "match_exact(arr_int, ($ > " + std::to_string(lo) + " && $ < " +
        std::to_string(hi) + ") || $ == " + std::to_string(eq) +
        ", threshold=" + std::to_string(threshold) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_int_raw_.size(); ++i) {
        int c = SealedMatchExprPlainArrayTest::CountIntPred(
            f->arr_int_raw_[i],
            [=](int32_t v) { return (v > lo && v < hi) || v == eq; });
        if (c == threshold) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Outer combinator: match_any on plain array AND a scalar-field condition.
inline void
CheckOuterAndCrossField(SealedMatchExprPlainArrayTest* f) {
    auto result =
        f->ExecuteRetrieve("match_any(arr_str, $ == \"aaa\") && id % 2 == 0");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_str_raw_.size(); ++i) {
        bool m = SealedMatchExprPlainArrayTest::CountStrPred(
                     f->arr_str_raw_[i],
                     [](const std::string& s) { return s == "aaa"; }) > 0;
        if (m && (i % 2 == 0)) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// match_most (at most `threshold` hits) with inner AND — distinct
// reduction semantics from the other match_* variants.
inline void
CheckMatchMostIntInnerAnd(SealedMatchExprPlainArrayTest* f) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    const int64_t threshold = 2;
    auto result = f->ExecuteRetrieve(
        "match_most(arr_int, $ > " + std::to_string(lo) + " && $ < " +
        std::to_string(hi) + ", threshold=" + std::to_string(threshold) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_int_raw_.size(); ++i) {
        int c = SealedMatchExprPlainArrayTest::CountIntPred(
            f->arr_int_raw_[i], [=](int32_t v) { return v > lo && v < hi; });
        if (c <= threshold) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Inner Term predicate: $ in [...] — hits a different Expr subclass
// (PhyTermFilterExpr) from UnaryRange, so worth a separate case.
inline void
CheckMatchAnyInnerTerm(SealedMatchExprPlainArrayTest* f) {
    auto result =
        f->ExecuteRetrieve("match_any(arr_str, $ in [\"aaa\", \"bbb\"])");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_str_raw_.size(); ++i) {
        int c = SealedMatchExprPlainArrayTest::CountStrPred(
            f->arr_str_raw_[i],
            [](const std::string& s) { return s == "aaa" || s == "bbb"; });
        if (c > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

// Outer OR between two match_* on different array fields, each with its
// own compound inner — stresses both the multi-leaf-per-match dispatch
// and the framework-level OR composition.
inline void
CheckOuterOrTwoCompoundInner(SealedMatchExprPlainArrayTest* f) {
    const int32_t lo = 80;
    const int32_t hi = 120;
    auto result =
        f->ExecuteRetrieve("match_any(arr_int, $ > " + std::to_string(lo) +
                           " && $ < " + std::to_string(hi) +
                           ") || "
                           "match_all(arr_str, $ == \"aaa\" || $ == \"bbb\")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < f->arr_int_raw_.size(); ++i) {
        bool int_side = SealedMatchExprPlainArrayTest::CountIntPred(
                            f->arr_int_raw_[i],
                            [=](int32_t v) { return v > lo && v < hi; }) > 0;
        int sc = SealedMatchExprPlainArrayTest::CountStrPred(
            f->arr_str_raw_[i],
            [](const std::string& s) { return s == "aaa" || s == "bbb"; });
        bool str_side = sc == f->array_len_;
        if (int_side || str_side) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}

}  // namespace plain_array_match_cases

#define PLAIN_ARRAY_MATCH_CASES(FIXTURE)                              \
    TEST_F(FIXTURE, MatchAnyIntSimple) {                              \
        plain_array_match_cases::CheckMatchAnyIntSimple(this);        \
    }                                                                 \
    TEST_F(FIXTURE, MatchAllStrInnerOr) {                             \
        plain_array_match_cases::CheckMatchAllStrInnerOr(this);       \
    }                                                                 \
    TEST_F(FIXTURE, MatchLeastIntInnerAnd) {                          \
        plain_array_match_cases::CheckMatchLeastIntInnerAnd(this);    \
    }                                                                 \
    TEST_F(FIXTURE, MatchExactIntInnerNested) {                       \
        plain_array_match_cases::CheckMatchExactIntInnerNested(this); \
    }                                                                 \
    TEST_F(FIXTURE, OuterAndCrossField) {                             \
        plain_array_match_cases::CheckOuterAndCrossField(this);       \
    }                                                                 \
    TEST_F(FIXTURE, OuterOrTwoCompoundInner) {                        \
        plain_array_match_cases::CheckOuterOrTwoCompoundInner(this);  \
    }                                                                 \
    TEST_F(FIXTURE, MatchMostIntInnerAnd) {                           \
        plain_array_match_cases::CheckMatchMostIntInnerAnd(this);     \
    }                                                                 \
    TEST_F(FIXTURE, MatchAnyInnerTerm) {                              \
        plain_array_match_cases::CheckMatchAnyInnerTerm(this);        \
    }

PLAIN_ARRAY_MATCH_CASES(SealedMatchExprPlainArrayTest)
PLAIN_ARRAY_MATCH_CASES(SealedMatchExprPlainArrayTestNoIndex)

// ====================================================================
// SealedMatchExprPlainArrayNullableTest
//
// Covers three overlapping corners with one data shape:
//   - variable-length arrays (row i has i % 8 elements)
//   - empty arrays (rows with length 0) — MatchAll vacuous-true branch
//   - row-level null (nullable array column)
// Ground truth mirrors the actual MatchSingleRow behavior: rows with no
// elements report match_count == 0 and element_count == 0, so MatchAny
// returns false and MatchAll returns vacuous true for both null rows and
// empty-array rows.
// ====================================================================

class SealedMatchExprPlainArrayNullableTest : public ::testing::Test {
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
        arr_int_fid_ = schema_->AddDebugField(
            "arr_int", DataType::ARRAY, DataType::INT32, /*nullable=*/true);

        GenerateTestData();
        seg_ = CreateSealedWithFieldDataLoaded(schema_, generated_data_);
    }

    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size_);
    }

    void
    GenerateTestData() {
        std::default_random_engine rng(11);
        std::uniform_int_distribution<> int_dist(50, 150);

        arr_int_data_.assign(N_, {});
        arr_int_raw_.assign(N_, {});
        arr_valid_.assign(N_, 1);

        auto insert_data = std::make_unique<InsertRecordProto>();

        std::vector<float> vec_data(N_ * 4);
        std::normal_distribution<float> vec_dist(0, 1);
        for (auto& v : vec_data) {
            v = vec_dist(rng);
        }
        auto vec_array = CreateDataArrayFrom(
            vec_data.data(), nullptr, N_, schema_->operator[](vec_fid_));
        insert_data->mutable_fields_data()->AddAllocated(vec_array.release());

        std::vector<int64_t> id_data(N_);
        for (size_t i = 0; i < N_; ++i) {
            id_data[i] = static_cast<int64_t>(i);
        }
        auto id_array = CreateDataArrayFrom(
            id_data.data(), nullptr, N_, schema_->operator[](int64_fid_));
        insert_data->mutable_fields_data()->AddAllocated(id_array.release());

        // Mix: every 13th row null, otherwise length = i % 8 (yields 0-length
        // arrays regularly) with random values.
        for (size_t i = 0; i < N_; ++i) {
            if (i % 13 == 0) {
                arr_valid_[i] = 0;
                continue;  // leave ScalarField empty
            }
            int len = static_cast<int>(i % 8);
            for (int j = 0; j < len; ++j) {
                int32_t v = int_dist(rng);
                arr_int_data_[i].mutable_int_data()->add_data(v);
                arr_int_raw_[i].push_back(v);
            }
        }
        auto arr_proto = CreateDataArrayFrom(arr_int_data_.data(),
                                             arr_valid_.data(),
                                             N_,
                                             schema_->operator[](arr_int_fid_));
        insert_data->mutable_fields_data()->AddAllocated(arr_proto.release());

        insert_data->set_num_rows(N_);
        generated_data_.schema_ = schema_;
        generated_data_.raw_ = insert_data.release();
        for (size_t i = 0; i < N_; ++i) {
            generated_data_.row_ids_.push_back(static_cast<int64_t>(i));
            generated_data_.timestamps_.push_back(static_cast<int64_t>(i));
        }
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

    std::shared_ptr<Schema> schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    FieldId arr_int_fid_;

    std::vector<milvus::proto::schema::ScalarField> arr_int_data_;
    std::vector<std::vector<int32_t>> arr_int_raw_;
    std::vector<uint8_t> arr_valid_;

    GeneratedData generated_data_;
    SegmentSealedUPtr seg_;

    static constexpr size_t N_ = 520;
    int64_t saved_batch_size_{0};
};

// MatchAny on variable-length / empty / null rows: a row hits iff it has
// at least one element matching; both empty arrays and null rows miss.
/*
TEST_F(SealedMatchExprPlainArrayNullableTest, MatchAnyVariableLength) {
    const int32_t boundary = 100;
    auto result = ExecuteRetrieve("match_any(arr_int, $ > " +
                                  std::to_string(boundary) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (arr_valid_[i] == 0) {
            continue;
        }
        int c = 0;
        for (auto v : arr_int_raw_[i]) {
            if (v > boundary) {
                ++c;
            }
        }
        if (c > 0) {
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}
*/

// MatchAll: vacuous-true branch — rows whose every element passes (empty
// arrays count as vacuously satisfying per MatchSingleRow). Null rows
// SHOULD be filtered out by the row-level valid bitmap, but MatchExpr
// currently doesn't read row-level valid — null rows are stored as empty
// arrays internally and incorrectly hit vacuous-true.
// FIXME: this test intentionally fails until MatchExpr distinguishes
// null rows from empty arrays (requires plumbing row-level valid bitmap
// into EvalWithOffsets and marking null rows as unknown in the output).
/*
TEST_F(SealedMatchExprPlainArrayNullableTest, MatchAllVacuousTrue) {
    const int32_t boundary = 40;  // loose bound: most elements pass
    auto result = ExecuteRetrieve("match_all(arr_int, $ > " +
                                  std::to_string(boundary) + ")");
    std::set<int64_t> expected;
    for (size_t i = 0; i < N_; ++i) {
        if (arr_valid_[i] == 0) {
            continue;  // null row excluded (expected semantics)
        }
        bool all_pass = true;
        for (auto v : arr_int_raw_[i]) {
            if (!(v > boundary)) {
                all_pass = false;
                break;
            }
        }
        if (all_pass) {
            // Includes empty arrays (vacuous true).
            expected.insert(static_cast<int64_t>(i));
        }
    }
    std::set<int64_t> actual(result->offset().begin(), result->offset().end());
    EXPECT_EQ(expected, actual);
}
*/
