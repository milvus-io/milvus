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
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stddef.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/FieldMeta.h"
#include "common/IndexMeta.h"
#include "common/PrometheusClient.h"
#include "common/QueryResult.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/MatchExpr.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/InvertedIndexTantivy.h"
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
#include "segcore/segment_c.h"
#include "storage/MmapManager.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/SegcoreConfigUtils.h"
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

namespace {

class FixedBitmapExpr : public exec::Expr {
 public:
    FixedBitmapExpr(TargetBitmap data, TargetBitmap valid)
        : Expr(DataType::BOOL, {}, "FixedBitmapExpr", nullptr),
          data_(std::move(data)),
          valid_(std::move(valid)) {
    }

    void
    Eval(exec::EvalCtx&, VectorPtr& result) override {
        result = std::make_shared<ColumnVector>(data_.clone(), valid_.clone());
    }

    std::string
    ToString() const override {
        return "FixedBitmapExpr";
    }

    std::optional<expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

 private:
    TargetBitmap data_;
    TargetBitmap valid_;
};

bool
MatchSingleRowReference(int64_t bitset_start,
                        int64_t row_elem_count,
                        const TargetBitmap& match_bitmap,
                        const TargetBitmap& valid_bitmap,
                        expr::MatchType match_type,
                        int64_t threshold) {
    int64_t hit_count = 0;
    for (int64_t i = 0; i < row_elem_count; ++i) {
        const auto bit = bitset_start + i;
        if (!valid_bitmap[bit]) {
            continue;
        }
        if (match_bitmap[bit]) {
            ++hit_count;
        } else if (match_type == expr::MatchType::MatchAll) {
            return false;
        }
    }

    switch (match_type) {
        case expr::MatchType::MatchAny:
            return hit_count > 0;
        case expr::MatchType::MatchAll:
            return true;
        case expr::MatchType::MatchLeast:
            return hit_count >= threshold;
        case expr::MatchType::MatchMost:
            return hit_count <= threshold;
        case expr::MatchType::MatchExact:
            return hit_count == threshold;
        default:
            return false;
    }
}

}  // namespace

TEST(MatchExprWordFoldTest, OffsetRowsMatchPerBitReference) {
    const std::vector<int64_t> row_lengths = {0, 1, 63, 64, 65, 200};
    const int64_t row_count = row_lengths.size();

    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);
    auto sub_int_fid = schema->AddDebugArrayField(
        "struct_array[sub_int]", DataType::INT32, false);

    auto insert_data = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids(row_count);
    std::vector<idx_t> row_ids(row_count);
    std::vector<Timestamp> timestamps(row_count);
    std::vector<milvus::proto::schema::ScalarField> sub_int_data(row_count);
    for (int64_t row = 0; row < row_count; ++row) {
        ids[row] = row;
        row_ids[row] = row;
        timestamps[row] = row;
        for (int64_t i = 0; i < row_lengths[row]; ++i) {
            sub_int_data[row].mutable_int_data()->add_data(i);
        }
    }

    auto id_array = CreateDataArrayFrom(
        ids.data(), nullptr, row_count, schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());
    auto sub_int_array = CreateDataArrayFrom(sub_int_data.data(),
                                             nullptr,
                                             row_count,
                                             schema->operator[](sub_int_fid));
    insert_data->mutable_fields_data()->AddAllocated(sub_int_array.release());
    insert_data->set_num_rows(row_count);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    const auto reserved_offset = segment->PreInsert(row_count);
    segment->Insert(reserved_offset,
                    row_count,
                    row_ids.data(),
                    timestamps.data(),
                    insert_data.get());

    exec::OffsetVector offsets = {1, 3, 4, 5, 0, 2};
    std::vector<int64_t> bitset_starts(offsets.size() + 1, 0);
    for (size_t i = 0; i < offsets.size(); ++i) {
        bitset_starts[i + 1] = bitset_starts[i] + row_lengths[offsets[i]];
    }
    const int64_t elem_count = bitset_starts.back();

    TargetBitmap boundary_matches(elem_count, false);
    TargetBitmap boundary_valid(elem_count, true);
    TargetBitmap random_matches(elem_count, false);
    TargetBitmap random_valid(elem_count, true);
    std::mt19937 random(0x51390);
    std::bernoulli_distribution random_match(0.5);
    std::bernoulli_distribution random_is_valid(0.75);
    for (size_t output_row = 0; output_row < offsets.size(); ++output_row) {
        const auto row_length = row_lengths[offsets[output_row]];
        for (int64_t i = 0; i < row_length; ++i) {
            const auto bit = bitset_starts[output_row] + i;
            const bool local_boundary = i == 0 || i + 1 == row_length ||
                                        i == 62 || i == 63 || i == 64 ||
                                        i == 199;
            const bool word_boundary = bit % 64 == 0 || bit % 64 == 63;
            boundary_matches[bit] = local_boundary || word_boundary;
            boundary_valid[bit] =
                !((local_boundary && output_row % 2 == 0) || bit % 17 == 0);
            random_matches[bit] = random_match(random);
            random_valid[bit] = random_is_valid(random);
        }
    }

    struct BitmapCase {
        std::string name;
        TargetBitmap matches;
        TargetBitmap valid;
    };
    std::vector<BitmapCase> bitmap_cases;
    bitmap_cases.push_back({"all-match/all-valid",
                            TargetBitmap(elem_count, true),
                            TargetBitmap(elem_count, true)});
    bitmap_cases.push_back({"no-match/all-valid",
                            TargetBitmap(elem_count, false),
                            TargetBitmap(elem_count, true)});
    bitmap_cases.push_back({"boundary/all-valid",
                            boundary_matches.clone(),
                            TargetBitmap(elem_count, true)});
    bitmap_cases.push_back({"boundary/mixed-valid",
                            boundary_matches.clone(),
                            boundary_valid.clone()});
    bitmap_cases.push_back({"random/all-valid",
                            random_matches.clone(),
                            TargetBitmap(elem_count, true)});
    bitmap_cases.push_back(
        {"random/mixed-valid", random_matches.clone(), random_valid.clone()});

    const std::vector<expr::MatchType> match_types = {
        expr::MatchType::MatchAny,
        expr::MatchType::MatchAll,
        expr::MatchType::MatchLeast,
        expr::MatchType::MatchMost,
        expr::MatchType::MatchExact,
    };
    const std::vector<int64_t> thresholds = {
        0, 1, 2, 62, 63, 64, 65, 66, 199, 200, 201};

    exec::QueryContext query_context(
        "match_word_fold_test", segment.get(), row_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(&query_context);
    for (const auto& bitmap_case : bitmap_cases) {
        for (const auto match_type : match_types) {
            for (const auto threshold : thresholds) {
                SCOPED_TRACE(::testing::Message()
                             << "bitmap=" << bitmap_case.name << ", type="
                             << proto::plan::MatchType_Name(match_type)
                             << ", threshold=" << threshold);

                auto child = std::make_shared<FixedBitmapExpr>(
                    bitmap_case.matches.clone(), bitmap_case.valid.clone());
                std::vector<std::shared_ptr<exec::Expr>> inputs = {child};
                auto logical_expr =
                    std::make_shared<expr::MatchExpr>("struct_array",
                                                      match_type,
                                                      threshold,
                                                      expr::TypedExprPtr{});
                exec::PhyMatchFilterExpr physical_expr(inputs,
                                                       logical_expr,
                                                       "PhyMatchFilterExpr",
                                                       nullptr,
                                                       segment.get(),
                                                       row_count,
                                                       row_count);
                exec::EvalCtx eval_context(&exec_context);
                eval_context.set_offset_input(&offsets);

                VectorPtr result;
                physical_expr.Eval(eval_context, result);
                auto output = std::dynamic_pointer_cast<ColumnVector>(result);
                ASSERT_NE(output, nullptr);
                ASSERT_EQ(output->size(), offsets.size());
                TargetBitmapView output_data(output->GetRawData(),
                                             output->size());
                TargetBitmapView output_valid(output->GetValidRawData(),
                                              output->size());
                for (size_t output_row = 0; output_row < offsets.size();
                     ++output_row) {
                    const bool expected = MatchSingleRowReference(
                        bitset_starts[output_row],
                        row_lengths[offsets[output_row]],
                        bitmap_case.matches,
                        bitmap_case.valid,
                        match_type,
                        threshold);
                    EXPECT_EQ(output_data[output_row], expected)
                        << "output_row=" << output_row
                        << ", source_row=" << offsets[output_row]
                        << ", row_length=" << row_lengths[offsets[output_row]];
                    EXPECT_TRUE(output_valid[output_row]);
                }
            }
        }
    }
}

TEST(MatchExprZeroElementBatch,
     NullableNestedRowsAcrossConsecutiveEmptyBatches) {
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
    const auto nested_int_fid = FieldId(int64_fid.get() + 1);
    proto::schema::TypeSchema nested_int_type;
    nested_int_type.mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(proto::schema::DataType::Int32);
    schema->AddField(FieldMeta(FieldName("struct_array[nested_values]"),
                               nested_int_fid,
                               DataType::ARRAY,
                               DataType::ARRAY,
                               true,
                               std::nullopt,
                               std::string{},
                               LOCAL_FORMAT_RAW,
                               std::make_optional(std::move(nested_int_type))));

    constexpr int64_t N = 7;
    auto insert_data = std::make_unique<InsertRecordProto>();

    std::vector<int64_t> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array = CreateDataArrayFrom(
        ids.data(), nullptr, N, schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    auto* nested_int_field = insert_data->add_fields_data();
    nested_int_field->set_field_id(nested_int_fid.get());
    nested_int_field->set_field_name("struct_array[nested_values]");
    nested_int_field->set_type(proto::schema::DataType::Array);
    for (const bool valid : {false, true, true, false, true, true, true}) {
        nested_int_field->mutable_scalars()->add_valid_data(valid);
    }
    auto* nested_int_rows =
        nested_int_field->mutable_scalars()->mutable_array_data();
    nested_int_rows->set_element_type(proto::schema::DataType::Array);
    auto append_row =
        [nested_int_rows](const std::vector<std::vector<int32_t>>& children) {
            auto* row = nested_int_rows->add_data()->mutable_array_data();
            row->set_element_type(proto::schema::DataType::Int32);
            for (const auto& child_values : children) {
                auto* child = row->add_data()->mutable_int_data();
                for (const auto value : child_values) {
                    child->add_data(value);
                }
            }
        };
    append_row({});
    append_row({});
    append_row({});
    append_row({});
    append_row({});
    append_row({{9001}});
    append_row({{1}});
    insert_data->set_num_rows(N);

    std::vector<Timestamp> timestamps(N);
    std::iota(timestamps.begin(), timestamps.end(), 0);
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(2);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    const auto reserved_offset = segment->PreInsert(N);
    segment->Insert(
        reserved_offset, N, ids.data(), timestamps.data(), insert_data.get());
    ScopedSchemaHandle schema_handle(*schema);
    auto retrieve = [&](const std::string& expression)
        -> std::unique_ptr<proto::segcore::RetrieveResults> {
        const auto plan_bytes = schema_handle.Parse(expression);
        auto plan = CreateRetrievePlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        EXPECT_NE(plan, nullptr);
        if (plan == nullptr) {
            return nullptr;
        }
        return segment->Retrieve(
            nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    };

    // Rows [0, 1] and [2, 3] form two consecutive batches with no elements.
    // The child cursor must still advance before rows 5 and 6 are evaluated.
    auto match_any = retrieve(
        "match_any(struct_array, array_contains($[nested_values], 9001))");
    ASSERT_NE(match_any, nullptr);
    ASSERT_EQ(match_any->offset_size(), 1);
    EXPECT_EQ(match_any->offset(0), 5);

    // Empty non-null rows match_all vacuously; nullable StructArray rows 0
    // and 3 must remain invalid and therefore must not be returned.
    auto match_all = retrieve(
        "match_all(struct_array, array_length($[nested_values]) >= 0)");
    ASSERT_NE(match_all, nullptr);
    const std::vector<int64_t> expected = {1, 2, 4, 5, 6};
    ASSERT_EQ(match_all->offset_size(), expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        EXPECT_EQ(match_all->offset(i), expected[i]);
    }

    // Row-level length uses the recursive ARRAY root and must preserve field
    // nullability: null rows 0 and 3 do not match length zero.
    auto root_length =
        retrieve("array_length(struct_array[nested_values]) == 0");
    ASSERT_NE(root_length, nullptr);
    const std::vector<int64_t> expected_empty = {1, 2, 4};
    ASSERT_EQ(root_length->offset_size(), expected_empty.size());
    for (size_t i = 0; i < expected_empty.size(); ++i) {
        EXPECT_EQ(root_length->offset(i), expected_empty[i]);
    }
}

TEST(MatchExprNestedArrayExpressions, MatchFamilyGrowingAndSealed) {
    const auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(2);
    auto batch_size_guard = folly::makeGuard([saved_batch_size] {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
    });

    auto schema = std::make_shared<Schema>();
    const auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    const auto nested_int_fid = FieldId(int64_fid.get() + 1);
    proto::schema::TypeSchema nested_int_type;
    nested_int_type.mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(proto::schema::DataType::Int32);
    schema->AddField(FieldMeta(FieldName("struct_array[nested_values]"),
                               nested_int_fid,
                               DataType::ARRAY,
                               DataType::ARRAY,
                               false,
                               std::nullopt,
                               std::string{},
                               LOCAL_FORMAT_RAW,
                               std::make_optional(std::move(nested_int_type))));

    const auto nested_string_fid = FieldId(int64_fid.get() + 2);
    proto::schema::TypeSchema nested_string_type;
    nested_string_type.mutable_array_element()
        ->mutable_array_element()
        ->set_leaf_type(proto::schema::DataType::VarChar);
    schema->AddField(
        FieldMeta(FieldName("struct_array[nested_strings]"),
                  nested_string_fid,
                  DataType::ARRAY,
                  DataType::ARRAY,
                  false,
                  std::nullopt,
                  std::string{},
                  LOCAL_FORMAT_RAW,
                  std::make_optional(std::move(nested_string_type))));

    constexpr int64_t row_count = 4;
    auto insert_data = std::make_unique<InsertRecordProto>();
    std::vector<int64_t> ids = {0, 1, 2, 3};
    auto id_array = CreateDataArrayFrom(
        ids.data(), nullptr, row_count, schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    auto* nested_int_field = insert_data->add_fields_data();
    nested_int_field->set_field_id(nested_int_fid.get());
    nested_int_field->set_field_name("struct_array[nested_values]");
    nested_int_field->set_type(proto::schema::DataType::Array);
    auto* nested_int_rows =
        nested_int_field->mutable_scalars()->mutable_array_data();
    nested_int_rows->set_element_type(proto::schema::DataType::Array);
    auto append_int_row =
        [nested_int_rows](const std::vector<std::vector<int32_t>>& children) {
            auto* row = nested_int_rows->add_data()->mutable_array_data();
            row->set_element_type(proto::schema::DataType::Int32);
            for (const auto& child_values : children) {
                auto* child = row->add_data()->mutable_int_data();
                for (const auto value : child_values) {
                    child->add_data(value);
                }
            }
        };
    append_int_row({{1, 2}, {3, 4}});
    append_int_row({{5, 6}, {7}});
    append_int_row({{8}});
    append_int_row({{9, 10}, {11, 12}, {13}});

    auto* nested_string_field = insert_data->add_fields_data();
    nested_string_field->set_field_id(nested_string_fid.get());
    nested_string_field->set_field_name("struct_array[nested_strings]");
    nested_string_field->set_type(proto::schema::DataType::Array);
    auto* nested_string_rows =
        nested_string_field->mutable_scalars()->mutable_array_data();
    nested_string_rows->set_element_type(proto::schema::DataType::Array);
    auto append_string_row =
        [nested_string_rows](
            const std::vector<std::vector<std::string>>& children) {
            auto* row = nested_string_rows->add_data()->mutable_array_data();
            row->set_element_type(proto::schema::DataType::VarChar);
            for (const auto& child_values : children) {
                auto* child = row->add_data()->mutable_string_data();
                for (const auto& value : child_values) {
                    child->add_data(value);
                }
            }
        };
    append_string_row({{"abc", "x"}, {"efg"}});
    append_string_row({{"abc"}, {"abc", "efg"}});
    append_string_row({{}});
    append_string_row({{"tail"}, {"abc", "efg"}, {"zzz"}});
    insert_data->set_num_rows(row_count);

    struct TestCase {
        std::string expression;
        std::vector<int64_t> expected_offsets;
    };
    const std::vector<TestCase> cases = {
        {"array_length(struct_array[nested_values]) == 2", {0, 1}},
        {"match_any(struct_array, "
         "array_length($[nested_values]) == 2)",
         {0, 1, 3}},
        {"match_all(struct_array, "
         "array_length($[nested_values]) == 2)",
         {0}},
        {"match_least(struct_array, "
         "array_length($[nested_values]) == 2, threshold=2)",
         {0, 3}},
        {"match_most(struct_array, "
         "array_length($[nested_values]) == 2, threshold=1)",
         {1, 2}},
        {"match_exact(struct_array, "
         "array_length($[nested_values]) == 2, threshold=2)",
         {0, 3}},
        {"match_any(struct_array, "
         "array_contains($[nested_values], 7))",
         {1}},
        {"match_all(struct_array, "
         "array_contains_any($[nested_values], [1, 4]))",
         {0}},
        {"match_any(struct_array, "
         "array_contains_all($[nested_values], [9, 10]))",
         {3}},
        {"match_any(struct_array, "
         "array_contains($[nested_strings], \"efg\"))",
         {0, 1, 3}},
        {"match_all(struct_array, "
         "array_contains_any($[nested_strings], [\"abc\", \"tail\"]))",
         {1}},
        {"match_any(struct_array, "
         "array_contains_all($[nested_strings], [\"abc\", \"efg\"]))",
         {1, 3}},
        {"match_all(struct_array, "
         "array_contains_all($[nested_strings], []))",
         {0, 1, 2, 3}},
        {"match_any(struct_array, "
         "array_contains_any($[nested_strings], []))",
         {}},
    };

    ScopedSchemaHandle schema_handle(*schema);
    auto check_segment = [&](SegmentInternalInterface* segment,
                             const char* segment_name) {
        for (const auto& test : cases) {
            SCOPED_TRACE(std::string(segment_name) + ": " + test.expression);
            const auto plan_bytes = schema_handle.Parse(test.expression);
            auto plan = CreateRetrievePlanByExpr(
                schema, plan_bytes.data(), plan_bytes.size());
            ASSERT_NE(plan, nullptr);

            auto result = segment->Retrieve(
                nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
            ASSERT_NE(result, nullptr);
            ASSERT_EQ(result->offset_size(), test.expected_offsets.size());
            for (size_t i = 0; i < test.expected_offsets.size(); ++i) {
                EXPECT_EQ(result->offset(i), test.expected_offsets[i]);
            }
        }

        const std::string offset_expression =
            "match_any(struct_array, "
            "array_contains_all($[nested_values], [9, 10]))";
        const auto plan_bytes = schema_handle.Parse(offset_expression);
        auto plan = CreateRetrievePlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);
        const auto& sources = plan->plan_node_->plannodes_->sources();
        ASSERT_EQ(sources.size(), 1);
        auto* filter_node = sources.front().get();
        ASSERT_NE(filter_node, nullptr);
        exec::OffsetVector offsets = {3, 1, 0};
        auto output = test::gen_filter_res(
            filter_node, segment, row_count, MAX_TIMESTAMP, &offsets);
        ASSERT_EQ(output->size(), offsets.size());
        TargetBitmapView output_data(output->GetRawData(), output->size());
        EXPECT_TRUE(output_data[0]);
        EXPECT_FALSE(output_data[1]);
        EXPECT_FALSE(output_data[2]);

        const std::string length_offset_expression =
            "match_all(struct_array, "
            "array_length($[nested_values]) == 2)";
        const auto length_plan_bytes =
            schema_handle.Parse(length_offset_expression);
        auto length_plan = CreateRetrievePlanByExpr(
            schema, length_plan_bytes.data(), length_plan_bytes.size());
        ASSERT_NE(length_plan, nullptr);
        const auto& length_sources =
            length_plan->plan_node_->plannodes_->sources();
        ASSERT_EQ(length_sources.size(), 1);
        auto* length_filter_node = length_sources.front().get();
        ASSERT_NE(length_filter_node, nullptr);
        exec::OffsetVector length_offsets = {3, 1, 0};
        auto length_output = test::gen_filter_res(length_filter_node,
                                                  segment,
                                                  row_count,
                                                  MAX_TIMESTAMP,
                                                  &length_offsets);
        ASSERT_EQ(length_output->size(), length_offsets.size());
        TargetBitmapView length_output_data(length_output->GetRawData(),
                                            length_output->size());
        EXPECT_FALSE(length_output_data[0]);
        EXPECT_FALSE(length_output_data[1]);
        EXPECT_TRUE(length_output_data[2]);

        const std::string row_length_offset_expression =
            "array_length(struct_array[nested_values]) == 2";
        const auto row_length_plan_bytes =
            schema_handle.Parse(row_length_offset_expression);
        auto row_length_plan = CreateRetrievePlanByExpr(
            schema, row_length_plan_bytes.data(), row_length_plan_bytes.size());
        ASSERT_NE(row_length_plan, nullptr);
        const auto& row_length_sources =
            row_length_plan->plan_node_->plannodes_->sources();
        ASSERT_EQ(row_length_sources.size(), 1);
        auto* row_length_filter_node = row_length_sources.front().get();
        ASSERT_NE(row_length_filter_node, nullptr);
        exec::OffsetVector row_length_offsets = {3, 1, 0};
        auto row_length_output = test::gen_filter_res(row_length_filter_node,
                                                      segment,
                                                      row_count,
                                                      MAX_TIMESTAMP,
                                                      &row_length_offsets);
        ASSERT_EQ(row_length_output->size(), row_length_offsets.size());
        TargetBitmapView row_length_output_data(row_length_output->GetRawData(),
                                                row_length_output->size());
        EXPECT_FALSE(row_length_output_data[0]);
        EXPECT_TRUE(row_length_output_data[1]);
        EXPECT_TRUE(row_length_output_data[2]);
    };

    std::vector<idx_t> row_ids = {0, 1, 2, 3};
    std::vector<Timestamp> timestamps = {0, 1, 2, 3};
    {
        auto& config = SegcoreConfig::default_config();
        ScopedSegcoreConfigRestore config_restore(config);
        config.set_chunk_rows(2);
        auto growing =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        const auto reserved_offset = growing->PreInsert(row_count);
        growing->Insert(reserved_offset,
                        row_count,
                        row_ids.data(),
                        timestamps.data(),
                        insert_data.get());
        ASSERT_GT(growing->num_chunk(nested_int_fid), 1);
        check_segment(growing.get(), "growing multi-chunk");
    }

    {
        auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
        const auto saved_growing_mmap = mmap_config.GetEnableGrowingMmap();
        mmap_config.SetEnableGrowingMmap(true);
        auto mmap_guard = folly::makeGuard([&mmap_config, saved_growing_mmap] {
            mmap_config.SetEnableGrowingMmap(saved_growing_mmap);
        });

        auto& config = SegcoreConfig::default_config();
        ScopedSegcoreConfigRestore config_restore(config);
        config.set_chunk_rows(2);
        auto growing =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        const auto reserved_offset = growing->PreInsert(row_count);
        growing->Insert(reserved_offset,
                        row_count,
                        row_ids.data(),
                        timestamps.data(),
                        insert_data.get());
        auto* growing_impl = dynamic_cast<SegmentGrowingImpl*>(growing.get());
        ASSERT_NE(growing_impl, nullptr);
        EXPECT_TRUE(growing_impl->get_insert_record()
                        .get_data<ArrayValue>(nested_int_fid)
                        ->is_mmap());
        check_segment(growing.get(), "growing mmap multi-chunk");
    }

    proto::schema::CollectionSchema storage_schema_proto;
    auto add_system_field = [&](FieldId field_id, const char* name) {
        auto* field = storage_schema_proto.add_fields();
        field->set_fieldid(field_id.get());
        field->set_name(name);
        field->set_data_type(proto::schema::DataType::Int64);
    };
    add_system_field(RowFieldID, "RowID");
    add_system_field(TimestampFieldID, "Timestamp");
    const auto user_schema_proto = schema->ToProto();
    for (const auto& field : user_schema_proto.fields()) {
        auto* storage_field = storage_schema_proto.add_fields();
        *storage_field = field;
        if (field.fieldid() == nested_int_fid.get() ||
            field.fieldid() == nested_string_fid.get()) {
            auto* mmap = storage_field->add_type_params();
            mmap->set_key(MMAP_ENABLED_KEY);
            mmap->set_value("false");
        }
    }
    auto storage_schema = Schema::ParseFrom(storage_schema_proto);

    auto storage_growing =
        CreateGrowingSegment(storage_schema, empty_index_meta, 2);
    const auto storage_offset = storage_growing->PreInsert(row_count);
    storage_growing->Insert(storage_offset,
                            row_count,
                            row_ids.data(),
                            timestamps.data(),
                            insert_data.get());

    const auto unique =
        std::chrono::steady_clock::now().time_since_epoch().count();
    const auto segment_path =
        (std::filesystem::temp_directory_path() /
         ("milvus_match_nested_array_v3_" + std::to_string(unique)))
            .string();
    std::filesystem::remove_all(segment_path);
    auto directory_guard = folly::makeGuard(
        [&segment_path] { std::filesystem::remove_all(segment_path); });

    auto schema_blob = storage_schema_proto.SerializeAsString();
    const auto column_group_pattern =
        "0|1|" + std::to_string(int64_fid.get()) + "," +
        std::to_string(nested_int_fid.get()) + "," +
        std::to_string(nested_string_fid.get());
    CFlushConfig flush_config{};
    flush_config.segment_path = segment_path.c_str();
    flush_config.read_version = -1;
    flush_config.retry_limit = 3;
    flush_config.schema_blob = schema_blob.data();
    flush_config.schema_length = static_cast<int64_t>(schema_blob.size());
    flush_config.schema_based_pattern = column_group_pattern.c_str();

    CFlushResult flush_result{};
    auto flush_guard =
        folly::makeGuard([&flush_result] { FreeFlushResult(&flush_result); });
    const auto flush_status = FlushGrowingSegmentData(
        storage_growing.get(), 0, row_count, &flush_config, &flush_result);
    ASSERT_EQ(flush_status.error_code, Success) << flush_status.error_msg;
    ASSERT_EQ(flush_result.num_rows, row_count);

    const auto manifest_path =
        "{\"base_path\":\"" + segment_path +
        "\",\"ver\":" + std::to_string(flush_result.committed_version) + "}";
    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(row_count);
    load_info.set_manifest_path(manifest_path);
    load_info.set_insert_channel("nested-array-match-test");

    auto load_sealed = [&](SchemaPtr load_schema, int64_t segment_id) {
        auto segment_load_info = load_info;
        segment_load_info.set_segmentid(segment_id);
        auto sealed = CreateSealedSegment(
            std::move(load_schema), empty_index_meta, segment_id);
        sealed->SetLoadInfo(segment_load_info);
        tracer::TraceContext trace_ctx;
        sealed->Load(trace_ctx, nullptr);
        return sealed;
    };

    auto sealed = load_sealed(storage_schema, 3);
    EXPECT_FALSE(sealed->is_mmap_field(nested_int_fid));
    check_segment(sealed.get(), "sealed");

    auto mmap_schema_proto = storage_schema_proto;
    for (auto& field : *mmap_schema_proto.mutable_fields()) {
        if (field.fieldid() != nested_int_fid.get() &&
            field.fieldid() != nested_string_fid.get()) {
            continue;
        }
        for (auto& type_param : *field.mutable_type_params()) {
            if (type_param.key() == MMAP_ENABLED_KEY) {
                type_param.set_value("true");
            }
        }
    }
    auto sealed_mmap = load_sealed(Schema::ParseFrom(mmap_schema_proto), 4);
    EXPECT_TRUE(sealed_mmap->is_mmap_field(nested_int_fid));
    check_segment(sealed_mmap.get(), "sealed mmap");
}

namespace {

std::unique_ptr<InsertRecordProto>
BuildNullableStructInsertData(const std::shared_ptr<Schema>& schema,
                              FieldId int64_fid,
                              FieldId sub_int_fid) {
    constexpr int64_t row_count = 5;
    auto insert_data = std::make_unique<InsertRecordProto>();

    std::vector<int64_t> ids(row_count);
    std::iota(ids.begin(), ids.end(), 0);
    auto id_array = CreateDataArrayFrom(
        ids.data(), nullptr, row_count, schema->operator[](int64_fid));
    insert_data->mutable_fields_data()->AddAllocated(id_array.release());

    auto* sub_field = insert_data->add_fields_data();
    sub_field->set_field_id(sub_int_fid.get());
    sub_field->set_field_name("struct_array[sub_int]");
    sub_field->set_type(proto::schema::DataType::Array);
    auto* array_data = sub_field->mutable_scalars()->mutable_array_data();
    array_data->set_element_type(proto::schema::DataType::Int32);

    const std::vector<bool> valid = {true, false, true, true, false};
    for (auto is_valid : valid) {
        sub_field->mutable_scalars()->add_valid_data(is_valid);
    }

    auto append_row = [array_data](std::initializer_list<int32_t> values) {
        auto* row = array_data->add_data();
        for (auto value : values) {
            row->mutable_int_data()->add_data(value);
        }
    };

    append_row({1, 2});
    append_row({});
    append_row({});
    append_row({9001});
    append_row({});

    insert_data->set_num_rows(row_count);
    return insert_data;
}

std::set<int64_t>
RetrieveOffsets(SegmentInternalInterface* segment,
                const std::shared_ptr<Schema>& schema,
                const std::string& expr) {
    ScopedSchemaHandle schema_handle(*schema);
    auto plan_str = schema_handle.Parse(expr);
    auto plan =
        CreateRetrievePlanByExpr(schema, plan_str.data(), plan_str.size());
    EXPECT_NE(plan, nullptr);
    auto result = segment->Retrieve(
        nullptr, plan.get(), 1L << 63, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_NE(result, nullptr);

    std::set<int64_t> offsets;
    if (result != nullptr) {
        offsets.insert(result->offset().begin(), result->offset().end());
    }
    return offsets;
}

void
CheckNullableStructExpressions(SegmentInternalInterface* segment,
                               const std::shared_ptr<Schema>& schema) {
    EXPECT_EQ(
        RetrieveOffsets(
            segment, schema, "match_any(struct_array, $[sub_int] >= 9000)"),
        (std::set<int64_t>{3}));
    EXPECT_EQ(RetrieveOffsets(
                  segment, schema, "match_all(struct_array, $[sub_int] >= 0)"),
              (std::set<int64_t>{0, 2, 3}));
    EXPECT_EQ(
        RetrieveOffsets(
            segment, schema, "not match_any(struct_array, $[sub_int] >= 9000)"),
        (std::set<int64_t>{0, 2}));
    EXPECT_TRUE(RetrieveOffsets(segment,
                                schema,
                                "match_any(struct_array, "
                                "$[sub_int] == 2147483648)")
                    .empty());
    EXPECT_EQ(RetrieveOffsets(segment,
                              schema,
                              "match_all(struct_array, "
                              "$[sub_int] != 2147483648)"),
              (std::set<int64_t>{0, 2, 3}));
    EXPECT_TRUE(RetrieveOffsets(segment,
                                schema,
                                "match_any(struct_array, "
                                "2147483648 < $[sub_int] < 2147483649)")
                    .empty());
    EXPECT_EQ(
        RetrieveOffsets(
            segment, schema, "json_contains_all(struct_array[sub_int], [])"),
        (std::set<int64_t>{0, 2, 3}));
    EXPECT_TRUE(RetrieveOffsets(segment,
                                schema,
                                "json_contains_any(struct_array[sub_int], [])")
                    .empty());
    EXPECT_EQ(
        RetrieveOffsets(segment,
                        schema,
                        "not json_contains_any(struct_array[sub_int], [])"),
        (std::set<int64_t>{0, 2, 3}));
}

std::shared_ptr<Schema>
BuildNullableStructSchema(FieldId& int64_fid, FieldId& sub_int_fid) {
    auto schema = std::make_shared<Schema>();
    int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);
    sub_int_fid = schema->AddDebugArrayField(
        "struct_array[sub_int]", DataType::INT32, true);
    return schema;
}

}  // namespace

TEST(MatchExprNullableStruct, SealedPropagatesStructRowValidity) {
    FieldId int64_fid;
    FieldId sub_int_fid;
    auto schema = BuildNullableStructSchema(int64_fid, sub_int_fid);
    auto insert_data =
        BuildNullableStructInsertData(schema, int64_fid, sub_int_fid);

    GeneratedData generated_data;
    generated_data.schema_ = schema;
    generated_data.raw_ = insert_data.release();
    for (int64_t i = 0; i < 5; ++i) {
        generated_data.row_ids_.push_back(i);
        generated_data.timestamps_.push_back(i);
    }

    auto segment = CreateSealedWithFieldDataLoaded(schema, generated_data);
    CheckNullableStructExpressions(segment.get(), schema);
}

TEST(MatchExprNullableStruct, GrowingPropagatesStructRowValidity) {
    FieldId int64_fid;
    FieldId sub_int_fid;
    auto schema = BuildNullableStructSchema(int64_fid, sub_int_fid);
    auto insert_data =
        BuildNullableStructInsertData(schema, int64_fid, sub_int_fid);

    std::vector<idx_t> row_ids = {0, 1, 2, 3, 4};
    std::vector<Timestamp> timestamps = {0, 1, 2, 3, 4};
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    segment->PreInsert(row_ids.size());
    segment->Insert(0,
                    row_ids.size(),
                    row_ids.data(),
                    timestamps.data(),
                    insert_data.get());

    CheckNullableStructExpressions(segment.get(), schema);
}

TEST(MatchExprNullableStruct, NestedIndexUsesPhysicalRowValidity) {
    FieldId int64_fid;
    FieldId sub_int_fid;
    auto schema = BuildNullableStructSchema(int64_fid, sub_int_fid);
    auto insert_data =
        BuildNullableStructInsertData(schema, int64_fid, sub_int_fid);

    GeneratedData generated_data;
    generated_data.schema_ = schema;
    generated_data.raw_ = insert_data.release();
    for (int64_t i = 0; i < 5; ++i) {
        generated_data.row_ids_.push_back(i);
        generated_data.timestamps_.push_back(i);
    }
    auto segment = CreateSealedWithFieldDataLoaded(schema, generated_data);

    std::vector<boost::container::vector<int32_t>> arrays = {
        {1, 2}, {}, {}, {9001}, {}};
    auto index = std::make_unique<index::InvertedIndexTantivy<int32_t>>();
    Config cfg;
    cfg["is_array"] = true;
    cfg["is_nested_index"] = true;
    index->BuildWithRawDataForUT(arrays.size(), arrays.data(), cfg);
    LoadIndexInfo info{};
    info.field_id = sub_int_fid.get();
    info.index_params = GenIndexParams(index.get());
    info.cache_index =
        CreateTestCacheIndex("nullable_sub_int", std::move(index));
    segment->LoadIndex(info);

    EXPECT_EQ(RetrieveOffsets(segment.get(),
                              schema,
                              "match_all(struct_array, "
                              "$[sub_int] != 2147483648)"),
              (std::set<int64_t>{0, 2, 3}));
    EXPECT_EQ(
        RetrieveOffsets(segment.get(),
                        schema,
                        "not array_contains(struct_array[sub_int], 9001)"),
        (std::set<int64_t>{0, 2}));
}

TEST(StructArrayOffsetsReopen, ConcurrentReadersAreSynchronized) {
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);
    auto base_sub_fid = schema->AddDebugArrayField(
        "base_struct[sub_int]", DataType::INT32, true);
    schema->set_schema_version(1);

    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    std::atomic<bool> stop{false};
    std::atomic<int64_t> missing{0};
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; ++i) {
        readers.emplace_back([&]() {
            while (!stop.load(std::memory_order_relaxed)) {
                if (segment->GetArrayOffsets(base_sub_fid) == nullptr) {
                    missing.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    auto latest_schema = schema;
    for (int version = 2; version <= 32; ++version) {
        auto next_schema = std::make_shared<Schema>(*latest_schema);
        next_schema->AddDebugArrayField(
            "struct_" + std::to_string(version) + "[sub_int]",
            DataType::INT32,
            true);
        next_schema->set_schema_version(version);
        segment->LazyCheckSchema(next_schema, nullptr);
        latest_schema = std::move(next_schema);
    }

    stop.store(true, std::memory_order_relaxed);
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_EQ(missing.load(), 0);
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

TEST_F(SealedMatchExprTest, RetrieveMatchAnyOverflowWithIndex) {
    auto result =
        ExecuteRetrieve("match_any(struct_array, $[sub_int] == 2147483648)");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->offset_size(), 0);
}

TEST_F(SealedMatchExprTest, OverflowShortcutWithIndexFollowsMovedCursor) {
    // With 128-row batches, the numeric predicate skips Match evaluation for
    // the first seven batches.  The overflow shortcut is evaluated only for
    // the final 104 rows, after its cursor has been advanced via MoveCursor().
    EXEC_EVAL_EXPR_BATCH_SIZE.store(128);

    for (const auto& predicate : {
             "$[sub_int] == 2147483648",
             "2147483648 < $[sub_int] < 2147483649",
         }) {
        auto result = ExecuteRetrieve("id >= 896 && match_any(struct_array, " +
                                      std::string(predicate) + ")");
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->offset_size(), 0);
    }
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

TEST_F(SealedMatchExprTestNoIndex, OverflowShortcutWithOffsetInput) {
    exec::OffsetVector offsets = {1, 123, 999};

    auto evaluate = [&](const std::string& filter) {
        ScopedSchemaHandle schema_handle(*schema_);
        auto plan_str = schema_handle.ParseSearch(
            filter, "vec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema_, plan_str.data(), plan_str.size());
        EXPECT_NE(plan, nullptr);

        auto filter_node =
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get();
        return test::gen_filter_res(
            filter_node, seg_.get(), N_, MAX_TIMESTAMP, &offsets);
    };

    auto equal_overflow =
        evaluate("match_any(struct_array, $[sub_int] == 2147483648)");
    ASSERT_EQ(equal_overflow->size(), offsets.size());
    TargetBitmapView equal_view(equal_overflow->GetRawData(),
                                equal_overflow->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_FALSE(equal_view[i]);
        EXPECT_TRUE(equal_overflow->ValidAt(i));
    }

    auto not_equal_overflow =
        evaluate("match_all(struct_array, $[sub_int] != 2147483648)");
    ASSERT_EQ(not_equal_overflow->size(), offsets.size());
    TargetBitmapView not_equal_view(not_equal_overflow->GetRawData(),
                                    not_equal_overflow->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_TRUE(not_equal_view[i]);
        EXPECT_TRUE(not_equal_overflow->ValidAt(i));
    }

    auto range_overflow = evaluate(
        "match_any(struct_array, 2147483648 < $[sub_int] < 2147483649)");
    ASSERT_EQ(range_overflow->size(), offsets.size());
    TargetBitmapView range_view(range_overflow->GetRawData(),
                                range_overflow->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_FALSE(range_view[i]);
        EXPECT_TRUE(range_overflow->ValidAt(i));
    }
}

TEST_F(SealedMatchExprTestNoIndex,
       OverflowShortcutWithoutIndexFollowsMovedCursor) {
    // Exercise the same skipped-batch transition through the raw-data cursor.
    EXEC_EVAL_EXPR_BATCH_SIZE.store(128);

    for (const auto& predicate : {
             "$[sub_int] == 2147483648",
             "2147483648 < $[sub_int] < 2147483649",
         }) {
        auto result = ExecuteRetrieve("id >= 896 && match_any(struct_array, " +
                                      std::string(predicate) + ")");
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->offset_size(), 0);
    }
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

    std::string
    OverflowLiteral() const {
        switch (int_type_) {
            case DataType::INT8:
                return "128";
            case DataType::INT16:
                return "32768";
            case DataType::INT32:
                return "2147483648";
            default:
                return "0";
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

TEST_P(SealedMatchExprIntTypeTest, MatchAnyOverflowShortcutReturnsNoRows) {
    auto param = GetParam();
    auto filter_expr =
        "match_any(struct_array, $[sub_int] == " + OverflowLiteral() + ")";
    auto result = ExecuteRetrieve(filter_expr);

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->offset_size(), 0)
        << param.type_name << " overflow equality should match no rows";
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
