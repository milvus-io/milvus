// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <unistd.h>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "exec/QueryContext.h"
#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "exec/expression/ExistsExpr.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/JsonContainsExpr.h"
#include "exec/expression/RawExprCacheAdapter.h"
#include "exec/expression/TimestamptzArithCompareExpr.h"
#include "segcore/SegmentGrowing.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::exec {
namespace {

constexpr int64_t kRows = 2500;
constexpr int64_t kBatch = 1024;
constexpr int64_t kTimestamp = 1700000000000000;

proto::plan::GenericValue
IntValue(int64_t n) {
    proto::plan::GenericValue value;
    value.set_int64_val(n);
    return value;
}

struct RawCalls {
    int eval = 0;
    std::atomic<int> prefetch{0};
};

// Keep the production evaluator and prefetch implementation. Counting their
// entry points proves that a hit bypasses work, not just returns equal bits.
template <typename Base>
class ObservedRawExpr : public Base {
 public:
    template <typename... Args>
    ObservedRawExpr(std::shared_ptr<RawCalls> calls, Args&&... args)
        : Base(std::forward<Args>(args)...), calls_(std::move(calls)) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override {
        ++calls_->eval;
        Base::Eval(context, result);
    }

    void
    PrefetchRawData() override {
        ++calls_->prefetch;
        Base::PrefetchRawData();
    }

 private:
    std::shared_ptr<RawCalls> calls_;
};

enum class Backend { MemorySealed, DiskSealed, MemoryGrowing };

struct PredicateCase {
    std::string name;
    expr::TypedExprPtr logical;
    // Bit i is the expected value/validity of literal row i in the repeating
    // eight-row dataset. These expectations do not use the cache or evaluator.
    uint8_t result;
    uint8_t valid;
};

class RawExprCacheExtendedTest : public ::testing::TestWithParam<Backend> {
 protected:
    void
    SetUp() override {
        auto& manager = ExprResCacheManager::Instance();
        cache_dir_ = std::filesystem::temp_directory_path() /
                     ("raw_expr_extended_" + std::to_string(::getpid()) + "_" +
                      std::to_string(std::rand()));
        CacheConfig config;
        config.mode = GetParam() == Backend::DiskSealed ? CacheMode::Disk
                                                        : CacheMode::Memory;
        config.mem_max_bytes = 4U << 20;
        config.mem_enable_growing = GetParam() == Backend::MemoryGrowing;
        config.disk_base_path = cache_dir_.string();
        config.disk_max_bytes = 4U << 20;
        config.disk_max_file_size = 1U << 20;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        config.disk_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(config));
        manager.Clear();
        ExprResCacheManager::SetEnabled(true);

        schema_ = std::make_shared<Schema>();
        auto pk = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk);
        json_ = schema_->AddDebugField("json", DataType::JSON, true);
        array_ = schema_->AddDebugField(
            "array", DataType::ARRAY, DataType::INT64, true);
        timestamp_ = schema_->AddDebugField("ts", DataType::TIMESTAMPTZ, true);
        auto dataset = segcore::DataGen(schema_, kRows);
        const std::array<std::string, 8> json_rows{
            R"({"items":[1,2,"x",true,[1,2]],"number":3})",
            R"({"items":[2,3],"number":7})",
            R"({"items":[],"number":null})",
            R"({"items":null,"number":"bad"})",
            R"({"other":[1,2]})",
            R"({"items":[2.5,true,"x",[2,3]],"number":2.5})",
            R"({"items":[1,1],"number":9007199254740993})",
            R"({"items":[1,2],"number":4})"};
        const std::array<std::vector<int64_t>, 8> array_rows{
            {{1, 2}, {2, 3}, {}, {9}, {1}, {3, 3}, {1, 2}, {}}};
        for (auto& field : *dataset.raw_->mutable_fields_data()) {
            if (field.field_id() != json_.get() &&
                field.field_id() != array_.get() &&
                field.field_id() != timestamp_.get()) {
                continue;
            }
            auto* scalars = field.mutable_scalars();
            scalars->mutable_valid_data()->Clear();
            if (field.field_id() == json_.get()) {
                scalars->mutable_json_data()->clear_data();
            } else if (field.field_id() == array_.get()) {
                scalars->mutable_array_data()->clear_data();
            } else {
                scalars->mutable_timestamptz_data()->clear_data();
            }
            for (int64_t row = 0; row < kRows; ++row) {
                scalars->add_valid_data(row % 8 != 7);
                if (field.field_id() == json_.get()) {
                    scalars->mutable_json_data()->add_data(json_rows[row % 8]);
                } else if (field.field_id() == array_.get()) {
                    auto* values = scalars->mutable_array_data()
                                       ->add_data()
                                       ->mutable_long_data();
                    for (auto value : array_rows[row % 8]) {
                        values->add_data(value);
                    }
                } else {
                    scalars->mutable_timestamptz_data()->add_data(
                        kTimestamp + (row % 8) * 1000000);
                }
            }
        }
        if (GetParam() == Backend::MemoryGrowing) {
            auto growing =
                segcore::CreateGrowingSegment(schema_, empty_index_meta);
            auto offset = growing->PreInsert(kRows);
            growing->Insert(offset,
                            kRows,
                            dataset.row_ids_.data(),
                            dataset.timestamps_.data(),
                            dataset.raw_);
            segment_ = std::move(growing);
        } else {
            segment_ = CreateSealedWithFieldDataLoaded(schema_, dataset);
        }
        auto config_map = std::unordered_map<std::string, std::string>{
            {QueryConfig::kExprEvalBatchSize, std::to_string(kBatch)}};
        query_ = std::make_unique<QueryContext>(
            "extended_raw_cache",
            segment_.get(),
            kRows,
            MAX_TIMESTAMP,
            0,
            0,
            query::PlanOptions{},
            std::make_shared<QueryConfig>(config_map));
        exec_ = std::make_unique<ExecContext>(query_.get());
        pool_ = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    }

    void
    TearDown() override {
        pool_.reset();
        ExprResCacheManager::Instance().Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(cache_dir_);
    }

    std::vector<PredicateCase>
    Cases() const {
        std::vector<PredicateCase> cases;
        const auto json =
            expr::ColumnInfo(json_, DataType::JSON, {"items"}, true);
        const auto array = expr::ColumnInfo(
            array_, DataType::ARRAY, DataType::INT64, {}, true);
        auto contains = [&](std::string name,
                            const expr::ColumnInfo& column,
                            ContainsType op,
                            bool same_type,
                            std::vector<proto::plan::GenericValue> values,
                            uint8_t bits,
                            uint8_t valid) {
            cases.push_back({std::move(name),
                             std::make_shared<expr::JsonContainsExpr>(
                                 column, op, same_type, values),
                             bits,
                             valid});
        };
        const auto one = IntValue(1);
        const auto two = IntValue(2);
        const auto three = IntValue(3);
        proto::plan::GenericValue boolean, floating, string, nested;
        boolean.set_bool_val(true);
        floating.set_float_val(2.5);
        string.set_string_val("x");
        *nested.mutable_array_val()->add_array() = one;
        *nested.mutable_array_val()->add_array() = two;
        using Op = proto::plan::JSONContainsExpr;
        contains("json_contains", json, Op::Contains, true, {one}, 0x41, 0x67);
        contains(
            "json_any", json, Op::ContainsAny, true, {one, three}, 0x43, 0x67);
        contains(
            "json_all", json, Op::ContainsAll, true, {one, two}, 0x01, 0x67);
        contains("json_all_duplicate",
                 json,
                 Op::ContainsAll,
                 true,
                 {one, one},
                 0x41,
                 0x67);
        for (auto op : {Op::ContainsAny, Op::ContainsAll}) {
            const auto suffix = std::to_string(op);
            contains(
                "json_bool_" + suffix, json, op, true, {boolean}, 0x21, 0x67);
            contains("json_double_" + suffix,
                     json,
                     op,
                     true,
                     {floating},
                     0x20,
                     0x67);
            contains(
                "json_string_" + suffix, json, op, true, {string}, 0x21, 0x67);
            contains(
                "json_nested_" + suffix, json, op, true, {nested}, 0x01, 0x67);
            contains("json_mixed_" + suffix,
                     json,
                     op,
                     false,
                     {one, string},
                     op == Op::ContainsAny ? 0x61 : 0x01,
                     0x67);
            contains("json_empty_" + suffix,
                     json,
                     op,
                     true,
                     {},
                     op == Op::ContainsAll ? 0xff : 0,
                     0xff);
            contains("array_empty_" + suffix,
                     array,
                     op,
                     true,
                     {},
                     op == Op::ContainsAll ? 0x7f : 0,
                     0x7f);
        }
        contains(
            "array_contains", array, Op::Contains, true, {one}, 0x51, 0x7f);
        contains("array_any",
                 array,
                 Op::ContainsAny,
                 true,
                 {one, three},
                 0x73,
                 0x7f);
        contains(
            "array_all", array, Op::ContainsAll, true, {one, two}, 0x41, 0x7f);
        // Json::exist treats null and recursively empty containers as absent.
        cases.push_back(
            {"exists", std::make_shared<expr::ExistsExpr>(json), 0x63, 0xff});
        cases.push_back({"exists_element",
                         std::make_shared<expr::ExistsExpr>(expr::ColumnInfo(
                             json_, DataType::JSON, {"items", "0"}, true)),
                         0x63,
                         0xff});
        auto arith = [&](std::string name,
                         const expr::ColumnInfo& column,
                         proto::plan::ArithOpType op,
                         proto::plan::GenericValue value,
                         proto::plan::GenericValue operand,
                         uint8_t bits,
                         uint8_t valid) {
            cases.push_back({std::move(name),
                             std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                                 column,
                                 proto::plan::OpType::GreaterThan,
                                 op,
                                 value,
                                 operand),
                             bits,
                             valid});
        };
        const auto number =
            expr::ColumnInfo(json_, DataType::JSON, {"number"}, true);
        arith("json_add",
              number,
              proto::plan::ArithOpType::Add,
              IntValue(4),
              one,
              0x42,
              0x63);
        arith("json_add_other_operand",
              number,
              proto::plan::ArithOpType::Add,
              IntValue(4),
              two,
              0x63,
              0x63);
        proto::plan::GenericValue double_value, double_operand;
        double_value.set_float_val(4.0);
        double_operand.set_float_val(0.5);
        arith("json_double_add",
              number,
              proto::plan::ArithOpType::Add,
              double_value,
              double_operand,
              0x42,
              0x63);
        arith("json_length",
              json,
              proto::plan::ArithOpType::ArrayLength,
              one,
              one,
              0x63,
              0x67);
        arith("array_length",
              array,
              proto::plan::ArithOpType::ArrayLength,
              one,
              one,
              0x63,
              0x7f);
        arith("array_add",
              expr::ColumnInfo(
                  array_, DataType::ARRAY, DataType::INT64, {"0"}, true),
              proto::plan::ArithOpType::Add,
              two,
              one,
              0x2a,
              0x7b);
        for (int seconds : {1, 2}) {
            proto::plan::Interval interval;
            interval.set_seconds(seconds);
            cases.push_back(
                {"timestamp_" + std::to_string(seconds),
                 std::make_shared<expr::TimestamptzArithCompareExpr>(
                     expr::ColumnInfo(
                         timestamp_, DataType::TIMESTAMPTZ, {}, true),
                     proto::plan::ArithOpType::Add,
                     interval,
                     proto::plan::OpType::GreaterThan,
                     IntValue(kTimestamp + 4000000)),
                 static_cast<uint8_t>(seconds == 1 ? 0x70 : 0x78),
                 0x7f});
        }
        return cases;
    }

    template <typename Physical, typename Logical>
    std::shared_ptr<SegmentExpr>
    Observe(const std::shared_ptr<const Logical>& logical,
            const std::shared_ptr<RawCalls>& calls,
            int64_t rows) {
        return std::make_shared<ObservedRawExpr<Physical>>(
            calls,
            std::vector<ExprPtr>{},
            logical,
            "observed_raw",
            nullptr,
            segment_.get(),
            rows,
            kBatch,
            0);
    }

    std::shared_ptr<SegmentExpr>
    MakeInput(const expr::TypedExprPtr& logical,
              const std::shared_ptr<RawCalls>& calls,
              int64_t rows = kRows) {
        if (auto p = std::dynamic_pointer_cast<const expr::JsonContainsExpr>(
                logical)) {
            return Observe<PhyJsonContainsFilterExpr>(p, calls, rows);
        }
        if (auto p =
                std::dynamic_pointer_cast<const expr::ExistsExpr>(logical)) {
            return Observe<PhyExistsFilterExpr>(p, calls, rows);
        }
        if (auto p = std::dynamic_pointer_cast<
                const expr::BinaryArithOpEvalRangeExpr>(logical)) {
            return Observe<PhyBinaryArithOpEvalRangeExpr>(p, calls, rows);
        }
        return Observe<PhyTimestamptzArithCompareExpr>(
            std::dynamic_pointer_cast<const expr::TimestamptzArithCompareExpr>(
                logical),
            calls,
            rows);
    }

    std::pair<TargetBitmap, TargetBitmap>
    ReadAll(const ExprPtr& input, EvalCtx& context, int64_t rows = kRows) {
        TargetBitmap data, valid;
        while (data.size() < rows) {
            VectorPtr result;
            input->Eval(context, result);
            auto column = std::dynamic_pointer_cast<ColumnVector>(result);
            EXPECT_NE(column, nullptr);
            if (!column || column->size() == 0) {
                break;
            }
            EXPECT_EQ(column->size(),
                      std::min<int64_t>(kBatch, rows - data.size()));
            data.append(TargetBitmapView(column->GetRawData(), column->size()));
            valid.append(
                TargetBitmapView(column->GetValidRawData(), column->size()));
        }
        EXPECT_EQ(data.size(), rows);
        return {std::move(data), std::move(valid)};
    }

    void
    Check(const std::pair<TargetBitmap, TargetBitmap>& result,
          const PredicateCase& c,
          int64_t start = 0,
          int64_t rows = kRows) {
        ASSERT_EQ(result.first.size(), rows);
        ASSERT_EQ(result.second.size(), rows);
        for (int64_t i = 0; i < rows; ++i) {
            ASSERT_EQ(result.first[i], (c.result >> ((start + i) % 8)) & 1)
                << i;
            ASSERT_EQ(result.second[i], (c.valid >> ((start + i) % 8)) & 1)
                << i;
        }
    }

    std::filesystem::path cache_dir_;
    SchemaPtr schema_;
    FieldId json_, array_, timestamp_;
    std::unique_ptr<segcore::SegmentInternalInterface> segment_;
    std::unique_ptr<QueryContext> query_;
    std::unique_ptr<ExecContext> exec_;
    std::shared_ptr<folly::CPUThreadPoolExecutor> pool_;
};

TEST_P(RawExprCacheExtendedTest, MissHitAndOffsetsPreserveRawResults) {
    auto& manager = ExprResCacheManager::Instance();
    for (const auto& c : Cases()) {
        SCOPED_TRACE(c.name);
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        auto uncached = CompileExpressions({c.logical}, exec_.get(), {}, false);
        ASSERT_EQ(std::dynamic_pointer_cast<RawExprCacheAdapter>(uncached[0]),
                  nullptr);
        EvalCtx raw_context(exec_.get());
        Check(ReadAll(uncached[0], raw_context), c);

        ExprResCacheManager::SetEnabled(true);
        auto compiled = CompileExpressions({c.logical}, exec_.get(), {}, false);
        ASSERT_NE(std::dynamic_pointer_cast<RawExprCacheAdapter>(compiled[0]),
                  nullptr);
        auto miss_calls = std::make_shared<RawCalls>();
        auto input = MakeInput(c.logical, miss_calls);
        auto miss = std::make_shared<RawExprCacheAdapter>(input, nullptr, true);
        miss->PrefetchAsync(pool_);
        EvalCtx context(exec_.get());
        Check(ReadAll(miss, context), c);
        EXPECT_EQ(miss_calls->eval, 3);
        EXPECT_EQ(miss_calls->prefetch.load(), 1);
        ASSERT_EQ(manager.GetEntryCount(), 1u);
        ExprResCacheManager::Value stored;
        stored.active_count = kRows;
        ASSERT_TRUE(manager.Get(
            {segment_->get_segment_id(), input->GetSignatureForRawExprCache()},
            stored));
        Check({stored.result->clone(), stored.valid_result->clone()}, c);

        auto hit_calls = std::make_shared<RawCalls>();
        auto hit = std::make_shared<RawExprCacheAdapter>(
            MakeInput(c.logical, hit_calls), nullptr, false);
        hit->PrefetchAsync(pool_);
        Check(ReadAll(hit, context), c);
        VectorPtr eof;
        hit->Eval(context, eof);
        EXPECT_EQ(eof, nullptr);
        EXPECT_EQ(hit_calls->eval, 0);
        EXPECT_EQ(hit_calls->prefetch.load(), 0);

        auto gathered = std::make_shared<RawExprCacheAdapter>(
            MakeInput(c.logical, hit_calls), nullptr, true);
        OffsetVector offsets{2499, 7, 0, 2, 0, 1024};
        context.set_offset_input(&offsets);
        VectorPtr result;
        gathered->Eval(context, result);
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), offsets.size());
        TargetBitmapView data(column->GetRawData(), column->size());
        TargetBitmapView valid(column->GetValidRawData(), column->size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            EXPECT_EQ(data[i], (c.result >> (offsets[i] % 8)) & 1);
            EXPECT_EQ(valid[i], (c.valid >> (offsets[i] % 8)) & 1);
        }
        context.set_offset_input(nullptr);
        Check(ReadAll(gathered, context), c);
        EXPECT_EQ(hit_calls->eval, 0);
    }
}

TEST_P(RawExprCacheExtendedTest, PartialExecutionNeverPopulatesAnEntry) {
    auto& manager = ExprResCacheManager::Instance();
    for (const auto& c : Cases()) {
        for (int mode = 0; mode < 3; ++mode) {
            SCOPED_TRACE(c.name + ": mode=" + std::to_string(mode));
            manager.Clear();
            auto input = MakeInput(c.logical, std::make_shared<RawCalls>());
            auto adapter =
                std::make_shared<RawExprCacheAdapter>(input, nullptr, true);
            EvalCtx context(exec_.get());
            if (mode == 0) {
                adapter->MoveCursor();
            } else if (mode == 1) {
                TargetBitmap mask(kBatch, true);
                mask[0] = false;
                context.set_bitmap_input(std::move(mask));
                VectorPtr ignored;
                adapter->Eval(context, ignored);
                context.set_bitmap_input(TargetBitmap{});
            } else {
                OffsetVector offsets{0, 7, 2};
                context.set_offset_input(&offsets);
                VectorPtr ignored;
                adapter->Eval(context, ignored);
                context.set_offset_input(nullptr);
            }
            const int64_t start = mode == 2 ? 0 : kBatch;
            Check(ReadAll(adapter, context, kRows - start),
                  c,
                  start,
                  kRows - start);
            EXPECT_EQ(manager.GetEntryCount(), 0u);
        }
    }
}

TEST_P(RawExprCacheExtendedTest, SignaturesAndNotPreserveIndependentResults) {
    auto& manager = ExprResCacheManager::Instance();
    for (int pass = 0; pass < 2; ++pass) {
        size_t entries = pass == 0 ? 0 : manager.GetEntryCount();
        for (const auto& c : Cases()) {
            SCOPED_TRACE(c.name);
            auto negated = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, c.logical);
            auto compiled =
                CompileExpressions({negated}, exec_.get(), {}, false);
            EvalCtx context(exec_.get());
            PredicateCase expected{c.name,
                                   negated,
                                   static_cast<uint8_t>((~c.result) & c.valid),
                                   c.valid};
            Check(ReadAll(compiled[0], context), expected);
            if (pass == 0) {
                EXPECT_EQ(manager.GetEntryCount(), ++entries);
            }
        }
        EXPECT_EQ(manager.GetEntryCount(), entries);
    }
}

TEST_P(RawExprCacheExtendedTest, GrowingSnapshotChangeRecomputes) {
    if (GetParam() != Backend::MemoryGrowing) {
        GTEST_SKIP()
            << "Growing snapshots are only cached by the memory backend";
    }
    for (const auto& c : Cases()) {
        SCOPED_TRACE(c.name);
        ExprResCacheManager::Instance().Clear();
        for (int pass = 0; pass < 3; ++pass) {
            const int64_t rows = pass == 0 ? kRows - 3 : kRows;
            auto calls = std::make_shared<RawCalls>();
            auto adapter = std::make_shared<RawExprCacheAdapter>(
                MakeInput(c.logical, calls, rows), nullptr, true);
            EvalCtx context(exec_.get());
            Check(ReadAll(adapter, context, rows), c, 0, rows);
            EXPECT_EQ(calls->eval, pass == 2 ? 0 : 3);
        }
        EXPECT_EQ(ExprResCacheManager::Instance().GetEntryCount(), 1u);
    }
}

TEST_P(RawExprCacheExtendedTest, ElementLevelExpressionsRemainUndecorated) {
    auto json = expr::ColumnInfo(json_, DataType::JSON, {"items"}, true);
    json.element_level_ = true;
    auto timestamp = expr::ColumnInfo(timestamp_, DataType::TIMESTAMPTZ);
    timestamp.element_level_ = true;
    const std::vector<expr::TypedExprPtr> logicals{
        std::make_shared<expr::JsonContainsExpr>(
            json,
            proto::plan::JSONContainsExpr::Contains,
            true,
            std::vector{IntValue(1)}),
        std::make_shared<expr::ExistsExpr>(json),
        std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            json,
            proto::plan::OpType::GreaterThan,
            proto::plan::ArithOpType::ArrayLength,
            IntValue(1),
            IntValue(1)),
        std::make_shared<expr::TimestamptzArithCompareExpr>(
            timestamp,
            proto::plan::ArithOpType::Add,
            proto::plan::Interval{},
            proto::plan::OpType::GreaterThan,
            IntValue(kTimestamp))};
    for (const auto& logical : logicals) {
        auto input = MakeInput(logical, std::make_shared<RawCalls>());
        EXPECT_FALSE(input->SupportsRawExprCache());
        std::vector<ExprPtr> decorated{input};
        DecorateRawExprCache(decorated, nullptr, true);
        EXPECT_EQ(decorated[0], input);
    }
}

TEST_P(RawExprCacheExtendedTest,
       InvalidJsonArithmeticStillThrowsWithoutCaching) {
    auto logical = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(json_, DataType::JSON, {"number"}, true),
        proto::plan::OpType::GreaterThan,
        proto::plan::ArithOpType::Div,
        IntValue(1),
        IntValue(0));
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto input = MakeInput(logical, std::make_shared<RawCalls>());
        auto adapter =
            std::make_shared<RawExprCacheAdapter>(input, nullptr, true);
        EvalCtx context(exec_.get());
        VectorPtr result;
        EXPECT_THROW(adapter->Eval(context, result), SegcoreError);
        EXPECT_EQ(ExprResCacheManager::Instance().GetEntryCount(), 0u);
    }
}

INSTANTIATE_TEST_SUITE_P(Backends,
                         RawExprCacheExtendedTest,
                         ::testing::Values(Backend::MemorySealed,
                                           Backend::DiskSealed,
                                           Backend::MemoryGrowing),
                         [](const ::testing::TestParamInfo<Backend>& info) {
                             switch (info.param) {
                                 case Backend::MemorySealed:
                                     return "MemorySealed";
                                 case Backend::DiskSealed:
                                     return "DiskSealed";
                                 case Backend::MemoryGrowing:
                                     return "MemoryGrowing";
                             }
                             return "Unknown";
                         });

}  // namespace
}  // namespace milvus::exec
