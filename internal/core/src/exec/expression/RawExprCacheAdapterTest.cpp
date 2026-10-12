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

#include "exec/expression/RawExprCacheAdapter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include <folly/ScopeGuard.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "cachinglayer/Metrics.h"
#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/QueryContext.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/skip_metrics_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::exec {
namespace {

constexpr int64_t kActiveCount = 2500;
constexpr int64_t kBatchSize = 1024;

ExprResCacheManager::Key
RawCacheKey(const SegmentExpr& input) {
    return {input.GetSegment()->get_segment_id(),
            input.GetSignatureForRawExprCache()};
}

class CountingCPUThreadPoolExecutor final
    : public folly::CPUThreadPoolExecutor {
 public:
    explicit CountingCPUThreadPoolExecutor(size_t num_threads)
        : folly::CPUThreadPoolExecutor(num_threads) {
    }

    void
    add(folly::Func func) override {
        submitted_tasks_.fetch_add(1, std::memory_order_relaxed);
        folly::CPUThreadPoolExecutor::add(std::move(func));
    }

    size_t
    submitted_tasks() const {
        return submitted_tasks_.load(std::memory_order_relaxed);
    }

 private:
    std::atomic<size_t> submitted_tasks_{0};
};

class CountingRawExpr final : public SegmentExpr {
 public:
    CountingRawExpr(const segcore::SegmentInternalInterface* segment,
                    FieldId field_id,
                    std::string signature,
                    int64_t active_count = kActiveCount)
        : SegmentExpr({},
                      "CountingRawExpr",
                      nullptr,
                      segment,
                      field_id,
                      {},
                      DataType::INT64,
                      active_count,
                      kBatchSize,
                      0),
          signature_(std::move(signature)) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override {
        ++eval_count;
        if (eval_count == throw_on_eval_count) {
            throw std::runtime_error("injected raw evaluation failure");
        }
        auto* offsets = context.get_offset_input();
        SetHasOffsetInput(offsets != nullptr);

        const auto rows = offsets == nullptr
                              ? GetNextBatchSize()
                              : static_cast<int64_t>(offsets->size());
        if (rows == 0) {
            result = nullptr;
            return;
        }

        TargetBitmap data(rows, false);
        TargetBitmap valid(rows, false);
        const auto& bitmap_input = context.get_bitmap_input();
        for (int64_t i = 0; i < rows; ++i) {
            const auto row = offsets == nullptr
                                 ? cursor_pos_ + i
                                 : static_cast<int64_t>((*offsets)[i]);
            valid[i] = IsValid(row);
            data[i] = valid[i] && Matches(row) &&
                      (bitmap_input.empty() || bitmap_input[i]);
        }
        result =
            std::make_shared<ColumnVector>(std::move(data), std::move(valid));
        if (offsets == nullptr) {
            MoveCursor();
        }
    }

    void
    MoveCursor() override {
        ++move_count;
        if (!has_offset_input_) {
            cursor_pos_ += GetNextBatchSize();
        }
        SegmentExpr::MoveCursor();
    }

    void
    DetermineExecPath() override {
        exec_path_ = ExprExecPath::RawData;
    }

    bool
    SupportsRawExprCache() const override {
        return true;
    }

    std::string
    ToString() const override {
        return signature_;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    void
    PrefetchRawData() override {
        ++prefetch_count;
        if (prefetch_delay.count() > 0) {
            std::this_thread::sleep_for(prefetch_delay);
        }
    }

    static bool
    Matches(int64_t row) {
        return row % 3 == 1;
    }

    static bool
    IsValid(int64_t row) {
        return row % 5 != 0;
    }

    int64_t
    cursor_pos() const {
        return cursor_pos_;
    }

    int eval_count{0};
    int move_count{0};
    int prefetch_count{0};
    int throw_on_eval_count{0};
    std::chrono::milliseconds prefetch_delay{0};

 private:
    std::string signature_;
    int64_t cursor_pos_{0};
};

class RawExprCacheAdapterTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto& manager = ExprResCacheManager::Instance();
        ExprResCacheManager::SetEnabled(true);
        CacheConfig config;
        config.mode = CacheMode::Memory;
        config.mem_max_bytes = 1U << 20;
        config.compression_enabled = false;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(config));
        manager.Clear();

        schema_ = std::make_shared<Schema>();
        auto primary_field_id = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(primary_field_id);
        field_id_ = schema_->AddDebugField("value", DataType::INT64);
        auto data = segcore::DataGen(schema_, kActiveCount);
        segment_ = CreateSealedWithFieldDataLoaded(schema_, data);
    }

    void
    TearDown() override {
        auto& manager = ExprResCacheManager::Instance();
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
    }

    std::pair<std::shared_ptr<CountingRawExpr>,
              std::shared_ptr<RawExprCacheAdapter>>
    MakeAdapter(const std::string& signature, bool enable_cache_write = true) {
        auto input = std::make_shared<CountingRawExpr>(
            segment_.get(), field_id_, signature);
        auto adapter = std::make_shared<RawExprCacheAdapter>(
            input, nullptr, enable_cache_write);
        return {std::move(input), std::move(adapter)};
    }

    std::vector<int64_t>
    EvalSequential(const std::shared_ptr<RawExprCacheAdapter>& adapter,
                   EvalCtx& context,
                   TargetBitmap* combined_data = nullptr,
                   TargetBitmap* combined_valid = nullptr) {
        std::vector<int64_t> batch_sizes;
        int64_t processed = 0;
        while (processed < kActiveCount) {
            VectorPtr result;
            adapter->Eval(context, result);
            auto column = std::dynamic_pointer_cast<ColumnVector>(result);
            EXPECT_NE(column, nullptr);
            if (column == nullptr) {
                break;
            }
            const auto size = static_cast<int64_t>(column->size());
            batch_sizes.push_back(size);
            processed += size;
            if (combined_data != nullptr) {
                combined_data->append(
                    TargetBitmapView(column->GetRawData(), size));
            }
            if (combined_valid != nullptr) {
                combined_valid->append(
                    TargetBitmapView(column->GetValidRawData(), size));
            }
        }
        return batch_sizes;
    }

    std::shared_ptr<Schema> schema_;
    FieldId field_id_;
    std::unique_ptr<segcore::SegmentSealed> segment_;
};

TEST_F(RawExprCacheAdapterTest,
       SequentialCaptureUsesInternalCursorAndHitBypassesEval) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:sequential");
    TargetBitmap miss_data;
    TargetBitmap miss_valid;
    EXPECT_EQ(
        EvalSequential(miss_adapter, eval_context, &miss_data, &miss_valid),
        (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(miss_input->eval_count, 3);
    EXPECT_EQ(miss_input->move_count, 3);

    const auto key = RawCacheKey(*miss_input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    ASSERT_TRUE(ExprResCacheManager::Instance().Get(key, cached));

    auto [hit_input, hit_adapter] = MakeAdapter("raw:sequential");
    TargetBitmap hit_data;
    TargetBitmap hit_valid;
    EXPECT_EQ(EvalSequential(hit_adapter, eval_context, &hit_data, &hit_valid),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(hit_input->eval_count, 0);
    EXPECT_EQ(hit_input->move_count, 3);
    EXPECT_TRUE(hit_data == miss_data);
    EXPECT_TRUE(hit_valid == miss_valid);

    VectorPtr eof;
    hit_adapter->Eval(eval_context, eof);
    EXPECT_EQ(eof, nullptr);
    EXPECT_EQ(hit_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest,
       MaterializationPressureBypassesHitAndMissCapture) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.materialization_max_bytes = 1;
    config.compression_enabled = false;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    constexpr auto kHitSignature = "raw:materialization-pressure-hit";
    auto [hit_input, hit_adapter] = MakeAdapter(kHitSignature);
    const auto hit_key = RawCacheKey(*hit_input);
    ExprResCacheManager::Value cached_value;
    cached_value.result = std::make_shared<TargetBitmap>(kActiveCount, true);
    cached_value.valid_result =
        std::make_shared<TargetBitmap>(kActiveCount, true);
    cached_value.active_count = kActiveCount;
    manager.Put(hit_key, cached_value);
    ASSERT_EQ(manager.GetEntryCount(), 1u);

    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    // The cached entry exists, but decoding a full bitmap pair cannot reserve
    // its bytes. The adapter must use normal batched evaluation instead.
    EXPECT_EQ(EvalSequential(hit_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(hit_input->eval_count, 3);
    EXPECT_EQ(manager.GetEntryCount(), 1u);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);

    // An absent key is still a miss. Its separate capture reservation must
    // reject accumulation of a full bitmap pair under the same pressure.
    auto [miss_input, miss_adapter] =
        MakeAdapter("raw:materialization-pressure-miss");
    EXPECT_EQ(EvalSequential(miss_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(miss_input->eval_count, 3);
    EXPECT_EQ(manager.GetEntryCount(), 1u);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
}

TEST_F(RawExprCacheAdapterTest, MemoryGaugeTracksCaptureAndHitLifetime) {
    auto& manager = ExprResCacheManager::Instance();
    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto baseline = gauge.Value();
    const auto pair_bytes = 2 * ((kActiveCount + 63) / 64) * 8;
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    VectorPtr result;

    auto [aborted_input, aborted_adapter] = MakeAdapter("raw:gauge");
    aborted_adapter->Eval(eval_context, result);
    EXPECT_EQ(manager.GetMaterializationBytes(), pair_bytes);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + pair_bytes);
    aborted_adapter.reset();
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:gauge");
    EvalSequential(miss_adapter, eval_context);
    ASSERT_EQ(manager.GetEntryCount(), 1u);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + manager.GetMemoryBytes());

    auto [hit_input, hit_adapter] = MakeAdapter("raw:gauge");
    hit_adapter->Eval(eval_context, result);
    EXPECT_EQ(hit_input->eval_count, 0);
    EXPECT_EQ(manager.GetMaterializationBytes(), pair_bytes);
    EXPECT_DOUBLE_EQ(gauge.Value(),
                     baseline + manager.GetMemoryBytes() + pair_bytes);
    manager.Clear();
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + pair_bytes);
    hit_adapter.reset();
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline);
}

TEST_F(RawExprCacheAdapterTest, ReadOnlyPolicyStillServesExistingEntry) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [writer_input, writer_adapter] = MakeAdapter("raw:read-only-hit");
    EvalSequential(writer_adapter, eval_context);
    ASSERT_EQ(writer_input->eval_count, 3);

    auto read_only_input = std::make_shared<CountingRawExpr>(
        segment_.get(), field_id_, "raw:read-only-hit");
    std::vector<ExprPtr> exprs{read_only_input};
    DecorateRawExprCache(exprs, nullptr, false);
    auto read_only_adapter =
        std::dynamic_pointer_cast<RawExprCacheAdapter>(exprs[0]);
    ASSERT_NE(read_only_adapter, nullptr);

    EXPECT_EQ(EvalSequential(read_only_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(read_only_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest, JsonNestedPathSignaturesDoNotCollide) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    const expr::ColumnInfo comma_key(
        field_id_, DataType::JSON, std::vector<std::string>{"a,b"});
    const expr::ColumnInfo nested_keys(
        field_id_, DataType::JSON, std::vector<std::string>{"a", "b"});
    const auto comma_key_signature = comma_key.ToString();
    const auto nested_keys_signature = nested_keys.ToString();
    ASSERT_NE(comma_key_signature, nested_keys_signature);

    auto [comma_key_input, comma_key_adapter] =
        MakeAdapter(comma_key_signature);
    EvalSequential(comma_key_adapter, eval_context);
    ASSERT_EQ(comma_key_input->eval_count, 3);

    auto [nested_keys_input, nested_keys_adapter] =
        MakeAdapter(nested_keys_signature);
    EvalSequential(nested_keys_adapter, eval_context);
    EXPECT_EQ(nested_keys_input->eval_count, 3);
}

TEST_F(RawExprCacheAdapterTest, ExternalMoveCursorPoisonsFullCoverage) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [input, adapter] = MakeAdapter("raw:external-move");
    adapter->MoveCursor();

    int64_t processed = kBatchSize;
    while (processed < kActiveCount) {
        VectorPtr result;
        adapter->Eval(eval_context, result);
        ASSERT_NE(result, nullptr);
        processed += result->size();
    }
    EXPECT_EQ(input->eval_count, 2);
    EXPECT_EQ(input->move_count, 3);

    const auto key = RawCacheKey(*input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(ExprResCacheManager::Instance().Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, AdmissionIsObservedOncePerExpression) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    auto [first_input, first_adapter] = MakeAdapter("raw:admission");
    const auto key = RawCacheKey(*first_input);
    EvalSequential(first_adapter, eval_context);
    ASSERT_EQ(first_input->eval_count, 3);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(key, cached));

    auto [second_input, second_adapter] = MakeAdapter("raw:admission");
    EvalSequential(second_adapter, eval_context);
    ASSERT_EQ(second_input->eval_count, 3);
    cached.active_count = kActiveCount;
    EXPECT_TRUE(manager.Get(key, cached));

    auto [hit_input, hit_adapter] = MakeAdapter("raw:admission");
    EvalSequential(hit_adapter, eval_context);
    EXPECT_EQ(hit_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest, ReadOnlyMissDoesNotObserveAdmissionOrWrite) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    auto read_only_input = std::make_shared<CountingRawExpr>(
        segment_.get(), field_id_, "raw:read-only-miss");
    const auto key = RawCacheKey(*read_only_input);
    std::vector<ExprPtr> exprs{read_only_input};
    DecorateRawExprCache(exprs, nullptr, false);
    auto read_only_adapter =
        std::dynamic_pointer_cast<RawExprCacheAdapter>(exprs[0]);
    ASSERT_NE(read_only_adapter, nullptr);
    EvalSequential(read_only_adapter, eval_context);
    ASSERT_EQ(read_only_input->eval_count, 3);

    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(key, cached));

    // The read-only miss must not count toward frequency admission. Therefore
    // the first writable miss is still below threshold and cannot populate.
    auto [first_writer_input, first_writer_adapter] =
        MakeAdapter("raw:read-only-miss");
    EvalSequential(first_writer_adapter, eval_context);
    ASSERT_EQ(first_writer_input->eval_count, 3);
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(key, cached));

    auto [second_writer_input, second_writer_adapter] =
        MakeAdapter("raw:read-only-miss");
    EvalSequential(second_writer_adapter, eval_context);
    ASSERT_EQ(second_writer_input->eval_count, 3);
    cached.active_count = kActiveCount;
    EXPECT_TRUE(manager.Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, MoveOnlyExpressionDoesNotCountForAdmission) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    auto [skipped_input, skipped_adapter] =
        MakeAdapter("raw:move-only-admission");
    const auto key = RawCacheKey(*skipped_input);
    skipped_adapter->MoveCursor();
    EXPECT_EQ(skipped_input->eval_count, 0);

    auto [first_input, first_adapter] = MakeAdapter("raw:move-only-admission");
    EvalSequential(first_adapter, eval_context);
    ASSERT_EQ(first_input->eval_count, 3);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(key, cached));

    auto [second_input, second_adapter] =
        MakeAdapter("raw:move-only-admission");
    EvalSequential(second_adapter, eval_context);
    ASSERT_EQ(second_input->eval_count, 3);
    cached.active_count = kActiveCount;
    EXPECT_TRUE(manager.Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, PartialBitmapInputDoesNotPopulateCache) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [input, adapter] = MakeAdapter("raw:masked");
    int64_t processed = 0;
    while (processed < kActiveCount) {
        const auto batch = std::min(kBatchSize, kActiveCount - processed);
        TargetBitmap mask(batch, true);
        mask[0] = false;
        eval_context.set_bitmap_input(std::move(mask));
        VectorPtr result;
        adapter->Eval(eval_context, result);
        ASSERT_NE(result, nullptr);
        processed += result->size();
    }
    EXPECT_EQ(input->eval_count, 3);

    const auto key = RawCacheKey(*input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(ExprResCacheManager::Instance().Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, EvaluationExceptionDoesNotPutPartialResult) {
    auto& manager = ExprResCacheManager::Instance();
    auto& gauge = milvus::cachinglayer::monitor::cache_loaded_bytes(
        milvus::cachinglayer::CellDataType::OTHER,
        milvus::cachinglayer::StorageType::MEMORY);
    const auto baseline = gauge.Value();
    const auto pair_bytes = 2 * ((kActiveCount + 63) / 64) * 8;
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [input, adapter] = MakeAdapter("raw:exception");
    input->throw_on_eval_count = 2;

    VectorPtr first;
    adapter->Eval(eval_context, first);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(manager.GetMaterializationBytes(), pair_bytes);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline + pair_bytes);
    EXPECT_THROW(
        {
            VectorPtr second;
            adapter->Eval(eval_context, second);
        },
        std::runtime_error);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0u);
    EXPECT_DOUBLE_EQ(gauge.Value(), baseline);

    const auto key = RawCacheKey(*input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(ExprResCacheManager::Instance().Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, CacheHitGathersOffsetsAndValidity) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx sequential_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:offsets");
    EvalSequential(miss_adapter, sequential_context);
    ASSERT_EQ(miss_input->eval_count, 3);

    OffsetVector offsets{9, 2, 9, 0, 17};
    EvalCtx offset_context(&exec_context, &offsets);
    auto [hit_input, hit_adapter] = MakeAdapter("raw:offsets");
    VectorPtr result;
    hit_adapter->Eval(offset_context, result);

    auto column = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(column->size(), offsets.size());
    TargetBitmapView data(column->GetRawData(), column->size());
    TargetBitmapView valid(column->GetValidRawData(), column->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(data[i],
                  CountingRawExpr::IsValid(offsets[i]) &&
                      CountingRawExpr::Matches(offsets[i]));
        EXPECT_EQ(valid[i], CountingRawExpr::IsValid(offsets[i]));
    }
    EXPECT_EQ(hit_input->eval_count, 0);
    EXPECT_EQ(hit_input->move_count, 0);

    hit_adapter->MoveCursor();
    EXPECT_EQ(hit_input->move_count, 1);
    EXPECT_EQ(hit_input->cursor_pos(), 0);
}

TEST_F(RawExprCacheAdapterTest, CacheHitPreservesEmptyOffsetExhaustion) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx sequential_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:empty-offsets");
    EvalSequential(miss_adapter, sequential_context);
    ASSERT_EQ(miss_input->eval_count, 3);

    OffsetVector offsets;
    EvalCtx offset_context(&exec_context, &offsets);
    auto [hit_input, hit_adapter] = MakeAdapter("raw:empty-offsets");
    VectorPtr result = std::make_shared<ColumnVector>(TargetBitmap(1, true),
                                                      TargetBitmap(1, true));
    hit_adapter->Eval(offset_context, result);

    EXPECT_EQ(result, nullptr);
    EXPECT_EQ(hit_input->eval_count, 0);
    EXPECT_EQ(hit_input->move_count, 0);
    EXPECT_EQ(hit_input->cursor_pos(), 0);
}

TEST_F(RawExprCacheAdapterTest, CacheHitReturnsLogicalValueDespiteWorkMask) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx fill_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:hit-work-mask");
    EvalSequential(miss_adapter, fill_context);
    ASSERT_EQ(miss_input->eval_count, 3);

    EvalCtx hit_context(&exec_context);
    hit_context.set_bitmap_input(TargetBitmap(kBatchSize, false));
    auto [hit_input, hit_adapter] = MakeAdapter("raw:hit-work-mask");
    VectorPtr result;
    hit_adapter->Eval(hit_context, result);

    auto column = std::dynamic_pointer_cast<ColumnVector>(result);
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(column->size(), kBatchSize);
    TargetBitmapView data(column->GetRawData(), column->size());
    TargetBitmapView valid(column->GetValidRawData(), column->size());
    for (int64_t row = 0; row < kBatchSize; ++row) {
        EXPECT_EQ(
            data[row],
            CountingRawExpr::IsValid(row) && CountingRawExpr::Matches(row));
        EXPECT_EQ(valid[row], CountingRawExpr::IsValid(row));
    }
    EXPECT_EQ(hit_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest, CacheHitSkipsWrappedRawPrefetch) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:prefetch-hit");
    EvalSequential(miss_adapter, eval_context);
    ASSERT_EQ(miss_input->eval_count, 3);

    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    auto [hit_input, hit_adapter] = MakeAdapter("raw:prefetch-hit");
    hit_adapter->PrefetchAsync(pool);
    hit_adapter->WaitPrefetch();
    EXPECT_EQ(hit_input->prefetch_count, 0);

    VectorPtr result;
    hit_adapter->Eval(eval_context, result);
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(hit_input->eval_count, 0);

    auto [new_miss_input, new_miss_adapter] = MakeAdapter("raw:prefetch-miss");
    new_miss_adapter->PrefetchAsync(pool);
    new_miss_adapter->WaitPrefetch();
    EXPECT_EQ(new_miss_input->prefetch_count, 1);
}

TEST_F(RawExprCacheAdapterTest, CacheMissPrefetchUsesSinglePoolTask) {
    auto pool = std::make_shared<CountingCPUThreadPoolExecutor>(1);
    auto [input, adapter] = MakeAdapter("raw:prefetch-single-task");

    adapter->PrefetchAsync(pool);
    adapter->WaitPrefetch();

    EXPECT_EQ(input->prefetch_count, 1);
    EXPECT_EQ(pool->submitted_tasks(), 1);
}

TEST_F(RawExprCacheAdapterTest, RawPrefetchContributesToLatencyAdmission) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 50'000;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    auto [fast_input, fast_adapter] = MakeAdapter("raw:prefetch-latency-fast");
    fast_adapter->PrefetchAsync(pool);
    EvalSequential(fast_adapter, eval_context);
    EXPECT_EQ(fast_input->prefetch_count, 1);
    const auto fast_key = RawCacheKey(*fast_input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(fast_key, cached));

    auto [slow_input, slow_adapter] = MakeAdapter("raw:prefetch-latency-slow");
    slow_input->prefetch_delay = std::chrono::milliseconds(100);
    slow_adapter->PrefetchAsync(pool);
    EvalSequential(slow_adapter, eval_context);
    EXPECT_EQ(slow_input->prefetch_count, 1);
    const auto slow_key = RawCacheKey(*slow_input);
    cached.active_count = kActiveCount;
    ASSERT_TRUE(manager.Get(slow_key, cached));

    auto [hit_input, hit_adapter] = MakeAdapter("raw:prefetch-latency-slow");
    hit_adapter->PrefetchAsync(pool);
    hit_adapter->WaitPrefetch();
    EXPECT_EQ(hit_input->prefetch_count, 0);

    VectorPtr result;
    hit_adapter->Eval(eval_context, result);
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(hit_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest, CacheHitRetainsBitmapsAcrossGlobalClear) {
    QueryContext query_context(
        "raw_cache_test", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto [miss_input, miss_adapter] = MakeAdapter("raw:retained-hit");
    TargetBitmap expected_data;
    TargetBitmap expected_valid;
    EvalSequential(miss_adapter, eval_context, &expected_data, &expected_valid);
    ASSERT_EQ(miss_input->eval_count, 3);

    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    auto [hit_input, hit_adapter] = MakeAdapter("raw:retained-hit");
    hit_adapter->PrefetchAsync(pool);
    hit_adapter->WaitPrefetch();
    ExprResCacheManager::Instance().Clear();

    TargetBitmap actual_data;
    TargetBitmap actual_valid;
    EXPECT_EQ(
        EvalSequential(hit_adapter, eval_context, &actual_data, &actual_valid),
        (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(hit_input->eval_count, 0);
    EXPECT_TRUE(actual_data == expected_data);
    EXPECT_TRUE(actual_valid == expected_valid);
}

TEST_F(RawExprCacheAdapterTest, MemoryModeRejectsGrowingByDefault) {
    auto data = segcore::DataGen(schema_, kActiveCount, 84);
    auto growing = segcore::CreateGrowingSegment(schema_, empty_index_meta);
    growing->PreInsert(kActiveCount);
    growing->Insert(0,
                    kActiveCount,
                    data.row_ids_.data(),
                    data.timestamps_.data(),
                    data.raw_);

    QueryContext query_context(
        "raw_cache_growing", growing.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto undecorated = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing-decoration");
    std::vector<ExprPtr> exprs{undecorated};
    DecorateRawExprCache(exprs, nullptr, true);
    ASSERT_EQ(exprs.size(), 1u);
    EXPECT_EQ(exprs[0], undecorated);

    auto miss_input = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing");
    auto miss_adapter =
        std::make_shared<RawExprCacheAdapter>(miss_input, nullptr, true);
    EXPECT_EQ(EvalSequential(miss_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(miss_input->eval_count, 3);

    auto second_input = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing");
    auto second_adapter =
        std::make_shared<RawExprCacheAdapter>(second_input, nullptr, true);
    EXPECT_EQ(EvalSequential(second_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(second_input->eval_count, 3);

    const auto key = RawCacheKey(*miss_input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(ExprResCacheManager::Instance().Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, MemoryModeCachesGrowingWhenEnabled) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.mem_enable_growing = true;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    auto data = segcore::DataGen(schema_, kActiveCount, 85);
    auto growing = segcore::CreateGrowingSegment(schema_, empty_index_meta);
    growing->PreInsert(kActiveCount);
    growing->Insert(0,
                    kActiveCount,
                    data.row_ids_.data(),
                    data.timestamps_.data(),
                    data.raw_);

    QueryContext query_context(
        "raw_cache_growing", growing.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    auto undecorated = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing-decoration-enabled");
    std::vector<ExprPtr> exprs{undecorated};
    DecorateRawExprCache(exprs, nullptr, true);
    ASSERT_EQ(exprs.size(), 1u);
    EXPECT_NE(std::dynamic_pointer_cast<RawExprCacheAdapter>(exprs[0]),
              nullptr);

    auto miss_input = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing-enabled");
    auto miss_adapter =
        std::make_shared<RawExprCacheAdapter>(miss_input, nullptr, true);
    EXPECT_EQ(EvalSequential(miss_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(miss_input->eval_count, 3);

    auto hit_input = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:memory-growing-enabled");
    auto hit_adapter =
        std::make_shared<RawExprCacheAdapter>(hit_input, nullptr, true);
    EXPECT_EQ(EvalSequential(hit_adapter, eval_context),
              (std::vector<int64_t>{1024, 1024, 452}));
    EXPECT_EQ(hit_input->eval_count, 0);
}

TEST_F(RawExprCacheAdapterTest, GrowingAdmissionSkipsCaptureForNewSnapshots) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.mem_enable_growing = true;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));

    auto growing = segcore::CreateGrowingSegment(schema_, empty_index_meta);
    auto append_rows = [&](int64_t rows) {
        auto data =
            segcore::DataGen(schema_, rows, 86, growing->get_row_count());
        const auto offset = growing->PreInsert(rows);
        growing->Insert(offset,
                        rows,
                        data.row_ids_.data(),
                        data.timestamps_.data(),
                        data.raw_);
    };
    const std::string signature = "raw:growing-snapshot-admission";
    auto evaluate = [&](bool expect_capture, bool expect_hit) {
        const auto active_count = growing->get_row_count();
        QueryContext query_context("raw_cache_growing_admission",
                                   growing.get(),
                                   active_count,
                                   MAX_TIMESTAMP);
        ExecContext exec_context(&query_context);
        EvalCtx eval_context(&exec_context);
        auto input = std::make_shared<CountingRawExpr>(
            growing.get(), field_id_, signature, active_count);
        auto adapter =
            std::make_shared<RawExprCacheAdapter>(input, nullptr, true);

        int64_t processed = 0;
        while (processed < active_count) {
            VectorPtr result;
            adapter->Eval(eval_context, result);
            auto column = std::dynamic_pointer_cast<ColumnVector>(result);
            ASSERT_NE(column, nullptr);
            ASSERT_GT(column->size(), 0);
            TargetBitmapView data(column->GetRawData(), column->size());
            TargetBitmapView valid(column->GetValidRawData(), column->size());
            for (size_t i = 0; i < column->size(); ++i) {
                EXPECT_EQ(valid[i], CountingRawExpr::IsValid(processed + i));
                EXPECT_EQ(data[i],
                          CountingRawExpr::IsValid(processed + i) &&
                              CountingRawExpr::Matches(processed + i));
            }
            if (processed == 0) {
                // The first batch leaves any full-segment capture in flight.
                // A one-off snapshot must never reserve its bitmap pair.
                const size_t full_bitmap_bytes =
                    2 * ((active_count + 63) / 64) * sizeof(uint64_t);
                EXPECT_EQ(manager.GetMaterializationBytes(),
                          expect_capture || expect_hit ? full_bitmap_bytes : 0);
            }
            processed += column->size();
        }
        EXPECT_EQ(processed, active_count);
        EXPECT_EQ(
            input->eval_count,
            expect_hit ? 0 : (active_count + kBatchSize - 1) / kBatchSize);
    };

    append_rows(kActiveCount);
    evaluate(false, false);
    EXPECT_EQ(manager.GetEntryCount(), 0);
    append_rows(64);
    evaluate(false, false);
    EXPECT_EQ(manager.GetEntryCount(), 0);

    evaluate(true, false);
    EXPECT_EQ(manager.GetEntryCount(), 1);
    evaluate(false, true);

    append_rows(64);
    evaluate(false, false);
    evaluate(true, false);
    evaluate(false, true);
    EXPECT_EQ(manager.GetEntryCount(), 1);
    EXPECT_EQ(manager.GetMaterializationBytes(), 0);

    const auto key = RawCacheKey(CountingRawExpr(
        growing.get(), field_id_, signature, growing->get_row_count()));
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount + 64;
    EXPECT_FALSE(manager.Get(key, cached));
}

TEST_F(RawExprCacheAdapterTest, DiskModeCachesSealedButRejectsGrowing) {
    auto cache_dir = std::filesystem::temp_directory_path() /
                     ("raw_expr_cache_adapter_" + std::to_string(::getpid()) +
                      "_" + std::to_string(std::rand()));
    std::filesystem::create_directories(cache_dir);

    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Disk;
    config.disk_base_path = cache_dir.string();
    config.disk_max_bytes = 4U << 20;
    config.disk_max_file_size = 1U << 20;
    config.admission_threshold = 1;
    config.disk_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext sealed_query(
        "raw_cache_disk_sealed", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext sealed_exec_context(&sealed_query);
    EvalCtx sealed_eval_context(&sealed_exec_context);
    auto [sealed_miss_input, sealed_miss_adapter] =
        MakeAdapter("raw:disk-sealed");
    EvalSequential(sealed_miss_adapter, sealed_eval_context);
    ASSERT_EQ(sealed_miss_input->eval_count, 3);

    auto [sealed_hit_input, sealed_hit_adapter] =
        MakeAdapter("raw:disk-sealed");
    EvalSequential(sealed_hit_adapter, sealed_eval_context);
    EXPECT_EQ(sealed_hit_input->eval_count, 0);

    manager.Clear();
    auto data = segcore::DataGen(schema_, kActiveCount, 126);
    auto growing = segcore::CreateGrowingSegment(schema_, empty_index_meta);
    growing->PreInsert(kActiveCount);
    growing->Insert(0,
                    kActiveCount,
                    data.row_ids_.data(),
                    data.timestamps_.data(),
                    data.raw_);
    QueryContext growing_query(
        "raw_cache_disk_growing", growing.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext growing_exec_context(&growing_query);
    EvalCtx growing_eval_context(&growing_exec_context);
    auto growing_input = std::make_shared<CountingRawExpr>(
        growing.get(), field_id_, "raw:disk-growing");
    auto growing_adapter =
        std::make_shared<RawExprCacheAdapter>(growing_input, nullptr, true);
    EvalSequential(growing_adapter, growing_eval_context);
    EXPECT_EQ(growing_input->eval_count, 3);

    const auto growing_key = RawCacheKey(*growing_input);
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    EXPECT_FALSE(manager.Get(growing_key, cached));

    manager.Clear();
    std::filesystem::remove_all(cache_dir);
}

TEST_F(RawExprCacheAdapterTest,
       CompileGateKeepsDisabledTreeUnchangedAndInstallsReadOnlyAdapter) {
    proto::plan::GenericValue value;
    value.set_int64_val(42);
    auto logical = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_id_, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        value,
        std::vector<proto::plan::GenericValue>{});

    ExprResCacheManager::SetEnabled(false);
    QueryContext disabled_query(
        "raw_cache_disabled", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext disabled_context(&disabled_query);
    auto disabled = CompileExpressions({logical}, &disabled_context, {}, false);
    ASSERT_EQ(disabled.size(), 1);
    EXPECT_NE(std::dynamic_pointer_cast<PhyUnaryRangeFilterExpr>(disabled[0]),
              nullptr);
    EXPECT_EQ(std::dynamic_pointer_cast<RawExprCacheAdapter>(disabled[0]),
              nullptr);

    ExprResCacheManager::SetEnabled(true);
    QueryContext enabled_query(
        "raw_cache_enabled", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext enabled_context(&enabled_query);
    auto enabled = CompileExpressions({logical}, &enabled_context, {}, false);
    ASSERT_EQ(enabled.size(), 1);
    EXPECT_NE(std::dynamic_pointer_cast<RawExprCacheAdapter>(enabled[0]),
              nullptr);

    QueryContext two_stage_query(
        "raw_cache_two_stage", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    two_stage_query.set_enable_sub_expr_cache_write(false);
    ExecContext two_stage_context(&two_stage_query);
    auto two_stage =
        CompileExpressions({logical}, &two_stage_context, {}, false);
    ASSERT_EQ(two_stage.size(), 1);
    EXPECT_NE(std::dynamic_pointer_cast<RawExprCacheAdapter>(two_stage[0]),
              nullptr);
}

TEST_F(RawExprCacheAdapterTest, InitialRawExpressionScopeIsDecorated) {
    proto::plan::GenericValue one;
    one.set_int64_val(1);
    proto::plan::GenericValue ten;
    ten.set_int64_val(10);

    const auto column = expr::ColumnInfo(field_id_, DataType::INT64);
    std::vector<expr::TypedExprPtr> logical_exprs;
    logical_exprs.push_back(std::make_shared<expr::UnaryRangeFilterExpr>(
        column, proto::plan::OpType::GreaterThan, one));
    logical_exprs.push_back(
        std::make_shared<expr::TermFilterExpr>(column, std::vector{one, ten}));
    logical_exprs.push_back(std::make_shared<expr::BinaryRangeFilterExpr>(
        column, one, ten, true, false));
    logical_exprs.push_back(std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        column,
        proto::plan::OpType::GreaterThan,
        proto::plan::ArithOpType::Add,
        ten,
        one));

    QueryContext query_context(
        "raw_cache_scope", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    auto compiled = CompileExpressions(logical_exprs, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), logical_exprs.size());
    for (const auto& physical_expr : compiled) {
        EXPECT_NE(std::dynamic_pointer_cast<RawExprCacheAdapter>(physical_expr),
                  nullptr);
    }
}

TEST_F(RawExprCacheAdapterTest,
       UnscopedEntryIsNotReusedByEitherNullRejectionMode) {
    auto& manager = ExprResCacheManager::Instance();
    QueryContext query_context(
        "raw_cache_unscoped", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);

    for (bool null_rejecting : {false, true}) {
        manager.Clear();
        auto [input, adapter] = MakeAdapter("raw:unscoped-entry");
        if (null_rejecting) {
            adapter->MarkNullRejecting();
        }
        // Other execution paths use plain ToString(). An entry without raw
        // null semantics must not alias either mode's lookup or insertion.
        ExprResCacheManager::Key unscoped{segment_->get_segment_id(),
                                          input->ToString()};
        ExprResCacheManager::Value value;
        value.active_count = kActiveCount;
        value.result = std::make_shared<TargetBitmap>(kActiveCount, true);
        value.valid_result = std::make_shared<TargetBitmap>(kActiveCount, true);
        manager.Put(unscoped, value);
        ASSERT_EQ(manager.GetEntryCount(), 1u);

        TargetBitmap data;
        TargetBitmap valid;
        EvalSequential(adapter, eval_context, &data, &valid);
        EXPECT_EQ(input->eval_count, 3);
        EXPECT_FALSE(data.all());
        EXPECT_FALSE(valid.all());
        EXPECT_EQ(manager.GetEntryCount(), 2u);
    }
}

TEST_F(RawExprCacheAdapterTest,
       NullRejectionModesHaveIndependentAdmissionAndHits) {
    auto& manager = ExprResCacheManager::Instance();
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    QueryContext query_context(
        "raw_cache_null_modes", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);
    EvalCtx eval_context(&exec_context);
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);

    for (int pass = 0; pass < 3; ++pass) {
        for (bool null_rejecting : {true, false}) {
            SCOPED_TRACE(::testing::Message()
                         << "pass=" << pass
                         << " null_rejecting=" << null_rejecting);
            auto [input, adapter] = MakeAdapter("raw:null-modes");
            if (null_rejecting) {
                adapter->MarkNullRejecting();
            }
            adapter->PrefetchAsync(pool);
            EvalSequential(adapter, eval_context);
            EXPECT_EQ(input->eval_count, pass < 2 ? 3 : 0);
            EXPECT_EQ(input->prefetch_count, pass < 2 ? 1 : 0);
        }
        EXPECT_EQ(manager.GetEntryCount(), pass == 0 ? 0u : 2u);
    }
}

class RawExprCacheNullRejectionTest
    : public RawExprCacheAdapterTest,
      public ::testing::WithParamInterface<CacheMode> {};

TEST_P(RawExprCacheNullRejectionTest, SkippedNullsRemainUnknownUnderNot) {
    auto& manager = ExprResCacheManager::Instance();
    const auto cache_dir =
        std::filesystem::temp_directory_path() /
        ("raw_cache_null_modes_" + std::to_string(::getpid()) + "_" +
         std::to_string(std::rand()));
    const auto cleanup = folly::makeGuard([&]() {
        manager.Clear();
        std::filesystem::remove_all(cache_dir);
    });
    CacheConfig config;
    config.mode = GetParam();
    config.mem_max_bytes = 1U << 20;
    config.disk_base_path = cache_dir.string();
    config.disk_max_bytes = 4U << 20;
    config.disk_max_file_size = 1U << 20;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    config.disk_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    auto schema = std::make_shared<Schema>();
    const auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    const auto field = schema->AddDebugField("value", DataType::INT64, true);
    auto dataset = segcore::DataGen(schema, kActiveCount);
    TargetBitmap expected_valid(kActiveCount, false);
    for (auto& field_data : *dataset.raw_->mutable_fields_data()) {
        if (field_data.field_id() != field.get()) {
            continue;
        }
        auto* scalars = field_data.mutable_scalars();
        auto* values = scalars->mutable_long_data()->mutable_data();
        std::fill(values->begin(), values->end(), 5);
        ASSERT_EQ(scalars->valid_data_size(), kActiveCount);
        for (int64_t row = 0; row < kActiveCount; ++row) {
            expected_valid[row] = scalars->valid_data(row);
        }
    }
    ASSERT_FALSE(expected_valid.none());
    ASSERT_FALSE(expected_valid.all());
    auto segment = CreateSealedWithFieldDataLoaded(schema, dataset);
    ASSERT_EQ(segment->num_chunk_data(field), 1);
    // Supply known min/max metadata, then use the production SkipIndex,
    // raw evaluator, cache adapter, and NOT operator throughout the test.
    InstallTestSkipMetrics(
        segment.get(),
        field,
        {std::make_shared<index::IntFieldChunkMetrics<int64_t>>(
            5, 5, nullptr)});
    ASSERT_TRUE(segment->GetFieldSkipMetrics(field).CanSkipUnaryRange<int64_t>(
        0, proto::plan::OpType::GreaterThan, 10));

    proto::plan::GenericValue value;
    value.set_int64_val(10);
    auto predicate = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        value);
    auto logical_not = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, predicate);
    QueryContext query_context(
        "raw_cache_nullable_skip", segment.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);

    auto evaluate = [&](bool negate) {
        expr::TypedExprPtr logical = negate ? expr::TypedExprPtr(logical_not)
                                            : expr::TypedExprPtr(predicate);
        // This is the same marking entry point used by FilterBitsNode.
        // NOT must stop propagation to its wrapped raw leaf.
        ExprSet expressions({logical}, &exec_context, /*null_rejecting=*/true);
        EvalCtx eval_context(&exec_context);
        TargetBitmap data;
        TargetBitmap valid;
        while (data.size() < kActiveCount) {
            std::vector<VectorPtr> results;
            expressions.Eval(eval_context, results);
            ASSERT_EQ(results.size(), 1);
            auto column = std::dynamic_pointer_cast<ColumnVector>(results[0]);
            ASSERT_NE(column, nullptr);
            ASSERT_GT(column->size(), 0);
            data.append(TargetBitmapView(column->GetRawData(), column->size()));
            valid.append(
                TargetBitmapView(column->GetValidRawData(), column->size()));
        }
        ASSERT_EQ(data.size(), kActiveCount);
        if (negate) {
            EXPECT_TRUE(valid == expected_valid);
            EXPECT_TRUE(data == expected_valid)
                << "NOT must select only non-NULL rows";
        } else {
            EXPECT_TRUE(data.none());
            // Prove the null-rejecting SkipIndex path actually omitted
            // validity, so reusing this entry below NOT would be unsafe.
            EXPECT_TRUE(valid.all());
        }
    };

    // Both warming orders, followed by repeated reads in each mode.
    for (bool negate_first : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "negate_first=" << negate_first);
        manager.Clear();
        evaluate(negate_first);
        ASSERT_EQ(manager.GetEntryCount(), 1u);
        evaluate(!negate_first);
        EXPECT_EQ(manager.GetEntryCount(), 2u);
        evaluate(negate_first);
        evaluate(!negate_first);
        EXPECT_EQ(manager.GetEntryCount(), 2u);
    }
}

INSTANTIATE_TEST_SUITE_P(MemoryAndDisk,
                         RawExprCacheNullRejectionTest,
                         ::testing::Values(CacheMode::Memory, CacheMode::Disk));

TEST_F(RawExprCacheAdapterTest, UnaryAllSkipRawResultIsCapturedAndReused) {
    proto::plan::GenericValue value;
    value.set_int64_val(std::numeric_limits<int64_t>::max());
    auto logical = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(field_id_, DataType::INT64),
        proto::plan::OpType::GreaterThan,
        value);

    QueryContext query_context(
        "raw_cache_real_unary", segment_.get(), kActiveCount, MAX_TIMESTAMP);
    ExecContext exec_context(&query_context);

    auto evaluate = [&](const ExprPtr& physical_expr) {
        EvalCtx eval_context(&exec_context);
        TargetBitmap data;
        TargetBitmap valid;
        while (static_cast<int64_t>(data.size()) < kActiveCount) {
            VectorPtr result;
            physical_expr->Eval(eval_context, result);
            auto column = std::dynamic_pointer_cast<ColumnVector>(result);
            EXPECT_NE(column, nullptr);
            if (column == nullptr) {
                break;
            }
            data.append(TargetBitmapView(column->GetRawData(), column->size()));
            valid.append(
                TargetBitmapView(column->GetValidRawData(), column->size()));
        }
        return std::pair{std::move(data), std::move(valid)};
    };

    auto first = CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(first.size(), 1);
    auto first_adapter =
        std::dynamic_pointer_cast<RawExprCacheAdapter>(first[0]);
    ASSERT_NE(first_adapter, nullptr);
    auto [first_data, first_valid] = evaluate(first[0]);
    EXPECT_EQ(first_data.size(), kActiveCount);
    EXPECT_TRUE(first_data.none());
    EXPECT_TRUE(first_valid.all());

    const auto key = RawCacheKey(*first_adapter->wrapped_expr());
    ExprResCacheManager::Value cached;
    cached.active_count = kActiveCount;
    ASSERT_TRUE(ExprResCacheManager::Instance().Get(key, cached));

    auto second = CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(second.size(), 1);
    auto [second_data, second_valid] = evaluate(second[0]);
    EXPECT_TRUE(first_data == second_data);
    EXPECT_TRUE(first_valid == second_valid);
}

TEST(ExprResCacheAdmissionTicketTest,
     ForwardAdmissionDoesNotApplyFrequencyTwice) {
    auto& manager = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 2;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    ExprResCacheManager::Key key{7001, "ticket:threshold"};
    EXPECT_FALSE(manager.ObserveMiss(key, 128, SegmentType::Sealed).admitted);
    auto ticket = manager.ObserveMiss(key, 128, SegmentType::Sealed);
    ASSERT_TRUE(ticket.admitted);

    ExprResCacheManager::Value value;
    value.result = std::make_shared<TargetBitmap>(128, true);
    value.valid_result = std::make_shared<TargetBitmap>(128, true);
    value.active_count = 128;
    value.eval_duration_us = 1;
    manager.PutAdmitted(key, value, ticket);

    ExprResCacheManager::Value cached;
    cached.active_count = 128;
    EXPECT_TRUE(manager.Get(key, cached));

    manager.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheAdmissionTicketTest, RejectsMismatchedAndStaleTickets) {
    auto& manager = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 0;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    ExprResCacheManager::Value value;
    value.result = std::make_shared<TargetBitmap>(64, true);
    value.valid_result = std::make_shared<TargetBitmap>(64, true);
    value.active_count = 64;
    value.eval_duration_us = 1;

    ExprResCacheManager::Key original{7002, "ticket:original"};
    auto mismatch_ticket =
        manager.ObserveMiss(original, 64, SegmentType::Sealed);
    ASSERT_TRUE(mismatch_ticket.admitted);
    ExprResCacheManager::Key different{7002, "ticket:different"};
    manager.PutAdmitted(different, value, mismatch_ticket);
    ExprResCacheManager::Value cached;
    cached.active_count = 64;
    EXPECT_FALSE(manager.Get(different, cached));

    // A ticket cannot authorize another row-count snapshot of the same key.
    auto snapshot_ticket =
        manager.ObserveMiss(original, 128, SegmentType::Sealed);
    ASSERT_TRUE(snapshot_ticket.admitted);
    manager.PutAdmitted(original, value, snapshot_ticket);
    cached.active_count = 64;
    EXPECT_FALSE(manager.Get(original, cached));

    auto stale_ticket = manager.ObserveMiss(original, 64, SegmentType::Sealed);
    ASSERT_TRUE(stale_ticket.admitted);
    ASSERT_TRUE(manager.SetConfig(config));
    manager.PutAdmitted(original, value, stale_ticket);
    cached.active_count = 64;
    EXPECT_FALSE(manager.Get(original, cached));

    auto toggled_ticket =
        manager.ObserveMiss(original, 64, SegmentType::Sealed);
    ASSERT_TRUE(toggled_ticket.admitted);
    ExprResCacheManager::SetEnabled(false);
    ExprResCacheManager::SetEnabled(true);
    manager.PutAdmitted(original, value, toggled_ticket);
    cached.active_count = 64;
    EXPECT_FALSE(manager.Get(original, cached));

    manager.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheAdmissionTicketTest, GrowingEligibilityIsBoundToTicketEpoch) {
    auto& manager = ExprResCacheManager::Instance();
    const auto cache_dir =
        std::filesystem::temp_directory_path() /
        ("expr_cache_ticket_eligibility_" + std::to_string(::getpid()) + "_" +
         std::to_string(std::rand()));
    const auto cleanup = folly::makeGuard([&]() {
        manager.Clear();
        ExprResCacheManager::SetEnabled(false);
        std::filesystem::remove_all(cache_dir);
    });
    ExprResCacheManager::SetEnabled(true);

    ExprResCacheManager::Key growing_key{7004, "ticket:growing-refresh"};
    ExprResCacheManager::Key sealed_key{7005, "ticket:sealed-refresh"};
    ExprResCacheManager::Value value;
    value.result = std::make_shared<TargetBitmap>(64, true);
    value.valid_result = std::make_shared<TargetBitmap>(64, true);
    value.active_count = 64;
    value.eval_duration_us = 1;

    for (auto next_mode : {CacheMode::Memory, CacheMode::Disk}) {
        SCOPED_TRACE(next_mode == CacheMode::Memory ? "disable growing"
                                                    : "switch to disk");
        CacheConfig config;
        config.mode = CacheMode::Memory;
        config.mem_max_bytes = 1U << 20;
        config.mem_enable_growing = true;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        config.disk_base_path = cache_dir.string();
        config.disk_max_bytes = 1U << 20;
        config.disk_max_file_size = 1U << 20;
        config.disk_min_eval_duration_us = 0;
        ASSERT_TRUE(manager.SetConfig(config));

        ASSERT_TRUE(manager.CanCacheSegment(SegmentType::Growing));
        const auto old_ticket =
            manager.ObserveMiss(growing_key, 64, SegmentType::Growing);
        ASSERT_TRUE(old_ticket.admitted);

        // Refresh after an eligible precheck. Disk must reject growing even
        // when the memory-mode growing opt-in remains enabled.
        config.mode = next_mode;
        config.mem_enable_growing = next_mode == CacheMode::Disk;
        ASSERT_TRUE(manager.SetConfig(config));
        const auto new_ticket =
            manager.ObserveMiss(growing_key, 64, SegmentType::Growing);
        EXPECT_FALSE(new_ticket.admitted);
        manager.PutAdmitted(growing_key, value, new_ticket);
        EXPECT_EQ(manager.GetEntryCount(), 0);
        EXPECT_EQ(manager.GetCurrentBytes(), 0);

        // A ticket issued before the same refresh must also reject the write.
        manager.PutAdmitted(growing_key, value, old_ticket);
        EXPECT_EQ(manager.GetEntryCount(), 0);
        EXPECT_EQ(manager.GetCurrentBytes(), 0);
        if (next_mode == CacheMode::Disk) {
            EXPECT_TRUE(std::filesystem::is_empty(cache_dir));
        }

        const auto sealed_ticket =
            manager.ObserveMiss(sealed_key, 64, SegmentType::Sealed);
        ASSERT_TRUE(sealed_ticket.admitted);
        manager.PutAdmitted(sealed_key, value, sealed_ticket);
        ExprResCacheManager::Value cached;
        cached.active_count = 64;
        ASSERT_TRUE(manager.Get(sealed_key, cached));
        EXPECT_TRUE(*cached.result == *value.result);
        EXPECT_TRUE(*cached.valid_result == *value.valid_result);
    }
}

TEST(ExprResCacheAdmissionTicketTest,
     ForwardAdmissionStillAppliesLatencyThreshold) {
    auto& manager = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.mem_max_bytes = 1U << 20;
    config.compression_enabled = false;
    config.admission_threshold = 1;
    config.mem_min_eval_duration_us = 100;
    ASSERT_TRUE(manager.SetConfig(config));
    manager.Clear();

    ExprResCacheManager::Key key{7003, "ticket:latency"};
    auto ticket = manager.ObserveMiss(key, 64, SegmentType::Sealed);
    ASSERT_TRUE(ticket.admitted);

    ExprResCacheManager::Value value;
    value.result = std::make_shared<TargetBitmap>(64, true);
    value.valid_result = std::make_shared<TargetBitmap>(64, true);
    value.active_count = 64;
    value.eval_duration_us = 99;
    manager.PutAdmitted(key, value, ticket);

    ExprResCacheManager::Value cached;
    cached.active_count = 64;
    EXPECT_FALSE(manager.Get(key, cached));

    value.eval_duration_us = 100;
    manager.PutAdmitted(key, value, ticket);
    cached.active_count = 64;
    EXPECT_TRUE(manager.Get(key, cached));

    manager.Clear();
    ExprResCacheManager::SetEnabled(false);
}

}  // namespace
}  // namespace milvus::exec
