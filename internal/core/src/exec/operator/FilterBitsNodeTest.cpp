// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <folly/ScopeGuard.h>

#include <initializer_list>
#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/Task.h"
#include "exec/operator/FilterBitsNode.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/trace/provider.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentSealed.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

namespace milvus {
namespace exec {
namespace {

TargetBitmap
MakeBitmap(std::initializer_list<bool> bits) {
    TargetBitmap bitmap(bits.size(), false);
    size_t i = 0;
    for (const auto bit : bits) {
        bitmap.set(i++, bit);
    }
    return bitmap;
}

std::vector<bool>
ToVector(const TargetBitmap& bitmap) {
    std::vector<bool> bits;
    bits.reserve(bitmap.size());
    for (size_t i = 0; i < bitmap.size(); ++i) {
        bits.push_back(bitmap[i]);
    }
    return bits;
}

std::vector<bool>
ToVector(TargetBitmapView bitmap) {
    std::vector<bool> bits;
    bits.reserve(bitmap.size());
    for (size_t i = 0; i < bitmap.size(); ++i) {
        bits.push_back(bitmap[i]);
    }
    return bits;
}

TEST(FilterBitsNodeTest, PredicateConversionUsesFastPathForAllValidResults) {
    auto data = MakeBitmap({true, false, true, false});
    TargetBitmap valid(data.size(), true);

    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        TargetBitmapView(data), TargetBitmapView(valid), data.size());

    EXPECT_TRUE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data), (std::vector<bool>{false, true, false, true}));
    EXPECT_TRUE(valid.all());
}

TEST(FilterBitsNodeTest, PredicateConversionUsesLiveAllValidBitmap) {
    auto data = MakeBitmap({true, false, true, false});
    TargetBitmap valid(data.size(), true);
    auto col_vec =
        std::make_shared<ColumnVector>(std::move(data), std::move(valid));

    TargetBitmapView data_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        data_view, valid_view, col_vec->size());

    EXPECT_TRUE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data_view),
              (std::vector<bool>{false, true, false, true}));
    EXPECT_TRUE(valid_view.all());
}

TEST(FilterBitsNodeTest, PredicateConversionUsesLiveInvalidBitmap) {
    auto data = MakeBitmap({true, false, true, false});
    auto valid = MakeBitmap({true, true, false, false});
    auto col_vec =
        std::make_shared<ColumnVector>(std::move(data), std::move(valid));

    TargetBitmapView data_view(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid_view(col_vec->GetValidRawData(), col_vec->size());
    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        data_view, valid_view, col_vec->size());

    EXPECT_FALSE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data_view),
              (std::vector<bool>{false, true, true, true}));
    EXPECT_TRUE(valid_view.all());
}

TEST(FilterBitsNodeTest, PredicateConversionFiltersOutInvalidResults) {
    auto data = MakeBitmap({true, false, true, false});
    auto valid = MakeBitmap({true, true, false, false});

    const bool used_all_valid_fast_path = ConvertPredicateToFilteredBitset(
        TargetBitmapView(data), TargetBitmapView(valid), data.size());

    EXPECT_FALSE(used_all_valid_fast_path);
    EXPECT_EQ(ToVector(data), (std::vector<bool>{false, true, true, true}));
    EXPECT_TRUE(valid.all());
}

TEST(FilterBitsNodeTest, PredicateConversionPreservesUnalignedViewBoundaries) {
    for (const size_t size : {0, 1, 7, 8, 63, 64, 65, 127, 128, 129, 4097}) {
        for (const size_t offset : {0, 1, 7, 63}) {
            TargetBitmap data(size + offset + 70, true);
            TargetBitmap valid(size + offset + 70, false);
            TargetBitmapView data_view(data.data(), offset, size);
            TargetBitmapView valid_view(valid.data(), offset + 3, size);
            for (size_t i = 0; i < size; ++i) {
                data_view[i] = i % 2 == 0;
                valid_view[i] = i % 3 != 0;
            }
            EXPECT_EQ(
                ConvertPredicateToFilteredBitset(data_view, valid_view, size),
                size == 0);
            for (size_t i = 0; i < size; ++i) {
                EXPECT_EQ(data_view[i], !(i % 2 == 0 && i % 3 != 0));
                EXPECT_TRUE(valid_view[i]);
            }
            for (size_t i = 0; i < data.size(); ++i) {
                if (i < offset || i >= offset + size) {
                    EXPECT_TRUE(data[i]);
                }
                if (i < offset + 3 || i >= offset + 3 + size) {
                    EXPECT_FALSE(valid[i]);
                }
            }
        }
    }
}

void
PartialTracePredicate(const RowVector&, VectorPtr& result) {
    result = std::make_shared<ColumnVector>(
        MakeBitmap({true, false, true, true, false, true, false, true}),
        MakeBitmap({true, true, false, true, true, true, true, false}));
}

void
AllTracePredicate(const RowVector& args, VectorPtr& result) {
    const auto rows = args.childrens().empty() ? 8 : args.size();
    result = std::make_shared<ColumnVector>(TargetBitmap(rows, true),
                                            TargetBitmap(rows, true));
}

void
NoneTracePredicate(const RowVector&, VectorPtr& result) {
    result = std::make_shared<ColumnVector>(TargetBitmap(8, false),
                                            TargetBitmap(8, true));
}

class FilterBitsTraceTest : public testing::Test {
 protected:
    void
    SetUp() override {
        previous_provider_ =
            opentelemetry::trace::Provider::GetTracerProvider();
        previous_root_ = tracer::GetRootSpan();
        previous_enabled_ = tracer::IsTraceEnabled();
        previous_cache_enabled_ = ExprResCacheManager::IsEnabled();
        tracer::SetRootSpan(nullptr);
        tracer::TraceConfig config{};
        config.exporter = "stdout";
        config.sampleFraction = 1.0;
        tracer::initTelemetry(config);
        ExprResCacheManager::SetEnabled(false);
        ExprResCacheManager::Instance().Clear();

        auto schema = std::make_shared<Schema>();
        schema->set_primary_field_id(
            schema->AddDebugField("pk", DataType::INT64));
        const auto data = DataGen(schema, 8);
        segment_ = CreateSealedWithFieldDataLoaded(schema, data);
    }

    void
    TearDown() override {
        ExprResCacheManager::Instance().Clear();
        ExprResCacheManager::SetEnabled(previous_cache_enabled_);
        tracer::TraceConfig config{};
        config.exporter = previous_enabled_ ? "stdout" : "noop";
        tracer::initTelemetry(config);
        opentelemetry::trace::Provider::SetTracerProvider(previous_provider_);
        tracer::SetRootSpan(previous_root_);
    }

    std::string
    Run(expression::FilterFunctionPtr function,
        const std::vector<bool>& expected,
        bool batched = false) {
        std::vector<expr::TypedExprPtr> inputs;
        if (batched) {
            proto::plan::GenericValue value;
            value.set_bool_val(true);
            inputs.push_back(std::make_shared<expr::ValueExpr>(value));
        }
        auto predicate = std::make_shared<expr::CallExpr>(
            "filter_trace_test", inputs, function);
        auto logical = std::make_shared<plan::FilterBitsNode>(
            "filter", predicate, std::vector<plan::PlanNodePtr>{});
        auto config = std::make_shared<QueryConfig>(
            std::unordered_map<std::string, std::string>{
                {QueryConfig::kExprEvalBatchSize, "3"}});
        auto context = std::make_shared<QueryContext>("filter-trace",
                                                      segment_.get(),
                                                      8,
                                                      MAX_TIMESTAMP,
                                                      0,
                                                      0,
                                                      query::PlanOptions{},
                                                      config);
        context->set_enable_expr_cache(true);
        auto task = Task::Create(
            "filter-trace", plan::PlanFragment(logical), 0, context);
        DriverContext driver(task, 0, 0, 0, 0);
        PhyFilterBitsNode node(0, &driver, logical);

        testing::internal::CaptureStdout();
        auto restore_stdout =
            folly::makeGuard([] { testing::internal::GetCapturedStdout(); });
        auto output = node.GetOutput();
        EXPECT_NE(output, nullptr);
        if (output != nullptr) {
            auto column =
                std::static_pointer_cast<ColumnVector>(output->child(0));
            EXPECT_EQ(ToVector(TargetBitmapView(column->GetRawData(),
                                                column->size())),
                      expected);
        }
        EXPECT_EQ(node.GetOutput(), nullptr);
        auto provider = opentelemetry::trace::Provider::GetTracerProvider();
        if (auto* sdk =
                dynamic_cast<opentelemetry::sdk::trace::TracerProvider*>(
                    provider.get())) {
            EXPECT_TRUE(sdk->ForceFlush());
        }
        auto text = testing::internal::GetCapturedStdout();
        restore_stdout.dismiss();
        text.erase(
            std::remove_if(text.begin(),
                           text.end(),
                           [](unsigned char c) { return std::isspace(c); }),
            text.end());
        return text;
    }

    segcore::SegmentSealedUPtr segment_;
    opentelemetry::nostd::shared_ptr<opentelemetry::trace::TracerProvider>
        previous_provider_;
    std::shared_ptr<tracer::trace::Span> previous_root_;
    bool previous_enabled_;
    bool previous_cache_enabled_;
};

TEST_F(FilterBitsTraceTest, ReportsPredicateCountsIncludingNullExclusion) {
    const auto trace = Run(PartialTracePredicate,
                           {false, true, true, false, true, false, true, true});
    EXPECT_NE(trace.find("PhyFilterBitsNode::Execute"), std::string::npos);
    EXPECT_NE(
        trace.find("segment_id:" + std::to_string(segment_->get_segment_id())),
        std::string::npos);
    EXPECT_NE(trace.find("input_rows:8"), std::string::npos);
    EXPECT_NE(trace.find("passed_rows:3"), std::string::npos);
    EXPECT_TRUE(trace.find("cache_hit:false") != std::string::npos ||
                trace.find("cache_hit:0") != std::string::npos);
    EXPECT_NE(trace.find("prefetch_wait_us:"), std::string::npos);
    EXPECT_NE(trace.find("filter_eval_us:"), std::string::npos);
}

TEST_F(FilterBitsTraceTest, ReportsAllAndNonePassing) {
    EXPECT_NE(Run(AllTracePredicate, std::vector<bool>(8, false))
                  .find("passed_rows:8"),
              std::string::npos);
    EXPECT_NE(Run(NoneTracePredicate, std::vector<bool>(8, true))
                  .find("passed_rows:0"),
              std::string::npos);
}

TEST_F(FilterBitsTraceTest, ReportsBatchedEvaluation) {
    EXPECT_NE(Run(AllTracePredicate, std::vector<bool>(8, false), true)
                  .find("passed_rows:8"),
              std::string::npos);
}

TEST_F(FilterBitsTraceTest, IncludesWholeFilterCacheHit) {
    CacheConfig config;
    config.mode = CacheMode::Memory;
    config.admission_threshold = 1;
    config.compression_enabled = false;
    ASSERT_TRUE(ExprResCacheManager::Instance().SetConfig(config));
    ExprResCacheManager::SetEnabled(true);
    const std::vector<bool> expected{
        false, true, true, false, true, false, true, true};
    Run(PartialTracePredicate, expected);
    const auto trace = Run(PartialTracePredicate, expected);
    EXPECT_TRUE(trace.find("cache_hit:true") != std::string::npos ||
                trace.find("cache_hit:1") != std::string::npos);
    EXPECT_NE(trace.find("passed_rows:3"), std::string::npos);
    EXPECT_NE(trace.find("prefetch_wait_us:0"), std::string::npos);
    EXPECT_NE(trace.find("filter_eval_us:0"), std::string::npos);
}

TEST_F(FilterBitsTraceTest, DisabledTracingPreservesResultsWithoutSpans) {
    tracer::initTelemetry(tracer::TraceConfig{});
    EXPECT_FALSE(tracer::IsTraceEnabled());
    const auto trace = Run(PartialTracePredicate,
                           {false, true, true, false, true, false, true, true});
    EXPECT_EQ(trace.find("PhyFilterBitsNode::Execute"), std::string::npos);
}

}  // namespace
}  // namespace exec
}  // namespace milvus
