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

#include "segcore/boost_score_c.h"

#include <arrow/array/array_primitive.h>
#include <arrow/c/bridge.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>

#include "common/Common.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "futures/Executor.h"
#include "futures/Future.h"
#include "pb/plan.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanProto.h"
#include "rescores/BoostScoreRunner.h"
#include "segcore/SegmentInterface.h"

namespace {

using OffsetChunks = std::vector<milvus::FixedVector<int32_t>>;

milvus::FixedVector<int32_t>
BuildScorerOffsets(const std::shared_ptr<arrow::Array>& offsets) {
    AssertInfo(offsets != nullptr, "offset array is null");
    AssertInfo(offsets->type_id() == arrow::Type::INT64,
               "offset array must be Int64, got {}",
               offsets->type()->ToString());
    AssertInfo(offsets->null_count() == 0, "offset array contains null");

    auto int64_offsets = std::static_pointer_cast<arrow::Int64Array>(offsets);
    auto count = int64_offsets->length();
    milvus::FixedVector<int32_t> scorer_offsets;
    scorer_offsets.reserve(count);
    for (auto i = 0; i < count; ++i) {
        auto offset = int64_offsets->Value(i);
        AssertInfo(
            offset >= 0, "offset must be non-negative, offset: {}", offset);
        AssertInfo(offset <= std::numeric_limits<int32_t>::max(),
                   "offset exceeds int32 range, offset: {}",
                   offset);
        scorer_offsets.push_back(static_cast<int32_t>(offset));
    }
    return scorer_offsets;
}

void
ValidateScorerScoreInputs(CSegmentInterface c_segment,
                          CSearchPlan c_plan,
                          const void* serialized_score_function,
                          int64_t serialized_score_function_size,
                          ArrowArray* offset_chunks,
                          ArrowSchema* offset_schemas,
                          int64_t num_chunks,
                          float* const* output_score_chunks,
                          bool* const* output_has_score_chunks) {
    AssertInfo(c_segment != nullptr, "segment is null");
    AssertInfo(c_plan != nullptr, "search plan is null");
    AssertInfo(serialized_score_function != nullptr,
               "serialized score function is null");
    AssertInfo(serialized_score_function_size > 0,
               "serialized score function is empty");
    AssertInfo(num_chunks >= 0, "chunk count must be non-negative");
    if (num_chunks > 0) {
        AssertInfo(offset_chunks != nullptr, "offset chunks is null");
        AssertInfo(offset_schemas != nullptr, "offset schemas is null");
        AssertInfo(output_score_chunks != nullptr,
                   "output score chunks is null");
        AssertInfo(output_has_score_chunks != nullptr,
                   "output has score chunks is null");
    }
}

std::shared_ptr<milvus::rescores::Scorer>
ParseScorer(milvus::query::Plan* plan,
            const void* serialized_score_function,
            int64_t serialized_score_function_size) {
    milvus::proto::plan::ScoreFunction score_function;
    auto ok = score_function.ParseFromArray(serialized_score_function,
                                            serialized_score_function_size);
    AssertInfo(ok, "failed to parse score function");

    milvus::query::ProtoParser parser(plan->schema_);
    return parser.ParseScorer(score_function);
}

OffsetChunks
BuildScorerOffsetChunks(ArrowArray* offset_chunks,
                        ArrowSchema* offset_schemas,
                        int64_t num_chunks) {
    OffsetChunks scorer_offset_chunks(num_chunks);
    for (auto chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
        auto offset_array_result = arrow::ImportArray(
            &offset_chunks[chunk_idx], &offset_schemas[chunk_idx]);
        AssertInfo(offset_array_result.ok(),
                   "failed to import offset chunk {}: {}",
                   chunk_idx,
                   offset_array_result.status().ToString());
        auto offset_array = offset_array_result.ValueOrDie();
        if (offset_array->length() == 0) {
            continue;
        }
        scorer_offset_chunks[chunk_idx] = BuildScorerOffsets(offset_array);
    }
    return scorer_offset_chunks;
}

void
ComputeScorerScoresOnPreparedChunks(
    milvus::segcore::SegmentInternalInterface* segment,
    milvus::query::Plan* plan,
    const std::shared_ptr<milvus::rescores::Scorer>& scorer,
    OffsetChunks& scorer_offset_chunks,
    uint64_t timestamp,
    uint64_t collection_ttl,
    int32_t consistency_level,
    uint64_t entity_ttl_physical_time_us,
    float* const* output_score_chunks,
    bool* const* output_has_score_chunks,
    const folly::CancellationToken& cancel_token = folly::CancellationToken()) {
    // Observe a pre-cancelled request before doing anything at all --
    // including the all-empty fast path below, which the per-chunk loop's
    // cancellation check used to cover before the fast path existed.
    milvus::futures::throwIfCancelled(cancel_token);

    // Nothing to score: skip the whole setup. Bailing out here matters for a
    // non-native filter (text match, GIS) -- otherwise it would scan the whole
    // segment to build a bitset no chunk ever consumes.
    auto has_offsets = std::any_of(
        scorer_offset_chunks.begin(),
        scorer_offset_chunks.end(),
        [](const auto& scorer_offsets) { return !scorer_offsets.empty(); });
    if (!has_offsets) {
        return;
    }

    // Preflight before the potentially expensive whole-segment filter
    // evaluation below (ComputeNonNativeFilterBitset): catch a null
    // per-chunk output pointer before doing any work rather than midway
    // through the chunk loop.
    for (auto chunk_idx = 0; chunk_idx < scorer_offset_chunks.size();
         ++chunk_idx) {
        if (scorer_offset_chunks[chunk_idx].empty()) {
            continue;
        }
        AssertInfo(output_score_chunks[chunk_idx] != nullptr,
                   "output score chunk {} is null",
                   chunk_idx);
        AssertInfo(output_has_score_chunks[chunk_idx] != nullptr,
                   "output has score chunk {} is null",
                   chunk_idx);
    }

    auto active_count = segment->get_active_count(timestamp);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID,
        segment,
        active_count,
        timestamp,
        collection_ttl,
        consistency_level,
        plan->plan_node_->plan_options_,
        std::make_shared<milvus::exec::QueryConfig>(),
        nullptr,
        std::unordered_map<std::string,
                           std::shared_ptr<milvus::exec::BaseConfig>>(),
        entity_ttl_physical_time_us);
    query_context->set_search_info(plan->plan_node_->search_info_);

    auto op_context = milvus::OpContext(cancel_token);
    query_context->set_op_context(&op_context);
    auto exec_context = milvus::exec::ExecContext(query_context.get());

    // A filter that cannot consume offset input (text match, GIS) is
    // evaluated over the whole segment; do that once here instead of once
    // per offset chunk inside ComputeScorerScores.
    // The compiled filter is reused across chunks too: deciding native vs
    // non-native already builds it, and a native filter would otherwise be
    // recompiled (and its scalar indexes re-pinned) once per chunk.
    std::unique_ptr<milvus::exec::ExprSet> filter_expr_set;
    auto filter_bitset = milvus::rescores::ComputeNonNativeFilterBitset(
        &exec_context, scorer, &filter_expr_set);

    for (auto chunk_idx = 0; chunk_idx < scorer_offset_chunks.size();
         ++chunk_idx) {
        milvus::futures::throwIfCancelled(cancel_token);
        auto& scorer_offsets = scorer_offset_chunks[chunk_idx];
        if (scorer_offsets.empty()) {
            continue;
        }
        // Output pointers were validated by the preflight above.
        milvus::rescores::ComputeScorerScores(
            &exec_context,
            &op_context,
            segment,
            scorer,
            scorer_offsets,
            output_score_chunks[chunk_idx],
            output_has_score_chunks[chunk_idx],
            filter_bitset.has_value() ? &filter_bitset.value() : nullptr,
            filter_expr_set.get());
    }
}

}  // namespace

CStatus
ComputeScorerScoresOnOffsetChunks(CSegmentInterface c_segment,
                                  CSearchPlan c_plan,
                                  const void* serialized_score_function,
                                  int64_t serialized_score_function_size,
                                  ArrowArray* offset_chunks,
                                  ArrowSchema* offset_schemas,
                                  int64_t num_chunks,
                                  uint64_t timestamp,
                                  uint64_t collection_ttl,
                                  int32_t consistency_level,
                                  uint64_t entity_ttl_physical_time_us,
                                  float* const* output_score_chunks,
                                  bool* const* output_has_score_chunks) {
    try {
        ValidateScorerScoreInputs(c_segment,
                                  c_plan,
                                  serialized_score_function,
                                  serialized_score_function_size,
                                  offset_chunks,
                                  offset_schemas,
                                  num_chunks,
                                  output_score_chunks,
                                  output_has_score_chunks);

        auto segment =
            static_cast<milvus::segcore::SegmentInternalInterface*>(c_segment);
        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        auto scorer = ParseScorer(
            plan, serialized_score_function, serialized_score_function_size);
        auto scorer_offset_chunks =
            BuildScorerOffsetChunks(offset_chunks, offset_schemas, num_chunks);
        ComputeScorerScoresOnPreparedChunks(segment,
                                            plan,
                                            scorer,
                                            scorer_offset_chunks,
                                            timestamp,
                                            collection_ttl,
                                            consistency_level,
                                            entity_ttl_physical_time_us,
                                            output_score_chunks,
                                            output_has_score_chunks);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CFuture*
AsyncComputeScorerScoresOnOffsetChunks(CSegmentInterface c_segment,
                                       CSearchPlan c_plan,
                                       const void* serialized_score_function,
                                       int64_t serialized_score_function_size,
                                       ArrowArray* offset_chunks,
                                       ArrowSchema* offset_schemas,
                                       int64_t num_chunks,
                                       uint64_t timestamp,
                                       uint64_t collection_ttl,
                                       int32_t consistency_level,
                                       uint64_t entity_ttl_physical_time_us,
                                       float* const* output_score_chunks,
                                       bool* const* output_has_score_chunks) {
    try {
        ValidateScorerScoreInputs(c_segment,
                                  c_plan,
                                  serialized_score_function,
                                  serialized_score_function_size,
                                  offset_chunks,
                                  offset_schemas,
                                  num_chunks,
                                  output_score_chunks,
                                  output_has_score_chunks);

        auto segment =
            static_cast<milvus::segcore::SegmentInternalInterface*>(c_segment);
        auto plan = static_cast<milvus::query::Plan*>(c_plan);
        auto scorer = ParseScorer(
            plan, serialized_score_function, serialized_score_function_size);
        auto scorer_offset_chunks = std::make_shared<OffsetChunks>(
            BuildScorerOffsetChunks(offset_chunks, offset_schemas, num_chunks));
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getSearchCPUExecutor(),
            milvus::futures::ExecutePriority::HIGH,
            [segment,
             plan,
             scorer = std::move(scorer),
             scorer_offset_chunks = std::move(scorer_offset_chunks),
             timestamp,
             collection_ttl,
             consistency_level,
             entity_ttl_physical_time_us,
             output_score_chunks,
             output_has_score_chunks](
                folly::CancellationToken cancel_token) -> bool* {
                ComputeScorerScoresOnPreparedChunks(segment,
                                                    plan,
                                                    scorer,
                                                    *scorer_offset_chunks,
                                                    timestamp,
                                                    collection_ttl,
                                                    consistency_level,
                                                    entity_ttl_physical_time_us,
                                                    output_score_chunks,
                                                    output_has_score_chunks,
                                                    cancel_token);
                return nullptr;
            });

        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    } catch (std::exception& e) {
        std::string error_msg = e.what();
        auto future = milvus::futures::Future<bool>::async(
            milvus::futures::getSearchCPUExecutor(),
            milvus::futures::ExecutePriority::HIGH,
            [error_msg = std::move(error_msg)](
                folly::CancellationToken cancel_token) -> bool* {
                (void)cancel_token;
                ThrowInfo(milvus::UnexpectedError,
                          "AsyncComputeScorerScoresOnOffsetChunks preflight "
                          "failed: {}",
                          error_msg);
                return nullptr;
            });
        return static_cast<CFuture*>(static_cast<void*>(
            static_cast<milvus::futures::IFuture*>(future.release())));
    }
}
