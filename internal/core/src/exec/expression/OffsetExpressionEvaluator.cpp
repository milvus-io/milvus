// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include "exec/expression/OffsetExpressionEvaluator.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <random>
#include <utility>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"

namespace milvus::exec {
namespace {

uint64_t
LaneMask(uint32_t count) {
    return count == 64
               ? ~uint64_t{0}
               : (count == 0 ? uint64_t{0} : (uint64_t{1} << count) - 1);
}

// TargetBitmap stores bits least-significant-first in uint8_t elements. Read
// only the allocated bytes (including a possible partial final byte), without
// alignment or host-endianness assumptions. This is result transport, not a
// second implementation of any predicate.
uint64_t
ReadPackedLanes(const void* data, uint32_t count) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    uint64_t mask = 0;
    for (uint32_t i = 0; i < (count + 7) / 8; ++i) {
        mask |= uint64_t{bytes[i]} << (i * 8);
    }
    return mask & LaneMask(count);
}

std::vector<expr::TypedExprPtr>
CheckedRoot(expr::TypedExprPtr expression, ExecContext* exec_context) {
    AssertInfo(expression != nullptr,
               "offset expression evaluator requires a nonnull root");
    AssertInfo(exec_context != nullptr,
               "offset expression evaluator requires an ExecContext");
    return {std::move(expression)};
}
}  // namespace

OffsetExpressionWorkspace::OffsetExpressionWorkspace(
    expr::TypedExprPtr expression,
    ExecContext* exec_context,
    bool null_rejecting)
    : exec_context_(exec_context),
      null_rejecting_(null_rejecting),
      expr_set_(CheckedRoot(std::move(expression), exec_context),
                exec_context,
                null_rejecting) {
    compact_offsets_.reserve(64);
    compact_to_lane_.reserve(64);
}

bool
OffsetExpressionWorkspace::SupportsOffsetInput() const {
    return std::all_of(expr_set_.exprs().begin(),
                       expr_set_.exprs().end(),
                       [](const auto& expression) {
                           return expression != nullptr &&
                                  expression->SupportOffsetInput();
                       });
}

std::shared_ptr<ColumnVector>
OffsetExpressionWorkspace::EvalOffsets(OffsetVector& row_ids) {
    EvalCtx eval_context(exec_context_, &row_ids);
    results_.clear();
    expr_set_.Eval(0, 1, true, eval_context, results_);
    AssertInfo(results_.size() == 1 && results_[0] != nullptr,
               "offset expression must produce exactly one result");
    auto output = std::dynamic_pointer_cast<ColumnVector>(results_[0]);
    AssertInfo(output != nullptr && output->IsBitmap(),
               "offset expression result must be a bitmap ColumnVector");
    AssertInfo(output->size() == row_ids.size(),
               "offset expression result size {} does not match input {}",
               output->size(),
               row_ids.size());
    return output;
}

OffsetExpressionTruth
OffsetExpressionWorkspace::EvalTruthBatch(const int32_t* row_ids,
                                          uint32_t count,
                                          uint64_t active_mask) {
    AssertInfo(!null_rejecting_, "exact truth requires null_rejecting=false");
    return EvalBatchImpl(row_ids, count, active_mask);
}

uint64_t
OffsetExpressionWorkspace::EvalAcceptedBatch(const int32_t* row_ids,
                                             uint32_t count,
                                             uint64_t active_mask) {
    return EvalBatchImpl(row_ids, count, active_mask).accepted_mask();
}

OffsetExpressionTruth
OffsetExpressionWorkspace::EvalBatchImpl(const int32_t* row_ids,
                                         uint32_t count,
                                         uint64_t active_mask) {
    AssertInfo(
        count <= 64, "offset expression batch size {} exceeds 64", count);
    const auto lane_mask = LaneMask(count);
    AssertInfo((active_mask & ~lane_mask) == 0,
               "offset expression active mask contains lanes outside batch");
    AssertInfo(count == 0 || row_ids != nullptr,
               "offset expression row IDs cannot be null");

    OffsetExpressionTruth truth;
    if (active_mask == 0) {
        return truth;
    }

    compact_offsets_.clear();
    compact_to_lane_.clear();
    const bool all_active = active_mask == lane_mask;
    if (all_active) {
        // EvalCtx borrows a mutable vector: copy IDs, but no lane map is needed.
        compact_offsets_.assign(row_ids, row_ids + count);
    } else {
        for (uint32_t lane = 0; lane < count; ++lane) {
            if ((active_mask & (uint64_t{1} << lane)) != 0) {
                compact_offsets_.push_back(row_ids[lane]);
                compact_to_lane_.push_back(lane);
            }
        }
    }

    auto output = EvalOffsets(compact_offsets_);

    if (all_active) {
        // Candidate order already matches expression-result order. Keep the
        // packed representation instead of unpacking and repacking each bit.
        truth.known_mask = ReadPackedLanes(output->GetValidRawData(), count);
        truth.true_mask =
            ReadPackedLanes(output->GetRawData(), count) & truth.known_mask;
        return truth;
    }

    TargetBitmapView data(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());
    for (size_t compact_lane = 0; compact_lane < compact_offsets_.size();
         ++compact_lane) {
        const auto original_lane = all_active ? compact_lane : compact_to_lane_[compact_lane];
        if (valid[compact_lane]) {
            truth.known_mask |= uint64_t{1} << original_lane;
            if (data[compact_lane]) {
                truth.true_mask |= uint64_t{1} << original_lane;
            }
        }
    }
    return truth;
}

PreparedOffsetExpressionEvaluator::PreparedOffsetExpressionEvaluator(
    expr::TypedExprPtr expression,
    ExecContext* exec_context,
    bool null_rejecting)
    : expression_(std::move(expression)),
      exec_context_(exec_context),
      null_rejecting_(null_rejecting) {
    AssertInfo(expression_ != nullptr,
               "offset expression evaluator requires a nonnull root");
    AssertInfo(exec_context_ != nullptr,
               "offset expression evaluator requires an ExecContext");
}

std::unique_ptr<OffsetExpressionWorkspace>
PreparedOffsetExpressionEvaluator::CreateWorkspace() const {
    return std::make_unique<OffsetExpressionWorkspace>(
        expression_, exec_context_, null_rejecting_);
}

std::optional<double>
SampleOffsetFilterRatio(const expr::TypedExprPtr& expression,
                        ExecContext* exec_context) {
    const auto started = std::chrono::steady_clock::now();
    auto* query = exec_context->get_query_context();
    const auto* op = query->get_op_context();
    const auto cold_before =
        op ? op->storage_usage.scanned_cold_bytes.load() : 0;
    const auto total_before =
        op ? op->storage_usage.scanned_total_bytes.load() : 0;
    const auto params = query->get_search_info().search_params_;
    // A whole-query debug ratio must never replace this expression's sample.
    const int requested = params.value("ann_fusing_sample_rows", 20);
    AssertInfo(requested == 10 || requested == 20,
               "ann_fusing_sample_rows must be 10 or 20");
    const auto* segment = query->get_segment();
    const auto active = query->get_active_count();
    if (active <= 0 || active > std::numeric_limits<int32_t>::max()) {
        return std::nullopt;
    }
    // Select offsets, never generate data values. Sampling uses the server's
    // scalar chunk boundaries, not client parquet row groups.
    std::mt19937_64 random(static_cast<uint64_t>(segment->get_segment_id()) ^
                           query->get_query_timestamp());
    PreparedOffsetExpressionEvaluator prepared(expression, exec_context, true);
    auto workspace = prepared.CreateWorkspace();
    if (!workspace->SupportsOffsetInput()) {
        return std::nullopt;
    }
    // Check the actual sampling workspace as well as the caller's preflight.
    // A single scalar index covering a segment is NOT a physical chunk.
    auto column = workspace->expr_set().exprs().front()->OffsetSamplingColumn();
    if (!column || column->num_chunks() <= 0 || column->NumRows() < active) {
        LOG_DEBUG(
            "ann_fusing sample unavailable: single-cell locality unknown");
        return std::nullopt;
    }
    const auto last_chunk = column->GetChunkIDByOffset(active - 1).first;
    const auto chunk =
        std::uniform_int_distribution<int64_t>(0, last_chunk)(random);
    const auto first = column->GetNumRowsUntilChunk(chunk);
    const auto rows =
        std::min<int64_t>(column->chunk_row_nums(chunk), active - first);
    if (first < 0 || rows <= 0) {
        return std::nullopt;
    }
    const auto count = std::min<int64_t>(requested, rows);
    OffsetVector offsets;
    offsets.reserve(count);
    std::uniform_int_distribution<int64_t> choose(0, rows - 1);
    while (offsets.size() < count) {
        const auto offset = first + choose(random);
        if (std::find(offsets.begin(), offsets.end(), offset) ==
            offsets.end()) {
            offsets.push_back(offset);
        }
    }
    const auto prepared_at = std::chrono::steady_clock::now();
    auto result = workspace->EvalOffsets(offsets);
    const auto evaluated_at = std::chrono::steady_clock::now();
    TargetBitmapView truth(result->GetRawData(), result->size());
    TargetBitmapView valid(result->GetValidRawData(), result->size());
    size_t accepted = 0;
    for (size_t i = 0; i < offsets.size(); ++i) {
        accepted += truth[i] && valid[i];
    }
    const double ratio = 1.0 - static_cast<double>(accepted) / count;
    LOG_DEBUG(
        "ann_fusing sample source=single_cell storage={} chunk={} rows={} "
        "chunk_first={} chunk_rows={} requested_rows={} filter_ratio={} "
        "prepare_us={} eval_us={} storage_cold_bytes={} storage_total_bytes={}",
        column->OffsetSamplingStorageIdentity(),
        chunk,
        count,
        first,
        rows,
        requested,
        ratio,
        std::chrono::duration<double, std::micro>(prepared_at - started)
            .count(),
        std::chrono::duration<double, std::micro>(evaluated_at - prepared_at)
            .count(),
        op ? op->storage_usage.scanned_cold_bytes.load() - cold_before : 0,
        op ? op->storage_usage.scanned_total_bytes.load() - total_before : 0);
    // Storage counters are cache-layer accounting, NOT physical disk bytes or
    // OS-page residency. Concurrent work sharing OpContext can contribute;
    // diagnostic experiments must isolate requests and measure OS IO separately.
    return ratio;
}

}  // namespace milvus::exec
