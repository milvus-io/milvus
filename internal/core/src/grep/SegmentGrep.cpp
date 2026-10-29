// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "grep/segment_grep_c.h"
#include <arrow/api.h>
#include <algorithm>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include <fmt/format.h>
#include <limits>
#include <nlohmann/json.hpp>
#include "common/CGoCatch.h"
#include "grep/Grep.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/arrow_field_utils.h"

namespace milvus::grep {
using namespace milvus::segcore;
namespace {
void
CheckArrow(const arrow::Status& status) {
    if (!status.ok()) {
        auto error = ArrowExportFailure(status);
        auto cleanup =
            folly::makeGuard([&] { free(const_cast<char*>(error.error_msg)); });
        throw SegcoreError(static_cast<ErrorCode>(error.error_code),
                           error.error_msg);
    }
}
void
CheckResultBudget(uint64_t total_bytes, uint64_t limit) {
    if (total_bytes > limit) {
        ThrowInfo(InvalidParameter,
                  fmt::format("grep result budget exceeded "
                              "(quotaAndLimits.limits.maxOutputSize={} bytes): "
                              "actual={} bytes",
                              limit,
                              total_bytes));
    }
}

// Builds the Arrow output and counts its bytes across all input batches.
class GrepResultBuilder {
 public:
    int64_t
    Count() const {
        return positions_.length();
    }

    uint64_t
    TotalBytes() const {
        return total_bytes_;
    }

    void
    Append(const Result& result,
           const std::vector<int64_t>& input_positions,
           uint64_t max_result_bytes) {
        for (size_t i = 0; i < input_positions.size(); ++i) {
            if (!result.Selected(i))
                continue;
            std::string document = "[";
            for (size_t j = 0; j < result.LineCount(i); ++j) {
                std::vector<int64_t> starts, ends;
                const auto count = result.SpanCount(i, j);
                starts.reserve(count);
                ends.reserve(count);
                for (size_t k = 0; k < count; ++k) {
                    auto hit = result.GetSpan(i, j, k);
                    AssertInfo(
                        hit.start <= static_cast<uint64_t>(
                                         std::numeric_limits<int64_t>::max()) &&
                            hit.end <= static_cast<uint64_t>(
                                           std::numeric_limits<int64_t>::max()),
                        "grep span boundary exceeds Int64");
                    starts.push_back(static_cast<int64_t>(hit.start));
                    ends.push_back(static_cast<int64_t>(hit.end));
                }
                auto line = nlohmann::json{
                    {"content", std::string(result.Content(i, j))},
                    {"starts", std::move(starts)},
                    {"ends",
                     std::move(
                         ends)}}.dump();
                // Charge actual JSON bytes, including escaping/punctuation,
                // plus position, Binary offset and validity allowance.
                CheckResultBudget(total_bytes_ + 16 + document.size() +
                                      (j != 0) + line.size() + 1,
                                  max_result_bytes);
                if (j != 0)
                    document += ',';
                document += line;
            }
            document += ']';
            CheckResultBudget(total_bytes_ + 16 + document.size(),
                              max_result_bytes);
            CheckArrow(positions_.Append(input_positions[i]));
            CheckArrow(details_.Append(document));
            total_bytes_ += 16 + document.size();
        }
    }

    std::shared_ptr<arrow::RecordBatch>
    Finish() {
        std::shared_ptr<arrow::Array> positions_array, details_array;
        CheckArrow(positions_.Finish(&positions_array));
        CheckArrow(details_.Finish(&details_array));
        return arrow::RecordBatch::Make(
            arrow::schema(
                {arrow::field("positions", arrow::int64(), false),
                 arrow::field("details",
                              arrow::binary(),
                              false,
                              arrow::key_value_metadata(
                                  {kMilvusDataTypeMetadataKey},
                                  {std::to_string(static_cast<int32_t>(
                                      DataType::JSON))}))}),
            positions_array->length(),
            {positions_array, details_array});
    }

 private:
    uint64_t total_bytes_ = 0;
    arrow::BinaryBuilder details_;
    arrow::Int64Builder positions_;
};

// Keeps either borrowed chunk views pinned or bulk-read strings owned until
// matching and result copying for this read batch have finished.
struct GrepInputBatch {
    using Views =
        PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>;
    int64_t count = 0;
    std::optional<Views> pinned;
    std::unique_ptr<DataArray> owned;
    std::vector<Views> text_pins;
    std::vector<std::optional<std::string_view>> inline_text;

    std::optional<std::string_view>
    Text(int64_t index) const {
        if (!inline_text.empty()) {
            return inline_text[index];
        }
        if (pinned) {
            const auto& [views, validity] = pinned->get();
            if (!validity.empty() && !validity[index]) {
                return std::nullopt;
            }
            return views[index];
        }
        const auto& validity = GetFieldDataRowValidData(*owned);
        if (!validity.empty() && !validity[index]) {
            return std::nullopt;
        }
        return owned->scalars().string_data().data(index);
    }
};

// Borrow only self-contained inline TEXT. If any non-null value is a LOB
// reference, leave the entire batch to the existing bulk reader so file-group
// batching and prefetch/error behavior stay unchanged. Keep every chunk pinned
// until matching and Arrow result copying have both completed.
bool
TryReadInlineTextBatch(const ChunkedColumnInterface* column,
                       milvus::OpContext* op_ctx,
                       const int64_t* offsets,
                       GrepInputBatch& batch) {
    const auto& boundaries = column->GetNumRowsUntilChunk();
    batch.inline_text.resize(batch.count);
    int64_t begin = 0;
    while (begin < batch.count) {
        const auto end = std::upper_bound(
            boundaries.begin(), boundaries.end(), offsets[begin]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        const int64_t chunk = std::distance(boundaries.begin(), end) - 1;
        const int64_t chunk_begin = *(end - 1), chunk_end = *end;
        auto stop = begin + 1;
        while (stop < batch.count && offsets[stop] >= chunk_begin &&
               offsets[stop] < chunk_end) {
            ++stop;
        }
        FixedVector<int32_t> local_offsets(stop - begin);
        for (auto i = begin; i < stop; ++i) {
            const auto relative = offsets[i] - chunk_begin;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i - begin] = relative;
        }
        auto pinned =
            column->StringViewsByOffsets(op_ctx, chunk, local_offsets);
        const auto& [views, validity] = pinned.get();
        for (auto i = begin; i < stop; ++i) {
            const auto local = i - begin;
            if (!validity.empty() && !validity[local])
                continue;
            const auto encoded = views[local];
            if (encoded.empty()) {
                batch.inline_text[i] = std::string_view{};
                continue;
            }
            const auto* data = reinterpret_cast<const uint8_t*>(encoded.data());
            if (!milvus_storage::lob_column::IsInlineData(data)) {
                batch.inline_text.clear();
                batch.text_pins.clear();
                return false;
            }
            const uint8_t* payload = nullptr;
            size_t length = 0;
            milvus_storage::lob_column::DecodeInlineData(
                data, encoded.size(), &payload, &length);
            batch.inline_text[i] =
                length == 0
                    ? std::string_view{}
                    : std::string_view(reinterpret_cast<const char*>(payload),
                                       length);
        }
        batch.text_pins.push_back(std::move(pinned));
        begin = stop;
    }
    return true;
}

GrepInputBatch
ReadGrepBatch(const SegmentInternalInterface* segment,
              const std::shared_ptr<const SegmentReadSnapshot>& snapshot,
              milvus::OpContext* op_ctx,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t max_rows,
              int64_t rows,
              bool use_views,
              const ChunkedColumnInterface* view_column) {
    const auto field = meta.get_id();
    GrepInputBatch batch;
    AssertInfo(offsets[0] >= 0 && offsets[0] < rows,
               "grep offset out of range");
    if (view_column != nullptr && meta.get_data_type() == DataType::TEXT) {
        batch.count = max_rows;
        // TEXT prefetch spans chunks; do not truncate its read-ahead at the
        // first chunk boundary or at the remaining match limit.
        for (int64_t i = 0; i < batch.count; ++i) {
            AssertInfo(offsets[i] >= 0 && offsets[i] < rows,
                       "grep offset out of range");
        }
        if (!TryReadInlineTextBatch(view_column, op_ctx, offsets, batch)) {
            batch.owned = BulkSubscriptWithSnapshot(
                segment, snapshot, op_ctx, field, meta, offsets, batch.count);
        }
        return batch;
    }
    if (view_column != nullptr) {
        // The request snapshot owns this sealed column under the read lease.
        // Locate only the first row. All subsequent rows in this read batch
        // must lie in the same chunk, even when offsets are sparse or unsorted.
        const auto& boundaries = view_column->GetNumRowsUntilChunk();
        const auto end =
            std::upper_bound(boundaries.begin(), boundaries.end(), offsets[0]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        const int64_t chunk = std::distance(boundaries.begin(), end) - 1;
        const int64_t chunk_begin = *(end - 1);
        const int64_t chunk_end = *end;
        while (batch.count < max_rows) {
            const auto offset = offsets[batch.count];
            // Preserve read-ahead validation: a bad offset within the configured
            // read batch fails even when an earlier row satisfies the limit.
            AssertInfo(offset >= 0 && offset < rows,
                       "grep offset out of range");
            if (offset < chunk_begin || offset >= chunk_end) {
                break;
            }
            ++batch.count;
        }
        FixedVector<int32_t> local_offsets(batch.count);
        for (int64_t i = 0; i < batch.count; ++i) {
            const auto relative = offsets[i] - chunk_begin;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i] = relative;
        }
        // Pin through the same column used for addressing; do not re-capture
        // published state or repeat the field lookup in the segment wrapper.
        batch.pinned.emplace(
            view_column->StringViewsByOffsets(op_ctx, chunk, local_offsets));
        return batch;
    }
    const auto chunk =
        use_views ? segment->get_chunk_by_offset(field, offsets[0]).first : 0;
    auto& count = batch.count;
    count = 1;
    // Read ahead independently of the remaining match quota. TEXT uses
    // the LOB-aware bulk reader, which can group references by file.
    while (count < max_rows) {
        auto off = offsets[count];
        AssertInfo(off >= 0 && off < rows, "grep offset out of range");
        if (use_views &&
            segment->get_chunk_by_offset(field, off).first != chunk)
            break;
        ++count;
    }
    if (use_views) {
        FixedVector<int32_t> local_offsets(count);
        for (int64_t i = 0; i < count; ++i) {
            auto relative =
                segment->get_chunk_by_offset(field, offsets[i]).second;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i] = relative;
        }
        batch.pinned.emplace(segment->chunk_views_by_offsets<std::string_view>(
            op_ctx, field, chunk, local_offsets));
    } else {
        batch.owned = BulkSubscriptWithSnapshot(
            segment, snapshot, op_ctx, field, meta, offsets, count);
    }
    return batch;
}

void
MatchGrepBatch(const GrepInputBatch& batch,
               int64_t begin,
               Session& session,
               const CGrepScanOptions& opt,
               GrepResultBuilder& output) {
    std::vector<std::string_view> texts;
    std::vector<int64_t> input_positions;
    int64_t consumed = 0;
    while (consumed < batch.count && output.Count() < opt.limit) {
        texts.clear();
        input_positions.clear();
        const auto quota = opt.limit - output.Count();
        // Matching groups are bounded by the read batch and remaining quota.
        // Prefetched rows stay owned/pinned until this read batch ends.
        while (consumed < batch.count &&
               static_cast<int64_t>(texts.size()) < quota) {
            auto text = batch.Text(consumed++);
            if (!text) {
                continue;
            }
            texts.push_back(*text);
            input_positions.push_back(begin + consumed - 1);
        }
        if (texts.empty())
            continue;

        const auto remaining = opt.max_result_bytes - output.TotalBytes();
        if (remaining == 0)
            ThrowInfo(
                InvalidParameter,
                fmt::format("grep result budget exceeded "
                            "(quotaAndLimits.limits.maxOutputSize={} bytes): "
                            "remaining=0 bytes",
                            opt.max_result_bytes));
        // The request cancellation token is the only execution deadline.
        auto result = session.ExecuteBatch(texts, remaining);
        output.Append(result, input_positions, opt.max_result_bytes);
        CheckResultBudget(output.TotalBytes(), opt.max_result_bytes);
    }
}

std::shared_ptr<arrow::RecordBatch>
ExecuteGrepScan(const SegmentInternalInterface* segment,
                const int64_t* offsets,
                int64_t size,
                const CGrepScanOptions& opt,
                const std::vector<std::string_view>& patterns,
                const folly::CancellationToken& cancel_token) {
    AssertInfo(size >= 0 && (size == 0 || offsets), "invalid grep offsets");
    AssertInfo(opt.batch_size > 0 && opt.limit > 0 && opt.max_regex_bytes > 0 &&
                   opt.max_dfa_cache_bytes > 0 && opt.max_result_bytes > 0,
               "invalid grep service budgets or limit");
    AssertInfo(opt.pattern_kind <= 1 && opt.case_mode <= 1 && opt.boundary <= 2,
               "invalid normalized grep options");
    auto check = [&] {
        if (cancel_token.isCancellationRequested()) {
            ThrowInfo(FollyCancel, "grep cancelled");
        }
    };
    check();
    // A lease prevents sealed publication/reopen while batch views are read.
    std::shared_ptr<SegmentReadLease> lease;
    if (segment->type() == SegmentType::Sealed) {
        auto sealed = dynamic_cast<const ChunkedSegmentSealedImpl*>(segment);
        AssertInfo(sealed, "grep requires a sealed read lease");
        lease = sealed->AcquireReadLease(cancel_token);
    }
    const auto schema = segment->get_schema_snapshot();
    const FieldId field(opt.field_id);
    const auto& meta = (*schema)[field];
    AssertInfo(meta.get_data_type() == DataType::VARCHAR ||
                   meta.get_data_type() == DataType::TEXT,
               "grep requires a text field");
    auto plan =
        grep::Plan::Compile(patterns,
                            {static_cast<grep::PatternKind>(opt.pattern_kind),
                             static_cast<grep::CaseMode>(opt.case_mode),
                             static_cast<grep::Boundary>(opt.boundary)},
                            {opt.max_regex_bytes, opt.max_dfa_cache_bytes});
    auto session = plan.CreateSession();
    folly::CancellationCallback cancel(
        cancel_token,
        [token = session.GetCancellationToken()] { token.Cancel(); });
    milvus::OpContext op_ctx(cancel_token);
    const auto snapshot = segment->CaptureReadSnapshot();
    const auto rows = segment->get_row_count();
    const bool borrow_inline_text =
        snapshot && meta.get_data_type() == DataType::TEXT &&
        !meta.is_external_field() && segment->HasFieldData(field);
    const auto* view_column =
        snapshot && (meta.get_data_type() == DataType::VARCHAR ||
                     borrow_inline_text)
            ? snapshot->GetColumn(field)
            : nullptr;
    const bool use_views = meta.get_data_type() == DataType::VARCHAR &&
                           segment->HasFieldData(field) &&
                           (!snapshot || view_column != nullptr);

    GrepResultBuilder output;
    for (int64_t begin = 0; begin < size && output.Count() < opt.limit;) {
        check();
        auto batch = ReadGrepBatch(
            segment,
            snapshot,
            &op_ctx,
            meta,
            offsets + begin,
            std::min<int64_t>(opt.batch_size, size - begin),
            rows,
            use_views,
            (use_views || borrow_inline_text) ? view_column : nullptr);
        MatchGrepBatch(batch, begin, session, opt, output);
        begin += batch.count;
    }
    return output.Finish();
}
}  // namespace
}  // namespace milvus::grep

CStatus
ScanGrepByOffsets(void* segment,
                  const int64_t* offsets,
                  int64_t size,
                  const CGrepScanOptions* options,
                  const uint8_t* patterns,
                  uint64_t pattern_bytes,
                  const uint64_t* lengths,
                  uint32_t pattern_count,
                  ArrowSchema* out_schema,
                  ArrowArray* out_array,
                  void* cancellation_source) {
    try {
        AssertInfo(segment && options && out_schema && out_array,
                   "null grep argument");
        AssertInfo(!out_schema->release && !out_array->release,
                   "grep outputs must be empty");
        auto cleanup = folly::makeGuard([&] {
            if (out_array->release)
                out_array->release(out_array);
            if (out_schema->release)
                out_schema->release(out_schema);
        });
        AssertInfo(
            pattern_count > 0 && lengths && (pattern_bytes == 0 || patterns),
            "invalid grep patterns");
        std::vector<std::string_view> views;
        uint64_t consumed = 0;
        for (uint32_t i = 0; i < pattern_count; ++i) {
            AssertInfo(lengths[i] <= pattern_bytes - consumed,
                       "invalid grep pattern lengths");
            views.emplace_back(
                pattern_bytes == 0
                    ? ""
                    : reinterpret_cast<const char*>(patterns + consumed),
                lengths[i]);
            consumed += lengths[i];
        }
        AssertInfo(consumed == pattern_bytes, "grep pattern bytes mismatch");
        auto token =
            cancellation_source
                ? static_cast<folly::CancellationSource*>(cancellation_source)
                      ->getToken()
                : folly::CancellationToken{};
        std::shared_ptr<arrow::RecordBatch> scan_result;
        try {
            scan_result = milvus::grep::ExecuteGrepScan(
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment),
                offsets,
                size,
                *options,
                views,
                token);
        } catch (const milvus::grep::GrepError& error) {
            using namespace milvus;
            using grep::Status;
            std::string reason = error.what();
            if (error.operation() == grep::Operation::Compile &&
                error.status() == Status::EngineBuildError) {
                reason = fmt::format(
                    "grep regex compilation failed "
                    "(function.grep.maxRegexBytes={} bytes, "
                    "function.grep.maxDFACacheBytes={} bytes): {}",
                    options->max_regex_bytes,
                    options->max_dfa_cache_bytes,
                    error.what());
            } else if (error.operation() == grep::Operation::Execute &&
                       error.status() == Status::ResourceLimit) {
                reason = fmt::format(
                    "grep result generation failed "
                    "(quotaAndLimits.limits.maxOutputSize={} bytes): {}",
                    options->max_result_bytes,
                    error.what());
            }
            // Keep the code assigned at construction without truncating the
            // added configuration context through Grep's bounded diagnostic.
            throw SegcoreError(error.get_error_code(), reason);
        }
        auto status =
            arrow::ExportRecordBatch(*scan_result, out_array, out_schema);
        if (!status.ok())
            return milvus::segcore::ArrowExportFailure(status);
        cleanup.dismiss();
        return milvus::SuccessCStatus();
    } catch (const folly::FutureCancellation& error) {
        return milvus::FailureCStatus(milvus::FollyCancel, error.what());
    } catch (const folly::FutureTimeout& error) {
        return milvus::FailureCStatus(milvus::FollyOtherException,
                                      error.what());
    }
    CGO_CATCH_AND_RETURN_CSTATUS
}
