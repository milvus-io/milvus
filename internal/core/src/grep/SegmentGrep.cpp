// Copyright (C) 2026 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#include "grep/segment_grep_c.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include <fmt/format.h>
#include <limits>
#include "common/CGoCatch.h"
#include "grep/Grep.h"
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
// These limits protect a configured operation, not malformed request values.
void
Charge(uint64_t amount, uint64_t& remaining, uint64_t limit) {
    if (amount > remaining) {
        ThrowInfo(OutOfRange,
                  fmt::format("grep result budget exceeded "
                              "(quotaAndLimits.limits.maxOutputSize={} bytes): "
                              "remaining={} bytes, requested={} bytes",
                              limit,
                              remaining,
                              amount));
    }
    remaining -= amount;
}

std::shared_ptr<arrow::RecordBatch>
ScanSegment(const SegmentInternalInterface* segment,
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
    check();
    auto session = plan.CreateSession();
    folly::CancellationCallback cancel(
        cancel_token,
        [token = session.GetCancellationToken()] { token.Cancel(); });
    milvus::OpContext op_ctx(cancel_token);
    const auto snapshot = segment->CaptureReadSnapshot();
    const auto rows = segment->get_row_count();
    const bool use_views = meta.get_data_type() == DataType::VARCHAR &&
                           segment->HasFieldData(field) &&
                           (!snapshot || snapshot->GetColumn(field) != nullptr);

    auto span_type =
        arrow::struct_({arrow::field("start", arrow::uint64(), false),
                        arrow::field("end", arrow::uint64(), false)});
    auto spans_type = arrow::list(arrow::field("item", span_type, false));
    auto line_type =
        arrow::struct_({arrow::field("content", arrow::utf8(), false),
                        arrow::field("spans", spans_type, false)});
    auto details_type = arrow::list(arrow::field("item", line_type, false));
    auto starts = std::make_shared<arrow::UInt64Builder>();
    auto ends = std::make_shared<arrow::UInt64Builder>();
    auto span = std::make_shared<arrow::StructBuilder>(
        span_type,
        arrow::default_memory_pool(),
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{starts, ends});
    auto spans = std::make_shared<arrow::ListBuilder>(
        arrow::default_memory_pool(), span, spans_type);
    auto content = std::make_shared<arrow::StringBuilder>();
    auto lines = std::make_shared<arrow::StructBuilder>(
        line_type,
        arrow::default_memory_pool(),
        std::vector<std::shared_ptr<arrow::ArrayBuilder>>{content, spans});
    arrow::ListBuilder details(
        arrow::default_memory_pool(), lines, details_type);
    arrow::Int64Builder positions;
    uint64_t remaining = opt.max_result_bytes;
    // Includes fixed offsets for empty lists; all later arithmetic is subtractive.
    Charge(16, remaining, opt.max_result_bytes);
    for (int64_t begin = 0; begin < size && positions.length() < opt.limit;) {
        check();
        AssertInfo(offsets[begin] >= 0 && offsets[begin] < rows,
                   "grep offset out of range");
        const auto chunk =
            use_views
                ? segment->get_chunk_by_offset(field, offsets[begin]).first
                : 0;
        const auto max_rows = std::min<int64_t>(opt.batch_size, size - begin);
        int64_t count = 1;
        // Read ahead independently of the remaining match quota. TEXT uses
        // the LOB-aware bulk reader, which can group references by file.
        while (count < max_rows) {
            auto off = offsets[begin + count];
            AssertInfo(off >= 0 && off < rows, "grep offset out of range");
            if (use_views &&
                segment->get_chunk_by_offset(field, off).first != chunk)
                break;
            ++count;
        }
        using Views = PinWrapper<
            std::pair<std::vector<std::string_view>, FixedVector<bool>>>;
        std::optional<Views> pinned;
        std::unique_ptr<DataArray> owned;
        std::vector<std::string_view> texts;
        std::vector<int64_t> input_positions;
        if (use_views) {
            FixedVector<int32_t> local_offsets(count);
            for (int64_t i = 0; i < count; ++i) {
                auto relative =
                    segment->get_chunk_by_offset(field, offsets[begin + i])
                        .second;
                AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                           "grep chunk offset exceeds int32");
                local_offsets[i] = relative;
            }
            pinned.emplace(segment->chunk_views_by_offsets<std::string_view>(
                &op_ctx, field, chunk, local_offsets));
        } else {
            owned = BulkSubscriptWithSnapshot(segment,
                                              snapshot,
                                              &op_ctx,
                                              field,
                                              meta,
                                              offsets + begin,
                                              count);
        }
        int64_t consumed = 0;
        while (consumed < count && positions.length() < opt.limit) {
            texts.clear();
            input_positions.clear();
            const auto quota = opt.limit - positions.length();
            // Matching groups are bounded by the read batch and remaining quota.
            // Prefetched rows stay owned/pinned until this read batch ends.
            while (consumed < count &&
                   static_cast<int64_t>(texts.size()) < quota) {
                check();
                bool valid;
                std::string_view text;
                if (pinned) {
                    const auto& [views, validity] = pinned->get();
                    valid = validity.empty() || validity[consumed];
                    text = views[consumed];
                } else {
                    const auto& validity = GetFieldDataRowValidData(*owned);
                    valid = validity.empty() || validity[consumed];
                    if (valid)
                        text = owned->scalars().string_data().data(consumed);
                }
                if (!valid) {
                    ++consumed;
                    continue;
                }
                texts.push_back(text);
                input_positions.push_back(begin + consumed);
                ++consumed;
            }
            if (texts.empty())
                continue;
            check();
            if (remaining == 0)
                ThrowInfo(OutOfRange,
                          fmt::format(
                              "grep result budget exceeded "
                              "(quotaAndLimits.limits.maxOutputSize={} bytes): "
                              "remaining=0 bytes",
                              opt.max_result_bytes));
            // The request cancellation token is the only execution deadline.
            auto result = session.ExecuteBatch(texts, {remaining});
            for (size_t i = 0; i < texts.size(); ++i) {
                check();
                if (!result.Selected(i))
                    continue;
                Charge(
                    16,
                    remaining,
                    opt.max_result_bytes);  // position, row list offset and validity
                CheckArrow(positions.Append(input_positions[i]));
                CheckArrow(details.Append());
                for (size_t j = 0; j < result.LineCount(i); ++j) {
                    check();
                    auto text = result.Content(i, j);
                    Charge(
                        16,
                        remaining,
                        opt.max_result_bytes);  // line/string/list offsets and validity
                    Charge(text.size(), remaining, opt.max_result_bytes);
                    CheckArrow(lines->Append());
                    CheckArrow(content->Append(text));
                    CheckArrow(spans->Append());
                    for (size_t k = 0; k < result.SpanCount(i, j); ++k) {
                        if (k % 1024 == 0)
                            check();
                        Charge(17, remaining, opt.max_result_bytes);
                        auto hit = result.GetSpan(i, j, k);
                        CheckArrow(span->Append());
                        CheckArrow(starts->Append(hit.start));
                        CheckArrow(ends->Append(hit.end));
                    }
                }
            }
        }
        begin += count;
    }
    check();
    std::shared_ptr<arrow::Array> positions_array, details_array;
    CheckArrow(positions.Finish(&positions_array));
    CheckArrow(details.Finish(&details_array));
    return arrow::RecordBatch::Make(
        arrow::schema({arrow::field("positions", arrow::int64(), false),
                       arrow::field("details", details_type, false)}),
        positions_array->length(),
        {positions_array, details_array});
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
        std::shared_ptr<arrow::RecordBatch> batch;
        try {
            batch = milvus::grep::ScanSegment(
                static_cast<milvus::segcore::SegmentInternalInterface*>(
                    segment),
                offsets,
                size,
                *options,
                views,
                token);
        } catch (const milvus::grep::Error& error) {
            using namespace milvus;
            using grep::Status;
            ErrorCode code = UnexpectedError;
            switch (error.status()) {
                case Status::InvalidPattern:
                    code = InvalidParameter;
                    break;
                case Status::InvalidUtf8:
                    code = error.operation() == grep::Operation::Compile
                               ? InvalidParameter
                               : DataFormatBroken;
                    break;
                case Status::ResourceLimit:
                    code = OutOfRange;
                    break;
                case Status::Cancelled:
                    code = FollyCancel;
                    break;
                case Status::AllocationError:
                    code = MemAllocateFailed;
                    break;
                default:
                    break;  // invalid ABI/spec/state is an internal error
            }
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
            // Keep the mapped code and the original diagnostic without passing
            // the added context through Grep's bounded 512-byte error buffer.
            throw SegcoreError(code, reason);
        }
        auto status = arrow::ExportRecordBatch(*batch, out_array, out_schema);
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
