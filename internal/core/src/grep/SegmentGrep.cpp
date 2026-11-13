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
#include <chrono>
#include <arrow/c/bridge.h>
#include <folly/ScopeGuard.h>
#include <fmt/format.h>
#include <limits>
#include <cmath>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include "common/CGoCatch.h"
#include "grep/Grep.h"
#include "grep/IndexCondition.h"
#include "log/Log.h"
#include "index/contracts/query/INgramReader.h"
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
    explicit GrepResultBuilder(const arrow::ListArray* elements = nullptr)
        : elements_(elements) {
    }

    int64_t
    Contribution(int64_t position) const {
        if (!elements_)
            return 1;
        const auto count = elements_->value_length(position);
        AssertInfo(!elements_->IsNull(position) && count > 0,
                   "grep candidate must have selected elements");
        return count;
    }

    int64_t
    MatchedUnits() const {
        return matched_units_;
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
                    {"line_number", result.LineNumber(i, j)},
                    {"line_start_offset", result.LineStartOffset(i, j)},
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
            const auto count = Contribution(input_positions[i]);
            matched_units_ += std::min(
                count, std::numeric_limits<int64_t>::max() - matched_units_);
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
    const arrow::ListArray* elements_;
    int64_t matched_units_ = 0;
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
    std::vector<int64_t> owned_positions;
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
        if (!owned_positions.empty()) {
            index = owned_positions[index];
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

// Compute the original read window before pruning. In particular, keep
// read-ahead offset validation and chunk boundaries independent of candidates.
int64_t
GrepReadCount(const SegmentInternalInterface* segment,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t max_rows,
              int64_t rows,
              bool use_views,
              const ChunkedColumnInterface* view_column) {
    AssertInfo(offsets[0] >= 0 && offsets[0] < rows,
               "grep offset out of range");
    const auto field = meta.get_id();
    int64_t chunk_begin = 0, chunk_end = rows;
    if (view_column && meta.get_data_type() != DataType::TEXT) {
        const auto& boundaries = view_column->GetNumRowsUntilChunk();
        const auto end =
            std::upper_bound(boundaries.begin(), boundaries.end(), offsets[0]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        chunk_begin = *(end - 1);
        chunk_end = *end;
    }
    const auto chunk =
        !view_column && use_views
            ? segment->get_chunk_by_offset(field, offsets[0]).first
            : 0;
    int64_t count = 1;
    while (count < max_rows) {
        const auto offset = offsets[count];
        AssertInfo(offset >= 0 && offset < rows, "grep offset out of range");
        if (offset < chunk_begin || offset >= chunk_end ||
            (!view_column && use_views &&
             segment->get_chunk_by_offset(field, offset).first != chunk)) {
            break;
        }
        ++count;
    }
    return count;
}

// LOB take can coalesce repeated row references. Fetch each input row once,
// then expose repeated views in input order without copying large documents.
void
ReadOwnedGrepBatch(const SegmentInternalInterface* segment,
                   const std::shared_ptr<const SegmentReadSnapshot>& snapshot,
                   milvus::OpContext* op_ctx,
                   const FieldMeta& meta,
                   const int64_t* offsets,
                   GrepInputBatch& batch) {
    const auto end = offsets + batch.count;
    const bool unique_sorted =
        std::adjacent_find(
            offsets, end, [](int64_t a, int64_t b) { return a >= b; }) == end;
    if (meta.get_data_type() != DataType::TEXT || unique_sorted) {
        batch.owned = BulkSubscriptWithSnapshot(segment,
                                                snapshot,
                                                op_ctx,
                                                meta.get_id(),
                                                meta,
                                                offsets,
                                                batch.count);
        return;
    }
    std::unordered_map<int64_t, int64_t> positions;
    std::vector<int64_t> unique_offsets;
    positions.reserve(batch.count);
    unique_offsets.reserve(batch.count);
    batch.owned_positions.reserve(batch.count);
    for (int64_t i = 0; i < batch.count; ++i) {
        auto [entry, inserted] =
            positions.try_emplace(offsets[i], unique_offsets.size());
        if (inserted)
            unique_offsets.push_back(offsets[i]);
        batch.owned_positions.push_back(entry->second);
    }
    batch.owned = BulkSubscriptWithSnapshot(segment,
                                            snapshot,
                                            op_ctx,
                                            meta.get_id(),
                                            meta,
                                            unique_offsets.data(),
                                            unique_offsets.size());
    if (unique_offsets.size() == static_cast<size_t>(batch.count)) {
        batch.owned_positions.clear();
    }
}

GrepInputBatch
ReadGrepBatch(const SegmentInternalInterface* segment,
              const std::shared_ptr<const SegmentReadSnapshot>& snapshot,
              milvus::OpContext* op_ctx,
              const FieldMeta& meta,
              const int64_t* offsets,
              int64_t count,
              bool use_views,
              const ChunkedColumnInterface* view_column) {
    const auto field = meta.get_id();
    GrepInputBatch batch;
    batch.count = count;
    if (view_column != nullptr && meta.get_data_type() == DataType::TEXT) {
        if (!TryReadInlineTextBatch(view_column, op_ctx, offsets, batch)) {
            ReadOwnedGrepBatch(segment, snapshot, op_ctx, meta, offsets, batch);
        }
        return batch;
    }
    if (view_column != nullptr) {
        const auto& boundaries = view_column->GetNumRowsUntilChunk();
        const auto end =
            std::upper_bound(boundaries.begin(), boundaries.end(), offsets[0]);
        AssertInfo(end != boundaries.begin() && end != boundaries.end(),
                   "grep offset out of column range");
        const int64_t chunk = std::distance(boundaries.begin(), end) - 1;
        const int64_t chunk_begin = *(end - 1);
        FixedVector<int32_t> local_offsets(count);
        for (int64_t i = 0; i < count; ++i) {
            const auto relative = offsets[i] - chunk_begin;
            AssertInfo(relative <= std::numeric_limits<int32_t>::max(),
                       "grep chunk offset exceeds int32");
            local_offsets[i] = relative;
        }
        batch.pinned.emplace(
            view_column->StringViewsByOffsets(op_ctx, chunk, local_offsets));
    } else if (use_views) {
        const auto chunk =
            segment->get_chunk_by_offset(field, offsets[0]).first;
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
        ReadOwnedGrepBatch(segment, snapshot, op_ctx, meta, offsets, batch);
    }
    return batch;
}

// Retain a request reader while deciding whether to query it. Preparation does not
// enumerate postings; a successful probe can finish without candidate lookup.
struct GrepIndex {
    index::IIndexReaderBasePtr owner;
    const index::INgramReader* reader;
    RegexLiteralCondition condition;
};

enum class GrepMode { Scan, Probe, Index };

constexpr int64_t kProbeMinRows = 32;
constexpr int64_t kProbeMaxRows = 256;
constexpr size_t kProbeMaxBytes = 256 * 1024;

struct GrepProbeBudget {
    int64_t row_limit;
    int64_t rows = 0;
    size_t bytes = 0;
    int64_t largest_batch = 0;
    bool exhausted = false;

    int64_t
    NextReadCount(int64_t available, int64_t remaining_matches) const {
        const auto growth = rows == 0 ? kProbeMinRows : largest_batch * 2;
        return std::min(
            {available, row_limit - rows, remaining_matches, growth});
    }

    void
    TrimAndCharge(GrepInputBatch& batch) {
        // Probe only borrows VARCHAR views. Inspect lengths before passing any
        // value to the matcher; an oversized value stays in the index suffix.
        const auto available = batch.count;
        int64_t accepted = 0;
        while (accepted < available && rows < row_limit) {
            const auto value = batch.Text(accepted);
            const auto length = value ? value->size() : 0;
            if (length > kProbeMaxBytes - bytes) {
                exhausted = true;
                break;
            }
            bytes += length;
            ++rows;  // NULLs, empty values, and repeated positions also count.
            ++accepted;
        }
        batch.count = accepted;
        largest_batch = std::max(largest_batch, accepted);
        exhausted = exhausted || rows == row_limit || bytes == kProbeMaxBytes;
    }
};

// Creates only sealed, whole-field NGRAM readers with complete row coverage.
// Loading errors propagate; they are not index ineligibility.
std::optional<GrepIndex>
PrepareGrepIndex(const SegmentInternalInterface* segment,
                 milvus::OpContext* op_ctx,
                 const FieldMeta& meta,
                 const Plan& plan,
                 int64_t size,
                 int64_t rows) {
    if (segment->type() != SegmentType::Sealed || size == 0) {
        return std::nullopt;
    }
    const auto registrations = segment->IndexRegistrations(meta.get_id());
    if (std::none_of(
            registrations.begin(), registrations.end(), [](const auto& r) {
                return r.kind == IndexKind::Ngram && r.json_path.empty();
            })) {
        return std::nullopt;
    }
    auto condition = DecodeLiteralConditionTree(plan.ConditionPayload());
    if (condition.IsTrue()) {
        return std::nullopt;
    }
    const auto capabilities =
        segment->ResolveIndexCapability(op_ctx, meta.get_id());
    for (const auto& entry : capabilities.entries()) {
        if (!entry.caps.ngram_candidates || entry.caps.nested ||
            !entry.json_path.empty() ||
            entry.value_type != meta.get_data_type()) {
            continue;
        }
        auto owner = segment->CreateIndexReader(op_ctx, entry.key);
        if (!owner || owner->CoordDomain() != index::Domain::Row ||
            owner->Count() != rows ||
            owner->ValueType() != meta.get_data_type()) {
            continue;
        }
        const auto* reader =
            dynamic_cast<const index::INgramReader*>(owner.get());
        if (!reader || !reader->CanHandleConditionTree(condition)) {
            continue;
        }
        return GrepIndex{std::move(owner), reader, std::move(condition)};
    }
    return std::nullopt;
}

TargetBitmap
BuildGrepCandidates(const GrepIndex& index,
                    const int64_t* offsets,
                    int64_t size,
                    int64_t rows) {
    TargetBitmap candidates(static_cast<size_t>(rows), false);
    for (int64_t i = 0; i < size; ++i) {
        // Invalid offsets remain subject to original read-window validation.
        // Include repeated row IDs when they occur again in the unread suffix.
        if (offsets[i] >= 0 && offsets[i] < rows) {
            candidates.set(offsets[i]);
        }
    }
    index.reader->ConditionTreeCandidates(index.condition, candidates);
    return candidates;
}

void
MatchGrepBatch(const GrepInputBatch& batch,
               int64_t begin,
               const int64_t* original_positions,
               Session& session,
               const CGrepScanOptions& opt,
               GrepResultBuilder& output) {
    std::vector<std::string_view> texts;
    std::vector<int64_t> input_positions;
    int64_t consumed = 0;
    while (consumed < batch.count && output.MatchedUnits() < opt.limit) {
        texts.clear();
        input_positions.clear();
        const auto quota = opt.limit - output.MatchedUnits();
        int64_t potential_units = 0;
        // Bound the group by possible contributions, so a document with many
        // elements can satisfy the quota without matching the rest of the batch.
        // Only selected documents are charged by Append; misses consume no quota.
        // Matching groups are bounded by the read batch and remaining quota.
        // Prefetched rows stay owned/pinned until this read batch ends.
        while (consumed < batch.count && potential_units < quota) {
            auto text = batch.Text(consumed++);
            if (!text) {
                continue;
            }
            texts.push_back(*text);
            input_positions.push_back(original_positions
                                          ? original_positions[consumed - 1]
                                          : begin + consumed - 1);
            potential_units +=
                std::min(output.Contribution(input_positions.back()),
                         quota - potential_units);
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
                const arrow::ListArray* elements,
                const CGrepScanOptions& opt,
                const std::vector<std::string_view>& patterns,
                const folly::CancellationToken& cancel_token) {
    const bool debug_enabled = VLOG_IS_ON(GLOG_DEBUG);
    const auto started = debug_enabled
                             ? std::chrono::steady_clock::now()
                             : std::chrono::steady_clock::time_point{};
    AssertInfo(size >= 0 && (size == 0 || offsets), "invalid grep offsets");
    AssertInfo(opt.batch_size > 0 && opt.limit > 0 && opt.max_regex_bytes > 0 &&
                   opt.max_dfa_cache_bytes > 0 && opt.max_result_bytes > 0,
               "invalid grep service budgets or limit");
    AssertInfo(opt.pattern_kind <= 1 && opt.case_mode <= 1 &&
                   opt.boundary <= 2 && opt.has_max_count <= 1,
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
    auto plan = grep::Plan::Compile(
        patterns,
        {static_cast<grep::PatternKind>(opt.pattern_kind),
         static_cast<grep::CaseMode>(opt.case_mode),
         static_cast<grep::Boundary>(opt.boundary),
         opt.has_max_count ? std::optional<uint64_t>(opt.max_count)
                           : std::nullopt,
         opt.before_context,
         opt.after_context},
        {opt.max_regex_bytes, opt.max_dfa_cache_bytes});
    if (opt.has_max_count && opt.max_count == 0) {
        // Compile first so -m 0 does not hide invalid patterns. No postings or
        // original text are needed when no matching lines may be returned.
        check();
        return GrepResultBuilder{}.Finish();
    }
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

    auto index = PrepareGrepIndex(segment, &op_ctx, meta, plan, size, rows);
    // A borrowed view lets Probe check byte lengths without materializing an
    // unbounded batch. TEXT and VARCHAR without such views remain index-first.
    auto mode = !index ? GrepMode::Scan
                : meta.get_data_type() == DataType::VARCHAR && use_views
                    ? GrepMode::Probe
                    : GrepMode::Index;
    // Saturate before multiplying: unlike the former policy, all positive
    // limits may use Probe, including values larger than INT64_MAX / 2.
    const auto probe_row_budget =
        std::min(size,
                 opt.limit >= kProbeMaxRows / 2
                     ? kProbeMaxRows
                     : std::max(kProbeMinRows, 2 * opt.limit));
    GrepProbeBudget probe{probe_row_budget};
    const bool probe_started = mode == GrepMode::Probe;
    std::optional<TargetBitmap> candidates;
    if (mode == GrepMode::Index) {
        candidates = BuildGrepCandidates(*index, offsets, size, rows);
        index.reset();
    }
    const auto* read_column =
        (use_views || borrow_inline_text) ? view_column : nullptr;
    std::vector<int64_t> selected_offsets, original_positions;
    GrepResultBuilder output(elements);
    int64_t begin = 0;
    int64_t window_end = 0;
    while (begin < size && output.MatchedUnits() < opt.limit) {
        check();
        if (begin == window_end) {
            // Validate the original window before any smaller probe reads.
            // Keep this boundary even when a probe switches to the index.
            window_end = begin + GrepReadCount(segment,
                                               meta,
                                               offsets + begin,
                                               std::min<int64_t>(opt.batch_size,
                                                                 size - begin),
                                               rows,
                                               use_views,
                                               read_column);
        }
        if (mode == GrepMode::Index && !candidates) {
            // Switch once. Do not repeat postings queries per window.
            candidates = BuildGrepCandidates(
                *index, offsets + begin, size - begin, rows);
            index.reset();
        }
        auto input_count =
            mode == GrepMode::Probe
                ? probe.NextReadCount(window_end - begin,
                                      opt.limit - output.MatchedUnits())
                : window_end - begin;
        const int64_t* read_offsets = offsets + begin;
        int64_t read_count = input_count;
        if (candidates) {
            selected_offsets.clear();
            original_positions.clear();
            for (int64_t i = 0; i < input_count; ++i) {
                if ((*candidates)[offsets[begin + i]]) {
                    selected_offsets.push_back(offsets[begin + i]);
                    original_positions.push_back(begin + i);
                }
            }
            read_offsets = selected_offsets.data();
            read_count = selected_offsets.size();
        }
        if (read_count != 0) {
            auto batch = ReadGrepBatch(segment,
                                       snapshot,
                                       &op_ctx,
                                       meta,
                                       read_offsets,
                                       read_count,
                                       use_views,
                                       read_column);
            if (mode == GrepMode::Probe) {
                probe.TrimAndCharge(batch);
                input_count = batch.count;
            }
            MatchGrepBatch(batch,
                           begin,
                           candidates ? original_positions.data() : nullptr,
                           session,
                           opt,
                           output);
        }
        begin += input_count;
        if (mode == GrepMode::Probe && probe.exhausted) {
            // The next iteration queries only the unread suffix. If input is
            // exhausted or the limit is satisfied, no lookup is needed.
            mode = GrepMode::Index;
        }
    }
    auto result = output.Finish();
    if (debug_enabled) {
        const auto elapsed = std::chrono::duration<double>(
                                 std::chrono::steady_clock::now() - started)
                                 .count();
        LOG_DEBUG(
            "grep execution finished: segmentID={}, fieldID={}, path={}, "
            "inputRows={}, selectedRows={}, elapsedMs={}",
            segment->get_segment_id(),
            opt.field_id,
            candidates ? (probe_started ? "probe_then_index" : "index_only")
                       : (probe_started ? "probe_only" : "scan"),
            size,
            result->num_rows(),
            elapsed * 1000);
    }
    return result;
}
}  // namespace
}  // namespace milvus::grep

CStatus
ScanGrep(void* segment,
         struct ArrowSchema* input_schema,
         struct ArrowArray* input_array,
         const CGrepScanRequest* request,
         ArrowSchema* out_schema,
         ArrowArray* out_array,
         void* cancellation_source) {
    try {
        AssertInfo(segment && request && out_schema && out_array,
                   "null grep argument");
        const auto* options = &request->options;
        const auto* patterns = request->patterns;
        const auto pattern_bytes = request->pattern_bytes;
        const auto* lengths = request->lengths;
        const auto pattern_count = request->pattern_count;
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
        AssertInfo(input_schema && input_array, "null grep input record");
        auto imported = arrow::ImportRecordBatch(input_array, input_schema);
        if (!imported.ok()) {
            ThrowInfo(milvus::segcore::ArrowExportErrorCode(imported.status()),
                      "import grep input record: {}",
                      imported.status().ToString());
        }
        auto input = std::move(imported).ValueOrDie();
        const auto columns = input->schema()->GetAllFieldIndices(
            milvus::segcore::kSegOffsetFieldName);
        AssertInfo(columns.size() == 1,
                   "grep input requires one {} column",
                   milvus::segcore::kSegOffsetFieldName);
        auto offsets = std::dynamic_pointer_cast<arrow::Int64Array>(
            input->column(columns[0]));
        AssertInfo(
            offsets && offsets->null_count() == 0 &&
                offsets->length() == input->num_rows(),
            "grep offsets must be non-null Int64 aligned with input rows");
        const auto element_columns = input->schema()->GetAllFieldIndices(
            milvus::segcore::kQueryElementIndicesFieldName);
        AssertInfo(element_columns.size() <= 1,
                   "duplicate grep element column");
        std::shared_ptr<arrow::ListArray> elements;
        if (!element_columns.empty()) {
            elements = std::dynamic_pointer_cast<arrow::ListArray>(
                input->column(element_columns[0]));
            AssertInfo(elements &&
                           elements->value_type()->id() == arrow::Type::INT32 &&
                           elements->length() == input->num_rows(),
                       "grep element column must be List<Int32> aligned with "
                       "input rows");
        }
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
                offsets->raw_values(),
                input->num_rows(),
                elements.get(),
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
