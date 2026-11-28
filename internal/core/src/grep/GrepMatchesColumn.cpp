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

#include "grep/GrepMatchesColumn.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/EasyAssert.h"
#include "fmt/format.h"
#include "folly/CancellationToken.h"
#include "grep/CompiledPlan.h"
#include "grep/Grep.h"
#include "grep/SegmentGrepReader.h"
#include "nlohmann/json.hpp"
#include "segcore/SegcoreConfig.h"

namespace milvus::grep {

namespace {

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

// Serializes the selected lines of text `index` of a batch result. `used` is
// the number of bytes already charged to the column.
std::string
FormatLines(const Result& result, size_t index, uint64_t used, uint64_t limit) {
    std::string document = "[";
    for (size_t j = 0; j < result.LineCount(index); ++j) {
        const bool is_match = result.IsMatch(index, j);
        auto detail = nlohmann::json{
            {"type", is_match ? "match" : "context"},
            {"content", std::string(result.Content(index, j))},
            {"line_number", result.LineNumber(index, j)},
            {"line_start_offset", result.LineStartOffset(index, j)}};
        if (is_match) {
            std::vector<int64_t> starts, ends;
            const auto count = result.SpanCount(index, j);
            starts.reserve(count);
            ends.reserve(count);
            for (size_t k = 0; k < count; ++k) {
                auto hit = result.GetSpan(index, j, k);
                const auto max =
                    static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
                AssertInfo(hit.start <= max && hit.end <= max,
                           "grep span boundary exceeds Int64");
                starts.push_back(static_cast<int64_t>(hit.start));
                ends.push_back(static_cast<int64_t>(hit.end));
            }
            detail["starts"] = std::move(starts);
            detail["ends"] = std::move(ends);
        }
        auto line = detail.dump();
        CheckResultBudget(used + document.size() + (j != 0) + line.size() + 1,
                          limit);
        if (j != 0) {
            document += ',';
        }
        document += line;
    }
    document += ']';
    return document;
}

}  // namespace

std::unique_ptr<DataArray>
BuildGrepMatchesColumn(const segcore::SegmentInternalInterface* segment,
                       FieldId output_id,
                       const proto::plan::GrepMatches& item,
                       const Plan* plan,
                       const int64_t* offsets,
                       int64_t size,
                       milvus::OpContext* op_ctx) {
    AssertInfo(size >= 0 && (size == 0 || offsets != nullptr),
               "invalid grep_matches offsets");
    const auto& spec = item.spec();
    auto column = std::make_unique<DataArray>();
    column->set_field_id(output_id.get());
    column->set_type(proto::schema::DataType::JSON);
    auto* values = column->mutable_scalars()->mutable_json_data();
    auto* valid = column->mutable_valid_data();
    values->mutable_data()->Reserve(size);
    valid->Resize(size, false);
    for (int64_t i = 0; i < size; ++i) {
        values->add_data();
    }
    if (size == 0 || spec.select_none()) {
        // A final "-m 0" selects no line: like rg, the pattern is not
        // compiled and no text is read.
        return column;
    }

    const FieldId field(spec.column_info().field_id());
    const auto schema = segment->get_schema_snapshot();
    const auto& meta = (*schema)[field];
    const auto data_type = meta.get_data_type();
    AssertInfo(data_type == DataType::VARCHAR || data_type == DataType::TEXT,
               "grep_matches requires a text field");

    const auto& config = segcore::SegcoreConfig::default_config();
    const auto read_batch_size =
        std::max<int64_t>(1, config.get_grep_batch_size());
    const auto max_result_bytes = config.get_grep_max_result_bytes();

    AssertInfo(plan != nullptr, "grep_matches patterns are not compiled");
    auto session = plan->CreateSession(MatchesSelection(item));
    std::optional<folly::CancellationCallback> cancel;
    if (op_ctx != nullptr) {
        cancel.emplace(
            op_ctx->cancellation_token,
            [token = session.GetCancellationToken()] { token.Cancel(); });
    }

    const auto snapshot = segment->CaptureReadSnapshot();
    const auto rows = segment->get_row_count();
    const bool borrow_inline_text = snapshot && data_type == DataType::TEXT &&
                                    !meta.is_external_field() &&
                                    segment->HasFieldData(field);
    const auto* view_column =
        snapshot && (data_type == DataType::VARCHAR || borrow_inline_text)
            ? snapshot->GetColumn(field)
            : nullptr;
    const bool use_views = data_type == DataType::VARCHAR &&
                           segment->HasFieldData(field) &&
                           (!snapshot || view_column != nullptr);
    const auto* read_column =
        (use_views || borrow_inline_text) ? view_column : nullptr;

    uint64_t total_bytes = 0;
    std::vector<std::string_view> texts;
    std::vector<int64_t> text_rows;
    int64_t begin = 0;
    while (begin < size) {
        if (op_ctx != nullptr &&
            op_ctx->cancellation_token.isCancellationRequested()) {
            ThrowInfo(FollyCancel, "grep cancelled");
        }
        const auto count =
            GrepReadCount(segment,
                          meta,
                          offsets + begin,
                          std::min(read_batch_size, size - begin),
                          rows,
                          use_views,
                          read_column);
        auto batch = ReadGrepBatch(segment,
                                   snapshot,
                                   op_ctx,
                                   meta,
                                   offsets + begin,
                                   count,
                                   use_views,
                                   read_column);
        texts.clear();
        text_rows.clear();
        for (int64_t i = 0; i < count; ++i) {
            if (auto text = batch.Text(i)) {
                texts.push_back(*text);
                text_rows.push_back(begin + i);
            }
        }
        if (!texts.empty()) {
            CheckResultBudget(total_bytes + 1, max_result_bytes);
            // The engine bounds the lines it copies by the remaining budget;
            // the JSON form is charged again below, with its punctuation.
            auto result =
                session.ExecuteBatch(texts, max_result_bytes - total_bytes);
            for (size_t i = 0; i < texts.size(); ++i) {
                if (!result.Selected(i)) {
                    continue;
                }
                auto document =
                    FormatLines(result, i, total_bytes, max_result_bytes);
                total_bytes += document.size();
                CheckResultBudget(total_bytes, max_result_bytes);
                *values->mutable_data(text_rows[i]) = std::move(document);
                valid->Set(text_rows[i], true);
            }
        }
        begin += count;
    }
    return column;
}

}  // namespace milvus::grep
