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

#include "index/growing/GrowingJsonPathExtractor.h"

#include <limits>
#include <utility>

#include "common/EasyAssert.h"
#include "simdjson/error.h"

namespace milvus::index {

namespace {

// RFC 6901 token split. Unlike parse_json_pointer (common/JsonUtils.h), this
// keeps empty tokens: "/" is the empty key and "/a/" is ["a", ""], which is
// how Json::pointer and simdjson at_pointer read them.
std::vector<std::string>
SplitJsonPointer(std::string_view pointer) {
    std::vector<std::string> tokens;
    if (pointer.empty()) {
        return tokens;
    }
    if (pointer.front() != '/') {
        ThrowInfo(ErrorCode::InvalidParameter,
                  "growing JSON path '{}' must be empty or start with '/'",
                  pointer);
    }
    std::string token;
    for (size_t i = 1; i <= pointer.size(); ++i) {
        if (i == pointer.size() || pointer[i] == '/') {
            tokens.push_back(std::move(token));
            token.clear();
            continue;
        }
        if (pointer[i] != '~') {
            token.push_back(pointer[i]);
            continue;
        }
        if (i + 1 < pointer.size() &&
            (pointer[i + 1] == '0' || pointer[i + 1] == '1')) {
            token.push_back(pointer[i + 1] == '0' ? '~' : '/');
            ++i;
            continue;
        }
        ThrowInfo(ErrorCode::InvalidParameter,
                  "growing JSON path '{}' has an invalid '~' escape",
                  pointer);
    }
    return tokens;
}

}  // namespace

GrowingJsonPathKey
GrowingJsonPathKey::FromPointer(int64_t field_id, std::string_view pointer) {
    auto tokens = SplitJsonPointer(pointer);
    // Every valid RFC 6901 pointer is canonical; keep the check so the
    // identity is always the Json::pointer spelling the scan builds.
    auto canonical = Json::pointer(tokens);
    AssertInfo(canonical == pointer,
               "growing JSON path '{}' re-escapes to '{}'",
               pointer,
               canonical);
    return GrowingJsonPathKey(
        field_id, std::move(canonical), std::move(tokens));
}

GrowingJsonPathKey
GrowingJsonPathKey::FromNestedPath(
    int64_t field_id, const std::vector<std::string>& nested_path) {
    return GrowingJsonPathKey(
        field_id, Json::pointer(nested_path), nested_path);
}

bool
GrowingJsonPathKey::IsSupportedCast(JsonCastType::DataType cast_type,
                                    std::string_view cast_function) {
    return cast_type == JsonCastType::DataType::VARCHAR &&
           cast_function.empty();
}

JsonPathRowState
ClassifyJsonPathString(const Json& json,
                       std::string_view pointer,
                       std::string& value) {
    value.clear();
    // The same call the VARCHAR ==/IN scan and the sealed JSON path index
    // extraction use.
    auto str = json.at<std::string_view>(pointer);
    if (str.error() == simdjson::SUCCESS) {
        // Copy before the next parse reuses the thread-local buffer.
        value.assign(str.value_unsafe().data(), str.value_unsafe().size());
        return JsonPathRowState::Value;
    }
    // The same call the EXISTS scan uses.
    return json.exist(pointer) ? JsonPathRowState::TypeMismatch
                               : JsonPathRowState::Missing;
}

int64_t
JsonPathStringBatch::ByteSize() const noexcept {
    return static_cast<int64_t>(
        states_.capacity() * sizeof(JsonPathRowState) + arena_.capacity() +
        views_.capacity() * sizeof(std::string_view) +
        (value_valid_ ? states_.size() * sizeof(bool) : 0));
}

GrowingJsonPathExtractor::GrowingJsonPathExtractor(GrowingJsonPathKey key)
    : key_(std::move(key)) {
}

JsonPathStringBatch
GrowingJsonPathExtractor::Extract(int64_t row_begin, const JsonBatch& batch) {
    AssertInfo(row_begin >= 0 &&
                   batch.row_count <=
                       static_cast<size_t>(std::numeric_limits<int64_t>::max() -
                                           row_begin),
               "growing JSON path batch [{}, +{}) is outside the row domain",
               row_begin,
               batch.row_count);
    AssertInfo(batch.row_count == 0 || batch.values != nullptr,
               "growing JSON path batch has null values for {} rows",
               batch.row_count);
    const auto row_end = row_begin + static_cast<int64_t>(batch.row_count);
    if (row_begin > extracted_row_end_) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "growing JSON path '{}' expected row {} or earlier, got "
                  "range [{}, {})",
                  key_.Pointer(),
                  extracted_row_end_,
                  row_begin,
                  row_end);
    }

    const auto n = batch.row_count;
    JsonPathStringBatch out;
    out.row_begin_ = row_begin;
    out.states_.resize(n);
    out.views_.resize(n);
    out.value_valid_ = std::make_unique<bool[]>(n);

    // Pass 1: classify and append values to the arena. Views are bound in
    // pass 2, once the arena no longer reallocates.
    std::vector<std::pair<size_t, size_t>> spans(n);
    std::string value;
    for (size_t i = 0; i < n; ++i) {
        JsonPathRowState state = JsonPathRowState::NullRow;
        if (batch.valid == nullptr || batch.valid[i]) {
            try {
                state = ClassifyJsonPathString(
                    batch.values[i], key_.Pointer(), value);
            } catch (const simdjson::simdjson_error& e) {
                ThrowInfo(SimdjsonParseErrorToErrorCode(e.error()),
                          "growing JSON path '{}' failed on row {}: {}",
                          key_.Pointer(),
                          row_begin + static_cast<int64_t>(i),
                          e.what());
            }
        }
        out.states_[i] = state;
        ++out.counts_[static_cast<size_t>(state)];
        if (state == JsonPathRowState::Value) {
            spans[i] = {out.arena_.size(), value.size()};
            out.arena_.insert(out.arena_.end(), value.begin(), value.end());
        }
    }
    for (size_t i = 0; i < n; ++i) {
        if (out.states_[i] != JsonPathRowState::Value) {
            continue;
        }
        out.value_valid_[i] = true;
        // An empty Value keeps the default (empty) view: arena_.data() may be
        // null when every value is empty.
        if (spans[i].second > 0) {
            out.views_[i] = std::string_view(out.arena_.data() + spans[i].first,
                                             spans[i].second);
        }
    }

    if (row_end > extracted_row_end_) {
        extracted_row_end_ = row_end;
    }
    return out;
}

}  // namespace milvus::index
