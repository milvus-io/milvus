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

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/Json.h"
#include "common/JsonCastType.h"
#include "index/contracts/growing/IGrowingIndex.h"

// Growing JSON path extraction (GJ01). Converts raw growing JSON rows into
// row-aligned VARCHAR batches for ONE configured path, before any growing
// index consumes them. This is the growing counterpart of the sealed
// "extract before the builder" rule in IJsonIndexReader.h. It routes no query
// and publishes no reader; see
// docs/design-docs/design_docs/20261018-growing_json_path_extraction.md.
namespace milvus::index {

// Borrowed growing JSON input: values[i] is Segment row row_begin + i.
// Null validity means every row is valid.
using JsonBatch = ScalarBatch<Json>;

// Per-row result of extracting the configured path as VARCHAR. Each state is
// defined by the scan's own calls, so a consumer reproduces the scan without
// re-parsing:
//   Value        Json::at<std::string_view>(pointer) succeeds (empty strings
//                included). Comparisons use the value.
//   NullRow      the field-level validity bit is clear.
//   Missing      the row is valid, has no string at the path, and
//                Json::exist(pointer) is false: the path is absent, holds
//                JSON null, or holds an empty object/array.
//   TypeMismatch the row is valid, has no string at the path, and
//                Json::exist(pointer) is true (number, bool, non-empty
//                object/array, undecodable string).
// For VARCHAR ==, !=, IN and NOT IN, every state except Value is UNKNOWN
// (data = 0, valid = 0). EXISTS is true for Value and TypeMismatch only.
enum class JsonPathRowState : uint8_t {
    Value = 0,
    NullRow = 1,
    Missing = 2,
    TypeMismatch = 3,
};

inline constexpr size_t kJsonPathRowStateCount = 4;

// Identity of one growing JSON path index input: (field, canonical RFC 6901
// pointer, cast type). Only VARCHAR without a cast function is supported. Two
// keys are equal exactly when their unescaped token lists are equal.
class GrowingJsonPathKey {
 public:
    // `pointer` is the json_path index parameter form, e.g. "/a/b" or
    // "/a~1b" for the key "a/b". "" addresses the document root and "/"
    // addresses the empty key. The pointer must be canonical: re-escaping its
    // tokens must give the same string, so "~2", a trailing "~" or a missing
    // leading '/' throws InvalidParameter.
    static GrowingJsonPathKey
    FromPointer(int64_t field_id, std::string_view pointer);

    // `nested_path` is the unescaped token list of an expression column
    // (ColumnInfo::nested_path_). The pointer is built with Json::pointer,
    // exactly as the scan builds it.
    static GrowingJsonPathKey
    FromNestedPath(int64_t field_id,
                   const std::vector<std::string>& nested_path);

    // True only for VARCHAR with no cast function. Callers reject other
    // index metadata before constructing an extractor; such paths stay on
    // the scan.
    static bool
    IsSupportedCast(JsonCastType::DataType cast_type,
                    std::string_view cast_function);

    int64_t
    FieldId() const noexcept {
        return field_id_;
    }

    const std::string&
    Pointer() const noexcept {
        return pointer_;
    }

    const std::vector<std::string>&
    Tokens() const noexcept {
        return tokens_;
    }

    JsonCastType::DataType
    CastType() const noexcept {
        return JsonCastType::DataType::VARCHAR;
    }

    // Whether an expression on (field_id, nested_path) addresses this key.
    bool
    Matches(int64_t field_id,
            const std::vector<std::string>& nested_path) const {
        return field_id == field_id_ && nested_path == tokens_;
    }

    bool
    operator==(const GrowingJsonPathKey& other) const {
        return field_id_ == other.field_id_ && pointer_ == other.pointer_;
    }

    bool
    operator!=(const GrowingJsonPathKey& other) const {
        return !(*this == other);
    }

 private:
    GrowingJsonPathKey(int64_t field_id,
                       std::string pointer,
                       std::vector<std::string> tokens)
        : field_id_(field_id),
          pointer_(std::move(pointer)),
          tokens_(std::move(tokens)) {
    }

    int64_t field_id_{0};
    std::string pointer_;
    std::vector<std::string> tokens_;
};

// Classify one valid row. On Value, `value` receives an owned copy of the
// string; otherwise it is cleared. The copy is required because the view
// returned by simdjson ondemand points into a thread-local parser buffer that
// the next parse on this thread overwrites.
JsonPathRowState
ClassifyJsonPathString(const Json& json,
                       std::string_view pointer,
                       std::string& value);

// One immutable, self-contained extraction result for Segment rows
// [RowBegin(), RowEnd()). Index i is row RowBegin() + i. The batch owns every
// string; it borrows nothing from the input and may be read from several
// threads once returned. Move-only, because Values() views its own arena.
class JsonPathStringBatch {
 public:
    JsonPathStringBatch() = default;
    JsonPathStringBatch(JsonPathStringBatch&&) noexcept = default;
    JsonPathStringBatch&
    operator=(JsonPathStringBatch&&) noexcept = default;
    JsonPathStringBatch(const JsonPathStringBatch&) = delete;
    JsonPathStringBatch&
    operator=(const JsonPathStringBatch&) = delete;

    int64_t
    RowBegin() const noexcept {
        return row_begin_;
    }

    int64_t
    RowEnd() const noexcept {
        return row_begin_ + static_cast<int64_t>(states_.size());
    }

    size_t
    RowCount() const noexcept {
        return states_.size();
    }

    JsonPathRowState
    State(size_t i) const {
        AssertInfo(i < states_.size(),
                   "json path batch row {} out of range {}",
                   i,
                   states_.size());
        return states_[i];
    }

    // Empty unless State(i) == Value; an empty Value is a real empty string.
    std::string_view
    Value(size_t i) const {
        AssertInfo(i < views_.size(),
                   "json path batch row {} out of range {}",
                   i,
                   views_.size());
        return views_[i];
    }

    const JsonPathRowState*
    States() const noexcept {
        return states_.data();
    }

    size_t
    CountOf(JsonPathRowState state) const noexcept {
        return counts_[static_cast<size_t>(state)];
    }

    // Flat view for existing string consumers (IAppendable<TextBatch>-style):
    // valid[i] is true exactly for Value rows, so NullRow, Missing and
    // TypeMismatch all read as NULL, the sealed JSON path index convention.
    // Borrowed from this batch; valid until the batch is destroyed.
    ScalarBatch<std::string_view>
    AsScalarBatch() const noexcept {
        return ScalarBatch<std::string_view>{
            states_.size(), views_.data(), value_valid_.get()};
    }

    // Heap bytes owned by this batch, for consumer memory accounting.
    int64_t
    ByteSize() const noexcept;

 private:
    friend class GrowingJsonPathExtractor;

    int64_t row_begin_{0};
    std::vector<JsonPathRowState> states_;
    std::vector<char> arena_;
    std::vector<std::string_view> views_;
    std::unique_ptr<bool[]> value_valid_;
    std::array<size_t, kJsonPathRowStateCount> counts_{};
};

// Stateful producer for one key. Not thread-safe: the Segment write path
// serializes calls, as for IAppendable. Rows are immutable once inserted, so
// extraction is deterministic and may be repeated.
class GrowingJsonPathExtractor {
 public:
    explicit GrowingJsonPathExtractor(GrowingJsonPathKey key);

    const GrowingJsonPathKey&
    Key() const noexcept {
        return key_;
    }

    // End of the contiguous Segment row prefix [0, end) extracted so far.
    // An upper bound for any consumer's coverage; it is not coverage itself.
    int64_t
    ExtractedRowEnd() const noexcept {
        return extracted_row_end_;
    }

    // Extract rows [row_begin, row_begin + batch.row_count). row_begin must
    // not exceed ExtractedRowEnd(): a later start would leave a hole. Ranges
    // at or below the watermark are re-extracted with identical results so a
    // consumer that failed after extraction can retry. All or nothing: if any
    // row fails, nothing is returned and the watermark is unchanged. A
    // malformed document throws DataFormatBroken naming the row.
    JsonPathStringBatch
    Extract(int64_t row_begin, const JsonBatch& batch);

 private:
    GrowingJsonPathKey key_;
    int64_t extracted_row_end_{0};
};

}  // namespace milvus::index
