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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "index/growing/GrowingJsonPathExtractor.h"

namespace milvus::index::test {
namespace {

using State = JsonPathRowState;

// (data, valid): the scan's three-valued result for one row.
using Tri = std::pair<bool, bool>;

// Owns padded JSON rows and a validity array the way a growing column does.
struct JsonRows {
    std::vector<Json> values;
    std::unique_ptr<bool[]> valid;
    size_t size = 0;

    explicit JsonRows(const std::vector<std::pair<std::string, bool>>& rows)
        : valid(std::make_unique<bool[]>(rows.size())), size(rows.size()) {
        values.reserve(rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            values.emplace_back(simdjson::padded_string(rows[i].first));
            valid[i] = rows[i].second;
        }
    }

    JsonBatch
    Slice(size_t begin, size_t end) const {
        return JsonBatch{
            end - begin, values.data() + begin, valid.get() + begin};
    }
};

// Scan oracles: the per-row logic of PhyUnaryRangeFilterExpr (VARCHAR == and
// !=), PhyTermFilterExpr (VARCHAR IN) and PhyExistsFilterExpr, copied from
// their brute-force JSON loops.
Tri
ScanCompare(const Json& json,
            bool valid,
            const std::string& pointer,
            const std::string& literal,
            bool equal) {
    if (!valid) {
        return {false, false};
    }
    auto x = json.at<std::string_view>(pointer);
    if (x.error()) {
        return {false, false};
    }
    return {equal ? x.value() == literal : x.value() != literal, true};
}

Tri
ScanIn(const Json& json,
       bool valid,
       const std::string& pointer,
       const std::set<std::string>& terms) {
    if (!valid) {
        return {false, false};
    }
    auto x = json.at<std::string_view>(pointer);
    if (x.error()) {
        return {false, false};
    }
    return {terms.count(std::string(x.value())) > 0, true};
}

Tri
ScanExists(const Json& json, bool valid, const std::string& pointer) {
    if (!valid) {
        return {false, true};
    }
    return {json.exist(pointer), true};
}

// What a GJ02/GJ03 consumer derives from one extracted row.
Tri
BatchCompare(const JsonPathStringBatch& batch,
             size_t i,
             const std::string& literal,
             bool equal) {
    if (batch.State(i) != State::Value) {
        return {false, false};
    }
    return {equal ? batch.Value(i) == literal : batch.Value(i) != literal,
            true};
}

Tri
BatchIn(const JsonPathStringBatch& batch,
        size_t i,
        const std::set<std::string>& terms) {
    if (batch.State(i) != State::Value) {
        return {false, false};
    }
    return {terms.count(std::string(batch.Value(i))) > 0, true};
}

Tri
BatchExists(const JsonPathStringBatch& batch, size_t i) {
    const auto state = batch.State(i);
    return {state == State::Value || state == State::TypeMismatch, true};
}

std::vector<JsonPathStringBatch>
ExtractInBatches(GrowingJsonPathExtractor& extractor,
                 const JsonRows& rows,
                 const std::vector<size_t>& cuts) {
    std::vector<JsonPathStringBatch> batches;
    size_t begin = 0;
    for (auto end : cuts) {
        batches.push_back(extractor.Extract(static_cast<int64_t>(begin),
                                            rows.Slice(begin, end)));
        begin = end;
    }
    return batches;
}

// Checks every extracted row against the scan oracles, including row
// alignment across batches.
void
ExpectMatchesScan(const JsonRows& rows,
                  const std::vector<JsonPathStringBatch>& batches,
                  const std::string& pointer,
                  const std::vector<std::string>& literals) {
    int64_t expected_begin = 0;
    for (const auto& batch : batches) {
        ASSERT_EQ(batch.RowBegin(), expected_begin);
        for (size_t i = 0; i < batch.RowCount(); ++i) {
            const auto row = static_cast<size_t>(batch.RowBegin()) + i;
            const auto& json = rows.values[row];
            const bool valid = rows.valid[row];
            SCOPED_TRACE(::testing::Message()
                         << "row " << row << " doc " << json.data()
                         << " pointer '" << pointer << "'");
            for (const auto& literal : literals) {
                EXPECT_EQ(BatchCompare(batch, i, literal, true),
                          ScanCompare(json, valid, pointer, literal, true));
                EXPECT_EQ(BatchCompare(batch, i, literal, false),
                          ScanCompare(json, valid, pointer, literal, false));
            }
            const std::set<std::string> terms(literals.begin(), literals.end());
            EXPECT_EQ(BatchIn(batch, i, terms),
                      ScanIn(json, valid, pointer, terms));
            EXPECT_EQ(BatchExists(batch, i), ScanExists(json, valid, pointer));
            EXPECT_EQ(batch.State(i) == State::NullRow, !valid);
        }
        expected_begin = batch.RowEnd();
    }
    EXPECT_EQ(expected_begin, static_cast<int64_t>(rows.size));
}

TEST(GrowingJsonPathKeyTest, CanonicalPointerIdentity) {
    auto key = GrowingJsonPathKey::FromPointer(101, "/a~1b/m~0n");
    EXPECT_EQ(key.FieldId(), 101);
    EXPECT_EQ(key.Pointer(), "/a~1b/m~0n");
    EXPECT_EQ(key.Tokens(), (std::vector<std::string>{"a/b", "m~n"}));
    EXPECT_EQ(key.CastType(), JsonCastType::DataType::VARCHAR);

    // An expression column carries unescaped tokens; both forms agree.
    auto from_expr = GrowingJsonPathKey::FromNestedPath(101, {"a/b", "m~n"});
    EXPECT_EQ(from_expr, key);
    EXPECT_TRUE(key.Matches(101, {"a/b", "m~n"}));
    EXPECT_FALSE(key.Matches(102, {"a/b", "m~n"}));
    EXPECT_FALSE(key.Matches(101, {"a", "b", "m~n"}));

    // "~01" is the key "~1", not "~/" or "/".
    auto tilde_one = GrowingJsonPathKey::FromPointer(1, "/~01");
    EXPECT_EQ(tilde_one.Tokens(), (std::vector<std::string>{"~1"}));

    // Root, the empty key and a nested empty key are distinct paths.
    auto root = GrowingJsonPathKey::FromPointer(1, "");
    auto empty_key = GrowingJsonPathKey::FromPointer(1, "/");
    auto nested_empty = GrowingJsonPathKey::FromPointer(1, "/a/");
    EXPECT_TRUE(root.Tokens().empty());
    EXPECT_EQ(empty_key.Tokens(), (std::vector<std::string>{""}));
    EXPECT_EQ(nested_empty.Tokens(), (std::vector<std::string>{"a", ""}));
    EXPECT_NE(root, empty_key);
    EXPECT_EQ(GrowingJsonPathKey::FromNestedPath(1, {}), root);
    EXPECT_EQ(GrowingJsonPathKey::FromNestedPath(1, {""}), empty_key);

    EXPECT_NE(GrowingJsonPathKey::FromPointer(1, "/a"),
              GrowingJsonPathKey::FromPointer(2, "/a"));
}

TEST(GrowingJsonPathKeyTest, RejectsNonCanonicalPointers) {
    for (const auto* pointer : {"a", "a/b", "/a~2", "/a~", "/~"}) {
        SCOPED_TRACE(pointer);
        try {
            GrowingJsonPathKey::FromPointer(1, pointer);
            FAIL() << "accepted non-canonical pointer";
        } catch (const SegcoreError& e) {
            EXPECT_EQ(e.get_error_code(), ErrorCode::InvalidParameter);
        }
    }
}

TEST(GrowingJsonPathKeyTest, OnlyPlainVarcharIsSupported) {
    using DT = JsonCastType::DataType;
    EXPECT_TRUE(GrowingJsonPathKey::IsSupportedCast(DT::VARCHAR, ""));
    EXPECT_FALSE(
        GrowingJsonPathKey::IsSupportedCast(DT::VARCHAR, "STRING_TO_DOUBLE"));
    for (auto type : {DT::UNKNOWN, DT::BOOL, DT::DOUBLE, DT::ARRAY, DT::JSON}) {
        EXPECT_FALSE(GrowingJsonPathKey::IsSupportedCast(type, ""));
    }
}

TEST(GrowingJsonPathExtractorTest, StateMatrixMatchesScan) {
    struct Case {
        std::string doc;
        bool valid;
        State state;
        std::string value;
    };
    const std::vector<Case> cases = {
        {R"({"a":"x"})", true, State::Value, "x"},
        {R"({"a":""})", true, State::Value, ""},
        {R"({"a":"say \"hi\"\n"})", true, State::Value, "say \"hi\"\n"},
        {R"({"a":"é中"})", true, State::Value, "\xc3\xa9\xe4\xb8\xad"},
        {R"({"a":"x"})", false, State::NullRow, ""},
        {R"({"a":""})", false, State::NullRow, ""},
        {R"({"b":"x"})", true, State::Missing, ""},
        {R"({})", true, State::Missing, ""},
        {R"({"a":null})", true, State::Missing, ""},
        {R"({"a":{}})", true, State::Missing, ""},
        {R"({"a":[]})", true, State::Missing, ""},
        {R"({"a":[null,{}]})", true, State::Missing, ""},
        {R"({"a":1})", true, State::TypeMismatch, ""},
        {R"({"a":-1.5e3})", true, State::TypeMismatch, ""},
        {R"({"a":true})", true, State::TypeMismatch, ""},
        {R"({"a":false})", true, State::TypeMismatch, ""},
        {R"({"a":["x"]})", true, State::TypeMismatch, ""},
        {R"({"a":{"b":"x"}})", true, State::TypeMismatch, ""},
        // A lone surrogate is a string token simdjson cannot decode.
        {R"({"a":"\uD800"})", true, State::TypeMismatch, ""},
        {R"({"a":["x"],"b":"y"})", true, State::TypeMismatch, ""},
        {R"({"A":"x"})", true, State::Missing, ""},
        {R"([1,2])", true, State::Missing, ""},
        {R"("a")", true, State::Missing, ""},
    };
    std::vector<std::pair<std::string, bool>> input;
    for (const auto& c : cases) {
        input.emplace_back(c.doc, c.valid);
    }
    JsonRows rows(input);
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(7, "/a"));
    auto batch = extractor.Extract(0, rows.Slice(0, rows.size));
    ASSERT_EQ(batch.RowCount(), cases.size());
    for (size_t i = 0; i < cases.size(); ++i) {
        SCOPED_TRACE(cases[i].doc);
        EXPECT_EQ(batch.State(i), cases[i].state);
        EXPECT_EQ(batch.Value(i), cases[i].value);
    }
    std::vector<JsonPathStringBatch> batches;
    batches.push_back(std::move(batch));
    ExpectMatchesScan(rows, batches, "/a", {"x", "", "y", "say \"hi\"\n"});
}

TEST(GrowingJsonPathExtractorTest, EmptyStringIsAValueNotNull) {
    JsonRows rows({{R"({"a":""})", true}, {R"({"a":null})", true}});
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    auto batch = extractor.Extract(0, rows.Slice(0, 2));
    EXPECT_EQ(batch.State(0), State::Value);
    EXPECT_TRUE(batch.Value(0).empty());
    EXPECT_EQ(batch.State(1), State::Missing);
    auto flat = batch.AsScalarBatch();
    ASSERT_EQ(flat.row_count, 2u);
    EXPECT_TRUE(flat.valid[0]);
    EXPECT_FALSE(flat.valid[1]);
    EXPECT_EQ(flat.values[0], "");
}

TEST(GrowingJsonPathExtractorTest, EscapedAndNestedPathsMatchScan) {
    const std::vector<std::pair<std::string, bool>> input = {
        {R"({"a/b":"slash","m~n":"tilde","~1":"t1","":"empty","a":{"b":"nested","":"inner"}})",
         true},
        {R"({"a":{"b":"nested-only"}})", true},
        {R"({"a/b":7,"m~n":null,"~1":{},"":["e"]})", true},
        {R"({"a":"flat"})", true},
        {R"({"arr":["zero","one"],"a":{"b":{"c":"deep"}}})", true},
        {R"({"a/b":"slash"})", false},
    };
    JsonRows rows(input);
    const std::vector<std::string> literals = {"slash",
                                               "tilde",
                                               "t1",
                                               "empty",
                                               "nested",
                                               "inner",
                                               "nested-only",
                                               "flat",
                                               "zero",
                                               "one",
                                               "deep",
                                               ""};
    for (const std::vector<std::string>& path :
         std::vector<std::vector<std::string>>{{"a/b"},
                                               {"m~n"},
                                               {"~1"},
                                               {""},
                                               {"a", "b"},
                                               {"a", ""},
                                               {"a", "b", "c"},
                                               {"arr", "1"},
                                               {"arr", "2"}}) {
        auto key = GrowingJsonPathKey::FromNestedPath(1, path);
        // The pointer is the one the scan builds for the same column.
        ASSERT_EQ(key.Pointer(), Json::pointer(path));
        GrowingJsonPathExtractor extractor(key);
        auto batches = ExtractInBatches(extractor, rows, {2, 3, rows.size});
        ExpectMatchesScan(rows, batches, key.Pointer(), literals);
    }

    // The key "a/b" and the nested path a -> b are different columns.
    GrowingJsonPathExtractor slash(GrowingJsonPathKey::FromPointer(1, "/a~1b"));
    GrowingJsonPathExtractor nested(GrowingJsonPathKey::FromPointer(1, "/a/b"));
    auto s = slash.Extract(0, rows.Slice(0, 2));
    auto n = nested.Extract(0, rows.Slice(0, 2));
    EXPECT_EQ(s.Value(0), "slash");
    EXPECT_EQ(n.Value(0), "nested");
    EXPECT_EQ(s.State(1), State::Missing);
    EXPECT_EQ(n.Value(1), "nested-only");
}

TEST(GrowingJsonPathExtractorTest, RootPointer) {
    JsonRows rows({{R"("top")", true},
                   {R"("")", true},
                   {R"({"a":"x"})", true},
                   {R"(null)", true},
                   {R"(3)", true}});
    GrowingJsonPathExtractor extractor(GrowingJsonPathKey::FromPointer(1, ""));
    std::vector<JsonPathStringBatch> batches;
    batches.push_back(extractor.Extract(0, rows.Slice(0, rows.size)));
    EXPECT_EQ(batches[0].State(0), State::Value);
    EXPECT_EQ(batches[0].Value(0), "top");
    EXPECT_EQ(batches[0].State(1), State::Value);
    EXPECT_EQ(batches[0].State(2), State::TypeMismatch);
    EXPECT_EQ(batches[0].State(3), State::Missing);
    EXPECT_EQ(batches[0].State(4), State::TypeMismatch);
    ExpectMatchesScan(rows, batches, "", {"top", "", "x"});
}

TEST(GrowingJsonPathExtractorTest, AllValidAndAllNullBatches) {
    JsonRows rows({{R"({"a":"x"})", true}, {R"({"a":1})", true}});
    GrowingJsonPathExtractor all_valid(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    // Null validity means every row is valid.
    auto batch =
        all_valid.Extract(0, JsonBatch{rows.size, rows.values.data(), nullptr});
    EXPECT_EQ(batch.State(0), State::Value);
    EXPECT_EQ(batch.State(1), State::TypeMismatch);
    EXPECT_EQ(batch.CountOf(State::NullRow), 0u);

    const bool invalid[2] = {false, false};
    GrowingJsonPathExtractor all_null(GrowingJsonPathKey::FromPointer(1, "/a"));
    auto nulls =
        all_null.Extract(0, JsonBatch{rows.size, rows.values.data(), invalid});
    EXPECT_EQ(nulls.CountOf(State::NullRow), 2u);
    EXPECT_EQ(nulls.Value(0), "");
    EXPECT_FALSE(nulls.AsScalarBatch().valid[0]);

    // A NULL row is never parsed, so its payload may be empty bytes.
    std::vector<Json> empty_payload(1);
    const bool null_row[1] = {false};
    GrowingJsonPathExtractor empty(GrowingJsonPathKey::FromPointer(1, "/a"));
    EXPECT_EQ(
        empty.Extract(0, JsonBatch{1, empty_payload.data(), null_row}).State(0),
        State::NullRow);
}

TEST(GrowingJsonPathExtractorTest, CountsAndScalarView) {
    JsonRows rows({{R"({"a":"x"})", true},
                   {R"({"a":"y"})", false},
                   {R"({"b":1})", true},
                   {R"({"a":2})", true},
                   {R"({"a":"z"})", true}});
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    auto first = extractor.Extract(0, rows.Slice(0, rows.size));
    EXPECT_EQ(first.RowBegin(), 0);
    EXPECT_EQ(first.RowEnd(), 5);
    // The same payload appended as Segment rows [5, 10).
    auto batch = extractor.Extract(5, rows.Slice(0, rows.size));
    EXPECT_EQ(batch.RowBegin(), 5);
    EXPECT_EQ(batch.RowEnd(), 10);
    EXPECT_EQ(batch.CountOf(State::Value), 2u);
    EXPECT_EQ(batch.CountOf(State::NullRow), 1u);
    EXPECT_EQ(batch.CountOf(State::Missing), 1u);
    EXPECT_EQ(batch.CountOf(State::TypeMismatch), 1u);
    EXPECT_GT(batch.ByteSize(), 0);

    auto flat = batch.AsScalarBatch();
    ASSERT_EQ(flat.row_count, 5u);
    const bool expected_valid[5] = {true, false, false, false, true};
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(flat.valid[i], expected_valid[i]) << i;
        EXPECT_EQ(flat.values[i], batch.Value(i)) << i;
    }
    EXPECT_EQ(flat.values[4], "z");
}

TEST(GrowingJsonPathExtractorTest, BatchOwnsValuesAfterInputAndMove) {
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    JsonPathStringBatch moved;
    {
        // Long values defeat any small-buffer storage.
        JsonRows rows({{R"({"a":")" + std::string(100, 'p') + R"("})", true},
                       {R"({"a":"q"})", true}});
        auto batch = extractor.Extract(0, rows.Slice(0, rows.size));
        moved = std::move(batch);
        // Parsing another document reuses the thread-local parser buffer.
        Json other(simdjson::padded_string(std::string(R"({"a":"zzzzzz"})")));
        ASSERT_FALSE(other.at<std::string_view>("/a").error());
    }
    EXPECT_EQ(moved.Value(0), std::string(100, 'p'));
    EXPECT_EQ(moved.Value(1), "q");
    JsonPathStringBatch again(std::move(moved));
    EXPECT_EQ(again.Value(0), std::string(100, 'p'));
    EXPECT_EQ(again.AsScalarBatch().values[1], "q");
}

TEST(GrowingJsonPathExtractorTest, WatermarkRejectsHolesAndAllowsRetry) {
    JsonRows rows({{R"({"a":"r0"})", true},
                   {R"({"a":"r1"})", true},
                   {R"({"a":"r2"})", true},
                   {R"({"a":"r3"})", true}});
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    EXPECT_EQ(extractor.ExtractedRowEnd(), 0);

    // A start beyond the watermark would leave a hole in coverage.
    EXPECT_THROW(extractor.Extract(1, rows.Slice(1, 2)), SegcoreError);
    EXPECT_EQ(extractor.ExtractedRowEnd(), 0);

    auto first = extractor.Extract(0, rows.Slice(0, 2));
    EXPECT_EQ(extractor.ExtractedRowEnd(), 2);

    // Retrying an extracted range is deterministic and keeps the watermark.
    auto retry = extractor.Extract(0, rows.Slice(0, 2));
    EXPECT_EQ(extractor.ExtractedRowEnd(), 2);
    ASSERT_EQ(retry.RowCount(), first.RowCount());
    for (size_t i = 0; i < first.RowCount(); ++i) {
        EXPECT_EQ(retry.State(i), first.State(i));
        EXPECT_EQ(retry.Value(i), first.Value(i));
    }

    // An overlapping range extends the watermark.
    auto overlap = extractor.Extract(1, rows.Slice(1, 3));
    EXPECT_EQ(overlap.RowBegin(), 1);
    EXPECT_EQ(overlap.Value(0), "r1");
    EXPECT_EQ(extractor.ExtractedRowEnd(), 3);

    // An empty batch at the watermark is accepted and changes nothing.
    auto empty = extractor.Extract(3, rows.Slice(3, 3));
    EXPECT_EQ(empty.RowCount(), 0u);
    EXPECT_EQ(empty.RowBegin(), 3);
    EXPECT_EQ(extractor.ExtractedRowEnd(), 3);

    auto last = extractor.Extract(3, rows.Slice(3, 4));
    EXPECT_EQ(last.Value(0), "r3");
    EXPECT_EQ(extractor.ExtractedRowEnd(), 4);

    EXPECT_THROW(extractor.Extract(-1, rows.Slice(0, 1)), SegcoreError);
    EXPECT_THROW(extractor.Extract(0, JsonBatch{1, nullptr, nullptr}),
                 SegcoreError);
    EXPECT_EQ(extractor.ExtractedRowEnd(), 4);
}

TEST(GrowingJsonPathExtractorTest, MalformedRowFailsWholeBatch) {
    // Unterminated string: simdjson rejects it while indexing the document.
    JsonRows rows({{R"({"a":"ok"})", true}, {R"({"a":"oops})", true}});
    GrowingJsonPathExtractor extractor(
        GrowingJsonPathKey::FromPointer(1, "/a"));
    try {
        extractor.Extract(0, rows.Slice(0, rows.size));
        FAIL() << "malformed JSON was extracted";
    } catch (const SegcoreError& e) {
        EXPECT_EQ(e.get_error_code(), ErrorCode::DataFormatBroken);
    }
    // Nothing was published: the good prefix is still unextracted.
    EXPECT_EQ(extractor.ExtractedRowEnd(), 0);
    auto good = extractor.Extract(0, rows.Slice(0, 1));
    EXPECT_EQ(good.Value(0), "ok");
    EXPECT_EQ(extractor.ExtractedRowEnd(), 1);
}

// Random documents over keys that need escaping, random paths, random batch
// cuts. Every row must agree with the scan oracles.
std::string
RandomKey(std::mt19937_64& rng) {
    static const std::vector<std::string> keys = {
        "a", "b", "", "a/b", "m~n", "~1", "~0", "/", "~", "0", "x y"};
    return keys[rng() % keys.size()];
}

std::string
QuoteKey(const std::string& key) {
    std::string out = "\"";
    for (char c : key) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    out.push_back('"');
    return out;
}

std::string
RandomValue(std::mt19937_64& rng, int depth) {
    static const std::vector<std::string> scalars = {R"("x")",
                                                     R"("")",
                                                     R"("y")",
                                                     R"("\"q\"")",
                                                     R"("é")",
                                                     "null",
                                                     "0",
                                                     "1.5",
                                                     "-7",
                                                     "true",
                                                     "false",
                                                     R"("a/b")",
                                                     R"("~1")",
                                                     "[]",
                                                     "{}"};
    const auto pick = rng() % 10;
    if (depth >= 3 || pick < 6) {
        return scalars[rng() % scalars.size()];
    }
    if (pick < 9) {
        std::string out = "{";
        const auto n = rng() % 4;
        for (size_t i = 0; i < n; ++i) {
            if (i > 0) {
                out += ",";
            }
            out += QuoteKey(RandomKey(rng)) + ":" + RandomValue(rng, depth + 1);
        }
        return out + "}";
    }
    std::string out = "[";
    const auto n = rng() % 3;
    for (size_t i = 0; i < n; ++i) {
        if (i > 0) {
            out += ",";
        }
        out += RandomValue(rng, depth + 1);
    }
    return out + "]";
}

TEST(GrowingJsonPathExtractorTest, RandomizedCrossBatchMatchesScan) {
    std::mt19937_64 rng(53855);
    const std::vector<std::string> literals = {
        "x", "", "y", "\"q\"", "\xc3\xa9", "a/b", "~1"};
    for (int round = 0; round < 200; ++round) {
        SCOPED_TRACE(round);
        const size_t n = rng() % 40;
        std::vector<std::pair<std::string, bool>> input;
        for (size_t i = 0; i < n; ++i) {
            std::string doc = "{";
            const auto keys = rng() % 4;
            for (size_t k = 0; k < keys; ++k) {
                if (k > 0) {
                    doc += ",";
                }
                doc += QuoteKey(RandomKey(rng)) + ":" + RandomValue(rng, 1);
            }
            doc += "}";
            input.emplace_back(doc, rng() % 8 != 0);
        }
        JsonRows rows(input);

        std::vector<std::string> path;
        const auto depth = rng() % 3;
        for (size_t d = 0; d < depth; ++d) {
            path.push_back(RandomKey(rng));
        }
        auto key = GrowingJsonPathKey::FromNestedPath(1, path);
        ASSERT_EQ(GrowingJsonPathKey::FromPointer(1, key.Pointer()), key);

        std::vector<size_t> cuts;
        for (size_t i = 1; i < n; ++i) {
            if (rng() % 4 == 0) {
                cuts.push_back(i);
            }
        }
        cuts.push_back(n);
        GrowingJsonPathExtractor extractor(key);
        auto batches = ExtractInBatches(extractor, rows, cuts);
        EXPECT_EQ(extractor.ExtractedRowEnd(), static_cast<int64_t>(n));
        ExpectMatchesScan(rows, batches, key.Pointer(), literals);
    }
}

}  // namespace
}  // namespace milvus::index::test
