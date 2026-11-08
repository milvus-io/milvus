// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <utility>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/RegexQuery.h"
#include "common/Schema.h"
#include "common/Volnitsky.h"
#include "index/scalar/fmindex/FmIndexReader.h"
#include "index/fmindex/FMIndex.h"
#include "exec/QueryContext.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;

namespace {

struct SealedFMMatch {
    SchemaPtr schema;
    FieldId varchar_id;
    FieldId int_id;
    std::unique_ptr<test::expr_index::RawFieldFiles> raw_files;
    segcore::SegmentSealedUPtr segment;
};

expr::TypedExprPtr
MakeMatchTypedExpr(const SchemaPtr& schema,
                   FieldId field_id,
                   const std::string& pattern,
                   bool nullable,
                   proto::plan::OpType op = proto::plan::OpType::Match) {
    auto* unary = test::GenUnaryRangeExpr(op, pattern);
    unary->set_allocated_column_info(
        test::GenColumnInfo(field_id.get(),
                            proto::schema::DataType::VarChar,
                            false,
                            false,
                            proto::schema::DataType::None,
                            nullable));
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary);
    auto parser = milvus::query::ProtoParser(schema);
    return parser.ParseExprs(*expr);
}

bool
CompiledUseIndexCursor(const expr::TypedExprPtr& typed_expr,
                       const segcore::SegmentInternalInterface* segment,
                       int64_t active_count) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({typed_expr}, &exec_context, {}, false);
    auto* seg_expr = dynamic_cast<exec::SegmentExpr*>(compiled[0].get());
    if (seg_expr == nullptr) {
        return false;
    }
    return seg_expr->UseIndexCursor();
}

SealedFMMatch
LoadSealedFMMatch(const std::vector<std::string>& rows,
                  const std::vector<FieldDataPtr>& varchar_chunks = {},
                  bool nullable = false,
                  const uint8_t* valid_bitmap = nullptr,
                  const std::vector<int64_t>* ints = nullptr,
                  bool with_index = true) {
    SealedFMMatch out;
    out.raw_files = std::make_unique<test::expr_index::RawFieldFiles>();
    out.schema = std::make_shared<Schema>();
    out.varchar_id =
        out.schema->AddDebugField("fm_match", DataType::VARCHAR, nullable);
    if (ints != nullptr) {
        out.int_id = out.schema->AddDebugField("fm_int", DataType::INT64);
    }
    out.segment = segcore::CreateSealedSegment(out.schema);
    auto chunks = varchar_chunks;
    if (chunks.empty()) {
        chunks.push_back(
            test::expr_index::StringField(rows, nullable, valid_bitmap));
    }
    auto raw_info = out.raw_files->Prepare(out.varchar_id, chunks);
    out.segment->LoadFieldData(raw_info);
    if (ints != nullptr) {
        auto field =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        field->FillFieldData(ints->data(), ints->size());
        auto info = out.raw_files->Prepare(out.int_id, {field});
        out.segment->LoadFieldData(info);
    }
    if (!with_index) {
        return out;
    }
    auto opened = test::expr_index::BuildIndex(out.varchar_id,
                                               DataType::VARCHAR,
                                               index::FMINDEX_INDEX_TYPE,
                                               chunks,
                                               {{index::FM_SA_SAMPLE_RATE, 8}},
                                               DataType::NONE,
                                               false,
                                               true);
    test::expr_index::InstallIndex(
        *out.segment, out.varchar_id, DataType::VARCHAR, std::move(opened));
    return out;
}

std::vector<std::string>
MakeLongTextZebraRows(size_t nb) {
    std::string filler(500, 'y');
    std::vector<std::string> data;
    data.reserve(nb);
    for (size_t i = 0; i < nb; i++) {
        std::string row = filler;
        if (i % 250 == 0) {
            row += "ZEBRA";
        }
        if (i >= 100 && (i - 100) % 250 == 0) {
            row = "QOP" + row;
        }
        if (i == 0) {
            row = "QOP" + row;
        }
        data.push_back(std::move(row));
    }
    return data;
}

std::shared_ptr<const index::FmIndexReader>
BuildFmPatternReader(const std::vector<std::string>& rows) {
    auto engine = std::make_shared<index::fmindex::FMIndex>();
    std::vector<std::string_view> docs(rows.begin(), rows.end());
    engine->Build(docs, 32);
    auto storage =
        index::FmIndexStorage::Create(nullptr,
                                      std::move(engine),
                                      TargetBitmap(rows.size(), false),
                                      rows.size(),
                                      DataType::VARCHAR,
                                      false,
                                      index::FmIndexStateOrigin::Builder);
    return std::make_shared<const index::FmIndexReader>(std::move(storage));
}

}  // namespace

TEST(FmIndexExpressionTest, DeclinedOperationsFallBackToRawScan) {
    const std::vector<std::string> data{
        "apple", "banana", "grape", "melon", "zebra", "apse", "ane", ""};
    const size_t nb = data.size();
    auto loaded = LoadSealedFMMatch(data);
    const auto& schema = loaded.schema;
    const auto field_id = loaded.varchar_id;
    const auto& segment = loaded.segment;
    auto run = [&](proto::plan::OpType op, std::string value) {
        auto* unary = test::GenUnaryRangeExpr(op, value);
        unary->set_allocated_column_info(test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false));
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary);
        auto parser = milvus::query::ProtoParser(schema);
        auto typed_expr = parser.ParseExprs(*expr);
        auto node = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           typed_expr);
        return milvus::query::ExecuteQueryExpr(
            node, segment.get(), nb, MAX_TIMESTAMP);
    };
    auto run_binary = [&](const std::string& lower, const std::string& upper) {
        proto::plan::GenericValue lower_val;
        lower_val.set_string_val(lower);
        proto::plan::GenericValue upper_val;
        upper_val.set_string_val(upper);
        auto typed_expr = std::make_shared<milvus::expr::BinaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(field_id, DataType::VARCHAR),
            lower_val,
            upper_val,
            false,
            false);
        auto node = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           typed_expr);
        return milvus::query::ExecuteQueryExpr(
            node, segment.get(), nb, MAX_TIMESTAMP);
    };
    auto expect_rows =
        [&](const BitsetType& got,
            const std::function<bool(const std::string&)>& oracle,
            const char* what) {
            for (size_t i = 0; i < nb; i++) {
                EXPECT_EQ(got[i], oracle(data[i]))
                    << what << " row " << i << " (" << data[i] << ")";
            }
        };

    // DECLINED: lexicographic range — must scan, not throw.
    expect_rows(
        run(proto::plan::OpType::GreaterThan, "m"),
        [](const std::string& s) { return s > "m"; },
        "GreaterThan");
    expect_rows(
        run(proto::plan::OpType::LessEqual, "banana"),
        [](const std::string& s) { return s <= "banana"; },
        "LessEqual");
    expect_rows(
        run_binary("a", "z"),
        [](const std::string& s) { return s > "a" && s < "z"; },
        "BinaryRange a-z");
    // General LIKE 'a%e' (interior wildcard -> Match). Either the candidate
    // recheck path or the scan (tiny corpus often trips the cost guard) must
    // equal the brute-force oracle.
    expect_rows(
        run(proto::plan::OpType::Match, "a%e"),
        [](const std::string& s) {
            return !s.empty() && s.front() == 'a' && s.back() == 'e';
        },
        "Match a%e");
    // ACCEPTED: anchored ops answered by the index, must equal the scan.
    expect_rows(
        run(proto::plan::OpType::InnerMatch, "an"),
        [](const std::string& s) { return s.find("an") != std::string::npos; },
        "InnerMatch an");
    expect_rows(
        run(proto::plan::OpType::PrefixMatch, "ap"),
        [](const std::string& s) { return s.rfind("ap", 0) == 0; },
        "PrefixMatch ap");
    // DECLINED: equality (`==`, lowered from `== "banana"`) is not accelerated
    // by FMINDEX — must fall back to the scan and still return correct rows.
    expect_rows(
        run(proto::plan::OpType::Equal, "banana"),
        [](const std::string& s) { return s == "banana"; },
        "Equal banana");
    // ACCEPTED: `LIKE '%'` lowers to PrefixMatch("") — all rows (no nulls here),
    // the empty-pattern fix exercised end-to-end.
    expect_rows(
        run(proto::plan::OpType::PrefixMatch, ""),
        [](const std::string&) { return true; },
        "PrefixMatch empty");
}

TEST(FmIndexExpressionTest, MatchCandidatesAreRecheckedAndSyntaxIsValidated) {
    const auto data = MakeLongTextZebraRows(1000);
    const auto nb = data.size();
    auto loaded = LoadSealedFMMatch(data);
    const auto& schema = loaded.schema;
    const auto field_id = loaded.varchar_id;
    const auto& segment = loaded.segment;
    auto make_typed_expr = [&](const std::string& value) {
        auto* unary =
            test::GenUnaryRangeExpr(proto::plan::OpType::Match, value);
        unary->set_allocated_column_info(test::GenColumnInfo(
            field_id.get(), proto::schema::DataType::VarChar, false, false));
        auto expr = test::GenExpr();
        expr->set_allocated_unary_range_expr(unary);
        auto parser = milvus::query::ProtoParser(schema);
        return parser.ParseExprs(*expr);
    };
    EXPECT_TRUE(
        CompiledUseIndexCursor(make_typed_expr("%ZEBRA%"), segment.get(), nb));
    EXPECT_TRUE(CompiledUseIndexCursor(
        make_typed_expr("QOP%ZEBRA"), segment.get(), nb));
    auto run = [&](const std::string& value) {
        auto typed_expr = make_typed_expr(value);
        auto node = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           typed_expr);
        return milvus::query::ExecuteQueryExpr(
            node, segment.get(), nb, MAX_TIMESTAMP);
    };
    auto expect_rows =
        [&](const BitsetType& got,
            const std::function<bool(const std::string&)>& oracle,
            const char* what) {
            for (size_t i = 0; i < nb; i++) {
                EXPECT_EQ(got[i], oracle(data[i]))
                    << what << " row " << i << " (" << data[i] << ")";
            }
        };

    expect_rows(
        run("%ZEBRA%"),
        [](const std::string& s) {
            return s.find("ZEBRA") != std::string::npos;
        },
        "Match %ZEBRA%");
    expect_rows(
        run("QOP%ZEBRA"),
        [](const std::string& s) {
            return s.rfind("QOP", 0) == 0 &&
                   s.find("ZEBRA") != std::string::npos;
        },
        "Match QOP%ZEBRA");

    // LIKE syntax is expression semantics, so reject an invalid pattern while
    // compiling the physical expression, before an empty FMINDEX candidate set
    // can skip exact recheck and silently turn the error into an empty result.
    auto invalid_expr = make_typed_expr("ABSENT\\");
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment.get(), nb, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    try {
        (void)exec::CompileExpressions(
            {invalid_expr}, &exec_context, {}, false);
        FAIL() << "expected invalid LIKE pattern to fail during compilation";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::ExprInvalid);
    }
}

class FmIndexPatternExpressionTest
    : public ::testing::TestWithParam<proto::plan::OpType> {};

INSTANTIATE_TEST_SUITE_P(FmIndex,
                         FmIndexPatternExpressionTest,
                         ::testing::Values(proto::plan::OpType::Match,
                                           proto::plan::OpType::RegexMatch));

TEST_P(FmIndexPatternExpressionTest, BatchesAndBitmap) {
    const auto op = GetParam();
    const std::string pattern =
        op == proto::plan::OpType::Match ? "QOP%ZEBRA" : "^QOP.*ZEBRA$";
    const size_t nb = 1000;
    auto rows = MakeLongTextZebraRows(nb);
    constexpr size_t kLateFalsePositive = 500;
    constexpr size_t kLateTrueMatch = 750;
    rows[kLateTrueMatch] = "QOP" + rows[kLateTrueMatch];
    std::vector<int64_t> ints(nb);
    for (size_t i = 0; i < nb; i++) {
        ints[i] = static_cast<int64_t>(i);
    }
    auto loaded = LoadSealedFMMatch(rows, {}, false, nullptr, &ints);

    auto match_expr = MakeMatchTypedExpr(
        loaded.schema, loaded.varchar_id, pattern, false, op);
    EXPECT_TRUE(CompiledUseIndexCursor(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));
    auto declined =
        MakeMatchTypedExpr(loaded.schema,
                           loaded.varchar_id,
                           op == proto::plan::OpType::Match ? "%%" : ".*",
                           false,
                           op);
    EXPECT_FALSE(CompiledUseIndexCursor(
        declined, loaded.segment.get(), static_cast<int64_t>(nb)));

    proto::plan::GenericValue match_val;
    match_val.set_string_val(pattern);
    auto match = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(loaded.varchar_id, DataType::VARCHAR), op, match_val);
    proto::plan::GenericValue int_val;
    int_val.set_int64_val(500);
    auto numeric = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(loaded.int_id, DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        int_val);
    auto conjunction = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, match, numeric);

    milvus::test::ExprBatchSizeGuard batch_size_guard(64);
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(
        conjunction, loaded.segment.get(), static_cast<int64_t>(nb)));
    auto node = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       conjunction);
    auto got = milvus::query::ExecuteQueryExpr(
        node, loaded.segment.get(), nb, MAX_TIMESTAMP);
    LikePatternMatcher matcher("QOP%ZEBRA");
    ASSERT_NE(rows[kLateFalsePositive].find("ZEBRA"), std::string::npos);
    ASSERT_EQ(rows[kLateFalsePositive].find("QOP"), std::string::npos);
    ASSERT_FALSE(matcher(rows[kLateFalsePositive]));
    ASSERT_TRUE(matcher(rows[kLateTrueMatch]));
    for (size_t i = 0; i < nb; i++) {
        const bool want = matcher(rows[i]) && ints[i] >= 500;
        EXPECT_EQ(got[i], want) << "row " << i;
    }
    EXPECT_FALSE(got[kLateFalsePositive]);
    EXPECT_TRUE(got[kLateTrueMatch]);
}

TEST_P(FmIndexPatternExpressionTest, MultiChunk) {
    const auto op = GetParam();
    const size_t nb = 300;
    std::string filler(500, 'y');
    std::vector<std::string> rows;
    rows.reserve(nb);
    for (size_t i = 0; i < nb; i++) {
        if (i % 100 == 25) {
            rows.push_back(filler + "ZEBRA");
        } else {
            rows.push_back(filler);
        }
    }
    auto chunk_of = [&](size_t begin, size_t end) {
        return test::expr_index::StringField(
            std::vector<std::string>(rows.begin() + begin, rows.begin() + end));
    };
    std::vector<FieldDataPtr> chunks{
        chunk_of(0, 100), chunk_of(100, 200), chunk_of(200, 300)};
    auto loaded = LoadSealedFMMatch(rows, chunks, false, nullptr, nullptr);
    ASSERT_EQ(loaded.segment->num_chunk_data(loaded.varchar_id), 3);

    auto match_expr = MakeMatchTypedExpr(
        loaded.schema,
        loaded.varchar_id,
        op == proto::plan::OpType::Match ? "%ZEBRA%" : "ZEBRA$",
        false,
        op);
    EXPECT_TRUE(CompiledUseIndexCursor(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));

    // FilterBits executes this scalar-index expression all at once. Hits at 25,
    // 125, and 225 therefore force one ProcessDataByOffsets call to read
    // candidates from all three raw chunks.
    auto node =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, match_expr);
    auto got = milvus::query::ExecuteQueryExpr(
        node, loaded.segment.get(), nb, MAX_TIMESTAMP);
    LikePatternMatcher matcher("%ZEBRA%");
    size_t hits = 0;
    std::array<size_t, 3> chunk_hits{};
    for (size_t i = 0; i < nb; i++) {
        const bool want = matcher(rows[i]);
        EXPECT_EQ(got[i], want) << "row " << i;
        hits += want;
        chunk_hits[i / 100] += want;
    }
    EXPECT_EQ(hits, 3u);
    EXPECT_EQ(chunk_hits, (std::array<size_t, 3>{1, 1, 1}));
    EXPECT_LT(hits, nb);
}

TEST_P(FmIndexPatternExpressionTest, NullableAndOffsets) {
    const auto op = GetParam();
    std::string filler(500, 'y');
    std::vector<std::string> rows(200, filler);
    rows[0] = filler + "ZEBRA";
    rows[50] = filler + "ZEBRA";
    rows[51] = "";
    std::vector<uint8_t> valid_bitmap((rows.size() + 7) / 8, 0);
    for (size_t i = 0; i < rows.size(); i++) {
        if (i != 51 && i != 52) {
            valid_bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 0x07));
        }
    }
    rows[52] = filler + "ZEBRA";
    auto loaded =
        LoadSealedFMMatch(rows, {}, true, valid_bitmap.data(), nullptr);
    const size_t nb = rows.size();
    auto match_expr = MakeMatchTypedExpr(
        loaded.schema,
        loaded.varchar_id,
        op == proto::plan::OpType::Match ? "%ZEBRA%" : "ZEBRA$",
        true,
        op);
    EXPECT_TRUE(CompiledUseIndexCursor(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));

    auto node =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, match_expr);
    auto full = milvus::query::ExecuteQueryExpr(
        node, loaded.segment.get(), nb, MAX_TIMESTAMP);
    LikePatternMatcher matcher("%ZEBRA%");
    for (size_t i = 0; i < nb; i++) {
        const bool valid = (valid_bitmap[i >> 3] & (1u << (i & 0x07))) != 0;
        const bool want = valid && matcher(rows[i]);
        EXPECT_EQ(full[i], want) << "row " << i;
    }

    exec::OffsetVector offsets;
    for (int32_t i = 0; i < static_cast<int32_t>(nb); i += 2) {
        offsets.push_back(i);
    }
    auto offset_res = milvus::test::gen_filter_res(
        node.get(), loaded.segment.get(), nb, MAX_TIMESTAMP, &offsets);
    ASSERT_EQ(offset_res->size(), offsets.size());
    TargetBitmapView offset_view(offset_res->GetRawData(), offsets.size());
    for (size_t j = 0; j < offsets.size(); j++) {
        const auto i = static_cast<size_t>(offsets[j]);
        EXPECT_EQ(offset_view[j], full[i]) << "offset row " << i;
    }
}

TEST(FmIndexExpressionTest, RegexExternalWordBoundaryCandidatesAndRawFallback) {
    // "bar" is absent. Before the fix, its spurious required prefix makes the
    // guard accept zero hits and FMIndex eliminates the actual matching rows.
    const std::vector<std::string> rows{"afoo", "a-foo", "other", ""};
    auto idx = BuildFmPatternReader(rows);
    for (const std::string pattern :
         {R"(\Bfoo|bar)", R"(\b-foo|bar)", R"((?:\Bfoo|bar))"}) {
        EXPECT_FALSE(
            idx->ShouldUseForOp(index::PatternOp::RegexMatch, pattern));
        const auto candidates =
            idx->PatternMatch(pattern, index::PatternOp::RegexMatch);
        EXPECT_LT(candidates.count(), rows.size());
        PartialRegexMatcher matcher(pattern);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(candidates[i] && matcher(rows[i]), matcher(rows[i]));
        }
    }

    // Exercise the physical raw-data executor both with an FMIndex that must
    // decline and on a segment without any scalar index.
    for (bool with_index : {true, false}) {
        auto loaded =
            LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, with_index);
        for (const std::string pattern :
             {R"(\Bfoo|bar)", R"(\b-foo|bar)", R"((?:\Bfoo|bar))"}) {
            auto expression = MakeMatchTypedExpr(loaded.schema,
                                                 loaded.varchar_id,
                                                 pattern,
                                                 false,
                                                 proto::plan::RegexMatch);
            EXPECT_FALSE(CompiledUseIndexCursor(
                expression, loaded.segment.get(), rows.size()));
            auto evaluated = milvus::test::EvalExprInBatches(
                expression, loaded.segment.get(), rows.size());
            TargetBitmapView result(evaluated.result->GetRawData(),
                                    rows.size());
            PartialRegexMatcher matcher(pattern);
            for (size_t i = 0; i < rows.size(); ++i) {
                EXPECT_EQ(result[i], matcher(rows[i]))
                    << "pattern=" << pattern << " row=" << i
                    << " with_index=" << with_index;
            }
        }
    }
}

TEST(FmIndexExpressionTest, RegexInteriorLiteralCandidatesAndFallback) {
    std::vector<std::string> rows(200, std::string(500, 'x') + "COMMON");
    rows[0] += "RARE123END";
    rows[1] += "RAREwrong";
    rows[2] = "A";
    rows[3] = "afoo";
    rows[4] = "bar";
    rows[5] = "xRAR";
    rows[6] = "x";
    rows[7] += "RARE123END";  // Candidate payload in a null row.
    std::vector<uint8_t> validity((rows.size() + 7) / 8, 0xff);
    validity[0] &= ~(1u << 7);
    for (bool with_index : {false, true}) {
        auto loaded = LoadSealedFMMatch(
            rows, {}, true, validity.data(), nullptr, with_index);
        for (const auto& [pattern, uses_index] :
             std::vector<std::pair<std::string, bool>>{
                 {".*RARE", true},
                 {"x.*RARE123END", true},
                 {".*RARE[0-9]+END", true},
                 {R"(.*RARE\d+END)", true},
                 {"x.*RARE123END+", true},
                 {"x.*RARE123END?", true},
                 {"x.*(RARE123END){1,2}", true},
                 {"x.*RARE(123)END", true},
                 {".*x{500}COMMONRARE123END", true},
                 {"(?s).*RARE123END", true},
                 {"x.*(?P<n>RARE123END)", true},
                 {R"(.*\QRARE123END\E)", true},
                 {"x.*(RARE123END)", true},
                 {".*(RARE123END)?COMMON", false},
                 {".*((RARE123END)+)?COMMON", false},
                 {".*RARE[[:digit:]]+END", true},
                 {"x.*RARE?", true},
                 {"x.*(RARE)?", false},
                 {"x.*RARE|bar", true},
                 {R"(.*\x41)", false},
                 {R"(\Bfoo|bar)", true}}) {
            auto expression = MakeMatchTypedExpr(loaded.schema,
                                                 loaded.varchar_id,
                                                 pattern,
                                                 true,
                                                 proto::plan::RegexMatch);
            EXPECT_EQ(CompiledUseIndexCursor(
                          expression, loaded.segment.get(), rows.size()),
                      with_index && uses_index)
                << pattern;
            auto evaluated = milvus::test::EvalExprInBatches(
                expression, loaded.segment.get(), rows.size());
            TargetBitmapView result(evaluated.result->GetRawData(),
                                    rows.size());
            TargetBitmapView valid(evaluated.result->GetValidRawData(),
                                   rows.size());
            PartialRegexMatcher matcher(pattern);
            for (size_t i = 0; i < rows.size(); ++i) {
                EXPECT_EQ(valid[i], i != 7);
                if (valid[i]) {
                    EXPECT_EQ(result[i], matcher(rows[i]))
                        << "pattern=" << pattern << " row=" << i
                        << " with_index=" << with_index;
                }
            }
        }
    }
}

TEST(FmIndexExpressionTest,
     RegexBooleanConditionsAcrossChunksBatchesNullAndNot) {
    constexpr size_t n = 384;
    std::vector<std::string> rows(n, std::string(500, 'x'));
    std::vector<uint8_t> validity(n / 8, 0xff);
    for (size_t base : {0, 128, 256}) {
        rows[base + 3] += "foo bar";
        rows[base + 4] += "baz qux";
        rows[base + 5] += "bar foo";  // candidate rejected by RE2
        rows[base + 6] += "baz qux";  // null candidate payload
        validity[(base + 6) / 8] &= ~(1u << ((base + 6) % 8));
    }
    std::vector<FieldDataPtr> chunks;
    for (size_t base : {0, 128, 256}) {
        auto chunk = test::expr_index::StringField(
            std::vector<std::string>(rows.begin() + base,
                                     rows.begin() + base + 128),
            true,
            validity.data() + base / 8);
        chunks.push_back(std::move(chunk));
    }
    auto loaded =
        LoadSealedFMMatch(rows, chunks, true, validity.data(), nullptr);
    ASSERT_EQ(loaded.segment->num_chunk_data(loaded.varchar_id), 3);
    milvus::test::ExprBatchSizeGuard batch_guard(64);
    for (const std::string pattern : {"foo.*bar|baz.*qux",
                                      "(foo|baz).*(bar|qux)",
                                      "a|foo",
                                      "(?i:foo)|baz"}) {
        auto expression = MakeMatchTypedExpr(loaded.schema,
                                             loaded.varchar_id,
                                             pattern,
                                             true,
                                             proto::plan::RegexMatch);
        PartialRegexMatcher matcher(pattern);
        for (bool negate : {false, true}) {
            expr::TypedExprPtr root = expression;
            if (negate) {
                root = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, expression);
            }
            auto query_context = std::make_shared<exec::QueryContext>(
                DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
            query_context->set_enable_expr_cache(false);
            exec::ExecContext exec_context(query_context.get());
            auto compiled =
                exec::CompileExpressions({root}, &exec_context, {}, false);
            exec::EvalCtx context(&exec_context);
            for (size_t base = 0; base < n; base += 64) {
                TargetBitmap input(64, true);
                // A nonmatching row and a true match are both excluded.
                input[0] = false;
                input[4] = false;
                auto saved = input.clone();
                context.set_bitmap_input(std::move(input));
                VectorPtr output;
                compiled[0]->Eval(context, output);
                auto result = std::dynamic_pointer_cast<ColumnVector>(output);
                ASSERT_NE(result, nullptr);
                ASSERT_EQ(result->size(), 64);
                TargetBitmapView hits(result->GetRawData(), 64);
                TargetBitmapView valid(result->GetValidRawData(), 64);
                for (size_t i = 0; i < 64; ++i) {
                    EXPECT_EQ(context.get_bitmap_input()[i], saved[i]);
                    // Bitmap-excluded rows are not consumed by the parent.
                    if (!saved[i])
                        continue;
                    const bool expected_valid =
                        validity[(base + i) / 8] & (1u << ((base + i) % 8));
                    EXPECT_EQ(valid[i], expected_valid);
                    if (expected_valid)
                        EXPECT_EQ(hits[i], negate != matcher(rows[base + i]))
                            << pattern << " row=" << base + i;
                }
            }
        }
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexFallbackNegationAndInvalidPattern) {
    std::vector<std::string> rows(200, std::string(500, 'x'));
    rows[0] = "";
    rows[1] = "Aa.";
    rows[2] = "foo\nbar";
    rows[3] = "FOO";
    rows[4] = "aab";
    rows[5] = "ééb";
    rows[6] = std::string("a\0b", 3);
    rows[7] = "foo";  // null despite the candidate-looking payload
    std::vector<uint8_t> validity((rows.size() + 7) / 8, 0xff);
    validity[0] &= ~(1u << 7);
    auto loaded = LoadSealedFMMatch(rows, {}, true, validity.data(), nullptr);
    for (const std::string pattern : {"",
                                      ".*",
                                      "foo|",
                                      "foo|bar",
                                      "(?i)foo",
                                      "foo.*bar",
                                      ".*foo",
                                      "x.*foo",
                                      R"(\x41\141\.)",
                                      "é+b",
                                      R"(a\x00b)",
                                      "x+",
                                      "ABSENT.*"}) {
        auto expression = MakeMatchTypedExpr(loaded.schema,
                                             loaded.varchar_id,
                                             pattern,
                                             true,
                                             proto::plan::RegexMatch);
        PartialRegexMatcher matcher(pattern);
        for (bool negate : {false, true}) {
            expr::TypedExprPtr root = expression;
            if (negate) {
                root = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, expression);
            }
            auto evaluated = milvus::test::EvalExprInBatches(
                root, loaded.segment.get(), rows.size());
            TargetBitmapView result(evaluated.result->GetRawData(),
                                    rows.size());
            TargetBitmapView valid(evaluated.result->GetValidRawData(),
                                   rows.size());
            for (size_t i = 0; i < rows.size(); ++i) {
                EXPECT_EQ(valid[i], i != 7) << pattern << " row=" << i;
                if (valid[i]) {
                    EXPECT_EQ(result[i], negate != matcher(rows[i]))
                        << pattern << " row=" << i;
                }
            }
        }
    }
    for (const std::string pattern : {"ABSENT[", "("}) {
        auto expression = MakeMatchTypedExpr(loaded.schema,
                                             loaded.varchar_id,
                                             pattern,
                                             true,
                                             proto::plan::RegexMatch);
        try {
            (void)CompiledUseIndexCursor(
                expression, loaded.segment.get(), rows.size());
            FAIL() << "invalid regex must fail before candidate generation";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::InvalidParameter);
        }
    }
}

TEST(FmIndexExpressionTest, RegexSealedColumnExperiment) {
    milvus::test::ExprBatchSizeGuard batch_guard(2048);
    using Clock = std::chrono::steady_clock;
    auto micros = [](Clock::time_point start) {
        return std::chrono::duration<double, std::micro>(Clock::now() - start)
            .count();
    };
    std::vector<std::string> rows(
        1000, std::string(500, 'x') + "COMMON_LONG foo qux ERROR");
    for (size_t i = 0; i < rows.size(); i += 100) {
        rows[i] += " bar RARE123END timeout";
        rows[i + 1] += " baz WARNtimeout";
        rows[i + 2] += " RAREwrong";
    }
    auto fixture = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, false);
    auto index = BuildFmPatternReader(rows);
    std::vector<std::string_view> docs(rows.begin(), rows.end());
    milvus::index::fmindex::FMIndex counts;
    counts.Build(docs, 8);
    const std::vector<std::pair<std::string, std::string>> workloads{
        {"independent", "foo.*bar"},
        {"short_rare", ".*COMMON_LONG.*RARE"},
        {"mandatory_suffix", "(ERROR|WARN).*timeout"},
        {"branches", "foo.*bar|baz.*qux"},
        {"frequent", "COMMON_LONG.*foo"},
        {"long", ".*x{500}COMMON_LONG.*RARE123END"},
        {"short_branch", "a|timeout"},
        {"folded_branch", "(?i:foo)|bar"},
        {"fallback", ".*"},
    };
    const std::string modes[]{"raw", "all", "selected"};
    const std::string stages[]{
        "prepare", "count", "lookup", "bitset", "raw_read", "recheck", "total"};
    RecordProperty("expression_result_cache",
                   "disabled: direct sealed reads; no expression cache");
    RecordProperty("rows", rows.size());
    RecordProperty("samples", 3);
    for (const auto& [name, pattern] : workloads) {
        PartialRegexMatcher oracle(pattern);
        for (bool sparse : {false, true}) {
            TargetBitmap input(rows.size(), !sparse);
            if (sparse) {
                for (size_t i = 0; i < rows.size(); i += 100) input.set(i);
            }
            for (bool cold : {false, true}) {
                // The old policy must be measured from a frozen old binary;
                // using RequiredLiteral() here would silently upgrade it too.
                auto prepare = [&](int mode) {
                    return mode == 0
                               ? RegexLiteralCondition{}
                               : PartialRegexMatcher::PrepareIndexCondition(
                                     pattern);
                };
                std::array<std::array<std::vector<double>, 7>, 3> samples;
                for (int round = 0; round < 4; ++round) {
                    for (int turn = 0; turn < 3; ++turn) {
                        const int mode = (round + turn) % 3;
                        if (cold) {
                            PartialRegexMatcher::PrepareIndexCondition(
                                "__evict__");
                        } else {
                            prepare(mode);
                        }
                        std::array<double, 7> elapsed{};
                        const auto total_start = Clock::now();
                        auto start = Clock::now();
                        PartialRegexMatcher matcher(pattern);
                        auto condition = prepare(mode);
                        const auto raw_literal = mode == 0
                                                     ? matcher.RequiredLiteral()
                                                     : std::string{};
                        std::unique_ptr<VolnitskySearcher> raw_searcher;
                        if (!raw_literal.empty()) {
                            raw_searcher = std::make_unique<VolnitskySearcher>(
                                raw_literal);
                        }
                        elapsed[0] = micros(start);
                        start = Clock::now();
                        double cost = 0;
                        auto count = [&](const std::string& atom) {
                            return counts.Count(
                                reinterpret_cast<const uint8_t*>(atom.data()),
                                atom.size());
                        };
                        if (mode == 2) {
                            condition = condition.Select(count, cost);
                        }
                        elapsed[1] = micros(start);
                        start = Clock::now();
                        auto candidates = condition.Evaluate(
                            [&](const std::string& atom) {
                                auto lookup_start = Clock::now();
                                TargetBitmap hits = index->PatternMatch(
                                    atom, index::PatternOp::InnerMatch);
                                elapsed[2] += micros(lookup_start);
                                return hits;
                            },
                            [&] { return TargetBitmap(rows.size(), true); });
                        candidates &= input;
                        elapsed[3] = std::max(0.0, micros(start) - elapsed[2]);
                        start = Clock::now();
                        using PinnedStrings =
                            decltype(fixture.segment->chunk_views_by_offsets<
                                     std::string_view>(
                                nullptr,
                                fixture.varchar_id,
                                0,
                                std::declval<const FixedVector<int32_t>&>()));
                        std::vector<PinnedStrings> pins;
                        std::vector<size_t> selected_rows;
                        FixedVector<int32_t> local_offsets;
                        int64_t current_chunk = -1;
                        auto flush = [&] {
                            if (!local_offsets.empty()) {
                                pins.push_back(
                                    fixture.segment->chunk_views_by_offsets<
                                        std::string_view>(nullptr,
                                                          fixture.varchar_id,
                                                          current_chunk,
                                                          local_offsets));
                                local_offsets.clear();
                            }
                        };
                        for (size_t row = 0; row < rows.size(); ++row) {
                            if (!candidates[row])
                                continue;
                            auto [chunk, offset] =
                                fixture.segment->get_chunk_by_offset(
                                    fixture.varchar_id, row);
                            if (chunk != current_chunk) {
                                flush();
                                current_chunk = chunk;
                            }
                            local_offsets.push_back(offset);
                            selected_rows.push_back(row);
                        }
                        flush();
                        elapsed[4] = micros(start);
                        start = Clock::now();
                        size_t checked = 0;
                        for (auto& pin : pins) {
                            const auto& [values, valid] = pin.get();
                            for (size_t j = 0; j < values.size(); ++j) {
                                candidates[selected_rows[checked++]] =
                                    (valid.empty() || valid[j]) &&
                                    (!raw_searcher ||
                                     raw_searcher->contains(values[j])) &&
                                    matcher(values[j]);
                            }
                        }
                        elapsed[5] = micros(start);
                        elapsed[6] = micros(total_start);
                        ASSERT_EQ(checked, selected_rows.size());
                        // Dense selective workloads must read/recheck fewer
                        // sealed rows in both candidate modes than a raw scan.
                        if (!sparse && mode != 0 &&
                            (name == "independent" || name == "short_rare" ||
                             name == "mandatory_suffix" || name == "branches" ||
                             name == "long")) {
                            EXPECT_GT(checked, 0u) << name;
                            EXPECT_LT(checked, rows.size())
                                << name << " " << modes[mode];
                        }
                        for (size_t row = 0; row < rows.size(); ++row) {
                            ASSERT_EQ(candidates[row],
                                      input[row] && oracle(rows[row]))
                                << name << " " << modes[mode] << " row=" << row;
                        }
                        if (round > 0) {
                            for (size_t stage = 0; stage < elapsed.size();
                                 ++stage)
                                samples[mode][stage].push_back(elapsed[stage]);
                        }
                    }
                }
                for (int mode = 0; mode < 3; ++mode) {
                    for (size_t stage = 0; stage < 7; ++stage) {
                        auto& values = samples[mode][stage];
                        std::sort(values.begin(), values.end());
                        RecordProperty(name + "_" + modes[mode] +
                                           (sparse ? "_sparse" : "_dense") +
                                           (cold ? "_cold_" : "_warm_") +
                                           stages[stage] + "_us",
                                       std::to_string(values[1]));
                    }
                }
            }
        }
    }
    // Exercise production expression dispatch with mixed patterns. Every query
    // owns fresh physical state and explicitly disables expression caching.
    auto indexed = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr);
    for (bool sparse : {false, true}) {
        for (bool with_index : {false, true}) {
            const auto& loaded = with_index ? indexed : fixture;
            std::vector<double> samples;
            for (int round = 0; round < 4; ++round) {
                double total = 0;
                for (const auto& [name, pattern] : workloads) {
                    auto expression =
                        MakeMatchTypedExpr(loaded.schema,
                                           loaded.varchar_id,
                                           pattern,
                                           false,
                                           proto::plan::RegexMatch);
                    TargetBitmap input(rows.size(), !sparse);
                    if (sparse) {
                        for (size_t i = 0; i < rows.size(); i += 100)
                            input.set(i);
                    }
                    const auto start = Clock::now();
                    auto query_context = std::make_shared<exec::QueryContext>(
                        DEAFULT_QUERY_ID,
                        loaded.segment.get(),
                        rows.size(),
                        MAX_TIMESTAMP);
                    query_context->set_enable_expr_cache(false);
                    exec::ExecContext exec_context(query_context.get());
                    auto compiled = exec::CompileExpressions(
                        {expression}, &exec_context, {}, false);
                    exec::EvalCtx context(&exec_context);
                    context.set_bitmap_input(input.clone());
                    VectorPtr output;
                    compiled[0]->Eval(context, output);
                    total += micros(start);
                    auto result =
                        std::dynamic_pointer_cast<ColumnVector>(output);
                    ASSERT_NE(result, nullptr);
                    ASSERT_EQ(result->size(), rows.size());
                    TargetBitmapView hits(result->GetRawData(), rows.size());
                    PartialRegexMatcher oracle(pattern);
                    for (size_t row = 0; row < rows.size(); ++row) {
                        if (input[row])
                            EXPECT_EQ(hits[row], oracle(rows[row]))
                                << name << " row=" << row;
                    }
                }
                if (round > 0)
                    samples.push_back(total / workloads.size());
            }
            std::sort(samples.begin(), samples.end());
            RecordProperty(std::string("mixed_") +
                               (sparse ? "sparse_" : "dense_") +
                               (with_index ? "indexed_us" : "scan_us"),
                           std::to_string(samples[1]));
        }
    }
}

TEST(FmIndexExpressionTest, DISABLED_RegexEndToEndBenchmark) {
    std::vector<std::string> rows(20000, std::string(500, 'x'));
    for (size_t i = 0; i < rows.size(); ++i) {
        rows[i] += "COMMON";
        if (i % 1000 == 0) {
            rows[i] += "RARE123END";
        } else if (i % 1000 == 1) {
            rows[i] = "RAREwrong" + rows[i];  // rejected by exact recheck
        }
    }
    auto indexed = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr);
    auto scan = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, false);
    auto candidate_index = BuildFmPatternReader(rows);
    const std::vector<std::pair<std::string, std::string>> workloads{
        {"selective", R"(RARE\d+END)"},
        {"unselective", "COMMON.*"},
        {"interior", ".*RARE"},
        {"common_prefix", "x.*RARE123END"},
        {"long_literal", ".*x{500}COMMONRARE123END"},
        {"rare_prefix", "RARE.*COMMON"},
        {"mandatory_suffix", "(COMMON|WARN).*RARE"},
        {"or_branches", "COMMON.*RARE123END|RAREwrong.*COMMON"},
        {"no_literal", ".*"},
        {"nullable_long", "(?:" + std::string(3000, 'x') + ")?"},
        {"empty_branch", "RARE|"},
        {"zero_hits", "ABSENT.*"},
    };
    // ExecuteQueryExpr constructs a fresh QueryContext with expression-result
    // caching disabled (QueryContext's default); no result cache is reused.
    RecordProperty("expression_result_cache", "disabled");
    RecordProperty("rows", static_cast<int>(rows.size()));
    RecordProperty("sa_sample_rate", 8);
    RecordProperty(
        "fmindex_cost_ratio",
        std::to_string(
            segcore::SegcoreConfig::default_config().get_fmindex_cost_ratio()));
    for (const auto& [name, pattern] : workloads) {
        PartialRegexMatcher matcher(pattern);
        const auto candidates = candidate_index->PatternMatch(
            pattern, index::PatternOp::RegexMatch);
        RecordProperty(name + "_candidates",
                       static_cast<int>(candidates.count()));
        auto measure = [&](const SealedFMMatch& fixture, bool cold = false) {
            auto expression = MakeMatchTypedExpr(fixture.schema,
                                                 fixture.varchar_id,
                                                 pattern,
                                                 false,
                                                 proto::plan::RegexMatch);
            auto node = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expression);
            std::vector<double> times;
            for (int iteration = 0; iteration < 8; ++iteration) {
                if (cold) {
                    PartialRegexMatcher::PrepareIndexCondition(
                        "__evict_benchmark_cache__");
                }
                const auto start = std::chrono::steady_clock::now();
                auto got = milvus::query::ExecuteQueryExpr(
                    node, fixture.segment.get(), rows.size(), MAX_TIMESTAMP);
                const auto elapsed =
                    std::chrono::duration<double, std::micro>(
                        std::chrono::steady_clock::now() - start)
                        .count();
                for (size_t i = 0; i < rows.size(); ++i) {
                    EXPECT_EQ(got[i], matcher(rows[i])) << name << " row=" << i;
                }
                if (iteration != 0) {
                    times.push_back(elapsed);
                }
            }
            std::sort(times.begin(), times.end());
            return times[times.size() / 2];
        };
        auto expression = MakeMatchTypedExpr(indexed.schema,
                                             indexed.varchar_id,
                                             pattern,
                                             false,
                                             proto::plan::RegexMatch);
        RecordProperty(name + "_uses_index",
                       CompiledUseIndexCursor(
                           expression, indexed.segment.get(), rows.size()));
        RecordProperty(name + "_scan_us", std::to_string(measure(scan)));
        RecordProperty(name + "_indexed_us", std::to_string(measure(indexed)));
        RecordProperty(name + "_indexed_cold_us",
                       std::to_string(measure(indexed, true)));
    }
}

TEST(FmIndexExpressionTest, NullableFieldNullPredicatesUseScalarIndex) {
    const std::vector<std::string> rows{"abc", "ignored", "", "zzz"};
    const uint8_t validity[] = {0x0D};
    auto fixture = LoadSealedFMMatch(rows, {}, true, validity);
    for (const auto op : {proto::plan::NullExpr_NullOp_IsNull,
                          proto::plan::NullExpr_NullOp_IsNotNull}) {
        auto filter = std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(fixture.varchar_id, DataType::VARCHAR, {}, true),
            op);
        auto query_context =
            std::make_shared<exec::QueryContext>(DEAFULT_QUERY_ID,
                                                 fixture.segment.get(),
                                                 rows.size(),
                                                 MAX_TIMESTAMP);
        exec::ExecContext exec_context(query_context.get());
        auto compiled =
            exec::CompileExpressions({filter}, &exec_context, {}, false);
        ASSERT_EQ(compiled.size(), 1);
        auto* segment_expr =
            dynamic_cast<exec::SegmentExpr*>(compiled[0].get());
        ASSERT_NE(segment_expr, nullptr);
        EXPECT_EQ(segment_expr->GetExecPath(), exec::ExprExecPath::ScalarIndex);
        auto evaluation =
            test::EvalExprInBatches(filter, fixture.segment.get(), rows.size());
        const auto& result = evaluation.result;
        TargetBitmapView bits(result->GetRawData(), result->size());
        TargetBitmapView valid(result->GetValidRawData(), result->size());
        for (size_t row = 0; row < rows.size(); ++row) {
            EXPECT_TRUE(valid[row]);
            EXPECT_EQ(bits[row],
                      op == proto::plan::NullExpr_NullOp_IsNull ? row == 1
                                                                : row != 1);
        }
    }
}
