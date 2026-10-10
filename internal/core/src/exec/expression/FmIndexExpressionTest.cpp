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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/RegexQuery.h"
#include "common/Schema.h"
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
                   bool nullable) {
    auto* unary = test::GenUnaryRangeExpr(proto::plan::OpType::Match, pattern);
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
                  const std::vector<int64_t>* ints = nullptr) {
    SealedFMMatch out;
    out.raw_files = std::make_unique<test::expr_index::RawFieldFiles>();
    out.schema = std::make_shared<Schema>();
    out.varchar_id = out.schema->AddDebugField(
        "fm_match", DataType::VARCHAR, nullable);
    if (ints != nullptr) {
        out.int_id = out.schema->AddDebugField("fm_int", DataType::INT64);
    }
    out.segment = segcore::CreateSealedSegment(out.schema);
    auto chunks = varchar_chunks;
    if (chunks.empty()) {
        chunks.push_back(test::expr_index::StringField(rows, nullable,
                                                       valid_bitmap));
    }
    auto raw_info = out.raw_files->Prepare(out.varchar_id, chunks);
    out.segment->LoadFieldData(raw_info);
    if (ints != nullptr) {
        auto field = std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        field->FillFieldData(ints->data(), ints->size());
        auto info = out.raw_files->Prepare(out.int_id, {field});
        out.segment->LoadFieldData(info);
    }
    auto opened = test::expr_index::BuildIndex(
        out.varchar_id, DataType::VARCHAR, index::FMINDEX_INDEX_TYPE, chunks,
        {{index::FM_SA_SAMPLE_RATE, 8}}, DataType::NONE, false, true);
    test::expr_index::InstallIndex(*out.segment, out.varchar_id,
                                   DataType::VARCHAR, std::move(opened));
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
    EXPECT_TRUE(CompiledUseIndexCursor(
        make_typed_expr("%ZEBRA%"), segment.get(), nb));
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

TEST(FmIndexExpressionTest, RecheckHonorsBatchesAndConjunctionBitmap) {
    const size_t nb = 1000;
    auto rows = MakeLongTextZebraRows(nb);
    constexpr size_t kLateFalsePositive = 500;
    constexpr size_t kLateTrueMatch = 750;
    rows[kLateTrueMatch] = "QOP" + rows[kLateTrueMatch];
    std::vector<int64_t> ints(nb);
    for (size_t i = 0; i < nb; i++) {
        ints[i] = static_cast<int64_t>(i);
    }
    auto loaded =
        LoadSealedFMMatch(rows, {}, false, nullptr, &ints);

    auto match_expr = MakeMatchTypedExpr(
        loaded.schema, loaded.varchar_id, "QOP%ZEBRA", false);
    EXPECT_TRUE(CompiledUseIndexCursor(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));
    auto declined =
        MakeMatchTypedExpr(loaded.schema, loaded.varchar_id, "%%", false);
    EXPECT_FALSE(CompiledUseIndexCursor(
        declined, loaded.segment.get(), static_cast<int64_t>(nb)));

    proto::plan::GenericValue match_val;
    match_val.set_string_val("QOP%ZEBRA");
    auto match = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(loaded.varchar_id, DataType::VARCHAR),
        proto::plan::OpType::Match,
        match_val);
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

TEST(FmIndexExpressionTest, RecheckVisitsCandidatesAcrossRawChunks) {
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
    auto loaded = LoadSealedFMMatch(
        rows, chunks, false, nullptr, nullptr);
    ASSERT_EQ(loaded.segment->num_chunk_data(loaded.varchar_id), 3);

    auto match_expr =
        MakeMatchTypedExpr(loaded.schema, loaded.varchar_id, "%ZEBRA%", false);
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

TEST(FmIndexExpressionTest, RecheckPreservesNullsAndSelectedOffsets) {
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
    auto loaded = LoadSealedFMMatch(
        rows, {}, true, valid_bitmap.data(), nullptr);
    const size_t nb = rows.size();
    auto match_expr =
        MakeMatchTypedExpr(loaded.schema, loaded.varchar_id, "%ZEBRA%", true);
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
