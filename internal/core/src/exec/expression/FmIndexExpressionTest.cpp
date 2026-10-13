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
#include <atomic>
#include <chrono>
#include <utility>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "common/RegexQuery.h"
#include "common/RegexIndexPlan.h"
#include "common/Schema.h"
#include "common/Volnitsky.h"
#include "index/scalar/fmindex/FmIndexReader.h"
#include "index/fmindex/FMIndex.h"
#include "exec/QueryContext.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "exec/expression/RawExprCacheAdapter.h"
#include "exec/expression/UnaryExpr.h"
#include "folly/executors/CPUThreadPoolExecutor.h"
#include "expr/ITypeExpr.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"

using namespace milvus;

namespace {

using FMReaderDecorator =
    std::function<index::IIndexReaderBasePtr(index::IIndexReaderBasePtr)>;

struct SealedFMMatch {
    SchemaPtr schema;
    FieldId varchar_id;
    FieldId int_id;
    std::unique_ptr<test::expr_index::RawFieldFiles> raw_files;
    segcore::SegmentSealedUPtr segment;
    double reader_cost_ratio = 0.001;
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
                  bool with_index = true,
                  FMReaderDecorator decorate_reader = {},
                  std::optional<double> reader_cost_ratio = std::nullopt) {
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
    if (reader_cost_ratio.has_value()) {
        // Explicit construction-time injection for policy tests. The loader's
        // production default does not follow later global config mutations.
        auto engine = std::make_shared<index::fmindex::FMIndex>();
        std::vector<std::string_view> docs(rows.begin(), rows.end());
        TargetBitmap nulls(rows.size(), false);
        for (size_t row = 0; row < rows.size(); ++row) {
            if (nullable && !(valid_bitmap[row / 8] & (1u << (row % 8)))) {
                nulls.set(row);
                docs[row] = {};
            }
        }
        engine->Build(docs, 8);
        auto storage =
            index::FmIndexStorage::Create(nullptr,
                                          std::move(engine),
                                          std::move(nulls),
                                          rows.size(),
                                          DataType::VARCHAR,
                                          nullable,
                                          index::FmIndexStateOrigin::Builder);
        opened.reader = std::make_unique<index::FmIndexReader>(
            std::move(storage), *reader_cost_ratio);
        out.reader_cost_ratio = *reader_cost_ratio;
    }
    if (decorate_reader) {
        opened.reader = decorate_reader(std::move(opened.reader));
    }
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

struct RegexRoutingCounts {
    size_t prepare = 0;
    size_t execute = 0;
    size_t legacy_guard = 0;
    size_t legacy_pattern = 0;
    std::shared_ptr<const RegexIndexPlan> plan;
};

// FmIndexReader remains final. This untimed observer retains the production
// reader and forwards all metadata and query mixins used by the consumer.
class CountingFMReader final : public index::IIndexReaderBase,
                               public index::IPatternMatchReader,
                               public index::INullReader {
 public:
    CountingFMReader(index::IIndexReaderBasePtr reader,
                     std::shared_ptr<RegexRoutingCounts> counts,
                     double preparation_floor)
        : reader_(std::move(reader)),
          pattern_(
              dynamic_cast<const index::IPatternMatchReader*>(reader_.get())),
          nulls_(dynamic_cast<const index::INullReader*>(reader_.get())),
          counts_(std::move(counts)),
          preparation_floor_(preparation_floor) {
        AssertInfo(pattern_ != nullptr && nulls_ != nullptr,
                   "FM reader observer requires pattern and null contracts");
    }

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }
    TargetBitmap
    IsNull() const override {
        return nulls_->IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        return nulls_->IsNotNull();
    }
    RegexIndexCostModel
    GetRegexIndexCostModel() const override {
        auto model = pattern_->GetRegexIndexCostModel();
        model.preparation_floor_bytes = preparation_floor_;
        return model;
    }
    std::shared_ptr<const RegexIndexPlan>
    PrepareRegexIndexPlan(
        const RegexLiteralCondition& requirements) const override {
        ++counts_->prepare;
        counts_->plan = pattern_->PrepareRegexIndexPlan(requirements);
        return counts_->plan;
    }
    TargetBitmap
    ExecuteRegexIndexPlan(const RegexIndexPlan& plan) const override {
        ++counts_->execute;
        return pattern_->ExecuteRegexIndexPlan(plan);
    }
    bool
    ShouldUseForOp(index::PatternOp op,
                   std::string_view pattern) const override {
        counts_->legacy_guard += op == index::PatternOp::RegexMatch;
        return pattern_->ShouldUseForOp(op, pattern);
    }
    bool
    PatternMatchIsExact(index::PatternOp op) const override {
        return pattern_->PatternMatchIsExact(op);
    }
    TargetBitmap
    PatternMatch(std::string_view pattern, index::PatternOp op) const override {
        counts_->legacy_pattern += op == index::PatternOp::RegexMatch;
        return pattern_->PatternMatch(pattern, op);
    }

 private:
    index::IIndexReaderBasePtr reader_;
    const index::IPatternMatchReader* pattern_;
    const index::INullReader* nulls_;
    std::shared_ptr<RegexRoutingCounts> counts_;
    double preparation_floor_;
};

FMReaderDecorator
ObserveFMReader(
    std::shared_ptr<RegexRoutingCounts> counts,
    double preparation_floor = RegexIndexCostModel{}.preparation_floor_bytes) {
    return [counts = std::move(counts),
            preparation_floor](index::IIndexReaderBasePtr reader) {
        return std::make_unique<CountingFMReader>(
            std::move(reader), counts, preparation_floor);
    };
}

class PrefetchObservingUnaryExpr : public exec::PhyUnaryRangeFilterExpr {
 public:
    using PhyUnaryRangeFilterExpr::PhyUnaryRangeFilterExpr;

    size_t
    RawPrefetchCalls() const {
        return raw_prefetch_calls_.load(std::memory_order_relaxed);
    }

 protected:
    void
    PrefetchRawData() override {
        raw_prefetch_calls_.fetch_add(1, std::memory_order_relaxed);
        exec::SegmentExpr::PrefetchRawData();
    }

 private:
    std::atomic<size_t> raw_prefetch_calls_{0};
};

std::vector<FieldDataPtr>
MakeNullableStringChunks(const std::vector<std::string>& rows,
                         const std::vector<uint8_t>& validity,
                         const std::vector<size_t>& boundaries) {
    std::vector<FieldDataPtr> chunks;
    for (size_t part = 1; part < boundaries.size(); ++part) {
        const size_t begin = boundaries[part - 1];
        const size_t length = boundaries[part] - begin;
        std::vector<uint8_t> local((length + 7) / 8, 0);
        for (size_t i = 0; i < length; ++i) {
            if (validity[(begin + i) / 8] & (1u << ((begin + i) % 8))) {
                local[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
            }
        }
        chunks.push_back(test::expr_index::StringField(
            std::vector<std::string>(rows.begin() + begin,
                                     rows.begin() + begin + length),
            true,
            local.data()));
    }
    return chunks;
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
    EXPECT_EQ(milvus::test::CanExprExecuteAllAtOnce(
                  match_expr, loaded.segment.get(), static_cast<int64_t>(nb)),
              op != proto::plan::OpType::RegexMatch);

    // LIKE keeps all-at-once execution; regex retains ordinary batches so
    // upstream bitmaps can inform routing. Both paths read candidates from
    // all three raw chunks.
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
    std::vector<std::string> rows(512, filler);
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
    // Enough scan work to exercise FM preparation, including after adding
    // the small-window gate. Preserve real candidate/recheck coverage.
    std::vector<std::string> rows(512, std::string(500, 'x') + "COMMON");
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
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls);
    for (bool with_index : {false, true}) {
        auto loaded = LoadSealedFMMatch(
            rows, {}, true, validity.data(), nullptr, with_index, factory);
        for (const auto& [pattern, initially_eligible, executes_candidates] :
             std::vector<std::tuple<std::string, bool, bool>>{
                 {".*RARE", true, true},
                 {"x.*RARE123END", true, true},
                 {".*RARE[0-9]+END", true, true},
                 {R"(.*RARE\d+END)", true, true},
                 {"x.*RARE123END+", true, true},
                 {"x.*RARE123END?", true, true},
                 {"x.*(RARE123END){1,2}", true, true},
                 {"x.*RARE(123)END", true, true},
                 {".*x{500}COMMONRARE123END", true, true},
                 {"(?s).*RARE123END", true, true},
                 {"x.*(?P<n>RARE123END)", true, true},
                 {R"(.*\QRARE123END\E)", true, true},
                 {"x.*(RARE123END)", true, true},
                 // COMMON is useful syntax, but Count makes Locate expensive.
                 {".*(RARE123END)?COMMON", true, false},
                 {".*((RARE123END)+)?COMMON", true, false},
                 {".*RARE[[:digit:]]+END", true, true},
                 {"x.*RARE?", true, true},
                 {"x.*(RARE)?", false, false},
                 {"x.*RARE|bar", true, true},
                 {R"(.*\x41)", false, false},
                 {R"(\Bfoo|bar)", true, true}}) {
            *calls = RegexRoutingCounts{};
            auto expression = MakeMatchTypedExpr(loaded.schema,
                                                 loaded.varchar_id,
                                                 pattern,
                                                 true,
                                                 proto::plan::RegexMatch);
            EXPECT_EQ(CompiledUseIndexCursor(
                          expression, loaded.segment.get(), rows.size()),
                      with_index && initially_eligible)
                << pattern;
            EXPECT_EQ(calls->prepare, 0u) << pattern;
            EXPECT_EQ(calls->execute, 0u) << pattern;
            auto evaluated = milvus::test::EvalExprInBatches(
                expression, loaded.segment.get(), rows.size());
            EXPECT_EQ(calls->prepare,
                      with_index && initially_eligible ? 1u : 0u)
                << pattern;
            EXPECT_EQ(calls->execute,
                      with_index && executes_candidates ? 1u : 0u)
                << pattern;
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

TEST(FmIndexExpressionTest, RegexGuardAndFallback) {
    std::vector<std::string> rows(1000, std::string(500, 'x'));
    rows[500] += "RARE123";
    auto idx = MakeRawDataIndex(rows);
    EXPECT_TRUE(idx->ShouldUseOp(proto::plan::RegexMatch, R"(RARE\d+)"));
    EXPECT_TRUE(idx->ShouldUseOp(proto::plan::RegexMatch, "ABSENT.*"));
    EXPECT_TRUE(idx->ShouldUseOp(proto::plan::RegexMatch, ".*RARE"));
    EXPECT_TRUE(idx->ShouldUseOp(proto::plan::RegexMatch, "x.*RARE123"));
    EXPECT_EQ(idx->PatternMatch("x.*RARE123", proto::plan::RegexMatch).count(),
              1);
    for (const std::string pattern : {"x+", ".*", "", "RARE|", "(?i)rare"}) {
        EXPECT_FALSE(idx->ShouldUseOp(proto::plan::RegexMatch, pattern))
            << pattern;
    }
    EXPECT_FALSE(
        idx->ShouldUseOp(proto::plan::RegexMatch, std::string(4097, 'a')));
}

TEST(FmIndexExpressionTest, RegexCandidatesKeepRarePrefixWhenInteriorIsCommon) {
    std::vector<std::string> rows(1000, std::string(500, 'x') + "COMMON");
    rows[500] = "RARE" + rows[500];
    auto idx = MakeRawDataIndex(rows);
    EXPECT_TRUE(idx->ShouldUseOp(proto::plan::RegexMatch, "RARE.*COMMON"));
    const auto candidates =
        idx->PatternMatch("RARE.*COMMON", proto::plan::RegexMatch);
    EXPECT_EQ(candidates.count(), 1);
    EXPECT_TRUE(candidates[500]);
}

TEST(FmIndexExpressionTest, RegexPreparationDoesNotCacheIndexStatistics) {
    std::vector<std::string> first_rows(1000, std::string(500, 'x') + "bar");
    first_rows[7] = "foo" + first_rows[7];
    std::vector<std::string> second_rows(1000, "foo" + std::string(500, 'x'));
    second_rows[9] += "bar";
    auto first = MakeRawDataIndex(first_rows);
    auto second = MakeRawDataIndex(second_rows);
    for (int repeat = 0; repeat < 2; ++repeat) {
        EXPECT_TRUE(first->ShouldUseOp(proto::plan::RegexMatch, "foo.*bar"));
        EXPECT_TRUE(second->ShouldUseOp(proto::plan::RegexMatch, "foo.*bar"));
        // The same cached requirements choose different rarest fragments.
        const auto first_candidates =
            first->PatternMatch("foo.*bar", proto::plan::RegexMatch);
        const auto second_candidates =
            second->PatternMatch("foo.*bar", proto::plan::RegexMatch);
        EXPECT_EQ(first_candidates.count(), 1);
        EXPECT_TRUE(first_candidates[7]);
        EXPECT_EQ(second_candidates.count(), 1);
        EXPECT_TRUE(second_candidates[9]);
    }
}

TEST(FmIndexExpressionTest, RegexBooleanConditionsUseCurrentCounts) {
    std::vector<std::string> rows(
        1000, std::string(500, 'x') + "COMMON_LONG foo qux ERROR");
    rows[9] += "RARE bar timeout";
    rows[19] += "baz";
    auto index = MakeRawDataIndex(rows);
    for (const auto& [pattern, expected] :
         std::vector<std::pair<std::string, size_t>>{
             {".*COMMON_LONG.*RARE", 1},
             {"(ERROR|WARN).*timeout", 1},
             {"foo.*bar|baz.*qux", 2},
             {"foo.*ABSENT|baz.*qux", 1}}) {
        EXPECT_TRUE(index->ShouldUseOp(proto::plan::RegexMatch, pattern));
        auto candidates = index->PatternMatch(pattern, proto::plan::RegexMatch);
        EXPECT_EQ(candidates.count(), expected) << pattern;
        PartialRegexMatcher matcher(pattern);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(candidates[i] && matcher(rows[i]), matcher(rows[i]))
                << pattern;
        }
    }
    for (const std::string pattern :
         {"a|timeout", "(?i:foo)|timeout", "(foo)?|timeout"}) {
        EXPECT_FALSE(index->ShouldUseOp(proto::plan::RegexMatch, pattern));
        EXPECT_EQ(index->PatternMatch(pattern, proto::plan::RegexMatch).count(),
                  rows.size());
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
    RecordProperty("fmindex_cost_ratio",
                   std::to_string(indexed.reader_cost_ratio));
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

TEST(FmIndexExpressionTest,
     ExecutorRegexPrefilterRoutesAndKeepsBatchSemantics) {
    constexpr size_t n = 1027;
    constexpr size_t batch_size = 128;
    std::vector<std::string> rows(n, "COMMON foo " + std::string(1000, 'x'));
    for (size_t i = 0; i < n; i += 101) {
        if ((i / 101) % 2 == 0) {
            rows[i] += " RARE123END";
        } else {
            // This is a necessary-literal candidate, but the order is wrong.
            rows[i] = "RARE123END " + rows[i];
        }
    }
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    for (size_t i : {size_t{0}, size_t{404}, size_t{1026}}) {
        validity[i / 8] &= static_cast<uint8_t>(~(1u << (i % 8)));
    }
    auto chunks = MakeNullableStringChunks(rows, validity, {0, 199, 540, n});
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls, 128 * 1024);
    auto loaded = LoadSealedFMMatch(
        rows, chunks, true, validity.data(), nullptr, true, factory);
    milvus::test::ExprBatchSizeGuard batch_guard(batch_size);
    const std::vector<std::pair<std::string, std::vector<int>>> schedules{
        {"absent", {-1}},
        {"zero", {0}},
        {"one_percent", {1}},
        {"ten_percent", {10}},
        {"dense", {100}},
        {"sparse_then_dense", {0, 1, 100}},
        {"dense_then_sparse", {100, 1, 0}},
    };
    for (const std::string pattern : {"foo.*RARE123END",
                                      "foo.*RARE123END|ABSENT_54191.*",
                                      "COMMON.*foo",
                                      ".*",
                                      "a|RARE123END",
                                      "ABSENT_54191.*"}) {
        auto expression = MakeMatchTypedExpr(loaded.schema,
                                             loaded.varchar_id,
                                             pattern,
                                             true,
                                             proto::plan::RegexMatch);
        PartialRegexMatcher oracle(pattern);
        for (const auto& [name, densities] : schedules) {
            for (bool negate : {false, true}) {
                SCOPED_TRACE(pattern + "/" + name +
                             (negate ? "/not" : "/positive"));
                *calls = RegexRoutingCounts{};
                expr::TypedExprPtr root = expression;
                if (negate) {
                    root = std::make_shared<expr::LogicalUnaryExpr>(
                        expr::LogicalUnaryExpr::OpType::LogicalNot, root);
                }
                auto query_context = std::make_shared<exec::QueryContext>(
                    DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
                query_context->set_enable_expr_cache(false);
                exec::ExecContext exec_context(query_context.get());
                auto compiled =
                    exec::CompileExpressions({root}, &exec_context, {}, false);
                ASSERT_FALSE(compiled[0]->CanExecuteAllAtOnce());
                exec::EvalCtx context(&exec_context);
                for (size_t begin = 0, part = 0; begin < n;
                     begin += batch_size, ++part) {
                    const size_t size = std::min(batch_size, n - begin);
                    const int density =
                        densities[std::min(part, densities.size() - 1)];
                    TargetBitmap input;
                    if (density >= 0) {
                        input = TargetBitmap(size, density == 100);
                        if (density > 0 && density < 100) {
                            for (size_t i = 0; i < size; i += 100 / density) {
                                input.set(i);
                            }
                        }
                    }
                    auto saved = input.clone();
                    context.set_bitmap_input(std::move(input));
                    VectorPtr output;
                    compiled[0]->Eval(context, output);
                    auto result =
                        std::dynamic_pointer_cast<ColumnVector>(output);
                    ASSERT_NE(result, nullptr);
                    ASSERT_EQ(result->size(), size);
                    TargetBitmapView hits(result->GetRawData(), size);
                    TargetBitmapView valid(result->GetValidRawData(), size);
                    for (size_t i = 0; i < size; ++i) {
                        if (!saved.empty()) {
                            EXPECT_EQ(context.get_bitmap_input()[i], saved[i]);
                            if (!saved[i]) {
                                continue;
                            }
                        }
                        const size_t row = begin + i;
                        const bool expected_valid =
                            validity[row / 8] & (1u << (row % 8));
                        EXPECT_EQ(valid[i], expected_valid) << "row=" << row;
                        if (expected_valid) {
                            EXPECT_EQ(hits[i], negate != oracle(rows[row]))
                                << "row=" << row;
                        }
                    }
                    EXPECT_LE(calls->prepare, 1u);
                    EXPECT_LE(calls->execute, 1u);
                    if (name == "sparse_then_dense" && part < 2) {
                        EXPECT_EQ(calls->prepare, 0u);
                        EXPECT_EQ(calls->execute, 0u);
                    }
                }
                // nullptr means actual exhaustion, never a rejected route.
                if (!negate) {
                    VectorPtr exhausted;
                    compiled[0]->Eval(context, exhausted);
                    EXPECT_EQ(exhausted, nullptr);
                }
                EXPECT_EQ(calls->legacy_guard, 0u);
                EXPECT_EQ(calls->legacy_pattern, 0u);
                if (name == "zero" || name == "one_percent") {
                    EXPECT_EQ(calls->prepare, 0u);
                    EXPECT_EQ(calls->execute, 0u);
                } else if (pattern == ".*" || pattern == "a|RARE123END") {
                    EXPECT_EQ(calls->prepare, 0u);
                    EXPECT_EQ(calls->execute, 0u);
                } else {
                    EXPECT_EQ(calls->prepare, 1u);
                    EXPECT_EQ(calls->execute,
                              pattern == "COMMON.*foo" ? 0u : 1u);
                    if (pattern == "ABSENT_54191.*") {
                        ASSERT_NE(calls->plan, nullptr);
                        EXPECT_TRUE(calls->plan->empty_result);
                    }
                }
            }
        }
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexCountPlanSurvivesSparseRejection) {
    constexpr size_t n = 1027;
    constexpr size_t batch_size = 128;
    // 21 occurrences retain sparse rejection with the calibrated Locate
    // weight, while a later dense batch can reuse the same prepared plan.
    constexpr size_t marker_stride = 49;
    std::vector<std::string> rows(n, std::string(1000, 'x'));
    for (size_t i = 0; i < n; i += marker_stride) {
        rows[i] += " RARE123END";
    }
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls, 0);
    auto loaded =
        LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, true, factory);
    milvus::test::ExprBatchSizeGuard batch_guard(batch_size);
    auto expression = MakeMatchTypedExpr(loaded.schema,
                                         loaded.varchar_id,
                                         "RARE123END$",
                                         false,
                                         proto::plan::RegexMatch);
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
    query_context->set_enable_expr_cache(false);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({expression}, &exec_context, {}, false);
    exec::EvalCtx context(&exec_context);
    std::shared_ptr<const RegexIndexPlan> first_plan;
    for (size_t begin = 0; begin < n; begin += batch_size) {
        const size_t size = std::min(batch_size, n - begin);
        TargetBitmap input(size, begin != 0);
        if (begin == 0) {
            input.set(0);
        }
        context.set_bitmap_input(std::move(input));
        VectorPtr output;
        compiled[0]->Eval(context, output);
        auto result = std::dynamic_pointer_cast<ColumnVector>(output);
        ASSERT_NE(result, nullptr);
        ASSERT_EQ(result->size(), size);
        ASSERT_EQ(calls->prepare, 1u);
        if (begin == 0) {
            EXPECT_EQ(calls->execute, 0u);
            first_plan = calls->plan;
            ASSERT_NE(first_plan, nullptr);
            EXPECT_EQ(first_plan->occurrences,
                      (n + marker_stride - 1) / marker_stride);
        } else {
            EXPECT_EQ(calls->execute, 1u);
            EXPECT_EQ(calls->plan, first_plan);
        }
        TargetBitmapView hits(result->GetRawData(), size);
        for (size_t i = 0; i < size; ++i) {
            if (context.get_bitmap_input()[i]) {
                EXPECT_EQ(hits[i], (begin + i) % marker_stride == 0);
            }
        }
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexParentSkipAndOffsetInput) {
    constexpr size_t n = 1027;
    constexpr size_t batch_size = 128;
    std::vector<std::string> rows(n, std::string(1000, 'x'));
    std::vector<int64_t> numbers(n);
    for (size_t i = 0; i < n; ++i) {
        numbers[i] = i;
        if (i % 101 == 0) {
            rows[i] += " RARE123END";
        }
    }
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls, 0);
    auto loaded =
        LoadSealedFMMatch(rows, {}, false, nullptr, &numbers, true, factory);
    milvus::test::ExprBatchSizeGuard batch_guard(batch_size);
    auto regex = MakeMatchTypedExpr(loaded.schema,
                                    loaded.varchar_id,
                                    "RARE123END$",
                                    false,
                                    proto::plan::RegexMatch);
    proto::plan::GenericValue lower;
    lower.set_int64_val(batch_size);
    auto numeric = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(loaded.int_id, DataType::INT64),
        proto::plan::GreaterEqual,
        lower);
    auto conjunction = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, numeric, regex);
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
    query_context->set_enable_expr_cache(false);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({conjunction}, &exec_context, {}, false);
    exec::EvalCtx context(&exec_context);
    for (size_t begin = 0; begin < n; begin += batch_size) {
        const size_t size = std::min(batch_size, n - begin);
        VectorPtr output;
        compiled[0]->Eval(context, output);
        auto result = std::dynamic_pointer_cast<ColumnVector>(output);
        ASSERT_NE(result, nullptr);
        ASSERT_EQ(result->size(), size);
        TargetBitmapView hits(result->GetRawData(), size);
        for (size_t i = 0; i < size; ++i) {
            const size_t row = begin + i;
            EXPECT_EQ(hits[i], row >= batch_size && row % 101 == 0);
        }
        EXPECT_EQ(calls->prepare, begin == 0 ? 0u : 1u);
        EXPECT_EQ(calls->execute, begin == 0 ? 0u : 1u);
    }

    // Offset input always uses the existing raw Take path. It must neither
    // Count nor Locate, including repeated requests in a different row order.
    *calls = RegexRoutingCounts{};
    auto offset_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
    offset_context->set_enable_expr_cache(false);
    exec::ExecContext offset_exec(offset_context.get());
    auto offset_compiled =
        exec::CompileExpressions({regex}, &offset_exec, {}, false);
    exec::EvalCtx eval_offset(&offset_exec);
    exec::OffsetVector offsets{0, 101, 199, 303, 404, 1026};
    for (bool reverse : {false, true}) {
        if (reverse) {
            std::reverse(offsets.begin(), offsets.end());
        }
        TargetBitmap input(offsets.size(), true);
        input[2] = false;
        auto saved = input.clone();
        eval_offset.set_bitmap_input(std::move(input));
        eval_offset.set_offset_input(&offsets);
        VectorPtr output;
        offset_compiled[0]->Eval(eval_offset, output);
        auto result = std::dynamic_pointer_cast<ColumnVector>(output);
        ASSERT_NE(result, nullptr);
        ASSERT_EQ(result->size(), offsets.size());
        TargetBitmapView hits(result->GetRawData(), offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            EXPECT_EQ(eval_offset.get_bitmap_input()[i], saved[i]);
            if (saved[i]) {
                EXPECT_EQ(hits[i], offsets[i] % 101 == 0);
            }
        }
        EXPECT_EQ(calls->prepare, 0u);
        EXPECT_EQ(calls->execute, 0u);
    }
}

TEST(FmIndexExpressionTest,
     ExecutorRegexNoCandidateProofPreservesEagerRawPrefetch) {
    constexpr size_t n = 1027;
    constexpr size_t batch_size = 128;
    std::vector<std::string> rows(n, "COMMON foo " + std::string(1000, 'x'));
    rows[101] += " RARE123END";
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls, 128 * 1024);
    auto loaded =
        LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, true, factory);
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    auto check = [&](const SealedFMMatch& fixture,
                     const std::string& pattern,
                     size_t active_rows,
                     bool expect_raw) {
        SCOPED_TRACE(pattern + "/rows=" + std::to_string(active_rows));
        *calls = RegexRoutingCounts{};
        auto typed = MakeMatchTypedExpr(fixture.schema,
                                        fixture.varchar_id,
                                        pattern,
                                        false,
                                        proto::plan::RegexMatch);
        auto unary =
            std::dynamic_pointer_cast<const expr::UnaryRangeFilterExpr>(typed);
        ASSERT_NE(unary, nullptr);
        auto physical = std::make_shared<PrefetchObservingUnaryExpr>(
            std::vector<std::shared_ptr<exec::Expr>>{},
            unary,
            "PhyUnaryRangeFilterExpr",
            nullptr,
            fixture.segment.get(),
            active_rows,
            batch_size,
            0);
        physical->PrefetchAsync(pool);
        physical->WaitPrefetch();
        EXPECT_EQ(physical->GetExecPath(),
                  expect_raw ? exec::ExprExecPath::RawData
                             : exec::ExprExecPath::ScalarIndex);
        EXPECT_EQ(physical->RawPrefetchCalls(), expect_raw ? 1u : 0u);
        EXPECT_EQ(calls->prepare, 0u);
        EXPECT_EQ(calls->execute, 0u);
        EXPECT_EQ(calls->legacy_guard, 0u);
    };
    check(loaded, ".*", n, true);
    check(loaded, "a|RARE123END", n, true);
    check(loaded, "(?i)foo", n, true);
    check(loaded, "RARE123END$", 64, true);
    check(loaded, "RARE123END$", n, false);
    // High Count is not known from syntax. Do not read that Count or eagerly
    // prefetch the whole column before the upstream bitmap becomes available.
    check(loaded, "COMMON.*foo", n, false);

    auto disabled = LoadSealedFMMatch(
        rows, {}, false, nullptr, nullptr, true, factory, 0.0);
    check(disabled, "RARE123END$", n, true);
}

class FMIndexRegexCacheTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        exec::ExprResCacheManager::SetEnabled(true);
        exec::CacheConfig config;
        config.mode = exec::CacheMode::Memory;
        config.mem_max_bytes = 1U << 20;
        config.compression_enabled = false;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(exec::ExprResCacheManager::Instance().SetConfig(config));
        exec::ExprResCacheManager::Instance().Clear();
    }

    void
    TearDown() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(false);
    }
};

TEST_F(FMIndexRegexCacheTest, DeferredFMRegexReadsExactPredicateCache) {
    constexpr size_t n = 1027;
    constexpr size_t batch_size = 128;
    std::vector<std::string> rows(n, "COMMON foo " + std::string(1000, 'x'));
    rows[101] += " RARE123END";
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    validity[404 / 8] &= static_cast<uint8_t>(~(1u << (404 % 8)));
    validity[1026 / 8] &= static_cast<uint8_t>(~(1u << (1026 % 8)));
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory = ObserveFMReader(calls, 0);
    auto loaded = LoadSealedFMMatch(
        rows, {}, true, validity.data(), nullptr, true, factory);
    milvus::test::ExprBatchSizeGuard batch_guard(batch_size);
    auto& manager = exec::ExprResCacheManager::Instance();
    for (const std::string pattern : {"COMMON.*foo", "foo.*RARE123END"}) {
        auto expression = MakeMatchTypedExpr(loaded.schema,
                                             loaded.varchar_id,
                                             pattern,
                                             true,
                                             proto::plan::RegexMatch);
        PartialRegexMatcher oracle(pattern);
        for (bool cache_hit : {false, true}) {
            *calls = RegexRoutingCounts{};
            auto query_context = std::make_shared<exec::QueryContext>(
                DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
            query_context->set_enable_expr_cache(false);
            exec::ExecContext exec_context(query_context.get());
            auto compiled = exec::CompileExpressions(
                {expression}, &exec_context, {}, false);
            auto adapter = std::dynamic_pointer_cast<exec::RawExprCacheAdapter>(
                compiled[0]);
            ASSERT_NE(adapter, nullptr);
            EXPECT_EQ(adapter->wrapped_expr()->GetExecPath(),
                      exec::ExprExecPath::ScalarIndex);
            exec::ExprResCacheManager::Key key{
                loaded.segment->get_segment_id(),
                adapter->wrapped_expr()->GetSignatureForRawExprCache()};
            exec::EvalCtx context(&exec_context);
            for (size_t begin = 0; begin < n; begin += batch_size) {
                const size_t size = std::min(batch_size, n - begin);
                VectorPtr output;
                compiled[0]->Eval(context, output);
                auto result = std::dynamic_pointer_cast<ColumnVector>(output);
                ASSERT_NE(result, nullptr);
                ASSERT_EQ(result->size(), size);
                TargetBitmapView hits(result->GetRawData(), size);
                TargetBitmapView valid(result->GetValidRawData(), size);
                for (size_t i = 0; i < size; ++i) {
                    const size_t row = begin + i;
                    const bool expected_valid =
                        validity[row / 8] & (1u << (row % 8));
                    EXPECT_EQ(valid[i], expected_valid);
                    if (expected_valid) {
                        EXPECT_EQ(hits[i], oracle(rows[row]));
                    }
                }
            }
            EXPECT_EQ(calls->prepare, cache_hit ? 0u : 1u);
            EXPECT_EQ(calls->execute,
                      !cache_hit && pattern == "foo.*RARE123END" ? 1u : 0u);
            exec::ExprResCacheManager::Value cached;
            cached.active_count = n;
            ASSERT_TRUE(manager.Get(key, cached));
            ASSERT_NE(cached.result, nullptr);
            ASSERT_NE(cached.valid_result, nullptr);
            EXPECT_EQ(cached.result->size(), n);
            EXPECT_EQ(cached.valid_result->size(), n);
        }
    }
}

// Opt-in paired measurement of the production expression path. Data and
// indexes and shared regex-analysis caches are warm. Every sample owns fresh
// query/planner state, including Count preparation. This does not measure the
// first-ever RE2 compilation. Route
// counters are collected separately from an untimed spy, never in production.
// BEGIN_COMPATIBLE_REGEX_PREFILTER_BENCHMARK
// Append this block to the baseline test TU with
// MILVUS_FM_PREFILTER_BASELINE defined to run the identical timed query body.
TEST(FmIndexExpressionTest, DISABLED_RegexPrefilterRoutingBenchmark) {
    constexpr size_t n = 32771;
    constexpr size_t batch_size = 8192;
    constexpr int warmup = 3;
    constexpr int repetitions = 31;
    milvus::test::ExprBatchSizeGuard batch_guard(batch_size);
    std::vector<std::string> rows(n, "COMMON foo " + std::string(512, 'x'));
    for (size_t i = 0; i < n; ++i) {
        if (i % 64 == 0) {
            rows[i] += " MID_54191 tail";
        }
        if (i % 4096 == 0) {
            rows[i] += " RARE_54191 tail";
        }
    }
    auto indexed = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr);
    auto scan = LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, false);
#ifndef MILVUS_FM_PREFILTER_BASELINE
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto factory =
        ObserveFMReader(calls, RegexIndexCostModel{}.preparation_floor_bytes);
    auto spy =
        LoadSealedFMMatch(rows, {}, false, nullptr, nullptr, true, factory);
#endif
    const std::vector<std::pair<std::string, std::string>> workloads{
        {"rare", "foo.*RARE_54191"},
        {"medium", "foo.*MID_54191"},
        {"common", "COMMON.*foo"},
        {"no_literal", ".*"},
        {"short_branch", "a|RARE_54191"},
        {"folded", "(?i)COMMON"},
        {"zero_hits", "ABSENT_54191.*"},
        {"or_branches", "foo.*MID_54191|RARE_54191.*foo"},
    };
    RecordProperty("rows", n);
    RecordProperty("batch_size", batch_size);
    RecordProperty("samples", repetitions);
    RecordProperty("expression_result_cache", "disabled");
    RecordProperty("timing_scope",
                   "fresh expression/RE2/Count plan; warm sealed data/index");
    auto row_ids = [](const TargetBitmap& bitmap) {
        std::vector<int64_t> ids;
        for (size_t row = 0; row < bitmap.size(); ++row) {
            if (bitmap[row]) {
                ids.push_back(static_cast<int64_t>(row));
            }
        }
        return ids;
    };
    RecordProperty("fmindex_cost_ratio", "0.001 (reader constructor default)");
    auto run = [&](const SealedFMMatch& fixture,
                   const expr::TypedExprPtr& expression,
                   const TargetBitmap& mask) {
        const auto start = std::chrono::steady_clock::now();
        auto query_context = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, fixture.segment.get(), n, MAX_TIMESTAMP);
        query_context->set_enable_expr_cache(false);
        exec::ExecContext exec_context(query_context.get());
        auto compiled =
            exec::CompileExpressions({expression}, &exec_context, {}, false);
        exec::EvalCtx context(&exec_context);
        TargetBitmap combined;
        for (size_t begin = 0; begin < n; begin += batch_size) {
            const size_t size = std::min(batch_size, n - begin);
            TargetBitmap input;
            input.append(mask, begin, size);
            context.set_bitmap_input(std::move(input));
            VectorPtr output;
            compiled[0]->Eval(context, output);
            auto result = std::dynamic_pointer_cast<ColumnVector>(output);
            AssertInfo(result != nullptr && result->size() == size,
                       "benchmark batch {} has unexpected result size",
                       begin);
            combined.append(TargetBitmapView(result->GetRawData(), size));
        }
        const double elapsed = std::chrono::duration<double, std::micro>(
                                   std::chrono::steady_clock::now() - start)
                                   .count();
        combined &= mask;
        return std::pair(std::move(combined), elapsed);
    };
    auto samples_text = [](const std::vector<double>& samples) {
        std::string text;
        for (double sample : samples) {
            if (!text.empty()) {
                text += ',';
            }
            text += std::to_string(sample);
        }
        return text;
    };
    for (const auto& [name, pattern] : workloads) {
        PartialRegexMatcher oracle(pattern);
        auto scan_expr = MakeMatchTypedExpr(scan.schema,
                                            scan.varchar_id,
                                            pattern,
                                            false,
                                            proto::plan::RegexMatch);
        auto indexed_expr = MakeMatchTypedExpr(indexed.schema,
                                               indexed.varchar_id,
                                               pattern,
                                               false,
                                               proto::plan::RegexMatch);
#ifndef MILVUS_FM_PREFILTER_BASELINE
        auto spy_expr = MakeMatchTypedExpr(spy.schema,
                                           spy.varchar_id,
                                           pattern,
                                           false,
                                           proto::plan::RegexMatch);
#endif
        for (int density : {0, 1, 10, 100}) {
            const std::string key = name + "_mask_" + std::to_string(density);
            SCOPED_TRACE(key);
            TargetBitmap mask(n, density == 100);
            if (density > 0 && density < 100) {
                for (size_t i = 0; i < n; i += 100 / density) {
                    mask.set(i);
                }
            }
            std::vector<int64_t> expected;
            for (size_t row = 0; row < n; ++row) {
                if (mask[row] && oracle(rows[row])) {
                    expected.push_back(row);
                }
            }
            std::array<std::vector<double>, 2> samples;
            for (int repetition = -warmup; repetition < repetitions;
                 ++repetition) {
                for (int order = 0; order < 2; ++order) {
                    const bool with_index =
                        (order + repetition + warmup) % 2 != 0;
                    auto [got, elapsed] = with_index
                                              ? run(indexed, indexed_expr, mask)
                                              : run(scan, scan_expr, mask);
                    ASSERT_EQ(row_ids(got), expected);
                    if (repetition >= 0) {
                        samples[with_index].push_back(elapsed);
                    }
                }
            }
            for (int indexed_mode = 0; indexed_mode < 2; ++indexed_mode) {
                const std::string prefix =
                    key + (indexed_mode ? "_indexed_" : "_scan_");
                RecordProperty(prefix + "samples_us",
                               samples_text(samples[indexed_mode]));
                auto sorted = samples[indexed_mode];
                std::sort(sorted.begin(), sorted.end());
                RecordProperty(prefix + "p50_us",
                               std::to_string(sorted[sorted.size() / 2]));
                RecordProperty(
                    prefix + "p95_us",
                    std::to_string(sorted[(sorted.size() * 95 - 1) / 100]));
            }
#ifndef MILVUS_FM_PREFILTER_BASELINE
            *calls = RegexRoutingCounts{};
            auto [got, ignored_elapsed] = run(spy, spy_expr, mask);
            ASSERT_EQ(row_ids(got), expected);
            RecordProperty(key + "_prepare_calls", calls->prepare);
            RecordProperty(key + "_execute_calls", calls->execute);
            RecordProperty(key + "_count",
                           calls->plan
                               ? std::to_string(calls->plan->occurrences)
                               : "not_prepared");
#endif
            RecordProperty(key + "_hits", expected.size());
        }
    }
}
// END_COMPATIBLE_REGEX_PREFILTER_BENCHMARK
