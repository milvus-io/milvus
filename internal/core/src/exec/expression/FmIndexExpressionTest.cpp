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
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/RawExprCacheAdapter.h"
#include "exec/expression/RegexPrefilterStats.h"
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
                  std::optional<double> reader_cost_ratio = std::nullopt,
                  bool ints_nullable = false,
                  const uint8_t* ints_validity = nullptr) {
    SealedFMMatch out;
    out.raw_files = std::make_unique<test::expr_index::RawFieldFiles>();
    out.schema = std::make_shared<Schema>();
    out.varchar_id =
        out.schema->AddDebugField("fm_match", DataType::VARCHAR, nullable);
    if (ints != nullptr) {
        out.int_id =
            out.schema->AddDebugField("fm_int", DataType::INT64, ints_nullable);
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
        auto field = std::make_shared<FieldData<int64_t>>(DataType::INT64,
                                                          ints_nullable);
        if (ints_nullable) {
            field->FillFieldData(ints->data(), ints_validity, ints->size(), 0);
        } else {
            field->FillFieldData(ints->data(), ints->size());
        }
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
    size_t like_pattern = 0;
    std::shared_ptr<const RegexIndexPlan> plan;
    const void* candidate_data = nullptr;
    const void* validity_data = nullptr;
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
        auto validity = nulls_->IsNotNull();
        counts_->validity_data = validity.data();
        return validity;
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
        auto candidates = pattern_->ExecuteRegexIndexPlan(plan);
        counts_->candidate_data = candidates.data();
        return candidates;
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
        counts_->like_pattern += op == index::PatternOp::Match;
        auto candidates = pattern_->PatternMatch(pattern, op);
        counts_->candidate_data = candidates.data();
        return candidates;
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

class FMCandidateCacheObservingUnaryExpr
    : public exec::PhyUnaryRangeFilterExpr {
 public:
    using PhyUnaryRangeFilterExpr::PhyUnaryRangeFilterExpr;

    void
    SeedCandidates(TargetBitmap candidates, TargetBitmap validity) {
        cached_phase1_res_ =
            std::make_shared<TargetBitmap>(std::move(candidates));
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(std::move(validity));
    }

    const TargetBitmap&
    Candidates() const {
        return *cached_phase1_res_;
    }

    const TargetBitmap&
    Validity() const {
        return *cached_index_chunk_valid_res_;
    }

    std::shared_ptr<const TargetBitmap>
    CandidateAlias() const {
        return cached_phase1_res_;
    }

    std::shared_ptr<const TargetBitmap>
    ValidityAlias() const {
        return cached_index_chunk_valid_res_;
    }
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
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
        match_expr, loaded.segment.get(), static_cast<int64_t>(nb)));

    // Both candidate paths keep one complete logical result. Regex may still
    // fall back to ordinary physical scan batches after seeing an input mask.
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
    auto idx = BuildFmPatternReader(rows);
    EXPECT_TRUE(
        idx->ShouldUseForOp(index::PatternOp::RegexMatch, R"(RARE\d+)"));
    EXPECT_TRUE(idx->ShouldUseForOp(index::PatternOp::RegexMatch, "ABSENT.*"));
    EXPECT_TRUE(idx->ShouldUseForOp(index::PatternOp::RegexMatch, ".*RARE"));
    EXPECT_TRUE(
        idx->ShouldUseForOp(index::PatternOp::RegexMatch, "x.*RARE123"));
    EXPECT_EQ(
        idx->PatternMatch("x.*RARE123", index::PatternOp::RegexMatch).count(),
        1);
    for (const std::string pattern : {"x+", ".*", "", "RARE|", "(?i)rare"}) {
        EXPECT_FALSE(idx->ShouldUseForOp(index::PatternOp::RegexMatch, pattern))
            << pattern;
    }
    EXPECT_FALSE(idx->ShouldUseForOp(index::PatternOp::RegexMatch,
                                     std::string(4097, 'a')));
}

TEST(FmIndexExpressionTest, RegexCandidatesKeepRarePrefixWhenInteriorIsCommon) {
    std::vector<std::string> rows(1000, std::string(500, 'x') + "COMMON");
    rows[500] = "RARE" + rows[500];
    auto idx = BuildFmPatternReader(rows);
    EXPECT_TRUE(
        idx->ShouldUseForOp(index::PatternOp::RegexMatch, "RARE.*COMMON"));
    const auto candidates =
        idx->PatternMatch("RARE.*COMMON", index::PatternOp::RegexMatch);
    EXPECT_EQ(candidates.count(), 1);
    EXPECT_TRUE(candidates[500]);
}

TEST(FmIndexExpressionTest, RegexPreparationDoesNotCacheIndexStatistics) {
    std::vector<std::string> first_rows(1000, std::string(500, 'x') + "bar");
    first_rows[7] = "foo" + first_rows[7];
    std::vector<std::string> second_rows(1000, "foo" + std::string(500, 'x'));
    second_rows[9] += "bar";
    auto first = BuildFmPatternReader(first_rows);
    auto second = BuildFmPatternReader(second_rows);
    for (int repeat = 0; repeat < 2; ++repeat) {
        EXPECT_TRUE(
            first->ShouldUseForOp(index::PatternOp::RegexMatch, "foo.*bar"));
        EXPECT_TRUE(
            second->ShouldUseForOp(index::PatternOp::RegexMatch, "foo.*bar"));
        // The same cached requirements choose different rarest fragments.
        const auto first_candidates =
            first->PatternMatch("foo.*bar", index::PatternOp::RegexMatch);
        const auto second_candidates =
            second->PatternMatch("foo.*bar", index::PatternOp::RegexMatch);
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
    auto index = BuildFmPatternReader(rows);
    for (const auto& [pattern, expected] :
         std::vector<std::pair<std::string, size_t>>{
             {".*COMMON_LONG.*RARE", 1},
             {"(ERROR|WARN).*timeout", 1},
             {"foo.*bar|baz.*qux", 2},
             {"foo.*ABSENT|baz.*qux", 1}}) {
        EXPECT_TRUE(
            index->ShouldUseForOp(index::PatternOp::RegexMatch, pattern));
        auto candidates =
            index->PatternMatch(pattern, index::PatternOp::RegexMatch);
        EXPECT_EQ(candidates.count(), expected) << pattern;
        PartialRegexMatcher matcher(pattern);
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(candidates[i] && matcher(rows[i]), matcher(rows[i]))
                << pattern;
        }
    }
    for (const std::string pattern :
         {"a|timeout", "(?i:foo)|timeout", "(foo)?|timeout"}) {
        EXPECT_FALSE(
            index->ShouldUseForOp(index::PatternOp::RegexMatch, pattern));
        EXPECT_EQ(
            index->PatternMatch(pattern, index::PatternOp::RegexMatch).count(),
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
                ASSERT_EQ(compiled[0]->CanExecuteAllAtOnce(),
                          pattern != ".*" && pattern != "a|RARE123END");
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

TEST(FmIndexExpressionTest, RegexPrefilterWorkTracksPhysicalWindowsAndScope) {
    constexpr size_t n = 1027;
    constexpr size_t batch = 128;
    auto truth = std::make_shared<TargetBitmap>(n, false);
    for (size_t row : {size_t{1}, size_t{67}, size_t{256}, size_t{1026}}) {
        truth->set(row);
    }
    const auto complete = exec::ComputeRegexBitmapWork(*truth, batch);
    EXPECT_EQ(complete.active_rows, 4u);
    EXPECT_EQ(complete.visited_rows, 259u);
    EXPECT_EQ(complete.visited_batches, 3u);
    exec::RegexPrefilterStats suffix(truth);
    const auto later = suffix.Get(batch, n - batch, batch);
    ASSERT_TRUE(later.has_value());
    EXPECT_EQ(later->active_rows, 2u);
    EXPECT_EQ(later->visited_rows, 131u);
    EXPECT_EQ(later->visited_batches, 2u);
    EXPECT_FALSE(suffix.Get(batch + 1, n - batch - 1, batch).has_value());
    EXPECT_FALSE(suffix.Get(batch, n, batch).has_value());
    exec::RegexPrefilterStats tiny_batches(
        std::make_shared<TargetBitmap>(8192, true));
    EXPECT_FALSE(tiny_batches.Get(0, 8192, 1).has_value());

    auto loaded =
        LoadSealedFMMatch({"row"}, {}, false, nullptr, nullptr, false);
    auto query = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, loaded.segment.get(), 1, MAX_TIMESTAMP);
    exec::ExecContext execution(query.get());
    exec::EvalCtx context(&execution);
    size_t requests = 0;
    exec::EvalCtx::RegexWorkProvider provider =
        [&](size_t begin, size_t remaining, size_t physical_batch) {
            ++requests;
            return suffix.Get(begin, remaining, physical_batch);
        };
    context.set_bitmap_input(TargetBitmap(batch, true));
    {
        exec::EvalCtx::ScopedRegexWorkProvider outer(context, &provider);
        ASSERT_TRUE(context.get_regex_work_stats(batch, n - batch, batch));
        {
            exec::EvalCtx::ScopedRegexWorkProvider unknown(context, nullptr);
            EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
        }
        ASSERT_TRUE(context.get_regex_work_stats(batch, n - batch, batch));
        // A new mask invalidates both this scope and the saved outer one.
        context.set_bitmap_input(TargetBitmap(batch, false));
        EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
    }
    EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
    EXPECT_EQ(requests, 2u);
    exec::OffsetVector offsets{0};
    {
        exec::EvalCtx::ScopedRegexWorkProvider outer(context, &provider);
        context.set_offset_input(&offsets);
        EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
    }
    context.set_offset_input(nullptr);
    EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
    {
        exec::EvalCtx::ScopedRegexWorkProvider outer(context, &provider);
        context.clear_bitmap_input();
        EXPECT_FALSE(context.get_regex_work_stats(batch, n - batch, batch));
    }
    EXPECT_EQ(requests, 2u);
}

TEST(FmIndexExpressionTest,
     ExecutorRegexOneShotFallbackMatchesOrdinaryMaskedBatches) {
    constexpr size_t n = 1027;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, "COMMON foo " + std::string(1000, 'x'));
    for (size_t row = 0; row < n; row += 49) {
        rows[row] += " RARE123END";
    }
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    for (size_t row : {size_t{0}, size_t{404}, size_t{765}, size_t{1026}}) {
        validity[row / 8] &= static_cast<uint8_t>(~(1u << (row % 8)));
    }
    auto chunks = MakeNullableStringChunks(rows, validity, {0, 199, 540, n});
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto indexed = LoadSealedFMMatch(rows,
                                     chunks,
                                     true,
                                     validity.data(),
                                     nullptr,
                                     true,
                                     ObserveFMReader(calls, 0));
    auto raw =
        LoadSealedFMMatch(rows, chunks, true, validity.data(), nullptr, false);
    ASSERT_EQ(indexed.segment->num_chunk_data(indexed.varchar_id), 3);
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (const std::string pattern : {"COMMON.*foo", "RARE123END$"}) {
        for (int mask_kind : {0, 1, 2, 3}) {
            if (pattern == "RARE123END$" && mask_kind < 2) {
                continue;  // Dense rare candidates belong to the next test.
            }
            TargetBitmap mask;
            if (mask_kind != 0) {
                mask = TargetBitmap(n, mask_kind == 1);
                if (mask_kind == 2) {
                    for (size_t row : {size_t{49},
                                       size_t{129},
                                       size_t{256},
                                       size_t{404},
                                       size_t{513},
                                       size_t{1026}}) {
                        mask.set(row);
                    }
                }
            }
            for (bool negate : {false, true}) {
                SCOPED_TRACE(pattern + "/mask=" + std::to_string(mask_kind) +
                             "/not=" + std::to_string(negate));
                auto evaluate = [&](const SealedFMMatch& fixture,
                                    bool one_shot) {
                    auto expression =
                        MakeMatchTypedExpr(fixture.schema,
                                           fixture.varchar_id,
                                           pattern,
                                           true,
                                           proto::plan::RegexMatch);
                    expr::TypedExprPtr root = expression;
                    if (negate) {
                        root = std::make_shared<expr::LogicalUnaryExpr>(
                            expr::LogicalUnaryExpr::OpType::LogicalNot, root);
                    }
                    auto query = std::make_shared<exec::QueryContext>(
                        DEAFULT_QUERY_ID,
                        fixture.segment.get(),
                        n,
                        MAX_TIMESTAMP);
                    query->set_enable_expr_cache(false);
                    exec::ExecContext execution(query.get());
                    auto compiled =
                        exec::CompileExpressions({root}, &execution, {}, false);
                    if (one_shot) {
                        EXPECT_TRUE(compiled[0]->CanExecuteAllAtOnce());
                        compiled[0]->SetExecuteAllAtOnce();
                    }
                    exec::EvalCtx context(&execution);
                    TargetBitmap hits;
                    TargetBitmap valid;
                    for (size_t begin = 0; begin < n;) {
                        const auto size =
                            one_shot ? n : std::min(batch, n - begin);
                        if (!mask.empty()) {
                            TargetBitmap local;
                            local.append(mask, begin, size);
                            context.set_bitmap_input(std::move(local));
                        }
                        VectorPtr output;
                        compiled[0]->Eval(context, output);
                        auto column =
                            std::dynamic_pointer_cast<ColumnVector>(output);
                        AssertInfo(
                            column != nullptr && column->size() == size,
                            "one-shot fallback changed the logical shape");
                        hits.append(
                            TargetBitmapView(column->GetRawData(), size));
                        valid.append(
                            TargetBitmapView(column->GetValidRawData(), size));
                        if (!mask.empty()) {
                            for (size_t i = 0; i < size; ++i) {
                                EXPECT_EQ(context.get_bitmap_input()[i],
                                          mask[begin + i]);
                            }
                        }
                        begin += size;
                    }
                    auto physical = compiled[0];
                    if (negate) {
                        physical = physical->GetInputsRef()[0];
                    }
                    if (auto adapter = std::dynamic_pointer_cast<
                            exec::RawExprCacheAdapter>(physical)) {
                        physical = adapter->wrapped_expr();
                    }
                    auto leaf = std::dynamic_pointer_cast<
                        exec::PhyUnaryRangeFilterExpr>(physical);
                    AssertInfo(leaf != nullptr,
                               "expected the original unary leaf");
                    EXPECT_EQ(leaf->GetNextBatchSize(), 0);
                    // Offset Take stays available after complete sequential
                    // exhaustion and does not resurrect a sequential window.
                    context.clear_bitmap_input();
                    exec::OffsetVector offsets{0, 101, 404, 1026};
                    context.set_offset_input(&offsets);
                    VectorPtr gathered;
                    leaf->Eval(context, gathered);
                    auto column =
                        std::dynamic_pointer_cast<ColumnVector>(gathered);
                    AssertInfo(column != nullptr,
                               "offset Take must return a vector");
                    EXPECT_EQ(column->size(), offsets.size());
                    TargetBitmapView offset_hits(column->GetRawData(),
                                                 offsets.size());
                    TargetBitmapView offset_valid(column->GetValidRawData(),
                                                  offsets.size());
                    PartialRegexMatcher offset_oracle(pattern);
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        const size_t row = offsets[i];
                        const bool row_valid =
                            validity[row / 8] & (1u << (row % 8));
                        EXPECT_EQ(offset_valid[i], row_valid);
                        if (row_valid) {
                            EXPECT_EQ(offset_hits[i], offset_oracle(rows[row]));
                        }
                    }
                    context.set_offset_input(nullptr);
                    VectorPtr exhausted;
                    leaf->Eval(context, exhausted);
                    EXPECT_EQ(exhausted, nullptr);
                    return std::pair(std::move(hits), std::move(valid));
                };
                *calls = RegexRoutingCounts{};
                const auto expected = evaluate(raw, false);
                const auto actual = evaluate(indexed, true);
                EXPECT_EQ(calls->prepare, mask_kind == 3 ? 0u : 1u);
                EXPECT_EQ(calls->execute, 0u);
                for (size_t row = 0; row < n; ++row) {
                    // Includes inactive NULL placeholders, active NULLs,
                    // skipped complete windows, chunk crossings and short tail.
                    EXPECT_EQ(actual.first[row], expected.first[row]) << row;
                    EXPECT_EQ(actual.second[row], expected.second[row]) << row;
                }
            }
        }
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexRareAndZeroKeepExactOneShotResults) {
    constexpr size_t n = 1027;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, std::string(1000, 'x'));
    rows[101] += " RARE123END";
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    validity[0] &= static_cast<uint8_t>(~1u);
    validity[1026 / 8] &= static_cast<uint8_t>(~(1u << (1026 % 8)));
    auto chunks = MakeNullableStringChunks(rows, validity, {0, 199, 540, n});
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto loaded = LoadSealedFMMatch(rows,
                                    chunks,
                                    true,
                                    validity.data(),
                                    nullptr,
                                    true,
                                    ObserveFMReader(calls, 0));
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (const std::string pattern : {"RARE123END$", "ABSENT_54191.*"}) {
        for (bool negate : {false, true}) {
            *calls = RegexRoutingCounts{};
            auto typed = MakeMatchTypedExpr(loaded.schema,
                                            loaded.varchar_id,
                                            pattern,
                                            true,
                                            proto::plan::RegexMatch);
            expr::TypedExprPtr root = typed;
            if (negate) {
                root = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, root);
            }
            auto node = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, root);
            ASSERT_TRUE(milvus::test::CanExprExecuteAllAtOnce(
                root, loaded.segment.get(), n));
            auto output = milvus::query::ExecuteQueryExpr(
                node, loaded.segment.get(), n, MAX_TIMESTAMP);
            PartialRegexMatcher oracle(pattern);
            for (size_t row = 0; row < n; ++row) {
                const bool valid = validity[row / 8] & (1u << (row % 8));
                EXPECT_EQ(output[row], valid && (negate != oracle(rows[row])));
            }
            EXPECT_EQ(calls->prepare, 1u);
            EXPECT_EQ(calls->execute, 1u);
            ASSERT_NE(calls->plan, nullptr);
            EXPECT_EQ(calls->plan->empty_result, pattern == "ABSENT_54191.*");
        }
    }
}

TEST_P(FmIndexPatternExpressionTest,
       ExecutorSparseCandidateWordsAndChunksMatchBatches) {
    const auto op = GetParam();
    constexpr size_t n = 1027;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, "foo " + std::string(1000, 'x'));
    for (size_t row : {size_t{63},
                       size_t{64},
                       size_t{127},
                       size_t{128},
                       size_t{198},
                       size_t{199},
                       size_t{539},
                       size_t{540},
                       size_t{1026}}) {
        rows[row] += " SPARSE_TOKEN";
    }
    // Both rows remain literal candidates, but fail the canonical ordering.
    rows[64] = "SPARSE_TOKEN " + rows[0];
    rows[540] = "SPARSE_TOKEN " + rows[0];
    rows[65] += " SPARSEXTOKEN";  // '_' remains a literal in the LIKE case.
    rows[127] += " SINGLE_TOKEN";
    rows.back() += " TAIL_TOKEN";
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    for (size_t row : {size_t{0}, size_t{128}, size_t{539}, size_t{1025}}) {
        validity[row / 8] &= static_cast<uint8_t>(~(1u << (row % 8)));
    }
    auto chunks = MakeNullableStringChunks(rows, validity, {0, 199, 540, n});
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto loaded = LoadSealedFMMatch(rows,
                                    chunks,
                                    true,
                                    validity.data(),
                                    nullptr,
                                    true,
                                    ObserveFMReader(calls, 0));
    ASSERT_EQ(loaded.segment->num_chunk_data(loaded.varchar_id), 3);
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (const auto& patterns :
         std::array<std::pair<std::string, std::string>, 4>{
             {{"foo.*SPARSE_TOKEN", R"(%foo%SPARSE\_TOKEN%)"},
              {"foo.*SINGLE_TOKEN", R"(%foo%SINGLE\_TOKEN%)"},
              {"foo.*TAIL_TOKEN", R"(%foo%TAIL\_TOKEN%)"},
              {"foo.*ABSENT_TOKEN", R"(%foo%ABSENT\_TOKEN%)"}}}) {
        const auto& pattern =
            op == proto::plan::Match ? patterns.second : patterns.first;
        PartialRegexMatcher regex_oracle(patterns.first);
        LikePatternMatcher like_oracle(patterns.second);
        const auto oracle = [&](const std::string& row) {
            return op == proto::plan::Match ? like_oracle(row)
                                            : regex_oracle(row);
        };
        for (bool masked : {false, true}) {
            TargetBitmap mask(n, true);
            if (masked) {
                for (size_t row : {size_t{63}, size_t{127}, size_t{199}}) {
                    mask.reset(row);
                }
            }
            for (bool negate : {false, true}) {
                std::optional<std::pair<TargetBitmap, TargetBitmap>> batched;
                for (bool one_shot : {false, true}) {
                    SCOPED_TRACE(pattern + "/masked=" + std::to_string(masked) +
                                 "/not=" + std::to_string(negate) +
                                 "/one_shot=" + std::to_string(one_shot));
                    *calls = RegexRoutingCounts{};
                    auto typed = MakeMatchTypedExpr(
                        loaded.schema, loaded.varchar_id, pattern, true, op);
                    expr::TypedExprPtr root = typed;
                    if (negate) {
                        root = std::make_shared<expr::LogicalUnaryExpr>(
                            expr::LogicalUnaryExpr::OpType::LogicalNot, root);
                    }
                    auto query = std::make_shared<exec::QueryContext>(
                        DEAFULT_QUERY_ID,
                        loaded.segment.get(),
                        n,
                        MAX_TIMESTAMP);
                    query->set_enable_expr_cache(false);
                    exec::ExecContext execution(query.get());
                    auto compiled =
                        exec::CompileExpressions({root}, &execution, {}, false);
                    if (one_shot) {
                        ASSERT_TRUE(compiled[0]->CanExecuteAllAtOnce());
                        compiled[0]->SetExecuteAllAtOnce();
                    }
                    exec::EvalCtx context(&execution);
                    TargetBitmap hits;
                    TargetBitmap valid;
                    for (size_t begin = 0; begin < n;) {
                        const size_t size =
                            one_shot ? n : std::min(batch, n - begin);
                        if (masked) {
                            TargetBitmap local;
                            local.append(mask, begin, size);
                            context.set_bitmap_input(std::move(local));
                        }
                        VectorPtr output;
                        compiled[0]->Eval(context, output);
                        auto column =
                            std::dynamic_pointer_cast<ColumnVector>(output);
                        ASSERT_NE(column, nullptr);
                        ASSERT_EQ(column->size(), size);
                        if (one_shot) {
                            // These are the actual reader allocations, observed
                            // without keeping an extra owner of either bitmap.
                            EXPECT_EQ(column->GetRawData(),
                                      calls->candidate_data);
                            EXPECT_EQ(column->GetValidRawData(),
                                      calls->validity_data);
                        }
                        hits.append(
                            TargetBitmapView(column->GetRawData(), size));
                        valid.append(
                            TargetBitmapView(column->GetValidRawData(), size));
                        if (masked) {
                            for (size_t i = 0; i < size; ++i) {
                                EXPECT_EQ(context.get_bitmap_input()[i],
                                          mask[begin + i]);
                            }
                        }
                        begin += size;
                    }
                    EXPECT_EQ(calls->prepare,
                              op == proto::plan::RegexMatch ? 1u : 0u);
                    EXPECT_EQ(calls->execute,
                              op == proto::plan::RegexMatch ? 1u : 0u);
                    EXPECT_EQ(calls->like_pattern,
                              op == proto::plan::Match ? 1u : 0u);
                    EXPECT_EQ(calls->legacy_guard, 0u);
                    EXPECT_EQ(calls->legacy_pattern, 0u);
                    for (size_t row = 0; row < n; ++row) {
                        const bool row_valid =
                            validity[row / 8] & (1u << (row % 8));
                        EXPECT_EQ(valid[row], row_valid) << row;
                        if (mask[row] && row_valid) {
                            EXPECT_EQ(hits[row], negate != oracle(rows[row]))
                                << row;
                        }
                        if (batched.has_value()) {
                            EXPECT_EQ(hits[row], batched->first[row]) << row;
                            EXPECT_EQ(valid[row], batched->second[row]) << row;
                        }
                    }
                    auto physical = compiled[0];
                    if (negate) {
                        physical = physical->GetInputsRef()[0];
                    }
                    if (auto adapter = std::dynamic_pointer_cast<
                            exec::RawExprCacheAdapter>(physical)) {
                        physical = adapter->wrapped_expr();
                    }
                    auto leaf = std::dynamic_pointer_cast<
                        exec::PhyUnaryRangeFilterExpr>(physical);
                    ASSERT_NE(leaf, nullptr);
                    // Check before re-entering Eval: a moved-from cache must
                    // never be read if one-shot completion missed its cursor.
                    ASSERT_EQ(leaf->GetNextBatchSize(), 0);
                    context.clear_bitmap_input();
                    VectorPtr exhausted;
                    leaf->Eval(context, exhausted);
                    EXPECT_EQ(exhausted, nullptr);
                    // Take has its own row domain after sequential exhaustion.
                    // Exercise disorder, duplicates, endpoints and NULLs.
                    exec::OffsetVector offsets{1026, 0, 199, 64, 128, 63, 1026};
                    context.set_offset_input(&offsets);
                    VectorPtr gathered;
                    leaf->Eval(context, gathered);
                    auto taken =
                        std::dynamic_pointer_cast<ColumnVector>(gathered);
                    ASSERT_NE(taken, nullptr);
                    ASSERT_EQ(taken->size(), offsets.size());
                    TargetBitmapView offset_hits(taken->GetRawData(),
                                                 offsets.size());
                    TargetBitmapView offset_valid(taken->GetValidRawData(),
                                                  offsets.size());
                    for (size_t i = 0; i < offsets.size(); ++i) {
                        const size_t row = offsets[i];
                        const bool row_valid =
                            validity[row / 8] & (1u << (row % 8));
                        EXPECT_EQ(offset_valid[i], row_valid);
                        if (row_valid) {
                            // The leaf is positive even when its original
                            // parent applies NOT to the sequential result.
                            EXPECT_EQ(offset_hits[i], oracle(rows[row]));
                        }
                    }
                    context.set_offset_input(nullptr);
                    ASSERT_EQ(leaf->GetNextBatchSize(), 0);
                    leaf->Eval(context, exhausted);
                    EXPECT_EQ(exhausted, nullptr);
                    EXPECT_EQ(calls->prepare,
                              op == proto::plan::RegexMatch ? 1u : 0u);
                    EXPECT_EQ(calls->execute,
                              op == proto::plan::RegexMatch ? 1u : 0u);
                    EXPECT_EQ(calls->like_pattern,
                              op == proto::plan::Match ? 1u : 0u);
                    EXPECT_EQ(calls->legacy_guard, 0u);
                    EXPECT_EQ(calls->legacy_pattern, 0u);
                    if (!one_shot) {
                        batched.emplace(std::move(hits), std::move(valid));
                    }
                }
            }
        }
    }
}

TEST_P(FmIndexPatternExpressionTest,
       ExecutorCandidateCacheOwnershipProtectsAliasesAndBatches) {
    const auto op = GetParam();
    constexpr size_t n = 259;
    constexpr size_t batch = 128;
    const std::string pattern = op == proto::plan::Match
                                    ? R"(%foo%SPARSE\_TOKEN%)"
                                    : "foo.*SPARSE_TOKEN";
    std::vector<std::string> rows(n, "foo " + std::string(1000, 'x'));
    TargetBitmap candidates(n, false);
    for (size_t row : {size_t{0},
                       size_t{63},
                       size_t{64},
                       size_t{65},
                       size_t{127},
                       size_t{128},
                       size_t{192},
                       size_t{193},
                       size_t{257},
                       size_t{258}}) {
        rows[row] += " SPARSE_TOKEN";
        candidates.set(row);
    }
    rows[63] = "SPARSE_TOKEN " + rows[1];
    rows[193] = "SPARSE_TOKEN " + rows[1];
    rows[66] += " SPARSEXTOKEN";
    candidates.set(66);  // Recheck must reject an unescaped-'_' near miss.
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    TargetBitmap valid(n, true);
    for (size_t row : {size_t{0}, size_t{128}}) {
        validity[row / 8] &= static_cast<uint8_t>(~(1u << (row % 8)));
        valid.reset(row);
        candidates.reset(row);
    }
    auto chunks = MakeNullableStringChunks(rows, validity, {0, 65, 193, n});
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto loaded = LoadSealedFMMatch(rows,
                                    chunks,
                                    true,
                                    validity.data(),
                                    nullptr,
                                    true,
                                    ObserveFMReader(calls, 0));
    auto typed =
        MakeMatchTypedExpr(loaded.schema, loaded.varchar_id, pattern, true, op);
    auto unary =
        std::dynamic_pointer_cast<const expr::UnaryRangeFilterExpr>(typed);
    ASSERT_NE(unary, nullptr);
    PartialRegexMatcher regex_oracle("foo.*SPARSE_TOKEN");
    LikePatternMatcher like_oracle(R"(%foo%SPARSE\_TOKEN%)");
    const auto oracle = [&](const std::string& row) {
        return op == proto::plan::Match ? like_oracle(row) : regex_oracle(row);
    };
    for (bool dense : {false, true}) {
        // Dense candidates include every valid row, so canonical recheck must
        // reject most rows across multiple bitmap words.
        const auto seeded_candidates =
            dense ? valid.clone() : candidates.clone();
        ASSERT_EQ(seeded_candidates.count() > n / 2, dense);
        for (bool one_shot : {false, true}) {
            for (int aliases : {0, 1, 2, 3}) {
                *calls = RegexRoutingCounts{};
                SCOPED_TRACE("dense=" + std::to_string(dense) +
                             "/one_shot=" + std::to_string(one_shot) +
                             "/aliases=" + std::to_string(aliases));
                auto physical =
                    std::make_shared<FMCandidateCacheObservingUnaryExpr>(
                        std::vector<std::shared_ptr<exec::Expr>>{},
                        unary,
                        "PhyUnaryRangeFilterExpr",
                        nullptr,
                        loaded.segment.get(),
                        n,
                        batch,
                        0);
                ASSERT_TRUE(physical->CanExecuteAllAtOnce());
                if (one_shot) {
                    physical->SetExecuteAllAtOnce();
                }
                physical->SeedCandidates(seeded_candidates.clone(),
                                         valid.clone());
                const void* candidate_data = physical->Candidates().data();
                const void* validity_data = physical->Validity().data();
                const auto candidate_alias =
                    aliases & 1 ? physical->CandidateAlias() : nullptr;
                const auto validity_alias =
                    aliases & 2 ? physical->ValidityAlias() : nullptr;
                auto query = std::make_shared<exec::QueryContext>(
                    DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
                exec::ExecContext execution(query.get());
                exec::EvalCtx context(&execution);
                for (size_t begin = 0; begin < n;) {
                    const size_t size =
                        one_shot ? n : std::min(batch, n - begin);
                    TargetBitmap input(size, true);
                    if (begin <= 64 && 64 < begin + size) {
                        input.reset(64 - begin);
                    }
                    auto saved = input.clone();
                    context.set_bitmap_input(std::move(input));
                    VectorPtr output;
                    physical->Eval(context, output);
                    auto column =
                        std::dynamic_pointer_cast<ColumnVector>(output);
                    ASSERT_NE(column, nullptr);
                    ASSERT_EQ(column->size(), size);
                    const bool consumed = one_shot && aliases == 0;
                    EXPECT_EQ(column->GetRawData() == candidate_data, consumed);
                    EXPECT_EQ(column->GetValidRawData() == validity_data,
                              consumed);
                    TargetBitmapView hits(column->GetRawData(), size);
                    TargetBitmapView result_valid(column->GetValidRawData(),
                                                  size);
                    for (size_t i = 0; i < size; ++i) {
                        const size_t row = begin + i;
                        EXPECT_EQ(context.get_bitmap_input()[i], saved[i]);
                        EXPECT_EQ(result_valid[i], valid[row]);
                        EXPECT_EQ(hits[i],
                                  saved[i] && valid[row] && oracle(rows[row]))
                            << row;
                    }
                    if (!consumed) {
                        // In particular, rechecking false positives and applying
                        // the external mask must not corrupt a later batch or an
                        // observer sharing only one of the two cache entries.
                        ASSERT_EQ(physical->Candidates().size(), n);
                        ASSERT_EQ(physical->Validity().size(), n);
                        for (size_t row = 0; row < n; ++row) {
                            EXPECT_EQ(physical->Candidates()[row],
                                      seeded_candidates[row]);
                            EXPECT_EQ(physical->Validity()[row], valid[row]);
                        }
                    }
                    begin += size;
                }
                ASSERT_EQ(physical->GetNextBatchSize(), 0);
                context.clear_bitmap_input();
                VectorPtr exhausted;
                physical->Eval(context, exhausted);
                EXPECT_EQ(exhausted, nullptr);
                exec::OffsetVector offsets{258, 0, 193, 64, 128, 63, 258};
                context.set_offset_input(&offsets);
                VectorPtr gathered;
                physical->Eval(context, gathered);
                auto taken = std::dynamic_pointer_cast<ColumnVector>(gathered);
                ASSERT_NE(taken, nullptr);
                ASSERT_EQ(taken->size(), offsets.size());
                TargetBitmapView offset_hits(taken->GetRawData(),
                                             offsets.size());
                TargetBitmapView offset_valid(taken->GetValidRawData(),
                                              offsets.size());
                for (size_t i = 0; i < offsets.size(); ++i) {
                    const size_t row = offsets[i];
                    EXPECT_EQ(offset_valid[i], valid[row]);
                    if (valid[row]) {
                        EXPECT_EQ(offset_hits[i], oracle(rows[row]));
                    }
                }
                context.set_offset_input(nullptr);
                ASSERT_EQ(physical->GetNextBatchSize(), 0);
                physical->Eval(context, exhausted);
                EXPECT_EQ(exhausted, nullptr);
                // Seeded candidates and later Take both bypass FM phase 1.
                EXPECT_EQ(calls->prepare, 0u);
                EXPECT_EQ(calls->execute, 0u);
                EXPECT_EQ(calls->legacy_pattern, 0u);
                EXPECT_EQ(calls->like_pattern, 0u);
            }
        }
    }
}

TEST_P(FmIndexPatternExpressionTest,
       ExecutorOneShotConjunctionSkipExhaustsCandidatesAndAllowsTake) {
    const auto op = GetParam();
    constexpr size_t n = 259;
    constexpr size_t batch = 128;
    const std::string pattern = op == proto::plan::Match
                                    ? R"(%foo%SPARSE\_TOKEN%)"
                                    : "foo.*SPARSE_TOKEN";
    std::vector<std::string> rows(n, "foo " + std::string(1000, 'x'));
    rows[63] += " SPARSE_TOKEN";
    rows.back() += " SPARSE_TOKEN";
    std::vector<uint8_t> validity((n + 7) / 8, 0xff);
    validity[128 / 8] &= static_cast<uint8_t>(~(1u << (128 % 8)));
    std::vector<int64_t> ids(n);
    for (size_t row = 0; row < n; ++row) {
        ids[row] = row;
    }
    auto calls = std::make_shared<RegexRoutingCounts>();
    auto loaded = LoadSealedFMMatch(
        rows, {}, true, validity.data(), &ids, true, ObserveFMReader(calls, 0));
    auto numeric_field =
        std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
    numeric_field->FillFieldData(ids.data(), ids.size());
    auto opened = test::expr_index::BuildIndex(
        loaded.int_id, DataType::INT64, "STL_SORT", {numeric_field});
    test::expr_index::InstallIndex(
        *loaded.segment, loaded.int_id, DataType::INT64, std::move(opened));
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    proto::plan::GenericValue upper;
    upper.set_int64_val(0);
    auto numeric = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(loaded.int_id, DataType::INT64),
        proto::plan::LessThan,
        upper);
    auto match =
        MakeMatchTypedExpr(loaded.schema, loaded.varchar_id, pattern, true, op);
    auto typed = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, numeric, match);
    auto query = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
    query->set_enable_expr_cache(false);
    exec::ExecContext execution(query.get());
    auto compiled = exec::CompileExpressions({typed}, &execution, {}, false);
    auto parent =
        std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(compiled[0]);
    ASSERT_NE(parent, nullptr);
    ASSERT_EQ(parent->GetInputsRef().size(), 2u);
    parent->Reorder({0, 1});
    auto physical = parent->GetInputsRef()[1];
    if (auto adapter =
            std::dynamic_pointer_cast<exec::RawExprCacheAdapter>(physical)) {
        physical = adapter->wrapped_expr();
    }
    auto leaf =
        std::dynamic_pointer_cast<exec::PhyUnaryRangeFilterExpr>(physical);
    ASSERT_NE(leaf, nullptr);
    ASSERT_TRUE(parent->CanExecuteAllAtOnce());
    ASSERT_EQ(leaf->GetExecPath(), exec::ExprExecPath::ScalarIndex);
    parent->SetExecuteAllAtOnce();
    exec::EvalCtx context(&execution);
    VectorPtr output;
    parent->Eval(context, output);
    auto column = std::dynamic_pointer_cast<ColumnVector>(output);
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(column->size(), n);
    EXPECT_TRUE(TargetBitmapView(column->GetRawData(), n).none());
    EXPECT_TRUE(TargetBitmapView(column->GetValidRawData(), n).all());
    EXPECT_TRUE(context.get_bitmap_input().empty());
    // The real AND executor skips its second leaf through MoveCursor().
    // Verify this before Eval so a missing completion cannot run FM phase 1.
    ASSERT_EQ(leaf->GetNextBatchSize(), 0);
    VectorPtr exhausted;
    leaf->Eval(context, exhausted);
    EXPECT_EQ(exhausted, nullptr);
    EXPECT_EQ(calls->prepare, 0u);
    EXPECT_EQ(calls->execute, 0u);
    EXPECT_EQ(calls->legacy_pattern, 0u);
    EXPECT_EQ(calls->like_pattern, 0u);

    exec::OffsetVector offsets{258, 0, 128, 63, 258};
    context.set_offset_input(&offsets);
    VectorPtr gathered;
    leaf->Eval(context, gathered);
    auto taken = std::dynamic_pointer_cast<ColumnVector>(gathered);
    ASSERT_NE(taken, nullptr);
    ASSERT_EQ(taken->size(), offsets.size());
    TargetBitmapView offset_hits(taken->GetRawData(), offsets.size());
    TargetBitmapView offset_valid(taken->GetValidRawData(), offsets.size());
    PartialRegexMatcher regex_oracle("foo.*SPARSE_TOKEN");
    LikePatternMatcher like_oracle(R"(%foo%SPARSE\_TOKEN%)");
    for (size_t i = 0; i < offsets.size(); ++i) {
        const size_t row = offsets[i];
        const bool row_valid = validity[row / 8] & (1u << (row % 8));
        EXPECT_EQ(offset_valid[i], row_valid);
        if (row_valid) {
            EXPECT_EQ(offset_hits[i],
                      op == proto::plan::Match ? like_oracle(rows[row])
                                               : regex_oracle(rows[row]));
        }
    }
    context.set_offset_input(nullptr);
    ASSERT_EQ(leaf->GetNextBatchSize(), 0);
    leaf->Eval(context, exhausted);
    EXPECT_EQ(exhausted, nullptr);
    EXPECT_EQ(calls->prepare, 0u);
    EXPECT_EQ(calls->execute, 0u);
    EXPECT_EQ(calls->legacy_guard, 0u);
    EXPECT_EQ(calls->legacy_pattern, 0u);
    EXPECT_EQ(calls->like_pattern, 0u);
}

TEST(FmIndexExpressionTest, ExecutorRegexCountProofsDoNotRequestSuffixStats) {
    constexpr size_t n = 1027;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, "COMMON foo " + std::string(1000, 'x'));
    auto counts = std::make_shared<RegexRoutingCounts>();
    auto loaded = LoadSealedFMMatch(
        rows, {}, false, nullptr, nullptr, true, ObserveFMReader(counts, 0));
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (const std::string pattern : {"COMMON.*foo", "ABSENT_54191.*"}) {
        *counts = RegexRoutingCounts{};
        auto typed = MakeMatchTypedExpr(loaded.schema,
                                        loaded.varchar_id,
                                        pattern,
                                        false,
                                        proto::plan::RegexMatch);
        auto query = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
        query->set_enable_expr_cache(false);
        exec::ExecContext execution(query.get());
        auto compiled =
            exec::CompileExpressions({typed}, &execution, {}, false);
        exec::EvalCtx context(&execution);
        context.set_bitmap_input(TargetBitmap(batch, true));
        size_t requests = 0;
        exec::EvalCtx::RegexWorkProvider provider =
            [&](size_t, size_t, size_t) -> std::optional<RegexIndexWorkStats> {
            ++requests;
            return std::nullopt;
        };
        exec::EvalCtx::ScopedRegexWorkProvider scope(context, &provider);
        VectorPtr output;
        compiled[0]->Eval(context, output);
        ASSERT_NE(output, nullptr);
        EXPECT_EQ(requests, 0u);
        EXPECT_EQ(counts->prepare, 1u);
        EXPECT_EQ(counts->execute, pattern == "ABSENT_54191.*" ? 1u : 0u);
        ASSERT_NE(counts->plan, nullptr);
        EXPECT_EQ(counts->plan->empty_result, pattern == "ABSENT_54191.*");
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexClusteredNumericPrefixAvoidsLocate) {
    constexpr size_t n = 32771;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, std::string(28, 'x'));
    for (size_t row = 0; row < n; row += 512) {
        rows[row].replace(0, 11, "MARKER54191");
    }
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (size_t prefix_begin : {size_t{0}, batch}) {
        std::vector<int64_t> ids(n);
        for (size_t row = 0; row < n; ++row) {
            ids[row] = (row + n - prefix_begin) % n;
        }
        auto calls = std::make_shared<RegexRoutingCounts>();
        auto loaded = LoadSealedFMMatch(
            rows, {}, false, nullptr, &ids, true, ObserveFMReader(calls, 0));
        auto numeric_field =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        numeric_field->FillFieldData(ids.data(), ids.size());
        auto opened = test::expr_index::BuildIndex(
            loaded.int_id, DataType::INT64, "STL_SORT", {numeric_field});
        test::expr_index::InstallIndex(
            *loaded.segment, loaded.int_id, DataType::INT64, std::move(opened));
        for (size_t limit : {size_t{0}, size_t{16}, size_t{64}, n}) {
            for (bool one_shot : {false, true}) {
                SCOPED_TRACE("start=" + std::to_string(prefix_begin) +
                             "/limit=" + std::to_string(limit) +
                             "/oneShot=" + std::to_string(one_shot));
                *calls = RegexRoutingCounts{};
                proto::plan::GenericValue upper;
                upper.set_int64_val(limit);
                auto numeric = std::make_shared<expr::UnaryRangeFilterExpr>(
                    expr::ColumnInfo(loaded.int_id, DataType::INT64),
                    proto::plan::LessThan,
                    upper);
                auto regex = MakeMatchTypedExpr(loaded.schema,
                                                loaded.varchar_id,
                                                "MARKER54191.*",
                                                false,
                                                proto::plan::RegexMatch);
                auto typed = std::make_shared<expr::LogicalBinaryExpr>(
                    expr::LogicalBinaryExpr::OpType::And, numeric, regex);
                auto query = std::make_shared<exec::QueryContext>(
                    DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
                query->set_enable_expr_cache(false);
                exec::ExecContext execution(query.get());
                auto compiled =
                    exec::CompileExpressions({typed}, &execution, {}, false);
                auto parent =
                    std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(
                        compiled[0]);
                ASSERT_NE(parent, nullptr);
                parent->Reorder({0, 1});
                if (one_shot) {
                    ASSERT_TRUE(parent->CanExecuteAllAtOnce());
                    parent->SetExecuteAllAtOnce();
                }
                exec::EvalCtx context(&execution);
                for (size_t begin = 0; begin < n;) {
                    const auto size = one_shot ? n : std::min(batch, n - begin);
                    VectorPtr output;
                    parent->Eval(context, output);
                    auto column =
                        std::dynamic_pointer_cast<ColumnVector>(output);
                    ASSERT_NE(column, nullptr);
                    ASSERT_EQ(column->size(), size);
                    TargetBitmapView hits(column->GetRawData(), size);
                    for (size_t i = 0; i < size; ++i) {
                        EXPECT_EQ(hits[i],
                                  static_cast<size_t>(ids[begin + i]) < limit &&
                                      (begin + i) % 512 == 0);
                    }
                    EXPECT_TRUE(context.get_bitmap_input().empty());
                    EXPECT_FALSE(context.has_regex_work_provider());
                    begin += size;
                }
                EXPECT_EQ(calls->prepare, limit == 0 ? 0u : 1u);
                EXPECT_EQ(calls->execute, limit == n ? 1u : 0u);
                if (limit != 0) {
                    ASSERT_NE(calls->plan, nullptr);
                    EXPECT_EQ(calls->plan->occurrences, (n + 511) / 512);
                    EXPECT_TRUE(RegexIndexPassesLocateGuard(*calls->plan));
                }
            }
        }
    }
}

TEST(FmIndexExpressionTest, ExecutorRegexNumericStatsKeepUnknownScopesUnknown) {
    constexpr size_t n = 32771;
    constexpr size_t batch = 128;
    std::vector<std::string> rows(n, std::string(28, 'x'));
    std::vector<int64_t> ids(n);
    for (size_t row = 0; row < n; ++row) {
        ids[row] = row;
        if (row % 512 == 0) {
            rows[row].replace(0, 11, "MARKER54191");
        }
    }
    milvus::test::ExprBatchSizeGuard batch_guard(batch);
    for (const std::string scope : {"external_mask",
                                    "multiple_prefixes",
                                    "not_regex",
                                    "nullable_numeric"}) {
        SCOPED_TRACE(scope);
        const bool nullable = scope == "nullable_numeric";
        std::vector<uint8_t> numeric_validity((n + 7) / 8, 0xff);
        if (nullable) {
            for (size_t row : {size_t{0}, size_t{200}, size_t{512}}) {
                numeric_validity[row / 8] &=
                    static_cast<uint8_t>(~(1u << (row % 8)));
            }
        }
        auto calls = std::make_shared<RegexRoutingCounts>();
        auto loaded = LoadSealedFMMatch(rows,
                                        {},
                                        false,
                                        nullptr,
                                        &ids,
                                        true,
                                        ObserveFMReader(calls, 0),
                                        std::nullopt,
                                        nullable,
                                        numeric_validity.data());
        auto field =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, nullable);
        if (nullable) {
            field->FillFieldData(
                ids.data(), numeric_validity.data(), ids.size(), 0);
        } else {
            field->FillFieldData(ids.data(), ids.size());
        }
        auto opened = test::expr_index::BuildIndex(
            loaded.int_id, DataType::INT64, "STL_SORT", {field});
        test::expr_index::InstallIndex(
            *loaded.segment, loaded.int_id, DataType::INT64, std::move(opened));
        proto::plan::GenericValue upper;
        upper.set_int64_val(16);
        auto numeric = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(loaded.int_id, DataType::INT64),
            proto::plan::LessThan,
            upper);
        expr::TypedExprPtr regex = MakeMatchTypedExpr(loaded.schema,
                                                      loaded.varchar_id,
                                                      "MARKER54191.*",
                                                      false,
                                                      proto::plan::RegexMatch);
        if (scope == "not_regex") {
            regex = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, regex);
        }
        expr::TypedExprPtr typed = numeric;
        if (scope == "multiple_prefixes") {
            proto::plan::GenericValue all_rows;
            all_rows.set_int64_val(n);
            auto second = std::make_shared<expr::UnaryRangeFilterExpr>(
                expr::ColumnInfo(loaded.int_id, DataType::INT64),
                proto::plan::LessThan,
                all_rows);
            typed = std::make_shared<expr::LogicalBinaryExpr>(
                expr::LogicalBinaryExpr::OpType::And, typed, second);
        }
        typed = std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, typed, regex);
        auto query = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
        query->set_enable_expr_cache(false);
        exec::ExecContext execution(query.get());
        auto compiled =
            exec::CompileExpressions({typed}, &execution, {}, false);
        auto parent =
            std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(compiled[0]);
        ASSERT_NE(parent, nullptr);
        std::vector<size_t> order(parent->GetInputsRef().size());
        for (size_t i = 0; i < order.size(); ++i) {
            order[i] = i;
        }
        parent->Reorder(order);
        exec::EvalCtx context(&execution);
        for (size_t begin = 0; begin < n; begin += batch) {
            const auto size = std::min(batch, n - begin);
            if (scope == "external_mask") {
                context.set_bitmap_input(TargetBitmap(size, true));
            }
            VectorPtr output;
            parent->Eval(context, output);
            auto column = std::dynamic_pointer_cast<ColumnVector>(output);
            ASSERT_NE(column, nullptr);
            ASSERT_EQ(column->size(), size);
            TargetBitmapView hits(column->GetRawData(), size);
            TargetBitmapView valid(column->GetValidRawData(), size);
            for (size_t i = 0; i < size; ++i) {
                const size_t row = begin + i;
                const bool numeric_valid =
                    numeric_validity[row / 8] & (1u << (row % 8));
                const bool regex_match =
                    (scope == "not_regex") != (row % 512 == 0);
                const bool expected_valid = numeric_valid || !regex_match;
                EXPECT_EQ(valid[i], expected_valid) << row;
                if (expected_valid) {
                    EXPECT_EQ(hits[i], numeric_valid && row < 16 && regex_match)
                        << row;
                }
            }
            EXPECT_FALSE(context.has_regex_work_provider());
        }
        // These narrow unsupported scopes retain the existing unknown-density
        // estimate. Their results stay exact; no whole-cache statistics are borrowed.
        EXPECT_EQ(calls->prepare, 1u);
        EXPECT_EQ(calls->execute, 1u);
    }
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
        for (bool one_shot : {false, true}) {
            manager.Clear();
            for (bool cache_hit : {false, true}) {
                *calls = RegexRoutingCounts{};
                auto query_context = std::make_shared<exec::QueryContext>(
                    DEAFULT_QUERY_ID, loaded.segment.get(), n, MAX_TIMESTAMP);
                query_context->set_enable_expr_cache(false);
                exec::ExecContext exec_context(query_context.get());
                auto compiled = exec::CompileExpressions(
                    {expression}, &exec_context, {}, false);
                auto adapter =
                    std::dynamic_pointer_cast<exec::RawExprCacheAdapter>(
                        compiled[0]);
                ASSERT_NE(adapter, nullptr);
                EXPECT_EQ(adapter->wrapped_expr()->GetExecPath(),
                          exec::ExprExecPath::ScalarIndex);
                if (one_shot) {
                    ASSERT_TRUE(compiled[0]->CanExecuteAllAtOnce());
                    compiled[0]->SetExecuteAllAtOnce();
                }
                exec::ExprResCacheManager::Key key{
                    loaded.segment->get_segment_id(),
                    adapter->wrapped_expr()->GetSignatureForRawExprCache()};
                exec::EvalCtx context(&exec_context);
                for (size_t begin = 0; begin < n;
                     begin += one_shot ? n : batch_size) {
                    const size_t size =
                        one_shot ? n : std::min(batch_size, n - begin);
                    VectorPtr output;
                    compiled[0]->Eval(context, output);
                    auto result =
                        std::dynamic_pointer_cast<ColumnVector>(output);
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
