// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/RegexQuery.h"
#include "common/Schema.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "expr/ITypeExpr.h"
#include "index/contracts/query/INgramReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

namespace milvus::test {
namespace {

struct Pattern {
    const char* name;
    const char* term;
    const char* like;
    proto::plan::OpType expression_op;
    index::PatternOp reader_op;
};

struct Measurement {
    double microseconds;
    size_t matches;
};

template <typename Query>
Measurement
Measure(Query&& query) {
    constexpr int warmups = 3;
    constexpr int iterations = 5;
    for (int i = 0; i < warmups; ++i) {
        (void)query();
    }
    size_t count = 0;
    const auto begin = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        count += query();
    }
    const auto duration = std::chrono::duration<double, std::micro>(
                              std::chrono::steady_clock::now() - begin)
                              .count();
    return {duration / iterations, count / iterations};
}

struct NgramBenchmarkData {
    std::vector<std::string> rows;
    SchemaPtr schema;
    FieldId field_id;
    expr_index::RawFieldFiles raw_files;
    segcore::SegmentSealedUPtr segment;
    // The segment's cache slot owns this reader through every measurement.
    const index::INgramReader* ngram;
    index::IIndexReaderBasePtr inverted;

    NgramBenchmarkData() {
        constexpr size_t count = 10000;
        std::mt19937 random(42);
        std::uniform_int_distribution<int> character('a', 'z');
        std::uniform_int_distribution<int> length(40, 120);
        rows.reserve(count);
        for (size_t row = 0; row < count; ++row) {
            std::string value;
            const auto bytes = length(random);
            value.reserve(bytes);
            for (int i = 0; i < bytes; ++i) {
                value.push_back(static_cast<char>(character(random)));
            }
            rows.push_back(std::move(value));
        }
        schema = std::make_shared<Schema>();
        field_id = schema->AddDebugField("ngram_benchmark", DataType::VARCHAR);
        auto field = expr_index::StringField(rows);
        segment = segcore::CreateSealedSegment(schema);
        auto raw_info = raw_files.Prepare(field_id, {field});
        segment->LoadFieldData(raw_info);
        auto opened = expr_index::BuildIndex(
            field_id,
            DataType::VARCHAR,
            index::NGRAM_INDEX_TYPE,
            {field},
            {{index::MIN_GRAM, 2}, {index::MAX_GRAM, 4}});
        ngram = dynamic_cast<const index::INgramReader*>(opened.reader.get());
        AssertInfo(ngram != nullptr,
                   "benchmark requires a production NGRAM reader");
        expr_index::InstallIndex(
            *segment, field_id, DataType::VARCHAR, std::move(opened));
        inverted = expr_index::BuildIndex(field_id,
                                          DataType::VARCHAR,
                                          index::INVERTED_INDEX_TYPE,
                                          {field})
                       .reader;
    }

    TargetBitmap
    Execute(const Pattern& pattern) const {
        proto::plan::GenericValue literal;
        literal.set_string_val(pattern.term);
        auto expression = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field_id, DataType::VARCHAR),
            pattern.expression_op,
            literal);
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           expression);
        return query::ExecuteQueryExpr(
            plan, segment.get(), rows.size(), MAX_TIMESTAMP);
    }

    TargetBitmap
    Exact(const Pattern& pattern) const {
        LikePatternMatcher matcher(pattern.like);
        TargetBitmap result(rows.size(), false);
        for (size_t row = 0; row < rows.size(); ++row) {
            result[row] = matcher(rows[row]);
        }
        return result;
    }

    void
    Check(const TargetBitmap& actual, const TargetBitmap& expected) const {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t row = 0; row < rows.size(); ++row) {
            ASSERT_EQ(actual[row], expected[row]) << "row=" << row;
        }
    }
};

void
PrintMeasurement(const char* name, const Measurement& measurement) {
    std::cout << "  " << std::left << std::setw(28) << name << std::right
              << std::setw(12) << std::fixed << std::setprecision(0)
              << measurement.microseconds << " us" << std::setw(12)
              << measurement.matches << " matches\n";
}

}  // namespace

// Benchmark suites belong to all_tests, outside the index contract target.
TEST(NgramBenchmark, NgramVsTantivyVsBruteForce) {
    NgramBenchmarkData data;
    const auto* inverted =
        dynamic_cast<const index::IPatternMatchReader*>(data.inverted.get());
    ASSERT_NE(inverted, nullptr);
    const std::vector<Pattern> patterns = {
        {"LIKE %ab%cd%ef%",
         "%ab%cd%ef%",
         "%ab%cd%ef%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"LIKE %ab%cd%",
         "%ab%cd%",
         "%ab%cd%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"LIKE abc%xyz%",
         "abc%xyz%",
         "abc%xyz%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"PREFIX abc",
         "abc",
         "abc%",
         proto::plan::PrefixMatch,
         index::PatternOp::PrefixMatch},
        {"INNER hello",
         "hello",
         "%hello%",
         proto::plan::InnerMatch,
         index::PatternOp::InnerMatch},
        {"SUFFIX xyz",
         "xyz",
         "%xyz",
         proto::plan::PostfixMatch,
         index::PatternOp::PostfixMatch},
    };
    for (const auto& pattern : patterns) {
        SCOPED_TRACE(pattern.name);
        const auto expected = data.Exact(pattern);
        ASSERT_NO_FATAL_FAILURE(data.Check(data.Execute(pattern), expected));
        ASSERT_NO_FATAL_FAILURE(data.Check(
            inverted->PatternMatch(pattern.term, pattern.reader_op), expected));
        PatternMatchTranslator translator;
        RegexMatcher regex(translator(std::string(pattern.like)));
        LikePatternMatcher like(pattern.like);
        const auto scan = [&](auto& matcher) {
            size_t count = 0;
            for (const auto& row : data.rows) count += matcher(row);
            return count;
        };
        std::cout << "\n" << pattern.name << "\n";
        const auto regex_result = Measure([&] { return scan(regex); });
        const auto like_result = Measure([&] { return scan(like); });
        const auto inverted_result = Measure([&] {
            return inverted->PatternMatch(pattern.term, pattern.reader_op)
                .count();
        });
        const auto ngram_result =
            Measure([&] { return data.Execute(pattern).count(); });
        EXPECT_EQ(regex_result.matches, expected.count());
        EXPECT_EQ(like_result.matches, expected.count());
        EXPECT_EQ(inverted_result.matches, expected.count());
        EXPECT_EQ(ngram_result.matches, expected.count());
        PrintMeasurement("RE2 scan", regex_result);
        PrintMeasurement("LikePatternMatcher scan", like_result);
        PrintMeasurement("Tantivy reader", inverted_result);
        PrintMeasurement("NGRAM expression with recheck", ngram_result);
        if (data.ngram->CanHandle(pattern.term, pattern.reader_op)) {
            TargetBitmap candidates(data.rows.size(), true);
            data.ngram->Candidates(pattern.term, pattern.reader_op, candidates);
            std::cout << "  phase 1 candidates: " << candidates.count() << "/"
                      << data.rows.size() << "\n";
        }
    }
}

TEST(NgramBenchmark, NgramFilteringEffectiveness) {
    NgramBenchmarkData data;
    const std::vector<Pattern> patterns = {
        {"three rare trigrams",
         "%xyz%def%ghi%",
         "%xyz%def%ghi%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"long literal",
         "%abcdef%",
         "%abcdef%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"rare prefix",
         "qzx",
         "qzx%",
         proto::plan::PrefixMatch,
         index::PatternOp::PrefixMatch},
        {"short LIKE segments",
         "%a%b%c%",
         "%a%b%c%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"mixed segment lengths",
         "%a%bc%",
         "%a%bc%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"only wildcards",
         "_%_%_%",
         "_%_%_%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"short suffix",
         "a",
         "%a",
         proto::plan::PostfixMatch,
         index::PatternOp::PostfixMatch},
        {"single bigram LIKE",
         "%ab%",
         "%ab%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"single bigram inner",
         "ab",
         "%ab%",
         proto::plan::InnerMatch,
         index::PatternOp::InnerMatch},
        {"common inner",
         "th",
         "%th%",
         proto::plan::InnerMatch,
         index::PatternOp::InnerMatch},
        {"two bigrams",
         "%ab%cd%",
         "%ab%cd%",
         proto::plan::Match,
         index::PatternOp::Match},
        {"rare four gram",
         "qzxw",
         "%qzxw%",
         proto::plan::InnerMatch,
         index::PatternOp::InnerMatch},
        {"two rare four grams",
         "%mnop%qrst%",
         "%mnop%qrst%",
         proto::plan::Match,
         index::PatternOp::Match},
    };
    for (const auto& pattern : patterns) {
        SCOPED_TRACE(pattern.name);
        const auto expected = data.Exact(pattern);
        ASSERT_NO_FATAL_FAILURE(data.Check(data.Execute(pattern), expected));
        std::cout << "\n" << pattern.name << ": " << pattern.like << "\n";
        if (data.ngram->CanHandle(pattern.term, pattern.reader_op)) {
            TargetBitmap candidates(data.rows.size(), true);
            data.ngram->Candidates(pattern.term, pattern.reader_op, candidates);
            for (size_t row = 0; row < data.rows.size(); ++row) {
                ASSERT_FALSE(expected[row] && !candidates[row])
                    << "row=" << row;
            }
            std::cout << "  phase 1 candidates: " << candidates.count() << "/"
                      << data.rows.size() << "\n";
            const auto result =
                Measure([&] { return data.Execute(pattern).count(); });
            EXPECT_EQ(result.matches, expected.count());
            PrintMeasurement("NGRAM expression with recheck", result);
        } else {
            std::cout << "  NGRAM declined; expression uses scan\n";
        }
        LikePatternMatcher matcher(pattern.like);
        PrintMeasurement("LikePatternMatcher scan", Measure([&] {
                             size_t count = 0;
                             for (const auto& row : data.rows)
                                 count += matcher(row);
                             return count;
                         }));
    }
}

}  // namespace milvus::test
