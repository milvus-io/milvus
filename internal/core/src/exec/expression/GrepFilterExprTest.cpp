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

#include <arrow/filesystem/localfs.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "grep/GrepTestUtils.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "nlohmann/json.hpp"
#include "pb/plan.pb.h"
#include "query/PlanImpl.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/test_utils/TextMatchTestUtils.h"

// Tests of grep(field, arguments) as a filter predicate, exercised through
// Retrieve so that plan construction, the filter operator, visibility and
// limit-bounded evaluation are all on the path.

namespace milvus::segcore {
namespace {

constexpr int64_t kPkField = 100;
constexpr int64_t kTextField = 101;

using Rows = std::vector<int64_t>;

class GrepFilterExprTest
    : public ::testing::TestWithParam<std::tuple<bool, DataType>> {
 protected:
    SchemaPtr schema;
    SegcoreConfig config;
    std::shared_ptr<storage::LocalDirectory> binlogs;
    std::shared_ptr<storage::LocalDirectory> lob_files;
    std::unique_ptr<SegmentInternalInterface> segment;
    std::vector<std::string> texts_;
    std::vector<bool> valid_;

    bool
    IsGrowing() const {
        return std::get<0>(GetParam());
    }

    DataType
    TextType() const {
        return std::get<1>(GetParam());
    }

    void
    SetUp() override {
        schema = std::make_shared<Schema>();
        schema->AddField(FieldMeta(FieldName("pk"),
                                   FieldId(kPkField),
                                   DataType::INT64,
                                   false,
                                   std::nullopt));
        schema->set_primary_field_id(FieldId(kPkField));
        std::map<std::string, std::string> params;
        schema->AddField(FieldMeta(FieldName("text"),
                                   FieldId(kTextField),
                                   TextType(),
                                   65536,
                                   true,
                                   false,
                                   false,
                                   params,
                                   std::nullopt));
        config = SegcoreConfig::default_config();
        config.set_chunk_rows(64);
    }

    // Primary key == row offset == insert timestamp in this fixture.
    // sealed_chunk_rows splits the sealed text column into chunks of the given
    // sizes. encode_text = false stores `texts` as they are, for values that
    // are already LOB-encoded.
    void
    Load(const std::vector<std::string>& texts,
         const std::vector<bool>& valid,
         const std::vector<int64_t>& sealed_chunk_rows = {},
         bool encode_text = true) {
        texts_ = texts;
        valid_ = valid;
        segment.reset();
        auto data = text_test::DataGen(schema, texts.size());
        binlogs = data.binlogs;
        auto* field = data.raw_->mutable_fields_data(1);
        for (size_t i = 0; i < texts.size(); ++i) {
            std::string value = texts[i];
            if (encode_text && !IsGrowing() && TextType() == DataType::TEXT) {
                auto encoded =
                    milvus_storage::lob_column::EncodeInlineText(value);
                value.assign(reinterpret_cast<const char*>(encoded.data()),
                             encoded.size());
            }
            field->mutable_scalars()->mutable_string_data()->set_data(i, value);
            MutableFieldDataRowValidData(field)->Set(i, valid[i]);
        }
        if (IsGrowing()) {
            auto growing =
                CreateGrowingSegment(schema, empty_index_meta, 0, config);
            auto offset = growing->PreInsert(texts.size());
            growing->Insert(offset,
                            texts.size(),
                            data.row_ids_.data(),
                            data.timestamps_.data(),
                            data.owner);
            segment = std::move(growing);
        } else {
            if (sealed_chunk_rows.empty()) {
                segment =
                    text_test::CreateSealedWithFieldDataLoaded(schema, data);
            } else {
                auto manager = storage::CreateChunkManager(
                    text_test::get_default_local_storage_config());
                auto info =
                    text_test::PrepareInsertBinlog(1, 2, 3, data, manager);
                info.field_infos.erase(kTextField);
                std::vector<FieldDataPtr> batches;
                int64_t begin = 0;
                const auto& stored = field->scalars().string_data().data();
                for (auto count : sealed_chunk_rows) {
                    ASSERT_LE(begin + count, texts.size());
                    auto native = storage::CreateFieldData(
                        TextType(), DataType::NONE, true);
                    std::vector<std::string> values(
                        stored.begin() + begin, stored.begin() + begin + count);
                    std::vector<uint8_t> bitmap((count + 7) / 8, 0);
                    for (int64_t i = 0; i < count; ++i) {
                        if (valid[begin + i])
                            bitmap[i / 8] |= uint8_t{1} << (i % 8);
                    }
                    native->FillFieldData(
                        values.data(), bitmap.data(), count, 0);
                    batches.push_back(std::move(native));
                    begin += count;
                }
                ASSERT_EQ(begin, texts.size());
                text_test::AddBinlog(
                    info, binlogs->Path(), kTextField, batches, manager);
                auto sealed = CreateSealedSegment(schema, empty_index_meta);
                sealed->LoadFieldData(info);
                segment = std::move(sealed);
                ASSERT_EQ(segment->num_chunk_data(FieldId(kTextField)),
                          sealed_chunk_rows.size());
            }
            if (TextType() == DataType::TEXT) {
                dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())
                    ->SetTextLobPathForTesting(FieldId(kTextField),
                                               binlogs->Path() + "/lobs");
            }
        }
    }

    void
    Load(const std::vector<std::string>& texts) {
        Load(texts, std::vector<bool>(texts.size(), true));
    }

    // Sealed TEXT only: values longer than 32 bytes are written to LOB files
    // and stored as references; shorter ones stay inline.
    void
    LoadLobText(const std::vector<std::string>& texts,
                const std::vector<bool>& valid,
                const std::vector<int64_t>& sealed_chunk_rows = {}) {
        using namespace milvus_storage::lob_column;
        auto directory = storage::LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "grep-predicate-lob-XXXXXX",
            "Grep predicate LOB test");
        LobColumnConfig lob;
        lob.lob_base_path = directory->Path() + "/lobs";
        lob.field_id = kTextField;
        lob.inline_threshold = 32;
        auto manager = LobColumnManager::Create(
            std::make_shared<arrow::fs::LocalFileSystem>(), lob);
        ASSERT_TRUE(manager.ok()) << manager.status();
        auto writer = manager.ValueOrDie()->CreateWriter();
        ASSERT_TRUE(writer.ok()) << writer.status();
        auto encoded = writer.ValueOrDie()->WriteBatch(texts);
        ASSERT_TRUE(encoded.ok()) << encoded.status();
        auto files = writer.ValueOrDie()->Close();
        ASSERT_TRUE(files.ok()) << files.status();
        std::vector<std::string> stored;
        for (const auto& value : encoded.ValueOrDie()) {
            stored.emplace_back(reinterpret_cast<const char*>(value.data()),
                                value.size());
        }
        Load(stored, valid, sealed_chunk_rows, false);
        // The oracle works on the logical values.
        texts_ = texts;
        dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get())
            ->SetTextLobPathForTesting(FieldId(kTextField), lob.lob_base_path);
        lob_files = std::move(directory);
    }

    // Sealed only. The probe counts NGRAM posting queries.
    std::shared_ptr<NgramProbe>
    AttachNgram() {
        std::vector<uint8_t> bitmap((texts_.size() + 7) / 8, 0);
        for (size_t i = 0; i < valid_.size(); ++i) {
            if (valid_[i])
                bitmap[i / 8] |= uint8_t{1} << (i % 8);
        }
        auto field = std::make_shared<FieldData<std::string>>(TextType(), true);
        static_cast<FieldDataBase&>(*field).FillFieldData(
            texts_.data(), bitmap.data(), texts_.size(), 0);
        auto opened = test::expr_index::BuildIndex(
            FieldId(kTextField),
            TextType(),
            "NGRAM",
            {field},
            Config{{"min_gram", 3}, {"max_gram", 4}},
            DataType::NONE,
            false,
            false);
        auto probe = std::make_shared<NgramProbe>();
        test::expr_index::DecorateIndex(
            opened, [probe](index::IIndexReaderBasePtr reader) {
                return std::make_unique<ObservedNgramReader>(std::move(reader),
                                                             probe);
            });
        test::expr_index::InstallIndex(
            *dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get()),
            FieldId(kTextField),
            TextType(),
            std::move(opened));
        return probe;
    }

    // ---- predicate builders ------------------------------------------------

    proto::plan::Expr
    Grep(const std::vector<std::string>& patterns,
         uint32_t pattern_kind = 1,
         uint32_t case_mode = 0,
         uint32_t boundary = 0,
         bool select_none = false) const {
        proto::plan::Expr expr;
        auto* grep = expr.mutable_grep_expr();
        auto* column = grep->mutable_column_info();
        column->set_field_id(kTextField);
        column->set_data_type(static_cast<proto::schema::DataType>(TextType()));
        column->set_nullable(true);
        for (const auto& pattern : patterns) {
            grep->add_patterns(pattern);
        }
        grep->set_pattern_kind(pattern_kind);
        grep->set_case_mode(case_mode);
        grep->set_boundary(boundary);
        grep->set_select_none(select_none);
        return expr;
    }

    proto::plan::Expr
    Grep(const std::string& literal) const {
        return Grep(std::vector<std::string>{literal});
    }

    static proto::plan::Expr
    Binary(proto::plan::BinaryExpr_BinaryOp op,
           const proto::plan::Expr& left,
           const proto::plan::Expr& right) {
        proto::plan::Expr expr;
        auto* binary = expr.mutable_binary_expr();
        binary->set_op(op);
        *binary->mutable_left() = left;
        *binary->mutable_right() = right;
        return expr;
    }

    static proto::plan::Expr
    And(const proto::plan::Expr& left, const proto::plan::Expr& right) {
        return Binary(proto::plan::BinaryExpr_BinaryOp_LogicalAnd, left, right);
    }

    static proto::plan::Expr
    Or(const proto::plan::Expr& left, const proto::plan::Expr& right) {
        return Binary(proto::plan::BinaryExpr_BinaryOp_LogicalOr, left, right);
    }

    static proto::plan::Expr
    Not(const proto::plan::Expr& child) {
        proto::plan::Expr expr;
        auto* unary = expr.mutable_unary_expr();
        unary->set_op(proto::plan::UnaryExpr_UnaryOp_Not);
        *unary->mutable_child() = child;
        return expr;
    }

    static proto::plan::Expr
    Pk(proto::plan::OpType op, int64_t value) {
        proto::plan::Expr expr;
        auto* range = expr.mutable_unary_range_expr();
        auto* column = range->mutable_column_info();
        column->set_field_id(kPkField);
        column->set_data_type(proto::schema::DataType::Int64);
        column->set_is_primary_key(true);
        range->set_op(op);
        range->mutable_value()->set_int64_val(value);
        return expr;
    }

    // ---- execution ---------------------------------------------------------

    std::unique_ptr<query::RetrievePlan>
    Plan(const proto::plan::Expr& predicate, int64_t limit) const {
        proto::plan::PlanNode plan_node;
        auto* query = plan_node.mutable_query();
        *query->mutable_predicates() = predicate;
        query->set_limit(limit);
        plan_node.add_output_field_ids(kPkField);
        auto parser = query::ProtoParser(schema);
        return parser.CreateRetrievePlan(plan_node);
    }

    struct Result {
        Rows rows;
        bool has_more = false;
    };

    Result
    Run(const query::RetrievePlan* plan,
        folly::CancellationToken token = folly::CancellationToken()) const {
        auto results = segment->Retrieve(
            nullptr, plan, 1L << 63, INT64_MAX, false, token, 0, 0);
        Result out;
        out.rows.assign(results->offset().begin(), results->offset().end());
        out.has_more = results->has_more_result();
        return out;
    }

    Result
    Run(const proto::plan::Expr& predicate, int64_t limit) const {
        auto plan = Plan(predicate, limit);
        return Run(plan.get());
    }

    // Every row the predicate selects, with grep evaluated in the filter
    // operator (no limit, hence no limit-bounded evaluation).
    Rows
    All(const proto::plan::Expr& predicate) const {
        auto plan = Plan(predicate, -1);
        EXPECT_EQ(plan->plan_node_->limit_predicate_, nullptr);
        return Run(plan.get()).rows;
    }

    // Rows whose value contains `literal`, computed without the matcher.
    Rows
    Containing(const std::string& literal) const {
        Rows rows;
        for (size_t i = 0; i < texts_.size(); ++i) {
            if (valid_[i] && texts_[i].find(literal) != std::string::npos) {
                rows.push_back(i);
            }
        }
        return rows;
    }

    static Rows
    Prefix(const Rows& rows, int64_t limit) {
        return Rows(rows.begin(),
                    rows.begin() + std::min<int64_t>(limit, rows.size()));
    }

    // Synthetic log lines: `hit` appears in every `period`-th row starting at
    // `first`.
    static std::vector<std::string>
    Corpus(int64_t rows,
           int64_t first,
           int64_t period,
           const std::string& hit = "timeout") {
        std::vector<std::string> texts;
        texts.reserve(rows);
        for (int64_t i = 0; i < rows; ++i) {
            std::string value = "request " + std::to_string(i) + " handled\n";
            if (i >= first && (i - first) % period == 0) {
                value += "upstream " + hit + " after 30s\n";
            }
            value += "done";
            texts.push_back(std::move(value));
        }
        return texts;
    }
};

INSTANTIATE_TEST_SUITE_P(SealedAndGrowing,
                         GrepFilterExprTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(DataType::VARCHAR,
                                                              DataType::TEXT)));

// ---- plan construction -----------------------------------------------------

TEST_P(GrepFilterExprTest, PlanTakesTopLevelGrepOutOnlyForPlainLimitQueries) {
    Load({"timeout", "ok"});
    const auto grep = Grep("timeout");
    const auto pk = Pk(proto::plan::OpType::GreaterEqual, 0);

    // Finite limit, grep at the root or directly under the root AND.
    EXPECT_NE(Plan(grep, 10)->plan_node_->limit_predicate_, nullptr);
    EXPECT_NE(Plan(And(pk, grep), 10)->plan_node_->limit_predicate_, nullptr);
    EXPECT_NE(
        Plan(And(grep, And(pk, Grep("ok"))), 10)->plan_node_->limit_predicate_,
        nullptr);

    // No limit: the filter operator evaluates everything.
    EXPECT_EQ(Plan(grep, -1)->plan_node_->limit_predicate_, nullptr);
    EXPECT_EQ(Plan(And(pk, grep), 0)->plan_node_->limit_predicate_, nullptr);

    // Under OR / NOT the predicate cannot be postponed.
    EXPECT_EQ(Plan(Or(pk, grep), 10)->plan_node_->limit_predicate_, nullptr);
    EXPECT_EQ(Plan(Not(grep), 10)->plan_node_->limit_predicate_, nullptr);
    EXPECT_EQ(Plan(And(pk, Or(grep, pk)), 10)->plan_node_->limit_predicate_,
              nullptr);

    // Nothing to take out.
    EXPECT_EQ(Plan(pk, 10)->plan_node_->limit_predicate_, nullptr);
}

TEST_P(GrepFilterExprTest, PlanKeepsGrepInFilterForCountAggregationAndOrder) {
    Load({"timeout", "ok"});
    auto build =
        [&](const std::function<void(proto::plan::QueryPlanNode*)>& mutate) {
            proto::plan::PlanNode plan_node;
            auto* query = plan_node.mutable_query();
            *query->mutable_predicates() = Grep("timeout");
            query->set_limit(10);
            mutate(query);
            plan_node.add_output_field_ids(kPkField);
            auto parser = query::ProtoParser(schema);
            return parser.CreateRetrievePlan(plan_node);
        };
    EXPECT_EQ(build([](auto* q) {
                  q->set_is_count(true);
              })->plan_node_->limit_predicate_,
              nullptr);
    EXPECT_EQ(build([](auto* q) {
                  q->add_group_by_field_ids(kPkField);
              })->plan_node_->limit_predicate_,
              nullptr);
    EXPECT_EQ(build([](auto* q) {
                  auto* order = q->add_order_by_fields();
                  order->set_field_id(kPkField);
              })->plan_node_->limit_predicate_,
              nullptr);
}

// ---- filter operator -------------------------------------------------------

TEST_P(GrepFilterExprTest, FilterSelectsExactlyMatchingRows) {
    Load(Corpus(300, 3, 7));
    EXPECT_EQ(All(Grep("timeout")), Containing("timeout"));
    EXPECT_EQ(All(Grep("no such text")), Rows{});
    // Every row has this line.
    EXPECT_EQ(All(Grep("handled")).size(), 300);
}

TEST_P(GrepFilterExprTest, MatchingIsLineOrientedAndHonoursOptions) {
    Load({"alpha\nbeta", "ALPHA", "alphabet", "beta alpha", "", "x\nalpha"});
    // Regex anchors apply to each line, not to the whole value.
    EXPECT_EQ(All(Grep({"^alpha$"}, /*pattern_kind=*/0)), (Rows{0, 5}));
    EXPECT_EQ(All(Grep({"^beta"}, 0)), (Rows{0, 3}));
    // A pattern cannot span lines.
    EXPECT_EQ(All(Grep({"alpha.beta"}, 0)), Rows{});
    // -i
    EXPECT_EQ(All(Grep({"alpha"}, 1, /*case_mode=*/1)), (Rows{0, 1, 2, 3, 5}));
    // -w
    EXPECT_EQ(All(Grep({"alpha"}, 1, 0, /*boundary=*/1)), (Rows{0, 3, 5}));
    // -x
    EXPECT_EQ(All(Grep({"alpha"}, 1, 0, /*boundary=*/2)), (Rows{0, 5}));
    // Several -e patterns are alternatives.
    EXPECT_EQ(All(Grep({"ALPHA", "x"}, 1)), (Rows{1, 5}));
    // -F treats metacharacters literally.
    EXPECT_EQ(All(Grep({"alpha.beta"}, 1)), Rows{});
}

TEST_P(GrepFilterExprTest, SelectNoneSkipsCompilation) {
    Load({"timeout", "ok"});
    // "[" is not a valid regex; a final "-m 0" never compiles it.
    EXPECT_EQ(All(Grep({"["}, 0, 0, 0, /*select_none=*/true)), Rows{});
    EXPECT_EQ(Run(Grep({"["}, 0, 0, 0, true), 10).rows, Rows{});
}

TEST_P(GrepFilterExprTest, InvalidRegexFailsTheRequest) {
    Load({"timeout", "ok"});
    EXPECT_ANY_THROW(All(Grep({"["}, 0)));
    EXPECT_ANY_THROW(Run(Grep({"["}, 0), 10));
}

TEST_P(GrepFilterExprTest, NullIsUnknownUnderNotAndOr) {
    Load({"timeout", "x", "ok", "timeout", "x"},
         {true, false, true, true, false});
    const auto grep = Grep("timeout");
    EXPECT_EQ(All(grep), (Rows{0, 3}));
    // NOT UNKNOWN is UNKNOWN: NULL rows are not selected.
    EXPECT_EQ(All(Not(grep)), (Rows{2}));
    // UNKNOWN OR TRUE is TRUE.
    EXPECT_EQ(All(Or(grep, Pk(proto::plan::OpType::Equal, 1))),
              (Rows{0, 1, 3}));
    EXPECT_EQ(All(Or(Not(grep), Pk(proto::plan::OpType::Equal, 4))),
              (Rows{2, 4}));
    // NOT (grep AND pk >= 3): row 0 fails pk, row 3 passes both, NULL rows
    // are FALSE AND UNKNOWN = FALSE for pk < 3 and UNKNOWN otherwise.
    EXPECT_EQ(All(Not(And(grep, Pk(proto::plan::OpType::GreaterEqual, 3)))),
              (Rows{0, 1, 2}));
}

TEST_P(GrepFilterExprTest, ComposesWithOtherPredicates) {
    Load(Corpus(200, 0, 5));
    const auto hits = Containing("timeout");
    Rows expected;
    for (auto row : hits) {
        if (row >= 100)
            expected.push_back(row);
    }
    const auto pk = Pk(proto::plan::OpType::GreaterEqual, 100);
    EXPECT_EQ(All(And(pk, Grep("timeout"))), expected);
    EXPECT_EQ(All(And(Grep("timeout"), pk)), expected);
    // Two grep items.
    EXPECT_EQ(All(And(Grep("timeout"), Grep("request 1"))).size(), [&] {
        int64_t n = 0;
        for (auto row : hits) {
            n += texts_[row].find("request 1") != std::string::npos;
        }
        return n;
    }());
    EXPECT_EQ(All(Or(Grep("request 7 "), Grep("request 8 "))), (Rows{7, 8}));
}

// ---- limit-bounded evaluation ----------------------------------------------

TEST_P(GrepFilterExprTest, LimitReturnsFirstMatchesInPrimaryKeyOrder) {
    Load(Corpus(500, 11, 9));
    const auto all = All(Grep("timeout"));
    ASSERT_EQ(all, Containing("timeout"));
    for (int64_t limit : {1, 2, 7, 31, 32, 33, 54, 55, 56, 1000}) {
        auto plan = Plan(Grep("timeout"), limit);
        ASSERT_NE(plan->plan_node_->limit_predicate_, nullptr);
        auto result = Run(plan.get());
        EXPECT_EQ(result.rows, Prefix(all, limit)) << "limit=" << limit;
    }
}

TEST_P(GrepFilterExprTest, HasMoreIsSetOnlyWhenCandidatesRemain) {
    // The last match is the last row: evaluation that reaches it leaves no
    // candidate behind.
    auto texts = Corpus(100, 0, 10);
    texts.back() = "final timeout";
    Load(texts);
    const auto all = All(Grep("timeout"));
    const auto count = static_cast<int64_t>(all.size());

    EXPECT_TRUE(Run(Grep("timeout"), 1).has_more);
    EXPECT_TRUE(Run(Grep("timeout"), count - 1).has_more);
    // Exactly enough: the quota is met by the final candidate.
    auto exact = Run(Grep("timeout"), count);
    EXPECT_EQ(exact.rows, all);
    EXPECT_FALSE(exact.has_more);
    auto beyond = Run(Grep("timeout"), count + 5);
    EXPECT_EQ(beyond.rows, all);
    EXPECT_FALSE(beyond.has_more);
    // No candidate at all.
    auto none =
        Run(And(Pk(proto::plan::OpType::LessThan, 0), Grep("timeout")), 3);
    EXPECT_EQ(none.rows, Rows{});
    EXPECT_FALSE(none.has_more);
}

TEST_P(GrepFilterExprTest, LimitCombinesWithRemainingFilter) {
    Load(Corpus(400, 0, 3));
    const auto pk = And(Pk(proto::plan::OpType::GreaterEqual, 120),
                        Pk(proto::plan::OpType::LessThan, 300));
    const auto all = All(And(pk, Grep("timeout")));
    ASSERT_FALSE(all.empty());
    for (int64_t limit : {1, 5, 40, 500}) {
        for (const auto& predicate :
             {And(pk, Grep("timeout")), And(Grep("timeout"), pk)}) {
            auto plan = Plan(predicate, limit);
            ASSERT_NE(plan->plan_node_->limit_predicate_, nullptr);
            EXPECT_EQ(Run(plan.get()).rows, Prefix(all, limit))
                << "limit=" << limit;
        }
    }
}

TEST_P(GrepFilterExprTest, LimitWithSeveralGrepItems) {
    Load(Corpus(300, 0, 2));
    const auto predicate = And(Grep("timeout"), Grep("request 1"));
    const auto all = All(predicate);
    ASSERT_FALSE(all.empty());
    for (int64_t limit : {1, 3, 1000}) {
        EXPECT_EQ(Run(predicate, limit).rows, Prefix(all, limit));
    }
}

TEST_P(GrepFilterExprTest, LimitSkipsNullRows) {
    std::vector<std::string> texts(120, "timeout");
    std::vector<bool> valid(120, true);
    for (size_t i = 0; i < valid.size(); i += 2) {
        valid[i] = false;
    }
    Load(texts, valid);
    const auto all = All(Grep("timeout"));
    ASSERT_EQ(all.size(), 60);
    EXPECT_EQ(Run(Grep("timeout"), 10).rows, Prefix(all, 10));
}

TEST_P(GrepFilterExprTest, LimitHonoursDeletes) {
    Load(Corpus(200, 0, 4));
    auto all = All(Grep("timeout"));
    // Delete the first three matches.
    auto ids = std::make_unique<IdArray>();
    std::vector<Timestamp> timestamps;
    for (int i = 0; i < 3; ++i) {
        ids->mutable_int_id()->add_data(all[i]);
        timestamps.push_back(1000 + i);
    }
    segment->Delete(3, ids.get(), timestamps.data());
    Rows expected(all.begin() + 3, all.end());
    EXPECT_EQ(All(Grep("timeout")), expected);
    EXPECT_EQ(Run(Grep("timeout"), 5).rows, Prefix(expected, 5));
}

TEST_P(GrepFilterExprTest, CancellationStopsEvaluation) {
    Load(Corpus(300, 0, 3));
    folly::CancellationSource source;
    source.requestCancellation();
    auto bounded = Plan(Grep("timeout"), 5);
    EXPECT_ANY_THROW(Run(bounded.get(), source.getToken()));
    auto unbounded = Plan(Grep("timeout"), -1);
    EXPECT_ANY_THROW(Run(unbounded.get(), source.getToken()));
}

// ---- NGRAM: lazily materialized candidate bitmap ---------------------------

TEST_P(GrepFilterExprTest, DenseLimitNeverQueriesTheIndex) {
    if (IsGrowing() || TextType() != DataType::VARCHAR) {
        GTEST_SKIP() << "lazy NGRAM applies to sealed VARCHAR views";
    }
    Load(Corpus(2000, 0, 2));
    auto probe = AttachNgram();
    const auto all = Containing("timeout");
    auto result = Run(Grep("timeout"), 10);
    EXPECT_EQ(result.rows, Prefix(all, 10));
    EXPECT_EQ(probe->queries, 0);
}

TEST_P(GrepFilterExprTest, SparseLimitQueriesTheIndexOnce) {
    if (IsGrowing()) {
        GTEST_SKIP() << "NGRAM requires a sealed segment";
    }
    // Matches start far beyond the lazy row threshold.
    Load(Corpus(3000, 2500, 50));
    auto probe = AttachNgram();
    const auto all = Containing("timeout");
    ASSERT_FALSE(all.empty());
    auto result = Run(Grep("timeout"), 3);
    EXPECT_EQ(result.rows, Prefix(all, 3));
    EXPECT_EQ(probe->queries, 1);
}

TEST_P(GrepFilterExprTest, TextQueriesTheIndexBeforeReading) {
    if (IsGrowing() || TextType() != DataType::TEXT) {
        GTEST_SKIP() << "TEXT index-first applies to sealed TEXT";
    }
    Load(Corpus(500, 0, 2));
    auto probe = AttachNgram();
    auto result = Run(Grep("timeout"), 2);
    EXPECT_EQ(result.rows, Prefix(Containing("timeout"), 2));
    EXPECT_EQ(probe->queries, 1);
}

TEST_P(GrepFilterExprTest, ByteThresholdTriggersTheIndex) {
    if (IsGrowing() || TextType() != DataType::VARCHAR) {
        GTEST_SKIP() << "lazy NGRAM applies to sealed VARCHAR views";
    }
    // 40 rows of 16 KiB exceed 256 KiB long before the 256-row threshold.
    std::vector<std::string> texts;
    for (int i = 0; i < 200; ++i) {
        texts.push_back(std::string(16 * 1024, 'a') +
                        (i == 150 ? "\ntimeout" : ""));
    }
    Load(texts);
    auto probe = AttachNgram();
    auto result = Run(Grep("timeout"), 1);
    EXPECT_EQ(result.rows, (Rows{150}));
    EXPECT_EQ(probe->queries, 1);
}

TEST_P(GrepFilterExprTest, FilterOperatorUsesTheIndexForWholeSegment) {
    if (IsGrowing()) {
        GTEST_SKIP() << "NGRAM requires a sealed segment";
    }
    Load(Corpus(3000, 7, 13));
    const auto expected = All(Grep("timeout"));
    ASSERT_EQ(expected, Containing("timeout"));
    auto probe = AttachNgram();
    EXPECT_EQ(All(Grep("timeout")), expected);
    EXPECT_EQ(probe->queries, 1);
}

TEST_P(GrepFilterExprTest, SelectiveSiblingAvoidsTheIndex) {
    if (IsGrowing() || TextType() != DataType::VARCHAR) {
        GTEST_SKIP() << "lazy NGRAM applies to sealed VARCHAR views";
    }
    Load(Corpus(3000, 0, 3));
    auto probe = AttachNgram();
    // Only 20 rows reach grep, far below the lazy threshold.
    const auto pk = And(Pk(proto::plan::OpType::GreaterEqual, 1000),
                        Pk(proto::plan::OpType::LessThan, 1020));
    Rows expected;
    for (auto row : Containing("timeout")) {
        if (row >= 1000 && row < 1020)
            expected.push_back(row);
    }
    EXPECT_EQ(All(And(pk, Grep("timeout"))), expected);
    EXPECT_EQ(probe->queries, 0);
}

TEST_P(GrepFilterExprTest, IndexAndScanAgreeIncludingNullsUnderNot) {
    if (IsGrowing()) {
        GTEST_SKIP() << "NGRAM requires a sealed segment";
    }
    auto texts = Corpus(1500, 5, 11);
    std::vector<bool> valid(texts.size(), true);
    for (size_t i = 0; i < valid.size(); i += 17) {
        valid[i] = false;
    }
    Load(texts, valid);
    const auto grep = Grep("timeout");
    const auto positive = All(grep);
    const auto negative = All(Not(grep));
    const auto bounded = Run(grep, 20).rows;
    AttachNgram();
    EXPECT_EQ(All(grep), positive);
    // NULL rows excluded by the index must stay UNKNOWN under NOT.
    EXPECT_EQ(All(Not(grep)), negative);
    EXPECT_EQ(Run(grep, 20).rows, bounded);
    EXPECT_EQ(
        positive.size() + negative.size(),
        static_cast<size_t>(std::count(valid.begin(), valid.end(), true)));
}

// ---- randomized agreement --------------------------------------------------

TEST_P(GrepFilterExprTest, RandomizedBoundedMatchesUnbounded) {
    std::mt19937 rng(20261124);
    const std::vector<std::string> words = {
        "timeout", "error", "ok", "retry", "disk", "full", "超时", "warn"};
    for (int round = 0; round < 6; ++round) {
        const int rows = 200 + static_cast<int>(rng() % 600);
        std::vector<std::string> texts;
        std::vector<bool> valid;
        for (int i = 0; i < rows; ++i) {
            std::string value;
            const int lines = static_cast<int>(rng() % 4);
            for (int line = 0; line < lines; ++line) {
                const int count = 1 + static_cast<int>(rng() % 4);
                for (int w = 0; w < count; ++w) {
                    value += words[rng() % words.size()];
                    value += ' ';
                }
                value += '\n';
            }
            texts.push_back(std::move(value));
            valid.push_back(rng() % 10 != 0);
        }
        Load(texts, valid);
        if (!IsGrowing() && round % 2 == 1) {
            AttachNgram();
        }
        for (const auto& word : {"timeout", "超时", "disk full", "absent"}) {
            const auto grep = Grep(word);
            const auto all = All(grep);
            EXPECT_EQ(all, Containing(word)) << word;
            const auto pk = Pk(proto::plan::OpType::GreaterEqual, rows / 3);
            const auto filtered = All(And(pk, grep));
            for (int64_t limit : {1, 4, 50, 5000}) {
                auto result = Run(grep, limit);
                EXPECT_EQ(result.rows, Prefix(all, limit))
                    << word << " limit=" << limit << " round=" << round;
                const auto total = static_cast<int64_t>(all.size());
                if (total > limit) {
                    EXPECT_TRUE(result.has_more)
                        << word << " limit=" << limit << " round=" << round;
                } else if (total < limit) {
                    EXPECT_FALSE(result.has_more)
                        << word << " limit=" << limit << " round=" << round;
                }
                EXPECT_EQ(Run(And(pk, grep), limit).rows,
                          Prefix(filtered, limit))
                    << word << " limit=" << limit << " round=" << round;
            }
        }
    }
}

// ---- GREP_MATCHES output item ------------------------------------------------

constexpr int64_t kMatchesOutput = -1000;

class GrepMatchesColumnTest : public GrepFilterExprTest {
 protected:
    struct Item {
        std::vector<std::string> patterns;
        uint32_t pattern_kind = 1;
        uint32_t case_mode = 0;
        uint32_t boundary = 0;
        bool select_none = false;
        std::optional<uint64_t> max_count;
        uint64_t before = 0;
        uint64_t after = 0;
    };

    std::unique_ptr<query::RetrievePlan>
    PlanWithItem(const proto::plan::Expr& predicate,
                 int64_t limit,
                 const Item& item) const {
        proto::plan::PlanNode plan_node;
        auto* query = plan_node.mutable_query();
        *query->mutable_predicates() = predicate;
        query->set_limit(limit);
        plan_node.add_output_field_ids(kPkField);
        auto* projection = plan_node.add_projection_items();
        projection->set_output_id(kMatchesOutput);
        projection->set_data_type(proto::schema::DataType::JSON);
        auto* matches = projection->mutable_grep_matches();
        *matches->mutable_spec() = Grep(item.patterns,
                                        item.pattern_kind,
                                        item.case_mode,
                                        item.boundary,
                                        item.select_none)
                                       .grep_expr();
        if (item.max_count.has_value()) {
            matches->set_has_max_count(true);
            matches->set_max_count(*item.max_count);
        }
        matches->set_before_context(item.before);
        matches->set_after_context(item.after);
        auto parser = query::ProtoParser(schema);
        return parser.CreateRetrievePlan(plan_node);
    }

    struct Output {
        Rows rows;
        // One entry per returned row; nullopt for NULL.
        std::vector<std::optional<nlohmann::json>> matches;
    };

    Output
    RunWithItem(const proto::plan::Expr& predicate,
                int64_t limit,
                const Item& item) const {
        auto plan = PlanWithItem(predicate, limit, item);
        auto results = segment->Retrieve(nullptr,
                                         plan.get(),
                                         1L << 63,
                                         INT64_MAX,
                                         false,
                                         folly::CancellationToken(),
                                         0,
                                         0);
        Output out;
        out.rows.assign(results->offset().begin(), results->offset().end());
        const DataArray* column = nullptr;
        for (const auto& field : results->fields_data()) {
            if (field.field_id() == kMatchesOutput) {
                column = &field;
            }
        }
        EXPECT_NE(column, nullptr);
        if (column == nullptr) {
            return out;
        }
        EXPECT_EQ(column->type(), proto::schema::DataType::JSON);
        const auto& data = column->scalars().json_data().data();
        EXPECT_EQ(data.size(), static_cast<int>(out.rows.size()));
        EXPECT_EQ(column->valid_data_size(), static_cast<int>(out.rows.size()));
        for (int i = 0; i < data.size(); ++i) {
            if (column->valid_data(i)) {
                out.matches.emplace_back(nlohmann::json::parse(data[i]));
            } else {
                out.matches.emplace_back(std::nullopt);
            }
        }
        return out;
    }

    proto::plan::Expr
    AllRows() const {
        return Pk(proto::plan::OpType::GreaterEqual, 0);
    }
};

INSTANTIATE_TEST_SUITE_P(SealedAndGrowing,
                         GrepMatchesColumnTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(DataType::VARCHAR,
                                                              DataType::TEXT)));

TEST_P(GrepMatchesColumnTest, HoldsMatchingLinesOfEveryReturnedRow) {
    Load(Corpus(300, 3, 7));
    auto out = RunWithItem(Grep("timeout"), -1, {.patterns = {"timeout"}});
    EXPECT_EQ(out.rows, Containing("timeout"));
    ASSERT_EQ(out.matches.size(), out.rows.size());
    for (size_t i = 0; i < out.rows.size(); ++i) {
        ASSERT_TRUE(out.matches[i].has_value()) << out.rows[i];
        const auto& lines = *out.matches[i];
        ASSERT_EQ(lines.size(), 1);
        EXPECT_EQ(lines[0]["type"], "match");
        EXPECT_EQ(lines[0]["content"], "upstream timeout after 30s");
        EXPECT_EQ(lines[0]["line_number"], 2);
        const auto first_line =
            "request " + std::to_string(out.rows[i]) + " handled\n";
        EXPECT_EQ(lines[0]["line_start_offset"], first_line.size());
        EXPECT_EQ(lines[0]["starts"], nlohmann::json::array({9}));
        EXPECT_EQ(lines[0]["ends"], nlohmann::json::array({16}));
    }
}

TEST_P(GrepMatchesColumnTest, NullFieldAndNoMatchAreNull) {
    auto texts = Corpus(120, 0, 3);
    std::vector<bool> valid(texts.size(), true);
    for (size_t i = 0; i < valid.size(); i += 5) {
        valid[i] = false;
    }
    Load(texts, valid);
    auto out = RunWithItem(AllRows(), -1, {.patterns = {"timeout"}});
    ASSERT_EQ(out.rows.size(), texts.size());
    ASSERT_EQ(out.matches.size(), texts.size());
    for (size_t i = 0; i < texts.size(); ++i) {
        const bool expect = valid[i] && i % 3 == 0;
        EXPECT_EQ(out.matches[i].has_value(), expect) << i;
    }
}

TEST_P(GrepMatchesColumnTest, IsIndependentOfThePredicate) {
    Load({"alpha\nbeta", "alpha", "beta", "gamma\nalpha\nbeta"});
    auto out = RunWithItem(Grep("alpha"), -1, {.patterns = {"beta"}});
    EXPECT_EQ(out.rows, (Rows{0, 1, 3}));
    ASSERT_EQ(out.matches.size(), 3);
    ASSERT_TRUE(out.matches[0].has_value());
    EXPECT_EQ((*out.matches[0])[0]["line_number"], 2);
    EXPECT_FALSE(out.matches[1].has_value());
    ASSERT_TRUE(out.matches[2].has_value());
    EXPECT_EQ((*out.matches[2])[0]["line_number"], 3);
}

TEST_P(GrepMatchesColumnTest, HonoursContextMaxCountAndMatchOptions) {
    Load({"one\nTWO hit\nthree\nfour hit\nfive\nsix HIT"});
    auto out = RunWithItem(
        AllRows(), -1, {.patterns = {"hit"}, .before = 1, .after = 1});
    ASSERT_TRUE(out.matches[0].has_value());
    std::vector<std::string> types;
    std::vector<int64_t> numbers;
    for (const auto& line : *out.matches[0]) {
        types.push_back(line["type"]);
        numbers.push_back(line["line_number"]);
        EXPECT_EQ(line.contains("starts"), line["type"] == "match");
    }
    EXPECT_EQ(numbers, (std::vector<int64_t>{1, 2, 3, 4, 5}));
    EXPECT_EQ(types,
              (std::vector<std::string>{
                  "context", "match", "context", "match", "context"}));

    out = RunWithItem(
        AllRows(), -1, {.patterns = {"hit"}, .case_mode = 1, .max_count = 2});
    ASSERT_TRUE(out.matches[0].has_value());
    ASSERT_EQ(out.matches[0]->size(), 2);
    EXPECT_EQ((*out.matches[0])[1]["line_number"], 4);

    out = RunWithItem(AllRows(), -1, {.patterns = {"hit"}, .case_mode = 1});
    ASSERT_EQ(out.matches[0]->size(), 3);

    // Regex with several spans on one line.
    out = RunWithItem(
        AllRows(), -1, {.patterns = {"[a-z]+e"}, .pattern_kind = 0});
    ASSERT_TRUE(out.matches[0].has_value());
    EXPECT_EQ((*out.matches[0])[0]["content"], "one");
    EXPECT_EQ((*out.matches[0])[0]["starts"], nlohmann::json::array({0}));

    // Whole-line boundary.
    out = RunWithItem(AllRows(), -1, {.patterns = {"three"}, .boundary = 2});
    ASSERT_EQ(out.matches[0]->size(), 1);
    EXPECT_EQ((*out.matches[0])[0]["line_number"], 3);
}

TEST_P(GrepMatchesColumnTest, SelectNoneYieldsNullWithoutCompiling) {
    Load(Corpus(40, 0, 2));
    // An invalid regex is not compiled when "-m 0" selects nothing.
    auto out = RunWithItem(
        AllRows(),
        -1,
        {.patterns = {"("}, .pattern_kind = 0, .select_none = true});
    ASSERT_EQ(out.matches.size(), 40);
    for (const auto& value : out.matches) {
        EXPECT_FALSE(value.has_value());
    }
}

TEST_P(GrepMatchesColumnTest, InvalidRegexFailsTheRequest) {
    Load(Corpus(40, 0, 2));
    EXPECT_ANY_THROW(
        RunWithItem(AllRows(), -1, {.patterns = {"("}, .pattern_kind = 0}));
}

TEST_P(GrepMatchesColumnTest, IsComputedOnlyForRowsWithinLimit) {
    Load(Corpus(2000, 0, 2));
    auto out = RunWithItem(Grep("timeout"), 7, {.patterns = {"timeout"}});
    EXPECT_EQ(out.rows, Prefix(Containing("timeout"), 7));
    ASSERT_EQ(out.matches.size(), 7);
    for (const auto& value : out.matches) {
        EXPECT_TRUE(value.has_value());
    }
}

TEST_P(GrepMatchesColumnTest, EmptyResultHasAnEmptyJsonColumn) {
    Load(Corpus(50, 0, 2));
    auto out = RunWithItem(Grep("absent"), -1, {.patterns = {"timeout"}});
    EXPECT_TRUE(out.rows.empty());
    EXPECT_TRUE(out.matches.empty());
}

TEST_P(GrepMatchesColumnTest, ResultBudgetFailsTheRequest) {
    Load(Corpus(200, 0, 1));
    auto& config = SegcoreConfig::default_config();
    const auto saved = config.get_grep_max_result_bytes();
    config.set_grep_max_result_bytes(512);
    EXPECT_ANY_THROW(RunWithItem(AllRows(), -1, {.patterns = {"timeout"}}));
    config.set_grep_max_result_bytes(saved);
    auto out = RunWithItem(AllRows(), -1, {.patterns = {"timeout"}});
    EXPECT_EQ(out.matches.size(), 200);
}

TEST_P(GrepMatchesColumnTest, MultibyteOffsetsAreUtf8Bytes) {
    Load({"第一行\n请求超时 timeout\n结束"});
    auto out = RunWithItem(AllRows(), -1, {.patterns = {"超时"}});
    ASSERT_TRUE(out.matches[0].has_value());
    const auto& line = (*out.matches[0])[0];
    EXPECT_EQ(line["content"], "请求超时 timeout");
    EXPECT_EQ(line["line_number"], 2);
    EXPECT_EQ(line["line_start_offset"], 10);
    EXPECT_EQ(line["starts"], nlohmann::json::array({6}));
    EXPECT_EQ(line["ends"], nlohmann::json::array({12}));
}

// ---- reading text: chunks, LOB files, stored bytes ---------------------------

TEST_P(GrepMatchesColumnTest, SealedChunkBoundaries) {
    if (IsGrowing()) {
        GTEST_SKIP() << "sealed chunk layout";
    }
    auto texts = Corpus(200, 1, 3);
    std::vector<bool> valid(texts.size(), true);
    for (size_t i = 0; i < valid.size(); i += 11) {
        valid[i] = false;
    }
    Load(texts, valid, {1, 63, 64, 2, 70});
    const auto expected = Containing("timeout");
    EXPECT_EQ(All(Grep("timeout")), expected);
    EXPECT_EQ(Run(Grep("timeout"), 9).rows, Prefix(expected, 9));
    auto out = RunWithItem(AllRows(), -1, {.patterns = {"timeout"}});
    ASSERT_EQ(out.matches.size(), texts.size());
    for (size_t i = 0; i < texts.size(); ++i) {
        const bool hit = valid[i] && i >= 1 && (i - 1) % 3 == 0;
        ASSERT_EQ(out.matches[i].has_value(), hit) << i;
        if (hit) {
            EXPECT_EQ((*out.matches[i])[0]["content"],
                      "upstream timeout after 30s");
        }
    }
}

TEST_P(GrepMatchesColumnTest, InlineAndFileBackedText) {
    if (IsGrowing() || TextType() != DataType::TEXT) {
        GTEST_SKIP() << "sealed internal TEXT only";
    }
    const std::vector<std::string> texts = {
        "timeout inline",
        std::string(4096, 'x') + "\ntimeout file",
        "",
        "timeout null",
        "超时 timeout",
        std::string(5000, 'y'),
        "short"};
    const std::vector<bool> valid = {true, true, true, false, true, true, true};
    for (bool indexed : {false, true}) {
        LoadLobText(texts, valid, {2, 5});
        if (indexed) {
            AttachNgram();
        }
        EXPECT_EQ(All(Grep("timeout")), (Rows{0, 1, 4})) << indexed;
        EXPECT_EQ(Run(Grep("timeout"), 2).rows, (Rows{0, 1})) << indexed;
        EXPECT_EQ(All(Not(Grep("timeout"))), (Rows{2, 5, 6})) << indexed;
        auto out = RunWithItem(AllRows(), -1, {.patterns = {"timeout"}});
        ASSERT_EQ(out.matches.size(), texts.size());
        const std::vector<std::string> content = {
            "timeout inline", "timeout file", "", "", "超时 timeout", "", ""};
        for (size_t i = 0; i < texts.size(); ++i) {
            ASSERT_EQ(out.matches[i].has_value(), !content[i].empty()) << i;
            if (!content[i].empty()) {
                ASSERT_EQ(out.matches[i]->size(), 1);
                EXPECT_EQ((*out.matches[i])[0]["content"], content[i]);
            }
        }
        EXPECT_EQ((*out.matches[1])[0]["line_number"], 2);
        EXPECT_EQ((*out.matches[1])[0]["line_start_offset"], 4097);
    }
}

TEST_P(GrepMatchesColumnTest, JsonEscapingRoundTrips) {
    const std::string text = "\"foo\"\\\t";
    Load({text});
    auto out = RunWithItem(AllRows(), -1, {.patterns = {"foo"}});
    ASSERT_TRUE(out.matches[0].has_value());
    EXPECT_EQ((*out.matches[0])[0]["content"], text);
    EXPECT_EQ((*out.matches[0])[0]["starts"], nlohmann::json::array({1}));
    EXPECT_EQ((*out.matches[0])[0]["ends"], nlohmann::json::array({4}));

    // Each control character becomes six JSON bytes; the budget charges the
    // encoded size.
    Load({std::string(256, '\1') + "foo"});
    auto& config = SegcoreConfig::default_config();
    const auto saved = config.get_grep_max_result_bytes();
    config.set_grep_max_result_bytes(1024);
    EXPECT_ANY_THROW(RunWithItem(AllRows(), -1, {.patterns = {"foo"}}));
    config.set_grep_max_result_bytes(saved);
    out = RunWithItem(AllRows(), -1, {.patterns = {"foo"}});
    EXPECT_TRUE(out.matches[0].has_value());
}

TEST_P(GrepMatchesColumnTest, NullRowNeverSuppliesContext) {
    Load({"private", "timeout", "after"}, {false, true, true});
    auto out = RunWithItem(
        AllRows(), -1, {.patterns = {"timeout"}, .before = 100, .after = 100});
    ASSERT_EQ(out.matches.size(), 3);
    EXPECT_FALSE(out.matches[0].has_value());
    ASSERT_TRUE(out.matches[1].has_value());
    EXPECT_EQ(out.matches[1]->size(), 1);
    EXPECT_FALSE(out.matches[2].has_value());
}

TEST_P(GrepMatchesColumnTest, StoredInvalidUtf8FailsTheRequest) {
    Load({std::string("\xff", 1), "timeout"});
    EXPECT_ANY_THROW(All(Grep("timeout")));
    EXPECT_ANY_THROW(RunWithItem(AllRows(), -1, {.patterns = {"timeout"}}));
}

TEST_P(GrepMatchesColumnTest, CancellationStopsTheColumn) {
    Load(Corpus(4000, 0, 1));
    folly::CancellationSource source;
    source.requestCancellation();
    auto plan = PlanWithItem(AllRows(), -1, {.patterns = {"timeout"}});
    EXPECT_ANY_THROW(segment->Retrieve(nullptr,
                                       plan.get(),
                                       1L << 63,
                                       INT64_MAX,
                                       false,
                                       source.getToken(),
                                       0,
                                       0));
}

}  // namespace
}  // namespace milvus::segcore
