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
#include <google/protobuf/unknown_field_set.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "grep/Grep.h"
#include "grep/IndexCondition.h"
#include "index/contracts/query/INgramReader.h"
#include "index/scalar/ngram/NgramRegex.h"
#include "index/test_utils/AssertHelpers.h"
#include "index/test_utils/ScalarReaderFactory.h"
#include "pb/cgo_msg.pb.h"

namespace milvus::index::test {
namespace {
using grep::DecodeLiteralConditionTree;
using Condition = RegexLiteralCondition;
using Op = Condition::Op;
using Envelope = proto::cgo::LiteralConditionTree;
using Wire = proto::cgo::LiteralCondition;

Condition
Literal(std::string value) {
    return {Op::Literal, std::move(value), {}};
}

Envelope
LiteralEnvelope(const std::string& value) {
    Envelope message;

    message.mutable_root()->set_literal(value);
    return message;
}

template <typename F>
void
ExpectCode(F&& call, ErrorCode expected) {
    try {
        call();
        FAIL() << "expected SegcoreError";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), expected);
        auto status = FailureCStatus(&error);
        EXPECT_EQ(status.error_code, expected);
        free(const_cast<char*>(status.error_msg));
    }
}

grep::Plan
Compile(std::string_view pattern) {
    return grep::Plan::Compile({pattern}, {}, {8 * 1024 * 1024, 1024 * 1024});
}

Envelope
Nested(size_t depth) {
    Envelope message;

    auto* node = message.mutable_root();
    for (size_t level = 1; level < depth; ++level) {
        auto* list = node->mutable_all();
        list->add_children()->mutable_always_true();
        node = list->add_children();
    }
    node->set_literal("needle");
    return message;
}

Envelope
Wide() {
    Envelope message;

    for (int i = 0; i < 200; ++i) {
        message.mutable_root()->mutable_any()->add_children()->set_literal(
            "word" + std::to_string(i));
    }
    return message;
}

std::vector<std::string>
MalformedPayloads() {
    std::vector<std::string> result{
        "", "error.*timeout", std::string("\x08\x80", 2)};
    const auto valid = LiteralEnvelope("needle").SerializeAsString();
    for (size_t length = 1; length < valid.size(); ++length) {
        result.push_back(valid.substr(0, length));
    }
    result.push_back(valid + '\0');
    auto message = LiteralEnvelope("needle");

    message.clear_root();
    result.push_back(message.SerializeAsString());
    message.mutable_root();
    result.push_back(message.SerializeAsString());
    message.mutable_root()->mutable_all();
    result.push_back(message.SerializeAsString());
    message.mutable_root()
        ->mutable_all()
        ->add_children()
        ->mutable_always_true();
    result.push_back(message.SerializeAsString());
    message.mutable_root()
        ->mutable_all()
        ->add_children();  // Missing child kind.
    result.push_back(message.SerializeAsString());
    result.push_back(LiteralEnvelope("").SerializeAsString());
    auto wide = Wide();
    wide.mutable_root()->mutable_any()->mutable_children(199)->clear_kind();
    result.push_back(
        wide.SerializeAsString());  // Must validate beyond planner budget.
    return result;
}

TEST(NgramGrepCodecTest, UnknownFieldsFollowProtobufCompatibility) {
    // Unknown fields at each message level, including the empty True message.
    for (int level = 0; level < 4; ++level) {
        Envelope message;

        auto* root = message.mutable_root();
        auto* list = root->mutable_all();
        auto* truth = list->add_children()->mutable_always_true();
        list->add_children()->set_literal("needle");
        google::protobuf::Message* target = &message;
        if (level == 1)
            target = root;
        if (level == 2)
            target = list;
        if (level == 3)
            target = truth;
        target->GetReflection()->MutableUnknownFields(target)->AddVarint(99, 1);
        EXPECT_EQ(DecodeLiteralConditionTree(message.SerializeAsString()),
                  (Condition{Op::And, {}, {Condition{}, Literal("needle")}}));
    }
}

TEST(NgramGrepCodecTest, RustPayloadsPreserveNecessaryTreesAndOwnership) {
    const std::vector<std::pair<std::string, Condition>> cases{
        {".*", {}},
        {"error", Literal("error")},
        {"error.*timeout",
         {Op::And, {}, {Literal("error"), Literal("timeout")}}},
        {"error|warn", {Op::Or, {}, {Literal("error"), Literal("warn")}}},
        {"(error|warn).*timeout",
         {Op::And,
          {},
          {{Op::Or, {}, {Literal("error"), Literal("warn")}},
           Literal("timeout")}}},
        {"中文", Literal("中文")},
        {R"(a\x00b)", Literal(std::string("a\0b", 3))},
    };
    for (const auto& [pattern, expected] : cases) {
        SCOPED_TRACE(pattern);
        auto payload = Compile(pattern).ConditionPayload();
        auto decoded = DecodeLiteralConditionTree(payload);
        payload.assign("released");
        EXPECT_EQ(decoded, expected);
    }
}

TEST(NgramGrepCodecTest, PlanningBudgetsAreNotWireValidityLimits) {
    const auto wide = Wide().SerializeAsString();
    EXPECT_EQ(DecodeLiteralConditionTree(wide).NodeCount(), 201);
    EXPECT_TRUE(
        PlanNgramRegexCondition(DecodeLiteralConditionTree(wide), 3).IsTrue());
    const auto deepest = Nested(64).SerializeAsString();
    EXPECT_EQ(DecodeLiteralConditionTree(deepest).NodeCount(), 127);
    EXPECT_EQ(PlanNgramRegexCondition(DecodeLiteralConditionTree(deepest), 3),
              Literal("needle"));
    const auto deeper = Nested(70).SerializeAsString();
    EXPECT_EQ(DecodeLiteralConditionTree(deeper).NodeCount(), 139);
    EXPECT_TRUE(PlanNgramRegexCondition(DecodeLiteralConditionTree(deeper), 3)
                    .IsTrue());
    // C++ does not duplicate the Rust encoder's size/depth budgets.
    EXPECT_EQ(
        DecodeLiteralConditionTree(Nested(110).SerializeAsString()).NodeCount(),
        219);
    const std::string long_literal(16385, 'x');
    EXPECT_EQ(DecodeLiteralConditionTree(
                  LiteralEnvelope(long_literal).SerializeAsString()),
              Literal(long_literal));
    EXPECT_TRUE(
        PlanNgramRegexCondition(DecodeLiteralConditionTree(
                                    LiteralEnvelope("a").SerializeAsString()),
                                3)
            .IsTrue());
}

TEST(NgramGrepCodecTest, MalformedPayloadsKeepCodes) {
    const auto invalid = MalformedPayloads();
    for (size_t i = 0; i < invalid.size(); ++i) {
        SCOPED_TRACE(i);
        ExpectCode([&] { DecodeLiteralConditionTree(invalid[i]); },
                   DataFormatBroken);
    }
}

TEST(NgramGrepCodecTest, StandardProtobufMergeSemantics) {
    auto first = LiteralEnvelope("first");
    auto last = LiteralEnvelope("last");
    EXPECT_EQ(DecodeLiteralConditionTree(first.SerializeAsString() +
                                         last.SerializeAsString()),
              Literal("last"));
}

TEST(NgramGrepReaderTest, CandidatesContainActualGrepMatchesForTextAndVarchar) {
    ScalarTestData<std::string_view> data({"error -- timeout",
                                           "timeout error",
                                           "warn timeout",
                                           "nothing",
                                           "ERROR timeout",
                                           "error\n timeout",
                                           "abc1def",
                                           "abc2def",
                                           "Kfoo",
                                           std::string("a\0b tail", 8),
                                           "error timeout",
                                           "",
                                           "中文结束",
                                           "error ab"});
    data.validity.reset(10);
    const ScalarTestInput<std::string_view> input(data);
    const auto values = MakeStringViews(data.values);
    for (const auto* name : {"NgramVarcharMin2Max4Heap",
                             "NgramVarcharMin2Max4Mmap",
                             "NgramTextMin2Max4Heap",
                             "NgramTextMin2Max4Mmap"}) {
        SCOPED_TRACE(name);
        auto reader = ScalarReaderBackends().Get<std::string_view>(name).Create(
            input.View());
        const auto* ngram = dynamic_cast<const INgramReader*>(reader.get());
        ASSERT_NE(ngram, nullptr);
        for (const auto* pattern : {"error.*timeout",
                                    "(error|warn).*timeout",
                                    "abc[12]def",
                                    "(?i:k)foo",
                                    R"(a\x00b)",
                                    "中文",
                                    "(?i:error).*timeout",
                                    "absent_phrase"}) {
            SCOPED_TRACE(pattern);
            auto plan = Compile(pattern);
            const auto condition =
                DecodeLiteralConditionTree(plan.ConditionPayload());
            ASSERT_TRUE(ngram->CanHandleConditionTree(condition));
            const auto exact =
                plan.CreateSession().ExecuteBatch(values, 1024 * 1024);
            for (bool sparse : {false, true}) {
                TargetBitmap candidates(data.values.size(), false);
                for (size_t i = 0; i < data.values.size(); ++i)
                    candidates[i] = !sparse || i % 3 == 0;
                auto initial = candidates.clone();
                ngram->ConditionTreeCandidates(condition, candidates);
                for (size_t i = 0; i < data.values.size(); ++i) {
                    if (initial[i] && data.validity[i] && exact.Selected(i))
                        EXPECT_TRUE(candidates[i]) << i;
                    EXPECT_FALSE(candidates[i] && !initial[i]) << i;
                    if (!data.validity[i])
                        EXPECT_FALSE(candidates[i]) << i;
                }
                if (!sparse && std::string_view(pattern) == "error.*timeout") {
                    EXPECT_TRUE(
                        candidates[1]);  // Both literals, but wrong order.
                    EXPECT_FALSE(exact.Selected(1));
                }
                if (std::string_view(pattern) == "absent_phrase")
                    EXPECT_TRUE(candidates.none());
            }
        }
        TargetBitmap re2(data.values.size(), true);
        ASSERT_TRUE(ngram->CanHandle("error.*timeout", PatternOp::RegexMatch));
        ngram->Candidates("error.*timeout", PatternOp::RegexMatch, re2);
        EXPECT_TRUE(re2[0]);
        EXPECT_TRUE(re2[1]);
    }
}

TEST(NgramGrepReaderTest, IneligibilityAndMalformedInputsRemainDistinct) {
    for (bool empty_reader : {false, true}) {
        ScalarTestData<std::string_view> data(
            empty_reader ? std::vector<std::string>{}
                         : std::vector<std::string>{"error timeout", "ab"});
        const ScalarTestInput<std::string_view> input(data);
        auto reader = ScalarReaderBackends()
                          .Get<std::string_view>("NgramVarcharMin3Max3Heap")
                          .Create(input.View());
        const auto* ngram = dynamic_cast<const INgramReader*>(reader.get());
        ASSERT_NE(ngram, nullptr);
        for (const auto& payload : {Compile(".*").ConditionPayload(),
                                    Compile("error|ab").ConditionPayload(),
                                    Wide().SerializeAsString(),
                                    Nested(70).SerializeAsString()}) {
            const auto condition = DecodeLiteralConditionTree(payload);
            EXPECT_FALSE(ngram->CanHandleConditionTree(condition));
        }
        auto invalid = MalformedPayloads();
        for (size_t i = 0; i < invalid.size(); ++i) {
            SCOPED_TRACE(i);
            const auto code = DataFormatBroken;
            ExpectCode([&] { DecodeLiteralConditionTree(invalid[i]); }, code);
            for (bool selected : {false, true}) {
                TargetBitmap candidates(data.values.size(), selected);
                auto before = candidates.clone();
                ExpectCode(
                    [&] {
                        const auto condition =
                            DecodeLiteralConditionTree(invalid[i]);
                        ngram->ConditionTreeCandidates(condition, candidates);
                    },
                    code);
                ExpectBitmap(candidates, before);
            }
        }
    }
}

void
CheckNoNgramInterface(const IIndexReaderBase& reader) {
    const auto* pattern = dynamic_cast<const IPatternMatchReader*>(&reader);
    ASSERT_NE(pattern, nullptr);
    EXPECT_EQ(dynamic_cast<const INgramReader*>(&reader), nullptr);
}

TEST(NgramGrepReaderTest, OtherPatternReadersDoNotExposeNgramInterface) {
    ScalarTestData<std::string_view> data({"alpha", "beta"});
    const ScalarTestInput<std::string_view> input(data);
    for (const auto* name : {"BitmapVarchar",
                             "SortedVarchar",
                             "InvertedVarchar",
                             "MarisaVarchar",
                             "FmIndexVarchar",
                             "HybridVarchar"}) {
        SCOPED_TRACE(name);
        auto reader = ScalarReaderBackends().Get<std::string_view>(name).Create(
            input.View());
        ASSERT_NO_FATAL_FAILURE(CheckNoNgramInterface(*reader));
    }
}
}  // namespace
}  // namespace milvus::index::test
