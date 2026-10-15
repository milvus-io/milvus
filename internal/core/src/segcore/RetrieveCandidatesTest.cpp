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
#include <glog/logging.h>
#include <algorithm>
#include <cstdlib>
#include <memory>
#include <tuple>
#include <vector>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "query/PlanImpl.h"
#include "query/PlanProto.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/plan_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {
namespace {

class CandidateScanWarnings : public google::LogSink {
 public:
    CandidateScanWarnings() {
        google::AddLogSink(this);
    }
    ~CandidateScanWarnings() override {
        google::RemoveLogSink(this);
    }
    void
    send(google::LogSeverity severity,
         const char*,
         const char*,
         int,
         const struct tm*,
         const char* message,
         size_t length) override {
        std::string text(message, length);
        if (severity == google::GLOG_WARNING &&
            text.find("Candidate scan reached row cap") != std::string::npos) {
            messages.push_back(std::move(text));
        }
    }
    std::vector<std::string> messages;
};

class RetrieveCandidatesTest
    : public ::testing::TestWithParam<std::tuple<bool, DataType>> {
 protected:
    static constexpr int64_t kRows = 32;
    static constexpr int kDim = 128;
    SchemaPtr schema;
    FieldId pk{100};
    FieldId payload{101};
    std::unique_ptr<SegmentInternalInterface> segment;
    std::unique_ptr<query::RetrievePlan> plan;
    std::vector<int64_t> int_pks;
    std::vector<std::string> string_pks;

    void
    SetUp() override {
        schema = std::make_shared<Schema>();
        pk = schema->AddDebugField("pk", std::get<1>(GetParam()));
        payload = schema->AddDebugField(
            "payload", DataType::VECTOR_FLOAT, kDim, "L2");
        schema->set_primary_field_id(pk);
        auto data = DataGen(schema, kRows);
        if (std::get<1>(GetParam()) == DataType::INT64) {
            const auto values = data.get_col<int64_t>(pk);
            int_pks.assign(values.begin(), values.end());
        } else {
            const auto values = data.get_col<std::string>(pk);
            string_pks.assign(values.begin(), values.end());
        }
        for (int i = 0; i < kRows; ++i) {
            data.timestamps_[i] = i < kRows / 2 ? 10 : 20;
        }
        if (std::get<0>(GetParam())) {
            auto growing = CreateGrowingSegment(schema, empty_index_meta);
            const auto offset = growing->PreInsert(kRows);
            growing->Insert(offset,
                            kRows,
                            data.row_ids_.data(),
                            data.timestamps_.data(),
                            data.SharedRaw());
            segment = std::move(growing);
        } else {
            segment = CreateSealedWithFieldDataLoaded(schema, data);
        }
        plan = std::make_unique<query::RetrievePlan>(schema);
        plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
        plan->plan_node_->plannodes_ =
            std::make_shared<milvus::plan::MvccNode>("candidates");
        plan->plan_node_->limit_ = 2;
        plan->field_ids_ = {payload, pk, TimestampFieldID};
    }

    void
    SetMode(CRetrieveMode mode, int64_t max_rows = 0) {
        auto status = SetRetrievePlanMode(plan.get(), mode, max_rows);
        ASSERT_EQ(status.error_code, Success) << status.error_msg;
    }

    template <typename F>
    void
    ExpectCode(F&& run, ErrorCode expected) {
        try {
            run();
            FAIL() << "expected a typed error";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), expected);
        }
    }

    proto::plan::GenericValue
    PkValue(size_t row) const {
        proto::plan::GenericValue value;
        if (std::get<1>(GetParam()) == DataType::INT64) {
            value.set_int64_val(int_pks[row]);
        } else {
            value.set_string_val(string_pks[row]);
        }
        return value;
    }
};

INSTANTIATE_TEST_SUITE_P(
    SealedAndGrowing,
    RetrieveCandidatesTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Values(DataType::INT64, DataType::VARCHAR)));

TEST_P(RetrieveCandidatesTest, UnlimitedLightweightResultAndModeReset) {
    const auto fields = plan->field_ids_;
    ASSERT_EQ(plan->mode_, query::RetrieveMode::Normal);
    auto normal = segment->Retrieve(
        nullptr, plan.get(), 100, DEFAULT_MAX_OUTPUT_SIZE, false);
    ASSERT_EQ(normal->offset_size(), 2);
    ASSERT_EQ(normal->fields_data_size(), 3);

    SetMode(RetrieveModeCandidates);
    EXPECT_TRUE(ShouldIgnoreNonPk(plan.get()));
    // This fits candidate metadata but not all 32 projected 128-dim vectors.
    auto candidates = segment->Retrieve(nullptr, plan.get(), 100, 4096, false);
    ASSERT_EQ(candidates->offset_size(), kRows);
    EXPECT_FALSE(candidates->has_more_result());
    EXPECT_FALSE(candidates->has_ids());
    ASSERT_EQ(candidates->fields_data_size(), 0);
    auto identities =
        segment->RetrieveIdentitiesByOffsets(nullptr,
                                             plan.get(),
                                             candidates->offset().data(),
                                             candidates->offset_size(),
                                             true);
    ASSERT_EQ(identities->fields_data_size(), 1);
    EXPECT_EQ(identities->fields_data(0).field_id(), TimestampFieldID.get());
    for (int i = 0; i < kRows; ++i) {
        const auto offset = candidates->offset(i);
        ASSERT_GE(offset, 0);
        ASSERT_LT(offset, kRows);
        if (std::get<1>(GetParam()) == DataType::INT64) {
            ASSERT_EQ(identities->ids().int_id().data_size(), kRows);
            EXPECT_EQ(identities->ids().int_id().data(i), int_pks[offset]);
        } else {
            ASSERT_EQ(identities->ids().str_id().data_size(), kRows);
            EXPECT_EQ(identities->ids().str_id().data(i), string_pks[offset]);
        }
        EXPECT_EQ(identities->fields_data(0).scalars().long_data().data(i),
                  offset < kRows / 2 ? 10 : 20);
    }
    EXPECT_EQ(plan->plan_node_->limit_, 2);
    EXPECT_EQ(plan->field_ids_, fields);

    // Candidate mode must not affect explicit late materialization by offsets.
    std::vector<int64_t> selected{candidates->offset(1), candidates->offset(7)};
    auto late = segment->Retrieve(
        nullptr, plan.get(), selected.data(), selected.size(), {});
    ASSERT_EQ(late->fields_data_size(), 3);
    EXPECT_EQ(late->fields_data(0).vectors().float_vector().data_size(),
              selected.size() * kDim);

    SetMode(RetrieveModeNormal);
    auto again = segment->Retrieve(
        nullptr, plan.get(), 100, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_EQ(again->offset_size(), 2);
    EXPECT_EQ(again->fields_data_size(), 3);
    EXPECT_EQ(again->ids().SerializeAsString(),
              normal->ids().SerializeAsString());
}

TEST_P(RetrieveCandidatesTest, DefersPkEvenWhenNotProjected) {
    plan->field_ids_ = {payload};
    SetMode(RetrieveModeCandidates);
    auto result = segment->Retrieve(nullptr, plan.get(), 100, 4096, false);
    EXPECT_EQ(result->offset_size(), kRows);
    EXPECT_FALSE(result->has_ids());
    const std::vector<int64_t> selected{result->offset(7), result->offset(1)};
    auto identities = segment->RetrieveIdentitiesByOffsets(
        nullptr, plan.get(), selected.data(), selected.size(), true);
    EXPECT_TRUE(identities->has_ids());
    EXPECT_EQ(identities->fields_data_size(), 0);
    for (size_t i = 0; i < selected.size(); ++i) {
        EXPECT_EQ(identities->offset(i), selected[i]);
        if (std::get<1>(GetParam()) == DataType::INT64) {
            EXPECT_EQ(identities->ids().int_id().data(i), int_pks[selected[i]]);
        } else {
            EXPECT_EQ(identities->ids().str_id().data(i),
                      string_pks[selected[i]]);
        }
    }
    EXPECT_EQ(result->fields_data_size(), 0);
    EXPECT_EQ(plan->field_ids_, std::vector<FieldId>{payload});
}

TEST_P(RetrieveCandidatesTest, IdentityTakeGateDoesNotMutateOutputPlan) {
    const std::vector<int64_t> offsets{7, 1};
    const auto fields = plan->field_ids_;
    for (bool output_allowed : {false, true}) {
        plan->take_for_output_allowed_ = output_allowed;
        auto identities = segment->RetrieveIdentitiesByOffsets(nullptr,
                                                               plan.get(),
                                                               offsets.data(),
                                                               offsets.size(),
                                                               !output_allowed);
        EXPECT_EQ(identities->offset_size(), offsets.size());
        EXPECT_EQ(plan->take_for_output_allowed_, output_allowed);
        EXPECT_EQ(plan->field_ids_, fields);
    }
}

TEST_P(RetrieveCandidatesTest, FilterAndVisibilityMatchUnlimitedReference) {
    proto::schema::IDs deleted;
    if (std::get<1>(GetParam()) == DataType::INT64) {
        deleted.mutable_int_id()->add_data(int_pks[0]);
    } else {
        deleted.mutable_str_id()->add_data(string_pks[0]);
    }
    const Timestamp delete_ts = 12;
    segment->Delete(1, &deleted, &delete_ts);
    auto filter = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(pk, std::get<1>(GetParam()), {}),
        std::vector<proto::plan::GenericValue>{
            PkValue(0), PkValue(1), PkValue(20)});
    plan->plan_node_->plannodes_ = test::CreateRetrievePlanByExpr(filter);
    plan->plan_node_->limit_ = Unlimited;
    auto reference = segment->Retrieve(
        nullptr, plan.get(), 15, DEFAULT_MAX_OUTPUT_SIZE, true);
    ASSERT_EQ(reference->offset_size(), 1);
    EXPECT_EQ(reference->offset(0), 1);

    plan->plan_node_->limit_ = 1;
    SetMode(RetrieveModeCandidates);
    auto candidates = segment->Retrieve(nullptr, plan.get(), 15, 4096, false);
    EXPECT_EQ(candidates->offset_size(), reference->offset_size());
    EXPECT_EQ(candidates->offset(0), reference->offset(0));
    auto empty = segment->Retrieve(nullptr, plan.get(), 1, 4096, false);
    EXPECT_EQ(empty->offset_size(), 0);
    auto empty_ids = segment->RetrieveIdentitiesByOffsets(
        nullptr, plan.get(), nullptr, 0, true);
    EXPECT_EQ(empty_ids->offset_size(), 0);
    EXPECT_EQ(empty_ids->ids().int_id().data_size(), 0);
    EXPECT_EQ(empty_ids->ids().str_id().data_size(), 0);
}

TEST_P(RetrieveCandidatesTest, BudgetsAndCancellation) {
    SetMode(RetrieveModeCandidates);
    ExpectCode([&] { segment->Retrieve(nullptr, plan.get(), 100, 1, false); },
               RetrieveError);  // Fails before allocating the complete offsets.
    auto offsets_only =
        segment->Retrieve(nullptr, plan.get(), 100, kRows * 8, false);
    EXPECT_EQ(offsets_only->offset_size(), kRows);
    ExpectCode([&] { segment->Retrieve(nullptr, plan.get(), 100, 0, false); },
               UnexpectedError);
    folly::CancellationSource cancellation;
    cancellation.requestCancellation();
    EXPECT_ANY_THROW(segment->Retrieve(
        nullptr, plan.get(), 100, 4096, false, cancellation.getToken(), 0, 0));
    EXPECT_ANY_THROW(segment->RetrieveIdentitiesByOffsets(
        nullptr, plan.get(), nullptr, 0, true, cancellation.getToken()));
}

TEST_P(RetrieveCandidatesTest, CandidateRowCap) {
    SetMode(RetrieveModeCandidates);
    auto reference = segment->Retrieve(nullptr, plan.get(), 100, 4096, false);
    SetMode(RetrieveModeCandidates, 4);
    {
        CandidateScanWarnings warnings;
        // The budget fits four offsets, not the full candidate set.
        auto capped = segment->Retrieve(nullptr, plan.get(), 100, 4 * 8, false);
        ASSERT_EQ(capped->offset_size(), 4);
        EXPECT_TRUE(capped->has_more_result());
        EXPECT_FALSE(capped->has_ids());
        EXPECT_EQ(plan->plan_node_->limit_, 2);
        for (int i = 0; i < 4; ++i) {
            EXPECT_EQ(capped->offset(i), reference->offset(i));
        }
        ASSERT_EQ(warnings.messages.size(), 1);
        EXPECT_NE(warnings.messages[0].find("maxRows=4"), std::string::npos);
        EXPECT_NE(warnings.messages[0].find("returnedRows=4"),
                  std::string::npos);
    }
    CandidateScanWarnings no_warnings;
    SetMode(RetrieveModeCandidates, kRows - 1);
    // Count after MVCC, not all rows in the segment.
    auto visible = segment->Retrieve(nullptr, plan.get(), 15, 4096, false);
    EXPECT_EQ(visible->offset_size(), kRows / 2);
    EXPECT_FALSE(visible->has_more_result());
    SetMode(RetrieveModeCandidates, kRows);
    auto exact = segment->Retrieve(nullptr, plan.get(), 100, 4096, false);
    EXPECT_EQ(exact->offset_size(), kRows);
    EXPECT_FALSE(exact->has_more_result());
    EXPECT_TRUE(no_warnings.messages.empty());
    auto status = SetRetrievePlanMode(plan.get(), RetrieveModeCandidates, -1);
    EXPECT_EQ(status.error_code, UnexpectedError);
    std::free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(plan->max_candidate_rows_, kRows);
    SetMode(RetrieveModeNormal, 1);
    auto normal = segment->Retrieve(nullptr, plan.get(), 100, 4096, false);
    EXPECT_EQ(normal->offset_size(), 2);
}

TEST_P(RetrieveCandidatesTest, RejectsNonCandidatePlansBeforeExecution) {
    SetMode(RetrieveModeCandidates);
    plan->plan_node_->has_order_by_ = true;
    ExpectCode([&] { segment->Retrieve(nullptr, plan.get(), 1, 4096, false); },
               NotImplemented);
    plan->plan_node_->has_order_by_ = false;
    plan->plan_node_->query_iterator_cursor_.emplace();
    ExpectCode([&] { segment->Retrieve(nullptr, plan.get(), 1, 4096, false); },
               NotImplemented);
    plan->plan_node_->query_iterator_cursor_.reset();
    proto::plan::PlanNode count;
    auto* aggregate = count.mutable_query()->add_aggregates();
    aggregate->set_op(proto::plan::count);
    aggregate->set_field_id(pk.get());
    plan = query::ProtoParser(schema).CreateRetrievePlan(count);
    SetMode(RetrieveModeCandidates);
    ExpectCode([&] { segment->Retrieve(nullptr, plan.get(), 1, 4096, false); },
               NotImplemented);
}

TEST(RetrieveCandidatesMode, CApiRejectsInvalidModeWithoutChangingPlan) {
    auto schema = std::make_shared<Schema>();
    query::RetrievePlan plan(schema);
    auto status = SetRetrievePlanMode(&plan, RetrieveModeCandidates, 0);
    ASSERT_EQ(status.error_code, Success);
    status = SetRetrievePlanMode(&plan, static_cast<CRetrieveMode>(999), 0);
    ASSERT_EQ(status.error_code, UnexpectedError);
    std::free(const_cast<char*>(status.error_msg));
    EXPECT_EQ(plan.mode_, query::RetrieveMode::Candidates);
    status = SetRetrievePlanMode(nullptr, RetrieveModeNormal, 0);
    ASSERT_EQ(status.error_code, UnexpectedError);
    std::free(const_cast<char*>(status.error_msg));
}

}  // namespace
}  // namespace milvus::segcore
